"""Resident V2 service: window polling plus one independent loop per resident stage.

A resident (INTERVAL or CONTINUOUS) stage ticks in its own thread, so a slow or failing reference
refresh, Options flow feed or window never delays another. Every tick and every window is isolated:
an exception is counted and reported, never propagated into a sibling.
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

from .orchestration_repository import OrchestrationRepository, WindowSpec
from .stage_graph import RESIDENT_SCHEDULES, STAGES_V1
from .stage_orchestrator import StageOrchestrator, WindowReport

logger = logging.getLogger(__name__)
# OPTIONS_FLOW does one bounded slice of work per tick, so it ticks back to back (a short pause only
# keeps an idle feed from spinning); REFERENCE refreshes every six hours.
DEFAULT_RESIDENT_INTERVALS = {"REFERENCE": 21_600, "OPTIONS_FLOW": 0.25}


def window_specs_for_session(
    cohort_id: UUID, session_date: date, provider_tier: str, *, completion_budget_seconds: int,
    serving_lag_seconds: int, calendar=None,
) -> tuple[WindowSpec, ...]:
    """Fifteen-minute Developer windows for a session; the window key is the cutoff in UTC."""
    from .session_windows import developer_session_windows

    windows = developer_session_windows(
        session_date, completion_budget_seconds=completion_budget_seconds,
        serving_lag_seconds=serving_lag_seconds, calendar=calendar)
    return tuple(
        WindowSpec(cohort_id, provider_tier, window.cutoff.astimezone(timezone.utc).isoformat(),
                   window.available_after, window.cutoff, window.deadline)
        for window in windows)


@dataclass(slots=True)
class PollReport:
    windows: list[WindowReport] = field(default_factory=list)
    window_errors: dict[str, str] = field(default_factory=dict)


class V2Service:
    def __init__(
        self,
        orchestrator: StageOrchestrator,
        repository: OrchestrationRepository,
        plan_id: UUID,
        specs: Sequence[WindowSpec],
        *,
        clock: Callable[[], datetime] | None = None,
        resident_intervals: Mapping[str, float] | None = None,
    ) -> None:
        self._orchestrator = orchestrator
        self._repository = repository
        self._plan_id = plan_id
        self._specs = tuple(sorted(specs, key=lambda spec: spec.release_at))
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._intervals = dict(DEFAULT_RESIDENT_INTERVALS)
        self._intervals.update(resident_intervals or {})
        self.resident_errors: dict[str, str] = {}

    def poll_windows(self) -> PollReport:
        """Run every window that is due, oldest first. A failing window never stops the next one."""
        report = PollReport()
        now = self._clock()
        for spec in self._specs:
            if spec.release_at > now:
                break
            try:
                record = self._repository.ensure_window(self._plan_id, spec)
                result = self._orchestrator.run_window(record.window_id)
            except Exception as error:  # noqa: BLE001 - window isolation
                logger.exception("window %s failed", spec.window_key)
                report.window_errors[spec.window_key] = type(error).__name__
                continue
            if result.status != "NOT_CLAIMED":
                report.windows.append(result)
        return report

    def tick_resident_once(self, stage_id: str) -> str:
        try:
            outcome = self._orchestrator.tick_resident(self._plan_id, stage_id)
        except Exception as error:  # noqa: BLE001 - resident isolation
            logger.exception("resident stage %s tick failed", stage_id)
            self.resident_errors[stage_id] = type(error).__name__
            return "ERROR"
        self.resident_errors.pop(stage_id, None)
        return outcome.state

    def resident_stage_ids(self) -> tuple[str, ...]:
        plan = self._repository.load_plan(self._plan_id)
        return tuple(sorted(name for name in plan.enabled_stages
                            if STAGES_V1[name].schedule_kind in RESIDENT_SCHEDULES))

    def run(self, stop: threading.Event, *, poll_seconds: float = 5.0) -> None:
        threads = [
            threading.Thread(target=self._resident_loop, args=(name, stop), name=f"resident-{name}", daemon=True)
            for name in self.resident_stage_ids()
        ]
        for thread in threads:
            thread.start()
        try:
            while not stop.is_set():
                self.poll_windows()
                stop.wait(poll_seconds)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=10)

    def _resident_loop(self, stage_id: str, stop: threading.Event) -> None:
        interval = self._intervals.get(stage_id)
        if interval is None or interval <= 0:
            raise ValueError(f"resident stage {stage_id} requires a positive interval")
        while not stop.is_set():
            self.tick_resident_once(stage_id)
            stop.wait(interval)
