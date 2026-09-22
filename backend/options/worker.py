from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from options.calendar import OptionExchangeCalendar
from options.orchestration import ManualCycleResult, ManualOptionPipeline
from options.repositories.leadership import OptionSchedulerLeadership


LOGGER = logging.getLogger("option-worker")
FAILURE_RETRY_DELAY = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class OptionWorkerSettings:
    poll_seconds: int = 15
    slot_seconds: int = 900
    provider_delay_seconds: int = 900
    publication_grace_seconds: int = 30

    @classmethod
    def from_environment(cls) -> "OptionWorkerSettings":
        return cls(
            poll_seconds=int(os.getenv("OPTION_WORKER_POLL_SECONDS", "15")),
            slot_seconds=int(os.getenv("OPTION_SLOT_SECONDS", "900")),
            provider_delay_seconds=int(
                os.getenv("OPTION_PROVIDER_DELAY_SECONDS", "900")
            ),
            publication_grace_seconds=int(
                os.getenv("OPTION_PUBLICATION_GRACE_SECONDS", "30")
            ),
        )

    def __post_init__(self) -> None:
        if self.poll_seconds <= 0 or self.slot_seconds <= 0:
            raise ValueError("option worker poll and slot durations must be positive")
        if self.provider_delay_seconds < 0 or self.publication_grace_seconds < 0:
            raise ValueError("option worker delay and grace must not be negative")


class OptionMaterializationWorker:
    def __init__(
        self,
        pipeline: ManualOptionPipeline,
        *,
        calendar: OptionExchangeCalendar | None = None,
        settings: OptionWorkerSettings | None = None,
        underlyers: tuple[str, ...] | None = None,
        outcome_service=None,
        partition_maintainer: Callable[[datetime], object] | None = None,
        validation_callback: Callable[[datetime], object] | None = None,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
        effective_from: datetime | None = None,
        detector_status_callback: Callable[[dict], object] | None = None,
    ) -> None:
        if effective_from is not None and effective_from.utcoffset() is None:
            raise ValueError("forward worker start must be timezone-aware")
        self.effective_from = effective_from
        self.detector_status_callback = detector_status_callback
        self.pipeline = pipeline
        self.calendar = calendar or OptionExchangeCalendar()
        self.settings = settings or OptionWorkerSettings.from_environment()
        self.underlyers = underlyers
        self.outcome_service = outcome_service
        self.partition_maintainer = partition_maintainer
        self.validation_callback = validation_callback
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep or time.sleep
        self._completed_slots: set[datetime] = set()
        self._retry_slot: datetime | None = None
        self._retry_not_before: datetime | None = None
        self._partition_month: tuple[int, int] | None = None

    def poll_once(
        self,
        progress_callback: Callable[[], object] | None = None,
    ) -> ManualCycleResult | None:
        now = self.clock().astimezone(timezone.utc)
        partition_month = (now.year, now.month)
        if (
            self.partition_maintainer is not None
            and self._partition_month != partition_month
        ):
            self.partition_maintainer(now)
            self._partition_month = partition_month
        latest_slot = self.calendar.latest_delayed_slot(
            now,
            interval=timedelta(seconds=self.settings.slot_seconds),
            provider_delay=timedelta(
                seconds=self.settings.provider_delay_seconds
            ),
            publication_grace=timedelta(
                seconds=self.settings.publication_grace_seconds
            ),
        )
        if latest_slot is None or self.effective_from is not None and latest_slot < self.effective_from:
            return None
        if self._retry_slot is not None and self._retry_slot != latest_slot:
            LOGGER.warning(
                "option retry superseded slot=%s current_slot=%s; historical evidence retained",
                self._retry_slot.isoformat(),
                latest_slot.isoformat(),
            )
            self._retry_slot = None
            self._retry_not_before = None
        if (
            self._retry_slot is not None
            and self._retry_not_before is not None
            and now < self._retry_not_before
        ):
            return None
        slot = latest_slot
        if slot in self._completed_slots:
            return None
        LOGGER.info("materializing option slot=%s", slot.isoformat())
        attempt = dict(scheduled_cycle=slot.isoformat(), started_at=now.isoformat(), status="RUNNING")
        self._detector_status(attempt)
        try:
            result = self.pipeline.run_once(
                self.underlyers,
                as_of=now,
                cycle_time=slot,
                progress_callback=progress_callback,
            )
        except Exception:
            self._detector_status(dict(attempt, status="FAILED", reason="PIPELINE_EXECUTION_FAILED",
                finished_at=self.clock().astimezone(timezone.utc).isoformat()))
            raise
        detector = getattr(result, "detector_evaluation", None)
        status = "RECORDED" if detector and detector.get("status") in ("RECORDED", "ALREADY_RECORDED") else "FAILED" if detector and detector.get("status") == "FAILED" else "INCOMPLETE"
        self._detector_status(dict(attempt, status=status,
            reason=detector.get("reason") if detector else "DETECTOR_NOT_EVALUATED",
            finished_at=self.clock().astimezone(timezone.utc).isoformat(),
            run_id=str(detector["run_id"]) if detector and detector.get("run_id") else None))
        counts: dict[str, int] = {}
        shadow_counts: dict[str, int] = {}
        package_counts: dict[str, int] = {}
        for item in result.results:
            counts[item.status] = counts.get(item.status, 0) + 1
            for reason in getattr(item, "reasons", ()):
                if reason.startswith("STOCK_BEHAVIOR_SHADOW_"):
                    shadow_status = reason.removeprefix("STOCK_BEHAVIOR_SHADOW_")
                    shadow_counts[shadow_status] = shadow_counts.get(shadow_status, 0) + 1
                if reason.startswith("OPTION_PACKAGE_ASSESSMENT_"):
                    package_status = reason.removeprefix(
                        "OPTION_PACKAGE_ASSESSMENT_"
                    )
                    package_counts[package_status] = (
                        package_counts.get(package_status, 0) + 1
                    )
        if any(getattr(item, "retryable", False) for item in result.results):
            self._retry_slot = slot
            self._retry_not_before = now + FAILURE_RETRY_DELAY
            LOGGER.warning(
                "option slot will retry slot=%s retry_at=%s",
                slot.isoformat(),
                self._retry_not_before.isoformat(),
            )
        else:
            self._completed_slots.add(slot)
            if slot == self._retry_slot:
                self._retry_slot = None
                self._retry_not_before = None
        LOGGER.info(
            "option slot complete slot=%s statuses=%s stock_behavior_shadow=%s "
            "package_assessments=%s",
            slot.isoformat(),
            ",".join(f"{key}:{value}" for key, value in sorted(counts.items())),
            ",".join(
                f"{key}:{value}" for key, value in sorted(shadow_counts.items())
            ) or "DISABLED_OR_NOT_APPLICABLE",
            ",".join(
                f"{key}:{value}" for key, value in sorted(package_counts.items())
            ) or "DISABLED",
        )
        if getattr(result, "detector_evaluation", None) is not None:
            LOGGER.info("option detector evaluation slot=%s result=%s", slot.isoformat(), result.detector_evaluation)
        if self.outcome_service is not None:
            outcome_result = self.outcome_service.mature(available_by=self.clock().astimezone(timezone.utc))
            LOGGER.info(
                "option outcomes candidates=%s due=%s available=%s persisted=%s pending=%s",
                outcome_result.candidates,
                outcome_result.due_measurements,
                outcome_result.available_measurements,
                outcome_result.persisted,
                outcome_result.pending,
            )
        if self.validation_callback is not None:
            try:
                validation = self.validation_callback(self.clock().astimezone(timezone.utc))
                LOGGER.info("option continuous validation=%s", validation)
            except Exception:
                LOGGER.exception(
                    "option continuous validation failed; materialization remains complete"
                )
        return result

    def _detector_status(self, payload):
        if self.detector_status_callback is not None:
            try:
                self.detector_status_callback(payload)
            except Exception:
                LOGGER.exception("detector operational status unavailable; retained results unchanged")

    def run_forever(self, leadership: OptionSchedulerLeadership) -> None:
        while True:
            leadership.heartbeat()
            try:
                self.poll_once(leadership.heartbeat)
            except Exception:
                LOGGER.exception("option worker poll failed")
            leadership.heartbeat()
            self.sleep(self.settings.poll_seconds)