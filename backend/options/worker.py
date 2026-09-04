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
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.calendar = calendar or OptionExchangeCalendar()
        self.settings = settings or OptionWorkerSettings.from_environment()
        self.underlyers = underlyers
        self.outcome_service = outcome_service
        self.partition_maintainer = partition_maintainer
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
        if latest_slot is None:
            return None
        if (
            self._retry_slot is not None
            and self._retry_not_before is not None
            and now < self._retry_not_before
        ):
            return None
        retry_slots = []
        if self._retry_slot is not None:
            retry_slots.append(self._retry_slot)
        durable_retry = self.pipeline.latest_due_retry_cycle()
        if durable_retry is not None:
            retry_slots.append(durable_retry.astimezone(timezone.utc))
        latest_session = self.calendar.session_for_slot(latest_slot)
        current_session_retries = [
            candidate
            for candidate in retry_slots
            if self.calendar.session_for_slot(candidate) == latest_session
        ]
        slot = max(current_session_retries) if current_session_retries else latest_slot
        if slot in self._completed_slots:
            return None
        LOGGER.info("materializing option slot=%s", slot.isoformat())
        result = self.pipeline.run_once(
            self.underlyers,
            as_of=now,
            cycle_time=slot,
            progress_callback=progress_callback,
        )
        if self.outcome_service is not None:
            outcome_result = self.outcome_service.mature(available_by=now)
            LOGGER.info(
                "option outcomes candidates=%s due=%s available=%s persisted=%s pending=%s",
                outcome_result.candidates,
                outcome_result.due_measurements,
                outcome_result.available_measurements,
                outcome_result.persisted,
                outcome_result.pending,
            )
        counts: dict[str, int] = {}
        for item in result.results:
            counts[item.status] = counts.get(item.status, 0) + 1
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
            "option slot complete slot=%s statuses=%s",
            slot.isoformat(),
            ",".join(f"{key}:{value}" for key, value in sorted(counts.items())),
        )
        return result

    def run_forever(self, leadership: OptionSchedulerLeadership) -> None:
        while True:
            leadership.heartbeat()
            try:
                self.poll_once(leadership.heartbeat)
            except Exception:
                LOGGER.exception("option worker poll failed")
            leadership.heartbeat()
            self.sleep(self.settings.poll_seconds)