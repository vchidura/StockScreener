from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID

from .acquisition_handoff import persist_developer_acquisition_batch_cursor
from .acquisition_producer import (
    DeveloperAcquisitionProducer,
    DeveloperAcquisitionProducerResult,
    DeveloperAcquisitionRequest,
)
from .acquisition_worker import (
    DeveloperAcquisitionWorkerConfig,
    DeveloperAcquisitionWorkerStatus,
    DeveloperAcquisitionWorkerVerdict,
    run_developer_acquisition_worker_once,
)
from .preflight import DetectorFamily
from .worker_database import RealtimeProcessDatabase


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Developer cycle checked_at must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class DeveloperCycleResult:
    cohort_id: UUID
    checked_at: datetime
    acquisition: DeveloperAcquisitionProducerResult
    raw_storage: DeveloperAcquisitionWorkerStatus
    unavailable_detectors: tuple[DetectorFamily, ...] = (DetectorFamily.O2,)

    def __post_init__(self) -> None:
        checked = _utc(self.checked_at)
        if self.raw_storage.checked_at != checked:
            raise ValueError("Developer cycle raw-storage clock changed")
        if self.unavailable_detectors != (DetectorFamily.O2,):
            raise ValueError("Developer cycle must keep O2 unavailable")
        batch_count = len(self.acquisition.acquisition_batch_ids)
        discovered = (
            self.raw_storage.discovered_equity
            + self.raw_storage.discovered_option
        )
        if (
            self.raw_storage.verdict is not DeveloperAcquisitionWorkerVerdict.COMPLETE
            or self.raw_storage.reason_codes
            or discovered != batch_count
            or self.raw_storage.claimed != batch_count
            or self.raw_storage.completed != batch_count
        ):
            raise ValueError(
                "Developer cycle did not store the exact acquisition batches"
            )
        minimum_outputs = (
            self.acquisition.equity_fact_count
            + self.raw_storage.discovered_option
        )
        if self.raw_storage.output_count < minimum_outputs:
            raise ValueError("Developer cycle stored incomplete raw outputs")
        object.__setattr__(self, "checked_at", checked)

    @property
    def state(self) -> str:
        return "COMPLETE"


class DeveloperCycleRunner:
    def __init__(
        self,
        producer: DeveloperAcquisitionProducer,
        *,
        acquisition_repository: Callable = persist_developer_acquisition_batch_cursor,
        raw_store: Callable = run_developer_acquisition_worker_once,
    ) -> None:
        if not isinstance(producer, DeveloperAcquisitionProducer):
            raise TypeError("Developer cycle producer is invalid")
        if not callable(acquisition_repository) or not callable(raw_store):
            raise TypeError("Developer cycle dependencies are invalid")
        self.producer = producer
        self.acquisition_repository = acquisition_repository
        self.raw_store = raw_store

    def run_once(
        self,
        ingress_database: RealtimeProcessDatabase,
        compute_database: RealtimeProcessDatabase,
        request: DeveloperAcquisitionRequest,
        worker_config: DeveloperAcquisitionWorkerConfig,
        *,
        checked_at: datetime,
    ) -> DeveloperCycleResult:
        checked = _utc(checked_at)
        if not isinstance(request, DeveloperAcquisitionRequest):
            raise TypeError("Developer cycle request is invalid")
        if not isinstance(worker_config, DeveloperAcquisitionWorkerConfig):
            raise TypeError("Developer cycle worker config is invalid")
        if worker_config.cohort_id != request.cohort_id:
            raise ValueError("Developer cycle cohort identity changed")
        if worker_config.acquisition_batch_ids:
            raise ValueError("Developer cycle owns the exact acquisition batch filter")

        acquisition = self.producer.run_once(
            ingress_database,
            request,
            repository=self.acquisition_repository,
        )
        exact_config = replace(
            worker_config,
            acquisition_batch_ids=acquisition.acquisition_batch_ids,
        )
        raw_storage = self.raw_store(
            compute_database,
            exact_config,
            checked_at=checked,
        )
        if not isinstance(raw_storage, DeveloperAcquisitionWorkerStatus):
            raise TypeError("Developer cycle raw-storage result is invalid")
        return DeveloperCycleResult(
            cohort_id=request.cohort_id,
            checked_at=checked,
            acquisition=acquisition,
            raw_storage=raw_storage,
        )

    @staticmethod
    def run_values(
        compute_database: RealtimeProcessDatabase,
        publication_database: RealtimeProcessDatabase,
        plan,
        **repositories,
    ):
        from .developer_cycle_compute import (
            DeveloperCycleComputePlan,
            persist_developer_cycle_values,
            produce_developer_cycle_values,
        )

        if not isinstance(plan, DeveloperCycleComputePlan):
            raise TypeError("Developer cycle compute plan is invalid")
        values = produce_developer_cycle_values(plan)
        return persist_developer_cycle_values(
            compute_database,
            publication_database,
            plan,
            values,
            **repositories,
        )