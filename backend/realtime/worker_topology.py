from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from .database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from .preflight import ProviderTier
from .tier_cohort import TierCohortBinding


class WorkerLane(str, Enum):
    HOT = "HOT"
    SLOW = "SLOW"
    CONTROL = "CONTROL"
    IO = "IO"


class RealtimeWorkerKind(str, Enum):
    PREFLIGHT_CONTROLLER = "PREFLIGHT_CONTROLLER"
    REFERENCE_SERVICE = "REFERENCE_SERVICE"
    DEVELOPER_STOCK_ACQUISITION = "DEVELOPER_STOCK_ACQUISITION"
    DEVELOPER_OPTION_ACQUISITION = "DEVELOPER_OPTION_ACQUISITION"
    ADVANCED_STOCK_GATEWAY = "ADVANCED_STOCK_GATEWAY"
    ADVANCED_OPTION_GATEWAY = "ADVANCED_OPTION_GATEWAY"
    RAW_JOURNAL_BRIDGE = "RAW_JOURNAL_BRIDGE"
    DECODER = "DECODER"
    NORMALIZED_DISPATCHER = "NORMALIZED_DISPATCHER"
    FAST_MATERIALIZER = "FAST_MATERIALIZER"
    BAR_FINALIZER = "BAR_FINALIZER"
    SLOW_MATERIALIZER = "SLOW_MATERIALIZER"
    DETECTOR_RUNNER = "DETECTOR_RUNNER"
    ARCHIVE_WORKER = "ARCHIVE_WORKER"
    PORTAL_PUBLISHER = "PORTAL_PUBLISHER"


RUNTIME_WORKER_POOL_BUDGET = RealtimePoolBudget(1, 1)
API_READER_POOL_BUDGET = RealtimePoolBudget(2, 20)

_WORKER_DATABASE_ROLES = {
    RealtimeWorkerKind.PREFLIGHT_CONTROLLER: RealtimeDatabaseRole.CONTROL,
    RealtimeWorkerKind.REFERENCE_SERVICE: RealtimeDatabaseRole.REFERENCES,
    RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION: RealtimeDatabaseRole.INGRESS,
    RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION: RealtimeDatabaseRole.INGRESS,
    RealtimeWorkerKind.ADVANCED_STOCK_GATEWAY: RealtimeDatabaseRole.INGRESS,
    RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY: RealtimeDatabaseRole.INGRESS,
    RealtimeWorkerKind.RAW_JOURNAL_BRIDGE: RealtimeDatabaseRole.INGRESS,
    RealtimeWorkerKind.DECODER: RealtimeDatabaseRole.INGRESS,
    RealtimeWorkerKind.NORMALIZED_DISPATCHER: RealtimeDatabaseRole.COMPUTE,
    RealtimeWorkerKind.FAST_MATERIALIZER: RealtimeDatabaseRole.COMPUTE,
    RealtimeWorkerKind.BAR_FINALIZER: RealtimeDatabaseRole.COMPUTE,
    RealtimeWorkerKind.SLOW_MATERIALIZER: RealtimeDatabaseRole.COMPUTE,
    RealtimeWorkerKind.DETECTOR_RUNNER: RealtimeDatabaseRole.COMPUTE,
    RealtimeWorkerKind.ARCHIVE_WORKER: RealtimeDatabaseRole.MAINTENANCE,
    RealtimeWorkerKind.PORTAL_PUBLISHER: RealtimeDatabaseRole.PUBLICATION,
}


@dataclass(frozen=True, slots=True)
class WorkerAssignment:
    kind: RealtimeWorkerKind
    lane: WorkerLane
    cohort_id: UUID | None
    provider_tier: ProviderTier | None
    publishes: bool = False

    @property
    def identity(self) -> tuple[RealtimeWorkerKind, UUID | None]:
        return self.kind, self.cohort_id

    @property
    def database_role(self) -> RealtimeDatabaseRole:
        return _WORKER_DATABASE_ROLES[self.kind]

    @property
    def database_plan(self) -> RealtimeProcessDatabasePlan:
        cohort = str(self.cohort_id) if self.cohort_id is not None else "global"
        return RealtimeProcessDatabasePlan(
            f"worker:{self.kind.value.lower()}:{cohort}",
            self.database_role,
            RUNTIME_WORKER_POOL_BUDGET,
        )


@dataclass(frozen=True, slots=True)
class TierWorkerPlan:
    active_cohort_id: UUID | None
    warming_cohort_id: UUID | None
    assignments: tuple[WorkerAssignment, ...]

    def __post_init__(self) -> None:
        identities = tuple(row.identity for row in self.assignments)
        if len(identities) != len(set(identities)):
            raise ValueError("tier worker assignments must be unique")
        publishers = tuple(row for row in self.assignments if row.publishes)
        expected_publishers = 1 if self.active_cohort_id is not None else 0
        if len(publishers) != expected_publishers:
            raise ValueError("tier worker plan must have exactly one active publisher")
        if publishers and publishers[0].cohort_id != self.active_cohort_id:
            raise ValueError("portal publisher must belong to the active cohort")
        if any(
            row.kind in {
                RealtimeWorkerKind.ADVANCED_STOCK_GATEWAY,
                RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY,
                RealtimeWorkerKind.RAW_JOURNAL_BRIDGE,
                RealtimeWorkerKind.DECODER,
                RealtimeWorkerKind.NORMALIZED_DISPATCHER,
                RealtimeWorkerKind.FAST_MATERIALIZER,
            }
            and row.provider_tier is not ProviderTier.MASSIVE_ADVANCED
            for row in self.assignments
        ):
            raise ValueError("Advanced hot-path workers require an Advanced cohort")
        if any(
            row.kind in {
                RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION,
                RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION,
            }
            and row.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            for row in self.assignments
        ):
            raise ValueError("Developer acquisition requires a Developer cohort")
        if any(
            row.cohort_id == self.warming_cohort_id and row.publishes
            for row in self.assignments
        ):
            raise ValueError("warming cohort workers cannot publish")

    @property
    def database_plans(self) -> tuple[RealtimeProcessDatabasePlan, ...]:
        return tuple(assignment.database_plan for assignment in self.assignments)


_GLOBAL_ASSIGNMENTS = (
    WorkerAssignment(
        RealtimeWorkerKind.PREFLIGHT_CONTROLLER,
        WorkerLane.CONTROL,
        None,
        None,
    ),
    WorkerAssignment(
        RealtimeWorkerKind.REFERENCE_SERVICE,
        WorkerLane.SLOW,
        None,
        None,
    ),
    WorkerAssignment(
        RealtimeWorkerKind.ARCHIVE_WORKER,
        WorkerLane.IO,
        None,
        None,
    ),
)


def _cohort_workers(binding: TierCohortBinding) -> tuple[WorkerAssignment, ...]:
    common = (
        WorkerAssignment(
            RealtimeWorkerKind.BAR_FINALIZER,
            WorkerLane.SLOW,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.SLOW_MATERIALIZER,
            WorkerLane.SLOW,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.DETECTOR_RUNNER,
            WorkerLane.SLOW,
            binding.cohort_id,
            binding.provider_tier,
        ),
    )
    if binding.provider_tier is ProviderTier.MASSIVE_DEVELOPER:
        return (
            WorkerAssignment(
                RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION,
                WorkerLane.SLOW,
                binding.cohort_id,
                binding.provider_tier,
            ),
            WorkerAssignment(
                RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION,
                WorkerLane.SLOW,
                binding.cohort_id,
                binding.provider_tier,
            ),
            *common,
        )
    return (
        WorkerAssignment(
            RealtimeWorkerKind.ADVANCED_STOCK_GATEWAY,
            WorkerLane.HOT,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY,
            WorkerLane.HOT,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.RAW_JOURNAL_BRIDGE,
            WorkerLane.IO,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.DECODER,
            WorkerLane.HOT,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.NORMALIZED_DISPATCHER,
            WorkerLane.HOT,
            binding.cohort_id,
            binding.provider_tier,
        ),
        WorkerAssignment(
            RealtimeWorkerKind.FAST_MATERIALIZER,
            WorkerLane.HOT,
            binding.cohort_id,
            binding.provider_tier,
        ),
        *common,
    )


def build_tier_worker_plan(
    *,
    active: TierCohortBinding | None,
    warming: TierCohortBinding | None = None,
) -> TierWorkerPlan:
    if active is None and warming is None:
        raise ValueError("tier worker plan requires an active or warming cohort")
    if active is not None and warming is not None:
        if active.cohort_id == warming.cohort_id:
            raise ValueError("active and warming cohorts must be distinct")
        if active.provider_tier is warming.provider_tier:
            raise ValueError("warming plan must target a different provider tier")
    assignments = list(_GLOBAL_ASSIGNMENTS)
    if active is not None:
        assignments.extend(_cohort_workers(active))
        assignments.append(WorkerAssignment(
            RealtimeWorkerKind.PORTAL_PUBLISHER,
            WorkerLane.CONTROL,
            active.cohort_id,
            active.provider_tier,
            publishes=True,
        ))
    if warming is not None:
        assignments.extend(_cohort_workers(warming))
    return TierWorkerPlan(
        active_cohort_id=active.cohort_id if active is not None else None,
        warming_cohort_id=warming.cohort_id if warming is not None else None,
        assignments=tuple(assignments),
    )


def build_api_reader_database_plan(
    process_name: str = "portal-api",
) -> RealtimeProcessDatabasePlan:
    return RealtimeProcessDatabasePlan(
        process_name,
        RealtimeDatabaseRole.READER,
        API_READER_POOL_BUDGET,
    )