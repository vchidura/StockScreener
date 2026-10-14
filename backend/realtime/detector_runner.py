from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from .database_access import RealtimeDatabaseRole
from .detector_evidence import (
    RealtimeDetectorCheckpoint,
    RealtimeDetectorEvent,
    RealtimeDetectorState,
    RealtimeDetectorVersion,
)
from .detector_evidence_repository import persist_detector_evaluation_batch_cursor
from .equity_evidence_repository import (
    RealtimeFeatureDefinition,
    persist_realtime_equity_materialization_cursor,
)
from .equity_materialization_adapter import RealtimeEquityMaterializationResult
from .portal_projection import RealtimeAlertProjection
from .worker_database import RealtimeProcessDatabase


@dataclass(frozen=True, slots=True)
class RealtimeFeaturePersistence:
    result: RealtimeEquityMaterializationResult
    definition: RealtimeFeatureDefinition
    generation: int

    def __post_init__(self) -> None:
        if type(self.generation) is not int or self.generation <= 0:
            raise ValueError("feature persistence generation must be positive")


@dataclass(frozen=True, slots=True)
class RealtimeDetectorPersistence:
    lease_resource_key: str
    owner_id: UUID
    checkpoint: RealtimeDetectorCheckpoint
    versions: tuple[RealtimeDetectorVersion, ...]
    states: tuple[RealtimeDetectorState, ...]
    events: tuple[RealtimeDetectorEvent, ...]
    alerts: tuple[RealtimeAlertProjection, ...] = ()

    def __post_init__(self) -> None:
        resource_key = self.lease_resource_key.strip()
        if not resource_key:
            raise ValueError("detector persistence lease resource key cannot be blank")
        object.__setattr__(self, "lease_resource_key", resource_key)


def persist_detector_runner_batch(
    database: RealtimeProcessDatabase,
    *,
    feature_writes: Sequence[RealtimeFeaturePersistence],
    detector_write: RealtimeDetectorPersistence,
) -> tuple[UUID, ...]:
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("detector runner database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("detector runner requires the compute database role")
    writes = tuple(feature_writes)
    if any(not isinstance(row, RealtimeFeaturePersistence) for row in writes):
        raise TypeError("detector runner feature persistence batch is invalid")
    if not isinstance(detector_write, RealtimeDetectorPersistence):
        raise TypeError("detector runner decision persistence is invalid")

    with database.transaction() as cursor:
        snapshot_ids = tuple(
            persist_realtime_equity_materialization_cursor(
                cursor,
                row.result,
                row.definition,
                generation=row.generation,
            )
            for row in writes
        )
        persist_detector_evaluation_batch_cursor(
            cursor,
            lease_resource_key=detector_write.lease_resource_key,
            owner_id=detector_write.owner_id,
            checkpoint=detector_write.checkpoint,
            versions=detector_write.versions,
            states=detector_write.states,
            events=detector_write.events,
            alerts=detector_write.alerts,
        )
    return snapshot_ids