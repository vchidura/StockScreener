from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import ValuationPolicy

from .acquisition_handoff import (
    DeveloperAcquisitionBatch,
    DeveloperAcquisitionFactKind,
    developer_equity_bar_facts,
    developer_option_snapshots,
    load_developer_acquisition_batch_cursor,
)
from .acquisition_ledger import require_complete_provider_request_cursor
from .contracts import canonical_sha256
from .database_access import RealtimeDatabaseRole
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .materialization_jobs import (
    MATERIALIZATION_JOB_RETENTION_DAYS,
    ClaimedMaterializationJob,
    MaterializationJob,
    complete_materialization_job_cursor,
    enqueue_materialization_job_cursor,
)
from .option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimePackageMarkStatus,
    materialize_developer_option_package_mark,
)
from .option_portal_repository import persist_realtime_option_package_mark_cursor
from .preflight import ProviderTier
from .worker_database import RealtimeProcessDatabase


MAX_ACQUISITION_DISCOVERY_BATCH = 100
MAX_OPTION_PACKAGE_MATERIALIZATIONS = 100
DEVELOPER_EQUITY_ACQUISITION_JOB = "DEVELOPER_EQUITY_ACQUISITION"
DEVELOPER_OPTION_ACQUISITION_JOB = "DEVELOPER_OPTION_ACQUISITION"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _job_kind(fact_kind: DeveloperAcquisitionFactKind) -> str:
    if fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR:
        return DEVELOPER_EQUITY_ACQUISITION_JOB
    return DEVELOPER_OPTION_ACQUISITION_JOB


def build_developer_acquisition_job(
    batch: DeveloperAcquisitionBatch,
) -> MaterializationJob:
    job_kind = _job_kind(batch.fact_kind)
    routing_instrument_id = min(fact.instrument_id for fact in batch.facts)
    closure_sha256 = canonical_sha256({
        "job_kind": job_kind,
        "acquisition_batch_id": batch.acquisition_batch_id,
        "batch_payload_sha256": batch.batch_payload_sha256,
        "cohort_id": batch.cohort_id,
        "provider_tier": batch.provider_tier,
        "fact_schema_version": batch.fact_kind.schema_version,
    })
    return MaterializationJob(
        job_id=uuid5(
            NAMESPACE_URL,
            f"developer-acquisition-job:{job_kind}:{batch.acquisition_batch_id}:"
            f"{closure_sha256}",
        ),
        closure_sha256=closure_sha256,
        job_kind=job_kind,
        instrument_id=routing_instrument_id,
        cohort_id=batch.cohort_id,
        source_revision_id=batch.acquisition_batch_id,
        bundle_sha256=None,
        available_at=batch.completed_at,
    )


def _require_compute(database: RealtimeProcessDatabase) -> None:
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("Developer acquisition consumer database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Developer acquisition consumer requires the compute database role")


def enqueue_pending_developer_acquisition_jobs(
    database: RealtimeProcessDatabase,
    *,
    cohort_id: UUID,
    fact_kind: DeveloperAcquisitionFactKind,
    limit: int,
    acquisition_batch_ids: tuple[UUID, ...] | None = None,
) -> tuple[UUID, ...]:
    _require_compute(database)
    if not isinstance(fact_kind, DeveloperAcquisitionFactKind):
        raise TypeError("Developer acquisition discovery fact kind is invalid")
    if not 1 <= limit <= MAX_ACQUISITION_DISCOVERY_BATCH:
        raise ValueError("Developer acquisition discovery limit must be in [1, 100]")
    if acquisition_batch_ids is not None and (
        type(acquisition_batch_ids) is not tuple
        or not acquisition_batch_ids
        or len(acquisition_batch_ids) > MAX_ACQUISITION_DISCOVERY_BATCH
        or len(acquisition_batch_ids) != len(set(acquisition_batch_ids))
        or any(not isinstance(value, UUID) for value in acquisition_batch_ids)
    ):
        raise ValueError("Developer acquisition discovery batch IDs are invalid")
    job_kind = _job_kind(fact_kind)
    batch_ids = (
        [str(value) for value in acquisition_batch_ids]
        if acquisition_batch_ids is not None
        else None
    )
    with database.transaction() as cursor:
        cursor.execute(
            "SELECT batch.acquisition_batch_id "
            "FROM rt.acquisition_batches AS batch "
            "WHERE batch.cohort_id=%s AND batch.provider_tier='MASSIVE_DEVELOPER' "
            "AND batch.fact_kind=%s "
            "AND (%s::uuid[] IS NULL OR batch.acquisition_batch_id=ANY(%s::uuid[])) "
            "AND (%s::uuid[] IS NOT NULL "
            "OR batch.completed_at >= clock_timestamp() - (%s * INTERVAL '1 day')) "
            "AND NOT EXISTS ("
            "SELECT 1 FROM rt.materialization_jobs AS job "
            "WHERE job.job_kind=%s "
            "AND job.source_revision_id=batch.acquisition_batch_id"
            ") ORDER BY batch.completed_at, batch.acquisition_batch_id LIMIT %s",
            (
                str(cohort_id),
                fact_kind.value,
                batch_ids,
                batch_ids,
                batch_ids,
                MATERIALIZATION_JOB_RETENTION_DAYS,
                job_kind,
                limit,
            ),
        )
        acquisition_batch_ids = tuple(UUID(str(row[0])) for row in cursor.fetchall())
        job_ids = []
        for acquisition_batch_id in acquisition_batch_ids:
            batch = load_developer_acquisition_batch_cursor(
                cursor,
                acquisition_batch_id,
            )
            if batch.cohort_id != cohort_id or batch.fact_kind is not fact_kind:
                raise ValueError("Developer acquisition discovery identity changed")
            job_ids.append(
                enqueue_materialization_job_cursor(
                    cursor,
                    build_developer_acquisition_job(batch),
                )
            )
    return tuple(job_ids)


@dataclass(frozen=True, slots=True)
class DeveloperOptionPackageMaterialization:
    package: RealtimeOptionPackage
    policy: ValuationPolicy
    checked_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.package, RealtimeOptionPackage):
            raise TypeError("Developer option materialization package is invalid")
        if self.package.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
            raise ValueError("Developer option materialization requires a Developer package")
        object.__setattr__(
            self,
            "checked_at",
            _utc(self.checked_at, "checked_at"),
        )


def developer_job_matches_batch(job, batch: DeveloperAcquisitionBatch) -> bool:
    expected = build_developer_acquisition_job(batch)
    # available_at is scheduling state: a retry moves it, so it is not part of the identity.
    return replace(job, available_at=expected.available_at) == expected


def _require_claim_matches_batch(
    claimed: ClaimedMaterializationJob,
    batch: DeveloperAcquisitionBatch,
) -> None:
    if not developer_job_matches_batch(claimed.job, batch):
        raise ValueError("Developer acquisition job identity does not match its batch")


def process_developer_acquisition_job(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
    *,
    option_materializations: Sequence[DeveloperOptionPackageMaterialization] = (),
    bar_repository: Callable | None = None,
    option_repository: Callable | None = None,
) -> tuple[UUID, ...]:
    _require_compute(database)
    if not isinstance(claimed, ClaimedMaterializationJob):
        raise TypeError("Developer acquisition claimed job is invalid")
    option_work = tuple(option_materializations)
    if len(option_work) > MAX_OPTION_PACKAGE_MATERIALIZATIONS:
        raise ValueError("Developer option materialization batch exceeds 100 packages")
    if any(not isinstance(row, DeveloperOptionPackageMaterialization) for row in option_work):
        raise TypeError("Developer option materialization work is invalid")
    persist_bars = bar_repository or persist_realtime_equity_bars_cursor
    persist_option_mark = option_repository or persist_realtime_option_package_mark_cursor
    output_ids: list[UUID] = []

    with database.transaction() as cursor:
        batch = load_developer_acquisition_batch_cursor(
            cursor,
            claimed.job.source_revision_id,
        )
        _require_claim_matches_batch(claimed, batch)
        if batch.request_id is not None:
            require_complete_provider_request_cursor(cursor, batch.request_id)

        def persist_output(active_cursor) -> None:
            if batch.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR:
                if option_work:
                    raise ValueError("equity acquisition cannot carry option work")
                facts = developer_equity_bar_facts(batch)
                persist_bars(active_cursor, facts)
                output_ids.extend(fact.revision.bar_revision_id for fact in facts)
                return
            snapshots = developer_option_snapshots(batch)
            for work in option_work:
                if work.package.cohort_id != batch.cohort_id:
                    raise ValueError("option package cohort does not match acquisition batch")
                result = materialize_developer_option_package_mark(
                    work.package,
                    snapshots,
                    checked_at=work.checked_at,
                    policy=work.policy,
                )
                if result.status is not RealtimePackageMarkStatus.READY:
                    reasons = ",".join(result.reason_codes) or "UNKNOWN"
                    raise ValueError(
                        f"Developer option acquisition mark is unavailable: {reasons}"
                    )
                output_ids.append(persist_option_mark(active_cursor, result))

        complete_materialization_job_cursor(
            cursor,
            claimed,
            persist_output=persist_output,
        )
    return tuple(output_ids)