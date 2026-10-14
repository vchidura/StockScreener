from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from options.config import ValuationPolicy

from .acquisition_consumer import (
    DEVELOPER_EQUITY_ACQUISITION_JOB,
    DEVELOPER_OPTION_ACQUISITION_JOB,
    DeveloperOptionPackageMaterialization,
    developer_job_matches_batch,
    enqueue_pending_developer_acquisition_jobs,
    process_developer_acquisition_job,
)
from .acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    developer_option_snapshots,
    load_developer_acquisition_batch_cursor,
)
from .database_access import RealtimeDatabaseRole
from .materialization_jobs import (
    ClaimedMaterializationJob,
    claim_materialization_jobs_cursor,
    fail_materialization_job_cursor,
)
from .option_portal_repository import select_developer_option_packages_cursor
from .worker_database import RealtimeProcessDatabase


class DeveloperAcquisitionWorkerVerdict(str, Enum):
    IDLE = "IDLE"
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class DeveloperAcquisitionWorkerConfig:
    cohort_id: UUID
    lease_owner: UUID
    valuation_policy: ValuationPolicy
    discovery_limit: int = 25
    claim_limit: int = 10
    option_package_limit: int = 100
    lease_seconds: int = 60
    maximum_attempts: int = 3
    retry_delay_seconds: int = 30
    acquisition_batch_ids: tuple[UUID, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.valuation_policy, ValuationPolicy):
            raise TypeError("Developer acquisition valuation policy is invalid")
        if not 1 <= self.discovery_limit <= 100:
            raise ValueError("Developer acquisition discovery limit must be in [1, 100]")
        if not 1 <= self.claim_limit <= 100:
            raise ValueError("Developer acquisition claim limit must be in [1, 100]")
        if not 1 <= self.option_package_limit <= 100:
            raise ValueError("Developer option package limit must be in [1, 100]")
        if self.lease_seconds <= 0 or self.maximum_attempts <= 0:
            raise ValueError("Developer acquisition lease and attempts must be positive")
        if self.retry_delay_seconds < 0:
            raise ValueError("Developer acquisition retry delay must be nonnegative")
        if (
            type(self.acquisition_batch_ids) is not tuple
            or len(self.acquisition_batch_ids) > 100
            or len(self.acquisition_batch_ids) != len(set(self.acquisition_batch_ids))
            or any(not isinstance(value, UUID) for value in self.acquisition_batch_ids)
        ):
            raise ValueError("Developer acquisition batch filter is invalid")


@dataclass(frozen=True, slots=True)
class DeveloperAcquisitionWorkerStatus:
    checked_at: datetime
    discovered_equity: int
    discovered_option: int
    claimed: int
    completed: int
    retried: int
    failed: int
    blocked: int
    output_count: int
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))
        counters = (
            self.discovered_equity,
            self.discovered_option,
            self.claimed,
            self.completed,
            self.retried,
            self.failed,
            self.blocked,
            self.output_count,
        )
        if any(type(value) is not int or value < 0 for value in counters):
            raise ValueError("Developer acquisition worker counters must be nonnegative")
        if self.claimed != self.completed + self.retried + self.failed + self.blocked:
            raise ValueError("Developer acquisition worker terminal counters are unbalanced")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("Developer acquisition worker reasons must be unique")

    @property
    def verdict(self) -> DeveloperAcquisitionWorkerVerdict:
        if self.retried or self.failed or self.blocked:
            return DeveloperAcquisitionWorkerVerdict.DEGRADED
        if self.claimed or self.discovered_equity or self.discovered_option:
            return DeveloperAcquisitionWorkerVerdict.COMPLETE
        return DeveloperAcquisitionWorkerVerdict.IDLE


def _require_compute(database: RealtimeProcessDatabase) -> None:
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("Developer acquisition worker database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Developer acquisition worker requires the compute database role")


def select_developer_option_materializations(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
    *,
    policy: ValuationPolicy,
    checked_at: datetime,
    limit: int,
) -> tuple[DeveloperOptionPackageMaterialization, ...]:
    _require_compute(database)
    cutoff = _utc(checked_at, "checked_at")
    with database.transaction() as cursor:
        batch = load_developer_acquisition_batch_cursor(
            cursor,
            claimed.job.source_revision_id,
        )
        if batch.fact_kind is not DeveloperAcquisitionFactKind.OPTION_SNAPSHOT:
            raise ValueError("Developer option selection requires an option acquisition")
        if not developer_job_matches_batch(claimed.job, batch):
            raise ValueError("Developer option selection job identity does not match")
        snapshots = developer_option_snapshots(batch)
        source_cutoff = max(snapshot.market_data_time for snapshot in snapshots.values())
        packages = select_developer_option_packages_cursor(
            cursor,
            cohort_id=batch.cohort_id,
            valuation_policy_sha256=policy.policy_sha256,
            source_batch_id=batch.provider_batch_id,
            instrument_ids=tuple(snapshots),
            source_cutoff=source_cutoff,
            limit=limit,
        )
    return tuple(
        DeveloperOptionPackageMaterialization(package, policy, cutoff)
        for package in packages
    )


def _claim_jobs(
    database: RealtimeProcessDatabase,
    config: DeveloperAcquisitionWorkerConfig,
) -> tuple[ClaimedMaterializationJob, ...]:
    with database.transaction() as cursor:
        equity = claim_materialization_jobs_cursor(
            cursor,
            job_kind=DEVELOPER_EQUITY_ACQUISITION_JOB,
            lease_owner=config.lease_owner,
            lease_seconds=config.lease_seconds,
            maximum_attempts=config.maximum_attempts,
            limit=config.claim_limit,
            cohort_id=config.cohort_id,
            source_revision_ids=config.acquisition_batch_ids or None,
        )
        option = claim_materialization_jobs_cursor(
            cursor,
            job_kind=DEVELOPER_OPTION_ACQUISITION_JOB,
            lease_owner=config.lease_owner,
            lease_seconds=config.lease_seconds,
            maximum_attempts=config.maximum_attempts,
            limit=config.claim_limit,
            cohort_id=config.cohort_id,
            source_revision_ids=config.acquisition_batch_ids or None,
        )
    return equity + option


def _record_failure(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
    config: DeveloperAcquisitionWorkerConfig,
    error: Exception,
) -> tuple[str, str]:
    retryable = not isinstance(error, (TypeError, ValueError))
    failure_code = (
        "DEVELOPER_ACQUISITION_OUTPUT_FAILED"
        if retryable
        else "DEVELOPER_ACQUISITION_CONTRACT_INVALID"
    )
    with database.transaction() as cursor:
        status = fail_materialization_job_cursor(
            cursor,
            claimed,
            failure_code=failure_code,
            maximum_attempts=config.maximum_attempts,
            retry_delay_seconds=config.retry_delay_seconds,
            retryable=retryable,
        )
    return status, failure_code


def run_developer_acquisition_worker_once(
    database: RealtimeProcessDatabase,
    config: DeveloperAcquisitionWorkerConfig,
    *,
    checked_at: datetime,
) -> DeveloperAcquisitionWorkerStatus:
    _require_compute(database)
    if not isinstance(config, DeveloperAcquisitionWorkerConfig):
        raise TypeError("Developer acquisition worker config is invalid")
    checked = _utc(checked_at, "checked_at")
    discovery_filter = config.acquisition_batch_ids or None
    discovered_equity = enqueue_pending_developer_acquisition_jobs(
        database,
        cohort_id=config.cohort_id,
        fact_kind=DeveloperAcquisitionFactKind.EQUITY_BAR,
        limit=config.discovery_limit,
        acquisition_batch_ids=discovery_filter,
    )
    discovered_option = enqueue_pending_developer_acquisition_jobs(
        database,
        cohort_id=config.cohort_id,
        fact_kind=DeveloperAcquisitionFactKind.OPTION_SNAPSHOT,
        limit=config.discovery_limit,
        acquisition_batch_ids=discovery_filter,
    )
    claimed_jobs = _claim_jobs(database, config)
    completed = retried = failed = blocked = output_count = 0
    reason_codes: list[str] = []
    for claimed in claimed_jobs:
        try:
            option_work = (
                select_developer_option_materializations(
                    database,
                    claimed,
                    policy=config.valuation_policy,
                    checked_at=checked,
                    limit=config.option_package_limit,
                )
                if claimed.job.job_kind == DEVELOPER_OPTION_ACQUISITION_JOB
                else ()
            )
            if (
                claimed.job.job_kind == DEVELOPER_OPTION_ACQUISITION_JOB
                and not option_work
            ):
                raise ValueError(
                    "Developer option acquisition requires materialized package marks"
                )
            outputs = process_developer_acquisition_job(
                database,
                claimed,
                option_materializations=option_work,
            )
            completed += 1
            output_count += len(outputs)
        except Exception as error:
            status, failure_code = _record_failure(
                database,
                claimed,
                config,
                error,
            )
            reason_codes.append(failure_code)
            if status == "PENDING":
                retried += 1
            elif status == "FAILED":
                failed += 1
            elif status == "BLOCKED":
                blocked += 1
            else:
                raise ValueError("Developer acquisition failure returned an invalid status")
    return DeveloperAcquisitionWorkerStatus(
        checked_at=checked,
        discovered_equity=len(discovered_equity),
        discovered_option=len(discovered_option),
        claimed=len(claimed_jobs),
        completed=completed,
        retried=retried,
        failed=failed,
        blocked=blocked,
        output_count=output_count,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )