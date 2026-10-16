from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from .database_access import RealtimeDatabaseRole
from .materialization_jobs import (
    BarrierOutcome,
    ClaimedMaterializationJob,
    claim_materialization_jobs_cursor,
    complete_materialization_job_with_barriers_cursor,
    fail_materialization_job_cursor,
)
from .worker_database import RealtimeProcessDatabase


EQUITY_INTERVAL_FINALIZE_JOB = "EQUITY_INTERVAL_FINALIZE"
EQUITY_INTERVAL_DERIVE_JOB = "EQUITY_INTERVAL_DERIVE"
STOCK_ALERT_DETECT_JOB = "STOCK_ALERT_DETECT"
EQUITY_PORTAL_PROJECT_JOB = "EQUITY_PORTAL_PROJECT"
EQUITY_WORKER_JOB_KINDS = frozenset({
    EQUITY_INTERVAL_FINALIZE_JOB,
    EQUITY_INTERVAL_DERIVE_JOB,
    STOCK_ALERT_DETECT_JOB,
    EQUITY_PORTAL_PROJECT_JOB,
})


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("worker input identity must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class PreparedEquityWorkerOutput:
    input_sha256: str
    output_count: int
    persist_output: Callable[[object], None]
    barrier_outcome: BarrierOutcome = BarrierOutcome.COMPLETE

    def __post_init__(self) -> None:
        object.__setattr__(self, "input_sha256", _sha256(self.input_sha256))
        if type(self.output_count) is not int or self.output_count < 0:
            raise ValueError("worker output count must be nonnegative")
        if not callable(self.persist_output):
            raise TypeError("worker persist_output must be callable")
        if not isinstance(self.barrier_outcome, BarrierOutcome):
            raise TypeError("worker barrier outcome is invalid")


@dataclass(frozen=True, slots=True)
class EquityWorkerConfig:
    cohort_id: UUID
    job_kind: str
    lease_owner: UUID
    claim_limit: int = 10
    lease_seconds: int = 60
    maximum_attempts: int = 3
    retry_delay_seconds: int = 30

    def __post_init__(self) -> None:
        kind = self.job_kind.strip().upper()
        if kind not in EQUITY_WORKER_JOB_KINDS:
            raise ValueError(f"unsupported equity worker job kind: {kind}")
        if not 1 <= self.claim_limit <= 100:
            raise ValueError("equity worker claim limit must be in [1, 100]")
        if self.lease_seconds <= 0 or self.maximum_attempts <= 0:
            raise ValueError("equity worker lease and attempts must be positive")
        if self.retry_delay_seconds < 0:
            raise ValueError("equity worker retry delay must be nonnegative")
        object.__setattr__(self, "job_kind", kind)


@dataclass(frozen=True, slots=True)
class EquityWorkerStatus:
    checked_at: datetime
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
            self.claimed,
            self.completed,
            self.retried,
            self.failed,
            self.blocked,
            self.output_count,
        )
        if any(type(value) is not int or value < 0 for value in counters):
            raise ValueError("equity worker counters must be nonnegative")
        if self.claimed != self.completed + self.retried + self.failed + self.blocked:
            raise ValueError("equity worker terminal counters are unbalanced")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("equity worker reason codes must be unique")


def _require_compute(database: RealtimeProcessDatabase) -> None:
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("equity worker database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("equity worker requires the compute role")


def _record_failure(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
    config: EquityWorkerConfig,
    error: Exception,
) -> tuple[str, str]:
    retryable = not isinstance(error, (TypeError, ValueError))
    failure_code = (
        "EQUITY_WORKER_OUTPUT_FAILED"
        if retryable
        else "EQUITY_WORKER_CONTRACT_INVALID"
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


def run_equity_worker_once(
    database: RealtimeProcessDatabase,
    config: EquityWorkerConfig,
    *,
    prepare: Callable[
        [RealtimeProcessDatabase, ClaimedMaterializationJob],
        PreparedEquityWorkerOutput,
    ],
    checked_at: datetime,
) -> EquityWorkerStatus:
    _require_compute(database)
    if not isinstance(config, EquityWorkerConfig):
        raise TypeError("equity worker config is invalid")
    if not callable(prepare):
        raise TypeError("equity worker prepare callback is invalid")
    checked = _utc(checked_at, "checked_at")
    with database.transaction() as cursor:
        claimed_jobs = claim_materialization_jobs_cursor(
            cursor,
            job_kind=config.job_kind,
            lease_owner=config.lease_owner,
            lease_seconds=config.lease_seconds,
            maximum_attempts=config.maximum_attempts,
            limit=config.claim_limit,
            cohort_id=config.cohort_id,
        )

    completed = retried = failed = blocked = output_count = 0
    reasons: list[str] = []
    for claimed in claimed_jobs:
        try:
            prepared = prepare(database, claimed)
            if not isinstance(prepared, PreparedEquityWorkerOutput):
                raise TypeError("equity worker prepare must return PreparedEquityWorkerOutput")
            with database.transaction() as cursor:
                completion = {
                    "outcome_input_sha256": prepared.input_sha256,
                    "persist_output": prepared.persist_output,
                }
                if prepared.barrier_outcome is not BarrierOutcome.COMPLETE:
                    completion["barrier_outcome"] = prepared.barrier_outcome
                complete_materialization_job_with_barriers_cursor(
                    cursor, claimed, **completion
                )
            completed += 1
            output_count += prepared.output_count
        except Exception as error:  # noqa: BLE001 - the leased job records every worker failure
            status, failure_code = _record_failure(database, claimed, config, error)
            reasons.append(failure_code)
            if status == "PENDING":
                retried += 1
            elif status == "FAILED":
                failed += 1
            elif status == "BLOCKED":
                blocked += 1
            else:
                raise ValueError("equity worker failure returned an invalid status")
    return EquityWorkerStatus(
        checked,
        len(claimed_jobs),
        completed,
        retried,
        failed,
        blocked,
        output_count,
        tuple(dict.fromkeys(reasons)),
    )
