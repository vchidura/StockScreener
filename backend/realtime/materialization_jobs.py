from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_values

from .contracts import canonical_sha256


# Completed jobs are pruned after this many days; discovery must not look further back.
MATERIALIZATION_JOB_RETENTION_DAYS = 7
BARRIER_MEMBER_PAGE_SIZE = 500
_JOB_INTERVALS = frozenset({"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"})


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _failure_code(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized or len(normalized) > 64:
        raise ValueError("materialization failure code must contain 1-64 characters")
    return normalized


@dataclass(frozen=True, slots=True)
class MaterializationJob:
    job_id: UUID
    closure_sha256: str
    job_kind: str
    instrument_id: int
    cohort_id: UUID
    source_revision_id: UUID | None
    bundle_sha256: str | None
    available_at: datetime
    interval: str | None = None
    primary_window_id: UUID | None = None
    confirmation_window_ids: tuple[UUID, ...] = ()
    policy_sha256: str | None = None
    build_id: str | None = None

    def __post_init__(self) -> None:
        kind = self.job_kind.strip().upper()
        if not kind:
            raise ValueError("materialization job kind cannot be blank")
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("materialization job instrument ID must be positive")
        if self.source_revision_id is None and self.primary_window_id is None:
            raise ValueError("materialization job needs a source revision or a primary window")
        if self.interval is not None and self.interval not in _JOB_INTERVALS:
            raise ValueError("materialization job interval is unsupported")
        confirmations = tuple(self.confirmation_window_ids)
        if len(confirmations) != len(set(confirmations)) or any(
            not isinstance(value, UUID) for value in confirmations
        ):
            raise ValueError("materialization job confirmation windows must be unique UUIDs")
        if confirmations and self.primary_window_id is None:
            raise ValueError("confirmation windows require a primary window")
        build_id = self.build_id.strip() if self.build_id is not None else None
        if build_id is not None and not 1 <= len(build_id) <= 96:
            raise ValueError("materialization job build ID must contain 1-96 characters")
        object.__setattr__(self, "job_kind", kind)
        object.__setattr__(
            self, "closure_sha256", _sha256(self.closure_sha256, "closure_sha256")
        )
        object.__setattr__(self, "confirmation_window_ids", confirmations)
        object.__setattr__(self, "build_id", build_id)
        if self.bundle_sha256 is not None:
            object.__setattr__(
                self,
                "bundle_sha256",
                _sha256(self.bundle_sha256, "bundle_sha256"),
            )
        if self.policy_sha256 is not None:
            object.__setattr__(
                self,
                "policy_sha256",
                _sha256(self.policy_sha256, "policy_sha256"),
            )
        object.__setattr__(self, "available_at", _utc(self.available_at, "available_at"))


def materialization_closure_sha256(
    *,
    job_kind: str,
    cohort_id: UUID,
    instrument_id: int,
    interval: str | None,
    primary_window_id: UUID | None,
    confirmation_window_ids: Sequence[UUID],
    source_revision_id: UUID | None = None,
    bundle_sha256: str | None = None,
    policy_sha256: str | None,
    build_id: str | None,
) -> str:
    return canonical_sha256({
        "job_kind": job_kind.strip().upper(),
        "cohort_id": cohort_id,
        "instrument_id": instrument_id,
        "interval": interval,
        "primary_window_id": primary_window_id,
        "confirmation_window_ids": tuple(confirmation_window_ids),
        "source_revision_id": source_revision_id,
        "bundle_sha256": bundle_sha256,
        "policy_sha256": policy_sha256,
        "build_id": build_id,
    })


def build_closure_materialization_job(
    *,
    job_kind: str,
    cohort_id: UUID,
    instrument_id: int,
    interval: str,
    primary_window_id: UUID,
    confirmation_window_ids: Sequence[UUID] = (),
    policy_sha256: str,
    build_id: str,
    available_at: datetime,
    bundle_sha256: str | None = None,
) -> MaterializationJob:
    closure = materialization_closure_sha256(
        job_kind=job_kind,
        cohort_id=cohort_id,
        instrument_id=instrument_id,
        interval=interval,
        primary_window_id=primary_window_id,
        confirmation_window_ids=confirmation_window_ids,
        bundle_sha256=bundle_sha256,
        policy_sha256=policy_sha256,
        build_id=build_id.strip(),
    )
    kind = job_kind.strip().upper()
    return MaterializationJob(
        job_id=uuid5(NAMESPACE_URL, f"materialization-job:{kind}:{closure}"),
        closure_sha256=closure,
        job_kind=kind,
        instrument_id=instrument_id,
        cohort_id=cohort_id,
        source_revision_id=None,
        bundle_sha256=bundle_sha256,
        available_at=available_at,
        interval=interval,
        primary_window_id=primary_window_id,
        confirmation_window_ids=tuple(confirmation_window_ids),
        policy_sha256=policy_sha256,
        build_id=build_id,
    )


@dataclass(frozen=True, slots=True)
class ClaimedMaterializationJob:
    job: MaterializationJob
    lease_owner: UUID
    lease_epoch: int
    lease_until: datetime
    attempts: int

    def __post_init__(self) -> None:
        if self.lease_epoch <= 0 or self.attempts <= 0:
            raise ValueError("claimed materialization job epochs and attempts must be positive")
        object.__setattr__(self, "lease_until", _utc(self.lease_until, "lease_until"))


def _job_values(job: MaterializationJob) -> tuple[object, ...]:
    return (
        str(job.job_id),
        job.closure_sha256,
        job.job_kind,
        str(job.cohort_id),
        job.instrument_id,
        job.interval,
        str(job.source_revision_id) if job.source_revision_id is not None else None,
        str(job.primary_window_id) if job.primary_window_id is not None else None,
        [str(value) for value in job.confirmation_window_ids],
        job.bundle_sha256,
        job.policy_sha256,
        job.build_id,
    )


def _identity(row) -> tuple[object, ...]:
    return tuple(
        str(value) if isinstance(value, UUID)
        else [str(item) if isinstance(item, UUID) else item for item in value]
        if isinstance(value, (list, tuple)) else value
        for value in row
    )


def enqueue_materialization_job_cursor(cursor, job: MaterializationJob) -> UUID:
    values = _job_values(job)
    cursor.execute(
        "INSERT INTO rt.materialization_jobs ("
        "job_id, closure_sha256, job_kind, cohort_id, instrument_id, interval, "
        "source_revision_id, primary_window_id, confirmation_window_ids, "
        "bundle_sha256, policy_sha256, build_id, status, available_at"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s, "
        "'PENDING', %s) ON CONFLICT DO NOTHING",
        (*values, job.available_at),
    )
    cursor.execute(
        "SELECT job_id, closure_sha256, job_kind, cohort_id, instrument_id, interval, "
        "source_revision_id, primary_window_id, confirmation_window_ids, "
        "bundle_sha256, policy_sha256, build_id "
        "FROM rt.materialization_jobs WHERE closure_sha256=%s",
        (job.closure_sha256,),
    )
    row = cursor.fetchone()
    if row is None or _identity(row) != _identity(values):
        raise ValueError("materialization job closure identity conflict")
    return job.job_id


def enqueue_materialization_job(connection, job: MaterializationJob) -> UUID:
    try:
        with connection.cursor() as cursor:
            job_id = enqueue_materialization_job_cursor(cursor, job)
        connection.commit()
        return job_id
    except Exception:
        connection.rollback()
        raise


def _claimed_job(row) -> ClaimedMaterializationJob:
    job = MaterializationJob(
        job_id=UUID(str(row[0])),
        closure_sha256=row[1],
        job_kind=row[2],
        instrument_id=row[3],
        cohort_id=UUID(str(row[4])),
        source_revision_id=UUID(str(row[5])) if row[5] is not None else None,
        bundle_sha256=row[6],
        available_at=row[7],
        interval=row[8],
        primary_window_id=UUID(str(row[9])) if row[9] is not None else None,
        confirmation_window_ids=tuple(UUID(str(value)) for value in row[10] or ()),
        policy_sha256=row[11],
        build_id=row[12],
    )
    return ClaimedMaterializationJob(
        job=job,
        lease_owner=UUID(str(row[13])),
        lease_epoch=row[14],
        lease_until=row[15],
        attempts=row[16],
    )


def claim_materialization_jobs_cursor(
    cursor,
    *,
    job_kind: str,
    lease_owner: UUID,
    lease_seconds: int,
    maximum_attempts: int,
    limit: int,
    cohort_id: UUID | None = None,
    source_revision_ids: tuple[UUID, ...] | None = None,
) -> tuple[ClaimedMaterializationJob, ...]:
    kind = job_kind.strip().upper()
    if not kind:
        raise ValueError("materialization job kind cannot be blank")
    if lease_seconds <= 0 or maximum_attempts <= 0:
        raise ValueError("materialization lease and attempt limits must be positive")
    if not 1 <= limit <= 100:
        raise ValueError("materialization claim limit must be in [1, 100]")
    if cohort_id is not None and not isinstance(cohort_id, UUID):
        raise TypeError("materialization claim cohort ID is invalid")
    if source_revision_ids is not None and (
        type(source_revision_ids) is not tuple
        or not source_revision_ids
        or len(source_revision_ids) > 100
        or len(source_revision_ids) != len(set(source_revision_ids))
        or any(not isinstance(value, UUID) for value in source_revision_ids)
    ):
        raise ValueError("materialization claim source revisions are invalid")
    cohort = str(cohort_id) if cohort_id is not None else None
    revisions = (
        [str(value) for value in source_revision_ids]
        if source_revision_ids is not None
        else None
    )
    cursor.execute(
        "UPDATE rt.materialization_jobs SET status='FAILED', lease_owner=NULL, "
        "lease_until=NULL, failure_code='MAXIMUM_ATTEMPTS_EXHAUSTED', "
        "updated_at=clock_timestamp() WHERE job_kind=%s AND status='LEASED' "
        "AND (%s::uuid IS NULL OR cohort_id=%s) "
        "AND (%s::uuid[] IS NULL OR source_revision_id=ANY(%s::uuid[])) "
        "AND attempts >= %s AND lease_until <= clock_timestamp()",
        (kind, cohort, cohort, revisions, revisions, maximum_attempts),
    )
    cursor.execute(
        "WITH candidates AS ("
        "SELECT job_id FROM rt.materialization_jobs "
        "WHERE job_kind=%s AND (%s::uuid IS NULL OR cohort_id=%s) "
        "AND (%s::uuid[] IS NULL OR source_revision_id=ANY(%s::uuid[])) "
        "AND attempts < %s AND ("
        "(status='PENDING' AND available_at <= clock_timestamp()) OR "
        "(status='LEASED' AND lease_until <= clock_timestamp())) "
        "ORDER BY available_at, job_id FOR UPDATE SKIP LOCKED LIMIT %s"
        ") UPDATE rt.materialization_jobs AS job SET "
        "status='LEASED', lease_owner=%s, lease_epoch=job.lease_epoch + 1, "
        "lease_until=clock_timestamp() + (%s * INTERVAL '1 second'), "
        "attempts=job.attempts + 1, failure_code=NULL, updated_at=clock_timestamp() "
        "FROM candidates WHERE job.job_id=candidates.job_id RETURNING "
        "job.job_id, job.closure_sha256, job.job_kind, job.instrument_id, "
        "job.cohort_id, job.source_revision_id, job.bundle_sha256, job.available_at, "
        "job.interval, job.primary_window_id, job.confirmation_window_ids, "
        "job.policy_sha256, job.build_id, "
        "job.lease_owner, job.lease_epoch, job.lease_until, job.attempts",
        (
            kind,
            cohort,
            cohort,
            revisions,
            revisions,
            maximum_attempts,
            limit,
            str(lease_owner),
            lease_seconds,
        ),
    )
    return tuple(_claimed_job(row) for row in cursor.fetchall())


def claim_materialization_jobs(
    connection,
    *,
    job_kind: str,
    lease_owner: UUID,
    lease_seconds: int,
    maximum_attempts: int,
    limit: int,
    cohort_id: UUID | None = None,
    source_revision_ids: tuple[UUID, ...] | None = None,
) -> tuple[ClaimedMaterializationJob, ...]:
    try:
        with connection.cursor() as cursor:
            claimed = claim_materialization_jobs_cursor(
                cursor,
                job_kind=job_kind,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
                maximum_attempts=maximum_attempts,
                limit=limit,
                cohort_id=cohort_id,
                source_revision_ids=source_revision_ids,
            )
        connection.commit()
        return claimed
    except Exception:
        connection.rollback()
        raise


def complete_materialization_job_cursor(
    cursor,
    claimed: ClaimedMaterializationJob,
    *,
    persist_output: Callable[[object], None],
) -> None:
    cursor.execute(
        "SELECT lease_owner, lease_epoch, lease_until > clock_timestamp() "
        "FROM rt.materialization_jobs WHERE job_id=%s AND status='LEASED' FOR UPDATE",
        (str(claimed.job.job_id),),
    )
    lease = cursor.fetchone()
    if lease is None or str(lease[0]) != str(claimed.lease_owner):
        raise ValueError("materialization job lease owner does not match")
    if lease[1] != claimed.lease_epoch or not lease[2]:
        raise ValueError("materialization job lease is stale or expired")
    persist_output(cursor)
    cursor.execute(
        "UPDATE rt.materialization_jobs SET status='COMPLETE', "
        "lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp() "
        "WHERE job_id=%s AND status='LEASED' AND lease_owner=%s "
        "AND lease_epoch=%s AND lease_until > clock_timestamp()",
        (
            str(claimed.job.job_id),
            str(claimed.lease_owner),
            claimed.lease_epoch,
        ),
    )
    if cursor.rowcount != 1:
        raise ValueError("materialization job completion lost its lease fence")


def complete_materialization_job(
    connection,
    claimed: ClaimedMaterializationJob,
    *,
    persist_output: Callable[[object], None],
) -> None:
    try:
        with connection.cursor() as cursor:
            complete_materialization_job_cursor(
                cursor,
                claimed,
                persist_output=persist_output,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def fail_materialization_job_cursor(
    cursor,
    claimed: ClaimedMaterializationJob,
    *,
    failure_code: str,
    maximum_attempts: int,
    retry_delay_seconds: int = 0,
    retryable: bool = True,
) -> str:
    code = _failure_code(failure_code)
    if type(maximum_attempts) is not int or maximum_attempts <= 0:
        raise ValueError("materialization maximum attempts must be positive")
    if type(retry_delay_seconds) is not int or retry_delay_seconds < 0:
        raise ValueError("materialization retry delay must be nonnegative")
    if type(retryable) is not bool:
        raise TypeError("materialization retryable flag must be boolean")
    cursor.execute(
        "SELECT lease_owner, lease_epoch, lease_until > clock_timestamp() "
        "FROM rt.materialization_jobs WHERE job_id=%s AND status='LEASED' FOR UPDATE",
        (str(claimed.job.job_id),),
    )
    lease = cursor.fetchone()
    if lease is None or str(lease[0]) != str(claimed.lease_owner):
        raise ValueError("materialization job lease owner does not match")
    if lease[1] != claimed.lease_epoch or not lease[2]:
        raise ValueError("materialization job lease is stale or expired")
    status = (
        "BLOCKED"
        if not retryable
        else "FAILED"
        if claimed.attempts >= maximum_attempts
        else "PENDING"
    )
    cursor.execute(
        "UPDATE rt.materialization_jobs SET status=%s, lease_owner=NULL, "
        "lease_until=NULL, failure_code=%s, available_at=CASE WHEN %s='PENDING' "
        "THEN clock_timestamp() + (%s * INTERVAL '1 second') ELSE available_at END, "
        "updated_at=clock_timestamp() WHERE job_id=%s AND status='LEASED' "
        "AND lease_owner=%s AND lease_epoch=%s RETURNING status",
        (
            status,
            code,
            status,
            retry_delay_seconds,
            str(claimed.job.job_id),
            str(claimed.lease_owner),
            claimed.lease_epoch,
        ),
    )
    updated = cursor.fetchone()
    if updated is None or updated[0] != status:
        raise ValueError("materialization job failure transition lost its lease fence")
    return status


def fail_materialization_job(
    connection,
    claimed: ClaimedMaterializationJob,
    *,
    failure_code: str,
    maximum_attempts: int,
    retry_delay_seconds: int = 0,
    retryable: bool = True,
) -> str:
    try:
        with connection.cursor() as cursor:
            status = fail_materialization_job_cursor(
                cursor,
                claimed,
                failure_code=failure_code,
                maximum_attempts=maximum_attempts,
                retry_delay_seconds=retry_delay_seconds,
                retryable=retryable,
            )
        connection.commit()
        return status
    except Exception:
        connection.rollback()
        raise


class BarrierOutcome(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    MISSING = "MISSING"


_JOB_STATUS_BARRIER_OUTCOME = {
    "FAILED": BarrierOutcome.FAILED,
    "BLOCKED": BarrierOutcome.BLOCKED,
    "EXPIRED": BarrierOutcome.MISSING,
    "SUPERSEDED": BarrierOutcome.MISSING,
}


@dataclass(frozen=True, slots=True)
class BarrierMember:
    member_key: str
    expected_input_sha256: str
    job_id: UUID | None = None

    def __post_init__(self) -> None:
        key = self.member_key.strip()
        if not 1 <= len(key) <= 128:
            raise ValueError("job barrier member key must contain 1-128 characters")
        object.__setattr__(self, "member_key", key)
        object.__setattr__(
            self,
            "expected_input_sha256",
            _sha256(self.expected_input_sha256, "expected_input_sha256"),
        )


@dataclass(frozen=True, slots=True)
class JobBarrierVerdict:
    barrier_id: UUID
    state: str
    expected_count: int
    complete_members: tuple[str, ...]
    failed_members: tuple[str, ...]
    open_members: tuple[str, ...]


def open_job_barrier_cursor(
    cursor,
    *,
    barrier_id: UUID,
    cohort_id: UUID,
    window_id: UUID,
    barrier_kind: str,
    universe_sha256: str,
    members: Sequence[BarrierMember],
    values_executor=execute_values,
) -> UUID:
    kind = barrier_kind.strip()
    if not 1 <= len(kind) <= 32:
        raise ValueError("job barrier kind must contain 1-32 characters")
    universe = _sha256(universe_sha256, "universe_sha256")
    rows = tuple(members)
    if not rows or any(not isinstance(row, BarrierMember) for row in rows):
        raise ValueError("job barrier requires BarrierMember rows")
    expected = {row.member_key: (row.expected_input_sha256, row.job_id) for row in rows}
    if len(expected) != len(rows):
        raise ValueError("job barrier member keys must be unique")
    cursor.execute(
        "INSERT INTO rt.job_barriers (barrier_id, cohort_id, window_id, barrier_kind, "
        "universe_sha256, expected_count) VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT DO NOTHING",
        (str(barrier_id), str(cohort_id), str(window_id), kind, universe, len(rows)),
    )
    cursor.execute(
        "SELECT barrier_id, cohort_id, universe_sha256, expected_count "
        "FROM rt.job_barriers WHERE window_id=%s AND barrier_kind=%s",
        (str(window_id), kind),
    )
    stored = cursor.fetchone()
    if stored is None or _identity(stored) != _identity(
        (barrier_id, cohort_id, universe, len(rows))
    ):
        raise ValueError("job barrier identity conflict")
    values_executor(
        cursor,
        "INSERT INTO rt.job_barrier_members (barrier_id, member_key, job_id, "
        "expected_input_sha256) VALUES %s ON CONFLICT DO NOTHING",
        [
            (
                str(barrier_id),
                row.member_key,
                str(row.job_id) if row.job_id is not None else None,
                row.expected_input_sha256,
            )
            for row in rows
        ],
        page_size=BARRIER_MEMBER_PAGE_SIZE,
    )
    cursor.execute(
        "SELECT member_key, expected_input_sha256, job_id "
        "FROM rt.job_barrier_members WHERE barrier_id=%s",
        (str(barrier_id),),
    )
    persisted = {
        str(row[0]): (row[1], UUID(str(row[2])) if row[2] is not None else None)
        for row in cursor.fetchall()
    }
    if persisted != expected:
        raise ValueError("job barrier members conflict with the expected universe")
    return barrier_id


def record_job_barrier_outcome_cursor(
    cursor,
    *,
    barrier_id: UUID,
    member_key: str,
    outcome: BarrierOutcome,
    outcome_input_sha256: str | None = None,
) -> None:
    if not isinstance(outcome, BarrierOutcome):
        raise TypeError("job barrier outcome is invalid")
    digest = (
        _sha256(outcome_input_sha256, "outcome_input_sha256")
        if outcome_input_sha256 is not None
        else None
    )
    if outcome is BarrierOutcome.COMPLETE and digest is None:
        raise ValueError("a COMPLETE barrier outcome requires its input hash")
    cursor.execute(
        "UPDATE rt.job_barrier_members SET outcome=%s, outcome_input_sha256=%s "
        "WHERE barrier_id=%s AND member_key=%s AND outcome IS NULL",
        (outcome.value, digest, str(barrier_id), member_key),
    )
    if cursor.rowcount == 1:
        return
    cursor.execute(
        "SELECT outcome, outcome_input_sha256 FROM rt.job_barrier_members "
        "WHERE barrier_id=%s AND member_key=%s",
        (str(barrier_id), member_key),
    )
    stored = cursor.fetchone()
    if stored is None or tuple(stored) != (outcome.value, digest):
        raise ValueError("job barrier member outcome conflict")


def evaluate_job_barrier_cursor(cursor, barrier_id: UUID) -> JobBarrierVerdict:
    cursor.execute(
        "SELECT state, expected_count FROM rt.job_barriers "
        "WHERE barrier_id=%s FOR UPDATE",
        (str(barrier_id),),
    )
    barrier = cursor.fetchone()
    if barrier is None:
        raise ValueError("job barrier does not exist")
    state, expected_count = barrier[0], barrier[1]
    cursor.execute(
        "SELECT member.member_key, member.expected_input_sha256, member.outcome, "
        "member.outcome_input_sha256, job.status "
        "FROM rt.job_barrier_members AS member "
        "LEFT JOIN rt.materialization_jobs AS job ON job.job_id=member.job_id "
        "WHERE member.barrier_id=%s ORDER BY member.member_key",
        (str(barrier_id),),
    )
    rows = tuple(cursor.fetchall())
    if len(rows) != expected_count:
        raise ValueError("job barrier members do not match its expected universe")
    complete: list[str] = []
    failed: list[str] = []
    pending: list[str] = []
    for key, expected_sha, outcome, outcome_sha, job_status in rows:
        if outcome is None and job_status in _JOB_STATUS_BARRIER_OUTCOME:
            derived = _JOB_STATUS_BARRIER_OUTCOME[job_status]
            record_job_barrier_outcome_cursor(
                cursor, barrier_id=barrier_id, member_key=key, outcome=derived,
            )
            outcome = derived.value
        if outcome is None:
            pending.append(key)
        elif outcome == BarrierOutcome.COMPLETE.value and outcome_sha == expected_sha:
            complete.append(key)
        else:
            failed.append(key)
    if state == "OPEN":
        verdict_state = "FAILED" if failed else "OPEN" if pending else "COMPLETE"
        if verdict_state != "OPEN":
            cursor.execute(
                "UPDATE rt.job_barriers SET state=%s, completed_at=clock_timestamp() "
                "WHERE barrier_id=%s AND state='OPEN'",
                (verdict_state, str(barrier_id)),
            )
    else:
        verdict_state = state
    return JobBarrierVerdict(
        barrier_id=barrier_id,
        state=verdict_state,
        expected_count=expected_count,
        complete_members=tuple(complete),
        failed_members=tuple(failed),
        open_members=tuple(pending),
    )