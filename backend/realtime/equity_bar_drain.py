"""Equity-only bar materialization: drain developer acquisition batches into rt.bar_* inside a stage.

EQUITY_INGEST (ingress role) only records acquisition batches, facts, the provider ledger and
watermarks. The registry lets only `compute` write rt.bar_catalog / rt.bar_revisions / rt.bar_current,
and the stage catalog is immutable, so the EQUITY_EVIDENCE runner calls this as its bounded first phase.

It reuses the existing machinery and adds no SQL: discover batches that have no job
(enqueue_pending_developer_acquisition_jobs), claim DEVELOPER_EQUITY_ACQUISITION jobs, run
process_developer_acquisition_job (which writes the bars through persist_realtime_equity_bars_cursor and
completes the job in one transaction) and record a failed job as acquisition_worker does. It never
touches option batches or a valuation policy (acquisition_worker.run_developer_acquisition_worker_once
does, so it is not used).

Per-cycle bound: at most `max_jobs` jobs are claimed per call (MAX_BAR_JOBS_PER_CYCLE = 100, about eleven
windows of backlog for nine tickers, each job one acquisition batch of at most 1000 bars). Jobs are
claimed one at a time and `should_stop` is polled before every claim, discovery and job, so a stopped
stage leaves no job leased but unprocessed; unclaimed work simply waits for the next cycle. A retryable
failure leaves the job PENDING for a later cycle (RETRY_DELAY_SECONDS) until MAXIMUM_ATTEMPTS.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from uuid import NAMESPACE_URL, UUID, uuid5

from .acquisition_consumer import (
    DEVELOPER_EQUITY_ACQUISITION_JOB,
    enqueue_pending_developer_acquisition_jobs,
    process_developer_acquisition_job,
)
from .acquisition_handoff import DeveloperAcquisitionFactKind
from .database_access import RealtimeDatabaseRole
from .database_access_v2 import WriteFenceRejected
from .materialization_jobs import claim_materialization_jobs_cursor, fail_materialization_job_cursor
from .stage_orchestrator import StageCancelled
from .worker_database import RealtimeProcessDatabase

logger = logging.getLogger(__name__)

EQUITY_BAR_MATERIALIZATION_RETRY = "EQUITY_BAR_MATERIALIZATION_RETRY"
EQUITY_BAR_MATERIALIZATION_CAPPED = "EQUITY_BAR_MATERIALIZATION_CAPPED"
EQUITY_BAR_MATERIALIZATION_FAILED = "EQUITY_BAR_MATERIALIZATION_FAILED"

MAX_BAR_JOBS_PER_CYCLE = 100
DISCOVERY_LIMIT = 25
CLAIM_LIMIT = 1
LEASE_SECONDS = 120
MAXIMUM_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 30
OUTPUT_FAILED = "DEVELOPER_ACQUISITION_OUTPUT_FAILED"
CONTRACT_INVALID = "DEVELOPER_ACQUISITION_CONTRACT_INVALID"


@dataclass(frozen=True, slots=True)
class EquityBarDrainResult:
    discovered: int = 0
    claimed: int = 0
    completed: int = 0
    retried: int = 0
    failed: int = 0
    blocked: int = 0
    bars: int = 0
    capped: bool = False  # the per-cycle bound was reached, so more work may remain for the next cycle
    max_jobs: int = MAX_BAR_JOBS_PER_CYCLE

    @property
    def unsuccessful(self) -> int:
        """Jobs that will not be retried (exhausted or contract-invalid): the stage fails."""
        return self.failed + self.blocked

    def degraded_reasons(self) -> tuple[str, ...]:
        reasons = []
        if self.retried:
            reasons.append(f"{EQUITY_BAR_MATERIALIZATION_RETRY}:{self.retried}")
        if self.capped:
            reasons.append(f"{EQUITY_BAR_MATERIALIZATION_CAPPED}:{self.max_jobs}")
        return tuple(reasons)


def lease_owner_for(stage_run_id: UUID) -> UUID:
    return uuid5(NAMESPACE_URL, f"equity-bar-materialization:{stage_run_id}")


def _checkpoint(should_stop: Callable[[], bool]) -> None:
    if should_stop():
        raise StageCancelled("equity bar materialization was abandoned")


def _record_failure(database: RealtimeProcessDatabase, claimed, error: Exception, *, maximum_attempts: int,
                    retry_delay_seconds: int) -> str:
    """Same classification as acquisition_worker._record_failure. Returns the job status: PENDING
    (retry later), FAILED (attempts exhausted) or BLOCKED (contract-invalid, never retried)."""
    retryable = not isinstance(error, (TypeError, ValueError))
    try:
        with database.transaction() as cursor:
            return fail_materialization_job_cursor(
                cursor, claimed, failure_code=OUTPUT_FAILED if retryable else CONTRACT_INVALID,
                maximum_attempts=maximum_attempts, retry_delay_seconds=retry_delay_seconds, retryable=retryable)
    except ValueError:
        # The lease was lost (stale, expired or taken over): the job is reclaimed by lease expiry.
        return "PENDING"


def drain_equity_bar_jobs(
    database: RealtimeProcessDatabase,
    *,
    cohort_id: UUID,
    lease_owner: UUID,
    should_stop: Callable[[], bool] = lambda: False,
    max_jobs: int = MAX_BAR_JOBS_PER_CYCLE,
    discovery_limit: int = DISCOVERY_LIMIT,
    claim_limit: int = CLAIM_LIMIT,
    lease_seconds: int = LEASE_SECONDS,
    maximum_attempts: int = MAXIMUM_ATTEMPTS,
    retry_delay_seconds: int = RETRY_DELAY_SECONDS,
    acquisition_batch_ids: tuple[UUID, ...] | None = None,
) -> EquityBarDrainResult:
    """Materialize pending equity acquisition batches of one cohort into rt.bar_*, under the compute role.

    An empty queue is a normal outcome (nothing was ingested since the last cycle). StageCancelled and
    WriteFenceRejected propagate: the stage was abandoned, which is not a job failure."""
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("equity bar materialization database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("equity bar materialization requires the compute database role")
    if max_jobs < 1 or not 1 <= discovery_limit <= 100 or not 1 <= claim_limit <= 100:
        raise ValueError("equity bar materialization bounds are invalid")
    if acquisition_batch_ids is not None and (
        type(acquisition_batch_ids) is not tuple
        or not acquisition_batch_ids or len(acquisition_batch_ids) > 100
        or len(set(acquisition_batch_ids)) != len(acquisition_batch_ids)
        or any(not isinstance(value, UUID) for value in acquisition_batch_ids)
    ):
        raise ValueError("equity bar materialization batch scope is invalid")
    claim_scope = (
        {"source_revision_ids": acquisition_batch_ids}
        if acquisition_batch_ids is not None else {}
    )
    discovery_scope = (
        {"acquisition_batch_ids": acquisition_batch_ids}
        if acquisition_batch_ids is not None else {}
    )
    discovered = claimed_total = completed = retried = failed = blocked = bars = 0
    # Every pass claims a job or discovers new ones, and discovery finds nothing twice, so this only
    # guards against a defect in a stubbed dependency.
    for _ in range(2 * max_jobs + 2):
        if claimed_total >= max_jobs:
            break
        _checkpoint(should_stop)
        with database.transaction() as cursor:
            claimed = claim_materialization_jobs_cursor(
                cursor, job_kind=DEVELOPER_EQUITY_ACQUISITION_JOB, lease_owner=lease_owner,
                lease_seconds=lease_seconds, maximum_attempts=maximum_attempts,
                limit=min(claim_limit, max_jobs - claimed_total), cohort_id=cohort_id,
                **claim_scope)
        if not claimed:
            _checkpoint(should_stop)
            queued = enqueue_pending_developer_acquisition_jobs(
                database, cohort_id=cohort_id, fact_kind=DeveloperAcquisitionFactKind.EQUITY_BAR,
                limit=min(discovery_limit, max_jobs - claimed_total), **discovery_scope)
            discovered += len(queued)
            if not queued:
                break
            continue
        claimed_total += len(claimed)
        for job in claimed:
            _checkpoint(should_stop)
            try:
                outputs = process_developer_acquisition_job(database, job)
            except (StageCancelled, WriteFenceRejected):
                raise
            except Exception as error:  # noqa: BLE001 - classified and recorded against the job
                logger.warning("equity bar job failed: %s", type(error).__name__)
                status = _record_failure(database, job, error, maximum_attempts=maximum_attempts,
                                         retry_delay_seconds=retry_delay_seconds)
                if status == "PENDING":
                    retried += 1
                elif status == "FAILED":
                    failed += 1
                elif status == "BLOCKED":
                    blocked += 1
                else:
                    raise ValueError("equity bar job failure returned an invalid status") from error
                continue
            completed += 1
            bars += len(outputs)
    return EquityBarDrainResult(discovered, claimed_total, completed, retried, failed, blocked, bars,
                                capped=claimed_total >= max_jobs, max_jobs=max_jobs)
