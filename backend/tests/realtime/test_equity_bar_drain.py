"""Equity bar drain (offline fakes): bounds, ordering, cancellation and failure classification.

The queue, claim, process and failure helpers are replaced by fakes, so no database or provider is
involved; the DB-backed variant is test_stage_runners_bind_v2_db."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

import realtime.equity_bar_drain as drain
from realtime.acquisition_handoff import DeveloperAcquisitionFactKind
from realtime.database_access import RealtimeDatabaseRole
from realtime.database_access_v2 import WriteFenceRejected
from realtime.materialization_jobs import ClaimedMaterializationJob, MaterializationJob
from realtime.stage_database_v2 import StageProcessPlan
from realtime.stage_orchestrator import StageCancelled
from realtime.worker_database import RealtimeProcessDatabase

NOW = datetime(2026, 10, 13, 15, 0, tzinfo=timezone.utc)
COHORT, OWNER = uuid4(), uuid4()


class _Cursor:
    def __init__(self, rows):
        self.rows, self.statements = list(rows), []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class _Compute(RealtimeProcessDatabase):
    def __init__(self, role=RealtimeDatabaseRole.COMPUTE, rows=()):
        self._process_plan = StageProcessPlan(role)
        self.transactions, self.cursor = 0, _Cursor(rows)

    @contextmanager
    def transaction(self, *, read_only=None, dict_cursor=False, isolation_level=None):
        self.transactions += 1
        yield self.cursor


def _claimed(attempts=1):
    job = MaterializationJob(uuid4(), "a" * 64, drain.DEVELOPER_EQUITY_ACQUISITION_JOB, 1, COHORT, uuid4(), None, NOW)
    return ClaimedMaterializationJob(job, OWNER, 1, NOW + timedelta(minutes=2), attempts)


class Queue:
    """`unqueued` batches become pending jobs on discovery; claim hands out pending jobs in order."""

    def __init__(self, monkeypatch, *, unqueued=0, pending=0, outputs=3, failures=None, statuses=None):
        self.unqueued, self.pending = unqueued, [_claimed() for _ in range(pending)]
        self.outputs, self.failures, self.statuses = outputs, dict(failures or {}), dict(statuses or {})
        self.claims, self.discoveries, self.processed, self.failed = [], [], [], []
        monkeypatch.setattr(drain, "claim_materialization_jobs_cursor", self.claim)
        monkeypatch.setattr(drain, "enqueue_pending_developer_acquisition_jobs", self.discover)
        monkeypatch.setattr(drain, "process_developer_acquisition_job", self.process)
        monkeypatch.setattr(drain, "fail_materialization_job_cursor", self.fail)

    def claim(self, cursor, **options):
        self.claims.append(options)
        taken, self.pending = self.pending[:options["limit"]], self.pending[options["limit"]:]
        return tuple(taken)

    def discover(self, database, **options):
        self.discoveries.append(options)
        count = min(self.unqueued, options["limit"])
        self.unqueued -= count
        self.pending.extend(_claimed() for _ in range(count))
        return tuple(uuid4() for _ in range(count))

    def process(self, database, claimed):
        index = len(self.processed)
        self.processed.append(claimed)
        if index in self.failures:
            raise self.failures[index]
        return tuple(uuid4() for _ in range(self.outputs))

    def fail(self, cursor, claimed, **options):
        self.failed.append((claimed, options))
        status = self.statuses.get(len(self.failed) - 1)
        if isinstance(status, Exception):
            raise status
        return status or ("BLOCKED" if not options["retryable"] else "PENDING")


def _drain(database=None, **options):
    return drain.drain_equity_bar_jobs(database or _Compute(), cohort_id=COHORT, lease_owner=OWNER, **options)


def test_an_empty_queue_is_a_normal_outcome_not_a_failure(monkeypatch):
    queue = Queue(monkeypatch)
    result = _drain()
    assert result == drain.EquityBarDrainResult()
    assert result.unsuccessful == 0 and result.degraded_reasons() == () and not result.capped
    assert len(queue.claims) == 1 and len(queue.discoveries) == 1 and queue.processed == []


def test_pending_batches_are_discovered_then_claimed_one_job_at_a_time_for_the_equity_kind(monkeypatch):
    queue = Queue(monkeypatch, unqueued=3, outputs=4)
    result = _drain(discovery_limit=2, lease_seconds=90, maximum_attempts=2)
    assert (result.discovered, result.claimed, result.completed, result.bars) == (3, 3, 3, 12)
    assert [options["limit"] for options in queue.discoveries] == [2, 2, 2]  # the last one finds nothing
    assert all(options["fact_kind"] is DeveloperAcquisitionFactKind.EQUITY_BAR and options["cohort_id"] == COHORT
               for options in queue.discoveries)
    assert all(options["limit"] == 1 and options["job_kind"] == "DEVELOPER_EQUITY_ACQUISITION"
               and options["cohort_id"] == COHORT and options["lease_owner"] == OWNER
               and options["lease_seconds"] == 90 and options["maximum_attempts"] == 2 for options in queue.claims)
    assert "source_revision_ids" not in queue.claims[0] and len(queue.processed) == 3


def test_retryable_jobs_left_by_earlier_cycles_are_claimed_before_new_batches_are_discovered(monkeypatch):
    queue = Queue(monkeypatch, pending=2, unqueued=1)
    result = _drain()
    assert (result.claimed, result.completed, result.discovered) == (3, 3, 1)
    assert queue.discoveries[0]["limit"] == drain.DISCOVERY_LIMIT and len(queue.processed) == 3


def test_the_per_cycle_bound_caps_claims_and_reports_it_as_a_degraded_reason(monkeypatch):
    queue = Queue(monkeypatch, pending=5)
    result = _drain(max_jobs=3)
    assert (result.claimed, result.completed, result.capped) == (3, 3, True)
    assert len(queue.pending) == 2 and queue.discoveries == []  # the rest waits for the next cycle
    assert result.degraded_reasons() == ("EQUITY_BAR_MATERIALIZATION_CAPPED:3",)
    assert drain.MAX_BAR_JOBS_PER_CYCLE == 100 and drain.CLAIM_LIMIT == 1 and drain.DISCOVERY_LIMIT <= 100
    assert not _drain_with(monkeypatch, pending=2, max_jobs=3).capped


def _drain_with(monkeypatch, **options):
    queue_options = {key: options.pop(key) for key in ("pending", "unqueued") if key in options}
    Queue(monkeypatch, **queue_options)
    return _drain(**options)


def test_discovery_never_asks_for_more_than_the_remaining_bound(monkeypatch):
    queue = Queue(monkeypatch, unqueued=10)
    result = _drain(max_jobs=4, discovery_limit=25)
    assert result.claimed == 4 and result.capped and queue.unqueued == 6
    assert [options["limit"] for options in queue.discoveries] == [4]


def test_a_stop_before_any_work_and_between_jobs_leaves_unclaimed_work_untouched(monkeypatch):
    queue = Queue(monkeypatch, pending=3)
    with pytest.raises(StageCancelled):
        _drain(should_stop=lambda: True)
    assert queue.claims == [] and queue.processed == []

    polls = []

    def stop_after_the_first_job():
        polls.append(len(queue.processed))
        return len(queue.processed) >= 1

    with pytest.raises(StageCancelled):
        _drain(should_stop=stop_after_the_first_job)
    assert len(queue.processed) == 1 and len(queue.pending) == 2  # nothing leased but unprocessed
    assert len(queue.claims) == 1 and queue.failed == []
    assert polls[0] == 0 and polls[-1] == 1


def test_a_stop_during_discovery_is_polled_between_the_claim_and_the_discovery(monkeypatch):
    queue = Queue(monkeypatch, unqueued=2)
    polls = []

    def should_stop():
        polls.append(len(queue.claims))
        return len(queue.claims) >= 1 and not queue.discoveries and len(polls) >= 2

    with pytest.raises(StageCancelled):
        _drain(should_stop=should_stop)
    assert queue.discoveries == [] and queue.processed == []


def test_failures_are_classified_like_the_worker_and_never_stop_the_drain(monkeypatch):
    queue = Queue(monkeypatch, pending=5, failures={
        0: RuntimeError("database unavailable"), 1: ValueError("bar identity conflict"),
        2: TypeError("bad claim"), 3: ConnectionError("reset")},
        statuses={3: "FAILED"})
    result = _drain(maximum_attempts=3, retry_delay_seconds=15)
    # job 0 retryable (PENDING), 1 and 2 contract-invalid (BLOCKED), 3 retryable but exhausted (FAILED), 4 fine.
    assert (result.retried, result.blocked, result.failed, result.completed) == (1, 2, 1, 1)
    codes = [(options["failure_code"], options["retryable"]) for _, options in queue.failed]
    assert codes == [(drain.OUTPUT_FAILED, True), (drain.CONTRACT_INVALID, False), (drain.CONTRACT_INVALID, False),
                     (drain.OUTPUT_FAILED, True)]
    assert all(options["maximum_attempts"] == 3 and options["retry_delay_seconds"] == 15 for _, options in queue.failed)
    assert result.unsuccessful == 3 and result.degraded_reasons() == ("EQUITY_BAR_MATERIALIZATION_RETRY:1",)
    assert len(queue.processed) == 5


def test_a_lost_lease_while_recording_a_failure_counts_as_a_retry_not_a_crash(monkeypatch):
    queue = Queue(monkeypatch, pending=1, failures={0: ValueError("materialization job lease is stale or expired")},
                  statuses={0: ValueError("materialization job lease is stale or expired")})
    result = _drain()
    assert (result.retried, result.blocked, result.failed) == (1, 0, 0) and len(queue.failed) == 1


@pytest.mark.parametrize("error", [StageCancelled("abandoned"), WriteFenceRejected("fenced")])
def test_an_abandoned_stage_is_not_a_job_failure(monkeypatch, error):
    queue = Queue(monkeypatch, pending=2, failures={0: error})
    with pytest.raises(type(error)):
        _drain()
    assert queue.failed == [] and len(queue.processed) == 1


def test_a_stage_abandoned_while_recording_a_failure_propagates(monkeypatch):
    queue = Queue(monkeypatch, pending=2, failures={0: RuntimeError("boom")}, statuses={0: StageCancelled("x")})
    with pytest.raises(StageCancelled):
        _drain()
    assert len(queue.processed) == 1


def test_a_retried_job_is_handed_to_the_consumer_unchanged(monkeypatch):
    """A retry moves available_at (scheduling state); the consumer ignores it when matching the batch, so the
    drain passes the claimed job through untouched and reads nothing extra."""
    queue = Queue(monkeypatch)
    fresh = _claimed()
    retried = _claimed(attempts=2)
    retried = replace(retried, job=replace(retried.job, available_at=NOW + timedelta(seconds=30)))
    queue.pending = [retried, fresh]
    database = _Compute()
    result = _drain(database)
    assert result.completed == 2
    assert queue.processed == [retried, fresh]
    assert database.cursor.statements == []


def test_a_retried_job_that_fails_again_is_recorded_against_the_claimed_job(monkeypatch):
    queue = Queue(monkeypatch, failures={0: RuntimeError("down")})
    retried = _claimed(attempts=2)
    queue.pending = [retried]
    _drain(_Compute())
    assert queue.failed[0][0] is retried  # the lease fence is checked against the job as claimed


def test_the_drain_requires_the_compute_role_a_process_database_and_sane_bounds(monkeypatch):
    Queue(monkeypatch)
    with pytest.raises(ValueError, match="compute"):
        _drain(_Compute(RealtimeDatabaseRole.INGRESS))
    with pytest.raises(TypeError):
        drain.drain_equity_bar_jobs(object(), cohort_id=COHORT, lease_owner=OWNER)
    for bad in (dict(max_jobs=0), dict(discovery_limit=0), dict(discovery_limit=101), dict(claim_limit=0)):
        with pytest.raises(ValueError, match="bounds"):
            _drain(**bad)


def test_the_lease_owner_is_stable_per_stage_run_and_distinct_between_runs():
    run = uuid4()
    assert drain.lease_owner_for(run) == drain.lease_owner_for(run) != drain.lease_owner_for(uuid4())
