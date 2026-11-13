"""G102 4.11-4 (Nov 12 night, test only): the equity bar drain recovers from a serialization failure or a deadlock.

The head upsert runs READ COMMITTED (a second writer waits instead of failing; `test_bar_head_v2_db.py` covers both
isolation levels ending on the canonical winner). If a writer still gets 40001 or 40P01 (a SERIALIZABLE caller, or a
deadlock between two batches), the job must be recorded as retryable and come back on a later cycle - never BLOCKED
(contract-invalid) and never stop the drain."""
from __future__ import annotations

from dataclasses import replace

import psycopg2.errors
import pytest

import realtime.equity_bar_drain as drain
from tests.realtime.test_equity_bar_drain import Queue, _Compute, _claimed, _drain


@pytest.mark.parametrize("error", [
    psycopg2.errors.SerializationFailure("could not serialize access due to concurrent update"),
    psycopg2.errors.DeadlockDetected("deadlock detected"),
])
def test_a_serialization_failure_is_retryable_and_the_next_cycle_completes_the_job(monkeypatch, error):
    queue = Queue(monkeypatch, pending=2, failures={0: error})
    first = _drain(maximum_attempts=3, retry_delay_seconds=30)
    assert (first.retried, first.blocked, first.failed, first.completed) == (1, 0, 0, 1)
    claimed, options = queue.failed[0]
    assert options["failure_code"] == drain.OUTPUT_FAILED and options["retryable"] is True
    assert options["maximum_attempts"] == 3 and options["retry_delay_seconds"] == 30
    assert first.degraded_reasons() == ("EQUITY_BAR_MATERIALIZATION_RETRY:1",)

    # The next cycle claims the retried job again (attempt 2) and the write goes through.
    queue.pending = [replace(claimed, attempts=2)]
    second = _drain()
    assert (second.retried, second.blocked, second.failed, second.completed) == (0, 0, 0, 1)
    assert queue.processed[-1].job == claimed.job


def test_serialization_errors_are_not_classified_as_contract_invalid():
    # The drain's classification: only TypeError / ValueError are contract-invalid (BLOCKED, never retried).
    for error in (psycopg2.errors.SerializationFailure("x"), psycopg2.errors.DeadlockDetected("x")):
        assert not isinstance(error, (TypeError, ValueError))
        assert error.pgcode in {"40001", "40P01"} or error.pgcode is None


def test_a_retry_that_runs_out_of_attempts_is_failed_not_blocked(monkeypatch):
    queue = Queue(monkeypatch, failures={0: psycopg2.errors.SerializationFailure("again")}, statuses={0: "FAILED"})
    queue.pending = [_claimed(attempts=3)]
    result = _drain(_Compute(), maximum_attempts=3)
    assert (result.retried, result.failed, result.blocked) == (0, 1, 0)
    assert queue.failed[0][1]["retryable"] is True
