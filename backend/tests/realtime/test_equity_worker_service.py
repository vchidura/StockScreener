from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

import realtime.equity_worker_service as workers
from realtime.database_access import RealtimeDatabaseRole
from realtime.materialization_jobs import BarrierOutcome, ClaimedMaterializationJob, MaterializationJob
from realtime.worker_database import RealtimeProcessDatabase


NOW = datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc)


class Database(RealtimeProcessDatabase):
    def __init__(self, role=RealtimeDatabaseRole.COMPUTE):
        self._process_plan = SimpleNamespace(role=role)
        self.cursor = MagicMock()

    def transaction(self, **options):
        return nullcontext(self.cursor)


def claimed(kind=workers.STOCK_ALERT_DETECT_JOB):
    job = MaterializationJob(
        uuid4(), "a" * 64, kind, 7, uuid4(), None, None, NOW,
        interval="30m", primary_window_id=uuid4(), policy_sha256="b" * 64, build_id="build-1",
    )
    return ClaimedMaterializationJob(job, uuid4(), 1, NOW + timedelta(minutes=1), 1)


def config(kind=workers.STOCK_ALERT_DETECT_JOB):
    return workers.EquityWorkerConfig(
        cohort_id=uuid4(), job_kind=kind, lease_owner=uuid4(), claim_limit=4,
    )


def test_worker_claims_prepares_and_atomically_completes_outputs_and_barriers(monkeypatch):
    database = Database()
    work = claimed()
    prepared = workers.PreparedEquityWorkerOutput(
        input_sha256="c" * 64,
        output_count=2,
        persist_output=lambda cursor: cursor.execute("INSERT output"),
    )
    claim = MagicMock(return_value=(work,))
    complete = MagicMock()
    monkeypatch.setattr(workers, "claim_materialization_jobs_cursor", claim)
    monkeypatch.setattr(workers, "complete_materialization_job_with_barriers_cursor", complete)

    status = workers.run_equity_worker_once(
        database, config(), prepare=lambda active, item: prepared, checked_at=NOW,
    )

    assert status == workers.EquityWorkerStatus(NOW, 1, 1, 0, 0, 0, 2, ())
    assert claim.call_args.kwargs["job_kind"] == workers.STOCK_ALERT_DETECT_JOB
    complete.assert_called_once_with(
        database.cursor,
        work,
        outcome_input_sha256="c" * 64,
        persist_output=prepared.persist_output,
    )


def test_worker_passes_noncomplete_barrier_outcome_for_gap_or_late(monkeypatch):
    database = Database()
    work = claimed(workers.EQUITY_INTERVAL_FINALIZE_JOB)
    prepared = workers.PreparedEquityWorkerOutput(
        input_sha256="c" * 64,
        output_count=0,
        persist_output=lambda cursor: cursor.execute("UPDATE watermark"),
        barrier_outcome=BarrierOutcome.MISSING,
    )
    monkeypatch.setattr(
        workers, "claim_materialization_jobs_cursor", MagicMock(return_value=(work,))
    )
    complete = MagicMock()
    monkeypatch.setattr(
        workers, "complete_materialization_job_with_barriers_cursor", complete
    )

    status = workers.run_equity_worker_once(
        database,
        config(workers.EQUITY_INTERVAL_FINALIZE_JOB),
        prepare=lambda active, item: prepared,
        checked_at=NOW,
    )

    assert status.completed == 1
    complete.assert_called_once_with(
        database.cursor,
        work,
        outcome_input_sha256="c" * 64,
        persist_output=prepared.persist_output,
        barrier_outcome=BarrierOutcome.MISSING,
    )


@pytest.mark.parametrize(
    ("error", "transition", "expected"),
    [
        (RuntimeError("retry"), "PENDING", (1, 0, 1, 0, 0)),
        (ValueError("bad contract"), "BLOCKED", (1, 0, 0, 0, 1)),
        (RuntimeError("exhausted"), "FAILED", (1, 0, 0, 1, 0)),
    ],
)
def test_worker_failures_are_explicit_and_balanced(monkeypatch, error, transition, expected):
    database = Database()
    monkeypatch.setattr(workers, "claim_materialization_jobs_cursor", MagicMock(return_value=(claimed(),)))
    failure = MagicMock(return_value=transition)
    monkeypatch.setattr(workers, "fail_materialization_job_cursor", failure)

    status = workers.run_equity_worker_once(
        database, config(), prepare=lambda active, item: (_ for _ in ()).throw(error), checked_at=NOW,
    )

    assert (status.claimed, status.completed, status.retried, status.failed, status.blocked) == expected
    assert status.reason_codes == (
        "EQUITY_WORKER_OUTPUT_FAILED" if isinstance(error, RuntimeError)
        else "EQUITY_WORKER_CONTRACT_INVALID",
    )
    assert failure.call_args.kwargs["retryable"] is isinstance(error, RuntimeError)


def test_worker_rejects_unknown_kinds_and_roles_and_blocks_invalid_prepared_output(monkeypatch):
    with pytest.raises(ValueError, match="unsupported"):
        config("UNKNOWN")
    with pytest.raises(ValueError, match="compute role"):
        workers.run_equity_worker_once(
            Database(RealtimeDatabaseRole.INGRESS), config(),
            prepare=lambda active, item: None, checked_at=NOW,
        )

    monkeypatch.setattr(workers, "claim_materialization_jobs_cursor", MagicMock(return_value=(claimed(),)))
    failure = MagicMock(return_value="BLOCKED")
    monkeypatch.setattr(workers, "fail_materialization_job_cursor", failure)
    status = workers.run_equity_worker_once(
        Database(), config(), prepare=lambda active, item: object(), checked_at=NOW,
    )
    assert status.blocked == 1
    assert status.reason_codes == ("EQUITY_WORKER_CONTRACT_INVALID",)
