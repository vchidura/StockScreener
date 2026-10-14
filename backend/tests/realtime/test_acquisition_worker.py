from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.acquisition_consumer import (
    DEVELOPER_EQUITY_ACQUISITION_JOB,
    DEVELOPER_OPTION_ACQUISITION_JOB,
    DeveloperOptionPackageMaterialization,
)
from realtime.acquisition_worker import (
    DeveloperAcquisitionWorkerConfig,
    DeveloperAcquisitionWorkerVerdict,
    run_developer_acquisition_worker_once,
    select_developer_option_materializations,
)
from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.preflight import ProviderTier
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_acquisition_handoff import (
    OWNER,
    _bar_batch,
    _claimed,
    _option_batch,
)
from tests.realtime.test_option_portal_adapter import _developer_policy, _package


OUTPUT = UUID("34343434-3434-3434-3434-343434343434")


def _database(*, role=RealtimeDatabaseRole.COMPUTE):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    cursor.rowcount = 1
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    plan = RealtimeProcessDatabasePlan(
        "worker:slow_materializer:developer",
        role,
        RealtimePoolBudget(1, 1),
    )
    database = RealtimeProcessDatabase.wire(
        plan,
        principal_pool=pool,
        role_name=f"stocks_realtime_{role.value.lower()}",
    )
    cursor.reset_mock()
    connection.reset_mock()
    pool.reset_mock()
    cursor.fetchone.side_effect = None
    cursor.rowcount = 1
    connection.closed = False
    return database, connection, cursor


def _config(cohort_id):
    return DeveloperAcquisitionWorkerConfig(
        cohort_id=cohort_id,
        lease_owner=OWNER,
        valuation_policy=_developer_policy(),
        discovery_limit=10,
        claim_limit=5,
        option_package_limit=25,
        lease_seconds=60,
        maximum_attempts=3,
        retry_delay_seconds=15,
    )


def test_option_selection_uses_complete_batch_and_domain_repository(monkeypatch):
    batch = _option_batch()
    claimed = _claimed(batch)
    database, connection, cursor = _database()
    policy = _developer_policy()
    package = replace(
        _package(policy.policy_sha256, ProviderTier.MASSIVE_DEVELOPER),
        cohort_id=batch.cohort_id,
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )
    selection = MagicMock(return_value=(package,))
    monkeypatch.setattr(
        "realtime.acquisition_worker.select_developer_option_packages_cursor",
        selection,
    )

    work = select_developer_option_materializations(
        database,
        claimed,
        policy=policy,
        checked_at=batch.completed_at,
        limit=25,
    )

    assert work == (
        DeveloperOptionPackageMaterialization(package, policy, batch.completed_at),
    )
    kwargs = selection.call_args.kwargs
    assert kwargs["source_batch_id"] == batch.provider_batch_id
    assert set(kwargs["instrument_ids"]) == {
        fact.instrument_id for fact in batch.facts
    }
    assert kwargs["source_cutoff"] == max(
        fact.market_time for fact in batch.facts
    )
    assert connection.commit.call_count == 1


def test_one_pass_discovers_claims_processes_and_returns_complete_status(monkeypatch):
    equity_batch = _bar_batch()
    option_batch = _option_batch()
    equity_claim = _claimed(equity_batch)
    option_claim = _claimed(option_batch)
    database, _, _ = _database()
    config = replace(
        _config(equity_batch.cohort_id),
        acquisition_batch_ids=(
            equity_batch.acquisition_batch_id,
            option_batch.acquisition_batch_id,
        ),
    )

    def discover(
        database_arg,
        *,
        cohort_id,
        fact_kind,
        limit,
        acquisition_batch_ids,
    ):
        assert acquisition_batch_ids == config.acquisition_batch_ids
        return (UUID(int=1),) if fact_kind.value == "EQUITY_BAR" else (UUID(int=2),)

    def claim(cursor, *, job_kind, **kwargs):
        assert kwargs["cohort_id"] == config.cohort_id
        assert kwargs["source_revision_ids"] == config.acquisition_batch_ids
        if job_kind == DEVELOPER_EQUITY_ACQUISITION_JOB:
            return (equity_claim,)
        if job_kind == DEVELOPER_OPTION_ACQUISITION_JOB:
            return (option_claim,)
        raise AssertionError(job_kind)

    monkeypatch.setattr(
        "realtime.acquisition_worker.enqueue_pending_developer_acquisition_jobs",
        discover,
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.claim_materialization_jobs_cursor",
        claim,
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.select_developer_option_materializations",
        MagicMock(return_value=(MagicMock(),)),
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.process_developer_acquisition_job",
        MagicMock(return_value=(OUTPUT,)),
    )

    status = run_developer_acquisition_worker_once(
        database,
        config,
        checked_at=equity_batch.completed_at,
    )

    assert status.verdict is DeveloperAcquisitionWorkerVerdict.COMPLETE
    assert status.discovered_equity == status.discovered_option == 1
    assert status.claimed == status.completed == 2
    assert status.output_count == 2
    assert status.reason_codes == ()


@pytest.mark.parametrize(
    ("error", "transition", "counter", "reason"),
    (
        (RuntimeError("write failed"), "PENDING", "retried", "DEVELOPER_ACQUISITION_OUTPUT_FAILED"),
        (ValueError("bad identity"), "BLOCKED", "blocked", "DEVELOPER_ACQUISITION_CONTRACT_INVALID"),
    ),
)
def test_one_pass_transitions_failures_and_reports_degraded_status(
    monkeypatch,
    error,
    transition,
    counter,
    reason,
):
    batch = _bar_batch()
    claimed = _claimed(batch)
    database, _, _ = _database()
    config = _config(batch.cohort_id)
    monkeypatch.setattr(
        "realtime.acquisition_worker.enqueue_pending_developer_acquisition_jobs",
        MagicMock(return_value=()),
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.claim_materialization_jobs_cursor",
        lambda cursor, *, job_kind, **kwargs: (
            (claimed,) if job_kind == DEVELOPER_EQUITY_ACQUISITION_JOB else ()
        ),
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.process_developer_acquisition_job",
        MagicMock(side_effect=error),
    )
    failure = MagicMock(return_value=transition)
    monkeypatch.setattr(
        "realtime.acquisition_worker.fail_materialization_job_cursor",
        failure,
    )

    status = run_developer_acquisition_worker_once(
        database,
        config,
        checked_at=batch.completed_at,
    )

    assert status.verdict is DeveloperAcquisitionWorkerVerdict.DEGRADED
    assert status.claimed == 1
    assert getattr(status, counter) == 1
    assert status.reason_codes == (reason,)
    assert failure.call_args.kwargs["retryable"] is isinstance(error, RuntimeError)


def test_idle_pass_and_non_compute_role_fail_closed(monkeypatch):
    batch = _bar_batch()
    database, _, _ = _database()
    config = _config(batch.cohort_id)
    monkeypatch.setattr(
        "realtime.acquisition_worker.enqueue_pending_developer_acquisition_jobs",
        MagicMock(return_value=()),
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.claim_materialization_jobs_cursor",
        MagicMock(return_value=()),
    )

    status = run_developer_acquisition_worker_once(
        database,
        config,
        checked_at=batch.completed_at,
    )

    assert status.verdict is DeveloperAcquisitionWorkerVerdict.IDLE
    ingress, connection, _ = _database(role=RealtimeDatabaseRole.INGRESS)
    with pytest.raises(ValueError, match="requires the compute database role"):
        run_developer_acquisition_worker_once(
            ingress,
            config,
            checked_at=batch.completed_at,
        )
    assert connection.cursor.call_count == 0


def test_worker_config_rejects_unbounded_limits():
    batch = _bar_batch()
    with pytest.raises(ValueError, match="claim limit"):
        replace(_config(batch.cohort_id), claim_limit=101)


def test_option_job_without_package_marks_is_blocked(monkeypatch):
    batch = _option_batch()
    claimed = _claimed(batch)
    database, _, _ = _database()
    config = _config(batch.cohort_id)
    monkeypatch.setattr(
        "realtime.acquisition_worker.enqueue_pending_developer_acquisition_jobs",
        MagicMock(return_value=()),
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.claim_materialization_jobs_cursor",
        lambda cursor, *, job_kind, **kwargs: (
            (claimed,) if job_kind == DEVELOPER_OPTION_ACQUISITION_JOB else ()
        ),
    )
    monkeypatch.setattr(
        "realtime.acquisition_worker.select_developer_option_materializations",
        MagicMock(return_value=()),
    )
    failure = MagicMock(return_value="BLOCKED")
    monkeypatch.setattr(
        "realtime.acquisition_worker.fail_materialization_job_cursor",
        failure,
    )

    status = run_developer_acquisition_worker_once(
        database,
        config,
        checked_at=batch.completed_at,
    )

    assert status.verdict is DeveloperAcquisitionWorkerVerdict.DEGRADED
    assert status.blocked == 1
    assert status.output_count == 0
    assert status.reason_codes == ("DEVELOPER_ACQUISITION_CONTRACT_INVALID",)