from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.database_access import RealtimeDatabaseRole
from realtime.equity_worker_service import (
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerStatus,
)
from realtime.stage_database_v2 import V2WorkerDatabase
from scripts import run_realtime_v2_equity_interval_finalizer as script


NOW = datetime(2026, 10, 15, 2, 30, tzinfo=timezone.utc)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
OWNER = UUID("22222222-2222-2222-2222-222222222222")
BASE = [
    "--database",
    "test_realtime_v2_finalizer",
    "--confirm-database-name",
    "test_realtime_v2_finalizer",
    "--allow-disposable",
    "--cohort-id",
    str(COHORT),
    "--lease-owner",
    str(OWNER),
    "--execute",
]


def test_cli_requires_explicit_safe_target_execution_and_mode():
    args = script.parse_args([*BASE, "--once", "--claim-limit", "7"])
    assert args.once is True
    assert args.claim_limit == 7

    with pytest.raises(ValueError, match="disposable"):
        script.parse_args(
            [
                "--database",
                "test_realtime_v2_finalizer",
                "--confirm-database-name",
                "test_realtime_v2_finalizer",
                "--cohort-id",
                str(COHORT),
                "--once",
                "--execute",
            ]
        )
    with pytest.raises(ValueError, match="confirmation"):
        script.parse_args(
            [
                "--database",
                "stocks_realtime_v2",
                "--confirm-database-name",
                "wrong",
                "--cohort-id",
                str(COHORT),
                "--once",
                "--execute",
            ]
        )
    with pytest.raises(SystemExit):
        script.parse_args([*BASE[:-1], "--once"])
    with pytest.raises(SystemExit):
        script.parse_args(BASE)
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--once", "--continuous"])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--once", "--claim-limit", "101"])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--continuous", "--poll-seconds", "0"])
    assert script.parse_args([
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--cohort-id", str(COHORT),
        "--once", "--execute", "--security-mode", "hardened",
    ]).security_mode == "hardened"


def test_once_wires_v2_worker_database_and_runs_one_bounded_claim(monkeypatch, capsys):
    pool = MagicMock()
    provider = MagicMock()
    provider.verify.return_value = "test_realtime_v2_finalizer"
    database = MagicMock(spec=V2WorkerDatabase)
    monkeypatch.setattr(script, "_pool", MagicMock(return_value=pool))
    provider_type = MagicMock(return_value=provider)
    worker_type = MagicMock(return_value=database)
    monkeypatch.setattr(script, "V2ConnectionProvider", provider_type)
    monkeypatch.setattr(script, "V2WorkerDatabase", worker_type)
    status = EquityWorkerStatus(NOW, 3, 2, 1, 0, 0, 2, ("RETRY",))
    run = MagicMock(return_value=status)
    monkeypatch.setattr(script, "run_five_minute_finalizer_once", run)
    monkeypatch.setattr(script, "_now", MagicMock(return_value=NOW))

    assert script.main([*BASE, "--once", "--claim-limit", "3"]) == 0

    provider.verify.assert_called_once_with()
    worker_type.assert_called_once_with(provider, RealtimeDatabaseRole.COMPUTE)
    config = run.call_args.args[1]
    assert config.cohort_id == COHORT
    assert config.job_kind == EQUITY_INTERVAL_FINALIZE_JOB
    assert config.lease_owner == OWNER
    assert config.claim_limit == 3
    assert run.call_args.kwargs["checked_at"] == NOW
    assert '"claimed": 3' in capsys.readouterr().out
    pool.closeall.assert_called_once_with()


def test_continuous_mode_polls_until_stopped_without_unbounded_claims(monkeypatch):
    statuses = (
        EquityWorkerStatus(NOW, 4, 4, 0, 0, 0, 4, ()),
        EquityWorkerStatus(NOW, 0, 0, 0, 0, 0, 0, ()),
    )
    run = MagicMock(side_effect=statuses)
    monkeypatch.setattr(script, "run_five_minute_finalizer_once", run)
    stop = MagicMock()
    stop.is_set.return_value = False
    stop.wait.side_effect = (False, True)
    emit = MagicMock()
    config = script.EquityWorkerConfig(
        cohort_id=COHORT,
        job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
        lease_owner=OWNER,
        claim_limit=4,
    )

    assert script.run_worker(
        MagicMock(),
        config,
        once=False,
        poll_seconds=0.25,
        stop=stop,
        now=lambda: NOW,
        emit=emit,
    ) == 0

    assert run.call_count == 2
    assert all(call.args[1].claim_limit == 4 for call in run.call_args_list)
    assert stop.wait.call_args_list == [((0.25,),), ((0.25,),)]
    assert emit.call_count == 2
