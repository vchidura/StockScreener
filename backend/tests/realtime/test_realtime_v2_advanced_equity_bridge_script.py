from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.advanced_equity_journal_bridge import (
    AdvancedEquityJournalBridgeStatus,
)
from realtime.database_access import RealtimeDatabaseRole
from realtime.stage_database_v2 import V2WorkerDatabase
from scripts import run_realtime_v2_advanced_equity_bridge as script


NOW = datetime(2026, 10, 15, 3, 0, tzinfo=timezone.utc)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")
BASE = [
    "--database",
    "test_realtime_v2_advanced_bridge",
    "--confirm-database-name",
    "test_realtime_v2_advanced_bridge",
    "--allow-disposable",
    "--journal",
    "advanced-equity.sqlite",
    "--cohort-id",
    str(COHORT),
    "--journal-generation",
    str(GENERATION),
    "--stream-session-id",
    str(SESSION),
    "--owner-epoch",
    "4",
    "--normalizer-version",
    "massive_market_event_v1",
    "--shard-id",
    "0",
    "--shard-id",
    "1",
    "--execute",
]


def test_cli_requires_safe_target_mode_and_bounded_batch():
    args = script.parse_args([*BASE, "--once", "--batch-limit", "17"])
    assert args.once is True
    assert args.batch_limit == 17
    assert args.shard_id == [0, 1]

    with pytest.raises(ValueError, match="disposable"):
        script.parse_args(
            [
                *BASE[0:4],
                *BASE[5:-1],
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
        script.parse_args([*BASE, "--once", "--batch-limit", "1001"])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--continuous", "--poll-seconds", "0"])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--once", "--security-mode", "hardened"])


def test_once_wires_raw_reader_and_v2_worker_database(monkeypatch, capsys):
    pool = MagicMock()
    provider = MagicMock()
    provider.verify.return_value = "test_realtime_v2_advanced_bridge"
    database = MagicMock(spec=V2WorkerDatabase)
    source = MagicMock()
    monkeypatch.setattr(script, "_pool", MagicMock(return_value=pool))
    monkeypatch.setattr(
        script, "V2ConnectionProvider", MagicMock(return_value=provider)
    )
    worker_type = MagicMock(return_value=database)
    reader_type = MagicMock(return_value=source)
    monkeypatch.setattr(script, "V2WorkerDatabase", worker_type)
    monkeypatch.setattr(script, "RawJournalReader", reader_type)
    monkeypatch.setattr(script, "_now", MagicMock(return_value=NOW))
    process = MagicMock(
        return_value=AdvancedEquityJournalBridgeStatus(NOW, 0, 9, 9, 2, 2)
    )
    monkeypatch.setattr(script, "process_advanced_equity_journal_once", process)

    assert script.main([*BASE, "--once", "--batch-limit", "9"]) == 0

    worker_type.assert_called_once_with(provider, RealtimeDatabaseRole.COMPUTE)
    reader_type.assert_called_once()
    config = process.call_args.args[2]
    assert config.cohort_id == COHORT
    assert config.journal_generation == GENERATION
    assert config.stream_session_id == SESSION
    assert config.shard_ids == (0, 1)
    assert config.batch_limit == 9
    assert process.call_args.kwargs["checked_at"] == NOW
    assert '"persisted_bars": 2' in capsys.readouterr().out
    pool.closeall.assert_called_once_with()


def test_continuous_mode_polls_bounded_batches_until_stopped(monkeypatch):
    statuses = (
        AdvancedEquityJournalBridgeStatus(NOW, 0, 4, 4, 1, 2),
        AdvancedEquityJournalBridgeStatus(NOW, 4, 4, 0, 0, 0),
    )
    process = MagicMock(side_effect=statuses)
    monkeypatch.setattr(script, "process_advanced_equity_journal_once", process)
    stop = MagicMock()
    stop.is_set.return_value = False
    stop.wait.side_effect = (False, True)
    emit = MagicMock()

    assert script.run_bridge(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        once=False,
        poll_seconds=0.25,
        stop=stop,
        now=lambda: NOW,
        emit=emit,
    ) == 0

    assert process.call_count == 2
    assert stop.wait.call_count == 2
    assert emit.call_count == 2
