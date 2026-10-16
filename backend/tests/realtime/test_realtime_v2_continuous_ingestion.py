import importlib
import json
from datetime import datetime, timedelta, timezone
from contextlib import nullcontext
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from realtime.orchestration_repository import (
    EQUITY_WINDOW_BUILD_ID, equity_window_recovery_cursor,
)
from realtime.session_windows import developer_session_windows

script = importlib.import_module("scripts.run_realtime_v2_continuous_ingestion")
BASE = [
    "--database", "stocks_realtime_v2",
    "--confirm-database-name", "stocks_realtime_v2",
    "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
    "--authorized-session-date", "2026-10-15",
    "--first-authorized-cutoff", "2026-10-15T18:30:00+00:00",
    "--daily-request-ceiling", "126",
    "--cache-root", "backend\\.realtime-canary\\continuous-cache",
    "--report-directory", "backend\\.realtime-canary\\continuous-reports",
    "--summary-report", "backend\\.realtime-canary\\continuous-summary.json",
]


def test_continuous_ingestion_has_exact_daily_ceiling():
    args = script.parse_args(BASE)
    assert args.daily_request_ceiling == 126
    assert args.windows[0].cutoff.isoformat() == "2026-10-15T18:30:00+00:00"
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("--daily-request-ceiling") + 1],
            "125",
            *BASE[BASE.index("--daily-request-ceiling") + 2:],
        ])


def test_continuous_ingestion_requires_authorized_execution():
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute"])


COHORT = UUID(BASE[BASE.index("--cohort-id") + 1])
CLOSE = datetime(2026, 10, 15, 20, tzinfo=timezone.utc)


def _receipt_row(*, state="COMPLETE", receipt=True, suffix=""):
    return (
        CLOSE.isoformat() + suffix, state, "MASSIVE_DEVELOPER", CLOSE + timedelta(minutes=16),
        "receipt" if receipt else None, COHORT, CLOSE, "a" * 64, "b" * 64,
        EQUITY_WINDOW_BUILD_ID, 1, "a" * 64, "stages_v2", COHORT,
        CLOSE + timedelta(minutes=20),
    )


def _recovery(rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    return equity_window_recovery_cursor(cursor, COHORT, CLOSE)


def test_latest_due_targets_close_not_older_missing_windows():
    windows = developer_session_windows(
        CLOSE.date(), completion_budget_seconds=300, serving_lag_seconds=30
    )
    assert script.latest_due_window(windows, CLOSE + timedelta(minutes=15)).cutoff == CLOSE
    assert script.latest_due_window(windows, windows[0].available_after - timedelta(seconds=1)) is None
    assert script.latest_due_window(windows, CLOSE + timedelta(days=2)).cutoff == CLOSE


def test_complete_window_requires_sealed_matching_input():
    assert _recovery([_receipt_row()])["complete"]
    assert not _recovery([_receipt_row(receipt=False)])["complete"]
    assert not _recovery([])["has_input_receipt"]


@pytest.mark.parametrize("index,value", (
    (2, "MASSIVE_ADVANCED"), (5, UUID(int=1)), (6, CLOSE - timedelta(minutes=15)),
    (7, "c" * 64), (8, "not-a-hash"), (9, "other-build"), (10, 0),
    (12, "stages_v1"), (13, UUID(int=2)),
))
def test_recovery_refuses_incompatible_receipt_identity(index, value):
    row = list(_receipt_row())
    row[index] = value
    with pytest.raises(ValueError, match="does not match"):
        _recovery([tuple(row)])


def test_failed_window_reuses_input_after_five_minutes_without_refetch():
    recovery = _recovery([_receipt_row(state="FAILED")])
    assert recovery["has_input_receipt"] and not recovery["complete"]
    assert recovery["input_receipt"].input_sha256 == "b" * 64
    assert recovery["input_known_by"] == CLOSE + timedelta(minutes=20)
    assert recovery["retry_suffix"] == "-receipt-retry1"
    assert recovery["retry_not_before"] == CLOSE + timedelta(minutes=21)
    missing = _recovery([_receipt_row(state="FAILED", receipt=False)])
    assert missing["retry_suffix"] == "-acquisition-retry1"


def test_retry_window_identity_never_overwrites_an_existing_attempt():
    recovery = _recovery([
        _receipt_row(state="FAILED"),
        _receipt_row(state="FAILED", suffix="-receipt-retry1"),
    ])
    assert recovery["retry_suffix"] == "-receipt-retry2"


@pytest.mark.parametrize("close", (
    CLOSE, datetime(2026, 11, 27, 18, tzinfo=timezone.utc),
))
def test_exact_regular_and_early_close_are_executable(monkeypatch, close):
    from scripts import run_realtime_v2_developer_window as window

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return close + timedelta(minutes=16)

    monkeypatch.setattr(window, "datetime", Clock)
    assert window._validate_cutoff(close) == close.date()
    with pytest.raises(ValueError, match="outside"):
        window._validate_cutoff(close + timedelta(minutes=15))


def test_completed_close_skips_provider_and_does_not_replay_older_slots(
    monkeypatch, tmp_path
):
    args = list(BASE)
    for option, name in (
        ("--cache-root", "cache"), ("--report-directory", "reports"),
        ("--summary-report", "summary.json"),
    ):
        args[args.index(option) + 1] = str(tmp_path / name)
    args += ["--execute", "--authorization-reference", "test"]
    handlers = {}
    monkeypatch.setattr(script, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(script, "_leadership", lambda *args: nullcontext(True))
    monkeypatch.setenv("REALTIME_V2_APP_DSN", "fixture")
    monkeypatch.setattr(script.signal, "signal", lambda number, callback: handlers.update({number: callback}))
    monkeypatch.setattr(script.time, "sleep", lambda _: handlers[script.signal.SIGTERM]())
    recovery = MagicMock(return_value={
        "complete": True, "has_input_receipt": True,
        "retry_suffix": "-receipt-retry1", "retry_not_before": None,
    })
    execute = MagicMock()
    monkeypatch.setattr(script, "_window_recovery_state", recovery)
    monkeypatch.setattr(script, "_run_target", execute)
    assert script.main(args) == 0
    assert script.main(args) == 0
    assert recovery.call_args.args[2] == CLOSE
    execute.assert_not_called()


def test_restart_refuses_foreign_status_and_changed_frozen_request_budget(monkeypatch, tmp_path):
    args = list(BASE)
    status_path = tmp_path / "status.json"
    args[args.index("--summary-report") + 1] = str(status_path)
    args += ["--authorization-reference", "approval"]
    parsed = script.parse_args(args)
    status_path.write_text(json.dumps({
        "mode": "EXECUTE", **script._service_identity(parsed), "cohort_id": str(UUID(int=99)),
    }), encoding="utf-8")
    with pytest.raises(SystemExit):
        script.parse_args([*args, "--execute"])
    assert json.loads(status_path.read_text())["cohort_id"] == str(UUID(int=99))

    connection = MagicMock()
    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    frozen = script._authority_prefix(parsed) + script.canonical_sha256(script._service_identity(parsed))
    cursor.fetchall.return_value = [(frozen + ":" + CLOSE.isoformat(), True), (frozen + ":retry", False)]
    monkeypatch.setattr(script.psycopg2, "connect", lambda *args, **kwargs: connection)
    assert script._charged_window_attempts("fixture", parsed) == 18
    parsed.daily_request_ceiling += 18
    with pytest.raises(RuntimeError, match="approval scope changed"):
        script._charged_window_attempts("fixture", parsed)


def test_legacy_unfrozen_approval_cannot_reset_admission_on_restart(monkeypatch):
    args = script.parse_args([*BASE, "--authorization-reference", "legacy"])
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("legacy:" + CLOSE.isoformat(), True)
    ]
    monkeypatch.setattr(script.psycopg2, "connect", lambda *args, **kwargs: connection)
    with pytest.raises(RuntimeError, match="frozen budget"):
        script._charged_window_attempts("fixture", args)


@pytest.mark.parametrize("newer_slot", (False, True))
def test_failed_target_backs_off_but_newer_target_is_not_blocked(
    monkeypatch, tmp_path, newer_slot
):
    args = list(BASE)
    for option, name in (
        ("--cache-root", "cache"), ("--report-directory", "reports"),
        ("--summary-report", "summary.json"),
    ):
        args[args.index(option) + 1] = str(tmp_path / name)
    args += ["--execute", "--authorization-reference", "test"]
    clock = [CLOSE.replace(hour=18, minute=59 if newer_slot else 50)]
    handlers, completed, attempts = {}, set(), []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]

    def sleep(_):
        if clock[0].minute == (59 if newer_slot else 50):
            clock[0] += timedelta(minutes=1)
        else:
            handlers[script.signal.SIGTERM]()

    def recovery(_dsn, _cohort, cutoff):
        return {
            "complete": cutoff in completed, "has_input_receipt": False,
            "retry_suffix": "", "retry_not_before": None,
        }

    def execute(_args, cutoff, _recovery):
        attempts.append(cutoff)
        if len(attempts) == 1:
            raise RuntimeError("injected window failure")
        completed.add(cutoff)
        return {"provider_requests": 18}

    monkeypatch.setattr(script, "datetime", Clock)
    monkeypatch.setattr(script, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(script, "_leadership", lambda *args: nullcontext(True))
    monkeypatch.setenv("REALTIME_V2_APP_DSN", "fixture")
    monkeypatch.setattr(script.signal, "signal", lambda number, callback: handlers.update({number: callback}))
    monkeypatch.setattr(script.time, "sleep", sleep)
    monkeypatch.setattr(script, "_window_recovery_state", recovery)
    monkeypatch.setattr(script, "_charged_window_attempts", lambda *args: 0)
    monkeypatch.setattr(script, "_run_target", execute)

    assert script.main(args) == 0
    assert len(attempts) == (2 if newer_slot else 1)
    if newer_slot:
        assert attempts == [CLOSE.replace(hour=18, minute=30), CLOSE.replace(hour=18, minute=45)]


def test_leadership_connection_closes_and_refuses_wrong_database(monkeypatch):
    connection = MagicMock()
    connection.closed = 0
    pool = MagicMock()
    pool.getconn.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [("stocks_realtime_v1",)]
    monkeypatch.setattr(script, "SimpleConnectionPool", lambda *args, **kwargs: pool)

    with pytest.raises(RuntimeError, match="wrong database"):
        with script._leadership("fixture", COHORT, CLOSE.date()):
            pytest.fail("wrong database must not acquire leadership")

    pool.closeall.assert_called_once()
    assert cursor.execute.call_count == 3
