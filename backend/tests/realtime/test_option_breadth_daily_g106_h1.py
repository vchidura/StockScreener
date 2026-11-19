"""G106 H1 (Nov 17 night): daily close chain snapshot, the parts that need no database (the copy itself, idempotence,
the open-session refusal and the backfill run on a disposable Postgres in test_option_breadth_daily_v2_db.py)."""
import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import realtime.option_breadth_daily as daily
from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations
from realtime.relation_registry import RELATIONS, grants

UTC = timezone.utc


def test_switch_is_off_by_default():
    assert daily.snapshot_enabled({}) is False
    assert daily.snapshot_enabled({daily.SNAPSHOT_SWITCH: " on "}) is True


def test_snapshot_is_allowed_from_the_exchange_close_plus_twenty_minutes():
    assert daily.snapshot_allowed_at(date(2026, 11, 16)) == datetime(2026, 11, 16, 21, 20, tzinfo=UTC)
    # Day after Thanksgiving: early close 13:00 New York.
    assert daily.snapshot_allowed_at(date(2026, 11, 27)) == datetime(2026, 11, 27, 18, 20, tzinfo=UTC)
    with pytest.raises(ValueError, match="not an exchange session"):
        daily.snapshot_allowed_at(date(2026, 11, 15))  # a Sunday
    with pytest.raises(daily.SessionStillOpen):
        daily.require_closed_session(date(2026, 11, 17), now=datetime(2026, 11, 17, 21, 19, 59, tzinfo=UTC))
    assert daily.require_closed_session(
        date(2026, 11, 17), now=datetime(2026, 11, 17, 21, 20, tzinfo=UTC)
    ) == datetime(2026, 11, 17, 21, 20, tzinfo=UTC)
    # A backfill of a past session is simply a closed session.
    daily.require_closed_session(date(2026, 11, 9), now=datetime(2026, 11, 17, 5, 0, tzinfo=UTC))


def test_migration_055_and_the_registry_entry():
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    last = migrations[-1]
    assert last.version == "055_g106_option_breadth_daily" and last.sequence == 55
    assert "CREATE TABLE rt.option_breadth_daily" in last.sql
    # Grants come from the registry and the hardening routine, never from the migration.
    assert not [line for line in last.sql.splitlines() if line.strip().upper().startswith(("GRANT", "REVOKE"))]
    spec = RELATIONS["rt.option_breadth_daily"]
    assert (spec.domain, spec.retention, spec.writes, spec.delete, spec.api) == ("options", "BOUNDED", "APPEND", False, True)
    assert grants()["rt.option_breadth_daily"] == {"SELECT": ("app", "api"), "INSERT": ("app",), "UPDATE": ("app",)}
    # The current table's readers: the same SELECT roles.
    assert grants()["rt.option_breadth_current"]["SELECT"] == grants()["rt.option_breadth_daily"]["SELECT"]


def test_csv_header_matches_the_current_table_columns():
    text = (V2_MIGRATIONS_DIR / "052_g75_option_breadth.sql").read_text(encoding="utf-8")
    body = text.split("CREATE TABLE rt.option_breadth_current (", 1)[1].split("PRIMARY KEY", 1)[0]
    columns = tuple(line.strip().split()[0] for line in body.splitlines()
                    if line.strip() and not line.strip().startswith("--"))
    assert columns == daily.BREADTH_COLUMNS


class _Unit:
    def __init__(self, cursor):
        self.cursor = cursor


class _Provider:
    def __init__(self, newest_session):
        self.newest_session = newest_session
        self.transactions = []
        self.snapshots = 0

    @contextmanager
    def transaction(self, role, *, read_only=False):
        self.transactions.append((role, read_only))
        yield _Unit(self)

    def execute(self, statement, params=()):
        self.last = statement

    def fetchall(self):
        return [(self.newest_session,)]


def test_the_scheduler_hook_does_nothing_while_off():
    provider = _Provider(date(2026, 11, 16))
    snapshotter = daily.ClosedSessionSnapshotter(provider, environment={})
    assert snapshotter.maybe_run() is None and provider.transactions == []


def test_the_scheduler_hook_waits_for_the_close_then_copies_once(monkeypatch):
    calls = []

    def fake_snapshot(cursor, *, session_date, now, calendar=None):
        calls.append((session_date, now))
        return {"session_date": session_date.isoformat(), "inserted": 17140, "complete": True, "by_underlying": {}}

    monkeypatch.setattr(daily, "snapshot_session_cursor", fake_snapshot)
    clock = [datetime(2026, 11, 17, 21, 6, tzinfo=UTC)]
    provider = _Provider(date(2026, 11, 17))
    snapshotter = daily.ClosedSessionSnapshotter(
        provider, clock=lambda: clock[0], environment={daily.SNAPSHOT_SWITCH: "ON"},
    )
    assert snapshotter.maybe_run() is None  # 21:06 < 21:20
    assert snapshotter.maybe_run() is None and len(provider.transactions) == 1  # waits until 21:20 without reading
    clock[0] = datetime(2026, 11, 17, 21, 20, 30, tzinfo=UTC)
    result = snapshotter.maybe_run()
    assert result == {"status": "BREADTH_DAILY_SNAPSHOT", "session_date": "2026-11-17", "inserted": 17140,
                      "complete": True}
    assert calls == [(date(2026, 11, 17), clock[0])]
    assert provider.transactions[-1] == ("app", False)
    reads = len(provider.transactions)
    assert snapshotter.maybe_run() is None and len(provider.transactions) == reads  # idle: no read for 10 minutes
    clock[0] = datetime(2026, 11, 17, 21, 31, tzinfo=UTC)
    assert snapshotter.maybe_run() is None and len(calls) == 1  # once per session per process
    assert len(provider.transactions) == reads + 1


def test_the_scheduler_hook_reports_a_failure_and_retries_later(monkeypatch):
    def broken(cursor, **_kwargs):
        raise RuntimeError("permission denied for table option_breadth_daily")

    monkeypatch.setattr(daily, "snapshot_session_cursor", broken)
    clock = [datetime(2026, 11, 17, 21, 30, tzinfo=UTC)]
    snapshotter = daily.ClosedSessionSnapshotter(
        _Provider(date(2026, 11, 17)), clock=lambda: clock[0], environment={daily.SNAPSHOT_SWITCH: "ON"},
        retry_seconds=300,
    )
    failed = snapshotter.maybe_run()
    assert failed["status"] == "BREADTH_DAILY_SNAPSHOT_FAILED" and "permission denied" in failed["reason"]
    assert snapshotter.maybe_run() is None  # inside the retry wait
    clock[0] = datetime(2026, 11, 17, 21, 35, tzinfo=UTC)
    assert snapshotter.maybe_run()["status"] == "BREADTH_DAILY_SNAPSHOT_FAILED"


def test_cli_needs_exactly_one_target():
    from scripts.snapshot_option_breadth_daily import parse_args

    assert parse_args(["--session", "2026-11-16"]).session == [date(2026, 11, 16)]
    assert parse_args(["--backfill-retained", "--apply"]).apply is True
    assert parse_args(["--csv", "x.csv.gz"]).csv == [Path("x.csv.gz")]
    with pytest.raises(SystemExit):
        parse_args([])
    with pytest.raises(SystemExit):
        parse_args(["--session", "2026-11-16", "--backfill-retained"])


def test_scheduler_entry_point_wires_the_hook():
    source = (Path(__file__).resolve().parents[2] / "scripts" / "run_advanced_options_v3_scheduler.py").read_text(
        encoding="utf-8")
    assert "ClosedSessionSnapshotter(provider)" in source
    assert 'if result.get("status") == "WAITING_FOR_SESSION":' in source


def test_live_plan_from_054_has_exactly_055_pending():
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME, plan_realtime_migrations

    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    applied = {item.version: item.checksum_sha256 for item in migrations if item.sequence <= 54}
    plan = plan_realtime_migrations(EXPECTED_V2_DATABASE_NAME, applied, migrations)
    assert [item.version for item in plan.pending] == ["055_g106_option_breadth_daily"]


# ---- r138 (Nov 18): the switch refuses an invalid value, like the scheduler's other switches ---------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, "OFF"), ("", "OFF"), ("   ", "OFF"), ("off", "OFF"), (" OFF ", "OFF"), ("on", "ON"), (" On ", "ON"), ("ON", "ON"),
])
def test_switch_values(raw, expected):
    environment = {} if raw is None else {daily.SNAPSHOT_SWITCH: raw}
    assert daily.snapshot_mode(environment) == expected
    assert daily.snapshot_enabled(environment) is (expected == "ON")
    assert daily.ClosedSessionSnapshotter(object(), environment=environment).enabled is (expected == "ON")


@pytest.mark.parametrize("raw", ["RECORD", "BOGUS", "1", "true", "yes", "ENABLED"])
def test_an_invalid_switch_value_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=daily.SNAPSHOT_SWITCH):
        daily.snapshot_mode({daily.SNAPSHOT_SWITCH: raw})
    with pytest.raises(ValueError, match=daily.SNAPSHOT_SWITCH):
        daily.ClosedSessionSnapshotter(object(), environment={daily.SNAPSHOT_SWITCH: raw})
    with pytest.raises(ValueError, match=daily.SNAPSHOT_SWITCH):
        daily.parse_snapshot_mode(1)


def test_the_options_scheduler_refuses_to_start_on_an_invalid_value(monkeypatch):
    import importlib

    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(daily.SNAPSHOT_SWITCH, "RECORD")
    with pytest.raises(ValueError, match=daily.SNAPSHOT_SWITCH):
        script.parse_args(argv)
    for good in ("", "OFF", "off", "ON", " on "):
        monkeypatch.setenv(daily.SNAPSHOT_SWITCH, good)
        assert script.parse_args(argv).continuous
    monkeypatch.delenv(daily.SNAPSHOT_SWITCH)
    assert script.parse_args(argv).continuous


class _NoDatabase:
    """Any database use fails the test: OFF must not read or write."""

    def __init__(self):
        self.calls = []

    def transaction(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise AssertionError("OFF must not touch the database")


def _scheduler_service(monkeypatch, value, provider, statuses):
    import importlib
    import threading
    from types import SimpleNamespace

    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    stop = threading.Event()
    if value is None:
        monkeypatch.delenv(daily.SNAPSHOT_SWITCH, raising=False)
    else:
        monkeypatch.setenv(daily.SNAPSHOT_SWITCH, value)
    for key in ("REALTIME_V3_OPTIONS_SLOT_ROTATION", "REALTIME_V3_OPTIONS_LEVELS"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(script, "validate_target_database_v2", lambda *a, **k: None)
    monkeypatch.setattr(script, "_provider", lambda: (provider, SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(script.signal, "signal", lambda *a: None)
    monkeypatch.setattr(script.threading, "Event", lambda: stop)
    monkeypatch.setattr(script, "AdvancedEquityLifecyclePlanner", lambda: SimpleNamespace(
        plan=lambda **_: SimpleNamespace(processing_session_date=date(2026, 11, 18))))
    remaining = list(statuses)

    def cycle(*_args, **_kwargs):
        status = remaining.pop(0)
        if not remaining:
            stop.set()
        return {"status": status}

    monkeypatch.setattr(script, "run_continuous_options_schedule_cycle", cycle)
    argv = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
            "--poll-seconds", "0.001", "--continuous", "--execute"]
    assert script.main(argv) == 0


@pytest.mark.parametrize("value", [None, "", "OFF", " off "])
def test_off_scheduler_is_unchanged_no_line_no_database(monkeypatch, capsys, value):
    provider = _NoDatabase()
    _scheduler_service(monkeypatch, value, provider, ["WAITING_FOR_MATERIALIZED_E", "WAITING_FOR_SESSION",
                                                      "WAITING_FOR_SESSION"])
    captured = capsys.readouterr()
    assert provider.calls == []
    assert captured.out.splitlines() == ['{"status": "WAITING_FOR_MATERIALIZED_E"}', '{"status": "WAITING_FOR_SESSION"}',
                                         '{"status": "WAITING_FOR_SESSION"}']
    assert "BREADTH_DAILY" not in captured.out + captured.err


def test_on_scheduler_logs_its_setting_once_and_copies_after_the_decision_stop(monkeypatch, capsys):
    calls = []

    def fake_snapshot(cursor, *, session_date, now, calendar=None):
        calls.append(session_date)
        return {"session_date": session_date.isoformat(), "inserted": 3, "complete": True, "by_underlying": {}}

    monkeypatch.setattr(daily, "snapshot_session_cursor", fake_snapshot)
    provider = _Provider(date(2026, 11, 16))  # a closed session: the copy is allowed whenever the test runs
    _scheduler_service(monkeypatch, "ON", provider, ["WAITING_FOR_MATERIALIZED_E", "WAITING_FOR_SESSION"])
    captured = capsys.readouterr()
    assert captured.err.count(daily.SNAPSHOT_SETTING_EVENT + " ") == 1
    assert json.loads(captured.err.split(daily.SNAPSHOT_SETTING_EVENT + " ", 1)[1].splitlines()[0]) == {
        "margin_minutes": 20, "mode": "ON", "setting": daily.SNAPSHOT_SWITCH}
    lines = captured.out.splitlines()
    assert lines[:2] == ['{"status": "WAITING_FOR_MATERIALIZED_E"}', '{"status": "WAITING_FOR_SESSION"}']
    assert json.loads(lines[2]) == {"status": "BREADTH_DAILY_SNAPSHOT", "session_date": "2026-11-16", "inserted": 3,
                                    "complete": True}
    assert calls == [date(2026, 11, 16)]
    # The hook runs only on WAITING_FOR_SESSION: one read-only read, one write transaction.
    assert provider.transactions == [("app", True), ("app", False)]
