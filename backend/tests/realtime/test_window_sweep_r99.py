"""G102 R99 (Nov 18): close-hour sweep of windows past their deadline - modes, eligibility, ACT gate (no database)."""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import pytest

from realtime import window_sweep as ws
from scripts import run_realtime_v2_self_heal as cli

UTC = timezone.utc
NOW = datetime(2026, 11, 18, 22, 46, tzinfo=UTC)  # 17:46 New York, after the 22:45 settle time


class _Calendar:
    """XNYS for the days the tests use: close 21:00 UTC (16:00 EST), next open 14:30 UTC the next weekday."""

    def session_close(self, session):
        return datetime(session.year, session.month, session.day, 21, tzinfo=UTC)

    def next_session_open(self, session):
        following = session + timedelta(days=3 if session.weekday() == 4 else 1)
        return datetime(following.year, following.month, following.day, 14, 30, tzinfo=UTC)


CAL = _Calendar()


def _window(key, *, state="PENDING", session=date(2026, 11, 13), cutoff=None, deadline=None, lease=None,
            stages=(), sibling_complete=False, window_id=None):
    cutoff = cutoff or datetime(session.year, session.month, session.day, 17, 0, tzinfo=UTC)
    return ws.StaleWindow(
        window_id=window_id or f"id-{key}", window_key=key, state=state, session_date=session, cutoff=cutoff,
        deadline=deadline or cutoff + timedelta(minutes=10), lease_until=lease, owner_epoch=1 if lease else 0,
        running_stages=tuple(stages), sibling_complete=sibling_complete,
    )


def test_mode_parsing_default_record_and_invalid_refused():
    assert ws.parse_window_sweep_mode(None) is ws.WindowSweepMode.RECORD
    assert ws.parse_window_sweep_mode("  ") is ws.WindowSweepMode.RECORD
    assert ws.parse_window_sweep_mode(" act ") is ws.WindowSweepMode.ACT
    assert ws.parse_window_sweep_mode("off") is ws.WindowSweepMode.OFF
    assert ws.window_sweep_mode(None, {"REALTIME_V2_WINDOW_SWEEP": "OFF"}) is ws.WindowSweepMode.OFF
    assert ws.window_sweep_mode("RECORD", {"REALTIME_V2_WINDOW_SWEEP": "ACT"}) is ws.WindowSweepMode.RECORD
    for bad in ("ON", "MARK_MISSED", "yes", 1):
        with pytest.raises(ValueError):
            ws.parse_window_sweep_mode(bad)


def test_act_needs_the_explicit_flag():
    assert ws.effective_mode(ws.WindowSweepMode.ACT, False) == (ws.WindowSweepMode.RECORD, True)
    assert ws.effective_mode(ws.WindowSweepMode.ACT, True) == (ws.WindowSweepMode.ACT, False)
    assert ws.effective_mode(ws.WindowSweepMode.RECORD, True) == (ws.WindowSweepMode.RECORD, False)
    assert ws.effective_mode(ws.WindowSweepMode.ACT, "yes") == (ws.WindowSweepMode.RECORD, True)


def test_plan_closes_stale_and_leaves_live_current_unknown_and_r77_windows():
    today = date(2026, 11, 18)
    windows = [
        _window("E:2026-11-13T17:00:00+00:00"),                                   # Options PENDING, never claimed
        _window("E:2026-11-16T14:45:00+00:00", state="RUNNING", session=date(2026, 11, 16),
                lease=datetime(2026, 11, 16, 14, 52, tzinfo=UTC), stages=("OPTIONS_CYCLE",)),  # dead owner
        _window("advanced-20261113T151500Z", state="RUNNING",
                lease=datetime(2026, 11, 13, 15, 30, tzinfo=UTC)),               # Equity RUNNING, dead owner
        _window("E:2026-11-18T14:35:00+00:00", session=today,
                cutoff=datetime(2026, 11, 18, 14, 35, tzinfo=UTC)),               # today's R100 window, settled
        _window("E:2026-11-17T19:00:00+00:00", state="RUNNING", session=date(2026, 11, 17),
                lease=NOW + timedelta(seconds=30)),                               # live lease
        _window("E:2026-11-17T19:05:00+00:00", state="RUNNING", session=date(2026, 11, 17),
                lease=NOW - timedelta(seconds=60)),                               # lease just expired: still live
        _window("weird-key", session=date(2026, 11, 13)),                         # unknown kind
        _window("advanced-20261118T210000Z", session=today,
                cutoff=datetime(2026, 11, 18, 21, tzinfo=UTC)),                   # R77's close window
        _window("advanced-20261113T210000Z", state="RUNNING", session=date(2026, 11, 13),
                cutoff=datetime(2026, 11, 13, 21, tzinfo=UTC),
                lease=datetime(2026, 11, 13, 21, 20, tzinfo=UTC)),                # old close window: R77 is past it
        _window("advanced-20261118T204500Z-retry1", session=today,
                cutoff=datetime(2026, 11, 18, 20, 45, tzinfo=UTC)),               # ordinary Equity window today
        _window("E:2026-11-18T21:00:00+00:00", state="COMPLETE", session=today),  # terminal: never a candidate
        _window("E:2026-11-18T20:55:00+00:00", state="RETAINED_GAP", session=today),
    ]
    plan = ws.plan_sweep(windows, now=NOW, calendar=CAL)
    assert [w.window_key for w in plan.eligible] == [
        "E:2026-11-13T17:00:00+00:00", "E:2026-11-16T14:45:00+00:00", "advanced-20261113T151500Z",
        "E:2026-11-18T14:35:00+00:00", "advanced-20261113T210000Z", "advanced-20261118T204500Z-retry1",
    ]
    assert {(w.window_key, reason) for w, reason in plan.skipped} == {
        ("E:2026-11-17T19:00:00+00:00", ws.SKIP_LIVE_LEASE),
        ("E:2026-11-17T19:05:00+00:00", ws.SKIP_LIVE_LEASE),
        ("weird-key", ws.SKIP_UNKNOWN_KIND),
        ("advanced-20261118T210000Z", ws.SKIP_R77_CLOSE_RECOVERY),
    }


def test_current_session_is_never_touched_before_its_settle_time():
    today = date(2026, 11, 18)
    stale_today = _window("E:2026-11-18T14:35:00+00:00", session=today,
                          cutoff=datetime(2026, 11, 18, 14, 35, tzinfo=UTC))
    old = _window("E:2026-11-13T17:00:00+00:00")
    for now in (datetime(2026, 11, 18, 15, 0, tzinfo=UTC), datetime(2026, 11, 18, 22, 44, tzinfo=UTC)):
        plan = ws.plan_sweep([stale_today, old], now=now, calendar=CAL)
        assert [w.window_key for w in plan.eligible] == ["E:2026-11-13T17:00:00+00:00"]
        assert [(w.window_key, r) for w, r in plan.skipped] == [("E:2026-11-18T14:35:00+00:00",
                                                                 ws.SKIP_CURRENT_SESSION)]
    plan = ws.plan_sweep([stale_today], now=datetime(2026, 11, 18, 22, 45, tzinfo=UTC), calendar=CAL)
    assert [w.window_key for w in plan.eligible] == ["E:2026-11-18T14:35:00+00:00"]


def test_r77_close_window_released_once_complete_or_after_the_next_open():
    close = datetime(2026, 11, 18, 21, tzinfo=UTC)
    stale = _window("advanced-20261118T210000Z", session=date(2026, 11, 18), cutoff=close)
    done = _window("advanced-20261118T210000Z", session=date(2026, 11, 18), cutoff=close, sibling_complete=True)
    assert not ws.plan_sweep([stale], now=NOW, calendar=CAL).eligible
    assert ws.plan_sweep([done], now=NOW, calendar=CAL).eligible == [done]
    after_open = datetime(2026, 11, 19, 14, 31, tzinfo=UTC)
    assert ws.plan_sweep([stale], now=after_open, calendar=CAL).eligible == [stale]
    # Options windows at the close cutoff are not R77's.
    options = _window("E:2026-11-18T21:00:00+00:00", session=date(2026, 11, 18), cutoff=close)
    assert ws.plan_sweep([options], now=NOW, calendar=CAL).eligible == [options]


def test_calendar_failure_falls_back_to_the_regular_close():
    class Broken:
        def session_close(self, session):
            raise ValueError("not a session")

        def next_session_open(self, session):
            raise ValueError("not a session")

    saturday = date(2026, 11, 14)
    window = _window("E:2026-11-14T17:00:00+00:00", session=saturday)
    assert ws.plan_sweep([window], now=datetime(2026, 11, 14, 22, 46, tzinfo=UTC), calendar=Broken()).eligible
    assert not ws.plan_sweep([window], now=datetime(2026, 11, 14, 22, 44, tzinfo=UTC), calendar=Broken()).eligible
    equity_close = _window("advanced-20261114T210000Z", session=saturday,
                           cutoff=datetime(2026, 11, 14, 21, tzinfo=UTC))
    plan = ws.plan_sweep([equity_close], now=datetime(2026, 11, 20, 22, 46, tzinfo=UTC), calendar=Broken())
    assert [r for _w, r in plan.skipped] == [ws.SKIP_R77_CLOSE_RECOVERY]


# ------------------------------------------------------------------------------------------------ run (fake cursors)
class _ReadCursor:
    def __init__(self, rows):
        self.rows, self.sql = rows, []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))

    def fetchall(self):
        return list(self.rows)


def _row(window):
    return (window.window_id, window.window_key, window.state, window.session_date, window.cutoff, window.deadline,
            window.lease_until, window.owner_epoch, list(window.running_stages), window.sibling_complete)


class _Store:
    """Window rows by id, as the ACT path sees them under the row lock."""

    def __init__(self, windows, stage_runs=None):
        self.windows = {w.window_id: {"state": w.state, "deadline": w.deadline, "lease_until": w.lease_until}
                        for w in windows}
        self.stage_runs = stage_runs or {}
        self.statements = []
        self.fail_boom = True

    @contextmanager
    def transaction(self):
        cursor = _WriteCursor(self)
        yield cursor


class _WriteCursor:
    def __init__(self, store):
        self.store, self._result, self.rowcount = store, [], 0

    def execute(self, sql, params=None):
        self.store.statements.append(sql.split()[0])
        window_id = params[-1]
        if sql.startswith("SELECT state"):
            row = self.store.windows.get(params[0])
            self._result = [] if row is None else [(row["state"], row["deadline"], row["lease_until"])]
        elif sql.startswith("UPDATE rt.stage_runs"):
            if window_id == "id-boom" and self.store.fail_boom:
                raise RuntimeError("database went away")
            running = [stage for stage, state in self.store.stage_runs.get(window_id, {}).items()
                       if state == "RUNNING"]
            for stage in running:
                self.store.stage_runs[window_id][stage] = "FAILED"
            assert "OWNER_LOST" in sql and "GREATEST" in sql
            self._result = [(stage,) for stage in running]
        elif sql.startswith("UPDATE rt.window_runs"):
            row = self.store.windows[window_id]
            assert "lease_until = NULL" in sql and "'MISSED'" in sql
            self.rowcount = 1 if row["state"] in ("PENDING", "RUNNING") else 0
            if self.rowcount:
                row["state"], row["lease_until"] = "MISSED", None
        else:  # pragma: no cover
            raise AssertionError(sql)

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)


def _capture():
    lines, faults = [], []

    def emit_line(event, document):
        lines.append((event, document))

    def emit(**kwargs):
        faults.append(kwargs)
        return kwargs

    return lines, faults, emit_line, emit


WINDOWS = [
    _window("E:2026-11-13T17:00:00+00:00"),
    _window("E:2026-11-16T14:45:00+00:00", state="RUNNING", session=date(2026, 11, 16),
            lease=datetime(2026, 11, 16, 14, 52, tzinfo=UTC), stages=("OPTIONS_CYCLE",)),
    _window("advanced-20261113T151500Z", state="RUNNING", lease=datetime(2026, 11, 13, 15, 30, tzinfo=UTC)),
    _window("E:2026-11-17T19:00:00+00:00", state="RUNNING", session=date(2026, 11, 17),
            lease=NOW + timedelta(minutes=1)),
]


def test_record_reads_only_and_reports_each_window():
    lines, faults, emit_line, emit = _capture()
    read = _ReadCursor([_row(w) for w in WINDOWS])
    summary = ws.run_window_sweep(read, now=NOW, mode="RECORD", calendar=CAL, emit_line=emit_line, emit=emit)
    assert len(read.sql) == 1 and read.sql[0][0].lstrip().startswith("SELECT")
    assert "deadline <" in read.sql[0][0] and read.sql[0][1] == (NOW - timedelta(minutes=2), ws.DEFAULT_LIMIT)
    records = [d for e, d in lines if e == ws.WINDOW_SWEEP_RECORD_EVENT]
    assert [d["window_key"] for d in records] == [w.window_key for w in WINDOWS[:3]]
    assert records[1]["running_stage_runs"] == ["OPTIONS_CYCLE"] and records[1]["would"] == "MISSED"
    assert summary["effective_mode"] == "RECORD" and summary["eligible"] == 3 and "closed" not in summary
    assert summary["eligible_per_session"] == {"2026-11-13": {"EQUITY": 1, "OPTIONS": 1}, "2026-11-16": {"OPTIONS": 1}}
    assert summary["skipped"] == {"LIVE_LEASE": 1} and summary["stage_runs_running"] == 1
    assert lines[-1][0] == ws.WINDOW_SWEEP_SUMMARY_EVENT
    assert [(f["kind"], f["severity"], f["action"], f["key"]) for f in faults] == [
        ("WINDOW_PAST_DEADLINE", "WOULD_ACT", "CLOSE_MISSED", "SWEEP|EQUITY"),
        ("WINDOW_PAST_DEADLINE", "WOULD_ACT", "CLOSE_MISSED", "SWEEP|OPTIONS"),
    ]


def test_act_without_flag_records_and_says_so():
    lines, faults, emit_line, emit = _capture()
    store = _Store(WINDOWS)
    summary = ws.run_window_sweep(_ReadCursor([_row(w) for w in WINDOWS]), now=NOW, mode="ACT", act_flag=False,
                                  write_transaction=store.transaction, calendar=CAL, emit_line=emit_line, emit=emit)
    assert summary["act_refused"] is True and summary["effective_mode"] == "RECORD"
    assert lines[0][0] == ws.WINDOW_SWEEP_ACT_REFUSED_EVENT
    assert store.statements == [] and all(f["severity"] == "WOULD_ACT" for f in faults)


def test_off_reads_nothing():
    lines, faults, emit_line, emit = _capture()
    read = _ReadCursor([])
    summary = ws.run_window_sweep(read, now=NOW, mode="OFF", calendar=CAL, emit_line=emit_line, emit=emit)
    assert read.sql == [] and faults == [] and [e for e, _d in lines] == [ws.WINDOW_SWEEP_SUMMARY_EVENT]
    assert summary["effective_mode"] == "OFF"


def test_act_closes_rechecks_under_the_lock_and_is_idempotent():
    claimed_meanwhile = _window("E:2026-11-13T17:05:00+00:00", window_id="id-claimed")
    boom = _window("E:2026-11-13T17:10:00+00:00", window_id="id-boom")
    windows = [*WINDOWS, claimed_meanwhile, boom]
    store = _Store(windows, stage_runs={"id-E:2026-11-16T14:45:00+00:00": {"OPTIONS_CYCLE": "RUNNING",
                                                                           "OPTIONS_ACQUIRE": "COMPLETE"}})
    # A role claimed this one after the snapshot: its lease is alive under the lock.
    store.windows["id-claimed"].update(state="RUNNING", lease_until=NOW + timedelta(seconds=90))
    lines, faults, emit_line, emit = _capture()
    summary = ws.run_window_sweep(_ReadCursor([_row(w) for w in windows]), now=NOW, mode="ACT", act_flag=True,
                                  write_transaction=store.transaction, calendar=CAL, emit_line=emit_line, emit=emit)
    acted = {d["window_key"]: d for e, d in lines if e == ws.WINDOW_SWEEP_ACTED_EVENT}
    assert acted["E:2026-11-13T17:00:00+00:00"]["outcome"] == "CLOSED"
    assert acted["E:2026-11-16T14:45:00+00:00"]["stage_runs_failed"] == ["OPTIONS_CYCLE"]
    assert acted["E:2026-11-13T17:05:00+00:00"]["outcome"] == "LIVE_LEASE"
    assert acted["E:2026-11-13T17:10:00+00:00"]["outcome"].startswith("ERROR RuntimeError")
    assert "E:2026-11-17T19:00:00+00:00" not in acted  # live lease in the snapshot: never locked
    assert summary["closed"] == 3 and summary["stage_runs_failed"] == 1
    assert summary["not_closed"] == {"ERROR": 1, "LIVE_LEASE": 1}
    assert store.stage_runs["id-E:2026-11-16T14:45:00+00:00"] == {"OPTIONS_CYCLE": "FAILED",
                                                                  "OPTIONS_ACQUIRE": "COMPLETE"}
    assert store.windows["id-E:2026-11-13T17:00:00+00:00"] == {
        "state": "MISSED", "deadline": WINDOWS[0].deadline, "lease_until": None}
    assert store.windows["id-claimed"]["state"] == "RUNNING"
    assert {f["key"]: f["severity"] for f in faults} == {"SWEEP|EQUITY": "ACTED", "SWEEP|OPTIONS": "ERROR"}
    # Second run (the next session's sweep): its snapshot holds only windows still open; the closed ones are not read.
    store.fail_boom = False
    still_open = [w for w in windows if store.windows[w.window_id]["state"] in ("PENDING", "RUNNING")]
    assert {w.window_key for w in still_open} == {"E:2026-11-17T19:00:00+00:00", "E:2026-11-13T17:05:00+00:00",
                                                  "E:2026-11-13T17:10:00+00:00"}
    lines, faults, emit_line, emit = _capture()
    store.windows["id-claimed"]["lease_until"] = NOW + timedelta(seconds=90)
    again = ws.run_window_sweep(_ReadCursor([_row(w) for w in still_open]), now=NOW, mode="ACT", act_flag=True,
                                write_transaction=store.transaction, calendar=CAL, emit_line=emit_line, emit=emit)
    outcomes = {d["window_key"]: d["outcome"] for e, d in lines if e == ws.WINDOW_SWEEP_ACTED_EVENT}
    assert outcomes == {"E:2026-11-13T17:05:00+00:00": "LIVE_LEASE", "E:2026-11-13T17:10:00+00:00": "CLOSED"}
    assert again["closed"] == 1
    # A stale snapshot (a window closed between the read and the lock) is refused under the lock.
    lines, faults, emit_line, emit = _capture()
    ws.run_window_sweep(_ReadCursor([_row(WINDOWS[0])]), now=NOW, mode="ACT", act_flag=True,
                        write_transaction=store.transaction, calendar=CAL, emit_line=emit_line, emit=emit)
    assert [d["outcome"] for e, d in lines if e == ws.WINDOW_SWEEP_ACTED_EVENT] == ["NOW_MISSED"]

def test_act_without_a_writer_is_refused():
    with pytest.raises(ValueError):
        ws.run_window_sweep(_ReadCursor([]), now=NOW, mode="ACT", act_flag=True, calendar=CAL,
                            emit_line=lambda *_: None, emit=lambda **_: None)


def test_cli_parses_window_sweep_flags_and_refuses_invalid_mode(monkeypatch, capsys):
    args = cli.parse_args(["--window-sweep", "--mode", "act", "--act", "--limit", "50"])
    assert args.window_sweep and args.mode == "act" and args.act and args.limit == 50
    args = cli.parse_args(["--once"])
    assert not args.window_sweep and args.mode is None and not args.act
    called = []
    monkeypatch.setattr(cli, "window_sweep_pass", lambda *a, **k: called.append(k) or {})
    assert cli.main(["--window-sweep", "--mode", "BOGUS", "--dsn-file", "/nonexistent"]) == 2
    assert called == [] and "OFF, RECORD or ACT" in capsys.readouterr().err
