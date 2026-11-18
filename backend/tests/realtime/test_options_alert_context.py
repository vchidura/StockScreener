"""G106 O1 tuning (Nov 18): REALTIME_V3_OPTIONS_ALERT_CONTEXT (OFF / RECORD) on the Options window.

OFF = nothing (no note, no SQL, no line); RECORD = the O1 / OS executors note what they select, and after the window's
run one read-only read of the window's chain capture logs the OV1 gamma / delta context per noted item. Never changes or
fails the window.
"""
import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

import realtime.advanced_options_alert_context as alert_context
from realtime.advanced_options_alert_context import (
    ALERT_CONTEXT_SETTING,
    AlertContextItem,
    AlertContextMode,
    direction_context,
    drain_alert_context_notes,
    note_o1_alert_context,
    note_os_alert_context,
    options_alert_context_mode,
    prepare_options_alert_context,
    read_captures_cursor,
    record_options_alert_context,
)
from realtime.options_exposure_read import _ROW_NAMES
from test_o1_gates import _by_event, _process_start, _run  # noqa: F401 - the autouse fixture resets the O1 switches

UTC = timezone.utc
SESSION = date(2026, 11, 17)
EQUITY_CUTOFF = datetime(2026, 11, 17, 18, 30, tzinfo=UTC)
CUTOFF = EQUITY_CUTOFF + timedelta(minutes=5)
OBSERVED = EQUITY_CUTOFF + timedelta(seconds=150)


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.delenv(ALERT_CONTEXT_SETTING, raising=False)
    drain_alert_context_notes()
    yield
    drain_alert_context_notes()


# ---- a synthetic chain (as the r136 levels tests) --------------------------------------------------------------------

def chain_rows(*, spot=100.0, observed=OBSERVED):
    """Calls and puts 80..120: 0 DTE (OI 100 each) and 7 DTE with walls at 103 (calls) and 97 (puts)."""
    rows = []
    for expiry in (SESSION, SESSION + timedelta(days=7)):
        for strike in range(80, 121):
            for kind in ("call", "put"):
                if kind == "call":
                    oi = 9000 if strike == 103 else 1500 if strike >= spot else 300
                else:
                    oi = 9000 if strike == 97 else 1800 if strike <= spot else 200
                if expiry == SESSION:
                    oi = 100
                rows.append({
                    "contract_type": kind, "expiration_date": expiry, "strike_price": Decimal(strike),
                    "open_interest": oi, "day_volume": 50, "local_iv": 0.32, "implied_volatility": 0.33,
                    "gamma": None, "delta": None, "risk_free_rate": 0.04, "dividend_yield": 0.0,
                    "spot": Decimal(str(spot)), "underlying_price": Decimal(str(spot)), "observed_at": observed,
                })
    return rows


class Cursor:
    """Answers each statement by the first matching needle (rows, an exception, or a function of the parameters)."""

    def __init__(self, script):
        self.script = list(script)
        self.statements = []
        self._rows = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        for needle, answer in self.script:
            if needle in sql:
                if callable(answer) and not isinstance(answer, Exception):
                    answer = answer(params)
                if isinstance(answer, Exception):
                    raise answer
                self._rows = list(answer)
                return
        self._rows = []

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows


class Provider:
    def __init__(self, cursor=None, error=None):
        self.cursor, self.error, self.transactions = cursor, error, []

    @contextmanager
    def transaction(self, role, **kwargs):
        self.transactions.append((role, kwargs))
        if self.error is not None:
            raise self.error
        yield SimpleNamespace(cursor=self.cursor)


def script(chains, captures, *, rows_error=None):
    """`chains`: {name: rows}; `captures`: (name, equity_cutoff, observed_at, state, contracts) newest first per name."""

    def groups(params):
        _session, name, observed = params
        rows = chains.get(name) or []
        return [(observed, len(rows))] if rows and rows[0]["observed_at"] == observed else []

    def rows(params):
        _session, name, _observed = params
        if rows_error and name in rows_error:
            return rows_error[name]
        return [tuple(row[key] for key in _ROW_NAMES) for row in chains.get(name, ())]

    def capture_rows(params):
        _session, names, cutoff = params
        return [row for row in captures if row[0] in names and row[1] <= cutoff]

    return [("rt.option_breadth_captures", capture_rows), ("GROUP BY observed_at", groups),
            ("SAVEPOINT", []), ("RELEASE", []), ("ROLLBACK", []), ("SET LOCAL", []),
            ("FROM rt.option_breadth_current", rows)]


def complete(name, rows, cutoff=EQUITY_CUTOFF, observed=OBSERVED):
    return (name, cutoff, observed, "COMPLETE", len(rows))


def lines(capsys, event):
    return [json.loads(line.split(" ", 1)[1]) for line in capsys.readouterr().err.strip().splitlines()
            if line.split(" ", 1)[0] == event]


def all_lines(capsys):
    return [line.split(" ", 1) for line in capsys.readouterr().err.strip().splitlines() if line.strip()]


def item(underlyer="SPY", direction=1, detector="O1", kind="SELECTED", key=None, cutoff=CUTOFF):
    return AlertContextItem(cutoff=cutoff, detector=detector, kind=kind, key=key or str(uuid4()), underlyer=underlyer,
                            direction=direction, contract_ticker="O:SPY261124C00101000", contract_type="CALL", dte=7)


# ---- the switch --------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert options_alert_context_mode({}) is AlertContextMode.OFF
    assert options_alert_context_mode({ALERT_CONTEXT_SETTING: " "}) is AlertContextMode.OFF
    assert options_alert_context_mode({ALERT_CONTEXT_SETTING: "off"}) is AlertContextMode.OFF
    assert options_alert_context_mode({ALERT_CONTEXT_SETTING: " Record "}) is AlertContextMode.RECORD
    for bad in ("ON", "YES", "1", "RECORDED"):
        with pytest.raises(ValueError, match=ALERT_CONTEXT_SETTING):
            options_alert_context_mode({ALERT_CONTEXT_SETTING: bad})
        with pytest.raises(ValueError, match=ALERT_CONTEXT_SETTING):
            prepare_options_alert_context({ALERT_CONTEXT_SETTING: bad})
    assert prepare_options_alert_context({ALERT_CONTEXT_SETTING: "RECORD"}) is AlertContextMode.RECORD


def test_window_service_start_checks_the_switch_and_the_window_calls_the_record():
    root = Path(alert_context.__file__).parents[1]
    production = (root / "scripts" / "run_advanced_options_v3_production.py").read_text(encoding="utf-8")
    start = production.index("if kind is AdvancedOptionsProcessKind.WINDOW:")
    assert "prepare_options_alert_context()" in production[start:production.index("provider = None", start)]
    window = (root / "realtime" / "advanced_options_p8_window.py").read_text(encoding="utf-8")
    run = window.index("report = orchestrator.run_window(binding.window_id)")
    assert window.index("record_options_alert_context(", run) < window.index("return {", run)


# ---- OFF ---------------------------------------------------------------------------------------------------------------

class _Refuse:
    def transaction(self, *args, **kwargs):
        raise AssertionError("OFF reads nothing")


def test_off_notes_nothing_reads_nothing_logs_nothing(monkeypatch, capsys):
    off = _run(monkeypatch)
    assert drain_alert_context_notes() == []
    note_os_alert_context("OS1", CUTOFF, (), None)
    assert drain_alert_context_notes() == []
    assert record_options_alert_context(_Refuse(), session_date=SESSION, cutoff=CUTOFF,
                                        equity_cutoff=EQUITY_CUTOFF) is None
    assert capsys.readouterr().err == ""
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "OFF")
    assert _run(monkeypatch) == off
    assert drain_alert_context_notes() == [] and capsys.readouterr().err == ""


# ---- RECORD: the notes -------------------------------------------------------------------------------------------------

def test_record_notes_o1_selections_and_changes_nothing(monkeypatch, capsys):
    off = _run(monkeypatch)
    capsys.readouterr()
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    record = _run(monkeypatch)
    assert record == off  # the window's evaluations are today's
    assert capsys.readouterr().err == ""  # the executor only notes; the window logs after its run
    notes = drain_alert_context_notes()
    selected = {row.event_id for row in record.evaluations if row.selection_status in ("SELECTED", "OBSERVATION")}
    assert {UUID(note.key) for note in notes} == selected and len(notes) == len(selected)
    by_key = {UUID(note.key): note for note in notes}
    for event_id, row in _by_event(record).items():
        if event_id in by_key:
            note = by_key[event_id]
            assert (note.detector, note.kind, note.underlyer) == ("O1", row.selection_status, "AAPL")
            assert note.direction == (1 if note.contract_type == "CALL" else -1)
            assert note.contract_ticker.startswith("O:") and note.dte >= 0


def test_record_notes_o1_aggressor_class_when_the_gate_records(monkeypatch):
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    _run(monkeypatch, aggressor="RECORD")
    classes = {note.detail.get("aggressor_class") for note in drain_alert_context_notes()}
    assert classes == {"BOUGHT", "SOLD", "UNCLEAR"}
    _run(monkeypatch, aggressor="ON")
    notes = drain_alert_context_notes()
    assert {note.kind for note in notes} == {"SELECTED", "OBSERVATION"}
    assert any(note.detail["gate_reasons"] == ["O1AG_SOLD"] for note in notes)


def _os_evaluation(status, direction=-1, underlyer="SPY"):
    """An OS lane evaluation as the executor builds it (only the attributes the note reads)."""
    return SimpleNamespace(
        event_id=uuid4(), underlyer=underlyer, selection_status=status, observed_at=CUTOFF,
        payload={"contract": "advanced-options-stock-first-evaluation-v1", "detector_id": "OS1",
                 "signal": {"signal_id": str(uuid4()), "direction": direction,
                            "market_time": EQUITY_CUTOFF.isoformat()},
                 "decision": {"disposition": "UNMATCHED" if status == "NOT_SELECTED" else "CONFIRMED"}})


def _outcome(first_window, confirmed=None, flow20=False, direction=-1, underlyer="QQQ"):
    signal = SimpleNamespace(signal_id=str(uuid4()), underlyer=underlyer, direction=direction,
                             first_window=first_window, market_time=EQUITY_CUTOFF)
    return SimpleNamespace(signal=signal, confirmed=confirmed is not None, confirmed_contract=confirmed,
                           flow20={"confirmed": flow20})


def test_record_notes_os_confirmations_and_v2_signals(monkeypatch):
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    evaluations = (_os_evaluation("SELECTED"), _os_evaluation("OBSERVATION", 1), _os_evaluation("NOT_SELECTED"))
    record = SimpleNamespace(outcomes=(
        _outcome(True), _outcome(False), _outcome(False, confirmed="O:QQQ261120P00600000"), _outcome(False, flow20=True),
    ))
    note_os_alert_context("OS1", CUTOFF, evaluations, record)
    notes = drain_alert_context_notes()
    assert [(note.kind, note.direction) for note in notes] == [
        ("SELECTED", -1), ("OBSERVATION", 1), ("SIGNAL", -1), ("SIGNAL", -1), ("SIGNAL", -1)]
    signals = [note for note in notes if note.kind == "SIGNAL"]
    assert [(n.detail["first_window"], n.detail["v2_confirmed"], n.detail["flow20_confirmed"]) for n in signals] == [
        (True, False, False), (False, True, False), (False, False, True)]
    assert signals[1].contract_ticker == "O:QQQ261120P00600000"
    assert {note.detector for note in notes} == {"OS1"}


def test_notes_never_raise_and_are_capped(monkeypatch):
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    note_o1_alert_context(CUTOFF, (object(),), ())  # not an evaluation: ignored, no raise
    note_os_alert_context("OS2", CUTOFF, (object(),), object())
    assert drain_alert_context_notes() == []
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "BAD")  # the service refuses to start; a window never fails on it
    note_o1_alert_context(CUTOFF, (), ())
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    monkeypatch.setattr(alert_context, "MAXIMUM_PENDING", 3)
    alert_context._note([item(key=str(n)) for n in range(5)])
    assert [note.key for note in drain_alert_context_notes()] == ["2", "3", "4"]


def test_drain_deduplicates_a_lane_run_twice():
    one = item(key="a")
    alert_context._note([one, one, item(key="a", kind="OBSERVATION")])
    assert len(drain_alert_context_notes()) == 2


# ---- the read ----------------------------------------------------------------------------------------------------------

def test_read_uses_the_newest_complete_capture_at_or_before_the_equity_cutoff():
    spy, qqq = chain_rows(), chain_rows(spot=200.0, observed=OBSERVED - timedelta(minutes=5))
    captures = [
        ("QQQ", EQUITY_CUTOFF, OBSERVED, "PARTIAL", 999),  # this window's capture is not complete: the one before
        complete("QQQ", qqq, EQUITY_CUTOFF - timedelta(minutes=5), OBSERVED - timedelta(minutes=5)),
        ("SPY", EQUITY_CUTOFF + timedelta(minutes=5), OBSERVED + timedelta(minutes=5), "COMPLETE", 10),  # look-ahead
        complete("SPY", spy),
    ]
    cursor = Cursor(script({"SPY": spy, "QQQ": qqq}, captures))
    reads = read_captures_cursor(cursor, session_date=SESSION, equity_cutoff=EQUITY_CUTOFF,
                                 underlyings=["spy", "QQQ", "AAPL"])
    assert reads["SPY"].chain is not None and reads["SPY"].equity_cutoff == EQUITY_CUTOFF
    assert not reads["SPY"].fallback and reads["SPY"].rows == len(spy)
    assert reads["QQQ"].chain is not None and reads["QQQ"].fallback
    assert reads["QQQ"].equity_cutoff == EQUITY_CUTOFF - timedelta(minutes=5)
    assert reads["AAPL"].chain is None and reads["AAPL"].reason == "NO_CAPTURE"
    assert cursor.statements[0][0] == "SET LOCAL statement_timeout = '20s'"


def test_read_refuses_a_capture_whose_rows_are_being_replaced_and_isolates_failures():
    spy, qqq = chain_rows(), chain_rows(spot=200.0)
    captures = [("SPY", EQUITY_CUTOFF, OBSERVED, "COMPLETE", len(spy) + 5), complete("QQQ", qqq),
                ("IWM", EQUITY_CUTOFF, OBSERVED, "FAILED", 0)]
    cursor = Cursor(script({"SPY": spy, "QQQ": qqq}, captures, rows_error={"QQQ": RuntimeError("boom")}))
    reads = read_captures_cursor(cursor, session_date=SESSION, equity_cutoff=EQUITY_CUTOFF,
                                 underlyings=["SPY", "QQQ", "IWM"])
    assert (reads["SPY"].chain, reads["SPY"].reason) == (None, "CAPTURE_ROWS_INCOMPLETE")
    assert (reads["QQQ"].chain, reads["QQQ"].reason) == (None, "READ:RuntimeError")
    assert (reads["IWM"].chain, reads["IWM"].reason, reads["IWM"].state) == (None, "NO_COMPLETE_CAPTURE", "FAILED")
    assert any(sql.startswith("ROLLBACK TO SAVEPOINT alert_context_name") for sql, _ in cursor.statements)


# ---- the context ---------------------------------------------------------------------------------------------------------

LEVELS = {"available": True, "spot": 100.0, "gex_sign": "POSITIVE", "regime_at_spot": "POSITIVE_GAMMA",
          "spot_vs_flip": "ABOVE", "call_wall": 103.0, "call_wall_pct": 3.0, "put_wall": 97.0, "put_wall_pct": -3.0,
          "dex_tilt": 0.2, "zero_dte_share": 0.1}


def test_direction_context_reads_the_levels_from_the_alerts_direction():
    bull = direction_context(LEVELS, 1)
    assert (bull["wall"], bull["wall_strike"], bull["room_to_wall_pct"], bull["flip_side"]) == (
        "CALL_WALL", 103.0, 3.0, "WITH")
    assert bull["dex_tilt_in_direction"] == 0.2 and bull["gex_sign"] == "POSITIVE"
    bear = direction_context(LEVELS, -1)
    assert (bear["wall"], bear["wall_strike"], bear["room_to_wall_pct"], bear["flip_side"]) == (
        "PUT_WALL", 97.0, 3.0, "AGAINST")
    assert bear["dex_tilt_in_direction"] == -0.2
    behind = direction_context({**LEVELS, "call_wall_pct": -0.5, "spot_vs_flip": None, "dex_tilt": None}, 1)
    assert (behind["room_to_wall_pct"], behind["flip_side"], behind["dex_tilt_in_direction"]) == (-0.5, None, None)
    assert direction_context({"available": False}, 1) == {"direction": 1, "available": False}


def test_record_logs_one_line_per_item_with_the_pages_levels(monkeypatch, capsys):
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    spy = chain_rows()
    provider = Provider(Cursor(script({"SPY": spy}, [complete("SPY", spy)])))
    earlier = item(cutoff=CUTOFF - timedelta(minutes=5), direction=-1)
    alert_context._note([item(direction=1, key="call"), item(direction=-1, detector="OS2", kind="SIGNAL", key="sig"),
                         item(underlyer="NFLX", key="nflx"), earlier])
    result = record_options_alert_context(provider, session_date=SESSION, cutoff=CUTOFF, equity_cutoff=EQUITY_CUTOFF,
                                          window_id="w1")
    assert result == {"status": "RECORDED", "items": 4, "available": 3}
    assert [kwargs for _role, kwargs in provider.transactions] == [{"read_only": True}]
    lines_ = all_lines(capsys)
    assert [event for event, _ in lines_] == ["OPTIONS_ALERT_CONTEXT_RECORD"] * 4 + ["OPTIONS_ALERT_CONTEXT_WINDOW"]
    docs = {doc["key"]: doc for doc in (json.loads(text) for _, text in lines_[:-1])}
    call = docs["call"]
    assert (call["mode"], call["applied"], call["available"], call["detector"], call["direction"]) == (
        "RECORD", False, True, "O1", 1)
    assert call["levels"]["gex_sign"] in ("POSITIVE", "NEGATIVE")
    assert float(call["levels"]["call_wall"]) == 103 and float(call["levels"]["put_wall"]) == 97
    assert call["levels"]["zero_dte_share"] is not None and call["levels"]["dex_tilt"] is not None
    assert call["context"]["wall"] == "CALL_WALL" and call["context"]["room_to_wall_pct"] == pytest.approx(3.0)
    assert call["capture"]["this_window"] is True and call["capture"]["rows"] == len(spy)
    assert call["capture"]["age_at_cutoff_s"] == 150.0
    sig = docs["sig"]
    assert sig["context"]["wall"] == "PUT_WALL" and sig["context"]["room_to_wall_pct"] == pytest.approx(3.0)
    assert sig["context"]["dex_tilt_in_direction"] == pytest.approx(-call["context"]["dex_tilt_in_direction"])
    assert (docs["nflx"]["available"], docs["nflx"]["reason"]) == (False, "NO_CAPTURE")
    window = json.loads(lines_[-1][1])
    assert (window["status"], window["items"], window["earlier_window_items"], window["available"]) == (
        "OK", 4, 1, 3)
    assert window["underlyings"] == ["NFLX", "SPY"] and window["unavailable_reasons"] == {"NO_CAPTURE": 1}
    assert record_options_alert_context(provider, session_date=SESSION, cutoff=CUTOFF,
                                        equity_cutoff=EQUITY_CUTOFF) == {"status": "NO_ITEMS"}
    assert capsys.readouterr().err == ""  # no items: no line


def test_record_never_raises(monkeypatch, capsys):
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    alert_context._note([item()])
    result = record_options_alert_context(Provider(error=RuntimeError("pool exhausted")), session_date=SESSION,
                                          cutoff=CUTOFF, equity_cutoff=EQUITY_CUTOFF)
    assert result == {"status": "RECORD_FAILED"}
    window = lines(capsys, "OPTIONS_ALERT_CONTEXT_WINDOW")[0]
    assert window["status"] == "RECORD_FAILED" and "pool exhausted" in window["reason"]

    def broken_levels(chain):
        raise ValueError("compute")

    spy = chain_rows()
    monkeypatch.setattr(alert_context, "alert_context_documents",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("documents")))
    alert_context._note([item()])
    assert record_options_alert_context(Provider(Cursor(script({"SPY": spy}, [complete("SPY", spy)]))),
                                        session_date=SESSION, cutoff=CUTOFF,
                                        equity_cutoff=EQUITY_CUTOFF)["status"] == "RECORD_FAILED"
    capsys.readouterr()
    monkeypatch.undo()
    monkeypatch.setenv(ALERT_CONTEXT_SETTING, "RECORD")
    docs, computed, reasons = alert_context.alert_context_documents(
        [item()], read_captures_cursor(Cursor(script({"SPY": spy}, [complete("SPY", spy)])), session_date=SESSION,
                                       equity_cutoff=EQUITY_CUTOFF, underlyings=["SPY"]),
        session_date=SESSION, equity_cutoff=EQUITY_CUTOFF, window_id=None, levels=broken_levels)
    assert (docs[0]["available"], docs[0]["reason"]) == (False, "COMPUTE:ValueError")
    assert reasons == {"COMPUTE:ValueError": 1}
