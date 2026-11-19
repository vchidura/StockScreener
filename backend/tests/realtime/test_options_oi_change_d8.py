"""G106 D8 / G102 R51 matched OI change (Nov 19): `REALTIME_V3_OPTIONS_OI_CHANGE` OFF (default) = no SQL, no line, the
cycle result unchanged; RECORD = once per session, after the first window with a COMPLETE chain capture, one read-only
transaction comparing today's open interest (the capture just stored) with the previous session's close snapshot
(`rt.option_breadth_daily`, H1) per watched contract and per name, logged as `OPTIONS_OI_CHANGE_RECORD` lines and one
`OPTIONS_OI_CHANGE_RECORD_SESSION`; failures are retried at the next window (bounded) and never fail the cycle."""
from __future__ import annotations

import importlib
import json
from datetime import date, timedelta
from decimal import Decimal

import pytest

from realtime import advanced_options_oi_change as oi
from test_advanced_options_levels import (
    CUTOFF,
    RECEIPT,
    SESSION,
    Cursor,
    Lines,
    Provider,
    _attached_cycle,
    _service,
)

SWITCH = oi.OI_CHANGE_SETTING
PREVIOUS = SESSION - timedelta(days=1)
EXPIRY = date(2026, 11, 20)
WATCHED = ("O:SPY261120C00600000", "O:SPY261120P00590000", "O:SPY261120C00610000", "O:QQQ261120C00500000",
           "O:QQQ261120P00480000", "O:NVDA261120C00200000")


def watched_rows():
    """WATCHED_SQL rows: ticker, underlying, type, expiry, strike, oi today, volume today, matched, oi before, volume
    before. NVDA's contract is not in today's capture."""
    return [
        ("O:QQQ261120C00500000", "QQQ", "call", EXPIRY, Decimal("500.00000000"), 1200, 50, True, 1300, 400),
        ("O:QQQ261120P00480000", "QQQ", "put", EXPIRY, Decimal("480.00000000"), 2500, 90, False, None, None),
        ("O:SPY261120C00600000", "SPY", "call", EXPIRY, Decimal("600.00000000"), 12000, 3000, True, 8000, 9000),
        ("O:SPY261120C00610000", "SPY", "call", EXPIRY, Decimal("610.00000000"), 700, 10, True, 4000, 1000),
        ("O:SPY261120P00590000", "SPY", "put", EXPIRY, Decimal("590.00000000"), 30000, 2000, True, 29500, 5000),
    ]


def chain_rows():
    # underlying, type, contracts, matched, oi today (matched), oi before (matched), opened, closed, new
    return [("QQQ", "call", 500, 480, 900_000, 880_000, 3, 1, 0), ("QQQ", "put", 520, 500, 1_100_000, 1_090_000, 2, 0, 1),
            ("SPY", "call", 800, 790, 4_000_000, 3_900_000, 12, 4, 2), ("SPY", "put", 820, 800, 5_000_000, 4_950_000, 9, 2, 0)]


def top_rows():
    return [("QQQ", "O:QQQ261120C00505000", "call", EXPIRY, Decimal("505"), 9000, 4000),
            ("SPY", "O:SPY261120C00600000", "call", EXPIRY, Decimal("600"), 12000, 8000)]


def script(previous=PREVIOUS, *, watched=None, chain=None, top=None, error=None):
    answers = [
        ("max(session_date)", [(previous,)]),
        ("ANY(", watched if watched is not None else watched_rows()),
        ("GROUP BY current.underlying", chain if chain is not None else chain_rows()),
        ("row_number()", top if top is not None else top_rows()),
    ]
    if error is not None:
        answers.insert(0, ("ANY(", error))
    return answers


def record(lines, mode="RECORD"):
    return oi.OiChangeRecorder(mode, emit=lines.emit, fault=lines.fault)


def run(recorder, provider, *, session=SESSION, breadth=None, watched=WATCHED):
    return recorder.record_window(provider, session_date=session, equity_cutoff=CUTOFF, window_id="w-1",
                                  watched=watched, breadth={"SPY": {"state": "COMPLETE"}} if breadth is None else breadth)


# ---- the switch -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [(None, "OFF"), ("", "OFF"), ("off", "OFF"), (" record ", "RECORD")])
def test_mode_values(raw, expected):
    assert oi.oi_change_mode({} if raw is None else {SWITCH: raw}).value == expected


@pytest.mark.parametrize("raw", ["ON", "SHADOW", "1", "yes"])
def test_an_invalid_value_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SWITCH):
        oi.oi_change_mode({SWITCH: raw})
    with pytest.raises(ValueError, match=SWITCH):
        oi.start_oi_change(raw)
    with pytest.raises(ValueError, match=SWITCH):
        oi.parse_oi_change_mode(2)


def test_the_options_scheduler_refuses_to_start_on_an_invalid_value(monkeypatch):
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    script_module = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(SWITCH, "ON")
    with pytest.raises(ValueError, match=SWITCH):
        script_module.parse_args(argv)
    for good in ("", "OFF", "RECORD"):
        monkeypatch.setenv(SWITCH, good)
        assert script_module.parse_args(argv).continuous


# ---- the arithmetic -----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("today, previous, matched, expected", [
    (12000, 8000, True, "OPENED_LARGE"),     # +4000, +50%
    (31000, 30000, True, None),              # +1000 but only +3.3%
    (900, 0, True, None),                    # below the absolute floor
    (1000, 0, True, "OPENED_LARGE"),         # from nothing
    (700, 4000, True, "CLOSED_LARGE"),       # -3300, -82%
    (29000, 30000, True, None),              # -1000, -3.3%
    (2500, None, False, "NEW"),
    (900, None, False, None),
    (5000, None, True, None),                # a previous row without open interest: not comparable
    (None, 5000, True, None),
])
def test_classify(today, previous, matched, expected):
    assert oi.classify(today, previous, matched=matched) == expected


def test_contract_document_from_tuple_and_dict_rows():
    row = watched_rows()[2]
    document = oi.contract_document(row)
    assert document == {
        "contract": "O:SPY261120C00600000", "underlying": "SPY", "type": "C", "expiry": "2026-11-20", "strike": "600",
        "oi": 12000, "oi_prev": 8000, "change": 4000, "change_fraction": 0.5, "volume_prev": 9000,
        "opened_share": 0.444, "volume_today": 3000, "flag": "OPENED_LARGE", "matched": True}
    names = ("contract_ticker", "underlying", "contract_type", "expiration_date", "strike_price", "oi_today",
             "volume_today", "matched", "oi_previous", "volume_previous")
    assert oi.contract_document(dict(zip(names, row))) == document
    unmatched = oi.contract_document(watched_rows()[1])
    assert unmatched["oi_prev"] is None and unmatched["change"] is None and unmatched["flag"] == "NEW"


# ---- OFF ----------------------------------------------------------------------------------------------------------------

def test_off_reads_nothing_and_logs_nothing(monkeypatch, capsys):
    monkeypatch.delenv(SWITCH, raising=False)
    assert oi.start_oi_change(None) is None and oi.start_oi_change("OFF") is None
    lines = Lines()
    off = record(lines, "OFF")
    off.start()
    provider = Provider(Cursor(script()))
    assert run(off, provider) is None
    assert provider.transactions == [] and lines.lines == [] and lines.faults == []
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


# ---- RECORD -------------------------------------------------------------------------------------------------------------

def test_record_logs_each_name_and_the_session_from_one_read_only_transaction():
    lines = Lines()
    recorder = record(lines)
    recorder.start()
    assert lines.lines[0] == (oi.OI_CHANGE_SETTING_EVENT, {
        "applied": False, "large_change": 1000, "large_fraction": 0.25, "maximum_attempts": 3, "mode": "RECORD",
        "setting": SWITCH, "version": oi.VERSION})
    cursor = Cursor(script())
    provider = Provider(cursor)
    result = run(recorder, provider)
    assert provider.transactions == [("app", {"read_only": True})]
    statements = [statement for statement, _ in cursor.statements]
    assert statements[0] == "SET LOCAL statement_timeout = '20s'" and len(statements) == 5
    assert all(statement.startswith(("SET LOCAL", "SELECT")) for statement in statements)
    assert cursor.statements[1][1] == (SESSION,)
    assert cursor.statements[2][1] == (PREVIOUS, SESSION, sorted(WATCHED))
    assert cursor.statements[3][1] == {"previous": PREVIOUS, "session": SESSION, "change": 1000, "fraction": 0.25,
                                       "top": 3}
    assert result == {"status": "RECORDED", "mode": "RECORD", "applied": False, "previous_session": str(PREVIOUS),
                      "watched": 6, "matched": 4, "flagged": 3, "seconds": result["seconds"]}

    named = {document["underlying"]: document for event, document in lines.lines if event == oi.OI_CHANGE_RECORD_EVENT}
    assert sorted(named) == ["QQQ", "SPY"]
    spy = named["SPY"]
    assert spy["previous_session"] == str(PREVIOUS) and spy["days_back"] == 1 and spy["session"] == str(SESSION)
    assert spy["watched"] == 3 and spy["matched"] == 3
    assert spy["flagged"] == {"OPENED_LARGE": 1, "CLOSED_LARGE": 1, "NEW": 0}
    assert spy["watched_call_change"] == 4000 - 3300 and spy["watched_put_change"] == 500
    assert [item["flag"] for item in spy["contracts"]] == ["OPENED_LARGE", "CLOSED_LARGE", None]
    assert spy["chain"] == {
        "contracts": 1620, "matched": 1590, "call_oi": 4_000_000, "call_oi_prev": 3_900_000, "call_change": 100_000,
        "put_oi": 5_000_000, "put_oi_prev": 4_950_000, "put_change": 50_000, "opened_large": 21, "closed_large": 6,
        "new_large": 2, "top_opened": [{"contract": "O:SPY261120C00600000", "type": "C", "expiry": "2026-11-20",
                                        "strike": "600", "oi": 12000, "oi_prev": 8000, "change": 4000}]}
    assert spy["applied"] is False and spy["execution_permission"] is False and spy["window_id"] == "w-1"
    assert named["QQQ"]["flagged"] == {"OPENED_LARGE": 0, "CLOSED_LARGE": 0, "NEW": 1}

    (session,) = [document for event, document in lines.lines if event == oi.OI_CHANGE_SESSION_EVENT]
    assert session["status"] == "RECORDED" and session["watched"] == 6 and session["in_capture"] == 5
    assert session["not_in_capture"] == ["O:NVDA261120C00200000"] and session["not_in_capture_count"] == 1
    assert session["flagged"] == {"OPENED_LARGE": 1, "CLOSED_LARGE": 1, "NEW": 1}
    assert session["flagged_contracts"] == {"CLOSED_LARGE": ["O:SPY261120C00610000"], "NEW": ["O:QQQ261120P00480000"],
                                            "OPENED_LARGE": ["O:SPY261120C00600000"]}
    assert session["chain_contracts"] == 2640 and session["chain_matched"] == 2570 and session["attempt"] == 1
    assert lines.events()[-1] == oi.OI_CHANGE_SESSION_EVENT and lines.faults == []


def test_once_per_session_and_again_on_the_next_session():
    lines = Lines()
    recorder = record(lines)
    provider = Provider(Cursor(script()))
    assert run(recorder, provider)["status"] == "RECORDED"
    assert run(recorder, provider) is None and len(provider.transactions) == 1
    assert run(recorder, provider, session=SESSION + timedelta(days=1))["status"] == "RECORDED"
    assert len(provider.transactions) == 2


def test_a_window_without_a_complete_capture_waits_without_reading():
    lines = Lines()
    recorder = record(lines)
    provider = Provider(Cursor(script()))
    assert run(recorder, provider, breadth={"status": "FAILED"}) is None
    assert run(recorder, provider, breadth={"SPY": {"state": "PARTIAL"}}) is None
    assert provider.transactions == [] and lines.lines == []
    assert run(recorder, provider, breadth={"SPY": {"state": "COMPLETE"}})["status"] == "RECORDED"


def test_no_previous_session_is_recorded_once():
    lines = Lines()
    recorder = record(lines)
    cursor = Cursor(script(previous=None))
    provider = Provider(cursor)
    assert run(recorder, provider)["status"] == "NO_PREVIOUS_SESSION"
    assert len(cursor.statements) == 2
    assert lines.events() == [oi.OI_CHANGE_SESSION_EVENT]
    assert lines.lines[0][1]["status"] == "NO_PREVIOUS_SESSION" and lines.lines[0][1]["previous_session"] is None
    assert run(recorder, provider) is None


def test_no_watched_contracts_still_records_the_chain():
    lines = Lines()
    cursor = Cursor(script())
    result = run(record(lines), Provider(cursor), watched=())
    assert result["status"] == "RECORDED" and result["watched"] == 0
    assert not any("ANY(" in statement for statement, _ in cursor.statements)
    named = {document["underlying"]: document for event, document in lines.lines if event == oi.OI_CHANGE_RECORD_EVENT}
    assert named["SPY"]["contracts"] == [] and named["SPY"]["chain"]["opened_large"] == 21


@pytest.mark.parametrize("error, reason", [(RuntimeError("canceling statement due to statement timeout"), "RuntimeError"),
                                           (type("PoolError", (Exception,), {})("pool exhausted"), "NO_CONNECTION")])
def test_a_failure_is_one_line_and_one_fault_retried_at_most_three_times(error, reason):
    lines = Lines()
    recorder = record(lines)
    failing = Provider(Cursor(script(error=error)))
    for attempt in (1, 2, 3):
        result = run(recorder, failing)
        assert result["status"] == "FAILED" and result["reason"] == reason and result["attempt"] == attempt
    assert run(recorder, failing) is None and len(failing.transactions) == 3
    failed = [document for event, document in lines.lines if event == oi.OI_CHANGE_FAILED_EVENT]
    assert [document["retry"] for document in failed] == [True, True, False]
    assert [fault["action"] for fault in lines.faults] == ["RETRY_NEXT_WINDOW", "RETRY_NEXT_WINDOW", "SKIP_SESSION"]
    assert {fault["kind"] for fault in lines.faults} == {"OPTIONS_OI_CHANGE_RECORD_FAILED"}


def test_a_failure_then_a_success_on_the_next_window():
    lines = Lines()
    recorder = record(lines)
    assert run(recorder, Provider(error=RuntimeError("no connection")))["status"] == "FAILED"
    result = run(recorder, Provider(Cursor(script())))
    assert result["status"] == "RECORDED"
    (session,) = [document for event, document in lines.lines if event == oi.OI_CHANGE_SESSION_EVENT]
    assert session["attempt"] == 2


def test_an_emit_that_raises_is_a_failed_attempt_and_never_escapes():
    def broken(event, document):
        raise OSError("pipe closed")

    recorder = oi.OiChangeRecorder("RECORD", emit=broken, fault=lambda **_: None)
    recorder.start()
    assert run(recorder, Provider(Cursor(script())))["status"] == "FAILED"


def test_every_line_stays_under_the_docker_limit():
    from realtime.advanced_options_levels import LINE_BYTE_LIMIT, render_line

    many = [(f"O:SPY261120C{index:08d}", "SPY", "call", EXPIRY, Decimal(index), 5000 + index, 10, True, 1000, 100)
            for index in range(400)]
    lines = Lines()
    run(record(lines), Provider(Cursor(script(watched=many))), watched=tuple(row[0] for row in many))
    for event, document in lines.lines:
        assert len(render_line(event, document).encode("utf-8")) <= LINE_BYTE_LIMIT


# ---- the scheduler hook -----------------------------------------------------------------------------------------------

def test_off_leaves_the_cycle_byte_identical(monkeypatch, capsys):
    baseline, base_provider, base_cursor = _attached_cycle(monkeypatch)
    off, off_provider, off_cursor = _attached_cycle(monkeypatch, oi_change=oi.start_oi_change("OFF"))
    assert json.dumps(off, sort_keys=True) == json.dumps(baseline, sort_keys=True) and "oi_change" not in off
    assert off_cursor.statements == base_cursor.statements and off_provider.transactions == base_provider.transactions
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_record_adds_one_key_and_one_read_only_transaction_once(monkeypatch):
    baseline, base_provider, base_cursor = _attached_cycle(monkeypatch)
    lines = Lines()
    recorder = record(lines)
    result, provider, cursor = _attached_cycle(monkeypatch, script=script(), oi_change=recorder)
    assert {key: value for key, value in result.items() if key != "oi_change"} == baseline
    assert result["oi_change"]["status"] == "RECORDED"
    assert provider.transactions[len(base_provider.transactions):] == [("app", {"read_only": True})]
    assert cursor.statements[:len(base_cursor.statements)] == base_cursor.statements
    watched = next(params for statement, params in cursor.statements if "ANY(" in statement)
    assert watched[2] == sorted(("O:SPY261127C00100000", "O:SPY261127P00100000"))
    again, _provider, _cursor = _attached_cycle(monkeypatch, script=script(), oi_change=recorder)
    assert "oi_change" not in again and again == baseline
    session = [document for event, document in lines.lines if event == oi.OI_CHANGE_SESSION_EVENT]
    assert len(session) == 1 and session[0]["equity_cutoff"] == RECEIPT[1].isoformat()


def test_oi_change_runs_after_the_levels_and_a_raising_recorder_never_fails_the_cycle(monkeypatch):
    order = []

    class Levels:
        def record_window(self, provider, **kwargs):
            order.append("levels")
            return {"status": "RECORDED"}

    class Broken:
        def record_window(self, provider, **kwargs):
            order.append("oi_change")
            assert set(kwargs) == {"session_date", "equity_cutoff", "window_id", "watched", "breadth"}
            raise RuntimeError("unexpected")

    result, _provider, _cursor = _attached_cycle(monkeypatch, levels=Levels(), oi_change=Broken())
    assert order == ["levels", "oi_change"] and result["status"] == "ATTACHED"
    assert result["oi_change"] == {"status": "FAILED", "reason": "RuntimeError: unexpected"}


def test_the_public_cycle_passes_the_recorder_through(monkeypatch):
    from realtime import advanced_options_continuous_scheduler as scheduler

    seen = {}

    def fake_cycle(provider, **kwargs):
        seen.update(kwargs)
        return {"status": "ATTACHED"}

    monkeypatch.setattr(scheduler, "_run_schedule_cycle", fake_cycle)
    provider = Provider(Cursor([("pg_try_advisory_xact_lock", [(True,)])]))
    marker = object()
    scheduler.run_continuous_options_schedule_cycle(provider, session_date=SESSION, api_key="k", oi_change=marker)
    assert seen["oi_change"] is marker
    scheduler.run_continuous_options_schedule_cycle(provider, session_date=SESSION, api_key="k")
    assert seen["oi_change"] is None


# ---- the scheduler service --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("value", [None, "", "OFF"])
def test_the_service_off_passes_no_recorder_and_logs_no_line(monkeypatch, capsys, value):
    if value is None:
        monkeypatch.delenv(SWITCH, raising=False)
    else:
        monkeypatch.setenv(SWITCH, value)
    monkeypatch.delenv("REALTIME_V3_OPTIONS_LEVELS_STABLE", raising=False)
    calls = _service(monkeypatch, None)
    assert calls[0]["oi_change"] is None
    assert "OPTIONS_OI_CHANGE" not in capsys.readouterr().err


def test_the_service_in_record_starts_one_recorder(monkeypatch, capsys):
    monkeypatch.setenv(SWITCH, "RECORD")
    monkeypatch.delenv("REALTIME_V3_OPTIONS_LEVELS_STABLE", raising=False)
    calls = _service(monkeypatch, None)
    assert isinstance(calls[0]["oi_change"], oi.OiChangeRecorder) and calls[0]["levels"] is None
    assert capsys.readouterr().err.count("OPTIONS_OI_CHANGE_SETTING ") == 1
