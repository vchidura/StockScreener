"""G106 D11 V-TERM (Nov 18 night): term-structure inversion (7d / 30d ATM IV > 1.3) as a RECORD observation lane on
the Options window, behind REALTIME_V3_VTERM (OFF / RECORD). Input = the A2 IV summary read. OFF reads nothing and
logs nothing; RECORD logs one VTERM_RECORD line per window, bounded by 2 s, and never raises (VTERM_RECORD_FAILED)."""
import inspect
import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import realtime.advanced_options_p8_window as window_module
from realtime.advanced_options_vterm import (
    VTERM_INVERSION_RATIO,
    VTERM_RECORD_EVENT,
    VTERM_RECORD_FAILED_EVENT,
    VTERM_SETTING,
    VTermMode,
    prepare_vterm,
    read_term_summaries_cursor,
    record_vterm,
    render_vterm_line,
    term_entry,
    vterm_document,
    vterm_mode,
)
from realtime.options_iv_summary_read import compute_iv_summary

UTC = timezone.utc
SESSION = date(2026, 11, 16)
CUTOFF = datetime(2026, 11, 16, 18, 0, tzinfo=UTC)
OBSERVED = CUTOFF - timedelta(minutes=3)


def _summary(front=0.948, back=0.640, *, front_method="TOTAL_VARIANCE_INTERPOLATION", available=True, reason=None):
    if not available:
        return {"available": False, "reason": reason}
    return {
        "available": True, "observed_at": OBSERVED.isoformat(), "contracts": 400,
        "tenors": [
            {"days": 7, "atm_iv": front, "atm_method": front_method, "quality_reasons": []},
            {"days": 30, "atm_iv": back, "atm_method": "TOTAL_VARIANCE_INTERPOLATION", "quality_reasons": ["X"]},
            {"days": 60, "atm_iv": 0.5, "atm_method": "FLAT_EXTRAPOLATION", "quality_reasons": []},
        ],
    }


class _Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)


class _Provider:
    def __init__(self):
        self.transactions = []
        self.cursor = _Cursor()

    @contextmanager
    def transaction(self, role, *, read_only=False, isolation_level=None, **kwargs):
        self.transactions.append((read_only, isolation_level))
        yield SimpleNamespace(cursor=self.cursor)


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    monkeypatch.delenv(VTERM_SETTING, raising=False)


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert vterm_mode({}) is VTermMode.OFF
    assert vterm_mode({VTERM_SETTING: " "}) is VTermMode.OFF
    assert vterm_mode({VTERM_SETTING: "record"}) is VTermMode.RECORD
    assert prepare_vterm({VTERM_SETTING: " Record "}) is VTermMode.RECORD
    for bad in ("ON", "YES", "1", "RECORDED"):
        with pytest.raises(ValueError, match=VTERM_SETTING):
            vterm_mode({VTERM_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(window_module.__file__).parents[1] / "scripts"
              / "run_advanced_options_v3_production.py").read_text(encoding="utf-8")
    start = source.index("if kind is AdvancedOptionsProcessKind.WINDOW:")
    assert "prepare_vterm()" in source[start:source.index("provider = None", start)]


def test_window_records_after_its_run_with_the_universe():
    source = inspect.getsource(window_module.build_and_run_p8_serial_window)
    run = source.index("report = orchestrator.run_window(binding.window_id)")
    call = source.index("record_vterm(")
    assert run < call < source.index("if started.enabled != frozenset(runners):")
    assert "underlyings=tuple(control.universe.option_underlyings)" in source[call:call + 300]


# --- the per-name entry ----------------------------------------------------------------------------------------------

def test_ratio_and_inversion_at_the_threshold():
    nvda = term_entry("NVDA", _summary(0.948, 0.640), cutoff=CUTOFF)
    assert nvda["status"] == "OK" and nvda["ratio"] == round(0.948 / 0.640, 4) and nvda["inverted"] is True
    assert nvda["capture_age_s"] == 180.0 and nvda["method_7d"] == "TOTAL_VARIANCE_INTERPOLATION"
    assert nvda["quality_30d"] == ["X"]
    spy = term_entry("SPY", _summary(0.222, 0.2), cutoff=CUTOFF)
    assert spy["ratio"] == 1.11 and spy["inverted"] is False
    edge = term_entry("EDGE", _summary(1.3, 1.0), cutoff=CUTOFF)
    assert edge["ratio"] == VTERM_INVERSION_RATIO and edge["inverted"] is False  # strictly above 1.3


def test_unavailable_names_say_why():
    assert term_entry("A", _summary(available=False, reason="NO_CHAIN_CAPTURE"), cutoff=CUTOFF) == {
        "underlyer": "A", "status": "NO_CHAIN_CAPTURE", "inverted": False}
    assert term_entry("B", None, cutoff=CUTOFF)["status"] == "NO_SUMMARY"
    front_missing = term_entry("C", _summary(front=None), cutoff=CUTOFF)
    assert front_missing["status"] == "TENOR_UNAVAILABLE:7" and front_missing["ratio"] is None
    both = term_entry("D", _summary(front=None, back=None), cutoff=CUTOFF)
    assert both["status"] == "TENOR_UNAVAILABLE:7|30"
    flat = term_entry("E", _summary(front_method="FLAT_EXTRAPOLATION"), cutoff=CUTOFF)
    assert flat["status"] == "OK" and flat["method_7d"] == "FLAT_EXTRAPOLATION"


def test_entry_from_a_real_a2_computation():
    # A synthetic chain where the near expiry is priced at a higher IV than the far one (an inverted term).
    rows = []
    for expiry, iv in ((date(2026, 11, 20), 1.2), (date(2026, 12, 18), 0.6), (date(2027, 1, 15), 0.55)):
        for strike in (95, 100, 105):
            for kind, delta in (("call", 0.5 - (strike - 100) / 20), ("put", -0.5 - (strike - 100) / 20)):
                rows.append({"contract_type": kind, "expiration_date": expiry, "strike_price": strike,
                             "day_volume": 10, "open_interest": 100, "bid": 1.0, "ask": 1.1,
                             "implied_volatility": iv, "delta": delta, "local_iv": iv, "local_delta": delta,
                             "spot": 100, "underlying_price": 100, "risk_free_rate": 0.04, "dividend_yield": 0.0,
                             "observed_at": OBSERVED})
    summary = compute_iv_summary(rows, underlyer="TEST", session_date=SESSION, observed_at=OBSERVED,
                                 source="CURRENT_LATEST_CAPTURE")
    entry = term_entry("TEST", summary, cutoff=CUTOFF)
    assert entry["status"] == "OK" and entry["atm_iv_7d"] > entry["atm_iv_30d"] and entry["inverted"] is True


# --- the read --------------------------------------------------------------------------------------------------------

def test_read_is_per_name_sorted_with_savepoints_and_one_failure_is_that_names():
    cursor = _Cursor()
    calls = []

    def read(cursor, *, underlyer, available_by, session_date):
        calls.append((underlyer, available_by, session_date))
        if underlyer == "BAD":
            raise RuntimeError("boom")
        return _summary()

    result = read_term_summaries_cursor(cursor, session_date=SESSION, cutoff=CUTOFF,
                                        underlyings=("spy", "BAD", "NVDA", "SPY", ""), read=read)
    assert [call[0] for call in calls] == ["BAD", "NVDA", "SPY"]
    assert all(call[1] == CUTOFF and call[2] == SESSION for call in calls)
    assert result["BAD"] == "READ:RuntimeError" and result["NVDA"]["available"] is True
    assert cursor.statements.count("SAVEPOINT vterm_name") == 3
    assert "ROLLBACK TO SAVEPOINT vterm_name" in cursor.statements
    assert cursor.statements.count("RELEASE SAVEPOINT vterm_name") == 2
    assert all(statement.startswith("SET LOCAL statement_timeout = '") for statement in cursor.statements[::3])


def test_read_stops_at_the_time_budget():
    cursor = _Cursor()
    ticks = iter([0.0, 0.0, 1.5, 2.1, 2.2, 2.3])
    result = read_term_summaries_cursor(cursor, session_date=SESSION, cutoff=CUTOFF, underlyings=("A", "B", "C", "D"),
                                        read=lambda *_a, **_k: _summary(), clock=lambda: next(ticks))
    assert result["A"]["available"] and result["B"]["available"]
    assert result["C"] == result["D"] == "TIME_BUDGET"
    # B's statements may use only what is left of the budget (0.5 s).
    assert "SET LOCAL statement_timeout = '500ms'" in cursor.statements


# --- the record ------------------------------------------------------------------------------------------------------

def test_off_reads_nothing_and_logs_nothing(monkeypatch):
    provider, lines = _Provider(), []
    for value in (None, "OFF", " "):
        if value is None:
            monkeypatch.delenv(VTERM_SETTING, raising=False)
        else:
            monkeypatch.setenv(VTERM_SETTING, value)
        assert record_vterm(provider, session_date=SESSION, cutoff=CUTOFF, underlyings=("NVDA",),
                            emit=lambda *args: lines.append(args)) is None
    assert provider.transactions == [] and lines == []


def test_record_logs_one_line_per_window_in_a_snapshot_read(monkeypatch):
    monkeypatch.setenv(VTERM_SETTING, "RECORD")
    provider, lines = _Provider(), []
    summaries = {"NVDA": _summary(0.948, 0.640), "SPY": _summary(0.222, 0.2),
                 "TSLA": _summary(available=False, reason="NO_CHAIN_CAPTURE")}
    result = record_vterm(provider, session_date=SESSION, cutoff=CUTOFF, underlyings=("SPY", "NVDA", "TSLA"),
                          window_id="w1", emit=lambda event, document: lines.append((event, document)),
                          read=lambda cursor, *, underlyer, **_: summaries[underlyer])
    assert provider.transactions == [(True, "REPEATABLE READ")]
    ((event, document),) = lines
    assert event == VTERM_RECORD_EVENT
    assert document["mode"] == "RECORD" and document["applied"] is False and document["execution_permission"] is False
    assert document["inverted"] == ["NVDA"] and document["available"] == 2 and document["underlyings"] == 3
    assert document["unavailable"] == {"NO_CHAIN_CAPTURE": 1} and document["budget_exhausted"] is False
    assert [row["underlyer"] for row in document["names"]] == ["NVDA", "SPY", "TSLA"]
    assert document["threshold"] == 1.3 and document["window_id"] == "w1" and document["cutoff"] == CUTOFF.isoformat()
    assert result == {"status": "RECORDED", "available": 2, "inverted": ["NVDA"]}


def test_a_failure_is_one_failed_line_and_never_raises(monkeypatch):
    monkeypatch.setenv(VTERM_SETTING, "RECORD")

    class _Broken:
        @contextmanager
        def transaction(self, *args, **kwargs):
            raise RuntimeError("database gone")
            yield

    lines = []
    result = record_vterm(_Broken(), session_date=SESSION, cutoff=CUTOFF, underlyings=("NVDA",),
                          emit=lambda event, document: lines.append((event, document)))
    assert result == {"status": "RECORD_FAILED"}
    ((event, document),) = lines
    assert event == VTERM_RECORD_FAILED_EVENT and document["status"] == "RECORD_FAILED"
    assert "database gone" in document["reason"]

    def broken_emit(event, document):
        raise OSError("stderr closed")

    assert record_vterm(_Provider(), session_date=SESSION, cutoff=CUTOFF, underlyings=("NVDA",), emit=broken_emit,
                        read=lambda *_a, **_k: _summary()) == {"status": "RECORD_FAILED"}


def test_an_invalid_value_inside_a_window_records_nothing(monkeypatch):
    monkeypatch.setenv(VTERM_SETTING, "BOGUS")
    provider = _Provider()
    assert record_vterm(provider, session_date=SESSION, cutoff=CUTOFF, underlyings=("NVDA",)) is None
    assert provider.transactions == []


def test_line_stays_under_the_limit(capsys):
    summaries = {f"N{index:03d}": _summary() for index in range(400)}
    document = vterm_document(summaries, session_date=SESSION, cutoff=CUTOFF, window_id="w", read_seconds=0.1,
                              seconds=0.2)
    line = render_vterm_line(VTERM_RECORD_EVENT, document)
    assert len(line.encode("utf-8")) <= 15000
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["truncated"] is True and payload["underlyings"] == 400 and len(payload["inverted"]) == 400
    small = render_vterm_line(VTERM_RECORD_EVENT, vterm_document({"A": _summary()}, session_date=SESSION,
                                                                cutoff=CUTOFF, window_id=None, read_seconds=0,
                                                                seconds=0))
    assert "truncated" not in json.loads(small.split(" ", 1)[1])
