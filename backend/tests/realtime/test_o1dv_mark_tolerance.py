"""G106 AL2 follow-up (Nov 19): O1 debit-vertical leg marks with a 5 s / 15 s quote-age tolerance, RECORD only
(REALTIME_V3_O1DV_MARK_TOLERANCE OFF / RECORD).

RECORD builds every evaluated O1 event's vertical again with journal quote marks (the barrier heads of the contracts the
1 s mark rule left out) and logs one O1_DEBIT_VERTICAL_MARKS_RECORD line per window; packages, identities and the
O1_DEBIT_VERTICAL_RECORD line stay exactly as without it. OFF does nothing at all.
"""
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

import realtime.advanced_options_native_loader as loader
import realtime.advanced_options_o1dv_mark_tolerance as tolerance
from realtime.advanced_options_o1_debit_vertical import (
    O1_DEBIT_VERTICAL_SETTING,
    O1DebitVerticalOutcome,
    O1VerticalInputs,
    is_o1_debit_vertical_package,
    o1_debit_vertical_policy,
    select_debit_vertical,
)
from realtime.advanced_options_o1dv_mark_tolerance import (
    O1DV_MARK_TOLERANCE_SETTING,
    O1DV_MARKS_RECORD_EVENT,
    O1DVMarkToleranceMode,
    build_tolerance_window,
    current_window,
    o1dv_mark_tolerance_mode,
    o1dv_marks_record,
    publish_o1dv_window_quotes,
    quote_mark,
)
from realtime.advanced_options_o3_inputs import LegQuote, WatchedContract
from realtime.advanced_options_o3_producer import LegMark

UTC = timezone.utc
COHORT = UUID("b096f5b7-2371-5522-925d-cf4db3912b9c")
MATRIX = UUID("11111111-1111-4111-8111-111111111111")
FLOW = UUID("22222222-2222-4222-8222-222222222222")
SECURITY = UUID("33333333-3333-4333-8333-333333333333")
GENERATION = UUID("44444444-4444-4444-8444-444444444444")
STREAM = UUID("55555555-5555-4555-8555-555555555555")
EVENT = uuid5(NAMESPACE_URL, "o1dv-mark-tolerance-event")
SESSION = date(2026, 11, 19)
CUTOFF = datetime(2026, 11, 19, 16, 35, tzinfo=UTC)
EXPIRY = date(2026, 11, 27)
SPOT = Decimal("340")


def _contract(instrument_id, strike, side="C"):
    ticker = f"O:AAPL{EXPIRY:%y%m%d}{side}{int(Decimal(strike) * 1000):08d}"
    return WatchedContract(instrument_id=instrument_id, contract_id=instrument_id - 8900,
                           catalog_version_id=instrument_id + 1000, contract_ticker=ticker, underlying="AAPL",
                           call_put=side, strike=Decimal(strike), expiration=EXPIRY, multiplier=100)


LONG_CALL = _contract(9001, "345")
NEAR_CALL = _contract(9005, "348")     # 3 / 340 = 0.88%: the nearest eligible short
SHORT_CALL = _contract(9003, "350")    # 5 / 340 = 1.47%
CONTRACTS = (LONG_CALL, NEAR_CALL, SHORT_CALL)


def _mark(instrument_id, value, at=CUTOFF - timedelta(milliseconds=100)):
    value = Decimal(value)
    return LegMark(evidence_id=uuid5(NAMESPACE_URL, f"mark:{instrument_id}"), instrument_id=instrument_id,
                   bid=value - Decimal("0.05"), ask=value + Decimal("0.05"), mark=value, market_time=at,
                   observed_time=at + timedelta(milliseconds=20), payload_sha256="c" * 64)


def _quotes():
    return {row.instrument_id: LegQuote(row.instrument_id, None, None, None, 500, 1000) for row in CONTRACTS}


def _row(bid="2.10", ask="2.30", age_seconds=3.0, **overrides):
    row = {
        "source_session": SESSION, "source_sequence": 7, "event_time_raw": 1, "event_time_unit": "ns",
        "event_at": CUTOFF - timedelta(seconds=age_seconds), "received_at": CUTOFF - timedelta(seconds=age_seconds),
        "bid": None if bid is None else Decimal(bid), "ask": Decimal(ask), "bid_size": 5, "ask_size": 7, "bid_exchange": 300,
        "ask_exchange": 301, "quality": "GOOD", "reason_codes": [], "session_id": str(STREAM),
        "journal_generation": str(GENERATION), "shard_id": 0, "source_offset": 99, "payload_sha256": "d" * 64,
    }
    row.update(overrides)
    return row


def _mark_of(row, tolerance_seconds=5):
    return quote_mark(1, row, cutoff=CUTOFF, tolerance_seconds=tolerance_seconds, journal_generation=GENERATION,
                      stream_session_ids=(STREAM,), session_date=SESSION)


SOURCE_FLOW = SimpleNamespace(source_flow_id=FLOW, source_flow_sha256="a" * 64, journal_generation=GENERATION,
                              stream_session_ids=(STREAM,))


def _publish(quotes, *, marked=(), selected=(9001, 9005, 9003), mode=O1DVMarkToleranceMode.RECORD, status="OK",
             read=None):
    return publish_o1dv_window_quotes(
        mode=mode, cohort_id=COHORT, cutoff=CUTOFF,
        cutoff_read=None if quotes is None else SimpleNamespace(quotes=quotes), cutoff_status=status,
        read=read or (lambda: (_ for _ in ()).throw(AssertionError("no second read"))),
        selected_ids=selected, marked_ids=marked, source_flow=SOURCE_FLOW, session_date=SESSION,
    )


@pytest.fixture(autouse=True)
def _clean_window():
    tolerance._set_window(None)
    yield
    tolerance._set_window(None)


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert o1dv_mark_tolerance_mode({}) is O1DVMarkToleranceMode.OFF
    assert o1dv_mark_tolerance_mode({O1DV_MARK_TOLERANCE_SETTING: " "}) is O1DVMarkToleranceMode.OFF
    assert o1dv_mark_tolerance_mode({O1DV_MARK_TOLERANCE_SETTING: "off"}) is O1DVMarkToleranceMode.OFF
    assert o1dv_mark_tolerance_mode({O1DV_MARK_TOLERANCE_SETTING: " Record "}) is O1DVMarkToleranceMode.RECORD
    for bad in ("ON", "YES", "1", "5", "RECORDED"):
        with pytest.raises(ValueError, match=O1DV_MARK_TOLERANCE_SETTING):
            o1dv_mark_tolerance_mode({O1DV_MARK_TOLERANCE_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(loader.__file__).parents[1] / "scripts" / "run_advanced_options_v3_production.py").read_text(
        encoding="utf-8")
    start = source.index("if kind is AdvancedOptionsProcessKind.WINDOW:")
    assert "o1dv_mark_tolerance_mode()" in source[start:start + 1200]


def test_window_publishes_after_its_marks_and_before_the_native_loader():
    source = (Path(loader.__file__).parent / "advanced_options_p8_window.py").read_text(encoding="utf-8")
    publish = source.index("publish_o1dv_window_quotes(\n")
    assert source.index("marked = {row.instrument_id for row in marks}") < publish
    assert publish < source.index("native_candidate = build_advanced_options_native_input_plan(")


# --- leg marks from the barrier heads --------------------------------------------------------------------------------

def test_a_two_sided_head_within_the_tolerance_is_a_midpoint_mark():
    mark = _mark_of(_row(age_seconds=3.25))
    assert mark.mark == Decimal("2.20") and (mark.bid, mark.ask) == (Decimal("2.10"), Decimal("2.30"))
    assert mark.market_time == CUTOFF - timedelta(seconds=3.25) and mark.age_ms == 3250
    assert _mark_of(_row(age_seconds=0)).age_ms == 0


def test_unusable_heads_have_stable_reasons():
    assert _mark_of(None) == "NO_QUOTE_AT_BARRIER"
    assert _mark_of(_row(age_seconds=5.001)) == "OLDER_THAN_TOLERANCE"
    assert _mark_of(_row(age_seconds=14), tolerance_seconds=15) != "OLDER_THAN_TOLERANCE"
    assert _mark_of(_row(age_seconds=-0.001)) == "AFTER_CUTOFF"
    assert _mark_of(_row(bid=None)) == "ONE_SIDED_OR_INVALID"
    assert _mark_of(_row(quality="ONE_SIDED")) == "ONE_SIDED_OR_INVALID"
    assert _mark_of(_row(reason_codes=["X"])) == "ONE_SIDED_OR_INVALID"
    assert _mark_of(_row(bid_exchange=None)) == "ONE_SIDED_OR_INVALID"
    assert _mark_of(_row(bid="0")) == "CROSSED_OR_NONPOSITIVE"
    assert _mark_of(_row(bid="2.40", ask="2.30")) == "CROSSED_OR_NONPOSITIVE"
    assert _mark_of(_row(ask_size=0)) == "CROSSED_OR_NONPOSITIVE"
    assert _mark_of(_row(journal_generation=str(FLOW))) == "GENERATION_MISMATCH"
    assert _mark_of(_row(session_id=str(FLOW))) == "SESSION_MISMATCH"
    assert _mark_of(_row(source_session=date(2026, 11, 18))) == "SOURCE_DATE_MISMATCH"
    assert _mark_of(_row(event_at=None)) == "NO_EVENT_TIME"
    assert _mark_of({**_row(), "bid": "x"}).startswith("INVALID_ROW:")


def test_only_contracts_the_marks_left_out_get_a_tolerance_mark():
    quotes = {9001: _row(age_seconds=3), 9005: _row(age_seconds=10), 9003: _row(age_seconds=0.5)}
    window = build_tolerance_window(cohort_id=COHORT, cutoff=CUTOFF, status="OK", quotes=quotes,
                                    unmarked_ids=(9001, 9005), journal_generation=GENERATION,
                                    stream_session_ids=(STREAM,), session_date=SESSION)
    assert set(window.marks[5]) == {9001} and set(window.marks[15]) == {9001, 9005}
    assert dict(window.quote_reasons[5]) == {"OLDER_THAN_TOLERANCE": 1} and not window.quote_reasons[15]
    assert window.unmarked_ids == frozenset({9001, 9005})


def test_publish_uses_the_windows_read_and_reads_only_when_marks_at_cutoff_is_off():
    window = _publish({9001: _row()}, marked=(9003,))
    assert current_window(CUTOFF, COHORT) is window and window.unmarked_ids == frozenset({9001, 9005})
    assert set(window.marks[5]) == {9001} and window.status == "OK"
    assert current_window(CUTOFF + timedelta(minutes=5)) is None
    assert current_window(CUTOFF, uuid5(NAMESPACE_URL, "other cohort")) is None
    # REALTIME_V3_MARKS_AT_CUTOFF=OFF: no read was made, this switch makes it.
    reads = []
    window = _publish(None, read=lambda: (reads.append(1), (SimpleNamespace(quotes={9005: _row()}), "OK"))[1])
    assert reads == [1] and set(window.marks[5]) == {9005}
    # The window's read failed: no second read, the status is in the line.
    window = _publish(None, status="MARKS_JOURNAL_UNAVAILABLE:JOURNAL_FILE_MISSING")
    assert window.status == "MARKS_JOURNAL_UNAVAILABLE:JOURNAL_FILE_MISSING" and window.marks[5] == {}
    # Never raises.
    window = _publish(None, read=lambda: 1 / 0)
    assert window.status.startswith("PUBLISH_FAILED:ZeroDivisionError")
    # OFF: nothing kept.
    assert _publish({9001: _row()}, mode=O1DVMarkToleranceMode.OFF) is None
    assert current_window(CUTOFF) is None


# --- _load_o1: rebuilt verticals, packages unchanged ------------------------------------------------------------------

def _flow_row(event_id=EVENT, contract=LONG_CALL):
    return {
        "event_id": str(event_id), "event_time": CUTOFF - timedelta(minutes=2), "contract_id": contract.contract_id,
        "contract_ticker": contract.contract_ticker, "underlyer": "AAPL", "cumulative_volume_after": 5000,
        "prior_open_interest": 1000, "open_interest_as_of": date(2026, 11, 18), "source_flow_id": str(FLOW),
        "source_offset": 7, "created_at": CUTOFF - timedelta(minutes=2) + timedelta(seconds=1),
        "underlying_security_id": str(SECURITY), "call_put": contract.call_put, "expiration": contract.expiration,
        "flow_policy_sha256": "e" * 64, "instrument_id": contract.instrument_id,
        "version_id": contract.catalog_version_id, "strike": contract.strike, "multiplier": Decimal("100"),
        "quote_bid": Decimal("2.10"), "quote_ask": Decimal("2.30"),
        "quote_time": CUTOFF - timedelta(minutes=2, seconds=1), "profiles": ["A"],
    }


class _Cursor:
    def __init__(self, rows):
        self.rows, self.result, self.statements = rows, [], []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "FROM rt.option_flow_events" in sql:
            self.result = list(self.rows)
        elif sql.startswith("SELECT event_id::text,candidate_id::text FROM rt.option_packages"):
            self.result = []
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def fetchall(self):
        return self.result


def _plan(unmarked=()):
    return SimpleNamespace(
        control_plane=SimpleNamespace(cohort_id=COHORT, session_date=SESSION),
        source_flow=SOURCE_FLOW,
        rolling_state=SimpleNamespace(committed_offset=10, state_id=MATRIX),
        reference_state=SimpleNamespace(known_at=CUTOFF - timedelta(hours=6)),
        cutoff=CUTOFF, unmarked_instrument_ids=tuple(unmarked),
    )


def _inputs(marked):
    marks = {9001: _mark(9001, "2.20"), 9005: _mark(9005, "1.30"), 9003: _mark(9003, "0.80")}
    return O1VerticalInputs(contracts=CONTRACTS, by_ticker={row.contract_ticker: row for row in CONTRACTS},
                            marks={key: value for key, value in marks.items() if key in marked}, quotes=_quotes(),
                            spots={"AAPL": SPOT})


def _load(monkeypatch, vertical, tolerance_mode, *, unmarked, rows=None):
    monkeypatch.setenv(O1_DEBIT_VERTICAL_SETTING, vertical)
    if tolerance_mode is None:
        monkeypatch.delenv(O1DV_MARK_TOLERANCE_SETTING, raising=False)
    else:
        monkeypatch.setenv(O1DV_MARK_TOLERANCE_SETTING, tolerance_mode)
    inputs = _inputs({key for key in (9001, 9005, 9003) if key not in unmarked})
    monkeypatch.setattr(loader, "load_o1_vertical_inputs_guarded", lambda cursor, plan: (inputs, None))
    return loader._load_o1(_Cursor([_flow_row()] if rows is None else rows), _plan(unmarked), [])


HEADS = {9001: _row("2.10", "2.30", age_seconds=3), 9005: _row("1.25", "1.35", age_seconds=9)}


@pytest.mark.parametrize("vertical", ["RECORD", "ON"])
def test_record_rebuilds_the_vertical_and_changes_no_package(monkeypatch, vertical):
    # The long leg had no quote inside the last second: the 1 s rule rejects the vertical.
    _publish(HEADS, marked=(9003,))
    off_lane, off_identity = _load(monkeypatch, vertical, None, unmarked=(9001, 9005))
    assert current_window(CUTOFF) is None or not current_window(CUTOFF).events  # OFF: nothing kept
    _publish(HEADS, marked=(9003,))
    lane, identity = _load(monkeypatch, vertical, "RECORD", unmarked=(9001, 9005))
    assert lane == off_lane and identity == off_identity
    (invocation,) = lane.invocations
    # The long leg unmarked at this window leaves the event without a package (R34), with or without this switch.
    assert invocation.debit_vertical.reason == "O1DV_LONG_LEG_UNMARKED" and invocation.package is None
    results = current_window(CUTOFF).events[str(EVENT)]
    # 5 s: the long leg is marked (3 s old), the 348 strike (9 s old) is not: the vertical uses the 350 strike.
    assert results[5].short is SHORT_CALL and results[5].long_mark.age_ms == 3000
    assert results[5].net_debit == Decimal("1.40")
    # 15 s: the nearer 348 strike is marked too and comes first.
    assert results[15].short is NEAR_CALL and results[15].short_mark.mark == Decimal("1.30")
    assert results[15].net_debit == Decimal("0.90")


def test_on_keeps_the_one_second_vertical_package(monkeypatch):
    _publish(HEADS, marked=(9001, 9003))
    off_lane, off_identity = _load(monkeypatch, "ON", None, unmarked=(9005,))
    _publish(HEADS, marked=(9001, 9003))
    lane, identity = _load(monkeypatch, "ON", "RECORD", unmarked=(9005,))
    assert lane == off_lane and identity == off_identity
    (invocation,) = lane.invocations
    assert is_o1_debit_vertical_package(invocation.package) and invocation.debit_vertical.vertical.short is SHORT_CALL
    assert current_window(CUTOFF).events[str(EVENT)][15].short is NEAR_CALL


def test_vertical_off_evaluates_nothing(monkeypatch):
    _publish(HEADS, marked=(9003,))
    monkeypatch.setenv(O1DV_MARK_TOLERANCE_SETTING, "RECORD")
    monkeypatch.setenv(O1_DEBIT_VERTICAL_SETTING, "OFF")
    lane, _ = loader._load_o1(_Cursor([_flow_row()]), _plan((9001, 9005)), [])
    assert lane.invocations[0].debit_vertical is None and not current_window(CUTOFF).events


def test_no_published_window_records_nothing_and_never_raises(monkeypatch):
    lane, _ = _load(monkeypatch, "ON", "RECORD", unmarked=(9001, 9005))
    assert lane.invocations[0].debit_vertical.reason == "O1DV_LONG_LEG_UNMARKED" and current_window(CUTOFF) is None


def test_invalid_switch_fails_the_window_loudly(monkeypatch):
    with pytest.raises(ValueError, match=O1DV_MARK_TOLERANCE_SETTING):
        _load(monkeypatch, "ON", "YES", unmarked=())


def test_r31_contracts_stay_unmarked():
    # 9001 was marked by the producer but its volume baseline is incomplete (R31): in the plan's unmarked set, not in
    # the window's (selected minus marked), so no tolerance mark is added for it.
    _publish(HEADS, marked=(9001, 9003))
    prepared = tolerance.tolerance_inputs(_inputs({9003}), _plan((9001, 9005)))
    marks, unmarked = prepared[15]
    assert 9001 not in marks and 9001 in unmarked and 9005 in marks and 9005 not in unmarked


# --- record line -----------------------------------------------------------------------------------------------------

def _entries_after_load(monkeypatch, vertical="ON"):
    second = uuid5(NAMESPACE_URL, "o1dv-mark-tolerance-event-2")
    _publish(HEADS, marked=(9003,))
    lane, _ = _load(monkeypatch, vertical, "RECORD", unmarked=(9001, 9005),
                    rows=[_flow_row(), _flow_row(event_id=second)])
    dispositions = {EVENT: "DETECTED", second: "NOT_DETECTED"}
    return [(row.event_id, dispositions[row.event_id], row.debit_vertical) for row in lane.invocations]


def test_one_marks_line_per_window_after_the_vertical_line(monkeypatch, capsys):
    from realtime.advanced_options_native_adapters import _log_o1_debit_vertical_record

    entries = _entries_after_load(monkeypatch)
    entries.append((uuid5(NAMESPACE_URL, "stored"), "DETECTED", None))
    monkeypatch.delenv(O1DV_MARK_TOLERANCE_SETTING, raising=False)
    _log_o1_debit_vertical_record(SimpleNamespace(cutoff=CUTOFF), entries)
    (base_line,) = capsys.readouterr().err.strip().splitlines()
    monkeypatch.setenv(O1DV_MARK_TOLERANCE_SETTING, "RECORD")
    _log_o1_debit_vertical_record(SimpleNamespace(cutoff=CUTOFF), entries)
    lines = capsys.readouterr().err.strip().splitlines()
    assert lines[0] == base_line and len(lines) == 2  # the 1 s line is byte-identical
    assert lines[1].startswith(O1DV_MARKS_RECORD_EVENT + " ")
    document = json.loads(lines[1].split(" ", 1)[1])
    assert document["mode"] == "RECORD" and document["applied"] is False and document["vertical_mode"] == "ON"
    assert document["cutoff"] == CUTOFF.isoformat() and document["quotes_status"] == "OK"
    assert document["policy_sha256"] == o1_debit_vertical_policy().policy_sha256
    assert document["tolerances_seconds"] == [5, 15] and document["base_maximum_age_seconds"] == 1
    assert (document["o1_events"], document["detected"], document["evaluated"]) == (3, 2, 2)
    assert document["stored_package_reused"] == 1 and document["unmarked_contracts"] == 2
    assert document["base"] == {"built": 0, "built_detected": 0, "unmarked": 2, "unmarked_detected": 1,
                                "rejected": {"O1DV_LONG_LEG_UNMARKED": 2}}
    five, fifteen = document["tolerance"]["5"], document["tolerance"]["15"]
    assert five["marked_added"] == 1 and five["quote_reasons"] == {"OLDER_THAN_TOLERANCE": 1}
    assert (five["built"], five["built_detected"], five["more_built"], five["more_built_detected"]) == (2, 1, 2, 1)
    assert five["lost"] == 0 and five["changed"] == 0 and five["new_verticals"] == 1
    (vertical,) = five["verticals"]
    assert vertical == {
        "long": LONG_CALL.contract_ticker, "short": SHORT_CALL.contract_ticker, "net_debit": "1.40",
        "long_mark": "2.20", "short_mark": "0.80", "long_age_ms": 3000, "short_age_ms": 100, "width": "5",
        "reward_risk": "2.571", "capital_at_risk": "140.00", "base_reason": "LONG_LEG_UNMARKED", "events": 2,
        "detected": 1,
    }
    assert fifteen["marked_added"] == 2 and fifteen["verticals"][0]["short"] == NEAR_CALL.contract_ticker
    assert fifteen["verticals"][0]["short_age_ms"] == 9000


def test_changed_counts_a_nearer_short_leg_and_no_vertical_is_lost(monkeypatch):
    _publish(HEADS, marked=(9001, 9003))
    lane, _ = _load(monkeypatch, "RECORD", "RECORD", unmarked=(9005,))
    entries = [(row.event_id, "DETECTED", row.debit_vertical) for row in lane.invocations]
    document = o1dv_marks_record(cutoff=CUTOFF, entries=entries, vertical_mode="RECORD",
                                 policy=o1_debit_vertical_policy(), window=current_window(CUTOFF))
    assert document["base"]["built"] == 1
    assert document["tolerance"]["5"]["changed"] == 0 and document["tolerance"]["15"]["changed"] == 1
    assert document["tolerance"]["15"]["more_built"] == 0 and document["tolerance"]["15"]["lost"] == 0


def test_line_stays_under_the_docker_line_limit():
    window = build_tolerance_window(cohort_id=COHORT, cutoff=CUTOFF, status="OK", quotes={}, unmarked_ids=(),
                                    journal_generation=GENERATION, stream_session_ids=(STREAM,), session_date=SESSION)
    long_vertical = select_debit_vertical(LONG_CALL, CONTRACTS, {9001: _mark(9001, "2.20"), 9003: _mark(9003, "0.80")},
                                          _quotes(), unmarked=(), spot=SPOT, session_date=SESSION, cutoff=CUTOFF,
                                          policy=o1_debit_vertical_policy())
    entries = []
    for index in range(400):
        event_id = uuid5(NAMESPACE_URL, f"many:{index}")
        ticker = f"O:AAPL{EXPIRY:%y%m%d}C{index:08d}"
        entries.append((event_id, "DETECTED", O1DebitVerticalOutcome(ticker, reason="O1DV_LONG_LEG_UNMARKED")))
        window.events[str(event_id)] = {5: long_vertical, 15: long_vertical}
    document = o1dv_marks_record(cutoff=CUTOFF, entries=entries, vertical_mode="ON", policy=o1_debit_vertical_policy(),
                                 window=window)
    assert len(json.dumps(document, sort_keys=True, default=str)) <= tolerance._LINE_LIMIT_BYTES
    assert document["tolerance"]["5"]["new_verticals"] == 400 and document["tolerance"]["5"]["more_built"] == 400
    assert 0 < len(document["tolerance"]["5"]["verticals"]) <= 20


def test_record_failure_is_a_line_not_an_exception(monkeypatch, capsys):
    monkeypatch.setenv(O1DV_MARK_TOLERANCE_SETTING, "RECORD")
    tolerance.log_window_o1dv_marks_record(CUTOFF, [(EVENT, "DETECTED", object())], vertical_mode="ON",
                                           policy_loader=o1_debit_vertical_policy)
    (line,) = capsys.readouterr().err.strip().splitlines()
    assert json.loads(line.split(" ", 1)[1])["status"] == "RECORD_FAILED"
    monkeypatch.setenv(O1DV_MARK_TOLERANCE_SETTING, "OFF")
    tolerance.log_window_o1dv_marks_record(CUTOFF, [], vertical_mode="ON", policy_loader=o1_debit_vertical_policy)
    assert capsys.readouterr().err == ""
