"""G106 AL2 (Nov 16): O1 debit verticals behind REALTIME_V3_O1_DEBIT_VERTICAL (OFF / RECORD / ON).

Rule: long leg = the O1 event's contract; short leg = same underlying, expiry and type, further OTM, among the window's
watched contracts marked at the cutoff; limits from the legacy DIRECTIONAL_DEBIT_SPREAD block. OFF keeps today's
single contract byte for byte; RECORD builds and logs the vertical but publishes today's package; ON publishes the
vertical when it qualifies, else today's single contract.
"""
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

import realtime.advanced_options_native_loader as loader
from realtime.advanced_options_o1_debit_vertical import (
    O1_DEBIT_VERTICAL_SETTING,
    O1DebitVerticalMode,
    O1DebitVerticalOutcome,
    O1VerticalInputs,
    is_o1_debit_vertical_package,
    o1_debit_vertical_candidate_id,
    o1_debit_vertical_mode,
    o1_debit_vertical_package,
    o1_debit_vertical_policy,
    o1_debit_vertical_record,
    select_debit_vertical,
)
from realtime.advanced_options_o3_inputs import LegQuote, WatchedContract
from realtime.advanced_options_o3_producer import LegMark

UTC = timezone.utc
COHORT = UUID("b096f5b7-2371-5522-925d-cf4db3912b9c")
MATRIX = UUID("11111111-1111-4111-8111-111111111111")
FLOW = UUID("22222222-2222-4222-8222-222222222222")
SECURITY = UUID("33333333-3333-4333-8333-333333333333")
SESSION = date(2026, 11, 13)
CUTOFF = datetime(2026, 11, 13, 16, 35, tzinfo=UTC)
EXPIRY = date(2026, 11, 20)
SPOT = Decimal("340")


def _contract(instrument_id, strike, side="C", expiration=EXPIRY):
    kind = side
    ticker = f"O:AAPL{expiration:%y%m%d}{kind}{int(Decimal(strike) * 1000):08d}"
    return WatchedContract(instrument_id=instrument_id, contract_id=instrument_id - 8900,
                           catalog_version_id=instrument_id + 1000, contract_ticker=ticker, underlying="AAPL",
                           call_put=side, strike=Decimal(strike), expiration=expiration, multiplier=100)


LONG_CALL = _contract(9001, "345")
NARROW_CALL = _contract(9002, "346")   # 1 / 340 = 0.29% of spot: under the 0.5% minimum width
SHORT_CALL = _contract(9003, "350")    # 5 / 340 = 1.47%
WIDE_CALL = _contract(9004, "380")     # 35 / 340 = 10.3%: over the 10% maximum
LONG_PUT = _contract(9101, "335", "P")
SHORT_PUT = _contract(9102, "330", "P")
CONTRACTS = (LONG_CALL, NARROW_CALL, SHORT_CALL, WIDE_CALL, LONG_PUT, SHORT_PUT)
MARK_VALUES = {9001: "2.20", 9002: "1.90", 9003: "0.80", 9004: "0.05", 9101: "2.00", 9102: "0.70"}


def _mark(instrument_id, value, at=CUTOFF - timedelta(milliseconds=100)):
    value = Decimal(value)
    return LegMark(evidence_id=uuid5(NAMESPACE_URL, f"mark:{instrument_id}"), instrument_id=instrument_id,
                   bid=value - Decimal("0.05"), ask=value + Decimal("0.05"), mark=value, market_time=at,
                   observed_time=at + timedelta(milliseconds=20), payload_sha256="c" * 64)


def _marks(**overrides):
    marks = {key: _mark(key, value) for key, value in MARK_VALUES.items()}
    marks.update(overrides)
    return marks


def _quotes(open_interest=1000, day_volume=500, **overrides):
    quotes = {row.instrument_id: LegQuote(row.instrument_id, None, None, None, day_volume, open_interest)
              for row in CONTRACTS}
    quotes.update(overrides)
    return quotes


def _select(long=LONG_CALL, marks=None, quotes=None, unmarked=(), spot=SPOT, contracts=CONTRACTS):
    return select_debit_vertical(long, contracts, _marks() if marks is None else marks,
                                 _quotes() if quotes is None else quotes, unmarked=unmarked, spot=spot,
                                 session_date=SESSION, cutoff=CUTOFF, policy=o1_debit_vertical_policy())


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert o1_debit_vertical_mode({}) is O1DebitVerticalMode.OFF
    assert o1_debit_vertical_mode({O1_DEBIT_VERTICAL_SETTING: "  "}) is O1DebitVerticalMode.OFF
    assert o1_debit_vertical_mode({O1_DEBIT_VERTICAL_SETTING: "record"}) is O1DebitVerticalMode.RECORD
    assert o1_debit_vertical_mode({O1_DEBIT_VERTICAL_SETTING: " On "}) is O1DebitVerticalMode.ON
    for bad in ("YES", "1", "RECORDED", "TRUE"):
        with pytest.raises(ValueError, match=O1_DEBIT_VERTICAL_SETTING):
            o1_debit_vertical_mode({O1_DEBIT_VERTICAL_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(loader.__file__).parents[1] / "scripts" / "run_advanced_options_v3_production.py").read_text(
        encoding="utf-8")
    start = source.index("kind = AdvancedOptionsProcessKind(")
    assert "if kind is AdvancedOptionsProcessKind.WINDOW:" in source[start:start + 400]
    assert "o1_debit_vertical_mode()" in source[start:start + 600]


def test_policy_is_the_legacy_debit_spread_block():
    policy = o1_debit_vertical_policy()
    legacy = json.loads((Path(loader.__file__).parents[1] / "options" / "policies"
                         / "strategy_o3_credit_v2.json").read_text(encoding="utf-8"))["debit_spread"]
    assert policy.version == "o1-debit-vertical-v1"
    # O1 exits at the session close, so 0DTE is allowed (the legacy floor of 1 is for multi-day holds).
    assert (policy.minimum_dte, policy.maximum_dte) == (0, legacy["maximum_dte"]) and legacy["minimum_dte"] == 1
    assert policy.minimum_width_fraction == Decimal(str(legacy["minimum_width_fraction"]))
    assert policy.maximum_width_fraction == Decimal(str(legacy["maximum_width_fraction"]))
    assert policy.minimum_return_on_risk == Decimal(str(legacy["minimum_return_on_risk"]))
    assert policy.minimum_leg_open_interest == legacy["minimum_open_interest"]
    assert policy.minimum_leg_day_volume == legacy["minimum_day_volume"]


# --- rule ------------------------------------------------------------------------------------------------------------

def test_call_vertical_skips_a_too_narrow_strike_and_prices_at_the_cutoff_marks():
    vertical = _select()
    assert vertical.structure_type == "CALL_DEBIT_VERTICAL" and vertical.direction == 1
    assert vertical.long is LONG_CALL and vertical.short is SHORT_CALL
    assert vertical.net_debit == Decimal("1.40") and vertical.width == Decimal("5")
    assert vertical.maximum_profit == Decimal("3.60") and vertical.breakeven == Decimal("346.40")
    assert vertical.capital_at_risk == Decimal("140.00")
    assert vertical.return_on_risk == Decimal("3.60") / Decimal("1.40")


def test_put_vertical_takes_the_lower_strike():
    vertical = _select(long=LONG_PUT)
    assert vertical.structure_type == "PUT_DEBIT_VERTICAL" and vertical.direction == -1
    assert vertical.short is SHORT_PUT
    assert vertical.net_debit == Decimal("1.30") and vertical.breakeven == Decimal("333.70")


def test_rejections_have_stable_reasons():
    assert _select(long=None) == "O1DV_LONG_LEG_NOT_WATCHED"
    far = _contract(9201, "345", expiration=date(2027, 1, 15))  # 63 DTE
    assert _select(long=far, contracts=(far,)) == "O1DV_DTE_OUTSIDE_POLICY"
    expired = _contract(9202, "345", expiration=date(2026, 11, 12))
    assert _select(long=expired, contracts=(expired,)) == "O1DV_DTE_OUTSIDE_POLICY"
    assert _select(unmarked=(9001,)) == "O1DV_LONG_LEG_UNMARKED"
    assert _select(marks={k: v for k, v in _marks().items() if k != 9001}) == "O1DV_LONG_LEG_UNMARKED"
    assert _select(quotes=_quotes(open_interest=100)) == "O1DV_LONG_LEG_LIQUIDITY_BELOW_POLICY"
    assert _select(spot=None) == "O1DV_SPOT_UNAVAILABLE"
    assert _select(contracts=(LONG_CALL,)) == "O1DV_NO_FURTHER_OTM_WATCHED_CONTRACT"
    assert _select(contracts=(LONG_CALL, NARROW_CALL, WIDE_CALL)) == "O1DV_NO_SHORT_WITHIN_WIDTH_POLICY"
    # R34 / R31: a short leg the window left unmarked (no mark, or an incomplete volume baseline) is not used.
    assert _select(unmarked=(9003,)) == "O1DV_SHORT_LEG_UNMARKED"
    assert _select(quotes=_quotes() | {9003: LegQuote(9003, None, None, None, 50, 1000)}) == (
        "O1DV_SHORT_LEG_LIQUIDITY_BELOW_POLICY")
    assert _select(marks=_marks() | {9003: _mark(9003, "2.20")}) == "O1DV_NON_POSITIVE_DEBIT"
    assert _select(marks=_marks() | {9003: _mark(9003, "-0.40")}) == "O1DV_SHORT_LEG_UNMARKED"  # unusable quote
    # debit 2.60 of width 5: max profit 2.40, reward/risk 0.92 < 1
    assert _select(marks=_marks() | {9001: _mark(9001, "3.40")}) == "O1DV_REWARD_RISK_BELOW_POLICY"
    assert _select(marks=_marks() | {9001: _mark(9001, "5.90")}) == "O1DV_DEBIT_NOT_BELOW_WIDTH"
    # A mark after the cutoff is never used.
    late = _mark(9003, "0.80", at=CUTOFF + timedelta(seconds=1))
    assert _select(marks=_marks() | {9003: late}) == "O1DV_SHORT_LEG_UNMARKED"


def test_next_strike_is_tried_when_the_nearest_eligible_one_fails():
    further = _contract(9005, "355")
    marks = _marks() | {9005: _mark(9005, "0.30")}
    quotes = _quotes() | {9005: LegQuote(9005, None, None, None, 500, 1000)}
    vertical = _select(contracts=CONTRACTS + (further,), marks=marks, quotes=quotes, unmarked=(9003,))
    assert vertical.short is further and vertical.net_debit == Decimal("1.90")


# --- package ---------------------------------------------------------------------------------------------------------

EVENT = UUID("00000000-0000-4000-8000-0000000000e1")


def _package(vertical=None, event_id=EVENT):
    return o1_debit_vertical_package(
        vertical or _select(), event_id=event_id, cohort_id=COHORT, matrix_id=MATRIX, session_date=SESSION,
        cutoff=CUTOFF, policy=o1_debit_vertical_policy(), valuation_policy_sha256=loader.session_valuation_policy_sha256(),
    )


def test_two_leg_package_economics_and_identity():
    package = _package()
    long, short = package.legs
    assert (long.side.value, short.side.value) == ("BUY", "SELL")
    assert (long.contract_id, short.contract_id) == (LONG_CALL.contract_id, SHORT_CALL.contract_id)
    assert (long.entry_mark, short.entry_mark) == (Decimal("2.20"), Decimal("0.80"))
    assert package.entry_net_premium == Decimal("-140.00") and package.capital_at_risk == Decimal("140.00")
    assert package.exit_deadline == datetime(2026, 11, 13, 21, 0, tzinfo=UTC)
    assert package.candidate_id == o1_debit_vertical_candidate_id(COHORT, EVENT)
    assert is_o1_debit_vertical_package(package) and not loader.is_options_alert_v1_package(package)
    assert _package().candidate_identity_sha256 == package.candidate_identity_sha256


def test_vertical_is_marked_by_the_window_like_any_package():
    from options.config import load_valuation_policy
    from realtime.advanced_options_p8_window import _package_mark

    package = _package()
    later = CUTOFF + timedelta(minutes=5)

    def window_mark(instrument_id, value, at):
        value = Decimal(value)
        return SimpleNamespace(instrument_id=instrument_id, journal_generation=FLOW, stream_session_id=SECURITY,
                               source_offset=instrument_id, market_time=at, observed_time=at + timedelta(milliseconds=5),
                               bid=value - Decimal("0.05"), ask=value + Decimal("0.05"), bid_size=10, ask_size=12,
                               payload_sha256="d" * 64)

    policy = load_valuation_policy(loader.SESSION_VALUATION_POLICY_PATH)
    at = later - timedelta(seconds=1)
    marks = (window_mark(9001, "2.60", at), window_mark(9003, "1.00", at + timedelta(milliseconds=40)))
    mark = _package_mark(package, marks, policy, later)
    assert mark is not None and mark.package == package
    # Value moved from a 1.40 debit to 1.60: +20 per package before the cost model.
    assert mark.outcome.entry_net_premium == package.entry_net_premium
    # Legs quoted further apart than the platform's 250 ms package skew: not marked, never a failed window.
    skewed = (window_mark(9001, "2.60", at), window_mark(9003, "1.00", at - timedelta(seconds=2)))
    assert _package_mark(package, skewed, policy, later) is None


# --- _load_o1 through the switch -------------------------------------------------------------------------------------

def _flow_row(event_id=EVENT, contract=LONG_CALL):
    return {
        "event_id": str(event_id), "event_time": CUTOFF - timedelta(minutes=2), "contract_id": contract.contract_id,
        "contract_ticker": contract.contract_ticker, "underlyer": "AAPL", "cumulative_volume_after": 5000,
        "prior_open_interest": 1000, "open_interest_as_of": date(2026, 11, 12), "source_flow_id": str(FLOW),
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
        source_flow=SimpleNamespace(source_flow_id=FLOW, source_flow_sha256="a" * 64),
        rolling_state=SimpleNamespace(committed_offset=10, state_id=MATRIX),
        reference_state=SimpleNamespace(known_at=CUTOFF - timedelta(hours=6)),
        cutoff=CUTOFF, unmarked_instrument_ids=tuple(unmarked),
    )


INPUTS = O1VerticalInputs(contracts=CONTRACTS, by_ticker={row.contract_ticker: row for row in CONTRACTS},
                          marks=_marks(), quotes=_quotes(), spots={"AAPL": SPOT})


def _load(monkeypatch, mode, rows=None, plan=None, inputs=INPUTS):
    if mode is None:
        monkeypatch.delenv(O1_DEBIT_VERTICAL_SETTING, raising=False)
    else:
        monkeypatch.setenv(O1_DEBIT_VERTICAL_SETTING, mode)
    calls = []

    def fake_inputs(cursor, plan):
        calls.append(plan)
        return inputs, None

    monkeypatch.setattr(loader, "load_o1_vertical_inputs_guarded", fake_inputs)
    cursor = _Cursor([_flow_row()] if rows is None else rows)
    lane, identity = loader._load_o1(cursor, plan or _plan(), [])
    return lane, identity, calls, cursor


def test_off_is_todays_single_contract_and_reads_nothing_more(monkeypatch):
    lane, identity, calls, cursor = _load(monkeypatch, None)
    (invocation,) = lane.invocations
    assert invocation.package.candidate_id == loader.options_alert_v1_candidate_id(COHORT, EVENT)
    assert len(invocation.package.legs) == 1 and invocation.debit_vertical is None
    assert calls == [] and len(cursor.statements) == 2
    off_again, *_ = _load(monkeypatch, "OFF")
    assert off_again.invocations[0].package == invocation.package


def test_record_publishes_todays_package_and_records_the_vertical(monkeypatch):
    off, off_identity, *_ = _load(monkeypatch, None)
    lane, identity, calls, _ = _load(monkeypatch, "RECORD")
    (invocation,) = lane.invocations
    assert invocation.package == off.invocations[0].package and identity == off_identity
    outcome = invocation.debit_vertical
    assert isinstance(outcome, O1DebitVerticalOutcome) and outcome.used is False
    assert outcome.vertical.short is SHORT_CALL and len(calls) == 1


def test_on_publishes_the_vertical_when_it_qualifies_else_the_single_contract(monkeypatch):
    lane, identity, *_ = _load(monkeypatch, "ON")
    (invocation,) = lane.invocations
    assert is_o1_debit_vertical_package(invocation.package) and invocation.debit_vertical.used
    assert identity[0][2] == str(o1_debit_vertical_candidate_id(COHORT, EVENT))
    # Short leg unmarked at this window: today's single contract, with the reason recorded.
    lane, *_ = _load(monkeypatch, "ON", plan=_plan(unmarked=(9003, 9004)))
    (invocation,) = lane.invocations
    assert loader.is_options_alert_v1_package(invocation.package)
    assert invocation.debit_vertical.reason == "O1DV_SHORT_LEG_UNMARKED" and not invocation.debit_vertical.used
    # The vertical inputs could not be read: single contract, reason recorded, the window goes on.
    lane, *_ = _load(monkeypatch, "ON", inputs=None)
    assert loader.is_options_alert_v1_package(lane.invocations[0].package)
    assert lane.invocations[0].debit_vertical.reason == "O1DV_INPUTS_UNAVAILABLE"


def test_invalid_switch_fails_the_window_loudly(monkeypatch):
    with pytest.raises(ValueError, match=O1_DEBIT_VERTICAL_SETTING):
        _load(monkeypatch, "YES")


def test_a_stored_vertical_with_an_unmarked_leg_is_never_replaced(monkeypatch):
    stored = _package()

    def packages_for_events(cursor, *, cohort_id, event_ids, unmarked_instrument_ids=(), filtered_out=None):
        filtered_out[EVENT] = stored
        return {}

    monkeypatch.setattr(loader, "_packages_for_events", packages_for_events)
    for mode in (None, "RECORD", "ON"):
        lane, *_ = _load(monkeypatch, mode, plan=_plan(unmarked=(9003,)))
        (invocation,) = lane.invocations
        # A second package for this event would make the next read of it ambiguous (one package per event).
        assert invocation.package is None and invocation.debit_vertical is None


def test_packages_for_events_reports_what_it_left_out(monkeypatch):
    stored = _package()

    class Cursor(_Cursor):
        def execute(self, sql, params=None):
            self.statements.append(sql)
            self.result = [{"event_id": str(EVENT), "candidate_id": str(stored.candidate_id)}]

    monkeypatch.setattr(loader, "load_realtime_option_packages_cursor", lambda cursor, ids, cohort_id: (stored,))
    left_out = {}
    kept = loader._packages_for_events(Cursor([]), cohort_id=COHORT, event_ids=(EVENT,),
                                       unmarked_instrument_ids=(9003,), filtered_out=left_out)
    assert kept == {} and left_out == {EVENT: stored}
    assert loader._packages_for_events(Cursor([]), cohort_id=COHORT, event_ids=(EVENT,)) == {EVENT: stored}


# --- record line -----------------------------------------------------------------------------------------------------

def test_one_record_line_per_window(monkeypatch, capsys):
    from realtime.advanced_options_native_adapters import _log_o1_debit_vertical_record

    built = O1DebitVerticalOutcome(LONG_CALL.contract_ticker, vertical=_select(), used=False)
    rejected = O1DebitVerticalOutcome(LONG_PUT.contract_ticker, reason="O1DV_SHORT_LEG_UNMARKED")
    entries = [(EVENT, "DETECTED", built), (uuid5(NAMESPACE_URL, "e2"), "NOT_DETECTED", built),
               (uuid5(NAMESPACE_URL, "e3"), "DETECTED", rejected), (uuid5(NAMESPACE_URL, "e4"), "DETECTED", None)]
    monkeypatch.delenv(O1_DEBIT_VERTICAL_SETTING, raising=False)
    _log_o1_debit_vertical_record(SimpleNamespace(cutoff=CUTOFF), entries)
    assert capsys.readouterr().err == ""
    monkeypatch.setenv(O1_DEBIT_VERTICAL_SETTING, "RECORD")
    _log_o1_debit_vertical_record(SimpleNamespace(cutoff=CUTOFF), entries)
    (line,) = capsys.readouterr().err.strip().splitlines()
    assert line.startswith("O1_DEBIT_VERTICAL_RECORD ")
    document = json.loads(line.split(" ", 1)[1])
    assert document["mode"] == "RECORD" and document["cutoff"] == CUTOFF.isoformat()
    assert (document["o1_events"], document["detected"], document["built"], document["built_detected"]) == (4, 3, 2, 1)
    assert document["used"] == 0 and document["stored_package_reused"] == 1
    assert document["rejected"] == {"O1DV_SHORT_LEG_UNMARKED": 1}
    (vertical,) = document["verticals"]
    assert vertical["short"] == SHORT_CALL.contract_ticker and vertical["net_debit"] == "1.40"
    assert vertical["events"] == 2 and vertical["detected"] == 1 and vertical["breakeven"] == "346.40"
    assert o1_debit_vertical_record(mode=O1DebitVerticalMode.ON, cutoff=CUTOFF, entries=[])["o1_events"] == 0


# --- projection ------------------------------------------------------------------------------------------------------

def _evaluation(package, *, event_id=EVENT, minute=33):
    from realtime.preflight import DetectorFamily

    return SimpleNamespace(
        evaluation_id=uuid5(NAMESPACE_URL, f"evaluation:{event_id}"), dataset_id=uuid5(NAMESPACE_URL, "dataset"),
        event_id=event_id, detector=DetectorFamily.O1, underlyer="AAPL", selection_status="SELECTED",
        observed_at=datetime(2026, 11, 13, 16, minute, tzinfo=UTC), payload={}, package=package,
    )


def test_candidate_detail_names_the_structure():
    from realtime.advanced_options_projection import _candidate_detail

    detail = _candidate_detail(_evaluation(_package()), "f" * 64)["candidate"]
    assert detail["candidate_kind"] == "MULTI_LEG" and detail["structure_type"] == "CALL_DEBIT_VERTICAL"
    assert detail["structure_risk_class"] == "PREMIUM_AT_RISK_DEBIT"
    assert detail["maximum_profit"] == "360.00" and detail["maximum_loss"] == "140.00"
    assert detail["breakevens"] == ["346.40"] and detail["source_contract_id"] == LONG_CALL.contract_id
    single = loader.options_alert_v1_event_package(_flow_row(), cohort_id=COHORT, matrix_id=MATRIX,
                                                   session_date=SESSION)
    detail = _candidate_detail(_evaluation(single), "f" * 64)["candidate"]
    assert detail["structure_type"] == "ADVANCED_CANONICAL_PACKAGE" and detail["candidate_kind"] == "SINGLE_CONTRACT"
    assert detail["maximum_profit"] is None and detail["breakevens"] == []


def test_alert_row_direction_follows_the_bought_leg():
    from realtime.advanced_options_projection import _alert_run_row

    call = _alert_run_row(_evaluation(_package()), CUTOFF, {})
    put = _alert_run_row(_evaluation(_package(_select(long=LONG_PUT))), CUTOFF, {})
    assert call["direction"] == 1 and put["direction"] == -1  # legs cancel by count; R30 would default to +1
    assert put["original_package"]["structure_type"] == "PUT_DEBIT_VERTICAL"
    assert put["original_package"]["maximum_profit"] == "370.00"
    assert len(put["original_package"]["legs"]) == 2


def test_session_alerts_keep_a_vertical_apart_from_the_single_contract():
    from realtime.advanced_options_projection import (
        _one_alert_per_contract,
        _session_alert_key,
        options_session_alert_id,
        persist_options_session_alerts_cursor,
    )
    from test_options_session_alerts import AlertTable

    second = uuid5(NAMESPACE_URL, "second-event")
    single = loader.options_alert_v1_event_package(_flow_row(), cohort_id=COHORT, matrix_id=MATRIX,
                                                   session_date=SESSION)
    vertical = _package(event_id=second)
    window = [_evaluation(single), _evaluation(vertical, event_id=second, minute=34)]
    assert _session_alert_key(window[0]) == (LONG_CALL.contract_id, "")
    assert _session_alert_key(window[1]) == (LONG_CALL.contract_id, f"O1_DEBIT_VERTICAL:{SHORT_CALL.contract_id}")
    pairs = [(item, dict(
        evaluation_id=str(item.evaluation_id), detector_id="O1", underlyer="AAPL",
        triggered_at=item.observed_at.isoformat(), first_selected_at=item.observed_at.isoformat(),
        last_seen_at=item.observed_at.isoformat(), hit_count=1, repeat_count=0, scheduled_cycle="c",
        exit_deadline="2026-11-13T21:00:00+00:00", current_mark={"status": "UNAVAILABLE"},
        management_status="UNAVAILABLE",
    )) for item in window]
    table = AlertTable()
    assert persist_options_session_alerts_cursor(table, cohort_id=COHORT, session_date="2026-11-13",
                                                 alert_rows=_one_alert_per_contract(pairs), evaluations=window) == 2
    v1_id = str(options_session_alert_id("2026-11-13", "O1", LONG_CALL.contract_id))
    vertical_id = str(options_session_alert_id("2026-11-13", "O1", LONG_CALL.contract_id,
                                               f"O1_DEBIT_VERTICAL:{SHORT_CALL.contract_id}"))
    assert set(table.rows) == {v1_id, vertical_id}
    assert json.loads(table.rows[vertical_id]["projection"])["contract_id"] == LONG_CALL.contract_id


def test_spot_from_the_live_snapshot_ticks_and_older_ticks():
    from realtime.advanced_options_o1_debit_vertical import spots_from_rolling_state

    # Live V3 windows carry UnderlyingSnapshotTick (price under `evidence`), stored as
    # advanced-options-underlying-snapshot-tick-v1; older ticks carry a trade or a quote.
    snapshot = SimpleNamespace(ticker="AAPL", evidence=SimpleNamespace(price=Decimal("357.8965"), ticker="AAPL"))
    traded = SimpleNamespace(ticker="msft", trade=SimpleNamespace(price=Decimal("410.5")), quote=None)
    quoted = SimpleNamespace(ticker="NVDA", trade=None, quote=SimpleNamespace(bid=Decimal("190"), ask=Decimal("190.2")))
    empty = SimpleNamespace(ticker="TSLA", trade=None, quote=None, evidence=SimpleNamespace(price=None))
    spots = spots_from_rolling_state(SimpleNamespace(underlying_ticks=(snapshot, traded, quoted, empty)))
    assert spots == {"AAPL": Decimal("357.8965"), "MSFT": Decimal("410.5"), "NVDA": Decimal("190.1")}


def test_zero_dte_vertical_exits_at_its_expiry_close():
    long, short = _contract(9301, "345", expiration=SESSION), _contract(9302, "350", expiration=SESSION)
    marks = {9301: _mark(9301, "1.10"), 9302: _mark(9302, "0.20")}
    quotes = {9301: LegQuote(9301, None, None, None, 500, 1000), 9302: LegQuote(9302, None, None, None, 500, 1000)}
    vertical = _select(long=long, contracts=(long, short), marks=marks, quotes=quotes)
    assert vertical.short is short and vertical.net_debit == Decimal("0.90")
    package = _package(vertical)
    assert package.exit_deadline == datetime(2026, 11, 13, 21, 0, tzinfo=UTC)
