"""G106 AL6 / AL7 (Nov 17): the O1 0DTE gate (REALTIME_V3_O1_ZERO_DTE) and the O1 aggressor-side direction
(REALTIME_V3_O1_AGGRESSOR_SIDE), each OFF / RECORD / ON, applied in the O1 executor after the AL4 liquidity floor
(floor, then the 0DTE gate, then the aggressor side). OFF keeps today's O1 byte for byte; RECORD keeps it and logs; ON
applies (0DTE: NOT_DETECTED; aggressor: BOUGHT keeps the alert with direction = type, SOLD / UNCLEAR -> OBSERVATION).
"""
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pytest

import realtime.advanced_options_native_adapters as adapters
import realtime.advanced_options_native_loader as loader
import realtime.advanced_options_o1_gates as gates
from options.dual_origin import ACTIVITY_LIQUIDITY_FLOOR_POLICY, ACTIVITY_POLICY, detect_option_participation
from realtime.advanced_options_native_adapters import build_advanced_o1_executor
from realtime.advanced_options_o1_debit_vertical import O1_DEBIT_VERTICAL_SETTING
from realtime.advanced_options_o1_gates import (
    AGGRESSOR_SIDE_POLICY,
    MIN_DTE_POLICY,
    O1AG_INPUTS_UNAVAILABLE,
    O1AG_NO_CLASSIFIED_PREMIUM,
    O1AG_NO_DOMINANT_SIDE,
    O1AG_NO_STORED_PRINTS,
    O1AG_SIDE_PREMIUM_BELOW_POLICY,
    O1AggressorPrint,
    O1AggressorPrints,
    O1AggressorTally,
    O1GateMode,
    O1ZD_DTE_BELOW_POLICY,
    O1ZD_EXPIRES_TODAY,
    O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING,
    O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING,
    O1_AGGRESSOR_SIDE_SETTING,
    O1_MIN_DTE_SETTING,
    O1_ZERO_DTE_SETTING,
    ActivityGatePolicy,
    AggressorSidePolicy,
    GatedActivityFinding,
    MinimumDtePolicy,
    apply_o1_gates,
    classify_aggressor,
    minimum_dte_reasons,
    o1_aggressor_side_mode,
    o1_aggressor_side_policy,
    o1_aggressor_side_policy_from,
    o1_aggressor_tally,
    o1_min_dte_policy,
    o1_min_dte_policy_from,
    o1_zero_dte_mode,
)
from realtime.advanced_options_o1_liquidity_floor import O1_LIQUIDITY_FLOOR_SETTING, o1_liquidity_floor_policy
from test_o1_debit_vertical import CUTOFF, LONG_CALL, LONG_PUT, SESSION, _Cursor, _contract, _flow_row, _plan
from test_o1_liquidity_floor import _context, _session_alerts

SETTINGS = (O1_ZERO_DTE_SETTING, O1_MIN_DTE_SETTING, O1_AGGRESSOR_SIDE_SETTING, O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING,
            O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, O1_LIQUIDITY_FLOOR_SETTING, O1_DEBIT_VERTICAL_SETTING,
            "REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST", "REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME")
# Pinned Nov 17: the built-in policies (defaults) and so the hashes RECORD lines name.
MIN_DTE_POLICY_SHA256 = "f3fd5fd1499ac57ccb7b771a91604d1324204a336a5aa099a049a1837b8388f1"
AGGRESSOR_POLICY_SHA256 = "df625f648b7ec9d4761cf834d160a9a4367ddc6aeaca21f1074dd0459d866fbb"


@pytest.fixture(autouse=True)
def _process_start(monkeypatch):
    """Every test starts as a fresh window process: switches and thresholds unset, policies not read yet."""
    for key in SETTINGS:
        monkeypatch.delenv(key, raising=False)
    for cached in (o1_min_dte_policy, o1_aggressor_side_policy, o1_liquidity_floor_policy):
        cached.cache_clear()
    yield
    for cached in (o1_min_dte_policy, o1_aggressor_side_policy, o1_liquidity_floor_policy):
        cached.cache_clear()


ZDTE_CALL = _contract(9201, "340", "C", expiration=SESSION)
ZDTE_PUT = _contract(9202, "340", "P", expiration=SESSION)


def _row(name, contract, *, minute, open_interest, day_volume):
    row = _flow_row(uuid5(NAMESPACE_URL, f"o1g:{name}"), contract)
    event_time = CUTOFF - timedelta(minutes=minute)
    row.update(event_time=event_time, created_at=event_time + timedelta(seconds=1),
               quote_time=event_time - timedelta(seconds=1), prior_open_interest=open_interest,
               cumulative_volume_after=day_volume, underlyer="AAPL")
    return row


def _id(name):
    return uuid5(NAMESPACE_URL, f"o1g:{name}")


def _print(contract, minute, side, confidence, premium, n=0):
    return {"contract_ticker": contract.contract_ticker, "event_time": CUTOFF - timedelta(minutes=minute),
            "event_id": str(uuid5(NAMESPACE_URL, f"print:{contract.contract_ticker}:{minute}:{n}")),
            "premium": Decimal(premium), "side_class": side, "side_confidence": confidence}


# A 7-DTE call: sold early (bid $40k), then bought (ask $90k by minute 3). A 7-DTE put: bought ($60k at the ask). A 0DTE
# call (ratio 225, above the floor) and a 0DTE put below the floor (OI 60). A thin 7-DTE call below the floor.
ROWS = (
    _row("call-early", LONG_CALL, minute=8, open_interest=1000, day_volume=5000),
    _row("put", LONG_PUT, minute=7, open_interest=1000, day_volume=4000),
    _row("zdte-call", ZDTE_CALL, minute=6, open_interest=400, day_volume=90000),
    _row("zdte-put", ZDTE_PUT, minute=5, open_interest=60, day_volume=2000),
    _row("call-late", LONG_CALL, minute=2, open_interest=1000, day_volume=6000),
)
CALL_EARLY, PUT, ZDTE_C, ZDTE_P, CALL_LATE = (_id(n) for n in ("call-early", "put", "zdte-call", "zdte-put", "call-late"))
PRINTS = (
    _print(LONG_CALL, 30, "AT_OR_BELOW_BID", "HIGH", "40000"),
    _print(LONG_CALL, 8, "INSIDE_SPREAD", "MEDIUM", "5000"),
    _print(LONG_CALL, 3, "AT_OR_ABOVE_ASK", "HIGH", "90000"),
    _print(LONG_CALL, 1, "AT_OR_BELOW_BID", "HIGH", "99000"),  # after every event: never counted
    _print(LONG_PUT, 20, "AT_OR_ABOVE_ASK", "MEDIUM", "60000"),
    _print(LONG_PUT, 9, "QUOTE_STALE", "NONE", "9000"),
    _print(ZDTE_CALL, 10, "AT_OR_ABOVE_ASK", "LOW", "90000"),
)


class GateCursor(_Cursor):
    """The test loader cursor plus the AL7 prints read (and its savepoint); `fail` makes that read raise."""

    def __init__(self, rows, prints=PRINTS, fail=False):
        super().__init__(rows)
        self.prints, self.fail, self.params = prints, fail, []

    def execute(self, sql, params=None):
        if sql.startswith(("SAVEPOINT", "RELEASE SAVEPOINT", "ROLLBACK TO SAVEPOINT")):
            self.statements.append(sql)
            self.result = []
            return
        if "event.side_confidence" in sql:
            self.statements.append(sql)
            self.params.append(params)
            if self.fail:
                raise RuntimeError("read failed")
            tickers = set(params[1])
            self.result = [p for p in self.prints if p["contract_ticker"] in tickers and p["event_time"] <= params[3]]
            return
        return super().execute(sql, params)


def _env(monkeypatch, floor=None, zero_dte=None, aggressor=None):
    monkeypatch.delenv(O1_DEBIT_VERTICAL_SETTING, raising=False)
    for key, value in ((O1_LIQUIDITY_FLOOR_SETTING, floor), (O1_ZERO_DTE_SETTING, zero_dte),
                       (O1_AGGRESSOR_SIDE_SETTING, aggressor)):
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)


def _load(monkeypatch, rows=ROWS, cursor=None, **modes):
    _env(monkeypatch, **modes)
    cursor = cursor or GateCursor(list(rows))
    lane, identity = loader._load_o1(cursor, _plan(), [])
    return lane, identity, cursor


def _run(monkeypatch, rows=ROWS, **modes):
    lane, _identity, _cursor = _load(monkeypatch, rows, **modes)
    return build_advanced_o1_executor(lane)(_context())


def _by_event(result):
    return {row.event_id: row for row in result.evaluations}


def _status(result):
    return {key: row.selection_status for key, row in _by_event(result).items()}


def _lines(capsys, event):
    return [json.loads(line.split(" ", 1)[1]) for line in capsys.readouterr().err.strip().splitlines()
            if line.split(" ", 1)[0] == event]


def _all_lines(capsys):
    return [line.split(" ", 1) for line in capsys.readouterr().err.strip().splitlines() if line.strip()]


# --- switches and policies ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("parse,key", [(o1_zero_dte_mode, O1_ZERO_DTE_SETTING),
                                       (o1_aggressor_side_mode, O1_AGGRESSOR_SIDE_SETTING)])
def test_switch_values_and_loud_failure(parse, key):
    assert parse({}) is O1GateMode.OFF
    assert parse({key: "  "}) is O1GateMode.OFF
    assert parse({key: "off"}) is O1GateMode.OFF
    assert parse({key: "record"}) is O1GateMode.RECORD
    assert parse({key: " On "}) is O1GateMode.ON
    for bad in ("YES", "1", "RECORDED", "TRUE", "ENABLED"):
        with pytest.raises(ValueError, match=key):
            parse({key: bad})


def test_built_in_policies_and_their_hashes():
    assert (MIN_DTE_POLICY.version, MIN_DTE_POLICY.minimum_dte) == ("option_participation_min_dte_v1", 1)
    assert (AGGRESSOR_SIDE_POLICY.minimum_dominance_pct, AGGRESSOR_SIDE_POLICY.minimum_side_premium) == (200, 25_000)
    assert AGGRESSOR_SIDE_POLICY.counted_confidence == ("HIGH", "MEDIUM")
    assert (MIN_DTE_POLICY.sha256, AGGRESSOR_SIDE_POLICY.sha256) == (MIN_DTE_POLICY_SHA256, AGGRESSOR_POLICY_SHA256)
    assert o1_min_dte_policy_from({}) is MIN_DTE_POLICY
    assert o1_aggressor_side_policy_from({}) is AGGRESSOR_SIDE_POLICY
    explicit = {O1_MIN_DTE_SETTING: "1", O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING: " 200 ",
                O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING: "25000"}
    assert o1_min_dte_policy_from(explicit) is MIN_DTE_POLICY
    assert o1_aggressor_side_policy_from(explicit) is AGGRESSOR_SIDE_POLICY


def test_set_thresholds_are_stamped_into_the_policies():
    dte = o1_min_dte_policy_from({O1_MIN_DTE_SETTING: "2"})
    aggressor = o1_aggressor_side_policy_from({O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING: "300",
                                               O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING: "100000"})
    assert dte.minimum_dte == 2 and dte.sha256 != MIN_DTE_POLICY_SHA256
    assert (aggressor.minimum_dominance_pct, aggressor.minimum_side_premium) == (300, 100_000)
    assert aggressor.sha256 != AGGRESSOR_POLICY_SHA256


@pytest.mark.parametrize("key,bad", [
    (O1_MIN_DTE_SETTING, "-1"), (O1_MIN_DTE_SETTING, "1.5"), (O1_MIN_DTE_SETTING, "one"),
    (O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING, "99"), (O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING, "2.0"),
    (O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, "25k"), (O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, "-5"),
])
def test_invalid_thresholds_fail_loudly(key, bad):
    with pytest.raises(ValueError, match=key):
        (o1_min_dte_policy_from if key == O1_MIN_DTE_SETTING else o1_aggressor_side_policy_from)({key: bad})


def test_thresholds_are_read_once_per_process(monkeypatch):
    monkeypatch.setenv(O1_MIN_DTE_SETTING, "2")
    monkeypatch.setenv(O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, "50000")
    first = (o1_min_dte_policy(), o1_aggressor_side_policy())
    monkeypatch.setenv(O1_MIN_DTE_SETTING, "5")
    monkeypatch.setenv(O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, "1")
    assert (o1_min_dte_policy(), o1_aggressor_side_policy()) == first
    assert (first[0].minimum_dte, first[1].minimum_side_premium) == (2, 50_000)


def test_window_service_start_checks_both_switches_and_thresholds():
    source = (Path(loader.__file__).parents[1] / "scripts" / "run_advanced_options_v3_production.py").read_text(
        encoding="utf-8")
    start = source.index("kind = AdvancedOptionsProcessKind(")
    block = source[start:source.index("provider = None", start)]
    assert "if kind is AdvancedOptionsProcessKind.WINDOW:" in block
    for check in ("o1_zero_dte_mode()", "o1_min_dte_policy()", "o1_aggressor_side_mode()", "o1_aggressor_side_policy()"):
        assert check in block


@pytest.mark.parametrize("key,value", [
    (O1_ZERO_DTE_SETTING, "MAYBE"), (O1_AGGRESSOR_SIDE_SETTING, "YES"), (O1_MIN_DTE_SETTING, "x"),
    (O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING, "50"), (O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, "1e5"),
])
def test_the_start_checks_raise_on_an_invalid_value(monkeypatch, key, value):
    """The four calls the window service makes at start (previous test) raise, naming the key."""
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match=key):
        o1_zero_dte_mode()
        o1_min_dte_policy()
        o1_aggressor_side_mode()
        o1_aggressor_side_policy()


# --- the 0DTE rule and the aggressor classification (pure) -------------------------------------------------------------

def test_minimum_dte_reasons():
    assert minimum_dte_reasons(0) == (O1ZD_EXPIRES_TODAY,)
    assert minimum_dte_reasons(1) == ()
    assert minimum_dte_reasons(1, MinimumDtePolicy(minimum_dte=2)) == (O1ZD_DTE_BELOW_POLICY,)
    assert minimum_dte_reasons(0, MinimumDtePolicy(minimum_dte=2)) == (O1ZD_EXPIRES_TODAY,)
    assert minimum_dte_reasons(0, MinimumDtePolicy(minimum_dte=0)) == ()


def _tally(ask=0, bid=0, prints=1, **other):
    return O1AggressorTally(prints=prints, ask_premium=Decimal(ask), bid_premium=Decimal(bid),
                            **{key: Decimal(value) for key, value in other.items()})


@pytest.mark.parametrize("tally,expected", [
    (_tally(50_000, 25_000), ("BOUGHT", ())),                       # exactly 2x and >= $25k: E1
    (_tally(25_000, 0), ("BOUGHT", ())),
    (_tally(49_999, 25_000), ("UNCLEAR", (O1AG_NO_DOMINANT_SIDE,))),
    (_tally(24_999, 0), ("UNCLEAR", (O1AG_SIDE_PREMIUM_BELOW_POLICY,))),
    (_tally(25_000, 50_000), ("SOLD", ())),                         # S1 mirror
    (_tally(10_000, 30_000), ("SOLD", ())),
    (_tally(10_000, 19_999), ("UNCLEAR", (O1AG_NO_DOMINANT_SIDE,))),
    (_tally(0, 24_999), ("UNCLEAR", (O1AG_SIDE_PREMIUM_BELOW_POLICY,))),
    (_tally(30_000, 30_000), ("UNCLEAR", (O1AG_NO_DOMINANT_SIDE,))),
    (_tally(0, 0, inside_premium=90_000, low_confidence_premium=90_000), ("UNCLEAR", (O1AG_NO_CLASSIFIED_PREMIUM,))),
    (_tally(prints=0), ("UNCLEAR", (O1AG_NO_STORED_PRINTS,))),
    (O1AggressorTally(inputs_reason="RuntimeError"), ("UNCLEAR", (O1AG_INPUTS_UNAVAILABLE,))),
])
def test_classification_is_research_e1_and_s1(tally, expected):
    assert classify_aggressor(tally, AGGRESSOR_SIDE_POLICY) == expected


def test_classification_with_set_thresholds_and_a_100_percent_dominance():
    loose = AggressorSidePolicy(minimum_dominance_pct=100, minimum_side_premium=0)
    assert classify_aggressor(_tally(2, 1), loose) == ("BOUGHT", ())
    assert classify_aggressor(_tally(1, 1), loose) == ("UNCLEAR", (O1AG_NO_DOMINANT_SIDE,))
    strict = AggressorSidePolicy(minimum_dominance_pct=300, minimum_side_premium=100_000)
    assert classify_aggressor(_tally(200_000, 70_000), strict) == ("UNCLEAR", (O1AG_NO_DOMINANT_SIDE,))
    assert classify_aggressor(_tally(210_000, 70_000), strict) == ("BOUGHT", ())


def test_tally_counts_only_high_or_medium_at_the_quote_and_only_prints_up_to_the_event():
    rows = [O1AggressorPrint(p["contract_ticker"], p["event_time"], p["event_id"], p["premium"], p["side_class"],
                             p["side_confidence"]) for p in PRINTS]
    book = O1AggressorPrints(rows)
    early = book.tally(LONG_CALL.contract_ticker, CUTOFF - timedelta(minutes=8))
    assert (early.prints, early.ask_premium, early.bid_premium, early.inside_premium) == (2, 0, 40_000, 5_000)
    late = book.tally(LONG_CALL.contract_ticker, CUTOFF - timedelta(minutes=2))
    assert (late.prints, late.ask_premium, late.bid_premium) == (3, 90_000, 40_000)
    zdte = book.tally(ZDTE_CALL.contract_ticker, CUTOFF)
    assert (zdte.ask_premium, zdte.low_confidence_premium) == (0, 90_000)
    put = book.tally(LONG_PUT.contract_ticker, CUTOFF)
    assert (put.ask_premium, put.stale_premium, put.total_premium) == (60_000, 9_000, 69_000)
    assert book.tally("O:NONE", CUTOFF).prints == 0
    assert O1AggressorPrints(inputs_reason="X").tally(LONG_CALL.contract_ticker, CUTOFF).inputs_reason == "X"
    assert o1_aggressor_tally(rows[:1]).first_print_at == rows[0].event_time


# --- the v3 finding ----------------------------------------------------------------------------------------------------

def _source(contract=LONG_CALL, open_interest=1000, day_volume=5000):
    rows = (_row("src", contract, minute=4, open_interest=open_interest, day_volume=day_volume),)
    lane, _identity, _cursor = _load(_NoPatch(), rows=rows)
    return lane.invocations[0].source


class _NoPatch:
    """A stand-in for monkeypatch in helpers that need no environment change (the autouse fixture cleared it)."""

    def delenv(self, *args, **kwargs):
        pass

    def setenv(self, *args, **kwargs):
        raise AssertionError("no environment change here")


def _v1(source):
    return detect_option_participation(source, market_cutoff=source.market_time, decision_at=source.received_at)


def test_v3_finding_drops_0dte_and_names_its_gate_policy():
    source = _source(ZDTE_CALL, 400, 90000)
    finding = apply_o1_gates(source, _v1(source), minimum_dte=MIN_DTE_POLICY, aggressor_side=None)
    assert (finding.disposition, finding.reasons, finding.gate_reasons) == (
        "NOT_DETECTED", (O1ZD_EXPIRES_TODAY,), (O1ZD_EXPIRES_TODAY,))
    assert finding.schema_version == "option_participation_finding_v3" and finding.dte == 0
    assert finding.detector_policy_sha256 == ActivityGatePolicy(
        base_policy_sha256=ACTIVITY_POLICY.sha256, minimum_dte=MIN_DTE_POLICY).sha256
    assert finding.base_finding == _v1(source)
    assert GatedActivityFinding.model_validate_json(finding.canonical_json()) == finding  # round trip


def test_v3_finding_aggressor_bought_sold_unclear():
    source = _source(LONG_PUT)
    bought = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_SIDE_POLICY,
                            tally=_tally(60_000, 0))
    assert (bought.disposition, bought.direction, bought.permits_package, bought.gate_reasons) == (
        "DETECTED", -1, True, ())
    assert (bought.aggressor.aggressor_class, bought.aggressor.implied_direction) == ("BOUGHT", -1)
    sold = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_SIDE_POLICY,
                          tally=_tally(0, 60_000))
    assert (sold.disposition, sold.direction, sold.permits_package, sold.gate_reasons) == (
        "DETECTED", None, False, ("O1AG_SOLD",))
    assert sold.aggressor.implied_direction == 1  # a sold put reads bullish: observation only
    unclear = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_SIDE_POLICY,
                             tally=_tally(prints=0))
    assert (unclear.permits_package, unclear.gate_reasons) == (False, ("O1AG_UNCLEAR", O1AG_NO_STORED_PRINTS))
    for finding in (bought, sold, unclear):
        assert GatedActivityFinding.model_validate_json(finding.canonical_json()) == finding


def test_v3_finding_refuses_tampering():
    source = _source(LONG_CALL)
    finding = apply_o1_gates(source, _v1(source), minimum_dte=MIN_DTE_POLICY, aggressor_side=AGGRESSOR_SIDE_POLICY,
                             tally=_tally(60_000, 0))
    document = json.loads(finding.canonical_json())
    for change in (
        {"direction": -1},
        {"detector_policy_sha256": "0" * 64},
        {"aggressor": {**document["aggressor"], "aggressor_class": "SOLD", "implied_direction": -1}},
        {"aggressor": {**document["aggressor"], "ask_premium": "10"}},
        {"gate_reasons": ["O1AG_SOLD"]},
        {"disposition": "NOT_DETECTED", "reasons": ["O1ZD_EXPIRES_TODAY"]},
    ):
        with pytest.raises(ValueError):
            GatedActivityFinding.model_validate({**document, **change})
    with pytest.raises(ValueError):
        apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_SIDE_POLICY)  # no tally
    with pytest.raises(ValueError):
        apply_o1_gates(source, finding, minimum_dte=MIN_DTE_POLICY, aggressor_side=None)  # not twice
    with pytest.raises(ValueError):
        ActivityGatePolicy(base_policy_sha256=ACTIVITY_POLICY.sha256)  # no gate


# --- the O1 executor through the switches --------------------------------------------------------------------------------

def test_off_is_todays_o1_byte_for_byte(monkeypatch, capsys):
    unset = _run(monkeypatch)
    assert capsys.readouterr().err == ""
    explicit = _run(monkeypatch, zero_dte="OFF", aggressor="OFF")
    assert capsys.readouterr().err == ""
    assert unset == explicit
    assert set(_status(unset).values()) == {"SELECTED"}
    lane, _identity, _cursor = _load(monkeypatch)
    for invocation in lane.invocations:  # the payload main stores: the v1 finding
        assert _by_event(unset)[invocation.event_id].payload == adapters._plain(_v1(invocation.source))


def test_record_changes_nothing_and_logs(monkeypatch, capsys):
    off = _run(monkeypatch)
    capsys.readouterr()
    record = _run(monkeypatch, zero_dte="RECORD", aggressor="RECORD")
    assert record == off
    lines = _all_lines(capsys)
    assert [event for event, _ in lines] == ["O1_ZERO_DTE_RECORD"] + ["O1_AGGRESSOR_RECORD"] * 5 + [
        "O1_AGGRESSOR_WINDOW"]
    zero = json.loads(lines[0][1])
    assert (zero["mode"], zero["applied"], zero["minimum_dte"], zero["policy_sha256"]) == (
        "RECORD", False, 1, MIN_DTE_POLICY_SHA256)
    assert (zero["detected"], zero["would_drop"], zero["would_drop_selected"]) == (5, 2, 2)
    assert (zero["alerts_without_gate"], zero["alerts_with_gate"], zero["alerts_dropped"]) == (4, 2, 2)
    assert zero["reasons"] == {O1ZD_EXPIRES_TODAY: 2} and zero["by_type"] == {"CALL": 1, "PUT": 1}
    assert [(e["event_id"], e["dte"], e["without_gate"], e["with_gate"], e["window_alert"], e["below_floor"])
            for e in zero["events"]] == [(str(ZDTE_C), 0, "SELECTED", "NOT_SELECTED", True, None),
                                         (str(ZDTE_P), 0, "SELECTED", "NOT_SELECTED", True, None)]
    assert zero["events"][0]["entry_mark"] == "2.20"
    events = {item["event_id"]: item for item in (json.loads(doc) for _, doc in lines[1:-1])}
    early, put, zc, late = (events[str(key)] for key in (CALL_EARLY, PUT, ZDTE_C, CALL_LATE))
    assert (early["aggressor_class"], early["today_direction"], early["implied_direction"]) == ("SOLD", 1, -1)
    assert (early["bid_premium"], early["ask_premium"], early["inside_premium"], early["prints"]) == (
        "40000", "0", "5000", 2)
    assert (early["bid_share"], early["coverage_share"], early["with_gate"]) == ("1.0000", "0.8889", "OBSERVATION")
    assert (late["aggressor_class"], late["ask_share"], late["with_gate"]) == ("BOUGHT", "0.6923", "SELECTED")
    assert (put["aggressor_class"], put["implied_direction"], put["stale_premium"]) == ("BOUGHT", -1, "9000")
    assert (zc["aggressor_class"], zc["reasons"], zc["low_confidence_premium"], zc["zero_dte_would_drop"]) == (
        "UNCLEAR", [O1AG_NO_CLASSIFIED_PREMIUM], "90000", True)
    window = json.loads(lines[-1][1])
    assert (window["mode"], window["status"], window["applied"], window["detected"]) == ("RECORD", "OK", False, 5)
    assert window["by_class"] == {"BOUGHT": 2, "SOLD": 1, "UNCLEAR": 2}
    assert window["by_type"] == {"CALL": {"BOUGHT": 1, "SOLD": 1, "UNCLEAR": 1}, "PUT": {"BOUGHT": 1, "UNCLEAR": 1}}
    assert window["implied_vs_today"] == {"agrees": 2, "none": 2, "opposite": 1}
    assert (window["alerts_without_gate"], window["alerts_with_gate"], window["alerts_refilled"]) == (4, 2, 1)
    assert window["alerts_all_gates"] == 2  # the 0DTE gate drops the unclear 0DTE call anyway
    assert window["contracts"] == 4 and window["premium_latest_per_contract"]["ask"] == "150000"
    assert (window["minimum_dominance_pct"], window["minimum_side_premium"], window["policy_sha256"]) == (
        200, 25_000, AGGRESSOR_POLICY_SHA256)


def test_zero_dte_on_drops_0dte_events_and_carries_the_v3_finding(monkeypatch, capsys):
    off = _by_event(_run(monkeypatch))
    capsys.readouterr()
    result = _run(monkeypatch, zero_dte="ON")
    assert _status(result) == {CALL_EARLY: "SELECTED", PUT: "SELECTED", ZDTE_C: "NOT_SELECTED",
                               ZDTE_P: "NOT_SELECTED", CALL_LATE: "SELECTED"}
    rows = _by_event(result)
    assert rows[ZDTE_C].package is None and rows[CALL_LATE].package == off[CALL_LATE].package
    policy = ActivityGatePolicy(base_policy_sha256=ACTIVITY_POLICY.sha256, minimum_dte=MIN_DTE_POLICY)
    for row in rows.values():
        assert row.payload["schema_version"] == "option_participation_finding_v3"
        assert row.payload["detector_policy_sha256"] == policy.sha256
        assert row.payload["base_finding"] == off[row.event_id].payload
    assert rows[ZDTE_C].payload["reasons"] == [O1ZD_EXPIRES_TODAY]
    zero = _lines(capsys, "O1_ZERO_DTE_RECORD")[0]
    assert zero["applied"] is True and zero["would_drop"] == 2


def test_aggressor_on_keeps_bought_alerts_with_direction_and_makes_others_observations(monkeypatch, capsys):
    off = _by_event(_run(monkeypatch))
    capsys.readouterr()
    result = _run(monkeypatch, aggressor="ON")
    assert _status(result) == {CALL_EARLY: "OBSERVATION", PUT: "SELECTED", ZDTE_C: "OBSERVATION",
                               ZDTE_P: "OBSERVATION", CALL_LATE: "SELECTED"}
    rows = _by_event(result)
    assert rows[CALL_EARLY].package is None and rows[PUT].package == off[PUT].package
    assert (rows[PUT].payload["direction"], rows[CALL_LATE].payload["direction"]) == (-1, 1)
    assert rows[CALL_EARLY].payload["direction"] is None
    assert rows[CALL_EARLY].payload["aggressor"]["implied_direction"] == -1
    assert rows[CALL_EARLY].payload["gate_reasons"] == ["O1AG_SOLD"]
    assert rows[ZDTE_P].payload["gate_reasons"] == ["O1AG_UNCLEAR", O1AG_NO_STORED_PRINTS]
    window = _lines(capsys, "O1_AGGRESSOR_WINDOW")[0]
    assert window["applied"] is True and window["selected_with_gate"] == 2


def test_aggressor_on_reruns_selection_so_a_later_bought_event_becomes_the_alert(monkeypatch):
    off_rows, off_alerts = _session_alerts(_run(monkeypatch))
    on_rows, on_alerts = _session_alerts(_run(monkeypatch, aggressor="ON"))
    assert off_alerts[LONG_CALL.contract_id]["event_id"] == str(CALL_EARLY)
    assert on_alerts[LONG_CALL.contract_id]["event_id"] == str(CALL_LATE)
    assert set(on_alerts) == {LONG_CALL.contract_id, LONG_PUT.contract_id}
    assert len(off_alerts) == 4


def test_order_floor_then_0dte_then_aggressor(monkeypatch, capsys):
    result = _run(monkeypatch, floor="ON", zero_dte="ON", aggressor="ON")
    assert _status(result) == {CALL_EARLY: "OBSERVATION", PUT: "SELECTED", ZDTE_C: "NOT_SELECTED",
                               ZDTE_P: "NOT_SELECTED", CALL_LATE: "SELECTED"}
    rows = _by_event(result)
    # the floor takes the thin 0DTE put first: its reasons are the floor's, its base is the v2 finding
    assert rows[ZDTE_P].payload["reasons"] == ["O1LF_OPEN_INTEREST_BELOW_POLICY"]
    assert rows[ZDTE_P].payload["gate_reasons"] == [] and rows[ZDTE_P].payload["aggressor"] is None
    assert rows[ZDTE_C].payload["reasons"] == [O1ZD_EXPIRES_TODAY] and rows[ZDTE_C].payload["aggressor"] is None
    policy = ActivityGatePolicy(base_policy_sha256=ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256, minimum_dte=MIN_DTE_POLICY,
                                aggressor_side=AGGRESSOR_SIDE_POLICY)
    for row in rows.values():
        assert row.payload["base_finding"]["schema_version"] == "option_participation_finding_v2"
        assert row.payload["detector_policy_sha256"] == policy.sha256
    lines = _all_lines(capsys)
    window = json.loads([doc for event, doc in lines if event == "O1_AGGRESSOR_WINDOW"][0])
    assert window["detected"] == 3  # the aggressor only judges what the floor and the 0DTE gate left
    zero = json.loads([doc for event, doc in lines if event == "O1_ZERO_DTE_RECORD"][0])
    assert zero["would_drop"] == 1  # the 0DTE put never reaches the gate: the floor took it


def test_record_gates_with_the_floor_on_flag_what_the_floor_would_drop(monkeypatch, capsys):
    floor_only = _run(monkeypatch, floor="ON")
    capsys.readouterr()
    assert _run(monkeypatch, floor="ON", zero_dte="RECORD", aggressor="RECORD") == floor_only
    capsys.readouterr()
    _run(monkeypatch, floor="RECORD", zero_dte="RECORD", aggressor="RECORD")
    lines = _all_lines(capsys)
    zero = json.loads([doc for event, doc in lines if event == "O1_ZERO_DTE_RECORD"][0])
    assert {e["event_id"]: e["below_floor"] for e in zero["events"]} == {str(ZDTE_C): False, str(ZDTE_P): True}
    assert zero["would_drop_below_floor_too"] == 1


def test_hold_switch_still_wins(monkeypatch):
    monkeypatch.setattr(adapters, "O1_PACKAGE_SELECTION_ENABLED", False)
    result = _run(monkeypatch, zero_dte="ON", aggressor="ON")
    assert _status(result) == {CALL_EARLY: "OBSERVATION", PUT: "OBSERVATION", ZDTE_C: "NOT_SELECTED",
                               ZDTE_P: "NOT_SELECTED", CALL_LATE: "OBSERVATION"}


def test_a_gate_failure_never_fails_the_window(monkeypatch, capsys):
    off = _run(monkeypatch)
    capsys.readouterr()

    def broken(*args, **kwargs):
        raise ValueError("boom")

    monkeypatch.setattr(gates, "o1_gate_entry", broken)
    assert _run(monkeypatch, zero_dte="RECORD", aggressor="RECORD") == off  # RECORD: today's O1
    zero = _lines(capsys, "O1_ZERO_DTE_RECORD")[0]
    assert zero["gate_failures"] == {"O1GATE_FAILED:ValueError": 5}
    on = _run(monkeypatch, zero_dte="ON")  # ON: fail closed, every event left out, the lane says why
    assert on.state.value == "UNAVAILABLE" and on.reason_codes == ("O1GATE_FAILED:VALUEERROR",)


def test_record_logging_failure_never_fails_the_window(monkeypatch, capsys):
    off = _run(monkeypatch)
    capsys.readouterr()

    def broken(**kwargs):
        raise RuntimeError("record")

    monkeypatch.setattr(gates, "o1_zero_dte_document", broken)
    assert _run(monkeypatch, zero_dte="RECORD", aggressor="RECORD") == off
    lines = _all_lines(capsys)
    assert [event for event, _ in lines] == ["O1_ZERO_DTE_RECORD", "O1_AGGRESSOR_WINDOW"]
    assert all('"status": "RECORD_FAILED"' in doc for _, doc in lines)


def test_invalid_switch_fails_the_o1_lane_loudly(monkeypatch):
    lane, _identity, _cursor = _load(monkeypatch)
    monkeypatch.setenv(O1_ZERO_DTE_SETTING, "YES")
    with pytest.raises(ValueError, match=O1_ZERO_DTE_SETTING):
        build_advanced_o1_executor(lane)(_context())
    monkeypatch.setenv(O1_AGGRESSOR_SIDE_SETTING, "YES")
    with pytest.raises(ValueError, match=O1_AGGRESSOR_SIDE_SETTING):
        _load(monkeypatch, aggressor="YES")


# --- the loader ----------------------------------------------------------------------------------------------------------

def test_loader_reads_nothing_more_when_off_and_keeps_the_identity_in_record(monkeypatch):
    lane_off, identity_off, cursor_off = _load(monkeypatch)
    _, identity_zero, cursor_zero = _load(monkeypatch, zero_dte="RECORD")
    lane_record, identity_record, cursor_record = _load(monkeypatch, aggressor="RECORD")
    assert identity_off == identity_zero == identity_record
    assert cursor_off.statements == cursor_zero.statements and len(cursor_off.statements) == 2
    assert cursor_record.statements[:2] == cursor_off.statements
    assert [sql.split(" ", 1)[0] for sql in cursor_record.statements[2:]] == ["SAVEPOINT", "SELECT", "RELEASE"]
    assert all(row.aggressor is None for row in lane_off.invocations)
    assert lane_record == lane_off  # the tallies are never compared
    params = cursor_record.params[0]
    assert sorted(params[1]) == sorted({LONG_CALL.contract_ticker, LONG_PUT.contract_ticker,
                                        ZDTE_CALL.contract_ticker, ZDTE_PUT.contract_ticker})
    assert params[3] == CUTOFF and params[4] == SESSION and params[6] == 10


def test_loader_reads_only_contracts_at_the_ratio(monkeypatch):
    rows = (_row("low", LONG_CALL, minute=4, open_interest=1000, day_volume=2999),
            _row("none", LONG_PUT, minute=3, open_interest=0, day_volume=500))
    _, _, cursor = _load(monkeypatch, rows=rows, aggressor="RECORD")
    assert len(cursor.statements) == 2 and cursor.params == []  # nothing can be detected: no read


def test_loader_on_puts_the_tallies_in_the_identity(monkeypatch):
    _, identity_off, _ = _load(monkeypatch)
    lane, identity_on, _ = _load(monkeypatch, aggressor="ON")
    assert identity_on != identity_off
    assert [item[:3] for item in identity_on] == list(identity_off)
    by_event = {row.event_id: row.aggressor for row in lane.invocations}
    assert by_event[CALL_EARLY].bid_premium == 40_000 and by_event[CALL_LATE].ask_premium == 90_000


def test_a_failed_prints_read_never_fails_the_window(monkeypatch, capsys):
    off = _run(monkeypatch)
    capsys.readouterr()
    _env(monkeypatch, aggressor="RECORD")
    cursor = GateCursor(list(ROWS), fail=True)
    lane, _identity = loader._load_o1(cursor, _plan(), [])
    assert cursor.statements[-1] == "ROLLBACK TO SAVEPOINT o1_aggressor_inputs"
    assert build_advanced_o1_executor(lane)(_context()) == off  # RECORD: today's O1
    window = _lines(capsys, "O1_AGGRESSOR_WINDOW")[0]
    assert window["status"] == "INPUTS_UNAVAILABLE:RuntimeError"
    assert window["unclear_reasons"] == {O1AG_INPUTS_UNAVAILABLE: 5}
    _env(monkeypatch, aggressor="ON")
    lane, _identity = loader._load_o1(GateCursor(list(ROWS), fail=True), _plan(), [])
    on = build_advanced_o1_executor(lane)(_context())  # ON: no package without aggressor evidence
    assert on.state.value == "COMPLETE" and set(_status(on).values()) == {"OBSERVATION"}
