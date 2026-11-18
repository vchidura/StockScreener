"""G106 O1 tuning (Nov 18): aggressor policy v2, contradiction only (REALTIME_V3_O1_AGGRESSOR_POLICY).

STRICT (default, unset) is today's AL7 byte for byte. CONTRADICTION_ONLY keeps the same classification but drops only a
clear contradiction: BOUGHT keeps the alert (direction = type), UNCLEAR keeps it (direction = type, marked O1AG_UNCLEAR),
SOLD becomes an OBSERVATION without a package. Helpers and the fixture rows come from test_o1_gates.
"""
import json

import pytest

from options.dual_origin import ACTIVITY_LIQUIDITY_FLOOR_POLICY
from realtime.advanced_options_native_adapters import build_advanced_o1_executor
import realtime.advanced_options_native_loader as loader
from realtime.advanced_options_o1_gates import (
    AGGRESSOR_CONTRADICTION_POLICY,
    AGGRESSOR_SIDE_POLICY,
    MIN_DTE_POLICY,
    O1AG_INPUTS_UNAVAILABLE,
    O1AG_NO_CLASSIFIED_PREMIUM,
    O1AG_NO_STORED_PRINTS,
    O1ZD_EXPIRES_TODAY,
    O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING,
    O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING,
    O1_AGGRESSOR_POLICY_SETTING,
    ActivityGatePolicy,
    AggressorContradictionPolicy,
    AggressorSidePolicy,
    GatedActivityFinding,
    apply_o1_gates,
    classify_aggressor,
    o1_aggressor_policy_name,
    o1_aggressor_side_policy,
    o1_aggressor_side_policy_from,
)
from test_o1_debit_vertical import LONG_CALL, LONG_PUT
from test_o1_gates import (  # noqa: F401 - the autouse fixture resets switches and policies
    AGGRESSOR_POLICY_SHA256,
    CALL_EARLY,
    CALL_LATE,
    PUT,
    ROWS,
    ZDTE_C,
    ZDTE_P,
    GateCursor,
    _all_lines,
    _by_event,
    _env,
    _lines,
    _load,
    _plan,
    _process_start,
    _run,
    _session_alerts,
    _source,
    _status,
    _tally,
    _v1,
)
from test_o1_liquidity_floor import _context

# Pinned Nov 18: the built-in v2 policy (default thresholds).
CONTRADICTION_POLICY_SHA256 = "a1867d1061111e0ed2e93031b72d37c346d12291f3aa53fd9083e0963587dc39"


@pytest.fixture(autouse=True)
def _policy_unset(monkeypatch):
    monkeypatch.delenv(O1_AGGRESSOR_POLICY_SETTING, raising=False)
    yield


def _v2(monkeypatch):
    """A window process started with CONTRADICTION_ONLY (the policy is read once per process)."""
    monkeypatch.setenv(O1_AGGRESSOR_POLICY_SETTING, "CONTRADICTION_ONLY")
    o1_aggressor_side_policy.cache_clear()


# --- switch and policy -------------------------------------------------------------------------------------------------

def test_policy_switch_values_and_loud_failure():
    assert o1_aggressor_policy_name({}) == "STRICT"
    assert o1_aggressor_policy_name({O1_AGGRESSOR_POLICY_SETTING: "  "}) == "STRICT"
    assert o1_aggressor_policy_name({O1_AGGRESSOR_POLICY_SETTING: "strict"}) == "STRICT"
    assert o1_aggressor_policy_name({O1_AGGRESSOR_POLICY_SETTING: " contradiction_only "}) == "CONTRADICTION_ONLY"
    for bad in ("ON", "V2", "CONTRADICTION", "LOOSE", "1"):
        with pytest.raises(ValueError, match=O1_AGGRESSOR_POLICY_SETTING):
            o1_aggressor_policy_name({O1_AGGRESSOR_POLICY_SETTING: bad})


def test_strict_is_the_v1_policy_object_and_hash():
    assert o1_aggressor_side_policy_from({}) is AGGRESSOR_SIDE_POLICY
    assert o1_aggressor_side_policy_from({O1_AGGRESSOR_POLICY_SETTING: "STRICT"}) is AGGRESSOR_SIDE_POLICY
    assert AGGRESSOR_SIDE_POLICY.sha256 == AGGRESSOR_POLICY_SHA256
    assert AGGRESSOR_SIDE_POLICY.keeps("BOUGHT") and not AGGRESSOR_SIDE_POLICY.keeps("UNCLEAR")
    assert not AGGRESSOR_SIDE_POLICY.keeps("SOLD")


def test_contradiction_only_is_the_v2_policy_with_the_same_thresholds():
    policy = o1_aggressor_side_policy_from({O1_AGGRESSOR_POLICY_SETTING: "CONTRADICTION_ONLY"})
    assert policy is AGGRESSOR_CONTRADICTION_POLICY
    assert policy.version == "option_participation_aggressor_side_v2"
    assert policy.direction_basis == "BOUGHT_AND_UNCLEAR_KEEP_TYPE_SOLD_IS_OPPOSITE_OBSERVATION"
    assert (policy.minimum_dominance_pct, policy.minimum_side_premium) == (200, 25_000)
    assert policy.counted_confidence == AGGRESSOR_SIDE_POLICY.counted_confidence
    assert policy.sha256 != AGGRESSOR_POLICY_SHA256
    assert policy.sha256 == CONTRADICTION_POLICY_SHA256
    assert policy.keeps("BOUGHT") and policy.keeps("UNCLEAR") and not policy.keeps("SOLD")
    for tally in (_tally(60_000, 0), _tally(0, 60_000), _tally(prints=0), _tally(30_000, 30_000)):
        assert classify_aggressor(tally, policy) == classify_aggressor(tally, AGGRESSOR_SIDE_POLICY)


def test_contradiction_only_thresholds_are_stamped():
    policy = o1_aggressor_side_policy_from({O1_AGGRESSOR_POLICY_SETTING: "CONTRADICTION_ONLY",
                                            O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING: "300",
                                            O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING: "50000"})
    assert isinstance(policy, AggressorContradictionPolicy)
    assert (policy.minimum_dominance_pct, policy.minimum_side_premium) == (300, 50_000)
    assert policy.sha256 not in (CONTRADICTION_POLICY_SHA256, AGGRESSOR_POLICY_SHA256)


def test_the_service_start_check_reads_the_policy_switch(monkeypatch):
    """The window service calls o1_aggressor_side_policy() at start (test_o1_gates checks the call is there)."""
    monkeypatch.setenv(O1_AGGRESSOR_POLICY_SETTING, "MAYBE")
    with pytest.raises(ValueError, match=O1_AGGRESSOR_POLICY_SETTING):
        o1_aggressor_side_policy()
    o1_aggressor_side_policy.cache_clear()
    monkeypatch.setenv(O1_AGGRESSOR_POLICY_SETTING, "contradiction_only")
    assert o1_aggressor_side_policy() is AGGRESSOR_CONTRADICTION_POLICY


# --- the v3 finding under v2 -------------------------------------------------------------------------------------------

def test_v2_finding_bought_unclear_sold():
    source = _source(LONG_CALL)
    v2 = AGGRESSOR_CONTRADICTION_POLICY
    bought = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=v2, tally=_tally(60_000, 0))
    assert (bought.disposition, bought.direction, bought.permits_package, bought.gate_reasons) == (
        "DETECTED", 1, True, ())
    unclear = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=v2, tally=_tally(prints=0))
    assert (unclear.disposition, unclear.direction, unclear.permits_package, unclear.gate_reasons) == (
        "DETECTED", 1, True, ("O1AG_UNCLEAR", O1AG_NO_STORED_PRINTS))
    assert (unclear.aggressor.aggressor_class, unclear.aggressor.implied_direction) == ("UNCLEAR", None)
    sold = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=v2, tally=_tally(0, 60_000))
    assert (sold.disposition, sold.direction, sold.permits_package, sold.gate_reasons) == (
        "DETECTED", None, False, ("O1AG_SOLD",))
    assert sold.aggressor.implied_direction == -1  # a sold call reads bearish: recorded, never published or flipped
    for finding in (bought, unclear, sold):
        assert GatedActivityFinding.model_validate_json(finding.canonical_json()) == finding  # round trip
        assert finding.detector_policy_sha256 == ActivityGatePolicy(
            base_policy_sha256=finding.base_finding.detector_policy_sha256, aggressor_side=v2).sha256
    # a different policy is a different episode (as each gate policy)
    strict = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_SIDE_POLICY,
                            tally=_tally(60_000, 0))
    assert strict.episode_id != bought.episode_id
    assert strict.detector_policy_sha256 != bought.detector_policy_sha256


def test_v2_finding_on_a_put_keeps_the_put_direction_when_unclear():
    source = _source(LONG_PUT)
    unclear = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_CONTRADICTION_POLICY,
                             tally=_tally(30_000, 30_000))
    assert (unclear.direction, unclear.permits_package) == (-1, True)
    sold = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_CONTRADICTION_POLICY,
                          tally=_tally(0, 30_000))
    assert (sold.direction, sold.permits_package, sold.aggressor.implied_direction) == (None, False, 1)


def test_v2_finding_refuses_tampering():
    source = _source(LONG_CALL)
    finding = apply_o1_gates(source, _v1(source), minimum_dte=MIN_DTE_POLICY,
                             aggressor_side=AGGRESSOR_CONTRADICTION_POLICY, tally=_tally(prints=0))
    document = json.loads(finding.canonical_json())
    assert document["gate_aggressor_side"]["version"] == "option_participation_aggressor_side_v2"
    for change in (
        {"direction": None},                                            # an unclear v2 event keeps its type
        {"direction": -1},
        {"gate_reasons": []},
        {"gate_aggressor_side": json.loads(AGGRESSOR_SIDE_POLICY.canonical_json())},  # policy swapped: hash differs
        {"aggressor": {**document["aggressor"], "aggressor_class": "SOLD", "implied_direction": -1}},
    ):
        with pytest.raises(ValueError):
            GatedActivityFinding.model_validate({**document, **change})


def test_strict_findings_still_validate_as_v1():
    source = _source(LONG_CALL)
    finding = apply_o1_gates(source, _v1(source), minimum_dte=None, aggressor_side=AGGRESSOR_SIDE_POLICY,
                             tally=_tally(prints=0))
    again = GatedActivityFinding.model_validate_json(finding.canonical_json())
    assert type(again.gate_aggressor_side) is AggressorSidePolicy
    assert (again.direction, again.permits_package) == (None, False)


# --- the executor ---------------------------------------------------------------------------------------------------------

def test_strict_explicit_is_the_unset_run_byte_for_byte(monkeypatch, capsys):
    for mode in ("RECORD", "ON"):
        unset = _run(monkeypatch, aggressor=mode)
        unset_lines = _all_lines(capsys)
        monkeypatch.setenv(O1_AGGRESSOR_POLICY_SETTING, "STRICT")
        o1_aggressor_side_policy.cache_clear()
        explicit = _run(monkeypatch, aggressor=mode)
        assert explicit == unset and _all_lines(capsys) == unset_lines
        monkeypatch.delenv(O1_AGGRESSOR_POLICY_SETTING)
        o1_aggressor_side_policy.cache_clear()


def test_v2_on_drops_only_the_contradicted_event(monkeypatch, capsys):
    off = _by_event(_run(monkeypatch))
    capsys.readouterr()
    _v2(monkeypatch)
    result = _run(monkeypatch, aggressor="ON")
    # CALL_EARLY sold (bid $40k) -> observation; PUT bought; both 0DTE events unclear -> kept; CALL_LATE bought.
    assert _status(result) == {CALL_EARLY: "OBSERVATION", PUT: "SELECTED", ZDTE_C: "SELECTED",
                               ZDTE_P: "SELECTED", CALL_LATE: "SELECTED"}
    rows = _by_event(result)
    for key in (PUT, ZDTE_C, ZDTE_P, CALL_LATE):
        assert rows[key].package == off[key].package  # today's package, unchanged
    assert rows[CALL_EARLY].package is None
    assert {key: rows[key].payload["direction"] for key in rows} == {
        CALL_EARLY: None, PUT: -1, ZDTE_C: 1, ZDTE_P: -1, CALL_LATE: 1}
    assert rows[ZDTE_C].payload["gate_reasons"] == ["O1AG_UNCLEAR", O1AG_NO_CLASSIFIED_PREMIUM]
    assert rows[ZDTE_P].payload["gate_reasons"] == ["O1AG_UNCLEAR", O1AG_NO_STORED_PRINTS]
    assert rows[CALL_EARLY].payload["gate_reasons"] == ["O1AG_SOLD"]
    assert rows[CALL_EARLY].payload["gate_aggressor_side"]["version"] == "option_participation_aggressor_side_v2"
    window = _lines(capsys, "O1_AGGRESSOR_WINDOW")[0]
    assert (window["applied"], window["policy"], window["aggressor_policy"]) == (
        True, "option_participation_aggressor_side_v2", "CONTRADICTION_ONLY")
    assert (window["selected_without_gate"], window["selected_with_gate"], window["selected_with_strict"]) == (5, 4, 2)
    assert (window["alerts_without_gate"], window["alerts_with_gate"], window["alerts_with_strict"]) == (4, 4, 2)
    assert window["kept_unclear"] == 2 and window["alerts_refilled"] == 1


def test_v2_on_session_alerts_move_to_the_next_uncontradicted_event(monkeypatch):
    _off_rows, off_alerts = _session_alerts(_run(monkeypatch))
    _v2(monkeypatch)
    _on_rows, on_alerts = _session_alerts(_run(monkeypatch, aggressor="ON"))
    assert off_alerts[LONG_CALL.contract_id]["event_id"] == str(CALL_EARLY)
    assert on_alerts[LONG_CALL.contract_id]["event_id"] == str(CALL_LATE)
    assert set(on_alerts) == set(off_alerts) and len(on_alerts) == 4


def test_v2_record_changes_nothing_and_says_what_each_policy_keeps(monkeypatch, capsys):
    off = _run(monkeypatch)
    capsys.readouterr()
    _v2(monkeypatch)
    assert _run(monkeypatch, aggressor="RECORD") == off
    lines = _all_lines(capsys)
    assert [event for event, _ in lines] == ["O1_AGGRESSOR_RECORD"] * 5 + ["O1_AGGRESSOR_WINDOW"]
    events = {item["event_id"]: item for item in (json.loads(doc) for _, doc in lines[:-1])}
    assert {key: events[str(key)]["with_gate"] for key in (CALL_EARLY, PUT, ZDTE_C, ZDTE_P, CALL_LATE)} == {
        CALL_EARLY: "OBSERVATION", PUT: "SELECTED", ZDTE_C: "SELECTED", ZDTE_P: "SELECTED", CALL_LATE: "SELECTED"}
    window = json.loads(lines[-1][1])
    assert (window["mode"], window["applied"], window["policy_sha256"]) == (
        "RECORD", False, CONTRADICTION_POLICY_SHA256)
    assert (window["alerts_with_gate"], window["alerts_with_strict"], window["alerts_all_gates"]) == (4, 2, 4)


def test_strict_record_window_has_no_v2_keys(monkeypatch, capsys):
    _run(monkeypatch, aggressor="RECORD")
    window = _lines(capsys, "O1_AGGRESSOR_WINDOW")[0]
    assert not {"aggressor_policy", "selected_with_strict", "alerts_with_strict", "kept_unclear"} & set(window)


def test_v2_full_stack_floor_then_0dte_then_contradiction(monkeypatch, capsys):
    _v2(monkeypatch)
    result = _run(monkeypatch, floor="ON", zero_dte="ON", aggressor="ON")
    assert _status(result) == {CALL_EARLY: "OBSERVATION", PUT: "SELECTED", ZDTE_C: "NOT_SELECTED",
                               ZDTE_P: "NOT_SELECTED", CALL_LATE: "SELECTED"}
    rows = _by_event(result)
    assert rows[ZDTE_C].payload["reasons"] == [O1ZD_EXPIRES_TODAY] and rows[ZDTE_C].payload["aggressor"] is None
    policy = ActivityGatePolicy(base_policy_sha256=ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256,
                                minimum_dte=MIN_DTE_POLICY, aggressor_side=AGGRESSOR_CONTRADICTION_POLICY)
    assert {row.payload["detector_policy_sha256"] for row in rows.values()} == {policy.sha256}
    window = _lines(capsys, "O1_AGGRESSOR_WINDOW")[0]
    assert window["detected"] == 3 and window["alerts_with_gate"] == 2


def test_v2_unreadable_prints_keep_todays_alerts(monkeypatch, capsys):
    off = _status(_run(monkeypatch))
    capsys.readouterr()
    _v2(monkeypatch)
    _env(monkeypatch, aggressor="ON")
    lane, _identity = loader._load_o1(GateCursor(list(ROWS), fail=True), _plan(), [])
    on = build_advanced_o1_executor(lane)(_context())
    assert on.state.value == "COMPLETE" and _status(on) == off  # no evidence of a contradiction: today's alerts
    for row in on.evaluations:
        assert row.payload["gate_reasons"] == ["O1AG_UNCLEAR", O1AG_INPUTS_UNAVAILABLE]
    window = _lines(capsys, "O1_AGGRESSOR_WINDOW")[0]
    assert window["status"] == "INPUTS_UNAVAILABLE:RuntimeError" and window["kept_unclear"] == 5


def test_v2_on_puts_the_tallies_in_the_identity_as_strict_does(monkeypatch):
    _, identity_strict, _ = _load(monkeypatch, aggressor="ON")
    _v2(monkeypatch)
    _, identity_v2, _ = _load(monkeypatch, aggressor="ON")
    assert identity_v2 == identity_strict
    _, identity_record, _ = _load(monkeypatch, aggressor="RECORD")
    _, identity_off, _ = _load(monkeypatch)
    assert identity_record == identity_off
