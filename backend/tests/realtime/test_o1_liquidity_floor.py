"""G102 R87 / G106 Q2 (Nov 17): the O1 liquidity floor behind REALTIME_V3_O1_LIQUIDITY_FLOOR (OFF / RECORD / ON).

Policy `option_participation_origin_v2_liquidity_floor`: O1's v1 rule (day volume / prior OI >= 3) plus prior OI >= 100
and day volume >= 500. OFF keeps today's O1 byte for byte; RECORD keeps it and logs what the floor would take out;
ON applies it in the O1 executor, so the window's grouping and the session alert run on what is left (refill).
"""
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

import pytest

import realtime.advanced_options_native_adapters as adapters
import realtime.advanced_options_native_loader as loader
from options.dual_origin import (
    ACTIVITY_LIQUIDITY_FLOOR_POLICY,
    ACTIVITY_POLICY,
    ActivityFinding,
    LiquidityFloorActivityFinding,
    activity_liquidity_floor_reasons,
    apply_activity_liquidity_floor,
    detect_option_participation,
    detect_option_participation_with_liquidity_floor,
)
from realtime.advanced_options_cycle import AdvancedOptionsDetectorContext
from realtime.advanced_options_native_adapters import (
    AdvancedO1Invocation,
    AdvancedOptionsLaneInputs,
    build_advanced_o1_executor,
)
from realtime.advanced_options_o1_debit_vertical import (
    O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING,
    O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING,
    O1_DEBIT_VERTICAL_SETTING,
    O1DebitVerticalMode,
    o1_debit_vertical_policy,
    o1_debit_vertical_policy_from,
    o1_debit_vertical_record,
)
from realtime.advanced_options_o1_liquidity_floor import (
    O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING,
    O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING,
    O1_LIQUIDITY_FLOOR_SETTING,
    O1LiquidityFloorMode,
    o1_liquidity_floor_documents,
    o1_liquidity_floor_entry,
    o1_liquidity_floor_mode,
    o1_liquidity_floor_policy,
    o1_liquidity_floor_policy_from,
)
from options.dual_origin import ActivityLiquidityFloorPolicy
from realtime.preflight import DetectorFamily
from test_advanced_options_cycle import _gate_and_receipt
from test_developer_cycle_compute import _confirmation_inputs
from test_o1_debit_vertical import COHORT, CUTOFF, LONG_CALL, LONG_PUT, SESSION, _Cursor, _flow_row, _plan

# v1 is not touched: its policy hash and a v1 finding's fields stay what live O1 stores today.
ACTIVITY_POLICY_V1_SHA256 = "b08daea00fac0e1637a59b3140ee82d0eed1706a4d825f431da21ddddaa8613e"  # main 67d47b9
FLOOR_POLICY_DEFAULT_SHA256 = "951693f49bf16d749d351e120c6a3857662734362b814073545880f0099ef265"  # merged Nov 17
AL2_POLICY_FILE_SHA256 = "3371465f199095d4466d70b2c920098de3a932fb77e2f9cc0242fcc527002a1e"  # r121 / main 67d47b9
THRESHOLD_SETTINGS = (
    O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING, O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING,
    O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING, O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING,
)
V1_FINDING_FIELDS = {
    "schema_version", "detector_id", "origin", "detector_policy_sha256", "episode_id", "source_sha256", "decision_at",
    "market_cutoff", "valid_until", "volume_oi_ratio", "disposition", "reasons", "direction",
    "publication_permission", "execution_permission",
}


@pytest.fixture(autouse=True)
def _process_start(monkeypatch):
    """Every test starts as a fresh window process: thresholds unset, the once-per-process policies not read yet."""
    for key in THRESHOLD_SETTINGS:
        monkeypatch.delenv(key, raising=False)
    o1_liquidity_floor_policy.cache_clear()
    o1_debit_vertical_policy.cache_clear()
    yield
    o1_liquidity_floor_policy.cache_clear()
    o1_debit_vertical_policy.cache_clear()


def _source(open_interest, day_volume):
    _, source = _confirmation_inputs()
    return source.model_copy(update={"open_interest": open_interest, "day_volume": day_volume})


def _v1(source):
    return detect_option_participation(source, market_cutoff=source.market_time, decision_at=source.received_at)


# --- policy ----------------------------------------------------------------------------------------------------------

def test_policy_is_a_new_version_over_the_unchanged_v1_rule():
    policy = ACTIVITY_LIQUIDITY_FLOOR_POLICY
    assert policy.version == "option_participation_origin_v2_liquidity_floor"
    assert (policy.minimum_prior_open_interest, policy.minimum_day_volume) == (100, 500)
    assert policy.base_policy_sha256 == ACTIVITY_POLICY.sha256
    assert policy.missing_input_basis == "FAIL_CLOSED_UNAVAILABLE"
    assert policy.sha256 != ACTIVITY_POLICY.sha256
    assert ACTIVITY_POLICY.version == "option_participation_origin_v1"
    assert ACTIVITY_POLICY.minimum_volume_oi_ratio == 3
    assert ACTIVITY_POLICY.sha256 == ACTIVITY_POLICY_V1_SHA256


def test_v1_finding_keeps_its_fields_and_policy_hash():
    finding = _v1(_source(1000, 5000))
    document = json.loads(finding.canonical_json())
    assert set(document) == V1_FINDING_FIELDS
    assert document["detector_policy_sha256"] == ACTIVITY_POLICY.sha256
    assert document["schema_version"] == "option_participation_finding_v1"
    with pytest.raises(ValueError):  # a v1 finding cannot carry the floor policy's hash
        ActivityFinding.model_validate({**document, "detector_policy_sha256": ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256})


@pytest.mark.parametrize("open_interest,day_volume,expected", [
    (100, 500, ()),
    (99, 500, ("O1LF_OPEN_INTEREST_BELOW_POLICY",)),
    (100, 499, ("O1LF_DAY_VOLUME_BELOW_POLICY",)),
    (27, 109, ("O1LF_OPEN_INTEREST_BELOW_POLICY", "O1LF_DAY_VOLUME_BELOW_POLICY")),  # Nov 16 AAPL 362.5P 11/27
    (152, 165_635, ()),  # Nov 16 QQQ 669C 0DTE: small prior OI, heavy volume: stays
    (None, 500, ("O1LF_OPEN_INTEREST_UNAVAILABLE",)),
    (100, None, ("O1LF_DAY_VOLUME_UNAVAILABLE",)),
    (None, None, ("O1LF_OPEN_INTEREST_UNAVAILABLE", "O1LF_DAY_VOLUME_UNAVAILABLE")),
])
def test_threshold_edges_and_missing_inputs(open_interest, day_volume, expected):
    assert activity_liquidity_floor_reasons(open_interest, day_volume) == expected


def test_detected_below_the_floor_is_not_detected_with_the_floor_reasons():
    source = _source(27, 109)  # ratio 4.04: v1 detects it
    v1 = _v1(source)
    assert v1.disposition == "DETECTED"
    floored = apply_activity_liquidity_floor(source, v1)
    assert isinstance(floored, LiquidityFloorActivityFinding)
    assert floored.disposition == "NOT_DETECTED"
    assert floored.reasons == floored.liquidity_reasons == (
        "O1LF_OPEN_INTEREST_BELOW_POLICY", "O1LF_DAY_VOLUME_BELOW_POLICY")
    assert (floored.open_interest, floored.day_volume) == (27, 109)
    assert floored.volume_oi_ratio == v1.volume_oi_ratio
    assert floored.detector_policy_sha256 == ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256
    assert floored.schema_version == "option_participation_finding_v2"
    assert floored.episode_id != v1.episode_id  # the episode basis is per policy
    assert (floored.decision_at, floored.market_cutoff, floored.valid_until, floored.source_sha256) == (
        v1.decision_at, v1.market_cutoff, v1.valid_until, v1.source_sha256)
    assert LiquidityFloorActivityFinding.model_validate_json(floored.canonical_json()) == floored
    assert detect_option_participation_with_liquidity_floor(
        source, market_cutoff=source.market_time, decision_at=source.received_at) == floored


def test_detected_above_the_floor_stays_detected():
    source = _source(100, 500)
    floored = apply_activity_liquidity_floor(source, _v1(source))
    assert floored.disposition == "DETECTED" and floored.reasons == () and floored.liquidity_reasons == ()


def test_findings_v1_does_not_detect_keep_their_disposition_and_reasons():
    below_ratio = _source(1000, 2999)
    v1 = _v1(below_ratio)
    floored = apply_activity_liquidity_floor(below_ratio, v1)
    assert (floored.disposition, floored.reasons) == ("NOT_DETECTED", ("PARTICIPATION_BELOW_BASELINE",))
    assert floored.liquidity_reasons == ()
    thin_below_ratio = _source(50, 100)
    floored = apply_activity_liquidity_floor(thin_below_ratio, _v1(thin_below_ratio))
    assert floored.reasons == ("PARTICIPATION_BELOW_BASELINE",)
    assert floored.liquidity_reasons == ("O1LF_OPEN_INTEREST_BELOW_POLICY", "O1LF_DAY_VOLUME_BELOW_POLICY")


def test_missing_floor_input_fails_closed():
    # v1 already makes a missing OI or volume UNAVAILABLE; the floor keeps it so and records why.
    for source in (_source(None, 5000), _source(1000, None), _source(0, 5000)):
        floored = apply_activity_liquidity_floor(source, _v1(source))
        assert floored.disposition == "UNAVAILABLE"
    no_open_interest = _source(None, 5000)
    assert apply_activity_liquidity_floor(no_open_interest, _v1(no_open_interest)).liquidity_reasons == (
        "O1LF_OPEN_INTEREST_UNAVAILABLE",)
    # A v2 finding can never be DETECTED below the floor or with an absent input, nor carry reasons it did not earn.
    liquid = _source(100, 500)
    good = apply_activity_liquidity_floor(liquid, _v1(liquid))
    document = json.loads(good.canonical_json())
    with pytest.raises(ValueError, match="clear the liquidity floor"):
        LiquidityFloorActivityFinding.model_validate({**document, "day_volume": None,
                                                      "liquidity_reasons": ["O1LF_DAY_VOLUME_UNAVAILABLE"]})
    with pytest.raises(ValueError, match="do not match"):
        LiquidityFloorActivityFinding.model_validate({**document, "liquidity_reasons": ["O1LF_DAY_VOLUME_BELOW_POLICY"]})
    with pytest.raises(ValueError, match="unsupported activity policy"):
        LiquidityFloorActivityFinding.model_validate({**document, "detector_policy_sha256": ACTIVITY_POLICY.sha256})


def test_floor_needs_the_v1_finding_of_the_same_source():
    source, other = _source(1000, 5000), _source(1000, 6000)
    with pytest.raises(ValueError, match="exact source"):
        apply_activity_liquidity_floor(source, _v1(other))
    floored = apply_activity_liquidity_floor(source, _v1(source))
    with pytest.raises(ValueError, match="exact source"):
        apply_activity_liquidity_floor(source, floored)


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert o1_liquidity_floor_mode({}) is O1LiquidityFloorMode.OFF
    assert o1_liquidity_floor_mode({O1_LIQUIDITY_FLOOR_SETTING: "  "}) is O1LiquidityFloorMode.OFF
    assert o1_liquidity_floor_mode({O1_LIQUIDITY_FLOOR_SETTING: "off"}) is O1LiquidityFloorMode.OFF
    assert o1_liquidity_floor_mode({O1_LIQUIDITY_FLOOR_SETTING: "record"}) is O1LiquidityFloorMode.RECORD
    assert o1_liquidity_floor_mode({O1_LIQUIDITY_FLOOR_SETTING: " On "}) is O1LiquidityFloorMode.ON
    for bad in ("YES", "1", "RECORDED", "TRUE", "ENABLED"):
        with pytest.raises(ValueError, match=O1_LIQUIDITY_FLOOR_SETTING):
            o1_liquidity_floor_mode({O1_LIQUIDITY_FLOOR_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(loader.__file__).parents[1] / "scripts" / "run_advanced_options_v3_production.py").read_text(
        encoding="utf-8")
    start = source.index("kind = AdvancedOptionsProcessKind(")
    block = source[start:source.index("provider = None", start)]
    assert "if kind is AdvancedOptionsProcessKind.WINDOW:" in block
    for check in ("o1_debit_vertical_mode()", "o1_liquidity_floor_mode()", "o1_liquidity_floor_policy()",
                  "o1_debit_vertical_policy()"):
        assert check in block


# --- the O1 executor through the switch ------------------------------------------------------------------------------

def _context():
    gate, receipt = _gate_and_receipt()
    return AdvancedOptionsDetectorContext(gate, receipt, DetectorFamily.O1, receipt.cutoff)


def _invocations(monkeypatch, rows):
    """Real `_load_o1` (AL2 OFF) over flow rows: real ActivitySources and real v1 single-contract packages."""
    monkeypatch.delenv(O1_DEBIT_VERTICAL_SETTING, raising=False)
    lane, _identity = loader._load_o1(_Cursor(rows), _plan(), [])
    return lane


def _row(name, contract, *, minute, open_interest, day_volume):
    row = _flow_row(uuid5(NAMESPACE_URL, f"o1lf:{name}"), contract)
    event_time = CUTOFF - timedelta(minutes=minute)
    row.update(event_time=event_time, created_at=event_time + timedelta(seconds=1),
               quote_time=event_time - timedelta(seconds=1), prior_open_interest=open_interest,
               cumulative_volume_after=day_volume, underlyer="AAPL")
    return row


# On one contract: an early event below the floor (OI 150, volume 450: ratio 3.0, volume < 500), then the same contract
# with volume 600 (clears it). On another: the Nov 16 case (OI 27, volume 109), never clears it in the session.
ROWS = (
    _row("thin-early", LONG_CALL, minute=6, open_interest=150, day_volume=450),
    _row("thin-put", LONG_PUT, minute=5, open_interest=27, day_volume=109),
    _row("later", LONG_CALL, minute=3, open_interest=150, day_volume=600),
)
THIN_EARLY, THIN_PUT, LATER = (uuid5(NAMESPACE_URL, f"o1lf:{name}") for name in ("thin-early", "thin-put", "later"))


def _run(monkeypatch, mode, rows=ROWS):
    lane = _invocations(monkeypatch, rows)
    if mode is None:
        monkeypatch.delenv(O1_LIQUIDITY_FLOOR_SETTING, raising=False)
    else:
        monkeypatch.setenv(O1_LIQUIDITY_FLOOR_SETTING, mode)
    return build_advanced_o1_executor(lane)(_context())


def _by_event(result):
    return {row.event_id: row for row in result.evaluations}


def test_off_is_todays_o1_byte_for_byte(monkeypatch, capsys):
    unset = _run(monkeypatch, None)
    assert capsys.readouterr().err == ""
    explicit_off = _run(monkeypatch, "OFF")
    assert capsys.readouterr().err == ""
    assert unset == explicit_off
    rows = _by_event(unset)
    assert {key: row.selection_status for key, row in rows.items()} == {
        THIN_EARLY: "SELECTED", THIN_PUT: "SELECTED", LATER: "SELECTED"}
    for invocation in _invocations(monkeypatch, ROWS).invocations:  # the payload main stores: the v1 finding
        assert rows[invocation.event_id].payload == adapters._plain(_v1(invocation.source))
    for row in rows.values():
        assert row.payload["schema_version"] == "option_participation_finding_v1"
        assert row.payload["detector_policy_sha256"] == ACTIVITY_POLICY.sha256
        assert "liquidity_reasons" not in row.payload


def test_record_changes_nothing_and_logs_what_the_floor_would_take_out(monkeypatch, capsys):
    off = _run(monkeypatch, None)
    capsys.readouterr()
    record = _run(monkeypatch, "RECORD")
    assert record == off  # same evaluations: status, payload (v1 policy hash), package
    lines = capsys.readouterr().err.strip().splitlines()
    assert [line.split(" ", 1)[0] for line in lines] == [
        "O1_LIQUIDITY_FLOOR_RECORD", "O1_LIQUIDITY_FLOOR_RECORD", "O1_LIQUIDITY_FLOOR_WINDOW"]
    first, second = (json.loads(line.split(" ", 1)[1]) for line in lines[:2])
    assert (first["event_id"], first["reasons"]) == (str(THIN_EARLY), ["O1LF_DAY_VOLUME_BELOW_POLICY"])
    assert (second["event_id"], second["open_interest"], second["day_volume"]) == (str(THIN_PUT), 27, 109)
    assert second["reasons"] == ["O1LF_OPEN_INTEREST_BELOW_POLICY", "O1LF_DAY_VOLUME_BELOW_POLICY"]
    assert all(item["without_floor"] == "SELECTED" and item["with_floor"] == "NOT_SELECTED"
               and item["mode"] == "RECORD" and item["applied"] is False for item in (first, second))
    window = json.loads(lines[2].split(" ", 1)[1])
    assert window["mode"] == "RECORD" and window["applied"] is False
    assert window["policy_sha256"] == ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256
    assert (window["o1_events"], window["detected"], window["below_floor"], window["below_floor_selected"]) == (3, 3, 2, 2)
    assert (window["selected_without_floor"], window["selected_with_floor"]) == (3, 1)
    assert (window["alerts_without_floor"], window["alerts_with_floor"], window["alerts_refilled"]) == (2, 1, 1)
    assert window["reasons"] == {"O1LF_DAY_VOLUME_BELOW_POLICY": 2, "O1LF_OPEN_INTEREST_BELOW_POLICY": 1}
    instead = {item["contract"]: item for item in window["instead"]}
    assert instead[LONG_CALL.contract_ticker]["alert_event_id"] == str(THIN_EARLY)
    assert instead[LONG_CALL.contract_ticker]["instead_event_id"] == str(LATER)
    assert instead[LONG_PUT.contract_ticker]["instead_event_id"] is None


def test_on_applies_the_floor_and_carries_the_v2_policy_on_every_o1_evaluation(monkeypatch, capsys):
    off = _by_event(_run(monkeypatch, None))
    capsys.readouterr()
    result = _run(monkeypatch, "ON")
    assert result.state.value == "COMPLETE"
    rows = _by_event(result)
    assert {key: row.selection_status for key, row in rows.items()} == {
        THIN_EARLY: "NOT_SELECTED", THIN_PUT: "NOT_SELECTED", LATER: "SELECTED"}
    assert rows[THIN_EARLY].package is None and rows[THIN_PUT].package is None
    assert rows[LATER].package == off[LATER].package
    for row in rows.values():  # the policy hash changes when (and only when) the floor is active
        assert row.payload["schema_version"] == "option_participation_finding_v2"
        assert row.payload["detector_policy_sha256"] == ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256
        assert row.payload != off[row.event_id].payload
    assert rows[THIN_PUT].payload["reasons"] == ["O1LF_OPEN_INTEREST_BELOW_POLICY", "O1LF_DAY_VOLUME_BELOW_POLICY"]
    assert rows[LATER].payload["liquidity_reasons"] == []
    window = json.loads(capsys.readouterr().err.strip().splitlines()[-1].split(" ", 1)[1])
    assert window["mode"] == "ON" and window["applied"] is True and window["below_floor"] == 2


def _session_alerts(result):
    """The window's grouping and the session alert writer, on the executor's SELECTED evaluations."""
    from realtime.advanced_options_projection import _one_alert_per_contract, persist_options_session_alerts_cursor
    from test_options_session_alerts import AlertTable

    evaluations = [
        SimpleNamespace(evaluation_id=uuid5(NAMESPACE_URL, f"evaluation:{row.event_id}"), event_id=row.event_id,
                        detector=DetectorFamily.O1, underlyer=row.underlyer, selection_status=row.selection_status,
                        observed_at=row.observed_at, payload=row.payload, package=row.package)
        for row in result.evaluations if row.selection_status == "SELECTED"
    ]
    pairs = [(item, dict(
        evaluation_id=str(item.evaluation_id), detector_id="O1", underlyer="AAPL",
        triggered_at=item.observed_at.isoformat(), first_selected_at=item.observed_at.isoformat(),
        last_seen_at=item.observed_at.isoformat(), hit_count=1, repeat_count=0, scheduled_cycle="c",
        exit_deadline="2026-11-13T21:00:00+00:00", current_mark={"status": "UNAVAILABLE"},
        management_status="UNAVAILABLE",
    )) for item in evaluations]
    rows = _one_alert_per_contract(pairs)
    table = AlertTable()
    persist_options_session_alerts_cursor(table, cohort_id=COHORT, session_date=str(SESSION), alert_rows=rows,
                                          evaluations=evaluations)
    return rows, {json.loads(row["projection"])["contract_id"]: row for row in table.rows.values()}


def test_selection_is_rerun_so_a_later_event_that_clears_the_floor_becomes_the_alert(monkeypatch):
    off_rows, off_alerts = _session_alerts(_run(monkeypatch, None))
    on_rows, on_alerts = _session_alerts(_run(monkeypatch, "ON"))
    # Today: the thin early event is the call's alert (the later one a hit) and the thin put has an alert of its own.
    assert len(off_rows) == 2 and set(off_alerts) == {LONG_CALL.contract_id, LONG_PUT.contract_id}
    assert off_alerts[LONG_CALL.contract_id]["event_id"] == str(THIN_EARLY)
    assert json.loads(off_alerts[LONG_CALL.contract_id]["projection"])["hit_count"] == 2
    # Floor ON: the put never clears it (OI is the prior session's); the call's alert is the later event, entered at
    # its own quote.
    assert len(on_rows) == 1 and set(on_alerts) == {LONG_CALL.contract_id}
    assert on_alerts[LONG_CALL.contract_id]["event_id"] == str(LATER)
    projection = json.loads(on_alerts[LONG_CALL.contract_id]["projection"])
    assert projection["hit_count"] == 1 and projection["event_ids"] == [str(LATER)]


@pytest.mark.parametrize("target,error", [
    ("apply_activity_liquidity_floor", ValueError),
    ("o1_liquidity_floor_entry", AttributeError),
])
def test_a_floor_failure_never_fails_the_window(monkeypatch, capsys, target, error):
    off = _run(monkeypatch, None)
    capsys.readouterr()

    def broken(*args):
        raise error("boom")

    monkeypatch.setattr(adapters, target, broken)
    assert _run(monkeypatch, "RECORD") == off  # RECORD: today's O1
    window = json.loads(capsys.readouterr().err.strip().splitlines()[-1].split(" ", 1)[1])
    assert window["floor_failures"] == {f"O1LF_FLOOR_FAILED:{error.__name__}": 3}
    assert window["below_floor"] == 0
    on = _run(monkeypatch, "ON")  # ON: fail closed, every event left out, the lane says why
    assert on.state.value == "UNAVAILABLE"
    assert on.reason_codes == (f"O1LF_FLOOR_FAILED:{error.__name__.upper()}",)


def test_record_logging_failure_never_fails_the_window(monkeypatch, capsys):
    off = _run(monkeypatch, None)
    capsys.readouterr()

    def broken(**kwargs):
        raise RuntimeError("record")

    monkeypatch.setattr(adapters, "o1_liquidity_floor_documents", broken)
    assert _run(monkeypatch, "RECORD") == off
    (line,) = capsys.readouterr().err.strip().splitlines()
    assert line.startswith("O1_LIQUIDITY_FLOOR_WINDOW ") and '"status": "RECORD_FAILED"' in line


def test_invalid_switch_fails_the_o1_lane_loudly(monkeypatch):
    lane = _invocations(monkeypatch, ROWS)
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_SETTING, "YES")
    with pytest.raises(ValueError, match=O1_LIQUIDITY_FLOOR_SETTING):
        build_advanced_o1_executor(lane)(_context())


def test_hold_switch_still_wins_over_the_floor(monkeypatch):
    monkeypatch.setattr(adapters, "O1_PACKAGE_SELECTION_ENABLED", False)
    rows = _by_event(_run(monkeypatch, "ON"))
    assert {key: row.selection_status for key, row in rows.items()} == {
        THIN_EARLY: "NOT_SELECTED", THIN_PUT: "NOT_SELECTED", LATER: "OBSERVATION"}


def test_record_groups_a_vertical_package_apart_from_the_single_contract():
    single = SimpleNamespace(legs=(SimpleNamespace(contract_ticker=LONG_CALL.contract_ticker),))
    vertical = SimpleNamespace(legs=(SimpleNamespace(contract_ticker=LONG_CALL.contract_ticker),
                                     SimpleNamespace(contract_ticker="O:AAPL261120C00350000")))
    thin, liquid = _source(150, 450), _source(150, 600)
    entries = [
        o1_liquidity_floor_entry(SimpleNamespace(event_id=THIN_EARLY, source=thin, package=single), _v1(thin),
                                 apply_activity_liquidity_floor(thin, _v1(thin))),
        o1_liquidity_floor_entry(SimpleNamespace(event_id=LATER, source=liquid, package=vertical), _v1(liquid),
                                 apply_activity_liquidity_floor(liquid, _v1(liquid))),
    ]
    assert entries[1].variant == "O1_DEBIT_VERTICAL:O:AAPL261120C00350000"
    window, events = o1_liquidity_floor_documents(mode=O1LiquidityFloorMode.RECORD, cutoff=CUTOFF, entries=entries)
    assert (window["alerts_without_floor"], window["alerts_with_floor"], window["alerts_refilled"]) == (2, 1, 0)
    assert [item["variant"] for item in window["instead"]] == [None]
    assert [item["event_id"] for item in events] == [str(THIN_EARLY)]
    empty, none = o1_liquidity_floor_documents(mode=O1LiquidityFloorMode.ON, cutoff=CUTOFF, entries=[])
    assert (empty["o1_events"], empty["below_floor"], none) == (0, 0, [])


def test_lane_inputs_identity_is_unchanged_by_the_switch(monkeypatch):
    """The floor is the detector's, not the loader's: `_load_o1` reads the same rows and returns the same identity."""
    monkeypatch.delenv(O1_DEBIT_VERTICAL_SETTING, raising=False)
    identities = []
    for mode in (None, "RECORD", "ON"):
        if mode is None:
            monkeypatch.delenv(O1_LIQUIDITY_FLOOR_SETTING, raising=False)
        else:
            monkeypatch.setenv(O1_LIQUIDITY_FLOOR_SETTING, mode)
        cursor = _Cursor(list(ROWS))
        lane, identity = loader._load_o1(cursor, _plan(), [])
        identities.append((identity, tuple(cursor.statements)))
    assert identities[0] == identities[1] == identities[2]


# --- thresholds from the environment (Nov 17) ----------------------------------------------------------------------

def test_unset_thresholds_keep_both_policy_hashes():
    for environ in ({}, {key: "" for key in THRESHOLD_SETTINGS}, {key: "  " for key in THRESHOLD_SETTINGS}):
        assert o1_liquidity_floor_policy_from(environ) is ACTIVITY_LIQUIDITY_FLOOR_POLICY
        assert o1_debit_vertical_policy_from(environ).policy_sha256 == AL2_POLICY_FILE_SHA256
    assert ACTIVITY_LIQUIDITY_FLOOR_POLICY.sha256 == FLOOR_POLICY_DEFAULT_SHA256
    assert o1_liquidity_floor_policy().sha256 == FLOOR_POLICY_DEFAULT_SHA256
    assert o1_debit_vertical_policy().policy_sha256 == AL2_POLICY_FILE_SHA256
    # The default values written out explicitly are the same policy.
    same = {O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING: "100", O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING: " 500 ",
            O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING: "250", O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING: "100"}
    assert o1_liquidity_floor_policy_from(same).sha256 == FLOOR_POLICY_DEFAULT_SHA256
    assert o1_debit_vertical_policy_from(same).policy_sha256 == AL2_POLICY_FILE_SHA256


def test_set_thresholds_are_stamped_into_both_policies():
    floor = o1_liquidity_floor_policy_from({O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING: "250",
                                            O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING: "0"})
    assert (floor.minimum_prior_open_interest, floor.minimum_day_volume) == (250, 0)
    assert floor.sha256 != FLOOR_POLICY_DEFAULT_SHA256
    assert floor == ActivityLiquidityFloorPolicy(minimum_prior_open_interest=250, minimum_day_volume=0)
    only_volume = o1_liquidity_floor_policy_from({O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING: "1000"})
    assert (only_volume.minimum_prior_open_interest, only_volume.minimum_day_volume) == (100, 1000)
    legs = o1_debit_vertical_policy_from({O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING: "100",
                                          O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING: "500"})
    assert (legs.minimum_leg_open_interest, legs.minimum_leg_day_volume) == (100, 500)
    assert legs.policy_sha256 != AL2_POLICY_FILE_SHA256
    assert legs.version == "o1-debit-vertical-v1" and legs.maximum_dte == 60  # the rest of the file is kept
    with pytest.raises(ValueError):  # the Contract refuses what the parser would never give it
        ActivityLiquidityFloorPolicy(minimum_prior_open_interest=-1)
    with pytest.raises(ValueError):
        ActivityLiquidityFloorPolicy(minimum_day_volume=True)


@pytest.mark.parametrize("key", THRESHOLD_SETTINGS)
@pytest.mark.parametrize("bad", ["-1", "+5", "1.0", "1e3", "0x10", "abc", "1 000", "\u0663", "ON"])
def test_invalid_thresholds_fail_loudly(key, bad):
    reader = o1_liquidity_floor_policy_from if "LIQUIDITY_FLOOR" in key else o1_debit_vertical_policy_from
    with pytest.raises(ValueError, match=key):
        reader({key: bad})


def test_thresholds_are_read_once_per_process(monkeypatch):
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING, "250")
    monkeypatch.setenv(O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING, "1000")
    started_floor, started_legs = o1_liquidity_floor_policy(), o1_debit_vertical_policy()  # what start validates
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING, "50")
    monkeypatch.setenv(O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING, "10")
    assert o1_liquidity_floor_policy() is started_floor and started_floor.minimum_prior_open_interest == 250
    assert o1_debit_vertical_policy() is started_legs and started_legs.minimum_leg_open_interest == 1000
    o1_liquidity_floor_policy.cache_clear()  # a new process (container recreate) reads the new values
    o1_debit_vertical_policy.cache_clear()
    assert o1_liquidity_floor_policy().minimum_prior_open_interest == 50
    assert o1_debit_vertical_policy().minimum_leg_open_interest == 10


def test_off_ignores_the_thresholds(monkeypatch, capsys):
    unset = _run(monkeypatch, None)
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING, "1")
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING, "1")
    assert _run(monkeypatch, None) == unset
    assert _run(monkeypatch, "OFF") == unset
    assert capsys.readouterr().err == ""


def test_on_with_set_thresholds_uses_and_names_them(monkeypatch, capsys):
    default_on = _by_event(_run(monkeypatch, "ON"))
    capsys.readouterr()
    o1_liquidity_floor_policy.cache_clear()
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING, "20")
    monkeypatch.setenv(O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING, "100")
    policy = ActivityLiquidityFloorPolicy(minimum_prior_open_interest=20, minimum_day_volume=100)
    rows = _by_event(_run(monkeypatch, "ON"))
    # OI 27 / volume 109 and OI 150 / volume 450 clear a 20 / 100 floor: every event is selected again.
    assert {key: row.selection_status for key, row in rows.items()} == {
        THIN_EARLY: "SELECTED", THIN_PUT: "SELECTED", LATER: "SELECTED"}
    for key, row in rows.items():
        assert row.payload["detector_policy_sha256"] == policy.sha256 != FLOOR_POLICY_DEFAULT_SHA256
        assert (row.payload["floor_minimum_prior_open_interest"], row.payload["floor_minimum_day_volume"]) == (20, 100)
        assert row.payload["episode_id"] != default_on[key].payload["episode_id"]  # the episode is per policy
        assert LiquidityFloorActivityFinding.model_validate(row.payload).floor_policy() == policy
    window = json.loads(capsys.readouterr().err.strip().splitlines()[-1].split(" ", 1)[1])
    assert (window["minimum_prior_open_interest"], window["minimum_day_volume"]) == (20, 100)
    assert window["policy_sha256"] == policy.sha256 and window["below_floor"] == 0
    assert default_on[THIN_PUT].payload["floor_minimum_prior_open_interest"] == 100


def test_v2_finding_names_its_own_policy():
    policy = ActivityLiquidityFloorPolicy(minimum_prior_open_interest=20, minimum_day_volume=100)
    source = _source(27, 109)
    floored = apply_activity_liquidity_floor(source, _v1(source), policy)
    assert floored.disposition == "DETECTED" and floored.detector_policy_sha256 == policy.sha256
    assert LiquidityFloorActivityFinding.model_validate_json(floored.canonical_json()) == floored
    document = json.loads(floored.canonical_json())
    with pytest.raises(ValueError, match="unsupported activity policy"):  # thresholds changed, hash not
        LiquidityFloorActivityFinding.model_validate({**document, "floor_minimum_prior_open_interest": 30})
    default = apply_activity_liquidity_floor(source, _v1(source))
    assert default.disposition == "NOT_DETECTED" and default.detector_policy_sha256 == FLOOR_POLICY_DEFAULT_SHA256


def test_al2_rule_and_record_line_use_the_set_leg_thresholds(monkeypatch):
    from test_o1_debit_vertical import _select

    vertical = _select()  # default legs (OI 1000, volume 500 in the fixture) clear 250 / 100
    assert not isinstance(vertical, str)
    o1_debit_vertical_policy.cache_clear()
    monkeypatch.setenv(O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING, "2000")
    assert _select() == "O1DV_LONG_LEG_LIQUIDITY_BELOW_POLICY"
    record = o1_debit_vertical_record(mode=O1DebitVerticalMode.RECORD, cutoff=CUTOFF, entries=[])
    assert (record["minimum_leg_open_interest"], record["minimum_leg_day_volume"]) == (2000, 100)
    assert record["policy_sha256"] == o1_debit_vertical_policy().policy_sha256 != AL2_POLICY_FILE_SHA256
    o1_debit_vertical_policy.cache_clear()
    monkeypatch.delenv(O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING)
    record = o1_debit_vertical_record(mode=O1DebitVerticalMode.RECORD, cutoff=CUTOFF, entries=[])
    assert (record["minimum_leg_open_interest"], record["minimum_leg_day_volume"], record["policy_sha256"]) == (
        250, 100, AL2_POLICY_FILE_SHA256)


@pytest.mark.parametrize("key", (*THRESHOLD_SETTINGS, O1_LIQUIDITY_FLOOR_SETTING, O1_DEBIT_VERTICAL_SETTING))
def test_window_service_refuses_to_start_with_an_invalid_value(monkeypatch, key):
    from scripts import run_advanced_options_v3_production as cli

    def no_database():
        raise AssertionError("PROVIDER_REACHED")

    monkeypatch.setattr(cli, "_provider", no_database)
    monkeypatch.setenv(key, "-1")
    with pytest.raises(ValueError, match=key):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])
    monkeypatch.setenv(key, "")  # valid (unset): the checks pass and the service goes on to its database
    o1_liquidity_floor_policy.cache_clear()
    o1_debit_vertical_policy.cache_clear()
    with pytest.raises(AssertionError, match="PROVIDER_REACHED"):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])
