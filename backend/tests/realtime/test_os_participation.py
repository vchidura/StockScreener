"""G106 AL5 (Nov 17): OS1/OS2 confirmation policy v2 behind REALTIME_V3_OS_PARTICIPATION (OFF / RECORD / ON).

v2: a watched contract of the signal's type confirms by its day participation (day volume at its latest print / prior OI
>= 3, latest print within 30 minutes, the O1 liquidity floor), the signal stays alive through its 30-minute validity,
ON packages the confirming contract at the cutoff mark. RECORD changes nothing and logs one line per live signal and one
per window; an ask-side alternative is recorded only."""
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

import realtime.advanced_options_native_adapters as adapters
import realtime.advanced_options_native_loader as loader
from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision
from options.dual_origin import ActivityLiquidityFloorPolicy
from realtime.advanced_options_cycle import AdvancedOptionsDetectorContext
from realtime.advanced_options_native_adapters import (
    AdvancedOSInvocation,
    AdvancedOSParticipationInvocation,
    AdvancedOptionsLaneInputs,
    build_advanced_os_executor,
)
from realtime.advanced_options_o1_liquidity_floor import (
    O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING,
    O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING,
    o1_liquidity_floor_policy,
)
from realtime.advanced_options_os_participation import (
    OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING,
    OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM_SETTING,
    OS_PARTICIPATION_SETTING,
    OSParticipationLaneRecord,
    OSParticipationMode,
    OSParticipationPrint,
    OSSignalView,
    contract_participation,
    evaluate_os_participation,
    live_signals,
    os_participation_documents,
    os_participation_event_id,
    os_participation_mode,
    os_participation_policy,
    os_participation_policy_from,
)
from realtime.preflight import DetectorFamily
from test_advanced_options_cycle import _gate_and_receipt

UTC = timezone.utc
CUTOFF = datetime(2026, 11, 16, 18, 0, tzinfo=UTC)
POLICY_DEFAULT_SHA256 = "00cd2392eba84adbefb9b1e9ef5a348ba1576c1582d9cef21544e305f1e51d4b"
# flow20 (Nov 17): its settings, cleared at every process start below.
FLOW20_SETTINGS = ("REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM",
                   "REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT",
                   "REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM",
                   "REALTIME_V3_OS_PARTICIPATION_CONFIRMATION")


@pytest.fixture(autouse=True)
def _process_start(monkeypatch):
    for key in (OS_PARTICIPATION_SETTING, OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM_SETTING,
                OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING, O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING,
                O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING, *FLOW20_SETTINGS):
        monkeypatch.delenv(key, raising=False)
    os_participation_policy.cache_clear()
    o1_liquidity_floor_policy.cache_clear()
    yield
    os_participation_policy.cache_clear()
    o1_liquidity_floor_policy.cache_clear()


def _print(ticker, minutes_before, *, oi, volume, size=10, premium="1000", side="AT_OR_ABOVE_ASK", underlyer="NVDA"):
    return OSParticipationPrint(
        contract_ticker=ticker, underlyer=underlyer, call_put=ticker[-9], event_time=CUTOFF - timedelta(minutes=minutes_before),
        size=size, premium=Decimal(premium), side_class=side, multi_leg=False, day_volume=volume, open_interest=oi)


def _signal(direction=-1, detector="OS2", first=True):
    return OSSignalView(detector=detector, signal_id=str(uuid5(NAMESPACE_URL, f"sig:{direction}:{detector}")),
                        underlyer="NVDA", direction=direction, market_time=CUTOFF - timedelta(minutes=5),
                        valid_until=CUTOFF + timedelta(minutes=25), first_window=first)


PUT = "O:NVDA261120P00170000"
PUT2 = "O:NVDA261120P00165000"
CALL = "O:NVDA261120C00180000"


# --- switch and policy -----------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert os_participation_mode({}) is OSParticipationMode.OFF
    assert os_participation_mode({OS_PARTICIPATION_SETTING: " record "}) is OSParticipationMode.RECORD
    assert os_participation_mode({OS_PARTICIPATION_SETTING: "On"}) is OSParticipationMode.ON
    for bad in ("YES", "1", "RECORDED", "SHADOW"):
        with pytest.raises(ValueError, match=OS_PARTICIPATION_SETTING):
            os_participation_mode({OS_PARTICIPATION_SETTING: bad})


def test_policy_hash_names_floor_and_alternative_thresholds():
    policy = os_participation_policy()
    assert policy.policy_sha256 == POLICY_DEFAULT_SHA256
    assert (policy.minimum_prior_open_interest, policy.minimum_day_volume) == (100, 500)
    assert policy.document["version"] == "canonical_stock_option_participation_v2"
    assert policy.document["alternative"]["applied"] is False
    assert (policy.minimum_ask_premium, policy.minimum_ask_share_pct, policy.minimum_classified_volume) == (50000, 60, 200)
    floor = ActivityLiquidityFloorPolicy(minimum_prior_open_interest=250, minimum_day_volume=100)
    assert os_participation_policy_from({}, floor).policy_sha256 != POLICY_DEFAULT_SHA256
    assert os_participation_policy_from({OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING: "55"}).policy_sha256 != (
        POLICY_DEFAULT_SHA256)
    for key, bad in ((OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING, "101"),
                     (OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING, "-1"),
                     (OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM_SETTING, "1e5")):
        with pytest.raises(ValueError, match=key):
            os_participation_policy_from({key: bad})


@pytest.mark.parametrize("key", (OS_PARTICIPATION_SETTING, OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM_SETTING))
def test_window_service_refuses_to_start_with_an_invalid_value(monkeypatch, key):
    from scripts import run_advanced_options_v3_production as cli

    def no_database():
        raise AssertionError("PROVIDER_REACHED")

    monkeypatch.setattr(cli, "_provider", no_database)
    monkeypatch.setenv(key, "-1")
    with pytest.raises(ValueError, match=key):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])
    monkeypatch.setenv(key, "RECORD" if key == OS_PARTICIPATION_SETTING else "75000")
    os_participation_policy.cache_clear()
    with pytest.raises(AssertionError, match="PROVIDER_REACHED"):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])


# --- participation and the decision ----------------------------------------------------------------------------------

def test_participation_takes_the_latest_print_and_the_day_by_aggressor_side():
    prints = [
        _print(PUT, 50, oi=200, volume=300, size=100, premium="5000", side="AT_OR_ABOVE_ASK"),
        _print(PUT, 20, oi=200, volume=700, size=50, premium="2500", side="AT_OR_BELOW_BID"),
        _print(PUT, 10, oi=200, volume=900, size=20, premium="1000", side="INSIDE_SPREAD"),
        _print(PUT, 5, oi=200, volume=950, size=5, premium="200", side="EXCLUDED_COMPLEX"),
        _print(PUT, 1, oi=200, volume=990, size=40, premium="1600", side="QUOTE_STALE"),
        _print(PUT, -1, oi=200, volume=5000, size=4000, premium="1", side="AT_OR_ABOVE_ASK"),  # after the cutoff
    ]
    row = contract_participation(prints, CUTOFF)[PUT]
    assert (row.day_volume, row.open_interest, row.ratio) == (990, 200, Decimal("4.95"))
    assert row.latest_print_at == CUTOFF - timedelta(minutes=1)
    assert (row.ask_volume, row.bid_volume, row.inside_volume, row.complex_volume, row.unclassified_volume) == (
        100, 50, 20, 5, 40)
    assert row.flow_volume == 215 and row.ask_share == Decimal(100) / Decimal(150)
    assert (row.flow_premium, row.ask_premium, row.bid_premium) == (Decimal("10300"), Decimal("5000"), Decimal("2500"))


def _evaluate(prints, signal=None, policy=None, packageable=lambda ticker: True):
    return evaluate_os_participation(signal or _signal(), contract_participation(prints, CUTOFF), cutoff=CUTOFF,
                                     policy=policy or os_participation_policy(), packageable=packageable)


def test_a_watched_contract_with_day_participation_confirms_without_a_print_in_the_last_10_minutes():
    outcome = _evaluate([_print(PUT, 25, oi=200, volume=900), _print(PUT2, 3, oi=5000, volume=6000)])
    assert outcome.confirmed_contract == PUT and outcome.reasons == ()  # ratio 4.5 at a 25-minute-old print
    assert outcome.best["ratio"] == 4.5 and outcome.best["last_print_age_s"] == 1500.0 and outcome.best["watched"]
    assert outcome.packageable and outcome.candidates == 2


def test_recency_edge_and_type_and_underlying():
    assert _evaluate([_print(PUT, 30, oi=200, volume=900)]).reasons == ("OSP_NO_WATCHED_PRINT_WITHIN_30_MIN",)
    assert _evaluate([_print(PUT, 29.99, oi=200, volume=900)]).confirmed
    assert not _evaluate([_print(CALL, 1, oi=200, volume=900)]).confirmed  # bearish signal, a call
    assert not _evaluate([_print(PUT, 1, oi=200, volume=900, underlyer="AAPL")]).confirmed


def test_ratio_threshold_and_floor_on_the_confirming_contract():
    assert _evaluate([_print(PUT, 1, oi=200, volume=599)]).reasons == ("OSP_PARTICIPATION_BELOW_BASELINE",)
    assert _evaluate([_print(PUT, 1, oi=200, volume=600)]).confirmed  # exactly 3
    thin = _evaluate([_print(PUT, 1, oi=42, volume=181)])  # Nov 16 NVDA 11/25 167.5P: ratio 4.3, below the floor
    assert thin.reasons == ("OSP_CONFIRMING_CONTRACT_BELOW_LIQUIDITY_FLOOR",) and not thin.confirmed
    assert thin.best["floor_reasons"] == ["O1LF_OPEN_INTEREST_BELOW_POLICY", "O1LF_DAY_VOLUME_BELOW_POLICY"]
    loose = os_participation_policy_from({}, ActivityLiquidityFloorPolicy(minimum_prior_open_interest=0,
                                                                           minimum_day_volume=0))
    assert _evaluate([_print(PUT, 1, oi=42, volume=181)], policy=loose).confirmed  # the env floor is the one used
    best = _evaluate([_print(PUT, 1, oi=42, volume=400), _print(PUT2, 2, oi=300, volume=1000)])
    assert best.confirmed_contract == PUT2  # the higher ratio fails the floor: the floor-passing one confirms


def test_alternative_is_recorded_only_with_its_reasons():
    rows = [_print(PUT, 5, oi=5000, volume=6000, size=500, premium="60000", side="AT_OR_ABOVE_ASK"),
            _print(PUT, 4, oi=5000, volume=6200, size=200, premium="20000", side="AT_OR_BELOW_BID")]
    outcome = _evaluate(rows)
    assert not outcome.confirmed and outcome.reasons == ("OSP_PARTICIPATION_BELOW_BASELINE",)  # ratio 1.24
    assert outcome.alternative_contract == PUT  # ask premium 60k >= 50k, ask share 71% >= 60%, classified 700
    assert outcome.best_alternative["ask_share"] == round(500 / 700, 4)
    weak = _evaluate([_print(PUT, 5, oi=5000, volume=6000, size=100, premium="60000", side="AT_OR_ABOVE_ASK")])
    assert weak.alternative_contract is None
    assert weak.alternative_reasons == ("OSP_ALT_CLASSIFIED_VOLUME_BELOW_POLICY",)


# --- signals alive through their validity ----------------------------------------------------------------------------

SECURITY = UUID("33333333-3333-4333-8333-333333333333")
SESSION = date(2026, 11, 16)
OPEN = datetime(2026, 11, 16, 14, 30, tzinfo=UTC)


def _bar(interval, start, minutes, close, session=SESSION):
    close = Decimal(str(close))
    return EquityBarRevision(bar_revision_id=uuid5(NAMESPACE_URL, f"bar:{interval}:{start.isoformat()}"),
        security_id=SECURITY, ticker="NVDA", interval=interval, session_date=session, bar_start=start,
        bar_end=start + timedelta(minutes=minutes), open_price=close, high_price=close + 1, low_price=close - 1,
        close_price=close, volume=Decimal("1000"), vwap=None, transaction_count=None,
        source_kind=BarSourceKind.NATIVE_REST, availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
        system_observed_at=start + timedelta(minutes=minutes, seconds=10), replay_available_at=None, adjusted=False,
        payload_sha256="a" * 64, session_scope=BarSessionScope.RTH)


def _bars(*closes):
    daily = [_bar("1d", datetime(2026, 10, 27, 14, 30, tzinfo=UTC) + timedelta(days=index), 390, 100,
                  session=date(2026, 10, 27) + timedelta(days=index)) for index in range(15)]
    five = [_bar("5m", OPEN + timedelta(minutes=5 * index), 5, close) for index, close in enumerate(closes)]
    return tuple((item, item.system_observed_at) for item in (*daily, *five))


def _live(bars, equity_cutoff, decision_at):
    return live_signals(security_id=SECURITY, underlyer="NVDA", bars=bars, equity_cutoff=equity_cutoff,
                        decision_at=decision_at, session_close=datetime(2026, 11, 16, 21, tzinfo=UTC))


def test_a_signal_stays_alive_through_its_30_minute_validity():
    # OS1 bullish on the 14:35-14:40 bar (inside 99-101, then 101.1); later bars stay above the band.
    bars = _bars(100, 101.1, 101.5, 101.6, 101.7, 101.8, 101.9, 102.0, 102.1)
    first = _live(bars, OPEN + timedelta(minutes=10), OPEN + timedelta(minutes=10, seconds=40))
    os1 = [(signal, flag) for signal, flag in first if signal.detector_id == "OS1"]
    assert len(os1) == 1 and os1[0][1] is True and os1[0][0].market_time == OPEN + timedelta(minutes=10)
    signal_id = os1[0][0].signal_id
    later = _live(bars, OPEN + timedelta(minutes=35), OPEN + timedelta(minutes=39, seconds=50))
    kept = [(signal, flag) for signal, flag in later if signal.signal_id == signal_id]
    assert len(kept) == 1 and kept[0][1] is False  # still alive 29 minutes later, not its first window
    assert not [s for s, _ in _live(bars, OPEN + timedelta(minutes=40), OPEN + timedelta(minutes=40, seconds=5))
                if s.signal_id == signal_id]  # expired at bar end + 30 minutes
    # v1 sees it only in its own window: the detector on the latest bars alone.
    from options.stock_bar_detection import detect_canonical_stock_signals

    latest_only = detect_canonical_stock_signals(
        security_id=SECURITY, underlyer="NVDA", bars=bars, market_cutoff=OPEN + timedelta(minutes=35),
        decision_at=OPEN + timedelta(minutes=39, seconds=50), session_close=datetime(2026, 11, 16, 21, tzinfo=UTC),
        detector_ids=("OS1", "OS2"))
    assert signal_id not in {s.signal_id for s in latest_only}


# --- the executor: RECORD changes nothing, ON decides ------------------------------------------------------------------

def _os1_fixture():
    """The OS1 executor test's signal and source (v1 CONFIRMED, no package)."""
    captured = {}
    original = adapters.build_advanced_os_executor

    def capture(detector, inputs):
        captured["inputs"] = inputs
        return original(detector, inputs)

    adapters.build_advanced_os_executor = capture
    try:
        import test_advanced_options_native_adapters as neighbour

        neighbour.build_advanced_os_executor = capture
        neighbour.test_os1_executor_reuses_canonical_stock_first_model()
    finally:
        adapters.build_advanced_os_executor = original
        neighbour.build_advanced_os_executor = original
    (invocation,) = captured["inputs"].invocations
    return invocation


def _context(cutoff):
    gate, receipt = _gate_and_receipt()
    return AdvancedOptionsDetectorContext(gate, receipt, DetectorFamily.OS1, cutoff)


def _record(invocation, mode, outcomes=None, status="OK"):
    signal = invocation.signal
    view = OSSignalView("OS1", str(signal.signal_id), signal.underlyer, int(signal.direction), signal.market_time,
                        signal.valid_until, True)
    if outcomes is None:
        outcomes = (evaluate_os_participation(view, {}, cutoff=signal.received_at, policy=os_participation_policy()),)
    return OSParticipationLaneRecord("OS1", mode, os_participation_policy(), signal.received_at, outcomes=outcomes,
                                     status=status)


def test_record_keeps_today_and_logs_one_line_per_signal_and_a_window_line(capsys):
    invocation = _os1_fixture()
    capsys.readouterr()
    cutoff = invocation.signal.received_at + timedelta(minutes=1)
    plain = AdvancedOptionsLaneInputs((invocation,))
    recorded = AdvancedOptionsLaneInputs((invocation,), record=_record(invocation, OSParticipationMode.RECORD))
    assert plain == recorded  # the record is never part of the lane's identity or comparison
    today = build_advanced_os_executor(DetectorFamily.OS1, plain)(_context(cutoff))
    assert capsys.readouterr().err == ""
    with_record = build_advanced_os_executor(DetectorFamily.OS1, recorded)(_context(cutoff))
    assert with_record == today
    lines = capsys.readouterr().err.strip().splitlines()
    assert [line.split(" ", 1)[0] for line in lines] == ["OS_PARTICIPATION_RECORD", "OS_PARTICIPATION_WINDOW"]
    line, window = (json.loads(item.split(" ", 1)[1]) for item in lines)
    assert line["v1"]["outcome"] == "CONFIRMED" and line["v1"]["pairings"] == 1
    assert line["v2"]["outcome"] == "UNMATCHED" and line["v2"]["reasons"] == ["OSP_NO_WATCHED_PRINT_WITHIN_30_MIN"]
    assert line["alternative"]["applied"] is False and line["first_window"] is True
    assert line["policy_sha256"] == POLICY_DEFAULT_SHA256
    assert (window["signals"], window["v1_confirmed"], window["v2_confirmed"], window["status"]) == (1, 1, 0, "OK")
    assert window["minimum_prior_open_interest"] == 100 and window["alternative"]["minimum_ask_share_pct"] == 60


def test_record_failure_never_fails_the_window(monkeypatch, capsys):
    invocation = _os1_fixture()
    capsys.readouterr()
    cutoff = invocation.signal.received_at + timedelta(minutes=1)
    import realtime.advanced_options_os_participation as module

    def broken(record, v1):
        raise RuntimeError("record")

    monkeypatch.setattr(module, "os_participation_documents", broken)
    lane = AdvancedOptionsLaneInputs((invocation,), record=_record(invocation, OSParticipationMode.RECORD))
    today = build_advanced_os_executor(DetectorFamily.OS1, AdvancedOptionsLaneInputs((invocation,)))(_context(cutoff))
    assert build_advanced_os_executor(DetectorFamily.OS1, lane)(_context(cutoff)) == today
    (line,) = capsys.readouterr().err.strip().splitlines()
    assert line.startswith("OS_PARTICIPATION_WINDOW ") and '"status": "RECORD_FAILED"' in line


def test_on_evaluates_v2_invocations_and_packages_the_confirming_contract(capsys):
    from test_option_portal_adapter import _package, _policy

    invocation = _os1_fixture()
    capsys.readouterr()
    signal = invocation.signal
    cutoff = signal.received_at + timedelta(minutes=1)
    view = OSSignalView("OS1", str(signal.signal_id), signal.underlyer, 1, signal.market_time, signal.valid_until, False)
    call = f"O:{signal.underlyer}261120C00180000"
    prints = [OSParticipationPrint(call, signal.underlyer, "C", cutoff - timedelta(minutes=20), 10, Decimal("1000"),
                                   "AT_OR_ABOVE_ASK", False, 1500, 300)]
    outcome = evaluate_os_participation(view, contract_participation(prints, cutoff), cutoff=cutoff,
                                        policy=os_participation_policy(), packageable=lambda ticker: True)
    assert outcome.confirmed_contract == call
    unconfirmed = replace(outcome, confirmed_contract=None, reasons=("OSP_PARTICIPATION_BELOW_BASELINE",))
    package = _package(_policy().policy_sha256)
    event = os_participation_event_id("OS1", str(signal.signal_id), call)
    record = OSParticipationLaneRecord("OS1", OSParticipationMode.ON, os_participation_policy(), cutoff,
                                       outcomes=(outcome,))
    lane = AdvancedOptionsLaneInputs((
        AdvancedOSParticipationInvocation(event, signal, outcome, package, os_participation_policy().policy_sha256),
        AdvancedOSParticipationInvocation(os_participation_event_id("OS1", str(signal.signal_id), None), signal,
                                          unconfirmed, None, os_participation_policy().policy_sha256),
    ), record=record)
    result = build_advanced_os_executor(DetectorFamily.OS1, lane)(_context(cutoff))
    selected, other = result.evaluations
    assert selected.selection_status == "SELECTED" and selected.package == package
    assert selected.payload["contract"] == "advanced-options-stock-first-evaluation-v2"
    assert selected.payload["decision"]["disposition"] == "CONFIRMED" and selected.payload["decision"]["direction"] == 1
    assert selected.payload["decision"]["policy_sha256"] == POLICY_DEFAULT_SHA256
    assert other.selection_status == "NOT_SELECTED" and other.package is None
    window = json.loads(capsys.readouterr().err.strip().splitlines()[-1].split(" ", 1)[1])
    assert window["mode"] == "ON" and window["v2_confirmed"] == 1 and window["v1_signals_evaluated"] == 0


def test_blocked_lane_records_why_and_stays_blocked(capsys):
    record = OSParticipationLaneRecord("OS1", OSParticipationMode.RECORD, os_participation_policy(), CUTOFF,
                                       status="LANE_BLOCKED", reasons=("OS1_BAR_MEMBERSHIP_UNAVAILABLE",))
    lane = AdvancedOptionsLaneInputs(blocked_reason_codes=("OS1_BAR_MEMBERSHIP_UNAVAILABLE",), record=record)
    result = build_advanced_os_executor(DetectorFamily.OS1, lane)(_context(CUTOFF))
    assert result.state.value == "BLOCKED"
    window = json.loads(capsys.readouterr().err.strip().splitlines()[-1].split(" ", 1)[1])
    assert window["status"] == "LANE_BLOCKED" and window["lane_reasons"] == ["OS1_BAR_MEMBERSHIP_UNAVAILABLE"]


# --- the loader: RECORD keeps the lanes, ON replaces them, failures never fail the window ------------------------------

def _plan():
    return SimpleNamespace(
        cutoff=CUTOFF, unmarked_instrument_ids=(),
        control_plane=SimpleNamespace(cohort_id=UUID("b096f5b7-2371-5522-925d-cf4db3912b9c"), session_date=SESSION),
        rolling_state=SimpleNamespace(state_id=UUID("11111111-1111-4111-8111-111111111111")),
    )


class _Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql.split()[0] if sql.split()[0] in ("SAVEPOINT", "RELEASE", "ROLLBACK") else sql[:40])

    def fetchall(self):
        return []


def _lanes():
    invocation = _os1_fixture()
    return invocation, {
        DetectorFamily.OS1: (AdvancedOptionsLaneInputs((invocation,)), ("os1-identity",)),
        DetectorFamily.OS2: (AdvancedOptionsLaneInputs(blocked_reason_codes=("OS2_X",)), {"blocked_reason_codes": ("OS2_X",)}),
    }


def test_record_keeps_lanes_and_identities_and_attaches_the_record(monkeypatch):
    invocation, lanes = _lanes()
    signal = invocation.signal

    def inputs(cursor, plan, captured):
        return {"OS1": [(signal, True)], "OS2": []}, (), None

    monkeypatch.setattr(loader, "_os_participation_inputs", inputs)
    cursor = _Cursor()
    (os1, os1_identity), (os2, os2_identity) = loader._apply_os_participation(
        cursor, _plan(), OSParticipationMode.RECORD, lanes, {})
    assert os1 == lanes[DetectorFamily.OS1][0] and os1_identity == ("os1-identity",)
    assert os2 == lanes[DetectorFamily.OS2][0] and os2_identity == {"blocked_reason_codes": ("OS2_X",)}
    assert os1.record.status == "OK" and len(os1.record.outcomes) == 1
    assert os2.record.status == "LANE_BLOCKED"
    assert cursor.statements == ["SAVEPOINT", "RELEASE"]


def test_inputs_failure_keeps_v1_in_record_and_blocks_v2_lanes_on(monkeypatch):
    invocation, lanes = _lanes()

    def broken(cursor, plan, captured):
        raise RuntimeError("read")

    monkeypatch.setattr(loader, "_os_participation_inputs", broken)
    (os1, os1_identity), _ = loader._apply_os_participation(_Cursor(), _plan(), OSParticipationMode.RECORD, lanes, {})
    assert os1 == lanes[DetectorFamily.OS1][0] and os1.record.status == "OS_PARTICIPATION_INPUTS_UNAVAILABLE:RuntimeError"
    cursor = _Cursor()
    (os1, os1_identity), _ = loader._apply_os_participation(cursor, _plan(), OSParticipationMode.ON, lanes, {})
    assert os1.blocked_reason_codes == ("OS_PARTICIPATION_INPUTS_UNAVAILABLE:RUNTIMEERROR",)
    assert cursor.statements == ["SAVEPOINT", "ROLLBACK"]


def test_on_replaces_the_pairings_with_one_v2_invocation_per_live_signal(monkeypatch):
    invocation, lanes = _lanes()
    signal = invocation.signal
    call = f"O:{signal.underlyer}261120C00180000"
    prints = (OSParticipationPrint(call, signal.underlyer, "C", CUTOFF - timedelta(minutes=5), 10, Decimal("1000"),
                                   "AT_OR_ABOVE_ASK", False, 1500, 300),)
    contract = SimpleNamespace(instrument_id=9001, contract_id=101, catalog_version_id=7, contract_ticker=call,
                               call_put="C", strike=Decimal("180"), expiration=date(2026, 11, 20), multiplier=100)
    mark = SimpleNamespace(instrument_id=9001, bid=Decimal("1.0"), ask=Decimal("1.2"), mark=Decimal("1.1"),
                           market_time=CUTOFF - timedelta(milliseconds=300), evidence_id=uuid5(NAMESPACE_URL, "m"))
    vertical = SimpleNamespace(by_ticker={call: contract}, marks={9001: mark})

    def inputs(cursor, plan, captured):
        return {"OS1": [(signal, False)], "OS2": []}, prints, vertical

    monkeypatch.setattr(loader, "_os_participation_inputs", inputs)
    monkeypatch.setattr(loader, "_packages_for_events", lambda cursor, **kwargs: {})
    (os1, os1_identity), _ = loader._apply_os_participation(_Cursor(), _plan(), OSParticipationMode.ON, lanes, {})
    (v2,) = os1.invocations
    assert isinstance(v2, AdvancedOSParticipationInvocation)
    assert v2.event_id == os_participation_event_id("OS1", str(signal.signal_id), call)
    assert v2.package.legs[0].contract_ticker == call and v2.package.legs[0].entry_mark == Decimal("1.1")
    assert v2.package.candidate_id == loader.options_alert_v1_candidate_id(_plan().control_plane.cohort_id, v2.event_id)
    assert os1_identity[0][2] == call and os1_identity[0][4] == POLICY_DEFAULT_SHA256


def test_documents_classify_v1_outcomes():
    record = OSParticipationLaneRecord("OS2", OSParticipationMode.RECORD, os_participation_policy(), CUTOFF, outcomes=(
        _evaluate([], signal=_signal(first=True)), _evaluate([], signal=_signal(direction=1, first=False))))
    lines, window = os_participation_documents(record, {})
    assert [line["v1"]["outcome"] for line in lines] == ["NOT_EVALUATED_NO_PAIRING", "NOT_LIVE_IN_V1"]
    assert window["signals"] == 2 and window["first_window"] == 1


def _row(bar):
    return {
        "bar_revision_id": str(bar.bar_revision_id), "security_id": str(bar.security_id), "ticker": bar.ticker,
        "interval": bar.interval, "session_date": bar.session_date, "bar_start": bar.bar_start, "bar_end": bar.bar_end,
        "open_price": bar.open_price, "high_price": bar.high_price, "low_price": bar.low_price,
        "close_price": bar.close_price, "volume": bar.volume, "source_kind": bar.source_kind.value,
        "availability_mode": bar.availability_mode.value, "is_final": True, "observed_at": bar.system_observed_at,
        "adjustment_basis": "RAW_UNADJUSTED", "payload_sha256": bar.payload_sha256, "quality_codes": [],
        "session_scope": "RTH", "created_at": bar.system_observed_at,
    }


def test_inputs_read_keeps_earlier_signals_and_reads_prints_only_for_their_underlyings(monkeypatch):
    """The real `_os_participation_inputs` over a fake cursor: the extended 5m read keeps the 14:40 OS1 signal alive
    at 15:09:50, the prints read is bounded to the cohort, session, flow, committed offset, cutoff and underlyings."""
    bars = _bars(100, 101.1, 101.5, 101.6, 101.7, 101.8, 101.9)
    daily = {"NVDA": [item for item in bars if item[0].interval == "1d"]}
    five = [_row(item[0]) for item in bars if item[0].interval == "5m"]
    equity_cutoff = OPEN + timedelta(minutes=35)
    calls = []

    def known(cursor, **kwargs):
        calls.append(kwargs)
        return {77: [row for row in five if row["bar_end"] <= kwargs["end"]]}

    monkeypatch.setattr(loader, "load_known_bar_revisions_cursor", known)
    monkeypatch.setattr(loader, "load_o1_vertical_inputs_cursor", lambda cursor, plan: "VERTICAL")

    class Cursor:
        def __init__(self):
            self.statements, self.result = [], []

        def execute(self, sql, params=None):
            self.statements.append((sql, params))
            if sql.startswith("SELECT instrument_id,security_id"):
                self.result = [{"instrument_id": 77, "security_id": str(SECURITY)}]
            elif sql == loader._OS_PARTICIPATION_PRINTS_SQL:
                self.result = [{"contract_ticker": "O:NVDA261120C00180000", "underlyer": "NVDA", "call_put": "C",
                                "event_time": OPEN + timedelta(minutes=30), "size": 10, "premium": Decimal("1000"),
                                "side_class": "AT_OR_ABOVE_ASK", "is_multi_leg": False,
                                "cumulative_volume_after": 1500, "prior_open_interest": 300}]
            else:
                raise AssertionError(sql)

        def fetchall(self):
            return self.result

    plan = SimpleNamespace(
        cutoff=OPEN + timedelta(minutes=39, seconds=50),
        equity_input_receipt=SimpleNamespace(cutoff=equity_cutoff),
        enrollment=SimpleNamespace(equity_plan=SimpleNamespace(security_ids=(("NVDA", SECURITY),))),
        control_plane=SimpleNamespace(universe=SimpleNamespace(option_underlyings=("NVDA",)),
                                      cohort_id=UUID("b096f5b7-2371-5522-925d-cf4db3912b9c"), session_date=SESSION),
        source_flow=SimpleNamespace(source_flow_id=UUID("22222222-2222-4222-8222-222222222222")),
        rolling_state=SimpleNamespace(committed_offset=10),
    )
    captured = {DetectorFamily.OS1: {"by_ticker": daily, "signals": (), "known_by": equity_cutoff}, DetectorFamily.OS2: {}}
    cursor = Cursor()
    signals, prints, vertical = loader._os_participation_inputs(cursor, plan, captured)
    assert calls[0]["latest"] == 9 and calls[0]["interval"] == "5m" and calls[0]["end"] == equity_cutoff
    (os1,) = signals["OS1"]
    assert os1[0].market_time == OPEN + timedelta(minutes=10) and os1[1] is False  # alive 29 minutes on
    sql, params = cursor.statements[-1]
    assert sql == loader._OS_PARTICIPATION_PRINTS_SQL and params[-1] == ["NVDA"] and params[3] == 10
    assert params[5] == plan.cutoff
    assert prints[0].day_volume == 1500 and prints[0].call_put == "C" and vertical == "VERTICAL"
    outcome = evaluate_os_participation(OSSignalView("OS1", str(os1[0].signal_id), "NVDA", 1, os1[0].market_time,
                                                     os1[0].valid_until, False),
                                        contract_participation(prints, plan.cutoff), cutoff=plan.cutoff,
                                        policy=os_participation_policy())
    assert outcome.confirmed_contract == "O:NVDA261120C00180000"
    # No live signal: no prints read and no marks read.
    quiet = {DetectorFamily.OS1: {"by_ticker": daily, "signals": (), "known_by": equity_cutoff}}
    flat = [_row(item[0]) for item in _bars(100, 100.2, 100.1) if item[0].interval == "5m"]
    monkeypatch.setattr(loader, "load_known_bar_revisions_cursor", lambda cursor, **kwargs: {77: flat})
    cursor = Cursor()
    assert loader._os_participation_inputs(cursor, plan, quiet) == ({"OS1": [], "OS2": []}, (), None)
    assert all(sql != loader._OS_PARTICIPATION_PRINTS_SQL for sql, _ in cursor.statements)


# --- flow20: the name-level flow confirmation, recorded (Nov 17) -----------------------------------------------------

from realtime.advanced_options_os_participation import (  # noqa: E402
    OS_PARTICIPATION_CONFIRMATION_SETTING,
    OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING,
    OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING,
    OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING,
    Flow20Tally,
    flow20_by_underlying,
    flow20_record,
    signal_levels,
    signal_view,
)


def _fprint(ticker, minutes_before, side, premium, *, confidence="HIGH", revision=1, underlyer="NVDA"):
    return OSParticipationPrint(
        contract_ticker=ticker, underlyer=underlyer, call_put=ticker[-9],
        event_time=CUTOFF - timedelta(minutes=minutes_before), size=10, premium=Decimal(premium), side_class=side,
        multi_leg=False, day_volume=900, open_interest=200, side_confidence=confidence, revision=revision)


FLOW = [
    _fprint(CALL, 19.9, "AT_OR_ABOVE_ASK", "30000"),                       # bull
    _fprint(PUT, 10, "AT_OR_BELOW_BID", "20000", confidence="MEDIUM"),     # bull (sold put)
    _fprint(PUT, 5, "AT_OR_ABOVE_ASK", "120000"),                          # bear
    _fprint(CALL, 1, "AT_OR_BELOW_BID", "10000"),                          # bear (sold call)
    _fprint(CALL, 0, "INSIDE_SPREAD", "40000"),                            # total only
    _fprint(PUT, 3, "AT_OR_ABOVE_ASK", "99000", confidence="LOW"),         # total only
    _fprint(PUT, 2, "AT_OR_ABOVE_ASK", "77000", revision=2),               # a correction: not counted
    _fprint(CALL, 20, "AT_OR_ABOVE_ASK", "55000"),                         # exactly 20 minutes before: out
    _fprint(CALL, -0.5, "AT_OR_ABOVE_ASK", "66000"),                       # after the cutoff: out
    _fprint(CALL, 4, "AT_OR_ABOVE_ASK", "11000", underlyer="AAPL"),
]


def test_flow20_tally_is_the_research_bias_over_all_watched_contracts_of_the_name():
    tallies = flow20_by_underlying(FLOW, CUTOFF)
    nvda = tallies["NVDA"]
    assert (nvda.bull_premium, nvda.bear_premium) == (Decimal("50000"), Decimal("130000"))
    assert (nvda.total_premium, nvda.prints) == (Decimal("319000"), 6)
    assert tallies["AAPL"].bull_premium == Decimal("11000")


def test_flow20_record_net_imbalance_coverage_and_the_research_default():
    policy = os_participation_policy()
    tally = flow20_by_underlying(FLOW, CUTOFF)["NVDA"]
    bear = flow20_record(tally, -1, policy, CUTOFF)
    assert (bear["net_premium"], bear["classified_premium"], bear["imbalance"]) == ("80000", "180000", -0.4444)
    assert bear["coverage"] == round(180000 / 319000, 4) and bear["confirmed"] and bear["reasons"] == []
    bull = flow20_record(tally, 1, policy, CUTOFF)
    assert bull["net_premium"] == "-80000" and not bull["confirmed"]
    assert bull["reasons"] == ["FLOW20_NET_NOT_ABOVE_MINIMUM"]
    empty = flow20_record(None, 1, policy, CUTOFF)
    assert not empty["confirmed"] and empty["imbalance"] is None and empty["coverage"] is None
    tied = flow20_record(Flow20Tally(Decimal(5), Decimal(5), Decimal(10), 2), 1, policy, CUTOFF)
    assert not tied["confirmed"]  # the research bias > 0 is strict


def test_flow20_thresholds_from_the_environment():
    tally = flow20_by_underlying(FLOW, CUTOFF)["NVDA"]  # net 80k bearish, |imbalance| 44%, classified 180k
    for env, confirmed in (
        ({OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING: "79999"}, True),
        ({OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING: "80000"}, False),
        ({OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING: "44"}, True),
        ({OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING: "45"}, False),
        ({OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING: "180000"}, True),
        ({OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING: "180001"}, False),
    ):
        policy = os_participation_policy_from(env)
        assert flow20_record(tally, -1, policy, CUTOFF)["confirmed"] is confirmed, env
        assert policy.policy_sha256 == POLICY_DEFAULT_SHA256  # flow20 thresholds: their own document and hash
        assert policy.flow20_sha256 != os_participation_policy_from({}).flow20_sha256
    for key, bad in ((OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING, "101"),
                     (OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING, "-1"),
                     (OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING, "1e5"),
                     (OS_PARTICIPATION_CONFIRMATION_SETTING, "BOTH")):
        with pytest.raises(ValueError, match=key):
            os_participation_policy_from({key: bad})
    assert os_participation_policy_from({OS_PARTICIPATION_CONFIRMATION_SETTING: " ratio "}).policy_sha256 == (
        POLICY_DEFAULT_SHA256)
    flow = os_participation_policy_from({OS_PARTICIPATION_CONFIRMATION_SETTING: "flow20"})
    assert flow.confirmation == "FLOW20" and flow.policy_sha256 != POLICY_DEFAULT_SHA256
    assert flow.document["confirmation"]["flow20_sha256"] == flow.flow20_sha256


@pytest.mark.parametrize("key", (OS_PARTICIPATION_CONFIRMATION_SETTING, OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING))
def test_window_service_refuses_to_start_with_an_invalid_flow20_value(monkeypatch, key):
    from scripts import run_advanced_options_v3_production as cli

    def no_database():
        raise AssertionError("PROVIDER_REACHED")

    monkeypatch.setattr(cli, "_provider", no_database)
    monkeypatch.setenv(key, "200")
    with pytest.raises(ValueError, match=key):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])


def test_flow20_is_recorded_and_changes_nothing_with_the_ratio_confirmation():
    prints = [_print(PUT, 25, oi=200, volume=900), *FLOW]
    tallies = flow20_by_underlying(prints, CUTOFF)
    plain = _evaluate(prints)
    with_flow = evaluate_os_participation(_signal(), contract_participation(prints, CUTOFF), cutoff=CUTOFF,
                                          policy=os_participation_policy(), packageable=lambda ticker: True,
                                          flow20=tallies["NVDA"])
    assert (with_flow.confirmed_contract, with_flow.reasons, with_flow.best) == (
        plain.confirmed_contract, plain.reasons, plain.best)
    assert with_flow.flow20["confirmed"] is True and plain.flow20["confirmed"] is False  # no tally: nothing classified


def test_flow20_on_path_confirms_by_the_names_flow_and_picks_the_bought_contract():
    policy = os_participation_policy_from({OS_PARTICIPATION_CONFIRMATION_SETTING: "FLOW20"})
    thin = PUT2.replace("165", "160")
    prints = [_print(PUT, 5, oi=5000, volume=6000, size=50, premium="9000", side="AT_OR_ABOVE_ASK"),
              _print(PUT2, 4, oi=5000, volume=6000, size=80, premium="40000", side="AT_OR_ABOVE_ASK"),
              _print(thin, 3, oi=40, volume=6000, size=900, premium="900000", side="AT_OR_ABOVE_ASK")]  # below floor
    participation = contract_participation(prints, CUTOFF)
    bearish = Flow20Tally(Decimal(0), Decimal(100000), Decimal(100000), 3)

    def run(tally, rows=participation):
        return evaluate_os_participation(_signal(), rows, cutoff=CUTOFF, policy=policy,
                                         packageable=lambda ticker: True, flow20=tally)

    outcome = run(bearish)  # ratio 1.2: the ratio confirmation would not confirm
    assert outcome.confirmed_contract == PUT2 and outcome.reasons == () and outcome.packageable
    assert outcome.best["contract"] == PUT2
    assert run(Flow20Tally(Decimal(100000), Decimal(0), Decimal(100000), 3)).reasons == ("OSP_FLOW20_NOT_CONFIRMED",)
    assert run(bearish, {}).reasons == ("OSP_FLOW20_NO_CONFIRMING_CONTRACT",)


def test_flow20_and_levels_in_the_record_and_window_lines():
    invocation = _os1_fixture()
    signal = invocation.signal
    levels = signal_levels(signal)
    close, atr = signal.bars[-1].close, signal.prior_atr14
    assert levels["pdc"] == str(signal.prior_close) and levels["pdh"] == str(signal.daily_bars[-1].high)
    assert levels["pdc_distance_atr"] == round(float((close - signal.prior_close) / atr), 3)
    assert signal_levels(SimpleNamespace()) is None
    view = signal_view(signal, True)
    assert view.levels == levels
    policy = os_participation_policy()
    bull = Flow20Tally(Decimal(90000), Decimal(10000), Decimal(120000), 4)
    up = replace(view, signal_id="up", direction=1)
    down = replace(view, signal_id="down", direction=-1)
    outcomes = tuple(evaluate_os_participation(item, {}, cutoff=CUTOFF, policy=policy, flow20=bull)
                     for item in (up, down))
    record = OSParticipationLaneRecord("OS1", OSParticipationMode.RECORD, policy, CUTOFF, outcomes=outcomes)
    lines, window = os_participation_documents(record, {})
    assert lines[0]["flow20"]["confirmed"] is True and lines[0]["flow20"]["imbalance"] == 0.8
    assert lines[0]["levels"] == levels and lines[1]["flow20"]["confirmed"] is False
    assert window["flow20_signals"] == {"bull": 1, "bear": 1}
    assert window["flow20_confirmed"] == {"bull": 1, "bear": 0}
    assert window["confirmation"] == "RATIO" and window["flow20"] == {
        "minimum_net_premium": 0, "minimum_imbalance_pct": 0, "minimum_classified_premium": 0}
    assert window["flow20_sha256"] == policy.flow20_sha256


def test_the_loader_passes_each_names_flow20_and_reads_confidence_and_revision(monkeypatch):
    invocation, lanes = _lanes()
    signal = invocation.signal
    call = f"O:{signal.underlyer}261120C00180000"
    plan = _plan()
    prints = (OSParticipationPrint(call, signal.underlyer, "C", plan.cutoff - timedelta(minutes=5), 10,
                                   Decimal("50000"), "AT_OR_ABOVE_ASK", False, 1500, 300, "HIGH", 1),)

    def inputs(cursor, plan, captured):
        return {"OS1": [(signal, True)], "OS2": []}, prints, None

    monkeypatch.setattr(loader, "_os_participation_inputs", inputs)
    (os1, _), _ = loader._apply_os_participation(_Cursor(), plan, OSParticipationMode.RECORD, lanes, {})
    (outcome,) = os1.record.outcomes
    expected = flow20_record(flow20_by_underlying(prints, plan.cutoff).get(signal.underlyer), int(signal.direction),
                             os_participation_policy(), plan.cutoff)
    assert outcome.flow20 == expected and outcome.signal.levels == signal_levels(signal)
    assert outcome.flow20["confirmed"] is (int(signal.direction) == 1)
    assert "event.side_confidence,event.revision " in loader._OS_PARTICIPATION_PRINTS_SQL
