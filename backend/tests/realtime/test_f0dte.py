"""G106 D5 (Nov 18): F0DTE, the 0DTE intraday lane on the Options window, behind REALTIME_V3_F0DTE (OFF / RECORD / ON).

OFF: nothing at all (no read, no line, the O1 lane byte for byte). RECORD: F0DTE_RECORD / F0DTE_ALERT_RECORD /
F0DTE_OUTCOME lines, no evaluation. ON: each alert is a SELECTED evaluation with a single-leg long package in the O1 lane,
shown as detector F0DTE. Any error: one RECORD_FAILED line and a DEGRADED fault; the window goes on.
"""
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

import realtime.advanced_options_f0dte as f0dte
from realtime.advanced_options_f0dte import (
    F0DTE_AGGRESSOR_UNCLEAR,
    F0DTE_CAP_REPEAT_CONTRACT,
    F0DTE_CAP_UNDERLYING_HOUR,
    F0DTE_CAP_WINDOW,
    F0DTE_CONFLICTING_DIRECTION,
    F0DTE_DELTA_OUTSIDE_RANGE,
    F0DTE_MID_OUTSIDE_RANGE,
    F0DTE_NO_FLOW_EVIDENCE,
    F0DTE_OUTSIDE_ENTRY_TIME,
    F0DTE_POLICY,
    F0DTE_PREMIUM_BELOW_THRESHOLD,
    F0DTE_PRINTS_UNAVAILABLE,
    F0DTE_SETTING,
    F0DTE_SOLD,
    F0DTE_SPREAD_TOO_WIDE,
    F0DTE_UNPACKAGEABLE,
    F0DTE_VOLUME_BELOW_THRESHOLD,
    F0DTEContract,
    F0DTEMode,
    F0DTEPolicy,
    F0DTEPrintsUnavailable,
    F0DTEPriorAlert,
    F0DTEQuote,
    F0DTEState,
    F0DTETally,
    JournalEvent,
    evaluate_f0dte_window,
    f0dte_mode,
    f0dte_policy,
    f0dte_policy_from,
    fold_contract_events,
    implied_delta,
)
from realtime.option_flow.volume import BaselineKind
from realtime.preflight import DetectorFamily

UTC = timezone.utc
SESSION = date(2026, 11, 18)
OPEN = datetime(2026, 11, 18, 14, 30, tzinfo=UTC)
CLOSE = datetime(2026, 11, 18, 21, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 11, 18, 16, 0, tzinfo=UTC)
COHORT = UUID("b096f5b7-2371-5522-925d-cf4db3912b9c")
STATE = UUID("11111111-1111-4111-8111-111111111111")
SPOT = Decimal("680")


def _contract(instrument_id, strike, side="C", underlyer="SPY"):
    ticker = f"O:{underlyer}{SESSION:%y%m%d}{side}{int(Decimal(strike) * 1000):08d}"
    return F0DTEContract(instrument_id=instrument_id, contract_id=instrument_id - 8000,
                         catalog_version_id=instrument_id + 1000, contract_ticker=ticker, underlyer=underlyer,
                         contract_type="CALL" if side == "C" else "PUT", strike=Decimal(strike),
                         expiration_date=SESSION, multiplier=100)


CALL = _contract(9001, "681")
PUT = _contract(9002, "679", "P")
CALL_2 = _contract(9003, "682")
QQQ_CALL = _contract(9101, "600", underlyer="QQQ")


def _tally(ask, bid, volume=4000, inside=0, trades=200):
    ask, bid, inside = Decimal(ask), Decimal(bid), Decimal(inside)
    return F0DTETally(trades=trades, volume=volume, premium=ask + bid + inside, ask_premium=ask, bid_premium=bid,
                      inside_premium=inside)


def _quotes(*pairs):
    return {contract.contract_ticker: F0DTEQuote(Decimal(bid), Decimal(ask), CUTOFF - timedelta(seconds=1))
            for contract, bid, ask in pairs}


# 16:00: the window is the 18th; day volume 20,000 at the cutoff with 4,000 in the window -> 16,000 over 17 windows
# before it = 941 a window; 3x = 2,824 <= 4,000. An ATM-ish call at $1.00/1.04 (delta ~0.4 with 5 h left).
BASE = dict(cutoff=CUTOFF, session_open=OPEN, session_close=CLOSE, spot={"SPY": SPOT, "QQQ": Decimal("600")},
            spot_before={"SPY": Decimal("679"), "QQQ": Decimal("600")})


def _evaluate(contracts, tallies, quotes, *, day_volume=20_000, prior=(), policy=F0DTE_POLICY, **extra):
    return evaluate_f0dte_window(
        **{**BASE, **extra}, contracts=contracts, tallies=tallies, quotes=quotes,
        day_volumes={c.contract_ticker: day_volume for c in contracts}, prior_alerts=prior, policy=policy)


@pytest.fixture(autouse=True)
def _fresh_process(monkeypatch):
    monkeypatch.delenv(F0DTE_SETTING, raising=False)
    for key in f0dte.F0DTE_INTEGER_SETTINGS:
        monkeypatch.delenv(key, raising=False)
    f0dte_policy.cache_clear()
    f0dte._OUTPUTS.clear()
    yield
    f0dte_policy.cache_clear()
    f0dte._OUTPUTS.clear()


# --- switch and policy ----------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert f0dte_mode({}) is F0DTEMode.OFF
    assert f0dte_mode({F0DTE_SETTING: " "}) is F0DTEMode.OFF
    assert f0dte_mode({F0DTE_SETTING: "record"}) is F0DTEMode.RECORD
    assert f0dte_mode({F0DTE_SETTING: " On "}) is F0DTEMode.ON
    for bad in ("1", "TRUE", "SHADOW", "OFFF"):
        with pytest.raises(ValueError, match=F0DTE_SETTING):
            f0dte_mode({F0DTE_SETTING: bad})


def test_policy_defaults_and_integer_settings():
    assert f0dte_policy_from({}) is F0DTE_POLICY
    tuned = f0dte_policy_from({"REALTIME_V3_F0DTE_MIN_WINDOW_PREMIUM_INDEX": "150000",
                               "REALTIME_V3_F0DTE_VOLUME_MULTIPLE_PCT": " 200 "})
    assert (tuned.minimum_window_premium_index, tuned.volume_multiple_pct) == (150_000, 200)
    assert tuned.sha256 != F0DTE_POLICY.sha256
    for bad in ("-1", "1.5", "abc", "1e3"):
        with pytest.raises(ValueError, match="REALTIME_V3_F0DTE_MAX_ALERTS_PER_WINDOW"):
            f0dte_policy_from({"REALTIME_V3_F0DTE_MAX_ALERTS_PER_WINDOW": bad})
    assert F0DTE_POLICY.execution_permission is False


def test_policy_is_read_once_per_process(monkeypatch):
    monkeypatch.setenv("REALTIME_V3_F0DTE_MAX_ALERTS_PER_WINDOW", "3")
    assert f0dte_policy().maximum_alerts_per_window == 3
    monkeypatch.setenv("REALTIME_V3_F0DTE_MAX_ALERTS_PER_WINDOW", "5")
    assert f0dte_policy().maximum_alerts_per_window == 3


@pytest.mark.parametrize("key,value", [(F0DTE_SETTING, "MAYBE"), ("REALTIME_V3_F0DTE_MIN_WINDOW_VOLUME_STOCK", "-1")])
def test_window_service_refuses_to_start_with_an_invalid_value(monkeypatch, key, value):
    from scripts import run_advanced_options_v3_production as cli

    def no_database():
        raise AssertionError("PROVIDER_REACHED")

    monkeypatch.setattr(cli, "_provider", no_database)
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match=key):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])
    monkeypatch.setenv(key, "")
    f0dte_policy.cache_clear()
    with pytest.raises(AssertionError, match="PROVIDER_REACHED"):
        cli._docker_service_main(["--docker-service-kind", "WINDOW", "--continuous"])


# --- prints: the fold -------------------------------------------------------------------------------------------------

class _Semantics:
    """Conditions: 1 plain, 2 multi-leg, 3 not volume-eligible, 4 cross."""

    def classify(self, conditions):
        code = conditions[0] if conditions else 0
        return SimpleNamespace(known=code in (1, 2, 3, 4), volume_eligible=code != 3, is_multi_leg=code == 2,
                               is_cross=code == 4, is_floor=False)


def _q(ms, bid, ask, offset=0):
    return JournalEvent(offset=offset, kind="Q", ms=ms, bid=Decimal(bid), ask=Decimal(ask))


def _t(ms, price, size, code=1, offset=0):
    return JournalEvent(offset=offset, kind="T", ms=ms, price=Decimal(price), size=size, conditions=(code,))


def test_fold_places_each_print_against_the_quote_before_its_millisecond():
    events = [
        _q(900, "1.00", "1.10"),
        _t(1_000, "1.10", 10),            # at the ask: bought, $1,100
        _q(1_000, "1.20", "1.30"),         # the same millisecond: after the print
        _t(1_500, "1.20", 5),              # bid of the new quote: sold, $600
        _t(1_600, "1.25", 4),              # inside
        _t(1_700, "1.30", 2, code=2),      # multi-leg: complex, not counted
        _t(1_800, "1.30", 3, code=3),      # not volume-eligible: excluded entirely
        _t(1_900, "1.30", 1, code=4),      # a cross at the ask: low confidence
        _t(9_000, "1.30", 1),              # quote 8 s old: stale
        _q(19_500, "1.20", "1.30"),
        _t(20_000, "1.30", 7),             # next window, at the ask
    ]
    first, second = fold_contract_events(events, windows=((0, 10_000), (10_000, 20_000)), multiplier=100,
                                         semantics=_Semantics())
    assert (first.trades, first.volume, first.excluded_trades) == (6, 23, 1)
    assert (first.ask_premium, first.bid_premium, first.inside_premium) == (Decimal("110.00") * 10,
                                                                             Decimal("120.00") * 5, Decimal("500.00"))
    assert first.complex_premium == Decimal("260.00") and first.low_confidence_premium == Decimal("130.00")
    assert first.stale_premium == Decimal("130.00") and (first.ask_volume, first.bid_volume) == (10, 5)
    assert first.premium == sum((first.ask_premium, first.bid_premium, first.inside_premium, first.complex_premium,
                                 first.low_confidence_premium, first.stale_premium))
    assert (second.trades, second.volume) == (1, 7) and second.ask_premium == Decimal("910.00")


def test_fold_without_a_quote_or_with_a_locked_one_counts_volume_but_no_side():
    events = [_t(100, "1.00", 2), _q(200, "1.00", "1.00"), _t(300, "1.00", 3)]
    (tally,) = fold_contract_events(events, windows=((0, 1_000),), multiplier=100)
    assert (tally.volume, tally.ask_premium, tally.bid_premium, tally.unquoted_premium) == (
        5, 0, 0, Decimal("500.00"))


def test_implied_delta_is_sane():
    years = 2 / 24 / 365
    atm, _vol = implied_delta(Decimal("1.00"), SPOT, Decimal("680"), years, call=True)
    assert 0.45 < atm < 0.55
    otm_put, _ = implied_delta(Decimal("0.30"), SPOT, Decimal("677"), years, call=False)
    assert -0.25 < otm_put < -0.1
    assert implied_delta(Decimal("700"), SPOT, Decimal("681"), years, call=True) is None  # above the spot
    assert implied_delta(Decimal("0.50"), SPOT, Decimal("670"), years, call=True) is None  # under intrinsic


# --- the rule -------------------------------------------------------------------------------------------------------

def test_a_bought_call_alerts_bullish_and_a_bought_put_bearish():
    result = _evaluate([CALL, PUT], {CALL.contract_ticker: _tally(90_000, 30_000),
                                     PUT.contract_ticker: _tally(0, 0)},
                       _quotes((CALL, "1.00", "1.04"), (PUT, "0.90", "0.94")))
    (alert,) = result.alerts
    assert alert.contract == CALL and alert.direction == 1 and alert.aggressor_class == "BOUGHT"
    assert 0.2 <= abs(alert.delta) <= 0.6 and alert.stock_move == "SAME"
    put_alone = _evaluate([PUT], {PUT.contract_ticker: _tally(90_000, 10_000)}, _quotes((PUT, "0.90", "0.94")))
    (alert,) = put_alone.alerts
    assert alert.direction == -1 and alert.stock_move == "OPPOSITE"


def test_sold_and_unclear_are_recorded_never_alerted():
    sold = _evaluate([CALL], {CALL.contract_ticker: _tally(30_000, 90_000)}, _quotes((CALL, "1.00", "1.04")))
    unclear = _evaluate([CALL], {CALL.contract_ticker: _tally(60_000, 50_000)}, _quotes((CALL, "1.00", "1.04")))
    small = _evaluate([CALL], {CALL.contract_ticker: _tally(40_000, 0, inside=70_000)},
                      _quotes((CALL, "1.00", "1.04")))  # ask-only but under the $50k bought minimum
    assert not sold.alerts and sold.candidates[0].reasons == (F0DTE_SOLD,)
    assert not unclear.alerts and unclear.candidates[0].reasons == (F0DTE_AGGRESSOR_UNCLEAR,)
    assert small.candidates[0].reasons == (F0DTE_AGGRESSOR_UNCLEAR,)


@pytest.mark.parametrize("minute,in_time", [(14 * 60 + 40, False), (14 * 60 + 45, True), (19 * 60 + 30, True),
                                             (19 * 60 + 35, False)])
def test_entry_time_skips_the_first_15_and_the_last_90_minutes(minute, in_time):
    cutoff = datetime(2026, 11, 18, minute // 60, minute % 60, tzinfo=UTC)
    result = _evaluate([CALL], {CALL.contract_ticker: _tally(90_000, 30_000)}, _quotes((CALL, "1.00", "1.04")),
                       cutoff=cutoff, day_volume=4_500)
    assert result.in_entry_time is in_time
    assert bool(result.alerts) is in_time
    if not in_time:
        assert F0DTE_OUTSIDE_ENTRY_TIME in result.candidates[0].reasons


def test_volume_needs_3x_its_own_average_and_the_floor():
    # 60,000 by the cutoff, 4,000 in the window: 56,000 / 17 = 3,294 a window, 3x = 9,883 > 4,000.
    busy = _evaluate([CALL], {CALL.contract_ticker: _tally(90_000, 30_000)}, _quotes((CALL, "1.00", "1.04")),
                     day_volume=60_000)
    assert F0DTE_VOLUME_BELOW_THRESHOLD in busy.candidates[0].reasons and busy.candidates[0].volume_threshold == 9_883
    floor = _evaluate([CALL], {CALL.contract_ticker: _tally(90_000, 30_000, volume=400)},
                      _quotes((CALL, "1.00", "1.04")), day_volume=400)
    assert F0DTE_VOLUME_BELOW_THRESHOLD in floor.candidates[0].reasons and floor.candidates[0].volume_threshold == 500


def test_premium_floor_by_tier():
    stock = _contract(9201, "350", underlyer="AAPL")
    quotes = _quotes((CALL, "1.00", "1.04"), (stock, "1.00", "1.04"))
    tallies = {CALL.contract_ticker: _tally(60_000, 20_000), stock.contract_ticker: _tally(60_000, 20_000)}
    result = _evaluate([CALL, stock], tallies, quotes, spot={"SPY": SPOT, "AAPL": Decimal("350")},
                       spot_before={})
    by = {row.contract.underlyer: row for row in result.candidates}
    assert F0DTE_PREMIUM_BELOW_THRESHOLD in by["SPY"].reasons  # $80k < $100k index ETF
    assert by["AAPL"].eligible and by["AAPL"].stock_move == "UNKNOWN"  # $80k >= $50k single stock


@pytest.mark.parametrize("bid,ask,reason", [("0.15", "0.17", F0DTE_MID_OUTSIDE_RANGE),
                                            ("10.10", "10.40", F0DTE_MID_OUTSIDE_RANGE),
                                            ("1.00", "1.20", F0DTE_SPREAD_TOO_WIDE)])
def test_mid_and_spread_bounds(bid, ask, reason):
    result = _evaluate([CALL], {CALL.contract_ticker: _tally(90_000, 30_000)}, _quotes((CALL, bid, ask)))
    assert reason in result.candidates[0].reasons and not result.alerts


def test_delta_bounds():
    deep = _contract(9004, "670")  # ITM by $10 with 5 h left: delta ~0.9
    far = _contract(9005, "690")   # OTM by $10: delta ~0.1
    result = _evaluate([deep, far], {deep.contract_ticker: _tally(90_000, 30_000),
                                     far.contract_ticker: _tally(90_000, 30_000)},
                       _quotes((deep, "10.05", "10.15"), (far, "0.30", "0.32")))
    by = {row.contract: row for row in result.candidates}
    assert F0DTE_DELTA_OUTSIDE_RANGE in by[far].reasons and by[far].delta < 0.2
    assert F0DTE_MID_OUTSIDE_RANGE in by[deep].reasons


def test_unpackageable_no_flow_evidence_and_unavailable_prints():
    tallies = {CALL.contract_ticker: _tally(90_000, 30_000)}
    quotes = _quotes((CALL, "1.00", "1.04"))
    assert _evaluate([CALL], tallies, quotes, unpackageable=frozenset({CALL.contract_ticker})).candidates[0].reasons == (
        F0DTE_UNPACKAGEABLE,)
    assert _evaluate([CALL], tallies, quotes, flow_evidence_available=False).candidates[0].reasons == (
        F0DTE_NO_FLOW_EVIDENCE,)
    assert F0DTE_PRINTS_UNAVAILABLE in _evaluate([CALL], tallies, quotes, prints_reason="X").candidates[0].reasons


def test_caps_repeat_underlying_hour_window_and_conflicting_direction():
    tallies = {CALL.contract_ticker: _tally(90_000, 30_000), CALL_2.contract_ticker: _tally(80_000, 20_000)}
    quotes = _quotes((CALL, "1.00", "1.04"), (CALL_2, "0.70", "0.73"))
    repeat = _evaluate([CALL, CALL_2], tallies, quotes,
                       prior=[F0DTEPriorAlert(CALL.contract_ticker, "SPY", CUTOFF - timedelta(minutes=25))])
    assert [row.contract for row in repeat.alerts] == [CALL_2]
    assert [(row.contract, reason) for row, reason in repeat.capped] == [(CALL, F0DTE_CAP_REPEAT_CONTRACT)]
    after_block = _evaluate([CALL], tallies, quotes,
                            prior=[F0DTEPriorAlert(CALL.contract_ticker, "SPY", CUTOFF - timedelta(minutes=35))])
    assert [row.contract for row in after_block.alerts] == [CALL]
    hour = _evaluate([CALL, CALL_2], tallies, quotes,
                     prior=[F0DTEPriorAlert("O:SPYX", "SPY", CUTOFF - timedelta(minutes=50))])
    assert [row.contract for row in hour.alerts] == [CALL]  # the larger bought premium first, then 2 per hour
    assert [(row.contract, reason) for row, reason in hour.capped] == [(CALL_2, F0DTE_CAP_UNDERLYING_HOUR)]
    window = _evaluate([CALL, CALL_2], tallies, quotes, policy=F0DTEPolicy(maximum_alerts_per_window=1))
    assert [(row.contract, reason) for row, reason in window.capped] == [(CALL_2, F0DTE_CAP_WINDOW)]
    both = _evaluate([CALL, PUT], {CALL.contract_ticker: _tally(90_000, 30_000),
                                   PUT.contract_ticker: _tally(120_000, 10_000)},
                     _quotes((CALL, "1.00", "1.04"), (PUT, "0.90", "0.94")))
    assert [row.contract for row in both.alerts] == [PUT]
    assert [(row.contract, reason) for row, reason in both.capped] == [(CALL, F0DTE_CONFLICTING_DIRECTION)]


def test_six_per_window_by_default():
    contracts = [_contract(9300 + i, str(679 + i % 3), underlyer=name) for i, name in
                 enumerate(("SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN"))]
    spot = {row.underlyer: Decimal(str(679 + i % 3)) for i, row in enumerate(contracts)}
    result = _evaluate(contracts, {row.contract_ticker: _tally(90_000 + i, 30_000) for i, row in enumerate(contracts)},
                       {row.contract_ticker: F0DTEQuote(Decimal("1.00"), Decimal("1.04")) for row in contracts},
                       spot=spot, spot_before={})
    assert len(result.alerts) == 6 and [reason for _, reason in result.capped] == [F0DTE_CAP_WINDOW] * 2


# --- the window hook ------------------------------------------------------------------------------------------------

def _binding(contracts=(CALL, PUT, CALL_2), universe=("SPY", "QQQ")):
    rows = [SimpleNamespace(instrument_id=c.instrument_id, contract_id=c.contract_id,
                            catalog_version_id=c.catalog_version_id, contract_ticker=c.contract_ticker,
                            underlyer=c.underlyer, call_put="C" if c.contract_type == "CALL" else "P",
                            expiration_date=c.expiration_date, strike=c.strike, multiplier=Decimal("100"))
            for c in contracts]
    # A contract that does not expire today is not in the lane.
    rows.append(SimpleNamespace(instrument_id=9999, contract_id=1999, catalog_version_id=10999,
                                contract_ticker="O:SPY261120C00690000", underlyer="SPY", call_put="C",
                                expiration_date=date(2026, 11, 20), strike=Decimal("690"), multiplier=Decimal("100")))
    control = SimpleNamespace(session_date=SESSION, cohort_id=COHORT,
                              universe=SimpleNamespace(option_underlyings=universe))
    return SimpleNamespace(enrollment=SimpleNamespace(control_plane=control,
                                                      activity_selection=SimpleNamespace(contracts=tuple(rows))))


def _rolling(volumes=None, incomplete=()):
    volumes = volumes or {CALL.contract_ticker: 20_000, PUT.contract_ticker: 20_000, CALL_2.contract_ticker: 20_000}
    return SimpleNamespace(
        state_id=STATE, committed_at=CUTOFF - timedelta(seconds=2), committed_offset=500,
        contract_volumes=tuple(SimpleNamespace(
            checkpoint=SimpleNamespace(contract_ticker=ticker, cumulative_volume=value),
            completeness=BaselineKind.INCOMPLETE if ticker in incomplete else BaselineKind.SNAPSHOT_RECONCILED)
            for ticker, value in volumes.items()),
        underlying_ticks=(SimpleNamespace(ticker="SPY", evidence=SimpleNamespace(price=SPOT)),))


def _marks(cutoff=CUTOFF, **asks):
    values = {CALL.instrument_id: ("1.00", "1.04"), PUT.instrument_id: ("0.90", "0.94"),
              CALL_2.instrument_id: ("0.70", "0.73")}
    values.update(asks)
    return tuple(SimpleNamespace(instrument_id=key, bid=Decimal(bid), ask=Decimal(ask),
                                 market_time=cutoff - timedelta(seconds=1)) for key, (bid, ask) in values.items())


FLOW_ENVIRONMENT = SimpleNamespace(
    options_flow_source=SimpleNamespace(path="/journal/options.sqlite",
                                        registration=SimpleNamespace(journal_generation=None)),
    options_flow_semantics=_Semantics())


class _Calls:
    def __init__(self, tallies=None, error=None, flow_evidence=True, prior=(), spot_before=None):
        self.tallies = tallies if tallies is not None else {CALL.contract_ticker: _tally(90_000, 30_000),
                                                            PUT.contract_ticker: _tally(10_000, 30_000)}
        self.error, self.flow_evidence, self.prior = error, flow_evidence, list(prior)
        self.spot_before = {"SPY": Decimal("679")} if spot_before is None else spot_before
        self.reads, self.db = [], []

    def read_tallies(self, path, **kwargs):
        self.reads.append((path, kwargs))
        if self.error is not None:
            raise self.error
        return self.tallies, {"events_read": 10, "read_ms": 1.0}

    def db_reads(self, provider, **kwargs):
        self.db.append(kwargs)
        return dict(self.spot_before), list(self.prior), self.flow_evidence


def _window(mode, calls=None, *, cutoff=CUTOFF, state=None, marks=None, rolling=None, **extra):
    calls = calls or _Calls()
    environ = {} if mode is None else {F0DTE_SETTING: mode}
    extra.setdefault("now", cutoff + timedelta(seconds=75))  # the window runs about a minute after its cutoff
    output = f0dte.run_f0dte_window(
        provider=object(), binding=_binding(), rolling=rolling or _rolling(), marks=marks or _marks(cutoff),
        flow_environment=FLOW_ENVIRONMENT, barrier_offset=1234, cutoff=cutoff,
        state=state or F0DTEState(), environ=environ, read_tallies=calls.read_tallies, db_reads=calls.db_reads,
        **extra)
    return output, calls


def test_a_window_run_long_after_its_cutoff_records_but_does_not_alert(capsys):
    output, _ = _window("ON", now=CUTOFF + timedelta(minutes=7))
    assert output.evaluations == ()
    (window,) = [doc for event, doc in _lines(capsys.readouterr().err) if event == "F0DTE_RECORD"]
    assert window["delay_seconds"] == 420.0 and window["reasons"]["F0DTE_WINDOW_LATE"] == 3
    on_time, _ = _window("ON", now=CUTOFF + timedelta(minutes=5))
    assert len(on_time.evaluations) == 1


def _lines(err):
    rows = []
    for line in err.strip().splitlines():
        event, _, body = line.partition(" ")
        rows.append((event, json.loads(body)))
    return rows


def test_off_reads_nothing_and_logs_nothing(capsys):
    for mode in (None, "OFF", "off"):
        output, calls = _window(mode)
        assert output is None and calls.reads == [] and calls.db == []
    assert capsys.readouterr().err == ""


def test_record_logs_the_window_and_the_would_alert_and_adds_no_evaluation(capsys):
    output, calls = _window("RECORD")
    assert output.evaluations == ()
    (path, read), = calls.reads
    assert path == "/journal/options.sqlite" and read["barrier_offset"] == 1234
    assert read["window_start"] == CUTOFF - timedelta(minutes=5) and read["window_end"] == CUTOFF
    assert {c.contract_ticker for c in read["contracts"]} == {CALL.contract_ticker, PUT.contract_ticker,
                                                              CALL_2.contract_ticker}  # 0DTE only
    lines = _lines(capsys.readouterr().err)
    assert [event for event, _ in lines] == ["F0DTE_RECORD", "F0DTE_ALERT_RECORD"]
    window, alert = lines[0][1], lines[1][1]
    assert (window["mode"], window["status"], window["applied"], window["alerts"]) == ("RECORD", "OK", False, 1)
    assert window["would_alert"] == [CALL.contract_ticker] and window["policy_sha256"] == F0DTE_POLICY.sha256
    rows = {"O:" + row["ticker"]: row for row in window["rows"]}
    assert rows[CALL.contract_ticker]["verdict"] == "ALERT"
    # $10k bought vs $30k sold: under the $50k minimum, so unclear, and $40k is under the $100k premium floor.
    assert rows[PUT.contract_ticker]["reasons"] == [F0DTE_PREMIUM_BELOW_THRESHOLD, F0DTE_AGGRESSOR_UNCLEAR]
    assert rows[CALL_2.contract_ticker]["reasons"] == ["F0DTE_PRINTS_UNAVAILABLE"]  # no tally read for it
    assert (alert["detector_id"], alert["direction"], alert["entry_limit"], alert["applied"]) == (
        "F0DTE", 1, "1.0400", False)
    assert alert["exit_deadline"] == (CUTOFF + timedelta(minutes=45)).isoformat()


def test_on_leaves_a_selected_single_leg_long_package_for_the_o1_lane(capsys):
    output, _calls = _window("ON")
    (evaluation,) = output.evaluations
    package = evaluation.package
    assert evaluation.selection_status == "SELECTED" and evaluation.underlyer == "SPY"
    assert evaluation.observed_at == CUTOFF and evaluation.event_id == package.event_id
    assert evaluation.event_id == f0dte.f0dte_event_id(COHORT, CALL.contract_ticker, CUTOFF)
    (leg,) = package.legs
    assert (leg.side.value, leg.ratio, leg.contract_type.value, leg.entry_mark) == ("BUY", 1, "CALL", Decimal("1.04"))
    assert (leg.instrument_id, leg.contract_id, leg.catalog_version_id) == (9001, 1001, 10001)
    assert package.entry_net_premium == Decimal("-104.00") and package.capital_at_risk == Decimal("104.00")
    assert package.exit_deadline == CUTOFF + timedelta(minutes=45) and package.cohort_id == COHORT
    assert package.matrix_id == STATE and package.provider_tier.value == "MASSIVE_ADVANCED"
    payload = evaluation.payload
    assert (payload["contract"], payload["detector_id"], payload["category"], payload["direction"]) == (
        "f0dte-evaluation-v1", "F0DTE", "ZERO_DTE_INTRADAY", 1)
    assert payload["management_policy"]["take_profit_fraction"] == "0.5"
    assert payload["management_policy"]["stop_loss_fraction"] == "0.35"
    assert payload["management_policy"]["maximum_hold_seconds"] == 2700
    assert (payload["target_price"], payload["stop_price"]) == ("1.5600", "0.6760")
    lines = _lines(capsys.readouterr().err)
    assert lines[0][1]["applied"] is True and lines[1][1]["applied"] is True
    # The O1 executor hook picks them up for this cutoff only, and only under ON.
    from realtime.advanced_options_native_adapters import _f0dte_evaluations

    import os
    os.environ[F0DTE_SETTING] = "ON"
    try:
        assert _f0dte_evaluations(SimpleNamespace(cutoff=CUTOFF)) == output.evaluations
        assert _f0dte_evaluations(SimpleNamespace(cutoff=CUTOFF + timedelta(minutes=5))) == ()
        os.environ[F0DTE_SETTING] = "RECORD"
        assert _f0dte_evaluations(SimpleNamespace(cutoff=CUTOFF)) == ()
    finally:
        os.environ.pop(F0DTE_SETTING, None)
    assert _f0dte_evaluations(SimpleNamespace(cutoff=CUTOFF)) == ()


def test_time_exit_stops_15_minutes_before_the_close():
    late = datetime(2026, 11, 18, 19, 30, tzinfo=UTC)
    output, _ = _window("ON", cutoff=late)
    (evaluation,) = output.evaluations
    assert evaluation.package.exit_deadline == datetime(2026, 11, 18, 20, 15, tzinfo=UTC)
    assert f0dte.exit_deadline(datetime(2026, 11, 18, 20, 20, tzinfo=UTC), CLOSE) == datetime(
        2026, 11, 18, 20, 45, tzinfo=UTC)


def test_on_caps_across_windows_from_the_prior_alerts_and_the_process_memory(capsys):
    state = F0DTEState()
    first, _ = _window("ON", state=state)
    assert len(first.evaluations) == 1
    # Ten minutes later the same contract is blocked (process memory), even if the database has not caught up.
    second, _ = _window("ON", state=state, cutoff=CUTOFF + timedelta(minutes=10))
    assert second.evaluations == ()
    record = _lines(capsys.readouterr().err)
    windows = [doc for event, doc in record if event == "F0DTE_RECORD"]
    assert windows[1]["capped"] == 1 and windows[1]["rows"][0]["reasons"] == [F0DTE_CAP_REPEAT_CONTRACT]
    # From the database (another process made it): the hour cap.
    prior = [F0DTEPriorAlert("O:SPY261118C00690000", "SPY", CUTOFF - timedelta(minutes=40)),
             F0DTEPriorAlert("O:SPY261118C00691000", "SPY", CUTOFF - timedelta(minutes=20))]
    third, _ = _window("ON", calls=_Calls(prior=prior))
    assert third.evaluations == ()


def test_outside_the_entry_time_the_journal_is_not_read(capsys):
    output, calls = _window("RECORD", cutoff=datetime(2026, 11, 18, 20, 0, tzinfo=UTC))
    assert calls.reads == [] and output.evaluations == ()
    (window,) = [doc for event, doc in _lines(capsys.readouterr().err) if event == "F0DTE_RECORD"]
    assert (window["prints_reason"], window["in_entry_time"], window["alerts"]) == ("OUTSIDE_ENTRY_TIME", False, 0)


def test_unmarked_or_baseline_incomplete_contracts_are_not_packaged(capsys):
    output, _ = _window("ON", unpackageable_instrument_ids=(CALL.instrument_id,))
    assert output.evaluations == ()
    output, _ = _window("ON", rolling=_rolling(incomplete=(CALL.contract_ticker,)))
    assert output.evaluations == ()
    output, _ = _window("ON", calls=_Calls(flow_evidence=False))
    assert output.evaluations == ()


def test_journal_unavailable_is_recorded_and_alerts_nothing(capsys):
    output, _ = _window("ON", calls=_Calls(error=F0DTEPrintsUnavailable("JOURNAL_FILE_MISSING")))
    assert output.evaluations == ()
    (window,) = [doc for event, doc in _lines(capsys.readouterr().err) if event == "F0DTE_RECORD"]
    assert window["status"] == "OK" and window["prints_reason"] == "JOURNAL_FILE_MISSING"
    assert window["reasons"]["F0DTE_PRINTS_UNAVAILABLE"] == 3


@pytest.mark.parametrize("mode", ["RECORD", "ON"])
def test_any_failure_is_one_record_failed_line_and_a_degraded_fault(capsys, mode):
    output, _ = _window(mode, calls=_Calls(error=RuntimeError("boom")))
    assert output is None
    lines = capsys.readouterr().err.strip().splitlines()
    assert len(lines) == 2
    event, document = lines[0].split(" ", 1)
    assert event == "F0DTE_RECORD" and json.loads(document)["status"] == "RECORD_FAILED"
    assert "boom" in json.loads(document)["reason"]
    fault = json.loads(lines[1].split("FAULT ", 1)[1])
    assert (fault["kind"], fault["severity"], fault["action"]) == ("F0DTE_FAILED", "DEGRADED", "LANE_SKIPPED")


def test_an_invalid_switch_fails_loudly_in_the_window():
    with pytest.raises(ValueError, match=F0DTE_SETTING):
        _window("SOMETIMES")


def test_outcome_lines_at_each_horizon_and_the_managed_exit(capsys):
    state = F0DTEState()
    _window("RECORD", state=state)
    capsys.readouterr()
    # +5: mid 1.20 (+15% on the 1.04 entry); +15: mid 1.60 >= the 1.56 target: exit TARGET; +30 and +45 follow.
    for minutes, (bid, ask) in ((5, ("1.18", "1.22")), (10, ("1.30", "1.34")), (15, ("1.58", "1.62")),
                                (30, ("1.00", "1.04")), (45, ("0.50", "0.54"))):
        cutoff = CUTOFF + timedelta(minutes=minutes)
        marks = (SimpleNamespace(instrument_id=CALL.instrument_id, bid=Decimal(bid), ask=Decimal(ask),
                                 market_time=cutoff - timedelta(seconds=1)),)
        _window("RECORD", state=state, cutoff=cutoff, marks=marks, calls=_Calls(tallies={}))
    outcomes = [doc for event, doc in _lines(capsys.readouterr().err) if event == "F0DTE_OUTCOME"]
    assert [(doc["kind"], doc["label"], doc["elapsed_minutes"]) for doc in outcomes] == [
        ("HORIZON", "+5m", 5), ("HORIZON", "+15m", 15), ("EXIT", "TARGET", 15), ("HORIZON", "+30m", 30),
        ("HORIZON", "+45m", 45)]
    assert outcomes[0]["return_mid_pct"] == "15.4" and outcomes[2]["mid"] == "1.6000"
    assert state.tracked == {}  # done: exit and every horizon


def test_a_new_session_starts_with_empty_memory():
    state = F0DTEState()
    _window("ON", state=state)
    assert state.alerts and state.tracked
    state.reset_for(date(2026, 11, 19))
    assert not state.alerts and not state.tracked


# --- the O1 lane, the evidence and the alert projection ------------------------------------------------------------------

def _on_evaluation():
    output, _ = _window("ON")
    (native,) = output.evaluations
    return SimpleNamespace(evaluation_id=uuid5(NAMESPACE_URL, f"evaluation:{native.event_id}"),
                           dataset_id=uuid5(NAMESPACE_URL, "dataset"), event_id=native.event_id,
                           detector=DetectorFamily.O1, underlyer=native.underlyer,
                           selection_status=native.selection_status, observed_at=native.observed_at,
                           payload=native.payload, package=native.package)


def test_the_o1_executor_adds_the_f0dte_alerts_only_under_on(monkeypatch, capsys):
    from realtime.advanced_options_native_adapters import AdvancedOptionsLaneInputs, build_advanced_o1_executor
    from test_o1_liquidity_floor import _context

    context = _context()
    output, _ = _window("ON", cutoff=context.cutoff)
    capsys.readouterr()
    executor = build_advanced_o1_executor(AdvancedOptionsLaneInputs())
    off = executor(context)
    monkeypatch.setenv(F0DTE_SETTING, "RECORD")
    assert executor(context) == off and off.evaluations == ()
    monkeypatch.setenv(F0DTE_SETTING, "ON")
    on = executor(context)
    assert on.state.value == "COMPLETE" and on.evaluations == output.evaluations
    assert capsys.readouterr().err == ""  # the executor logs nothing for F0DTE


def test_alert_rows_show_detector_f0dte_with_its_management_and_entry_limit():
    from realtime.advanced_options_projection import (
        _alert_evaluation_row,
        _alert_run_row,
        _evaluation_detector_id,
        _one_alert_per_contract,
        _session_alert_key,
    )

    evaluation = _on_evaluation()
    assert _evaluation_detector_id(evaluation) == "F0DTE"
    row = _alert_evaluation_row(evaluation, CUTOFF)
    assert (row["detector_id"], row["category"], row["strategy_name"], row["origin"], row["direction"]) == (
        "F0DTE", "ZERO_DTE_INTRADAY", "LONG_CALL", "OPTIONS_FIRST", 1)
    run = _alert_run_row(evaluation, CUTOFF, {})
    assert run["detector_id"] == "F0DTE" and run["entry_limit"] == "104.00"
    assert run["management_policy"]["policy_version"] == "f0dte_intraday_management_v1"
    assert run["original_package"]["structure_type"] == "LONG_CALL"
    assert run["original_package"]["calendar_dte"] == 0
    assert run["exit_deadline"] == (CUTOFF + timedelta(minutes=45)).isoformat()
    assert _session_alert_key(evaluation) == (CALL.contract_id, "F0DTE:1600")
    (grouped,) = _one_alert_per_contract([(evaluation, run)])
    assert grouped["detector_id"] == "F0DTE"
    # An O1 evaluation is untouched.
    o1 = SimpleNamespace(**{**vars(evaluation), "payload": {"direction": 1}})
    assert _evaluation_detector_id(o1) == "O1" and _alert_evaluation_row(o1, CUTOFF)["category"] == "O1"


def test_the_session_alert_is_one_row_per_f0dte_trigger():
    from realtime.advanced_options_projection import (
        _alert_run_row,
        _one_alert_per_contract,
        options_session_alert_id,
        persist_options_session_alerts_cursor,
    )
    from test_options_session_alerts import AlertTable

    evaluation = _on_evaluation()
    rows = _one_alert_per_contract([(evaluation, _alert_run_row(evaluation, CUTOFF, {}))])
    table = AlertTable()
    written = persist_options_session_alerts_cursor(table, cohort_id=COHORT, session_date=str(SESSION),
                                                    alert_rows=rows, evaluations=[evaluation])
    assert written == 1
    (alert_id, stored), = table.rows.items()
    assert alert_id == str(options_session_alert_id(str(SESSION), "F0DTE", CALL.contract_id, "F0DTE:1600"))
    projection = json.loads(stored["projection"])
    assert (projection["detector_id"], projection["event_id"] if "event_id" in projection else None) == ("F0DTE", None)
    assert stored["event_id"] == str(evaluation.event_id)
    assert stored["valid_until"] == CUTOFF + timedelta(minutes=45)


def test_package_evidence_accepts_an_f0dte_package_without_a_flow_event():
    import realtime.advanced_options_package_evidence as evidence

    evaluation = _on_evaluation()
    assert evidence._is_f0dte_evaluation(evaluation)
    assert not evidence._is_f0dte_evaluation(SimpleNamespace(payload={"contract": "x"}))

    # Past the flow-event binding check the builder goes on to the evidence contract (these empty inputs fail it).
    with pytest.raises(Exception) as caught:
        evidence.build_flow_package_evidence(
            evaluation, flow_observations=(), cluster_observations=(), threshold_evidence=None,
            reference_state=SimpleNamespace(state_id=STATE, state_sha256="a" * 64),
            rolling_state=SimpleNamespace(state_id=STATE, state_sha256="b" * 64), package_mark=None)
    assert "does not bind a flow event" not in str(caught.value)
    o1 = SimpleNamespace(**{**vars(evaluation), "payload": {}})
    with pytest.raises(ValueError, match="does not bind a flow event"):
        evidence.build_flow_package_evidence(
            o1, flow_observations=(), cluster_observations=(), threshold_evidence=None, reference_state=None,
            rolling_state=None, package_mark=None)


def test_candidate_detail_names_f0dte_its_structure_and_management():
    from realtime.advanced_options_projection import _candidate_detail

    detail = _candidate_detail(_on_evaluation(), "c" * 64)["candidate"]
    assert (detail["strategy_name"], detail["display_name"], detail["structure_type"]) == (
        "F0DTE", "F0DTE SPY", "LONG_CALL")
    assert detail["management_policy_version"] == "f0dte_intraday_management_v1"
    assert detail["management_policy"]["take_profit_fraction"] == "0.5"
    o1 = SimpleNamespace(**{**vars(_on_evaluation()), "payload": {}})
    o1_detail = _candidate_detail(o1, "c" * 64)["candidate"]
    assert (o1_detail["strategy_name"], o1_detail["structure_type"], o1_detail["management_policy"]) == (
        "O1", "ADVANCED_CANONICAL_PACKAGE", {})


def test_the_evidence_contract_takes_an_f0dte_package_with_its_mark_and_legs():
    """The real evidence contract (fixtures of test_advanced_options_package_evidence): a single-leg long package under
    its own event id (not a stored flow event), marked at the cutoff, with one stored print carried (the window gives
    the underlyer's newest), builds SELECTED evidence with its leg evidence."""
    from dataclasses import replace as _replace

    from options.domain import ContractType
    from options.strategies.domain import OptionSide
    from realtime.advanced_options_cycle import AdvancedOptionsDetectorEvaluation
    from realtime.advanced_options_package_evidence import build_flow_package_evidence
    from realtime.materialization import MaterializedQuote
    from realtime.option_portal_adapter import (
        RealtimeOptionPackage,
        RealtimeOptionPackageLeg,
        materialize_realtime_option_package_mark,
    )
    from realtime.preflight import ProviderTier
    import test_advanced_options_package_evidence as fixtures

    reference, rolling, _event, flow, _cluster = fixtures._inputs()
    policy = fixtures._policy()
    now = fixtures.NOW
    event_id = uuid5(NAMESPACE_URL, "f0dte-evidence-event")
    leg = RealtimeOptionPackageLeg(0, 1, 101, 201, "O:SPY261120C00500000", OptionSide.BUY, 1, 100, Decimal("500"),
                                   date(2026, 11, 20), ContractType.CALL, Decimal("5.30"), now, policy.policy_sha256)
    package = RealtimeOptionPackage(
        candidate_id=f0dte.f0dte_candidate_id(fixtures.COHORT, event_id), event_id=event_id,
        matrix_id=fixtures.MATRIX, candidate_identity_sha256="d" * 64, cohort_id=fixtures.COHORT,
        provider_tier=ProviderTier.MASSIVE_ADVANCED, entry_market_time=now, entry_net_premium=Decimal("-530"),
        capital_at_risk=Decimal("530"), exit_deadline=now + timedelta(minutes=45), legs=(leg,))
    quote = MaterializedQuote(
        journal_generation=fixtures.GENERATION, stream_session_id=fixtures.STREAM, instrument_id=1,
        provider_symbol=leg.contract_ticker, source_session=fixtures.SESSION, source_sequence=1, source_timestamp=1,
        source_timestamp_unit="ms", event_at=now + timedelta(milliseconds=10),
        received_at=now + timedelta(milliseconds=11), bid=Decimal("5.10"), ask=Decimal("5.30"), bid_size=10,
        ask_size=11, bid_exchange=1, ask_exchange=2, quality="GOOD", reason_codes=(), source_offset=1,
        payload_sha256=f"{1:064x}")
    mark = materialize_realtime_option_package_mark(package, {1: quote}, checked_at=now + timedelta(milliseconds=20),
                                                    policy=policy).mark
    assert mark is not None and flow.event_id != event_id
    evaluation = AdvancedOptionsDetectorEvaluation(
        uuid5(NAMESPACE_URL, "f0dte-evidence-evaluation"), fixtures.DATASET, event_id, DetectorFamily.O1, "SPY",
        "SELECTED", now, "f" * 64, {"contract": "f0dte-evaluation-v1", "detector_id": "F0DTE"}, package)
    evidence = build_flow_package_evidence(
        evaluation, flow_observations=(flow,), cluster_observations=(), threshold_evidence=fixtures._thresholds(),
        reference_state=reference, rolling_state=rolling, package_mark=mark, model_version="black-scholes-merton-v1",
        model_bundle_sha256="1" * 64, valuation_policy_sha256=policy.policy_sha256)
    assert evidence.selection_status == "SELECTED" and len(evidence.package_leg_evidence) == 1
    o1 = _replace(evaluation, payload={"native": "O1"})
    with pytest.raises(ValueError, match="does not bind a flow event"):
        build_flow_package_evidence(
            o1, flow_observations=(flow,), cluster_observations=(), threshold_evidence=fixtures._thresholds(),
            reference_state=reference, rolling_state=rolling, package_mark=mark)


def test_the_window_evidence_carries_one_stored_print_for_an_f0dte_alert(monkeypatch):
    """p8 window `_evidence_builder`: an F0DTE alert's evidence carries its underlyer's newest stored print (not the
    session's), and an unmarked F0DTE package is recorded with PACKAGE_MARK_UNAVAILABLE, never failing the window."""
    import realtime.advanced_options_p8_window as window
    import test_advanced_options_package_evidence as fixtures
    from realtime.advanced_options_cycle import AdvancedOptionsDetectorEvaluation

    reference, rolling, event, flow, _cluster = fixtures._inputs()
    monkeypatch.setattr(window, "_flow_evidence", lambda *_args: ({event.event_id: event}, (flow,)))
    package, _mark, _policy = fixtures._package_and_mark()
    event_id = uuid5(NAMESPACE_URL, "f0dte-window-evidence")
    package = _replaced(package, event_id=event_id)
    evaluation = AdvancedOptionsDetectorEvaluation(
        uuid5(NAMESPACE_URL, "f0dte-window-evaluation"), fixtures.DATASET, event_id, DetectorFamily.O1, "SPY",
        "SELECTED", fixtures.NOW, "f" * 64, {"contract": "f0dte-evaluation-v1", "detector_id": "F0DTE"}, package)
    cycle = SimpleNamespace(dataset_id=fixtures.DATASET, cycle_sha256="9" * 64, evaluations=(evaluation,))
    control = SimpleNamespace(cohort_id=fixtures.COHORT, pins=SimpleNamespace(model_bundle_sha256="1" * 64,
                                                                             valuation_policy_sha256="2" * 64))
    build = window._evidence_builder(object(), reference, rolling, (), None, fixtures.NOW, control)
    batch = build(cycle)
    (evidence,) = batch.evidences
    assert evidence.flow_observations == (flow,)
    assert "PACKAGE_MARK_UNAVAILABLE" in evidence.degraded_reason_codes and evidence.package == package


def _replaced(package, **changes):
    from dataclasses import replace as _replace

    return _replace(package, **changes)


def _journal(path, generation, events):
    import sqlite3

    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE journal_metadata (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
        "journal_generation TEXT NOT NULL, schema_version INTEGER NOT NULL, created_at TEXT NOT NULL);"
        "CREATE TABLE normalized_events (event_offset INTEGER PRIMARY KEY AUTOINCREMENT, instrument_id INTEGER NOT NULL,"
        " event_kind TEXT NOT NULL, source_timestamp INTEGER NOT NULL, price_text TEXT, size_text TEXT, bid_text TEXT,"
        " ask_text TEXT, conditions_json TEXT NOT NULL);"
        "CREATE INDEX idx_normalized_event_instrument_time ON normalized_events (instrument_id, source_timestamp,"
        " event_offset);")
    connection.execute("INSERT INTO journal_metadata VALUES (1, ?, 1, 'x')", (str(generation),))
    connection.executemany(
        "INSERT INTO normalized_events (instrument_id,event_kind,source_timestamp,price_text,size_text,bid_text,"
        "ask_text,conditions_json) VALUES (?,?,?,?,?,?,?,?)", events)
    connection.commit()
    connection.close()


def test_read_window_tallies_from_a_journal_file(tmp_path):
    generation = uuid5(NAMESPACE_URL, "journal")
    start = CUTOFF - timedelta(minutes=5)
    ms = int(start.timestamp() * 1000)
    path = tmp_path / "options.sqlite"
    _journal(path, generation, [
        (CALL.instrument_id, "Q", ms - 30_000, None, None, "1.00", "1.04", "[]"),  # the quote before the window
        (CALL.instrument_id, "T", ms - 1_000, "1.04", "50", None, None, "[1]"),     # before the window: not counted
        (CALL.instrument_id, "Q", ms + 100, None, None, "1.00", "1.04", "[]"),
        (CALL.instrument_id, "T", ms + 1_000, "1.04", "100", None, None, "[1]"),    # at the ask
        (CALL.instrument_id, "T", ms + 1_200, "1.00", "20", None, None, "[1]"),     # at the bid
        (PUT.instrument_id, "Q", ms + 100, None, None, "0.90", "0.94", "[]"),
        (PUT.instrument_id, "T", ms + 2_000, "0.92", "10", None, None, "[1]"),      # inside
        (CALL.instrument_id, "T", ms + 3_000, "1.04", "999", None, None, "[1]"),    # past the barrier offset
    ])
    tallies, stats = f0dte.read_window_tallies(
        path, contracts=(CALL, PUT), window_start=start, window_end=CUTOFF, barrier_offset=7,
        journal_generation=generation, semantics=_Semantics())
    call, put = tallies[CALL.contract_ticker], tallies[PUT.contract_ticker]
    assert (call.trades, call.volume, call.ask_premium, call.bid_premium) == (
        2, 120, Decimal("10400.00"), Decimal("2000.00"))
    assert (put.trades, put.inside_premium) == (1, Decimal("920.00")) and stats["events_read"] == 7
    with pytest.raises(F0DTEPrintsUnavailable, match="JOURNAL_GENERATION_MISMATCH"):
        f0dte.read_window_tallies(path, contracts=(CALL,), window_start=start, window_end=CUTOFF, barrier_offset=7,
                                  journal_generation=uuid5(NAMESPACE_URL, "other"))
    with pytest.raises(F0DTEPrintsUnavailable, match="JOURNAL_FILE_MISSING"):
        f0dte.read_window_tallies(tmp_path / "missing.sqlite", contracts=(CALL,), window_start=start,
                                  window_end=CUTOFF, barrier_offset=None, journal_generation=None)


def test_an_alert_known_to_the_database_and_the_process_counts_once(capsys):
    state = F0DTEState()
    first, _ = _window("ON", state=state)
    assert len(first.evaluations) == 1
    # 20 minutes later: the same alert from the database and from memory; another SPY contract may still alert
    # (1 of 2 this hour), the repeat on the first contract may not.
    later = CUTOFF + timedelta(minutes=20)
    stored = [F0DTEPriorAlert(CALL.contract_ticker, "SPY", CUTOFF)]
    tallies = {CALL.contract_ticker: _tally(90_000, 30_000), CALL_2.contract_ticker: _tally(80_000, 20_000)}
    second, _ = _window("ON", state=state, cutoff=later, calls=_Calls(tallies=tallies, prior=stored))
    assert [row.package.legs[0].contract_ticker for row in second.evaluations] == [CALL_2.contract_ticker]
    windows = [doc for event, doc in _lines(capsys.readouterr().err) if event == "F0DTE_RECORD"]
    assert windows[-1]["prior_alerts_considered"] == 1


def test_the_window_line_stays_under_docker_s_16_kib_with_many_contracts(capsys):
    contracts = [_contract(9400 + i, str(660 + i), "C" if i % 2 else "P") for i in range(40)]
    binding = _binding(contracts=contracts)
    tallies = {row.contract_ticker: _tally(60_000, 40_000, inside=30_000) for row in contracts}
    marks = tuple(SimpleNamespace(instrument_id=row.instrument_id, bid=Decimal("1.00"), ask=Decimal("1.30"),
                                  market_time=CUTOFF) for row in contracts)
    rolling = _rolling(volumes={row.contract_ticker: 900_000 for row in contracts})
    calls = _Calls(tallies=tallies)
    f0dte.run_f0dte_window(provider=object(), binding=binding, rolling=rolling, marks=marks,
                           flow_environment=FLOW_ENVIRONMENT, barrier_offset=1, cutoff=CUTOFF, state=F0DTEState(),
                           environ={F0DTE_SETTING: "RECORD"}, read_tallies=calls.read_tallies,
                           db_reads=calls.db_reads, now=CUTOFF + timedelta(seconds=60))
    (line,) = [line for line in capsys.readouterr().err.splitlines() if line.startswith("F0DTE_RECORD ")]
    document = json.loads(line.split(" ", 1)[1])
    assert len(line.encode()) < 16 * 1024 and document["rows_listed"] == 32 and document["contracts"] == 40
    assert sum(document["reasons"].values()) >= 40  # every rejection is counted, listed or not


def test_the_cycle_carries_an_f0dte_alert_in_the_o1_lane_and_persists_it(monkeypatch):
    """The real cycle: under ON the O1 executor's F0DTE evaluation passes the cycle's scope checks (underlying in the
    universe, package cohort and tier), makes the O1 lane COMPLETE_SELECTED and is persisted as an O1 evaluation."""
    from unittest.mock import MagicMock

    from realtime.advanced_options_cycle import (
        AdvancedOptionsNativeEvaluation,
        CycleLaneState,
        persist_advanced_options_cycle_cursor,
        run_advanced_options_cycle,
    )
    from realtime.advanced_options_native_adapters import AdvancedOptionsLaneInputs, build_advanced_o1_executor
    from test_advanced_options_control_plane import _plan as _control_plane
    from test_advanced_options_cycle import NOW, _complete, _gate_and_receipt, _reference_repository

    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    assert "SPY" in control.universe.option_underlyings
    close = NOW.replace(hour=21, minute=0, second=0, microsecond=0)
    candidate = _evaluate([CALL], {CALL.contract_ticker: _tally(90_000, 30_000)},
                          _quotes((CALL, "1.00", "1.04"))).alerts[0]
    package = f0dte.f0dte_package(candidate, cohort_id=control.cohort_id, matrix_id=STATE, cutoff=NOW,
                                  session_close=close, valuation_policy_sha256="a" * 64)
    payload = f0dte.f0dte_payload(candidate, cutoff=NOW, session_close=close)
    native = AdvancedOptionsNativeEvaluation(package.event_id, "SPY", "SELECTED", NOW, payload, package)
    f0dte._OUTPUTS[NOW.isoformat()] = f0dte.F0DTEWindowOutput(NOW, (native,))
    monkeypatch.setenv(F0DTE_SETTING, "ON")
    executors = {detector: (lambda _context: _complete()) for detector in control.manifest_policy.required_detectors
                 if detector is not DetectorFamily.O2}
    executors[DetectorFamily.O1] = build_advanced_o1_executor(AdvancedOptionsLaneInputs())
    cycle = run_advanced_options_cycle(
        control_plane=control, input_gate=gate, input_receipt=receipt, executors=executors, cutoff=NOW,
        reference_generation=7, reference_repository=_reference_repository(), build_id="p4-build")
    outcomes = {row.detector: row for row in cycle.outcomes}
    assert outcomes[DetectorFamily.O1].state is CycleLaneState.COMPLETE_SELECTED
    (evaluation,) = cycle.evaluations
    assert (evaluation.detector, evaluation.event_id, evaluation.package) == (DetectorFamily.O1, package.event_id,
                                                                               package)
    import realtime.advanced_options_cycle as cycle_module

    persisted = []
    monkeypatch.setattr(cycle_module, "persist_realtime_option_package_cursor",
                        lambda _cursor, value: persisted.append(value))
    cursor = MagicMock()
    persist_advanced_options_cycle_cursor(cursor, cycle, control_plane=control)
    calls = [(call.args[0], call.args[1] if len(call.args) > 1 else None) for call in cursor.execute.call_args_list]
    assert persisted == [package]
    (event_row,) = [params for sql, params in calls if sql.startswith("INSERT INTO rt.detector_events")]
    assert event_row[0] == str(package.event_id) and event_row[8] == "SELECTED"
    (evaluation_row,) = [params for sql, params in calls if sql.startswith("INSERT INTO rt.detector_evaluations")]
    assert evaluation_row[3] == "O1" and json.loads(evaluation_row[8])["detector_id"] == "F0DTE"
    # OFF: the same executor adds nothing.
    monkeypatch.setenv(F0DTE_SETTING, "OFF")
    off = run_advanced_options_cycle(
        control_plane=control, input_gate=gate, input_receipt=receipt, executors=executors, cutoff=NOW,
        reference_generation=7, reference_repository=_reference_repository(), build_id="p4-build")
    assert off.evaluations == () and {row.detector: row for row in off.outcomes}[DetectorFamily.O1].state is (
        CycleLaneState.COMPLETE_ZERO)


def test_a_restarted_on_process_goes_on_tracking_stored_alerts(capsys):
    first, _ = _window("ON", state=F0DTEState())
    payload = first.evaluations[0].payload
    assert payload["instrument_id"] == CALL.instrument_id
    stored = f0dte._stored_prior_alert({
        "contract_ticker": payload["contract_ticker"], "underlyer": "SPY", "observed_at": CUTOFF,
        "event_id": str(first.evaluations[0].event_id), "instrument_id": str(payload["instrument_id"]),
        "entry": payload["entry_limit"], "target": payload["target_price"], "stop": payload["stop_price"],
        "deadline": payload["exit_deadline"], "direction": str(payload["direction"])})
    assert stored.entry == Decimal("1.0400") and stored.deadline == CUTOFF + timedelta(minutes=45)
    capsys.readouterr()
    restarted = F0DTEState()  # a new process: nothing in memory
    later = CUTOFF + timedelta(minutes=20)
    marks = (SimpleNamespace(instrument_id=CALL.instrument_id, bid=Decimal("0.60"), ask=Decimal("0.64"),
                             market_time=later),)
    _window("ON", state=restarted, cutoff=later, marks=marks, calls=_Calls(tallies={}, prior=[stored]))
    outcomes = [doc for event, doc in _lines(capsys.readouterr().err) if event == "F0DTE_OUTCOME"]
    # +5 and +15 were past at the restart: not reported again; the stop (mid 0.62 <= 0.676) is.
    assert [(doc["kind"], doc["label"]) for doc in outcomes] == [("EXIT", "STOP")]
    assert str(first.evaluations[0].event_id) in restarted.tracked
    # A malformed stored payload still counts for the caps but is not tracked.
    bare = f0dte._stored_prior_alert({"contract_ticker": "O:X", "underlyer": "SPY", "observed_at": CUTOFF})
    assert bare.event_id is None and bare.contract_ticker == "O:X"
