"""G106 AL10 phase 1 (Nov 17): Options watched-set rotation policy, RECORD only.

`REALTIME_V3_OPTIONS_SLOT_ROTATION` (options-scheduler): OFF (default) = no SQL, no line, the cycle result unchanged;
RECORD = after each window's breadth capture one read-only transaction and the policy's would-be swaps logged only;
ON = accepted, behaves as RECORD (no declared reserve yet) with an ON_UNAVAILABLE line and a DEGRADED fault."""
from __future__ import annotations

import importlib
import json
import random
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime import advanced_options_continuous_scheduler as scheduler
from realtime import advanced_options_slot_rotation as rotation
from realtime.contracts import canonical_sha256

UTC = timezone.utc
SESSION = date(2026, 11, 17)
OPEN = datetime(2026, 11, 17, 14, 30, tzinfo=UTC)
CLOSE = datetime(2026, 11, 17, 21, 0, tzinfo=UTC)
SWITCH = rotation.SLOT_ROTATION_SETTING
# Pinned at the first build (image r134 candidate): any policy change must change this on purpose.
PINNED_POLICY_SHA256 = "0b3d9302b00d27ee4eb8cfdf56bf27dc8790333d58a0736f55f22f8b9148988e"


def cutoff(index):
    return OPEN + timedelta(minutes=5 * index)


def contract(underlying, side, strike, expiry="261127"):
    return f"O:{underlying}{expiry}{'C' if side == 'call' else 'P'}{int(strike * 1000):08d}"


def watched_name(underlying, calls=12, puts=12):
    return ([contract(underlying, "call", 100 + index) for index in range(calls)]
            + [contract(underlying, "put", 100 + index) for index in range(puts)])


def row(ticker, *, dvol=0, dprem=0.0, burst=0.0, tradable=True, oi=1000, delta=0.5, vol=1000, mid=1.0, spot=100.0,
        side="DEMAND", direction=1, reasons=None):
    underlying, kind = rotation.contract_parts(ticker)
    return {
        "c": ticker, "u": underlying, "type": kind, "dte": 10, "strike": 100.0, "vol": vol, "oi": oi, "mid": mid,
        "spread": 0.02, "delta": delta if kind == "call" else -delta, "iv": 0.3, "spot": spot, "tradable": tradable,
        "reasons": list(reasons if reasons is not None else ([] if tradable else ["VOLUME"])),
        "dvol": dvol, "dprem": dprem, "burst": burst, "side": side, "dir": direction,
    }


def hot(ticker, dprem=100_000.0, burst=5.0, **kwargs):
    return row(ticker, dvol=int(dprem / 100), dprem=dprem, burst=burst, **kwargs)


class Session:
    """Drives `decide_window` window by window: every (real or simulated) watched contract gets an idle row unless the
    window passes its own row for it."""

    def __init__(self, watched, names=None):
        self.state = rotation.RotationState()
        self.watched = tuple(watched)
        self.names = names or sorted({rotation.contract_parts(ticker)[0] for ticker in self.watched})

    def window(self, index, extra=(), *, active=(), complete=None, flow=None, packages=frozenset(), catalogued=None,
               as_of=None):
        rows = {
            ticker: row(ticker, dvol=10 if ticker in active else 0, tradable=False)
            for ticker in set(self.watched) | set(self.state.watched)
        }
        rows.update({item["c"]: item for item in extra})
        evals = list(rows.values())
        return rotation.decide_window(
            self.state, session_date=SESSION, equity_cutoff=cutoff(index),
            as_of=as_of or cutoff(index) + timedelta(seconds=90), session_open=OPEN, session_close=CLOSE,
            evals=evals, names=self.names, complete=self.names if complete is None else complete,
            watched=self.watched,
            catalogued={item["c"]: True for item in evals} if catalogued is None else catalogued,
            flow_last_print={} if flow is None else flow, package_legs=packages,
        )


def decisions(decision):
    return {item["c"]: item["decision"] for name in decision["names"] for item in name["candidates"]}


def warm(session, last_index=6, **kwargs):
    """Windows 3..last_index: the first has no prior window, then every watched contract idles."""
    for index in range(3, last_index + 1):
        session.window(index, **kwargs)


# ---- the switch and the policy ---------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, "OFF"), ("", "OFF"), ("  ", "OFF"), ("off", "OFF"), (" Record ", "RECORD"), ("on", "ON"),
])
def test_mode_values(raw, expected):
    environ = {} if raw is None else {SWITCH: raw}
    assert rotation.slot_rotation_mode(environ).value == expected


@pytest.mark.parametrize("raw", ["SHADOW", "1", "true", "RECORD_ONLY"])
def test_invalid_mode_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SWITCH):
        rotation.slot_rotation_mode({SWITCH: raw})


def test_the_options_scheduler_refuses_to_start_on_an_invalid_value(monkeypatch):
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(SWITCH, "SHADOW")
    with pytest.raises(ValueError, match=SWITCH):
        script.parse_args(argv)
    for good in ("", "OFF", "RECORD", "ON", "record"):
        monkeypatch.setenv(SWITCH, good)
        assert script.parse_args(argv).continuous


def test_policy_sha_is_the_canonical_hash_and_pinned():
    assert rotation.POLICY_SHA256 == canonical_sha256(rotation.POLICY) == PINNED_POLICY_SHA256
    assert canonical_sha256(json.loads(json.dumps(rotation.POLICY))) == PINNED_POLICY_SHA256
    assert rotation.POLICY["applied"] is False


def test_policy_values_are_the_reviewed_v1():
    policy = rotation.POLICY
    assert policy["tradable"]["min_dte"] == 1 and policy["tradable"]["max_dte"] == 45
    assert (policy["tradable"]["min_abs_delta"], policy["tradable"]["max_abs_delta"]) == (0.25, 0.70)
    assert policy["candidate"]["min_premium_5m"] == 50_000 and policy["candidate"]["min_burst"] == 3.0
    assert policy["candidate"]["confirm_windows"] == 2
    swap = policy["swap"]
    assert (swap["max_per_name_per_window"], swap["max_per_window"], swap["max_per_name_per_session"],
            swap["max_per_session"]) == (2, 8, 6, 48)
    assert (swap["min_calls_per_name"], swap["min_puts_per_name"]) == (8, 8)
    assert policy["eviction"]["idle_windows"] == 3 and policy["eviction"]["protect_promoted_windows"] == 6
    assert policy["timing"]["skip_first_windows"] == 2 and policy["timing"]["stop_minutes_before_close"] == 30


# ---- row evaluation -------------------------------------------------------------------------------------------------

def raw_row(ticker="O:SPY261127C00100000", *, observed=None, **overrides):
    observed = observed or cutoff(40) + timedelta(seconds=70)
    values = {
        "contract_ticker": ticker, "underlying": "SPY", "contract_type": "call",
        "expiration_date": date(2026, 11, 27), "strike_price": Decimal("100"), "day_volume": 1000,
        "open_interest": 500, "last_trade_price": Decimal("1.04"), "last_trade_at": observed - timedelta(seconds=3),
        "bid": Decimal("1.00"), "ask": Decimal("1.04"), "quote_at": observed - timedelta(seconds=5),
        "implied_volatility": 0.31, "delta": 0.45, "local_iv": 0.32, "local_delta": 0.5, "spot": Decimal("100.1"),
        "underlying_price": Decimal("100.0"), "observed_at": observed,
    }
    values.update(overrides)
    return values


def test_a_tradable_row_and_its_five_minute_activity():
    previous_observed = cutoff(39) + timedelta(seconds=70)
    prior = (400, 0.90, 0.30, 100.0, previous_observed.timestamp(), cutoff(39))
    evaluated = rotation.evaluate_row(raw_row(), prior, session_date=SESSION, session_open=OPEN,
                                      equity_cutoff=cutoff(40))
    assert evaluated["tradable"] and evaluated["reasons"] == []
    assert evaluated["delta"] == 0.5 and evaluated["iv"] == 0.32 and evaluated["spot"] == 100.1  # ours first
    assert evaluated["dte"] == 10 and evaluated["mid"] == pytest.approx(1.02)
    assert evaluated["dvol"] == 600
    assert evaluated["dprem"] == pytest.approx(600 * 1.02 * 100)
    intervals = (previous_observed - OPEN).total_seconds() / 300
    assert evaluated["burst"] == pytest.approx(600 / (400 / intervals))
    assert evaluated["div"] == pytest.approx(2.0)
    assert evaluated["pvd"] == pytest.approx(((1.02 - 0.90) - 0.5 * 0.1) / 0.90)
    assert evaluated["side"] == "DEMAND" and evaluated["dir"] == 1
    assert evaluated["last"] == "AT_ASK"
    assert rotation.is_candidate(evaluated)


def test_put_demand_points_down_and_unclear_has_no_direction():
    previous_observed = cutoff(39) + timedelta(seconds=70)
    prior = (400, 0.90, 0.30, 100.0, previous_observed.timestamp(), cutoff(39))
    put = rotation.evaluate_row(
        raw_row("O:SPY261127P00100000", contract_type="put", local_delta=-0.5, spot=Decimal("99.9")), prior,
        session_date=SESSION, session_open=OPEN, equity_cutoff=cutoff(40))
    assert put["side"] == "DEMAND" and put["dir"] == -1
    unclear = rotation.evaluate_row(raw_row(local_iv=0.29), prior, session_date=SESSION, session_open=OPEN,
                                    equity_cutoff=cutoff(40))
    assert unclear["side"] == "UNCLEAR" and unclear["dir"] == 0


def test_values_from_an_older_window_give_no_five_minute_activity():
    older = cutoff(38) + timedelta(seconds=70)
    evaluated = rotation.evaluate_row(raw_row(), (400, 0.9, 0.3, 100.0, older.timestamp(), cutoff(38)),
                                      session_date=SESSION, session_open=OPEN, equity_cutoff=cutoff(40))
    assert "dvol" not in evaluated and not rotation.is_candidate(evaluated)
    assert "dvol" not in rotation.evaluate_row(raw_row(), None, session_date=SESSION, session_open=OPEN,
                                               equity_cutoff=cutoff(40))


@pytest.mark.parametrize("overrides, reason", [
    ({"expiration_date": date(2026, 11, 17)}, "DTE"),
    ({"expiration_date": date(2027, 1, 30)}, "DTE"),
    ({"local_delta": 0.2}, "DELTA"),
    ({"local_delta": None, "delta": 0.75}, "DELTA"),
    ({"open_interest": 99}, "OI"),
    ({"open_interest": None}, "OI"),
    ({"day_volume": 499}, "VOLUME"),
    ({"bid": Decimal("0")}, "NO_BID"),
    ({"bid": Decimal("0.80"), "ask": Decimal("1.00")}, "SPREAD"),
    ({"bid": Decimal("0.20"), "ask": Decimal("0.21")}, "MID"),
    ({"quote_at": None}, "QUOTE_AGE"),
])
def test_each_tradable_filter_names_its_reason(overrides, reason):
    observed = cutoff(40) + timedelta(seconds=70)
    if "quote_at" not in overrides:
        overrides = {**overrides}
    evaluated = rotation.evaluate_row(raw_row(observed=observed, **overrides), None, session_date=SESSION,
                                      session_open=OPEN, equity_cutoff=cutoff(40))
    assert reason in evaluated["reasons"] and not evaluated["tradable"]


def test_a_stale_quote_is_not_tradable():
    observed = cutoff(40) + timedelta(seconds=70)
    evaluated = rotation.evaluate_row(raw_row(observed=observed, quote_at=observed - timedelta(seconds=61)), None,
                                      session_date=SESSION, session_open=OPEN, equity_cutoff=cutoff(40))
    assert evaluated["reasons"] == ["QUOTE_AGE"]


# ---- confirmation and hysteresis ------------------------------------------------------------------------------------

def test_a_candidate_is_promoted_only_after_two_consecutive_windows():
    session = Session(watched_name("SPY"))
    first = session.window(3)
    assert first["summary"]["blocked"]["names"] == {"NO_PRIOR_WINDOW": ["SPY"]}
    for index in (4, 5):
        assert session.window(index)["summary"]["evictable"] == 0  # idle 1, 2 windows
    assert session.window(6)["summary"]["evictable"] == 24  # idle 3 windows
    candidate = contract("SPY", "call", 150)
    seventh = session.window(7, [hot(candidate)])
    assert decisions(seventh) == {candidate: "NOT_CONFIRMED"}
    assert (seventh["summary"]["swaps_now"], seventh["summary"]["swaps_confirmed"]) == (1, 0)
    assert seventh["names"][0]["would_promote_unconfirmed"] == [candidate]
    eighth = session.window(8, [hot(candidate)])
    assert decisions(eighth) == {candidate: "PROMOTE"}
    (swap,) = eighth["swaps"]
    assert swap["promote"] == candidate and swap["evict"] == contract("SPY", "call", 100)  # same side, ticker order
    assert candidate in session.state.watched and swap["evict"] not in session.state.watched
    assert swap["evict"] in session.state.evicted
    assert eighth["summary"]["applied"] is False and eighth["summary"]["mode"] == "RECORD"


def test_a_missed_window_restarts_confirmation_and_idleness():
    session = Session(watched_name("SPY"))
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    gap = session.window(9, [hot(candidate)])
    assert gap["summary"]["blocked"]["names"] == {"PRIOR_WINDOW_GAP": ["SPY"]}
    assert decisions(gap) == {candidate: "PRIOR_WINDOW_GAP"}
    assert session.state.idle_streak == {}
    after = session.window(10, [hot(candidate)])
    assert decisions(after)[candidate] == "NOT_CONFIRMED"


def test_an_active_watched_contract_is_not_evicted_until_idle_three_windows():
    watched = watched_name("SPY")
    busy = set(watched)
    session = Session(watched)
    warm(session, active=busy)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)], active=busy)
    eighth = session.window(8, [hot(candidate)], active=busy - {watched[0]})
    assert decisions(eighth)[candidate] == "NO_EVICTABLE"
    assert eighth["summary"]["protected"]["NOT_IDLE"] == 24
    session.window(9, [hot(candidate)], active=busy - {watched[0]})
    tenth = session.window(10, [hot(candidate)], active=busy - {watched[0]})
    assert decisions(tenth)[candidate] == "PROMOTE" and tenth["swaps"][0]["evict"] == watched[0]
    assert tenth["swaps"][0]["evict_idle_windows"] == 3


# ---- timing gates ---------------------------------------------------------------------------------------------------

def test_no_swaps_in_the_first_two_windows():
    session = Session(watched_name("SPY"))
    candidate = contract("SPY", "call", 150)
    for index in (1, 2):
        decision = session.window(index, [hot(candidate)])
        assert decision["summary"]["blocked"]["window"] == ["FIRST_WINDOWS"]
        assert decisions(decision) == {candidate: "FIRST_WINDOWS"}
    assert session.window(3, [hot(candidate)])["summary"]["blocked"]["window"] == []


def test_no_swaps_from_thirty_minutes_before_the_close():
    session = Session(watched_name("SPY"))
    first, second = contract("SPY", "call", 150), contract("SPY", "call", 151)
    for index in range(66, 70):
        session.window(index)
    session.window(70, [hot(first)])
    before = session.window(71, [hot(first), hot(second, dprem=60_000.0)])  # 20:25
    assert decisions(before)[first] == "PROMOTE"
    closing = session.window(72, [hot(second, dprem=60_000.0)])  # 20:30
    assert closing["summary"]["blocked"]["window"] == ["CLOSING"]
    assert decisions(closing) == {second: "CLOSING"} and closing["swaps"] == []


def test_the_close_comes_from_the_calendar_not_a_fixed_clock():
    early_close = datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    state = rotation.RotationState()
    decision = rotation.decide_window(
        state, session_date=date(2026, 11, 27), equity_cutoff=datetime(2026, 11, 27, 17, 30, tzinfo=UTC),
        as_of=datetime(2026, 11, 27, 17, 31, tzinfo=UTC), session_open=datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
        session_close=early_close, evals=[], names=["SPY"], complete=["SPY"], watched=watched_name("SPY"),
        catalogued={}, flow_last_print={}, package_legs=set(),
    )
    assert decision["summary"]["blocked"]["window"] == ["CLOSING"]


# ---- budgets --------------------------------------------------------------------------------------------------------

def test_at_most_two_swaps_per_name_per_window_in_rank_order():
    session = Session(watched_name("SPY"))
    warm(session)
    hot_rows = [hot(contract("SPY", "call", 150 + index), dprem=100_000.0 + index * 1_000) for index in range(5)]
    session.window(7, hot_rows)
    decision = session.window(8, hot_rows)
    promoted = [swap["promote"] for swap in decision["swaps"]]
    assert promoted == [contract("SPY", "call", 154), contract("SPY", "call", 153)]
    assert sorted(decisions(decision).values()) == ["BUDGET_NAME_WINDOW"] * 3 + ["PROMOTE"] * 2


def test_at_most_eight_swaps_per_window_across_names():
    names = ["AAPL", "IWM", "MSFT", "QQQ", "SPY"]
    session = Session([ticker for name in names for ticker in watched_name(name)])
    warm(session)
    hot_rows = [
        hot(contract(name, "call", 150 + index), dprem=100_000.0 + rank * 10_000 + index)
        for rank, name in enumerate(names) for index in range(2)
    ]
    session.window(7, hot_rows)
    decision = session.window(8, hot_rows)
    assert decision["summary"]["swaps_confirmed"] == 8
    blocked = {ticker for ticker, value in decisions(decision).items() if value == "BUDGET_WINDOW"}
    assert blocked == {contract("AAPL", "call", 150), contract("AAPL", "call", 151)}  # the lowest ranked
    assert decision["summary"]["budgets_remaining"]["window"] == 0


def test_at_most_six_swaps_per_name_per_session():
    session = Session(watched_name("SPY"))
    warm(session, last_index=5)
    hot_rows = [hot(contract("SPY", "call", 150 + index), dprem=200_000.0 - index * 1_000) for index in range(10)]
    session.window(6, hot_rows[:0])
    session.window(7, hot_rows)
    counts = [session.window(index, hot_rows)["summary"]["swaps_confirmed"] for index in (8, 9, 10, 11)]
    assert counts == [2, 2, 2, 0]
    last = session.window(12, hot_rows)
    assert set(decisions(last).values()) == {"BUDGET_NAME_SESSION"}
    assert last["summary"]["budgets_remaining"]["name_session"] == {"SPY": 0}
    assert session.state.swaps_total == 6


def test_at_most_forty_eight_swaps_per_session():
    session = Session(watched_name("SPY"))
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    session.state.swaps_total = 48
    decision = session.window(8, [hot(candidate)])
    assert decisions(decision) == {candidate: "BUDGET_SESSION"}
    assert decision["summary"]["budgets_remaining"]["session"] == 0


def test_at_most_half_of_a_names_original_contracts_are_replaced():
    session = Session(watched_name("SPY", calls=4, puts=4))
    warm(session, last_index=5)
    hot_rows = [hot(contract("SPY", "call", 150 + index), dprem=200_000.0 - index * 1_000) for index in range(10)]
    session.window(6, hot_rows)
    assert session.window(7, hot_rows)["summary"]["swaps_confirmed"] == 2
    assert session.window(8, hot_rows)["summary"]["swaps_confirmed"] == 2
    capped = session.window(9, hot_rows)
    assert capped["swaps"] == []
    assert set(decisions(capped).values()) == {"BUDGET_REPLACED_FRACTION"}
    assert session.state.replaced_by_name["SPY"] == 4


def test_each_name_keeps_eight_calls_and_eight_puts():
    calls = [contract("SPY", "call", 100 + index) for index in range(16)]
    puts = [contract("SPY", "put", 100 + index) for index in range(8)]
    session = Session(calls + puts)
    warm(session, active=set(calls))
    call_candidate, put_candidate = contract("SPY", "call", 150), contract("SPY", "put", 150)
    session.window(7, [hot(call_candidate, dprem=90_000.0), hot(put_candidate, dprem=80_000.0)], active=set(calls))
    decision = session.window(8, [hot(call_candidate, dprem=90_000.0), hot(put_candidate, dprem=80_000.0)],
                              active=set(calls))
    assert decisions(decision) == {call_candidate: "SIDE_MINIMUM", put_candidate: "PROMOTE"}
    assert decision["swaps"][0]["evict_side"] == "put"


def test_a_side_already_short_is_never_reduced_but_may_swap_within_itself():
    calls = [contract("SPY", "call", 100 + index) for index in range(18)]
    puts = [contract("SPY", "put", 100 + index) for index in range(6)]
    session = Session(calls + puts)
    warm(session, active=set(calls))
    call_candidate = contract("SPY", "call", 150)
    session.window(7, [hot(call_candidate)], active=set(calls))
    assert decisions(session.window(8, [hot(call_candidate)], active=set(calls))) == {call_candidate: "SIDE_MINIMUM"}
    put_candidate = contract("SPY", "put", 150)
    session.window(9, [hot(put_candidate)], active=set(calls))
    assert decisions(session.window(10, [hot(put_candidate)], active=set(calls))) == {put_candidate: "PROMOTE"}


# ---- protections ----------------------------------------------------------------------------------------------------

def test_a_recent_flow_print_protects_and_an_old_one_does_not():
    watched = watched_name("SPY", calls=12, puts=12)
    session = Session(watched)
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    as_of = cutoff(8) + timedelta(seconds=90)
    flow = {watched[0]: as_of - timedelta(minutes=5), watched[1]: as_of - timedelta(minutes=20)}
    decision = session.window(8, [hot(candidate)], flow=flow, as_of=as_of)
    assert decision["summary"]["protected"] == {"RECENT_FLOW": 1}
    assert decision["swaps"][0]["evict"] == watched[1]


def test_unknown_flow_or_packages_protect_every_watched_contract():
    for kwargs, reason in (({"flow": None}, "FLOW_UNKNOWN"), ({"packages": None}, "PACKAGES_UNKNOWN")):
        session = Session(watched_name("SPY"))
        warm(session)
        candidate = contract("SPY", "call", 150)
        session.window(7, [hot(candidate)])
        decision = rotation.decide_window(
            session.state, session_date=SESSION, equity_cutoff=cutoff(8), as_of=cutoff(8) + timedelta(seconds=90),
            session_open=OPEN, session_close=CLOSE,
            evals=[row(ticker, tradable=False) for ticker in session.watched] + [hot(candidate)],
            names=["SPY"], complete=["SPY"], watched=session.watched, catalogued={candidate: True},
            flow_last_print=kwargs.get("flow", {}), package_legs=kwargs.get("packages", set()),
        )
        assert decision["summary"]["protected"][reason] == 24
        assert decision["summary"]["protected_only_by_unknown"] == 24  # idle 3+ windows: only the input blocks
        assert decisions(decision) == {candidate: "NO_EVICTABLE"}


def test_a_leg_of_a_session_package_is_protected():
    watched = watched_name("SPY")
    session = Session(watched)
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    decision = session.window(8, [hot(candidate)], packages={watched[0]})
    assert decision["summary"]["protected"] == {"PACKAGE_LEG": 1}
    assert decision["swaps"][0]["evict"] == watched[1]


def test_a_promoted_contract_is_protected_for_six_windows_and_an_evicted_one_never_returns():
    watched = watched_name("SPY")
    session = Session(watched)
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    promoted = session.window(8, [hot(candidate)])
    evicted = promoted["swaps"][0]["evict"]
    for index in range(9, 14):
        decision = session.window(index, [hot(evicted)])
        assert decision["names"][0]["protected"].get("RECENTLY_PROMOTED") == 1
        assert decisions(decision)[evicted] == "EVICTED_THIS_SESSION"
        assert decision["swaps"] == []
    free = session.window(14, [])  # 30 min after the promotion
    assert "RECENTLY_PROMOTED" not in free["summary"]["protected"]
    assert free["summary"]["evictable"] == 24 and session.state.idle_streak[candidate] == 6


def test_a_repeated_window_is_labelled_and_promotes_nothing():
    session = Session(watched_name("SPY"))
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    repeated = session.window(7, [hot(candidate)])
    assert repeated["summary"]["blocked"]["names"] == {"WINDOW_REPEATED": ["SPY"]}
    assert repeated["swaps"] == []


def test_a_name_without_a_complete_capture_gets_no_swaps():
    session = Session(watched_name("SPY") + watched_name("QQQ"))
    warm(session)
    spy, qqq = contract("SPY", "call", 150), contract("QQQ", "call", 150)
    session.window(7, [hot(spy), hot(qqq)])
    decision = session.window(8, [hot(spy), hot(qqq)], complete=["SPY"])
    assert decision["summary"]["captures"] == {"complete": 1, "names": 2, "not_complete": ["QQQ"]}
    assert decision["summary"]["blocked"]["names"] == {"CAPTURE_NOT_COMPLETE": ["QQQ"]}
    assert [swap["u"] for swap in decision["swaps"]] == ["SPY"]
    assert qqq not in decisions(decision)


def test_an_uncatalogued_candidate_is_never_promoted_and_the_share_is_reported():
    session = Session(watched_name("SPY"))
    warm(session)
    known, unknown = contract("SPY", "call", 150), contract("SPY", "call", 151)
    session.window(7, [hot(known), hot(unknown, dprem=200_000.0)])
    evals_catalogue = {known: True, unknown: False}
    decision = session.window(8, [hot(known), hot(unknown, dprem=200_000.0)], catalogued=evals_catalogue)
    assert decisions(decision) == {unknown: "NOT_CATALOGUED", known: "PROMOTE"}
    assert decision["summary"]["catalogued_share"] == 0.5
    missing = Session(watched_name("SPY"))
    warm(missing)
    missing.window(7, [hot(known)])
    assert decisions(missing.window(8, [hot(known)], catalogued={}))[known] == "CATALOGUE_UNKNOWN"


# ---- reserve hypothesis and outcomes --------------------------------------------------------------------------------

def test_the_hypothetical_reserve_is_frozen_at_the_first_window_and_counts_candidates():
    session = Session(watched_name("SPY"))
    statics = [row(contract("SPY", "call", 200 + index), oi=1000 + index * 10, tradable=False, reasons=["VOLUME"])
               for index in range(30)]
    not_static = row(contract("SPY", "call", 300), oi=99_999, tradable=False, reasons=["DELTA"])
    session.window(3, statics + [not_static])
    frozen = session.state.d_hyp["SPY"]
    assert frozen == tuple(contract("SPY", "call", 200 + index) for index in range(29, 5, -1))
    assert session.state.d_hyp_frozen_at["SPY"] == cutoff(3).isoformat()
    later = [hot(contract("SPY", "call", 229)), hot(contract("SPY", "call", 215)), hot(contract("SPY", "call", 208)),
             hot(contract("SPY", "call", 300)), hot(contract("SPY", "put", 400))]
    decision = session.window(4, later + [row(contract("SPY", "call", 250), oi=10**6, reasons=["VOLUME"],
                                              tradable=False)])
    assert session.state.d_hyp["SPY"] == frozen
    assert decision["summary"]["candidates_in_d_hyp"] == {"8": 1, "16": 2, "24": 3}
    assert decision["summary"]["session_totals"]["unique_candidates_in_d_hyp"] == {"8": 1, "16": 2, "24": 3}
    sizes = {item["c"]: item["d_hyp"] for item in decision["names"][0]["candidates"]}
    assert sizes[contract("SPY", "call", 229)] == 8 and sizes[contract("SPY", "call", 300)] is None


def test_outcomes_at_fifteen_thirty_and_sixty_minutes():
    watched = watched_name("SPY")
    session = Session(watched)
    warm(session)
    candidate = contract("SPY", "call", 150)
    session.window(7, [hot(candidate)])
    session.window(8, [hot(candidate, mid=1.0, spot=100.0, direction=1)])
    evicted = contract("SPY", "call", 100)
    outcomes = []
    for index in range(9, 22):
        decision = session.window(index, [row(candidate, mid=1.2, spot=101.0), row(evicted, vol=1500, mid=1.1,
                                                                                    tradable=False)])
        outcomes.extend(decision["outcomes"])
    assert [item["minutes"] for item in outcomes] == [15, 30, 60]
    first = outcomes[0]
    assert first["contract"] == candidate and first["promoted_cutoff"] == cutoff(8).isoformat()
    assert first["option_return"] == pytest.approx(0.2) and first["signed_stock_move"] == pytest.approx(0.01)
    assert first["evicted"] == evicted and first["evicted_volume_since"] == 500
    assert first["evicted_option_return"] == pytest.approx(0.1)
    assert first["applied"] is False and session.state.pending == []


def test_the_decisions_do_not_depend_on_row_order():
    def run(shuffle):
        session = Session(watched_name("SPY") + watched_name("QQQ"))
        warm(session)
        hot_rows = [hot(contract(name, side, 150 + index), dprem=100_000.0 + (index % 3) * 1_000)
                    for name in ("SPY", "QQQ") for side in ("call", "put") for index in range(4)]
        results = []
        for index in (7, 8, 9):
            rows = list(hot_rows)
            if shuffle:
                random.Random(index).shuffle(rows)
            decision = session.window(index, rows)
            decision["summary"].pop("seconds")
            results.append((decision["summary"], decision["names"], decision["swaps"]))
        return results

    assert run(False) == run(True)


# ---- the read ---------------------------------------------------------------------------------------------------------

OBSERVED = cutoff(40) + timedelta(seconds=70)


class FakeCursor:
    def __init__(self, script):
        self.script = list(script)
        self.statements = []
        self._rows = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        for needle, rows in self.script:
            if needle in sql:
                if isinstance(rows, Exception):
                    raise rows
                self._rows = list(rows)
                return
        self._rows = []

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, size):
        rows, self._rows = self._rows[:size], self._rows[size:]
        return rows


class FakeProvider:
    def __init__(self, cursor=None, error=None):
        self.cursor = cursor
        self.error = error
        self.transactions = []

    @contextmanager
    def transaction(self, role, **kwargs):
        self.transactions.append((role, kwargs))
        if self.error is not None:
            raise self.error
        yield SimpleNamespace(cursor=self.cursor)


def db_row(**overrides):
    values = raw_row(observed=OBSERVED, **overrides)
    return tuple(values[name] for name in rotation.ROW_COLUMNS)


def window_script(rows, *, flow=(), legs=(), catalogue=(), captures=None):
    return [
        ("rt.option_breadth_captures", captures if captures is not None else [("SPY", "COMPLETE", OBSERVED)]),
        ("rt.option_breadth_current", rows),
        ("ref.option_contracts", catalogue),
        ("rt.option_volume_checkpoints", flow),
        ("rt.option_packages", legs),
    ]


def test_the_read_is_one_bounded_read_only_pass():
    cursor = FakeCursor(window_script(
        [db_row(), db_row(contract_ticker="O:SPY261127C00101000", observed_at=OBSERVED - timedelta(minutes=5)),
         db_row(contract_ticker="O:QQQ261127C00101000", underlying="QQQ")],
        flow=[("O:SPY261127C00100000", OBSERVED - timedelta(minutes=2))], legs=[("O:SPY261127P00100000",)],
        catalogue=[("O:SPY261127C00102000",)],
        captures=[("QQQ", "PARTIAL", OBSERVED), ("SPY", "COMPLETE", OBSERVED), ("SPY", "FAILED", OBSERVED)],
    ))
    seen = []
    read = rotation.read_window_cursor(
        cursor, session_date=SESSION, equity_cutoff=cutoff(40), watched=["O:SPY261127C00100000"],
        consume=lambda rows: seen.extend(rows),
        candidate_tickers=lambda: ["O:SPY261127C00102000", "O:SPY261127C00103000", "O:SPY261127C00104000"],
        known_catalogued={"O:SPY261127C00104000"},
    )
    sql = [statement for statement, _ in cursor.statements]
    assert sql[0] == "SET LOCAL statement_timeout = '20s'"
    assert all(statement.startswith(("SET LOCAL", "SELECT", "SAVEPOINT", "RELEASE")) for statement in sql)
    assert read.captures == {"QQQ": ("PARTIAL", OBSERVED), "SPY": ("COMPLETE", OBSERVED)}
    assert read.complete == {"SPY": OBSERVED}
    rows_params = next(params for statement, params in cursor.statements if "option_breadth_current" in statement)
    assert rows_params == (SESSION, ["SPY"], [OBSERVED])
    assert [item["contract_ticker"] for item in seen] == ["O:SPY261127C00100000"]  # its capture only
    assert read.catalogued == {"O:SPY261127C00102000": True, "O:SPY261127C00103000": False,
                               "O:SPY261127C00104000": True}
    catalogue_params = next(params for statement, params in cursor.statements if "ref.option_contracts" in statement)
    assert catalogue_params == (["O:SPY261127C00102000", "O:SPY261127C00103000"],)
    assert read.flow_last_print == {"O:SPY261127C00100000": OBSERVED - timedelta(minutes=2)}
    assert read.package_legs == {"O:SPY261127P00100000"}
    assert read.states == {"catalogue": "KNOWN", "flow": "KNOWN", "packages": "KNOWN"}


def test_an_optional_read_that_fails_is_unknown_and_rolled_back_to_its_savepoint():
    script = window_script([db_row()])
    script[3] = ("rt.option_volume_checkpoints", RuntimeError("permission denied for table option_volume_checkpoints"))
    cursor = FakeCursor(script)
    read = rotation.read_window_cursor(
        cursor, session_date=SESSION, equity_cutoff=cutoff(40), watched=["O:SPY261127C00100000"],
        consume=lambda rows: list(rows), candidate_tickers=lambda: [],
    )
    assert read.flow_last_print is None and read.states["flow"] == "UNKNOWN"
    assert "permission denied" in read.errors["flow"]
    assert ("ROLLBACK TO SAVEPOINT al10_flow", ()) in cursor.statements
    assert read.package_legs == set() and read.states["packages"] == "KNOWN"
    assert read.states["catalogue"] == "NOT_NEEDED"


def test_no_checkpoint_for_any_watched_contract_is_unknown_flow():
    cursor = FakeCursor(window_script([db_row()]))
    read = rotation.read_window_cursor(
        cursor, session_date=SESSION, equity_cutoff=cutoff(40), watched=["O:SPY261127C00100000"],
        consume=lambda rows: list(rows), candidate_tickers=lambda: [],
    )
    assert read.flow_last_print is None and read.states["flow"] == "EMPTY"


# ---- the recorder ---------------------------------------------------------------------------------------------------

class Lines:
    def __init__(self):
        self.lines = []
        self.faults = []

    def emit(self, event, document):
        line = rotation.render_line(event, document)
        assert len(line.encode("utf-8")) < 16 * 1024
        self.lines.append((event, json.loads(line.split(" ", 1)[1])))

    def fault(self, **kwargs):
        self.faults.append(kwargs)

    def events(self):
        return [event for event, _ in self.lines]


def recorder(mode, lines):
    return rotation.SlotRotationRecorder(mode, emit=lines.emit, fault=lines.fault,
                                         session_bounds=lambda _day: (OPEN, CLOSE))


def test_off_runs_nothing_and_logs_nothing(monkeypatch, capsys):
    monkeypatch.delenv(SWITCH, raising=False)
    assert rotation.start_slot_rotation("OFF") is None
    assert rotation.start_slot_rotation(None) is None
    monkeypatch.setenv(SWITCH, " off ")
    assert rotation.start_slot_rotation(None) is None
    lines = Lines()
    off = recorder("OFF", lines)
    off.start()
    provider = FakeProvider(FakeCursor([]))
    assert off.record_window(provider, session_date=SESSION, equity_cutoff=cutoff(40), window_id="w",
                             watched=watched_name("SPY")) is None
    assert provider.transactions == [] and lines.lines == [] and lines.faults == []
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_record_reads_once_read_only_and_logs_the_window():
    lines = Lines()
    record = recorder("RECORD", lines)
    record.start()
    assert lines.events() == ["OPTIONS_SLOT_ROTATION_SETTING"]
    setting = lines.lines[0][1]
    assert setting["policy_sha256"] == PINNED_POLICY_SHA256 and setting["mode"] == "RECORD"
    provider = FakeProvider(FakeCursor(window_script([db_row()])))
    result = record.record_window(provider, session_date=SESSION, equity_cutoff=cutoff(40), window_id="window-1",
                                  watched=watched_name("SPY"), breadth={"SPY": {"state": "COMPLETE"}})
    assert provider.transactions == [("app", {"read_only": True})]
    assert result["status"] == "RECORDED" and result["applied"] is False and result["swaps_confirmed"] == 0
    event, summary = lines.lines[1]
    assert event == "OPTIONS_SLOT_ROTATION_RECORD"
    assert summary["window_id"] == "window-1" and summary["equity_cutoff"] == cutoff(40).isoformat()
    assert summary["contracts"] == 1 and summary["tradable"] == 1
    assert summary["blocked"]["names"] == {"NO_PRIOR_WINDOW": ["SPY"]}  # a fresh process
    assert summary["inputs"] == {"catalogue": "NOT_NEEDED", "flow": "EMPTY", "packages": "KNOWN"}
    assert summary["policy_sha256"] == PINNED_POLICY_SHA256 and summary["applied"] is False
    assert record.state.priors["O:SPY261127C00100000"][5] == cutoff(40)


def test_record_end_to_end_over_two_windows_uses_the_previous_values():
    lines = Lines()
    record = recorder("RECORD", lines)
    outside = contract("SPY", "call", 150)
    providers = []
    for index, volume in ((40, 1000), (41, 3000)):
        observed = cutoff(index) + timedelta(seconds=70)
        values = raw_row(outside, observed=observed, day_volume=volume)
        rows = [tuple(values[name] for name in rotation.ROW_COLUMNS)]
        providers.append(FakeProvider(FakeCursor(window_script(
            rows, captures=[("SPY", "COMPLETE", observed)], catalogue=[(outside,)]))))
        record.record_window(providers[-1], session_date=SESSION, equity_cutoff=cutoff(index), window_id=str(index),
                             watched=watched_name("SPY"))
    summaries = [document for event, document in lines.lines if event == "OPTIONS_SLOT_ROTATION_RECORD"]
    assert summaries[0]["blocked"]["names"] == {"NO_PRIOR_WINDOW": ["SPY"]}
    assert summaries[1]["blocked"]["names"] == {}
    assert summaries[1]["candidates_outside"] == 1 and summaries[1]["candidates_catalogued"] == 1
    assert summaries[1]["catalogued_share"] == 1.0 and summaries[1]["mode"] == "RECORD"
    name_lines = [document for event, document in lines.lines if event == "OPTIONS_SLOT_ROTATION_RECORD_NAME"]
    assert name_lines[-1]["candidates"][0]["dvol"] == 2000
    assert name_lines[-1]["candidates"][0]["decision"] == "NOT_CONFIRMED"
    # The catalogue is asked once per contract: a catalogued contract is remembered.
    asked = [params for statement, params in providers[1].cursor.statements if "ref.option_contracts" in statement]
    assert asked == [([outside],)] and outside in record.state.catalogued


def test_on_is_accepted_but_records_only():
    lines = Lines()
    on = recorder("ON", lines)
    on.start()
    assert lines.events() == ["OPTIONS_SLOT_ROTATION_SETTING", "OPTIONS_SLOT_ROTATION_ON_UNAVAILABLE"]
    assert lines.lines[1][1]["reason"] == "NO_DECLARED_RESERVE"
    (fault,) = lines.faults
    assert fault["severity"] == "DEGRADED" and fault["kind"] == "SLOT_ROTATION_ON_UNAVAILABLE"
    provider = FakeProvider(FakeCursor(window_script([db_row()])))
    result = on.record_window(provider, session_date=SESSION, equity_cutoff=cutoff(40), window_id="w",
                              watched=watched_name("SPY"))
    assert result["mode"] == "ON" and result["applied"] is False
    summary = lines.lines[2][1]
    assert summary["mode"] == "ON" and summary["applied"] is False
    assert provider.transactions == [("app", {"read_only": True})]


@pytest.mark.parametrize("error, reason", [
    (RuntimeError("database is gone"), "RuntimeError"),
    (type("PoolError", (Exception,), {})("connection pool exhausted"), "NO_CONNECTION"),
])
def test_a_failure_is_one_failed_line_and_one_fault_and_never_raises(error, reason):
    lines = Lines()
    record = recorder("RECORD", lines)
    result = record.record_window(FakeProvider(error=error), session_date=SESSION, equity_cutoff=cutoff(40),
                                  window_id="w", watched=watched_name("SPY"))
    assert result["status"] == "FAILED" and result["reason"] == reason
    assert lines.events() == ["OPTIONS_SLOT_ROTATION_RECORD_FAILED"]
    assert lines.lines[0][1]["reason"] == reason
    (fault,) = lines.faults
    assert fault["kind"] == "SLOT_ROTATION_RECORD_FAILED" and fault["key"] == reason
    assert fault["severity"] == "DEGRADED" and fault["service"] == "options-scheduler"


def test_a_failed_optional_input_is_a_fault_and_protects():
    lines = Lines()
    record = recorder("RECORD", lines)
    script = window_script([db_row()])
    script[4] = ("rt.option_packages", RuntimeError("relation does not exist"))
    record.record_window(FakeProvider(FakeCursor(script)), session_date=SESSION, equity_cutoff=cutoff(40),
                         window_id="w", watched=watched_name("SPY"))
    summary = lines.lines[0][1]
    assert summary["inputs"]["packages"] == "UNKNOWN"
    assert [fault["key"] for fault in lines.faults] == ["packages"]


def test_a_window_without_a_complete_capture_reads_nothing():
    lines = Lines()
    record = recorder("RECORD", lines)
    provider = FakeProvider(FakeCursor([]))
    result = record.record_window(provider, session_date=SESSION, equity_cutoff=cutoff(40), window_id="w",
                                  watched=watched_name("SPY"), breadth={"status": "FAILED", "reason": "x"})
    assert provider.transactions == []
    assert result["status"] == "RECORDED"
    assert lines.lines[0][1]["blocked"]["names"] == {"CAPTURE_NOT_COMPLETE": ["SPY"]}


def test_a_missing_calendar_blocks_swaps_and_is_reported():
    lines = Lines()

    def broken(_day):
        raise ImportError("exchange_calendars")

    record = rotation.SlotRotationRecorder("RECORD", emit=lines.emit, fault=lines.fault, session_bounds=broken)
    record.record_window(FakeProvider(FakeCursor(window_script([db_row()]))), session_date=SESSION,
                         equity_cutoff=cutoff(40), window_id="w", watched=watched_name("SPY"))
    assert "SESSION_CALENDAR_UNAVAILABLE" in lines.lines[0][1]["blocked"]["window"]
    assert [fault["key"] for fault in lines.faults] == ["calendar"]


def test_every_line_stays_under_the_docker_limit():
    names = ["AAPL", "AMD", "AMZN", "AVGO", "COIN", "GOOGL", "IWM", "MSFT", "MSTR", "MU", "NFLX", "NVDA", "PLTR",
             "QQQ", "SPY", "TSLA"]
    session = Session([ticker for name in names for ticker in watched_name(name)])
    warm(session)
    hot_rows = [hot(contract(name, side, 150 + index), dprem=60_000.0 + index)
                for name in names for side in ("call", "put") for index in range(40)]
    session.window(7, hot_rows)
    decision = session.window(8, hot_rows)
    documents = [("OPTIONS_SLOT_ROTATION_RECORD", decision["summary"])] + [
        ("OPTIONS_SLOT_ROTATION_RECORD_NAME", document) for document in decision["names"]]
    for event, document in documents:
        line = rotation.render_line(event, document)
        assert len(line.encode("utf-8")) < 16 * 1024
        json.loads(line.split(" ", 1)[1])
    huge = {"mode": "RECORD", "items": [{"c": "x" * 50, "n": index} for index in range(5_000)], "other": list(range(9))}
    line = rotation.render_line("OPTIONS_SLOT_ROTATION_RECORD", huge)
    document = json.loads(line.split(" ", 1)[1])
    assert len(line.encode("utf-8")) <= rotation.LINE_BYTE_LIMIT and document["truncated"] is True


def test_the_default_line_goes_to_stderr(capsys):
    rotation._print_line("OPTIONS_SLOT_ROTATION_RECORD", {"mode": "RECORD"})
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == 'OPTIONS_SLOT_ROTATION_RECORD {"mode":"RECORD"}\n'


# ---- the scheduler hook ----------------------------------------------------------------------------------------------

NOW = datetime(2026, 11, 17, 18, 31, 20, tzinfo=UTC)
RECEIPT = ("0f7f1d0e-8a51-4d55-9a3e-6c2b1d2f3a40", datetime(2026, 11, 17, 18, 30, tzinfo=UTC),
           NOW - timedelta(seconds=15), "a" * 64)
BREADTH_OBSERVED = NOW + timedelta(seconds=60)
AUTHORIZATION_ID = UUID("5b0d8f8e-3f43-4a51-9a64-2f1f6f3c9b10")
COHORT_ID = UUID("8e2f4a1c-7c55-4d0e-a0f1-0b3c5d7e9f21")


def _attached_cycle(monkeypatch, *, script=(), **cycle_kwargs):
    """`_run_schedule_cycle` through to ATTACHED with every collaborator faked (no database, no provider)."""
    production = importlib.import_module("scripts.run_advanced_options_v3_production")
    acquisition = SimpleNamespace(
        authority=SimpleNamespace(acquisition_stop=NOW + timedelta(hours=2), authorization_id=AUTHORIZATION_ID,
                                  session_date=SESSION, cohort_id=COHORT_ID),
        contract_tickers=tuple(sorted(watched_name("SPY"))),
    )
    state = SimpleNamespace(
        load_package=lambda: {"authorization_reference": "ref", "cohort": {"underlyings": ["SPY"]}},
        load_bundle_document=lambda: {}, load_preopen_session_binding=lambda bundle: "session",
        load_downstream_bindings=lambda session: (), load_acquisition_binding=lambda: acquisition,
        attach_downstream_binding=lambda *args: None,
    )
    capture = SimpleNamespace(
        selection=SimpleNamespace(reviewed_at=NOW, selected=(), valid_until=NOW + timedelta(seconds=60),
                                  policy=SimpleNamespace(maximum_snapshot_age_seconds=60)),
        evidence={"selection_sha256": "b" * 64},
    )

    class Factory:
        production_binding_payload = {}

        def __init__(self, *args):
            pass

        def bind_continuous_selection(self, **kwargs):
            return SimpleNamespace(history=SimpleNamespace(binding_sha256="c" * 64))

        def persist_selection_cursor(self, *args, **kwargs):
            return None

    monkeypatch.setattr(scheduler, "PostgresContinuousSessionStateRepository", lambda *a, **k: state)
    monkeypatch.setattr(scheduler, "_scheduler_budget", lambda *a, **k: "budget")
    monkeypatch.setattr(scheduler, "_capture", lambda *a, **k: capture)
    monkeypatch.setattr(scheduler, "p8_options_cycle_cutoff_from_capture", lambda *a, **k: NOW + timedelta(seconds=200))
    monkeypatch.setattr(production, "_authorize_window", lambda *a, **k: {
        "window_id": "6c1b3c3a-58c2-4bd0-bd52-3b1f6b0f6c11", "window_cycle_authorization_sha256": "d" * 64,
        "publication_authorization_reference": "ref:PUBLICATION"})
    monkeypatch.setattr(scheduler, "load_advanced_options_equity_input_binding_cursor",
                        lambda *a, **k: SimpleNamespace(binding_sha256="e" * 64))
    monkeypatch.setattr(scheduler, "_P8ProductionCapture", Factory)
    monkeypatch.setattr(scheduler, "build_p8_downstream_binding", lambda **k: SimpleNamespace(sequence=7))
    monkeypatch.setattr(scheduler, "_capture_breadth", lambda *a, **k: {
        "SPY": {"state": "COMPLETE", "contracts": 1, "pages": 1, "seconds": 1.0}})
    cursor = FakeCursor([
        ("rt.advanced_options_continuous_sessions", [("cohort",)]),
        ("rt.cycle_receipts", [RECEIPT]),
        *script,
    ])
    provider = FakeProvider(cursor)
    result = scheduler._run_schedule_cycle(
        provider, session_date=SESSION, api_key="k", clock=lambda: NOW, transport_factory=None,
        skipped_receipts=frozenset(), **cycle_kwargs,
    )
    return result, provider, cursor


def test_off_leaves_the_cycle_byte_identical(monkeypatch, capsys):
    baseline, base_provider, base_cursor = _attached_cycle(monkeypatch)
    assert baseline["status"] == "ATTACHED"
    off, off_provider, off_cursor = _attached_cycle(monkeypatch, slot_rotation=rotation.start_slot_rotation("OFF"))
    assert json.dumps(off, sort_keys=True) == json.dumps(baseline, sort_keys=True)
    assert list(off) == list(baseline) and "slot_rotation" not in off
    assert off_cursor.statements == base_cursor.statements
    assert off_provider.transactions == base_provider.transactions
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_record_adds_one_key_and_one_read_only_transaction(monkeypatch):
    baseline, base_provider, base_cursor = _attached_cycle(monkeypatch)
    lines = Lines()
    observed = BREADTH_OBSERVED
    values = raw_row(observed=observed)
    script = window_script([tuple(values[name] for name in rotation.ROW_COLUMNS)],
                           captures=[("SPY", "COMPLETE", observed)])
    result, provider, cursor = _attached_cycle(monkeypatch, script=script, slot_rotation=recorder("RECORD", lines))
    assert {key: value for key, value in result.items() if key != "slot_rotation"} == baseline
    assert result["slot_rotation"]["status"] == "RECORDED"
    assert provider.transactions[len(base_provider.transactions):] == [("app", {"read_only": True})]
    assert cursor.statements[:len(base_cursor.statements)] == base_cursor.statements
    summary = lines.lines[0][1]
    assert summary["window_id"] == baseline["window_id"] and summary["equity_cutoff"] == RECEIPT[1].isoformat()


def test_a_recorder_that_raises_never_fails_the_cycle(monkeypatch):
    class Broken:
        def record_window(self, *args, **kwargs):
            raise RuntimeError("unexpected")

    result, _provider, _cursor = _attached_cycle(monkeypatch, slot_rotation=Broken())
    assert result["status"] == "ATTACHED"
    assert result["slot_rotation"] == {"status": "FAILED", "reason": "RuntimeError: unexpected"}


def test_a_failing_read_is_a_failed_line_and_the_cycle_continues(monkeypatch):
    lines = Lines()
    script = [("rt.option_breadth_captures", RuntimeError("canceling statement due to statement timeout"))]
    result, _provider, _cursor = _attached_cycle(monkeypatch, script=script, slot_rotation=recorder("RECORD", lines))
    assert result["status"] == "ATTACHED"
    assert result["slot_rotation"]["status"] == "FAILED"
    assert lines.events() == ["OPTIONS_SLOT_ROTATION_RECORD_FAILED"]
    assert lines.faults[0]["kind"] == "SLOT_ROTATION_RECORD_FAILED"


def test_the_public_cycle_passes_the_recorder_through(monkeypatch):
    seen = {}

    def fake_cycle(provider, **kwargs):
        seen.update(kwargs)
        return {"status": "ATTACHED"}

    monkeypatch.setattr(scheduler, "_run_schedule_cycle", fake_cycle)
    provider = FakeProvider(FakeCursor([("pg_try_advisory_xact_lock", [(True,)])]))
    marker = object()
    scheduler.run_continuous_options_schedule_cycle(provider, session_date=SESSION, api_key="k", slot_rotation=marker)
    assert seen["slot_rotation"] is marker
    scheduler.run_continuous_options_schedule_cycle(provider, session_date=SESSION, api_key="k")
    assert seen["slot_rotation"] is None


# ---- the scheduler service --------------------------------------------------------------------------------------------

def _service(monkeypatch, value):
    script = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    stop = threading.Event()
    if value is None:
        monkeypatch.delenv(SWITCH, raising=False)
    else:
        monkeypatch.setenv(SWITCH, value)
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(script, "validate_target_database_v2", lambda *a, **k: None)
    pool = SimpleNamespace(closeall=lambda: None)
    monkeypatch.setattr(script, "_provider", lambda: (object(), pool))
    monkeypatch.setattr(script.signal, "signal", lambda *a: None)
    monkeypatch.setattr(script.threading, "Event", lambda: stop)
    monkeypatch.setattr(script, "AdvancedEquityLifecyclePlanner", lambda: SimpleNamespace(
        plan=lambda **_: SimpleNamespace(processing_session_date=SESSION)))
    calls = []

    def cycle(*_args, **kwargs):
        calls.append(kwargs)
        stop.set()
        return {"status": "WAITING_FOR_SESSION"}

    monkeypatch.setattr(script, "run_continuous_options_schedule_cycle", cycle)
    argv = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
            "--poll-seconds", "0.001", "--continuous", "--execute"]
    assert script.main(argv) == 0
    return calls


@pytest.mark.parametrize("value", [None, "", "OFF"])
def test_the_service_off_passes_no_recorder_and_logs_no_line(monkeypatch, capsys, value):
    calls = _service(monkeypatch, value)
    assert calls[0]["slot_rotation"] is None
    assert "OPTIONS_SLOT_ROTATION" not in capsys.readouterr().err


def test_the_service_in_record_starts_one_recorder(monkeypatch, capsys):
    calls = _service(monkeypatch, "RECORD")
    assert isinstance(calls[0]["slot_rotation"], rotation.SlotRotationRecorder)
    assert calls[0]["slot_rotation"].mode is rotation.SlotRotationMode.RECORD
    err = capsys.readouterr().err
    assert err.count("OPTIONS_SLOT_ROTATION_SETTING ") == 1 and "ON_UNAVAILABLE" not in err
