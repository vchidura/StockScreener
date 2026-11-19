"""G106 D5 follow-up (Nov 18): single-stock same-day (0DTE) contracts in the Options watched set, RECORD first.

`REALTIME_V3_OPTIONS_STOCK_SAME_DAY` (options-lifecycle, CAPACITY_SHARDS stage, before AL11): OFF (default) = today's
allocation byte for byte; RECORD = today's allocation stored, the rule's computed and logged only; ON = a single stock
whose listed expiries include the session date (and whose weekly ladder does not: Mon / Wed) takes up to 4 same-day
contracts (2 calls + 2 puts, |delta| nearest 0.40 in 0.20-0.60, then prior OI) inside its own share, counted in its WEEKLY
bucket; today's allocation on any failure; invalid values refuse to start."""
from __future__ import annotations

import io
import json
from collections import Counter
from contextlib import contextmanager, redirect_stderr
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime import advanced_options_stock_same_day as same_day_module
from realtime.advanced_options_activity_momentum_c6 import (
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_STOCK_SAME_DAY_CONTRACTS,
    _v4_bucket_context,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
    v4_contract_bucket,
    v4_stock_same_day_eligible,
    v4_stock_same_day_plan,
    v4_stock_same_day_rank,
)
from realtime.advanced_options_automatic_preparation import PostgresAutomaticOptionsPreparationExecutor
from realtime.advanced_options_p8_selection_source import SEALED_CAPTURE_GENERALIZED_C6_PROFILE
from realtime.advanced_options_selection_c6 import OptionSide
from realtime.advanced_options_slot_selection import (
    PREVIOUS_CHAIN_SQL,
    SLOT_SELECTION_RECORD_EVENT,
    SLOT_SELECTION_SETTING,
    SlotSelectionMode,
)
from realtime.advanced_options_slot_side_balance import (
    SLOT_SIDE_BALANCE_RECORD_EVENT,
    SLOT_SIDE_BALANCE_SETTING,
    SlotSideBalanceMode,
)
from realtime.advanced_options_stock_same_day import (
    STOCK_SAME_DAY_POLICY,
    STOCK_SAME_DAY_RECORD_EVENT,
    STOCK_SAME_DAY_RECORD_FAILED_EVENT,
    STOCK_SAME_DAY_SETTING,
    StockSameDayMode,
    _print_line,
    allocate_with_stock_same_day,
    parse_stock_same_day_mode,
    stock_same_day_mode,
)
from realtime.contracts import canonical_sha256
from tests.realtime.test_advanced_options_activity_momentum_c6 import _candidate, _expiry_calendar

UTC = timezone.utc
MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY = (date(2026, 11, 16) + timedelta(days=n) for n in range(5))
# Single stocks list Monday / Wednesday / Friday expiries (NVDA, TSLA, AAPL, ... in Nov 2026) plus later Fridays.
STOCK_LISTING_WEEKDAYS = (0, 2, 4)
# Pinned on c2d5edb (image r137 lifecycle code, before this change): the OFF allocation of the fixture each weekday.
GOLDEN_OFF = {
    MONDAY: "7665d0cc3b4bc47008bac37d92135a577bced4ca2940437c3bf26c60112351b9",
    TUESDAY: "e07b9fea0aba085ac8e5d4c58fc75fab0ddaff97d8eee661ad4db85476d7832b",
    WEDNESDAY: "53bee125a8fc60f6cf22b19f01ffc3626ca0002bf70cddbd03cf15ec606fd376",
    THURSDAY: "87a8ee6c340b9154f348585eac6ec04f7ca44b8990d8ec961744ca46d6bb7c01",
    FRIDAY: "f6720a39d197c83425b29fae334f23cc5a0868f5453f78986c028d175fcb2653",
}
GOLDEN_AL8_ON = {
    MONDAY: "a1c5cd84a6a67dcb44cac1e23941130689d3d42c8cc05fc66f41c10297d13fd5",
    WEDNESDAY: "7f19cd97b8934a089ab2095e00f807b6613952fda4c4ae701745d2745693076f",
    FRIDAY: "098bb91864d0892b5ee92d15b66b00e75feaa6cbd07c032ad1552c1bf6216685",
}


# --- fixture: SPY (daily expiries) and ACME (a single stock, Mon / Wed / Fri listings) on each weekday --------------

def _listed(session, underlying):
    monday = session - timedelta(days=session.weekday())
    out = set()
    for offset in range(14):
        value = session + timedelta(days=offset)
        if value.weekday() >= 5:
            continue
        if underlying == "SPY" and offset <= 6:
            out.add(value)
        elif underlying != "SPY" and value.weekday() in STOCK_LISTING_WEEKDAYS:
            out.add(value)
    for weeks in (2, 3, 4, 5):
        out.add(monday + timedelta(days=7 * weeks + 4))
    return tuple(sorted(out))


def _observed(session):
    return datetime(session.year, session.month, session.day, 14, 33, tzinfo=UTC)


def _rows(session, *, adjust=None):
    """9 strikes 96-104 (spot 100) per expiry and side; call delta 0.50 - 0.08 per strike above 100 (so the 0.40-nearest
    picks are calls 101 / 102 and puts 99 / 98); calls more active than puts; OI 900 + 40 per strike step."""
    rows, index = [], 0
    for underlying in ("ACME", "SPY"):
        for expiration in _listed(session, underlying):
            dte = (expiration - session).days
            for step in range(-4, 5):
                strike = Decimal("100") + Decimal(step)
                for side in (OptionSide.CALL, OptionSide.PUT):
                    call_delta = max(Decimal("0.05"), min(
                        Decimal("0.95"), Decimal("0.50") - (strike - Decimal("100")) * Decimal("0.08")))
                    delta = call_delta if side is OptionSide.CALL else call_delta - Decimal("1")
                    base = 4_000 - 150 * abs(step) - 60 * dte
                    volume = base if side is OptionSide.CALL else base - 700
                    row = _candidate(underlying, index, dte=dte, side=side, session_date=session,
                                     observed_at=_observed(session), strike_millis=int(strike * 1000),
                                     prior_open_interest=900 + 40 * step, delta=delta)
                    row = replace(row, contract_id=1_000 + index + (0 if underlying == "SPY" else 50_000),
                                  completed_session_volume=max(volume, 60))
                    index += 1
                    if adjust is not None:
                        row = adjust(row)
                        if row is None:
                            continue
                    rows.append(row)
    return tuple(rows)


def _resolution(profile, session, rows):
    friday = session + timedelta(days=4 - session.weekday())
    calendars = []
    for underlying in sorted({row.underlying for row in rows}):
        expiries = tuple(sorted({row.expiration for row in rows
                                 if row.underlying == underlying and (row.expiration - session).days <= 5}))
        calendars.append(_expiry_calendar(
            underlying, session_date=session, week_end_session_date=friday, expiries=expiries,
            last_trading_at=datetime(session.year, session.month, session.day, 21, 0, tzinfo=UTC)))
    return resolve_activity_momentum_selection(
        profile, session_date=session, candidates=rows, required_contract_tickers=(),
        preparation_at=_observed(session), expiry_calendars=tuple(calendars), same_day_enabled=None,
        short_delta_override=None)


def _arguments(session, resolution):
    return dict(session_date=session, rotation_key="stock-same-day-fixture", ready_underlyings=("ACME", "SPY"),
                groups=resolution.groups, resolution=resolution)


@pytest.fixture(scope="module")
def profile():
    return load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)


@pytest.fixture(scope="module")
def days(profile):
    out = {}
    for session in (MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY):
        rows = _rows(session)
        out[session] = (rows, _resolution(profile, session, rows))
    return out


def _same_day(allocation, session, underlying="ACME"):
    prefix = f"O:{underlying}{session:%y%m%d}"
    return sorted(t for t in allocation.selected_contract_tickers if t.startswith(prefix))


def _names(allocation):
    return Counter(t[2:].split("2", 1)[0] for t in allocation.selected_contract_tickers)


def _tickers(allocation, underlying):
    return sorted(t for t in allocation.selected_contract_tickers if t.startswith(f"O:{underlying}"))


# --- switch ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, StockSameDayMode.OFF), ("", StockSameDayMode.OFF), ("  ", StockSameDayMode.OFF),
    ("off", StockSameDayMode.OFF), ("Record", StockSameDayMode.RECORD), (" On ", StockSameDayMode.ON),
    (StockSameDayMode.ON, StockSameDayMode.ON)])
def test_mode_values(raw, expected):
    assert parse_stock_same_day_mode(raw) is expected
    environ = {} if raw is None or isinstance(raw, StockSameDayMode) else {STOCK_SAME_DAY_SETTING: raw}
    if not isinstance(raw, StockSameDayMode):
        assert stock_same_day_mode(environ) is expected


@pytest.mark.parametrize("raw", ["YES", "1", "true", "SAME_DAY", "RECORD_ONLY", "OFF,ON", 1, True])
def test_invalid_mode_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=STOCK_SAME_DAY_SETTING):
        parse_stock_same_day_mode(raw)
    if isinstance(raw, str):
        with pytest.raises(ValueError, match=STOCK_SAME_DAY_SETTING):
            stock_same_day_mode({STOCK_SAME_DAY_SETTING: raw})
    with pytest.raises(ValueError, match=STOCK_SAME_DAY_SETTING):
        allocate_with_stock_same_day(mode=raw, profile=None, resolution=None, session_date=WEDNESDAY, arguments={},
                                     allocate_used=None, allocate_plain=None)


def test_flag_needs_the_v4_profile_and_a_boolean(profile, days):
    _rows_, resolution = days[WEDNESDAY]
    v3 = load_activity_momentum_profile(version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    with pytest.raises(ValueError, match="stock same-day reservation requires the v4 profile"):
        allocate_activity_momentum(v3, session_date=WEDNESDAY, rotation_key="x", ready_underlyings=("ACME",),
                                   groups=(), resolution=resolution, stock_same_day=True)
    with pytest.raises(ValueError, match="stock same-day reservation requires the v4 profile"):
        allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution), stock_same_day="ON")
    with pytest.raises(ValueError, match="v4 admission profile"):
        allocate_with_stock_same_day(mode="RECORD", profile=v3, resolution=resolution, session_date=WEDNESDAY,
                                     arguments={}, allocate_used=None, allocate_plain=None)


# --- OFF: byte for byte ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("session", [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY])
def test_off_matches_the_pinned_golden_each_weekday(profile, days, session):
    _rows_, resolution = days[session]
    default = allocate_activity_momentum(profile, **_arguments(session, resolution))
    explicit = allocate_activity_momentum(profile, **_arguments(session, resolution), stock_same_day=False)
    assert default.allocation_sha256 == explicit.allocation_sha256 == GOLDEN_OFF[session]
    assert canonical_sha256(activity_momentum_allocation_payload(default)) == canonical_sha256(
        activity_momentum_allocation_payload(explicit))
    if session in GOLDEN_AL8_ON:
        balanced = allocate_activity_momentum(profile, **_arguments(session, resolution), side_balance=True)
        assert balanced.allocation_sha256 == GOLDEN_AL8_ON[session]


def test_off_through_the_helper_is_one_call_and_no_line(profile, days):
    _rows_, resolution = days[WEDNESDAY]
    used_calls, plain_calls, lines = [], [], []

    def used(arguments):
        used_calls.append(dict(arguments))
        return allocate_activity_momentum(profile, **arguments)

    def plain(arguments):
        plain_calls.append(dict(arguments))
        return allocate_activity_momentum(profile, **arguments)

    allocation = allocate_with_stock_same_day(
        mode=StockSameDayMode.OFF, profile=profile, resolution=resolution, session_date=WEDNESDAY,
        arguments=_arguments(WEDNESDAY, resolution), allocate_used=used, allocate_plain=plain,
        emit=lambda *line: lines.append(line))
    assert allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]
    assert len(used_calls) == 1 and "stock_same_day" not in used_calls[0]
    assert plain_calls == [] and lines == []


def test_the_fixture_shows_the_cause(profile, days):
    """Today's code: Wednesday no single-stock 0DTE (the buckets use the whole share), Monday only leftovers (this week's
    Friday is DTE 4, short in the shortlist), Friday about 4 (today's expiry shares the WEEKLY bucket)."""
    counts = {session: len(_same_day(allocate_activity_momentum(profile, **_arguments(session, days[session][1])),
                                     session))
              for session in (MONDAY, WEDNESDAY, FRIDAY)}
    assert counts[WEDNESDAY] == 0
    assert 0 < counts[MONDAY] < V4_STOCK_SAME_DAY_CONTRACTS
    assert counts[FRIDAY] == V4_STOCK_SAME_DAY_CONTRACTS


# --- the rule -------------------------------------------------------------------------------------------------------

def _plan(profile, days, session, underlying="ACME"):
    rows, resolution = days[session]
    evidence = {row.underlying: row for row in resolution.short_expiry_evidence}
    return v4_stock_same_day_plan(profile, groups=[g for g in resolution.groups if g.underlying == underlying],
                                  session_date=session, expiry_evidence=evidence.get(underlying))


def test_plan_reasons_by_weekday_and_product(profile, days):
    assert _plan(profile, days, MONDAY)[0] == "APPLIED"
    assert _plan(profile, days, WEDNESDAY)[0] == "APPLIED"
    assert _plan(profile, days, TUESDAY) == ("NO_SAME_DAY_EXPIRY", ())
    assert _plan(profile, days, THURSDAY) == ("NO_SAME_DAY_EXPIRY", ())
    assert _plan(profile, days, FRIDAY) == ("SAME_DAY_IN_LADDER", ())
    for session in (MONDAY, WEDNESDAY, FRIDAY):
        assert _plan(profile, days, session, "SPY") == ("NOT_WEEKLY_EXPIRY", ())


def test_plan_picks_two_calls_and_two_puts_nearest_040(profile, days):
    reason, picks = _plan(profile, days, WEDNESDAY)
    chosen = [g.candidates[0] for g in picks]
    assert reason == "APPLIED" and len(chosen) == 4
    assert Counter(row.option_side for row in chosen) == Counter({OptionSide.CALL: 2, OptionSide.PUT: 2})
    assert sorted((row.option_side.value, row.strike) for row in chosen) == [
        ("CALL", Decimal("101")), ("CALL", Decimal("102")), ("PUT", Decimal("98")), ("PUT", Decimal("99"))]
    assert all(row.dte_for(WEDNESDAY) == 0 for row in chosen)
    assert [g.candidates[0] for g in picks] == sorted(chosen, key=v4_stock_same_day_rank)


class _Row:
    """A stand-in candidate: the given fields over a real fixture row (the real candidate validates its fields)."""

    def __init__(self, row, **overrides):
        self._row, self._overrides = row, overrides

    def __getattr__(self, name):
        if name in self._overrides:
            return self._overrides[name]
        return getattr(self._row, name)

    def dte_for(self, session_date):
        return self._row.dte_for(session_date)

    @property
    def relative_spread(self):
        bid, ask = self.bid, self.ask
        if bid is None or ask is None or bid + ask <= 0:
            return None
        return (ask - bid) / ((bid + ask) / 2)


def test_rank_band_then_distance_then_oi_then_ticker():
    base = _rows(WEDNESDAY)[0]
    near = _Row(base, delta=Decimal("0.41"), prior_open_interest=10, contract_ticker="O:ACME261118C00000002")
    near_more_oi = _Row(base, delta=Decimal("0.41"), prior_open_interest=5_000, contract_ticker="O:ACME261118C00000003")
    near_same_oi = _Row(base, delta=Decimal("0.41"), prior_open_interest=10, contract_ticker="O:ACME261118C00000001")
    far_in_band = _Row(base, delta=Decimal("0.59"), prior_open_interest=99_999)
    out_of_band = _Row(base, delta=Decimal("0.62"), prior_open_interest=99_999)
    below_band = _Row(base, delta=Decimal("0.15"), prior_open_interest=99_999)
    no_delta = _Row(base, delta=None, prior_open_interest=99_999)
    put = _Row(base, delta=Decimal("-0.40"), prior_open_interest=None)
    ordered = sorted([no_delta, below_band, out_of_band, far_in_band, near, near_more_oi, near_same_oi, put],
                     key=v4_stock_same_day_rank)
    assert ordered == [put, near_more_oi, near_same_oi, near, far_in_band, out_of_band, below_band, no_delta]


def test_eligibility_is_the_profile_dte0_criterion(profile, days):
    rows, _resolution_ = days[WEDNESDAY]
    row = next(r for r in rows if r.underlying == "ACME" and r.dte_for(WEDNESDAY) == 0)
    assert v4_stock_same_day_eligible(row, profile, WEDNESDAY)
    later = next(r for r in rows if r.underlying == "ACME" and r.dte_for(WEDNESDAY) == 2)
    assert not v4_stock_same_day_eligible(later, profile, WEDNESDAY)
    assert v4_stock_same_day_eligible(_Row(row), profile, WEDNESDAY)
    for changed in (
        _Row(row, bid=Decimal("0")),
        _Row(row, bid=None),
        _Row(row, ask=Decimal("0.90")),
        _Row(row, quote_as_of=None),
        _Row(row, quote_as_of=row.observed_at - timedelta(hours=2)),
        _Row(row, bid=Decimal("0.50"), ask=Decimal("1.00")),
        _Row(row, strike=Decimal("110")),
        _Row(row, spot=None),
        _Row(row, deliverable="ADJUSTED"),
        _Row(row, unavailable_reasons=("CATALOG_UNAVAILABLE",)),
    ):
        assert not v4_stock_same_day_eligible(changed, profile, WEDNESDAY), changed


# --- ON: the allocation ---------------------------------------------------------------------------------------------

def _on(profile, days, session, **extra):
    return allocate_activity_momentum(profile, **_arguments(session, days[session][1]), stock_same_day=True, **extra)


@pytest.mark.parametrize("session", [MONDAY, WEDNESDAY])
def test_on_adds_k_same_day_contracts_inside_the_share_on_mon_and_wed(profile, days, session):
    off = allocate_activity_momentum(profile, **_arguments(session, days[session][1]))
    on = _on(profile, days, session)
    assert _names(on) == _names(off) == Counter({"ACME": 24, "SPY": 24})
    assert len(on.selected_contract_tickers) == len(off.selected_contract_tickers) == 48
    _reason, picks = _plan(profile, days, session)
    planned = {g.candidates[0].contract_ticker for g in picks}
    assert planned <= set(_same_day(on, session))
    assert len(_same_day(on, session)) >= V4_STOCK_SAME_DAY_CONTRACTS
    # Index ETFs are untouched.
    assert _tickers(on, "SPY") == _tickers(off, "SPY")
    assert on.allocation_sha256 != off.allocation_sha256


def test_on_wednesday_takes_the_room_from_the_weekly_bucket_only(profile, days):
    rows, resolution = days[WEDNESDAY]
    by_ticker = {row.contract_ticker: row for row in rows}
    off = allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution))
    on = _on(profile, days, WEDNESDAY)
    evidence = {row.underlying: row for row in resolution.short_expiry_evidence}
    _product, ladder, ordinals, buckets = _v4_bucket_context(
        profile, groups=[g for g in resolution.groups if g.underlying == "ACME"], session_date=WEDNESDAY,
        expiry_evidence=evidence["ACME"])

    def buckets_of(allocation):
        return Counter(v4_contract_bucket(by_ticker[t], session_date=WEDNESDAY, ladder=ladder, ordinals=ordinals,
                                          buckets=buckets) for t in _tickers(allocation, "ACME"))

    assert buckets_of(off) == Counter({"WEEKLY": 8, "CORE": 12, "EXTENSION": 4})
    assert buckets_of(on) == Counter({"UNBUCKETED": 4, "WEEKLY": 4, "CORE": 12, "EXTENSION": 4})
    assert len(_same_day(on, WEDNESDAY)) == 4
    dropped = set(_tickers(off, "ACME")) - set(_tickers(on, "ACME"))
    assert len(dropped) == 4 and all(by_ticker[t].expiration == FRIDAY for t in dropped)


@pytest.mark.parametrize("session", [TUESDAY, THURSDAY, FRIDAY])
def test_on_changes_nothing_on_tue_thu_and_never_double_counts_friday(profile, days, session):
    on = _on(profile, days, session)
    assert on.allocation_sha256 == GOLDEN_OFF[session]
    if session is FRIDAY:
        assert len(_same_day(on, FRIDAY)) == V4_STOCK_SAME_DAY_CONTRACTS


def test_shortfall_when_fewer_than_k_are_eligible(profile):
    """Only 3 same-day contracts are admitted (2 calls, 1 put; the rest quote too wide): all 3 are kept, the WEEKLY
    bucket gives up 3, the name keeps 24."""
    narrow = {(OptionSide.CALL, Decimal("101")), (OptionSide.CALL, Decimal("102")), (OptionSide.PUT, Decimal("99"))}

    def adjust(row):
        if row.underlying == "ACME" and row.dte_for(WEDNESDAY) == 0 and (row.option_side, row.strike) not in narrow:
            return replace(row, bid=Decimal("0.50"), ask=Decimal("1.00"))
        return row

    rows = _rows(WEDNESDAY, adjust=adjust)
    resolution = _resolution(profile, WEDNESDAY, rows)
    evidence = {row.underlying: row for row in resolution.short_expiry_evidence}
    reason, picks = v4_stock_same_day_plan(profile, groups=[g for g in resolution.groups if g.underlying == "ACME"],
                                           session_date=WEDNESDAY, expiry_evidence=evidence["ACME"])
    assert reason == "SHORTFALL" and len(picks) == 3
    on = allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution), stock_same_day=True)
    off = allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution))
    assert len(_same_day(on, WEDNESDAY)) == 3 and len(_same_day(off, WEDNESDAY)) == 0
    assert _names(on) == _names(off) == Counter({"ACME": 24, "SPY": 24})


def test_a_missing_side_is_topped_up_from_the_other(profile):
    rows = _rows(WEDNESDAY, adjust=lambda row: None if (
        row.underlying == "ACME" and row.dte_for(WEDNESDAY) == 0 and row.option_side is OptionSide.PUT) else row)
    resolution = _resolution(profile, WEDNESDAY, rows)
    on = allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution), stock_same_day=True)
    same = _same_day(on, WEDNESDAY)
    assert len(same) == 4 and all(t[12] == "C" for t in same)
    assert _names(on) == Counter({"ACME": 24, "SPY": 24})


def test_no_eligible_same_day_changes_nothing(profile):
    rows = _rows(WEDNESDAY, adjust=lambda row: replace(row, bid=Decimal("0.50"), ask=Decimal("1.00")) if (
        row.underlying == "ACME" and row.dte_for(WEDNESDAY) == 0) else row)
    resolution = _resolution(profile, WEDNESDAY, rows)
    evidence = {row.underlying: row for row in resolution.short_expiry_evidence}
    assert v4_stock_same_day_plan(profile, groups=[g for g in resolution.groups if g.underlying == "ACME"],
                                  session_date=WEDNESDAY, expiry_evidence=evidence["ACME"]) == (
        "NO_ELIGIBLE_SAME_DAY", ())
    on = allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution), stock_same_day=True)
    off = allocate_activity_momentum(profile, **_arguments(WEDNESDAY, resolution))
    assert on.allocation_sha256 == off.allocation_sha256


def test_a_tight_share_keeps_the_budget(profile, days):
    """With 6 contracts per name the reservation still fits inside the share (4 same-day + 2 weekly)."""
    _rows_, resolution = days[WEDNESDAY]
    arguments = {**_arguments(WEDNESDAY, resolution), "per_underlying_contract_limit": 6,
                 "active_contract_budget": 12}
    off = allocate_activity_momentum(profile, **arguments)
    on = allocate_activity_momentum(profile, **arguments, stock_same_day=True)
    assert _names(on) == _names(off) == Counter({"ACME": 6, "SPY": 6})
    assert len(_same_day(on, WEDNESDAY)) == 4


# --- AL8 interplay --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("session", [MONDAY, WEDNESDAY])
def test_on_with_al8_on_keeps_both_balances(profile, days, session):
    rows, resolution = days[session]
    by_ticker = {row.contract_ticker: row for row in rows}
    on = _on(profile, days, session, side_balance=True)
    balanced = allocate_activity_momentum(profile, **_arguments(session, resolution), side_balance=True)
    assert _names(on) == _names(balanced)
    acme = Counter(by_ticker[t].option_side for t in _tickers(on, "ACME"))
    assert acme == Counter({OptionSide.CALL: 12, OptionSide.PUT: 12})
    same = Counter(by_ticker[t].option_side for t in _same_day(on, session))
    assert same[OptionSide.CALL] >= 2 and same[OptionSide.PUT] >= 2
    assert _tickers(on, "SPY") == _tickers(balanced, "SPY")


def test_al8_on_wednesday_weekly_bucket_splits_two_and_two(profile, days):
    rows, _resolution_ = days[WEDNESDAY]
    by_ticker = {row.contract_ticker: row for row in rows}
    on = _on(profile, days, WEDNESDAY, side_balance=True)
    friday = Counter(by_ticker[t].option_side for t in _tickers(on, "ACME") if by_ticker[t].expiration == FRIDAY)
    assert friday == Counter({OptionSide.CALL: 2, OptionSide.PUT: 2})
    assert Counter(by_ticker[t].option_side for t in _same_day(on, WEDNESDAY)) == Counter(
        {OptionSide.CALL: 2, OptionSide.PUT: 2})


# --- RECORD / ON through the helper ---------------------------------------------------------------------------------

def _helper(profile, days, session, mode, *, side_balance=False, plain=None, used=None, emit=None):
    _rows_, resolution = days[session]
    lines, calls = [], Counter()

    def allocate_used(arguments):
        calls["used"] += 1
        extra = {"side_balance": True} if side_balance else {}
        return allocate_activity_momentum(profile, **arguments, **extra)

    def allocate_plain(arguments):
        calls["plain"] += 1
        extra = {"side_balance": True} if side_balance else {}
        return allocate_activity_momentum(profile, **arguments, **extra)

    allocation = allocate_with_stock_same_day(
        mode=mode, profile=profile, resolution=resolution, session_date=session,
        arguments=_arguments(session, resolution), allocate_used=used or allocate_used,
        allocate_plain=plain or allocate_plain, emit=emit or (lambda event, document: lines.append((event, document))))
    return allocation, lines, calls


def test_record_stores_today_and_logs_summary_and_one_line_per_underlying(profile, days):
    allocation, lines, calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.RECORD)
    on = _on(profile, days, WEDNESDAY)
    assert allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]
    assert calls == Counter({"used": 1, "plain": 1})
    assert [event for event, _doc in lines] == [STOCK_SAME_DAY_RECORD_EVENT] * 3
    summary, acme, spy = [doc for _event, doc in lines]
    assert summary["part"] == "summary" and summary["mode"] == "RECORD" and summary["applied"] is False
    assert summary["off_allocation_sha256"] == GOLDEN_OFF[WEDNESDAY]
    assert summary["rule_allocation_sha256"] == on.allocation_sha256
    assert summary["rule_names"] == ["ACME"] and summary["reasons"] == {"APPLIED": 1, "NOT_WEEKLY_EXPIRY": 1}
    assert summary["same_day"] == {"off": len(_same_day(allocation, WEDNESDAY, "SPY")),
                                   "rule": len(_same_day(allocation, WEDNESDAY, "SPY")) + 4}
    assert summary["would_add"] == summary["would_drop"] == 4
    assert summary["contracts"] == {"off": 48, "rule": 48} and summary["per_name_counts_equal"] is True
    assert summary["policy"] == dict(STOCK_SAME_DAY_POLICY) and summary["lines"] == 3
    assert acme["underlying"] == "ACME" and acme["reason"] == "APPLIED" and acme["product"] == "WEEKLY_EXPIRY"
    assert acme["off_same_day"] == 0 and acme["rule_same_day"] == 4 and acme["target"] == 4
    _rows_, resolution = days[WEDNESDAY]
    admitted = {g.candidates[0].contract_ticker: g.candidates[0] for g in resolution.groups
                if g.underlying == "ACME" and len(g.candidates) == 1 and g.candidates[0].dte_for(WEDNESDAY) == 0}
    assert acme["eligible_same_day"] == {
        "calls": sum(1 for row in admitted.values() if row.option_side is OptionSide.CALL),
        "puts": sum(1 for row in admitted.values() if row.option_side is OptionSide.PUT)}
    assert min(acme["eligible_same_day"].values()) >= 2
    assert {row["contract"] for row in acme["would_add"]} == set(_same_day(on, WEDNESDAY))
    assert {row["contract"] for row in acme["chosen"]} == set(_same_day(on, WEDNESDAY))
    assert [row["abs_delta"] for row in acme["chosen"]] == ["0.420", "0.420", "0.340", "0.340"]
    assert all(row["bucket"] == "WEEKLY" and row["dte"] == 2 for row in acme["would_drop"])
    assert set(acme["chosen"][0]) == {"contract", "side", "dte", "abs_delta", "prior_oi"}
    assert spy["reason"] == "NOT_WEEKLY_EXPIRY" and spy["would_add_count"] == spy["would_drop_count"] == 0
    assert spy["target"] == 0 and spy["chosen"] == []


@pytest.mark.parametrize("session, reason", [(TUESDAY, "NO_SAME_DAY_EXPIRY"), (FRIDAY, "SAME_DAY_IN_LADDER")])
def test_record_on_days_without_a_rule(profile, days, session, reason):
    allocation, lines, _calls = _helper(profile, days, session, StockSameDayMode.RECORD)
    assert allocation.allocation_sha256 == GOLDEN_OFF[session]
    summary, acme, _spy = [doc for _event, doc in lines]
    assert summary["rule_names"] == [] and summary["would_add"] == 0
    assert summary["rule_allocation_sha256"] == summary["off_allocation_sha256"] == GOLDEN_OFF[session]
    assert acme["reason"] == reason and acme["chosen"] == []


def test_record_lines_are_json_under_the_docker_line_limit(profile, days, capsys):
    allocation, _lines, _calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.RECORD, emit=_print_line)
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 3 and allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]
    for line in err:
        event, document = line.split(" ", 1)
        assert event == STOCK_SAME_DAY_RECORD_EVENT
        assert json.loads(document)["session_date"] == "2026-11-18"
        assert len(line.encode()) < 16_384


def test_record_never_fails_the_stage(profile, days):
    def rule_fails(arguments):
        if arguments.get("stock_same_day"):
            raise RuntimeError("boom")
        return allocate_activity_momentum(profile, **arguments)

    allocation, lines, _calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.RECORD, plain=rule_fails)
    assert allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]
    assert [event for event, _doc in lines] == [STOCK_SAME_DAY_RECORD_FAILED_EVENT]
    assert lines[0][1]["stage"] == "RECORD" and "RuntimeError: boom" in lines[0][1]["error"]

    def emit_fails(event, document):
        raise OSError("stderr closed")

    allocation, _lines, _calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.RECORD, emit=emit_fails)
    assert allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]


def test_on_through_the_helper_uses_the_rule(profile, days):
    allocation, lines, calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.ON)
    assert allocation.allocation_sha256 == _on(profile, days, WEDNESDAY).allocation_sha256
    assert calls == Counter({"used": 1, "plain": 2})
    summary = lines[0][1]
    assert summary["applied"] is True and summary["mode"] == "ON"
    assert summary["rule_allocation_sha256"] == allocation.allocation_sha256
    assert summary["off_allocation_sha256"] == GOLDEN_OFF[WEDNESDAY]


def test_on_keeps_today_when_the_rule_fails(profile, days):
    def rule_fails(arguments):
        if arguments.get("stock_same_day"):
            raise RuntimeError("rule failed")
        return allocate_activity_momentum(profile, **arguments)

    allocation, lines, calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.ON, plain=rule_fails)
    assert allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]
    assert [event for event, _doc in lines] == [STOCK_SAME_DAY_RECORD_FAILED_EVENT]
    assert lines[0][1]["kept"] == "TODAY" and lines[0][1]["stage"] == "ALLOCATION"
    assert calls["used"] == 1


def test_on_keeps_today_when_a_name_would_change_its_count(profile, days):
    _rows_, resolution = days[WEDNESDAY]

    def short_rule(arguments):
        if arguments.get("stock_same_day"):
            return allocate_activity_momentum(profile, **{**arguments, "per_underlying_contract_limit": 20,
                                                          "active_contract_budget": 40})
        return allocate_activity_momentum(profile, **arguments)

    allocation, lines, _calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.ON, plain=short_rule)
    assert allocation.allocation_sha256 == GOLDEN_OFF[WEDNESDAY]
    assert lines[0][0] == STOCK_SAME_DAY_RECORD_FAILED_EVENT and "per-name contract counts" in lines[0][1]["error"]


def test_on_with_al8_on_through_the_helper(profile, days):
    allocation, _lines, _calls = _helper(profile, days, WEDNESDAY, StockSameDayMode.ON, side_balance=True)
    assert allocation.allocation_sha256 == _on(profile, days, WEDNESDAY, side_balance=True).allocation_sha256


# --- the executor: _allocate_capture end to end on a synthetic Wednesday capture, with AL8 and AL11 -----------------

CAPTURED_AT = datetime(2026, 11, 18, 14, 33, tzinfo=UTC)
PREVIOUS_CLOSE = datetime(2026, 11, 17, 21, 0, tzinfo=UTC)
REVISION_ID = UUID("22222222-2222-2222-2222-222222222222")


def _nanos(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000_000)


def _raw_capture():
    """SPY (daily: 0, 1, 2, 5, 6 DTE plus 9/16/23) and AAPL (Wed 0, Fri 2, Mon 5, then Fridays 9/16/23/30) on Wednesday
    Nov 18, 21 strikes 1% apart; deltas fall with the strike."""
    snapshots = {}
    plans = {"SPY": (Decimal("700"), (0, 1, 2, 5, 6, 9, 16, 23)), "AAPL": (Decimal("300"), (0, 2, 5, 9, 16, 23, 30))}
    for underlying, (spot, dtes) in plans.items():
        for dte in dtes:
            expiration = WEDNESDAY + timedelta(days=dte)
            for step in range(-10, 11):
                strike = spot * (1 + Decimal(step) / 100)
                call_delta = max(Decimal("0.03"), min(Decimal("0.97"),
                                                      Decimal("0.50") - Decimal(step) * Decimal("0.06")))
                for kind, delta in (("call", call_delta), ("put", call_delta - 1)):
                    ticker = f"O:{underlying}{expiration:%y%m%d}{kind[0].upper()}{int(strike * 1000):08d}"
                    session_row = (step + dte) % 5 == 0
                    snapshots[ticker] = {
                        "details": {"ticker": ticker, "contract_type": kind, "exercise_style": "american",
                                    "expiration_date": expiration.isoformat(), "strike_price": float(strike),
                                    "shares_per_contract": 100},
                        "last_quote": {"bid": 2.0, "ask": 2.1,
                                       "last_updated": _nanos(CAPTURED_AT - timedelta(seconds=20))},
                        "day": {"volume": 30 if session_row else 2_000 - 90 * abs(step),
                                "last_updated": _nanos(CAPTURED_AT - timedelta(seconds=40) if session_row
                                                       else datetime(2026, 11, 17, 20, 59, tzinfo=UTC))},
                        "greeks": {"delta": float(delta)},
                        "implied_volatility": 0.3,
                        "open_interest": 1_000 + 50 * step,
                        "underlying_asset": {"ticker": underlying, "price": float(spot)},
                    }
    return {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
        "configured_underlyings": ["AAPL", "SPY"],
        "captured_at_utc": CAPTURED_AT.isoformat(),
        "session_date": WEDNESDAY.isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [{"underlyer": "AAPL"}, {"underlyer": "SPY"}],
        "selected_snapshots": snapshots,
    }


def _previous_rows(capture):
    rows = []
    for ticker, raw in capture["selected_snapshots"].items():
        details = raw["details"]
        expiration = date.fromisoformat(details["expiration_date"])
        volume = (3_000 if details["contract_type"] == "put" else 1_000) + int(details["strike_price"]) % 37
        rows.append((raw["underlying_asset"]["ticker"], ticker, expiration, volume, Decimal("2.0"), Decimal("2.1"),
                     PREVIOUS_CLOSE - timedelta(minutes=4)))
    return rows


class _Provider:
    def __init__(self, previous_rows):
        self.previous_rows, self.previous_reads = previous_rows, 0

    @contextmanager
    def transaction(self, *_args, **kwargs):
        provider = self

        class Cursor:
            rows = []

            def execute(self, sql, params=None):
                if sql == PREVIOUS_CHAIN_SQL:
                    assert kwargs.get("read_only") is True
                    provider.previous_reads += 1
                    self.rows = provider.previous_rows
                else:
                    self.rows = []

            def fetchall(self):
                return list(self.rows)

        yield SimpleNamespace(cursor=Cursor())


def _executor(capture, **attributes):
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._exact_four_compatibility = False
    executor._admission_profile_version = V4_ACTIVITY_MOMENTUM_PROFILE_VERSION
    executor._enabled_strategies = ()
    executor._active_quote_contract_budget = None
    executor._per_underlying_contract_limit = None
    executor._measured_capacity_limit = None
    executor._effective_provider_project_ceiling = None
    executor._provider = _Provider(_previous_rows(capture))
    for name, value in attributes.items():
        setattr(executor, name, value)
    return executor


def _same_day_count(tickers, underlying):
    prefix = f"O:{underlying}{WEDNESDAY:%y%m%d}"
    return sum(1 for ticker in tickers if ticker.startswith(prefix))


@pytest.fixture(scope="module")
def executor_runs():
    capture = _raw_capture()
    runs = {}
    plans = (
        ("legacy", {}),
        ("OFF", {"_stock_same_day": StockSameDayMode.OFF}),
        ("RECORD", {"_stock_same_day": StockSameDayMode.RECORD}),
        ("ON", {"_stock_same_day": StockSameDayMode.ON}),
        ("RECORD+AL8REC", {"_stock_same_day": StockSameDayMode.RECORD,
                           "_slot_side_balance": SlotSideBalanceMode.RECORD}),
        ("ON+AL8REC", {"_stock_same_day": StockSameDayMode.ON, "_slot_side_balance": SlotSideBalanceMode.RECORD}),
        ("ON+AL8ON", {"_stock_same_day": StockSameDayMode.ON, "_slot_side_balance": SlotSideBalanceMode.ON}),
        ("AL8ON", {"_slot_side_balance": SlotSideBalanceMode.ON}),
        ("ON+AL11REC", {"_stock_same_day": StockSameDayMode.ON, "_slot_selection": SlotSelectionMode.RECORD}),
        ("ON+AL11ON", {"_stock_same_day": StockSameDayMode.ON, "_slot_selection": SlotSelectionMode.ON}),
        ("ON+AL11ON+AL8ON", {"_stock_same_day": StockSameDayMode.ON, "_slot_selection": SlotSelectionMode.ON,
                             "_slot_side_balance": SlotSideBalanceMode.ON}),
        ("AL11ON", {"_slot_selection": SlotSelectionMode.ON}),
    )
    for label, attributes in plans:
        executor = _executor(capture, **attributes)
        claim = SimpleNamespace(revision_id=REVISION_ID, session_date=WEDNESDAY, option_underlyings=("AAPL", "SPY"))
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            filtered, allocation = executor._allocate_capture(claim, json.loads(json.dumps(capture)))
        lines = []
        for line in stderr.getvalue().splitlines():
            event, _, body = line.partition(" ")
            if event.startswith("OPTIONS_") and body.startswith("{"):
                lines.append((event, json.loads(body)))
        runs[label] = (filtered, allocation, lines, executor._provider.previous_reads)
    return runs


def test_executor_off_and_record_are_byte_identical(executor_runs):
    legacy_capture, legacy, legacy_lines, _reads = executor_runs["legacy"]
    assert legacy_lines == []
    for label in ("OFF", "RECORD", "RECORD+AL8REC"):
        filtered, allocation, lines, reads = executor_runs[label]
        assert allocation.allocation_sha256 == legacy.allocation_sha256
        assert canonical_sha256(filtered) == canonical_sha256(legacy_capture)
        assert reads == 0
    assert executor_runs["OFF"][2] == []
    assert _same_day_count(legacy.selected_contract_tickers, "AAPL") == 0
    record_lines = [doc for event, doc in executor_runs["RECORD"][2] if event == STOCK_SAME_DAY_RECORD_EVENT]
    assert len(record_lines) == 3 and record_lines[0]["rule_names"] == ["AAPL"]
    assert record_lines[0]["same_day"]["rule"] - record_lines[0]["same_day"]["off"] == 4


def test_executor_on_stores_the_rule_within_the_same_capacity(executor_runs):
    legacy_capture, legacy, _lines, _reads = executor_runs["legacy"]
    for label in ("ON", "ON+AL8REC", "ON+AL8ON"):
        filtered, allocation, lines, _reads = executor_runs[label]
        assert allocation.allocation_sha256 != legacy.allocation_sha256
        assert set(filtered["selected_snapshots"]) == set(allocation.selected_contract_tickers)
        assert filtered["activity_momentum_allocation"]["allocation_sha256"] == allocation.allocation_sha256
        assert Counter(t[2:5] for t in allocation.selected_contract_tickers) == Counter(
            t[2:5] for t in legacy.selected_contract_tickers)
        assert _same_day_count(allocation.selected_contract_tickers, "AAPL") == 4
        assert _same_day_count(allocation.selected_contract_tickers, "SPY") >= 1
        assert not any(event == STOCK_SAME_DAY_RECORD_FAILED_EVENT for event, _doc in lines)


def test_executor_al8_lines_are_logged_once(executor_runs):
    """RECORD and ON of this switch keep the AL8 record at one summary + one line per underlying."""
    for label in ("RECORD+AL8REC", "ON+AL8REC", "ON+AL8ON"):
        lines = executor_runs[label][2]
        al8 = [doc for event, doc in lines if event == SLOT_SIDE_BALANCE_RECORD_EVENT]
        assert len(al8) == 3 and al8[0]["part"] == "summary"
        stock = [doc for event, doc in lines if event == STOCK_SAME_DAY_RECORD_EVENT]
        assert len(stock) == 3
    on_al8 = executor_runs["ON+AL8ON"][1]
    al8_summary = next(doc for event, doc in executor_runs["ON+AL8ON"][2]
                       if event == SLOT_SIDE_BALANCE_RECORD_EVENT and doc["part"] == "summary")
    assert al8_summary["balanced_allocation_sha256"] == on_al8.allocation_sha256
    sides = Counter(t[-9] for t in on_al8.selected_contract_tickers)
    assert sides["C"] == sides["P"]


def test_executor_al11_keeps_the_new_same_day_count(executor_runs):
    """AL11 runs after this switch and keeps today's 0DTE count per name: with this switch ON, AAPL's 4."""
    _f, legacy_al11, _l, _r = executor_runs["AL11ON"]
    assert _same_day_count(legacy_al11.selected_contract_tickers, "AAPL") == 0
    rule = executor_runs["ON"][1]
    for label in ("ON+AL11ON", "ON+AL11ON+AL8ON"):
        filtered, allocation, lines, reads = executor_runs[label]
        assert reads == 1
        assert _same_day_count(allocation.selected_contract_tickers, "AAPL") == 4
        assert _same_day_count(allocation.selected_contract_tickers, "SPY") == _same_day_count(
            rule.selected_contract_tickers, "SPY")
        al11 = [doc for event, doc in lines if event == SLOT_SELECTION_RECORD_EVENT]
        aapl = next(doc for doc in al11 if doc.get("underlying") == "AAPL")
        assert aapl["rule_applied"] is True and aapl["same_day"] == 4 and aapl["selection_count"] == 24
        assert set(filtered["selected_snapshots"]) == set(allocation.selected_contract_tickers)
    _f, recorded, lines, _r = executor_runs["ON+AL11REC"]
    assert recorded.allocation_sha256 == rule.allocation_sha256
    aapl = next(doc for event, doc in lines if event == SLOT_SELECTION_RECORD_EVENT and doc.get("underlying") == "AAPL")
    assert aapl["same_day"] == 4 and aapl["dte_mix"]["off"]["0"] == aapl["dte_mix"]["selection"]["0"] == 4


def test_allocate_capture_routes_through_the_switch():
    """OFF keeps the AL8 call as it was; RECORD / ON go through `_stock_same_day_allocation`; AL11 still runs after."""
    import inspect

    from realtime import advanced_options_automatic_preparation as preparation

    source = inspect.getsource(preparation.PostgresAutomaticOptionsPreparationExecutor._allocate_capture)
    assert '"_stock_same_day"' in source and "StockSameDayMode.OFF" in source
    assert "if stock_same_day is StockSameDayMode.OFF:" in source
    assert source.index("allocate_with_side_balance(") < source.index("self._stock_same_day_allocation(") < (
        source.index("self._slot_selection_allocation("))
    assert "= allocate_activity_momentum(" not in source


# --- service: constructor and lifecycle main ------------------------------------------------------------------------

def test_executor_refuses_an_invalid_value_and_a_non_v4_profile():
    with pytest.raises(ValueError, match=STOCK_SAME_DAY_SETTING):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            stock_same_day="MAYBE")
    for mode in ("RECORD", "ON"):
        with pytest.raises(ValueError, match=f"{STOCK_SAME_DAY_SETTING} requires the v4"):
            PostgresAutomaticOptionsPreparationExecutor(
                None, api_key="k", backend_image_id="i",
                admission_profile_version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION, stock_same_day=mode)
    # A valid v4 value passes this check (the next check, the provider, then fails as before).
    with pytest.raises(ValueError, match=r"configuration is invalid$"):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            stock_same_day="ON")


class _Constructed(Exception):
    pass


def _run_lifecycle_main(monkeypatch, value):
    from scripts import run_advanced_options_v3_lifecycle as resident

    seen = {}

    def executor(*_args, **kwargs):
        seen.update(kwargs)
        raise _Constructed

    monkeypatch.setenv("POLYGON_API_KEY", "offline-fixture")
    monkeypatch.setenv("REALTIME_V3_OPTIONS_ADMISSION_PROFILE", V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    monkeypatch.delenv(SLOT_SIDE_BALANCE_SETTING, raising=False)
    monkeypatch.delenv(SLOT_SELECTION_SETTING, raising=False)
    if value is None:
        monkeypatch.delenv(STOCK_SAME_DAY_SETTING, raising=False)
    else:
        monkeypatch.setenv(STOCK_SAME_DAY_SETTING, value)
    monkeypatch.setattr(resident, "dotenv_values", lambda *_: {})
    monkeypatch.setattr(resident, "_provider", lambda: (SimpleNamespace(), SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(resident, "PostgresAutomaticOptionsPreparationExecutor", executor)
    args = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2", "--continuous",
            "--execute"]
    return resident, seen, args


@pytest.mark.parametrize("value, expected", [
    (None, StockSameDayMode.OFF), ("", StockSameDayMode.OFF), ("record", StockSameDayMode.RECORD),
    (" ON ", StockSameDayMode.ON)])
def test_lifecycle_service_passes_the_switch(monkeypatch, value, expected):
    resident, seen, args = _run_lifecycle_main(monkeypatch, value)
    with pytest.raises(_Constructed):
        resident.main(args)
    assert seen["stock_same_day"] is expected
    assert seen["slot_side_balance"] is SlotSideBalanceMode.OFF
    assert seen["slot_selection"] is SlotSelectionMode.OFF


def test_lifecycle_service_refuses_to_start_on_an_invalid_value(monkeypatch):
    resident, seen, args = _run_lifecycle_main(monkeypatch, "SAMEDAY")
    with pytest.raises(ValueError, match=STOCK_SAME_DAY_SETTING):
        resident.main(args)
    assert seen == {}


def test_setting_line(capsys):
    same_day_module.log_stock_same_day_setting(StockSameDayMode.RECORD)
    event, _, body = capsys.readouterr().err.strip().partition(" ")
    assert event == "OPTIONS_STOCK_SAME_DAY_SETTING"
    assert json.loads(body) == {"mode": "RECORD", "setting": STOCK_SAME_DAY_SETTING,
                                "policy_version": STOCK_SAME_DAY_POLICY["version"]}
