"""G106 B1 / V4.1 (Nov 13): C6 profile v4 - 24 contracts per underlying in product buckets recognised from the listed
expiries, the weekday weekly ladder, one call/put quality ranking. v1-v3 stay as they were (their own suite)."""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from realtime.advanced_options_activity_momentum_c6 import (
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    allocate_activity_momentum,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
    v4_product_kind,
    v4_weekly_ladder,
)
from realtime.advanced_options_selection_c6 import OptionSide
from tests.realtime.test_advanced_options_activity_momentum_c6 import _candidate, _expiry_calendar

UTC = timezone.utc
MONDAY = date(2026, 11, 2)
FRIDAY = date(2026, 11, 6)


def _v4():
    return load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)


def test_v4_profile_is_explicit_and_older_profiles_keep_their_bounds():
    profile = _v4()
    assert profile.version == "c6_activity_momentum_v4"
    assert (profile.default_contracts_per_underlying, profile.maximum_contracts_per_underlying) == (24, 24)
    assert [basket.basket_id for basket in profile.baskets] == [
        "SHORT_DATED_ACTIVITY", "CORE_MOMENTUM", "EXTENSION_ACTIVITY"]
    by_product = Counter()
    for bucket in profile.product_buckets:
        by_product[bucket.product] += bucket.unique_contracts
    assert by_product == Counter({"DAILY_EXPIRY": 24, "WEEKLY_EXPIRY": 24})
    assert [b.bucket_id for b in profile.product_buckets if b.product == "DAILY_EXPIRY"] == [
        "SAME_DAY", "NEAR", "WEEKLY", "CORE"]
    assert [b.bucket_id for b in profile.product_buckets if b.product == "WEEKLY_EXPIRY"] == [
        "WEEKLY", "CORE", "EXTENSION"]
    for version in (ACTIVITY_MOMENTUM_PROFILE_VERSION, SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION):
        older = load_activity_momentum_profile(version=version)
        assert (older.default_contracts_per_underlying, older.maximum_contracts_per_underlying) == (8, 12)
        assert older.product_buckets == ()


def test_weekly_ladder_follows_the_weekday():
    weekly = (FRIDAY, FRIDAY + timedelta(days=7), FRIDAY + timedelta(days=14))
    daily = tuple(MONDAY + timedelta(days=offset) for offset in range(0, 14) if (MONDAY + timedelta(days=offset)).weekday() < 5)
    # Monday to Wednesday: this week's weekly.
    for day in (MONDAY, MONDAY + timedelta(days=2)):
        assert v4_weekly_ladder(day, weekly, "WEEKLY_EXPIRY") == (FRIDAY,)
    # Thursday: this week's (1 DTE) and next week's.
    assert v4_weekly_ladder(MONDAY + timedelta(days=3), weekly, "WEEKLY_EXPIRY") == (FRIDAY, FRIDAY + timedelta(days=7))
    # Friday: next week's first; single stocks keep today's expiry (their 0DTE), index ETFs hold it in SAME_DAY.
    assert v4_weekly_ladder(FRIDAY, weekly, "WEEKLY_EXPIRY") == (FRIDAY + timedelta(days=7), FRIDAY)
    assert v4_weekly_ladder(FRIDAY, daily, "DAILY_EXPIRY") == (FRIDAY + timedelta(days=7),)


def test_weekly_ladder_keeps_a_holiday_weeks_thursday():
    # Good Friday 2026-04-03: the week's last listed expiry is Thursday 2026-04-02.
    monday = date(2026, 3, 30)
    expiries = (date(2026, 4, 2), date(2026, 4, 10))
    assert v4_weekly_ladder(monday, expiries, "WEEKLY_EXPIRY") == (date(2026, 4, 2),)


def test_product_recognition_from_listed_expiries():
    daily = SimpleNamespace(candidate_expiries=tuple(MONDAY + timedelta(days=offset) for offset in range(5)))
    weekly = SimpleNamespace(candidate_expiries=(FRIDAY,))
    holiday_week = SimpleNamespace(candidate_expiries=(MONDAY, MONDAY + timedelta(days=1), MONDAY + timedelta(days=2),
                                                       FRIDAY))
    assert v4_product_kind(daily, MONDAY) == "DAILY_EXPIRY"
    assert v4_product_kind(holiday_week, MONDAY) == "DAILY_EXPIRY"
    assert v4_product_kind(weekly, MONDAY) == "WEEKLY_EXPIRY"
    assert v4_product_kind(None, MONDAY) == "WEEKLY_EXPIRY"


def _rows(underlying, plan, *, session_date, observed_at):
    rows, index = [], 0
    for dte, count in plan:
        for number in range(count):
            side = OptionSide.CALL if number % 2 == 0 else OptionSide.PUT
            rows.append(_candidate(underlying, index, dte=dte, side=side, session_date=session_date,
                                   observed_at=observed_at, strike_millis=100_000 + 500 * (number // 2)))
            index += 1
    return tuple(rows)


def _allocate(rows, *, session_date, preparation_at, expiries):
    profile = _v4()
    underlying = rows[0].underlying
    week_end = session_date + timedelta(days=4 - session_date.weekday())
    calendar = _expiry_calendar(
        underlying, session_date=session_date, week_end_session_date=week_end,
        expiries=tuple(sorted(value for value in expiries if (value - session_date).days <= 5)),
        last_trading_at=datetime(session_date.year, session_date.month, session_date.day, 21, 0, tzinfo=UTC),
    )
    resolution = resolve_activity_momentum_selection(
        profile, session_date=session_date, candidates=rows, required_contract_tickers=(),
        preparation_at=preparation_at, expiry_calendars=(calendar,), same_day_enabled=None,
        short_delta_override=None,
    )
    allocation = allocate_activity_momentum(
        profile, session_date=session_date, rotation_key="v4-fixture", ready_underlyings=(underlying,),
        groups=resolution.groups, resolution=resolution,
    )
    selected = set(allocation.selected_contract_tickers)
    by_dte = Counter((row.expiration - session_date).days for row in rows if row.contract_ticker in selected)
    return allocation, by_dte


def test_index_etf_monday_fills_same_day_near_weekly_and_core():
    preparation_at = datetime(2026, 11, 1, 23, 0, tzinfo=UTC)
    plan = ((0, 10), (1, 6), (2, 6), (3, 4), (4, 12), (11, 6), (18, 6))
    rows = _rows("SPY", plan, session_date=MONDAY, observed_at=preparation_at)
    expiries = {MONDAY + timedelta(days=dte) for dte, _ in plan}
    allocation, by_dte = _allocate(rows, session_date=MONDAY, preparation_at=preparation_at, expiries=expiries)
    assert len(allocation.selected_contract_tickers) == 24
    assert by_dte[0] == 6                       # SAME_DAY
    assert by_dte[1] + by_dte[2] == 6           # NEAR
    assert by_dte[4] == 8                       # WEEKLY: this week's (Friday)
    assert by_dte[11] + by_dte[18] == 4         # CORE


def test_single_stock_monday_fills_weekly_core_and_extension():
    preparation_at = datetime(2026, 11, 1, 23, 0, tzinfo=UTC)
    plan = ((4, 12), (11, 10), (18, 10), (25, 6), (32, 6))
    rows = _rows("ACME", plan, session_date=MONDAY, observed_at=preparation_at)
    expiries = {MONDAY + timedelta(days=dte) for dte, _ in plan}
    allocation, by_dte = _allocate(rows, session_date=MONDAY, preparation_at=preparation_at, expiries=expiries)
    assert len(allocation.selected_contract_tickers) == 24
    assert by_dte[4] == 8                       # WEEKLY
    assert by_dte[11] + by_dte[18] == 12        # CORE
    assert by_dte[25] + by_dte[32] == 4         # EXTENSION


def test_single_stock_friday_takes_next_weeks_weekly_and_its_own_0dte():
    preparation_at = datetime(2026, 11, 5, 23, 0, tzinfo=UTC)
    plan = ((0, 8), (7, 12), (14, 10), (21, 6), (28, 6))
    rows = _rows("ACME", plan, session_date=FRIDAY, observed_at=preparation_at)
    expiries = {FRIDAY + timedelta(days=dte) for dte, _ in plan}
    allocation, by_dte = _allocate(rows, session_date=FRIDAY, preparation_at=preparation_at, expiries=expiries)
    assert len(allocation.selected_contract_tickers) == 24
    assert by_dte[0] + by_dte[7] >= 8           # WEEKLY ladder: next week's + today's
    assert by_dte[0] >= 1 and by_dte[7] >= 1
    assert by_dte[28] == 4                      # EXTENSION


def test_index_etf_friday_near_is_the_next_two_listed_sessions():
    preparation_at = datetime(2026, 11, 5, 23, 0, tzinfo=UTC)
    # Friday: 0DTE today, Monday (3), Tuesday (4), Wednesday (5), next Friday (7), core 14/21.
    plan = ((0, 10), (3, 6), (4, 6), (5, 4), (7, 12), (14, 6), (21, 6))
    rows = _rows("SPY", plan, session_date=FRIDAY, observed_at=preparation_at)
    expiries = {FRIDAY + timedelta(days=dte) for dte, _ in plan}
    allocation, by_dte = _allocate(rows, session_date=FRIDAY, preparation_at=preparation_at, expiries=expiries)
    assert len(allocation.selected_contract_tickers) == 24
    assert by_dte[0] == 6                       # SAME_DAY
    assert by_dte[3] + by_dte[4] == 6           # NEAR = Monday and Tuesday, not the empty weekend
    assert by_dte[7] >= 8                       # WEEKLY: next week's
    assert by_dte[5] == 0                       # Wednesday is neither NEAR nor the weekly
