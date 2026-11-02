from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from realtime.advanced_options_activity_momentum_c6 import (
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ActivityMomentumCandidate,
    ActivityMomentumExpiryCalendarEvidence,
    ActivityMomentumGroup,
    ActivityMomentumLastTradingEvidence,
    CoverageState,
    activity_momentum_allocation_from_payload,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    c6_coverage_from_activity_allocation,
    c6_members_from_activity_allocation,
    compare_activity_momentum_profiles,
    load_activity_momentum_profile,
    resolve_activity_momentum_groups,
    resolve_activity_momentum_selection,
)
from realtime.advanced_options_selection_c6 import (
    C6ConnectionCapacity,
    C6ReadinessState,
    C6UnderlyingReadiness,
    OptionSide,
    StrategyPolicyIdentity,
    build_c6_selection_plan,
    c6_selection_plan_from_payload,
    c6_selection_plan_payload,
    load_advanced_options_selection_c6_policy,
)
from realtime.contracts import canonical_sha256
from scripts.prepare_advanced_options_v3_pre_session import (
    parse_args as parse_pre_session_args,
)


UTC = timezone.utc
SESSION_DATE = date(2026, 11, 2)
OBSERVED_AT = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _symbols(count: int) -> tuple[str, ...]:
    values = []
    index = 0
    while len(values) < count:
        number = index
        letters = ""
        while True:
            letters = chr(ord("A") + number % 26) + letters
            number = number // 26 - 1
            if number < 0:
                break
        values.append(letters)
        index += 1
    return tuple(values)


def _ticker(
    underlying: str,
    side: OptionSide,
    *,
    dte: int,
    strike_millis: int,
    session_date: date = SESSION_DATE,
) -> str:
    expiration = session_date + timedelta(days=dte)
    marker = "C" if side is OptionSide.CALL else "P"
    return (
        f"O:{underlying}{expiration:%y%m%d}"
        f"{marker}{strike_millis:08d}"
    )


def _candidate(
    underlying: str,
    index: int,
    *,
    dte: int = 7,
    side: OptionSide | None = None,
    prior_open_interest: int | None = 500,
    flow_eligible: bool = True,
    strike_millis: int | None = None,
    session_date: date = SESSION_DATE,
    observed_at: datetime = OBSERVED_AT,
    delta: Decimal | None | object = ...,
) -> ActivityMomentumCandidate:
    option_side = side or (
        OptionSide.CALL if index % 2 == 0 else OptionSide.PUT
    )
    resolved_strike_millis = (
        100_000 + index
        if strike_millis is None
        else strike_millis
    )
    ticker = _ticker(
        underlying,
        option_side,
        dte=dte,
        strike_millis=resolved_strike_millis,
        session_date=session_date,
    )
    return ActivityMomentumCandidate(
        contract_id=abs(hash((underlying, index, dte))) % 2_000_000 + 1,
        contract_ticker=ticker,
        underlying=underlying,
        expiration=session_date + timedelta(days=dte),
        option_side=option_side,
        strike=(
            Decimal(str(resolved_strike_millis))
            / Decimal("1000")
        ),
        multiplier=Decimal("100"),
        deliverable="STANDARD",
        catalog_version_id=10_000 + index,
        catalog_payload_sha256=canonical_sha256({
            "catalog": ticker,
        }),
        observed_at=observed_at,
        spot=Decimal("100"),
        bid=Decimal("1.00"),
        ask=Decimal("1.10"),
        quote_as_of=observed_at - timedelta(minutes=1),
        completed_session_volume=1_000 - index,
        volume_as_of=observed_at - timedelta(minutes=2),
        prior_open_interest=prior_open_interest,
        open_interest_as_of=(
            None
            if prior_open_interest is None
            else observed_at - timedelta(days=1)
        ),
        delta=(
            (
                Decimal("0.50")
                if option_side is OptionSide.CALL
                else Decimal("-0.50")
            )
            if delta is ...
            else delta
        ),
        implied_volatility=(
            None if delta is None else Decimal("0.25")
        ),
        greeks_as_of=(
            None
            if delta is None
            else observed_at - timedelta(minutes=1)
        ),
        source_evidence_references=(
            canonical_sha256({"snapshot": ticker}),
        ),
        flow_eligible=flow_eligible,
        flow_unavailable_reason=(
            None
            if flow_eligible
            else "FLOW_V1_DTE_OR_UNDERLYING_UNAVAILABLE"
        ),
    )


def _expiry_calendar(
    underlying: str,
    *,
    session_date: date,
    week_end_session_date: date,
    expiries: tuple[date, ...],
    last_trading_at: datetime,
    convention: str = "US_EQUITY_STANDARD_PM",
) -> ActivityMomentumExpiryCalendarEvidence:
    references = tuple(
        canonical_sha256({
            "underlying": underlying,
            "expiration": expiration,
            "last_trading_at": last_trading_at,
            "convention": convention,
        })
        for expiration in expiries
    )
    return ActivityMomentumExpiryCalendarEvidence(
        underlying=underlying,
        target_session_date=session_date,
        calendar_name="XNYS",
        week_end_session_date=week_end_session_date,
        product_convention=convention,
        last_trading_evidence=tuple(
            ActivityMomentumLastTradingEvidence(
                expiration=expiration,
                last_trading_session_date=(
                    session_date
                    if expiration == session_date
                    else expiration
                ),
                last_trading_at=last_trading_at,
                convention=convention,
                evidence_reference=reference,
            )
            for expiration, reference in zip(
                expiries,
                references,
            )
        ),
        evidence_references=(
            canonical_sha256({
                "calendar": "XNYS",
                "session_date": session_date,
                "week_end_session_date": week_end_session_date,
            }),
        ),
    )


def _group(
    underlying: str,
    index: int,
    *,
    dte: int = 7,
    role: str = "DIRECTIONAL_MOMENTUM",
    candidates: tuple[ActivityMomentumCandidate, ...] | None = None,
    matching_strategies: tuple[str, ...] = (),
) -> ActivityMomentumGroup:
    rows = candidates or (_candidate(underlying, index, dte=dte),)
    return ActivityMomentumGroup(
        group_id=f"{underlying}:{role}:{dte}:{index}",
        group_version="resolver-groups-v1",
        underlying=underlying,
        roles=(role,),
        candidates=rows,
        requirement_identities=(
            canonical_sha256({
                "underlying": underlying,
                "role": role,
                "dte": dte,
            }),
        ),
        matching_strategies=matching_strategies,
        target_delta=Decimal("0.50"),
        target_moneyness=Decimal("0.02"),
        target_dte=dte,
    )


def _resolve_and_allocate_v3(
    *,
    session_date: date,
    preparation_at: datetime,
    candidates,
    calendars,
    same_day_enabled: bool | None = None,
    short_delta_override: Decimal | None = None,
    required_contract_tickers=(),
    active_contract_budget: int | None = None,
):
    rows = tuple(candidates)
    profile = load_activity_momentum_profile(
        version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    resolution = resolve_activity_momentum_selection(
        profile,
        session_date=session_date,
        candidates=rows,
        required_contract_tickers=required_contract_tickers,
        preparation_at=preparation_at,
        expiry_calendars=calendars,
        same_day_enabled=same_day_enabled,
        short_delta_override=short_delta_override,
    )
    allocation = allocate_activity_momentum(
        profile,
        session_date=session_date,
        rotation_key="v3-weekly-expiry-fixture",
        ready_underlyings=tuple(sorted({
            row.underlying for row in rows
        })),
        groups=resolution.groups,
        active_contract_budget=active_contract_budget,
        resolution=resolution,
    )
    return profile, resolution, allocation


def test_profile_pins_existing_policies_and_zero_work_deferrals():
    profile = load_activity_momentum_profile(
        version=LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )

    assert profile.version == "c6_activity_momentum_v1"
    assert profile.default_connection_limit == 1
    assert profile.quote_contract_ceiling == 1000
    assert profile.active_quote_contract_budget == 900
    assert profile.quote_slot_reserve == 100
    assert profile.selection_policy_sha256 == (
        "2ed9cc0f5f3ca27ab18b8c8c748e7515ced01760d89b5b"
        "779f4add95a03130c8"
    )
    assert profile.flow_policy_sha256 == (
        "a09fa7207c4cff7e028e54ae42ff87400ea1670247906bd2b4b"
        "4c3ebbd3d2833"
    )
    assert load_activity_momentum_profile().version == (
        ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    current = load_activity_momentum_profile()
    assert current.discovery_maximum_dte(
        enabled_strategies=(),
        required_contract_tickers=(),
    ) == 21
    assert current.discovery_maximum_dte(
        enabled_strategies=("DIRECTIONAL_LONG_PREMIUM",),
        required_contract_tickers=(),
    ) == 60


def test_v3_profile_is_explicit_and_preserves_v1_v2_identities():
    legacy = load_activity_momentum_profile(
        version=LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    current = load_activity_momentum_profile(
        version=ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    successor = load_activity_momentum_profile(
        version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )

    assert load_activity_momentum_profile() == current
    assert legacy.version == "c6_activity_momentum_v1"
    assert current.version == "c6_activity_momentum_v2"
    assert successor.version == "c6_activity_momentum_v3"
    assert len({
        legacy.profile_sha256,
        current.profile_sha256,
        successor.profile_sha256,
    }) == 3
    criteria = {
        row.criterion_id: row
        for row in successor.selection_criteria
    }
    assert criteria["SHORT_ACTIVITY"].minimum_dte == 0
    assert criteria["SHORT_ACTIVITY"].maximum_dte == 5
    assert criteria["SHORT_ACTIVITY"].target_absolute_delta == Decimal(
        "0.50"
    )
    assert criteria["SHORT_ACTIVITY"].target_moneyness_fraction == 0
    assert criteria["SHORT_ACTIVITY"].maximum_moneyness_fraction == Decimal(
        "0.075"
    )
    assert criteria["SHORT_ACTIVITY"].maximum_relative_spread == Decimal(
        "0.20"
    )
    assert criteria["CORE_ACTIVITY"].minimum_dte == 6
    assert criteria["CORE_ACTIVITY"].maximum_dte == 21
    assert criteria["CORE_ACTIVITY"].target_absolute_delta == Decimal(
        "0.60"
    )
    assert criteria["STRATEGY_EXTENSION"].criteria_owner == (
        "ENABLED_STRATEGY_POLICY"
    )
    assert successor.same_day_enabled
    assert successor.short_near_expiry_contract_target == 2
    assert successor.short_end_of_week_contract_target == 2


def test_preparation_keeps_v2_default_and_requires_explicit_v3_config(
    tmp_path,
):
    base = [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--source-cohort-id",
        "11111111-2222-3333-4444-555555555555",
        "--session-date",
        "2026-11-02",
        "--authorized-until",
        "2026-11-02T13:00:00+00:00",
        "--reference-valid-until",
        "2026-11-02T20:00:00+00:00",
        "--maximum-provider-requests",
        "2",
        "--maximum-response-decoded-bytes",
        "100000",
        "--maximum-decoded-bytes",
        "200000",
        "--maximum-moneyness-fraction",
        "0.10",
        "--authorization-reference",
        "offline-fixture",
        "--capture-path",
        str(tmp_path / "capture.json"),
        "--report-path",
        str(tmp_path / "report.json"),
    ]
    current = parse_pre_session_args(base)
    successor = parse_pre_session_args([
        *base,
        "--admission-profile-version",
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
        "--short-delta-override",
        "0.45",
        "--disable-same-day",
        "--last-trading-convention",
        "QQQ=US_EQUITY_STANDARD_PM",
        "--last-trading-convention",
        "SPY=US_EQUITY_STANDARD_PM",
    ])

    assert current.admission_profile_version == (
        ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    assert successor.admission_profile_version == (
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    assert successor.short_delta_override == Decimal("0.45")
    assert successor.disable_same_day
    assert successor.last_trading_conventions == {
        "QQQ": "US_EQUITY_STANDARD_PM",
        "SPY": "US_EQUITY_STANDARD_PM",
    }
    with pytest.raises(SystemExit):
        parse_pre_session_args([
            *base,
            "--short-delta-override",
            "0.45",
        ])


@pytest.mark.parametrize(
    (
        "session_date",
        "preparation_at",
        "friday",
        "expected_dte",
    ),
    (
        (
            date(2026, 11, 2),
            datetime(2026, 11, 1, 23, 0, tzinfo=UTC),
            date(2026, 11, 6),
            4,
        ),
        (
            date(2026, 11, 4),
            datetime(2026, 11, 4, 12, 0, tzinfo=UTC),
            date(2026, 11, 6),
            2,
        ),
        (
            date(2026, 11, 6),
            datetime(2026, 11, 6, 12, 0, tzinfo=UTC),
            date(2026, 11, 6),
            0,
        ),
    ),
)
def test_v3_friday_only_chain_uses_one_listed_expiry_for_four_slots(
    session_date,
    preparation_at,
    friday,
    expected_dte,
):
    rows = tuple(
        _candidate(
            "ACME",
            index,
            dte=expected_dte,
            side=(
                OptionSide.CALL
                if index < 2
                else OptionSide.PUT
            ),
            session_date=session_date,
            observed_at=preparation_at,
        )
        for index in range(4)
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=friday,
        expiries=(friday,),
        last_trading_at=datetime(
            2026,
            11,
            6,
            21,
            0,
            tzinfo=UTC,
        ),
    )

    _, resolution, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=reversed(rows),
        calendars=(calendar,),
    )

    evidence = allocation.short_expiry_evidence[0]
    assert resolution.short_expiry_evidence[0].single_expiry
    assert evidence.near_expiry == friday
    assert evidence.end_of_week_expiry == friday
    assert evidence.near_end_of_week_coincide
    assert len(allocation.selected_contract_tickers) == 4
    assert {
        expiration for expiration, _side, _count
        in evidence.selected_counts
    } == {friday}
    assert {
        side for _expiration, side, _count
        in evidence.selected_counts
    } == {OptionSide.CALL, OptionSide.PUT}


def test_v3_daily_plus_weekly_chain_softly_allocates_two_plus_two():
    session_date = date(2026, 11, 2)
    preparation_at = datetime(
        2026,
        11,
        1,
        23,
        0,
        tzinfo=UTC,
    )
    wednesday = date(2026, 11, 4)
    friday = date(2026, 11, 6)
    rows = tuple(
        _candidate(
            "ACME",
            index,
            dte=dte,
            side=side,
            session_date=session_date,
            observed_at=preparation_at,
        )
        for index, (dte, side) in enumerate((
            (2, OptionSide.CALL),
            (2, OptionSide.PUT),
            (2, OptionSide.CALL),
            (2, OptionSide.PUT),
            (4, OptionSide.CALL),
            (4, OptionSide.PUT),
            (4, OptionSide.CALL),
            (4, OptionSide.PUT),
        ))
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=friday,
        expiries=(wednesday, friday),
        last_trading_at=datetime(
            2026,
            11,
            6,
            21,
            0,
            tzinfo=UTC,
        ),
    )

    _, _, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=rows,
        calendars=(calendar,),
    )

    evidence = allocation.short_expiry_evidence[0]
    assert evidence.resolution_reason == (
        "DISTINCT_NEAR_AND_END_OF_WEEK"
    )
    assert evidence.near_expiry == wednesday
    assert evidence.end_of_week_expiry == friday
    assert len(allocation.selected_contract_tickers) == 4
    selected_by_expiry = Counter({
        expiration: sum(
            count
            for row_expiration, _side, count
            in evidence.selected_counts
            if row_expiration == expiration
        )
        for expiration in (wednesday, friday)
    })
    assert selected_by_expiry == Counter({
        wednesday: 2,
        friday: 2,
    })
    assert not evidence.shortfall_reasons


@pytest.mark.parametrize(
    (
        "session_date",
        "week_end_session",
        "near_dte",
        "end_dte",
        "coincident",
    ),
    (
        (
            date(2026, 3, 30),
            date(2026, 4, 2),
            2,
            3,
            False,
        ),
        (
            date(2026, 11, 18),
            date(2026, 11, 20),
            2,
            2,
            True,
        ),
    ),
)
def test_v3_resolves_holiday_adjusted_and_coincident_monthly_eow(
    session_date,
    week_end_session,
    near_dte,
    end_dte,
    coincident,
):
    preparation_at = datetime.combine(
        session_date,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=12)
    dtes = (
        (near_dte,) * 4
        if coincident
        else (near_dte, near_dte, end_dte, end_dte)
    )
    rows = tuple(
        _candidate(
            "ACME",
            index,
            dte=dte,
            side=(
                OptionSide.CALL
                if index % 2 == 0
                else OptionSide.PUT
            ),
            session_date=session_date,
            observed_at=preparation_at,
        )
        for index, dte in enumerate(dtes)
    )
    expiries = tuple(sorted({
        row.expiration for row in rows
    }))
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=week_end_session,
        expiries=expiries,
        last_trading_at=preparation_at + timedelta(days=4),
    )

    _, _, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=rows,
        calendars=(calendar,),
    )

    evidence = allocation.short_expiry_evidence[0]
    assert evidence.week_end_session_date == week_end_session
    assert evidence.end_of_week_expiry == week_end_session
    assert evidence.near_end_of_week_coincide is coincident
    assert len(allocation.selected_contract_tickers) == 4


def test_v3_absent_weekly_and_six_seven_dte_remain_core_only():
    session_date = date(2026, 11, 2)
    preparation_at = datetime(
        2026,
        11,
        2,
        12,
        0,
        tzinfo=UTC,
    )
    rows = tuple(
        _candidate(
            "ACME",
            index,
            dte=dte,
            session_date=session_date,
            observed_at=preparation_at,
        )
        for index, dte in enumerate((6, 7, 14, 14))
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=date(2026, 11, 6),
        expiries=(),
        last_trading_at=preparation_at,
    )

    _, resolution, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=rows,
        calendars=(calendar,),
    )

    evidence = allocation.short_expiry_evidence[0]
    assert evidence.near_expiry is None
    assert evidence.end_of_week_expiry is None
    assert "LISTED_SHORT_EXPIRY_UNAVAILABLE" in (
        evidence.shortfall_reasons
    )
    assert all(
        "SHORT_DATED_SWEEP_LIKE" not in group.roles
        for group in resolution.groups
    )
    assert all(
        min(
            row.dte_for(session_date)
            for row in group.candidates
        )
        >= 6
        for group in allocation.selected_groups
    )


@pytest.mark.parametrize(
    (
        "preparation_at",
        "same_day_enabled",
        "convention",
        "expected_selected",
        "expected_rejection",
    ),
    (
        (
            datetime(2026, 11, 6, 15, 0, tzinfo=UTC),
            True,
            "US_EQUITY_STANDARD_PM",
            True,
            None,
        ),
        (
            datetime(2026, 11, 6, 22, 0, tzinfo=UTC),
            True,
            "US_EQUITY_STANDARD_PM",
            False,
            "SAME_DAY_LAST_TRADING_PASSED",
        ),
        (
            datetime(2026, 11, 6, 15, 0, tzinfo=UTC),
            False,
            "US_EQUITY_STANDARD_PM",
            False,
            "SAME_DAY_DISABLED",
        ),
        (
            datetime(2026, 11, 6, 15, 0, tzinfo=UTC),
            True,
            "UNSUPPORTED_INDEX_AM",
            False,
            "SHORT_EXPIRY_PRODUCT_CONVENTION_UNSUPPORTED",
        ),
    ),
)
def test_v3_same_day_uses_explicit_last_trading_evidence(
    preparation_at,
    same_day_enabled,
    convention,
    expected_selected,
    expected_rejection,
):
    session_date = date(2026, 11, 6)
    rows = tuple(
        _candidate(
            "ACME",
            index,
            dte=0,
            session_date=session_date,
            observed_at=preparation_at,
        )
        for index in range(4)
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=session_date,
        expiries=(session_date,),
        last_trading_at=datetime(
            2026,
            11,
            6,
            21,
            0,
            tzinfo=UTC,
        ),
        convention=convention,
    )

    _, resolution, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=rows,
        calendars=(calendar,),
        same_day_enabled=same_day_enabled,
    )

    assert bool(allocation.selected_contract_tickers) is expected_selected
    if expected_rejection is not None:
        assert dict(resolution.rejection_counts)[
            expected_rejection
        ] == 4
        assert "ELIGIBLE_SHORT_EXPIRY_UNAVAILABLE" in (
            allocation.short_expiry_evidence[0].shortfall_reasons
        )


def test_v3_sparse_quality_missing_greeks_and_oi_are_persisted():
    session_date = date(2026, 11, 2)
    preparation_at = datetime(
        2026,
        11,
        2,
        12,
        0,
        tzinfo=UTC,
    )
    wednesday = date(2026, 11, 4)
    friday = date(2026, 11, 6)
    fallback = _candidate(
        "ACME",
        0,
        dte=2,
        side=OptionSide.CALL,
        prior_open_interest=None,
        session_date=session_date,
        observed_at=preparation_at,
        delta=None,
    )
    zero_volume = replace(
        _candidate(
            "ACME",
            1,
            dte=4,
            side=OptionSide.PUT,
            session_date=session_date,
            observed_at=preparation_at,
        ),
        completed_session_volume=0,
    )
    wide = replace(
        _candidate(
            "ACME",
            2,
            dte=4,
            session_date=session_date,
            observed_at=preparation_at,
        ),
        bid=Decimal("0.85"),
        ask=Decimal("1.15"),
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=friday,
        expiries=(wednesday, friday),
        last_trading_at=datetime(
            2026,
            11,
            6,
            21,
            0,
            tzinfo=UTC,
        ),
    )

    _, resolution, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=(wide, zero_volume, fallback),
        calendars=(calendar,),
    )

    assert dict(resolution.rejection_counts)[
        "ROLE_SPREAD_LIMIT"
    ] == 1
    assert set(allocation.selected_contract_tickers) == {
        fallback.contract_ticker,
        zero_volume.contract_ticker,
    }
    evidence = allocation.short_expiry_evidence[0]
    assert evidence.fallback_contract_count == 1
    assert evidence.ranking_anchor_contract_count == 1
    assert "SHORT_BASKET_SELECTION_SHORTFALL" in (
        evidence.shortfall_reasons
    )
    assert any(
        row.missing_oi_contract_count == 1
        for row in allocation.coverage
    )


def test_v3_required_group_replay_override_and_payload_are_exact():
    session_date = date(2026, 11, 2)
    preparation_at = datetime(
        2026,
        11,
        2,
        12,
        0,
        tzinfo=UTC,
    )
    friday = date(2026, 11, 6)
    short_rows = tuple(
        _candidate(
            "ACME",
            index,
            dte=dte,
            side=(
                OptionSide.CALL
                if index % 2 == 0
                else OptionSide.PUT
            ),
            session_date=session_date,
            observed_at=preparation_at,
        )
        for index, dte in enumerate((2, 2, 4, 4, 2, 2, 4, 4))
    )
    required = _candidate(
        "ACME",
        900,
        dte=30,
        side=OptionSide.CALL,
        session_date=session_date,
        observed_at=preparation_at,
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=friday,
        expiries=(
            session_date + timedelta(days=2),
            friday,
        ),
        last_trading_at=datetime(
            2026,
            11,
            6,
            21,
            0,
            tzinfo=UTC,
        ),
    )

    _, first_resolution, first = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=(*short_rows, required),
        calendars=(calendar,),
        required_contract_tickers=(required.contract_ticker,),
        short_delta_override=Decimal("0.45"),
    )
    _, replay_resolution, replay = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=reversed((*short_rows, required)),
        calendars=(calendar,),
        required_contract_tickers=(required.contract_ticker,),
        short_delta_override=Decimal("0.45"),
    )

    assert first_resolution == replay_resolution
    assert first.allocation_sha256 == replay.allocation_sha256
    assert required.contract_ticker in first.selected_contract_tickers
    assert first.short_delta_override == Decimal("0.45")
    assert first.short_expiry_evidence[
        0
    ].displaced_coverage_contract_count == 1
    payload = activity_momentum_allocation_payload(first)
    assert activity_momentum_allocation_from_payload(payload) == first
    assert payload["profile_version"] == (
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    assert payload["short_delta_override"] == "0.45"


@pytest.mark.parametrize(
    ("underlying_count", "expected_budget"),
    ((10, 80), (25, 200), (50, 400)),
)
def test_v3_preserves_default_total_and_per_name_bounds(
    underlying_count,
    expected_budget,
):
    profile = load_activity_momentum_profile(
        version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    session_date = SESSION_DATE
    underlyings = _symbols(underlying_count)
    rows = tuple(
        _candidate(
            underlying,
            index,
            dte=(2 if index < 4 else 14),
            session_date=session_date,
            observed_at=OBSERVED_AT,
        )
        for underlying in underlyings
        for index in range(8)
    )
    calendars = tuple(
        _expiry_calendar(
            underlying,
            session_date=session_date,
            week_end_session_date=date(2026, 11, 6),
            expiries=(session_date + timedelta(days=2),),
            last_trading_at=OBSERVED_AT,
        )
        for underlying in underlyings
    )
    resolution = resolve_activity_momentum_selection(
        profile,
        session_date=session_date,
        candidates=reversed(rows),
        preparation_at=OBSERVED_AT,
        expiry_calendars=calendars,
    )
    allocation = allocate_activity_momentum(
        profile,
        session_date=session_date,
        rotation_key="v3-default-bounds",
        ready_underlyings=underlyings,
        groups=resolution.groups,
        resolution=resolution,
    )

    assert allocation.active_contract_budget == expected_budget
    assert len(allocation.selected_contract_tickers) == expected_budget
    assert all(
        len({
            ticker
            for group in allocation.selected_groups
            if group.underlying == underlying
            for ticker in group.contract_tickers
        })
        == 8
        for underlying in underlyings
    )


def test_v3_unsupported_same_day_does_not_block_other_underlying():
    session_date = date(2026, 11, 6)
    preparation_at = datetime(
        2026,
        11,
        6,
        15,
        0,
        tzinfo=UTC,
    )
    rows = (
        _candidate(
            "ACME",
            0,
            dte=0,
            session_date=session_date,
            observed_at=preparation_at,
        ),
        _candidate(
            "BETA",
            0,
            dte=3,
            session_date=session_date,
            observed_at=preparation_at,
        ),
    )
    calendars = (
        _expiry_calendar(
            "ACME",
            session_date=session_date,
            week_end_session_date=session_date,
            expiries=(session_date,),
            last_trading_at=preparation_at + timedelta(hours=6),
            convention="UNSUPPORTED_INDEX_AM",
        ),
        _expiry_calendar(
            "BETA",
            session_date=session_date,
            week_end_session_date=session_date,
            expiries=(session_date + timedelta(days=3),),
            last_trading_at=preparation_at + timedelta(days=3),
        ),
    )

    _, resolution, allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=rows,
        calendars=calendars,
    )

    assert dict(resolution.rejection_counts)[
        "SHORT_EXPIRY_PRODUCT_CONVENTION_UNSUPPORTED"
    ] == 1
    assert allocation.selected_contract_tickers == (
        rows[1].contract_ticker,
    )


def test_v2_v3_fixture_comparison_is_offline_and_deterministic():
    session_date = date(2026, 11, 2)
    preparation_at = datetime(
        2026,
        11,
        2,
        12,
        0,
        tzinfo=UTC,
    )
    friday = date(2026, 11, 6)
    good = tuple(
        _candidate(
            "ACME",
            index,
            dte=dte,
            session_date=session_date,
            observed_at=preparation_at,
            delta=(None if index == 0 else ...),
        )
        for index, dte in enumerate((2, 2, 4, 4))
    )
    wide = replace(
        _candidate(
            "ACME",
            10,
            dte=2,
            session_date=session_date,
            observed_at=preparation_at,
        ),
        bid=Decimal("0.85"),
        ask=Decimal("1.15"),
    )
    far = _candidate(
        "ACME",
        11,
        dte=4,
        strike_millis=108_000,
        session_date=session_date,
        observed_at=preparation_at,
    )
    rows = (*good, wide, far)
    v2_profile = load_activity_momentum_profile(
        version=ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    v2_resolution = resolve_activity_momentum_selection(
        v2_profile,
        session_date=session_date,
        candidates=rows,
    )
    v2_allocation = allocate_activity_momentum(
        v2_profile,
        session_date=session_date,
        rotation_key="v2-v3-comparison",
        ready_underlyings=("ACME",),
        groups=v2_resolution.groups,
        resolution=v2_resolution,
    )
    calendar = _expiry_calendar(
        "ACME",
        session_date=session_date,
        week_end_session_date=friday,
        expiries=(
            session_date + timedelta(days=2),
            friday,
        ),
        last_trading_at=datetime(
            2026,
            11,
            6,
            21,
            0,
            tzinfo=UTC,
        ),
    )
    _, v3_resolution, v3_allocation = _resolve_and_allocate_v3(
        session_date=session_date,
        preparation_at=preparation_at,
        candidates=reversed(rows),
        calendars=(calendar,),
    )

    first = compare_activity_momentum_profiles(
        session_date=session_date,
        candidates=rows,
        v2_resolution=v2_resolution,
        v2_allocation=v2_allocation,
        v3_resolution=v3_resolution,
        v3_allocation=v3_allocation,
    )
    replay = compare_activity_momentum_profiles(
        session_date=session_date,
        candidates=reversed(rows),
        v2_resolution=v2_resolution,
        v2_allocation=v2_allocation,
        v3_resolution=v3_resolution,
        v3_allocation=v3_allocation,
    )

    assert first == replay
    assert first["evidence_class"] == (
        "OFFLINE_CALIBRATION_NOT_PRODUCTION_CAPACITY_EVIDENCE"
    )
    assert not first["production_superiority_claimed"]
    assert not first["safe_capacity_claimed"]
    assert first["v2"]["preparation_work_counts"][
        "filtered"
    ] == 6
    assert first["v3"]["preparation_work_counts"][
        "filtered"
    ] == 4
    assert first["v3"]["fallback_contract_count"] == 1
    assert sum(
        row["count"]
        for row in first["v2"][
            "accepted_by_role_side_expiry"
        ]
    ) == 6
    assert sum(
        row["count"]
        for row in first["v3"][
            "accepted_by_role_side_expiry"
        ]
    ) == 4
    assert sum(
        row["count"]
        for row in first["v3"][
            "rejected_by_role_side_expiry"
        ]
    ) == 2
    assert first[
        "routed_event_invocation_unit_difference"
    ] == -10
    assert {
        row["reason"]
        for row in first["v3"]["rejected_by_reason"]
    } >= {"ROLE_SPREAD_LIMIT", "ROLE_STRIKE_DISTANCE_LIMIT"}


@pytest.mark.parametrize(
    ("underlying_count", "candidates_per_underlying"),
    ((50, 20), (100, 10)),
)
def test_fair_share_uses_900_slots_without_provider_ordering(
    underlying_count,
    candidates_per_underlying,
):
    profile = load_activity_momentum_profile(
        version=LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    underlyings = _symbols(underlying_count)
    groups = tuple(
        _group(underlying, index)
        for underlying in underlyings
        for index in range(candidates_per_underlying)
    )

    first = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="desired-revision-1",
        ready_underlyings=underlyings,
        groups=groups,
    )
    replay = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="desired-revision-1",
        ready_underlyings=reversed(underlyings),
        groups=reversed(groups),
    )

    expected_share = 18 if underlying_count == 50 else 9
    assert len(first.selected_contract_tickers) == 900
    assert first.initial_share == expected_share
    assert first.allocation_sha256 == replay.allocation_sha256
    assert (
        first.selected_contract_tickers
        == replay.selected_contract_tickers
    )
    assert activity_momentum_allocation_from_payload(
        activity_momentum_allocation_payload(first)
    ) == first
    selected_by_underlying = {
        underlying: sum(
            row.underlying == underlying
            for group in first.selected_groups
            for row in group.candidates
        )
        for underlying in underlyings
    }
    assert set(selected_by_underlying.values()) == {expected_share}


@pytest.mark.parametrize(
    ("underlying_count", "expected_budget"),
    ((10, 80), (25, 200), (50, 400)),
)
def test_v2_default_budget_is_eight_per_eligible_name(
    underlying_count,
    expected_budget,
):
    profile = load_activity_momentum_profile()
    underlyings = _symbols(underlying_count)
    groups = tuple(
        _group(underlying, index)
        for underlying in underlyings
        for index in range(12)
    )

    first = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-default-budget",
        ready_underlyings=underlyings,
        groups=groups,
    )
    replay = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-default-budget",
        ready_underlyings=reversed(underlyings),
        groups=reversed(groups),
    )

    assert first.active_contract_budget == expected_budget
    assert len(first.selected_contract_tickers) == expected_budget
    assert first.allocation_sha256 == replay.allocation_sha256
    assert first.admission_evidence is not None
    assert (
        first.admission_evidence.configured_contracts_per_underlying
        == 8
    )
    assert first.admission_evidence.effective_per_underlying_limit == 12
    assert not first.admission_evidence.expansion_enabled


@pytest.mark.parametrize(
    ("underlying_count", "expected_budget"),
    ((10, 120), (25, 300), (50, 600)),
)
def test_v2_explicit_expansion_reaches_twelve_per_name(
    underlying_count,
    expected_budget,
):
    profile = load_activity_momentum_profile()
    underlyings = _symbols(underlying_count)
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-expanded-budget",
        ready_underlyings=underlyings,
        groups=tuple(
            _group(underlying, index)
            for underlying in underlyings
            for index in range(12)
        ),
        per_underlying_contract_limit=12,
    )

    assert allocation.active_contract_budget == expected_budget
    assert len(allocation.selected_contract_tickers) == expected_budget
    assert allocation.admission_evidence is not None
    assert allocation.admission_evidence.expansion_enabled


def test_v2_smaller_configured_total_and_redistribution_are_bounded():
    profile = load_activity_momentum_profile()
    underlyings = _symbols(10)
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-small-total",
        ready_underlyings=underlyings,
        groups=tuple(
            _group(underlying, index)
            for underlying in underlyings
            for index in range(12)
        ),
        active_contract_budget=37,
    )

    assert allocation.active_contract_budget == 37
    assert len(allocation.selected_contract_tickers) == 37
    selected_counts = {
        underlying: len({
            ticker
            for group in allocation.selected_groups
            if group.underlying == underlying
            for ticker in group.contract_tickers
        })
        for underlying in underlyings
    }
    assert max(selected_counts.values()) <= 12

    sparse = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-sparse-redistribution",
        ready_underlyings=("A", "B"),
        groups=(
            _group("A", 0),
            *tuple(_group("B", index) for index in range(20)),
        ),
    )
    sparse_counts = {
        underlying: len({
            ticker
            for group in sparse.selected_groups
            if group.underlying == underlying
            for ticker in group.contract_tickers
        })
        for underlying in ("A", "B")
    }
    assert sparse.active_contract_budget == 16
    assert sparse_counts == {"A": 1, "B": 12}
    assert len(sparse.selected_contract_tickers) == 13


def test_v2_budget_smaller_than_eligible_names_never_overallocates():
    profile = load_activity_momentum_profile()
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-budget-smaller-than-n",
        ready_underlyings=("A", "B", "C"),
        groups=tuple(
            _group(underlying, 0)
            for underlying in ("A", "B", "C")
        ),
        active_contract_budget=2,
    )

    assert allocation.active_contract_budget == 2
    assert len(allocation.selected_contract_tickers) == 2
    assert len({
        group.underlying for group in allocation.selected_groups
    }) == 2


def test_v2_no_eligible_names_persists_zero_budget_coverage():
    profile = load_activity_momentum_profile()
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-no-eligible",
        ready_underlyings=("A", "B"),
        groups=(),
    )

    assert allocation.active_contract_budget == 0
    assert allocation.eligible_underlyings == ()
    assert allocation.selected_contract_tickers == ()
    assert {
        row.state for row in allocation.coverage
    } == {CoverageState.UNCOVERED}
    assert all(
        "ROLE_HAS_NO_ELIGIBLE_GROUP" in row.reason_codes
        for row in allocation.coverage
    )
    assert activity_momentum_allocation_from_payload(
        activity_momentum_allocation_payload(allocation)
    ) == allocation


def test_v2_sparse_quality_does_not_pad_and_optional_inputs_do_not_block():
    profile = load_activity_momentum_profile()
    rows = (
        replace(
            _candidate("SPY", 0, dte=2),
            completed_session_volume=0,
        ),
        replace(
            _candidate(
                "SPY",
                1,
                dte=3,
                prior_open_interest=None,
            ),
            delta=None,
            implied_volatility=None,
            greeks_as_of=None,
        ),
        _candidate("SPY", 2, dte=5),
    )
    resolution = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=reversed(rows),
    )
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-sparse-quality",
        ready_underlyings=("SPY",),
        groups=resolution.groups,
        resolution=resolution,
    )

    assert resolution.discovered_count == 3
    assert resolution.filtered_count == 3
    assert allocation.active_contract_budget == 8
    assert len(allocation.selected_contract_tickers) == 3
    assert set(allocation.selected_contract_tickers) == {
        row.contract_ticker for row in rows
    }
    assert allocation.admission_evidence is not None
    assert allocation.admission_evidence.admitted_contract_count == 3


def test_v2_role_expiry_target_is_not_candidate_own_dte():
    profile = load_activity_momentum_profile()
    near_start = replace(
        _candidate(
            "SPY",
            0,
            dte=6,
            side=OptionSide.CALL,
        ),
        completed_session_volume=100,
    )
    target = replace(
        _candidate(
            "SPY",
            1,
            dte=14,
            side=OptionSide.CALL,
        ),
        completed_session_volume=100,
    )
    groups = resolve_activity_momentum_groups(
        profile,
        session_date=SESSION_DATE,
        candidates=(near_start, target),
    )
    singles = tuple(
        row for row in groups if len(row.candidates) == 1
    )
    assert {row.target_dte for row in singles} == {14}

    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="v2-role-expiry-target",
        ready_underlyings=("SPY",),
        groups=groups,
        active_contract_budget=1,
    )
    assert allocation.selected_contract_tickers == (
        target.contract_ticker,
    )


def test_v2_candidate_shortlist_and_group_enumeration_are_bounded():
    profile = load_activity_momentum_profile()
    rows = tuple(
        _candidate(
            "SPY",
            index,
            dte=7 + index % 10,
            strike_millis=75_000 + index * 500,
        )
        for index in range(180)
    )

    resolution = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=reversed(rows),
        enabled_strategies=(
            "DIRECTIONAL_DEBIT_SPREAD",
            "VOLATILITY_SMILE_DISTORTION",
        ),
    )

    assert resolution.discovered_count == 180
    assert resolution.shortlisted_count <= (
        profile.maximum_shortlisted_candidates_per_underlying
    )
    assert resolution.group_enumeration_count <= (
        profile.maximum_complete_groups_per_underlying
    )
    assert dict(resolution.rejection_counts)
    assert resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=rows,
        enabled_strategies=(
            "DIRECTIONAL_DEBIT_SPREAD",
            "VOLATILITY_SMILE_DISTORTION",
        ),
    ) == resolution


def test_v2_strategy_extension_requires_explicit_strategy_or_group():
    profile = load_activity_momentum_profile()
    candidate = _candidate("SPY", 0, dte=30)

    default = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=(candidate,),
    )
    enabled = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=(candidate,),
        enabled_strategies=("DIRECTIONAL_LONG_PREMIUM",),
    )

    assert default.shortlisted_candidates == ()
    assert dict(default.rejection_counts) == {
        "STRATEGY_EXTENSION_DISABLED": 1,
    }
    assert enabled.shortlisted_candidates == (candidate,)
    assert any(
        row.matching_strategies
        == ("DIRECTIONAL_LONG_PREMIUM",)
        for row in enabled.groups
    )


def test_allocation_converts_to_c6_members_and_normalized_coverage():
    profile = load_activity_momentum_profile()
    candidate = _candidate(
        "XYZ",
        3,
        dte=4,
        flow_eligible=False,
    )
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="c6-conversion",
        ready_underlyings=("XYZ",),
        groups=(
            _group(
                "XYZ",
                3,
                dte=4,
                role="DIRECTIONAL_MOMENTUM",
                candidates=(candidate,),
                matching_strategies=(
                    "DIRECTIONAL_LONG_PREMIUM",
                ),
            ),
        ),
        active_contract_budget=1,
    )

    members = c6_members_from_activity_allocation(allocation)
    coverage = c6_coverage_from_activity_allocation(allocation)

    assert len(members) == 1
    assert not members[0].flow_eligible
    assert members[0].contract_roles == (
        "DIRECTIONAL_MOMENTUM",
    )
    assert members[0].matching_strategies == (
        "DIRECTIONAL_LONG_PREMIUM",
    )
    assert coverage[0].underlying == "XYZ"
    assert coverage[0].state == "FULL"


def test_activity_allocation_builds_restart_stable_c5_plan():
    profile = load_activity_momentum_profile()
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="c5-plan",
        ready_underlyings=("QQQ", "SPY"),
        groups=tuple(
            _group(underlying, index, dte=7)
            for underlying in ("QQQ", "SPY")
            for index in range(2)
        ),
        active_contract_budget=4,
    )
    members = c6_members_from_activity_allocation(allocation)
    owner_id = UUID("11111111-2222-3333-4444-555555555555")
    journal_generation = UUID(
        "22222222-3333-4444-5555-666666666666"
    )
    plan = build_c6_selection_plan(
        session_date=SESSION_DATE,
        desired_revision_id=UUID(
            "33333333-4444-5555-6666-777777777777"
        ),
        desired_snapshot_sha256="1" * 64,
        selection_policy=(
            load_advanced_options_selection_c6_policy()
        ),
        members=members,
        readiness=(
            C6UnderlyingReadiness(
                underlying="QQQ",
                catalog=C6ReadinessState.AVAILABLE,
                open_interest=C6ReadinessState.AVAILABLE,
                history=C6ReadinessState.AVAILABLE,
                feed_states=(
                    (
                        "OPTION_QUOTE",
                        C6ReadinessState.AVAILABLE,
                    ),
                    (
                        "OPTION_TRADE",
                        C6ReadinessState.AVAILABLE,
                    ),
                ),
                reason_codes=(),
                observed_at=OBSERVED_AT,
                source_as_of=OBSERVED_AT,
            ),
            C6UnderlyingReadiness(
                underlying="SPY",
                catalog=C6ReadinessState.AVAILABLE,
                open_interest=C6ReadinessState.AVAILABLE,
                history=C6ReadinessState.AVAILABLE,
                feed_states=(
                    (
                        "OPTION_QUOTE",
                        C6ReadinessState.AVAILABLE,
                    ),
                    (
                        "OPTION_TRADE",
                        C6ReadinessState.AVAILABLE,
                    ),
                ),
                reason_codes=(),
                observed_at=OBSERVED_AT,
                source_as_of=OBSERVED_AT,
            ),
        ),
        capacity=C6ConnectionCapacity(
            account_alias="MASSIVE_ADVANCED",
            provider_cluster="OPTIONS",
            configured_cluster_connection_limit=1,
            provider_cluster_connection_limit=1,
            active_cluster_connections=0,
            proposed_physical_connections=1,
            quote_contract_ceiling=1000,
            active_quote_contract_budget=4,
            quote_slot_reserve=996,
            maximum_contracts_per_logical_shard=2,
        ),
        strategy_policies=(
            StrategyPolicyIdentity("fixture-v1", "2" * 64),
        ),
        catalog_identity="catalog-fixture",
        reference_identity="reference-fixture",
        source_as_of=OBSERVED_AT,
        admission_profile_version=profile.version,
        admission_profile_sha256=profile.profile_sha256,
        allocation_document=(
            activity_momentum_allocation_payload(allocation)
        ),
        allocation_coverage=(
            c6_coverage_from_activity_allocation(allocation)
        ),
        physical_owner_id=owner_id,
        journal_generation=journal_generation,
    )
    replayed = c6_selection_plan_from_payload(
        c6_selection_plan_payload(plan)
    )

    assert replayed == plan
    assert len(plan.shards) == 2
    assert plan.physical_subscription.owner_id == owner_id
    assert (
        plan.physical_subscription.journal_generation
        == journal_generation
    )
    assert {
        row.physical_subscription_id for row in plan.shards
    } == {plan.physical_subscription.subscription_id}


def test_strategy_resolver_builds_complete_debit_spread_group():
    profile = load_activity_momentum_profile()
    lower = _candidate(
        "SPY",
        0,
        dte=7,
        side=OptionSide.CALL,
        strike_millis=100_000,
    )
    upper = _candidate(
        "SPY",
        1,
        dte=7,
        side=OptionSide.CALL,
        strike_millis=101_000,
    )

    groups = resolve_activity_momentum_groups(
        profile,
        session_date=SESSION_DATE,
        candidates=(upper, lower),
        enabled_strategies=("DIRECTIONAL_DEBIT_SPREAD",),
    )
    spread = next(
        row
        for row in groups
        if row.matching_strategies
        == ("DIRECTIONAL_DEBIT_SPREAD",)
    )

    assert spread.contract_tickers == (
        lower.contract_ticker,
        upper.contract_ticker,
    )
    assert dict(spread.contract_roles) == {
        lower.contract_ticker: ("LONG_ANCHOR",),
        upper.contract_ticker: ("SHORT_WING",),
    }


def test_strategy_resolver_keeps_explicit_legs_atomic():
    profile = load_activity_momentum_profile()
    rows = tuple(
        _candidate("SPY", index, dte=30)
        for index in range(3)
    )
    groups = resolve_activity_momentum_groups(
        profile,
        session_date=SESSION_DATE,
        candidates=rows,
        required_contract_tickers=tuple(
            row.contract_ticker for row in rows
        ),
    )
    required = next(
        row
        for row in groups
        if {
            role
            for _, roles in row.contract_roles
            for role in roles
        }
        == {"REQUIRED_LEG"}
    )

    assert set(required.contract_tickers) == {
        row.contract_ticker for row in rows
    }


def test_low_ranked_required_leg_survives_every_shortlist_bound():
    profile = load_activity_momentum_profile()
    ordinary = tuple(
        _candidate(
            "SPY",
            index,
            dte=7,
            side=OptionSide.CALL,
            strike_millis=92_000 + index * 1_000,
        )
        for index in range(17)
    )
    required = _candidate(
        "SPY",
        900,
        dte=7,
        side=OptionSide.CALL,
        strike_millis=109_000,
    )

    resolution = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=(*ordinary, required),
        required_contract_tickers=(required.contract_ticker,),
    )
    required_group = next(
        group
        for group in resolution.groups
        if any(
            "REQUIRED_LEG" in roles
            for _, roles in group.contract_roles
        )
    )

    assert len(resolution.shortlisted_candidates) == 16
    assert required in resolution.shortlisted_candidates
    assert required_group.contract_tickers == (
        required.contract_ticker,
    )


def test_enabled_complete_group_protects_low_ranked_dependency_leg():
    profile = load_activity_momentum_profile()
    ordinary = tuple(
        _candidate(
            "SPY",
            index,
            dte=dte,
            side=OptionSide.CALL,
            strike_millis=100_000,
        )
        for index, dte in enumerate(
            value for value in range(22, 46) if value != 30
        )
    )
    lower = _candidate(
        "SPY",
        900,
        dte=30,
        side=OptionSide.CALL,
        strike_millis=100_000,
    )
    upper = _candidate(
        "SPY",
        901,
        dte=30,
        side=OptionSide.CALL,
        strike_millis=101_000,
    )

    first = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=(*ordinary, upper, lower),
        enabled_strategies=("DIRECTIONAL_DEBIT_SPREAD",),
    )
    replay = resolve_activity_momentum_selection(
        profile,
        session_date=SESSION_DATE,
        candidates=reversed((*ordinary, upper, lower)),
        enabled_strategies=("DIRECTIONAL_DEBIT_SPREAD",),
    )
    spread = next(
        group
        for group in first.groups
        if group.matching_strategies
        == ("DIRECTIONAL_DEBIT_SPREAD",)
    )

    assert first == replay
    assert len(first.shortlisted_candidates) == 24
    assert {lower, upper}.issubset(first.shortlisted_candidates)
    assert set(spread.contract_tickers) == {
        lower.contract_ticker,
        upper.contract_ticker,
    }


def test_required_group_over_shortlist_bound_fails_as_a_whole():
    profile = load_activity_momentum_profile()
    rows = tuple(
        _candidate(
            "SPY",
            index,
            dte=30,
            side=OptionSide.CALL,
            strike_millis=88_000 + index * 1_000,
        )
        for index in range(25)
    )

    with pytest.raises(
        ValueError,
        match="required strategy group exceeds shortlist bound",
    ):
        resolve_activity_momentum_selection(
            profile,
            session_date=SESSION_DATE,
            candidates=rows,
            required_contract_tickers=tuple(
                row.contract_ticker for row in rows
            ),
        )


def test_strategy_resolver_builds_smile_expiry_neighborhood():
    profile = load_activity_momentum_profile()
    rows = tuple(
        _candidate(
            "SPY",
            index,
            dte=14,
            side=OptionSide.CALL,
            strike_millis=97_000 + index * 1_000,
        )
        for index in range(7)
    )
    groups = resolve_activity_momentum_groups(
        profile,
        session_date=SESSION_DATE,
        candidates=reversed(rows),
        enabled_strategies=("VOLATILITY_SMILE_DISTORTION",),
    )
    smile = next(
        row
        for row in groups
        if row.matching_strategies
        == ("VOLATILITY_SMILE_DISTORTION",)
    )

    assert len(smile.candidates) == 7
    assert {
        role
        for _, roles in smile.contract_roles
        for role in roles
    } == {"SURFACE_NEIGHBOR"}


def test_unused_share_redistributes_in_deterministic_rounds():
    profile = load_activity_momentum_profile()
    groups = (
        _group("A", 0),
        *tuple(_group("B", index) for index in range(5)),
    )

    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="redistribution",
        ready_underlyings=("A", "B"),
        groups=groups,
        active_contract_budget=6,
    )

    assert len(allocation.selected_contract_tickers) == 6
    assert {
        group.underlying for group in allocation.selected_groups
    } == {"A", "B"}


def test_short_dated_share_preserves_call_priority_and_put_coverage():
    profile = load_activity_momentum_profile()
    groups = (
        _group(
            "SPY",
            2,
            dte=3,
            role="SHORT_DATED_SWEEP_LIKE",
            candidates=(
                _candidate(
                    "SPY",
                    2,
                    dte=3,
                    side=OptionSide.CALL,
                ),
            ),
        ),
        _group(
            "SPY",
            1,
            dte=3,
            role="DIRECTIONAL_MOMENTUM",
            candidates=(
                _candidate(
                    "SPY",
                    1,
                    dte=3,
                    side=OptionSide.PUT,
                ),
            ),
        ),
        _group(
            "SPY",
            0,
            dte=3,
            role="SHORT_DATED_SWEEP_LIKE",
            candidates=(
                _candidate(
                    "SPY",
                    0,
                    dte=3,
                    side=OptionSide.CALL,
                ),
            ),
        ),
    )

    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="call-put-coverage",
        ready_underlyings=("SPY",),
        groups=reversed(groups),
        active_contract_budget=2,
    )
    sides = {
        row.option_side
        for group in allocation.selected_groups
        for row in group.candidates
    }

    assert sides == {OptionSide.CALL, OptionSide.PUT}


def test_complete_group_is_never_cut_to_fit_budget():
    profile = load_activity_momentum_profile()
    first = _candidate("SPY", 0, dte=7, side=OptionSide.CALL)
    shared = _candidate("SPY", 1, dte=7, side=OptionSide.PUT)
    extra = _candidate("SPY", 2, dte=7, side=OptionSide.CALL)
    groups = (
        _group(
            "SPY",
            0,
            candidates=(first, shared),
        ),
        _group(
            "SPY",
            1,
            candidates=(shared, extra),
        ),
    )

    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="complete-groups",
        ready_underlyings=("SPY",),
        groups=groups,
        active_contract_budget=2,
    )

    assert len(allocation.selected_groups) == 1
    assert set(allocation.selected_contract_tickers) == {
        first.contract_ticker,
        shared.contract_ticker,
    }
    assert dict(allocation.rejected_groups)[groups[1].group_id] == (
        "BUDGET_DEFERRED_COMPLETE_GROUP",
    )


def test_missing_oi_is_explicit_but_does_not_block_capture():
    profile = load_activity_momentum_profile()
    candidate = _candidate(
        "SPY",
        0,
        prior_open_interest=None,
    )
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="missing-oi",
        ready_underlyings=("SPY",),
        groups=(
            _group("SPY", 0, candidates=(candidate,)),
        ),
        active_contract_budget=1,
    )
    coverage = next(
        row
        for row in allocation.coverage
        if row.role == "DIRECTIONAL_MOMENTUM"
    )

    assert allocation.selected_contract_tickers == (
        candidate.contract_ticker,
    )
    assert "MISSING_PRIOR_OPEN_INTEREST" in candidate.missing_inputs
    assert coverage.state is CoverageState.FULL
    assert coverage.missing_oi_contract_count == 1
    assert coverage.reason_codes == (
        "MISSING_PRIOR_OPEN_INTEREST",
    )


def test_three_four_dte_capture_keeps_flow_unavailable():
    profile = load_activity_momentum_profile()
    candidates = tuple(
        _candidate(
            "XYZ",
            dte,
            dte=dte,
            flow_eligible=False,
        )
        for dte in (3, 4)
    )
    groups = tuple(
        _group(
            "XYZ",
            index,
            dte=candidate.calendar_dte,
            role="SHORT_DATED_SWEEP_LIKE",
            candidates=(candidate,),
        )
        for index, candidate in enumerate(candidates)
    )

    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="flow-unavailable",
        ready_underlyings=("XYZ",),
        groups=groups,
        active_contract_budget=2,
    )

    assert len(allocation.selected_contract_tickers) == 2
    assert all(not row.flow_eligible for row in candidates)
    assert {
        row.flow_unavailable_reason for row in candidates
    } == {"FLOW_V1_DTE_OR_UNDERLYING_UNAVAILABLE"}


def test_22_60_strategy_group_uses_only_remaining_budget():
    profile = load_activity_momentum_profile()
    groups = (
        _group(
            "SPY",
            0,
            dte=3,
            role="SHORT_DATED_SWEEP_LIKE",
        ),
        _group(
            "SPY",
            1,
            dte=14,
            role="DIRECTIONAL_MOMENTUM",
        ),
        _group(
            "SPY",
            2,
            dte=45,
            role="STRATEGY_REQUIRED",
        ),
    )

    two_slots = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="dte-priority",
        ready_underlyings=("SPY",),
        groups=groups,
        active_contract_budget=2,
    )
    three_slots = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="dte-priority",
        ready_underlyings=("SPY",),
        groups=groups,
        active_contract_budget=3,
    )

    assert {
        group.roles[0] for group in two_slots.selected_groups
    } == {
        "SHORT_DATED_SWEEP_LIKE",
        "DIRECTIONAL_MOMENTUM",
    }
    assert {
        group.roles[0] for group in three_slots.selected_groups
    } == {
        "SHORT_DATED_SWEEP_LIKE",
        "DIRECTIONAL_MOMENTUM",
        "STRATEGY_REQUIRED",
    }


def test_120_730_dte_groups_add_zero_live_workload():
    profile = load_activity_momentum_profile()
    groups = tuple(
        _group(
            "SPY",
            index,
            dte=dte,
            role="STRATEGY_REQUIRED",
        )
        for index, dte in enumerate((120, 730))
    )

    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="silent-disabled",
        ready_underlyings=("SPY",),
        groups=groups,
        active_contract_budget=10,
    )

    assert allocation.selected_contract_tickers == ()
    assert set(dict(allocation.rejected_groups).values()) == {
        (
            "LIVE_DTE_OR_ROLE_UNAVAILABLE",
            "SILENT_ACCUMULATION_DISABLED",
        )
    }
