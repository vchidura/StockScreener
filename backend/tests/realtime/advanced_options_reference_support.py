from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from realtime.advanced_options_reference_state import (
    AdvancedOptionsAdjustedDailyBarIdentity,
    AdvancedOptionsCorporateActionIdentity,
    AdvancedOptionsDividendCoverageReference,
    AdvancedOptionsEquityMetric,
    AdvancedOptionsHistoricalVolatilityFact,
    AdvancedOptionsOpenInterestReference,
    AdvancedOptionsRateReference,
    AdvancedOptionsValuationReference,
    EquityMetricKind,
    ReferenceAvailability,
    build_historical_volatility_fact,
    build_advanced_options_reference_state,
)


UTC = timezone.utc
SESSION = date(2026, 10, 16)
KNOWN = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
VALID_UNTIL = datetime(2026, 10, 16, 21, 0, tzinfo=UTC)
SECURITY = UUID("11111111-2222-3333-4444-555555555555")
EXPIRATION = date(2026, 11, 20)


def adjusted_bars():
    sessions = tuple(
        SESSION - timedelta(days=31 - index)
        for index in range(31)
    )
    return tuple(
        AdvancedOptionsAdjustedDailyBarIdentity(
            bar_revision_id=UUID(int=index + 1),
            security_id=SECURITY,
            ticker="SPY",
            session_date=session,
            adjusted_close=Decimal("500") + index,
            market_close_at=datetime(
                session.year,
                session.month,
                session.day,
                20,
                tzinfo=UTC,
            ),
            known_at=datetime(
                session.year,
                session.month,
                session.day,
                21,
                tzinfo=UTC,
            ),
            adjustment_lineage_sha256=f"{index + 1:064x}",
            finality_sha256=f"{index + 101:064x}",
            payload_sha256=f"{index + 201:064x}",
        )
        for index, session in enumerate(sessions)
    )


def open_interest(known_at=KNOWN):
    return (
        AdvancedOptionsOpenInterestReference(
            contract_id=101,
            contract_ticker="O:SPY261120C00500000",
            underlyer="SPY",
            availability=ReferenceAvailability.AVAILABLE,
            known_at=known_at,
            baseline_id=UUID("22222222-2222-2222-2222-222222222222"),
            settlement_session=date(2026, 10, 15),
            observed_session=SESSION,
            open_interest=2_500,
            source="massive",
            date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
            observed_at=known_at - timedelta(minutes=10),
            received_at=known_at - timedelta(minutes=5),
            payload_sha256="1" * 64,
        ),
        AdvancedOptionsOpenInterestReference(
            contract_id=102,
            contract_ticker="O:SPY261120P00450000",
            underlyer="SPY",
            availability=ReferenceAvailability.UNAVAILABLE,
            known_at=known_at,
            reason_codes=("PRIOR_SESSION_OI_NOT_OBSERVED",),
        ),
    )


def volatility(known_at=KNOWN):
    bars = adjusted_bars()
    cutoff = bars[-1].market_close_at
    return (
        build_historical_volatility_fact(
            security_id=SECURITY,
            ticker="SPY",
            window_sessions=20,
            market_cutoff=cutoff,
            known_at=known_at,
            bars=bars[-21:],
        ),
        build_historical_volatility_fact(
            security_id=SECURITY,
            ticker="SPY",
            window_sessions=30,
            market_cutoff=cutoff,
            known_at=known_at,
            bars=bars,
        ),
    )


def valuation(known_at=KNOWN):
    received = known_at - timedelta(minutes=20)
    action = AdvancedOptionsCorporateActionIdentity(
        action_id=UUID("33333333-3333-3333-3333-333333333333"),
        payload_sha256="4" * 64,
        observed_at=received - timedelta(minutes=1),
        received_at=received,
    )
    coverage = AdvancedOptionsDividendCoverageReference(
        coverage_id=UUID("44444444-4444-4444-4444-444444444444"),
        security_id=SECURITY,
        ticker="SPY",
        window_start=SESSION,
        window_end=EXPIRATION,
        status="AVAILABLE",
        observed_at=received - timedelta(minutes=1),
        received_at=received,
        payload_sha256="5" * 64,
        response_sha256="6" * 64,
        actions=(action,),
    )
    rates = (
        AdvancedOptionsRateReference(
            observation_id=UUID("55555555-5555-5555-5555-555555555555"),
            rate_date=date(2026, 10, 15),
            tenor_days=30,
            annual_rate=Decimal("0.04"),
            source="us_treasury_par_yield_v1",
            source_key="2026-10-15:30d",
            observed_at=received - timedelta(minutes=2),
            received_at=received,
            payload_sha256="7" * 64,
        ),
        AdvancedOptionsRateReference(
            observation_id=UUID("66666666-6666-6666-6666-666666666666"),
            rate_date=date(2026, 10, 15),
            tenor_days=60,
            annual_rate=Decimal("0.041"),
            source="us_treasury_par_yield_v1",
            source_key="2026-10-15:60d",
            observed_at=received - timedelta(minutes=2),
            received_at=received,
            payload_sha256="8" * 64,
        ),
    )
    return (
        AdvancedOptionsValuationReference(
            security_id=SECURITY,
            ticker="SPY",
            valuation_date=SESSION,
            expiration_date=EXPIRATION,
            risk_free_rate=Decimal("0.0405"),
            dividend_yield=Decimal("0.012"),
            rate_observations=rates,
            dividend_coverage=coverage,
            model_version="black-scholes-merton-v1",
            model_bundle_sha256="9" * 64,
            valuation_policy_sha256="a" * 64,
            known_at=known_at,
        ),
    )


def equity_metrics(
    known_at=KNOWN,
    *,
    short_interest=Decimal("0.08"),
):
    received = known_at - timedelta(minutes=30)
    sessions = tuple(
        SESSION - timedelta(days=30 - index)
        for index in range(30)
    )
    return (
        AdvancedOptionsEquityMetric(
            security_id=SECURITY,
            ticker="SPY",
            metric_kind=EquityMetricKind.SHORT_INTEREST_PERCENT,
            as_of_session=sessions[-1],
            availability=ReferenceAvailability.AVAILABLE,
            known_at=known_at,
            value=short_interest,
            source="reviewed_fundamentals",
            source_key="SPY:short-interest:2026-10-15",
            observed_at=received - timedelta(minutes=1),
            received_at=received,
            payload_sha256=(
                "b" * 64
                if short_interest == Decimal("0.08")
                else "c" * 64
            ),
        ),
        AdvancedOptionsEquityMetric(
            security_id=SECURITY,
            ticker="SPY",
            metric_kind=EquityMetricKind.TOTAL_FLOAT,
            as_of_session=sessions[-1],
            availability=ReferenceAvailability.UNAVAILABLE,
            known_at=known_at,
            reason_codes=("REVIEWED_FLOAT_SOURCE_UNAVAILABLE",),
        ),
        AdvancedOptionsEquityMetric(
            security_id=SECURITY,
            ticker="SPY",
            metric_kind=EquityMetricKind.AVERAGE_DAILY_VOLUME_30,
            as_of_session=sessions[-1],
            availability=ReferenceAvailability.AVAILABLE,
            known_at=known_at,
            value=Decimal("65000000"),
            source="finalized_adjusted_daily_bars",
            source_key="SPY:ADV30:2026-10-15",
            observed_at=received - timedelta(minutes=1),
            received_at=received,
            payload_sha256="d" * 64,
            calculation_version="adv30-v1",
            member_sessions=sessions,
            member_sha256="e" * 64,
        ),
    )


def reference_state(
    *,
    generation=7,
    known_at=KNOWN,
    valid_until=VALID_UNTIL,
    short_interest=Decimal("0.08"),
    contract_scope_sha256="0" * 64,
):
    return build_advanced_options_reference_state(
        reference_generation=generation,
        session_date=SESSION,
        known_at=known_at,
        valid_until=valid_until,
        reference_policy_sha256="f" * 64,
        contract_scope_sha256=contract_scope_sha256,
        underlyings=("SPY",),
        open_interest=open_interest(known_at),
        historical_volatility=volatility(known_at),
        valuation_inputs=valuation(known_at),
        equity_metrics=equity_metrics(
            known_at,
            short_interest=short_interest,
        ),
    )
