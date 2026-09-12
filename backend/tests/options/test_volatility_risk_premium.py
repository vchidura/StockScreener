from datetime import date
from decimal import Decimal

import pytest

from options.analytics.volatility_risk_premium import (
    ContractIvObservation,
    ExpirationIvPoint,
    calculate_variance_risk_premium,
    interpolate_total_variance,
    interpolate_total_variance_at_maturity,
    summarize_expiration_atm_iv,
)
from options.domain import ContractType


def _contract(
    contract_type: ContractType,
    strike: str,
    iv: float,
    *,
    expiration: date = date(2026, 10, 16),
    maturity: float = 30 / 365,
) -> ContractIvObservation:
    return ContractIvObservation(
        expiration_date=expiration,
        maturity_years=maturity,
        contract_type=contract_type,
        strike=Decimal(strike),
        spot=Decimal("100"),
        implied_volatility=iv,
    )


def _expiration(expiration: date, maturity: float, iv: float) -> ExpirationIvPoint:
    return ExpirationIvPoint(expiration, maturity, iv, 4, 2, 2, 0.01)


def test_atm_summary_requires_call_and_put_evidence():
    calls = (_contract(ContractType.CALL, "100", 0.20),)
    assert summarize_expiration_atm_iv(calls) is None


def test_atm_summary_uses_nearest_contracts_on_each_side():
    observations = (
        _contract(ContractType.CALL, "100", 0.20),
        _contract(ContractType.CALL, "101", 0.22),
        _contract(ContractType.CALL, "102", 0.90),
        _contract(ContractType.PUT, "100", 0.24),
        _contract(ContractType.PUT, "99", 0.26),
        _contract(ContractType.PUT, "98", 0.80),
    )
    point = summarize_expiration_atm_iv(observations, contracts_per_side=2)
    assert point is not None
    assert point.contract_count == 4
    assert point.call_count == 2
    assert point.put_count == 2
    assert point.atm_implied_volatility == pytest.approx(0.23)


def test_atm_summary_rejects_contracts_outside_moneyness_band():
    observations = (
        _contract(ContractType.CALL, "110", 0.20),
        _contract(ContractType.PUT, "90", 0.24),
    )
    assert summarize_expiration_atm_iv(observations) is None


def test_atm_summary_requires_one_expiration():
    observations = (
        _contract(ContractType.CALL, "100", 0.20),
        _contract(
            ContractType.PUT,
            "100",
            0.24,
            expiration=date(2026, 11, 20),
        ),
    )
    with pytest.raises(ValueError, match="one expiration"):
        summarize_expiration_atm_iv(observations)


def test_total_variance_interpolation_matches_variance_not_raw_iv():
    lower = _expiration(date(2026, 1, 9), 10 / 252, 0.20)
    upper = _expiration(date(2026, 2, 6), 30 / 252, 0.40)
    matched = interpolate_total_variance((lower, upper), 20)
    assert matched is not None
    expected_variance = (
        (0.20**2 * (10 / 252)) + 0.5 * (
            0.40**2 * (30 / 252) - 0.20**2 * (10 / 252)
        )
    )
    assert matched.implied_volatility == pytest.approx(
        (expected_variance / (20 / 252)) ** 0.5
    )
    assert matched.source_contract_count == 8


def test_total_variance_interpolation_uses_exact_maturity():
    exact = _expiration(date(2026, 1, 30), 21 / 252, 0.31)
    matched = interpolate_total_variance((exact,), 21)
    assert matched is not None
    assert matched.implied_volatility == pytest.approx(0.31)
    assert matched.lower_expiration == matched.upper_expiration
    assert matched.source_contract_count == 4


def test_total_variance_interpolation_refuses_extrapolation():
    points = (
        _expiration(date(2026, 2, 6), 30 / 252, 0.30),
        _expiration(date(2026, 3, 6), 50 / 252, 0.32),
    )
    assert interpolate_total_variance(points, 21) is None


def test_calendar_maturity_does_not_expand_days_into_trading_sessions():
    exact = _expiration(date(2026, 2, 14), 45 / 365, 0.31)

    calendar_match = interpolate_total_variance_at_maturity((exact,), 45 / 365)

    assert calendar_match is not None
    assert calendar_match.implied_volatility == pytest.approx(0.31)
    assert interpolate_total_variance((exact,), 45) is None


def test_variance_risk_premium_reports_forecast_and_realized_spreads():
    result = calculate_variance_risk_premium(0.30, 0.20, 0.25)
    assert result.forecast_variance_premium == pytest.approx(0.05)
    assert result.realized_variance_premium == pytest.approx(0.0275)
    assert result.forecast_richness_ratio == pytest.approx(1.5)
    assert result.realized_richness_ratio == pytest.approx(1.2)


def test_variance_risk_premium_rejects_nonpositive_inputs():
    with pytest.raises(ValueError, match="positive and finite"):
        calculate_variance_risk_premium(0.30, 0.0, 0.25)
