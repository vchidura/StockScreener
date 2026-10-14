from datetime import date
from decimal import Decimal

import pytest

from options.analytics.iv_comparability import DEFAULT_IV_COMPARABILITY_POLICY
from options.analytics.volatility_risk_premium import (
    ContractIvObservation,
    ExpirationIvPoint,
    assess_expiration_atm_iv,
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
    **carry,
) -> ContractIvObservation:
    return ContractIvObservation(
        expiration_date=expiration,
        maturity_years=maturity,
        contract_type=contract_type,
        strike=Decimal(strike),
        spot=Decimal("100"),
        implied_volatility=iv,
        **carry,
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


CARRY = dict(risk_free_rate=0.04, dividend_yield=0.01)
POLICY = DEFAULT_IV_COMPARABILITY_POLICY


def _with_itm_put(itm_put_iv: float):
    return (
        _contract(ContractType.CALL, "100", 0.20, **CARRY),
        _contract(ContractType.CALL, "101", 0.22, **CARRY),
        _contract(ContractType.PUT, "100", 0.24, **CARRY),
        _contract(ContractType.PUT, "101.5", itm_put_iv, **CARRY),
        _contract(ContractType.PUT, "98", 0.26, **CARRY),
    )


def test_itm_put_no_longer_moves_the_atm_point_and_the_next_comparable_put_replaces_it():
    low, high = _with_itm_put(0.10), _with_itm_put(0.60)

    comparable_low = summarize_expiration_atm_iv(low, comparability_policy=POLICY)
    comparable_high = summarize_expiration_atm_iv(high, comparability_policy=POLICY)
    legacy_low = summarize_expiration_atm_iv(low)
    legacy_high = summarize_expiration_atm_iv(high)

    assert comparable_low.atm_implied_volatility == pytest.approx(0.23)
    assert comparable_high.atm_implied_volatility == pytest.approx(0.23)
    assert (comparable_low.contract_count, comparable_low.call_count, comparable_low.put_count) == (4, 2, 2)
    assert legacy_low.atm_implied_volatility == pytest.approx(0.21)
    assert legacy_high.atm_implied_volatility == pytest.approx(0.23)
    assert legacy_low.excluded_contract_count == 0 and legacy_low.excluded_reason_counts == ()


def test_assessment_reports_excluded_contracts_and_degraded_status():
    assessment = assess_expiration_atm_iv(_with_itm_put(0.60), comparability_policy=POLICY)

    assert assessment.status == "DEGRADED"
    assert assessment.point.excluded_contract_count == 1
    assert assessment.point.excluded_reason_counts == (("ITM_PUT_AMERICAN_BIAS", 1),)
    assert (assessment.comparability.considered_count, assessment.comparability.excluded_count) == (5, 1)
    clean = assess_expiration_atm_iv(_with_itm_put(0.60)[:3] + _with_itm_put(0.60)[4:], comparability_policy=POLICY)
    assert clean.status == "COMPARABLE" and clean.point.excluded_contract_count == 0


def test_all_excluded_side_is_unavailable_with_a_reason_not_a_silent_none():
    observations = (
        _contract(ContractType.CALL, "100", 0.20, **CARRY),
        _contract(ContractType.PUT, "102", 0.50, **CARRY),
        _contract(ContractType.PUT, "103", 0.55, **CARRY),
    )

    assessment = assess_expiration_atm_iv(observations, comparability_policy=POLICY)

    assert assessment.point is None and assessment.status == "UNAVAILABLE"
    assert assessment.unavailable_reasons == ("NO_COMPARABLE_PUT_IN_BAND",)
    assert dict(assessment.comparability.reason_counts) == {"ITM_PUT_AMERICAN_BIAS": 2}
    assert summarize_expiration_atm_iv(observations, comparability_policy=POLICY) is None
    assert summarize_expiration_atm_iv(observations).atm_implied_volatility == pytest.approx(0.50)


def test_unavailable_reasons_distinguish_empty_out_of_band_and_not_comparable():
    assert assess_expiration_atm_iv((), comparability_policy=POLICY).unavailable_reasons == ("NO_OBSERVATIONS",)
    only_puts = (_contract(ContractType.PUT, "100", 0.24, **CARRY),)
    assert assess_expiration_atm_iv(only_puts).unavailable_reasons == ("NO_CALL_IN_BAND",)
    far = (_contract(ContractType.CALL, "110", 0.20), _contract(ContractType.PUT, "90", 0.24))
    assert assess_expiration_atm_iv(far).unavailable_reasons == ("NO_CALL_IN_BAND", "NO_PUT_IN_BAND")


def test_missing_rate_and_yield_assume_zero_carry_so_an_itm_call_is_excluded():
    itm_call = (
        _contract(ContractType.CALL, "98.5", 0.20),
        _contract(ContractType.PUT, "99", 0.24),
    )
    with_carry = tuple(
        _contract(row.contract_type, str(row.strike), row.implied_volatility, **CARRY) for row in itm_call
    )
    assert assess_expiration_atm_iv(itm_call, comparability_policy=POLICY).unavailable_reasons == (
        "NO_COMPARABLE_CALL_IN_BAND",
    )
    assert assess_expiration_atm_iv(with_carry, comparability_policy=POLICY).point is not None


def test_low_vega_contracts_are_skipped_with_their_reason():
    observations = (
        _contract(ContractType.CALL, "100", 0.20, vega_per_vol_point=0.0001, **CARRY),
        _contract(ContractType.CALL, "101", 0.22, vega_per_vol_point=0.08, **CARRY),
        _contract(ContractType.PUT, "100", 0.24, vega_per_vol_point=0.08, **CARRY),
    )

    point = summarize_expiration_atm_iv(observations, contracts_per_side=1, comparability_policy=POLICY)

    assert point.call_count == 1 and point.atm_implied_volatility == pytest.approx(0.23)
    assert point.excluded_reason_counts == (("LOW_VEGA", 1),)


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
