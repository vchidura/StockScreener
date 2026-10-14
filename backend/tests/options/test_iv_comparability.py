import json
import math
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.american_reference import american_price_control_variate
from options.analytics.greeks import (
    IvFailureReason,
    OptionValuationInput,
    price_and_greeks,
    solve_local_greeks,
)
from options.analytics.iv_comparability import (
    DEFAULT_IV_COMPARABILITY_POLICY,
    IvComparabilityPolicy,
    IvComparabilityReason as Reason,
    classify_iv_comparability,
    classify_snapshot_iv,
    comparison_iv,
    summarize_iv_comparability,
    vega_weighted_mean,
)
from options.domain import ContractType

PUT, CALL = ContractType.PUT, ContractType.CALL


def classify(contract_type=PUT, *, strike=100.0, spot=100.0, maturity=30 / 365, rate=0.0, dividend=0.0, iv=0.20, **extra):
    return classify_iv_comparability(
        contract_type=contract_type, spot=spot, strike=strike, time_to_expiration_years=maturity,
        risk_free_rate=rate, dividend_yield=dividend, local_iv=iv, **extra,
    )


def test_default_policy_derives_the_vega_floor_from_the_quote_resolution():
    policy = DEFAULT_IV_COMPARABILITY_POLICY
    assert policy.version == "iv_comparability_v1"
    assert policy.quote_resolution == 0.005 and policy.maximum_quote_noise_vol_points == 1.0
    assert policy.minimum_vega_per_vol_point == pytest.approx(0.005)


def test_put_band_edge_is_inclusive_and_just_beyond_it_is_excluded():
    # Zero carry: forward == spot, so the band edge is 100 * 1.01.
    assert classify(strike=100 * 1.01).comparable
    inside = classify(strike=100.99)
    beyond = classify(strike=101.02)
    assert inside.comparable and inside.reasons == ()
    assert not beyond.comparable and beyond.reasons == (Reason.ITM_PUT_AMERICAN_BIAS,)
    assert beyond.itm_depth == pytest.approx(0.0102)
    assert beyond.weight == 0.0


def test_out_of_the_money_and_at_the_money_puts_are_comparable():
    assert classify(strike=100.0).comparable
    assert classify(strike=90.0).comparable
    assert classify(strike=95.0, maturity=7 / 365).comparable


def test_band_is_measured_against_the_forward_not_the_spot():
    # carry 4.3%: forward 100 * exp(0.043 * 0.5) = 102.17, so a 102 put is OTM by forward.
    near = classify(strike=102.0, maturity=0.5, rate=0.043, dividend=0.0)
    assert near.comparable and near.forward == pytest.approx(102.1730, abs=1e-3)
    assert not classify(strike=104.5, maturity=0.5, rate=0.043, dividend=0.0).comparable


def test_calls_are_comparable_at_any_depth_while_carry_is_a_point_or_more():
    deep = classify(CALL, strike=90.0, maturity=60 / 365, rate=0.043, dividend=0.013, iv=0.30)
    assert deep.comparable and deep.reasons == ()
    assert classify(CALL, strike=90.0, maturity=60 / 365, rate=0.043, dividend=0.033, iv=0.30).comparable


def test_calls_follow_the_itm_band_once_the_carry_is_below_the_floor():
    # q == r (the measured +17 point case) and q > r.
    for dividend in (0.043, 0.08, 0.0345):
        result = classify(CALL, strike=95.0, maturity=7 / 365, rate=0.043, dividend=dividend, iv=0.30)
        assert not result.comparable
        assert result.reasons == (Reason.ITM_CALL_AMERICAN_BIAS,)
    assert classify(CALL, strike=99.5, maturity=30 / 365, rate=0.043, dividend=0.06, iv=0.30).comparable
    assert classify(CALL, strike=105.0, maturity=60 / 365, rate=0.043, dividend=0.06, iv=0.30).comparable


def test_puts_are_comparable_at_any_depth_when_the_yield_exceeds_the_rate_by_the_floor():
    assert classify(PUT, strike=110.0, maturity=30 / 365, rate=0.043, dividend=0.053, iv=0.30).comparable
    assert not classify(PUT, strike=110.0, maturity=30 / 365, rate=0.043, dividend=0.0525, iv=0.30).comparable


def test_solver_failures_become_explicit_reasons():
    not_identifiable = classify(iv=None, failure_reason=IvFailureReason.IV_NOT_IDENTIFIABLE)
    as_text = classify(iv=0.2, failure_reason="IV_NOT_IDENTIFIABLE")
    other = classify(iv=None, failure_reason=IvFailureReason.IV_CONVERGENCE_FAILED)
    assert not_identifiable.reasons == as_text.reasons == (Reason.NOT_IDENTIFIABLE,)
    assert other.reasons == (Reason.IV_UNAVAILABLE,)
    for bad_iv in (None, 0.0, -0.1, float("nan"), float("inf")):
        assert classify(iv=bad_iv).reasons == (Reason.IV_UNAVAILABLE,)


@pytest.mark.parametrize(
    "overrides",
    [{"maturity": 0.0}, {"maturity": -1.0}, {"spot": 0.0}, {"strike": -5.0}, {"rate": float("nan")}, {"dividend": float("inf")}],
)
def test_invalid_inputs_are_reported_not_raised(overrides):
    result = classify(**overrides)
    assert not result.comparable and result.reasons == (Reason.INVALID_INPUT,)


def test_decimal_prices_are_accepted():
    assert classify(strike=Decimal("95"), spot=Decimal("100")).comparable


def test_vega_floor_boundary_uses_supplied_vega():
    floor = DEFAULT_IV_COMPARABILITY_POLICY.minimum_vega_per_vol_point
    at_floor = classify(local_vega_per_vol_point=floor)
    below = classify(local_vega_per_vol_point=floor - 1e-9)
    assert at_floor.comparable
    assert not below.comparable and below.reasons == (Reason.LOW_VEGA,)
    assert below.quote_noise_vol_points == pytest.approx(0.005 / (floor - 1e-9))
    assert classify(local_vega_per_vol_point=0.0).quote_noise_vol_points == math.inf
    assert classify(local_vega_per_vol_point=0.0).reasons == (Reason.LOW_VEGA,)


def test_vega_is_derived_from_the_iv_when_not_supplied_and_matches_the_solver_units():
    derived = classify(strike=102.0, maturity=45 / 365, rate=0.04, dividend=0.013, iv=0.24)
    reference = price_and_greeks(
        PUT, spot=100.0, strike=102.0, maturity=45 / 365, rate=0.04, dividend=0.013, volatility=0.24
    )[4]
    assert derived.vega_per_vol_point == pytest.approx(reference, rel=1e-12)


def test_far_wing_short_dated_option_is_excluded_for_low_vega():
    result = classify(CALL, strike=112.0, maturity=1 / 365, iv=0.2)
    assert not result.comparable and result.reasons == (Reason.LOW_VEGA,)


def test_one_contract_can_carry_both_the_bias_and_the_vega_reason():
    result = classify(strike=108.0, maturity=1 / 365, iv=0.15)
    assert result.reasons == (Reason.ITM_PUT_AMERICAN_BIAS, Reason.LOW_VEGA)
    assert result.reason_codes == ("ITM_PUT_AMERICAN_BIAS", "LOW_VEGA")


def test_weight_is_the_inverse_quote_variance_and_zero_when_excluded():
    result = classify(local_vega_per_vol_point=0.05)
    assert result.weight == pytest.approx((0.05 / 0.005) ** 2)
    assert classify(local_vega_per_vol_point=0.0049).weight == 0.0
    heavier = classify(local_vega_per_vol_point=0.10)
    assert heavier.weight == pytest.approx(4 * result.weight)


def test_policy_rejects_invalid_parameters():
    for kwargs in ({"itm_tolerance": -0.01}, {"quote_resolution": 0.0}, {"maximum_quote_noise_vol_points": 0.0}, {"carry_floor": float("nan")}):
        with pytest.raises(ValueError):
            IvComparabilityPolicy(**kwargs)
    wider = IvComparabilityPolicy(itm_tolerance=0.03)
    assert classify(strike=102.5, policy=wider).comparable


def test_summary_counts_contracts_and_reasons_and_reports_status():
    results = [
        classify(strike=100.0),
        classify(strike=105.0),
        classify(strike=108.0, maturity=1 / 365, iv=0.15),
        classify(iv=None, failure_reason=IvFailureReason.IV_NOT_IDENTIFIABLE),
    ]
    summary = summarize_iv_comparability(results)
    assert (summary.considered_count, summary.comparable_count, summary.excluded_count) == (4, 1, 3)
    assert summary.status == "DEGRADED"
    assert dict(summary.reason_counts) == {"ITM_PUT_AMERICAN_BIAS": 2, "LOW_VEGA": 1, "NOT_IDENTIFIABLE": 1}
    assert json.loads(json.dumps(summary.to_dict())) == {
        "policy_version": "iv_comparability_v1", "status": "DEGRADED", "considered": 4, "comparable": 1,
        "excluded": 3, "reasons": {"ITM_PUT_AMERICAN_BIAS": 2, "LOW_VEGA": 1, "NOT_IDENTIFIABLE": 1},
    }
    assert summarize_iv_comparability(results[:1]).status == "COMPARABLE"


def test_empty_and_fully_excluded_cohorts_are_unavailable_not_zero():
    assert summarize_iv_comparability(()).status == "UNAVAILABLE"
    only_itm = summarize_iv_comparability([classify(strike=105.0), classify(strike=110.0)])
    assert only_itm.status == "UNAVAILABLE" and only_itm.comparable_count == 0
    assert only_itm.to_dict()["reasons"] == {"ITM_PUT_AMERICAN_BIAS": 2}


def test_vega_weighted_mean_uses_only_comparable_contracts():
    results = [
        classify(strike=100.0, local_vega_per_vol_point=0.05),
        classify(strike=100.0, local_vega_per_vol_point=0.10),
        classify(strike=110.0, local_vega_per_vol_point=0.50),
    ]
    expected = (1 * 0.20 + 4 * 0.30) / 5
    assert vega_weighted_mean([0.20, 0.30, 0.90], results) == pytest.approx(expected)
    assert vega_weighted_mean([0.90], [results[2]]) is None
    assert vega_weighted_mean([], []) is None
    assert vega_weighted_mean([0.2, float("nan")], results[:2]) == pytest.approx(0.20)
    with pytest.raises(ValueError):
        vega_weighted_mean([0.2], results)


def snapshot(contract_type=PUT, *, strike="100", iv=0.20, converged=True, failure=None, vega=0.1):
    return SimpleNamespace(
        contract_type=contract_type, spot=Decimal("100"), strike=Decimal(strike),
        time_to_expiration_years=30 / 365, risk_free_rate=0.0, dividend_yield=0.0, local_iv=iv,
        local_vega_per_vol_point=vega, iv_converged=converged, iv_failure_reason=failure,
    )


def test_snapshot_adapter_reads_the_snapshot_fields_and_accepts_enum_like_types():
    assert classify_snapshot_iv(snapshot(strike="100")).comparable
    assert classify_snapshot_iv(snapshot(SimpleNamespace(value="PUT"), strike="105")).reasons == (
        Reason.ITM_PUT_AMERICAN_BIAS,
    )
    failed = classify_snapshot_iv(snapshot(iv=None, converged=False, failure="IV_NOT_IDENTIFIABLE"))
    assert failed.reasons == (Reason.NOT_IDENTIFIABLE,)
    assert classify_snapshot_iv(snapshot(iv=None, converged=False, failure=None)).reasons == (Reason.IV_UNAVAILABLE,)


def test_comparison_iv_returns_the_iv_or_measurement_ready_reason_codes():
    assert comparison_iv(snapshot(strike="99")) == (0.20, ())
    value, reasons = comparison_iv(snapshot(strike="105", vega=0.0001))
    assert value is None
    assert reasons == ("IV_NOT_COMPARABLE_ITM_PUT_AMERICAN_BIAS", "IV_NOT_COMPARABLE_LOW_VEGA")
    assert all(len(code) <= 80 and " " not in code for code in reasons)


def european_iv_bias_points(contract_type, *, depth, days, volatility, rate=0.043, dividend=0.013, spot=600.0):
    maturity = days / 365.0
    forward = spot * math.exp((rate - dividend) * maturity)
    strike = round(forward * (1.0 + depth if contract_type is PUT else 1.0 - depth), 6)
    price = american_price_control_variate(
        contract_type, spot=spot, strike=strike, maturity=maturity, rate=rate,
        dividend=dividend, volatility=volatility, steps=300,
    )
    solved = solve_local_greeks((OptionValuationInput(
        contract_type=contract_type, spot=Decimal(str(spot)), strike=Decimal(str(strike)),
        model_mark=Decimal(repr(price)), time_to_expiration_years=maturity,
        risk_free_rate=rate, dividend_yield=dividend,
    ),))[0]
    assert solved.converged
    return (solved.local_iv - volatility) * 100.0


def test_policy_band_keeps_measured_american_bias_small_and_exclusion_is_warranted():
    # Inside the band every grid point stays within half a point of the true volatility.
    for days in (3, 7, 30, 60):
        for volatility in (0.15, 0.40):
            assert abs(european_iv_bias_points(PUT, depth=0.01, days=days, volatility=volatility)) < 0.5
    # Just outside it the bias is material, which is what the exclusion protects against.
    assert european_iv_bias_points(PUT, depth=0.05, days=7, volatility=0.20) > 1.5
    assert european_iv_bias_points(PUT, depth=0.10, days=30, volatility=0.20) > 3.0


def test_call_claim_holds_with_positive_carry_and_fails_without_it():
    assert abs(european_iv_bias_points(CALL, depth=0.10, days=7, volatility=0.20, dividend=0.013)) < 0.1
    assert european_iv_bias_points(CALL, depth=0.10, days=7, volatility=0.20, dividend=0.043) > 5.0
    assert classify(CALL, strike=90.0, maturity=60 / 365, rate=0.043, dividend=0.013, iv=0.30).comparable
    assert not classify(CALL, strike=90.0, maturity=60 / 365, rate=0.043, dividend=0.043, iv=0.30).comparable
