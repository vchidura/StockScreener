from __future__ import annotations

from datetime import date
from decimal import Decimal

import numpy as np
import pytest

from options.analytics.gamma_exposure import (
    GammaExposureInput,
    build_gamma_profile,
    dealer_signs,
    solve_gamma_flip,
)
from options.analytics.greeks import black_scholes_gamma, price_and_greeks
from options.domain import ContractType, DealerConvention, GammaRegime, VolatilityAssumption

MARKET_DATE = date(2026, 9, 4)


def _row(
    contract_id: int,
    contract_type: ContractType,
    strike: str,
    open_interest: int,
    *,
    local_iv: float = 0.25,
    maturity: float = 0.08,
    expiration: date = date(2026, 10, 2),
) -> GammaExposureInput:
    return GammaExposureInput(
        contract_id=contract_id,
        contract_type=contract_type,
        expiration_date=expiration,
        strike=Decimal(strike),
        open_interest=open_interest,
        local_iv=local_iv,
        time_to_expiration_years=maturity,
        risk_free_rate=0.04,
        dividend_yield=0.0,
    )


def _chain() -> tuple[GammaExposureInput, ...]:
    rows: list[GammaExposureInput] = []
    contract_id = 1
    for strike in range(90, 111):
        for contract_type in (ContractType.CALL, ContractType.PUT):
            rows.append(_row(contract_id, contract_type, str(strike), 500))
            contract_id += 1
    return tuple(rows)


def test_vectorized_gamma_matches_scalar_engine():
    spot, strike, maturity, rate, dividend, volatility = 100.0, 105.0, 0.08, 0.04, 0.01, 0.3
    _, _, scalar_gamma, _, _, _ = price_and_greeks(
        ContractType.CALL,
        spot=spot,
        strike=strike,
        maturity=maturity,
        rate=rate,
        dividend=dividend,
        volatility=volatility,
    )
    vector_gamma = black_scholes_gamma(
        np.array([spot]),
        np.array([strike]),
        np.array([maturity]),
        np.array([rate]),
        np.array([dividend]),
        np.array([volatility]),
    )
    assert vector_gamma[0] == pytest.approx(scalar_gamma, rel=1e-12)


def test_vectorized_gamma_is_identical_for_calls_and_puts():
    args = (
        np.array([100.0]),
        np.array([105.0]),
        np.array([0.08]),
        np.array([0.04]),
        np.array([0.0]),
        np.array([0.3]),
    )
    _, _, call_gamma, _, _, _ = price_and_greeks(
        ContractType.CALL, spot=100.0, strike=105.0, maturity=0.08, rate=0.04,
        dividend=0.0, volatility=0.3,
    )
    _, _, put_gamma, _, _, _ = price_and_greeks(
        ContractType.PUT, spot=100.0, strike=105.0, maturity=0.08, rate=0.04,
        dividend=0.0, volatility=0.3,
    )
    assert call_gamma == pytest.approx(put_gamma, rel=1e-12)
    assert black_scholes_gamma(*args)[0] == pytest.approx(call_gamma, rel=1e-12)


def test_invalid_inputs_produce_nan_not_exceptions():
    gamma = black_scholes_gamma(
        np.array([100.0, -1.0, 100.0, 100.0]),
        np.array([100.0, 100.0, 100.0, 100.0]),
        np.array([0.08, 0.08, 0.0, 0.08]),
        np.array([0.04, 0.04, 0.04, 0.04]),
        np.array([0.0, 0.0, 0.0, 0.0]),
        np.array([0.3, 0.3, 0.3, 0.0]),
    )
    assert np.isfinite(gamma[0])
    assert np.isnan(gamma[1:]).all()


def test_strike_aggregation_keeps_calls_and_puts_separate():
    rows = (
        _row(1, ContractType.CALL, "100", 300),
        _row(2, ContractType.PUT, "100", 700),
    )
    profile = build_gamma_profile(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        minimum_contracts_for_profile=1,
    )
    assert len(profile.strikes) == 1
    row = profile.strikes[0]
    assert row.call_open_interest == 300
    assert row.put_open_interest == 700
    assert row.call_gamma_notional_per_percent > 0
    assert row.put_gamma_notional_per_percent > 0
    # Same strike and maturity, so per-contract gamma is identical; only OI differs.
    assert row.put_gamma_notional_per_percent == pytest.approx(
        row.call_gamma_notional_per_percent * 700 / 300
    )


def test_conventions_produce_opposite_signs_from_identical_facts():
    rows = (
        _row(1, ContractType.CALL, "100", 1000),
        _row(2, ContractType.PUT, "100", 200),
    )
    long_calls = build_gamma_profile(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        minimum_contracts_for_profile=1,
    )
    short_calls = build_gamma_profile(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_SHORT_CALLS_LONG_PUTS,
        minimum_contracts_for_profile=1,
    )
    assert long_calls.net_gamma_notional_per_percent > 0
    assert short_calls.net_gamma_notional_per_percent < 0
    assert long_calls.net_gamma_notional_per_percent == pytest.approx(
        -short_calls.net_gamma_notional_per_percent
    )
    # The underlying unsigned facts must be identical under both conventions.
    assert long_calls.strikes == short_calls.strikes
    assert long_calls.absolute_gamma_notional_per_percent == pytest.approx(
        short_calls.absolute_gamma_notional_per_percent
    )


def test_dealer_signs_are_exhaustive_and_opposed():
    assert dealer_signs(DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS) == (1.0, -1.0)
    assert dealer_signs(DealerConvention.DEALER_SHORT_CALLS_LONG_PUTS) == (-1.0, 1.0)


def test_notional_scaling_uses_spot_squared_and_multiplier():
    rows = (_row(1, ContractType.CALL, "100", 10),)
    spot = 100.0
    profile = build_gamma_profile(
        rows,
        Decimal(str(int(spot))),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        minimum_contracts_for_profile=1,
    )
    row = profile.strikes[0]
    gamma = float(
        black_scholes_gamma(
            np.array([spot]), np.array([100.0]), np.array([0.08]),
            np.array([0.04]), np.array([0.0]), np.array([0.25]),
        )[0]
    )
    assert row.call_gamma_shares_per_point == pytest.approx(gamma * 10 * 100)
    assert row.call_gamma_notional_per_percent == pytest.approx(
        gamma * 10 * 100 * spot * spot * 0.01
    )
    assert row.call_gamma_notional_per_percent == pytest.approx(
        row.call_gamma_shares_per_point * spot * spot * 0.01
    )


def test_flip_point_found_between_opposing_gamma_regions():
    # Puts concentrated low, calls concentrated high: signed net gamma must cross zero.
    rows = (
        _row(1, ContractType.PUT, "80", 5000),
        _row(2, ContractType.CALL, "120", 5000),
    )
    flip = solve_gamma_flip(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        search_fraction=0.3,
        grid_points=121,
    )
    assert flip.flip_spot is not None
    assert flip.searched_low_spot < flip.flip_spot < flip.searched_high_spot
    assert flip.sign_change_count >= 1


def test_flip_absent_when_all_gamma_has_one_sign():
    rows = (_row(1, ContractType.CALL, "100", 1000),)
    flip = solve_gamma_flip(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
    )
    assert flip.flip_spot is None
    assert flip.regime_at_spot is GammaRegime.POSITIVE_GAMMA
    assert "NO_FLIP_IN_SEARCH_RANGE" in flip.reasons


def test_regime_flips_with_convention():
    rows = (_row(1, ContractType.CALL, "100", 1000),)
    negative = solve_gamma_flip(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_SHORT_CALLS_LONG_PUTS,
    )
    assert negative.regime_at_spot is GammaRegime.NEGATIVE_GAMMA


def test_gamma_is_recomputed_not_reweighted_across_the_grid():
    # A far OTM contract carries almost no gamma at spot but material gamma near
    # its strike; a re-weighting implementation would keep the curve flat.
    rows = (_row(1, ContractType.CALL, "118", 5000),)
    flip = solve_gamma_flip(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        search_fraction=0.2,
        grid_points=81,
    )
    gamma_at_spot = black_scholes_gamma(
        np.array([100.0]), np.array([118.0]), np.array([0.08]),
        np.array([0.04]), np.array([0.0]), np.array([0.25]),
    )[0]
    gamma_at_strike = black_scholes_gamma(
        np.array([118.0]), np.array([118.0]), np.array([0.08]),
        np.array([0.04]), np.array([0.0]), np.array([0.25]),
    )[0]
    assert gamma_at_strike > gamma_at_spot * 5
    assert flip.grid_points == 81


def test_expired_zero_iv_and_thin_contracts_are_excluded():
    rows = (
        _row(1, ContractType.CALL, "100", 500),
        _row(2, ContractType.CALL, "101", 500, maturity=0.0),
        _row(3, ContractType.CALL, "102", 500, local_iv=0.0),
        _row(4, ContractType.CALL, "103", 0),
    )
    profile = build_gamma_profile(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        minimum_contracts_for_profile=1,
    )
    assert profile.contributing_contract_count == 1
    assert profile.eligible_contract_count == 4
    assert profile.coverage_fraction == pytest.approx(0.25)


def test_dte_window_is_applied_against_market_date():
    rows = (
        _row(1, ContractType.CALL, "100", 500, expiration=date(2026, 9, 18)),
        _row(2, ContractType.CALL, "100", 500, expiration=date(2027, 6, 18)),
    )
    profile = build_gamma_profile(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        maximum_dte=45,
        market_date=MARKET_DATE,
        minimum_contracts_for_profile=1,
    )
    assert profile.contributing_contract_count == 1


def test_empty_and_all_ineligible_chains_are_reason_coded():
    empty = build_gamma_profile(
        (),
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
    )
    assert empty.strikes == ()
    assert empty.net_gamma_notional_per_percent == 0.0
    assert "NO_ELIGIBLE_GAMMA_CONTRACT" in empty.quality_reasons
    assert empty.flip.regime_at_spot is GammaRegime.UNDETERMINED


def test_insufficient_contract_count_is_reason_coded_not_fatal():
    profile = build_gamma_profile(
        (_row(1, ContractType.CALL, "100", 500),),
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        minimum_contracts_for_profile=20,
    )
    assert "INSUFFICIENT_GAMMA_CONTRACTS" in profile.quality_reasons
    assert profile.contributing_contract_count == 1


def test_profile_records_assumptions_and_peak_strike():
    profile = build_gamma_profile(
        _chain(),
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
    )
    assert profile.volatility_assumption is VolatilityAssumption.STICKY_STRIKE
    assert profile.shares_per_contract == 100
    assert profile.convention is DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS
    # Peak gamma tracks the forward, which sits above spot while r > q.
    assert profile.peak_gamma_strike == Decimal("101")
    assert profile.quality_reasons == ()


def test_balanced_chain_reports_flat_curve_rather_than_many_flips():
    # Calls and puts share gamma at each strike, so convention A nets to exactly zero.
    profile = build_gamma_profile(
        _chain(),
        Decimal("100"),
        convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
    )
    assert profile.net_gamma_notional_per_percent == pytest.approx(0.0)
    assert profile.flip.flip_spot is None
    assert profile.flip.sign_change_count == 0
    assert profile.flip.reasons == ("FLAT_ZERO_GAMMA_CURVE",)
    assert profile.flip.regime_at_spot is GammaRegime.UNDETERMINED
    # The unsigned facts remain non-zero even though the signed net cancels.
    assert profile.absolute_gamma_notional_per_percent > 0


def test_invalid_grid_and_spot_are_rejected():
    rows = (_row(1, ContractType.CALL, "100", 500),)
    with pytest.raises(ValueError):
        build_gamma_profile(
            rows, Decimal("0"),
            convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
        )
    with pytest.raises(ValueError):
        build_gamma_profile(
            rows, Decimal("100"),
            convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
            flip_grid_points=80,
        )
    with pytest.raises(ValueError):
        build_gamma_profile(
            rows, Decimal("100"),
            convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS,
            maximum_dte=45,
        )
