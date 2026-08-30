import sys
from decimal import Decimal
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import (
    IvFailureReason,
    OptionValuationInput,
    convergence_fraction,
    passes_convergence_gate,
    solve_local_greeks,
)
from options.domain import ContractType


def _input(contract_type, mark):
    return OptionValuationInput(
        contract_type=contract_type,
        spot=Decimal("100"),
        strike=Decimal("100"),
        model_mark=Decimal(str(mark)),
        time_to_expiration_years=1.0,
        risk_free_rate=0.05,
        dividend_yield=0.0,
    )


def test_known_call_and_put_prices_recover_iv_and_unit_explicit_greeks():
    results = solve_local_greeks(
        (
            _input(ContractType.CALL, "10.450583572185565"),
            _input(ContractType.PUT, "5.573526022256971"),
        )
    )

    call, put = results
    assert call.converged and put.converged
    assert call.local_iv == pytest.approx(0.2, abs=1e-8)
    assert put.local_iv == pytest.approx(0.2, abs=1e-8)
    assert call.local_delta == pytest.approx(0.63683065, rel=1e-6)
    assert put.local_delta == pytest.approx(-0.36316935, rel=1e-6)
    assert call.local_gamma == pytest.approx(0.01876202, rel=1e-6)
    assert call.local_theta_per_day < 0
    assert call.local_vega_per_vol_point == pytest.approx(0.37524035, rel=1e-6)
    assert call.local_rho_per_rate_point == pytest.approx(0.53232482, rel=1e-6)
    assert call.price_error < 1e-6


def test_no_arbitrage_violation_is_rejected_without_provider_fallback():
    result = solve_local_greeks((_input(ContractType.CALL, "101"),))[0]

    assert result.converged is False
    assert result.local_iv is None
    assert result.local_gamma is None
    assert result.failure_reason is IvFailureReason.ABOVE_NO_ARBITRAGE_BOUND


def test_brent_fallback_recovers_when_newton_budget_is_exhausted():
    result = solve_local_greeks(
        (_input(ContractType.CALL, "18.02295145021668"),),
        newton_iterations=1,
    )[0]

    assert result.converged is True
    assert result.local_iv == pytest.approx(0.4, abs=1e-8)
    assert result.solver.value == "BRENT"


def test_per_underlying_convergence_gate_is_literal_ninety_five_percent():
    good = solve_local_greeks((_input(ContractType.CALL, "10.450583572185565"),))[0]
    bad = solve_local_greeks((_input(ContractType.CALL, "101"),))[0]
    nineteen_good = tuple(good for _ in range(19))

    assert convergence_fraction(nineteen_good + (bad,)) == pytest.approx(0.95)
    assert passes_convergence_gate(nineteen_good + (bad,)) is True
    assert passes_convergence_gate(tuple(good for _ in range(18)) + (bad, bad)) is False