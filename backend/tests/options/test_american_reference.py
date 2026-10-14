import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.american_reference import american_price_control_variate, crr_price
from options.analytics.greeks import price_and_greeks
from options.domain import ContractType

PUT, CALL = ContractType.PUT, ContractType.CALL


def test_american_put_matches_the_textbook_reference_value():
    # Hull, American put S=K=50, r=10%, sigma=40%, T=5 months: 4.28.
    price = crr_price(
        PUT, spot=50.0, strike=50.0, maturity=5 / 12, rate=0.10, dividend=0.0, volatility=0.40, steps=1000
    )
    assert price == pytest.approx(4.28, abs=0.01)


def test_call_put_symmetry_holds_for_american_prices():
    # McDonald-Schroder: C(S, K, r, q) = P(K, S, q, r).
    call = crr_price(
        CALL, spot=100.0, strike=95.0, maturity=0.5, rate=0.04, dividend=0.07, volatility=0.3, steps=400
    )
    put = crr_price(
        PUT, spot=95.0, strike=100.0, maturity=0.5, rate=0.07, dividend=0.04, volatility=0.3, steps=400
    )
    assert call == pytest.approx(put, abs=1e-8)


def test_european_lattice_converges_to_black_scholes_and_american_never_below_it():
    common = dict(spot=100.0, strike=105.0, maturity=0.25, rate=0.04, dividend=0.01, volatility=0.25)
    for contract_type in (PUT, CALL):
        closed_form = price_and_greeks(contract_type, **common)[0]
        european = crr_price(contract_type, steps=1000, american=False, **common)
        american = crr_price(contract_type, steps=1000, american=True, **common)
        assert european == pytest.approx(closed_form, abs=0.01)
        assert american >= european - 1e-12


def test_call_without_dividend_has_no_early_exercise_premium():
    common = dict(spot=100.0, strike=90.0, maturity=1.0, rate=0.05, dividend=0.0, volatility=0.2)
    american = crr_price(CALL, steps=500, american=True, **common)
    european = crr_price(CALL, steps=500, american=False, **common)
    assert american == pytest.approx(european, abs=1e-9)


def test_control_variate_removes_lattice_error_but_keeps_the_exercise_premium():
    common = dict(spot=100.0, strike=110.0, maturity=0.1, rate=0.05, dividend=0.0, volatility=0.2)
    closed_form = price_and_greeks(PUT, **common)[0]
    adjusted = american_price_control_variate(PUT, steps=200, **common)
    reference = crr_price(PUT, steps=4000, american=True, **common)
    assert adjusted > closed_form
    assert adjusted == pytest.approx(reference, abs=0.005)


@pytest.mark.parametrize(
    "overrides",
    [{"spot": 0.0}, {"strike": -1.0}, {"maturity": 0.0}, {"volatility": 0.0}, {"steps": 0}, {"spot": float("nan")}],
)
def test_invalid_inputs_are_rejected(overrides):
    arguments = dict(
        spot=100.0, strike=100.0, maturity=0.5, rate=0.04, dividend=0.0, volatility=0.2, steps=50
    )
    arguments.update(overrides)
    with pytest.raises(ValueError):
        crr_price(PUT, **arguments)
