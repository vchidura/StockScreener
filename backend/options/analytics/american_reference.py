"""Cox-Ross-Rubinstein American option reference pricer.

Validation and measurement only: nothing in the production path imports this module.
The IV solver in `greeks.py` stays European; this lattice exists to measure how far a
European implied volatility is from an American-exercise price (see
`iv_comparability.py` and `backend/scripts/measure_iv_american_bias.py`).
"""

from __future__ import annotations

import math

import numpy as np

from options.analytics.greeks import price_and_greeks
from options.domain import ContractType


def crr_price(
    contract_type: ContractType,
    *,
    spot: float,
    strike: float,
    maturity: float,
    rate: float,
    dividend: float,
    volatility: float,
    steps: int = 1000,
    american: bool = True,
) -> float:
    """Binomial price with a continuous dividend yield (vectorized backward induction)."""
    values = (spot, strike, maturity, rate, dividend, volatility)
    if any(not math.isfinite(value) for value in values):
        raise ValueError("valuation inputs must be finite")
    if spot <= 0 or strike <= 0 or maturity <= 0 or volatility <= 0 or steps < 1:
        raise ValueError("spot, strike, maturity, volatility and steps must be positive")
    dt = maturity / steps
    step_move = volatility * math.sqrt(dt)
    up = math.exp(step_move)
    down = 1.0 / up
    probability = (math.exp((rate - dividend) * dt) - down) / (up - down)
    if not 0.0 < probability < 1.0:
        raise ValueError("lattice is not arbitrage free; increase steps")
    discount = math.exp(-rate * dt)
    sign = 1.0 if contract_type is ContractType.CALL else -1.0
    # Node prices at step i are spot * exp((2j - i) * step_move).
    offsets = np.arange(steps + 1, dtype=np.float64)
    option = np.maximum(sign * (spot * np.exp((2.0 * offsets - steps) * step_move) - strike), 0.0)
    for level in range(steps - 1, -1, -1):
        option = discount * (
            probability * option[1 : level + 2] + (1.0 - probability) * option[: level + 1]
        )
        if american:
            nodes = spot * np.exp((2.0 * offsets[: level + 1] - level) * step_move)
            option = np.maximum(option, sign * (nodes - strike))
    return float(option[0])


def american_price_control_variate(
    contract_type: ContractType,
    *,
    spot: float,
    strike: float,
    maturity: float,
    rate: float,
    dividend: float,
    volatility: float,
    steps: int = 1000,
) -> float:
    """American price corrected for lattice error with the European closed form.

    The lattice's own European error is removed: Black-Scholes plus the lattice
    early-exercise premium. This isolates the exercise effect, which is the quantity
    the IV comparability policy needs, from CRR discretization noise.
    """
    common = dict(
        spot=spot, strike=strike, maturity=maturity, rate=rate,
        dividend=dividend, volatility=volatility, steps=steps,
    )
    premium = crr_price(contract_type, american=True, **common) - crr_price(
        contract_type, american=False, **common
    )
    closed_form = price_and_greeks(
        contract_type, spot=spot, strike=strike, maturity=maturity,
        rate=rate, dividend=dividend, volatility=volatility,
    )[0]
    return closed_form + max(premium, 0.0)
