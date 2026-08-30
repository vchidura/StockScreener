from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

import numpy as np
from scipy.optimize import brentq
from scipy.special import ndtr

from options.domain import ContractType


class IvSolver(str, Enum):
    NEWTON = "NEWTON"
    BRENT = "BRENT"


class IvFailureReason(str, Enum):
    INVALID_INPUT = "INVALID_INPUT"
    BELOW_NO_ARBITRAGE_BOUND = "BELOW_NO_ARBITRAGE_BOUND"
    ABOVE_NO_ARBITRAGE_BOUND = "ABOVE_NO_ARBITRAGE_BOUND"
    IV_OUT_OF_BOUNDS = "IV_OUT_OF_BOUNDS"
    IV_CONVERGENCE_FAILED = "IV_CONVERGENCE_FAILED"


@dataclass(frozen=True, slots=True)
class OptionValuationInput:
    contract_type: ContractType
    spot: Decimal
    strike: Decimal
    model_mark: Decimal
    time_to_expiration_years: float
    risk_free_rate: float
    dividend_yield: float

    def __post_init__(self) -> None:
        for value, name in (
            (self.spot, "spot"),
            (self.strike, "strike"),
            (self.model_mark, "model_mark"),
        ):
            if type(value) is not Decimal:
                raise TypeError(f"{name} must be Decimal")
        for value, name in (
            (self.time_to_expiration_years, "time_to_expiration_years"),
            (self.risk_free_rate, "risk_free_rate"),
            (self.dividend_yield, "dividend_yield"),
        ):
            if type(value) is not float or not math.isfinite(value):
                raise TypeError(f"{name} must be a finite float")


@dataclass(frozen=True, slots=True)
class LocalGreeksResult:
    converged: bool
    local_iv: float | None
    local_delta: float | None
    local_gamma: float | None
    local_theta_per_day: float | None
    local_vega_per_vol_point: float | None
    local_rho_per_rate_point: float | None
    solver: IvSolver | None
    iteration_count: int
    price_error: float | None
    failure_reason: IvFailureReason | None


def solve_local_greeks(
    inputs: tuple[OptionValuationInput, ...],
    *,
    minimum_iv: float = 0.01,
    maximum_iv: float = 5.0,
    newton_iterations: int = 20,
    price_error_tolerance: float = 1e-6,
    minimum_vega: float = 1e-8,
    use_brent_fallback: bool = True,
) -> tuple[LocalGreeksResult, ...]:
    if not inputs:
        return ()
    if minimum_iv <= 0 or maximum_iv <= minimum_iv:
        raise ValueError("IV bounds are invalid")
    count = len(inputs)
    spot = np.asarray([float(item.spot) for item in inputs], dtype=np.float64)
    strike = np.asarray([float(item.strike) for item in inputs], dtype=np.float64)
    mark = np.asarray([float(item.model_mark) for item in inputs], dtype=np.float64)
    maturity = np.asarray(
        [item.time_to_expiration_years for item in inputs], dtype=np.float64
    )
    rate = np.asarray([item.risk_free_rate for item in inputs], dtype=np.float64)
    dividend = np.asarray([item.dividend_yield for item in inputs], dtype=np.float64)
    is_call = np.asarray(
        [item.contract_type is ContractType.CALL for item in inputs], dtype=np.bool_
    )
    finite = (
        np.isfinite(spot)
        & np.isfinite(strike)
        & np.isfinite(mark)
        & np.isfinite(maturity)
        & np.isfinite(rate)
        & np.isfinite(dividend)
    )
    valid = finite & (spot > 0) & (strike > 0) & (mark > 0) & (maturity > 0)
    failures: list[IvFailureReason | None] = [None] * count
    for index in np.flatnonzero(~valid):
        failures[int(index)] = IvFailureReason.INVALID_INPUT

    safe_spot = np.where(valid, spot, 1.0)
    safe_strike = np.where(valid, strike, 1.0)
    safe_maturity = np.where(valid, maturity, 1.0)
    discounted_spot = safe_spot * np.exp(-dividend * safe_maturity)
    discounted_strike = safe_strike * np.exp(-rate * safe_maturity)
    lower = np.where(
        is_call,
        np.maximum(discounted_spot - discounted_strike, 0.0),
        np.maximum(discounted_strike - discounted_spot, 0.0),
    )
    upper = np.where(is_call, discounted_spot, discounted_strike)
    below = valid & (mark < lower - price_error_tolerance)
    above = valid & (mark > upper + price_error_tolerance)
    for index in np.flatnonzero(below):
        failures[int(index)] = IvFailureReason.BELOW_NO_ARBITRAGE_BOUND
    for index in np.flatnonzero(above):
        failures[int(index)] = IvFailureReason.ABOVE_NO_ARBITRAGE_BOUND
    valid &= ~below & ~above

    initial = 0.2 + np.abs(np.log(safe_spot / safe_strike)) / np.sqrt(safe_maturity)
    volatility = np.clip(initial, minimum_iv, maximum_iv).astype(np.float64)
    converged = np.zeros(count, dtype=np.bool_)
    iteration_counts = np.zeros(count, dtype=np.int64)
    solvers: list[IvSolver | None] = [None] * count
    active = valid.copy()

    for iteration in range(1, newton_iterations + 1):
        if not np.any(active):
            break
        prices, vegas = _price_and_vega(
            safe_spot,
            safe_strike,
            safe_maturity,
            rate,
            dividend,
            volatility,
            is_call,
        )
        errors = prices - mark
        iteration_counts[active] = iteration
        newly_converged = active & (np.abs(errors) <= price_error_tolerance)
        converged |= newly_converged
        for index in np.flatnonzero(newly_converged):
            solvers[int(index)] = IvSolver.NEWTON
        active &= ~newly_converged
        can_step = active & np.isfinite(vegas) & (np.abs(vegas) >= minimum_vega)
        volatility[can_step] = np.clip(
            volatility[can_step] - errors[can_step] / vegas[can_step],
            minimum_iv,
            maximum_iv,
        )
        active = can_step

    if use_brent_fallback:
        for index in np.flatnonzero(valid & ~converged):
            index = int(index)

            def objective(volatility_value: float) -> float:
                return _scalar_price(
                    spot[index],
                    strike[index],
                    maturity[index],
                    rate[index],
                    dividend[index],
                    volatility_value,
                    bool(is_call[index]),
                ) - mark[index]

            low_error = objective(minimum_iv)
            high_error = objective(maximum_iv)
            if abs(low_error) <= price_error_tolerance:
                root = minimum_iv
                brent_iterations = 0
            elif abs(high_error) <= price_error_tolerance:
                root = maximum_iv
                brent_iterations = 0
            elif low_error * high_error > 0:
                failures[index] = IvFailureReason.IV_OUT_OF_BOUNDS
                continue
            else:
                try:
                    root, result = brentq(
                        objective,
                        minimum_iv,
                        maximum_iv,
                        xtol=1e-12,
                        rtol=1e-12,
                        maxiter=100,
                        full_output=True,
                    )
                    brent_iterations = result.iterations
                except (ValueError, RuntimeError, OverflowError):
                    failures[index] = IvFailureReason.IV_CONVERGENCE_FAILED
                    continue
            volatility[index] = root
            converged[index] = True
            solvers[index] = IvSolver.BRENT
            iteration_counts[index] += brent_iterations

    results: list[LocalGreeksResult] = []
    for index in range(count):
        if not converged[index]:
            if failures[index] is None:
                failures[index] = IvFailureReason.IV_CONVERGENCE_FAILED
            results.append(
                LocalGreeksResult(
                    converged=False,
                    local_iv=None,
                    local_delta=None,
                    local_gamma=None,
                    local_theta_per_day=None,
                    local_vega_per_vol_point=None,
                    local_rho_per_rate_point=None,
                    solver=None,
                    iteration_count=int(iteration_counts[index]),
                    price_error=None,
                    failure_reason=failures[index],
                )
            )
            continue
        price, delta, gamma, theta, vega, rho = _scalar_price_and_greeks(
            spot[index],
            strike[index],
            maturity[index],
            rate[index],
            dividend[index],
            volatility[index],
            bool(is_call[index]),
        )
        results.append(
            LocalGreeksResult(
                converged=True,
                local_iv=float(volatility[index]),
                local_delta=float(delta),
                local_gamma=float(gamma),
                local_theta_per_day=float(theta / 365.0),
                local_vega_per_vol_point=float(vega * 0.01),
                local_rho_per_rate_point=float(rho * 0.01),
                solver=solvers[index],
                iteration_count=int(iteration_counts[index]),
                price_error=float(abs(price - mark[index])),
                failure_reason=None,
            )
        )
    return tuple(results)


def convergence_fraction(results: tuple[LocalGreeksResult, ...]) -> float:
    if not results:
        return 0.0
    return sum(result.converged for result in results) / len(results)


def price_and_greeks(
    contract_type: ContractType,
    *,
    spot: float,
    strike: float,
    maturity: float,
    rate: float,
    dividend: float,
    volatility: float,
) -> tuple[float, float, float, float, float, float]:
    values = (spot, strike, maturity, rate, dividend, volatility)
    if any(not math.isfinite(value) for value in values):
        raise ValueError("valuation inputs must be finite")
    if spot <= 0 or strike <= 0 or maturity <= 0 or volatility <= 0:
        raise ValueError("spot, strike, maturity, and volatility must be positive")
    price, delta, gamma, theta, vega, rho = _scalar_price_and_greeks(
        spot,
        strike,
        maturity,
        rate,
        dividend,
        volatility,
        contract_type is ContractType.CALL,
    )
    return price, delta, gamma, theta / 365.0, vega * 0.01, rho * 0.01


def passes_convergence_gate(
    results: tuple[LocalGreeksResult, ...],
    minimum_fraction: float = 0.95,
) -> bool:
    return bool(results) and convergence_fraction(results) >= minimum_fraction


def _price_and_vega(spot, strike, maturity, rate, dividend, volatility, is_call):
    root_time = np.sqrt(maturity)
    d1 = (
        np.log(spot / strike)
        + (rate - dividend + 0.5 * volatility * volatility) * maturity
    ) / (volatility * root_time)
    d2 = d1 - volatility * root_time
    discounted_spot = spot * np.exp(-dividend * maturity)
    discounted_strike = strike * np.exp(-rate * maturity)
    call_price = discounted_spot * ndtr(d1) - discounted_strike * ndtr(d2)
    put_price = discounted_strike * ndtr(-d2) - discounted_spot * ndtr(-d1)
    density = np.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
    vega = discounted_spot * density * root_time
    return np.where(is_call, call_price, put_price), vega


def _scalar_price(spot, strike, maturity, rate, dividend, volatility, is_call):
    return _scalar_price_and_greeks(
        spot, strike, maturity, rate, dividend, volatility, is_call
    )[0]


def _scalar_price_and_greeks(
    spot,
    strike,
    maturity,
    rate,
    dividend,
    volatility,
    is_call,
):
    root_time = math.sqrt(maturity)
    d1 = (
        math.log(spot / strike)
        + (rate - dividend + 0.5 * volatility * volatility) * maturity
    ) / (volatility * root_time)
    d2 = d1 - volatility * root_time
    normal_d1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
    discounted_spot = spot * math.exp(-dividend * maturity)
    discounted_strike = strike * math.exp(-rate * maturity)
    if is_call:
        price = discounted_spot * ndtr(d1) - discounted_strike * ndtr(d2)
        delta = math.exp(-dividend * maturity) * ndtr(d1)
        theta = (
            -discounted_spot * normal_d1 * volatility / (2.0 * root_time)
            - rate * discounted_strike * ndtr(d2)
            + dividend * discounted_spot * ndtr(d1)
        )
        rho = maturity * discounted_strike * ndtr(d2)
    else:
        price = discounted_strike * ndtr(-d2) - discounted_spot * ndtr(-d1)
        delta = math.exp(-dividend * maturity) * (ndtr(d1) - 1.0)
        theta = (
            -discounted_spot * normal_d1 * volatility / (2.0 * root_time)
            + rate * discounted_strike * ndtr(-d2)
            - dividend * discounted_spot * ndtr(-d1)
        )
        rho = -maturity * discounted_strike * ndtr(-d2)
    gamma = math.exp(-dividend * maturity) * normal_d1 / (
        spot * volatility * root_time
    )
    vega = discounted_spot * normal_d1 * root_time
    return price, delta, gamma, theta, vega, rho