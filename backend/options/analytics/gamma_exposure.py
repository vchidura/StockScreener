"""Chain-level gamma exposure (GEX): unsigned per-strike facts plus a versioned
dealer-positioning interpretation.

Open interest states how many contracts exist, never who is long or short. The
per-strike aggregates here are measurements; the sign applied to them is a
hypothesis owned by `DealerConvention` and is deliberately kept separable so a
convention can be revised or tested without invalidating stored facts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import numpy as np

from options.domain import (
    ContractType,
    DealerConvention,
    GammaRegime,
    GammaScope,
    VolatilityAssumption,
)

from .greeks import black_scholes_gamma


@dataclass(frozen=True, slots=True)
class GammaExposureInput:
    contract_id: int
    contract_type: ContractType
    expiration_date: date
    strike: Decimal
    open_interest: int
    local_iv: float
    time_to_expiration_years: float
    risk_free_rate: float
    dividend_yield: float

    def __post_init__(self) -> None:
        if self.strike <= 0:
            raise ValueError("gamma exposure strike must be positive")
        if self.open_interest < 0:
            raise ValueError("gamma exposure open interest must not be negative")


@dataclass(frozen=True, slots=True)
class StrikeGammaExposure:
    """Unsigned per-strike aggregation. Calls and puts stay separate on purpose."""

    strike: Decimal
    call_open_interest: int
    put_open_interest: int
    call_contract_count: int
    put_contract_count: int
    call_gamma_shares_per_point: float
    put_gamma_shares_per_point: float
    call_gamma_notional_per_percent: float
    put_gamma_notional_per_percent: float


@dataclass(frozen=True, slots=True)
class GammaFlipPoint:
    flip_spot: Decimal | None
    regime_at_spot: GammaRegime
    searched_low_spot: Decimal
    searched_high_spot: Decimal
    grid_points: int
    sign_change_count: int
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GammaProfile:
    spot: Decimal
    convention: DealerConvention
    volatility_assumption: VolatilityAssumption
    shares_per_contract: int
    strikes: tuple[StrikeGammaExposure, ...]
    net_gamma_shares_per_point: float
    net_gamma_notional_per_percent: float
    absolute_gamma_notional_per_percent: float
    call_gamma_notional_per_percent: float
    put_gamma_notional_per_percent: float
    contributing_contract_count: int
    eligible_contract_count: int
    coverage_fraction: float
    peak_gamma_strike: Decimal | None
    flip: GammaFlipPoint
    quality_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ScopedGammaProfile:
    scope: GammaScope
    profile: GammaProfile


@dataclass(frozen=True, slots=True)
class GammaWall:
    """A strike carrying enough gamma that dealer hedging concentrates there."""

    strike: Decimal
    gamma_notional_per_percent: float
    gamma_share: float
    distance_to_spot_fraction: float


def detect_gamma_walls(
    profile: GammaProfile,
    *,
    minimum_share: float,
    maximum_distance_fraction: float | None = None,
) -> tuple[GammaWall, ...]:
    """Strikes holding at least `minimum_share` of the chain's unsigned gamma.

    Share is measured against the unsigned total so the result does not depend on
    the dealer convention.
    """
    if not 0 < minimum_share <= 1:
        raise ValueError("minimum_share must be in (0, 1]")
    total = profile.absolute_gamma_notional_per_percent
    if total <= 0 or not profile.strikes:
        return ()
    spot = float(profile.spot)
    walls: list[GammaWall] = []
    for row in profile.strikes:
        notional = (
            row.call_gamma_notional_per_percent + row.put_gamma_notional_per_percent
        )
        share = notional / total
        if share < minimum_share:
            continue
        distance = abs(float(row.strike) - spot) / spot
        if maximum_distance_fraction is not None and distance > maximum_distance_fraction:
            continue
        walls.append(
            GammaWall(
                strike=row.strike,
                gamma_notional_per_percent=notional,
                gamma_share=share,
                distance_to_spot_fraction=distance,
            )
        )
    walls.sort(key=lambda wall: (-wall.gamma_share, wall.distance_to_spot_fraction, wall.strike))
    return tuple(walls)


def scope_contains(scope: GammaScope, calendar_dte: int) -> bool:
    if scope is GammaScope.TOTAL:
        return True
    if scope is GammaScope.ZERO_DTE:
        return calendar_dte == 0
    if scope is GammaScope.WEEKLY:
        return 1 <= calendar_dte <= 14
    return calendar_dte >= 15


def build_gamma_profile(
    rows: tuple[GammaExposureInput, ...],
    spot: Decimal,
    *,
    convention: DealerConvention,
    scope: GammaScope = GammaScope.TOTAL,
    shares_per_contract: int = 100,
    minimum_open_interest: int = 1,
    maximum_dte: int | None = None,
    market_date: date | None = None,
    minimum_contracts_for_profile: int = 20,
    volatility_assumption: VolatilityAssumption = VolatilityAssumption.STICKY_STRIKE,
    flip_search_fraction: float = 0.2,
    flip_grid_points: int = 81,
) -> GammaProfile:
    if spot <= 0:
        raise ValueError("spot must be positive")
    if shares_per_contract <= 0:
        raise ValueError("shares_per_contract must be positive")
    if not 0 < flip_search_fraction < 1:
        raise ValueError("flip_search_fraction must be in (0, 1)")
    if flip_grid_points < 3 or flip_grid_points % 2 == 0:
        raise ValueError("flip_grid_points must be odd and at least 3")
    if maximum_dte is not None and market_date is None:
        raise ValueError("maximum_dte requires market_date")
    if scope is not GammaScope.TOTAL and market_date is None:
        raise ValueError("a non-total scope requires market_date")

    reasons: list[str] = []
    eligible: list[GammaExposureInput] = []
    for row in rows:
        if row.open_interest < minimum_open_interest:
            continue
        if row.time_to_expiration_years <= 0:
            continue
        if row.local_iv <= 0:
            continue
        if market_date is not None:
            calendar_dte = (row.expiration_date - market_date).days
            if maximum_dte is not None and calendar_dte > maximum_dte:
                continue
            if not scope_contains(scope, calendar_dte):
                continue
        eligible.append(row)

    if not eligible:
        reasons.append("NO_ELIGIBLE_GAMMA_CONTRACT")
        return _empty_profile(
            spot,
            convention,
            volatility_assumption,
            shares_per_contract,
            len(rows),
            tuple(reasons),
        )

    spot_value = float(spot)
    gamma = black_scholes_gamma(
        np.full(len(eligible), spot_value),
        np.asarray([float(row.strike) for row in eligible]),
        np.asarray([row.time_to_expiration_years for row in eligible]),
        np.asarray([row.risk_free_rate for row in eligible]),
        np.asarray([row.dividend_yield for row in eligible]),
        np.asarray([row.local_iv for row in eligible]),
    )
    finite = np.isfinite(gamma)
    if not bool(finite.any()):
        reasons.append("GAMMA_RECOMPUTATION_FAILED")
        return _empty_profile(
            spot,
            convention,
            volatility_assumption,
            shares_per_contract,
            len(rows),
            tuple(reasons),
        )
    if not bool(finite.all()):
        reasons.append("PARTIAL_GAMMA_RECOMPUTATION")

    contributing = [row for row, ok in zip(eligible, finite) if ok]
    contributing_gamma = gamma[finite]
    if len(contributing) < minimum_contracts_for_profile:
        reasons.append("INSUFFICIENT_GAMMA_CONTRACTS")

    notional_scale = spot_value * spot_value * 0.01 * shares_per_contract
    share_scale = float(shares_per_contract)

    buckets: dict[Decimal, dict[str, float | int]] = {}
    for row, contract_gamma in zip(contributing, contributing_gamma):
        bucket = buckets.setdefault(
            row.strike,
            {
                "call_oi": 0,
                "put_oi": 0,
                "call_count": 0,
                "put_count": 0,
                "call_shares": 0.0,
                "put_shares": 0.0,
                "call_notional": 0.0,
                "put_notional": 0.0,
            },
        )
        shares = float(contract_gamma) * row.open_interest * share_scale
        notional = float(contract_gamma) * row.open_interest * notional_scale
        if row.contract_type is ContractType.CALL:
            bucket["call_oi"] += row.open_interest
            bucket["call_count"] += 1
            bucket["call_shares"] += shares
            bucket["call_notional"] += notional
        else:
            bucket["put_oi"] += row.open_interest
            bucket["put_count"] += 1
            bucket["put_shares"] += shares
            bucket["put_notional"] += notional

    strikes = tuple(
        StrikeGammaExposure(
            strike=strike,
            call_open_interest=int(bucket["call_oi"]),
            put_open_interest=int(bucket["put_oi"]),
            call_contract_count=int(bucket["call_count"]),
            put_contract_count=int(bucket["put_count"]),
            call_gamma_shares_per_point=float(bucket["call_shares"]),
            put_gamma_shares_per_point=float(bucket["put_shares"]),
            call_gamma_notional_per_percent=float(bucket["call_notional"]),
            put_gamma_notional_per_percent=float(bucket["put_notional"]),
        )
        for strike, bucket in sorted(buckets.items())
    )

    call_sign, put_sign = dealer_signs(convention)
    net_shares = sum(
        call_sign * row.call_gamma_shares_per_point
        + put_sign * row.put_gamma_shares_per_point
        for row in strikes
    )
    net_notional = sum(
        call_sign * row.call_gamma_notional_per_percent
        + put_sign * row.put_gamma_notional_per_percent
        for row in strikes
    )
    call_notional_total = sum(row.call_gamma_notional_per_percent for row in strikes)
    put_notional_total = sum(row.put_gamma_notional_per_percent for row in strikes)
    peak_strike = max(
        strikes,
        key=lambda row: (
            row.call_gamma_notional_per_percent + row.put_gamma_notional_per_percent,
            row.strike,
        ),
    ).strike

    flip = solve_gamma_flip(
        tuple(contributing),
        spot,
        convention=convention,
        shares_per_contract=shares_per_contract,
        search_fraction=flip_search_fraction,
        grid_points=flip_grid_points,
    )

    return GammaProfile(
        spot=spot,
        convention=convention,
        volatility_assumption=volatility_assumption,
        shares_per_contract=shares_per_contract,
        strikes=strikes,
        net_gamma_shares_per_point=net_shares,
        net_gamma_notional_per_percent=net_notional,
        absolute_gamma_notional_per_percent=call_notional_total + put_notional_total,
        call_gamma_notional_per_percent=call_notional_total,
        put_gamma_notional_per_percent=put_notional_total,
        contributing_contract_count=len(contributing),
        eligible_contract_count=len(rows),
        coverage_fraction=len(contributing) / len(rows) if rows else 0.0,
        peak_gamma_strike=peak_strike,
        flip=flip,
        quality_reasons=tuple(dict.fromkeys(reasons)),
    )


def dealer_signs(convention: DealerConvention) -> tuple[float, float]:
    """Return (call_sign, put_sign) for the assumed dealer side."""
    if convention is DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS:
        return 1.0, -1.0
    return -1.0, 1.0


def solve_gamma_flip(
    rows: tuple[GammaExposureInput, ...],
    spot: Decimal,
    *,
    convention: DealerConvention,
    shares_per_contract: int = 100,
    search_fraction: float = 0.2,
    grid_points: int = 81,
) -> GammaFlipPoint:
    """Locate where signed net gamma crosses zero.

    Gamma is itself a function of spot, so it is recomputed at every grid level
    rather than re-weighted. Each contract keeps its own IV (sticky strike).
    """
    spot_value = float(spot)
    low = spot_value * (1.0 - search_fraction)
    high = spot_value * (1.0 + search_fraction)
    searched_low = Decimal(str(low))
    searched_high = Decimal(str(high))
    if not rows:
        return GammaFlipPoint(
            flip_spot=None,
            regime_at_spot=GammaRegime.UNDETERMINED,
            searched_low_spot=searched_low,
            searched_high_spot=searched_high,
            grid_points=grid_points,
            sign_change_count=0,
            reasons=("NO_GAMMA_CONTRACTS",),
        )

    grid = np.linspace(low, high, grid_points, dtype=np.float64)
    strike = np.asarray([float(row.strike) for row in rows])
    maturity = np.asarray([row.time_to_expiration_years for row in rows])
    rate = np.asarray([row.risk_free_rate for row in rows])
    dividend = np.asarray([row.dividend_yield for row in rows])
    volatility = np.asarray([row.local_iv for row in rows])
    call_sign, put_sign = dealer_signs(convention)
    signed_oi = np.asarray(
        [
            (call_sign if row.contract_type is ContractType.CALL else put_sign)
            * row.open_interest
            for row in rows
        ]
    )

    curve = np.empty(grid_points, dtype=np.float64)
    for index, level in enumerate(grid):
        gamma = black_scholes_gamma(
            np.full(len(rows), level),
            strike,
            maturity,
            rate,
            dividend,
            volatility,
        )
        gamma = np.nan_to_num(gamma, nan=0.0, posinf=0.0, neginf=0.0)
        curve[index] = float(
            np.sum(gamma * signed_oi) * shares_per_contract * level * level * 0.01
        )

    at_spot = float(np.interp(spot_value, grid, curve))
    if at_spot > 0:
        regime = GammaRegime.POSITIVE_GAMMA
    elif at_spot < 0:
        regime = GammaRegime.NEGATIVE_GAMMA
    else:
        regime = GammaRegime.UNDETERMINED

    if not np.any(curve != 0.0):
        return GammaFlipPoint(
            flip_spot=None,
            regime_at_spot=GammaRegime.UNDETERMINED,
            searched_low_spot=searched_low,
            searched_high_spot=searched_high,
            grid_points=grid_points,
            sign_change_count=0,
            reasons=("FLAT_ZERO_GAMMA_CURVE",),
        )

    crossings: list[float] = []
    for index in range(grid_points - 1):
        left, right = curve[index], curve[index + 1]
        if left == 0.0:
            crossings.append(float(grid[index]))
        elif left * right < 0:
            span = right - left
            crossings.append(float(grid[index] - left * (grid[index + 1] - grid[index]) / span))
    if curve[-1] == 0.0:
        crossings.append(float(grid[-1]))

    reasons: list[str] = []
    if not crossings:
        reasons.append("NO_FLIP_IN_SEARCH_RANGE")
        flip_spot = None
    else:
        flip_spot = Decimal(str(min(crossings, key=lambda value: abs(value - spot_value))))
        if len(crossings) > 1:
            reasons.append("MULTIPLE_FLIP_CANDIDATES")

    return GammaFlipPoint(
        flip_spot=flip_spot,
        regime_at_spot=regime,
        searched_low_spot=searched_low,
        searched_high_spot=searched_high,
        grid_points=grid_points,
        sign_change_count=len(crossings),
        reasons=tuple(reasons),
    )


def _empty_profile(
    spot: Decimal,
    convention: DealerConvention,
    volatility_assumption: VolatilityAssumption,
    shares_per_contract: int,
    eligible_contract_count: int,
    reasons: tuple[str, ...],
) -> GammaProfile:
    return GammaProfile(
        spot=spot,
        convention=convention,
        volatility_assumption=volatility_assumption,
        shares_per_contract=shares_per_contract,
        strikes=(),
        net_gamma_shares_per_point=0.0,
        net_gamma_notional_per_percent=0.0,
        absolute_gamma_notional_per_percent=0.0,
        call_gamma_notional_per_percent=0.0,
        put_gamma_notional_per_percent=0.0,
        contributing_contract_count=0,
        eligible_contract_count=eligible_contract_count,
        coverage_fraction=0.0,
        peak_gamma_strike=None,
        flip=GammaFlipPoint(
            flip_spot=None,
            regime_at_spot=GammaRegime.UNDETERMINED,
            searched_low_spot=spot,
            searched_high_spot=spot,
            grid_points=0,
            sign_change_count=0,
            reasons=("NO_GAMMA_CONTRACTS",),
        ),
        quality_reasons=reasons,
    )
