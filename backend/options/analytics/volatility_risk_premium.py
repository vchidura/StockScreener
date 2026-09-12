"""Matched-maturity implied volatility and variance-risk-premium calculations."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from options.domain import ContractType


@dataclass(frozen=True, slots=True)
class ContractIvObservation:
    expiration_date: date
    maturity_years: float
    contract_type: ContractType
    strike: Decimal
    spot: Decimal
    implied_volatility: float

    def __post_init__(self) -> None:
        if self.maturity_years <= 0 or not math.isfinite(self.maturity_years):
            raise ValueError("maturity_years must be positive and finite")
        if self.strike <= 0 or self.spot <= 0:
            raise ValueError("strike and spot must be positive")
        if self.implied_volatility <= 0 or not math.isfinite(self.implied_volatility):
            raise ValueError("implied_volatility must be positive and finite")


@dataclass(frozen=True, slots=True)
class ExpirationIvPoint:
    expiration_date: date
    maturity_years: float
    atm_implied_volatility: float
    contract_count: int
    call_count: int
    put_count: int
    maximum_absolute_log_moneyness: float


@dataclass(frozen=True, slots=True)
class MatchedHorizonIv:
    horizon_sessions: int
    target_maturity_years: float
    implied_volatility: float
    lower_expiration: date
    upper_expiration: date
    lower_maturity_years: float
    upper_maturity_years: float
    source_contract_count: int


@dataclass(frozen=True, slots=True)
class MatchedMaturityIv:
    target_maturity_years: float
    implied_volatility: float
    lower_expiration: date
    upper_expiration: date
    lower_maturity_years: float
    upper_maturity_years: float
    source_contract_count: int


@dataclass(frozen=True, slots=True)
class VarianceRiskPremiumObservation:
    implied_volatility: float
    forecast_realized_volatility: float
    actual_realized_volatility: float

    @property
    def forecast_variance_premium(self) -> float:
        return self.implied_volatility**2 - self.forecast_realized_volatility**2

    @property
    def realized_variance_premium(self) -> float:
        return self.implied_volatility**2 - self.actual_realized_volatility**2

    @property
    def forecast_richness_ratio(self) -> float:
        return self.implied_volatility / self.forecast_realized_volatility

    @property
    def realized_richness_ratio(self) -> float:
        return self.implied_volatility / self.actual_realized_volatility


def summarize_expiration_atm_iv(
    observations: tuple[ContractIvObservation, ...],
    *,
    maximum_absolute_log_moneyness: float = 0.03,
    contracts_per_side: int = 2,
) -> ExpirationIvPoint | None:
    """Build one robust ATM point from the nearest calls and puts at an expiration."""
    if not observations:
        return None
    if maximum_absolute_log_moneyness <= 0:
        raise ValueError("maximum_absolute_log_moneyness must be positive")
    if contracts_per_side < 1:
        raise ValueError("contracts_per_side must be positive")

    expirations = {row.expiration_date for row in observations}
    if len(expirations) != 1:
        raise ValueError("observations must share one expiration")

    selected: list[tuple[float, ContractIvObservation]] = []
    side_counts: dict[ContractType, int] = {}
    for contract_type in (ContractType.CALL, ContractType.PUT):
        ranked = sorted(
            (
                (abs(math.log(float(row.strike / row.spot))), row)
                for row in observations
                if row.contract_type is contract_type
            ),
            key=lambda item: (item[0], item[1].strike),
        )
        eligible = [item for item in ranked if item[0] <= maximum_absolute_log_moneyness]
        chosen = eligible[:contracts_per_side]
        if not chosen:
            return None
        selected.extend(chosen)
        side_counts[contract_type] = len(chosen)

    maturities = [row.maturity_years for _, row in selected]
    maturity = statistics.median(maturities)
    if max(maturities) - min(maturities) > 1e-9:
        raise ValueError("one expiration produced inconsistent maturities")

    return ExpirationIvPoint(
        expiration_date=next(iter(expirations)),
        maturity_years=maturity,
        atm_implied_volatility=statistics.median(
            row.implied_volatility for _, row in selected
        ),
        contract_count=len(selected),
        call_count=side_counts[ContractType.CALL],
        put_count=side_counts[ContractType.PUT],
        maximum_absolute_log_moneyness=max(distance for distance, _ in selected),
    )


def interpolate_total_variance(
    points: tuple[ExpirationIvPoint, ...],
    horizon_sessions: int,
    *,
    trading_days_per_year: int = 252,
) -> MatchedHorizonIv | None:
    """Interpolate total variance between expirations without extrapolating."""
    if horizon_sessions < 1 or trading_days_per_year < 1:
        raise ValueError("horizon and trading_days_per_year must be positive")
    target = horizon_sessions / trading_days_per_year
    matched = interpolate_total_variance_at_maturity(points, target)
    if matched is None:
        return None
    return MatchedHorizonIv(
        horizon_sessions=horizon_sessions,
        target_maturity_years=matched.target_maturity_years,
        implied_volatility=matched.implied_volatility,
        lower_expiration=matched.lower_expiration,
        upper_expiration=matched.upper_expiration,
        lower_maturity_years=matched.lower_maturity_years,
        upper_maturity_years=matched.upper_maturity_years,
        source_contract_count=matched.source_contract_count,
    )


def interpolate_total_variance_at_maturity(
    points: tuple[ExpirationIvPoint, ...],
    target_maturity_years: float,
) -> MatchedMaturityIv | None:
    """Interpolate total variance at an explicit year-fraction maturity."""
    if target_maturity_years <= 0 or not math.isfinite(target_maturity_years):
        raise ValueError("target maturity must be positive and finite")
    target = target_maturity_years
    ordered = sorted(points, key=lambda point: point.maturity_years)
    if not ordered or target < ordered[0].maturity_years or target > ordered[-1].maturity_years:
        return None

    lower = max(
        (point for point in ordered if point.maturity_years <= target),
        key=lambda point: point.maturity_years,
    )
    upper = min(
        (point for point in ordered if point.maturity_years >= target),
        key=lambda point: point.maturity_years,
    )
    if math.isclose(lower.maturity_years, upper.maturity_years, abs_tol=1e-12):
        implied = lower.atm_implied_volatility
    else:
        lower_variance = lower.atm_implied_volatility**2 * lower.maturity_years
        upper_variance = upper.atm_implied_volatility**2 * upper.maturity_years
        weight = (
            (target - lower.maturity_years)
            / (upper.maturity_years - lower.maturity_years)
        )
        target_variance = lower_variance + weight * (upper_variance - lower_variance)
        if target_variance <= 0:
            return None
        implied = math.sqrt(target_variance / target)

    return MatchedMaturityIv(
        target_maturity_years=target,
        implied_volatility=implied,
        lower_expiration=lower.expiration_date,
        upper_expiration=upper.expiration_date,
        lower_maturity_years=lower.maturity_years,
        upper_maturity_years=upper.maturity_years,
        source_contract_count=lower.contract_count + (
            0 if lower is upper else upper.contract_count
        ),
    )


def calculate_variance_risk_premium(
    implied_volatility: float,
    forecast_realized_volatility: float,
    actual_realized_volatility: float,
) -> VarianceRiskPremiumObservation:
    values = (
        implied_volatility,
        forecast_realized_volatility,
        actual_realized_volatility,
    )
    if any(value <= 0 or not math.isfinite(value) for value in values):
        raise ValueError("volatility inputs must be positive and finite")
    return VarianceRiskPremiumObservation(*values)
