"""Volatility smile fitting shared by the live detector and offline research.

The fit is an unweighted quadratic in log-moneyness and the outlier statistic is a
MAD-scaled residual. Both the strategy engine and the persistence study consume
these functions so a study can never measure a different rule than production runs.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import numpy as np

from options.domain import ContractType

# A residual dispersion below this is floating-point noise from an essentially exact
# fit, not market structure. Dividing by it manufactures large robust scores from
# nothing, so such groups are treated as having no measurable distortion. The value is
# in implied-volatility units and sits far below any economically meaningful spread.
MINIMUM_RESIDUAL_MAD = 1e-9


@dataclass(frozen=True, slots=True)
class SmileInput:
    contract_id: int
    contract_type: ContractType
    expiration_date: date
    strike: Decimal
    spot: Decimal
    local_iv: float


@dataclass(frozen=True, slots=True)
class SmileResidual:
    contract_id: int
    contract_type: ContractType
    expiration_date: date
    strike: Decimal
    index: int
    log_moneyness: float
    local_iv: float
    fitted_iv: float
    residual: float
    robust_z: float
    neighbors_consistent: bool
    is_edge: bool


@dataclass(frozen=True, slots=True)
class SmileGroupFit:
    expiration_date: date
    contract_type: ContractType
    input_count: int
    distinct_strike_count: int
    coefficients: tuple[float, float, float]
    residual_median: float
    mad: float
    residuals: tuple[SmileResidual, ...]


def coefficient_payload(coefficients: tuple[float, float, float]) -> dict[str, float]:
    quadratic, linear, intercept = coefficients
    return {"quadratic": quadratic, "linear": linear, "intercept": intercept}


def fit_smile_groups(
    rows: tuple[SmileInput, ...],
    *,
    minimum_strikes: int,
) -> tuple[SmileGroupFit, ...]:
    """Fit each expiration/type group that has enough strikes on both sides of spot."""
    groups: dict[tuple[date, ContractType], list[SmileInput]] = defaultdict(list)
    for row in rows:
        groups[(row.expiration_date, row.contract_type)].append(row)
    fits: list[SmileGroupFit] = []
    for (expiration_date, contract_type), members in sorted(
        groups.items(), key=lambda item: item[0]
    ):
        ordered = sorted(members, key=lambda row: (row.strike, row.contract_id))
        distinct = len({row.strike for row in ordered})
        if (
            distinct < minimum_strikes
            or not any(row.strike < row.spot for row in ordered)
            or not any(row.strike > row.spot for row in ordered)
        ):
            continue
        x = np.asarray(
            [np.log(float(row.strike / row.spot)) for row in ordered], dtype=np.float64
        )
        y = np.asarray([float(row.local_iv) for row in ordered], dtype=np.float64)
        coefficients = np.polyfit(x, y, 2)
        fitted = np.polyval(coefficients, x)
        residuals = y - fitted
        residual_median = float(np.median(residuals))
        mad = float(np.median(np.abs(residuals - residual_median)))
        if mad < MINIMUM_RESIDUAL_MAD:
            continue
        robust = (residuals - residual_median) / (1.4826 * mad)
        entries: list[SmileResidual] = []
        for index, row in enumerate(ordered):
            score = float(robust[index])
            is_edge = index == 0 or index == len(ordered) - 1
            consistent = not is_edge and bool(
                np.sign(robust[index - 1]) == np.sign(score)
                or np.sign(robust[index + 1]) == np.sign(score)
            )
            entries.append(
                SmileResidual(
                    contract_id=row.contract_id,
                    contract_type=contract_type,
                    expiration_date=expiration_date,
                    strike=row.strike,
                    index=index,
                    log_moneyness=float(x[index]),
                    local_iv=float(y[index]),
                    fitted_iv=float(fitted[index]),
                    residual=float(residuals[index]),
                    robust_z=score,
                    neighbors_consistent=consistent,
                    is_edge=is_edge,
                )
            )
        fits.append(
            SmileGroupFit(
                expiration_date=expiration_date,
                contract_type=contract_type,
                input_count=len(ordered),
                distinct_strike_count=distinct,
                coefficients=(
                    float(coefficients[0]),
                    float(coefficients[1]),
                    float(coefficients[2]),
                ),
                residual_median=residual_median,
                mad=mad,
                residuals=tuple(entries),
            )
        )
    return tuple(fits)


def qualifying_distortions(
    fit: SmileGroupFit,
    *,
    minimum_absolute_robust_z: float,
) -> tuple[SmileResidual, ...]:
    """Non-edge residuals that clear the threshold and agree with a neighbour."""
    return tuple(
        entry
        for entry in fit.residuals
        if not entry.is_edge
        and entry.neighbors_consistent
        and abs(entry.robust_z) >= minimum_absolute_robust_z
    )
