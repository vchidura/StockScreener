from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from statistics import median

import numpy as np


@dataclass(frozen=True, slots=True)
class OiWallInput:
    strike: Decimal
    open_interest: int

    def __post_init__(self) -> None:
        if self.strike <= 0 or self.open_interest < 0:
            raise ValueError("OI wall strike and open interest must be non-negative")


@dataclass(frozen=True, slots=True)
class OiWallCluster:
    member_strikes: tuple[Decimal, ...]
    center_strike: Decimal
    maximum_robust_z: float
    total_open_interest: int
    distance_to_spot_fraction: float


def detect_oi_wall_clusters(
    rows: tuple[OiWallInput, ...],
    spot: Decimal,
    *,
    percentile: float = 0.9,
    minimum_robust_z: float = 2.5,
    maximum_clusters: int = 5,
    allow_zero_mad_fallback: bool = False,
) -> tuple[OiWallCluster, ...]:
    if not rows:
        return ()
    if spot <= 0:
        raise ValueError("spot must be positive")
    ordered = sorted(rows, key=lambda row: row.strike)
    open_interest = np.asarray(
        [row.open_interest for row in ordered], dtype=np.float64
    )
    median_oi = float(median(open_interest.tolist()))
    mad = float(median(np.abs(open_interest - median_oi).tolist()))
    threshold = float(np.quantile(open_interest, percentile))
    if mad == 0 and not allow_zero_mad_fallback:
        return ()
    if mad == 0:
        robust_z = np.where(open_interest > median_oi, np.inf, 0.0)
    else:
        robust_z = 0.6744897501960817 * (open_interest - median_oi) / mad
    wall_indexes = {
        index
        for index, value in enumerate(open_interest)
        if value > threshold and robust_z[index] >= minimum_robust_z
    }
    if not wall_indexes:
        return ()
    index_clusters: list[list[int]] = []
    for index in sorted(wall_indexes):
        if index_clusters and index == index_clusters[-1][-1] + 1:
            index_clusters[-1].append(index)
        else:
            index_clusters.append([index])
    clusters: list[OiWallCluster] = []
    for indexes in index_clusters:
        members = tuple(ordered[index] for index in indexes)
        total_oi = sum(member.open_interest for member in members)
        center = sum(
            (member.strike * member.open_interest for member in members),
            Decimal("0"),
        ) / Decimal(total_oi)
        clusters.append(
            OiWallCluster(
                member_strikes=tuple(member.strike for member in members),
                center_strike=center,
                maximum_robust_z=max(float(robust_z[index]) for index in indexes),
                total_open_interest=total_oi,
                distance_to_spot_fraction=float(abs(center - spot) / spot),
            )
        )
    clusters.sort(
        key=lambda cluster: (
            -cluster.maximum_robust_z,
            -cluster.total_open_interest,
            cluster.distance_to_spot_fraction,
            cluster.center_strike,
        )
    )
    return tuple(clusters[:maximum_clusters])