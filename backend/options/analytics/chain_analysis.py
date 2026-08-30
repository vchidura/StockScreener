from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from enum import Enum
from statistics import median
from uuid import UUID

from options.domain import ContractType, OptionContractSnapshot, OptionExpirationAnalytics
from options.analytics.oi_walls import OiWallInput, detect_oi_wall_clusters


class ContractMoneyness(str, Enum):
    ITM = "ITM"
    ATM = "ATM"
    OTM = "OTM"


@dataclass(frozen=True, slots=True)
class ContractAnalysis:
    contract_id: int
    forward_price: Decimal
    log_forward_moneyness: float
    moneyness: ContractMoneyness
    distance_from_spot_fraction: float
    standard_deviation_distance: float | None
    volume_open_interest_ratio: float | None
    print_notional: Decimal | None
    premium_yield: float | None
    intrinsic_extrinsic_ratio: float | None
    quality_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ChainHealth:
    received_count: int
    retained_count: int
    catalog_coverage_fraction: float
    mark_alignment_fraction: float
    iv_convergence_fraction: float
    unknown_reference_fraction: float
    rejection_fraction: float
    status: str
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExpirationContractInput:
    contract_id: int
    contract_type: ContractType
    expiration_date: date
    strike: Decimal
    spot: Decimal
    time_to_expiration_years: float
    risk_free_rate: float
    dividend_yield: float
    local_iv: float
    local_delta: float
    day_volume: int | None
    open_interest: int | None


def analyze_contract(snapshot: OptionContractSnapshot) -> ContractAnalysis:
    forward = float(snapshot.spot) * math.exp(
        (snapshot.risk_free_rate - snapshot.dividend_yield)
        * snapshot.time_to_expiration_years
    )
    log_moneyness = math.log(float(snapshot.strike) / forward)
    if math.isclose(log_moneyness, 0.0, abs_tol=1e-12):
        moneyness = ContractMoneyness.ATM
    elif snapshot.contract_type is ContractType.CALL:
        moneyness = ContractMoneyness.ITM if log_moneyness < 0 else ContractMoneyness.OTM
    else:
        moneyness = ContractMoneyness.ITM if log_moneyness > 0 else ContractMoneyness.OTM
    standard_deviation_distance = None
    if snapshot.local_iv is not None and snapshot.local_iv > 0:
        denominator = snapshot.local_iv * math.sqrt(snapshot.time_to_expiration_years)
        if denominator > 0:
            standard_deviation_distance = log_moneyness / denominator
    volume_oi_ratio = (
        snapshot.day_volume / snapshot.open_interest
        if snapshot.day_volume is not None
        and snapshot.open_interest is not None
        and snapshot.open_interest > 0
        else None
    )
    print_notional = (
        snapshot.model_mark * snapshot.day_volume * snapshot.shares_per_contract
        if snapshot.model_mark is not None and snapshot.day_volume is not None
        else None
    )
    premium_yield = (
        float(snapshot.model_mark / snapshot.spot)
        if snapshot.model_mark is not None and snapshot.spot > 0
        else None
    )
    intrinsic_extrinsic_ratio = (
        float(snapshot.intrinsic_value / snapshot.extrinsic_value)
        if snapshot.intrinsic_value is not None
        and snapshot.extrinsic_value is not None
        and snapshot.extrinsic_value != 0
        else None
    )
    reasons: list[str] = []
    if snapshot.open_interest == 0:
        reasons.append("OPEN_INTEREST_ZERO")
    if snapshot.extrinsic_value == 0:
        reasons.append("EXTRINSIC_VALUE_ZERO")
    return ContractAnalysis(
        contract_id=snapshot.contract_id,
        forward_price=Decimal(str(forward)),
        log_forward_moneyness=log_moneyness,
        moneyness=moneyness,
        distance_from_spot_fraction=float((snapshot.strike - snapshot.spot) / snapshot.spot),
        standard_deviation_distance=standard_deviation_distance,
        volume_open_interest_ratio=volume_oi_ratio,
        print_notional=print_notional,
        premium_yield=premium_yield,
        intrinsic_extrinsic_ratio=intrinsic_extrinsic_ratio,
        quality_reasons=tuple(reasons),
    )


def build_chain_health(
    *,
    received_count: int,
    retained_count: int,
    catalog_matched_count: int,
    mark_aligned_count: int,
    iv_attempt_count: int,
    iv_converged_count: int,
    unknown_reference_count: int,
    reference_drift_failed: bool,
    batch_complete: bool = True,
    minimum_iv_convergence_fraction: float = 0.95,
) -> ChainHealth:
    catalog_coverage = catalog_matched_count / received_count if received_count else 0.0
    mark_alignment = mark_aligned_count / retained_count if retained_count else 0.0
    iv_convergence = iv_converged_count / iv_attempt_count if iv_attempt_count else 0.0
    unknown_fraction = unknown_reference_count / received_count if received_count else 0.0
    rejection_fraction = (
        (received_count - retained_count) / received_count if received_count else 1.0
    )
    reasons: list[str] = []
    if not batch_complete:
        reasons.append("INCOMPLETE_MATRIX")
    if reference_drift_failed:
        reasons.append("REFERENCE_DRIFT_FAILED")
    if iv_attempt_count == 0 or iv_convergence < minimum_iv_convergence_fraction:
        reasons.append("DATA_QUALITY_GATE_FAILED")
    if mark_alignment < 1.0:
        reasons.append("PARTIAL_MARK_ALIGNMENT")
    status = "FAILED" if any(
        reason in {
            "INCOMPLETE_MATRIX",
            "REFERENCE_DRIFT_FAILED",
            "DATA_QUALITY_GATE_FAILED",
        }
        for reason in reasons
    ) else ("DEGRADED" if reasons else "COMPLETE")
    return ChainHealth(
        received_count,
        retained_count,
        catalog_coverage,
        mark_alignment,
        iv_convergence,
        unknown_fraction,
        rejection_fraction,
        status,
        tuple(reasons),
    )


def analyze_expirations(
    matrix_id: UUID,
    contracts: tuple[ExpirationContractInput, ...],
    *,
    maximum_delta_interpolation_gap: float = 0.15,
    oi_wall_percentile: float = 0.9,
    minimum_oi_wall_robust_z: float = 2.5,
    maximum_oi_wall_clusters: int = 5,
    allow_zero_mad_wall_fallback: bool = False,
) -> tuple[OptionExpirationAnalytics, ...]:
    grouped: dict[object, list[ExpirationContractInput]] = {}
    for contract in contracts:
        grouped.setdefault(contract.expiration_date, []).append(contract)
    results: list[OptionExpirationAnalytics] = []
    for expiration_date in sorted(grouped):
        rows = grouped[expiration_date]
        maturity = median(row.time_to_expiration_years for row in rows)
        forwards = [
            float(row.spot)
            * math.exp((row.risk_free_rate - row.dividend_yield) * row.time_to_expiration_years)
            for row in rows
        ]
        forward = median(forwards)
        lower_strikes = sorted({float(row.strike) for row in rows if float(row.strike) < forward})
        upper_strikes = sorted({float(row.strike) for row in rows if float(row.strike) > forward})
        reasons = ["DEVELOPER_QUOTES_NOT_AVAILABLE"]
        atm_iv = None
        if lower_strikes and upper_strikes:
            lower_strike = lower_strikes[-1]
            upper_strike = upper_strikes[0]
            atm_rows = [
                row
                for row in rows
                if float(row.strike) in (lower_strike, upper_strike)
            ]
            if len(atm_rows) >= 4:
                atm_iv = median(row.local_iv for row in atm_rows)
        if atm_iv is None:
            reasons.append("ATM_IV_INSUFFICIENT")
        call_25 = _interpolate_delta(
            rows,
            ContractType.CALL,
            0.25,
            maximum_delta_interpolation_gap,
        )
        put_25 = _interpolate_delta(
            rows,
            ContractType.PUT,
            -0.25,
            maximum_delta_interpolation_gap,
        )
        if call_25 is None:
            reasons.append("CALL_25_DELTA_IV_INSUFFICIENT")
        if put_25 is None:
            reasons.append("PUT_25_DELTA_IV_INSUFFICIENT")
        call_skew = call_25 - atm_iv if call_25 is not None and atm_iv is not None else None
        put_skew = put_25 - atm_iv if put_25 is not None and atm_iv is not None else None
        risk_reversal = call_25 - put_25 if call_25 is not None and put_25 is not None else None
        calls = [row for row in rows if row.contract_type is ContractType.CALL]
        puts = [row for row in rows if row.contract_type is ContractType.PUT]
        call_volume = sum(row.day_volume or 0 for row in calls)
        put_volume = sum(row.day_volume or 0 for row in puts)
        call_oi = sum(row.open_interest or 0 for row in calls)
        put_oi = sum(row.open_interest or 0 for row in puts)
        total_oi = call_oi + put_oi
        max_oi = max((row.open_interest or 0 for row in rows), default=0)
        diagnostics = {
            "maximum_delta_interpolation_gap": maximum_delta_interpolation_gap,
            "call_25_delta_available": call_25 is not None,
            "put_25_delta_available": put_25 is not None,
        }
        concentration = {
            "maximum_contract_oi_fraction": max_oi / total_oi if total_oi else None,
            "put_call_volume_ratio": put_volume / call_volume if call_volume else None,
            "put_call_open_interest_ratio": put_oi / call_oi if call_oi else None,
        }
        wall_clusters = []
        for contract_type, type_rows in (
            (ContractType.CALL, calls),
            (ContractType.PUT, puts),
        ):
            clusters = detect_oi_wall_clusters(
                tuple(
                    OiWallInput(row.strike, row.open_interest or 0)
                    for row in type_rows
                ),
                rows[0].spot,
                percentile=oi_wall_percentile,
                minimum_robust_z=minimum_oi_wall_robust_z,
                maximum_clusters=maximum_oi_wall_clusters,
                allow_zero_mad_fallback=allow_zero_mad_wall_fallback,
            )
            wall_clusters.extend(
                {
                    "contract_type": contract_type.value,
                    "member_strikes": [str(strike) for strike in cluster.member_strikes],
                    "center_strike": str(cluster.center_strike),
                    "maximum_robust_z": cluster.maximum_robust_z,
                    "total_open_interest": cluster.total_open_interest,
                }
                for cluster in clusters
            )
        results.append(
            OptionExpirationAnalytics(
                matrix_id=matrix_id,
                expiration_date=expiration_date,
                fractional_maturity_years=maturity,
                forward_price=Decimal(str(forward)),
                atm_iv=atm_iv,
                call_25_delta_iv=call_25,
                put_25_delta_iv=put_25,
                call_skew_25_delta=call_skew,
                put_skew_25_delta=put_skew,
                risk_reversal_25_delta=risk_reversal,
                interpolation_diagnostics_json=json.dumps(diagnostics, sort_keys=True),
                put_volume=put_volume,
                call_volume=call_volume,
                put_open_interest=put_oi,
                call_open_interest=call_oi,
                breadth=len({row.strike for row in rows}),
                concentration_metrics_json=json.dumps(concentration, sort_keys=True),
                wall_clusters_json=json.dumps(wall_clusters, sort_keys=True),
                term_change=None,
                term_slope=None,
                quality_reasons=tuple(reasons),
            )
        )
    for index in range(1, len(results)):
        previous = results[index - 1]
        current = results[index]
        if previous.atm_iv is None or current.atm_iv is None:
            continue
        maturity_gap = (
            current.fractional_maturity_years - previous.fractional_maturity_years
        )
        if maturity_gap <= 0:
            continue
        change = current.atm_iv - previous.atm_iv
        results[index] = replace(
            current,
            term_change=change,
            term_slope=change / maturity_gap,
        )
    return tuple(results)


def _interpolate_delta(
    rows: list[ExpirationContractInput],
    contract_type: ContractType,
    target: float,
    maximum_gap: float,
) -> float | None:
    points = sorted(
        (row.local_delta, row.local_iv)
        for row in rows
        if row.contract_type is contract_type
    )
    for (left_delta, left_iv), (right_delta, right_iv) in zip(points, points[1:]):
        if left_delta <= target <= right_delta:
            gap = right_delta - left_delta
            if gap > maximum_gap:
                return None
            if gap == 0:
                return median((left_iv, right_iv))
            weight = (target - left_delta) / gap
            return left_iv + weight * (right_iv - left_iv)
    return None