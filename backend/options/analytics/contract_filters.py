from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from zoneinfo import ZoneInfo

from options.domain import ContractType, DataQualityFlag, ExerciseStyle


ET = ZoneInfo("America/New_York")


class DteBucket(str, Enum):
    ZERO_DTE = "ZERO_DTE"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class FilterReason(str, Enum):
    UNSUPPORTED_CONTRACT_TYPE = "UNSUPPORTED_CONTRACT_TYPE"
    UNSUPPORTED_EXERCISE_STYLE = "UNSUPPORTED_EXERCISE_STYLE"
    UNSUPPORTED_MULTIPLIER = "UNSUPPORTED_MULTIPLIER"
    ADJUSTED_CONTRACT = "ADJUSTED_CONTRACT"
    NON_POSITIVE_STRIKE = "NON_POSITIVE_STRIKE"
    MISSING_SPOT_REFERENCE = "MISSING_SPOT_REFERENCE"
    EXPIRED_CONTRACT = "EXPIRED_CONTRACT"
    DTE_OUT_OF_RANGE = "DTE_OUT_OF_RANGE"
    OUTSIDE_STRIKE_CORRIDOR = "OUTSIDE_STRIKE_CORRIDOR"
    LIQUIDITY_FLOOR = "LIQUIDITY_FLOOR"


@dataclass(frozen=True, slots=True)
class ContractFilterResult:
    eligible: bool
    calendar_dte: int
    dte_bucket: DteBucket | None
    reasons: tuple[FilterReason, ...]
    quality_flags: tuple[DataQualityFlag, ...]


def filter_contract(
    *,
    contract_type: ContractType | None,
    exercise_style: ExerciseStyle | None,
    shares_per_contract: int,
    has_additional_deliverables: bool,
    expiration_date: date,
    expiration_cutoff: datetime,
    market_time: datetime,
    strike: Decimal,
    spot: Decimal | None,
    day_volume: int | None,
    open_interest: int | None,
    minimum_dte: int = 0,
    maximum_dte: int = 45,
    strike_corridor_fraction: Decimal = Decimal("0.15"),
    minimum_day_volume: int = 20,
    minimum_open_interest: int = 100,
) -> ContractFilterResult:
    market_time = _as_utc(market_time, "market_time")
    expiration_cutoff = _as_utc(expiration_cutoff, "expiration_cutoff")
    calendar_dte = (expiration_date - market_time.astimezone(ET).date()).days
    reasons: list[FilterReason] = []
    quality_flags: list[DataQualityFlag] = []

    if contract_type not in (ContractType.CALL, ContractType.PUT):
        reasons.append(FilterReason.UNSUPPORTED_CONTRACT_TYPE)
    if exercise_style is not ExerciseStyle.AMERICAN:
        reasons.append(FilterReason.UNSUPPORTED_EXERCISE_STYLE)
    if shares_per_contract != 100:
        reasons.append(FilterReason.UNSUPPORTED_MULTIPLIER)
    if has_additional_deliverables:
        reasons.append(FilterReason.ADJUSTED_CONTRACT)
    if strike <= 0:
        reasons.append(FilterReason.NON_POSITIVE_STRIKE)
    if spot is None or spot <= 0:
        reasons.append(FilterReason.MISSING_SPOT_REFERENCE)
    if market_time >= expiration_cutoff:
        reasons.append(FilterReason.EXPIRED_CONTRACT)
    if calendar_dte < minimum_dte or calendar_dte > maximum_dte:
        reasons.append(FilterReason.DTE_OUT_OF_RANGE)

    if calendar_dte == 0:
        bucket = DteBucket.ZERO_DTE
    elif 1 <= calendar_dte <= 14:
        bucket = DteBucket.WEEKLY
    elif 15 <= calendar_dte <= 45:
        bucket = DteBucket.MONTHLY
    else:
        bucket = None

    if spot is not None and spot > 0 and strike > 0:
        lower = spot * (Decimal("1") - strike_corridor_fraction)
        upper = spot * (Decimal("1") + strike_corridor_fraction)
        if strike < lower or strike > upper:
            reasons.append(FilterReason.OUTSIDE_STRIKE_CORRIDOR)

    if day_volume is None:
        quality_flags.append(DataQualityFlag.MISSING_DAY_VOLUME)
    if open_interest is None:
        quality_flags.append(DataQualityFlag.MISSING_OPEN_INTEREST)
    volume_passes = day_volume is not None and day_volume >= minimum_day_volume
    open_interest_passes = (
        open_interest is not None and open_interest >= minimum_open_interest
    )
    if not volume_passes and not open_interest_passes:
        reasons.append(FilterReason.LIQUIDITY_FLOOR)

    return ContractFilterResult(
        eligible=not reasons,
        calendar_dte=calendar_dte,
        dte_bucket=bucket,
        reasons=tuple(dict.fromkeys(reasons)),
        quality_flags=tuple(quality_flags),
    )


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)