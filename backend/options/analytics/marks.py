from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from options.domain import ContractType, DataQualityFlag, MarkSource


@dataclass(frozen=True, slots=True)
class UnderlyingMinuteBar:
    close: Decimal
    market_data_time: datetime

    def __post_init__(self) -> None:
        if type(self.close) is not Decimal or not self.close.is_finite() or self.close <= 0:
            raise ValueError("underlying close must be a positive Decimal")
        object.__setattr__(
            self,
            "market_data_time",
            _as_utc(self.market_data_time, "market_data_time"),
        )


@dataclass(frozen=True, slots=True)
class DeveloperMarkResult:
    display_mark: Decimal | None
    model_mark: Decimal | None
    aligned_spot: Decimal | None
    spot_market_data_time: datetime | None
    mark_market_data_time: datetime | None
    mark_source: MarkSource
    source_skew_seconds: float | None
    source_age_seconds: float | None
    quality_flags: tuple[DataQualityFlag, ...]


@dataclass(frozen=True, slots=True)
class ContractEconomics:
    intrinsic_value: Decimal
    extrinsic_value: Decimal | None
    single_contract_breakeven: Decimal | None
    model_mark: Decimal | None
    quality_flags: tuple[DataQualityFlag, ...]


def select_developer_marks(
    *,
    day_close: Decimal | None,
    day_vwap: Decimal | None,
    option_mark_time: datetime | None,
    underlying_bars: tuple[UnderlyingMinuteBar, ...],
    observed_at: datetime,
    maximum_source_skew_seconds: int = 60,
    maximum_source_age_seconds: int = 1800,
) -> DeveloperMarkResult:
    observed_at = _as_utc(observed_at, "observed_at")
    flags: list[DataQualityFlag] = []
    display_mark = day_close if _positive(day_close) else None
    mark_source = MarkSource.DISPLAY_DAY_CLOSE
    if display_mark is None and _positive(day_vwap):
        display_mark = day_vwap
        mark_source = MarkSource.DISPLAY_DAY_VWAP
        flags.append(DataQualityFlag.FALLBACK_MARK)
    if option_mark_time is None:
        flags.append(DataQualityFlag.MISSING_MARK_TIMESTAMP)
        return DeveloperMarkResult(
            display_mark,
            None,
            None,
            None,
            None,
            mark_source,
            None,
            None,
            tuple(flags),
        )
    option_mark_time = _as_utc(option_mark_time, "option_mark_time")
    source_age_seconds = (observed_at - option_mark_time).total_seconds()
    if source_age_seconds < 0 or source_age_seconds > maximum_source_age_seconds:
        flags.append(DataQualityFlag.STALE_MARK)
    if not _positive(day_close):
        flags.append(DataQualityFlag.NON_POSITIVE_MARK)
    prior_bars = [bar for bar in underlying_bars if bar.market_data_time <= option_mark_time]
    if not prior_bars:
        flags.append(DataQualityFlag.MISSING_ALIGNED_SPOT)
        aligned_bar = None
        source_skew_seconds = None
    else:
        aligned_bar = max(prior_bars, key=lambda bar: bar.market_data_time)
        source_skew_seconds = (
            option_mark_time - aligned_bar.market_data_time
        ).total_seconds()
        if source_skew_seconds > maximum_source_skew_seconds:
            flags.append(DataQualityFlag.OPTION_SPOT_SKEW)
    model_mark = day_close if _positive(day_close) else None
    if any(
        flag
        in {
            DataQualityFlag.STALE_MARK,
            DataQualityFlag.NON_POSITIVE_MARK,
            DataQualityFlag.MISSING_ALIGNED_SPOT,
            DataQualityFlag.OPTION_SPOT_SKEW,
        }
        for flag in flags
    ):
        model_mark = None
    return DeveloperMarkResult(
        display_mark=display_mark,
        model_mark=model_mark,
        aligned_spot=aligned_bar.close if aligned_bar else None,
        spot_market_data_time=aligned_bar.market_data_time if aligned_bar else None,
        mark_market_data_time=option_mark_time,
        mark_source=(
            MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE
            if model_mark is not None
            else mark_source
        ),
        source_skew_seconds=source_skew_seconds,
        source_age_seconds=source_age_seconds,
        quality_flags=tuple(dict.fromkeys(flags)),
    )


def calculate_contract_economics(
    *,
    contract_type: ContractType,
    strike: Decimal,
    spot: Decimal,
    model_mark: Decimal | None,
    intrinsic_price_tolerance: Decimal,
) -> ContractEconomics:
    intrinsic = (
        max(spot - strike, Decimal("0"))
        if contract_type is ContractType.CALL
        else max(strike - spot, Decimal("0"))
    )
    if model_mark is None:
        return ContractEconomics(intrinsic, None, None, None, ())
    extrinsic = model_mark - intrinsic
    flags: tuple[DataQualityFlag, ...] = ()
    accepted_mark = model_mark
    if extrinsic < -intrinsic_price_tolerance:
        flags = (DataQualityFlag.BELOW_INTRINSIC_MARK,)
        accepted_mark = None
    breakeven = None
    if accepted_mark is not None:
        breakeven = (
            strike + accepted_mark
            if contract_type is ContractType.CALL
            else strike - accepted_mark
        )
    return ContractEconomics(
        intrinsic_value=intrinsic,
        extrinsic_value=extrinsic,
        single_contract_breakeven=breakeven,
        model_mark=accepted_mark,
        quality_flags=flags,
    )


def _positive(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)