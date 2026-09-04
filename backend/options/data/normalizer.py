from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from options.analytics.contract_filters import filter_contract
from options.analytics.greeks import OptionValuationInput, solve_local_greeks
from options.analytics.marks import (
    UnderlyingMinuteBar,
    calculate_contract_economics,
    select_developer_marks,
)
from options.config import DeveloperPolicy
from options.domain import (
    CatalogEligibility,
    DataQualityFlag,
    OptionContractCatalogEntry,
    OptionContractSnapshot,
)


@dataclass(frozen=True, slots=True)
class RawDeveloperOptionObservation:
    contract_ticker: str
    corridor_spot: Decimal | None
    day_close: Decimal | None
    day_vwap: Decimal | None
    day_volume: int | None
    open_interest: int | None
    option_mark_time: datetime | None
    provider_iv: float | None
    provider_gamma: float | None
    first_observed_at: datetime
    revised_observed_at: datetime | None
    raw_payload_json: str
    raw_payload_sha256: str


@dataclass(frozen=True, slots=True)
class DeveloperNormalizationInput:
    raw: RawDeveloperOptionObservation
    catalog: OptionContractCatalogEntry
    underlying_bars: tuple[UnderlyingMinuteBar, ...]
    expiration_cutoff: datetime
    risk_free_rate: float
    dividend_yield: float
    input_quality_flags: tuple[DataQualityFlag, ...] = ()
    normalized_observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DeveloperNormalizationResult:
    snapshots: tuple[OptionContractSnapshot, ...]
    matrix_snapshots: tuple[OptionContractSnapshot, ...]
    received_count: int
    retained_count: int
    rejected_counts: tuple[tuple[str, int], ...]
    iv_attempt_count: int
    iv_converged_count: int
    iv_convergence_fraction: float
    strategy_eligible: bool


def parse_polygon_snapshot(
    payload: dict[str, Any],
    first_observed_at: datetime,
    revised_observed_at: datetime | None = None,
) -> RawDeveloperOptionObservation:
    details = payload.get("details")
    if not isinstance(details, dict) or not details.get("ticker"):
        raise ValueError("Polygon snapshot details.ticker is required")
    day = payload.get("day") if isinstance(payload.get("day"), dict) else {}
    underlying = (
        payload.get("underlying_asset")
        if isinstance(payload.get("underlying_asset"), dict)
        else {}
    )
    greeks = payload.get("greeks") if isinstance(payload.get("greeks"), dict) else {}
    option_mark_time = _provider_timestamp(day.get("last_updated"))
    if option_mark_time is None:
        last_trade = payload.get("last_trade")
        if isinstance(last_trade, dict):
            option_mark_time = _provider_timestamp(last_trade.get("sip_timestamp"))
    raw_payload_json = _canonical_json(payload)
    return RawDeveloperOptionObservation(
        contract_ticker=str(details["ticker"]),
        corridor_spot=_optional_decimal(underlying.get("price")),
        day_close=_optional_decimal(day.get("close")),
        day_vwap=_optional_decimal(day.get("vwap")),
        day_volume=_optional_int(day.get("volume")),
        open_interest=_optional_int(payload.get("open_interest")),
        option_mark_time=option_mark_time,
        provider_iv=_optional_float(payload.get("implied_volatility")),
        provider_gamma=_optional_float(greeks.get("gamma")),
        first_observed_at=_as_utc(first_observed_at, "first_observed_at"),
        revised_observed_at=(
            _as_utc(revised_observed_at, "revised_observed_at")
            if revised_observed_at is not None
            else None
        ),
        raw_payload_json=raw_payload_json,
        raw_payload_sha256=_sha256(raw_payload_json),
    )


class DeveloperOptionNormalizer:
    def __init__(
        self,
        policy: DeveloperPolicy,
        *,
        model_version: str = "black_scholes_european_v1",
    ) -> None:
        self.policy = policy
        self.model_version = model_version

    def normalize(
        self,
        batch_id: UUID,
        inputs: tuple[DeveloperNormalizationInput, ...],
    ) -> DeveloperNormalizationResult:
        rejected: dict[str, int] = {}
        prepared: list[dict[str, Any]] = []
        valuation_inputs: list[OptionValuationInput] = []
        valuation_indexes: list[int] = []
        seen_raw_facts: set[tuple[str, datetime | None, str]] = set()
        ordered_inputs = sorted(
            inputs,
            key=lambda item: (
                item.raw.contract_ticker,
                item.raw.option_mark_time or datetime.min.replace(tzinfo=timezone.utc),
                item.raw.first_observed_at,
                item.raw.raw_payload_sha256,
            ),
        )
        for item in ordered_inputs:
            raw = item.raw
            catalog = item.catalog
            normalized_observed_at = (
                _as_utc(item.normalized_observed_at, "normalized_observed_at")
                if item.normalized_observed_at is not None
                else raw.revised_observed_at or raw.first_observed_at
            )
            raw_fact_key = (
                raw.contract_ticker,
                raw.option_mark_time,
                raw.raw_payload_sha256,
            )
            if raw_fact_key in seen_raw_facts:
                continue
            seen_raw_facts.add(raw_fact_key)
            if raw.contract_ticker != catalog.contract_ticker:
                _increment(rejected, "CATALOG_TICKER_MISMATCH")
                continue
            if catalog.eligibility_status is not CatalogEligibility.VALIDATED_ACTIVE:
                _increment(rejected, "CATALOG_NOT_ELIGIBLE")
                continue
            if raw.option_mark_time is None:
                _increment(rejected, DataQualityFlag.MISSING_MARK_TIMESTAMP.value)
                continue
            marks = select_developer_marks(
                day_close=raw.day_close,
                day_vwap=raw.day_vwap,
                option_mark_time=raw.option_mark_time,
                underlying_bars=item.underlying_bars,
                observed_at=normalized_observed_at,
                maximum_source_skew_seconds=(
                    self.policy.model_quality.maximum_option_spot_skew_seconds
                ),
                maximum_source_age_seconds=(
                    self.policy.model_quality.maximum_developer_source_age_seconds
                ),
            )
            if marks.aligned_spot is None or marks.spot_market_data_time is None:
                _increment(rejected, DataQualityFlag.MISSING_ALIGNED_SPOT.value)
                continue
            filtered = filter_contract(
                contract_type=catalog.contract_type,
                exercise_style=catalog.exercise_style,
                shares_per_contract=catalog.shares_per_contract,
                has_additional_deliverables=False,
                expiration_date=catalog.expiration_date,
                expiration_cutoff=item.expiration_cutoff,
                market_time=raw.option_mark_time,
                strike=catalog.strike,
                spot=raw.corridor_spot,
                day_volume=raw.day_volume,
                open_interest=raw.open_interest,
                minimum_dte=self.policy.contract_filter.minimum_dte,
                maximum_dte=self.policy.contract_filter.maximum_dte,
                strike_corridor_fraction=(
                    self.policy.contract_filter.strike_corridor_fraction
                ),
                minimum_day_volume=self.policy.contract_filter.minimum_day_volume,
                minimum_open_interest=self.policy.contract_filter.minimum_open_interest,
            )
            if not filtered.eligible:
                for reason in filtered.reasons:
                    _increment(rejected, reason.value)
                continue
            maturity = (
                _as_utc(item.expiration_cutoff, "expiration_cutoff")
                - raw.option_mark_time
            ).total_seconds() / (365.0 * 24.0 * 60.0 * 60.0)
            economics = calculate_contract_economics(
                contract_type=catalog.contract_type,
                strike=catalog.strike,
                spot=marks.aligned_spot,
                model_mark=marks.model_mark,
                intrinsic_price_tolerance=(
                    self.policy.model_quality.intrinsic_price_tolerance
                ),
            )
            quality_flags = tuple(
                dict.fromkeys(
                    filtered.quality_flags
                    + marks.quality_flags
                    + economics.quality_flags
                    + item.input_quality_flags
                )
            )
            prepared.append(
                {
                    "input": item,
                    "marks": marks,
                    "filter": filtered,
                    "economics": economics,
                    "maturity": maturity,
                    "quality_flags": quality_flags,
                    "normalized_observed_at": normalized_observed_at,
                }
            )
            if economics.model_mark is not None:
                valuation_indexes.append(len(prepared) - 1)
                valuation_inputs.append(
                    OptionValuationInput(
                        contract_type=catalog.contract_type,
                        spot=marks.aligned_spot,
                        strike=catalog.strike,
                        model_mark=economics.model_mark,
                        time_to_expiration_years=float(maturity),
                        risk_free_rate=item.risk_free_rate,
                        dividend_yield=item.dividend_yield,
                    )
                )

        greek_results = solve_local_greeks(
            tuple(valuation_inputs),
            minimum_iv=float(self.policy.model_quality.minimum_iv),
            maximum_iv=float(self.policy.model_quality.maximum_iv),
            newton_iterations=self.policy.model_quality.newton_iterations,
            price_error_tolerance=self.policy.model_quality.price_error_tolerance,
            minimum_vega=self.policy.model_quality.minimum_vega,
            use_brent_fallback=self.policy.model_quality.use_brent_fallback,
        )
        by_prepared_index = dict(zip(valuation_indexes, greek_results))
        snapshots: list[OptionContractSnapshot] = []
        revision_by_key: dict[tuple[str, datetime], int] = {}
        for index, values in enumerate(prepared):
            item = values["input"]
            raw = item.raw
            catalog = item.catalog
            marks = values["marks"]
            filtered = values["filter"]
            economics = values["economics"]
            quality_flags = values["quality_flags"]
            normalized_observed_at = values["normalized_observed_at"]
            greek = by_prepared_index.get(index)
            if greek is None:
                local_values = (None, None, None, None, None, None)
                iv_converged = False
                iv_solver = None
                iv_iterations = 0
                iv_price_error = None
                iv_failure_reason = "MODEL_MARK_UNAVAILABLE"
            elif greek.converged:
                local_values = (
                    greek.local_iv,
                    greek.local_gamma,
                    greek.local_delta,
                    greek.local_theta_per_day,
                    greek.local_vega_per_vol_point,
                    greek.local_rho_per_rate_point,
                )
                iv_converged = True
                iv_solver = greek.solver.value
                iv_iterations = greek.iteration_count
                iv_price_error = greek.price_error
                iv_failure_reason = None
            else:
                local_values = (None, None, None, None, None, None)
                iv_converged = False
                iv_solver = None
                iv_iterations = greek.iteration_count
                iv_price_error = greek.price_error
                iv_failure_reason = greek.failure_reason.value
                quality_flags = tuple(
                    dict.fromkeys(quality_flags + (DataQualityFlag.NON_CONVERGED_IV,))
                )
            revision_key = (catalog.contract_ticker, raw.option_mark_time)
            revision = revision_by_key.get(revision_key, 0) + 1
            revision_by_key[revision_key] = revision
            normalized_payload = {
                "contract_id": catalog.contract_id,
                "contract_ticker": catalog.contract_ticker,
                "contract_type": catalog.contract_type.value,
                "expiration_date": catalog.expiration_date.isoformat(),
                "expiration_cutoff": item.expiration_cutoff.isoformat(),
                "strike": str(catalog.strike),
                "spot": str(marks.aligned_spot),
                "spot_market_data_time": marks.spot_market_data_time.isoformat(),
                "display_mark": str(marks.display_mark) if marks.display_mark else None,
                "model_mark": str(economics.model_mark) if economics.model_mark else None,
                "mark_market_data_time": raw.option_mark_time.isoformat(),
                "mark_source": marks.mark_source.value,
                "day_volume": raw.day_volume,
                "open_interest": raw.open_interest,
                "local_iv": local_values[0],
                "local_gamma": local_values[1],
                "local_delta": local_values[2],
                "local_theta_per_day": local_values[3],
                "local_vega_per_vol_point": local_values[4],
                "local_rho_per_rate_point": local_values[5],
                "intrinsic_value": str(economics.intrinsic_value),
                "extrinsic_value": (
                    str(economics.extrinsic_value)
                    if economics.extrinsic_value is not None
                    else None
                ),
                "single_contract_breakeven": (
                    str(economics.single_contract_breakeven)
                    if economics.single_contract_breakeven is not None
                    else None
                ),
                "provider_iv": raw.provider_iv,
                "provider_gamma": raw.provider_gamma,
                "risk_free_rate": item.risk_free_rate,
                "dividend_yield": item.dividend_yield,
                "iv_converged": iv_converged,
                "iv_solver": iv_solver,
                "iv_iteration_count": iv_iterations,
                "iv_price_error": iv_price_error,
                "iv_failure_reason": iv_failure_reason,
                "quality_flags": [flag.value for flag in quality_flags],
                "model_version": self.model_version,
                "raw_payload_sha256": raw.raw_payload_sha256,
            }
            normalized_payload_sha256 = _sha256(_canonical_json(normalized_payload))
            snapshots.append(
                OptionContractSnapshot(
                    snapshot_id=uuid5(
                        NAMESPACE_URL,
                        f"{normalized_payload_sha256}:{normalized_observed_at.isoformat()}",
                    ),
                    contract_id=catalog.contract_id,
                    contract_ticker=catalog.contract_ticker,
                    underlyer=catalog.underlyer,
                    provider="polygon",
                    contract_type=catalog.contract_type,
                    expiration_date=catalog.expiration_date,
                    expiration_cutoff=item.expiration_cutoff,
                    calendar_dte=filtered.calendar_dte,
                    time_to_expiration_years=float(values["maturity"]),
                    strike=catalog.strike,
                    shares_per_contract=catalog.shares_per_contract,
                    exercise_style=catalog.exercise_style,
                    spot=marks.aligned_spot,
                    spot_market_data_time=marks.spot_market_data_time,
                    bid=None,
                    ask=None,
                    midpoint=None,
                    display_mark=marks.display_mark,
                    model_mark=economics.model_mark,
                    mark_market_data_time=raw.option_mark_time,
                    mark_source=marks.mark_source,
                    day_volume=raw.day_volume,
                    open_interest=raw.open_interest,
                    market_data_time=raw.option_mark_time,
                    first_observed_at=normalized_observed_at,
                    revised_observed_at=raw.revised_observed_at,
                    local_iv=local_values[0],
                    local_gamma=local_values[1],
                    local_delta=local_values[2],
                    local_theta_per_day=local_values[3],
                    local_vega_per_vol_point=local_values[4],
                    local_rho_per_rate_point=local_values[5],
                    intrinsic_value=economics.intrinsic_value,
                    extrinsic_value=economics.extrinsic_value,
                    single_contract_breakeven=economics.single_contract_breakeven,
                    provider_iv=raw.provider_iv,
                    provider_gamma=raw.provider_gamma,
                    risk_free_rate=item.risk_free_rate,
                    dividend_yield=item.dividend_yield,
                    iv_converged=iv_converged,
                    iv_solver=iv_solver,
                    iv_iteration_count=iv_iterations,
                    iv_price_error=iv_price_error,
                    iv_failure_reason=iv_failure_reason,
                    model_version=self.model_version,
                    quality_flags=quality_flags,
                    batch_id=batch_id,
                    raw_payload_sha256=raw.raw_payload_sha256,
                    normalized_payload_sha256=normalized_payload_sha256,
                    revision=revision,
                )
            )
        latest: dict[tuple[str, datetime], OptionContractSnapshot] = {}
        for snapshot in snapshots:
            key = (snapshot.contract_ticker, snapshot.market_data_time)
            latest[key] = snapshot
        converged_count = sum(result.converged for result in greek_results)
        attempted_count = len(greek_results)
        fraction = converged_count / attempted_count if attempted_count else 0.0
        return DeveloperNormalizationResult(
            snapshots=tuple(snapshots),
            matrix_snapshots=tuple(
                sorted(
                    (
                        snapshot
                        for snapshot in latest.values()
                        if snapshot.model_mark is not None
                    ),
                    key=lambda item: item.contract_id,
                )
            ),
            received_count=len(inputs),
            retained_count=len(snapshots),
            rejected_counts=tuple(sorted(rejected.items())),
            iv_attempt_count=attempted_count,
            iv_converged_count=converged_count,
            iv_convergence_fraction=fraction,
            strategy_eligible=(
                attempted_count > 0
                and fraction
                >= float(self.policy.model_quality.minimum_iv_success_fraction)
            ),
        )


def _provider_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    timestamp = int(value)
    divisor = 1_000_000_000 if abs(timestamp) >= 10_000_000_000_000_000 else 1_000
    return datetime.fromtimestamp(timestamp / divisor, tz=timezone.utc)


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _increment(counts: dict[str, int], reason: str) -> None:
    counts[reason] = counts.get(reason, 0) + 1