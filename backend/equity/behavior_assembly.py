"""Pure causal assembly of stock behavior components from finalized bar inputs."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from datetime import timezone
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, field_validator, model_validator

from .behavior import (
    OPTIONS_SWING_PROFILE,
    BehaviorComponent,
    BehaviorMetric,
    BehaviorSource,
    Contract,
    Factor,
    Interval,
    Name,
    Sha256,
    StockBehaviorSnapshot,
)
from .behavior_calculators import (
    calculate_location_metrics,
    calculate_momentum_metrics,
    calculate_participation_metrics,
    calculate_relative_strength_metric,
    calculate_trend_metrics,
    calculate_volatility_metrics,
)
from .domain import BarAvailabilityMode, BarSessionScope, DecisionWatermark, EquityBarRevision


class FinalizedBarSeries(Contract):
    security_id: UUID
    ticker: Name
    interval: Interval
    bar_revision_ids: tuple[UUID, ...] = Field(min_length=1, max_length=1024)
    bar_payload_sha256s: tuple[Sha256, ...] = Field(min_length=1, max_length=1024)
    bar_ends: tuple[AwareDatetime, ...] = Field(min_length=1, max_length=1024)
    bar_available_ats: tuple[AwareDatetime, ...] = Field(min_length=1, max_length=1024)
    high: tuple[float, ...] = Field(min_length=1, max_length=1024)
    low: tuple[float, ...] = Field(min_length=1, max_length=1024)
    close: tuple[float, ...] = Field(min_length=1, max_length=1024)
    volume: tuple[float, ...] = Field(min_length=1, max_length=1024)
    source: BehaviorSource
    adjusted: bool
    session_scope: Literal["RTH"] = "RTH"
    completed_bars_only: Literal[True] = True

    @field_validator("bar_ends", "bar_available_ats", mode="after")
    @classmethod
    def utc_bar_ends(cls, values):
        return tuple(value.astimezone(timezone.utc) for value in values)

    @model_validator(mode="after")
    def validate_series(self):
        lengths = {
            len(self.bar_revision_ids), len(self.bar_payload_sha256s), len(self.bar_ends),
            len(self.bar_available_ats), len(self.high), len(self.low), len(self.close), len(self.volume),
        }
        if len(lengths) != 1:
            raise ValueError("bar identities, clocks and prices must have equal length")
        if len(set(self.bar_revision_ids)) != len(self.bar_revision_ids):
            raise ValueError("bar revision IDs must be distinct")
        if any(current <= previous for previous, current in zip(self.bar_ends, self.bar_ends[1:])):
            raise ValueError("finalized bars must be strictly ordered by market time")
        if any(available_at < bar_end for bar_end, available_at in zip(self.bar_ends, self.bar_available_ats)):
            raise ValueError("a finalized bar cannot be available before its end")
        if any(available_at > self.source.received_at for available_at in self.bar_available_ats):
            raise ValueError("source receipt must cover every finalized bar revision")
        if any(volume < 0 for volume in self.volume):
            raise ValueError("bar volume cannot be negative")
        if self.source.security_id != self.security_id or self.source.interval != self.interval:
            raise ValueError("source identity and interval must match the finalized bars")
        if self.source.market_time != self.bar_ends[-1]:
            raise ValueError("source market time must equal the latest finalized bar end")
        adjusted_bases = {"PROVIDER_SPLIT_ADJUSTED", "REVIEWED_SPLIT_ADJUSTED"}
        if (self.adjusted and self.source.price_basis not in adjusted_bases) or (
            not self.adjusted and self.source.price_basis != "RAW_ACTION_GATED"
        ):
            raise ValueError("source price basis must match the bar adjustment basis")
        expected_history_mode = (
            "RECONSTRUCTED_HISTORY" if self.source.history_available_at is not None
            else "LIVE_OBSERVED_HISTORY"
        )
        if self.source.history_mode != expected_history_mode:
            raise ValueError("source history mode and availability must agree")
        history_available_at = self.source.history_available_at or self.source.received_at
        if any(available_at > history_available_at for available_at in self.bar_available_ats):
            raise ValueError("source history availability does not cover every bar revision")
        return self


def finalized_series_from_revisions(
    bars: tuple[EquityBarRevision, ...], source: BehaviorSource,
    *, bar_recorded_ats: tuple[datetime, ...],
) -> FinalizedBarSeries:
    """Convert already selected canonical revisions into one calculator input."""
    if not bars:
        raise ValueError("at least one finalized bar revision is required")
    if len(bar_recorded_ats) != len(bars) or any(
        value.tzinfo is None or value.utcoffset() is None for value in bar_recorded_ats
    ):
        raise ValueError("every bar revision requires an aware database recording clock")
    first = bars[0]
    if any(
        not bar.is_final or bar.session_scope is not BarSessionScope.RTH
        or bar.security_id != first.security_id or bar.ticker != first.ticker
        or bar.interval != first.interval or bar.adjusted != first.adjusted
        for bar in bars
    ):
        raise ValueError("bar revisions must be finalized RTH facts with one identity and basis")
    modes = {bar.availability_mode for bar in bars}
    if len(modes) != 1:
        raise ValueError("bar revisions cannot mix availability modes")
    expected_mode = (
        "RECONSTRUCTED"
        if first.availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
        else "PROSPECTIVE_RECEIPT"
    )
    expected_history_mode = (
        "RECONSTRUCTED_HISTORY" if expected_mode == "RECONSTRUCTED"
        else "LIVE_OBSERVED_HISTORY"
    )
    if source.history_mode != expected_history_mode:
        raise ValueError("behavior source history mode must match the selected bar revisions")
    available_ats = tuple(max(
        clock for clock in (
            bar.bar_end, bar.system_observed_at, bar.provider_published_at,
            bar.replay_available_at, recorded_at,
        ) if clock is not None
    ) for bar, recorded_at in zip(bars, bar_recorded_ats))
    return FinalizedBarSeries(
        security_id=first.security_id, ticker=first.ticker, interval=first.interval,
        bar_revision_ids=tuple(bar.bar_revision_id for bar in bars),
        bar_payload_sha256s=tuple(bar.payload_sha256 for bar in bars),
        bar_ends=tuple(bar.bar_end for bar in bars), bar_available_ats=available_ats,
        high=tuple(float(bar.high_price) for bar in bars),
        low=tuple(float(bar.low_price) for bar in bars),
        close=tuple(float(bar.close_price) for bar in bars),
        volume=tuple(float(bar.volume) for bar in bars),
        source=source, adjusted=first.adjusted,
    )


def assemble_trend_component(
    series: FinalizedBarSeries, decision: DecisionWatermark,
) -> BehaviorComponent:
    """Build measurement-ready trend evidence without assigning a trend state."""
    blocked = _source_blocker(series, decision, "TREND")
    return blocked or _component_from_metrics(
        "TREND", series, calculate_trend_metrics(series.high, series.low, series.close, series.interval),
    )


def assemble_momentum_component(series: FinalizedBarSeries, decision: DecisionWatermark) -> BehaviorComponent:
    blocked = _source_blocker(series, decision, "MOMENTUM")
    return blocked or _component_from_metrics(
        "MOMENTUM", series, calculate_momentum_metrics(series.close, series.interval),
    )


def assemble_volatility_component(series: FinalizedBarSeries, decision: DecisionWatermark) -> BehaviorComponent:
    blocked = _source_blocker(series, decision, "VOLATILITY")
    return blocked or _component_from_metrics(
        "VOLATILITY", series, calculate_volatility_metrics(series.high, series.low, series.close, series.interval),
    )


def assemble_participation_component(series: FinalizedBarSeries, decision: DecisionWatermark) -> BehaviorComponent:
    if series.interval != "1d":
        return _not_applicable("PARTICIPATION", series.interval)
    blocked = _source_blocker(series, decision, "PARTICIPATION")
    return blocked or _component_from_metrics(
        "PARTICIPATION", series, calculate_participation_metrics(series.close, series.volume, series.interval),
    )


def assemble_location_component(series: FinalizedBarSeries, decision: DecisionWatermark) -> BehaviorComponent:
    blocked = _source_blocker(series, decision, "LOCATION")
    return blocked or _component_from_metrics(
        "LOCATION", series, calculate_location_metrics(series.high, series.low, series.close, series.interval),
    )


def assemble_relative_strength_component(
    series: FinalizedBarSeries, benchmark: FinalizedBarSeries, decision: DecisionWatermark,
) -> BehaviorComponent:
    if series.interval != "1d" or benchmark.interval != "1d":
        return _not_applicable("RELATIVE_STRENGTH", series.interval)
    if series.security_id == benchmark.security_id:
        return _unavailable("RELATIVE_STRENGTH", series.interval, "BENCHMARK_SECURITY_NOT_DISTINCT")
    if series.bar_ends != benchmark.bar_ends or series.adjusted != benchmark.adjusted:
        return _unavailable("RELATIVE_STRENGTH", series.interval, "BENCHMARK_WINDOW_OR_BASIS_MISMATCH")
    for candidate in (series, benchmark):
        blocked = _source_blocker(candidate, decision, "RELATIVE_STRENGTH")
        if blocked is not None:
            return _unavailable("RELATIVE_STRENGTH", series.interval, blocked.reason_codes[0])
    metric = calculate_relative_strength_metric(
        series.close, benchmark.close, benchmark.security_id, series.interval,
    )
    return _component_from_metrics("RELATIVE_STRENGTH", series, (metric,), sources=(series.source, benchmark.source))


def assemble_interval_components(
    series: FinalizedBarSeries, decision: DecisionWatermark,
) -> tuple[BehaviorComponent, ...]:
    """Build every v1 non-benchmark factor defined for one selected interval."""
    components = (
        assemble_trend_component(series, decision),
        assemble_momentum_component(series, decision),
        assemble_volatility_component(series, decision),
        assemble_participation_component(series, decision),
        assemble_location_component(series, decision),
    )
    return tuple(component for component in components if component.status != "NOT_APPLICABLE")


def assemble_stock_behavior_snapshot(
    series_by_interval: Mapping[Interval, FinalizedBarSeries | None],
    decision: DecisionWatermark,
    *,
    security_id: UUID,
    security_revision_id: UUID,
    ticker: str,
    computed_at: datetime,
    available_at: datetime,
    valid_until: datetime,
    benchmark_daily: FinalizedBarSeries | None = None,
) -> StockBehaviorSnapshot:
    """Assemble one immutable v1 profile from explicitly selected interval inputs."""
    if set(series_by_interval) != {"1d", "1h", "30m"}:
        raise ValueError("profile assembly requires explicit 1d, 1h and 30m inputs")
    if decision.observed_time > computed_at or computed_at > available_at:
        raise ValueError("assembly clocks must follow the source observation cutoff")
    selected = tuple(series for series in series_by_interval.values() if series is not None)
    if any(series.security_id != security_id for series in selected):
        raise ValueError("all interval inputs must match the snapshot security")
    if any(series.ticker != ticker.upper() for series in selected):
        raise ValueError("all interval inputs must match the snapshot ticker")
    components = []
    for interval in ("1d", "1h", "30m"):
        series = series_by_interval[interval]
        if series is None:
            components.append(_unavailable("TREND", interval, "SOURCE_INTERVAL_MISSING"))
            continue
        if series.interval != interval:
            raise ValueError("interval map key must match the finalized series")
        components.extend(assemble_interval_components(series, decision))

    daily = series_by_interval["1d"]
    if daily is not None and benchmark_daily is not None:
        components.append(assemble_relative_strength_component(daily, benchmark_daily, decision))
    sources = [source for component in components for source in component.sources]
    availability_mode = (
        "PROSPECTIVE_RECEIPT"
        if all(source.availability_mode == "PROSPECTIVE_RECEIPT" for source in sources)
        else "RECONSTRUCTED"
    )
    required_components = tuple(key for key, _ in OPTIONS_SWING_PROFILE.required_metrics)
    return StockBehaviorSnapshot(
        security_id=security_id, security_revision_id=security_revision_id, ticker=ticker,
        policy_sha256=OPTIONS_SWING_PROFILE.sha256, market_time=decision.market_time,
        computed_at=computed_at, available_at=available_at, valid_until=valid_until,
        availability_mode=availability_mode, components=tuple(components),
        required_components=required_components,
    )


def _source_blocker(
    series: FinalizedBarSeries, decision: DecisionWatermark, factor: Factor,
) -> BehaviorComponent | None:
    source = series.source
    if source.market_time > decision.market_time:
        return _unavailable(factor, series.interval, "SOURCE_MARKET_TIME_AFTER_CUTOFF")
    if source.received_at > decision.observed_time or any(available_at > decision.observed_time for available_at in series.bar_available_ats):
        return _unavailable(factor, series.interval, "SOURCE_RECEIPT_AFTER_CUTOFF")
    if source.valid_until <= decision.observed_time:
        return BehaviorComponent(
            factor=factor, interval=series.interval, status="STALE",
            sources=(source,), reason_codes=("SOURCE_EXPIRED_AT_CUTOFF",),
        )
    return None


def _component_from_metrics(
    factor: Factor, series: FinalizedBarSeries, metrics: tuple[BehaviorMetric, ...],
    *, sources: tuple[BehaviorSource, ...] | None = None,
) -> BehaviorComponent:
    component_sources = sources or (series.source,)
    if all(metric.status == "READY" for metric in metrics):
        return BehaviorComponent(
            factor=factor, interval=series.interval, status="READY",
            metrics=metrics, sources=component_sources,
        )
    reason_codes = tuple(dict.fromkeys(
        reason for metric in metrics if metric.status != "READY" for reason in metric.reason_codes
    ))
    status = "INSUFFICIENT_HISTORY" if all(
        metric.status in ("READY", "INSUFFICIENT_HISTORY") for metric in metrics
    ) else "UNAVAILABLE"
    return BehaviorComponent(
        factor=factor, interval=series.interval, status=status,
        metrics=metrics, sources=component_sources, reason_codes=reason_codes,
    )


def _not_applicable(factor: Factor, interval: Interval) -> BehaviorComponent:
    return BehaviorComponent(
        factor=factor, interval=interval, status="NOT_APPLICABLE",
        reason_codes=("FACTOR_NOT_DEFINED_FOR_INTERVAL",),
    )


def _unavailable(factor: Factor, interval: Interval, reason: str) -> BehaviorComponent:
    return BehaviorComponent(
        factor=factor, interval=interval, status="UNAVAILABLE", reason_codes=(reason,),
    )