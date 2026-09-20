"""Read-only coverage assessment for legacy feature evidence and v1 behavior metrics."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime

from .behavior import Contract, Interval, Name
from .behavior_calculators import (
    calculate_location_metrics,
    calculate_momentum_metrics,
    calculate_participation_metrics,
    calculate_relative_strength_metric,
    calculate_trend_metrics,
    calculate_volatility_metrics,
)
from .domain import BarAvailabilityMode, BarSessionScope, DecisionWatermark, EquityBarRevision, QualityState
from .repositories import BehaviorFeatureSourceRead, BehaviorGroupedDailyRead
from .behavior_sources import (
    bind_adjusted_daily_history, bind_feature_source,
    bind_provider_adjustment_continuity, split_coverage_gap_sessions,
)


class MetricCoverage(Contract):
    metric_id: Name
    status: Literal["READY", "STALE", "INSUFFICIENT_HISTORY", "UNAVAILABLE", "NOT_APPLICABLE"]
    value: float | None
    sample_count: int
    reason_codes: tuple[Name, ...]
    benchmark_security_id: UUID | None = None


class FeatureSourceCoverage(Contract):
    ticker: Name
    security_id: UUID | None
    interval: Interval
    evidence_id: UUID | None
    evidence_market_time: AwareDatetime | None
    evidence_observed_at: AwareDatetime | None
    evidence_created_at: AwareDatetime | None
    evidence_valid_until: AwareDatetime | None
    source_revision_count: int
    available_revision_count: int
    source_window_start: date | None
    source_window_end: date | None
    earliest_legal_decision_at: AwareDatetime
    market_to_receipt_seconds: float | None
    observation_to_receipt_seconds: float | None
    recording_to_receipt_seconds: float | None
    mathematical_status: Literal["READY", "PARTIAL", "UNAVAILABLE"]
    contract_status: Literal["READY", "STALE", "UNAVAILABLE"]
    source_policy_sha256: str | None = None
    action_coverage_ids: tuple[UUID, ...] = ()
    metrics: tuple[MetricCoverage, ...] = ()
    blocker_codes: tuple[Name, ...]


def assess_feature_source_coverage(
    read: BehaviorFeatureSourceRead, cutoff: DecisionWatermark, received_at: datetime,
    coverage_rows: Sequence[Mapping] = (),
    actions_by_coverage: Mapping[UUID, Sequence[Mapping]] | None = None,
) -> FeatureSourceCoverage:
    if received_at.tzinfo is None or received_at.utcoffset() is None or received_at < cutoff.observed_time:
        raise ValueError("report receipt must be timezone-aware and follow its read cutoff")
    evidence = read.evidence
    blockers = []
    if read.missing_revision_ids or len(read.bars) != len(read.selected_revision_ids):
        blockers.append("SOURCE_REVISION_LINEAGE_INCOMPLETE")
    if tuple(bar.bar_revision_id for bar in read.bars) != read.selected_revision_ids:
        blockers.append("SOURCE_REVISION_ORDER_MISMATCH")
    if len(read.bar_created_ats) != len(read.bars):
        blockers.append("BAR_RECORDING_CLOCKS_INCOMPLETE")
    if evidence.market_time > cutoff.market_time or evidence.observed_at > cutoff.observed_time or read.evidence_created_at > cutoff.observed_time:
        blockers.append("EVIDENCE_AFTER_REPORT_CUTOFF")
    if any(created_at > cutoff.observed_time for created_at in read.bar_created_ats) or any(
        max(clock for clock in (
            bar.bar_end, bar.system_observed_at, bar.provider_published_at,
            bar.replay_available_at,
        ) if clock is not None) > cutoff.observed_time
        for bar in read.bars
    ):
        blockers.append("BAR_AFTER_REPORT_CUTOFF")
    if read.bars and (
        evidence.latest_bar_revision_id != read.bars[-1].bar_revision_id
        or evidence.market_time != read.bars[-1].bar_end
    ):
        blockers.append("LATEST_BAR_IDENTITY_MISMATCH")
    if any(
        not bar.is_final or bar.session_scope is not BarSessionScope.RTH
        or bar.security_id != evidence.security_id or bar.ticker != evidence.ticker
        or bar.interval != evidence.interval
        for bar in read.bars
    ):
        blockers.append("BAR_IDENTITY_OR_FINALITY_MISMATCH")
    if evidence.quality_state is not QualityState.COMPLETE:
        blockers.append("FEATURE_EVIDENCE_NOT_COMPLETE")
    if evidence.valid_until is None or evidence.valid_until <= cutoff.observed_time:
        blockers.append("FEATURE_EVIDENCE_EXPIRED_AT_REPORT")
    binding = bind_feature_source(
        read, coverage_rows, actions_by_coverage or {}, received_at=received_at,
    )
    blockers.extend(binding.blocker_codes)

    metrics = ()
    lineage_blockers = {
        "SOURCE_REVISION_LINEAGE_INCOMPLETE", "SOURCE_REVISION_ORDER_MISMATCH",
        "BAR_RECORDING_CLOCKS_INCOMPLETE", "EVIDENCE_AFTER_REPORT_CUTOFF",
        "BAR_AFTER_REPORT_CUTOFF", "LATEST_BAR_IDENTITY_MISMATCH",
        "BAR_IDENTITY_OR_FINALITY_MISMATCH",
    }
    if read.bars and not lineage_blockers.intersection(blockers):
        high = tuple(float(bar.high_price) for bar in read.bars)
        low = tuple(float(bar.low_price) for bar in read.bars)
        close = tuple(float(bar.close_price) for bar in read.bars)
        volume = tuple(float(bar.volume) for bar in read.bars)
        calculated = (
            *calculate_trend_metrics(high, low, close, evidence.interval),
            *calculate_momentum_metrics(close, evidence.interval),
            *calculate_volatility_metrics(high, low, close, evidence.interval),
            *(() if evidence.interval != "1d" else calculate_participation_metrics(close, volume, evidence.interval)),
            *calculate_location_metrics(high, low, close, evidence.interval),
        )
        metrics = tuple(MetricCoverage(
            metric_id=metric.definition.metric_id, status=metric.status, value=metric.value,
            sample_count=metric.sample_count, reason_codes=metric.reason_codes,
            benchmark_security_id=metric.benchmark_security_id,
        ) for metric in calculated)
    ready = sum(metric.status == "READY" for metric in metrics)
    mathematical_status = "READY" if metrics and ready == len(metrics) else "PARTIAL" if ready else "UNAVAILABLE"
    return FeatureSourceCoverage(
        ticker=evidence.ticker, security_id=evidence.security_id, interval=evidence.interval,
        evidence_id=evidence.evidence_id, evidence_market_time=evidence.market_time,
        evidence_observed_at=evidence.observed_at, evidence_created_at=read.evidence_created_at,
        evidence_valid_until=evidence.valid_until,
        source_revision_count=len(evidence.source_revision_ids),
        available_revision_count=len(read.bars),
        source_window_start=read.bars[0].session_date if read.bars else None,
        source_window_end=read.bars[-1].session_date if read.bars else None,
        earliest_legal_decision_at=received_at,
        market_to_receipt_seconds=(received_at - evidence.market_time).total_seconds(),
        observation_to_receipt_seconds=(received_at - evidence.observed_at).total_seconds(),
        recording_to_receipt_seconds=(received_at - read.evidence_created_at).total_seconds(),
        mathematical_status=mathematical_status, metrics=metrics,
        contract_status=binding.status, source_policy_sha256=binding.policy_sha256,
        action_coverage_ids=binding.coverage_ids,
        blocker_codes=tuple(dict.fromkeys(blockers)),
    )


def missing_source_coverage(
    ticker: str, interval: Interval, cutoff: DecisionWatermark, received_at: datetime,
) -> FeatureSourceCoverage:
    return FeatureSourceCoverage(
        ticker=ticker, security_id=None, interval=interval, evidence_id=None,
        evidence_market_time=None, evidence_observed_at=None, evidence_created_at=None,
        evidence_valid_until=None, source_revision_count=0, available_revision_count=0,
        source_window_start=None, source_window_end=None,
        earliest_legal_decision_at=received_at, market_to_receipt_seconds=None,
        observation_to_receipt_seconds=None, recording_to_receipt_seconds=None,
        mathematical_status="UNAVAILABLE", contract_status="UNAVAILABLE",
        blocker_codes=("FEATURE_EVIDENCE_MISSING",),
    )


def add_spy_relative_strength(
    row: FeatureSourceCoverage,
    read: BehaviorFeatureSourceRead,
    spy_read: BehaviorFeatureSourceRead | None,
) -> FeatureSourceCoverage:
    if row.interval != "1d" or row.ticker == "SPY":
        return row
    if spy_read is None or row.mathematical_status == "UNAVAILABLE":
        return row.model_copy(update={"blocker_codes": (*row.blocker_codes, "SPY_BENCHMARK_UNAVAILABLE")})
    if tuple(bar.bar_end for bar in read.bars) != tuple(bar.bar_end for bar in spy_read.bars):
        return row.model_copy(update={"blocker_codes": (*row.blocker_codes, "SPY_BENCHMARK_WINDOW_MISMATCH")})
    metric = calculate_relative_strength_metric(
        tuple(float(bar.close_price) for bar in read.bars),
        tuple(float(bar.close_price) for bar in spy_read.bars),
        spy_read.evidence.security_id, "1d",
    )
    coverage = MetricCoverage(
        metric_id=metric.definition.metric_id, status=metric.status, value=metric.value,
        sample_count=metric.sample_count, reason_codes=metric.reason_codes,
        benchmark_security_id=metric.benchmark_security_id,
    )
    metrics = (*row.metrics, coverage)
    ready = sum(item.status == "READY" for item in metrics)
    status = "READY" if ready == len(metrics) else "PARTIAL" if ready else "UNAVAILABLE"
    return row.model_copy(update={"metrics": metrics, "mathematical_status": status})


def summarize_coverage(
    reads: tuple[BehaviorFeatureSourceRead, ...], tickers: tuple[str, ...],
    intervals: tuple[Interval, ...], cutoff: DecisionWatermark, received_at: datetime,
    coverage_rows: Sequence[Mapping] = (),
    actions_by_coverage: Mapping[UUID, Sequence[Mapping]] | None = None,
) -> dict:
    by_key = {(read.evidence.ticker, read.evidence.interval): read for read in reads}
    response_bound_coverage = tuple(
        row for row in coverage_rows
        if row.get("security_id") is not None
        and row.get("response_action_count") is not None
        and row.get("response_sha256") is not None
    )
    bar_mode_counts = _counts(
        bar.availability_mode.value for read in reads for bar in read.bars[-1:]
    )
    coverage_mode_counts = _counts(
        getattr(row.get("availability_mode"), "value", row.get("availability_mode"))
        for row in response_bound_coverage
    )
    spy_daily = by_key.get(("SPY", "1d"))
    rows = []
    for ticker in tickers:
        for interval in intervals:
            read = by_key.get((ticker, interval))
            row = (
                missing_source_coverage(ticker, interval, cutoff, received_at)
                if read is None else assess_feature_source_coverage(
                    read, cutoff, received_at, coverage_rows, actions_by_coverage,
                )
            )
            if read is not None:
                row = add_spy_relative_strength(row, read, spy_daily)
            rows.append(row)
    return {
        "contract": "STOCK_BEHAVIOR_COVERAGE_V1",
        "generated_at": received_at.isoformat(),
        "read_started_at": cutoff.observed_time.isoformat(),
        "received_at": received_at.isoformat(),
        "market_cutoff": cutoff.market_time.isoformat(),
        "observed_cutoff": cutoff.observed_time.isoformat(),
        "expected_rows": len(tickers) * len(intervals),
        "observed_rows": len(reads),
        "split_coverage_rows": len(coverage_rows),
        "response_bound_split_coverage_rows": len(response_bound_coverage),
        "response_bound_split_coverage_tickers": sorted({row["ticker"] for row in response_bound_coverage}),
        "feature_bar_mode_counts": bar_mode_counts,
        "response_bound_split_coverage_mode_counts": coverage_mode_counts,
        "split_coverage_window_start": (
            min(row["window_start"] for row in response_bound_coverage).isoformat()
            if response_bound_coverage else None
        ),
        "split_coverage_window_end": (
            max(row["window_end"] for row in response_bound_coverage).isoformat()
            if response_bound_coverage else None
        ),
        "mathematical_status_counts": _counts(row.mathematical_status for row in rows),
        "contract_status_counts": _counts(row.contract_status for row in rows),
        "contract_status_counts_by_interval": {
            interval: _counts(row.contract_status for row in rows if row.interval == interval)
            for interval in intervals
        },
        "blocker_counts_by_interval": {
            interval: _counts(
                code for row in rows if row.interval == interval for code in row.blocker_codes
            ) for interval in intervals
        },
        "source_windows_by_interval": {
            interval: _source_window(rows, interval) for interval in intervals
        },
        "blocker_counts": _counts(code for row in rows for code in row.blocker_codes),
        "option_timing_status": (
            "NOT_ASSESSED_STOCK_SOURCE_CONTRACT_UNAVAILABLE"
            if any(row.contract_status != "READY" for row in rows)
            else "READY_FOR_SEPARATE_OPTION_TIMING_ASSESSMENT"
        ),
        "rows": [row.model_dump(mode="json") for row in rows],
    }


def _counts(values) -> dict[str, int]:
    result = {}
    for value in values:
        result[value] = result.get(value, 0) + 1
    return dict(sorted(result.items()))


def _source_window(rows: Sequence[FeatureSourceCoverage], interval: Interval) -> dict[str, str | None]:
    selected = [row for row in rows if row.interval == interval and row.source_window_start is not None]
    return {
        "start": min(row.source_window_start for row in selected).isoformat() if selected else None,
        "end": max(row.source_window_end for row in selected).isoformat() if selected else None,
    }


def summarize_adjusted_bar_inventory(
    reads: tuple[BehaviorFeatureSourceRead, ...],
    adjusted_by_interval: Mapping[Interval, Mapping[str, tuple[EquityBarRevision, ...]]],
    tickers: tuple[str, ...],
    adjusted_daily_recorded_ats: Mapping[str, tuple[datetime, ...]] | None = None,
    received_at: datetime | None = None,
) -> dict:
    recorded_by_ticker = adjusted_daily_recorded_ats or {}
    evidence_security = {
        (read.evidence.ticker, read.evidence.interval): read.evidence.security_id
        for read in reads
    }
    required = {"1d": 273, "1h": 200, "30m": 200}
    rows = []
    for interval, minimum_bars in required.items():
        for ticker in tickers:
            bars = tuple(adjusted_by_interval.get(interval, {}).get(ticker, ()))
            modes = sorted({bar.availability_mode.value for bar in bars})
            expected_security = evidence_security.get((ticker, interval))
            identity_match = bool(bars) and expected_security is not None and all(
                bar.security_id == expected_security for bar in bars
            )
            blockers = []
            if len(bars) < minimum_bars:
                blockers.append("ADJUSTED_HISTORY_SHORT")
            if bars and not identity_match:
                blockers.append("ADJUSTED_SECURITY_IDENTITY_MISMATCH")
            derived_evidence_candidate_ready = False
            source_manifest_sha256 = None
            history_available_at = None
            if interval == "1d" and expected_security is not None and received_at is not None:
                history = bind_adjusted_daily_history(
                    bars, recorded_by_ticker.get(ticker, ()),
                    security_id=expected_security, ticker=ticker, received_at=received_at,
                )
                derived_evidence_candidate_ready = history.status == "READY_FOR_DERIVED_EVIDENCE"
                source_manifest_sha256 = history.source_manifest_sha256
                history_available_at = history.history_available_at.isoformat() if history.history_available_at else None
                blockers.extend(history.blocker_codes)
                if derived_evidence_candidate_ready:
                    blockers.append("DERIVED_EVIDENCE_NOT_MATERIALIZED")
            elif bars and any(bar.availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED for bar in bars):
                blockers.append("ADJUSTED_HISTORY_RECONSTRUCTED")
            rows.append({
                "ticker": ticker, "interval": interval, "bars": len(bars),
                "minimum_bars": minimum_bars,
                "window_start": bars[0].session_date.isoformat() if bars else None,
                "window_end": bars[-1].session_date.isoformat() if bars else None,
                "availability_modes": modes, "security_identity_match": identity_match,
                "mathematical_history_ready": len(bars) >= minimum_bars and identity_match,
                "derived_evidence_candidate_ready": derived_evidence_candidate_ready,
                "source_manifest_sha256": source_manifest_sha256,
                "history_available_at": history_available_at,
                "prospective_behavior_contract_ready": False,
                "blocker_codes": blockers,
            })
    return {
        "rows": rows,
        "mathematical_history_ready_counts_by_interval": {
            interval: sum(
                row["mathematical_history_ready"] for row in rows if row["interval"] == interval
            ) for interval in required
        },
        "mathematical_history_ready_tickers_by_interval": {
            interval: sorted(
                row["ticker"] for row in rows
                if row["interval"] == interval and row["mathematical_history_ready"]
            ) for interval in required
        },
        "mathematical_history_unready_tickers_by_interval": {
            interval: sorted(
                row["ticker"] for row in rows
                if row["interval"] == interval and not row["mathematical_history_ready"]
            ) for interval in required
        },
        "bar_counts_by_interval": {
            interval: sum(row["bars"] for row in rows if row["interval"] == interval)
            for interval in required
        },
        "prospective_behavior_contract_ready": 0,
        "derived_evidence_candidate_counts_by_interval": {
            interval: sum(
                row["derived_evidence_candidate_ready"] for row in rows if row["interval"] == interval
            ) for interval in required
        },
        "blocker_counts": _counts(code for row in rows for code in row["blocker_codes"]),
    }


def summarize_hourly_split_continuity(
    reads: tuple[BehaviorFeatureSourceRead, ...],
    raw_grouped_daily_reads: tuple[BehaviorGroupedDailyRead, ...],
    adjusted_daily_reads: tuple[BehaviorGroupedDailyRead, ...],
    coverage_rows: Sequence[Mapping],
    actions_by_coverage: Mapping[UUID, Sequence[Mapping]],
    tickers: tuple[str, ...],
    received_at: datetime,
) -> dict:
    by_key = {(read.evidence.ticker, read.evidence.interval): read for read in reads}
    raw_grouped = {read.ticker: read for read in raw_grouped_daily_reads}
    adjusted = {read.ticker: read for read in adjusted_daily_reads}
    rows = []
    for ticker in tickers:
        raw_daily = raw_grouped.get(ticker)
        hourly = by_key.get((ticker, "1h"))
        adjusted_daily = adjusted.get(ticker)
        blockers = []
        gap_sessions = ()
        review = None
        if raw_daily is None:
            blockers.append("RAW_DAILY_SOURCE_MISSING")
        if hourly is None:
            blockers.append("RAW_HOURLY_SOURCE_MISSING")
        if adjusted_daily is None:
            blockers.append("ADJUSTED_DAILY_SOURCE_MISSING")
        if not blockers:
            try:
                gap_sessions = split_coverage_gap_sessions(
                    coverage_rows, actions_by_coverage, ticker=ticker,
                    security_id=hourly.evidence.security_id,
                    sessions=tuple(bar.session_date for bar in hourly.bars),
                    received_at=received_at,
                )
            except ValueError:
                blockers.append("SPLIT_COVERAGE_PRESENT_BUT_INVALID")
        if not blockers and gap_sessions:
            review = bind_provider_adjustment_continuity(
                raw_daily.bars, raw_daily.bar_created_ats,
                adjusted_daily.bars, adjusted_daily.bar_created_ats,
                required_sessions=gap_sessions, security_id=hourly.evidence.security_id,
                ticker=ticker, received_at=received_at,
            )
            blockers.extend(review.blocker_codes)
            if review.status == "READY_FOR_DERIVED_EVIDENCE":
                blockers.append("RESPONSE_COVERAGE_STILL_REQUIRED")
        status = (
            "RESPONSE_COVERAGE_COMPLETE" if not blockers and not gap_sessions
            else "DIAGNOSTIC_STABLE_FACTOR" if review is not None and review.status == "READY_FOR_DERIVED_EVIDENCE"
            else "UNAVAILABLE"
        )
        rows.append({
            "ticker": ticker, "status": status,
            "gap_session_count": len(gap_sessions),
            "gap_start": gap_sessions[0].isoformat() if gap_sessions else None,
            "gap_end": gap_sessions[-1].isoformat() if gap_sessions else None,
            "review_manifest_sha256": review.review_manifest_sha256 if review else None,
            "median_close_factor": review.median_close_factor if review else None,
            "minimum_close_factor": review.minimum_close_factor if review else None,
            "maximum_close_factor": review.maximum_close_factor if review else None,
            "largest_deviation_session": review.largest_deviation_session.isoformat() if review and review.largest_deviation_session else None,
            "largest_deviation_factor": review.largest_deviation_factor if review else None,
            "largest_deviation_raw_close": review.largest_deviation_raw_close if review else None,
            "largest_deviation_adjusted_close": review.largest_deviation_adjusted_close if review else None,
            "blocker_codes": tuple(dict.fromkeys(blockers)),
        })
    return {
        "rows": rows,
        "status_counts": _counts(row["status"] for row in rows),
        "candidate_tickers": sorted(
            row["ticker"] for row in rows if row["status"] == "DIAGNOSTIC_STABLE_FACTOR"
        ),
        "unavailable_tickers": sorted(
            row["ticker"] for row in rows if row["status"] == "UNAVAILABLE"
        ),
        "blocker_counts": _counts(code for row in rows for code in row["blocker_codes"]),
    }