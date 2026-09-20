"""Bounded prospective producer for immutable stock behavior snapshots."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Sequence

from .behavior_assembly import assemble_stock_behavior_snapshot
from .behavior_sources import (
    bind_adjusted_daily_evidence,
    bind_adjusted_daily_history,
    bind_feature_source,
    bind_raw_source_evidence,
    build_adjusted_daily_evidence_draft,
    build_raw_source_evidence_draft,
    source_tail_bars_by_interval,
)
from .domain import DecisionWatermark, SecurityReferenceRevision
from .repositories import (
    EquityBarRepository,
    EquityCorporateActionRepository,
    EquityEvidenceRepository,
)


@dataclass(frozen=True, slots=True)
class BehaviorProductionResult:
    market_time: datetime
    received_at: datetime
    attempted: int
    inserted: int
    existing: int
    skipped: tuple[tuple[str, tuple[str, ...]], ...]
    failed: tuple[tuple[str, str], ...]
    planned: int = 0


def produce_stock_behavior_snapshots(
    securities: Sequence[SecurityReferenceRevision],
    *,
    watermark: DecisionWatermark,
    evidence_repository: EquityEvidenceRepository,
    bar_repository: EquityBarRepository,
    corporate_action_repository: EquityCorporateActionRepository,
    persist: bool = True,
    received_at: datetime | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> BehaviorProductionResult:
    security_by_ticker = {security.ticker: security for security in securities}
    if len(security_by_ticker) != len(securities):
        raise ValueError("behavior producer requires distinct security tickers")
    tickers = tuple(security_by_ticker)
    if not tickers:
        raise ValueError("behavior producer requires a bounded non-empty security scope")
    limits = source_tail_bars_by_interval()
    raw_reads = evidence_repository.read_behavior_feature_sources(
        tickers, watermark, intervals=("1h", "30m"),
        maximum_source_bars=max(limits.values()),
        source_bars_by_interval={interval: limits[interval] for interval in ("1h", "30m")},
    )
    adjusted_reads = bar_repository.read_behavior_adjusted_daily(
        tickers, watermark, limit_per_ticker=limits["1d"],
    )
    selected_raw_bars = tuple(bar for read in raw_reads for bar in read.bars)
    coverage_rows, actions_by_coverage = ((), {})
    if selected_raw_bars:
        coverage_rows, actions_by_coverage = corporate_action_repository.read_behavior_split_coverage(
            tickers, watermark,
            window_start=min(bar.session_date for bar in selected_raw_bars),
            window_end=max(bar.session_date for bar in selected_raw_bars),
        )
    read_completed_at = received_at or clock()
    if (
        read_completed_at.tzinfo is None or read_completed_at.utcoffset() is None
        or read_completed_at < watermark.observed_time
    ):
        raise ValueError("behavior producer receipt must follow its read cutoff")

    raw_by_key = {(read.evidence.ticker, read.evidence.interval): read for read in raw_reads}
    adjusted_by_ticker = {read.ticker: read for read in adjusted_reads}
    daily_inputs = {}
    raw_inputs = {}
    preparation_blockers: dict[str, list[str]] = {ticker: [] for ticker in tickers}
    for ticker, security in security_by_ticker.items():
        adjusted = adjusted_by_ticker.get(ticker)
        if adjusted is None:
            preparation_blockers[ticker].append("ADJUSTED_DAILY_HISTORY_MISSING")
        else:
            history = bind_adjusted_daily_history(
                adjusted.bars, adjusted.bar_created_ats,
                security_id=security.security_id, ticker=ticker,
                received_at=read_completed_at,
            )
            if history.status != "READY_FOR_DERIVED_EVIDENCE":
                preparation_blockers[ticker].extend(history.blocker_codes)
            else:
                evidence = build_adjusted_daily_evidence_draft(
                    history, security_revision_id=security.security_revision_id,
                )
                if evidence.valid_until is None or evidence.valid_until <= read_completed_at:
                    preparation_blockers[ticker].append("ADJUSTED_DAILY_EVIDENCE_EXPIRED_AT_RECEIPT")
                else:
                    daily_inputs[ticker] = (history, evidence)
        for interval in ("1h", "30m"):
            read = raw_by_key.get((ticker, interval))
            if read is None:
                preparation_blockers[ticker].append(f"{interval.upper()}_FEATURE_EVIDENCE_MISSING")
                continue
            binding = bind_feature_source(
                read, coverage_rows, actions_by_coverage, received_at=read_completed_at,
            )
            if binding.status != "READY":
                preparation_blockers[ticker].extend(binding.blocker_codes or (
                    f"{interval.upper()}_SOURCE_{binding.status}",
                ))
            else:
                raw_inputs[(ticker, interval)] = (
                    binding,
                    build_raw_source_evidence_draft(
                        binding, security_revision_id=security.security_revision_id,
                    ),
                )

    inserted = existing = planned = 0
    skipped = []
    failed = []
    for ticker, security in security_by_ticker.items():
        blockers = tuple(dict.fromkeys(preparation_blockers[ticker]))
        if blockers:
            skipped.append((ticker, blockers))
            continue
        history, daily_evidence = daily_inputs[ticker]
        hourly_binding, hourly_evidence = raw_inputs[(ticker, "1h")]
        half_hour_binding, half_hour_evidence = raw_inputs[(ticker, "30m")]
        benchmark = daily_inputs.get("SPY") if ticker != "SPY" else None
        if benchmark is not None and tuple(
            bar.bar_end for bar in benchmark[0].bars
        ) != tuple(bar.bar_end for bar in history.bars):
            benchmark = None
        derived_evidence = [daily_evidence, hourly_evidence, half_hour_evidence]
        if benchmark is not None:
            derived_evidence.append(benchmark[1])
        required_valid_until = min(
            daily_evidence.valid_until,
            hourly_evidence.valid_until,
            half_hour_evidence.valid_until,
        )

        def build_snapshot(clocks, *, current_security=security, current_history=history,
                           current_daily=daily_evidence, current_hourly=hourly_binding,
                           current_hourly_evidence=hourly_evidence,
                           current_half_hour=half_hour_binding,
                           current_half_hour_evidence=half_hour_evidence,
                           current_benchmark=benchmark,
                           current_valid_until=required_valid_until):
            assembly_at = max(read_completed_at, clock(), *clocks.values())
            if current_valid_until <= assembly_at:
                raise ValueError("behavior sources expired before atomic persistence")
            series_by_interval = {
                "1d": bind_adjusted_daily_evidence(
                    current_history, current_daily,
                    evidence_recorded_at=clocks[current_daily.evidence_id],
                    received_at=assembly_at,
                ),
                "1h": bind_raw_source_evidence(
                    current_hourly, current_hourly_evidence,
                    evidence_recorded_at=clocks[current_hourly_evidence.evidence_id],
                    received_at=assembly_at,
                ),
                "30m": bind_raw_source_evidence(
                    current_half_hour, current_half_hour_evidence,
                    evidence_recorded_at=clocks[current_half_hour_evidence.evidence_id],
                    received_at=assembly_at,
                ),
            }
            benchmark_series = None
            if current_benchmark is not None:
                benchmark_history, benchmark_evidence = current_benchmark
                benchmark_series = bind_adjusted_daily_evidence(
                    benchmark_history, benchmark_evidence,
                    evidence_recorded_at=clocks[benchmark_evidence.evidence_id],
                    received_at=assembly_at,
                )
            return assemble_stock_behavior_snapshot(
                series_by_interval,
                DecisionWatermark(watermark.market_time, assembly_at),
                security_id=current_security.security_id,
                security_revision_id=current_security.security_revision_id,
                ticker=current_security.ticker,
                computed_at=assembly_at, available_at=assembly_at,
                valid_until=current_valid_until,
                benchmark_daily=benchmark_series,
            )

        try:
            if not persist:
                build_snapshot({
                    row.evidence_id: max(read_completed_at, row.observed_at)
                    for row in derived_evidence
                })
                planned += 1
                continue
            _, was_inserted = evidence_repository.persist_behavior(
                build_snapshot, derived_evidence=tuple(derived_evidence),
            )
            if was_inserted:
                inserted += 1
            else:
                existing += 1
        except Exception as exc:
            failed.append((ticker, type(exc).__name__))
    return BehaviorProductionResult(
        market_time=watermark.market_time, received_at=read_completed_at,
        attempted=len(tickers), inserted=inserted, existing=existing,
        skipped=tuple(skipped), failed=tuple(failed), planned=planned,
    )