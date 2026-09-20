from dataclasses import asdict, replace
from datetime import timedelta
from uuid import UUID

from equity.behavior_producer import produce_stock_behavior_snapshots
from equity.behavior_sources import snapshot_uses_current_source_policies
from equity.domain import BarAvailabilityMode, DecisionWatermark
from equity.repositories import BehaviorGroupedDailyRead
from scripts.run_corporate_action_worker import build_coverage
from test_equity_behavior_coverage import NOW, feature_read
from test_equity_orchestration import security


def source_read(ticker, interval, security_id, security_revision_id):
    read = feature_read(ticker, count=273 if interval == "1d" else 200)
    bars = tuple(replace(bar, security_id=security_id, interval=interval) for bar in read.bars)
    evidence = replace(
        read.evidence, security_id=security_id, security_revision_id=security_revision_id,
        interval=interval, evidence_key=f"feature:{ticker}:{interval}",
        lifecycle_key=f"feature:{ticker}:{interval}",
        latest_bar_revision_id=bars[-1].bar_revision_id,
        source_revision_ids=tuple(bar.bar_revision_id for bar in bars),
        market_time=bars[-1].bar_end,
    )
    return replace(
        read, evidence=evidence, selected_revision_ids=evidence.source_revision_ids,
        bars=bars, missing_revision_ids=(),
    )


def adjusted_read(ticker, security_id, security_revision_id):
    read = source_read(ticker, "1d", security_id, security_revision_id)
    bars = tuple(replace(
        bar, adjusted=True,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        replay_available_at=bar.system_observed_at,
        quality_codes=("GROUPED_DAILY_EXACT_TICKER_V2",),
    ) for bar in read.bars)
    return BehaviorGroupedDailyRead(
        ticker=ticker, bars=bars, bar_created_ats=read.bar_created_ats,
    )


class EvidenceRepository:
    def __init__(self, reads, recorded_at):
        self.reads = tuple(reads)
        self.recorded_at = recorded_at
        self.persisted_evidence = {}
        self.snapshots = {}
        self.calls = 0

    def read_behavior_feature_sources(self, *args, **kwargs):
        return self.reads

    def persist_behavior(self, factory, *, derived_evidence):
        self.calls += 1
        for row in derived_evidence:
            existing = self.persisted_evidence.setdefault(row.evidence_id, row)
            assert existing == row
        clocks = {row.evidence_id: self.recorded_at for row in derived_evidence}
        requested = factory(clocks)
        existing = self.snapshots.setdefault(requested.snapshot_id, requested)
        return existing, existing is requested


class BarRepository:
    def __init__(self, reads):
        self.reads = tuple(reads)

    def read_behavior_adjusted_daily(self, *args, **kwargs):
        return self.reads


class ActionRepository:
    def __init__(self, coverage):
        self.coverage = tuple(coverage)

    def read_behavior_split_coverage(self, *args, **kwargs):
        return self.coverage, {
            UUID(str(row["coverage_id"])): () for row in self.coverage
        }


def coverage_for(read):
    row = build_coverage(
        {read.evidence.ticker: read.evidence.security_id},
        start=read.bars[0].session_date, end=read.bars[-1].session_date,
        observed_at=NOW - timedelta(seconds=1),
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        responses={"SPLIT": [], "DIVIDEND": []},
    )[0]
    return {
        **asdict(row), "availability_mode": row.availability_mode.value,
        "created_at": NOW - timedelta(seconds=1),
    }


def inputs():
    securities = []
    raw_reads = []
    adjusted_reads = []
    coverage = []
    for ticker in ("AAPL", "SPY"):
        item = security(ticker)
        securities.append(item)
        hourly = source_read(
            ticker, "1h", item.security_id, item.security_revision_id,
        )
        half_hour = source_read(
            ticker, "30m", item.security_id, item.security_revision_id,
        )
        raw_reads.extend((hourly, half_hour))
        adjusted_reads.append(adjusted_read(
            ticker, item.security_id, item.security_revision_id,
        ))
        coverage.append(coverage_for(hourly))
    return tuple(securities), raw_reads, adjusted_reads, coverage


def test_producer_persists_dedicated_envelopes_and_retries_exactly():
    securities, raw_reads, adjusted_reads, coverage = inputs()
    recorded_at = NOW + timedelta(seconds=2)
    evidence_repository = EvidenceRepository(raw_reads, recorded_at)
    kwargs = {
        "securities": securities,
        "watermark": DecisionWatermark(NOW, NOW),
        "evidence_repository": evidence_repository,
        "bar_repository": BarRepository(adjusted_reads),
        "corporate_action_repository": ActionRepository(coverage),
        "clock": lambda: recorded_at,
    }

    first = produce_stock_behavior_snapshots(
        **kwargs, received_at=NOW + timedelta(seconds=1),
    )
    repeated = produce_stock_behavior_snapshots(
        **kwargs, received_at=NOW + timedelta(seconds=10),
    )

    assert (first.inserted, first.existing, first.skipped, first.failed) == (2, 0, (), ())
    assert (repeated.inserted, repeated.existing, repeated.skipped, repeated.failed) == (0, 2, (), ())
    assert len(evidence_repository.snapshots) == 2
    assert {row.source_name for row in evidence_repository.persisted_evidence.values()} == {
        "STOCK_BEHAVIOR_ADJUSTED_DAILY", "STOCK_BEHAVIOR_RAW_SOURCE",
    }
    aapl = next(row for row in evidence_repository.snapshots.values() if row.ticker == "AAPL")
    assert all(snapshot_uses_current_source_policies(row) for row in evidence_repository.snapshots.values())
    relative_strength = next(component for component in aapl.components if component.factor == "RELATIVE_STRENGTH")
    assert relative_strength.status == "READY" and len(relative_strength.sources) == 2


def test_producer_skips_stale_raw_sources_without_writes():
    securities, raw_reads, adjusted_reads, coverage = inputs()
    stale_reads = [replace(
        read, evidence=replace(read.evidence, valid_until=NOW),
    ) for read in raw_reads]
    repository = EvidenceRepository(stale_reads, NOW + timedelta(seconds=2))

    result = produce_stock_behavior_snapshots(
        securities,
        watermark=DecisionWatermark(NOW, NOW),
        evidence_repository=repository,
        bar_repository=BarRepository(adjusted_reads),
        corporate_action_repository=ActionRepository(coverage),
        received_at=NOW + timedelta(seconds=1),
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert result.inserted == result.existing == 0
    assert len(result.skipped) == 2 and result.failed == ()
    assert repository.calls == 0


def test_producer_check_mode_assembles_without_persistence():
    securities, raw_reads, adjusted_reads, coverage = inputs()
    repository = EvidenceRepository(raw_reads, NOW + timedelta(seconds=2))

    result = produce_stock_behavior_snapshots(
        securities,
        watermark=DecisionWatermark(NOW, NOW),
        evidence_repository=repository,
        bar_repository=BarRepository(adjusted_reads),
        corporate_action_repository=ActionRepository(coverage),
        persist=False, received_at=NOW + timedelta(seconds=1),
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert result.planned == 2 and result.inserted == result.existing == 0
    assert result.skipped == result.failed == () and repository.calls == 0