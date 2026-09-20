from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import exchange_calendars
import pandas as pd
import pytest

from equity.behavior_coverage import (
    assess_feature_source_coverage, summarize_adjusted_bar_inventory, summarize_coverage,
    summarize_hourly_split_continuity,
)
from equity.domain import (
    BarAvailabilityMode, BarSessionScope, BarSourceKind, DecisionWatermark, EquityBarRevision,
    EquityEvidence, EvidenceRole, EvidenceType, LifecycleStatus, QualityState,
)
from equity.repositories import BehaviorFeatureSourceRead, BehaviorGroupedDailyRead
from scripts.run_corporate_action_worker import build_coverage


NOW = datetime(2026, 9, 17, 21, tzinfo=timezone.utc)


def feature_read(ticker="SPY", *, count=273):
    security_id = uuid4()
    bars = []
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = calendar.sessions_in_range(
        pd.Timestamp(NOW.date() - timedelta(days=count * 2)),
        pd.Timestamp(NOW.date()),
    )[-count:]
    for index, session in enumerate(sessions):
        bar_end = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        close = Decimal(100 + index)
        bars.append(EquityBarRevision(
            bar_revision_id=uuid4(), security_id=security_id, ticker=ticker, interval="1d",
            session_date=bar_end.date(), bar_start=bar_end - timedelta(hours=6, minutes=30), bar_end=bar_end,
            open_price=close, high_price=close + 1, low_price=close - 1, close_price=close,
            volume=Decimal(1000 + index), vwap=None, transaction_count=None,
            source_kind=BarSourceKind.DERIVED, availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
            is_final=True, system_observed_at=bar_end + timedelta(seconds=1), replay_available_at=None,
            adjusted=False, payload_sha256=f"{index + 1:064x}", session_scope=BarSessionScope.RTH,
        ))
    evidence = EquityEvidence(
        evidence_id=uuid4(), evidence_key=f"feature:{ticker}", lifecycle_key=f"feature:{ticker}:1d",
        evidence_type=EvidenceType.FEATURE_SNAPSHOT, evidence_role=EvidenceRole.REGIME,
        security_id=security_id, ticker=ticker, interval="1d", direction=1,
        lifecycle_status=LifecycleStatus.SNAPSHOT, strength=None, market_time=bars[-1].bar_end,
        observed_at=NOW - timedelta(seconds=2), valid_until=NOW + timedelta(hours=1),
        source_name="EQUITY_FEATURES", source_version="equity_features_v1", payload_schema_version="1.0",
        analysis_run_id=uuid4(), latest_bar_revision_id=bars[-1].bar_revision_id,
        security_revision_id=uuid4(), fundamental_report_ids=(),
        source_revision_ids=tuple(bar.bar_revision_id for bar in bars),
        quality_state=QualityState.COMPLETE, quality_codes=(), qualification_revision_id=None,
        payload_json="{}", payload_sha256="a" * 64,
    )
    return BehaviorFeatureSourceRead(
        evidence=evidence, evidence_created_at=NOW - timedelta(seconds=1),
        selected_revision_ids=evidence.source_revision_ids, bars=tuple(bars),
        bar_created_ats=tuple(bar.system_observed_at for bar in bars), missing_revision_ids=(),
    )


def empty_split_coverage(read):
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


def test_legacy_feature_math_can_be_ready_without_contract_readiness():
    received_at = NOW + timedelta(seconds=1)
    row = assess_feature_source_coverage(feature_read(), DecisionWatermark(NOW, NOW), received_at)

    assert row.mathematical_status == "READY"
    assert row.contract_status == "UNAVAILABLE"
    assert {metric.metric_id for metric in row.metrics} == {
        "ema50_slope10_atr", "adx14", "return5", "momentum_change5", "atr14_fraction",
        "rv20_cc_annual", "rv20_percentile252", "compression_tr5_20", "daily_rvol20",
        "median_dollar_volume20", "extension_ema21_atr", "prior_range20_position",
    }
    assert "SPLIT_RESPONSE_COVERAGE_MISSING" in row.blocker_codes
    assert row.source_policy_sha256 is not None
    assert row.earliest_legal_decision_at == received_at
    assert row.recording_to_receipt_seconds == 2


def test_incomplete_or_late_lineage_never_exposes_metric_values():
    read = feature_read()
    missing = read.selected_revision_ids[-1]
    incomplete = replace(read, bars=read.bars[:-1], bar_created_ats=read.bar_created_ats[:-1], missing_revision_ids=(missing,))
    row = assess_feature_source_coverage(incomplete, DecisionWatermark(NOW, NOW), NOW + timedelta(seconds=1))
    assert row.mathematical_status == "UNAVAILABLE" and row.metrics == ()
    assert "SOURCE_REVISION_LINEAGE_INCOMPLETE" in row.blocker_codes

    late = replace(read, evidence_created_at=NOW + timedelta(seconds=1))
    row = assess_feature_source_coverage(late, DecisionWatermark(NOW, NOW), NOW + timedelta(seconds=1))
    assert row.mathematical_status == "UNAVAILABLE" and "EVIDENCE_AFTER_REPORT_CUTOFF" in row.blocker_codes


def test_summary_preserves_expected_missing_rows_and_paired_spy_policy():
    spy = feature_read("SPY")
    aapl = feature_read("AAPL")
    report = summarize_coverage(
        (aapl, spy), ("AAPL", "SPY"), ("1d", "1h", "30m"),
        DecisionWatermark(NOW, NOW), NOW + timedelta(seconds=1),
    )

    assert report["expected_rows"] == 6 and report["observed_rows"] == 2
    assert report["contract_status_counts"] == {"UNAVAILABLE": 6}
    assert report["blocker_counts"]["FEATURE_EVIDENCE_MISSING"] == 4
    aapl_daily = next(row for row in report["rows"] if row["ticker"] == "AAPL" and row["interval"] == "1d")
    assert "SPY_BENCHMARK_WINDOW_MISMATCH" not in aapl_daily["blocker_codes"]
    assert any(metric["metric_id"] == "excess_return20" for metric in aapl_daily["metrics"])
    assert report["option_timing_status"] == "NOT_ASSESSED_STOCK_SOURCE_CONTRACT_UNAVAILABLE"
    assert report["split_coverage_rows"] == 0
    assert report["contract_status_counts_by_interval"] == {
        "1d": {"UNAVAILABLE": 2}, "1h": {"UNAVAILABLE": 2}, "30m": {"UNAVAILABLE": 2},
    }


def test_coverage_rejects_receipt_before_read_cutoff():
    with pytest.raises(ValueError, match="follow its read cutoff"):
        assess_feature_source_coverage(
            feature_read(), DecisionWatermark(NOW, NOW), NOW - timedelta(microseconds=1),
        )


def test_response_bound_split_coverage_makes_fresh_rows_contract_ready():
    spy = feature_read("SPY")
    aapl = feature_read("AAPL")
    coverage = tuple(empty_split_coverage(read) for read in (spy, aapl))
    actions = {UUID(str(row["coverage_id"])): () for row in coverage}
    report = summarize_coverage(
        (aapl, spy), ("AAPL", "SPY"), ("1d",), DecisionWatermark(NOW, NOW),
        NOW + timedelta(seconds=1), coverage, actions,
    )
    assert report["contract_status_counts"] == {"READY": 2}
    assert report["response_bound_split_coverage_rows"] == 2
    assert report["response_bound_split_coverage_tickers"] == ["AAPL", "SPY"]
    assert report["feature_bar_mode_counts"] == {"LIVE_OBSERVED": 2}
    assert report["response_bound_split_coverage_mode_counts"] == {"LIVE_OBSERVED": 2}
    assert report["source_windows_by_interval"]["1d"] == {
        "start": aapl.bars[0].session_date.isoformat(),
        "end": aapl.bars[-1].session_date.isoformat(),
    }
    assert report["option_timing_status"] == "READY_FOR_SEPARATE_OPTION_TIMING_ASSESSMENT"
    assert all(row["source_policy_sha256"] and row["action_coverage_ids"] for row in report["rows"])


def test_adjusted_inventory_is_mathematical_history_not_prospective_source_proof():
    read = feature_read("SPY")
    adjusted = tuple(replace(
        bar, adjusted=True, availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        replay_available_at=bar.bar_end,
        quality_codes=("GROUPED_DAILY_EXACT_TICKER_V2",),
    ) for bar in read.bars)
    report = summarize_adjusted_bar_inventory(
        (read,), {"1d": {"SPY": adjusted}, "1h": {}, "30m": {}}, ("SPY",),
        {"SPY": read.bar_created_ats}, NOW,
    )

    assert report["mathematical_history_ready_counts_by_interval"] == {"1d": 1, "1h": 0, "30m": 0}
    assert report["prospective_behavior_contract_ready"] == 0
    assert report["derived_evidence_candidate_counts_by_interval"] == {"1d": 1, "1h": 0, "30m": 0}
    assert report["mathematical_history_ready_tickers_by_interval"]["1d"] == ["SPY"]
    assert report["mathematical_history_unready_tickers_by_interval"]["1h"] == ["SPY"]
    daily = next(row for row in report["rows"] if row["interval"] == "1d")
    assert daily["security_identity_match"] is True
    assert daily["availability_modes"] == ["HISTORICAL_RECONSTRUCTED"]
    assert daily["derived_evidence_candidate_ready"] is True
    assert daily["source_manifest_sha256"]
    assert daily["blocker_codes"] == ["DERIVED_EVIDENCE_NOT_MATERIALIZED"]


def test_hourly_gap_can_use_stable_adjusted_daily_continuity_candidate():
    daily = feature_read("SPY")
    adjusted_bars = tuple(replace(
        bar, adjusted=True, availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        replay_available_at=bar.bar_end,
        quality_codes=("GROUPED_DAILY_EXACT_TICKER_V2",),
    ) for bar in daily.bars)
    raw_grouped = BehaviorGroupedDailyRead(
        ticker="SPY", bars=daily.bars, bar_created_ats=daily.bar_created_ats,
    )
    adjusted = BehaviorGroupedDailyRead(
        ticker="SPY", bars=adjusted_bars, bar_created_ats=daily.bar_created_ats,
    )
    hourly = replace(
        daily,
        evidence=replace(daily.evidence, interval="1h", evidence_key="feature:SPY:1h"),
        selected_revision_ids=daily.selected_revision_ids[-30:], bars=daily.bars[-30:],
        bar_created_ats=daily.bar_created_ats[-30:],
    )
    coverage = empty_split_coverage(daily)
    coverage["window_start"] = daily.bars[-10].session_date
    coverage["payload_sha256"] = __import__("equity.polygon", fromlist=["sha256_json"]).sha256_json({
        "ticker": coverage["ticker"], "action_type": "SPLIT",
        "window_start": coverage["window_start"].isoformat(),
        "window_end": coverage["window_end"].isoformat(),
    })
    coverage_id = UUID(str(coverage["coverage_id"]))
    report = summarize_hourly_split_continuity(
        (daily, hourly), (raw_grouped,), (adjusted,), (coverage,), {coverage_id: ()},
        ("SPY",), NOW + timedelta(seconds=1),
    )
    assert report["status_counts"] == {"DIAGNOSTIC_STABLE_FACTOR": 1}
    assert report["candidate_tickers"] == ["SPY"]
    assert report["rows"][0]["gap_session_count"] == 20
    assert report["rows"][0]["median_close_factor"] == 1
    assert report["blocker_counts"] == {"RESPONSE_COVERAGE_STILL_REQUIRED": 1}