from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from equity.behavior_sources import (
    ADJUSTED_DAILY_HISTORY_POLICY,
    BEHAVIOR_SOURCE_SELECTION_POLICY,
    OPTIONS_SWING_HYBRID_SOURCE_POLICY,
    PROVIDER_ADJUSTMENT_CONTINUITY_POLICY,
    bind_adjusted_daily_evidence,
    bind_adjusted_daily_history,
    bind_feature_source,
    bind_provider_adjustment_continuity,
    bind_raw_source_evidence,
    build_adjusted_daily_evidence_draft,
    build_raw_source_evidence_draft,
    source_tail_bars_by_interval,
    split_coverage_gap_sessions,
)
from equity.domain import BarAvailabilityMode
from scripts.run_corporate_action_worker import build_coverage
from test_equity_behavior_coverage import NOW, feature_read


def empty_split_coverage(read, *, mode=BarAvailabilityMode.LIVE_OBSERVED):
    row = build_coverage(
        {read.evidence.ticker: read.evidence.security_id},
        start=read.bars[0].session_date, end=read.bars[-1].session_date,
        observed_at=NOW - timedelta(seconds=1), availability_mode=mode,
        responses={"SPLIT": [], "DIVIDEND": []},
    )[0]
    return {**asdict(row), "availability_mode": row.availability_mode.value,
            "created_at": NOW - timedelta(seconds=1)}


def test_response_complete_empty_split_coverage_binds_exact_raw_source():
    read = feature_read()
    coverage = empty_split_coverage(read)
    binding = bind_feature_source(
        read, (coverage,), {UUID(str(coverage["coverage_id"])): ()},
        received_at=NOW + timedelta(seconds=1),
    )

    assert binding.status == "READY" and binding.series is not None
    assert binding.policy_sha256 == BEHAVIOR_SOURCE_SELECTION_POLICY.sha256
    assert binding.series.source.policy_sha256 == BEHAVIOR_SOURCE_SELECTION_POLICY.sha256
    assert binding.series.source.action_review_ids == (str(coverage["coverage_id"]),)
    assert binding.coverage_ids == (UUID(str(coverage["coverage_id"])),)
    assert binding.series.source.availability_mode == "PROSPECTIVE_RECEIPT"
    assert source_tail_bars_by_interval() == {"1d": 273, "1h": 200, "30m": 200}


def test_scope_only_or_wrong_identity_coverage_never_means_no_splits():
    read = feature_read()
    coverage = empty_split_coverage(read)
    for mutation in ("legacy", "identity", "window", "late"):
        candidate = dict(coverage)
        if mutation == "legacy":
            candidate.update(response_action_count=None, response_sha256=None)
        if mutation == "identity":
            candidate["security_id"] = uuid4()
        if mutation == "window":
            candidate["window_start"] = read.bars[0].session_date + timedelta(days=1)
        if mutation == "late":
            candidate["created_at"] = NOW + timedelta(seconds=2)
        binding = bind_feature_source(
            read, (candidate,), {UUID(str(candidate["coverage_id"])): ()},
            received_at=NOW + timedelta(seconds=1),
        )
        assert binding.status == "UNAVAILABLE" and binding.series is None
        assert binding.blocker_codes


def test_expired_feature_binds_only_as_stale_source():
    read = feature_read()
    read = replace(read, evidence=replace(read.evidence, valid_until=NOW))
    coverage = empty_split_coverage(read)
    binding = bind_feature_source(
        read, (coverage,), {UUID(str(coverage["coverage_id"])): ()},
        received_at=NOW + timedelta(seconds=1),
    )
    assert binding.status == "STALE" and binding.series is not None
    assert binding.blocker_codes == ("FEATURE_EVIDENCE_EXPIRED_AT_RECEIPT",)


def test_source_policy_version_or_lineage_mismatch_fails_closed():
    read = feature_read()
    coverage = empty_split_coverage(read)
    wrong_version = replace(read, evidence=replace(read.evidence, source_version="other"))
    missing = replace(read, missing_revision_ids=(read.selected_revision_ids[-1],))
    for candidate in (wrong_version, missing):
        binding = bind_feature_source(
            candidate, (coverage,), {UUID(str(coverage["coverage_id"])): ()},
            received_at=NOW + timedelta(seconds=1),
        )
        assert binding.status == "UNAVAILABLE" and binding.series is None


def test_reconstructed_bars_require_reconstructed_coverage():
    read = feature_read()
    bars = tuple(replace(
        bar, availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        replay_available_at=bar.system_observed_at,
    ) for bar in read.bars)
    read = replace(read, bars=bars)
    live = empty_split_coverage(read)
    binding = bind_feature_source(
        read, (live,), {UUID(str(live["coverage_id"])): ()},
        received_at=NOW + timedelta(seconds=1),
    )
    assert binding.status == "UNAVAILABLE"

    reconstructed = empty_split_coverage(read, mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED)
    binding = bind_feature_source(
        read, (reconstructed,), {UUID(str(reconstructed["coverage_id"])): ()},
        received_at=NOW + timedelta(seconds=1),
    )
    assert binding.status == "READY"
    assert binding.series.source.availability_mode == "RECONSTRUCTED"


def test_wider_response_blocks_only_splits_inside_selected_bar_window(monkeypatch):
    read = feature_read()
    coverage = empty_split_coverage(read)
    coverage["window_start"] -= timedelta(days=1)
    coverage["window_end"] += timedelta(days=1)
    monkeypatch.setattr("equity.behavior_sources.validate_action_coverage", lambda *args: None)
    action = {
        "effective_date": coverage["window_start"], "first_observed_at": NOW - timedelta(seconds=2),
        "created_at": NOW - timedelta(seconds=2), "revised_observed_at": None,
        "source": coverage["source"], "availability_mode": "LIVE_OBSERVED",
    }
    coverage_id = UUID(str(coverage["coverage_id"]))
    binding = bind_feature_source(read, (coverage,), {coverage_id: (action,)}, received_at=NOW + timedelta(seconds=1))
    assert binding.status == "READY"

    action["effective_date"] = read.bars[-1].session_date
    binding = bind_feature_source(read, (coverage,), {coverage_id: (action,)}, received_at=NOW + timedelta(seconds=1))
    assert binding.status == "UNAVAILABLE" and binding.blocker_codes == ("RAW_PRICE_SPLIT_PRESENT",)


def test_malformed_coverage_or_action_contract_fails_closed(monkeypatch):
    read = feature_read()
    coverage = empty_split_coverage(read)
    malformed = dict(coverage, security_id=None)
    binding = bind_feature_source(read, (malformed,), {}, received_at=NOW + timedelta(seconds=1))
    assert binding.blocker_codes == ("SPLIT_RESPONSE_COVERAGE_IDENTITY_MISMATCH",)

    monkeypatch.setattr("equity.behavior_sources.validate_action_coverage", lambda *args: None)
    coverage_id = UUID(str(coverage["coverage_id"]))
    action = {
        "effective_date": read.bars[-1].session_date,
        "first_observed_at": NOW - timedelta(seconds=2), "created_at": NOW - timedelta(seconds=2),
        "revised_observed_at": None, "source": "OTHER", "availability_mode": "LIVE_OBSERVED",
    }
    binding = bind_feature_source(read, (coverage,), {coverage_id: (action,)}, received_at=NOW + timedelta(seconds=1))
    assert binding.blocker_codes == ("SPLIT_ACTION_SOURCE_CONTRACT_MISMATCH",)


def test_coverage_near_misses_report_window_mode_and_receipt_separately():
    read = feature_read()
    coverage = empty_split_coverage(read)
    coverage_id = UUID(str(coverage["coverage_id"]))
    cases = []
    narrow = dict(coverage, window_start=read.bars[0].session_date + timedelta(days=1))
    cases.append((narrow, "SPLIT_RESPONSE_COVERAGE_WINDOW_MISSING"))
    wrong_mode = dict(coverage, availability_mode="HISTORICAL_RECONSTRUCTED",
                      replay_available_at=coverage["first_observed_at"])
    cases.append((wrong_mode, "SPLIT_RESPONSE_COVERAGE_MODE_MISMATCH"))
    late = dict(coverage, created_at=NOW + timedelta(seconds=2))
    cases.append((late, "SPLIT_RESPONSE_COVERAGE_AFTER_RECEIPT"))
    for candidate, reason in cases:
        binding = bind_feature_source(
            read, (candidate,), {coverage_id: ()}, received_at=NOW + timedelta(seconds=1),
        )
        assert binding.status == "UNAVAILABLE" and binding.blocker_codes == (reason,)


def test_adjacent_response_complete_windows_compose_without_gaps():
    read = feature_read()
    first = empty_split_coverage(read)
    split_day = read.bars[len(read.bars) // 2].session_date
    second = dict(first, coverage_id=uuid4(), source_key=first["source_key"] + ":second")
    first["window_end"] = split_day
    second["window_start"] = split_day + timedelta(days=1)
    first["payload_sha256"] = __import__("equity.polygon", fromlist=["sha256_json"]).sha256_json({
        "ticker": first["ticker"], "action_type": "SPLIT",
        "window_start": first["window_start"].isoformat(), "window_end": first["window_end"].isoformat(),
    })
    second["payload_sha256"] = __import__("equity.polygon", fromlist=["sha256_json"]).sha256_json({
        "ticker": second["ticker"], "action_type": "SPLIT",
        "window_start": second["window_start"].isoformat(), "window_end": second["window_end"].isoformat(),
    })
    actions = {UUID(str(first["coverage_id"])): (), UUID(str(second["coverage_id"])): ()}
    binding = bind_feature_source(read, (first, second), actions, received_at=NOW + timedelta(seconds=1))
    assert binding.status == "READY" and set(binding.coverage_ids) == set(actions)
    assert set(binding.series.source.action_review_ids) == {str(value) for value in actions}

    second["window_start"] += timedelta(days=1)
    binding = bind_feature_source(read, (first, second), actions, received_at=NOW + timedelta(seconds=1))
    assert binding.status == "UNAVAILABLE"
    assert binding.blocker_codes == ("SPLIT_RESPONSE_COVERAGE_WINDOW_MISSING",)


def adjusted_history():
    read = feature_read()
    bars = tuple(replace(
        bar, adjusted=True,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        replay_available_at=bar.system_observed_at,
        quality_codes=("GROUPED_DAILY_EXACT_TICKER_V2",),
    ) for bar in read.bars)
    return read, bars


def test_adjusted_history_requires_real_derived_evidence_before_snapshot_source():
    read, bars = adjusted_history()
    history = bind_adjusted_daily_history(
        bars, read.bar_created_ats, security_id=read.evidence.security_id,
        ticker=read.evidence.ticker, received_at=NOW,
    )
    assert history.status == "READY_FOR_DERIVED_EVIDENCE"
    assert history.policy_sha256 == ADJUSTED_DAILY_HISTORY_POLICY.sha256
    assert history.source_manifest_sha256 and len(history.bars) == 273
    assert OPTIONS_SWING_HYBRID_SOURCE_POLICY.interval_sources == (
        ("1d", "PROVIDER_SPLIT_ADJUSTED"),
        ("1h", "RAW_ACTION_GATED"),
        ("30m", "RAW_ACTION_GATED"),
    )
    assert OPTIONS_SWING_HYBRID_SOURCE_POLICY.daily_source_policy_sha256 == history.policy_sha256

    evidence = build_adjusted_daily_evidence_draft(
        history, security_revision_id=read.evidence.security_revision_id,
    )
    repeated = build_adjusted_daily_evidence_draft(
        history, security_revision_id=read.evidence.security_revision_id,
    )
    assert evidence == repeated
    assert evidence.observed_at == history.history_available_at
    assert evidence.valid_until > history.history_available_at
    assert evidence.source_revision_ids == tuple(bar.bar_revision_id for bar in history.bars)
    series = bind_adjusted_daily_evidence(
        history, evidence, evidence_recorded_at=NOW, received_at=NOW,
    )
    assert series.adjusted is True
    assert series.source.availability_mode == "PROSPECTIVE_RECEIPT"
    assert series.source.history_mode == "RECONSTRUCTED_HISTORY"
    assert series.source.price_basis == "PROVIDER_SPLIT_ADJUSTED"
    assert series.source.history_available_at == history.history_available_at

    mismatched = replace(evidence, source_revision_ids=evidence.source_revision_ids[:-1])
    with pytest.raises(ValueError, match="exact history"):
        bind_adjusted_daily_evidence(
            history, mismatched, evidence_recorded_at=NOW, received_at=NOW,
        )


def test_adjusted_history_fails_closed_for_short_identity_or_future_inputs():
    read, bars = adjusted_history()
    cases = (
        (bars[:-1], read.bar_created_ats[:-1], read.evidence.security_id, NOW),
        (bars, read.bar_created_ats, uuid4(), NOW),
        (bars, (*read.bar_created_ats[:-1], NOW + timedelta(seconds=1)), read.evidence.security_id, NOW),
    )
    for candidate_bars, clocks, identity, receipt in cases:
        binding = bind_adjusted_daily_history(
            candidate_bars, clocks, security_id=identity,
            ticker=read.evidence.ticker, received_at=receipt,
        )
        assert binding.status != "READY_FOR_DERIVED_EVIDENCE"
        assert binding.bars == () and binding.blocker_codes

    gapped_bars = (*bars[:100], *bars[101:])
    gapped_clocks = (*read.bar_created_ats[:100], *read.bar_created_ats[101:])
    binding = bind_adjusted_daily_history(
        gapped_bars, gapped_clocks, security_id=read.evidence.security_id,
        ticker=read.evidence.ticker, received_at=NOW,
    )
    assert "ADJUSTED_DAILY_SESSION_GAPS" in binding.blocker_codes


def test_adjusted_evidence_stays_valid_through_next_session_after_friday():
    read, bars = adjusted_history()
    friday_close = datetime(2026, 9, 18, 20, tzinfo=timezone.utc)
    friday_bar = replace(
        bars[-1], session_date=date(2026, 9, 18),
        bar_start=friday_close - timedelta(hours=6, minutes=30),
        bar_end=friday_close, system_observed_at=friday_close + timedelta(minutes=1),
        replay_available_at=friday_close + timedelta(minutes=1),
    )
    history = bind_adjusted_daily_history(
        (*bars[1:], friday_bar),
        (*read.bar_created_ats[1:], friday_close + timedelta(minutes=1)),
        security_id=read.evidence.security_id, ticker=read.evidence.ticker,
        received_at=friday_close + timedelta(hours=1),
    )
    assert history.status == "READY_FOR_DERIVED_EVIDENCE", history.blocker_codes
    evidence = build_adjusted_daily_evidence_draft(
        history, security_revision_id=read.evidence.security_revision_id,
    )

    assert evidence.valid_until == datetime(2026, 9, 22, 0, tzinfo=timezone.utc)


def test_provider_adjusted_pairs_can_prove_stable_raw_hourly_split_basis():
    read, adjusted = adjusted_history()
    sessions = tuple(bar.session_date for bar in read.bars[-30:])
    review = bind_provider_adjustment_continuity(
        read.bars, read.bar_created_ats, adjusted, read.bar_created_ats,
        required_sessions=sessions, security_id=read.evidence.security_id,
        ticker=read.evidence.ticker, received_at=NOW,
    )
    assert review.status == "READY_FOR_DERIVED_EVIDENCE"
    assert review.policy_sha256 == PROVIDER_ADJUSTMENT_CONTINUITY_POLICY.sha256
    assert review.review_manifest_sha256 and review.median_close_factor == 1
    assert review.required_sessions == sessions


def test_provider_adjusted_continuity_blocks_factor_transition_and_missing_pair():
    read, adjusted = adjusted_history()
    sessions = tuple(bar.session_date for bar in read.bars[-30:])
    transition = list(adjusted)
    for index in range(len(transition) - 15, len(transition)):
        bar = transition[index]
        transition[index] = replace(
            bar, open_price=bar.open_price * 2, high_price=bar.high_price * 2,
            low_price=bar.low_price * 2, close_price=bar.close_price * 2,
        )
    review = bind_provider_adjustment_continuity(
        read.bars, read.bar_created_ats, transition, read.bar_created_ats,
        required_sessions=sessions, security_id=read.evidence.security_id,
        ticker=read.evidence.ticker, received_at=NOW,
    )
    assert review.status == "UNAVAILABLE"
    assert review.blocker_codes == ("ADJUSTMENT_FACTOR_TRANSITION_OR_DRIFT",)
    assert review.minimum_close_factor == 1
    assert review.maximum_close_factor == 2
    assert review.largest_deviation_factor == 1

    review = bind_provider_adjustment_continuity(
        read.bars, read.bar_created_ats, adjusted[:-1], read.bar_created_ats[:-1],
        required_sessions=sessions, security_id=read.evidence.security_id,
        ticker=read.evidence.ticker, received_at=NOW,
    )
    assert review.blocker_codes == ("PAIRED_DAILY_SESSION_MISSING",)


def test_split_coverage_gap_selector_returns_only_uncovered_sessions():
    read = feature_read()
    coverage = empty_split_coverage(read)
    midpoint = read.bars[-10].session_date
    coverage["window_start"] = midpoint
    coverage["payload_sha256"] = __import__("equity.polygon", fromlist=["sha256_json"]).sha256_json({
        "ticker": coverage["ticker"], "action_type": "SPLIT",
        "window_start": coverage["window_start"].isoformat(),
        "window_end": coverage["window_end"].isoformat(),
    })
    coverage_id = UUID(str(coverage["coverage_id"]))
    sessions = tuple(bar.session_date for bar in read.bars[-20:])
    gaps = split_coverage_gap_sessions(
        (coverage,), {coverage_id: ()}, ticker=read.evidence.ticker,
        security_id=read.evidence.security_id, sessions=sessions,
        received_at=NOW + timedelta(seconds=1),
    )
    assert gaps == tuple(session for session in sessions if session < midpoint)


def test_raw_source_uses_dedicated_deterministic_evidence_envelope():
    read = feature_read()
    coverage = empty_split_coverage(read)
    coverage_id = UUID(str(coverage["coverage_id"]))
    binding = bind_feature_source(
        read, (coverage,), {coverage_id: ()}, received_at=NOW + timedelta(seconds=1),
    )
    evidence = build_raw_source_evidence_draft(
        binding, security_revision_id=read.evidence.security_revision_id,
    )
    repeated = build_raw_source_evidence_draft(
        binding, security_revision_id=read.evidence.security_revision_id,
    )
    later_binding = bind_feature_source(
        read, (coverage,), {coverage_id: ()}, received_at=NOW + timedelta(seconds=10),
    )
    later = build_raw_source_evidence_draft(
        later_binding, security_revision_id=read.evidence.security_revision_id,
    )
    assert evidence == repeated == later
    assert evidence.observed_at == binding.available_at == later_binding.available_at
    assert evidence.evidence_id != read.evidence.evidence_id
    assert evidence.source_revision_ids == binding.series.bar_revision_ids
    assert str(read.evidence.evidence_id) in evidence.payload_json

    recorded_at = NOW + timedelta(seconds=2)
    series = bind_raw_source_evidence(
        binding, evidence, evidence_recorded_at=recorded_at, received_at=recorded_at,
    )
    assert series.source.evidence_id == evidence.evidence_id
    assert series.source.recorded_at == recorded_at
    assert series.source.action_review_ids == (str(coverage_id),)
    assert series.source.payload_sha256 == evidence.payload_sha256

    mismatched = replace(evidence, source_revision_ids=evidence.source_revision_ids[:-1])
    with pytest.raises(ValueError, match="exact origin"):
        bind_raw_source_evidence(
            binding, mismatched, evidence_recorded_at=recorded_at,
            received_at=recorded_at,
        )