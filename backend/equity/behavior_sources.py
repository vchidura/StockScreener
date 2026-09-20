"""Versioned, read-only source binding for stock behavior assembly."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
from statistics import median
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd
from pydantic import AwareDatetime

from .behavior import (
    ADJUSTED_DAILY_EVIDENCE_SCHEMA, ADJUSTED_DAILY_EVIDENCE_SOURCE,
    RAW_FEATURE_EVIDENCE_SCHEMA, RAW_FEATURE_EVIDENCE_SOURCE,
    BehaviorSource, Contract, Sha256,
    StockBehaviorSnapshot,
)
from .behavior_assembly import FinalizedBarSeries, finalized_series_from_revisions
from .domain import (
    BarAvailabilityMode, BarSessionScope, EquityBarRevision, EquityEvidence,
    EvidenceRole, EvidenceType, LifecycleStatus, QualityState,
)
from .historical_actions import validate_action_coverage
from .polygon import canonical_json, sha256_json
from .repositories import BehaviorFeatureSourceRead


class BehaviorSourceSelectionPolicy(Contract):
    version: Literal["behavior_feature_source_v1"] = "behavior_feature_source_v1"
    evidence_type: Literal["FEATURE_SNAPSHOT"] = "FEATURE_SNAPSHOT"
    source_name: Literal["EQUITY_FEATURES"] = "EQUITY_FEATURES"
    source_version: Literal["equity_features_v1"] = "equity_features_v1"
    payload_schema_version: Literal["1.0"] = "1.0"
    intervals: tuple[Literal["1d", "1h", "30m"], ...] = ("1d", "1h", "30m")
    tail_bars_by_interval: tuple[
        tuple[Literal["1d", "1h", "30m"], int], ...
    ] = (("1d", 273), ("1h", 200), ("30m", 200))
    session_scope: Literal["RTH"] = "RTH"
    adjusted: Literal[False] = False
    require_final_bars: Literal[True] = True
    require_market_observed_recorded_replay_cutoffs: Literal[True] = True
    action_source: Literal["POLYGON_CORPORATE_ACTIONS_V1"] = "POLYGON_CORPORATE_ACTIONS_V1"
    action_type: Literal["SPLIT"] = "SPLIT"
    require_response_bound_action_coverage: Literal[True] = True


BEHAVIOR_SOURCE_SELECTION_POLICY = BehaviorSourceSelectionPolicy()


def snapshot_uses_current_source_policies(snapshot: StockBehaviorSnapshot) -> bool:
    for component in snapshot.components:
        for source in component.sources:
            if source.interval == "1d":
                if (
                    source.policy_sha256 != ADJUSTED_DAILY_HISTORY_POLICY.sha256
                    or source.price_basis != "PROVIDER_SPLIT_ADJUSTED"
                ):
                    return False
            elif source.interval in {"1h", "30m"}:
                if (
                    source.policy_sha256 != BEHAVIOR_SOURCE_SELECTION_POLICY.sha256
                    or source.price_basis != "RAW_ACTION_GATED"
                ):
                    return False
            else:
                return False
    return True


def source_tail_bars_by_interval() -> dict[str, int]:
    return dict(BEHAVIOR_SOURCE_SELECTION_POLICY.tail_bars_by_interval)


class AdjustedDailyHistoryPolicy(Contract):
    version: Literal["provider_adjusted_daily_history_v1"] = "provider_adjusted_daily_history_v1"
    interval: Literal["1d"] = "1d"
    minimum_tail_bars: Literal[273] = 273
    adjusted: Literal[True] = True
    session_scope: Literal["RTH"] = "RTH"
    bar_availability_mode: Literal["HISTORICAL_RECONSTRUCTED"] = "HISTORICAL_RECONSTRUCTED"
    quality_code: Literal["GROUPED_DAILY_EXACT_TICKER_V2"] = "GROUPED_DAILY_EXACT_TICKER_V2"
    price_basis: Literal["PROVIDER_SPLIT_ADJUSTED"] = "PROVIDER_SPLIT_ADJUSTED"
    require_derived_evidence_before_snapshot: Literal[True] = True
    require_consecutive_exchange_sessions: Literal[True] = True
    valid_through_next_session: Literal[True] = True
    post_close_grace_minutes: Literal[240] = 240


ADJUSTED_DAILY_HISTORY_POLICY = AdjustedDailyHistoryPolicy()


class OptionsSwingHybridSourcePolicy(Contract):
    version: Literal["options_swing_hybrid_sources_v1"] = "options_swing_hybrid_sources_v1"
    daily_source_policy_sha256: Sha256 = ADJUSTED_DAILY_HISTORY_POLICY.sha256
    intraday_source_policy_sha256: Sha256 = BEHAVIOR_SOURCE_SELECTION_POLICY.sha256
    interval_sources: tuple[tuple[str, str], ...] = (
        ("1d", "PROVIDER_SPLIT_ADJUSTED"),
        ("1h", "RAW_ACTION_GATED"),
        ("30m", "RAW_ACTION_GATED"),
    )
    allow_component_local_price_basis: Literal[True] = True
    require_matching_relative_strength_basis: Literal[True] = True
    require_actual_history_availability: Literal[True] = True
    allow_historical_availability_backdating: Literal[False] = False


OPTIONS_SWING_HYBRID_SOURCE_POLICY = OptionsSwingHybridSourcePolicy()


class ProviderAdjustmentContinuityPolicy(Contract):
    version: Literal["provider_adjustment_continuity_v1"] = "provider_adjustment_continuity_v1"
    raw_interval: Literal["1d"] = "1d"
    adjusted_interval: Literal["1d"] = "1d"
    adjusted_quality_code: Literal["GROUPED_DAILY_EXACT_TICKER_V2"] = "GROUPED_DAILY_EXACT_TICKER_V2"
    maximum_close_factor_log_deviation: float = 0.01
    require_every_intraday_session: Literal[True] = True
    require_persisted_review_before_source_use: Literal[True] = True


PROVIDER_ADJUSTMENT_CONTINUITY_POLICY = ProviderAdjustmentContinuityPolicy()


@dataclass(frozen=True, slots=True)
class ProviderAdjustmentContinuityBinding:
    status: str
    policy_sha256: str
    review_manifest_sha256: str | None
    ticker: str
    security_id: UUID
    required_sessions: tuple[date, ...]
    median_close_factor: float | None
    minimum_close_factor: float | None
    maximum_close_factor: float | None
    largest_deviation_session: date | None
    largest_deviation_factor: float | None
    largest_deviation_raw_close: float | None
    largest_deviation_adjusted_close: float | None
    available_at: datetime | None
    blocker_codes: tuple[str, ...]


def bind_provider_adjustment_continuity(
    raw_daily_bars: Sequence[EquityBarRevision],
    raw_recorded_ats: Sequence[datetime],
    adjusted_daily_bars: Sequence[EquityBarRevision],
    adjusted_recorded_ats: Sequence[datetime],
    *,
    required_sessions: Sequence[date],
    security_id: UUID,
    ticker: str,
    received_at: datetime,
) -> ProviderAdjustmentContinuityBinding:
    policy = PROVIDER_ADJUSTMENT_CONTINUITY_POLICY
    sessions = tuple(sorted(set(required_sessions)))
    blockers = []
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("continuity review receipt must be timezone-aware")
    if not sessions:
        blockers.append("INTRADAY_SESSION_SCOPE_EMPTY")
    raw = _bars_by_session(raw_daily_bars)
    adjusted = _bars_by_session(adjusted_daily_bars)
    raw_recorded = _recorded_by_session(raw_daily_bars, raw_recorded_ats)
    adjusted_recorded = _recorded_by_session(adjusted_daily_bars, adjusted_recorded_ats)
    if any(session not in raw or session not in adjusted for session in sessions):
        blockers.append("PAIRED_DAILY_SESSION_MISSING")
    factors = []
    factor_sessions = []
    available_ats = []
    if not blockers:
        for session in sessions:
            raw_bar, adjusted_bar = raw[session], adjusted[session]
            if (
                raw_bar.security_id != security_id or adjusted_bar.security_id != security_id
                or raw_bar.ticker != ticker.upper() or adjusted_bar.ticker != ticker.upper()
                or raw_bar.interval != "1d" or adjusted_bar.interval != "1d"
                or raw_bar.adjusted or not adjusted_bar.adjusted
                or not raw_bar.is_final or not adjusted_bar.is_final
                or raw_bar.session_scope is not BarSessionScope.RTH
                or adjusted_bar.session_scope is not BarSessionScope.RTH
                or adjusted_bar.availability_mode is not BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
                or policy.adjusted_quality_code not in adjusted_bar.quality_codes
            ):
                blockers.append("PAIRED_DAILY_IDENTITY_OR_SOURCE_MISMATCH")
                break
            factor = float(adjusted_bar.close_price / raw_bar.close_price)
            if not math.isfinite(factor) or factor <= 0:
                blockers.append("ADJUSTMENT_FACTOR_INVALID")
                break
            factors.append(factor)
            factor_sessions.append(session)
            for bar, recorded_at in (
                (raw_bar, raw_recorded.get(session)),
                (adjusted_bar, adjusted_recorded.get(session)),
            ):
                clocks = [bar.bar_end, bar.system_observed_at, recorded_at]
                if bar.provider_published_at is not None:
                    clocks.append(bar.provider_published_at)
                if bar.availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED:
                    clocks.append(bar.replay_available_at)
                if any(clock is None or clock.tzinfo is None or clock.utcoffset() is None for clock in clocks):
                    blockers.append("PAIRED_DAILY_AVAILABILITY_CLOCK_MISSING")
                    break
                available_ats.append(max(clocks))
            if blockers:
                break
    if factors and not blockers:
        center = median(factors)
        if max(abs(math.log(factor / center)) for factor in factors) > policy.maximum_close_factor_log_deviation:
            blockers.append("ADJUSTMENT_FACTOR_TRANSITION_OR_DRIFT")
    if available_ats and max(available_ats) > received_at:
        blockers.append("PAIRED_DAILY_AFTER_RECEIPT")
    factor_statistics = (
        (median(factors), min(factors), max(factors)) if factors else (None, None, None)
    )
    largest_deviation_session = None
    largest_deviation_factor = None
    if factors:
        center = factor_statistics[0]
        largest_index = max(
            range(len(factors)), key=lambda index: abs(math.log(factors[index] / center))
        )
        largest_deviation_session = factor_sessions[largest_index]
        largest_deviation_factor = factors[largest_index]
    if blockers:
        return ProviderAdjustmentContinuityBinding(
            status="UNAVAILABLE", policy_sha256=policy.sha256,
            review_manifest_sha256=None, ticker=ticker.upper(), security_id=security_id,
            required_sessions=sessions, median_close_factor=factor_statistics[0],
            minimum_close_factor=factor_statistics[1], maximum_close_factor=factor_statistics[2],
            largest_deviation_session=largest_deviation_session,
            largest_deviation_factor=largest_deviation_factor,
            largest_deviation_raw_close=(
                float(raw[largest_deviation_session].close_price) if largest_deviation_session else None
            ),
            largest_deviation_adjusted_close=(
                float(adjusted[largest_deviation_session].close_price) if largest_deviation_session else None
            ),
            available_at=max(available_ats) if available_ats else None,
            blocker_codes=tuple(dict.fromkeys(blockers)),
        )
    manifest = {
        "policy_sha256": policy.sha256, "ticker": ticker.upper(),
        "security_id": str(security_id), "required_sessions": [value.isoformat() for value in sessions],
        "pairs": [{
            "session": session.isoformat(),
            "raw_revision_id": str(raw[session].bar_revision_id),
            "raw_payload_sha256": raw[session].payload_sha256,
            "adjusted_revision_id": str(adjusted[session].bar_revision_id),
            "adjusted_payload_sha256": adjusted[session].payload_sha256,
            "close_factor": str(adjusted[session].close_price / raw[session].close_price),
        } for session in sessions],
        "available_at": max(available_ats).isoformat(),
    }
    return ProviderAdjustmentContinuityBinding(
        status="READY_FOR_DERIVED_EVIDENCE", policy_sha256=policy.sha256,
        review_manifest_sha256=sha256_json(manifest), ticker=ticker.upper(),
        security_id=security_id, required_sessions=sessions,
        median_close_factor=median(factors), minimum_close_factor=min(factors),
        maximum_close_factor=max(factors), available_at=max(available_ats),
        largest_deviation_session=largest_deviation_session,
        largest_deviation_factor=largest_deviation_factor,
        largest_deviation_raw_close=float(raw[largest_deviation_session].close_price),
        largest_deviation_adjusted_close=float(adjusted[largest_deviation_session].close_price),
        blocker_codes=(),
    )


def _bars_by_session(bars: Sequence[EquityBarRevision]) -> dict[date, EquityBarRevision]:
    result = {}
    for bar in bars:
        if bar.session_date in result:
            raise ValueError("daily continuity inputs contain duplicate sessions")
        result[bar.session_date] = bar
    return result


def _recorded_by_session(
    bars: Sequence[EquityBarRevision], recorded_ats: Sequence[datetime],
) -> dict[date, datetime]:
    if len(bars) != len(recorded_ats):
        return {}
    return {bar.session_date: recorded_at for bar, recorded_at in zip(bars, recorded_ats)}


@dataclass(frozen=True, slots=True)
class AdjustedDailyHistoryBinding:
    status: str
    policy_sha256: str
    source_manifest_sha256: str | None
    security_id: UUID | None
    ticker: str
    history_available_at: datetime | None
    bars: tuple[EquityBarRevision, ...]
    bar_recorded_ats: tuple[datetime, ...]
    blocker_codes: tuple[str, ...]


def bind_adjusted_daily_history(
    bars: Sequence[EquityBarRevision],
    bar_recorded_ats: Sequence[datetime],
    *,
    security_id: UUID,
    ticker: str,
    received_at: datetime,
) -> AdjustedDailyHistoryBinding:
    policy = ADJUSTED_DAILY_HISTORY_POLICY
    ordered = tuple(bars)
    recorded = tuple(bar_recorded_ats)
    blockers = []
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("adjusted history receipt must be timezone-aware")
    if len(ordered) < policy.minimum_tail_bars:
        blockers.append("ADJUSTED_DAILY_HISTORY_SHORT")
    if len(ordered) != len(recorded):
        blockers.append("ADJUSTED_DAILY_RECORDING_CLOCKS_INCOMPLETE")
    selected = ordered[-policy.minimum_tail_bars:]
    selected_recorded = recorded[-policy.minimum_tail_bars:]
    if any(
        not bar.is_final or bar.interval != "1d" or not bar.adjusted
        or bar.session_scope is not BarSessionScope.RTH
        or bar.availability_mode is not BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
        or policy.quality_code not in bar.quality_codes
        or bar.security_id != security_id or bar.ticker != ticker.upper()
        for bar in selected
    ):
        blockers.append("ADJUSTED_DAILY_IDENTITY_OR_SOURCE_MISMATCH")
    if any(current.bar_end <= previous.bar_end for previous, current in zip(selected, selected[1:])):
        blockers.append("ADJUSTED_DAILY_ORDER_AMBIGUOUS")
    if selected:
        calendar = exchange_calendars.get_calendar("XNYS")
        expected_sessions = tuple(
            session.date() for session in calendar.sessions_in_range(
                pd.Timestamp(selected[0].session_date),
                pd.Timestamp(selected[-1].session_date),
            )
        )
        if tuple(bar.session_date for bar in selected) != expected_sessions:
            blockers.append("ADJUSTED_DAILY_SESSION_GAPS")
    available_ats = []
    if len(selected) == len(selected_recorded):
        for bar, created_at in zip(selected, selected_recorded):
            clocks = (bar.bar_end, bar.system_observed_at, bar.replay_available_at, created_at)
            if any(clock is None or clock.tzinfo is None or clock.utcoffset() is None for clock in clocks):
                blockers.append("ADJUSTED_DAILY_AVAILABILITY_CLOCK_MISSING")
                break
            if bar.provider_published_at is not None:
                clocks = (*clocks, bar.provider_published_at)
            available_ats.append(max(clocks))
        if available_ats and max(available_ats) > received_at:
            blockers.append("ADJUSTED_DAILY_AFTER_RECEIPT")
    if blockers:
        return AdjustedDailyHistoryBinding(
            status="INSUFFICIENT_HISTORY" if blockers == ["ADJUSTED_DAILY_HISTORY_SHORT"] else "UNAVAILABLE",
            policy_sha256=policy.sha256, source_manifest_sha256=None,
            security_id=security_id, ticker=ticker.upper(), history_available_at=None,
            bars=(), bar_recorded_ats=(), blocker_codes=tuple(dict.fromkeys(blockers)),
        )
    manifest = {
        "policy_sha256": policy.sha256, "security_id": str(security_id),
        "ticker": ticker.upper(), "interval": "1d",
        "bar_revision_ids": [str(bar.bar_revision_id) for bar in selected],
        "bar_payload_sha256s": [bar.payload_sha256 for bar in selected],
        "bar_ends": [bar.bar_end.isoformat() for bar in selected],
        "bar_available_ats": [value.isoformat() for value in available_ats],
    }
    return AdjustedDailyHistoryBinding(
        status="READY_FOR_DERIVED_EVIDENCE", policy_sha256=policy.sha256,
        source_manifest_sha256=sha256_json(manifest), security_id=security_id,
        ticker=ticker.upper(), history_available_at=max(available_ats),
        bars=selected, bar_recorded_ats=selected_recorded, blocker_codes=(),
    )


def build_adjusted_daily_evidence_draft(
    history: AdjustedDailyHistoryBinding,
    *,
    security_revision_id: UUID,
) -> EquityEvidence:
    if history.status != "READY_FOR_DERIVED_EVIDENCE" or history.source_manifest_sha256 is None or not history.bars:
        raise ValueError("adjusted daily history is not ready for derived evidence")
    available_at = history.history_available_at
    if available_at is None:
        raise ValueError("adjusted history availability is missing")
    calendar = exchange_calendars.get_calendar("XNYS")
    latest_session = pd.Timestamp(history.bars[-1].session_date)
    if not calendar.is_session(latest_session):
        raise ValueError("adjusted history latest bar is not an exchange session")
    next_session = calendar.next_session(latest_session)
    valid_until = (
        calendar.session_close(next_session).to_pydatetime().astimezone(timezone.utc)
        + timedelta(minutes=ADJUSTED_DAILY_HISTORY_POLICY.post_close_grace_minutes)
    )
    if valid_until <= available_at:
        raise ValueError("adjusted daily history is no longer current at availability")
    payload = {
        "schema_version": ADJUSTED_DAILY_EVIDENCE_SCHEMA,
        "source_policy_sha256": history.policy_sha256,
        "source_manifest_sha256": history.source_manifest_sha256,
        "price_basis": "PROVIDER_SPLIT_ADJUSTED",
        "history_mode": "RECONSTRUCTED_HISTORY",
        "history_available_at": history.history_available_at.isoformat(),
        "bar_count": len(history.bars),
        "window_start": history.bars[0].session_date.isoformat(),
        "window_end": history.bars[-1].session_date.isoformat(),
    }
    payload_sha256 = sha256_json(payload)
    evidence_key = ":".join((
        ADJUSTED_DAILY_EVIDENCE_SOURCE, ADJUSTED_DAILY_HISTORY_POLICY.version,
        str(history.security_id), str(security_revision_id), history.source_manifest_sha256,
    ))
    return EquityEvidence(
        evidence_id=uuid5(NAMESPACE_URL, f"equity-evidence:{evidence_key}"),
        evidence_key=evidence_key,
        lifecycle_key=f"stock-behavior-adjusted-daily:{history.ticker}",
        evidence_type=EvidenceType.FEATURE_SNAPSHOT, evidence_role=EvidenceRole.REGIME,
        security_id=history.security_id, ticker=history.ticker, interval="1d",
        direction=None, lifecycle_status=LifecycleStatus.SNAPSHOT, strength=None,
        market_time=history.bars[-1].bar_end, observed_at=available_at,
        valid_until=valid_until, source_name=ADJUSTED_DAILY_EVIDENCE_SOURCE,
        source_version=ADJUSTED_DAILY_HISTORY_POLICY.version,
        payload_schema_version=ADJUSTED_DAILY_EVIDENCE_SCHEMA,
        analysis_run_id=None, latest_bar_revision_id=history.bars[-1].bar_revision_id,
        security_revision_id=security_revision_id, fundamental_report_ids=(),
        source_revision_ids=tuple(bar.bar_revision_id for bar in history.bars),
        quality_state=QualityState.COMPLETE,
        quality_codes=("HISTORICAL_RECONSTRUCTED_HISTORY", "PROVIDER_SPLIT_ADJUSTED"),
        qualification_revision_id=None, payload_json=canonical_json(payload),
        payload_sha256=payload_sha256,
    )


def bind_adjusted_daily_evidence(
    history: AdjustedDailyHistoryBinding,
    evidence: EquityEvidence,
    *,
    evidence_recorded_at: datetime,
    received_at: datetime,
) -> FinalizedBarSeries:
    if history.status != "READY_FOR_DERIVED_EVIDENCE" or history.source_manifest_sha256 is None or not history.bars:
        raise ValueError("adjusted daily history is not ready for derived evidence")
    try:
        payload = json.loads(evidence.payload_json)
    except (TypeError, ValueError) as exc:
        raise ValueError("adjusted daily evidence payload is invalid") from exc
    if (
        evidence.source_name != ADJUSTED_DAILY_EVIDENCE_SOURCE
        or evidence.source_version != ADJUSTED_DAILY_HISTORY_POLICY.version
        or evidence.payload_schema_version != ADJUSTED_DAILY_EVIDENCE_SCHEMA
        or evidence.evidence_type is not EvidenceType.FEATURE_SNAPSHOT
        or evidence.quality_state is not QualityState.COMPLETE
        or evidence.security_id != history.security_id or evidence.ticker != history.ticker
        or evidence.interval != "1d" or evidence.latest_bar_revision_id != history.bars[-1].bar_revision_id
        or evidence.source_revision_ids != tuple(bar.bar_revision_id for bar in history.bars)
        or evidence.payload_sha256 != sha256_json(payload)
        or payload.get("source_manifest_sha256") != history.source_manifest_sha256
        or payload.get("source_policy_sha256") != history.policy_sha256
        or evidence.valid_until is None
    ):
        raise ValueError("adjusted daily evidence does not bind the exact history")
    if (
        evidence_recorded_at.tzinfo is None or evidence_recorded_at.utcoffset() is None
        or received_at.tzinfo is None or received_at.utcoffset() is None
        or not history.history_available_at <= evidence.observed_at <= evidence_recorded_at <= received_at
    ):
        raise ValueError("adjusted daily evidence clocks are not causal")
    source = BehaviorSource(
        evidence_id=evidence.evidence_id, security_id=history.security_id, interval="1d",
        payload_sha256=evidence.payload_sha256, policy_sha256=history.policy_sha256,
        market_time=evidence.market_time, observed_at=evidence.observed_at,
        recorded_at=evidence_recorded_at, received_at=received_at, valid_until=evidence.valid_until,
        price_basis="PROVIDER_SPLIT_ADJUSTED",
        source_manifest_sha256=history.source_manifest_sha256,
        availability_mode="PROSPECTIVE_RECEIPT",
        history_mode="RECONSTRUCTED_HISTORY",
        history_available_at=history.history_available_at,
    )
    return finalized_series_from_revisions(
        history.bars, source, bar_recorded_ats=history.bar_recorded_ats,
    )


class BehaviorSourceBinding(Contract):
    status: Literal["READY", "STALE", "UNAVAILABLE"]
    policy_sha256: Sha256
    coverage_ids: tuple[UUID, ...] = ()
    series: FinalizedBarSeries | None = None
    available_at: AwareDatetime | None = None
    received_at: AwareDatetime
    blocker_codes: tuple[str, ...] = ()


def build_raw_source_evidence_draft(
    binding: BehaviorSourceBinding,
    *,
    security_revision_id: UUID,
) -> EquityEvidence:
    if binding.status != "READY" or binding.series is None:
        raise ValueError("raw source binding must be fresh and ready")
    series = binding.series
    original = series.source
    if binding.available_at is None:
        raise ValueError("raw source binding availability is missing")
    manifest = {
        "schema_version": RAW_FEATURE_EVIDENCE_SCHEMA,
        "source_policy_sha256": binding.policy_sha256,
        "origin_evidence_id": str(original.evidence_id),
        "origin_payload_sha256": original.payload_sha256,
        "action_coverage_ids": [str(value) for value in binding.coverage_ids],
        "history_mode": original.history_mode,
        "history_available_at": (
            original.history_available_at.isoformat() if original.history_available_at else None
        ),
        "source_available_at": binding.available_at.isoformat(),
        "price_basis": original.price_basis,
        "bar_revision_ids": [str(value) for value in series.bar_revision_ids],
        "bar_payload_sha256s": list(series.bar_payload_sha256s),
        "bar_ends": [value.isoformat() for value in series.bar_ends],
        "bar_available_ats": [value.isoformat() for value in series.bar_available_ats],
    }
    payload_sha256 = sha256_json(manifest)
    evidence_key = ":".join((
        RAW_FEATURE_EVIDENCE_SOURCE, BEHAVIOR_SOURCE_SELECTION_POLICY.version,
        str(series.security_id), str(security_revision_id), series.interval,
        payload_sha256,
    ))
    return EquityEvidence(
        evidence_id=uuid5(NAMESPACE_URL, f"equity-evidence:{evidence_key}"),
        evidence_key=evidence_key,
        lifecycle_key=f"stock-behavior-raw:{series.ticker}:{series.interval}",
        evidence_type=EvidenceType.FEATURE_SNAPSHOT, evidence_role=EvidenceRole.REGIME,
        security_id=series.security_id, ticker=series.ticker, interval=series.interval,
        direction=None, lifecycle_status=LifecycleStatus.SNAPSHOT, strength=None,
        market_time=original.market_time, observed_at=binding.available_at,
        valid_until=original.valid_until, source_name=RAW_FEATURE_EVIDENCE_SOURCE,
        source_version=BEHAVIOR_SOURCE_SELECTION_POLICY.version,
        payload_schema_version=RAW_FEATURE_EVIDENCE_SCHEMA,
        analysis_run_id=None, latest_bar_revision_id=series.bar_revision_ids[-1],
        security_revision_id=security_revision_id, fundamental_report_ids=(),
        source_revision_ids=series.bar_revision_ids, quality_state=QualityState.COMPLETE,
        quality_codes=("ORIGIN_EVIDENCE_BOUND", "RAW_ACTION_GATED"),
        qualification_revision_id=None, payload_json=canonical_json(manifest),
        payload_sha256=payload_sha256,
    )


def bind_raw_source_evidence(
    binding: BehaviorSourceBinding,
    evidence: EquityEvidence,
    *,
    evidence_recorded_at: datetime,
    received_at: datetime,
) -> FinalizedBarSeries:
    if binding.status != "READY" or binding.series is None:
        raise ValueError("raw source binding must be fresh and ready")
    original_series = binding.series
    original = original_series.source
    try:
        payload = json.loads(evidence.payload_json)
    except (TypeError, ValueError) as exc:
        raise ValueError("raw source evidence payload is invalid") from exc
    expected = build_raw_source_evidence_draft(
        binding, security_revision_id=evidence.security_revision_id,
    )
    if (
        evidence != expected
        or evidence.payload_sha256 != sha256_json(payload)
    ):
        raise ValueError("raw source evidence does not bind the exact origin and history")
    if (
        evidence_recorded_at.tzinfo is None or evidence_recorded_at.utcoffset() is None
        or received_at.tzinfo is None or received_at.utcoffset() is None
        or binding.available_at is None
        or not binding.available_at <= evidence.observed_at <= evidence_recorded_at <= received_at
    ):
        raise ValueError("raw source evidence clocks are not causal")
    source = BehaviorSource(
        evidence_id=evidence.evidence_id, security_id=evidence.security_id,
        interval=evidence.interval, payload_sha256=evidence.payload_sha256,
        policy_sha256=binding.policy_sha256, market_time=evidence.market_time,
        observed_at=evidence.observed_at, recorded_at=evidence_recorded_at,
        received_at=received_at, valid_until=evidence.valid_until,
        price_basis="RAW_ACTION_GATED",
        action_review_ids=tuple(map(str, binding.coverage_ids)),
        source_manifest_sha256=evidence.payload_sha256,
        availability_mode="PROSPECTIVE_RECEIPT",
        history_mode=original.history_mode,
        history_available_at=original.history_available_at,
    )
    return original_series.model_copy(update={"source": source})


def bind_feature_source(
    read: BehaviorFeatureSourceRead,
    coverage_rows: Sequence[Mapping],
    actions_by_coverage: Mapping[UUID, Sequence[Mapping]],
    *,
    received_at: datetime,
) -> BehaviorSourceBinding:
    policy = BEHAVIOR_SOURCE_SELECTION_POLICY
    evidence = read.evidence
    blockers = []
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("source receipt must be timezone-aware")
    if (
        evidence.evidence_type.value != policy.evidence_type
        or evidence.source_name != policy.source_name
        or evidence.source_version != policy.source_version
        or evidence.payload_schema_version != policy.payload_schema_version
        or evidence.interval not in policy.intervals
    ):
        blockers.append("FEATURE_SOURCE_POLICY_MISMATCH")
    if evidence.quality_state is not QualityState.COMPLETE:
        blockers.append("FEATURE_EVIDENCE_NOT_COMPLETE")
    if evidence.valid_until is None:
        blockers.append("FEATURE_VALIDITY_UNAVAILABLE")
    if evidence.observed_at > received_at or read.evidence_created_at > received_at:
        blockers.append("FEATURE_EVIDENCE_AFTER_RECEIPT")
    if read.missing_revision_ids or tuple(bar.bar_revision_id for bar in read.bars) != read.selected_revision_ids:
        blockers.append("SOURCE_REVISION_LINEAGE_INCOMPLETE")
    if len(read.bar_created_ats) != len(read.bars) or any(value > received_at for value in read.bar_created_ats):
        blockers.append("BAR_RECORDING_AFTER_RECEIPT")
    if not read.bars:
        blockers.append("SOURCE_REVISION_LINEAGE_EMPTY")
    if blockers:
        return _unavailable(received_at, *blockers)

    bar_modes = {bar.availability_mode for bar in read.bars}
    if len(bar_modes) != 1:
        return _unavailable(received_at, "BAR_AVAILABILITY_MODE_MIXED")
    source_mode = (
        "RECONSTRUCTED"
        if next(iter(bar_modes)) is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
        else "PROSPECTIVE_RECEIPT"
    )
    window_start = read.bars[0].session_date
    window_end = read.bars[-1].session_date
    coverage, actions, coverage_blockers = _select_split_coverage(
        coverage_rows, actions_by_coverage, ticker=evidence.ticker,
        security_id=evidence.security_id, window_start=window_start, window_end=window_end,
        received_at=received_at, source_mode=source_mode,
    )
    if not coverage:
        return _unavailable(received_at, *coverage_blockers)
    relevant_actions = tuple(
        action for action in actions
        if window_start <= action["effective_date"] <= window_end
    )
    if relevant_actions:
        return _unavailable(received_at, "RAW_PRICE_SPLIT_PRESENT")

    coverage_ids = tuple(sorted((UUID(str(row["coverage_id"])) for row in coverage), key=str))
    source_available_at = max(
        evidence.observed_at,
        read.evidence_created_at,
        *(max(clock for clock in (
            bar.bar_end, bar.system_observed_at, bar.provider_published_at,
            bar.replay_available_at, created_at,
        ) if clock is not None) for bar, created_at in zip(read.bars, read.bar_created_ats)),
        *(max(clock for clock in (
            row.get("first_observed_at"), row.get("created_at"), row.get("replay_available_at"),
        ) if clock is not None) for row in coverage),
        *(max(clock for clock in (
            row.get("first_observed_at"), row.get("created_at"),
            row.get("revised_observed_at"), row.get("replay_available_at"),
        ) if clock is not None) for row in actions),
    )
    source = BehaviorSource(
        evidence_id=evidence.evidence_id, security_id=evidence.security_id,
        interval=evidence.interval, payload_sha256=evidence.payload_sha256,
        policy_sha256=policy.sha256, market_time=evidence.market_time,
        observed_at=evidence.observed_at, recorded_at=read.evidence_created_at,
        received_at=received_at, valid_until=evidence.valid_until,
        price_basis="RAW_ACTION_GATED", action_review_ids=tuple(map(str, coverage_ids)),
        availability_mode=source_mode,
        history_mode=(
            "RECONSTRUCTED_HISTORY" if source_mode == "RECONSTRUCTED"
            else "LIVE_OBSERVED_HISTORY"
        ),
        history_available_at=(
            _history_available_at(read) if source_mode == "RECONSTRUCTED" else None
        ),
    )
    try:
        series = finalized_series_from_revisions(
            read.bars, source, bar_recorded_ats=read.bar_created_ats,
        )
    except (TypeError, ValueError):
        return _unavailable(received_at, "FINALIZED_SERIES_CONTRACT_MISMATCH")
    status = "STALE" if source.valid_until <= received_at else "READY"
    return BehaviorSourceBinding(
        status=status, policy_sha256=policy.sha256, coverage_ids=coverage_ids,
        series=series, available_at=source_available_at, received_at=received_at,
        blocker_codes=("FEATURE_EVIDENCE_EXPIRED_AT_RECEIPT",) if status == "STALE" else (),
    )


def _select_split_coverage(
    coverage_rows: Sequence[Mapping], actions_by_coverage: Mapping[UUID, Sequence[Mapping]],
    *, ticker: str, security_id: UUID, window_start, window_end,
    received_at: datetime, source_mode: str,
) -> tuple[tuple[Mapping, ...], tuple[Mapping, ...], tuple[str, ...]]:
    expected_mode = "HISTORICAL_RECONSTRUCTED" if source_mode == "RECONSTRUCTED" else "LIVE_OBSERVED"
    scoped = []
    for coverage in coverage_rows:
        mode = getattr(coverage.get("availability_mode"), "value", coverage.get("availability_mode"))
        try:
            coverage_security_id = UUID(str(coverage.get("security_id")))
        except (TypeError, ValueError):
            coverage_security_id = None
        if (
            coverage.get("source") != BEHAVIOR_SOURCE_SELECTION_POLICY.action_source
            or coverage.get("action_type") != "SPLIT"
            or coverage.get("ticker") != ticker
        ):
            continue
        scoped.append((coverage, coverage_security_id, mode))
    if not scoped:
        return (), (), ("SPLIT_RESPONSE_COVERAGE_MISSING",)
    identity_matched = [item for item in scoped if item[1] == security_id]
    if not identity_matched:
        return (), (), ("SPLIT_RESPONSE_COVERAGE_IDENTITY_MISMATCH",)
    window_matched = [item for item in identity_matched if (
        item[0].get("window_start") is not None and item[0].get("window_end") is not None
        and item[0]["window_start"] <= window_end and item[0]["window_end"] >= window_start
    )]
    if not window_matched:
        return (), (), ("SPLIT_RESPONSE_COVERAGE_WINDOW_MISSING",)
    mode_matched = [item for item in window_matched if item[2] == expected_mode]
    if not mode_matched:
        return (), (), ("SPLIT_RESPONSE_COVERAGE_MODE_MISMATCH",)
    candidates = []
    future_candidates = []
    for coverage, _, mode in mode_matched:
        clocks = [coverage.get("first_observed_at"), coverage.get("created_at")]
        if mode == "HISTORICAL_RECONSTRUCTED":
            clocks.append(coverage.get("replay_available_at"))
        if any(value is None or value.tzinfo is None or value.utcoffset() is None for value in clocks):
            continue
        if max(clocks) > received_at:
            future_candidates.append(coverage)
            continue
        candidates.append(coverage)
    selected_by_day = {}
    day = window_start
    while day <= window_end:
        available = [row for row in candidates if row["window_start"] <= day <= row["window_end"]]
        if not available:
            future_covers_day = any(row["window_start"] <= day <= row["window_end"] for row in future_candidates)
            reason = "SPLIT_RESPONSE_COVERAGE_AFTER_RECEIPT" if future_covers_day else "SPLIT_RESPONSE_COVERAGE_WINDOW_MISSING"
            return (), (), (reason,)
        available.sort(key=lambda row: (row["first_observed_at"], row["created_at"], str(row["coverage_id"])), reverse=True)
        if len(available) > 1 and (available[0]["first_observed_at"], available[0]["created_at"]) == (
            available[1]["first_observed_at"], available[1]["created_at"],
        ):
            return (), (), ("SPLIT_RESPONSE_COVERAGE_AMBIGUOUS",)
        selected_by_day[day] = UUID(str(available[0]["coverage_id"]))
        day += timedelta(days=1)
    chosen_ids = frozenset(selected_by_day.values())
    selected = tuple(row for row in candidates if UUID(str(row["coverage_id"])) in chosen_ids)
    selected_actions = []
    for coverage in selected:
        coverage_id = UUID(str(coverage["coverage_id"]))
        actions = tuple(actions_by_coverage.get(coverage_id, ()))
        try:
            validate_action_coverage(coverage, actions)
        except (KeyError, TypeError, ValueError):
            return (), (), ("SPLIT_RESPONSE_COVERAGE_INVALID",)
        for action in actions:
            mode = getattr(action.get("availability_mode"), "value", action.get("availability_mode"))
            if action.get("source") != coverage["source"] or mode != expected_mode:
                return (), (), ("SPLIT_ACTION_SOURCE_CONTRACT_MISMATCH",)
            clocks = [action.get("first_observed_at"), action.get("created_at")]
            if mode == "HISTORICAL_RECONSTRUCTED":
                clocks.append(action.get("replay_available_at"))
            if action.get("revised_observed_at") is not None:
                clocks.append(action["revised_observed_at"])
            if any(value is None or value.tzinfo is None or value.utcoffset() is None for value in clocks) or max(clocks) > received_at:
                return (), (), ("SPLIT_ACTION_AFTER_RECEIPT",)
            if action["first_observed_at"] > coverage["first_observed_at"]:
                return (), (), ("SPLIT_ACTION_AFTER_COVERAGE_RESPONSE",)
            effective_date = action["effective_date"]
            if window_start <= effective_date <= window_end and selected_by_day[effective_date] == coverage_id:
                selected_actions.append(action)
    return selected, tuple(selected_actions), ()


def split_coverage_gap_sessions(
    coverage_rows: Sequence[Mapping], actions_by_coverage: Mapping[UUID, Sequence[Mapping]],
    *, ticker: str, security_id: UUID, sessions: Sequence[date],
    received_at: datetime, source_mode: str = "PROSPECTIVE_RECEIPT",
) -> tuple[date, ...]:
    gaps = []
    for session in sorted(set(sessions)):
        selected, _, blockers = _select_split_coverage(
            coverage_rows, actions_by_coverage, ticker=ticker,
            security_id=security_id, window_start=session, window_end=session,
            received_at=received_at, source_mode=source_mode,
        )
        if selected:
            continue
        if set(blockers) <= {
            "SPLIT_RESPONSE_COVERAGE_MISSING",
            "SPLIT_RESPONSE_COVERAGE_WINDOW_MISSING",
        }:
            gaps.append(session)
            continue
        raise ValueError(f"split coverage is present but unusable: {blockers}")
    return tuple(gaps)


def _unavailable(received_at: datetime, *blockers: str) -> BehaviorSourceBinding:
    return BehaviorSourceBinding(
        status="UNAVAILABLE", policy_sha256=BEHAVIOR_SOURCE_SELECTION_POLICY.sha256,
        received_at=received_at, blocker_codes=tuple(dict.fromkeys(blockers)),
    )


def _history_available_at(read: BehaviorFeatureSourceRead) -> datetime:
    return max(
        max(clock for clock in (
            bar.bar_end, bar.system_observed_at, bar.provider_published_at,
            bar.replay_available_at, created_at,
        ) if clock is not None)
        for bar, created_at in zip(read.bars, read.bar_created_ats)
    )