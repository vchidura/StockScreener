"""Durable evidence and outcome policies for reconstructed signal subjects."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid5

from research.composite_scanners import (
    COMPOSITE_OUTCOME_HORIZONS,
    COMPOSITE_SCANNER_REGISTRY,
)
from research.historical_signal_replay import HistoricalSignalEvent

from .domain import (
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    LifecycleStatus,
    QualityState,
    SecurityReferenceRevision,
)
from .outcomes import (
    OutcomePolicy,
    default_directional_policy,
    recommendation_plan_policy,
)
from .polygon import canonical_json, sha256_json


GAP_PRIMARY_SOURCES = (
    "GAP_BREAKAWAY_HOLD",
    "GAP_CONTINUATION_HOLD",
    "GAP_FADE_REVERSAL",
)
HISTORICAL_SIGNAL_SCHEMA_VERSION = "historical_signal_event_v2"


def historical_event_evidence_id(event: HistoricalSignalEvent):
    evidence_key = (
        f"historical-signal:{HISTORICAL_SIGNAL_SCHEMA_VERSION}:{event.event_id}"
    )
    return uuid5(NAMESPACE_URL, f"equity-evidence:{evidence_key}")


def historical_event_evidence(
    event: HistoricalSignalEvent,
    security: SecurityReferenceRevision,
) -> EquityEvidence:
    payload = _event_payload(event)
    payload_sha256 = sha256_json(payload)
    evidence_key = (
        f"historical-signal:{HISTORICAL_SIGNAL_SCHEMA_VERSION}:{event.event_id}"
    )
    return EquityEvidence(
        evidence_id=historical_event_evidence_id(event),
        evidence_key=evidence_key,
        lifecycle_key=f"historical-signal:{event.source_name}:{event.setup_anchor}",
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION,
        security_id=security.security_id,
        ticker=event.ticker,
        interval="1d",
        direction=event.direction,
        lifecycle_status=LifecycleStatus.MATCH,
        strength=None,
        market_time=event.signal_time,
        observed_at=event.signal_time,
        valid_until=None,
        source_name=event.source_name,
        source_version=event.source_version,
        payload_schema_version=HISTORICAL_SIGNAL_SCHEMA_VERSION,
        analysis_run_id=None,
        latest_bar_revision_id=(
            event.source_bar_revision_ids[-1]
            if event.source_bar_revision_ids else None
        ),
        security_revision_id=security.security_revision_id,
        fundamental_report_ids=(),
        source_revision_ids=event.source_bar_revision_ids,
        quality_state=QualityState.RESEARCH_ONLY,
        quality_codes=(
            "HISTORICAL_RECONSTRUCTED",
            "REPLAY_ONLY",
            "UNQUALIFIED_DIRECTION",
        ),
        qualification_revision_id=None,
        payload_json=canonical_json(payload),
        payload_sha256=payload_sha256,
    )


def primary_gap_outcome_policies(
    *,
    source_version: str,
    effective_from: datetime,
    horizon_sessions: Sequence[int] = (5, 10, 21),
    round_trip_cost_bps: float = 4.0,
    source_names: Sequence[str] = GAP_PRIMARY_SOURCES,
) -> tuple[OutcomePolicy, ...]:
    horizons = tuple(dict.fromkeys(int(value) for value in horizon_sessions))
    if not horizons or any(value <= 0 for value in horizons):
        raise ValueError("horizon_sessions must contain positive values")
    sources = tuple(dict.fromkeys(str(value) for value in source_names if str(value)))
    if not sources:
        raise ValueError("source_names must not be empty")
    return tuple(
        default_directional_policy(
            source_name=source_name,
            source_version=source_version,
            interval="1d",
            horizons={f"{value}d": value for value in horizons},
            effective_from=effective_from,
            round_trip_cost_bps=round_trip_cost_bps,
            sector_benchmark=True,
            primary_benchmark="SECTOR",
            policy_version="directional_outcome_v3_sector",
        )
        for source_name in sources
    )


def composite_scanner_outcome_policies(
    *,
    interval: str,
    effective_from: datetime,
    round_trip_cost_bps: float = 4.0,
) -> tuple[OutcomePolicy, ...]:
    horizons = COMPOSITE_OUTCOME_HORIZONS.get(interval)
    if horizons is None:
        raise ValueError(f"unsupported composite scanner interval: {interval}")
    policies = []
    for registration in COMPOSITE_SCANNER_REGISTRY.values():
        if interval not in registration.supported_intervals:
            continue
        common = {
            "source_name": registration.source_name,
            "source_version": registration.source_version,
            "interval": interval,
            "horizons": horizons,
            "effective_from": effective_from,
            "round_trip_cost_bps": round_trip_cost_bps,
            "primary_benchmark": "SECTOR",
        }
        policies.append(default_directional_policy(
            **common,
            policy_version="directional_outcome_v3_sector",
        ))
        policies.append(recommendation_plan_policy(
            **common,
            policy_version="recommendation_plan_sector_v1",
        ))
    return tuple(policies)


def _event_payload(event: HistoricalSignalEvent) -> dict[str, Any]:
    payload = dict(event.payload)
    payload.update({
        "historical_event_id": str(event.event_id),
        "setup_anchor": event.setup_anchor,
        "source_bar_revision_ids": [
            str(value) for value in event.source_bar_revision_ids
        ],
        "universe_policy_version": event.universe_policy_version,
        "universe_run_id": str(event.universe_run_id),
    })
    close = _number(payload.get("last_close"))
    explicit_stop = _number(payload.get("stop_price"))
    explicit_target = _number(payload.get("target_price"))
    if (
        close is not None
        and explicit_stop is not None
        and explicit_target is not None
        and event.direction * (close - explicit_stop) > 0
        and event.direction * (explicit_target - close) > 0
    ):
        payload["stop_price"] = round(explicit_stop, 8)
        payload["target_price"] = round(explicit_target, 8)
        return payload
    if payload.get("hypothesis") == "FADE_REVERSAL":
        stop = _number(payload.get("current_open"))
    else:
        stop = _number(payload.get("fill_target"))
    if close is not None and stop is not None:
        risk = abs(close - stop)
        if risk > 0 and event.direction * (close - stop) > 0:
            payload["stop_price"] = round(stop, 8)
            payload["target_price"] = round(close + event.direction * 2 * risk, 8)
            payload["risk_basis"] = "FORMATION_CLOSE_TO_INVALIDATION"
    return payload


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number