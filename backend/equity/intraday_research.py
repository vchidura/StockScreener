"""Durable evidence and outcome policies for intraday scanner research subjects.

Events produced here are fixed-cohort exploratory research over the bootstrapped
five-year 30m history. They are never eligible for live recommendations.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import pandas as pd

from research.intraday_scanners import (
    INTRADAY_DETECTOR_POLICY,
    INTRADAY_OUTCOME_HORIZONS,
    INTRADAY_SCANNER_REGISTRY,
)

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


INTRADAY_SIGNAL_SCHEMA_VERSION = "intraday_scanner_event_v1"
INTRADAY_RESEARCH_COHORT = "FIXED_COHORT_EXPLORATORY"
INTRADAY_EVIDENCE_QUALITY_CODES = (
    "INTRADAY_FIXED_COHORT",
    "LIVE_OBSERVED_BOOTSTRAP",
    "REPLAY_ONLY",
    "UNQUALIFIED_DIRECTION",
)


@dataclass(frozen=True, slots=True)
class IntradayScannerEvent:
    event_id: UUID
    source_name: str
    source_version: str
    ticker: str
    interval: str
    session_date: date
    signal_time: datetime
    direction: int
    setup_anchor: str
    universe_run_id: UUID
    universe_policy_version: str
    source_bar_revision_ids: tuple[UUID, ...]
    payload: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "source_name": self.source_name,
            "source_version": self.source_version,
            "ticker": self.ticker,
            "interval": self.interval,
            "session_date": self.session_date.isoformat(),
            "signal_time": self.signal_time.isoformat(),
            "direction": self.direction,
            "setup_anchor": self.setup_anchor,
            "universe_run_id": str(self.universe_run_id),
            "universe_policy_version": self.universe_policy_version,
            "source_bar_revision_ids": [
                str(value) for value in self.source_bar_revision_ids
            ],
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "IntradayScannerEvent":
        signal_time = pd.Timestamp(row["signal_time"])
        if signal_time.tzinfo is None:
            raise ValueError("intraday event signal_time must be timezone-aware")
        return cls(
            event_id=UUID(str(row["event_id"])),
            source_name=str(row["source_name"]),
            source_version=str(row["source_version"]),
            ticker=str(row["ticker"]).upper(),
            interval=str(row.get("interval") or "30m"),
            session_date=date.fromisoformat(str(row["session_date"])),
            signal_time=signal_time.tz_convert("UTC").to_pydatetime(),
            direction=int(row["direction"]),
            setup_anchor=str(row["setup_anchor"]),
            universe_run_id=UUID(str(row["universe_run_id"])),
            universe_policy_version=str(row["universe_policy_version"]),
            source_bar_revision_ids=tuple(
                UUID(str(value)) for value in row.get("source_bar_revision_ids", ())
            ),
            payload=dict(row.get("payload") or {}),
        )


def build_intraday_event(
    *,
    detection: Mapping[str, Any],
    universe_run_id: UUID,
    universe_policy_version: str,
    interval: str = "30m",
) -> IntradayScannerEvent:
    """Convert a detector row into a deterministic, replayable research event."""
    source_name = str(detection["scanner_name"])
    registration = INTRADAY_SCANNER_REGISTRY[source_name]
    setup_anchor = str(detection["setup_anchor"])
    identity = sha256_json({
        "anchor": setup_anchor,
        "detector_policy_version": INTRADAY_DETECTOR_POLICY["detector_policy_version"],
        "interval": interval,
        "source_name": source_name,
        "source_version": registration.source_version,
        "universe_run_id": str(universe_run_id),
    })
    signal_time = pd.Timestamp(detection["signal_time"]).tz_convert("UTC")
    payload = {
        key: value for key, value in detection.items()
        if key not in ("scanner_name", "scanner_version", "signal_time",
                       "session_date", "signal_bar_id", "source_bar_revision_ids",
                       "ticker", "direction", "setup_anchor")
    }
    payload.update({
        "detector_policy": dict(INTRADAY_DETECTOR_POLICY),
        "interval": interval,
        "outcome_modes": list(registration.outcome_modes),
        "research_cohort": INTRADAY_RESEARCH_COHORT,
        "signal_bar_id": str(detection["signal_bar_id"]),
        "qualification_eligible": bool(
            detection.get("metadata", {}).get("qualification_eligible", True)
        ),
    })
    return IntradayScannerEvent(
        event_id=uuid5(NAMESPACE_URL, f"intraday-scanner-event:{identity}"),
        source_name=source_name,
        source_version=registration.source_version,
        ticker=str(detection["ticker"]).upper(),
        interval=interval,
        session_date=_as_date(detection["session_date"]),
        signal_time=signal_time.to_pydatetime(),
        direction=int(detection["direction"]),
        setup_anchor=setup_anchor,
        universe_run_id=universe_run_id,
        universe_policy_version=universe_policy_version,
        source_bar_revision_ids=tuple(
            UUID(str(value)) for value in detection.get("source_bar_revision_ids", ())
        ),
        payload=payload,
    )


def intraday_event_evidence_id(event: IntradayScannerEvent) -> UUID:
    evidence_key = (
        f"intraday-scanner:{INTRADAY_SIGNAL_SCHEMA_VERSION}:{event.event_id}"
    )
    return uuid5(NAMESPACE_URL, f"equity-evidence:{evidence_key}")


def intraday_event_evidence(
    event: IntradayScannerEvent,
    security: SecurityReferenceRevision,
) -> EquityEvidence:
    payload = dict(event.payload)
    payload.update({
        "intraday_event_id": str(event.event_id),
        "setup_anchor": event.setup_anchor,
        "source_bar_revision_ids": [
            str(value) for value in event.source_bar_revision_ids
        ],
        "universe_policy_version": event.universe_policy_version,
        "universe_run_id": str(event.universe_run_id),
    })
    evidence_key = (
        f"intraday-scanner:{INTRADAY_SIGNAL_SCHEMA_VERSION}:{event.event_id}"
    )
    payload_json = canonical_json(payload)
    return EquityEvidence(
        evidence_id=intraday_event_evidence_id(event),
        evidence_key=evidence_key,
        lifecycle_key=f"intraday-scanner:{event.source_name}:{event.setup_anchor}",
        evidence_type=EvidenceType.SCANNER_RESULT,
        evidence_role=EvidenceRole.DIRECTION,
        security_id=security.security_id,
        ticker=event.ticker,
        interval=event.interval,
        direction=event.direction,
        lifecycle_status=LifecycleStatus.MATCH,
        strength=None,
        market_time=event.signal_time,
        observed_at=event.signal_time,
        valid_until=None,
        source_name=event.source_name,
        source_version=event.source_version,
        payload_schema_version=INTRADAY_SIGNAL_SCHEMA_VERSION,
        analysis_run_id=None,
        latest_bar_revision_id=(
            event.source_bar_revision_ids[-1]
            if event.source_bar_revision_ids else None
        ),
        security_revision_id=security.security_revision_id,
        fundamental_report_ids=(),
        source_revision_ids=event.source_bar_revision_ids,
        quality_state=QualityState.RESEARCH_ONLY,
        quality_codes=INTRADAY_EVIDENCE_QUALITY_CODES,
        qualification_revision_id=None,
        payload_json=payload_json,
        payload_sha256=sha256_json(payload),
    )


def intraday_scanner_outcome_policies(
    *,
    interval: str = "30m",
    effective_from: datetime,
    round_trip_cost_bps: float = 4.0,
    source_names: Sequence[str] = (),
) -> tuple[OutcomePolicy, ...]:
    horizons = INTRADAY_OUTCOME_HORIZONS.get(interval)
    if horizons is None:
        raise ValueError(f"unsupported intraday scanner interval: {interval}")
    selected = tuple(source_names) or tuple(INTRADAY_SCANNER_REGISTRY)
    policies: list[OutcomePolicy] = []
    for source_name in selected:
        registration = INTRADAY_SCANNER_REGISTRY[source_name]
        if interval not in registration.supported_intervals:
            continue
        common = {
            "source_name": registration.source_name,
            "source_version": registration.source_version,
            "interval": interval,
            "horizons": dict(horizons),
            "effective_from": effective_from,
            "round_trip_cost_bps": round_trip_cost_bps,
            "primary_benchmark": "SECTOR",
        }
        policies.append(default_directional_policy(
            **common, policy_version="directional_outcome_v3_sector",
        ))
        if "RECOMMENDATION_PLAN" in registration.outcome_modes:
            policies.append(recommendation_plan_policy(
                **common, policy_version="recommendation_plan_sector_v1",
            ))
    return tuple(policies)


def intraday_policy_keys(source_names: Sequence[str], interval: str = "30m") -> tuple[str, ...]:
    keys = []
    for source_name in source_names:
        registration = INTRADAY_SCANNER_REGISTRY[source_name]
        version = registration.source_version
        keys.append(f"{source_name}:{version}:{interval}:SIGNED:SECTOR_PRIMARY")
        if "RECOMMENDATION_PLAN" in registration.outcome_modes:
            keys.append(
                f"{source_name}:{version}:{interval}:RECOMMENDATION_PLAN:SECTOR_PRIMARY"
            )
    return tuple(sorted(set(keys)))


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()
