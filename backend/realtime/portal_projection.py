from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256
from .detector_evidence import RealtimeDetectorEvent
from .preflight import ProviderTier


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class RealtimePortalSnapshot:
    snapshot_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    snapshot_type: str
    snapshot_key: str
    source_generation: int
    source_manifest_sha256: str
    source_manifest: Mapping[str, object]
    payload_sha256: str
    payload: Mapping[str, object]
    generated_at: datetime
    valid_until: datetime | None


@dataclass(frozen=True, slots=True)
class RealtimeAlertProjection:
    alert_id: UUID
    cohort_id: UUID
    event_id: UUID
    alert_kind: str
    lifecycle: str
    as_of: datetime
    valid_until: datetime | None
    projection_schema: str
    projection_sha256: str
    projection: Mapping[str, object]
    destinations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RealtimeScreeningRow:
    security_id: UUID
    ticker: str
    eligible: bool
    payload_sha256: str
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class RealtimeScreeningGeneration:
    generation_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    session_date: date
    source_manifest_sha256: str
    expected_members: int
    status: str
    generated_at: datetime
    manifest: Mapping[str, object]
    rows: tuple[RealtimeScreeningRow, ...]


@dataclass(frozen=True, slots=True)
class RealtimeDetectorEvaluation:
    evaluation_id: UUID
    event_id: UUID
    detector_id: str
    underlyer: str
    selection_status: str
    observed_at: datetime
    payload_sha256: str
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class RealtimeDetectorDataset:
    dataset_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    dataset_key: str
    session_date: date
    status: str
    detector_ids: tuple[str, ...]
    source_manifest_sha256: str
    started_at: datetime
    completed_at: datetime
    summary: Mapping[str, object]
    evaluations: tuple[RealtimeDetectorEvaluation, ...]


@dataclass(frozen=True, slots=True)
class RealtimeManagementAssessment:
    assessment_id: UUID
    cohort_id: UUID
    alert_id: UUID
    assessed_at: datetime
    status: str
    reason_codes: tuple[str, ...]
    payload_sha256: str
    payload: Mapping[str, object]


def build_portal_snapshot(
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    snapshot_type: str,
    snapshot_key: str,
    source_generation: int,
    source_manifest: Mapping[str, object],
    payload: Mapping[str, object],
    generated_at: datetime,
    valid_until: datetime | None = None,
) -> RealtimePortalSnapshot:
    if not snapshot_type.strip() or not snapshot_key.strip():
        raise ValueError("portal snapshot identity cannot be blank")
    if type(source_generation) is not int or source_generation <= 0:
        raise ValueError("portal source generation must be positive")
    generated = _utc(generated_at, "generated_at")
    expiry = _utc(valid_until, "valid_until") if valid_until is not None else None
    if expiry is not None and expiry <= generated:
        raise ValueError("portal snapshot validity must follow generation")
    manifest_sha256 = canonical_sha256(source_manifest)
    payload_sha256 = canonical_sha256(payload)
    snapshot_id = uuid5(
        NAMESPACE_URL,
        ":".join((
            "realtime-portal",
            str(cohort_id),
            snapshot_type,
            snapshot_key,
            manifest_sha256,
            payload_sha256,
        )),
    )
    return RealtimePortalSnapshot(
        snapshot_id=snapshot_id,
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        snapshot_type=snapshot_type,
        snapshot_key=snapshot_key,
        source_generation=source_generation,
        source_manifest_sha256=manifest_sha256,
        source_manifest=source_manifest,
        payload_sha256=payload_sha256,
        payload=payload,
        generated_at=generated,
        valid_until=expiry,
    )


def build_alert_projection(
    event: RealtimeDetectorEvent,
    *,
    alert_kind: str,
    lifecycle: str,
    projection_schema: str,
    projection: Mapping[str, object],
    destinations: tuple[str, ...],
) -> RealtimeAlertProjection:
    if alert_kind not in {"STOCK", "OPTION"}:
        raise ValueError("unsupported alert kind")
    if not lifecycle.strip() or not projection_schema.strip():
        raise ValueError("alert projection identity cannot be blank")
    normalized_destinations = tuple(sorted(set(value.strip() for value in destinations)))
    if not normalized_destinations or any(not value for value in normalized_destinations):
        raise ValueError("alert projection requires destinations")
    projection_sha256 = canonical_sha256(projection)
    alert_id = uuid5(
        NAMESPACE_URL,
        f"realtime-alert:{event.cohort_id}:{event.event_id}:{alert_kind}",
    )
    return RealtimeAlertProjection(
        alert_id=alert_id,
        cohort_id=event.cohort_id,
        event_id=event.event_id,
        alert_kind=alert_kind,
        lifecycle=lifecycle,
        as_of=event.decision_at,
        valid_until=event.valid_until,
        projection_schema=projection_schema,
        projection_sha256=projection_sha256,
        projection=projection,
        destinations=normalized_destinations,
    )


def build_screening_generation(
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    session_date: date,
    expected_members: int,
    generated_at: datetime,
    manifest: Mapping[str, object],
    rows: tuple[tuple[UUID, str, bool, Mapping[str, object]], ...],
) -> RealtimeScreeningGeneration:
    if expected_members <= 0 or len(rows) > expected_members:
        raise ValueError("invalid screening generation coverage")
    normalized_rows = tuple(
        RealtimeScreeningRow(
            security_id=security_id,
            ticker=ticker.strip().upper(),
            eligible=eligible,
            payload_sha256=canonical_sha256(payload),
            payload=payload,
        )
        for security_id, ticker, eligible, payload in rows
    )
    if any(not row.ticker for row in normalized_rows):
        raise ValueError("screening row ticker cannot be blank")
    if len({row.security_id for row in normalized_rows}) != len(normalized_rows):
        raise ValueError("screening generation securities must be unique")
    manifest_sha256 = canonical_sha256(manifest)
    identity = canonical_sha256({
        "cohort_id": cohort_id,
        "session_date": session_date,
        "source_manifest_sha256": manifest_sha256,
        "rows": tuple((row.security_id, row.payload_sha256) for row in normalized_rows),
    })
    return RealtimeScreeningGeneration(
        generation_id=uuid5(NAMESPACE_URL, f"realtime-screening:{identity}"),
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        session_date=session_date,
        source_manifest_sha256=manifest_sha256,
        expected_members=expected_members,
        status="COMPLETE" if len(normalized_rows) == expected_members else "DEGRADED",
        generated_at=_utc(generated_at, "generated_at"),
        manifest=manifest,
        rows=normalized_rows,
    )


def build_detector_dataset(
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    dataset_key: str,
    session_date: date,
    detector_ids: tuple[str, ...],
    source_manifest_sha256: str,
    started_at: datetime,
    completed_at: datetime,
    summary: Mapping[str, object],
    evaluations: tuple[
        tuple[UUID, str, str, str, datetime, Mapping[str, object]], ...
    ],
    status: str = "COMPLETE",
) -> RealtimeDetectorDataset:
    if not dataset_key.strip() or not detector_ids or status not in {
        "COMPLETE", "DEGRADED", "FAILED"
    }:
        raise ValueError("invalid detector dataset identity or status")
    started = _utc(started_at, "started_at")
    completed = _utc(completed_at, "completed_at")
    if completed < started:
        raise ValueError("detector dataset completion precedes start")
    normalized = []
    for event_id, detector_id, underlyer, selection_status, observed_at, payload in evaluations:
        if detector_id not in detector_ids or selection_status not in {
            "SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"
        }:
            raise ValueError("detector evaluation does not match dataset")
        payload_sha256 = canonical_sha256(payload)
        evaluation_id = uuid5(
            NAMESPACE_URL,
            f"realtime-evaluation:{dataset_key}:{event_id}:{payload_sha256}",
        )
        normalized.append(RealtimeDetectorEvaluation(
            evaluation_id=evaluation_id,
            event_id=event_id,
            detector_id=detector_id,
            underlyer=underlyer.strip().upper(),
            selection_status=selection_status,
            observed_at=_utc(observed_at, "observed_at"),
            payload_sha256=payload_sha256,
            payload=payload,
        ))
    if len({row.event_id for row in normalized}) != len(normalized):
        raise ValueError("detector dataset event IDs must be unique")
    dataset_identity = canonical_sha256({
        "cohort_id": cohort_id,
        "dataset_key": dataset_key,
        "source_manifest_sha256": source_manifest_sha256,
        "evaluations": tuple(row.evaluation_id for row in normalized),
    })
    return RealtimeDetectorDataset(
        dataset_id=uuid5(NAMESPACE_URL, f"realtime-dataset:{dataset_identity}"),
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        dataset_key=dataset_key,
        session_date=session_date,
        status=status,
        detector_ids=tuple(sorted(set(detector_ids))),
        source_manifest_sha256=source_manifest_sha256,
        started_at=started,
        completed_at=completed,
        summary=summary,
        evaluations=tuple(normalized),
    )


def build_management_assessment(
    *,
    cohort_id: UUID,
    alert_id: UUID,
    assessed_at: datetime,
    status: str,
    reason_codes: tuple[str, ...],
    payload: Mapping[str, object],
) -> RealtimeManagementAssessment:
    if status not in {"READY", "UNAVAILABLE", "EXPIRED", "TARGET", "STOP"}:
        raise ValueError("unsupported management status")
    if status == "READY" and reason_codes:
        raise ValueError("ready management assessment cannot have reasons")
    payload_sha256 = canonical_sha256(payload)
    assessed = _utc(assessed_at, "assessed_at")
    assessment_id = uuid5(
        NAMESPACE_URL,
        f"realtime-management:{cohort_id}:{alert_id}:{assessed.isoformat()}:{payload_sha256}",
    )
    return RealtimeManagementAssessment(
        assessment_id=assessment_id,
        cohort_id=cohort_id,
        alert_id=alert_id,
        assessed_at=assessed,
        status=status,
        reason_codes=reason_codes,
        payload_sha256=payload_sha256,
        payload=payload,
    )