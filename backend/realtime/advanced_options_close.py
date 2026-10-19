"""P4D Advanced Options close, archive dependencies and retention handoff."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_journal_registry import AdvancedJournalRegistration
from .advanced_options_control_plane import AdvancedOptionsControlPlanePlan
from .advanced_options_input import AdvancedOptionsInputGatePlan
from .advanced_options_projection import AdvancedOptionsProjectionBatch
from .contracts import canonical_sha256
from .retention_maintenance import RetentionMaintenanceRequest
from .session_archive import (
    DETECTOR_EVALUATIONS,
    OPTION_ALERTS,
    OPTION_FLOW_CLUSTERS,
    OPTION_FLOW_EVENTS,
    PIN_OPEN_HORIZON,
    PIN_OUTCOME_UNVERIFIED,
    ArchiveIdentity,
    Pin,
)


ADVANCED_OPTIONS_ARCHIVE_KINDS = (
    OPTION_ALERTS,
    DETECTOR_EVALUATIONS,
    OPTION_FLOW_EVENTS,
    OPTION_FLOW_CLUSTERS,
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCloseCoverage:
    coverage_id: UUID
    coverage_sha256: str
    status: str
    reason_codes: tuple[str, ...]
    journal_generation: UUID
    journal_status: str
    raw_committed_offset: int
    normalized_committed_offset: int
    flow_checkpoint_offset: int
    market_state_checkpoint_offset: int
    input_closure_sha256: str
    cycle_sha256: str
    projection_batch_sha256: str
    slot_coverage_sha256: str
    oi_coverage_sha256: str
    checked_at: datetime


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRetentionHandoff:
    close_coverage: AdvancedOptionsCloseCoverage
    archive_identity: ArchiveIdentity
    required_archive_kinds: tuple[str, ...]
    pins: tuple[Pin, ...]
    request: RetentionMaintenanceRequest
    raw_journal_removal_eligible: bool
    archive_execution_authorized: bool = False
    retention_execution_authorized: bool = False
    journal_deletion_authorized: bool = False


def build_advanced_options_close_coverage(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    input_gate: AdvancedOptionsInputGatePlan,
    projection: AdvancedOptionsProjectionBatch,
    journal: AdvancedJournalRegistration,
    flow_checkpoint_offset: int,
    market_state_checkpoint_offset: int,
    unresolved_gap_ids: tuple[UUID, ...],
    slot_coverage_sha256: str,
    oi_coverage_sha256: str,
    checked_at: datetime,
) -> AdvancedOptionsCloseCoverage:
    checked = _utc(checked_at, "checked_at")
    for value, name in (
        (slot_coverage_sha256, "slot_coverage_sha256"),
        (oi_coverage_sha256, "oi_coverage_sha256"),
    ):
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError(f"{name} must be a SHA-256 digest")
    if (
        input_gate.closure.control_plane_plan_id != control_plane.plan_id
        or projection.cycle.input_gate.closure_sha256
        != input_gate.closure_sha256
        or journal.plan_id != control_plane.plan_id
        or journal.cohort_id != control_plane.cohort_id
        or journal.manifest_id != control_plane.manifest_id
        or journal.journal_generation
        != control_plane.source_flow.journal_generation
    ):
        raise ValueError("Advanced Options close scope changed")
    if any(
        type(value) is not int or value < 0
        for value in (
            flow_checkpoint_offset,
            market_state_checkpoint_offset,
        )
    ):
        raise ValueError("Advanced Options close checkpoint is invalid")
    gaps = tuple(sorted(set(unresolved_gap_ids), key=str))
    reasons = []
    if journal.status != "DRAINED":
        reasons.append("JOURNAL_NOT_DRAINED")
    if journal.sealed_files_sha256 is None:
        reasons.append("JOURNAL_CHECKSUM_UNAVAILABLE")
    if flow_checkpoint_offset < journal.raw_committed_offset:
        reasons.append("FLOW_CHECKPOINT_INCOMPLETE")
    if market_state_checkpoint_offset < journal.normalized_committed_offset:
        reasons.append("MARKET_STATE_CHECKPOINT_INCOMPLETE")
    if gaps:
        reasons.append("UNRESOLVED_GAPS")
    if len(projection.cycle.outcomes) != len(
        control_plane.manifest_policy.required_detectors
    ):
        reasons.append("DETECTOR_LANE_OUTCOMES_INCOMPLETE")
    if len(projection.snapshots) != 4:
        reasons.append("OPTIONS_PROJECTION_MEMBERSHIP_INCOMPLETE")
    status = "COMPLETE" if not reasons else "BLOCKED"
    identity = {
        "contract": "advanced-options-close-coverage-v1",
        "control_plane_plan_id": control_plane.plan_id,
        "session_date": control_plane.session_date,
        "journal_generation": journal.journal_generation,
        "journal_status": journal.status,
        "sealed_files_sha256": journal.sealed_files_sha256,
        "raw_committed_offset": journal.raw_committed_offset,
        "normalized_committed_offset": journal.normalized_committed_offset,
        "flow_checkpoint_offset": flow_checkpoint_offset,
        "market_state_checkpoint_offset": market_state_checkpoint_offset,
        "unresolved_gap_ids": gaps,
        "input_closure_sha256": input_gate.closure_sha256,
        "cycle_sha256": projection.cycle.cycle_sha256,
        "projection_batch_sha256": projection.batch_sha256,
        "slot_coverage_sha256": slot_coverage_sha256,
        "oi_coverage_sha256": oi_coverage_sha256,
        "status": status,
        "reason_codes": tuple(reasons),
        "checked_at": checked,
    }
    coverage_sha256 = canonical_sha256(identity)
    return AdvancedOptionsCloseCoverage(
        coverage_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-close:{coverage_sha256}",
        ),
        coverage_sha256=coverage_sha256,
        status=status,
        reason_codes=tuple(reasons),
        journal_generation=journal.journal_generation,
        journal_status=journal.status,
        raw_committed_offset=journal.raw_committed_offset,
        normalized_committed_offset=journal.normalized_committed_offset,
        flow_checkpoint_offset=flow_checkpoint_offset,
        market_state_checkpoint_offset=market_state_checkpoint_offset,
        input_closure_sha256=input_gate.closure_sha256,
        cycle_sha256=projection.cycle.cycle_sha256,
        projection_batch_sha256=projection.batch_sha256,
        slot_coverage_sha256=slot_coverage_sha256,
        oi_coverage_sha256=oi_coverage_sha256,
        checked_at=checked,
    )


def build_advanced_options_retention_handoff(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    close_coverage: AdvancedOptionsCloseCoverage,
    archive_identity: ArchiveIdentity,
    archives_verified: bool,
    open_alert_ids: tuple[UUID, ...],
    journal_retention_expires_at: datetime,
    checked_at: datetime,
) -> AdvancedOptionsRetentionHandoff:
    checked = _utc(checked_at, "checked_at")
    expiry = _utc(
        journal_retention_expires_at,
        "journal_retention_expires_at",
    )
    pins = tuple(
        Pin(
            control_plane.session_date,
            PIN_OPEN_HORIZON,
            str(alert_id),
        )
        for alert_id in sorted(set(open_alert_ids), key=str)
    )
    if not archives_verified:
        pins += (
            Pin(
                control_plane.session_date,
                PIN_OUTCOME_UNVERIFIED,
                close_coverage.coverage_sha256,
            ),
        )
    request = RetentionMaintenanceRequest(
        as_of_date=checked.date(),
        as_of_session=control_plane.session_date,
        archive_session_dates=(control_plane.session_date,),
        archive_kinds=ADVANCED_OPTIONS_ARCHIVE_KINDS,
    )
    eligible = (
        close_coverage.status == "COMPLETE"
        and archives_verified
        and not pins
        and expiry <= checked
    )
    return AdvancedOptionsRetentionHandoff(
        close_coverage=close_coverage,
        archive_identity=archive_identity,
        required_archive_kinds=ADVANCED_OPTIONS_ARCHIVE_KINDS,
        pins=pins,
        request=request,
        raw_journal_removal_eligible=eligible,
    )
