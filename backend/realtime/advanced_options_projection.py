"""P4C stored Advanced Options projections and inert publisher handoff."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_control_plane import (
    CANONICAL_PUBLISHER_OWNER,
    AdvancedOptionsControlPlanePlan,
)
from .advanced_options_cycle import AdvancedOptionsCycleResult
from .contracts import canonical_sha256
from .orchestration_repository import (
    OPTIONS_CYCLE,
    OPTIONS_PORTAL_READY,
    CycleReceiptSpec,
    build_options_receipt_spec,
)
from .portal_projection import (
    RealtimePortalSnapshot,
    build_portal_snapshot,
)
from .portal_projection_repository import (
    PortalReleaseReceipt,
    persist_portal_snapshot_cursor,
)


OPTIONS_PROJECTION_TYPES = (
    "OPTIONS_FLOW",
    "OPTIONS_SCREENER",
    "OPTIONS_ALERTS",
    "OPTIONS_MARKS",
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProjectionBatch:
    cycle: AdvancedOptionsCycleResult
    snapshots: tuple[RealtimePortalSnapshot, ...]
    batch_sha256: str
    portal_ready_receipt: CycleReceiptSpec
    publication_authorized: bool = False


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPublisherHandoff:
    owner_name: str
    receipt: PortalReleaseReceipt
    snapshots: tuple[RealtimePortalSnapshot, ...]
    batch_sha256: str
    dispatch_authorized: bool = False
    release_authorized: bool = False
    pointer_authorized: bool = False


def build_advanced_options_projection_batch(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    cycle: AdvancedOptionsCycleResult,
    generated_at: datetime,
    build_id: str,
) -> AdvancedOptionsProjectionBatch:
    generated = _utc(generated_at, "generated_at")
    if (
        cycle.input_gate.closure.control_plane_plan_id
        != control_plane.plan_id
        or cycle.receipt.receipt_kind != OPTIONS_CYCLE
        or cycle.receipt.cutoff != generated
    ):
        raise ValueError("Advanced Options projection cycle scope changed")
    lanes = [
        {
            "detector": row.detector.value,
            "state": row.state.value,
            "reason_codes": list(row.reason_codes),
            "outcome_sha256": row.outcome_sha256,
        }
        for row in cycle.outcomes
    ]
    evaluations = [
        {
            "evaluation_id": str(row.evaluation_id),
            "event_id": str(row.event_id),
            "detector": row.detector.value,
            "underlyer": row.underlyer,
            "selection_status": row.selection_status,
            "observed_at": row.observed_at,
            "payload_sha256": row.payload_sha256,
            "payload": row.payload,
            "candidate_id": (
                str(row.package.candidate_id)
                if row.package is not None
                else None
            ),
        }
        for row in cycle.evaluations
    ]
    manifest = {
        "contract": "advanced-options-projection-manifest-v1",
        "cohort_id": str(control_plane.cohort_id),
        "manifest_sha256": control_plane.manifest_sha256,
        "cycle_sha256": cycle.cycle_sha256,
        "input_sha256": cycle.input_gate.closure_sha256,
        "valuation_policy_sha256": (
            control_plane.pins.valuation_policy_sha256
        ),
        "model_bundle_sha256": control_plane.pins.model_bundle_sha256,
        "build_id": build_id,
    }
    payloads: Mapping[str, Mapping[str, object]] = {
        "OPTIONS_FLOW": {
            "status": cycle.dataset_status,
            "lanes": lanes,
            "evaluations": [
                row for row in evaluations
                if row["detector"] in {
                    "AGGRESSIVE_PREMIUM_BURST",
                    "PERSISTENT_REALTIME_FLOW",
                    "SWEEP_LIKE_CLUSTER",
                }
            ],
        },
        "OPTIONS_SCREENER": {
            "status": cycle.dataset_status,
            "lanes": lanes,
            "evaluations": evaluations,
        },
        "OPTIONS_ALERTS": {
            "status": cycle.dataset_status,
            "lanes": lanes,
            "evaluations": [
                row for row in evaluations
                if row["selection_status"] == "SELECTED"
            ],
            "production_authorized": False,
            "delivery_authorized": False,
        },
        "OPTIONS_MARKS": {
            "status": cycle.dataset_status,
            "candidate_ids": sorted({
                row["candidate_id"]
                for row in evaluations
                if row["candidate_id"] is not None
            }),
            "valuation_policy_sha256": (
                control_plane.pins.valuation_policy_sha256
            ),
        },
    }
    snapshots = tuple(
        build_portal_snapshot(
            cohort_id=control_plane.cohort_id,
            provider_tier=control_plane.binding.provider_tier,
            snapshot_type=snapshot_type,
            snapshot_key="CURRENT",
            source_generation=cycle.receipt.reference_generation,
            source_manifest={**manifest, "snapshot_type": snapshot_type},
            payload=payloads[snapshot_type],
            generated_at=generated,
        )
        for snapshot_type in OPTIONS_PROJECTION_TYPES
    )
    batch_sha256 = canonical_sha256({
        "contract": "advanced-options-projection-batch-v1",
        "cycle_sha256": cycle.cycle_sha256,
        "snapshot_ids": tuple(row.snapshot_id for row in snapshots),
        "publication_authorized": False,
    })
    portal_ready = build_options_receipt_spec(
        receipt_kind=OPTIONS_PORTAL_READY,
        cutoff=generated,
        reference_generation=cycle.receipt.reference_generation,
        universe_sha256=control_plane.universe.sha256,
        input_sha256=batch_sha256,
        build_id=build_id,
    )
    return AdvancedOptionsProjectionBatch(
        cycle,
        snapshots,
        batch_sha256,
        portal_ready,
    )


def persist_advanced_options_projection_cursor(
    cursor,
    batch: AdvancedOptionsProjectionBatch,
    *,
    snapshot_repository=persist_portal_snapshot_cursor,
) -> None:
    if batch.publication_authorized:
        raise ValueError("P4C projection cannot authorize publication")
    for snapshot in batch.snapshots:
        snapshot_repository(cursor, snapshot)


def build_advanced_options_publisher_handoff(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    batch: AdvancedOptionsProjectionBatch,
) -> AdvancedOptionsPublisherHandoff:
    receipt_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-options-portal-ready:"
        f"{control_plane.cohort_id}:{batch.batch_sha256}",
    )
    return AdvancedOptionsPublisherHandoff(
        owner_name=CANONICAL_PUBLISHER_OWNER,
        receipt=PortalReleaseReceipt(
            receipt_id,
            control_plane.cohort_id,
            control_plane.binding.provider_tier,
            batch.portal_ready_receipt.cutoff,
            control_plane.universe.sha256,
        ),
        snapshots=batch.snapshots,
        batch_sha256=batch.batch_sha256,
    )


def read_advanced_options_projection_cursor(
    cursor,
    *,
    cohort_id: UUID,
    snapshot_type: str,
    available_by: datetime,
) -> dict[str, object]:
    if snapshot_type not in OPTIONS_PROJECTION_TYPES:
        raise ValueError("Advanced Options projection type is invalid")
    checked = _utc(available_by, "available_by")
    cursor.execute(
        "SELECT catalog.snapshot_id,catalog.generated_at,catalog.payload_sha256,"
        "hot.payload FROM rt.snapshot_catalog AS catalog "
        "JOIN rt.portal_snapshot_hot AS hot "
        "ON hot.snapshot_id=catalog.snapshot_id "
        "AND hot.generated_at=catalog.generated_at "
        "WHERE catalog.cohort_id=%s AND catalog.snapshot_type=%s "
        "AND catalog.snapshot_key='CURRENT' "
        "AND catalog.generated_at<=%s "
        "ORDER BY catalog.generated_at DESC,catalog.snapshot_id DESC LIMIT 1",
        (str(cohort_id), snapshot_type, checked),
    )
    row = cursor.fetchone()
    if row is None:
        return {
            "available": False,
            "reason": "STORED_OPTIONS_PROJECTION_UNAVAILABLE",
            "snapshot_type": snapshot_type,
            "provider_fetch_performed": False,
            "detector_work_performed": False,
            "publication_performed": False,
            "data": None,
        }
    payload = row[3]
    if canonical_sha256(payload) != str(row[2]):
        raise ValueError("stored Advanced Options projection hash mismatch")
    return {
        "available": True,
        "reason": None,
        "snapshot_type": snapshot_type,
        "snapshot_id": str(row[0]),
        "generated_at": row[1],
        "provider_fetch_performed": False,
        "detector_work_performed": False,
        "publication_performed": False,
        "data": payload,
    }
