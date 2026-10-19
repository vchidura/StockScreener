from unittest.mock import MagicMock

import pytest

from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_cycle import (
    NOW,
    _complete,
    _gate_and_receipt,
    _reference_repository,
)
from realtime.advanced_options_cycle import run_advanced_options_cycle
from realtime.advanced_options_projection import (
    OPTIONS_PROJECTION_TYPES,
    build_advanced_options_projection_batch,
    build_advanced_options_publisher_handoff,
    persist_advanced_options_projection_cursor,
    read_advanced_options_projection_cursor,
)
from realtime.contracts import canonical_sha256
from realtime.orchestration_repository import OPTIONS_PORTAL_READY
from realtime.preflight import DetectorFamily


def _cycle():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    executors = {
        detector: (lambda _context: _complete())
        for detector in control.manifest_policy.required_detectors
        if detector is not DetectorFamily.O2
    }
    return control, run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p4-build",
    )


def test_projection_batch_is_deterministic_complete_and_nonpublishing():
    control, cycle = _cycle()
    first = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    repeated = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )

    assert first == repeated
    assert tuple(row.snapshot_type for row in first.snapshots) == (
        OPTIONS_PROJECTION_TYPES
    )
    assert first.portal_ready_receipt.receipt_kind == OPTIONS_PORTAL_READY
    assert not first.portal_ready_receipt.advance_release
    assert not first.publication_authorized
    alerts = next(
        row for row in first.snapshots
        if row.snapshot_type == "OPTIONS_ALERTS"
    )
    assert alerts.payload["production_authorized"] is False
    assert alerts.payload["delivery_authorized"] is False


def test_projection_persistence_uses_snapshot_owner_and_no_release_owner():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    cursor = MagicMock()
    stored = []

    persist_advanced_options_projection_cursor(
        cursor,
        batch,
        snapshot_repository=lambda active_cursor, snapshot: stored.append(
            (active_cursor, snapshot)
        ),
    )

    assert [row[1] for row in stored] == list(batch.snapshots)
    assert all(row[0] is cursor for row in stored)
    assert cursor.execute.call_count == 0


def test_publisher_handoff_is_canonical_and_inert():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    handoff = build_advanced_options_publisher_handoff(
        control_plane=control,
        batch=batch,
    )

    assert handoff.owner_name == "portal-publisher"
    assert handoff.snapshots == batch.snapshots
    assert not handoff.dispatch_authorized
    assert not handoff.release_authorized
    assert not handoff.pointer_authorized


def test_stored_reader_performs_no_provider_detector_or_publication_work():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    snapshot = batch.snapshots[0]
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        snapshot.snapshot_id,
        snapshot.generated_at,
        snapshot.payload_sha256,
        snapshot.payload,
    )

    result = read_advanced_options_projection_cursor(
        cursor,
        cohort_id=control.cohort_id,
        snapshot_type=snapshot.snapshot_type,
        available_by=NOW,
    )

    assert result["available"]
    assert result["data"] == snapshot.payload
    assert result["provider_fetch_performed"] is False
    assert result["detector_work_performed"] is False
    assert result["publication_performed"] is False
    assert cursor.execute.call_count == 1
    assert "snapshot_catalog" in cursor.execute.call_args.args[0]


def test_stored_reader_rejects_hash_drift_and_missing_is_explicit():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    snapshot = batch.snapshots[0]
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        snapshot.snapshot_id,
        snapshot.generated_at,
        "f" * 64,
        snapshot.payload,
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        read_advanced_options_projection_cursor(
            cursor,
            cohort_id=control.cohort_id,
            snapshot_type=snapshot.snapshot_type,
            available_by=NOW,
        )
    cursor.fetchone.return_value = None
    missing = read_advanced_options_projection_cursor(
        cursor,
        cohort_id=control.cohort_id,
        snapshot_type=snapshot.snapshot_type,
        available_by=NOW,
    )
    assert missing["available"] is False
    assert missing["provider_fetch_performed"] is False


def test_projection_rejects_cycle_clock_drift():
    control, cycle = _cycle()
    with pytest.raises(ValueError, match="cycle scope changed"):
        build_advanced_options_projection_batch(
            control_plane=control,
            cycle=cycle,
            generated_at=NOW.replace(minute=31),
            build_id="p4-build",
        )
