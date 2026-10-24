from unittest.mock import MagicMock
from uuid import UUID, uuid4

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
    AdvancedOptionsCanaryPublicationAuthorization,
    AdvancedOptionsPublicationAuthorization,
    OPTIONS_PAGE_CONTRACTS,
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
    OPTIONS_SNAPSHOT_TYPES,
    authorize_advanced_options_canary_publication,
    authorize_advanced_options_publication,
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
        OPTIONS_SNAPSHOT_TYPES
    )
    pages = {
        row.payload["artifact_kind"]: row
        for row in first.snapshots
        if row.snapshot_type in OPTIONS_PAGE_SNAPSHOT_TYPES.values()
    }
    assert set(pages) == set(OPTIONS_PAGE_SNAPSHOT_TYPES)
    assert {
        kind: row.payload["contract"] for kind, row in pages.items()
    } == OPTIONS_PAGE_CONTRACTS
    assert len({
        row.payload["scope_sha256"] for row in pages.values()
    }) == 1
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


def test_owned_disposable_canary_authorizes_only_release_and_pointer():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    inert = build_advanced_options_publisher_handoff(
        control_plane=control,
        batch=batch,
    )
    authorization = AdvancedOptionsCanaryPublicationAuthorization(
        database_name="test_realtime_v2_p8_exact",
        cohort_id=control.cohort_id,
        batch_sha256=batch.batch_sha256,
        receipt_id=UUID(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ),
    )

    canary = authorize_advanced_options_canary_publication(
        inert,
        authorization,
    )

    assert canary.owner_name == "portal-publisher"
    assert canary.release_authorized
    assert canary.pointer_authorized
    assert not canary.dispatch_authorized
    with pytest.raises(ValueError, match="authority is invalid"):
        AdvancedOptionsCanaryPublicationAuthorization(
            database_name="stocks_realtime_v1",
            cohort_id=control.cohort_id,
            batch_sha256=batch.batch_sha256,
            receipt_id=UUID(
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            ),
        )


def test_v5_publication_authority_is_exact_and_keeps_forbidden_effects_false():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="v5-build",
    )
    inert = build_advanced_options_publisher_handoff(
        control_plane=control,
        batch=batch,
    )
    authority = AdvancedOptionsPublicationAuthorization(
        uuid4(),
        control.cohort_id,
        control.binding.provider_tier,
        batch.batch_sha256,
        uuid4(),
        "OWNED_DISPOSABLE_V5_PUBLICATION",
    )

    authorized = authorize_advanced_options_publication(
        inert,
        authority,
    )

    assert authorized.release_authorized
    assert authorized.pointer_authorized
    assert not authorized.dispatch_authorized
    assert authorized.receipt.receipt_id == authority.receipt_id
    with pytest.raises(ValueError, match="authority scope changed"):
        authorize_advanced_options_publication(
            inert,
            AdvancedOptionsPublicationAuthorization(
                uuid4(),
                control.cohort_id,
                control.binding.provider_tier,
                "f" * 64,
                uuid4(),
                "OWNED_DISPOSABLE_V5_PUBLICATION",
            ),
        )


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
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        snapshot.generated_at,
        "e" * 64,
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
    assert result["release_id"] == (
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    )
    assert cursor.execute.call_count == 1
    assert "portal_release_current" in cursor.execute.call_args.args[0]


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
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        snapshot.generated_at,
        "e" * 64,
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
