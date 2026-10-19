from dataclasses import replace
from datetime import timedelta
from uuid import UUID

from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_projection import NOW, _cycle
from realtime.advanced_journal_registry import (
    build_advanced_journal_registration,
)
from realtime.advanced_options_close import (
    ADVANCED_OPTIONS_ARCHIVE_KINDS,
    build_advanced_options_close_coverage,
    build_advanced_options_retention_handoff,
)
from realtime.advanced_options_projection import (
    build_advanced_options_projection_batch,
)
from realtime.session_archive import (
    PIN_OPEN_HORIZON,
    PIN_OUTCOME_UNVERIFIED,
    ArchiveIdentity,
)


ALERT = UUID("99999999-8888-7777-6666-555555555555")
GAP = UUID("44444444-3333-2222-1111-000000000000")


def _projection():
    control, cycle = _cycle()
    return control, build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )


def _journal(tmp_path, control):
    root = tmp_path / "mounted"
    root.mkdir(exist_ok=True)
    registration = build_advanced_journal_registration(
        root=root,
        path=root / "options.sqlite",
        journal_generation=control.source_flow.journal_generation,
        plan_id=control.plan_id,
        stream_session_id=control.source_flow.stream_session_ids[0],
        cohort_id=control.cohort_id,
        manifest_id=control.manifest_id,
        session_date=control.session_date,
        owner_epoch=1,
        normalizer_version="massive_market_event_v2",
        shard_ids=(0,),
        shard_map_sha256="a" * 64,
    )
    return replace(
        registration,
        status="DRAINED",
        raw_committed_offset=10,
        normalized_committed_offset=20,
        bridge_committed_offset=20,
        sealed_files_sha256="b" * 64,
    )


def _coverage(tmp_path, **changes):
    control, projection = _projection()
    values = dict(
        control_plane=control,
        input_gate=projection.cycle.input_gate,
        projection=projection,
        journal=_journal(tmp_path, control),
        flow_checkpoint_offset=10,
        market_state_checkpoint_offset=20,
        unresolved_gap_ids=(),
        slot_coverage_sha256="c" * 64,
        oi_coverage_sha256="d" * 64,
        checked_at=NOW,
    )
    values.update(changes)
    return control, build_advanced_options_close_coverage(**values)


def test_complete_close_binds_all_options_coverage_without_equity_reuse(
    tmp_path,
):
    control, first = _coverage(tmp_path)
    _, repeated = _coverage(tmp_path)

    assert first == repeated
    assert first.status == "COMPLETE"
    assert first.reason_codes == ()
    assert first.journal_generation == control.source_flow.journal_generation
    assert first.input_closure_sha256
    assert first.cycle_sha256
    assert first.projection_batch_sha256
    assert first.slot_coverage_sha256 == "c" * 64
    assert first.oi_coverage_sha256 == "d" * 64


def test_close_is_blocked_by_gap_checkpoint_or_undrained_journal(tmp_path):
    control, projection = _projection()
    journal = _journal(tmp_path, control)
    blocked = build_advanced_options_close_coverage(
        control_plane=control,
        input_gate=projection.cycle.input_gate,
        projection=projection,
        journal=replace(journal, status="DRAINING"),
        flow_checkpoint_offset=9,
        market_state_checkpoint_offset=19,
        unresolved_gap_ids=(GAP,),
        slot_coverage_sha256="c" * 64,
        oi_coverage_sha256="d" * 64,
        checked_at=NOW,
    )

    assert blocked.status == "BLOCKED"
    assert set(blocked.reason_codes) == {
        "JOURNAL_NOT_DRAINED",
        "FLOW_CHECKPOINT_INCOMPLETE",
        "MARKET_STATE_CHECKPOINT_INCOMPLETE",
        "UNRESOLVED_GAPS",
    }


def test_retention_handoff_uses_existing_archive_kinds_and_never_executes(
    tmp_path,
):
    control, coverage = _coverage(tmp_path)
    handoff = build_advanced_options_retention_handoff(
        control_plane=control,
        close_coverage=coverage,
        archive_identity=ArchiveIdentity("p4-build", "options-close-v1"),
        archives_verified=True,
        open_alert_ids=(),
        journal_retention_expires_at=NOW - timedelta(seconds=1),
        checked_at=NOW,
    )

    assert handoff.required_archive_kinds == ADVANCED_OPTIONS_ARCHIVE_KINDS
    assert set(handoff.request.archive_kinds) == set(
        ADVANCED_OPTIONS_ARCHIVE_KINDS
    )
    assert handoff.raw_journal_removal_eligible
    assert not handoff.archive_execution_authorized
    assert not handoff.retention_execution_authorized
    assert not handoff.journal_deletion_authorized


def test_open_alerts_and_unverified_archives_pin_every_dependency(tmp_path):
    control, coverage = _coverage(tmp_path)
    handoff = build_advanced_options_retention_handoff(
        control_plane=control,
        close_coverage=coverage,
        archive_identity=ArchiveIdentity("p4-build", "options-close-v1"),
        archives_verified=False,
        open_alert_ids=(ALERT,),
        journal_retention_expires_at=NOW - timedelta(days=30),
        checked_at=NOW,
    )

    assert not handoff.raw_journal_removal_eligible
    assert {pin.reason for pin in handoff.pins} == {
        PIN_OPEN_HORIZON,
        PIN_OUTCOME_UNVERIFIED,
    }
    assert all(pin.session_date == control.session_date for pin in handoff.pins)


def test_retention_expiry_and_close_status_are_both_required(tmp_path):
    control, coverage = _coverage(tmp_path)
    future = build_advanced_options_retention_handoff(
        control_plane=control,
        close_coverage=coverage,
        archive_identity=ArchiveIdentity("p4-build", "options-close-v1"),
        archives_verified=True,
        open_alert_ids=(),
        journal_retention_expires_at=NOW + timedelta(days=1),
        checked_at=NOW,
    )
    blocked = build_advanced_options_retention_handoff(
        control_plane=control,
        close_coverage=replace(
            coverage,
            status="BLOCKED",
            reason_codes=("UNRESOLVED_GAPS",),
        ),
        archive_identity=ArchiveIdentity("p4-build", "options-close-v1"),
        archives_verified=True,
        open_alert_ids=(),
        journal_retention_expires_at=NOW - timedelta(days=1),
        checked_at=NOW,
    )

    assert not future.raw_journal_removal_eligible
    assert not blocked.raw_journal_removal_eligible
