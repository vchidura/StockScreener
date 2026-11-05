from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
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
    AdvancedOptionsAlertsPagePublication,
    AdvancedOptionsCanaryPublicationAuthorization,
    AdvancedOptionsPublicationAuthorization,
    OPTIONS_PAGE_CONTRACTS,
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
    OPTIONS_SNAPSHOT_TYPES,
    ADVANCED_OPTIONS_PAGE_FLOW_EVENT_DETAIL_LIMIT,
    authorize_advanced_options_canary_publication,
    authorize_advanced_options_publication,
    build_advanced_options_projection_batch,
    build_advanced_options_publisher_handoff,
    load_advanced_options_page_release_cursor,
    load_advanced_options_page_source_cursor,
    persist_advanced_options_projection_cursor,
    read_advanced_options_projection_cursor,
)
from realtime.contracts import canonical_sha256
from realtime.option_alert_projection import DetectorEvaluationQuery
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


def test_page_release_loader_uses_the_selected_release_identity():
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = ()
    release_id = uuid4()

    assert load_advanced_options_page_release_cursor(
        cursor,
        cohort_id=UUID(int=2),
        available_by=NOW,
        artifact_kind="FLOW",
        release_id=release_id,
        release_cutoff=NOW,
    ) is None

    statement, parameters = cursor.execute.call_args.args
    assert "portal_release_current" not in statement
    assert "WHERE release.release_id=%s" in statement
    assert parameters[1:4] == (
        str(release_id),
        NOW,
        str(UUID(int=2)),
    )


def test_page_source_uses_complete_aggregates_and_bounded_detail():
    control = _control_plane()
    underlyer = control.universe.option_underlyings[0]
    contract_ticker = (
        f"O:{underlyer}{NOW:%y%m%d}C00500000"
    )
    aggregate = {
        "event_id": uuid4(),
        "event_time": NOW,
        "first_event_time": NOW - timedelta(minutes=5),
        "contract_ticker": contract_ticker,
        "contract_id": 1,
        "underlyer": underlyer,
        "price": Decimal("1.25"),
        "size": 6001,
        "multiplier": 100,
        "premium": Decimal("750125"),
        "prior_open_interest": None,
        "event_count": 6001,
    }
    detail = {
        key: value
        for key, value in aggregate.items()
        if key not in {"first_event_time", "event_count"}
    }
    cursor = MagicMock()
    admitted = tuple(
        (
            member.instrument_id,
            index + 100,
            f"O:{underlyer}{NOW:%y%m%d}C{index + 1:08d}",
            underlyer,
            "C",
            NOW.date() + timedelta(days=30),
            Decimal(index + 1),
            "ELIGIBLE",
            NOW,
        )
        for index, member in enumerate(control.slot_set.members)
    )
    cursor.fetchall.side_effect = (
        (aggregate,),
        (detail,),
        admitted,
    )
    cycle = SimpleNamespace(
        input_gate=SimpleNamespace(
            closure=SimpleNamespace(
                cycle_input=SimpleNamespace(
                    cohort_id=control.cohort_id,
                )
            )
        ),
        evaluations=(),
    )

    source = load_advanced_options_page_source_cursor(
        cursor,
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
    )

    assert source.flow_events[0]["event_count"] == 6001
    assert source.flow_event_detail == (detail,)
    assert source.admitted_contracts[0]["instrument_id"] == (
        control.slot_set.members[0].instrument_id
    )
    aggregate_query = cursor.execute.call_args_list[0].args[0]
    detail_query, detail_parameters = (
        cursor.execute.call_args_list[1].args
    )
    assert "GROUP BY event.contract_ticker" in aggregate_query
    assert "LIMIT" not in aggregate_query
    assert "event.event_time>%s" not in aggregate_query
    assert detail_query.endswith(
        "ORDER BY event.event_time DESC,event.event_id DESC LIMIT %s"
    )
    assert detail_parameters[-1] == (
        ADVANCED_OPTIONS_PAGE_FLOW_EVENT_DETAIL_LIMIT
    )


def test_page_source_recomputes_full_session_for_late_inserts_and_corrections():
    control = _control_plane()
    underlyer = control.universe.option_underlyings[0]
    contract_ticker = f"O:{underlyer}{NOW:%y%m%d}C00500000"
    base = {
        "event_id": uuid4(),
        "event_time": NOW - timedelta(minutes=2),
        "first_event_time": NOW - timedelta(minutes=5),
        "contract_ticker": contract_ticker,
        "contract_id": 1,
        "underlyer": underlyer,
        "price": Decimal("1.25"),
        "size": 20,
        "multiplier": 100,
        "premium": Decimal("2500"),
        "prior_open_interest": None,
        "event_count": 2,
    }
    corrected = {
        **base,
        "event_id": uuid4(),
        "size": 15,
        "premium": Decimal("1875"),
        "event_count": 3,
    }
    detail = {
        key: value
        for key, value in corrected.items()
        if key not in {"first_event_time", "event_count"}
    }
    admitted = tuple(
        {"instrument_id": member.instrument_id}
        for member in control.slot_set.members
    )
    cycle = SimpleNamespace(
        input_gate=SimpleNamespace(
            closure=SimpleNamespace(
                cycle_input=SimpleNamespace(
                    cohort_id=control.cohort_id,
                )
            )
        ),
        evaluations=(),
    )

    def load(aggregate):
        cursor = MagicMock()
        cursor.fetchall.side_effect = (
            (aggregate,),
            (detail,),
            admitted,
        )
        source = load_advanced_options_page_source_cursor(
            cursor,
            control_plane=control,
            cycle=cycle,
            generated_at=NOW,
        )
        return source, cursor

    original, _ = load(base)
    recomputed, cursor = load(corrected)

    assert original.flow_events[0]["size"] == 20
    assert recomputed.flow_events[0]["size"] == 15
    assert recomputed.flow_events[0]["event_count"] == 3
    aggregate_query = cursor.execute.call_args_list[0].args[0]
    assert "event.event_time>%s" not in aggregate_query
    assert "portal_release_current" not in aggregate_query


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


def test_alert_page_reports_only_actual_cycle_lanes_and_acceptance():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    snapshot = next(
        row
        for row in batch.snapshots
        if row.snapshot_type == OPTIONS_PAGE_SNAPSHOT_TYPES["ALERTS"]
    )
    publication = AdvancedOptionsAlertsPagePublication(snapshot.payload)
    identity = {
        "cohort_id": control.cohort_id,
        "provider_tier": control.binding.provider_tier,
        "scope_sha256": snapshot.payload["scope_sha256"],
    }

    result = publication.evaluations(
        DetectorEvaluationQuery(),
        **identity,
    )
    models = {
        row["detector_id"]: row
        for row in result["data"]["models"]
    }

    assert set(models) == {"O1", "O2", "O3", "OS1", "OS2"}
    assert models["O1"]["accepted"]
    assert models["O1"]["lane_state"] == "COMPLETE_ZERO"
    assert models["O3"]["accepted"]
    assert models["O3"]["lane_state"] == "COMPLETE_ZERO"
    assert models["OS1"]["accepted"]
    assert models["OS1"]["lane_state"] == "COMPLETE_ZERO"
    assert models["OS2"]["accepted"]
    assert models["OS2"]["lane_state"] == "COMPLETE_ZERO"
    assert not models["O2"]["accepted"]
    assert models["O2"]["lane_state"] == "UNAVAILABLE"
    assert models["O2"]["reason_codes"] == ["O2_MODEL_NOT_ACCEPTED"]
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
