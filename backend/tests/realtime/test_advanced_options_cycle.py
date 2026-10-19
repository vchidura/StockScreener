from dataclasses import replace
from datetime import datetime, timezone
from uuid import UUID
from unittest.mock import MagicMock

import pytest

from advanced_options_reference_support import reference_state
from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_input import (
    EQUITY_WINDOW,
    NOW,
    WINDOW,
    _closure,
)
from test_option_portal_adapter import _package, _policy
from realtime.advanced_options_cycle import (
    O2_MODEL_NOT_ACCEPTED,
    AdvancedOptionsDatasetFlowIdentity,
    AdvancedOptionsNativeEvaluation,
    AdvancedOptionsNativeLaneResult,
    CycleLaneState,
    NativeLaneState,
    persist_advanced_options_cycle_cursor,
    run_advanced_options_cycle,
)
from realtime.option_source_flow import build_option_source_flow_identity
from realtime.advanced_options_input import (
    build_advanced_options_input_gate_plan,
    reconstruct_options_input_ready_receipt,
)
from realtime.advanced_options_reference_state import (
    InMemoryAdvancedOptionsReferenceStateRepository,
)
from realtime.materialization_jobs import JobBarrierVerdict
from realtime.orchestration_repository import OPTIONS_CYCLE
from realtime.preflight import DetectorFamily


UTC = timezone.utc
EVENT = UUID("56565656-7878-9090-abab-cdcdcdcdcdcd")


def _gate_and_receipt():
    gate = build_advanced_options_input_gate_plan(
        _closure(),
        window_id=WINDOW,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        build_id="p4-build",
        available_at=NOW,
    )
    members = tuple(row.member_key for row in gate.barrier_members)
    receipt = reconstruct_options_input_ready_receipt(
        gate,
        JobBarrierVerdict(
            gate.barrier_id,
            "COMPLETE",
            len(members),
            members,
            (),
            (),
        ),
        cutoff=NOW,
        reference_generation=7,
        build_id="p4-build",
    )
    return gate, receipt


def _complete(*evaluations):
    return AdvancedOptionsNativeLaneResult(
        NativeLaneState.COMPLETE,
        (),
        tuple(evaluations),
    )


def _reference_repository():
    repository = InMemoryAdvancedOptionsReferenceStateRepository()
    repository.put(reference_state())
    return repository


def test_cycle_records_every_lane_and_keeps_o2_explicitly_unavailable():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    package = replace(
        _package(_policy().policy_sha256),
        cohort_id=control.cohort_id,
    )
    selected = AdvancedOptionsNativeEvaluation(
        EVENT,
        "SPY",
        "SELECTED",
        NOW,
        {"native": "O1", "score": "1.0"},
        package,
    )
    observed = AdvancedOptionsNativeEvaluation(
        UUID("11111111-aaaa-bbbb-cccc-222222222222"),
        "SPY",
        "OBSERVATION",
        NOW,
        {"native": "S2", "value": 0},
    )
    o2_calls = []
    reference_repository = _reference_repository()
    executors = {
        DetectorFamily.O1: lambda _context: _complete(selected),
        DetectorFamily.O2: lambda _context: (
            o2_calls.append(True) or _complete()
        ),
        DetectorFamily.O3: lambda _context: _complete(),
        DetectorFamily.S1: lambda _context: (
            AdvancedOptionsNativeLaneResult(
                NativeLaneState.BLOCKED,
                ("PACKAGE_PREREQUISITE_MISSING",),
            )
        ),
        DetectorFamily.S2: lambda _context: _complete(observed),
    }

    first = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=reference_repository,
        build_id="p4-build",
    )
    repeated = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=reference_repository,
        build_id="p4-build",
    )

    assert first == repeated
    assert not o2_calls
    assert first.dataset_status == "DEGRADED"
    assert first.receipt.receipt_kind == OPTIONS_CYCLE
    assert not first.receipt.advance_release
    outcomes = {row.detector: row for row in first.outcomes}
    assert set(outcomes) == set(control.manifest_policy.required_detectors)
    assert outcomes[DetectorFamily.O1].state is (
        CycleLaneState.COMPLETE_SELECTED
    )
    assert outcomes[DetectorFamily.O3].state is (
        CycleLaneState.COMPLETE_ZERO
    )
    assert outcomes[DetectorFamily.S1].state is CycleLaneState.BLOCKED
    assert outcomes[DetectorFamily.O2].state is CycleLaneState.UNAVAILABLE
    assert outcomes[DetectorFamily.O2].reason_codes == (
        O2_MODEL_NOT_ACCEPTED,
    )


def test_missing_executors_are_blocked_not_missing_cycle_rows():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    cycle = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors={},
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p4-build",
    )

    assert len(cycle.outcomes) == len(
        control.manifest_policy.required_detectors
    )
    assert all(
        row.state in {CycleLaneState.BLOCKED, CycleLaneState.UNAVAILABLE}
        for row in cycle.outcomes
    )
    assert cycle.evaluations == ()


def test_cycle_persistence_uses_generic_datasets_and_evaluations_only():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    observation = AdvancedOptionsNativeEvaluation(
        EVENT,
        "SPY",
        "OBSERVATION",
        NOW,
        {"native": "O1", "value": 0},
    )
    executors = {
        detector: (lambda _context: _complete())
        for detector in control.manifest_policy.required_detectors
        if detector is not DetectorFamily.O2
    }
    executors[DetectorFamily.O1] = lambda _context: _complete(observation)
    cycle = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p4-build",
    )
    cursor = MagicMock()

    persist_advanced_options_cycle_cursor(
        cursor,
        cycle,
        control_plane=control,
    )

    statements = tuple(call.args[0] for call in cursor.execute.call_args_list)
    assert any("INSERT INTO rt.detector_datasets" in row for row in statements)
    assert any(
        "INSERT INTO rt.detector_evaluations" in row for row in statements
    )
    assert all("alert_current" not in row for row in statements)
    assert all("alert_outbox" not in row for row in statements)
    assert all("release_current" not in row for row in statements)


def test_cycle_dataset_persists_exact_flow_identity_when_bound():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    cycle = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors={},
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p4-build",
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=UUID(
            "12345678-1234-5678-1234-567812345678"
        ),
        stream_session_ids=(
            UUID("87654321-4321-8765-4321-876543218765"),
        ),
        condition_semantics_sha256="c" * 64,
        flow_policy_sha256="d" * 64,
        advanced_flow_policy_sha256="a" * 64,
        contract_scope_sha256="b" * 64,
    )
    identity = AdvancedOptionsDatasetFlowIdentity(
        source_flow,
        "a" * 64,
        "b" * 64,
        gate.closure.manifest_sha256,
        control.slot_set_sha256,
        "e" * 64,
    )
    cursor = MagicMock()

    persist_advanced_options_cycle_cursor(
        cursor,
        cycle,
        control_plane=control,
        flow_identity=identity,
    )

    dataset_call = next(
        call
        for call in cursor.execute.call_args_list
        if "INSERT INTO rt.detector_datasets" in call.args[0]
    )
    values = dataset_call.args[1]
    assert values[11:] == (
        2,
        str(source_flow.source_flow_id),
        "a" * 64,
        "b" * 64,
        control.slot_set_sha256,
        "e" * 64,
    )


def test_cycle_rejects_input_receipt_drift_and_invalid_selected_result():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    with pytest.raises(ValueError, match="input receipt changed"):
        run_advanced_options_cycle(
            control_plane=control,
            input_gate=gate,
            input_receipt=replace(receipt, input_sha256="f" * 64),
            executors={},
            cutoff=NOW,
            reference_generation=7,
            reference_repository=_reference_repository(),
            build_id="p4-build",
        )
    with pytest.raises(ValueError, match="input receipt changed"):
        run_advanced_options_cycle(
            control_plane=control,
            input_gate=gate,
            input_receipt=receipt,
            executors={},
            cutoff=NOW,
            reference_generation=8,
            reference_repository=_reference_repository(),
            build_id="p4-build",
        )
    with pytest.raises(ValueError, match="requires a package"):
        AdvancedOptionsNativeEvaluation(
            EVENT,
            "SPY",
            "SELECTED",
            datetime(2026, 10, 16, 14, 30, tzinfo=UTC),
            {"native": "O1"},
        )
