from dataclasses import replace
from datetime import timedelta

from realtime.advanced_options_native_adapters import (
    AdvancedO1Invocation,
    AdvancedO3Invocation,
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
    advanced_o3_invocation_document,
    build_advanced_o1_executor,
    build_advanced_o3_executor,
    decode_advanced_o3_invocation_document,
)
from realtime.preflight import DetectorFamily
from test_advanced_options_cycle import _gate_and_receipt
from test_advanced_options_control_plane import _plan as _control_plane
from test_developer_cycle_compute import (
    _confirmation_inputs,
    _credit_fixture,
)
from test_option_portal_adapter import _package, _policy
from realtime.advanced_options_cycle import AdvancedOptionsDetectorContext


def test_native_bundle_has_exact_accepted_executor_membership():
    bundle = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(),
    )

    assert set(bundle.executors()) == {
        DetectorFamily.O1,
        DetectorFamily.O3,
    }


def test_empty_complete_inputs_produce_complete_zero_for_every_lane():
    gate, receipt = _gate_and_receipt()
    bundle = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(),
    )

    for detector, executor in bundle.executors().items():
        result = executor(AdvancedOptionsDetectorContext(
            gate,
            receipt,
            detector,
            receipt.cutoff,
        ))
        assert result.state.value == "COMPLETE"
        assert result.evaluations == ()


def test_missing_canonical_inputs_are_blocked_not_complete_zero():
    gate, receipt = _gate_and_receipt()
    bundle = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(
            blocked_reason_codes=("O1_INPUT_UNAVAILABLE",),
        ),
        o3=AdvancedOptionsLaneInputs(
            blocked_reason_codes=("O3_INPUT_UNAVAILABLE",),
        ),
    )

    for detector, executor in bundle.executors().items():
        result = executor(AdvancedOptionsDetectorContext(
            gate,
            receipt,
            detector,
            receipt.cutoff,
        ))
        assert result.state.value == "BLOCKED"
        assert result.reason_codes == (
            f"{detector.value}_INPUT_UNAVAILABLE",
        )


def test_o1_adapter_retains_real_stable_detector_output():
    gate, receipt = _gate_and_receipt()
    _, source = _confirmation_inputs()
    invocation = AdvancedO1Invocation(source.snapshot_id, source)
    result = build_advanced_o1_executor(
        AdvancedOptionsLaneInputs((invocation,))
    )(AdvancedOptionsDetectorContext(
        gate,
        receipt,
        DetectorFamily.O1,
        receipt.cutoff,
    ))

    assert result.state.value == "COMPLETE"
    assert len(result.evaluations) == 1
    assert result.evaluations[0].payload["detector_id"] == "O1"
    assert result.evaluations[0].package is None


def test_o3_adapter_maps_real_qualified_credit_to_selected_package():
    gate, receipt = _gate_and_receipt()
    control = _control_plane()
    candidate, decision, snapshots, invalidation = _credit_fixture()
    package = replace(
        _package(_policy().policy_sha256),
        cohort_id=control.cohort_id,
    )
    scheduled_cycle = decision.decision_at - timedelta(seconds=1)
    invocation = AdvancedO3Invocation(
        decision.activity.episode_id,
        candidate,
        decision,
        candidate.legs[0].contract_id,
        snapshots,
        invalidation,
        scheduled_cycle,
        package,
    )
    result = build_advanced_o3_executor(
        AdvancedOptionsLaneInputs((invocation,))
    )(AdvancedOptionsDetectorContext(
        gate,
        receipt,
        DetectorFamily.O3,
        scheduled_cycle,
    ))

    assert result.state.value == "COMPLETE"
    assert len(result.evaluations) == 1
    assert result.evaluations[0].selection_status == "SELECTED"
    assert result.evaluations[0].payload["schema_version"] == (
        "option_o3_credit_observation_v1"
    )
    assert result.evaluations[0].package == package


def test_o3_invocation_document_round_trip_is_exact():
    control = _control_plane()
    candidate, decision, snapshots, invalidation = _credit_fixture()
    package = replace(
        _package(_policy().policy_sha256),
        candidate_id=candidate.candidate_id,
        matrix_id=candidate.matrix_id,
        candidate_identity_sha256=candidate.identity_sha256,
        cohort_id=control.cohort_id,
    )
    invocation = AdvancedO3Invocation(
        decision.activity.episode_id,
        candidate,
        decision,
        candidate.legs[0].contract_id,
        snapshots,
        invalidation,
        decision.decision_at - timedelta(seconds=1),
        package,
    )

    document = advanced_o3_invocation_document(invocation)
    decoded = decode_advanced_o3_invocation_document(
        document,
        package=package,
    )

    assert advanced_o3_invocation_document(decoded) == document
    assert decoded.candidate == candidate
    assert decoded.package == package
