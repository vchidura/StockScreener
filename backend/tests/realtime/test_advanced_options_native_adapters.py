from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid4, uuid5

from realtime.advanced_options_native_adapters import (
    AdvancedO1Invocation,
    AdvancedO3Invocation,
    AdvancedOSInvocation,
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
    advanced_o3_invocation_document,
    build_advanced_o1_executor,
    build_advanced_o3_executor,
    build_advanced_os_executor,
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
from options.stock_bar_detection import (
    ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY_SHA256,
    CanonicalStockBarPoint,
    CanonicalStockSignal,
)
from options.calendar import OptionExchangeCalendar


def test_native_bundle_has_exact_accepted_executor_membership():
    bundle = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(),
    )

    assert set(bundle.executors()) == {
        DetectorFamily.O1,
        DetectorFamily.O3,
        DetectorFamily.OS1,
        DetectorFamily.OS2,
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
        os1=AdvancedOptionsLaneInputs(
            blocked_reason_codes=("OS1_INPUT_UNAVAILABLE",),
        ),
        os2=AdvancedOptionsLaneInputs(
            blocked_reason_codes=("OS2_INPUT_UNAVAILABLE",),
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


def test_os1_executor_reuses_canonical_stock_first_model():
    gate, receipt = _gate_and_receipt()
    _, original = _confirmation_inputs()
    cutoff = receipt.cutoff
    source = original.model_copy(update={
        "volume_session": cutoff.date(),
        "market_time": cutoff - timedelta(minutes=2),
        "observed_at": cutoff - timedelta(minutes=1),
        "recorded_at": cutoff - timedelta(minutes=1),
        "received_at": cutoff - timedelta(minutes=1),
        "oi_observed_at": cutoff - timedelta(minutes=3),
        "oi_settlement_session": (
            OptionExchangeCalendar().previous_session(cutoff.date())
        ),
        "expiration_cutoff": cutoff + timedelta(days=7),
    })
    points = tuple(
        CanonicalStockBarPoint(
            bar_revision_id=uuid4(),
            interval="5m",
            session_date=cutoff.date(),
            bar_start=cutoff - timedelta(minutes=12 - index * 5),
            bar_end=cutoff - timedelta(minutes=7 - index * 5),
            open=Decimal(value),
            high=Decimal(value) + 1,
            low=Decimal(value) - 1,
            close=Decimal(value),
            system_observed_at=cutoff - timedelta(minutes=2),
            created_at=cutoff - timedelta(minutes=2),
            payload_sha256="a" * 64,
        )
        for index, value in enumerate(("100", "101.1"))
    )
    daily = tuple(
        CanonicalStockBarPoint(
            bar_revision_id=uuid4(),
            interval="1d",
            session_date=cutoff.date() - timedelta(days=15 - index),
            bar_start=cutoff - timedelta(days=15 - index, hours=7),
            bar_end=cutoff - timedelta(days=15 - index),
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            system_observed_at=cutoff - timedelta(days=15 - index),
            created_at=cutoff - timedelta(days=15 - index),
            payload_sha256="b" * 64,
        )
        for index in range(15)
    )
    signal_id = uuid5(
        NAMESPACE_URL,
        ":".join((
            "canonical-stock-signal",
            ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY_SHA256,
            "OS1",
            str(source.security_id),
            cutoff.date().isoformat(),
            "1",
            str(points[-1].bar_revision_id),
        )),
    )
    signal = CanonicalStockSignal(
        signal_id=signal_id,
        detector_id="OS1",
        policy_sha256=ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY_SHA256,
        security_id=source.security_id,
        underlyer=source.underlyer,
        direction=1,
        interval="5m",
        session_date=cutoff.date(),
        prior_close=Decimal("100"),
        prior_atr14=Decimal("2"),
        threshold=Decimal("1"),
        trigger_price=Decimal("101.1"),
        stop=Decimal("100"),
        target=Decimal("102.2"),
        daily_bars=daily,
        bars=points,
        market_time=points[-1].bar_end,
        available_at=cutoff - timedelta(minutes=2),
        received_at=cutoff - timedelta(minutes=1),
        valid_until=cutoff + timedelta(minutes=10),
    )
    executor = build_advanced_os_executor(
        DetectorFamily.OS1,
        AdvancedOptionsLaneInputs((
            AdvancedOSInvocation(uuid4(), signal, source),
        )),
    )

    result = executor(AdvancedOptionsDetectorContext(
        gate,
        receipt,
        DetectorFamily.OS1,
        cutoff,
    ))

    assert result.state.value == "COMPLETE"
    assert len(result.evaluations) == 1
    assert result.evaluations[0].payload["detector_id"] == "OS1"
    assert (
        result.evaluations[0].payload["decision"]["disposition"]
        == "CONFIRMED"
    ), result.evaluations[0].payload["decision"]["reasons"]
    assert result.evaluations[0].selection_status == "OBSERVATION"


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


def test_o1_lane_survives_one_contract_without_open_interest():
    gate, receipt = _gate_and_receipt()
    _, source = _confirmation_inputs()
    good = AdvancedO1Invocation(source.snapshot_id, source)
    bad = AdvancedO1Invocation(
        uuid4(),
        source.model_copy(update={"open_interest": 0}),
    )
    context = AdvancedOptionsDetectorContext(
        gate,
        receipt,
        DetectorFamily.O1,
        receipt.cutoff,
    )

    mixed = build_advanced_o1_executor(
        AdvancedOptionsLaneInputs((bad, good))
    )(context)
    assert mixed.state.value == "COMPLETE"
    assert len(mixed.evaluations) == 1

    none_usable = build_advanced_o1_executor(
        AdvancedOptionsLaneInputs((bad,))
    )(context)
    assert none_usable.state.value == "UNAVAILABLE"
    assert "POSITIVE_OPEN_INTEREST_REQUIRED" in none_usable.reason_codes


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


def _o1_detection(monkeypatch=None, enabled=True):
    from realtime import advanced_options_native_adapters as adapters

    if monkeypatch is not None:
        monkeypatch.setattr(adapters, "O1_PACKAGE_SELECTION_ENABLED", enabled)
    gate, receipt = _gate_and_receipt()
    _, source = _confirmation_inputs()
    package = _package(_policy().policy_sha256)
    detected = source.model_copy(update={"open_interest": 1, "day_volume": 1_000_000})
    result = build_advanced_o1_executor(
        AdvancedOptionsLaneInputs((
            AdvancedO1Invocation(source.snapshot_id, detected, package),
        ))
    )(AdvancedOptionsDetectorContext(
        gate,
        receipt,
        DetectorFamily.O1,
        receipt.cutoff,
    ))
    return result, package


def test_o1_detection_with_a_package_is_selected_again():
    result, package = _o1_detection()

    assert result.state.value == "COMPLETE"
    assert result.evaluations[0].selection_status == "SELECTED"
    assert result.evaluations[0].package == package


def test_o1_hold_switch_still_records_an_observation(monkeypatch):
    result, _ = _o1_detection(monkeypatch, enabled=False)

    assert result.evaluations[0].selection_status == "OBSERVATION"
    assert result.evaluations[0].package is None
