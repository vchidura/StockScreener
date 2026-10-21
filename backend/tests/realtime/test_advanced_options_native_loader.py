from contextlib import contextmanager
from types import SimpleNamespace

from psycopg2 import Error as PsycopgError

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from realtime.advanced_options_native_adapters import (
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
)
from realtime.advanced_options_native_loader import (
    AdvancedOptionsNativeInputPreflight,
    NativeInputPlanStatus,
    PostgresAdvancedOptionsNativeInputLoader,
    advanced_options_native_input_plan_payload,
    build_advanced_options_native_input_plan,
    freeze_advanced_options_native_input_plan,
)
from realtime.advanced_options_rolling_state import (
    AdvancedOptionsRollingProducerBinding,
)
from realtime.database_access_v2 import V2ConnectionProvider
from realtime.option_source_flow import build_option_source_flow_identity
from realtime.orchestration_repository import CycleReceiptSpec
from realtime.advanced_options_producers import (
    build_advanced_options_producer_plan,
)
from test_advanced_options_control_plane import _enrollment_plan
from test_advanced_options_input import EQUITY_RECEIPT, EQUITY_WINDOW, NOW
from test_advanced_options_producers import (
    _clocks,
    _discovery_scope,
    _limits,
)


def _plan():
    enrollment = _enrollment_plan()
    control = enrollment.control_plane
    producer = build_advanced_options_producer_plan(
        control_plane=control,
        discovery_scope=_discovery_scope(control),
        provider_budget=control.provider_budget,
        clocks=_clocks(control),
        endpoint_response_byte_limits=_limits(),
    )
    reference = reference_state()
    source_flow = build_option_source_flow_identity(
        journal_generation=control.source_flow.journal_generation,
        stream_session_ids=control.source_flow.stream_session_ids,
        condition_semantics_sha256=control.pins.condition_semantics_sha256,
        flow_policy_sha256="1" * 64,
        advanced_flow_policy_sha256=control.pins.flow_policy_sha256,
        contract_scope_sha256=reference.contract_scope_sha256,
    )
    binding = AdvancedOptionsRollingProducerBinding(
        producer_plan_id=producer.plan_id,
        producer_plan_sha256=producer.plan_sha256,
        reference_state_id=reference.state_id,
        reference_state_sha256=reference.state_sha256,
        reference_generation=reference.reference_generation,
        source_flow_id=source_flow.source_flow_id,
        source_flow_sha256=source_flow.source_flow_sha256,
        known_at=NOW,
        valid_until=reference.valid_until,
        unresolved_gaps_sha256="2" * 64,
        source_watermarks_sha256="3" * 64,
        market_state_heads_sha256="4" * 64,
        volume_checkpoint_sha256="5" * 64,
    )
    rolling = rolling_state(
        cohort_id=control.cohort_id,
        journal_generation=source_flow.journal_generation,
        committed_at=NOW,
        producer_binding=binding,
    )
    receipt = CycleReceiptSpec(
        "INPUT_READY",
        NOW,
        reference.reference_generation,
        enrollment.equity_plan.universe.sha256,
        "6" * 64,
        "advanced-equity-build-v1",
    )
    return build_advanced_options_native_input_plan(
        enrollment=enrollment,
        producer_plan=producer,
        reference_state=reference,
        rolling_state=rolling,
        source_flow=source_flow,
        cutoff=NOW,
        equity_input_receipt_id=EQUITY_RECEIPT,
        equity_input_receipt=receipt,
        equity_window_id=EQUITY_WINDOW,
        loader_build_id="advanced-options-native-loader-v1",
        stock_detector_sha256s=(
            ("S1", "7" * 64),
            ("S2", "8" * 64),
        ),
    )


def _preflight(plan):
    bundle = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(
            blocked_reason_codes=("CANONICAL_MARKS_UNAVAILABLE",),
        ),
        s1=AdvancedOptionsLaneInputs(),
        s2=AdvancedOptionsLaneInputs(),
        plan_id=plan.plan_id,
        input_sha256="9" * 64,
        executor_membership_sha256="a" * 64,
    )
    return AdvancedOptionsNativeInputPreflight(
        status=NativeInputPlanStatus.READY,
        plan_id=plan.plan_id,
        preflight_sha256="b" * 64,
        input_sha256=bundle.input_sha256,
        executor_membership_sha256=bundle.executor_membership_sha256,
        bundle=bundle,
        evidence=(),
    )


def test_native_input_plan_freezes_exact_scope_and_zero_authority():
    plan = _plan()
    payload = advanced_options_native_input_plan_payload(plan)
    frozen = freeze_advanced_options_native_input_plan(
        plan,
        _preflight(plan),
    )

    assert payload["mode"] == "PLAN"
    assert payload["identity"]["cohort_id"] == plan.control_plane.cohort_id
    assert payload["identity"]["reference_state_sha256"] == (
        plan.reference_state.state_sha256
    )
    assert payload["identity"]["rolling_state_sha256"] == (
        plan.rolling_state.state_sha256
    )
    assert set(payload["effect_counts"].values()) == {0}
    assert frozen.frozen
    assert frozen.expected_input_sha256 == "9" * 64
    assert frozen.plan_id != plan.plan_id


class _RetryableError(PsycopgError):
    pgcode = "40001"


class _RetryProvider(V2ConnectionProvider):
    def __init__(self):
        self.attempts = 0

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.attempts += 1
        if self.attempts == 1:
            raise _RetryableError("retry")
        yield SimpleNamespace(cursor=object(), connection=object())


def test_native_input_loader_retries_serialization_rollback(monkeypatch):
    plan = _plan()
    preflight = _preflight(plan)
    frozen = freeze_advanced_options_native_input_plan(plan, preflight)
    provider = _RetryProvider()
    waits = []
    loader = PostgresAdvancedOptionsNativeInputLoader(
        provider,
        frozen,
        maximum_attempts=2,
        retry_initial_seconds=0.01,
        wait=waits.append,
    )
    monkeypatch.setattr(loader, "_assemble", lambda _unit: preflight)

    bundle = loader.load()

    assert bundle.input_sha256 == preflight.input_sha256
    assert bundle.executor_membership_sha256 == (
        preflight.executor_membership_sha256
    )
    assert bundle.plan_id == frozen.plan_id
    assert provider.attempts == 2
    assert waits == [0.01]
