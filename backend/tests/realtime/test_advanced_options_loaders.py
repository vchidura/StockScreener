from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from realtime.advanced_options_loaders import (
    AdvancedOptionsCanonicalCycleFacts,
    AdvancedOptionsCanonicalInputFacts,
    AdvancedOptionsCanonicalLoaders,
    build_canonical_advanced_options_runtime_binding,
)
from realtime.advanced_options_projection import (
    AdvancedOptionsPageSource,
    build_advanced_options_projection_batch,
)
from realtime.advanced_options_cycle import run_advanced_options_cycle
from realtime.advanced_options_native_adapters import (
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
)
from realtime.options_v2_runtime import OptionsPreSessionStageBinding
from realtime.advanced_options_runtime import AdvancedOptionsRuntimeHandoff
from realtime.advanced_options_package_evidence import (
    build_flow_package_evidence_batch,
)
from realtime.advanced_options_input import (
    reconstruct_options_input_ready_receipt,
)
from realtime.materialization_jobs import JobBarrierVerdict
from realtime.orchestration_repository import CycleReceiptSpec
from realtime.preflight import DetectorFamily
from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_cycle import _reference_repository
from test_advanced_options_input import (
    EQUITY_RECEIPT,
    EQUITY_WINDOW,
    NOW,
    _cycle_input,
)
from test_advanced_options_producers import _producer_plan


class _Context:
    def __init__(self, control):
        self.plan = SimpleNamespace(
            plan_id=control.plan_id,
            cohort_id=control.cohort_id,
            session_date=control.session_date,
            stage_catalog_version="stages_v4",
        )
        self.window = SimpleNamespace(
            window_id=uuid4(),
            plan_id=control.plan_id,
            cohort_id=control.cohort_id,
            cutoff=NOW,
        )
        self._unit = SimpleNamespace(cursor=MagicMock())

    @contextmanager
    def transaction(self, **_kwargs):
        yield self._unit


def _input_facts():
    control = _control_plane()
    reference = reference_state()
    rolling = rolling_state(
        cohort_id=control.cohort_id,
        journal_generation=control.source_flow.journal_generation,
    )
    return AdvancedOptionsCanonicalInputFacts(
        control_plane=control,
        producer_plan=_producer_plan(),
        cycle_input=_cycle_input(control),
        reference_state=reference,
        rolling_state=rolling,
        reference_policy_sha256=reference.reference_policy_sha256,
        oi_facts_sha256=reference.open_interest_sha256,
        market_state_checkpoint_sha256=(
            rolling.market_state_checkpoint_sha256
        ),
        flow_checkpoint_sha256=rolling.flow_checkpoint_sha256,
        unresolved_gaps_sha256="b" * 64,
        market_state_heads_sha256="c" * 64,
        equity_input_receipt_id=EQUITY_RECEIPT,
        equity_input_receipt=CycleReceiptSpec(
            "INPUT_READY",
            NOW,
            7,
            control.universe.sha256,
            "d" * 64,
            "equity-build",
        ),
        equity_input_sha256="d" * 64,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        available_at=NOW,
        maximum_equity_input_age_seconds=None,
    )


def test_unbound_equity_receipt_preserves_exact_cutoff_behavior():
    facts = _input_facts()
    stale_receipt = replace(
        facts.equity_input_receipt,
        cutoff=NOW - timedelta(seconds=1),
    )

    with pytest.raises(
        ValueError,
        match="Equity input receipt changed",
    ):
        replace(
            facts,
            equity_input_receipt=stale_receipt,
            maximum_equity_input_age_seconds=None,
        )

    assert replace(
        facts,
        equity_input_receipt=stale_receipt,
        maximum_equity_input_age_seconds=1,
    ).maximum_equity_input_age_seconds == 1


def test_canonical_loader_reconstructs_and_opens_exact_input_barrier():
    facts = _input_facts()
    context = _Context(facts.control_plane)
    port = MagicMock()
    port.load_input_facts.return_value = facts
    enqueued = []
    opened = []
    recorded = []

    def evaluate(_cursor, barrier_id):
        return JobBarrierVerdict(
            barrier_id,
            "COMPLETE",
            2,
            ("EQUITY_INPUT_READY", "UNDERLYER:SPY"),
            (),
            (),
        )

    loaders = AdvancedOptionsCanonicalLoaders(
        port,
        build_id="advanced-options-runtime-v1",
        enqueue_job=lambda _cursor, job: enqueued.append(job),
        open_barrier=lambda _cursor, **values: opened.append(values),
        record_barrier_outcome=(
            lambda _cursor, **values: recorded.append(values)
        ),
        evaluate_barrier=evaluate,
    )
    stage = loaders.input(context)

    assert stage.input_gate.closure.cycle_input.cohort_id == (
        facts.control_plane.cohort_id
    )
    assert len(enqueued) == 1
    assert opened[0]["barrier_id"] == stage.input_gate.barrier_id
    assert recorded[0]["member_key"] == "EQUITY_INPUT_READY"
    assert stage.barrier_verdict.state == "COMPLETE"


def test_canonical_loader_builds_exact_native_executor_membership():
    facts = _input_facts()
    context = _Context(facts.control_plane)
    port = MagicMock()
    port.load_input_facts.return_value = facts

    def evaluate(_cursor, barrier_id):
        return JobBarrierVerdict(
            barrier_id,
            "COMPLETE",
            2,
            ("EQUITY_INPUT_READY", "UNDERLYER:SPY"),
            (),
            (),
        )

    loaders = AdvancedOptionsCanonicalLoaders(
        port,
        build_id="advanced-options-runtime-v1",
        enqueue_job=lambda *_args: None,
        open_barrier=lambda *_args, **_kwargs: None,
        record_barrier_outcome=lambda *_args, **_kwargs: None,
        evaluate_barrier=evaluate,
    )
    input_stage = loaders.input(context)
    receipt = reconstruct_options_input_ready_receipt(
        input_stage.input_gate,
        input_stage.barrier_verdict,
        cutoff=NOW,
        reference_generation=input_stage.reference_generation,
        build_id="advanced-options-runtime-v1",
    )
    native = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(),
    )
    port.load_cycle_facts.return_value = AdvancedOptionsCanonicalCycleFacts(
        receipt,
        native,
        _reference_repository(),
        lambda cycle: build_flow_package_evidence_batch(cycle, ()),
        "cycle-evidence",
    )

    cycle = loaders.cycle(context)

    assert cycle.input_receipt == receipt
    assert set(cycle.executors) == {
        DetectorFamily.O1,
        DetectorFamily.O3,
    }


def test_canonical_binding_factory_wires_one_loader_owner():
    port = MagicMock()
    operations = MagicMock()

    binding, loaders = build_canonical_advanced_options_runtime_binding(
        build_id="advanced-options-runtime-v1",
        pre_session=OptionsPreSessionStageBinding(operations),
        port=port,
        cycle_writer=lambda *_args, **_kwargs: None,
        projection_writer=lambda *_args, **_kwargs: None,
        page_source_loader=(
            lambda *_args, **_kwargs: AdvancedOptionsPageSource()
        ),
    )

    assert binding.input_loader.__self__ is loaders
    assert binding.cycle_loader.__self__ is loaders
    assert binding.pre_session.operations is operations


def test_completed_stage_handoffs_rehydrate_from_durable_hashes():
    facts = _input_facts()
    context = _Context(facts.control_plane)
    port = MagicMock()
    port.load_input_facts.return_value = facts

    def evaluate(_cursor, barrier_id):
        return JobBarrierVerdict(
            barrier_id,
            "COMPLETE",
            2,
            ("EQUITY_INPUT_READY", "UNDERLYER:SPY"),
            (),
            (),
        )

    initial = AdvancedOptionsCanonicalLoaders(
        port,
        build_id="advanced-options-runtime-v1",
        enqueue_job=lambda *_args: None,
        open_barrier=lambda *_args, **_kwargs: None,
        record_barrier_outcome=lambda *_args, **_kwargs: None,
        evaluate_barrier=evaluate,
    )
    input_stage = initial.input(context)
    receipt = reconstruct_options_input_ready_receipt(
        input_stage.input_gate,
        input_stage.barrier_verdict,
        cutoff=NOW,
        reference_generation=input_stage.reference_generation,
        build_id="advanced-options-runtime-v1",
    )
    native = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(),
    )
    cycle = run_advanced_options_cycle(
        control_plane=facts.control_plane,
        input_gate=input_stage.input_gate,
        input_receipt=receipt,
        executors=native.executors(),
        cutoff=NOW,
        reference_generation=input_stage.reference_generation,
        reference_repository=_reference_repository(),
        build_id="advanced-options-runtime-v1",
    )
    projection = build_advanced_options_projection_batch(
        control_plane=facts.control_plane,
        cycle=cycle,
        generated_at=NOW,
        build_id="advanced-options-runtime-v1",
        page_source=AdvancedOptionsPageSource(),
    )
    port.load_input_receipt.return_value = receipt
    port.load_cycle_facts.return_value = AdvancedOptionsCanonicalCycleFacts(
        receipt,
        native,
        _reference_repository(),
        lambda value: build_flow_package_evidence_batch(value, ()),
        "cycle-evidence",
    )
    port.load_completed_cycle.return_value = cycle
    port.load_completed_projection.return_value = projection

    def unexpected_write(*_args, **_kwargs):
        raise AssertionError("completed input stage attempted a write")

    restarted = AdvancedOptionsCanonicalLoaders(
        port,
        build_id="advanced-options-runtime-v1",
        enqueue_job=unexpected_write,
        open_barrier=unexpected_write,
        record_barrier_outcome=unexpected_write,
        evaluate_barrier=evaluate,
    )
    cycle_data = restarted.cycle(context)

    assert cycle_data.input_gate.closure_sha256 == (
        receipt.input_sha256
    )
    after_cycle = SimpleNamespace(
        handoff=AdvancedOptionsRuntimeHandoff()
    )
    projection_data = restarted.projection(after_cycle, context)
    assert projection_data.cycle.cycle_sha256 == cycle.cycle_sha256
    assert after_cycle.handoff.cycle(
        facts.control_plane.cohort_id,
        NOW,
    ) == cycle

    after_projection = AdvancedOptionsCanonicalLoaders(
        port,
        build_id="advanced-options-runtime-v1",
        enqueue_job=unexpected_write,
        open_barrier=unexpected_write,
        record_barrier_outcome=unexpected_write,
        evaluate_barrier=evaluate,
    )
    publish_binding = SimpleNamespace(
        handoff=AdvancedOptionsRuntimeHandoff()
    )
    publish_data = after_projection.publish(
        publish_binding,
        context,
    )

    assert publish_data.projection.batch_sha256 == (
        projection.portal_ready_receipt.input_sha256
    )
    assert port.load_completed_cycle.call_count == 2
    assert port.load_completed_projection.call_count == 1
