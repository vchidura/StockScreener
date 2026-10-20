from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from test_advanced_options_close import NOW, _journal, _projection
from test_advanced_options_cycle import (
    _complete,
    _gate_and_receipt,
    _reference_repository,
)
from test_advanced_options_control_plane import _plan as _control_plane
from realtime.advanced_options_package_evidence import (
    build_flow_package_evidence_batch,
)
from realtime.advanced_options_input import (
    reconstruct_options_input_ready_receipt,
)
from realtime.advanced_options_runtime import (
    AdvancedOptionsCloseStageData,
    AdvancedOptionsCycleStageData,
    AdvancedOptionsInputStageData,
    AdvancedOptionsProjectionStageData,
    AdvancedOptionsPublishStageData,
    AdvancedOptionsRuntimeBinding,
)
from realtime.materialization_jobs import JobBarrierVerdict
from realtime.options_v2_runtime import OptionsPreSessionStageBinding
from realtime.preflight import DetectorFamily
from realtime.runner_environment import RunnerEnvironment
from realtime.session_archive import ArchiveIdentity
from realtime.stage_graph import STAGE_CATALOG_VERSION_V4
from realtime.stage_runners_options import (
    options_cycle_runner,
    options_input_gate_runner,
    options_projection_runner,
    options_publish_runner,
)


class _Context:
    def __init__(self, control, *, catalog=STAGE_CATALOG_VERSION_V4):
        plan_id = control.plan_id
        self.plan = SimpleNamespace(
            plan_id=plan_id,
            cohort_id=control.cohort_id,
            session_date=control.session_date,
            stage_catalog_version=catalog,
        )
        self.window = SimpleNamespace(
            plan_id=plan_id,
            cohort_id=control.cohort_id,
            cutoff=NOW,
        )
        self.stage = SimpleNamespace(stage_id="OPTIONS_INPUT_GATE")
        self.stage_run_id = plan_id
        self.started_at = NOW
        self._unit = SimpleNamespace(cursor=MagicMock())

    def is_cancelled(self):
        return False

    @contextmanager
    def transaction(self, **_kwargs):
        yield self._unit


def _binding(tmp_path):
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    verdict = JobBarrierVerdict(
        gate.barrier_id,
        "COMPLETE",
        len(gate.barrier_members),
        tuple(row.member_key for row in gate.barrier_members),
        (),
        (),
    )
    runtime_receipt = reconstruct_options_input_ready_receipt(
        gate,
        verdict,
        cutoff=NOW,
        reference_generation=receipt.reference_generation,
        build_id="provider-free-runtime-v1",
    )
    executors = {
        detector: (lambda _context: _complete())
        for detector in control.manifest_policy.required_detectors
        if detector is not DetectorFamily.O2
    }
    state = {}

    def write_cycle(
        _cursor,
        *,
        cycle,
        control_plane,
        evidence_batch,
        flow_identity,
    ):
        assert control_plane == control
        assert flow_identity is None
        assert evidence_batch.cycle_sha256 == cycle.cycle_sha256
        state["cycle"] = cycle

    def write_projection(_cursor, batch):
        state["projection"] = batch

    operations = MagicMock()
    pre_session = OptionsPreSessionStageBinding(operations)
    binding = AdvancedOptionsRuntimeBinding(
        build_id="provider-free-runtime-v1",
        pre_session=pre_session,
        input_loader=lambda _context: AdvancedOptionsInputStageData(
            control,
            gate,
            verdict,
            runtime_receipt.reference_generation,
            "input-ready",
        ),
        cycle_loader=lambda _context: AdvancedOptionsCycleStageData(
            control,
            gate,
            runtime_receipt,
            executors,
            runtime_receipt.reference_generation,
            _reference_repository(),
            lambda cycle: build_flow_package_evidence_batch(cycle, ()),
            "cycle",
        ),
        projection_loader=lambda _context: AdvancedOptionsProjectionStageData(
            control,
            state["cycle"],
            "projection",
        ),
        publish_loader=lambda _context: AdvancedOptionsPublishStageData(
            control,
            state["projection"],
            "non-serving-handoff",
        ),
        cycle_writer=write_cycle,
        projection_writer=write_projection,
    )
    return binding, control, state


def test_runtime_binding_completes_non_serving_stage_chain(tmp_path):
    binding, control, state = _binding(tmp_path)
    environment = binding.bind(RunnerEnvironment(None))
    context = _Context(control)

    assert options_input_gate_runner(environment)(context).state == "COMPLETE"
    cycle = options_cycle_runner(environment)(context)
    assert cycle.state == "COMPLETE"
    assert options_projection_runner(environment)(context).state == "COMPLETE"
    publish = options_publish_runner(environment)(context)

    assert publish.state == "COMPLETE"
    assert "cycle" in state
    assert "projection" in state
    outcomes = {row.detector: row for row in state["cycle"].outcomes}
    assert outcomes[DetectorFamily.O2].state.value == "UNAVAILABLE"
    assert not state["projection"].publication_authorized


def test_runtime_binding_requires_v4_exact_scope_and_all_native_executors(
    tmp_path,
):
    binding, control, _ = _binding(tmp_path)
    environment = binding.bind(RunnerEnvironment(None))

    with pytest.raises(ValueError, match="runtime scope changed"):
        environment.options_input_gate_action(
            _Context(control, catalog="stages_v3")
        )

    data = binding.cycle_loader(_Context(control))
    incomplete = AdvancedOptionsCycleStageData(
        data.control_plane,
        data.input_gate,
        data.input_receipt,
        {},
        data.reference_generation,
        data.reference_repository,
        data.evidence_builder,
        data.evidence_ref,
    )
    object.__setattr__(binding, "cycle_loader", lambda _context: incomplete)
    with pytest.raises(ValueError, match="receipt or executor membership"):
        environment.options_cycle_action(_Context(control))


def test_runtime_binding_refuses_action_override(tmp_path):
    binding, _, _ = _binding(tmp_path)
    environment = RunnerEnvironment(
        None,
        options_cycle_action=lambda _context: None,
    )

    with pytest.raises(ValueError, match="already bound"):
        binding.bind(environment)


def test_runtime_close_builds_inert_retention_handoff(tmp_path):
    binding, _, _ = _binding(tmp_path)
    control, projection = _projection()
    data = AdvancedOptionsCloseStageData(
        control_plane=control,
        input_gate=projection.cycle.input_gate,
        projection=projection,
        journal=_journal(tmp_path, control),
        flow_checkpoint_offset=10,
        market_state_checkpoint_offset=20,
        unresolved_gap_ids=(),
        slot_coverage_sha256="c" * 64,
        oi_coverage_sha256="d" * 64,
        archive_identity=ArchiveIdentity("runtime-build", "runtime-policy"),
        archives_verified=True,
        open_alert_ids=(),
        journal_retention_expires_at=NOW - timedelta(days=1),
        checked_at=NOW,
    )

    handoff = binding.close(data)

    assert handoff.close_coverage.status == "COMPLETE"
    assert handoff.raw_journal_removal_eligible
    assert not any(
        (
            handoff.archive_execution_authorized,
            handoff.retention_execution_authorized,
            handoff.journal_deletion_authorized,
        )
    )
