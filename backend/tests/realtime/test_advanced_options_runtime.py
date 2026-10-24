from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from option_flow_runner_support import (
    FakeOptionsDatabase,
    harness as flow_harness,
    scenario_feed,
)
from orchestration_fakes import InMemoryOrchestrationRepository
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
from realtime.advanced_options_projection import (
    AdvancedOptionsPublicationAuthorization,
    AdvancedOptionsPageSource,
    OPTIONS_SNAPSHOT_TYPES,
)
from realtime.contracts import canonical_sha256
from realtime.advanced_options_api import (
    AdvancedOptionsStoredProjectionRepository,
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
    build_advanced_options_v3_runtime_composition,
)
from realtime.materialization_jobs import JobBarrierVerdict
from realtime.options_v2_runtime import OptionsPreSessionStageBinding
from realtime.orchestration_repository import WindowSpec
from realtime.preflight import DetectorFamily
from realtime.runner_environment import RunnerEnvironment
from realtime.session_archive import ArchiveIdentity
from realtime.stage_graph import (
    STAGES_V4,
    STAGES_V5,
    STAGE_CATALOG_VERSION_V4,
    STAGE_CATALOG_VERSION_V5,
)
from realtime.stage_orchestrator import StageOrchestrator
from realtime.stage_runners_options import (
    OptionsAcquireResult,
    OptionsAdmissionResult,
    OptionsReferenceStageResult,
    options_cycle_runner,
    options_input_gate_runner,
    options_projection_runner,
    options_publish_runner,
    options_reference_runner,
)
from realtime.stage_runners_v2 import (
    build_advanced_options_non_serving_runners,
)
from realtime.advanced_options_service import (
    OPTIONS_NON_SERVING_STAGE_ORDER,
    OPTIONS_V3_STAGE_ORDER,
    build_advanced_options_non_serving_qualification_plan,
)
from realtime.portal_projection_repository import (
    RELEASE_CLASS_INTRADAY,
    PortalRelease,
    PortalReleasePointer,
)
from realtime.portal_publisher import (
    CanonicalPortalPublicationResult,
    PortalPublicationStatus,
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


def _binding(tmp_path, *, catalog=STAGE_CATALOG_VERSION_V4):
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
    operations.acquire.return_value = OptionsAcquireResult(
        uuid4(),
        ("SPY",),
        ("SPY",),
        0,
        0,
    )
    operations.admit.return_value = OptionsAdmissionResult(
        uuid4(),
        "READY",
        (),
        control.cohort_id,
        1,
        1,
    )
    pre_session = OptionsPreSessionStageBinding(operations)
    reference = _reference_repository().load_generation(7)
    authority_id = uuid4()

    def authority(_context, handoff):
        return AdvancedOptionsPublicationAuthorization(
            authority_id,
            control.cohort_id,
            control.binding.provider_tier,
            handoff.batch_sha256,
            uuid4(),
            "PROVIDER_FREE_V5_QUALIFICATION",
        )

    def publish(batch, *, checked_at):
        state["publication_calls"] = (
            state.get("publication_calls", 0) + 1
        )
        manifest = {
            "version": "realtime-portal-release-v1",
            "release_class": RELEASE_CLASS_INTRADAY,
            "session_date": batch.receipt.cutoff.date().isoformat(),
            "snapshots": [],
        }
        release_id = uuid4()
        release = PortalRelease(
            release_id,
            control.cohort_id,
            control.binding.provider_tier,
            RELEASE_CLASS_INTRADAY,
            batch.receipt.receipt_id,
            batch.receipt.cutoff.date(),
            batch.receipt.cutoff,
            batch.receipt.universe_sha256,
            manifest,
            canonical_sha256(manifest),
            True,
        )
        pointer = PortalReleasePointer(
            control.cohort_id,
            control.binding.provider_tier,
            release_id,
            batch.receipt.cutoff,
            batch.receipt.cutoff.date(),
        )
        status = PortalPublicationStatus(
            checked_at,
            len(batch.snapshots),
            0,
            0,
            0,
            0,
            (),
            release,
        )
        result = CanonicalPortalPublicationResult(
            status,
            release,
            pointer,
        )
        state["publication"] = result
        return result

    values = dict(
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
        page_source_loader=(
            lambda *_args, **_kwargs: AdvancedOptionsPageSource()
        ),
    )
    if catalog == STAGE_CATALOG_VERSION_V5:
        values.update(
            catalog_version=catalog,
            reference_loader=lambda _context: OptionsReferenceStageResult(
                reference.state_id,
                reference.state_sha256,
                reference.reference_generation,
                f"reference:{reference.state_id}",
            ),
            publication_authority_loader=authority,
            canonical_publisher=publish,
        )
    binding = AdvancedOptionsRuntimeBinding(**values)
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


def test_v5_runtime_composition_uses_v5_graph_and_truthful_publish(
    tmp_path,
):
    binding, control, state = _binding(
        tmp_path,
        catalog=STAGE_CATALOG_VERSION_V5,
    )
    composition = build_advanced_options_v3_runtime_composition(
        binding=binding,
        repository=MagicMock(),
        database=None,
    )
    context = _Context(control, catalog=STAGE_CATALOG_VERSION_V5)

    assert tuple(composition.runners) == OPTIONS_V3_STAGE_ORDER
    assert composition.orchestrator._stages == STAGES_V5
    assert composition.orchestrator._catalog_version == (
        STAGE_CATALOG_VERSION_V5
    )
    assert options_reference_runner(composition.environment)(
        context
    ).state == "COMPLETE"
    assert options_input_gate_runner(composition.environment)(
        context
    ).state == "COMPLETE"
    assert options_cycle_runner(composition.environment)(
        context
    ).state == "COMPLETE"
    assert options_projection_runner(composition.environment)(
        context
    ).state == "COMPLETE"
    published = options_publish_runner(composition.environment)(context)

    assert published.state == "COMPLETE"
    assert state["publication"].release.release_id == (
        state["publication"].pointer.release_id
    )
    assert state["publication_calls"] == 1
    assert not state["publication"].status.claimed


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


def test_exact_v4_composition_executes_with_equity_isolated_and_no_effects(
    tmp_path,
):
    binding, control, state = _binding(tmp_path)
    flow = flow_harness(
        scenario_feed(),
        database=FakeOptionsDatabase(),
        enabled={"OPTIONS_FLOW"},
        cohort_id=control.cohort_id,
    )
    environment = binding.bind(flow.environment)
    runners = build_advanced_options_non_serving_runners(
        flow.database,
        environment=environment,
    )
    qualification = build_advanced_options_non_serving_qualification_plan(
        plan_id=control.plan_id,
        cohort_id=control.cohort_id,
        session_date=control.session_date,
        config_sha256="9" * 64,
        deadline=NOW + timedelta(hours=1),
    )
    repository = InMemoryOrchestrationRepository(
        qualification.service_plan,
        STAGES_V4,
    )
    for stage_id in ("REFERENCE", "EQUITY_MATERIALIZE"):
        repository.upsert_readiness(
            control.cohort_id,
            stage_id,
            STAGE_CATALOG_VERSION_V4,
            "READY",
            (),
            f"qualified-{stage_id.lower()}",
            NOW,
        )
    orchestrator = StageOrchestrator(
        repository,
        runners,
        database=flow.database,
        stages=STAGES_V4,
        catalog_version=STAGE_CATALOG_VERSION_V4,
        clock=lambda: NOW,
        parallel_lanes=False,
    )

    start = orchestrator.start(control.plan_id)
    flow_outcome = orchestrator.tick_resident(
        control.plan_id,
        "OPTIONS_FLOW",
    )
    window = repository.ensure_window(
        control.plan_id,
        WindowSpec(
            control.cohort_id,
            "MASSIVE_ADVANCED",
            "owp3-qualified-window",
            NOW,
            NOW,
            NOW + timedelta(minutes=30),
        ),
    )
    report = orchestrator.run_window(window.window_id)

    assert tuple(runners) == OPTIONS_NON_SERVING_STAGE_ORDER
    assert start.enabled == frozenset(OPTIONS_NON_SERVING_STAGE_ORDER)
    assert flow_outcome.state == "COMPLETE"
    assert (report.status, report.window_state) == ("FINISHED", "COMPLETE")
    assert {
        row["stage"] for row in repository.runs
    } == set(OPTIONS_NON_SERVING_STAGE_ORDER) - {"OPTIONS_FLOW"}
    assert all(
        STAGES_V4[row["stage"]].domain == "options"
        for row in repository.runs
    )
    assert repository.release == {}
    assert len(state["projection"].snapshots) == len(
        OPTIONS_SNAPSHOT_TYPES
    )
    assert not state["projection"].publication_authorized
    assert not any(qualification.effect_authorizations.values())

    snapshot = state["projection"].snapshots[0]
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

    @contextmanager
    def stored_cursor():
        yield cursor

    stored = AdvancedOptionsStoredProjectionRepository(
        stored_cursor
    ).read(
        cohort_id=control.cohort_id,
        snapshot_type=snapshot.snapshot_type,
        available_by=NOW,
    )

    assert stored["available"]
    assert stored["provider_fetch_performed"] is False
    assert stored["detector_work_performed"] is False
    assert stored["publication_performed"] is False


def test_integrated_flow_restart_replays_from_the_durable_checkpoint():
    database = FakeOptionsDatabase()
    first = flow_harness(scenario_feed(), database=database)
    assert first.tick().state == "COMPLETE"
    covered = (
        first.environment.options_flow_state.session.consumer
        .checkpointed_offset
    )
    restarted = flow_harness(
        scenario_feed(),
        database=database,
        cohort_id=first.plan.cohort_id,
    )

    outcome = restarted.tick()

    assert outcome.state == "COMPLETE"
    assert "start=RESTORED" in outcome.evidence_ref
    assert (
        restarted.environment.options_flow_state.session.start.offset
        == covered
    )
