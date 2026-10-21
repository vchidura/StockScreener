"""Canonical repository composition for non-serving Advanced Options stages."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from .advanced_options_control_plane import AdvancedOptionsControlPlanePlan
from .advanced_options_cycle import (
    AdvancedOptionsDatasetFlowIdentity,
)
from .advanced_options_input import (
    build_advanced_options_input_closure,
    build_advanced_options_input_gate_plan,
)
from .advanced_options_native_adapters import (
    AdvancedOptionsNativeInputBundle,
)
from .advanced_options_producers import AdvancedOptionsProducerPlan
from .advanced_options_reference_state import (
    AdvancedOptionsReferenceState,
    AdvancedOptionsReferenceStateRepository,
)
from .advanced_options_rolling_state import AdvancedOptionsRollingState
from .advanced_options_runtime import (
    AdvancedOptionsCloseStageData,
    AdvancedOptionsCycleStageData,
    AdvancedOptionsInputStageData,
    AdvancedOptionsProjectionStageData,
    AdvancedOptionsPublishStageData,
    AdvancedOptionsRuntimeBinding,
    EvidenceBuilder,
)
from .materialization_jobs import (
    BarrierOutcome,
    JobBarrierVerdict,
    enqueue_materialization_job_cursor,
    evaluate_job_barrier_cursor,
    open_job_barrier_cursor,
    record_job_barrier_outcome_cursor,
)
from .option_cycle_input import OptionCycleInput
from .orchestration_repository import CycleReceiptSpec
from .stage_orchestrator import StageContext
from .options_v2_runtime import OptionsPreSessionStageBinding


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCanonicalInputFacts:
    control_plane: AdvancedOptionsControlPlanePlan
    producer_plan: AdvancedOptionsProducerPlan
    cycle_input: OptionCycleInput
    reference_state: AdvancedOptionsReferenceState
    rolling_state: AdvancedOptionsRollingState
    reference_policy_sha256: str
    oi_facts_sha256: str
    market_state_checkpoint_sha256: str
    flow_checkpoint_sha256: str
    unresolved_gaps_sha256: str
    market_state_heads_sha256: str
    equity_input_receipt_id: UUID
    equity_input_receipt: CycleReceiptSpec
    equity_input_sha256: str
    equity_window_id: UUID
    instrument_ids: dict[str, int]
    available_at: datetime
    native_input_plan_sha256: str | None = None
    native_input_sha256: str | None = None
    executor_membership_sha256: str | None = None
    mark_producer_plan_sha256: str | None = None
    marks_sha256: str | None = None

    def __post_init__(self) -> None:
        if (
            self.equity_input_receipt.receipt_kind != "INPUT_READY"
            or self.equity_input_receipt.input_sha256
            != self.equity_input_sha256
            or self.equity_input_receipt.cutoff
            != self.cycle_input.scheduled_cycle
            or self.equity_input_receipt.advance_release
        ):
            raise ValueError(
                "Advanced Options Equity input receipt changed"
            )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCanonicalCycleFacts:
    input_receipt: CycleReceiptSpec
    native_inputs: AdvancedOptionsNativeInputBundle
    reference_repository: AdvancedOptionsReferenceStateRepository
    evidence_builder: EvidenceBuilder
    evidence_ref: str
    flow_identity: AdvancedOptionsDatasetFlowIdentity | None = None


class AdvancedOptionsCanonicalRuntimePort(Protocol):
    def load_input_facts(
        self,
        context: StageContext,
    ) -> AdvancedOptionsCanonicalInputFacts: ...

    def load_cycle_facts(
        self,
        context: StageContext,
        input_stage: AdvancedOptionsInputStageData,
    ) -> AdvancedOptionsCanonicalCycleFacts: ...

    def load_close_data(
        self,
        context: StageContext,
        projection,
    ) -> AdvancedOptionsCloseStageData: ...


class AdvancedOptionsCanonicalLoaders:
    """Build stage inputs only from one explicit canonical repository port."""

    def __init__(
        self,
        port: AdvancedOptionsCanonicalRuntimePort,
        *,
        build_id: str,
        enqueue_job: Callable = enqueue_materialization_job_cursor,
        open_barrier: Callable = open_job_barrier_cursor,
        record_barrier_outcome: Callable = (
            record_job_barrier_outcome_cursor
        ),
        evaluate_barrier: Callable = evaluate_job_barrier_cursor,
    ) -> None:
        if not build_id.strip():
            raise ValueError("Advanced Options loader build ID cannot be blank")
        self._port = port
        self._build_id = build_id.strip()
        for value, name in (
            (enqueue_job, "enqueue job"),
            (open_barrier, "open barrier"),
            (record_barrier_outcome, "record barrier outcome"),
            (evaluate_barrier, "evaluate barrier"),
        ):
            if not callable(value):
                raise TypeError(
                    f"Advanced Options loader {name} callback is invalid"
                )
        self._enqueue_job = enqueue_job
        self._open_barrier = open_barrier
        self._record_barrier_outcome = record_barrier_outcome
        self._evaluate_barrier = evaluate_barrier
        self._inputs: dict[
            tuple[UUID, datetime],
            AdvancedOptionsInputStageData,
        ] = {}

    @staticmethod
    def _key(
        context: StageContext,
    ) -> tuple[UUID, datetime]:
        return context.plan.cohort_id, context.window.cutoff

    def input(self, context: StageContext) -> AdvancedOptionsInputStageData:
        facts = self._port.load_input_facts(context)
        closure = build_advanced_options_input_closure(
            control_plane=facts.control_plane,
            producer_plan=facts.producer_plan,
            cycle_input=facts.cycle_input,
            reference_state=facts.reference_state,
            rolling_state=facts.rolling_state,
            reference_policy_sha256=facts.reference_policy_sha256,
            oi_facts_sha256=facts.oi_facts_sha256,
            market_state_checkpoint_sha256=(
                facts.market_state_checkpoint_sha256
            ),
            flow_checkpoint_sha256=facts.flow_checkpoint_sha256,
            unresolved_gaps_sha256=facts.unresolved_gaps_sha256,
            market_state_heads_sha256=facts.market_state_heads_sha256,
            equity_input_receipt_id=facts.equity_input_receipt_id,
            equity_input_sha256=facts.equity_input_sha256,
            native_input_plan_sha256=(
                facts.native_input_plan_sha256
            ),
            native_input_sha256=facts.native_input_sha256,
            executor_membership_sha256=(
                facts.executor_membership_sha256
            ),
            mark_producer_plan_sha256=(
                facts.mark_producer_plan_sha256
            ),
            marks_sha256=facts.marks_sha256,
        )
        gate = build_advanced_options_input_gate_plan(
            closure,
            window_id=context.window.window_id,
            equity_window_id=facts.equity_window_id,
            instrument_ids=facts.instrument_ids,
            build_id=self._build_id,
            available_at=facts.available_at,
        )
        with context.transaction() as unit:
            for job in gate.jobs:
                self._enqueue_job(unit.cursor, job)
            self._open_barrier(
                unit.cursor,
                barrier_id=gate.barrier_id,
                cohort_id=facts.control_plane.cohort_id,
                window_id=gate.window_id,
                barrier_kind="OPTIONS_INPUT_READY",
                universe_sha256=facts.control_plane.universe.sha256,
                members=gate.barrier_members,
            )
            completed_members = (
                gate.barrier_members
                if closure.has_canonical_producer_evidence
                else tuple(
                    member
                    for member in gate.barrier_members
                    if member.member_key == "EQUITY_INPUT_READY"
                )
            )
            for member in completed_members:
                self._record_barrier_outcome(
                    unit.cursor,
                    barrier_id=gate.barrier_id,
                    member_key=member.member_key,
                    outcome=BarrierOutcome.COMPLETE,
                    outcome_input_sha256=(
                        member.expected_input_sha256
                    ),
                )
            verdict = self._evaluate_barrier(
                unit.cursor,
                gate.barrier_id,
            )
        stage = AdvancedOptionsInputStageData(
            facts.control_plane,
            gate,
            verdict,
            facts.reference_state.reference_generation,
            f"options-input:{gate.barrier_id}:{gate.closure_sha256}",
        )
        self._inputs[self._key(context)] = stage
        return stage

    def cycle(self, context: StageContext) -> AdvancedOptionsCycleStageData:
        input_stage = self._inputs.get(self._key(context))
        if input_stage is None:
            raise RuntimeError(
                "Advanced Options input stage handoff is unavailable"
            )
        facts = self._port.load_cycle_facts(context, input_stage)
        return AdvancedOptionsCycleStageData(
            input_stage.control_plane,
            input_stage.input_gate,
            facts.input_receipt,
            facts.native_inputs.executors(),
            input_stage.reference_generation,
            facts.reference_repository,
            facts.evidence_builder,
            facts.evidence_ref,
            facts.flow_identity,
        )

    def projection(
        self,
        binding: AdvancedOptionsRuntimeBinding,
        context: StageContext,
    ) -> AdvancedOptionsProjectionStageData:
        input_stage = self._inputs.get(self._key(context))
        cycle = binding.handoff.cycle(
            context.plan.cohort_id,
            context.window.cutoff,
        )
        if input_stage is None or cycle is None:
            raise RuntimeError(
                "Advanced Options cycle handoff is unavailable"
            )
        return AdvancedOptionsProjectionStageData(
            input_stage.control_plane,
            cycle,
            f"options-projection:{cycle.cycle_sha256}",
        )

    def publish(
        self,
        binding: AdvancedOptionsRuntimeBinding,
        context: StageContext,
    ) -> AdvancedOptionsPublishStageData:
        input_stage = self._inputs.get(self._key(context))
        projection = binding.handoff.projection(
            context.plan.cohort_id,
            context.window.cutoff,
        )
        if input_stage is None or projection is None:
            raise RuntimeError(
                "Advanced Options projection handoff is unavailable"
            )
        return AdvancedOptionsPublishStageData(
            input_stage.control_plane,
            projection,
            f"options-publish-inert:{projection.batch_sha256}",
        )

    def close(
        self,
        binding: AdvancedOptionsRuntimeBinding,
        context: StageContext,
    ):
        projection = binding.handoff.projection(
            context.plan.cohort_id,
            context.window.cutoff,
        )
        if projection is None:
            raise RuntimeError(
                "Advanced Options close projection is unavailable"
            )
        return binding.close(
            self._port.load_close_data(context, projection)
        )


def build_canonical_advanced_options_runtime_binding(
    *,
    build_id: str,
    pre_session: OptionsPreSessionStageBinding,
    port: AdvancedOptionsCanonicalRuntimePort,
    cycle_writer: Callable | None = None,
    projection_writer: Callable | None = None,
    page_source_loader: Callable | None = None,
) -> tuple[
    AdvancedOptionsRuntimeBinding,
    AdvancedOptionsCanonicalLoaders,
]:
    loaders = AdvancedOptionsCanonicalLoaders(port, build_id=build_id)
    values = {
        "build_id": build_id,
        "pre_session": pre_session,
        "input_loader": loaders.input,
        "cycle_loader": loaders.cycle,
        "projection_loader": lambda context: loaders.projection(
            binding,
            context,
        ),
        "publish_loader": lambda context: loaders.publish(
            binding,
            context,
        ),
    }
    if cycle_writer is not None:
        values["cycle_writer"] = cycle_writer
    if projection_writer is not None:
        values["projection_writer"] = projection_writer
    if page_source_loader is not None:
        values["page_source_loader"] = page_source_loader
    binding = AdvancedOptionsRuntimeBinding(**values)
    return binding, loaders
