"""Non-serving Advanced Options stage composition with injected durable loaders."""
from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from .advanced_journal_registry import AdvancedJournalRegistration
from .advanced_options_close import (
    AdvancedOptionsRetentionHandoff,
    build_advanced_options_close_coverage,
    build_advanced_options_retention_handoff,
)
from .advanced_options_control_plane import AdvancedOptionsControlPlanePlan
from .advanced_options_cycle import (
    AdvancedOptionsCycleResult,
    AdvancedOptionsDatasetFlowIdentity,
    NativeDetectorExecutor,
    run_advanced_options_cycle,
)
from .advanced_options_input import (
    AdvancedOptionsInputGatePlan,
    reconstruct_options_input_ready_receipt,
)
from .advanced_options_package_evidence import (
    AdvancedOptionsFlowPackageEvidenceBatch,
    persist_advanced_options_cycle_with_evidence_cursor,
)
from .advanced_options_projection import (
    AdvancedOptionsPublicationAuthorization,
    AdvancedOptionsProjectionBatch,
    AdvancedOptionsPublisherHandoff,
    authorize_advanced_options_publication,
    build_advanced_options_projection_batch,
    build_advanced_options_publisher_handoff,
    load_advanced_options_page_source_cursor,
    persist_advanced_options_projection_cursor,
)
from .advanced_options_reference_state import (
    AdvancedOptionsReferenceStateRepository,
)
from .materialization_jobs import JobBarrierVerdict
from .options_v2_runtime import OptionsPreSessionStageBinding
from .orchestration_repository import CycleReceiptSpec
from .preflight import DetectorFamily
from .portal_publisher import (
    CanonicalPortalPublicationResult,
    PortalPublicationBatch,
)
from .runner_environment import RunnerEnvironment
from .session_archive import ArchiveIdentity
from .stage_graph import (
    STAGES_V5,
    STAGE_CATALOG_VERSION_V4,
    STAGE_CATALOG_VERSION_V5,
)
from .stage_orchestrator import StageContext, StageOrchestrator
from .stage_runners_options import (
    OptionsPublisherStageResult,
    OptionsReferenceStageResult,
    OptionsReceiptStageResult,
)


EvidenceBuilder = Callable[
    [AdvancedOptionsCycleResult],
    AdvancedOptionsFlowPackageEvidenceBatch,
]


class AdvancedOptionsRuntimeHandoff:
    """Bounded in-process handoff; durable repositories remain authoritative."""

    def __init__(self, capacity: int = 2) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("Advanced Options handoff capacity is invalid")
        self._capacity = capacity
        self._cycles: OrderedDict[
            tuple[UUID, datetime],
            AdvancedOptionsCycleResult,
        ] = OrderedDict()
        self._projections: OrderedDict[
            tuple[UUID, datetime],
            AdvancedOptionsProjectionBatch,
        ] = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def _key(
        cohort_id: UUID,
        cutoff: datetime,
    ) -> tuple[UUID, datetime]:
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("Advanced Options handoff cutoff must be aware")
        return cohort_id, cutoff

    def _put(self, values, key, value) -> None:
        with self._lock:
            values[key] = value
            values.move_to_end(key)
            while len(values) > self._capacity:
                values.popitem(last=False)

    def put_cycle(
        self,
        cohort_id: UUID,
        cutoff: datetime,
        cycle: AdvancedOptionsCycleResult,
    ) -> None:
        self._put(self._cycles, self._key(cohort_id, cutoff), cycle)

    def cycle(
        self,
        cohort_id: UUID,
        cutoff: datetime,
    ) -> AdvancedOptionsCycleResult | None:
        with self._lock:
            return self._cycles.get(self._key(cohort_id, cutoff))

    def put_projection(
        self,
        cohort_id: UUID,
        cutoff: datetime,
        projection: AdvancedOptionsProjectionBatch,
    ) -> None:
        self._put(
            self._projections,
            self._key(cohort_id, cutoff),
            projection,
        )

    def projection(
        self,
        cohort_id: UUID,
        cutoff: datetime,
    ) -> AdvancedOptionsProjectionBatch | None:
        with self._lock:
            return self._projections.get(self._key(cohort_id, cutoff))


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputStageData:
    control_plane: AdvancedOptionsControlPlanePlan
    input_gate: AdvancedOptionsInputGatePlan
    barrier_verdict: JobBarrierVerdict
    reference_generation: int
    evidence_ref: str


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCycleStageData:
    control_plane: AdvancedOptionsControlPlanePlan
    input_gate: AdvancedOptionsInputGatePlan
    input_receipt: CycleReceiptSpec
    executors: Mapping[DetectorFamily, NativeDetectorExecutor]
    reference_generation: int
    reference_repository: AdvancedOptionsReferenceStateRepository
    evidence_builder: EvidenceBuilder
    evidence_ref: str
    flow_identity: AdvancedOptionsDatasetFlowIdentity | None = None


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProjectionStageData:
    control_plane: AdvancedOptionsControlPlanePlan
    cycle: AdvancedOptionsCycleResult
    evidence_ref: str


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPublishStageData:
    control_plane: AdvancedOptionsControlPlanePlan
    projection: AdvancedOptionsProjectionBatch
    evidence_ref: str


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCloseStageData:
    control_plane: AdvancedOptionsControlPlanePlan
    input_gate: AdvancedOptionsInputGatePlan
    projection: AdvancedOptionsProjectionBatch
    journal: AdvancedJournalRegistration
    flow_checkpoint_offset: int
    market_state_checkpoint_offset: int
    unresolved_gap_ids: tuple[UUID, ...]
    slot_coverage_sha256: str
    oi_coverage_sha256: str
    archive_identity: ArchiveIdentity
    archives_verified: bool
    open_alert_ids: tuple[UUID, ...]
    journal_retention_expires_at: datetime
    checked_at: datetime
    selected_alert_ids: tuple[UUID, ...] = ()
    active_hold_refs: tuple[str, ...] = ()


InputLoader = Callable[[StageContext], AdvancedOptionsInputStageData]
CycleLoader = Callable[[StageContext], AdvancedOptionsCycleStageData]
ProjectionLoader = Callable[[StageContext], AdvancedOptionsProjectionStageData]
PublishLoader = Callable[[StageContext], AdvancedOptionsPublishStageData]
ReferenceLoader = Callable[[StageContext], OptionsReferenceStageResult]
PublicationAuthorityLoader = Callable[
    [StageContext, AdvancedOptionsPublisherHandoff],
    AdvancedOptionsPublicationAuthorization,
]
CanonicalPublisher = Callable[
    [PortalPublicationBatch],
    CanonicalPortalPublicationResult,
]


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRuntimeBinding:
    build_id: str
    pre_session: OptionsPreSessionStageBinding
    input_loader: InputLoader
    cycle_loader: CycleLoader
    projection_loader: ProjectionLoader
    publish_loader: PublishLoader
    catalog_version: str = STAGE_CATALOG_VERSION_V4
    reference_loader: ReferenceLoader | None = None
    publication_authority_loader: PublicationAuthorityLoader | None = None
    canonical_publisher: CanonicalPublisher | None = None
    cycle_writer: Callable[..., None] = (
        persist_advanced_options_cycle_with_evidence_cursor
    )
    projection_writer: Callable[..., None] = (
        persist_advanced_options_projection_cursor
    )
    page_source_loader: Callable = (
        load_advanced_options_page_source_cursor
    )
    handoff: AdvancedOptionsRuntimeHandoff = field(
        default_factory=AdvancedOptionsRuntimeHandoff
    )

    def __post_init__(self) -> None:
        if not self.build_id.strip():
            raise ValueError("Advanced Options runtime build ID cannot be blank")
        if self.catalog_version not in {
            STAGE_CATALOG_VERSION_V4,
            STAGE_CATALOG_VERSION_V5,
        }:
            raise ValueError(
                "Advanced Options runtime catalog version is invalid"
            )
        for value, name in (
            (self.input_loader, "input loader"),
            (self.cycle_loader, "cycle loader"),
            (self.projection_loader, "projection loader"),
            (self.publish_loader, "publish loader"),
            (self.cycle_writer, "cycle writer"),
            (self.projection_writer, "projection writer"),
            (self.page_source_loader, "page source loader"),
        ):
            if not callable(value):
                raise TypeError(f"Advanced Options runtime {name} is invalid")
        if self.catalog_version == STAGE_CATALOG_VERSION_V5 and any(
            not callable(value)
            for value in (
                self.reference_loader,
                self.publication_authority_loader,
                self.canonical_publisher,
            )
        ):
            raise ValueError(
                "Advanced Options V5 runtime composition is incomplete"
            )

    def _validate_scope(
        self,
        context: StageContext,
        control_plane: AdvancedOptionsControlPlanePlan,
    ) -> None:
        if (
            context.plan.stage_catalog_version != self.catalog_version
            or context.plan.cohort_id != control_plane.cohort_id
            or context.plan.session_date != control_plane.session_date
            or context.window.plan_id != context.plan.plan_id
            or context.window.cohort_id != control_plane.cohort_id
        ):
            raise ValueError("Advanced Options runtime scope changed")

    def reference(
        self,
        context: StageContext,
    ) -> OptionsReferenceStageResult:
        if (
            self.catalog_version != STAGE_CATALOG_VERSION_V5
            or self.reference_loader is None
        ):
            raise ValueError(
                "Advanced Options reference stage is not V5-bound"
            )
        result = self.reference_loader(context)
        if not isinstance(result, OptionsReferenceStageResult):
            raise TypeError(
                "Advanced Options reference loader returned invalid data"
            )
        return result

    def input_gate(self, context: StageContext) -> OptionsReceiptStageResult:
        data = self.input_loader(context)
        self._validate_scope(context, data.control_plane)
        if (
            data.input_gate.closure.control_plane_plan_id
            != data.control_plane.plan_id
        ):
            raise ValueError("Advanced Options input gate scope changed")
        receipt = reconstruct_options_input_ready_receipt(
            data.input_gate,
            data.barrier_verdict,
            cutoff=context.window.cutoff,
            reference_generation=data.reference_generation,
            build_id=self.build_id,
        )
        return OptionsReceiptStageResult(receipt, data.evidence_ref)

    def cycle(self, context: StageContext) -> OptionsReceiptStageResult:
        data = self.cycle_loader(context)
        self._validate_scope(context, data.control_plane)
        required = set(data.control_plane.manifest_policy.required_detectors)
        required.discard(DetectorFamily.O2)
        if (
            data.input_receipt.build_id != self.build_id
            or set(data.executors) != required
        ):
            raise ValueError(
                "Advanced Options cycle receipt or executor membership changed"
            )
        cycle = run_advanced_options_cycle(
            control_plane=data.control_plane,
            input_gate=data.input_gate,
            input_receipt=data.input_receipt,
            executors=data.executors,
            cutoff=context.window.cutoff,
            reference_generation=data.reference_generation,
            reference_repository=data.reference_repository,
            build_id=self.build_id,
        )
        evidence = data.evidence_builder(cycle)
        with context.transaction() as unit:
            self.cycle_writer(
                unit.cursor,
                cycle=cycle,
                control_plane=data.control_plane,
                evidence_batch=evidence,
                flow_identity=data.flow_identity,
            )
        self.handoff.put_cycle(
            data.control_plane.cohort_id,
            context.window.cutoff,
            cycle,
        )
        return OptionsReceiptStageResult(cycle.receipt, data.evidence_ref)

    def projection(self, context: StageContext) -> OptionsReceiptStageResult:
        data = self.projection_loader(context)
        self._validate_scope(context, data.control_plane)
        with context.transaction() as unit:
            page_source = self.page_source_loader(
                unit.cursor,
                control_plane=data.control_plane,
                cycle=data.cycle,
                generated_at=context.window.cutoff,
            )
            batch = build_advanced_options_projection_batch(
                control_plane=data.control_plane,
                cycle=data.cycle,
                generated_at=context.window.cutoff,
                build_id=self.build_id,
                page_source=page_source,
            )
            self.projection_writer(unit.cursor, batch)
        self.handoff.put_projection(
            data.control_plane.cohort_id,
            context.window.cutoff,
            batch,
        )
        return OptionsReceiptStageResult(
            batch.portal_ready_receipt,
            data.evidence_ref,
        )

    def publish(self, context: StageContext) -> OptionsPublisherStageResult:
        data = self.publish_loader(context)
        self._validate_scope(context, data.control_plane)
        handoff = build_advanced_options_publisher_handoff(
            control_plane=data.control_plane,
            batch=data.projection,
        )
        if self.catalog_version == STAGE_CATALOG_VERSION_V4:
            if any(
                (
                    handoff.dispatch_authorized,
                    handoff.release_authorized,
                    handoff.pointer_authorized,
                )
            ):
                raise ValueError(
                    "Advanced Options V4 inert publisher gained authority"
                )
            return OptionsPublisherStageResult(
                handoff.batch_sha256,
                data.evidence_ref,
            )
        if (
            self.publication_authority_loader is None
            or self.canonical_publisher is None
        ):
            raise ValueError(
                "Advanced Options V5 publication is not composed"
            )
        authorization = self.publication_authority_loader(
            context,
            handoff,
        )
        authorized = authorize_advanced_options_publication(
            handoff,
            authorization,
        )
        result = self.canonical_publisher(
            PortalPublicationBatch(
                authorized.snapshots,
                receipt=authorized.receipt,
            ),
            checked_at=max(
                context.started_at,
                authorized.receipt.cutoff,
            ),
        )
        if (
            not isinstance(result, CanonicalPortalPublicationResult)
            or str(authorized.receipt.receipt_id)
            not in {
                str(value)
                for value in result.release.manifest.get(
                    "source_receipt_ids",
                    (result.release.receipt_id,),
                )
            }
            or result.pointer is None
            or result.release.release_id != result.pointer.release_id
        ):
            raise ValueError(
                "Advanced Options canonical publication result changed"
            )
        return OptionsPublisherStageResult(
            authorized.batch_sha256,
            (
                f"options-publish:{result.release.release_id}:"
                f"{result.release.manifest_sha256}"
            ),
            release_authorized=True,
            pointer_authorized=True,
            release_id=result.release.release_id,
            pointer_release_id=result.pointer.release_id,
        )

    def close(
        self,
        data: AdvancedOptionsCloseStageData,
    ) -> AdvancedOptionsRetentionHandoff:
        coverage = build_advanced_options_close_coverage(
            control_plane=data.control_plane,
            input_gate=data.input_gate,
            projection=data.projection,
            journal=data.journal,
            flow_checkpoint_offset=data.flow_checkpoint_offset,
            market_state_checkpoint_offset=data.market_state_checkpoint_offset,
            unresolved_gap_ids=data.unresolved_gap_ids,
            slot_coverage_sha256=data.slot_coverage_sha256,
            oi_coverage_sha256=data.oi_coverage_sha256,
            checked_at=data.checked_at,
        )
        return build_advanced_options_retention_handoff(
            control_plane=data.control_plane,
            close_coverage=coverage,
            archive_identity=data.archive_identity,
            archives_verified=data.archives_verified,
            open_alert_ids=data.open_alert_ids,
            journal_retention_expires_at=(
                data.journal_retention_expires_at
            ),
            checked_at=data.checked_at,
            selected_alert_ids=data.selected_alert_ids,
            active_hold_refs=data.active_hold_refs,
        )

    def bind(self, environment: RunnerEnvironment) -> RunnerEnvironment:
        action_names = (
            "options_acquire_action",
            "options_admission_action",
            "options_reference_action",
            "options_input_gate_action",
            "options_cycle_action",
            "options_projection_action",
            "options_publish_action",
        )
        if any(getattr(environment, name) is not None for name in action_names):
            raise ValueError("Advanced Options runtime actions are already bound")
        environment.options_acquire_action = self.pre_session.acquire
        environment.options_admission_action = self.pre_session.admit
        if self.catalog_version == STAGE_CATALOG_VERSION_V5:
            environment.options_reference_action = self.reference
        environment.options_input_gate_action = self.input_gate
        environment.options_cycle_action = self.cycle
        environment.options_projection_action = self.projection
        environment.options_publish_action = self.publish
        return environment


@dataclass(frozen=True, slots=True)
class AdvancedOptionsV3RuntimeComposition:
    binding: AdvancedOptionsRuntimeBinding
    environment: RunnerEnvironment
    runners: Mapping[str, object]
    orchestrator: StageOrchestrator


def build_advanced_options_v3_runtime_composition(
    *,
    binding: AdvancedOptionsRuntimeBinding,
    repository,
    database,
    environment: RunnerEnvironment | None = None,
) -> AdvancedOptionsV3RuntimeComposition:
    if binding.catalog_version != STAGE_CATALOG_VERSION_V5:
        raise ValueError(
            "Advanced Options V3 composition requires stages_v5"
        )
    from .advanced_options_service import OPTIONS_V3_STAGE_ORDER
    from .stage_runners_v2 import build_advanced_options_v3_runners

    bound = binding.bind(environment or RunnerEnvironment(database))
    runners = build_advanced_options_v3_runners(
        database,
        environment=bound,
    )
    if tuple(runners) != OPTIONS_V3_STAGE_ORDER:
        raise ValueError(
            "Advanced Options V3 runtime stage order changed"
        )
    orchestrator = StageOrchestrator(
        repository,
        runners,
        database=database,
        stages=STAGES_V5,
        catalog_version=STAGE_CATALOG_VERSION_V5,
        parallel_lanes=False,
    )
    return AdvancedOptionsV3RuntimeComposition(
        binding,
        bound,
        runners,
        orchestrator,
    )
