"""PostgreSQL canonical port for Advanced Options non-serving runtime inputs."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from psycopg2 import Error as PsycopgError
from psycopg2.extras import RealDictCursor

from .advanced_options_cycle import (
    AdvancedOptionsCycleResult,
    AdvancedOptionsDatasetFlowIdentity,
    load_advanced_options_cycle_cursor,
)
from .advanced_options_input import (
    AdvancedOptionsInputDependencyEvidence,
    AdvancedOptionsInputDependencyStatus,
    AdvancedOptionsInputReadyBlocked,
    AdvancedOptionsInputReadyPlan,
    AdvancedOptionsInputReadyPreflight,
    AdvancedOptionsInputReadyResult,
    _validate_input_ready_plan,
    build_advanced_options_input_closure,
    build_advanced_options_input_gate_plan,
    reconstruct_options_input_ready_receipt,
)
from .advanced_options_loaders import (
    AdvancedOptionsCanonicalCycleFacts,
    AdvancedOptionsCanonicalInputFacts,
    AdvancedOptionsCanonicalRuntimePort,
)
from .advanced_options_mark_producer import (
    AdvancedOptionsMarkPreflight,
    AdvancedOptionsMarkProducerPlan,
    AdvancedOptionsMarkStatus,
    PostgresAdvancedOptionsMarkProducer,
    assemble_advanced_options_marks_cursor,
)
from .advanced_options_native_adapters import AdvancedOptionsNativeInputBundle
from .advanced_options_native_loader import (
    AdvancedOptionsNativeInputPlan,
    AdvancedOptionsNativeInputPreflight,
    NativeInputPlanStatus,
    PostgresAdvancedOptionsNativeInputLoader,
    assemble_advanced_options_native_inputs_cursor,
)
from .advanced_options_producers import AdvancedOptionsProducerPlan
from .advanced_options_reference_producer import (
    AdvancedOptionsReferenceProducerPlan,
    AdvancedOptionsReferenceProducerPreflight,
    PostgresAdvancedOptionsReferenceProducer,
    ReferenceProducerStatus,
    assemble_advanced_options_reference_state_cursor,
)
from .advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
    decode_advanced_options_reference_state,
)
from .advanced_options_rolling_producer import (
    AdvancedOptionsRollingProducerPlan,
    AdvancedOptionsRollingProducerPreflight,
    PostgresAdvancedOptionsRollingProducer,
    RollingProducerStatus,
    assemble_advanced_options_rolling_state_cursor,
)
from .advanced_options_rolling_repository import (
    PostgresAdvancedOptionsRollingStateRepository,
    decode_advanced_options_rolling_state,
)
from .advanced_options_rolling_state import RollingAvailability
from .advanced_options_runtime import (
    AdvancedOptionsCloseStageData,
    EvidenceBuilder,
)
from .advanced_options_projection import (
    AdvancedOptionsProjectionBatch,
    load_advanced_options_projection_batch_cursor,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .materialization_jobs import (
    BarrierOutcome,
    JobBarrierVerdict,
    enqueue_materialization_job_cursor,
    evaluate_job_barrier_cursor,
    open_job_barrier_cursor,
    record_job_barrier_outcome_cursor,
)
from .option_cycle_input import (
    OptionCycleInput,
    OptionCycleUnderlying,
    verify_option_cycle_input_against_tables,
)
from .option_discovery_verify import read_discovery_batches
from .option_slot_set_repository import load_active_slot_set_cursor
from .orchestration_repository import (
    OPTIONS_CYCLE,
    OPTIONS_INPUT_READY,
    OPTIONS_PORTAL_READY,
    CycleReceiptSpec,
    ServicePlan,
    WindowRecord,
    persist_cycle_receipt_cursor,
)
from .preflight import ProviderTier
from .stage_orchestrator import StageContext


NativeInputsLoader = Callable[
    [StageContext],
    AdvancedOptionsNativeInputBundle,
]
CloseDataLoader = Callable[
    [StageContext, object],
    AdvancedOptionsCloseStageData,
]
_RETRYABLE_POSTGRES_SQLSTATES = frozenset({"40001", "40P01"})


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPostgresRuntimeScope:
    control_plane: object
    producer_plan: AdvancedOptionsProducerPlan
    code_sha256: str
    native_inputs_loader: NativeInputsLoader | None
    evidence_builder: EvidenceBuilder
    close_data_loader: CloseDataLoader
    flow_identity: AdvancedOptionsDatasetFlowIdentity | None = None
    require_discovery_pages: bool = True
    reference_producer_plan: (
        AdvancedOptionsReferenceProducerPlan | None
    ) = None
    rolling_producer_plan: AdvancedOptionsRollingProducerPlan | None = None
    native_input_plan: AdvancedOptionsNativeInputPlan | None = None
    mark_producer_plan: AdvancedOptionsMarkProducerPlan | None = None
    input_ready_plan: AdvancedOptionsInputReadyPlan | None = None

    def __post_init__(self) -> None:
        from .advanced_options_control_plane import (
            AdvancedOptionsControlPlanePlan,
        )

        if (
            not isinstance(
                self.control_plane,
                AdvancedOptionsControlPlanePlan,
            )
            or self.producer_plan.control_plane_plan_id
            != self.control_plane.plan_id
            or (
                (self.native_inputs_loader is None)
                == (self.native_input_plan is None)
            )
            or (
                self.native_inputs_loader is not None
                and not callable(self.native_inputs_loader)
            )
            or not callable(self.evidence_builder)
            or not callable(self.close_data_loader)
            or (
                self.reference_producer_plan is not None
                and (
                    not isinstance(
                        self.reference_producer_plan,
                        AdvancedOptionsReferenceProducerPlan,
                    )
                    or self.reference_producer_plan.control_plane.plan_id
                    != self.control_plane.plan_id
                )
            )
            or (
                self.rolling_producer_plan is not None
                and (
                    not isinstance(
                        self.rolling_producer_plan,
                        AdvancedOptionsRollingProducerPlan,
                    )
                    or self.rolling_producer_plan.control_plane.plan_id
                    != self.control_plane.plan_id
                )
            )
            or (
                self.native_input_plan is not None
                and (
                    not isinstance(
                        self.native_input_plan,
                        AdvancedOptionsNativeInputPlan,
                    )
                    or self.native_input_plan.control_plane.plan_id
                    != self.control_plane.plan_id
                    or self.native_input_plan.producer_plan.plan_id
                    != self.producer_plan.plan_id
                )
            )
            or (
                self.mark_producer_plan is not None
                and (
                    not isinstance(
                        self.mark_producer_plan,
                        AdvancedOptionsMarkProducerPlan,
                    )
                    or self.mark_producer_plan.control_plane.plan_id
                    != self.control_plane.plan_id
                )
            )
            or (
                self.input_ready_plan is not None
                and (
                    not isinstance(
                        self.input_ready_plan,
                        AdvancedOptionsInputReadyPlan,
                    )
                    or self.reference_producer_plan
                    != self.input_ready_plan.reference_producer_plan
                    or self.rolling_producer_plan
                    != self.input_ready_plan.rolling_producer_plan
                    or self.native_input_plan
                    != self.input_ready_plan.native_input_plan
                    or self.mark_producer_plan
                    != self.input_ready_plan.mark_producer_plan
                    or self.producer_plan
                    != self.input_ready_plan.producer_plan
                    or self.control_plane
                    != self.input_ready_plan.control_plane
                )
            )
        ):
            raise ValueError(
                "Advanced Options PostgreSQL runtime scope is invalid"
            )
        object.__setattr__(
            self,
            "code_sha256",
            _sha256(self.code_sha256, "code_sha256"),
        )
        if self.input_ready_plan is not None:
            _validate_input_ready_plan(self.input_ready_plan)


@dataclass(frozen=True, slots=True)
class _AdvancedOptionsInputReadyContext:
    plan: ServicePlan
    window: WindowRecord


class PostgresAdvancedOptionsCanonicalRuntimePort(
    AdvancedOptionsCanonicalRuntimePort
):
    def __init__(
        self,
        provider: V2ConnectionProvider,
        scope: AdvancedOptionsPostgresRuntimeScope,
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("Advanced Options PostgreSQL provider is invalid")
        if not isinstance(scope, AdvancedOptionsPostgresRuntimeScope):
            raise TypeError("Advanced Options PostgreSQL scope is invalid")
        self._provider = provider
        self._scope = scope
        self._reference = PostgresAdvancedOptionsReferenceStateRepository(
            provider
        )
        self._rolling = PostgresAdvancedOptionsRollingStateRepository(
            provider
        )
        self._reference_producer = (
            None
            if scope.reference_producer_plan is None
            else PostgresAdvancedOptionsReferenceProducer(
                provider,
                scope.reference_producer_plan,
            )
        )
        self._rolling_producer = (
            None
            if scope.rolling_producer_plan is None
            else PostgresAdvancedOptionsRollingProducer(
                provider,
                scope.rolling_producer_plan,
            )
        )
        self._native_input_loader = (
            None
            if scope.native_input_plan is None
            else PostgresAdvancedOptionsNativeInputLoader(
                provider,
                scope.native_input_plan,
            )
        )
        self._mark_producer = (
            None
            if scope.mark_producer_plan is None
            else PostgresAdvancedOptionsMarkProducer(
                provider,
                scope.mark_producer_plan,
            )
        )

    def preflight_reference_state(
        self,
    ) -> AdvancedOptionsReferenceProducerPreflight:
        if self._reference_producer is None:
            raise ValueError(
                "Advanced Options reference producer plan is not selected"
            )
        return self._reference_producer.preflight()

    def produce_reference_state(self):
        if self._reference_producer is None:
            raise ValueError(
                "Advanced Options reference producer plan is not selected"
            )
        return self._reference_producer.produce()

    def preflight_rolling_state(
        self,
    ) -> AdvancedOptionsRollingProducerPreflight:
        if self._rolling_producer is None:
            raise ValueError(
                "Advanced Options rolling producer plan is not selected"
            )
        return self._rolling_producer.preflight()

    def produce_rolling_state(self):
        if self._rolling_producer is None:
            raise ValueError(
                "Advanced Options rolling producer plan is not selected"
            )
        return self._rolling_producer.produce()

    def preflight_native_inputs(
        self,
    ) -> AdvancedOptionsNativeInputPreflight:
        if self._native_input_loader is None:
            raise ValueError(
                "Advanced Options native input PLAN is not selected"
            )
        return self._native_input_loader.preflight()

    def preflight_option_marks(self) -> AdvancedOptionsMarkPreflight:
        if self._mark_producer is None:
            raise ValueError(
                "Advanced Options mark producer PLAN is not selected"
            )
        return self._mark_producer.preflight()

    def produce_option_marks(self):
        if self._mark_producer is None:
            raise ValueError(
                "Advanced Options mark producer PLAN is not selected"
            )
        return self._mark_producer.produce()

    def _verify_control_plane_cursor(
        self,
        cursor,
        context: StageContext,
    ) -> None:
        control = self._scope.control_plane
        cursor.execute(
            "SELECT cohort.cohort_sha256,cohort.manifest_id,"
            "cohort.configuration_sha256,cohort.model_bundle_sha256,"
            "cohort.valuation_sha256,cohort.mode,"
            "manifest.manifest_sha256,manifest.universe_sha256,"
            "manifest.session_date,manifest.profile_id,"
            "manifest.manifest_kind,manifest.option_underlying_count,"
            "manifest.allocation_policy_sha256,profile.profile_sha256,"
            "profile.provider_tier "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest "
            "ON manifest.manifest_id=cohort.manifest_id "
            "JOIN rt.capability_profiles AS profile "
            "ON profile.profile_id=manifest.profile_id "
            "WHERE cohort.cohort_id=%s",
            (str(control.cohort_id),),
        )
        row = cursor.fetchone()
        expected = (
            control.cohort_sha256,
            str(control.manifest_id),
            (
                context.plan.config_sha256
                if control.stored_identity is None
                else control.stored_identity.configuration_sha256
            ),
            control.pins.model_bundle_sha256,
            control.pins.valuation_policy_sha256,
            "SHADOW" if control.stored_identity is None else control.stored_identity.mode,
            control.manifest_sha256,
            control.universe.sha256,
            control.session_date,
            str(control.profile_id),
            "MARKET_OPTIONS",
            len(control.universe.option_underlyings),
            (
                control.manifest_policy.sha256
                if control.stored_identity is None
                else control.stored_identity.allocation_policy_sha256
            ),
            control.profile_sha256,
            ProviderTier.MASSIVE_ADVANCED.value,
        )
        if row is None or tuple(str(value) if isinstance(value, UUID) else value for value in row) != expected:
            raise ValueError(
                "stored Advanced Options control-plane identity changed"
            )

    def _cycle_input_cursor(
        self,
        cursor,
        context: StageContext,
    ) -> OptionCycleInput:
        control = self._scope.control_plane
        cursor.execute(
            "SELECT discovery_batch_id FROM rt.option_discovery_batches "
            "WHERE profile_id=%s AND provider_tier=%s "
            "AND scheduled_cycle=%s AND policy_sha256=%s "
            "AND underlyer=ANY(%s) "
            "ORDER BY underlyer,discovery_batch_id",
            (
                str(control.profile_id),
                ProviderTier.MASSIVE_ADVANCED.value,
                context.window.cutoff,
                control.manifest_policy.sha256,
                list(control.universe.option_underlyings),
            ),
        )
        batch_ids = tuple(UUID(str(row[0])) for row in cursor.fetchall())
        stored = read_discovery_batches(cursor, batch_ids)
        by_underlyer = {}
        for batch in stored.values():
            if batch.underlyer in by_underlyer:
                raise ValueError(
                    "multiple discovery batches match one Options underlyer"
                )
            if (
                self._scope.require_discovery_pages
                and len(batch.pages) != batch.page_count
            ):
                raise ValueError(
                    "Advanced Options discovery replay pages are incomplete"
                )
            by_underlyer[batch.underlyer] = batch
        if set(by_underlyer) != set(control.universe.option_underlyings):
            raise ValueError(
                "Advanced Options discovery batch membership is incomplete"
            )
        active = load_active_slot_set_cursor(cursor, control.cohort_id)
        if (
            active is None
            or active.slot_set.version != control.slot_set.version
            or active.slot_set.member_sha256
            != control.slot_set.member_sha256
        ):
            raise ValueError("Advanced Options active slot set changed")
        entries = tuple(
            OptionCycleUnderlying(
                underlyer=underlyer,
                underlying_security_id=(
                    by_underlyer[underlyer].underlying_security_id
                ),
                discovery_batch_id=(
                    by_underlyer[underlyer].discovery_batch_id
                ),
                discovery_evidence_sha256=(
                    by_underlyer[underlyer].evidence_sha256
                ),
                payload_chain_sha256=(
                    by_underlyer[underlyer].payload_chain_digest
                ),
                coverage_sha256=(
                    by_underlyer[underlyer].coverage_digest
                ),
                request_filter_sha256=(
                    by_underlyer[underlyer].request_filter_sha256
                ),
                page_count=by_underlyer[underlyer].page_count,
                row_count=by_underlyer[underlyer].row_count,
                response_bytes=by_underlyer[underlyer].response_bytes,
                expiration_from=by_underlyer[underlyer].expiration_from,
                expiration_through=(
                    by_underlyer[underlyer].expiration_through
                ),
                strike_min=by_underlyer[underlyer].strike_min,
                strike_max=by_underlyer[underlyer].strike_max,
            )
            for underlyer in control.universe.option_underlyings
        )
        cycle_input = OptionCycleInput(
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            scheduled_cycle=context.window.cutoff,
            profile_id=control.profile_id,
            profile_sha256=control.profile_sha256,
            universe_sha256=control.universe.sha256,
            policy_sha256=control.manifest_policy.sha256,
            configuration_sha256=context.plan.config_sha256,
            code_sha256=self._scope.code_sha256,
            cohort_id=control.cohort_id,
            underlyings=entries,
            slot_set_version=active.slot_set.version,
            slot_set_member_sha256=active.slot_set.member_sha256,
        )
        verify_option_cycle_input_against_tables(
            cursor,
            cycle_input,
            require_pages=self._scope.require_discovery_pages,
        )
        return cycle_input

    @staticmethod
    def _equity_receipt_cursor(
        cursor,
        context: StageContext,
        input_ready_plan: AdvancedOptionsInputReadyPlan | None = None,
    ) -> tuple[UUID, UUID, CycleReceiptSpec]:
        if input_ready_plan is not None:
            expected_plan = input_ready_plan.native_input_plan
            expected = expected_plan.equity_input_receipt
            cursor.execute(
                "SELECT receipt.receipt_id,receipt.window_id,"
                "receipt.cohort_id,receipt.cutoff,"
                "receipt.reference_generation,receipt.universe_sha256,"
                "receipt.input_sha256,receipt.build_id "
                "FROM rt.cycle_receipts AS receipt "
                "WHERE receipt.receipt_id=%s "
                "AND receipt.receipt_kind='INPUT_READY'",
                (str(expected_plan.equity_input_receipt_id),),
            )
            row = cursor.fetchone()
            if row is None or (
                UUID(str(row[0])),
                UUID(str(row[1])),
                UUID(str(row[2])),
                row[3],
                int(row[4]),
                str(row[5]),
                str(row[6]),
                str(row[7]),
            ) != (
                expected_plan.equity_input_receipt_id,
                expected_plan.equity_window_id,
                input_ready_plan.enrollment.equity_plan.cohort_id,
                expected.cutoff,
                expected.reference_generation,
                expected.universe_sha256,
                expected.input_sha256,
                expected.build_id,
            ):
                raise ValueError(
                    "exact Equity INPUT_READY receipt identity changed"
                )
            return (
                expected_plan.equity_input_receipt_id,
                expected_plan.equity_window_id,
                expected,
            )
        cursor.execute(
            "SELECT receipt.receipt_id,receipt.window_id,receipt.cutoff,"
            "receipt.reference_generation,receipt.universe_sha256,"
            "receipt.input_sha256,receipt.build_id "
            "FROM rt.cycle_receipts AS receipt "
            "WHERE receipt.cohort_id=%s AND receipt.receipt_kind='INPUT_READY' "
            "AND receipt.cutoff=%s ORDER BY receipt.window_id,receipt.receipt_id",
            (str(context.plan.cohort_id), context.window.cutoff),
        )
        rows = tuple(cursor.fetchall())
        if not rows:
            raise ValueError(
                "Advanced Options requires an exact Equity INPUT_READY receipt"
            )
        identities = {
            (
                row[2],
                int(row[3]),
                str(row[4]),
                str(row[5]),
                str(row[6]),
            )
            for row in rows
        }
        if len(identities) != 1:
            raise ValueError(
                "durable Equity INPUT_READY receipts disagree"
            )
        row = rows[0]
        return (
            UUID(str(row[0])),
            UUID(str(row[1])),
            CycleReceiptSpec(
                "INPUT_READY",
                row[2],
                int(row[3]),
                str(row[4]),
                str(row[5]),
                str(row[6]),
            ),
        )

    @staticmethod
    def _instrument_ids_cursor(
        cursor,
        control,
    ) -> dict[str, int]:
        cursor.execute(
            "SELECT DISTINCT ON (security.security_id) "
            "revision.ticker,instrument.instrument_id "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument "
            "ON instrument.instrument_id=member.instrument_id "
            "JOIN ref.securities AS security "
            "ON security.security_id=instrument.security_id "
            "JOIN ref.security_revisions AS revision "
            "ON revision.security_id=security.security_id "
            "WHERE member.manifest_id=%s AND member.option_underlying "
            "ORDER BY security.security_id,revision.effective_from DESC,"
            "revision.observed_at DESC",
            (str(control.manifest_id),),
        )
        values = {str(row[0]): int(row[1]) for row in cursor.fetchall()}
        if set(values) != set(control.universe.option_underlyings):
            raise ValueError(
                "Advanced Options underlying instrument membership changed"
            )
        return values

    @staticmethod
    def _unresolved_gaps_sha256(cursor, control) -> str:
        cursor.execute(
            "SELECT gap_id::text,session_id::text,instrument_id,channel,"
            "gap_start,gap_end,reason,last_durable_offset,recovery_status "
            "FROM rt.stream_gaps WHERE session_id=ANY(%s::uuid[]) "
            "AND recovery_status IN ('UNRESOLVED','WARMING') "
            "ORDER BY gap_start,gap_id",
            (
                [
                    str(value)
                    for value in control.source_flow.stream_session_ids
                ],
            ),
        )
        return canonical_sha256(tuple(tuple(row) for row in cursor.fetchall()))

    @staticmethod
    def _market_state_heads_sha256(cursor, control) -> str:
        instrument_ids = sorted({
            member.instrument_id for member in control.slot_set.members
        })
        cursor.execute(
            "SELECT 'Q',instrument_id,journal_generation::text,"
            "source_offset,payload_sha256 FROM rt.quote_current "
            "WHERE provider='massive' AND instrument_id=ANY(%s) "
            "UNION ALL "
            "SELECT 'T',instrument_id,journal_generation::text,"
            "source_offset,payload_sha256 FROM rt.trade_current "
            "WHERE provider='massive' AND instrument_id=ANY(%s) "
            "ORDER BY 1,2",
            (instrument_ids, instrument_ids),
        )
        return canonical_sha256(tuple(tuple(row) for row in cursor.fetchall()))

    def _reference_state_cursor(self, cursor):
        control = self._scope.control_plane
        input_plan = self._scope.input_ready_plan
        if input_plan is not None:
            expected = input_plan.native_input_plan.reference_state
            cursor.execute(
                "SELECT state_id,state_sha256,state_document FROM "
                "rt.advanced_options_reference_states WHERE state_id=%s",
                (str(expected.state_id),),
            )
            row = cursor.fetchone()
            reference = (
                None
                if row is None
                else decode_advanced_options_reference_state(
                    state_id=UUID(str(row[0])),
                    state_sha256=str(row[1]),
                    document=row[2],
                )
            )
            if reference != expected:
                raise ValueError(
                    "exact Advanced Options reference generation changed"
                )
            return reference
        cursor.execute(
            "SELECT state.state_id,state.state_sha256,state.state_document "
            "FROM rt.advanced_options_reference_session_freezes "
            "AS session_freeze JOIN "
            "rt.advanced_options_reference_states AS state USING(state_id) "
            "WHERE session_freeze.session_date=%s "
            "AND state.known_at<=%s AND state.valid_until>%s",
            (
                control.session_date,
                self._scope.producer_plan.clocks.known_at,
                self._scope.producer_plan.clocks.known_at,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                "SELECT state_id,state_sha256,state_document FROM "
                "rt.advanced_options_reference_states "
                "WHERE session_date=%s AND known_at<=%s "
                "AND valid_until>%s ORDER BY reference_generation DESC "
                "LIMIT 1",
                (
                    control.session_date,
                    self._scope.producer_plan.clocks.known_at,
                    self._scope.producer_plan.clocks.known_at,
                ),
            )
            row = cursor.fetchone()
        return (
            None
            if row is None
            else decode_advanced_options_reference_state(
                state_id=UUID(str(row[0])),
                state_sha256=str(row[1]),
                document=row[2],
            )
        )

    def _rolling_state_cursor(self, cursor):
        control = self._scope.control_plane
        input_plan = self._scope.input_ready_plan
        if input_plan is not None:
            expected = input_plan.native_input_plan.rolling_state
            cursor.execute(
                "SELECT state_id,state_sha256,state_document FROM "
                "rt.advanced_options_rolling_checkpoints WHERE state_id=%s",
                (str(expected.state_id),),
            )
            row = cursor.fetchone()
            rolling = (
                None
                if row is None
                else decode_advanced_options_rolling_state(
                    state_id=UUID(str(row[0])),
                    state_sha256=str(row[1]),
                    document=row[2],
                )
            )
            if rolling != expected:
                raise ValueError(
                    "exact Advanced Options rolling generation changed"
                )
            return rolling
        cursor.execute(
            "SELECT state_id,state_sha256,state_document FROM "
            "rt.advanced_options_rolling_checkpoints "
            "WHERE cohort_id=%s AND session_date=%s "
            "AND journal_generation=%s "
            "ORDER BY owner_epoch DESC,committed_offset DESC LIMIT 1",
            (
                str(control.cohort_id),
                control.session_date,
                str(control.source_flow.journal_generation),
            ),
        )
        row = cursor.fetchone()
        return (
            None
            if row is None
            else decode_advanced_options_rolling_state(
                state_id=UUID(str(row[0])),
                state_sha256=str(row[1]),
                document=row[2],
            )
        )

    def _load_input_facts_unit(
        self,
        context,
        unit,
    ) -> AdvancedOptionsCanonicalInputFacts:
        control = self._scope.control_plane
        if (
            context.plan.cohort_id != control.cohort_id
            or context.plan.session_date != control.session_date
            or context.window.cutoff
            != self._scope.producer_plan.clocks.market_cutoff
        ):
            raise ValueError("Advanced Options PostgreSQL context changed")
        cursor = unit.cursor
        self._verify_control_plane_cursor(cursor, context)
        cycle_input = self._cycle_input_cursor(cursor, context)
        receipt_id, equity_window_id, equity_receipt = (
            self._equity_receipt_cursor(
                cursor,
                context,
                self._scope.input_ready_plan,
            )
        )
        instrument_ids = self._instrument_ids_cursor(cursor, control)
        gaps_sha256 = self._unresolved_gaps_sha256(cursor, control)
        heads_sha256 = self._market_state_heads_sha256(cursor, control)
        reference = self._reference_state_cursor(cursor)
        rolling = self._rolling_state_cursor(cursor)
        if reference is None or rolling is None:
            raise ValueError(
                "Advanced Options canonical reference or rolling state is absent"
            )
        binding = rolling.producer_binding
        if self._scope.rolling_producer_plan is not None and (
            binding is None
            or binding.producer_plan_id
            != self._scope.rolling_producer_plan.plan_id
            or binding.reference_state_id != reference.state_id
            or binding.reference_state_sha256 != reference.state_sha256
            or binding.reference_generation
            != reference.reference_generation
        ):
            raise ValueError(
                "Advanced Options rolling producer binding changed"
            )
        input_plan = self._scope.input_ready_plan
        if input_plan is not None:
            gaps_sha256 = binding.unresolved_gaps_sha256
            heads_sha256 = binding.market_state_heads_sha256
        return AdvancedOptionsCanonicalInputFacts(
            control_plane=control,
            producer_plan=self._scope.producer_plan,
            cycle_input=cycle_input,
            reference_state=reference,
            rolling_state=rolling,
            reference_policy_sha256=reference.reference_policy_sha256,
            oi_facts_sha256=reference.open_interest_sha256,
            market_state_checkpoint_sha256=(
                rolling.market_state_checkpoint_sha256
            ),
            flow_checkpoint_sha256=rolling.flow_checkpoint_sha256,
            unresolved_gaps_sha256=gaps_sha256,
            market_state_heads_sha256=heads_sha256,
            equity_input_receipt_id=receipt_id,
            equity_input_receipt=equity_receipt,
            equity_input_sha256=equity_receipt.input_sha256,
            equity_window_id=equity_window_id,
            instrument_ids=instrument_ids,
            available_at=self._scope.producer_plan.clocks.known_at,
            maximum_equity_input_age_seconds=(
                None
                if input_plan is None
                else (
                    input_plan.native_input_plan
                    .maximum_equity_input_age_seconds
                )
            ),
            native_input_plan_sha256=(
                None
                if input_plan is None
                else input_plan.native_input_plan.plan_sha256
            ),
            native_input_sha256=(
                None
                if input_plan is None
                else input_plan.native_input_plan.expected_input_sha256
            ),
            executor_membership_sha256=(
                None
                if input_plan is None
                else input_plan.native_executor_membership_sha256
            ),
            mark_producer_plan_sha256=(
                None
                if input_plan is None
                else input_plan.mark_producer_plan.plan_sha256
            ),
            marks_sha256=(
                None
                if input_plan is None
                else input_plan.mark_producer_plan.expected_marks_sha256
            ),
        )

    def load_input_facts(
        self,
        context: StageContext,
    ) -> AdvancedOptionsCanonicalInputFacts:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            return self._load_input_facts_unit(context, unit)

    def _input_ready_context_cursor(
        self,
        cursor,
        *,
        lock_window: bool,
    ) -> _AdvancedOptionsInputReadyContext:
        input_plan = self._scope.input_ready_plan
        if input_plan is None:
            raise ValueError(
                "Advanced Options input-ready PLAN is not selected"
            )
        cursor.execute(
            "SELECT service.plan_id,service.cohort_id,"
            "service.session_date,service.stage_catalog_version,"
            "service.enabled_stages,service.config_sha256,service.deadline,"
            "window_run.window_id,window_run.plan_id,window_run.cohort_id,"
            "window_run.provider_tier,window_run.window_key,"
            "window_run.release_at,window_run.cutoff,window_run.deadline,"
            "window_run.state,window_run.owner_id "
            "FROM rt.window_runs AS window_run "
            "JOIN rt.service_plans AS service "
            "ON service.plan_id=window_run.plan_id "
            "WHERE window_run.window_id=%s"
            + (" FOR UPDATE OF window_run" if lock_window else ""),
            (str(input_plan.window_id),),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(
                "Advanced Options input-ready window is absent"
            )
        service = ServicePlan(
            plan_id=UUID(str(row[0])),
            cohort_id=UUID(str(row[1])),
            session_date=row[2],
            stage_catalog_version=str(row[3]),
            enabled_stages=frozenset(row[4]),
            config_sha256=str(row[5]),
            deadline=row[6],
        )
        window = WindowRecord(
            window_id=UUID(str(row[7])),
            plan_id=UUID(str(row[8])),
            cohort_id=UUID(str(row[9])),
            provider_tier=str(row[10]),
            window_key=str(row[11]),
            release_at=row[12],
            cutoff=row[13],
            deadline=row[14],
            state=str(row[15]),
        )
        control = input_plan.control_plane
        if (
            service.plan_id != input_plan.service_plan_id
            or service.cohort_id != control.cohort_id
            or service.session_date != control.session_date
            or service.config_sha256 != (
                input_plan.enrollment.enrollment_sha256
                if control.stored_identity is None
                else control.stored_identity.service_plan_sha256
            )
            or window.plan_id != service.plan_id
            or window.cohort_id != control.cohort_id
            or window.provider_tier != control.binding.options_tier.value
            or window.cutoff != input_plan.cutoff
            or window.state != "PENDING"
            or row[16] is not None
        ):
            raise ValueError(
                "Advanced Options input-ready window fence changed"
            )
        return _AdvancedOptionsInputReadyContext(service, window)

    @staticmethod
    def _input_dependency(
        member_kind: str,
        member_key: str,
        status: AdvancedOptionsInputDependencyStatus,
        identity,
        *,
        reasons: tuple[str, ...] = (),
        accepted_unavailable: bool = False,
    ) -> AdvancedOptionsInputDependencyEvidence:
        return AdvancedOptionsInputDependencyEvidence(
            member_kind=member_kind,
            member_key=member_key,
            status=status,
            evidence_sha256=canonical_sha256(identity),
            reason_codes=reasons,
            accepted_unavailable=accepted_unavailable,
        )

    @staticmethod
    def _persisted_mark_reasons_cursor(
        cursor,
        marks,
    ) -> tuple[str, ...]:
        expected = {
            str(mark.evidence_id): (
                mark.instrument_id,
                mark.contract_id,
                mark.catalog_version_id,
                ProviderTier.MASSIVE_ADVANCED.value,
                "ADVANCED_NBBO",
                mark.mark_source.value,
                str(mark.journal_generation),
                str(mark.stream_session_id),
                mark.source_offset,
                mark.market_time,
                mark.observed_time,
                mark.bid,
                mark.ask,
                mark.midpoint,
                mark.payload_sha256,
            )
            for mark in marks
        }
        if not expected:
            return ("IMMUTABLE_ADVANCED_NBBO_MARKS_ABSENT",)
        cursor.execute(
            "SELECT evidence_id::text,instrument_id,contract_id,"
            "catalog_version_id,provider_tier,source_kind,mark_source,"
            "journal_generation::text,stream_session_id::text,"
            "source_offset,market_time,observed_time,bid,ask,mark,"
            "payload_sha256 FROM rt.option_mark_evidence "
            "WHERE evidence_id=ANY(%s::uuid[]) ORDER BY evidence_id",
            (list(expected),),
        )
        stored = {
            str(row[0]): tuple(row[1:])
            for row in cursor.fetchall()
        }
        if set(stored) != set(expected):
            return ("IMMUTABLE_ADVANCED_NBBO_MARKS_PARTIAL",)
        if any(stored[key] != expected[key] for key in expected):
            return ("IMMUTABLE_ADVANCED_NBBO_MARK_IDENTITY_MISMATCH",)
        return ()

    def _input_ready_evidence(
        self,
        *,
        reference,
        rolling,
        marks,
        native,
        facts: AdvancedOptionsCanonicalInputFacts,
        persisted_mark_reasons: tuple[str, ...],
    ) -> tuple[AdvancedOptionsInputDependencyEvidence, ...]:
        mark_reasons = tuple(sorted({
            *marks.reason_codes,
            *persisted_mark_reasons,
        }))
        rows = [
            self._input_dependency(
                "SCOPE",
                "ACTIVE_CONTRACTS",
                AdvancedOptionsInputDependencyStatus.AVAILABLE,
                (
                    facts.cycle_input.slot_set_version,
                    facts.cycle_input.slot_set_member_sha256,
                    tuple(facts.instrument_ids.items()),
                ),
            ),
            self._input_dependency(
                "REFERENCE",
                "GENERATION",
                (
                    AdvancedOptionsInputDependencyStatus.AVAILABLE
                    if reference.status is ReferenceProducerStatus.READY
                    else AdvancedOptionsInputDependencyStatus.BLOCKED
                ),
                reference.preflight_sha256,
                reasons=reference.reason_codes,
            ),
            self._input_dependency(
                "ROLLING",
                "GENERATION",
                (
                    AdvancedOptionsInputDependencyStatus.AVAILABLE
                    if rolling.status is RollingProducerStatus.READY
                    else AdvancedOptionsInputDependencyStatus.BLOCKED
                ),
                rolling.preflight_sha256,
                reasons=rolling.reason_codes,
            ),
            self._input_dependency(
                "MARKS",
                "ACTIVE_CONTRACTS",
                (
                    AdvancedOptionsInputDependencyStatus.AVAILABLE
                    if (
                        marks.status is AdvancedOptionsMarkStatus.READY
                        and not persisted_mark_reasons
                    )
                    else AdvancedOptionsInputDependencyStatus.BLOCKED
                ),
                (
                    marks.preflight_sha256,
                    tuple(mark.evidence_id for mark in marks.marks),
                    persisted_mark_reasons,
                ),
                reasons=mark_reasons,
            ),
            self._input_dependency(
                "EQUITY",
                "INPUT_READY",
                AdvancedOptionsInputDependencyStatus.AVAILABLE,
                (
                    facts.equity_input_receipt_id,
                    facts.equity_window_id,
                    facts.equity_input_receipt,
                ),
            ),
        ]
        for detector, lane in (
            ("O1", native.bundle.o1),
            ("O3", native.bundle.o3),
            ("OS1", native.bundle.os1),
            ("OS2", native.bundle.os2),
        ):
            reasons = tuple(lane.blocked_reason_codes)
            accepted_unavailable = (
                bool(reasons) and native.status is NativeInputPlanStatus.READY
            )
            status = (
                AdvancedOptionsInputDependencyStatus.UNAVAILABLE
                if accepted_unavailable
                else AdvancedOptionsInputDependencyStatus.BLOCKED
                if reasons
                else AdvancedOptionsInputDependencyStatus.AVAILABLE
                if lane.invocations
                else AdvancedOptionsInputDependencyStatus.COMPLETE_ZERO
            )
            rows.append(self._input_dependency(
                "NATIVE",
                detector,
                status,
                (
                    native.input_sha256,
                    native.executor_membership_sha256,
                    detector,
                    len(lane.invocations),
                    reasons,
                ),
                reasons=reasons,
                accepted_unavailable=accepted_unavailable,
            ))
        rows.append(self._input_dependency(
            "OPTIONAL",
            "O2",
            AdvancedOptionsInputDependencyStatus.UNAVAILABLE,
            ("O2", "O2_MODEL_NOT_ACCEPTED"),
            reasons=("O2_MODEL_NOT_ACCEPTED",),
            accepted_unavailable=True,
        ))
        for lane in (
            facts.rolling_state.iv_surface,
            facts.rolling_state.dark_pool,
        ):
            available = lane.availability is RollingAvailability.READY
            rows.append(self._input_dependency(
                "OPTIONAL",
                lane.lane,
                (
                    AdvancedOptionsInputDependencyStatus.AVAILABLE
                    if available
                    else AdvancedOptionsInputDependencyStatus.UNAVAILABLE
                ),
                lane,
                reasons=() if available else lane.reason_codes,
                accepted_unavailable=not available,
            ))
        if rolling.status is RollingProducerStatus.READY:
            rows.extend((
                self._input_dependency(
                    "ROLLING",
                    "MARKET_STATE_CHECKPOINT",
                    AdvancedOptionsInputDependencyStatus.AVAILABLE,
                    facts.market_state_checkpoint_sha256,
                ),
                self._input_dependency(
                    "ROLLING",
                    "FLOW_CHECKPOINT",
                    AdvancedOptionsInputDependencyStatus.AVAILABLE,
                    facts.flow_checkpoint_sha256,
                ),
                self._input_dependency(
                    "ROLLING",
                    "UNRESOLVED_GAPS",
                    AdvancedOptionsInputDependencyStatus.COMPLETE_ZERO,
                    facts.unresolved_gaps_sha256,
                ),
                self._input_dependency(
                    "ROLLING",
                    "SOURCE_CLOCKS",
                    AdvancedOptionsInputDependencyStatus.AVAILABLE,
                    facts.producer_plan.clocks.sha256,
                ),
            ))
        if native.status is NativeInputPlanStatus.BLOCKED and not any(
            row.status is AdvancedOptionsInputDependencyStatus.BLOCKED
            for row in rows
            if row.member_kind == "NATIVE"
        ):
            rows.append(self._input_dependency(
                "NATIVE",
                "PLAN",
                AdvancedOptionsInputDependencyStatus.BLOCKED,
                native.preflight_sha256,
                reasons=native.reason_codes,
            ))
        return tuple(rows)

    def _assemble_input_ready(
        self,
        unit,
        *,
        lock_window: bool,
    ) -> AdvancedOptionsInputReadyPreflight:
        input_plan = self._scope.input_ready_plan
        if input_plan is None:
            raise ValueError(
                "Advanced Options input-ready PLAN is not selected"
            )
        context = self._input_ready_context_cursor(
            unit.cursor,
            lock_window=lock_window,
        )
        facts = self._load_input_facts_unit(context, unit)
        with unit.connection.cursor(
            cursor_factory=RealDictCursor,
        ) as cursor:
            reference = assemble_advanced_options_reference_state_cursor(
                cursor,
                input_plan.reference_producer_plan,
            )
            rolling = assemble_advanced_options_rolling_state_cursor(
                cursor,
                input_plan.rolling_producer_plan,
            )
            marks = assemble_advanced_options_marks_cursor(
                cursor,
                input_plan.mark_producer_plan,
            )
            native = assemble_advanced_options_native_inputs_cursor(
                cursor,
                input_plan.native_input_plan,
            )
        persisted_mark_reasons = self._persisted_mark_reasons_cursor(
            unit.cursor,
            marks.marks,
        )
        evidence = self._input_ready_evidence(
            reference=reference,
            rolling=rolling,
            marks=marks,
            native=native,
            facts=facts,
            persisted_mark_reasons=persisted_mark_reasons,
        )
        blocked = any(
            row.status is AdvancedOptionsInputDependencyStatus.BLOCKED
            or (
                row.status
                is AdvancedOptionsInputDependencyStatus.UNAVAILABLE
                and not row.accepted_unavailable
            )
            for row in evidence
        )
        if blocked:
            digest = canonical_sha256({
                "plan_id": input_plan.plan_id,
                "status": AdvancedOptionsInputDependencyStatus.BLOCKED,
                "evidence": evidence,
            })
            return AdvancedOptionsInputReadyPreflight(
                status=AdvancedOptionsInputDependencyStatus.BLOCKED,
                plan_id=input_plan.plan_id,
                preflight_sha256=digest,
                evidence=evidence,
            )
        changed_identities = [
            name
            for name, changed in (
                ("reference_state", reference.state != facts.reference_state),
                ("rolling_state", rolling.state != facts.rolling_state),
                (
                    "marks",
                    marks.marks_sha256
                    != input_plan.mark_producer_plan.expected_marks_sha256,
                ),
                (
                    "native_input",
                    native.input_sha256
                    != input_plan.native_input_plan.expected_input_sha256,
                ),
                (
                    "native_executor_membership",
                    native.executor_membership_sha256
                    != input_plan.native_executor_membership_sha256,
                ),
            )
            if changed
        ]
        if changed_identities:
            raise ValueError(
                "Advanced Options input-ready producer identities changed: "
                + ",".join(changed_identities)
            )
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
            native_input_plan_sha256=facts.native_input_plan_sha256,
            native_input_sha256=facts.native_input_sha256,
            executor_membership_sha256=(
                facts.executor_membership_sha256
            ),
            mark_producer_plan_sha256=(
                facts.mark_producer_plan_sha256
            ),
            marks_sha256=facts.marks_sha256,
        )
        if not closure.has_canonical_producer_evidence:
            raise ValueError(
                "Advanced Options input-ready closure lacks producer evidence"
            )
        if (
            input_plan.expected_closure_sha256 is not None
            and input_plan.expected_closure_sha256 != closure.sha256
        ):
            raise ValueError(
                "Advanced Options input-ready replay diverged from frozen PLAN"
            )
        gate = build_advanced_options_input_gate_plan(
            closure,
            window_id=input_plan.window_id,
            equity_window_id=facts.equity_window_id,
            instrument_ids=facts.instrument_ids,
            build_id=input_plan.producer_build_id,
            available_at=facts.available_at,
        )
        members = tuple(
            sorted(row.member_key for row in gate.barrier_members)
        )
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
            cutoff=input_plan.cutoff,
            reference_generation=facts.reference_state.reference_generation,
            build_id=input_plan.producer_build_id,
        )
        unit.cursor.execute(
            "SELECT receipt_id,cohort_id,cutoff,reference_generation,"
            "universe_sha256,input_sha256,build_id "
            "FROM rt.cycle_receipts WHERE window_id=%s "
            "AND receipt_kind=%s",
            (str(input_plan.window_id), OPTIONS_INPUT_READY),
        )
        stored = unit.cursor.fetchone()
        existing_receipt_id = None
        if stored is not None:
            existing_receipt_id = UUID(str(stored[0]))
            if (
                UUID(str(stored[1])),
                stored[2],
                int(stored[3]),
                str(stored[4]),
                str(stored[5]),
                str(stored[6]),
            ) != (
                facts.control_plane.cohort_id,
                receipt.cutoff,
                receipt.reference_generation,
                receipt.universe_sha256,
                receipt.input_sha256,
                receipt.build_id,
            ):
                raise ValueError(
                    "durable OPTIONS_INPUT_READY receipt identity changed"
                )
        digest = canonical_sha256({
            "plan_id": input_plan.plan_id,
            "status": AdvancedOptionsInputDependencyStatus.AVAILABLE,
            "evidence": evidence,
            "closure_sha256": closure.sha256,
            "barrier_id": gate.barrier_id,
            "receipt": receipt,
        })
        return AdvancedOptionsInputReadyPreflight(
            status=AdvancedOptionsInputDependencyStatus.AVAILABLE,
            plan_id=input_plan.plan_id,
            preflight_sha256=digest,
            evidence=evidence,
            closure=closure,
            gate=gate,
            receipt=receipt,
            existing_receipt_id=existing_receipt_id,
        )

    def preflight_input_ready(
        self,
    ) -> AdvancedOptionsInputReadyPreflight:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return self._assemble_input_ready(
                unit,
                lock_window=False,
            )

    def produce_input_ready(
        self,
        *,
        maximum_attempts: int = 3,
        retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> AdvancedOptionsInputReadyResult:
        input_plan = self._scope.input_ready_plan
        if input_plan is None:
            raise ValueError(
                "Advanced Options input-ready PLAN is not selected"
            )
        if not input_plan.frozen:
            raise ValueError(
                "Advanced Options input-ready PLAN is not frozen by preflight"
            )
        if (
            type(maximum_attempts) is not int
            or maximum_attempts < 1
            or retry_initial_seconds < 0
            or not callable(wait)
        ):
            raise ValueError(
                "Advanced Options input-ready retry is invalid"
            )
        for attempt in range(1, maximum_attempts + 1):
            try:
                with self._provider.transaction(
                    APP_ROLE,
                    isolation_level="SERIALIZABLE",
                ) as unit:
                    preflight = self._assemble_input_ready(
                        unit,
                        lock_window=True,
                    )
                    if (
                        preflight.status
                        is AdvancedOptionsInputDependencyStatus.BLOCKED
                    ):
                        raise AdvancedOptionsInputReadyBlocked(preflight)
                    if (
                        preflight.gate is None
                        or preflight.receipt is None
                    ):
                        raise ValueError(
                            "Advanced Options input-ready preflight is incomplete"
                        )
                    gate = preflight.gate
                    for job in gate.jobs:
                        enqueue_materialization_job_cursor(
                            unit.cursor,
                            job,
                        )
                    open_job_barrier_cursor(
                        unit.cursor,
                        barrier_id=gate.barrier_id,
                        cohort_id=gate.closure.cycle_input.cohort_id,
                        window_id=gate.window_id,
                        barrier_kind=OPTIONS_INPUT_READY,
                        universe_sha256=(
                            gate.closure.cycle_input.universe_sha256
                        ),
                        members=gate.barrier_members,
                    )
                    for member in gate.barrier_members:
                        record_job_barrier_outcome_cursor(
                            unit.cursor,
                            barrier_id=gate.barrier_id,
                            member_key=member.member_key,
                            outcome=BarrierOutcome.COMPLETE,
                            outcome_input_sha256=(
                                member.expected_input_sha256
                            ),
                        )
                    verdict = evaluate_job_barrier_cursor(
                        unit.cursor,
                        gate.barrier_id,
                    )
                    receipt = reconstruct_options_input_ready_receipt(
                        gate,
                        verdict,
                        cutoff=input_plan.cutoff,
                        reference_generation=(
                            gate.closure.reference_generation
                        ),
                        build_id=input_plan.producer_build_id,
                    )
                    if receipt != preflight.receipt:
                        raise ValueError(
                            "Advanced Options input-ready receipt changed"
                        )
                    receipt_id = persist_cycle_receipt_cursor(
                        unit.cursor,
                        window_id=input_plan.window_id,
                        cohort_id=input_plan.control_plane.cohort_id,
                        spec=receipt,
                    )
                    return AdvancedOptionsInputReadyResult(
                        receipt_id=receipt_id,
                        receipt=receipt,
                        closure_sha256=gate.closure_sha256,
                        preflight_sha256=preflight.preflight_sha256,
                    )
            except PsycopgError as error:
                if (
                    error.pgcode not in _RETRYABLE_POSTGRES_SQLSTATES
                    or attempt == maximum_attempts
                ):
                    raise
                wait(retry_initial_seconds * (2 ** (attempt - 1)))
        raise AssertionError(
            "unreachable Advanced Options input-ready retry state"
        )

    def load_cycle_facts(
        self,
        context: StageContext,
        input_stage,
    ) -> AdvancedOptionsCanonicalCycleFacts:
        receipt = self.load_input_receipt(context)
        if (
            receipt.input_sha256 != input_stage.input_gate.closure_sha256
            or receipt.reference_generation
            != input_stage.reference_generation
        ):
            raise ValueError(
                "Advanced Options durable input receipt changed"
            )
        if self._native_input_loader is not None:
            plan = self._scope.native_input_plan
            closure = input_stage.input_gate.closure
            if (
                plan is None
                or plan.cutoff != context.window.cutoff
                or plan.reference_state.state_id
                != closure.reference_state_id
                or plan.reference_state.state_sha256
                != closure.reference_state_sha256
                or plan.rolling_state.state_id != closure.rolling_state_id
                or plan.rolling_state.state_sha256
                != closure.rolling_state_sha256
                or plan.source_flow.source_flow_sha256
                != closure.source_flow_sha256
                or plan.equity_input_receipt_id
                != closure.equity_input_receipt_id
                or plan.equity_input_receipt.input_sha256
                != closure.equity_input_sha256
            ):
                raise ValueError(
                    "Advanced Options native input PLAN changed"
                )
            native_inputs = self._native_input_loader.load()
        else:
            native_inputs = self._scope.native_inputs_loader(context)
        return AdvancedOptionsCanonicalCycleFacts(
            receipt,
            native_inputs,
            self._reference,
            self._scope.evidence_builder,
            f"postgres-options-cycle:{receipt.input_sha256}",
            self._scope.flow_identity,
        )

    def _load_receipt(
        self,
        context: StageContext,
        receipt_kind: str,
    ) -> CycleReceiptSpec:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT cutoff,reference_generation,universe_sha256,"
                "input_sha256,build_id FROM rt.cycle_receipts "
                "WHERE window_id=%s AND cohort_id=%s "
                "AND receipt_kind=%s",
                (
                    str(context.window.window_id),
                    str(context.plan.cohort_id),
                    receipt_kind,
                ),
            )
            row = unit.cursor.fetchone()
        if row is None:
            raise ValueError(
                f"Advanced Options durable {receipt_kind} receipt is absent"
            )
        return CycleReceiptSpec(
            receipt_kind,
            row[0],
            int(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
        )

    def load_input_receipt(
        self,
        context: StageContext,
    ) -> CycleReceiptSpec:
        return self._load_receipt(context, OPTIONS_INPUT_READY)

    def load_completed_cycle(
        self,
        context: StageContext,
        input_stage,
    ) -> AdvancedOptionsCycleResult:
        input_receipt = self.load_input_receipt(context)
        cycle_receipt = self._load_receipt(context, OPTIONS_CYCLE)
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return load_advanced_options_cycle_cursor(
                unit.cursor,
                control_plane=input_stage.control_plane,
                input_gate=input_stage.input_gate,
                input_receipt=input_receipt,
                cycle_receipt=cycle_receipt,
            )

    def load_completed_projection(
        self,
        context: StageContext,
        cycle: AdvancedOptionsCycleResult,
    ) -> AdvancedOptionsProjectionBatch:
        receipt = self._load_receipt(context, OPTIONS_PORTAL_READY)
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return load_advanced_options_projection_batch_cursor(
                unit.cursor,
                control_plane=self._scope.control_plane,
                cycle=cycle,
                portal_ready_receipt=receipt,
            )

    def load_close_data(
        self,
        context: StageContext,
        projection,
    ) -> AdvancedOptionsCloseStageData:
        return self._scope.close_data_loader(context, projection)
