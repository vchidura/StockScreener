"""PostgreSQL canonical port for Advanced Options non-serving runtime inputs."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from .advanced_options_cycle import AdvancedOptionsDatasetFlowIdentity
from .advanced_options_loaders import (
    AdvancedOptionsCanonicalCycleFacts,
    AdvancedOptionsCanonicalInputFacts,
    AdvancedOptionsCanonicalRuntimePort,
)
from .advanced_options_native_adapters import AdvancedOptionsNativeInputBundle
from .advanced_options_producers import AdvancedOptionsProducerPlan
from .advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
)
from .advanced_options_rolling_repository import (
    PostgresAdvancedOptionsRollingStateRepository,
)
from .advanced_options_runtime import (
    AdvancedOptionsCloseStageData,
    EvidenceBuilder,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .option_cycle_input import (
    OptionCycleInput,
    OptionCycleUnderlying,
    verify_option_cycle_input_against_tables,
)
from .option_discovery_verify import read_discovery_batches
from .option_slot_set_repository import load_active_slot_set_cursor
from .orchestration_repository import (
    OPTIONS_INPUT_READY,
    CycleReceiptSpec,
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
    native_inputs_loader: NativeInputsLoader
    evidence_builder: EvidenceBuilder
    close_data_loader: CloseDataLoader
    flow_identity: AdvancedOptionsDatasetFlowIdentity | None = None
    require_discovery_pages: bool = True

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
            or not callable(self.native_inputs_loader)
            or not callable(self.evidence_builder)
            or not callable(self.close_data_loader)
        ):
            raise ValueError(
                "Advanced Options PostgreSQL runtime scope is invalid"
            )
        object.__setattr__(
            self,
            "code_sha256",
            _sha256(self.code_sha256, "code_sha256"),
        )


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
            context.plan.config_sha256,
            control.pins.model_bundle_sha256,
            control.pins.valuation_policy_sha256,
            "SHADOW",
            control.manifest_sha256,
            control.universe.sha256,
            control.session_date,
            str(control.profile_id),
            "MARKET_OPTIONS",
            len(control.universe.option_underlyings),
            control.manifest_policy.sha256,
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
    ) -> tuple[UUID, UUID, CycleReceiptSpec]:
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

    def load_input_facts(
        self,
        context: StageContext,
    ) -> AdvancedOptionsCanonicalInputFacts:
        control = self._scope.control_plane
        if (
            context.plan.cohort_id != control.cohort_id
            or context.plan.session_date != control.session_date
            or context.window.cutoff
            != self._scope.producer_plan.clocks.market_cutoff
        ):
            raise ValueError("Advanced Options PostgreSQL context changed")
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            cursor = unit.cursor
            self._verify_control_plane_cursor(cursor, context)
            cycle_input = self._cycle_input_cursor(cursor, context)
            receipt_id, equity_window_id, equity_receipt = (
                self._equity_receipt_cursor(cursor, context)
            )
            instrument_ids = self._instrument_ids_cursor(cursor, control)
            gaps_sha256 = self._unresolved_gaps_sha256(cursor, control)
            heads_sha256 = self._market_state_heads_sha256(cursor, control)
        reference = self._reference.load_for_session(
            control.session_date,
            known_at=self._scope.producer_plan.clocks.known_at,
        )
        rolling = self._rolling.load_latest(
            cohort_id=control.cohort_id,
            session_date=control.session_date,
            journal_generation=control.source_flow.journal_generation,
        )
        if reference is None or rolling is None:
            raise ValueError(
                "Advanced Options canonical reference or rolling state is absent"
            )
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
        )

    def load_cycle_facts(
        self,
        context: StageContext,
        input_stage,
    ) -> AdvancedOptionsCanonicalCycleFacts:
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
                    OPTIONS_INPUT_READY,
                ),
            )
            row = unit.cursor.fetchone()
        if row is None:
            raise ValueError(
                "Advanced Options durable input receipt is absent"
            )
        receipt = CycleReceiptSpec(
            OPTIONS_INPUT_READY,
            row[0],
            int(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
        )
        if (
            receipt.input_sha256 != input_stage.input_gate.closure_sha256
            or receipt.reference_generation
            != input_stage.reference_generation
        ):
            raise ValueError(
                "Advanced Options durable input receipt changed"
            )
        return AdvancedOptionsCanonicalCycleFacts(
            receipt,
            self._scope.native_inputs_loader(context),
            self._reference,
            self._scope.evidence_builder,
            f"postgres-options-cycle:{receipt.input_sha256}",
            self._scope.flow_identity,
        )

    def load_close_data(
        self,
        context: StageContext,
        projection,
    ) -> AdvancedOptionsCloseStageData:
        return self._scope.close_data_loader(context, projection)
