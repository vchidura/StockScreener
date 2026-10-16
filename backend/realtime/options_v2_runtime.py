"""Repository-backed V2 OPTIONS_ACQUIRE and OPTIONS_ADMISSION operations.

The provider source is constructed outside this module under an explicit authorization envelope.
These operations only coordinate the existing staged pre-session repositories. They persist and
verify every package before admission, build the immutable MARKET_OPTIONS cohort and slot set, and
seed admitted prior-open-interest facts. OPTIONS_CYCLE is deliberately not implemented here.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256
from .database_access import RealtimeDatabaseRole
from .forward_bootstrap import dated_open_interest_facts
from .forward_launcher import DeveloperForwardEnrollmentReceipt
from .option_manifest_cohort_repository import DeveloperForwardCohortBinding
from .option_pre_session_staged_runner import StagedTargetOptionPreSessionRunner
from .option_pre_session_target import OptionPreSessionTargetPlan
from .option_session_manifest import ManifestSelectionStatus
from .reference_repository import persist_reference_facts_cursor
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageCancelled, StageContext
from .stage_runners_options import OptionsAcquireResult, OptionsAdmissionResult
from .worker_database import RealtimeProcessDatabase


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _persist_strategy_admissions(cursor, result, cohort_id: UUID, selection_id: UUID) -> None:
    for coverage in result.plan.coverage:
        identity = canonical_sha256({
            "cohort_id": cohort_id,
            "session_date": result.plan.session_date,
            "policy_sha256": result.plan.policy.sha256,
            "underlyer": coverage.underlyer,
            "target_dte": coverage.target_dte,
            "side": coverage.contract_type.value,
        })
        admission_id = uuid5(NAMESPACE_URL, f"options-v2-admission:{identity}")
        expected = (
            str(cohort_id),
            result.plan.session_date,
            result.plan.policy.sha256,
            coverage.underlyer,
            coverage.target_dte,
            coverage.contract_type.value,
            coverage.required_count,
            coverage.selected_count,
            coverage.status,
            list(coverage.reason_codes),
            f"option-selection:{selection_id}",
        )
        cursor.execute(
            "INSERT INTO rt.option_strategy_admissions ("
            "admission_id, cohort_id, session_date, policy_sha256, underlyer, target_dte, "
            "side, required_depth, qualifying_count, status, reason_codes, evidence_ref"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (admission_id) DO NOTHING",
            (str(admission_id), *expected),
        )
        cursor.execute(
            "SELECT cohort_id::text, session_date, policy_sha256, underlyer, target_dte, "
            "side, required_depth, qualifying_count, status, reason_codes, evidence_ref "
            "FROM rt.option_strategy_admissions WHERE admission_id=%s",
            (str(admission_id),),
        )
        stored = cursor.fetchone()
        if stored is None or tuple(stored) != expected:
            raise ValueError("persisted Options admission conflicts with its immutable identity")


@dataclass(slots=True)
class OptionsPreSessionOperations:
    process_plan_sha256: str
    target_plan: OptionPreSessionTargetPlan
    source: object
    cohort_binding: DeveloperForwardCohortBinding
    collection_deadline: datetime
    owner_id: UUID
    clock: Callable[[], datetime]
    scheduler_factory: Callable[[UUID], object] | None = None

    def __post_init__(self) -> None:
        if (len(self.process_plan_sha256) != 64
                or any(character not in "0123456789abcdef" for character in self.process_plan_sha256)
                or not isinstance(self.target_plan, OptionPreSessionTargetPlan)
                or not callable(getattr(self.source, "collect_underlyer", None))
                or not isinstance(self.cohort_binding, DeveloperForwardCohortBinding)
                or not isinstance(self.owner_id, UUID)
                or not callable(self.clock)):
            raise ValueError("Options V2 pre-session operations are not fully bound")
        self.collection_deadline = _utc(self.collection_deadline, "collection_deadline")

    def _runner(
        self,
        references_database: RealtimeProcessDatabase,
        control_database: RealtimeProcessDatabase,
    ) -> StagedTargetOptionPreSessionRunner:
        return StagedTargetOptionPreSessionRunner(
            references_database,
            control_database,
            self.source,
            clock=self.clock,
        )

    def acquire(
        self,
        references_database: RealtimeProcessDatabase,
        control_database: RealtimeProcessDatabase,
        *,
        cancelled: Callable[[], bool] = lambda: False,
    ) -> OptionsAcquireResult:
        runner = self._runner(references_database, control_database)
        run_id = runner.prepare(
            self.target_plan, collection_deadline=self.collection_deadline
        )
        if self.scheduler_factory is not None:
            transport = getattr(self.source, "request_transport", None)
            if transport is None or not hasattr(transport, "scheduler"):
                raise ValueError("Options acquire source has no replaceable request scheduler")
            transport.scheduler = self.scheduler_factory(run_id)
        expected = self.target_plan.scope.universe.option_underlyings
        for _ in expected:
            if cancelled():
                raise StageCancelled("OPTIONS_ACQUIRE was cancelled")
            receipt = runner.acquire_one(self.target_plan, self.owner_id)
            if receipt is None:
                break
        with references_database.transaction(
            read_only=True, isolation_level="REPEATABLE READ"
        ) as cursor:
            cursor.execute(
                "SELECT expected_underlyings FROM rt.option_pre_session_runs WHERE run_id=%s",
                (str(run_id),),
            )
            run = cursor.fetchone()
            cursor.execute(
                "SELECT underlyer, status FROM rt.option_pre_session_work "
                "WHERE run_id=%s ORDER BY underlyer",
                (str(run_id),),
            )
            work = tuple((str(symbol), str(status)) for symbol, status in cursor.fetchall())
            cursor.execute(
                "SELECT provider_requests, response_bytes "
                "FROM rt.option_pre_session_account_usage WHERE run_id=%s",
                (str(run_id),),
            )
            usage = cursor.fetchone()
        if run is None or tuple(run[0]) != tuple(expected) or usage is None:
            raise ValueError("Options acquire durable run identity is incomplete")
        if {symbol for symbol, _ in work} != set(expected):
            raise ValueError("Options acquire durable work set is incomplete")
        completed = tuple(symbol for symbol, status in work if status == "COMPLETE")
        return OptionsAcquireResult(
            run_id,
            tuple(expected),
            completed,
            int(usage[0]),
            int(usage[1]),
        )

    def admit(
        self,
        references_database: RealtimeProcessDatabase,
        control_database: RealtimeProcessDatabase,
        *,
        cancelled: Callable[[], bool] = lambda: False,
    ) -> OptionsAdmissionResult:
        if cancelled():
            raise StageCancelled("OPTIONS_ADMISSION was cancelled")
        result = self._runner(references_database, control_database).finalize(
            self.target_plan, cohort_binding=self.cohort_binding
        )
        if result.plan.status is not ManifestSelectionStatus.READY:
            reasons = result.plan.reason_codes or ("OPTIONS_ADMISSION_INCOMPLETE",)
            return OptionsAdmissionResult(
                result.selection_id, "BLOCKED", tuple(reasons)
            )
        enrollment = DeveloperForwardEnrollmentReceipt(
            self.process_plan_sha256,
            result,
            self.cohort_binding,
            self.target_plan.effective_at,
        )
        cohort_id = enrollment.cohort_id
        if cohort_id is None:
            raise ValueError("ready Options admission did not produce a cohort identity")
        received_at = _utc(self.clock(), "clock")
        facts = dated_open_interest_facts(enrollment, received_at=received_at)
        with references_database.transaction() as cursor:
            persist_reference_facts_cursor(cursor, facts)
        with control_database.transaction() as cursor:
            _persist_strategy_admissions(
                cursor, result, cohort_id, result.selection_id
            )
            cursor.execute(
                "SELECT current.version, version.primary_contract_count, "
                "version.reserve_contract_count "
                "FROM rt.slot_set_current AS current "
                "JOIN rt.slot_set_versions AS version USING(cohort_id, version) "
                "WHERE current.cohort_id=%s",
                (str(cohort_id),),
            )
            slot = cursor.fetchone()
        if slot is None:
            raise ValueError("ready Options admission has no active slot set")
        version, primary_count, reserve_count = map(int, slot)
        return OptionsAdmissionResult(
            result.selection_id,
            "READY",
            (),
            cohort_id,
            version,
            primary_count + reserve_count,
        )


@dataclass(frozen=True, slots=True)
class OptionsPreSessionStageBinding:
    operations: OptionsPreSessionOperations

    def acquire(self, context: StageContext) -> OptionsAcquireResult:
        references = V2StageDatabase(context, RealtimeDatabaseRole.REFERENCES)
        control = V2StageDatabase(context, RealtimeDatabaseRole.CONTROL)
        return self.operations.acquire(
            references, control, cancelled=context.is_cancelled
        )

    def admit(self, context: StageContext) -> OptionsAdmissionResult:
        references = V2StageDatabase(context, RealtimeDatabaseRole.REFERENCES)
        control = V2StageDatabase(context, RealtimeDatabaseRole.CONTROL)
        return self.operations.admit(
            references, control, cancelled=context.is_cancelled
        )
