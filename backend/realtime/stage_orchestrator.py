"""Resident stage orchestrator for V2 (RT-01): independent stages driven by the stage catalog.

Decisions come from stage_graph.decide_window; state lives behind OrchestrationRepository. Stages
are grouped into lanes by principal profile and the lanes run concurrently, so a failing, blocked
or slow Options stage never delays or blocks an equity, reference or portal stage. A stage failure
is recorded against that stage only; its declared dependents are recorded BLOCKED_PREREQ and
every other stage proceeds. Exception messages are never persisted (only the exception type).

Lanes run in daemon threads and the coordinator waits for them only within a bounded budget (the
window deadline plus a grace, capped by max_lane_seconds), renewing the window lease while it
waits. A lane that overruns is cancelled cooperatively (StageContext.cancelled), its running stages
are recorded FAILED with STAGE_TIMEOUT, and every write it attempts afterwards is rejected: orchestration
writes by the window or stage fence, and ordinary stage data because StageContext.transaction() fences each
write transaction to its stage run being RUNNING (rt.fence_stage_run, checked before COMMIT) and refuses to
open one at all once the stage is cancelled. A hung stage therefore cannot starve later windows or other
lanes. Python cannot kill a thread, so a runner should still poll context.is_cancelled() to stop its own
work early; the fence makes polling an optimization, not a safety requirement, for writes made through
context.transaction() (the only supported write path).
"""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, uuid4

from .database_access_v2 import StageRunFence, V2ConnectionProvider
from .orchestration_repository import (
    OWNER_LOST,
    CycleReceiptSpec,
    LeaseLost,
    OrchestrationRepository,
    ServicePlan,
    StageRunRecord,
    WindowClaim,
    WindowRecord,
)
from .stage_graph import (
    RESIDENT_SCHEDULES,
    STAGE_CATALOG_VERSION,
    STAGES_V1,
    Action,
    StageDefinition,
    decide_window,
    principal_profiles_required,
    startable_stages,
)

logger = logging.getLogger(__name__)
_OUTCOME_STATES = frozenset({"COMPLETE", "FAILED", "BLOCKED_POLICY"})


class CatalogMismatch(RuntimeError):
    """The database stage catalog differs from the source catalog."""


class MissingRunner(RuntimeError):
    """An enabled stage has no runner; the plan's approved scope cannot be honored."""


@dataclass(frozen=True, slots=True)
class StageOutcome:
    """What a stage runner reports. COMPLETE with reason codes means completed but DEGRADED."""

    state: str
    reason_codes: tuple[str, ...] = ()
    receipt: CycleReceiptSpec | None = None
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        if self.state not in _OUTCOME_STATES:
            raise ValueError(f"stage outcome state must be one of {sorted(_OUTCOME_STATES)}")
        if self.state != "COMPLETE" and not self.reason_codes:
            raise ValueError("a failed or blocked stage outcome requires reason codes")
        if self.receipt is not None and self.state != "COMPLETE":
            raise ValueError("only a completed stage can record a receipt")

    @classmethod
    def complete(cls, *, degraded: tuple[str, ...] = (), receipt: CycleReceiptSpec | None = None,
                 evidence_ref: str | None = None) -> StageOutcome:
        return cls("COMPLETE", degraded, receipt, evidence_ref)

    @classmethod
    def failed(cls, *reasons: str) -> StageOutcome:
        return cls("FAILED", tuple(reasons))

    @classmethod
    def blocked(cls, *reasons: str) -> StageOutcome:
        return cls("BLOCKED_POLICY", tuple(reasons))


class StageCancelled(RuntimeError):
    """A stage the coordinator abandoned asked to open a write transaction."""


@dataclass(frozen=True, slots=True)
class StageContext:
    plan: ServicePlan
    window: WindowRecord
    stage: StageDefinition
    stage_run_id: UUID
    attempt: int
    started_at: datetime
    database: V2ConnectionProvider | None = None
    cancelled: threading.Event | None = None
    fenced: bool = False

    def is_cancelled(self) -> bool:
        """True once the coordinator abandoned this stage (timeout or lost lease); stop side effects."""
        return self.cancelled is not None and self.cancelled.is_set()

    def transaction(self, *, read_only: bool = False, dict_cursor: bool = False,
                    isolation_level: str | None = None, role: str | None = None):
        """A unit of work as this stage's capability role and principal profile.

        This is the only supported way for a runner to write. A cancelled stage cannot open a write
        transaction, and for a window stage every write transaction commits only while its stage
        run is still RUNNING (checked in the same transaction, see StageRunFence), so a runner that
        ignores cancellation cannot land data after STAGE_TIMEOUT. Resident stages have no stage
        run and are not fenced here. `role` selects another capability role of the same principal
        (a stage that persists evidence and publishes snapshots uses compute and publication); the
        access layer still refuses a role the principal is not a member of."""
        if self.database is None:
            raise RuntimeError("this orchestrator has no database provider")
        capability_role = role or self.stage.capability_role
        if read_only:
            return self.database.transaction(capability_role, self.stage.principal_profile, read_only=True,
                                             dict_cursor=dict_cursor, isolation_level=isolation_level)
        if self.is_cancelled():
            raise StageCancelled(f"{self.stage.stage_id} was abandoned; it may not write")
        return self.database.transaction(
            capability_role, self.stage.principal_profile, dict_cursor=dict_cursor,
            isolation_level=isolation_level,
            fence=StageRunFence(self.stage_run_id) if self.fenced else None)


StageRunner = Callable[[StageContext], StageOutcome]


@dataclass(frozen=True, slots=True)
class StartReport:
    plan_id: UUID
    enabled: frozenset[str]
    window_stages: frozenset[str]
    resident_stages: frozenset[str]
    startable: frozenset[str]
    held: frozenset[str]
    principal_profiles: frozenset[str]


@dataclass(slots=True)
class WindowReport:
    window_id: UUID
    status: str
    window_state: str | None = None
    executed: list[tuple[str, str]] = field(default_factory=list)
    lane_errors: dict[str, str] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StageOrchestrator:
    def __init__(
        self,
        repository: OrchestrationRepository,
        runners: Mapping[str, StageRunner],
        *,
        database: V2ConnectionProvider | None = None,
        stages: Mapping[str, StageDefinition] = STAGES_V1,
        clock: Callable[[], datetime] = utc_now,
        owner_id: UUID | None = None,
        lease_seconds: int = 300,
        max_attempts: int = 2,
        parallel_lanes: bool = True,
        max_lane_seconds: float = 900.0,
        lane_grace_seconds: float = 30.0,
        heartbeat_seconds: float = 20.0,
    ) -> None:
        if lease_seconds <= 0 or max_attempts < 1:
            raise ValueError("lease seconds and max attempts must be positive")
        if max_lane_seconds <= 0 or lane_grace_seconds < 0 or heartbeat_seconds <= 0:
            raise ValueError("lane budget, grace and heartbeat must be positive")
        unknown = set(runners) - set(stages)
        if unknown:
            raise ValueError(f"runners for unknown stages: {sorted(unknown)}")
        self._repository = repository
        self._runners = dict(runners)
        self._database = database
        self._stages = dict(stages)
        self._clock = clock
        self._owner_id = owner_id or uuid4()
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._parallel = parallel_lanes
        self._max_lane_seconds = max_lane_seconds
        self._lane_grace_seconds = lane_grace_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._plans: dict[UUID, ServicePlan] = {}

    @property
    def owner_id(self) -> UUID:
        return self._owner_id

    def start(self, plan_id: UUID) -> StartReport:
        plan = self._repository.load_plan(plan_id)
        catalog = self._repository.load_catalog(plan.stage_catalog_version)
        if plan.stage_catalog_version != STAGE_CATALOG_VERSION or dict(catalog) != self._stages:
            raise CatalogMismatch("database stage catalog differs from the source catalog")
        missing = sorted(plan.enabled_stages - set(self._runners))
        if missing:
            raise MissingRunner("enabled stages without a runner: " + ", ".join(missing))
        self._plans[plan.plan_id] = plan
        readiness = self._repository.readiness(plan.cohort_id)
        startable = startable_stages(plan.enabled_stages, readiness, self._stages)
        resident = frozenset(name for name in plan.enabled_stages
                             if self._stages[name].schedule_kind in RESIDENT_SCHEDULES)
        return StartReport(
            plan.plan_id, plan.enabled_stages, plan.enabled_stages - resident, resident, startable,
            plan.enabled_stages - startable, principal_profiles_required(plan.enabled_stages, self._stages))

    def tick_resident(self, plan_id: UUID, stage_id: str, window: WindowRecord | None = None) -> StageOutcome:
        """Run one tick of a resident (INTERVAL or CONTINUOUS) stage. It reports through
        readiness only; a failure here never raises into, or blocks, any other stage."""
        plan = self._plan(plan_id)
        stage = self._stages[stage_id]
        if stage.schedule_kind not in RESIDENT_SCHEDULES or stage_id not in plan.enabled_stages:
            raise ValueError(f"{stage_id} is not an enabled resident stage")
        now = self._clock()
        context = StageContext(plan, window or self._placeholder_window(plan, now), stage, uuid4(), 1, now,
                               self._database)
        outcome = self._invoke(self._runners[stage_id], context)
        status = "BLOCKED" if outcome.state != "COMPLETE" else ("DEGRADED" if outcome.reason_codes else "READY")
        self._repository.upsert_readiness(plan.cohort_id, stage_id, plan.stage_catalog_version, status,
                                          outcome.reason_codes, outcome.evidence_ref, self._clock())
        return outcome

    def run_window(self, window_id: UUID) -> WindowReport:
        now = self._clock()
        claim = self._repository.claim_window(window_id, self._owner_id, now, self._lease_seconds)
        if claim is None:
            return WindowReport(window_id, "NOT_CLAIMED")
        plan = self._plan(claim.window.plan_id)
        report = WindowReport(window_id, "CLAIMED")
        window_stages = frozenset(name for name in plan.enabled_stages
                                  if self._stages[name].schedule_kind not in RESIDENT_SCHEDULES)
        cancelled = threading.Event()
        try:
            self._repository.recover_orphans(claim, now)
            lanes: dict[str, set[str]] = defaultdict(set)
            for name in window_stages:
                lanes[self._stages[name].principal_profile].add(name)
            abandoned: set[str] = set()
            while True:
                progressed = self._run_round(claim, plan, window_stages, lanes, report, cancelled, abandoned)
                if not progressed:
                    break
            self._conclude(claim, plan, window_stages, report)
        except LeaseLost:
            report.status = "LEASE_LOST"
        finally:
            cancelled.set()  # any abandoned lane thread stops at its next checkpoint
        return report

    def _plan(self, plan_id: UUID) -> ServicePlan:
        if plan_id not in self._plans:
            self._plans[plan_id] = self._repository.load_plan(plan_id)
        return self._plans[plan_id]

    @staticmethod
    def _placeholder_window(plan: ServicePlan, now: datetime) -> WindowRecord:
        return WindowRecord(uuid4(), plan.plan_id, plan.cohort_id, "RESIDENT", "RESIDENT", now, now, plan.deadline,
                            "RUNNING")

    def _lane_budget(self, claim: WindowClaim) -> float:
        remaining = (claim.window.deadline - self._clock()).total_seconds()
        return max(0.0, min(self._max_lane_seconds, max(0.0, remaining) + self._lane_grace_seconds))

    def _run_round(self, claim: WindowClaim, plan: ServicePlan, window_stages: frozenset[str],
                   lanes: Mapping[str, set[str]], report: WindowReport, cancelled: threading.Event,
                   abandoned: set[str]) -> bool:
        progressed: dict[str, bool] = {}
        failures: dict[str, BaseException] = {}

        def lane_worker(profile: str) -> None:
            try:
                progressed[profile] = self._advance_lane(claim, plan, window_stages, lanes[profile], report,
                                                         cancelled)
            except LeaseLost as error:
                failures[profile] = error
            except Exception as error:  # noqa: BLE001 - a lane failure must not stop the other lanes
                logger.exception("orchestrator lane %s failed", profile)
                report.lane_errors[profile] = type(error).__name__
                progressed[profile] = False

        active = [profile for profile in sorted(lanes) if profile not in abandoned]
        threads = {profile: threading.Thread(target=lane_worker, args=(profile,), daemon=True,
                                             name=f"stage-lane-{profile}") for profile in active}
        end = time.monotonic() + self._lane_budget(claim)
        last_beat = time.monotonic()
        groups = [active] if self._parallel else [[profile] for profile in active]
        for group in groups:
            for profile in group:
                threads[profile].start()
            while any(threads[profile].is_alive() for profile in group):
                if time.monotonic() >= end:
                    break
                threads[next(p for p in group if threads[p].is_alive())].join(timeout=0.05)
                if time.monotonic() - last_beat >= self._heartbeat_seconds:
                    self._repository.renew_lease(claim, self._clock(), self._lease_seconds)
                    last_beat = time.monotonic()
            overrun = [profile for profile in group if threads[profile].is_alive()]
            if overrun:
                cancelled.set()
                self._time_out_lanes(claim, plan, overrun, lanes, report)
                abandoned.update(overrun)
        for profile, error in list(failures.items()):
            if profile not in abandoned:  # an abandoned lane's late LeaseLost is expected and ignored
                raise error
        return any(progressed.get(profile, False) for profile in active) or bool(abandoned & set(active))

    def _time_out_lanes(self, claim: WindowClaim, plan: ServicePlan, overrun: list[str],
                        lanes: Mapping[str, set[str]], report: WindowReport) -> None:
        now = self._clock()
        stage_ids = sorted(name for profile in overrun for name in lanes[profile])
        for run in self._repository.timeout_running_stages(claim, stage_ids, now):
            self._repository.upsert_readiness(plan.cohort_id, run.stage_id, plan.stage_catalog_version, "BLOCKED",
                                              ("STAGE_TIMEOUT",), None, now)
            report.executed.append((run.stage_id, "FAILED"))
        for profile in overrun:
            report.lane_errors[profile] = "STAGE_TIMEOUT"

    def _decision_states(self, claim: WindowClaim) -> tuple[Mapping[str, StageRunRecord], dict[str, str]]:
        records = self._repository.stage_states(claim.window.window_id)
        states = {}
        for stage_id, record in records.items():
            if (record.state == "FAILED" and OWNER_LOST in record.reason_codes
                    and record.attempt < self._max_attempts):
                continue  # a crashed attempt is retried as a new attempt
            states[stage_id] = record.state
        return records, states

    def _advance_lane(self, claim: WindowClaim, plan: ServicePlan, window_stages: frozenset[str],
                      lane: set[str], report: WindowReport, cancelled: threading.Event) -> bool:
        progressed = False
        while True:
            if cancelled.is_set():
                return progressed  # the coordinator abandoned this lane; start nothing further
            if self._clock() >= claim.window.deadline:
                return progressed  # a stage never starts after its window deadline; _conclude records MISSED
            records, states = self._decision_states(claim)
            decisions = decide_window(window_stages, states, self._repository.readiness(plan.cohort_id),
                                      self._stages)
            step = next((decisions[name] for name in self._stages
                         if name in lane and decisions[name].action in (Action.RUN, Action.BLOCK_PREREQ)), None)
            if step is None:
                return progressed
            progressed = True
            stage = self._stages[step.stage_id]
            now = self._clock()
            if step.action is Action.BLOCK_PREREQ:
                self._repository.record_blocked(claim, step.stage_id, plan.stage_catalog_version, step.blocked_by,
                                                states[step.blocked_by], now)
                self._repository.upsert_readiness(
                    plan.cohort_id, step.stage_id, plan.stage_catalog_version, "BLOCKED",
                    (f"PREREQUISITE_{states[step.blocked_by]}:{step.blocked_by}",), None, now)
                report.executed.append((step.stage_id, "BLOCKED_PREREQ"))
                continue
            previous = records.get(step.stage_id)
            attempt = previous.attempt + 1 if previous is not None else 1
            self._repository.renew_lease(claim, now, self._lease_seconds)
            run = self._repository.start_stage(claim, stage, plan.stage_catalog_version, attempt, now)
            outcome = self._invoke(self._runners[step.stage_id], StageContext(
                plan, claim.window, stage, run.stage_run_id, attempt, now, self._database, cancelled,
                fenced=True))
            self._finish(claim, plan, run, outcome)
            report.executed.append((step.stage_id, outcome.state))

    def _invoke(self, runner: StageRunner, context: StageContext) -> StageOutcome:
        try:
            outcome = runner(context)
        except LeaseLost:
            raise
        except Exception as error:  # noqa: BLE001 - recorded against this stage only
            logger.exception("stage %s raised", context.stage.stage_id)
            return StageOutcome.failed(f"STAGE_EXCEPTION:{type(error).__name__}")
        if not isinstance(outcome, StageOutcome):
            return StageOutcome.failed("STAGE_RETURNED_INVALID_OUTCOME")
        return outcome

    def _finish(self, claim: WindowClaim, plan: ServicePlan, run: StageRunRecord, outcome: StageOutcome) -> None:
        receipt_id = None
        if outcome.receipt is not None:
            try:
                receipt_id = self._repository.record_receipt(claim, outcome.receipt, run)
            except LeaseLost:
                raise
            except Exception as error:  # noqa: BLE001 - the stage is recorded failed, never left RUNNING
                logger.exception("stage %s receipt was rejected", run.stage_id)
                outcome = StageOutcome.failed(f"RECEIPT_REJECTED:{type(error).__name__}")
        now = self._clock()
        self._repository.finish_stage(claim, run, outcome.state, outcome.reason_codes, receipt_id, now)
        if receipt_id is not None and outcome.receipt is not None and outcome.receipt.advance_release:
            self._repository.advance_release(claim, receipt_id, outcome.receipt.cutoff, now)
        status = "BLOCKED" if outcome.state != "COMPLETE" else ("DEGRADED" if outcome.reason_codes else "READY")
        self._repository.upsert_readiness(plan.cohort_id, run.stage_id, plan.stage_catalog_version, status,
                                          outcome.reason_codes, outcome.evidence_ref, now)

    def _conclude(self, claim: WindowClaim, plan: ServicePlan, window_stages: frozenset[str],
                  report: WindowReport) -> None:
        records, _ = self._decision_states(claim)
        now = self._clock()
        pending = sorted(name for name in window_stages
                         if name not in records or records[name].state in ("PENDING", "RUNNING")
                         or (records[name].state == "FAILED" and OWNER_LOST in records[name].reason_codes
                             and records[name].attempt < self._max_attempts))
        if pending and now >= claim.window.deadline:
            readiness = self._repository.readiness(plan.cohort_id)
            for name in pending:
                waiting_on = next((item for item in self._stages[name].prerequisites
                                   if self._stages[item].schedule_kind in RESIDENT_SCHEDULES
                                   and readiness.get(item) != "READY"), None)
                reason = f"PREREQUISITE_NOT_READY:{waiting_on}" if waiting_on else "WINDOW_DEADLINE_PASSED"
                self._repository.record_missed(claim, name, plan.stage_catalog_version, reason, now)
                self._repository.upsert_readiness(plan.cohort_id, name, plan.stage_catalog_version, "BLOCKED",
                                                  (reason,), None, now)
                report.executed.append((name, "MISSED"))
            records, _ = self._decision_states(claim)
            pending = []
        if pending:
            self._repository.release_window(claim, now)
            report.status, report.window_state = "PENDING_STAGES", "PENDING"
            return
        states = {name: records[name].state for name in window_stages}
        if all(state == "COMPLETE" for state in states.values()):
            final = "COMPLETE"
        elif any(state == "MISSED" for state in states.values()):
            final = "MISSED"
        else:
            final = "FAILED"
        self._repository.finish_window(claim, final, now)
        report.status, report.window_state = "FINISHED", final
