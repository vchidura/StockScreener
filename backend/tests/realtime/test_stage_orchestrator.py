"""Stage orchestrator scenarios, run against an in-memory repository and (opt-in) the real V2 tables.

The DB-backed variant needs STOCKS_V2_DB_TESTS=1 and pytest run without the offline I/O guard
plugin; it provisions the disposable database test_realtime_v2_b1 and always drops it.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.orchestration_repository import (
    CycleReceiptSpec,
    LeaseLost,
    PostgresOrchestrationRepository,
    ServicePlan,
    WindowSpec,
)
from realtime.stage_graph import STAGE_CATALOGS, STAGES_V1, STAGES_V2, RESIDENT_SCHEDULES
from realtime.stage_orchestrator import (
    CatalogMismatch,
    MissingRunner,
    StageOrchestrator,
    StageOutcome,
)

from orchestration_fakes import InMemoryOrchestrationRepository
from v2_database import db_tests_enabled, disposable_v2_database, insert_cohort, insert_service_plan

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
ALL = frozenset(STAGES_V1)
WINDOW_STAGES = frozenset(name for name, stage in STAGES_V1.items() if stage.schedule_kind not in RESIDENT_SCHEDULES)
EQUITY = frozenset(name for name, stage in STAGES_V1.items() if stage.domain == "equity")
OPTIONS_WINDOW = frozenset(name for name in WINDOW_STAGES if STAGES_V1[name].domain == "options")


class Clock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


@dataclass
class Harness:
    repository: object
    plan: ServicePlan
    clock: Clock
    query_release: object
    query_runs: object
    query_receipts: object = None
    counter: int = 0
    database: object = None
    pool: object = None

    def window(self, key: str | None = None, *, cutoff_offset: int = -90):
        self.counter += 1
        spec = WindowSpec(self.plan.cohort_id, "MASSIVE_DEVELOPER", key or f"w{self.counter}", NOW,
                          NOW + timedelta(seconds=cutoff_offset), NOW + timedelta(seconds=300))
        return self.repository.ensure_window(self.plan.plan_id, spec)

    def orchestrator(self, runners, **kwargs):
        defaults = {stage: (lambda context: StageOutcome.complete()) for stage in self.plan.enabled_stages}
        defaults.update(runners)
        kwargs.setdefault("clock", self.clock)
        kwargs.setdefault("database", self.database)
        kwargs.setdefault("stages", STAGE_CATALOGS[self.plan.stage_catalog_version])
        kwargs.setdefault("catalog_version", self.plan.stage_catalog_version)
        return StageOrchestrator(self.repository, defaults, **kwargs)

    def mark_reference_ready(self):
        self.repository.upsert_readiness(self.plan.cohort_id, "REFERENCE", "stages_v1", "READY", (), None, NOW)

    def states(self, window_id):
        return {stage: record.state for stage, record in self.repository.stage_states(window_id).items()}


def _memory_harness(enabled=ALL, *, catalog=STAGES_V1, version="stages_v1") -> Harness:
    plan = ServicePlan(uuid4(), uuid4(), date(2026, 10, 12), version, frozenset(enabled), "a" * 64,
                       NOW + timedelta(hours=8))
    repository = InMemoryOrchestrationRepository(plan, catalog)
    return Harness(
        repository, plan, Clock(),
        query_release=lambda: next(iter(repository.release.values()), None),
        query_runs=lambda window_id: [(r["stage"], r["attempt"], r["state"], tuple(r["reasons"]))
                                      for r in repository.runs if r["window"] == window_id],
        query_receipts=lambda window_id: sum(1 for key in repository.receipts if key[0] == window_id))


@pytest.fixture(scope="module")
def database():
    if not db_tests_enabled():
        pytest.skip("DB-backed variant is opt-in (STOCKS_V2_DB_TESTS=1, no offline I/O guard)")
    with disposable_v2_database("test_realtime_v2_b1") as pool:
        yield pool


def _postgres_harness(pool, enabled=ALL) -> Harness:
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True))
    provider.verify()
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cohort = insert_cohort(cursor, NOW)
            plan_id = insert_service_plan(cursor, cohort, enabled, NOW)
        connection.commit()
    finally:
        pool.putconn(connection)
    plan = ServicePlan(plan_id, cohort, NOW.date(), "stages_v1", frozenset(enabled), "a" * 64, NOW + timedelta(hours=8))

    def query(statement, parameters):
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                return cursor.fetchall()
        finally:
            connection.rollback()
            pool.putconn(connection)

    return Harness(
        PostgresOrchestrationRepository(provider), plan, Clock(),
        query_release=lambda: next(iter(query(
            "SELECT receipt_id, cutoff FROM rt.release_current WHERE cohort_id = %s", (str(cohort),))), None),
        query_runs=lambda window_id: [(r[0], r[1], r[2], tuple(r[3])) for r in query(
            "SELECT stage_id, attempt, state, reason_codes FROM rt.stage_runs WHERE window_id = %s "
            "ORDER BY started_at NULLS LAST, stage_id, attempt", (str(window_id),))],
        query_receipts=lambda window_id: query(
            "SELECT count(*) FROM rt.cycle_receipts WHERE window_id = %s", (str(window_id),))[0][0],
        database=provider, pool=pool)


@pytest.fixture(params=["memory", "postgres"])
def harness(request):
    if request.param == "memory":
        return _memory_harness()
    return _postgres_harness(request.getfixturevalue("database"))


@pytest.fixture
def memory():
    return _memory_harness()


def test_all_window_stages_complete_in_dependency_order(harness):
    harness.mark_reference_ready()
    window = harness.window()
    report = harness.orchestrator({}).run_window(window.window_id)
    assert (report.status, report.window_state) == ("FINISHED", "COMPLETE")
    assert set(harness.states(window.window_id)) == WINDOW_STAGES
    assert set(harness.states(window.window_id).values()) == {"COMPLETE"}
    order = [stage for stage, _state in report.executed]
    assert order.index("EQUITY_INGEST") < order.index("EQUITY_EVIDENCE") < order.index("PORTAL_PUBLISH")
    assert order.index("OPTIONS_ACQUIRE") < order.index("OPTIONS_ADMISSION") < order.index("OPTIONS_CYCLE")


def test_blocked_options_admission_leaves_every_other_stage_running(harness):
    harness.mark_reference_ready()
    window = harness.window()
    cycle_invoked = []
    runners = {
        "OPTIONS_ADMISSION": lambda context: StageOutcome.blocked("REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE"),
        "OPTIONS_CYCLE": lambda context: cycle_invoked.append(True) or StageOutcome.complete(),
    }
    report = harness.orchestrator(runners).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert {states[name] for name in EQUITY - {"REFERENCE"}} == {"COMPLETE"}
    assert states["OPTIONS_ACQUIRE"] == "COMPLETE"
    assert states["OPTIONS_ADMISSION"] == "BLOCKED_POLICY"
    assert states["OPTIONS_CYCLE"] == "BLOCKED_PREREQ"
    assert not cycle_invoked
    assert report.window_state == "FAILED"
    readiness = harness.repository.readiness(harness.plan.cohort_id)
    assert readiness["PORTAL_PUBLISH"] == readiness["STOCK_ALERTS"] == "READY"
    assert readiness["OPTIONS_ADMISSION"] == "BLOCKED" and readiness["OPTIONS_CYCLE"] == "BLOCKED"
    runs = harness.query_runs(window.window_id)
    assert ("OPTIONS_ADMISSION", 1, "BLOCKED_POLICY", ("REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE",)) in runs
    assert ("OPTIONS_CYCLE", 1, "BLOCKED_PREREQ", ("PREREQUISITE_BLOCKED_POLICY:OPTIONS_ADMISSION",)) in runs


def test_options_exception_never_blocks_equity_and_its_message_is_not_persisted(harness):
    harness.mark_reference_ready()
    window = harness.window()

    def explode(context):
        raise RuntimeError("provider said token=SECRET-VALUE")

    report = harness.orchestrator({"OPTIONS_ACQUIRE": explode}).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert states["OPTIONS_ACQUIRE"] == "FAILED"
    assert states["OPTIONS_ADMISSION"] == "BLOCKED_PREREQ" and states["OPTIONS_CYCLE"] == "BLOCKED_PREREQ"
    assert {states[name] for name in EQUITY - {"REFERENCE"}} == {"COMPLETE"}
    persisted = repr(harness.query_runs(window.window_id)) + repr(harness.repository.readiness(harness.plan.cohort_id))
    assert "SECRET-VALUE" not in persisted and "STAGE_EXCEPTION:RuntimeError" in persisted
    assert report.window_state == "FAILED"


def test_a_slow_options_lane_does_not_delay_the_equity_lane(harness):
    harness.mark_reference_ready()
    window = harness.window()
    equity_published = threading.Event()

    def slow_options(context):
        # Completes only once the equity lane has finished its portal publication; a sequential
        # orchestrator would reach this stage first and time out.
        return StageOutcome.complete() if equity_published.wait(timeout=20) else StageOutcome.failed("EQUITY_WAS_BLOCKED")

    def publish(context):
        equity_published.set()
        return StageOutcome.complete()

    report = harness.orchestrator({"OPTIONS_ACQUIRE": slow_options, "PORTAL_PUBLISH": publish}).run_window(
        window.window_id)
    assert harness.states(window.window_id)["OPTIONS_ACQUIRE"] == "COMPLETE"
    assert report.window_state == "COMPLETE"


def test_a_slow_v2_portal_lane_does_not_delay_the_alert_lane():
    enabled = frozenset({"STOCK_ALERTS", "EQUITY_PROJECTION"})
    harness = _memory_harness(enabled, catalog=STAGES_V2, version="stages_v2")
    harness.repository.upsert_readiness(
        harness.plan.cohort_id, "EQUITY_MATERIALIZE", "stages_v2", "READY", (), None, NOW,
    )
    window = harness.window()
    alerted = threading.Event()

    def slow_projection(context):
        return StageOutcome.complete() if alerted.wait(timeout=20) else StageOutcome.failed("ALERT_WAS_BLOCKED")

    def publish_alert(context):
        alerted.set()
        return StageOutcome.complete()

    report = harness.orchestrator({
        "EQUITY_PROJECTION": slow_projection,
        "STOCK_ALERTS": publish_alert,
    }).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert states["STOCK_ALERTS"] == states["EQUITY_PROJECTION"] == "COMPLETE"
    assert report.window_state == "COMPLETE"


def test_a_v2_alert_deadline_timeout_does_not_cancel_the_longer_portal_lane():
    catalog = dict(STAGES_V2)
    catalog["STOCK_ALERTS"] = replace(catalog["STOCK_ALERTS"], deadline_offset_seconds=1)
    catalog["EQUITY_PROJECTION"] = replace(catalog["EQUITY_PROJECTION"], deadline_offset_seconds=3)
    enabled = frozenset({"STOCK_ALERTS", "EQUITY_PROJECTION"})
    harness = _memory_harness(enabled, catalog=catalog, version="stages_v2")
    harness.repository.upsert_readiness(
        harness.plan.cohort_id, "EQUITY_MATERIALIZE", "stages_v2", "READY", (), None, NOW,
    )
    window = harness.window(cutoff_offset=-90)
    portal_finished = threading.Event()

    def hung_alert(context):
        while not context.is_cancelled():
            time.sleep(0.01)
        return StageOutcome.complete()

    def slow_portal(context):
        time.sleep(1.5)
        portal_finished.set()
        return StageOutcome.complete()

    report = harness.orchestrator(
        {"STOCK_ALERTS": hung_alert, "EQUITY_PROJECTION": slow_portal},
        max_lane_seconds=5,
        lane_grace_seconds=0,
        heartbeat_seconds=0.1,
    ).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert portal_finished.is_set()
    assert states["STOCK_ALERTS"] == "FAILED"
    assert states["EQUITY_PROJECTION"] == "COMPLETE"
    assert report.lane_errors == {"alert": "STAGE_TIMEOUT"}


def test_an_equity_failure_blocks_only_its_declared_dependents(harness):
    harness.mark_reference_ready()
    window = harness.window()
    report = harness.orchestrator(
        {"EQUITY_EVIDENCE": lambda context: StageOutcome.failed("EVIDENCE_INCOMPLETE")}).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert states["EQUITY_EVIDENCE"] == "FAILED"
    assert {states[name] for name in ("PORTAL_PUBLISH", "STRATEGY_PUBLISH", "STOCK_ALERTS")} == {"BLOCKED_PREREQ"}
    assert {states[name] for name in OPTIONS_WINDOW} == {"COMPLETE"}
    assert states["EQUITY_INGEST"] == "COMPLETE" and report.window_state == "FAILED"


def test_invalid_runner_output_fails_only_that_stage(harness):
    harness.mark_reference_ready()
    window = harness.window()
    harness.orchestrator({"STOCK_ALERTS": lambda context: "ok"}).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert states["STOCK_ALERTS"] == "FAILED"
    assert states["PORTAL_PUBLISH"] == "COMPLETE" and states["OPTIONS_CYCLE"] == "COMPLETE"
    assert ("STOCK_ALERTS", 1, "FAILED", ("STAGE_RETURNED_INVALID_OUTCOME",)) in harness.query_runs(window.window_id)


def test_degraded_completion_reports_degraded_readiness(harness):
    harness.mark_reference_ready()
    window = harness.window()
    harness.orchestrator({"EQUITY_INGEST": lambda context: StageOutcome.complete(
        degraded=("PARTIAL_SYMBOL_COVERAGE",))}).run_window(window.window_id)
    readiness = harness.repository.readiness(harness.plan.cohort_id)
    assert readiness["EQUITY_INGEST"] == "DEGRADED" and readiness["EQUITY_EVIDENCE"] == "READY"
    assert harness.states(window.window_id)["EQUITY_EVIDENCE"] == "COMPLETE"


def test_orphaned_running_stage_is_failed_owner_lost_and_retried_as_attempt_two(harness):
    harness.mark_reference_ready()
    window = harness.window()
    repository = harness.repository
    dead = repository.claim_window(window.window_id, uuid4(), NOW, 30)
    repository.start_stage(dead, STAGES_V1["EQUITY_INGEST"], "stages_v1", 1, NOW)
    harness.clock.value = NOW + timedelta(seconds=60)  # the dead owner's lease expired
    report = harness.orchestrator({}).run_window(window.window_id)
    runs = harness.query_runs(window.window_id)
    assert ("EQUITY_INGEST", 1, "FAILED", ("OWNER_LOST",)) in runs
    assert ("EQUITY_INGEST", 2, "COMPLETE", ()) in runs
    assert report.window_state == "COMPLETE"


def test_a_second_crash_exhausts_attempts_and_blocks_dependents(harness):
    harness.mark_reference_ready()
    window = harness.window()
    repository = harness.repository
    ingest = STAGES_V1["EQUITY_INGEST"]
    first = repository.claim_window(window.window_id, uuid4(), NOW, 30)
    repository.start_stage(first, ingest, "stages_v1", 1, NOW)
    second = repository.claim_window(window.window_id, uuid4(), NOW + timedelta(seconds=40), 30)
    repository.recover_orphans(second, NOW + timedelta(seconds=40))
    repository.start_stage(second, ingest, "stages_v1", 2, NOW + timedelta(seconds=40))
    harness.clock.value = NOW + timedelta(seconds=100)
    harness.orchestrator({}).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert states["EQUITY_INGEST"] == "FAILED" and states["EQUITY_EVIDENCE"] == "BLOCKED_PREREQ"
    assert harness.repository.stage_states(window.window_id)["EQUITY_INGEST"].attempt == 2
    assert states["OPTIONS_CYCLE"] == "COMPLETE"


def test_a_window_held_by_a_live_owner_is_not_claimed_and_a_stale_owner_is_fenced(harness):
    window = harness.window()
    repository = harness.repository
    holder = repository.claim_window(window.window_id, uuid4(), NOW, 60)
    assert holder is not None
    other = harness.orchestrator({}).run_window(window.window_id)
    assert other.status == "NOT_CLAIMED"
    run = repository.start_stage(holder, STAGES_V1["EQUITY_INGEST"], "stages_v1", 1, NOW)
    successor = repository.claim_window(window.window_id, uuid4(), NOW + timedelta(seconds=61), 60)
    assert successor is not None and successor.epoch == holder.epoch + 1
    with pytest.raises(LeaseLost):
        repository.finish_stage(holder, run, "COMPLETE", (), None, NOW + timedelta(seconds=62))
    assert repository.renew_lease(holder, NOW + timedelta(seconds=62), 60) is False


def test_stages_past_the_window_deadline_are_missed_never_run_late(harness):
    harness.mark_reference_ready()
    window = harness.window()
    harness.clock.value = NOW + timedelta(seconds=301)
    invoked = []
    runners = {name: (lambda context, name=name: invoked.append(name) or StageOutcome.complete())
               for name in WINDOW_STAGES}
    report = harness.orchestrator(runners).run_window(window.window_id)
    assert not invoked
    assert set(harness.states(window.window_id).values()) == {"MISSED"}
    assert report.window_state == "MISSED"
    assert all(reasons == ("WINDOW_DEADLINE_PASSED",) for _s, _a, state, reasons in harness.query_runs(window.window_id)
               if state == "MISSED")


def test_unready_reference_leaves_window_stages_waiting_then_missed_at_the_deadline(harness):
    window = harness.window()
    orchestrator = harness.orchestrator({})
    first = orchestrator.run_window(window.window_id)
    assert (first.status, first.window_state) == ("PENDING_STAGES", "PENDING")
    assert harness.states(window.window_id) == {}
    harness.clock.value = NOW + timedelta(seconds=301)
    last = orchestrator.run_window(window.window_id)
    assert last.window_state == "MISSED"
    runs = harness.query_runs(window.window_id)
    assert ("EQUITY_INGEST", 1, "MISSED", ("PREREQUISITE_NOT_READY:REFERENCE",)) in runs
    assert ("OPTIONS_ACQUIRE", 1, "MISSED", ("PREREQUISITE_NOT_READY:REFERENCE",)) in runs


def test_receipt_and_release_pointer_advance_monotonically(harness):
    harness.mark_reference_ready()

    def receipt(cutoff, digest):
        return CycleReceiptSpec("EQUITY_CYCLE", cutoff, 7, "b" * 64, digest * 64, "build-1", advance_release=True)

    newer = harness.window("newer", cutoff_offset=-60)
    harness.orchestrator({"EQUITY_EVIDENCE": lambda context: StageOutcome.complete(
        receipt=receipt(newer.cutoff, "c"))}).run_window(newer.window_id)
    pointer = harness.query_release()
    assert pointer is not None and pointer[1] == newer.cutoff
    older = harness.window("older", cutoff_offset=-120)
    harness.orchestrator({"EQUITY_EVIDENCE": lambda context: StageOutcome.complete(
        receipt=receipt(older.cutoff, "d"))}).run_window(older.window_id)
    assert harness.query_release() == pointer


def test_a_rejected_receipt_fails_that_stage_instead_of_leaving_it_running(harness):
    harness.mark_reference_ready()
    window = harness.window()
    bad = CycleReceiptSpec("EQUITY_CYCLE", NOW, 0, "b" * 64, "c" * 64, "build-1")  # reference_generation must be > 0
    harness.orchestrator({"EQUITY_EVIDENCE": lambda context: StageOutcome.complete(receipt=bad)}).run_window(
        window.window_id)
    assert harness.states(window.window_id)["EQUITY_EVIDENCE"] == "FAILED"
    reasons = [r for stage, _a, state, r in harness.query_runs(window.window_id) if stage == "EQUITY_EVIDENCE"]
    assert reasons and reasons[0][0].startswith("RECEIPT_REJECTED:")


def test_resident_stage_failure_is_isolated_and_reported_through_readiness(harness):
    harness.mark_reference_ready()
    orchestrator = harness.orchestrator({
        "OPTIONS_FLOW": lambda context: (_ for _ in ()).throw(ConnectionError("feed down")),
    })
    orchestrator.start(harness.plan.plan_id)
    outcome = orchestrator.tick_resident(harness.plan.plan_id, "OPTIONS_FLOW")
    assert outcome.reason_codes == ("STAGE_EXCEPTION:ConnectionError",)
    readiness = harness.repository.readiness(harness.plan.cohort_id)
    assert readiness["OPTIONS_FLOW"] == "BLOCKED" and readiness["REFERENCE"] == "READY"
    window = harness.window()
    assert orchestrator.run_window(window.window_id).window_state == "COMPLETE"
    with pytest.raises(ValueError):
        orchestrator.tick_resident(harness.plan.plan_id, "EQUITY_INGEST")


def test_start_reports_scope_and_never_requires_options_for_an_equity_only_plan(memory):
    equity_only = _memory_harness(enabled=frozenset({"REFERENCE", *EQUITY}))
    report = equity_only.orchestrator({}).start(equity_only.plan.plan_id)
    assert report.lanes == frozenset({"data"})
    assert report.resident_stages == frozenset({"REFERENCE"})
    memory.repository.upsert_readiness(memory.plan.cohort_id, "OPTIONS_ADMISSION", "stages_v1", "BLOCKED",
                                       ("REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE",), None, NOW)
    full = memory.orchestrator({}).start(memory.plan.plan_id)
    assert full.lanes == frozenset({"data", "options"})
    assert EQUITY <= full.startable and "OPTIONS_FLOW" in full.startable
    assert {"OPTIONS_ADMISSION", "OPTIONS_CYCLE"} <= full.held


def test_start_fails_closed_on_catalog_drift_or_a_missing_runner(memory):
    drifted = dict(STAGES_V1)
    stage = drifted["OPTIONS_CYCLE"]
    drifted["OPTIONS_CYCLE"] = type(stage)(
        stage.stage_id, stage.domain, ("EQUITY_EVIDENCE",), stage.lane, stage.schedule_kind, stage.gates_strategy,
    )
    memory.repository.catalog = drifted
    with pytest.raises(CatalogMismatch):
        memory.orchestrator({}).start(memory.plan.plan_id)
    memory.repository.catalog = dict(STAGES_V1)
    runners = {name: (lambda context: StageOutcome.complete()) for name in ALL - {"OPTIONS_FLOW"}}
    with pytest.raises(MissingRunner, match="OPTIONS_FLOW"):
        StageOrchestrator(memory.repository, runners, clock=memory.clock).start(memory.plan.plan_id)


def test_a_hung_options_stage_times_out_without_starving_equity_or_later_windows(harness):
    harness.mark_reference_ready()
    first = harness.window()
    late_receipt = CycleReceiptSpec("OPTIONS_CYCLE", NOW, 3, "b" * 64, "e" * 64, "build-1")
    exited = threading.Event()

    def hung_options(context):
        # A runner that ignores its deadline until it is told it was abandoned, then tries to commit a receipt.
        while not context.is_cancelled():
            time.sleep(0.01)
        exited.set()
        return StageOutcome.complete(receipt=late_receipt)

    orchestrator = harness.orchestrator({"OPTIONS_ACQUIRE": hung_options}, max_lane_seconds=0.4, heartbeat_seconds=0.1)
    started = time.monotonic()
    report = orchestrator.run_window(first.window_id)
    assert time.monotonic() - started < 10
    states = harness.states(first.window_id)
    assert {states[name] for name in EQUITY - {"REFERENCE"}} == {"COMPLETE"}
    assert states["OPTIONS_ACQUIRE"] == "FAILED" and "OPTIONS_ADMISSION" not in states
    assert report.lane_errors == {"options": "STAGE_TIMEOUT"}
    assert report.status == "PENDING_STAGES"
    assert ("OPTIONS_ACQUIRE", 1, "FAILED", ("STAGE_TIMEOUT",)) in harness.query_runs(first.window_id)
    assert harness.repository.readiness(harness.plan.cohort_id)["OPTIONS_ACQUIRE"] == "BLOCKED"
    # The abandoned runner finishes late: nothing it writes is accepted (no receipt, state unchanged).
    assert exited.wait(timeout=10)
    for thread in threading.enumerate():
        if thread.name == "stage-lane-options":
            thread.join(timeout=10)
    assert harness.query_receipts(first.window_id) == 0
    assert harness.states(first.window_id)["OPTIONS_ACQUIRE"] == "FAILED"
    # The next poll of the same window records the dependents as blocked; a later window is not delayed.
    again = harness.orchestrator({}).run_window(first.window_id)
    assert again.window_state == "FAILED"
    assert harness.states(first.window_id)["OPTIONS_CYCLE"] == "BLOCKED_PREREQ"
    second = harness.window()
    began = time.monotonic()
    later = harness.orchestrator({}).run_window(second.window_id)
    assert later.window_state == "COMPLETE" and time.monotonic() - began < 5


def test_a_stage_that_ignores_cancellation_cannot_commit_ordinary_data_after_its_timeout(database):
    """R1: the commit is checked against the stage run in its own transaction, so a runner that never
    polls is_cancelled() still cannot land data after STAGE_TIMEOUT; a healthy stage's data commits."""
    harness = _postgres_harness(database)
    harness.mark_reference_ready()
    connection = database.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS public.fence_probe (note text NOT NULL)")
            cursor.execute("TRUNCATE public.fence_probe")
        connection.commit()
    finally:
        database.putconn(connection)
    release, exited, outcomes = threading.Event(), threading.Event(), {}

    def healthy(context):
        with context.transaction() as unit:
            unit.cursor.execute("INSERT INTO public.fence_probe VALUES ('healthy')")
        return StageOutcome.complete()

    def ignores_cancellation(context):
        try:
            with context.transaction() as unit:
                unit.cursor.execute("INSERT INTO public.fence_probe VALUES ('after-timeout')")
                release.wait(timeout=20)  # never polls context.is_cancelled()
        except Exception as error:  # noqa: BLE001 - the scenario records what the fence did
            outcomes["commit"] = type(error).__name__
        try:
            context.transaction()
        except Exception as error:  # noqa: BLE001
            outcomes["reopen"] = type(error).__name__
        exited.set()
        return StageOutcome.complete()

    window = harness.window()
    orchestrator = harness.orchestrator({"EQUITY_INGEST": healthy, "OPTIONS_ACQUIRE": ignores_cancellation},
                                        max_lane_seconds=0.4, heartbeat_seconds=0.1)
    try:
        orchestrator.run_window(window.window_id)
        states = harness.states(window.window_id)
        assert states["OPTIONS_ACQUIRE"] == "FAILED" and states["EQUITY_INGEST"] == "COMPLETE"
        release.set()
        assert exited.wait(timeout=10)
        for thread in threading.enumerate():
            if thread.name == "stage-lane-options":
                thread.join(timeout=10)
        assert outcomes == {"commit": "WriteFenceRejected", "reopen": "StageCancelled"}
        connection = database.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT note FROM public.fence_probe ORDER BY note")
                assert [row[0] for row in cursor.fetchall()] == ["healthy"]
        finally:
            connection.rollback()
            database.putconn(connection)
    finally:
        release.set()
        connection = database.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS public.fence_probe")
            connection.commit()
        finally:
            database.putconn(connection)


def test_the_stage_database_adapter_runs_ported_code_on_the_real_schema_with_dict_rows(database):
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2StageDatabase

    harness = _postgres_harness(database)
    harness.mark_reference_ready()
    seen = {}

    def runner(context):
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        with compute.transaction(dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
            cursor.execute("SELECT lineage_name FROM rt.database_identity")
            seen["row"] = cursor.fetchone()
        return StageOutcome.complete()

    window = harness.window()
    harness.orchestrator({"EQUITY_INGEST": runner}).run_window(window.window_id)
    assert seen["row"] == {"lineage_name": "advanced_realtime_v2"}
    assert harness.states(window.window_id)["EQUITY_INGEST"] == "COMPLETE"


def test_the_equity_runners_run_through_the_orchestrator_on_the_real_schema(database, monkeypatch):
    """EQUITY_EVIDENCE reports a durable EQUITY_CYCLE receipt whose generation comes from the READY seed
    manifests; PORTAL_PUBLISH is dispatched from that row and publishes the handed-off cycle."""
    from types import SimpleNamespace

    import realtime.stage_runners_equity as equity_runners
    from realtime.runner_environment import RunnerEnvironment
    from realtime.screening_projection import RealtimeScreeningInput
    from realtime.preflight import ProviderTier

    harness = _postgres_harness(database)
    harness.mark_reference_ready()
    connection = database.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.seed_manifests (seed_id, seed_sha256, universe_sha256, session_date, status, "
                "member_count, created_at, effective_at, valid_until, source_generations) "
                "VALUES (%s, %s, %s, %s, 'READY', 9, %s, %s, %s, '{}'::jsonb)",
                (str(uuid4()), "c" * 64, "d" * 64, NOW.date(), NOW - timedelta(days=1), NOW - timedelta(days=1),
                 NOW + timedelta(days=1)))
        connection.commit()
    finally:
        database.putconn(connection)
    inputs = RealtimeScreeningInput(
        cohort_id=harness.plan.cohort_id, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        source_manifest_sha256="a" * 64, source_generation=1, session=NOW.date(), cutoff=NOW,
        members=(), bars=(), actions=(), coverage=())
    published = []
    monkeypatch.setattr(equity_runners, "compute_realtime_ticker_cycle",
                        lambda database, **scope: SimpleNamespace(inputs=inputs, scope=scope))
    monkeypatch.setattr(equity_runners, "publish_realtime_ticker_cycle",
                        lambda database, cycle, **options: published.append((cycle, options)))
    environment = RunnerEnvironment(harness.database, build_id="build-test")
    window = harness.window()
    report = harness.orchestrator({
        "EQUITY_EVIDENCE": equity_runners.equity_evidence_runner(environment),
        "PORTAL_PUBLISH": equity_runners.portal_publish_runner(environment),
    }).run_window(window.window_id)
    states = harness.states(window.window_id)
    assert states["EQUITY_EVIDENCE"] == "COMPLETE" and states["PORTAL_PUBLISH"] == "COMPLETE", (states, report)
    connection = database.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT receipt_id, reference_generation, build_id, receipt_kind FROM rt.cycle_receipts "
                           "WHERE window_id = %s", (str(window.window_id),))
            stored = cursor.fetchall()
    finally:
        connection.rollback()
        database.putconn(connection)
    assert [(row[1], row[2], row[3]) for row in stored] == [(1, "build-test", "EQUITY_CYCLE")]
    (cycle, options), = published
    assert str(options["receipt"].receipt_id) == str(stored[0][0]) and cycle.scope["source_generation"] == 1
    assert options["receipt"].cohort_id == harness.plan.cohort_id and options["session_close"] is False
    assert len(environment.handoff) == 0


def test_the_coordinator_renews_the_window_lease_while_it_waits_for_a_slow_lane(memory):
    memory.mark_reference_ready()

    class Counting(InMemoryOrchestrationRepository):
        renewals = 0

        def renew_lease(self, claim, now, lease_seconds):
            type(self).renewals += 1
            return super().renew_lease(claim, now, lease_seconds)

    repository = Counting(memory.plan)
    repository.upsert_readiness(memory.plan.cohort_id, "REFERENCE", "stages_v1", "READY", (), None, NOW)
    window = repository.ensure_window(memory.plan.plan_id, WindowSpec(
        memory.plan.cohort_id, "MASSIVE_DEVELOPER", "slow", NOW, NOW - timedelta(seconds=90),
        NOW + timedelta(seconds=300)))

    def slow(context):
        time.sleep(0.5)
        return StageOutcome.complete()

    runners = {name: (lambda context: StageOutcome.complete()) for name in ALL}
    runners["OPTIONS_ACQUIRE"] = slow
    baseline = Counting.renewals
    report = StageOrchestrator(repository, runners, clock=memory.clock, heartbeat_seconds=0.05).run_window(
        window.window_id)
    assert report.window_state == "COMPLETE"
    assert Counting.renewals - baseline >= 5  # per-stage renewals plus at least a few heartbeats


def test_a_blocked_reference_is_reported_at_start_and_holds_only_its_dependents(memory):
    memory.repository.upsert_readiness(memory.plan.cohort_id, "REFERENCE", "stages_v1", "BLOCKED",
                                       ("REFERENCE_REFRESH_FAILED",), None, NOW)
    report = memory.orchestrator({}).start(memory.plan.plan_id)
    assert not ({"EQUITY_INGEST", "OPTIONS_ACQUIRE", "OPTIONS_FLOW"} & report.startable)
    assert {"EQUITY_INGEST", "OPTIONS_ACQUIRE", "OPTIONS_FLOW"} <= report.held


def test_a_repository_failure_in_one_lane_does_not_stop_the_other_lane(memory):
    memory.mark_reference_ready()

    class FlakyOptions(InMemoryOrchestrationRepository):
        def start_stage(self, claim, stage, version, attempt, now):
            if stage.domain == "options":
                raise ConnectionError("options store is unavailable")
            return super().start_stage(claim, stage, version, attempt, now)

    flaky = FlakyOptions(memory.plan)
    flaky.upsert_readiness(memory.plan.cohort_id, "REFERENCE", "stages_v1", "READY", (), None, NOW)
    window = flaky.ensure_window(memory.plan.plan_id, WindowSpec(
        memory.plan.cohort_id, "MASSIVE_DEVELOPER", "w1", NOW, NOW - timedelta(seconds=90),
        NOW + timedelta(seconds=300)))
    report = StageOrchestrator(
        flaky, {name: (lambda context: StageOutcome.complete()) for name in ALL}, clock=memory.clock,
    ).run_window(window.window_id)
    states = {stage: record.state for stage, record in flaky.stage_states(window.window_id).items()}
    assert {states[name] for name in EQUITY - {"REFERENCE"}} == {"COMPLETE"}
    assert report.lane_errors == {"options": "ConnectionError"}
    assert report.status == "PENDING_STAGES"
