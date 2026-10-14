"""EQUITY_EVIDENCE and PORTAL_PUBLISH runners (offline fakes plus a DB variant on the real V2 schema)."""
from __future__ import annotations

import threading
from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

import realtime.stage_runners_equity as runners
from realtime.database_access_v2 import WriteFenceRejected
from realtime.equity_bar_drain import EquityBarDrainResult, lease_owner_for
from realtime.preflight import ProviderTier
from realtime.runner_environment import CycleHandoff, RunnerEnvironment
from realtime.screening_projection import RealtimeScreeningInput
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageCancelled, StageContext, StageOutcome

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
SESSION = date(2026, 10, 12)


def _inputs(cutoff=NOW):
    return RealtimeScreeningInput(
        cohort_id=uuid4(), provider_tier=ProviderTier.MASSIVE_DEVELOPER, source_manifest_sha256="a" * 64,
        source_generation=2, session=SESSION, cutoff=cutoff, members=(), bars=(), actions=(), coverage=())


def _context(stage, *, cancelled=None, database=None, cutoff=NOW):
    plan = SimpleNamespace(cohort_id=uuid4(), session_date=SESSION)
    window = SimpleNamespace(window_id=uuid4(), cutoff=cutoff, deadline=cutoff + timedelta(minutes=16, seconds=30))
    return StageContext(plan, window, STAGES_V1[stage], uuid4(), 1, NOW, database, cancelled, fenced=True)


class _Cursor:
    def __init__(self, rows):
        self.rows, self.statements = rows, []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class _Database:
    """Answers the stage's read-only queries; records every transaction request."""

    def __init__(self, rows):
        self.cursor, self.calls = _Cursor(rows), []

    def transaction(self, role, profile, **options):
        self.calls.append((role, options))
        return nullcontext(SimpleNamespace(cursor=self.cursor))


@pytest.fixture(autouse=True)
def bar_drain_calls(monkeypatch):
    """The bar phase has its own tests (test_equity_bar_drain); here it reports an empty queue."""
    calls = []

    def drain(database, **options):
        calls.append((database.process_plan.role.value, options))
        return EquityBarDrainResult()

    monkeypatch.setattr(runners, "drain_equity_bar_jobs", drain)
    return calls


def test_handoff_is_bounded_and_survives_a_failed_publication_attempt():
    handoff, cohort = CycleHandoff(capacity=2), uuid4()
    for minute in range(3):
        handoff.put(cohort, NOW + timedelta(minutes=minute), f"cycle-{minute}")
    assert len(handoff) == 2 and handoff.peek(cohort, NOW) is None
    assert handoff.peek(cohort, NOW + timedelta(minutes=1)) == "cycle-1"
    assert handoff.peek(cohort, NOW + timedelta(minutes=1)) == "cycle-1"
    handoff.discard(cohort, NOW + timedelta(minutes=1))
    assert handoff.peek(cohort, NOW + timedelta(minutes=1)) is None
    with pytest.raises(ValueError):
        CycleHandoff(capacity=0)


def test_evidence_runner_blocks_without_a_ready_reference_generation(monkeypatch):
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle",
                        lambda *args, **kwargs: pytest.fail("no compute without a reference generation"))
    database = _Database([(0,)])
    outcome = runners.equity_evidence_runner(RunnerEnvironment(database))(_context("EQUITY_EVIDENCE", database=database))
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.REFERENCE_GENERATION_UNAVAILABLE,)
    assert "rt.seed_manifests" in database.cursor.statements[0][0] and "'READY'" in database.cursor.statements[0][0]


def test_evidence_runner_computes_hands_off_and_reports_an_equity_cycle_receipt(monkeypatch):
    seen = {}
    cycle = SimpleNamespace(inputs=_inputs())

    def compute(database, *, state, loader, **scope):
        seen.update(role=database.process_plan.role.value, state=state, loader=loader, scope=scope)
        return cycle

    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle", compute)
    database, environment = _Database([(3,)]), RunnerEnvironment(None, build_id="build-9")
    context = _context("EQUITY_EVIDENCE", database=database)
    outcome = runners.equity_evidence_runner(environment)(context)
    assert outcome.state == "COMPLETE"
    receipt = outcome.receipt
    assert (receipt.receipt_kind, receipt.cutoff, receipt.reference_generation, receipt.build_id) == (
        "EQUITY_CYCLE", NOW, 3, "build-9")
    assert len(receipt.universe_sha256) == len(receipt.input_sha256) == 64
    assert seen["role"] == "COMPUTE" and seen["state"] is environment.ticker_state
    assert seen["loader"] is environment.screening_loader
    assert seen["scope"] == dict(cohort_id=context.plan.cohort_id, session=SESSION, cutoff=NOW,
                                 known_by=context.window.deadline, source_generation=3)
    assert seen["scope"]["known_by"] == NOW + timedelta(minutes=16, seconds=30) != seen["scope"]["cutoff"]
    assert environment.handoff.peek(context.plan.cohort_id, NOW) is cycle


def test_a_cancelled_evidence_stage_computes_and_persists_nothing(monkeypatch):
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle", lambda *a, **k: pytest.fail("cancelled"))
    cancelled = threading.Event()
    cancelled.set()
    database = _Database([(1,)])
    outcome = runners.equity_evidence_runner(RunnerEnvironment(None))(
        _context("EQUITY_EVIDENCE", database=database, cancelled=cancelled))
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def test_a_write_rejected_after_timeout_becomes_a_cancelled_failure_not_a_crash(monkeypatch):
    def compute(database, **scope):
        raise StageCancelled("abandoned")

    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle", compute)
    database = _Database([(1,)])
    outcome = runners.equity_evidence_runner(RunnerEnvironment(None))(_context("EQUITY_EVIDENCE", database=database))
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def test_the_bar_phase_runs_first_under_compute_with_a_lease_owner_derived_from_the_stage_run(monkeypatch):
    seen = {}

    def drain(database, **options):
        seen["statements_before"] = list(database._context.database.cursor.statements)
        seen["role"], seen["options"] = database.process_plan.role.value, options
        return EquityBarDrainResult()

    monkeypatch.setattr(runners, "drain_equity_bar_jobs", drain)
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle",
                        lambda database, **scope: SimpleNamespace(inputs=_inputs()))
    database = _Database([(3,)])
    context = _context("EQUITY_EVIDENCE", database=database)
    assert runners.equity_evidence_runner(RunnerEnvironment(None))(context).state == "COMPLETE"
    assert seen["statements_before"] == []  # no reference-generation query before the bars are materialized
    assert seen["role"] == "COMPUTE"
    assert seen["options"]["cohort_id"] == context.plan.cohort_id
    assert seen["options"]["lease_owner"] == lease_owner_for(context.stage_run_id)
    assert seen["options"]["should_stop"] == context.is_cancelled
    assert lease_owner_for(context.stage_run_id) != lease_owner_for(uuid4())


@pytest.mark.parametrize("drain_result, codes", [
    (EquityBarDrainResult(claimed=3, completed=2, retried=1), ("EQUITY_BAR_MATERIALIZATION_RETRY:1",)),
    (EquityBarDrainResult(claimed=100, completed=100, capped=True), ("EQUITY_BAR_MATERIALIZATION_CAPPED:100",)),
    (EquityBarDrainResult(claimed=100, completed=98, retried=2, capped=True),
     ("EQUITY_BAR_MATERIALIZATION_RETRY:2", "EQUITY_BAR_MATERIALIZATION_CAPPED:100")),
])
def test_retryable_or_capped_bar_work_degrades_the_stage_but_still_reports_the_receipt(monkeypatch, drain_result, codes):
    monkeypatch.setattr(runners, "drain_equity_bar_jobs", lambda database, **options: drain_result)
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle",
                        lambda database, **scope: SimpleNamespace(inputs=_inputs()))
    environment = RunnerEnvironment(None)
    context = _context("EQUITY_EVIDENCE", database=_Database([(3,)]))
    outcome = runners.equity_evidence_runner(environment)(context)
    assert outcome.state == "COMPLETE" and outcome.reason_codes == codes
    assert outcome.receipt is not None and outcome.receipt.reference_generation == 3
    assert environment.handoff.peek(context.plan.cohort_id, NOW) is not None


@pytest.mark.parametrize("drain_result", [EquityBarDrainResult(claimed=2, failed=1, completed=1),
                                          EquityBarDrainResult(claimed=1, blocked=1)])
def test_a_job_that_cannot_be_retried_fails_the_stage_before_any_compute(monkeypatch, drain_result):
    monkeypatch.setattr(runners, "drain_equity_bar_jobs", lambda database, **options: drain_result)
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle", lambda *a, **k: pytest.fail("bars are missing"))
    database = _Database([(3,)])
    outcome = runners.equity_evidence_runner(RunnerEnvironment(None))(
        _context("EQUITY_EVIDENCE", database=database))
    assert outcome.state == "FAILED" and outcome.reason_codes == ("EQUITY_BAR_MATERIALIZATION_FAILED",)
    assert database.cursor.statements == []  # not even the reference generation was read


@pytest.mark.parametrize("error", [StageCancelled("abandoned"), WriteFenceRejected("fenced")])
def test_a_stage_abandoned_during_the_bar_phase_is_a_cancelled_failure(monkeypatch, error):
    def drain(database, **options):
        raise error

    monkeypatch.setattr(runners, "drain_equity_bar_jobs", drain)
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle", lambda *a, **k: pytest.fail("cancelled"))
    outcome = runners.equity_evidence_runner(RunnerEnvironment(None))(
        _context("EQUITY_EVIDENCE", database=_Database([(3,)])))
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def _receipt_row(context):
    return {"receipt_id": uuid4(), "cohort_id": context.plan.cohort_id, "cutoff": NOW,
            "universe_sha256": "b" * 64, "provider_tier": "MASSIVE_DEVELOPER"}


def test_publish_runner_needs_the_durable_receipt_and_the_handoff(monkeypatch):
    published = []
    monkeypatch.setattr(runners, "publish_realtime_ticker_cycle",
                        lambda database, cycle, **options: published.append((database, cycle, options)))
    context = _context("PORTAL_PUBLISH", database=_Database([None]))
    environment = RunnerEnvironment(None)
    assert runners.portal_publish_runner(environment)(context).reason_codes == (runners.EQUITY_CYCLE_RECEIPT_MISSING,)

    database = _Database([_receipt_row(context)])
    context = replace(context, database=database)
    assert runners.portal_publish_runner(environment)(context).reason_codes == (runners.CYCLE_HANDOFF_UNAVAILABLE,)
    assert published == []


def test_publish_runner_publishes_the_handed_off_cycle_for_its_receipt_and_consumes_it_on_success(monkeypatch):
    published = []
    monkeypatch.setattr(runners, "publish_realtime_ticker_cycle",
                        lambda database, cycle, **options: published.append((database.process_plan.role.value, cycle, options)))
    context = _context("PORTAL_PUBLISH")
    row = _receipt_row(context)
    context = replace(context, database=_Database([row]))
    environment = RunnerEnvironment(None)
    environment.handoff.put(context.plan.cohort_id, NOW, "the-cycle")
    assert runners.portal_publish_runner(environment)(context).state == "COMPLETE"
    role, cycle, options = published[0]
    assert (role, cycle) == ("PUBLICATION", "the-cycle")
    assert options["receipt"].receipt_id == row["receipt_id"] and options["receipt"].universe_sha256 == "b" * 64
    assert options["receipt"].provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert options["session_close"] is False  # 15:00 UTC is mid-session
    assert environment.handoff.peek(context.plan.cohort_id, NOW) is None


def test_a_failed_publication_keeps_the_cycle_for_the_retry(monkeypatch):
    def broken(database, cycle, **options):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(runners, "publish_realtime_ticker_cycle", broken)
    context = _context("PORTAL_PUBLISH")
    context = replace(context, database=_Database([_receipt_row(context)]))
    environment = RunnerEnvironment(None)
    environment.handoff.put(context.plan.cohort_id, NOW, "the-cycle")
    with pytest.raises(RuntimeError):
        runners.portal_publish_runner(environment)(context)
    assert environment.handoff.peek(context.plan.cohort_id, NOW) == "the-cycle"


def test_the_final_window_of_a_session_publishes_the_kept_session_close_release(monkeypatch):
    import exchange_calendars

    close = exchange_calendars.get_calendar("XNYS").session_close(str(SESSION)).to_pydatetime()
    published = []
    monkeypatch.setattr(runners, "publish_realtime_ticker_cycle",
                        lambda database, cycle, **options: published.append(options))
    context = _context("PORTAL_PUBLISH", cutoff=close)
    row = dict(_receipt_row(context), cutoff=close)
    context = replace(context, database=_Database([row]))
    environment = RunnerEnvironment(None)
    environment.handoff.put(context.plan.cohort_id, close, "the-cycle")
    runners.portal_publish_runner(environment)(context)
    assert published[0]["session_close"] is True


def test_the_runner_factories_are_registered_for_the_six_non_options_stages_and_options_flow():
    from realtime.stage_runners_v2 import RUNNER_FACTORIES, unwired_stages

    assert set(RUNNER_FACTORIES) == {
        "REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STRATEGY_PUBLISH", "STOCK_ALERTS",
        "OPTIONS_FLOW"}
    assert unwired_stages() == frozenset(name for name in STAGES_V1 if name.startswith("OPTIONS_")) - {"OPTIONS_FLOW"}
    assert STAGES_V1["EQUITY_EVIDENCE"].capability_role == "compute"
    assert STAGES_V1["PORTAL_PUBLISH"].capability_role == "publication"
    assert isinstance(StageOutcome.complete(), StageOutcome)
