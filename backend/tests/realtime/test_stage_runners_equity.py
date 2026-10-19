"""EQUITY_EVIDENCE and PORTAL_PUBLISH runners (offline fakes plus a DB variant on the real V2 schema)."""
from __future__ import annotations

import threading
from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

import realtime.stage_runners_equity as runners
from realtime.database_access_v2 import WriteFenceRejected
from realtime.equity_bar_drain import EquityBarDrainResult, lease_owner_for
from realtime.orchestration_repository import CycleReceiptSpec
from realtime.preflight import ProviderTier
from realtime.runner_environment import CycleHandoff, RunnerEnvironment
from realtime.screening_projection import RealtimeScreeningInput
from realtime.stage_graph import STAGES_V1, STAGES_V2
from realtime.stage_orchestrator import StageCancelled, StageContext, StageOutcome
from realtime.ticker_repository import CycleRehydrationMismatch

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
SESSION = date(2026, 10, 12)


def _inputs(cutoff=NOW):
    return RealtimeScreeningInput(
        cohort_id=uuid4(), provider_tier=ProviderTier.MASSIVE_DEVELOPER, source_manifest_sha256="a" * 64,
        source_generation=2, session=SESSION, cutoff=cutoff, members=(), bars=(), actions=(), coverage=())


def _context(stage, *, cancelled=None, database=None, cutoff=NOW):
    plan = SimpleNamespace(cohort_id=uuid4(), session_date=SESSION)
    window = SimpleNamespace(window_id=uuid4(), cutoff=cutoff, deadline=cutoff + timedelta(minutes=16, seconds=30))
    catalog = STAGES_V2 if stage in STAGES_V2 and stage not in STAGES_V1 else STAGES_V1
    return StageContext(plan, window, catalog[stage], uuid4(), 1, NOW, database, cancelled, fenced=True)


class _Cursor:
    def __init__(self, rows):
        self.rows, self.statements = rows, []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        return self.rows.pop(0) if self.rows else ()


class _Database:
    """Answers the stage's read-only queries; records every transaction request."""

    def __init__(self, rows):
        self.cursor, self.calls = _Cursor(rows), []

    def transaction(self, role="app", **options):
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


def test_v2_materialize_runner_emits_input_ready_without_loading_or_projecting(monkeypatch):
    monkeypatch.setattr(runners, "drain_equity_bar_jobs", lambda *args, **kwargs: EquityBarDrainResult())
    monkeypatch.setattr(runners, "finalize_five_minute_inputs",
                        lambda *args, **kwargs: ("COMPLETE", ()))
    monkeypatch.setattr(runners, "reference_generation", lambda database, cutoff: 3)
    monkeypatch.setattr(
        runners,
        "input_ready_identity",
        lambda database, **scope: ("b" * 64, "c" * 64),
    )
    monkeypatch.setattr(
        runners,
        "compute_realtime_ticker_cycle",
        lambda *args, **kwargs: pytest.fail("shared materialization must not load portal history or project"),
    )
    context = _context("EQUITY_MATERIALIZE")
    outcome = runners.equity_materialize_runner(RunnerEnvironment(None))(context)
    assert outcome.state == "COMPLETE"
    assert (
        outcome.receipt.receipt_kind,
        outcome.receipt.reference_generation,
        outcome.receipt.universe_sha256,
        outcome.receipt.input_sha256,
    ) == (runners.INPUT_READY, 3, "b" * 64, "c" * 64)


def _input_identity_database(*, second_watermark=True, batch_sha="9" * 64):
    scope = (
        "1" * 64, "2" * 64, "3" * 64, "4" * 64,
        UUID(int=1), "5" * 64, "6" * 64, "7" * 64, 2,
        UUID(int=2), "8" * 64, "9" * 64, "a" * 64, "b" * 64,
        "MASSIVE_DEVELOPER", "c" * 64,
    )
    members = (
        (11, UUID(int=3), "AAPL", UUID(int=4), "d" * 64),
        (12, UUID(int=5), "MSFT", UUID(int=6), "e" * 64),
    )
    seeds = ((UUID(int=7), "f" * 64, "6" * 64, {"reference": 1}),)
    watermarks = [
        (
            11, "1m", "NATIVE_REST", NOW, "developer_rest_overlap_v1", "COMPLETE", [], UUID(int=8),
            "0" * 64, "COMPLETE", "OK", "COMPLETE", NOW, NOW, "RAW_FINALIZED_MINUTE_CLOSE_V1",
            UUID(int=9), "8" * 64,
        ),
    ]
    if second_watermark:
        watermarks.append((
            12, "1m", "NATIVE_REST", NOW, "developer_rest_overlap_v1", "NO_TRADE",
            ["NO_TRADE"], UUID(int=10), "1" * 64, "COMPLETE", "OK", "COMPLETE", NOW, NOW,
            "RAW_FINALIZED_MINUTE_CLOSE_V1", UUID(int=11), batch_sha,
        ))
        watermarks.extend((
            (
                instrument_id, "5m", "DERIVED", NOW, "XNYS_REALTIME_1M_TO_5M_V1", "COMPLETE",
                [], None, None, None, None, None, None, None, "NOT_APPLICABLE", None, None,
            )
            for instrument_id in (11, 12)
        ))
    return _Database([scope, members, seeds, tuple(watermarks)])


def _advanced_input_identity_database(
    *,
    gaps=(),
    no_trade_instrument=None,
):
    scope = (
        "1" * 64, "2" * 64, "3" * 64, "4" * 64,
        UUID(int=1), "5" * 64, "6" * 64, "7" * 64, 2,
        UUID(int=2), "8" * 64, "9" * 64, "a" * 64, "b" * 64,
        "MASSIVE_ADVANCED", "c" * 64,
    )
    members = (
        (11, UUID(int=3), "AAPL", UUID(int=4), "d" * 64),
        (12, UUID(int=5), "MSFT", UUID(int=6), "e" * 64),
    )
    seeds = ((UUID(int=7), "f" * 64, "6" * 64, {"reference": 1}),)
    sessions = (
        (
            UUID(int=20), "massive", "STOCKS", UUID(int=1),
            UUID(int=21), 4, NOW, "LIVE", UUID(int=22), NOW,
        ),
    )
    checkpoints = (
        (
            "equity-finalizer", UUID(int=22), 0, "0" * 64, 4,
            100, UUID(int=23), "1" * 64, str(UUID(int=20)), NOW,
        ),
    )
    heads = tuple(row for row in (
        (11, "1m", UUID(int=24), "2" * 64, NOW, UUID(int=22), UUID(int=20), 90, []),
        (12, "1m", UUID(int=25), "3" * 64, NOW, UUID(int=22), UUID(int=20), 91, []),
        (11, "5m", UUID(int=26), "4" * 64, NOW, None, None, None, [UUID(int=24)]),
        (12, "5m", UUID(int=27), "5" * 64, NOW, None, None, None, [UUID(int=25)]),
    ) if row[0] != no_trade_instrument)
    finalizer_progress = (
        (
            no_trade_instrument,
            NOW,
            NOW,
            "COMPLETE",
            ["FINALIZER_NO_TRADE"],
        ),
    ) if no_trade_instrument is not None else ()
    return _Database([
        scope,
        members,
        seeds,
        sessions,
        checkpoints,
        tuple(gaps),
        heads,
        finalizer_progress,
    ])


def test_input_ready_binds_full_scope_exact_members_sources_and_reference_generations():
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2StageDatabase

    cohort_id = UUID(int=12)
    first_raw = _input_identity_database()
    first = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=first_raw),
        RealtimeDatabaseRole.COMPUTE,
    )
    universe, digest = runners.input_ready_identity(
        first,
        cohort_id=cohort_id,
        cutoff=NOW,
        known_by=NOW + timedelta(minutes=1),
        reference_generation=1,
    )
    assert universe == "6" * 64 and len(digest) == 64

    changed_raw = _input_identity_database(batch_sha="7" * 64)
    changed = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=changed_raw),
        RealtimeDatabaseRole.COMPUTE,
    )
    _, changed_digest = runners.input_ready_identity(
        changed,
        cohort_id=cohort_id,
        cutoff=NOW,
        known_by=NOW + timedelta(minutes=1),
        reference_generation=1,
    )
    assert changed_digest != digest

    incomplete_raw = _input_identity_database(second_watermark=False)
    incomplete = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=incomplete_raw),
        RealtimeDatabaseRole.COMPUTE,
    )
    with pytest.raises(ValueError, match="exact manifest member"):
        runners.input_ready_identity(
            incomplete,
            cohort_id=cohort_id,
            cutoff=NOW,
            known_by=NOW + timedelta(minutes=1),
            reference_generation=1,
        )


def test_advanced_input_ready_binds_stream_checkpoint_gap_and_finalizer_heads():
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2StageDatabase

    cohort_id = UUID(int=12)
    raw = _advanced_input_identity_database()
    database = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=raw),
        RealtimeDatabaseRole.COMPUTE,
    )
    universe, digest = runners.input_ready_identity(
        database,
        cohort_id=cohort_id,
        cutoff=NOW,
        known_by=NOW + timedelta(minutes=1),
        reference_generation=1,
        advanced_input_gate_sha256="a" * 64,
    )
    assert universe == "6" * 64 and len(digest) == 64

    no_trade_raw = _advanced_input_identity_database(
        no_trade_instrument=12,
    )
    no_trade_database = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=no_trade_raw),
        RealtimeDatabaseRole.COMPUTE,
    )
    no_trade_universe, no_trade_digest = runners.input_ready_identity(
        no_trade_database,
        cohort_id=cohort_id,
        cutoff=NOW,
        known_by=NOW + timedelta(minutes=1),
        reference_generation=1,
        advanced_input_gate_sha256="a" * 64,
    )
    assert no_trade_universe == universe
    assert no_trade_digest != digest

    changed_gate_raw = _advanced_input_identity_database()
    changed_gate_database = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=changed_gate_raw),
        RealtimeDatabaseRole.COMPUTE,
    )
    _, changed_gate_digest = runners.input_ready_identity(
        changed_gate_database,
        cohort_id=cohort_id,
        cutoff=NOW,
        known_by=NOW + timedelta(minutes=1),
        reference_generation=1,
        advanced_input_gate_sha256="b" * 64,
    )
    assert changed_gate_digest != digest

    gap = (UUID(int=30), UUID(int=20), 11, "AM", NOW, None, UUID(int=22), 0, 89, "OVERLOAD", "UNRESOLVED")
    raw_gap = _advanced_input_identity_database(gaps=(gap,))
    gap_database = V2StageDatabase(
        _context("EQUITY_MATERIALIZE", database=raw_gap),
        RealtimeDatabaseRole.COMPUTE,
    )
    with pytest.raises(ValueError, match="unresolved stream gaps"):
        runners.input_ready_identity(
            gap_database,
            cohort_id=cohort_id,
            cutoff=NOW,
            known_by=NOW + timedelta(minutes=1),
            reference_generation=1,
            advanced_input_gate_sha256="a" * 64,
        )


def test_advanced_input_ready_requires_the_gate_digest():
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2StageDatabase

    database = V2StageDatabase(
        _context(
            "EQUITY_MATERIALIZE",
            database=_advanced_input_identity_database(),
        ),
        RealtimeDatabaseRole.COMPUTE,
    )

    with pytest.raises(ValueError, match="input-gate digest"):
        runners.input_ready_identity(
            database,
            cohort_id=UUID(int=12),
            cutoff=NOW,
            known_by=NOW + timedelta(minutes=1),
            reference_generation=1,
        )


def test_reused_advanced_input_ready_revalidates_the_exact_gate_digest(
    monkeypatch,
):
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.stage_database_v2 import V2StageDatabase

    cohort_id = UUID(int=12)
    known_by = NOW + timedelta(minutes=1)
    identity_database = V2StageDatabase(
        _context(
            "EQUITY_MATERIALIZE",
            database=_advanced_input_identity_database(),
        ),
        RealtimeDatabaseRole.COMPUTE,
    )
    universe, input_sha256 = runners.input_ready_identity(
        identity_database,
        cohort_id=cohort_id,
        cutoff=NOW,
        known_by=known_by,
        reference_generation=1,
        advanced_input_gate_sha256="a" * 64,
    )
    receipt = CycleReceiptSpec(
        runners.INPUT_READY,
        NOW,
        1,
        universe,
        input_sha256,
        runners.bind_advanced_input_gate_build_id(
            "advanced-equity-window-v1",
            "a" * 64,
        ),
    )
    environment = RunnerEnvironment(
        None,
        build_id="advanced-equity-window-v1",
    )

    def context(gate_digest):
        base = _context(
            "EQUITY_MATERIALIZE",
            database=_advanced_input_identity_database(),
        )
        return replace(
            base,
            plan=SimpleNamespace(
                cohort_id=cohort_id,
                session_date=SESSION,
                stage_catalog_version="stages_v3",
                gate_digest=gate_digest,
            ),
        )

    monkeypatch.setattr(
        runners,
        "advanced_input_gate_identity",
        lambda _database, own_context: own_context.plan.gate_digest,
    )
    runner = runners.reuse_advanced_input_ready_runner(
        environment,
        receipt=receipt,
        input_known_by=known_by,
        expected_gate_sha256="a" * 64,
    )

    assert runner(context("a" * 64)).receipt == receipt
    assert runner(context("b" * 64)).reason_codes == (
        runners.ADVANCED_INPUT_GATE_DIGEST_CHANGED,
    )


def test_v2_projection_runner_requires_input_ready_and_emits_portal_ready(monkeypatch):
    monkeypatch.setattr(
        runners,
        "compute_realtime_ticker_cycle",
        lambda *args, **kwargs: pytest.fail("projection must not run without INPUT_READY"),
    )
    missing = _context("EQUITY_PROJECTION", database=_Database([None]))
    assert runners.equity_projection_runner(RunnerEnvironment(None))(missing).reason_codes == (
        runners.INPUT_READY_RECEIPT_MISSING,
    )

    inputs = _inputs()
    cycle = SimpleNamespace(inputs=inputs)
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle", lambda *args, **kwargs: cycle)
    context = _context("EQUITY_PROJECTION")
    row = _receipt_row(context)
    context = replace(context, database=_Database([row]))
    environment = RunnerEnvironment(None)
    outcome = runners.equity_projection_runner(environment)(context)
    assert outcome.state == "COMPLETE"
    assert outcome.receipt.receipt_kind == runners.PORTAL_READY
    assert environment.handoff.peek(context.plan.cohort_id, NOW) is cycle


def test_a_service_environment_freezes_the_heap_once_after_the_first_successful_cycle(monkeypatch):
    calls = []
    monkeypatch.setattr(runners.gc, "freeze", lambda: calls.append("freeze"))
    monkeypatch.setattr(runners.gc, "collect", lambda *a: calls.append("collect") or 0)
    monkeypatch.setattr(runners, "compute_realtime_ticker_cycle",
                        lambda database, **kwargs: SimpleNamespace(inputs=_inputs()))
    for freeze_heap, expected in ((False, []), (True, ["collect", "freeze"])):
        calls.clear()
        environment = RunnerEnvironment(None, freeze_heap=freeze_heap)
        for _ in range(2):
            database = _Database([(3,)])
            assert runners.equity_evidence_runner(environment)(
                _context("EQUITY_EVIDENCE", database=database)).state == "COMPLETE"
        assert calls == expected and environment.heap_frozen is freeze_heap


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
            "universe_sha256": "b" * 64, "provider_tier": "MASSIVE_DEVELOPER",
            "reference_generation": 3, "input_sha256": "c" * 64,
            "deadline": context.window.deadline}


def test_publish_runner_needs_the_durable_receipt(monkeypatch):
    monkeypatch.setattr(runners, "publish_realtime_portal_cycle_atomic",
                        lambda *args, **kwargs: pytest.fail("nothing is published without a receipt"))
    monkeypatch.setattr(runners, "rehydrate_realtime_ticker_cycle",
                        lambda *args, **kwargs: pytest.fail("nothing is rebuilt without a receipt"))
    context = _context("PORTAL_PUBLISH", database=_Database([None]))
    assert runners.portal_publish_runner(RunnerEnvironment(None))(context).reason_codes == (
        runners.EQUITY_CYCLE_RECEIPT_MISSING,)


def test_publish_runner_rehydrates_a_missing_handoff_from_the_receipt_and_publishes_it(monkeypatch):
    published, rebuilt = [], []

    def rehydrate(database, **options):
        rebuilt.append((database.process_plan.role.value, options))
        return "the-rebuilt-cycle"

    monkeypatch.setattr(runners, "rehydrate_realtime_ticker_cycle", rehydrate)
    monkeypatch.setattr(
        runners,
        "publish_realtime_portal_cycle_atomic",
        lambda database, cycle, dashboard, **options:
        published.append((cycle, dashboard, options)),
    )
    context = _context("PORTAL_PUBLISH")
    row = _receipt_row(context)
    context = replace(context, database=_Database([row]))
    environment = RunnerEnvironment(None)
    assert runners.portal_publish_runner(environment)(context).state == "COMPLETE"
    # Read-only rebuild under the publication role, for exactly the window's scope, proven against the receipt.
    assert rebuilt == [("PUBLICATION", dict(
        input_sha256="c" * 64, universe_sha256="b" * 64, cohort_id=context.plan.cohort_id, session=SESSION,
        cutoff=NOW, known_by=context.window.deadline, source_generation=3,
        allow_late_identity_continuity=False))]
    assert rebuilt[0][1]["known_by"] != NOW
    assert published[0][0] == "the-rebuilt-cycle"
    assert published[0][1] is None
    assert published[0][2]["receipt"].receipt_id == row["receipt_id"]
    assert len(environment.handoff) == 0


def test_a_rehydrated_cycle_that_differs_from_the_receipt_is_never_published(monkeypatch):
    def rehydrate(database, **options):
        raise CycleRehydrationMismatch("the rebuilt cycle is not the one the EQUITY_CYCLE receipt names")

    monkeypatch.setattr(runners, "rehydrate_realtime_ticker_cycle", rehydrate)
    monkeypatch.setattr(runners, "publish_realtime_portal_cycle_atomic",
                        lambda *args, **kwargs: pytest.fail("a cycle that is not the receipt's must not be published"))
    context = _context("PORTAL_PUBLISH")
    context = replace(context, database=_Database([_receipt_row(context)]))
    outcome = runners.portal_publish_runner(RunnerEnvironment(None))(context)
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.CYCLE_REHYDRATION_MISMATCH,)


def test_a_stage_cancelled_during_rehydration_publishes_nothing(monkeypatch):
    cancelled = threading.Event()

    def rehydrate(database, **options):
        cancelled.set()
        return "the-rebuilt-cycle"

    monkeypatch.setattr(runners, "rehydrate_realtime_ticker_cycle", rehydrate)
    monkeypatch.setattr(runners, "publish_realtime_portal_cycle_atomic",
                        lambda *args, **kwargs: pytest.fail("a cancelled stage must not publish"))
    context = _context("PORTAL_PUBLISH", cancelled=cancelled)
    context = replace(context, database=_Database([_receipt_row(context)]))
    outcome = runners.portal_publish_runner(RunnerEnvironment(None))(context)
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def test_the_handed_off_cycle_is_used_without_any_rebuild(monkeypatch):
    monkeypatch.setattr(runners, "rehydrate_realtime_ticker_cycle",
                        lambda *args, **kwargs: pytest.fail("the hand-off is present: no cold projection"))
    monkeypatch.setattr(
        runners,
        "publish_realtime_portal_cycle_atomic",
        lambda *args, **kwargs: None,
    )
    context = _context("PORTAL_PUBLISH")
    context = replace(context, database=_Database([_receipt_row(context)]))
    environment = RunnerEnvironment(None)
    environment.handoff.put(context.plan.cohort_id, NOW, "the-cycle")
    assert runners.portal_publish_runner(environment)(context).state == "COMPLETE"


def test_v2_portal_ready_proves_projection_but_publishes_into_the_input_ready_release(monkeypatch):
    published = []
    dashboards = []
    monkeypatch.setattr(
        runners,
        "publish_realtime_portal_cycle_atomic",
        lambda database, cycle, dashboard, **options:
        published.append((dashboard, options["receipt"])),
    )
    monkeypatch.setattr(
        runners,
        "compute_realtime_dashboard",
        lambda database, **options: dashboards.append(options) or "dashboard",
    )
    context = _context("PORTAL_PUBLISH")
    context = replace(
        context,
        plan=SimpleNamespace(
            cohort_id=context.plan.cohort_id,
            session_date=SESSION,
            stage_catalog_version="stages_v2",
        ),
    )
    portal_row = _receipt_row(context)
    input_row = dict(_receipt_row(context), receipt_id=uuid4())
    context = replace(context, database=_Database([portal_row, input_row]))
    environment = RunnerEnvironment(None)
    environment.handoff.put(context.plan.cohort_id, NOW, "the-cycle")

    assert runners.portal_publish_runner(environment)(context).state == "COMPLETE"
    assert published[0][0] == "dashboard"
    assert published[0][1].receipt_id == input_row["receipt_id"]
    assert dashboards[0]["known_by"] == context.window.deadline
    assert dashboards[0]["allow_late_identity_continuity"] is False


def test_publication_recovery_reads_both_receipts_from_the_explicit_source_window(monkeypatch):
    source_window_id = uuid4()
    rebuilt, dashboards = [], []
    context = _context("PORTAL_PUBLISH")
    context = replace(
        context,
        plan=SimpleNamespace(
            cohort_id=context.plan.cohort_id,
            session_date=SESSION,
            stage_catalog_version="stages_v2",
        ),
    )
    portal_row = _receipt_row(context)
    input_row = dict(_receipt_row(context), receipt_id=uuid4())
    database = _Database([portal_row, input_row])
    context = replace(context, database=database)
    environment = RunnerEnvironment(
        None,
        allow_late_identity_continuity=True,
    )
    monkeypatch.setattr(
        runners,
        "rehydrate_realtime_ticker_cycle",
        lambda database, **options: rebuilt.append(options) or "cycle",
    )
    monkeypatch.setattr(
        runners,
        "compute_realtime_dashboard",
        lambda *args, **kwargs: dashboards.append(kwargs) or "dashboard",
    )
    monkeypatch.setattr(
        runners,
        "publish_realtime_portal_cycle_atomic",
        lambda *args, **kwargs: None,
    )

    outcome = runners.portal_publish_runner(
        environment,
        receipt_window_id=source_window_id,
    )(context)

    assert outcome.state == "COMPLETE"
    assert [call[1][0] for call in database.cursor.statements[:2]] == [
        str(source_window_id),
        str(source_window_id),
    ]
    assert rebuilt[0]["known_by"] == context.window.deadline
    assert rebuilt[0]["allow_late_identity_continuity"] is True
    assert dashboards[0]["allow_late_identity_continuity"] is True


def test_atomic_portal_publish_never_advances_pointer_after_dashboard_failure(
    monkeypatch,
):
    context = _context("PORTAL_PUBLISH", database=_Database([]))
    publication = runners.V2StageDatabase(
        context,
        runners.RealtimeDatabaseRole.PUBLICATION,
    )
    monkeypatch.setattr(
        runners,
        "persist_realtime_ticker_cycle_cursor",
        lambda cursor, cycle, **options: (("ticker",), ()),
    )

    def fail_dashboard(cursor, dashboard, **options):
        raise RuntimeError("dashboard persistence failed")

    monkeypatch.setattr(
        runners,
        "persist_realtime_dashboard_cursor",
        fail_dashboard,
    )
    publish = MagicMock()
    monkeypatch.setattr(runners, "publish_portal_release_cursor", publish)

    with pytest.raises(RuntimeError, match="dashboard persistence failed"):
        runners.publish_realtime_portal_cycle_atomic(
            publication,
            SimpleNamespace(summary="summary"),
            "dashboard",
            receipt=runners.PortalReleaseReceipt(
                uuid4(),
                context.plan.cohort_id,
                ProviderTier.MASSIVE_DEVELOPER,
                NOW,
                "b" * 64,
            ),
            session_close=False,
        )

    publish.assert_not_called()


def test_rehydration_accepts_only_the_publication_role_and_a_cycle_whose_receipt_and_evidence_are_durable(monkeypatch):
    import realtime.ticker_repository as repository
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.lineage_head import inputs_sha256, reference_set
    from realtime.stage_database_v2 import V2StageDatabase

    inputs = _inputs()
    projection = SimpleNamespace(snapshots=("snapshot",), screening_generation=None)
    seen = {}

    def project(database, *, state, loader, scope):
        seen.update(state=state, loader=loader, scope=scope)
        return inputs, projection, "summary", "pattern"

    monkeypatch.setattr(repository, "_project_ticker_cycle", project)
    groups, verified = [], []
    monkeypatch.setattr(repository, "_materialization_write_groups", lambda projection, inputs: iter(groups))
    monkeypatch.setattr(repository, "verify_realtime_equity_materializations_persisted_cursor",
                        lambda cursor, writes: verified.append(writes))
    context = _context("PORTAL_PUBLISH", database=_Database([]))
    publication = V2StageDatabase(context, RealtimeDatabaseRole.PUBLICATION)
    names = dict(input_sha256=inputs_sha256(inputs), universe_sha256=reference_set(inputs.members)["sha256"])
    scope = dict(cohort_id=inputs.cohort_id, session=SESSION, cutoff=NOW, known_by=NOW, source_generation=2)
    groups.extend((("workspace-writes",), ("pattern-writes",)))
    cycle = repository.rehydrate_realtime_ticker_cycle(publication, **names, **scope)
    assert (cycle.inputs, cycle.summary, cycle.pattern_publication) == (inputs, "summary", "pattern")
    # Stateless: the long-lived loader and projection state are never touched, and the scope is forwarded as is.
    assert seen == dict(state=None, loader=None, scope=scope)
    # Every evidence group of the rebuilt cycle was checked against the durable rows, read-only.
    assert verified == [("workspace-writes",), ("pattern-writes",)]
    with pytest.raises(repository.CycleRehydrationMismatch):
        repository.rehydrate_realtime_ticker_cycle(publication, **dict(names, input_sha256="f" * 64), **scope)
    with pytest.raises(repository.CycleRehydrationMismatch):
        repository.rehydrate_realtime_ticker_cycle(publication, **dict(names, universe_sha256="f" * 64), **scope)
    compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
    with pytest.raises(ValueError, match="PUBLICATION"):
        repository.rehydrate_realtime_ticker_cycle(compute, **names, **scope)

    def missing_evidence(cursor, writes):
        raise ValueError("realtime equity evidence is not persisted as the materializations require")

    monkeypatch.setattr(repository, "verify_realtime_equity_materializations_persisted_cursor", missing_evidence)
    with pytest.raises(repository.CycleRehydrationMismatch, match="not persisted"):
        repository.rehydrate_realtime_ticker_cycle(publication, **names, **scope)


def test_publish_runner_publishes_the_handed_off_cycle_for_its_receipt_and_consumes_it_on_success(monkeypatch):
    published = []
    monkeypatch.setattr(
        runners,
        "publish_realtime_portal_cycle_atomic",
        lambda database, cycle, dashboard, **options:
        published.append((
            database.process_plan.role.value,
            cycle,
            dashboard,
            options,
        )),
    )
    context = _context("PORTAL_PUBLISH")
    row = _receipt_row(context)
    context = replace(context, database=_Database([row]))
    environment = RunnerEnvironment(None)
    environment.handoff.put(context.plan.cohort_id, NOW, "the-cycle")
    assert runners.portal_publish_runner(environment)(context).state == "COMPLETE"
    role, cycle, dashboard, options = published[0]
    assert (role, cycle) == ("PUBLICATION", "the-cycle")
    assert dashboard is None
    assert options["receipt"].receipt_id == row["receipt_id"] and options["receipt"].universe_sha256 == "b" * 64
    assert options["receipt"].provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert options["session_close"] is False  # 15:00 UTC is mid-session
    assert environment.handoff.peek(context.plan.cohort_id, NOW) is None


def test_a_failed_publication_keeps_the_cycle_for_the_retry(monkeypatch):
    def broken(database, cycle, dashboard, **options):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(runners, "publish_realtime_portal_cycle_atomic", broken)
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
    monkeypatch.setattr(
        runners,
        "publish_realtime_portal_cycle_atomic",
        lambda database, cycle, dashboard, **options:
        published.append(options),
    )
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
        "OPTIONS_ACQUIRE", "OPTIONS_ADMISSION", "OPTIONS_FLOW",
        "OPTIONS_INPUT_GATE", "OPTIONS_CYCLE", "OPTIONS_PROJECTION",
        "OPTIONS_PUBLISH",
        "EQUITY_INPUT_GATE", "EQUITY_MATERIALIZE", "EQUITY_PROJECTION"}
    assert unwired_stages() == frozenset()
    assert unwired_stages(frozenset(
        name for name in STAGES_V2 if not name.startswith("OPTIONS_")
    )) == frozenset()
    assert STAGES_V1["EQUITY_EVIDENCE"].lane == STAGES_V1["PORTAL_PUBLISH"].lane == "data"
    assert RUNNER_FACTORIES["EQUITY_MATERIALIZE"] is runners.equity_materialize_runner
    assert RUNNER_FACTORIES["EQUITY_PROJECTION"] is runners.equity_projection_runner
    assert STAGES_V2["STOCK_ALERTS"].lane == "alert"
    assert STAGES_V2["EQUITY_PROJECTION"].lane == STAGES_V2["PORTAL_PUBLISH"].lane == "portal"
    assert isinstance(StageOutcome.complete(), StageOutcome)
