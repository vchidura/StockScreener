"""REFERENCE and EQUITY_INGEST runners (offline fakes; the DB variant is test_stage_runners_bind_v2_db).

No provider is reachable here: the sources are fakes, and a stage whose source is not configured must
be blocked without touching the database or any source."""
from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

import realtime.stage_runners_ingest as runners
from realtime.acquisition_handoff import DeveloperAcquisitionFactKind, DeveloperEquityBarAcquisitionFact
from realtime.acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
)
from realtime.acquisition_producer import (
    MAX_DEVELOPER_CANARY_REQUESTS,
    MAX_DEVELOPER_CANARY_RESPONSE_BYTES,
    CompletedDeveloperSourceBatch,
    DeveloperSourceResult,
)
from realtime.contracts import UniverseManifest
from realtime.database_access import RealtimeDatabaseRole
from realtime.database_access_v2 import WriteFenceRejected
from realtime.reference_seed import (
    ReferenceKind,
    ReferenceSeedMember,
    SeedMemberStatus,
    SeedStatus,
    build_reference_seed_manifest,
)
from realtime.reference_service import ProviderEntitlementUnavailable, ReferenceServiceResult
from realtime.runner_environment import RunnerEnvironment
from realtime.stage_database_v2 import StageProcessPlan
from realtime.stage_orchestrator import StageCancelled
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_equity_bar_adapter import _developer_revision
from tests.realtime.test_stage_runners_equity import NOW, SESSION, _context, _Database

SMALL_UNIVERSE = UniverseManifest("fixture-v1", ("AAPL", "MSFT"), ())
ONE_TICKER_UNIVERSE = UniverseManifest("fixture-reference-v1", ("SPY",), ())


def reference_members(universe=ONE_TICKER_UNIVERSE, *, unavailable_rate=False, created_at=NOW):
    """The members a one-ticker, ETF universe needs, READY unless `unavailable_rate`."""
    def member(kind, key, status=SeedMemberStatus.READY):
        reasons = () if status is SeedMemberStatus.READY else (
            "VERIFIED_ETF" if status is SeedMemberStatus.NOT_APPLICABLE else "SOURCE_UNAVAILABLE",)
        return ReferenceSeedMember(kind, key, f"fixture:{kind.value}:{key}", "a" * 64, status, created_at,
                                   created_at + timedelta(seconds=1), reasons)

    rows = []
    for ticker in universe.equity_tickers:
        rows.extend((member(ReferenceKind.SECURITY, ticker), member(ReferenceKind.CORPORATE_ACTION_COVERAGE, ticker),
                     member(ReferenceKind.EARNINGS_COVERAGE, ticker, SeedMemberStatus.NOT_APPLICABLE)))
    rows.append(member(ReferenceKind.FOMC_COVERAGE, "GLOBAL"))
    rows.append(member(ReferenceKind.RATE_CURVE, "USD",
                       SeedMemberStatus.UNAVAILABLE if unavailable_rate else SeedMemberStatus.READY))
    return tuple(rows)


def reference_manifest(request, *, unavailable_rate=False):
    return build_reference_seed_manifest(
        request.universe, session_date=request.session_date,
        members=reference_members(request.universe, unavailable_rate=unavailable_rate, created_at=request.created_at),
        source_generations=(("federal_reserve", "e" * 64), ("finnhub", "b" * 64), ("massive", "c" * 64),
                            ("treasury", "d" * 64)),
        created_at=request.created_at, effective_at=request.effective_at, valid_until=request.valid_until)


class FakeReferenceService:
    """Stands in for ReferenceSeedService: records the request, never touches a provider."""

    def __init__(self, *, unavailable_rate=False, error=None, on_run=None):
        self.requests, self.unavailable_rate, self.error, self.on_run = [], unavailable_rate, error, on_run

    def run_once(self, request):
        self.requests.append(request)
        if self.on_run is not None:
            self.on_run()
        if self.error is not None:
            raise self.error
        return ReferenceServiceResult(reference_manifest(request, unavailable_rate=self.unavailable_rate), 0, 0, ())


class FailingService:
    def run_once(self, request):
        pytest.fail("a provider was called although the stage must have been blocked")


class FakeEquitySource:
    """An equity source with a canned result: records each request, never touches a provider."""

    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(self, build, on_collect=None):
        self.build, self.requests, self.on_collect = build, [], on_collect

    def collect(self, request):
        self.requests.append(request)
        if self.on_collect is not None:
            self.on_collect()
        return self.build(request)


def equity_result(request, *, security_ids=None, instrument_ids=None, incomplete=(), omit=(), extra=(),
                  provider_requests=None, response_bytes=None, observed_after=timedelta(seconds=5),
                  bars=(("1m", 1),)):
    """One batch and one provider request per ticker; `bars` lists (interval, count) per ticker, consecutive
    bars of each interval ending at the request time."""
    batches, ledger = [], []
    end = request.requested_at.replace(second=0, microsecond=0)
    for index, ticker in enumerate((*request.universe.equity_tickers, *extra), start=1):
        if ticker in omit:
            continue
        segment, request_id = uuid4(), uuid4()
        facts = []
        for interval, count in bars:
            width = timedelta(minutes=int(interval[:-1]))
            for position in range(count):
                bar_end = end - width * (count - 1 - position)
                revision = replace(
                    _developer_revision(), bar_revision_id=uuid4(), ticker=ticker, interval=interval,
                    ingestion_segment_id=segment, security_id=(security_ids or {}).get(ticker, uuid4()),
                    session_date=bar_end.date(), bar_start=bar_end - width, bar_end=bar_end,
                    system_observed_at=end + observed_after, payload_sha256=uuid4().hex + uuid4().hex)
                facts.append(DeveloperEquityBarAcquisitionFact((instrument_ids or {}).get(ticker, index), revision))
        batches.append(CompletedDeveloperSourceBatch(
            DeveloperAcquisitionFactKind.EQUITY_BAR, segment, end, end + observed_after, tuple(facts),
            request_id=request_id))
        complete = ticker not in incomplete
        ledger.append(RecordedProviderRequest(
            ProviderRequestReservation(request_id, "developer", ProviderRequestLane.EQUITY_HISTORY, "aggs_minute",
                                       uuid4().hex + uuid4().hex, end, cohort_id=request.cohort_id,
                                       window_id=request.window_id, stage_run_id=request.stage_run_id),
            ProviderRequestCompletion(
                ProviderRequestState.COMPLETE, started_at=end, completed_at=end + observed_after,
                outcome=ProviderRequestOutcome.OK, http_status=200, response_bytes=1_000, row_count=1,
                pages_fetched=1, requested_from=end - timedelta(days=1), requested_to=end,
                price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
                completeness=RequestCompleteness.COMPLETE if complete else RequestCompleteness.INCOMPLETE,
                failure_code=None if complete else "PAGINATION_NOT_EXHAUSTED")))
    return DeveloperSourceResult(
        "massive_developer", tuple(batches),
        len(batches) if provider_requests is None else provider_requests,
        1_000 * len(batches) if response_bytes is None else response_bytes, tuple(ledger))


class _Ingress(RealtimeProcessDatabase):
    """An ingress-role process database over a recording cursor (no connection)."""

    def __init__(self, role=RealtimeDatabaseRole.INGRESS):
        self._process_plan = StageProcessPlan(role)
        self.events = []

    @contextmanager
    def transaction(self, *, read_only=None, dict_cursor=False, isolation_level=None):
        self.events.append("transaction")
        yield SimpleNamespace()


def _request(universe=SMALL_UNIVERSE):
    return runners.EquityAcquisitionRequest(uuid4(), universe, NOW)


def _hooks(database):
    events = database.events
    return dict(
        repository=lambda cursor, batch: events.append(("batch", batch)),
        request_repository=lambda cursor, reservation, completion: events.append(("ledger", reservation.request_id)),
        watermark_repository=lambda cursor, watermark: events.append(("watermark", watermark)),
    )


def _environment(**fields):
    return RunnerEnvironment(None, **fields)


# --- REFERENCE ---------------------------------------------------------------------------------------------------

def test_reference_blocks_without_a_configured_service_and_touches_nothing():
    database = _Database([])
    context = _context("REFERENCE", database=database)
    outcome = runners.reference_runner(_environment(universe=ONE_TICKER_UNIVERSE))(context)
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.REFERENCE_SOURCE_NOT_CONFIGURED,)
    assert database.calls == []


def test_reference_blocks_without_a_universe_before_any_provider_call():
    database = _Database([])
    context = _context("REFERENCE", database=database)
    outcome = runners.reference_runner(_environment(reference_service=FailingService()))(context)
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.UNIVERSE_NOT_CONFIGURED,)
    assert database.calls == []


def test_a_ready_seed_is_persisted_under_the_references_role_with_the_next_ready_count(monkeypatch):
    persisted = []
    monkeypatch.setattr(runners, "persist_reference_result_cursor",
                        lambda cursor, result, **options: persisted.append((result, options)))
    service = FakeReferenceService()
    environment = _environment(reference_service=service, universe=ONE_TICKER_UNIVERSE,
                               reference_scope="EQUITY_VALIDATION_REFERENCES")
    database = _Database([None, (4,)])  # the seed is new; four READY seeds exist
    context = _context("REFERENCE", database=database)
    outcome = runners.reference_runner(environment)(context)
    request, = service.requests
    assert (request.universe, request.session_date, request.created_at) == (ONE_TICKER_UNIVERSE, SESSION, NOW)
    assert (request.effective_at, request.valid_until) == (NOW + timedelta(minutes=1), NOW + timedelta(hours=6))
    (result, options), = persisted
    assert options == dict(generation=5, scope="EQUITY_VALIDATION_REFERENCES")
    assert result.manifest.status is SeedStatus.READY
    assert outcome.state == "COMPLETE" and outcome.reason_codes == () and outcome.evidence_ref == str(
        result.manifest.seed_id)
    assert [role for role, _ in database.calls] == ["references"]
    assert "rt.seed_manifests" in database.cursor.statements[0][0]
    assert "status = 'READY'" in database.cursor.statements[1][0]


def test_an_unavailable_seed_is_persisted_and_reported_blocked_with_the_manifest_reasons(monkeypatch):
    persisted = []
    monkeypatch.setattr(runners, "persist_reference_result_cursor",
                        lambda cursor, result, **options: persisted.append(result))
    environment = _environment(reference_service=FakeReferenceService(unavailable_rate=True),
                               universe=ONE_TICKER_UNIVERSE)
    outcome = runners.reference_runner(environment)(_context("REFERENCE", database=_Database([None, (0,)])))
    assert persisted and persisted[0].manifest.status is SeedStatus.UNAVAILABLE
    assert outcome.state == "BLOCKED_POLICY"
    assert outcome.reason_codes == ("UNAVAILABLE_RATE_CURVE:USD",) == persisted[0].manifest.reason_codes
    assert outcome.evidence_ref == str(persisted[0].manifest.seed_id)


def test_an_already_persisted_seed_is_not_written_twice(monkeypatch):
    monkeypatch.setattr(runners, "persist_reference_result_cursor", lambda *a, **k: pytest.fail("idempotent"))
    environment = _environment(reference_service=FakeReferenceService(), universe=ONE_TICKER_UNIVERSE)
    outcome = runners.reference_runner(environment)(_context("REFERENCE", database=_Database([(1,)])))
    assert outcome.state == "COMPLETE"


def test_an_entitlement_failure_is_a_policy_block_and_persists_nothing():
    database = _Database([])
    environment = _environment(reference_service=FakeReferenceService(error=ProviderEntitlementUnavailable("x")),
                               universe=ONE_TICKER_UNIVERSE)
    outcome = runners.reference_runner(environment)(_context("REFERENCE", database=database))
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.PROVIDER_ENTITLEMENT_UNAVAILABLE,)
    assert database.calls == []


def test_reference_polls_cancellation_before_the_provider_call_and_before_persisting():
    cancelled = threading.Event()
    cancelled.set()
    service = FakeReferenceService()
    database = _Database([])
    outcome = runners.reference_runner(_environment(reference_service=service, universe=ONE_TICKER_UNIVERSE))(
        _context("REFERENCE", database=database, cancelled=cancelled))
    assert outcome.reason_codes == (runners.STAGE_CANCELLED,) and service.requests == [] and database.calls == []

    cancelled = threading.Event()
    service = FakeReferenceService(on_run=cancelled.set)  # abandoned while the provider call is running
    outcome = runners.reference_runner(_environment(reference_service=service, universe=ONE_TICKER_UNIVERSE))(
        _context("REFERENCE", database=database, cancelled=cancelled))
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)
    assert len(service.requests) == 1 and database.calls == []


@pytest.mark.parametrize("error", [StageCancelled("abandoned"), WriteFenceRejected("fenced")])
def test_a_rejected_reference_write_is_a_cancelled_failure(monkeypatch, error):
    def persist(*args, **kwargs):
        raise error

    monkeypatch.setattr(runners, "persist_reference_result_cursor", persist)
    environment = _environment(reference_service=FakeReferenceService(), universe=ONE_TICKER_UNIVERSE)
    outcome = runners.reference_runner(environment)(_context("REFERENCE", database=_Database([None, (0,)])))
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


# --- EQUITY_INGEST: the equity-only acquisition function ---------------------------------------------------------

def test_equity_acquisition_records_the_ledger_first_then_batches_and_only_complete_requests_advance_watermarks():
    database, request = _Ingress(), _request()
    source = FakeEquitySource(lambda req: equity_result(req, incomplete=("MSFT",)))
    result = runners.acquire_equity_bars(database, source, request, **_hooks(database))
    assert source.requests == [request] and request.requested_at == NOW
    kinds = [event[0] if isinstance(event, tuple) else event for event in database.events]
    assert kinds == ["transaction", "ledger", "ledger", "transaction", "batch", "watermark", "batch"]
    watermark = next(event[1] for event in database.events if event[0] == "watermark")
    assert (watermark.last_complete_bar_end, watermark.cohort_id) == (NOW, request.cohort_id)
    assert (result.fact_count, result.watermarks_advanced, result.unadvanced_batches) == (2, 1, 1)
    assert len(result.acquisition_batch_ids) == 2 and result.provider_requests == 2


@pytest.mark.parametrize("build, code", [
    (lambda req: equity_result(req, omit=("MSFT",)), runners.EQUITY_COVERAGE_MISMATCH),
    (lambda req: equity_result(req, extra=("NVDA",)), runners.EQUITY_COVERAGE_MISMATCH),
    (lambda req: equity_result(req, provider_requests=MAX_DEVELOPER_CANARY_REQUESTS + 1),
     runners.EQUITY_REQUEST_BOUND_EXCEEDED),
    (lambda req: equity_result(req, response_bytes=MAX_DEVELOPER_CANARY_RESPONSE_BYTES + 1),
     runners.EQUITY_RESPONSE_BOUND_EXCEEDED),
    (lambda req: "not a source result", runners.EQUITY_SOURCE_RESULT_INVALID),
])
def test_a_contract_breaking_source_result_writes_nothing(build, code):
    database = _Ingress()
    with pytest.raises(runners.EquityAcquisitionRejected) as rejected:
        runners.acquire_equity_bars(database, FakeEquitySource(build), _request(), **_hooks(database))
    assert rejected.value.code == code and database.events == []


def test_the_acquisition_function_requires_the_ingress_role_and_an_equity_source():
    source = FakeEquitySource(lambda req: pytest.fail("must not collect"))
    with pytest.raises(ValueError, match="ingress"):
        runners.acquire_equity_bars(_Ingress(RealtimeDatabaseRole.COMPUTE), source, _request())
    with pytest.raises(TypeError):
        runners.acquire_equity_bars(object(), source, _request())
    option_source = FakeEquitySource(lambda req: pytest.fail("must not collect"))
    option_source.fact_kind = DeveloperAcquisitionFactKind.OPTION_SNAPSHOT
    with pytest.raises(ValueError, match="fact kind"):
        runners.acquire_equity_bars(_Ingress(), option_source, _request())
    assert source.requests == option_source.requests == []


def test_equity_acquisition_polls_cancellation_between_every_phase():
    def run(stop_after):
        """Cancel once the n-th checkpoint is reached; returns (source calls, database events)."""
        database, polls = _Ingress(), []
        source = FakeEquitySource(lambda req: equity_result(req))

        def should_stop():
            polls.append(True)
            return len(polls) > stop_after

        with pytest.raises(StageCancelled):
            runners.acquire_equity_bars(database, source, _request(), should_stop=should_stop, **_hooks(database))
        return len(source.requests), database.events

    assert run(0) == (0, [])  # before the provider call
    calls, events = run(1)  # after the provider call, before any write
    assert calls == 1 and events == []
    calls, events = run(2)  # after the ledger transaction, before the batches
    assert calls == 1 and [e if isinstance(e, str) else e[0] for e in events] == ["transaction", "ledger", "ledger"]


# --- EQUITY_INGEST: the runner -----------------------------------------------------------------------------------

def test_equity_ingest_blocks_without_a_source_or_with_a_non_equity_source_and_touches_nothing():
    database = _Database([])
    context = _context("EQUITY_INGEST", database=database)
    outcome = runners.equity_ingest_runner(_environment(universe=SMALL_UNIVERSE))(context)
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.EQUITY_SOURCE_NOT_CONFIGURED,)
    option_source = FakeEquitySource(lambda req: pytest.fail("must not collect"))
    option_source.fact_kind = DeveloperAcquisitionFactKind.OPTION_SNAPSHOT
    outcome = runners.equity_ingest_runner(_environment(equity_source=option_source, universe=SMALL_UNIVERSE))(context)
    assert outcome.reason_codes == (runners.EQUITY_SOURCE_NOT_CONFIGURED,)
    assert database.calls == [] and option_source.requests == []


def test_equity_ingest_blocks_without_a_universe_before_any_provider_call():
    source = FakeEquitySource(lambda req: pytest.fail("must not collect"))
    outcome = runners.equity_ingest_runner(_environment(equity_source=source))(
        _context("EQUITY_INGEST", database=_Database([])))
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.UNIVERSE_NOT_CONFIGURED,)
    assert source.requests == []


def test_equity_ingest_asks_for_the_window_cutoff_under_the_ingress_role(monkeypatch):
    seen = {}

    def acquire(database, source, request, *, should_stop):
        seen.update(role=database.process_plan.role.value, source=source, request=request, stop=should_stop)
        return runners.EquityAcquisitionResult((uuid4(),), 2, 2, 2_000, 2, 0)

    monkeypatch.setattr(runners, "acquire_equity_bars", acquire)
    source = FakeEquitySource(lambda req: pytest.fail("the fake acquire never collects"))
    context = _context("EQUITY_INGEST", database=_Database([]))
    outcome = runners.equity_ingest_runner(_environment(equity_source=source, universe=SMALL_UNIVERSE))(context)
    assert outcome.state == "COMPLETE" and outcome.reason_codes == () and outcome.receipt is None
    request = seen["request"]
    assert seen["role"] == "INGRESS" and seen["source"] is source and seen["stop"] == context.is_cancelled
    assert (request.cohort_id, request.universe, request.requested_at) == (context.plan.cohort_id, SMALL_UNIVERSE, NOW)
    assert (request.window_id, request.stage_run_id) == (context.window.window_id, context.stage_run_id)


@pytest.mark.parametrize("result, state, codes", [
    (runners.EquityAcquisitionResult((uuid4(),), 1, 1, 1, 0, 1), "COMPLETE", (runners.EQUITY_WATERMARK_NOT_ADVANCED,)),
    (runners.EquityAcquisitionRejected(runners.EQUITY_COVERAGE_MISMATCH), "FAILED", (runners.EQUITY_COVERAGE_MISMATCH,)),
    (StageCancelled("abandoned"), "FAILED", (runners.STAGE_CANCELLED,)),
    (WriteFenceRejected("fenced"), "FAILED", (runners.STAGE_CANCELLED,)),
])
def test_equity_ingest_maps_acquisition_outcomes_to_stage_outcomes(monkeypatch, result, state, codes):
    def acquire(*args, **kwargs):
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(runners, "acquire_equity_bars", acquire)
    source = FakeEquitySource(lambda req: pytest.fail("the fake acquire never collects"))
    outcome = runners.equity_ingest_runner(_environment(equity_source=source, universe=SMALL_UNIVERSE))(
        _context("EQUITY_INGEST", database=_Database([])))
    assert (outcome.state, outcome.reason_codes) == (state, codes)


def test_the_ingest_runners_construct_no_provider_client_and_import_no_provider_sdk():
    import inspect

    source = inspect.getsource(runners)
    for forbidden in ("import requests", "httpx", "polygon", "finnhub", "urllib", "PolygonHttpTransport",
                      "build_live_reference_service", "datetime.now", "utc_now"):
        assert forbidden not in source, forbidden


def test_the_ingest_stages_are_registered_with_their_capability_roles():
    from realtime.stage_graph import STAGES_V1
    from realtime.stage_runners_v2 import RUNNER_FACTORIES

    assert RUNNER_FACTORIES["REFERENCE"] is runners.reference_runner
    assert RUNNER_FACTORIES["EQUITY_INGEST"] is runners.equity_ingest_runner
    assert STAGES_V1["REFERENCE"].capability_role == "references"
    assert STAGES_V1["EQUITY_INGEST"].capability_role == "ingress"


def test_fail_closed_stages_name_the_enabled_source_gated_stages_an_environment_cannot_serve():
    from realtime.stage_runners_v2 import fail_closed_stages

    enabled = frozenset({"REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE"})
    assert fail_closed_stages(enabled, _environment()) == {
        "REFERENCE": runners.REFERENCE_SOURCE_NOT_CONFIGURED, "EQUITY_INGEST": runners.EQUITY_SOURCE_NOT_CONFIGURED}
    assert fail_closed_stages(frozenset({"EQUITY_EVIDENCE", "STOCK_ALERTS"}), _environment()) == {}
    source = FakeEquitySource(lambda req: pytest.fail("must not collect"))
    configured = _environment(reference_service=FailingService(), equity_source=source, universe=SMALL_UNIVERSE)
    assert fail_closed_stages(enabled, configured) == {}
    no_universe = _environment(reference_service=FailingService(), equity_source=source)
    assert fail_closed_stages(enabled, no_universe) == {
        "REFERENCE": runners.UNIVERSE_NOT_CONFIGURED, "EQUITY_INGEST": runners.UNIVERSE_NOT_CONFIGURED}
    assert source.requests == []


def test_the_orchestrator_script_reports_the_fail_closed_stages_and_keeps_its_guards():
    import importlib

    from realtime.stage_runners_v2 import fail_closed_stages, unwired_stages

    script = importlib.import_module("scripts.run_realtime_v2_orchestrator")
    assert script.fail_closed_stages is fail_closed_stages and script.unwired_stages is unwired_stages
    assert "REFERENCE_SOURCE_NOT_CONFIGURED" in script.__doc__ and "EQUITY_SOURCE_NOT_CONFIGURED" in script.__doc__
    assert "Only the OPTIONS_* stages are unwired" in script.__doc__
    base = ["--database", "test_realtime_v2_x", "--confirm-database-name", "test_realtime_v2_x",
            "--allow-disposable", "--plan-id", str(uuid4())]
    assert script.parse_args(["plan", *base]).mode == "plan"
    with pytest.raises(SystemExit):
        script.parse_args(["once", *base])  # mutation still needs --execute
    with pytest.raises(ValueError):
        script.parse_args(["plan", "--database", "test_realtime_v2_x", "--confirm-database-name",
                           "test_realtime_v2_x", "--plan-id", str(uuid4())])  # a test database needs --allow-disposable
