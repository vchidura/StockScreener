"""DB-backed checks that the six non-options runners work through the real stage orchestrator on the V2 schema.

Opt in with STOCKS_V2_DB_TESTS=1 and run WITHOUT the offline I/O guard plugin. The database is
test_realtime_v2_bind_a; it is created from the sql_v2 baseline and always dropped. The sources are
fakes (no provider is called); credentials stay inside tests/realtime/v2_database.py.

Exercised for real: REFERENCE (seed manifests, current pointer, readiness), EQUITY_INGEST (provider
ledger, acquisition batches/facts, watermarks, all fenced under the ingress role), EQUITY_EVIDENCE's
bar phase (acquisition batches -> materialization jobs -> rt.bar_catalog / bar_revisions / bar_current under
the compute role: idempotent replay, cancellation between jobs, retry, contract-invalid) and its receipt
generation, STOCK_ALERTS (the retained-state guard SQL, the snapshot and both releases under the
publication role) and STRATEGY_PUBLISH's receipt dispatch. NOT exercised here: the strategy producer's
own SQL and EQUITY_EVIDENCE's projection (both replaced by fakes: compute_realtime_ticker_cycle is stubbed
AFTER the real bar phase because the projection needs deep history), the session-close kept release, and
the hardened role grants (the runtime connects as the owner in deferred security mode; the transaction
roles each stage asked for are recorded and checked instead).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

import exchange_calendars
import pytest

import realtime.equity_bar_drain as bar_drain
import realtime.stage_runners_alerts as alert_runners
import realtime.stage_runners_equity as equity_runners
import realtime.stock_alert_repository as alert_repository
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.lineage_head import reference_set
from realtime.orchestration_repository import PostgresOrchestrationRepository, WindowSpec
from realtime.portal_projection import build_portal_snapshot
from realtime.preflight import ProviderTier
from realtime.reference_facts import ReferenceFact, ReferenceFactRelation
from realtime.reference_service import ReferenceServiceResult
from realtime.runner_environment import RunnerEnvironment
from realtime.screening_projection import RealtimeScreeningInput
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome
from realtime.stage_runners_ingest import equity_ingest_runner, reference_runner
from realtime.stock_alert_projection import STOCK_ALERTS_VERSION
from tests.realtime.test_stage_runners_ingest import (
    SMALL_UNIVERSE,
    FakeEquitySource,
    FakeReferenceService,
    equity_result,
    reference_manifest,
)
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_bind_a"
UTC = timezone.utc
ENABLED = frozenset({"REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STRATEGY_PUBLISH",
                     "STOCK_ALERTS"})


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as connection_pool:
        yield connection_pool


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _query(pool, statement, parameters=()):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.fetchall()
    finally:
        connection.rollback()
        pool.putconn(connection)


class _Clock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


@dataclass
class World:
    pool: object
    provider: V2ConnectionProvider
    repository: PostgresOrchestrationRepository
    cohort: object
    plan_id: object
    now: datetime
    clock: _Clock
    security_ids: dict
    instrument_ids: dict
    transactions: list = field(default_factory=list)
    windows: int = 0

    def window(self, cutoff_offset: int):
        self.windows += 1
        return self.repository.ensure_window(self.plan_id, WindowSpec(
            self.cohort, "MASSIVE_DEVELOPER", f"bind-{self.windows}", self.now, self.now + timedelta(seconds=cutoff_offset),
            self.now + timedelta(seconds=300)))

    def orchestrator(self, runners):
        table = {stage: (lambda context: StageOutcome.complete()) for stage in ENABLED}
        table.update(runners)
        return StageOrchestrator(self.repository, table, database=self.provider, clock=self.clock)

    def states(self, window_id):
        return {stage: record.state for stage, record in self.repository.stage_states(window_id).items()}

    def roles(self, *, write=None, fenced=None):
        return {role for role, _, read_only, has_fence in self.transactions
                if (write is None or write == (not read_only)) and (fenced is None or fenced == has_fence)}


def _world(pool) -> World:
    now = datetime.now(UTC)  # microseconds kept: two worlds must never produce identical seed manifests
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True))
    provider.verify()
    session = exchange_calendars.get_calendar("XNYS").date_to_session(str(now.date()), direction="previous").date()
    security_ids, instrument_ids = {}, {}
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cohort = insert_cohort(cursor, now)
            plan_id = uuid4()
            cursor.execute(
                "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, "
                "enabled_stages, config_sha256, deadline, scope_reference) "
                "VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,'fixture scope')",
                (str(plan_id), str(cohort), session, sorted(ENABLED), uuid4().hex + uuid4().hex,
                 now + timedelta(hours=8)))
            for ticker in SMALL_UNIVERSE.equity_tickers:
                security_ids[ticker] = uuid4()
                cursor.execute("INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                               "VALUES (%s, 'STOCK', 'fixture', %s)", (str(security_ids[ticker]), now))
                cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                               "RETURNING instrument_id", (str(security_ids[ticker]),))
                instrument_ids[ticker] = cursor.fetchone()[0]
        connection.commit()
    finally:
        pool.putconn(connection)
    world = World(pool, provider, PostgresOrchestrationRepository(provider), cohort, plan_id, now, _Clock(now),
                  security_ids, instrument_ids)
    original = provider.transaction

    def recording(capability_role, principal_profile, **options):
        world.transactions.append((capability_role, principal_profile, bool(options.get("read_only")),
                                   options.get("fence") is not None))
        return original(capability_role, principal_profile, **options)

    provider.transaction = recording
    return world


def reference_facts(universe, base):
    """Durable facts for every READY member of the manifest FakeReferenceService builds; ids are fixed so
    repeated refreshes re-use the same rows (ON CONFLICT DO NOTHING)."""
    def uid(*parts):
        return uuid5(NAMESPACE_URL, "bind-test:" + ":".join(parts))

    facts = []
    for ticker in universe.equity_tickers:
        security = uid("security", ticker)
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.SECURITY, "SECURITY", ticker, security_id=security,
            revision_id=uid("revision", ticker), ticker=ticker, asset_type="STOCK", source_origin="fixture",
            exchange="XNAS", calendar_id="XNYS", effective_from=base - timedelta(days=30), effective_to=None,
            observed_at=base - timedelta(days=29), payload_sha256=_sha(f"security:{ticker}")))
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.CORPORATE_ACTION_COVERAGE, "CORPORATE_ACTION_COVERAGE", ticker,
            coverage_id=uid("coverage", ticker), security_id=security, action_type="DIVIDEND",
            window_start=(base - timedelta(days=30)).date(), window_end=base.date(), source="massive",
            source_key=f"dividends:{ticker}", response_action_count=0, response_sha256=_sha(f"response:{ticker}"),
            observed_at=base - timedelta(hours=1), received_at=base, payload_sha256=_sha(f"coverage:{ticker}")))
    facts.append(ReferenceFact.build(
        ReferenceFactRelation.MARKET_EVENT_COVERAGE, "FOMC_COVERAGE", "GLOBAL", coverage_id=uid("fomc"),
        security_id=None, event_type="FED_RATE_DECISION", window_start=(base - timedelta(days=30)).date(),
        window_end=(base + timedelta(days=30)).date(), source="federal_reserve", verdict="COVERED",
        checked_at=base, valid_until=base + timedelta(hours=6), evidence_sha256=_sha("fomc")))
    facts.append(ReferenceFact.build(
        ReferenceFactRelation.RATE_OBSERVATION, "RATE_CURVE", "USD", observation_id=uid("rate"),
        rate_date=base.date(), tenor_days=30, annual_rate=0.04, source="treasury", source_key="30d",
        source_observed_at=base, received_at=base, payload_sha256=_sha("rate")))
    return tuple(facts)


class ReferenceService(FakeReferenceService):
    """Builds a READY manifest with its durable facts (or an UNAVAILABLE one without facts)."""

    def run_once(self, request):
        self.requests.append(request)
        manifest = reference_manifest(request, unavailable_rate=self.unavailable_rate)
        facts = () if self.unavailable_rate else reference_facts(request.universe, self.base)
        return ReferenceServiceResult(manifest, 0, 0, facts)

    base = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)


def _environment(world, **fields):
    return RunnerEnvironment(world.provider, build_id="build-bind", universe=SMALL_UNIVERSE, **fields)


def _tick_reference(world, service, *, at, scope="MARKET_REFERENCES"):
    environment = _environment(world, reference_service=service, reference_scope=scope)
    world.clock.value = at
    orchestrator = world.orchestrator({"REFERENCE": reference_runner(environment)})
    return orchestrator.tick_resident(world.plan_id, "REFERENCE")


def _seed_state(pool):
    ready = _query(pool, "SELECT count(*) FROM rt.seed_manifests WHERE status = 'READY'")[0][0]
    current = _query(pool, "SELECT generation FROM rt.seed_current WHERE scope = 'MARKET_REFERENCES'")
    return ready, (current[0][0] if current else None)


def test_a_ready_refresh_raises_the_ready_count_and_the_pointer_together_and_an_unavailable_one_changes_neither(pool):
    world = _world(pool)
    ready_before, pointer_before = _seed_state(pool)
    first = _tick_reference(world, ReferenceService(), at=world.now - timedelta(minutes=30))
    assert first.state == "COMPLETE" and first.reason_codes == ()
    assert _seed_state(pool) == (ready_before + 1, ready_before + 1)
    assert world.repository.readiness(world.cohort)["REFERENCE"] == "READY"
    seed_id = _query(pool, "SELECT seed_id FROM rt.seed_current WHERE scope = 'MARKET_REFERENCES'")[0][0]
    assert first.evidence_ref == str(seed_id)

    unavailable = _tick_reference(world, ReferenceService(unavailable_rate=True),
                                  at=world.now - timedelta(minutes=29))
    assert unavailable.state == "BLOCKED_POLICY" and unavailable.reason_codes == ("UNAVAILABLE_RATE_CURVE:USD",)
    assert _seed_state(pool) == (ready_before + 1, ready_before + 1)  # persisted, never current, not counted
    stored = _query(pool, "SELECT status, reason_codes FROM rt.seed_manifests WHERE seed_id = %s",
                    (unavailable.evidence_ref,))
    assert stored == [("UNAVAILABLE", ["UNAVAILABLE_RATE_CURVE:USD"])]
    assert world.repository.readiness(world.cohort)["REFERENCE"] == "BLOCKED"

    second = _tick_reference(world, ReferenceService(), at=world.now - timedelta(minutes=28))
    assert second.state == "COMPLETE"
    assert _seed_state(pool) == (ready_before + 2, ready_before + 2)
    cutoff = world.now - timedelta(seconds=90)
    assert _query(pool, "SELECT count(*) FROM rt.seed_manifests WHERE status = 'READY' AND created_at <= %s",
                  (cutoff,))[0][0] == ready_before + 2  # the generation reference_generation derives
    assert world.provider.role_violations == []
    assert world.roles(write=True) >= {"references"} and "ingress" not in world.roles()


def test_the_unconfigured_reference_and_ingest_stages_are_blocked_through_the_orchestrator_without_writes(pool):
    world = _world(pool)
    world.clock.value = world.now - timedelta(minutes=5)
    baseline = _seed_state(pool)
    blocked = world.orchestrator({"REFERENCE": reference_runner(_environment(world))}).tick_resident(
        world.plan_id, "REFERENCE")
    assert (blocked.state, blocked.reason_codes) == ("BLOCKED_POLICY", ("REFERENCE_SOURCE_NOT_CONFIGURED",))
    assert world.repository.readiness(world.cohort)["REFERENCE"] == "BLOCKED"
    assert _seed_state(pool) == baseline

    world.repository.upsert_readiness(world.cohort, "REFERENCE", "stages_v1", "READY", (), None, world.now)
    world.clock.value = world.now
    window = world.window(-90)
    world.orchestrator({"EQUITY_INGEST": equity_ingest_runner(_environment(world))}).run_window(window.window_id)
    assert world.states(window.window_id)["EQUITY_INGEST"] == "BLOCKED_POLICY"
    assert ("EQUITY_INGEST", 1, "BLOCKED_POLICY", ("EQUITY_SOURCE_NOT_CONFIGURED",)) in [
        (row[0], row[1], row[2], tuple(row[3])) for row in _query(
            pool, "SELECT stage_id, attempt, state, reason_codes FROM rt.stage_runs WHERE window_id = %s",
            (str(window.window_id),))]
    assert _query(pool, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id = %s",
                  (str(world.cohort),))[0][0] == 0
    assert "ingress" not in world.roles(write=True)


def _alerts_snapshot(streams):
    """Stands in for produce_realtime_stock_alerts; `streams` is mutated between windows to model forward state."""
    def produce(inputs):
        generated = datetime.now(UTC) - timedelta(minutes=1)
        return build_portal_snapshot(
            cohort_id=inputs.cohort_id, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            snapshot_type=STOCK_ALERTS_VERSION, snapshot_key="PLATFORM", source_generation=inputs.source_generation,
            source_manifest=dict(version=STOCK_ALERTS_VERSION, cohort_id=str(inputs.cohort_id),
                                 source_manifest_sha256="a" * 64, source_generation=inputs.source_generation,
                                 provider_tier="MASSIVE_DEVELOPER", reference_set=reference_set([])),
            payload=dict(version=STOCK_ALERTS_VERSION, references=[], streams=list(streams),
                         cutoff=inputs.cutoff.isoformat(),
                         missing_producers={"forward": "not produced", "replay": "not produced"},
                         provider_fetch_performed=False, publication_permission=False, execution_permission=False),
            generated_at=generated)

    return produce


def test_the_ingest_evidence_alerts_and_strategy_stages_run_in_dependency_order_on_the_real_schema(pool, monkeypatch):
    world = _world(pool)
    ready_before, _ = _seed_state(pool)
    for index in range(2):
        assert _tick_reference(world, ReferenceService(), at=world.now - timedelta(minutes=30 - index)).state == (
            "COMPLETE")
    generation = ready_before + 2

    source = FakeEquitySource(lambda request: equity_result(
        request, security_ids=world.security_ids, instrument_ids=world.instrument_ids))
    strategy_calls, streams = [], []
    monkeypatch.setattr(equity_runners, "compute_realtime_ticker_cycle", lambda database, **scope: SimpleNamespace(
        inputs=RealtimeScreeningInput(
            cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER, source_manifest_sha256="a" * 64,
            source_generation=scope["source_generation"], session=scope["session"], cutoff=scope["cutoff"],
            members=(), bars=(), actions=(), coverage=())))
    monkeypatch.setattr(alert_repository, "load_realtime_screening_input", lambda cursor, **scope: SimpleNamespace(**scope))
    monkeypatch.setattr(alert_repository, "produce_realtime_stock_alerts", _alerts_snapshot(streams))
    monkeypatch.setattr(alert_runners, "materialize_due_realtime_strategies",
                        lambda compute, publication, **options: strategy_calls.append(
                            (compute.process_plan.role.value, publication.process_plan.role.value, options)) or (
                                dict(interval=options["intervals"][0], market_time="t", status="PUBLISHED",
                                     snapshot_id=str(uuid4())),))
    environment = _environment(world, equity_source=source)
    runners = {
        "EQUITY_INGEST": equity_ingest_runner(environment),
        "EQUITY_EVIDENCE": equity_runners.equity_evidence_runner(environment),
        "STOCK_ALERTS": alert_runners.stock_alerts_runner(environment),
        "STRATEGY_PUBLISH": alert_runners.strategy_publish_runner(environment),
    }
    world.clock.value = world.now
    world.transactions.clear()
    first = world.window(-90)
    report = world.orchestrator(runners).run_window(first.window_id)
    states = world.states(first.window_id)
    assert report.window_state == "COMPLETE", (states, report)
    assert {states[stage] for stage in ("EQUITY_INGEST", "EQUITY_EVIDENCE", "STOCK_ALERTS", "STRATEGY_PUBLISH")} == {
        "COMPLETE"}
    order = [stage for stage, _ in report.executed]
    assert order.index("EQUITY_INGEST") < order.index("EQUITY_EVIDENCE") < order.index("STOCK_ALERTS")
    assert order.index("EQUITY_EVIDENCE") < order.index("STRATEGY_PUBLISH")

    # EQUITY_INGEST: the source was asked for the window cutoff; ledger, batches, facts and watermarks landed.
    request, = source.requests
    assert request.requested_at == first.cutoff and request.universe == SMALL_UNIVERSE
    cohort = str(world.cohort)
    assert _query(pool, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id = %s", (cohort,))[0][0] == 2
    assert _query(pool, "SELECT count(*) FROM rt.acquisition_facts AS fact JOIN rt.acquisition_batches AS batch "
                  "USING (acquisition_batch_id) WHERE batch.cohort_id = %s", (cohort,))[0][0] == 2
    assert _query(pool, "SELECT count(*) FROM rt.acquisition_watermarks WHERE cohort_id = %s", (cohort,))[0][0] == 2
    ledger = _query(pool, "SELECT window_id, completeness, state FROM rt.provider_request_ledger WHERE cohort_id = %s",
                    (cohort,))
    assert len(ledger) == 2 and {(str(row[0]), row[1], row[2]) for row in ledger} == {
        (str(first.window_id), "COMPLETE", "COMPLETE")}

    # The receipt carries the generation EQUITY_EVIDENCE derived from the READY seeds; the alerts snapshot too.
    receipts = _query(pool, "SELECT receipt_id, reference_generation FROM rt.cycle_receipts WHERE window_id = %s",
                      (str(first.window_id),))
    assert [row[1] for row in receipts] == [generation]
    snapshots = _query(pool, "SELECT snapshot_type, source_generation FROM rt.snapshot_catalog WHERE cohort_id = %s",
                       (cohort,))
    assert snapshots == [(STOCK_ALERTS_VERSION, generation)]
    releases = _query(pool, "SELECT receipt_id, release_class, cutoff FROM rt.portal_releases WHERE cohort_id = %s",
                      (cohort,))
    assert releases == [(receipts[0][0], "INTRADAY", first.cutoff)]
    assert _query(pool, "SELECT cutoff FROM rt.portal_release_current WHERE cohort_id = %s", (cohort,)) == [
        (first.cutoff,)]

    # STRATEGY_PUBLISH: one call per interval, compute then publication, against the window's receipt.
    assert [(c, p, o["intervals"], o["available_by"]) for c, p, o in strategy_calls] == [
        ("COMPUTE", "PUBLICATION", ("1d",), first.deadline), ("COMPUTE", "PUBLICATION", ("1h",), first.deadline)]
    assert all(str(o["receipt"].receipt_id) == str(receipts[0][0]) for _, _, o in strategy_calls)

    # Capability roles: ingress writes (fenced), compute writes only the bars/jobs of the bar phase (fenced),
    # publication writes (fenced).
    assert world.roles(write=True, fenced=True) == {"ingress", "compute", "publication"}
    assert "compute" in world.roles(write=False)
    # The bar phase materialized the two ingested 1m bars in the same window, before the (stubbed) projection.
    assert _query(pool, "SELECT count(*) FROM rt.bar_catalog WHERE cohort_id = %s", (cohort,))[0][0] == 2
    assert _query(pool, "SELECT count(*) FROM rt.bar_current WHERE cohort_id = %s", (cohort,))[0][0] == 2
    assert _query(pool, "SELECT status, count(*) FROM rt.materialization_jobs WHERE cohort_id = %s GROUP BY status",
                  (cohort,)) == [("COMPLETE", 2)]
    assert world.provider.role_violations == []

    # A second window replaces the retained alert snapshot (no forward state) through the real guard SQL.
    later = {stage: runner for stage, runner in runners.items() if stage != "EQUITY_INGEST"}
    world.clock.value = world.now
    second = world.window(-30)
    report = world.orchestrator(later).run_window(second.window_id)
    assert world.states(second.window_id)["STOCK_ALERTS"] == "COMPLETE", report
    assert _query(pool, "SELECT count(*) FROM rt.snapshot_catalog WHERE cohort_id = %s", (cohort,))[0][0] == 2
    assert _query(pool, "SELECT cutoff FROM rt.portal_release_current WHERE cohort_id = %s", (cohort,)) == [
        (second.cutoff,)]
    assert world.repository.readiness(world.cohort)["STOCK_ALERTS"] == "DEGRADED"  # forward/replay not produced

    # Once a published snapshot carries forward streams, the next window refuses to replace it.
    streams.append({"stream": "intraday"})
    third = world.window(-20)
    world.orchestrator(later).run_window(third.window_id)
    assert world.states(third.window_id)["STOCK_ALERTS"] == "COMPLETE"
    fourth = world.window(-10)
    world.orchestrator(later).run_window(fourth.window_id)
    assert world.states(fourth.window_id)["STOCK_ALERTS"] == "BLOCKED_POLICY"
    blocked = _query(pool, "SELECT reason_codes FROM rt.stage_runs WHERE window_id = %s AND stage_id = 'STOCK_ALERTS'",
                     (str(fourth.window_id),))
    assert [tuple(row[0]) for row in blocked] == [("STOCK_ALERTS_RETAINED_FORWARD_STATE",)]
    assert _query(pool, "SELECT count(*) FROM rt.snapshot_catalog WHERE cohort_id = %s", (cohort,))[0][0] == 3
    assert world.repository.readiness(world.cohort)["STOCK_ALERTS"] == "BLOCKED"
    assert world.provider.role_violations == []


def test_a_missing_equity_cycle_receipt_fails_alerts_and_strategy_on_the_real_schema(pool, monkeypatch):
    world = _world(pool)
    world.clock.value = world.now - timedelta(minutes=5)
    world.repository.upsert_readiness(world.cohort, "REFERENCE", "stages_v1", "READY", (), None, world.now)
    environment = _environment(world)
    runners = {
        "EQUITY_EVIDENCE": lambda context: StageOutcome.complete(),  # reports no receipt
        "STOCK_ALERTS": alert_runners.stock_alerts_runner(environment),
        "STRATEGY_PUBLISH": alert_runners.strategy_publish_runner(environment),
    }
    monkeypatch.setattr(alert_runners, "materialize_due_realtime_strategies",
                        lambda *a, **k: pytest.fail("no receipt, nothing to publish"))
    monkeypatch.setattr(alert_repository, "load_realtime_screening_input",
                        lambda *a, **k: pytest.fail("no receipt, nothing to compute"))
    world.clock.value = world.now
    window = world.window(-90)
    world.orchestrator(runners).run_window(window.window_id)
    stored = {row[0]: (row[1], tuple(row[2])) for row in _query(
        pool, "SELECT stage_id, state, reason_codes FROM rt.stage_runs WHERE window_id = %s", (str(window.window_id),))}
    assert stored["STOCK_ALERTS"] == ("FAILED", ("EQUITY_CYCLE_RECEIPT_MISSING",))
    assert stored["STRATEGY_PUBLISH"] == ("FAILED", ("EQUITY_CYCLE_RECEIPT_MISSING",))
    assert _query(pool, "SELECT count(*) FROM rt.snapshot_catalog WHERE cohort_id = %s",
                  (str(world.cohort),))[0][0] == 0


# --- the bar phase of EQUITY_EVIDENCE ------------------------------------------------------------------------------

BARS = (("1m", 3), ("5m", 1))  # per ticker: three 1m bars and one 5m bar
JOB_KIND = "DEVELOPER_EQUITY_ACQUISITION"


def _execute(pool, statement, parameters=()):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters)
        connection.commit()
    finally:
        pool.putconn(connection)


def _caching_source(world, **options):
    """A fake equity source returning real EquityBarRevision facts; a replay gets the identical result."""
    cache = []

    def build(request):
        if not cache:
            cache.append(equity_result(request, security_ids=world.security_ids, instrument_ids=world.instrument_ids,
                                       bars=BARS, **options))
        return cache[0]

    return FakeEquitySource(build), cache


def _ready_world(pool, monkeypatch):
    """A world with a READY seed (so the receipt generation is positive) and the projection stubbed.

    Only compute_realtime_ticker_cycle is replaced, and it runs AFTER the real bar phase: the real
    projection needs deep history that a handful of bars cannot provide."""
    world = _world(pool)
    assert _tick_reference(world, ReferenceService(), at=world.now - timedelta(minutes=30)).state == "COMPLETE"
    monkeypatch.setattr(equity_runners, "compute_realtime_ticker_cycle", lambda database, **scope: SimpleNamespace(
        inputs=RealtimeScreeningInput(
            cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER, source_manifest_sha256="a" * 64,
            source_generation=scope["source_generation"], session=scope["session"], cutoff=scope["cutoff"],
            members=(), bars=(), actions=(), coverage=())))
    world.clock.value = world.now
    return world


def _bar_rows(pool, cohort):
    parameters = (str(cohort),)
    return dict(
        catalog=_query(pool, "SELECT bar_revision_id::text, provider_tier, instrument_id, interval "
                       "FROM rt.bar_catalog WHERE cohort_id = %s", parameters),
        revisions=_query(pool, "SELECT bar_revision_id::text, provider_tier, instrument_id, ticker, interval, "
                         "payload_sha256 FROM rt.bar_revisions WHERE cohort_id = %s", parameters),
        current=_query(pool, "SELECT bar_revision_id::text, provider_tier, instrument_id, interval, source_kind "
                       "FROM rt.bar_current WHERE cohort_id = %s", parameters))


def _expected_bars(result, tickers=None):
    facts = [fact for batch in result.batches for fact in batch.facts
             if tickers is None or fact.revision.ticker in tickers]
    return dict(
        catalog=sorted((str(f.revision.bar_revision_id), "MASSIVE_DEVELOPER", f.instrument_id, f.revision.interval)
                       for f in facts),
        revisions=sorted((str(f.revision.bar_revision_id), "MASSIVE_DEVELOPER", f.instrument_id, f.revision.ticker,
                          f.revision.interval, f.revision.payload_sha256) for f in facts),
        current=sorted((str(f.revision.bar_revision_id), "MASSIVE_DEVELOPER", f.instrument_id, f.revision.interval,
                        "NATIVE_REST") for f in facts))


def _sorted_rows(rows):
    return {name: sorted(values) for name, values in rows.items()}


def _watermarks(pool, cohort):
    return _query(pool, "SELECT instrument_id, interval, source_kind, last_complete_bar_end, terminal_state, "
                  "request_id::text, updated_at FROM rt.acquisition_watermarks WHERE cohort_id = %s "
                  "ORDER BY instrument_id, interval", (str(cohort),))


def _jobs(pool, cohort):
    rows = _query(pool, "SELECT status, attempts, failure_code FROM rt.materialization_jobs "
                  "WHERE cohort_id = %s AND job_kind = %s", (str(cohort), JOB_KIND))
    return sorted(rows, key=lambda row: (row[0], row[1]))


def _stage(pool, window, stage):
    rows = _query(pool, "SELECT state, reason_codes FROM rt.stage_runs WHERE window_id = %s AND stage_id = %s",
                  (str(window.window_id), stage))
    assert len(rows) == 1
    return rows[0][0], tuple(rows[0][1])


def _receipt_count(pool, window):
    return _query(pool, "SELECT count(*) FROM rt.cycle_receipts WHERE window_id = %s", (str(window.window_id),))[0][0]


def test_the_evidence_bar_phase_materializes_ingested_bars_once_and_a_replay_adds_nothing(pool, monkeypatch):
    world = _ready_world(pool, monkeypatch)
    source, cache = _caching_source(world)
    environment = _environment(world, equity_source=source)
    ingest = equity_ingest_runner(environment)
    evidence = equity_runners.equity_evidence_runner(environment)

    # Window 1: only the ingest runs (the evidence stage is a stub): batches and watermarks, no bars, no jobs.
    first = world.window(-90)
    world.orchestrator({"EQUITY_INGEST": ingest}).run_window(first.window_id)
    assert _stage(pool, first, "EQUITY_INGEST") == ("COMPLETE", ())
    result = cache[0]
    assert _query(pool, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id = %s",
                  (str(world.cohort),))[0][0] == 2
    assert _bar_rows(pool, world.cohort) == dict(catalog=[], revisions=[], current=[])
    assert _jobs(pool, world.cohort) == []
    watermarks = _watermarks(pool, world.cohort)
    assert len(watermarks) == 4  # two tickers x (1m, 5m)

    # Window 2: the real evidence runner drains the batches under the compute role before the projection.
    world.transactions.clear()
    second = world.window(-60)
    world.orchestrator({"EQUITY_EVIDENCE": evidence}).run_window(second.window_id)
    assert _stage(pool, second, "EQUITY_EVIDENCE") == ("COMPLETE", ()) and _receipt_count(pool, second) == 1
    rows = _sorted_rows(_bar_rows(pool, world.cohort))
    assert rows == _expected_bars(result) and len(rows["revisions"]) == 8
    assert {row[1] for row in rows["catalog"]} == {"MASSIVE_DEVELOPER"}
    assert {(row[3], row[2]) for row in rows["revisions"]} == {
        (ticker, world.instrument_ids[ticker]) for ticker in SMALL_UNIVERSE.equity_tickers}
    assert _jobs(pool, world.cohort) == [("COMPLETE", 1, None), ("COMPLETE", 1, None)]
    assert _watermarks(pool, world.cohort) == watermarks  # the bar phase never touches acquisition watermarks
    assert world.roles(write=True, fenced=True) == {"compute"} and world.provider.role_violations == []

    # Window 3: the same ingest result is replayed and the bar phase runs again: nothing is added anywhere.
    third = world.window(-30)
    world.orchestrator({"EQUITY_INGEST": ingest, "EQUITY_EVIDENCE": evidence}).run_window(third.window_id)
    assert _stage(pool, third, "EQUITY_INGEST") == ("COMPLETE", ()) and _stage(
        pool, third, "EQUITY_EVIDENCE") == ("COMPLETE", ())
    assert _sorted_rows(_bar_rows(pool, world.cohort)) == rows
    assert _jobs(pool, world.cohort) == [("COMPLETE", 1, None), ("COMPLETE", 1, None)]
    assert _watermarks(pool, world.cohort) == watermarks
    assert _query(pool, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id = %s",
                  (str(world.cohort),))[0][0] == 2
    assert _query(pool, "SELECT count(*) FROM rt.provider_request_ledger WHERE cohort_id = %s",
                  (str(world.cohort),))[0][0] == 2
    assert len(source.requests) == 2 and world.repository.readiness(world.cohort)["EQUITY_EVIDENCE"] == "READY"


def test_a_stage_cancelled_between_bar_jobs_leaves_the_rest_unclaimed_for_the_next_window(pool, monkeypatch):
    world = _ready_world(pool, monkeypatch)
    source, cache = _caching_source(world)
    environment = _environment(world, equity_source=source)
    world.orchestrator({"EQUITY_INGEST": equity_ingest_runner(environment)}).run_window(world.window(-90).window_id)

    real_process, holder = bar_drain.process_developer_acquisition_job, {}

    def process_then_cancel(database, claimed):
        outputs = real_process(database, claimed)
        holder["context"].cancelled.set()  # the coordinator abandons the stage right after the first job
        return outputs

    inner = equity_runners.equity_evidence_runner(environment)

    def runner(context):
        holder["context"] = context
        return inner(context)

    monkeypatch.setattr(bar_drain, "process_developer_acquisition_job", process_then_cancel)
    second = world.window(-60)
    world.orchestrator({"EQUITY_EVIDENCE": runner}).run_window(second.window_id)
    assert _stage(pool, second, "EQUITY_EVIDENCE") == ("FAILED", ("STAGE_CANCELLED",))
    assert _receipt_count(pool, second) == 0
    # One batch was materialized (and its job completed); the other job was never claimed, so nothing is leased.
    assert _jobs(pool, world.cohort) == [("COMPLETE", 1, None), ("PENDING", 0, None)]
    partial = _bar_rows(pool, world.cohort)
    assert len(partial["revisions"]) == 4 and len({row[3] for row in partial["revisions"]}) == 1

    monkeypatch.setattr(bar_drain, "process_developer_acquisition_job", real_process)
    third = world.window(-30)
    world.orchestrator({"EQUITY_EVIDENCE": inner}).run_window(third.window_id)
    assert _stage(pool, third, "EQUITY_EVIDENCE") == ("COMPLETE", ()) and _receipt_count(pool, third) == 1
    assert _sorted_rows(_bar_rows(pool, world.cohort)) == _expected_bars(cache[0])
    assert _jobs(pool, world.cohort) == [("COMPLETE", 1, None), ("COMPLETE", 1, None)]
    assert world.provider.role_violations == []


def test_a_transient_bar_job_failure_degrades_the_stage_and_a_later_window_retries_it(pool, monkeypatch):
    world = _ready_world(pool, monkeypatch)
    source, cache = _caching_source(world)
    environment = _environment(world, equity_source=source)
    world.orchestrator({"EQUITY_INGEST": equity_ingest_runner(environment)}).run_window(world.window(-90).window_id)
    evidence = equity_runners.equity_evidence_runner(environment)

    real_process, calls = bar_drain.process_developer_acquisition_job, []

    def flaky(database, claimed):
        calls.append(claimed.job.job_id)
        if len(calls) == 1:
            raise RuntimeError("database unavailable")
        return real_process(database, claimed)

    monkeypatch.setattr(bar_drain, "process_developer_acquisition_job", flaky)
    second = world.window(-60)
    world.orchestrator({"EQUITY_EVIDENCE": evidence}).run_window(second.window_id)
    assert _stage(pool, second, "EQUITY_EVIDENCE") == ("COMPLETE", ("EQUITY_BAR_MATERIALIZATION_RETRY:1",))
    assert _receipt_count(pool, second) == 1  # a retryable failure does not block the cycle
    assert world.repository.readiness(world.cohort)["EQUITY_EVIDENCE"] == "DEGRADED"
    assert _jobs(pool, world.cohort) == [("COMPLETE", 1, None), ("PENDING", 1, "DEVELOPER_ACQUISITION_OUTPUT_FAILED")]
    assert len(_bar_rows(pool, world.cohort)["revisions"]) == 4

    third = world.window(-45)  # inside the retry delay: nothing is claimable, which is not a failure
    world.orchestrator({"EQUITY_EVIDENCE": evidence}).run_window(third.window_id)
    assert _stage(pool, third, "EQUITY_EVIDENCE") == ("COMPLETE", ())
    assert len(_bar_rows(pool, world.cohort)["revisions"]) == 4

    _execute(pool, "UPDATE rt.materialization_jobs SET available_at = clock_timestamp() - interval '1 second' "
             "WHERE cohort_id = %s AND status = 'PENDING'", (str(world.cohort),))
    fourth = world.window(-30)
    world.orchestrator({"EQUITY_EVIDENCE": evidence}).run_window(fourth.window_id)
    assert _stage(pool, fourth, "EQUITY_EVIDENCE") == ("COMPLETE", ())
    assert _jobs(pool, world.cohort) == [("COMPLETE", 1, None), ("COMPLETE", 2, None)]
    assert _sorted_rows(_bar_rows(pool, world.cohort)) == _expected_bars(cache[0])


def test_a_batch_from_an_incomplete_provider_request_is_refused_and_fails_the_stage_once(pool, monkeypatch):
    world = _ready_world(pool, monkeypatch)
    source, cache = _caching_source(world, incomplete=("MSFT",))
    environment = _environment(world, equity_source=source)
    first = world.window(-90)
    world.orchestrator({"EQUITY_INGEST": equity_ingest_runner(environment)}).run_window(first.window_id)
    assert _stage(pool, first, "EQUITY_INGEST") == ("COMPLETE", ("EQUITY_WATERMARK_NOT_ADVANCED",))
    assert {row[0] for row in _watermarks(pool, world.cohort)} == {world.instrument_ids["AAPL"]}
    assert {row[0] for row in _query(
        pool, "SELECT completeness FROM rt.provider_request_ledger WHERE cohort_id = %s",
        (str(world.cohort),))} == {"COMPLETE", "INCOMPLETE"}

    evidence = equity_runners.equity_evidence_runner(environment)
    second = world.window(-60)
    world.orchestrator({"EQUITY_EVIDENCE": evidence}).run_window(second.window_id)
    assert _stage(pool, second, "EQUITY_EVIDENCE") == ("FAILED", ("EQUITY_BAR_MATERIALIZATION_FAILED",))
    assert _receipt_count(pool, second) == 0
    assert world.repository.readiness(world.cohort)["EQUITY_EVIDENCE"] == "BLOCKED"
    assert _jobs(pool, world.cohort) == [("BLOCKED", 1, "DEVELOPER_ACQUISITION_CONTRACT_INVALID"),
                                         ("COMPLETE", 1, None)]
    assert _sorted_rows(_bar_rows(pool, world.cohort)) == _expected_bars(cache[0], tickers={"AAPL"})

    # The refused batch is never retried, so the next window proceeds (without MSFT's bars from that read).
    third = world.window(-30)
    world.orchestrator({"EQUITY_EVIDENCE": evidence}).run_window(third.window_id)
    assert _stage(pool, third, "EQUITY_EVIDENCE") == ("COMPLETE", ()) and _receipt_count(pool, third) == 1
    assert _sorted_rows(_bar_rows(pool, world.cohort)) == _expected_bars(cache[0], tickers={"AAPL"})
