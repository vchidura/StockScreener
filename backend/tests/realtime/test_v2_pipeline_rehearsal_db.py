r"""End-to-end rehearsal of the V2 stage pipeline on a disposable database, fakes only.

RUNBOOK
    Opt in and run WITHOUT the offline I/O guard plugin (the module skips itself under the guard)::

        $env:STOCKS_V2_DB_TESTS = '1'
        backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_v2_pipeline_rehearsal_db.py -q -s

    The tests share ONE database and run in file order (later tests use the state earlier ones leave):
    the module-scoped fixtures build it step by step, so a failing step fails every later test.

    Database   test_realtime_v2_e2e_a (created from the sql_v2 baseline, dropped at the end).
    Roles      test_rt_e2e_<6 hex>, created by the hardening in Part B only. Roles are cluster-global:
               they are dropped (and verified absent) in a finally, AFTER the database is dropped. All of
               them are NOLOGIN; nothing here creates a LOGIN role or a credential.
    Network    none. The reference service and the equity source are fakes (rehearsal_support.py).
    Killed run  If the process was killed before its finally blocks ran, drop the database with
               v2_database.drop_database_if_exists("test_realtime_v2_e2e_a") and the roles with
               security_v2.drop_disposable_roles_v2 (list them: SELECT rolname FROM pg_roles WHERE
               rolname LIKE 'test\_rt\_e2e\_%').

WALL CLOCK (read this before running)
    The rehearsal replays a session in REAL TIME: its bars are inserted now, after the session's final cutoff
    and inside the window deadline, and a bar's `created_at` is the database insert time (it cannot be set).
    The strategies read the window DEADLINE (their `available_by`) as the time the data is known and subtract
    the 15-minute provider delay themselves, so the session they resolve is a function of the wall clock:
    both the 1d and the 1h strategy resolve the replayed session's close only while the wall clock is between
    "15 minutes after a session close" and the next open. The session replayed is therefore the latest one that
    closed at least 15 minutes ago, and the module SKIPS (fixture `anchor`) when the next open is less than
    20 minutes away (US cash-session hours: weekdays about 13:30-20:15 UTC, an hour later in winter). Run it in the
    evening, at night or on a weekend/holiday; a run takes about 2 minutes.

PART A (deferred security: one owner pool, the runtime connects as the database owner)
    REAL-TIME WINDOWS OF THE REPLAYED SESSION S. Reference tick -> 13 history-bootstrap windows (the real
    EQUITY_INGEST and the bar drain; a window may emit at most 2,400 facts, so the 27.6k facts of two tickers
    need 14 windows) -> W1: the FINAL window of S (cutoff = the close, so the session-close paths run: kept
    snapshots, SESSION_CLOSE releases), released now with a 25 s deadline, through V2Service and the real
    StageOrchestrator (EQUITY_INGEST -> EQUITY_EVIDENCE [bar drain + real compute_realtime_ticker_cycle +
    screening generation] -> PORTAL_PUBLISH / STOCK_ALERTS / STRATEGY_PUBLISH). The history is fetched, observed
    and inserted after the cutoff and inside the deadline, as in production (the first minutes after the close
    plus the provider delay), so the two-clock loader (cutoff bounds bar_end, known_by = deadline bounds
    observed_at / created_at / replay / published) is what makes the bars visible to their own window. The
    strategies run at the deadline and PUBLISH both intervals, evaluated (COMPLETE / COMPLETE_EMPTY).
    -> W2 (cutoff = close + 1 min, 12 s budget): restated 1m bars fetched after W1's deadline, the delta-loader
    path -> W3 (+2 min): restated bars fetched after W2's deadline (invisible to W2 for ever, new to W3), a crash
    of EQUITY_EVIDENCE after its work (a BaseException kills the lane thread) and the retry -> W3b (+3 min):
    restated bars recorded after W3's load but inside W3's deadline (a late arrival: the loader must fall back
    to a full reload, VISIBLE_SET_DRIFT, publishing the identity of a fresh full load). W2..W4 are not on the
    production schedule (the final window of a session has cutoff = close): they are restatement re-runs whose
    cutoffs follow the close; the strategies find the retained snapshots of W1's market time (ALREADY_STORED,
    validated, nothing new published) in every one of them.

PART B (hardened security, roles simulated with SET ROLE from the admin connection)
    harden_database_v2 creates the NOLOGIN admin, app and api roles and grants and the exact-privilege audit
    must end AUDITED. The runtime then uses one pool per role (app, api; no owner pool) whose connections
    are the admin connection with `SET ROLE <role>` applied at checkout (and RESET ROLE on release), so
    V2ConnectionProvider's per-transaction `SET LOCAL ROLE <role>` runs inside it as in
    production. A new REFERENCE seed (generation 2), a new ingest of restated bars and one full window (W4,
    +4 min) then run as `app`, including the reader's default `current` mode through the `api`
    role, the screening repository and the kept tier (strategies, session-close releases); forbidden
    writes are rejected by PostgreSQL (an UPDATE of an append-only table by its trigger, a DELETE, a read of
    an owner-only table and DDL by missing privileges, and any write as api). W4's strategies only READ
    (ALREADY_STORED, same market time as W1), so the session-close writes (kept-only, hot-and-kept,
    SESSION_CLOSE release) are probed as the hardened `app` role with probe snapshots and rolled back; W1 took
    them for real under the owner pool.

TEST-SIDE SIMULATIONS (all that remain; each one is marked where it happens)
    * Time. The orchestrator Clock is a settable value: a window runs at its release time + 1 s (real now), and
      the two REFERENCE ticks run at a past logical instant (the refresh that preceded the windows it serves: a
      seed counts for a cutoff when it was created before it). Short deadlines are waited out in real time
      (`wait_until`) so that rows inserted afterwards are late arrivals; the orchestrator's lane grace is
      long, so the short recording budget is not also the lane budget. No other clock is pinned or patched:
      bars are observed and inserted at real time, before every `datetime.now()` the producers read.
    * The control plane (cohort, manifest, capability profile, securities) is inserted with created_at /
      effective_at one hour before the open: it exists before the session, as in production.
    * History bootstrap. No stage bootstraps a history (finding F4), so the 13 bootstrap windows run the real
      EQUITY_INGEST plus the bar drain of EQUITY_EVIDENCE only (its projection needs the whole history).
    * Windows after the final one (W2..W4) have cutoffs after the close: they stand for restatement re-runs.
    * The roles: PostgreSQL lets a superuser session user SET ROLE to any role, so the simulation proves the
      grants (the server enforces them for the role in force) but not a LOGIN credential; admin, app and api
      share no membership, which is verified with pg_has_role.
    * `created_at` of a bar is the database insert time and cannot be set: a deadline is "passed" for it only by
      real time passing, which is why W1 and W2 have short budgets.
    * STOCK_ALERTS has no context / forward / replay producer: every window reports exactly those three
      MISSING_PRODUCER codes (asserted), the only degraded codes of the whole run.

NOT EXERCISED: provider calls (none exist here), the options stages, a LOGIN role, the connection-pool
    budget of the real deployment, a strategy FIRST publication as the hardened app role (probed in b5
    instead), a second session (the retained-history input of the rolling strategies is empty) and the dashboard
    publication (no stage publishes it).
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections import Counter, deque, namedtuple
from contextlib import closing, contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
import psycopg2.errors
from psycopg2 import sql
from fastapi.testclient import TestClient

import exchange_calendars
import realtime.ticker_repository as ticker_repository
from equity.calendar import latest_expected_market_time
from equity.domain import BarSourceKind
from realtime.advanced_equity_journal_bridge import (
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.advanced_scheduler import (
    ADVANCED_WINDOW_BUILD_ID,
    discover_advanced_window_recovery_cursor,
)
from realtime.advanced_gateway import AdvancedGateway, GatewayConfiguration
from realtime.bridge import drain_raw_buffer_once
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import (
    AssetClass,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_json,
    canonical_sha256,
)
from realtime.database_access import (
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
)
from realtime.database_access_v2 import (
    APP_ROLE,
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
    verify_pool_target_v2,
)
from realtime.equity_bar_drain import drain_equity_bar_jobs, lease_owner_for
from realtime.equity_bar_adapter import (
    RealtimeEquityBarFact,
    adapt_derived_equity_bar,
)
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.equity_interval_finalizer import (
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
    enqueue_five_minute_finalizer_jobs_cursor,
    run_five_minute_finalizer_once,
)
from realtime.equity_worker_service import EQUITY_INTERVAL_DERIVE_JOB
from realtime.journal import RawJournalReader, RawJournalWriter
from realtime.lineage_head import (
    BOUNDED_MANIFEST_BYTES,
    BOUNDED_WORKSPACE_PAYLOAD_BYTES,
    inputs_sha256,
    reference_set,
)
from realtime.materialization_jobs import (
    BarrierMember,
    build_closure_materialization_job,
    enqueue_materialization_job_cursor,
    open_job_barrier_cursor,
)
from realtime.normalizer import (
    NORMALIZER_VERSION,
    InstrumentAdmission,
    normalize_raw_message,
)
from realtime.orchestration_repository import (
    CycleReceiptSpec,
    PostgresOrchestrationRepository,
    WindowSpec,
)
from realtime.portal_compatibility_repository import RealtimeScreeningRepository, load_session_close_releases
from realtime.portal_api import build_realtime_portal_repositories, create_realtime_portal_app
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE,
    RETENTION_HOT_AND_KEPT,
    RETENTION_KEPT,
    PortalReleaseReceipt,
    persist_portal_snapshot_cursor,
    publish_portal_release_cursor,
)
from realtime.preflight import ProviderTier
from realtime.relation_registry import ROLES
from realtime.runner_environment import CycleHandoff, RunnerEnvironment
from realtime.screening_delta_loader import ScreeningInputLoader
from realtime.screening_projection import RealtimeScreeningInput
from realtime.screening_repository import load_realtime_screening_input
from realtime.security_v2 import (
    build_role_names_v2,
    current_security_phase_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.stage_database_v2 import StageProcessPlan, V2StageDatabase, V2WorkerDatabase
from realtime.stage_graph import STAGES_V2, STAGES_V3
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome
from realtime.stage_runners_alerts import EVALUATED_ANALYSIS_STATUSES
from realtime.stage_runners_equity import (
    FINALIZER_POLICY_SHA256,
    INPUT_READY,
    PORTAL_READY,
    input_ready_identity,
    reuse_advanced_input_ready_runner,
)
from realtime.stage_runners_v2 import build_runners
from realtime.stock_alert_projection import STOCK_ALERTS_VERSION
from realtime.stock_alert_repository import RealtimeStockAlertViewRepository, StockAlertQueryRejected
from realtime.strategy_projection import HOURLY_ROLLING_VERSION, ROLLING_VERSION, STRATEGY_KEY
from realtime.strategy_repository import RealtimeDailyStrategyRepository
from realtime.ticker_projection import (
    PATTERN_WATCH_PUBLICATION_VERSION,
    TICKER_WORKSPACE_SUMMARY_VERSION,
    TICKER_WORKSPACE_VERSION,
    TickerProjectionState,
    _chart_point,
    project_realtime_ticker_workspace,
)
from realtime.ticker_references import compose_evidence
from realtime.ticker_repository import (
    RealtimeTickerWorkspaceRepository, compute_realtime_ticker_cycle, publish_realtime_ticker_cycle,
)
from realtime.v2_service import V2Service
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime import rehearsal_support as support
from tests.realtime.test_ticker_vertical_slice import old_embedded_evidence
from v2_database import _connect, _settings, disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

UTC = timezone.utc
DATABASE = "test_realtime_v2_e2e_a"
ROLE_PREFIX = "test_rt_e2e_a"
PRESERVE_DATABASE = os.getenv("STOCKS_V2_PRESERVE_DATABASE") == "1"
TICKERS = support.FIRST_TICKERS
UNIVERSE = UniverseManifest("rehearsal-v1", TICKERS, ())
ENABLED = frozenset({"REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STRATEGY_PUBLISH",
                     "STOCK_ALERTS"})
WINDOW_STAGES = ("EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STOCK_ALERTS", "STRATEGY_PUBLISH")
Used = namedtuple("Used", "stage window read_only fenced")


class SimulatedCrash(BaseException):
    """Escapes the orchestrator's `except Exception`, so the lane thread dies the way a killed process would."""


class Clock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


# The (window key, stage) the current thread is running, and the wall time of the named phases inside the stages.
ACTIVE = threading.local()
PHASES: dict[tuple, float] = {}


def timed_phases(patch) -> None:
    """Wrap the heavy calls of the runners so the report can split a stage's time into its phases."""
    import realtime.stage_runners_alerts as alert_runners
    import realtime.stage_runners_equity as equity_runners
    import realtime.stage_runners_ingest as ingest_runners

    for module, name in ((equity_runners, "drain_equity_bar_jobs"), (equity_runners, "compute_realtime_ticker_cycle"),
                         (equity_runners, "publish_realtime_portal_cycle_atomic"),
                         (alert_runners, "compute_realtime_stock_alerts"),
                         (alert_runners, "publish_realtime_stock_alerts"),
                         (alert_runners, "materialize_due_realtime_strategies"),
                         (ingest_runners, "acquire_equity_bars")):
        original = getattr(module, name)

        def wrapper(*args, __original=original, __name=name, **options):
            started = time.perf_counter()
            try:
                return __original(*args, **options)
            finally:
                where = getattr(ACTIVE, "stage", None)
                if where is not None:
                    key = (where[0], __name)
                    PHASES[key] = PHASES.get(key, 0.0) + time.perf_counter() - started

        patch.setattr(module, name, wrapper)


# --- fixtures: roles are created first so they are dropped last ------------------------------------------------------

def _drop_roles(names) -> list[str]:
    with closing(_connect(_settings(), "postgres")) as connection:
        connection.autocommit = True
        drop_disposable_roles_v2(connection, names)
        with connection.cursor() as cursor:
            cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (list(names.all_names),))
            return [row[0] for row in cursor.fetchall()]


@pytest.fixture(scope="module")
def roles():
    names = build_role_names_v2(ROLE_PREFIX)
    try:
        yield names
    finally:
        if not PRESERVE_DATABASE:
            remaining = _drop_roles(names)  # the database fixture is torn down first, so nothing depends on them
            assert remaining == [], f"disposable roles were left behind: {remaining}"


@pytest.fixture(scope="module")
def pool(roles, anchor):
    with disposable_v2_database(DATABASE, preserve=PRESERVE_DATABASE) as connection_pool:
        yield connection_pool


# --- the rehearsal world ----------------------------------------------------------------------------------------------

class Runtime:
    """One orchestrator process: access provider, orchestration repository, runner environment, and the
    instrumentation the assertions use (transactions per stage, wall time per stage, injected crashes)."""

    def __init__(self, label: str, provider: V2ConnectionProvider, rehearsal: "Rehearsal") -> None:
        self.label, self.provider, self.rehearsal = label, provider, rehearsal
        self.repository = PostgresOrchestrationRepository(provider)
        self.clock = Clock(rehearsal.closed - timedelta(hours=1))
        self.environment = RunnerEnvironment(
            provider, build_id=f"rehearsal-{label}", reference_service=rehearsal.reference_service,
            equity_source=rehearsal.source, universe=UNIVERSE)
        self.transactions: list[Used] = []
        self.timings: dict[tuple, float] = {}
        self.crash_after: set[tuple] = set()
        self.crash_before: set[tuple] = set()
        self.watermarks_before_drain: dict[str, list] = {}
        self.loads: dict[tuple, object] = {}
        original = provider.transaction

        @contextmanager
        def recording(*args, **options):
            where = getattr(ACTIVE, "stage", None) or (None, None)
            with original(*args, **options) as unit:
                self.transactions.append(Used(where[1], where[0], bool(options.get("read_only")),
                                              options.get("fence") is not None))
                yield unit

        provider.transaction = recording
        base = build_runners(ENABLED, provider, environment=self.environment)
        self.runners = {stage: self._instrument(stage, runner) for stage, runner in base.items()}
        self.runners["EQUITY_EVIDENCE"] = self._instrument("EQUITY_EVIDENCE", self._crashable(
            "EQUITY_EVIDENCE", self._watching(base["EQUITY_EVIDENCE"])))
        self.runners["PORTAL_PUBLISH"] = self._instrument("PORTAL_PUBLISH", self._crashable(
            "PORTAL_PUBLISH", self._dying_first("PORTAL_PUBLISH", base["PORTAL_PUBLISH"])))
        # The history-bootstrap windows run the bar drain of EQUITY_EVIDENCE only: its projection needs the whole
        # history, which the bootstrap is still collecting (finding F4: no stage bootstraps a history).
        self.bootstrap_evidence = self._instrument("EQUITY_EVIDENCE", self._bootstrap_drain())

    # -- instrumentation
    def _instrument(self, stage, runner):
        def run(context):
            ACTIVE.stage = (context.window.window_key, stage)
            started = time.perf_counter()
            try:
                return runner(context)
            finally:
                self.timings[(context.window.window_key, stage, context.attempt)] = time.perf_counter() - started
                ACTIVE.stage = None

        return run

    def _crashable(self, stage, runner):
        def run(context):
            outcome = runner(context)  # all the stage's work is done and committed; only the finish is lost
            if (context.window.window_key, stage, context.attempt) in self.crash_after:
                raise SimulatedCrash(f"{stage} died after its work")
            return outcome

        return run

    def _dying_first(self, stage, runner):
        def run(context):
            if (context.window.window_key, stage, context.attempt) in self.crash_before:
                raise SimulatedCrash(f"{stage} died before its work")  # the stage's input stage committed
            return runner(context)

        return run

    def restart(self) -> None:
        """What a new process starts with: no hand-off, no projection state, no delta loader. The database (and
        so every receipt, evidence row and release) is all that survives."""
        self.environment.handoff = CycleHandoff()
        self.environment.ticker_state = TickerProjectionState()
        self.environment.screening_loader = ScreeningInputLoader()

    def _watching(self, runner):
        def run(context):
            # What the ingest left behind before the evidence stage drains it, and what the loader reports
            # after the stage's compute (the report is per process, so it is read right after each attempt).
            self.watermarks_before_drain[context.window.window_key] = self.rehearsal.watermarks()
            try:
                return runner(context)
            finally:
                self.loads[(context.window.window_key, context.attempt)] = self.environment.screening_loader.last_report

        return run

    def _bootstrap_drain(self):
        def run(context):
            compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
            result = drain_equity_bar_jobs(
                compute, cohort_id=context.plan.cohort_id, lease_owner=lease_owner_for(context.stage_run_id),
                should_stop=context.is_cancelled)
            assert not result.unsuccessful, result
            return StageOutcome.complete(degraded=result.degraded_reasons())

        return run

    # -- driving
    def orchestrator(self, table=None, **options) -> StageOrchestrator:
        runners = {stage: (lambda context: StageOutcome.complete()) for stage in ENABLED}
        runners.update(table if table is not None else self.runners)
        return StageOrchestrator(self.repository, runners, database=self.provider, clock=self.clock,
                                 max_lane_seconds=1_500.0, lane_grace_seconds=1_200.0, **options)

    def tick_reference(self, at):
        """One REFERENCE refresh whose seed is created at the logical instant `at` (a past time: the refresh
        that preceded the windows it serves)."""
        self.clock.value = at
        service = V2Service(self.orchestrator(), self.repository, self.rehearsal.plan_id, ())
        return service.tick_resident_once("REFERENCE")

    def run_ingest_window(self, spec):
        """A history-bootstrap window: the real EQUITY_INGEST and the bar drain; every other stage a no-op."""
        record = self.repository.ensure_window(self.rehearsal.plan_id, spec)
        self.clock.value = spec.release_at + timedelta(seconds=1)
        report = self.orchestrator({"EQUITY_INGEST": self.runners["EQUITY_INGEST"],
                                    "EQUITY_EVIDENCE": self.bootstrap_evidence}).run_window(record.window_id)
        return record, report

    def run_window(self, spec, *, stub=(), **options):
        """The real window through V2Service.poll_windows with the real runner table (`stub` stages are no-ops;
        `options` go to the StageOrchestrator, e.g. max_attempts)."""
        self.clock.value = spec.release_at + timedelta(seconds=1)
        table = {stage: runner for stage, runner in self.runners.items() if stage not in stub}
        service = V2Service(self.orchestrator(table, **options), self.repository, self.rehearsal.plan_id, (spec,),
                            clock=self.clock)
        started = time.perf_counter()
        report = service.poll_windows()
        return report, time.perf_counter() - started

    def stage_times(self, window_key):
        return {stage: round(seconds, 2) for (key, stage, attempt), seconds in sorted(self.timings.items())
                if key == window_key}

    def stages_used(self, window_key=None, *, write=None):
        """The stages that opened a transaction (of the given kind) through the access layer."""
        return {used.stage for used in self.transactions
                if used.stage is not None and (window_key is None or used.window == window_key)
                and (write is None or write == (not used.read_only))}


PROVIDER_DELAY = timedelta(minutes=15)
RUN_HORIZON = timedelta(minutes=20)  # the whole rehearsal (~3 minutes) plus the budgets of its last windows


@pytest.fixture(scope="module")
def anchor():
    """The session the rehearsal replays: the latest one that closed at least the provider delay ago, provided the
    wall clock is still before the next session's open for the whole run (see "WALL CLOCK" in the module docs)."""
    now = datetime.now(UTC)
    calendar = exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(str(now.date()), direction="previous")
    while calendar.session_close(session).to_pydatetime() + PROVIDER_DELAY > now:
        session = calendar.previous_session(session)
    following_open = calendar.session_open(calendar.next_session(session)).to_pydatetime()
    if now + RUN_HORIZON >= following_open:
        pytest.skip(f"the rehearsal runs in real time after a session close: it needs the wall clock between "
                    f"{PROVIDER_DELAY} after a close and {RUN_HORIZON} before the next open "
                    f"(now {now:%Y-%m-%d %H:%MZ}, next open {following_open:%Y-%m-%d %H:%MZ})")
    return session.date()


class Rehearsal:
    def __init__(self, pool, session: date) -> None:
        # REAL-TIME ANCHOR. The session replayed is the latest one that closed at least the provider delay ago
        # (see `anchor`), and its FINAL window (cutoff = the close) is W1: released now, the bars are fetched,
        # observed and inserted NOW, after the cutoff (close + provider delay) and before the window deadline,
        # which is what the two-clock loader (cutoff for bar_end, known_by = deadline for the recording clocks)
        # is for. The strategies read the deadline as the time the data is known (they subtract the provider
        # delay themselves), so they resolve this very session at every window of the run.
        self.started = datetime.now(UTC)
        self.session = session
        self.opened, self.closed = support.session_bounds(self.session)
        horizon = self.started + RUN_HORIZON - PROVIDER_DELAY
        assert latest_expected_market_time(horizon, "1d") == latest_expected_market_time(horizon, "1h") == self.closed
        self.pool = pool
        for day in {self.started.date(), (self.started + RUN_HORIZON).date(), self.session}:
            self.admin("SELECT rt.ensure_partitions_all(%s)", (day,))
        self.cohort, self.plan_id, self.security_ids, self.instrument_ids = self._seed_control_plane()
        started = time.perf_counter()
        self.history = support.build_history(
            self.session, TICKERS, cohort_id=self.cohort, instrument_ids=self.instrument_ids,
            fetched_at=self.started)
        self.history_seconds = time.perf_counter() - started
        self.chunks = {ticker: support.chunked_batches(self.history, ticker, segment_prefix="rehearsal-history")
                       for ticker in TICKERS}
        assert len({len(chunks) for chunks in self.chunks.values()}) == 1
        self.source = support.ChunkSource()
        self.reference_service = support.RehearsalReferenceService(
            TICKERS, self.session, facts_base=self.closed - timedelta(minutes=40),
            facts_valid_until=self.closed + timedelta(hours=6))
        provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True))
        provider.verify()
        self.deferred = Runtime("deferred", provider, self)

    # -- admin access (the owner pool, outside the provider: test fixture work and read-only checks)
    @contextmanager
    def cursor(self):
        connection = self.pool.getconn()
        try:
            with connection.cursor() as cursor:
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self.pool.putconn(connection)

    def admin(self, statement, parameters=()):
        with self.cursor() as cursor:
            cursor.execute(statement, parameters)

    def execute(self, statement, parameters=()):
        with self.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.rowcount

    def q(self, statement, parameters=()):
        with self.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.fetchall()

    def scalar(self, statement, parameters=()):
        return self.q(statement, parameters)[0][0]

    def watermarks(self):
        return self.q("SELECT instrument_id, interval, source_kind, last_complete_bar_end, terminal_state "
                      "FROM rt.acquisition_watermarks WHERE cohort_id = %s ORDER BY 1, 2, 3", (str(self.cohort),))

    # -- seed
    def _seed_control_plane(self):
        profile, manifest, cohort, plan = uuid4(), uuid4(), uuid4(), uuid4()
        digest = lambda: uuid4().hex + uuid4().hex  # noqa: E731
        security_ids, instrument_ids = {}, {}
        # The control plane exists before the session (as in production): the cohort, its manifest and profile
        # were created, and became effective, an hour before the open, so every window cutoff comes after them.
        born = self.opened - timedelta(hours=1)
        with self.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
                "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, "
                "provider_tier, security_policy_sha256, preflight_mode, provider_requests, action_authorized, "
                "created_at) VALUES (%s,%s,%s,'rehearsal',%s,%s,%s,%s,'PASS',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',"
                "false,false,%s)",
                (str(profile), digest(), digest(), digest(), digest(), born, born + timedelta(days=30), digest(),
                 digest(), born))
            cursor.execute(
                "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, "
                "session_date, effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, "
                "member_count, allocation_policy_sha256, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',%s,0,%s,%s,%s)",
                (str(manifest), digest(), digest(), self.session, born, str(profile), len(TICKERS), len(TICKERS),
                 digest(), born))
            for priority, ticker in enumerate(TICKERS, start=1):
                security = support._security(ticker).security_id
                security_ids[ticker] = security
                cursor.execute("INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                               "VALUES (%s, 'STOCK', 'rehearsal', %s)", (str(security), born))
                cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                               "RETURNING instrument_id", (str(security),))
                instrument_ids[ticker] = cursor.fetchone()[0]
                cursor.execute("INSERT INTO rt.subscription_members (manifest_id, instrument_id, channel, priority, "
                               "purpose, option_underlying) VALUES (%s, %s, 'AM', %s, 'PRIMARY', false)",
                               (str(manifest), instrument_ids[ticker], priority))
            cursor.execute(
                "INSERT INTO rt.cohorts (cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
                "model_bundle_sha256, valuation_sha256, effective_at, mode, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW',%s)",
                (str(cohort), digest(), str(manifest), digest(), digest(), digest(), born, born))
            cursor.execute(
                "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, "
                "enabled_stages, config_sha256, deadline, scope_reference) "
                "VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,'rehearsal scope')",
                (str(plan), str(cohort), self.session, sorted(ENABLED), digest(),
                 self.started + timedelta(hours=12)))
        return cohort, plan, security_ids, instrument_ids

    # -- windows
    def spec(self, key: str, cutoff: datetime, *, budget: timedelta = timedelta(minutes=10)) -> WindowSpec:
        """A catch-up window: the market cutoff is in the past, the window is released NOW and may record bars
        until its deadline, `budget` after the release (the loader's known_by; the strategies' available_by)."""
        release = datetime.now(UTC)
        assert cutoff <= release
        return WindowSpec(self.cohort, "MASSIVE_DEVELOPER", key, release, cutoff, release + budget)

    @staticmethod
    def wait_until(moment: datetime) -> None:
        """Real time passing: after this a row inserted by the database is later than `moment`."""
        remaining = (moment - datetime.now(UTC)).total_seconds()
        if remaining > 0:
            time.sleep(remaining + 0.25)

    def cutoff(self, minutes_after_close: float) -> datetime:
        return self.closed + timedelta(minutes=minutes_after_close)

    @property
    def c1(self):
        """W1's cutoff: the session close, so W1 is the FINAL window of the session."""
        return self.closed

    def ready_seeds(self, cutoff=None):
        if cutoff is None:
            return self.scalar("SELECT count(*) FROM rt.seed_manifests WHERE status = 'READY'")
        return self.scalar("SELECT count(*) FROM rt.seed_manifests WHERE status = 'READY' AND created_at <= %s",
                           (cutoff,))

    def counts(self):
        tables = ("rt.bar_catalog", "rt.bar_revisions", "rt.bar_current", "rt.evidence_catalog",
                  "rt.equity_evidence", "rt.snapshot_catalog", "rt.portal_releases", "rt.cycle_receipts",
                  "rt.materialization_jobs", "rt.acquisition_batches", "rt.screening_generations")
        return {table: self.scalar(f"SELECT count(*) FROM {table} WHERE cohort_id = %s", (str(self.cohort),))
                for table in tables}

    def stage_rows(self, window_key):
        return [(row[0], row[1], row[2], tuple(row[3])) for row in self.q(
            "SELECT run.stage_id, run.attempt, run.state, run.reason_codes FROM rt.stage_runs AS run "
            "JOIN rt.window_runs AS window_run USING (window_id) WHERE window_run.cohort_id = %s "
            "AND window_run.window_key = %s ORDER BY run.stage_id, run.attempt", (str(self.cohort), window_key))]

    def window_state(self, window_key):
        return self.scalar("SELECT state FROM rt.window_runs WHERE cohort_id = %s AND window_key = %s",
                           (str(self.cohort), window_key))


@pytest.fixture(scope="module")
def rehearsal(pool, anchor):
    with pytest.MonkeyPatch.context() as patch:
        timed_phases(patch)
        yield Rehearsal(pool, anchor)


@dataclass
class Outcome:
    spec: WindowSpec
    report: object
    seconds: float
    extra: dict = field(default_factory=dict)


@pytest.fixture(scope="module")
def reference_tick(rehearsal):
    """The refresh that preceded the session's windows: its seed is created (logical clock) 30 minutes before
    the close, so every window cutoff counts it."""
    return rehearsal.deferred.tick_reference(rehearsal.closed - timedelta(minutes=30))


@pytest.fixture(scope="module")
def backfill(rehearsal, reference_tick):
    """Windows 0..n-2: EQUITY_INGEST of every history chunk but the newest one (W1 ingests that one), each
    followed by the bar drain, so the history's bars exist before W1."""
    runtime = rehearsal.deferred
    assert reference_tick == "COMPLETE"
    count = len(rehearsal.chunks[TICKERS[0]]) - 1
    results = []
    started = time.perf_counter()
    for index in range(count):
        rehearsal.source.queue(f"history-{index}", {ticker: rehearsal.chunks[ticker][index] for ticker in TICKERS})
        spec = rehearsal.spec(f"backfill-{index:02d}", rehearsal.closed - timedelta(hours=8) + timedelta(minutes=15 * index))
        record, report = runtime.run_ingest_window(spec)
        results.append((spec, report))
    return Outcome(None, results, time.perf_counter() - started)


def strategy_status(rehearsal):
    return rehearsal.scalar("SELECT evidence_ref FROM rt.stage_readiness_current WHERE cohort_id = %s "
                            "AND stage_id = 'STRATEGY_PUBLISH'", (str(rehearsal.cohort),))


@pytest.fixture(scope="module")
def w1(rehearsal, backfill):
    """W1: the newest history chunk is fetched after the window's cutoff and inside its deadline (a 25 s budget:
    the drain and the compute of the chunk take a few seconds), so the bars are visible to W1 itself. The fixture
    returns only after the deadline has passed in real time: everything inserted later is a late arrival."""
    runtime = rehearsal.deferred
    last = len(rehearsal.chunks[TICKERS[0]]) - 1
    rehearsal.source.queue("history-last", {ticker: rehearsal.chunks[ticker][last] for ticker in TICKERS})
    spec = rehearsal.spec("w1", rehearsal.c1, budget=timedelta(seconds=25))
    report, seconds = runtime.run_window(spec)
    outcome = Outcome(spec, report, seconds, extra=dict(
        counts=rehearsal.counts(), load=runtime.loads.get(("w1", 1)), strategy=strategy_status(rehearsal)))
    rehearsal.wait_until(spec.deadline)
    return outcome


# --- PART A -----------------------------------------------------------------------------------------------------------

def test_a1_reference_ticks_a_ready_seed_and_the_history_backfills_through_the_ingest_windows(rehearsal, backfill):
    assert rehearsal.ready_seeds() == 1
    assert rehearsal.scalar("SELECT generation FROM rt.seed_current WHERE scope = 'MARKET_REFERENCES'") == 1
    assert rehearsal.deferred.repository.readiness(rehearsal.cohort)["REFERENCE"] == "READY"
    assert len(backfill.report) == len(rehearsal.chunks[TICKERS[0]]) - 1 == 13
    for spec, report in backfill.report:
        assert (report.status, report.window_state) == ("FINISHED", "COMPLETE"), spec.window_key
    # Every chunk landed: batches, facts, ledger requests (all COMPLETE) and watermarks, and the bootstrap drain
    # turned each batch into bars (26 of the 28 jobs; W1 ingests and drains the newest chunk itself).
    cohort = str(rehearsal.cohort)
    assert rehearsal.scalar("SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id = %s", (cohort,)) == 26
    assert rehearsal.scalar("SELECT count(*) FROM rt.provider_request_ledger WHERE cohort_id = %s AND "
                            "completeness = 'COMPLETE'", (cohort,)) == 26
    bootstrapped = sum(len(rehearsal.chunks[ticker][index]) for ticker in TICKERS for index in range(13))
    assert rehearsal.scalar("SELECT count(*) FROM rt.bar_revisions WHERE cohort_id = %s", (cohort,)) == bootstrapped
    assert rehearsal.q("SELECT status, count(*) FROM rt.materialization_jobs WHERE cohort_id = %s GROUP BY 1",
                       (cohort,)) == [("COMPLETE", 26)]
    assert rehearsal.watermarks()


def test_a2_the_real_window_runs_through_v2_service_with_one_receipt_and_the_ready_seed_generation(rehearsal, w1):
    runtime = rehearsal.deferred
    print("\nW1 stage seconds", runtime.stage_times("w1"), "total", round(w1.seconds, 1))
    print("W1 phases", {name: round(seconds, 1) for (key, name), seconds in PHASES.items() if key == "w1"})
    print("W1 rows", rehearsal.stage_rows("w1"))
    assert w1.report.window_errors == {}
    (report,) = w1.report.windows
    states = {stage: (state, codes) for stage, _, state, codes in rehearsal.stage_rows("w1")}
    # The whole window COMPLETEs, every stage included. The only degraded codes are what the producers say they
    # did not produce (STOCK_ALERTS has no forward/replay/context producer yet): in particular the strategy stage
    # reports no STRATEGY_*_FAILED and no unevaluated analysis.
    assert report.window_state == "COMPLETE", (states, report)
    assert {stage: state for stage, (state, _) in states.items()} == {stage: "COMPLETE" for stage in WINDOW_STAGES}
    assert {stage: codes for stage, (state, codes) in states.items() if codes} == {
        "STOCK_ALERTS": ("MISSING_PRODUCER:context", "MISSING_PRODUCER:forward", "MISSING_PRODUCER:replay")}
    receipts = rehearsal.q("SELECT receipt_kind, reference_generation, cutoff FROM rt.cycle_receipts "
                           "WHERE window_id = %s", (str(report.window_id),))
    assert receipts == [("EQUITY_CYCLE", 1, rehearsal.c1)]
    assert rehearsal.ready_seeds(rehearsal.c1) == receipts[0][1]
    assert rehearsal.window_state("w1") == "COMPLETE"


def strategy_snapshots(rehearsal):
    """{version: (payload, source_manifest, snapshot_class, session_date, generated_at)} of the strategy snapshots
    stored for the cohort (one per interval so far)."""
    rows = rehearsal.q(
        "SELECT catalog.snapshot_type, kept.payload, kept.source_manifest, catalog.snapshot_class, "
        "catalog.session_date, catalog.generated_at FROM rt.snapshot_catalog AS catalog "
        "JOIN rt.portal_snapshot_kept AS kept ON kept.snapshot_id = catalog.snapshot_id "
        "AND kept.cohort_id = catalog.cohort_id WHERE catalog.cohort_id = %s AND catalog.snapshot_key = %s "
        "ORDER BY catalog.generated_at", (str(rehearsal.cohort), STRATEGY_KEY))
    return rows


def test_a2b_strategy_publication_evaluates_the_session_in_both_intervals_and_keeps_its_close(rehearsal, w1):
    """The strategies resolve their market time from the window DEADLINE (available_by - 15 min): the session is
    the replayed one, the final window's close. Both intervals publish and evaluate every member."""
    assert w1.extra["strategy"] == "1d=PUBLISHED;1h=PUBLISHED"
    rows = strategy_snapshots(rehearsal)
    assert {row[0] for row in rows} == {ROLLING_VERSION, HOURLY_ROLLING_VERSION}, [row[0] for row in rows]
    for version, payload, manifest, snapshot_class, session, generated_at in rows:
        assert (snapshot_class, session) == ("SESSION_CLOSE", rehearsal.session)
        assert payload["market_time"] == rehearsal.closed.isoformat() and payload["session"] == str(rehearsal.session)
        assert w1.spec.cutoff <= generated_at <= w1.spec.deadline  # produced at the deadline: the data was known
        analysis = payload["analysis"]
        assert analysis and {item["status"] for item in analysis.values()} <= EVALUATED_ANALYSIS_STATUSES, (
            version, {name: item["status"] for name, item in analysis.items()})

# --- helpers for the data assertions ---------------------------------------------------------------------------------

def current_release(rehearsal):
    """(release_id, class, receipt_id, cutoff, {(type, key): entry}) of the release rt.portal_release_current
    points at. Every publishing stage of a window publishes its own release for the window's receipt and the
    intraday ones carry the other lanes' entries forward, so the pointed-at release names all of them."""
    rows = rehearsal.q(
        "SELECT release.release_id, release.release_class, release.receipt_id, release.cutoff, release.manifest "
        "FROM rt.portal_release_current AS pointer JOIN rt.portal_releases AS release "
        "ON release.release_id = pointer.release_id AND release.cutoff = pointer.cutoff "
        "WHERE pointer.cohort_id = %s", (str(rehearsal.cohort),))
    assert len(rows) == 1, rows
    release_id, release_class, receipt_id, cutoff, manifest = rows[0]
    return str(release_id), release_class, str(receipt_id), cutoff, {
        (entry["snapshot_type"], entry["snapshot_key"]): entry for entry in manifest["snapshots"]}


def workspace_payloads(rehearsal):
    """{ticker: (snapshot_id, payload, source_manifest, manifest_text_bytes)} of the current release."""
    entries = current_release(rehearsal)[4]
    result = {}
    for ticker in TICKERS:
        snapshot_id = entries[(TICKER_WORKSPACE_VERSION, ticker)]["snapshot_id"]
        (row,) = rehearsal.q(
            "SELECT payload, source_manifest, octet_length(source_manifest::text) FROM rt.portal_snapshot_hot "
            "WHERE cohort_id = %s AND snapshot_id = %s", (str(rehearsal.cohort), snapshot_id))
        result[ticker] = (snapshot_id, *row)
    return result


def reader_for(runtime):
    @contextmanager
    def cursor_factory():
        with runtime.provider.transaction("api", read_only=True, dict_cursor=True,
                                          isolation_level="REPEATABLE READ") as unit:
            yield unit.cursor

    return RealtimeTickerWorkspaceRepository(cursor_factory, transaction_configured=True), cursor_factory


def expected_chart(rehearsal, ticker, chart_ref, revised=()):
    """The chart points of `ticker`'s `<interval>:<basis>` group from the ingested bar facts themselves (the
    producer's own point arithmetic); a revised bar replaces the stored bar with its bar identity."""
    interval, _, basis = chart_ref.partition(":")
    bars = {}
    for fact in (*rehearsal.history[ticker], *revised):
        bar = fact.revision
        if (bar.interval == interval and ("ADJUSTED" if bar.adjusted else "RAW_UNADJUSTED") == basis
                and bar.session_scope.value == "RTH" and bar.is_final):
            bars[bar.bar_start] = fact
    return [_chart_point(bars[start]) for start in sorted(bars)]


def as_stored_json(value):
    return json.loads(canonical_json(value))


def differing_paths(left, right, path="", limit=10):
    """The first paths at which two JSON values differ (a readable assertion message)."""
    found = []
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(set(left) | set(right), key=str):
            if key not in left or key not in right:
                found.append(f"{path}/{key}: only in {'right' if key not in left else 'left'}")
            else:
                found.extend(differing_paths(left[key], right[key], f"{path}/{key}", limit))
    elif isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        for index, (one, other) in enumerate(zip(left, right)):
            found.extend(differing_paths(one, other, f"{path}[{index}]", limit))
    elif left != right:
        found.append(f"{path}: {str(left)[:70]!r} != {str(right)[:70]!r}")
    return found[:limit]


# --- Part A, W1: what the window wrote --------------------------------------------------------------------------------

def test_a3_bars_jobs_and_watermarks_of_the_ingest_and_evidence_stages(rehearsal, w1):
    runtime, cohort = rehearsal.deferred, str(rehearsal.cohort)
    facts = [fact for ticker in TICKERS for fact in rehearsal.history[ticker]]
    counts = w1.extra["counts"]
    assert counts["rt.bar_catalog"] == counts["rt.bar_revisions"] == counts["rt.bar_current"] == len(facts) == 27_608
    assert rehearsal.q("SELECT provider_tier, count(*) FROM rt.bar_catalog WHERE cohort_id = %s GROUP BY 1",
                       (cohort,)) == [("MASSIVE_DEVELOPER", len(facts))]
    stored_ids = {str(row[0]) for row in rehearsal.q("SELECT bar_revision_id FROM rt.bar_revisions "
                                                        "WHERE cohort_id = %s", (cohort,))}
    assert stored_ids == {str(fact.revision.bar_revision_id) for fact in facts}
    # Identity and kind: natives stay native, derived stay derived with their sources, per interval and basis.
    ingested = Counter((fact.revision.ticker, fact.revision.interval, "ADJUSTED" if fact.revision.adjusted
                        else "RAW_UNADJUSTED", fact.revision.source_kind.value) for fact in facts)
    stored = Counter((row[0], row[1], row[2], row[3]) for row in rehearsal.q(
        "SELECT ticker, interval, adjustment_basis, source_kind FROM rt.bar_revisions WHERE cohort_id = %s",
        (cohort,)))
    assert stored == ingested and {key[3] for key in stored} == {"NATIVE_REST", "DERIVED"}
    derived = next(fact for fact in facts if fact.revision.source_kind.value == "DERIVED")
    assert rehearsal.scalar("SELECT source_revision_ids::text[] FROM rt.bar_revisions WHERE bar_revision_id = %s",
                            (str(derived.revision.bar_revision_id),)) == [
        str(value) for value in derived.revision.source_bar_revision_ids]
    # Jobs: one COMPLETE job per ingested batch (26 drained by the bootstrap windows, 2 by W1's evidence stage).
    assert rehearsal.q("SELECT status, count(*) FROM rt.materialization_jobs WHERE cohort_id = %s GROUP BY 1",
                       (cohort,)) == [("COMPLETE", 2 * len(rehearsal.chunks[TICKERS[0]]))]
    # Watermarks: written only by the ingest (complete requests), unchanged by the bar drain.
    assert runtime.watermarks_before_drain["w1"] == rehearsal.watermarks() and runtime.watermarks_before_drain["w1"]
    latest = {}
    for fact in facts:
        key = (fact.instrument_id, fact.revision.interval, fact.revision.source_kind.value)
        latest[key] = max(latest.get(key, fact.revision.bar_end), fact.revision.bar_end)
    assert {(row[0], row[1], row[2]): row[3] for row in rehearsal.watermarks()} == latest
    assert {row[4] for row in rehearsal.watermarks()} == {"COMPLETE"}


def oracle_inputs(runtime, rehearsal, spec, generation):
    """The screening input a FRESH full load builds for the window `spec` right now: market cutoff = the window's
    cutoff, recording clocks bounded by the window's deadline (the oracle the loader and the runner must match)."""
    with runtime.provider.transaction(read_only=True, dict_cursor=True) as unit:
        return load_realtime_screening_input(
            unit.cursor, cohort_id=rehearsal.cohort, session=rehearsal.session, cutoff=spec.cutoff,
            known_by=spec.deadline, source_generation=generation, include_portal_history=True)


def receipt_input_sha(rehearsal, spec):
    return rehearsal.scalar(
        "SELECT receipt.input_sha256 FROM rt.cycle_receipts AS receipt JOIN rt.window_runs AS window_run "
        "USING (window_id) WHERE window_run.cohort_id = %s AND window_run.window_key = %s",
        (str(rehearsal.cohort), spec.window_key))


def test_a3b_bars_fetched_after_the_cutoff_are_visible_to_their_own_window_and_appear_in_its_release(rehearsal, w1):
    runtime, cohort = rehearsal.deferred, str(rehearsal.cohort)
    spec = w1.spec
    # Every bar was observed by the provider AND inserted by the database after the window's market cutoff (the
    # old single-clock rule would have hidden all of them from the window): the cutoff bounds bar_end only.
    assert rehearsal.scalar("SELECT count(*) FROM rt.bar_revisions WHERE cohort_id = %s AND "
                            "(observed_at <= %s OR created_at <= %s OR bar_end > %s)",
                            (cohort, spec.cutoff, spec.cutoff, spec.cutoff)) == 0
    # W1's own chunk (newest bars, ending at the session close) was inserted inside W1's deadline.
    last = len(rehearsal.chunks[TICKERS[0]]) - 1
    ids = [str(fact.revision.bar_revision_id) for ticker in TICKERS for fact in rehearsal.chunks[ticker][last]]
    created = rehearsal.q("SELECT min(created_at), max(created_at), max(observed_at) FROM rt.bar_revisions "
                          "WHERE bar_revision_id = ANY(%s::uuid[])", (ids,))[0]
    assert spec.cutoff < created[0] <= created[1] <= spec.deadline and created[2] <= spec.deadline
    # Visible to W1's own compute: the loader read every one of the 27,608 bars under (cutoff, deadline) ...
    load = w1.extra["load"]
    assert (load.mode, load.reason, load.bar_rows_read, load.retained_bars) == ("full", "NO_STATE", 27_608, 27_608)
    # ... and the input the receipt names is exactly what a fresh full load under the same two clocks builds.
    assert inputs_sha256(oracle_inputs(runtime, rehearsal, spec, 1)) == receipt_input_sha(rehearsal, spec)
    # With the single old clock (known_by = cutoff) none of them would be visible: the window would be empty.
    with runtime.provider.transaction(read_only=True, dict_cursor=True) as unit:
        old_rule = load_realtime_screening_input(
            unit.cursor, cohort_id=rehearsal.cohort, session=rehearsal.session, cutoff=spec.cutoff,
            source_generation=1, include_portal_history=True)
    assert old_rule.bars == ()


def test_a4_evidence_snapshots_release_pointer_and_the_compact_hot_payload(rehearsal, w1):
    cohort = str(rehearsal.cohort)
    counts = w1.extra["counts"]
    assert counts["rt.evidence_catalog"] == counts["rt.equity_evidence"] > 0
    assert counts["rt.cycle_receipts"] == 1
    (receipt_id,) = [str(row[0]) for row in rehearsal.q("SELECT receipt_id FROM rt.cycle_receipts WHERE cohort_id = %s",
                                                         (cohort,))]
    # Every publishing stage (portal, alerts, strategies) published its releases for THE receipt of the window;
    # none for any other receipt. W1 is the session's final window, so the session-close releases exist too.
    releases = rehearsal.q("SELECT release_class, receipt_id, cutoff, session_date FROM rt.portal_releases "
                           "WHERE cohort_id = %s", (cohort,))
    assert {(str(row[1]), row[2]) for row in releases} == {(receipt_id, rehearsal.c1)}
    assert {row[3] for row in releases} == {rehearsal.session}
    assert {row[0] for row in releases} == {"INTRADAY", "SESSION_CLOSE"}
    release_id, release_class, release_receipt, cutoff, entries = current_release(rehearsal)
    assert (release_class, release_receipt, cutoff) == ("INTRADAY", receipt_id, rehearsal.c1)
    assert {key for key in entries if key[0] == TICKER_WORKSPACE_VERSION} == {
        (TICKER_WORKSPACE_VERSION, ticker) for ticker in TICKERS}
    # The pointed-at release names every lane of the window: workspaces, summary, patterns, alerts, both strategies.
    assert {key[0] for key in entries} == {
        TICKER_WORKSPACE_VERSION, TICKER_WORKSPACE_SUMMARY_VERSION, PATTERN_WATCH_PUBLICATION_VERSION,
        STOCK_ALERTS_VERSION, ROLLING_VERSION, HOURLY_ROLLING_VERSION}
    assert len(entries) == len(TICKERS) + 5
    # The stored workspace is the compact by-reference payload: far below the 128 KiB cap, no embedded
    # charts/evidence, a lineage-head manifest under 10 KB, and the 16 KiB database CHECK was never relaxed.
    for ticker, (snapshot_id, payload, manifest, manifest_bytes) in workspace_payloads(rehearsal).items():
        assert len(canonical_json(payload).encode()) < BOUNDED_WORKSPACE_PAYLOAD_BYTES, ticker
        assert "charts" not in payload and "evidence" not in payload
        assert set(payload["chart_refs"]) == set(payload["chart_market_times"]) == {
            "1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"}
        assert manifest_bytes < 10_000 and len(canonical_json(manifest).encode()) <= BOUNDED_MANIFEST_BYTES
    definitions = [row[0] for row in rehearsal.q(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'rt.portal_snapshot_hot'::regclass "
        "AND contype = 'c'")]
    assert any("16384" in definition and "source_manifest" in definition for definition in definitions), definitions
    assert BOUNDED_WORKSPACE_PAYLOAD_BYTES == 128 * 1024


def test_a4b_the_final_window_keeps_its_session_close_and_the_retention_readers_serve_it(rehearsal, w1):
    """W1's cutoff is the session close: the stages keep the session summary, patterns and alerts (hot AND kept)
    and the strategies (kept only) and publish SESSION_CLOSE releases of them; the readers of the kept tier
    (retained mode, the 21-session navigation window of the alerts, the frozen session-close releases) serve them
    through the `api` role. The 26-session physical retention is the registry's
    (rt.portal_snapshot_kept, SESSION_WINDOW 26); the 21-session picker is enforced by the readers."""
    runtime, cohort = rehearsal.deferred, str(rehearsal.cohort)
    kept = rehearsal.q("SELECT snapshot_id, snapshot_type, session_date FROM rt.portal_snapshot_kept "
                       "WHERE cohort_id = %s", (cohort,))
    assert {(row[1]) for row in kept} == {TICKER_WORKSPACE_SUMMARY_VERSION, PATTERN_WATCH_PUBLICATION_VERSION,
                                          STOCK_ALERTS_VERSION, ROLLING_VERSION, HOURLY_ROLLING_VERSION}
    assert len(kept) == 5 and {row[2] for row in kept} == {rehearsal.session}
    _, cursor_factory = reader_for(runtime)
    with cursor_factory() as cursor:
        releases = load_session_close_releases(cursor, rehearsal.cohort, rehearsal.session)
    assert releases and {(item.release_class, item.session_date, item.cutoff) for item in releases} == {
        ("SESSION_CLOSE", rehearsal.session, rehearsal.c1)}
    named = {str(entry["snapshot_id"]) for item in releases for entry in item.entries.values()}
    assert named == {str(row[0]) for row in kept}
    read_at = datetime.now(UTC)
    # Strategies: the kept release is self-contained; both intervals read READY in both modes, fully evaluated.
    strategies = RealtimeDailyStrategyRepository(cursor_factory, transaction_configured=True)
    for interval, version in (("1d", ROLLING_VERSION), ("1h", HOURLY_ROLLING_VERSION)):
        for mode in ("current", "retained"):
            result = strategies.read(available_by=read_at, cohort_id=rehearsal.cohort, interval=interval,
                                     view="evaluations", read_mode=mode)
            publication = result["publication"]
            assert (publication["status"], publication["release_type"], publication["session"],
                    publication["market_time"]) == ("READY", version, str(rehearsal.session),
                                                    rehearsal.closed.isoformat()), (interval, mode)
            assert set(result["members"]) == set(TICKERS)
            assert {item["status"] for item in result["analysis"].values()} <= EVALUATED_ANALYSIS_STATUSES
    # Ticker kept tier: the session-close summary and patterns read in retained mode by their kept ids.
    ticker_reader = RealtimeTickerWorkspaceRepository(cursor_factory, transaction_configured=True)
    kept_ids = {row[1]: str(row[0]) for row in kept}
    publication = ticker_reader.workspace_publication(
        available_by=read_at, cohort_id=rehearsal.cohort, snapshot_id=kept_ids[TICKER_WORKSPACE_SUMMARY_VERSION],
        read_mode="retained")
    assert {item["ticker"] for item in publication["workspaces"]} == set(TICKERS)
    patterns = ticker_reader.scan_patterns(
        interval="1d", limit=10, available_by=read_at, cohort_id=rehearsal.cohort,
        snapshot_id=kept_ids[PATTERN_WATCH_PUBLICATION_VERSION], read_mode="retained")
    assert patterns["status"] == "READY"
    # Alerts: the view and the end-of-day review of the closed session; a session older than the 21-session
    # navigation window is refused by the reader.
    alerts = RealtimeStockAlertViewRepository(cursor_factory, transaction_configured=True)
    assert alerts.alert_view(available_by=read_at, cohort_id=rehearsal.cohort)["missing_producers"]
    review = alerts.eod_review(available_by=read_at, cohort_id=rehearsal.cohort, session_date=str(rehearsal.session))
    assert review["read_at"] == read_at.isoformat()
    outside = str(exchange_calendars.get_calendar("XNYS").session_offset(str(rehearsal.session), -21).date())
    with pytest.raises(StockAlertQueryRejected, match="21-session navigation window"):
        alerts.eod_review(available_by=read_at, cohort_id=rehearsal.cohort, session_date=outside)


@pytest.fixture(scope="module")
def expected(rehearsal, w1):
    """The in-memory projection of the same bars, members and coverage (the ticker_fixture() inputs rebuilt for
    two tickers): its chart points and evidence bodies are what the stored payload must reproduce. The calendar
    and fundamental inputs are read from the database as the producer reads them (the rehearsal's reference facts
    are the only source of them), so the comparison covers the bar -> evidence -> payload round trip."""
    inputs = RealtimeScreeningInput(
        cohort_id=rehearsal.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        source_manifest_sha256=rehearsal.scalar(
            "SELECT manifest.manifest_sha256 FROM rt.cohorts AS cohort JOIN rt.subscription_manifests AS manifest "
            "USING (manifest_id) WHERE cohort.cohort_id = %s", (str(rehearsal.cohort),)),
        source_generation=1, session=rehearsal.session, cutoff=w1.spec.cutoff, known_by=w1.spec.deadline,
        members=support.members_for(rehearsal.history, rehearsal.instrument_ids),
        bars=tuple(support.bar_row(fact) for facts in rehearsal.history.values() for fact in facts), actions=(),
        coverage=tuple(support.split_coverage_rows(rehearsal.history, rehearsal.session,
                                                   received_at=rehearsal.closed - timedelta(minutes=40))))
    _, cursor_factory = reader_for(rehearsal.deferred)
    with cursor_factory() as cursor:
        events, event_coverage = ticker_repository._load_ticker_calendar_inputs(cursor, inputs.members, inputs.cutoff)
        reports, report_coverage = ticker_repository._load_ticker_fundamental_inputs(cursor, inputs.members,
                                                                                     inputs.cutoff)
    return project_realtime_ticker_workspace(
        inputs, market_events=events, market_event_coverage=event_coverage, fundamental_reports=reports,
        fundamental_coverage=report_coverage)


def screening_checks(rehearsal, runtime, cursor_factory, window_cutoff, digest, *, generations):
    """The screening generation EQUITY_EVIDENCE persisted for the window with `window_cutoff`, the current
    pointer on it, and the screening repository (stored-only reader) reading it through `runtime`'s roles:
    the first load recomputes every member's bar heads over rt.bar_current and verifies them."""
    cohort = str(rehearsal.cohort)
    (row,) = rehearsal.q(
        "SELECT generation.status, generation.provider_tier, generation.session_date, generation.generated_at, "
        "generation.expected_members, generation.actual_members, generation.manifest->>'generation', "
        "octet_length(generation.manifest::text), (SELECT count(*) FROM rt.screening_rows AS stored "
        "WHERE stored.generation_id = generation.generation_id) FROM rt.screening_current AS pointer "
        "JOIN rt.screening_generations AS generation ON generation.generation_id = pointer.generation_id "
        "WHERE pointer.cohort_id = %s", (cohort,))
    assert row[:6] == ("COMPLETE", "MASSIVE_DEVELOPER", rehearsal.session, window_cutoff, 2, 2)
    assert row[6] == digest and row[8] == 2
    assert row[7] < 24 * 1024 + 3 * 1024 * len(TICKERS)  # the compact manifest: heads, not bars
    assert rehearsal.scalar("SELECT count(*) FROM rt.screening_generations WHERE cohort_id = %s", (cohort,)) == generations
    repository = RealtimeScreeningRepository(cursor_factory, transaction_configured=True)
    read_at = datetime.now(UTC)
    catalog = repository.catalog(available_by=read_at, cohort_id=rehearsal.cohort)
    assert catalog["status"] == "READY" and catalog["publications"][0]["generation"] == digest
    with cursor_factory() as cursor:
        payload = repository._load(cursor, cohort, read_at)
    assert payload["generation"] == digest and [item["ticker"] for item in payload["rows"]] == sorted(TICKERS)
    return payload


def test_a5_the_reader_composes_charts_and_evidence_equal_to_the_fixture_projection(rehearsal, w1, expected):
    runtime = rehearsal.deferred
    reader, cursor_factory = reader_for(runtime)
    read_at = datetime.now(UTC)  # after every recording clock: the reader's evidence rows must be durable
    stored = workspace_payloads(rehearsal)
    offline = {snapshot.snapshot_key: snapshot for snapshot in expected.snapshots}
    for ticker in TICKERS:
        snapshot_id, payload, manifest, _ = stored[ticker]
        loaded = reader._snapshot(ticker, read_at, rehearsal.cohort, None, "current")
        assert "charts" not in loaded and "evidence" not in loaded
        for interval, chart_ref in payload["chart_refs"].items():
            # The default `current` mode (it resolves the cohort through the persisted screening generation) ...
            chart = reader.read(ticker, "chart", interval=interval, period="5y", available_by=read_at,
                                cohort_id=rehearsal.cohort)
            assert chart == expected_chart(rehearsal, ticker, chart_ref), (ticker, interval)
        # ... and the retained mode serve the same bars.
        assert reader.read(ticker, "chart", interval="1d", period="5y", available_by=read_at,
                           cohort_id=rehearsal.cohort, read_mode="retained") == expected_chart(
            rehearsal, ticker, payload["chart_refs"]["1d"])
        assert reader.read(ticker, "quote", available_by=read_at, cohort_id=rehearsal.cohort) == payload["quote"]
        # The stored payload and the evidence the reader verifies and composes equal the offline projection's.
        stored_json, offline_json = as_stored_json(payload), as_stored_json(offline[ticker].payload)
        # `context.cohort.generation` is the digest of the screening manifest, which embeds the digest of the whole
        # input set (row clocks such as the database insert time included): the only identity that cannot be rebuilt
        # offline. Everything else of the payload must be equal.
        assert len(stored_json["context"]["cohort"].pop("generation")) == 64
        offline_json["context"]["cohort"].pop("generation")
        assert stored_json == offline_json, (ticker, differing_paths(stored_json, offline_json))
        with cursor_factory() as cursor:
            bodies = ticker_repository._verified_evidence_bodies(
                cursor, manifest, payload, cohort_id=rehearsal.cohort, cutoff=read_at)
        assert as_stored_json(compose_evidence(payload["evidence_index"], bodies)) == as_stored_json(
            old_embedded_evidence(offline[ticker], expected))
    # F5 (fixed): EQUITY_EVIDENCE persists the screening generation the workspaces name, the pointer is on it,
    # and the stored-only screening reader verifies it against the bar tables.
    digests = {stored[ticker][1]["context"]["cohort"]["generation"] for ticker in TICKERS}
    assert len(digests) == 1
    screening_checks(rehearsal, runtime, cursor_factory, w1.spec.cutoff, digests.pop(), generations=1)


# --- Part A, W2: the delta-loader path -------------------------------------------------------------------------------

@contextmanager
def swallow_simulated_crash():
    """The crash kills a lane thread by design; keep the default thread excepthook from reporting it."""
    original = threading.excepthook

    def hook(args):
        if not issubclass(args.exc_type, SimulatedCrash):
            original(args)

    threading.excepthook = hook
    try:
        yield
    finally:
        threading.excepthook = original


@pytest.fixture(scope="module")
def w2(rehearsal, w1):
    """A second ingest of restated 1m bars (three per ticker) fetched AFTER W1's deadline (W1's fixture returned
    only once it had passed), so they are not late arrivals for W1's loader: they became visible by the clocks
    advancing, which is the delta path. W2's own budget is 12 s: the ingest, the drain and the compute of six
    bars take a few seconds; the fixture returns after W2's deadline has passed."""
    runtime = rehearsal.deferred
    before = dict(counts=rehearsal.counts(), release=current_release(rehearsal)[0],
                  ids={ticker: value[0] for ticker, value in workspace_payloads(rehearsal).items()})
    revised = support.revise_last_bars(rehearsal.history, count=3, observed_at=datetime.now(UTC), label="w2")
    rehearsal.source.queue("revisions-w2", revised)
    spec = rehearsal.spec("w2", rehearsal.cutoff(1), budget=timedelta(seconds=12))
    report, seconds = runtime.run_window(spec)
    outcome = Outcome(spec, report, seconds, extra=dict(
        before=before, revised=revised, counts=rehearsal.counts(), load=runtime.loads.get(("w2", 1)),
        strategy=strategy_status(rehearsal)))
    rehearsal.wait_until(spec.deadline)
    return outcome


def test_a6_the_second_window_takes_the_delta_loader_path_and_publishes_a_new_identity(rehearsal, w1, w2):
    runtime = rehearsal.deferred
    print("\nW2 stage seconds", runtime.stage_times("w2"), "total", round(w2.seconds, 1))
    print("W2 rows", rehearsal.stage_rows("w2"), "strategy", w2.extra["strategy"], "W1 strategy", w1.extra["strategy"])
    (report,) = w2.report.windows
    assert w2.report.window_errors == {} and report.window_state == "COMPLETE", rehearsal.stage_rows("w2")
    # The restated bars were fetched and inserted after W1's deadline and inside W2's: they became visible by the
    # recording clocks advancing. The loader kept W1's 27,608 bars in process and read only those six.
    revised = [fact for ticker in TICKERS for fact in w2.extra["revised"][ticker]]
    assert len(revised) == 6
    ids = [str(fact.revision.bar_revision_id) for fact in revised]
    stamps = rehearsal.q("SELECT min(observed_at), min(created_at), max(observed_at), max(created_at) "
                         "FROM rt.bar_revisions WHERE bar_revision_id = ANY(%s::uuid[])", (ids,))[0]
    assert w1.spec.deadline < min(stamps[0], stamps[1]) and max(stamps[2], stamps[3]) <= w2.spec.deadline
    assert (w1.extra["load"].mode, w1.extra["load"].reason) == ("full", "NO_STATE")
    load = w2.extra["load"]
    assert (load.mode, load.reason, load.retained_bars) == ("delta", "", 27_608)
    assert 0 < load.bar_rows_read <= 6
    before, after = w2.extra["before"], w2.extra["counts"]
    for table in ("rt.bar_catalog", "rt.bar_revisions"):
        assert after[table] == before["counts"][table] + 6
    assert after["rt.bar_current"] == before["counts"]["rt.bar_current"]  # the six bars now point at their revisions
    assert rehearsal.scalar("SELECT count(*) FROM rt.bar_current WHERE bar_revision_id = ANY(%s::uuid[])",
                            (ids,)) == 6
    assert after["rt.materialization_jobs"] == before["counts"]["rt.materialization_jobs"] + 2
    assert rehearsal.q("SELECT status, count(*) FROM rt.materialization_jobs WHERE cohort_id = %s GROUP BY 1",
                       (str(rehearsal.cohort),)) == [("COMPLETE", after["rt.materialization_jobs"])]
    # The restated bars are in W2's own receipt and release: a fresh full load under W2's two clocks builds the
    # input the receipt names; a new cutoff, a second receipt (same reference generation), a new release and new
    # workspace identities were published.
    assert inputs_sha256(oracle_inputs(runtime, rehearsal, w2.spec, 1)) == receipt_input_sha(rehearsal, w2.spec)
    release_id, _, _, cutoff, _ = current_release(rehearsal)
    assert cutoff == w2.spec.cutoff and release_id != before["release"]
    assert rehearsal.q("SELECT cutoff, reference_generation FROM rt.cycle_receipts WHERE cohort_id = %s "
                       "ORDER BY cutoff", (str(rehearsal.cohort),)) == [(rehearsal.c1, 1), (w2.spec.cutoff, 1)]
    current = {ticker: value[0] for ticker, value in workspace_payloads(rehearsal).items()}
    assert all(current[ticker] != before["ids"][ticker] for ticker in TICKERS)
    # The reader serves the restated 1m bars. The reader composes a chart per interval, lazily: the W1 workspace
    # pinned by id still loads, its 1m chart (whose bars were restated) fails closed with 'republish required',
    # while its other charts and its quote keep serving.
    reader, _ = reader_for(runtime)
    read_at = datetime.now(UTC)
    for ticker in TICKERS:
        _, payload, _, _ = workspace_payloads(rehearsal)[ticker]
        chart = reader.read(ticker, "chart", interval="1m", period="5y", available_by=read_at,
                            cohort_id=rehearsal.cohort)
        assert chart == expected_chart(rehearsal, ticker, payload["chart_refs"]["1m"], w2.extra["revised"][ticker])
        pinned = dict(available_by=read_at, cohort_id=rehearsal.cohort, snapshot_id=before["ids"][ticker],
                      read_mode="retained", period="5y")
        reader._snapshot(ticker, read_at, rehearsal.cohort, before["ids"][ticker], "retained")  # loads, no bar read
        with pytest.raises(ticker_repository.TickerPublicationUnavailable,
                           match="bar lineage changed; republish required"):
            reader.read(ticker, "chart", interval="1m", **pinned)
        assert reader.read(ticker, "chart", interval="1d", **pinned)
        assert reader.read(ticker, "chart", interval="5m", **pinned)
        assert reader.read(ticker, "quote", **{key: value for key, value in pinned.items() if key != "period"})


# --- Part A, W3: crash after the stage's work, then retry -------------------------------------------------------------

@pytest.fixture(scope="module")
def w3(rehearsal, w2):
    """Restated bars fetched after W2's deadline (so they are invisible to W2 for ever and new to W3), then W3: the
    EQUITY_EVIDENCE stage dies after its work and the window is run again."""
    runtime = rehearsal.deferred
    revised = support.revise_last_bars(rehearsal.history, count=2, observed_at=datetime.now(UTC), label="w3")
    rehearsal.source.queue("revisions-w3", revised)
    spec = rehearsal.spec("w3", rehearsal.cutoff(2))
    stubs = ("STOCK_ALERTS",)
    before = rehearsal.counts()
    runtime.crash_after.add(("w3", "EQUITY_EVIDENCE", 1))
    with swallow_simulated_crash():
        first, _ = runtime.run_window(spec, stub=stubs)
    crashed = dict(counts=rehearsal.counts(), rows=rehearsal.stage_rows("w3"), state=rehearsal.window_state("w3"),
                   released=first, load=runtime.loads.get(("w3", 1)))
    report, seconds = runtime.run_window(spec, stub=stubs)
    return Outcome(spec, report, seconds, extra=dict(
        before=before, crashed=crashed, counts=rehearsal.counts(), revised=revised,
        load=runtime.loads.get(("w3", 2))))


def test_a7_a_runner_killed_after_its_work_is_retried_without_duplicating_bars_evidence_or_releases(rehearsal, w3):
    print("\nW3 rows", rehearsal.stage_rows("w3"))
    crashed, before, final = w3.extra["crashed"], w3.extra["before"], w3.extra["counts"]
    # The first run: the ingest finished, EQUITY_EVIDENCE did all of its work and died before it could finish;
    # nothing after it ran and the window was released for another owner.
    (released,) = crashed["released"].windows
    assert released.status == "PENDING_STAGES" and crashed["state"] == "PENDING"
    assert [(stage, attempt, state) for stage, attempt, state, _ in crashed["rows"]] == [
        ("EQUITY_EVIDENCE", 1, "RUNNING"), ("EQUITY_INGEST", 1, "COMPLETE")]
    assert crashed["counts"]["rt.bar_revisions"] == before["rt.bar_revisions"] + 4  # the crashed attempt drained
    assert crashed["counts"]["rt.cycle_receipts"] == before["rt.cycle_receipts"]  # but never reported its receipt
    # The restated bars were fetched after W2's deadline: new to W3's loader, a delta on the first attempt and an
    # empty delta on the retry (same clocks, nothing new).
    assert (crashed["load"].mode, crashed["load"].reason) == ("delta", "") and crashed["load"].bar_rows_read == 4
    assert (w3.extra["load"].mode, w3.extra["load"].bar_rows_read) == ("delta", 0)
    # The retry: the dead attempt is OWNER_LOST, attempt 2 re-runs the whole stage and the window completes.
    (report,) = w3.report.windows
    assert report.window_state == "COMPLETE", rehearsal.stage_rows("w3")
    rows = {(stage, attempt): (state, codes) for stage, attempt, state, codes in rehearsal.stage_rows("w3")}
    assert rows[("EQUITY_EVIDENCE", 1)] == ("FAILED", ("OWNER_LOST",))
    assert rows[("EQUITY_EVIDENCE", 2)] == ("COMPLETE", ())
    assert {key: value[0] for key, value in rows.items() if key[0] != "EQUITY_EVIDENCE" or key[1] == 2} == {
        (stage, 1 if stage != "EQUITY_EVIDENCE" else 2): "COMPLETE" for stage in WINDOW_STAGES}
    # Idempotent: the replayed bar drain, evidence and screening-generation persistence added no row of any kind.
    for table in ("rt.bar_catalog", "rt.bar_revisions", "rt.bar_current", "rt.evidence_catalog", "rt.equity_evidence",
                  "rt.materialization_jobs", "rt.acquisition_batches"):
        assert final[table] == crashed["counts"][table], table
    cohort = str(rehearsal.cohort)
    assert rehearsal.scalar("SELECT count(*) FROM (SELECT bar_revision_id FROM rt.bar_revisions WHERE cohort_id = %s "
                            "GROUP BY 1 HAVING count(*) > 1) AS duplicated", (cohort,)) == 0
    assert rehearsal.scalar("SELECT count(*) - count(DISTINCT evidence_key) FROM rt.equity_evidence "
                            "WHERE cohort_id = %s", (cohort,)) == 0
    assert rehearsal.scalar("SELECT count(*) FROM rt.screening_generations WHERE cohort_id = %s", (cohort,)) == 3
    assert rehearsal.q("SELECT count(*) FROM rt.cycle_receipts WHERE cohort_id = %s AND cutoff = %s",
                       (cohort, w3.spec.cutoff)) == [(1,)]
    assert rehearsal.q("SELECT release_class, count(*) FROM rt.portal_releases WHERE cohort_id = %s AND cutoff = %s "
                       "GROUP BY 1", (cohort, w3.spec.cutoff)) == [("INTRADAY", 1)]
    assert current_release(rehearsal)[3] == w3.spec.cutoff


def test_a7b_a_bar_recorded_after_a_deadline_is_invisible_to_that_window_and_visible_to_the_next(rehearsal, w2, w3):
    runtime = rehearsal.deferred
    restated = [fact for ticker in TICKERS for fact in w3.extra["revised"][ticker]]
    ids = {str(fact.revision.bar_revision_id) for fact in restated}
    stamps = rehearsal.q("SELECT min(observed_at), min(created_at) FROM rt.bar_revisions "
                         "WHERE bar_revision_id = ANY(%s::uuid[])", (list(ids),))[0]
    assert w2.spec.deadline < min(stamps)  # observed AND inserted after W2's deadline
    # W3's bars exist now, yet a fresh full load under W2's own two clocks still builds exactly the input W2's
    # receipt names: the late rows are not part of it. The next window's receipt names them.
    at_w2 = oracle_inputs(runtime, rehearsal, w2.spec, 1)
    at_w3 = oracle_inputs(runtime, rehearsal, w3.spec, 1)
    assert inputs_sha256(at_w2) == receipt_input_sha(rehearsal, w2.spec)
    assert inputs_sha256(at_w3) == receipt_input_sha(rehearsal, w3.spec) != receipt_input_sha(rehearsal, w2.spec)
    assert ids.isdisjoint(str(bar["bar_revision_id"]) for bar in at_w2.bars)
    assert ids <= {str(bar["bar_revision_id"]) for bar in at_w3.bars}
    # ... and they are in W3's release: the chart read serves them (the restatements of W2 and W3 applied).
    reader, _ = reader_for(runtime)
    for ticker in TICKERS:
        payload = workspace_payloads(rehearsal)[ticker][1]
        assert reader.read(ticker, "chart", interval="1m", period="5y", available_by=datetime.now(UTC),
                           cohort_id=rehearsal.cohort) == expected_chart(
            rehearsal, ticker, payload["chart_refs"]["1m"], (*w2.extra["revised"][ticker], *w3.extra["revised"][ticker]))


# --- Part A, W3b: a late arrival that forces a full reload ------------------------------------------------------------

class ProviderDatabase(RealtimeProcessDatabase):
    """A process database over the access provider for work outside any window (no stage run, no fence)."""

    def __init__(self, provider, role: RealtimeDatabaseRole) -> None:
        self._process_plan = StageProcessPlan(role)
        self._connection_provider = None
        self._provider = provider

    @contextmanager
    def transaction(self, *, read_only=None, dict_cursor=False, isolation_level=None):
        with self._provider.transaction(read_only=bool(read_only), dict_cursor=dict_cursor,
                                        isolation_level=isolation_level) as unit:
            yield unit.cursor


@pytest.fixture(scope="module")
def w3b(rehearsal, w3):
    """Restated bars fetched and inserted AFTER W3's load but with every recording clock before W3's deadline
    (10 minutes of budget): no clock crossed a bound, yet the bars are visible to the next window's clocks. The
    loader's integrity count must catch that and fall back to a full reload."""
    runtime = rehearsal.deferred
    revised = support.revise_last_bars(rehearsal.history, count=2, observed_at=datetime.now(UTC), label="w3b")
    rehearsal.source.queue("revisions-w3b", revised)
    spec = rehearsal.spec("w3b", rehearsal.cutoff(3))
    before = rehearsal.counts()
    report, seconds = runtime.run_window(spec)
    published = {ticker: value[0] for ticker, value in workspace_payloads(rehearsal).items()}
    after = rehearsal.counts()
    # What a brand-new process would have published for the same window: new loader, new projection state,
    # the cycle recomputed from scratch (the evidence persistence is idempotent: nothing may change).
    fresh = compute_realtime_ticker_cycle(
        ProviderDatabase(runtime.provider, RealtimeDatabaseRole.COMPUTE), state=TickerProjectionState(),
        loader=ScreeningInputLoader(), cohort_id=rehearsal.cohort, session=rehearsal.session, cutoff=spec.cutoff,
        known_by=spec.deadline, source_generation=1)
    return Outcome(spec, report, seconds, extra=dict(
        before=before, after=after, revised=revised, load=runtime.loads.get(("w3b", 1)), published=published,
        fresh={snapshot.snapshot_key: str(snapshot.snapshot_id) for snapshot in fresh.projection.snapshots},
        fresh_inputs=inputs_sha256(fresh.inputs), counts_after_fresh=rehearsal.counts()))


def test_a7c_a_late_arrival_forces_a_full_reload_that_publishes_the_identity_of_a_fresh_full_load(rehearsal, w3, w3b):
    (report,) = w3b.report.windows
    assert w3b.report.window_errors == {} and report.window_state == "COMPLETE", rehearsal.stage_rows("w3b")
    revised = [fact for ticker in TICKERS for fact in w3b.extra["revised"][ticker]]
    ids = [str(fact.revision.bar_revision_id) for fact in revised]
    stamps = rehearsal.q("SELECT max(observed_at), max(created_at) FROM rt.bar_revisions "
                         "WHERE bar_revision_id = ANY(%s::uuid[])", (ids,))[0]
    # Late by the loader's rule: recorded after W3's load, with every clock still before W3's deadline.
    assert max(stamps) <= w3.spec.deadline
    load = w3b.extra["load"]
    assert (load.mode, load.reason) == ("full", "VISIBLE_SET_DRIFT") and load.bar_rows_read == 27_608
    # The full reload is correct: the receipt names the input a fresh full load builds, and the published workspaces
    # are the ones a brand-new process (new loader, new projection state) computes for the same window.
    assert receipt_input_sha(rehearsal, w3b.spec) == w3b.extra["fresh_inputs"]
    assert w3b.extra["published"] == w3b.extra["fresh"] and set(w3b.extra["fresh"]) == set(TICKERS)
    assert w3b.extra["counts_after_fresh"] == w3b.extra["after"]  # the fresh recompute persisted nothing new
    assert len(ids) == 4 and rehearsal.scalar(
        "SELECT count(*) FROM rt.bar_current WHERE bar_revision_id = ANY(%s::uuid[])", (ids,)) == 4


# --- Part A: the role audit and the timings --------------------------------------------------------------------------

def test_a8_every_stage_ran_through_the_one_provider_and_window_writes_were_fenced(rehearsal, w3b):
    runtime = rehearsal.deferred
    stages = {"REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STOCK_ALERTS", "STRATEGY_PUBLISH"}
    assert runtime.stages_used(write=True) == stages
    # STRATEGY_PUBLISH writes only in the window that PUBLISHES a strategy (W1: the market time is the session
    # close for every later window of the run too); the later windows find the retained snapshot
    # (ALREADY_STORED) and only read.
    assert "STRATEGY_PUBLISH" in runtime.stages_used("w1", write=True)
    assert all("STRATEGY_PUBLISH" not in runtime.stages_used(window, write=True) for window in ("w2", "w3", "w3b"))
    assert all("STRATEGY_PUBLISH" in runtime.stages_used(window) for window in ("w2", "w3", "w3b"))
    assert runtime.stages_used() == stages
    # Window-stage writes commit only under the stage-run fence; the resident REFERENCE stage has no stage run.
    assert all(used.fenced for used in runtime.transactions if used.stage in WINDOW_STAGES and not used.read_only)
    assert not any(used.fenced for used in runtime.transactions if used.stage == "REFERENCE")


def test_a9_the_timings_per_stage_are_reported(rehearsal, w3b):
    runtime = rehearsal.deferred
    rows = {}
    for (window, stage, attempt), seconds in sorted(runtime.timings.items()):
        rows.setdefault(window, {})[f"{stage}#{attempt}"] = round(seconds, 1)
    print("\nTIMINGS part A (seconds)")
    print(f"  history build                 {rehearsal.history_seconds:.1f}")
    for window in ("RESIDENT", "w1", "w2", "w3", "w3b"):
        print(f"  {window:<8} {rows.get(window)}")
    print("  phases", {f"{key[0]}:{key[1]}": round(value, 1) for key, value in sorted(PHASES.items())
                       if not key[0].startswith("backfill")})
    print("  backfill windows (13): ingest + bar drain",
          round(sum(value for key, value in PHASES.items() if key[0].startswith("backfill")), 1), "ingest;",
          round(sum(seconds for key, seconds in runtime.timings.items() if key[0].startswith("backfill")), 1),
          "all stages")
    assert sum(seconds for key, seconds in runtime.timings.items() if key[0] == "w1") < 300


# --- PART B ------------------------------------------------------------------------------------------------------------

class RolePool:
    """Pool shim for a NOLOGIN role: a checkout is a connection of the admin pool with `SET ROLE <role>`
    applied for the session (committed, so a rollback inside a unit of work does not undo it); a check-in
    resets the role. V2ConnectionProvider then runs its per-transaction `SET LOCAL ROLE <role>` inside it.
    LIMIT OF THE SIMULATION: PostgreSQL lets a superuser session user `SET ROLE` to any role, so this proves
    the role's grants, which the server enforces for the role in force, and not a LOGIN credential."""

    def __init__(self, pool, role: str) -> None:
        self._pool, self.role = pool, role

    def getconn(self):
        connection = self._pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.role)))
            connection.commit()
        except BaseException:
            self._pool.putconn(connection, close=True)
            raise
        return connection

    def putconn(self, connection, close=False):
        try:
            if not connection.closed:
                connection.rollback()
                with connection.cursor() as cursor:
                    cursor.execute("RESET ROLE")
                connection.commit()
        except Exception:  # noqa: BLE001 - a connection that cannot be reset is discarded
            close = True
        self._pool.putconn(connection, close=close or bool(connection.closed))


def hardened_config():
    return V2AccessConfig(security_mode="hardened", role_prefix=ROLE_PREFIX, allow_disposable=True)


def test_b0_a_hardened_runtime_refuses_the_database_while_it_is_still_deferred(rehearsal, w3b):
    with pytest.raises(RuntimeError, match="hardened access requires security phase AUDITED, database is DEFERRED"):
        verify_pool_target_v2(rehearsal.pool, hardened_config())


@pytest.fixture(scope="module")
def hardening(rehearsal, roles, w3b):
    """The hardening of the database Part A just ran on: NOLOGIN roles, exact grants, exact-privilege audit."""
    connection = rehearsal.pool.getconn()
    started = time.perf_counter()
    try:
        created, audit = harden_database_v2(connection, roles)
        seconds = time.perf_counter() - started
        phase = current_security_phase_v2(connection)
        connection.rollback()
    finally:
        rehearsal.pool.putconn(connection)
    return SimpleNamespace(created=created, audit=audit, phase=phase, seconds=seconds)


@pytest.fixture(scope="module")
def hardened(rehearsal, roles, hardening):
    pools = {role: RolePool(rehearsal.pool, roles.name(role)) for role in ("app", "api")}
    provider = V2ConnectionProvider(pools, hardened_config())
    return Runtime("hardened", provider, rehearsal), pools


def test_b1_hardening_creates_only_nologin_roles_and_the_exact_privilege_audit_passes(rehearsal, roles, hardening):
    assert sorted(hardening.created) == sorted(roles.all_names)
    assert hardening.audit and all(status.startswith("PASS") for _, status in hardening.audit), hardening.audit
    assert hardening.phase == "AUDITED"
    assert rehearsal.scalar("SELECT count(*) FROM pg_roles WHERE rolname = ANY(%s) AND rolcanlogin",
                            (list(roles.all_names),)) == 0
    assert rehearsal.scalar("SELECT count(*) FROM pg_roles WHERE rolname = ANY(%s)", (list(roles.all_names),)) == len(
        roles.all_names)
    print("\nHARDENING seconds", round(hardening.seconds, 1), "audit checks", len(hardening.audit))


def test_b2_the_runtime_gate_reads_as_the_role_it_logs_in_as_and_the_roles_share_no_membership(
        rehearsal, roles, hardened):
    runtime, pools = hardened
    config = runtime.provider.config
    # Both runtime roles may read the identity relations: the gate passes for each of them, with or without
    # naming the role the pool logs in as.
    for role in ("app", "api"):
        assert verify_pool_target_v2(pools[role], config) == DATABASE
        assert verify_pool_target_v2(pools[role], config, role) == DATABASE
    assert runtime.provider.verify() == DATABASE
    with pytest.raises(ValueError, match="must not hold an owner pool"):
        V2ConnectionProvider({OWNER_POOL: rehearsal.pool}, config)
    with pytest.raises(ValueError, match="read-only"):
        with runtime.provider.transaction("api"):
            pass
    members = {(member, role): rehearsal.scalar("SELECT pg_has_role(%s, %s, 'MEMBER')",
                                                (roles.name(member), roles.name(role)))
               for member in ROLES for role in ROLES if member != role}
    assert len(members) == 6 and not any(members.values())


@pytest.fixture(scope="module")
def w4(rehearsal, hardened, w3b):
    """A full window as the app role: a new reference seed (generation 2, created between W3b's and W4's
    cutoffs), a new ingest of restated bars fetched now, five stages."""
    runtime, _ = hardened
    tick = runtime.tick_reference(rehearsal.cutoff(3.5))
    revised = support.revise_last_bars(rehearsal.history, count=2, observed_at=datetime.now(UTC), label="w4")
    rehearsal.source.queue("revisions-w4", revised)
    spec = rehearsal.spec("w4", rehearsal.cutoff(4))
    report, seconds = runtime.run_window(spec)
    return Outcome(spec, report, seconds, extra=dict(
        tick=tick, revised=revised, counts=rehearsal.counts(), load=runtime.loads.get(("w4", 1)),
        strategy=strategy_status(rehearsal)))


def test_b3_a_full_window_completes_as_the_app_role_with_the_grants_of_the_registry(
        rehearsal, roles, hardened, w2, w3, w3b, w4):
    runtime, _ = hardened
    print("\nW4 stage seconds", runtime.stage_times("w4"), "total", round(w4.seconds, 1))
    print("W4 rows", rehearsal.stage_rows("w4"), "strategy", w4.extra["strategy"])
    assert w4.extra["tick"] == "COMPLETE" and w4.report.window_errors == {}
    (report,) = w4.report.windows
    states = {stage: (state, codes) for stage, _, state, codes in rehearsal.stage_rows("w4")}
    # As in W1, the whole window COMPLETEs as app, with the same degraded codes. The strategies'
    # market time is the same session close as W1's, so the stage finds the retained snapshots (ALREADY_STORED),
    # validates them as app and publishes nothing new.
    assert report.window_state == "COMPLETE", states
    assert {stage: state for stage, (state, _) in states.items()} == {stage: "COMPLETE" for stage in WINDOW_STAGES}
    assert {stage: codes for stage, (state, codes) in states.items() if codes} == {
        "STOCK_ALERTS": ("MISSING_PRODUCER:context", "MISSING_PRODUCER:forward", "MISSING_PRODUCER:replay")}
    assert w4.extra["strategy"] == "1d=ALREADY_STORED;1h=ALREADY_STORED"
    # Two READY seeds now; the new receipt carries that count as its generation; the pointer moved to W4.
    assert rehearsal.ready_seeds() == 2
    assert rehearsal.scalar("SELECT generation FROM rt.seed_current WHERE scope = 'MARKET_REFERENCES'") == 2
    assert rehearsal.q("SELECT cutoff, reference_generation FROM rt.cycle_receipts WHERE cohort_id = %s "
                       "AND window_id = %s", (str(rehearsal.cohort), str(report.window_id))) == [(w4.spec.cutoff, 2)]
    assert rehearsal.ready_seeds(w4.spec.cutoff) == 2
    release_id, release_class, _, cutoff, entries = current_release(rehearsal)
    assert (release_class, cutoff) == ("INTRADAY", w4.spec.cutoff)
    # The pointed-at release still names the strategies of W1 (carried forward by the lanes that published).
    assert len(entries) == len(TICKERS) + 5 and {ROLLING_VERSION, HOURLY_ROLLING_VERSION} <= {key[0] for key in entries}
    # The stages that wrote are those of Part A (a hardened provider raises instead of letting a wrong role
    # through, and PostgreSQL enforced every grant for `app`). W4's strategy stage only reads, so its writes
    # are those of W1 (deferred) plus the probe b5.
    assert runtime.stages_used(write=True) == rehearsal.deferred.stages_used(write=True) - {"STRATEGY_PUBLISH"}
    assert runtime.stages_used() == rehearsal.deferred.stages_used()
    assert all(used.fenced for used in runtime.transactions if used.stage in WINDOW_STAGES and not used.read_only)
    # Reads through the api role, in the default `current` mode: the workspace the window
    # published, composed from the tables the reader role may read, equals the bar facts (every restatement
    # generation applied, oldest first: a later observation of the same bar wins); the screening generation the
    # compute stage persisted reads back through the stored-only screening reader (first load verifies bar heads).
    reader, cursor_factory = reader_for(runtime)
    restated = {ticker: (*w2.extra["revised"][ticker], *w3.extra["revised"][ticker], *w3b.extra["revised"][ticker],
                         *w4.extra["revised"][ticker]) for ticker in TICKERS}
    digests = set()
    for ticker, (snapshot_id, payload, manifest, manifest_bytes) in workspace_payloads(rehearsal).items():
        assert manifest_bytes < 10_000
        digests.add(payload["context"]["cohort"]["generation"])
        for interval in ("1m", "1d"):
            chart = reader.read(ticker, "chart", interval=interval, period="5y", available_by=datetime.now(UTC),
                                cohort_id=rehearsal.cohort)
            assert chart == expected_chart(rehearsal, ticker, payload["chart_refs"][interval],
                                           restated[ticker] if interval == "1m" else ())
        assert reader.read(ticker, "quote", available_by=datetime.now(UTC), cohort_id=rehearsal.cohort) == payload["quote"]
    assert len(digests) == 1
    screening_checks(rehearsal, runtime, cursor_factory, w4.spec.cutoff, digests.pop(), generations=5)
    # The kept tier through the api role: the strategies W1 published are still named by the
    # pointed-at release and read READY in `current` mode; the session-close releases of the session are readable.
    strategies = RealtimeDailyStrategyRepository(cursor_factory, transaction_configured=True)
    for interval in ("1d", "1h"):
        publication = strategies.read(available_by=datetime.now(UTC), cohort_id=rehearsal.cohort, interval=interval,
                                      view="evaluations")["publication"]
        assert (publication["status"], publication["market_time"]) == ("READY", rehearsal.closed.isoformat())
    with cursor_factory() as cursor:
        assert load_session_close_releases(cursor, rehearsal.cohort, rehearsal.session)


def test_b4_forbidden_writes_are_rejected_by_postgresql_and_leave_no_row(rehearsal, roles, hardened, w4):
    runtime, _ = hardened
    cohort = str(rehearsal.cohort)
    before = rehearsal.counts()
    snapshot_insert = (
        "INSERT INTO rt.snapshot_catalog (snapshot_id, cohort_id, provider_tier, snapshot_class, snapshot_type, "
        "snapshot_key, session_date, source_generation, manifest_sha256, payload_sha256, generated_at) "
        "VALUES (%s, %s, 'MASSIVE_DEVELOPER', 'EPHEMERAL_PORTAL', 'OUT_OF_ROLE', 'X', %s, 1, %s, %s, %s)",
        (str(uuid4()), cohort, rehearsal.session, "a" * 64, "b" * 64, rehearsal.closed))
    # app holds INSERT and UPDATE on every writable table, so an append-only table is guarded by its trigger;
    # DELETE, owner-only tables and DDL are guarded by missing privileges.
    attempts = [
        (psycopg2.errors.RaiseException, "immutable",
         ("UPDATE rt.snapshot_catalog SET snapshot_key = snapshot_key WHERE cohort_id = %s", (cohort,))),
        (psycopg2.errors.InsufficientPrivilege, "permission denied for table snapshot_catalog",
         ("DELETE FROM rt.snapshot_catalog WHERE cohort_id = %s", (cohort,))),
        (psycopg2.errors.InsufficientPrivilege, "permission denied for table schema_migrations",
         ("SELECT count(*) FROM rt.schema_migrations", ())),
        (psycopg2.errors.InsufficientPrivilege, "permission denied for schema rt",
         ("CREATE TABLE rt.out_of_role_probe (x integer)", ())),
    ]
    for error, message, (statement, parameters) in attempts:
        with pytest.raises(error, match=message):
            with runtime.provider.transaction() as unit:
                unit.cursor.execute(statement, parameters)
    # api is read-only: the provider refuses a read-write unit of work, and a write inside a read-only one fails.
    with pytest.raises(ValueError, match="read-only"):
        with runtime.provider.transaction("api"):
            pass
    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        with runtime.provider.transaction("api", read_only=True) as unit:
            unit.cursor.execute(*snapshot_insert)
    assert not rehearsal.scalar("SELECT has_table_privilege(%s, 'rt.snapshot_catalog', 'INSERT')",
                                (roles.name("api"),))
    assert rehearsal.counts() == before


class ProbeRollback(Exception):
    pass


def test_b5_the_kept_and_session_close_writes_of_w1_are_granted_to_the_hardened_app_role(
        rehearsal, hardened, w4):
    """W1 (deferred) took the first publication writes of the session close for real: the kept snapshot of the
    strategies, the hot-and-kept pair of the ticker/alerts stages and the session-close releases. W4, as
    the hardened app role, is not a session-close window and its strategies are ALREADY_STORED, so the same three
    writes (kept-only, hot-and-kept, session-close release) are exercised as `app`
    here with probe snapshots and rolled back: the grants cover them."""
    runtime, _ = hardened
    cohort = rehearsal.cohort
    (receipt_row,) = rehearsal.q(
        "SELECT receipt.receipt_id, receipt.universe_sha256 FROM rt.cycle_receipts AS receipt "
        "JOIN rt.window_runs AS window_run USING (window_id) WHERE window_run.cohort_id = %s "
        "AND window_run.window_key = 'w4'", (str(cohort),))
    receipt = PortalReleaseReceipt(receipt_row[0], cohort, ProviderTier.MASSIVE_DEVELOPER, w4.spec.cutoff,
                                   receipt_row[1])
    snapshot = build_portal_snapshot(
        cohort_id=cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER, snapshot_type=STOCK_ALERTS_VERSION,
        snapshot_key="REHEARSAL_PROBE", source_generation=2,
        source_manifest=dict(version=STOCK_ALERTS_VERSION, cohort_id=str(cohort), source_manifest_sha256="a" * 64,
                             source_generation=2, provider_tier="MASSIVE_DEVELOPER", reference_set=reference_set([])),
        payload=dict(version=STOCK_ALERTS_VERSION, references=[], streams=[], cutoff=w4.spec.cutoff.isoformat(),
                     missing_producers={}, provider_fetch_performed=False, publication_permission=False,
                     execution_permission=False),
        generated_at=w4.spec.cutoff - timedelta(seconds=30))
    kept_only = build_portal_snapshot(
        cohort_id=cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER, snapshot_type=STOCK_ALERTS_VERSION,
        snapshot_key="REHEARSAL_PROBE_KEPT", source_generation=2,
        source_manifest=dict(version=STOCK_ALERTS_VERSION, cohort_id=str(cohort), source_manifest_sha256="a" * 64,
                             source_generation=2, provider_tier="MASSIVE_DEVELOPER", reference_set=reference_set([])),
        payload=dict(version=STOCK_ALERTS_VERSION, references=[], streams=[], cutoff=w4.spec.cutoff.isoformat(),
                     missing_producers={}, provider_fetch_performed=False, publication_permission=False,
                     execution_permission=False),
        generated_at=w4.spec.cutoff - timedelta(seconds=30))
    before = rehearsal.counts()
    written = {}
    with pytest.raises(ProbeRollback):
        with runtime.provider.transaction() as unit:
            kept = persist_portal_snapshot_cursor(unit.cursor, snapshot, retention=RETENTION_HOT_AND_KEPT)
            only = persist_portal_snapshot_cursor(unit.cursor, kept_only, retention=RETENTION_KEPT)
            intraday = publish_portal_release_cursor(unit.cursor, receipt, (snapshot,))
            session_close = publish_portal_release_cursor(
                unit.cursor, receipt, (kept, only), release_class=RELEASE_CLASS_SESSION_CLOSE,
                session_date=rehearsal.session)
            unit.cursor.execute("SELECT (SELECT count(*) FROM rt.portal_snapshot_hot WHERE snapshot_key = %s), "
                                "(SELECT count(*) FROM rt.portal_snapshot_kept WHERE snapshot_key = %s), "
                                "(SELECT count(*) FROM rt.portal_snapshot_hot WHERE snapshot_key = %s), "
                                "(SELECT count(*) FROM rt.portal_snapshot_kept WHERE snapshot_key = %s)",
                                ("REHEARSAL_PROBE", "REHEARSAL_PROBE", "REHEARSAL_PROBE_KEPT",
                                 "REHEARSAL_PROBE_KEPT"))
            written.update(rows=unit.cursor.fetchone(), intraday=intraday.release_class,
                           session_close=session_close.release_class)
            raise ProbeRollback
    assert written == dict(rows=(1, 1, 0, 1), intraday="INTRADAY", session_close="SESSION_CLOSE")
    assert rehearsal.counts() == before
    assert rehearsal.scalar("SELECT count(*) FROM rt.portal_snapshot_kept WHERE snapshot_key IN "
                            "('REHEARSAL_PROBE', 'REHEARSAL_PROBE_KEPT')") == 0


# --- PART C: a process restart between EQUITY_EVIDENCE and PORTAL_PUBLISH (hardened app role) ------------------------

class RollbackPublication(ProviderDatabase):
    """Publishes as the hardened app role, reads the release rows it just wrote and rolls everything
    back: what an uninterrupted PORTAL_PUBLISH would publish for a receipt, without publishing it."""

    def __init__(self, provider, receipt_id) -> None:
        super().__init__(provider, RealtimeDatabaseRole.PUBLICATION)
        self.receipt_id, self.written = receipt_id, None

    @contextmanager
    def transaction(self, *, read_only=None, dict_cursor=False, isolation_level=None):
        with self._provider.transaction(read_only=False, dict_cursor=dict_cursor,
                                        isolation_level=isolation_level) as unit:
            yield unit.cursor
            unit.cursor.execute("SELECT release_id::text, release_class, manifest_sha256 FROM rt.portal_releases "
                                "WHERE receipt_id = %s", (str(self.receipt_id),))
            self.written = unit.cursor.fetchall()
            raise ProbeRollback


def published_identities(snapshots):
    return {(snapshot.snapshot_type, snapshot.snapshot_key):
            (str(snapshot.snapshot_id), snapshot.source_manifest_sha256, snapshot.payload_sha256)
            for snapshot in snapshots}


@pytest.fixture(scope="module")
def w5(rehearsal, hardened, w4):
    """W5, run through the real orchestrator as the hardened app role, with the process dying between
    EQUITY_EVIDENCE and PORTAL_PUBLISH: attempt 1 dies before its work, the restarted process (empty hand-off,
    projection state and loader) publishes in attempt 2 from a cycle rebuilt out of durable rows but dies before
    reporting, and a second restart publishes again in attempt 3."""
    runtime, _ = hardened
    cohort = rehearsal.cohort
    revised = support.revise_last_bars(rehearsal.history, count=2, observed_at=datetime.now(UTC), label="w5")
    rehearsal.source.queue("revisions-w5", revised)
    spec = rehearsal.spec("w5", rehearsal.cutoff(5))
    stubs = ("STOCK_ALERTS", "STRATEGY_PUBLISH")
    options = dict(stub=stubs, max_attempts=3)
    before = rehearsal.counts()
    runtime.crash_before.add(("w5", "PORTAL_PUBLISH", 1))
    with swallow_simulated_crash():
        runtime.run_window(spec, **options)
    died = dict(counts=rehearsal.counts(), rows=rehearsal.stage_rows("w5"), state=rehearsal.window_state("w5"))
    # What an uninterrupted PORTAL_PUBLISH would publish: the hand-off cycle of the evidence stage, published for
    # the durable receipt inside a transaction that is rolled back.
    cycle = runtime.environment.handoff.peek(cohort, spec.cutoff)
    assert cycle is not None
    receipt_row = rehearsal.q(
        "SELECT receipt.receipt_id, receipt.universe_sha256 FROM rt.cycle_receipts AS receipt "
        "JOIN rt.window_runs AS window_run USING (window_id) WHERE window_run.cohort_id = %s "
        "AND window_run.window_key = 'w5'", (str(cohort),))[0]
    receipt = PortalReleaseReceipt(receipt_row[0], cohort, ProviderTier.MASSIVE_DEVELOPER, spec.cutoff, receipt_row[1])
    twin = RollbackPublication(runtime.provider, receipt.receipt_id)
    with pytest.raises(ProbeRollback):
        publish_realtime_ticker_cycle(twin, cycle, receipt=receipt, session_close=False)
    uninterrupted = dict(
        snapshots=published_identities((*cycle.projection.snapshots, cycle.summary, cycle.pattern_publication)),
        releases=twin.written, counts=rehearsal.counts(), input_sha256=inputs_sha256(cycle.inputs))
    runtime.restart()
    runtime.crash_after.add(("w5", "PORTAL_PUBLISH", 2))
    with swallow_simulated_crash():
        runtime.run_window(spec, **options)
    first_recovery = dict(
        counts=rehearsal.counts(), rows=rehearsal.stage_rows("w5"), release=current_release(rehearsal)[0],
        loader=runtime.environment.screening_loader.last_report, state=len(runtime.environment.ticker_state),
        handoff=len(runtime.environment.handoff))
    runtime.restart()
    report, seconds = runtime.run_window(spec, **options)
    return Outcome(spec, report, seconds, extra=dict(
        before=before, died=died, uninterrupted=uninterrupted, first_recovery=first_recovery,
        counts=rehearsal.counts(), release=current_release(rehearsal)[0], receipt=receipt, revised=revised))


def test_c1_a_restart_between_the_evidence_and_publication_stages_recovers_the_identical_release(
        rehearsal, hardened, w5):
    runtime, _ = hardened
    print("\nW5 rows", rehearsal.stage_rows("w5"), "seconds", round(w5.seconds, 1))
    before, died, expected = w5.extra["before"], w5.extra["died"], w5.extra["uninterrupted"]
    cohort = str(rehearsal.cohort)
    # The process died after EQUITY_EVIDENCE had committed and before PORTAL_PUBLISH did anything: the receipt,
    # the evidence and the screening generation are durable, nothing is published.
    assert {stage: (attempt, state) for stage, attempt, state, _ in died["rows"]} == {
        "EQUITY_INGEST": (1, "COMPLETE"), "EQUITY_EVIDENCE": (1, "COMPLETE"), "PORTAL_PUBLISH": (1, "RUNNING")}
    assert died["counts"]["rt.cycle_receipts"] == before["rt.cycle_receipts"] + 1
    assert died["counts"]["rt.screening_generations"] == before["rt.screening_generations"] + 1
    for table in ("rt.snapshot_catalog", "rt.portal_releases"):
        assert died["counts"][table] == before[table], table
    # The twin published the uninterrupted cycle for the same receipt and rolled back: one intraday release.
    assert len(expected["releases"]) == 1 and expected["releases"][0][1] == "INTRADAY"
    assert expected["counts"] == died["counts"]
    assert expected["input_sha256"] == receipt_input_sha(rehearsal, w5.spec)
    # Attempt 2 ran in a process with nothing in memory: it rebuilt the cycle from durable rows (the loader and
    # the projection state were never touched), published, and died before reporting.
    recovery = w5.extra["first_recovery"]
    assert recovery["loader"] is None and recovery["state"] == 0 and recovery["handoff"] == 0
    assert {(stage, attempt): state for stage, attempt, state, _ in recovery["rows"]}[("PORTAL_PUBLISH", 1)] == "FAILED"
    after = recovery["counts"]
    assert after["rt.snapshot_catalog"] == before["rt.snapshot_catalog"] + len(TICKERS) + 2
    assert after["rt.portal_releases"] == before["rt.portal_releases"] + 1
    for table in ("rt.bar_catalog", "rt.bar_revisions", "rt.bar_current", "rt.evidence_catalog", "rt.equity_evidence",
                  "rt.cycle_receipts", "rt.screening_generations", "rt.materialization_jobs"):
        assert after[table] == died["counts"][table], table  # the recovery is read-only apart from the release
    # Identical identities: every snapshot (id, manifest and payload digests) and the release itself are the ones
    # the uninterrupted run would have published.
    stored = {(row[0], row[1]): (row[2], row[3], row[4]) for row in rehearsal.q(
        "SELECT snapshot_type, snapshot_key, snapshot_id::text, manifest_sha256, payload_sha256 "
        "FROM rt.snapshot_catalog WHERE cohort_id = %s AND generated_at = %s", (cohort, w5.spec.cutoff))}
    assert stored == expected["snapshots"] and len(stored) == len(TICKERS) + 2
    assert rehearsal.q("SELECT release_id::text, release_class, manifest_sha256 FROM rt.portal_releases "
                       "WHERE receipt_id = %s", (str(w5.extra["receipt"].receipt_id),)) == expected["releases"]
    assert w5.extra["release"] == w5.extra["first_recovery"]["release"] == expected["releases"][0][0]
    # The retry after a second restart (attempt 3) rebuilt and published the same cycle again: no row of any kind.
    (report,) = w5.report.windows
    assert w5.report.window_errors == {} and report.window_state == "COMPLETE", rehearsal.stage_rows("w5")
    rows = {(stage, attempt): (state, codes) for stage, attempt, state, codes in rehearsal.stage_rows("w5")}
    assert rows[("PORTAL_PUBLISH", 1)] == ("FAILED", ("OWNER_LOST",))
    assert rows[("PORTAL_PUBLISH", 2)] == ("FAILED", ("OWNER_LOST",))
    assert rows[("PORTAL_PUBLISH", 3)] == ("COMPLETE", ())
    assert w5.extra["counts"] == recovery["counts"]
    assert rehearsal.scalar("SELECT count(*) FROM rt.portal_releases WHERE receipt_id = %s",
                            (str(w5.extra["receipt"].receipt_id),)) == 1
    # Publication needed nothing beyond the grants of app: the rebuild is a read-only transaction.
    assert "PORTAL_PUBLISH" in runtime.stages_used("w5")


def test_c2_a_cycle_that_is_not_the_one_the_receipt_names_is_refused_and_writes_nothing(rehearsal, hardened, w5):
    runtime, _ = hardened
    before = rehearsal.counts()
    assert rehearsal.scalar("SELECT reference_generation FROM rt.cycle_receipts WHERE receipt_id = %s",
                            (str(w5.extra["receipt"].receipt_id),)) == 2
    publication = ProviderDatabase(runtime.provider, RealtimeDatabaseRole.PUBLICATION)
    scope = dict(cohort_id=rehearsal.cohort, session=rehearsal.session, cutoff=w5.spec.cutoff,
                 known_by=w5.spec.deadline, source_generation=2, universe_sha256=w5.extra["receipt"].universe_sha256)
    rebuilt = ticker_repository.rehydrate_realtime_ticker_cycle(
        publication, input_sha256=receipt_input_sha(rehearsal, w5.spec), **scope)
    assert published_identities((*rebuilt.projection.snapshots, rebuilt.summary, rebuilt.pattern_publication)) == (
        w5.extra["uninterrupted"]["snapshots"])
    with pytest.raises(ticker_repository.CycleRehydrationMismatch):
        ticker_repository.rehydrate_realtime_ticker_cycle(publication, input_sha256="0" * 64, **scope)
    assert rehearsal.counts() == before


def test_d1_stages_v2_runs_the_two_ticker_input_alert_projection_and_shared_release_end_to_end(
    rehearsal, hardened, w5, monkeypatch,
):
    import realtime.stage_runners_equity as equity_runners

    monkeypatch.setattr(
        equity_runners,
        "finalize_five_minute_inputs",
        lambda *args, **kwargs: ("COMPLETE", ()),
    )
    runtime, _ = hardened
    enabled = frozenset({
        "EQUITY_INGEST",
        "EQUITY_MATERIALIZE",
        "STOCK_ALERTS",
        "EQUITY_PROJECTION",
        "PORTAL_PUBLISH",
    })
    spec = rehearsal.spec("v2-small", rehearsal.cutoff(6))
    plan_id = uuid4()
    rehearsal.admin(
        "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, "
        "enabled_stages, config_sha256, deadline, scope_reference) "
        "VALUES (%s,%s,%s,'stages_v2',%s,%s,%s,'two-ticker stages_v2 rehearsal')",
        (
            str(plan_id),
            str(rehearsal.cohort),
            rehearsal.session,
            sorted(enabled),
            uuid4().hex + uuid4().hex,
            spec.deadline + timedelta(hours=1),
        ),
    )
    environment = RunnerEnvironment(runtime.provider, build_id="rehearsal-stages-v2")
    runners = dict(build_runners(enabled, runtime.provider, environment=environment))
    runners["EQUITY_INGEST"] = lambda context: StageOutcome.complete()
    repository = PostgresOrchestrationRepository(runtime.provider)
    window = repository.ensure_window(plan_id, spec)
    clock = Clock(spec.release_at + timedelta(seconds=1))
    report = StageOrchestrator(
        repository,
        runners,
        database=runtime.provider,
        stages=STAGES_V2,
        catalog_version="stages_v2",
        clock=clock,
        max_lane_seconds=1_500.0,
        lane_grace_seconds=1_200.0,
    ).run_window(window.window_id)

    rows = {
        stage: state
        for stage, _attempt, state, _reasons in rehearsal.stage_rows("v2-small")
    }
    assert report.window_state == "COMPLETE"
    assert rows == {stage: "COMPLETE" for stage in enabled}
    receipts = rehearsal.q(
        "SELECT receipt_kind, receipt_id::text FROM rt.cycle_receipts WHERE window_id=%s ORDER BY receipt_kind",
        (str(window.window_id),),
    )
    assert {row[0] for row in receipts} == {INPUT_READY, PORTAL_READY}
    input_receipt = next(row[1] for row in receipts if row[0] == INPUT_READY)
    release = current_release(rehearsal)
    assert release[2:4] == (input_receipt, spec.cutoff)
    assert (STOCK_ALERTS_VERSION, "PLATFORM") in release[4]
    assert {(TICKER_WORKSPACE_VERSION, ticker) for ticker in TICKERS} <= set(release[4])


class AdvancedFakeConnection:
    def __init__(self, incoming):
        self.incoming = deque(incoming)
        self.sent = []
        self.closed = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False

    async def send(self, payload):
        self.sent.append(json.loads(payload))

    async def recv(self, *, decode=None):
        assert decode is False
        return self.incoming.popleft()

    async def close(self, *, code, reason):
        self.closed = (code, reason)


def _advanced_status(status):
    return json.dumps(
        [{"ev": "status", "status": status, "message": status}],
        separators=(",", ":"),
    ).encode("ascii")


def _seed_advanced_rehearsal(
    rehearsal,
    *,
    cutoff,
    enabled,
    stage_catalog_version="stages_v2",
):
    profile_id, manifest_id, cohort_id, plan_id = uuid4(), uuid4(), uuid4(), uuid4()
    session_id, owner_id = uuid4(), uuid4()
    authorization_id, ingress_plan_id = uuid4(), uuid4()
    digest = lambda: uuid4().hex + uuid4().hex  # noqa: E731
    born = rehearsal.opened - timedelta(hours=1)
    runtime_profile_sha256 = digest()
    runtime_policy_sha256 = digest()
    authorization_scope_sha256 = digest()
    configuration_sha256 = digest()
    universe_sha256, allocation_policy_sha256 = rehearsal.q(
        "SELECT manifest.universe_sha256, manifest.allocation_policy_sha256 "
        "FROM rt.cohorts AS cohort JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
        "WHERE cohort.cohort_id=%s",
        (str(rehearsal.cohort),),
    )[0]
    with rehearsal.cursor() as cursor:
        cursor.execute(
            "INSERT INTO rt.capability_profiles ("
            "profile_id, config_sha256, schema_sha256, account_alias, capabilities_sha256, "
            "universe_sha256, checked_at, expires_at, status, profile_sha256, provider_tier, "
            "security_policy_sha256, preflight_mode, provider_requests, action_authorized, created_at"
            ") VALUES (%s,%s,%s,'advanced-e2e',%s,%s,%s,%s,'PASS',%s,'MASSIVE_ADVANCED',"
            "%s,'PASSIVE',false,false,%s)",
            (
                str(profile_id),
                configuration_sha256,
                digest(),
                digest(),
                universe_sha256,
                born,
                born + timedelta(days=30),
                digest(),
                digest(),
                born,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.subscription_manifests ("
            "manifest_id, manifest_sha256, universe_sha256, session_date, effective_at, "
            "profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
            "allocation_policy_sha256, created_at"
            ") VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',%s,0,%s,%s,%s)",
            (
                str(manifest_id),
                digest(),
                universe_sha256,
                rehearsal.session,
                born,
                str(profile_id),
                len(TICKERS),
                len(TICKERS),
                allocation_policy_sha256,
                born,
            ),
        )
        for priority, ticker in enumerate(TICKERS, start=1):
            cursor.execute(
                "INSERT INTO rt.subscription_members ("
                "manifest_id, instrument_id, channel, priority, purpose, option_underlying"
                ") VALUES (%s,%s,'AM',%s,'PRIMARY',false)",
                (str(manifest_id), rehearsal.instrument_ids[ticker], priority),
            )
        cursor.execute(
            "INSERT INTO rt.cohorts ("
            "cohort_id, cohort_sha256, manifest_id, configuration_sha256, model_bundle_sha256, "
            "valuation_sha256, effective_at, mode, created_at"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW',%s)",
            (
                str(cohort_id),
                digest(),
                str(manifest_id),
                configuration_sha256,
                digest(),
                digest(),
                born,
                born,
            ),
        )
        calendar = exchange_calendars.get_calendar("XNYS")
        previous_session = calendar.previous_session(str(rehearsal.session)).date()
        coverage_start = calendar.session_offset(
            str(previous_session),
            -282,
        ).date()
        for ticker in TICKERS:
            cursor.execute(
                "INSERT INTO ref.corporate_action_coverage ("
                "coverage_id, security_id, action_type, window_start, window_end, "
                "source, source_key, response_action_count, response_sha256, "
                "observed_at, received_at, payload_sha256"
                ") VALUES (%s,%s,'SPLIT',%s,%s,'synthetic','advanced-e2e',0,%s,%s,%s,%s)",
                (
                    str(
                        uuid5(
                            NAMESPACE_URL,
                            f"advanced-e2e-coverage:{cohort_id}:{ticker}",
                        )
                    ),
                    str(rehearsal.security_ids[ticker]),
                    coverage_start,
                    rehearsal.session,
                    digest(),
                    cutoff - timedelta(hours=1),
                    cutoff - timedelta(hours=1),
                    digest(),
                ),
            )
        cursor.execute(
            "INSERT INTO rt.service_plans ("
            "plan_id, cohort_id, session_date, stage_catalog_version, enabled_stages, "
            "config_sha256, deadline, scope_reference"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,'synthetic Advanced e2e')",
            (
                str(plan_id),
                str(cohort_id),
                rehearsal.session,
                stage_catalog_version,
                sorted(enabled),
                digest(),
                datetime.now(UTC) + timedelta(hours=1),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.advanced_ingress_runtime_profiles ("
            "runtime_profile_sha256,runtime_profile_version,universe_target,"
            "ladder_position,provider_tier,feed,channels,profile_document,"
            "configuration_sha256,runtime_policy_sha256,status,"
            "created_at"
            ") VALUES (%s,%s,%s,0,'MASSIVE_ADVANCED','STOCKS',"
            "ARRAY['AM']::text[],'{}'::jsonb,%s,%s,'CANDIDATE',%s)",
            (
                runtime_profile_sha256,
                f"ew-01-profile-{runtime_profile_sha256[:12]}",
                len(TICKERS),
                configuration_sha256,
                runtime_policy_sha256,
                born,
            ),
        )
        cursor.execute(
            "UPDATE rt.advanced_ingress_runtime_profiles SET status='ACTIVE',"
            "stability_evidence_sha256=%s,capacity_evidence_sha256=%s,"
            "activation_approval_reference='ew-01-rehearsal',activated_at=%s "
            "WHERE runtime_profile_sha256=%s",
            (
                digest(),
                digest(),
                born,
                runtime_profile_sha256,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.advanced_stream_authorizations ("
            "authorization_id,authorization_reference,scope_sha256,provider,"
            "feed,channel,allowed_universe_targets,valid_from,valid_through,"
            "maximum_daily_stream_sessions,maximum_daily_connection_attempts,"
            "maximum_daily_decoded_bytes,maximum_daily_rest_repair_requests,"
            "maximum_daily_rest_repair_decoded_bytes,runtime_policy_sha256,"
            "approval_source,approval_reference,status,created_at"
            ") VALUES (%s,%s,%s,'massive','STOCKS','AM',%s,%s,%s,2,2,"
            "1000000,0,0,%s,'synthetic','ew-01-rehearsal','READY',%s)",
            (
                str(authorization_id),
                f"ew-01-{authorization_id}",
                authorization_scope_sha256,
                [len(TICKERS)],
                rehearsal.session,
                rehearsal.session,
                runtime_policy_sha256,
                born,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.advanced_ingress_plans ("
            "plan_id,plan_sha256,authorization_id,authorization_scope_sha256,"
            "service_plan_id,cohort_id,manifest_id,session_date,universe_target,"
            "mode,runtime_profile_version,runtime_profile_sha256,"
            "configuration_sha256,runtime_policy_sha256,authorized_start_at,"
            "authorized_end_at,maximum_stream_sessions,"
            "maximum_connection_attempts,maximum_decoded_bytes,"
            "maximum_rest_repair_requests,"
            "maximum_rest_repair_decoded_bytes,status,created_at,activated_at"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'SHADOW',%s,%s,%s,%s,"
            "%s,%s,2,2,1000000,0,0,'READY',%s,NULL)",
            (
                str(ingress_plan_id),
                digest(),
                str(authorization_id),
                authorization_scope_sha256,
                str(plan_id),
                str(cohort_id),
                str(manifest_id),
                rehearsal.session,
                len(TICKERS),
                f"ew-01-profile-{runtime_profile_sha256[:12]}",
                runtime_profile_sha256,
                configuration_sha256,
                runtime_policy_sha256,
                rehearsal.opened,
                rehearsal.closed + timedelta(hours=1),
                born,
            ),
        )
        cursor.execute(
            "UPDATE rt.advanced_ingress_plans SET status='ACTIVE',"
            "activated_at=%s WHERE plan_id=%s",
            (born, str(ingress_plan_id)),
        )
        cursor.execute(
            "INSERT INTO rt.advanced_ingress_plan_usage (plan_id) "
            "VALUES (%s)",
            (str(ingress_plan_id),),
        )
        cursor.execute(
            "INSERT INTO rt.stream_sessions ("
            "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
            "started_at,subscribed_at,status,advanced_ingress_plan_id"
            ") VALUES (%s,'synthetic-advanced','STOCKS',%s,%s,7,%s,%s,"
            "'LIVE',%s)",
            (
                str(session_id),
                str(manifest_id),
                str(owner_id),
                cutoff - timedelta(minutes=10),
                cutoff - timedelta(minutes=9),
                str(ingress_plan_id),
            ),
        )
    return SimpleNamespace(
        cohort_id=cohort_id,
        plan_id=plan_id,
        manifest_id=manifest_id,
        session_id=session_id,
        ingress_plan_id=ingress_plan_id,
    )


def _advanced_history_facts(
    rehearsal,
    *,
    cohort_id,
    cutoff,
    stream_session_id,
    journal_generation,
):
    current_facts = tuple(
        fact for ticker in TICKERS for fact in rehearsal.history[ticker]
    )
    previous_session = exchange_calendars.get_calendar("XNYS").previous_session(
        str(rehearsal.session)
    ).date()
    previous_history = support.build_history(
        previous_session,
        TICKERS,
        cohort_id=rehearsal.cohort,
        instrument_ids=rehearsal.instrument_ids,
        fetched_at=rehearsal.started,
    )
    current_keys = {
        (
            fact.revision.ticker,
            fact.revision.interval,
            fact.revision.adjusted,
            fact.revision.bar_start,
        )
        for fact in current_facts
    }
    previous_by_id = {
        fact.revision.bar_revision_id: fact
        for ticker in TICKERS
        for fact in previous_history[ticker]
    }
    extra_ids = {
        revision_id
        for revision_id, fact in previous_by_id.items()
        if (
            fact.revision.ticker,
            fact.revision.interval,
            fact.revision.adjusted,
            fact.revision.bar_start,
        )
        not in current_keys
    }
    pending = list(extra_ids)
    while pending:
        fact = previous_by_id[pending.pop()]
        for source_id in fact.revision.source_bar_revision_ids:
            if source_id in previous_by_id and source_id not in extra_ids:
                extra_ids.add(source_id)
                pending.append(source_id)
    all_facts = (
        *current_facts,
        *(previous_by_id[revision_id] for revision_id in sorted(extra_ids, key=str)),
    )
    all_ids = {fact.revision.bar_revision_id for fact in all_facts}
    window_start = cutoff - timedelta(minutes=5)
    selected = {
        fact.revision.bar_revision_id: fact
        for fact in all_facts
        if fact.revision.bar_end <= cutoff
        and not (
            fact.revision.interval == "1m"
            and fact.revision.bar_start >= window_start
        )
        and not (
            fact.revision.interval == "5m"
            and fact.revision.bar_start == window_start
        )
    }
    changed = True
    while changed:
        changed = False
        for revision_id, fact in tuple(selected.items()):
            if (
                fact.revision.source_kind is BarSourceKind.DERIVED
                and any(
                    source in all_ids and source not in selected
                    for source in fact.revision.source_bar_revision_ids
                )
            ):
                del selected[revision_id]
                changed = True
    mapped_ids = {
        revision_id: uuid5(
            NAMESPACE_URL,
            f"advanced-e2e-history:{cohort_id}:{revision_id}",
        )
        for revision_id in selected
    }
    native_facts = []
    derived_winners = []
    for source_offset, (old_id, fact) in enumerate(
        sorted(
            selected.items(),
            key=lambda item: (
                item[1].revision.bar_end,
                item[1].revision.ticker,
                item[1].revision.interval,
                str(item[0]),
            ),
        ),
        start=1,
    ):
        revision = fact.revision
        source_kind = revision.source_kind
        lineage_ids = tuple(
            mapped_ids.get(source, source)
            for source in revision.source_bar_revision_ids
        )
        if source_kind is not BarSourceKind.DERIVED:
            historical_revision = replace(
                revision,
                bar_revision_id=mapped_ids[old_id],
                source_kind=BarSourceKind.REALTIME_STREAM,
                ingestion_segment_id=None,
                source_bar_revision_ids=(),
                supersedes_bar_revision_id=None,
                payload_sha256=canonical_sha256(
                    {
                        "contract": "advanced-e2e-history-v1",
                        "cohort_id": cohort_id,
                        "source_revision_id": old_id,
                        "source_payload_sha256": revision.payload_sha256,
                    }
                ),
            )
            native_facts.append(
                RealtimeEquityBarFact(
                    cohort_id=cohort_id,
                    provider_tier=ProviderTier.MASSIVE_ADVANCED,
                    instrument_id=fact.instrument_id,
                    revision=historical_revision,
                    journal_generation=journal_generation,
                    stream_session_id=stream_session_id,
                    source_offset=source_offset,
                )
            )
            continue
        rewritten = replace(
            revision,
            bar_revision_id=mapped_ids[old_id],
            ingestion_segment_id=uuid5(
                NAMESPACE_URL,
                f"advanced-e2e-history-segment:{cohort_id}:{revision.ticker}",
            ),
            source_bar_revision_ids=lineage_ids,
            supersedes_bar_revision_id=None,
            payload_sha256=canonical_sha256(
                {
                    "contract": "advanced-e2e-history-v1",
                    "cohort_id": cohort_id,
                    "source_revision_id": old_id,
                    "source_payload_sha256": revision.payload_sha256,
                }
            ),
        )
        derived_winners.append(
            adapt_derived_equity_bar(
                rewritten,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                instrument_id=fact.instrument_id,
            )
        )
    return tuple((*native_facts, *derived_winners))


def _persist_advanced_history(rehearsal, facts):
    with rehearsal.cursor() as cursor:
        for offset in range(0, len(facts), 1_000):
            persist_realtime_equity_bars_cursor(
                cursor,
                facts[offset : offset + 1_000],
            )


def _advanced_payloads(cutoff):
    payloads = []
    sequence = 1
    for minute in range(5):
        start = cutoff - timedelta(minutes=5 - minute)
        end = start + timedelta(minutes=1)
        for ticker_index, ticker in enumerate(TICKERS):
            base = 180 + ticker_index * 20 + minute
            payloads.append(
                (
                    '[{"ev":"AM","sym":"'
                    + ticker
                    + '","o":'
                    + str(base)
                    + ',"h":'
                    + str(base + 2)
                    + ',"l":'
                    + str(base - 1)
                    + ',"c":'
                    + str(base + 1)
                    + ',"v":1000,"vw":'
                    + str(base + 0.5)
                    + ',"s":'
                    + str(int(start.timestamp() * 1000))
                    + ',"e":'
                    + str(int(end.timestamp() * 1000))
                    + ',"q":'
                    + str(sequence)
                    + "}]"
                ).encode("ascii")
            )
            sequence += 1
    payloads.append(
        (
            '[{"ev":"Q","sym":"'
            + TICKERS[0]
            + '","bp":200,"ap":201,"bs":10,"as":11,"t":'
            + str(int(cutoff.timestamp() * 1000))
            + ',"q":'
            + str(sequence)
            + "}]"
        ).encode("ascii")
    )
    return tuple(payloads)


def _run_advanced_fake_gateway(*, payloads, session_id, received_at):
    manifest = SubscriptionManifest(
        version="advanced-e2e-v1",
        session_date=received_at.date(),
        universe=UNIVERSE,
        capability_profile_sha256="a" * 64,
        allocation_policy_sha256="b" * 64,
        members=tuple(
            SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
            for ticker in TICKERS
            for channel in (
                MarketChannel.TRADE,
                MarketChannel.QUOTE,
                MarketChannel.MINUTE_AGGREGATE,
            )
        ),
    )
    incoming = (
        _advanced_status("auth_success"),
        _advanced_status("success"),
        *payloads,
    )
    connection = AdvancedFakeConnection(incoming)
    buffer = RawMessageBuffer(
        BufferLimits(
            maximum_events=len(payloads),
            maximum_bytes=sum(len(payload) for payload in payloads),
            maximum_age_ns=1_000_000,
        ),
        monotonic_ns=lambda: 100,
    )
    gateway = AdvancedGateway(
        GatewayConfiguration.stocks(),
        manifest,
        buffer,
        api_key="synthetic-never-print",
        connection_factory=lambda _: connection,
        utc_clock=lambda: received_at,
        monotonic_ns=lambda: 100,
        session_id_factory=lambda: session_id,
    )
    result = asyncio.run(
        gateway.run_session(maximum_market_messages=len(payloads))
    )
    assert result.provider_requests is False
    assert result.accepted_messages == len(payloads)
    assert buffer.snapshot().admitted_events == len(payloads)
    return buffer


def _drain_advanced_buffer(buffer, writer):
    commits = []
    while True:
        commit = drain_raw_buffer_once(
            buffer,
            writer,
            maximum_events=4,
            maximum_bytes=4_096,
        )
        if commit is None:
            return tuple(commits)
        commits.append(commit)


def _restart_advanced_projection(environment):
    environment.handoff = CycleHandoff()
    environment.ticker_state = TickerProjectionState()
    environment.screening_loader = ScreeningInputLoader()


def _advanced_api_app(*, api_pool, api_role, clock):
    plan = RealtimeProcessDatabasePlan(
        "advanced-e2e-api",
        RealtimeDatabaseRole.READER,
        RealtimePoolBudget(1, 2),
    )
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.API: api_pool},
        {RealtimeDatabaseRole.READER: api_role},
        statement_timeout_ms=60_000,
    )
    database = RealtimeProcessDatabase(plan, provider)
    repositories = build_realtime_portal_repositories(database)
    return create_realtime_portal_app(repositories, clock=lambda: clock)


def _read_advanced_api_publication(app, cohort_id):
    cohort = str(cohort_id)
    calls = (
        ("screening_catalog", "GET", "/api/stocks/screening/catalog", {"cohort_id": cohort}, None),
        ("screening_query", "POST", "/api/stocks/screening/query", {"cohort_id": cohort}, {}),
        ("stock_alerts", "GET", "/api/stocks/alert-view", {"cohort_id": cohort}, None),
        ("workspace", "GET", "/api/tickers/workspace-publication", {"cohort_id": cohort}, None),
        *tuple(
            (
                f"ticker:{ticker}",
                "GET",
                f"/api/stock/{ticker}/workspace-publication",
                {"cohort_id": cohort},
                None,
            )
            for ticker in TICKERS
        ),
        *tuple(
            (
                f"chart:{ticker}",
                "GET",
                f"/api/stock/{ticker}/chart",
                {"cohort_id": cohort, "interval": "5m", "period": "5y"},
                None,
            )
            for ticker in TICKERS
        ),
        ("dashboard", "GET", "/api/tickers/overview", {"cohort_id": cohort}, None),
    )
    publication = {}
    with TestClient(app) as client:
        for name, method, path, params, body in calls:
            response = client.request(method, path, params=params, json=body)
            publication[name] = (
                response.status_code,
                response.content,
                response.json(),
            )
    return publication


def test_d2_synthetic_advanced_bytes_restart_through_real_api_to_one_exact_shared_release(
    rehearsal, roles, hardened, w5, tmp_path,
):
    runtime, pools = hardened
    provider = runtime.provider
    enabled = frozenset(
        {
            "EQUITY_INPUT_GATE",
            "EQUITY_MATERIALIZE",
            "STOCK_ALERTS",
            "EQUITY_PROJECTION",
            "PORTAL_PUBLISH",
        }
    )
    cutoff = rehearsal.closed - timedelta(minutes=15)
    seeded = _seed_advanced_rehearsal(
        rehearsal,
        cutoff=cutoff,
        enabled=enabled,
        stage_catalog_version="stages_v3",
    )
    history = _advanced_history_facts(
        rehearsal,
        cohort_id=seeded.cohort_id,
        cutoff=cutoff,
        stream_session_id=seeded.session_id,
        journal_generation=uuid4(),
    )
    _persist_advanced_history(rehearsal, history)

    release_at = datetime.now(UTC) - timedelta(seconds=2)
    checked_at = release_at + timedelta(seconds=1)
    spec = WindowSpec(
        seeded.cohort_id,
        ProviderTier.MASSIVE_ADVANCED.value,
        f"advanced-e2e-{seeded.cohort_id}",
        release_at,
        cutoff,
        release_at + timedelta(minutes=5),
    )
    repository = PostgresOrchestrationRepository(provider)
    window = repository.ensure_window(seeded.plan_id, spec)
    repository.upsert_readiness(
        seeded.cohort_id,
        "REFERENCE",
        "stages_v3",
        "READY",
        (),
        "synthetic-existing-seed",
        checked_at,
    )

    payloads = _advanced_payloads(cutoff)
    received_at = cutoff + timedelta(seconds=1)
    journal_generation = uuid4()
    journal_path = tmp_path / "advanced-e2e.sqlite"
    buffer = _run_advanced_fake_gateway(
        payloads=payloads,
        session_id=seeded.session_id,
        received_at=received_at,
    )
    writer = RawJournalWriter(
        journal_path,
        generation=journal_generation,
        clock=lambda: cutoff + timedelta(seconds=2),
    )
    first_commit = drain_raw_buffer_once(
        buffer,
        writer,
        maximum_events=4,
        maximum_bytes=4_096,
    )
    writer.close()
    with RawJournalWriter(
        journal_path,
        generation=journal_generation,
        clock=lambda: cutoff + timedelta(seconds=2),
    ) as writer:
        committed = (first_commit, *_drain_advanced_buffer(buffer, writer))
    duplicate_buffer = _run_advanced_fake_gateway(
        payloads=payloads,
        session_id=seeded.session_id,
        received_at=received_at,
    )
    with RawJournalWriter(
        journal_path,
        generation=journal_generation,
        clock=lambda: cutoff + timedelta(seconds=2),
    ) as writer:
        duplicates = _drain_advanced_buffer(duplicate_buffer, writer)
        reader = RawJournalReader(journal_path)
        admissions = {
            ticker: InstrumentAdmission(
                rehearsal.instrument_ids[ticker],
                ticker,
                ticker,
                AssetClass.EQUITY,
                shard_id,
            )
            for shard_id, ticker in enumerate(TICKERS)
        }
        for raw in reader.read_after(0):
            writer.commit_normalized(normalize_raw_message(raw, admissions))
    reader = RawJournalReader(journal_path)
    assert sum(row.inserted_count for row in committed) == len(payloads)
    assert sum(row.duplicate_count for row in duplicates) == len(payloads)
    assert len(reader.read_after(0)) == len(payloads)
    assert len(reader.read_normalized_after(0)) == len(payloads)

    bridge_config = AdvancedEquityJournalBridgeConfig(
        cohort_id=seeded.cohort_id,
        journal_generation=journal_generation,
        stream_session_id=seeded.session_id,
        owner_epoch=7,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=tuple(range(len(TICKERS))),
        batch_limit=len(payloads) - 1,
    )
    rehearsal.admin(
        "UPDATE rt.stream_sessions SET journal_generation=%s,"
        "normalizer_version=%s,authorization_reference=%s,"
        "budget_scope_sha256=%s,connection_attempt=1,"
        "connection_ceiling=2,decoded_byte_ceiling=1000000,"
        "received_frame_count=%s,received_byte_count=1000,"
        "admitted_market_message_count=%s,"
        "admitted_market_byte_count=1000,last_raw_offset=%s,"
        "last_event_offset=%s,event_time_watermark=%s,"
        "heartbeat_at=%s WHERE session_id=%s",
        (
            str(journal_generation),
            NORMALIZER_VERSION,
            "ew-01-rehearsal",
            "a" * 64,
            len(payloads),
            len(payloads),
            len(payloads),
            len(payloads),
            cutoff,
            cutoff,
            str(seeded.session_id),
        ),
    )
    rehearsal.admin(
        "INSERT INTO rt.advanced_journal_registry ("
        "journal_generation,plan_id,stream_session_id,cohort_id,"
        "manifest_id,session_date,owner_epoch,relative_path,"
        "path_sha256,metadata_sha256,journal_codec,"
        "journal_schema_version,normalizer_version,shard_ids,"
        "shard_map_sha256,status,raw_committed_offset,"
        "normalized_committed_offset,bridge_committed_offset"
        ") VALUES (%s,%s,%s,%s,%s,%s,7,%s,%s,%s,'sqlite-wal',1,"
        "%s,%s,%s,'WRITING',%s,%s,%s)",
        (
            str(journal_generation),
            str(seeded.ingress_plan_id),
            str(seeded.session_id),
            str(seeded.cohort_id),
            str(seeded.manifest_id),
            rehearsal.session,
            f"ew-01/{journal_generation}.sqlite",
            canonical_sha256(str(journal_path)),
            canonical_sha256(
                {
                    "generation": journal_generation,
                    "session": seeded.session_id,
                }
            ),
            NORMALIZER_VERSION,
            list(bridge_config.shard_ids),
            bridge_config.shard_map_sha256,
            len(payloads),
            len(payloads),
            0,
        ),
    )
    bridge_database = V2WorkerDatabase(
        provider,
        RealtimeDatabaseRole.COMPUTE,
    )
    bridged = process_advanced_equity_journal_once(
        bridge_database,
        reader,
        bridge_config,
        checked_at=checked_at,
    )
    assert (
        bridged.persisted_bars,
        bridged.committed_offset,
        bridged.shards_advanced,
    ) == (len(TICKERS) * 5, len(payloads) - 1, len(TICKERS))

    gap_id = uuid4()
    rehearsal.admin(
        "INSERT INTO rt.stream_gaps ("
        "gap_id, session_id, instrument_id, channel, gap_start, gap_end, "
        "journal_generation, shard_id, last_durable_offset, reason, recovery_status"
        ") VALUES (%s,%s,%s,'AM',%s,NULL,%s,0,%s,'SYNTHETIC_OFFSET_GAP','UNRESOLVED')",
        (
            str(gap_id),
            str(seeded.session_id),
            rehearsal.instrument_ids[TICKERS[0]],
            cutoff - timedelta(minutes=1),
            str(journal_generation),
            len(payloads) - 1,
        ),
    )
    restarted_bridge = V2WorkerDatabase(
        provider,
        RealtimeDatabaseRole.COMPUTE,
    )
    with pytest.raises(
        ValueError,
        match="unresolved Advanced equity AM gap",
    ):
        process_advanced_equity_journal_once(
            restarted_bridge,
            RawJournalReader(journal_path),
            bridge_config,
            checked_at=checked_at,
        )
    reference_generation = rehearsal.scalar(
        "SELECT count(*) FROM rt.seed_manifests "
        "WHERE status='READY' AND created_at<=%s",
        (cutoff,),
    )
    with pytest.raises(ValueError, match="unresolved stream gaps"):
        input_ready_identity(
            restarted_bridge,
            cohort_id=seeded.cohort_id,
            cutoff=cutoff,
            known_by=spec.deadline,
            reference_generation=reference_generation,
            advanced_input_gate_sha256="a" * 64,
        )
    assert rehearsal.q(
        "SELECT shard_id, committed_offset FROM rt.consumer_checkpoints "
        "WHERE cohort_id=%s ORDER BY shard_id",
        (str(seeded.cohort_id),),
    ) == [(0, len(payloads) - 1), (1, len(payloads) - 1)]
    rehearsal.admin(
        "UPDATE rt.stream_gaps SET recovery_status='WAIVED',gap_end=%s "
        "WHERE gap_id=%s",
        (cutoff, str(gap_id)),
    )
    resumed = process_advanced_equity_journal_once(
        restarted_bridge,
        RawJournalReader(journal_path),
        bridge_config,
        checked_at=checked_at,
    )
    replayed = process_advanced_equity_journal_once(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        RawJournalReader(journal_path),
        bridge_config,
        checked_at=checked_at,
    )
    assert (resumed.persisted_bars, resumed.committed_offset) == (
        0,
        len(payloads),
    )
    assert (replayed.persisted_bars, replayed.scanned_records) == (0, 0)

    reconnect_session_id = uuid4()
    reconnect_generation = uuid4()
    reconnect_payloads = tuple(
        payload.replace(b'"v":1000', b'"v":1001')
        for payload in payloads[-3:-1]
    )
    reconnect_path = tmp_path / "advanced-e2e-reconnect.sqlite"
    reconnect_buffer = _run_advanced_fake_gateway(
        payloads=reconnect_payloads,
        session_id=reconnect_session_id,
        received_at=checked_at + timedelta(seconds=2),
    )
    with RawJournalWriter(
        reconnect_path,
        generation=reconnect_generation,
        clock=lambda: checked_at + timedelta(seconds=2),
    ) as writer:
        reconnect_commits = _drain_advanced_buffer(
            reconnect_buffer,
            writer,
        )
        reconnect_reader = RawJournalReader(reconnect_path)
        admissions = {
            ticker: InstrumentAdmission(
                rehearsal.instrument_ids[ticker],
                ticker,
                ticker,
                AssetClass.EQUITY,
                shard_id,
            )
            for shard_id, ticker in enumerate(TICKERS)
        }
        for raw in reconnect_reader.read_after(0):
            writer.commit_normalized(normalize_raw_message(raw, admissions))
    assert sum(
        row.inserted_count for row in reconnect_commits
    ) == len(reconnect_payloads)
    reconnect_config = AdvancedEquityJournalBridgeConfig(
        cohort_id=seeded.cohort_id,
        journal_generation=reconnect_generation,
        stream_session_id=reconnect_session_id,
        owner_epoch=8,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=tuple(range(len(TICKERS))),
        batch_limit=len(reconnect_payloads),
    )
    rehearsal.admin(
        "UPDATE rt.stream_sessions SET status='STOPPED',ended_at=%s "
        "WHERE session_id=%s",
        (cutoff, str(seeded.session_id)),
    )
    rehearsal.admin(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,subscribed_at,status,journal_generation,"
        "normalizer_version,authorization_reference,budget_scope_sha256,"
        "connection_attempt,connection_ceiling,decoded_byte_ceiling,"
        "received_frame_count,received_byte_count,"
        "admitted_market_message_count,admitted_market_byte_count,"
        "last_raw_offset,last_event_offset,event_time_watermark,"
        "heartbeat_at,advanced_ingress_plan_id"
        ") VALUES (%s,'synthetic-advanced','STOCKS',%s,%s,8,%s,%s,"
        "'LIVE',%s,%s,'ew-01-rehearsal',%s,2,2,1000000,%s,1000,%s,"
        "1000,%s,%s,%s,%s,%s)",
        (
            str(reconnect_session_id),
            str(seeded.manifest_id),
            str(uuid4()),
            cutoff - timedelta(minutes=2),
            cutoff - timedelta(seconds=90),
            str(reconnect_generation),
            NORMALIZER_VERSION,
            "a" * 64,
            len(reconnect_payloads),
            len(reconnect_payloads),
            len(reconnect_payloads),
            len(reconnect_payloads),
            cutoff,
            cutoff,
            str(seeded.ingress_plan_id),
        ),
    )
    rehearsal.admin(
        "INSERT INTO rt.advanced_journal_registry ("
        "journal_generation,plan_id,stream_session_id,cohort_id,"
        "manifest_id,session_date,owner_epoch,relative_path,"
        "path_sha256,metadata_sha256,journal_codec,"
        "journal_schema_version,normalizer_version,shard_ids,"
        "shard_map_sha256,status,raw_committed_offset,"
        "normalized_committed_offset,bridge_committed_offset"
        ") VALUES (%s,%s,%s,%s,%s,%s,8,%s,%s,%s,'sqlite-wal',1,"
        "%s,%s,%s,'WRITING',%s,%s,0)",
        (
            str(reconnect_generation),
            str(seeded.ingress_plan_id),
            str(reconnect_session_id),
            str(seeded.cohort_id),
            str(seeded.manifest_id),
            rehearsal.session,
            f"ew-01/{reconnect_generation}.sqlite",
            canonical_sha256(str(reconnect_path)),
            canonical_sha256(
                {
                    "generation": reconnect_generation,
                    "session": reconnect_session_id,
                }
            ),
            NORMALIZER_VERSION,
            list(reconnect_config.shard_ids),
            reconnect_config.shard_map_sha256,
            len(reconnect_payloads),
            len(reconnect_payloads),
        ),
    )
    reconnect_result = process_advanced_equity_journal_once(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        RawJournalReader(reconnect_path),
        reconnect_config,
        checked_at=checked_at + timedelta(seconds=3),
    )
    assert reconnect_result.persisted_bars == len(TICKERS)
    assert rehearsal.scalar(
        "SELECT count(DISTINCT revision.journal_generation) "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.cohort_id=%s AND current.interval='1m' "
        "AND current.bar_start>=%s AND current.bar_end<=%s",
        (
            str(seeded.cohort_id),
            cutoff - timedelta(minutes=5),
            cutoff,
        ),
    ) == 2

    build_id = ADVANCED_WINDOW_BUILD_ID
    barrier_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:equity-5m-finalizer:{window.window_id}",
    )
    with restarted_bridge.transaction() as cursor:
        enqueue_five_minute_finalizer_jobs_cursor(
            cursor,
            cohort_id=seeded.cohort_id,
            window_id=window.window_id,
            instrument_ids=tuple(
                rehearsal.instrument_ids[ticker] for ticker in TICKERS
            ),
            checked_at=checked_at,
            policy_sha256=FINALIZER_POLICY_SHA256,
            build_id=build_id,
            barrier_id=barrier_id,
        )
    worker_config = EquityWorkerConfig(
        cohort_id=seeded.cohort_id,
        job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
        lease_owner=uuid4(),
        claim_limit=1,
    )
    first_worker = run_five_minute_finalizer_once(
        restarted_bridge,
        worker_config,
        checked_at=checked_at,
    )
    second_worker = run_five_minute_finalizer_once(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        replace(worker_config, lease_owner=uuid4()),
        checked_at=checked_at,
    )
    idle_worker = run_five_minute_finalizer_once(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        replace(worker_config, lease_owner=uuid4()),
        checked_at=checked_at,
    )
    assert (first_worker.claimed, first_worker.completed) == (1, 1)
    assert (second_worker.claimed, second_worker.completed) == (1, 1)
    assert idle_worker.claimed == 0
    universe_sha256, exact_input_sha256 = input_ready_identity(
        restarted_bridge,
        cohort_id=seeded.cohort_id,
        cutoff=cutoff,
        known_by=spec.deadline,
        reference_generation=reference_generation,
        advanced_input_gate_sha256="a" * 64,
    )
    assert len(universe_sha256) == len(exact_input_sha256) == 64
    with restarted_bridge.transaction(read_only=True, dict_cursor=True) as cursor:
        projection_inputs = ScreeningInputLoader().load(
            cursor,
            cohort_id=seeded.cohort_id,
            session=rehearsal.session,
            cutoff=cutoff,
            source_generation=reference_generation,
            known_by=spec.deadline,
        )
    calendar = exchange_calendars.get_calendar("XNYS")
    previous_session = calendar.previous_session(str(rehearsal.session))
    expected_raw_daily_sessions = [
        stamp.date()
        for stamp in calendar.sessions_in_range(
            calendar.session_offset(previous_session, -19),
            previous_session,
        )
    ]
    expected_adjusted_sessions = [
        stamp.date()
        for stamp in calendar.sessions_in_range(
            calendar.session_offset(previous_session, -282),
            previous_session,
        )
    ]
    for ticker in TICKERS:
        adjusted_sessions = sorted(
            row["session_date"]
            for row in projection_inputs.bars
            if row["ticker"] == ticker
            and row["interval"] == "1d"
            and row["adjustment_basis"] == "ADJUSTED"
        )
        assert adjusted_sessions == expected_adjusted_sessions, (
            ticker,
            len(adjusted_sessions),
            adjusted_sessions[:2],
            adjusted_sessions[-2:],
            expected_adjusted_sessions[:2],
            expected_adjusted_sessions[-2:],
        )
        raw_daily_sessions = sorted(
            row["session_date"]
            for row in projection_inputs.bars
            if row["ticker"] == ticker
            and row["interval"] == "1d"
            and row["adjustment_basis"] == "RAW_UNADJUSTED"
        )
        assert raw_daily_sessions[-20:] == expected_raw_daily_sessions, (
            ticker,
            len(raw_daily_sessions),
            raw_daily_sessions[-20:],
            expected_raw_daily_sessions,
        )

    projection_plan_id = repository.create_plan(
        seeded.cohort_id,
        rehearsal.session,
        (
            "EQUITY_INPUT_GATE",
            "EQUITY_MATERIALIZE",
            "EQUITY_PROJECTION",
        ),
        config_sha256=canonical_sha256(
            {
                "contract": "ew-01-projection-only-v1",
                "cohort_id": seeded.cohort_id,
                "cutoff": cutoff,
            }
        ),
        deadline=spec.deadline + timedelta(minutes=5),
        scope_reference="ew-01 projection-only rehearsal",
        catalog_version="stages_v3",
    )
    projection_window = repository.ensure_window(
        projection_plan_id,
        WindowSpec(
            seeded.cohort_id,
            ProviderTier.MASSIVE_ADVANCED.value,
            f"advanced-projection-only-{cutoff:%Y%m%dT%H%M%SZ}",
            release_at,
            cutoff,
            spec.deadline,
        ),
    )
    developer_universe_sha256 = rehearsal.scalar(
        "SELECT manifest.universe_sha256 FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "WHERE cohort.cohort_id=%s",
        (str(rehearsal.cohort),),
    )
    developer_plan_id = repository.create_plan(
        rehearsal.cohort,
        rehearsal.session,
        ("EQUITY_MATERIALIZE", "EQUITY_PROJECTION"),
        config_sha256=canonical_sha256(
            {
                "contract": "ew-01-developer-lane-v1",
                "cohort_id": rehearsal.cohort,
                "cutoff": cutoff,
            }
        ),
        deadline=spec.deadline + timedelta(minutes=5),
        scope_reference="ew-01 Developer serving lane",
        catalog_version="stages_v2",
    )
    developer_window = repository.ensure_window(
        developer_plan_id,
        WindowSpec(
            rehearsal.cohort,
            ProviderTier.MASSIVE_DEVELOPER.value,
            f"developer-ew01-{cutoff:%Y%m%dT%H%M%SZ}",
            release_at,
            cutoff,
            spec.deadline,
        ),
    )
    repository.upsert_readiness(
        rehearsal.cohort,
        "EQUITY_INGEST",
        "stages_v2",
        "READY",
        (),
        "ew-01-existing-developer-input",
        checked_at,
    )
    developer_job = build_closure_materialization_job(
        job_kind="EW01_ISOLATION",
        cohort_id=rehearsal.cohort,
        instrument_id=rehearsal.instrument_ids[TICKERS[0]],
        interval="5m",
        primary_window_id=developer_window.window_id,
        policy_sha256="d" * 64,
        build_id="ew-01-developer-lane-v1",
        available_at=checked_at,
    )
    developer_barrier_id = uuid5(
        NAMESPACE_URL,
        f"ew-01-developer-barrier:{developer_window.window_id}",
    )
    developer_database = V2WorkerDatabase(
        provider,
        RealtimeDatabaseRole.COMPUTE,
    )
    with developer_database.transaction() as cursor:
        enqueue_materialization_job_cursor(cursor, developer_job)
        open_job_barrier_cursor(
            cursor,
            barrier_id=developer_barrier_id,
            cohort_id=rehearsal.cohort,
            window_id=developer_window.window_id,
            barrier_kind="EW01_ISOLATION",
            universe_sha256=developer_universe_sha256,
            members=(
                BarrierMember(
                    "developer",
                    "e" * 64,
                    developer_job.job_id,
                ),
            ),
        )
    developer_receipts = {
        "EQUITY_MATERIALIZE": CycleReceiptSpec(
            INPUT_READY,
            cutoff,
            reference_generation,
            developer_universe_sha256,
            "f" * 64,
            "ew-01-developer-lane-v1",
        ),
        "EQUITY_PROJECTION": CycleReceiptSpec(
            PORTAL_READY,
            cutoff,
            reference_generation,
            developer_universe_sha256,
            "1" * 64,
            "ew-01-developer-lane-v1",
        ),
    }
    developer_runners = {
        stage_id: (
            lambda _context, receipt=receipt: StageOutcome.complete(
                receipt=receipt
            )
        )
        for stage_id, receipt in developer_receipts.items()
    }
    developer_orchestrator = StageOrchestrator(
        repository,
        developer_runners,
        database=provider,
        stages=STAGES_V2,
        catalog_version="stages_v2",
        clock=Clock(checked_at),
    )
    developer_orchestrator.start(developer_plan_id)
    serving_pointer_before = (
        rehearsal.q(
            "SELECT * FROM rt.portal_cohort_current "
            "ORDER BY provider_tier"
        ),
        rehearsal.q(
            "SELECT * FROM rt.provider_tier_cohorts "
            "ORDER BY provider_tier,cohort_id"
        ),
    )
    projection_environment = RunnerEnvironment(
        provider,
        build_id=build_id,
    )
    projection_runners = dict(
        build_runners(
            {
                "EQUITY_INPUT_GATE",
                "EQUITY_MATERIALIZE",
                "EQUITY_PROJECTION",
            },
            provider,
            environment=projection_environment,
        )
    )
    project = projection_runners["EQUITY_PROJECTION"]

    def crash_after_input_ready(context):
        if context.attempt == 1:
            raise SimulatedCrash("projection process stopped after INPUT_READY")
        return project(context)

    projection_runners["EQUITY_PROJECTION"] = crash_after_input_ready

    def projection_orchestrator(runners=projection_runners):
        return StageOrchestrator(
            repository,
            runners,
            database=provider,
            stages=STAGES_V3,
            catalog_version="stages_v3",
            clock=Clock(checked_at),
            max_attempts=2,
            max_lane_seconds=1_500.0,
            lane_grace_seconds=1_200.0,
        )

    projection_orchestrator().start(projection_plan_id)
    with swallow_simulated_crash():
        projection_orchestrator().run_window(
            projection_window.window_id
        )
    assert rehearsal.q(
        "SELECT receipt_kind FROM rt.cycle_receipts "
        "WHERE window_id=%s ORDER BY receipt_kind",
        (str(projection_window.window_id),),
    ) == [(INPUT_READY,)]
    developer_report = developer_orchestrator.run_window(
        developer_window.window_id
    )
    assert developer_report.window_state == "COMPLETE"
    assert rehearsal.scalar(
        "SELECT state FROM rt.window_runs WHERE window_id=%s",
        (str(projection_window.window_id),),
    ) == "PENDING"
    _restart_advanced_projection(projection_environment)
    projection_report = projection_orchestrator().run_window(
        projection_window.window_id
    )
    assert projection_report.window_state == "COMPLETE"
    assert rehearsal.q(
        "SELECT receipt_kind FROM rt.cycle_receipts "
        "WHERE window_id=%s ORDER BY receipt_kind",
        (str(projection_window.window_id),),
    ) == [(INPUT_READY,), (PORTAL_READY,)]
    projection_state = rehearsal.q(
        "SELECT "
        "(SELECT count(*) FROM rt.equity_evidence "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.screening_generations "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.materialization_jobs "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.portal_releases "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.portal_release_current "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.release_current "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.portal_cohort_current "
        " WHERE cohort_id=%s)",
        tuple(str(seeded.cohort_id) for _ in range(7)),
    )[0]
    assert projection_state[0] > 0
    assert projection_state[1] == 1
    assert projection_state[3:] == (0, 0, 0, 0)
    _restart_advanced_projection(projection_environment)
    assert projection_orchestrator().run_window(
        projection_window.window_id
    ).status == "NOT_CLAIMED"

    with provider.transaction(APP_ROLE, read_only=True) as unit:
        recovery = discover_advanced_window_recovery_cursor(
            unit.cursor,
            cohort_id=seeded.cohort_id,
            cutoff=cutoff,
            checked_at=checked_at,
        )
    assert recovery.complete
    assert recovery.input_receipt is not None
    assert recovery.portal_receipt is not None
    retry_window = repository.ensure_window(
        projection_plan_id,
        WindowSpec(
            seeded.cohort_id,
            ProviderTier.MASSIVE_ADVANCED.value,
            f"advanced-projection-only-{cutoff:%Y%m%dT%H%M%SZ}-retry1",
            release_at,
            cutoff,
            spec.deadline,
        ),
    )
    retry_environment = RunnerEnvironment(
        provider,
        build_id=build_id,
        input_known_by=recovery.input_known_by,
    )
    retry_runners = dict(
        build_runners(
            {
                "EQUITY_INPUT_GATE",
                "EQUITY_MATERIALIZE",
                "EQUITY_PROJECTION",
            },
            provider,
            environment=retry_environment,
        )
    )
    retry_runners["EQUITY_MATERIALIZE"] = (
        reuse_advanced_input_ready_runner(
            retry_environment,
            receipt=recovery.input_receipt,
            input_known_by=recovery.input_known_by,
            expected_gate_sha256=recovery.input_gate_sha256,
        )
    )
    retry_runners["EQUITY_PROJECTION"] = (
        lambda _context: StageOutcome.complete(
            receipt=recovery.portal_receipt,
            evidence_ref=(
                "reused-portal-ready:"
                f"{recovery.portal_receipt.input_sha256}"
            ),
        )
    )
    retry_report = projection_orchestrator(
        retry_runners
    ).run_window(retry_window.window_id)
    assert retry_report.window_state == "COMPLETE"
    assert rehearsal.q(
        "SELECT receipt_kind,reference_generation,universe_sha256,"
        "input_sha256,build_id FROM rt.cycle_receipts "
        "WHERE window_id=ANY(%s::uuid[]) "
        "ORDER BY receipt_kind,window_id",
        (
            [
                str(projection_window.window_id),
                str(retry_window.window_id),
            ],
        ),
    )[::2] == rehearsal.q(
        "SELECT receipt_kind,reference_generation,universe_sha256,"
        "input_sha256,build_id FROM rt.cycle_receipts "
        "WHERE window_id=ANY(%s::uuid[]) "
        "ORDER BY receipt_kind,window_id",
        (
            [
                str(projection_window.window_id),
                str(retry_window.window_id),
            ],
        ),
    )[1::2]
    assert rehearsal.q(
        "SELECT "
        "(SELECT count(*) FROM rt.equity_evidence "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.screening_generations "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.materialization_jobs "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.portal_releases "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.portal_release_current "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.release_current "
        " WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.portal_cohort_current "
        " WHERE cohort_id=%s)",
        tuple(str(seeded.cohort_id) for _ in range(7)),
    )[0] == projection_state
    assert (
        rehearsal.q(
            "SELECT * FROM rt.portal_cohort_current "
            "ORDER BY provider_tier"
        ),
        rehearsal.q(
            "SELECT * FROM rt.provider_tier_cohorts "
            "ORDER BY provider_tier,cohort_id"
        ),
    ) == serving_pointer_before
    assert set(rehearsal.q(
        "SELECT plan.cohort_id,plan.stage_catalog_version,"
        "run.window_id,receipt.receipt_kind "
        "FROM rt.cycle_receipts AS receipt "
        "JOIN rt.window_runs AS run USING(window_id) "
        "JOIN rt.service_plans AS plan USING(plan_id) "
        "WHERE run.window_id=ANY(%s::uuid[]) "
        "ORDER BY plan.stage_catalog_version,receipt.receipt_kind",
        (
            [
                str(developer_window.window_id),
                str(projection_window.window_id),
                str(retry_window.window_id),
            ],
        ),
    )) == {
        (
            rehearsal.cohort,
            "stages_v2",
            developer_window.window_id,
            INPUT_READY,
        ),
        (
            rehearsal.cohort,
            "stages_v2",
            developer_window.window_id,
            PORTAL_READY,
        ),
        (
            seeded.cohort_id,
            "stages_v3",
            projection_window.window_id,
            INPUT_READY,
        ),
        (
            seeded.cohort_id,
            "stages_v3",
            retry_window.window_id,
            INPUT_READY,
        ),
        (
            seeded.cohort_id,
            "stages_v3",
            projection_window.window_id,
            PORTAL_READY,
        ),
        (
            seeded.cohort_id,
            "stages_v3",
            retry_window.window_id,
            PORTAL_READY,
        ),
    }
    assert {
        (row[0], row[1])
        for row in rehearsal.q(
            "SELECT cohort_id,window_id FROM rt.job_barriers "
            "WHERE window_id=ANY(%s::uuid[])",
            (
                [
                    str(developer_window.window_id),
                    str(projection_window.window_id),
                ],
            ),
        )
    } == {
        (rehearsal.cohort, developer_window.window_id),
        (seeded.cohort_id, projection_window.window_id),
    }
    assert {
        (row[0], row[1])
        for row in rehearsal.q(
            "SELECT cohort_id,primary_window_id "
            "FROM rt.materialization_jobs "
            "WHERE primary_window_id=ANY(%s::uuid[])",
            (
                [
                    str(developer_window.window_id),
                    str(projection_window.window_id),
                ],
            ),
        )
    } == {
        (rehearsal.cohort, developer_window.window_id),
        (seeded.cohort_id, projection_window.window_id),
    }

    environment = RunnerEnvironment(provider, build_id=build_id)
    runners = dict(
        build_runners(enabled, provider, environment=environment)
    )
    publish = runners["PORTAL_PUBLISH"]

    def crash_at_publication_boundaries(context):
        if context.attempt == 1:
            raise SimulatedCrash("portal publication died before work")
        outcome = publish(context)
        if context.attempt == 2:
            raise SimulatedCrash("portal publication died after commit")
        return outcome

    runners["PORTAL_PUBLISH"] = crash_at_publication_boundaries

    def orchestrator():
        return StageOrchestrator(
            repository,
            runners,
            database=provider,
            stages=STAGES_V3,
            catalog_version="stages_v3",
            clock=Clock(checked_at),
            max_attempts=3,
            max_lane_seconds=1_500.0,
            lane_grace_seconds=1_200.0,
        )

    assert rehearsal.scalar(
        "SELECT state FROM rt.window_runs WHERE window_id=%s",
        (str(window.window_id),),
    ) == "PENDING"
    with swallow_simulated_crash():
        orchestrator().run_window(window.window_id)
    receipts_after_projection = rehearsal.q(
        "SELECT receipt_kind, count(*) FROM rt.cycle_receipts "
        "WHERE window_id=%s GROUP BY receipt_kind ORDER BY receipt_kind",
        (str(window.window_id),),
    )
    stage_rows_after_projection = rehearsal.q(
        "SELECT stage_id, attempt, state, reason_codes "
        "FROM rt.stage_runs WHERE window_id=%s ORDER BY stage_id, attempt",
        (str(window.window_id),),
    )
    assert receipts_after_projection == [
        (INPUT_READY, 1),
        (PORTAL_READY, 1),
    ], stage_rows_after_projection
    _restart_advanced_projection(environment)
    with swallow_simulated_crash():
        orchestrator().run_window(window.window_id)
    stored_after_commit = {
        "bars": rehearsal.q(
            "SELECT instrument_id, interval, bar_start, count(*) "
            "FROM rt.bar_current WHERE cohort_id=%s "
            "AND interval IN ('1m','5m') AND bar_start>=%s AND bar_end<=%s "
            "GROUP BY instrument_id, interval, bar_start ORDER BY 1,2,3",
            (
                str(seeded.cohort_id),
                cutoff - timedelta(minutes=5),
                cutoff,
            ),
        ),
        "jobs": rehearsal.q(
            "SELECT job_kind, status, count(*) FROM rt.materialization_jobs "
            "WHERE cohort_id=%s GROUP BY job_kind, status ORDER BY 1,2",
            (str(seeded.cohort_id),),
        ),
        "snapshots": rehearsal.q(
            "SELECT snapshot_type, snapshot_key, snapshot_id::text "
            "FROM rt.snapshot_catalog WHERE cohort_id=%s AND generated_at=%s "
            "ORDER BY snapshot_type, snapshot_key",
            (str(seeded.cohort_id), cutoff),
        ),
        "releases": rehearsal.q(
            "SELECT release_id::text, receipt_id::text, manifest_sha256 "
            "FROM rt.portal_releases WHERE cohort_id=%s ORDER BY release_id",
            (str(seeded.cohort_id),),
        ),
        "screening": rehearsal.q(
            "SELECT generation_id::text, source_manifest_sha256, status "
            "FROM rt.screening_generations WHERE cohort_id=%s AND generated_at=%s",
            (str(seeded.cohort_id), cutoff),
        ),
    }
    _restart_advanced_projection(environment)
    report = orchestrator().run_window(window.window_id)
    stored_after_retry = {
        "bars": rehearsal.q(
            "SELECT instrument_id, interval, bar_start, count(*) "
            "FROM rt.bar_current WHERE cohort_id=%s "
            "AND interval IN ('1m','5m') AND bar_start>=%s AND bar_end<=%s "
            "GROUP BY instrument_id, interval, bar_start ORDER BY 1,2,3",
            (
                str(seeded.cohort_id),
                cutoff - timedelta(minutes=5),
                cutoff,
            ),
        ),
        "jobs": rehearsal.q(
            "SELECT job_kind, status, count(*) FROM rt.materialization_jobs "
            "WHERE cohort_id=%s GROUP BY job_kind, status ORDER BY 1,2",
            (str(seeded.cohort_id),),
        ),
        "snapshots": rehearsal.q(
            "SELECT snapshot_type, snapshot_key, snapshot_id::text "
            "FROM rt.snapshot_catalog WHERE cohort_id=%s AND generated_at=%s "
            "ORDER BY snapshot_type, snapshot_key",
            (str(seeded.cohort_id), cutoff),
        ),
        "releases": rehearsal.q(
            "SELECT release_id::text, receipt_id::text, manifest_sha256 "
            "FROM rt.portal_releases WHERE cohort_id=%s ORDER BY release_id",
            (str(seeded.cohort_id),),
        ),
        "screening": rehearsal.q(
            "SELECT generation_id::text, source_manifest_sha256, status "
            "FROM rt.screening_generations WHERE cohort_id=%s AND generated_at=%s",
            (str(seeded.cohort_id), cutoff),
        ),
    }
    assert report.window_state == "COMPLETE"
    assert stored_after_retry == stored_after_commit
    assert all(row[3] == 1 for row in stored_after_retry["bars"])
    assert len(stored_after_retry["bars"]) == len(TICKERS) * 6
    assert stored_after_retry["jobs"] == [
        (EQUITY_INTERVAL_DERIVE_JOB, "COMPLETE", 84),
        (
            EQUITY_INTERVAL_FINALIZE_JOB,
            "COMPLETE",
            len(TICKERS) * 2,
        ),
    ]
    assert len(stored_after_retry["screening"]) == 1
    snapshot_identities = {
        (row[0], row[1]) for row in stored_after_retry["snapshots"]
    }
    assert len(snapshot_identities) == len(stored_after_retry["snapshots"]) == (
        len(TICKERS) + 4
    )
    assert (STOCK_ALERTS_VERSION, "PLATFORM") in snapshot_identities
    assert {
        (TICKER_WORKSPACE_VERSION, ticker) for ticker in TICKERS
    } <= snapshot_identities
    input_receipt = rehearsal.scalar(
        "SELECT receipt_id::text FROM rt.cycle_receipts "
        "WHERE window_id=%s AND receipt_kind=%s",
        (str(window.window_id), INPUT_READY),
    )
    assert {row[1] for row in stored_after_retry["releases"]} == {
        input_receipt
    }
    current = rehearsal.q(
        "SELECT release.release_id::text, release.receipt_id::text, release.cutoff, "
        "release.provider_tier, release.manifest "
        "FROM rt.portal_release_current AS pointer "
        "JOIN rt.portal_releases AS release "
        "ON release.release_id=pointer.release_id AND release.cutoff=pointer.cutoff "
        "WHERE pointer.cohort_id=%s AND pointer.provider_tier='MASSIVE_ADVANCED'",
        (str(seeded.cohort_id),),
    )
    assert len(current) == 1
    release_id, release_receipt, release_cutoff, release_tier, release_manifest = current[0]
    assert (release_receipt, release_cutoff, release_tier) == (
        input_receipt,
        cutoff,
        ProviderTier.MASSIVE_ADVANCED.value,
    )
    release_identities = {
        (entry["snapshot_type"], entry["snapshot_key"])
        for entry in release_manifest["snapshots"]
    }
    assert snapshot_identities == release_identities

    screening_rows = rehearsal.q(
        "SELECT generation_id::text, provider_tier, source_manifest_sha256, manifest "
        "FROM rt.screening_generations WHERE cohort_id=%s AND generated_at=%s",
        (str(seeded.cohort_id), cutoff),
    )
    assert len(screening_rows) == 1
    screening_id, screening_tier, screening_sha256, screening_manifest = screening_rows[0]
    assert screening_tier == ProviderTier.MASSIVE_ADVANCED.value
    assert screening_sha256 == canonical_sha256(screening_manifest)
    assert screening_manifest["source_cutoff"] == cutoff.isoformat()

    snapshot_rows = rehearsal.q(
        "SELECT catalog.snapshot_type, catalog.snapshot_key, catalog.snapshot_id::text, "
        "catalog.provider_tier, catalog.source_generation, "
        "snapshot.payload->>'provider_fetch_performed' "
        "FROM rt.snapshot_catalog AS catalog "
        "JOIN rt.portal_snapshot_hot AS snapshot "
        "ON snapshot.snapshot_id=catalog.snapshot_id "
        "AND snapshot.generated_at=catalog.generated_at "
        "WHERE catalog.cohort_id=%s AND catalog.generated_at=%s "
        "ORDER BY catalog.snapshot_type, catalog.snapshot_key",
        (str(seeded.cohort_id), cutoff),
    )
    expected_snapshots = {
        (snapshot_type, snapshot_key): snapshot_id
        for snapshot_type, snapshot_key, snapshot_id, _tier, _generation, _provider_fetch in snapshot_rows
    }
    assert len(expected_snapshots) == len(TICKERS) + 4
    assert {
        tier for _type, _key, _id, tier, _generation, _provider_fetch in snapshot_rows
    } == {ProviderTier.MASSIVE_ADVANCED.value}
    assert {
        generation for _type, _key, _id, _tier, generation, _provider_fetch in snapshot_rows
    } == {screening_manifest["source_generation"]}
    assert all(
        provider_fetch == "false"
        for _type, _key, _id, _tier, _generation, provider_fetch in snapshot_rows
    )

    storage_counts_before_api = rehearsal.q(
        "SELECT "
        "(SELECT count(*) FROM rt.cycle_receipts WHERE window_id=%s), "
        "(SELECT count(*) FROM rt.snapshot_catalog WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.portal_releases WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.screening_generations WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.materialization_jobs WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.bar_revisions WHERE cohort_id=%s)",
        (
            str(window.window_id),
            *(str(seeded.cohort_id) for _ in range(5)),
        ),
    )[0]
    api_read_at = datetime.now(UTC)
    first_app = _advanced_api_app(
        api_pool=pools["api"],
        api_role=roles.name("api"),
        clock=api_read_at,
    )
    first_api = _read_advanced_api_publication(first_app, seeded.cohort_id)
    assert {
        name: response[0] for name, response in first_api.items()
    } == {
        "screening_catalog": 200,
        "screening_query": 200,
        "stock_alerts": 200,
        "workspace": 200,
        **{f"ticker:{ticker}": 200 for ticker in TICKERS},
        **{f"chart:{ticker}": 200 for ticker in TICKERS},
        "dashboard": 200,
    }

    catalog = first_api["screening_catalog"][2]
    query = first_api["screening_query"][2]
    alerts = first_api["stock_alerts"][2]
    workspace = first_api["workspace"][2]
    assert catalog["status"] == "READY"
    assert [row["generation"] for row in catalog["publications"]] == [
        screening_manifest["generation"]
    ]
    assert query["universe_count"] == query["matched_count"] == len(TICKERS)
    assert query["generation"]["generation"] == screening_manifest["generation"]
    assert query["generation"]["provider_tier"] == screening_tier
    assert query["generation"]["source_cutoff"] == release_cutoff.isoformat()
    assert query["generation"]["source_generation"] == screening_manifest["source_generation"]
    assert alerts["provider_fetch_performed"] is False
    assert set(alerts["missing_producers"]) == {"context", "forward", "replay"}
    assert isinstance(first_api["dashboard"][2], list)

    exposed_snapshot_ids = {
        workspace["publication"]["snapshot_id"],
        *(row["snapshot_id"] for row in workspace["workspaces"]),
        *(first_api[f"ticker:{ticker}"][2]["snapshot_id"] for ticker in TICKERS),
    }
    expected_exposed_snapshot_ids = {
        expected_snapshots[(TICKER_WORKSPACE_SUMMARY_VERSION, "PLATFORM")],
        *(expected_snapshots[(TICKER_WORKSPACE_VERSION, ticker)] for ticker in TICKERS),
    }
    assert exposed_snapshot_ids == expected_exposed_snapshot_ids
    release_snapshot_ids = {
        entry["snapshot_id"] for entry in release_manifest["snapshots"]
    }
    assert exposed_snapshot_ids <= release_snapshot_ids
    assert len(release_snapshot_ids) == len(TICKERS) + 4
    for ticker in TICKERS:
        chart = first_api[f"chart:{ticker}"][2]
        assert len(chart) > 1
        assert chart[-1]["time"] == int((cutoff - timedelta(minutes=5)).timestamp())

    second_app = _advanced_api_app(
        api_pool=pools["api"],
        api_role=roles.name("api"),
        clock=api_read_at,
    )
    second_api = _read_advanced_api_publication(second_app, seeded.cohort_id)
    assert second_api == first_api
    assert rehearsal.q(
        "SELECT "
        "(SELECT count(*) FROM rt.cycle_receipts WHERE window_id=%s), "
        "(SELECT count(*) FROM rt.snapshot_catalog WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.portal_releases WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.screening_generations WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.materialization_jobs WHERE cohort_id=%s), "
        "(SELECT count(*) FROM rt.bar_revisions WHERE cohort_id=%s)",
        (
            str(window.window_id),
            *(str(seeded.cohort_id) for _ in range(5)),
        ),
    )[0] == storage_counts_before_api
    assert str(UUID(release_id)) == release_id
    assert str(UUID(screening_id)) == screening_id
