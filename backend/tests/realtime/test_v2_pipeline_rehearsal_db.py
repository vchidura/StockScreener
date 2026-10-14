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

PART B (hardened security, principals simulated with SET ROLE from the admin connection)
    harden_database_v2 creates the NOLOGIN roles and grants and the exact-privilege audit must end AUDITED.
    The runtime then uses one pool per principal profile (runtime_data, api; no owner pool) whose connections
    are the admin connection with `SET ROLE <principal>` applied at checkout (and RESET ROLE on release), so
    V2ConnectionProvider's per-transaction `SET LOCAL ROLE <capability>` runs inside the principal as in
    production. A new REFERENCE seed (generation 2), a new ingest of restated bars and one full window (W4,
    +4 min) then run under the principals, including the reader's default `current` mode through the `api`
    principal, the screening repository and the kept tier (strategies, session-close releases); out-of-role
    writes are rejected by PostgreSQL (compute -> portal snapshot, publication -> bar, ingress -> bar_current
    update, reader -> anything). W4's strategies only READ (ALREADY_STORED, same market time as W1), so the
    session-close writes (kept-only, hot-and-kept, SESSION_CLOSE release) are probed under the hardened
    `publication` role with probe snapshots and rolled back; W1 took them for real under the owner pool.

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
    * The principals: PostgreSQL lets a superuser session user SET ROLE to any role, so the principal ->
      capability MEMBERSHIP is enforced by the provider (PermissionError) and verified with pg_has_role; the
      capability roles' table grants are enforced by the server (b4 shows the rejections).
    * `created_at` of a bar is the database insert time and cannot be set: a deadline is "passed" for it only by
      real time passing, which is why W1 and W2 have short budgets.
    * STOCK_ALERTS has no context / forward / replay producer: every window reports exactly those three
      MISSING_PRODUCER codes (asserted), the only degraded codes of the whole run.

NOT EXERCISED: provider calls (none exist here), the options stages, a LOGIN principal, the connection-pool
    budget of the real deployment, a strategy FIRST publication under the hardened principals (probed in b5
    instead), a second session (the retained-history input of the rolling strategies is empty) and the dashboard
    publication (no stage publishes it).
"""
from __future__ import annotations

import json
import threading
import time
from collections import Counter, namedtuple
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
import psycopg2.errors
from psycopg2 import sql

import exchange_calendars
import realtime.ticker_repository as ticker_repository
from equity.calendar import latest_expected_market_time
from realtime.contracts import UniverseManifest, canonical_json
from realtime.database_access import RealtimeDatabaseRole
from realtime.database_access_v2 import (
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
    verify_pool_target_v2,
)
from realtime.equity_bar_drain import drain_equity_bar_jobs, lease_owner_for
from realtime.lineage_head import (
    BOUNDED_MANIFEST_BYTES,
    BOUNDED_WORKSPACE_PAYLOAD_BYTES,
    inputs_sha256,
    reference_set,
)
from realtime.orchestration_repository import PostgresOrchestrationRepository, WindowSpec
from realtime.portal_compatibility_repository import RealtimeScreeningRepository, load_session_close_releases
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
from realtime.relation_registry import CAPABILITY_ROLES, PRINCIPAL_PROFILES
from realtime.runner_environment import RunnerEnvironment
from realtime.screening_delta_loader import ScreeningInputLoader
from realtime.screening_projection import RealtimeScreeningInput
from realtime.screening_repository import load_realtime_screening_input
from realtime.security_v2 import (
    build_role_names_v2,
    current_security_phase_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.stage_database_v2 import StageProcessPlan, V2StageDatabase
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome
from realtime.stage_runners_alerts import EVALUATED_ANALYSIS_STATUSES
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
from realtime.ticker_repository import RealtimeTickerWorkspaceRepository, compute_realtime_ticker_cycle
from realtime.v2_service import V2Service
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime import rehearsal_support as support
from tests.realtime.test_ticker_vertical_slice import old_embedded_evidence
from v2_database import _connect, _settings, disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

UTC = timezone.utc
DATABASE = "test_realtime_v2_e2e_a"
ROLE_PREFIX = f"test_rt_e2e_{uuid4().hex[:6]}"
TICKERS = support.FIRST_TICKERS
UNIVERSE = UniverseManifest("rehearsal-v1", TICKERS, ())
ENABLED = frozenset({"REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STRATEGY_PUBLISH",
                     "STOCK_ALERTS"})
WINDOW_STAGES = ("EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STOCK_ALERTS", "STRATEGY_PUBLISH")
Used = namedtuple("Used", "stage window role profile read_only fenced")


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
                         (equity_runners, "publish_realtime_ticker_cycle"),
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
        remaining = _drop_roles(names)  # the database fixture is torn down first, so nothing depends on them
        assert remaining == [], f"disposable roles were left behind: {remaining}"


@pytest.fixture(scope="module")
def pool(roles, anchor):
    with disposable_v2_database(DATABASE) as connection_pool:
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
        self.watermarks_before_drain: dict[str, list] = {}
        self.loads: dict[tuple, object] = {}
        original = provider.transaction

        @contextmanager
        def recording(capability_role, principal_profile, **options):
            where = getattr(ACTIVE, "stage", None) or (None, None)
            with original(capability_role, principal_profile, **options) as unit:
                # recorded once the access layer accepted the (role, profile) pair
                self.transactions.append(Used(where[1], where[0], capability_role, principal_profile,
                                              bool(options.get("read_only")), options.get("fence") is not None))
                yield unit

        provider.transaction = recording
        base = build_runners(ENABLED, provider, environment=self.environment)
        self.runners = {stage: self._instrument(stage, runner) for stage, runner in base.items()}
        self.runners["EQUITY_EVIDENCE"] = self._instrument("EQUITY_EVIDENCE", self._crashable(
            "EQUITY_EVIDENCE", self._watching(base["EQUITY_EVIDENCE"])))
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

    def run_window(self, spec, *, stub=()):
        """The real window through V2Service.poll_windows with the real runner table (`stub` stages are no-ops)."""
        self.clock.value = spec.release_at + timedelta(seconds=1)
        table = {stage: runner for stage, runner in self.runners.items() if stage not in stub}
        service = V2Service(self.orchestrator(table), self.repository, self.rehearsal.plan_id, (spec,),
                            clock=self.clock)
        started = time.perf_counter()
        report = service.poll_windows()
        return report, time.perf_counter() - started

    def stage_times(self, window_key):
        return {stage: round(seconds, 2) for (key, stage, attempt), seconds in sorted(self.timings.items())
                if key == window_key}

    def roles_by_stage(self, window_key=None, *, write=None):
        result = {}
        for used in self.transactions:
            if used.stage is None or (window_key is not None and used.window != window_key):
                continue
            if write is None or write == (not used.read_only):
                result.setdefault(used.stage, set()).add(used.role)
        return result


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
        with runtime.provider.transaction("reader", "api", read_only=True, dict_cursor=True,
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
    with runtime.provider.transaction("compute", "runtime_data", read_only=True, dict_cursor=True) as unit:
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
    with runtime.provider.transaction("compute", "runtime_data", read_only=True, dict_cursor=True) as unit:
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
    through the `api` principal's reader role. The 26-session physical retention is the registry's
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
    pointer on it, and the screening repository (stored-only reader) reading it through `runtime`'s principals:
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

    def __init__(self, provider, role: RealtimeDatabaseRole, profile: str = "runtime_data") -> None:
        self._process_plan = StageProcessPlan(role)
        self._connection_provider = None
        self._provider, self._profile = provider, profile

    @contextmanager
    def transaction(self, *, read_only=None, dict_cursor=False, isolation_level=None):
        with self._provider.transaction(self._process_plan.role.value.lower(), self._profile,
                                        read_only=bool(read_only), dict_cursor=dict_cursor,
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

def test_a8_every_stage_used_exactly_the_capabilities_of_the_registry_and_no_role_violation_was_recorded(rehearsal, w3b):
    runtime = rehearsal.deferred
    assert runtime.provider.role_violations == []
    assert {used.profile for used in runtime.transactions if used.stage is not None} == {"runtime_data"}
    # STRATEGY_PUBLISH writes its evidence under compute and its snapshots and releases under publication, but
    # only in the window that PUBLISHES a strategy (W1: the market time is the session close for every later
    # window of the run too); the later windows find the retained snapshot (ALREADY_STORED) and only read.
    assert runtime.roles_by_stage(write=True) == {
        "REFERENCE": {"references"}, "EQUITY_INGEST": {"ingress"}, "EQUITY_EVIDENCE": {"compute"},
        "PORTAL_PUBLISH": {"publication"}, "STOCK_ALERTS": {"publication"},
        "STRATEGY_PUBLISH": {"compute", "publication"}}
    assert runtime.roles_by_stage("w1", write=True)["STRATEGY_PUBLISH"] == {"compute", "publication"}
    assert all("STRATEGY_PUBLISH" not in runtime.roles_by_stage(window, write=True)
               for window in ("w2", "w3", "w3b"))
    assert all(runtime.roles_by_stage(window)["STRATEGY_PUBLISH"] == {"compute", "publication"}
               for window in ("w2", "w3", "w3b"))
    assert runtime.roles_by_stage() == {
        "REFERENCE": {"references"}, "EQUITY_INGEST": {"ingress"}, "EQUITY_EVIDENCE": {"compute"},
        "PORTAL_PUBLISH": {"publication"}, "STOCK_ALERTS": {"compute", "publication"},
        "STRATEGY_PUBLISH": {"compute", "publication"}}
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

class PrincipalPool:
    """Pool shim for a NOLOGIN principal: a checkout is a connection of the admin pool with `SET ROLE <principal>`
    applied for the session (committed, so a rollback inside a unit of work does not undo it); a check-in
    resets the role. V2ConnectionProvider then runs its per-transaction `SET LOCAL ROLE <capability>` inside
    the principal. LIMIT OF THE SIMULATION: PostgreSQL lets a superuser session user `SET ROLE` to any role,
    so the principal -> capability MEMBERSHIP is enforced by the provider (PermissionError), not by the
    server; the capability role's own table grants ARE enforced by the server."""

    def __init__(self, pool, principal: str) -> None:
        self._pool, self.principal = pool, principal

    def getconn(self):
        connection = self._pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.principal)))
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
    pools = {profile: PrincipalPool(rehearsal.pool, roles.name(profile)) for profile in ("runtime_data", "api")}
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


def test_b2_the_runtime_gate_reads_under_a_capability_role_so_the_noinherit_data_principal_passes(
        rehearsal, roles, hardened):
    runtime, pools = hardened
    config = runtime.provider.config
    # The api principal inherits `reader`, which may read both identity relations: the gate passes.
    assert verify_pool_target_v2(pools["api"], config) == DATABASE
    # runtime_data is NOINHERIT and holds no grant itself: without a capability role the gate cannot read
    # rt.database_identity (the rehearsal's finding F1, fixed by `principal_profile`: the gate now assumes
    # `control`, or `reader` for the api principal, which the registry lets read both relations).
    with pytest.raises(psycopg2.errors.InsufficientPrivilege, match="permission denied for schema rt"):
        verify_pool_target_v2(pools["runtime_data"], config)
    assert verify_pool_target_v2(pools["runtime_data"], config, "runtime_data") == DATABASE
    assert verify_pool_target_v2(pools["api"], config, "api") == DATABASE
    assert runtime.provider.verify() == DATABASE
    with pytest.raises(ValueError, match="must not hold an owner pool"):
        V2ConnectionProvider({OWNER_POOL: rehearsal.pool}, config)
    with pytest.raises(PermissionError, match="api is not a member of compute"):
        with runtime.provider.transaction("compute", "api"):
            pass
    members = {(profile, capability): rehearsal.scalar("SELECT pg_has_role(%s, %s, 'MEMBER')",
                                                       (roles.name(profile), roles.name(capability)))
               for profile, capabilities in PRINCIPAL_PROFILES.items() for capability in CAPABILITY_ROLES}
    assert {key for key, value in members.items() if value} == {
        (profile, capability) for profile, capabilities in PRINCIPAL_PROFILES.items() for capability in capabilities
        if capability in CAPABILITY_ROLES}


@pytest.fixture(scope="module")
def w4(rehearsal, hardened, w3b):
    """A full window under the principals: a new reference seed (generation 2, created between W3b's and W4's
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


def test_b3_a_full_window_completes_under_the_principals_with_the_capabilities_of_the_registry(
        rehearsal, roles, hardened, w2, w3, w3b, w4):
    runtime, _ = hardened
    print("\nW4 stage seconds", runtime.stage_times("w4"), "total", round(w4.seconds, 1))
    print("W4 rows", rehearsal.stage_rows("w4"), "strategy", w4.extra["strategy"])
    assert w4.extra["tick"] == "COMPLETE" and w4.report.window_errors == {}
    (report,) = w4.report.windows
    states = {stage: (state, codes) for stage, _, state, codes in rehearsal.stage_rows("w4")}
    # As in W1, the whole window COMPLETEs under the principals, with the same degraded codes. The strategies'
    # market time is the same session close as W1's, so the stage finds the retained snapshots (ALREADY_STORED),
    # validates them under the hardened compute role and publishes nothing new.
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
    # The access layer recorded nothing (a hardened provider raises instead of recording) and the capabilities
    # each stage used are those of Part A, all of them members of the profile as PostgreSQL knows it. W4's
    # strategy stage only reads, so the strategy write capabilities are those of W1 (deferred) plus the probe b5.
    assert runtime.provider.role_violations == []
    assert runtime.roles_by_stage(write=True) == {
        stage: roles_used for stage, roles_used in rehearsal.deferred.roles_by_stage(write=True).items()
        if stage != "STRATEGY_PUBLISH"}
    assert runtime.roles_by_stage() == rehearsal.deferred.roles_by_stage()
    for used in {(u.role, u.profile) for u in runtime.transactions}:
        assert rehearsal.scalar("SELECT pg_has_role(%s, %s, 'MEMBER')",
                                (roles.name(used[1]), roles.name(used[0]))), used
    assert all(used.fenced for used in runtime.transactions if used.stage in WINDOW_STAGES and not used.read_only)
    # Reads through the api principal (reader role), in the default `current` mode: the workspace the window
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
    # The kept tier through the api principal's reader role: the strategies W1 published are still named by the
    # pointed-at release and read READY in `current` mode; the session-close releases of the session are readable.
    strategies = RealtimeDailyStrategyRepository(cursor_factory, transaction_configured=True)
    for interval in ("1d", "1h"):
        publication = strategies.read(available_by=datetime.now(UTC), cohort_id=rehearsal.cohort, interval=interval,
                                      view="evaluations")["publication"]
        assert (publication["status"], publication["market_time"]) == ("READY", rehearsal.closed.isoformat())
    with cursor_factory() as cursor:
        assert load_session_close_releases(cursor, rehearsal.cohort, rehearsal.session)


def test_b4_out_of_role_writes_are_rejected_by_postgresql_and_leave_no_row(rehearsal, hardened, w4):
    runtime, _ = hardened
    cohort = str(rehearsal.cohort)
    before = rehearsal.counts()
    snapshot = (
        "INSERT INTO rt.snapshot_catalog (snapshot_id, cohort_id, provider_tier, snapshot_class, snapshot_type, "
        "snapshot_key, session_date, source_generation, manifest_sha256, payload_sha256, generated_at) "
        "VALUES (%s, %s, 'MASSIVE_DEVELOPER', 'EPHEMERAL_PORTAL', 'OUT_OF_ROLE', 'X', %s, 1, %s, %s, %s)",
        (str(uuid4()), cohort, rehearsal.session, "a" * 64, "b" * 64, rehearsal.closed))
    bar = (
        "INSERT INTO rt.bar_catalog (bar_revision_id, cohort_id, provider_tier, instrument_id, interval, "
        "bar_start, bar_end, observed_at, session_date) VALUES (%s, %s, 'MASSIVE_DEVELOPER', %s, '1m', %s, %s, %s, %s)",
        (str(uuid4()), cohort, next(iter(rehearsal.instrument_ids.values())), rehearsal.closed - timedelta(minutes=1),
         rehearsal.closed, rehearsal.closed + timedelta(minutes=1), rehearsal.session))
    attempts = [
        ("compute", "runtime_data", snapshot, "permission denied for table snapshot_catalog"),
        ("publication", "runtime_data", bar, "permission denied for table bar_catalog"),
        ("ingress", "runtime_data", ("UPDATE rt.bar_current SET observed_at = observed_at WHERE cohort_id = %s",
                                     (cohort,)), "permission denied for table bar_current"),
        ("reader", "api", snapshot, "permission denied for table snapshot_catalog"),
    ]
    for capability, profile, (statement, parameters), message in attempts:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege, match=message):
            with runtime.provider.transaction(capability, profile) as unit:
                unit.cursor.execute(statement, parameters)
    assert rehearsal.counts() == before
    assert runtime.provider.role_violations == []


class ProbeRollback(Exception):
    pass


def test_b5_the_kept_and_session_close_writes_of_w1_are_granted_to_the_hardened_publication_role(
        rehearsal, hardened, w4):
    """W1 (deferred) took the first publication writes of the session close for real: the kept snapshot of the
    strategies, the hot-and-kept pair of the ticker/alerts stages and the session-close releases. W4, under the
    principals, is not a session-close window and its strategies are ALREADY_STORED, so the same three writes
    (kept-only, hot-and-kept, session-close release) are exercised under the hardened `publication` principal
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
        with runtime.provider.transaction("publication", "runtime_data") as unit:
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
