"""DB-backed OPTIONS_FLOW runner checks on a disposable V2 database, through the real StageOrchestrator.

Opt in with STOCKS_V2_DB_TESTS=1 and run WITHOUT the offline I/O guard plugin. The database is
test_realtime_v2_oflow_a (created from the sql_v2 baseline, always dropped). The feed is the local scripted
mock; no gateway, socket or provider exists anywhere in this file. Exercised on the real schema: the writer's
COPY and divergence check, the checkpoint rows with their regression guard, gap rows under the ingress
role, the prior-open-interest query, restart equivalence, fail-closed divergence next to a completing
equity window, and the roles every transaction asked for.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from option_flow_runner_support import pinned_semantics, scenario_messages, settings
from option_flow_support import CALL, OTHER, PUT, policy, semantics
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.option_flow.clock import SessionClock
from realtime.option_flow.consumer import ConsumerLimits, GapEntry
from realtime.option_flow.mock_feed import MockOptionFeed
from realtime.orchestration_repository import PostgresOrchestrationRepository, WindowSpec
from realtime.runner_environment import RunnerEnvironment
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome
from realtime.stage_runners_options import (
    OPEN_INTEREST_SQL,
    load_prior_open_interest_cursor,
    options_flow_runner,
    persist_gap_entries,
)
from realtime.v2_service import V2Service
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_oflow_a"
UTC = timezone.utc
ALL = frozenset(STAGES_V1)

EVENTS_SQL = (
    "SELECT e.contract_ticker, e.source_seq, e.event_time, e.revision, e.underlyer, e.exchange_id, e.price, e.size, "
    "e.multiplier, e.premium, e.conditions, e.condition_semantics_version, e.trade_treatment, e.is_multi_leg, "
    "e.side_class, e.side_confidence, e.quote_bid, e.quote_ask, e.quote_bid_size, e.quote_ask_size, e.quote_time, "
    "e.quote_seq, e.cumulative_volume_before, e.cumulative_volume_after, e.volume_baseline_kind, "
    "e.prior_open_interest, e.open_interest_as_of, e.volume_exceeds_prior_oi, e.opening_inference, "
    "e.spread_position, e.opening_score, e.opening_reasons, e.shard_id, e.source_offset, "
    "p.contract_ticker, p.source_seq, p.revision FROM rt.option_flow_events e "
    "LEFT JOIN rt.option_flow_events p ON p.event_id = e.supersedes_event_id WHERE e.cohort_id = %s "
    "ORDER BY e.contract_ticker, e.source_seq, e.event_time, e.revision")
TREATMENT, PRIOR_OI, EXCEEDS = 12, 25, 27
CLUSTERS_SQL = (
    "SELECT session_date, contract_ticker, underlyer, window_seconds, window_start, window_end, side_class, "
    "print_count, total_size, total_premium, distinct_exchanges, first_event_time, last_event_time, status, "
    "revision, policy_sha256 FROM rt.option_flow_clusters WHERE cohort_id = %s "
    "ORDER BY contract_ticker, window_seconds, window_start, side_class, revision")
STATUS = 13
VOLUME_SQL = (
    "SELECT session_date, contract_ticker, cumulative_volume, last_source_seq, last_event_time, baseline_kind, "
    "snapshot_volume, snapshot_watermark_seq FROM rt.option_volume_checkpoints WHERE cohort_id = %s "
    "ORDER BY session_date, contract_ticker")
BASELINE = 5
CHECKPOINT_SQL = ("SELECT shard_id, committed_offset, octet_length(state_payload), state_codec, state_hash "
                  "FROM rt.consumer_checkpoints WHERE cohort_id = %s AND consumer_name = %s ORDER BY shard_id")


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as connection_pool:
        yield connection_pool


def _query(pool, statement, parameters=()):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.fetchall()
    finally:
        connection.rollback()
        pool.putconn(connection)


def _execute(pool, statement, parameters=()):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters)
        connection.commit()
    finally:
        pool.putconn(connection)


class Counter:
    """The injected bounded-slice clock: it never advances, so only the message bound applies."""

    def __call__(self):
        return 0.0


class Spy:
    """Counts reads on a feed."""

    def __init__(self, feed):
        self._feed = feed
        self.generation = feed.generation
        self.reads = 0

    def read_after(self, offset, *, limit=1000):
        self.reads += 1
        return self._feed.read_after(offset, limit=limit)

    def last_offset(self):
        return self._feed.last_offset()


ALIGNMENT_MS = 900_000  # every cluster window hop divides 15 minutes, so a shifted scenario keeps its windows
_bases = []


def next_base() -> int:
    """A distinct 15-minute-aligned scenario start per world: rt.option_flow_events is unique by
    (contract, sequence, time, revision) whatever the cohort, so two worlds may never share prints."""
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    base = now_ms - now_ms % ALIGNMENT_MS + (len(_bases) + 1) * ALIGNMENT_MS
    _bases.append(base)
    return base


def relative(rows, base_ms):
    """Rows with every timestamp expressed relative to the scenario start, for cross-world comparison."""
    origin = datetime.fromtimestamp(base_ms / 1000, UTC)
    return [tuple(value - origin if isinstance(value, datetime) else value for value in row) for row in rows]


class World:
    def __init__(self, pool, feed, *, cohort=None, plan_id=None, base_ms=None, runners=None, enabled=ALL,
                 database_open_interest=False, **environment):
        self.pool = pool
        self.now = datetime.now(UTC)
        self.provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True))
        self.provider.verify()
        self.transactions = []
        original = self.provider.transaction

        def recording(capability_role, principal_profile, **options):
            self.transactions.append((capability_role, principal_profile, bool(options.get("read_only"))))
            return original(capability_role, principal_profile, **options)

        self.provider.transaction = recording
        self.base_ms = base_ms or next_base()
        self.session_date = SessionClock().session_date(self.base_ms)
        if cohort is None:
            connection = pool.getconn()
            try:
                with connection.cursor() as cursor:
                    cohort = insert_cohort(cursor, self.now)
                    plan_id = uuid4()
                    cursor.execute(
                        "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, "
                        "enabled_stages, config_sha256, deadline, scope_reference) "
                        "VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,'fixture scope')",
                        (str(plan_id), str(cohort), self.session_date, sorted(enabled), uuid4().hex + uuid4().hex,
                         self.now + timedelta(hours=8)))
                connection.commit()
            finally:
                pool.putconn(connection)
        self.cohort, self.plan_id = cohort, plan_id
        self.repository = PostgresOrchestrationRepository(self.provider)
        values = dict(options_flow_source=feed, options_flow_semantics=pinned_semantics(),
                      options_flow_policy=policy(sweep_window_seconds=5, minimum_sweep_prints=3),
                      options_flow_settings=settings(), options_flow_clock=Counter())
        if not database_open_interest:
            values["options_flow_oi_loader"] = lambda context, session: {}
        values.update(environment)
        self.environment = RunnerEnvironment(self.provider, **values)
        table = {name: (lambda context: StageOutcome.complete()) for name in enabled}
        table["OPTIONS_FLOW"] = options_flow_runner(self.environment)
        table.update(runners or {})
        self.orchestrator = StageOrchestrator(self.repository, table, database=self.provider, clock=lambda: self.now)

    def again(self, feed, **environment):
        """A new process over the same cohort and plan: nothing of this world's memory survives."""
        return World(self.pool, feed, cohort=self.cohort, plan_id=self.plan_id, base_ms=self.base_ms, **environment)

    def tick(self):
        return self.orchestrator.tick_resident(self.plan_id, "OPTIONS_FLOW")

    def session(self):
        return self.environment.options_flow_state.session

    def readiness(self):
        rows = _query(self.pool, "SELECT status, reason_codes, evidence_ref FROM rt.stage_readiness_current "
                                 "WHERE cohort_id = %s AND stage_id = 'OPTIONS_FLOW'", (str(self.cohort),))
        return (rows[0][0], tuple(rows[0][1]), rows[0][2]) if rows else None

    def roles(self, *, write=None):
        """Capability roles the stage asked for (the orchestrator's own control transactions are not the stage's)."""
        return {role for role, profile, read_only in self.transactions
                if profile == "runtime_options" and (write is None or write == (not read_only))}

    def stage_transactions(self):
        return [item for item in self.transactions if item[1] == "runtime_options"]

    def run_to_end(self, *, finish=True):
        state = self.environment.options_flow_state
        for _ in range(500):
            outcome = self.tick()
            assert outcome.state == "COMPLETE", outcome
            if state.last_report.stopped == "CAUGHT_UP":
                break
        consumer = self.session().consumer
        if finish:
            consumer.finish()
        return consumer

    def rows(self, *, normalized=False):
        rows = {name: _query(self.pool, sql, (str(self.cohort),))
                for name, sql in (("events", EVENTS_SQL), ("clusters", CLUSTERS_SQL), ("volume", VOLUME_SQL))}
        return {name: relative(value, self.base_ms) for name, value in rows.items()} if normalized else rows

    def checkpoint_rows(self, consumer_name="option_flow_stream_v1"):
        return _query(self.pool, CHECKPOINT_SQL, (str(self.cohort), consumer_name))


def db_feed(base_ms, *, seed=None, frame_size=1, disconnect=True):
    """The scenario with a gateway reconnect in the middle; a different seed is a different journal
    generation and different stream sessions carrying the very same messages."""
    messages = scenario_messages(base_ms)
    half = len(messages) // 2
    feed = MockOptionFeed(seed=seed or f"db-{uuid4().hex}")
    feed.extend(messages[:half], frame_size=frame_size)
    if disconnect:
        feed.disconnect()
    feed.extend(messages[half:], frame_size=frame_size)
    return feed


def register_sessions(pool, feed, cohort):
    """The producer registers every stream session before it journals (rt.stream_gaps references it)."""
    manifest = _query(pool, "SELECT manifest_id FROM rt.cohorts WHERE cohort_id = %s", (str(cohort),))[0][0]
    provider = f"mock-{uuid4().hex[:20]}"
    sessions = []
    for record in feed.records:
        if record.stream_session_id not in sessions:
            sessions.append(record.stream_session_id)
    now = datetime.now(UTC)
    for index, session in enumerate(sessions):
        last = index == len(sessions) - 1
        _execute(pool, "INSERT INTO rt.stream_sessions (session_id, provider, feed, manifest_id, owner_id, "
                       "owner_epoch, started_at, ended_at, status) VALUES (%s,%s,'OPTIONS',%s,%s,1,%s,%s,%s)",
                 (str(session), provider, str(manifest), str(uuid4()), now - timedelta(minutes=5 - index),
                  None if last else now - timedelta(minutes=1), "LIVE" if last else "STOPPED"))
    return sessions


def gap_rows(pool, feed):
    sessions = sorted({str(record.stream_session_id) for record in feed.records})
    return _query(pool, "SELECT session_id, channel, gap_start, gap_end, journal_generation, shard_id, "
                        "last_durable_offset, reason, recovery_status, instrument_id FROM rt.stream_gaps "
                        "WHERE session_id = ANY(%s::uuid[]) ORDER BY session_id, shard_id, reason, gap_start",
                  (sessions,))


# --- restart equivalence on the real schema ---------------------------------------------------------------------

@pytest.mark.parametrize("checkpoint_interval", [0.0, 1e9], ids=["state-at-every-flush", "state-once-then-offsets"])
def test_a_crash_and_restart_persists_the_same_rows_as_an_uninterrupted_session(pool, checkpoint_interval):
    # Two cohorts see the same messages shifted in time (prints are unique by contract, sequence and time in
    # every cohort), so rows are compared relative to each scenario's start.
    options = dict(checkpoint_interval_seconds=checkpoint_interval)
    base_ms = next_base()
    reference_feed = db_feed(base_ms)
    reference = World(pool, reference_feed, base_ms=base_ms, options_flow_settings=settings(**options))
    register_sessions(pool, reference_feed, reference.cohort)
    uninterrupted = reference.run_to_end()
    expected = reference.rows(normalized=True)
    assert expected["events"] and expected["clusters"] and expected["volume"]
    assert {row[TREATMENT] for row in expected["events"]} >= {"INCLUDED", "CANCELED"}
    assert {row[STATUS] for row in expected["clusters"]} >= {"PROVISIONAL", "FINAL"}
    expected_gaps = relative(gap_rows(pool, reference_feed), base_ms)
    assert expected_gaps and {row[7] for row in expected_gaps} == {"GATEWAY_SESSION_CHANGED"}

    # Crash mid-session, then restart in a new process, over another cohort and journal.
    other_ms = next_base()
    feed = db_feed(other_ms)
    limits = ConsumerLimits(batch_size=2, max_messages=14, max_seconds=1e6)
    crashed = World(pool, feed, base_ms=other_ms, options_flow_settings=settings(limits=limits, **options))
    register_sessions(pool, feed, crashed.cohort)
    crashed.tick()
    partial = crashed.session().consumer
    assert 0 < partial.checkpointed_offset <= partial.flushed_offset < feed.last_offset()
    checkpoint = crashed.checkpoint_rows()
    assert [row[0] for row in checkpoint] == [0, 1] and {row[1] for row in checkpoint} == {partial.flushed_offset}
    assert all(0 < row[2] <= 1_048_576 for row in checkpoint)
    assert {row[3] for row in checkpoint} == {"option_flow_state_v1+json+zlib"}
    assert all(len(row[4].strip()) == 64 for row in checkpoint)
    assert crashed.rows(normalized=True)["events"] != expected["events"]
    resumed_world = crashed.again(feed, options_flow_settings=settings(**options))
    del crashed, partial  # the process died: no flush, no finish, nothing in memory survived
    resumed = resumed_world.run_to_end()

    assert resumed_world.session().start.mode == "RESTORED"
    got = resumed_world.rows(normalized=True)
    assert got["events"] == expected["events"]
    assert got["clusters"] == expected["clusters"]
    assert got["volume"] == expected["volume"]
    assert resumed.engine.state_digest() == uninterrupted.engine.state_digest()
    assert asdict_stats(resumed) == asdict_stats(uninterrupted)
    resumed_gaps = relative(gap_rows(pool, feed), other_ms)
    # the same gaps, not duplicated by the replay (the session and journal identities differ by feed)
    assert [row[1:4] + row[5:] for row in resumed_gaps] == [row[1:4] + row[5:] for row in expected_gaps]
    assert resumed_world.provider.role_violations == []


def asdict_stats(consumer):
    from dataclasses import asdict

    return asdict(consumer.engine.stats)


# --- gaps -----------------------------------------------------------------------------------------------------------

def test_gaps_are_durable_before_their_rows_and_a_missing_stream_session_fails_closed_then_recovers(pool):
    base_ms = next_base()
    feed = MockOptionFeed(seed=f"gaps-{uuid4().hex}")
    feed.add_trade(CALL, 1, base_ms, 5.0, 100, conditions=(9001,))
    feed.disconnect()
    feed.add_trade(CALL, 2, base_ms + 5_000, 5.0, 100, conditions=(9001,))
    feed.lose_messages(2)
    feed.add_trade(CALL, 3, base_ms + 6_000, 5.0, 100, conditions=(9001,))
    feed.skip_offsets(1)
    feed.add_trade(CALL, 4, base_ms + 7_000, 5.0, 100, conditions=(9001,))
    world = World(pool, feed, base_ms=base_ms)

    outcome = world.tick()  # the producer never registered the stream session: the gap cannot be stored
    assert (outcome.state, outcome.reason_codes) == ("FAILED", ("OPTIONS_FLOW_GAP_PERSIST_FAILED",))
    assert world.readiness()[:2] == ("BLOCKED", ("OPTIONS_FLOW_GAP_PERSIST_FAILED",))
    assert world.rows()["events"] == [] and world.checkpoint_rows() == []  # nothing of the batch was flushed

    register_sessions(pool, feed, world.cohort)
    outcome = world.tick()
    assert outcome.state == "COMPLETE" and any(code.startswith("OPTIONS_FLOW_GAP:") for code in outcome.reason_codes)
    rows = gap_rows(pool, feed)
    assert {row[7] for row in rows} == {"GATEWAY_SESSION_CHANGED", "SOURCE_MESSAGE_HOLE", "SOURCE_OFFSET_HOLE"}
    assert len(rows) == 3 * 2 and {row[8] for row in rows} == {"UNRESOLVED"} and {row[1] for row in rows} == {None}
    assert {row[9] for row in rows} == {None} and {row[4] for row in rows} == {feed.generation}
    assert {row[0] for row in rows} == {feed.records[-1].stream_session_id}
    offsets = {row[7]: row[6] for row in rows}
    assert offsets["SOURCE_OFFSET_HOLE"] == 3  # the offset before the missing one
    status, reasons, _ = world.readiness()
    assert status == "DEGRADED" and "OPTIONS_FLOW_GAP:3" in reasons
    world.session().consumer.finish()
    assert {row[BASELINE] for row in world.rows()["volume"]} == {"INCOMPLETE"}
    assert len(world.rows()["events"]) == 4

    # a restart replays the same gaps under the same identities: nothing is recorded twice
    again = world.again(feed)
    del world
    again.run_to_end()
    assert gap_rows(pool, feed) == rows
    assert again.provider.role_violations == []
    assert again.roles(write=True) <= {"options_compute", "options_ingress"}


def test_gaps_name_each_contract_that_is_an_instrument_and_one_row_for_the_rest(pool):
    feed = MockOptionFeed(seed=f"inst-{uuid4().hex}")
    feed.add_trade(CALL, 1, 1, 5.0, 100, conditions=(9001,))
    cohort = World(pool, MockOptionFeed()).cohort
    session = register_sessions(pool, feed, cohort)[0]
    security = uuid4()
    _execute(pool, "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                   "VALUES (%s, 'ETF', 'fixture', now())", (str(security),))
    contract_id = 770_000 + int(uuid4().int % 100_000)
    ticker = f"O:SPY261218C{contract_id:08d}"
    _execute(pool, "INSERT INTO ref.option_contracts (contract_id, underlying_security_id, provider, "
                   "contract_ticker, first_observed_at, source_origin) VALUES (%s, %s, 'massive', %s, now(), 'fixture')",
             (contract_id, str(security), ticker))
    _execute(pool, "INSERT INTO rt.instruments (asset_class, contract_id) VALUES ('OPTION', %s)", (contract_id,))
    instrument = _query(pool, "SELECT instrument_id FROM rt.instruments WHERE contract_id = %s", (contract_id,))[0][0]
    unknown = "O:SPY261218P09999999"
    named = GapEntry(session, feed.generation, 0, "Q", "QUOTE_GAP", 1_000, 2_000, 5, 9, 4, (ticker,))
    mixed = GapEntry(session, feed.generation, 1, "T", "TRADE_GAP", 1_000, 2_000, 5, 9, 4, (ticker, unknown))
    everyone = GapEntry(session, feed.generation, 1, None, "ALL_GAP", 1_000, 2_000, None, None, 0, ())
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            assert persist_gap_entries(cursor, [named, mixed, everyone]) == 4
            assert persist_gap_entries(cursor, [named, mixed, everyone]) == 0  # deterministic ids: replay is a no-op
        connection.commit()
    finally:
        pool.putconn(connection)
    stored = _query(pool, "SELECT reason, instrument_id, channel, last_durable_offset FROM rt.stream_gaps "
                          "WHERE session_id = %s ORDER BY reason, instrument_id NULLS LAST", (str(session),))
    assert stored == [("ALL_GAP", None, None, 0), ("QUOTE_GAP", instrument, "Q", 4), ("TRADE_GAP", instrument, "T", 4),
                      ("TRADE_GAP", None, "T", 4)]


# --- fail-closed divergence next to a completing equity window -----------------------------------------------------

def test_a_divergent_replay_blocks_options_flow_only_and_an_equity_window_still_completes(pool):
    base_ms = next_base()
    feed = db_feed(base_ms, disconnect=False)
    first = World(pool, feed, base_ms=base_ms, options_flow_settings=settings(consumer_name="consumer_a"))
    first.run_to_end()
    stored = first.rows()
    assert stored["events"]

    # Another deployment (consumer b) replays the same journal, but one print now has a different size.
    victim = next(record.raw_offset for record in feed.records
                  if b'"s":100' in record.payload and b'"p":5.0' in record.payload
                  and b'"c":[9001]' in record.payload and record.raw_offset > 6)
    changed = feed.divergent_replay({victim: {"s": 101}})
    spy = Spy(changed)
    blocked_world = first.again(spy, options_flow_settings=settings(consumer_name="consumer_b"))
    outcome = blocked_world.tick()
    assert (outcome.state, outcome.reason_codes) == ("BLOCKED_POLICY", ("REPLAY_DIVERGENCE",))
    status, reasons, evidence = blocked_world.readiness()
    assert (status, reasons) == ("BLOCKED", ("REPLAY_DIVERGENCE",)) and evidence.startswith(
        "divergence:rt.option_flow_events:")
    quarantine = blocked_world.environment.options_flow_state.quarantine
    assert quarantine.table == "rt.option_flow_events" and quarantine.identities
    assert all(isinstance(part, (str, int, datetime)) for identity in quarantine.identities for part in identity)
    assert blocked_world.environment.options_flow_state.session is None
    committed = {row[1] for row in blocked_world.checkpoint_rows("consumer_b")}
    assert committed <= {quarantine.first_offset - 1}  # what consumer b committed before the divergent flush
    assert blocked_world.rows() == stored  # the divergent flush rolled back: the stored evidence is untouched
    reads, transactions = spy.reads, len(blocked_world.stage_transactions())
    for _ in range(3):
        assert blocked_world.tick().reason_codes == ("REPLAY_DIVERGENCE",)
    assert spy.reads == reads and len(blocked_world.stage_transactions()) == transactions  # no resume by itself

    # The equity side is unaffected: a window completes and every equity stage reports READY.
    blocked_world.repository.upsert_readiness(blocked_world.cohort, "REFERENCE", "stages_v1", "READY", (), None,
                                              blocked_world.now)
    window = blocked_world.repository.ensure_window(blocked_world.plan_id, WindowSpec(
        blocked_world.cohort, "MASSIVE_DEVELOPER", f"oflow-{uuid4().hex[:8]}", blocked_world.now,
        blocked_world.now - timedelta(seconds=90), blocked_world.now + timedelta(seconds=300)))
    report = blocked_world.orchestrator.run_window(window.window_id)
    assert report.window_state == "COMPLETE"
    states = {stage: record.state for stage, record in blocked_world.repository.stage_states(window.window_id).items()}
    assert set(states.values()) == {"COMPLETE"} and "OPTIONS_FLOW" not in states
    readiness = blocked_world.repository.readiness(blocked_world.cohort)
    assert readiness["OPTIONS_FLOW"] == "BLOCKED" and readiness["PORTAL_PUBLISH"] == "READY"
    assert readiness["STOCK_ALERTS"] == "READY" and readiness["EQUITY_EVIDENCE"] == "READY"

    # The explicit operator reset, with the journal back to what was stored: it resumes from its checkpoint.
    cleared = blocked_world.environment.options_flow_state.reset_divergence()
    assert cleared is quarantine
    blocked_world.environment.options_flow_source = feed
    resumed = blocked_world.run_to_end()
    assert blocked_world.session().start.mode in ("RESTORED", "WARMING")
    assert blocked_world.rows() == stored and resumed.fed_offset == feed.last_offset()
    assert blocked_world.readiness()[0] in ("READY", "DEGRADED")
    assert blocked_world.provider.role_violations == []


# --- checkpoints on the real schema ---------------------------------------------------------------------------------

def _partial(pool, base_ms=None):
    base_ms = base_ms or next_base()
    feed = db_feed(base_ms, disconnect=False)
    limits = ConsumerLimits(batch_size=2, max_messages=14, max_seconds=1e6)
    world = World(pool, feed, base_ms=base_ms, options_flow_settings=settings(limits=limits))
    world.tick()
    assert world.checkpoint_rows()
    return feed, world


def test_a_corrupt_checkpoint_is_rejected_loudly_and_replaced_by_the_restarted_session(pool):
    feed, world = _partial(pool)
    durable = world.session().consumer.flushed_offset
    _execute(pool, "UPDATE rt.consumer_checkpoints SET state_hash = repeat('0', 64) "
                   "WHERE cohort_id = %s AND shard_id = 0", (str(world.cohort),))
    second = world.again(feed)
    outcome = second.tick()
    assert "OPTIONS_FLOW_WARMING:CHECKPOINT_HASH_MISMATCH" in outcome.reason_codes
    assert second.session().start.mode == "WARMING" and second.session().start.offset == durable
    assert second.readiness()[0] == "DEGRADED"
    rows = second.checkpoint_rows()
    assert {row[4].strip() for row in rows} != {"0" * 64} and all(row[4].strip() != "0" * 64 for row in rows)
    third = world.again(feed)
    outcome = third.tick()
    assert third.session().start.mode == "RESTORED" and "start=RESTORED" in outcome.evidence_ref


def test_a_checkpoint_ahead_of_the_journal_is_rejected_and_then_deliberately_replaced(pool):
    feed, world = _partial(pool)
    _execute(pool, "UPDATE rt.consumer_checkpoints SET committed_offset = committed_offset + 1000 "
                   "WHERE cohort_id = %s", (str(world.cohort),))
    second = world.again(feed)
    outcome = second.tick()
    assert "OPTIONS_FLOW_WARMING:CHECKPOINT_AHEAD_OF_SOURCE" in outcome.reason_codes
    assert second.session().start.offset == feed.last_offset()
    feed.add_trade(CALL, 700, world.base_ms + 20_000, 5.0, 100, conditions=(9001,))
    second.tick()
    assert {row[1] for row in second.checkpoint_rows()} == {feed.last_offset()}  # replaced once, on purpose
    assert second.provider.role_violations == []


def test_the_regression_guard_refuses_a_stale_writer_and_its_flush_rolls_back(pool):
    base_ms = next_base()
    feed = db_feed(base_ms, disconnect=False)
    short = feed.truncated(14)
    world = World(pool, short, base_ms=base_ms)
    world.tick()
    before = world.rows()
    _execute(pool, "UPDATE rt.consumer_checkpoints SET committed_offset = committed_offset + 1000 "
                   "WHERE cohort_id = %s", (str(world.cohort),))
    short.add_trade(CALL, 701, base_ms + 30_000, 5.0, 100, conditions=(9001,))
    short.add_trade(CALL, 702, base_ms + 30_100, 5.0, 100, conditions=(9001,))
    outcome = world.tick()
    assert (outcome.state, outcome.reason_codes) == ("FAILED", ("OPTIONS_FLOW_CHECKPOINT_REGRESSION",))
    assert world.readiness()[:2] == ("BLOCKED", ("OPTIONS_FLOW_CHECKPOINT_REGRESSION",))
    assert world.rows() == before  # the rows of the refused flush were rolled back with it


def test_a_state_over_the_size_bound_blocks_the_flush_and_never_stores_a_partial_checkpoint(pool):
    base_ms = next_base()
    feed = db_feed(base_ms, disconnect=False)
    world = World(pool, feed, base_ms=base_ms, options_flow_settings=settings(max_checkpoint_bytes=300))
    outcome = world.tick()
    assert (outcome.state, outcome.reason_codes) == ("FAILED", ("OPTIONS_FLOW_CHECKPOINT_TOO_LARGE",))
    assert world.rows()["events"] == [] and world.checkpoint_rows() == []


# --- prior open interest ---------------------------------------------------------------------------------------------

def _seed_open_interest(pool, session_date):
    now = datetime.now(UTC)
    security = uuid4()
    _execute(pool, "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                   "VALUES (%s, 'ETF', 'fixture', now())", (str(security),))
    ids = {}
    for index, ticker in enumerate((CALL, PUT, OTHER)):
        ids[ticker] = 880_000 + int(uuid4().int % 100_000) + index * 100_000
        _execute(pool, "INSERT INTO ref.option_contracts (contract_id, underlying_security_id, provider, "
                       "contract_ticker, first_observed_at, source_origin) "
                       "VALUES (%s, %s, %s, %s, now(), 'fixture') ON CONFLICT DO NOTHING",
                 (ids[ticker], str(security), f"fixture-{uuid4().hex[:8]}", ticker))
    prior = session_date - timedelta(days=1)

    def baseline(ticker, open_interest, *, received_offset, supersedes=None):
        baseline_id = uuid4()
        observed = now - timedelta(hours=3)
        _execute(pool, "INSERT INTO ref.oi_baselines (baseline_id, contract_id, settlement_session, open_interest, "
                       "source, date_basis, observed_session, observed_at, received_at, payload_sha256, "
                       "supersedes_baseline_id) VALUES (%s,%s,%s,%s,'massive','FIXTURE',%s,%s,%s,%s,%s)",
                 (str(baseline_id), ids[ticker], prior, open_interest, session_date, observed,
                  observed + received_offset, uuid4().hex + uuid4().hex, supersedes))
        return baseline_id

    first = baseline(CALL, 250, received_offset=timedelta(minutes=1))
    baseline(CALL, 300, received_offset=timedelta(minutes=30), supersedes=str(first))  # the revision wins
    baseline(OTHER, 999, received_offset=timedelta(hours=5))  # received after the tick: not known yet
    return ids


def test_prior_open_interest_is_read_from_the_reference_baselines_and_a_missing_one_is_never_guessed(pool):
    base_ms = next_base()
    session_date = SessionClock().session_date(base_ms)
    ids = _seed_open_interest(pool, session_date)
    known_at = datetime.now(UTC)
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            loaded = load_prior_open_interest_cursor(cursor, session_date, known_at, max_rows=100)
            assert (loaded[CALL].open_interest, loaded[CALL].as_of) == (300, session_date - timedelta(days=1))
            assert PUT not in loaded and OTHER not in loaded
            with pytest.raises(Exception):
                load_prior_open_interest_cursor(cursor, session_date, known_at, max_rows=0)
            other_day = load_prior_open_interest_cursor(cursor, session_date + timedelta(days=1), known_at, max_rows=100)
            assert CALL not in other_day  # baselines are per observed session
        connection.rollback()
    finally:
        pool.putconn(connection)
    assert "supersedes_baseline_id" in OPEN_INTEREST_SQL and len(ids) == 3

    feed = MockOptionFeed(seed=f"oi-{uuid4().hex}")
    feed.add_trade(CALL, 1, base_ms, 5.0, 350, conditions=(9001,))
    feed.add_trade(PUT, 1, base_ms + 1, 5.0, 350, conditions=(9001,))
    world = World(pool, feed, base_ms=base_ms, database_open_interest=True)
    world.run_to_end()
    events = {row[0]: row for row in world.rows()["events"]}
    assert events[CALL][PRIOR_OI] == 300 and events[CALL][EXCEEDS] is True
    assert events[PUT][PRIOR_OI] is None and events[PUT][EXCEEDS] is False
    assert world.roles(write=False) == {"options_compute"}


# --- configuration, service and roles ----------------------------------------------------------------------------------

def test_an_unconfigured_runner_blocks_through_the_database_orchestrator_without_touching_the_database(pool):
    for environment, code in ((dict(options_flow_source=None), "OPTIONS_FLOW_SOURCE_NOT_CONFIGURED"),
                              (dict(options_flow_semantics=semantics()), "OPTIONS_FLOW_CONDITIONS_UNPINNED")):
        world = World(pool, MockOptionFeed(), **environment)
        before = world.rows()
        outcome = world.tick()
        assert (outcome.state, outcome.reason_codes) == ("BLOCKED_POLICY", (code,))
        assert world.readiness()[:2] == ("BLOCKED", (code,))
        assert world.stage_transactions() == [] and world.rows() == before


def test_the_v2_service_ticks_options_flow_and_every_transaction_uses_the_runtime_options_roles(pool):
    base_ms = next_base()
    feed = db_feed(base_ms, disconnect=False)
    world = World(pool, feed, base_ms=base_ms)
    service = V2Service(world.orchestrator, world.repository, world.plan_id, [], clock=lambda: world.now,
                        resident_intervals={"OPTIONS_FLOW": 0.01})
    assert service.tick_resident_once("OPTIONS_FLOW") == "COMPLETE"
    assert service.resident_errors == {}
    assert world.readiness()[0] == "READY"
    assert world.stage_transactions() and {role for role, profile, _ in world.transactions
                                          if profile != "runtime_options"} <= {"control"}  # the orchestrator's own
    assert world.roles(write=True) == {"options_compute"}  # a clean stream needs no ingress write
    assert world.roles(write=False) == {"options_compute"}
    assert world.provider.role_violations == []
    assert len(world.rows()["events"]) > 0
