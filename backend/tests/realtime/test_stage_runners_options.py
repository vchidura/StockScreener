"""OPTIONS_FLOW runner, offline: fail-closed configuration, bounded slices, readiness reporting, checkpoint
restore and rejection, gap persistence order, fail-closed divergence and isolation from the equity stages.
Everything runs through the real StageOrchestrator.tick_resident over an in-memory repository and an
in-memory options database; no gateway, socket, file or provider is touched."""
from __future__ import annotations

import inspect
import threading
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

import realtime.stage_runners_options as runners
from option_flow_runner_support import (
    ALL,
    NOW,
    SESSION,
    FakeOptionsDatabase,
    MonotonicClock,
    harness,
    parse_rows,
    pinned_semantics,
    scenario_feed,
    scenario_messages,
    settings,
)
from option_flow_support import CALL, CANCEL, OTHER, PUT, T0, policy, semantics, trade
from psycopg2 import errors
from realtime.option_flow.checkpoint_state import CheckpointRow
from realtime.option_flow.consumer import ConsumerLimits
from realtime.option_flow.labels import PriorOpenInterest
from realtime.option_flow.mock_feed import MockOptionFeed
from realtime.option_flow.volume import BaselineKind
from realtime.option_flow.writer import CHECKPOINT_COLUMNS, EVENT_COLUMNS
from realtime.orchestration_repository import ServicePlan
from realtime.runner_environment import RunnerEnvironment
from realtime.stage_orchestrator import StageContext, StageOutcome
from realtime.stage_runners_options import (
    OPTIONS_FLOW_CONDITIONS_UNPINNED,
    OPTIONS_FLOW_POLICY_NOT_CONFIGURED,
    OPTIONS_FLOW_SOURCE_NOT_CONFIGURED,
    REPLAY_DIVERGENCE,
    OptionsFlowSettings,
    options_flow_block_reason,
    options_flow_runner,
)


class SpySource:
    """A feed whose every read is counted; any other attribute access would be a protocol violation."""

    def __init__(self, feed):
        self._feed = feed
        self.generation = feed.generation
        self.reads = 0

    def read_after(self, offset, *, limit=1000):
        self.reads += 1
        return self._feed.read_after(offset, limit=limit)

    def last_offset(self):
        return self._feed.last_offset()


# --- fail-closed configuration -----------------------------------------------------------------------

def test_no_source_blocks_before_any_database_access():
    world = harness(None)
    outcome = world.tick()
    assert (outcome.state, outcome.reason_codes) == ("BLOCKED_POLICY", (OPTIONS_FLOW_SOURCE_NOT_CONFIGURED,))
    assert world.database.transactions == []
    assert world.readiness() == ("BLOCKED", (OPTIONS_FLOW_SOURCE_NOT_CONFIGURED,))


def test_an_object_that_is_not_a_journal_source_is_not_a_source():
    class NoGeneration:
        def read_after(self, offset, *, limit=1000):
            raise AssertionError("must not read")

    world = harness(NoGeneration())
    assert world.tick().reason_codes == (OPTIONS_FLOW_SOURCE_NOT_CONFIGURED,)


def test_the_synthetic_conditions_fixture_is_refused_before_anything_is_read():
    source = SpySource(scenario_feed())
    world = harness(source, semantics=semantics())  # the synthetic fixture, relabel-free
    outcome = world.tick()
    assert (outcome.state, outcome.reason_codes) == ("BLOCKED_POLICY", (OPTIONS_FLOW_CONDITIONS_UNPINNED,))
    assert source.reads == 0 and world.database.transactions == []
    assert world.readiness() == ("BLOCKED", (OPTIONS_FLOW_CONDITIONS_UNPINNED,))


def test_missing_conditions_and_missing_policy_each_block_with_their_own_code():
    source = SpySource(scenario_feed())
    environment = RunnerEnvironment(None, options_flow_source=source)
    assert options_flow_block_reason(environment) == OPTIONS_FLOW_CONDITIONS_UNPINNED
    environment.options_flow_semantics = pinned_semantics()
    assert options_flow_block_reason(environment) == OPTIONS_FLOW_POLICY_NOT_CONFIGURED
    environment.options_flow_policy = policy()
    assert options_flow_block_reason(environment) is None
    assert source.reads == 0
    # The source gate comes first, then the conditions pin.
    assert options_flow_block_reason(RunnerEnvironment(None, options_flow_semantics=semantics())) == (
        OPTIONS_FLOW_SOURCE_NOT_CONFIGURED)


def test_a_runner_without_a_database_blocks_instead_of_raising():
    source = SpySource(scenario_feed())
    environment = RunnerEnvironment(None, options_flow_source=source, options_flow_semantics=pinned_semantics(),
                                    options_flow_policy=policy())
    world = harness(source)
    context = StageContext(world.plan, world.orchestrator._placeholder_window(world.plan, NOW),
                           world.orchestrator._stages["OPTIONS_FLOW"], uuid4(), 1, NOW, None)
    outcome = options_flow_runner(environment)(context)
    assert outcome.reason_codes == (runners.OPTIONS_FLOW_DATABASE_NOT_CONFIGURED,) and source.reads == 0


def test_the_runner_is_registered_and_reported_as_fail_closed_until_configured():
    from realtime.stage_runners_v2 import RUNNER_FACTORIES, build_runners, fail_closed_stages, unwired_stages

    assert RUNNER_FACTORIES["OPTIONS_FLOW"] is options_flow_runner
    assert unwired_stages() == {"OPTIONS_ACQUIRE", "OPTIONS_ADMISSION", "OPTIONS_CYCLE"}
    assert set(build_runners({"OPTIONS_FLOW"}, None)) == {"OPTIONS_FLOW"}
    assert fail_closed_stages({"OPTIONS_FLOW"}, RunnerEnvironment(None)) == {
        "OPTIONS_FLOW": OPTIONS_FLOW_SOURCE_NOT_CONFIGURED}
    configured = RunnerEnvironment(None, options_flow_source=SpySource(MockOptionFeed()),
                                   options_flow_semantics=pinned_semantics(), options_flow_policy=policy())
    assert fail_closed_stages({"OPTIONS_FLOW"}, configured) == {}


def test_the_runner_module_builds_no_gateway_websocket_or_http_client():
    import realtime.option_flow.consumer as consumer
    import realtime.option_flow.mock_feed as mock_feed

    for module in (runners, consumer, mock_feed):
        source = inspect.getsource(module)
        for forbidden in ("import requests", "httpx", "websockets", "AdvancedGateway", "advanced_gateway", "urllib",
                          "import socket", "polygon", "RawJournalReader(", "from .journal", "from ..journal",
                          "sqlite3", "datetime.now", "utc_now"):
            assert forbidden not in source, (module.__name__, forbidden)


# --- ticks, readiness and bounded slices --------------------------------------------------------------

def test_a_tick_without_a_checkpoint_starts_warming_and_reports_why():
    world = harness(scenario_feed(), flow_settings=settings(initial_baseline=BaselineKind.WARMING))
    outcome = world.tick()
    assert outcome.state == "COMPLETE" and outcome.reason_codes == ("OPTIONS_FLOW_WARMING:NO_CHECKPOINT",)
    assert world.readiness()[0] == "DEGRADED"
    assert world.database.roles(write=True) == {"options_compute"}
    assert world.database.role_violations == []


def test_a_current_stage_is_ready_and_its_evidence_names_the_offsets():
    feed = scenario_feed()
    world = harness(feed)
    outcome = world.tick()
    assert (outcome.state, outcome.reason_codes) == ("COMPLETE", ())
    assert world.readiness() == ("READY", ())
    assert f"fed={feed.last_offset()}" in outcome.evidence_ref and "start=WARMING" in outcome.evidence_ref
    assert world.database.event_rows() and world.database.volume_rows()


def test_a_slice_is_bounded_by_messages_and_the_rest_waits_in_the_journal():
    feed = scenario_feed()
    total = feed.last_offset()
    world = harness(feed, flow_settings=settings(limits=ConsumerLimits(batch_size=3, max_messages=10, max_seconds=1e6)))
    world.tick()
    report = world.environment.options_flow_state.last_report
    assert report.stopped == "MAX_MESSAGES" and 10 <= report.messages <= 12
    consumer = world.environment.options_flow_state.session.consumer
    assert 0 < consumer.fed_offset < total and consumer.backlog() == total - consumer.fed_offset
    for _ in range(20):
        world.tick()
    assert consumer.fed_offset == total and world.environment.options_flow_state.last_report.stopped == "CAUGHT_UP"


def test_a_slice_is_bounded_by_seconds_with_the_injected_clock():
    feed = scenario_feed()
    clock = MonotonicClock(step=1.0)
    world = harness(feed, clock=clock, flow_settings=settings(
        limits=ConsumerLimits(batch_size=2, max_messages=10_000, max_seconds=5.0)))
    world.tick()
    report = world.environment.options_flow_state.last_report
    assert report.stopped == "MAX_SECONDS" and 0 < report.records < feed.last_offset()


def test_cancellation_is_polled_between_batches_and_before_the_first_read():
    feed = scenario_feed()
    source = SpySource(feed)
    world = harness(source, flow_settings=settings(limits=ConsumerLimits(batch_size=2, max_messages=1000,
                                                                          max_seconds=1e6)))
    cancelled = threading.Event()
    original = source.read_after

    def cancelling(offset, *, limit=1000):
        records = original(offset, limit=limit)
        if source.reads == 2:
            cancelled.set()
        return records

    source.read_after = cancelling
    start = world.orchestrator._placeholder_window(world.plan, NOW)
    context = StageContext(world.plan, start, world.orchestrator._stages["OPTIONS_FLOW"], uuid4(), 1, NOW,
                           world.database, cancelled)
    runner = options_flow_runner(world.environment)
    outcome = runner(context)
    assert outcome.reason_codes == ("STAGE_CANCELLED",) and source.reads == 2
    reads = source.reads
    assert runner(context).reason_codes == ("STAGE_CANCELLED",) and source.reads == reads  # nothing read once cancelled


def test_the_consumer_only_reads_and_the_engine_queue_stays_bounded_under_overload():
    from realtime.option_flow.engine import SubmitResult

    feed = MockOptionFeed()
    feed.extend([trade(seq=index, time_ms=T0 + index, price=5.0, size=100, exchange=300 + index % 3)
                 for index in range(1, 41)], frame_size=40)
    world = harness(SpySource(feed), flow_settings=settings(
        queue_capacity=4, shard_count=1, limits=ConsumerLimits(batch_size=10, max_messages=1000, max_seconds=1e6)))
    outcome = world.tick()
    engine = world.environment.options_flow_state.session.consumer.engine
    report = world.environment.options_flow_state.last_report
    assert report.gap_recorded > 0 and report.accepted + report.gap_recorded == 40
    assert engine.queued() == 0 and engine.stats.queue_dropped == report.gap_recorded
    assert engine.stats.accounted() == engine.stats.submitted == 40
    assert any(code.startswith("OPTIONS_FLOW_GAP:") for code in outcome.reason_codes)
    assert {entry[9] for entry in world.database.gaps.values()} == {"BACKPRESSURE_BUFFER_FULL"}
    assert SubmitResult.GAP_RECORDED.value == "GAP_RECORDED"


# --- source discontinuities become gaps, persisted before the rows -----------------------------------------

def test_a_gateway_reconnect_a_lost_frame_and_an_offset_hole_are_persisted_as_gaps_before_the_flush():
    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(9001,))
    feed.add_trade(CALL, 2, T0 + 10, 5.0, 100, conditions=(9001,))
    feed.disconnect()
    feed.add_trade(CALL, 3, T0 + 5_000, 5.0, 100, conditions=(9001,))
    feed.lose_messages(3)
    feed.add_trade(CALL, 4, T0 + 6_000, 5.0, 100, conditions=(9001,))
    feed.skip_offsets(2)
    feed.add_trade(CALL, 5, T0 + 7_000, 5.0, 100, conditions=(9001,))
    feed.add_status()
    feed.add_undecodable()
    world = harness(feed)
    outcome = world.tick()
    assert outcome.state == "COMPLETE" and "OPTIONS_FLOW_GAP:4" in outcome.reason_codes
    reasons = sorted({entry[9] for entry in world.database.gaps.values()})
    assert reasons == ["GATEWAY_SESSION_CHANGED", "SOURCE_MESSAGE_HOLE", "SOURCE_OFFSET_HOLE", "UNREADABLE_FRAME"]
    sessions = {entry[1] for entry in world.database.gaps.values()}
    assert sessions == {str(feed.records[-1].stream_session_id)}  # every gap belongs to the session that saw it
    # One row per gap and engine shard (the engine reports every shard), all unresolved, channel unspecified.
    assert len(world.database.gaps) == 4 * 2 and {entry[3] for entry in world.database.gaps.values()} == {None}
    log = world.database.commit_log
    first_gap = log.index(("options_ingress", False))
    assert first_gap < log.index(("options_compute", False), 1)  # the gap transaction commits before any flush...
    flushes_before = [item for item in log[:first_gap] if item == ("options_compute", False)]
    assert flushes_before == []
    # The affected accumulators are INCOMPLETE in the persisted volume checkpoints.
    assert {row["baseline_kind"] for row in world.database.volume_rows()} == {"INCOMPLETE"}


def test_a_gap_that_cannot_be_persisted_blocks_the_flush_and_is_retried_without_loss():
    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(9001,))
    feed.disconnect()
    feed.add_trade(CALL, 2, T0 + 5_000, 5.0, 100, conditions=(9001,))
    database = FakeOptionsDatabase(fail_gap_insert=errors.ForeignKeyViolation("stream session not registered"))
    world = harness(feed, database=database)
    outcome = world.tick()
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.OPTIONS_FLOW_GAP_PERSIST_FAILED,)
    assert database.events == [] and database.checkpoints == {}  # no row of the batch was flushed
    assert "stream session" not in repr(outcome)
    database.fail_gap_insert = None
    outcome = world.tick()
    assert outcome.state == "COMPLETE" and len(database.gaps) == 2
    assert len(database.event_rows()) == 2  # both prints, once each


# --- checkpoints: restore, reject, regress -------------------------------------------------------------------

def _stored_rows(database):
    return [CheckpointRow(shard, row[0], row[1], row[2], row[3], row[4], row[5])
            for shard, row in sorted(database.checkpoints.items())]


def _shard_key(database, shard):
    return next(key for key in database.checkpoints if key[3] == shard)


def test_a_restart_restores_the_latest_checkpoint_and_resumes_after_its_offset():
    feed = scenario_feed()
    database = FakeOptionsDatabase()
    first = harness(feed, database=database, flow_settings=settings(
        limits=ConsumerLimits(batch_size=3, max_messages=15, max_seconds=1e6)))
    first.tick()
    consumer = first.environment.options_flow_state.session.consumer
    covered = consumer.checkpointed_offset
    assert covered == consumer.fed_offset > 0
    assert {row[0] for row in database.checkpoints.values()} == {covered}
    source = SpySource(feed)
    # a new process: no in-memory state at all, the same cohort
    second = harness(source, database=database, cohort_id=first.plan.cohort_id)
    outcome = second.tick()
    assert outcome.state == "COMPLETE" and "start=RESTORED" in outcome.evidence_ref
    assert not any(code.startswith("OPTIONS_FLOW_WARMING") for code in outcome.reason_codes)
    assert second.environment.options_flow_state.session.start.offset == covered
    assert second.environment.options_flow_state.session.consumer.fed_offset == feed.last_offset()


@pytest.mark.parametrize("damage, code", [
    ("hash", "CHECKPOINT_HASH_MISMATCH"),
    ("policy", "CHECKPOINT_POLICY_MISMATCH"),
    ("missing_shard", "CHECKPOINT_INCOMPLETE_SET"),
    ("codec", "CHECKPOINT_BAD_CODEC"),
    ("payload", "CHECKPOINT_UNREADABLE"),
    ("shard_map", "CHECKPOINT_SHARD_MAP_MISMATCH"),
])
def test_a_damaged_or_incompatible_checkpoint_is_rejected_loudly_and_the_engine_restarts_warming(damage, code):
    feed = scenario_feed()
    database = FakeOptionsDatabase()
    first = harness(feed.truncated(10), database=database)
    first.tick()
    stored = database.checkpoints
    assert stored
    changed_policy = None
    key = _shard_key(database, 0)
    row = list(stored[key])
    if damage == "hash":
        row[4] = "0" * 64
        stored[key] = tuple(row)
    elif damage == "missing_shard":
        del stored[key]
    elif damage == "codec":
        row[3] = "other_codec"
        stored[key] = tuple(row)
    elif damage == "payload":
        row[5] = b"not zlib"
        stored[key] = tuple(row)
    elif damage == "shard_map":
        row[1] = "f" * 64
        stored[key] = tuple(row)
    elif damage == "policy":
        changed_policy = policy(sweep_window_seconds=5, minimum_sweep_prints=4)
    second = harness(SpySource(feed), database=database, flow_policy=changed_policy, cohort_id=first.plan.cohort_id)
    outcome = second.tick()
    assert outcome.state == "COMPLETE" and f"OPTIONS_FLOW_WARMING:{code}" in outcome.reason_codes
    session = second.environment.options_flow_state.session
    assert session.start.mode == "WARMING" and session.start.reason == code
    assert session.start.offset == 10  # resumes after the rows that are persisted, never silently trusts the state
    assert second.readiness()[0] == "DEGRADED"


def test_a_checkpoint_ahead_of_the_source_is_rejected_and_replaced_not_trusted():
    feed = scenario_feed()
    database = FakeOptionsDatabase()
    first = harness(feed, database=database)
    first.tick()
    ahead = next(iter(database.checkpoints.values()))[0]
    behind = feed.truncated(ahead - 8)
    world = harness(SpySource(behind), database=database, cohort_id=first.plan.cohort_id)
    outcome = world.tick()
    assert "OPTIONS_FLOW_WARMING:CHECKPOINT_AHEAD_OF_SOURCE" in outcome.reason_codes
    session = world.environment.options_flow_state.session
    assert session.start.offset == behind.last_offset()
    behind.add_trade(CALL, 900, T0 + 10_000, 5.0, 100, conditions=(9001,))
    world.tick()
    assert {row[0] for row in database.checkpoints.values()} == {behind.last_offset()}  # replaced, once, deliberately
    assert world.tick().state == "COMPLETE"


def test_the_regression_guard_refuses_an_older_checkpoint_loudly():
    feed = scenario_feed()
    short = feed.truncated(12)
    database = FakeOptionsDatabase()
    world = harness(short, database=database)
    world.tick()
    stored_offset = next(iter(database.checkpoints.values()))[0]
    # Another writer moved the stored checkpoint ahead of this consumer (a split brain).
    for key, row in list(database.checkpoints.items()):
        database.checkpoints[key] = (stored_offset + 100, *row[1:])
    short.add_trade(CALL, 901, T0 + 20_000, 5.0, 100, conditions=(9001,))
    outcome = world.tick()
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.OPTIONS_FLOW_CHECKPOINT_REGRESSION,)


def test_a_checkpoint_that_exceeds_the_size_bound_fails_the_flush_instead_of_dropping_state():
    feed = scenario_feed()
    world = harness(feed, flow_settings=settings(max_checkpoint_bytes=300))
    outcome = world.tick()
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.OPTIONS_FLOW_CHECKPOINT_TOO_LARGE,)
    assert world.database.events == [] and world.database.checkpoints == {}
    assert world.readiness() == ("BLOCKED", (runners.OPTIONS_FLOW_CHECKPOINT_TOO_LARGE,))


# --- prior open interest -------------------------------------------------------------------------------

def test_prior_open_interest_comes_from_the_injected_loader_and_a_missing_baseline_is_never_guessed():
    calls = []

    def loader(context, session_date):
        calls.append(session_date)
        return {CALL: PriorOpenInterest(100, SESSION - timedelta(days=1))}

    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 150, conditions=(9001,))
    feed.add_trade(PUT, 1, T0 + 1, 5.0, 150, conditions=(9001,))
    world = harness(feed, oi_loader=loader)
    world.tick()
    world.tick()
    assert calls == [SESSION]  # once per session
    rows = {row["contract_ticker"]: row for row in world.database.event_rows()}
    assert rows[CALL]["prior_open_interest"] == "100" and rows[CALL]["volume_exceeds_prior_oi"] == "t"
    assert rows[PUT]["prior_open_interest"] == "\\N" and rows[PUT]["volume_exceeds_prior_oi"] == "f"
    assert rows[PUT]["opening_inference"] == "NOT_ASSESSED"


def test_an_open_interest_load_failure_fails_the_tick_without_starting_a_session():
    def loader(context, session_date):
        raise runners.OpenInterestLoadError("OPEN_INTEREST_ROW_BOUND_EXCEEDED")

    world = harness(scenario_feed(), oi_loader=loader)
    outcome = world.tick()
    assert outcome.reason_codes == (runners.OPTIONS_FLOW_OPEN_INTEREST_FAILED,)
    assert world.environment.options_flow_state.session is None


def test_the_open_interest_query_is_bounded_and_ignores_superseded_baselines():
    sql = runners.OPEN_INTEREST_SQL
    assert "supersedes_baseline_id" in sql and "LIMIT" in sql and "observed_session" in sql
    cursor = type("Cursor", (), {"execute": lambda self, *a: None, "fetchall": lambda self: [("O:X", 1, SESSION)] * 3})()
    with pytest.raises(runners.OpenInterestLoadError):
        runners.load_prior_open_interest_cursor(cursor, SESSION, NOW, max_rows=2)
    assert list(runners.load_prior_open_interest_cursor(cursor, SESSION, NOW, max_rows=3)) == ["O:X"]


# --- fail-closed divergence ----------------------------------------------------------------------------------

def test_a_replay_divergence_blocks_quarantines_and_does_not_resume_by_itself():
    feed = scenario_feed()
    source = SpySource(feed)
    database = FakeOptionsDatabase(diverge_events=True)
    world = harness(source, database=database)
    outcome = world.tick()
    assert (outcome.state, outcome.reason_codes) == ("BLOCKED_POLICY", (REPLAY_DIVERGENCE,))
    assert world.readiness() == ("BLOCKED", (REPLAY_DIVERGENCE,))
    state = world.environment.options_flow_state
    assert state.session is None  # the engine state of the session is discarded
    quarantine = state.quarantine
    assert quarantine.table == "rt.option_flow_events" and quarantine.identities
    assert (quarantine.first_offset, quarantine.last_offset) == (1, 8)  # the uncommitted range of the failed flush
    assert quarantine.evidence_ref == outcome.evidence_ref and "divergence:rt.option_flow_events" in outcome.evidence_ref
    # identifiers only: nothing that looks like a payload or a row value
    assert all(isinstance(part, (str, int)) for identity in quarantine.identities for part in identity)
    assert set(quarantine.__slots__) == {"table", "identities", "first_offset", "last_offset", "journal_generation",
                                         "consumer_name", "session_date"}
    assert database.events == [] and database.checkpoints == {} and database.rollbacks >= 1
    reads, transactions = source.reads, len(database.transactions)
    for _ in range(3):
        again = world.tick()
        assert again.reason_codes == (REPLAY_DIVERGENCE,)
    assert source.reads == reads and len(database.transactions) == transactions  # nothing read, nothing written


def test_an_operator_reset_clears_the_quarantine_and_the_session_restarts_from_the_last_checkpoint():
    feed = scenario_feed()
    database = FakeOptionsDatabase()
    world = harness(feed, database=database, flow_settings=settings(
        limits=ConsumerLimits(batch_size=3, max_messages=12, max_seconds=1e6)))
    world.tick()  # a good first slice and its checkpoint
    covered = world.environment.options_flow_state.session.consumer.checkpointed_offset
    assert covered > 0
    database.diverge_events = True
    assert world.tick().reason_codes == (REPLAY_DIVERGENCE,)
    state = world.environment.options_flow_state
    assert state.quarantine.first_offset == covered + 1
    cleared = state.reset_divergence()
    assert cleared is not None and state.quarantine is None and state.session is None
    assert state.reset_divergence() is None
    database.diverge_events = False  # the operator fixed the cause
    outcome = world.tick()
    assert outcome.state == "COMPLETE" and "start=RESTORED" in outcome.evidence_ref
    assert state.session.start.offset == covered
    assert world.readiness()[0] in ("READY", "DEGRADED")


def test_a_blocked_options_flow_never_delays_or_blocks_an_equity_window():
    from realtime.orchestration_repository import WindowSpec

    feed = scenario_feed()
    world = harness(feed, database=FakeOptionsDatabase(diverge_events=True))
    equity_ran = []
    world.orchestrator._runners["EQUITY_INGEST"] = lambda context: equity_ran.append(True) or StageOutcome.complete()
    assert world.tick().reason_codes == (REPLAY_DIVERGENCE,)
    window = world.repository.ensure_window(world.plan.plan_id, WindowSpec(
        world.plan.cohort_id, "MASSIVE_DEVELOPER", "w1", NOW, NOW - timedelta(seconds=90), NOW + timedelta(seconds=300)))
    report = world.orchestrator.run_window(window.window_id)
    states = {stage: record.state for stage, record in world.repository.stage_states(window.window_id).items()}
    assert report.window_state == "COMPLETE" and set(states.values()) == {"COMPLETE"} and equity_ran
    assert world.repository.ready[(world.plan.cohort_id, "OPTIONS_FLOW")][0] == "BLOCKED"
    assert world.repository.ready[(world.plan.cohort_id, "PORTAL_PUBLISH")][0] == "READY"
    assert "OPTIONS_FLOW" not in states  # a resident stage has no window stage run


# --- source contract -----------------------------------------------------------------------------------------

def test_a_source_that_goes_backwards_or_changes_generation_fails_the_tick_with_a_code_only():
    class Backwards(SpySource):
        def read_after(self, offset, *, limit=1000):
            records = super().read_after(0, limit=limit)
            return records

    feed = scenario_feed()
    world = harness(Backwards(feed))
    outcome = world.tick()
    assert outcome.state == "FAILED" and outcome.reason_codes == (
        runners.OPTIONS_FLOW_SOURCE_FAILED, "SOURCE_OFFSET_NOT_MONOTONIC")

    class Broken(SpySource):
        def read_after(self, offset, *, limit=1000):
            raise OSError("C:\\secret\\journal.db is locked")

    broken = harness(Broken(feed))
    outcome = broken.tick()
    assert outcome.reason_codes == (runners.OPTIONS_FLOW_SOURCE_FAILED, "SOURCE_READ_FAILED")
    assert "secret" not in repr(outcome) and "secret" not in repr(broken.readiness())


def test_every_relation_the_runner_writes_is_writable_by_the_capability_role_it_uses():
    import re
    from pathlib import Path

    from realtime.option_flow import checkpoint_state
    from realtime.relation_registry import DEFAULT_READ_ROLES, PRINCIPAL_PROFILES, RELATIONS

    assert {"options_compute", "options_ingress"} <= set(PRINCIPAL_PROFILES["runtime_options"])
    # the writer's flush transaction (options_compute): rows, volume state and the checkpoint commit together
    for table in ("rt.option_flow_events", "rt.option_flow_clusters", "rt.option_volume_checkpoints",
                  "rt.consumer_checkpoints"):
        assert "options_compute" in RELATIONS[table].insert, table
    assert "options_compute" in RELATIONS["rt.option_volume_checkpoints"].update
    assert "options_compute" in RELATIONS["rt.consumer_checkpoints"].update  # the checkpoint upsert and the advance
    # gap rows are an ingress relation: options_compute may not write them, options_ingress may
    assert "options_ingress" in RELATIONS["rt.stream_gaps"].insert
    assert "options_compute" not in RELATIONS["rt.stream_gaps"].insert
    for table in ("ref.oi_baselines", "ref.option_contracts"):
        assert "options_compute" in DEFAULT_READ_ROLES and RELATIONS[table].insert == ("references",)
    # and the only tables the runner modules write themselves are exactly those two
    written = set()
    for module in (runners, checkpoint_state):
        for match in re.finditer(r"(?:INSERT INTO|UPDATE) ([a-z_]+\.[a-z_]+)", Path(module.__file__).read_text()):
            written.add(match.group(1))
    assert written == {"rt.consumer_checkpoints", "rt.stream_gaps"}


def test_an_unconfigured_runner_with_the_equity_stages_enabled_still_lets_a_window_complete():
    from realtime.orchestration_repository import WindowSpec

    world = harness(None)
    assert world.tick().reason_codes == (OPTIONS_FLOW_SOURCE_NOT_CONFIGURED,)
    window = world.repository.ensure_window(world.plan.plan_id, WindowSpec(
        world.plan.cohort_id, "MASSIVE_DEVELOPER", "w1", NOW, NOW - timedelta(seconds=90), NOW + timedelta(seconds=300)))
    assert world.orchestrator.run_window(window.window_id).window_state == "COMPLETE"
    assert world.readiness() == ("BLOCKED", (OPTIONS_FLOW_SOURCE_NOT_CONFIGURED,))


def test_a_rolled_over_session_starts_a_fresh_report_and_drops_the_accumulators_of_old_sessions():
    from realtime.option_flow.checkpoint_state import baseline_counts

    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(9001,))
    feed.disconnect()
    feed.add_trade(CALL, 2, T0 + 5_000, 5.0, 100, conditions=(9001,))
    world = harness(feed)
    first = world.tick()
    assert "OPTIONS_FLOW_GAP:1" in first.reason_codes
    consumer = world.environment.options_flow_state.session.consumer
    assert baseline_counts(consumer.engine) == {"INCOMPLETE": 1}
    plan = world.orchestrator._plans[world.plan.plan_id]
    world.orchestrator._plans[world.plan.plan_id] = ServicePlan(
        plan.plan_id, plan.cohort_id, SESSION + timedelta(days=2), plan.stage_catalog_version, plan.enabled_stages,
        plan.config_sha256, plan.deadline)
    outcome = world.tick()
    session = world.environment.options_flow_state.session
    assert session.start.mode == "RESTORED" and session.consumer.gaps_seen == 0
    assert baseline_counts(session.consumer.engine) == {}  # yesterday is gone from the books (and the next checkpoint)
    assert outcome.reason_codes == () and world.readiness() == ("READY", ())


def test_the_session_rolls_over_when_the_plan_session_changes():
    feed = scenario_feed()
    world = harness(feed)
    world.tick()
    first = world.environment.options_flow_state.session
    plan = world.orchestrator._plans[world.plan.plan_id]
    world.orchestrator._plans[world.plan.plan_id] = ServicePlan(
        plan.plan_id, plan.cohort_id, SESSION + timedelta(days=1), plan.stage_catalog_version, plan.enabled_stages,
        plan.config_sha256, plan.deadline)
    world.tick()
    second = world.environment.options_flow_state.session
    assert second is not first and second.session_date == SESSION + timedelta(days=1)
    assert second.start.mode == "RESTORED"  # the closing flush stored a checkpoint the new session restores
