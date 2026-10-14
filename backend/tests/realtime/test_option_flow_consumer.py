"""The OPTIONS_FLOW consumer against the in-memory options database: frame decoding, continuity checks,
gap ordering and, above all, restart equivalence: a session that crashes and restarts from a checkpoint
must end with exactly the rows (and engine state) of an uninterrupted session, whether the engine state
is checkpointed at every flush or only occasionally (the replay of already-durable offsets must reproduce
the stored rows exactly)."""
from __future__ import annotations

import json
from dataclasses import asdict
from uuid import uuid4

import pytest

from option_flow_runner_support import (
    FakeOptionsDatabase,
    MonotonicClock,
    harness,
    scenario_messages,
    settings,
)
from option_flow_support import CALL, CANCEL, OTHER, PUT, T0, policy, trade
from realtime.option_flow.consumer import (
    ConsumerLimits,
    GapEntry,
    OptionFlowConsumer,
    OptionFlowSourceError,
    decode_frame,
)
from realtime.option_flow.engine import EngineConfig, OptionFlowEngine
from realtime.option_flow.mock_feed import MockOptionFeed
from realtime.option_flow.records import EngineOutput
from realtime.option_flow.volume import BaselineKind, VolumeConfig
from realtime.option_flow.writer import OptionFlowReplayDivergence, OptionFlowWriter

REGULAR = 9001


# --- fixtures ---------------------------------------------------------------------------------------

class RecordingWriter:
    """Stands in for the OptionFlowWriter: records what the consumer hands over and in which order."""

    def __init__(self, hook, log):
        self.hook = hook
        self.log = log
        self.pending_rows = 0
        self.outputs = []

    def add(self, output, *, journal_generation=None):
        self.log.append(("add", len(output.events), len(output.gaps)))
        self.outputs.append(output)
        self.pending_rows += len(output.events) + len(output.clusters)

    def poll(self):
        self.log.append(("poll",))

    def flush(self, *, reason="FORCED"):
        self.log.append(("flush", reason))
        self.hook(None, None)
        self.pending_rows = 0

    def close(self):
        self.flush(reason="CLOSE")


def engine_for(feed, **overrides) -> OptionFlowEngine:
    from option_flow_runner_support import pinned_semantics

    values = dict(cohort_id=uuid4(), provider_tier="MASSIVE_ADVANCED",
                  policy=policy(sweep_window_seconds=5, minimum_sweep_prints=3), semantics=pinned_semantics(),
                  journal_generation=feed.generation, shard_count=2,
                  volume=VolumeConfig(initial_baseline=BaselineKind.SESSION_COMPLETE))
    values.update(overrides)
    return OptionFlowEngine(EngineConfig(**values))


def consumer_for(feed, *, engine=None, log=None, gap_log=None, limits=None, clock=None, source=None):
    log = log if log is not None else []
    gaps = gap_log if gap_log is not None else []
    consumer = OptionFlowConsumer(
        engine or engine_for(feed), source or feed, lambda hook: RecordingWriter(hook, log), generation=feed.generation,
        gap_sink=lambda entries: (log.append(("gaps", len(entries))), gaps.extend(entries)),
        limits=limits or ConsumerLimits(batch_size=3, max_messages=1000, max_seconds=1e6),
        clock=clock or MonotonicClock())
    return consumer, log, gaps


def frames(*messages):
    feed = MockOptionFeed()
    for message in messages:
        feed.append_frame(message if isinstance(message, list) else [message])
    return feed


# --- the read protocol and frames -------------------------------------------------------------------------

def test_a_frame_may_carry_several_messages_and_they_share_the_frames_offset():
    feed = frames([trade(seq=1, time_ms=T0, price=5.0, size=100), trade(seq=2, time_ms=T0 + 1, price=5.0, size=100)])
    consumer, _, _ = consumer_for(feed)
    report = consumer.run_slice()
    assert (report.records, report.messages, report.accepted) == (1, 2, 2) and consumer.fed_offset == 1
    assert consumer.engine.stats.persisted_events == 2


def test_control_frames_are_skipped_and_counted_not_submitted():
    feed = frames({"ev": "status", "status": "success", "message": "subscribed"},
                  [{"ev": "status", "status": "ok"}, trade(seq=1, time_ms=T0, price=5.0, size=100)])
    consumer, _, _ = consumer_for(feed)
    report = consumer.run_slice()
    assert (report.control_frames, report.messages) == (2, 1) and consumer.engine.stats.submitted == 1


def test_decode_frame_reads_arrays_objects_and_refuses_the_rest():
    assert decode_frame(b'[{"ev":"T"}]') == [{"ev": "T"}]
    assert decode_frame(b'{"ev":"T"}') == [{"ev": "T"}]
    for bad in (b"{nope", b"[1, 2]", b'["x"]', b"\xff\xfe"):
        assert decode_frame(bad) is None


def test_a_frame_with_a_non_object_item_is_unreadable_and_becomes_a_gap():
    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(REGULAR,))
    feed.append_payload(b'[{"ev":"T","sym":"x"}, 3]')
    consumer, _, gaps = consumer_for(feed)
    report = consumer.run_slice()
    assert report.markers == 1 and {entry.reason for entry in gaps} == {"UNREADABLE_FRAME"}
    assert consumer.gaps_seen == 1


def test_the_consumer_uses_only_the_read_protocol_of_its_source():
    class Strict:
        generation = None

        def __init__(self, feed):
            self._feed = feed
            self.generation = feed.generation
            self.calls = []

        def read_after(self, offset, *, limit=1000):
            self.calls.append(("read_after", offset, limit))
            return self._feed.read_after(offset, limit=limit)

        def last_offset(self):
            self.calls.append(("last_offset",))
            return self._feed.last_offset()

        def __getattr__(self, name):
            raise AssertionError(f"the consumer used source.{name}")

    feed = MockOptionFeed()
    feed.extend([trade(seq=i, time_ms=T0 + i, price=5.0, size=100) for i in range(1, 8)])
    source = Strict(feed)
    consumer, _, _ = consumer_for(feed, source=source, limits=ConsumerLimits(batch_size=3, max_messages=5, max_seconds=1e6))
    consumer.run_slice()
    reads = [call for call in source.calls if call[0] == "read_after"]
    assert reads and all(limit <= 3 for _, _, limit in reads)
    assert sum(limit for _, _, limit in reads) <= 6  # the last read asks only for what the message budget leaves
    assert consumer.backlog() == 7 - consumer.fed_offset


# --- bounds -----------------------------------------------------------------------------------------------

def test_a_slice_stops_at_the_message_bound_the_time_bound_and_on_cancellation():
    feed = MockOptionFeed()
    feed.extend([trade(seq=i, time_ms=T0 + i, price=5.0, size=100) for i in range(1, 31)])
    consumer, _, _ = consumer_for(feed, limits=ConsumerLimits(batch_size=4, max_messages=10, max_seconds=1e6))
    report = consumer.run_slice()
    assert report.stopped == "MAX_MESSAGES" and 10 <= report.messages <= 12

    consumer, _, _ = consumer_for(feed, clock=MonotonicClock(step=1.0),
                                  limits=ConsumerLimits(batch_size=2, max_messages=1000, max_seconds=3.0))
    assert consumer.run_slice().stopped == "MAX_SECONDS"

    consumer, _, _ = consumer_for(feed, limits=ConsumerLimits(batch_size=2, max_messages=1000, max_seconds=1e6))
    polls = []
    report = consumer.run_slice(is_cancelled=lambda: polls.append(1) or len(polls) > 2)
    assert report.stopped == "CANCELLED" and report.records == 4 and len(polls) == 3


def test_limits_reject_nonsense():
    for kwargs in (dict(batch_size=0), dict(max_messages=0), dict(max_seconds=0)):
        with pytest.raises(ValueError):
            ConsumerLimits(**kwargs)


def test_the_writers_interval_rule_is_polled_after_every_batch_not_only_at_the_end_of_a_slice():
    feed = MockOptionFeed()
    feed.extend([trade(seq=i, time_ms=T0 + i, price=5.0, size=100) for i in range(1, 10)])
    consumer, log, _ = consumer_for(feed, limits=ConsumerLimits(batch_size=3, max_messages=1000, max_seconds=1e6))
    consumer.run_slice()
    assert [item[0] for item in log].count("poll") == 4  # three batches and the end of the slice


def test_a_source_that_repeats_an_offset_or_changes_generation_is_refused():
    feed = MockOptionFeed()
    feed.extend([trade(seq=i, time_ms=T0 + i, price=5.0, size=100) for i in range(1, 4)])

    class Repeats:
        generation = feed.generation

        def read_after(self, offset, *, limit=1000):
            return feed.read_after(0, limit=limit)

    consumer, _, _ = consumer_for(feed, source=Repeats(), limits=ConsumerLimits(batch_size=2, max_messages=100, max_seconds=1e6))
    with pytest.raises(OptionFlowSourceError) as repeated:
        consumer.run_slice()
    assert repeated.value.code == "SOURCE_OFFSET_NOT_MONOTONIC"

    other = MockOptionFeed(seed="another journal")
    other.extend([trade(seq=1, time_ms=T0, price=5.0, size=100)])
    consumer, _, _ = consumer_for(feed, source=other)
    with pytest.raises(OptionFlowSourceError) as changed:
        consumer.run_slice()
    assert changed.value.code == "SOURCE_GENERATION_CHANGED"

    reordered = MockOptionFeed()
    reordered.add_trade(CALL, 1, T0, 5.0, 100, conditions=(REGULAR,))
    reordered.add_trade(CALL, 2, T0 + 1, 5.0, 100, conditions=(REGULAR,))
    records = list(reordered.records)
    records[1] = type(records[1])(2, records[1].generation, records[1].stream_session_id, 0, records[1].payload)

    class Backwards:
        generation = reordered.generation

        def read_after(self, offset, *, limit=1000):
            return tuple(record for record in records if record.raw_offset > offset)[:limit]

    consumer, _, _ = consumer_for(reordered, source=Backwards())
    with pytest.raises(OptionFlowSourceError) as regressed:
        consumer.run_slice()
    assert regressed.value.code == "SOURCE_MESSAGE_NUMBER_REGRESSED"


def test_a_failing_read_is_wrapped_and_keeps_only_the_error_type():
    class Down:
        generation = None

        def read_after(self, offset, *, limit=1000):
            raise TimeoutError("token=SECRET")

    feed = MockOptionFeed()
    consumer, _, _ = consumer_for(feed, source=type("S", (Down,), {"generation": feed.generation})())
    with pytest.raises(OptionFlowSourceError) as failed:
        consumer.run_slice()
    assert failed.value.code == "SOURCE_READ_FAILED" and failed.value.error_type == "TimeoutError"
    assert "SECRET" not in str(failed.value)


# --- continuity: gaps in stream order, durable before their rows ------------------------------------------------

def test_a_reconnect_a_lost_frame_and_an_offset_hole_reach_the_gap_sink_before_the_rows_of_their_batch():
    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(REGULAR,))
    feed.disconnect()
    feed.add_trade(CALL, 2, T0 + 5_000, 5.0, 100, conditions=(REGULAR,))
    feed.lose_messages(2)
    feed.add_trade(CALL, 3, T0 + 6_000, 5.0, 100, conditions=(REGULAR,))
    feed.skip_offsets(1)
    feed.add_trade(CALL, 4, T0 + 7_000, 5.0, 100, conditions=(REGULAR,))
    consumer, log, gaps = consumer_for(feed, limits=ConsumerLimits(batch_size=10, max_messages=100, max_seconds=1e6))
    consumer.run_slice()
    assert sorted({entry.reason for entry in gaps}) == [
        "GATEWAY_SESSION_CHANGED", "SOURCE_MESSAGE_HOLE", "SOURCE_OFFSET_HOLE"]
    assert log.index(("gaps", len(gaps))) < log.index(next(item for item in log if item[0] == "add"))
    assert consumer.gaps_seen == 3
    hole = next(entry for entry in gaps if entry.reason == "SOURCE_OFFSET_HOLE")
    assert (hole.first_offset, hole.last_offset, hole.last_durable_offset) == (4, 4, 3)
    change = next(entry for entry in gaps if entry.reason == "GATEWAY_SESSION_CHANGED")
    assert change.start_ms == T0 and change.end_ms == T0 + 5_000 and change.contracts == ()
    assert all(entry.stream_session_id == feed.records[-1].stream_session_id for entry in gaps)
    # the session change gap belongs to the session that saw it, and the old accumulator is INCOMPLETE
    out = EngineOutput()
    out.extend(consumer.engine.flush())
    assert {row.baseline_kind for row in out.checkpoints} == {"INCOMPLETE"}


def test_no_gap_is_invented_at_the_start_of_a_stream_or_inside_a_clean_session():
    feed = MockOptionFeed()
    feed.skip_offsets(5)  # a journal that starts at offset 6: nothing earlier was ever fed
    feed.extend([trade(seq=i, time_ms=T0 + i, price=5.0, size=100) for i in range(1, 6)])
    consumer, _, gaps = consumer_for(feed)
    consumer.run_slice()
    assert gaps == [] and consumer.gaps_seen == 0 and consumer.fed_offset == feed.last_offset()


def test_an_overloaded_engine_records_an_explicit_gap_per_dropped_run_and_never_queues_without_bound():
    feed = MockOptionFeed()
    feed.append_frame([trade(seq=i, time_ms=T0 + i, price=5.0, size=100, exchange=300 + i % 3) for i in range(1, 31)])
    consumer, _, gaps = consumer_for(feed, engine=engine_for(feed, shard_count=1, queue_capacity=5))
    report = consumer.run_slice()
    stats = consumer.engine.stats
    assert report.gap_recorded == stats.queue_dropped > 0 and report.accepted + report.gap_recorded == 30
    assert consumer.engine.queued() == 0 and stats.accounted() == stats.submitted == 30
    assert {entry.reason for entry in gaps} == {"BACKPRESSURE_BUFFER_FULL"}
    (entry,) = gaps
    assert entry.dropped_messages == stats.queue_dropped and entry.first_offset == entry.last_offset == 1
    assert entry.stream_session_id == feed.records[0].stream_session_id
    assert consumer.gaps_seen == 1


def test_a_gap_sink_failure_keeps_the_batch_and_the_retry_delivers_it_once():
    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(REGULAR,))
    feed.disconnect()
    feed.add_trade(CALL, 2, T0 + 5_000, 5.0, 100, conditions=(REGULAR,))
    calls = []

    def flaky(entries):
        calls.append(len(entries))
        if len(calls) == 1:
            raise RuntimeError("stream session not registered")

    log = []
    consumer = OptionFlowConsumer(engine_for(feed), feed, lambda hook: RecordingWriter(hook, log),
                                  generation=feed.generation, gap_sink=flaky,
                                  limits=ConsumerLimits(batch_size=10, max_messages=100, max_seconds=1e6))
    with pytest.raises(RuntimeError):
        consumer.run_slice()
    assert [item for item in log if item[0] == "add"] == []  # nothing was handed to the writer
    consumer.run_slice()
    assert len(calls) == 2 and len([item for item in log if item[0] == "add"]) == 1
    assert consumer.engine.stats.persisted_events == 2


def test_reconcile_and_mark_reconciled_clear_the_warming_and_gap_report_only_when_the_books_are_complete():
    feed = MockOptionFeed()
    feed.add_trade(CALL, 1, T0, 5.0, 100, conditions=(REGULAR,))
    feed.disconnect()
    feed.add_trade(PUT, 1, T0 + 5_000, 5.0, 100, conditions=(REGULAR,))
    consumer, _, _ = consumer_for(feed, engine=engine_for(feed, volume=VolumeConfig(initial_baseline=BaselineKind.WARMING)))
    consumer.warming_reason = "NO_CHECKPOINT"
    consumer.run_slice()
    assert consumer.gaps_seen == 1 and consumer.mark_reconciled() is False
    assert consumer.warming_reason == "NO_CHECKPOINT"  # unchanged while accumulators are WARMING / INCOMPLETE
    for contract in (CALL, PUT):
        result = consumer.reconcile(contract, 1_000, 0, T0 + 10_000)
        assert result.accepted and result.baseline_kind == "SNAPSHOT_RECONCILED"
    assert consumer.mark_reconciled() is True
    assert consumer.warming_reason is None and consumer.gaps_seen == 0


def test_the_checkpoint_carries_the_continuity_state_so_a_gap_during_downtime_is_still_seen():
    feed = MockOptionFeed()
    feed.extend([trade(seq=i, time_ms=T0 + i, price=5.0, size=100) for i in range(1, 4)])
    first, _, _ = consumer_for(feed)
    first.run_slice()
    extras = first.extras()
    assert extras["next_message_number"] == 3 and extras["last_event_ms"] == T0 + 3 and extras["session_id"]
    json.dumps(extras)  # it travels inside the checkpoint document


# --- restart equivalence ---------------------------------------------------------------------------------------

def restart_feed(frame_size: int = 1) -> MockOptionFeed:
    messages = scenario_messages(T0)
    half = len(messages) // 2
    feed = MockOptionFeed()
    feed.extend(messages[:half], frame_size=frame_size)
    feed.disconnect()  # a gateway reconnect in the middle of the session
    feed.extend(messages[half:], frame_size=frame_size)
    feed.add_status()
    return feed


def run_to_end(world, *, finish=True):
    state = world.environment.options_flow_state
    for _ in range(500):
        outcome = world.tick()
        assert outcome.state == "COMPLETE", outcome
        if state.last_report.stopped == "CAUGHT_UP":
            break
    consumer = state.session.consumer
    if finish:
        consumer.finish()
    return consumer


def persisted(database):
    return (sorted(database.events), sorted(database.clusters), database.volume_rows(), sorted(database.gaps))


@pytest.mark.parametrize("frame_size", [1, 3])
@pytest.mark.parametrize("checkpoint_interval", [0.0, 1e9], ids=["state-at-every-flush", "state-once-then-offsets"])
@pytest.mark.parametrize("crash_after", [3, 8, 12, 19, 25, 33, 41])
def test_a_crash_and_restart_ends_with_the_rows_and_state_of_an_uninterrupted_session(
        crash_after, checkpoint_interval, frame_size):
    cohort = uuid4()
    feed = restart_feed(frame_size)
    options = dict(checkpoint_interval_seconds=checkpoint_interval)

    reference_db = FakeOptionsDatabase()
    reference = harness(feed, database=reference_db, cohort_id=cohort, flow_settings=settings(**options))
    uninterrupted = run_to_end(reference)

    database = FakeOptionsDatabase()
    limits = ConsumerLimits(batch_size=2, max_messages=crash_after, max_seconds=1e6)
    first = harness(feed, database=database, cohort_id=cohort, flow_settings=settings(limits=limits, **options))
    first.tick()
    crashed = first.environment.options_flow_state.session.consumer
    assert 0 < crashed.fed_offset <= feed.last_offset()
    del first, crashed  # the process dies: no flush, no finish, nothing in memory survives
    had_checkpoint = bool(database.checkpoints)

    second = harness(feed, database=database, cohort_id=cohort, flow_settings=settings(**options))
    resumed = run_to_end(second)

    start = second.environment.options_flow_state.session.start
    assert start.mode == ("RESTORED" if had_checkpoint else "WARMING"), start
    assert persisted(database) == persisted(reference_db)
    assert resumed.engine.state_digest() == uninterrupted.engine.state_digest()
    assert asdict(resumed.engine.stats) == asdict(uninterrupted.engine.stats)
    assert resumed.gaps_seen == uninterrupted.gaps_seen and resumed.fed_offset == uninterrupted.fed_offset
    assert uninterrupted.gaps_seen >= 1 and reference_db.gaps


def test_offsets_only_flushes_replay_the_durable_window_without_moving_stored_offsets_backwards():
    cohort = uuid4()
    feed = restart_feed()
    database = FakeOptionsDatabase()
    limits = ConsumerLimits(batch_size=2, max_messages=30, max_seconds=1e6)
    first = harness(feed, database=database, cohort_id=cohort,
                    flow_settings=settings(limits=limits, checkpoint_interval_seconds=1e9))
    first.tick()
    consumer = first.environment.options_flow_state.session.consumer
    state_offset, durable = consumer.checkpointed_offset, consumer.flushed_offset
    assert 0 < state_offset < durable == consumer.fed_offset  # rows are ahead of the stored state
    stored = {row[0] for row in database.checkpoints.values()}
    assert stored == {durable}  # the row offset advanced; the payload still describes `state_offset`
    second = harness(feed, database=database, cohort_id=cohort, flow_settings=settings(checkpoint_interval_seconds=1e9))
    second.tick()
    session = second.environment.options_flow_state.session
    assert session.start.mode == "RESTORED" and session.start.offset == state_offset
    assert session.consumer.checkpointed_offset >= state_offset
    assert min(row[0] for row in database.checkpoints.values()) >= durable  # never backwards


def test_a_replay_that_no_longer_matches_the_stored_rows_is_a_divergence_not_a_silent_skip():
    cohort = uuid4()
    feed = restart_feed()
    database = FakeOptionsDatabase()
    options = dict(checkpoint_interval_seconds=1e9)
    first = harness(feed, database=database, cohort_id=cohort,
                    flow_settings=settings(limits=ConsumerLimits(batch_size=2, max_messages=30, max_seconds=1e6), **options))
    first.tick()
    consumer = first.environment.options_flow_state.session.consumer
    state_offset, durable = consumer.checkpointed_offset, consumer.flushed_offset
    assert state_offset < durable
    # The journal now holds a different size for a print inside the durable window that a restart replays.
    candidates = [record.raw_offset for record in feed.read_after(state_offset, limit=durable - state_offset)
                  if any(message.get("ev") == "T" and message.get("s") == 100 and message.get("p") == 5.0
                         for message in json.loads(record.payload))]
    assert candidates
    changed = feed.divergent_replay({candidates[0]: {"s": 101}})
    second = harness(changed, database=database, cohort_id=cohort, flow_settings=settings(**options))
    outcome = second.tick()
    assert outcome.reason_codes == ("REPLAY_DIVERGENCE",) and second.readiness()[0] == "BLOCKED"
    quarantine = second.environment.options_flow_state.quarantine
    assert quarantine.last_offset <= durable + 2 and quarantine.identities


# --- measurement: sustained rate through consumer, engine and writer (no database) -----------------------------------

class NullCursor:
    """Accepts the writer's COPY and INSERT statements and the checkpoint upsert; stores nothing."""

    def __init__(self):
        self.rowcount = 0
        self._copied = 0
        self.copied = {"events": 0, "clusters": 0, "volume": 0}

    def copy_expert(self, statement, file):
        text = file.read()
        self._copied = text.count("\n")
        for name, marker in (("events", "option_flow_events"), ("clusters", "option_flow_clusters"),
                             ("volume", "option_volume_checkpoints")):
            if marker in statement:
                self.copied[name] += self._copied

    def execute(self, statement, parameters=None):
        text = " ".join(statement.split())
        if text.startswith("INSERT INTO rt.consumer_checkpoints"):
            self.rowcount = 1
        elif text.startswith("INSERT INTO"):
            self.rowcount = self._copied
        elif text.startswith("UPDATE rt.consumer_checkpoints"):
            self.rowcount = 4

    def fetchall(self):
        return []

    def close(self):
        pass


class NullConnection:
    autocommit = False

    def __init__(self):
        self.cursor_ = NullCursor()

    def cursor(self):
        return self.cursor_

    def commit(self):
        pass

    def rollback(self):
        pass


@pytest.mark.parametrize("checkpoint_interval", [None, 10.0, 0.0],
                         ids=["no-checkpoint", "state-every-10s", "state-every-flush"])
def test_sustained_message_rate_through_the_consumer_the_engine_and_the_writer(checkpoint_interval, capsys):
    import time

    from option_flow_runner_support import pinned_semantics
    from realtime.option_flow.checkpoint_state import CheckpointStore
    from realtime.option_flow.mock_feed import generate_messages

    contracts = [f"O:{root}261218C{10_000 + index * 500:08d}" for root in ("AAA", "BBB", "CCC", "DDD", "EEE")
                 for index in range(200)]
    quoted = contracts[:100]
    total = 120_000
    messages = generate_messages(contracts=contracts, count=total, base_ms=T0, rate_per_second=2_000, quoted=quoted,
                                 regular_condition=REGULAR, big_fraction=0.03)
    feed = MockOptionFeed(seed="throughput")
    feed.extend(messages, frame_size=10)  # the gateway journals provider frames of several messages
    engine = OptionFlowEngine(EngineConfig(
        cohort_id=uuid4(), provider_tier="MASSIVE_ADVANCED", policy=policy(sweep_window_seconds=5, minimum_sweep_prints=3),
        semantics=pinned_semantics(), journal_generation=feed.generation, shard_count=4,
        quote_universe=frozenset(quoted), queue_capacity=200_000,
        volume=VolumeConfig(history_per_contract=512, initial_baseline=BaselineKind.SESSION_COMPLETE)))
    connection = NullConnection()
    store = None if checkpoint_interval is None else CheckpointStore(
        consumer_name="throughput", cohort_id=engine.config.cohort_id, generation=feed.generation,
        max_state_bytes=100_000_000)  # measured unbounded: the database row cap (1 MiB) is reported below
    consumer = OptionFlowConsumer(
        engine, feed, lambda hook: OptionFlowWriter(connection, in_transaction=hook), generation=feed.generation,
        checkpoints=store, limits=ConsumerLimits(batch_size=500, max_messages=10_000, max_seconds=1e9),
        checkpoint_interval_seconds=checkpoint_interval or 0.0)
    started = time.perf_counter()
    while consumer.run_slice().stopped != "CAUGHT_UP":
        pass
    consumer.finish()
    elapsed = time.perf_counter() - started

    stats = engine.stats
    assert stats.submitted == stats.accounted() == total and stats.queue_dropped == 0 and engine.queued() == 0
    assert connection.cursor_.copied["events"] == stats.persisted_events + stats.cancel_revisions
    assert consumer.fed_offset == feed.last_offset() == total // 10
    checkpoints = "" if store is None else (
        f", {store.sequence} full checkpoints (last {store.last_write.payload_bytes / 1e6:.2f} MB zlib, largest row "
        f"{store.last_write.largest_row_bytes / 1e3:.0f} KB vs the 1,048 KB row cap, {store.last_write.seconds * 1000:.0f} ms)")
    with capsys.disabled():
        print(f"\nWINDOWS_CONSTRAINED consumer+engine+writer (no database), {checkpoint_interval!r} checkpoint: "
              f"{total:,} messages in {elapsed:.2f}s = {total / elapsed:,.0f} msg/s "
              f"({stats.persisted_events:,} events, {connection.cursor_.copied['clusters']:,} cluster rows, "
              f"{connection.cursor_.copied['volume']:,} volume rows{checkpoints})")
