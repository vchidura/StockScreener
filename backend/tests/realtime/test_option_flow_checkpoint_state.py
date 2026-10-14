"""Engine checkpoint serialization: a restored engine must continue exactly like the uninterrupted one, and a
checkpoint that cannot be proven identical is rejected with a stable code (never half trusted)."""
from __future__ import annotations

import json
import time
import zlib
from dataclasses import asdict, fields, replace
from datetime import date

import pytest

import realtime.option_flow.checkpoint_state as state
from option_flow_support import (
    CALL,
    CANCEL,
    COHORT,
    JOURNAL_GENERATION,
    MULTI_LEG,
    OTHER,
    PUT,
    REGULAR,
    T0,
    UNKNOWN_CODE,
    config,
    policy,
    quote,
    trade,
)
from realtime.option_flow.checkpoint_state import (
    CheckpointRejected,
    CheckpointRegression,
    CheckpointRow,
    CheckpointStore,
    CheckpointTooLarge,
    EngineNotQuiescent,
    encode_checkpoint,
    restore_checkpoint,
)
from realtime.option_flow.clusters import ClusterEngine
from realtime.option_flow.clusters import _State as ClusterState
from realtime.option_flow.engine import EngineStats, GapMarker, OptionFlowEngine
from realtime.option_flow.labels import PriorOpenInterest
from realtime.option_flow.mock_feed import generate_messages
from realtime.option_flow.quote_cache import QuoteCache, QuoteView
from realtime.option_flow.records import EngineOutput, FlowEvent
from realtime.option_flow.volume import BaselineKind, ContractVolume, PrintRecord, VolumeBook, VolumeConfig

IWM = "O:IWM261218C00200000"
DIA = "O:DIA261218C00400000"
CONSUMER = "option_flow_stream_v1"
SESSION = date(2026, 10, 12)
EXTRAS = {"session_id": None, "next_message_number": 7, "last_event_ms": T0, "gaps_seen": 2, "warming_reason": None}


def _config(**overrides):
    values = dict(shard_count=3, policy=policy(sweep_window_seconds=5, minimum_sweep_prints=3),
                  cluster_windows_seconds=(5, 60), quote_universe=frozenset({CALL, PUT}),
                  volume=VolumeConfig(history_per_contract=16, initial_baseline=BaselineKind.SESSION_COMPLETE))
    values.update(overrides)
    return config(**values)


def _engine(**overrides) -> OptionFlowEngine:
    prior = {CALL: PriorOpenInterest(300, date(2026, 10, 9)), PUT: PriorOpenInterest(100, date(2026, 10, 9))}
    return OptionFlowEngine(_config(**overrides), prior)


def scenario():
    """(kind, value) steps: provider messages and every control the engine accepts."""
    steps = [("msg", quote(seq=1, time_ms=T0 - 100, bid=4.9, ask=5.0)),
             ("msg", quote(symbol=PUT, seq=1, time_ms=T0 - 100, bid=2.0, ask=2.1))]
    steps += [("msg", trade(seq=seq, time_ms=T0 + seq, price=1.0, size=10)) for seq in range(1, 4)]
    steps += [("msg", trade(seq=10 + index, time_ms=T0 + 100 + index * 100, price=5.0, size=100, exchange=302 + index))
              for index in range(3)]
    steps.append(("msg", trade(seq=20, time_ms=T0 + 600, price=5.0, size=150, exchange=305)))
    steps.append(("msg", trade(seq=20, time_ms=T0 + 600, price=5.0, size=150, exchange=305)))  # a duplicate
    steps.append(("msg", trade(seq=21, time_ms=T0 + 700, price=5.0, size=150, exchange=305, conditions=(CANCEL,))))
    steps.append(("msg", trade(symbol=PUT, seq=1, time_ms=T0 + 1000, price=2.1, size=300, exchange=310)))
    steps.append(("msg", trade(seq=22, time_ms=T0 + 800, price=5.0, size=100, conditions=(MULTI_LEG,))))
    steps.append(("msg", trade(seq=23, time_ms=T0 + 900, price=5.0, size=100, conditions=(UNKNOWN_CODE,))))
    steps.append(("gap", GapMarker("GATEWAY_SESSION_CHANGED", T0 + 1000, T0 + 1200, "Q", (CALL,), 30, 31)))
    steps += [("msg", trade(symbol=OTHER, seq=1 + index, time_ms=T0 + 1100 + index * 50, price=5.0, size=100,
                            exchange=311 + index)) for index in range(3)]
    steps.append(("reconcile", (OTHER, 400, 0, T0 + 1500)))
    steps.append(("msg", trade(symbol=IWM, seq=30, time_ms=T0 + 1300, price=5.0, size=100, exchange=330)))
    steps.append(("msg", trade(symbol=IWM, seq=30, time_ms=T0 + 1300, price=5.0, size=100, exchange=330,
                               conditions=(CANCEL,))))
    burst = [(40 + index, T0 + 1400 + index * 50, 320 + index) for index in range(3)]
    steps += [("msg", trade(symbol=DIA, seq=seq, time_ms=time_ms, price=5.0, size=100, exchange=exchange))
              for seq, time_ms, exchange in burst]
    steps.append(("universe", frozenset({CALL})))
    steps.append(("msg", quote(seq=2, time_ms=T0 + 1450, bid=4.9, ask=5.0)))
    steps.append(("msg", trade(seq=24, time_ms=T0 + 1500, price=5.0, size=120, exchange=306)))
    steps += [("msg", trade(symbol=DIA, seq=seq, time_ms=time_ms, price=5.0, size=100, exchange=exchange,
                            conditions=(CANCEL,))) for seq, time_ms, exchange in burst[:2]]
    steps.append(("msg", trade(seq=15, time_ms=T0 + 250, price=5.0, size=110, exchange=307)))  # a late print
    steps += [("msg", trade(seq=100 + index, time_ms=T0 + 3000 + index * 40, price=5.0, size=100,
                            exchange=300 + index % 6)) for index in range(8)]
    steps.append(("time", T0 + 60_000))
    return steps


STEPS = scenario()


def apply(engine: OptionFlowEngine, step, offset: int) -> EngineOutput:
    kind, value = step
    if kind == "msg":
        return engine.feed(value, offset)
    if kind == "gap":
        engine.submit_gap(value)
    elif kind == "reconcile":
        engine.submit_reconcile(*value)
    elif kind == "universe":
        engine.submit_quote_universe(value)
    elif kind == "time":
        engine.submit_time(value)
    return engine.process()


def run_steps(engine: OptionFlowEngine, steps, first_offset: int = 1) -> EngineOutput:
    out = EngineOutput()
    for index, step in enumerate(steps):
        out.extend(apply(engine, step, first_offset + index))
    return out


def merged(first: EngineOutput, second: EngineOutput) -> EngineOutput:
    out = EngineOutput()
    out.extend(first)
    out.extend(second)
    return out


def _rows(engine, offset, *, max_bytes=state.DEFAULT_MAX_STATE_BYTES):
    rows, _ = encode_checkpoint(engine, consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION,
                                offset=offset, sequence=5, extras=EXTRAS, max_state_bytes=max_bytes)
    return rows


def _restore(rows, engine=None, **overrides):
    engine = engine or _engine()
    arguments = dict(consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION, source_tail=None)
    arguments.update(overrides)
    restored = restore_checkpoint(engine, rows, **arguments)
    return engine, restored


# --- exact continuation ------------------------------------------------------------------------------

def test_the_scenario_exercises_every_kind_of_state():
    engine = _engine()
    out = run_steps(engine, STEPS)
    out.extend(engine.flush(final=True))
    assert {event.trade_treatment for event in out.events} >= {"INCLUDED", "CANCELED", "UNKNOWN"}
    assert {row.status for row in out.clusters} >= {"PROVISIONAL", "FINAL"}
    assert out.reconciles and out.gaps and engine.stats.duplicates and engine.stats.late_prints
    assert any(event.volume_exceeds_prior_oi for event in out.events)
    assert {row.baseline_kind for row in out.checkpoints} >= {"SESSION_COMPLETE", "SNAPSHOT_RECONCILED", "INCOMPLETE"}


@pytest.mark.parametrize("split", range(1, len(STEPS)))
def test_a_restored_engine_continues_exactly_like_the_uninterrupted_one(split):
    uninterrupted = _engine()
    expected = run_steps(uninterrupted, STEPS)
    expected.extend(uninterrupted.flush(final=True))

    before = _engine()
    first = run_steps(before, STEPS[:split])
    rows = _rows(before, split)
    after, restored = _restore(rows)
    assert restored.offset == split and restored.sequence == 5 and restored.extras == EXTRAS
    assert after.state_digest() == before.state_digest()
    assert asdict(after.stats) == asdict(before.stats)
    second = run_steps(after, STEPS[split:], first_offset=split + 1)
    second.extend(after.flush(final=True))

    combined = merged(first, second)
    assert combined.events == expected.events
    assert combined.clusters == expected.clusters
    assert combined.checkpoints == expected.checkpoints
    assert combined.gaps == expected.gaps and combined.reconciles == expected.reconciles
    assert after.state_digest() == uninterrupted.state_digest()
    assert asdict(after.stats) == asdict(uninterrupted.stats)


def test_a_checkpoint_taken_after_a_restore_is_byte_identical_to_one_taken_without_it():
    original = _engine()
    run_steps(original, STEPS[:30])
    restored, _ = _restore(_rows(original, 30))
    assert [row.state_hash for row in _rows(restored, 30)] == [row.state_hash for row in _rows(original, 30)]
    run_steps(original, STEPS[30:], first_offset=31)
    run_steps(restored, STEPS[30:], first_offset=31)
    assert [row.state_hash for row in _rows(restored, 99)] == [row.state_hash for row in _rows(original, 99)]


def test_stored_events_survive_the_round_trip_field_for_field():
    engine = _engine()
    out = run_steps(engine, STEPS)
    assert out.events
    for event in out.events:
        copy = state._event_in(json.loads(json.dumps(state._event_out(event))))
        assert copy == event and all(
            type(getattr(copy, field.name)) is type(getattr(event, field.name)) for field in fields(FlowEvent))


def test_encoding_is_deterministic_and_stores_one_hashed_compressed_row_per_shard():
    engine = _engine()
    run_steps(engine, STEPS[:25])
    first, second = _rows(engine, 25), _rows(engine, 25)
    assert first == second and [row.shard_id for row in first] == [0, 1, 2]
    for row in first:
        assert row.state_codec == state.CODEC and row.committed_offset == 25
        raw = zlib.decompress(row.state_payload)
        assert len(row.state_hash) == 64 and row.state_hash == __import__("hashlib").sha256(raw).hexdigest()
        header = json.loads(raw)["header"]
        assert header["policy"] == engine.policy_sha256 and header["offset"] == 25 and header["sequence"] == 5
        assert header["schema"] == state.CHECKPOINT_SCHEMA_VERSION and header["shards"] == 3


def test_an_engine_with_queued_work_cannot_be_captured():
    engine = _engine()
    engine.submit(trade(seq=1, time_ms=T0, price=5.0, size=100), 1)
    with pytest.raises(EngineNotQuiescent):
        _rows(engine, 1)
    engine.process()
    assert _rows(engine, 1)


def test_a_state_over_the_size_bound_is_refused_whole_never_truncated():
    engine = _engine()
    run_steps(engine, STEPS)
    with pytest.raises(CheckpointTooLarge) as refused:
        _rows(engine, 5, max_bytes=100)
    assert refused.value.limit == 100 and refused.value.size > 100


# --- rejection ------------------------------------------------------------------------------------------

def _good_rows():
    engine = _engine()
    run_steps(engine, STEPS[:30])
    return _rows(engine, 30)


def _tamper(row: CheckpointRow, change) -> CheckpointRow:
    document = json.loads(zlib.decompress(row.state_payload))
    change(document)
    raw = json.dumps(document, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    import hashlib

    return replace(row, state_hash=hashlib.sha256(raw).hexdigest(), state_payload=zlib.compress(raw))


def _expect(code, rows, **overrides):
    with pytest.raises(CheckpointRejected) as rejected:
        _restore(rows, **overrides)
    assert rejected.value.code == code, rejected.value
    return rejected.value


def test_a_hash_that_does_not_match_the_content_is_rejected():
    rows = _good_rows()
    rows[1] = replace(rows[1], state_hash="0" * 64)
    assert _expect("CHECKPOINT_HASH_MISMATCH", rows).offset == 30


def test_a_flipped_payload_byte_is_unreadable_or_a_hash_mismatch():
    rows = _good_rows()
    damaged = bytearray(rows[0].state_payload)
    damaged[len(damaged) // 2] ^= 0xFF
    rows[0] = replace(rows[0], state_payload=bytes(damaged))
    with pytest.raises(CheckpointRejected) as rejected:
        _restore(rows)
    assert rejected.value.code in ("CHECKPOINT_UNREADABLE", "CHECKPOINT_HASH_MISMATCH")


def test_other_stored_forms_are_rejected_with_their_own_codes():
    rows = _good_rows()
    _expect("CHECKPOINT_BAD_CODEC", [replace(rows[0], state_codec="x"), *rows[1:]])
    _expect("CHECKPOINT_UNREADABLE", [replace(rows[0], state_payload=b"not zlib"), *rows[1:]])
    _expect("CHECKPOINT_SHARD_MAP_MISMATCH", [replace(rows[0], shard_map_sha256="f" * 64), *rows[1:]])
    _expect("CHECKPOINT_INCOMPLETE_SET", rows[:2])
    _expect("CHECKPOINT_INCOMPLETE_SET", [rows[0], rows[1], replace(rows[1], shard_id=1)])
    bad_offset = [rows[0], rows[1], replace(rows[2], committed_offset=29)]
    _expect("CHECKPOINT_INCOMPLETE_SET", bad_offset)
    other_sequence = [rows[0], rows[1], _tamper(rows[2], lambda document: document["header"].update(sequence=9))]
    _expect("CHECKPOINT_INCOMPLETE_SET", other_sequence)
    notdict = zlib.compress(b"[1,2]")
    import hashlib

    _expect("CHECKPOINT_UNREADABLE", [replace(rows[0], state_payload=notdict,
                                              state_hash=hashlib.sha256(b"[1,2]").hexdigest()), *rows[1:]])


def test_a_different_policy_configuration_version_or_identity_is_rejected():
    rows = _good_rows()
    other_policy = _engine(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=4))
    _expect("CHECKPOINT_POLICY_MISMATCH", rows, engine=other_policy)
    other_config = _engine(volume=VolumeConfig(history_per_contract=17, initial_baseline=BaselineKind.SESSION_COMPLETE))
    _expect("CHECKPOINT_CONFIG_MISMATCH", rows, engine=other_config)
    stale = [_tamper(row, lambda document: document["header"].update(layout="0" * 64)) for row in rows]
    _expect("CHECKPOINT_VERSION_MISMATCH", stale)
    old_schema = [_tamper(row, lambda document: document["header"].update(schema=0)) for row in rows]
    _expect("CHECKPOINT_VERSION_MISMATCH", old_schema)
    _expect("CHECKPOINT_IDENTITY_MISMATCH", rows, consumer_name="another_consumer")
    from uuid import uuid4

    _expect("CHECKPOINT_IDENTITY_MISMATCH", rows, generation=uuid4())
    fewer_shards = _engine(shard_count=2)
    _expect("CHECKPOINT_INCOMPLETE_SET", rows, engine=fewer_shards)


def test_a_checkpoint_ahead_of_the_source_is_rejected_and_one_at_the_tail_is_accepted():
    rows = _good_rows()
    refused = _expect("CHECKPOINT_AHEAD_OF_SOURCE", rows, source_tail=29)
    assert refused.offset == 30
    _, restored = _restore(rows, source_tail=30)
    assert restored.offset == 30
    _, restored = _restore(rows, source_tail=None)  # an unknown tail cannot refute the checkpoint
    assert restored.offset == 30


def test_state_that_restores_to_a_different_digest_is_rejected_even_with_a_valid_hash():
    rows = _good_rows()

    def inflate(document):
        for session in document["state"]["book"]["sessions"]:
            for contract in session[1]:
                contract[1] += 1
                return

    tampered = [_tamper(row, inflate) for row in rows]
    _expect("CHECKPOINT_STATE_DIGEST_MISMATCH", tampered)


def test_a_restore_that_cannot_decode_the_state_is_rejected_not_raised():
    rows = _good_rows()
    broken = [_tamper(row, lambda document: document["state"]["book"].pop("floors")) for row in rows]
    _expect("CHECKPOINT_UNREADABLE", broken)


# --- the layout guard -------------------------------------------------------------------------------------

def test_every_attribute_of_the_state_holders_is_serialized_or_declared_derived():
    engine = _engine()
    shard = engine._shards[0]
    covered = state.SERIALIZED_ATTRIBUTES
    actual = {
        "VolumeBook": set(vars(shard.book)),
        "ContractVolume": set(ContractVolume.__slots__),
        "PrintRecord": set(PrintRecord.__slots__),
        "QuoteCache": set(QuoteCache.__slots__),
        "QuoteView": set(QuoteView._fields),
        "ClusterEngine": set(vars(shard.clusters)),
        "ClusterState": set(ClusterState.__slots__),
        "Shard": set(type(shard).__slots__),
    }
    for name, attributes in actual.items():
        declared = covered[name] | covered.get(f"{name}.derived", frozenset())
        assert attributes == declared, f"{name}: new or removed attributes {attributes ^ declared} need serialization"
    assert {field.name for field in fields(EngineStats)} and set(vars(engine.stats.__class__)) is not None


def test_adding_an_attribute_to_a_state_holder_changes_the_layout_hash():
    engine = _engine()
    before = state.layout_sha256(engine)
    engine._shards[0].book.extra_state = 1
    assert state.layout_sha256(engine) != before


# --- storage -----------------------------------------------------------------------------------------------

class RecordingCursor:
    def __init__(self, stored=None, refuse_after=None):
        self.statements = []
        self.rowcount = 1
        self.stored = stored or {}
        self.refuse_after = refuse_after

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        if statement.startswith("INSERT"):
            shard, offset, allow = parameters[3], parameters[6], parameters[-1]
            stored = self.stored.get(shard)
            self.rowcount = 0 if (stored is not None and not allow and offset < stored) else 1
            if self.rowcount:
                self.stored[shard] = offset


def test_the_store_upserts_one_row_per_shard_and_bumps_the_sequence():
    engine = _engine()
    run_steps(engine, STEPS[:20])
    store = CheckpointStore(consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION, owner_epoch=3)
    cursor = RecordingCursor()
    first = store.write(cursor, engine, offset=20, extras=EXTRAS)
    second = store.write(cursor, engine, offset=21, extras=EXTRAS)
    assert (first.sequence, second.sequence, store.sequence) == (1, 2, 2) and first.payload_bytes > 0
    assert len(cursor.statements) == 6
    statement, parameters = cursor.statements[0]
    assert "ON CONFLICT (consumer_name, cohort_id, journal_generation, shard_id) DO UPDATE" in statement
    assert "EXCLUDED.committed_offset >= rt.consumer_checkpoints.committed_offset" in statement
    assert parameters[:4] == (CONSUMER, str(COHORT), str(JOURNAL_GENERATION), 0) and parameters[5] == 3
    assert parameters[8] == state.CODEC and len(parameters[9]) == 64 and parameters[-1] is False
    assert all(len(set(parameters[7] for _, parameters in cursor.statements[:3])) == 3 for _ in [0])  # unique ids


def test_the_regression_guard_raises_unless_one_replacement_is_explicitly_allowed():
    engine = _engine()
    run_steps(engine, STEPS[:20])
    store = CheckpointStore(consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION)
    cursor = RecordingCursor(stored={0: 50, 1: 50, 2: 50})
    with pytest.raises(CheckpointRegression):
        store.write(cursor, engine, offset=20, extras=EXTRAS)
    store.allow_regression = True
    store.write(cursor, engine, offset=20, extras=EXTRAS)
    assert store.allow_regression is False and cursor.stored == {0: 20, 1: 20, 2: 20}
    with pytest.raises(CheckpointRegression):
        store.write(RecordingCursor(stored={0: 99}), engine, offset=20, extras=EXTRAS)


def test_the_store_validates_its_arguments():
    with pytest.raises(ValueError):
        CheckpointStore(consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION, owner_epoch=0)
    with pytest.raises(ValueError):
        encode_checkpoint(_engine(), consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION,
                          offset=-1, sequence=1, extras={})


# --- measurement (state NOT_CHECKED for real T.* rates) --------------------------------------------------------

def test_checkpoint_size_and_time_at_one_thousand_contracts(capsys):
    contracts = [f"O:{root}261218C{10_000 + index * 500:08d}" for root in ("AAA", "BBB", "CCC", "DDD", "EEE")
                 for index in range(200)]
    assert len(set(contracts)) == 1000
    quoted = contracts[:100]
    engine = OptionFlowEngine(config(
        shard_count=4, policy=policy(sweep_window_seconds=5, minimum_sweep_prints=3), cluster_windows_seconds=(5, 60),
        quote_universe=frozenset(quoted), queue_capacity=200_000,
        volume=VolumeConfig(history_per_contract=512, initial_baseline=BaselineKind.SESSION_COMPLETE)))
    messages = generate_messages(contracts=contracts, count=60_000, base_ms=T0, rate_per_second=1_000, quoted=quoted,
                                 regular_condition=REGULAR, big_fraction=0.03)
    started = time.perf_counter()
    for offset, message in enumerate(messages, start=1):
        engine.submit(message, offset)
    engine.process()
    drive = time.perf_counter() - started
    started = time.perf_counter()
    rows, raw = encode_checkpoint(engine, consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION,
                                  offset=len(messages), sequence=1, extras=EXTRAS)
    encode = time.perf_counter() - started
    compressed = sum(len(row.state_payload) for row in rows)
    started = time.perf_counter()
    restored = OptionFlowEngine(engine.config)
    restore_checkpoint(restored, rows, consumer_name=CONSUMER, cohort_id=COHORT, generation=JOURNAL_GENERATION)
    restore = time.perf_counter() - started
    assert restored.state_digest() == engine.state_digest()
    assert max(len(row.state_payload) for row in rows) < 1_048_576  # the rt.consumer_checkpoints cap per row
    held = engine._shards[0].book.contract_count() + sum(shard.book.contract_count() for shard in engine._shards[1:])
    with capsys.disabled():
        print(f"\nWINDOWS_CONSTRAINED checkpoint, {held} contracts, {len(messages):,} messages fed in {drive:.2f}s: "
              f"{raw / 1e6:.2f} MB json -> {compressed / 1e6:.2f} MB zlib in {len(rows)} rows "
              f"(max row {max(len(row.state_payload) for row in rows) / 1e3:.0f} KB), encode {encode * 1000:.0f} ms, "
              f"restore+digest {restore * 1000:.0f} ms")
