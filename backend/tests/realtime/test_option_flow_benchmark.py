"""Synthetic throughput benchmark of the pure engine (no I/O).

The numbers are labelled WINDOWS_CONSTRAINED: one Python process, one thread, on a Windows
laptop. No timing is asserted; the checks are correctness, accounting and determinism: every
message is accounted for and every contract's cumulative volume equals an independent recount.
"""
from __future__ import annotations

import random
import time
from collections import defaultdict
from dataclasses import replace
from decimal import Decimal
from typing import Mapping
from uuid import UUID

import pytest

from option_flow_support import (
    CANCEL,
    FLOOR,
    MULTI_LEG,
    NON_VOLUME,
    REGULAR,
    SWEEP,
    T0,
    UNKNOWN_CODE,
    advanced_contract_scope,
    advanced_policy,
    check_event_rules,
    config,
)
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from realtime.option_flow.consumer import ConsumerLimits, OptionFlowConsumer
from realtime.option_flow.conditions import load_default_live_semantics
from realtime.option_flow.checkpoint_state import CheckpointStore
from realtime.option_flow.engine import (
    DEFAULT_SHARD_COUNT,
    OptionFlowEngine,
    SubmitResult,
)
from realtime.option_flow.mock_feed import MockOptionFeed
from realtime.option_flow.records import EngineOutput
from realtime.option_flow.volume import BaselineKind, VolumeConfig
from realtime.option_flow.writer import OptionFlowWriter
from realtime.option_source_flow import build_option_source_flow_identity

THRESHOLD = Decimal("50000")
UNDERLYERS = 50
CONTRACTS_PER_UNDERLYER = 100
QUOTED_PER_UNDERLYER = 20
ADVANCED_ROOTS = (
    "IWM",
    "QQQ",
    "SPY",
    "AAPL",
    "AMZN",
    "MSFT",
    "NFLX",
    "NVDA",
    "TSLA",
)
P6D2_STREAM_SESSION = UUID("9a623a0e-9b5d-4d91-8f9e-5dd4a666ec1d")


def _roots():
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return [f"{letters[index // 26 % 26]}{letters[index % 26]}X" for index in range(UNDERLYERS)]


def _contracts():
    contracts = []
    for root in _roots():
        for index in range(CONTRACTS_PER_UNDERLYER):
            kind = "C" if index % 2 else "P"
            contracts.append((root, f"O:{root}261218{kind}{10_000 + index * 500:08d}"))
    return contracts


def _advanced_contracts():
    return [
        (
            root,
            f"O:{root}261120{'C' if index % 2 else 'P'}"
            f"{10_000 + index * 500:08d}",
        )
        for root in ADVANCED_ROOTS
        for index in range(CONTRACTS_PER_UNDERLYER)
    ]


class Stream:
    def __init__(
        self,
        count: int,
        rate: int,
        seed: int = 7,
        *,
        contracts: list[tuple[str, str]] | None = None,
        condition_ids: Mapping[str, int] | None = None,
    ) -> None:
        rng = random.Random(seed)
        contracts = _contracts() if contracts is None else contracts
        roots = tuple(dict.fromkeys(root for root, _ in contracts))
        condition_ids = condition_ids or {
            "regular": REGULAR,
            "sweep": SWEEP,
            "floor": FLOOR,
            "multi_leg": MULTI_LEG,
            "non_volume": NON_VOLUME,
            "cancel": CANCEL,
            "unknown": UNKNOWN_CODE,
        }
        quoted = [symbol for root in roots for _, symbol in
                  [item for item in contracts if item[0] == root][:QUOTED_PER_UNDERLYER]]
        self.quoted = frozenset(quoted)
        sequences: dict[str, int] = defaultdict(int)
        self.messages: list[dict] = []
        self.expected_volume: dict[str, int] = defaultdict(int)
        self.expected_persisted = 0
        self.duplicates = 0
        self.cancels = 0
        recent_big: list[dict] = []
        for index in range(count):
            time_ms = T0 + index * 1000 // rate
            if rng.random() < 0.3:
                symbol = rng.choice(quoted)
                bid = round(rng.uniform(0.5, 20.0), 2)
                sequences[("Q", symbol)] += 1
                self.messages.append({"ev": "Q", "sym": symbol, "bx": 301, "ax": 302, "bp": bid,
                                      "ap": round(bid + 0.05, 2), "bs": 10, "as": 12, "t": time_ms,
                                      "q": sequences[("Q", symbol)]})
                continue
            roll = rng.random()
            if roll < 0.001 and recent_big:
                original = recent_big.pop(rng.randrange(len(recent_big)))
                sequences[original["sym"]] += 1
                message = dict(
                    original,
                    c=[condition_ids["cancel"]],
                    t=time_ms,
                    q=sequences[original["sym"]],
                )
                self.messages.append(message)
                self.expected_volume[original["sym"]] -= original["s"]
                self.expected_persisted += 1
                self.cancels += 1
                continue
            if roll < 0.0015 and self.messages:
                previous = self.messages[-1]
                if previous["ev"] == "T" and previous["c"] != [CANCEL]:
                    self.messages.append(dict(previous))
                    self.duplicates += 1
                    continue
            symbol = rng.choice(quoted) if rng.random() < 0.8 else rng.choice(contracts)[1]
            sequences[symbol] += 1
            big = rng.random() < 0.01
            price = round(rng.uniform(1.0, 20.0), 2) if big else round(rng.uniform(0.05, 4.0), 2)
            size = rng.randint(500, 2000) if big else rng.randint(1, 50)
            kind = rng.random()
            conditions = [condition_ids["regular"]]
            if kind < 0.02:
                conditions = [condition_ids["sweep"]]
            elif kind < 0.03:
                conditions = [condition_ids["floor"]]
            elif kind < 0.035:
                conditions = [condition_ids["multi_leg"]]
            elif kind < 0.038:
                conditions = [condition_ids["non_volume"]]
            elif kind < 0.0385:
                conditions = [condition_ids["unknown"]]
            message = {"ev": "T", "sym": symbol, "x": 300 + rng.randrange(6), "p": price, "s": size,
                       "c": conditions, "t": time_ms, "q": sequences[symbol]}
            self.messages.append(message)
            persisted = Decimal(repr(price)) * size * 100 >= THRESHOLD
            if conditions[0] in (
                condition_ids["regular"],
                condition_ids["sweep"],
                condition_ids["floor"],
                condition_ids["multi_leg"],
            ):
                self.expected_volume[symbol] += size
                if persisted:
                    self.expected_persisted += 1
                    if conditions[0] != condition_ids["multi_leg"]:
                        recent_big.append(message)
            elif conditions[0] == condition_ids["unknown"] and persisted:
                self.expected_persisted += 1


def _engine(stream: Stream) -> OptionFlowEngine:
    return OptionFlowEngine(config(
        shard_count=4, queue_capacity=200_000, quote_universe=stream.quoted,
        volume=VolumeConfig(initial_baseline=BaselineKind.SESSION_COMPLETE)))


def _p6d2_condition_ids(semantics) -> dict[str, int]:
    rows = tuple(semantics.reviewed.values())

    def find(predicate) -> int:
        return next(row.condition_id for row in rows if predicate(row))

    return {
        "regular": find(
            lambda row: row.treatment.value == "INCLUDED"
            and row.volume_eligible
            and not any((
                row.is_cancel,
                row.is_late,
                row.is_multi_leg,
                row.is_cross,
                row.is_sweep,
                row.is_floor,
            ))
        ),
        "sweep": find(lambda row: row.is_sweep and not row.is_multi_leg),
        "floor": find(lambda row: row.is_floor),
        "multi_leg": find(lambda row: row.is_multi_leg),
        "non_volume": find(lambda row: row.treatment.value == "EXCLUDED"),
        "cancel": find(lambda row: row.is_cancel),
        "unknown": UNKNOWN_CODE,
    }


def _p6d2_engine(
    stream: Stream,
    semantics,
    *,
    journal_generation: UUID | None = None,
    shard_count: int = DEFAULT_SHARD_COUNT,
) -> OptionFlowEngine:
    flow_policy = advanced_policy()
    scope = advanced_contract_scope(
        tuple(symbol for _, symbol in _advanced_contracts()),
        flow_policy=flow_policy,
    )
    exchange_reference = load_default_options_exchange_reference()
    base = config(
        semantics=semantics,
        allow_synthetic_semantics=False,
        shard_count=shard_count,
        queue_capacity=200_000,
        quote_universe=stream.quoted,
        volume=VolumeConfig(
            initial_baseline=BaselineKind.SESSION_COMPLETE,
        ),
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
        exchange_reference=exchange_reference,
        **(
            {}
            if journal_generation is None
            else {"journal_generation": journal_generation}
        ),
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=base.journal_generation,
        stream_session_ids=(P6D2_STREAM_SESSION,),
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=flow_policy.sha256,
        contract_scope_sha256=scope.sha256,
        provider_contract_sha256=MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        condition_reference_sha256=semantics.pin.payload_sha256,
        exchange_reference_sha256=exchange_reference.sha256,
    )
    return OptionFlowEngine(replace(base, source_flow=source_flow))


class _CountingCursor:
    def __init__(self) -> None:
        self.rowcount = 0
        self._copied = 0
        self.copied = {"events": 0, "clusters": 0, "volume": 0}

    def copy_expert(self, statement, file) -> None:
        text = file.read()
        self._copied = text.count("\n")
        for name, marker in (
            ("events", "option_flow_events"),
            ("clusters", "option_flow_clusters"),
            ("volume", "option_volume_checkpoints"),
        ):
            if marker in statement:
                self.copied[name] += self._copied

    def execute(self, statement, parameters=None) -> None:
        text = " ".join(statement.split())
        if text.startswith("INSERT INTO rt.consumer_checkpoints"):
            self.rowcount = 1
        elif text.startswith("INSERT INTO"):
            self.rowcount = self._copied
        elif text.startswith("UPDATE rt.consumer_checkpoints"):
            self.rowcount = 4

    def fetchall(self):
        return []

    def close(self) -> None:
        pass


class _CountingConnection:
    autocommit = False

    def __init__(self) -> None:
        self.cursor_ = _CountingCursor()

    def cursor(self):
        return self.cursor_

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


def _drive(eng: OptionFlowEngine, messages, chunk: int = 2000) -> EngineOutput:
    out = EngineOutput()
    submit, process = eng.submit, eng.process
    for offset, message in enumerate(messages, start=1):
        if submit(message, offset) is SubmitResult.GAP_RECORDED:
            out.extend(process())
            submit(message, offset)
        if offset % chunk == 0:
            out.extend(process())
    out.extend(process())
    out.extend(eng.flush(final=True))
    return out


@pytest.mark.parametrize("rate, seconds", [(10_000, 3), (50_000, 3)])
def test_engine_throughput_and_accounting_at_target_rates(rate, seconds, capsys):
    stream = Stream(rate * seconds, rate)
    eng = _engine(stream)
    started = time.perf_counter()
    out = _drive(eng, stream.messages)
    elapsed = time.perf_counter() - started

    total = len(stream.messages)
    achieved = total / elapsed
    with capsys.disabled():
        print(f"\nWINDOWS_CONSTRAINED option flow engine: target {rate:,} msg/s for {seconds}s "
              f"({total:,} msgs) -> {elapsed:.2f}s, {achieved:,.0f} msg/s, "
              f"{achieved / rate:.2f}x real time; persisted events {len(out.events):,}, "
              f"clusters {len(out.clusters):,}, checkpoints {len(out.checkpoints):,}")

    stats = eng.stats
    assert stats.submitted == total
    assert stats.queue_dropped == 0 and stats.rejected == 0 and eng.queued() == 0
    assert stats.submitted == stats.accounted() == stats.processed
    assert stats.trades + stats.quotes == total
    assert stats.duplicates == stream.duplicates
    assert stats.cancels == stream.cancels and stats.cancels_unmatched == 0
    assert len(out.events) == stats.persisted_events + stats.cancel_revisions == stream.expected_persisted
    final_volume = {}
    for row in out.checkpoints:
        final_volume[row.contract_ticker] = row.cumulative_volume
    assert {key: value for key, value in final_volume.items() if value} == {
        key: value for key, value in stream.expected_volume.items() if value}
    for event in out.events[:2000]:
        check_event_rules(event)
    assert any(event.side_class == "AT_OR_ABOVE_ASK" for event in out.events)
    assert any(event.side_class == "AGGRESSION_UNAVAILABLE_NO_QUOTE" for event in out.events)


@pytest.mark.parametrize("trial", (1, 2, 3))
@pytest.mark.parametrize("rate, seconds", ((1_000, 30), (5_000, 3)))
def test_p6d2_v4_engine_capacity_challenger(
    trial,
    rate,
    seconds,
    capsys,
):
    semantics = load_default_live_semantics()
    stream = Stream(
        rate * seconds,
        rate,
        contracts=_advanced_contracts(),
        condition_ids=_p6d2_condition_ids(semantics),
    )
    eng = _p6d2_engine(stream, semantics)
    started = time.perf_counter()
    out = _drive(eng, stream.messages)
    elapsed = time.perf_counter() - started

    total = len(stream.messages)
    achieved = total / elapsed
    with capsys.disabled():
        print(
            "\nWINDOWS_CONSTRAINED P6D2 source-flow v4 engine: "
            f"trial {trial}, target {rate:,} msg/s for {seconds}s "
            f"({total:,} msgs) -> {elapsed:.2f}s, "
            f"{achieved:,.0f} msg/s, {achieved / rate:.2f}x real time; "
            f"persisted events {len(out.events):,}, "
            f"clusters {len(out.clusters):,}, "
            f"checkpoints {len(out.checkpoints):,}"
        )

    stats = eng.stats
    assert stats.submitted == stats.accounted() == stats.processed == total
    assert stats.queue_dropped == 0 and stats.rejected == 0
    assert eng.queued() == 0
    assert out.events
    assert {
        (
            event.lineage_version,
            event.provider_contract_sha256,
            event.condition_reference_sha256,
            event.exchange_reference_sha256,
        )
        for event in out.events
    } == {(
        4,
        eng.config.source_flow.provider_contract_sha256,
        eng.config.source_flow.condition_reference_sha256,
        eng.config.source_flow.exchange_reference_sha256,
    )}
    assert all(row.lineage_version == 4 for row in out.clusters)
    final_volume = {
        row.contract_ticker: row.cumulative_volume
        for row in out.checkpoints
    }
    assert {
        key: value for key, value in final_volume.items() if value
    } == {
        key: value
        for key, value in stream.expected_volume.items()
        if value
    }


@pytest.mark.parametrize("trial", (1, 2, 3))
@pytest.mark.parametrize("rate, seconds", ((1_000, 30), (5_000, 3)))
def test_p6d2_v4_consumer_writer_capacity_challenger(
    trial,
    rate,
    seconds,
    capsys,
):
    semantics = load_default_live_semantics()
    stream = Stream(
        rate * seconds,
        rate,
        contracts=_advanced_contracts(),
        condition_ids=_p6d2_condition_ids(semantics),
    )
    feed = MockOptionFeed(seed=f"p6d2-capacity-{rate}-{trial}")
    feed.extend(stream.messages, frame_size=10)
    eng = _p6d2_engine(
        stream,
        semantics,
        journal_generation=feed.generation,
    )
    connection = _CountingConnection()
    consumer = OptionFlowConsumer(
        eng,
        feed,
        lambda hook: OptionFlowWriter(
            connection,
            in_transaction=hook,
        ),
        generation=feed.generation,
        limits=ConsumerLimits(
            batch_size=500,
            max_messages=10_000,
            max_seconds=1e9,
        ),
        checkpoint_interval_seconds=0.0,
    )
    started = time.perf_counter()
    while consumer.run_slice().stopped != "CAUGHT_UP":
        pass
    consumer.finish()
    elapsed = time.perf_counter() - started

    total = len(stream.messages)
    achieved = total / elapsed
    with capsys.disabled():
        print(
            "\nWINDOWS_CONSTRAINED P6D2 consumer+v4 engine+writer "
            f"(no database/checkpoint state): trial {trial}, "
            f"target {rate:,} msg/s for {seconds}s "
            f"({total:,} msgs) -> {elapsed:.2f}s, "
            f"{achieved:,.0f} msg/s, {achieved / rate:.2f}x real time; "
            f"persisted events {eng.stats.persisted_events:,}, "
            f"cluster rows {connection.cursor_.copied['clusters']:,}, "
            f"volume rows {connection.cursor_.copied['volume']:,}"
        )

    assert eng.stats.submitted == eng.stats.accounted() == total
    assert eng.stats.queue_dropped == 0 and eng.stats.rejected == 0
    assert eng.queued() == 0
    assert connection.cursor_.copied["events"] == (
        eng.stats.persisted_events + eng.stats.cancel_revisions
    )
    assert consumer.fed_offset == feed.last_offset()


@pytest.mark.parametrize("trial", (1, 2, 3))
def test_p6d2_v4_consumer_checkpoint_capacity_challenger(
    trial,
    capsys,
):
    rate, seconds = 1_000, 30
    semantics = load_default_live_semantics()
    stream = Stream(
        rate * seconds,
        rate,
        contracts=_advanced_contracts(),
        condition_ids=_p6d2_condition_ids(semantics),
    )
    feed = MockOptionFeed(seed=f"p6d2-checkpoint-{trial}")
    feed.extend(stream.messages, frame_size=10)
    eng = _p6d2_engine(
        stream,
        semantics,
        journal_generation=feed.generation,
    )
    connection = _CountingConnection()
    store = CheckpointStore(
        consumer_name="p6d2-capacity",
        cohort_id=eng.config.cohort_id,
        generation=feed.generation,
    )
    consumer = OptionFlowConsumer(
        eng,
        feed,
        lambda hook: OptionFlowWriter(
            connection,
            in_transaction=hook,
        ),
        generation=feed.generation,
        checkpoints=store,
        limits=ConsumerLimits(
            batch_size=500,
            max_messages=10_000,
            max_seconds=1e9,
        ),
        checkpoint_interval_seconds=10.0,
    )
    started = time.perf_counter()
    while consumer.run_slice().stopped != "CAUGHT_UP":
        pass
    consumer.finish()
    elapsed = time.perf_counter() - started

    total = len(stream.messages)
    achieved = total / elapsed
    with capsys.disabled():
        print(
            "\nWINDOWS_CONSTRAINED P6D2 consumer+v4 engine+writer "
            f"with checkpoint state: trial {trial}, "
            f"target {rate:,} msg/s for {seconds}s "
            f"({total:,} msgs) -> {elapsed:.2f}s, "
            f"{achieved:,.0f} msg/s, {achieved / rate:.2f}x real time; "
            f"{store.sequence} full checkpoints, "
            f"last {store.last_write.payload_bytes / 1e6:.2f} MB zlib, "
            f"largest row {store.last_write.largest_row_bytes / 1e3:.0f} KB"
        )

    assert eng.stats.submitted == eng.stats.accounted() == total
    assert eng.stats.queue_dropped == 0 and eng.stats.rejected == 0
    assert consumer.fed_offset == feed.last_offset()
    assert store.sequence >= 2
    assert store.last_write.largest_row_bytes <= store.max_state_bytes


def test_benchmark_run_is_deterministic():
    stream = Stream(20_000, 10_000)
    first_engine, second_engine = _engine(stream), _engine(stream)
    first, second = _drive(first_engine, stream.messages), _drive(second_engine, stream.messages)
    assert first.events == second.events and first.clusters == second.clusters
    assert first.checkpoints == second.checkpoints
    assert first_engine.state_digest() == second_engine.state_digest()
