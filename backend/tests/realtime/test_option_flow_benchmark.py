"""Synthetic throughput benchmark of the pure engine (no I/O).

The numbers are labelled WINDOWS_CONSTRAINED: one Python process, one thread, on a Windows
laptop. No timing is asserted; the checks are correctness, accounting and determinism: every
message is accounted for and every contract's cumulative volume equals an independent recount.
"""
from __future__ import annotations

import random
import time
from collections import defaultdict
from decimal import Decimal

import pytest

from option_flow_support import CANCEL, FLOOR, MULTI_LEG, NON_VOLUME, REGULAR, SWEEP, T0, UNKNOWN_CODE, check_event_rules, config
from realtime.option_flow.engine import OptionFlowEngine, SubmitResult
from realtime.option_flow.records import EngineOutput
from realtime.option_flow.volume import BaselineKind, VolumeConfig

THRESHOLD = Decimal("50000")
UNDERLYERS = 50
CONTRACTS_PER_UNDERLYER = 100
QUOTED_PER_UNDERLYER = 20


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


class Stream:
    def __init__(self, count: int, rate: int, seed: int = 7) -> None:
        rng = random.Random(seed)
        contracts = _contracts()
        quoted = [symbol for root in _roots() for _, symbol in
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
                message = dict(original, c=[CANCEL], t=time_ms, q=sequences[original["sym"]])
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
            conditions = [REGULAR]
            if kind < 0.02:
                conditions = [SWEEP]
            elif kind < 0.03:
                conditions = [FLOOR]
            elif kind < 0.035:
                conditions = [MULTI_LEG]
            elif kind < 0.038:
                conditions = [NON_VOLUME]
            elif kind < 0.0385:
                conditions = [UNKNOWN_CODE]
            message = {"ev": "T", "sym": symbol, "x": 300 + rng.randrange(6), "p": price, "s": size,
                       "c": conditions, "t": time_ms, "q": sequences[symbol]}
            self.messages.append(message)
            persisted = Decimal(repr(price)) * size * 100 >= THRESHOLD
            if conditions[0] in (REGULAR, SWEEP, FLOOR, MULTI_LEG):
                self.expected_volume[symbol] += size
                if persisted:
                    self.expected_persisted += 1
                    if conditions[0] != MULTI_LEG:
                        recent_big.append(message)
            elif conditions[0] == UNKNOWN_CODE and persisted:
                self.expected_persisted += 1


def _engine(stream: Stream) -> OptionFlowEngine:
    return OptionFlowEngine(config(
        shard_count=4, queue_capacity=200_000, quote_universe=stream.quoted,
        volume=VolumeConfig(initial_baseline=BaselineKind.SESSION_COMPLETE)))


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


def test_benchmark_run_is_deterministic():
    stream = Stream(20_000, 10_000)
    first_engine, second_engine = _engine(stream), _engine(stream)
    first, second = _drive(first_engine, stream.messages), _drive(second_engine, stream.messages)
    assert first.events == second.events and first.clusters == second.clusters
    assert first.checkpoints == second.checkpoints
    assert first_engine.state_digest() == second_engine.state_digest()
