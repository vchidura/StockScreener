#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import AssetClass
from realtime.materialization import (
    MAX_CHECKPOINT_STATE_BYTES,
    MaterializedQuote,
    MaterializedTrade,
    MaterializerCheckpoint,
    MaterializerShardMap,
    MaterializerShardState,
    build_materializer_checkpoint,
    encode_materializer_state,
    restore_materializer_checkpoint,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
GENERATION = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SESSION = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
COHORT = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure synthetic realtime checkpoint payload sizes without I/O."
    )
    parser.add_argument("--contracts-per-shard", type=int, default=125)
    parser.add_argument(
        "--history-depths",
        default="1,4,8,16,32",
        help="Comma-separated quote-history depths per contract.",
    )
    parser.add_argument("--iterations", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.contracts_per_shard <= 1_000:
        parser.error("contracts-per-shard must be in [1, 1000]")
    try:
        args.history_depths = tuple(
            int(value) for value in args.history_depths.split(",")
        )
    except ValueError:
        parser.error("history-depths must contain integers")
    if (
        not args.history_depths
        or any(not 1 <= value <= 256 for value in args.history_depths)
        or len(set(args.history_depths)) != len(args.history_depths)
    ):
        parser.error("history-depths must be unique values in [1, 256]")
    if not 1 <= args.iterations <= 200:
        parser.error("iterations must be in [1, 200]")
    return args


def _quote(instrument_id: int, source_offset: int, age: int) -> MaterializedQuote:
    event_at = NOW - timedelta(milliseconds=age)
    ticker = f"O:SPY261218C{instrument_id:08d}"
    return MaterializedQuote(
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=instrument_id,
        provider_symbol=ticker,
        source_session=date(2026, 10, 5),
        source_sequence=source_offset,
        source_timestamp=1_000_000 + source_offset,
        source_timestamp_unit="ms",
        event_at=event_at,
        received_at=event_at + timedelta(milliseconds=1),
        bid=Decimal("10.10"),
        ask=Decimal("10.20"),
        bid_size=10,
        ask_size=11,
        bid_exchange=1,
        ask_exchange=2,
        quality="GOOD",
        reason_codes=(),
        source_offset=source_offset,
        payload_sha256=f"{source_offset:064x}",
    )


def _trade(instrument_id: int, source_offset: int) -> MaterializedTrade:
    event_at = NOW - timedelta(milliseconds=1)
    ticker = f"O:SPY261218C{instrument_id:08d}"
    return MaterializedTrade(
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=instrument_id,
        provider_symbol=ticker,
        asset_class=AssetClass.OPTION,
        source_session=date(2026, 10, 5),
        source_sequence=source_offset,
        source_timestamp=2_000_000 + source_offset,
        source_timestamp_unit="ms",
        event_at=event_at,
        received_at=event_at + timedelta(milliseconds=1),
        price=Decimal("10.15"),
        size=Decimal("100"),
        exchange=4,
        conditions=(208,),
        quality="UNCLASSIFIED",
        reason_codes=("CONDITION_POLICY_PENDING",),
        eligibility="INCLUDED",
        eligibility_reason_codes=(
            "PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE",
            "AGGRESSOR_SIDE_UNAVAILABLE",
        ),
        semantics_version="massive_options_conditions_v1",
        source_offset=source_offset,
        payload_sha256=f"{source_offset:064x}",
    )


def checkpoint_state(
    contracts: int,
    history_depth: int,
    *,
    shard_map_sha256: str = "c" * 64,
) -> MaterializerShardState:
    quotes = []
    trades = []
    history = []
    source_offset = 0
    for instrument_id in range(1, contracts + 1):
        instrument_history = []
        for age in range(history_depth, 0, -1):
            source_offset += 1
            instrument_history.append(_quote(instrument_id, source_offset, age))
        history.extend(instrument_history)
        quotes.append(instrument_history[-1])
        source_offset += 1
        trades.append(_trade(instrument_id, source_offset))
    return MaterializerShardState(
        shard_id=0,
        shard_map_sha256=shard_map_sha256,
        scanned_offset=source_offset,
        quotes=tuple(quotes),
        trades=tuple(trades),
        quote_history=tuple(history),
    )


def checkpoint_bytes(contracts: int, history_depth: int) -> int:
    state = checkpoint_state(contracts, history_depth)
    return len(encode_materializer_state(state, maximum_bytes=128 * 1024 * 1024))


def checkpoint_fixture(
    contracts: int,
    history_depth: int,
) -> tuple[MaterializerShardMap, MaterializerShardState, MaterializerCheckpoint]:
    shard_map = MaterializerShardMap(
        "checkpoint-benchmark-v1",
        1,
        tuple((instrument_id, 0) for instrument_id in range(1, contracts + 1)),
    )
    state = checkpoint_state(
        contracts,
        history_depth,
        shard_map_sha256=shard_map.sha256,
    )
    checkpoint = build_materializer_checkpoint(
        state,
        consumer_name="checkpoint-benchmark",
        cohort_id=COHORT,
        journal_generation=GENERATION,
        owner_epoch=1,
        committed_at=NOW,
        maximum_bytes=128 * 1024 * 1024,
    )
    return shard_map, state, checkpoint


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile) - 1))
    return ordered[index]


def main() -> int:
    args = parse_args()
    rows = []
    for depth in args.history_depths:
        shard_map, state, checkpoint = checkpoint_fixture(
            args.contracts_per_shard,
            depth,
        )
        serialization_durations = []
        restore_durations = []
        for _ in range(args.iterations):
            started = time.perf_counter_ns()
            encode_materializer_state(state, maximum_bytes=128 * 1024 * 1024)
            serialization_durations.append(
                (time.perf_counter_ns() - started) / 1_000_000
            )
            started = time.perf_counter_ns()
            restored = restore_materializer_checkpoint(
                checkpoint,
                shard_map,
                maximum_bytes=128 * 1024 * 1024,
            )
            restore_durations.append((time.perf_counter_ns() - started) / 1_000_000)
            if restored != state:
                raise RuntimeError("checkpoint benchmark restore did not match its state")
        payload_bytes = len(checkpoint.state_payload)
        rows.append({
            "contracts_per_shard": args.contracts_per_shard,
            "quote_history_per_contract": depth,
            "payload_bytes": payload_bytes,
            "bytes_per_contract": round(payload_bytes / args.contracts_per_shard, 2),
            "fits_current_1_mib_cap": payload_bytes <= MAX_CHECKPOINT_STATE_BYTES,
            "fits_64_mib": payload_bytes <= 64 * 1024 * 1024,
            "serialization_iterations": args.iterations,
            "serialization_p50_ms": round(_percentile(serialization_durations, 0.50), 3),
            "serialization_p95_ms": round(_percentile(serialization_durations, 0.95), 3),
            "serialization_p99_ms": round(_percentile(serialization_durations, 0.99), 3),
            "serialization_max_ms": round(max(serialization_durations), 3),
            "restore_iterations": args.iterations,
            "restore_p50_ms": round(_percentile(restore_durations, 0.50), 3),
            "restore_p95_ms": round(_percentile(restore_durations, 0.95), 3),
            "restore_p99_ms": round(_percentile(restore_durations, 0.99), 3),
            "restore_max_ms": round(max(restore_durations), 3),
        })
    print(json.dumps({
        "status": "SYNTHETIC_ONLY",
        "codec_limit_bytes": MAX_CHECKPOINT_STATE_BYTES,
        "developer_batch_limit_bytes": 64 * 1024 * 1024,
        "rows": rows,
        "provider_requests": False,
        "database_access": False,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())