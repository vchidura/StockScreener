#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.bridge import drain_raw_buffer_once
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import MarketFeed, RawMessageEnvelope
from realtime.journal import RawJournalWriter


PLANNING_CAPS_GB = (50, 100, 250)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("percentile requires observations")
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _synthetic_payload(index: int, target_bytes: int) -> bytes:
    event = {
        "ap": 100.03,
        "as": 12,
        "bp": 100.01,
        "bs": 10,
        "ev": "Q",
        "pad": "",
        "q": index + 1,
        "sym": "SPY",
        "t": 1791210600000 + index,
    }
    base = json.dumps([event], sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )
    if target_bytes < len(base):
        raise ValueError(f"payload_bytes must be at least {len(base)}")
    event["pad"] = "x" * (target_bytes - len(base))
    payload = json.dumps([event], sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )
    if len(payload) != target_bytes:
        raise RuntimeError("synthetic payload sizing is not deterministic")
    return payload


def _journal_files_bytes(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (
            path,
            Path(str(path) + "-wal"),
            Path(str(path) + "-shm"),
        )
        if candidate.exists()
    )


def run_benchmark(
    path: Path,
    *,
    message_count: int,
    payload_bytes: int,
    batch_size: int,
) -> dict[str, object]:
    if message_count <= 0 or batch_size <= 0 or batch_size > message_count:
        raise ValueError("message_count and batch_size must define nonempty batches")
    session_id = uuid4()
    batch_byte_budget = payload_bytes * batch_size
    buffer = RawMessageBuffer(
        BufferLimits(
            maximum_events=batch_size,
            maximum_bytes=batch_byte_budget,
            maximum_age_ns=5_000_000_000,
        ),
        monotonic_ns=time.perf_counter_ns,
    )
    latencies_ms: list[float] = []
    total_payload_bytes = 0
    started = time.perf_counter_ns()
    with RawJournalWriter(path) as writer:
        for batch_start in range(0, message_count, batch_size):
            batch_end = min(batch_start + batch_size, message_count)
            before = time.perf_counter_ns()
            for index in range(batch_start, batch_end):
                payload = _synthetic_payload(index, payload_bytes)
                received_monotonic_ns = time.perf_counter_ns()
                buffer.put_nowait(
                    RawMessageEnvelope(
                        stream_session_id=session_id,
                        message_number=index,
                        feed=MarketFeed.STOCKS,
                        received_at=datetime.now(timezone.utc),
                        received_monotonic_ns=received_monotonic_ns,
                        payload=payload,
                    )
                )
                total_payload_bytes += len(payload)
            commit = drain_raw_buffer_once(
                buffer,
                writer,
                maximum_events=batch_size,
                maximum_bytes=batch_byte_budget,
            )
            if commit is None or commit.inserted_count != batch_end - batch_start:
                raise RuntimeError("benchmark batch did not commit completely")
            latencies_ms.append((time.perf_counter_ns() - before) / 1_000_000)
        health = writer.health()
        physical_bytes = _journal_files_bytes(path)
    elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000
    physical_bytes = max(physical_bytes, _journal_files_bytes(path))
    messages_per_second = message_count / elapsed_seconds
    payload_megabytes_per_second = total_payload_bytes / elapsed_seconds / 1_000_000
    physical_bytes_per_message = physical_bytes / message_count
    projections = {}
    for cap_gb in PLANNING_CAPS_GB:
        cap_bytes = cap_gb * 1024**3
        projected_messages = int(cap_bytes / physical_bytes_per_message)
        projections[str(cap_gb)] = {
            "cap_bytes": cap_bytes,
            "projected_messages_at_measured_density": projected_messages,
            "projected_hours_at_synthetic_benchmark_rate": (
                projected_messages / messages_per_second / 3600
            ),
        }
    return {
        "status": "MEASURED_SYNTHETIC_LOCAL_ONLY",
        "message_count": message_count,
        "batch_size": batch_size,
        "batch_count": len(latencies_ms),
        "payload_bytes_per_message": payload_bytes,
        "total_payload_bytes": total_payload_bytes,
        "physical_journal_bytes": physical_bytes,
        "physical_bytes_per_message": physical_bytes_per_message,
        "elapsed_seconds": elapsed_seconds,
        "messages_per_second": messages_per_second,
        "payload_megabytes_per_second": payload_megabytes_per_second,
        "durable_batch_latency_ms": {
            "p50": _percentile(latencies_ms, 0.50),
            "p95": _percentile(latencies_ms, 0.95),
            "p99": _percentile(latencies_ms, 0.99),
            "maximum": max(latencies_ms),
        },
        "journal": {
            "mode": health.journal_mode,
            "synchronous": health.synchronous,
            "last_offset": health.last_offset,
        },
        "planning_cap_projections_gb": projections,
        "limitations": (
            "Synthetic local write capacity only; not provider completeness, "
            "production event rate, end-to-end latency, or retained-session coverage."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark the local FULL SQLite raw journal with synthetic data."
    )
    parser.add_argument("--messages", type=int, default=5000)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=25)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with tempfile.TemporaryDirectory(prefix="realtime-journal-benchmark-") as directory:
        report = run_benchmark(
            Path(directory) / "raw.sqlite",
            message_count=args.messages,
            payload_bytes=args.payload_bytes,
            batch_size=args.batch_size,
        )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())