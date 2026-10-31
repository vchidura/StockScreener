#!/usr/bin/env python3
"""Qualify bounded host capacity for a 1,000-contract Options stream."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_ingress import (  # noqa: E402
    AdvancedOptionsJournalPump,
)
from realtime.advanced_options_p7_selection import (  # noqa: E402
    P7TransportCandidate,
)
from realtime.advanced_options_v3_broad_stream import (  # noqa: E402
    build_broad_options_stream_plan,
)
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import (  # noqa: E402
    AssetClass,
    MarketFeed,
    RawMessageEnvelope,
)
from realtime.normalizer import InstrumentAdmission  # noqa: E402


UTC = timezone.utc
SESSION_DATE = date(2026, 11, 2)
REVIEWED_AT = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)
CONTRACT_COUNT = 1_000
SUBSCRIPTION_COUNT = CONTRACT_COUNT * 2
MESSAGE_COUNT = 20_000
BATCH_SIZE = 500
BUFFER_MAXIMUM_BYTES = 32 * 1024 * 1024
JOURNAL_ACCEPT_P95_MILLISECONDS = 25
JOURNAL_ACCEPT_P99_MILLISECONDS = 100
MAXIMUM_ELAPSED_SECONDS = 5
PUMP_JOIN_TIMEOUT_SECONDS = 60


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("percentile requires observations")
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _candidates() -> tuple[P7TransportCandidate, ...]:
    rows = []
    identity = 1
    for underlyer in ("QQQ", "SPY"):
        underlyer_rows = []
        for dte in range(61):
            expiration = SESSION_DATE + timedelta(days=dte)
            for option_type in ("C", "P"):
                for strike in (
                    Decimal("98"),
                    Decimal("99"),
                    Decimal("100"),
                    Decimal("101"),
                    Decimal("102"),
                ):
                    underlyer_rows.append(P7TransportCandidate(
                        contract_ticker=(
                            f"O:{underlyer}{expiration.strftime('%y%m%d')}"
                            f"{option_type}{int(strike * 1000):08d}"
                        ),
                        underlyer=underlyer,
                        expiration_date=expiration,
                        option_type=option_type,
                        strike=strike,
                        spot=Decimal("100"),
                        bid=Decimal("1.00"),
                        ask=Decimal("1.10"),
                        day_volume=0,
                        open_interest=0,
                        calendar_dte=dte,
                        market_data_time=REVIEWED_AT - timedelta(seconds=1),
                        source_sha256=f"{identity:064x}",
                    ))
                    identity += 1
        rows.extend(underlyer_rows[: CONTRACT_COUNT // 2])
    if len(rows) != CONTRACT_COUNT:
        raise RuntimeError("capacity fixture contract count changed")
    return tuple(rows)


def _payload(ticker: str, sequence: int) -> bytes:
    timestamp = int(REVIEWED_AT.timestamp() * 1000) + sequence
    return json.dumps([
        {
            "ev": "T",
            "sym": ticker,
            "p": 1.05,
            "s": 10,
            "x": 302,
            "t": timestamp,
            "q": sequence * 2 + 1,
        },
        {
            "ev": "Q",
            "sym": ticker,
            "bp": 1.00,
            "ap": 1.10,
            "bs": 10,
            "as": 12,
            "t": timestamp,
            "q": sequence * 2 + 2,
        },
    ], sort_keys=True, separators=(",", ":")).encode("ascii")


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


def run_qualification(
    *,
    message_count: int = MESSAGE_COUNT,
    batch_size: int = BATCH_SIZE,
) -> dict[str, object]:
    if (
        type(message_count) is not int
        or not 1 <= message_count <= MESSAGE_COUNT
        or type(batch_size) is not int
        or not 1 <= batch_size <= message_count
    ):
        raise ValueError("capacity qualification bounds are invalid")
    candidates = _candidates()
    plan_started = time.perf_counter_ns()
    plan = build_broad_options_stream_plan(
        candidates,
        session_date=SESSION_DATE,
        reviewed_at=REVIEWED_AT,
        capability_profile_sha256="a" * 64,
        authorization_reference="provider-free-broad-stream-capacity-v1",
    )
    plan_milliseconds = (
        time.perf_counter_ns() - plan_started
    ) / 1_000_000
    subscription_payload = json.dumps(
        {
            "action": "subscribe",
            "params": ",".join(
                f"{row.channel.value}.{row.provider_symbol}"
                for row in plan.manifest.members
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    admissions = {
        row.contract_ticker: InstrumentAdmission(
            instrument_id=index,
            provider_symbol=row.contract_ticker,
            underlying=row.underlyer,
            asset_class=AssetClass.OPTION,
            shard_id=0,
        )
        for index, row in enumerate(plan.contracts, start=1)
    }
    stream_session_id = uuid5(
        NAMESPACE_URL,
        "provider-free-broad-stream-capacity-v1:session",
    )
    journal_generation = uuid5(
        NAMESPACE_URL,
        "provider-free-broad-stream-capacity-v1:journals",
    )
    buffer = RawMessageBuffer(
        BufferLimits(
            maximum_events=MESSAGE_COUNT,
            maximum_bytes=BUFFER_MAXIMUM_BYTES,
            maximum_age_ns=60_000_000_000,
        ),
        monotonic_ns=time.perf_counter_ns,
    )
    total_payload_bytes = 0
    for index in range(message_count):
        payload = _payload(
            plan.contracts[index % len(plan.contracts)].contract_ticker,
            index,
        )
        total_payload_bytes += len(payload)
        buffer.put_nowait(RawMessageEnvelope(
            stream_session_id=stream_session_id,
            message_number=index,
            feed=MarketFeed.OPTIONS,
            received_at=REVIEWED_AT,
            received_monotonic_ns=time.perf_counter_ns(),
            payload=payload,
        ))
    snapshot = buffer.snapshot()
    if (
        snapshot.queued_events != message_count
        or snapshot.payload_bytes != total_payload_bytes
    ):
        raise RuntimeError("capacity fixture did not fill the buffer exactly")

    callback_times = []
    with tempfile.TemporaryDirectory(
        prefix="advanced-options-v3-broad-capacity-",
    ) as directory:
        directory_path = Path(directory)
        journal_path = directory_path / "options.sqlite"
        pump = AdvancedOptionsJournalPump(
            buffer,
            journal_path,
            journal_generation,
            admissions,
            maximum_batch_messages=batch_size,
            maximum_batch_bytes=BUFFER_MAXIMUM_BYTES,
            on_progress=lambda progress: (
                callback_times.append(time.perf_counter_ns())
                if progress.last_raw_offset > 0
                else None
            ),
        )
        started = time.perf_counter_ns()
        pump.start()
        pump.request_stop()
        progress = pump.join(PUMP_JOIN_TIMEOUT_SECONDS)
        completed = time.perf_counter_ns()
        physical_journal_bytes = _journal_files_bytes(journal_path)
        elapsed_seconds = (completed - started) / 1_000_000_000
        interval_starts = [started, *callback_times[:-1]]
        batch_latencies_ms = [
            (ended - began) / 1_000_000
            for began, ended in zip(interval_starts, callback_times)
        ]
        cleanup_root = directory_path
    cleanup_empty = not cleanup_root.exists()

    p95 = _percentile(batch_latencies_ms, 0.95)
    p99 = _percentile(batch_latencies_ms, 0.99)
    exact_offsets = (
        progress.last_raw_offset == message_count
        and progress.last_event_offset == message_count * 2
        and progress.admitted_market_messages == message_count
        and progress.admitted_market_bytes == total_payload_bytes
    )
    passed = (
        len(plan.contracts) == CONTRACT_COUNT
        and len(plan.manifest.members) == SUBSCRIPTION_COUNT
        and total_payload_bytes <= BUFFER_MAXIMUM_BYTES
        and exact_offsets
        and p95 <= JOURNAL_ACCEPT_P95_MILLISECONDS
        and p99 <= JOURNAL_ACCEPT_P99_MILLISECONDS
        and elapsed_seconds <= MAXIMUM_ELAPSED_SECONDS
        and cleanup_empty
    )
    return {
        "schema": "advanced-options-v3-broad-stream-capacity-v1",
        "status": "PASS" if passed else "FAIL",
        "classification": "PROVIDER_FREE_SYNTHETIC_HOST_ONLY",
        "scope": {
            "underlyings": ["QQQ", "SPY"],
            "minimum_dte": 0,
            "maximum_dte": 60,
            "contract_count": len(plan.contracts),
            "subscription_count": len(plan.manifest.members),
            "manifest_sha256": plan.manifest.sha256,
            "plan_sha256": plan.plan_sha256,
            "subscription_control_payload_bytes": len(subscription_payload),
        },
        "load": {
            "raw_message_count": message_count,
            "normalized_event_count": message_count * 2,
            "batch_size": batch_size,
            "batch_count": len(batch_latencies_ms),
            "total_payload_bytes": total_payload_bytes,
            "buffer_maximum_events": MESSAGE_COUNT,
            "buffer_maximum_bytes": BUFFER_MAXIMUM_BYTES,
        },
        "result": {
            "plan_milliseconds": plan_milliseconds,
            "elapsed_seconds": elapsed_seconds,
            "raw_messages_per_second": message_count / elapsed_seconds,
            "normalized_events_per_second": (
                message_count * 2 / elapsed_seconds
            ),
            "payload_megabytes_per_second": (
                total_payload_bytes / elapsed_seconds / 1_000_000
            ),
            "physical_journal_bytes": physical_journal_bytes,
            "physical_bytes_per_raw_message": (
                physical_journal_bytes / message_count
            ),
            "last_raw_offset": progress.last_raw_offset,
            "last_event_offset": progress.last_event_offset,
            "batch_latency_milliseconds": {
                "p95": p95,
                "p99": p99,
                "maximum": max(batch_latencies_ms),
            },
            "cleanup_empty": cleanup_empty,
        },
        "thresholds": {
            "journal_accept_p95_milliseconds": (
                JOURNAL_ACCEPT_P95_MILLISECONDS
            ),
            "journal_accept_p99_milliseconds": (
                JOURNAL_ACCEPT_P99_MILLISECONDS
            ),
            "maximum_elapsed_seconds": MAXIMUM_ELAPSED_SECONDS,
            "exact_offsets_required": True,
            "cleanup_empty_required": True,
        },
        "effects": {
            "provider_requests": 0,
            "provider_connections": 0,
            "production_database_reads": 0,
            "production_database_writes": 0,
            "docker_actions": 0,
            "service_actions": 0,
            "publication": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
        "limitations": [
            "Synthetic host capacity only; provider message rate and wire size "
            "remain NOT_CHECKED.",
            "Subscription acknowledgement, entitlement and one-connection "
            "behavior remain NOT_CHECKED.",
            "This result does not authorize a provider connection.",
        ],
    }


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--messages", type=int, default=MESSAGE_COUNT)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args(argv)
    args.output = args.output.resolve()
    if args.output.exists() or not args.output.parent.is_dir():
        parser.error("capacity report output scope is invalid")
    return args


def main(argv=None):
    args = parse_args(argv)
    report = run_qualification(
        message_count=args.messages,
        batch_size=args.batch_size,
    )
    _write(args.output, report)
    print(json.dumps({
        "status": report["status"],
        "contract_count": report["scope"]["contract_count"],
        "subscription_count": report["scope"]["subscription_count"],
        "raw_messages_per_second": (
            report["result"]["raw_messages_per_second"]
        ),
        "journal_accept_p95_milliseconds": (
            report["result"]["batch_latency_milliseconds"]["p95"]
        ),
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
