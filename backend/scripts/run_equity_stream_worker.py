#!/usr/bin/env python3
"""Run Polygon Advanced stock minute aggregates into immutable equity bars."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from database import get_selected_tickers
from equity.domain import DecisionWatermark
from equity.leadership import try_advisory_leadership
from equity.polygon_stream import PolygonAdvancedStreamClient
from equity.repositories import EquityIngestionRepository, EquityReferenceRepository
from equity.stream import EquityStreamProcessor, StreamBatchResult


LOGGER = logging.getLogger("equity-stream-worker")
WORKER_LOCK_NAME = os.getenv(
    "EQUITY_STREAM_WORKER_LOCK_NAME", "stock-screener:equity-stream-worker"
)
FLUSH_SECONDS = float(os.getenv("EQUITY_STREAM_FLUSH_SECONDS", "1"))
MAX_RECONNECTS = int(os.getenv("EQUITY_STREAM_MAX_RECONNECTS", "20"))


@dataclass
class StreamCounters:
    received_messages: int = 0
    accepted_minutes: int = 0
    duplicate_minutes: int = 0
    ignored_messages: int = 0
    finalized_bars: int = 0
    inserted_bars: int = 0

    def __post_init__(self) -> None:
        self._lock = Lock()

    def add(self, result: StreamBatchResult) -> None:
        with self._lock:
            self.received_messages += result.received_messages
            self.accepted_minutes += result.accepted_minutes
            self.duplicate_minutes += result.duplicate_minutes
            self.ignored_messages += result.ignored_messages
            self.finalized_bars += result.finalized_bars
            self.inserted_bars += result.inserted_bars

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "received_messages": self.received_messages,
                "accepted_minutes": self.accepted_minutes,
                "duplicate_minutes": self.duplicate_minutes,
                "ignored_messages": self.ignored_messages,
                "finalized_bars": self.finalized_bars,
                "inserted_bars": self.inserted_bars,
            }


def configured_tickers(explicit: str | None) -> tuple[str, ...]:
    if explicit:
        return tuple(sorted({
            value.strip().upper() for value in explicit.split(",") if value.strip()
        }))
    return tuple(sorted(get_selected_tickers(active_only=True)))


def load_references(tickers: tuple[str, ...], observed_at: datetime):
    watermark = DecisionWatermark(observed_at, observed_at)
    references = EquityReferenceRepository().list_securities_as_of(tickers, watermark)
    found = {row.ticker for row in references}
    missing = tuple(sorted(set(tickers) - found))
    if missing:
        raise RuntimeError(f"missing security references: {','.join(missing[:20])}")
    return references


async def run_stream_session(
    client: PolygonAdvancedStreamClient,
    processor: EquityStreamProcessor,
    counters: StreamCounters,
    tickers: tuple[str, ...],
) -> None:
    async def handler(messages) -> None:
        result = await asyncio.to_thread(
            processor.process,
            messages,
            observed_at=datetime.now(timezone.utc),
        )
        counters.add(result)
        if result.finalized_bars:
            LOGGER.info("stream batch=%s totals=%s", result, counters.snapshot())

    async def flush_loop() -> None:
        while True:
            await asyncio.sleep(FLUSH_SECONDS)
            result = await asyncio.to_thread(
                processor.flush, observed_at=datetime.now(timezone.utc)
            )
            counters.add(result)
            if result.finalized_bars:
                LOGGER.info("timer flush=%s totals=%s", result, counters.snapshot())

    stream_task = asyncio.create_task(
        client.connect_minute_aggregates(tickers, handler)
    )
    flush_task = asyncio.create_task(flush_loop())
    try:
        done, _ = await asyncio.wait(
            (stream_task, flush_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            task.result()
    finally:
        for task in (stream_task, flush_task):
            task.cancel()
        for task in (stream_task, flush_task):
            with suppress(asyncio.CancelledError):
                await task


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--tickers", help="Comma-separated subset; defaults to active universe")
    result.add_argument("--validate-config", action="store_true")
    return result


def run(args) -> int:
    if FLUSH_SECONDS <= 0:
        raise ValueError("EQUITY_STREAM_FLUSH_SECONDS must be positive")
    if MAX_RECONNECTS < 0:
        raise ValueError("EQUITY_STREAM_MAX_RECONNECTS cannot be negative")
    tickers = configured_tickers(args.tickers)
    if not tickers:
        raise RuntimeError("no active equity tickers are configured")
    observed_at = datetime.now(timezone.utc)
    references = load_references(tickers, observed_at)
    client = PolygonAdvancedStreamClient(max_reconnects=MAX_RECONNECTS)
    if args.validate_config:
        print({
            "status": "CONFIG_VALID",
            "ticker_count": len(tickers),
            "reference_count": len(references),
            "subscriptions": len(tickers),
        })
        return 0

    segment_id = uuid4()
    ingestion_repository = EquityIngestionRepository()
    ingestion_repository.start_segment(
        ingestion_segment_id=segment_id,
        provider="polygon",
        provider_mode="WEBSOCKET",
        dataset="EQUITY_MINUTE_AGGREGATES",
        interval="1m",
        requested_from=observed_at,
        requested_to=None,
        observed_at=observed_at,
    )
    processor = EquityStreamProcessor(
        references, ingestion_segment_id=segment_id
    )
    counters = StreamCounters()
    status = "COMPLETE"
    reason = "STREAM_CLOSED"
    error = None
    try:
        asyncio.run(run_stream_session(client, processor, counters, tickers))
    except KeyboardInterrupt:
        reason = "OPERATOR_INTERRUPTED"
    except Exception as exc:
        status = "FAILED"
        reason = type(exc).__name__
        error = exc
    finally:
        metrics = counters.snapshot()
        ingestion_repository.complete_segment(
            segment_id,
            status=status,
            market_watermark=processor.market_watermark,
            record_count=metrics["accepted_minutes"],
            gap_details={"completion_reason": reason, **metrics},
            completed_at=datetime.now(timezone.utc),
        )
        LOGGER.info("stream segment=%s status=%s metrics=%s", segment_id, status, metrics)
    if error is not None:
        raise error
    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )
    args = parser().parse_args()
    with try_advisory_leadership(WORKER_LOCK_NAME) as is_leader:
        if not is_leader:
            LOGGER.error("another equity worker holds leadership")
            return 2
        return run(args)


if __name__ == "__main__":
    raise SystemExit(main())