import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import run_equity_stream_worker as worker


UTC = timezone.utc


class FakeIngestionRepository:
    instance = None

    def __init__(self):
        self.started = None
        self.completed = None
        FakeIngestionRepository.instance = self

    def start_segment(self, **kwargs):
        self.started = kwargs

    def complete_segment(self, segment_id, **kwargs):
        self.completed = (segment_id, kwargs)


class FakeProcessor:
    def __init__(self, references, *, ingestion_segment_id):
        self.references = references
        self.ingestion_segment_id = ingestion_segment_id
        self.market_watermark = None


def test_validate_config_checks_references_without_creating_segment(capsys):
    args = SimpleNamespace(tickers="AAPL,MSFT", validate_config=True)
    references = (SimpleNamespace(ticker="AAPL"), SimpleNamespace(ticker="MSFT"))

    with (
        patch.object(worker, "configured_tickers", return_value=("AAPL", "MSFT")),
        patch.object(worker, "load_references", return_value=references),
        patch.object(worker, "PolygonAdvancedStreamClient"),
        patch.object(worker, "EquityIngestionRepository") as ingestion,
    ):
        result = worker.run(args)

    assert result == 0
    ingestion.assert_not_called()
    assert "'status': 'CONFIG_VALID'" in capsys.readouterr().out


def test_stream_failure_terminalizes_segment_before_reraising():
    args = SimpleNamespace(tickers="AAPL", validate_config=False)
    references = (SimpleNamespace(ticker="AAPL"),)

    async def fail_session(*args, **kwargs):
        raise RuntimeError("stream disconnected")

    with (
        patch.object(worker, "configured_tickers", return_value=("AAPL",)),
        patch.object(worker, "load_references", return_value=references),
        patch.object(worker, "PolygonAdvancedStreamClient", return_value=object()),
        patch.object(worker, "EquityIngestionRepository", FakeIngestionRepository),
        patch.object(worker, "EquityStreamProcessor", FakeProcessor),
        patch.object(worker, "run_stream_session", fail_session),
    ):
        with pytest.raises(RuntimeError, match="stream disconnected"):
            worker.run(args)

    repository = FakeIngestionRepository.instance
    assert repository.started["provider_mode"] == "WEBSOCKET"
    assert repository.started["dataset"] == "EQUITY_MINUTE_AGGREGATES"
    segment_id, completion = repository.completed
    assert segment_id == repository.started["ingestion_segment_id"]
    assert completion["status"] == "FAILED"
    assert completion["gap_details"]["completion_reason"] == "RuntimeError"
    assert completion["record_count"] == 0


def test_stream_counters_accumulate_thread_safe_batch_metrics():
    counters = worker.StreamCounters()
    result = SimpleNamespace(
        received_messages=10,
        accepted_minutes=7,
        duplicate_minutes=1,
        ignored_messages=2,
        finalized_bars=3,
        inserted_bars=10,
    )

    counters.add(result)
    counters.add(result)

    assert counters.snapshot() == {
        "received_messages": 20,
        "accepted_minutes": 14,
        "duplicate_minutes": 2,
        "ignored_messages": 4,
        "finalized_bars": 6,
        "inserted_bars": 20,
    }


def test_timer_flush_failure_stops_stream_session():
    class WaitingClient:
        async def connect_minute_aggregates(self, tickers, handler):
            await __import__("asyncio").sleep(60)

    class FailingProcessor:
        def flush(self, *, observed_at):
            raise RuntimeError("database unavailable")

    with patch.object(worker, "FLUSH_SECONDS", 0.001):
        with pytest.raises(RuntimeError, match="database unavailable"):
            __import__("asyncio").run(worker.run_stream_session(
                WaitingClient(), FailingProcessor(), worker.StreamCounters(), ("AAPL",)
            ))
