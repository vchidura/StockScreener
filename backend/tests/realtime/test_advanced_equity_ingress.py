import json
import time
from datetime import datetime, timezone
from uuid import UUID

import pytest

from realtime.advanced_equity_ingress import (
    AdvancedEquityJournalPump,
    AdvancedJournalDecodeError,
)
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import (
    AssetClass,
    MarketFeed,
    RawMessageEnvelope,
)
from realtime.journal import RawJournalReader
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION


NOW = datetime(2026, 10, 16, 16, 30, 1, tzinfo=timezone.utc)
SESSION = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")


def _buffer():
    return RawMessageBuffer(
        BufferLimits(8, 64 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )


def _message(payload, number=0):
    return RawMessageEnvelope(
        SESSION,
        number,
        MarketFeed.STOCKS,
        NOW,
        time.monotonic_ns(),
        payload,
    )


def _admissions():
    return {
        "SPY": InstrumentAdmission(
            1,
            "SPY",
            "SPY",
            AssetClass.EQUITY,
            0,
        )
    }


def test_journal_pump_commits_and_normalizes_fractional_am_bytes(tmp_path):
    start = int(NOW.timestamp() * 1000) - 60_000
    end = start + 60_000
    payload = json.dumps([{
        "ev": "AM",
        "sym": "SPY",
        "o": 100,
        "h": 101,
        "l": 99,
        "c": 100.5,
        "v": 1000,
        "dv": "1000.125",
        "vw": 100.25,
        "s": start,
        "e": end,
    }]).encode()
    buffer = _buffer()
    buffer.put_nowait(_message(payload))
    path = tmp_path / "advanced.sqlite"
    pump = AdvancedEquityJournalPump(
        buffer,
        path,
        GENERATION,
        _admissions(),
        maximum_batch_messages=4,
        maximum_batch_bytes=64 * 1024,
    )

    pump.start()
    pump.request_stop()
    progress = pump.join(5)

    reader = RawJournalReader(path)
    normalized = reader.read_normalized_after(0)
    assert progress.admitted_market_messages == 1
    assert progress.last_raw_offset == progress.last_event_offset == 1
    assert normalized[0].normalizer_version == NORMALIZER_VERSION
    assert str(normalized[0].size) == "1000.125"
    usage = pump.usage(
        received_frame_count=3,
        received_byte_count=len(payload) + 100,
    )
    assert usage.admitted_market_byte_count == len(payload)


def test_journal_pump_fails_closed_for_an_unexpected_channel(tmp_path):
    timestamp = int((NOW.timestamp() - 1) * 1000)
    payload = json.dumps([{
        "ev": "T",
        "sym": "SPY",
        "p": 100,
        "s": 1,
        "x": 4,
        "t": timestamp,
    }]).encode()
    buffer = _buffer()
    buffer.put_nowait(_message(payload))
    pump = AdvancedEquityJournalPump(
        buffer,
        tmp_path / "advanced.sqlite",
        GENERATION,
        _admissions(),
        maximum_batch_messages=4,
        maximum_batch_bytes=64 * 1024,
    )

    pump.start()
    pump.request_stop()
    with pytest.raises(
        AdvancedJournalDecodeError,
        match="UNEXPECTED_CHANNEL",
    ):
        pump.join(5)
