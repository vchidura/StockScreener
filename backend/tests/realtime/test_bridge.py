from datetime import datetime, timezone
from uuid import UUID

import pytest

from realtime.bridge import drain_raw_buffer_once
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import MarketFeed, RawMessageEnvelope
from realtime.journal import RawJournalReader, RawJournalWriter


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
SESSION = UUID("22222222-2222-2222-2222-222222222222")
GENERATION = UUID("11111111-1111-1111-1111-111111111111")


def _message(number):
    return RawMessageEnvelope(
        stream_session_id=SESSION,
        message_number=number,
        feed=MarketFeed.STOCKS,
        received_at=NOW,
        received_monotonic_ns=100 + number,
        payload=f"message-{number}".encode("ascii"),
    )


def _buffer():
    return RawMessageBuffer(
        BufferLimits(maximum_events=5, maximum_bytes=100, maximum_age_ns=1_000),
        monotonic_ns=lambda: 110,
    )


def test_bridge_acknowledges_only_after_durable_append(tmp_path):
    buffer = _buffer()
    buffer.put_nowait(_message(0))
    buffer.put_nowait(_message(1))
    path = tmp_path / "raw.sqlite"

    with RawJournalWriter(path, generation=GENERATION) as writer:
        commit = drain_raw_buffer_once(
            buffer,
            writer,
            maximum_events=5,
            maximum_bytes=100,
        )

    assert commit.inserted_count == 2
    assert buffer.snapshot().admitted_events == 0
    assert len(RawJournalReader(path).read_after(0)) == 2


def test_bridge_releases_claim_in_original_order_after_writer_failure():
    class FailedWriter:
        def append_batch(self, messages):
            raise OSError("journal unavailable")

    buffer = _buffer()
    buffer.put_nowait(_message(0))
    buffer.put_nowait(_message(1))

    with pytest.raises(OSError, match="journal unavailable"):
        drain_raw_buffer_once(
            buffer,
            FailedWriter(),
            maximum_events=5,
            maximum_bytes=100,
        )
    claim = buffer.claim_nowait(maximum_events=5, maximum_bytes=100)

    assert [message.message_number for message in claim.messages] == [0, 1]