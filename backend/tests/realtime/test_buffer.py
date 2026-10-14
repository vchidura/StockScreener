from datetime import datetime, timezone
from uuid import UUID

import pytest

from realtime.buffer import BufferFull, BufferLimits, RawMessageBuffer
from realtime.contracts import MarketFeed, RawMessageEnvelope


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
SESSION = UUID("22222222-2222-2222-2222-222222222222")


class Clock:
    def __init__(self, value=100):
        self.value = value

    def __call__(self):
        return self.value


def _message(number, payload=b"1234"):
    return RawMessageEnvelope(
        stream_session_id=SESSION,
        message_number=number,
        feed=MarketFeed.STOCKS,
        received_at=NOW,
        received_monotonic_ns=100 + number,
        payload=payload,
    )


def test_buffer_claim_ack_retains_capacity_until_durable_ack():
    clock = Clock()
    buffer = RawMessageBuffer(
        BufferLimits(maximum_events=2, maximum_bytes=8, maximum_age_ns=100),
        monotonic_ns=clock,
    )
    buffer.put_nowait(_message(0))
    buffer.put_nowait(_message(1))

    claim = buffer.claim_nowait(maximum_events=2, maximum_bytes=8)
    claimed = buffer.snapshot()
    with pytest.raises(BufferFull, match="MAXIMUM_EVENTS_EXCEEDED"):
        buffer.put_nowait(_message(2))
    buffer.acknowledge(claim.claim_id)
    buffer.put_nowait(_message(2))

    assert claimed.queued_events == 0
    assert claimed.claimed_events == 2
    assert claimed.payload_bytes == 8
    assert buffer.snapshot().admitted_events == 1


def test_buffer_release_returns_claim_to_front_without_reordering():
    clock = Clock()
    buffer = RawMessageBuffer(
        BufferLimits(maximum_events=3, maximum_bytes=12, maximum_age_ns=100),
        monotonic_ns=clock,
    )
    for number in range(3):
        buffer.put_nowait(_message(number))

    first = buffer.claim_nowait(maximum_events=2, maximum_bytes=8)
    buffer.release(first.claim_id)
    repeated = buffer.claim_nowait(maximum_events=3, maximum_bytes=12)

    assert [message.message_number for message in repeated.messages] == [0, 1, 2]


def test_buffer_never_evicts_on_byte_or_age_exhaustion():
    clock = Clock()
    buffer = RawMessageBuffer(
        BufferLimits(maximum_events=5, maximum_bytes=5, maximum_age_ns=10),
        monotonic_ns=clock,
    )
    buffer.put_nowait(_message(0))
    with pytest.raises(BufferFull, match="MAXIMUM_BYTES_EXCEEDED"):
        buffer.put_nowait(_message(1))
    assert buffer.snapshot().admitted_events == 1

    clock.value = 111
    with pytest.raises(BufferFull, match="MAXIMUM_AGE_EXCEEDED"):
        buffer.put_nowait(_message(2, b"1"))
    snapshot = buffer.snapshot()
    assert snapshot.admitted_events == 1
    assert snapshot.full_count == 2


def test_buffer_rejects_single_oversized_message_and_claim_mismatch():
    buffer = RawMessageBuffer(
        BufferLimits(maximum_events=2, maximum_bytes=4, maximum_age_ns=100),
        monotonic_ns=Clock(),
    )
    with pytest.raises(BufferFull, match="MESSAGE_EXCEEDS_BYTE_BUDGET"):
        buffer.put_nowait(_message(0, b"12345"))
    buffer.put_nowait(_message(1))
    claim = buffer.claim_nowait(maximum_events=1, maximum_bytes=4)
    with pytest.raises(ValueError, match="does not own"):
        buffer.acknowledge(UUID("33333333-3333-3333-3333-333333333333"))
    buffer.acknowledge(claim.claim_id)