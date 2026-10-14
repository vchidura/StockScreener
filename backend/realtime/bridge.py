from __future__ import annotations

from .buffer import RawMessageBuffer
from .journal import JournalCommit, RawJournalWriter


def drain_raw_buffer_once(
    buffer: RawMessageBuffer,
    writer: RawJournalWriter,
    *,
    maximum_events: int,
    maximum_bytes: int,
) -> JournalCommit | None:
    claim = buffer.claim_nowait(
        maximum_events=maximum_events,
        maximum_bytes=maximum_bytes,
    )
    if claim is None:
        return None
    try:
        commit = writer.append_batch(claim.messages)
    except Exception:
        buffer.release(claim.claim_id)
        raise
    buffer.acknowledge(claim.claim_id)
    return commit