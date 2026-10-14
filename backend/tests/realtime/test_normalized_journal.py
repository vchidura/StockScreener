import multiprocessing
import os
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from realtime.contracts import AssetClass, MarketFeed, RawMessageEnvelope
from realtime.journal import RawJournalReader, RawJournalWriter
from realtime.normalizer import (
    NORMALIZER_VERSION,
    InstrumentAdmission,
    normalize_raw_message,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, 1, tzinfo=UTC)
GENERATION = UUID("11111111-1111-1111-1111-111111111111")
SESSION = UUID("22222222-2222-2222-2222-222222222222")
ADMISSIONS = {
    "SPY": InstrumentAdmission(1, "SPY", "SPY", AssetClass.EQUITY, 0),
}


def _message(number, payload):
    return RawMessageEnvelope(
        stream_session_id=SESSION,
        message_number=number,
        feed=MarketFeed.STOCKS,
        received_at=NOW,
        received_monotonic_ns=100 + number,
        payload=payload,
    )


def _quote_payload(sequence=1):
    timestamp = int((NOW.timestamp() - 0.1) * 1000)
    return (
        '[{"ev":"Q","sym":"SPY","bp":100,"ap":101,"bs":10,'
        f'"as":11,"t":{timestamp},"q":{sequence}}}]'
    ).encode("ascii")


def _normalize_committed_raw(writer, reader, raw_offset):
    raw = reader.read_after(raw_offset - 1, limit=1)[0]
    return writer.commit_normalized(normalize_raw_message(raw, ADMISSIONS))


def _normalize_then_crash(path):
    writer = RawJournalWriter(path, generation=GENERATION)
    writer.append_batch((_message(0, _quote_payload()),))
    reader = RawJournalReader(path)
    _normalize_committed_raw(writer, reader, 1)
    os._exit(23)


def test_normalized_events_and_progress_commit_atomically(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION, clock=lambda: NOW) as writer:
        writer.append_batch(
            (
                _message(0, _quote_payload(1)),
                _message(1, b"not-json"),
            )
        )
        reader = RawJournalReader(path)
        first = _normalize_committed_raw(writer, reader, 1)
        second = _normalize_committed_raw(writer, reader, 2)

    reader = RawJournalReader(path)
    progress = reader.decoder_progress(NORMALIZER_VERSION)
    events = reader.read_normalized_after(0)
    quarantines = reader.read_quarantines()

    assert first.event_count == 1 and first.quarantine_count == 0
    assert second.event_count == 0 and second.quarantine_count == 1
    assert progress.committed_raw_offset == 2
    assert progress.committed_event_offset == 1
    assert len(events) == 1 and events[0].raw_offset == 1
    assert events[0].bid == Decimal("100")
    assert quarantines[0].reason_code == "INVALID_JSON"
    assert quarantines[0].event_index == -1


def test_decoder_progress_cannot_skip_a_retained_raw_offset(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION) as writer:
        writer.append_batch(
            (
                _message(0, _quote_payload(1)),
                _message(1, _quote_payload(2)),
            )
        )
        reader = RawJournalReader(path)
        second_raw = reader.read_after(1, limit=1)[0]
        second_batch = normalize_raw_message(second_raw, ADMISSIONS)

        with pytest.raises(ValueError, match="next retained raw offset"):
            writer.commit_normalized(second_batch)

    assert RawJournalReader(path).decoder_progress(NORMALIZER_VERSION) is None


def test_normalized_replay_survives_reopen_with_exact_fields(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION) as writer:
        writer.append_batch((_message(0, _quote_payload()),))
        _normalize_committed_raw(writer, RawJournalReader(path), 1)

    event = RawJournalReader(path).read_normalized_after(0)[0]

    assert event.instrument_id == 1
    assert event.journal_generation == GENERATION
    assert event.stream_session_id == SESSION
    assert str(event.bid) == "100"
    assert str(event.ask) == "101"
    assert event.conditions == ()
    assert event.details_json
    assert event.received_at == NOW


def test_normalized_commit_survives_process_crash(tmp_path):
    path = tmp_path / "raw.sqlite"
    process = multiprocessing.get_context("spawn").Process(
        target=_normalize_then_crash,
        args=(path,),
    )
    process.start()
    process.join(15)

    assert process.exitcode == 23
    reader = RawJournalReader(path)
    assert reader.decoder_progress(NORMALIZER_VERSION).committed_raw_offset == 1
    assert len(reader.read_normalized_after(0)) == 1