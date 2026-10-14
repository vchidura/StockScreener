import multiprocessing
import os
import sqlite3
import threading
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from realtime.contracts import MarketFeed, RawMessageEnvelope
from realtime.journal import RawJournalReader, RawJournalWriter


UTC = timezone.utc
NOW = datetime(2026, 10, 3, 17, 0, tzinfo=UTC)
GENERATION = UUID("11111111-1111-1111-1111-111111111111")
SESSION = UUID("22222222-2222-2222-2222-222222222222")


def _message(number=0, payload=b'[{"ev":"Q","sym":"SPY"}]'):
    return RawMessageEnvelope(
        stream_session_id=SESSION,
        message_number=number,
        feed=MarketFeed.STOCKS,
        received_at=NOW,
        received_monotonic_ns=100 + number,
        payload=payload,
    )


def _append_then_crash(path):
    writer = RawJournalWriter(path, generation=GENERATION)
    writer.append_batch((_message(),))
    os._exit(17)


def _insert_without_commit_then_crash(path):
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "INSERT INTO raw_messages ("
        "journal_generation, stream_session_id, message_number, feed, received_at, "
        "received_monotonic_ns, byte_length, payload, payload_sha256, "
        "schema_version, committed_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            str(GENERATION),
            str(SESSION),
            99,
            "STOCKS",
            NOW.isoformat(),
            199,
            2,
            b"{}",
            "4" * 64,
            1,
            NOW.isoformat(),
        ),
    )
    os._exit(19)


def test_journal_uses_full_wal_and_replays_committed_bytes(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION, clock=lambda: NOW) as writer:
        commit = writer.append_batch((_message(0), _message(1, b'[{"ev":"T"}]')))
        health = writer.health()

    records = RawJournalReader(path).read_after(0)

    assert health.journal_mode == "wal"
    assert health.synchronous == 2
    assert health.last_offset == 2
    assert commit.first_offset == 1 and commit.last_offset == 2
    assert commit.inserted_count == 2 and commit.duplicate_count == 0
    assert [record.payload for record in records] == [
        _message(0).payload,
        _message(1, b'[{"ev":"T"}]').payload,
    ]
    assert all(record.generation == GENERATION for record in records)


def test_journal_reopen_preserves_generation_and_never_reuses_offsets(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION) as writer:
        writer.append_batch((_message(0),))
    with RawJournalWriter(path, generation=GENERATION) as writer:
        second = writer.append_batch((_message(1),))

    assert second.first_offset == 2
    assert [record.raw_offset for record in RawJournalReader(path).read_after(0)] == [
        1,
        2,
    ]
    with pytest.raises(RuntimeError, match="generation does not match"):
        RawJournalWriter(path, generation=uuid4())


def test_journal_duplicate_is_idempotent_but_conflict_fails(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION) as writer:
        first = writer.append_batch((_message(),))
        duplicate = writer.append_batch((_message(),))
        with pytest.raises(ValueError, match="conflicts"):
            writer.append_batch((_message(payload=b"different"),))

    assert first.inserted_count == 1
    assert duplicate.inserted_count == 0 and duplicate.duplicate_count == 1
    assert len(RawJournalReader(path).read_after(0)) == 1


def test_journal_writer_rejects_another_thread(tmp_path):
    path = tmp_path / "raw.sqlite"
    writer = RawJournalWriter(path, generation=GENERATION)
    errors = []

    def append():
        try:
            writer.append_batch((_message(),))
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=append)
    thread.start()
    thread.join()
    writer.close()

    assert len(errors) == 1
    assert "owner thread" in str(errors[0])


def test_acknowledged_commit_survives_process_crash(tmp_path):
    path = tmp_path / "raw.sqlite"
    process = multiprocessing.get_context("spawn").Process(
        target=_append_then_crash,
        args=(path,),
    )
    process.start()
    process.join(15)

    assert process.exitcode == 17
    records = RawJournalReader(path).read_after(0)
    assert len(records) == 1
    assert records[0].payload == _message().payload


def test_uncommitted_process_crash_is_not_acknowledged(tmp_path):
    path = tmp_path / "raw.sqlite"
    with RawJournalWriter(path, generation=GENERATION):
        pass
    process = multiprocessing.get_context("spawn").Process(
        target=_insert_without_commit_then_crash,
        args=(path,),
    )
    process.start()
    process.join(15)

    assert process.exitcode == 19
    assert RawJournalReader(path).read_after(0) == ()