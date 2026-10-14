from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.archive import (
    ConsumerWatermark,
    assert_archive_truncation_safe,
    persist_archive_segment,
    verify_archive_segment,
    write_normalized_archive_segment,
)
from realtime.contracts import AssetClass, MarketChannel
from realtime.journal import NormalizedJournalRecord


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
GENERATION = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SESSION = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _record(offset):
    return NormalizedJournalRecord(
        event_offset=offset,
        raw_offset=offset,
        event_index=0,
        normalizer_version="fixture-v1",
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=1,
        provider_symbol="SPY",
        underlying="SPY",
        asset_class=AssetClass.EQUITY,
        shard_id=0,
        event_kind=MarketChannel.QUOTE,
        source_session=date(2026, 10, 5),
        source_sequence=offset,
        source_timestamp=1_000 + offset,
        source_timestamp_unit="ms",
        event_at=NOW + timedelta(milliseconds=offset),
        price=None,
        size=None,
        bid=Decimal("100"),
        ask=Decimal("101"),
        bid_size=10,
        ask_size=11,
        exchange=None,
        conditions=(),
        quality="GOOD",
        reason_codes=(),
        details_json="{}",
        payload_sha256=f"{offset:064x}",
        received_at=NOW + timedelta(milliseconds=offset + 1),
        committed_at=NOW + timedelta(milliseconds=offset + 2),
    )


def test_archive_write_is_atomic_immutable_and_self_verifying(tmp_path):
    destination = tmp_path / "segment.jsonl.gz"
    segment = write_normalized_archive_segment(
        (_record(1), _record(3)),
        destination,
        journal_generation=GENERATION,
        shard_id=0,
        verified_at=NOW,
    )

    assert destination.exists()
    assert not tuple(tmp_path.glob("*.partial"))
    assert segment.first_offset == 1
    assert segment.last_offset == 3
    assert segment.row_count == 2
    verify_archive_segment(segment)

    with pytest.raises(FileExistsError):
        write_normalized_archive_segment(
            (_record(1),),
            destination,
            journal_generation=GENERATION,
            shard_id=0,
            verified_at=NOW,
        )
    with pytest.raises(ValueError, match="row limit"):
        write_normalized_archive_segment(
            (_record(1), _record(3)),
            tmp_path / "too-many.jsonl.gz",
            journal_generation=GENERATION,
            shard_id=0,
            verified_at=NOW,
            maximum_rows=1,
        )
    with pytest.raises(ValueError, match="byte limit"):
        write_normalized_archive_segment(
            (_record(1),),
            tmp_path / "too-large.jsonl.gz",
            journal_generation=GENERATION,
            shard_id=0,
            verified_at=NOW,
            maximum_uncompressed_bytes=1,
        )


def test_archive_truncation_gate_requires_all_watermarks_and_no_protection(tmp_path):
    segment = write_normalized_archive_segment(
        (_record(1), _record(3)),
        tmp_path / "segment.jsonl.gz",
        journal_generation=GENERATION,
        shard_id=0,
        verified_at=NOW,
    )
    cleared = (
        ConsumerWatermark("quotes", GENERATION, 0, 3),
        ConsumerWatermark("trades", GENERATION, 0, 4),
    )

    assert assert_archive_truncation_safe(segment, cleared) == 3
    with pytest.raises(ValueError, match="explicit consumer"):
        assert_archive_truncation_safe(segment, ())
    with pytest.raises(ValueError, match="has not cleared"):
        assert_archive_truncation_safe(
            segment,
            (ConsumerWatermark("quotes", GENERATION, 0, 2),),
        )
    with pytest.raises(ValueError, match="protected evidence"):
        assert_archive_truncation_safe(segment, cleared, protected_offsets=(2,))
    with pytest.raises(ValueError, match="pending materialization"):
        assert_archive_truncation_safe(segment, cleared, pending_job_count=1)
    with pytest.raises(ValueError, match="must be unique"):
        assert_archive_truncation_safe(segment, (cleared[0], cleared[0]))


def test_archive_verification_detects_tampering(tmp_path):
    destination = tmp_path / "segment.jsonl.gz"
    segment = write_normalized_archive_segment(
        (_record(1),),
        destination,
        journal_generation=GENERATION,
        shard_id=0,
        verified_at=NOW,
    )
    destination.write_bytes(destination.read_bytes() + b"tampered")

    with pytest.raises(ValueError, match="checksum"):
        verify_archive_segment(segment)


def test_archive_manifest_persistence_reconciles_immutable_identity(tmp_path):
    segment = write_normalized_archive_segment(
        (_record(1),),
        tmp_path / "segment.jsonl.gz",
        journal_generation=GENERATION,
        shard_id=0,
        verified_at=NOW,
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        segment.journal_generation,
        segment.shard_id,
        segment.first_offset,
        segment.last_offset,
        segment.row_count,
        segment.uri,
        segment.checksum_sha256,
        segment.schema_version,
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_archive_segment(connection, segment)

    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0