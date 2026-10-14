from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from .journal import NormalizedJournalRecord


NORMALIZED_ARCHIVE_SCHEMA = "realtime-normalized-jsonl-gzip-v1"
MAX_ARCHIVE_ROWS = 100_000
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 256 * 1024 * 1024


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class ArchiveSegment:
    segment_id: UUID
    journal_generation: UUID
    shard_id: int
    first_offset: int
    last_offset: int
    row_count: int
    first_event_at: datetime
    last_event_at: datetime
    uri: str
    checksum_sha256: str
    schema_version: str
    verified_at: datetime

    def __post_init__(self) -> None:
        uri = self.uri.strip()
        schema = self.schema_version.strip()
        if self.shard_id < 0:
            raise ValueError("archive shard ID must be nonnegative")
        if self.first_offset <= 0 or self.last_offset < self.first_offset:
            raise ValueError("archive offset range is invalid")
        if not 0 < self.row_count <= self.last_offset - self.first_offset + 1:
            raise ValueError("archive row count is invalid")
        first_event = _utc(self.first_event_at, "first_event_at")
        last_event = _utc(self.last_event_at, "last_event_at")
        if last_event < first_event:
            raise ValueError("archive event range is invalid")
        if not uri or not schema:
            raise ValueError("archive URI and schema cannot be blank")
        object.__setattr__(self, "first_event_at", first_event)
        object.__setattr__(self, "last_event_at", last_event)
        object.__setattr__(self, "uri", uri)
        object.__setattr__(self, "checksum_sha256", _sha256(self.checksum_sha256, "checksum_sha256"))
        object.__setattr__(self, "schema_version", schema)
        object.__setattr__(self, "verified_at", _utc(self.verified_at, "verified_at"))


@dataclass(frozen=True, slots=True)
class ConsumerWatermark:
    consumer_name: str
    journal_generation: UUID
    shard_id: int
    committed_offset: int

    def __post_init__(self) -> None:
        consumer = self.consumer_name.strip()
        if not consumer:
            raise ValueError("retention consumer name cannot be blank")
        if self.shard_id < 0 or self.committed_offset < 0:
            raise ValueError("retention watermark coordinates must be nonnegative")
        object.__setattr__(self, "consumer_name", consumer)


def _stamp(value: datetime) -> str:
    return _utc(value, "archive timestamp").isoformat().replace("+00:00", "Z")


def _decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _record_document(record: NormalizedJournalRecord) -> dict[str, object]:
    return {
        "event_offset": record.event_offset,
        "raw_offset": record.raw_offset,
        "event_index": record.event_index,
        "normalizer_version": record.normalizer_version,
        "journal_generation": str(record.journal_generation),
        "stream_session_id": str(record.stream_session_id),
        "instrument_id": record.instrument_id,
        "provider_symbol": record.provider_symbol,
        "underlying": record.underlying,
        "asset_class": record.asset_class.value,
        "shard_id": record.shard_id,
        "event_kind": record.event_kind.value,
        "source_session": record.source_session.isoformat(),
        "source_sequence": record.source_sequence,
        "source_timestamp": record.source_timestamp,
        "source_timestamp_unit": record.source_timestamp_unit,
        "event_at": _stamp(record.event_at),
        "price": _decimal(record.price),
        "size": _decimal(record.size),
        "bid": _decimal(record.bid),
        "ask": _decimal(record.ask),
        "bid_size": record.bid_size,
        "ask_size": record.ask_size,
        "exchange": record.exchange,
        "conditions": list(record.conditions),
        "quality": record.quality,
        "reason_codes": list(record.reason_codes),
        "details_json": record.details_json,
        "payload_sha256": record.payload_sha256,
        "received_at": _stamp(record.received_at),
        "committed_at": _stamp(record.committed_at),
    }


def _decode_archive(payload: bytes) -> tuple[dict[str, object], ...]:
    try:
        content = gzip.decompress(payload).decode("ascii")
        rows = tuple(
            json.loads(line)
            for line in content.splitlines()
            if line
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("archive payload is not valid normalized JSONL gzip") from exc
    if not rows or any(not isinstance(row, dict) for row in rows):
        raise ValueError("archive payload contains no normalized rows")
    return rows


def write_normalized_archive_segment(
    records: tuple[NormalizedJournalRecord, ...],
    destination: str | Path,
    *,
    journal_generation: UUID,
    shard_id: int,
    verified_at: datetime,
    maximum_rows: int = MAX_ARCHIVE_ROWS,
    maximum_uncompressed_bytes: int = MAX_ARCHIVE_UNCOMPRESSED_BYTES,
) -> ArchiveSegment:
    if not records:
        raise ValueError("archive segment requires normalized records")
    if maximum_rows <= 0 or maximum_uncompressed_bytes <= 0:
        raise ValueError("archive limits must be positive")
    if len(records) > maximum_rows:
        raise ValueError("archive segment exceeds its row limit")
    ordered = tuple(sorted(records, key=lambda row: row.event_offset))
    offsets = tuple(row.event_offset for row in ordered)
    if len(offsets) != len(set(offsets)):
        raise ValueError("archive event offsets must be unique")
    if any(
        row.journal_generation != journal_generation or row.shard_id != shard_id
        for row in ordered
    ):
        raise ValueError("archive record lineage does not match its segment")
    path = Path(destination)
    if path.exists():
        raise FileExistsError("archive destination already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    document = b"\n".join(
        json.dumps(
            _record_document(record),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        for record in ordered
    ) + b"\n"
    if len(document) > maximum_uncompressed_bytes:
        raise ValueError("archive segment exceeds its uncompressed byte limit")
    compressed = gzip.compress(document, compresslevel=9, mtime=0)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=path.name + ".",
            suffix=".partial",
            delete=False,
        ) as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        written = temporary.read_bytes()
        rows = _decode_archive(written)
        if len(rows) != len(ordered):
            raise ValueError("archive verification row count differs")
        verified_offsets = tuple(int(row["event_offset"]) for row in rows)
        if verified_offsets != offsets:
            raise ValueError("archive verification offsets differ")
        checksum = hashlib.sha256(written).hexdigest()
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    verified = _utc(verified_at, "verified_at")
    segment_id = uuid5(
        NAMESPACE_URL,
        f"realtime-archive:{journal_generation}:{shard_id}:{offsets[0]}:"
        f"{offsets[-1]}:{checksum}",
    )
    return ArchiveSegment(
        segment_id=segment_id,
        journal_generation=journal_generation,
        shard_id=shard_id,
        first_offset=offsets[0],
        last_offset=offsets[-1],
        row_count=len(ordered),
        first_event_at=min(row.event_at for row in ordered),
        last_event_at=max(row.event_at for row in ordered),
        uri=path.resolve().as_uri(),
        checksum_sha256=checksum,
        schema_version=NORMALIZED_ARCHIVE_SCHEMA,
        verified_at=verified,
    )


def _segment_path(segment: ArchiveSegment) -> Path:
    parsed = urlsplit(segment.uri)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        raise ValueError("archive verification requires a local file URI")
    path_text = unquote(parsed.path)
    if os.name == "nt" and path_text.startswith("/"):
        path_text = path_text[1:]
    return Path(path_text)


def verify_archive_segment(segment: ArchiveSegment) -> None:
    if segment.schema_version != NORMALIZED_ARCHIVE_SCHEMA:
        raise ValueError("archive schema version is unsupported")
    payload = _segment_path(segment).read_bytes()
    if hashlib.sha256(payload).hexdigest() != segment.checksum_sha256:
        raise ValueError("archive checksum differs from its manifest")
    rows = _decode_archive(payload)
    offsets = tuple(int(row["event_offset"]) for row in rows)
    if (
        len(rows) != segment.row_count
        or offsets[0] != segment.first_offset
        or offsets[-1] != segment.last_offset
        or len(offsets) != len(set(offsets))
        or tuple(sorted(offsets)) != offsets
    ):
        raise ValueError("archive coverage differs from its manifest")
    if any(
        row.get("journal_generation") != str(segment.journal_generation)
        or row.get("shard_id") != segment.shard_id
        for row in rows
    ):
        raise ValueError("archive lineage differs from its manifest")


def persist_archive_segment(connection, segment: ArchiveSegment) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.archive_segments ("
                "segment_id, journal_generation, shard_id, first_offset, last_offset, "
                "row_count, first_event_at, last_event_at, uri, checksum_sha256, "
                "schema_version, verified_at) VALUES (%s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s) ON CONFLICT (segment_id) DO NOTHING",
                (
                    str(segment.segment_id),
                    str(segment.journal_generation),
                    segment.shard_id,
                    segment.first_offset,
                    segment.last_offset,
                    segment.row_count,
                    segment.first_event_at,
                    segment.last_event_at,
                    segment.uri,
                    segment.checksum_sha256,
                    segment.schema_version,
                    segment.verified_at,
                ),
            )
            cursor.execute(
                "SELECT journal_generation, shard_id, first_offset, last_offset, row_count, "
                "uri, checksum_sha256, schema_version FROM rt.archive_segments "
                "WHERE segment_id=%s",
                (str(segment.segment_id),),
            )
            row = cursor.fetchone()
            expected = (
                str(segment.journal_generation),
                segment.shard_id,
                segment.first_offset,
                segment.last_offset,
                segment.row_count,
                segment.uri,
                segment.checksum_sha256,
                segment.schema_version,
            )
            if row is None or tuple(str(value) if isinstance(value, UUID) else value for value in row) != expected:
                raise ValueError("persisted archive segment identity conflict")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def assert_archive_truncation_safe(
    segment: ArchiveSegment,
    required_watermarks: tuple[ConsumerWatermark, ...],
    *,
    protected_offsets: tuple[int, ...] = (),
    pending_job_count: int = 0,
) -> int:
    verify_archive_segment(segment)
    if not required_watermarks:
        raise ValueError("journal truncation requires explicit consumer watermarks")
    consumer_names = tuple(row.consumer_name for row in required_watermarks)
    if len(consumer_names) != len(set(consumer_names)):
        raise ValueError("journal truncation consumer watermarks must be unique")
    if pending_job_count < 0:
        raise ValueError("pending job count cannot be negative")
    if pending_job_count:
        raise ValueError("pending materialization jobs block journal truncation")
    for watermark in required_watermarks:
        if (
            watermark.journal_generation != segment.journal_generation
            or watermark.shard_id != segment.shard_id
        ):
            raise ValueError("consumer watermark lineage differs from archive segment")
        if watermark.committed_offset < segment.last_offset:
            raise ValueError("consumer watermark has not cleared the archive segment")
    if any(segment.first_offset <= offset <= segment.last_offset for offset in protected_offsets):
        raise ValueError("protected evidence offset blocks journal truncation")
    return segment.last_offset