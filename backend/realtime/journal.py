from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Iterable
from uuid import UUID, uuid4

from .contracts import AssetClass, MarketChannel, MarketFeed, RawMessageEnvelope


if TYPE_CHECKING:
    from .normalizer import NormalizationBatch


JOURNAL_SCHEMA_VERSION = 1
JOURNAL_CODEC = "sqlite-wal-raw-v1"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("journal timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


@dataclass(frozen=True, slots=True)
class JournalCommit:
    generation: UUID
    first_offset: int
    last_offset: int
    inserted_count: int
    duplicate_count: int
    payload_bytes: int
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class RawJournalRecord:
    raw_offset: int
    generation: UUID
    stream_session_id: UUID
    message_number: int
    feed: MarketFeed
    received_at: datetime
    received_monotonic_ns: int
    payload: bytes
    payload_sha256: str
    schema_version: int
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class JournalHealth:
    generation: UUID
    journal_mode: str
    synchronous: int
    schema_version: int
    last_offset: int
    page_count: int
    page_size: int

    @property
    def allocated_bytes(self) -> int:
        return self.page_count * self.page_size


@dataclass(frozen=True, slots=True)
class NormalizedCommit:
    generation: UUID
    normalizer_version: str
    committed_raw_offset: int
    committed_event_offset: int
    event_count: int
    quarantine_count: int
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class DecoderProgress:
    generation: UUID
    normalizer_version: str
    committed_raw_offset: int
    committed_event_offset: int
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class NormalizedJournalRecord:
    event_offset: int
    raw_offset: int
    event_index: int
    normalizer_version: str
    journal_generation: UUID
    stream_session_id: UUID
    instrument_id: int
    provider_symbol: str
    underlying: str
    asset_class: AssetClass
    shard_id: int
    event_kind: MarketChannel
    source_session: date
    source_sequence: int | None
    source_timestamp: int
    source_timestamp_unit: str
    event_at: datetime
    price: Decimal | None
    size: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    bid_size: int | None
    ask_size: int | None
    exchange: int | None
    conditions: tuple[int, ...]
    quality: str
    reason_codes: tuple[str, ...]
    details_json: str
    payload_sha256: str
    received_at: datetime
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class QuarantineJournalRecord:
    raw_offset: int
    event_index: int
    normalizer_version: str
    reason_code: str
    payload_sha256: str
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class JournalRecoveryState:
    generation: UUID
    schema_version: int
    raw_committed_offset: int
    normalized_committed_raw_offset: int
    normalized_committed_event_offset: int
    quarantine_count: int
    integrity_check: str


def journal_files_sha256(path: str | Path) -> str:
    journal = Path(path)
    if not journal.is_file():
        raise FileNotFoundError(journal)
    digest = hashlib.sha256()
    for suffix in ("", "-wal"):
        candidate = Path(str(journal) + suffix)
        if not candidate.is_file():
            continue
        digest.update(suffix.encode("ascii"))
        digest.update(candidate.stat().st_size.to_bytes(8, "big"))
        with candidate.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def _connect(path: Path, *, timeout_seconds: float, read_only: bool) -> sqlite3.Connection:
    if read_only:
        connection = sqlite3.connect(
            path.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=timeout_seconds,
        )
        connection.execute("PRAGMA query_only=ON")
    else:
        connection = sqlite3.connect(path, timeout=timeout_seconds)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(f"PRAGMA busy_timeout={int(timeout_seconds * 1000)}")
    return connection


class RawJournalWriter:
    def __init__(
        self,
        path: str | Path,
        *,
        generation: UUID | None = None,
        timeout_seconds: float = 5.0,
        clock=_utc_now,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.parent.is_dir():
            raise ValueError("journal parent must be a local directory")
        self._owner_thread = threading.get_ident()
        self._clock = clock
        self._connection = _connect(
            self.path,
            timeout_seconds=timeout_seconds,
            read_only=False,
        )
        mode = self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            self._connection.close()
            raise RuntimeError("SQLite journal_mode WAL is required")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA wal_autocheckpoint=1000")
        self._initialize(generation)

    def _assert_owner(self) -> None:
        if threading.get_ident() != self._owner_thread:
            raise RuntimeError("raw journal writer may only be used by its owner thread")

    def _initialize(self, requested_generation: UUID | None) -> None:
        self._connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS journal_metadata (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                journal_generation TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                CHECK (schema_version > 0)
            );
            CREATE TABLE IF NOT EXISTS raw_messages (
                raw_offset INTEGER PRIMARY KEY AUTOINCREMENT,
                journal_generation TEXT NOT NULL,
                stream_session_id TEXT NOT NULL,
                message_number INTEGER NOT NULL,
                feed TEXT NOT NULL,
                received_at TEXT NOT NULL,
                received_monotonic_ns INTEGER NOT NULL,
                byte_length INTEGER NOT NULL,
                payload BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                committed_at TEXT NOT NULL,
                UNIQUE (stream_session_id, message_number),
                CHECK (message_number >= 0),
                CHECK (feed IN ('STOCKS', 'OPTIONS')),
                CHECK (received_monotonic_ns >= 0),
                CHECK (byte_length > 0 AND byte_length = length(payload)),
                CHECK (length(payload_sha256) = 64),
                CHECK (schema_version > 0)
            );
            CREATE TABLE IF NOT EXISTS normalized_events (
                event_offset INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_offset INTEGER NOT NULL REFERENCES raw_messages(raw_offset),
                event_index INTEGER NOT NULL,
                normalizer_version TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                provider_symbol TEXT NOT NULL,
                underlying TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                shard_id INTEGER NOT NULL,
                event_kind TEXT NOT NULL,
                source_session TEXT NOT NULL,
                source_sequence INTEGER,
                source_timestamp INTEGER NOT NULL,
                source_timestamp_unit TEXT NOT NULL,
                event_at TEXT NOT NULL,
                price_text TEXT,
                size_text TEXT,
                bid_text TEXT,
                ask_text TEXT,
                bid_size INTEGER,
                ask_size INTEGER,
                exchange INTEGER,
                conditions_json TEXT NOT NULL,
                quality TEXT NOT NULL,
                reason_codes_json TEXT NOT NULL,
                details_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                committed_at TEXT NOT NULL,
                UNIQUE (raw_offset, event_index, normalizer_version),
                CHECK (event_index >= 0),
                CHECK (instrument_id > 0),
                CHECK (asset_class IN ('EQUITY', 'OPTION')),
                CHECK (shard_id >= 0),
                CHECK (event_kind IN ('T', 'Q', 'AM')),
                CHECK (source_timestamp_unit = 'ms'),
                CHECK (length(payload_sha256) = 64),
                CHECK (json_valid(conditions_json)),
                CHECK (json_valid(reason_codes_json)),
                CHECK (json_valid(details_json))
            );
            CREATE INDEX IF NOT EXISTS idx_normalized_event_shard_offset
                ON normalized_events (shard_id, event_offset);
            CREATE INDEX IF NOT EXISTS idx_normalized_event_instrument_time
                ON normalized_events (instrument_id, source_timestamp, event_offset);
            CREATE TABLE IF NOT EXISTS quarantined_events (
                raw_offset INTEGER NOT NULL REFERENCES raw_messages(raw_offset),
                event_index INTEGER NOT NULL,
                normalizer_version TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                committed_at TEXT NOT NULL,
                PRIMARY KEY (raw_offset, event_index, normalizer_version),
                CHECK (event_index >= -1),
                CHECK (trim(reason_code) <> ''),
                CHECK (length(payload_sha256) = 64)
            );
            CREATE TABLE IF NOT EXISTS decoder_progress (
                normalizer_version TEXT PRIMARY KEY,
                journal_generation TEXT NOT NULL,
                committed_raw_offset INTEGER NOT NULL,
                committed_event_offset INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK (committed_raw_offset >= 0),
                CHECK (committed_event_offset >= 0)
            );
            COMMIT;
            """
        )
        row = self._connection.execute(
            "SELECT journal_generation, schema_version "
            "FROM journal_metadata WHERE singleton = 1"
        ).fetchone()
        if row is None:
            generation = requested_generation or uuid4()
            with self._connection:
                self._connection.execute(
                    "INSERT INTO journal_metadata "
                    "(singleton, journal_generation, schema_version, created_at) "
                    "VALUES (1, ?, ?, ?)",
                    (
                        str(generation),
                        JOURNAL_SCHEMA_VERSION,
                        _stamp(self._clock()),
                    ),
                )
        else:
            generation = UUID(row["journal_generation"])
            if row["schema_version"] != JOURNAL_SCHEMA_VERSION:
                self._connection.close()
                raise RuntimeError("unsupported raw journal schema version")
            if requested_generation is not None and requested_generation != generation:
                self._connection.close()
                raise RuntimeError("journal generation does not match the existing file")
        self.generation = generation

    def append_batch(
        self, messages: Iterable[RawMessageEnvelope]
    ) -> JournalCommit:
        self._assert_owner()
        batch = tuple(messages)
        if not batch:
            raise ValueError("raw journal append batch cannot be empty")
        identities = tuple(
            (message.stream_session_id, message.message_number) for message in batch
        )
        if len(identities) != len(set(identities)):
            raise ValueError("raw journal append batch contains duplicate identities")
        committed_at = self._clock()
        committed_stamp = _stamp(committed_at)
        offsets: list[int] = []
        inserted_count = 0
        duplicate_count = 0
        payload_bytes = 0
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            for message in batch:
                digest = hashlib.sha256(message.payload).hexdigest()
                existing = self._connection.execute(
                    "SELECT raw_offset, feed, byte_length, payload_sha256, schema_version "
                    "FROM raw_messages "
                    "WHERE stream_session_id = ? AND message_number = ?",
                    (str(message.stream_session_id), message.message_number),
                ).fetchone()
                if existing is not None:
                    expected = (
                        message.feed.value,
                        message.byte_length,
                        digest,
                        message.schema_version,
                    )
                    observed = (
                        existing["feed"],
                        existing["byte_length"],
                        existing["payload_sha256"],
                        existing["schema_version"],
                    )
                    if observed != expected:
                        raise ValueError(
                            "raw message identity conflicts with committed payload"
                        )
                    offsets.append(existing["raw_offset"])
                    duplicate_count += 1
                    payload_bytes += message.byte_length
                    continue
                cursor = self._connection.execute(
                    "INSERT INTO raw_messages ("
                    "journal_generation, stream_session_id, message_number, feed, "
                    "received_at, received_monotonic_ns, byte_length, payload, "
                    "payload_sha256, schema_version, committed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        str(self.generation),
                        str(message.stream_session_id),
                        message.message_number,
                        message.feed.value,
                        _stamp(message.received_at),
                        message.received_monotonic_ns,
                        message.byte_length,
                        sqlite3.Binary(message.payload),
                        digest,
                        message.schema_version,
                        committed_stamp,
                    ),
                )
                offsets.append(cursor.lastrowid)
                inserted_count += 1
                payload_bytes += message.byte_length
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        return JournalCommit(
            generation=self.generation,
            first_offset=min(offsets),
            last_offset=max(offsets),
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
            payload_bytes=payload_bytes,
            committed_at=committed_at,
        )

    def health(self) -> JournalHealth:
        self._assert_owner()
        mode = str(
            self._connection.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        synchronous = int(
            self._connection.execute("PRAGMA synchronous").fetchone()[0]
        )
        page_count = int(
            self._connection.execute("PRAGMA page_count").fetchone()[0]
        )
        page_size = int(self._connection.execute("PRAGMA page_size").fetchone()[0])
        last_offset = int(
            self._connection.execute(
                "SELECT COALESCE(MAX(raw_offset), 0) FROM raw_messages"
            ).fetchone()[0]
        )
        return JournalHealth(
            generation=self.generation,
            journal_mode=mode,
            synchronous=synchronous,
            schema_version=JOURNAL_SCHEMA_VERSION,
            last_offset=last_offset,
            page_count=page_count,
            page_size=page_size,
        )

    def commit_normalized(self, batch: NormalizationBatch) -> NormalizedCommit:
        self._assert_owner()
        version = batch.normalizer_version.strip()
        if not version:
            raise ValueError("normalizer_version cannot be blank")
        if batch.raw_offset <= 0:
            raise ValueError("normalized batch raw_offset must be positive")
        event_indices = [event.event_index for event in batch.events]
        quarantine_indices = [row.event_index for row in batch.quarantines]
        all_indices = event_indices + quarantine_indices
        if len(all_indices) != len(set(all_indices)):
            raise ValueError("normalized batch event indexes must be unique")
        if any(
            event.raw_offset != batch.raw_offset
            or event.normalizer_version != version
            for event in batch.events
        ) or any(
            row.raw_offset != batch.raw_offset
            or row.normalizer_version != version
            for row in batch.quarantines
        ):
            raise ValueError("normalized batch lineage does not match its events")
        raw = self._connection.execute(
            "SELECT journal_generation FROM raw_messages WHERE raw_offset = ?",
            (batch.raw_offset,),
        ).fetchone()
        if raw is None or UUID(raw["journal_generation"]) != self.generation:
            raise ValueError("normalized batch has no raw lineage in this generation")
        progress = self._connection.execute(
            "SELECT committed_raw_offset, committed_event_offset "
            "FROM decoder_progress WHERE normalizer_version = ?",
            (version,),
        ).fetchone()
        if progress is None:
            first_raw = self._connection.execute(
                "SELECT MIN(raw_offset) FROM raw_messages"
            ).fetchone()[0]
            expected_raw_offset = first_raw
            committed_event_offset = 0
        else:
            expected_raw_offset = progress["committed_raw_offset"] + 1
            committed_event_offset = progress["committed_event_offset"]
        if batch.raw_offset != expected_raw_offset:
            raise ValueError(
                "normalized progress must consume the next retained raw offset"
            )
        committed_at = self._clock()
        committed_stamp = _stamp(committed_at)
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            for event in batch.events:
                cursor = self._connection.execute(
                    "INSERT INTO normalized_events ("
                    "raw_offset, event_index, normalizer_version, instrument_id, "
                    "provider_symbol, underlying, asset_class, shard_id, event_kind, "
                    "source_session, source_sequence, source_timestamp, "
                    "source_timestamp_unit, event_at, price_text, size_text, bid_text, "
                    "ask_text, bid_size, ask_size, exchange, conditions_json, quality, "
                    "reason_codes_json, details_json, payload_sha256, committed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.raw_offset,
                        event.event_index,
                        event.normalizer_version,
                        event.instrument_id,
                        event.provider_symbol,
                        event.underlying,
                        event.asset_class.value,
                        event.shard_id,
                        event.event_kind.value,
                        event.source_session.isoformat(),
                        event.source_sequence,
                        event.source_timestamp,
                        event.source_timestamp_unit,
                        _stamp(event.event_at),
                        str(event.price) if event.price is not None else None,
                        str(event.size) if event.size is not None else None,
                        str(event.bid) if event.bid is not None else None,
                        str(event.ask) if event.ask is not None else None,
                        event.bid_size,
                        event.ask_size,
                        event.exchange,
                        json.dumps(event.conditions, separators=(",", ":")),
                        event.quality.value,
                        json.dumps(event.reason_codes, separators=(",", ":")),
                        event.details_json,
                        event.payload_sha256,
                        committed_stamp,
                    ),
                )
                committed_event_offset = cursor.lastrowid
            for row in batch.quarantines:
                self._connection.execute(
                    "INSERT INTO quarantined_events ("
                    "raw_offset, event_index, normalizer_version, reason_code, "
                    "payload_sha256, committed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        row.raw_offset,
                        row.event_index,
                        row.normalizer_version,
                        row.reason_code,
                        row.payload_sha256,
                        committed_stamp,
                    ),
                )
            self._connection.execute(
                "INSERT INTO decoder_progress ("
                "normalizer_version, journal_generation, committed_raw_offset, "
                "committed_event_offset, updated_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(normalizer_version) DO UPDATE SET "
                "journal_generation = excluded.journal_generation, "
                "committed_raw_offset = excluded.committed_raw_offset, "
                "committed_event_offset = excluded.committed_event_offset, "
                "updated_at = excluded.updated_at",
                (
                    version,
                    str(self.generation),
                    batch.raw_offset,
                    committed_event_offset,
                    committed_stamp,
                ),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        return NormalizedCommit(
            generation=self.generation,
            normalizer_version=version,
            committed_raw_offset=batch.raw_offset,
            committed_event_offset=committed_event_offset,
            event_count=len(batch.events),
            quarantine_count=len(batch.quarantines),
            committed_at=committed_at,
        )

    def close(self) -> None:
        self._assert_owner()
        self._connection.close()

    def __enter__(self) -> RawJournalWriter:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class RawJournalReader:
    def __init__(self, path: str | Path, *, timeout_seconds: float = 5.0) -> None:
        self.path = Path(path)
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        self.timeout_seconds = timeout_seconds

    def read_after(self, raw_offset: int, *, limit: int = 1000) -> tuple[RawJournalRecord, ...]:
        if type(raw_offset) is not int or raw_offset < 0:
            raise ValueError("raw_offset must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("limit must be in [1, 10000]")
        with closing(
            _connect(
                self.path,
                timeout_seconds=self.timeout_seconds,
                read_only=True,
            )
        ) as connection:
            rows = connection.execute(
                "SELECT raw_offset, journal_generation, stream_session_id, "
                "message_number, feed, received_at, received_monotonic_ns, payload, "
                "payload_sha256, schema_version, committed_at "
                "FROM raw_messages WHERE raw_offset > ? "
                "ORDER BY raw_offset LIMIT ?",
                (raw_offset, limit),
            ).fetchall()
        records = tuple(
            RawJournalRecord(
                raw_offset=row["raw_offset"],
                generation=UUID(row["journal_generation"]),
                stream_session_id=UUID(row["stream_session_id"]),
                message_number=row["message_number"],
                feed=MarketFeed(row["feed"]),
                received_at=_parse_stamp(row["received_at"]),
                received_monotonic_ns=row["received_monotonic_ns"],
                payload=bytes(row["payload"]),
                payload_sha256=row["payload_sha256"],
                schema_version=row["schema_version"],
                committed_at=_parse_stamp(row["committed_at"]),
            )
            for row in rows
        )
        for record in records:
            if hashlib.sha256(record.payload).hexdigest() != record.payload_sha256:
                raise ValueError("raw journal payload checksum mismatch")
        return records

    def decoder_progress(self, normalizer_version: str) -> DecoderProgress | None:
        version = normalizer_version.strip()
        if not version:
            raise ValueError("normalizer_version cannot be blank")
        with closing(
            _connect(
                self.path,
                timeout_seconds=self.timeout_seconds,
                read_only=True,
            )
        ) as connection:
            row = connection.execute(
                "SELECT journal_generation, normalizer_version, "
                "committed_raw_offset, committed_event_offset, updated_at "
                "FROM decoder_progress WHERE normalizer_version = ?",
                (version,),
            ).fetchone()
        if row is None:
            return None
        return DecoderProgress(
            generation=UUID(row["journal_generation"]),
            normalizer_version=row["normalizer_version"],
            committed_raw_offset=row["committed_raw_offset"],
            committed_event_offset=row["committed_event_offset"],
            updated_at=_parse_stamp(row["updated_at"]),
        )

    def recovery_state(
        self,
        normalizer_version: str,
    ) -> JournalRecoveryState:
        version = normalizer_version.strip()
        if not version:
            raise ValueError("normalizer_version cannot be blank")
        with closing(
            _connect(
                self.path,
                timeout_seconds=self.timeout_seconds,
                read_only=True,
            )
        ) as connection:
            integrity = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            metadata = connection.execute(
                "SELECT journal_generation,schema_version "
                "FROM journal_metadata WHERE singleton=1"
            ).fetchone()
            if metadata is None:
                raise ValueError("raw journal metadata is unavailable")
            raw_offset = int(
                connection.execute(
                    "SELECT COALESCE(MAX(raw_offset),0) FROM raw_messages"
                ).fetchone()[0]
            )
            event_offset = int(
                connection.execute(
                    "SELECT COALESCE(MAX(event_offset),0) "
                    "FROM normalized_events"
                ).fetchone()[0]
            )
            progress = connection.execute(
                "SELECT committed_raw_offset,committed_event_offset "
                "FROM decoder_progress WHERE normalizer_version=?",
                (version,),
            ).fetchone()
            quarantine_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM quarantined_events"
                ).fetchone()[0]
            )
        normalized_raw = int(progress[0]) if progress is not None else 0
        normalized_event = int(progress[1]) if progress is not None else 0
        if normalized_raw > raw_offset or normalized_event > event_offset:
            raise ValueError("journal decoder progress exceeds committed rows")
        return JournalRecoveryState(
            generation=UUID(metadata["journal_generation"]),
            schema_version=int(metadata["schema_version"]),
            raw_committed_offset=raw_offset,
            normalized_committed_raw_offset=normalized_raw,
            normalized_committed_event_offset=normalized_event,
            quarantine_count=quarantine_count,
            integrity_check=integrity,
        )

    def read_normalized_after(
        self, event_offset: int, *, limit: int = 1000
    ) -> tuple[NormalizedJournalRecord, ...]:
        if type(event_offset) is not int or event_offset < 0:
            raise ValueError("event_offset must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("limit must be in [1, 10000]")
        with closing(
            _connect(
                self.path,
                timeout_seconds=self.timeout_seconds,
                read_only=True,
            )
        ) as connection:
            rows = connection.execute(
                "SELECT normalized_events.*, "
                "raw_messages.journal_generation AS raw_journal_generation, "
                "raw_messages.stream_session_id AS raw_stream_session_id, "
                "raw_messages.received_at AS raw_received_at "
                "FROM normalized_events JOIN raw_messages USING (raw_offset) "
                "WHERE event_offset > ? ORDER BY event_offset LIMIT ?",
                (event_offset, limit),
            ).fetchall()
        return tuple(
            NormalizedJournalRecord(
                event_offset=row["event_offset"],
                raw_offset=row["raw_offset"],
                event_index=row["event_index"],
                normalizer_version=row["normalizer_version"],
                journal_generation=UUID(row["raw_journal_generation"]),
                stream_session_id=UUID(row["raw_stream_session_id"]),
                instrument_id=row["instrument_id"],
                provider_symbol=row["provider_symbol"],
                underlying=row["underlying"],
                asset_class=AssetClass(row["asset_class"]),
                shard_id=row["shard_id"],
                event_kind=MarketChannel(row["event_kind"]),
                source_session=date.fromisoformat(row["source_session"]),
                source_sequence=row["source_sequence"],
                source_timestamp=row["source_timestamp"],
                source_timestamp_unit=row["source_timestamp_unit"],
                event_at=_parse_stamp(row["event_at"]),
                price=Decimal(row["price_text"]) if row["price_text"] else None,
                size=Decimal(row["size_text"]) if row["size_text"] else None,
                bid=Decimal(row["bid_text"]) if row["bid_text"] else None,
                ask=Decimal(row["ask_text"]) if row["ask_text"] else None,
                bid_size=row["bid_size"],
                ask_size=row["ask_size"],
                exchange=row["exchange"],
                conditions=tuple(json.loads(row["conditions_json"])),
                quality=row["quality"],
                reason_codes=tuple(json.loads(row["reason_codes_json"])),
                details_json=row["details_json"],
                payload_sha256=row["payload_sha256"],
                received_at=_parse_stamp(row["raw_received_at"]),
                committed_at=_parse_stamp(row["committed_at"]),
            )
            for row in rows
        )

    def read_quarantines(
        self, *, raw_offset: int | None = None
    ) -> tuple[QuarantineJournalRecord, ...]:
        parameters: tuple[object, ...] = ()
        predicate = ""
        if raw_offset is not None:
            if type(raw_offset) is not int or raw_offset <= 0:
                raise ValueError("raw_offset must be a positive integer")
            predicate = "WHERE raw_offset = ?"
            parameters = (raw_offset,)
        with closing(
            _connect(
                self.path,
                timeout_seconds=self.timeout_seconds,
                read_only=True,
            )
        ) as connection:
            rows = connection.execute(
                "SELECT raw_offset, event_index, normalizer_version, reason_code, "
                "payload_sha256, committed_at FROM quarantined_events "
                f"{predicate} ORDER BY raw_offset, event_index",
                parameters,
            ).fetchall()
        return tuple(
            QuarantineJournalRecord(
                raw_offset=row["raw_offset"],
                event_index=row["event_index"],
                normalizer_version=row["normalizer_version"],
                reason_code=row["reason_code"],
                payload_sha256=row["payload_sha256"],
                committed_at=_parse_stamp(row["committed_at"]),
            )
            for row in rows
        )