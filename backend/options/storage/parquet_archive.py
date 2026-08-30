from __future__ import annotations

import hashlib
import os
import re
import threading
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from options.domain import (
    ManifestCreationStatus,
    OptionTradeEvent,
    RawFileEventType,
    RawFileManifest,
)
from options.repositories.retention import OptionRetentionRepository


ET = ZoneInfo("America/New_York")
DECIMAL_SCALE = Decimal("0.00000001")
_UUID_TEXT = r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
_ARCHIVE_FILENAME = re.compile(
    rf"^part-(?P<writer>{_UUID_TEXT})-(?P<sequence>[0-9]{{8}})-(?P<file>{_UUID_TEXT})\.parquet$",
    re.IGNORECASE,
)

TRADE_SCHEMA = pa.schema(
    [
        pa.field("trade_event_id", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=False),
        pa.field("contract_id", pa.int64(), nullable=False),
        pa.field("contract_ticker", pa.string(), nullable=False),
        pa.field("underlying", pa.string(), nullable=False),
        pa.field("sip_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("sequence_number", pa.int64(), nullable=False),
        pa.field("participant_timestamp", pa.timestamp("ns", tz="UTC")),
        pa.field("first_observed_at", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("revised_observed_at", pa.timestamp("ns", tz="UTC")),
        pa.field("exchange", pa.int32()),
        pa.field(
            "conditions",
            pa.list_(pa.field("element", pa.int32(), nullable=True)),
            nullable=False,
        ),
        pa.field("correction", pa.int32()),
        pa.field("provider_trade_id", pa.string()),
        pa.field("price", pa.decimal128(20, 8), nullable=False),
        pa.field("size", pa.int64(), nullable=False),
        pa.field("shares_per_contract", pa.int32(), nullable=False),
        pa.field("notional", pa.decimal128(24, 8), nullable=False),
        pa.field("payload_sha256", pa.string(), nullable=False),
        pa.field("raw_batch_id", pa.string(), nullable=False),
        pa.field("classification_status", pa.string(), nullable=False),
    ],
    metadata={
        b"schema_name": b"option_trade_event",
        b"schema_version": b"1",
    },
)


class ArchiveBackpressure(RuntimeError):
    """Raised when required archive evidence cannot enter the bounded queue."""


class ArchiveValidationError(RuntimeError):
    """Raised when a partial Parquet file fails validation before publication."""


@dataclass(frozen=True, slots=True)
class ArchiveQueueItem:
    event: OptionTradeEvent
    estimated_bytes: int


@dataclass(frozen=True, slots=True)
class ArchivePartition:
    market_date: date
    underlyer: str
    market_hour: int


@dataclass(frozen=True, slots=True)
class ArchiveReconciliationReport:
    stale_partials_removed: tuple[str, ...]
    recent_partials_retained: tuple[str, ...]
    orphan_files_adopted: tuple[str, ...]
    corrupt_orphan_files: tuple[str, ...]
    missing_manifest_files: tuple[str, ...]
    corrupt_manifest_files: tuple[str, ...]


class RawMarketArchive(ABC):
    @abstractmethod
    def write_trades(
        self,
        events: tuple[OptionTradeEvent, ...],
    ) -> tuple[RawFileManifest, ...]:
        raise NotImplementedError


class BoundedTradeArchiveQueue:
    def __init__(self, maximum_items: int, maximum_bytes: int) -> None:
        if maximum_items <= 0 or maximum_bytes <= 0:
            raise ValueError("archive queue limits must be positive")
        self.maximum_items = maximum_items
        self.maximum_bytes = maximum_bytes
        self._items: deque[ArchiveQueueItem] = deque()
        self._bytes = 0
        self._condition = threading.Condition()

    @property
    def item_count(self) -> int:
        with self._condition:
            return len(self._items)

    @property
    def byte_count(self) -> int:
        with self._condition:
            return self._bytes

    def put(self, event: OptionTradeEvent, *, required: bool) -> bool:
        estimated_bytes = estimate_trade_event_bytes(event)
        with self._condition:
            fits = (
                len(self._items) < self.maximum_items
                and self._bytes + estimated_bytes <= self.maximum_bytes
            )
            if not fits:
                if required:
                    raise ArchiveBackpressure(
                        "required archive evidence exceeded bounded queue capacity"
                    )
                return False
            self._items.append(ArchiveQueueItem(event, estimated_bytes))
            self._bytes += estimated_bytes
            self._condition.notify()
            return True

    def get(self) -> OptionTradeEvent:
        with self._condition:
            if not self._items:
                raise IndexError("archive queue is empty")
            item = self._items.popleft()
            self._bytes -= item.estimated_bytes
            return item.event


class ParquetRawMarketArchive(RawMarketArchive):
    def __init__(
        self,
        root: Path,
        manifest_repository: OptionRetentionRepository,
        *,
        writer_instance_id: UUID | None = None,
    ) -> None:
        self.root = root.resolve()
        self.manifest_repository = manifest_repository
        self.writer_instance_id = writer_instance_id or uuid4()
        self._sequence = 0
        self._sequence_lock = threading.Lock()

    def write_trades(
        self,
        events: tuple[OptionTradeEvent, ...],
    ) -> tuple[RawFileManifest, ...]:
        if not events:
            return ()
        grouped: dict[ArchivePartition, list[OptionTradeEvent]] = {}
        for event in events:
            grouped.setdefault(_partition(event), []).append(event)
        manifests = []
        for partition in sorted(
            grouped,
            key=lambda value: (value.market_date, value.underlyer, value.market_hour),
        ):
            partition_events = tuple(
                sorted(
                    grouped[partition],
                    key=lambda event: (
                        event.sip_timestamp,
                        event.sequence_number,
                        event.trade_event_id,
                    ),
                )
            )
            manifests.append(self._write_trade_partition(partition, partition_events))
        return tuple(manifests)

    def _write_trade_partition(
        self,
        partition: ArchivePartition,
        events: tuple[OptionTradeEvent, ...],
    ) -> RawFileManifest:
        file_id = uuid4()
        sequence = self._next_sequence()
        directory = (
            self.root
            / "event_type=trades"
            / f"market_date={partition.market_date.isoformat()}"
            / f"underlying={partition.underlyer}"
            / f"hour={partition.market_hour:02d}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        filename = (
            f"part-{self.writer_instance_id}-{sequence:08d}-{file_id}.parquet"
        )
        final_path = directory / filename
        partial_path = final_path.with_suffix(final_path.suffix + ".partial")
        table = pa.Table.from_pylist(
            [_trade_record(event) for event in events],
            schema=TRADE_SCHEMA,
        )
        pq.write_table(
            table,
            partial_path,
            compression="zstd",
            version="2.6",
            write_statistics=True,
        )
        self._validate_partial(partial_path, events)
        payload_sha256 = _file_sha256(partial_path)
        byte_size = partial_path.stat().st_size
        os.replace(partial_path, final_path)
        manifest = RawFileManifest(
            file_id=file_id,
            event_type=RawFileEventType.TRADE,
            market_date=partition.market_date,
            underlyer=partition.underlyer,
            market_hour=partition.market_hour,
            object_key=final_path.relative_to(self.root).as_posix(),
            schema_version=1,
            row_count=len(events),
            minimum_source_time=events[0].sip_timestamp,
            maximum_source_time=events[-1].sip_timestamp,
            byte_size=byte_size,
            payload_sha256=payload_sha256,
            creation_status=ManifestCreationStatus.COMPLETE,
            retention_class="RAW_30_DAY",
        )
        if not self.manifest_repository.record_completed_manifest(manifest):
            raise RuntimeError("published Parquet object key already has a manifest")
        return manifest

    @staticmethod
    def _validate_partial(
        partial_path: Path,
        events: tuple[OptionTradeEvent, ...],
    ) -> None:
        parquet_file = pq.ParquetFile(partial_path)
        if not parquet_file.schema_arrow.equals(TRADE_SCHEMA, check_metadata=True):
            raise ArchiveValidationError("Parquet trade schema does not match version 1")
        if parquet_file.metadata.num_rows != len(events):
            raise ArchiveValidationError("Parquet trade row count does not match input")
        timestamp_table = parquet_file.read(
            columns=["sip_timestamp", "first_observed_at", "revised_observed_at"]
        )
        _validate_observation_times(timestamp_table)
        timestamps = timestamp_table.column("sip_timestamp").to_pylist()
        if not timestamps:
            raise ArchiveValidationError("Parquet trade file has no source timestamps")
        if min(timestamps) != events[0].sip_timestamp:
            raise ArchiveValidationError("Parquet minimum source time does not match input")
        if max(timestamps) != events[-1].sip_timestamp:
            raise ArchiveValidationError("Parquet maximum source time does not match input")

    def _next_sequence(self) -> int:
        with self._sequence_lock:
            self._sequence += 1
            return self._sequence


class BufferedTradeArchive:
    def __init__(
        self,
        archive: RawMarketArchive,
        *,
        maximum_rows: int = 100_000,
        maximum_age_seconds: float = 60.0,
        monotonic: Callable[[], float],
    ) -> None:
        if maximum_rows <= 0 or maximum_age_seconds <= 0:
            raise ValueError("archive buffer thresholds must be positive")
        self.archive = archive
        self.maximum_rows = maximum_rows
        self.maximum_age_seconds = maximum_age_seconds
        self.monotonic = monotonic
        self._events: dict[ArchivePartition, list[OptionTradeEvent]] = {}
        self._started_at: dict[ArchivePartition, float] = {}

    def add(self, event: OptionTradeEvent) -> tuple[RawFileManifest, ...]:
        partition = _partition(event)
        boundary_partitions = [
            existing
            for existing in self._events
            if existing.underlyer == partition.underlyer and existing != partition
        ]
        manifests = list(self._flush_partitions(boundary_partitions))
        self._events.setdefault(partition, []).append(event)
        self._started_at.setdefault(partition, self.monotonic())
        if len(self._events[partition]) >= self.maximum_rows:
            manifests.extend(self._flush_partition(partition))
        return tuple(manifests)

    def flush_due(self) -> tuple[RawFileManifest, ...]:
        now = self.monotonic()
        due = [
            partition
            for partition, started_at in self._started_at.items()
            if now - started_at >= self.maximum_age_seconds
        ]
        return self._flush_partitions(due)

    def flush_all(self) -> tuple[RawFileManifest, ...]:
        return self._flush_partitions(list(self._events))

    def _flush_partitions(
        self,
        partitions: list[ArchivePartition],
    ) -> tuple[RawFileManifest, ...]:
        manifests: list[RawFileManifest] = []
        for partition in sorted(
            partitions,
            key=lambda value: (value.market_date, value.underlyer, value.market_hour),
        ):
            manifests.extend(self._flush_partition(partition))
        return tuple(manifests)

    def _flush_partition(
        self,
        partition: ArchivePartition,
    ) -> tuple[RawFileManifest, ...]:
        events = tuple(self._events.pop(partition, []))
        self._started_at.pop(partition, None)
        return self.archive.write_trades(events)


class ArchiveReconciler:
    def __init__(
        self,
        root: Path,
        manifest_repository: OptionRetentionRepository,
        *,
        partial_grace_period: timedelta = timedelta(minutes=10),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if partial_grace_period.total_seconds() <= 0:
            raise ValueError("partial grace period must be positive")
        self.root = root.resolve()
        self.manifest_repository = manifest_repository
        self.partial_grace_period = partial_grace_period
        self.clock = clock

    def reconcile(self) -> ArchiveReconciliationReport:
        now = self.clock().astimezone(timezone.utc)
        removed_partials: list[str] = []
        retained_partials: list[str] = []
        adopted_orphans: list[str] = []
        corrupt_orphans: list[str] = []
        missing_manifests: list[str] = []
        corrupt_manifests: list[str] = []

        for partial_path in sorted(self.root.rglob("*.partial")):
            modified_at = datetime.fromtimestamp(
                partial_path.stat().st_mtime,
                tz=timezone.utc,
            )
            object_key = partial_path.relative_to(self.root).as_posix()
            if now - modified_at >= self.partial_grace_period:
                partial_path.unlink()
                removed_partials.append(object_key)
            else:
                retained_partials.append(object_key)

        known = {
            manifest.object_key: manifest
            for manifest in self.manifest_repository.list_non_deleted_manifests()
        }
        seen: set[str] = set()
        for final_path in sorted(self.root.rglob("*.parquet")):
            object_key = final_path.relative_to(self.root).as_posix()
            seen.add(object_key)
            manifest = known.get(object_key)
            try:
                inspected = _inspect_trade_file(self.root, final_path)
            except (ArchiveValidationError, OSError, ValueError, pa.ArrowException):
                if manifest is None:
                    corrupt_orphans.append(object_key)
                else:
                    self.manifest_repository.mark_integrity_failure(
                        manifest.file_id,
                        ManifestCreationStatus.CORRUPT,
                    )
                    corrupt_manifests.append(object_key)
                continue
            if manifest is None:
                if not self.manifest_repository.record_completed_manifest(inspected):
                    concurrent = self.manifest_repository.get_manifest_by_object_key(
                        object_key
                    )
                    if concurrent is None or not _manifest_matches(concurrent, inspected):
                        raise RuntimeError("orphan manifest adoption conflicted")
                adopted_orphans.append(object_key)
            elif not _manifest_matches(manifest, inspected):
                self.manifest_repository.mark_integrity_failure(
                    manifest.file_id,
                    ManifestCreationStatus.CORRUPT,
                )
                corrupt_manifests.append(object_key)

        for object_key, manifest in known.items():
            if object_key not in seen:
                self.manifest_repository.mark_integrity_failure(
                    manifest.file_id,
                    ManifestCreationStatus.MISSING,
                )
                missing_manifests.append(object_key)

        return ArchiveReconciliationReport(
            stale_partials_removed=tuple(removed_partials),
            recent_partials_retained=tuple(retained_partials),
            orphan_files_adopted=tuple(adopted_orphans),
            corrupt_orphan_files=tuple(corrupt_orphans),
            missing_manifest_files=tuple(missing_manifests),
            corrupt_manifest_files=tuple(corrupt_manifests),
        )


def estimate_trade_event_bytes(event: OptionTradeEvent) -> int:
    return (
        256
        + len(event.provider.encode("utf-8"))
        + len(event.contract_ticker.encode("utf-8"))
        + len(event.underlyer.encode("utf-8"))
        + len(event.payload_sha256)
        + len(event.conditions) * 4
        + (len(event.provider_trade_id.encode("utf-8")) if event.provider_trade_id else 0)
    )


def _trade_record(event: OptionTradeEvent) -> dict[str, object]:
    return {
        "trade_event_id": str(event.trade_event_id),
        "provider": event.provider,
        "contract_id": event.contract_id,
        "contract_ticker": event.contract_ticker,
        "underlying": event.underlyer,
        "sip_timestamp": event.sip_timestamp,
        "sequence_number": event.sequence_number,
        "participant_timestamp": event.participant_timestamp,
        "first_observed_at": event.first_observed_at,
        "revised_observed_at": event.revised_observed_at,
        "exchange": event.exchange,
        "conditions": list(event.conditions),
        "correction": event.correction,
        "provider_trade_id": event.provider_trade_id,
        "price": _fixed_decimal(event.price, "price"),
        "size": event.size,
        "shares_per_contract": event.shares_per_contract,
        "notional": _fixed_decimal(event.notional, "notional"),
        "payload_sha256": event.payload_sha256,
        "raw_batch_id": str(event.raw_batch_id),
        "classification_status": event.classification_status.value,
    }


def _fixed_decimal(value: Decimal, name: str) -> Decimal:
    fixed = value.quantize(DECIMAL_SCALE)
    if fixed != value:
        raise ValueError(f"{name} exceeds the archive's 8-decimal fixed precision")
    return fixed


def _partition(event: OptionTradeEvent) -> ArchivePartition:
    source_time_et = event.sip_timestamp.astimezone(ET)
    return ArchivePartition(
        market_date=source_time_et.date(),
        underlyer=event.underlyer,
        market_hour=source_time_et.hour,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inspect_trade_file(root: Path, path: Path) -> RawFileManifest:
    object_key = path.relative_to(root).as_posix()
    parts = dict(
        part.split("=", 1)
        for part in path.relative_to(root).parts[:-1]
        if "=" in part
    )
    if parts.get("event_type") != "trades":
        raise ArchiveValidationError("unsupported raw archive event type")
    filename_match = _ARCHIVE_FILENAME.fullmatch(path.name)
    if filename_match is None:
        raise ArchiveValidationError("raw archive filename is malformed")
    try:
        UUID(filename_match.group("writer"))
        if int(filename_match.group("sequence")) <= 0:
            raise ValueError("sequence must be positive")
        file_id = UUID(filename_match.group("file"))
        market_date = date.fromisoformat(parts["market_date"])
        underlyer = parts["underlying"]
        market_hour = int(parts["hour"])
    except (KeyError, ValueError) as exc:
        raise ArchiveValidationError("raw archive path is malformed") from exc
    parquet_file = pq.ParquetFile(path)
    if not parquet_file.schema_arrow.equals(TRADE_SCHEMA, check_metadata=True):
        raise ArchiveValidationError("Parquet trade schema does not match version 1")
    timestamp_table = parquet_file.read(
        columns=["sip_timestamp", "first_observed_at", "revised_observed_at"]
    )
    _validate_observation_times(timestamp_table)
    timestamps = timestamp_table.column("sip_timestamp").to_pylist()
    if not timestamps:
        raise ArchiveValidationError("Parquet trade file has no source timestamps")
    return RawFileManifest(
        file_id=file_id,
        event_type=RawFileEventType.TRADE,
        market_date=market_date,
        underlyer=underlyer,
        market_hour=market_hour,
        object_key=object_key,
        schema_version=1,
        row_count=parquet_file.metadata.num_rows,
        minimum_source_time=min(timestamps),
        maximum_source_time=max(timestamps),
        byte_size=path.stat().st_size,
        payload_sha256=_file_sha256(path),
        creation_status=ManifestCreationStatus.COMPLETE,
        retention_class="RAW_30_DAY",
    )


def _manifest_matches(expected: RawFileManifest, actual: RawFileManifest) -> bool:
    return (
        expected.event_type == actual.event_type
        and expected.market_date == actual.market_date
        and expected.underlyer == actual.underlyer
        and expected.market_hour == actual.market_hour
        and expected.schema_version == actual.schema_version
        and expected.row_count == actual.row_count
        and expected.minimum_source_time == actual.minimum_source_time
        and expected.maximum_source_time == actual.maximum_source_time
        and expected.byte_size == actual.byte_size
        and expected.payload_sha256 == actual.payload_sha256
    )


def _validate_observation_times(table: pa.Table) -> None:
    sip_times = table.column("sip_timestamp").to_pylist()
    first_observed = table.column("first_observed_at").to_pylist()
    revised_observed = table.column("revised_observed_at").to_pylist()
    for sip_time, first_observed_at, revised_observed_at in zip(
        sip_times,
        first_observed,
        revised_observed,
    ):
        if first_observed_at < sip_time:
            raise ArchiveValidationError(
                "Parquet first observation time precedes SIP source time"
            )
        if revised_observed_at is not None and revised_observed_at < first_observed_at:
            raise ArchiveValidationError(
                "Parquet revised observation time precedes first observation time"
            )