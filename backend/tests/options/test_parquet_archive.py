import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import OptionTradeEvent
from options.storage import (
    ArchiveReconciler,
    BoundedTradeArchiveQueue,
    BufferedTradeArchive,
    ParquetRawMarketArchive,
    TRADE_SCHEMA,
    build_raw_archive,
)
from options.config import load_option_runtime_configuration
from options.storage.parquet_archive import ArchiveBackpressure


UTC = timezone.utc


class RecordingManifestRepository:
    def __init__(self, root):
        self.root = root
        self.manifests = []
        self.failures = []

    def record_completed_manifest(self, manifest):
        assert (self.root / manifest.object_key).exists()
        self.manifests.append(manifest)
        return True

    def list_non_deleted_manifests(self):
        return tuple(self.manifests)

    def mark_integrity_failure(self, file_id, status):
        self.failures.append((file_id, status))

    def get_manifest_by_object_key(self, object_key):
        return next(
            (manifest for manifest in self.manifests if manifest.object_key == object_key),
            None,
        )


class RecordingArchive:
    def __init__(self):
        self.batches = []

    def write_trades(self, events):
        self.batches.append(events)
        return ()


def _event(sequence=1, seconds=0):
    sip_timestamp = datetime(2026, 8, 29, 14, 30, tzinfo=UTC) + timedelta(seconds=seconds)
    return OptionTradeEvent(
        trade_event_id=uuid4(),
        provider="polygon",
        contract_id=42,
        contract_ticker="O:SPY260904C00650000",
        underlyer="SPY",
        sip_timestamp=sip_timestamp,
        sequence_number=sequence,
        participant_timestamp=sip_timestamp,
        first_observed_at=sip_timestamp + timedelta(minutes=15),
        revised_observed_at=None,
        exchange=1,
        conditions=(1, 2),
        correction=0,
        price=Decimal("1.25000000"),
        size=2,
        shares_per_contract=100,
        notional=Decimal("250.00000000"),
        payload_sha256="a" * 64,
        raw_batch_id=uuid4(),
        provider_trade_id="provider-metadata",
    )


def test_trade_archive_writes_validated_zstd_file_before_manifest(tmp_path):
    repository = RecordingManifestRepository(tmp_path)
    archive = ParquetRawMarketArchive(
        tmp_path,
        repository,
        writer_instance_id=uuid4(),
    )

    manifests = archive.write_trades((_event(2, 1), _event(1, 0)))

    assert len(manifests) == 1
    manifest = manifests[0]
    final_path = tmp_path / manifest.object_key
    assert final_path.exists()
    assert list(tmp_path.rglob("*.partial")) == []
    parquet_file = pq.ParquetFile(final_path)
    assert parquet_file.schema_arrow.equals(TRADE_SCHEMA, check_metadata=True)
    assert parquet_file.metadata.num_rows == 2
    assert {
        parquet_file.metadata.row_group(0).column(index).compression
        for index in range(parquet_file.metadata.num_columns)
    } == {"ZSTD"}
    assert "market_date=2026-08-29" in manifest.object_key
    assert "hour=10" in manifest.object_key
    assert repository.manifests == [manifest]


def test_archive_queue_bounds_items_and_bytes_without_dropping_required_data():
    event = _event()
    queue = BoundedTradeArchiveQueue(maximum_items=1, maximum_bytes=10_000)

    assert queue.put(event, required=True) is True
    assert queue.put(event, required=False) is False
    with pytest.raises(ArchiveBackpressure):
        queue.put(event, required=True)
    assert queue.get() == event
    assert queue.item_count == 0
    assert queue.byte_count == 0


def test_buffer_flushes_on_row_and_age_thresholds():
    monotonic_time = [0.0]
    archive = RecordingArchive()
    buffer = BufferedTradeArchive(
        archive,
        maximum_rows=2,
        maximum_age_seconds=60,
        monotonic=lambda: monotonic_time[0],
    )

    assert buffer.add(_event(1)) == ()
    assert buffer.add(_event(2, 1)) == ()
    assert len(archive.batches) == 1
    assert len(archive.batches[0]) == 2

    buffer.add(_event(3, 2))
    monotonic_time[0] = 60
    assert buffer.flush_due() == ()
    assert len(archive.batches) == 2


def test_buffer_flushes_same_underlying_at_hour_boundary():
    archive = RecordingArchive()
    buffer = BufferedTradeArchive(
        archive,
        maximum_rows=100,
        maximum_age_seconds=60,
        monotonic=lambda: 0.0,
    )

    buffer.add(_event(1))
    next_hour = _event(2, 60 * 60)
    buffer.add(next_hour)

    assert len(archive.batches) == 1
    assert archive.batches[0][0].sequence_number == 1
    buffer.flush_all()
    assert archive.batches[1][0] == next_hour


def test_startup_reconciliation_adopts_orphan_and_removes_only_stale_partial(tmp_path):
    first_repository = RecordingManifestRepository(tmp_path)
    archive = ParquetRawMarketArchive(tmp_path, first_repository)
    manifest = archive.write_trades((_event(),))[0]
    first_repository.manifests.clear()

    stale_partial = tmp_path / "stale.parquet.partial"
    stale_partial.write_bytes(b"stale")
    recent_partial = tmp_path / "recent.parquet.partial"
    recent_partial.write_bytes(b"recent")
    now = datetime.now(UTC)
    stale_time = (now - timedelta(minutes=20)).timestamp()
    recent_time = now.timestamp()
    import os

    os.utime(stale_partial, (stale_time, stale_time))
    os.utime(recent_partial, (recent_time, recent_time))

    report = ArchiveReconciler(
        tmp_path,
        first_repository,
        clock=lambda: now,
    ).reconcile()

    assert manifest.object_key in report.orphan_files_adopted
    assert "stale.parquet.partial" in report.stale_partials_removed
    assert "recent.parquet.partial" in report.recent_partials_retained
    assert not stale_partial.exists()
    assert recent_partial.exists()
    assert first_repository.manifests[0].payload_sha256 == manifest.payload_sha256


def test_startup_reconciliation_marks_missing_and_corrupt_manifests(tmp_path):
    repository = RecordingManifestRepository(tmp_path)
    archive = ParquetRawMarketArchive(tmp_path, repository)
    missing = archive.write_trades((_event(1),))[0]
    missing_path = tmp_path / missing.object_key
    missing_path.unlink()
    corrupt = archive.write_trades((_event(2, 1),))[0]
    corrupt_path = tmp_path / corrupt.object_key
    corrupt_path.write_bytes(b"not parquet")

    report = ArchiveReconciler(tmp_path, repository).reconcile()

    assert missing.object_key in report.missing_manifest_files
    assert corrupt.object_key in report.corrupt_manifest_files
    assert len(repository.failures) == 2


def test_archive_factory_is_opt_in_and_uses_versioned_limits(tmp_path):
    disabled = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    repository = RecordingManifestRepository(tmp_path)
    assert build_raw_archive(disabled, repository, monotonic=lambda: 0.0) is None

    enabled = load_option_runtime_configuration(
        {
            "POLYGON_API_KEY": "test-secret",
            "OPTION_RAW_ARCHIVE_ENABLED": "true",
            "OPTION_RAW_ARCHIVE_ROOT": str(tmp_path),
        },
        BACKEND_DIR,
    )
    components = build_raw_archive(enabled, repository, monotonic=lambda: 0.0)

    assert components is not None
    assert components.queue.maximum_items == enabled.policy.archive.maximum_queue_items
    assert components.archive.root == tmp_path.resolve()