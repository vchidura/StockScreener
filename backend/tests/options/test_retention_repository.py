import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import ManifestCreationStatus, RawFileEventType, RawFileManifest
from options.repositories.retention import OptionRetentionRepository
from options.storage import RetentionReporter, RetentionVerification


def _manifest(status=ManifestCreationStatus.COMPLETE):
    now = datetime(2026, 8, 29, 20, 0, tzinfo=timezone.utc)
    return RawFileManifest(
        file_id=uuid4(),
        event_type=RawFileEventType.TRADE,
        market_date=date(2026, 8, 29),
        underlyer="SPY",
        market_hour=16,
        object_key="option-raw/event_type=trades/market_date=2026-08-29/part.parquet",
        schema_version=1,
        row_count=100,
        minimum_source_time=now,
        maximum_source_time=now,
        byte_size=4096,
        payload_sha256="a" * 64,
        creation_status=status,
        retention_class="RAW_30_DAY",
    )


def test_manifest_repository_accepts_only_completed_validated_files():
    cursor = MagicMock()
    cursor.closed = False
    cursor.rowcount = 1
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionRetentionRepository(factory)
    assert repository.record_completed_manifest(_manifest()) is True

    with pytest.raises(ValueError, match="complete file"):
        repository.record_completed_manifest(_manifest(ManifestCreationStatus.WRITING))


def test_tombstone_query_blocks_every_matching_active_hold_selector():
    cursor = MagicMock()
    cursor.closed = False
    cursor.rowcount = 1
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionRetentionRepository(factory)
    repository.tombstone_file(
        uuid4(),
        datetime(2026, 9, 30, 20, 0, tzinfo=timezone.utc),
        "retention complete",
    )

    sql = cursor.execute.call_args.args[0]
    assert "NOT EXISTS" in sql
    assert "@> hold.selector" in sql
    assert "hold.expires_at" in sql


def test_retention_report_defaults_to_blocked_and_never_mutates():
    manifest = _manifest()

    class ReportRepository:
        def __init__(self):
            self.calls = []

        def list_file_retention_candidates(self, cutoff, assessed_at):
            self.calls.append((cutoff, assessed_at))
            return ((manifest, 0),)

    repository = ReportRepository()
    assessed_at = datetime(2026, 10, 1, 20, 0, tzinfo=timezone.utc)
    report = RetentionReporter(repository).generate(assessed_at)

    assert report.eligible_file_count == 0
    assert report.blocked_file_count == 1
    assert report.assessments[0].blocked_reasons == (
        "ROLLUP_NOT_RECONCILED",
        "ACCEPTED_BACKUP_NOT_VERIFIED",
        "DEPENDENCIES_NOT_VERIFIED",
    )
    assert not hasattr(RetentionReporter, "delete")
    assert not hasattr(RetentionReporter, "purge")


def test_retention_report_marks_only_fully_verified_unheld_file_eligible():
    manifest = _manifest()

    class ReportRepository:
        def list_file_retention_candidates(self, cutoff, assessed_at):
            return ((manifest, 0),)

    verification = RetentionVerification(
        manifest.file_id,
        rollup_reconciled=True,
        accepted_backup_exists=True,
        dependencies_clear=True,
    )
    report = RetentionReporter(ReportRepository()).generate(
        datetime(2026, 10, 1, 20, 0, tzinfo=timezone.utc),
        (verification,),
    )

    assert report.eligible_file_count == 1
    assert report.eligible_bytes == manifest.byte_size
    assert report.assessments[0].blocked_reasons == ()