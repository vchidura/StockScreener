import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.errors import InvalidBatchTransition
from options.repositories.ingestion import OptionIngestionRepository


def _repository():
    cursor = MagicMock()
    cursor.closed = False
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def connection_factory():
        yield connection

    return OptionIngestionRepository(connection_factory), connection, cursor


def test_batch_completion_rejects_missing_terminal_page_and_rolls_back():
    repository, connection, cursor = _repository()
    cursor.fetchone.return_value = {
        "status": "FETCHING",
        "request_filter_sha256": "a" * 64,
    }
    cursor.fetchall.return_value = [
        {
            "page_number": 1,
            "row_count": 20,
            "byte_count": 100,
            "terminal_page": False,
            "request_filter_sha256": "a" * 64,
            "request_cursor_sha256": None,
            "next_cursor_sha256": "b" * 64,
            "validation_status": "VALID",
        }
    ]

    with pytest.raises(InvalidBatchTransition, match="terminal page"):
        repository.complete_batch(
            uuid4(), uuid4(), "normalize:batch", 5, 40, 10_000, 64 * 1024 * 1024
        )

    connection.rollback.assert_called_once_with()
    mutation_sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "UPDATE option_ingestion_runs" not in mutation_sql
    assert "INSERT INTO option_work_items" not in mutation_sql


def test_batch_completion_and_work_enqueue_share_one_transaction():
    repository, connection, cursor = _repository()
    cursor.fetchone.side_effect = [
        {"status": "FETCHING", "request_filter_sha256": "a" * 64}
    ]
    cursor.fetchall.return_value = [
        {
            "page_number": 1,
            "row_count": 20,
            "byte_count": 100,
            "terminal_page": True,
            "request_filter_sha256": "a" * 64,
            "request_cursor_sha256": None,
            "next_cursor_sha256": None,
            "validation_status": "VALID",
        }
    ]
    cursor.rowcount = 1
    batch_id = uuid4()
    work_id = uuid4()

    returned_work_id = repository.complete_batch(
        batch_id, work_id, f"normalize:{batch_id}", 5, 40, 10_000, 64 * 1024 * 1024
    )

    assert returned_work_id == work_id
    mutation_sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "UPDATE option_ingestion_runs" in mutation_sql
    assert "INSERT INTO option_work_items" in mutation_sql
    connection.commit.assert_called_once_with()


def test_batch_completion_rejects_cursor_or_filter_drift_before_mutation():
    repository, connection, cursor = _repository()
    cursor.fetchone.return_value = {
        "status": "FETCHING",
        "request_filter_sha256": "a" * 64,
    }
    cursor.fetchall.return_value = [
        {
            "page_number": 1,
            "row_count": 250,
            "byte_count": 100,
            "terminal_page": False,
            "request_filter_sha256": "a" * 64,
            "request_cursor_sha256": None,
            "next_cursor_sha256": "b" * 64,
            "validation_status": "VALID",
        },
        {
            "page_number": 2,
            "row_count": 10,
            "byte_count": 100,
            "terminal_page": True,
            "request_filter_sha256": "c" * 64,
            "request_cursor_sha256": "d" * 64,
            "next_cursor_sha256": None,
            "validation_status": "VALID",
        },
    ]

    with pytest.raises(InvalidBatchTransition, match="request filters"):
        repository.complete_batch(
            uuid4(), uuid4(), "normalize:drifted", 5, 40, 10_000, 64 * 1024 * 1024
        )

    connection.rollback.assert_called_once_with()
    mutation_sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "UPDATE option_ingestion_runs" not in mutation_sql


def test_existing_ingestion_slot_rejects_different_evidence_cohort():
    from datetime import datetime, timezone

    from options.domain import AssetType, BatchStatus, RawOptionBatch
    from options.errors import DuplicateFactConflict

    repository, connection, cursor = _repository()
    batch_id = uuid4()
    now = datetime(2026, 8, 29, 20, 0, tzinfo=timezone.utc)
    batch = RawOptionBatch(
        batch_id=batch_id,
        provider="polygon",
        underlyer="SPY",
        scheduled_cycle=now,
        request_filter_sha256="a" * 64,
        policy_sha256="b" * 64,
        status=BatchStatus.FETCHING,
        pages=(),
        started_at=now,
    )
    cursor.fetchone.side_effect = [
        None,
        {
            "batch_id": uuid4(),
            "asset_type": "ETF",
            "policy_version": "developer_v1",
            "policy_sha256": "c" * 64,
            "configuration_sha256": "d" * 64,
        },
    ]

    with pytest.raises(DuplicateFactConflict, match="different policy"):
        repository.begin_batch(
            batch,
            AssetType.ETF,
            "developer_v1",
            "d" * 64,
        )

    connection.rollback.assert_called_once_with()


def test_ingestion_slot_identity_includes_policy_and_configuration():
    source = (
        BACKEND_DIR / "options" / "repositories" / "ingestion.py"
    ).read_text(encoding="utf-8")
    schema = " ".join((
        BACKEND_DIR / "migrations" / "000_canonical_schema.sql"
    ).read_text(encoding="utf-8").split())

    identity = (
        "provider, underlying, scheduled_cycle, request_filter_sha256, "
        "policy_sha256, configuration_sha256"
    )
    assert identity in schema
    assert "request_filter_sha256,\n                    policy_sha256" in source
    assert 'BASELINE_VERSION = "000_canonical_schema"' in (
        BACKEND_DIR / "stock_screener" / "schema.py"
    ).read_text(encoding="utf-8")


def test_normalization_telemetry_updates_only_complete_batch():
    repository, connection, cursor = _repository()
    cursor.rowcount = 1
    batch_id = uuid4()

    repository.record_normalization(
        batch_id,
        catalog_row_count=100,
        retained_row_count=80,
        rejected_counts={"LIQUIDITY_FLOOR": 20},
        unknown_reference_count=0,
        market_data_time=None,
        first_observed_at=None,
    )

    sql, parameters = cursor.execute.call_args.args
    assert "status = 'COMPLETE'" in sql
    assert parameters[0:2] == (100, 80)
    assert parameters[-3:] == (None, None, batch_id)
    connection.commit.assert_called_once_with()