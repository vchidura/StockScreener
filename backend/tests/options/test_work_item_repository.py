import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import WorkStage, WorkStatus
from options.repositories.work_items import OptionWorkItemRepository


def test_claim_uses_skip_locked_database_time_and_returns_frozen_work_item():
    cursor = MagicMock()
    cursor.closed = False
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)
    work_id = uuid4()
    cursor.fetchone.return_value = {
        "work_id": work_id,
        "stage": "NORMALIZE",
        "subject_id": "batch-1",
        "business_key": "normalize:batch-1",
        "status": "CLAIMED",
        "attempt_count": 1,
        "maximum_attempts": 5,
        "created_at": now,
        "next_attempt_at": now,
        "lease_owner": "worker-1",
        "lease_expires_at": now + timedelta(minutes=2),
        "last_error": None,
        "completed_at": None,
    }

    @contextmanager
    def connection_factory():
        yield connection

    repository = OptionWorkItemRepository(connection_factory)
    item = repository.claim_next(WorkStage.NORMALIZE, "worker-1", timedelta(minutes=2))

    assert item is not None
    assert item.work_id == work_id
    assert item.status is WorkStatus.CLAIMED
    sql, parameters = cursor.execute.call_args.args
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "lease_expires_at = NOW()" in sql
    assert parameters == ("NORMALIZE", "worker-1", 120)
    connection.commit.assert_called_once_with()


def test_expired_claim_recovery_is_bounded_by_attempt_count():
    cursor = MagicMock()
    cursor.closed = False
    cursor.rowcount = 3
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def connection_factory():
        yield connection

    repository = OptionWorkItemRepository(connection_factory)

    assert repository.recover_expired_claims() == 3
    sql = cursor.execute.call_args.args[0]
    assert "attempt_count >= maximum_attempts" in sql
    assert "lease_expires_at <= NOW()" in sql


def test_exact_business_key_claim_does_not_take_unrelated_work():
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = None
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def connection_factory():
        yield connection

    repository = OptionWorkItemRepository(connection_factory)
    result = repository.claim_by_business_key(
        "normalize:batch-1",
        "worker-1",
        timedelta(minutes=2),
    )

    assert result is None
    sql, parameters = cursor.execute.call_args.args
    assert "work.business_key = %s" in sql
    assert parameters == ("worker-1", 120, "normalize:batch-1")


def test_exact_business_key_lookup_returns_terminal_failure_state():
    cursor = MagicMock()
    cursor.closed = False
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)
    cursor.fetchone.return_value = {
        "work_id": uuid4(),
        "stage": "STRATEGY",
        "subject_id": "matrix-1",
        "business_key": "strategy:matrix-1:phase2_v1",
        "status": "TERMINAL_FAILED",
        "attempt_count": 5,
        "maximum_attempts": 5,
        "created_at": now,
        "next_attempt_at": now,
        "lease_owner": None,
        "lease_expires_at": None,
        "last_error": "strategy failed",
        "completed_at": None,
    }
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def connection_factory():
        yield connection

    item = OptionWorkItemRepository(connection_factory).get_by_business_key(
        "strategy:matrix-1:phase2_v1"
    )

    assert item is not None
    assert item.status is WorkStatus.TERMINAL_FAILED
    assert item.last_error == "strategy failed"