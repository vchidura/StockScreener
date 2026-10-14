from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.materialization_jobs import (
    ClaimedMaterializationJob,
    MaterializationJob,
    complete_materialization_job_cursor,
)
from realtime.worker_database import RealtimeProcessDatabase


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, 0, tzinfo=UTC)
OWNER = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def _claimed_job():
    job = MaterializationJob(
        UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        "a" * 64,
        "BAR_FEATURE",
        1,
        UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        "b" * 64,
        NOW,
    )
    return ClaimedMaterializationJob(
        job,
        OWNER,
        2,
        NOW + timedelta(minutes=1),
        1,
    )


def _wired_database():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    cursor.rowcount = 1
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    process_plan = RealtimeProcessDatabasePlan(
        "worker:slow_materializer:active",
        RealtimeDatabaseRole.COMPUTE,
        RealtimePoolBudget(1, 1),
    )
    database = RealtimeProcessDatabase.wire(
        process_plan,
        principal_pool=pool,
        role_name="stocks_realtime_compute",
    )
    cursor.reset_mock()
    connection.reset_mock()
    pool.reset_mock()
    cursor.fetchone.side_effect = None
    cursor.fetchone.return_value = (OWNER, 2, True)
    cursor.rowcount = 1
    connection.closed = False
    return database, pool, connection, cursor


def test_worker_composes_job_and_output_repositories_on_one_cursor_and_commit():
    database, pool, connection, cursor = _wired_database()
    output_repository = MagicMock()

    with database.transaction() as active_cursor:
        complete_materialization_job_cursor(
            active_cursor,
            _claimed_job(),
            persist_output=output_repository,
        )
        assert connection.commit.call_count == 0

    output_repository.assert_called_once_with(cursor)
    assert connection.cursor.call_count == 1
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0
    pool.putconn.assert_called_once_with(connection, close=False)


def test_worker_repository_failure_rolls_back_the_shared_transaction():
    database, pool, connection, _ = _wired_database()
    output_repository = MagicMock(side_effect=RuntimeError("output failed"))

    with pytest.raises(RuntimeError, match="output failed"):
        with database.transaction() as active_cursor:
            complete_materialization_job_cursor(
                active_cursor,
                _claimed_job(),
                persist_output=output_repository,
            )

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1
    pool.putconn.assert_called_once_with(connection, close=False)