from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.detector_runner import (
    RealtimeDetectorPersistence,
    RealtimeFeaturePersistence,
    persist_detector_runner_batch,
)
from realtime.worker_database import RealtimeProcessDatabase


OWNER = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SNAPSHOT = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _database():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    plan = RealtimeProcessDatabasePlan(
        "worker:detector_runner:active",
        RealtimeDatabaseRole.COMPUTE,
        RealtimePoolBudget(1, 1),
    )
    database = RealtimeProcessDatabase.wire(
            plan,
            principal_pool=pool,
            role_name="stocks_realtime_compute",
        )
    cursor.reset_mock()
    connection.reset_mock()
    pool.reset_mock()
    cursor.fetchone.side_effect = None
    connection.closed = False
    return (
        database,
        pool,
        connection,
        cursor,
    )


def _writes():
    feature = RealtimeFeaturePersistence(MagicMock(), MagicMock(), 7)
    detector = RealtimeDetectorPersistence(
        "detector:0",
        OWNER,
        MagicMock(),
        (),
        (),
        (),
    )
    return feature, detector


def test_feature_and_detector_repositories_share_one_worker_transaction(monkeypatch):
    database, pool, connection, cursor = _database()
    feature, detector = _writes()
    operation_order = []

    def persist_feature(active_cursor, result, definition, *, generation):
        assert active_cursor is cursor
        operation_order.append(("FEATURE", generation))
        return SNAPSHOT

    def persist_detector(active_cursor, **kwargs):
        assert active_cursor is cursor
        operation_order.append(("DETECTOR", kwargs["lease_resource_key"]))
        assert connection.commit.call_count == 0

    monkeypatch.setattr(
        "realtime.detector_runner.persist_realtime_equity_materialization_cursor",
        persist_feature,
    )
    monkeypatch.setattr(
        "realtime.detector_runner.persist_detector_evaluation_batch_cursor",
        persist_detector,
    )

    snapshot_ids = persist_detector_runner_batch(
        database,
        feature_writes=(feature,),
        detector_write=detector,
    )

    assert snapshot_ids == (SNAPSHOT,)
    assert operation_order == [("FEATURE", 7), ("DETECTOR", "detector:0")]
    assert connection.cursor.call_count == 1
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0
    pool.putconn.assert_called_once_with(connection, close=False)


def test_detector_repository_failure_rolls_back_feature_writes(monkeypatch):
    database, pool, connection, _ = _database()
    feature, detector = _writes()
    monkeypatch.setattr(
        "realtime.detector_runner.persist_realtime_equity_materialization_cursor",
        MagicMock(return_value=SNAPSHOT),
    )
    monkeypatch.setattr(
        "realtime.detector_runner.persist_detector_evaluation_batch_cursor",
        MagicMock(side_effect=RuntimeError("detector persistence failed")),
    )

    with pytest.raises(RuntimeError, match="detector persistence failed"):
        persist_detector_runner_batch(
            database,
            feature_writes=(feature,),
            detector_write=detector,
        )

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1
    pool.putconn.assert_called_once_with(connection, close=False)