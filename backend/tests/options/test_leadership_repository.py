import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.repositories.leadership import OptionSchedulerLeadership


def _leadership(fetch_rows):
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    cursor.fetchone.side_effect = fetch_rows
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    leadership = OptionSchedulerLeadership(
        instance_id=uuid4(),
        configuration_sha256="a" * 64,
        policy_sha256="b" * 64,
        process_id=123,
        host_name="test-host",
        connection_factory=factory,
    )
    return leadership, connection, cursor


def test_leadership_holds_dedicated_connection_and_uses_database_heartbeat_time():
    heartbeat_at = datetime(2026, 8, 29, 20, 0, tzinfo=timezone.utc)
    leadership, connection, cursor = _leadership(
        [{"acquired": True}, {"last_heartbeat_at": heartbeat_at}, {"released": True}]
    )

    assert leadership.acquire() is True
    assert leadership.acquired is True
    assert leadership.heartbeat() == heartbeat_at
    leadership.release()

    statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "pg_try_advisory_lock" in statements
    assert "INSERT INTO option_scheduler_instances" in statements
    assert "last_heartbeat_at = NOW()" in statements
    assert "pg_advisory_unlock" in statements
    assert leadership.acquired is False
    assert connection.commit.call_count == 3


def test_failed_leadership_acquisition_exits_without_polling():
    leadership, connection, cursor = _leadership([{"acquired": False}])

    assert leadership.acquire() is False
    assert leadership.acquired is False
    assert cursor.execute.call_count == 1
    connection.rollback.assert_called_once_with()


def test_heartbeat_requires_held_leadership():
    leadership, _, _ = _leadership([])

    with pytest.raises(RuntimeError, match="not acquired"):
        leadership.heartbeat()