import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.leadership import advisory_lock_key, try_advisory_leadership


def connection_factory(acquired):
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = (acquired,)
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return factory, connection, cursor


def test_advisory_leadership_holds_and_releases_same_session_lock():
    factory, connection, cursor = connection_factory(True)

    with try_advisory_leadership("equity-worker", connection_factory=factory) as leader:
        assert leader is True
        assert cursor.execute.call_count == 1

    lock_key = advisory_lock_key("equity-worker")
    assert cursor.execute.call_args_list[0].args == (
        "SELECT pg_try_advisory_lock(%s)", (lock_key,),
    )
    assert cursor.execute.call_args_list[1].args == (
        "SELECT pg_advisory_unlock(%s)", (lock_key,),
    )
    assert connection.commit.call_count == 2


def test_advisory_leadership_does_not_unlock_unowned_lock():
    factory, connection, cursor = connection_factory(False)

    with try_advisory_leadership("equity-worker", connection_factory=factory) as leader:
        assert leader is False

    assert cursor.execute.call_count == 1
    assert connection.commit.call_count == 1


def test_advisory_lock_key_is_stable_and_namespaced():
    assert advisory_lock_key("equity-worker") == advisory_lock_key("equity-worker")
    assert advisory_lock_key("equity-worker") != advisory_lock_key("option-worker")