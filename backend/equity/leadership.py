"""Session-scoped PostgreSQL leadership for singleton equity operations."""
from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
from typing import Any, Callable, ContextManager, Iterator


ConnectionFactory = Callable[[], ContextManager[Any]]


def advisory_lock_key(name: str) -> int:
    if not name:
        raise ValueError("advisory lock name is required")
    return int.from_bytes(sha256(name.encode("utf-8")).digest()[:8], "big", signed=True)


@contextmanager
def try_advisory_leadership(
    name: str,
    *,
    connection_factory: ConnectionFactory | None = None,
) -> Iterator[bool]:
    if connection_factory is None:
        from database import get_db_connection

        connection_factory = get_db_connection
    lock_key = advisory_lock_key(name)
    with connection_factory() as connection:
        cursor = connection.cursor()
        acquired = False
        try:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            acquired = bool(cursor.fetchone()[0])
            connection.commit()
            yield acquired
        finally:
            if acquired and not connection.closed:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
                connection.commit()
            if not cursor.closed:
                cursor.close()