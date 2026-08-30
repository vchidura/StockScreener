from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, ContextManager, Iterator

from psycopg2.extras import RealDictCursor, register_uuid


register_uuid()


ConnectionFactory = Callable[[], ContextManager[Any]]


def default_connection_factory() -> ContextManager[Any]:
    from database import get_db_connection

    return get_db_connection()


class PostgresRepository:
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self._connection_factory = connection_factory or default_connection_factory

    @contextmanager
    def _cursor(self, *, dict_rows: bool = True) -> Iterator[Any]:
        with self._connection_factory() as connection:
            cursor = connection.cursor(cursor_factory=RealDictCursor if dict_rows else None)
            try:
                yield cursor
                connection.commit()
            except Exception:
                if not connection.closed:
                    connection.rollback()
                raise
            finally:
                if not cursor.closed:
                    cursor.close()