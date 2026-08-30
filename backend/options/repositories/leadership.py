from __future__ import annotations

import hashlib
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from .base import ConnectionFactory, default_connection_factory


OPTION_SCHEDULER_ADVISORY_LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"stock-screener:option-scheduler:v1").digest()[:8],
    byteorder="big",
    signed=True,
)


class OptionSchedulerLeadership:
    def __init__(
        self,
        instance_id: UUID,
        configuration_sha256: str,
        policy_sha256: str,
        process_id: int,
        host_name: str,
        connection_factory: ConnectionFactory | None = None,
        lock_key: int = OPTION_SCHEDULER_ADVISORY_LOCK_KEY,
    ) -> None:
        self.instance_id = instance_id
        self.configuration_sha256 = configuration_sha256
        self.policy_sha256 = policy_sha256
        self.process_id = process_id
        self.host_name = host_name
        self.lock_key = lock_key
        self._connection_factory = connection_factory or default_connection_factory
        self._connection_context: AbstractContextManager[Any] | None = None
        self._connection: Any | None = None

    @property
    def acquired(self) -> bool:
        return self._connection is not None and not self._connection.closed

    def acquire(self) -> bool:
        if self.acquired:
            raise RuntimeError("scheduler leadership is already acquired")
        connection_context = self._connection_factory()
        connection = connection_context.__enter__()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s) AS acquired",
                    (self.lock_key,),
                )
                if not cursor.fetchone()["acquired"]:
                    connection.rollback()
                    connection_context.__exit__(None, None, None)
                    return False
                cursor.execute(
                    """
                    INSERT INTO option_scheduler_instances (
                        instance_id, configuration_sha256, policy_sha256,
                        process_id, host_name, status, acquired_at,
                        last_heartbeat_at
                    ) VALUES (%s, %s, %s, %s, %s, 'LEADER', NOW(), NOW())
                    """,
                    (
                        self.instance_id,
                        self.configuration_sha256,
                        self.policy_sha256,
                        self.process_id,
                        self.host_name,
                    ),
                )
            connection.commit()
        except Exception:
            if not connection.closed:
                connection.rollback()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s)", (self.lock_key,))
                connection.commit()
            connection_context.__exit__(None, None, None)
            raise
        self._connection_context = connection_context
        self._connection = connection
        return True

    def heartbeat(self) -> datetime:
        connection = self._require_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE option_scheduler_instances
                    SET last_heartbeat_at = NOW(), updated_at = NOW()
                    WHERE instance_id = %s AND status = 'LEADER'
                    RETURNING last_heartbeat_at
                    """,
                    (self.instance_id,),
                )
                row = cursor.fetchone()
                if not row:
                    raise RuntimeError("scheduler heartbeat row is missing or not leader")
            connection.commit()
            return row["last_heartbeat_at"]
        except Exception:
            if not connection.closed:
                connection.rollback()
            raise

    def release(self) -> None:
        if self._connection is None or self._connection_context is None:
            return
        connection = self._connection
        connection_context = self._connection_context
        try:
            if not connection.closed:
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s) AS released",
                        (self.lock_key,),
                    )
                    if not cursor.fetchone()["released"]:
                        raise RuntimeError("scheduler advisory lock was not held")
                    cursor.execute(
                        """
                        UPDATE option_scheduler_instances
                        SET status = 'STOPPED', stopped_at = NOW(), updated_at = NOW()
                        WHERE instance_id = %s AND status = 'LEADER'
                        """,
                        (self.instance_id,),
                    )
                connection.commit()
        except Exception:
            if not connection.closed:
                connection.rollback()
            raise
        finally:
            self._connection = None
            self._connection_context = None
            connection_context.__exit__(None, None, None)

    def _require_connection(self):
        if not self.acquired:
            raise RuntimeError("scheduler leadership is not acquired")
        return self._connection

    def __enter__(self) -> "OptionSchedulerLeadership":
        if not self.acquire():
            raise RuntimeError("another option scheduler holds the advisory lock")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()