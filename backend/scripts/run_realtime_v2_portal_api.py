"""Hardened stored-only V2 portal API factory."""
from __future__ import annotations

import os
import sys
import threading
from contextlib import asynccontextmanager
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import API_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.portal_api import build_realtime_portal_repositories, create_realtime_portal_app  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402


class V2ApiDatabase(V2WorkerDatabase):
    def __init__(
        self,
        provider,
        role: RealtimeDatabaseRole,
        *,
        maximum_concurrent_transactions: int = 4,
        acquire_timeout_seconds: float = 30.0,
    ) -> None:
        if maximum_concurrent_transactions <= 0 or acquire_timeout_seconds <= 0:
            raise ValueError("V2 API transaction limits must be positive")
        super().__init__(provider, role)
        self._transaction_slots = threading.BoundedSemaphore(
            maximum_concurrent_transactions
        )
        self._acquire_timeout_seconds = acquire_timeout_seconds

    @contextmanager
    def transaction(
        self,
        *,
        read_only: bool | None = None,
        dict_cursor: bool = False,
        isolation_level: str | None = None,
    ):
        acquired = self._transaction_slots.acquire(
            timeout=self._acquire_timeout_seconds
        )
        if not acquired:
            raise RuntimeError("V2 API database transaction capacity timed out")
        try:
            with self._provider.transaction(
                API_ROLE,
                read_only=True,
                dict_cursor=dict_cursor,
                isolation_level=isolation_level,
            ) as unit_of_work:
                yield unit_of_work.cursor
        finally:
            self._transaction_slots.release()


def create_app():
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_API_DSN", "").strip()
    if not dsn:
        raise RuntimeError("V2 portal API requires REALTIME_V2_API_DSN")
    pool = ThreadedConnectionPool(1, 4, dsn=dsn)
    try:
        provider = V2ConnectionProvider(
            {API_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix="stocks_realtime_v2",
                statement_timeout_ms=30_000,
            ),
        )
        provider.verify()
        database = V2ApiDatabase(provider, RealtimeDatabaseRole.READER)
        app = create_realtime_portal_app(
            build_realtime_portal_repositories(database),
            database_target="stocks_realtime_v2",
        )
    except Exception:
        pool.closeall()
        raise
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        try:
            async with original_lifespan(application) as state:
                yield state
        finally:
            pool.closeall()

    app.router.lifespan_context = lifespan
    return app
