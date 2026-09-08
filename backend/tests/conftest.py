import os
import sys
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


# Local development read flags must not change default-path tests at import time.
for name in (
    "EQUITY_MATERIALIZED_30M_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1H_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1D_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1WK_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1MO_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_PATTERN_WATCH_ENABLED",
    "EQUITY_MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED",
    "EQUITY_MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED",
):
    os.environ[name] = "false"


class _NonCommittingConnection:
    """Hands the outer connection to a repository but swallows its commit.

    Lets a test exercise the real schema, constraints included, and still discard every
    write when the fixture rolls back.
    """

    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return _NonCommittingCursorSource(self._connection)

    def __exit__(self, *args):
        return False


class _NonCommittingCursorSource:
    def __init__(self, connection):
        self._connection = connection
        self.closed = False

    def cursor(self, **kwargs):
        return self._connection.cursor(**kwargs)

    def commit(self):
        return None

    def rollback(self):
        return None


@pytest.fixture
def rolled_back_connection():
    """Yields (connection_factory, connection); everything written is rolled back."""
    from database import get_db_connection

    with get_db_connection() as connection:
        def factory():
            return _NonCommittingConnection(connection)

        try:
            yield factory, connection
        finally:
            connection.rollback()
