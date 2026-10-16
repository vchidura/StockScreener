from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from realtime.database_access import RealtimeDatabaseRole
from scripts.run_realtime_v2_portal_api import V2ApiDatabase


class ConcurrentProvider:
    def __init__(self):
        self._lock = threading.Lock()
        self.active = 0
        self.maximum_active = 0

    @contextmanager
    def transaction(self, *args, **kwargs):
        with self._lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            time.sleep(0.02)
            yield SimpleNamespace(cursor=object())
        finally:
            with self._lock:
                self.active -= 1


def test_api_database_queues_bursts_at_the_pool_capacity():
    provider = ConcurrentProvider()
    database = V2ApiDatabase(
        provider,
        RealtimeDatabaseRole.READER,
        maximum_concurrent_transactions=4,
    )

    def read_once():
        with database.transaction(read_only=True):
            return True

    with ThreadPoolExecutor(max_workers=12) as executor:
        assert all(executor.map(lambda _: read_once(), range(12)))
    assert provider.maximum_active == 4


def test_api_database_rejects_invalid_transaction_limits():
    with pytest.raises(ValueError, match="transaction limits"):
        V2ApiDatabase(
            ConcurrentProvider(),
            RealtimeDatabaseRole.READER,
            maximum_concurrent_transactions=0,
        )
