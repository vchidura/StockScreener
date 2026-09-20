from contextlib import contextmanager

import pytest

from scripts import report_option_stock_behavior_assessments as report


class FakeConnection:
    def __init__(self):
        self.closed = False
        self.rollbacks = 0
        self.sessions = []

    def rollback(self):
        self.rollbacks += 1

    def set_session(self, **settings):
        self.sessions.append(settings)


@pytest.mark.parametrize("fail", [False, True])
def test_read_only_report_always_restores_writable_session(monkeypatch, fail):
    connection = FakeConnection()

    @contextmanager
    def connection_factory():
        yield connection

    monkeypatch.setattr(report, "get_db_connection", connection_factory)

    if fail:
        with pytest.raises(RuntimeError, match="report failed"):
            with report._read_only_connection():
                raise RuntimeError("report failed")
    else:
        with report._read_only_connection() as current:
            assert current is connection

    assert connection.rollbacks == 2
    assert connection.sessions == [
        {"readonly": True, "isolation_level": "REPEATABLE READ"},
        {"readonly": False, "isolation_level": "READ COMMITTED"},
    ]