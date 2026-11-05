"""Earnings refresh when Finnhub answers. With no key or a failure, nothing depends on it."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from options.calendar import OptionExchangeCalendar
from realtime.earnings_calendar_refresh import (
    EARNINGS_CALENDAR_VALIDITY,
    earnings_calendar_refresh_due_cursor,
    refresh_earnings_calendar,
    run_earnings_calendar_refresh,
)

UTC = timezone.utc
CALENDAR = OptionExchangeCalendar("XNYS")
CLOSE = datetime(2026, 11, 5, 21, 0, tzinfo=UTC)
ROWS = (
    {
        "symbol": "AAPL",
        "date": "2026-12-15",
        "hour": "amc",
        "quarter": 4,
        "year": 2026,
        "epsActual": None,
        "epsEstimate": 1.0,
        "revenueActual": None,
        "revenueEstimate": 100,
    },
)


class Cursor:
    def __init__(self, row=None):
        self.row = row
        self.statements = []

    def execute(self, statement, values=None):
        self.statements.append((statement, values))

    def fetchone(self):
        return self.row


class Provider:
    def __init__(self, row=None):
        self.cursor = Cursor(row)
        self.modes = []

    @contextmanager
    def transaction(self, _role, *, read_only=False):
        self.modes.append(read_only)
        yield SimpleNamespace(cursor=self.cursor)


def test_due_when_nothing_is_valid_or_nothing_since_the_last_close():
    now = CLOSE + timedelta(minutes=10)
    assert earnings_calendar_refresh_due_cursor(Cursor((None, None)), now=now, calendar=CALENDAR)
    assert earnings_calendar_refresh_due_cursor(
        Cursor((now - timedelta(days=4), now - timedelta(days=1))),
        now=now,
        calendar=CALENDAR,
    )
    assert earnings_calendar_refresh_due_cursor(
        Cursor((CLOSE - timedelta(days=1), now + timedelta(days=2))),
        now=now,
        calendar=CALENDAR,
    )
    assert not earnings_calendar_refresh_due_cursor(
        Cursor((CLOSE + timedelta(minutes=5), now + timedelta(days=3))),
        now=now,
        calendar=CALENDAR,
    )


def test_a_refresh_stores_coverage_valid_for_three_days():
    now = CLOSE + timedelta(minutes=5)
    provider = Provider((None, None))

    result = refresh_earnings_calendar(
        provider,
        clock=lambda: now,
        fetch=lambda _start, _end: ROWS,
    )

    assert result["earnings_calendar"] == "REFRESHED"
    assert EARNINGS_CALENDAR_VALIDITY == timedelta(days=3)
    writes = [
        values
        for statement, values in provider.cursor.statements
        if "INSERT INTO ref.market_event_coverage" in statement
    ]
    assert writes
    assert all(now + timedelta(days=3) in values for values in writes)


def test_no_key_is_not_an_error(tmp_path):
    provider = Provider((None, None))

    result = run_earnings_calendar_refresh(
        provider,
        clock=lambda: CLOSE,
        key_file=tmp_path / "absent",
    )

    assert result == {"earnings_calendar": "NOT_CONFIGURED"}
    assert provider.modes == []


def test_a_failed_fetch_reports_only_its_type_and_writes_nothing():
    provider = Provider((None, None))

    def fetch(_start, _end):
        raise ConnectionError("secret-looking-detail")

    result = run_earnings_calendar_refresh(
        provider,
        clock=lambda: CLOSE,
        fetch=fetch,
    )

    assert result == {"earnings_calendar": "FAILED", "error_type": "ConnectionError"}
    assert "secret-looking-detail" not in str(result)
    assert provider.modes == [True]
