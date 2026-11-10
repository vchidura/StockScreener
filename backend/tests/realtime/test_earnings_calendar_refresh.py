"""Earnings refresh when Finnhub answers. With no key or a failure, nothing depends on it.

G92 (Nov 10): the desired revision's members, one Finnhub request per company, and a company Finnhub does not answer
reads N/A (no coverage) while the others refresh."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from options.calendar import OptionExchangeCalendar
from realtime import earnings_calendar_refresh as refresh_module
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
MEMBERS = (("AAPL", "EQUITY"), ("CRWD", "EQUITY"), ("SPY", "EQUITY"), ("XLK", "SECTOR_ETF"))


@pytest.fixture(autouse=True)
def _no_backoff():
    refresh_module._last_attempt.clear()
    yield
    refresh_module._last_attempt.clear()


class Cursor:
    def __init__(self, row=None, members=(), missing=()):
        self.row = row
        self.members = members
        self.missing = missing
        self.statements = []
        self._last = ""

    def execute(self, statement, values=None):
        self._last = statement
        self.statements.append((statement, values))

    def fetchone(self):
        return self.row

    def fetchall(self):
        if "realtime_desired_member_intents" in self._last:
            return list(self.members)
        if "NOT EXISTS" in self._last:
            return [(ticker,) for ticker in self.missing]
        return []


class Provider:
    def __init__(self, row=None, members=(), missing=()):
        self.cursor = Cursor(row, members, missing)
        self.modes = []

    @contextmanager
    def transaction(self, _role, *, read_only=False):
        self.modes.append(read_only)
        yield SimpleNamespace(cursor=self.cursor)


def _coverage_writes(provider):
    return [
        values for statement, values in provider.cursor.statements
        if "INSERT INTO ref.market_event_coverage" in statement
    ]


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


def test_due_when_one_company_has_no_valid_coverage():
    now = CLOSE + timedelta(minutes=10)
    fresh = (CLOSE + timedelta(minutes=5), now + timedelta(days=3))
    assert earnings_calendar_refresh_due_cursor(
        Cursor(fresh, missing=("CRWD",)), now=now, calendar=CALENDAR, companies=("AAPL", "CRWD"),
    )
    assert not earnings_calendar_refresh_due_cursor(
        Cursor(fresh), now=now, calendar=CALENDAR, companies=("AAPL", "CRWD"),
    )


def test_a_refresh_stores_coverage_valid_for_three_days():
    now = CLOSE + timedelta(minutes=5)
    provider = Provider((None, None))

    result = refresh_earnings_calendar(
        provider,
        clock=lambda: now,
        fetch=lambda _start, _end, symbol=None: tuple(row for row in ROWS if row["symbol"] == symbol),
    )

    assert result["earnings_calendar"] == "REFRESHED"
    assert EARNINGS_CALENDAR_VALIDITY == timedelta(days=3)
    writes = _coverage_writes(provider)
    assert writes
    assert all(now + timedelta(days=3) in values for values in writes)


def test_the_desired_members_are_asked_one_company_at_a_time():
    now = CLOSE + timedelta(minutes=5)
    provider = Provider((None, None), members=MEMBERS)
    asked = []

    def fetch(_start, _end, symbol=None):
        asked.append(symbol)
        return tuple(row for row in ROWS if row["symbol"] == symbol)

    result = refresh_earnings_calendar(provider, clock=lambda: now, fetch=fetch)

    # Companies only: SPY (index ETF) and XLK (sector ETF) are verified ETFs, never asked.
    assert asked == ["AAPL", "CRWD"]
    assert result["earnings_calendar"] == "REFRESHED"
    assert result["members"] == 4 and result["companies"] == 2


def test_a_company_finnhub_does_not_answer_is_not_recorded_as_having_no_earnings():
    now = CLOSE + timedelta(minutes=5)
    provider = Provider((None, None), members=MEMBERS)

    def fetch(_start, _end, symbol=None):
        if symbol == "CRWD":
            raise ConnectionError("finnhub hiccup")
        return tuple(row for row in ROWS if row["symbol"] == symbol)

    result = refresh_earnings_calendar(provider, clock=lambda: now, fetch=fetch)

    assert result["earnings_calendar"] == "PARTIAL"
    assert result["not_answered"] == ["CRWD"]
    covered = [values for values in _coverage_writes(provider)]
    # AAPL is covered; CRWD has no coverage row at all (N/A downstream), never KNOWN_NO_EVENT.
    crwd = str(__import__("uuid").uuid5(__import__("uuid").NAMESPACE_URL, "realtime-security:CRWD"))
    assert covered and not any(crwd in str(values) for values in covered)


def test_a_retry_waits_for_the_backoff():
    now = CLOSE + timedelta(minutes=5)
    provider = Provider((None, None), members=MEMBERS)
    calls = []

    def fetch(_start, _end, symbol=None):
        calls.append(symbol)
        raise ConnectionError("down")

    first = run_earnings_calendar_refresh(provider, clock=lambda: now, fetch=fetch)
    second = run_earnings_calendar_refresh(provider, clock=lambda: now + timedelta(minutes=5), fetch=fetch)
    third = run_earnings_calendar_refresh(provider, clock=lambda: now + timedelta(minutes=16), fetch=fetch)

    assert first["earnings_calendar"] == "FAILED"
    assert second["earnings_calendar"] == "BACKOFF"
    assert third["earnings_calendar"] == "FAILED"
    assert calls == ["AAPL", "CRWD", "AAPL", "CRWD"]


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

    def fetch(_start, _end, symbol=None):
        raise ConnectionError("secret-looking-detail")

    result = run_earnings_calendar_refresh(
        provider,
        clock=lambda: CLOSE,
        fetch=fetch,
    )

    assert result == {"earnings_calendar": "FAILED", "error_type": "ValueError"}
    assert "secret-looking-detail" not in str(result)
    assert provider.modes == [True]
