"""The Fed calendar refreshes after each close, stays valid a week, and cannot stop the role."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from market_events import FomcCalendarSnapshot, FomcMeeting
from options.calendar import OptionExchangeCalendar
from realtime.canonical_stock_alert_product import _event_context
from realtime.fed_calendar_refresh import (
    FED_CALENDAR_VALIDITY,
    fed_calendar_refresh_due_cursor,
    latest_completed_session_close,
    refresh_fed_calendar,
    run_fed_calendar_refresh,
)

UTC = timezone.utc
CALENDAR = OptionExchangeCalendar("XNYS")
THURSDAY_CLOSE = datetime(2026, 11, 5, 21, 0, tzinfo=UTC)
SNAPSHOT = FomcCalendarSnapshot(
    datetime(2026, 8, 19, tzinfo=UTC),
    (
        FomcMeeting(2026, 8, date(2026, 12, 9)),
        FomcMeeting(2027, 1, date(2027, 1, 27)),
    ),
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


def _coverage_writes(provider):
    return [
        values
        for statement, values in provider.cursor.statements
        if "INSERT INTO ref.market_event_coverage" in statement
    ]


@pytest.mark.parametrize(
    ("now", "expected"),
    (
        # After Thursday's close, that close.
        (THURSDAY_CLOSE + timedelta(minutes=1), THURSDAY_CLOSE),
        # During Thursday's session, Wednesday's.
        (datetime(2026, 11, 5, 18, tzinfo=UTC), datetime(2026, 11, 4, 21, tzinfo=UTC)),
        # Over the weekend, Friday's.
        (datetime(2026, 11, 8, 12, tzinfo=UTC), datetime(2026, 11, 6, 21, tzinfo=UTC)),
    ),
)
def test_latest_completed_session_close(now, expected):
    assert latest_completed_session_close(now, CALENDAR) == expected


@pytest.mark.parametrize(
    ("row", "now", "due"),
    (
        # Nothing stored.
        ((None, None), THURSDAY_CLOSE, True),
        # Nov 4 as it stands on live: expired since Oct 30.
        (
            (datetime(2026, 10, 30, 5, tzinfo=UTC), datetime(2026, 10, 30, 11, tzinfo=UTC)),
            datetime(2026, 11, 5, 8, tzinfo=UTC),
            True,
        ),
        # Valid, but not fetched since the last close: the daily refresh.
        (
            (datetime(2026, 11, 4, 22, tzinfo=UTC), datetime(2026, 11, 11, 22, tzinfo=UTC)),
            THURSDAY_CLOSE + timedelta(minutes=1),
            True,
        ),
        # Valid and fetched since the last close.
        (
            (datetime(2026, 11, 5, 21, 5, tzinfo=UTC), datetime(2026, 11, 12, 21, 5, tzinfo=UTC)),
            THURSDAY_CLOSE + timedelta(minutes=30),
            False,
        ),
        # During the session, after last night's refresh.
        (
            (datetime(2026, 11, 4, 22, tzinfo=UTC), datetime(2026, 11, 11, 22, tzinfo=UTC)),
            datetime(2026, 11, 5, 18, tzinfo=UTC),
            False,
        ),
    ),
)
def test_refresh_is_due_only_after_a_close_or_when_nothing_is_valid(row, now, due):
    assert fed_calendar_refresh_due_cursor(Cursor(row), now=now, calendar=CALENDAR) is due


def test_a_refresh_stores_coverage_valid_for_a_week():
    now = THURSDAY_CLOSE + timedelta(minutes=5)
    provider = Provider((None, None))

    result = refresh_fed_calendar(provider, clock=lambda: now, fetch=lambda: SNAPSHOT)

    assert result["fed_calendar"] == "REFRESHED"
    assert result["valid_until"] == (now + timedelta(days=7)).isoformat()
    assert FED_CALENDAR_VALIDITY == timedelta(days=7)
    assert provider.modes == [True, False]
    writes = _coverage_writes(provider)
    assert writes
    assert all(now + timedelta(days=7) in values for values in writes)


def test_a_current_calendar_makes_no_request_and_no_write():
    now = THURSDAY_CLOSE + timedelta(minutes=30)
    provider = Provider((now - timedelta(minutes=5), now + timedelta(days=7)))

    def fetch():
        raise AssertionError("a current calendar must not be fetched")

    assert refresh_fed_calendar(provider, clock=lambda: now, fetch=fetch) == {
        "fed_calendar": "CURRENT"
    }
    assert provider.modes == [True]


def test_a_failed_fetch_is_reported_and_writes_nothing():
    provider = Provider((None, None))

    def fetch():
        raise ConnectionError("no route")

    result = run_fed_calendar_refresh(
        provider,
        clock=lambda: THURSDAY_CLOSE,
        fetch=fetch,
    )

    assert result == {"fed_calendar": "FAILED", "error_type": "ConnectionError"}
    assert provider.modes == [True]
    assert _coverage_writes(provider) == []


def test_refreshed_coverage_clears_event_context_for_an_etf_all_week():
    """What the alert product itself reads. An ETF needs only the Fed calendar."""
    refreshed_at = THURSDAY_CLOSE + timedelta(minutes=5)
    provider = Provider((None, None))
    refresh_fed_calendar(provider, clock=lambda: refreshed_at, fetch=lambda: SNAPSHOT)
    window_start, window_end = None, None
    for statement, values in provider.cursor.statements:
        if "INSERT INTO ref.market_event_coverage" in statement:
            window_start = next(item for item in values if isinstance(item, date))
            window_end = [item for item in values if isinstance(item, date)][1]
    coverage = {
        "coverage_id": "c",
        "security_id": None,
        "event_type": "FED_RATE_DECISION",
        "source": "federal_reserve",
        "verdict": "COVERED",
        "checked_at": refreshed_at,
        "valid_until": refreshed_at + FED_CALENDAR_VALIDITY,
        "window_start": window_start,
        "window_end": window_end,
        "evidence_sha256": "a" * 64,
    }
    member = {"security_id": "s", "security_type": "ETF"}

    for cutoff, known in (
        (refreshed_at + timedelta(hours=18), False),
        (refreshed_at + timedelta(days=6, hours=23), False),
        (refreshed_at + timedelta(days=7, hours=1), None),
    ):
        event_risk, _evidence, _policy = _event_context(
            SimpleNamespace(cutoff=cutoff),
            member,
            (),
            (coverage,),
        )
        assert event_risk is known
