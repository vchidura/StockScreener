"""Keep the Fed calendar current without a person.

The Fed changes its calendar rarely, but alerts refuse to evaluate without valid coverage,
and nothing used to refresh it: the one-off reference tool stamped 6 hours of validity, so
the data expired the day it last ran. So this refreshes once after each session's close, in
the preparation role's idle slot, and stamps a week of validity, so a failed refresh or a
weekend never leaves alerts without it.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from market_events import FederalReserveCalendarClient, FomcCalendarSnapshot
from options.calendar import OptionExchangeCalendar

from .contracts import EQUITY_VALIDATION_UNIVERSE
from .database_access_v2 import APP_ROLE
from .reference_adapters import FederalReserveReferenceAdapter
from .reference_repository import persist_reference_facts_cursor
from .reference_service import ReferenceSeedRequest

FED_CALENDAR_VALIDITY = timedelta(days=7)
FED_CALENDAR_CHECK_INTERVAL = timedelta(minutes=30)
FED_CALENDAR_SOURCE = "federal_reserve"


def latest_completed_session_close(
    now: datetime,
    calendar: OptionExchangeCalendar,
) -> datetime:
    session = calendar.session_on_or_after(now.astimezone(timezone.utc).date())
    if session == now.astimezone(timezone.utc).date():
        close = calendar.session_close(session)
        if close <= now:
            return close
    return calendar.session_close(calendar.previous_session_date(session))


def fed_calendar_refresh_due_cursor(
    cursor,
    *,
    now: datetime,
    calendar: OptionExchangeCalendar,
) -> bool:
    """Due when nothing valid is stored, or nothing was fetched since the last close."""
    cursor.execute(
        "SELECT MAX(checked_at),MAX(valid_until) "
        "FROM ref.market_event_coverage "
        "WHERE event_type='FED_RATE_DECISION' AND source=%s "
        "AND security_id IS NULL AND checked_at<=%s",
        (FED_CALENDAR_SOURCE, now),
    )
    row = cursor.fetchone()
    checked_at, valid_until = (row[0], row[1]) if row is not None else (None, None)
    if checked_at is None or valid_until is None or valid_until <= now:
        return True
    return checked_at < latest_completed_session_close(now, calendar)


def refresh_fed_calendar(
    provider,
    *,
    clock: Callable[[], datetime],
    fetch: Callable[[], FomcCalendarSnapshot] | None = None,
    calendar: OptionExchangeCalendar | None = None,
) -> dict[str, object]:
    """One public page request when due, and no write unless the page parsed."""
    exchange = calendar or OptionExchangeCalendar("XNYS")
    now = clock()
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        due = fed_calendar_refresh_due_cursor(
            unit.cursor,
            now=now,
            calendar=exchange,
        )
    if not due:
        return {"fed_calendar": "CURRENT"}
    adapter = FederalReserveReferenceAdapter(
        fetch or FederalReserveCalendarClient().fetch
    )
    valid_until = now + FED_CALENDAR_VALIDITY
    generation = adapter.collect(
        ReferenceSeedRequest(
            EQUITY_VALIDATION_UNIVERSE,
            now.date(),
            now,
            now,
            valid_until,
        )
    )
    if not generation.facts:
        raise ValueError("Federal Reserve calendar produced no coverage")
    with provider.transaction(APP_ROLE) as unit:
        persist_reference_facts_cursor(unit.cursor, generation.facts)
    return {
        "fed_calendar": "REFRESHED",
        "facts": len(generation.facts),
        "valid_until": valid_until.isoformat(),
    }


def run_fed_calendar_refresh(
    provider,
    *,
    clock: Callable[[], datetime],
    fetch: Callable[[], FomcCalendarSnapshot] | None = None,
) -> dict[str, object]:
    """The maintenance slot's entry: a failure is logged and retried, and never stops the
    role, because the last refresh stays valid for a week."""
    try:
        return refresh_fed_calendar(provider, clock=clock, fetch=fetch)
    except Exception as error:  # noqa: BLE001 - see above
        return {
            "fed_calendar": "FAILED",
            "error_type": type(error).__name__,
        }
