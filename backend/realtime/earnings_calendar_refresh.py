"""Keep earnings dates current when Finnhub is available, and never depend on it.

The user's decision on Nov 5: use Finnhub if it answers, and do not treat it as mandatory.
Stock alerts evaluate with the event gate NOT_APPLICABLE when no valid coverage exists, so a
missing key or a failed fetch costs a warning label and nothing else. Like the Fed calendar,
this runs once after each close in the preparation role's idle slot. Earnings dates move more
often than Fed meetings, so coverage is valid for 3 days rather than a week.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path

from options.calendar import OptionExchangeCalendar

from .contracts import EQUITY_VALIDATION_UNIVERSE
from .database_access_v2 import APP_ROLE
from .fed_calendar_refresh import latest_completed_session_close
from .historical_reference_backfill import ETF_TICKERS
from .reference_adapters import FinnhubReferenceAdapter
from .reference_repository import persist_reference_facts_cursor
from .reference_service import ReferenceSeedRequest
from .reference_transports import (
    FinnhubReferenceTransport,
    RequestsReferenceHttpTransport,
)

EARNINGS_CALENDAR_VALIDITY = timedelta(days=3)
EARNINGS_CALENDAR_SOURCE = "finnhub"
FINNHUB_KEY_FILE = Path("/run/secrets/realtime_v2_finnhub_key")


def earnings_calendar_refresh_due_cursor(
    cursor,
    *,
    now: datetime,
    calendar: OptionExchangeCalendar,
) -> bool:
    """Due when no usable coverage is stored, or none was fetched since the last close."""
    cursor.execute(
        "SELECT MAX(checked_at),MAX(valid_until) "
        "FROM ref.market_event_coverage "
        "WHERE event_type='EARNINGS' AND source=%s "
        "AND verdict IN ('COVERED','KNOWN_NO_EVENT') AND checked_at<=%s",
        (EARNINGS_CALENDAR_SOURCE, now),
    )
    row = cursor.fetchone()
    checked_at, valid_until = (row[0], row[1]) if row is not None else (None, None)
    if checked_at is None or valid_until is None or valid_until <= now:
        return True
    return checked_at < latest_completed_session_close(now, calendar)


def refresh_earnings_calendar(
    provider,
    *,
    clock: Callable[[], datetime],
    fetch: Callable[[object, object], Sequence[Mapping[str, object]]],
    calendar: OptionExchangeCalendar | None = None,
) -> dict[str, object]:
    exchange = calendar or OptionExchangeCalendar("XNYS")
    now = clock()
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        due = earnings_calendar_refresh_due_cursor(
            unit.cursor,
            now=now,
            calendar=exchange,
        )
    if not due:
        return {"earnings_calendar": "CURRENT"}
    valid_until = now + EARNINGS_CALENDAR_VALIDITY
    generation = FinnhubReferenceAdapter(fetch, etf_tickers=ETF_TICKERS).collect(
        ReferenceSeedRequest(
            EQUITY_VALIDATION_UNIVERSE,
            now.date(),
            now,
            now,
            valid_until,
        )
    )
    if not generation.facts:
        raise ValueError("Finnhub earnings calendar produced no coverage")
    with provider.transaction(APP_ROLE) as unit:
        persist_reference_facts_cursor(unit.cursor, generation.facts)
    return {
        "earnings_calendar": "REFRESHED",
        "facts": len(generation.facts),
        "valid_until": valid_until.isoformat(),
    }


def run_earnings_calendar_refresh(
    provider,
    *,
    clock: Callable[[], datetime],
    key_file: Path = FINNHUB_KEY_FILE,
    fetch: Callable[[object, object], Sequence[Mapping[str, object]]] | None = None,
) -> dict[str, object]:
    """The maintenance slot's entry. Nothing here can stop the role, and only an error's
    type is reported, so the key cannot reach a log."""
    try:
        if fetch is None:
            if not key_file.is_file():
                return {"earnings_calendar": "NOT_CONFIGURED"}
            key = key_file.read_text(encoding="utf-8").strip()
            if not key:
                return {"earnings_calendar": "NOT_CONFIGURED"}
            fetch = FinnhubReferenceTransport(
                key,
                RequestsReferenceHttpTransport(),
            ).fetch
        return refresh_earnings_calendar(provider, clock=clock, fetch=fetch)
    except Exception as error:  # noqa: BLE001 - see above
        return {
            "earnings_calendar": "FAILED",
            "error_type": type(error).__name__,
        }
