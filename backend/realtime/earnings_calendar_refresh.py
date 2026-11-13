"""Keep earnings dates current when Finnhub is available, and never depend on it.

The user's decision on Nov 5: use Finnhub if it answers, and do not treat it as mandatory.
Stock alerts evaluate with the event gate NOT_APPLICABLE when no valid coverage exists, so a
missing key or a failed fetch costs a warning label and nothing else. Like the Fed calendar,
this runs once after each close in the preparation role's idle slot. Earnings dates move more
often than Fed meetings, so coverage is valid for 3 days rather than a week.

G92 (Nov 10, user: "if Finnhub returns use it, otherwise that ticker is N/A without blocking the
detectors and still part of the evaluation"):
- the universe is the current desired revision's Equity members, not a fixed list (the 16 tickers
  added Nov 6 had no calendar);
- one Finnhub request per company: the market-wide call returns at most 1,500 rows, newest first,
  so busy weeks silently lost their earliest days and read as "no earnings";
- a company Finnhub does not answer gets no coverage (N/A), never "no earnings", and the others
  still refresh;
- due per member, with a retry backoff so a symbol that keeps failing cannot flood the provider.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path

from options.calendar import OptionExchangeCalendar

from .contracts import EQUITY_VALIDATION_UNIVERSE, UniverseManifest
from .database_access_v2 import APP_ROLE
from .fed_calendar_refresh import latest_completed_session_close
from .historical_reference_backfill import ETF_TICKERS
from .reference_adapters import FinnhubReferenceAdapter
from .reference_repository import persist_reference_facts_cursor
from .reference_service import ReferenceSeedRequest
from .reference_transports import (
    FinnhubReferenceTransport,
    ReferenceTransportLimits,
    RequestsReferenceHttpTransport,
)

EARNINGS_CALENDAR_VALIDITY = timedelta(days=3)
EARNINGS_CALENDAR_SOURCE = "finnhub"
FINNHUB_KEY_FILE = Path("/run/secrets/realtime_v2_finnhub_key")
# A company Finnhub did not answer is retried after this long, not on every idle loop.
EARNINGS_RETRY_BACKOFF = timedelta(minutes=15)
# R60 (Nov 13): per-symbol calls over the 47-day window are two 30-day requests per company; the transport's default
# cap of 64 refused the 33rd company onward (35 companies at 07:26 and 07:44: WFC, WMT, XOM N/A). Sized for 400 names.
EARNINGS_MAXIMUM_REQUESTS = 1024
_DESIRED_STATE_SCOPE = "ADVANCED_REALTIME"  # advanced_realtime_desired_state_repository.DESIRED_STATE_SCOPE
_last_attempt: dict[str, datetime] = {}


def desired_equity_members_cursor(cursor) -> tuple[tuple[str, str], ...]:
    """(ticker, member kind) of the current desired revision's enabled Equity and sector ETF members."""
    cursor.execute(
        "SELECT intent.ticker,intent.member_kind FROM rt.realtime_desired_current AS current "
        "JOIN rt.realtime_desired_member_intents AS intent USING(revision_id) "
        "WHERE current.scope_key=%s AND intent.enabled "
        "AND intent.member_kind IN ('EQUITY','SECTOR_ETF') ORDER BY intent.ticker",
        (_DESIRED_STATE_SCOPE,),
    )
    return tuple(
        (str(row[0] if not isinstance(row, Mapping) else row["ticker"]).strip().upper(),
         str(row[1] if not isinstance(row, Mapping) else row["member_kind"]))
        for row in cursor.fetchall()
    )


def _universe(members: Sequence[tuple[str, str]]) -> tuple[UniverseManifest, frozenset[str]]:
    if not members:
        return EQUITY_VALIDATION_UNIVERSE, ETF_TICKERS
    tickers = tuple(sorted({ticker for ticker, _kind in members}))
    etfs = frozenset(ETF_TICKERS | {ticker for ticker, kind in members if kind == "SECTOR_ETF"})
    return UniverseManifest(
        version="advanced-equity-earnings-calendar-v2",
        equity_tickers=tickers,
        option_underlyings=(),
    ), etfs


def earnings_calendar_refresh_due_cursor(
    cursor,
    *,
    now: datetime,
    calendar: OptionExchangeCalendar,
    companies: Sequence[str] | None = None,
) -> bool:
    """Due when nothing was fetched since the last close, or (`companies` given) when one of them has no valid
    coverage now. The caller applies the retry backoff."""
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
    if checked_at < latest_completed_session_close(now, calendar):
        return True
    if not companies:
        return False
    cursor.execute(
        "SELECT revision.ticker FROM ref.security_revisions AS revision "
        "WHERE revision.active AND revision.ticker=ANY(%s) AND NOT EXISTS ("
        "SELECT 1 FROM ref.market_event_coverage AS coverage "
        "WHERE coverage.security_id=revision.security_id AND coverage.event_type='EARNINGS' "
        "AND coverage.source=%s AND coverage.verdict IN ('COVERED','KNOWN_NO_EVENT') "
        "AND coverage.checked_at<=%s AND coverage.valid_until>%s)",
        (list(companies), EARNINGS_CALENDAR_SOURCE, now, now),
    )
    return bool(cursor.fetchall())


def refresh_earnings_calendar(
    provider,
    *,
    clock: Callable[[], datetime],
    fetch: Callable[..., Sequence[Mapping[str, object]]],
    calendar: OptionExchangeCalendar | None = None,
    per_symbol: bool = True,
) -> dict[str, object]:
    exchange = calendar or OptionExchangeCalendar("XNYS")
    now = clock()
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        universe, etfs = _universe(desired_equity_members_cursor(unit.cursor))
        companies = tuple(ticker for ticker in universe.equity_tickers if ticker not in etfs)
        due = earnings_calendar_refresh_due_cursor(
            unit.cursor,
            now=now,
            calendar=exchange,
            companies=companies,
        )
    if not due:
        return {"earnings_calendar": "CURRENT"}
    previous = _last_attempt.get("finnhub")
    if previous is not None and now - previous < EARNINGS_RETRY_BACKOFF:
        return {"earnings_calendar": "BACKOFF", "retry_after": (previous + EARNINGS_RETRY_BACKOFF).isoformat()}
    _last_attempt["finnhub"] = now
    valid_until = now + EARNINGS_CALENDAR_VALIDITY
    generation = FinnhubReferenceAdapter(fetch, etf_tickers=etfs, per_symbol=per_symbol).collect(
        ReferenceSeedRequest(
            universe,
            now.date(),
            now,
            now,
            valid_until,
        )
    )
    unanswered = sorted(
        member.key for member in generation.members
        if any(str(reason).startswith("FINNHUB_SYMBOL_UNAVAILABLE") for reason in member.reason_codes)
    )
    if not generation.facts or (companies and len(unanswered) == len(companies)):
        raise ValueError("Finnhub earnings calendar produced no coverage")
    with provider.transaction(APP_ROLE) as unit:
        persist_reference_facts_cursor(unit.cursor, generation.facts)
    return {
        "earnings_calendar": "REFRESHED" if not unanswered else "PARTIAL",
        "members": len(universe.equity_tickers),
        "companies": len(companies),
        "not_answered": unanswered,
        "facts": len(generation.facts),
        "valid_until": valid_until.isoformat(),
    }


def run_earnings_calendar_refresh(
    provider,
    *,
    clock: Callable[[], datetime],
    key_file: Path = FINNHUB_KEY_FILE,
    fetch: Callable[..., Sequence[Mapping[str, object]]] | None = None,
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
                limits=ReferenceTransportLimits(maximum_requests=EARNINGS_MAXIMUM_REQUESTS),
            ).fetch
        return refresh_earnings_calendar(provider, clock=clock, fetch=fetch)
    except Exception as error:  # noqa: BLE001 - see above
        return {
            "earnings_calendar": "FAILED",
            "error_type": type(error).__name__,
        }
