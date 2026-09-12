from __future__ import annotations

from collections import Counter
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from html.parser import HTMLParser
import json
import time as clock
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import requests

from http_client import get_session
from options.event_calendar import parse_event_calendar_document
from options.repositories.market_events import (
    OptionEventCalendarPersistResult,
    OptionMarketEventRepository,
)
from security_types import (
    is_earnings_applicable_security_type,
)


PUBLIC_CALENDAR_SOURCE = "public_calendar_v1"
FINNHUB_EARNINGS_URL = "https://finnhub.io/api/v1/calendar/earnings"
FINNHUB_MAX_WINDOW_DAYS = 30
FINNHUB_MAX_ROWS_PER_REQUEST = 10_000
FINNHUB_REQUEST_INTERVAL_SECONDS = 2.1
FEDERAL_RESERVE_CALENDAR_URL = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)
ET = ZoneInfo("America/New_York")
_MONTHS = {
    name: number
    for number, name in enumerate(
        (
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ),
        start=1,
    )
}
_MONTHS.update({name[:3]: number for name, number in tuple(_MONTHS.items())})


class FinnhubRateLimitError(requests.HTTPError):
    def __init__(self, retry_after_seconds: float | None) -> None:
        super().__init__("Finnhub earnings request was rate limited")
        self.retry_after_seconds = retry_after_seconds


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


class FinnhubEarningsClient:
    def __init__(
        self,
        api_key: str,
        session: requests.Session | None = None,
        sleep=clock.sleep,
    ) -> None:
        if not api_key.strip():
            raise ValueError("Finnhub API key is required")
        self.api_key = api_key
        self.session = session or get_session()
        self.sleep = sleep

    def fetch(self, start: date, end: date) -> tuple[Mapping[str, Any], ...]:
        if end < start:
            raise ValueError("earnings calendar end must not precede start")
        rows: list[Mapping[str, Any]] = []
        chunk_start = start
        request_count = 0
        while chunk_start <= end:
            chunk_end = min(
                chunk_start + timedelta(days=FINNHUB_MAX_WINDOW_DAYS - 1),
                end,
            )
            if request_count:
                self.sleep(FINNHUB_REQUEST_INTERVAL_SECONDS)
            rows.extend(self._fetch_chunk(chunk_start, chunk_end))
            request_count += 1
            chunk_start = chunk_end + timedelta(days=1)
        if request_count > 1:
            self.sleep(FINNHUB_REQUEST_INTERVAL_SECONDS)
            global_rows = self._fetch_chunk(start, end)
            bounded_facts = Counter(_canonical_provider_row(row) for row in rows)
            global_facts = Counter(_canonical_provider_row(row) for row in global_rows)
            if bounded_facts != global_facts:
                raise ValueError(
                    "Finnhub global and bounded earnings responses disagree"
                )
        return tuple(rows)

    def _fetch_chunk(
        self, start: date, end: date
    ) -> tuple[Mapping[str, Any], ...]:
        response = self.session.get(
            FINNHUB_EARNINGS_URL,
            params={"from": start.isoformat(), "to": end.isoformat()},
            headers={"X-Finnhub-Token": self.api_key},
            timeout=(5, 30),
            allow_redirects=False,
        )
        if response.status_code == 429:
            raise FinnhubRateLimitError(
                _retry_after_seconds(response.headers.get("Retry-After"))
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(
            payload.get("earningsCalendar"), list
        ):
            raise ValueError("Finnhub earnings response has an invalid shape")
        rows = payload["earningsCalendar"]
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("Finnhub earnings rows must be objects")
        if len(rows) > FINNHUB_MAX_ROWS_PER_REQUEST:
            raise ValueError("Finnhub earnings response exceeds the row limit")
        for row in rows:
            symbol = row.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("Finnhub earnings row has no symbol")
            try:
                report_date = date.fromisoformat(str(row.get("date") or ""))
            except ValueError as exc:
                raise ValueError("Finnhub earnings row has an invalid date") from exc
            if not start <= report_date <= end:
                raise ValueError("Finnhub earnings row falls outside its request window")
        return tuple(rows)


def _canonical_provider_row(row: Mapping[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


@dataclass(frozen=True, slots=True)
class FomcMeeting:
    year: int
    ordinal: int
    decision_date: date


@dataclass(frozen=True, slots=True)
class FomcCalendarSnapshot:
    source_updated_at: datetime | None
    meetings: tuple[FomcMeeting, ...]


class _FomcHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.current_year: int | None = None
        self.pending_month: str | None = None
        self.raw_meetings: list[tuple[int, str, str]] = []
        self.last_update: str | None = None
        self._capture: str | None = None
        self._capture_tag: str | None = None
        self._text: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = dict(attrs)
        classes = set((attributes.get("class") or "").split())
        capture = None
        if tag == "h4":
            capture = "heading"
        elif tag == "div" and "fomc-meeting__month" in classes:
            capture = "month"
        elif tag == "div" and "fomc-meeting__date" in classes:
            capture = "date"
        elif tag == "div" and attributes.get("id") == "lastUpdate":
            capture = "last_update"
        if capture is not None:
            self._capture = capture
            self._capture_tag = tag
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._capture is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._capture is None or tag != self._capture_tag:
            return
        value = " ".join("".join(self._text).split())
        if self._capture == "heading":
            match = re.search(r"\b(20\d{2})\s+FOMC Meetings\b", value)
            if match:
                self.current_year = int(match.group(1))
        elif self._capture == "month":
            self.pending_month = value
        elif (
            self._capture == "date"
            and self.current_year is not None
            and self.pending_month
        ):
            self.raw_meetings.append(
                (self.current_year, self.pending_month, value)
            )
            self.pending_month = None
        elif self._capture == "last_update":
            self.last_update = value
        self._capture = None
        self._capture_tag = None
        self._text = []


def parse_fomc_calendar(html: str) -> FomcCalendarSnapshot:
    parser = _FomcHtmlParser()
    parser.feed(html)
    by_year: dict[int, list[date]] = {}
    for year, month_label, day_label in parser.raw_meetings:
        month_name = month_label.split("/")[-1].strip()
        month = _MONTHS.get(month_name)
        days = re.findall(r"\d+", day_label)
        if month is None or not days:
            raise ValueError("Federal Reserve calendar contains an unknown meeting date")
        by_year.setdefault(year, []).append(date(year, month, int(days[-1])))
    meetings = tuple(
        FomcMeeting(year, ordinal, decision_date)
        for year in sorted(by_year)
        for ordinal, decision_date in enumerate(by_year[year], start=1)
    )
    if not meetings:
        raise ValueError("Federal Reserve calendar contains no FOMC meetings")
    updated_at = None
    if parser.last_update:
        match = re.search(r"Last Update:\s*(.+)$", parser.last_update)
        if match:
            updated_at = datetime.strptime(
                match.group(1), "%B %d, %Y"
            ).replace(tzinfo=timezone.utc)
    return FomcCalendarSnapshot(updated_at, meetings)


class FederalReserveCalendarClient:
    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or get_session()

    def fetch(self) -> FomcCalendarSnapshot:
        response = self.session.get(
            FEDERAL_RESERVE_CALENDAR_URL,
            headers={"User-Agent": "StockScreener/1.0 market-event research"},
            timeout=(5, 30),
        )
        response.raise_for_status()
        return parse_fomc_calendar(response.text)


@dataclass(frozen=True, slots=True)
class MarketEventMaterializationResult:
    source: str
    universe_size: int
    earnings_event_count: int
    fomc_event_count: int
    coverage_count: int
    persisted: OptionEventCalendarPersistResult | None
    reasons: tuple[str, ...]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _earnings_scheduled_time(row: Mapping[str, Any]) -> datetime:
    report_date = date.fromisoformat(str(row.get("date") or ""))
    event_time = {
        "bmo": time(8, 0),
        "amc": time(16, 15),
        "dmh": time(12, 0),
    }.get(str(row.get("hour") or "").lower(), time(12, 0))
    return datetime.combine(report_date, event_time, ET).astimezone(timezone.utc)


def _earnings_confidence(row: Mapping[str, Any]) -> str:
    return (
        "ESTIMATED"
        if str(row.get("hour") or "").lower() in {"bmo", "amc", "dmh"}
        else "UNKNOWN"
    )


def build_public_calendar_document(
    *,
    company_tickers: Sequence[str],
    window_start: datetime,
    window_end: datetime,
    observed_at: datetime,
    earnings_rows: Sequence[Mapping[str, Any]] | None,
    fomc: FomcCalendarSnapshot | None,
) -> dict[str, Any]:
    if window_start.tzinfo is None or window_end.tzinfo is None:
        raise ValueError("calendar coverage window must be timezone-aware")
    if window_end <= window_start:
        raise ValueError("calendar coverage window must be increasing")
    tickers = tuple(sorted({ticker.strip().upper() for ticker in company_tickers}))
    coverage: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    if earnings_rows is not None:
        coverage.extend(
            {
                "source_key": (
                    f"finnhub:coverage:{ticker}:"
                    f"{window_start.date()}:{window_end.date()}"
                ),
                "event_type": "EARNINGS",
                "affected_underlying": ticker,
                "window_start": _iso(window_start),
                "window_end": _iso(window_end),
                "upstream_source": "finnhub",
            }
            for ticker in tickers
        )
        seen_keys: set[str] = set()
        for row in earnings_rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol not in tickers:
                continue
            year = int(row.get("year"))
            quarter = int(row.get("quarter"))
            if quarter not in range(1, 5):
                raise ValueError("Finnhub earnings quarter must be in [1, 4]")
            source_key = f"finnhub:earnings:{symbol}:{year}:q{quarter}"
            if source_key in seen_keys:
                raise ValueError("Finnhub returned duplicate fiscal-period earnings")
            seen_keys.add(source_key)
            scheduled = _earnings_scheduled_time(row)
            if not window_start <= scheduled <= window_end:
                continue
            events.append({
                "source_key": source_key,
                "event_type": "EARNINGS",
                "affected_underlying": symbol,
                "scheduled_time": _iso(scheduled),
                "announcement_time": None,
                "confidence": _earnings_confidence(row),
                "status": (
                    "COMPLETED" if scheduled < observed_at else "SCHEDULED"
                ),
                "upstream_source": "finnhub",
                "provider_hour": row.get("hour"),
                "fiscal_year": year,
                "fiscal_quarter": quarter,
            })
    if fomc is not None:
        coverage.append({
            "source_key": (
                f"federal-reserve:coverage:"
                f"{window_start.date()}:{window_end.date()}"
            ),
            "event_type": "FED_RATE_DECISION",
            "affected_underlying": None,
            "window_start": _iso(window_start),
            "window_end": _iso(window_end),
            "upstream_source": "federal_reserve",
            "source_updated_at": (
                _iso(fomc.source_updated_at) if fomc.source_updated_at else None
            ),
        })
        for meeting in fomc.meetings:
            scheduled = datetime.combine(
                meeting.decision_date, time(14, 0), ET
            ).astimezone(timezone.utc)
            if not window_start <= scheduled <= window_end:
                continue
            events.append({
                "source_key": (
                    f"federal-reserve:fomc:{meeting.year}:{meeting.ordinal}"
                ),
                "event_type": "FED_RATE_DECISION",
                "affected_underlying": None,
                "scheduled_time": _iso(scheduled),
                "announcement_time": None,
                "confidence": (
                    "CONFIRMED" if scheduled < observed_at else "ESTIMATED"
                ),
                "status": (
                    "COMPLETED" if scheduled < observed_at else "SCHEDULED"
                ),
                "upstream_source": "federal_reserve",
                "source_updated_at": (
                    _iso(fomc.source_updated_at) if fomc.source_updated_at else None
                ),
            })
    return {
        "schema_version": 1,
        "source": PUBLIC_CALENDAR_SOURCE,
        "observed_at": _iso(observed_at),
        "coverage": coverage,
        "events": events,
    }


class PublicMarketEventMaterializer:
    def __init__(
        self,
        *,
        earnings_client: FinnhubEarningsClient | None,
        fomc_client: FederalReserveCalendarClient | None = None,
        repository: OptionMarketEventRepository | None = None,
    ) -> None:
        self.earnings_client = earnings_client
        self.fomc_client = fomc_client or FederalReserveCalendarClient()
        self.repository = repository or OptionMarketEventRepository()

    def materialize(
        self,
        company_tickers: Sequence[str],
        *,
        start: date,
        end: date,
        observed_at: datetime,
    ) -> MarketEventMaterializationResult:
        reasons: list[str] = []
        earnings_rows = None
        if self.earnings_client is None:
            reasons.append("FINNHUB_API_KEY_MISSING")
        else:
            try:
                earnings_rows = self.earnings_client.fetch(start, end)
                if not earnings_rows:
                    reasons.append("FINNHUB_EARNINGS_EMPTY")
                    earnings_rows = None
            except (requests.RequestException, ValueError) as exc:
                reasons.append(f"FINNHUB_EARNINGS_FAILED:{type(exc).__name__}")
        fomc = None
        try:
            fomc = self.fomc_client.fetch()
        except (requests.RequestException, ValueError) as exc:
            reasons.append(f"FEDERAL_RESERVE_CALENDAR_FAILED:{type(exc).__name__}")
        window_start = datetime.combine(start, time.min, ET).astimezone(timezone.utc)
        window_end = datetime.combine(end, time.max, ET).astimezone(timezone.utc)
        earnings_document_available = False
        try:
            document = build_public_calendar_document(
                company_tickers=company_tickers,
                window_start=window_start,
                window_end=window_end,
                observed_at=observed_at,
                earnings_rows=earnings_rows,
                fomc=fomc,
            )
            earnings_document_available = earnings_rows is not None
        except (TypeError, ValueError) as exc:
            if earnings_rows is None:
                raise
            reasons.append(f"FINNHUB_EARNINGS_INVALID:{type(exc).__name__}")
            document = build_public_calendar_document(
                company_tickers=company_tickers,
                window_start=window_start,
                window_end=window_end,
                observed_at=observed_at,
                earnings_rows=None,
                fomc=fomc,
            )
        if earnings_document_available:
            previous_events = self.repository.latest_active_events(
                source=PUBLIC_CALENDAR_SOURCE,
                event_type="EARNINGS",
                affected_underlyings=company_tickers,
                window_start=window_start,
                window_end=window_end,
                observed_at=observed_at,
            )
            current_by_key = {
                event["source_key"]: event
                for event in document["events"]
                if event["event_type"] == "EARNINGS"
            }
            for previous in previous_events:
                current = current_by_key.get(previous["source_key"])
                if current is not None:
                    if current["scheduled_time"] != _iso(previous["scheduled_time"]):
                        current["status"] = "REVISED"
                    continue
                document["events"].append({
                    "source_key": previous["source_key"],
                    "event_type": "EARNINGS",
                    "affected_underlying": previous["affected_underlying"],
                    "scheduled_time": _iso(previous["scheduled_time"]),
                    "announcement_time": None,
                    "confidence": previous["confidence"],
                    "status": "CANCELED",
                    "upstream_source": "finnhub",
                    "cancellation_reason": "ABSENT_FROM_COMPLETE_WINDOW",
                })
        batch = parse_event_calendar_document(document, received_at=observed_at)
        persisted = (
            self.repository.persist_batch(batch.coverage, batch.events)
            if batch.coverage or batch.events
            else None
        )
        return MarketEventMaterializationResult(
            source=PUBLIC_CALENDAR_SOURCE,
            universe_size=len(set(company_tickers)),
            earnings_event_count=sum(
                event.event_type.value == "EARNINGS" for event in batch.events
            ),
            fomc_event_count=sum(
                event.event_type.value == "FED_RATE_DECISION"
                for event in batch.events
            ),
            coverage_count=len(batch.coverage),
            persisted=persisted,
            reasons=tuple(reasons),
        )