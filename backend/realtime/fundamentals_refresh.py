"""Financials tab data (G37, docs/FINANCIALS_TAB_TASK.md part 2): filed statements from Massive for the current desired
revision's companies.

When (R10, Nov 11): the preparation role checks in its first idle slot after a start and then every 30 minutes
(`FED_CALENDAR_CHECK_INTERVAL`); it fetches only when due (`fundamentals_refresh_due_cursor`: a company without valid
coverage, or whose newest check predates the last completed close), and a failed fetch waits
`FUNDAMENTALS_RETRY_BACKOFF` (1 hour). So it normally runs once after each close, but a start or deploy before the open
whose coverage predates the last close fetches statements at once (about 4 provider requests).

The reader, normalizer, storage (`ref.fundamental_reports`, `ref.fundamental_report_coverage`), ticker projection and
Financials table already existed and were switched off. This turns them on without touching anything else: only report
and coverage facts are written (never security revisions or corporate-action coverage, which drive screening), and each
report points at the member's current stored security revision. A report is visible from its filing availability time
(point in time). Nothing here can stop the role or a window: a failed fetch leaves the tab as it was.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import SecurityReferenceRevision
from equity.polygon import normalize_fundamental_reports
from options.calendar import OptionExchangeCalendar

from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE
from .earnings_calendar_refresh import _universe, desired_equity_members_cursor
from .fed_calendar_refresh import latest_completed_session_close
from .reference_adapters import _fundamental_report_fact
from .reference_facts import ReferenceFact, ReferenceFactRelation
from .reference_repository import persist_reference_facts_cursor
from .reference_seed import ReferenceKind

FUNDAMENTALS_VALIDITY = timedelta(days=3)
FUNDAMENTALS_LOOKBACK = timedelta(days=366 * 5)
FUNDAMENTALS_RETRY_BACKOFF = timedelta(hours=1)
PROVIDER_KEY_FILE = Path("/run/secrets/realtime_v2_provider_key")
# R60 (Nov 13): one batched request per endpoint for every company passed the transport's 2 MB per-response cap once the
# list grew to 35 companies (income statements; ValueError at 07:26). Companies are fetched in groups of
# FUNDAMENTALS_BATCH (live Nov 13: about 0.6 MB per group on the largest endpoint, 20 requests for 35 companies), and
# the refresh's own transport caps cover a 400-name list (about 50 groups x 4 endpoints).
FUNDAMENTALS_BATCH = 8
FUNDAMENTALS_MAXIMUM_REQUESTS = 512
FUNDAMENTALS_MAXIMUM_TOTAL_BYTES = 128_000_000
FUNDAMENTALS_MAXIMUM_ROWS = 400_000
_last_attempt: dict[str, datetime] = {}


def stored_security_revisions_cursor(cursor, tickers: Sequence[str]) -> dict[str, SecurityReferenceRevision]:
    """The current stored revision per ticker; reports reference it, so no new security revision is written."""
    cursor.execute(
        "SELECT DISTINCT ON (ticker) revision_id,security_id,ticker,exchange,effective_from,observed_at,"
        "payload_sha256,company_name,security_type,active,cik,composite_figi,share_class_figi,sic_code,"
        "sic_description,sector,industry,list_date,delisted_date,weighted_shares,free_float,free_float_percent,"
        "market_cap FROM ref.security_revisions WHERE active AND ticker=ANY(%s) "
        "ORDER BY ticker,effective_from DESC,observed_at DESC,revision_id",
        (list(tickers),),
    )
    names = ("revision_id", "security_id", "ticker", "exchange", "effective_from", "observed_at", "payload_sha256",
             "company_name", "security_type", "active", "cik", "composite_figi", "share_class_figi", "sic_code",
             "sic_description", "sector", "industry", "list_date", "delisted_date", "weighted_shares", "free_float",
             "free_float_percent", "market_cap")
    revisions = {}
    for values in cursor.fetchall():
        row = dict(values) if isinstance(values, Mapping) else dict(zip(names, values))
        revisions[str(row["ticker"]).upper()] = SecurityReferenceRevision(
            security_revision_id=UUID(str(row["revision_id"])), security_id=UUID(str(row["security_id"])),
            ticker=str(row["ticker"]).upper(), active=bool(row["active"]), company_name=row["company_name"],
            security_type=row["security_type"], cik=row["cik"], composite_figi=row["composite_figi"],
            share_class_figi=row["share_class_figi"], primary_exchange=row["exchange"], sic_code=row["sic_code"],
            sic_description=row["sic_description"], sector=row["sector"], industry=row["industry"],
            list_date=row["list_date"], delisted_date=row["delisted_date"], weighted_shares=row["weighted_shares"],
            free_float=row["free_float"],
            free_float_percent=None if row["free_float_percent"] is None else float(row["free_float_percent"]),
            market_cap=row["market_cap"], source="massive", effective_from=row["effective_from"],
            observed_at=row["observed_at"], payload_sha256=row["payload_sha256"], raw_payload_json="{}",
        )
    return revisions


def collect_fundamental_facts(
    securities: Mapping[str, SecurityReferenceRevision],
    *,
    fetch_income_statements: Callable,
    fetch_balance_sheets: Callable,
    fetch_cash_flow_statements: Callable,
    fetch_filing_index: Callable,
    created_at: datetime,
    valid_until: datetime,
) -> tuple[list[ReferenceFact], dict[str, int]]:
    """Report facts and one coverage fact per company, from one batched request per endpoint and group of
    FUNDAMENTALS_BATCH companies (R60)."""
    tickers = tuple(sorted(securities))
    if not tickers:
        return [], {}
    as_of = created_at.date()
    groups = tuple(tickers[start:start + FUNDAMENTALS_BATCH] for start in range(0, len(tickers), FUNDAMENTALS_BATCH))

    def fetched(fetch: Callable) -> tuple:
        return tuple(row for group in groups for row in fetch(group, as_of))

    income = fetched(fetch_income_statements)
    balance = fetched(fetch_balance_sheets)
    cash_flow = fetched(fetch_cash_flow_statements)
    filings = fetched(fetch_filing_index)
    window_start = as_of - FUNDAMENTALS_LOOKBACK
    facts: list[ReferenceFact] = []
    counts: dict[str, int] = {}
    for ticker in tickers:
        security = securities[ticker]
        reports = normalize_fundamental_reports(
            security, income_rows=income, balance_rows=balance, cash_flow_rows=cash_flow,
            filing_rows=filings, observed_at=created_at,
        )
        counts[ticker] = len(reports)
        facts.extend(_fundamental_report_fact(ticker, report) for report in reports)
        response = {
            "ticker": ticker, "window_start": window_start, "window_end": as_of,
            "report_hashes": sorted(report.payload_sha256 for report in reports),
        }
        digest = canonical_sha256(response)
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.FUNDAMENTAL_COVERAGE,
            ReferenceKind.FUNDAMENTALS.value,
            ticker,
            coverage_id=uuid5(NAMESPACE_URL, f"realtime-fundamental-coverage:{ticker}:{created_at.isoformat()}:{digest}"),
            security_id=security.security_id,
            window_start=window_start, window_end=as_of,
            source="massive", report_count=len(reports), response_sha256=digest,
            checked_at=created_at, valid_until=valid_until,
        ))
    return facts, counts


def fundamentals_refresh_due_cursor(cursor, *, now: datetime, calendar: OptionExchangeCalendar,
                                    securities: Mapping[str, SecurityReferenceRevision]) -> bool:
    """Due when a company has no valid coverage, or the newest check predates the last close."""
    if not securities:
        return False
    cursor.execute(
        "SELECT security_id::text AS security_id,max(checked_at) AS checked_at,max(valid_until) AS valid_until "
        "FROM ref.fundamental_report_coverage WHERE security_id=ANY(%s::uuid[]) AND checked_at<=%s GROUP BY 1",
        ([str(security.security_id) for security in securities.values()], now),
    )
    rows = {}
    for values in cursor.fetchall():
        row = dict(values) if isinstance(values, Mapping) else dict(zip(("security_id", "checked_at", "valid_until"),
                                                                           values))
        rows[str(row["security_id"])] = row
    last_close = latest_completed_session_close(now, calendar)
    for security in securities.values():
        row = rows.get(str(security.security_id)) or {}
        checked, valid = row.get("checked_at"), row.get("valid_until")
        if checked is None or valid is None or valid <= now or checked < last_close:
            return True
    return False


def refresh_fundamentals(provider, *, clock: Callable[[], datetime], transport,
                         calendar: OptionExchangeCalendar | None = None) -> dict[str, object]:
    exchange = calendar or OptionExchangeCalendar("XNYS")
    now = clock()
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        universe, etfs = _universe(desired_equity_members_cursor(unit.cursor))
        companies = tuple(ticker for ticker in universe.equity_tickers if ticker not in etfs)
        securities = stored_security_revisions_cursor(unit.cursor, companies)
        due = fundamentals_refresh_due_cursor(unit.cursor, now=now, calendar=exchange, securities=securities)
    if not due:
        return {"fundamentals": "CURRENT"}
    previous = _last_attempt.get("massive")
    if previous is not None and now - previous < FUNDAMENTALS_RETRY_BACKOFF:
        return {"fundamentals": "BACKOFF"}
    _last_attempt["massive"] = now
    facts, counts = collect_fundamental_facts(
        securities,
        fetch_income_statements=transport.fetch_income_statements,
        fetch_balance_sheets=transport.fetch_balance_sheets,
        fetch_cash_flow_statements=transport.fetch_cash_flow_statements,
        fetch_filing_index=transport.fetch_filing_index,
        created_at=now, valid_until=now + FUNDAMENTALS_VALIDITY,
    )
    with provider.transaction(APP_ROLE) as unit:
        persist_reference_facts_cursor(unit.cursor, facts)
    return {
        "fundamentals": "REFRESHED",
        "companies": len(securities),
        "missing_identity": sorted(set(companies) - set(securities)),
        "reports": sum(counts.values()),
        "without_reports": sorted(ticker for ticker, count in counts.items() if count == 0),
    }


def run_fundamentals_refresh(provider, *, clock: Callable[[], datetime], api_key: str | None = None,
                             key_file: Path = PROVIDER_KEY_FILE, transport=None) -> dict[str, object]:
    """The maintenance slot's entry: never raises, and only an error's type is reported (the key never reaches a
    log). `api_key` is the role's own Massive key; `key_file` is the fallback."""
    try:
        if transport is None:
            key = (api_key or "").strip()
            if not key and key_file.is_file():
                key = key_file.read_text(encoding="utf-8").strip()
            if not key:
                return {"fundamentals": "NOT_CONFIGURED"}
            from .reference_transports import (
                MassiveReferenceTransport,
                ReferenceTransportLimits,
                RequestsReferenceHttpTransport,
            )

            transport = MassiveReferenceTransport(key, RequestsReferenceHttpTransport(), limits=ReferenceTransportLimits(
                maximum_requests=FUNDAMENTALS_MAXIMUM_REQUESTS, maximum_total_bytes=FUNDAMENTALS_MAXIMUM_TOTAL_BYTES,
                maximum_rows=FUNDAMENTALS_MAXIMUM_ROWS,
            ))
        return refresh_fundamentals(provider, clock=clock, transport=transport)
    except Exception as error:  # noqa: BLE001 - the Financials tab never blocks the role
        return {"fundamentals": "FAILED", "error_type": type(error).__name__}


def summary_json(result: Mapping[str, object]) -> str:
    return json.dumps(dict(result), sort_keys=True, default=str)
