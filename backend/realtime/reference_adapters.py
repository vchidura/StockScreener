from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd

from equity.domain import SecurityReferenceRevision
from equity.polygon import normalize_fundamental_reports
from market_events import FomcCalendarSnapshot, build_public_calendar_document
from options.model_inputs import parse_treasury_curve
from research.gics_sectors import MANUAL_SECTORS, sector_for_sic

from .contracts import canonical_sha256
from .reference_facts import ReferenceFact, ReferenceFactRelation
from .reference_seed import (
    ReferenceKind,
    ReferenceSeedMember,
    SeedMemberStatus,
)
from .reference_service import (
    ProviderEntitlementUnavailable,
    ReferenceSeedRequest,
    ReferenceSourceGeneration,
)


def _payload_bytes(value: object) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8"))


def _metric_snapshot(metrics) -> tuple[int, int] | None:
    if metrics is None:
        return None
    snapshot = metrics.snapshot()
    return int(snapshot.provider_requests), int(snapshot.response_bytes)


def _begin_metric_generation(metrics) -> None:
    if metrics is not None:
        begin = getattr(metrics, "begin_generation", None)
        if begin is not None:
            begin()


def _measured_counters(
    metrics,
    before: tuple[int, int] | None,
    fallback_requests: int,
    fallback_bytes: int,
) -> tuple[int, int]:
    after = _metric_snapshot(metrics)
    if before is None or after is None:
        return fallback_requests, fallback_bytes
    return after[0] - before[0], after[1] - before[1]


def _member(kind, key, source, payload, request, *, status=SeedMemberStatus.READY, reasons=()):
    digest = canonical_sha256(payload)
    return ReferenceSeedMember(
        kind,
        key,
        f"{source}:{kind.value}:{key}:{digest}",
        digest,
        status,
        request.created_at,
        request.created_at,
        tuple(reasons),
    )


def _security_id(ticker: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"realtime-security:{ticker.upper()}")


def _positive_bigint(identity: str) -> int:
    return (int(canonical_sha256(identity)[:16], 16) & ((1 << 63) - 1)) or 1


def _date_value(value: object) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ValueError(f"invalid provider date: {value}") from exc


def _decimal_value(value: object) -> Decimal | None:
    return None if value in (None, "") else Decimal(str(value))


def _effective_from(request: ReferenceSeedRequest) -> datetime:
    return datetime.combine(request.session_date, time.min, tzinfo=timezone.utc)


def _prior_market_session(session_date: date) -> date:
    calendar = exchange_calendars.get_calendar("XNYS")
    candidate = pd.Timestamp(session_date)
    session = (
        candidate
        if calendar.is_session(candidate)
        else calendar.date_to_session(candidate, direction="previous")
    )
    if session.date() >= session_date:
        session = calendar.previous_session(session)
    return session.date()


def _security_fact(
    ticker: str,
    payload: Mapping[str, object],
    request: ReferenceSeedRequest,
) -> ReferenceFact:
    body = dict(payload)
    digest = canonical_sha256(body)
    security_id = _security_id(ticker)
    provider_type = str(body.get("type") or "").upper()
    asset_type = "ETF" if provider_type == "ETF" else "INDEX" if provider_type == "INDEX" else "STOCK"
    sic_code = str(body["sic_code"]) if body.get("sic_code") else None
    effective_from = request.created_at
    revision_id = uuid5(
        NAMESPACE_URL,
        f"realtime-security-revision:{security_id}:{effective_from.isoformat()}:"
        f"{request.created_at.isoformat()}:{digest}",
    )
    return ReferenceFact.build(
        ReferenceFactRelation.SECURITY,
        ReferenceKind.SECURITY.value,
        ticker,
        security_id=security_id,
        revision_id=revision_id,
        ticker=ticker,
        asset_type=asset_type,
        source_origin="MASSIVE_TICKER_OVERVIEW_V3",
        exchange=(str(body["primary_exchange"]) if body.get("primary_exchange") else None),
        calendar_id="XNYS",
        effective_from=effective_from,
        effective_to=request.valid_until,
        observed_at=request.created_at,
        payload_sha256=digest,
        company_name=str(body["name"]) if body.get("name") else None,
        security_type=provider_type or None,
        active=bool(body.get("active", False)),
        cik=str(body["cik"]) if body.get("cik") else None,
        composite_figi=str(body["composite_figi"]) if body.get("composite_figi") else None,
        share_class_figi=str(body["share_class_figi"]) if body.get("share_class_figi") else None,
        sic_code=sic_code,
        sic_description=str(body["sic_description"]) if body.get("sic_description") else None,
        sector=sector_for_sic(sic_code) or MANUAL_SECTORS.get(ticker),
        industry=str(body["sic_description"]) if body.get("sic_description") else None,
        list_date=_date_value(body.get("list_date")),
        delisted_date=_date_value(body.get("delisted_utc")),
        weighted_shares=_decimal_value(body.get("weighted_shares_outstanding")),
        free_float=None,
        free_float_percent=None,
        market_cap=_decimal_value(body.get("market_cap")),
    )


def _security_revision(fact: ReferenceFact) -> SecurityReferenceRevision:
    row = dict(fact.values)
    return SecurityReferenceRevision(
        security_revision_id=row["revision_id"], security_id=row["security_id"],
        ticker=row["ticker"], active=bool(row["active"]), company_name=row["company_name"],
        security_type=row["security_type"], cik=row["cik"], composite_figi=row["composite_figi"],
        share_class_figi=row["share_class_figi"], primary_exchange=row["exchange"],
        sic_code=row["sic_code"], sic_description=row["sic_description"], sector=row["sector"],
        industry=row["industry"], list_date=row["list_date"], delisted_date=row["delisted_date"],
        weighted_shares=row["weighted_shares"], free_float=row["free_float"],
        free_float_percent=row["free_float_percent"], market_cap=row["market_cap"],
        source=row["source_origin"], effective_from=row["effective_from"],
        observed_at=row["observed_at"], payload_sha256=row["payload_sha256"], raw_payload_json="{}",
    )


def _fundamental_report_fact(ticker, report):
    return ReferenceFact.build(
        ReferenceFactRelation.FUNDAMENTAL_REPORT,
        ReferenceKind.FUNDAMENTALS.value,
        ticker,
        report_id=report.fundamental_report_id, security_id=report.security_id,
        security_revision_id=report.security_revision_id, cik=report.cik,
        accession_number=report.accession_number, form_type=report.form_type,
        timeframe=report.timeframe, fiscal_year=report.fiscal_year,
        fiscal_quarter=report.fiscal_quarter, period_end=report.period_end,
        filing_date=report.filing_date, availability_time=report.availability_time,
        observed_at=report.observed_at, source=report.source, source_key=report.source_key,
        metrics=json.loads(report.metrics_json), raw_payload=json.loads(report.raw_payload_json),
        payload_sha256=report.payload_sha256, quality_codes=report.quality_codes,
    )


_OPTION_TICKER = re.compile(r"^O:([A-Z.]+)(\d{6})([CP])(\d{8})$")


def _option_ticker(payload: Mapping[str, object]) -> str:
    details = payload.get("details")
    nested = details if isinstance(details, Mapping) else {}
    return str(
        payload.get("contract_ticker")
        or nested.get("ticker")
        or payload.get("ticker")
        or ""
    ).upper()


def _option_fact(
    underlyer: str,
    payload: Mapping[str, object],
    request: ReferenceSeedRequest,
) -> ReferenceFact:
    body = dict(payload)
    ticker = _option_ticker(body)
    match = _OPTION_TICKER.fullmatch(ticker)
    if match is None:
        raise ValueError(f"Massive option contract identity is invalid for {underlyer}")
    parsed_underlyer, expiration_code, parsed_call_put, strike_code = match.groups()
    payload_underlyer = str(body.get("underlying_ticker") or parsed_underlyer).upper()
    if payload_underlyer != underlyer:
        raise ValueError(f"Massive option contract underlyer drifted for {underlyer}")
    expiration = _date_value(body.get("expiration_date")) or datetime.strptime(
        expiration_code, "%y%m%d"
    ).date()
    strike = _decimal_value(body.get("strike_price")) or Decimal(str(int(strike_code))) / 1000
    contract_type = str(body.get("contract_type") or parsed_call_put).upper()
    call_put = "C" if contract_type in {"C", "CALL"} else "P" if contract_type in {"P", "PUT"} else ""
    if not call_put:
        raise ValueError(f"Massive option contract type is invalid for {ticker}")
    digest = canonical_sha256(body)
    contract_id = _positive_bigint(f"massive-option-contract:{ticker}")
    effective_from = request.created_at
    version_id = _positive_bigint(
        f"massive-option-version:{ticker}:{effective_from.isoformat()}:{digest}"
    )
    return ReferenceFact.build(
        ReferenceFactRelation.OPTION_CONTRACT,
        ReferenceKind.OPTION_CATALOG.value,
        underlyer,
        contract_id=contract_id,
        version_id=version_id,
        underlying_security_id=_security_id(underlyer),
        provider="massive",
        contract_ticker=ticker,
        first_observed_at=request.created_at,
        source_origin="MASSIVE_OPTION_REFERENCE_V3",
        call_put=call_put,
        expiration=expiration,
        strike=strike,
        exercise_style=str(body.get("exercise_style") or "AMERICAN").upper(),
        multiplier=Decimal(str(body.get("shares_per_contract") or 100)),
        deliverables=body.get("additional_underlyings") or [],
        eligibility="VALIDATED_ACTIVE",
        effective_from=effective_from,
        effective_to=request.valid_until,
        observed_at=request.created_at,
        payload_sha256=digest,
    )


def _corporate_action_fact(
    ticker: str,
    action_type: str,
    payload: Mapping[str, object],
    request: ReferenceSeedRequest,
) -> ReferenceFact:
    body = dict(payload)
    digest = canonical_sha256(body)
    source_key = str(body.get("id") or digest)
    effective_date = _date_value(
        body.get("execution_date") if action_type == "SPLIT" else body.get("ex_dividend_date")
    )
    if effective_date is None:
        raise ValueError(f"Massive {action_type.lower()} is missing its effective date")
    action_id = uuid5(
        NAMESPACE_URL,
        f"realtime-corporate-action:massive:{action_type}:{source_key}:{request.created_at.isoformat()}:{digest}",
    )
    return ReferenceFact.build(
        ReferenceFactRelation.CORPORATE_ACTION,
        ReferenceKind.CORPORATE_ACTION_COVERAGE.value,
        ticker,
        action_id=action_id,
        security_id=_security_id(ticker),
        ticker=ticker,
        action_type=action_type,
        effective_date=effective_date,
        declaration_date=_date_value(body.get("declaration_date")),
        ex_date=_date_value(body.get("ex_dividend_date")),
        record_date=_date_value(body.get("record_date")),
        pay_date=_date_value(body.get("pay_date")),
        cash_amount=_decimal_value(body.get("cash_amount")),
        split_from=_decimal_value(body.get("split_from")),
        split_to=_decimal_value(body.get("split_to")),
        new_ticker=(str(body["new_ticker"]).upper() if body.get("new_ticker") else None),
        source="massive",
        source_key=source_key,
        observed_at=request.created_at,
        received_at=request.created_at,
        payload_sha256=digest,
        raw_payload=body,
        supersedes_action_id=None,
    )


def _corporate_action_coverage_fact(
    ticker: str,
    action_type: str,
    rows: tuple[Mapping[str, object], ...],
    start: date,
    end: date,
    request: ReferenceSeedRequest,
) -> ReferenceFact:
    response_sha256 = canonical_sha256(rows)
    source_key = f"massive:{action_type.lower()}:{ticker}:{start}:{end}"
    payload = {
        "source_key": source_key,
        "response_action_count": len(rows),
        "response_sha256": response_sha256,
    }
    payload_sha256 = canonical_sha256(payload)
    coverage_id = uuid5(
        NAMESPACE_URL,
        f"realtime-corporate-action-coverage:{source_key}:{request.created_at.isoformat()}:{payload_sha256}",
    )
    return ReferenceFact.build(
        ReferenceFactRelation.CORPORATE_ACTION_COVERAGE,
        ReferenceKind.CORPORATE_ACTION_COVERAGE.value,
        ticker,
        coverage_id=coverage_id,
        security_id=_security_id(ticker),
        action_type=action_type,
        window_start=start,
        window_end=end,
        source="massive",
        source_key=source_key,
        response_action_count=len(rows),
        response_sha256=response_sha256,
        observed_at=request.created_at,
        received_at=request.created_at,
        payload_sha256=payload_sha256,
    )


def _datetime_value(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("provider datetime must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _calendar_facts(
    document: Mapping[str, object],
    request: ReferenceSeedRequest,
    *,
    source: str,
    member_kind: ReferenceKind,
) -> tuple[ReferenceFact, ...]:
    facts = []
    events = document.get("events")
    coverage = document.get("coverage")
    if not isinstance(events, Sequence) or not isinstance(coverage, Sequence):
        raise ValueError(f"{source} calendar document has an invalid shape")
    event_scopes = {
        (
            str(row.get("event_type") or ""),
            str(row.get("affected_underlying") or "GLOBAL").upper(),
        )
        for row in events
        if isinstance(row, Mapping)
    }
    for raw in events:
        if not isinstance(raw, Mapping):
            raise ValueError(f"{source} calendar event must be an object")
        row = dict(raw)
        member_key = str(row.get("affected_underlying") or "GLOBAL").upper()
        scheduled = _datetime_value(row.get("scheduled_time"))
        if scheduled is None:
            raise ValueError(f"{source} calendar event is missing its scheduled time")
        digest = canonical_sha256(row)
        event_key = str(row.get("source_key") or "")
        revision_id = uuid5(
            NAMESPACE_URL,
            f"realtime-market-event:{source}:{event_key}:{request.created_at.isoformat()}:{digest}",
        )
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.MARKET_EVENT,
            member_kind.value,
            member_key,
            revision_id=revision_id,
            security_id=(_security_id(member_key) if member_key != "GLOBAL" else None),
            event_key=event_key,
            event_type=str(row.get("event_type") or ""),
            event_date=scheduled.date(),
            announced_at=_datetime_value(row.get("announcement_time")),
            event_timezone="America/New_York",
            time_precision=str(row.get("confidence") or "UNKNOWN"),
            status=str(row.get("status") or "SCHEDULED"),
            source=source,
            observed_at=request.created_at,
            received_at=request.created_at,
            valid_until=request.valid_until,
            payload_sha256=digest,
            supersedes_revision_id=None,
        ))
    for raw in coverage:
        if not isinstance(raw, Mapping):
            raise ValueError(f"{source} calendar coverage must be an object")
        row = dict(raw)
        member_key = str(row.get("affected_underlying") or "GLOBAL").upper()
        event_type = str(row.get("event_type") or "")
        digest = canonical_sha256(row)
        source_key = str(row.get("source_key") or "")
        coverage_id = uuid5(
            NAMESPACE_URL,
            f"realtime-market-event-coverage:{source}:{source_key}:"
            f"{request.created_at.isoformat()}:{digest}",
        )
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.MARKET_EVENT_COVERAGE,
            member_kind.value,
            member_key,
            coverage_id=coverage_id,
            security_id=(_security_id(member_key) if member_key != "GLOBAL" else None),
            event_type=event_type,
            window_start=_date_value(row.get("window_start")),
            window_end=_date_value(row.get("window_end")),
            source=source,
            verdict=(
                "COVERED"
                if (event_type, member_key) in event_scopes
                else "KNOWN_NO_EVENT"
            ),
            checked_at=request.created_at,
            valid_until=request.valid_until,
            evidence_sha256=digest,
        ))
    return tuple(facts)


def _etf_earnings_coverage_fact(
    ticker: str,
    request: ReferenceSeedRequest,
) -> ReferenceFact:
    window_start = (request.created_at - timedelta(days=1)).date()
    window_end = (request.created_at + timedelta(days=45)).date()
    evidence = {
        "ticker": ticker,
        "verdict": "NOT_APPLICABLE",
        "window_start": window_start,
        "window_end": window_end,
    }
    digest = canonical_sha256(evidence)
    return ReferenceFact.build(
        ReferenceFactRelation.MARKET_EVENT_COVERAGE,
        ReferenceKind.EARNINGS_COVERAGE.value,
        ticker,
        coverage_id=uuid5(
            NAMESPACE_URL,
            f"realtime-market-event-coverage:finnhub:etf:{ticker}:"
            f"{request.created_at.isoformat()}:{digest}",
        ),
        security_id=_security_id(ticker),
        event_type="EARNINGS",
        window_start=window_start,
        window_end=window_end,
        source="finnhub",
        verdict="NOT_APPLICABLE",
        checked_at=request.created_at,
        valid_until=request.valid_until,
        evidence_sha256=digest,
    )


class MassiveReferenceAdapter:
    source = "massive"

    def __init__(
        self,
        *,
        fetch_security: Callable[[str, object], Mapping[str, object] | None],
        fetch_splits: Callable[[str, object, object], Sequence[Mapping[str, object]]],
        fetch_dividends: Callable[[str, object, object], Sequence[Mapping[str, object]]],
        fetch_option_catalog: Callable[[str, object], Sequence[Mapping[str, object]]],
        fetch_open_interest: Callable[
            [str, tuple[str, ...], object], Sequence[Mapping[str, object]]
        ],
        fetch_income_statements: Callable[[tuple[str, ...], object], Sequence[Mapping[str, object]]] | None = None,
        fetch_balance_sheets: Callable[[tuple[str, ...], object], Sequence[Mapping[str, object]]] | None = None,
        fetch_cash_flow_statements: Callable[[tuple[str, ...], object], Sequence[Mapping[str, object]]] | None = None,
        fetch_filing_index: Callable[[tuple[str, ...], object], Sequence[Mapping[str, object]]] | None = None,
        maximum_rows_per_response: int = 5000,
        corporate_action_lookback_days: int = 180,
        metrics=None,
    ) -> None:
        if not 180 <= corporate_action_lookback_days <= 730:
            raise ValueError("corporate action lookback must be in [180, 730]")
        self.fetch_security = fetch_security
        self.fetch_splits = fetch_splits
        self.fetch_dividends = fetch_dividends
        self.fetch_option_catalog = fetch_option_catalog
        self.fetch_open_interest = fetch_open_interest
        self.fetch_income_statements = fetch_income_statements
        self.fetch_balance_sheets = fetch_balance_sheets
        self.fetch_cash_flow_statements = fetch_cash_flow_statements
        self.fetch_filing_index = fetch_filing_index
        self.maximum_rows_per_response = maximum_rows_per_response
        self.corporate_action_lookback_days = corporate_action_lookback_days
        self.metrics = metrics

    def _bounded(self, rows, label):
        rows = tuple(rows)
        if len(rows) > self.maximum_rows_per_response or any(not isinstance(row, Mapping) for row in rows):
            raise ValueError(f"Massive {label} response exceeds bounds or shape")
        return rows

    def collect(self, request: ReferenceSeedRequest) -> ReferenceSourceGeneration:
        _begin_metric_generation(self.metrics)
        metric_start = _metric_snapshot(self.metrics)
        payloads = []
        members = []
        facts = []
        requests = 0
        security_facts = {}
        for ticker in request.universe.equity_tickers:
            payload = self.fetch_security(ticker, request.session_date)
            requests += 1
            if not isinstance(payload, Mapping) or str(payload.get("ticker") or "").upper() != ticker:
                raise ValueError(f"Massive security identity unavailable for {ticker}")
            body = dict(payload)
            payloads.append(body)
            members.append(_member(ReferenceKind.SECURITY, ticker, self.source, body, request))
            security_fact = _security_fact(ticker, body, request)
            facts.append(security_fact)
            security_facts[ticker] = security_fact
        fundamental_fetchers = (
            self.fetch_income_statements, self.fetch_balance_sheets,
            self.fetch_cash_flow_statements, self.fetch_filing_index,
        )
        if any(fundamental_fetchers) and not all(fundamental_fetchers):
            raise ValueError("Massive fundamental acquisition requires all four bounded readers")
        if all(fundamental_fetchers):
            tickers = tuple(request.universe.equity_tickers)
            try:
                income_rows = self._bounded(self.fetch_income_statements(tickers, request.session_date), "income statements")
                balance_rows = self._bounded(self.fetch_balance_sheets(tickers, request.session_date), "balance sheets")
                cash_flow_rows = self._bounded(self.fetch_cash_flow_statements(tickers, request.session_date), "cash flow statements")
                filing_rows = self._bounded(self.fetch_filing_index(tickers, request.session_date), "filing index")
            except ProviderEntitlementUnavailable:
                requests += 1
                for ticker in tickers:
                    response = {
                        "ticker": ticker,
                        "status": "UNAVAILABLE",
                        "reason": "POLYGON_FINANCIALS_ENTITLEMENT_UNAVAILABLE",
                    }
                    payloads.append(response)
                    members.append(_member(
                        ReferenceKind.FUNDAMENTALS, ticker, self.source, response, request,
                        status=SeedMemberStatus.UNAVAILABLE,
                        reasons=("POLYGON_FINANCIALS_ENTITLEMENT_UNAVAILABLE",),
                    ))
            else:
                requests += 4
                payloads.extend((income_rows, balance_rows, cash_flow_rows, filing_rows))
                window_start = request.session_date - timedelta(days=366 * 5)
                for ticker in tickers:
                    reports = normalize_fundamental_reports(
                        _security_revision(security_facts[ticker]),
                        income_rows=income_rows, balance_rows=balance_rows,
                        cash_flow_rows=cash_flow_rows, filing_rows=filing_rows,
                        observed_at=request.created_at,
                    )
                    report_facts = tuple(_fundamental_report_fact(ticker, report) for report in reports)
                    response = {
                        "ticker": ticker, "window_start": window_start, "window_end": request.session_date,
                        "report_hashes": sorted(report.payload_sha256 for report in reports),
                    }
                    digest = canonical_sha256(response)
                    members.append(_member(ReferenceKind.FUNDAMENTALS, ticker, self.source, response, request))
                    facts.extend(report_facts)
                    facts.append(ReferenceFact.build(
                        ReferenceFactRelation.FUNDAMENTAL_COVERAGE,
                        ReferenceKind.FUNDAMENTALS.value,
                        ticker,
                        coverage_id=uuid5(
                            NAMESPACE_URL,
                            f"realtime-fundamental-coverage:{ticker}:{request.created_at.isoformat()}:{digest}",
                        ),
                        security_id=dict(security_facts[ticker].values)["security_id"],
                        window_start=window_start, window_end=request.session_date,
                        source="massive", report_count=len(reports), response_sha256=digest,
                        checked_at=request.created_at, valid_until=request.valid_until,
                    ))
        start = request.session_date - timedelta(
            days=self.corporate_action_lookback_days
        )
        end = request.session_date + timedelta(days=365)
        for ticker in request.universe.equity_tickers:
            ticker_splits = self._bounded(
                self.fetch_splits(ticker, start, end),
                f"{ticker} splits",
            )
            ticker_dividends = self._bounded(
                self.fetch_dividends(ticker, start, end),
                f"{ticker} dividends",
            )
            if any(str(row.get("ticker") or "").upper() != ticker for row in ticker_splits):
                raise ValueError(f"Massive splits identity drifted for {ticker}")
            if any(
                str(row.get("ticker") or "").upper() != ticker
                for row in ticker_dividends
            ):
                raise ValueError(f"Massive dividends identity drifted for {ticker}")
            requests += 2
            payloads.extend((ticker_splits, ticker_dividends))
            response = {
                "ticker": ticker,
                "split_hashes": sorted(canonical_sha256(row) for row in ticker_splits),
                "dividend_hashes": sorted(canonical_sha256(row) for row in ticker_dividends),
                "window_start": start,
                "window_end": end,
            }
            members.append(_member(ReferenceKind.CORPORATE_ACTION_COVERAGE, ticker, self.source, response, request))
            facts.extend(
                _corporate_action_fact(ticker, "SPLIT", row, request)
                for row in ticker_splits
            )
            facts.extend(
                _corporate_action_fact(ticker, "DIVIDEND", row, request)
                for row in ticker_dividends
            )
            facts.append(_corporate_action_coverage_fact(
                ticker, "SPLIT", ticker_splits, start, end, request
            ))
            facts.append(_corporate_action_coverage_fact(
                ticker, "DIVIDEND", ticker_dividends, start, end, request
            ))
        settlement_session = _prior_market_session(request.session_date)
        for ticker in request.universe.option_underlyings:
            catalog = self._bounded(self.fetch_option_catalog(ticker, request.session_date), "option catalog")
            if len(catalog) > 1:
                raise ValueError(f"Massive option manifest is not singular for {ticker}")
            contract_tickers = tuple(
                sorted(_option_ticker(row) for row in catalog)
            )
            oi = self._bounded(
                self.fetch_open_interest(
                    ticker,
                    contract_tickers,
                    request.session_date,
                ),
                "open interest",
            )
            requests += 2
            payloads.extend((catalog, oi))
            catalog_facts = tuple(_option_fact(ticker, row, request) for row in catalog)
            contract_ids = {
                str(dict(row.values)["contract_ticker"]): dict(row.values)["contract_id"]
                for row in catalog_facts
            }
            oi_facts = []
            for row in oi:
                contract_ticker = _option_ticker(row)
                contract_id = contract_ids.get(contract_ticker)
                if contract_id is None:
                    raise ValueError(
                        f"Massive OI contract is absent from the {ticker} catalog"
                    )
                open_interest = row.get("open_interest")
                if not isinstance(open_interest, int) or open_interest < 0:
                    raise ValueError(f"Massive OI value is invalid for {contract_ticker}")
                digest = canonical_sha256(dict(row))
                oi_facts.append(ReferenceFact.build(
                    ReferenceFactRelation.OI_BASELINE,
                    ReferenceKind.OI_COVERAGE.value,
                    ticker,
                    baseline_id=uuid5(
                        NAMESPACE_URL,
                        f"realtime-oi:massive:{contract_ticker}:{settlement_session}:"
                        f"{request.created_at.isoformat()}:{digest}",
                    ),
                    contract_id=contract_id,
                    settlement_session=settlement_session,
                    open_interest=open_interest,
                    source="massive",
                    date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
                    observed_session=request.session_date,
                    observed_at=request.created_at,
                    received_at=request.created_at,
                    payload_sha256=digest,
                    supersedes_baseline_id=None,
                ))
            facts.extend(catalog_facts)
            facts.extend(oi_facts)
            catalog_status = SeedMemberStatus.READY if catalog_facts else SeedMemberStatus.UNAVAILABLE
            oi_status = SeedMemberStatus.READY if oi_facts else SeedMemberStatus.UNAVAILABLE
            members.append(_member(
                ReferenceKind.OPTION_CATALOG,
                ticker,
                self.source,
                catalog,
                request,
                status=catalog_status,
                reasons=(() if catalog else ("OPTION_CATALOG_EMPTY",)),
            ))
            members.append(_member(
                ReferenceKind.OI_COVERAGE,
                ticker,
                self.source,
                oi,
                request,
                status=oi_status,
                reasons=(() if oi else ("OI_BASELINE_EMPTY",)),
            ))
        provider_requests, response_bytes = _measured_counters(
            self.metrics,
            metric_start,
            requests,
            sum(_payload_bytes(payload) for payload in payloads),
        )
        return ReferenceSourceGeneration(
            self.source,
            canonical_sha256(payloads),
            tuple(members),
            provider_requests,
            response_bytes,
            tuple(facts),
        )


class FinnhubReferenceAdapter:
    source = "finnhub"

    def __init__(
        self,
        fetch: Callable[[object, object], Sequence[Mapping[str, object]]],
        *,
        etf_tickers: frozenset[str],
        metrics=None,
    ) -> None:
        self.fetch = fetch
        self.etf_tickers = etf_tickers
        self.metrics = metrics

    def collect(self, request: ReferenceSeedRequest) -> ReferenceSourceGeneration:
        _begin_metric_generation(self.metrics)
        metric_start = _metric_snapshot(self.metrics)
        companies = tuple(ticker for ticker in request.universe.equity_tickers if ticker not in self.etf_tickers)
        start = request.session_date - timedelta(days=1)
        end = request.session_date + timedelta(days=45)
        rows = tuple(self.fetch(start, end))
        document = build_public_calendar_document(
            company_tickers=companies,
            window_start=request.created_at - timedelta(days=1),
            window_end=request.created_at + timedelta(days=45),
            observed_at=request.created_at,
            earnings_rows=rows,
            fomc=None,
        )
        members = []
        facts = list(_calendar_facts(
            document,
            request,
            source=self.source,
            member_kind=ReferenceKind.EARNINGS_COVERAGE,
        ))
        for ticker in request.universe.equity_tickers:
            if ticker in self.etf_tickers:
                members.append(_member(
                    ReferenceKind.EARNINGS_COVERAGE,
                    ticker,
                    self.source,
                    {"ticker": ticker, "verdict": "VERIFIED_ETF"},
                    request,
                    status=SeedMemberStatus.NOT_APPLICABLE,
                    reasons=("VERIFIED_ETF",),
                ))
                facts.append(_etf_earnings_coverage_fact(ticker, request))
            else:
                coverage = tuple(row for row in document["coverage"] if row["affected_underlying"] == ticker)
                status = SeedMemberStatus.READY if coverage else SeedMemberStatus.UNAVAILABLE
                members.append(_member(
                    ReferenceKind.EARNINGS_COVERAGE,
                    ticker,
                    self.source,
                    coverage,
                    request,
                    status=status,
                    reasons=(() if coverage else ("EARNINGS_COVERAGE_MISSING",)),
                ))
        provider_requests, response_bytes = _measured_counters(
            self.metrics,
            metric_start,
            1,
            _payload_bytes(document),
        )
        return ReferenceSourceGeneration(
            self.source,
            canonical_sha256(document),
            tuple(members),
            provider_requests,
            response_bytes,
            tuple(facts),
        )


class FederalReserveReferenceAdapter:
    source = "federal_reserve"

    def __init__(self, fetch: Callable[[], FomcCalendarSnapshot], *, metrics=None) -> None:
        self.fetch = fetch
        self.metrics = metrics

    def collect(self, request: ReferenceSeedRequest) -> ReferenceSourceGeneration:
        _begin_metric_generation(self.metrics)
        metric_start = _metric_snapshot(self.metrics)
        snapshot = self.fetch()
        document = build_public_calendar_document(
            company_tickers=(),
            window_start=request.created_at - timedelta(days=1),
            window_end=request.created_at + timedelta(days=365),
            observed_at=request.created_at,
            earnings_rows=None,
            fomc=snapshot,
        )
        coverage = tuple(row for row in document["coverage"] if row["event_type"] == "FED_RATE_DECISION")
        status = SeedMemberStatus.READY if coverage else SeedMemberStatus.UNAVAILABLE
        member = _member(
            ReferenceKind.FOMC_COVERAGE,
            "GLOBAL",
            self.source,
            document,
            request,
            status=status,
            reasons=(() if coverage else ("FOMC_COVERAGE_MISSING",)),
        )
        provider_requests, response_bytes = _measured_counters(
            self.metrics,
            metric_start,
            1,
            _payload_bytes(document),
        )
        return ReferenceSourceGeneration(
            self.source,
            canonical_sha256(document),
            (member,),
            provider_requests,
            response_bytes,
            _calendar_facts(
                document,
                request,
                source=self.source,
                member_kind=ReferenceKind.FOMC_COVERAGE,
            ),
        )


class TreasuryReferenceAdapter:
    source = "treasury"

    def __init__(self, fetch_xml: Callable[[int], str], *, metrics=None) -> None:
        self.fetch_xml = fetch_xml
        self.metrics = metrics

    def collect(self, request: ReferenceSeedRequest) -> ReferenceSourceGeneration:
        _begin_metric_generation(self.metrics)
        metric_start = _metric_snapshot(self.metrics)
        xml = self.fetch_xml(request.session_date.year)
        observations = parse_treasury_curve(xml, received_at=request.created_at)
        latest_date = max(row.rate_date for row in observations)
        latest = tuple(row for row in observations if row.rate_date == latest_date)
        member = _member(
            ReferenceKind.RATE_CURVE,
            "USD",
            self.source,
            latest,
            request,
        )
        provider_requests, response_bytes = _measured_counters(
            self.metrics,
            metric_start,
            1,
            len(xml.encode("utf-8")),
        )
        return ReferenceSourceGeneration(
            self.source,
            canonical_sha256(latest),
            (member,),
            provider_requests,
            response_bytes,
            tuple(
                ReferenceFact.build(
                    ReferenceFactRelation.RATE_OBSERVATION,
                    ReferenceKind.RATE_CURVE.value,
                    "USD",
                    observation_id=row.rate_observation_id,
                    rate_date=row.rate_date,
                    tenor_days=row.tenor_days,
                    annual_rate=row.annual_rate,
                    source=row.source,
                    source_key=row.source_key,
                    source_observed_at=row.source_observed_at,
                    received_at=row.first_observed_at,
                    payload_sha256=row.payload_sha256,
                )
                for row in latest
            ),
        )