"""Polygon/Massive equity reference, fundamentals, and native-bar ingestion."""
from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd
import requests

from http_client import get_session
from research.gics_sectors import MANUAL_SECTORS, sector_for_sic

from .domain import (
    BarAvailabilityMode,
    BarSourceKind,
    EquityCorporateAction,
    EquityBarRevision,
    FundamentalReport,
    SecurityReferenceRevision,
)


POLYGON_BASE_URL = "https://api.polygon.io"
REQUEST_TIMEOUT_SECONDS = 30
_ALLOWED_API_HOSTS = {"api.polygon.io", "api.massive.com"}
_INTRADAY_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30}
_NATIVE_AGGREGATES = {
    **{interval: (minutes, "minute") for interval, minutes in _INTRADAY_MINUTES.items()},
    "1mo": (1, "month"),
}


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


class PolygonEquityClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        session=None,
        base_url: str = POLYGON_BASE_URL,
        timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self.api_key = api_key or os.getenv("POLYGON_API_KEY", "")
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.session = session or get_session()
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fork(self) -> PolygonEquityClient:
        """Create an equivalent client with an independent HTTP session."""
        return type(self)(
            self.api_key,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
        )

    def fetch_ticker_overview(
        self, ticker: str, *, as_of_date: date | None = None
    ) -> dict[str, Any] | None:
        params = {"date": as_of_date.isoformat()} if as_of_date else {}
        payload = self._get_json(f"/v3/reference/tickers/{ticker.upper()}", params)
        result = payload.get("results")
        return dict(result) if isinstance(result, dict) else None

    def fetch_tickers_as_of(
        self,
        as_of_date: date,
        *,
        market: str = "stocks",
        security_type: str = "CS",
        active: bool = True,
    ) -> tuple[dict[str, Any], ...]:
        return self._get_results(
            "/v3/reference/tickers",
            {
                "date": as_of_date.isoformat(),
                "market": market,
                "type": security_type.upper(),
                "active": str(active).lower(),
                "limit": 1000,
                "sort": "ticker",
                "order": "asc",
            },
        )

    def fetch_grouped_daily(
        self, session_date: date, *, adjusted: bool = False
    ) -> tuple[dict[str, Any], ...]:
        return self._get_results(
            f"/v2/aggs/grouped/locale/us/market/stocks/{session_date.isoformat()}",
            {"adjusted": str(adjusted).lower()},
        )

    def fetch_splits(
        self, start_date: date, end_date: date
    ) -> tuple[dict[str, Any], ...]:
        return self._get_results(
            "/stocks/v1/splits",
            {
                "execution_date.gte": start_date.isoformat(),
                "execution_date.lte": end_date.isoformat(),
                "limit": 5000,
                "sort": "execution_date.asc,ticker.asc",
            },
        )

    def fetch_dividends(
        self, start_date: date, end_date: date
    ) -> tuple[dict[str, Any], ...]:
        return self._get_results(
            "/stocks/v1/dividends",
            {
                "ex_dividend_date.gte": start_date.isoformat(),
                "ex_dividend_date.lte": end_date.isoformat(),
                "limit": 5000,
                "sort": "ex_dividend_date.asc,ticker.asc",
            },
        )

    def fetch_float(self, tickers: Sequence[str]) -> tuple[dict[str, Any], ...]:
        if not tickers:
            return ()
        return self._get_results(
            "/stocks/vX/float",
            {"ticker.any_of": ",".join(ticker.upper() for ticker in tickers)},
        )

    def fetch_ratios(self, tickers: Sequence[str]) -> tuple[dict[str, Any], ...]:
        if not tickers:
            return ()
        return self._get_results(
            "/stocks/financials/v1/ratios",
            {"ticker.any_of": ",".join(ticker.upper() for ticker in tickers)},
        )

    def fetch_income_statements(
        self, tickers: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        return self._fetch_statements("income-statements", tickers)

    def fetch_balance_sheets(
        self, tickers: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        return self._fetch_statements("balance-sheets", tickers)

    def fetch_cash_flow_statements(
        self, tickers: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        return self._fetch_statements("cash-flow-statements", tickers)

    def fetch_filing_index(
        self,
        tickers: Sequence[str],
        *,
        filing_date_gte: date | None = None,
    ) -> tuple[dict[str, Any], ...]:
        if not tickers:
            return ()
        params = {"ticker.any_of": ",".join(ticker.upper() for ticker in tickers)}
        if filing_date_gte is not None:
            params["filing_date.gte"] = filing_date_gte.isoformat()
        return self._get_results("/stocks/filings/vX/index", params)

    def fetch_native_bars(
        self,
        ticker: str,
        interval: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> tuple[dict[str, Any], ...]:
        if interval not in _NATIVE_AGGREGATES:
            raise ValueError(f"unsupported native interval: {interval}")
        multiplier, timespan = _NATIVE_AGGREGATES[interval]
        return self._get_results(
            f"/v2/aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/"
            f"{start.isoformat()}/{end.isoformat()}",
            {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000},
        )

    def _fetch_statements(
        self, statement: str, tickers: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        if not tickers:
            return ()
        return self._get_results(
            f"/stocks/financials/v1/{statement}",
            {"tickers.any_of": ",".join(ticker.upper() for ticker in tickers)},
        )

    def _get_results(
        self, path: str, params: dict[str, Any]
    ) -> tuple[dict[str, Any], ...]:
        results: list[dict[str, Any]] = []
        url = self._url(path)
        request_params = dict(params)
        while url:
            payload = self._request(url, request_params)
            rows = payload.get("results") or []
            if not isinstance(rows, list):
                raise ValueError("Polygon results must be a list")
            results.extend(dict(row) for row in rows)
            next_url = payload.get("next_url")
            if next_url:
                self._validate_url(next_url)
                url = next_url
                request_params = {}
            else:
                url = ""
        return tuple(results)

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        return self._request(self._url(path), dict(params))

    def _request(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        self._validate_url(url)
        response = self.session.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout_seconds,
        )
        if response.status_code != 200:
            raise requests.HTTPError(
                f"Polygon request failed with HTTP {response.status_code}",
                response=response,
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Polygon response must be a JSON object")
        return payload

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    @staticmethod
    def _validate_url(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_API_HOSTS:
            raise ValueError("Polygon pagination URL must remain on an approved HTTPS host")


def normalize_security_reference(
    payload: dict[str, Any],
    *,
    observed_at: datetime,
    source_as_of_date: date | None = None,
    float_payload: dict[str, Any] | None = None,
) -> SecurityReferenceRevision:
    ticker = str(payload.get("ticker") or "").upper()
    if not ticker:
        raise ValueError("ticker overview is missing ticker")
    observed_utc = _utc(observed_at, "observed_at")
    effective_date = source_as_of_date or observed_utc.date()
    effective_from = datetime.combine(effective_date, datetime.min.time(), tzinfo=timezone.utc)
    identity = (
        payload.get("composite_figi")
        or payload.get("share_class_figi")
        or f"{payload.get('cik') or 'NO_CIK'}:{ticker}"
    )
    security_id = uuid5(NAMESPACE_URL, f"equity-security:{identity}")
    raw = dict(payload)
    if float_payload:
        raw["float"] = dict(float_payload)
    digest = sha256_json(raw)
    sic_code = _optional_string(payload.get("sic_code"))
    sector = sector_for_sic(sic_code) or MANUAL_SECTORS.get(ticker)
    return SecurityReferenceRevision(
        security_revision_id=uuid5(
            NAMESPACE_URL,
            f"equity-security-revision:{security_id}:{effective_from.isoformat()}:{digest}",
        ),
        security_id=security_id,
        ticker=ticker,
        active=bool(payload.get("active", False)),
        company_name=_optional_string(payload.get("name")),
        security_type=_optional_string(payload.get("type")),
        cik=_optional_string(payload.get("cik")),
        composite_figi=_optional_string(payload.get("composite_figi")),
        share_class_figi=_optional_string(payload.get("share_class_figi")),
        primary_exchange=_optional_string(payload.get("primary_exchange")),
        sic_code=sic_code,
        sic_description=_optional_string(payload.get("sic_description")),
        sector=sector,
        industry=_optional_string(payload.get("sic_description")),
        list_date=_optional_date(payload.get("list_date")),
        delisted_date=_optional_date(payload.get("delisted_utc")),
        weighted_shares=_optional_decimal(payload.get("weighted_shares_outstanding")),
        free_float=_optional_decimal((float_payload or {}).get("free_float")),
        free_float_percent=_optional_float((float_payload or {}).get("free_float_percent")),
        market_cap=_optional_decimal(payload.get("market_cap")),
        source="POLYGON_TICKER_OVERVIEW_V3",
        effective_from=effective_from,
        observed_at=observed_utc,
        payload_sha256=digest,
        raw_payload_json=canonical_json(raw),
    )


def normalize_corporate_actions(
    rows: Iterable[dict[str, Any]],
    *,
    security_ids: Mapping[str, UUID],
    action_type: str,
    observed_at: datetime,
    availability_mode: BarAvailabilityMode,
) -> tuple[EquityCorporateAction, ...]:
    observed_utc = _utc(observed_at, "observed_at")
    results = []
    for row in rows:
        ticker = str(row.get("ticker") or "").upper()
        security_id = security_ids.get(ticker)
        effective_date = _optional_date(
            row.get("execution_date") if action_type == "SPLIT"
            else row.get("ex_dividend_date")
        )
        if not ticker or security_id is None or effective_date is None:
            continue
        payload = dict(row)
        digest = sha256_json(payload)
        source_key = str(row.get("id") or digest)
        replay_available_at = (
            datetime.combine(effective_date, datetime.min.time(), tzinfo=timezone.utc)
            if availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
            else None
        )
        results.append(EquityCorporateAction(
            corporate_action_id=uuid5(
                NAMESPACE_URL,
                f"equity-corporate-action:POLYGON:{action_type}:{source_key}:{digest}",
            ),
            security_id=security_id,
            ticker=ticker,
            action_type=action_type,
            effective_date=effective_date,
            declaration_date=_optional_date(row.get("declaration_date")),
            ex_date=_optional_date(row.get("ex_dividend_date")),
            record_date=_optional_date(row.get("record_date")),
            pay_date=_optional_date(row.get("pay_date")),
            cash_amount=_optional_decimal(row.get("cash_amount")),
            split_from=_optional_decimal(row.get("split_from")),
            split_to=_optional_decimal(row.get("split_to")),
            new_ticker=None,
            source="POLYGON_CORPORATE_ACTIONS_V1",
            source_key=source_key,
            first_observed_at=observed_utc,
            revised_observed_at=None,
            payload_sha256=digest,
            raw_payload_json=canonical_json(payload),
            availability_mode=availability_mode,
            replay_available_at=replay_available_at,
        ))
    return tuple(results)


def normalize_grouped_daily_bars(
    rows: Iterable[dict[str, Any]],
    *,
    session_date: date,
    security_ids: Mapping[str, UUID],
    observed_at: datetime,
    ingestion_segment_id: UUID,
    availability_mode: BarAvailabilityMode,
    adjusted: bool = False,
    calendar_name: str = "XNYS",
) -> tuple[EquityBarRevision, ...]:
    observed_utc = _utc(observed_at, "observed_at")
    calendar = exchange_calendars.get_calendar(calendar_name)
    session = pd.Timestamp(session_date)
    if not calendar.is_session(session):
        return ()
    start = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
    end = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
    replay_available_at = (
        end if availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
        else None
    )
    results = []
    for row in rows:
        ticker = str(row.get("T") or row.get("ticker") or "")
        security_id = security_ids.get(ticker)
        if not ticker or security_id is None:
            continue
        try:
            open_price = Decimal(str(row["o"]))
            high_price = Decimal(str(row["h"]))
            low_price = Decimal(str(row["l"]))
            close_price = Decimal(str(row["c"]))
            volume = Decimal(str(row.get("v") or 0))
        except (KeyError, TypeError, ValueError):
            continue
        payload = dict(row)
        normalizer_version = "POLYGON_GROUPED_DAILY_EXACT_TICKER_V2"
        digest = sha256_json({
            "normalizer_version": normalizer_version,
            "provider_payload": payload,
        })
        results.append(EquityBarRevision(
            bar_revision_id=uuid5(
                NAMESPACE_URL,
                f"equity-bar:{ticker}:1d:{start.isoformat()}:"
                f"NATIVE_REST:{availability_mode.value}:{normalizer_version}:{digest}",
            ),
            security_id=security_id,
            ticker=ticker,
            interval="1d",
            session_date=session_date,
            bar_start=start,
            bar_end=end,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
            vwap=_optional_decimal(row.get("vw")),
            transaction_count=(int(row["n"]) if row.get("n") is not None else None),
            source_kind=BarSourceKind.NATIVE_REST,
            availability_mode=availability_mode,
            is_final=True,
            system_observed_at=max(observed_utc, end),
            replay_available_at=replay_available_at,
            ingestion_segment_id=ingestion_segment_id,
            adjusted=adjusted,
            payload_sha256=digest,
            quality_codes=("GROUPED_DAILY_EXACT_TICKER_V2",),
        ))
    return tuple(results)


def normalize_fundamental_reports(
    security: SecurityReferenceRevision,
    *,
    income_rows: Sequence[dict[str, Any]],
    balance_rows: Sequence[dict[str, Any]],
    cash_flow_rows: Sequence[dict[str, Any]],
    filing_rows: Sequence[dict[str, Any]],
    observed_at: datetime,
) -> tuple[FundamentalReport, ...]:
    observed_utc = _utc(observed_at, "observed_at")
    grouped: dict[tuple[Any, ...], dict[str, dict[str, Any]]] = defaultdict(dict)
    for statement_name, rows in (
        ("income", income_rows),
        ("balance", balance_rows),
        ("cash_flow", cash_flow_rows),
    ):
        for row in rows:
            tickers = {str(value).upper() for value in row.get("tickers") or []}
            if tickers and security.ticker not in tickers:
                continue
            key = (
                row.get("cik") or security.cik,
                row.get("period_end"),
                row.get("timeframe"),
                row.get("fiscal_year"),
                row.get("fiscal_quarter"),
                row.get("filing_date"),
            )
            if not key[1] or not key[2]:
                continue
            grouped[key][statement_name] = dict(row)

    filings_by_key = {
        (
            _normalize_cik(row.get("cik")),
            row.get("filing_date"),
            _optional_string(row.get("form_type")),
        ): row
        for row in filing_rows
    }
    reports: list[FundamentalReport] = []
    for key, statements in sorted(grouped.items(), key=lambda item: str(item[0])):
        cik, period_end_raw, timeframe, fiscal_year, fiscal_quarter, filing_date_raw = key
        period_end = _optional_date(period_end_raw)
        if period_end is None or timeframe not in (
            "quarterly", "annual", "trailing_twelve_months"
        ):
            continue
        filing_date = _optional_date(filing_date_raw)
        expected_form = "10-K" if timeframe == "annual" else "10-Q"
        filing = filings_by_key.get(
            (_normalize_cik(cik), filing_date.isoformat() if filing_date else None, expected_form)
        )
        availability = conservative_filing_availability(filing_date or period_end)
        if availability > observed_utc:
            continue
        metrics = _statement_metrics(statements)
        raw = {"statements": statements, "filing": filing or {}}
        digest = sha256_json(raw)
        source_key = ":".join(
            [
                _normalize_cik(cik) or str(security.security_id),
                period_end.isoformat(),
                str(timeframe),
                str(fiscal_year or ""),
                str(fiscal_quarter or ""),
                filing_date.isoformat() if filing_date else "NO_FILING_DATE",
            ]
        )
        quality = ["FILING_AVAILABILITY_DATE_ONLY"]
        if filing is None:
            quality.append("ACCESSION_UNRESOLVED")
        if len(statements) < 3:
            quality.append("PARTIAL_STATEMENT_SET")
        reports.append(
            FundamentalReport(
                fundamental_report_id=uuid5(
                    NAMESPACE_URL,
                    f"equity-fundamental:{source_key}:{digest}",
                ),
                security_id=security.security_id,
                security_revision_id=security.security_revision_id,
                cik=_optional_string(cik),
                accession_number=_optional_string((filing or {}).get("accession_number")),
                form_type=_optional_string((filing or {}).get("form_type")),
                timeframe=str(timeframe),
                fiscal_year=int(fiscal_year) if fiscal_year is not None else None,
                fiscal_quarter=int(fiscal_quarter) if fiscal_quarter is not None else None,
                period_end=period_end,
                filing_date=filing_date,
                availability_time=availability,
                observed_at=observed_utc,
                source="POLYGON_FINANCIALS_V1",
                source_key=source_key,
                metrics_json=canonical_json(metrics),
                raw_payload_json=canonical_json(raw),
                payload_sha256=digest,
                quality_codes=tuple(quality),
            )
        )
    return tuple(reports)


def normalize_native_bars(
    security_id: UUID,
    ticker: str,
    interval: str,
    rows: Iterable[dict[str, Any]],
    *,
    observed_at: datetime,
    adjusted: bool,
    availability_mode: BarAvailabilityMode,
    ingestion_segment_id: UUID | None = None,
    replay_available_at: datetime | None = None,
    calendar_name: str = "XNYS",
) -> tuple[EquityBarRevision, ...]:
    if interval not in _NATIVE_AGGREGATES:
        raise ValueError(f"unsupported native interval: {interval}")
    observed_utc = _utc(observed_at, "observed_at")
    replay_available_utc = (
        _utc(replay_available_at, "replay_available_at")
        if replay_available_at is not None else None
    )
    calendar = exchange_calendars.get_calendar(calendar_name)
    results: list[EquityBarRevision] = []
    for row in rows:
        provider_start = pd.Timestamp(row["t"], unit="ms", tz="UTC")
        if interval == "1mo":
            provider_date = provider_start.tz_convert("America/New_York").date()
            month_start = date(provider_date.year, provider_date.month, 1)
            next_month = (
                date(provider_date.year + 1, 1, 1)
                if provider_date.month == 12
                else date(provider_date.year, provider_date.month + 1, 1)
            )
            sessions = calendar.sessions_in_range(
                pd.Timestamp(month_start), pd.Timestamp(next_month - timedelta(days=1))
            )
            if not len(sessions):
                continue
            start = calendar.session_open(sessions[0]).to_pydatetime().astimezone(
                timezone.utc
            )
            end = calendar.session_close(sessions[-1]).to_pydatetime().astimezone(
                timezone.utc
            )
            session_date = sessions[-1]
        else:
            start = provider_start.to_pydatetime()
            end = start + timedelta(minutes=_INTRADAY_MINUTES[interval])
            session_date = pd.Timestamp(start.date())
        effective_replay_available = (
            replay_available_utc or end
            if availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
            else None
        )
        available = effective_replay_available or observed_utc
        if available < end:
            continue
        if not calendar.is_session(session_date):
            continue
        if interval != "1mo":
            session_open = calendar.session_open(session_date).to_pydatetime().astimezone(timezone.utc)
            session_close = calendar.session_close(session_date).to_pydatetime().astimezone(timezone.utc)
            if start < session_open or end > session_close:
                continue
        payload = dict(row)
        digest = sha256_json(payload)
        results.append(
            EquityBarRevision(
                bar_revision_id=uuid5(
                    NAMESPACE_URL,
                    f"equity-bar:{ticker.upper()}:{interval}:{start.isoformat()}:"
                    f"NATIVE_REST:{availability_mode.value}:{digest}",
                ),
                security_id=security_id,
                ticker=ticker,
                interval=interval,
                session_date=session_date.date(),
                bar_start=start,
                bar_end=end,
                open_price=Decimal(str(row["o"])),
                high_price=Decimal(str(row["h"])),
                low_price=Decimal(str(row["l"])),
                close_price=Decimal(str(row["c"])),
                volume=Decimal(str(row.get("v") or 0)),
                vwap=_optional_decimal(row.get("vw")),
                transaction_count=(
                    int(row["n"]) if row.get("n") is not None else None
                ),
                source_kind=BarSourceKind.NATIVE_REST,
                availability_mode=availability_mode,
                is_final=True,
                system_observed_at=max(observed_utc, end),
                replay_available_at=effective_replay_available,
                adjusted=adjusted,
                payload_sha256=digest,
                ingestion_segment_id=ingestion_segment_id,
            )
        )
    return tuple(results)


def conservative_filing_availability(filing_date: date) -> datetime:
    """Treat a date-only filing as usable at the next regular-session open."""
    calendar = exchange_calendars.get_calendar("XNYS")
    candidate = pd.Timestamp(filing_date)
    if calendar.is_session(candidate):
        session = calendar.next_session(candidate)
    else:
        session = calendar.date_to_session(candidate, direction="next")
    return calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)


def _statement_metrics(statements: dict[str, dict[str, Any]]) -> dict[str, Any]:
    income = statements.get("income", {})
    balance = statements.get("balance", {})
    cash = statements.get("cash_flow", {})
    operating_cash_flow = cash.get("net_cash_from_operating_activities")
    capital_expenditures = cash.get("purchase_of_property_plant_and_equipment")
    free_cash_flow = None
    if operating_cash_flow is not None and capital_expenditures is not None:
        free_cash_flow = float(operating_cash_flow) + float(capital_expenditures)
    return {
        "revenue": income.get("revenue"),
        "gross_profit": income.get("gross_profit"),
        "operating_income": income.get("operating_income"),
        "ebitda": income.get("ebitda"),
        "pretax_income": income.get("income_before_income_taxes"),
        "interest_expense": income.get("interest_expense"),
        "income_taxes": income.get("income_taxes"),
        "net_income": income.get("net_income_loss_attributable_common_shareholders")
        or income.get("consolidated_net_income_loss"),
        "basic_eps": income.get("basic_earnings_per_share"),
        "diluted_eps": income.get("diluted_earnings_per_share"),
        "basic_weighted_shares": income.get("basic_shares_outstanding"),
        "diluted_weighted_shares": income.get("diluted_shares_outstanding"),
        "research_and_development": income.get("research_development"),
        "selling_general_admin": income.get("selling_general_administrative"),
        "depreciation_amortization": income.get("depreciation_depletion_amortization")
        or cash.get("depreciation_depletion_and_amortization"),
        "cash_and_equivalents": balance.get("cash_and_equivalents"),
        "short_term_investments": balance.get("short_term_investments"),
        "current_assets": balance.get("total_current_assets"),
        "current_liabilities": balance.get("total_current_liabilities"),
        "total_assets": balance.get("total_assets"),
        "current_debt": balance.get("debt_current"),
        "long_term_debt": balance.get("long_term_debt_and_capital_lease_obligations"),
        "total_liabilities": balance.get("total_liabilities"),
        "total_equity": balance.get("total_equity"),
        "operating_cash_flow": operating_cash_flow,
        "capital_expenditures": capital_expenditures,
        "free_cash_flow": free_cash_flow,
        "dividends": cash.get("dividends"),
        "investing_cash_flow": cash.get("net_cash_from_investing_activities"),
        "financing_cash_flow": cash.get("net_cash_from_financing_activities"),
    }


def _normalize_cik(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text.zfill(10) if text else None


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    return date.fromisoformat(str(value)[:10])


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    number = Decimal(str(value))
    return number if number.is_finite() else None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if number == number else None


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)