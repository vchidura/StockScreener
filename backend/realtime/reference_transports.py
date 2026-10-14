from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol
from urllib.parse import quote, urlsplit

from http_client import get_session
from market_events import (
    FINNHUB_REQUEST_INTERVAL_SECONDS,
    FomcCalendarSnapshot,
    parse_fomc_calendar,
)
from options.model_inputs import TREASURY_CURVE_URL
from provider_endpoints import REST_BASE_URL, REST_HOSTS

from .reference_adapters import (
    FederalReserveReferenceAdapter,
    FinnhubReferenceAdapter,
    MassiveReferenceAdapter,
    TreasuryReferenceAdapter,
)
from .reference_service import ProviderEntitlementUnavailable, ReferenceSeedService


@dataclass(frozen=True, slots=True)
class ReferenceHttpResponse:
    status_code: int
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()


class ReferenceHttpTransport(Protocol):
    def get(
        self,
        url: str,
        params: Mapping[str, str],
        headers: Mapping[str, str],
    ) -> ReferenceHttpResponse:
        ...


class RequestsReferenceHttpTransport:
    def __init__(
        self,
        *,
        timeout_seconds: float = 30.0,
        session_factory: Callable = get_session,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("reference HTTP timeout must be positive")
        self.timeout_seconds = timeout_seconds
        self.session_factory = session_factory

    def get(
        self,
        url: str,
        params: Mapping[str, str],
        headers: Mapping[str, str],
    ) -> ReferenceHttpResponse:
        response = self.session_factory().get(
            url,
            params=dict(params),
            headers=dict(headers),
            timeout=(5, self.timeout_seconds),
            allow_redirects=False,
        )
        return ReferenceHttpResponse(
            response.status_code,
            response.content,
            tuple((str(key), str(value)) for key, value in response.headers.items()),
        )


@dataclass(frozen=True, slots=True)
class ReferenceTransportLimits:
    maximum_requests: int = 64
    maximum_response_bytes: int = 2_000_000
    maximum_total_bytes: int = 16_000_000
    maximum_rows: int = 50_000
    maximum_pages: int = 16

    def __post_init__(self) -> None:
        if min(
            self.maximum_requests,
            self.maximum_response_bytes,
            self.maximum_total_bytes,
            self.maximum_rows,
            self.maximum_pages,
        ) <= 0:
            raise ValueError("reference transport limits must be positive")
        if self.maximum_response_bytes > self.maximum_total_bytes:
            raise ValueError("per-response byte limit cannot exceed the total byte limit")


@dataclass(frozen=True, slots=True)
class ReferenceTransportSnapshot:
    provider_requests: int
    response_bytes: int
    rows: int


class _BoundedReferenceTransport:
    def __init__(
        self,
        http: ReferenceHttpTransport,
        *,
        allowed_host: str,
        limits: ReferenceTransportLimits,
    ) -> None:
        self.http = http
        self.allowed_host = allowed_host.lower()
        self.limits = limits
        self._requests = 0
        self._response_bytes = 0
        self._rows = 0
        self._generation_requests = 0
        self._generation_response_bytes = 0
        self._generation_rows = 0

    def begin_generation(self) -> None:
        self._generation_requests = 0
        self._generation_response_bytes = 0
        self._generation_rows = 0

    def snapshot(self) -> ReferenceTransportSnapshot:
        return ReferenceTransportSnapshot(
            self._requests,
            self._response_bytes,
            self._rows,
        )

    def _get(
        self,
        url: str,
        params: Mapping[str, str],
        headers: Mapping[str, str],
    ) -> ReferenceHttpResponse:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != self.allowed_host:
            raise ValueError("reference request must remain on its approved HTTPS host")
        if self._generation_requests >= self.limits.maximum_requests:
            raise ValueError("reference transport exceeded its request limit")
        response = self.http.get(url, params, headers)
        self._requests += 1
        self._generation_requests += 1
        response_bytes = len(response.body)
        if response_bytes > self.limits.maximum_response_bytes:
            raise ValueError("reference response exceeded its byte limit")
        self._response_bytes += response_bytes
        self._generation_response_bytes += response_bytes
        if self._generation_response_bytes > self.limits.maximum_total_bytes:
            raise ValueError("reference transport exceeded its total byte limit")
        if response.status_code == 403:
            raise ProviderEntitlementUnavailable(
                "reference provider entitlement unavailable"
            )
        if response.status_code != 200:
            raise ValueError(f"reference request failed with HTTP {response.status_code}")
        return response

    def _json(
        self,
        url: str,
        params: Mapping[str, str],
        headers: Mapping[str, str],
    ) -> Mapping[str, object]:
        response = self._get(url, params, headers)
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("reference response is not valid UTF-8 JSON") from exc
        if not isinstance(payload, Mapping):
            raise ValueError("reference JSON response must be an object")
        return payload

    def _admit_rows(self, rows: object, label: str) -> tuple[Mapping[str, object], ...]:
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise ValueError(f"{label} response has an invalid results array")
        self._rows += len(rows)
        self._generation_rows += len(rows)
        if self._generation_rows > self.limits.maximum_rows:
            raise ValueError("reference transport exceeded its row limit")
        return tuple(dict(row) for row in rows)


class MassiveReferenceTransport(_BoundedReferenceTransport):
    def __init__(
        self,
        api_key: str,
        http: ReferenceHttpTransport,
        *,
        base_url: str = REST_BASE_URL,
        limits: ReferenceTransportLimits = ReferenceTransportLimits(),
        maximum_expiration_days: int = 60,
    ) -> None:
        if not api_key.strip():
            raise ValueError("Massive API key is required")
        base = urlsplit(base_url.rstrip("/"))
        if base.scheme != "https" or base.hostname not in REST_HOSTS:
            raise ValueError("Massive base URL must use an approved HTTPS host")
        if maximum_expiration_days <= 0:
            raise ValueError("Massive expiration horizon must be positive")
        super().__init__(http, allowed_host=base.hostname, limits=limits)
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.maximum_expiration_days = maximum_expiration_days

    def _results(
        self,
        path: str,
        params: Mapping[str, str],
    ) -> tuple[Mapping[str, object], ...]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_params = dict(params)
        pages = 0
        seen = set()
        results = []
        while url:
            identity = (url, tuple(sorted(request_params.items())))
            if identity in seen:
                raise ValueError("Massive pagination repeated a request")
            seen.add(identity)
            pages += 1
            if pages > self.limits.maximum_pages:
                raise ValueError("Massive response exceeded its page limit")
            payload = self._json(url, request_params, self.headers)
            results.extend(self._admit_rows(payload.get("results") or [], "Massive"))
            next_url = payload.get("next_url")
            if next_url is None:
                break
            if not isinstance(next_url, str) or not next_url:
                raise ValueError("Massive pagination URL is invalid")
            url = next_url
            request_params = {}
        return tuple(results)

    def _first_results(
        self,
        path: str,
        params: Mapping[str, str],
    ) -> tuple[Mapping[str, object], ...]:
        payload = self._json(
            f"{self.base_url}/{path.lstrip('/')}",
            params,
            self.headers,
        )
        return self._admit_rows(payload.get("results") or [], "Massive")

    def fetch_security(
        self,
        ticker: str,
        as_of: object,
    ) -> Mapping[str, object] | None:
        payload = self._json(
            f"{self.base_url}/v3/reference/tickers/{quote(ticker.upper(), safe='')}",
            {"date": str(as_of)},
            self.headers,
        )
        result = payload.get("results")
        if result is None:
            return None
        if not isinstance(result, Mapping):
            raise ValueError("Massive security response has an invalid results object")
        self._rows += 1
        self._generation_rows += 1
        if self._generation_rows > self.limits.maximum_rows:
            raise ValueError("reference transport exceeded its row limit")
        return dict(result)

    def _fetch_fundamentals(
        self, path: str, tickers: tuple[str, ...], as_of: object, *, ticker_parameter: str,
    ) -> tuple[Mapping[str, object], ...]:
        if not tickers:
            return ()
        end = date.fromisoformat(str(as_of))
        return self._results(
            path,
            {
                ticker_parameter: ",".join(ticker.upper() for ticker in tickers),
                "filing_date.gte": (end - timedelta(days=366 * 5)).isoformat(),
                "filing_date.lte": end.isoformat(),
                "limit": "5000",
            },
        )

    def fetch_income_statements(self, tickers, as_of):
        return self._fetch_fundamentals(
            "/stocks/financials/v1/income-statements", tickers, as_of,
            ticker_parameter="tickers.any_of",
        )

    def fetch_balance_sheets(self, tickers, as_of):
        return self._fetch_fundamentals(
            "/stocks/financials/v1/balance-sheets", tickers, as_of,
            ticker_parameter="tickers.any_of",
        )

    def fetch_cash_flow_statements(self, tickers, as_of):
        return self._fetch_fundamentals(
            "/stocks/financials/v1/cash-flow-statements", tickers, as_of,
            ticker_parameter="tickers.any_of",
        )

    def fetch_filing_index(self, tickers, as_of):
        return self._fetch_fundamentals(
            "/stocks/filings/vX/index", tickers, as_of,
            ticker_parameter="ticker.any_of",
        )

    def fetch_splits(
        self, ticker: str, start: object, end: object
    ) -> tuple[Mapping[str, object], ...]:
        return self._results(
            "/stocks/v1/splits",
            {
                "ticker": ticker.upper(),
                "execution_date.gte": str(start),
                "execution_date.lte": str(end),
                "limit": "5000",
                "sort": "execution_date.asc,ticker.asc",
            },
        )

    def fetch_dividends(
        self, ticker: str, start: object, end: object
    ) -> tuple[Mapping[str, object], ...]:
        return self._results(
            "/stocks/v1/dividends",
            {
                "ticker": ticker.upper(),
                "ex_dividend_date.gte": str(start),
                "ex_dividend_date.lte": str(end),
                "limit": "5000",
                "sort": "ex_dividend_date.asc,ticker.asc",
            },
        )

    def _option_filters(self, underlyer: str, as_of: object) -> dict[str, str]:
        start = date.fromisoformat(str(as_of))
        return {
            "underlying_ticker": underlyer.upper(),
            "expiration_date.gte": start.isoformat(),
            "expiration_date.lte": (start + timedelta(days=self.maximum_expiration_days)).isoformat(),
            "limit": "1000",
            "sort": "ticker",
            "order": "asc",
        }

    def fetch_option_catalog(
        self,
        underlyer: str,
        as_of: object,
    ) -> tuple[Mapping[str, object], ...]:
        return self._first_results(
            "/v3/reference/options/contracts",
            {
                **self._option_filters(underlyer, as_of),
                "as_of": str(as_of),
                "expired": "false",
                "contract_type": "call",
                "limit": "1",
            },
        )

    def fetch_open_interest(
        self,
        underlyer: str,
        contract_tickers: tuple[str, ...],
        as_of: object,
    ) -> tuple[Mapping[str, object], ...]:
        if len(contract_tickers) > 1:
            raise ValueError("Developer reference OI accepts one admitted contract")
        if not contract_tickers:
            return ()
        contract_ticker = contract_tickers[0]
        payload = self._json(
            f"{self.base_url}/v3/snapshot/options/"
            f"{quote(underlyer.upper(), safe='')}/"
            f"{quote(contract_ticker, safe=':')}",
            {},
            self.headers,
        )
        result = payload.get("results")
        if result is None:
            return ()
        if not isinstance(result, Mapping):
            raise ValueError("Massive exact option snapshot has an invalid result")
        self._rows += 1
        self._generation_rows += 1
        if self._generation_rows > self.limits.maximum_rows:
            raise ValueError("reference transport exceeded its row limit")
        return (dict(result),)


class FinnhubReferenceTransport(_BoundedReferenceTransport):
    url = "https://finnhub.io/api/v1/calendar/earnings"

    def __init__(
        self,
        api_key: str,
        http: ReferenceHttpTransport,
        *,
        limits: ReferenceTransportLimits = ReferenceTransportLimits(),
        maximum_window_days: int = 30,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key.strip():
            raise ValueError("Finnhub API key is required")
        if maximum_window_days <= 0:
            raise ValueError("Finnhub window limit must be positive")
        super().__init__(http, allowed_host="finnhub.io", limits=limits)
        self.headers = {"X-Finnhub-Token": api_key}
        self.maximum_window_days = maximum_window_days
        self.sleep = sleep

    def fetch(self, start: object, end: object) -> tuple[Mapping[str, object], ...]:
        window_start = date.fromisoformat(str(start))
        window_end = date.fromisoformat(str(end))
        if window_end < window_start:
            raise ValueError("Finnhub earnings window is invalid")
        rows = []
        chunk_start = window_start
        request_count = 0
        while chunk_start <= window_end:
            chunk_end = min(
                chunk_start + timedelta(days=self.maximum_window_days - 1),
                window_end,
            )
            if request_count:
                self.sleep(FINNHUB_REQUEST_INTERVAL_SECONDS)
            payload = self._json(
                self.url,
                {"from": chunk_start.isoformat(), "to": chunk_end.isoformat()},
                self.headers,
            )
            chunk = self._admit_rows(payload.get("earningsCalendar"), "Finnhub earnings")
            for row in chunk:
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or not symbol.strip():
                    raise ValueError("Finnhub earnings row has no symbol")
                try:
                    report_date = date.fromisoformat(str(row.get("date") or ""))
                except ValueError as exc:
                    raise ValueError("Finnhub earnings row has an invalid date") from exc
                if not chunk_start <= report_date <= chunk_end:
                    raise ValueError("Finnhub earnings row is outside its request window")
            rows.extend(chunk)
            request_count += 1
            chunk_start = chunk_end + timedelta(days=1)
        return tuple(rows)


class FederalReserveReferenceTransport(_BoundedReferenceTransport):
    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

    def __init__(
        self,
        http: ReferenceHttpTransport,
        *,
        limits: ReferenceTransportLimits = ReferenceTransportLimits(),
    ) -> None:
        super().__init__(http, allowed_host="www.federalreserve.gov", limits=limits)

    def fetch(self) -> FomcCalendarSnapshot:
        response = self._get(
            self.url,
            {},
            {"User-Agent": "StockScreener/1.0 realtime-reference"},
        )
        try:
            html = response.body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("Federal Reserve calendar is not UTF-8") from exc
        return parse_fomc_calendar(html)


class TreasuryReferenceTransport(_BoundedReferenceTransport):
    def __init__(
        self,
        http: ReferenceHttpTransport,
        *,
        limits: ReferenceTransportLimits = ReferenceTransportLimits(),
    ) -> None:
        super().__init__(http, allowed_host="home.treasury.gov", limits=limits)

    def fetch_xml(self, year: int) -> str:
        if year < 2000 or year > 2100:
            raise ValueError("Treasury curve year is outside the supported range")
        url = TREASURY_CURVE_URL.format(year=year)
        response = self._get(
            url,
            {},
            {"User-Agent": "StockScreener/1.0 realtime-reference"},
        )
        try:
            return response.body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("Treasury curve response is not UTF-8") from exc


def build_live_reference_service(
    *,
    massive_api_key: str,
    finnhub_api_key: str,
    etf_tickers: frozenset[str],
    http: ReferenceHttpTransport | None = None,
    limits: ReferenceTransportLimits = ReferenceTransportLimits(),
    massive_base_url: str = REST_BASE_URL,
    corporate_action_lookback_days: int = 180,
    include_fundamentals: bool = False,
    finnhub_sleep: Callable[[float], None] = time.sleep,
) -> ReferenceSeedService:
    shared_http = http or RequestsReferenceHttpTransport()
    massive = MassiveReferenceTransport(
        massive_api_key,
        shared_http,
        base_url=massive_base_url,
        limits=limits,
    )
    finnhub = FinnhubReferenceTransport(
        finnhub_api_key,
        shared_http,
        limits=limits,
        sleep=finnhub_sleep,
    )
    federal_reserve = FederalReserveReferenceTransport(shared_http, limits=limits)
    treasury = TreasuryReferenceTransport(shared_http, limits=limits)
    return ReferenceSeedService((
        MassiveReferenceAdapter(
            fetch_security=massive.fetch_security,
            fetch_splits=massive.fetch_splits,
            fetch_dividends=massive.fetch_dividends,
            fetch_option_catalog=massive.fetch_option_catalog,
            fetch_open_interest=massive.fetch_open_interest,
            **(
                {
                    "fetch_income_statements": massive.fetch_income_statements,
                    "fetch_balance_sheets": massive.fetch_balance_sheets,
                    "fetch_cash_flow_statements": massive.fetch_cash_flow_statements,
                    "fetch_filing_index": massive.fetch_filing_index,
                }
                if include_fundamentals else {}
            ),
            corporate_action_lookback_days=corporate_action_lookback_days,
            metrics=massive,
        ),
        FinnhubReferenceAdapter(
            finnhub.fetch,
            etf_tickers=etf_tickers,
            metrics=finnhub,
        ),
        FederalReserveReferenceAdapter(
            federal_reserve.fetch,
            metrics=federal_reserve,
        ),
        TreasuryReferenceAdapter(
            treasury.fetch_xml,
            metrics=treasury,
        ),
    ))