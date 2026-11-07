"""G75 breadth (docs/OPTIONS_COVERAGE_TASK.md; the user's decision of Nov 6): the option chain beside the stream.

At each Options window the scheduler takes REST chain snapshots of every in-scope contract of each Options underlying
(expiring within 60 days, strikes within 20% of spot, same-day contracts included) and keeps one current row per
contract: day volume, open interest, NBBO, IV and Greeks. The stream still covers only the frozen contract set; breadth
feeds the Options Screener and O1 over the whole chain.

Breadth never decides a window. Each underlying is captured on its own: a provider error, a page ceiling or the
deadline leaves that underlying's previous rows in place and records the capture as PARTIAL or FAILED with a reason,
which the pages show as the rows' freshness. Every request is reserved and finished in rt.provider_request_ledger under
its own session budget; response bodies are not kept (measured Nov 7 on Friday's chains: SPY 19 pages, 4,746
contracts, 4.5 MB; about 65 requests and 15 MB per window at 9 underlyings).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Mapping, Sequence
from urllib.parse import quote, urlsplit
from uuid import UUID, uuid4

from psycopg2.extras import execute_values

from .acquisition_ledger import (
    PriceBasis, ProviderRequestCompletion, ProviderRequestLane, ProviderRequestOutcome, ProviderRequestReservation,
    ProviderRequestState, RequestCompleteness, finish_provider_request_cursor,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE
from .provider_budget import (
    MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING, MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING, ProviderBudgetExceeded,
    ProviderBudgetScope, reserve_coordinated_provider_request_cursor,
)

UTC = timezone.utc
_LOGGER = logging.getLogger(__name__)

BREADTH_POLICY = {
    "version": "options-breadth-v1",
    "minimum_dte": 0,
    "maximum_dte": 60,
    "moneyness_fraction": "0.20",
    "page_limit": 250,
}
BREADTH_POLICY_SHA256 = canonical_sha256(BREADTH_POLICY)
BASE_URL = "https://api.massive.com"
APPROVED_HOSTS = frozenset({"api.massive.com", "api.polygon.io"})
ENDPOINT_CLASS = "OPTIONS_CHAIN_SNAPSHOT"
MAXIMUM_PAGES_PER_UNDERLYING = 40
RESPONSE_BYTE_CEILING = 4_000_000
ACCOUNTED_RESPONSE_BYTE_CEILING = RESPONSE_BYTE_CEILING + 1
SESSION_REQUEST_CEILING = 12_000


class BreadthPageError(ValueError):
    """A page that cannot be used; the underlying's capture ends with what it has."""


@dataclass(frozen=True, slots=True)
class BreadthRow:
    contract_ticker: str
    underlying: str
    contract_type: str
    expiration_date: date
    strike_price: Decimal
    day_volume: int | None
    open_interest: int | None
    last_trade_price: Decimal | None
    last_trade_at: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    quote_at: datetime | None
    implied_volatility: float | None
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    underlying_price: Decimal | None


@dataclass(frozen=True, slots=True)
class UnderlyingBreadth:
    underlying: str
    rows: tuple[BreadthRow, ...]
    pages: int
    skipped_rows: int
    response_bytes: int
    state: str
    reason: str | None


def _decimal(value) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() and number >= 0 else None


def _count(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = int(value)
    return number if number >= 0 and number == value else None


def _ratio(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if number == number and abs(number) != float("inf") else None


def _nanoseconds(value) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)


def parse_breadth_page(document: Mapping[str, object], *, underlying: str) -> tuple[list[BreadthRow], int]:
    """Rows of one chain snapshot page and the number of rows that could not be used."""
    results = document.get("results") if isinstance(document, Mapping) else None
    if not isinstance(results, list):
        raise BreadthPageError("chain snapshot page has no results list")
    rows, skipped = [], 0
    for raw in results:
        details = raw.get("details") if isinstance(raw, Mapping) else None
        if not isinstance(details, Mapping):
            skipped += 1
            continue
        ticker = str(details.get("ticker") or "").strip().upper()
        kind = str(details.get("contract_type") or "").strip().lower()
        strike = _decimal(details.get("strike_price"))
        try:
            expiration = date.fromisoformat(str(details.get("expiration_date")))
        except ValueError:
            expiration = None
        if not ticker.startswith("O:") or kind not in {"call", "put"} or not strike or expiration is None:
            skipped += 1
            continue
        day = raw.get("day") if isinstance(raw.get("day"), Mapping) else {}
        trade = raw.get("last_trade") if isinstance(raw.get("last_trade"), Mapping) else {}
        quote_row = raw.get("last_quote") if isinstance(raw.get("last_quote"), Mapping) else {}
        greeks = raw.get("greeks") if isinstance(raw.get("greeks"), Mapping) else {}
        asset = raw.get("underlying_asset") if isinstance(raw.get("underlying_asset"), Mapping) else {}
        rows.append(BreadthRow(
            contract_ticker=ticker,
            underlying=underlying,
            contract_type=kind,
            expiration_date=expiration,
            strike_price=strike,
            day_volume=_count(day.get("volume")),
            open_interest=_count(raw.get("open_interest")),
            last_trade_price=_decimal(trade.get("price")),
            last_trade_at=_nanoseconds(trade.get("sip_timestamp")),
            bid=_decimal(quote_row.get("bid")),
            ask=_decimal(quote_row.get("ask")),
            quote_at=_nanoseconds(quote_row.get("last_updated")),
            implied_volatility=_ratio(raw.get("implied_volatility")),
            delta=_ratio(greeks.get("delta")),
            gamma=_ratio(greeks.get("gamma")),
            theta=_ratio(greeks.get("theta")),
            vega=_ratio(greeks.get("vega")),
            underlying_price=_decimal(asset.get("price")),
        ))
    return rows, skipped


def breadth_query(underlying: str, spot: Decimal, session_date: date) -> tuple[str, dict[str, str]]:
    if not isinstance(spot, Decimal) or not spot > 0:
        raise ValueError("breadth needs a positive spot")
    band = Decimal(BREADTH_POLICY["moneyness_fraction"])
    cents = Decimal("0.01")
    return (
        f"{BASE_URL}/v3/snapshot/options/{quote(underlying, safe='')}",
        {
            "expiration_date.gte": (session_date + timedelta(days=BREADTH_POLICY["minimum_dte"])).isoformat(),
            "expiration_date.lte": (session_date + timedelta(days=BREADTH_POLICY["maximum_dte"])).isoformat(),
            "strike_price.gte": str((spot * (1 - band)).quantize(cents)),
            "strike_price.lte": str((spot * (1 + band)).quantize(cents)),
            "limit": str(BREADTH_POLICY["page_limit"]),
            "sort": "ticker",
            "order": "asc",
        },
    )


def fetch_underlying_breadth(
    transport,
    *,
    underlying: str,
    spot: Decimal,
    session_date: date,
    deadline: datetime,
    clock: Callable[[], datetime],
    before_request: Callable[[int, str], object],
    after_request: Callable[..., None],
    maximum_pages: int = MAXIMUM_PAGES_PER_UNDERLYING,
) -> UnderlyingBreadth:
    """Every page of one underlying's chain. `before_request(page, url)` reserves the request (and may raise to stop);
    `after_request(token, status=, response_bytes=, rows=, error=)` finishes it."""
    url, params = breadth_query(underlying, spot, session_date)
    rows: dict[str, BreadthRow] = {}
    pages = skipped = response_bytes = 0
    seen: set[str] = set()
    while url is not None:
        if pages >= maximum_pages:
            return UnderlyingBreadth(underlying, tuple(rows.values()), pages, skipped, response_bytes, "PARTIAL",
                                     "PAGE_CEILING")
        if clock() >= deadline:
            return UnderlyingBreadth(underlying, tuple(rows.values()), pages, skipped, response_bytes, "PARTIAL",
                                     "DEADLINE")
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https" or parsed.netloc not in APPROVED_HOSTS or parsed.fragment
            or not parsed.path.startswith("/v3/snapshot/options/") or url in seen
        ):
            raise BreadthPageError("chain snapshot next_url is not approved")
        seen.add(url)
        token = before_request(pages, url)
        try:
            response = transport.get(url, params)
        except Exception as error:
            after_request(token, status=getattr(transport, "http_status", None),
                          response_bytes=getattr(transport, "response_bytes", 0), rows=None, error=error)
            raise
        pages += 1
        response_bytes += len(response.body)
        if response.status_code != 200:
            error = BreadthPageError(f"HTTP {response.status_code}")
            after_request(token, status=response.status_code, response_bytes=len(response.body), rows=None,
                          error=error)
            raise error
        try:
            document = json.loads(response.body)
            page_rows, page_skipped = parse_breadth_page(document, underlying=underlying)
        except (ValueError, TypeError) as error:
            after_request(token, status=200, response_bytes=len(response.body), rows=None, error=error)
            raise BreadthPageError(f"unreadable page: {type(error).__name__}") from error
        after_request(token, status=200, response_bytes=len(response.body), rows=len(page_rows), error=None)
        skipped += page_skipped
        for row in page_rows:
            rows[row.contract_ticker] = row
        next_url = document.get("next_url")
        url, params = (str(next_url), {}) if next_url else (None, {})
    return UnderlyingBreadth(underlying, tuple(rows.values()), pages, skipped, response_bytes, "COMPLETE", None)


_COLUMNS = (
    "session_date,contract_ticker,underlying,contract_type,expiration_date,strike_price,day_volume,open_interest,"
    "last_trade_price,last_trade_at,bid,ask,quote_at,implied_volatility,delta,gamma,theta,vega,underlying_price,"
    "equity_cutoff,observed_at"
)


def persist_breadth_cursor(
    cursor,
    *,
    session_date: date,
    equity_cutoff: datetime,
    started_at: datetime,
    observed_at: datetime,
    captured: UnderlyingBreadth,
) -> UUID:
    """Replace the underlying's current rows with this capture's (a later capture never loses to an earlier one) and
    record the capture."""
    if captured.rows:
        execute_values(
            cursor,
            f"INSERT INTO rt.option_breadth_current ({_COLUMNS}) VALUES %s "
            "ON CONFLICT (session_date,contract_ticker) DO UPDATE SET "
            + ",".join(f"{name}=EXCLUDED.{name}" for name in _COLUMNS.split(",")[2:])
            + " WHERE rt.option_breadth_current.observed_at<=EXCLUDED.observed_at",
            [
                (
                    session_date, row.contract_ticker, row.underlying, row.contract_type, row.expiration_date,
                    row.strike_price, row.day_volume, row.open_interest, row.last_trade_price, row.last_trade_at,
                    row.bid, row.ask, row.quote_at, row.implied_volatility, row.delta, row.gamma, row.theta,
                    row.vega, row.underlying_price, equity_cutoff, observed_at,
                )
                for row in captured.rows
            ],
            page_size=1000,
        )
    capture_id = uuid4()
    cursor.execute(
        "INSERT INTO rt.option_breadth_captures (capture_id,session_date,underlying,equity_cutoff,started_at,"
        "observed_at,state,pages,contracts,skipped_rows,response_bytes,reason) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(capture_id), session_date, captured.underlying, equity_cutoff, started_at, observed_at,
            captured.state, captured.pages, len(captured.rows), captured.skipped_rows, captured.response_bytes,
            None if captured.reason is None else captured.reason[:160],
        ),
    )
    return capture_id


def breadth_budget(*, session_date: date, authorization_reference: str, authorized_until: datetime) -> ProviderBudgetScope:
    return ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=session_date,
        authorization_reference=f"{authorization_reference}:BREADTH",
        provider_authorization_reference=authorization_reference,
        expected_cost="G75 option chain breadth snapshots at each Options window",
        request_ceiling=SESSION_REQUEST_CEILING,
        decoded_byte_ceiling=SESSION_REQUEST_CEILING * ACCOUNTED_RESPONSE_BYTE_CEILING,
        account_request_ceiling=MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING,
        account_decoded_byte_ceiling=MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING,
        lanes=frozenset({ProviderRequestLane.OPTIONS_CHAIN}),
        authorized_until=authorized_until,
    )


def capture_options_breadth(
    provider,
    *,
    session_date: date,
    underlyings: Sequence[str],
    spots: Mapping[str, Decimal],
    equity_cutoff: datetime,
    cohort_id: UUID,
    authorization_reference: str,
    authorized_until: datetime,
    transport,
    deadline: datetime,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    """Capture each underlying's chain; never raises for one underlying's failure."""
    budget = breadth_budget(
        session_date=session_date, authorization_reference=authorization_reference,
        authorized_until=authorized_until,
    )
    summary: dict[str, object] = {}

    def before_request(underlying):
        def reserve(page, url):
            reserved_at = clock()
            reservation = ProviderRequestReservation(
                request_id=uuid4(), account_alias=budget.account_alias, lane=ProviderRequestLane.OPTIONS_CHAIN,
                endpoint_class=ENDPOINT_CLASS,
                request_sha256=canonical_sha256({
                    "policy": BREADTH_POLICY_SHA256, "underlying": underlying, "page": page, "url": url,
                    "equity_cutoff": equity_cutoff.isoformat(),
                }),
                reserved_at=reserved_at, cohort_id=cohort_id,
            )
            with provider.transaction(APP_ROLE) as unit:
                reserve_coordinated_provider_request_cursor(
                    unit.cursor, budget, reservation,
                    reserved_response_bytes=ACCOUNTED_RESPONSE_BYTE_CEILING, account_wide=True,
                )
            return reservation.request_id, reserved_at
        return reserve

    def after_request(token, *, status, response_bytes, rows, error):
        request_id, started = token
        completed = clock()
        outcome = (
            ProviderRequestOutcome.OK if error is None
            else ProviderRequestOutcome.HTTP_ERROR if status not in (None, 200)
            else ProviderRequestOutcome.INVALID
        )
        with provider.transaction(APP_ROLE) as unit:
            finish_provider_request_cursor(
                unit.cursor, request_id,
                ProviderRequestCompletion(
                    state=ProviderRequestState.COMPLETE if error is None else ProviderRequestState.FAILED,
                    started_at=started, completed_at=completed, outcome=outcome, http_status=status,
                    response_bytes=response_bytes, row_count=rows, pages_fetched=int(status is not None),
                    price_basis=PriceBasis.NOT_APPLICABLE,
                    completeness=(
                        RequestCompleteness.COMPLETE if error is None else RequestCompleteness.INCOMPLETE
                    ),
                    failure_code=None if error is None else type(error).__name__[:96],
                ),
            )

    for underlying in underlyings:
        started = clock()
        spot = spots.get(underlying)
        try:
            if spot is None:
                captured = UnderlyingBreadth(underlying, (), 0, 0, 0, "FAILED", "SPOT_UNAVAILABLE")
            else:
                captured = fetch_underlying_breadth(
                    transport, underlying=underlying, spot=spot, session_date=session_date, deadline=deadline,
                    clock=clock, before_request=before_request(underlying), after_request=after_request,
                )
        except ProviderBudgetExceeded as error:
            captured = UnderlyingBreadth(underlying, (), 0, 0, 0, "FAILED", f"BUDGET: {str(error)[:120]}")
        except Exception as error:  # noqa: BLE001 - one underlying never stops the others or the window
            _LOGGER.warning("options_breadth_capture_failed underlying=%s error=%s", underlying, error)
            captured = UnderlyingBreadth(underlying, (), 0, 0, 0, "FAILED", f"{type(error).__name__}: {error}"[:160])
        observed = clock()
        try:
            with provider.transaction(APP_ROLE) as unit:
                persist_breadth_cursor(
                    unit.cursor, session_date=session_date, equity_cutoff=equity_cutoff, started_at=started,
                    observed_at=max(observed, started), captured=captured,
                )
        except Exception as error:  # noqa: BLE001
            _LOGGER.warning("options_breadth_persist_failed underlying=%s error=%s", underlying, error)
            summary[underlying] = {"state": "FAILED", "reason": f"PERSIST: {type(error).__name__}"}
            continue
        summary[underlying] = {
            "state": captured.state, "contracts": len(captured.rows), "pages": captured.pages,
            "seconds": round((observed - started).total_seconds(), 1),
            **({"reason": captured.reason} if captured.reason else {}),
        }
    return summary


# ---- reads (Options Screener) -------------------------------------------------------------------------------------

BREADTH_QUALITY_FLAGS = ("BREADTH_SNAPSHOT", "PROVIDER_GREEKS", "NOT_DETECTOR_EVALUATED")


def _text(value) -> str | None:
    return None if value is None else format(Decimal(str(value)).normalize(), "f")


def _iso(value) -> str | None:
    return None if value is None else value.isoformat()


def breadth_screener_row(row: Mapping[str, object]) -> dict[str, object]:
    """One breadth contract in the Options Screener's row shape. Greeks and IV are the provider's (flagged), since no
    model values exist for contracts outside the stream; the mark is the quote midpoint."""
    bid, ask = row.get("bid"), row.get("ask")
    mark = (Decimal(str(bid)) + Decimal(str(ask))) / 2 if bid is not None and ask is not None and ask >= bid else None
    spot = row.get("underlying_price")
    strike = Decimal(str(row["strike_price"]))
    kind = "CALL" if row["contract_type"] == "call" else "PUT"
    otm = None
    if spot:
        spot = Decimal(str(spot))
        otm = (strike - spot) / spot if kind == "CALL" else (spot - strike) / spot
    volume, open_interest = row.get("day_volume"), row.get("open_interest")
    ratio = (
        Decimal(volume) / Decimal(open_interest)
        if volume is not None and open_interest
        else None
    )
    observed = row["observed_at"]
    return {
        "snapshot_id": str(UUID(canonical_sha256({
            "breadth": str(row["session_date"]), "contract": row["contract_ticker"],
        })[:32])),
        "contract_id": int(row.get("contract_id") or 0),
        "contract_ticker": str(row["contract_ticker"]),
        "underlying": str(row["underlying"]),
        "contract_type": kind,
        "expiration_date": row["expiration_date"].isoformat(),
        "strike": format(strike.normalize(), "f"),
        "calendar_dte": max(0, (row["expiration_date"] - row["session_date"]).days),
        "spot": _text(spot),
        "model_mark": _text(mark),
        "mark": _text(mark),
        "display_mark": _text(mark),
        "absolute_delta": None if row.get("delta") is None else abs(float(row["delta"])),
        "local_delta": row.get("delta"),
        "local_iv": row.get("implied_volatility"),
        "local_gamma": row.get("gamma"),
        "local_theta_per_day": row.get("theta"),
        "local_vega_per_vol_point": row.get("vega"),
        "local_rho_per_rate_point": None,
        "provider_iv": row.get("implied_volatility"),
        "provider_gamma": row.get("gamma"),
        "bid": _text(bid),
        "ask": _text(ask),
        "mark_market_data_time": _iso(row.get("quote_at")) or observed.isoformat(),
        "day_volume": volume,
        "open_interest": open_interest,
        "volume_open_interest_ratio": _text(ratio),
        "otm_fraction": _text(otm),
        "market_data_time": observed.isoformat(),
        "first_observed_at": observed.isoformat(),
        "mark_source": "BREADTH_QUOTE_MIDPOINT" if mark is not None else "UNAVAILABLE",
        "quality_flags": list(BREADTH_QUALITY_FLAGS),
        "asset_type": "ETF" if str(row["underlying"]) in {"SPY", "QQQ", "IWM", "DIA"} else "EQUITY",
        "batch_id": None,
        "matrix_id": None,
        "time_to_expiration_years": None,
        "risk_free_rate": None,
        "dividend_yield": None,
        "model_version": None,
        "valuation_policy_version": BREADTH_POLICY["version"],
        "valuation_policy_sha256": BREADTH_POLICY_SHA256,
        "coverage_source": "BREADTH",
        "last_trade_price": _text(row.get("last_trade_price")),
        "last_trade_at": _iso(row.get("last_trade_at")),
    }


def load_breadth_screener_rows_cursor(
    cursor,
    *,
    session_date: date,
    underlyings: Sequence[str],
    available_by: datetime,
) -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    """The breadth rows visible at `available_by` and, per underlying, its latest capture (for the freshness label)."""
    cursor.execute(
        "SELECT breadth.*,catalog.contract_id FROM rt.option_breadth_current AS breadth "
        "LEFT JOIN ref.option_contracts AS catalog ON catalog.contract_ticker=breadth.contract_ticker "
        "WHERE breadth.session_date=%s AND breadth.underlying=ANY(%s) AND breadth.observed_at<=%s",
        (session_date, list(underlyings), available_by),
    )
    columns = [column[0] for column in cursor.description]
    rows = [breadth_screener_row(dict(zip(columns, values))) for values in cursor.fetchall()]
    cursor.execute(
        "SELECT DISTINCT ON (underlying) underlying,state,observed_at,contracts,reason "
        "FROM rt.option_breadth_captures WHERE session_date=%s AND underlying=ANY(%s) AND observed_at<=%s "
        "ORDER BY underlying,observed_at DESC",
        (session_date, list(underlyings), available_by),
    )
    captures = {
        str(values[0]): {
            "breadth_state": values[1], "breadth_observed_at": values[2].isoformat(),
            "breadth_contracts": int(values[3]), "breadth_reason": values[4],
        }
        for values in cursor.fetchall()
    }
    return rows, captures
