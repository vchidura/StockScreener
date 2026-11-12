from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Callable, Literal
from uuid import UUID
import logging

from fastapi import FastAPI, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool
from equity.calendar import market_notification_window
from options.discovery import CONTRACT_FILTERS, EligibleChainQuery
from options.strategies.registry import build_discovery_catalog
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from research.screening import Query as ScreeningQuery, SnapshotRowRequest, GapDetailRequest

from .contracts import CANARY_UNIVERSE, EQUITY_VALIDATION_UNIVERSE
from .advanced_options_api import (
    AdvancedOptionsStoredProjectionRepository,
    advanced_options_v2_disabled_payload,
    advanced_options_v2_status_payload,
)
from .advanced_equity_preparation import (
    AdvancedEquityPreparationStatusRepository,
)
from .database_access import RealtimeDatabaseRole
from .migrations import EXPECTED_DATABASE_NAME
from .portal_identity import PortalIdentity, PortalIdentityRepository
from .retained_market_context import (
    retained_market_conditions,
    retained_market_regime,
    retained_sector_intelligence,
)
from .portal_compatibility_repository import (
    RealtimeOptionAlertRepository, RealtimeOptionFlowRepository, RealtimeOptionScreenerRepository,
    RealtimeOptionsAlertsPageRepository,
    RealtimeStockAlertRepository,
    RealtimeTickerScreenerRepository,
    RealtimeScreeningRepository,
    ScreeningPublicationUnavailable,
    ScreeningQueryRejected,
)
from .worker_database import RealtimeProcessDatabase
from .ticker_repository import RealtimeTickerWorkspaceRepository, TickerPublicationUnavailable
from .stock_alert_repository import (
    RealtimeStockAlertViewRepository, StockAlertPublicationUnavailable, StockAlertQueryRejected,
)
from .dashboard_repository import (
    RealtimeDashboardRepository, DashboardPublicationUnavailable, DashboardQueryRejected,
)
from .strategy_repository import RealtimeDailyStrategyRepository, StrategyPublicationUnavailable, StrategyQueryRejected
from psycopg2 import Error as DatabaseError


@dataclass(frozen=True, slots=True)
class RealtimePortalRepositories:
    ticker_screener: object
    stock_alerts: object
    option_alerts: object
    screening: RealtimeScreeningRepository | None = None
    ticker_workspace: RealtimeTickerWorkspaceRepository | None = None
    stock_alert_views: RealtimeStockAlertViewRepository | None = None
    dashboard: RealtimeDashboardRepository | None = None
    option_flow: RealtimeOptionFlowRepository | None = None
    option_screener: RealtimeOptionScreenerRepository | None = None
    option_alert_views: RealtimeOptionsAlertsPageRepository | None = None
    daily_strategies: RealtimeDailyStrategyRepository | None = None
    advanced_options: AdvancedOptionsStoredProjectionRepository | None = None
    equity_preparation: (
        AdvancedEquityPreparationStatusRepository | None
    ) = None
    portal_identity: PortalIdentityRepository | None = None


# Routes that never read a publication, so they need no current cohort.
_IDENTITY_FREE_PATHS = frozenset({
    "/api/realtime/health",
    "/api/realtime/portal/current",
    "/api/realtime/options-v2/status",
    "/api/realtime/equity-preparation",
    "/api/equity/notification-window",
})


def _is_options_read(path: str) -> bool:
    return (
        path == "/api/realtime/option-alerts"
        or path.startswith("/api/realtime/options-v2/")
        or (
            path.startswith("/api/options/")
            and not path.startswith("/api/options/calendar/")
        )
    )


def _current_cohort(
    identity: PortalIdentity,
    path: str,
    session_date: str | None,
    *,
    options_reads_enabled: bool,
) -> UUID | None:
    """Each product has its own per-session cohort; a requested session is served
    from the cohort that holds it. Options stays unresolved while its reads are off,
    because its page readers follow the release pointer directly."""
    if _is_options_read(path):
        if not options_reads_enabled:
            return None
        if session_date is not None:
            return identity.options_session_cohorts.get(session_date)
        return (
            None if identity.options is None else identity.options.cohort_id
        )
    if session_date in identity.session_cohorts:
        return identity.session_cohorts[session_date]
    return None if identity.equity is None else identity.equity.cohort_id


def build_realtime_portal_repositories(
    database: RealtimeProcessDatabase,
    *,
    advanced_options_v2_enabled: bool,
) -> RealtimePortalRepositories:
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("realtime portal database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.READER:
        raise ValueError("realtime portal requires the reader database role")
    if type(advanced_options_v2_enabled) is not bool:
        raise TypeError("Advanced Options stored-read flag is invalid")

    @contextmanager
    def cursor_factory():
        with database.transaction(
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as cursor:
            yield cursor

    @contextmanager
    def positional_cursor_factory():
        # The preparation status reader indexes its rows by position.
        with database.transaction(
            isolation_level="REPEATABLE READ",
        ) as cursor:
            yield cursor

    return RealtimePortalRepositories(
        ticker_screener=RealtimeTickerScreenerRepository(
            cursor_factory,
            transaction_configured=True,
        ),
        stock_alerts=RealtimeStockAlertRepository(
            cursor_factory,
            transaction_configured=True,
        ),
        option_alerts=RealtimeOptionAlertRepository(
            cursor_factory,
            transaction_configured=True,
        ),
        screening=RealtimeScreeningRepository(cursor_factory, transaction_configured=True),
        ticker_workspace=RealtimeTickerWorkspaceRepository(cursor_factory, transaction_configured=True),
        stock_alert_views=RealtimeStockAlertViewRepository(cursor_factory, transaction_configured=True),
        dashboard=RealtimeDashboardRepository(cursor_factory, transaction_configured=True),
        option_flow=RealtimeOptionFlowRepository(
            cursor_factory,
            transaction_configured=True,
            require_read_pin=True,
            reads_enabled=advanced_options_v2_enabled,
        ),
        option_screener=RealtimeOptionScreenerRepository(
            cursor_factory,
            transaction_configured=True,
            require_read_pin=True,
            reads_enabled=advanced_options_v2_enabled,
        ),
        option_alert_views=RealtimeOptionsAlertsPageRepository(
            cursor_factory,
            transaction_configured=True,
            require_read_pin=True,
            reads_enabled=advanced_options_v2_enabled,
        ),
        daily_strategies=RealtimeDailyStrategyRepository(cursor_factory, transaction_configured=True),
        advanced_options=AdvancedOptionsStoredProjectionRepository(
            cursor_factory,
            require_read_pin=True,
        ),
        equity_preparation=AdvancedEquityPreparationStatusRepository(
            positional_cursor_factory,
        ),
        portal_identity=PortalIdentityRepository(cursor_factory),
    )


class RealtimeOptionAlertManagementRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    policy_version: str = Field(min_length=1, max_length=80, pattern=r"\S")
    strategy_name: Literal["DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD"]
    stop_loss_fraction: Decimal = Field(gt=0, lt=1)
    take_profit_fraction: Decimal = Field(gt=0)
    maximum_hold_seconds: int = Field(strict=True, ge=1, le=366 * 86400)
    minimum_exit_dte: int = Field(default=1, strict=True, ge=1)


class RealtimeOptionAlertPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_deadline: AwareDatetime | None = None
    exit_deadline: AwareDatetime | None = None
    entry_limit: Decimal | None = Field(default=None, gt=0)
    management_policy: RealtimeOptionAlertManagementRequest | None = None

    @model_validator(mode="after")
    def validate_deadlines(self):
        if self.entry_deadline and self.exit_deadline and self.exit_deadline <= self.entry_deadline:
            raise ValueError("exit deadline must follow entry deadline")
        return self


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _provider_fetch_performed(payload: dict[str, object]) -> bool:
    if payload.get("provider_fetch_performed") is True:
        return True
    data = payload.get("data")
    return isinstance(data, dict) and data.get("provider_fetch_performed") is True


def build_realtime_canary_status(
    repositories: RealtimePortalRepositories,
    *,
    available_by: datetime,
    cohort_id: UUID | None = None,
) -> dict[str, object]:
    cohort_scope = {"cohort_id": cohort_id} if cohort_id is not None else {}
    ticker_rows = {
        ticker: repositories.ticker_screener.ticker_page(
            ticker,
            available_by=available_by,
            **cohort_scope,
        )
        for ticker in CANARY_UNIVERSE.equity_tickers
    }
    screening = repositories.ticker_screener.build_latest(
        available_by=available_by,
        **cohort_scope,
    )
    stock_alerts = repositories.stock_alerts.alert_view(
        available_by=available_by,
        limit=1,
        **cohort_scope,
    )
    option_alerts = repositories.option_alerts.option_alert_evaluations(
        available_by=available_by,
        limit=1,
        **cohort_scope,
    )
    reasons = []
    missing_tickers = tuple(
        ticker
        for ticker, row in ticker_rows.items()
        if row is None
        or not row.get("is_serveable")
        or row.get("payload", {}).get("ticker") != ticker
    )
    if missing_tickers:
        reasons.append("TICKER_PAGE_COVERAGE_INCOMPLETE")
    screening_tickers = {
        str(row.get("ticker") or "").upper()
        for row in screening.get("rows", ())
        if isinstance(row, dict)
    }
    if not screening.get("available") or screening_tickers != set(
        CANARY_UNIVERSE.equity_tickers
    ):
        reasons.append("SCREENING_COVERAGE_INCOMPLETE")
    if not stock_alerts.get("available"):
        reasons.append("STOCK_ALERT_PROJECTION_UNAVAILABLE")
    if not option_alerts.get("available"):
        reasons.append("OPTION_ALERT_PROJECTION_UNAVAILABLE")
    payloads = (
        *(row for row in ticker_rows.values() if row is not None),
        screening,
        stock_alerts,
        option_alerts,
    )
    provider_fetch = any(_provider_fetch_performed(payload) for payload in payloads)
    if provider_fetch:
        reasons.append("PORTAL_PROVIDER_FETCH_PROHIBITED")
    return {
        "status": "READY" if not reasons else "BLOCKED",
        "checked_at": available_by,
        "database_target": EXPECTED_DATABASE_NAME,
        "universe_version": CANARY_UNIVERSE.version,
        "universe_sha256": CANARY_UNIVERSE.sha256,
        "expected_tickers": list(CANARY_UNIVERSE.equity_tickers),
        "ready_tickers": sorted(set(CANARY_UNIVERSE.equity_tickers) - set(missing_tickers)),
        "missing_tickers": list(missing_tickers),
        "screening_tickers": sorted(screening_tickers),
        "stock_alerts_available": bool(stock_alerts.get("available")),
        "option_alerts_available": bool(option_alerts.get("available")),
        "provider_fetch_performed": provider_fetch,
        "canonical_fallback": False,
        "reason_codes": reasons,
    }


def build_realtime_equity_validation_status(
    repositories: RealtimePortalRepositories,
    *,
    available_by: datetime,
    cohort_id: UUID | None = None,
) -> dict[str, object]:
    cohort_scope = {"cohort_id": cohort_id} if cohort_id is not None else {}
    tickers = EQUITY_VALIDATION_UNIVERSE.equity_tickers
    ticker_rows = {
        ticker: repositories.ticker_screener.ticker_page(
            ticker,
            available_by=available_by,
            **cohort_scope,
        )
        for ticker in tickers
    }
    screening = repositories.ticker_screener.build_latest(
        available_by=available_by,
        **cohort_scope,
    )
    stock_alerts = repositories.stock_alerts.alert_view(
        available_by=available_by,
        limit=1,
        **cohort_scope,
    )
    missing_tickers = tuple(
        ticker
        for ticker, row in ticker_rows.items()
        if row is None
        or not row.get("is_serveable")
        or row.get("payload", {}).get("ticker") != ticker
    )
    screening_tickers = {
        str(row.get("ticker") or "").upper()
        for row in screening.get("rows", ())
        if isinstance(row, dict)
    }
    reasons = []
    if missing_tickers:
        reasons.append("TICKER_PAGE_COVERAGE_INCOMPLETE")
    if not screening.get("available") or screening_tickers != set(tickers):
        reasons.append("SCREENING_COVERAGE_INCOMPLETE")
    if not stock_alerts.get("available"):
        reasons.append("STOCK_ALERT_PROJECTION_UNAVAILABLE")
    payloads = (
        *(row for row in ticker_rows.values() if row is not None),
        screening,
        stock_alerts,
    )
    provider_fetch = any(_provider_fetch_performed(payload) for payload in payloads)
    if provider_fetch:
        reasons.append("PORTAL_PROVIDER_FETCH_PROHIBITED")
    return {
        "status": "READY" if not reasons else "BLOCKED",
        "checked_at": available_by,
        "database_target": EXPECTED_DATABASE_NAME,
        "universe_version": EQUITY_VALIDATION_UNIVERSE.version,
        "universe_sha256": EQUITY_VALIDATION_UNIVERSE.sha256,
        "expected_tickers": list(tickers),
        "ready_tickers": sorted(set(tickers) - set(missing_tickers)),
        "missing_tickers": list(missing_tickers),
        "screening_tickers": sorted(screening_tickers),
        "stock_alerts_available": bool(stock_alerts.get("available")),
        "option_alerts_available": False,
        "option_alerts_status": "NOT_APPLICABLE_EQUITY_VALIDATION",
        "provider_fetch_performed": provider_fetch,
        "canonical_fallback": False,
        "reason_codes": reasons,
    }


def create_realtime_portal_app(
    repositories: RealtimePortalRepositories,
    *,
    clock: Callable[[], datetime] = _now,
    database_target: str = EXPECTED_DATABASE_NAME,
    advanced_options_v2_enabled: bool = False,
) -> FastAPI:
    if not isinstance(repositories, RealtimePortalRepositories):
        raise TypeError("realtime portal repositories are invalid")
    app = FastAPI(
        title="StockScreener Realtime Canary API",
        version="1.0.0-canary",
    )
    app.state.database_target = database_target
    app.state.canonical_fallback = False

    def cutoff() -> datetime:
        value = clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError("realtime portal clock must be timezone-aware")
        return value.astimezone(timezone.utc)

    def unavailable(error: Exception) -> HTTPException:
        return HTTPException(
            status_code=503,
            detail=f"Realtime portal projection unavailable: {type(error).__name__}",
        )

    @app.middleware("http")
    async def serve_current_publication(request: Request, call_next):
        """A reader naming no cohort gets the current publication. An explicit
        cohort is never substituted; repositories still enforce authorization."""
        cohort_id = None
        if (
            repositories.portal_identity is not None
            and request.url.path.startswith("/api/")
            and request.url.path not in _IDENTITY_FREE_PATHS
            and "cohort_id" not in request.query_params
        ):
            try:
                identity = await run_in_threadpool(
                    repositories.portal_identity.current,
                    available_by=cutoff(),
                )
            except DatabaseError:
                # The route itself reports the unavailable read.
                logging.getLogger(__name__).exception(
                    "Realtime portal identity read failed"
                )
            else:
                cohort_id = _current_cohort(
                    identity,
                    request.url.path,
                    request.query_params.get("session_date"),
                    options_reads_enabled=advanced_options_v2_enabled,
                )
                if cohort_id is not None:
                    request.state.portal_identity = identity
                    query = request.scope["query_string"]
                    request.scope["query_string"] = (
                        query + (b"&" if query else b"")
                        + b"cohort_id=" + str(cohort_id).encode("ascii")
                    )
        response = await call_next(request)
        if cohort_id is not None:
            response.headers["X-Realtime-Cohort-Id"] = str(cohort_id)
        return response

    @app.get("/api/realtime/portal/current")
    def portal_current():
        if repositories.portal_identity is None:
            raise HTTPException(503, "Realtime portal identity is unavailable")
        available_by = cutoff()
        try:
            return repositories.portal_identity.current(
                available_by=available_by,
            ).payload(
                available_by,
                options_reads_enabled=advanced_options_v2_enabled,
            )
        except DatabaseError as error:
            raise unavailable(error) from error

    def across_sessions(request: Request, cohort_id: UUID, read):
        """Sessions span cohorts, because each session has its own.

        A reader naming no cohort gets all of them, and so does one naming the
        current cohort: the portal names the cohort it was just given, so its
        date pickers listed only today (found Nov 5). Any other explicit cohort
        lists only itself.
        """
        identity = getattr(request.state, "portal_identity", None)
        if (
            identity is None
            and cohort_id is not None
            and repositories.portal_identity is not None
        ):
            try:
                current = repositories.portal_identity.current(
                    available_by=cutoff(),
                )
            except DatabaseError:
                logging.getLogger(__name__).exception(
                    "Realtime portal identity read failed"
                )
            else:
                if (
                    current.equity is not None
                    and current.equity.cohort_id == cohort_id
                ):
                    identity = current
        if identity is None:
            return read(cohort_id)
        results = []
        unavailable_error = None
        for cohort in dict.fromkeys(
            (cohort_id, *identity.session_cohorts.values())
        ):
            try:
                results.append(read(cohort))
            except TickerPublicationUnavailable as error:
                unavailable_error = error
        if not results:
            raise unavailable_error
        snapshots = {}
        for result in reversed(results):
            snapshots.update(result.get("snapshots") or {})
        merged = {
            **results[0],
            "sessions": sorted(
                {
                    session
                    for result in results
                    for session in result["sessions"]
                }
            ),
            "snapshots": snapshots,
        }
        # The named cohort may hold nothing yet, early in its session. The portal
        # refuses a list answered for another cohort, so always answer for this one.
        if "cohort_id" in merged:
            merged["cohort_id"] = str(cohort_id)
        return merged

    @app.get("/api/realtime/health")
    def health():
        return {
            "status": "READY",
            "database_target": app.state.database_target,
            "canonical_fallback": False,
            "provider_fetch_performed": False,
        }

    @app.get("/api/realtime/options-v2/status")
    def advanced_options_v2_status():
        return advanced_options_v2_status_payload(
            enabled=advanced_options_v2_enabled,
        )

    @app.get(
        "/api/realtime/options-v2/projections/{snapshot_type}"
    )
    def advanced_options_v2_projection(
        snapshot_type: str,
        cohort_id: UUID,
    ):
        if not advanced_options_v2_enabled:
            return advanced_options_v2_disabled_payload(snapshot_type)
        if repositories.advanced_options is None:
            raise HTTPException(
                503,
                "Advanced Options V2 repository is unavailable",
            )
        try:
            return repositories.advanced_options.read(
                cohort_id=cohort_id,
                snapshot_type=snapshot_type,
                available_by=cutoff(),
            )
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except DatabaseError as error:
            raise unavailable(error) from error

    @app.get("/api/equity/notification-window")
    def notification_window():
        return market_notification_window(cutoff())

    def screening_read(method, request=None, cohort_id=None, **params):
        if repositories.screening is None:
            raise HTTPException(503, "Realtime screening repository is unavailable")
        try:
            reader = getattr(repositories.screening, method)
            args = (request,) if request is not None else ()
            return reader(*args, available_by=cutoff(), cohort_id=cohort_id, **params)
        except ScreeningPublicationUnavailable as error:
            raise HTTPException(404, str(error)) from error
        except ScreeningQueryRejected as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime screening stored projection read failed")
            raise unavailable(error) from error

    @app.get("/api/stocks/screening/catalog")
    def screening_catalog(cohort_id: UUID | None = None):
        return screening_read("catalog", cohort_id=cohort_id)

    @app.post("/api/stocks/screening/query")
    def screening_query(
        request: ScreeningQuery,
        cohort_id: UUID | None = None,
        read_mode: Literal["current", "retained"] = "current",
    ):
        return screening_read("query", request, cohort_id, read_mode=read_mode)

    @app.get("/api/stocks/screening/status")
    def screening_status(
        cohort_id: UUID | None = None,
        read_mode: Literal["current", "retained"] = "retained",
    ):
        return screening_read("status", cohort_id=cohort_id, read_mode=read_mode)

    @app.post("/api/stocks/screening/gap-details")
    def screening_gap_details(
        request: GapDetailRequest,
        cohort_id: UUID | None = None,
        read_mode: Literal["current", "retained"] = "current",
    ):
        return screening_read("gap_details", request, cohort_id, read_mode=read_mode)

    @app.post("/api/stocks/screening/hourly-details")
    def screening_hourly_details(
        request: SnapshotRowRequest,
        cohort_id: UUID | None = None,
        read_mode: Literal["current", "retained"] = "current",
    ):
        return screening_read("hourly_details", request, cohort_id, read_mode=read_mode)

    def stock_alert_read(method, cohort_id, **filters):
        if repositories.stock_alert_views is None:
            raise HTTPException(503, "Realtime Stock Alerts repository is unavailable")
        try:
            return getattr(repositories.stock_alert_views, method)(
                available_by=cutoff(), cohort_id=cohort_id, **filters,
            )
        except StockAlertPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except StockAlertQueryRejected as error:
            raise HTTPException(422, str(error)) from error
        except (ValueError, KeyError, TypeError, DatabaseError) as error:
            logging.getLogger(__name__).exception("Realtime stock alert retained projection read failed")
            raise unavailable(error) from error

    @app.get("/api/stocks/alert-view")
    def stock_alert_view(
        cohort_id: UUID | None = None, source: Literal["SHADOW", "REPLAY"] = "SHADOW",
        session_date: date | None = None, view: Literal["latest", "history", "open"] = "latest",
        run: str | None = Query(None, max_length=150), search: str = Query("", max_length=80),
        direction: int | None = Query(None, ge=-1, le=1), model: str | None = None,
        interval: str | None = None, status: str | None = None,
        lane: Literal["TRADE", "WATCH"] | None = None, sort: str | None = None,
        descending: bool = True, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200),
        trade_type: Literal["INTRADAY", "SWING"] | None = None, combined: bool | None = None,
    ):
        return stock_alert_read(
            "alert_view", cohort_id, source=source, session_date=session_date, view=view, run=run,
            search=search, direction=direction, model=model, interval=interval, status=status,
            lane=lane, sort=sort, descending=descending, offset=offset, limit=limit,
            trade_type=trade_type, combined=combined,
        )

    @app.get("/api/stocks/alert-eod-review")
    def stock_alert_eod_review(
        cohort_id: UUID | None = None, session_date: date | None = None,
        search: str = Query("", max_length=80),
        model: Literal["resumption", "acceptance", "failure"] | None = None,
        selection_status: Literal["SELECTED", "NOT_SELECTED", "REPEAT"] | None = None,
        direction: int | None = Query(None, ge=-1, le=1),
        trade_type: Literal["INTRADAY", "SWING"] | None = None,
        offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200),
    ):
        return stock_alert_read(
            "eod_review", cohort_id, session_date=session_date, search=search, model=model,
            selection_status=selection_status, direction=direction, trade_type=trade_type,
            offset=offset, limit=limit,
        )

    def ticker_read(ticker, contract, cohort_id, **params):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return repositories.ticker_workspace.read(
                ticker, contract, available_by=cutoff(), cohort_id=cohort_id, **params,
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            logging.getLogger(__name__).exception("Realtime ticker contract rejected")
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime ticker stored projection read failed")
            raise unavailable(error) from error

    @app.get("/api/stock/{ticker}/chart")
    def ticker_chart(
        ticker: str, interval: Literal["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"] = "1d",
        period: Literal["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "4y", "5y"] = "1y",
        refresh: bool = False, cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
        read_mode: Literal["current", "retained", "session_close"] = "current",
    ):
        return ticker_read(ticker, "chart", cohort_id, interval=interval, period=period, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/stock/{ticker}/chart/forming")
    def ticker_chart_forming(
        ticker: str, interval: Literal["5m", "15m", "30m", "1h", "1d", "1wk", "1mo"] = "1d",
        cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
        read_mode: Literal["current", "retained", "session_close"] = "current",
    ):
        """Nov 12 (user): the developing bar of a chart interval (display only; `status` NONE when there is none)."""
        return ticker_read(ticker, "forming", cohort_id, interval=interval, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/stock/{ticker}/chart-patterns")
    def ticker_patterns(ticker: str, interval: str = "1d", refresh: bool = False, cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "patterns", cohort_id, interval=interval, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/stock/{ticker}/price-channel")
    def ticker_channel(ticker: str, interval: str = "1d", refresh: bool = False, cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "channel", cohort_id, interval=interval, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/chart-patterns/ticker/{ticker}")
    def ticker_cross_patterns(ticker: str, refresh: bool = False, cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "cross_patterns", cohort_id, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/tickers")
    def ticker_catalog(refresh: bool = False, cohort_id: UUID | None = None):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return repositories.ticker_workspace.list_tickers(
                available_by=cutoff(), cohort_id=cohort_id,
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            raise unavailable(error) from error

    @app.get("/api/trading-sessions")
    def trading_sessions(
        request: Request,
        interval: Literal["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"] = "1d",
        limit: int = Query(260, ge=1, le=2_000),
        cohort_id: UUID | None = None,
    ):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return across_sessions(
                request,
                cohort_id,
                lambda cohort: repositories.ticker_workspace.trading_sessions(
                    interval=interval,
                    limit=limit,
                    available_by=cutoff(),
                    cohort_id=cohort,
                ),
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception(
                "Realtime ticker trading sessions read failed")
            raise unavailable(error) from error

    @app.get("/api/tickers/workspace-publication")
    def ticker_workspace_publication(cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
                                    read_mode: Literal["current", "retained", "session_close"] = "current",
                                    session_date: str | None = None):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return repositories.ticker_workspace.workspace_publication(
                available_by=cutoff(), cohort_id=cohort_id, snapshot_id=snapshot_id, read_mode=read_mode, session_date=session_date,
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime ticker workspace summary failed")
            raise unavailable(error) from error

    @app.get("/api/tickers/sessions")
    def all_ticker_closing_sessions(request: Request, cohort_id: UUID):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return across_sessions(request, cohort_id, lambda cohort: repositories.ticker_workspace.workspace_closing_sessions(
                available_by=cutoff(), cohort_id=cohort,
            ))
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("All Tickers closing-session lookup failed")
            raise unavailable(error) from error

    @app.get("/api/chart-patterns/scan")
    def pattern_watch_scan(
        interval: Literal["30m", "1h", "1d", "1wk", "1mo"] = "1d",
        limit: int = Query(1000, ge=1, le=1500),
        refresh: bool = False,
        cohort_id: UUID | None = None,
        snapshot_id: UUID | None = None,
        read_mode: Literal["current", "retained", "session_close"] = "current",
        session_date: str | None = None,
    ):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return repositories.ticker_workspace.scan_patterns(
                interval=interval, limit=limit, available_by=cutoff(), cohort_id=cohort_id, snapshot_id=snapshot_id, read_mode=read_mode,
                session_date=session_date,
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            raise unavailable(error) from error

    @app.get("/api/chart-patterns/sessions")
    def pattern_watch_closing_sessions(request: Request, cohort_id: UUID):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return across_sessions(request, cohort_id, lambda cohort: repositories.ticker_workspace.pattern_closing_sessions(
                available_by=cutoff(), cohort_id=cohort,
            ))
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Pattern Watch closing-session lookup failed")
            raise unavailable(error) from error

    @app.get("/api/stock/{ticker}/quote")
    def ticker_quote(ticker: str, refresh: bool = False, cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "quote", cohort_id, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/stock/{ticker}/workspace-publication")
    def ticker_publication(ticker: str, cohort_id: UUID, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current", session_date: str | None = None):
        return ticker_read(ticker, "publication", cohort_id, snapshot_id=snapshot_id, read_mode=read_mode, session_date=session_date)

    @app.get("/api/stock/{ticker}/sessions")
    def ticker_closing_sessions(request: Request, ticker: str, cohort_id: UUID):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return across_sessions(request, cohort_id, lambda cohort: repositories.ticker_workspace.read(
                ticker, "sessions", available_by=cutoff(), cohort_id=cohort,
            ))
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime ticker closing-session lookup failed")
            raise unavailable(error) from error

    @app.get("/api/stock/{ticker}/trade-setup/multi")
    def ticker_multi(ticker: str, refresh: bool = False, cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "multi", cohort_id, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/equity/security/{ticker}")
    def ticker_profile(ticker: str, report_limit: int = Query(12, ge=0, le=100), cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "profile", cohort_id, report_limit=report_limit, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/options/calendar/{ticker}")
    def ticker_calendar(ticker: str, days_forward: int = Query(30, ge=1, le=120), cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        return ticker_read(ticker, "calendar", cohort_id, days_forward=days_forward, snapshot_id=snapshot_id, read_mode=read_mode)

    @app.get("/api/options/flow")
    def option_flow(
        underlyer: str | None = None, session_date: date | None = None,
        cohort_id: UUID | None = None,
        read_mode: Literal["current", "retained"] = "current",
    ):
        if repositories.option_flow is None:
            raise HTTPException(503, "Realtime Options Flow repository is unavailable")
        try:
            return repositories.option_flow.flow(
                available_by=cutoff(), cohort_id=cohort_id,
                underlyer=underlyer, session_date=session_date,
                read_mode=read_mode,
            )
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime Options Flow stored read failed")
            raise unavailable(error) from error

    @app.get("/api/options/discovery-catalog")
    def option_discovery_catalog(cohort_id: UUID | None = None):
        if cohort_id is None:
            raise HTTPException(422, "Options Screener requires an explicit realtime cohort")
        from options.screens import builtin_screen_catalog

        return {**build_discovery_catalog(), "contract_filters": CONTRACT_FILTERS, "screens": builtin_screen_catalog()}

    @app.post("/api/options/eligible-chain/query")
    def option_eligible_chain(
        request: EligibleChainQuery,
        cohort_id: UUID | None = None,
        session_date: date | None = None,
    ):
        if repositories.option_screener is None:
            raise HTTPException(503, "Realtime Options Screener repository is unavailable")
        if (
            session_date is not None
            and request.session_date != session_date
        ):
            raise HTTPException(
                422,
                "Options Screener session identity mismatch",
            )
        try:
            return repositories.option_screener.eligible_chain(
                request, available_by=cutoff(), cohort_id=cohort_id,
            )
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime Options Screener stored read failed")
            raise unavailable(error) from error

    def option_alert_page_read(contract, cohort_id, **params):
        if repositories.option_alert_views is None:
            raise HTTPException(503, "Realtime Options Alerts repository is unavailable")
        try:
            return repositories.option_alert_views.read(
                contract, available_by=cutoff(), cohort_id=cohort_id, **params,
            )
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime Options Alerts stored read failed")
            raise unavailable(error) from error

    @app.get("/api/options/alerts/datasets")
    def option_alert_datasets(cohort_id: UUID | None = None):
        return option_alert_page_read("datasets", cohort_id)

    @app.get("/api/options/alerts/schedule")
    def option_alert_schedule(cohort_id: UUID | None = None):
        return option_alert_page_read("schedule", cohort_id)

    @app.get("/api/options/alerts/evaluations")
    def option_alert_evaluations(
        session_date: date | None = None,
        dataset_id: str | None = Query(None, min_length=1, max_length=80),
        detector: Literal["O1", "O2", "O3", "S1", "S2", "OS1", "OS2"] | None = None,
        selection_status: Literal["SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"] | None = None,
        underlyer: str | None = None,
        sort_by: Literal["triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"] = "run",
        sort_order: Literal["asc", "desc"] = "asc",
        limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
        cohort_id: UUID | None = None,
    ):
        return option_alert_page_read("evaluations", cohort_id, query=dict(session_date=session_date, dataset_id=dataset_id,
            detector=detector, selection_status=selection_status, underlyer=underlyer,
            sort_by=sort_by, sort_order=sort_order, limit=limit, offset=offset))

    @app.get("/api/options/alerts/detector-runs")
    def option_alert_detector_runs(
        dataset_id: str | None = Query(None, min_length=1, max_length=80),
        scope: Literal["LATEST", "HISTORY"] = "LATEST", session_date: date | None = None,
        session_rollup: bool = False,
        detector: Literal["O1", "O2", "O3", "S1", "S2", "OS1", "OS2"] | None = None,
        underlyer: str | None = None,
        sort_by: Literal["triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"] = "run",
        sort_order: Literal["asc", "desc"] = "asc",
        limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
        cohort_id: UUID | None = None,
    ):
        if dataset_id is None:
            raise HTTPException(422, "Detector dataset identity is required")
        return option_alert_page_read("detector_runs", cohort_id, query=dict(dataset_id=dataset_id, scope=scope,
            session_date=session_date, session_rollup=session_rollup, detector=detector, underlyer=underlyer,
            sort_by=sort_by, sort_order=sort_order, limit=limit, offset=offset))

    @app.get("/api/options/candidates/{candidate_id}")
    def option_alert_candidate(candidate_id: UUID, cohort_id: UUID | None = None):
        return option_alert_page_read("candidate", cohort_id, candidate_id=candidate_id)

    @app.get("/api/options/quotes/current")
    def option_alert_quotes(
        contract_id: list[int] | None = Query(None), cohort_id: UUID | None = None,
    ):
        ids = contract_id or []
        if not 1 <= len(ids) <= 8 or any(value <= 0 for value in ids):
            raise HTTPException(422, "Current option quotes require 1-8 positive contract IDs")
        return option_alert_page_read(
            "quotes", cohort_id, requested_contract_ids=tuple(ids),
        )

    @app.post("/api/options/alerts/preview/{candidate_id}")
    def option_alert_preview(
        candidate_id: UUID, request: RealtimeOptionAlertPreviewRequest,
        cohort_id: UUID | None = None,
    ):
        return option_alert_page_read("preview", cohort_id, candidate_id=candidate_id,
            preview_request=request.model_dump())

    def retained_workspace_summary(cohort_id):
        if repositories.ticker_workspace is None:
            raise HTTPException(503, "Realtime Ticker Workspace repository is unavailable")
        try:
            return repositories.ticker_workspace.workspace_publication(
                available_by=cutoff(),
                cohort_id=cohort_id,
                read_mode="retained",
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception(
                "Realtime retained market context read failed"
            )
            raise unavailable(error) from error

    @app.get("/api/stocks/market-conditions")
    def market_conditions(cohort_id: UUID | None = None):
        return retained_market_conditions(retained_workspace_summary(cohort_id))

    @app.get("/api/sector-intelligence")
    def sector_intelligence(
        leader_limit: int = Query(5, ge=1, le=20),
        cohort_id: UUID | None = None,
    ):
        return retained_sector_intelligence(
            retained_workspace_summary(cohort_id),
            leader_limit=leader_limit,
        )

    @app.get("/api/market-regime")
    def market_regime(cohort_id: UUID | None = None):
        return retained_market_regime(retained_workspace_summary(cohort_id))

    def dashboard_read(contract, cohort_id, snapshot_id=None, **params):
        if repositories.dashboard is None:
            raise HTTPException(503, "Realtime Dashboard repository is unavailable")
        try:
            return repositories.dashboard.read(
                contract, available_by=cutoff(), cohort_id=cohort_id, snapshot_id=snapshot_id, **params,
            )
        except DashboardPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except DashboardQueryRejected as error:
            raise HTTPException(422, str(error)) from error
        except (ValueError, KeyError, TypeError, DatabaseError) as error:
            logging.getLogger(__name__).exception("Realtime Dashboard retained projection read failed")
            raise unavailable(error) from error

    @app.get("/api/strategies/daily")
    def daily_strategy_release(request: Request, cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
                               strategy: str | None = None, ticker: str | None = None, view: str | None = None,
                               evaluation_session: str | None = None, interval: str = "1d", read_mode: Literal["current", "retained"] = "current",
                               session_date: str | None = None):
        if set(request.query_params) - {"cohort_id", "snapshot_id", "strategy", "ticker", "view", "evaluation_session", "interval", "read_mode", "session_date"} or strategy and ticker:
            raise HTTPException(422, "Daily strategy releases retain only exact-cohort defaults")
        if repositories.daily_strategies is None:
            raise HTTPException(503, "Independent daily strategy repository is unavailable")
        try:
            return repositories.daily_strategies.read(available_by=cutoff(), cohort_id=cohort_id,
                snapshot_id=snapshot_id, strategy=strategy, ticker=ticker, view=view, evaluation_session=evaluation_session, interval=interval,
                read_mode=read_mode, session_date=session_date)
        except StrategyPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except StrategyQueryRejected as error:
            raise HTTPException(422, str(error)) from error
        except (ValueError, KeyError, TypeError, DatabaseError) as error:
            logging.getLogger(__name__).exception("Independent daily strategy retained read failed")
            raise unavailable(error) from error

    @app.get("/api/latest-price-date")
    def dashboard_latest(cohort_id: UUID | None = None, snapshot_id: UUID | None = None, refresh: bool = False):
        return dashboard_read("latest", cohort_id, snapshot_id)

    @app.get("/api/equity/materialization-status")
    def dashboard_materialization_status(
        cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
    ):
        if snapshot_id is not None or repositories.ticker_workspace is None:
            return dashboard_read("materialization_status", cohort_id, snapshot_id)
        if repositories.dashboard is not None:
            try:
                return dashboard_read("materialization_status", cohort_id)
            except HTTPException as error:
                if error.status_code != 503:
                    raise
        try:
            return repositories.ticker_workspace.materialization_status(
                available_by=cutoff(), cohort_id=cohort_id,
            )
        except TickerPublicationUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            logging.getLogger(__name__).exception("Realtime ticker freshness read failed")
            raise unavailable(error) from error

    @app.get("/api/scan/gaps")
    @app.get("/api/scan/fvg")
    @app.get("/api/scan/ma-crossover")
    @app.get("/api/scan/momentum-pullback")
    @app.get("/api/scan/bearish-bounce")
    @app.get("/api/scan/fibonacci")
    def daily_scanner(
        request: Request, cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
        interval: str = "1d", short_period: int = 9, long_period: int = 21,
        lookback: int = 50, min_swing_pct: float = 5.0, refresh: bool = False,
    ):
        contract = request.url.path.rsplit("/", 1)[-1]
        allowed = {"cohort_id", "snapshot_id", "interval", "refresh"}
        allowed.update({"ma-crossover": {"short_period", "long_period"}, "fvg": {"lookback"},
                        "fibonacci": {"min_swing_pct"}}.get(contract, set()))
        if set(request.query_params) - allowed:
            raise HTTPException(422, "Daily scanners retain only exact-cohort default parameters; ticker/history overrides are unavailable")
        return dashboard_read(contract, cohort_id, snapshot_id, interval=interval,
                              short_period=short_period, long_period=long_period,
                              lookback=lookback, min_swing_pct=min_swing_pct)

    @app.get("/api/tickers/overview")
    def dashboard_overview(
        cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
        scan_date: date | None = None, refresh: bool = False,
    ):
        return dashboard_read("overview", cohort_id, snapshot_id, scan_date=scan_date)

    @app.get("/api/scan/all")
    def dashboard_scan(
        cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
        scan_date: date | None = None, min_swing_pct: float = Query(5.0, ge=3.0, le=15.0),
        refresh: bool = False, tickers: str | None = None,
    ):
        if tickers is not None:
            raise HTTPException(422, "Dashboard scans retain the exact published cohort; ticker overrides are unsupported")
        return dashboard_read("scan", cohort_id, snapshot_id, scan_date=scan_date, min_swing_pct=min_swing_pct)

    @app.get("/api/tickers/setup-summary")
    def tickers_setup_summary(
        cohort_id: UUID | None = None, snapshot_id: UUID | None = None,
        interval: Literal["30m", "1h", "1d", "1wk", "1mo"] = "1d",
    ):
        return dashboard_read("setup_summary", cohort_id, snapshot_id, interval=interval)

    @app.get("/api/stocks/context")
    def ticker_context(ticker: str | None = None, cohort_id: UUID | None = None, snapshot_id: UUID | None = None, read_mode: Literal["current", "retained", "session_close"] = "current"):
        if ticker is not None:
            return ticker_read(ticker, "context", cohort_id, snapshot_id=snapshot_id, read_mode=read_mode)
        return dashboard_read("context", cohort_id, snapshot_id)

    @app.get("/api/realtime/canary")
    def canary(cohort_id: UUID | None = None):
        try:
            return build_realtime_canary_status(
                repositories,
                available_by=cutoff(),
                cohort_id=cohort_id,
            )
        except Exception as error:
            raise unavailable(error) from error

    @app.get("/api/realtime/equity-validation")
    def equity_validation(cohort_id: UUID | None = None):
        try:
            return build_realtime_equity_validation_status(
                repositories,
                available_by=cutoff(),
                cohort_id=cohort_id,
            )
        except Exception as error:
            raise unavailable(error) from error

    @app.get("/api/realtime/equity-preparation")
    def equity_preparation(ticker: str | None = None):
        if repositories.equity_preparation is None:
            raise HTTPException(
                status_code=503,
                detail="Equity preparation status is unavailable",
            )
        try:
            result = repositories.equity_preparation.read(
                active_on=cutoff().date(),
                ticker=ticker,
            )
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except Exception as error:
            raise unavailable(error) from error
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Equity preparation status is unavailable",
            )
        return result

    @app.get("/api/realtime/tickers/{ticker}")
    def ticker_page(ticker: str, cohort_id: UUID | None = None):
        symbol = ticker.strip().upper()
        if symbol not in set(EQUITY_VALIDATION_UNIVERSE.equity_tickers):
            if repositories.equity_preparation is None:
                raise HTTPException(
                    status_code=422,
                    detail="Ticker is outside the canary",
                )
            try:
                preparation = repositories.equity_preparation.read(
                    active_on=cutoff().date(),
                    ticker=symbol,
                )
            except Exception as error:
                raise unavailable(error) from error
            if preparation is None or not preparation["members"]:
                raise HTTPException(
                    status_code=404,
                    detail="Ticker is not in desired membership",
                )
            member = preparation["members"][0]
            return {
                "payload": {"ticker": symbol, "setups": []},
                "is_fresh": False,
                "is_serveable": False,
                "available_intervals": member["available_intervals"],
                "preparation": member,
                "active_snapshot": preparation["active_snapshot"],
                "canonical_fallback": False,
                "provider_fetch_performed": False,
            }
        try:
            row = repositories.ticker_screener.ticker_page(
                symbol,
                available_by=cutoff(),
                cohort_id=cohort_id,
            )
        except Exception as error:
            raise unavailable(error) from error
        if row is None:
            raise HTTPException(status_code=404, detail="Ticker projection unavailable")
        return row

    @app.get("/api/realtime/screening")
    def screening(
        session_date: date | None = None,
        allow_degraded: bool = False,
        cohort_id: UUID | None = None,
    ):
        try:
            return repositories.ticker_screener.build_latest(
                session_date,
                available_by=cutoff(),
                allow_degraded=allow_degraded,
                cohort_id=cohort_id,
            )
        except Exception as error:
            raise unavailable(error) from error

    @app.get("/api/realtime/stock-alerts")
    def stock_alerts(
        view: Literal["latest", "history", "open"] = "latest",
        session_date: date | None = None,
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=200),
        cohort_id: UUID | None = None,
    ):
        try:
            return repositories.stock_alerts.alert_view(
                available_by=cutoff(),
                view=view,
                session_date=session_date,
                offset=offset,
                limit=limit,
                cohort_id=cohort_id,
            )
        except Exception as error:
            raise unavailable(error) from error

    @app.get("/api/realtime/option-alerts")
    def option_alerts(
        dataset_id: UUID | None = None,
        detector: Literal["O1", "O2", "O3", "OS1", "OS2"] | None = None,
        underlyer: str | None = None,
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=200),
        cohort_id: UUID | None = None,
    ):
        try:
            return repositories.option_alerts.option_alert_evaluations(
                available_by=cutoff(),
                dataset_id=dataset_id,
                detector=detector,
                underlyer=underlyer,
                offset=offset,
                limit=limit,
                cohort_id=cohort_id,
            )
        except Exception as error:
            raise unavailable(error) from error

    return app