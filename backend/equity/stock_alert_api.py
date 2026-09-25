"""Current read-only stock alert, review and market-context endpoints."""
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query

from equity.api import expected_materialized_market_time


router = APIRouter(prefix="/api/stocks", tags=["stock-alerts"])


@router.get("/market-conditions")
def market_conditions():
    from research.stock_rotation import load_rotation_page
    now = datetime.now(timezone.utc)
    expected = expected_materialized_market_time(now, "1d").date().isoformat()
    path = Path(__file__).resolve().parents[1] / "backups/stock-rotation/latest.json"
    try:
        return load_rotation_page(path, expected, now)
    except (ValueError, OSError, KeyError, TypeError) as error:
        raise HTTPException(status_code=503, detail="Stored market conditions are unavailable or incompatible") from error


@router.get("/alert-view")
def alert_view(source: Literal["SHADOW", "REPLAY", "LEGACY"] = "SHADOW", session_date: date | None = None,
               view: Literal["latest", "history", "open"] = "latest", run: str | None = Query(None, max_length=150),
               search: str = Query("", max_length=80), direction: Annotated[int | None, Query(ge=-1, le=1)] = None,
               model: str | None = None, interval: str | None = None, status: str | None = None,
               lane: Literal["TRADE", "WATCH"] | None = None, sort: str | None = None, descending: bool = True,
               offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200),
               trade_type: Literal["INTRADAY", "SWING"] | None = None, combined: bool | None = None):
    from psycopg2 import Error as DatabaseError
    from equity.stock_alert_views import attach_alert_context, current_history_prices, history_snapshot_for_date, load_alert_view, current_alert_schedules, forward_session_snapshot
    from research.stock_alerts import alert_page
    try:
        snapshot = load_alert_view(source) if combined is None else load_alert_view(source, combined=combined)
        snapshot = forward_session_snapshot(snapshot)
        if view == "history":
            snapshot = history_snapshot_for_date(snapshot, str(session_date) if session_date else None)
            snapshot = current_history_prices(snapshot, str(session_date) if session_date else None)
        elif view == "open":
            snapshot = current_history_prices(snapshot, view="open")
    except (ValueError, OSError, DatabaseError) as error:
        raise HTTPException(status_code=503, detail="Retained alert source is unavailable or incompatible") from error
    try:
        page = alert_page(snapshot, session=str(session_date) if session_date else None, view=view, run=run,
            search=search, direction=direction, model=model, interval=interval, status=status, lane=lane,
            sort=sort, descending=descending, offset=offset, limit=limit, trade_type=trade_type)
        page["schedule_streams"] = current_alert_schedules(snapshot)
        return attach_alert_context(page)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.get("/alert-eod-review")
def alert_eod_review(session_date: date | None = None, search: str = Query("", max_length=80),
                     model: Literal["resumption", "acceptance", "failure"] | None = None,
                     selection_status: Literal["SELECTED", "NOT_SELECTED", "REPEAT"] | None = None,
                     direction: Annotated[int | None, Query(ge=-1, le=1)] = None,
                     trade_type: Literal["INTRADAY", "SWING"] | None = None,
                     offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
    from psycopg2 import Error as DatabaseError
    from equity.stock_alert_results import load_stock_eod_review
    try:
        return load_stock_eod_review(as_of=datetime.now(timezone.utc), session_date=session_date,
            search=search, model=model, selection_status=selection_status, direction=direction,
            trade_type=trade_type, offset=offset, limit=limit)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except (OSError, DatabaseError) as error:
        raise HTTPException(status_code=503, detail="Retained stock evaluation evidence is unavailable") from error