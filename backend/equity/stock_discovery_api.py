"""Read-only stock discovery and paper alert ledger endpoints."""
from datetime import date, datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Query

from database import get_db_cursor
from equity.api import expected_materialized_market_time
from equity.stock_discovery import ALERT_SOURCE, MARK_SOURCE, VERSION, filter_stocks, NOTIONAL, COST_BPS
from equity.stock_discovery_service import load_snapshot
from equity.stock_alert_api import alert_view as alert_view, alert_eod_review as alert_eod_review, market_conditions as market_conditions

router = APIRouter(prefix="/api/stocks", tags=["stock-discovery"])


@router.get("/screener")
def screener(session_date: date | None = None, search: str = Query("", max_length=80), sector: str | None = None,
             side: Literal["LONG_INTEREST", "BEARISH_RISK", "NEUTRAL"] | None = None,
             state: str | None = None, eligible_only: bool = True,
             minimum_price: float = Query(0, ge=0), minimum_dollar_volume: float = Query(0, ge=0),
             minimum_relative_volume: float = Query(0, ge=0),
             sort: Literal["RANK", "WEAKEST", "CHANGE", "VOLUME", "LIQUIDITY", "VOLATILITY"] = "RANK",
             offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
    now = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        stamp, rows = load_snapshot(cursor, session_date)
    result = filter_stocks(rows, search=search, sector=sector, side=side, state=state, eligible_only=eligible_only,
                           minimum_price=minimum_price, minimum_dollar_volume=minimum_dollar_volume,
                           minimum_relative_volume=minimum_relative_volume, sort=sort, offset=offset, limit=limit)
    result["rows"] = [{key: value for key, value in row.items() if key != "source_ids"} for row in result["rows"]]
    return dict(result, session=str(stamp["market_time"].date()) if stamp else None,
                market_time=stamp["market_time"] if stamp else None, observed_at=stamp["observed_at"] if stamp else None,
                stale=bool(stamp and session_date is None and stamp["market_time"] < expected_materialized_market_time(now, "1d")),
                status="READY" if stamp else "AWAITING_FIRST_SNAPSHOT", ranking="12_MINUS_1_MONTH_MOMENTUM",
                capture_status=rows[0].get("capture_status") if rows else None)


@router.get("/alerts")
def alerts(search: str = Query("", max_length=80), direction: Literal[-1, 0, 1] | None = None,
           criterion: str | None = None, status: str | None = None, days: int = Query(30, ge=1, le=90),
           offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
    now = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        stamp, _ = load_snapshot(cursor)
        cursor.execute("""
            WITH alerts AS MATERIALIZED (
                SELECT evidence_id,ticker,direction,observed_at,market_time,payload FROM equity_evidence
                WHERE source_name=%s AND source_version=%s AND observed_at >= %s
                  AND (%s='' OR ticker ILIKE %s) AND (%s::int IS NULL OR direction=%s)
                  AND (%s::text IS NULL OR payload->'criteria' ? %s)
            ), marks AS (
                SELECT DISTINCT ON(mark.payload->>'alert_id') mark.payload->>'alert_id' AS alert_id,
                       mark.payload,mark.observed_at
                FROM equity_evidence mark JOIN alerts ON mark.payload->>'alert_id'=alerts.evidence_id::text
                WHERE mark.source_name=%s AND mark.source_version=%s
                ORDER BY mark.payload->>'alert_id',mark.observed_at DESC,mark.created_at DESC,mark.evidence_id
            ), combined AS (
                SELECT alerts.*,marks.payload AS paper,marks.observed_at AS evaluated_at,
                       COALESCE(marks.payload->>'status','WAITING_FOR_EVALUATION') AS paper_status
                FROM alerts LEFT JOIN marks ON marks.alert_id=alerts.evidence_id::text
            ) SELECT *,COUNT(*) OVER() AS matched_total FROM combined
              WHERE (%s::text IS NULL OR paper_status=%s)
              ORDER BY observed_at DESC,ticker,evidence_id LIMIT %s OFFSET %s
        """, (ALERT_SOURCE, VERSION, now - timedelta(days=days), search, f"%{search}%", direction, direction,
              criterion, criterion, MARK_SOURCE, VERSION, status, status, limit, offset))
        records = [dict(row) for row in cursor.fetchall()]
    rows = []
    for record in records:
        paper = dict(record["paper"] or {})
        paper.pop("source_ids", None)
        paper["status"] = record["paper_status"]
        if paper.get("mark_time"):
            mark_time = datetime.fromisoformat(paper["mark_time"])
            paper["stale"] = paper["status"] == "OPEN_PAPER" and mark_time < expected_materialized_market_time(now, "5m")
        rows.append(dict(record["payload"], alert_id=str(record["evidence_id"]), observed_at=record["observed_at"],
                         market_time=record["market_time"], paper=paper, evaluated_at=record["evaluated_at"]))
    return dict(rows=rows, total=records[0]["matched_total"] if records else 0, offset=offset, limit=limit,
                days=days, notional=NOTIONAL, cost_bps=COST_BPS, accounting="PAPER_NOT_ACTUAL_TRADES",
                latest_snapshot=stamp, status="READY" if stamp else "AWAITING_FIRST_SNAPSHOT"
                )
