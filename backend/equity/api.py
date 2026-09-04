"""Read-only APIs over published equity materialization facts."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from time import perf_counter
from typing import Any

from fastapi import APIRouter, Query

from .calendar import latest_expected_market_time


router = APIRouter(prefix="/api/equity", tags=["equity-materialization"])


def expected_materialized_market_time(
    now: datetime,
    interval: str,
    *,
    provider_delay_minutes: int | None = None,
) -> datetime:
    delay_minutes = (
        int(os.getenv("EQUITY_PROVIDER_DELAY_MINUTES", "15"))
        if provider_delay_minutes is None
        else provider_delay_minutes
    )
    if delay_minutes < 0:
        raise ValueError("provider delay must not be negative")
    return latest_expected_market_time(
        now - timedelta(minutes=delay_minutes), interval
    )


def minimum_fresh_materialized_market_time(
    now: datetime,
    interval: str,
    *,
    provider_delay_minutes: int | None = None,
    publication_grace_seconds: int | None = None,
) -> datetime:
    grace_seconds = (
        int(os.getenv("EQUITY_PUBLICATION_GRACE_SECONDS", "600"))
        if publication_grace_seconds is None
        else publication_grace_seconds
    )
    if grace_seconds < 0:
        raise ValueError("publication grace must not be negative")
    return expected_materialized_market_time(
        now - timedelta(seconds=grace_seconds),
        interval,
        provider_delay_minutes=provider_delay_minutes,
    )


def current_trade_setup_projection(
    ticker: str,
    interval: str = "30m",
    *,
    now: datetime | None = None,
    provider_delay_minutes: int | None = None,
    publication_grace_seconds: int | None = None,
) -> dict[str, Any] | None:
    from database import get_db_cursor

    started = perf_counter()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT payload, evidence_id, analysis_run_id,
                   market_time, observed_at, published_at
            FROM equity_current_projection
            WHERE ticker = %s
              AND interval_key = %s
              AND projection_type = 'TRADE_SETUP'
              AND source_name = 'EQUITY_SETUP'
            """,
            (ticker.upper(), interval),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    result = dict(row)
    current_time = now or datetime.now(timezone.utc)
    expected_market_time = expected_materialized_market_time(
        current_time, interval,
        provider_delay_minutes=provider_delay_minutes,
    )
    minimum_fresh_market_time = minimum_fresh_materialized_market_time(
        current_time, interval,
        provider_delay_minutes=provider_delay_minutes,
        publication_grace_seconds=publication_grace_seconds,
    )
    staleness_seconds = max(
        0, int((expected_market_time - result["market_time"]).total_seconds())
    )
    result.update({
        "expected_market_time": expected_market_time,
        "minimum_fresh_market_time": minimum_fresh_market_time,
        "is_fresh": minimum_fresh_market_time <= result["market_time"] <= expected_market_time,
        "read_latency_ms": round((perf_counter() - started) * 1000, 3),
        "staleness_seconds": staleness_seconds,
    })
    return result


def current_pattern_watch_projection(
    interval: str,
    *,
    ticker: str | None = None,
    now: datetime | None = None,
    provider_delay_minutes: int | None = None,
    publication_grace_seconds: int | None = None,
) -> dict[str, Any]:
    from database import get_db_cursor

    started = perf_counter()
    clauses = [
        "projection.interval_key = %s",
        "projection.projection_type = ANY(%s)",
    ]
    parameters: list[Any] = [
        interval,
        ["FEATURE_SNAPSHOT", "PATTERN_OBSERVATION", "PRICE_CHANNEL"],
    ]
    if ticker:
        clauses.append("projection.ticker = %s")
        parameters.append(ticker.upper())
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT projection.ticker, projection.projection_type,
                   projection.source_name, projection.payload,
                   projection.analysis_run_id, projection.market_time,
                   projection.observed_at, projection.published_at,
                   selected.sector
            FROM equity_current_projection AS projection
            LEFT JOIN selected_tickers AS selected
              ON selected.ticker = projection.ticker
            WHERE {' AND '.join(clauses)}
            ORDER BY projection.ticker, projection.projection_type,
                     projection.source_name
            """,
            parameters,
        )
        rows = [dict(row) for row in cursor.fetchall()]
    current_time = now or datetime.now(timezone.utc)
    expected_market_time = expected_materialized_market_time(
        current_time, interval,
        provider_delay_minutes=provider_delay_minutes,
    )
    minimum_fresh_market_time = minimum_fresh_materialized_market_time(
        current_time, interval,
        provider_delay_minutes=provider_delay_minutes,
        publication_grace_seconds=publication_grace_seconds,
    )
    feature_rows = [
        row for row in rows if row["projection_type"] == "FEATURE_SNAPSHOT"
    ]
    market_times = {row["market_time"] for row in rows}
    analysis_run_ids = {row["analysis_run_id"] for row in rows}
    is_fresh = bool(
        feature_rows
        and len(market_times) == 1
        and minimum_fresh_market_time <= next(iter(market_times)) <= expected_market_time
        and len(analysis_run_ids) == 1
    )
    features = {row["ticker"]: row for row in feature_rows}
    channels = {
        row["ticker"]: row["payload"]
        for row in rows if row["projection_type"] == "PRICE_CHANNEL"
    }
    results = [
        {
            "ticker": row["ticker"],
            "sector": row["sector"],
            "interval": interval,
            "last_close": (features.get(row["ticker"], {}).get("payload") or {}).get("close"),
            "pattern": row["payload"],
            "channel": channels.get(row["ticker"]),
        }
        for row in rows if row["projection_type"] == "PATTERN_OBSERVATION"
    ]
    return {
        "analysis_run_id": (
            str(next(iter(analysis_run_ids))) if len(analysis_run_ids) == 1 else None
        ),
        "computed_at": (
            max(row["published_at"] for row in rows).isoformat() if rows else None
        ),
        "expected_market_time": expected_market_time,
        "minimum_fresh_market_time": minimum_fresh_market_time,
        "is_fresh": is_fresh,
        "market_times": tuple(sorted(market_times)),
        "read_latency_ms": round((perf_counter() - started) * 1000, 3),
        "results": results,
        "scanned": len(feature_rows),
        "channels": channels,
        "last_closes": {
            ticker: row["payload"].get("close") for ticker, row in features.items()
        },
    }


def current_chart_bar_projection(
    ticker: str,
    interval: str,
    *,
    limit: int,
    now: datetime | None = None,
    provider_delay_minutes: int | None = None,
    publication_grace_seconds: int | None = None,
) -> dict[str, Any] | None:
    if interval not in ("5m", "15m", "30m", "1h", "1d", "1wk"):
        raise ValueError(f"unsupported materialized chart interval: {interval}")
    if limit <= 0:
        raise ValueError("limit must be positive")

    from database import get_db_cursor

    started = perf_counter()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH feature AS (
                SELECT projection.evidence_id, projection.analysis_run_id,
                       projection.market_time, projection.observed_at,
                       projection.published_at, evidence.source_revision_ids
                FROM equity_current_projection projection
                JOIN equity_evidence evidence USING (evidence_id)
                WHERE projection.ticker = %s
                  AND projection.interval_key = %s
                  AND projection.projection_type = 'FEATURE_SNAPSHOT'
                  AND projection.source_name = 'EQUITY_FEATURES'
            )
            SELECT feature.evidence_id, feature.analysis_run_id,
                   feature.market_time, feature.observed_at,
                   feature.published_at, source.ordinal,
                   bar.bar_revision_id, bar.bar_start, bar.bar_end,
                   bar.open_price, bar.high_price, bar.low_price,
                   bar.close_price, bar.volume
            FROM feature
            CROSS JOIN LATERAL unnest(feature.source_revision_ids)
                WITH ORDINALITY AS source(bar_revision_id, ordinal)
            JOIN equity_bar_revisions bar
              ON bar.bar_revision_id = source.bar_revision_id
            WHERE source.ordinal > GREATEST(
                cardinality(feature.source_revision_ids) - %s, 0
            )
            ORDER BY source.ordinal
            """,
            (ticker.upper(), interval, limit),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    if not rows:
        return None

    current_time = now or datetime.now(timezone.utc)
    expected_market_time = expected_materialized_market_time(
        current_time, interval,
        provider_delay_minutes=provider_delay_minutes,
    )
    minimum_fresh_market_time = minimum_fresh_materialized_market_time(
        current_time, interval,
        provider_delay_minutes=provider_delay_minutes,
        publication_grace_seconds=publication_grace_seconds,
    )
    metadata = rows[0]
    return {
        "analysis_run_id": metadata["analysis_run_id"],
        "evidence_id": metadata["evidence_id"],
        "market_time": metadata["market_time"],
        "observed_at": metadata["observed_at"],
        "published_at": metadata["published_at"],
        "expected_market_time": expected_market_time,
        "minimum_fresh_market_time": minimum_fresh_market_time,
        "is_fresh": (
            minimum_fresh_market_time <= metadata["market_time"] <= expected_market_time
            and rows[-1]["bar_end"] == metadata["market_time"]
        ),
        "read_latency_ms": round((perf_counter() - started) * 1000, 3),
        "bars": tuple({
            key: row[key]
            for key in (
                "bar_revision_id", "bar_start", "bar_end", "open_price",
                "high_price", "low_price", "close_price", "volume",
            )
        } for row in rows),
    }


@router.get("/health")
def equity_materialization_health() -> dict[str, Any]:
    from database import get_db_cursor

    tables = (
        "equity_bar_revisions",
        "equity_analysis_runs",
        "equity_evidence",
        "equity_context_snapshots",
        "equity_current_projection",
        "equity_research_outcomes",
    )
    result: dict[str, Any] = {"schema_ready": True, "tables": {}}
    with get_db_cursor() as cursor:
        for table in tables:
            cursor.execute("SELECT to_regclass(%s) AS name", (f"public.{table}",))
            exists = cursor.fetchone()["name"] is not None
            result["tables"][table] = {"exists": exists}
            result["schema_ready"] = result["schema_ready"] and exists
        if result["schema_ready"]:
            cursor.execute(
                """
                SELECT interval, status, market_time, observed_at, published_at,
                       completed_members, no_match_members,
                       insufficient_members, failed_members
                FROM equity_analysis_runs
                ORDER BY market_time DESC, observed_at DESC
                LIMIT 10
                """
            )
            result["recent_runs"] = [dict(row) for row in cursor.fetchall()]
    result["checked_at"] = datetime.now(timezone.utc)
    return result


@router.get("/current")
def current_equity_materialization(
    ticker: str | None = None,
    interval: str | None = None,
    projection_type: str | None = None,
    source_name: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    from database import get_db_cursor

    clauses = []
    parameters: list[Any] = []
    for column, value in (
        ("ticker", ticker.upper() if ticker else None),
        ("interval_key", interval),
        ("projection_type", projection_type),
        ("source_name", source_name),
    ):
        if value is not None:
            clauses.append(f"{column} = %s")
            parameters.append(value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_db_cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) AS count FROM equity_current_projection {where}",
            parameters,
        )
        total = int(cursor.fetchone()["count"])
        cursor.execute(
            f"""
            SELECT ticker, interval_key, projection_type, source_name,
                   evidence_id, equity_context_snapshot_id, analysis_run_id,
                   market_time, observed_at, published_at, payload
            FROM equity_current_projection
            {where}
            ORDER BY published_at DESC, ticker, interval_key, projection_type,
                     source_name
            LIMIT %s OFFSET %s
            """,
            [*parameters, limit, offset],
        )
        rows = [dict(row) for row in cursor.fetchall()]
    return {
        "count": len(rows),
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < total,
        "results": rows,
    }


@router.get("/security/{ticker}")
def equity_security_profile(
    ticker: str,
    report_limit: int = Query(default=12, ge=0, le=100),
) -> dict[str, Any]:
    from database import get_db_cursor

    symbol = ticker.upper()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT * FROM equity_security_reference_revisions
            WHERE ticker = %s
            ORDER BY effective_from DESC, observed_at DESC
            LIMIT 1
            """,
            (symbol,),
        )
        security = cursor.fetchone()
        reports = []
        if security and report_limit:
            cursor.execute(
                """
                SELECT fundamental_report_id, timeframe, fiscal_year,
                       fiscal_quarter, period_end, filing_date, availability_time,
                       revenue, gross_profit, operating_income, ebitda, net_income,
                       basic_eps, diluted_eps, basic_weighted_shares,
                       diluted_weighted_shares, cash_and_equivalents, total_assets,
                       current_debt, long_term_debt, total_liabilities, total_equity,
                       operating_cash_flow, capital_expenditures, free_cash_flow,
                       dividends, source, quality_codes
                FROM equity_fundamental_reports
                WHERE security_id = %s
                ORDER BY availability_time DESC, period_end DESC
                LIMIT %s
                """,
                (security["security_id"], report_limit),
            )
            reports = [dict(row) for row in cursor.fetchall()]
    return {
        "ticker": symbol,
        "security": dict(security) if security else None,
        "fundamental_reports": reports,
    }


@router.get("/context/{ticker}")
def equity_context_history(
    ticker: str,
    strategy_horizon: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    from database import get_db_cursor

    clauses = ["ticker = %s"]
    parameters: list[Any] = [ticker.upper()]
    if strategy_horizon:
        clauses.append("strategy_horizon = %s")
        parameters.append(strategy_horizon)
    parameters.append(limit)
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT * FROM equity_context_snapshots
            WHERE {' AND '.join(clauses)}
            ORDER BY market_time DESC, observed_at DESC
            LIMIT %s
            """,
            parameters,
        )
        rows = [dict(row) for row in cursor.fetchall()]
    return {"ticker": ticker.upper(), "count": len(rows), "results": rows}


@router.get("/outcomes")
def equity_outcomes(
    ticker: str | None = None,
    source_name: str | None = None,
    interval: str | None = None,
    horizon: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    from database import get_db_cursor

    clauses = []
    parameters: list[Any] = []
    for expression, value in (
        ("evidence.ticker = %s", ticker.upper() if ticker else None),
        ("evidence.source_name = %s", source_name),
        ("evidence.interval = %s", interval),
        ("outcome.horizon_key = %s", horizon),
    ):
        if value is not None:
            clauses.append(expression)
            parameters.append(value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    parameters.append(limit)
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT evidence.ticker, evidence.interval, evidence.source_name,
                   evidence.source_version, evidence.direction,
                   outcome.*
            FROM equity_research_outcomes AS outcome
            JOIN equity_evidence AS evidence
              ON evidence.evidence_id = outcome.subject_evidence_id
            {where}
            ORDER BY outcome.outcome_available_at DESC
            LIMIT %s
            """,
            parameters,
        )
        rows = [dict(row) for row in cursor.fetchall()]
    return {"count": len(rows), "results": rows}


@router.get("/qualifications")
def equity_qualifications(
    source_name: str | None = None,
    qualification_state: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    from database import get_db_cursor

    clauses = []
    parameters: list[Any] = []
    if source_name:
        clauses.append("source_name = %s")
        parameters.append(source_name)
    if qualification_state:
        clauses.append("qualification_state = %s")
        parameters.append(qualification_state)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    parameters.append(limit)
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT * FROM equity_qualification_revisions
            {where}
            ORDER BY effective_from DESC, source_name, interval, direction
            LIMIT %s
            """,
            parameters,
        )
        rows = [dict(row) for row in cursor.fetchall()]
    return {"count": len(rows), "results": rows}