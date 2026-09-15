"""Read-only session coverage and consistency audit over retained equity data."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import exchange_calendars
import pandas as pd

from database import get_db_cursor


QUERIES = {
    "missing_bar_windows": """
        WITH frames(interval, minutes) AS (VALUES ('5m',5),('15m',15)), expected AS (
            SELECT selected.ticker,frames.interval,slot AS bar_start,
                   LEAST(slot+make_interval(mins=>frames.minutes),%(closed)s) AS bar_end
            FROM selected_tickers selected CROSS JOIN frames
            CROSS JOIN LATERAL generate_series(
                %(opened)s,%(closed)s-interval '1 minute',make_interval(mins=>frames.minutes)
            ) slot WHERE selected.is_active
        ), bars AS (
            SELECT ticker,interval,bar_start,bar_end FROM equity_canonical_bars
            WHERE bar_start>=%(day_start)s AND bar_start<%(day_end)s AND interval IN ('5m','15m')
        )
        SELECT expected.ticker,expected.interval,COUNT(*) AS missing,
               array_agg(expected.bar_start AT TIME ZONE 'America/New_York'
                         ORDER BY expected.bar_start) AS missing_starts_eastern
        FROM expected LEFT JOIN bars USING(ticker,interval,bar_start,bar_end)
        WHERE bars.bar_start IS NULL GROUP BY expected.ticker,expected.interval
        ORDER BY expected.ticker,expected.interval
    """,
    "session_coverage": """
        WITH frames(interval, minutes) AS (
            VALUES ('5m',5),('15m',15),('30m',30),('1h',60),('1d',390)
        ), expected AS (
            SELECT selected.ticker, selected.security_id, frames.interval,
                   slot AS bar_start,
                   LEAST(slot + make_interval(mins => frames.minutes), %(closed)s) AS bar_end
            FROM selected_tickers selected CROSS JOIN frames
            CROSS JOIN LATERAL generate_series(
                %(opened)s, %(closed)s - interval '1 minute',
                make_interval(mins => frames.minutes)
            ) slot
            WHERE selected.is_active
        ), bars AS (
            SELECT * FROM equity_canonical_bars
            WHERE bar_start >= %(day_start)s AND bar_start < %(day_end)s
              AND interval IN ('5m','15m','30m','1h','1d')
        ), checked AS (
            SELECT expected.*, bars.bar_revision_id, bars.volume,
                   bars.security_id AS actual_id
            FROM expected LEFT JOIN bars USING(ticker,interval,bar_start,bar_end)
        )
        SELECT interval, COUNT(*) AS expected_bars, COUNT(bar_revision_id) AS present_bars,
               COUNT(*) FILTER(WHERE bar_revision_id IS NULL) AS missing_bars,
               COUNT(DISTINCT ticker) FILTER(WHERE bar_revision_id IS NOT NULL) AS tickers_present,
               COUNT(*) FILTER(WHERE volume=0) AS zero_volume,
               COUNT(*) FILTER(WHERE security_id IS NOT NULL AND security_id<>actual_id) AS identity_mismatches,
               array_agg(DISTINCT ticker) FILTER(WHERE bar_revision_id IS NULL) AS missing_tickers
        FROM checked GROUP BY interval ORDER BY interval
    """,
    "derived_ohlcv": """
        WITH targets AS (
            SELECT * FROM equity_canonical_bars
            WHERE bar_start >= %(day_start)s AND bar_start < %(day_end)s
              AND interval IN ('1h','1d') AND source_kind='DERIVED'
              AND ticker IN (SELECT ticker FROM selected_tickers WHERE is_active)
        ), checked AS (
            SELECT target.interval, target.ticker, target.bar_revision_id,
                   COUNT(source.bar_revision_id)=cardinality(target.source_bar_revision_ids)
                   AND COUNT(DISTINCT source.bar_revision_id)=cardinality(target.source_bar_revision_ids)
                   AND COUNT(source.bar_revision_id)=EXTRACT(EPOCH FROM(target.bar_end-target.bar_start))/1800
                   AND bool_and(source.security_id=target.security_id AND source.ticker=target.ticker
                                AND source.interval='30m')
                   AND MIN(source.bar_start)=target.bar_start AND MAX(source.bar_end)=target.bar_end
                   AND (array_agg(source.open_price ORDER BY source.bar_start))[1]=target.open_price
                   AND MAX(source.high_price)=target.high_price AND MIN(source.low_price)=target.low_price
                   AND (array_agg(source.close_price ORDER BY source.bar_start DESC))[1]=target.close_price
                   AND SUM(source.volume)=target.volume AS matches
            FROM targets target
            CROSS JOIN LATERAL unnest(target.source_bar_revision_ids) source_id
            LEFT JOIN equity_bar_revisions source ON source.bar_revision_id=source_id
            GROUP BY target.interval,target.ticker,target.bar_revision_id,target.security_id,
                     target.bar_start,target.bar_end,target.open_price,target.high_price,
                     target.low_price,target.close_price,target.volume,target.source_bar_revision_ids
        )
        SELECT interval, COUNT(*) AS checked, COUNT(*) FILTER(WHERE matches IS NOT TRUE) AS defects,
               array_agg(ticker) FILTER(WHERE matches IS NOT TRUE) AS defect_tickers
        FROM checked GROUP BY interval ORDER BY interval
    """,
    "analysis_checkpoints": """
        WITH frames(interval,minutes) AS (
            VALUES ('5m',5),('15m',15),('30m',30),('1h',60),('1d',390)
        ), expected AS (
            SELECT interval, LEAST(slot+make_interval(mins=>minutes),%(closed)s) AS market_time
            FROM frames CROSS JOIN LATERAL generate_series(
                %(opened)s,%(closed)s-interval '1 minute',make_interval(mins=>minutes)
            ) slot
        ), published AS (
            SELECT DISTINCT interval,market_time FROM equity_analysis_runs
            WHERE run_purpose='ORIGINAL' AND market_time>=%(opened)s AND market_time<=%(closed)s
              AND status IN ('COMPLETE','DEGRADED') AND published_at IS NOT NULL
        )
        SELECT expected.interval,COUNT(*) AS expected,COUNT(published.market_time) AS published,
               array_agg(expected.market_time ORDER BY expected.market_time)
                   FILTER(WHERE published.market_time IS NULL) AS missing
        FROM expected LEFT JOIN published USING(interval,market_time)
        GROUP BY expected.interval ORDER BY expected.interval
    """,
    "closing_cohorts": """
        SELECT run.interval,run.analysis_run_id,run.status,run.expected_members,
               run.completed_members,run.insufficient_members,run.failed_members,run.published_at,
               (SELECT SUM(evidence_count) FROM equity_analysis_members
                WHERE analysis_run_id=run.analysis_run_id) AS expected_evidence,
               (SELECT COUNT(*) FROM equity_evidence
                WHERE analysis_run_id=run.analysis_run_id) AS actual_evidence
        FROM equity_analysis_runs run
        WHERE run.run_purpose='ORIGINAL' AND run.market_time=%(closed)s
        ORDER BY run.interval,run.created_at
    """,
    "closing_member_exceptions": """
        SELECT run.interval,member.ticker,member.status,member.source_bar_count,member.failure_reason
        FROM equity_analysis_runs run JOIN equity_analysis_members member USING(analysis_run_id)
        WHERE run.run_purpose='ORIGINAL' AND run.market_time=%(closed)s AND member.status<>'COMPLETE'
        ORDER BY run.interval,member.ticker
    """,
    "historical_evidence_mismatches": """
        WITH expected AS (
            SELECT analysis_run_id,SUM(evidence_count) AS count
            FROM equity_analysis_members GROUP BY analysis_run_id
        ), actual AS (
            SELECT analysis_run_id,COUNT(*) AS count FROM equity_evidence
            WHERE analysis_run_id IS NOT NULL GROUP BY analysis_run_id
        ), mismatches AS (
            SELECT run.interval,run.market_time,expected.count AS expected,
                   COALESCE(actual.count,0) AS actual
            FROM equity_analysis_runs run JOIN expected USING(analysis_run_id)
            LEFT JOIN actual USING(analysis_run_id)
            WHERE run.status IN ('COMPLETE','DEGRADED')
              AND expected.count<>COALESCE(actual.count,0)
        )
        SELECT market_time::date AS session,COUNT(*) AS runs,SUM(actual-expected) AS excess_evidence,
               array_agg(DISTINCT interval) AS intervals
        FROM mismatches GROUP BY market_time::date ORDER BY session
    """,
    "failed_members": """
        SELECT run.interval,run.market_time,run.status,member.failure_reason,COUNT(*) AS members
        FROM equity_analysis_runs run JOIN equity_analysis_members member USING(analysis_run_id)
        WHERE run.run_purpose='ORIGINAL' AND run.market_time>=%(opened)s AND run.market_time<=%(closed)s
          AND member.status='FAILED'
        GROUP BY run.interval,run.market_time,run.status,member.failure_reason ORDER BY run.market_time
    """,
    "historical_member_accounting": """
        WITH actual AS (
            SELECT analysis_run_id,security_id,COUNT(*) AS count FROM equity_evidence
            WHERE analysis_run_id IS NOT NULL GROUP BY analysis_run_id,security_id
        )
        SELECT run.market_time::date AS session,member.ticker,member.status,
               member.failure_reason,COUNT(*) AS affected_runs,
               SUM(actual.count-member.evidence_count) AS excess_evidence
        FROM equity_analysis_runs run JOIN equity_analysis_members member USING(analysis_run_id)
        JOIN actual USING(analysis_run_id,security_id)
        WHERE run.status IN ('COMPLETE','DEGRADED') AND actual.count<>member.evidence_count
        GROUP BY run.market_time::date,member.ticker,member.status,member.failure_reason
        ORDER BY session,member.ticker
    """,
    "daily_setup_price_parity": """
        SELECT COUNT(*) AS setup_rows,
               COUNT(*) FILTER(WHERE bar.bar_revision_id IS NULL) AS missing_source_bars,
               COUNT(*) FILTER(WHERE projection.payload->>'last_close' IS NULL OR
                   ABS((projection.payload->>'last_close')::numeric-bar.close_price)>0.005) AS close_mismatches,
               MAX(ABS((projection.payload->>'last_close')::numeric-bar.close_price)) AS max_rounding_difference,
               COUNT(*) FILTER(WHERE projection.market_time IS DISTINCT FROM bar.bar_end) AS time_mismatches
        FROM equity_current_projection projection
        LEFT JOIN equity_evidence evidence ON evidence.evidence_id=projection.evidence_id
        LEFT JOIN equity_bar_revisions bar ON bar.bar_revision_id=evidence.latest_bar_revision_id
        WHERE projection.interval_key='1d' AND projection.projection_type='TRADE_SETUP'
          AND projection.market_time=%(closed)s
    """,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    calendar = exchange_calendars.get_calendar("XNYS")
    session = pd.Timestamp(args.session)
    if not calendar.is_session(session):
        parser.error("The selected date is not an XNYS trading session")
    closed = calendar.session_close(session).to_pydatetime()
    if datetime.now(timezone.utc) < closed:
        parser.error("Select a completed trading session")
    parameters = {
        "opened": calendar.session_open(session).to_pydatetime(),
        "closed": closed,
        "day_start": datetime.combine(args.session, datetime.min.time(), timezone.utc),
        "day_end": datetime.combine(args.session + timedelta(days=1), datetime.min.time(), timezone.utc),
    }
    report = {"session": str(args.session), "universe_basis": "CURRENT_ACTIVE_TRACKED_TICKERS"}
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='90s'")
        cursor.execute("SELECT CURRENT_TIMESTAMP AS checked_at")
        report.update(dict(cursor.fetchone()))
        for name, query in QUERIES.items():
            cursor.execute(query, parameters)
            report[name] = [dict(row) for row in cursor.fetchall()]
            print(json.dumps({name: report[name]}, default=str), flush=True)
    concerns = (
        len(report["session_coverage"]) != 5
        or len({row["interval"] for row in report["closing_cohorts"]}) != 5
        or any(row["missing_bars"] or row["identity_mismatches"] for row in report["session_coverage"])
        or any(row["defects"] for row in report["derived_ohlcv"])
        or any(row["published"] != row["expected"] for row in report["analysis_checkpoints"])
        or any(row["failed_members"] or row["actual_evidence"] != row["expected_evidence"]
               for row in report["closing_cohorts"])
        or bool(report["historical_evidence_mismatches"])
        or bool(report["closing_member_exceptions"])
        or any(row["missing_source_bars"] or row["close_mismatches"] or row["time_mismatches"]
               for row in report["daily_setup_price_parity"])
    )
    print(json.dumps({
        "status": "REVIEW_REQUIRED" if concerns else "PASS",
        "session": report["session"], "checked_at": report["checked_at"],
        "universe_basis": report["universe_basis"],
    }, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())