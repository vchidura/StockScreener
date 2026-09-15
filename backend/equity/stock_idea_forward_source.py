"""Bounded read-only canonical inputs for the forward stock-idea worker."""
from __future__ import annotations

from datetime import timedelta
import json

from database import get_db_cursor


def enrolled_members(now):
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute("""SELECT publication_id,market_time,published_at,expected_members
            FROM equity_bar_publications WHERE interval='1d' AND session_scope='RTH' AND NOT adjusted
              AND status='COMPLETE' AND selected_members=expected_members AND published_at<=%s AND created_at<=%s
            ORDER BY market_time DESC,published_at DESC,publication_id LIMIT 1""", (now, now))
        publication = cursor.fetchone()
        if not publication:
            raise ValueError("no complete daily cohort is available for forward enrollment")
        cursor.execute("""SELECT security_id::text,ticker FROM equity_bar_publication_members
            WHERE publication_id=%s AND status='SELECTED' ORDER BY security_id""", (publication["publication_id"],))
        members = [dict(row) for row in cursor.fetchall()]
        if len(members) != publication["expected_members"] or not 0 < len(members) <= 1000:
            raise ValueError("forward enrollment membership is incomplete or exceeds 1000 securities")
    return members, json.loads(json.dumps(dict(publication), default=str))


def read_forward_inputs(members, now, *, bootstrap=False, include_daily=False, after=None):
    native_start = now - timedelta(days=100) if bootstrap else (after - timedelta(hours=1) if after else now - timedelta(days=2))
    action_start = now - timedelta(days=450)
    daily_start = action_start if bootstrap else min(now - timedelta(days=7), after - timedelta(days=1) if after else now)
    names = sorted({member["ticker"] for member in members} | {"SPY"})
    bars, actions = [], []
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        intervals = [("30m", native_start, 800)] + ([("1d", daily_start, 320)] if bootstrap or include_daily else [])
        for interval, start, limit in intervals:
            for offset in range(0, len(names), 25):
                cursor.execute("""WITH pinned AS (
                    SELECT DISTINCT ON(ticker,bar_start) ticker,security_id::text,interval,
                        session_date::text AS session,bar_start,bar_end,bar_revision_id::text AS revision_id,
                        open_price::float8 AS open,high_price::float8 AS high,low_price::float8 AS low,
                        close_price::float8 AS close,volume::float8,system_observed_at,created_at
                    FROM equity_bar_revisions WHERE ticker=ANY(%s::text[]) AND interval=%s AND session_scope='RTH'
                      AND NOT adjusted AND is_final AND bar_start>=%s AND bar_end<=%s
                      AND system_observed_at<=%s AND created_at<=%s
                    ORDER BY ticker,bar_start,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                        system_observed_at DESC,created_at DESC,bar_revision_id
                ), numbered AS (SELECT *,row_number() OVER(PARTITION BY ticker ORDER BY bar_start DESC) AS number FROM pinned)
                SELECT * FROM numbered WHERE number<=%s ORDER BY ticker,bar_start""",
                (names[offset:offset + 25], interval, start, now, now, now, limit))
                bars.extend(dict(row) for row in cursor.fetchall())
        cursor.execute("""SELECT security_id::text,ticker,action_type,effective_date::text,
                   split_from::float8,split_to::float8,first_observed_at,created_at
            FROM equity_corporate_actions WHERE ticker=ANY(%s::text[]) AND effective_date>=%s::date
              AND effective_date<=%s::date AND first_observed_at<=%s AND created_at<=%s
            ORDER BY security_id,effective_date,action_type,first_observed_at""", (names, action_start, now, now, now))
        actions = [dict(row) for row in cursor.fetchall()]
    return json.loads(json.dumps(dict(bars=bars, actions=actions), default=str))


def forward_input_readiness(members, boundary, now, *, include_daily=False):
    expected = {member["security_id"] for member in members}
    publications = []
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        for interval in (["30m", "1d"] if include_daily else ["30m"]):
            cursor.execute("""SELECT publication_id::text,published_at,created_at FROM equity_bar_publications
                WHERE interval=%s AND session_scope='RTH' AND NOT adjusted AND market_time=%s
                  AND status='COMPLETE' AND selected_members=expected_members AND published_at<=%s AND created_at<=%s
                ORDER BY published_at DESC,publication_id LIMIT 1""", (interval, boundary, now, now))
            publication = cursor.fetchone()
            if not publication:
                return None
            cursor.execute("""SELECT security_id::text FROM equity_bar_publication_members
                WHERE publication_id=%s AND status='SELECTED'""", (publication["publication_id"],))
            if not expected <= {row["security_id"] for row in cursor.fetchall()}:
                return None
            publications.append(dict(publication, interval=interval))
    return json.loads(json.dumps(dict(publications=publications, checked_at=now), default=str))