#!/usr/bin/env python3
"""Report latest and active immutable equity analysis cohorts by interval."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor


def latency_report():
    from scripts.equity_worker_stages import publication_watermarks
    from scripts.run_equity_worker import INTERVALS, PROVIDER_DELAY_MINUTES, latest_due_slot
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    publications = publication_watermarks(INTERVALS)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        cursor.execute("""
            SELECT DISTINCT ON (interval) interval, status, market_time, created_at, published_at,
                completed_members, expected_members
            FROM equity_analysis_runs WHERE interval = ANY(%s) AND run_purpose = 'ORIGINAL'
            ORDER BY interval, market_time DESC, created_at DESC
            """, (list(INTERVALS),))
        analysis = {row["interval"]: dict(row) for row in cursor.fetchall()}
        cursor.execute("""
            SELECT DISTINCT ON (interval) interval, status, market_watermark, created_at, completed_at,
                gap_details->'timings_seconds' AS timings_seconds
            FROM equity_ingestion_segments WHERE interval = ANY(%s)
                AND dataset IN ('EQUITY_BARS', 'EQUITY_DERIVED_BARS')
            ORDER BY interval, created_at DESC
            """, (list(INTERVALS),))
        ingestion = {row["interval"]: dict(row) for row in cursor.fetchall()}
    rows = []
    for interval in INTERVALS:
        due = latest_due_slot(now, interval, provider_delay=timedelta(minutes=PROVIDER_DELAY_MINUTES))
        publication = publications.get(interval)
        rows.append(dict(interval=interval, expected_market_time=due, publication=publication,
            canonical_backlog_seconds=max(0, (due - publication["market_time"]).total_seconds())
                if due and publication else None,
            canonical_completion_extra_delay_seconds=max(0, (publication["published_at"] - publication["market_time"]
                - timedelta(minutes=PROVIDER_DELAY_MINUTES)).total_seconds()) if publication and publication["published_at"] else None,
            analysis_completion_extra_delay_seconds=max(0, (analysis[interval]["published_at"] - analysis[interval]["market_time"]
                - timedelta(minutes=PROVIDER_DELAY_MINUTES)).total_seconds()) if interval in analysis and analysis[interval]["published_at"] else None,
            analysis=analysis.get(interval), ingestion=ingestion.get(interval)))
    return dict(checked_at=now, provider_delay_seconds=PROVIDER_DELAY_MINUTES * 60, intervals=rows,
        clock_basis="FINALIZED_BAR_BOUNDARIES_NOT_TICK_LATENCY")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval", choices=("5m", "15m", "30m", "1h", "1d", "1wk", "1mo"),
        help="Limit run, ingestion and publication detail to one interval.",
    )
    parser.add_argument(
        "--active-only", action="store_true",
        help="Emit only active run/member and advisory-lock state.",
    )
    parser.add_argument("--latency", action="store_true", help="Bounded read-only provider-delay, canonical and analysis lag summary.")
    args = parser.parse_args()
    if args.latency:
        print(json.dumps(latency_report(), default=str, indent=2))
        return 0
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (interval)
                   interval, analysis_run_id, model_bundle_version, status,
                     market_time, observed_at, expected_members,
                     completed_members, no_match_members,
                   insufficient_members, failed_members, created_at,
                   completed_at, published_at
            FROM equity_analysis_runs
            ORDER BY interval, created_at DESC
            """
        )
        latest = [dict(row) for row in cursor.fetchall()]
        if args.interval:
            latest = [row for row in latest if row["interval"] == args.interval]
        cursor.execute(
            """
            SELECT DISTINCT ON (interval)
                   interval, ingestion_segment_id, dataset, provider_mode,
                   status, market_watermark, record_count, gap_details,
                   created_at, completed_at
            FROM equity_ingestion_segments
            WHERE interval IN ('5m', '15m', '30m', '1h', '1d', '1wk', '1mo')
            ORDER BY interval, created_at DESC
            """
        )
        latest_ingestion = [dict(row) for row in cursor.fetchall()]
        if args.interval:
            latest_ingestion = [
                row for row in latest_ingestion if row["interval"] == args.interval
            ]
        cursor.execute(
            """
            SELECT DISTINCT ON (interval)
                   interval, publication_id, market_time, status,
                   expected_members, selected_members, missing_members,
                   failed_members, created_at, published_at
            FROM equity_bar_publications
            ORDER BY interval, created_at DESC
            """
        )
        latest_publications = [dict(row) for row in cursor.fetchall()]
        if args.interval:
            latest_publications = [
                row for row in latest_publications if row["interval"] == args.interval
            ]
        for run in latest:
            if not run["failed_members"] and not run["insufficient_members"]:
                continue
            cursor.execute(
                """
                SELECT member.status, reference.ticker, member.failure_reason
                FROM equity_analysis_members AS member
                LEFT JOIN equity_security_reference_revisions AS reference
                  ON reference.security_id = member.security_id
                 AND reference.security_revision_id = (
                     SELECT revision.security_revision_id
                     FROM equity_security_reference_revisions AS revision
                     WHERE revision.security_id = member.security_id
                     ORDER BY revision.effective_from DESC, revision.observed_at DESC
                     LIMIT 1
                 )
                WHERE member.analysis_run_id = %s
                  AND member.status IN ('FAILED', 'INSUFFICIENT_DATA')
                ORDER BY member.status, reference.ticker
                LIMIT 20
                """,
                (run["analysis_run_id"],),
            )
            run["failure_examples"] = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            SELECT interval, analysis_run_id, model_bundle_version, status,
                   expected_members, created_at
            FROM equity_analysis_runs
            WHERE status IN ('PENDING', 'RUNNING')
            ORDER BY created_at
            """
        )
        active = [dict(row) for row in cursor.fetchall()]
        if args.interval:
            active = [row for row in active if row["interval"] == args.interval]
        for run in active:
            cursor.execute(
                """
                SELECT status, COUNT(*) AS members
                FROM equity_analysis_members
                WHERE analysis_run_id = %s
                GROUP BY status ORDER BY status
                """,
                (run["analysis_run_id"],),
            )
            run["member_status"] = {
                row["status"]: int(row["members"]) for row in cursor.fetchall()
            }
        cursor.execute(
            """
            SELECT activity.pid, activity.application_name, activity.state,
                   activity.backend_start, activity.query_start,
                   LEFT(activity.query, 240) AS query
            FROM pg_locks lock
            JOIN pg_stat_activity activity ON activity.pid = lock.pid
            WHERE lock.locktype = 'advisory' AND lock.granted = TRUE
            ORDER BY activity.backend_start
            """
        )
        advisory_locks = [dict(row) for row in cursor.fetchall()]
    payload = {
        "checked_at": datetime.now(timezone.utc),
        "latest": latest,
        "latest_ingestion": latest_ingestion,
        "latest_publications": latest_publications,
        "active": active,
        "advisory_locks": advisory_locks,
    }
    if args.active_only:
        payload = {
            "checked_at": payload["checked_at"],
            "active": payload["active"],
            "advisory_locks": payload["advisory_locks"],
        }
    print(json.dumps(
        payload,
        default=str,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
