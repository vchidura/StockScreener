#!/usr/bin/env python3
"""Report latest and active immutable equity analysis cohorts by interval."""
from __future__ import annotations

import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor


def main() -> int:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (interval)
                   interval, analysis_run_id, model_bundle_version, status,
                   expected_members, completed_members, no_match_members,
                   insufficient_members, failed_members, created_at,
                   completed_at, published_at
            FROM equity_analysis_runs
            ORDER BY interval, created_at DESC
            """
        )
        latest = [dict(row) for row in cursor.fetchall()]
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
    print(json.dumps(
        {"latest": latest, "active": active, "advisory_locks": advisory_locks},
        default=str,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
