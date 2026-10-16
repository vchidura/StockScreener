#!/usr/bin/env python3
"""Read-only exact nine-ticker V2-10 bar and lineage audit."""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from uuid import UUID

from psycopg2.extras import RealDictCursor

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan  # noqa: E402
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from scripts.inventory_realtime_v2_backfill_source import (  # noqa: E402
    _coverage_rows,
    coverage_report,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("exact V2 backfill audit targets only stocks_realtime_v2")
    if args.report_path.exists():
        parser.error("exact V2 backfill audit requires a new report path")
    return args


def main(argv=None):
    args = parse_args(argv)
    settings = admin_settings(BACKEND_DIR / ".env")
    with closing(connect(args.database, settings, application_name="audit_v2_exact_backfill")) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT set_config('statement_timeout', '120s', true)")
                cursor.execute(
                    "SELECT MIN(session_date) AS first_session,MAX(session_date) AS last_session "
                    "FROM rt.bar_catalog WHERE cohort_id=%s",
                    (str(args.cohort_id),),
                )
                envelope = dict(cursor.fetchone())
                rows = _coverage_rows(
                    cursor,
                    cohort_id=str(args.cohort_id),
                    start_date=envelope["first_session"],
                    end_date=envelope["last_session"],
                )
                coverage = coverage_report(rows, session_date=args.session_date)
                adjusted_start = build_realtime_equity_portal_history_plan(
                    args.session_date
                ).adjusted_sessions[0]
                cursor.execute(
                    "SELECT COUNT(DISTINCT source_id) AS support_count "
                    "FROM rt.bar_revisions AS derived "
                    "CROSS JOIN LATERAL unnest(derived.source_revision_ids) AS source_id "
                    "JOIN rt.bar_revisions AS source ON source.bar_revision_id=source_id "
                    "JOIN rt.bar_current AS current ON current.cohort_id=source.cohort_id "
                    "AND current.bar_revision_id=source.bar_revision_id "
                    "WHERE derived.cohort_id=%s AND derived.source_kind='DERIVED' "
                    "AND source.interval='1d' AND source.adjustment_basis='ADJUSTED' "
                    "AND source.session_date<%s",
                    (str(args.cohort_id), adjusted_start),
                )
                support_count = int(cursor.fetchone()["support_count"])
                cursor.execute(
                    "SELECT status,COUNT(*) AS count,COUNT(*) FILTER "
                    "(WHERE failure_code IS NOT NULL) AS failures "
                    "FROM rt.materialization_jobs WHERE cohort_id=%s GROUP BY status ORDER BY status",
                    (str(args.cohort_id),),
                )
                jobs = tuple(dict(row) for row in cursor.fetchall())
                cursor.execute(
                    "SELECT endpoint_class,COUNT(*) AS count FROM rt.provider_request_ledger "
                    "WHERE cohort_id=%s GROUP BY endpoint_class ORDER BY endpoint_class",
                    (str(args.cohort_id),),
                )
                requests = tuple(dict(row) for row in cursor.fetchall())
        finally:
            connection.rollback()
    status = "PASS" if (
        coverage["complete"]
        and coverage["expected_rows"] == 124_236
        and coverage["actual_rows"] == 124_236
        and jobs == ({"status": "COMPLETE", "count": 2898, "failures": 0},)
    ) else "FAIL"
    report = {
        "status": status,
        "database": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "universe_sha256": EQUITY_VALIDATION_UNIVERSE.sha256,
        "coverage": coverage,
        "lineage_support_count": support_count,
        "materialization_jobs": jobs,
        "provider_request_groups": requests,
        "provider_requests_performed_by_audit": 0,
        "database_writes": 0,
    }
    write_report(args.report_path.resolve(), json.loads(json.dumps(report, default=str)))
    print(json.dumps({
        "state": "COMPLETE",
        "status": status,
        "exact_rows": coverage["actual_rows"],
        "expected_rows": coverage["expected_rows"],
        "lineage_support_count": support_count,
        "report_path": str(args.report_path.resolve()),
        "provider_requests": 0,
        "database_writes": 0,
    }, sort_keys=True))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
