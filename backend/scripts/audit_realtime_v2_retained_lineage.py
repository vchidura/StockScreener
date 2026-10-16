#!/usr/bin/env python3
"""Read-only audit of noncurrent V1 source revisions needed by exact V2-10 bars."""
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

from realtime.equity_v2_backfill_source import load_retained_v1_facts  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    if args.database != "stocks_realtime_v1" or args.confirm_database_name != args.database:
        parser.error("retained lineage audit targets only stocks_realtime_v1")
    return args


def main(argv=None):
    args = parse_args(argv)
    settings = admin_settings(BACKEND_DIR / ".env")
    with closing(connect(args.database, settings, application_name="audit_v2_retained_lineage")) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                facts = load_retained_v1_facts(
                    cursor,
                    cohort_id=args.cohort_id,
                    session_date=args.session_date,
                )
                selected = {fact.revision.bar_revision_id for fact in facts}
                referenced = {
                    source
                    for fact in facts
                    for source in fact.revision.source_bar_revision_ids
                }
                missing = tuple(sorted(referenced - selected, key=str))
                cursor.execute(
                    "SELECT interval,adjustment_basis,source_kind,COUNT(*) AS count,"
                    "COUNT(*) FILTER (WHERE is_final) AS final_count "
                    "FROM rt.bar_revisions WHERE bar_revision_id=ANY(%s::uuid[]) "
                    "GROUP BY interval,adjustment_basis,source_kind "
                    "ORDER BY interval,adjustment_basis,source_kind",
                    ([str(value) for value in missing],),
                )
                groups = tuple(dict(row) for row in cursor.fetchall())
                cursor.execute(
                    "SELECT COUNT(*) FROM rt.bar_revisions "
                    "WHERE bar_revision_id=ANY(%s::uuid[])",
                    ([str(value) for value in missing],),
                )
                resolved = int(cursor.fetchone()["count"])
        finally:
            connection.rollback()
    print(json.dumps({
        "state": "COMPLETE",
        "selected_exact_facts": len(facts),
        "referenced_source_revisions": len(referenced),
        "noncurrent_source_revisions": len(missing),
        "resolved_noncurrent_source_revisions": resolved,
        "groups": groups,
        "provider_requests": 0,
        "database_writes": 0,
    }, sort_keys=True))
    return 0 if resolved == len(missing) else 2


if __name__ == "__main__":
    raise SystemExit(main())
