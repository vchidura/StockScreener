#!/usr/bin/env python3
"""Read-only explicit 1m/5m gap plan for recent completed V2 sessions."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_recent_repair import recent_completed_sessions  # noqa: E402
from realtime.gap_repair_plan import plan_gap_repair_cursor  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--through-session", type=date.fromisoformat, required=True)
    parser.add_argument("--sessions", type=int, required=True)
    parser.add_argument(
        "--interval",
        action="append",
        choices=("1m", "5m"),
        dest="intervals",
    )
    parser.add_argument("--detail-limit", type=int, default=2_000)
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("gap repair planning targets only stocks_realtime_v2")
    if not 1 <= args.sessions <= 10 or args.detail_limit < 1:
        parser.error("gap repair planning requires 1-10 sessions and detail")
    args.intervals = tuple(dict.fromkeys(args.intervals or ("1m", "5m")))
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def main(argv=None):
    args = parse_args(argv)
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("gap repair planning requires REALTIME_V2_APP_DSN")
    pool = SimpleConnectionPool(1, 1, dsn=dsn, options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode=SECURITY_MODE_HARDENED,
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=30_000,
            ),
        )
        provider.verify()
        now = datetime.now(timezone.utc)
        sessions = recent_completed_sessions(
            args.through_session,
            args.sessions,
            known_by=now,
        )
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            report = plan_gap_repair_cursor(
                unit.cursor,
                cohort_id=args.cohort_id,
                sessions=sessions,
                known_by=now,
                intervals=args.intervals,
                detail_limit=args.detail_limit,
            )
        serializable = json.loads(json.dumps(report, default=str))
        write_report(args.report_path, serializable)
        print(json.dumps(serializable, indent=2, sort_keys=True))
        return 0 if report["status"] == "COMPLETE" else 2
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
