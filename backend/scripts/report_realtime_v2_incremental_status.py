#!/usr/bin/env python3
"""Read-only V2 incremental acquisition and publication status report."""
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
from realtime.incremental_health import collect_incremental_health_cursor  # noqa: E402
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
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--account-alias", default="MASSIVE_DEVELOPER")
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("incremental status targets only stocks_realtime_v2")
    args.account_alias = args.account_alias.strip()
    if not args.account_alias:
        parser.error("--account-alias cannot be blank")
    if args.report_path is not None:
        args.report_path = args.report_path.resolve()
        if args.report_path.exists():
            parser.error("--report-path must be new")
    return args


def main(argv=None):
    args = parse_args(argv)
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("incremental status requires REALTIME_V2_APP_DSN")
    pool = SimpleConnectionPool(1, 1, dsn=dsn, options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode=SECURITY_MODE_HARDENED,
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=5_000,
            ),
        )
        provider.verify()
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            report = collect_incremental_health_cursor(
                unit.cursor,
                cohort_id=args.cohort_id,
                account_alias=args.account_alias,
                session_date=args.session_date,
                checked_at=datetime.now(timezone.utc),
            )
        serializable = json.loads(json.dumps(report, default=str))
        if args.report_path is not None:
            write_report(args.report_path, serializable)
        print(json.dumps(serializable, indent=2, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
