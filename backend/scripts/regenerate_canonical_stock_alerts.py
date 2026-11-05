#!/usr/bin/env python3
"""Regenerate closed sessions' stock alerts beside their originals. See realtime/canonical_stock_alert_regeneration.py.

--plan computes everything and rolls it back. --execute stores it, one transaction per session.
Makes no provider request. Reads the database address from the mounted secret and never prints it.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timezone
from pathlib import Path

from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.canonical_stock_alert_regeneration import regenerate_session  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    EXPECTED_V2_DATABASE_NAME,
    V2AccessConfig,
    V2ConnectionProvider,
)

DSN_FILE = Path("/run/secrets/realtime_v2_app_dsn")
UTC = timezone.utc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, action="append", required=True)
    parser.add_argument("--report-path", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.confirm_database_name != EXPECTED_V2_DATABASE_NAME:
        parser.error("regeneration targets only stocks_realtime_v2")
    pool = ThreadedConnectionPool(1, 2, dsn=DSN_FILE.read_text(encoding="utf-8").strip(), options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=120_000,
            ),
        )
        provider.verify()
        reports = [
            regenerate_session(provider, session_date=session, execute=args.execute)
            for session in sorted(set(args.session_date))
        ]
    finally:
        pool.closeall()
    text = json.dumps(reports, indent=1, sort_keys=True, default=str)
    print(text)
    if args.report_path is not None:
        args.report_path.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
