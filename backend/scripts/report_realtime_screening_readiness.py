from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import parse_dsn


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import validate_target_database
from realtime.portal_compatibility_repository import RealtimeScreeningRepository
from research.screening import Query


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read-only exact screening repository diagnostic.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    args = parser.parse_args(argv)
    validate_target_database(args.database, args.confirm_database_name)
    load_dotenv(args.env_file, override=True)
    dsn = os.getenv("REALTIME_API_DSN", "").strip()
    if not dsn or parse_dsn(dsn).get("dbname") != args.database:
        raise ValueError("screening diagnostic requires the exact realtime API target")
    active_pool = pool.SimpleConnectionPool(1, 1, dsn=dsn)
    @contextmanager
    def cursor_factory():
        connection = active_pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
            connection.rollback()
        finally:
            active_pool.putconn(connection, close=bool(connection.closed))
    try:
        repository = RealtimeScreeningRepository(cursor_factory)
        try:
            result = repository.query(Query(), available_by=datetime.now(timezone.utc), cohort_id=args.cohort_id)
        except Exception as error:
            report = {"status": "BLOCKED", "error_type": type(error).__name__,
                "error": str(error), "database_writes": 0, "provider_requests": 0,
                "values_exposed": False}
        else:
            report = {"status": "PASS", "generation": result["generation"],
                "row_count": len(result["rows"]), "database_writes": 0,
                "provider_requests": 0, "values_exposed": False}
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        active_pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())