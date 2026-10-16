#!/usr/bin/env python3
"""Read-only reproduction of one blocked V2 five-minute finalizer job."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_interval_finalizer import (  # noqa: E402
    EQUITY_INTERVAL_FINALIZE_JOB,
    prepare_five_minute_finalizer_output,
)
from realtime.materialization_jobs import ClaimedMaterializationJob, MaterializationJob  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.database != "stocks_realtime_v2" or args.confirm_database_name != args.database:
        raise ValueError("finalizer diagnosis targets only stocks_realtime_v2")
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    pool = ThreadedConnectionPool(1, 2, dsn=dsn)
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix="stocks_realtime_v2",
                statement_timeout_ms=30_000,
            ),
        )
        provider.verify()
        with provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT job_id,closure_sha256,job_kind,instrument_id,cohort_id,"
                "source_revision_id,bundle_sha256,available_at,interval,primary_window_id,"
                "confirmation_window_ids,policy_sha256,build_id,attempts "
                "FROM rt.materialization_jobs "
                "WHERE job_kind=%s AND status='BLOCKED' "
                "ORDER BY updated_at DESC,job_id LIMIT 1",
                (EQUITY_INTERVAL_FINALIZE_JOB,),
            )
            row = unit.cursor.fetchone()
        if row is None:
            raise RuntimeError("no blocked finalizer job exists")
        job = MaterializationJob(
            job_id=UUID(str(row[0])),
            closure_sha256=row[1],
            job_kind=row[2],
            instrument_id=row[3],
            cohort_id=UUID(str(row[4])),
            source_revision_id=UUID(str(row[5])) if row[5] else None,
            bundle_sha256=row[6],
            available_at=row[7],
            interval=row[8],
            primary_window_id=UUID(str(row[9])) if row[9] else None,
            confirmation_window_ids=tuple(UUID(str(value)) for value in row[10]),
            policy_sha256=row[11],
            build_id=row[12],
        )
        claimed = ClaimedMaterializationJob(
            job,
            uuid4(),
            1,
            datetime.now(timezone.utc) + timedelta(minutes=5),
            max(1, row[13]),
        )
        try:
            prepared = prepare_five_minute_finalizer_output(
                V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
                claimed,
            )
        except Exception as error:
            print(json.dumps({
                "state": "REPRODUCED",
                "job_id": str(job.job_id),
                "instrument_id": job.instrument_id,
                "window_id": str(job.primary_window_id),
                "error_type": type(error).__name__,
                "error": str(error),
                "provider_requests": 0,
                "database_writes": 0,
            }, sort_keys=True))
            return 0
        class RollbackProbe(RuntimeError):
            pass

        try:
            with provider.transaction() as unit:
                prepared.persist_output(unit.cursor)
                raise RollbackProbe()
        except RollbackProbe:
            print(json.dumps({
                "state": "NOT_REPRODUCED",
                "job_id": str(job.job_id),
                "output_count": prepared.output_count,
                "provider_requests": 0,
                "database_writes": 0,
            }, sort_keys=True))
            return 2
        except Exception as error:
            print(json.dumps({
                "state": "REPRODUCED",
                "phase": "PERSIST",
                "job_id": str(job.job_id),
                "instrument_id": job.instrument_id,
                "window_id": str(job.primary_window_id),
                "error_type": type(error).__name__,
                "error": str(error),
                "provider_requests": 0,
                "database_writes": 0,
            }, sort_keys=True))
            return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
