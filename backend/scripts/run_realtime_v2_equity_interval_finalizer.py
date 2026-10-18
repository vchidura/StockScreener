#!/usr/bin/env python3
"""Run the provider-free V2 EQUITY_INTERVAL_FINALIZE leased-job worker.

Exactly one mode is required. --once claims at most --claim-limit jobs and exits.
--continuous repeats the same bounded claim after --poll-seconds until interrupted.
Every mode mutates job, barrier, watermark and possibly bar rows, so --execute and an
exact database confirmation are required. Disposable targets additionally require
--allow-disposable. This process makes no provider request.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from uuid import UUID, uuid4

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_interval_finalizer import (  # noqa: E402
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
    EquityWorkerStatus,
    run_five_minute_finalizer_once,
)
from realtime.migrations import (  # noqa: E402
    SECURITY_MODE_DEFERRED,
    SECURITY_MODE_HARDENED,
    SECURITY_MODES_V2,
    validate_target_database_v2,
)
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402


def _now() -> datetime:
    return datetime.now(timezone.utc)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--allow-disposable", action="store_true")
    parser.add_argument(
        "--security-mode",
        choices=SECURITY_MODES_V2,
        default=SECURITY_MODE_DEFERRED,
    )
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--lease-owner", type=UUID, default=None)
    parser.add_argument("--claim-limit", type=int, default=10)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--maximum-attempts", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=int, default=30)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--statement-timeout-ms", type=int, default=5_000)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        allow_disposable=args.allow_disposable,
        security_mode=args.security_mode,
    )
    if not args.execute:
        parser.error("the finalizer mutates the database and requires --execute")
    if not 1 <= args.claim_limit <= 100:
        parser.error("--claim-limit must be in [1, 100]")
    if args.lease_seconds <= 0 or args.maximum_attempts <= 0:
        parser.error("--lease-seconds and --maximum-attempts must be positive")
    if args.retry_delay_seconds < 0:
        parser.error("--retry-delay-seconds must be nonnegative")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if args.statement_timeout_ms <= 0:
        parser.error("--statement-timeout-ms must be positive")
    args.lease_owner = args.lease_owner or uuid4()
    return args


def _pool(args: argparse.Namespace) -> ThreadedConnectionPool:
    if args.security_mode == SECURITY_MODE_HARDENED:
        dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
        if not dsn:
            raise RuntimeError("hardened finalizer requires REALTIME_V2_APP_DSN")
        return ThreadedConnectionPool(1, 1, dsn=dsn)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("requires: " + ", ".join(missing))
    return ThreadedConnectionPool(
        1,
        1,
        dbname=args.database,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        application_name="run_realtime_v2_equity_interval_finalizer",
        options="-c timezone=UTC",
    )


def _status_json(status: EquityWorkerStatus) -> str:
    return json.dumps(
        {
            "checked_at": status.checked_at.isoformat(),
            "job_kind": EQUITY_INTERVAL_FINALIZE_JOB,
            "claimed": status.claimed,
            "completed": status.completed,
            "retried": status.retried,
            "failed": status.failed,
            "blocked": status.blocked,
            "output_count": status.output_count,
            "reason_codes": status.reason_codes,
        },
        indent=2,
    )


def run_worker(
    database: V2WorkerDatabase,
    config: EquityWorkerConfig,
    *,
    once: bool,
    poll_seconds: float,
    stop: threading.Event,
    now: Callable[[], datetime] | None = None,
    emit: Callable[[str], None] = print,
) -> int:
    exit_code = 0
    clock = now or _now
    while not stop.is_set():
        status = run_five_minute_finalizer_once(
            database, config, checked_at=clock()
        )
        emit(_status_json(status))
        if status.failed or status.blocked:
            exit_code = 1
        if once or stop.wait(poll_seconds):
            return exit_code
    return exit_code


def main(argv=None) -> int:
    args = parse_args(argv)
    pool = _pool(args)
    try:
        provider = V2ConnectionProvider(
            {
                APP_ROLE if args.security_mode == SECURITY_MODE_HARDENED else OWNER_POOL: pool
            },
            V2AccessConfig(
                security_mode=args.security_mode,
                allow_disposable=args.allow_disposable,
                role_prefix=(
                    "stocks_realtime_v2"
                    if args.security_mode == SECURITY_MODE_HARDENED
                    else None
                ),
                statement_timeout_ms=args.statement_timeout_ms,
            ),
        )
        provider.verify()
        database = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
        config = EquityWorkerConfig(
            cohort_id=args.cohort_id,
            job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
            lease_owner=args.lease_owner,
            claim_limit=args.claim_limit,
            lease_seconds=args.lease_seconds,
            maximum_attempts=args.maximum_attempts,
            retry_delay_seconds=args.retry_delay_seconds,
        )
        stop = threading.Event()
        if args.continuous:
            signal.signal(signal.SIGINT, lambda *_: stop.set())
            signal.signal(signal.SIGTERM, lambda *_: stop.set())
        return run_worker(
            database,
            config,
            once=args.once,
            poll_seconds=args.poll_seconds,
            stop=stop,
        )
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
