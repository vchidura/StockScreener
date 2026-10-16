#!/usr/bin/env python3
"""Bridge committed Advanced equity AM journal records into V2 PostgreSQL.

Exactly one mode is required. --once scans at most --batch-limit normalized
records and exits. --continuous repeats bounded scans until interrupted. Every
successful nonempty batch atomically persists canonical final 1m bars and all
configured shard checkpoints. The process makes no provider request.

All modes mutate PostgreSQL and therefore require --execute, an exact database
confirmation, and --allow-disposable for test_realtime_v2_* targets.
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
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    MAX_ADVANCED_EQUITY_BATCH,
    AdvancedEquityJournalBridgeConfig,
    AdvancedEquityJournalBridgeStatus,
    process_advanced_equity_journal_once,
)
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.journal import RawJournalReader  # noqa: E402
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
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--journal-generation", type=UUID, required=True)
    parser.add_argument("--stream-session-id", type=UUID, required=True)
    parser.add_argument("--owner-epoch", type=int, required=True)
    parser.add_argument("--normalizer-version", required=True)
    parser.add_argument("--shard-id", type=int, action="append", required=True)
    parser.add_argument("--consumer-name", default="advanced-equity-am-v1")
    parser.add_argument("--batch-limit", type=int, default=500)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--journal-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--statement-timeout-ms", type=int, default=10_000)
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
    if args.security_mode == SECURITY_MODE_HARDENED:
        parser.error(
            "hardened runtime credentials are provisioned in the final hardening phase"
        )
    if not args.execute:
        parser.error("the Advanced equity bridge mutates PostgreSQL and requires --execute")
    if not 1 <= args.batch_limit <= MAX_ADVANCED_EQUITY_BATCH:
        parser.error("--batch-limit must be in [1, 1000]")
    if args.owner_epoch <= 0:
        parser.error("--owner-epoch must be positive")
    if args.poll_seconds <= 0 or args.journal_timeout_seconds <= 0:
        parser.error("poll and journal timeout seconds must be positive")
    if args.statement_timeout_ms <= 0:
        parser.error("--statement-timeout-ms must be positive")
    return args


def _pool(args: argparse.Namespace) -> ThreadedConnectionPool:
    load_dotenv(BACKEND_DIR / ".env", override=True)
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
        application_name="run_realtime_v2_advanced_equity_bridge",
        options="-c timezone=UTC",
    )


def _status_json(status: AdvancedEquityJournalBridgeStatus) -> str:
    return json.dumps(
        {
            "checked_at": status.checked_at.isoformat(),
            "start_offset": status.start_offset,
            "committed_offset": status.committed_offset,
            "scanned_records": status.scanned_records,
            "persisted_bars": status.persisted_bars,
            "shards_advanced": status.shards_advanced,
        },
        indent=2,
    )


def run_bridge(
    database: V2WorkerDatabase,
    reader: RawJournalReader,
    config: AdvancedEquityJournalBridgeConfig,
    *,
    once: bool,
    poll_seconds: float,
    stop: threading.Event,
    now: Callable[[], datetime] | None = None,
    emit: Callable[[str], None] = print,
) -> int:
    clock = now or _now
    while not stop.is_set():
        status = process_advanced_equity_journal_once(
            database, reader, config, checked_at=clock()
        )
        emit(_status_json(status))
        if once or stop.wait(poll_seconds):
            return 0
    return 0


def main(argv=None) -> int:
    args = parse_args(argv)
    pool = _pool(args)
    try:
        provider = V2ConnectionProvider(
            {OWNER_POOL: pool},
            V2AccessConfig(
                security_mode=args.security_mode,
                allow_disposable=args.allow_disposable,
                statement_timeout_ms=args.statement_timeout_ms,
            ),
        )
        provider.verify()
        database = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
        reader = RawJournalReader(
            args.journal, timeout_seconds=args.journal_timeout_seconds
        )
        config = AdvancedEquityJournalBridgeConfig(
            cohort_id=args.cohort_id,
            journal_generation=args.journal_generation,
            stream_session_id=args.stream_session_id,
            owner_epoch=args.owner_epoch,
            normalizer_version=args.normalizer_version,
            shard_ids=tuple(args.shard_id),
            batch_limit=args.batch_limit,
            consumer_name=args.consumer_name,
        )
        stop = threading.Event()
        if args.continuous:
            signal.signal(signal.SIGINT, lambda *_: stop.set())
            signal.signal(signal.SIGTERM, lambda *_: stop.set())
        return run_bridge(
            database,
            reader,
            config,
            once=args.once,
            poll_seconds=args.poll_seconds,
            stop=stop,
        )
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
