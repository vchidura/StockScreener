#!/usr/bin/env python3
"""Discover, reconcile and drain registered Advanced journals."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from psycopg2 import Error as PsycopgError
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_journal_registry import (  # noqa: E402
    discover_incomplete_advanced_journals_cursor,
    quarantine_advanced_journal_cursor,
)
from realtime.advanced_journal_replay import (  # noqa: E402
    JournalReplayQuarantine,
    replay_registered_journal_once,
)
from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedJournalBlockedError,
)
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402


UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--journal-root", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced journal replay targets stocks_realtime_v2")
    if args.journal_root is not None:
        args.journal_root = args.journal_root.resolve()
        if not args.journal_root.is_dir():
            parser.error(
                "--journal-root must be an existing mounted directory"
            )
    if not args.execute:
        parser.error("Advanced journal replay requires --execute")
    return args


def _provider(config):
    if config.app_dsn is None:
        raise ValueError("Advanced journal replay app DSN is unavailable")
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def replay_registered_once(
    provider,
    database,
    journal_root,
    *,
    checked_at,
    emit=print,
) -> tuple[int, int]:
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        registrations = discover_incomplete_advanced_journals_cursor(
            unit.cursor
        )
    drained = 0
    quarantined = 0
    for registration in registrations:
        try:
            status = replay_registered_journal_once(
                provider,
                database,
                journal_root,
                registration,
                checked_at=checked_at,
                batch_limit=1_000,
            )
            emit(json.dumps({
                "journal_generation": str(
                    registration.journal_generation
                ),
                "status": "REPLAYED",
                "committed_offset": status.committed_offset,
                "persisted_bars": status.persisted_bars,
            }))
            drained += int(
                status.committed_offset
                >= registration.normalized_committed_offset
            )
        except JournalReplayQuarantine as error:
            with provider.transaction(APP_ROLE) as unit:
                quarantine_advanced_journal_cursor(
                    unit.cursor,
                    registration.journal_generation,
                    reason=error.reason,
                    detail=error.detail,
                    checked_at=checked_at,
                )
            emit(json.dumps({
                "journal_generation": str(
                    registration.journal_generation
                ),
                "status": "QUARANTINED",
                "reason": error.reason,
            }))
            quarantined += 1
        except AdvancedJournalBlockedError as error:
            emit(json.dumps({
                "journal_generation": str(
                    registration.journal_generation
                ),
                "status": "BLOCKED_GAP",
                "reason": str(error)[:160],
            }))
        except (PsycopgError, OSError, TimeoutError) as error:
            emit(json.dumps({
                "journal_generation": str(
                    registration.journal_generation
                ),
                "status": "RETRYABLE_INFRASTRUCTURE",
                "reason": type(error).__name__,
            }))
        except ValueError as error:
            with provider.transaction(APP_ROLE) as unit:
                quarantine_advanced_journal_cursor(
                    unit.cursor,
                    registration.journal_generation,
                    reason="BRIDGE_VALIDATION_FAILED",
                    detail=str(error)[:320],
                    checked_at=checked_at,
                )
            emit(json.dumps({
                "journal_generation": str(
                    registration.journal_generation
                ),
                "status": "QUARANTINED",
                "reason": "BRIDGE_VALIDATION_FAILED",
            }))
            quarantined += 1
    return drained, quarantined


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.BRIDGE,
    )
    journal_root = args.journal_root or config.advanced_journal_dir
    if journal_root is None or not journal_root.is_dir():
        raise RuntimeError("Advanced journal replay root is unavailable")
    provider, pool = _provider(config)
    database = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
    stop = threading.Event()
    if args.continuous:
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        while not stop.is_set():
            replay_registered_once(
                provider,
                database,
                journal_root,
                checked_at=_now(),
            )
            if args.once or stop.wait(1.0):
                return 0
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
