#!/usr/bin/env python3
"""Continuously attach live E/O bindings for the current Options session."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from collections import deque
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_lifecycle import AdvancedEquityLifecyclePlanner  # noqa: E402
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    ADVANCED_REALTIME_V3_DRAIN_SECONDS,
    ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
)
from realtime.advanced_options_continuous_scheduler import (  # noqa: E402
    STALE_SNAPSHOT_SKIPPED,
    run_continuous_options_schedule_cycle,
)
from realtime.migrations import validate_target_database_v2  # noqa: E402
from scripts.run_advanced_options_v3_lifecycle import _provider  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=2)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database, args.confirm_database_name, security_mode="hardened"
    )
    if args.poll_seconds <= 0 or not args.continuous or not args.execute:
        parser.error("Options scheduler requires --continuous --execute and positive polling")
    return args


MAXIMUM_CONSECUTIVE_CYCLE_FAILURES = 20


def main(argv=None):
    args = parse_args(argv)
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or dotenv_values(BACKEND_DIR / ".env").get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY is required")
    provider, pool = _provider()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    planner = AdvancedEquityLifecyclePlanner()
    # G74: Equity windows whose Options snapshot was stale; never retried in this process (bounded, newest kept).
    skipped = deque(maxlen=256)
    failures = 0
    try:
        while not stop.is_set():
            lifecycle = planner.plan(
                drain_seconds=ADVANCED_REALTIME_V3_DRAIN_SECONDS,
                finality_delay_seconds=ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
                completion_budget_seconds=ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
            )
            # B11 (Nov 6): any refusal in a cycle raised out of main and Docker restarted the scheduler into a loop;
            # the cause was only in the traceback. A failed cycle now reports why, waits (backing off) and tries
            # again; only a run of failures exits, so a broken connection pool is still restarted.
            try:
                result = run_continuous_options_schedule_cycle(
                    provider, session_date=lifecycle.processing_session_date,
                    api_key=api_key, skipped_receipts=frozenset(skipped),
                )
            except Exception as error:  # noqa: BLE001 - reported, retried, bounded
                failures += 1
                print(json.dumps({
                    "status": "CYCLE_FAILED_WAITING", "consecutive_failures": failures,
                    "reason": f"{type(error).__name__}: {str(error)[:300]}",
                }, sort_keys=True), flush=True)
                if failures >= MAXIMUM_CONSECUTIVE_CYCLE_FAILURES:
                    raise
                stop.wait(args.poll_seconds * min(failures, 6))
                continue
            failures = 0
            if result.get("status") == STALE_SNAPSHOT_SKIPPED:
                skipped.append(result["equity_receipt_id"])
            print(json.dumps(result, sort_keys=True), flush=True)
            stop.wait(args.poll_seconds)
    finally:
        pool.closeall()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
