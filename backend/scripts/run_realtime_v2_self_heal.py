#!/usr/bin/env python3
"""Self-heal checks (G102 R21, Nov 11): report stuck or failing work as fault records, record-only by default.

Usage (as the host collector runs it, one pass, read-only):
  python -B scripts/run_realtime_v2_self_heal.py --once --dsn-file /run/secrets/realtime_v2_app_dsn
Every finding is one ``FAULT {json}`` line on stderr (``realtime.fault_journal``); a summary line goes to stdout.

Acting: only with REALTIME_V2_SELF_HEAL_MODE=ACT and the action listed in REALTIME_V2_SELF_HEAL_ACTIONS (today only
MARK_MISSED has an ACT path). The user's rule (Nov 11): record-only for the first one or two sessions, then act.

R99 close-hour window sweep (`realtime/window_sweep.py`), once per session after the close, then exit:
  python -B scripts/run_realtime_v2_self_heal.py --window-sweep [--mode OFF|RECORD|ACT] [--act]
Mode: --mode, else REALTIME_V2_WINDOW_SWEEP, else RECORD (read-only). ACT closes windows only with --act as well;
without it the sweep records and says so. Exit 0; 1 when an ACT close failed; 2 on an invalid mode.

FUTURE: run as its own long-lived service (``--interval-seconds``) in compose or on the servers, with the app DSN
secret; until then the host collector (`runner/fault_collector.py`) runs one pass every few minutes.
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.self_heal import configured_mode, run_self_heal_pass  # noqa: E402
from realtime.window_sweep import (  # noqa: E402
    DEFAULT_LIMIT as WINDOW_SWEEP_LIMIT,
    WindowSweepMode,
    effective_mode as window_sweep_effective_mode,
    run_window_sweep,
    window_sweep_mode,
)

APP_ROLE_NAME = "stocks_realtime_v2_app"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-file", default="/run/secrets/realtime_v2_app_dsn")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=300.0)
    parser.add_argument("--window-sweep", action="store_true",
                        help="R99: one close-hour sweep of windows past their deadline, then exit")
    parser.add_argument("--mode", default=None,
                        help="window sweep mode OFF / RECORD / ACT (default: REALTIME_V2_WINDOW_SWEEP, else RECORD)")
    parser.add_argument("--act", action="store_true",
                        help="window sweep: required, with mode ACT, to close windows; without it ACT only records")
    parser.add_argument("--limit", type=int, default=WINDOW_SWEEP_LIMIT,
                        help="window sweep: at most this many windows past their deadline per run")
    return parser.parse_args(argv)


def _connect(dsn: str, *, read_only: bool):
    connection = psycopg2.connect(
        dsn,
        options="-c statement_timeout=30000" + (" -c default_transaction_read_only=on" if read_only else ""),
    )
    connection.set_session(readonly=read_only, isolation_level="REPEATABLE READ" if read_only else None)
    return connection


def one_pass(dsn: str) -> dict:
    mode, actions = configured_mode()
    now = datetime.now(timezone.utc)
    reader = _connect(dsn, read_only=True)
    try:
        cursor = reader.cursor()
        cursor.execute(f"SET ROLE {APP_ROLE_NAME}")

        @contextmanager
        def write_transaction():
            writer = _connect(dsn, read_only=False)
            try:
                with writer:
                    with writer.cursor() as write_cursor:
                        write_cursor.execute(f"SET LOCAL ROLE {APP_ROLE_NAME}")
                        yield write_cursor
            finally:
                writer.close()

        records = run_self_heal_pass(
            cursor, now=now, mode=mode, actions=actions,
            write_transaction=write_transaction if mode == "ACT" and actions else None,
        )
        reader.rollback()
    finally:
        reader.close()
    summary = {
        "contract": "realtime-self-heal-pass-v1", "checked_at": now.isoformat(), "mode": mode,
        "actions_on": sorted(actions), "findings": len(records),
        "by_kind": {}, "would_act": sum(1 for row in records if row.get("severity") == "WOULD_ACT"),
        "acted": sum(1 for row in records if row.get("severity") == "ACTED"),
    }
    for row in records:
        summary["by_kind"][row["kind"]] = summary["by_kind"].get(row["kind"], 0) + 1
    print(json.dumps(summary, sort_keys=True), flush=True)
    return summary


def window_sweep_pass(dsn: str, *, mode: WindowSweepMode, act_flag: bool, limit: int) -> dict:
    """R99: the sweep in one read-only REPEATABLE READ snapshot; one writer connection is opened only for ACT with
    --act, one transaction per window (as the app role)."""
    now = datetime.now(timezone.utc)
    running, _refused = window_sweep_effective_mode(mode, act_flag)
    if running is WindowSweepMode.OFF:
        return run_window_sweep(None, now=now, mode=mode, act_flag=act_flag)  # no connection at all
    reader = _connect(dsn, read_only=True)
    writers = []
    try:
        cursor = reader.cursor()
        cursor.execute(f"SET ROLE {APP_ROLE_NAME}")

        @contextmanager
        def write_transaction():
            if not writers:
                writers.append(_connect(dsn, read_only=False))
            with writers[0]:  # commit on success, rollback on error; the connection stays open
                with writers[0].cursor() as write_cursor:
                    write_cursor.execute(f"SET LOCAL ROLE {APP_ROLE_NAME}")
                    yield write_cursor

        summary = run_window_sweep(
            cursor, now=now, mode=mode, act_flag=act_flag, limit=limit,
            write_transaction=write_transaction if running is WindowSweepMode.ACT else None,
        )
        reader.rollback()
    finally:
        reader.close()
        for writer in writers:
            writer.close()
    return summary


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.window_sweep:
        try:
            mode = window_sweep_mode(args.mode)
        except ValueError as error:
            print(json.dumps({"contract": "realtime-window-sweep-v1", "error": str(error)}), file=sys.stderr,
                  flush=True)
            return 2
        dsn = Path(args.dsn_file).read_text(encoding="utf-8").strip()
        summary = window_sweep_pass(dsn, mode=mode, act_flag=args.act, limit=args.limit)
        not_closed = summary.get("not_closed") or {}
        return 1 if any(str(reason).startswith("ERROR") for reason in not_closed) else 0
    dsn = Path(args.dsn_file).read_text(encoding="utf-8").strip()
    if args.once:
        one_pass(dsn)
        return 0
    stop = threading.Event()
    while not stop.is_set():
        try:
            one_pass(dsn)
        except Exception as error:  # noqa: BLE001 - the loop reports and goes on
            print(json.dumps({"contract": "realtime-self-heal-pass-v1", "error": f"{type(error).__name__}: {error}"}),
                  flush=True)
        stop.wait(args.interval_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
