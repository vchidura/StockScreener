#!/usr/bin/env python3
"""Plan or terminalize exact stale V3 orchestration records."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import psycopg2
from dotenv import dotenv_values
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc


def _jsonable(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(member) for key, member in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(member) for member in value]
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--preflight-report", type=Path, required=True)
    parser.add_argument("--expected-preflight-semantic-sha256", required=True)
    parser.add_argument("--expected-preflight-file-sha256", required=True)
    parser.add_argument("--window-id", type=UUID, action="append", required=True)
    parser.add_argument("--stream-session-id", type=UUID, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("stale-record terminalization targets stocks_realtime_v2")
    args.preflight_report = args.preflight_report.resolve()
    args.report_path = args.report_path.resolve()
    if not args.preflight_report.is_file():
        parser.error("--preflight-report must exist")
    if args.report_path.exists():
        parser.error("--report-path must be new")
    args.window_ids = tuple(sorted(set(args.window_id), key=str))
    if len(args.window_ids) != 2:
        parser.error("exactly two distinct --window-id values are required")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    return args


def _load_preflight(args):
    body = args.preflight_report.read_bytes()
    report = json.loads(body)
    if (
        hashlib.sha256(body).hexdigest()
        != args.expected_preflight_file_sha256
        or report.get("semantic_sha256")
        != args.expected_preflight_semantic_sha256
    ):
        raise ValueError("preflight report identity changed")
    rows = tuple(
        row
        for row in report["gap_report"]["buckets"]["STALE"]
        if row["fact"] == "expired_windows_ended_nonterminal_streams"
    )
    if len(rows) != 1:
        raise ValueError("stale-record evidence is ambiguous")
    evidence = rows[0]["evidence"]
    if (
        {UUID(row["window_id"]) for row in evidence["windows"]}
        != set(args.window_ids)
        or {UUID(row["session_id"]) for row in evidence["streams"]}
        != {args.stream_session_id}
    ):
        raise ValueError("approved stale-record membership changed")
    return report


def _dsn() -> str:
    values = dotenv_values(BACKEND_DIR / ".env")
    value = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not value:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    return value


def _snapshot(cursor, args, *, lock: bool):
    lock_clause = " FOR UPDATE" if lock else ""
    cursor.execute(
        "SELECT window_id::text,state,owner_id::text,owner_epoch,"
        "lease_until,deadline,updated_at FROM rt.window_runs "
        "WHERE window_id=ANY(%s::uuid[]) ORDER BY window_id"
        + lock_clause,
        ([str(value) for value in args.window_ids],),
    )
    windows = tuple(dict(row) for row in cursor.fetchall())
    cursor.execute(
        "SELECT session_id::text,status,failure_code,ended_at,heartbeat_at "
        "FROM rt.stream_sessions WHERE session_id=%s" + lock_clause,
        (str(args.stream_session_id),),
    )
    streams = tuple(dict(row) for row in cursor.fetchall())
    cursor.execute(
        "SELECT resource_key,owner_id::text,lease_until "
        "FROM rt.worker_leases WHERE lease_until>clock_timestamp()"
    )
    live_leases = tuple(dict(row) for row in cursor.fetchall())
    cursor.execute("SELECT clock_timestamp() AS observed_at")
    observed_at = cursor.fetchone()["observed_at"]
    if (
        len(windows) != 2
        or {row["window_id"] for row in windows}
        != {str(value) for value in args.window_ids}
        or any(
            row["state"] != "RUNNING"
            or row["deadline"] >= observed_at
            or row["lease_until"] is None
            or row["lease_until"] >= observed_at
            for row in windows
        )
        or len(streams) != 1
        or streams[0]["status"] != "BLOCKED"
        or streams[0]["ended_at"] is None
        or live_leases
    ):
        raise RuntimeError("stale-record runtime identity changed")
    return observed_at, windows, streams


def main(argv=None) -> int:
    args = parse_args(argv)
    preflight = _load_preflight(args)
    with closing(
        psycopg2.connect(
            _dsn(),
            application_name="terminalize_advanced_options_v3_stale_records",
            options="-c timezone=UTC",
            cursor_factory=RealDictCursor,
        )
    ) as connection:
        connection.set_session(
            readonly=not args.execute,
            isolation_level="SERIALIZABLE",
        )
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SET LOCAL ROLE {}").format(
                        sql.Identifier(f"{EXPECTED_V2_DATABASE_NAME}_app")
                    )
                )
                observed_at, windows, streams = _snapshot(
                    cursor,
                    args,
                    lock=args.execute,
                )
                if args.execute:
                    cursor.execute(
                        "UPDATE rt.window_runs SET state='FAILED',"
                        "owner_id=NULL,lease_until=NULL,"
                        "updated_at=clock_timestamp() "
                        "WHERE window_id=ANY(%s::uuid[]) "
                        "AND state='RUNNING' AND deadline<clock_timestamp() "
                        "AND lease_until<clock_timestamp()",
                        ([str(value) for value in args.window_ids],),
                    )
                    if cursor.rowcount != 2:
                        raise RuntimeError(
                            "stale window terminalization lost its fence"
                        )
                    cursor.execute(
                        "UPDATE rt.stream_sessions SET status='FAILED',"
                        "failure_code='STALE_ENDED_BLOCKED_TERMINALIZED' "
                        "WHERE session_id=%s AND status='BLOCKED' "
                        "AND ended_at IS NOT NULL",
                        (str(args.stream_session_id),),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            "stale stream terminalization lost its fence"
                        )
        if not args.execute:
            connection.rollback()
    report = {
        "contract": "advanced-options-v3-stale-terminalization-v1",
        "mode": "EXECUTE" if args.execute else "PLAN",
        "status": "TERMINALIZED" if args.execute else "PLANNED",
        "database": args.database,
        "authorization_reference": args.authorization_reference,
        "preflight_semantic_sha256": preflight["semantic_sha256"],
        "observed_at_utc": observed_at.astimezone(UTC).isoformat(),
        "windows": _jsonable(windows),
        "streams": _jsonable(streams),
        "transitions": {
            "window_runs": "RUNNING->FAILED",
            "stream_sessions": "BLOCKED->FAILED",
        },
        "rows_updated": 3 if args.execute else 0,
        "rows_deleted": 0,
        "effects": {
            "provider_requests": 0,
            "service_actions": 0,
            "publication": 0,
            "stored_read_changes": 0,
            "trading": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
