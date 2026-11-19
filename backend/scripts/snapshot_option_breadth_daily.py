"""G106 H1 operator tool: copy closed sessions' option chains into rt.option_breadth_daily (migration 055).

Dry run by default (read-only transaction): one JSON line per session with the rows in the current table, the rows
already in the daily table, what a copy would add, and whether the session is closed (exchange close + 20 minutes).
`--apply` writes in one transaction (INSERT ... ON CONFLICT DO NOTHING: a re-run adds nothing) and prints the rows
written per session and underlying. A session that is still open is refused (exit code 2) and nothing is written.

    # after the close, inside the Options scheduler container (it holds the app role's DSN):
    python scripts/snapshot_option_breadth_daily.py --session 2026-11-17 [--apply]
    # every session still in rt.option_breadth_current (the open one, if any, is skipped):
    python scripts/snapshot_option_breadth_daily.py --backfill-retained [--apply]
    # a CSV copy of the current table's rows for one session (gzip or plain), when those rows are gone:
    python scripts/snapshot_option_breadth_daily.py --csv /tmp/close-retained.csv.gz [--apply]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from realtime.option_breadth_daily import (  # noqa: E402
    SessionStillOpen,
    backfill_retained_cursor,
    load_csv_session_cursor,
    plan_session_cursor,
    report_json,
    retained_sessions_cursor,
    snapshot_session_cursor,
)

DEFAULT_DSN_FILE = "/run/secrets/realtime_v2_app_dsn"
DEFAULT_ROLE = "stocks_realtime_v2_app"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--session", action="append", type=date.fromisoformat)
    target.add_argument("--backfill-retained", action="store_true")
    target.add_argument("--csv", action="append", type=Path)
    parser.add_argument("--dsn-file", default=DEFAULT_DSN_FILE)
    parser.add_argument("--role", default=DEFAULT_ROLE, help="role to SET (empty: the login role, test databases only)")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def _connect(dsn_file: str):
    import psycopg2

    connection = psycopg2.connect(
        Path(dsn_file).read_text(encoding="utf-8").strip(),
        options="-c timezone=UTC -c statement_timeout=120000 -c lock_timeout=5000",
    )
    return connection


def run(cursor, args, *, now: datetime) -> tuple[list[dict], int]:
    lines, exit_code = [], 0
    if args.csv:
        for path in args.csv:
            if not args.apply:
                lines.append({"csv": str(path), "status": "DRY_RUN", "note": "--apply loads the file"})
                continue
            try:
                lines.append(load_csv_session_cursor(cursor, path=path, now=now))
            except SessionStillOpen as error:
                lines.append({"csv": str(path), "status": "REFUSED_OPEN", "reason": str(error)})
                exit_code = 2
        return lines, exit_code
    if args.backfill_retained and args.apply:
        lines = backfill_retained_cursor(cursor, now=now)
    else:
        sessions = retained_sessions_cursor(cursor) if args.backfill_retained else tuple(args.session)
        for session_date in sessions:
            if not args.apply:
                lines.append(plan_session_cursor(cursor, session_date=session_date, now=now))
                continue
            try:
                lines.append(snapshot_session_cursor(cursor, session_date=session_date, now=now))
            except SessionStillOpen as error:
                lines.append({"session_date": session_date.isoformat(), "status": "REFUSED_OPEN", "reason": str(error)})
                exit_code = 2
            except ValueError as error:
                lines.append({"session_date": session_date.isoformat(), "status": "REFUSED_NOT_A_SESSION",
                              "reason": str(error)})
                exit_code = 2
    if args.apply and any(line.get("complete") is False for line in lines):
        exit_code = exit_code or 3
    return lines, exit_code


def main(argv=None) -> int:
    args = parse_args(argv)
    connection = _connect(args.dsn_file)
    connection.set_session(readonly=not args.apply)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            database = cursor.fetchone()[0]
            if database != "stocks_realtime_v2" and not database.startswith("test_realtime_v2_"):
                raise SystemExit(f"refused: {database} is not stocks_realtime_v2 or a test_realtime_v2_ database")
            if args.role:
                from psycopg2 import sql

                cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(args.role)))
            lines, exit_code = run(cursor, args, now=datetime.now(timezone.utc))
        for line in lines:
            print(report_json(line), flush=True)
        if args.apply and exit_code in (0, 3):
            connection.commit()
        else:
            connection.rollback()
        print(json.dumps({
            "database": database, "applied": bool(args.apply and exit_code in (0, 3)),
            "sessions": len(lines), "inserted": sum(int(line.get("inserted") or 0) for line in lines),
            "exit_code": exit_code,
        }, sort_keys=True), flush=True)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
