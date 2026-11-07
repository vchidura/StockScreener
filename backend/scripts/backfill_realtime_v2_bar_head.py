#!/usr/bin/env python3
"""G102 A4 (design 4.7 step 1): fill rt.bar_head from every cohort's current bars.

One transaction per instrument, so the run is resumable: an instrument already filled keeps its rows (ON CONFLICT DO
NOTHING; the writer chose them by the same policy). PLAN (the default) only counts. --apply writes. The target must be
stocks_realtime_v2 or an explicit test_realtime_v2_ disposable copy (--allow-disposable-target), confirmed by name.
Prints one JSON summary.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import sys
import time

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

import psycopg2  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from realtime.equity_bar_repository import backfill_bar_head_cursor  # noqa: E402
from realtime.migrations import SECURITY_MODE_HARDENED, validate_target_database_v2  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--allow-disposable-target", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--statement-timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database, args.confirm_database_name, allow_disposable=args.allow_disposable_target,
        security_mode=SECURITY_MODE_HARDENED,
    )
    return args


def _connect(database: str):
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("bar head backfill requires: " + ", ".join(missing))
    return psycopg2.connect(
        dbname=database, user=settings["POSTGRES_ADMIN_USER"], password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"], port=settings["DB_PORT"], application_name="backfill_realtime_v2_bar_head",
        options="-c timezone=UTC",
    )


def run(connection, *, apply: bool, statement_timeout_seconds: int, clock=time.monotonic) -> dict[str, object]:
    started = clock()
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT cur.instrument_id,count(*) FROM rt.bar_current AS cur GROUP BY 1 ORDER BY 1"
        )
        instruments = [(int(row[0]), int(row[1])) for row in cursor.fetchall()]
        cursor.execute("SELECT count(*) FROM rt.bar_head")
        before = int(cursor.fetchone()[0])
    connection.rollback()
    summary = {
        "mode": "APPLY" if apply else "PLAN",
        "instruments": len(instruments),
        "current_rows": sum(count for _instrument, count in instruments),
        "head_rows_before": before,
    }
    if not apply:
        return summary
    inserted, slowest = 0, (None, 0.0)
    for instrument_id, _count in instruments:
        began = clock()
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = %s", (f"{statement_timeout_seconds}s",))
            inserted += backfill_bar_head_cursor(cursor, instrument_id=instrument_id)
        connection.commit()
        elapsed = clock() - began
        if elapsed > slowest[1]:
            slowest = (instrument_id, elapsed)
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM rt.bar_head")
        after = int(cursor.fetchone()[0])
    connection.rollback()
    summary.update(
        inserted=inserted, head_rows_after=after, seconds=round(clock() - started, 1),
        slowest_instrument={"instrument_id": slowest[0], "seconds": round(slowest[1], 2)},
    )
    return summary


def main(argv=None) -> int:
    args = parse_args(argv)
    with closing(_connect(args.database)) as connection:
        print(json.dumps(run(connection, apply=args.apply, statement_timeout_seconds=args.statement_timeout_seconds),
                         sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
