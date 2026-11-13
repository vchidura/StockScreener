"""R54 operator tool: finalize Options journal registrations (re-seal at the journal's own end, mark DRAINED).

Dry run by default (read-only transaction): one JSON line per Options registration with the sealed offsets, the
journal's end and hash (main + WAL), the consumer checkpoints and the action it would take. `--apply` writes only the
generations named with `--generation` (never a blanket apply): past sessions need the user's yes.

    python scripts/finalize_advanced_options_journal.py --dsn-file /run/secrets/realtime_v2_app_dsn \
        --journal-dir /var/lib/realtime-v2-advanced [--since 2026-11-09] [--generation UUID ...] [--apply]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from realtime.advanced_options_journal_finalize import (  # noqa: E402
    apply_finalize_cursor,
    consumer_offsets_cursor,
    load_options_registrations_cursor,
    plan_finalize,
    read_journal_end,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn-file", required=True)
    parser.add_argument("--journal-dir", required=True)
    parser.add_argument("--role", default="stocks_realtime_v2_app")
    parser.add_argument("--since", type=date.fromisoformat)
    parser.add_argument("--generation", action="append", type=UUID, default=[])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--no-hash", action="store_true", help="dry run without hashing the journal files")
    args = parser.parse_args(argv)
    if args.apply and not args.generation:
        parser.error("--apply needs at least one --generation (no blanket apply)")
    if args.apply and args.no_hash:
        parser.error("--apply needs the journal hash")
    import psycopg2

    connection = psycopg2.connect(Path(args.dsn_file).read_text(encoding="utf-8").strip(), options="-c timezone=UTC")
    connection.set_session(readonly=not args.apply, isolation_level="REPEATABLE READ")
    exit_code = 0
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SET ROLE {args.role}")
            registrations = load_options_registrations_cursor(cursor, generations=args.generation, since=args.since)
            for registration in registrations:
                path = Path(args.journal_dir) / registration.relative_path
                try:
                    end = read_journal_end(path, with_hash=not args.no_hash)
                except FileNotFoundError:
                    end = None
                plan = plan_finalize(registration, end, consumer_offsets_cursor(cursor, registration))
                line = plan.summary()
                if args.apply:
                    line["applied"] = apply_finalize_cursor(cursor, plan, checked_at=datetime.now(timezone.utc))
                if plan.action == "REFUSE":
                    exit_code = 2
                print(json.dumps(line, sort_keys=True), flush=True)
        if args.apply:
            connection.commit()
        else:
            connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
