"""Manage the realtime universe from one file (G102 universe sizing, Nov 15).

Commands:
  check  --file F                 validate the file (no database)
  diff   --file F                 what applying it would change against the current desired revision (read-only)
  apply  --file F [--commit]      write it as a new desired revision; without --commit the same transaction runs and
                                  is rolled back (dry run). Optional --source-cohort-id for a fresh database.
  status [--ticker T]             readiness of the current revision: per member READY / WARMING / FAILED (and any
                                  exclusion), the preparation stage and the reason when not READY (read-only)
  export [--out F]                the current desired revision as a universe file

The services keep reading the desired-state tables; this file is only how the operator changes them. A change applies
to the next session that has not opened yet: names READY by the 14:15 UTC activation run that session, the rest join
when READY. Design: docs/g102/G102_UNIVERSE_SIZING_20261114.md section 3. The database connection uses
REALTIME_V2_APP_DSN and the app role; nothing is printed from the DSN.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.universe_limits import universe_limits  # noqa: E402
from realtime.universe_file import (  # noqa: E402
    UniverseFileError,
    diff_universe,
    load_universe_file,
    render_universe,
    universe_from_members,
)

DEFAULT_FILE = BACKEND_DIR / "config" / "realtime_universe.toml"
SCOPE = "ADVANCED_REALTIME"
EXPORT_HEADER = (
    "Realtime universe (G102 universe sizing). Edit, then:\n"
    "  python -B scripts/manage_realtime_universe.py diff --file config/realtime_universe.toml\n"
    "  python -B scripts/manage_realtime_universe.py apply --file config/realtime_universe.toml            (dry run)\n"
    "  python -B scripts/manage_realtime_universe.py apply --file config/realtime_universe.toml --commit\n"
    "Options underlyings must also be listed under [equity] or [sector_etfs]. A change applies to the next session\n"
    "that has not opened; names not READY by the 14:15 UTC activation join when READY."
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "diff", "apply"):
        command = commands.add_parser(name)
        command.add_argument("--file", type=Path, default=DEFAULT_FILE)
        if name == "apply":
            command.add_argument("--commit", action="store_true")
            command.add_argument("--source-cohort-id", type=UUID)
            command.add_argument("--observed-at", type=_datetime)
    status = commands.add_parser("status")
    status.add_argument("--ticker")
    export = commands.add_parser("export")
    export.add_argument("--out", type=Path)
    return parser.parse_args(argv)


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed


def _connect():
    import psycopg2

    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    dsn = os.environ.get("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise SystemExit("REALTIME_V2_APP_DSN is required")
    connection = psycopg2.connect(dsn=dsn, options="-c timezone=UTC")
    with connection.cursor() as cursor:
        cursor.execute(f"SET ROLE {EXPECTED_V2_DATABASE_NAME}_app")
    connection.commit()  # the role holds for the session; a later rollback (dry run) must not undo it
    return connection


def load_current_revision_cursor(cursor):
    """(revision_id, effective_session_date, source_cohort_id, source_slot_version, member rows) or None."""
    cursor.execute(
        "SELECT revision.revision_id::text,revision.effective_session_date,revision.source_cohort_id::text,"
        "revision.source_slot_version FROM rt.realtime_desired_current AS current "
        "JOIN rt.realtime_desired_revisions AS revision USING (revision_id) WHERE current.scope_key=%s",
        (SCOPE,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    cursor.execute(
        "SELECT ticker,member_kind,priority,enabled FROM rt.realtime_desired_members WHERE revision_id=%s "
        "ORDER BY member_kind,ticker",
        (row[0],),
    )
    return row[0], row[1], row[2], row[3], tuple(cursor.fetchall())


def command_diff(cursor, wanted) -> dict:
    current = load_current_revision_cursor(cursor)
    if current is None:
        return {"current_revision": None, "diff": diff_universe(universe_from_members(()), wanted)}
    return {
        "current_revision": current[0],
        "current_effective_session": str(current[1]),
        "diff": diff_universe(universe_from_members(current[4]), wanted),
    }


def command_apply(connection, wanted, *, commit: bool, source_cohort_id: UUID | None, observed_at: datetime) -> dict:
    from realtime.advanced_realtime_desired_state_repository import (
        reconcile_desired_state_revision_cursor,
        record_desired_state_revision_cursor,
    )

    try:
        with connection.cursor() as cursor:
            current = load_current_revision_cursor(cursor)
            if source_cohort_id is None:
                if current is None:
                    raise SystemExit("no current desired revision: pass --source-cohort-id (the bootstrap cohort)")
                source_cohort_id = UUID(current[2])
            diff = diff_universe(universe_from_members(() if current is None else current[4]), wanted)
            change = wanted.to_change(
                source_cohort_id=source_cohort_id,
                source_slot_version=None if current is None else current[3],
            )
            revision = record_desired_state_revision_cursor(cursor, change, observed_at=observed_at)
            result = reconcile_desired_state_revision_cursor(
                cursor, revision.revision_id, materialized_at=observed_at,
            )
        if commit:
            connection.commit()
        else:
            connection.rollback()
    except BaseException:
        connection.rollback()
        raise
    statuses: dict[str, list[str]] = {}
    for row in result.preparations:
        statuses.setdefault(row.status.value, []).append(f"{row.ticker}:{row.kind.value}")
    return {
        "committed": commit,
        "revision_id": str(revision.revision_id),
        "effective_session_date": revision.effective_session_date.isoformat(),
        "state": result.state,
        "reason_codes": list(result.reason_codes),
        "diff": diff,
        "readiness": {status: sorted(names) for status, names in sorted(statuses.items())},
    }


def command_status(cursor, ticker: str | None) -> dict:
    current = load_current_revision_cursor(cursor)
    if current is None:
        return {"current_revision": None}
    cursor.execute(
        "SELECT intent.ticker,intent.member_kind,intent.enabled,readiness.status,readiness.reasons,"
        "readiness.checked_at,work.stage,work.stage_order,work.status,work.attempts,work.next_retry_at,"
        "work.terminal_reason,work.blocked_reason "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_intent_readiness AS readiness ON readiness.intent_id=intent.intent_id "
        "LEFT JOIN rt.realtime_equity_preparation_work AS work "
        "ON work.revision_id=intent.revision_id AND work.intent_id=intent.intent_id "
        "WHERE intent.revision_id=%s" + (" AND intent.ticker=%s" if ticker else "")
        + " ORDER BY intent.ticker,intent.member_kind,work.stage_order",
        (current[0], ticker.strip().upper()) if ticker else (current[0],),
    )
    members: dict[tuple[str, str], dict] = {}
    for row in cursor.fetchall():
        member = members.setdefault((row[0], row[1]), {
            "ticker": row[0], "kind": row[1], "enabled": bool(row[2]), "status": row[3],
            "reasons": list(row[4] or ()), "checked_at": row[5].isoformat() if row[5] else None, "stage": None,
        })
        if row[6] is not None and row[8] != "COMPLETE" and member["stage"] is None:
            member["stage"] = {
                "stage": row[6], "status": row[8], "attempts": row[9],
                "next_retry_at": row[10].isoformat() if row[10] else None,
                "terminal_reason": row[11], "blocked_reason": row[12],
            }
    counts: dict[str, int] = {}
    for member in members.values():
        counts[f"{member['kind']}:{member['status']}"] = counts.get(f"{member['kind']}:{member['status']}", 0) + 1
    stock_like = {key: member for key, member in members.items() if member["kind"] != "OPTION_UNDERLYING"}
    equity_status = {member["ticker"]: member["status"] for member in stock_like.values()}
    # An Options underlying is prepared by the Options preparation on the session morning (WARMING until then);
    # what the operator controls is that its Equity name is READY before contracts are selected.
    options = {
        member["ticker"]: {"equity": equity_status.get(member["ticker"], "MISSING"), "options": member["status"]}
        for member in members.values() if member["kind"] == "OPTION_UNDERLYING"
    }
    from realtime.advanced_realtime_desired_state_repository import load_desired_member_exclusions_cursor

    # Left out of the session by the activation rules (provider inactive or delisted, FAILED, an underlying whose
    # Equity member is not in the session), with the reason.
    exclusions = [dict(row) for row in load_desired_member_exclusions_cursor(cursor, UUID(current[0]))]
    return {
        "current_revision": current[0],
        "effective_session_date": str(current[1]),
        "counts": dict(sorted(counts.items())),
        "equity_not_ready": [member for member in stock_like.values() if member["status"] != "READY"],
        "excluded": exclusions,
        "options_underlyings": options,
        **({"members": list(members.values())} if ticker else {}),
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.command in ("check", "diff", "apply"):
        try:
            wanted = load_universe_file(args.file)
        except (OSError, UniverseFileError) as error:
            print(json.dumps({"valid": False, "file": str(args.file), "error": str(error)}, indent=2))
            return 2
        if args.command == "check":
            print(json.dumps({
                "valid": True, "file": str(args.file), "names": len(wanted.names),
                "equity": len(wanted.equity), "sector_etfs": len(wanted.sector_etfs),
                "options": len(wanted.option_underlyings),
                "limits": universe_limits(),
            }, indent=2))
            return 0
    connection = _connect()
    try:
        if args.command == "apply":
            result = command_apply(
                connection, wanted, commit=args.commit, source_cohort_id=args.source_cohort_id,
                observed_at=args.observed_at or datetime.now(timezone.utc),
            )
        else:
            connection.set_session(readonly=True)
            with connection.cursor() as cursor:
                if args.command == "diff":
                    result = command_diff(cursor, wanted)
                elif args.command == "status":
                    result = command_status(cursor, args.ticker)
                else:
                    current = load_current_revision_cursor(cursor)
                    if current is None:
                        raise SystemExit("no current desired revision")
                    text = render_universe(universe_from_members(current[4]), header=EXPORT_HEADER)
                    if args.out:
                        args.out.write_text(text, encoding="utf-8")
                        result = {"written": str(args.out), "revision_id": current[0]}
                    else:
                        sys.stdout.write(text)
                        return 0
            connection.rollback()
    finally:
        connection.close()
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
