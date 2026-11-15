"""Options journal retention (R54 part 3, G106 H3, R81): keep the newest sessions hot, archive and delete older ones.

DRY RUN by default: prints one JSON line per Options journal (tier, files, archive state, finalize plan, planned steps,
reasons) and a summary line. Reads the registry in a READ ONLY transaction and the journal files read-only; writes
nothing. The current and previous XNYS sessions are never touched, whatever --hot-sessions says.

Applying needs one explicit flag per step, in this order:
  --apply finalize   re-seal and mark DRAINED (registry write, R54 rule; only SEAL_AND_DRAIN / DRAIN plans)
  --apply archive    write <archive-dir>/<generation>.tar.{zst,xz} + manifest, then verify it (decompress, re-hash)
  --apply delete     delete the originals: verified archive, registration DRAINED, originals unchanged since archived
`--generation UUID` (repeatable) limits the apply steps to those journals. The first live apply needs the user's yes.

    python scripts/retain_advanced_options_journals.py --dsn-file /run/secrets/realtime_v2_app_dsn \
        --journal-dir /var/lib/realtime-v2-advanced [--hot-sessions 10] [--archive-dir DIR] [--no-hash]
        [--apply finalize --apply archive --apply delete] [--generation UUID ...]

Rehearsal on copies without a database: `--export-registry FILE` (read-only, with --dsn-file) writes the registry rows
and consumer offsets; `--registry-snapshot FILE --rehearsal` plans and applies archive/delete from that file instead
(finalize is refused without a database).
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
    OptionsJournalRegistration,
    apply_finalize_cursor,
    consumer_offsets_cursor,
    load_options_registrations_cursor,
    plan_finalize,
    read_journal_end,
)
from realtime.advanced_options_journal_retention import (  # noqa: E402
    DEFAULT_ARCHIVE_DIRECTORY,
    DEFAULT_HOT_SESSIONS,
    FINALIZE_ACTIONS,
    RetentionError,
    Step,
    Tier,
    archive_options_journal,
    delete_options_journal_originals,
    load_archive_manifests,
    plan_options_journal_retention,
    preliminary_tiers,
    scan_options_journals,
    select_codec,
    verify_options_journal_archive,
)

UTC = timezone.utc
STEPS = ("finalize", "archive", "delete")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--dsn-file", type=Path)
    source.add_argument("--registry-snapshot", type=Path)
    parser.add_argument("--role", default="stocks_realtime_v2_app")
    parser.add_argument("--journal-dir", type=Path, default=Path("/var/lib/realtime-v2-advanced"))
    parser.add_argument("--archive-dir", type=Path)
    parser.add_argument("--hot-sessions", type=int, default=DEFAULT_HOT_SESSIONS)
    parser.add_argument("--today", type=date.fromisoformat, default=None, help="planning date (default: UTC today)")
    parser.add_argument("--generation", action="append", type=UUID, default=[])
    parser.add_argument("--apply", action="append", choices=STEPS, default=[])
    parser.add_argument("--codec", choices=("auto", "zstd", "xz"), default="auto")
    parser.add_argument("--xz-preset", type=int, default=1)
    parser.add_argument("--no-hash", action="store_true", help="dry run: skip hashing the COLD journals")
    parser.add_argument("--export-registry", type=Path, help="write the registry rows and consumer offsets (read-only)")
    parser.add_argument("--rehearsal", action="store_true", help="allow applying from a registry snapshot (copies)")
    args = parser.parse_args(argv)
    if args.hot_sessions < 1:
        parser.error("--hot-sessions must be at least 1")
    if args.apply and args.no_hash:
        parser.error("--apply needs the journal hash")
    if args.registry_snapshot is not None:
        if "finalize" in args.apply:
            parser.error("--apply finalize needs the database (--dsn-file)")
        if args.apply and not args.rehearsal:
            parser.error("applying from a registry snapshot needs --rehearsal (copies only)")
        if args.export_registry is not None:
            parser.error("--export-registry needs --dsn-file")
    if args.export_registry is not None and args.apply:
        parser.error("--export-registry is read-only; do not combine it with --apply")
    if args.archive_dir is None:
        args.archive_dir = args.journal_dir / DEFAULT_ARCHIVE_DIRECTORY
    if args.today is None:
        args.today = datetime.now(UTC).date()
    return args


def _emit(document) -> None:
    print(json.dumps(document, sort_keys=True, default=str), flush=True)


class _Database:
    def __init__(self, dsn_file: Path, role: str) -> None:
        import psycopg2

        self._psycopg2 = psycopg2
        self._dsn = dsn_file.read_text(encoding="utf-8").strip()
        self._role = role

    def connect(self, *, read_only: bool):
        connection = self._psycopg2.connect(self._dsn, options="-c timezone=UTC")
        connection.set_session(readonly=read_only, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute(f"SET ROLE {self._role}")
        return connection


def _registration_document(row: OptionsJournalRegistration) -> dict:
    return {
        "journal_generation": str(row.journal_generation), "cohort_id": str(row.cohort_id),
        "session_date": row.session_date.isoformat(), "relative_path": row.relative_path, "status": row.status,
        "raw_committed_offset": row.raw_committed_offset,
        "normalized_committed_offset": row.normalized_committed_offset,
        "sealed_files_sha256": row.sealed_files_sha256,
    }


def _registration_from_document(value) -> OptionsJournalRegistration:
    return OptionsJournalRegistration(
        UUID(str(value["journal_generation"])), UUID(str(value["cohort_id"])),
        date.fromisoformat(str(value["session_date"])), str(value["relative_path"]), str(value["status"]),
        int(value["raw_committed_offset"]), int(value["normalized_committed_offset"]),
        value.get("sealed_files_sha256"),
    )


def _load_snapshot(path: Path):
    document = json.loads(path.read_text(encoding="utf-8"))
    registrations = tuple(_registration_from_document(row) for row in document["registrations"])
    consumers = {UUID(key): {str(k): int(v) for k, v in value.items()}
                 for key, value in (document.get("consumers") or {}).items()}
    return registrations, consumers


def _journal_end(journal_dir: Path, registration: OptionsJournalRegistration, *, with_hash: bool):
    try:
        return read_journal_end(journal_dir / registration.relative_path, with_hash=with_hash)
    except (FileNotFoundError, OSError):
        return None
    except Exception:  # noqa: BLE001 - an unreadable SQLite file plans as unavailable (REFUSE)
        return None


def main(argv=None) -> int:
    args = parse_args(argv)
    database = None if args.dsn_file is None else _Database(args.dsn_file, args.role)
    if database is not None:
        connection = database.connect(read_only=True)
        try:
            with connection.cursor() as cursor:
                registrations = load_options_registrations_cursor(cursor)
                consumers = {row.journal_generation: consumer_offsets_cursor(cursor, row) for row in registrations}
        finally:
            connection.rollback()
            connection.close()
    else:
        registrations, consumers = _load_snapshot(args.registry_snapshot)
    if args.export_registry is not None:
        args.export_registry.write_text(json.dumps({
            "exported_at": datetime.now(UTC).isoformat(),
            "registrations": [_registration_document(row) for row in registrations],
            "consumers": {str(key): value for key, value in consumers.items()},
        }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _emit({"line": "export", "path": str(args.export_registry), "registrations": len(registrations)})

    _current, _previous, _hot, tiers = preliminary_tiers(
        registrations, today=args.today, hot_sessions=args.hot_sessions,
    )
    finalize_plans = {}
    for row in registrations:
        if tiers[row.journal_generation] is Tier.COLD and row.status in {"WRITING", "DRAINING"}:
            end = _journal_end(args.journal_dir, row, with_hash=not args.no_hash)
            finalize_plans[row.journal_generation] = plan_finalize(row, end, consumers.get(row.journal_generation, {}))
    plan = plan_options_journal_retention(
        registrations,
        scan_options_journals(args.journal_dir),
        load_archive_manifests(args.archive_dir),
        today=args.today,
        hot_sessions=args.hot_sessions,
        finalize_plans=finalize_plans,
    )
    for entry in plan.entries:
        _emit({"line": "journal", **entry.summary()})
    _emit({"line": "plan", "dry_run": not args.apply, "apply": args.apply,
           "journal_dir": str(args.journal_dir), "archive_dir": str(args.archive_dir), **plan.summary()})
    if not args.apply:
        return 0

    selected = set(args.generation)
    targets = [entry for entry in plan.entries if not selected or entry.journal_generation in selected]
    failures = 0

    if "finalize" in args.apply:
        for entry in targets:
            if Step.FINALIZE not in entry.steps:
                continue
            connection = database.connect(read_only=False)
            try:
                with connection.cursor() as cursor:
                    rows = load_options_registrations_cursor(cursor, generations=(entry.journal_generation,))
                    if len(rows) != 1:
                        raise RetentionError("registration disappeared")
                    fresh = plan_finalize(
                        rows[0], _journal_end(args.journal_dir, rows[0], with_hash=True),
                        consumer_offsets_cursor(cursor, rows[0]),
                    )
                    applied = (
                        apply_finalize_cursor(cursor, fresh, checked_at=datetime.now(UTC))
                        if fresh.action in FINALIZE_ACTIONS else "SKIPPED:" + fresh.action
                    )
                connection.commit()
                _emit({"line": "finalize", "journal_generation": str(entry.journal_generation), "applied": applied,
                       "reasons": list(fresh.reasons)})
            except Exception as error:  # noqa: BLE001 - one journal's failure is reported, the rest continue
                connection.rollback()
                failures += 1
                _emit({"line": "finalize", "journal_generation": str(entry.journal_generation),
                       "error": f"{type(error).__name__}:{error}"[:300]})
            finally:
                connection.close()

    if "archive" in args.apply:
        codec = select_codec(args.codec)
        for entry in targets:
            if Step.ARCHIVE not in entry.steps:
                continue
            started = datetime.now(UTC)
            try:
                manifest = archive_options_journal(
                    args.journal_dir, args.archive_dir, entry, codec=codec, xz_preset=args.xz_preset,
                )
                manifest = verify_options_journal_archive(args.archive_dir, manifest)
                _emit({"line": "archive", "journal_generation": str(entry.journal_generation),
                       "archive": manifest.archive_name, "codec": codec, "original_bytes": entry.original_bytes,
                       "archive_bytes": manifest.archive_bytes, "verified": manifest.verified,
                       "seconds": round((datetime.now(UTC) - started).total_seconds(), 1)})
            except Exception as error:  # noqa: BLE001
                failures += 1
                _emit({"line": "archive", "journal_generation": str(entry.journal_generation),
                       "error": f"{type(error).__name__}:{error}"[:300]})

    if "delete" in args.apply:
        manifests = load_archive_manifests(args.archive_dir)
        if database is not None:
            connection = database.connect(read_only=True)
            try:
                with connection.cursor() as cursor:
                    statuses = {row.journal_generation: row.status for row in load_options_registrations_cursor(cursor)}
            finally:
                connection.rollback()
                connection.close()
        else:
            statuses = {row.journal_generation: row.status for row in _load_snapshot(args.registry_snapshot)[0]}
        for entry in targets:
            if Step.DELETE not in entry.steps:
                continue
            try:
                manifest = manifests.get(entry.journal_generation)
                if manifest is None:
                    raise RetentionError("refusing to delete: no archive manifest")
                deleted = delete_options_journal_originals(
                    args.journal_dir, args.archive_dir, entry, manifest,
                    registry_status=str(statuses.get(entry.journal_generation)),
                )
                _emit({"line": "delete", "journal_generation": str(entry.journal_generation),
                       "freed_bytes": entry.original_bytes, "deleted_at": deleted.deleted_at})
            except Exception as error:  # noqa: BLE001
                failures += 1
                _emit({"line": "delete", "journal_generation": str(entry.journal_generation),
                       "error": f"{type(error).__name__}:{error}"[:300]})
    return 2 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
