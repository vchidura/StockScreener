#!/usr/bin/env python3
"""Purge research scanner evidence and outcomes for a source family.

Dry run by default. Deletes in foreign-key order: outcomes, dependent evidence
references, then evidence. Qualification revisions are retained unless
--drop-qualification is given, because they are the published record and carry
no foreign key back to evidence. That default is the intended retention model:
keep the verdict for reporting, discard the bulk it was derived from.

Composite scanner names are shared between the live pipeline and research
replay, so --exclude-production scopes deletion to rows with no analysis run
rather than refusing the whole family.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--source-prefix", action="append", dest="prefixes", required=True,
        help="Repeatable source_name prefix, e.g. INTRADAY_ or CONTROL_",
    )
    result.add_argument("--interval")
    result.add_argument("--apply", action="store_true")
    result.add_argument("--drop-qualification", action="store_true")
    result.add_argument(
        "--research-only", action="store_true", default=True,
        help="Refuse to touch evidence linked to a production analysis run",
    )
    result.add_argument(
        "--exclude-production", action="store_true",
        help="Delete only rows with no analysis run instead of aborting when the "
             "source family also has production evidence",
    )
    return result


def counts(cursor, patterns: list[str], interval: str | None) -> dict[str, int]:
    cursor.execute(
        """
        SELECT
          (SELECT count(*) FROM equity_evidence e
             WHERE e.source_name LIKE ANY(%s::TEXT[])
               AND (%s IS NULL OR e.interval = %s)) AS evidence,
          (SELECT count(*) FROM equity_evidence e
             WHERE e.source_name LIKE ANY(%s::TEXT[])
               AND (%s IS NULL OR e.interval = %s)
               AND e.analysis_run_id IS NOT NULL) AS production_linked,
          (SELECT count(*) FROM equity_research_outcomes o
             JOIN equity_evidence e ON e.evidence_id = o.subject_evidence_id
             WHERE e.source_name LIKE ANY(%s::TEXT[])
               AND (%s IS NULL OR e.interval = %s)) AS outcomes,
          (SELECT count(*) FROM equity_qualification_revisions q
             WHERE q.source_name LIKE ANY(%s::TEXT[])
               AND (%s IS NULL OR q.interval = %s)) AS revisions
        """,
        (patterns, interval, interval) * 4,
    )
    return dict(cursor.fetchone())


def main() -> int:
    arguments = parser().parse_args()
    patterns = [f"{prefix}%" for prefix in arguments.prefixes]
    interval = arguments.interval

    with get_db_cursor() as cursor:
        before = counts(cursor, patterns, interval)

    report = {
        "source_prefixes": arguments.prefixes,
        "interval": interval,
        "matched": before,
        "mode": "APPLY" if arguments.apply else "DRY_RUN",
        "drop_qualification": arguments.drop_qualification,
        "scope": (
            "RESEARCH_ROWS_ONLY" if arguments.exclude_production
            else "ALL_MATCHED_ROWS"
        ),
    }
    if arguments.exclude_production:
        report["targeted"] = {
            "evidence": before["evidence"] - before["production_linked"],
            "retained_production_evidence": before["production_linked"],
        }

    # Deleting evidence and outcomes scans every child table whose foreign key is
    # unindexed; see scripts/check_unindexed_foreign_keys.py. Creating them needs
    # table ownership, so report rather than fail when they are absent.
    required_indexes = {
        "equity_research_outcomes": "idx_equity_research_outcomes_supersedes",
        "equity_evidence": "idx_equity_evidence_supersedes",
    }
    missing = []
    with get_db_cursor() as cursor:
        for table, index in required_indexes.items():
            cursor.execute(
                "SELECT 1 FROM pg_indexes WHERE tablename = %s AND indexname = %s",
                (table, index),
            )
            if cursor.fetchone() is None:
                missing.append(index)
    report["supporting_indexes"] = (
        "present" if not missing else
        f"missing {', '.join(missing)}; expect a slow purge, "
        "create them as the table owner"
    )

    if (before["production_linked"] and arguments.research_only
            and not arguments.exclude_production):
        report["aborted"] = (
            f"{before['production_linked']} matched evidence rows are linked to an "
            "analysis run; refusing to delete production lineage. Re-run with "
            "--exclude-production to purge only the research rows"
        )
        print(json.dumps(report, indent=2))
        return 2

    if not arguments.apply:
        report["note"] = "no rows deleted; re-run with --apply"
        print(json.dumps(report, indent=2))
        return 0

    deleted: dict[str, int] = {}
    subject_sql = """
        SELECT evidence_id FROM equity_evidence
        WHERE source_name LIKE ANY(%s::TEXT[])
          AND (%s IS NULL OR interval = %s)
          AND (%s = FALSE OR analysis_run_id IS NULL)
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            subject_sql,
            (patterns, interval, interval, arguments.exclude_production),
        )
        subject_ids = [row["evidence_id"] for row in cursor.fetchall()]

    batch = 20000
    for name in ("outcomes", "equity_context_evidence",
                 "equity_current_projection", "evidence"):
        deleted[name] = 0
    for offset in range(0, len(subject_ids), batch):
        chunk = subject_ids[offset:offset + batch]
        with get_db_cursor() as cursor:
            cursor.execute(
                "DELETE FROM equity_research_outcomes "
                "WHERE subject_evidence_id = ANY(%s::UUID[])",
                (chunk,),
            )
            deleted["outcomes"] += cursor.rowcount
            for table, column in (
                ("equity_context_evidence", "evidence_id"),
                ("equity_current_projection", "evidence_id"),
            ):
                cursor.execute(
                    f"DELETE FROM {table} WHERE {column} = ANY(%s::UUID[])",
                    (chunk,),
                )
                deleted[table] += cursor.rowcount
            cursor.execute(
                "DELETE FROM equity_evidence WHERE evidence_id = ANY(%s::UUID[])",
                (chunk,),
            )
            deleted["evidence"] += cursor.rowcount
        print(
            f"purged {min(offset + batch, len(subject_ids))}/{len(subject_ids)} subjects",
            flush=True,
        )

    if arguments.drop_qualification:
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM equity_qualification_revisions
                WHERE source_name LIKE ANY(%s::TEXT[])
                  AND (%s IS NULL OR interval = %s)
                """,
                (patterns, interval, interval),
            )
            deleted["qualification_revisions"] = cursor.rowcount

    report["deleted"] = deleted
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
