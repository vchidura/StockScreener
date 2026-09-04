#!/usr/bin/env python3
"""Mark a research scanner's outcomes stale so they are recomputed.

`list_pending_directional_subjects` skips any subject that already holds a
non-stale outcome, so a correction to inputs - a missing benchmark lineage, a
changed bar source - is invisible until the affected rows are marked stale.
Recomputation then supersedes them rather than duplicating.

Selects by source name instead of an events file, which avoids re-reading a
multi-hundred-megabyte JSONL to recover identities already stored as evidence.
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

from database import get_db_cursor  # noqa: E402
from equity.repositories import EquityOutcomeRepository  # noqa: E402

BATCH = 10000


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--interval")
    parser.add_argument("--reason", required=True)
    parser.add_argument(
        "--research-only", action="store_true", default=True,
        help="Restrict to evidence with no analysis run",
    )
    parser.add_argument("--apply", action="store_true")
    arguments = parser.parse_args()

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT evidence.evidence_id
            FROM equity_evidence AS evidence
            WHERE evidence.source_name = %s
              AND (%s IS NULL OR evidence.interval = %s)
              AND (%s = FALSE OR evidence.analysis_run_id IS NULL)
              AND EXISTS (
                  SELECT 1 FROM equity_research_outcomes AS outcome
                  WHERE outcome.subject_evidence_id = evidence.evidence_id
                    AND outcome.is_stale = FALSE
              )
            """,
            (
                arguments.source_name, arguments.interval, arguments.interval,
                arguments.research_only,
            ),
        )
        identities = [row["evidence_id"] for row in cursor.fetchall()]

    report = {
        "source_name": arguments.source_name,
        "interval": arguments.interval,
        "subjects_with_live_outcomes": len(identities),
        "reason": arguments.reason,
        "mode": "APPLY" if arguments.apply else "DRY_RUN",
    }
    if not arguments.apply:
        report["note"] = "nothing marked; re-run with --apply"
        print(json.dumps(report, indent=2))
        return 0

    repository = EquityOutcomeRepository()
    marked = 0
    for offset in range(0, len(identities), BATCH):
        marked += repository.mark_outcomes_stale(
            identities[offset:offset + BATCH], arguments.reason
        )
        print(f"  {min(offset + BATCH, len(identities))}/{len(identities)} subjects",
              flush=True)
    report["outcomes_marked_stale"] = marked
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
