#!/usr/bin/env python3
"""Validate immutable equity bars, lineage, publications, and current pointers."""
from __future__ import annotations

import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor
from equity.portal_snapshots import SNAPSHOT_TYPES


def validate() -> dict[str, object]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS invalid_rows
            FROM equity_bar_revisions
            WHERE open_price <= 0 OR high_price <= 0 OR low_price <= 0
               OR close_price <= 0 OR volume < 0
               OR high_price < GREATEST(open_price, close_price, low_price)
               OR low_price > LEAST(open_price, close_price, high_price)
               OR bar_end <= bar_start
               OR (is_final AND system_observed_at < bar_end)
               OR session_scope NOT IN ('RTH', 'EXTENDED', 'FULL_DAY')
            """
        )
        invalid_rows = int(cursor.fetchone()["invalid_rows"])

        cursor.execute(
            """
            SELECT COUNT(*) AS invalid_derived
            FROM equity_bar_revisions
            WHERE source_kind = 'DERIVED'
              AND interval IN ('1h', '1d', '1wk', '1mo')
                            AND EXISTS (
                                    SELECT 1
                                    FROM unnest(quality_codes) quality_code
                                    WHERE quality_code LIKE 'DERIVED_FROM_CANONICAL_%'
                            )
              AND cardinality(source_bar_revision_ids) = 0
            """
        )
        invalid_derived = int(cursor.fetchone()["invalid_derived"])

        cursor.execute(
            """
            SELECT COUNT(*) AS unresolved
            FROM equity_bar_revisions derived
            CROSS JOIN LATERAL unnest(derived.source_bar_revision_ids) source_id
            LEFT JOIN equity_bar_revisions source
              ON source.bar_revision_id = source_id
            WHERE source.bar_revision_id IS NULL
            """
        )
        unresolved_bar_lineage = int(cursor.fetchone()["unresolved"])

        cursor.execute(
            """
            SELECT COUNT(*) AS invalid_publications
            FROM equity_bar_publications publication
            LEFT JOIN LATERAL (
                SELECT COUNT(*) FILTER (WHERE status = 'SELECTED') AS selected,
                       COUNT(*) FILTER (WHERE status = 'MISSING') AS missing,
                       COUNT(*) FILTER (WHERE status = 'FAILED') AS failed
                FROM equity_bar_publication_members member
                WHERE member.publication_id = publication.publication_id
            ) counts ON TRUE
            WHERE publication.selected_members <> counts.selected
               OR publication.missing_members <> counts.missing
               OR publication.failed_members <> counts.failed
            """
        )
        invalid_publications = int(cursor.fetchone()["invalid_publications"])

        cursor.execute(
            """
            WITH latest AS (
                SELECT interval, MAX(bar_end) AS market_time
                FROM equity_current_bar_projection
                GROUP BY interval
            )
            SELECT projection.interval,
                   COUNT(*) AS members,
                   latest.market_time
            FROM equity_current_bar_projection projection
            JOIN latest
                ON latest.interval = projection.interval
               AND latest.market_time = projection.bar_end
            GROUP BY projection.interval, latest.market_time
            ORDER BY projection.interval
            """
        )
        current_coverage = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT reconciliation_status, quality_codes, COUNT(*) AS members
            FROM equity_canonical_bars
            WHERE interval = '1mo'
              AND bar_end = (
                  SELECT MAX(bar_end) FROM equity_canonical_bars WHERE interval = '1mo'
              )
            GROUP BY reconciliation_status, quality_codes
            ORDER BY members DESC, reconciliation_status, quality_codes
            """
        )
        monthly_reconciliation = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT snapshot.snapshot_type,
                   snapshot.source_generation = source.generation AS is_fresh,
                   snapshot.generated_at, projection.published_at
            FROM equity_portal_current_projections projection
            JOIN equity_portal_snapshots snapshot
              ON snapshot.snapshot_id = projection.snapshot_id
            CROSS JOIN equity_portal_source_state source
            WHERE source.singleton = TRUE
            ORDER BY snapshot.snapshot_type
            """
        )
        portal_snapshots = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            WITH expected AS (
                SELECT analysis_run_id, SUM(evidence_count)::bigint AS evidence_count
                FROM equity_analysis_members
                GROUP BY analysis_run_id
            ), actual AS (
                SELECT analysis_run_id, COUNT(*)::bigint AS evidence_count
                FROM equity_evidence
                WHERE analysis_run_id IS NOT NULL
                GROUP BY analysis_run_id
            )
            SELECT run.analysis_run_id, run.interval, run.model_bundle_version,
                   expected.evidence_count AS expected_evidence,
                   COALESCE(actual.evidence_count, 0) AS actual_evidence
            FROM equity_analysis_runs run
            JOIN expected ON expected.analysis_run_id = run.analysis_run_id
            LEFT JOIN actual ON actual.analysis_run_id = run.analysis_run_id
            WHERE run.status IN ('COMPLETE', 'DEGRADED')
              AND expected.evidence_count <> COALESCE(actual.evidence_count, 0)
            ORDER BY run.created_at
            """
        )
        analysis_evidence_mismatches = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT COUNT(*) AS mismatches
            FROM equity_current_projection projection
            JOIN equity_evidence evidence ON evidence.evidence_id = projection.evidence_id
            WHERE projection.analysis_run_id IS DISTINCT FROM evidence.analysis_run_id
            """
        )
        current_projection_run_mismatches = int(cursor.fetchone()["mismatches"])

    portal_by_type = {row["snapshot_type"]: row for row in portal_snapshots}
    invalid_portal_snapshots = sorted(
        snapshot_type for snapshot_type in SNAPSHOT_TYPES
        if snapshot_type not in portal_by_type
        or not portal_by_type[snapshot_type]["is_fresh"]
    )

    failures = {
        "invalid_bar_rows": invalid_rows,
        "derived_without_lineage": invalid_derived,
        "unresolved_bar_lineage": unresolved_bar_lineage,
        "invalid_publication_counts": invalid_publications,
        "invalid_portal_snapshots": invalid_portal_snapshots,
        "analysis_evidence_mismatches": analysis_evidence_mismatches,
        "current_projection_run_mismatches": current_projection_run_mismatches,
    }
    return {
        "status": "PASS" if not any(failures.values()) else "FAIL",
        "failures": failures,
        "current_coverage": current_coverage,
        "monthly_reconciliation": monthly_reconciliation,
        "portal_snapshots": portal_snapshots,
    }


def main() -> int:
    result = validate()
    print(json.dumps(result, default=str, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())