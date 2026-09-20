#!/usr/bin/env python3
"""Report WP6 package-assessment storage and evidence without changing data."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env", override=True)

from database import get_db_connection  # noqa: E402
from options.outcome_contracts import PACKAGE_ASSESSMENT_POLICY  # noqa: E402


MIGRATION_VERSION = "048_option_package_assessments"
UNAVAILABLE_MIGRATION_VERSION = "049_option_outcome_unavailable_evidence"
POLICY_MIGRATION_VERSION = "050_option_package_assessment_policy"
LEG_CONTRACT_MIGRATION_VERSION = "051_option_outcome_unavailable_leg_contract"
PACKAGE_CONTRACT_MIGRATION_VERSION = "052_option_package_candidate_contract"
TABLE_NAME = "public.option_package_assessments"
UNAVAILABLE_TABLE_NAME = "public.option_outcome_unavailable_evidence"
EXPECTED_TRIGGERS = {
    "trg_guard_option_package_assessment",
    "trg_option_package_assessment_immutable",
    "trg_option_package_assessment_no_truncate",
}
EXPECTED_UNAVAILABLE_TRIGGERS = {
    "trg_guard_option_outcome_unavailable_evidence",
    "trg_option_outcome_unavailable_immutable",
    "trg_option_outcome_unavailable_no_truncate",
}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=24, choices=range(1, 745))
    parser.add_argument("--require-storage-ready", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _enabled(value: str | None) -> bool:
    return (value or "false").strip().lower() in {"1", "true", "yes", "on"}


def build_report(hours: int) -> dict[str, object]:
    with get_db_connection() as connection:
        connection.rollback()
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '10s'")
                cursor.execute("SELECT clock_timestamp() AS checked_at")
                checked_at = cursor.fetchone()["checked_at"]
                since = checked_at - timedelta(hours=hours)
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM public.schema_migrations WHERE version=%s
                    ) AS registered,
                    to_regclass(%s) IS NOT NULL AS table_present,
                    EXISTS (
                        SELECT 1 FROM public.schema_migrations WHERE version=%s
                    ) AS unavailable_registered,
                    to_regclass(%s) IS NOT NULL AS unavailable_table_present,
                    EXISTS (
                        SELECT 1 FROM public.schema_migrations WHERE version=%s
                    ) AS policy_registered,
                    EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public'
                          AND table_name='option_package_assessments'
                          AND column_name='assessment_policy_version'
                          AND is_nullable='NO'
                    ) AS policy_version_column_ready,
                    EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public'
                          AND table_name='option_package_assessments'
                          AND column_name='assessment_policy_sha256'
                          AND is_nullable='NO'
                    ) AS policy_hash_column_ready,
                    EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conrelid=to_regclass(%s)
                          AND conname='ck_option_package_assessment_policy'
                    ) AS policy_constraint_ready,
                    to_regclass('public.idx_option_package_assessment_policy')
                                                IS NOT NULL AS policy_index_ready,
                                        EXISTS (
                                                SELECT 1 FROM public.schema_migrations WHERE version=%s
                                        ) AS unavailable_leg_contract_registered,
                                        EXISTS (
                                                SELECT 1 FROM pg_constraint
                                                WHERE conrelid=to_regclass(%s)
                                                    AND conname='ck_option_outcome_unavailable_payload_arrays'
                                        ) AS unavailable_payload_arrays_ready,
                                        EXISTS (
                                                SELECT 1 FROM pg_proc AS function
                                                JOIN pg_namespace AS namespace
                                                    ON namespace.oid=function.pronamespace
                                                WHERE namespace.nspname='public'
                                                    AND function.proname='guard_option_outcome_unavailable_evidence'
                                                    AND pg_get_functiondef(function.oid)
                                                            LIKE '%%package legs or policy disagree%%'
                                        ) AS unavailable_leg_guard_ready
                                        ,EXISTS (
                                                SELECT 1 FROM public.schema_migrations WHERE version=%s
                                        ) AS package_candidate_contract_registered
                                        ,EXISTS (
                                                SELECT 1 FROM pg_proc AS function
                                                JOIN pg_namespace AS namespace
                                                    ON namespace.oid=function.pronamespace
                                                WHERE namespace.nspname='public'
                                                    AND function.proname='option_package_assessment_matches_candidate'
                                        ) AS package_candidate_matcher_ready
                                        ,EXISTS (
                                                SELECT 1 FROM pg_proc AS function
                                                JOIN pg_namespace AS namespace
                                                    ON namespace.oid=function.pronamespace
                                                WHERE namespace.nspname='public'
                                                    AND function.proname='guard_option_package_assessment'
                                                    AND pg_get_functiondef(function.oid)
                                                            LIKE '%%candidate or package disagrees%%'
                                        ) AS package_candidate_guard_ready
                    """,
                    (
                        MIGRATION_VERSION, TABLE_NAME,
                        UNAVAILABLE_MIGRATION_VERSION, UNAVAILABLE_TABLE_NAME,
                        POLICY_MIGRATION_VERSION, TABLE_NAME,
                        LEG_CONTRACT_MIGRATION_VERSION, UNAVAILABLE_TABLE_NAME,
                        PACKAGE_CONTRACT_MIGRATION_VERSION,
                    ),
                )
                state = dict(cursor.fetchone())
                report: dict[str, object] = {
                    "checked_at": checked_at.isoformat(),
                    "window_start": since.isoformat(),
                    "migration_version": MIGRATION_VERSION,
                    **state,
                    "writer_enabled": _enabled(
                        os.getenv("OPTION_PACKAGE_ASSESSMENTS_ENABLED")
                    ),
                    "unavailable_writer_enabled": _enabled(
                        os.getenv("OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED")
                    ),
                    "research_only": True,
                    "execution_permission": False,
                }
                if not state["table_present"]:
                    report.update(
                        storage_ready=False, storage_state="MIGRATION_REQUIRED",
                        runtime_select=False, runtime_insert=False, triggers=[],
                        assessment_count=0, status_counts={}, structure_counts={},
                        outcome_unavailable_count=0, framework_storage_ready=False,
                        unavailable_runtime_select=False,
                        unavailable_runtime_insert=False,
                        unavailable_triggers=[],
                        policy_usage=None,
                    )
                    return report
                cursor.execute(
                    """
                    SELECT has_table_privilege(current_user, %s, 'SELECT') AS runtime_select,
                           has_table_privilege(current_user, %s, 'INSERT') AS runtime_insert
                    """,
                    (TABLE_NAME, TABLE_NAME),
                )
                report.update(dict(cursor.fetchone()))
                cursor.execute(
                    """
                    SELECT trigger.tgname
                    FROM pg_trigger AS trigger
                    JOIN pg_class AS relation ON relation.oid=trigger.tgrelid
                    JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
                    WHERE namespace.nspname='public'
                      AND relation.relname='option_package_assessments'
                      AND NOT trigger.tgisinternal
                    ORDER BY trigger.tgname
                    """
                )
                triggers = [row["tgname"] for row in cursor.fetchall()]
                report["triggers"] = triggers
                cursor.execute(
                    """
                    SELECT assessment_status, COUNT(*) AS count
                    FROM public.option_package_assessments
                    WHERE recorded_at >= %s AND recorded_at < %s
                    GROUP BY assessment_status ORDER BY assessment_status
                    """,
                    (since, checked_at),
                )
                status_counts = {
                    row["assessment_status"]: row["count"] for row in cursor.fetchall()
                }
                report["status_counts"] = status_counts
                report["assessment_count"] = sum(status_counts.values())
                cursor.execute(
                    """
                    SELECT payload_text::jsonb->'package'->>'structure' AS structure,
                           COUNT(*) AS count
                    FROM public.option_package_assessments
                    WHERE recorded_at >= %s AND recorded_at < %s
                      AND assessment_status='READY'
                    GROUP BY structure ORDER BY structure
                    """,
                    (since, checked_at),
                )
                report["structure_counts"] = {
                    row["structure"]: row["count"] for row in cursor.fetchall()
                }
                policy_ready = bool(
                    state["policy_registered"]
                    and state["policy_version_column_ready"]
                    and state["policy_hash_column_ready"]
                    and state["policy_constraint_ready"]
                    and state["policy_index_ready"]
                )
                if policy_ready:
                    policy_since = checked_at - timedelta(
                        seconds=PACKAGE_ASSESSMENT_POLICY.usage_window_seconds
                    )
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS assessment_count,
                               COALESCE(SUM(octet_length(payload_text)), 0)
                                   AS payload_bytes,
                               COUNT(*) FILTER (
                                   WHERE assessment_status='UNAVAILABLE'
                               ) AS unavailable_count
                        FROM public.option_package_assessments
                        WHERE assessed_at >= %s AND assessed_at < %s
                          AND assessment_policy_sha256=%s
                        """,
                        (policy_since, checked_at, PACKAGE_ASSESSMENT_POLICY.sha256),
                    )
                    policy_usage = dict(cursor.fetchone())
                    policy_usage["unavailable_fraction"] = (
                        policy_usage["unavailable_count"]
                        / policy_usage["assessment_count"]
                        if policy_usage["assessment_count"] else None
                    )
                    policy_usage.update(
                        policy_version=PACKAGE_ASSESSMENT_POLICY.version,
                        policy_sha256=PACKAGE_ASSESSMENT_POLICY.sha256,
                        window_start=policy_since.isoformat(),
                        window_end=checked_at.isoformat(),
                    )
                    pause_reasons = []
                    if (
                        policy_usage["assessment_count"]
                        >= PACKAGE_ASSESSMENT_POLICY.maximum_assessments
                    ):
                        pause_reasons.append("ASSESSMENT_LIMIT_PAUSE")
                    if (
                        policy_usage["payload_bytes"]
                        >= PACKAGE_ASSESSMENT_POLICY.maximum_payload_bytes
                    ):
                        pause_reasons.append("PAYLOAD_LIMIT_PAUSE")
                    if (
                        policy_usage["assessment_count"]
                        >= PACKAGE_ASSESSMENT_POLICY.minimum_assessments_before_rate_pause
                        and policy_usage["unavailable_fraction"]
                        > PACKAGE_ASSESSMENT_POLICY.maximum_unavailable_fraction
                    ):
                        pause_reasons.append("UNAVAILABLE_RATE_PAUSE")
                    policy_usage["pause_reasons"] = pause_reasons
                    report["policy_usage"] = policy_usage
                else:
                    report["policy_usage"] = None
                if state["unavailable_table_present"]:
                    cursor.execute(
                        """
                        SELECT has_table_privilege(current_user, %s, 'SELECT')
                                   AS unavailable_runtime_select,
                               has_table_privilege(current_user, %s, 'INSERT')
                                   AS unavailable_runtime_insert
                        """,
                        (UNAVAILABLE_TABLE_NAME, UNAVAILABLE_TABLE_NAME),
                    )
                    report.update(dict(cursor.fetchone()))
                    cursor.execute(
                        """
                        SELECT trigger.tgname
                        FROM pg_trigger AS trigger
                        JOIN pg_class AS relation ON relation.oid=trigger.tgrelid
                        JOIN pg_namespace AS namespace
                          ON namespace.oid=relation.relnamespace
                        WHERE namespace.nspname='public'
                          AND relation.relname='option_outcome_unavailable_evidence'
                          AND NOT trigger.tgisinternal
                        ORDER BY trigger.tgname
                        """
                    )
                    report["unavailable_triggers"] = [
                        row["tgname"] for row in cursor.fetchall()
                    ]
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM public.option_outcome_unavailable_evidence
                        WHERE recorded_at >= %s AND recorded_at < %s
                        """,
                        (since, checked_at),
                    )
                    report["outcome_unavailable_count"] = cursor.fetchone()["count"]
                else:
                    report["outcome_unavailable_count"] = 0
                    report["unavailable_runtime_select"] = False
                    report["unavailable_runtime_insert"] = False
                    report["unavailable_triggers"] = []
                storage_ready = bool(
                    state["registered"] and report["runtime_select"]
                    and report["runtime_insert"]
                    and EXPECTED_TRIGGERS.issubset(triggers)
                )
                report["storage_ready"] = storage_ready
                report["storage_state"] = (
                    "READY_ENABLED" if storage_ready and report["writer_enabled"]
                    else "READY_DISABLED" if storage_ready else "INCOMPLETE"
                )
                report["framework_storage_ready"] = bool(
                    storage_ready and state["unavailable_registered"]
                    and state["unavailable_table_present"]
                    and report["unavailable_runtime_select"]
                    and report["unavailable_runtime_insert"]
                    and EXPECTED_UNAVAILABLE_TRIGGERS.issubset(
                        report["unavailable_triggers"]
                    )
                    and policy_ready
                    and state["unavailable_leg_contract_registered"]
                    and state["unavailable_payload_arrays_ready"]
                    and state["unavailable_leg_guard_ready"]
                    and state["package_candidate_contract_registered"]
                    and state["package_candidate_matcher_ready"]
                    and state["package_candidate_guard_ready"]
                )
                report["collection_state"] = (
                    "INCOMPLETE" if not report["framework_storage_ready"]
                    else "PAUSED" if report["policy_usage"]["pause_reasons"]
                    else "RUNNING" if report["writer_enabled"]
                    else "READY_DISABLED"
                )
            connection.rollback()
            return report
        finally:
            if not connection.closed:
                connection.rollback()
                connection.set_session(
                    readonly=False, isolation_level="READ COMMITTED"
                )


def main() -> int:
    args = _arguments()
    report = build_report(args.hours)
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return (
        0
        if report["framework_storage_ready"] or not args.require_storage_ready
        else 2
    )


if __name__ == "__main__":
    raise SystemExit(main())