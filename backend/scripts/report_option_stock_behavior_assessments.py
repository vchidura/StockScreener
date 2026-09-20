#!/usr/bin/env python3
"""Report WP5 assessment storage and rolling validation without changing data."""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env", override=True)

from database import get_db_connection  # noqa: E402
from options.stock_behavior_shadow_launch import (  # noqa: E402
    OptionStockBehaviorShadowLaunch,
    load_option_stock_behavior_shadow_launch,
)


MIGRATION_VERSION = "046_option_stock_behavior_assessments"
LAUNCH_IDENTITY_MIGRATION_VERSION = "047_option_stock_behavior_launch_identity"
TABLE_NAME = "public.option_stock_behavior_assessments"
EXPECTED_TRIGGERS = {
    "trg_guard_option_stock_behavior_assessment",
    "trg_option_stock_behavior_assessment_immutable",
    "trg_option_stock_behavior_assessment_no_truncate",
}
ADMISSION_BLOCKERS = (
    "VERSIONED_SETUP_EPISODE_MEASUREMENT_REQUIRED",
    "VERSIONED_ACCEPTANCE_OR_RESUMPTION_TRIGGER_REQUIRED",
    "VERSIONED_TARGET_ROOM_MEASUREMENT_REQUIRED",
    "PREREGISTERED_THRESHOLD_RESEARCH_REQUIRED",
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=24, choices=range(1, 745))
    parser.add_argument("--require-storage-ready", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--launch-file", type=Path)
    parser.add_argument("--write-launch-artifact", action="store_true")
    parser.add_argument("--session-date", type=date.fromisoformat, help="Read-only daily behavior/outcome scorecard for one session.")
    parser.add_argument("--as-of", type=datetime.fromisoformat, help="Aware observation cutoff; defaults to actual receipt time.")
    parser.add_argument("--review-view", choices=("SHORTLIST", "ELIGIBLE", "ALL", "DAILY"), default="DAILY")
    return parser.parse_args()


def _enabled(value: str | None) -> bool:
    return (value or "false").strip().lower() in {"1", "true", "yes", "on"}


@contextmanager
def _read_only_connection():
    with get_db_connection() as connection:
        connection.rollback()
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            yield connection
        finally:
            if not connection.closed:
                connection.rollback()
                connection.set_session(
                    readonly=False, isolation_level="READ COMMITTED"
                )


def build_report(
    hours: int, launch: OptionStockBehaviorShadowLaunch | None = None,
) -> dict[str, object]:
    with _read_only_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '10s'")
                cursor.execute("SELECT clock_timestamp() AS checked_at")
                checked_at = cursor.fetchone()["checked_at"]
                since = checked_at - timedelta(hours=hours)
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM public.schema_migrations WHERE version = %s
                    ) AS registered,
                    EXISTS (
                        SELECT 1 FROM public.schema_migrations WHERE version = %s
                    ) AS launch_identity_registered,
                    to_regclass(%s) IS NOT NULL AS table_present
                    """,
                    (MIGRATION_VERSION, LAUNCH_IDENTITY_MIGRATION_VERSION, TABLE_NAME),
                )
                state = dict(cursor.fetchone())
                report: dict[str, object] = {
                    "checked_at": checked_at.isoformat(),
                    "window_start": since.isoformat(),
                    "migration_version": MIGRATION_VERSION,
                    **state,
                    "shadow_writer_enabled": _enabled(
                        os.getenv("OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED")
                    ),
                    "admission_activation_ready": False,
                    "admission_blockers": ADMISSION_BLOCKERS,
                }
                if not state["table_present"]:
                    report.update(
                        runtime_select=False, runtime_insert=False,
                        triggers=(), assessment_count=0, candidate_count=0,
                        disposition_counts={}, latest_decision_at=None,
                        latest_recorded_at=None, storage_ready=False,
                        storage_state="MIGRATION_REQUIRED",
                    )
                    return report
                cursor.execute(
                    """
                    SELECT has_table_privilege(current_user, %s, 'SELECT') AS runtime_select,
                                                     has_table_privilege(current_user, %s, 'INSERT') AS runtime_insert,
                                                     EXISTS (
                                                             SELECT 1 FROM information_schema.columns
                                                             WHERE table_schema = 'public'
                                                                 AND table_name = 'option_stock_behavior_assessments'
                                                                 AND column_name = 'launch_id' AND is_nullable = 'NO'
                                                     ) AS launch_id_column_ready,
                                                     EXISTS (
                                                             SELECT 1 FROM information_schema.columns
                                                             WHERE table_schema = 'public'
                                                                 AND table_name = 'option_stock_behavior_assessments'
                                                                 AND column_name = 'launch_manifest_sha256'
                                                                 AND is_nullable = 'NO'
                                                     ) AS launch_hash_column_ready,
                                                     EXISTS (
                                                             SELECT 1 FROM pg_constraint
                                                             WHERE conrelid = %s::regclass
                                                                 AND conname = 'ck_option_stock_behavior_launch_contract'
                                                     ) AS launch_constraint_ready,
                                                     to_regclass('public.idx_option_stock_behavior_launch') IS NOT NULL
                                                             AS launch_index_ready
                    """,
                                        (TABLE_NAME, TABLE_NAME, TABLE_NAME),
                )
                report.update(dict(cursor.fetchone()))
                cursor.execute(
                    """
                                        SELECT trigger.tgname AS trigger_name
                                        FROM pg_trigger AS trigger
                                        JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
                                        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                                        WHERE namespace.nspname = 'public'
                                            AND relation.relname = 'option_stock_behavior_assessments'
                                            AND NOT trigger.tgisinternal
                                        ORDER BY trigger.tgname
                    """
                )
                triggers = tuple(row["trigger_name"] for row in cursor.fetchall())
                report["triggers"] = triggers
                cursor.execute(
                    """
                    SELECT COUNT(*) AS assessment_count,
                           COUNT(DISTINCT candidate_id) AS candidate_count,
                           MAX(decision_at) AS latest_decision_at,
                           MAX(recorded_at) AS latest_recorded_at
                    FROM public.option_stock_behavior_assessments
                    WHERE recorded_at >= %s AND recorded_at < %s
                    """,
                    (since, checked_at),
                )
                counts = dict(cursor.fetchone())
                for key in ("latest_decision_at", "latest_recorded_at"):
                    counts[key] = counts[key].isoformat() if counts[key] else None
                report.update(counts)
                cursor.execute(
                    """
                    SELECT disposition, COUNT(*) AS count
                    FROM public.option_stock_behavior_assessments
                    WHERE recorded_at >= %s AND recorded_at < %s
                    GROUP BY disposition ORDER BY disposition
                    """,
                    (since, checked_at),
                )
                report["disposition_counts"] = {
                    row["disposition"]: row["count"] for row in cursor.fetchall()
                }
                cursor.execute(
                    """
                    SELECT COUNT(*) FILTER (
                               WHERE status = 'LEADER'
                                 AND last_heartbeat_at >= %s - INTERVAL '60 seconds'
                           ) AS fresh_leader_count,
                           MAX(last_heartbeat_at) FILTER (
                               WHERE status = 'LEADER'
                           ) AS latest_leader_heartbeat
                    FROM public.option_scheduler_instances
                    """,
                    (checked_at,),
                )
                leadership = dict(cursor.fetchone())
                if leadership["latest_leader_heartbeat"] is not None:
                    leadership["latest_leader_heartbeat"] = leadership[
                        "latest_leader_heartbeat"
                    ].isoformat()
                report["leadership"] = leadership
                if launch is not None:
                    usage_since, usage_until = launch.usage_bounds(checked_at)
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS assessment_count,
                               COUNT(DISTINCT candidate_id) AS candidate_count,
                               COALESCE(SUM(octet_length(payload_text)), 0) AS payload_bytes,
                               COUNT(*) FILTER (WHERE disposition = 'UNAVAILABLE') AS unavailable_count,
                               percentile_cont(0.50) WITHIN GROUP (
                                   ORDER BY EXTRACT(EPOCH FROM recorded_at - decision_at)
                               ) AS p50_decision_lag_seconds,
                               percentile_cont(0.95) WITHIN GROUP (
                                   ORDER BY EXTRACT(EPOCH FROM recorded_at - decision_at)
                               ) AS p95_decision_lag_seconds,
                               percentile_cont(0.05) WITHIN GROUP (
                                   ORDER BY EXTRACT(EPOCH FROM (
                                       (payload_text::jsonb->>'entry_deadline')::timestamptz
                                       - recorded_at
                                   ))
                               ) FILTER (
                                   WHERE payload_text::jsonb->>'entry_deadline' IS NOT NULL
                               ) AS p05_entry_deadline_slack_seconds
                        FROM public.option_stock_behavior_assessments
                        WHERE decision_at >= %s AND decision_at < %s
                                                    AND launch_manifest_sha256 = %s
                          AND underlying = ANY(%s)
                        """,
                        (
                            usage_since, usage_until,
                            launch.sha256, list(launch.underlyers),
                        ),
                    )
                    launch_metrics = dict(cursor.fetchone())
                    assessment_count = launch_metrics["assessment_count"]
                    unavailable_fraction = (
                        launch_metrics["unavailable_count"] / assessment_count
                        if assessment_count else None
                    )
                    stop_reasons = []
                    validation_warnings = []
                    if assessment_count >= launch.maximum_assessments:
                        stop_reasons.append("ASSESSMENT_LIMIT_REACHED")
                    if launch_metrics["payload_bytes"] >= launch.maximum_payload_bytes:
                        stop_reasons.append("PAYLOAD_LIMIT_REACHED")
                    if assessment_count >= launch.minimum_assessments_before_rate_stops:
                        if unavailable_fraction > launch.maximum_unavailable_fraction:
                            stop_reasons.append("UNAVAILABLE_RATE_STOP")
                        if (
                            launch_metrics["p95_decision_lag_seconds"] is not None
                            and launch_metrics["p95_decision_lag_seconds"]
                            > launch.maximum_p95_decision_lag_seconds
                        ):
                            stop_reasons.append("DECISION_LAG_STOP")
                    if (
                        launch_metrics["p05_entry_deadline_slack_seconds"] is not None
                        and launch_metrics["p05_entry_deadline_slack_seconds"] < 0
                    ):
                        validation_warnings.append("NEGATIVE_ENTRY_DEADLINE_SLACK")
                    cursor.execute(
                        """
                        SELECT underlying, COUNT(*) AS assessment_count,
                               COUNT(*) FILTER (
                                   WHERE disposition = 'ELIGIBLE_RESEARCH'
                               ) AS eligible_count,
                               COUNT(*) FILTER (
                                   WHERE disposition = 'BLOCKED'
                               ) AS blocked_count,
                               COUNT(*) FILTER (
                                   WHERE disposition = 'UNAVAILABLE'
                               ) AS unavailable_count,
                               MAX(decision_at) AS latest_decision_at
                        FROM public.option_stock_behavior_assessments
                        WHERE decision_at >= %s AND decision_at < %s
                          AND launch_manifest_sha256 = %s
                          AND underlying = ANY(%s)
                        GROUP BY underlying ORDER BY underlying
                        """,
                        (
                            usage_since, usage_until, launch.sha256,
                            list(launch.underlyers),
                        ),
                    )
                    observed_underlyers = {
                        row["underlying"]: dict(row) for row in cursor.fetchall()
                    }
                    coverage_by_underlying = []
                    for underlyer in launch.underlyers:
                        coverage = observed_underlyers.get(underlyer)
                        if coverage is None:
                            coverage_by_underlying.append({
                                "underlying": underlyer,
                                "assessment_count": 0,
                                "eligible_count": 0,
                                "blocked_count": 0,
                                "unavailable_count": 0,
                                "latest_decision_at": None,
                                "gap_reasons": ["NO_ASSESSMENT_IN_ROLLING_WINDOW"],
                            })
                            continue
                        coverage["latest_decision_at"] = coverage[
                            "latest_decision_at"
                        ].isoformat()
                        coverage["gap_reasons"] = (
                            ["UNDERLYING_BEHAVIOR_UNAVAILABLE"]
                            if coverage["unavailable_count"] else []
                        )
                        coverage_by_underlying.append(coverage)
                    cursor.execute(
                        """
                        WITH scoped AS (
                            SELECT assessment_id, payload_text::jsonb AS payload
                            FROM public.option_stock_behavior_assessments
                            WHERE decision_at >= %s AND decision_at < %s
                              AND launch_manifest_sha256 = %s
                        ), gates AS (
                            SELECT assessment_id, gate
                            FROM scoped
                            CROSS JOIN LATERAL jsonb_array_elements(
                                payload->'gates'
                            ) AS gate
                        )
                        SELECT RIGHT(gate->>'gate_id', -LENGTH('TREND_SLOPE_')) AS interval,
                               COUNT(DISTINCT assessment_id) AS assessment_count,
                               COUNT(DISTINCT assessment_id) FILTER (
                                   WHERE gate->>'verdict' <> 'UNAVAILABLE'
                               ) AS available_count,
                               COUNT(DISTINCT assessment_id) FILTER (
                                   WHERE gate->>'verdict' = 'UNAVAILABLE'
                               ) AS unavailable_count
                        FROM gates
                        WHERE gate->>'gate_id' LIKE 'TREND_SLOPE_%%'
                        GROUP BY interval ORDER BY interval
                        """,
                        (usage_since, usage_until, launch.sha256),
                    )
                    observed_intervals = {
                        row["interval"]: dict(row) for row in cursor.fetchall()
                    }
                    coverage_by_timeframe = []
                    for interval in ("1d", "1h", "30m"):
                        coverage = observed_intervals.get(interval, {
                            "interval": interval, "assessment_count": 0,
                            "available_count": 0, "unavailable_count": 0,
                        })
                        coverage["gap_reasons"] = (
                            ["NO_REQUIRED_TREND_GATE_IN_ROLLING_WINDOW"]
                            if not coverage["assessment_count"]
                            else ["REQUIRED_TREND_GATE_UNAVAILABLE"]
                            if coverage["unavailable_count"] else []
                        )
                        coverage_by_timeframe.append(coverage)
                    launch_state = (
                        "PAUSED" if stop_reasons
                        else "EXPIRED" if launch.ends_at is not None and checked_at >= launch.ends_at
                        else "PENDING" if checked_at < launch.starts_at
                        else "RUNNING" if report["shadow_writer_enabled"]
                        else "READY_DISABLED"
                    )
                    report["launch"] = {
                        "launch_id": launch.launch_id,
                        "launch_sha256": launch.sha256,
                        "mode": launch.mode,
                        "starts_at": launch.starts_at.isoformat(),
                        "ends_at": launch.ends_at.isoformat() if launch.ends_at else None,
                        "usage_window_start": usage_since.isoformat(),
                        "usage_window_end": usage_until.isoformat(),
                        "usage_window_seconds": launch.usage_window_seconds,
                        "state": launch_state,
                        "stop_reasons": stop_reasons,
                        "validation_warnings": validation_warnings,
                        "coverage_by_underlying": coverage_by_underlying,
                        "coverage_by_timeframe": coverage_by_timeframe,
                        "unavailable_fraction": unavailable_fraction,
                        **launch_metrics,
                    }
                    missing_underlyers = sum(
                        not row["assessment_count"] for row in coverage_by_underlying
                    )
                    missing_timeframes = sum(
                        not row["available_count"] for row in coverage_by_timeframe
                    )
                    report["realtime_evidence_validations"] = {
                        "PER_UNDERLYING_COVERAGE": {
                            "state": "COLLECTING" if missing_underlyers else "SATISFIED",
                            "missing": missing_underlyers,
                        },
                        "REQUIRED_TIMEFRAME_COVERAGE": {
                            "state": "COLLECTING" if missing_timeframes else "SATISFIED",
                            "missing": missing_timeframes,
                        },
                        "WORKER_MEMORY_OVERHEAD": {
                            "state": "PENDING_REALTIME_EVIDENCE",
                            "reason": "POST_CYCLE_WORKER_MEMORY_MEASUREMENT_REQUIRED",
                        },
                        "ENTRY_WINDOW_SUITABILITY": {
                            "state": (
                                "WARNING" if "NEGATIVE_ENTRY_DEADLINE_SLACK"
                                in validation_warnings else "COLLECTING"
                            ),
                            "reason": (
                                "NEGATIVE_ENTRY_DEADLINE_SLACK"
                                if validation_warnings else
                                "MORE_REALTIME_EVIDENCE_REQUIRED"
                            ),
                        },
                    }
                storage_ready = bool(
                    state["registered"]
                    and state["launch_identity_registered"]
                    and report["runtime_select"]
                    and report["runtime_insert"]
                    and report["launch_id_column_ready"]
                    and report["launch_hash_column_ready"]
                    and report["launch_constraint_ready"]
                    and report["launch_index_ready"]
                    and EXPECTED_TRIGGERS.issubset(triggers)
                )
                report["storage_ready"] = storage_ready
                report["storage_state"] = (
                    "READY_DISABLED" if storage_ready and not report["shadow_writer_enabled"]
                    else "READY_ENABLED" if storage_ready else "INCOMPLETE"
                )
        return report


def main() -> int:
    args = _arguments()
    if args.session_date:
        from options.analytics.behavior_review import build_behavior_review
        from options.config import load_option_runtime_configuration

        if args.write_launch_artifact or args.launch_file:
            raise ValueError("daily review cannot overwrite or reinterpret a launch status artifact")
        now = datetime.now(timezone.utc)
        cutoff = args.as_of or now
        if cutoff.tzinfo is None or cutoff > now:
            raise ValueError("daily review cutoff must be aware and not in the future")
        report = build_behavior_review(load_option_runtime_configuration(), session_date=args.session_date,
            as_of=cutoff, view=args.review_view)
        rendered = json.dumps(report, sort_keys=True, indent=2, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as output:
                output.write(rendered + "\n")
        print(rendered)
        return 0 if not args.require_storage_ready or all(report["schema"].values()) else 2
    if args.as_of:
        raise ValueError("--as-of requires --session-date")
    launch = (
        load_option_stock_behavior_shadow_launch(args.launch_file.resolve())
        if args.launch_file else None
    )
    if args.write_launch_artifact and launch is None:
        raise ValueError("--write-launch-artifact requires --launch-file")
    report = build_report(args.hours, launch)
    configured_path_value = os.getenv("OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE")
    configured_path = (
        (BACKEND_DIR / configured_path_value).resolve()
        if configured_path_value else None
    )
    configured_launch = (
        load_option_stock_behavior_shadow_launch(configured_path)
        if configured_path is not None else None
    )
    report["configured_launch"] = {
        "path": configured_path_value,
        "launch_id": configured_launch.launch_id if configured_launch else None,
        "launch_sha256": configured_launch.sha256 if configured_launch else None,
        "matches_report_launch": bool(
            configured_launch is not None
            and launch is not None
            and configured_launch.sha256 == launch.sha256
        ),
    }
    if launch is not None and report["shadow_writer_enabled"]:
        if not report["configured_launch"]["matches_report_launch"]:
            report["launch"]["state"] = "CONFIGURATION_MISMATCH"
    rendered = json.dumps(report, indent=2, sort_keys=True)
    output = args.output
    if args.write_launch_artifact and launch is not None:
        output = BACKEND_DIR / launch.artifact_destination
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report["storage_ready"] or not args.require_storage_ready else 2


if __name__ == "__main__":
    raise SystemExit(main())