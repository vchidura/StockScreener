#!/usr/bin/env python3
"""Report redacted Advanced V2 component health from durable state."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from uuid import UUID

import psycopg2


UTC = timezone.utc
COMPONENTS = (
    "controller",
    "ingress",
    "bridge",
    "scheduler",
    "finalizer",
    "publication",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", choices=COMPONENTS, required=True)
    parser.add_argument("--cohort-id", type=UUID)
    parser.add_argument("--authorization-id", type=UUID)
    parser.add_argument("--fail-on-degraded", action="store_true")
    args = parser.parse_args(argv)
    if args.component in {"controller", "ingress"}:
        if args.authorization_id is None:
            parser.error(f"{args.component} requires --authorization-id")
    elif args.authorization_id is not None:
        parser.error("--authorization-id is only valid for provider components")
    if args.component in {"finalizer", "publication"}:
        if args.cohort_id is None:
            parser.error(f"{args.component} requires --cohort-id")
    elif args.cohort_id is not None:
        parser.error("--cohort-id is only valid for cohort-local components")
    return args


def _one(cursor, statement, parameters=()):
    cursor.execute(statement, parameters)
    return cursor.fetchone()


def _provider_health(cursor, args):
    if _one(
        cursor,
        "SELECT to_regclass('rt.advanced_ingress_plans')",
    )[0] is None:
        return "BLOCKED", {
            "reason": "ADVANCED_OVERLAY_SCHEMA_NOT_INSTALLED",
        }
    row = _one(
        cursor,
        "SELECT auth.status,plan.plan_id,plan.status,"
        "usage.connection_attempt_count,usage.maximum_connection_attempts,"
        "session.status,session.heartbeat_at,session.event_time_watermark "
        "FROM rt.advanced_stream_authorizations AS auth "
        "LEFT JOIN LATERAL ("
        "SELECT own.plan_id,own.status FROM rt.advanced_ingress_plans AS own "
        "WHERE own.authorization_id=auth.authorization_id "
        "ORDER BY own.session_date DESC,own.created_at DESC LIMIT 1"
        ") AS plan ON true "
        "LEFT JOIN LATERAL ("
        "SELECT own.connection_attempt_count,"
        "plan_row.maximum_connection_attempts,"
        "own.latest_stream_session_id "
        "FROM rt.advanced_ingress_plan_usage AS own "
        "JOIN rt.advanced_ingress_plans AS plan_row USING(plan_id) "
        "WHERE own.plan_id=plan.plan_id"
        ") AS usage ON true "
        "LEFT JOIN rt.stream_sessions AS session "
        "ON session.session_id=usage.latest_stream_session_id "
        "WHERE auth.authorization_id=%s",
        (str(args.authorization_id),),
    )
    if row is None:
        return "BLOCKED", {"reason": "AUTHORIZATION_NOT_FOUND"}
    auth_status, plan_id, plan_status, attempts, ceiling, session_status, heartbeat, watermark = row
    state = (
        "BLOCKED"
        if auth_status != "READY"
        else "IDLE"
        if plan_id is None
        else "BLOCKED"
        if plan_status in {"BLOCKED", "FAILED"}
        else "READY"
    )
    return state, {
        "authorization_status": auth_status,
        "plan_configured": plan_id is not None,
        "plan_status": plan_status,
        "connection_attempts": attempts,
        "connection_ceiling": ceiling,
        "stream_status": session_status,
        "heartbeat_at": heartbeat,
        "event_time_watermark": watermark,
    }


def _bridge_health(cursor):
    if _one(
        cursor,
        "SELECT to_regclass('rt.advanced_journal_registry')",
    )[0] is None:
        return "BLOCKED", {
            "reason": "ADVANCED_OVERLAY_SCHEMA_NOT_INSTALLED",
        }
    journals = _one(
        cursor,
        "SELECT count(*) FILTER (WHERE status='QUARANTINED'),"
        "count(*) FILTER (WHERE status IN ('REGISTERED','WRITING','DRAINING')),"
        "COALESCE(max(normalized_committed_offset-bridge_committed_offset),0) "
        "FROM rt.advanced_journal_registry",
    )
    gaps = _one(
        cursor,
        "SELECT count(*) FROM rt.stream_gaps "
        "WHERE recovery_status NOT IN ('REPAIRED','WAIVED')",
    )[0]
    quarantined, draining, lag = map(int, journals)
    state = "BLOCKED" if quarantined else "DEGRADED" if gaps else "READY"
    return state, {
        "quarantined_journals": quarantined,
        "draining_journals": draining,
        "bridge_offset_lag": lag,
        "unresolved_gaps": int(gaps),
    }


def _scheduler_health(cursor):
    row = _one(
        cursor,
        "SELECT count(*) FILTER (WHERE state IN ('PENDING','RUNNING')),"
        "count(*) FILTER (WHERE state IN ('FAILED','MISSED','RETAINED_GAP')),"
        "max(cutoff) FILTER (WHERE state='COMPLETE') "
        "FROM rt.window_runs WHERE provider_tier='MASSIVE_ADVANCED'",
    )
    pending, failed, latest = row
    state = "DEGRADED" if int(failed) else "READY"
    return state, {
        "open_windows": int(pending),
        "failed_windows": int(failed),
        "latest_complete_cutoff": latest,
    }


def _finalizer_health(cursor, cohort_id):
    row = _one(
        cursor,
        "SELECT count(*) FILTER (WHERE status IN ('PENDING','LEASED')),"
        "count(*) FILTER (WHERE status IN ('FAILED','BLOCKED')),"
        "array_remove(array_agg(DISTINCT failure_code) FILTER ("
        "WHERE status IN ('FAILED','BLOCKED')),NULL) "
        "FROM rt.materialization_jobs WHERE cohort_id=%s "
        "AND job_kind='EQUITY_INTERVAL_FINALIZE'",
        (str(cohort_id),),
    )
    pending, failed = map(int, row[:2])
    return ("DEGRADED" if failed else "READY"), {
        "pending_jobs": pending,
        "failed_jobs": failed,
        "failure_codes": tuple(row[2] or ()),
    }


def _publication_health(cursor, cohort_id):
    releases = int(_one(
        cursor,
        "SELECT count(*) FROM rt.portal_releases WHERE cohort_id=%s",
        (str(cohort_id),),
    )[0])
    cohort_pointers = int(_one(
        cursor,
        "SELECT count(*) FROM rt.portal_release_current WHERE cohort_id=%s",
        (str(cohort_id),),
    )[0])
    platform_pointers = int(_one(
        cursor,
        "SELECT count(*) FROM rt.portal_cohort_current WHERE cohort_id=%s",
        (str(cohort_id),),
    )[0])
    state = (
        "BLOCKED"
        if platform_pointers
        else "DEGRADED"
        if releases or cohort_pointers
        else "READY"
    )
    return state, {
        "portal_releases": releases,
        "cohort_pointers": cohort_pointers,
        "platform_pointers": platform_pointers,
    }


def collect_health(cursor, args):
    if args.component in {"controller", "ingress"}:
        return _provider_health(cursor, args)
    if args.component == "bridge":
        return _bridge_health(cursor)
    if args.component == "scheduler":
        return _scheduler_health(cursor)
    if args.component == "finalizer":
        return _finalizer_health(cursor, args.cohort_id)
    return _publication_health(cursor, args.cohort_id)


def _json_default(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    raise TypeError(f"unsupported health value: {type(value).__name__}")


def main(argv=None) -> int:
    args = parse_args(argv)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("Advanced health requires REALTIME_V2_APP_DSN")
    with psycopg2.connect(dsn) as connection:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='5s'")
            cursor.execute("SET LOCAL ROLE stocks_realtime_v2_app")
            state, detail = collect_health(cursor, args)
    print(json.dumps(
        {
            "checked_at": datetime.now(UTC).isoformat(),
            "component": args.component,
            "state": state,
            "detail": detail,
            "provider_requests": 0,
            "credentials_exposed": False,
        },
        default=_json_default,
        separators=(",", ":"),
    ))
    if state == "BLOCKED" or (
        state == "DEGRADED" and args.fail_on_degraded
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
