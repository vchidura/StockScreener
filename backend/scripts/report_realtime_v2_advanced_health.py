#!/usr/bin/env python3
"""Report redacted Advanced V2 component health from durable state."""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import psycopg2


UTC = timezone.utc
COMPONENTS = (
    "capability-refresh",
    "controller",
    "ingress",
    "bridge",
    "scheduler",
    "finalizer",
    "detectors",
    "publication",
)
ACTIVE_STREAM_STATUSES = frozenset({
    "CONNECTING",
    "AUTHENTICATING",
    "SUBSCRIBING",
    "WARMING",
    "LIVE",
    "BACKOFF",
})


@dataclass(frozen=True, slots=True)
class ContinuousHealthPlan:
    plan_id: UUID
    authorization_id: UUID
    cohort_id: UUID
    session_date: object


@dataclass(frozen=True, slots=True)
class ContinuousHealthScope:
    acquisition: ContinuousHealthPlan | None
    processing: ContinuousHealthPlan | None
    future: ContinuousHealthPlan | None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", choices=COMPONENTS, required=True)
    parser.add_argument(
        "--runtime-mode",
        choices=("continuous", "legacy-diagnostic"),
        default="legacy-diagnostic",
    )
    parser.add_argument("--cohort-id", type=UUID)
    parser.add_argument("--authorization-id", type=UUID)
    parser.add_argument("--fail-on-degraded", action="store_true")
    args = parser.parse_args(argv)
    if args.runtime_mode == "continuous":
        if args.authorization_id is not None or args.cohort_id is not None:
            parser.error(
                "continuous health derives authorization and cohort"
            )
        return args
    if args.component in {"controller", "ingress"}:
        if args.authorization_id is None:
            parser.error(f"{args.component} requires --authorization-id")
    elif args.authorization_id is not None:
        parser.error("--authorization-id is only valid for provider components")
    if args.component in {
        "bridge",
        "finalizer",
        "detectors",
        "publication",
    }:
        if args.cohort_id is None:
            parser.error(f"{args.component} requires --cohort-id")
    elif args.cohort_id is not None:
        parser.error("--cohort-id is only valid for cohort-local components")
    return args


def _one(cursor, statement, parameters=()):
    cursor.execute(statement, parameters)
    return cursor.fetchone()


def _health_plan(row):
    if row is None:
        return None
    return ContinuousHealthPlan(
        plan_id=UUID(str(row[0])),
        authorization_id=UUID(str(row[1])),
        cohort_id=UUID(str(row[2])),
        session_date=row[3],
    )


def _continuous_health_scope(
    cursor,
    *,
    checked_at: datetime,
) -> ContinuousHealthScope:
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("health checked_at must be timezone-aware")
    checked = checked_at.astimezone(UTC)
    select = (
        "SELECT plan_id::text,authorization_id::text,cohort_id::text,"
        "session_date FROM rt.advanced_ingress_plans "
    )
    cursor.execute(
        select
        + "WHERE authorized_start_at<=%s AND authorized_end_at>%s "
        "ORDER BY session_date DESC,created_at DESC LIMIT 1",
        (checked, checked),
    )
    acquisition = _health_plan(cursor.fetchone())
    cursor.execute(
        select
        + "WHERE authorized_end_at<=%s "
        "AND COALESCE(("
        "SELECT MAX(run.deadline) FROM rt.window_runs AS run "
        "JOIN rt.service_plans AS service USING(plan_id) "
        "WHERE service.cohort_id="
        "rt.advanced_ingress_plans.cohort_id "
        "AND service.session_date="
        "rt.advanced_ingress_plans.session_date"
        "),authorized_end_at)>%s "
        "ORDER BY authorized_end_at DESC,created_at DESC LIMIT 1",
        (checked, checked),
    )
    processing = _health_plan(cursor.fetchone())
    cursor.execute(
        select
        + "WHERE authorized_start_at>%s "
        "ORDER BY authorized_start_at,created_at DESC LIMIT 1",
        (checked,),
    )
    future = _health_plan(cursor.fetchone())
    return ContinuousHealthScope(acquisition, processing, future)


def _capability_refresh_health(cursor, *, checked_at: datetime):
    cursor.execute(
        "SELECT owner_id::text,owner_epoch,lease_until,updated_at "
        "FROM rt.worker_leases WHERE resource_key="
        "'advanced-equity:capability-refresh'"
    )
    lease = cursor.fetchone()
    cursor.execute(
        "SELECT status,reasons,checked_at,source_watermark "
        "FROM rt.readiness_current "
        "WHERE scope_type='RESIDENT' "
        "AND scope_id='ADVANCED_EQUITY_CAPABILITY_REFRESH' "
        "AND capability='CAPABILITY_REFRESH'"
    )
    readiness = cursor.fetchone()
    active = (
        lease is not None
        and lease[2] > checked_at.astimezone(UTC)
    )
    status = None if readiness is None else str(readiness[0])
    watermark = {} if readiness is None else dict(readiness[3])
    resident_state = watermark.get("state") or status
    state = (
        "READY"
        if active and resident_state == "READY"
        else "WAITING"
        if active and resident_state == "WAITING"
        else "DEGRADED"
    )
    return state, {
        "owner_id": None if lease is None else str(lease[0]),
        "owner_epoch": None if lease is None else int(lease[1]),
        "lease_until": None if lease is None else lease[2],
        "heartbeat_at": (
            None if lease is None else lease[3]
        ),
        "status": resident_state,
        "readiness_status": status,
        "reasons": (
            ()
            if readiness is None
            else tuple(str(reason) for reason in readiness[1])
        ),
        "last_checked_at": (
            None if readiness is None else readiness[2]
        ),
        "last_success_at": watermark.get("last_success_at"),
        "next_retry_at": watermark.get("next_retry_at"),
        "request_identity_sha256": watermark.get(
            "request_identity_sha256"
        ),
    }


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
        "SELECT auth.status,plan.plan_id,plan.status,plan.terminal_outcome,"
        "usage.connection_attempt_count,usage.maximum_connection_attempts,"
        "session.status,session.failure_code,session.heartbeat_at,"
        "session.event_time_watermark,session.ended_at,"
        "registry.status,registry.quarantine_reason,registry.relative_path "
        "FROM rt.advanced_stream_authorizations AS auth "
        "LEFT JOIN LATERAL ("
        "SELECT own.plan_id,own.status,own.terminal_outcome "
        "FROM rt.advanced_ingress_plans AS own "
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
        "LEFT JOIN rt.advanced_journal_registry AS registry "
        "ON registry.journal_generation=session.journal_generation "
        "WHERE auth.authorization_id=%s",
        (str(args.authorization_id),),
    )
    if row is None:
        return "BLOCKED", {"reason": "AUTHORIZATION_NOT_FOUND"}
    (
        auth_status,
        plan_id,
        plan_status,
        terminal_outcome,
        attempts,
        ceiling,
        session_status,
        session_failure,
        heartbeat,
        watermark,
        session_ended_at,
        journal_status,
        quarantine_reason,
        journal_relative_path,
    ) = row
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
        "terminal_outcome": terminal_outcome,
        "connection_attempts": attempts,
        "connection_ceiling": ceiling,
        "stream_status": session_status,
        "stream_failure_code": session_failure,
        "heartbeat_at": heartbeat,
        "event_time_watermark": watermark,
        "stream_ended_at": session_ended_at,
        "journal_status": journal_status,
        "journal_quarantine_reason": quarantine_reason,
        "journal_relative_path": journal_relative_path,
    }


def _provider_plan_health(
    cursor,
    plan_id: UUID,
    *,
    checked_at: datetime,
    scope: ContinuousHealthScope,
):
    row = _one(
        cursor,
        "SELECT auth.status,plan.status,plan.terminal_outcome,"
        "usage.connection_attempt_count,"
        "plan.maximum_connection_attempts,session.status,"
        "session.failure_code,session.heartbeat_at,"
        "session.event_time_watermark,session.ended_at,"
        "registry.status,registry.quarantine_reason,"
        "registry.relative_path,plan.session_date "
        "FROM rt.advanced_ingress_plans AS plan "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "LEFT JOIN rt.advanced_ingress_plan_usage AS usage "
        "USING(plan_id) "
        "LEFT JOIN rt.stream_sessions AS session "
        "ON session.session_id=usage.latest_stream_session_id "
        "LEFT JOIN rt.advanced_journal_registry AS registry "
        "ON registry.journal_generation=session.journal_generation "
        "WHERE plan.plan_id=%s",
        (str(plan_id),),
    )
    if row is None:
        return "DEGRADED", {"reason": "CURRENT_PLAN_NOT_FOUND"}
    (
        auth_status,
        plan_status,
        terminal_outcome,
        attempts,
        ceiling,
        session_status,
        session_failure,
        heartbeat,
        watermark,
        session_ended_at,
        journal_status,
        quarantine_reason,
        journal_relative_path,
        session_date,
    ) = row
    stale_seconds = int(
        os.getenv("REALTIME_V2_ADVANCED_HEALTH_STALE_SECONDS") or 60
    )
    stale = (
        session_status in ACTIVE_STREAM_STATUSES
        and (
            heartbeat is None
            or heartbeat
            <= checked_at.astimezone(UTC)
            - timedelta(seconds=stale_seconds)
        )
    )
    if plan_status in {"BLOCKED", "FAILED"} or quarantine_reason:
        state = "DEGRADED"
    elif terminal_outcome is not None or plan_status == "COMPLETE":
        state = "TERMINAL"
    elif stale:
        state = "STALE"
    elif session_status in ACTIVE_STREAM_STATUSES:
        state = "READY"
    else:
        state = "DEGRADED"
    return state, {
        "reason": (
            "STREAM_HEARTBEAT_STALE"
            if stale
            else "STREAM_NOT_ACTIVE"
            if state == "DEGRADED" and session_status is None
            else None
        ),
        "acquisition_session": session_date,
        "processing_session": (
            None
            if scope.processing is None
            else scope.processing.session_date
        ),
        "future_prepared_session": (
            None if scope.future is None else scope.future.session_date
        ),
        "authorization_status": auth_status,
        "plan_status": plan_status,
        "terminal_outcome": terminal_outcome,
        "connection_attempts": attempts,
        "connection_ceiling": ceiling,
        "stream_status": session_status,
        "stream_failure_code": session_failure,
        "heartbeat_at": heartbeat,
        "event_time_watermark": watermark,
        "stream_ended_at": session_ended_at,
        "journal_status": journal_status,
        "journal_quarantine_reason": quarantine_reason,
        "journal_relative_path": journal_relative_path,
    }


def _bridge_health(cursor, cohort_id=None):
    if _one(
        cursor,
        "SELECT to_regclass('rt.advanced_journal_registry')",
    )[0] is None:
        return "BLOCKED", {
            "reason": "ADVANCED_OVERLAY_SCHEMA_NOT_INSTALLED",
        }
    has_recovery_overlay = bool(_one(
        cursor,
        "SELECT to_regclass('rt.advanced_journal_recovery_events') "
        "IS NOT NULL",
    )[0])
    cohort_clause = (
        ""
        if cohort_id is None
        else " WHERE registry.cohort_id=%s"
    )
    parameters = () if cohort_id is None else (str(cohort_id),)
    journals = _one(
        cursor,
        "SELECT count(*) FILTER (WHERE registry.status='QUARANTINED'"
        + (
            " AND NOT EXISTS (SELECT 1 FROM "
            "rt.advanced_journal_recovery_events AS recovery "
            "WHERE recovery.journal_generation="
            "registry.journal_generation "
            "AND recovery.event_type='RECOVERED')"
            if has_recovery_overlay
            else ""
        )
        + "),count(*) FILTER (WHERE registry.status IN "
        "('REGISTERED','WRITING','DRAINING')),"
        "COALESCE(max(registry.normalized_committed_offset-"
        "registry.bridge_committed_offset),0) "
        "FROM rt.advanced_journal_registry AS registry"
        + cohort_clause,
        parameters,
    )
    if cohort_id is None:
        gaps = _one(
            cursor,
            "SELECT count(*) FROM rt.stream_gaps "
            "WHERE recovery_status NOT IN ('REPAIRED','WAIVED')",
        )[0]
    else:
        gaps = _one(
            cursor,
            "SELECT count(*) FROM rt.stream_gaps AS gap "
            "JOIN rt.stream_sessions AS session USING(session_id) "
            "JOIN rt.advanced_journal_registry AS registry "
            "ON registry.journal_generation=session.journal_generation "
            "WHERE gap.recovery_status NOT IN ('REPAIRED','WAIVED') "
            "AND registry.cohort_id=%s",
            (str(cohort_id),),
        )[0]
    quarantined, draining, lag = map(int, journals)
    state = "BLOCKED" if quarantined else "DEGRADED" if gaps else "READY"
    return state, {
        "quarantined_journals": quarantined,
        "draining_journals": draining,
        "bridge_offset_lag": lag,
        "unresolved_gaps": int(gaps),
    }


def _scheduler_health(cursor, *, cohort_id=None, session_date=None):
    scope = (
        ""
        if cohort_id is None or session_date is None
        else (
            " AND cohort_id=%s AND EXISTS ("
            "SELECT 1 FROM rt.service_plans AS service "
            "WHERE service.plan_id=rt.window_runs.plan_id "
            "AND service.cohort_id=%s AND service.session_date=%s)"
        )
    )
    parameters = (
        ()
        if not scope
        else (str(cohort_id), str(cohort_id), session_date)
    )
    row = _one(
        cursor,
        "SELECT count(*) FILTER (WHERE state IN ('PENDING','RUNNING')),"
        "count(*) FILTER (WHERE state IN ('FAILED','MISSED','RETAINED_GAP')),"
        "max(cutoff) FILTER (WHERE state='COMPLETE') "
        "FROM rt.window_runs WHERE provider_tier='MASSIVE_ADVANCED'"
        + scope,
        parameters,
    )
    pending, failed, latest = row
    state = "DEGRADED" if int(failed) else "READY"
    return state, {
        "open_windows": int(pending),
        "failed_windows": int(failed),
        "latest_complete_cutoff": latest,
        "historical_failures_excluded": bool(scope),
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


def _detector_health(cursor, cohort_id):
    expected = int(_one(
        cursor,
        "SELECT count(DISTINCT member.instrument_id) * 2 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_members AS member USING(manifest_id) "
        "WHERE cohort.cohort_id=%s AND member.channel='AM'",
        (str(cohort_id),),
    )[0])
    active_journals = int(_one(
        cursor,
        "SELECT count(*) FROM rt.advanced_journal_registry "
        "WHERE cohort_id=%s "
        "AND status IN ('WRITING','DRAINING','DRAINED')",
        (str(cohort_id),),
    )[0])
    active_leases = int(_one(
        cursor,
        "SELECT count(*) FROM rt.worker_leases "
        "WHERE resource_key LIKE %s AND lease_until>%s",
        (
            f"advanced-equity-detector:{cohort_id}:%",
            datetime.now(UTC),
        ),
    )[0])
    checkpoints = int(_one(
        cursor,
        "SELECT count(*) FROM rt.consumer_checkpoints "
        "WHERE cohort_id=%s AND consumer_name LIKE "
        "'advanced-equity-detector-shadow:ADVANCED_EQUITY_S_:0'",
        (str(cohort_id),),
    )[0])
    required_leases = expected * active_journals
    state = (
        "DEGRADED"
        if required_leases and active_leases != required_leases
        else "READY"
    )
    return state, {
        "expected_consumers_per_journal": expected,
        "active_journals": active_journals,
        "required_leases": required_leases,
        "active_leases": active_leases,
        "durable_checkpoints": checkpoints,
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


def collect_health(cursor, args, *, checked_at=None):
    if args.runtime_mode == "continuous":
        checked = (
            datetime.now(UTC)
            if checked_at is None
            else checked_at
        )
        scope = _continuous_health_scope(
            cursor,
            checked_at=checked,
        )
        refresh_state, refresh_detail = _capability_refresh_health(
            cursor,
            checked_at=checked,
        )
        if args.component == "capability-refresh":
            return refresh_state, refresh_detail

        def with_refresh(result):
            state, detail = result
            detail = dict(detail)
            detail["capability_refresh"] = refresh_detail
            return state, detail

        if args.component in {"controller", "ingress"}:
            if scope.acquisition is not None:
                return with_refresh(_provider_plan_health(
                    cursor,
                    scope.acquisition.plan_id,
                    checked_at=checked,
                    scope=scope,
                ))
            if scope.processing is not None:
                return with_refresh(("IDLE", {
                    "reason": "PROCESSING_DRAIN",
                    "processing_session": (
                        scope.processing.session_date
                    ),
                    "future_prepared_session": (
                        None
                        if scope.future is None
                        else scope.future.session_date
                    ),
                }))
            if scope.future is not None:
                return with_refresh(("IDLE", {
                    "reason": "FUTURE_PREPARED",
                    "future_prepared_session": (
                        scope.future.session_date
                    ),
                }))
            return with_refresh(("IDLE", {
                "reason": "NO_CURRENT_PLAN",
            }))
        selected = (
            scope.acquisition
            or scope.processing
            or scope.future
        )
        if selected is None and args.component != "scheduler":
            return with_refresh((
                "IDLE",
                {"reason": "NO_CURRENT_PLAN"},
            ))
        if selected is not None:
            args.cohort_id = selected.cohort_id
    else:
        with_refresh = lambda result: result
    if args.component in {"controller", "ingress"}:
        return with_refresh(_provider_health(cursor, args))
    if args.component == "bridge":
        return with_refresh(_bridge_health(cursor, args.cohort_id))
    if args.component == "scheduler":
        return with_refresh(_scheduler_health(
            cursor,
            cohort_id=(
                None if not args.runtime_mode == "continuous"
                or selected is None else selected.cohort_id
            ),
            session_date=(
                None if not args.runtime_mode == "continuous"
                or selected is None else selected.session_date
            ),
        ))
    if args.component == "finalizer":
        return with_refresh(_finalizer_health(cursor, args.cohort_id))
    if args.component == "detectors":
        return with_refresh(_detector_health(cursor, args.cohort_id))
    return with_refresh(_publication_health(cursor, args.cohort_id))


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
            state, detail = collect_health(
                cursor,
                args,
                checked_at=datetime.now(UTC),
            )
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
    if state in {"BLOCKED", "STALE"} or (
        state == "DEGRADED" and args.fail_on_degraded
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
