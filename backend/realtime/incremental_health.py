"""Read-only V2 incremental pipeline health aggregation."""
from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("health clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _rows(cursor, columns):
    return tuple(
        dict(row) if isinstance(row, dict) else dict(zip(columns, row))
        for row in cursor.fetchall()
    )


def collect_incremental_health_cursor(
    cursor,
    *,
    cohort_id: UUID,
    account_alias: str,
    session_date: date,
    checked_at: datetime,
) -> dict:
    if not isinstance(cohort_id, UUID):
        raise TypeError("cohort_id must be a UUID")
    if not isinstance(session_date, date):
        raise TypeError("session_date must be a date")
    account = account_alias.strip()
    if not account:
        raise ValueError("account_alias cannot be blank")
    checked = _utc(checked_at)

    cursor.execute(
        "SELECT COUNT(*) FILTER (WHERE budget_scope_sha256 IS NOT NULL), "
        "COUNT(*) FILTER (WHERE budget_scope_sha256 IS NULL), "
        "COALESCE(SUM(reserved_response_bytes) FILTER "
        "(WHERE budget_scope_sha256 IS NOT NULL),0), "
        "MAX(account_request_ceiling) FILTER "
        "(WHERE budget_scope_sha256 IS NOT NULL), "
        "MAX(account_decoded_byte_ceiling) FILTER "
        "(WHERE budget_scope_sha256 IS NOT NULL), "
        "COUNT(*) FILTER (WHERE state='RESERVED'), "
        "COUNT(*) FILTER (WHERE state='COMPLETE'), "
        "COUNT(*) FILTER (WHERE state='FAILED'), "
        "COUNT(*) FILTER (WHERE state='AMBIGUOUS') "
        "FROM rt.provider_request_ledger "
        "WHERE account_alias=%s AND (budget_session_date=%s "
        "OR (budget_session_date IS NULL "
        "AND (reserved_at AT TIME ZONE 'UTC')::date=%s))",
        (account, session_date, session_date),
    )
    budget_row = cursor.fetchone()
    provider_budget = {
        "coordinated_requests": int(budget_row[0]),
        "legacy_uncoordinated_requests": int(budget_row[1]),
        "reserved_decoded_bytes": int(budget_row[2]),
        "request_ceiling": budget_row[3],
        "decoded_byte_ceiling": budget_row[4],
        "reserved": int(budget_row[5]),
        "complete": int(budget_row[6]),
        "failed": int(budget_row[7]),
        "ambiguous": int(budget_row[8]),
    }

    cursor.execute(
        "SELECT interval, source_kind, terminal_state, COUNT(*) AS members, "
        "MIN(last_complete_bar_end) AS earliest_complete, "
        "MAX(last_complete_bar_end) AS latest_complete, "
        "MAX(last_examined_bar_end) AS latest_examined, "
        "MAX(updated_at) AS updated_at "
        "FROM rt.acquisition_watermarks WHERE cohort_id=%s "
        "GROUP BY interval,source_kind,terminal_state "
        "ORDER BY interval,source_kind,terminal_state",
        (str(cohort_id),),
    )
    watermarks = _rows(cursor, (
        "interval",
        "source_kind",
        "terminal_state",
        "members",
        "earliest_complete",
        "latest_complete",
        "latest_examined",
        "updated_at",
    ))

    cursor.execute(
        "SELECT job_kind, status, COUNT(*) AS jobs, "
        "MIN(available_at) AS earliest_available, "
        "MAX(updated_at) AS latest_update "
        "FROM rt.materialization_jobs WHERE cohort_id=%s "
        "GROUP BY job_kind,status ORDER BY job_kind,status",
        (str(cohort_id),),
    )
    jobs = _rows(cursor, (
        "job_kind",
        "status",
        "jobs",
        "earliest_available",
        "latest_update",
    ))

    cursor.execute(
        "SELECT barrier_kind,state,COUNT(*) AS barriers, "
        "MIN(created_at) AS earliest_created, "
        "MAX(COALESCE(completed_at,created_at)) AS latest_activity "
        "FROM rt.job_barriers WHERE cohort_id=%s "
        "GROUP BY barrier_kind,state ORDER BY barrier_kind,state",
        (str(cohort_id),),
    )
    barriers = _rows(cursor, (
        "barrier_kind",
        "state",
        "barriers",
        "earliest_created",
        "latest_activity",
    ))

    cursor.execute(
        "SELECT window_run.state,COUNT(*) AS windows,"
        "MAX(window_run.cutoff) AS latest_cutoff "
        "FROM rt.window_runs AS window_run "
        "JOIN rt.service_plans AS plan USING(plan_id) "
        "WHERE window_run.cohort_id=%s AND plan.session_date=%s "
        "GROUP BY window_run.state ORDER BY window_run.state",
        (str(cohort_id), session_date),
    )
    windows = _rows(cursor, ("state", "windows", "latest_cutoff"))

    cursor.execute(
        "SELECT stage.stage_id,stage.state,COUNT(*) AS runs, "
        "MAX(window_run.cutoff) AS latest_cutoff "
        "FROM rt.stage_runs AS stage "
        "JOIN rt.window_runs AS window_run USING(window_id) "
        "JOIN rt.service_plans AS plan ON plan.plan_id=window_run.plan_id "
        "WHERE window_run.cohort_id=%s AND plan.session_date=%s "
        "GROUP BY stage.stage_id,stage.state "
        "ORDER BY stage.stage_id,stage.state",
        (str(cohort_id), session_date),
    )
    stages = _rows(cursor, ("stage_id", "state", "runs", "latest_cutoff"))

    cursor.execute(
        "SELECT pointer.cutoff,pointer.session_date,pointer.updated_at,"
        "pointer.release_id FROM rt.portal_release_current AS pointer "
        "WHERE pointer.cohort_id=%s",
        (str(cohort_id),),
    )
    release = cursor.fetchone()
    served_release = None if release is None else {
        "cutoff": release[0],
        "session_date": release[1],
        "updated_at": release[2],
        "release_id": str(release[3]),
    }

    cursor.execute(
        "SELECT generation.generated_at,generation.session_date,"
        "generation.actual_members,generation.expected_members,generation.status "
        "FROM rt.screening_current AS current "
        "JOIN rt.screening_generations AS generation "
        "ON generation.generation_id=current.generation_id "
        "WHERE current.cohort_id=%s",
        (str(cohort_id),),
    )
    screening = cursor.fetchone()
    screening_current = None if screening is None else {
        "generated_at": screening[0],
        "session_date": screening[1],
        "actual_members": int(screening[2]),
        "expected_members": int(screening[3]),
        "status": screening[4],
    }

    reason_codes = []
    gap_members = sum(
        int(row["members"])
        for row in watermarks
        if row["terminal_state"] == "GAP"
    )
    pending_jobs = sum(
        int(row["jobs"])
        for row in jobs
        if row["status"] in {"PENDING", "LEASED", "BLOCKED", "FAILED"}
    )
    open_barriers = sum(
        int(row["barriers"])
        for row in barriers
        if row["state"] != "COMPLETE"
    )
    if provider_budget["legacy_uncoordinated_requests"]:
        reason_codes.append("LEGACY_UNCOORDINATED_PROVIDER_REQUESTS")
    if provider_budget["reserved"] or provider_budget["ambiguous"]:
        reason_codes.append("UNRESOLVED_PROVIDER_REQUESTS")
    if gap_members:
        reason_codes.append("ACQUISITION_GAPS")
    if pending_jobs:
        reason_codes.append("MATERIALIZATION_BACKLOG")
    if open_barriers:
        reason_codes.append("OPEN_OR_FAILED_BARRIERS")
    if served_release is None:
        reason_codes.append("PORTAL_RELEASE_UNAVAILABLE")

    return {
        "status": "OBSERVED" if not reason_codes else "DEGRADED",
        "reason_codes": reason_codes,
        "checked_at": checked,
        "cohort_id": str(cohort_id),
        "session_date": session_date,
        "provider_budget": provider_budget,
        "watermarks": watermarks,
        "jobs": jobs,
        "barriers": barriers,
        "windows": windows,
        "stages": stages,
        "screening_current": screening_current,
        "served_release": served_release,
        "provider_requests": 0,
        "database_writes": 0,
    }
