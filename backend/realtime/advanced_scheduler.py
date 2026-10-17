"""Durable Advanced window scheduling, retries and daily lifecycle."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256
from .gap_repair_plan import plan_gap_repair_cursor
from .orchestration_repository import CycleReceiptSpec


ADVANCED_WINDOW_BUILD_ID = "advanced-equity-window-v1"
@dataclass(frozen=True, slots=True)
class AdvancedSchedulerSession:
    scheduler_session_id: UUID
    authorization_id: UUID
    ingress_plan_id: UUID | None
    cohort_id: UUID | None
    session_date: date
    first_authorized_cutoff: datetime
    session_close_at: datetime
    drain_deadline: datetime
    status: str


@dataclass(frozen=True, slots=True)
class AdvancedWindowRecovery:
    complete: bool
    active_window_id: UUID | None
    latest_state: str | None
    next_attempt: int
    retry_not_before: datetime
    input_receipt: CycleReceiptSpec | None
    portal_receipt: CycleReceiptSpec | None


@dataclass(frozen=True, slots=True)
class AdvancedWindowRetry:
    retry_id: UUID
    scheduler_session_id: UUID
    cutoff: datetime
    attempt: int
    window_key: str
    window_id: UUID
    retry_not_before: datetime
    report_relative_path: str
    reused_input_receipt: bool
    reused_portal_receipt: bool


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def ensure_advanced_scheduler_session_cursor(
    cursor,
    *,
    authorization_id: UUID,
    ingress_plan_id: UUID | None,
    cohort_id: UUID | None,
    session_date: date,
    first_authorized_cutoff: datetime,
    session_close_at: datetime,
    drain_deadline: datetime,
    checked_at: datetime,
) -> AdvancedSchedulerSession:
    first = _utc(first_authorized_cutoff, "first_authorized_cutoff")
    closed = _utc(session_close_at, "session_close_at")
    drain = _utc(drain_deadline, "drain_deadline")
    checked = _utc(checked_at, "checked_at")
    identity = {
        "contract": "advanced-scheduler-session-v1",
        "authorization_id": authorization_id,
        "session_date": session_date,
        "first_authorized_cutoff": first,
        "session_close_at": closed,
        "drain_deadline": drain,
    }
    scheduler_session_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-scheduler:{canonical_sha256(identity)}",
    )
    cursor.execute(
        "INSERT INTO rt.advanced_scheduler_sessions ("
        "scheduler_session_id,authorization_id,ingress_plan_id,cohort_id,"
        "session_date,first_authorized_cutoff,session_close_at,"
        "drain_deadline,status,reason_code,next_action,updated_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',"
        "'WINDOW_DISCOVERY','DISCOVER_DUE_WINDOWS',%s) "
        "ON CONFLICT (scheduler_session_id) DO UPDATE SET "
        "ingress_plan_id=COALESCE("
        "rt.advanced_scheduler_sessions.ingress_plan_id,"
        "EXCLUDED.ingress_plan_id),"
        "cohort_id=COALESCE("
        "rt.advanced_scheduler_sessions.cohort_id,EXCLUDED.cohort_id),"
        "updated_at=EXCLUDED.updated_at "
        "RETURNING scheduler_session_id,authorization_id,ingress_plan_id,"
        "cohort_id,session_date,first_authorized_cutoff,session_close_at,"
        "drain_deadline,status",
        (
            str(scheduler_session_id),
            str(authorization_id),
            str(ingress_plan_id) if ingress_plan_id else None,
            str(cohort_id) if cohort_id else None,
            session_date,
            first,
            closed,
            drain,
            checked,
        ),
    )
    row = cursor.fetchone()
    return AdvancedSchedulerSession(
        UUID(str(row[0])),
        UUID(str(row[1])),
        UUID(str(row[2])) if row[2] else None,
        UUID(str(row[3])) if row[3] else None,
        row[4],
        row[5],
        row[6],
        row[7],
        str(row[8]),
    )


def _receipt(row, kind: str) -> CycleReceiptSpec:
    return CycleReceiptSpec(
        kind,
        row[1],
        int(row[2]),
        str(row[3]),
        str(row[4]),
        str(row[5]),
    )


def discover_advanced_window_recovery_cursor(
    cursor,
    *,
    cohort_id: UUID,
    cutoff: datetime,
    checked_at: datetime,
    retry_initial_backoff_seconds: int = 30,
    retry_max_backoff_seconds: int = 300,
) -> AdvancedWindowRecovery:
    cutoff_utc = _utc(cutoff, "cutoff")
    now = _utc(checked_at, "checked_at")
    cursor.execute(
        "SELECT run.window_id,run.window_key,run.state,"
        "run.lease_until,run.updated_at,"
        "receipt.receipt_kind,receipt.cutoff,"
        "receipt.reference_generation,receipt.universe_sha256,"
        "receipt.input_sha256,receipt.build_id,receipt.cohort_id,"
        "manifest.universe_sha256,service.stage_catalog_version,"
        "service.cohort_id "
        "FROM rt.window_runs AS run "
        "JOIN rt.cohorts AS cohort ON cohort.cohort_id=run.cohort_id "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.service_plans AS service ON service.plan_id=run.plan_id "
        "LEFT JOIN rt.cycle_receipts AS receipt "
        "ON receipt.window_id=run.window_id "
        "AND receipt.receipt_kind IN ('INPUT_READY','PORTAL_READY') "
        "WHERE run.cohort_id=%s AND run.provider_tier='MASSIVE_ADVANCED' "
        "AND run.cutoff=%s ORDER BY run.updated_at,run.window_key,"
        "receipt.receipt_kind",
        (str(cohort_id), cutoff_utc),
    )
    rows = tuple(cursor.fetchall())
    if not rows:
        return AdvancedWindowRecovery(
            False,
            None,
            None,
            0,
            now,
            None,
            None,
        )
    windows = {}
    receipts: dict[str, set[CycleReceiptSpec]] = {
        "INPUT_READY": set(),
        "PORTAL_READY": set(),
    }
    for row in rows:
        windows[str(row[0])] = row[:5]
        if row[5] is None:
            continue
        spec = CycleReceiptSpec(
            str(row[5]),
            row[6],
            int(row[7]),
            str(row[8]),
            str(row[9]),
            str(row[10]),
        )
        if (
            spec.cutoff != cutoff_utc
            or spec.build_id != ADVANCED_WINDOW_BUILD_ID
            or str(row[11]) != str(cohort_id)
            or str(row[8]) != str(row[12])
            or row[13] != "stages_v3"
            or str(row[14]) != str(cohort_id)
        ):
            raise ValueError(
                "durable Advanced receipt does not match recovery scope"
            )
        receipts[spec.receipt_kind].add(spec)
    if any(len(values) > 1 for values in receipts.values()):
        raise ValueError("durable Advanced receipts disagree")
    own = tuple(windows.values())
    complete = any(row[2] == "COMPLETE" for row in own)
    latest = max(own, key=lambda row: (row[4], row[1]))
    active_window_id = (
        UUID(str(latest[0]))
        if latest[2] in {"PENDING", "RUNNING"}
        and (latest[2] != "RUNNING" or latest[3] is None or latest[3] <= now)
        else None
    )
    attempts = []
    for row in own:
        marker = "-retry"
        if marker in row[1]:
            suffix = row[1].rsplit(marker, 1)[1]
            if suffix.isdigit():
                attempts.append(int(suffix))
        else:
            attempts.append(0)
    latest_attempt = max(attempts, default=-1)
    next_attempt = (
        latest_attempt
        if active_window_id is not None
        else latest_attempt + 1
    )
    retry_delay = min(
        retry_max_backoff_seconds,
        retry_initial_backoff_seconds
        * (2 ** max(0, next_attempt - 1)),
    )
    retry_not_before = (
        latest[4] + timedelta(seconds=retry_delay)
        if latest[2] in {"FAILED", "MISSED", "RETAINED_GAP"}
        else now
    )
    return AdvancedWindowRecovery(
        complete,
        active_window_id,
        str(latest[2]),
        next_attempt,
        retry_not_before,
        next(iter(receipts["INPUT_READY"]), None),
        next(iter(receipts["PORTAL_READY"]), None),
    )


def ensure_advanced_window_retry_cursor(
    cursor,
    scheduler_session_id: UUID,
    *,
    cohort_id: UUID,
    cutoff: datetime,
    attempt: int,
    retry_not_before: datetime,
    input_receipt: CycleReceiptSpec | None,
    portal_receipt: CycleReceiptSpec | None,
) -> AdvancedWindowRetry:
    cutoff_utc = _utc(cutoff, "cutoff")
    available = _utc(retry_not_before, "retry_not_before")
    if type(attempt) is not int or attempt < 0:
        raise ValueError("window retry attempt must be nonnegative")
    suffix = "" if attempt == 0 else f"-retry{attempt}"
    window_key = f"advanced-{cutoff_utc:%Y%m%dT%H%M%SZ}{suffix}"
    window_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:window:{cohort_id}:{window_key}",
    )
    retry_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-window-retry:{scheduler_session_id}:"
        f"{cutoff_utc.isoformat()}:{attempt}",
    )
    report_relative_path = (
        f"advanced-window-{cutoff_utc:%Y%m%dT%H%M%SZ}"
        f"-retry{attempt}.json"
    )
    cursor.execute(
        "INSERT INTO rt.advanced_window_retries ("
        "retry_id,scheduler_session_id,cutoff,attempt,window_key,window_id,"
        "retry_not_before,status,reason_code,reused_input_receipt,"
        "reused_portal_receipt,report_relative_path) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,'PLANNED','DURABLE_WINDOW_DUE',%s,%s,%s) "
        "ON CONFLICT (retry_id) DO NOTHING",
        (
            str(retry_id),
            str(scheduler_session_id),
            cutoff_utc,
            attempt,
            window_key,
            str(window_id),
            available,
            input_receipt is not None,
            portal_receipt is not None,
            report_relative_path,
        ),
    )
    return AdvancedWindowRetry(
        retry_id,
        scheduler_session_id,
        cutoff_utc,
        attempt,
        window_key,
        window_id,
        available,
        report_relative_path,
        input_receipt is not None,
        portal_receipt is not None,
    )


def load_advanced_scheduler_scope_cursor(
    cursor,
    *,
    authorization_id: UUID,
    session_date: date,
):
    cursor.execute(
        "SELECT plan.plan_id,plan.cohort_id,plan.universe_target,"
        "plan.runtime_profile_sha256,plan.status "
        "FROM rt.advanced_ingress_plans AS plan "
        "WHERE plan.authorization_id=%s AND plan.session_date=%s "
        "ORDER BY plan.created_at,plan.plan_id",
        (str(authorization_id), session_date),
    )
    rows = tuple(cursor.fetchall())
    eligible = tuple(
        row for row in rows if row[4] in {"READY", "ACTIVE", "COMPLETE"}
    )
    if len(eligible) > 1:
        raise ValueError("daily scheduler scope is ambiguous")
    if not eligible:
        return None
    row = eligible[0]
    return {
        "plan_id": UUID(str(row[0])),
        "cohort_id": UUID(str(row[1])),
        "universe_target": int(row[2]),
        "runtime_profile_sha256": str(row[3]),
        "status": str(row[4]),
    }


def set_advanced_window_retry_status_cursor(
    cursor,
    retry_id: UUID,
    *,
    status: str,
    reason_code: str,
    checked_at: datetime,
) -> None:
    terminal = status in {"COMPLETE", "FAILED", "BLOCKED", "SKIPPED"}
    cursor.execute(
        "UPDATE rt.advanced_window_retries SET status=%s,reason_code=%s,"
        "updated_at=%s,completed_at=CASE WHEN %s THEN %s "
        "ELSE completed_at END WHERE retry_id=%s RETURNING retry_id",
        (
            status,
            reason_code,
            _utc(checked_at, "checked_at"),
            terminal,
            _utc(checked_at, "checked_at"),
            str(retry_id),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("durable window retry update was rejected")


def persist_advanced_scheduler_lifecycle_cursor(
    cursor,
    scheduler_session_id: UUID,
    *,
    status: str,
    reason_code: str,
    next_action: str,
    last_discovered_cutoff: datetime | None,
    checked_at: datetime,
) -> None:
    completed = status in {"IDLE", "BLOCKED"}
    cursor.execute(
        "UPDATE rt.advanced_scheduler_sessions SET status=%s,"
        "reason_code=%s,next_action=%s,last_discovered_cutoff=%s,"
        "updated_at=%s,completed_at=CASE WHEN %s THEN COALESCE("
        "completed_at,%s) ELSE completed_at END "
        "WHERE scheduler_session_id=%s RETURNING scheduler_session_id",
        (
            status,
            reason_code,
            next_action,
            last_discovered_cutoff,
            _utc(checked_at, "checked_at"),
            completed,
            _utc(checked_at, "checked_at"),
            str(scheduler_session_id),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("scheduler lifecycle update was rejected")


def audit_completed_session_coverage_cursor(
    cursor,
    *,
    cohort_id: UUID,
    sessions: tuple[date, ...],
    checked_at: datetime,
) -> dict:
    if not 1 <= len(sessions) <= 2:
        raise ValueError("startup coverage audit is bounded to two sessions")
    return plan_gap_repair_cursor(
        cursor,
        cohort_id=cohort_id,
        sessions=sessions,
        known_by=checked_at,
        intervals=("1m", "5m"),
    )


def prioritize_live_over_blocked_backfill_cursor(
    cursor,
    plan_id: UUID,
    *,
    checked_at: datetime,
) -> int:
    cursor.execute(
        "UPDATE rt.advanced_backfill_runs SET "
        "reason_code='LIVE_INGRESS_PRIORITY',"
        "next_action='PAUSE_BACKFILL_KEEP_HISTORY_PUBLICATION_BLOCKED',"
        "updated_at=%s WHERE plan_id=%s AND state='BACKFILL_BLOCKED' "
        "RETURNING backfill_id",
        (_utc(checked_at, "checked_at"), str(plan_id)),
    )
    return len(cursor.fetchall())
