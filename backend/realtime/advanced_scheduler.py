"""Advanced scheduling decisions over canonical V2 orchestration state."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .gap_repair_plan import plan_gap_repair_cursor
from .orchestration_repository import CycleReceiptSpec


ADVANCED_WINDOW_BUILD_ID = "advanced-equity-window-v1"


def bind_advanced_input_gate_build_id(
    build_id: str,
    gate_sha256: str,
) -> str:
    if (
        not build_id.strip()
        or len(gate_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in gate_sha256
        )
    ):
        raise ValueError("Advanced input receipt gate identity is invalid")
    bound = f"{build_id}:{gate_sha256}"
    if len(bound) > 96:
        raise ValueError("Advanced input receipt build identity is too long")
    return bound


def advanced_input_gate_from_build_id(
    build_id: str,
    *,
    expected_build_id: str,
) -> str:
    prefix = f"{expected_build_id}:"
    if not build_id.startswith(prefix):
        raise ValueError("Advanced input receipt build identity is invalid")
    gate_sha256 = build_id[len(prefix):]
    bind_advanced_input_gate_build_id(expected_build_id, gate_sha256)
    return gate_sha256


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
    input_known_by: datetime | None
    input_gate_sha256: str | None


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


def order_advanced_due_windows(windows):
    """Run the latest live slot first and the oldest backlog item second."""
    rows = tuple(windows)
    if not rows:
        return ()
    cutoffs = tuple(_utc(row.cutoff, "window cutoff") for row in rows)
    if len(set(cutoffs)) != len(cutoffs):
        raise ValueError("Advanced due windows must have unique cutoffs")
    ordered = tuple(
        row
        for _cutoff, row in sorted(
            zip(cutoffs, rows),
            key=lambda item: item[0],
            reverse=True,
        )
    )
    if len(ordered) <= 2:
        return ordered
    return (ordered[0], ordered[-1], *ordered[1:-1])


def ensure_advanced_scheduler_session_cursor(
    _cursor,
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
    _utc(checked_at, "checked_at")
    scheduler_session_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-scheduler:"
        f"{authorization_id}:{session_date}:{first.isoformat()}",
    )
    return AdvancedSchedulerSession(
        scheduler_session_id,
        authorization_id,
        ingress_plan_id,
        cohort_id,
        session_date,
        first,
        closed,
        drain,
        "ACTIVE",
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
        "service.cohort_id,run.deadline "
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
            False, None, None, 0, now, None, None, None, None
        )
    windows = {}
    receipts: dict[str, set[CycleReceiptSpec]] = {
        "INPUT_READY": set(),
        "PORTAL_READY": set(),
    }
    input_scopes: list[tuple[int, datetime, str]] = []
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
        gate_digest = (
            advanced_input_gate_from_build_id(
                spec.build_id,
                expected_build_id=ADVANCED_WINDOW_BUILD_ID,
            )
            if spec.receipt_kind == "INPUT_READY"
            else None
        )
        if (
            spec.cutoff != cutoff_utc
            or (
                spec.receipt_kind != "INPUT_READY"
                and spec.build_id != ADVANCED_WINDOW_BUILD_ID
            )
            or str(row[11]) != str(cohort_id)
            or (
                spec.receipt_kind == "INPUT_READY"
                and str(row[8]) != str(row[12])
            )
            or row[13] != "stages_v3"
            or str(row[14]) != str(cohort_id)
        ):
            raise ValueError(
                "durable Advanced receipt does not match recovery scope"
            )
        receipts[spec.receipt_kind].add(spec)
        if spec.receipt_kind == "INPUT_READY":
            marker = "-retry"
            suffix = (
                str(row[1]).rsplit(marker, 1)[1]
                if marker in str(row[1])
                else "0"
            )
            attempt = int(suffix) if suffix.isdigit() else 0
            input_scopes.append((attempt, row[15], gate_digest))
    if any(len(values) > 1 for values in receipts.values()):
        raise ValueError("durable Advanced receipts disagree")
    if receipts["INPUT_READY"] and not input_scopes:
        raise ValueError(
            "durable Advanced INPUT_READY lacks its recovery scope"
        )
    if len({row[2] for row in input_scopes}) > 1:
        raise ValueError(
            "durable Advanced INPUT_READY gate digests disagree"
        )
    input_scope = (
        min(input_scopes, key=lambda row: (row[0], row[1]))
        if input_scopes
        else None
    )
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
        suffix = row[1].rsplit(marker, 1)[1] if marker in row[1] else "0"
        attempts.append(int(suffix) if suffix.isdigit() else 0)
    latest_attempt = max(attempts, default=-1)
    next_attempt = (
        latest_attempt if active_window_id is not None else latest_attempt + 1
    )
    delay = min(
        retry_max_backoff_seconds,
        retry_initial_backoff_seconds * (2 ** max(0, next_attempt - 1)),
    )
    retry_not_before = (
        latest[4] + timedelta(seconds=delay)
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
        None if input_scope is None else input_scope[1],
        None if input_scope is None else input_scope[2],
    )


def ensure_advanced_window_retry_cursor(
    _cursor,
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
    return AdvancedWindowRetry(
        retry_id,
        scheduler_session_id,
        cutoff_utc,
        attempt,
        window_key,
        window_id,
        available,
        (
            f"advanced-window-{cutoff_utc:%Y%m%dT%H%M%SZ}"
            f"-retry{attempt}.json"
        ),
        input_receipt is not None,
        portal_receipt is not None,
    )


def set_advanced_window_retry_status_cursor(
    _cursor,
    _retry_id: UUID,
    *,
    status: str,
    reason_code: str,
    checked_at: datetime,
) -> None:
    if status not in {
        "PLANNED", "RUNNING", "COMPLETE", "FAILED", "BLOCKED", "SKIPPED"
    } or not reason_code.strip():
        raise ValueError("window retry status is invalid")
    _utc(checked_at, "checked_at")


def persist_advanced_scheduler_lifecycle_cursor(
    _cursor,
    _scheduler_session_id: UUID,
    *,
    status: str,
    reason_code: str,
    next_action: str,
    last_discovered_cutoff: datetime | None,
    checked_at: datetime,
) -> None:
    if status not in {"ACTIVE", "DRAINING", "IDLE", "BLOCKED"}:
        raise ValueError("scheduler lifecycle status is invalid")
    if not reason_code.strip() or not next_action.strip():
        raise ValueError("scheduler lifecycle evidence is invalid")
    if last_discovered_cutoff is not None:
        _utc(last_discovered_cutoff, "last_discovered_cutoff")
    _utc(checked_at, "checked_at")


def load_advanced_scheduler_scope_cursor(
    cursor,
    *,
    authorization_id: UUID,
    session_date: date,
):
    cursor.execute(
        "SELECT plan.plan_id,plan.cohort_id,plan.universe_target,"
        "plan.runtime_profile_sha256,plan.status,"
        "plan.authorized_start_at,plan.authorized_end_at "
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
        "authorized_start_at": row[5],
        "authorized_end_at": row[6],
    }


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
    # The canonical windows remain RETAINED_GAP while live windows use their
    # own identities. No separate backfill state row is mutated.
    cursor.execute(
        "SELECT COUNT(*) FROM rt.window_runs AS run "
        "JOIN rt.service_plans AS service USING(plan_id) "
        "WHERE (service.cohort_id,service.session_date)=("
        "SELECT cohort_id,session_date FROM rt.advanced_ingress_plans "
        "WHERE plan_id=%s) "
        "AND service.scope_reference LIKE 'advanced-backfill:%%' "
        "AND run.state='RETAINED_GAP'",
        (str(plan_id),),
    )
    _utc(checked_at, "checked_at")
    return int(cursor.fetchone()[0])
