"""Provider-free planning and durable ownership for continuous Advanced Equity."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Callable
from uuid import UUID

from options.calendar import OptionExchangeCalendar

from .advanced_ingress_plan import (
    AdvancedIngressBudgets,
    AdvancedStreamAuthorization,
    build_advanced_stream_authorization,
)
from .advanced_ingress_repository import (
    create_advanced_stream_authorization_cursor,
)
from .advanced_realtime_desired_state import (
    DesiredSessionSnapshot,
    DesiredStateChange,
    DesiredStateRevision,
)
from .advanced_realtime_desired_state_repository import (
    load_current_desired_state_revision_cursor,
    load_effective_desired_state_reconciliation_cursor,
    reconcile_desired_state_revision_cursor,
    record_desired_state_rollover_revision_cursor,
)
from .session_windows import advanced_session_windows


UTC = timezone.utc
ADVANCED_EQUITY_LIFECYCLE_RESOURCE = "advanced-equity:lifecycle"
AUTOMATIC_AUTHORIZATION_SOURCE = "ADVANCED_EQUITY_CONTINUOUS_V3"


class AdvancedEquityLifecyclePhase(str, Enum):
    BEFORE_OPEN = "BEFORE_OPEN"
    MARKET_HOURS = "MARKET_HOURS"
    DRAINING = "DRAINING"
    IDLE = "IDLE"


@dataclass(frozen=True, slots=True)
class AdvancedEquityLifecyclePlan:
    checked_at: datetime
    phase: AdvancedEquityLifecyclePhase
    active_session_date: date | None
    acquisition_session_date: date | None
    processing_session_date: date
    preparation_session_date: date
    target_session_date: date
    next_session_date: date
    session_open: datetime
    session_close: datetime
    drain_deadline: datetime
    processing_deadline: datetime
    first_due_cutoff: datetime | None
    catch_up_needed: bool


@dataclass(frozen=True, slots=True)
class EffectiveDesiredSession:
    revision: DesiredStateRevision
    snapshot: DesiredSessionSnapshot
    equity_member_count: int

    def __post_init__(self) -> None:
        if (
            self.revision.revision_id != self.snapshot.revision_id
            or self.revision.effective_session_date
            != self.snapshot.session_date
            or self.equity_member_count <= 0
        ):
            raise ValueError("effective desired session identity is invalid")


@dataclass(frozen=True, slots=True)
class LifecycleLease:
    resource_key: str
    owner_id: UUID
    owner_epoch: int
    lease_until: datetime


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


class AdvancedEquityLifecyclePlanner:
    def __init__(
        self,
        *,
        calendar: OptionExchangeCalendar | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._calendar = calendar or OptionExchangeCalendar("XNYS")
        self._clock = clock or (lambda: datetime.now(UTC))

    def plan(
        self,
        *,
        drain_seconds: int,
        finality_delay_seconds: int,
        completion_budget_seconds: int,
        last_completed_cutoff: datetime | None = None,
    ) -> AdvancedEquityLifecyclePlan:
        if (
            type(drain_seconds) is not int
            or drain_seconds < 0
            or type(finality_delay_seconds) is not int
            or not 1 <= finality_delay_seconds <= 60
            or type(completion_budget_seconds) is not int
            or not 1 <= completion_budget_seconds <= 300
        ):
            raise ValueError("Advanced Equity lifecycle timing is invalid")
        checked = _utc(self._clock(), "lifecycle clock")
        candidate = self._calendar.session_on_or_after(checked.date())
        active_session = None
        acquisition_session = None
        if candidate == checked.date():
            candidate_open = self._calendar.session_open(candidate)
            candidate_close = self._calendar.session_close(candidate)
            candidate_windows = advanced_session_windows(
                candidate,
                finality_delay_seconds=finality_delay_seconds,
                completion_budget_seconds=completion_budget_seconds,
                serving_lag_seconds=30,
                calendar=self._calendar,
            )
            processing_drain = max(
                candidate_windows[-1].deadline,
                candidate_close + timedelta(seconds=drain_seconds),
            )
            if checked < candidate_open:
                phase = AdvancedEquityLifecyclePhase.BEFORE_OPEN
                target = candidate
                acquisition_session = candidate
                preparation = candidate
            elif checked < candidate_close:
                phase = AdvancedEquityLifecyclePhase.MARKET_HOURS
                active_session = candidate
                target = candidate
                acquisition_session = candidate
                preparation = candidate
            elif checked < processing_drain:
                phase = AdvancedEquityLifecyclePhase.DRAINING
                active_session = candidate
                target = candidate
                preparation = self._calendar.next_session_date(
                    candidate
                )
            else:
                phase = AdvancedEquityLifecyclePhase.IDLE
                target = self._calendar.next_session_date(candidate)
                acquisition_session = target
                preparation = target
        else:
            phase = AdvancedEquityLifecyclePhase.IDLE
            target = candidate
            acquisition_session = target
            preparation = target

        opened = self._calendar.session_open(target)
        closed = self._calendar.session_close(target)
        drain_deadline = closed + timedelta(seconds=drain_seconds)
        windows = advanced_session_windows(
            target,
            finality_delay_seconds=finality_delay_seconds,
            completion_budget_seconds=completion_budget_seconds,
            serving_lag_seconds=30,
            calendar=self._calendar,
        )
        processing_deadline = max(
            windows[-1].deadline,
            drain_deadline,
        )
        completed = (
            None
            if last_completed_cutoff is None
            else _utc(last_completed_cutoff, "last_completed_cutoff")
        )
        first_due = next(
            (
                window.cutoff.astimezone(UTC)
                for window in windows
                if completed is None
                or window.cutoff.astimezone(UTC) > completed
            ),
            None,
        )
        catch_up_needed = (
            phase
            in {
                AdvancedEquityLifecyclePhase.MARKET_HOURS,
                AdvancedEquityLifecyclePhase.DRAINING,
            }
            and first_due is not None
            and any(
                window.cutoff.astimezone(UTC) >= first_due
                and window.available_after.astimezone(UTC) <= checked
                for window in windows
            )
        )
        return AdvancedEquityLifecyclePlan(
            checked_at=checked,
            phase=phase,
            active_session_date=active_session,
            acquisition_session_date=acquisition_session,
            processing_session_date=target,
            preparation_session_date=preparation,
            target_session_date=target,
            next_session_date=self._calendar.next_session_date(target),
            session_open=opened,
            session_close=closed,
            drain_deadline=drain_deadline,
            processing_deadline=processing_deadline,
            first_due_cutoff=first_due,
            catch_up_needed=catch_up_needed,
        )


def load_or_roll_effective_desired_session_cursor(
    cursor,
    *,
    target_session_date: date,
    checked_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> EffectiveDesiredSession:
    checked = _utc(checked_at, "desired session checked_at")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    revision = ensure_desired_session_revision_cursor(
        cursor,
        target_session_date=target_session_date,
        checked_at=checked,
        calendar=exchange,
    )
    effective = reconcile_desired_state_revision_cursor(
        cursor,
        revision.revision_id,
        materialized_at=checked,
        calendar=exchange,
    )
    if effective.state != "READY" or effective.snapshot is None:
        reasons = ",".join(effective.reason_codes)
        raise ValueError(
            "Advanced realtime effective desired snapshot is not READY: "
            f"{effective.state}"
            + (f":{reasons}" if reasons else "")
        )
    cursor.execute(
        "SELECT stock_count FROM rt.subscription_manifests "
        "WHERE manifest_id=%s AND session_date=%s",
        (
            str(effective.snapshot.manifest_id),
            target_session_date,
        ),
    )
    row = cursor.fetchone()
    if row is None or int(row[0]) <= 0:
        raise ValueError(
            "Advanced realtime effective desired manifest is unavailable"
        )
    return EffectiveDesiredSession(
        revision=effective.revision,
        snapshot=effective.snapshot,
        equity_member_count=int(row[0]),
    )


def ensure_desired_session_revision_cursor(
    cursor,
    *,
    target_session_date: date,
    checked_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> DesiredStateRevision:
    checked = _utc(checked_at, "desired intent checked_at")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    effective = load_effective_desired_state_reconciliation_cursor(
        cursor,
        session_date=target_session_date,
    )
    if (
        effective is not None
        and effective.revision.effective_session_date
        == target_session_date
    ):
        return effective.revision
    if effective is not None and effective.snapshot is None:
        effective = reconcile_desired_state_revision_cursor(
            cursor,
            effective.revision.revision_id,
            materialized_at=checked,
            calendar=exchange,
        )
    needs_rollover = (
        effective is not None
        and effective.snapshot is not None
        and effective.snapshot.session_date < target_session_date
    )
    if effective is None or needs_rollover:
        if effective is None:
            current = load_current_desired_state_revision_cursor(cursor)
            if current is None:
                raise ValueError(
                    "Advanced realtime desired-state pointer is missing"
                )
            if current.effective_session_date > target_session_date:
                raise ValueError(
                    "Advanced realtime desired snapshot is missing "
                    "for the active session"
                )
            current_state = reconcile_desired_state_revision_cursor(
                cursor,
                current.revision_id,
                materialized_at=checked,
                calendar=exchange,
            )
        else:
            current = effective.revision
            current_state = effective
        if current_state.state != "READY" or current_state.snapshot is None:
            reasons = ",".join(current_state.reason_codes)
            raise ValueError(
                "Advanced realtime current desired snapshot is not READY: "
                f"{current_state.state}"
                + (f":{reasons}" if reasons else "")
            )
        carry_forward = DesiredStateChange(
            source_cohort_id=current_state.snapshot.cohort_id,
            source_slot_version=current_state.snapshot.slot_set_version,
            members=current.members,
            enabled=current.enabled,
        )
        revision = record_desired_state_rollover_revision_cursor(
            cursor,
            carry_forward,
            effective_session_date=target_session_date,
            recorded_at=checked,
            calendar=exchange,
        )
        return revision
    if effective is None:
        raise ValueError(
            "Advanced realtime desired intent is unavailable"
        )
    return effective.revision


def ensure_automatic_stream_authorization_cursor(
    cursor,
    *,
    desired: EffectiveDesiredSession,
    budgets: AdvancedIngressBudgets,
    runtime_policy_sha256: str,
    promotion_ladder: tuple[int, ...],
    calendar: OptionExchangeCalendar | None = None,
) -> AdvancedStreamAuthorization:
    ladder = tuple(promotion_ladder)
    target = desired.equity_member_count
    if (
        not ladder
        or tuple(sorted(set(ladder))) != ladder
        or target not in ladder
    ):
        raise ValueError(
            "automatic authorization promotion ladder is invalid"
        )
    exchange = calendar or OptionExchangeCalendar("XNYS")
    targets = ladder[:ladder.index(target) + 1]
    sessions = [desired.snapshot.session_date]
    for _target in reversed(targets[:-1]):
        sessions.append(exchange.previous_session_date(sessions[-1]))
    sessions.reverse()
    parent_id = None
    stored = None
    for rung_target, session_date in zip(targets, sessions):
        authorization = build_advanced_stream_authorization(
            parent_authorization_id=parent_id,
            allowed_universe_targets=(rung_target,),
            valid_from=session_date,
            valid_through=session_date,
            maximum_budgets=budgets,
            runtime_policy_sha256=runtime_policy_sha256,
            approval_source=AUTOMATIC_AUTHORIZATION_SOURCE,
            approval_reference=(
                "continuous-session:"
                f"{desired.snapshot.snapshot_sha256}:"
                f"{rung_target}:{session_date.isoformat()}"
            ),
        )
        stored = create_advanced_stream_authorization_cursor(
            cursor,
            authorization,
            promotion_ladder=ladder,
            calendar=exchange,
        )
        parent_id = stored.authorization_id
    if stored is None:
        raise AssertionError("automatic authorization ladder is empty")
    return stored


def claim_lifecycle_lease_cursor(
    cursor,
    *,
    owner_id: UUID,
    checked_at: datetime,
    lease_seconds: int,
    resource_key: str = ADVANCED_EQUITY_LIFECYCLE_RESOURCE,
) -> LifecycleLease | None:
    checked = _utc(checked_at, "lifecycle lease checked_at")
    key = resource_key.strip()
    if not key or not 10 <= lease_seconds <= 3600:
        raise ValueError("Advanced Equity lifecycle lease scope is invalid")
    lease_until = checked + timedelta(seconds=lease_seconds)
    cursor.execute(
        "INSERT INTO rt.worker_leases "
        "(resource_key,owner_id,owner_epoch,lease_until,updated_at) "
        "VALUES (%s,%s,1,%s,%s) ON CONFLICT (resource_key) DO UPDATE SET "
        "owner_id=EXCLUDED.owner_id,"
        "owner_epoch=rt.worker_leases.owner_epoch+1,"
        "lease_until=EXCLUDED.lease_until,updated_at=EXCLUDED.updated_at "
        "WHERE rt.worker_leases.lease_until<=%s "
        "RETURNING owner_epoch,lease_until",
        (
            key,
            str(owner_id),
            lease_until,
            checked,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return LifecycleLease(
        resource_key=key,
        owner_id=owner_id,
        owner_epoch=int(row[0]),
        lease_until=_utc(row[1], "stored lifecycle lease_until"),
    )


def renew_lifecycle_lease_cursor(
    cursor,
    lease: LifecycleLease,
    *,
    checked_at: datetime,
    lease_seconds: int,
) -> LifecycleLease | None:
    checked = _utc(checked_at, "lifecycle lease checked_at")
    if not 10 <= lease_seconds <= 3600:
        raise ValueError("Advanced Equity lifecycle lease duration is invalid")
    lease_until = checked + timedelta(seconds=lease_seconds)
    cursor.execute(
        "UPDATE rt.worker_leases SET lease_until=%s,updated_at=%s "
        "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s "
        "AND lease_until>%s RETURNING lease_until",
        (
            lease_until,
            checked,
            lease.resource_key,
            str(lease.owner_id),
            lease.owner_epoch,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return LifecycleLease(
        lease.resource_key,
        lease.owner_id,
        lease.owner_epoch,
        _utc(row[0], "stored lifecycle lease_until"),
    )


def release_lifecycle_lease_cursor(cursor, lease: LifecycleLease) -> bool:
    cursor.execute(
        "UPDATE rt.worker_leases SET "
        "lease_until=LEAST(lease_until,clock_timestamp()),"
        "updated_at=clock_timestamp() "
        "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s",
        (
            lease.resource_key,
            str(lease.owner_id),
            lease.owner_epoch,
        ),
    )
    return cursor.rowcount == 1
