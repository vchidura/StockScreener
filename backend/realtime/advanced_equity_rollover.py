"""Keep established Advanced members current once a session has completed.

Member warmup only runs for new members, so nothing else writes the adjusted bars
or refreshes the dividend coverage that the next session reads.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar
from psycopg2.extras import RealDictCursor

from .acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from .advanced_equity_lifecycle import (
    AdvancedEquityLifecyclePhase,
    AdvancedEquityLifecyclePlanner,
)
from .advanced_equity_preparation import (
    EquityPreparationBlockedError,
    EquityPreparationClaim,
    EquityPreparationFetchRequest,
    EquityPreparationLimits,
    EquityPreparationSource,
    EquityPreparationSourceError,
    EquityPreparationStage,
    advanced_equity_live_priority_cursor,
    reconcile_equity_preparation_provider_requests_cursor,
)
from .advanced_realtime_desired_state import DesiredMemberKind
from .advanced_realtime_desired_state_repository import DESIRED_STATE_SCOPE
from .advanced_realtime_fencing import (
    DurableRuntimeLease,
    claim_durable_runtime_lease_cursor,
    release_durable_runtime_lease_cursor,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, WriteFenceRejected
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .equity_history import equity_history_coordinates, load_equity_history_cursor
from .equity_portal_history import build_realtime_equity_portal_history_plan
from .provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from .reference_facts import ReferenceFactRelation
from .reference_repository import persist_reference_facts_cursor


UTC = timezone.utc
ROLLOVER_MAXIMUM_ATTEMPTS = 3
ROLLOVER_RETRY_INITIAL_SECONDS = 60
ROLLOVER_RETRY_MAXIMUM_SECONDS = 900
# dividend_reference_window treats coverage as stale after one day.
DIVIDEND_COVERAGE_VALIDITY = timedelta(days=1)
# Activation normally follows the open by minutes; cover a late first capture.
DIVIDEND_COVERAGE_NEEDED_AFTER_OPEN = timedelta(hours=2)
# Long enough to renew on a Friday evening what would expire over the weekend.
IDENTITY_RENEWAL_MARGIN = timedelta(days=3)
ADJUSTED_INTERVALS = {
    "ADJUSTED_1D": "1d",
    "ADJUSTED_1WK": "1wk",
    "ADJUSTED_1MO": "1mo",
}
REFERENCE_ACTION = "REFERENCE"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def dividend_coverage_refresh_due(
    received_at: datetime | None,
    *,
    checked_at: datetime,
    session_open: datetime,
) -> bool:
    """A refresh is due only when one made now would last through activation."""
    needed_through = session_open + DIVIDEND_COVERAGE_NEEDED_AFTER_OPEN
    if checked_at + DIVIDEND_COVERAGE_VALIDITY <= needed_through:
        return False
    return (
        received_at is None
        or received_at + DIVIDEND_COVERAGE_VALIDITY <= needed_through
    )


@dataclass(frozen=True, slots=True)
class EquityRolloverMember:
    intent_id: UUID
    ticker: str
    member_kind: DesiredMemberKind
    security_id: UUID
    instrument_id: int


@dataclass(frozen=True, slots=True)
class EquityRolloverWork:
    member: EquityRolloverMember
    action: str
    sessions: tuple[date, ...] = ()
    renew_identity: bool = False

    @property
    def key(self) -> str:
        return f"{self.member.ticker}:{self.action}"


@dataclass(frozen=True, slots=True)
class EquityRolloverPlan:
    revision_id: UUID
    source_cohort_id: UUID
    target_session_date: date
    completed_session_date: date
    members: tuple[EquityRolloverMember, ...]
    work: tuple[EquityRolloverWork, ...]


@dataclass(frozen=True, slots=True)
class EquityRolloverResult:
    status: str
    checked_at: datetime
    target_session_date: date | None = None
    action: str | None = None
    reason: str | None = None
    remaining: int = 0
    provider_requests: int = 0
    response_bytes: int = 0
    persisted_bars: int = 0
    persisted_reference_facts: int = 0
    next_retry_at: datetime | None = None


class EquityRolloverBackoff(RuntimeError):
    def __init__(self, next_retry_at: datetime) -> None:
        super().__init__("ROLLOVER_BACKOFF")
        self.next_retry_at = next_retry_at


@dataclass(frozen=True, slots=True)
class EquityRolloverFence:
    lease: DurableRuntimeLease
    plan: EquityRolloverPlan
    work: EquityRolloverWork

    def verify(self, cursor) -> None:
        cursor.execute(
            "SELECT owner_epoch FROM rt.worker_leases "
            "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s "
            "AND lease_until>clock_timestamp() FOR UPDATE",
            (self.lease.resource_key, str(self.lease.owner_id), self.lease.owner_epoch),
        )
        if cursor.fetchone() is None:
            raise WriteFenceRejected("ROLLOVER_LEASE_LOST")
        cursor.execute(
            "SELECT 1 FROM rt.realtime_desired_current AS current "
            "JOIN rt.realtime_desired_revisions AS revision USING(revision_id) "
            "JOIN rt.realtime_desired_member_intents AS intent USING(revision_id) "
            "JOIN rt.realtime_desired_member_bindings AS binding USING(intent_id) "
            "JOIN rt.realtime_desired_intent_readiness AS readiness USING(intent_id) "
            "WHERE current.scope_key=%s AND current.revision_id=%s "
            "AND current.effective_session_date=%s AND revision.source_cohort_id=%s "
            "AND intent.intent_id=%s AND intent.enabled AND intent.ticker=%s "
            "AND intent.member_kind=%s AND readiness.status='READY' "
            "AND binding.security_id=%s AND binding.instrument_id=%s "
            "FOR SHARE OF current,readiness",
            (
                DESIRED_STATE_SCOPE, str(self.plan.revision_id),
                self.plan.target_session_date, str(self.plan.source_cohort_id),
                str(self.work.member.intent_id), self.work.member.ticker,
                self.work.member.member_kind.value, str(self.work.member.security_id),
                self.work.member.instrument_id,
            ),
        )
        if cursor.fetchone() is None:
            raise WriteFenceRejected("ROLLOVER_DESIRED_SCOPE_CHANGED")


def _member_rows_cursor(cursor, revision_id: UUID):
    cursor.execute(
        "SELECT intent.intent_id,intent.ticker,intent.member_kind,"
        "binding.security_id,binding.instrument_id "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_member_bindings AS binding "
        "USING(intent_id) "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "USING(intent_id) "
        "WHERE intent.revision_id=%s AND intent.enabled "
        "AND intent.member_kind IN ('EQUITY','SECTOR_ETF') "
        "AND readiness.status='READY' "
        "ORDER BY intent.ticker",
        (str(revision_id),),
    )
    return tuple(
        EquityRolloverMember(
            UUID(str(row["intent_id"])),
            str(row["ticker"]),
            DesiredMemberKind(str(row["member_kind"])),
            UUID(str(row["security_id"])),
            int(row["instrument_id"]),
        )
        for row in cursor.fetchall()
    )


def _missing_sessions_cursor(
    cursor,
    member: EquityRolloverMember,
    *,
    cohort_id: UUID,
    interval: str,
    sessions: tuple[date, ...],
    checked_at: datetime,
    adjustments_through: date,
    calendar: OptionExchangeCalendar,
    maximum_facts_per_chunk: int,
) -> tuple[date, ...]:
    history = load_equity_history_cursor(
        cursor,
        cohort_id=cohort_id,
        security_id=member.security_id,
        ticker=member.ticker,
        interval=interval,
        adjusted=True,
        sessions=sessions,
        checked_at=checked_at,
        adjustments_through=adjustments_through,
    )
    by_session = defaultdict(list)
    for row in history.rows:
        by_session[row["session_date"]].append(row)
    present = {
        session for session, rows in by_session.items()
        if len(rows) == 1
        and not rows[0]["stale_adjustment_lineage"]
        and (rows[0]["bar_start"], rows[0]["bar_end"])
        == equity_history_coordinates(calendar, interval, session)[0]
    }
    missing = []
    for session in sessions:
        if session in present:
            if missing:
                break
            continue
        missing.append(session)
        if len(missing) == maximum_facts_per_chunk:
            break
    return tuple(missing)


def _reference_work_cursor(
    cursor,
    member: EquityRolloverMember,
    *,
    target_session_date: date,
    session_open: datetime,
    checked_at: datetime,
) -> EquityRolloverWork | None:
    cursor.execute(
        "SELECT max(received_at) AS received_at "
        "FROM ref.corporate_action_coverage "
        "WHERE security_id=%s AND action_type='DIVIDEND' "
        "AND window_start<=%s AND window_end>=%s",
        (
            str(member.security_id),
            target_session_date,
            target_session_date + timedelta(days=45),
        ),
    )
    received_at = cursor.fetchone()["received_at"]
    cursor.execute(
        "SELECT max(effective_to) AS effective_to "
        "FROM ref.security_revisions WHERE security_id=%s AND active",
        (str(member.security_id),),
    )
    effective_to = cursor.fetchone()["effective_to"]
    renew_identity = (
        effective_to is None
        or effective_to - checked_at <= IDENTITY_RENEWAL_MARGIN
    )
    if not renew_identity and not dividend_coverage_refresh_due(
        received_at,
        checked_at=checked_at,
        session_open=session_open,
    ):
        return None
    return EquityRolloverWork(
        member,
        REFERENCE_ACTION,
        renew_identity=renew_identity,
    )


def plan_equity_rollover_cursor(
    cursor,
    *,
    checked_at: datetime,
    calendar: OptionExchangeCalendar,
    maximum_facts_per_chunk: int = 1000,
) -> EquityRolloverPlan | None:
    """Read-only. Requires a dictionary cursor."""
    if not 1 <= maximum_facts_per_chunk <= 1000:
        raise ValueError("rollover fact bound must be within 1-1000")
    checked = _utc(checked_at, "rollover checked_at")
    cursor.execute(
        "SELECT current.revision_id,current.effective_session_date,"
        "revision.source_cohort_id "
        "FROM rt.realtime_desired_current AS current "
        "JOIN rt.realtime_desired_revisions AS revision USING(revision_id) "
        "WHERE current.scope_key=%s",
        (DESIRED_STATE_SCOPE,),
    )
    row = cursor.fetchone()
    if row is None or row["source_cohort_id"] is None:
        return None
    revision_id = UUID(str(row["revision_id"]))
    cohort_id = UUID(str(row["source_cohort_id"]))
    target = row["effective_session_date"]
    completed = calendar.previous_session_date(target)
    members = _member_rows_cursor(cursor, revision_id)
    portal_plan = build_realtime_equity_portal_history_plan(completed)
    work = []
    for member in members:
        reference = _reference_work_cursor(
            cursor,
            member,
            target_session_date=target,
            session_open=calendar.session_open(target),
            checked_at=checked,
        )
        if reference is not None:
            work.append(reference)
        for action, interval, candidates in (
            ("ADJUSTED_1D", "1d", portal_plan.adjusted_sessions),
            ("ADJUSTED_1WK", "1wk", portal_plan.weekly_sessions),
            ("ADJUSTED_1MO", "1mo", portal_plan.monthly_sessions),
        ):
            missing = _missing_sessions_cursor(
                cursor,
                member,
                cohort_id=cohort_id,
                interval=interval,
                sessions=tuple(candidates),
                checked_at=checked,
                adjustments_through=completed,
                calendar=calendar,
                maximum_facts_per_chunk=maximum_facts_per_chunk,
            )
            if missing:
                work.append(EquityRolloverWork(member, action, missing))
    return EquityRolloverPlan(
        revision_id,
        cohort_id,
        target,
        completed,
        members,
        tuple(work),
    )


class AdvancedEquityRolloverResident:
    def __init__(
        self,
        provider,
        source: EquityPreparationSource,
        *,
        owner_id: UUID,
        limits: EquityPreparationLimits = EquityPreparationLimits(),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        calendar: OptionExchangeCalendar | None = None,
    ) -> None:
        self.provider = provider
        self.source = source
        self.owner_id = owner_id
        self.limits = limits
        self.clock = clock
        self.calendar = calendar or OptionExchangeCalendar("XNYS")
        if not 10 <= limits.lease_seconds <= 3600:
            raise ValueError("rollover lease duration must be within 10-3600 seconds")
        self.planner = AdvancedEquityLifecyclePlanner(
            calendar=self.calendar,
            clock=clock,
        )

    def plan(self, checked_at: datetime) -> EquityRolloverPlan | None:
        with self.provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
        ) as unit:
            return plan_equity_rollover_cursor(
                unit.cursor,
                checked_at=checked_at,
                calendar=self.calendar,
                maximum_facts_per_chunk=self.limits.maximum_facts_per_chunk,
            )

    def run_once(self) -> EquityRolloverResult:
        checked = _utc(self.clock(), "rollover resident clock")
        lifecycle = self.planner.plan(
            drain_seconds=30,
            finality_delay_seconds=5,
            completion_budget_seconds=300,
        )
        if lifecycle.phase not in {
            AdvancedEquityLifecyclePhase.IDLE,
            AdvancedEquityLifecyclePhase.BEFORE_OPEN,
        }:
            return EquityRolloverResult(
                "LIVE_PRIORITY",
                checked,
                reason=f"LIFECYCLE_{lifecycle.phase.value}",
            )
        with self.provider.transaction(APP_ROLE, read_only=True) as unit:
            if advanced_equity_live_priority_cursor(
                unit.cursor,
                checked_at=checked,
            ):
                return EquityRolloverResult(
                    "LIVE_PRIORITY",
                    checked,
                    reason="ACTIVE_STOCKS_INGRESS",
                )
        plan = self.plan(checked)
        if plan is None:
            return EquityRolloverResult(
                "IDLE",
                checked,
                reason="NO_DESIRED_SOURCE_COHORT",
            )
        if plan.target_session_date != lifecycle.target_session_date:
            return EquityRolloverResult(
                "IDLE",
                checked,
                plan.target_session_date,
                reason="DESIRED_SESSION_NOT_ROLLED",
            )
        if not plan.work:
            return EquityRolloverResult(
                "IDLE",
                checked,
                plan.target_session_date,
                reason="NO_DUE_ROLLOVER_WORK",
            )
        deferred = None
        for work in plan.work:
            result = self._execute(plan, work, checked, len(plan.work))
            if result.reason != "ROLLOVER_BACKOFF":
                return result
            deferred = result
        assert deferred is not None
        return deferred

    def _execute(
        self,
        plan: EquityRolloverPlan,
        work: EquityRolloverWork,
        checked: datetime,
        remaining: int,
    ) -> EquityRolloverResult:
        resource = f"advanced-equity-rollover:{plan.revision_id}"
        with self.provider.transaction(APP_ROLE) as unit:
            unit.cursor.execute(
                "SELECT owner_id,owner_epoch,lease_until FROM rt.worker_leases "
                "WHERE resource_key=%s FOR UPDATE",
                (resource,),
            )
            row = unit.cursor.fetchone()
            previous = (
                DurableRuntimeLease(resource, UUID(str(row[0])), int(row[1]), row[2])
                if row is not None else None
            )
            lease = claim_durable_runtime_lease_cursor(
                unit.cursor, resource_key=resource, owner_id=self.owner_id,
                checked_at=checked, lease_seconds=self.limits.lease_seconds,
            )
        if lease is None:
            return EquityRolloverResult(
                "BUSY", checked, plan.target_session_date, work.key,
                reason="ROLLOVER_ACTIVE_OWNER", remaining=remaining,
            )
        try:
            fence = EquityRolloverFence(lease, plan, work)
            with self.provider.transaction(APP_ROLE, fence=fence) as unit:
                fence.verify(unit.cursor)
                with unit.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    fresh = plan_equity_rollover_cursor(
                        cursor, checked_at=checked, calendar=self.calendar,
                        maximum_facts_per_chunk=self.limits.maximum_facts_per_chunk,
                    )
                if fresh is None or work not in fresh.work:
                    return EquityRolloverResult(
                        "RETRY" if fresh is not None and fresh.work else "IDLE",
                        checked, plan.target_session_date, work.key,
                        reason="ROLLOVER_WORK_ALREADY_SATISFIED", remaining=remaining,
                    )
                _recover_rollover_reservations_cursor(
                    unit.cursor, plan, work, checked_at=checked, previous_lease=previous,
                )
            return self._execute_owned(plan, work, checked, remaining, fence)
        except (WriteFenceRejected, EquityPreparationBlockedError) as error:
            return EquityRolloverResult(
                "BLOCKED", checked, plan.target_session_date, work.key,
                reason=str(error), remaining=remaining,
            )
        finally:
            with self.provider.transaction(APP_ROLE) as unit:
                release_durable_runtime_lease_cursor(
                    unit.cursor, lease, checked_at=_utc(self.clock(), "rollover release clock"),
                )

    def _execute_owned(
        self,
        plan: EquityRolloverPlan,
        work: EquityRolloverWork,
        checked: datetime,
        remaining: int,
        fence: EquityRolloverFence,
    ) -> EquityRolloverResult:
        reference = work.action == REFERENCE_ACTION
        try:
            with self.provider.transaction(APP_ROLE, fence=fence) as unit:
                fence.verify(unit.cursor)
                reservations, attempt = _reserve_rollover_requests_cursor(
                    unit.cursor,
                    plan,
                    work,
                    checked_at=checked,
                    limits=self.limits,
                    calendar=self.calendar,
                    lease=fence.lease,
                )
        except EquityRolloverBackoff as error:
            return EquityRolloverResult(
                "RETRY", checked, plan.target_session_date, work.key,
                reason=str(error), remaining=remaining, next_retry_at=error.next_retry_at,
            )
        except (ProviderBudgetExceeded, EquityPreparationBlockedError) as error:
            return EquityRolloverResult(
                "BLOCKED",
                checked,
                plan.target_session_date,
                work.key,
                reason=str(error),
                remaining=remaining,
            )

        def before_request(timeout_seconds: float) -> None:
            now = _utc(self.clock(), "rollover provider clock")
            deadline = min(
                fence.lease.lease_until, self.calendar.session_open(plan.target_session_date),
            )
            if now + timedelta(seconds=timeout_seconds + 5) >= deadline:
                raise EquityPreparationBlockedError("ROLLOVER_IO_BUDGET_EXHAUSTED")
            with self.provider.transaction(APP_ROLE, fence=fence) as unit:
                fence.verify(unit.cursor)
                if advanced_equity_live_priority_cursor(unit.cursor, checked_at=now):
                    raise EquityPreparationBlockedError("ACTIVE_STOCKS_INGRESS")

        request = EquityPreparationFetchRequest(
            replace(
                _rollover_claim(plan, work, checked, attempt, self.owner_id),
                lease_epoch=fence.lease.owner_epoch, lease_until=fence.lease.lease_until,
            ),
            checked,
            self.limits,
            provider_reservations=reservations,
            history_sessions=work.sessions if not reference else (),
            before_provider_request=before_request,
        )
        try:
            before_request(0)
            chunk = self.source.fetch(request)
            chunk.validate(request)
            if not chunk.complete:
                raise EquityPreparationSourceError(
                    "ROLLOVER_CHUNK_INCOMPLETE",
                    provider_requests=chunk.provider_requests,
                    response_bytes=chunk.response_bytes,
                )
        except (
            EquityPreparationBlockedError,
            EquityPreparationSourceError,
        ) as error:
            completed = _utc(self.clock(), "rollover failure clock")
            with self.provider.transaction(APP_ROLE, fence=fence) as unit:
                fence.verify(unit.cursor)
                reconcile_equity_preparation_provider_requests_cursor(
                    unit.cursor,
                    reservations,
                    provider_requests=error.provider_requests,
                    response_bytes=error.response_bytes,
                    checked_at=completed,
                    success=False,
                    failure_code=str(error),
                )
            return EquityRolloverResult(
                "RETRY",
                checked,
                plan.target_session_date,
                work.key,
                reason=str(error)[:160],
                remaining=remaining,
                provider_requests=error.provider_requests,
                response_bytes=error.response_bytes,
                next_retry_at=_rollover_retry_at(completed, attempt),
            )
        facts = tuple(
            fact
            for fact in chunk.reference_facts
            if work.renew_identity
            or fact.relation is not ReferenceFactRelation.SECURITY
        )
        with self.provider.transaction(APP_ROLE, fence=fence) as unit:
            fence.verify(unit.cursor)
            reconcile_equity_preparation_provider_requests_cursor(
                unit.cursor,
                reservations,
                provider_requests=chunk.provider_requests,
                response_bytes=chunk.response_bytes,
                checked_at=_utc(self.clock(), "rollover completion clock"),
                success=True,
            )
            if facts:
                persist_reference_facts_cursor(unit.cursor, facts)
            if not reference and chunk.bars:
                persist_realtime_equity_bars_cursor(unit.cursor, chunk.bars)
        return EquityRolloverResult(
            "PROGRESSED",
            checked,
            plan.target_session_date,
            work.key,
            remaining=remaining - 1,
            provider_requests=chunk.provider_requests,
            response_bytes=chunk.response_bytes,
            persisted_bars=len(chunk.bars),
            persisted_reference_facts=len(facts),
        )


def _rollover_claim(
    plan: EquityRolloverPlan,
    work: EquityRolloverWork,
    checked: datetime,
    attempt: int,
    owner_id: UUID,
) -> EquityPreparationClaim:
    reference = work.action == REFERENCE_ACTION
    target: dict[str, object] = {
        "contract": "advanced-equity-rollover-v1",
        "effective_session_date": plan.target_session_date.isoformat(),
        "history_through_session": plan.completed_session_date.isoformat(),
    }
    if reference:
        target["window_start"] = plan.completed_session_date.isoformat()
        target["window_end"] = plan.target_session_date.isoformat()
    else:
        target["interval_sessions"] = {
            ADJUSTED_INTERVALS[work.action]: [
                session.isoformat() for session in work.sessions
            ],
        }
    return EquityPreparationClaim(
        revision_id=plan.revision_id,
        intent_id=work.member.intent_id,
        security_id=work.member.security_id,
        member_kind=work.member.member_kind,
        ticker=work.member.ticker,
        stage=(
            EquityPreparationStage.REFERENCE
            if reference
            else EquityPreparationStage.ADJUSTED_DAILY
        ),
        stage_order=0,
        target=target,
        watermark={},
        coverage={},
        attempts=attempt,
        failure_streak=0,
        lease_owner=owner_id,
        lease_epoch=1,
        lease_until=checked + timedelta(minutes=5),
        source_cohort_id=plan.source_cohort_id,
        instrument_id=work.member.instrument_id,
    )


def _reserve_rollover_requests_cursor(
    cursor,
    plan: EquityRolloverPlan,
    work: EquityRolloverWork,
    *,
    checked_at: datetime,
    limits: EquityPreparationLimits,
    calendar: OptionExchangeCalendar,
    lease: DurableRuntimeLease,
) -> tuple[tuple[ProviderRequestReservation, ...], int]:
    reference = work.action == REFERENCE_ACTION
    slots = 3 if reference else 1
    lane = (
        ProviderRequestLane.REFERENCE
        if reference
        else ProviderRequestLane.EQUITY_HISTORY
    )
    authorization_reference = (
        f"advanced-equity-rollover:{plan.revision_id}:"
        f"{work.member.ticker}:{work.action}"
    )
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (authorization_reference,),
    )
    cursor.execute(
        "SELECT request_id,attempt,state,reserved_at,completed_at,request_sha256 "
        "FROM rt.provider_request_ledger WHERE authorization_reference=%s "
        "AND attempt=(SELECT max(attempt) FROM rt.provider_request_ledger "
        "WHERE authorization_reference=%s) ORDER BY request_id",
        (authorization_reference, authorization_reference),
    )
    latest = tuple(cursor.fetchall())
    if any(row[2] == "RESERVED" for row in latest):
        raise EquityPreparationBlockedError("ROLLOVER_UNRECONCILED_RESERVATION")
    attempt = max((int(row[1]) for row in latest), default=0) + 1
    if attempt > ROLLOVER_MAXIMUM_ATTEMPTS:
        raise ProviderBudgetExceeded("rollover request attempts exhausted")
    if latest:
        retry_at = _rollover_retry_at(
            max(row[4] or row[3] for row in latest), attempt - 1,
        )
        if checked_at < retry_at:
            raise EquityRolloverBackoff(retry_at)
    cursor.execute(
        "SELECT 1 FROM rt.worker_leases WHERE resource_key=%s "
        "AND owner_id=%s AND owner_epoch=%s AND lease_until>clock_timestamp()",
        (lease.resource_key, str(lease.owner_id), lease.owner_epoch),
    )
    if cursor.fetchone() is None:
        raise WriteFenceRejected("ROLLOVER_LEASE_LOST")
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=plan.target_session_date,
        authorization_reference=authorization_reference,
        provider_authorization_reference="MASSIVE_ADVANCED_ACCOUNT",
        expected_cost="EXISTING_SUBSCRIPTION",
        request_ceiling=slots * ROLLOVER_MAXIMUM_ATTEMPTS,
        decoded_byte_ceiling=(
            limits.maximum_response_bytes_per_chunk
            * ROLLOVER_MAXIMUM_ATTEMPTS
        ),
        account_request_ceiling=limits.maximum_account_requests,
        account_decoded_byte_ceiling=(
            limits.maximum_account_response_bytes
        ),
        lanes=frozenset({lane}),
        authorized_until=calendar.session_close(plan.target_session_date),
        policy_version="advanced-equity-rollover-budget-v1",
    )
    base_bytes, remainder = divmod(
        limits.maximum_response_bytes_per_chunk,
        slots,
    )
    reservations = []
    for slot in range(slots):
        reserved_bytes = base_bytes + (1 if slot < remainder else 0)
        base = ProviderRequestReservation(
            request_id=uuid5(
                NAMESPACE_URL,
                "advanced-equity-rollover-request:"
                f"{authorization_reference}:{attempt}:{slot}",
            ),
            account_alias=scope.account_alias,
            lane=lane,
            endpoint_class=f"ROLLOVER_{work.action}"[:48],
            request_sha256=_rollover_request_sha256(
                authorization_reference, attempt, slot, work,
                lease.owner_id, lease.owner_epoch,
            ),
            reserved_at=checked_at,
            attempt=attempt,
            cohort_id=plan.source_cohort_id,
        )
        reserve_coordinated_provider_request_cursor(
            cursor,
            scope,
            base,
            reserved_response_bytes=reserved_bytes,
            account_wide=True,
        )
        reservations.append(
            scope.bind(base, reserved_response_bytes=reserved_bytes)
        )
    return tuple(reservations), attempt


def _rollover_retry_at(completed_at: datetime, attempt: int) -> datetime:
    return completed_at + timedelta(seconds=min(
        ROLLOVER_RETRY_MAXIMUM_SECONDS,
        ROLLOVER_RETRY_INITIAL_SECONDS * (2 ** (attempt - 1)),
    ))


def _recover_rollover_reservations_cursor(
    cursor,
    plan: EquityRolloverPlan,
    work: EquityRolloverWork,
    *,
    checked_at: datetime,
    previous_lease: DurableRuntimeLease | None,
) -> None:
    authorization_reference = (
        f"advanced-equity-rollover:{plan.revision_id}:"
        f"{work.member.ticker}:{work.action}"
    )
    cursor.execute(
        "SELECT attempt,request_sha256 FROM rt.provider_request_ledger "
        "WHERE state='RESERVED' AND authorization_reference=%s FOR UPDATE",
        (authorization_reference,),
    )
    reserved = tuple(cursor.fetchall())
    if not reserved:
        return
    slots = 3 if work.action == REFERENCE_ACTION else 1
    if (
        previous_lease is None
        or previous_lease.lease_until > checked_at
        or any(
            str(digest) not in {
                _rollover_request_sha256(
                    authorization_reference, int(attempt), slot, work,
                    previous_lease.owner_id, previous_lease.owner_epoch,
                )
                for slot in range(slots)
            }
            for attempt, digest in reserved
        )
    ):
        raise EquityPreparationBlockedError("ROLLOVER_UNPROVEN_RESERVATION_OWNER")
    cursor.execute(
        "UPDATE rt.provider_request_ledger "
        "SET state='AMBIGUOUS',failure_code='ROLLOVER_PROCESS_LOST' "
        "WHERE state='RESERVED' AND authorization_reference=%s",
        (authorization_reference,),
    )


def _rollover_request_sha256(
    authorization_reference: str,
    attempt: int,
    slot: int,
    work: EquityRolloverWork,
    owner_id: UUID,
    owner_epoch: int,
) -> str:
    return canonical_sha256({
        "contract": "advanced-equity-rollover-request-v2",
        "authorization_reference": authorization_reference,
        "attempt": attempt,
        "slot": slot,
        "sessions": work.sessions,
        "lease_owner": owner_id,
        "lease_epoch": owner_epoch,
    })
