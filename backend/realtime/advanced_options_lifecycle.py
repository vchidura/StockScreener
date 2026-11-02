"""Durable desired-state lifecycle for continuous Advanced Options sessions."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
import threading
from typing import Mapping, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json
from psycopg2 import Error as PsycopgError

from options.calendar import OptionExchangeCalendar

from .advanced_realtime_desired_state import (
    DesiredMemberKind,
    MemberPreparationStatus,
)
from .advanced_realtime_desired_state_repository import (
    load_current_desired_state_revision_cursor,
    load_desired_member_preparations_cursor,
    load_effective_desired_state_reconciliation_cursor,
    reconcile_desired_state_revision_cursor,
    set_desired_member_preparation_cursor,
)
from .acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RequestCompleteness,
    finish_provider_request_cursor,
    mark_stale_provider_requests_ambiguous_cursor,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .provider_budget import (
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)


UTC = timezone.utc
OPTIONS_PREPARATION_STAGES = (
    ("CATALOG_QUOTE", 10),
    ("REFERENCE_OI", 20),
    ("HISTORY", 30),
    ("CAPACITY_SHARDS", 40),
    ("SESSION_ACTIVATION", 50),
)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsLifecycleStatus:
    revision_id: UUID
    session_date: date
    status: str
    current_stage: str
    option_underlyings: tuple[str, ...]
    desired_snapshot_sha256: str | None
    desired_cohort_id: UUID | None
    session_binding_id: UUID | None
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPreparationClaim:
    revision_id: UUID
    intent_id: UUID
    session_date: date
    ticker: str
    stage: str
    stage_order: int
    attempt: int
    owner_id: UUID
    owner_epoch: int
    lease_until: datetime
    watermark: dict[str, object]
    prior_evidence: dict[str, object]
    option_underlyings: tuple[str, ...]
    desired_snapshot_sha256: str | None
    desired_cohort_id: UUID | None
    source_equity_cohort_id: UUID


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProviderRequestPlan:
    lane: ProviderRequestLane
    endpoint_class: str
    request_sha256: str
    reserved_response_bytes: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.lane, ProviderRequestLane)
            or not self.endpoint_class.strip()
            or len(self.request_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.request_sha256
            )
            or type(self.reserved_response_bytes) is not int
            or self.reserved_response_bytes <= 0
        ):
            raise ValueError("Options provider request plan is invalid")


@dataclass(frozen=True, slots=True)
class AdvancedOptionsStageProviderBudget:
    scope: ProviderBudgetScope
    requests: tuple[AdvancedOptionsProviderRequestPlan, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ProviderBudgetScope)
            or not self.requests
            or len(self.requests) > self.scope.request_ceiling
            or sum(
                row.reserved_response_bytes for row in self.requests
            )
            > self.scope.decoded_byte_ceiling
        ):
            raise ValueError("Options stage provider budget is invalid")


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProviderRequestResult:
    request_id: UUID
    response_bytes: int
    row_count: int
    pages_fetched: int = 1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request_id, UUID)
            or self.request_id.int == 0
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.response_bytes,
                    self.row_count,
                    self.pages_fetched,
                )
            )
        ):
            raise ValueError("Options provider request result is invalid")


@dataclass(frozen=True, slots=True)
class AdvancedOptionsStageResult:
    watermark: dict[str, object]
    evidence: dict[str, object]
    provider_results: tuple[
        AdvancedOptionsProviderRequestResult, ...
    ] = ()
    session_binding_id: UUID | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.watermark, dict)
            or not self.watermark
            or not isinstance(self.evidence, dict)
            or not self.evidence
            or (
                self.session_binding_id is not None
                and (
                    not isinstance(self.session_binding_id, UUID)
                    or self.session_binding_id.int == 0
                )
            )
        ):
            raise ValueError("Options preparation stage result is invalid")


class AdvancedOptionsPreparationExecutor(Protocol):
    def provider_budget(
        self,
        claim: AdvancedOptionsPreparationClaim,
    ) -> AdvancedOptionsStageProviderBudget | None: ...

    def execute(
        self,
        claim: AdvancedOptionsPreparationClaim,
        *,
        provider_request_ids: tuple[UUID, ...],
    ) -> AdvancedOptionsStageResult: ...


class AdvancedOptionsPreparationRetryable(RuntimeError):
    pass


class AdvancedOptionsPreparationTerminal(ValueError):
    pass


class _PreparationClaimHeartbeat:
    def __init__(
        self,
        provider,
        claim,
        *,
        lease_seconds,
        clock,
    ):
        self._provider = provider
        self._claim = claim
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="advanced-options-preparation-lease",
            daemon=True,
        )

    @property
    def lost(self):
        return self._lost.is_set()

    def start(self):
        self._thread.start()

    def _run(self):
        interval = max(1.0, self._lease_seconds / 3)
        while not self._stop.wait(interval):
            try:
                with self._provider.transaction(APP_ROLE) as unit:
                    renewed = renew_advanced_options_preparation_cursor(
                        unit.cursor,
                        self._claim,
                        checked_at=self._clock(),
                        lease_seconds=self._lease_seconds,
                    )
            except PsycopgError:
                self._lost.set()
                return
            if renewed is None:
                self._lost.set()
                return
            self._claim = renewed

    def close(self):
        self._stop.set()
        self._thread.join(timeout=max(2.0, self._lease_seconds / 2))


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _next_retry(checked_at: datetime, failure_streak: int = 0) -> datetime:
    return checked_at + timedelta(
        seconds=min(60, 2 ** min(max(failure_streak, 0), 6))
    )


def _upsert_session(
    cursor,
    *,
    revision_id: UUID,
    session_date: date,
    status: str,
    current_stage: str,
    option_underlyings: tuple[str, ...],
    checked_at: datetime,
    snapshot_sha256: str | None = None,
    cohort_id: UUID | None = None,
    session_binding_id: UUID | None = None,
    last_error: str | None = None,
) -> None:
    cursor.execute(
        "INSERT INTO rt.advanced_options_preparation_sessions ("
        "revision_id,session_date,desired_snapshot_sha256,"
        "desired_cohort_id,status,current_stage,option_underlyings,"
        "next_retry_at,checked_at,activated_session_binding_id,last_error"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (revision_id) DO UPDATE SET "
        "desired_snapshot_sha256=EXCLUDED.desired_snapshot_sha256,"
        "desired_cohort_id=EXCLUDED.desired_cohort_id,"
        "status=EXCLUDED.status,current_stage=EXCLUDED.current_stage,"
        "option_underlyings=EXCLUDED.option_underlyings,"
        "next_retry_at=EXCLUDED.next_retry_at,"
        "checked_at=EXCLUDED.checked_at,"
        "activated_session_binding_id="
        "EXCLUDED.activated_session_binding_id,"
        "last_error=EXCLUDED.last_error,"
        "updated_at=clock_timestamp()",
        (
            str(revision_id),
            session_date,
            snapshot_sha256,
            None if cohort_id is None else str(cohort_id),
            status,
            current_stage,
            list(option_underlyings),
            _next_retry(checked_at),
            checked_at,
            (
                None
                if session_binding_id is None
                else str(session_binding_id)
            ),
            last_error,
        ),
    )


def _ensure_work(
    cursor,
    *,
    revision_id: UUID,
    option_underlyings: tuple[str, ...],
    checked_at: datetime,
) -> None:
    activation_ticker = option_underlyings[0]
    for ticker in option_underlyings:
        cursor.execute(
            "SELECT intent_id::text "
            "FROM rt.realtime_desired_member_intents "
            "WHERE revision_id=%s AND ticker=%s "
            "AND member_kind='OPTION_UNDERLYING' AND enabled",
            (str(revision_id), ticker),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(
                f"Options desired intent is unavailable for {ticker}"
            )
        intent_id = UUID(str(row[0]))
        for stage, stage_order in OPTIONS_PREPARATION_STAGES:
            if stage == "SESSION_ACTIVATION" and ticker != activation_ticker:
                continue
            cursor.execute(
                "INSERT INTO rt.advanced_options_preparation_work ("
                "revision_id,intent_id,ticker,stage,stage_order,status,"
                "watermark,evidence,next_retry_at"
                ") VALUES (%s,%s,%s,%s,%s,'PENDING',%s,%s,%s) "
                "ON CONFLICT (revision_id,intent_id,stage) DO NOTHING",
                (
                    str(revision_id),
                    str(intent_id),
                    ticker,
                    stage,
                    stage_order,
                    Json({}),
                    Json({}),
                    checked_at,
                ),
            )


def claim_advanced_options_preparation_cursor(
    cursor,
    *,
    owner_id: UUID,
    checked_at: datetime,
    lease_seconds: int,
) -> AdvancedOptionsPreparationClaim | None:
    checked = _utc(checked_at, "Options preparation claim checked_at")
    if (
        not isinstance(owner_id, UUID)
        or owner_id.int == 0
        or not 10 <= lease_seconds <= 3600
    ):
        raise ValueError("Options preparation claim scope is invalid")
    lease_until = checked + timedelta(seconds=lease_seconds)
    cursor.execute(
        "WITH candidate AS ("
        "SELECT work.revision_id,work.intent_id,work.stage "
        "FROM rt.advanced_options_preparation_work AS work "
        "JOIN rt.advanced_options_preparation_sessions AS session "
        "USING(revision_id) "
        "WHERE work.next_retry_at<=%s "
        "AND (work.status IN ('PENDING','RETRY') "
        "OR (work.status='RUNNING' AND work.lease_until<=%s)) "
        "AND (work.stage<>'SESSION_ACTIVATION' "
        "OR session.desired_snapshot_sha256 IS NOT NULL) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM rt.advanced_options_preparation_work AS prior "
        "WHERE prior.revision_id=work.revision_id "
        "AND ("
        "(work.stage='SESSION_ACTIVATION' "
        "AND prior.stage_order<work.stage_order) "
        "OR (work.stage<>'SESSION_ACTIVATION' "
        "AND prior.intent_id=work.intent_id "
        "AND prior.stage_order<work.stage_order)"
        ") AND prior.status<>'COMPLETE'"
        ") "
        "ORDER BY session.session_date,work.stage_order,work.ticker "
        "FOR UPDATE OF work SKIP LOCKED LIMIT 1"
        ") "
        "UPDATE rt.advanced_options_preparation_work AS work SET "
        "status='RUNNING',attempts=work.attempts+1,"
        "lease_owner=%s,lease_epoch=work.lease_epoch+1,"
        "lease_until=%s,next_retry_at=%s,"
        "blocked_reason=NULL,terminal_reason=NULL,"
        "updated_at=clock_timestamp() "
        "FROM candidate,rt.advanced_options_preparation_sessions AS session,"
        "rt.realtime_desired_revisions AS revision "
        "WHERE work.revision_id=candidate.revision_id "
        "AND work.intent_id=candidate.intent_id "
        "AND work.stage=candidate.stage "
        "AND session.revision_id=work.revision_id "
        "AND revision.revision_id=work.revision_id "
        "RETURNING work.revision_id::text,work.intent_id::text,"
        "session.session_date,work.ticker,work.stage,work.stage_order,"
        "work.attempts,work.lease_epoch,work.lease_until,"
        "work.watermark,work.evidence,session.option_underlyings,"
        "session.desired_snapshot_sha256,"
        "session.desired_cohort_id::text,"
        "revision.source_cohort_id::text",
        (
            checked,
            checked,
            str(owner_id),
            lease_until,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return AdvancedOptionsPreparationClaim(
        revision_id=UUID(str(row[0])),
        intent_id=UUID(str(row[1])),
        session_date=row[2],
        ticker=str(row[3]),
        stage=str(row[4]),
        stage_order=int(row[5]),
        attempt=int(row[6]),
        owner_id=owner_id,
        owner_epoch=int(row[7]),
        lease_until=_utc(row[8], "stored preparation lease_until"),
        watermark=dict(row[9]),
        prior_evidence=dict(row[10]),
        option_underlyings=tuple(str(value) for value in row[11]),
        desired_snapshot_sha256=(
            None if row[12] is None else str(row[12])
        ),
        desired_cohort_id=(
            None if row[13] is None else UUID(str(row[13]))
        ),
        source_equity_cohort_id=UUID(str(row[14])),
    )


def renew_advanced_options_preparation_cursor(
    cursor,
    claim: AdvancedOptionsPreparationClaim,
    *,
    checked_at: datetime,
    lease_seconds: int,
) -> AdvancedOptionsPreparationClaim | None:
    checked = _utc(checked_at, "Options preparation renew checked_at")
    if not 10 <= lease_seconds <= 3600:
        raise ValueError("Options preparation lease duration is invalid")
    lease_until = checked + timedelta(seconds=lease_seconds)
    cursor.execute(
        "UPDATE rt.advanced_options_preparation_work SET "
        "lease_until=%s,updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s "
        "AND lease_until>%s RETURNING lease_until",
        (
            lease_until,
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage,
            str(claim.owner_id),
            claim.owner_epoch,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return replace(
        claim,
        lease_until=_utc(
            row[0],
            "stored preparation lease_until",
        ),
    )


def _complete_provider_requests_cursor(
    cursor,
    *,
    reserved_request_ids: tuple[UUID, ...],
    results: tuple[AdvancedOptionsProviderRequestResult, ...],
    checked_at: datetime,
    failure_code: str | None = None,
) -> None:
    by_id = {row.request_id: row for row in results}
    if (
        len(by_id) != len(results)
        or not set(by_id).issubset(reserved_request_ids)
    ):
        raise ValueError("Options provider results exceed reservations")
    for request_id in reserved_request_ids:
        result = by_id.get(request_id)
        if result is None:
            completion = ProviderRequestCompletion(
                ProviderRequestState.FAILED,
                completed_at=checked_at,
                started_at=checked_at,
                outcome=ProviderRequestOutcome.INVALID,
                response_bytes=0,
                row_count=0,
                pages_fetched=0,
                price_basis=PriceBasis.NOT_APPLICABLE,
                completeness=RequestCompleteness.INCOMPLETE,
                failure_code=failure_code or "RESERVATION_NOT_USED",
            )
        else:
            completion = ProviderRequestCompletion(
                ProviderRequestState.COMPLETE,
                completed_at=checked_at,
                started_at=checked_at,
                outcome=ProviderRequestOutcome.OK,
                response_bytes=result.response_bytes,
                row_count=result.row_count,
                pages_fetched=result.pages_fetched,
                price_basis=PriceBasis.NOT_APPLICABLE,
                completeness=RequestCompleteness.COMPLETE,
            )
        finish_provider_request_cursor(
            cursor,
            request_id,
            completion,
        )


def complete_advanced_options_preparation_cursor(
    cursor,
    claim: AdvancedOptionsPreparationClaim,
    result: AdvancedOptionsStageResult,
    *,
    provider_request_ids: tuple[UUID, ...],
    completed_at: datetime,
) -> None:
    completed = _utc(completed_at, "Options preparation completed_at")
    if not isinstance(result, AdvancedOptionsStageResult):
        raise TypeError("Options preparation result is invalid")
    result_ids = tuple(row.request_id for row in result.provider_results)
    _complete_provider_requests_cursor(
        cursor,
        reserved_request_ids=provider_request_ids,
        results=result.provider_results,
        checked_at=completed,
    )
    evidence_document = {
        "version": "advanced-options-preparation-evidence-v1",
        "revision_id": claim.revision_id,
        "intent_id": claim.intent_id,
        "ticker": claim.ticker,
        "stage": claim.stage,
        "attempt": claim.attempt,
        "watermark_before": claim.watermark,
        "watermark_after": result.watermark,
        "evidence": result.evidence,
        "provider_request_ids": provider_request_ids,
        "provider_result_ids": result_ids,
        "session_binding_id": result.session_binding_id,
    }
    evidence_sha256 = canonical_sha256(evidence_document)
    cursor.execute(
        "INSERT INTO rt.advanced_options_preparation_evidence ("
        "revision_id,intent_id,stage,attempt,owner_id,owner_epoch,"
        "watermark_before,watermark_after,evidence,evidence_sha256,"
        "provider_request_ids,provider_request_count,response_bytes,"
        "completed_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s::uuid[],%s,%s,%s) "
        "ON CONFLICT (revision_id,intent_id,stage) DO NOTHING",
        (
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage,
            claim.attempt,
            str(claim.owner_id),
            claim.owner_epoch,
            Json(claim.watermark),
            Json(result.watermark),
            Json(result.evidence),
            evidence_sha256,
            [str(value) for value in provider_request_ids],
            len(provider_request_ids),
            sum(row.response_bytes for row in result.provider_results),
            completed,
        ),
    )
    cursor.execute(
        "SELECT evidence_sha256 FROM "
        "rt.advanced_options_preparation_evidence "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s",
        (
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage,
        ),
    )
    stored = cursor.fetchone()
    if stored is None or str(stored[0]) != evidence_sha256:
        raise ValueError(
            "Options preparation completion replay diverged"
        )
    cursor.execute(
        "UPDATE rt.advanced_options_preparation_work SET "
        "status='COMPLETE',watermark=%s,evidence=%s,"
        "provider_request_count=provider_request_count+%s,"
        "response_bytes=response_bytes+%s,"
        "lease_owner=NULL,lease_until=NULL,"
        "failure_streak=0,next_retry_at=%s,"
        "completed_at=%s,blocked_reason=NULL,terminal_reason=NULL,"
        "updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s "
        "AND lease_until>%s",
        (
            Json(result.watermark),
            Json(result.evidence),
            len(result.provider_results),
            sum(row.response_bytes for row in result.provider_results),
            completed,
            completed,
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage,
            str(claim.owner_id),
            claim.owner_epoch,
            completed,
        ),
    )
    if cursor.rowcount != 1:
        raise ValueError("Options preparation completion lost its fence")
    if claim.stage == "SESSION_ACTIVATION":
        if result.session_binding_id is None:
            raise ValueError(
                "session activation requires a session binding"
            )
        cursor.execute(
            "UPDATE rt.advanced_options_preparation_sessions SET "
            "status='ACTIVE',current_stage='COMPLETE',"
            "activated_session_binding_id=%s,checked_at=%s,"
            "next_retry_at=%s,last_error=NULL,"
            "updated_at=clock_timestamp() "
            "WHERE revision_id=%s",
            (
                str(result.session_binding_id),
                completed,
                completed,
                str(claim.revision_id),
            ),
        )


def fail_advanced_options_preparation_cursor(
    cursor,
    claim: AdvancedOptionsPreparationClaim,
    *,
    failed_at: datetime,
    reason: str,
    retryable: bool,
    provider_request_ids: tuple[UUID, ...] = (),
) -> None:
    failed = _utc(failed_at, "Options preparation failed_at")
    normalized = reason.strip()[:160]
    if not normalized:
        raise ValueError("Options preparation failure reason is blank")
    _complete_provider_requests_cursor(
        cursor,
        reserved_request_ids=provider_request_ids,
        results=(),
        checked_at=failed,
        failure_code=normalized,
    )
    delay = min(300, 2 ** min(claim.attempt, 8))
    status = "RETRY" if retryable else "FAILED"
    cursor.execute(
        "UPDATE rt.advanced_options_preparation_work SET "
        "status=%s,failure_streak=failure_streak+1,"
        "next_retry_at=%s,lease_owner=NULL,lease_until=NULL,"
        "terminal_reason=%s,blocked_reason=NULL,"
        "completed_at=%s,updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s",
        (
            status,
            failed + timedelta(seconds=delay),
            None if retryable else normalized,
            None if retryable else failed,
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage,
            str(claim.owner_id),
            claim.owner_epoch,
        ),
    )
    if cursor.rowcount != 1:
        raise ValueError("Options preparation failure lost its fence")


def _reserve_stage_provider_budget(
    cursor,
    claim: AdvancedOptionsPreparationClaim,
    budget: AdvancedOptionsStageProviderBudget | None,
    *,
    checked_at: datetime,
) -> tuple[UUID, ...]:
    if budget is None:
        return ()
    request_ids = []
    for index, request in enumerate(budget.requests, start=1):
        request_id = uuid5(
            NAMESPACE_URL,
            "stockscreener:advanced-options-preparation-request:"
            f"{claim.revision_id}:{claim.intent_id}:{claim.stage}:"
            f"{claim.attempt}:{index}",
        )
        reservation = ProviderRequestReservation(
            request_id=request_id,
            account_alias=budget.scope.account_alias,
            lane=request.lane,
            endpoint_class=request.endpoint_class,
            request_sha256=request.request_sha256,
            reserved_at=checked_at,
            attempt=claim.attempt,
        )
        reserve_coordinated_provider_request_cursor(
            cursor,
            budget.scope,
            reservation,
            reserved_response_bytes=(
                request.reserved_response_bytes
            ),
            account_wide=True,
        )
        request_ids.append(request_id)
    return tuple(request_ids)


def run_advanced_options_preparation_cycle(
    provider: V2ConnectionProvider,
    executor: AdvancedOptionsPreparationExecutor,
    *,
    owner_id: UUID,
    checked_at: datetime,
    lease_seconds: int = 60,
    clock=lambda: datetime.now(UTC),
) -> dict[str, object]:
    checked = _utc(checked_at, "Options preparation cycle checked_at")
    with provider.transaction(
        APP_ROLE,
        isolation_level="SERIALIZABLE",
    ) as unit:
        mark_stale_provider_requests_ambiguous_cursor(
            unit.cursor,
            reserved_before=(
                checked - timedelta(seconds=lease_seconds)
            ),
        )
        claim = claim_advanced_options_preparation_cursor(
            unit.cursor,
            owner_id=owner_id,
            checked_at=checked,
            lease_seconds=lease_seconds,
        )
    if claim is None:
        return {
            "status": "IDLE",
            "checked_at_utc": checked.isoformat(),
            "provider_requests": 0,
        }
    budget = executor.provider_budget(claim)
    with provider.transaction(
        APP_ROLE,
        isolation_level="SERIALIZABLE",
    ) as unit:
        provider_request_ids = _reserve_stage_provider_budget(
            unit.cursor,
            claim,
            budget,
            checked_at=checked,
        )
    heartbeat = _PreparationClaimHeartbeat(
        provider,
        claim,
        lease_seconds=lease_seconds,
        clock=clock,
    )
    heartbeat.start()
    try:
        result = executor.execute(
            claim,
            provider_request_ids=provider_request_ids,
        )
    except AdvancedOptionsPreparationTerminal as error:
        heartbeat.close()
        if heartbeat.lost:
            raise RuntimeError(
                "Options preparation fence was lost"
            ) from error
        with provider.transaction(APP_ROLE) as unit:
            fail_advanced_options_preparation_cursor(
                unit.cursor,
                claim,
                failed_at=checked,
                reason=str(error),
                retryable=False,
                provider_request_ids=provider_request_ids,
            )
        return {
            "status": "FAILED",
            "stage": claim.stage,
            "ticker": claim.ticker,
            "reason": str(error),
            "provider_requests": len(provider_request_ids),
        }
    except AdvancedOptionsPreparationRetryable as error:
        heartbeat.close()
        if heartbeat.lost:
            raise RuntimeError(
                "Options preparation fence was lost"
            ) from error
        with provider.transaction(APP_ROLE) as unit:
            fail_advanced_options_preparation_cursor(
                unit.cursor,
                claim,
                failed_at=checked,
                reason=str(error),
                retryable=True,
                provider_request_ids=provider_request_ids,
            )
        return {
            "status": "RETRY",
            "stage": claim.stage,
            "ticker": claim.ticker,
            "reason": str(error),
            "provider_requests": len(provider_request_ids),
        }
    except Exception as error:
        heartbeat.close()
        if heartbeat.lost:
            raise RuntimeError(
                "Options preparation fence was lost"
            ) from error
        with provider.transaction(APP_ROLE) as unit:
            fail_advanced_options_preparation_cursor(
                unit.cursor,
                claim,
                failed_at=checked,
                reason=type(error).__name__,
                retryable=True,
                provider_request_ids=provider_request_ids,
            )
        raise
    heartbeat.close()
    if heartbeat.lost:
        raise RuntimeError("Options preparation fence was lost")
    completed_at = _utc(
        clock(),
        "Options preparation completion clock",
    )
    with provider.transaction(
        APP_ROLE,
        isolation_level="SERIALIZABLE",
    ) as unit:
        complete_advanced_options_preparation_cursor(
            unit.cursor,
            claim,
            result,
            provider_request_ids=provider_request_ids,
            completed_at=completed_at,
        )
    return {
        "status": "COMPLETE",
        "stage": claim.stage,
        "ticker": claim.ticker,
        "attempt": claim.attempt,
        "owner_epoch": claim.owner_epoch,
        "provider_requests": len(result.provider_results),
        "response_bytes": sum(
            row.response_bytes for row in result.provider_results
        ),
        "session_binding_id": (
            None
            if result.session_binding_id is None
            else str(result.session_binding_id)
        ),
    }


def reconcile_advanced_options_lifecycle_cursor(
    cursor,
    *,
    target_session_date: date,
    checked_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> AdvancedOptionsLifecycleStatus | None:
    checked = _utc(checked_at, "Options lifecycle checked_at")
    exchange = calendar or OptionExchangeCalendar("XNYS")
    if exchange.session_on_or_after(target_session_date) != target_session_date:
        raise ValueError("Options lifecycle target must be an XNYS session")
    revision = load_current_desired_state_revision_cursor(cursor)
    if revision is None:
        return None
    if revision.effective_session_date != target_session_date:
        effective = load_effective_desired_state_reconciliation_cursor(
            cursor,
            session_date=target_session_date,
        )
        if effective is None:
            return None
        revision = effective.revision
    underlyings = tuple(sorted(
        member.ticker
        for member in revision.members
        if member.enabled
        and member.kind is DesiredMemberKind.OPTION_UNDERLYING
    ))
    if not underlyings:
        return None
    preparations = load_desired_member_preparations_cursor(
        cursor,
        revision.revision_id,
    )
    base_ready = {
        row.ticker
        for row in preparations
        if row.enabled
        and row.kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
        and row.status is MemberPreparationStatus.READY
    }
    missing_equity = tuple(sorted(set(underlyings) - base_ready))
    reconciliation = load_effective_desired_state_reconciliation_cursor(
        cursor,
        session_date=target_session_date,
    )
    snapshot = (
        None
        if reconciliation is None
        or reconciliation.state != "READY"
        else reconciliation.snapshot
    )
    snapshot_refresh_error = None
    cursor.execute(
        "SELECT session_binding_id::text "
        "FROM rt.advanced_options_continuous_sessions "
        "WHERE session_date=%s",
        (target_session_date,),
    )
    active = cursor.fetchone()
    if active is not None:
        session_binding_id = UUID(str(active[0]))
        _upsert_session(
            cursor,
            revision_id=revision.revision_id,
            session_date=target_session_date,
            status="ACTIVE",
            current_stage="COMPLETE",
            option_underlyings=underlyings,
            checked_at=checked,
            snapshot_sha256=(
                None if snapshot is None else snapshot.snapshot_sha256
            ),
            cohort_id=(
                None if snapshot is None else snapshot.cohort_id
            ),
            session_binding_id=session_binding_id,
        )
        return AdvancedOptionsLifecycleStatus(
            revision.revision_id,
            target_session_date,
            "ACTIVE",
            "COMPLETE",
            underlyings,
            None if snapshot is None else snapshot.snapshot_sha256,
            None if snapshot is None else snapshot.cohort_id,
            session_binding_id,
            (),
        )
    if missing_equity:
        _upsert_session(
            cursor,
            revision_id=revision.revision_id,
            session_date=target_session_date,
            status="WAITING_EQUITY",
            current_stage="EQUITY_READINESS",
            option_underlyings=underlyings,
            checked_at=checked,
            snapshot_sha256=(
                None if snapshot is None else snapshot.snapshot_sha256
            ),
            cohort_id=(
                None if snapshot is None else snapshot.cohort_id
            ),
            last_error=(
                "Options underlyings require READY Equity: "
                + ",".join(missing_equity)
            ),
        )
        return AdvancedOptionsLifecycleStatus(
            revision.revision_id,
            target_session_date,
            "WAITING_EQUITY",
            "EQUITY_READINESS",
            underlyings,
            None if snapshot is None else snapshot.snapshot_sha256,
            None if snapshot is None else snapshot.cohort_id,
            None,
            tuple(
                f"EQUITY_NOT_READY:{ticker}"
                for ticker in missing_equity
            ),
        )
    _upsert_session(
        cursor,
        revision_id=revision.revision_id,
        session_date=target_session_date,
        status="PREPARING",
        current_stage="CATALOG_QUOTE",
        option_underlyings=underlyings,
        checked_at=checked,
        snapshot_sha256=(
            None if snapshot is None else snapshot.snapshot_sha256
        ),
        cohort_id=None if snapshot is None else snapshot.cohort_id,
    )
    _ensure_work(
        cursor,
        revision_id=revision.revision_id,
        option_underlyings=underlyings,
        checked_at=checked,
    )
    cursor.execute(
        "SELECT stage,status FROM rt.advanced_options_preparation_work "
        "WHERE revision_id=%s "
        "ORDER BY stage_order,ticker",
        (str(revision.revision_id),),
    )
    work = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT ticker,bool_and(status='COMPLETE') "
        "FROM rt.advanced_options_preparation_work "
        "WHERE revision_id=%s AND stage<>'SESSION_ACTIVATION' "
        "GROUP BY ticker ORDER BY ticker",
        (str(revision.revision_id),),
    )
    readiness_rows = tuple(cursor.fetchall())
    for ticker, ready in readiness_rows:
        if ready:
            set_desired_member_preparation_cursor(
                cursor,
                revision.revision_id,
                ticker=str(ticker),
                kind=DesiredMemberKind.OPTION_UNDERLYING,
                status=MemberPreparationStatus.READY,
                checked_at=checked,
            )
    if (
        snapshot is None
        and readiness_rows
        and all(bool(row[1]) for row in readiness_rows)
    ):
        try:
            refreshed = reconcile_desired_state_revision_cursor(
                cursor,
                revision.revision_id,
                materialized_at=checked,
                calendar=exchange,
                allow_partial_activation=True,
            )
        except ValueError as error:
            snapshot_refresh_error = str(error)
        else:
            if refreshed.state == "READY":
                snapshot = refreshed.snapshot
            else:
                snapshot_refresh_error = (
                    refreshed.state
                    + (
                        ":"
                        + ",".join(refreshed.reason_codes)
                        if refreshed.reason_codes
                        else ""
                    )
                )
    failed = tuple(
        str(row[0]) for row in work if str(row[1]) == "FAILED"
    )
    pending = next(
        (
            str(row[0])
            for row in work
            if str(row[1]) != "COMPLETE"
        ),
        None,
    )
    waiting_snapshot = (
        snapshot is None
        and pending == "SESSION_ACTIVATION"
        and not failed
    )
    status = (
        "DEGRADED"
        if failed
        else "WAITING_DESIRED"
        if waiting_snapshot
        else "READY"
        if pending is None
        else "PREPARING"
    )
    current_stage = "COMPLETE" if pending is None else pending
    _upsert_session(
        cursor,
        revision_id=revision.revision_id,
        session_date=target_session_date,
        status=status,
        current_stage=current_stage,
        option_underlyings=underlyings,
        checked_at=checked,
        snapshot_sha256=(
            None if snapshot is None else snapshot.snapshot_sha256
        ),
        cohort_id=None if snapshot is None else snapshot.cohort_id,
        last_error=(
            (
                snapshot_refresh_error
                or "effective desired snapshot is not READY"
            )
            if waiting_snapshot
            else None
            if not failed
            else "failed Options preparation stages: "
            + ",".join(sorted(set(failed)))
        ),
    )
    return AdvancedOptionsLifecycleStatus(
        revision.revision_id,
        target_session_date,
        status,
        current_stage,
        underlyings,
        None if snapshot is None else snapshot.snapshot_sha256,
        None if snapshot is None else snapshot.cohort_id,
        None,
        (
            ("DESIRED_SNAPSHOT_NOT_READY",)
            if waiting_snapshot
            else tuple(f"FAILED_STAGE:{stage}" for stage in failed)
        ),
    )
