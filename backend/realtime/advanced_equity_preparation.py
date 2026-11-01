"""Durable off-hours preparation for newly desired Advanced Equity members."""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from equity.domain import BarAvailabilityMode, BarSourceKind
from equity.polygon import PolygonEquityClient, normalize_native_bars
from options.calendar import OptionExchangeCalendar

from .advanced_equity_lifecycle import (
    AdvancedEquityLifecyclePhase,
    AdvancedEquityLifecyclePlanner,
)
from .advanced_realtime_desired_state import (
    DesiredMemberKind,
    MemberPreparationStatus,
)
from .advanced_realtime_desired_state_repository import (
    DESIRED_STATE_SCOPE,
    bind_desired_member_identity_cursor,
    promote_prepared_desired_member_cursor,
    set_desired_member_preparation_cursor,
)
from .contracts import (
    EQUITY_VALIDATION_UNIVERSE,
    UniverseManifest,
    canonical_sha256,
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
)
from .database_access_v2 import APP_ROLE
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .equity_interval_derivation import derive_realtime_interval_facts
from .equity_interval_derivation_worker import (
    load_current_five_minute_facts_cursor,
    load_derivation_security_cursor,
)
from .preflight import ProviderTier
from .provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from .reference_adapters import MassiveReferenceAdapter
from .reference_facts import ReferenceFact, ReferenceFactRelation
from .reference_repository import persist_reference_facts_cursor
from .reference_service import ReferenceSeedRequest


UTC = timezone.utc
PREPARATION_RESOURCE_KEY = "advanced-equity-preparation"
PREPARATION_STAGES = (
    "REFERENCE",
    "ADJUSTED_DAILY",
    "NATIVE_INTRADAY",
    "DERIVED_INTERVALS",
    "TECHNICAL_WARMUP",
)


class EquityPreparationStage(str, Enum):
    REFERENCE = "REFERENCE"
    ADJUSTED_DAILY = "ADJUSTED_DAILY"
    NATIVE_INTRADAY = "NATIVE_INTRADAY"
    DERIVED_INTERVALS = "DERIVED_INTERVALS"
    TECHNICAL_WARMUP = "TECHNICAL_WARMUP"


class EquityPreparationSourceError(RuntimeError):
    def __init__(
        self,
        reason: str,
        *,
        provider_requests: int = 0,
        response_bytes: int = 0,
    ) -> None:
        super().__init__(reason)
        if provider_requests < 0 or response_bytes < 0:
            raise ValueError("source failure counters cannot be negative")
        self.provider_requests = provider_requests
        self.response_bytes = response_bytes


class EquityPreparationBlockedError(RuntimeError):
    def __init__(
        self,
        reason: str,
        *,
        provider_requests: int = 0,
        response_bytes: int = 0,
    ) -> None:
        super().__init__(reason)
        if provider_requests < 0 or response_bytes < 0:
            raise ValueError("blocked source counters cannot be negative")
        self.provider_requests = provider_requests
        self.response_bytes = response_bytes


class EquityPreparationUnavailableError(EquityPreparationSourceError):
    pass


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _json_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return {str(key): item for key, item in value.items()}


def _interval_counts(coverage: Mapping[str, object]) -> dict[str, int]:
    values = coverage.get("available_intervals", {})
    if not isinstance(values, Mapping):
        raise ValueError("preparation interval coverage must be an object")
    counts = {str(key): int(value) for key, value in values.items()}
    if any(value < 0 for value in counts.values()):
        raise ValueError("preparation interval coverage cannot be negative")
    return counts


def preparation_retry_delay_seconds(
    limits: EquityPreparationLimits,
    *,
    failure_streak: int,
) -> int:
    if type(failure_streak) is not int or failure_streak < 0:
        raise ValueError("preparation failure streak is invalid")
    return min(
        limits.retry_maximum_seconds,
        limits.retry_initial_seconds
        * (2 ** min(failure_streak, 10)),
    )


@dataclass(frozen=True, slots=True)
class EquityPreparationLimits:
    maximum_concurrency: int = 1
    maximum_requests_per_chunk: int = 3
    maximum_response_bytes_per_chunk: int = 8 * 1024 * 1024
    maximum_facts_per_chunk: int = 1_000
    maximum_requests_per_member: int = 160
    maximum_response_bytes_per_member: int = 2 * 1024 * 1024 * 1024
    maximum_account_requests: int = 64_000
    maximum_account_response_bytes: int = 512 * 1024 * 1024 * 1024
    lease_seconds: int = 300
    retry_initial_seconds: int = 15
    retry_maximum_seconds: int = 900

    def __post_init__(self) -> None:
        values = (
            self.maximum_concurrency,
            self.maximum_requests_per_chunk,
            self.maximum_response_bytes_per_chunk,
            self.maximum_facts_per_chunk,
            self.maximum_requests_per_member,
            self.maximum_response_bytes_per_member,
            self.maximum_account_requests,
            self.maximum_account_response_bytes,
            self.lease_seconds,
            self.retry_initial_seconds,
            self.retry_maximum_seconds,
        )
        if (
            any(type(value) is not int or value <= 0 for value in values)
            or self.maximum_concurrency > 16
            or self.maximum_requests_per_chunk > 10
            or self.maximum_facts_per_chunk > 1_000
            or self.retry_initial_seconds > self.retry_maximum_seconds
        ):
            raise ValueError("Advanced Equity preparation limits are invalid")


@dataclass(frozen=True, slots=True)
class EquityPreparationClaim:
    revision_id: UUID
    intent_id: UUID
    security_id: UUID | None
    member_kind: DesiredMemberKind
    ticker: str
    stage: EquityPreparationStage
    stage_order: int
    target: dict[str, object]
    watermark: dict[str, object]
    coverage: dict[str, object]
    attempts: int
    failure_streak: int
    lease_owner: UUID
    lease_epoch: int
    lease_until: datetime
    source_cohort_id: UUID
    instrument_id: int | None


@dataclass(frozen=True, slots=True)
class EquityPreparationFetchRequest:
    claim: EquityPreparationClaim
    checked_at: datetime
    limits: EquityPreparationLimits
    provider_reservations: tuple[
        ProviderRequestReservation, ...
    ] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "checked_at",
            _utc(self.checked_at, "preparation fetch checked_at"),
        )


@dataclass(frozen=True, slots=True)
class EquityPreparationChunk:
    stage: EquityPreparationStage
    watermark_after: dict[str, object]
    coverage_after: dict[str, object]
    source_lineage: dict[str, object]
    complete: bool
    provider_requests: int = 0
    response_bytes: int = 0
    bars: tuple[RealtimeEquityBarFact, ...] = ()
    reference_facts: tuple[ReferenceFact, ...] = ()

    def validate(
        self,
        request: EquityPreparationFetchRequest,
    ) -> None:
        if self.stage is not request.claim.stage:
            raise ValueError("preparation chunk changed its stage")
        if (
            type(self.complete) is not bool
            or self.provider_requests < 0
            or self.response_bytes < 0
            or self.provider_requests
            > request.limits.maximum_requests_per_chunk
            or self.response_bytes
            > request.limits.maximum_response_bytes_per_chunk
            or len(self.bars) + len(self.reference_facts)
            > request.limits.maximum_facts_per_chunk
            or not self.source_lineage
        ):
            raise ValueError("preparation chunk exceeds its bounded contract")
        if not self.complete and self.watermark_after == request.claim.watermark:
            raise ValueError("partial preparation chunk must advance its watermark")
        before = _interval_counts(request.claim.coverage)
        after = _interval_counts(self.coverage_after)
        if any(after.get(key, 0) < value for key, value in before.items()):
            raise ValueError("preparation coverage cannot move backwards")
        if any(
            fact.cohort_id != request.claim.source_cohort_id
            or fact.instrument_id != request.claim.instrument_id
            or fact.revision.security_id != request.claim.security_id
            or fact.revision.ticker != request.claim.ticker
            for fact in self.bars
        ):
            raise ValueError("preparation bar identity changed")
        reference_security_ids = {
            dict(fact.values).get("security_id")
            for fact in self.reference_facts
            if fact.relation is ReferenceFactRelation.SECURITY
        }
        if request.claim.security_id is None and (
            len(reference_security_ids) != 1
            or None in reference_security_ids
        ):
            raise ValueError(
                "unresolved preparation requires one security fact"
            )
        for fact in self.reference_facts:
            values = dict(fact.values)
            if (
                request.claim.security_id is not None
                and
                values.get("security_id") != request.claim.security_id
                and fact.relation
                not in {
                    ReferenceFactRelation.CORPORATE_ACTION_COVERAGE,
                }
            ):
                raise ValueError("preparation reference identity changed")


@dataclass(frozen=True, slots=True)
class EquityPreparationRunResult:
    status: str
    checked_at: datetime
    ticker: str | None = None
    stage: str | None = None
    attempts: int | None = None
    next_retry_at: datetime | None = None
    reason: str | None = None
    reconciliation_state: str | None = None


class EquityPreparationSource(Protocol):
    def fetch(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        ...


def _claim_from_row(row) -> EquityPreparationClaim:
    return EquityPreparationClaim(
        revision_id=UUID(str(row[0])),
        intent_id=UUID(str(row[1])),
        security_id=(
            None if row[2] is None else UUID(str(row[2]))
        ),
        member_kind=DesiredMemberKind(str(row[3])),
        ticker=str(row[4]),
        stage=EquityPreparationStage(str(row[5])),
        stage_order=int(row[6]),
        target=_json_object(row[7], "stored preparation target"),
        watermark=_json_object(row[8], "stored preparation watermark"),
        coverage=_json_object(row[9], "stored preparation coverage"),
        attempts=int(row[10]),
        failure_streak=int(row[11]),
        lease_owner=UUID(str(row[12])),
        lease_epoch=int(row[13]),
        lease_until=_utc(row[14], "stored preparation lease"),
        source_cohort_id=UUID(str(row[15])),
        instrument_id=(
            None if row[16] is None else int(row[16])
        ),
    )


def claim_equity_preparation_work_cursor(
    cursor,
    *,
    owner_id: UUID,
    checked_at: datetime,
    limits: EquityPreparationLimits,
) -> EquityPreparationClaim | None:
    checked = _utc(checked_at, "preparation claim checked_at")
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"{PREPARATION_RESOURCE_KEY}:concurrency",),
    )
    cursor.execute(
        "SELECT count(*) FROM rt.realtime_equity_preparation_work "
        "WHERE status='RUNNING' AND lease_until>%s",
        (checked,),
    )
    if int(cursor.fetchone()[0]) >= limits.maximum_concurrency:
        return None
    cursor.execute(
        "SELECT work.revision_id::text,work.intent_id::text,"
        "work.security_id::text,work.member_kind,work.ticker,"
        "work.stage,work.stage_order,work.target,work.watermark,"
        "work.coverage,work.attempts,work.failure_streak,"
        "revision.source_cohort_id::text,work.instrument_id "
        "FROM rt.realtime_equity_preparation_work AS work "
        "JOIN rt.realtime_desired_current AS current "
        "ON current.revision_id=work.revision_id "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "ON readiness.intent_id=work.intent_id "
        "JOIN rt.realtime_desired_revisions AS revision "
        "ON revision.revision_id=work.revision_id "
        "WHERE current.scope_key=%s AND readiness.status='WARMING' "
        "AND work.next_retry_at<=%s "
        "AND (work.status IN ('PENDING','RETRY') "
        "OR (work.status='RUNNING' AND work.lease_until<=%s)) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM rt.realtime_equity_preparation_work AS prior "
        "WHERE prior.revision_id=work.revision_id "
        "AND prior.intent_id=work.intent_id "
        "AND prior.stage_order<work.stage_order "
        "AND prior.status<>'COMPLETE'"
        ") "
        "ORDER BY work.stage_order,work.next_retry_at,"
        "work.ticker,work.intent_id "
        "FOR UPDATE OF work SKIP LOCKED LIMIT 1",
        (DESIRED_STATE_SCOPE, checked, checked),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    revision_id = UUID(str(row[0]))
    intent_id = UUID(str(row[1]))
    stage = str(row[5])
    lease_until = checked + timedelta(seconds=limits.lease_seconds)
    cursor.execute(
        "UPDATE rt.realtime_equity_preparation_work SET "
        "status='RUNNING',attempts=attempts+1,lease_owner=%s,"
        "lease_epoch=lease_epoch+1,lease_until=%s,"
        "terminal_reason=NULL,blocked_reason=NULL,"
        "updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "RETURNING attempts,lease_epoch",
        (
            str(owner_id),
            lease_until,
            str(revision_id),
            str(intent_id),
            stage,
        ),
    )
    claimed = cursor.fetchone()
    return _claim_from_row(
        (
            *row[:10],
            int(claimed[0]),
            int(row[11]),
            str(owner_id),
            int(claimed[1]),
            lease_until,
            str(row[12]),
            row[13],
        )
    )


def advanced_equity_live_priority_cursor(
    cursor,
    *,
    checked_at: datetime,
) -> bool:
    checked = _utc(checked_at, "live priority checked_at")
    cursor.execute(
        "SELECT EXISTS ("
        "SELECT 1 FROM rt.stream_sessions "
        "WHERE feed='STOCKS' "
        "AND status IN ("
        "'CONNECTING','AUTHENTICATING','SUBSCRIBING',"
        "'WARMING','LIVE','BACKOFF'"
        ") AND ended_at IS NULL "
        "AND COALESCE(heartbeat_at,started_at)>=%s"
        ")",
        (checked - timedelta(minutes=2),),
    )
    return bool(cursor.fetchone()[0])


def _preparation_provider_request_slots(
    stage: EquityPreparationStage,
) -> int:
    if stage is EquityPreparationStage.REFERENCE:
        return 3
    if stage in {
        EquityPreparationStage.ADJUSTED_DAILY,
        EquityPreparationStage.NATIVE_INTRADAY,
    }:
        return 1
    return 0


def _preparation_authorization_prefix(
    claim: EquityPreparationClaim,
) -> str:
    return (
        "equity-preparation:"
        f"{claim.revision_id}:{claim.intent_id}:"
    )


def reserve_equity_preparation_provider_requests_cursor(
    cursor,
    claim: EquityPreparationClaim,
    *,
    checked_at: datetime,
    limits: EquityPreparationLimits,
) -> tuple[ProviderRequestReservation, ...]:
    slots = _preparation_provider_request_slots(claim.stage)
    if slots == 0:
        return ()
    checked = _utc(
        checked_at,
        "preparation provider reservation checked_at",
    )
    prefix = _preparation_authorization_prefix(claim)
    authorization_reference = (
        f"{prefix}{claim.stage.value}:"
        f"{claim.lease_epoch}:{claim.attempts}"
    )
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"v2-preparation-member-budget:{claim.intent_id}",),
    )
    cursor.execute(
        "UPDATE rt.provider_request_ledger "
        "SET state='AMBIGUOUS',failure_code="
        "'PREPARATION_PROCESS_LOST' "
        "WHERE state='RESERVED' AND authorization_reference LIKE %s "
        "AND authorization_reference<>%s",
        (prefix + "%", authorization_reference),
    )
    session_date = date.fromisoformat(
        str(claim.target["effective_session_date"])
    )
    lane = (
        ProviderRequestLane.REFERENCE
        if claim.stage is EquityPreparationStage.REFERENCE
        else ProviderRequestLane.EQUITY_HISTORY
    )
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=session_date,
        authorization_reference=authorization_reference,
        provider_authorization_reference=(
            "MASSIVE_ADVANCED_ACCOUNT"
        ),
        expected_cost="EXISTING_SUBSCRIPTION",
        request_ceiling=limits.maximum_account_requests,
        decoded_byte_ceiling=(
            limits.maximum_account_response_bytes
        ),
        lanes=frozenset({lane}),
        authorized_until=OptionExchangeCalendar(
            "XNYS"
        ).session_close(session_date),
        policy_version="advanced-equity-preparation-budget-v1",
    )
    cursor.execute(
        "SELECT count(*),COALESCE(sum(reserved_response_bytes),0) "
        "FROM rt.provider_request_ledger "
        "WHERE authorization_reference LIKE %s",
        (prefix + "%",),
    )
    member_requests, member_bytes = (
        int(value) for value in cursor.fetchone()
    )
    request_ids = tuple(
        uuid5(
            NAMESPACE_URL,
            "advanced-equity-preparation-request:"
            f"{claim.revision_id}:{claim.intent_id}:"
            f"{claim.stage.value}:{claim.lease_epoch}:"
            f"{claim.attempts}:{slot}",
        )
        for slot in range(slots)
    )
    cursor.execute(
        "SELECT request_id::text FROM rt.provider_request_ledger "
        "WHERE request_id=ANY(%s::uuid[])",
        ([str(value) for value in request_ids],),
    )
    existing_ids = {
        UUID(str(row[0])) for row in cursor.fetchall()
    }
    new_slots = sum(
        request_id not in existing_ids for request_id in request_ids
    )
    new_bytes = (
        limits.maximum_response_bytes_per_chunk
        if new_slots
        else 0
    )
    if (
        member_requests + new_slots
        > limits.maximum_requests_per_member
        or member_bytes + new_bytes
        > limits.maximum_response_bytes_per_member
    ):
        raise ProviderBudgetExceeded(
            "preparation member provider budget is exhausted"
        )
    base_bytes, remainder = divmod(
        limits.maximum_response_bytes_per_chunk,
        slots,
    )
    reservations = []
    for slot, request_id in enumerate(request_ids):
        reserved_bytes = base_bytes + (1 if slot < remainder else 0)
        request_identity = {
            "contract": "advanced-equity-preparation-request-v1",
            "revision_id": claim.revision_id,
            "intent_id": claim.intent_id,
            "stage": claim.stage.value,
            "lease_epoch": claim.lease_epoch,
            "attempt": claim.attempts,
            "slot": slot,
            "watermark": claim.watermark,
        }
        base = ProviderRequestReservation(
            request_id=request_id,
            account_alias=scope.account_alias,
            lane=lane,
            endpoint_class=f"PREP_{claim.stage.value}"[:48],
            request_sha256=canonical_sha256(request_identity),
            reserved_at=checked,
            attempt=claim.attempts,
            cohort_id=claim.source_cohort_id,
        )
        reservation = scope.bind(
            base,
            reserved_response_bytes=reserved_bytes,
        )
        reserve_coordinated_provider_request_cursor(
            cursor,
            scope,
            base,
            reserved_response_bytes=reserved_bytes,
            account_wide=True,
        )
        reservations.append(reservation)
    return tuple(reservations)


def reconcile_equity_preparation_provider_requests_cursor(
    cursor,
    reservations: tuple[ProviderRequestReservation, ...],
    *,
    provider_requests: int,
    response_bytes: int,
    checked_at: datetime,
    success: bool,
    failure_code: str | None = None,
) -> None:
    if not reservations:
        if provider_requests or response_bytes:
            raise ValueError(
                "internal preparation stage reported provider effects"
            )
        return
    if (
        provider_requests < 0
        or provider_requests > len(reservations)
        or response_bytes < 0
    ):
        raise ValueError("preparation provider outcome exceeds reservation")
    checked = _utc(checked_at, "provider reconciliation checked_at")
    consumed = max(provider_requests, 1 if response_bytes else 0)
    base_bytes, remainder = divmod(
        response_bytes,
        consumed or 1,
    )
    normalized_failure = (
        (failure_code or "PREPARATION_REQUEST_FAILED")[:64]
    )
    for index, reservation in enumerate(reservations):
        used = index < consumed
        bytes_used = (
            base_bytes + (1 if index < remainder else 0)
            if used
            else 0
        )
        if used and success:
            completion = ProviderRequestCompletion(
                state=ProviderRequestState.COMPLETE,
                started_at=reservation.reserved_at,
                completed_at=checked,
                outcome=ProviderRequestOutcome.OK,
                response_bytes=bytes_used,
                pages_fetched=1,
                price_basis=PriceBasis.NOT_APPLICABLE,
                completeness=RequestCompleteness.COMPLETE,
            )
        else:
            code = (
                normalized_failure
                if used
                else "PREPARATION_RESERVATION_UNUSED"
            )
            outcome = (
                ProviderRequestOutcome.HTTP_429
                if "429" in code
                else ProviderRequestOutcome.HTTP_ERROR
                if "HTTP" in code
                else ProviderRequestOutcome.INVALID
            )
            completion = ProviderRequestCompletion(
                state=ProviderRequestState.FAILED,
                started_at=reservation.reserved_at if used else None,
                completed_at=checked,
                outcome=outcome,
                response_bytes=bytes_used,
                pages_fetched=1 if used else 0,
                price_basis=PriceBasis.NOT_APPLICABLE,
                completeness=(
                    RequestCompleteness.INCOMPLETE
                    if used
                    else RequestCompleteness.NOT_APPLICABLE
                ),
                failure_code=code,
            )
        finish_provider_request_cursor(
            cursor,
            reservation.request_id,
            completion,
        )


def _member_usage_cursor(
    cursor,
    claim: EquityPreparationClaim,
) -> tuple[int, int]:
    cursor.execute(
        "SELECT COALESCE(sum(provider_request_count),0),"
        "COALESCE(sum(response_bytes),0) "
        "FROM rt.realtime_equity_preparation_work "
        "WHERE revision_id=%s AND intent_id=%s",
        (
            str(claim.revision_id),
            str(claim.intent_id),
        ),
    )
    row = cursor.fetchone()
    return int(row[0]), int(row[1])


def _verify_running_claim_cursor(
    cursor,
    claim: EquityPreparationClaim,
) -> None:
    cursor.execute(
        "SELECT status,lease_owner::text,lease_epoch,watermark,coverage "
        "FROM rt.realtime_equity_preparation_work "
        "WHERE revision_id=%s AND intent_id=%s "
        "AND stage=%s FOR UPDATE",
        (
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage.value,
        ),
    )
    row = cursor.fetchone()
    if (
        row is None
        or row[0] != "RUNNING"
        or UUID(str(row[1])) != claim.lease_owner
        or int(row[2]) != claim.lease_epoch
        or _json_object(row[3], "stored preparation watermark")
        != claim.watermark
        or _json_object(row[4], "stored preparation coverage")
        != claim.coverage
    ):
        raise RuntimeError("Advanced Equity preparation claim was lost")


def complete_equity_preparation_chunk_cursor(
    cursor,
    request: EquityPreparationFetchRequest,
    chunk: EquityPreparationChunk,
) -> str | None:
    chunk.validate(request)
    claim = request.claim
    _verify_running_claim_cursor(cursor, claim)
    requests_used, bytes_used = _member_usage_cursor(cursor, claim)
    if (
        requests_used + chunk.provider_requests
        > request.limits.maximum_requests_per_member
        or bytes_used + chunk.response_bytes
        > request.limits.maximum_response_bytes_per_member
    ):
        raise EquityPreparationBlockedError(
            "MEMBER_PROVIDER_BUDGET_EXHAUSTED"
        )
    reconcile_equity_preparation_provider_requests_cursor(
        cursor,
        request.provider_reservations,
        provider_requests=chunk.provider_requests,
        response_bytes=chunk.response_bytes,
        checked_at=request.checked_at,
        success=True,
    )
    if chunk.reference_facts:
        persist_reference_facts_cursor(cursor, chunk.reference_facts)
    if (
        claim.security_id is None
        and claim.stage is EquityPreparationStage.REFERENCE
    ):
        security_fact = next(
            fact
            for fact in chunk.reference_facts
            if fact.relation is ReferenceFactRelation.SECURITY
        )
        values = dict(security_fact.values)
        binding = bind_desired_member_identity_cursor(
            cursor,
            claim.revision_id,
            ticker=claim.ticker,
            kind=claim.member_kind,
            security_id=values["security_id"],
            security_revision_id=values["revision_id"],
            checked_at=request.checked_at,
            source_lineage=chunk.source_lineage,
        )
        cursor.execute(
            "SELECT instrument_id FROM "
            "rt.realtime_desired_member_bindings "
            "WHERE intent_id=%s",
            (str(claim.intent_id),),
        )
        instrument_id = int(cursor.fetchone()[0])
        claim = replace(
            claim,
            security_id=binding.security_id,
            instrument_id=instrument_id,
        )
        request = replace(request, claim=claim)
    for offset in range(0, len(chunk.bars), 1_000):
        persist_realtime_equity_bars_cursor(
            cursor,
            chunk.bars[offset:offset + 1_000],
        )
    chunk_identity = {
        "contract": "advanced-equity-preparation-chunk-v1",
        "revision_id": claim.revision_id,
        "intent_id": claim.intent_id,
        "security_id": claim.security_id,
        "member_kind": claim.member_kind.value,
        "stage": claim.stage.value,
        "attempt": claim.attempts,
        "watermark_before": claim.watermark,
        "watermark_after": chunk.watermark_after,
        "coverage_after": chunk.coverage_after,
        "source_lineage": chunk.source_lineage,
        "provider_requests": chunk.provider_requests,
        "response_bytes": chunk.response_bytes,
        "bar_revision_ids": tuple(
            fact.revision.bar_revision_id for fact in chunk.bars
        ),
        "reference_fact_ids": tuple(
            (fact.relation.value, str(fact.identity[1]))
            for fact in chunk.reference_facts
        ),
    }
    chunk_sha256 = canonical_sha256(chunk_identity)
    chunk_id = uuid5(
        NAMESPACE_URL,
        f"advanced-equity-preparation-chunk:{chunk_sha256}",
    )
    cursor.execute(
        "INSERT INTO rt.realtime_equity_preparation_chunks ("
        "chunk_id,chunk_sha256,revision_id,intent_id,security_id,member_kind,"
        "stage,attempt,watermark_before,watermark_after,coverage_after,"
        "source_lineage,provider_requests,response_bytes,fact_count,"
        "committed_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (chunk_id) DO NOTHING",
        (
            str(chunk_id),
            chunk_sha256,
            str(claim.revision_id),
            str(claim.intent_id),
            (
                None
                if claim.security_id is None
                else str(claim.security_id)
            ),
            claim.member_kind.value,
            claim.stage.value,
            claim.attempts,
            Json(claim.watermark),
            Json(chunk.watermark_after),
            Json(chunk.coverage_after),
            Json(chunk.source_lineage),
            chunk.provider_requests,
            chunk.response_bytes,
            len(chunk.bars) + len(chunk.reference_facts),
            request.checked_at,
        ),
    )
    cursor.execute(
        "SELECT chunk_sha256 FROM rt.realtime_equity_preparation_chunks "
        "WHERE chunk_id=%s",
        (str(chunk_id),),
    )
    if tuple(cursor.fetchone() or ()) != (chunk_sha256,):
        raise ValueError("persisted preparation chunk identity conflicts")
    status = "COMPLETE" if chunk.complete else "PENDING"
    cursor.execute(
        "UPDATE rt.realtime_equity_preparation_work SET "
        "status=%s,watermark=%s,coverage=%s,source_lineage=%s,"
        "next_retry_at=%s,lease_owner=NULL,lease_until=NULL,"
        "terminal_reason=%s,blocked_reason=NULL,"
        "failure_streak=0,"
        "provider_request_count=provider_request_count+%s,"
        "response_bytes=response_bytes+%s,completed_at=%s,"
        "updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s",
        (
            status,
            Json(chunk.watermark_after),
            Json(chunk.coverage_after),
            Json(chunk.source_lineage),
            request.checked_at,
            "STAGE_COMPLETE" if chunk.complete else None,
            chunk.provider_requests,
            chunk.response_bytes,
            request.checked_at if chunk.complete else None,
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage.value,
            str(claim.lease_owner),
            claim.lease_epoch,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeError("Advanced Equity preparation completion lost its lease")
    if not (
        chunk.complete
        and claim.stage is EquityPreparationStage.TECHNICAL_WARMUP
    ):
        return None
    reconciliation = promote_prepared_desired_member_cursor(
        cursor,
        claim.revision_id,
        ticker=claim.ticker,
        kind=claim.member_kind,
        checked_at=request.checked_at,
    )
    return reconciliation.state


def retry_equity_preparation_work_cursor(
    cursor,
    claim: EquityPreparationClaim,
    *,
    checked_at: datetime,
    reason: str,
    limits: EquityPreparationLimits,
    provider_requests: int = 0,
    response_bytes: int = 0,
    provider_reservations: tuple[
        ProviderRequestReservation, ...
    ] = (),
) -> tuple[datetime | None, str | None]:
    checked = _utc(checked_at, "preparation retry checked_at")
    _verify_running_claim_cursor(cursor, claim)
    normalized = reason.strip()
    if not normalized:
        raise ValueError("preparation retry reason cannot be blank")
    if provider_requests < 0 or response_bytes < 0:
        raise ValueError("preparation retry counters cannot be negative")
    reconcile_equity_preparation_provider_requests_cursor(
        cursor,
        provider_reservations,
        provider_requests=provider_requests,
        response_bytes=response_bytes,
        checked_at=checked,
        success=False,
        failure_code=normalized,
    )
    requests_used, bytes_used = _member_usage_cursor(cursor, claim)
    if (
        requests_used + provider_requests
        > limits.maximum_requests_per_member
        or bytes_used + response_bytes
        > limits.maximum_response_bytes_per_member
    ):
        budget_reason = "MEMBER_PROVIDER_BUDGET_EXHAUSTED"
        cursor.execute(
            "UPDATE rt.realtime_equity_preparation_work SET "
            "status='BLOCKED',next_retry_at=%s,lease_owner=NULL,"
            "lease_until=NULL,terminal_reason=NULL,blocked_reason=%s,"
            "source_lineage=%s,"
            "failure_streak=failure_streak+1,"
            "provider_request_count=provider_request_count+%s,"
            "response_bytes=response_bytes+%s,completed_at=%s,"
            "updated_at=clock_timestamp() "
            "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
            "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s",
            (
                checked,
                budget_reason,
                Json({
                    "failure": normalized,
                    "failed_at": checked.isoformat(),
                    "budget_exhausted": True,
                }),
                provider_requests,
                response_bytes,
                checked,
                str(claim.revision_id),
                str(claim.intent_id),
                claim.stage.value,
                str(claim.lease_owner),
                claim.lease_epoch,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                "Advanced Equity preparation budget block lost its lease"
            )
        set_desired_member_preparation_cursor(
            cursor,
            claim.revision_id,
            ticker=claim.ticker,
            kind=claim.member_kind,
            status=MemberPreparationStatus.FAILED,
            reasons=(budget_reason,),
            checked_at=checked,
        )
        return None, "ACTIVATION_PENDING"
    delay = preparation_retry_delay_seconds(
        limits,
        failure_streak=claim.failure_streak,
    )
    next_retry = checked + timedelta(seconds=delay)
    cursor.execute(
        "UPDATE rt.realtime_equity_preparation_work SET "
        "status='RETRY',next_retry_at=%s,lease_owner=NULL,"
        "lease_until=NULL,terminal_reason=NULL,blocked_reason=NULL,"
        "source_lineage=%s,"
        "failure_streak=failure_streak+1,"
        "provider_request_count=provider_request_count+%s,"
        "response_bytes=response_bytes+%s,"
        "updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s",
        (
            next_retry,
            Json({
                "failure": normalized,
                "failed_at": checked.isoformat(),
            }),
            provider_requests,
            response_bytes,
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage.value,
            str(claim.lease_owner),
            claim.lease_epoch,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeError("Advanced Equity preparation retry lost its lease")
    return next_retry, None


def block_equity_preparation_work_cursor(
    cursor,
    claim: EquityPreparationClaim,
    *,
    checked_at: datetime,
    reason: str,
    provider_requests: int = 0,
    response_bytes: int = 0,
    provider_reservations: tuple[
        ProviderRequestReservation, ...
    ] = (),
) -> str:
    checked = _utc(checked_at, "preparation blocked checked_at")
    _verify_running_claim_cursor(cursor, claim)
    normalized = reason.strip()
    if not normalized:
        raise ValueError("preparation blocked reason cannot be blank")
    reconcile_equity_preparation_provider_requests_cursor(
        cursor,
        provider_reservations,
        provider_requests=provider_requests,
        response_bytes=response_bytes,
        checked_at=checked,
        success=False,
        failure_code=normalized,
    )
    cursor.execute(
        "UPDATE rt.realtime_equity_preparation_work SET "
        "status='BLOCKED',next_retry_at=%s,lease_owner=NULL,"
        "lease_until=NULL,terminal_reason=NULL,blocked_reason=%s,"
        "failure_streak=failure_streak+1,"
        "completed_at=%s,updated_at=clock_timestamp() "
        "WHERE revision_id=%s AND intent_id=%s AND stage=%s "
        "AND status='RUNNING' AND lease_owner=%s AND lease_epoch=%s",
        (
            checked,
            normalized,
            checked,
            str(claim.revision_id),
            str(claim.intent_id),
            claim.stage.value,
            str(claim.lease_owner),
            claim.lease_epoch,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeError("Advanced Equity preparation block lost its lease")
    set_desired_member_preparation_cursor(
        cursor,
        claim.revision_id,
        ticker=claim.ticker,
        kind=claim.member_kind,
        status=MemberPreparationStatus.FAILED,
        reasons=(normalized,),
        checked_at=checked,
    )
    return "ACTIVATION_PENDING"


def _coverage_with_count(
    current: Mapping[str, object],
    key: str,
    added: int,
) -> dict[str, object]:
    coverage = dict(current)
    counts = _interval_counts(current)
    counts[key] = counts.get(key, 0) + added
    coverage["available_intervals"] = counts
    return coverage


def _advanced_rest_facts(
    revisions,
    *,
    claim: EquityPreparationClaim,
) -> tuple[RealtimeEquityBarFact, ...]:
    if claim.security_id is None or claim.instrument_id is None:
        raise EquityPreparationUnavailableError(
            "RESOLVED_MEMBER_IDENTITY_REQUIRED"
        )
    return tuple(
        RealtimeEquityBarFact(
            cohort_id=claim.source_cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            instrument_id=claim.instrument_id,
            revision=replace(
                revision,
                quality_codes=tuple(dict.fromkeys((
                    *revision.quality_codes,
                    "EXPLICIT_ADVANCED_REST_REPAIR_V1",
                ))),
            ),
        )
        for revision in revisions
    )


class MassiveEquityPreparationSource:
    """Bounded Massive adapter; tests inject the protocol instead."""

    def __init__(
        self,
        client: PolygonEquityClient,
        *,
        adjusted_daily_sessions_per_chunk: int = 20,
    ) -> None:
        if not 1 <= adjusted_daily_sessions_per_chunk <= 50:
            raise ValueError("adjusted daily preparation chunk is invalid")
        self.client = client
        self.adjusted_daily_sessions_per_chunk = (
            adjusted_daily_sessions_per_chunk
        )

    def fetch(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        stage = request.claim.stage
        begin_chunk = getattr(self.client, "begin_chunk", None)
        if begin_chunk is not None:
            begin_chunk(
                maximum_requests=(
                    len(request.provider_reservations)
                    or request.limits.maximum_requests_per_chunk
                ),
                maximum_response_bytes=(
                    request.limits.maximum_response_bytes_per_chunk
                ),
                reservation_ids=tuple(
                    reservation.request_id
                    for reservation in request.provider_reservations
                ),
            )
        try:
            if stage is EquityPreparationStage.REFERENCE:
                chunk = self._reference(request)
            elif stage is EquityPreparationStage.ADJUSTED_DAILY:
                chunk = self._adjusted_daily(request)
            elif stage is EquityPreparationStage.NATIVE_INTRADAY:
                chunk = self._native_intraday(request)
            else:
                raise ValueError(
                    "provider source cannot execute an internal stage"
                )
            chunk_metrics = getattr(self.client, "chunk_metrics", None)
            if chunk_metrics is not None:
                requests, response_bytes = chunk_metrics()
                chunk = replace(
                    chunk,
                    provider_requests=requests,
                    response_bytes=response_bytes,
                )
            return chunk
        except EquityPreparationBlockedError as error:
            requests = response_bytes = 0
            chunk_metrics = getattr(self.client, "chunk_metrics", None)
            if chunk_metrics is not None:
                requests, response_bytes = chunk_metrics()
            raise EquityPreparationBlockedError(
                str(error),
                provider_requests=requests,
                response_bytes=response_bytes,
            ) from error
        except EquityPreparationSourceError:
            raise
        except Exception as error:
            requests = response_bytes = 0
            chunk_metrics = getattr(self.client, "chunk_metrics", None)
            if chunk_metrics is not None:
                requests, response_bytes = chunk_metrics()
            raise EquityPreparationSourceError(
                f"{stage.value}_SOURCE_REQUEST_FAILED:{type(error).__name__}",
                provider_requests=requests,
                response_bytes=response_bytes,
            ) from error

    def _reference(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        claim = request.claim

        def no_options(*_args):
            raise AssertionError("Equity preparation cannot request Options data")

        adapter = MassiveReferenceAdapter(
            fetch_security=(
                lambda ticker, as_of: self.client.fetch_ticker_overview(
                    ticker,
                    as_of_date=as_of,
                )
            ),
            fetch_splits=(
                lambda ticker, start, end: tuple(
                    row
                    for row in self.client.fetch_splits(start, end)
                    if str(row.get("ticker") or "").upper() == ticker
                )
            ),
            fetch_dividends=(
                lambda ticker, start, end: tuple(
                    row
                    for row in self.client.fetch_dividends(start, end)
                    if str(row.get("ticker") or "").upper() == ticker
                )
            ),
            fetch_option_catalog=no_options,
            fetch_open_interest=no_options,
        )
        session_date = date.fromisoformat(
            str(claim.target["history_through_session"])
        )
        created = request.checked_at
        generation = adapter.collect(
            ReferenceSeedRequest(
                universe=UniverseManifest(
                    version=f"advanced-equity-preparation-{claim.ticker}-v1",
                    equity_tickers=(claim.ticker,),
                    option_underlyings=(),
                ),
                session_date=session_date,
                created_at=created,
                effective_at=created,
                valid_until=created + timedelta(days=7),
            )
        )
        security = next(
            (
                fact
                for fact in generation.facts
                if fact.relation is ReferenceFactRelation.SECURITY
            ),
            None,
        )
        if (
            security is None
            or (
                claim.security_id is not None
                and dict(security.values)["security_id"]
                != claim.security_id
            )
        ):
            raise EquityPreparationBlockedError(
                "STORED_SECURITY_IDENTITY_CHANGED"
            )
        coverage_relations = {
            fact.relation
            for fact in generation.facts
        }
        coverage = {
            "sector_ready": bool(dict(security.values).get("sector")),
            "corporate_actions_ready": (
                ReferenceFactRelation.CORPORATE_ACTION_COVERAGE
                in coverage_relations
            ),
            "available_intervals": {},
        }
        lineage = {
            "provider": "MASSIVE",
            "endpoint_classes": [
                "TICKER_OVERVIEW",
                "SPLITS",
                "DIVIDENDS",
            ],
            "request_sha256": canonical_sha256({
                "ticker": claim.ticker,
                "session_date": session_date,
                "window_start": claim.target["window_start"],
                "window_end": claim.target["window_end"],
            }),
            "response_sha256": generation.generation_sha256,
        }
        return EquityPreparationChunk(
            stage=claim.stage,
            watermark_after={"complete": True},
            coverage_after=coverage,
            source_lineage=lineage,
            complete=True,
            provider_requests=generation.provider_requests,
            response_bytes=generation.response_bytes,
            reference_facts=generation.facts,
        )

    def _adjusted_daily(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        claim = request.claim
        interval_sessions = _json_object(
            claim.target["interval_sessions"],
            "adjusted interval sessions",
        )
        intervals = tuple(interval_sessions)
        interval_index = int(claim.watermark.get("interval_index", 0))
        session_index = int(claim.watermark.get("session_index", 0))
        if interval_index >= len(intervals):
            raise EquityPreparationBlockedError(
                "ADJUSTED_HISTORY_WATERMARK_EXHAUSTED"
            )
        interval = intervals[interval_index]
        sessions = tuple(
            date.fromisoformat(str(value))
            for value in interval_sessions[interval]
        )
        selected = sessions[
            session_index:
            session_index + self.adjusted_daily_sessions_per_chunk
        ]
        if not selected:
            raise EquityPreparationBlockedError(
                "ADJUSTED_HISTORY_WATERMARK_EXHAUSTED"
            )
        request_start = selected[0]
        request_end = selected[-1]
        if interval == "1wk":
            first_iso = selected[0].isocalendar()
            last_iso = selected[-1].isocalendar()
            request_start = date.fromisocalendar(
                first_iso.year,
                first_iso.week,
                7,
            )
            request_end = date.fromisocalendar(
                last_iso.year,
                last_iso.week,
                7,
            )
        elif interval == "1mo":
            request_start = selected[0].replace(day=1)
        rows = self.client.fetch_native_bars(
            claim.ticker,
            interval,
            request_start,
            request_end,
            adjusted=True,
        )
        segment = uuid5(
            NAMESPACE_URL,
            "advanced-equity-preparation:"
            f"{claim.revision_id}:{claim.ticker}:{interval}:adjusted:"
            f"{selected[0]}:{selected[-1]}",
        )
        revisions = normalize_native_bars(
            claim.security_id,
            claim.ticker,
            interval,
            rows,
            observed_at=request.checked_at,
            adjusted=True,
            availability_mode=(
                BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
            ),
            ingestion_segment_id=segment,
        )
        actual = {revision.session_date for revision in revisions}
        if actual != set(selected):
            raise EquityPreparationUnavailableError(
                "ADJUSTED_HISTORY_COVERAGE_INCOMPLETE"
            )
        next_session_index = session_index + len(selected)
        next_interval_index = interval_index
        if next_session_index == len(sessions):
            next_interval_index += 1
            next_session_index = 0
        response_sha = canonical_sha256(rows)
        return EquityPreparationChunk(
            stage=claim.stage,
            watermark_after={
                "interval_index": next_interval_index,
                "session_index": next_session_index,
            },
            coverage_after=_coverage_with_count(
                claim.coverage,
                f"{interval}:ADJUSTED",
                len(revisions),
            ),
            source_lineage={
                "provider": "MASSIVE",
                "endpoint_class": (
                    f"STOCKS_AGGREGATES_{interval.upper()}"
                ),
                "request_sha256": canonical_sha256({
                    "ticker": claim.ticker,
                    "interval": interval,
                    "start": request_start,
                    "end": request_end,
                    "adjusted": True,
                }),
                "response_sha256": response_sha,
            },
            complete=next_interval_index == len(intervals),
            provider_requests=1,
            response_bytes=len(
                json.dumps(rows, sort_keys=True).encode("utf-8")
            ),
            bars=_advanced_rest_facts(revisions, claim=claim),
        )

    def _native_intraday(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        claim = request.claim
        interval_sessions = _json_object(
            claim.target["interval_sessions"],
            "native interval sessions",
        )
        intervals = tuple(interval_sessions)
        interval_index = int(claim.watermark.get("interval_index", 0))
        session_index = int(claim.watermark.get("session_index", 0))
        if interval_index >= len(intervals):
            raise EquityPreparationBlockedError(
                "NATIVE_INTRADAY_WATERMARK_EXHAUSTED"
            )
        interval = intervals[interval_index]
        sessions = tuple(
            date.fromisoformat(str(value))
            for value in interval_sessions[interval]
        )
        session = sessions[session_index]
        rows = self.client.fetch_native_bars(
            claim.ticker,
            interval,
            session,
            session,
            adjusted=False,
        )
        segment = uuid5(
            NAMESPACE_URL,
            "advanced-equity-preparation:"
            f"{claim.revision_id}:{claim.ticker}:{interval}:{session}",
        )
        revisions = normalize_native_bars(
            claim.security_id,
            claim.ticker,
            interval,
            rows,
            observed_at=request.checked_at,
            adjusted=False,
            availability_mode=(
                BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
            ),
            ingestion_segment_id=segment,
        )
        if not revisions or {
            revision.session_date for revision in revisions
        } != {session}:
            raise EquityPreparationUnavailableError(
                "NATIVE_INTRADAY_COVERAGE_INCOMPLETE"
            )
        next_session_index = session_index + 1
        next_interval_index = interval_index
        if next_session_index == len(sessions):
            next_interval_index += 1
            next_session_index = 0
        complete = next_interval_index == len(intervals)
        return EquityPreparationChunk(
            stage=claim.stage,
            watermark_after={
                "interval_index": next_interval_index,
                "session_index": next_session_index,
            },
            coverage_after=_coverage_with_count(
                claim.coverage,
                f"{interval}:RAW_UNADJUSTED",
                len(revisions),
            ),
            source_lineage={
                "provider": "MASSIVE",
                "endpoint_class": (
                    f"STOCKS_AGGREGATES_{interval.upper()}"
                ),
                "request_sha256": canonical_sha256({
                    "ticker": claim.ticker,
                    "interval": interval,
                    "session": session,
                    "adjusted": False,
                }),
                "response_sha256": canonical_sha256(rows),
            },
            complete=complete,
            provider_requests=1,
            response_bytes=len(
                json.dumps(rows, sort_keys=True).encode("utf-8")
            ),
            bars=_advanced_rest_facts(revisions, claim=claim),
        )


class BoundedPolygonEquityPreparationClient(PolygonEquityClient):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._chunk_maximum_requests = 0
        self._chunk_maximum_response_bytes = 0
        self._chunk_provider_requests = 0
        self._chunk_response_bytes = 0
        self._chunk_reservation_ids: tuple[UUID, ...] = ()

    def begin_chunk(
        self,
        *,
        maximum_requests: int,
        maximum_response_bytes: int,
        reservation_ids: tuple[UUID, ...] = (),
    ) -> None:
        if maximum_requests <= 0 or maximum_response_bytes <= 0:
            raise ValueError("provider chunk bounds must be positive")
        self._chunk_maximum_requests = maximum_requests
        self._chunk_maximum_response_bytes = maximum_response_bytes
        self._chunk_provider_requests = 0
        self._chunk_response_bytes = 0
        self._chunk_reservation_ids = tuple(reservation_ids)
        if (
            self._chunk_reservation_ids
            and len(self._chunk_reservation_ids) < maximum_requests
        ):
            raise ValueError(
                "provider reservations do not cover the chunk request bound"
            )

    def _request(self, url, params):
        if (
            self._chunk_provider_requests
            >= self._chunk_maximum_requests
        ):
            raise RuntimeError(
                "preparation provider request ceiling reached"
            )
        if (
            self._chunk_reservation_ids
            and self._chunk_provider_requests
            >= len(self._chunk_reservation_ids)
        ):
            raise RuntimeError(
                "preparation provider reservation slots exhausted"
            )
        self._chunk_provider_requests += 1
        try:
            payload = super()._request(url, params)
        except Exception as error:
            response = getattr(error, "response", None)
            content = getattr(response, "content", b"")
            self._chunk_response_bytes += len(content or b"")
            if (
                self._chunk_response_bytes
                > self._chunk_maximum_response_bytes
            ):
                raise RuntimeError(
                    "preparation provider response-byte ceiling reached"
                ) from error
            raise
        self._chunk_response_bytes += len(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        if (
            self._chunk_response_bytes
            > self._chunk_maximum_response_bytes
        ):
            raise RuntimeError(
                "preparation provider response-byte ceiling reached"
            )
        return payload

    def chunk_metrics(self) -> tuple[int, int]:
        return (
            self._chunk_provider_requests,
            self._chunk_response_bytes,
        )


class PreparationTransactionProvider(Protocol):
    def transaction(
        self,
        role: str,
        *,
        read_only: bool = False,
        dict_cursor: bool = False,
    ) -> AbstractContextManager:
        ...


class AdvancedEquityPreparationResident:
    def __init__(
        self,
        provider: PreparationTransactionProvider,
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
        self.planner = AdvancedEquityLifecyclePlanner(
            calendar=self.calendar,
            clock=clock,
        )

    def run_once(self) -> EquityPreparationRunResult:
        checked = _utc(self.clock(), "preparation resident clock")
        lifecycle = self.planner.plan(
            drain_seconds=30,
            finality_delay_seconds=5,
            completion_budget_seconds=300,
        )
        if lifecycle.phase not in {
            AdvancedEquityLifecyclePhase.IDLE,
            AdvancedEquityLifecyclePhase.BEFORE_OPEN,
        }:
            return EquityPreparationRunResult(
                "LIVE_PRIORITY",
                checked,
                reason=f"LIFECYCLE_{lifecycle.phase.value}",
            )
        with self.provider.transaction(APP_ROLE) as unit:
            if advanced_equity_live_priority_cursor(
                unit.cursor,
                checked_at=checked,
            ):
                claim = None
                live_priority = True
            else:
                claim = claim_equity_preparation_work_cursor(
                    unit.cursor,
                    owner_id=self.owner_id,
                    checked_at=checked,
                    limits=self.limits,
                )
                live_priority = False
        if live_priority:
            return EquityPreparationRunResult(
                "LIVE_PRIORITY",
                checked,
                reason="ACTIVE_STOCKS_INGRESS",
            )
        if claim is None:
            return EquityPreparationRunResult(
                "IDLE",
                checked,
                reason="NO_DUE_PREPARATION_WORK",
            )
        try:
            with self.provider.transaction(APP_ROLE) as unit:
                provider_reservations = (
                    reserve_equity_preparation_provider_requests_cursor(
                        unit.cursor,
                        claim,
                        checked_at=checked,
                        limits=self.limits,
                    )
                )
        except ProviderBudgetExceeded as error:
            budget_reason = (
                "MEMBER_PROVIDER_BUDGET_EXHAUSTED"
                if "member" in str(error).lower()
                else "ACCOUNT_PROVIDER_BUDGET_EXHAUSTED"
            )
            with self.provider.transaction(APP_ROLE) as unit:
                reconciliation = block_equity_preparation_work_cursor(
                    unit.cursor,
                    claim,
                    checked_at=checked,
                    reason=budget_reason,
                )
            return EquityPreparationRunResult(
                "BLOCKED",
                checked,
                ticker=claim.ticker,
                stage=claim.stage.value,
                attempts=claim.attempts,
                reason=budget_reason,
                reconciliation_state=reconciliation,
            )
        request = EquityPreparationFetchRequest(
            claim,
            checked,
            self.limits,
            provider_reservations,
        )
        try:
            if claim.stage is EquityPreparationStage.DERIVED_INTERVALS:
                chunk = self._derive(request)
            elif claim.stage is EquityPreparationStage.TECHNICAL_WARMUP:
                chunk = self._warmup(request)
            else:
                chunk = self.source.fetch(request)
            with self.provider.transaction(APP_ROLE) as unit:
                reconciliation = complete_equity_preparation_chunk_cursor(
                    unit.cursor,
                    request,
                    chunk,
                )
            return EquityPreparationRunResult(
                "COMPLETE" if chunk.complete else "PROGRESSED",
                checked,
                ticker=claim.ticker,
                stage=claim.stage.value,
                attempts=claim.attempts,
                reconciliation_state=reconciliation,
            )
        except EquityPreparationSourceError as error:
            with self.provider.transaction(APP_ROLE) as unit:
                (
                    next_retry,
                    reconciliation,
                ) = retry_equity_preparation_work_cursor(
                    unit.cursor,
                    claim,
                    checked_at=checked,
                    reason=str(error),
                    limits=self.limits,
                    provider_requests=error.provider_requests,
                    response_bytes=error.response_bytes,
                    provider_reservations=(
                        request.provider_reservations
                    ),
                )
            if next_retry is None:
                return EquityPreparationRunResult(
                    "BLOCKED",
                    checked,
                    ticker=claim.ticker,
                    stage=claim.stage.value,
                    attempts=claim.attempts,
                    reason="MEMBER_PROVIDER_BUDGET_EXHAUSTED",
                    reconciliation_state=reconciliation,
                )
            return EquityPreparationRunResult(
                "RETRY",
                checked,
                ticker=claim.ticker,
                stage=claim.stage.value,
                attempts=claim.attempts,
                next_retry_at=next_retry,
                reason=str(error),
            )
        except EquityPreparationBlockedError as error:
            with self.provider.transaction(APP_ROLE) as unit:
                reconciliation = block_equity_preparation_work_cursor(
                    unit.cursor,
                    claim,
                    checked_at=checked,
                    reason=str(error),
                    provider_requests=error.provider_requests,
                    response_bytes=error.response_bytes,
                    provider_reservations=(
                        request.provider_reservations
                    ),
                )
            return EquityPreparationRunResult(
                "BLOCKED",
                checked,
                ticker=claim.ticker,
                stage=claim.stage.value,
                attempts=claim.attempts,
                reason=str(error),
                reconciliation_state=reconciliation,
            )

    def _derive(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        claim = request.claim
        if claim.security_id is None or claim.instrument_id is None:
            raise EquityPreparationUnavailableError(
                "RESOLVED_MEMBER_IDENTITY_REQUIRED"
            )
        targets = tuple(str(value) for value in claim.target["target_intervals"])
        sessions = tuple(
            date.fromisoformat(str(value))
            for value in claim.target["source_sessions"]
        )
        target_index = int(claim.watermark.get("target_index", 0))
        session_index = int(claim.watermark.get("session_index", 0))
        if target_index >= len(targets):
            raise EquityPreparationBlockedError(
                "DERIVATION_WATERMARK_EXHAUSTED"
            )
        target = targets[target_index]
        session = sessions[session_index]
        with self.provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
        ) as unit:
            sources = load_current_five_minute_facts_cursor(
                unit.cursor,
                cohort_id=claim.source_cohort_id,
                instrument_id=claim.instrument_id,
                session_date=session,
                checked_at=request.checked_at,
            )
            if not sources:
                raise EquityPreparationUnavailableError(
                    "DERIVATION_SOURCE_UNAVAILABLE"
                )
            security = load_derivation_security_cursor(
                unit.cursor,
                instrument_id=claim.instrument_id,
                window_end=max(
                    fact.revision.bar_end for fact in sources
                ),
                as_of=request.checked_at,
            )
        source_sha256 = canonical_sha256(tuple(
            fact.revision.bar_revision_id for fact in sources
        ))
        facts = derive_realtime_interval_facts(
            security,
            sources,
            target_interval=target,
            observed_at=request.checked_at,
            ingestion_segment_id=uuid5(
                NAMESPACE_URL,
                "advanced-equity-preparation-derivation:"
                f"{claim.revision_id}:{claim.ticker}:{target}:"
                f"{session}:{source_sha256}",
            ),
        )
        if not facts:
            raise EquityPreparationUnavailableError(
                "DERIVATION_OUTPUT_UNAVAILABLE"
            )
        next_session_index = session_index + 1
        next_target_index = target_index
        if next_session_index == len(sessions):
            next_target_index += 1
            next_session_index = 0
        complete = next_target_index == len(targets)
        return EquityPreparationChunk(
            stage=claim.stage,
            watermark_after={
                "target_index": next_target_index,
                "session_index": next_session_index,
            },
            coverage_after=_coverage_with_count(
                claim.coverage,
                f"{target}:RAW_UNADJUSTED",
                len(facts),
            ),
            source_lineage={
                "provider": "INTERNAL",
                "owner": "equity_interval_derivation",
                "request_sha256": canonical_sha256({
                    "target": target,
                    "session": session,
                    "source_sha256": source_sha256,
                }),
                "response_sha256": canonical_sha256(tuple(
                    fact.revision.bar_revision_id for fact in facts
                )),
            },
            complete=complete,
            bars=facts,
        )

    def _warmup(
        self,
        request: EquityPreparationFetchRequest,
    ) -> EquityPreparationChunk:
        claim = request.claim
        with self.provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT stage,status,coverage "
                "FROM rt.realtime_equity_preparation_work "
                "WHERE revision_id=%s AND intent_id=%s "
                "ORDER BY stage_order",
                (
                    str(claim.revision_id),
                    str(claim.intent_id),
                ),
            )
            stages = {
                str(row[0]): (
                    str(row[1]),
                    _json_object(row[2], "stored stage coverage"),
                )
                for row in unit.cursor.fetchall()
            }
        if any(
            stages.get(stage, ("MISSING", {}))[0] != "COMPLETE"
            for stage in PREPARATION_STAGES[:-1]
        ):
            raise EquityPreparationUnavailableError(
                "PREREQUISITE_STAGE_INCOMPLETE"
            )
        reference = stages["REFERENCE"][1]
        daily = _interval_counts(stages["ADJUSTED_DAILY"][1])
        derived = _interval_counts(stages["DERIVED_INTERVALS"][1])
        required_daily = int(
            claim.target["required_adjusted_daily_bars"]
        )
        required_hourly = int(
            claim.target["required_derived_hourly_bars"]
        )
        required_adjusted = _json_object(
            claim.target.get("required_adjusted_intervals", {}),
            "required adjusted intervals",
        )
        reasons = []
        if not reference.get("sector_ready"):
            reasons.append("SECTOR_REFERENCE_UNAVAILABLE")
        if not reference.get("corporate_actions_ready"):
            reasons.append("CORPORATE_ACTION_COVERAGE_UNAVAILABLE")
        if daily.get("1d:ADJUSTED", 0) < required_daily:
            reasons.append("DAILY_TECHNICAL_WARMUP_INCOMPLETE")
        for interval, required in required_adjusted.items():
            if daily.get(interval, 0) < int(required):
                reasons.append(
                    f"{interval}:PORTAL_HISTORY_INCOMPLETE"
                )
        if derived.get("1h:RAW_UNADJUSTED", 0) < required_hourly:
            reasons.append("HOURLY_TECHNICAL_WARMUP_INCOMPLETE")
        if reasons:
            raise EquityPreparationUnavailableError(",".join(reasons))
        coverage = {
            "available_intervals": {
                **_interval_counts(stages["NATIVE_INTRADAY"][1]),
                **daily,
                **derived,
            },
            "sector_ready": True,
            "corporate_actions_ready": True,
            "declared_products": claim.target["declared_products"],
            "technical_warmup": {
                "adjusted_daily": daily["1d:ADJUSTED"],
                "derived_hourly": derived["1h:RAW_UNADJUSTED"],
            },
        }
        return EquityPreparationChunk(
            stage=claim.stage,
            watermark_after={"complete": True},
            coverage_after=coverage,
            source_lineage={
                "provider": "INTERNAL",
                "owner": "advanced_equity_preparation",
                "request_sha256": canonical_sha256({
                    "revision_id": claim.revision_id,
                    "security_id": claim.security_id,
                    "target": claim.target,
                }),
                "response_sha256": canonical_sha256(coverage),
            },
            complete=True,
        )


def load_equity_preparation_status_cursor(
    cursor,
    *,
    active_on: date,
    ticker: str | None = None,
) -> dict[str, object] | None:
    normalized_ticker = None if ticker is None else ticker.strip().upper()
    cursor.execute(
        "SELECT current.revision_id::text,"
        "revision.effective_session_date,revision.revision_sha256,"
        "revision.source_cohort_id::text "
        "FROM rt.realtime_desired_current AS current "
        "JOIN rt.realtime_desired_revisions AS revision "
        "ON revision.revision_id=current.revision_id "
        "WHERE current.scope_key=%s",
        (DESIRED_STATE_SCOPE,),
    )
    current = cursor.fetchone()
    if current is None:
        return None
    revision_id = UUID(str(current[0]))
    cursor.execute(
        "SELECT binding.security_id::text,intent.ticker,"
        "intent.member_kind,intent.enabled,readiness.status,readiness.reasons,"
        "readiness.checked_at,work.stage,work.stage_order,work.status,"
        "work.watermark,work.coverage,work.source_lineage,work.attempts,"
        "work.failure_streak,work.next_retry_at,"
        "work.terminal_reason,work.blocked_reason,"
        "work.provider_request_count,work.response_bytes "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "ON readiness.intent_id=intent.intent_id "
        "LEFT JOIN rt.realtime_desired_member_bindings AS binding "
        "ON binding.intent_id=intent.intent_id "
        "LEFT JOIN rt.realtime_equity_preparation_work AS work "
        "ON work.revision_id=intent.revision_id "
        "AND work.intent_id=intent.intent_id "
        "WHERE intent.revision_id=%s "
        "AND intent.member_kind IN ('EQUITY','SECTOR_ETF') "
        + ("AND intent.ticker=%s " if normalized_ticker else "")
        + "ORDER BY intent.ticker,intent.member_kind,work.stage_order",
        (
            (str(revision_id), normalized_ticker)
            if normalized_ticker
            else (str(revision_id),)
        ),
    )
    rows = tuple(cursor.fetchall())
    if normalized_ticker and not rows:
        return None
    members: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (str(row[1]), str(row[2]))
        member = members.setdefault(key, {
            "security_id": (
                None if row[0] is None else str(row[0])
            ),
            "ticker": key[0],
            "kind": key[1],
            "enabled": bool(row[3]),
            "readiness": str(row[4]),
            "reasons": [str(value) for value in row[5]],
            "checked_at": _utc(
                row[6],
                "stored member readiness checked_at",
            ).isoformat(),
            "available_intervals": {},
            "stored_availability": [],
            "reference_availability": None,
            "stages": [],
        })
        if row[7] is None:
            continue
        coverage = _json_object(row[11], "stored preparation coverage")
        available = member["available_intervals"]
        for interval, count in _interval_counts(coverage).items():
            available[interval] = max(available.get(interval, 0), count)
        member["stages"].append({
            "stage": str(row[7]),
            "stage_order": int(row[8]),
            "status": str(row[9]),
            "watermark": _json_object(
                row[10],
                "stored preparation watermark",
            ),
            "coverage": coverage,
            "source_lineage": _json_object(
                row[12],
                "stored preparation lineage",
            ),
            "attempts": int(row[13]),
            "failure_streak": int(row[14]),
            "next_retry_at": _utc(
                row[15],
                "stored preparation retry",
            ).isoformat(),
            "terminal_reason": row[16],
            "blocked_reason": row[17],
            "provider_requests": int(row[18]),
            "response_bytes": int(row[19]),
        })
    security_ids = tuple(sorted({
        member["security_id"]
        for member in members.values()
        if member["security_id"] is not None
    }))
    if security_ids:
        cursor.execute(
            "WITH selected AS ("
            "SELECT revision.ticker,current.interval,"
            "current.adjustment_basis,revision.bar_revision_id::text,"
            "revision.bar_start,revision.bar_end,revision.open_price,"
            "revision.high_price,revision.low_price,"
            "revision.close_price,revision.volume,"
            "revision.source_kind,revision.source_revision_ids,"
            "revision.ingestion_segment_id::text,"
            "revision.payload_sha256,revision.observed_at,"
            "count(*) OVER (PARTITION BY revision.ticker,"
            "current.interval,current.adjustment_basis) AS retained_count,"
            "min(revision.bar_start) OVER (PARTITION BY revision.ticker,"
            "current.interval,current.adjustment_basis) AS first_market_time,"
            "max(revision.bar_end) OVER (PARTITION BY revision.ticker,"
            "current.interval,current.adjustment_basis) AS last_market_time "
            "FROM rt.bar_current AS current "
            "JOIN rt.bar_revisions AS revision "
            "ON revision.bar_revision_id=current.bar_revision_id "
            "AND revision.observed_at=current.observed_at "
            "AND revision.cohort_id=current.cohort_id "
            "AND revision.provider_tier=current.provider_tier "
            "AND revision.instrument_id=current.instrument_id "
            "AND revision.interval=current.interval "
            "WHERE current.cohort_id=%s "
            "AND revision.security_id=ANY(%s::uuid[]) "
            "AND revision.is_final"
            ") SELECT DISTINCT ON (ticker,interval,adjustment_basis) "
            "ticker,interval,adjustment_basis,retained_count,"
            "first_market_time,last_market_time,bar_revision_id,"
            "bar_start,bar_end,open_price,high_price,low_price,"
            "close_price,volume,source_kind,source_revision_ids,"
            "ingestion_segment_id,payload_sha256 "
            "FROM selected ORDER BY ticker,interval,adjustment_basis,"
            "bar_end DESC,observed_at DESC,bar_revision_id DESC",
            (str(current[3]), list(security_ids)),
        )
        for availability in cursor.fetchall():
            summary = {
                "interval": str(availability[1]),
                "adjustment_basis": str(availability[2]),
                "count": int(availability[3]),
                "first_market_time": _utc(
                    availability[4],
                    "stored availability first market time",
                ).isoformat(),
                "last_market_time": _utc(
                    availability[5],
                    "stored availability last market time",
                ).isoformat(),
                "latest": {
                    "bar_revision_id": str(availability[6]),
                    "bar_start": _utc(
                        availability[7],
                        "stored availability bar start",
                    ).isoformat(),
                    "bar_end": _utc(
                        availability[8],
                        "stored availability bar end",
                    ).isoformat(),
                    "open": str(availability[9]),
                    "high": str(availability[10]),
                    "low": str(availability[11]),
                    "close": str(availability[12]),
                    "volume": str(availability[13]),
                    "payload_sha256": str(availability[17]),
                },
                "source_lineage": {
                    "source_kind": str(availability[14]),
                    "source_revision_ids": [
                        str(value)
                        for value in availability[15]
                    ],
                    "ingestion_segment_id": (
                        None
                        if availability[16] is None
                        else str(availability[16])
                    ),
                },
            }
            for key, member in members.items():
                if key[0] == str(availability[0]):
                    member["stored_availability"].append(summary)
        cursor.execute(
            "SELECT DISTINCT ON (revision.security_id) "
            "revision.security_id::text,revision.revision_id::text,"
            "revision.ticker,revision.sector,revision.industry,"
            "revision.observed_at,revision.payload_sha256 "
            "FROM ref.security_revisions AS revision "
            "WHERE revision.security_id=ANY(%s::uuid[]) "
            "ORDER BY revision.security_id,revision.effective_from DESC,"
            "revision.observed_at DESC,revision.revision_id DESC",
            (list(security_ids),),
        )
        reference_by_security = {
            str(row[0]): {
                "security_revision_id": str(row[1]),
                "ticker": str(row[2]),
                "sector": row[3],
                "industry": row[4],
                "observed_at": _utc(
                    row[5],
                    "stored reference observed_at",
                ).isoformat(),
                "payload_sha256": str(row[6]),
            }
            for row in cursor.fetchall()
        }
        for member in members.values():
            member["reference_availability"] = (
                reference_by_security.get(member["security_id"])
            )
    cursor.execute(
        "SELECT snapshot.revision_id::text,snapshot.session_date,"
        "snapshot.manifest_id::text,snapshot.cohort_id::text,"
        "snapshot.snapshot_sha256 "
        "FROM rt.realtime_desired_session_snapshots AS snapshot "
        "WHERE snapshot.session_date<=%s "
        "ORDER BY snapshot.session_date DESC,snapshot.materialized_at DESC "
        "LIMIT 1",
        (active_on,),
    )
    active = cursor.fetchone()
    cursor.execute(
        "SELECT manifest_id::text,cohort_id::text,snapshot_sha256 "
        "FROM rt.realtime_desired_session_snapshots "
        "WHERE revision_id=%s",
        (str(revision_id),),
    )
    intended = cursor.fetchone()
    member_rows = list(members.values())
    target9 = set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    handoff_members = []
    for member in member_rows:
        if (
            member["readiness"] == "READY"
            and member["ticker"] not in target9
        ):
            member["publication_state"] = (
                "PREPARATION_ONLY_READY_AWAITING_C7"
            )
            member["c7_handoff_required"] = True
            handoff_members.append(member["ticker"])
        elif member["readiness"] == "READY":
            member["publication_state"] = "CANONICAL_TARGET9"
            member["c7_handoff_required"] = False
        else:
            member["publication_state"] = "PREPARING"
            member["c7_handoff_required"] = False
    return {
        "revision_id": str(revision_id),
        "revision_sha256": str(current[2]),
        "effective_session_date": current[1].isoformat(),
        "complete_intended_membership": all(
            not member["enabled"] or member["readiness"] == "READY"
            for member in member_rows
        ),
        "members": member_rows,
        "c7_handoff": {
            "contract": "advanced-equity-c7-handoff-v1",
            "status": (
                "REQUIRED" if handoff_members else "NOT_REQUIRED"
            ),
            "members": sorted(handoff_members),
            "owner": "C7_CANONICAL_RELEASE_MERGE",
        },
        "active_snapshot": (
            None
            if active is None
            else {
                "revision_id": str(active[0]),
                "session_date": active[1].isoformat(),
                "manifest_id": str(active[2]),
                "cohort_id": str(active[3]),
                "snapshot_sha256": str(active[4]),
            }
        ),
        "intended_snapshot": (
            None
            if intended is None
            else {
                "manifest_id": str(intended[0]),
                "cohort_id": str(intended[1]),
                "snapshot_sha256": str(intended[2]),
            }
        ),
        "canonical_release_boundary": (
            "C7 owns canonical release merge/retry/outbox; "
            "C4 keeps the prior canonical release readable."
        ),
        "provider_fetch_performed": False,
    }


class AdvancedEquityPreparationStatusRepository:
    def __init__(
        self,
        cursor_factory: Callable[[], AbstractContextManager],
    ) -> None:
        self._cursor_factory = cursor_factory

    def read(
        self,
        *,
        active_on: date,
        ticker: str | None = None,
    ) -> dict[str, object] | None:
        with self._cursor_factory() as cursor:
            return load_equity_preparation_status_cursor(
                cursor,
                active_on=active_on,
                ticker=ticker,
            )
