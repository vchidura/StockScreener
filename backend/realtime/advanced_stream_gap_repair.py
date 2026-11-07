"""Fill minutes missing from stored bars with exact per-member REST evidence.

Ingress records a stream-wide gap, but the input gate proves each repaired bar back
to its own ledger request through a gap row. So each member gets its own repair row
here, and the stream-wide gap closes only once stored coverage proves it.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import BarAvailabilityMode, BarSourceKind
from equity.polygon import normalize_native_bars

from .acquisition_ledger import (
    ProviderRequestCompletion,
    ProviderRequestOutcome,
    ProviderRequestState,
    RequestCompleteness,
    finish_provider_request_cursor,
)
from .advanced_gap_repair import (
    plan_advanced_gap_repair_cursor,
    reserve_advanced_gap_repair_cursor,
    seal_advanced_repair_payload_cursor,
    verify_and_close_gap_repair_cursor,
)
from .advanced_stream_control import (
    RegisteredAdvancedStreamSession,
    record_advanced_stream_gap_cursor,
)
from .contracts import MarketFeed, canonical_sha256
from .database_access_v2 import APP_ROLE
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .preflight import ProviderTier
from .provider_budget import ProviderBudgetExceeded


UTC = timezone.utc
# Repair rows belong to no journal shard, so a failed repair can never hold back
# a live bridge. Only the input gate and the stream-wide gap read them.
REST_REPAIR_SHARD_ID = 32_767
REST_REPAIR_FACT_SCHEMA = "advanced-stream-gap-rest-bar-v1"
REST_REPAIR_PROVIDER = "massive"
ONE_MINUTE = timedelta(minutes=1)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _minute_floor(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def _minute_ceiling(value: datetime) -> datetime:
    floor = _minute_floor(value)
    return floor if floor == value else floor + ONE_MINUTE


def first_missing_minute_range(
    present: set[tuple[int, datetime]],
    *,
    instrument_ids: Sequence[int],
    start: datetime,
    end: datetime,
) -> tuple[datetime, datetime] | None:
    """The earliest contiguous minutes where any member lacks a stored bar."""
    cursor = _minute_floor(_utc(start, "start"))
    boundary = _minute_floor(_utc(end, "end"))

    def complete(minute):
        return all(
            (instrument_id, minute) in present
            for instrument_id in instrument_ids
        )

    while cursor < boundary and complete(cursor):
        cursor += ONE_MINUTE
    if cursor >= boundary:
        return None
    hole_start = cursor
    while cursor < boundary and not complete(cursor):
        cursor += ONE_MINUTE
    return hole_start, cursor


@dataclass(frozen=True, slots=True)
class StreamGapMember:
    instrument_id: int
    security_id: UUID
    ticker: str


@dataclass(frozen=True, slots=True)
class StreamGapRepairScope:
    plan_id: UUID
    cohort_id: UUID
    session_date: object
    session: RegisteredAdvancedStreamSession
    members: tuple[StreamGapMember, ...]


@dataclass(frozen=True, slots=True)
class StreamGapMemberResult:
    ticker: str
    status: str
    detail: str | None = None
    provider_requests: int = 0
    response_bytes: int = 0
    persisted_bars: int = 0


def load_stream_gap_repair_scope_cursor(
    cursor,
    plan_id: UUID,
) -> StreamGapRepairScope:
    cursor.execute(
        "SELECT cohort_id,session_date,manifest_id "
        "FROM rt.advanced_ingress_plans WHERE plan_id=%s",
        (str(plan_id),),
    )
    plan = cursor.fetchone()
    if plan is None:
        raise ValueError("Advanced ingress plan is unavailable")
    # A repair row must name a session; the newest one owns any new evidence.
    cursor.execute(
        "SELECT session_id,journal_generation,owner_id,owner_epoch,"
        "started_at FROM rt.stream_sessions "
        "WHERE advanced_ingress_plan_id=%s AND feed='STOCKS' "
        "AND journal_generation IS NOT NULL "
        "ORDER BY owner_epoch DESC LIMIT 1",
        (str(plan_id),),
    )
    session = cursor.fetchone()
    if session is None:
        raise ValueError("Advanced ingress plan has no stream session")
    cursor.execute(
        "SELECT DISTINCT ON (member.instrument_id) "
        "member.instrument_id,instrument.security_id,reference.ticker "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "JOIN ref.security_revisions AS reference USING(security_id) "
        "WHERE member.manifest_id=%s AND member.channel='AM' "
        "AND instrument.asset_class='EQUITY' "
        "ORDER BY member.instrument_id,reference.observed_at DESC,"
        "reference.revision_id DESC",
        (str(plan[2]),),
    )
    members = tuple(
        StreamGapMember(int(row[0]), UUID(str(row[1])), str(row[2]))
        for row in cursor.fetchall()
    )
    if not members:
        raise ValueError("Advanced ingress plan has no AM members")
    return StreamGapRepairScope(
        plan_id,
        UUID(str(plan[0])),
        plan[1],
        RegisteredAdvancedStreamSession(
            UUID(str(session[0])),
            plan_id,
            UUID(str(session[1])),
            UUID(str(session[2])),
            int(session[3]),
            MarketFeed.STOCKS,
            session[4],
        ),
        members,
    )


def load_stored_minutes_cursor(
    cursor,
    *,
    cohort_id: UUID,
    instrument_ids: Sequence[int],
    start: datetime,
    end: datetime,
) -> set[tuple[int, datetime]]:
    # G102 A4: the current bar by instrument from the head, whatever cohort wrote it. `cohort_id` is the run's identity only:
    # a minute stored by any writer (stream or REST repair) is stored. B23: one table, no join.
    cursor.execute(
        "SELECT current.instrument_id,current.bar_start "
        "FROM rt.bar_head AS current "
        "WHERE current.instrument_id=ANY(%s::bigint[]) "
        "AND current.interval='1m' AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "AND current.bar_start>=%s AND current.bar_end<=%s "
        "AND current.is_final",
        (list(instrument_ids), start, end),
    )
    return {(int(row[0]), row[1]) for row in cursor.fetchall()}


def _supersede_unfinished_repairs_cursor(
    cursor,
    scope: StreamGapRepairScope,
    member: StreamGapMember,
    *,
    hole_start: datetime,
    hole_end: datetime,
) -> int:
    """Retire interrupted attempts, returning the next attempt number."""
    cursor.execute(
        "SELECT gap_id,recovery_status,repair_reference "
        "FROM rt.stream_gaps WHERE plan_id=%s AND instrument_id=%s "
        "AND shard_id=%s AND gap_start=%s AND gap_end=%s "
        "ORDER BY recorded_at FOR UPDATE",
        (
            str(scope.plan_id),
            member.instrument_id,
            REST_REPAIR_SHARD_ID,
            hole_start,
            hole_end,
        ),
    )
    rows = cursor.fetchall()
    for gap_id, status, request_id in rows:
        if status != "UNRESOLVED":
            continue
        if request_id is not None:
            cursor.execute(
                "UPDATE rt.provider_request_ledger "
                "SET state='AMBIGUOUS',failure_code='REPAIR_PROCESS_LOST' "
                "WHERE request_id=%s AND state='RESERVED'",
                (str(request_id),),
            )
        # The gate only reads a row through its bars, so a superseded attempt
        # with none cannot satisfy it; missing minutes still block the window.
        cursor.execute(
            "UPDATE rt.stream_gaps SET recovery_status='WAIVED',"
            "operator_alert_required=true WHERE gap_id=%s",
            (str(gap_id),),
        )
    return len(rows) + 1


def _reserve_member_repair_cursor(
    cursor,
    scope: StreamGapRepairScope,
    member: StreamGapMember,
    *,
    hole_start: datetime,
    hole_end: datetime,
    checked_at: datetime,
    reserved_response_bytes: int,
):
    attempt = _supersede_unfinished_repairs_cursor(
        cursor,
        scope,
        member,
        hole_start=hole_start,
        hole_end=hole_end,
    )
    gap_id = record_advanced_stream_gap_cursor(
        cursor,
        scope.session,
        gap_start=hole_start,
        gap_end=hole_end,
        last_durable_offset=0,
        reason=f"REST_REPAIR:{member.instrument_id}:{attempt}",
        instrument_id=member.instrument_id,
        shard_id=REST_REPAIR_SHARD_ID,
    )
    repair = plan_advanced_gap_repair_cursor(
        cursor,
        gap_id,
        reserved_response_bytes=reserved_response_bytes,
    )
    request_id = reserve_advanced_gap_repair_cursor(
        cursor,
        repair,
        reserved_at=checked_at,
    )
    return repair, request_id


def _repair_facts(
    scope: StreamGapRepairScope,
    member: StreamGapMember,
    rows,
    *,
    request_id: UUID,
    hole_start: datetime,
    hole_end: datetime,
    observed_at: datetime,
) -> tuple[tuple[RealtimeEquityBarFact, ...], UUID]:
    segment = uuid5(
        NAMESPACE_URL,
        f"advanced-stream-gap-rest:{scope.cohort_id}:{request_id}",
    )
    revisions = normalize_native_bars(
        member.security_id,
        member.ticker,
        "1m",
        rows,
        observed_at=observed_at,
        adjusted=False,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        ingestion_segment_id=segment,
    )
    selected = tuple(
        revision
        for revision in revisions
        if hole_start <= revision.bar_start and revision.bar_end <= hole_end
    )
    expected = {
        hole_start + ONE_MINUTE * index
        for index in range(
            int((hole_end - hole_start).total_seconds() // 60)
        )
    }
    if {revision.bar_start for revision in selected} != expected:
        raise ValueError("provider did not return every missing minute")
    return (
        tuple(
            RealtimeEquityBarFact(
                scope.cohort_id,
                ProviderTier.MASSIVE_ADVANCED,
                member.instrument_id,
                replace(
                    revision,
                    bar_revision_id=uuid5(
                        NAMESPACE_URL,
                        "advanced-stream-gap-rest-bar:"
                        f"{scope.cohort_id}:{revision.bar_revision_id}",
                    ),
                    source_kind=BarSourceKind.NATIVE_REST,
                    quality_codes=tuple(dict.fromkeys((
                        *revision.quality_codes,
                        "EXPLICIT_ADVANCED_REST_REPAIR_V1",
                    ))),
                ),
            )
            for revision in selected
        ),
        segment,
    )


def _record_repair_batch_cursor(
    cursor,
    scope: StreamGapRepairScope,
    *,
    segment: UUID,
    request_id: UUID,
    facts: tuple[RealtimeEquityBarFact, ...],
    started_at: datetime,
    completed_at: datetime,
) -> None:
    cursor.execute(
        "INSERT INTO rt.acquisition_batches ("
        "acquisition_batch_id,cohort_id,provider_tier,fact_kind,"
        "fact_schema_version,provider,provider_batch_id,request_id,"
        "started_at,completed_at,fact_count,batch_payload_sha256"
        ") VALUES (%s,%s,'MASSIVE_ADVANCED','EQUITY_BAR',%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (acquisition_batch_id) DO NOTHING",
        (
            str(segment),
            str(scope.cohort_id),
            REST_REPAIR_FACT_SCHEMA,
            REST_REPAIR_PROVIDER,
            str(segment),
            str(request_id),
            started_at,
            completed_at,
            len(facts),
            canonical_sha256(
                tuple(fact.revision.bar_revision_id for fact in facts)
            ),
        ),
    )


def _repair_member(
    provider,
    client,
    scope: StreamGapRepairScope,
    member: StreamGapMember,
    *,
    hole_start: datetime,
    hole_end: datetime,
    clock: Callable[[], datetime],
    response_byte_limit: int,
) -> StreamGapMemberResult:
    checked = _utc(clock(), "gap repair clock")
    try:
        with provider.transaction(APP_ROLE) as unit:
            repair, request_id = _reserve_member_repair_cursor(
                unit.cursor,
                scope,
                member,
                hole_start=hole_start,
                hole_end=hole_end,
                checked_at=checked,
                reserved_response_bytes=response_byte_limit,
            )
    except ProviderBudgetExceeded as error:
        return StreamGapMemberResult(member.ticker, "BLOCKED", str(error))
    client.begin_chunk(
        maximum_requests=1,
        maximum_response_bytes=response_byte_limit,
    )
    try:
        rows = client.fetch_native_bars(
            member.ticker,
            "1m",
            scope.session_date,
            scope.session_date,
            adjusted=False,
        )
        requests, response_bytes = client.chunk_metrics()
        completed = _utc(clock(), "gap repair clock")
        facts, segment = _repair_facts(
            scope,
            member,
            rows,
            request_id=request_id,
            hole_start=hole_start,
            hole_end=hole_end,
            observed_at=completed,
        )
        with provider.transaction(APP_ROLE) as unit:
            seal_advanced_repair_payload_cursor(
                unit.cursor,
                repair.repair_id,
                json.dumps(rows, sort_keys=True).encode("utf-8"),
                sealed_at=completed,
            )
            _record_repair_batch_cursor(
                unit.cursor,
                scope,
                segment=segment,
                request_id=request_id,
                facts=facts,
                started_at=checked,
                completed_at=completed,
            )
            persist_realtime_equity_bars_cursor(unit.cursor, facts)
            unit.cursor.execute(
                "UPDATE rt.stream_gaps SET repair_status='REPLAYED' "
                "WHERE repair_id=%s",
                (str(repair.repair_id),),
            )
            # The 5m bars come from the window this repair unblocks, so
            # requiring them here could never succeed for a 5m-sized hole.
            verify_and_close_gap_repair_cursor(
                unit.cursor,
                repair.repair_id,
                checked_at=completed,
                require_derived=False,
            )
    except Exception as error:
        requests, response_bytes = client.chunk_metrics()
        failed = _utc(clock(), "gap repair clock")
        detail = f"{type(error).__name__}:{str(error)[:120]}"
        with provider.transaction(APP_ROLE) as unit:
            finish_provider_request_cursor(
                unit.cursor,
                request_id,
                ProviderRequestCompletion(
                    ProviderRequestState.FAILED,
                    completed_at=failed,
                    started_at=checked,
                    outcome=ProviderRequestOutcome.INVALID,
                    response_bytes=response_bytes,
                    pages_fetched=requests,
                    completeness=RequestCompleteness.INCOMPLETE,
                    failure_code="STREAM_GAP_REPAIR_FAILED",
                ),
            )
            unit.cursor.execute(
                "UPDATE rt.stream_gaps SET recovery_status='WAIVED',"
                "repair_status='BLOCKED',operator_alert_required=true "
                "WHERE repair_id=%s",
                (str(repair.repair_id),),
            )
        return StreamGapMemberResult(
            member.ticker,
            "FAILED",
            detail,
            requests,
            response_bytes,
        )
    return StreamGapMemberResult(
        member.ticker,
        "REPAIRED",
        None,
        requests,
        response_bytes,
        len(facts),
    )


def close_covered_stream_gaps_cursor(
    cursor,
    scope: StreamGapRepairScope,
) -> tuple[UUID, ...]:
    """Close stream-wide gaps whose every minute is now stored for every member."""
    cursor.execute(
        "SELECT gap_id,gap_start,COALESCE(gap_end,reconnect_at) "
        "FROM rt.stream_gaps WHERE plan_id=%s AND instrument_id IS NULL "
        "AND recovery_status='UNRESOLVED' "
        "AND COALESCE(gap_end,reconnect_at) IS NOT NULL "
        "ORDER BY gap_start FOR UPDATE",
        (str(scope.plan_id),),
    )
    instrument_ids = tuple(member.instrument_id for member in scope.members)
    closed = []
    for gap_id, gap_start, gap_end in cursor.fetchall():
        start = _minute_floor(gap_start)
        end = _minute_ceiling(gap_end)
        present = load_stored_minutes_cursor(
            cursor,
            cohort_id=scope.cohort_id,
            instrument_ids=instrument_ids,
            start=start,
            end=end,
        )
        if first_missing_minute_range(
            present,
            instrument_ids=instrument_ids,
            start=start,
            end=end,
        ) is not None:
            continue
        scope_sha256 = canonical_sha256({
            "contract": "advanced-stream-gap-closure-v1",
            "gap_id": gap_id,
            "requested_from": start,
            "requested_to": end,
            "instrument_ids": instrument_ids,
        })
        cursor.execute(
            "UPDATE rt.stream_gaps SET recovery_status='REPAIRED',"
            "repair_status='REPAIRED',repair_id=%s,repair_scope_sha256=%s,"
            "canonical_coverage_sha256=%s,operator_alert_required=false "
            "WHERE gap_id=%s",
            (
                str(uuid5(
                    NAMESPACE_URL,
                    f"advanced-stream-gap-closure:{scope_sha256}",
                )),
                scope_sha256,
                scope_sha256,
                str(gap_id),
            ),
        )
        closed.append(UUID(str(gap_id)))
    return tuple(closed)


def repair_advanced_stream_hole(
    provider,
    client,
    *,
    plan_id: UUID,
    hole_start: datetime,
    hole_end: datetime,
    response_byte_limit: int,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    start = _utc(hole_start, "hole_start")
    end = _utc(hole_end, "hole_end")
    if (
        start >= end
        or start != _minute_floor(start)
        or end != _minute_floor(end)
        or response_byte_limit <= 0
    ):
        raise ValueError("stream hole repair scope is invalid")
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        scope = load_stream_gap_repair_scope_cursor(unit.cursor, plan_id)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        present = load_stored_minutes_cursor(
            unit.cursor,
            cohort_id=scope.cohort_id,
            instrument_ids=tuple(
                member.instrument_id for member in scope.members
            ),
            start=start,
            end=end,
        )
    results = tuple(
        _repair_member(
            provider,
            client,
            scope,
            member,
            hole_start=start,
            hole_end=end,
            clock=clock,
            response_byte_limit=response_byte_limit,
        )
        for member in scope.members
        if any(
            (member.instrument_id, start + ONE_MINUTE * index) not in present
            for index in range(int((end - start).total_seconds() // 60))
        )
    )
    with provider.transaction(APP_ROLE) as unit:
        closed = close_covered_stream_gaps_cursor(unit.cursor, scope)
    return {
        "status": (
            "COMPLETE"
            if all(row.status == "REPAIRED" for row in results)
            else "INCOMPLETE"
        ),
        "plan_id": str(plan_id),
        "hole_start": start.isoformat(),
        "hole_end": end.isoformat(),
        "members": [
            {
                "ticker": row.ticker,
                "status": row.status,
                "detail": row.detail,
                "provider_requests": row.provider_requests,
                "response_bytes": row.response_bytes,
                "persisted_bars": row.persisted_bars,
            }
            for row in results
        ],
        "provider_requests": sum(row.provider_requests for row in results),
        "closed_stream_gaps": [str(gap_id) for gap_id in closed],
    }
