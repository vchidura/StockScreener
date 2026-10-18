"""Advanced gap evidence and repair on canonical V2 ledgers/orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from math import ceil
from typing import Callable, Protocol, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from .acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RequestCompleteness,
    finish_provider_request_cursor,
    load_raw_payload_cursor,
    persist_raw_payload_cursor,
)
from .contracts import canonical_json, canonical_sha256
from .equity_interval_derivation import derive_realtime_interval_facts
from .gap_repair_plan import plan_gap_repair_cursor
from .provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)


class GapContinuity(str, Enum):
    LOCAL_OFFSET_DISCONTINUITY = "LOCAL_OFFSET_DISCONTINUITY"
    PROVIDER_TIME_OUTAGE = "PROVIDER_TIME_OUTAGE"
    NO_TRADE_INTERVAL = "NO_TRADE_INTERVAL"
    UNKNOWN_CONTINUITY = "UNKNOWN_CONTINUITY"


@dataclass(frozen=True, slots=True)
class GapReconnectEvidence:
    gap_id: UUID
    prior_journal_generation: UUID
    reconnect_journal_generation: UUID
    disconnect_at: datetime
    reconnect_at: datetime
    last_durable_offset: int
    first_post_reconnect_offset: int
    last_durable_watermark: datetime | None
    first_post_reconnect_event_at: datetime | None
    no_trade_evidence: bool = False


@dataclass(frozen=True, slots=True)
class AdvancedGapRepairPlan:
    repair_id: UUID
    gap_id: UUID
    plan_id: UUID
    allocation_kind: str
    instrument_id: int | None
    ticker: str
    requested_from: datetime
    requested_to: datetime
    scope_sha256: str
    reserved_response_bytes: int
    status: str
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class ColdStartWorkItem:
    work_id: UUID
    session_date: date
    instrument_id: int
    ticker: str
    requested_from: datetime
    requested_to: datetime
    expected_raw_1m: int
    expected_raw_5m: int


@dataclass(frozen=True, slots=True)
class ColdStartBackfillPlan:
    backfill_id: UUID
    plan_id: UUID
    cohort_id: UUID
    session_dates: tuple[date, ...]
    universe_target: int
    request_ceiling: int
    decoded_byte_ceiling: int
    expected_cost: str
    scope_sha256: str
    state: str
    reason_code: str
    next_action: str
    work: tuple[ColdStartWorkItem, ...]


@dataclass(frozen=True, slots=True)
class HistoricalMinuteExecutionResult:
    provider_requests: int
    response_bytes: int
    persisted_raw_1m: int
    derived_intervals: tuple[str, ...]
    adjustment_basis: str = "RAW_UNADJUSTED"


class HistoricalMinuteTransport(Protocol):
    def fetch(self, repair: AdvancedGapRepairPlan) -> bytes:
        ...


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def classify_gap_continuity(
    evidence: GapReconnectEvidence,
) -> tuple[GapContinuity, str]:
    disconnect = _utc(evidence.disconnect_at, "disconnect_at")
    reconnect = _utc(evidence.reconnect_at, "reconnect_at")
    if reconnect < disconnect:
        raise ValueError("reconnect cannot predate disconnect")
    if evidence.no_trade_evidence:
        classification = GapContinuity.NO_TRADE_INTERVAL
    elif (
        evidence.prior_journal_generation
        == evidence.reconnect_journal_generation
        and evidence.first_post_reconnect_offset
        != evidence.last_durable_offset + 1
    ):
        classification = GapContinuity.LOCAL_OFFSET_DISCONTINUITY
    elif (
        evidence.last_durable_watermark is not None
        and evidence.first_post_reconnect_event_at is not None
        and _utc(
            evidence.first_post_reconnect_event_at,
            "first_post_reconnect_event_at",
        )
        - _utc(
            evidence.last_durable_watermark,
            "last_durable_watermark",
        )
        > timedelta(minutes=1)
    ):
        classification = GapContinuity.PROVIDER_TIME_OUTAGE
    else:
        classification = GapContinuity.UNKNOWN_CONTINUITY
    return classification, canonical_sha256({
        "contract": "advanced-gap-continuity-v1",
        "evidence": evidence,
        "classification": classification.value,
    })


def record_gap_reconnect_evidence_cursor(
    cursor,
    evidence: GapReconnectEvidence,
) -> GapContinuity:
    classification, digest = classify_gap_continuity(evidence)
    cursor.execute(
        "UPDATE rt.stream_gaps SET gap_end=%s,reconnect_at=%s,"
        "first_post_reconnect_event_at=%s,"
        "first_post_reconnect_offset=%s,continuity_class=%s,"
        "classification_sha256=%s "
        "WHERE gap_id=%s AND recovery_status='UNRESOLVED' "
        "AND journal_generation=%s RETURNING gap_id",
        (
            evidence.reconnect_at,
            evidence.reconnect_at,
            evidence.first_post_reconnect_event_at,
            evidence.first_post_reconnect_offset,
            classification.value,
            digest,
            str(evidence.gap_id),
            str(evidence.prior_journal_generation),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("gap reconnect evidence was rejected")
    return classification


def record_first_post_reconnect_event_cursor(
    cursor,
    *,
    plan_id: UUID,
    reconnect_journal_generation: UUID,
    first_event_at: datetime,
    first_offset: int,
) -> GapContinuity | None:
    cursor.execute(
        "SELECT gap_id,journal_generation,gap_start,reconnect_at,"
        "last_durable_offset,last_durable_watermark "
        "FROM rt.stream_gaps WHERE plan_id=%s "
        "AND recovery_status='UNRESOLVED' "
        "AND reconnect_at IS NOT NULL "
        "AND first_post_reconnect_event_at IS NULL "
        "ORDER BY reconnect_at,gap_id LIMIT 1 FOR UPDATE",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return record_gap_reconnect_evidence_cursor(
        cursor,
        GapReconnectEvidence(
            UUID(str(row[0])),
            UUID(str(row[1])),
            reconnect_journal_generation,
            row[2],
            row[3],
            int(row[4]),
            first_offset,
            row[5],
            first_event_at,
        ),
    )


def attach_gap_dependencies_cursor(
    cursor,
    gap_id: UUID,
    window_ids: tuple[UUID, ...],
    *,
    blocked_at: datetime,
) -> None:
    # Canonical window/stage state owns the block. The gap remains the evidence.
    cursor.execute(
        "UPDATE rt.window_runs SET state='RETAINED_GAP',updated_at=%s "
        "WHERE window_id=ANY(%s::uuid[]) "
        "AND state IN ('PENDING','FAILED','MISSED')",
        (
            _utc(blocked_at, "blocked_at"),
            [str(value) for value in sorted(set(window_ids), key=str)],
        ),
    )


def plan_advanced_gap_repair_cursor(
    cursor,
    gap_id: UUID,
    *,
    reserved_response_bytes: int,
    allocation_kind: str = "LIVE_GAP",
) -> AdvancedGapRepairPlan:
    if (
        allocation_kind not in {"LIVE_GAP", "STARTUP_BACKFILL"}
        or type(reserved_response_bytes) is not int
        or reserved_response_bytes <= 0
    ):
        raise ValueError("advanced repair planning scope is invalid")
    cursor.execute(
        "SELECT gap.plan_id,gap.instrument_id,gap.gap_start,"
        "COALESCE(gap.gap_end,gap.reconnect_at),gap.continuity_class,"
        "reference.ticker,plan.maximum_rest_repair_requests,"
        "plan.maximum_rest_repair_decoded_bytes "
        "FROM rt.stream_gaps AS gap "
        "JOIN rt.advanced_ingress_plans AS plan USING(plan_id) "
        "LEFT JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=gap.instrument_id "
        "LEFT JOIN LATERAL ("
        "SELECT revision.ticker FROM ref.security_revisions AS revision "
        "WHERE revision.security_id=instrument.security_id "
        "ORDER BY revision.observed_at DESC,revision.revision_id DESC LIMIT 1"
        ") AS reference ON true "
        "WHERE gap.gap_id=%s FOR UPDATE OF gap,plan",
        (str(gap_id),),
    )
    row = cursor.fetchone()
    if row is None or row[0] is None or row[3] is None:
        raise ValueError("gap is not ready for exact repair planning")
    plan_id = UUID(str(row[0]))
    ticker = str(row[5] or "ALL")
    scope = {
        "contract": "advanced-gap-repair-v2",
        "gap_id": gap_id,
        "plan_id": plan_id,
        "allocation_kind": allocation_kind,
        "instrument_id": int(row[1]) if row[1] is not None else None,
        "ticker": ticker,
        "requested_from": row[2],
        "requested_to": row[3],
        "reserved_response_bytes": reserved_response_bytes,
    }
    scope_sha256 = canonical_sha256(scope)
    repair_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-gap-repair:{scope_sha256}",
    )
    authorized = (
        row[4] != GapContinuity.NO_TRADE_INTERVAL.value
        and int(row[6]) > 0
        and int(row[7]) >= reserved_response_bytes
    )
    status = "PLANNED" if authorized else "BLOCKED"
    reason = None if authorized else (
        "NO_TRADE_EVIDENCE"
        if row[4] == GapContinuity.NO_TRADE_INTERVAL.value
        else "REPAIR_UNAUTHORIZED"
    )
    cursor.execute(
        "UPDATE rt.stream_gaps SET repair_id=%s,repair_scope_sha256=%s,"
        "repair_reserved_response_bytes=%s,repair_status=%s,"
        "operator_alert_required=%s "
        "WHERE gap_id=%s AND recovery_status='UNRESOLVED' "
        "AND (repair_id IS NULL OR repair_id=%s) RETURNING gap_id",
        (
            str(repair_id),
            scope_sha256,
            reserved_response_bytes,
            status,
            not authorized,
            str(gap_id),
            str(repair_id),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("gap repair plan conflicts with durable gap state")
    return AdvancedGapRepairPlan(
        repair_id,
        gap_id,
        plan_id,
        allocation_kind,
        int(row[1]) if row[1] is not None else None,
        ticker,
        row[2],
        row[3],
        scope_sha256,
        reserved_response_bytes,
        status,
        reason,
    )


def reserve_advanced_gap_repair_cursor(
    cursor,
    repair: AdvancedGapRepairPlan,
    *,
    reserved_at: datetime,
) -> UUID:
    if repair.status != "PLANNED":
        raise ProviderBudgetExceeded(
            repair.reason_code or "advanced repair is not authorized"
        )
    cursor.execute(
        "SELECT plan.session_date,plan.cohort_id,"
        "plan.maximum_rest_repair_requests,"
        "plan.maximum_rest_repair_decoded_bytes,"
        "plan.authorized_end_at,auth.authorization_reference,"
        "usage.rest_repair_request_count,"
        "usage.rest_repair_decoded_byte_count,gap.repair_status,"
        "gap.repair_reference "
        "FROM rt.stream_gaps AS gap "
        "JOIN rt.advanced_ingress_plans AS plan USING(plan_id) "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
        "WHERE gap.repair_id=%s FOR UPDATE OF gap,plan,usage",
        (str(repair.repair_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("advanced repair plan is unavailable")
    if row[8] != "PLANNED":
        if row[9] is None:
            raise ValueError("advanced repair reservation is inconsistent")
        return UUID(str(row[9]))
    if (
        int(row[6]) + 1 > int(row[2])
        or int(row[7]) + repair.reserved_response_bytes > int(row[3])
    ):
        cursor.execute(
            "UPDATE rt.stream_gaps SET repair_status='BLOCKED',"
            "operator_alert_required=true WHERE repair_id=%s",
            (str(repair.repair_id),),
        )
        raise ProviderBudgetExceeded("advanced repair budget is exhausted")
    request_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-gap-request:{repair.scope_sha256}",
    )
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=row[0],
        authorization_reference=str(row[5]),
        provider_authorization_reference="stocks-advanced",
        expected_cost="standing authorization repair allocation",
        request_ceiling=int(row[2]),
        decoded_byte_ceiling=int(row[3]),
        lanes=frozenset({ProviderRequestLane.EQUITY_HISTORY}),
        authorized_until=row[4],
    )
    reserve_coordinated_provider_request_cursor(
        cursor,
        scope,
        ProviderRequestReservation(
            request_id,
            scope.account_alias,
            ProviderRequestLane.EQUITY_HISTORY,
            "ADVANCED_HISTORICAL_1M",
            repair.scope_sha256,
            _utc(reserved_at, "reserved_at"),
            cohort_id=UUID(str(row[1])),
        ),
        reserved_response_bytes=repair.reserved_response_bytes,
    )
    cursor.execute(
        "UPDATE rt.advanced_ingress_plan_usage SET "
        "rest_repair_request_count=rest_repair_request_count+1,"
        "rest_repair_decoded_byte_count="
        "rest_repair_decoded_byte_count+%s,updated_at=%s "
        "WHERE plan_id=%s",
        (
            repair.reserved_response_bytes,
            _utc(reserved_at, "reserved_at"),
            str(repair.plan_id),
        ),
    )
    cursor.execute(
        "UPDATE rt.stream_gaps SET repair_status='RESERVED',"
        "repair_reference=%s WHERE repair_id=%s",
        (str(request_id), str(repair.repair_id)),
    )
    return request_id


def seal_advanced_repair_payload_cursor(
    cursor,
    repair_id: UUID,
    body: bytes,
    *,
    sealed_at: datetime,
) -> UUID:
    if not isinstance(body, bytes) or not body:
        raise ValueError("advanced repair payload must be nonempty bytes")
    sealed = _utc(sealed_at, "sealed_at")
    cursor.execute(
        "SELECT repair_reference,gap_start,"
        "COALESCE(gap_end,reconnect_at),repair_status "
        "FROM rt.stream_gaps WHERE repair_id=%s FOR UPDATE",
        (str(repair_id),),
    )
    row = cursor.fetchone()
    if row is None or row[0] is None or row[3] != "RESERVED":
        raise ValueError("advanced repair is not reserved for sealing")
    request_id = UUID(str(row[0]))
    payload_id = persist_raw_payload_cursor(
        cursor,
        body,
        arrived_at=sealed,
        request_id=request_id,
    )
    finish_provider_request_cursor(
        cursor,
        request_id,
        ProviderRequestCompletion(
            ProviderRequestState.COMPLETE,
            completed_at=sealed,
            started_at=sealed,
            outcome=ProviderRequestOutcome.OK,
            http_status=200,
            response_bytes=len(body),
            row_count=0,
            pages_fetched=1,
            requested_from=row[1],
            requested_to=row[2],
            price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
            completeness=RequestCompleteness.COMPLETE,
            payload_id=payload_id,
        ),
    )
    cursor.execute(
        "UPDATE rt.stream_gaps SET repair_status='SEALED' "
        "WHERE repair_id=%s",
        (str(repair_id),),
    )
    return payload_id


def load_sealed_advanced_repair_payload_cursor(
    cursor,
    repair_id: UUID,
) -> bytes:
    cursor.execute(
        "SELECT request.payload_id "
        "FROM rt.stream_gaps AS gap "
        "JOIN rt.provider_request_ledger AS request "
        "ON request.request_id=gap.repair_reference "
        "WHERE gap.repair_id=%s AND gap.repair_status "
        "IN ('SEALED','REPLAYED','VERIFIED','REPAIRED')",
        (str(repair_id),),
    )
    row = cursor.fetchone()
    if row is None or row[0] is None:
        raise ValueError("sealed advanced repair payload is unavailable")
    return load_raw_payload_cursor(cursor, UUID(str(row[0])))


def verify_and_close_gap_repair_cursor(
    cursor,
    repair_id: UUID,
    *,
    checked_at: datetime,
) -> str:
    cursor.execute(
        "SELECT gap_id,plan_id,instrument_id,gap_start,"
        "COALESCE(gap_end,reconnect_at),repair_status "
        "FROM rt.stream_gaps WHERE repair_id=%s FOR UPDATE",
        (str(repair_id),),
    )
    repair = cursor.fetchone()
    if repair is None or repair[2] is None or repair[5] not in {
        "SEALED",
        "REPLAYED",
        "VERIFIED",
    }:
        raise ValueError("advanced repair is not ready for verification")
    seconds = int((repair[4] - repair[3]).total_seconds())
    expected_1m = seconds // 60
    expected_5m = seconds // 300 if seconds % 300 == 0 else 0
    cursor.execute(
        "SELECT current.interval,COUNT(DISTINCT current.bar_start) "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.cohort_id=("
        "SELECT cohort_id FROM rt.advanced_ingress_plans WHERE plan_id=%s"
        ") AND current.instrument_id=%s "
        "AND current.bar_start>=%s AND current.bar_end<=%s "
        "AND current.interval IN ('1m','5m') "
        "AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.is_final GROUP BY current.interval",
        (str(repair[1]), int(repair[2]), repair[3], repair[4]),
    )
    counts = {str(interval): int(count) for interval, count in cursor.fetchall()}
    if counts.get("1m", 0) != expected_1m or (
        expected_5m and counts.get("5m", 0) != expected_5m
    ):
        raise ValueError("exact canonical repair coverage is incomplete")
    coverage_sha256 = canonical_sha256({
        "repair_id": repair_id,
        "requested_from": repair[3],
        "requested_to": repair[4],
        "counts": counts,
    })
    cursor.execute(
        "UPDATE rt.stream_gaps SET recovery_status='REPAIRED',"
        "repair_status='REPAIRED',canonical_coverage_sha256=%s,"
        "operator_alert_required=false WHERE repair_id=%s",
        (coverage_sha256, str(repair_id)),
    )
    return coverage_sha256


def plan_advanced_cold_start_cursor(
    cursor,
    *,
    plan_id: UUID,
    sessions: tuple[date, ...],
    known_by: datetime,
    request_ceiling: int,
    decoded_byte_ceiling: int,
    expected_cost: str,
) -> ColdStartBackfillPlan:
    if (
        not 1 <= len(sessions) <= 2
        or tuple(sorted(set(sessions))) != sessions
        or request_ceiling < 0
        or decoded_byte_ceiling < 0
        or not expected_cost.strip()
    ):
        raise ValueError("cold-start backfill scope is invalid")
    cursor.execute(
        "SELECT cohort_id,universe_target,session_date,"
        "maximum_rest_repair_requests,maximum_rest_repair_decoded_bytes,"
        "service_plan_id FROM rt.advanced_ingress_plans WHERE plan_id=%s",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("cold-start ingress plan is unavailable")
    cohort_id = UUID(str(row[0]))
    coverage = plan_gap_repair_cursor(
        cursor,
        cohort_id=cohort_id,
        sessions=sessions,
        known_by=known_by,
        intervals=("1m", "5m"),
    )
    work = []
    for session_plan in (
        item for item in coverage["plans"] if item["interval"] == "1m"
    ):
        for missing in session_plan["missing_ranges"]:
            identity = {
                "contract": "advanced-cold-start-work-v2",
                "plan_id": plan_id,
                "session_date": session_plan["session_date"],
                **missing,
            }
            work.append(
                ColdStartWorkItem(
                    uuid5(
                        NAMESPACE_URL,
                        "stockscreener:advanced-cold-start-work:"
                        f"{canonical_sha256(identity)}",
                    ),
                    session_plan["session_date"],
                    int(missing["instrument_id"]),
                    str(missing["ticker"]),
                    missing["requested_from"],
                    missing["requested_to"],
                    int(missing["missing_members"]),
                    ceil(int(missing["missing_members"]) / 5),
                )
            )
    scope = {
        "contract": "advanced-cold-start-backfill-v2",
        "ingress_plan_id": plan_id,
        "cohort_id": cohort_id,
        "sessions": sessions,
        "universe_target": int(row[1]),
        "request_ceiling": request_ceiling,
        "decoded_byte_ceiling": decoded_byte_ceiling,
        "expected_cost": expected_cost.strip(),
        "work": tuple(work),
    }
    scope_sha256 = canonical_sha256(scope)
    service_plan_id = (
        UUID(str(row[5]))
        if row[5] is not None
        else uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-backfill-service:{scope_sha256}",
        )
    )
    authorized = (
        len(work) <= request_ceiling <= int(row[3])
        and decoded_byte_ceiling <= int(row[4])
        and (not work or decoded_byte_ceiling > 0)
    )
    state = "BACKFILL_PLANNED" if authorized else "BACKFILL_BLOCKED"
    reason = (
        "MISSING_RAW_1M"
        if authorized and work
        else "COVERAGE_COMPLETE"
        if authorized
        else "STARTUP_BACKFILL_UNAUTHORIZED_OR_EXHAUSTED"
    )
    next_action = (
        "RESERVE_MISSING_RAW_1M"
        if authorized and work
        else "VERIFY_AND_WAIT_NEXT_SESSION"
        if authorized
        else "OPERATOR_AUTHORIZATION_REQUIRED"
    )
    cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference) VALUES ("
        "%s,%s,%s,'stages_v3',%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            str(service_plan_id),
            str(cohort_id),
            row[2],
            ["EQUITY_INPUT_GATE", "EQUITY_MATERIALIZE", "EQUITY_PROJECTION"],
            scope_sha256,
            _utc(known_by, "known_by") + timedelta(days=1),
            f"advanced-backfill:{state}:{reason}:{next_action}",
        ),
    )
    cursor.execute(
        "UPDATE rt.advanced_ingress_plans SET service_plan_id=%s "
        "WHERE plan_id=%s AND service_plan_id IS NULL",
        (str(service_plan_id), str(plan_id)),
    )
    for session_date in sessions:
        cutoff = max(
            item.requested_to
            for item in work
            if item.session_date == session_date
        ) if any(item.session_date == session_date for item in work) else _utc(
            known_by, "known_by"
        )
        window_key = f"advanced-backfill-{session_date.isoformat()}"
        window_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:window:{cohort_id}:{window_key}",
        )
        window_state = (
            "PENDING" if authorized and work
            else "COMPLETE" if authorized
            else "RETAINED_GAP"
        )
        cursor.execute(
            "INSERT INTO rt.window_runs ("
            "window_id,plan_id,cohort_id,provider_tier,window_key,"
            "release_at,cutoff,deadline,state,updated_at) VALUES ("
            "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (window_id) DO NOTHING",
            (
                str(window_id),
                str(service_plan_id),
                str(cohort_id),
                window_key,
                _utc(known_by, "known_by"),
                cutoff,
                _utc(known_by, "known_by") + timedelta(minutes=5),
                window_state,
                _utc(known_by, "known_by"),
            ),
        )
    return ColdStartBackfillPlan(
        service_plan_id,
        plan_id,
        cohort_id,
        sessions,
        int(row[1]),
        request_ceiling,
        decoded_byte_ceiling,
        expected_cost.strip(),
        scope_sha256,
        state,
        reason,
        next_action,
        tuple(work),
    )


def transition_backfill_state_cursor(
    cursor,
    backfill_id: UUID,
    *,
    state: str,
    reason_code: str,
    next_action: str,
    checked_at: datetime,
) -> None:
    mapping = {
        "BACKFILL_BLOCKED": "RETAINED_GAP",
        "BACKFILL_COMPLETE": "COMPLETE",
        "WAIT_NEXT_SESSION": "COMPLETE",
    }
    target = mapping.get(state)
    if target is None:
        return
    cursor.execute(
        "UPDATE rt.window_runs SET state=%s,updated_at=%s "
        "WHERE plan_id=%s AND state IN ('PENDING','FAILED','MISSED',"
        "'RETAINED_GAP')",
        (target, _utc(checked_at, "checked_at"), str(backfill_id)),
    )


def publish_cohort_local_backfill_release_cursor(
    cursor,
    backfill_id: UUID,
    *,
    coverage_sha256: str,
    job_barrier_sha256: str,
    published_at: datetime,
) -> UUID:
    checked = _utc(published_at, "published_at")
    cursor.execute(
        "SELECT plan.cohort_id,plan.session_date,run.window_id,"
        "run.cutoff,manifest.universe_sha256 "
        "FROM rt.service_plans AS plan "
        "JOIN rt.window_runs AS run USING(plan_id) "
        "JOIN rt.cohorts AS cohort ON cohort.cohort_id=plan.cohort_id "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "WHERE plan.plan_id=%s AND run.state='COMPLETE' "
        "ORDER BY run.cutoff DESC LIMIT 1",
        (str(backfill_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("canonical backfill windows are not complete")
    receipt_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-backfill-receipt:{backfill_id}:"
        f"{coverage_sha256}:{job_barrier_sha256}",
    )
    cursor.execute(
        "INSERT INTO rt.cycle_receipts ("
        "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
        "reference_generation,universe_sha256,input_sha256,build_id) "
        "VALUES (%s,%s,%s,'PORTAL_READY',%s,1,%s,%s,"
        "'advanced-backfill-v2') ON CONFLICT DO NOTHING",
        (
            str(receipt_id),
            str(row[2]),
            str(row[0]),
            row[3],
            str(row[4]),
            coverage_sha256,
        ),
    )
    release_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-backfill-release:{receipt_id}",
    )
    manifest = {
        "kind": "ADVANCED_BACKFILL",
        "coverage_sha256": coverage_sha256,
        "job_barrier_sha256": job_barrier_sha256,
    }
    cursor.execute(
        "INSERT INTO rt.portal_releases ("
        "release_id,cohort_id,provider_tier,release_class,receipt_id,"
        "session_date,cutoff,universe_sha256,manifest,manifest_sha256) "
        "VALUES (%s,%s,'MASSIVE_ADVANCED','SESSION_CLOSE',%s,%s,%s,%s,"
        "%s::jsonb,%s)",
        (
            str(release_id),
            str(row[0]),
            str(receipt_id),
            row[1],
            checked,
            str(row[4]),
            canonical_json(manifest),
            canonical_sha256(manifest),
        ),
    )
    return release_id


class AdvancedHistoricalMinuteExecutor:
    DERIVED_INTERVALS = ("5m", "15m", "30m", "1h", "1d")

    def __init__(
        self,
        transport: HistoricalMinuteTransport,
        parser: Callable[[bytes, AdvancedGapRepairPlan], Sequence[object]],
        persister: Callable[[Sequence[object]], int],
        deriver: Callable[[Sequence[object], tuple[str, ...], str], int],
    ) -> None:
        self.transport = transport
        self.parser = parser
        self.persister = persister
        self.deriver = deriver

    def execute(
        self,
        repair: AdvancedGapRepairPlan,
        *,
        sealed_body: bytes | None,
        seal: Callable[[bytes], None],
    ) -> HistoricalMinuteExecutionResult:
        if repair.status == "BLOCKED":
            return HistoricalMinuteExecutionResult(0, 0, 0, ())
        body = sealed_body
        provider_requests = 0
        if body is None:
            body = self.transport.fetch(repair)
            provider_requests = 1
            seal(body)
        facts = tuple(self.parser(body, repair))
        if not facts:
            raise ValueError("historical 1m response has no exact facts")
        persisted = self.persister(facts)
        self.deriver(facts, self.DERIVED_INTERVALS, "RAW_UNADJUSTED")
        return HistoricalMinuteExecutionResult(
            provider_requests,
            len(body),
            persisted,
            self.DERIVED_INTERVALS,
        )


def derive_raw_historical_intervals(
    security,
    raw_one_minute_facts: Sequence[object],
    *,
    observed_at: datetime,
    ingestion_segment_id: UUID,
) -> tuple[object, ...]:
    raw = tuple(raw_one_minute_facts)
    if not raw or any(
        fact.revision.interval != "1m" or fact.revision.adjusted
        for fact in raw
    ):
        raise ValueError(
            "historical derivation requires raw unadjusted 1m facts"
        )
    five = derive_realtime_interval_facts(
        security,
        raw,
        target_interval="5m",
        observed_at=observed_at,
        ingestion_segment_id=ingestion_segment_id,
    )
    derived = list(five)
    for interval in ("15m", "30m", "1h", "1d"):
        derived.extend(
            derive_realtime_interval_facts(
                security,
                five,
                target_interval=interval,
                observed_at=observed_at,
                ingestion_segment_id=ingestion_segment_id,
            )
        )
    if any(fact.revision.adjusted for fact in derived):
        raise ValueError(
            "raw historical derivation cannot produce adjusted facts"
        )
    return tuple(derived)
