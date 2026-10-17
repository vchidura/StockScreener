"""Advanced gap evidence, authorized repair and cold-start backfill contracts."""
from __future__ import annotations

import hashlib
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
from .contracts import canonical_sha256
from .gap_repair_plan import plan_gap_repair_cursor
from .equity_interval_derivation import derive_realtime_interval_facts
from .provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from .session_close_coverage import five_minute_session


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
    classification_sha256 = canonical_sha256({
        "contract": "advanced-gap-continuity-v1",
        "evidence": evidence,
        "classification": classification.value,
    })
    return classification, classification_sha256


def record_gap_reconnect_evidence_cursor(
    cursor,
    evidence: GapReconnectEvidence,
) -> GapContinuity:
    classification, classification_sha256 = classify_gap_continuity(evidence)
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
            classification_sha256,
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
            gap_id=UUID(str(row[0])),
            prior_journal_generation=UUID(str(row[1])),
            reconnect_journal_generation=reconnect_journal_generation,
            disconnect_at=row[2],
            reconnect_at=row[3],
            last_durable_offset=int(row[4]),
            first_post_reconnect_offset=first_offset,
            last_durable_watermark=row[5],
            first_post_reconnect_event_at=first_event_at,
        ),
    )


def attach_gap_dependencies_cursor(
    cursor,
    gap_id: UUID,
    window_ids: tuple[UUID, ...],
    *,
    blocked_at: datetime,
) -> None:
    checked = _utc(blocked_at, "blocked_at")
    for window_id in tuple(sorted(set(window_ids), key=str)):
        cursor.execute(
            "INSERT INTO rt.advanced_gap_dependencies ("
            "gap_id,window_id,status,blocked_at) "
            "VALUES (%s,%s,'BLOCKED',%s) ON CONFLICT DO NOTHING",
            (str(gap_id), str(window_id), checked),
        )


def _open_gap_alert_cursor(
    cursor,
    gap_id: UUID,
    *,
    reason_code: str,
    checked_at: datetime,
) -> UUID:
    reason = reason_code.strip().upper()
    alert_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-gap-alert:{gap_id}:{reason}",
    )
    cursor.execute(
        "INSERT INTO rt.advanced_gap_alerts ("
        "alert_id,gap_id,reason_code,severity,status,created_at) "
        "VALUES (%s,%s,%s,'CRITICAL','OPEN',%s) "
        "ON CONFLICT (gap_id) DO NOTHING",
        (
            str(alert_id),
            str(gap_id),
            reason,
            _utc(checked_at, "checked_at"),
        ),
    )
    return alert_id


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
        "contract": "advanced-gap-repair-v1",
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
    reason_code = None if authorized else (
        "NO_TRADE_EVIDENCE"
        if row[4] == GapContinuity.NO_TRADE_INTERVAL.value
        else "REPAIR_UNAUTHORIZED"
    )
    cursor.execute(
        "INSERT INTO rt.advanced_gap_repairs ("
        "repair_id,gap_id,plan_id,allocation_kind,instrument_id,ticker,"
        "requested_from,requested_to,scope_sha256,reserved_response_bytes,"
        "status,reason_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (repair_id) DO NOTHING",
        (
            str(repair_id),
            str(gap_id),
            str(plan_id),
            allocation_kind,
            row[1],
            ticker,
            row[2],
            row[3],
            scope_sha256,
            reserved_response_bytes,
            status,
            reason_code,
        ),
    )
    if not authorized:
        cursor.execute(
            "UPDATE rt.stream_gaps SET operator_alert_required=true "
            "WHERE gap_id=%s",
            (str(gap_id),),
        )
        _open_gap_alert_cursor(
            cursor,
            gap_id,
            reason_code=reason_code,
            checked_at=datetime.now(timezone.utc),
        )
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
        reason_code,
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
        "usage.rest_repair_decoded_byte_count,repair.status,"
        "repair.request_id "
        "FROM rt.advanced_gap_repairs AS repair "
        "JOIN rt.advanced_ingress_plans AS plan USING(plan_id) "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
        "WHERE repair.repair_id=%s FOR UPDATE OF repair,plan,usage",
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
            "UPDATE rt.advanced_gap_repairs SET status='BLOCKED',"
            "reason_code='REPAIR_BUDGET_EXHAUSTED' WHERE repair_id=%s",
            (str(repair.repair_id),),
        )
        cursor.execute(
            "UPDATE rt.stream_gaps SET operator_alert_required=true "
            "WHERE gap_id=%s",
            (str(repair.gap_id),),
        )
        _open_gap_alert_cursor(
            cursor,
            repair.gap_id,
            reason_code="REPAIR_BUDGET_EXHAUSTED",
            checked_at=reserved_at,
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
    reservation = ProviderRequestReservation(
        request_id,
        scope.account_alias,
        ProviderRequestLane.EQUITY_HISTORY,
        "ADVANCED_HISTORICAL_1M",
        repair.scope_sha256,
        _utc(reserved_at, "reserved_at"),
        cohort_id=UUID(str(row[1])),
    )
    reserve_coordinated_provider_request_cursor(
        cursor,
        scope,
        reservation,
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
        "UPDATE rt.advanced_gap_repairs SET status='RESERVED',"
        "request_id=%s,reserved_at=%s WHERE repair_id=%s",
        (
            str(request_id),
            _utc(reserved_at, "reserved_at"),
            str(repair.repair_id),
        ),
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
        "SELECT request_id,requested_from,requested_to,status "
        "FROM rt.advanced_gap_repairs WHERE repair_id=%s FOR UPDATE",
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
    digest = hashlib.sha256(body).hexdigest()
    cursor.execute(
        "INSERT INTO rt.advanced_repair_payloads ("
        "payload_id,repair_id,request_id,payload_sha256,payload_bytes,"
        "body,sealed_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (
            str(payload_id),
            str(repair_id),
            str(request_id),
            digest,
            len(body),
            body,
            sealed,
        ),
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
        "UPDATE rt.advanced_gap_repairs SET status='SEALED',"
        "sealed_payload_id=%s,sealed_at=%s WHERE repair_id=%s",
        (str(payload_id), sealed, str(repair_id)),
    )
    return payload_id


def load_sealed_advanced_repair_payload_cursor(
    cursor,
    repair_id: UUID,
) -> bytes:
    cursor.execute(
        "SELECT payload_id,body,payload_sha256 "
        "FROM rt.advanced_repair_payloads WHERE repair_id=%s",
        (str(repair_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("sealed advanced repair payload is unavailable")
    body = bytes(row[1])
    if hashlib.sha256(body).hexdigest() != row[2]:
        raise ValueError("sealed advanced repair payload checksum mismatch")
    stored = load_raw_payload_cursor(cursor, UUID(str(row[0])))
    if stored != body:
        raise ValueError("sealed payload stores disagree")
    return body


def verify_and_close_gap_repair_cursor(
    cursor,
    repair_id: UUID,
    *,
    checked_at: datetime,
) -> str:
    cursor.execute(
        "SELECT repair.gap_id,repair.plan_id,repair.instrument_id,"
        "repair.requested_from,repair.requested_to,repair.status "
        "FROM rt.advanced_gap_repairs AS repair "
        "WHERE repair.repair_id=%s FOR UPDATE",
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
        (
            str(repair[1]),
            int(repair[2]),
            repair[3],
            repair[4],
        ),
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
    checked = _utc(checked_at, "checked_at")
    cursor.execute(
        "UPDATE rt.advanced_gap_repairs SET status='REPAIRED',"
        "canonical_coverage_sha256=%s,verified_at=%s,repaired_at=%s "
        "WHERE repair_id=%s",
        (coverage_sha256, checked, checked, str(repair_id)),
    )
    cursor.execute(
        "UPDATE rt.stream_gaps SET recovery_status='REPAIRED',"
        "repair_reference=%s,operator_alert_required=false "
        "WHERE gap_id=%s",
        (str(repair_id), str(repair[0])),
    )
    cursor.execute(
        "UPDATE rt.advanced_gap_dependencies SET status='RELEASED',"
        "released_at=%s WHERE gap_id=%s AND status='BLOCKED'",
        (checked, str(repair[0])),
    )
    cursor.execute(
        "UPDATE rt.advanced_gap_alerts SET status='RESOLVED',resolved_at=%s "
        "WHERE gap_id=%s AND status='OPEN'",
        (checked, str(repair[0])),
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
        "SELECT cohort_id,universe_target,"
        "maximum_rest_repair_requests,maximum_rest_repair_decoded_bytes "
        "FROM rt.advanced_ingress_plans WHERE plan_id=%s",
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
    one_minute_plans = [
        item for item in coverage["plans"] if item["interval"] == "1m"
    ]
    for session_plan in one_minute_plans:
        for missing in session_plan["missing_ranges"]:
            expected_1m = int(missing["missing_members"])
            expected_5m = ceil(expected_1m / 5)
            identity = {
                "contract": "advanced-cold-start-work-v1",
                "plan_id": plan_id,
                "session_date": session_plan["session_date"],
                **missing,
            }
            work_id = uuid5(
                NAMESPACE_URL,
                "stockscreener:advanced-cold-start-work:"
                f"{canonical_sha256(identity)}",
            )
            work.append(
                ColdStartWorkItem(
                    work_id,
                    session_plan["session_date"],
                    int(missing["instrument_id"]),
                    str(missing["ticker"]),
                    missing["requested_from"],
                    missing["requested_to"],
                    expected_1m,
                    expected_5m,
                )
            )
    scope = {
        "contract": "advanced-cold-start-backfill-v1",
        "plan_id": plan_id,
        "cohort_id": cohort_id,
        "sessions": sessions,
        "universe_target": int(row[1]),
        "request_ceiling": request_ceiling,
        "decoded_byte_ceiling": decoded_byte_ceiling,
        "expected_cost": expected_cost.strip(),
        "work": tuple(work),
    }
    scope_sha256 = canonical_sha256(scope)
    backfill_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-cold-start:{scope_sha256}",
    )
    authorized = (
        len(work) <= request_ceiling <= int(row[2])
        and decoded_byte_ceiling <= int(row[3])
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
        "INSERT INTO rt.advanced_backfill_runs ("
        "backfill_id,plan_id,cohort_id,session_dates,universe_target,"
        "request_ceiling,decoded_byte_ceiling,expected_cost,scope_sha256,"
        "state,reason_code,next_action) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (backfill_id) DO NOTHING",
        (
            str(backfill_id),
            str(plan_id),
            str(cohort_id),
            list(sessions),
            int(row[1]),
            request_ceiling,
            decoded_byte_ceiling,
            expected_cost.strip(),
            scope_sha256,
            state,
            reason,
            next_action,
        ),
    )
    for item in work:
        cursor.execute(
            "INSERT INTO rt.advanced_backfill_work ("
            "work_id,backfill_id,session_date,instrument_id,ticker,"
            "requested_from,requested_to,expected_raw_1m,expected_raw_5m,"
            "status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (work_id) DO NOTHING",
            (
                str(item.work_id),
                str(backfill_id),
                item.session_date,
                item.instrument_id,
                item.ticker,
                item.requested_from,
                item.requested_to,
                item.expected_raw_1m,
                item.expected_raw_5m,
                "MISSING" if authorized else "BLOCKED",
            ),
        )
    cursor.execute(
        "SELECT state,reason_code,next_action "
        "FROM rt.advanced_backfill_runs WHERE backfill_id=%s",
        (str(backfill_id),),
    )
    stored_state, stored_reason, stored_next_action = cursor.fetchone()
    return ColdStartBackfillPlan(
        backfill_id,
        plan_id,
        cohort_id,
        sessions,
        int(row[1]),
        request_ceiling,
        decoded_byte_ceiling,
        expected_cost.strip(),
        scope_sha256,
        str(stored_state),
        str(stored_reason),
        str(stored_next_action),
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
    allowed = {
        "BACKFILL_PLANNED",
        "BACKFILL_RUNNING",
        "BACKFILL_BLOCKED",
        "BACKFILL_COMPLETE",
        "WAIT_NEXT_SESSION",
    }
    if state not in allowed or not reason_code.strip() or not next_action.strip():
        raise ValueError("backfill controller state is invalid")
    completed = (
        _utc(checked_at, "checked_at")
        if state in {"BACKFILL_COMPLETE", "WAIT_NEXT_SESSION"}
        else None
    )
    cursor.execute(
        "UPDATE rt.advanced_backfill_runs SET state=%s,reason_code=%s,"
        "next_action=%s,updated_at=%s,completed_at=COALESCE(completed_at,%s) "
        "WHERE backfill_id=%s RETURNING backfill_id",
        (
            state,
            reason_code.strip(),
            next_action.strip(),
            _utc(checked_at, "checked_at"),
            completed,
            str(backfill_id),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("backfill controller state update was rejected")


def reserve_backfill_work_cursor(
    cursor,
    work_id: UUID,
    *,
    reserved_response_bytes: int,
    reserved_at: datetime,
) -> UUID:
    cursor.execute(
        "SELECT work.backfill_id,work.session_date,work.ticker,"
        "work.requested_from,work.requested_to,work.status,work.request_id,"
        "run.plan_id,run.request_ceiling,run.decoded_byte_ceiling,"
        "run.requests_reserved,run.bytes_reserved,run.state,"
        "plan.cohort_id,plan.session_date,plan.authorized_end_at,"
        "plan.maximum_rest_repair_requests,"
        "plan.maximum_rest_repair_decoded_bytes,"
        "auth.authorization_reference,"
        "usage.rest_repair_request_count,"
        "usage.rest_repair_decoded_byte_count "
        "FROM rt.advanced_backfill_work AS work "
        "JOIN rt.advanced_backfill_runs AS run USING(backfill_id) "
        "JOIN rt.advanced_ingress_plans AS plan USING(plan_id) "
        "JOIN rt.advanced_stream_authorizations AS auth "
        "USING(authorization_id) "
        "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
        "WHERE work.work_id=%s "
        "FOR UPDATE OF work,run,plan,usage",
        (str(work_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("cold-start work is unavailable")
    if row[5] != "MISSING":
        if row[6] is None:
            raise ValueError("cold-start work reservation is inconsistent")
        return UUID(str(row[6]))
    if (
        row[12] not in {"BACKFILL_PLANNED", "BACKFILL_RUNNING"}
        or int(row[10]) + 1 > int(row[8])
        or int(row[11]) + reserved_response_bytes > int(row[9])
        or int(row[19]) + 1 > int(row[16])
        or int(row[20]) + reserved_response_bytes > int(row[17])
    ):
        cursor.execute(
            "UPDATE rt.advanced_backfill_work SET status='BLOCKED' "
            "WHERE work_id=%s",
            (str(work_id),),
        )
        transition_backfill_state_cursor(
            cursor,
            UUID(str(row[0])),
            state="BACKFILL_BLOCKED",
            reason_code="STARTUP_BACKFILL_BUDGET_EXHAUSTED",
            next_action="OPERATOR_AUTHORIZATION_REQUIRED",
            checked_at=reserved_at,
        )
        raise ProviderBudgetExceeded("startup backfill budget is exhausted")
    request_sha256 = canonical_sha256({
        "contract": "advanced-startup-raw-1m-v1",
        "work_id": work_id,
        "session_date": row[1],
        "ticker": row[2],
        "requested_from": row[3],
        "requested_to": row[4],
    })
    request_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-startup-request:{request_sha256}",
    )
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=row[14],
        authorization_reference=str(row[18]),
        provider_authorization_reference="stocks-advanced",
        expected_cost="startup backfill allocation",
        request_ceiling=int(row[16]),
        decoded_byte_ceiling=int(row[17]),
        lanes=frozenset({ProviderRequestLane.EQUITY_HISTORY}),
        authorized_until=row[15],
    )
    reserve_coordinated_provider_request_cursor(
        cursor,
        scope,
        ProviderRequestReservation(
            request_id,
            scope.account_alias,
            ProviderRequestLane.EQUITY_HISTORY,
            "ADVANCED_HISTORICAL_1M",
            request_sha256,
            _utc(reserved_at, "reserved_at"),
            cohort_id=UUID(str(row[13])),
        ),
        reserved_response_bytes=reserved_response_bytes,
    )
    cursor.execute(
        "UPDATE rt.advanced_ingress_plan_usage SET "
        "rest_repair_request_count=rest_repair_request_count+1,"
        "rest_repair_decoded_byte_count="
        "rest_repair_decoded_byte_count+%s,updated_at=%s "
        "WHERE plan_id=%s",
        (
            reserved_response_bytes,
            _utc(reserved_at, "reserved_at"),
            str(row[7]),
        ),
    )
    cursor.execute(
        "UPDATE rt.advanced_backfill_runs SET state='BACKFILL_RUNNING',"
        "reason_code='STARTUP_REQUEST_RESERVED',"
        "next_action='FETCH_AND_SEAL_RAW_1M',"
        "requests_reserved=requests_reserved+1,"
        "bytes_reserved=bytes_reserved+%s,updated_at=%s "
        "WHERE backfill_id=%s",
        (
            reserved_response_bytes,
            _utc(reserved_at, "reserved_at"),
            str(row[0]),
        ),
    )
    cursor.execute(
        "UPDATE rt.advanced_backfill_work SET status='RESERVED',"
        "request_id=%s WHERE work_id=%s",
        (str(request_id), str(work_id)),
    )
    return request_id


def seal_backfill_work_payload_cursor(
    cursor,
    work_id: UUID,
    body: bytes,
    *,
    sealed_at: datetime,
) -> UUID:
    cursor.execute(
        "SELECT request_id,requested_from,requested_to,status "
        "FROM rt.advanced_backfill_work WHERE work_id=%s FOR UPDATE",
        (str(work_id),),
    )
    row = cursor.fetchone()
    if row is None or row[0] is None or row[3] != "RESERVED":
        raise ValueError("startup work is not reserved for sealing")
    request_id = UUID(str(row[0]))
    sealed = _utc(sealed_at, "sealed_at")
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
        "UPDATE rt.advanced_backfill_work SET status='SEALED' "
        "WHERE work_id=%s",
        (str(work_id),),
    )
    return payload_id


def verify_backfill_work_cursor(
    cursor,
    work_id: UUID,
    *,
    checked_at: datetime,
) -> str:
    cursor.execute(
        "SELECT work.backfill_id,work.instrument_id,work.requested_from,"
        "work.requested_to,work.expected_raw_1m,work.expected_raw_5m,"
        "run.cohort_id,work.status "
        "FROM rt.advanced_backfill_work AS work "
        "JOIN rt.advanced_backfill_runs AS run USING(backfill_id) "
        "WHERE work.work_id=%s FOR UPDATE OF work,run",
        (str(work_id),),
    )
    row = cursor.fetchone()
    if row is None or row[7] not in {"SEALED", "PERSISTED"}:
        raise ValueError("startup work is not ready for coverage verification")
    cursor.execute(
        "SELECT current.interval,COUNT(DISTINCT current.bar_start) "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.cohort_id=%s AND current.instrument_id=%s "
        "AND current.bar_start>=%s AND current.bar_end<=%s "
        "AND current.interval IN ('1m','5m') "
        "AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.is_final GROUP BY current.interval",
        (str(row[6]), int(row[1]), row[2], row[3]),
    )
    counts = {str(key): int(value) for key, value in cursor.fetchall()}
    if (
        counts.get("1m", 0) != int(row[4])
        or counts.get("5m", 0) != int(row[5])
    ):
        raise ValueError("startup work exact 1m/5m coverage is incomplete")
    coverage_sha256 = canonical_sha256({
        "work_id": work_id,
        "requested_from": row[2],
        "requested_to": row[3],
        "counts": counts,
    })
    cursor.execute(
        "UPDATE rt.advanced_backfill_work SET status='VERIFIED',"
        "coverage_sha256=%s,completed_at=%s WHERE work_id=%s",
        (
            coverage_sha256,
            _utc(checked_at, "checked_at"),
            str(work_id),
        ),
    )
    cursor.execute(
        "SELECT COUNT(*) FROM rt.advanced_backfill_work "
        "WHERE backfill_id=%s AND status<>'VERIFIED'",
        (str(row[0]),),
    )
    if int(cursor.fetchone()[0]) == 0:
        transition_backfill_state_cursor(
            cursor,
            UUID(str(row[0])),
            state="BACKFILL_COMPLETE",
            reason_code="EXACT_1M_5M_COVERAGE_COMPLETE",
            next_action="PUBLISH_COHORT_LOCAL_RELEASE",
            checked_at=checked_at,
        )
    return coverage_sha256


def publish_cohort_local_backfill_release_cursor(
    cursor,
    backfill_id: UUID,
    *,
    coverage_sha256: str,
    job_barrier_sha256: str,
    published_at: datetime,
) -> UUID:
    cursor.execute(
        "SELECT cohort_id,state FROM rt.advanced_backfill_runs "
        "WHERE backfill_id=%s FOR UPDATE",
        (str(backfill_id),),
    )
    row = cursor.fetchone()
    if row is None or row[1] != "BACKFILL_COMPLETE":
        raise ValueError("backfill is not complete for release")
    cursor.execute(
        "SELECT COUNT(*) FROM rt.advanced_backfill_work "
        "WHERE backfill_id=%s AND status<>'VERIFIED'",
        (str(backfill_id),),
    )
    if int(cursor.fetchone()[0]):
        raise ValueError("backfill work remains incomplete")
    release_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-backfill-release:"
        f"{backfill_id}:{coverage_sha256}:{job_barrier_sha256}",
    )
    cursor.execute(
        "INSERT INTO rt.advanced_backfill_releases ("
        "release_id,backfill_id,cohort_id,coverage_sha256,"
        "job_barrier_sha256,published_at,platform_pointer_advanced) "
        "VALUES (%s,%s,%s,%s,%s,%s,false) "
        "ON CONFLICT (release_id) DO NOTHING",
        (
            str(release_id),
            str(backfill_id),
            str(row[0]),
            coverage_sha256,
            job_barrier_sha256,
            _utc(published_at, "published_at"),
        ),
    )
    transition_backfill_state_cursor(
        cursor,
        backfill_id,
        state="WAIT_NEXT_SESSION",
        reason_code="COHORT_LOCAL_RELEASE_READY",
        next_action="WAIT_FOR_NEXT_XNYS_SESSION",
        checked_at=published_at,
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
        provider_requests = 0
        body = sealed_body
        if body is None:
            body = self.transport.fetch(repair)
            provider_requests = 1
            seal(body)
        facts = tuple(self.parser(body, repair))
        if not facts:
            raise ValueError("historical 1m response has no exact facts")
        persisted = self.persister(facts)
        self.deriver(
            facts,
            self.DERIVED_INTERVALS,
            "RAW_UNADJUSTED",
        )
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
