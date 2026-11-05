"""Canonical PostgreSQL producer for Advanced NBBO option marks."""
from __future__ import annotations

import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Callable
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import ValuationPolicy
from options.domain import MarkSource
from psycopg2 import Error as PsycopgError
from psycopg2.extras import RealDictCursor

from .advanced_options_control_plane import (
    AdvancedOptionsEnrollmentPlan,
    advanced_options_enrollment_plan_payload,
)
from .advanced_options_reference_state import AdvancedOptionsReferenceState
from .advanced_options_rolling_state import (
    AdvancedOptionsRollingState,
    OPTION_CONTRACTS_EXCLUDED_ABOVE_LIMIT,
    RollingAvailability,
    excluded_contract_share_exceeded,
)
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .option_portal_adapter import (
    MAX_PACKAGE_QUOTE_SKEW_MS,
    RealtimeQuoteEvidence,
)
from .option_portal_repository import (
    persist_realtime_quote_evidence_cursor,
)
from .option_source_flow import OptionSourceFlowIdentity


ADVANCED_NBBO_MARK_PLAN_VERSION = "advanced-options-nbbo-mark-plan-v1"
ADVANCED_NBBO_MAXIMUM_AGE_SECONDS = 1
_RETRYABLE_POSTGRES_SQLSTATES = frozenset({"40001", "40P01"})
# A contract left out for these has a good quote that is not fresh enough to value
# at this moment. That is a quiet contract, so it costs that contract its mark and
# does not count toward the share that says the data is broken.
_QUIET_MARK_REASONS = frozenset({
    "OPTION_NBBO_STALE_OR_NONCAUSAL",
    "OPTION_SPOT_SKEW_EXCEEDED",
    "UNDERLYING_SNAPSHOT_STALE_OR_NONCAUSAL",
})


def _utc(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


class AdvancedOptionsMarkStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsMarkEvidence:
    instrument_id: int
    contract_ticker: str
    status: AdvancedOptionsMarkStatus
    evidence_sha256: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        ticker = self.contract_ticker.strip().upper()
        reasons = tuple(sorted({
            value.strip().upper()
            for value in self.reason_codes
            if value.strip()
        }))
        if (
            type(self.instrument_id) is not int
            or self.instrument_id <= 0
            or not ticker
            or (
                self.status is AdvancedOptionsMarkStatus.READY
                and reasons
            )
            or (
                self.status is AdvancedOptionsMarkStatus.BLOCKED
                and not reasons
            )
        ):
            raise ValueError("Advanced Options mark evidence is invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsMarkProducerPlan:
    plan_id: UUID
    plan_sha256: str
    enrollment: AdvancedOptionsEnrollmentPlan
    reference_state: AdvancedOptionsReferenceState
    rolling_state: AdvancedOptionsRollingState
    source_flow: OptionSourceFlowIdentity
    cutoff: datetime
    producer_build_id: str
    producer_build_sha256: str
    valuation_policy: ValuationPolicy
    maximum_source_age_seconds: int = ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
    maximum_package_quote_skew_ms: int = MAX_PACKAGE_QUOTE_SKEW_MS
    expected_marks_sha256: str | None = None
    provider_calls_authorized: bool = False
    runtime_actions_authorized: bool = False
    market_state_committed_offset: int | None = None

    @property
    def control_plane(self):
        return self.enrollment.control_plane

    @property
    def frozen(self) -> bool:
        return self.expected_marks_sha256 is not None

    @property
    def zero_effect_counts(self) -> dict[str, int]:
        return {
            name: 0
            for name in (
                "provider",
                "socket",
                "service",
                "stages_v4",
                "publication",
                "delivery",
                "repair",
                "backfill",
                "replay",
                "archive",
                "retention",
            )
        }


@dataclass(frozen=True, slots=True)
class AdvancedOptionsMarkPreflight:
    status: AdvancedOptionsMarkStatus
    plan_id: UUID
    preflight_sha256: str
    marks_sha256: str
    marks: tuple[RealtimeQuoteEvidence, ...]
    evidence: tuple[AdvancedOptionsMarkEvidence, ...]

    @property
    def reason_codes(self) -> tuple[str, ...]:
        """What blocks the window. A ready result's rows name only what it left out."""
        if self.status is AdvancedOptionsMarkStatus.READY:
            return ()
        return tuple(sorted({
            reason
            for row in self.evidence
            for reason in row.reason_codes
        }))


class AdvancedOptionsMarkPreflightBlocked(ValueError):
    def __init__(self, result: AdvancedOptionsMarkPreflight) -> None:
        self.result = result
        super().__init__(
            "Advanced Options mark preflight is blocked: "
            + ",".join(result.reason_codes)
        )


def _plan_identity(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
    source_flow: OptionSourceFlowIdentity,
    cutoff: datetime,
    producer_build_id: str,
    producer_build_sha256: str,
    valuation_policy: ValuationPolicy,
    maximum_source_age_seconds: int,
    maximum_package_quote_skew_ms: int,
    expected_marks_sha256: str | None,
    market_state_committed_offset: int | None = None,
) -> dict[str, object]:
    control = enrollment.control_plane
    return {
        "contract": ADVANCED_NBBO_MARK_PLAN_VERSION,
        "enrollment_sha256": enrollment.enrollment_sha256,
        "cohort_id": control.cohort_id,
        "cohort_sha256": control.cohort_sha256,
        "profile_id": control.profile_id,
        "profile_sha256": control.profile_sha256,
        "manifest_id": control.manifest_id,
        "manifest_sha256": control.manifest_sha256,
        "slot_set_version": control.slot_set.version,
        "slot_set_sha256": control.slot_set_sha256,
        "slot_member_sha256": control.slot_set.member_sha256,
        "reference_state_id": reference_state.state_id,
        "reference_state_sha256": reference_state.state_sha256,
        "reference_generation": reference_state.reference_generation,
        "rolling_state_id": rolling_state.state_id,
        "rolling_state_sha256": rolling_state.state_sha256,
        "rolling_owner_epoch": rolling_state.owner_epoch,
        "rolling_committed_offset": rolling_state.committed_offset,
        "market_state_checkpoint_sha256": (
            rolling_state.market_state_checkpoint_sha256
        ),
        "source_flow_id": source_flow.source_flow_id,
        "source_flow_sha256": source_flow.source_flow_sha256,
        "journal_generation": source_flow.journal_generation,
        "stream_session_ids": source_flow.stream_session_ids,
        "cutoff": cutoff,
        "producer_build_id": producer_build_id,
        "producer_build_sha256": producer_build_sha256,
        "valuation_policy_version": valuation_policy.policy_version,
        "valuation_policy_sha256": valuation_policy.policy_sha256,
        "maximum_source_age_seconds": maximum_source_age_seconds,
        "maximum_package_quote_skew_ms": maximum_package_quote_skew_ms,
        "expected_marks_sha256": expected_marks_sha256,
        "provider_calls_authorized": False,
        "runtime_actions_authorized": False,
        **(
            {}
            if market_state_committed_offset is None
            else {
                "market_state_committed_offset": (
                    market_state_committed_offset
                ),
            }
        ),
    }


def _validate_plan(
    plan: AdvancedOptionsMarkProducerPlan,
) -> dict[str, object]:
    if not isinstance(plan, AdvancedOptionsMarkProducerPlan):
        raise TypeError("Advanced Options mark producer plan is invalid")
    advanced_options_enrollment_plan_payload(plan.enrollment)
    control = plan.control_plane
    cutoff = _utc(plan.cutoff, "cutoff")
    binding = plan.rolling_state.producer_binding
    if (
        plan.reference_state.session_date != control.session_date
        or plan.rolling_state.cohort_id != control.cohort_id
        or plan.rolling_state.session_date != control.session_date
        or plan.rolling_state.journal_generation
        != plan.source_flow.journal_generation
        or binding is None
        or binding.reference_state_id != plan.reference_state.state_id
        or binding.reference_state_sha256
        != plan.reference_state.state_sha256
        or binding.reference_generation
        != plan.reference_state.reference_generation
        or binding.source_flow_id != plan.source_flow.source_flow_id
        or binding.source_flow_sha256 != plan.source_flow.source_flow_sha256
    ):
        raise ValueError("Advanced Options mark generation scope changed")
    if (
        plan.source_flow.journal_generation
        != control.source_flow.journal_generation
        or plan.source_flow.stream_session_ids
        != control.source_flow.stream_session_ids
        or plan.source_flow.condition_semantics_sha256
        != control.pins.condition_semantics_sha256
    ):
        raise ValueError("Advanced Options mark source-flow scope changed")
    policy = plan.valuation_policy
    if (
        policy.policy_sha256 != control.pins.valuation_policy_sha256
        or policy.primary_model_mark_source
        is not MarkSource.ADVANCED_NBBO_MIDPOINT
        or policy.maximum_source_age_seconds
        != ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
        or MarkSource.ADVANCED_NBBO_MIDPOINT
        not in policy.allowed_entry_mark_sources
        or MarkSource.ADVANCED_NBBO_MIDPOINT
        not in policy.allowed_exit_mark_sources
    ):
        raise ValueError("Advanced Options mark valuation policy changed")
    if (
        not plan.producer_build_id.strip()
        or type(plan.maximum_source_age_seconds) is not int
        or plan.maximum_source_age_seconds
        != ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
        or type(plan.maximum_package_quote_skew_ms) is not int
        or plan.maximum_package_quote_skew_ms
        != MAX_PACKAGE_QUOTE_SKEW_MS
        or not binding.known_at <= cutoff < binding.valid_until
        or not plan.reference_state.known_at
        <= cutoff
        < plan.reference_state.valid_until
        or plan.provider_calls_authorized
        or plan.runtime_actions_authorized
        or (
            plan.market_state_committed_offset is not None
            and (
                type(plan.market_state_committed_offset) is not int
                or plan.market_state_committed_offset < 0
            )
        )
    ):
        raise ValueError("Advanced Options mark clocks, bounds or authority changed")
    build_sha256 = _sha256(
        plan.producer_build_sha256,
        "producer_build_sha256",
    )
    expected = (
        None
        if plan.expected_marks_sha256 is None
        else _sha256(plan.expected_marks_sha256, "expected_marks_sha256")
    )
    identity = _plan_identity(
        enrollment=plan.enrollment,
        reference_state=plan.reference_state,
        rolling_state=plan.rolling_state,
        source_flow=plan.source_flow,
        cutoff=cutoff,
        producer_build_id=plan.producer_build_id.strip(),
        producer_build_sha256=build_sha256,
        valuation_policy=policy,
        maximum_source_age_seconds=plan.maximum_source_age_seconds,
        maximum_package_quote_skew_ms=(
            plan.maximum_package_quote_skew_ms
        ),
        expected_marks_sha256=expected,
        market_state_committed_offset=plan.market_state_committed_offset,
    )
    digest = canonical_sha256(identity)
    identifier = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-nbbo-mark-plan:{digest}",
    )
    if plan.plan_sha256 != digest or plan.plan_id != identifier:
        raise ValueError("Advanced Options mark producer plan identity changed")
    return identity


def build_advanced_options_mark_producer_plan(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
    source_flow: OptionSourceFlowIdentity,
    cutoff: datetime,
    producer_build_id: str,
    producer_build_sha256: str,
    valuation_policy: ValuationPolicy,
    expected_marks_sha256: str | None = None,
    market_state_committed_offset: int | None = None,
) -> AdvancedOptionsMarkProducerPlan:
    normalized_cutoff = _utc(cutoff, "cutoff")
    identity = _plan_identity(
        enrollment=enrollment,
        reference_state=reference_state,
        rolling_state=rolling_state,
        source_flow=source_flow,
        cutoff=normalized_cutoff,
        producer_build_id=producer_build_id.strip(),
        producer_build_sha256=producer_build_sha256,
        valuation_policy=valuation_policy,
        maximum_source_age_seconds=ADVANCED_NBBO_MAXIMUM_AGE_SECONDS,
        maximum_package_quote_skew_ms=MAX_PACKAGE_QUOTE_SKEW_MS,
        expected_marks_sha256=expected_marks_sha256,
        market_state_committed_offset=market_state_committed_offset,
    )
    digest = canonical_sha256(identity)
    plan = AdvancedOptionsMarkProducerPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-nbbo-mark-plan:{digest}",
        ),
        plan_sha256=digest,
        enrollment=enrollment,
        reference_state=reference_state,
        rolling_state=rolling_state,
        source_flow=source_flow,
        cutoff=normalized_cutoff,
        producer_build_id=producer_build_id.strip(),
        producer_build_sha256=producer_build_sha256,
        valuation_policy=valuation_policy,
        expected_marks_sha256=expected_marks_sha256,
        market_state_committed_offset=market_state_committed_offset,
    )
    _validate_plan(plan)
    return plan


def advanced_options_mark_producer_plan_payload(
    plan: AdvancedOptionsMarkProducerPlan,
) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "plan_id": str(plan.plan_id),
        "plan_sha256": plan.plan_sha256,
        "identity": _validate_plan(plan),
        "effect_counts": plan.zero_effect_counts,
    }


def freeze_advanced_options_mark_producer_plan(
    plan: AdvancedOptionsMarkProducerPlan,
    preflight: AdvancedOptionsMarkPreflight,
) -> AdvancedOptionsMarkProducerPlan:
    _validate_plan(plan)
    if plan.frozen or preflight.plan_id != plan.plan_id:
        raise ValueError("Advanced Options mark preflight does not match PLAN")
    return build_advanced_options_mark_producer_plan(
        enrollment=plan.enrollment,
        reference_state=plan.reference_state,
        rolling_state=plan.rolling_state,
        source_flow=plan.source_flow,
        cutoff=plan.cutoff,
        producer_build_id=plan.producer_build_id,
        producer_build_sha256=plan.producer_build_sha256,
        valuation_policy=plan.valuation_policy,
        expected_marks_sha256=preflight.marks_sha256,
        market_state_committed_offset=plan.market_state_committed_offset,
    )


def _blocked(
    row,
    *reasons: str,
) -> AdvancedOptionsMarkEvidence:
    return AdvancedOptionsMarkEvidence(
        instrument_id=int(row["instrument_id"]),
        contract_ticker=str(row["contract_ticker"]),
        status=AdvancedOptionsMarkStatus.BLOCKED,
        evidence_sha256=canonical_sha256(tuple(row.values())),
        reason_codes=tuple(reasons),
    )


def assemble_advanced_options_marks_cursor(
    cursor,
    plan: AdvancedOptionsMarkProducerPlan,
) -> AdvancedOptionsMarkPreflight:
    _validate_plan(plan)
    control = plan.control_plane
    cursor.execute(
        "SELECT member.instrument_id,member.catalog_version_id,"
        "contract.contract_id,contract.contract_ticker,"
        "contract.underlying_security_id::text,version.expiration,"
        "quote.source_session,quote.source_sequence,quote.event_time_raw,"
        "quote.event_time_unit,quote.event_at,quote.received_at,"
        "quote.bid,quote.ask,quote.bid_size,quote.ask_size,"
        "quote.bid_exchange,quote.ask_exchange,quote.quality,"
        "quote.reason_codes,quote.session_id::text,"
        "quote.journal_generation::text,quote.shard_id,"
        "quote.source_offset,quote.payload_sha256 "
        "FROM rt.slot_set_current AS current "
        "JOIN rt.slot_set_members AS member "
        "ON member.cohort_id=current.cohort_id "
        "AND member.version=current.version AND member.channel='Q' "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=instrument.contract_id "
        "JOIN ref.option_contract_versions AS version "
        "ON version.version_id=member.catalog_version_id "
        "AND version.contract_id=contract.contract_id "
        "LEFT JOIN rt.quote_current AS quote "
        "ON quote.provider='massive' "
        "AND quote.instrument_id=member.instrument_id "
        "WHERE current.cohort_id=%s "
        "ORDER BY contract.contract_ticker",
        (str(control.cohort_id),),
    )
    rows = tuple(cursor.fetchall())
    selected = plan.enrollment.activity_selection.contracts
    expected = tuple(
        (
            row.instrument_id,
            row.catalog_version_id,
            row.contract_id,
            row.contract_ticker,
        )
        for row in selected
    )
    actual = tuple(
        (
            int(row["instrument_id"]),
            int(row["catalog_version_id"]),
            int(row["contract_id"]),
            str(row["contract_ticker"]),
        )
        for row in rows
    )
    if actual != expected:
        raise ValueError("Advanced Options mark active slot identity changed")
    tick_by_ticker = {
        row.ticker: row for row in plan.rolling_state.underlying_ticks
    }
    evidence_rows = []
    marks = []
    sessions = set(plan.source_flow.stream_session_ids)
    underlyer_by_id = {item.instrument_id: item.underlyer for item in selected}
    # Option heads hold normalized offsets, and the rolling state's own offset is
    # the raw offset of the flow checkpoint.
    head_offset_limit = (
        plan.rolling_state.committed_offset
        if plan.market_state_committed_offset is None
        else plan.market_state_committed_offset
    )
    for row in rows:
        reasons = []
        ticker = underlyer_by_id[int(row["instrument_id"])]
        tick = tick_by_ticker.get(ticker)
        if row["journal_generation"] is None:
            reasons.append("OPTION_NBBO_ABSENT")
        elif (
            str(row["journal_generation"])
            != str(plan.source_flow.journal_generation)
        ):
            reasons.append("OPTION_NBBO_GENERATION_MISMATCH")
        elif UUID(str(row["session_id"])) not in sessions:
            reasons.append("OPTION_NBBO_SOURCE_SESSION_MISMATCH")
        elif row["source_session"] != control.session_date:
            reasons.append("OPTION_NBBO_SOURCE_DATE_MISMATCH")
        elif int(row["source_offset"]) > head_offset_limit:
            reasons.append("OPTION_NBBO_AHEAD_OF_ROLLING_CHECKPOINT")
        # A quote from at or before the cutoff can arrive just after it, so the
        # event time decides whether it belongs to this window.
        if row["event_at"] is not None and (
            row["event_at"] > row["received_at"]
            or row["event_at"] > plan.cutoff
            or (
                plan.cutoff - row["received_at"]
            ).total_seconds() > plan.maximum_source_age_seconds
        ):
            reasons.append("OPTION_NBBO_STALE_OR_NONCAUSAL")
        if row["quality"] == "CROSSED":
            reasons.append("OPTION_NBBO_CROSSED_OR_NONPOSITIVE")
        elif (
            row["quality"] != "GOOD"
            or tuple(row["reason_codes"] or ())
            or row["bid"] is None
            or row["ask"] is None
            or row["bid_size"] is None
            or row["ask_size"] is None
            or row["bid_exchange"] is None
            or row["ask_exchange"] is None
        ):
            reasons.append("OPTION_NBBO_ONE_SIDED_OR_INVALID")
        elif (
            row["bid"] <= 0
            or row["ask"] <= 0
            or row["ask"] < row["bid"]
            or int(row["bid_size"]) <= 0
            or int(row["ask_size"]) <= 0
        ):
            reasons.append("OPTION_NBBO_CROSSED_OR_NONPOSITIVE")
        from .advanced_options_underlying_snapshot import (
            UnderlyingSnapshotTick,
        )

        if isinstance(tick, UnderlyingSnapshotTick):
            if row["event_at"] is not None:
                try:
                    tick.require_usable(
                        plan.cutoff,
                        row["event_at"],
                    )
                except ValueError:
                    reasons.append(
                        "UNDERLYING_SNAPSHOT_STALE_OR_NONCAUSAL"
                    )
        elif (
            tick is None
            or tick.availability is not RollingAvailability.READY
            or tick.quote is None
            or tick.trade is None
        ):
            reasons.append("UNDERLYING_SPOT_EVIDENCE_UNAVAILABLE")
        elif row["event_at"] is not None:
            spot_time = max(tick.quote.event_at, tick.trade.event_at)
            if (
                abs((row["event_at"] - spot_time).total_seconds())
                > plan.valuation_policy.maximum_option_spot_skew_seconds
            ):
                reasons.append("OPTION_SPOT_SKEW_EXCEEDED")
        if reasons:
            evidence_rows.append(_blocked(row, *reasons))
            continue
        midpoint = (row["bid"] + row["ask"]) / Decimal(2)
        mark_identity = {
            "contract": ADVANCED_NBBO_MARK_PLAN_VERSION,
            "cohort_id": control.cohort_id,
            "instrument_id": int(row["instrument_id"]),
            "contract_id": int(row["contract_id"]),
            "catalog_version_id": int(row["catalog_version_id"]),
            "journal_generation": plan.source_flow.journal_generation,
            "stream_session_id": UUID(str(row["session_id"])),
            "source_offset": int(row["source_offset"]),
        }
        mark_sha256 = canonical_sha256({
            **mark_identity,
            "contract_ticker": str(row["contract_ticker"]),
            "source_session": row["source_session"],
            "source_sequence": row["source_sequence"],
            "event_time_raw": row["event_time_raw"],
            "event_time_unit": row["event_time_unit"],
            "market_time": row["event_at"],
            "observed_time": row["received_at"],
            "bid": row["bid"],
            "ask": row["ask"],
            "midpoint": midpoint,
            "bid_size": int(row["bid_size"]),
            "ask_size": int(row["ask_size"]),
            "bid_exchange": int(row["bid_exchange"]),
            "ask_exchange": int(row["ask_exchange"]),
            "quote_payload_sha256": str(row["payload_sha256"]).strip(),
            "source_flow_sha256": plan.source_flow.source_flow_sha256,
            "market_state_checkpoint_sha256": (
                plan.rolling_state.market_state_checkpoint_sha256
            ),
            "rolling_state_id": plan.rolling_state.state_id,
            "rolling_state_sha256": plan.rolling_state.state_sha256,
            "reference_state_id": plan.reference_state.state_id,
            "reference_state_sha256": plan.reference_state.state_sha256,
            "producer_plan_sha256": plan.producer_build_sha256,
            "producer_build_id": plan.producer_build_id,
            "producer_build_sha256": plan.producer_build_sha256,
            "cutoff": plan.cutoff,
        })
        mark = RealtimeQuoteEvidence(
            evidence_id=uuid5(
                NAMESPACE_URL,
                "stockscreener:advanced-options-nbbo-mark:"
                f"{canonical_sha256(mark_identity)}",
            ),
            instrument_id=int(row["instrument_id"]),
            contract_id=int(row["contract_id"]),
            catalog_version_id=int(row["catalog_version_id"]),
            journal_generation=plan.source_flow.journal_generation,
            stream_session_id=UUID(str(row["session_id"])),
            source_offset=int(row["source_offset"]),
            market_time=row["event_at"],
            observed_time=row["received_at"],
            bid=row["bid"],
            ask=row["ask"],
            midpoint=midpoint,
            payload_sha256=mark_sha256,
            bid_size=int(row["bid_size"]),
            ask_size=int(row["ask_size"]),
            bid_exchange=int(row["bid_exchange"]),
            ask_exchange=int(row["ask_exchange"]),
            quote_payload_sha256=str(row["payload_sha256"]).strip(),
            source_flow_id=plan.source_flow.source_flow_id,
            source_flow_sha256=plan.source_flow.source_flow_sha256,
            market_state_checkpoint_sha256=(
                plan.rolling_state.market_state_checkpoint_sha256
            ),
            rolling_state_id=plan.rolling_state.state_id,
            rolling_state_sha256=plan.rolling_state.state_sha256,
            reference_state_id=plan.reference_state.state_id,
            reference_state_sha256=plan.reference_state.state_sha256,
            producer_plan_sha256=plan.producer_build_sha256,
        )
        marks.append(mark)
        evidence_rows.append(AdvancedOptionsMarkEvidence(
            instrument_id=mark.instrument_id,
            contract_ticker=str(row["contract_ticker"]),
            status=AdvancedOptionsMarkStatus.READY,
            evidence_sha256=mark.payload_sha256,
        ))
    # A contract without a mark is left out for this window. Quote skew belongs to
    # the legs of one package and is checked when that package is marked, never
    # across the whole contract set.
    totals: dict[str, int] = {}
    broken: dict[str, int] = {}
    for row in evidence_rows:
        underlyer = underlyer_by_id[row.instrument_id]
        totals[underlyer] = totals.get(underlyer, 0) + 1
        if set(row.reason_codes) - _QUIET_MARK_REASONS:
            broken[underlyer] = broken.get(underlyer, 0) + 1
    over_limit = {
        underlyer
        for underlyer, count in broken.items()
        if excluded_contract_share_exceeded(count, totals[underlyer])
    }
    if over_limit:
        marks = []
        evidence_rows = [
            replace(
                row,
                reason_codes=(
                    *row.reason_codes,
                    OPTION_CONTRACTS_EXCLUDED_ABOVE_LIMIT,
                ),
            )
            if row.reason_codes
            and underlyer_by_id[row.instrument_id] in over_limit
            else row
            for row in evidence_rows
        ]
    marks_sha256 = canonical_sha256(tuple(
        (
            row.evidence_id,
            row.payload_sha256,
            row.instrument_id,
            row.catalog_version_id,
        )
        for row in marks
    ))
    status = (
        AdvancedOptionsMarkStatus.BLOCKED
        if over_limit
        else AdvancedOptionsMarkStatus.READY
    )
    result_identity = {
        "plan_id": plan.plan_id,
        "status": status,
        "marks_sha256": marks_sha256,
        "marks": tuple(marks),
        "evidence": tuple(evidence_rows),
    }
    result = AdvancedOptionsMarkPreflight(
        status=status,
        plan_id=plan.plan_id,
        preflight_sha256=canonical_sha256(result_identity),
        marks_sha256=marks_sha256,
        marks=tuple(marks),
        evidence=tuple(evidence_rows),
    )
    if (
        plan.expected_marks_sha256 is not None
        and result.status is AdvancedOptionsMarkStatus.READY
        and plan.expected_marks_sha256 != marks_sha256
    ):
        raise ValueError("Advanced Options mark replay diverged from frozen PLAN")
    return result


class PostgresAdvancedOptionsMarkProducer:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        plan: AdvancedOptionsMarkProducerPlan,
        *,
        maximum_attempts: int = 3,
        retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("Advanced Options mark provider is invalid")
        _validate_plan(plan)
        if (
            type(maximum_attempts) is not int
            or maximum_attempts < 1
            or retry_initial_seconds < 0
            or not callable(wait)
        ):
            raise ValueError("Advanced Options mark retry is invalid")
        self._provider = provider
        self._plan = plan
        self._maximum_attempts = maximum_attempts
        self._retry_initial_seconds = retry_initial_seconds
        self._wait = wait

    def _assemble(self, unit) -> AdvancedOptionsMarkPreflight:
        with unit.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            return assemble_advanced_options_marks_cursor(cursor, self._plan)

    def preflight(self) -> AdvancedOptionsMarkPreflight:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return self._assemble(unit)

    def produce(self) -> tuple[RealtimeQuoteEvidence, ...]:
        if not self._plan.frozen:
            raise ValueError(
                "Advanced Options mark PLAN is not frozen by preflight"
            )
        for attempt in range(1, self._maximum_attempts + 1):
            try:
                with self._provider.transaction(
                    APP_ROLE,
                    isolation_level="SERIALIZABLE",
                ) as unit:
                    result = self._assemble(unit)
                    if result.status is AdvancedOptionsMarkStatus.BLOCKED:
                        raise AdvancedOptionsMarkPreflightBlocked(result)
                    return tuple(
                        persist_realtime_quote_evidence_cursor(
                            unit.cursor,
                            mark,
                        )
                        for mark in result.marks
                    )
            except PsycopgError as error:
                if (
                    error.pgcode not in _RETRYABLE_POSTGRES_SQLSTATES
                    or attempt == self._maximum_attempts
                ):
                    raise
                self._wait(
                    self._retry_initial_seconds * (2 ** (attempt - 1))
                )
        raise AssertionError("unreachable mark producer retry state")
