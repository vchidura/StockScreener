"""Canonical PostgreSQL loader for exact Advanced Options native inputs."""
from __future__ import annotations

import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Callable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)
from options.calendar import OptionExchangeCalendar
from options.dual_origin import ActivitySource
from options.stock_bar_detection import detect_canonical_stock_signals
from psycopg2 import Error as PsycopgError
from psycopg2.extras import RealDictCursor

from .advanced_options_control_plane import (
    AdvancedOptionsEnrollmentPlan,
    advanced_options_enrollment_plan_payload,
)
from .advanced_options_equity_input import (
    AdvancedOptionsEquityContextRequirement,
    advanced_options_equity_context_requirements_payload,
    load_advanced_options_equity_context_cursor,
)
from .advanced_options_native_adapters import (
    AdvancedO1Invocation,
    AdvancedOSInvocation,
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
    advanced_o3_invocation_document,
    decode_advanced_o3_invocation_document,
)
from .advanced_options_producers import AdvancedOptionsProducerPlan
from .advanced_options_reference_state import AdvancedOptionsReferenceState
from .advanced_options_rolling_state import AdvancedOptionsRollingState
from .contracts import canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .option_portal_adapter import RealtimeOptionPackage
from .option_portal_repository import load_realtime_option_packages_cursor
from .option_source_flow import OptionSourceFlowIdentity
from .orchestration_repository import CycleReceiptSpec
from .preflight import DetectorFamily


NATIVE_INPUT_PLAN_VERSION = "advanced-options-native-input-plan-v2"
_RETRYABLE_POSTGRES_SQLSTATES = frozenset({"40001", "40P01"})


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


class NativeInputPlanStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNativeInputEvidence:
    member_kind: str
    member_key: str
    available: bool
    evidence_sha256: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        kind = self.member_kind.strip().upper()
        key = self.member_key.strip()
        reasons = tuple(sorted({
            value.strip().upper()
            for value in self.reason_codes
            if value.strip()
        }))
        if (
            not kind
            or not key
            or self.available == bool(reasons)
        ):
            raise ValueError("native input evidence availability is invalid")
        object.__setattr__(self, "member_kind", kind)
        object.__setattr__(self, "member_key", key)
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNativeInputPlan:
    plan_id: UUID
    plan_sha256: str
    enrollment: AdvancedOptionsEnrollmentPlan
    producer_plan: AdvancedOptionsProducerPlan
    reference_state: AdvancedOptionsReferenceState
    rolling_state: AdvancedOptionsRollingState
    source_flow: OptionSourceFlowIdentity
    cutoff: datetime
    equity_input_receipt_id: UUID
    equity_input_receipt: CycleReceiptSpec
    equity_window_id: UUID
    maximum_equity_input_age_seconds: int
    loader_build_id: str
    history_source_cohort_id: UUID
    equity_context_requirements: tuple[
        AdvancedOptionsEquityContextRequirement,
        ...,
    ]
    expected_input_sha256: str | None = None
    provider_calls_authorized: bool = False
    runtime_actions_authorized: bool = False

    @property
    def control_plane(self):
        return self.enrollment.control_plane

    @property
    def frozen(self) -> bool:
        return self.expected_input_sha256 is not None

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
class AdvancedOptionsNativeInputPreflight:
    status: NativeInputPlanStatus
    plan_id: UUID
    preflight_sha256: str
    input_sha256: str
    executor_membership_sha256: str
    bundle: AdvancedOptionsNativeInputBundle
    evidence: tuple[AdvancedOptionsNativeInputEvidence, ...]

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(sorted({
            reason
            for row in self.evidence
            for reason in row.reason_codes
        }))


class AdvancedOptionsNativeInputBlocked(ValueError):
    def __init__(self, result: AdvancedOptionsNativeInputPreflight) -> None:
        self.result = result
        super().__init__(
            "Advanced Options native input preflight is blocked: "
            + ",".join(result.reason_codes)
        )


def _plan_identity(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    producer_plan: AdvancedOptionsProducerPlan,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
    source_flow: OptionSourceFlowIdentity,
    cutoff: datetime,
    equity_input_receipt_id: UUID,
    equity_input_receipt: CycleReceiptSpec,
    equity_window_id: UUID,
    maximum_equity_input_age_seconds: int,
    loader_build_id: str,
    history_source_cohort_id: UUID,
    equity_context_requirements: tuple[
        AdvancedOptionsEquityContextRequirement,
        ...,
    ],
    expected_input_sha256: str | None,
) -> dict[str, object]:
    control = enrollment.control_plane
    return {
        "contract": NATIVE_INPUT_PLAN_VERSION,
        "enrollment_sha256": enrollment.enrollment_sha256,
        "control_plane_plan_id": control.plan_id,
        "cohort_id": control.cohort_id,
        "cohort_sha256": control.cohort_sha256,
        "profile_id": control.profile_id,
        "profile_sha256": control.profile_sha256,
        "manifest_id": control.manifest_id,
        "manifest_sha256": control.manifest_sha256,
        "slot_set_version": control.slot_set.version,
        "slot_set_sha256": control.slot_set_sha256,
        "slot_member_sha256": control.slot_set.member_sha256,
        "producer_plan_id": producer_plan.plan_id,
        "producer_plan_sha256": producer_plan.plan_sha256,
        "reference_state_id": reference_state.state_id,
        "reference_state_sha256": reference_state.state_sha256,
        "reference_generation": reference_state.reference_generation,
        "rolling_state_id": rolling_state.state_id,
        "rolling_state_sha256": rolling_state.state_sha256,
        "rolling_owner_epoch": rolling_state.owner_epoch,
        "rolling_committed_offset": rolling_state.committed_offset,
        "source_flow_id": source_flow.source_flow_id,
        "source_flow_sha256": source_flow.source_flow_sha256,
        "journal_generation": source_flow.journal_generation,
        "cutoff": cutoff,
        "equity_cohort_id": enrollment.equity_plan.cohort_id,
        "history_source_cohort_id": history_source_cohort_id,
        "equity_manifest_id": enrollment.equity_plan.manifest_id,
        "equity_input_receipt_id": equity_input_receipt_id,
        "equity_input_receipt": equity_input_receipt,
        "equity_window_id": equity_window_id,
        "maximum_equity_input_age_seconds": (
            maximum_equity_input_age_seconds
        ),
        "loader_build_id": loader_build_id,
        "equity_context_policy": (
            advanced_options_equity_context_requirements_payload(
                equity_context_requirements
            )
        ),
        "expected_input_sha256": expected_input_sha256,
        "provider_calls_authorized": False,
        "runtime_actions_authorized": False,
    }


def _validate_plan(plan: AdvancedOptionsNativeInputPlan) -> dict[str, object]:
    if not isinstance(plan, AdvancedOptionsNativeInputPlan):
        raise TypeError("Advanced Options native input plan is invalid")
    advanced_options_enrollment_plan_payload(plan.enrollment)
    control = plan.control_plane
    cutoff = _utc(plan.cutoff, "cutoff")
    advanced_options_equity_context_requirements_payload(
        plan.equity_context_requirements
    )
    binding = plan.rolling_state.producer_binding
    if (
        plan.producer_plan.control_plane_plan_id != control.plan_id
        or plan.producer_plan.clocks.market_cutoff != cutoff
        or plan.reference_state.session_date != control.session_date
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
        raise ValueError("native input generation scope changed")
    if (
        plan.source_flow.stream_session_ids
        != control.source_flow.stream_session_ids
        or plan.source_flow.journal_generation
        != control.source_flow.journal_generation
        or plan.source_flow.condition_semantics_sha256
        != control.pins.condition_semantics_sha256
    ):
        raise ValueError("native input source-flow scope changed")
    if (
        not isinstance(plan.equity_input_receipt_id, UUID)
        or not isinstance(plan.equity_window_id, UUID)
        or plan.equity_input_receipt.receipt_kind != "INPUT_READY"
        or plan.equity_input_receipt.cutoff.date() != cutoff.date()
        or plan.equity_input_receipt.cutoff > cutoff
        or plan.equity_input_receipt.advance_release
        or type(plan.maximum_equity_input_age_seconds) is not int
        or plan.maximum_equity_input_age_seconds <= 0
        or (
            cutoff - plan.equity_input_receipt.cutoff
        ).total_seconds() > plan.maximum_equity_input_age_seconds
        or not plan.loader_build_id.strip()
        or not isinstance(plan.history_source_cohort_id, UUID)
        or plan.history_source_cohort_id.int == 0
        or plan.history_source_cohort_id == control.cohort_id
        or plan.history_source_cohort_id
        == plan.enrollment.equity_plan.cohort_id
        or plan.provider_calls_authorized
        or plan.runtime_actions_authorized
    ):
        raise ValueError("native input receipt, build or authority changed")
    expected = (
        None
        if plan.expected_input_sha256 is None
        else _sha256(plan.expected_input_sha256, "expected_input_sha256")
    )
    identity = _plan_identity(
        enrollment=plan.enrollment,
        producer_plan=plan.producer_plan,
        reference_state=plan.reference_state,
        rolling_state=plan.rolling_state,
        source_flow=plan.source_flow,
        cutoff=cutoff,
        equity_input_receipt_id=plan.equity_input_receipt_id,
        equity_input_receipt=plan.equity_input_receipt,
        equity_window_id=plan.equity_window_id,
        maximum_equity_input_age_seconds=(
            plan.maximum_equity_input_age_seconds
        ),
        loader_build_id=plan.loader_build_id.strip(),
        history_source_cohort_id=plan.history_source_cohort_id,
        equity_context_requirements=plan.equity_context_requirements,
        expected_input_sha256=expected,
    )
    digest = canonical_sha256(identity)
    identifier = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-native-input-plan:{digest}",
    )
    if plan.plan_sha256 != digest or plan.plan_id != identifier:
        raise ValueError("native input plan identity changed")
    return identity


def build_advanced_options_native_input_plan(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    producer_plan: AdvancedOptionsProducerPlan,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
    source_flow: OptionSourceFlowIdentity,
    cutoff: datetime,
    equity_input_receipt_id: UUID,
    equity_input_receipt: CycleReceiptSpec,
    equity_window_id: UUID,
    loader_build_id: str,
    history_source_cohort_id: UUID,
    maximum_equity_input_age_seconds: int,
    equity_context_requirements: tuple[
        AdvancedOptionsEquityContextRequirement,
        ...,
    ],
    expected_input_sha256: str | None = None,
) -> AdvancedOptionsNativeInputPlan:
    normalized_cutoff = _utc(cutoff, "cutoff")
    identity = _plan_identity(
        enrollment=enrollment,
        producer_plan=producer_plan,
        reference_state=reference_state,
        rolling_state=rolling_state,
        source_flow=source_flow,
        cutoff=normalized_cutoff,
        equity_input_receipt_id=equity_input_receipt_id,
        equity_input_receipt=equity_input_receipt,
        equity_window_id=equity_window_id,
        maximum_equity_input_age_seconds=(
            maximum_equity_input_age_seconds
        ),
        loader_build_id=loader_build_id.strip(),
        history_source_cohort_id=history_source_cohort_id,
        equity_context_requirements=equity_context_requirements,
        expected_input_sha256=expected_input_sha256,
    )
    digest = canonical_sha256(identity)
    plan = AdvancedOptionsNativeInputPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-native-input-plan:{digest}",
        ),
        plan_sha256=digest,
        enrollment=enrollment,
        producer_plan=producer_plan,
        reference_state=reference_state,
        rolling_state=rolling_state,
        source_flow=source_flow,
        cutoff=normalized_cutoff,
        equity_input_receipt_id=equity_input_receipt_id,
        equity_input_receipt=equity_input_receipt,
        equity_window_id=equity_window_id,
        maximum_equity_input_age_seconds=(
            maximum_equity_input_age_seconds
        ),
        loader_build_id=loader_build_id.strip(),
        history_source_cohort_id=history_source_cohort_id,
        equity_context_requirements=equity_context_requirements,
        expected_input_sha256=expected_input_sha256,
    )
    _validate_plan(plan)
    return plan


def advanced_options_native_input_plan_payload(
    plan: AdvancedOptionsNativeInputPlan,
) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "plan_id": str(plan.plan_id),
        "plan_sha256": plan.plan_sha256,
        "identity": _validate_plan(plan),
        "effect_counts": plan.zero_effect_counts,
    }


def freeze_advanced_options_native_input_plan(
    plan: AdvancedOptionsNativeInputPlan,
    preflight: AdvancedOptionsNativeInputPreflight,
) -> AdvancedOptionsNativeInputPlan:
    _validate_plan(plan)
    if plan.frozen or preflight.plan_id != plan.plan_id:
        raise ValueError("native input preflight does not match its PLAN")
    return build_advanced_options_native_input_plan(
        enrollment=plan.enrollment,
        producer_plan=plan.producer_plan,
        reference_state=plan.reference_state,
        rolling_state=plan.rolling_state,
        source_flow=plan.source_flow,
        cutoff=plan.cutoff,
        equity_input_receipt_id=plan.equity_input_receipt_id,
        equity_input_receipt=plan.equity_input_receipt,
        equity_window_id=plan.equity_window_id,
        maximum_equity_input_age_seconds=(
            plan.maximum_equity_input_age_seconds
        ),
        loader_build_id=plan.loader_build_id,
        history_source_cohort_id=plan.history_source_cohort_id,
        equity_context_requirements=plan.equity_context_requirements,
        expected_input_sha256=preflight.input_sha256,
    )


def _evidence(
    kind: str,
    key: str,
    value: object,
    *,
    reasons: tuple[str, ...] = (),
) -> AdvancedOptionsNativeInputEvidence:
    return AdvancedOptionsNativeInputEvidence(
        member_kind=kind,
        member_key=key,
        available=not reasons,
        evidence_sha256=canonical_sha256(value),
        reason_codes=reasons,
    )


def _verify_scope(
    cursor,
    plan: AdvancedOptionsNativeInputPlan,
    evidence: list[AdvancedOptionsNativeInputEvidence],
) -> bool:
    control = plan.control_plane
    cursor.execute(
        "SELECT cohort.cohort_sha256,cohort.manifest_id::text,"
        "cohort.configuration_sha256,manifest.manifest_sha256,"
        "manifest.profile_id::text,profile.profile_sha256,"
        "current.version,version.member_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "JOIN rt.slot_set_current AS current "
        "ON current.cohort_id=cohort.cohort_id "
        "JOIN rt.slot_set_versions AS version "
        "ON version.cohort_id=current.cohort_id "
        "AND version.version=current.version "
        "WHERE cohort.cohort_id=%s",
        (str(control.cohort_id),),
    )
    row = cursor.fetchone()
    expected = (
        control.cohort_sha256,
        str(control.manifest_id),
        (
            plan.enrollment.enrollment_sha256
            if control.stored_identity is None
            else control.stored_identity.configuration_sha256
        ),
        control.manifest_sha256,
        str(control.profile_id),
        control.profile_sha256,
        control.slot_set.version,
        control.slot_set.member_sha256,
    )
    if row is None or tuple(row.values()) != expected:
        raise ValueError("stored native input control scope changed")
    cursor.execute(
        "SELECT member.instrument_id,member.channel,"
        "member.catalog_version_id,contract.contract_ticker "
        "FROM rt.slot_set_current AS current "
        "JOIN rt.slot_set_members AS member "
        "ON member.cohort_id=current.cohort_id "
        "AND member.version=current.version "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=instrument.contract_id "
        "WHERE current.cohort_id=%s "
        "ORDER BY contract.contract_ticker,member.channel",
        (str(control.cohort_id),),
    )
    slots = tuple(cursor.fetchall())
    selected = plan.enrollment.activity_selection.contracts
    expected_slots = tuple(sorted(
        (
            (
                item.instrument_id,
                channel,
                item.catalog_version_id,
                item.contract_ticker,
            )
            for item in selected
            for channel in ("Q", "T")
        ),
        key=lambda row: (row[3], row[1]),
    ))
    actual_slots = tuple(
        (
            int(row["instrument_id"]),
            str(row["channel"]),
            int(row["catalog_version_id"]),
            str(row["contract_ticker"]),
        )
        for row in slots
    )
    if actual_slots != expected_slots:
        raise ValueError("stored native input slot membership changed")
    cursor.execute(
        "SELECT state_sha256 FROM rt.advanced_options_reference_states "
        "WHERE state_id=%s AND reference_generation=%s",
        (
            str(plan.reference_state.state_id),
            plan.reference_state.reference_generation,
        ),
    )
    reference = cursor.fetchone()
    cursor.execute(
        "SELECT state_sha256 FROM rt.advanced_options_rolling_checkpoints "
        "WHERE state_id=%s AND owner_epoch=%s AND committed_offset=%s",
        (
            str(plan.rolling_state.state_id),
            plan.rolling_state.owner_epoch,
            plan.rolling_state.committed_offset,
        ),
    )
    rolling = cursor.fetchone()
    if (
        reference is None
        or str(reference["state_sha256"])
        != plan.reference_state.state_sha256
        or rolling is None
        or str(rolling["state_sha256"]) != plan.rolling_state.state_sha256
    ):
        raise ValueError("stored native input reference or rolling state changed")
    cursor.execute(
        "SELECT source_flow_sha256,journal_generation::text,"
        "stream_session_ids::text[] FROM rt.option_source_flows "
        "WHERE source_flow_id=%s AND source_cohort_id=%s",
        (
            str(plan.source_flow.source_flow_id),
            str(control.cohort_id),
        ),
    )
    source = cursor.fetchone()
    if (
        source is None
        or str(source["source_flow_sha256"])
        != plan.source_flow.source_flow_sha256
        or str(source["journal_generation"])
        != str(plan.source_flow.journal_generation)
        or tuple(UUID(str(value)) for value in source["stream_session_ids"])
        != plan.source_flow.stream_session_ids
    ):
        raise ValueError("stored native input source-flow identity changed")
    cursor.execute(
        "SELECT window_id::text,cohort_id::text,receipt_kind,cutoff,"
        "reference_generation,universe_sha256,input_sha256,build_id "
        "FROM rt.cycle_receipts WHERE receipt_id=%s",
        (str(plan.equity_input_receipt_id),),
    )
    receipt = cursor.fetchone()
    expected_receipt = (
        str(plan.equity_window_id),
        str(plan.enrollment.equity_plan.cohort_id),
        plan.equity_input_receipt.receipt_kind,
        plan.equity_input_receipt.cutoff,
        plan.equity_input_receipt.reference_generation,
        plan.equity_input_receipt.universe_sha256,
        plan.equity_input_receipt.input_sha256,
        plan.equity_input_receipt.build_id,
    )
    if receipt is None or tuple(receipt.values()) != expected_receipt:
        raise ValueError("stored native input Equity receipt changed")
    evidence.append(_evidence(
        "SCOPE",
        str(control.cohort_id),
        (expected, expected_slots, expected_receipt),
    ))
    return True


def _packages_for_events(
    cursor,
    *,
    cohort_id: UUID,
    event_ids: tuple[UUID, ...],
) -> dict[UUID, RealtimeOptionPackage]:
    if not event_ids:
        return {}
    cursor.execute(
        "SELECT event_id::text,candidate_id::text FROM rt.option_packages "
        "WHERE cohort_id=%s AND event_id=ANY(%s::uuid[]) "
        "ORDER BY event_id,candidate_id",
        (
            str(cohort_id),
            [str(value) for value in event_ids],
        ),
    )
    rows = tuple(cursor.fetchall())
    grouped: dict[UUID, list[UUID]] = {}
    for row in rows:
        grouped.setdefault(UUID(str(row["event_id"])), []).append(
            UUID(str(row["candidate_id"]))
        )
    if any(len(values) != 1 for values in grouped.values()):
        raise ValueError("native input event package mapping is ambiguous")
    packages = load_realtime_option_packages_cursor(
        cursor,
        tuple(values[0] for values in grouped.values()),
        cohort_id=cohort_id,
    )
    by_candidate = {row.candidate_id: row for row in packages}
    return {
        event_id: by_candidate[candidates[0]]
        for event_id, candidates in grouped.items()
    }


def _load_o1(
    cursor,
    plan: AdvancedOptionsNativeInputPlan,
    evidence: list[AdvancedOptionsNativeInputEvidence],
) -> tuple[AdvancedOptionsLaneInputs, object]:
    cursor.execute(
        "SELECT event.event_id::text,event.event_time,event.contract_id,"
        "event.contract_ticker,event.underlyer,event.cumulative_volume_after,"
        "event.prior_open_interest,event.open_interest_as_of,"
        "event.source_flow_id::text,event.source_offset,event.created_at,"
        "contract.underlying_security_id::text,version.call_put,"
        "version.expiration,event.flow_policy_sha256,"
        "array_agg(profile.profile ORDER BY profile.profile) AS profiles "
        "FROM rt.option_flow_events AS event "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=event.contract_id "
        "JOIN rt.slot_set_current AS current "
        "ON current.cohort_id=event.cohort_id "
        "JOIN rt.slot_set_members AS member "
        "ON member.cohort_id=current.cohort_id "
        "AND member.version=current.version "
        "AND member.channel='T' "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "AND instrument.contract_id=event.contract_id "
        "JOIN ref.option_contract_versions AS version "
        "ON version.version_id=member.catalog_version_id "
        "AND version.contract_id=event.contract_id "
        "JOIN rt.option_flow_event_profiles AS profile "
        "ON profile.event_id=event.event_id "
        "AND profile.event_time=event.event_time "
        "WHERE event.cohort_id=%s AND event.session_date=%s "
        "AND event.source_flow_id=%s AND event.source_offset<=%s "
        "AND event.event_time<=%s AND event.trade_treatment='INCLUDED' "
        "GROUP BY event.event_id,event.event_time,event.contract_id,"
        "event.contract_ticker,event.underlyer,event.cumulative_volume_after,"
        "event.prior_open_interest,event.open_interest_as_of,"
        "event.source_flow_id,event.source_offset,event.created_at,"
        "contract.underlying_security_id,version.call_put,"
        "version.expiration,event.flow_policy_sha256 "
        "ORDER BY event.event_time,event.event_id",
        (
            str(plan.control_plane.cohort_id),
            plan.control_plane.session_date,
            str(plan.source_flow.source_flow_id),
            plan.rolling_state.committed_offset,
            plan.cutoff,
        ),
    )
    rows = tuple(cursor.fetchall())
    packages = _packages_for_events(
        cursor,
        cohort_id=plan.control_plane.cohort_id,
        event_ids=tuple(UUID(str(row["event_id"])) for row in rows),
    )
    calendar = OptionExchangeCalendar()
    invocations = []
    for row in rows:
        event_id = UUID(str(row["event_id"]))
        observed_at = max(row["created_at"], row["event_time"])
        source = ActivitySource(
            security_id=UUID(str(row["underlying_security_id"])),
            underlyer=str(row["underlyer"]),
            contract_id=int(row["contract_id"]),
            contract_ticker=str(row["contract_ticker"]),
            contract_type=str(row["call_put"]),
            expiration_date=row["expiration"],
            expiration_cutoff=calendar.session_close(row["expiration"]),
            snapshot_id=event_id,
            snapshot_payload_sha256=canonical_sha256(tuple(row.values())),
            batch_id=plan.source_flow.source_flow_id,
            matrix_id=plan.rolling_state.state_id,
            configuration_sha256=plan.source_flow.source_flow_sha256,
            market_policy_sha256=canonical_sha256(tuple(row["profiles"])),
            volume_session=plan.control_plane.session_date,
            day_volume=row["cumulative_volume_after"],
            open_interest=row["prior_open_interest"],
            oi_settlement_session=row["open_interest_as_of"],
            oi_observed_at=(
                None
                if row["prior_open_interest"] is None
                else plan.reference_state.known_at
            ),
            market_time=row["event_time"],
            observed_at=observed_at,
            recorded_at=observed_at,
            received_at=observed_at,
        )
        invocations.append(AdvancedO1Invocation(
            event_id,
            source,
            packages.get(event_id),
        ))
    value = tuple(tuple(row.values()) for row in rows)
    evidence.append(_evidence("O1", "RETAINED_FLOW", value))
    identity = tuple(
        (
            str(row.event_id),
            row.source.model_dump(mode="json"),
            (
                None
                if row.package is None
                else str(row.package.candidate_id)
            ),
        )
        for row in invocations
    )
    return AdvancedOptionsLaneInputs(tuple(invocations)), identity


def _load_o3(
    cursor,
    plan: AdvancedOptionsNativeInputPlan,
    evidence: list[AdvancedOptionsNativeInputEvidence],
) -> tuple[AdvancedOptionsLaneInputs, object, bool]:
    instrument_ids = tuple(sorted({
        member.instrument_id for member in plan.control_plane.slot_set.members
    }))
    cursor.execute(
        "SELECT evidence_id::text,instrument_id,contract_id,"
        "catalog_version_id,journal_generation::text,stream_session_id::text,"
        "source_offset,market_time,observed_time,mark,payload_sha256 "
        "FROM rt.option_mark_evidence WHERE provider_tier='MASSIVE_ADVANCED' "
        "AND source_kind='ADVANCED_NBBO' "
        "AND instrument_id=ANY(%s) AND market_time<=%s "
        "ORDER BY instrument_id,market_time DESC,evidence_id",
        (list(instrument_ids), plan.cutoff),
    )
    mark_rows = tuple(cursor.fetchall())
    latest_marks = {}
    for row in mark_rows:
        latest_marks.setdefault(int(row["instrument_id"]), row)
    if set(latest_marks) != set(instrument_ids):
        reasons = ("CANONICAL_MARKS_UNAVAILABLE",)
        evidence.append(_evidence(
            "O3",
            "CANONICAL_MARKS",
            tuple(tuple(row.values()) for row in mark_rows),
        ))
        return AdvancedOptionsLaneInputs(
            blocked_reason_codes=reasons,
        ), {"blocked_reason_codes": reasons}, True
    if any(
        str(row["journal_generation"])
        != str(plan.source_flow.journal_generation)
        or UUID(str(row["stream_session_id"]))
        not in plan.source_flow.stream_session_ids
        or int(row["source_offset"]) > plan.rolling_state.committed_offset
        for row in latest_marks.values()
    ):
        raise ValueError("native O3 mark source generation changed")
    cursor.execute(
        "SELECT event.event_id::text,input.retained_input "
        "FROM rt.detector_events AS event "
        "JOIN rt.detector_versions AS version "
        "ON version.detector_sha256=event.detector_sha256 "
        "JOIN rt.detector_event_inputs AS input "
        "ON input.event_id=event.event_id "
        "AND input.input_role='O3_NATIVE_INVOCATION' "
        "WHERE event.cohort_id=%s "
        "AND version.detector_id='ADVANCED_OPTIONS_O3_INPUT' "
        "AND event.trigger_at=%s AND event.disposition='CONFIRMED' "
        "ORDER BY event.event_id",
        (str(plan.control_plane.cohort_id), plan.cutoff),
    )
    rows = tuple(cursor.fetchall())
    if not rows:
        reasons = ("O3_ACCEPTED_INPUT_EVIDENCE_ABSENT",)
        evidence.append(_evidence(
            "O3",
            "ACCEPTED_INPUTS",
            (),
            reasons=reasons,
        ))
        return AdvancedOptionsLaneInputs(
            blocked_reason_codes=reasons,
        ), {"blocked_reason_codes": reasons}, False
    candidate_ids = tuple(
        UUID(str(row["retained_input"]["package_candidate_id"]))
        for row in rows
    )
    packages = load_realtime_option_packages_cursor(
        cursor,
        candidate_ids,
        cohort_id=plan.control_plane.cohort_id,
    )
    by_candidate = {row.candidate_id: row for row in packages}
    invocations = tuple(
        decode_advanced_o3_invocation_document(
            row["retained_input"],
            package=by_candidate[
                UUID(str(row["retained_input"]["package_candidate_id"]))
            ],
        )
        for row in rows
    )
    if any(
        row.scheduled_cycle != plan.cutoff
        or not 7 <= (
            row.candidate.expiration_date
            - row.candidate.market_data_time.date()
        ).days <= 30
        or any(
            snapshot.open_interest is None
            or snapshot.day_volume is None
            or (
                snapshot.mark_source.value
                if hasattr(snapshot.mark_source, "value")
                else str(snapshot.mark_source)
            ) != "ADVANCED_NBBO_MIDPOINT"
            for snapshot in row.snapshots
        )
        for row in invocations
    ):
        raise ValueError("native O3 accepted input scope changed")
    evidence.append(_evidence(
        "O3",
        "ACCEPTED_INPUTS",
        tuple(tuple(row.values()) for row in rows),
    ))
    identity = tuple(
        advanced_o3_invocation_document(row) for row in invocations
    )
    return AdvancedOptionsLaneInputs(invocations), identity, False


def _load_equity_context(
    cursor,
    plan: AdvancedOptionsNativeInputPlan,
    evidence: list[AdvancedOptionsNativeInputEvidence],
) -> tuple[
    dict[DetectorFamily, tuple[str, ...]],
    object,
    tuple,
]:
    contexts = load_advanced_options_equity_context_cursor(
        cursor,
        equity_plan=plan.enrollment.equity_plan,
        history_source_cohort_id=plan.history_source_cohort_id,
        equity_cutoff=plan.equity_input_receipt.cutoff,
        requirements=plan.equity_context_requirements,
        underlyings=plan.control_plane.universe.option_underlyings,
        allow_shared_history_source=plan.control_plane.stored_identity is not None,
    )
    reasons_by_lane = {}
    identities = []
    for context in contexts:
        reasons_by_lane[context.lane] = context.reason_codes
        identity = {
            "lane": context.lane.value,
            "policy_version": context.policy_version,
            "requirements_sha256": context.requirements_sha256,
            "membership_sha256": context.membership_sha256,
            "members": tuple(
                (
                    row.ticker,
                    str(row.source_cohort_id),
                    str(row.bar_revision_id),
                    row.bar_end,
                    row.observed_at,
                    row.payload_sha256,
                )
                for row in context.members
            ),
        }
        identities.append(identity)
        evidence.append(_evidence(
            "EQUITY_CONTEXT",
            context.lane.value,
            identity,
            reasons=context.reason_codes,
        ))
    return reasons_by_lane, tuple(identities), contexts


def _equity_bar(row) -> EquityBarRevision:
    return EquityBarRevision(
        bar_revision_id=UUID(str(row["bar_revision_id"])),
        security_id=UUID(str(row["security_id"])),
        ticker=str(row["ticker"]),
        interval=str(row["interval"]),
        session_date=row["session_date"],
        bar_start=row["bar_start"],
        bar_end=row["bar_end"],
        open_price=Decimal(str(row["open_price"])),
        high_price=Decimal(str(row["high_price"])),
        low_price=Decimal(str(row["low_price"])),
        close_price=Decimal(str(row["close_price"])),
        volume=Decimal(str(row["volume"])),
        vwap=(
            None
            if row.get("vwap") is None
            else Decimal(str(row["vwap"]))
        ),
        transaction_count=row.get("transaction_count"),
        source_kind=BarSourceKind(str(row["source_kind"])),
        availability_mode=BarAvailabilityMode(
            str(row["availability_mode"])
        ),
        is_final=bool(row["is_final"]),
        system_observed_at=row["observed_at"],
        replay_available_at=row.get("replay_available_at"),
        adjusted=str(row["adjustment_basis"]) != "RAW_UNADJUSTED",
        payload_sha256=str(row["payload_sha256"]),
        quality_codes=tuple(row.get("quality_codes") or ()),
        provider_published_at=row.get("provider_published_at"),
        ingestion_segment_id=(
            None
            if row.get("ingestion_segment_id") is None
            else UUID(str(row["ingestion_segment_id"]))
        ),
        source_bar_revision_ids=tuple(
            UUID(str(value))
            for value in row.get("source_revision_ids") or ()
        ),
        supersedes_bar_revision_id=(
            None
            if row.get("supersedes_bar_revision_id") is None
            else UUID(str(row["supersedes_bar_revision_id"]))
        ),
        reconciliation_status=row.get("reconciliation_status"),
        session_scope=BarSessionScope(str(row["session_scope"])),
    )


def _load_os(
    cursor,
    plan: AdvancedOptionsNativeInputPlan,
    *,
    detector: DetectorFamily,
    contexts,
    o1_inputs: AdvancedOptionsLaneInputs,
    evidence: list[AdvancedOptionsNativeInputEvidence],
) -> tuple[AdvancedOptionsLaneInputs, object]:
    if detector not in {DetectorFamily.OS1, DetectorFamily.OS2}:
        raise ValueError("Advanced stock-first Options lane is invalid")
    if o1_inputs.blocked_reason_codes:
        reasons = tuple(o1_inputs.blocked_reason_codes)
        return (
            AdvancedOptionsLaneInputs(blocked_reason_codes=reasons),
            {"blocked_reason_codes": reasons},
        )
    context = next(row for row in contexts if row.lane is detector)
    if context.reason_codes:
        reasons = tuple(context.reason_codes)
        return (
            AdvancedOptionsLaneInputs(blocked_reason_codes=reasons),
            {"blocked_reason_codes": reasons},
        )
    member_ids = tuple(row.bar_revision_id for row in context.members)
    cursor.execute(
        "SELECT * FROM rt.bar_revisions "
        "WHERE bar_revision_id=ANY(%s::uuid[]) "
        "AND observed_at<=%s AND created_at<=%s "
        "ORDER BY ticker,interval,bar_end,bar_revision_id",
        (
            [str(value) for value in member_ids],
            plan.equity_input_receipt.cutoff,
            plan.equity_input_receipt.cutoff,
        ),
    )
    rows = tuple(cursor.fetchall())
    if (
        len(rows) != len(member_ids)
        or {UUID(str(row["bar_revision_id"])) for row in rows}
        != set(member_ids)
    ):
        reasons = (f"{detector.value}_BAR_MEMBERSHIP_UNAVAILABLE",)
        return (
            AdvancedOptionsLaneInputs(blocked_reason_codes=reasons),
            {"blocked_reason_codes": reasons},
        )
    by_ticker = {}
    for row in rows:
        by_ticker.setdefault(str(row["ticker"]), []).append(
            (_equity_bar(row), row["created_at"])
        )
    security_ids = dict(plan.enrollment.equity_plan.security_ids)
    calendar = OptionExchangeCalendar()
    signals = []
    for ticker in plan.control_plane.universe.option_underlyings:
        signals.extend(detect_canonical_stock_signals(
            security_id=security_ids[ticker],
            underlyer=ticker,
            bars=tuple(by_ticker.get(ticker, ())),
            market_cutoff=plan.equity_input_receipt.cutoff,
            decision_at=plan.cutoff,
            session_close=calendar.session_close(
                plan.control_plane.session_date
            ),
            detector_ids=("OS1", "OS2"),
        ))
    invocations = []
    for signal in signals:
        if signal.detector_id != detector.value:
            continue
        matching = tuple(
            row
            for row in o1_inputs.invocations
            if row.source.underlyer == signal.underlyer
            and row.source.contract_type
            == ("CALL" if signal.direction == 1 else "PUT")
        )
        for source in matching:
            event_id = uuid5(
                NAMESPACE_URL,
                "advanced-options-stock-first:"
                f"{detector.value}:{signal.signal_id}:{source.event_id}",
            )
            invocations.append(AdvancedOSInvocation(
                event_id,
                signal,
                source.source,
                source.package,
            ))
    identity = tuple(
        (
            str(row.event_id),
            str(row.signal.signal_id),
            str(row.source.snapshot_id) if row.source is not None else None,
            (
                str(row.package.candidate_id)
                if row.package is not None
                else None
            ),
        )
        for row in invocations
    )
    evidence.append(_evidence(
        detector.value,
        "CANONICAL_STOCK_FIRST",
        (
            context.membership_sha256,
            identity,
        ),
    ))
    return AdvancedOptionsLaneInputs(tuple(invocations)), identity


def assemble_advanced_options_native_inputs_cursor(
    cursor,
    plan: AdvancedOptionsNativeInputPlan,
) -> AdvancedOptionsNativeInputPreflight:
    _validate_plan(plan)
    evidence: list[AdvancedOptionsNativeInputEvidence] = []
    _verify_scope(cursor, plan, evidence)
    equity_context, equity_context_identity, equity_contexts = _load_equity_context(
        cursor,
        plan,
        evidence,
    )
    o1, o1_identity = _load_o1(cursor, plan, evidence)
    os1, os1_identity = _load_os(
        cursor,
        plan,
        detector=DetectorFamily.OS1,
        contexts=equity_contexts,
        o1_inputs=o1,
        evidence=evidence,
    )
    os2, os2_identity = _load_os(
        cursor,
        plan,
        detector=DetectorFamily.OS2,
        contexts=equity_contexts,
        o1_inputs=o1,
        evidence=evidence,
    )
    o3, o3_identity, o3_accepted_unavailable = _load_o3(
        cursor,
        plan,
        evidence,
    )
    o3_context_reasons = equity_context.get(DetectorFamily.O3, ())
    if o3_context_reasons:
        o3 = AdvancedOptionsLaneInputs(
            invocations=o3.invocations,
            blocked_reason_codes=tuple(sorted({
                *o3.blocked_reason_codes,
                *o3_context_reasons,
            })),
        )
    input_identity = {
        "contract": NATIVE_INPUT_PLAN_VERSION,
        "scope": {
            "enrollment_sha256": plan.enrollment.enrollment_sha256,
            "reference_state_sha256": plan.reference_state.state_sha256,
            "rolling_state_sha256": plan.rolling_state.state_sha256,
            "source_flow_sha256": plan.source_flow.source_flow_sha256,
            "cutoff": plan.cutoff,
            "equity_input_sha256": plan.equity_input_receipt.input_sha256,
            "equity_build_id": plan.equity_input_receipt.build_id,
            "loader_build_id": plan.loader_build_id,
        },
        "O1": o1_identity,
        "O3": o3_identity,
        "OS1": os1_identity,
        "OS2": os2_identity,
        "equity_context": equity_context_identity,
        "O2": {"state": "UNAVAILABLE", "reason": "O2_MODEL_NOT_ACCEPTED"},
    }
    input_sha256 = canonical_sha256(input_identity)
    membership = {
        detector.value: {
            "invocation_count": len(lane.invocations),
            "blocked_reason_codes": lane.blocked_reason_codes,
        }
        for detector, lane in (
            (DetectorFamily.O1, o1),
            (DetectorFamily.O3, o3),
            (DetectorFamily.OS1, os1),
            (DetectorFamily.OS2, os2),
        )
    }
    membership_sha256 = canonical_sha256(membership)
    bundle = AdvancedOptionsNativeInputBundle(
        o1=o1,
        o3=o3,
        os1=os1,
        os2=os2,
        plan_id=plan.plan_id,
        input_sha256=input_sha256,
        executor_membership_sha256=membership_sha256,
    )
    blocking = tuple(
        row
        for row in evidence
        if not row.available and row.member_kind != "EQUITY_CONTEXT"
    )
    if o3_accepted_unavailable:
        blocking = tuple(
            row for row in blocking if row.member_kind != "O3"
        )
    status = (
        NativeInputPlanStatus.READY
        if not blocking
        else NativeInputPlanStatus.BLOCKED
    )
    result_identity = {
        "plan_id": plan.plan_id,
        "input_sha256": input_sha256,
        "executor_membership_sha256": membership_sha256,
        "status": status,
        "evidence": tuple(evidence),
    }
    result = AdvancedOptionsNativeInputPreflight(
        status=status,
        plan_id=plan.plan_id,
        preflight_sha256=canonical_sha256(result_identity),
        input_sha256=input_sha256,
        executor_membership_sha256=membership_sha256,
        bundle=bundle,
        evidence=tuple(evidence),
    )
    if (
        plan.expected_input_sha256 is not None
        and result.status is NativeInputPlanStatus.READY
        and plan.expected_input_sha256 != input_sha256
    ):
        raise ValueError("native input replay diverged from frozen PLAN")
    return result


class PostgresAdvancedOptionsNativeInputLoader:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        plan: AdvancedOptionsNativeInputPlan,
        *,
        maximum_attempts: int = 3,
        retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("Advanced Options native input provider is invalid")
        _validate_plan(plan)
        if (
            type(maximum_attempts) is not int
            or maximum_attempts < 1
            or retry_initial_seconds < 0
            or not callable(wait)
        ):
            raise ValueError("Advanced Options native input retry is invalid")
        self._provider = provider
        self._plan = plan
        self._maximum_attempts = maximum_attempts
        self._retry_initial_seconds = retry_initial_seconds
        self._wait = wait

    def _assemble(self, unit) -> AdvancedOptionsNativeInputPreflight:
        with unit.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            return assemble_advanced_options_native_inputs_cursor(
                cursor,
                self._plan,
            )

    def preflight(self) -> AdvancedOptionsNativeInputPreflight:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return self._assemble(unit)

    def load(self) -> AdvancedOptionsNativeInputBundle:
        if not self._plan.frozen:
            raise ValueError(
                "Advanced Options native input PLAN is not frozen by preflight"
            )
        for attempt in range(1, self._maximum_attempts + 1):
            try:
                with self._provider.transaction(
                    APP_ROLE,
                    read_only=True,
                    isolation_level="SERIALIZABLE",
                ) as unit:
                    result = self._assemble(unit)
                    if result.status is NativeInputPlanStatus.BLOCKED:
                        raise AdvancedOptionsNativeInputBlocked(result)
                    return replace(
                        result.bundle,
                        plan_id=self._plan.plan_id,
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
        raise AssertionError("unreachable native input loader retry state")
