"""Compact P4A Advanced Options input closure and existing job/barrier binding."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_control_plane import (
    AdvancedOptionsControlPlanePlan,
    AdvancedOptionsEnrollmentPlan,
)
from .advanced_options_producers import AdvancedOptionsProducerPlan
from .advanced_options_reference_state import AdvancedOptionsReferenceState
from .advanced_options_rolling_state import AdvancedOptionsRollingState
from .contracts import canonical_json, canonical_sha256
from .materialization_jobs import (
    BarrierMember,
    JobBarrierVerdict,
    MaterializationJob,
    build_closure_materialization_job,
)
from .option_cycle_input import (
    MAX_OPTION_CYCLE_INPUT_BYTES,
    OptionCycleInput,
    decode_option_cycle_input,
    encode_option_cycle_input,
)
from .orchestration_repository import (
    OPTIONS_INPUT_READY,
    CycleReceiptSpec,
    build_options_receipt_spec,
)

if TYPE_CHECKING:
    from .advanced_options_mark_producer import (
        AdvancedOptionsMarkProducerPlan,
    )
    from .advanced_options_native_loader import AdvancedOptionsNativeInputPlan
    from .advanced_options_reference_producer import (
        AdvancedOptionsReferenceProducerPlan,
    )
    from .advanced_options_rolling_producer import (
        AdvancedOptionsRollingProducerPlan,
    )


ADVANCED_OPTIONS_INPUT_VERSION = "advanced_options_input_closure_v3"
ADVANCED_OPTIONS_INPUT_READY_PLAN_VERSION = (
    "advanced-options-input-ready-plan-v1"
)
MAX_ADVANCED_OPTIONS_INPUT_BYTES = MAX_OPTION_CYCLE_INPUT_BYTES
OPTIONS_INPUT_BARRIER_KIND = "OPTIONS_INPUT_READY"


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputClosure:
    cycle_input: OptionCycleInput
    control_plane_plan_id: UUID
    manifest_id: UUID
    manifest_sha256: str
    producer_plan_id: UUID
    producer_plan_sha256: str
    reference_state_id: UUID
    reference_state_sha256: str
    reference_generation: int
    reference_known_at: datetime
    reference_valid_until: datetime
    rolling_state_id: UUID
    rolling_state_sha256: str
    rolling_owner_epoch: int
    rolling_committed_offset: int
    rolling_committed_at: datetime
    source_flow_sha256: str
    condition_semantics_sha256: str
    flow_policy_sha256: str
    valuation_policy_sha256: str
    model_bundle_sha256: str
    catalog_sha256: str
    reference_policy_sha256: str
    oi_facts_sha256: str
    market_state_checkpoint_sha256: str
    flow_checkpoint_sha256: str
    unresolved_gaps_sha256: str
    market_state_heads_sha256: str
    source_clocks_sha256: str
    equity_input_receipt_id: UUID
    equity_input_sha256: str
    native_input_plan_sha256: str | None = None
    native_input_sha256: str | None = None
    executor_membership_sha256: str | None = None
    mark_producer_plan_sha256: str | None = None
    marks_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.cycle_input, OptionCycleInput):
            raise TypeError("Advanced Options closure cycle input is invalid")
        if (
            not isinstance(self.reference_state_id, UUID)
            or type(self.reference_generation) is not int
            or self.reference_generation <= 0
            or self.reference_known_at.tzinfo is None
            or self.reference_known_at.utcoffset() is None
            or self.reference_valid_until.tzinfo is None
            or self.reference_valid_until.utcoffset() is None
            or self.reference_known_at >= self.reference_valid_until
            or not isinstance(self.rolling_state_id, UUID)
            or type(self.rolling_owner_epoch) is not int
            or self.rolling_owner_epoch <= 0
            or type(self.rolling_committed_offset) is not int
            or self.rolling_committed_offset < 0
            or self.rolling_committed_at.tzinfo is None
            or self.rolling_committed_at.utcoffset() is None
        ):
            raise ValueError("Advanced Options reference state identity is invalid")
        for name in (
            "producer_plan_sha256",
            "manifest_sha256",
            "reference_state_sha256",
            "rolling_state_sha256",
            "source_flow_sha256",
            "condition_semantics_sha256",
            "flow_policy_sha256",
            "valuation_policy_sha256",
            "model_bundle_sha256",
            "catalog_sha256",
            "reference_policy_sha256",
            "oi_facts_sha256",
            "market_state_checkpoint_sha256",
            "flow_checkpoint_sha256",
            "unresolved_gaps_sha256",
            "market_state_heads_sha256",
            "source_clocks_sha256",
            "equity_input_sha256",
        ):
            object.__setattr__(
                self,
                name,
                _sha256(getattr(self, name), name),
            )
        producer_digests = (
            self.native_input_plan_sha256,
            self.native_input_sha256,
            self.executor_membership_sha256,
            self.mark_producer_plan_sha256,
            self.marks_sha256,
        )
        if any(value is None for value in producer_digests) and any(
            value is not None for value in producer_digests
        ):
            raise ValueError(
                "Advanced Options closure producer evidence is partial"
            )
        for name in (
            "native_input_plan_sha256",
            "native_input_sha256",
            "executor_membership_sha256",
            "mark_producer_plan_sha256",
            "marks_sha256",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _sha256(value, name))

    @property
    def sha256(self) -> str:
        return encode_advanced_options_input_closure(self)[1]

    @property
    def has_canonical_producer_evidence(self) -> bool:
        return self.native_input_plan_sha256 is not None


def build_advanced_options_input_closure(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    producer_plan: AdvancedOptionsProducerPlan,
    cycle_input: OptionCycleInput,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
    reference_policy_sha256: str,
    oi_facts_sha256: str,
    market_state_checkpoint_sha256: str,
    flow_checkpoint_sha256: str,
    unresolved_gaps_sha256: str,
    market_state_heads_sha256: str,
    equity_input_receipt_id: UUID,
    equity_input_sha256: str,
    native_input_plan_sha256: str | None = None,
    native_input_sha256: str | None = None,
    executor_membership_sha256: str | None = None,
    mark_producer_plan_sha256: str | None = None,
    marks_sha256: str | None = None,
) -> AdvancedOptionsInputClosure:
    if (
        not isinstance(control_plane, AdvancedOptionsControlPlanePlan)
        or not isinstance(producer_plan, AdvancedOptionsProducerPlan)
        or producer_plan.control_plane_plan_id != control_plane.plan_id
    ):
        raise ValueError("Advanced Options closure control scope is invalid")
    if not isinstance(reference_state, AdvancedOptionsReferenceState):
        raise TypeError("Advanced Options reference state is invalid")
    if not isinstance(rolling_state, AdvancedOptionsRollingState):
        raise TypeError("Advanced Options rolling state is invalid")
    if (
        cycle_input.provider_tier != control_plane.binding.options_tier
        or cycle_input.profile_id != control_plane.profile_id
        or cycle_input.profile_sha256 != control_plane.profile_sha256
        or cycle_input.universe_sha256 != control_plane.universe.sha256
        or cycle_input.cohort_id != control_plane.cohort_id
        or cycle_input.slot_set_version != control_plane.slot_set.version
        or cycle_input.slot_set_member_sha256
        != control_plane.slot_set.member_sha256
        or tuple(row.underlyer for row in cycle_input.underlyings)
        != control_plane.universe.option_underlyings
    ):
        raise ValueError("Advanced Options cycle input changed control scope")
    if (
        reference_state.session_date != cycle_input.scheduled_cycle.date()
        or reference_state.underlyings
        != tuple(row.underlyer for row in cycle_input.underlyings)
        or reference_state.reference_policy_sha256
        != _sha256(reference_policy_sha256, "reference_policy_sha256")
        or reference_state.open_interest_sha256
        != _sha256(oi_facts_sha256, "oi_facts_sha256")
        or not (
            reference_state.known_at
            <= producer_plan.clocks.known_at
            < reference_state.valid_until
        )
    ):
        raise ValueError("Advanced Options Tier A reference state changed")
    # B9c/B16 (Nov 6): name the condition that failed instead of one opaque message.
    tier_b_failures = [
        name for name, failed in (
            ("cohort", rolling_state.cohort_id != control_plane.cohort_id),
            ("session", rolling_state.session_date != cycle_input.scheduled_cycle.date()),
            ("journal_generation", rolling_state.journal_generation
             != control_plane.source_flow.journal_generation),
            ("underlying_ticks:" + ",".join(row.ticker for row in rolling_state.underlying_ticks)
             + "|cycle:" + ",".join(row.underlyer for row in cycle_input.underlyings),
             # B16 (Nov 6): same 9 underlyings, different order (market state put NFLX last); compare as sets.
             sorted(row.ticker for row in rolling_state.underlying_ticks)
             != sorted(row.underlyer for row in cycle_input.underlyings)),
            ("market_state_checkpoint", rolling_state.market_state_checkpoint_sha256
             != _sha256(market_state_checkpoint_sha256, "market_state_checkpoint_sha256")),
            ("flow_checkpoint", rolling_state.flow_checkpoint_sha256
             != _sha256(flow_checkpoint_sha256, "flow_checkpoint_sha256")),
            (f"committed_at:{rolling_state.committed_at.isoformat()}>known_at:"
             f"{producer_plan.clocks.known_at.isoformat()}",
             rolling_state.committed_at > producer_plan.clocks.known_at),
        ) if failed
    ]
    if tier_b_failures:
        raise ValueError(
            "Advanced Options Tier B rolling state changed: " + "; ".join(tier_b_failures)
        )
    binding = rolling_state.producer_binding
    if binding is not None and (
        binding.reference_state_id != reference_state.state_id
        or binding.reference_state_sha256 != reference_state.state_sha256
        or binding.reference_generation != reference_state.reference_generation
        or binding.known_at > producer_plan.clocks.known_at
        or not (
            binding.known_at
            <= producer_plan.clocks.known_at
            < binding.valid_until
        )
        or binding.unresolved_gaps_sha256
        != _sha256(unresolved_gaps_sha256, "unresolved_gaps_sha256")
        or binding.market_state_heads_sha256
        != _sha256(market_state_heads_sha256, "market_state_heads_sha256")
    ):
        raise ValueError("Advanced Options rolling producer evidence changed")
    return AdvancedOptionsInputClosure(
        cycle_input=cycle_input,
        control_plane_plan_id=control_plane.plan_id,
        manifest_id=control_plane.manifest_id,
        manifest_sha256=control_plane.manifest_sha256,
        producer_plan_id=producer_plan.plan_id,
        producer_plan_sha256=producer_plan.plan_sha256,
        reference_state_id=reference_state.state_id,
        reference_state_sha256=reference_state.state_sha256,
        reference_generation=reference_state.reference_generation,
        reference_known_at=reference_state.known_at,
        reference_valid_until=reference_state.valid_until,
        rolling_state_id=rolling_state.state_id,
        rolling_state_sha256=rolling_state.state_sha256,
        rolling_owner_epoch=rolling_state.owner_epoch,
        rolling_committed_offset=rolling_state.committed_offset,
        rolling_committed_at=rolling_state.committed_at,
        source_flow_sha256=(
            control_plane.source_flow.source_flow_sha256
            if binding is None
            else binding.source_flow_sha256
        ),
        condition_semantics_sha256=(
            control_plane.pins.condition_semantics_sha256
        ),
        flow_policy_sha256=control_plane.pins.flow_policy_sha256,
        valuation_policy_sha256=(
            control_plane.pins.valuation_policy_sha256
        ),
        model_bundle_sha256=control_plane.pins.model_bundle_sha256,
        catalog_sha256=control_plane.pins.catalog_sha256,
        reference_policy_sha256=reference_policy_sha256,
        oi_facts_sha256=oi_facts_sha256,
        market_state_checkpoint_sha256=market_state_checkpoint_sha256,
        flow_checkpoint_sha256=flow_checkpoint_sha256,
        unresolved_gaps_sha256=unresolved_gaps_sha256,
        market_state_heads_sha256=market_state_heads_sha256,
        source_clocks_sha256=producer_plan.clocks.sha256,
        equity_input_receipt_id=equity_input_receipt_id,
        equity_input_sha256=equity_input_sha256,
        native_input_plan_sha256=native_input_plan_sha256,
        native_input_sha256=native_input_sha256,
        executor_membership_sha256=executor_membership_sha256,
        mark_producer_plan_sha256=mark_producer_plan_sha256,
        marks_sha256=marks_sha256,
    )


def _closure_payload(
    closure: AdvancedOptionsInputClosure,
) -> dict[str, object]:
    cycle_text, cycle_sha256 = encode_option_cycle_input(closure.cycle_input)
    return {
        "version": ADVANCED_OPTIONS_INPUT_VERSION,
        "cycle_input": json.loads(cycle_text),
        "cycle_input_sha256": cycle_sha256,
        "control_plane_plan_id": str(closure.control_plane_plan_id),
        "manifest_id": str(closure.manifest_id),
        "manifest_sha256": closure.manifest_sha256,
        "producer_plan_id": str(closure.producer_plan_id),
        "producer_plan_sha256": closure.producer_plan_sha256,
        "reference_state_id": str(closure.reference_state_id),
        "reference_state_sha256": closure.reference_state_sha256,
        "reference_generation": closure.reference_generation,
        "reference_known_at": closure.reference_known_at.isoformat(),
        "reference_valid_until": closure.reference_valid_until.isoformat(),
        "rolling_state_id": str(closure.rolling_state_id),
        "rolling_state_sha256": closure.rolling_state_sha256,
        "rolling_owner_epoch": closure.rolling_owner_epoch,
        "rolling_committed_offset": closure.rolling_committed_offset,
        "rolling_committed_at": closure.rolling_committed_at.isoformat(),
        "source_flow_sha256": closure.source_flow_sha256,
        "condition_semantics_sha256": closure.condition_semantics_sha256,
        "flow_policy_sha256": closure.flow_policy_sha256,
        "valuation_policy_sha256": closure.valuation_policy_sha256,
        "model_bundle_sha256": closure.model_bundle_sha256,
        "catalog_sha256": closure.catalog_sha256,
        "reference_policy_sha256": closure.reference_policy_sha256,
        "oi_facts_sha256": closure.oi_facts_sha256,
        "market_state_checkpoint_sha256": (
            closure.market_state_checkpoint_sha256
        ),
        "flow_checkpoint_sha256": closure.flow_checkpoint_sha256,
        "unresolved_gaps_sha256": closure.unresolved_gaps_sha256,
        "market_state_heads_sha256": closure.market_state_heads_sha256,
        "source_clocks_sha256": closure.source_clocks_sha256,
        "equity_input_receipt_id": str(closure.equity_input_receipt_id),
        "equity_input_sha256": closure.equity_input_sha256,
        "native_input_plan_sha256": closure.native_input_plan_sha256,
        "native_input_sha256": closure.native_input_sha256,
        "executor_membership_sha256": (
            closure.executor_membership_sha256
        ),
        "mark_producer_plan_sha256": (
            closure.mark_producer_plan_sha256
        ),
        "marks_sha256": closure.marks_sha256,
    }


def encode_advanced_options_input_closure(
    closure: AdvancedOptionsInputClosure,
) -> tuple[str, str]:
    if not isinstance(closure, AdvancedOptionsInputClosure):
        raise TypeError("Advanced Options input closure is invalid")
    text = canonical_json(_closure_payload(closure))
    encoded = text.encode("ascii")
    if len(encoded) > MAX_ADVANCED_OPTIONS_INPUT_BYTES:
        raise ValueError(
            "Advanced Options input closure exceeds the compact byte bound"
        )
    return text, hashlib.sha256(encoded).hexdigest()


def decode_advanced_options_input_closure(
    payload_text: str,
    payload_sha256: str,
) -> AdvancedOptionsInputClosure:
    if not isinstance(payload_text, str):
        raise TypeError("Advanced Options closure payload must be text")
    encoded = payload_text.encode("ascii")
    if len(encoded) > MAX_ADVANCED_OPTIONS_INPUT_BYTES:
        raise ValueError("Advanced Options input closure exceeds byte bound")
    if hashlib.sha256(encoded).hexdigest() != _sha256(
        payload_sha256,
        "payload_sha256",
    ):
        raise ValueError("Advanced Options closure digest mismatch")
    payload = json.loads(payload_text)
    if (
        not isinstance(payload, dict)
        or payload.get("version") != ADVANCED_OPTIONS_INPUT_VERSION
        or set(payload) != set(_closure_payload_keys())
    ):
        raise ValueError("Advanced Options closure fields are invalid")
    cycle_text = canonical_json(payload["cycle_input"])
    cycle_input = decode_option_cycle_input(
        cycle_text,
        payload["cycle_input_sha256"],
    )
    closure = AdvancedOptionsInputClosure(
        cycle_input=cycle_input,
        control_plane_plan_id=UUID(payload["control_plane_plan_id"]),
        manifest_id=UUID(payload["manifest_id"]),
        manifest_sha256=payload["manifest_sha256"],
        producer_plan_id=UUID(payload["producer_plan_id"]),
        producer_plan_sha256=payload["producer_plan_sha256"],
        reference_state_id=UUID(payload["reference_state_id"]),
        reference_state_sha256=payload["reference_state_sha256"],
        reference_generation=payload["reference_generation"],
        reference_known_at=datetime.fromisoformat(
            payload["reference_known_at"]
        ),
        reference_valid_until=datetime.fromisoformat(
            payload["reference_valid_until"]
        ),
        rolling_state_id=UUID(payload["rolling_state_id"]),
        rolling_state_sha256=payload["rolling_state_sha256"],
        rolling_owner_epoch=payload["rolling_owner_epoch"],
        rolling_committed_offset=payload["rolling_committed_offset"],
        rolling_committed_at=datetime.fromisoformat(
            payload["rolling_committed_at"]
        ),
        source_flow_sha256=payload["source_flow_sha256"],
        condition_semantics_sha256=payload["condition_semantics_sha256"],
        flow_policy_sha256=payload["flow_policy_sha256"],
        valuation_policy_sha256=payload["valuation_policy_sha256"],
        model_bundle_sha256=payload["model_bundle_sha256"],
        catalog_sha256=payload["catalog_sha256"],
        reference_policy_sha256=payload["reference_policy_sha256"],
        oi_facts_sha256=payload["oi_facts_sha256"],
        market_state_checkpoint_sha256=(
            payload["market_state_checkpoint_sha256"]
        ),
        flow_checkpoint_sha256=payload["flow_checkpoint_sha256"],
        unresolved_gaps_sha256=payload["unresolved_gaps_sha256"],
        market_state_heads_sha256=payload["market_state_heads_sha256"],
        source_clocks_sha256=payload["source_clocks_sha256"],
        equity_input_receipt_id=UUID(payload["equity_input_receipt_id"]),
        equity_input_sha256=payload["equity_input_sha256"],
        native_input_plan_sha256=payload["native_input_plan_sha256"],
        native_input_sha256=payload["native_input_sha256"],
        executor_membership_sha256=payload[
            "executor_membership_sha256"
        ],
        mark_producer_plan_sha256=payload[
            "mark_producer_plan_sha256"
        ],
        marks_sha256=payload["marks_sha256"],
    )
    if encode_advanced_options_input_closure(closure)[0] != payload_text:
        raise ValueError("Advanced Options closure is not canonical")
    return closure


def _closure_payload_keys() -> tuple[str, ...]:
    return (
        "version",
        "cycle_input",
        "cycle_input_sha256",
        "control_plane_plan_id",
        "manifest_id",
        "manifest_sha256",
        "producer_plan_id",
        "producer_plan_sha256",
        "reference_state_id",
        "reference_state_sha256",
        "reference_generation",
        "reference_known_at",
        "reference_valid_until",
        "rolling_state_id",
        "rolling_state_sha256",
        "rolling_owner_epoch",
        "rolling_committed_offset",
        "rolling_committed_at",
        "source_flow_sha256",
        "condition_semantics_sha256",
        "flow_policy_sha256",
        "valuation_policy_sha256",
        "model_bundle_sha256",
        "catalog_sha256",
        "reference_policy_sha256",
        "oi_facts_sha256",
        "market_state_checkpoint_sha256",
        "flow_checkpoint_sha256",
        "unresolved_gaps_sha256",
        "market_state_heads_sha256",
        "source_clocks_sha256",
        "equity_input_receipt_id",
        "equity_input_sha256",
        "native_input_plan_sha256",
        "native_input_sha256",
        "executor_membership_sha256",
        "mark_producer_plan_sha256",
        "marks_sha256",
    )


class AdvancedOptionsInputDependencyStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    COMPLETE_ZERO = "COMPLETE_ZERO"
    BLOCKED = "BLOCKED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputDependencyEvidence:
    member_kind: str
    member_key: str
    status: AdvancedOptionsInputDependencyStatus
    evidence_sha256: str
    reason_codes: tuple[str, ...] = ()
    accepted_unavailable: bool = False

    def __post_init__(self) -> None:
        kind = self.member_kind.strip().upper()
        key = self.member_key.strip()
        reasons = tuple(sorted({
            value.strip().upper()
            for value in self.reason_codes
            if value.strip()
        }))
        if not kind or not key:
            raise ValueError(
                "Advanced Options input dependency identity is invalid"
            )
        if self.status in {
            AdvancedOptionsInputDependencyStatus.AVAILABLE,
            AdvancedOptionsInputDependencyStatus.COMPLETE_ZERO,
        } and reasons:
            raise ValueError(
                "available Advanced Options input dependency has reasons"
            )
        if self.status in {
            AdvancedOptionsInputDependencyStatus.BLOCKED,
            AdvancedOptionsInputDependencyStatus.UNAVAILABLE,
        } and not reasons:
            raise ValueError(
                "non-ready Advanced Options input dependency lacks reasons"
            )
        if self.accepted_unavailable and (
            self.status is not AdvancedOptionsInputDependencyStatus.UNAVAILABLE
        ):
            raise ValueError(
                "only an unavailable input dependency can be accepted"
            )
        object.__setattr__(self, "member_kind", kind)
        object.__setattr__(self, "member_key", key)
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputReadyPlan:
    plan_id: UUID
    plan_sha256: str
    enrollment: AdvancedOptionsEnrollmentPlan
    producer_plan: AdvancedOptionsProducerPlan
    reference_producer_plan: AdvancedOptionsReferenceProducerPlan
    rolling_producer_plan: AdvancedOptionsRollingProducerPlan
    native_input_plan: AdvancedOptionsNativeInputPlan
    mark_producer_plan: AdvancedOptionsMarkProducerPlan
    native_executor_membership_sha256: str
    service_plan_id: UUID
    window_id: UUID
    cutoff: datetime
    producer_build_id: str
    producer_build_sha256: str
    expected_closure_sha256: str | None = None
    provider_calls_authorized: bool = False
    runtime_actions_authorized: bool = False

    @property
    def control_plane(self) -> AdvancedOptionsControlPlanePlan:
        return self.enrollment.control_plane

    @property
    def frozen(self) -> bool:
        return self.expected_closure_sha256 is not None

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


def _input_ready_plan_identity(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    producer_plan: AdvancedOptionsProducerPlan,
    reference_producer_plan: AdvancedOptionsReferenceProducerPlan,
    rolling_producer_plan: AdvancedOptionsRollingProducerPlan,
    native_input_plan: AdvancedOptionsNativeInputPlan,
    mark_producer_plan: AdvancedOptionsMarkProducerPlan,
    native_executor_membership_sha256: str,
    service_plan_id: UUID,
    window_id: UUID,
    cutoff: datetime,
    producer_build_id: str,
    producer_build_sha256: str,
    expected_closure_sha256: str | None,
) -> dict[str, object]:
    control = enrollment.control_plane
    return {
        "contract": ADVANCED_OPTIONS_INPUT_READY_PLAN_VERSION,
        "enrollment_id": enrollment.enrollment_id,
        "enrollment_sha256": enrollment.enrollment_sha256,
        "control_plane_plan_id": control.plan_id,
        "control_plane_plan_sha256": control.plan_sha256,
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
        "reference_producer_plan_id": reference_producer_plan.plan_id,
        "reference_producer_plan_sha256": (
            reference_producer_plan.plan_sha256
        ),
        "rolling_producer_plan_id": rolling_producer_plan.plan_id,
        "rolling_producer_plan_sha256": rolling_producer_plan.plan_sha256,
        "native_input_plan_id": native_input_plan.plan_id,
        "native_input_plan_sha256": native_input_plan.plan_sha256,
        "native_input_sha256": native_input_plan.expected_input_sha256,
        "native_executor_membership_sha256": (
            native_executor_membership_sha256
        ),
        "mark_producer_plan_id": mark_producer_plan.plan_id,
        "mark_producer_plan_sha256": mark_producer_plan.plan_sha256,
        "marks_sha256": mark_producer_plan.expected_marks_sha256,
        "reference_state_id": native_input_plan.reference_state.state_id,
        "reference_state_sha256": (
            native_input_plan.reference_state.state_sha256
        ),
        "reference_generation": (
            native_input_plan.reference_state.reference_generation
        ),
        "rolling_state_id": native_input_plan.rolling_state.state_id,
        "rolling_state_sha256": (
            native_input_plan.rolling_state.state_sha256
        ),
        "source_flow_id": native_input_plan.source_flow.source_flow_id,
        "source_flow_sha256": (
            native_input_plan.source_flow.source_flow_sha256
        ),
        "journal_generation": (
            native_input_plan.source_flow.journal_generation
        ),
        "equity_input_receipt_id": (
            native_input_plan.equity_input_receipt_id
        ),
        "equity_input_sha256": (
            native_input_plan.equity_input_receipt.input_sha256
        ),
        "equity_input_build_id": (
            native_input_plan.equity_input_receipt.build_id
        ),
        "equity_context_requirements": (
            native_input_plan.equity_context_requirements
        ),
        "service_plan_id": service_plan_id,
        "window_id": window_id,
        "cutoff": cutoff,
        "producer_build_id": producer_build_id,
        "producer_build_sha256": producer_build_sha256,
        "expected_closure_sha256": expected_closure_sha256,
        "provider_calls_authorized": False,
        "runtime_actions_authorized": False,
    }


def _validate_input_ready_plan(
    plan: AdvancedOptionsInputReadyPlan,
) -> dict[str, object]:
    if not isinstance(plan, AdvancedOptionsInputReadyPlan):
        raise TypeError("Advanced Options input-ready PLAN is invalid")
    control = plan.control_plane
    reference_state = plan.native_input_plan.reference_state
    rolling_state = plan.native_input_plan.rolling_state
    source_flow = plan.native_input_plan.source_flow
    cutoff = plan.cutoff
    if (
        not isinstance(plan.enrollment, AdvancedOptionsEnrollmentPlan)
        or plan.producer_plan.control_plane_plan_id != control.plan_id
        or plan.reference_producer_plan.enrollment != plan.enrollment
        or plan.rolling_producer_plan.enrollment != plan.enrollment
        or plan.native_input_plan.enrollment != plan.enrollment
        or plan.mark_producer_plan.enrollment != plan.enrollment
        or plan.native_input_plan.producer_plan != plan.producer_plan
        or plan.rolling_producer_plan.reference_state != reference_state
        or plan.mark_producer_plan.reference_state != reference_state
        or plan.mark_producer_plan.rolling_state != rolling_state
        or plan.rolling_producer_plan.source_flow != source_flow
        or plan.mark_producer_plan.source_flow != source_flow
        or plan.native_input_plan.cutoff != cutoff
        or plan.mark_producer_plan.cutoff != cutoff
        or plan.producer_plan.clocks.market_cutoff != cutoff
        or not plan.native_input_plan.frozen
        or not plan.mark_producer_plan.frozen
        or plan.provider_calls_authorized
        or plan.runtime_actions_authorized
    ):
        raise ValueError(
            "Advanced Options input-ready PLAN scope or authority changed"
        )
    if (
        not isinstance(plan.service_plan_id, UUID)
        or not isinstance(plan.window_id, UUID)
        or cutoff.tzinfo is None
        or cutoff.utcoffset() is None
        or not plan.producer_build_id.strip()
    ):
        raise ValueError(
            "Advanced Options input-ready window or build is invalid"
        )
    executor_sha256 = _sha256(
        plan.native_executor_membership_sha256,
        "native_executor_membership_sha256",
    )
    build_sha256 = _sha256(
        plan.producer_build_sha256,
        "producer_build_sha256",
    )
    expected = (
        None
        if plan.expected_closure_sha256 is None
        else _sha256(
            plan.expected_closure_sha256,
            "expected_closure_sha256",
        )
    )
    identity = _input_ready_plan_identity(
        enrollment=plan.enrollment,
        producer_plan=plan.producer_plan,
        reference_producer_plan=plan.reference_producer_plan,
        rolling_producer_plan=plan.rolling_producer_plan,
        native_input_plan=plan.native_input_plan,
        mark_producer_plan=plan.mark_producer_plan,
        native_executor_membership_sha256=executor_sha256,
        service_plan_id=plan.service_plan_id,
        window_id=plan.window_id,
        cutoff=cutoff,
        producer_build_id=plan.producer_build_id.strip(),
        producer_build_sha256=build_sha256,
        expected_closure_sha256=expected,
    )
    digest = canonical_sha256(identity)
    identifier = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-input-ready-plan:{digest}",
    )
    if plan.plan_sha256 != digest or plan.plan_id != identifier:
        raise ValueError(
            "Advanced Options input-ready PLAN identity changed"
        )
    return identity


def build_advanced_options_input_ready_plan(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    producer_plan: AdvancedOptionsProducerPlan,
    reference_producer_plan: AdvancedOptionsReferenceProducerPlan,
    rolling_producer_plan: AdvancedOptionsRollingProducerPlan,
    native_input_plan: AdvancedOptionsNativeInputPlan,
    mark_producer_plan: AdvancedOptionsMarkProducerPlan,
    native_executor_membership_sha256: str,
    service_plan_id: UUID,
    window_id: UUID,
    cutoff: datetime,
    producer_build_id: str,
    producer_build_sha256: str,
    expected_closure_sha256: str | None = None,
) -> AdvancedOptionsInputReadyPlan:
    identity = _input_ready_plan_identity(
        enrollment=enrollment,
        producer_plan=producer_plan,
        reference_producer_plan=reference_producer_plan,
        rolling_producer_plan=rolling_producer_plan,
        native_input_plan=native_input_plan,
        mark_producer_plan=mark_producer_plan,
        native_executor_membership_sha256=(
            native_executor_membership_sha256
        ),
        service_plan_id=service_plan_id,
        window_id=window_id,
        cutoff=cutoff,
        producer_build_id=producer_build_id.strip(),
        producer_build_sha256=producer_build_sha256,
        expected_closure_sha256=expected_closure_sha256,
    )
    digest = canonical_sha256(identity)
    plan = AdvancedOptionsInputReadyPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-input-ready-plan:{digest}",
        ),
        plan_sha256=digest,
        enrollment=enrollment,
        producer_plan=producer_plan,
        reference_producer_plan=reference_producer_plan,
        rolling_producer_plan=rolling_producer_plan,
        native_input_plan=native_input_plan,
        mark_producer_plan=mark_producer_plan,
        native_executor_membership_sha256=(
            native_executor_membership_sha256
        ),
        service_plan_id=service_plan_id,
        window_id=window_id,
        cutoff=cutoff,
        producer_build_id=producer_build_id.strip(),
        producer_build_sha256=producer_build_sha256,
        expected_closure_sha256=expected_closure_sha256,
    )
    _validate_input_ready_plan(plan)
    return plan


def advanced_options_input_ready_plan_payload(
    plan: AdvancedOptionsInputReadyPlan,
) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "plan_id": str(plan.plan_id),
        "plan_sha256": plan.plan_sha256,
        "identity": _validate_input_ready_plan(plan),
        "effect_counts": plan.zero_effect_counts,
    }


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputGatePlan:
    closure: AdvancedOptionsInputClosure
    closure_text: str
    closure_sha256: str
    window_id: UUID
    barrier_id: UUID
    jobs: tuple[MaterializationJob, ...]
    barrier_members: tuple[BarrierMember, ...]


def build_advanced_options_input_gate_plan(
    closure: AdvancedOptionsInputClosure,
    *,
    window_id: UUID,
    equity_window_id: UUID,
    instrument_ids: dict[str, int],
    build_id: str,
    available_at: datetime,
) -> AdvancedOptionsInputGatePlan:
    expected = {
        row.underlyer for row in closure.cycle_input.underlyings
    }
    if set(instrument_ids) != expected:
        raise ValueError("Options input gate instrument membership changed")
    text, digest = encode_advanced_options_input_closure(closure)
    jobs = tuple(
        build_closure_materialization_job(
            job_kind="OPTIONS_INPUT_MEMBER",
            cohort_id=closure.cycle_input.cohort_id,
            instrument_id=instrument_ids[row.underlyer],
            interval="1m",
            primary_window_id=window_id,
            confirmation_window_ids=(equity_window_id,),
            policy_sha256=closure.cycle_input.policy_sha256,
            build_id=build_id,
            available_at=available_at,
            bundle_sha256=digest,
        )
        for row in closure.cycle_input.underlyings
    )
    members = tuple(
        BarrierMember(
            member_key=f"UNDERLYER:{row.underlyer}",
            expected_input_sha256=canonical_sha256({
                "closure_sha256": digest,
                "underlyer": row.underlyer,
                "discovery_evidence_sha256": (
                    row.discovery_evidence_sha256
                ),
            }),
            job_id=job.job_id,
        )
        for row, job in zip(closure.cycle_input.underlyings, jobs)
    ) + (
        BarrierMember(
            member_key="EQUITY_INPUT_READY",
            expected_input_sha256=closure.equity_input_sha256,
            job_id=None,
        ),
    )
    barrier_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:options-input-barrier:{window_id}:{digest}",
    )
    return AdvancedOptionsInputGatePlan(
        closure,
        text,
        digest,
        window_id,
        barrier_id,
        jobs,
        members,
    )


def reconstruct_options_input_ready_receipt(
    gate: AdvancedOptionsInputGatePlan,
    verdict: JobBarrierVerdict,
    *,
    cutoff: datetime,
    reference_generation: int,
    build_id: str,
) -> CycleReceiptSpec:
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("OPTIONS_INPUT_READY cutoff must be timezone-aware")
    expected = tuple(
        sorted(member.member_key for member in gate.barrier_members)
    )
    if (
        verdict.barrier_id != gate.barrier_id
        or verdict.state != "COMPLETE"
        or verdict.expected_count != len(expected)
        or tuple(sorted(verdict.complete_members)) != expected
        or verdict.failed_members
        or verdict.open_members
        or reference_generation != gate.closure.reference_generation
        or not (
            gate.closure.reference_known_at
            <= cutoff
            < gate.closure.reference_valid_until
        )
    ):
        raise ValueError("OPTIONS_INPUT_READY barrier is incomplete")
    return build_options_receipt_spec(
        receipt_kind=OPTIONS_INPUT_READY,
        cutoff=cutoff,
        reference_generation=reference_generation,
        universe_sha256=gate.closure.cycle_input.universe_sha256,
        input_sha256=gate.closure_sha256,
        build_id=build_id,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputReadyPreflight:
    status: AdvancedOptionsInputDependencyStatus
    plan_id: UUID
    preflight_sha256: str
    evidence: tuple[AdvancedOptionsInputDependencyEvidence, ...]
    closure: AdvancedOptionsInputClosure | None = None
    gate: AdvancedOptionsInputGatePlan | None = None
    receipt: CycleReceiptSpec | None = None
    existing_receipt_id: UUID | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "preflight_sha256",
            _sha256(self.preflight_sha256, "preflight_sha256"),
        )
        rows = tuple(self.evidence)
        if (
            len({
                (row.member_kind, row.member_key) for row in rows
            }) != len(rows)
            or any(
                not isinstance(
                    row,
                    AdvancedOptionsInputDependencyEvidence,
                )
                for row in rows
            )
        ):
            raise ValueError(
                "Advanced Options input-ready evidence is invalid"
            )
        ready = self.status is AdvancedOptionsInputDependencyStatus.AVAILABLE
        if ready != (
            self.closure is not None
            and self.gate is not None
            and self.receipt is not None
        ):
            raise ValueError(
                "Advanced Options input-ready preflight payload is invalid"
            )
        blocking = tuple(
            row
            for row in rows
            if row.status is AdvancedOptionsInputDependencyStatus.BLOCKED
            or (
                row.status
                is AdvancedOptionsInputDependencyStatus.UNAVAILABLE
                and not row.accepted_unavailable
            )
        )
        if ready and blocking:
            raise ValueError(
                "ready Advanced Options input preflight has blockers"
            )
        if not ready and not blocking:
            raise ValueError(
                "blocked Advanced Options input preflight lacks blockers"
            )

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(sorted({
            reason
            for row in self.evidence
            for reason in row.reason_codes
            if (
                row.status is AdvancedOptionsInputDependencyStatus.BLOCKED
                or (
                    row.status
                    is AdvancedOptionsInputDependencyStatus.UNAVAILABLE
                    and not row.accepted_unavailable
                )
            )
        }))


@dataclass(frozen=True, slots=True)
class AdvancedOptionsInputReadyResult:
    receipt_id: UUID
    receipt: CycleReceiptSpec
    closure_sha256: str
    preflight_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.receipt_id, UUID)
            or not isinstance(self.receipt, CycleReceiptSpec)
            or self.receipt.receipt_kind != OPTIONS_INPUT_READY
            or self.receipt.input_sha256
            != _sha256(self.closure_sha256, "closure_sha256")
        ):
            raise ValueError(
                "Advanced Options input-ready result is invalid"
            )
        object.__setattr__(
            self,
            "preflight_sha256",
            _sha256(self.preflight_sha256, "preflight_sha256"),
        )


class AdvancedOptionsInputReadyBlocked(ValueError):
    def __init__(
        self,
        result: AdvancedOptionsInputReadyPreflight,
    ) -> None:
        self.result = result
        super().__init__(
            "Advanced Options input-ready preflight is blocked: "
            + ",".join(result.reason_codes)
        )


def freeze_advanced_options_input_ready_plan(
    plan: AdvancedOptionsInputReadyPlan,
    preflight: AdvancedOptionsInputReadyPreflight,
) -> AdvancedOptionsInputReadyPlan:
    _validate_input_ready_plan(plan)
    if (
        plan.frozen
        or preflight.plan_id != plan.plan_id
        or preflight.status
        is not AdvancedOptionsInputDependencyStatus.AVAILABLE
        or preflight.closure is None
    ):
        raise ValueError(
            "Advanced Options input-ready preflight does not match PLAN"
        )
    return build_advanced_options_input_ready_plan(
        enrollment=plan.enrollment,
        producer_plan=plan.producer_plan,
        reference_producer_plan=plan.reference_producer_plan,
        rolling_producer_plan=plan.rolling_producer_plan,
        native_input_plan=plan.native_input_plan,
        mark_producer_plan=plan.mark_producer_plan,
        native_executor_membership_sha256=(
            plan.native_executor_membership_sha256
        ),
        service_plan_id=plan.service_plan_id,
        window_id=plan.window_id,
        cutoff=plan.cutoff,
        producer_build_id=plan.producer_build_id,
        producer_build_sha256=plan.producer_build_sha256,
        expected_closure_sha256=preflight.closure.sha256,
    )
