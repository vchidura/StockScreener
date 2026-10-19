"""Compact P4A Advanced Options input closure and existing job/barrier binding."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_control_plane import AdvancedOptionsControlPlanePlan
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


ADVANCED_OPTIONS_INPUT_VERSION = "advanced_options_input_closure_v2"
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

    @property
    def sha256(self) -> str:
        return encode_advanced_options_input_closure(self)[1]


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
    if (
        rolling_state.cohort_id != control_plane.cohort_id
        or rolling_state.session_date != cycle_input.scheduled_cycle.date()
        or rolling_state.journal_generation
        != control_plane.source_flow.journal_generation
        or tuple(
            row.ticker for row in rolling_state.underlying_ticks
        ) != tuple(row.underlyer for row in cycle_input.underlyings)
        or rolling_state.market_state_checkpoint_sha256
        != _sha256(
            market_state_checkpoint_sha256,
            "market_state_checkpoint_sha256",
        )
        or rolling_state.flow_checkpoint_sha256
        != _sha256(flow_checkpoint_sha256, "flow_checkpoint_sha256")
        or rolling_state.committed_at > producer_plan.clocks.known_at
    ):
        raise ValueError("Advanced Options Tier B rolling state changed")
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
        source_flow_sha256=control_plane.source_flow.source_flow_sha256,
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
    )


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
