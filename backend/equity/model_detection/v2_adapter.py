"""Qualified materialization -> V2-compatible input/result envelopes, in memory.

Pass ``StableDetectorAdapterResult`` and ``RealtimeDetectorInput`` as the two
factories. Constructor injection avoids their current legacy research imports
inside this package. Cohort, tier and snapshot IDs are envelope coordinates only;
they never enter feature, policy, trigger or episode identities.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, Mapping, TypeVar, cast
from uuid import UUID

from .codec import decode_input
from .contracts import CanonicalInput, Decision, Disposition, canonical_sha256, canonical_value, freeze, sha256
from .engine import ModelEvaluator, evaluate
from .policies import BreakoutPolicy


ResultT = TypeVar("ResultT")
InputT = TypeVar("InputT")


@dataclass(frozen=True, slots=True)
class QualifiedV2Materialization:
    feature_snapshot_id: UUID
    input_payload: Mapping[str, Any]
    payload_sha256: str
    qualified: bool

    def __post_init__(self) -> None:
        if not isinstance(self.feature_snapshot_id, UUID) or self.qualified is not True:
            raise ValueError("a qualified V2 snapshot with immutable ID is required")
        sha256(self.payload_sha256)
        object.__setattr__(self, "input_payload", cast(Mapping[str, Any], freeze(self.input_payload)))
        if canonical_sha256(self.input_payload) != self.payload_sha256:
            raise ValueError("V2 materialization payload hash mismatch")


@dataclass(frozen=True, slots=True)
class V2Evaluation(Generic[ResultT, InputT]):
    adapter: ResultT
    inputs: tuple[InputT, ...]
    canonical_input: CanonicalInput
    decision: Decision
    state_payload: Mapping[str, object]
    event_payload: Mapping[str, object]


def run_v2(materialization: QualifiedV2Materialization, policy: BreakoutPolicy, *,
           cohort_id: UUID, provider_tier: Any,
           result_factory: Callable[..., ResultT], input_factory: Callable[..., InputT],
           evaluator: ModelEvaluator = evaluate) -> V2Evaluation[ResultT, InputT]:
    """Construct actual existing V2 types without persistence or authority flags."""
    if not isinstance(cohort_id, UUID):
        raise ValueError("cohort_id must be a UUID")
    packet = decode_input(materialization.input_payload)
    decision = evaluator(packet, policy)
    observations = (*packet.bars, *packet.events, packet.context)
    causal_input = input_factory(
        input_role="FEATURE_SNAPSHOT", input_key=str(materialization.feature_snapshot_id),
        source_kind="FEATURE_SNAPSHOT", input_sha256=materialization.payload_sha256,
        available_at=max(item.clocks.available_at for item in observations),
        received_at=max(item.clocks.received_at for item in observations),
        retained_input=cast(Mapping[str, object], canonical_value(materialization.input_payload)),
        feature_snapshot_id=materialization.feature_snapshot_id,
    )
    adapter = result_factory(
        adapter_id=f"shared-equity:{policy.model_id}:{policy.version}", cohort_id=cohort_id,
        provider_tier=provider_tier, input_sha256=packet.input_sha256,
        status="UNAVAILABLE" if decision.disposition in (Disposition.UNAVAILABLE, Disposition.ERROR) else "EXECUTED",
        output=decision, reason_codes=decision.reason_codes,
        publication_permission=False, execution_permission=False,
    )
    state = cast(Mapping[str, object], freeze(canonical_value(decision.state)))
    event = cast(Mapping[str, object], freeze(canonical_value({
        "schema": "equity_model_event_v1", "model": decision.model,
        "input_sha256": decision.input_sha256, "feature_sha256": decision.features.feature_sha256,
        "decision_sha256": decision.decision_sha256, "disposition": decision.disposition,
        "gates": decision.gates, "direction": decision.direction, "trigger_identity": decision.trigger_identity,
        "state_key": decision.state.state_key, "state_revision": decision.state.revision,
        "geometry": decision.geometry, "valid_until": decision.valid_until, "reason_codes": decision.reason_codes,
        "publication_permission": False, "execution_permission": False,
    })))
    return V2Evaluation(adapter, (causal_input,), packet, decision, state, event)
