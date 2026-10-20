"""Evidence-only Advanced Equity detector shadow contract."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

from .contracts import canonical_sha256
from .detector_adapters import StableDetectorAdapterResult
from .detector_evidence import (
    RealtimeDetectorCheckpoint,
    RealtimeDetectorEvent,
    RealtimeDetectorInput,
    RealtimeDetectorState,
    RealtimeDetectorVersion,
    build_detector_checkpoint,
    build_detector_event,
    build_detector_version,
)
from .detector_evidence_repository import (
    persist_detector_evaluation_batch_cursor,
)
from .preflight import ProviderTier


CONTRACT_VERSION = "advanced-equity-detector-shadow-v1"
STATE_SCHEMA = "advanced_equity_detector_shadow_state_v1"
PAYLOAD_SOURCE = "MASSIVE_ADVANCED_JOURNAL"
PRODUCTION_AUTHORITY: Mapping[str, bool] = MappingProxyType({
    "provider_requests": False,
    "portal_publication": False,
    "outbox_creation": False,
    "outbox_dispatch": False,
    "platform_pointer": False,
    "external_delivery": False,
    "trade_execution": False,
})


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorShadowBatch:
    lease_resource_key: str
    owner_id: UUID
    version: RealtimeDetectorVersion
    state: RealtimeDetectorState
    event: RealtimeDetectorEvent
    checkpoint: RealtimeDetectorCheckpoint

    def __post_init__(self) -> None:
        resource_key = self.lease_resource_key.strip()
        if not resource_key:
            raise ValueError("detector shadow lease resource key cannot be blank")
        if (
            self.state.detector_sha256 != self.version.detector_sha256
            or self.event.detector_sha256 != self.version.detector_sha256
            or self.state.cohort_id != self.checkpoint.cohort_id
            or self.event.cohort_id != self.checkpoint.cohort_id
        ):
            raise ValueError("detector shadow batch identity is inconsistent")
        object.__setattr__(self, "lease_resource_key", resource_key)


def detector_shadow_input_sha256(
    inputs: tuple[RealtimeDetectorInput, ...],
) -> str:
    if not inputs:
        raise ValueError("detector shadow requires retained causal inputs")
    return canonical_sha256({
        "contract_version": CONTRACT_VERSION,
        "inputs": tuple(
            {
                "input_role": row.input_role,
                "input_key": row.input_key,
                "source_kind": row.source_kind,
                "input_sha256": row.input_sha256,
                "available_at": row.available_at,
                "received_at": row.received_at,
            }
            for row in inputs
        ),
    })


def build_advanced_equity_detector_shadow_batch(
    *,
    adapter: StableDetectorAdapterResult,
    detector_id: str,
    detector_version: str,
    code_sha256: str,
    policy_sha256: str,
    requirements: Mapping[str, object],
    output_kind: str,
    instrument_id: int,
    trigger_identity: str,
    trigger_at: datetime,
    decision_at: datetime,
    valid_until: datetime | None,
    disposition: str,
    direction: int | None,
    reason_codes: tuple[str, ...],
    readiness_epoch: int,
    payload_schema: str,
    payload: Mapping[str, object],
    inputs: tuple[RealtimeDetectorInput, ...],
    state_key: str,
    state_revision: int,
    state_payload: Mapping[str, object],
    lease_resource_key: str,
    owner_id: UUID,
    journal_generation: UUID,
    shard_id: int,
    shard_map_sha256: str,
    owner_epoch: int,
    committed_offset: int,
    committed_at: datetime,
) -> AdvancedEquityDetectorShadowBatch:
    if adapter.provider_tier is not ProviderTier.MASSIVE_ADVANCED:
        raise ValueError("detector shadow requires MASSIVE_ADVANCED inputs")
    if adapter.status != "EXECUTED":
        raise ValueError("detector shadow requires an executed detector result")
    if adapter.publication_permission or adapter.execution_permission:
        raise ValueError("detector shadow cannot carry publication or execution authority")
    if (
        not isinstance(adapter.output, Mapping)
        or canonical_sha256(adapter.output) != canonical_sha256(payload)
    ):
        raise ValueError("detector shadow payload differs from its adapter result")
    if output_kind not in {"OBSERVATION", "SIGNAL"}:
        raise ValueError("detector shadow output must be evidence-only")
    source_inputs = tuple(
        row for row in inputs if row.journal_generation is not None
    )
    if not source_inputs:
        raise ValueError("detector shadow requires a journal-bound input")
    if any(
        row.journal_generation != journal_generation
        or row.source_offset is None
        or row.source_offset > committed_offset
        for row in source_inputs
    ):
        raise ValueError("detector shadow input exceeds its journal checkpoint")
    expected_input_sha256 = detector_shadow_input_sha256(inputs)
    if adapter.input_sha256 != expected_input_sha256:
        raise ValueError("detector shadow adapter input identity changed")

    version = build_detector_version(
        detector_id=detector_id,
        version=detector_version,
        code_sha256=code_sha256,
        policy_sha256=policy_sha256,
        requirements={
            "contract_version": CONTRACT_VERSION,
            "source": PAYLOAD_SOURCE,
            "requirements": requirements,
        },
        output_kind=output_kind,
    )
    event = build_detector_event(
        adapter,
        version,
        instrument_id=instrument_id,
        trigger_identity=trigger_identity,
        trigger_at=trigger_at,
        decision_at=decision_at,
        valid_until=valid_until,
        disposition=disposition,
        direction=direction,
        reason_codes=tuple(dict.fromkeys((*adapter.reason_codes, *reason_codes))),
        readiness_epoch=readiness_epoch,
        payload_schema=payload_schema,
        payload=payload,
        inputs=inputs,
    )
    retained_state = {
        "contract_version": CONTRACT_VERSION,
        "detector_sha256": version.detector_sha256,
        "last_event_id": str(event.event_id),
        "last_event_dedup_sha256": event.dedup_sha256,
        "committed_offset": committed_offset,
        "state": state_payload,
    }
    state = RealtimeDetectorState(
        detector_sha256=version.detector_sha256,
        cohort_id=adapter.cohort_id,
        state_key=state_key,
        revision=state_revision,
        state_schema=STATE_SCHEMA,
        state_sha256=canonical_sha256(retained_state),
        state_payload=retained_state,
        updated_at=committed_at,
    )
    checkpoint = build_detector_checkpoint(
        {
            "contract_version": CONTRACT_VERSION,
            "detector_sha256": version.detector_sha256,
            "event_id": str(event.event_id),
            "event_dedup_sha256": event.dedup_sha256,
            "trigger_at": event.trigger_at.isoformat(),
            "state_key": state.state_key,
            "state_revision": state.revision,
            "state_sha256": state.state_sha256,
            "committed_offset": committed_offset,
        },
        consumer_name=(
            f"advanced-equity-detector-shadow:{detector_id}:{shard_id}"
        ),
        cohort_id=adapter.cohort_id,
        journal_generation=journal_generation,
        shard_id=shard_id,
        shard_map_sha256=shard_map_sha256,
        owner_epoch=owner_epoch,
        committed_offset=committed_offset,
        committed_at=committed_at,
    )
    return AdvancedEquityDetectorShadowBatch(
        lease_resource_key=lease_resource_key,
        owner_id=owner_id,
        version=version,
        state=state,
        event=event,
        checkpoint=checkpoint,
    )


def persist_advanced_equity_detector_shadow_cursor(
    cursor,
    batch: AdvancedEquityDetectorShadowBatch,
) -> None:
    if not isinstance(batch, AdvancedEquityDetectorShadowBatch):
        raise TypeError("Advanced Equity detector shadow batch is invalid")
    persist_detector_evaluation_batch_cursor(
        cursor,
        lease_resource_key=batch.lease_resource_key,
        owner_id=batch.owner_id,
        checkpoint=batch.checkpoint,
        versions=(batch.version,),
        states=(batch.state,),
        events=(batch.event,),
        alerts=(),
    )
