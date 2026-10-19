from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_json, canonical_sha256
from .detector_adapters import StableDetectorAdapterResult


MAX_DETECTOR_CHECKPOINT_BYTES = 1_048_576
DETECTOR_CHECKPOINT_CODEC = "realtime_detector_state_json_v1"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: str, name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class RealtimeDetectorVersion:
    detector_sha256: str
    detector_id: str
    version: str
    code_sha256: str
    policy_sha256: str
    valuation_sha256: str | None
    requirements: Mapping[str, object]
    output_kind: str

    def __post_init__(self) -> None:
        _sha256(self.detector_sha256, "detector_sha256")
        _sha256(self.code_sha256, "code_sha256")
        _sha256(self.policy_sha256, "policy_sha256")
        if self.valuation_sha256 is not None:
            _sha256(self.valuation_sha256, "valuation_sha256")
        if not self.detector_id.strip() or not self.version.strip():
            raise ValueError("detector identity cannot be blank")
        if self.output_kind not in {"OBSERVATION", "SIGNAL", "ALERT", "ASSESSMENT"}:
            raise ValueError("unsupported detector output kind")
        canonical_json(self.requirements)


@dataclass(frozen=True, slots=True)
class RealtimeDetectorInput:
    input_role: str
    input_key: str
    source_kind: str
    input_sha256: str
    available_at: datetime
    received_at: datetime
    retained_input: Mapping[str, object]
    feature_snapshot_id: UUID | None = None
    bar_revision_id: UUID | None = None
    option_mark_evidence_id: UUID | None = None
    source_record_id: UUID | None = None
    journal_generation: UUID | None = None
    stream_session_id: UUID | None = None
    source_offset: int | None = None
    source_flow_id: UUID | None = None
    source_cohort_id: UUID | None = None

    def __post_init__(self) -> None:
        if not self.input_role.strip() or not self.input_key.strip():
            raise ValueError("detector input identity cannot be blank")
        _sha256(self.input_sha256, "input_sha256")
        available = _utc(self.available_at, "available_at")
        received = _utc(self.received_at, "received_at")
        if received < available:
            raise ValueError("detector input receipt precedes availability")
        direct_sources = tuple(value for value in (
            self.feature_snapshot_id,
            self.bar_revision_id,
            self.option_mark_evidence_id,
            self.source_record_id,
        ) if value is not None)
        journal_values = (
            self.journal_generation,
            self.stream_session_id,
            self.source_offset,
        )
        has_journal = all(value is not None for value in journal_values)
        if any(value is not None for value in journal_values) and not has_journal:
            raise ValueError("journal detector input identity must be complete")
        source_flow_values = (self.source_flow_id, self.source_cohort_id)
        if any(value is not None for value in source_flow_values) and not all(
            value is not None for value in source_flow_values
        ):
            raise ValueError("source-flow detector input binding must be complete")
        if self.source_flow_id is not None and not has_journal:
            raise ValueError("source-flow binding requires a journal detector input")
        if len(direct_sources) + int(has_journal) != 1:
            raise ValueError("detector input must name exactly one causal source")
        if self.source_offset is not None and self.source_offset <= 0:
            raise ValueError("detector input source offset must be positive")
        canonical_json(self.retained_input)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "received_at", received)


@dataclass(frozen=True, slots=True)
class RealtimeDetectorEvent:
    event_id: UUID
    dedup_sha256: str
    cohort_id: UUID
    provider_tier: str
    detector_sha256: str
    instrument_id: int
    trigger_identity: str
    trigger_at: datetime
    decision_at: datetime
    valid_until: datetime | None
    disposition: str
    direction: int | None
    reason_codes: tuple[str, ...]
    readiness_epoch: int
    evidence_sha256: str
    payload_schema: str
    payload: Mapping[str, object]
    inputs: tuple[RealtimeDetectorInput, ...]


@dataclass(frozen=True, slots=True)
class RealtimeDetectorState:
    detector_sha256: str
    cohort_id: UUID
    state_key: str
    revision: int
    state_schema: str
    state_sha256: str
    state_payload: Mapping[str, object]
    updated_at: datetime

    def __post_init__(self) -> None:
        _sha256(self.detector_sha256, "detector_sha256")
        _sha256(self.state_sha256, "state_sha256")
        if not self.state_key.strip() or not self.state_schema.strip():
            raise ValueError("detector state identity cannot be blank")
        if type(self.revision) is not int or self.revision <= 0:
            raise ValueError("detector state revision must be positive")
        if canonical_sha256(self.state_payload) != self.state_sha256:
            raise ValueError("detector state payload hash does not match")
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))


@dataclass(frozen=True, slots=True)
class RealtimeDetectorCheckpoint:
    checkpoint_id: UUID
    consumer_name: str
    cohort_id: UUID
    journal_generation: UUID
    shard_id: int
    shard_map_sha256: str
    owner_epoch: int
    committed_offset: int
    state_codec: str
    state_sha256: str
    state_payload: bytes
    committed_at: datetime

    def __post_init__(self) -> None:
        _sha256(self.shard_map_sha256, "shard_map_sha256")
        _sha256(self.state_sha256, "state_sha256")
        if not self.consumer_name.strip():
            raise ValueError("checkpoint consumer cannot be blank")
        if self.shard_id < 0 or self.owner_epoch <= 0 or self.committed_offset < 0:
            raise ValueError("invalid detector checkpoint coordinate")
        if self.state_codec != DETECTOR_CHECKPOINT_CODEC:
            raise ValueError("unsupported detector checkpoint codec")
        if not 1 <= len(self.state_payload) <= MAX_DETECTOR_CHECKPOINT_BYTES:
            raise ValueError("detector checkpoint payload is outside size bounds")
        if hashlib.sha256(self.state_payload).hexdigest() != self.state_sha256:
            raise ValueError("detector checkpoint payload hash does not match")
        object.__setattr__(self, "committed_at", _utc(self.committed_at, "committed_at"))


def build_detector_version(
    *,
    detector_id: str,
    version: str,
    code_sha256: str,
    policy_sha256: str,
    requirements: Mapping[str, object],
    output_kind: str,
    valuation_sha256: str | None = None,
) -> RealtimeDetectorVersion:
    identity = {
        "detector_id": detector_id,
        "version": version,
        "code_sha256": code_sha256,
        "policy_sha256": policy_sha256,
        "valuation_sha256": valuation_sha256,
        "requirements": requirements,
        "output_kind": output_kind,
    }
    return RealtimeDetectorVersion(
        detector_sha256=canonical_sha256(identity),
        detector_id=detector_id,
        version=version,
        code_sha256=code_sha256,
        policy_sha256=policy_sha256,
        valuation_sha256=valuation_sha256,
        requirements=requirements,
        output_kind=output_kind,
    )


def build_detector_event(
    adapter: StableDetectorAdapterResult,
    version: RealtimeDetectorVersion,
    *,
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
) -> RealtimeDetectorEvent:
    trigger = _utc(trigger_at, "trigger_at")
    decision = _utc(decision_at, "decision_at")
    expiry = _utc(valid_until, "valid_until") if valid_until is not None else None
    if decision < trigger or expiry is not None and expiry <= trigger:
        raise ValueError("detector event clocks are not causal")
    if instrument_id <= 0 or readiness_epoch <= 0:
        raise ValueError("detector event instrument and readiness epoch must be positive")
    if direction not in (None, -1, 0, 1):
        raise ValueError("detector event direction is invalid")
    if not trigger_identity.strip() or not payload_schema.strip():
        raise ValueError("detector event identity cannot be blank")
    if len({(row.input_role, row.input_key) for row in inputs}) != len(inputs):
        raise ValueError("detector event input identities must be unique")
    evidence_sha256 = canonical_sha256({
        "payload": payload,
        "inputs": tuple((row.input_role, row.input_key, row.input_sha256) for row in inputs),
    })
    dedup_sha256 = canonical_sha256({
        "cohort_id": adapter.cohort_id,
        "detector_sha256": version.detector_sha256,
        "instrument_id": instrument_id,
        "trigger_identity": trigger_identity,
        "trigger_at": trigger,
        "adapter_input_sha256": adapter.input_sha256,
        "evidence_sha256": evidence_sha256,
    })
    event_id = uuid5(NAMESPACE_URL, f"realtime-detector-event:{dedup_sha256}")
    return RealtimeDetectorEvent(
        event_id=event_id,
        dedup_sha256=dedup_sha256,
        cohort_id=adapter.cohort_id,
        provider_tier=adapter.provider_tier.value,
        detector_sha256=version.detector_sha256,
        instrument_id=instrument_id,
        trigger_identity=trigger_identity,
        trigger_at=trigger,
        decision_at=decision,
        valid_until=expiry,
        disposition=disposition,
        direction=direction,
        reason_codes=reason_codes,
        readiness_epoch=readiness_epoch,
        evidence_sha256=evidence_sha256,
        payload_schema=payload_schema,
        payload=payload,
        inputs=inputs,
    )


def build_detector_checkpoint(
    state_payload: Mapping[str, object],
    *,
    consumer_name: str,
    cohort_id: UUID,
    journal_generation: UUID,
    shard_id: int,
    shard_map_sha256: str,
    owner_epoch: int,
    committed_offset: int,
    committed_at: datetime,
) -> RealtimeDetectorCheckpoint:
    payload = canonical_json(state_payload).encode("ascii")
    state_sha256 = hashlib.sha256(payload).hexdigest()
    checkpoint_id = uuid5(
        NAMESPACE_URL,
        f"realtime-detector-checkpoint:{consumer_name.strip()}:{cohort_id}:"
        f"{journal_generation}:{shard_id}:{committed_offset}:{state_sha256}",
    )
    return RealtimeDetectorCheckpoint(
        checkpoint_id=checkpoint_id,
        consumer_name=consumer_name,
        cohort_id=cohort_id,
        journal_generation=journal_generation,
        shard_id=shard_id,
        shard_map_sha256=shard_map_sha256,
        owner_epoch=owner_epoch,
        committed_offset=committed_offset,
        state_codec=DETECTOR_CHECKPOINT_CODEC,
        state_sha256=state_sha256,
        state_payload=payload,
        committed_at=committed_at,
    )