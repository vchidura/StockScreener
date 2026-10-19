from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping
from uuid import UUID


@dataclass(frozen=True, slots=True)
class FlowRetentionEvidence:
    profile: str
    universe_tier: str
    dte_lane: str
    threshold_mode: str
    minimum_contracts: int
    minimum_print_premium: Decimal
    contracts_matched: bool
    premium_matched: bool
    minimum_cluster_prints: int | None
    minimum_distinct_exchanges: int | None
    minimum_cluster_premium: Decimal | None
    minimum_cumulative_contracts: int | None
    window_seconds: int | None


@dataclass(frozen=True, slots=True)
class FlowEvent:
    """One persisted print. The first block maps 1:1 to rt.option_flow_events."""

    event_id: UUID
    event_time: datetime
    cohort_id: UUID
    session_date: date
    provider_tier: str
    contract_ticker: str
    underlyer: str
    source_seq: int
    exchange_id: int | None
    price: Decimal
    size: int
    multiplier: int
    premium: Decimal
    conditions: tuple[int, ...]
    condition_semantics_version: str
    trade_treatment: str
    is_multi_leg: bool
    side_class: str
    side_confidence: str
    quote_bid: Decimal | None
    quote_ask: Decimal | None
    quote_bid_size: int | None
    quote_ask_size: int | None
    quote_time: datetime | None
    quote_seq: int | None
    cumulative_volume_before: int | None
    cumulative_volume_after: int | None
    volume_baseline_kind: str
    prior_open_interest: int | None
    open_interest_as_of: date | None
    volume_exceeds_prior_oi: bool
    opening_inference: str
    spread_position: Decimal | None
    opening_score: Decimal | None
    opening_reasons: tuple[str, ...]
    journal_generation: UUID | None
    shard_id: int
    source_offset: int | None
    source_flow_id: UUID | None
    condition_semantics_sha256: str | None
    flow_policy_sha256: str | None
    lineage_version: int
    # 1 for a print; a cancel is a new row at the superseded row's revision + 1.
    revision: int
    supersedes_event_id: UUID | None
    # Not persisted: the schema has no column for these.
    time_ms: int
    reasons: tuple[str, ...]
    is_cross: bool
    is_sweep: bool
    is_floor: bool
    is_late: bool
    matched_profiles: tuple[str, ...] = ()
    flow_universe_tier: str | None = None
    flow_dte_lane: str | None = None
    contract_expiration: date | None = None
    retention_thresholds: tuple[FlowRetentionEvidence, ...] = ()
    advanced_flow_policy_sha256: str | None = None
    contract_scope_sha256: str | None = None
    manifest_sha256: str | None = None
    slot_sha256: str | None = None
    runtime_policy_sha256: str | None = None
    provider_contract_sha256: str | None = None
    condition_reference_sha256: str | None = None
    exchange_reference_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class ClusterRow:
    """One cluster revision. Maps 1:1 to rt.option_flow_clusters.

    A cluster that lost every print to cancels is a RETRACTED revision with zero counts.
    """

    cluster_id: UUID
    revision: int
    session_date: date
    cohort_id: UUID
    provider_tier: str
    contract_ticker: str
    underlyer: str
    window_seconds: int
    window_start: datetime
    window_end: datetime
    side_class: str
    print_count: int
    total_size: int
    total_premium: Decimal
    distinct_exchanges: int
    first_event_time: datetime
    last_event_time: datetime
    status: str
    policy_sha256: str
    source_flow_id: UUID | None
    condition_semantics_sha256: str | None
    lineage_version: int
    flow_policy_sha256: str | None = None
    profile: str | None = None
    flow_universe_tier: str | None = None
    flow_dte_lane: str | None = None
    retention_threshold: FlowRetentionEvidence | None = None
    advanced_flow_policy_sha256: str | None = None
    contract_scope_sha256: str | None = None
    manifest_sha256: str | None = None
    slot_sha256: str | None = None
    runtime_policy_sha256: str | None = None
    cumulative_contracts: int | None = None
    qualifies: bool | None = None
    member_event_ids: tuple[UUID, ...] = ()
    member_event_keys: tuple[tuple[UUID, datetime], ...] = ()
    provider_contract_sha256: str | None = None
    condition_reference_sha256: str | None = None
    exchange_reference_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class VolumeCheckpoint:
    """Maps 1:1 to rt.option_volume_checkpoints."""

    cohort_id: UUID
    session_date: date
    contract_ticker: str
    cumulative_volume: int
    last_source_seq: int
    last_event_time: datetime | None
    baseline_kind: str
    snapshot_volume: int | None
    snapshot_watermark_seq: int | None


@dataclass(frozen=True, slots=True)
class GapRecord:
    """A stream gap in rt.stream_gaps terms (the orchestrator adds session and generation)."""

    shard_id: int | None
    channel: str | None
    reason: str
    start_ms: int | None
    end_ms: int | None
    first_offset: int | None
    last_offset: int | None
    dropped_messages: int
    contracts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    contract_ticker: str
    session_date: date
    accepted: bool
    reason: str | None
    cumulative_volume: int
    baseline_kind: str
    previous_cumulative_volume: int


class EngineOutput:
    """Rows produced by one processing call."""

    __slots__ = ("events", "clusters", "checkpoints", "gaps", "reconciles", "offsets")

    def __init__(self) -> None:
        self.events: list[FlowEvent] = []
        self.clusters: list[ClusterRow] = []
        self.checkpoints: list[VolumeCheckpoint] = []
        self.gaps: list[GapRecord] = []
        self.reconciles: list[ReconcileResult] = []
        # Highest journal offset processed per shard; valid once these rows are durable.
        self.offsets: dict[int, int] = {}

    def extend(self, other: "EngineOutput") -> None:
        self.events.extend(other.events)
        self.clusters.extend(other.clusters)
        self.checkpoints.extend(other.checkpoints)
        self.gaps.extend(other.gaps)
        self.reconciles.extend(other.reconciles)
        for shard, offset in other.offsets.items():
            if offset > self.offsets.get(shard, 0):
                self.offsets[shard] = offset

    def __bool__(self) -> bool:
        return any((self.events, self.clusters, self.checkpoints, self.gaps, self.reconciles))

    def counts(self) -> Mapping[str, int]:
        return {name: len(getattr(self, name)) for name in self.__slots__ if name != "offsets"}
