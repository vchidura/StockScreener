"""Immutable P6D5 flow-to-package evidence over existing persisted owners."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from .advanced_options_cycle import (
    AdvancedOptionsCycleResult,
    AdvancedOptionsDetectorEvaluation,
    persist_advanced_options_cycle_cursor,
)
from .advanced_options_reference_state import AdvancedOptionsReferenceState
from .advanced_options_rolling_state import (
    AdvancedOptionsRollingState,
    RollingAvailability,
)
from .contracts import canonical_json, canonical_sha256
from .option_flow.records import ClusterRow, FlowEvent
from .option_flow.volume import BaselineKind, COMPLETE_BASELINES
from .option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimeOptionPackageMark,
)


FLOW_PACKAGE_EVIDENCE_VERSION = "advanced-options-flow-package-evidence-v1"


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


def _reasons(values: tuple[str, ...]) -> tuple[str, ...]:
    reasons = tuple(sorted({value.strip().upper() for value in values}))
    if any(not value for value in reasons):
        raise ValueError("evidence reason codes cannot be blank")
    return reasons


@dataclass(frozen=True, slots=True)
class AdvancedOptionsFlowObservation:
    observation_id: UUID
    observation_sha256: str
    event_id: UUID
    event_revision: int
    event_time: datetime
    source_flow_id: UUID
    source_offset: int
    contract_ticker: str
    underlyer: str
    prior_open_interest: int | None
    cumulative_volume_before: int
    cumulative_volume_after: int
    baseline_kind: BaselineKind
    volume_exceeds_prior_oi: bool
    premium: Decimal
    conditions: tuple[int, ...]
    side_class: str
    side_confidence: str
    spread_position: Decimal | None
    quote_bid: Decimal | None
    quote_ask: Decimal | None
    quote_bid_size: int | None
    quote_ask_size: int | None
    quote_time: datetime | None
    quote_seq: int | None
    rolling_state_id: UUID
    rolling_state_sha256: str
    flow_checkpoint_sha256: str
    reference_state_id: UUID
    reference_state_sha256: str
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        event_time = _utc(self.event_time, "event_time")
        quote_time = (
            None
            if self.quote_time is None
            else _utc(self.quote_time, "quote_time")
        )
        reasons = _reasons(self.reason_codes)
        if (
            not isinstance(self.event_id, UUID)
            or not isinstance(self.source_flow_id, UUID)
            or not isinstance(self.rolling_state_id, UUID)
            or not isinstance(self.reference_state_id, UUID)
            or type(self.event_revision) is not int
            or self.event_revision <= 0
            or type(self.source_offset) is not int
            or self.source_offset <= 0
            or self.cumulative_volume_before < 0
            or self.cumulative_volume_after < self.cumulative_volume_before
            or not self.premium.is_finite()
            or self.premium < 0
            or quote_time is not None
            and quote_time > event_time
        ):
            raise ValueError("flow observation identity or clocks are invalid")
        classified = self.side_class in {
            "AT_OR_ABOVE_ASK",
            "AT_OR_BELOW_BID",
            "INSIDE_SPREAD",
        }
        quote_complete = all(
            value is not None
            for value in (
                self.quote_bid,
                self.quote_ask,
                self.quote_bid_size,
                self.quote_ask_size,
                quote_time,
                self.quote_seq,
            )
        )
        if classified != quote_complete:
            raise ValueError("aggression evidence is incomplete")
        if (self.side_class == "INSIDE_SPREAD") != (
            self.spread_position is not None
        ):
            raise ValueError("spread-position evidence is inconsistent")
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "quote_time", quote_time)
        object.__setattr__(self, "reason_codes", reasons)
        for name in (
            "rolling_state_sha256",
            "flow_checkpoint_sha256",
            "reference_state_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected_sha256 = canonical_sha256(self.identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-flow-observation:{expected_sha256}",
        )
        if (
            self.observation_sha256 != expected_sha256
            or self.observation_id != expected_id
        ):
            raise ValueError("flow observation ID or SHA changed")

    @property
    def identity(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "event_revision": self.event_revision,
            "event_time": self.event_time,
            "source_flow_id": self.source_flow_id,
            "source_offset": self.source_offset,
            "contract_ticker": self.contract_ticker,
            "underlyer": self.underlyer,
            "prior_open_interest": self.prior_open_interest,
            "cumulative_volume_before": self.cumulative_volume_before,
            "cumulative_volume_after": self.cumulative_volume_after,
            "baseline_kind": self.baseline_kind,
            "volume_exceeds_prior_oi": self.volume_exceeds_prior_oi,
            "premium": self.premium,
            "conditions": self.conditions,
            "side_class": self.side_class,
            "side_confidence": self.side_confidence,
            "spread_position": self.spread_position,
            "quote_bid": self.quote_bid,
            "quote_ask": self.quote_ask,
            "quote_bid_size": self.quote_bid_size,
            "quote_ask_size": self.quote_ask_size,
            "quote_time": self.quote_time,
            "quote_seq": self.quote_seq,
            "rolling_state_id": self.rolling_state_id,
            "rolling_state_sha256": self.rolling_state_sha256,
            "flow_checkpoint_sha256": self.flow_checkpoint_sha256,
            "reference_state_id": self.reference_state_id,
            "reference_state_sha256": self.reference_state_sha256,
            "reason_codes": self.reason_codes,
        }


def build_flow_observation(
    event: FlowEvent,
    *,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
) -> AdvancedOptionsFlowObservation:
    if (
        event.source_flow_id is None
        or event.source_offset is None
        or event.cumulative_volume_before is None
        or event.cumulative_volume_after is None
    ):
        raise ValueError("flow event source/checkpoint identity is incomplete")
    volume = next(
        (
            row for row in rolling_state.contract_volumes
            if row.checkpoint.contract_ticker == event.contract_ticker
        ),
        None,
    )
    if (
        volume is None
        or volume.checkpoint.cumulative_volume < event.cumulative_volume_after
        or event.session_date != reference_state.session_date
        or event.session_date != rolling_state.session_date
    ):
        raise ValueError("flow event does not match reference/rolling state")
    reasons = tuple(event.reasons)
    if BaselineKind(event.volume_baseline_kind) not in COMPLETE_BASELINES:
        reasons = (*reasons, "VOLUME_BASELINE_INCOMPLETE")
    identity = {
        "event_id": event.event_id,
        "event_revision": event.revision,
        "event_time": event.event_time,
        "source_flow_id": event.source_flow_id,
        "source_offset": event.source_offset,
        "contract_ticker": event.contract_ticker,
        "underlyer": event.underlyer,
        "prior_open_interest": event.prior_open_interest,
        "cumulative_volume_before": event.cumulative_volume_before,
        "cumulative_volume_after": event.cumulative_volume_after,
        "baseline_kind": BaselineKind(event.volume_baseline_kind),
        "volume_exceeds_prior_oi": event.volume_exceeds_prior_oi,
        "premium": event.premium,
        "conditions": event.conditions,
        "side_class": event.side_class,
        "side_confidence": event.side_confidence,
        "spread_position": event.spread_position,
        "quote_bid": event.quote_bid,
        "quote_ask": event.quote_ask,
        "quote_bid_size": event.quote_bid_size,
        "quote_ask_size": event.quote_ask_size,
        "quote_time": event.quote_time,
        "quote_seq": event.quote_seq,
        "rolling_state_id": rolling_state.state_id,
        "rolling_state_sha256": rolling_state.state_sha256,
        "flow_checkpoint_sha256": rolling_state.flow_checkpoint_sha256,
        "reference_state_id": reference_state.state_id,
        "reference_state_sha256": reference_state.state_sha256,
        "reason_codes": _reasons(reasons),
    }
    digest = canonical_sha256(identity)
    return AdvancedOptionsFlowObservation(
        observation_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-flow-observation:{digest}",
        ),
        observation_sha256=digest,
        **identity,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsClusterObservation:
    observation_id: UUID
    observation_sha256: str
    cluster_id: UUID
    revision: int
    window_seconds: int
    hop_milliseconds: int
    side_bucket: str
    event_ids: tuple[UUID, ...]
    source_offsets: tuple[int, ...]
    exchange_ids: tuple[int, ...]
    policy_sha256: str
    watermark: datetime
    status: str
    member_event_keys: tuple[tuple[UUID, datetime], ...]
    rolling_state_id: UUID
    rolling_state_sha256: str

    def __post_init__(self) -> None:
        watermark = _utc(self.watermark, "watermark")
        event_ids = tuple(sorted(self.event_ids, key=str))
        offsets = tuple(sorted(self.source_offsets))
        exchanges = tuple(sorted(self.exchange_ids))
        keys = tuple(sorted(
            (
                (event_id, _utc(event_time, "member event time"))
                for event_id, event_time in self.member_event_keys
            ),
            key=lambda row: (row[1], str(row[0])),
        ))
        if (
            not isinstance(self.cluster_id, UUID)
            or not isinstance(self.rolling_state_id, UUID)
            or self.revision <= 0
            or self.window_seconds <= 0
            or self.hop_milliseconds != self.window_seconds * 500
            or not event_ids
            or len(event_ids) != len(set(event_ids))
            or len(offsets) != len(event_ids)
            or len(exchanges) != len(set(exchanges))
            or {row[0] for row in keys} != set(event_ids)
        ):
            raise ValueError("cluster observation membership is invalid")
        object.__setattr__(self, "event_ids", event_ids)
        object.__setattr__(self, "source_offsets", offsets)
        object.__setattr__(self, "exchange_ids", exchanges)
        object.__setattr__(self, "member_event_keys", keys)
        object.__setattr__(self, "watermark", watermark)
        for name in ("policy_sha256", "rolling_state_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected_sha256 = canonical_sha256(self.identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-cluster-observation:{expected_sha256}",
        )
        if (
            self.observation_sha256 != expected_sha256
            or self.observation_id != expected_id
        ):
            raise ValueError("cluster observation ID or SHA changed")

    @property
    def identity(self) -> dict[str, object]:
        return {
            "cluster_id": self.cluster_id,
            "revision": self.revision,
            "window_seconds": self.window_seconds,
            "hop_milliseconds": self.hop_milliseconds,
            "side_bucket": self.side_bucket,
            "event_ids": self.event_ids,
            "source_offsets": self.source_offsets,
            "exchange_ids": self.exchange_ids,
            "policy_sha256": self.policy_sha256,
            "watermark": self.watermark,
            "status": self.status,
            "member_event_keys": self.member_event_keys,
            "rolling_state_id": self.rolling_state_id,
            "rolling_state_sha256": self.rolling_state_sha256,
        }


def build_cluster_observation(
    cluster: ClusterRow,
    *,
    events: Mapping[UUID, FlowEvent],
    watermark: datetime,
    rolling_state: AdvancedOptionsRollingState,
) -> AdvancedOptionsClusterObservation:
    members = tuple(events[event_id] for event_id in cluster.member_event_ids)
    if (
        not members
        or set(cluster.member_event_ids) != {row.event_id for row in members}
        or any(row.source_offset is None for row in members)
        or len({row.exchange_id for row in members if row.exchange_id is not None})
        != cluster.distinct_exchanges
        or cluster.status == "FINAL"
        and _utc(watermark, "watermark") < cluster.window_end
    ):
        raise ValueError("cluster members, exchanges or watermark changed")
    identity = {
        "cluster_id": cluster.cluster_id,
        "revision": cluster.revision,
        "window_seconds": cluster.window_seconds,
        "hop_milliseconds": cluster.window_seconds * 500,
        "side_bucket": cluster.side_class,
        "event_ids": tuple(row.event_id for row in members),
        "source_offsets": tuple(row.source_offset for row in members),
        "exchange_ids": tuple(sorted({
            row.exchange_id for row in members
            if row.exchange_id is not None
        })),
        "policy_sha256": cluster.policy_sha256,
        "watermark": _utc(watermark, "watermark"),
        "status": cluster.status,
        "member_event_keys": cluster.member_event_keys,
        "rolling_state_id": rolling_state.state_id,
        "rolling_state_sha256": rolling_state.state_sha256,
    }
    digest = canonical_sha256(identity)
    return AdvancedOptionsClusterObservation(
        observation_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-cluster-observation:{digest}",
        ),
        observation_sha256=digest,
        **identity,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsThresholdEvidence:
    minimum_cumulative_volume: int
    observed_cumulative_volume: int
    minimum_volume_oi_ratio: Decimal
    observed_volume_oi_ratio: Decimal | None
    minimum_premium: Decimal
    observed_premium: Decimal
    accepted_side_classes: tuple[str, ...]
    accepted_confidences: tuple[str, ...]
    observed_side_class: str
    observed_confidence: str
    minimum_cluster_prints: int
    observed_cluster_prints: int
    minimum_distinct_exchanges: int
    observed_distinct_exchanges: int
    minimum_cluster_premium: Decimal
    observed_cluster_premium: Decimal
    liquidity_policy_sha256: str
    liquidity_evidence_sha256: str
    liquidity_passed: bool
    maximum_freshness_ms: int
    observed_freshness_ms: int
    completeness: BaselineKind
    passed: bool

    def __post_init__(self) -> None:
        for name in ("liquidity_policy_sha256", "liquidity_evidence_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        numeric = (
            self.minimum_cumulative_volume,
            self.observed_cumulative_volume,
            self.minimum_cluster_prints,
            self.observed_cluster_prints,
            self.minimum_distinct_exchanges,
            self.observed_distinct_exchanges,
            self.maximum_freshness_ms,
            self.observed_freshness_ms,
        )
        if any(type(value) is not int or value < 0 for value in numeric):
            raise ValueError("detector threshold counts are invalid")
        if any(
            not value.is_finite() or value < 0
            for value in (
                self.minimum_volume_oi_ratio,
                self.minimum_premium,
                self.observed_premium,
                self.minimum_cluster_premium,
                self.observed_cluster_premium,
            )
        ) or (
            self.observed_volume_oi_ratio is not None
            and (
                not self.observed_volume_oi_ratio.is_finite()
                or self.observed_volume_oi_ratio < 0
            )
        ):
            raise ValueError("detector threshold values are invalid")
        actual_pass = (
            self.observed_cumulative_volume >= self.minimum_cumulative_volume
            and self.observed_volume_oi_ratio is not None
            and self.observed_volume_oi_ratio >= self.minimum_volume_oi_ratio
            and self.observed_premium >= self.minimum_premium
            and self.observed_side_class in self.accepted_side_classes
            and self.observed_confidence in self.accepted_confidences
            and self.observed_cluster_prints >= self.minimum_cluster_prints
            and self.observed_distinct_exchanges
            >= self.minimum_distinct_exchanges
            and self.observed_cluster_premium
            >= self.minimum_cluster_premium
            and self.liquidity_passed
            and self.observed_freshness_ms <= self.maximum_freshness_ms
            and self.completeness in COMPLETE_BASELINES
        )
        if self.passed != actual_pass:
            raise ValueError("detector threshold verdict changed")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPackageLegEvidence:
    leg_index: int
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    mark_evidence_id: UUID
    open_interest_sha256: str
    volume_state_sha256: str
    rate_observation_ids: tuple[UUID, ...]
    rate_payload_sha256: tuple[str, ...]
    dividend_coverage_sha256: str
    hv20_sha256: str
    hv30_sha256: str
    iv_evidence_sha256: str | None
    iv_reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        reasons = _reasons(self.iv_reason_codes)
        if (
            type(self.leg_index) is not int
            or self.leg_index < 0
            or min(
                self.instrument_id,
                self.contract_id,
                self.catalog_version_id,
            ) <= 0
            or not isinstance(self.mark_evidence_id, UUID)
            or not self.rate_observation_ids
            or len(self.rate_observation_ids) != len(self.rate_payload_sha256)
        ):
            raise ValueError("package leg evidence identity is invalid")
        for name in (
            "open_interest_sha256",
            "volume_state_sha256",
            "dividend_coverage_sha256",
            "hv20_sha256",
            "hv30_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        object.__setattr__(
            self,
            "rate_payload_sha256",
            tuple(
                _sha256(value, "rate_payload_sha256")
                for value in self.rate_payload_sha256
            ),
        )
        if self.iv_evidence_sha256 is None:
            if not reasons:
                raise ValueError("unavailable IV evidence needs reasons")
        elif reasons:
            raise ValueError("available IV evidence cannot carry reasons")
        else:
            object.__setattr__(
                self,
                "iv_evidence_sha256",
                _sha256(self.iv_evidence_sha256, "iv_evidence_sha256"),
            )
        object.__setattr__(self, "iv_reason_codes", reasons)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsFlowPackageEvidence:
    evidence_id: UUID
    evidence_sha256: str
    version: str
    dataset_id: UUID
    evaluation_id: UUID
    selection_status: str
    detector: str
    underlyer: str
    flow_observations: tuple[AdvancedOptionsFlowObservation, ...]
    cluster_observations: tuple[AdvancedOptionsClusterObservation, ...]
    threshold_evidence: AdvancedOptionsThresholdEvidence
    reference_state_id: UUID
    reference_state_sha256: str
    rolling_state_id: UUID
    rolling_state_sha256: str
    package: RealtimeOptionPackage | None
    package_mark: RealtimeOptionPackageMark | None
    package_leg_evidence: tuple[AdvancedOptionsPackageLegEvidence, ...]
    equity_spot_sha256: str | None
    model_version: str | None
    model_bundle_sha256: str | None
    valuation_policy_sha256: str | None
    degraded_reason_codes: tuple[str, ...]
    publication_authorized: bool = False
    delivery_authorized: bool = False
    paper_fill_authorized: bool = False
    execution_authorized: bool = False

    def __post_init__(self) -> None:
        flows = tuple(sorted(
            self.flow_observations,
            key=lambda row: (row.event_time, str(row.event_id)),
        ))
        clusters = tuple(sorted(
            self.cluster_observations,
            key=lambda row: (row.window_seconds, str(row.cluster_id)),
        ))
        reasons = _reasons(self.degraded_reason_codes)
        status = self.selection_status.strip().upper()
        leg_evidence = tuple(sorted(
            self.package_leg_evidence,
            key=lambda row: row.leg_index,
        ))
        if (
            self.version != FLOW_PACKAGE_EVIDENCE_VERSION
            or not flows
            or len({row.observation_id for row in flows}) != len(flows)
            or len({row.observation_id for row in clusters}) != len(clusters)
            or any(row.reference_state_id != self.reference_state_id for row in flows)
            or any(row.rolling_state_id != self.rolling_state_id for row in flows)
            or any(row.rolling_state_id != self.rolling_state_id for row in clusters)
            or any((
                self.publication_authorized,
                self.delivery_authorized,
                self.paper_fill_authorized,
                self.execution_authorized,
            ))
        ):
            raise ValueError("flow-package evidence scope or authority is invalid")
        if status == "SELECTED":
            if (
                self.package is None
                or self.package_mark is None
                or self.package_mark.package != self.package
                or not self.threshold_evidence.passed
                or tuple(row.leg_index for row in self.package.legs)
                != tuple(range(len(self.package.legs)))
                or self.model_version is None
                or self.model_bundle_sha256 is None
                or self.valuation_policy_sha256 is None
                or self.equity_spot_sha256 is None
                or tuple(row.leg_index for row in leg_evidence)
                != tuple(range(len(self.package.legs)))
                or tuple(row.instrument_id for row in leg_evidence)
                != tuple(row.instrument_id for row in self.package.legs)
                or tuple(row.mark_evidence_id for row in leg_evidence)
                != tuple(
                    row.evidence_id
                    for row in sorted(
                        self.package_mark.mark_evidence,
                        key=lambda row: next(
                            leg.leg_index
                            for leg in self.package.legs
                            if leg.instrument_id == row.instrument_id
                        ),
                    )
                )
            ):
                raise ValueError("selected package evidence is incomplete")
            object.__setattr__(
                self,
                "equity_spot_sha256",
                _sha256(self.equity_spot_sha256, "equity_spot_sha256"),
            )
            for name in (
                "model_bundle_sha256",
                "valuation_policy_sha256",
            ):
                object.__setattr__(
                    self,
                    name,
                    _sha256(getattr(self, name), name),
                )
        elif (
            self.package is not None
            or self.package_mark is not None
            or self.model_version is not None
            or self.model_bundle_sha256 is not None
            or self.valuation_policy_sha256 is not None
            or leg_evidence
            or self.equity_spot_sha256 is not None
        ):
            raise ValueError(
                "observation without coherent ordered legs cannot be a package"
            )
        object.__setattr__(self, "selection_status", status)
        object.__setattr__(self, "flow_observations", flows)
        object.__setattr__(self, "cluster_observations", clusters)
        object.__setattr__(self, "package_leg_evidence", leg_evidence)
        object.__setattr__(self, "degraded_reason_codes", reasons)
        for name in ("reference_state_sha256", "rolling_state_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected_sha256 = canonical_sha256(self.identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-flow-package:{expected_sha256}",
        )
        if self.evidence_sha256 != expected_sha256 or self.evidence_id != expected_id:
            raise ValueError("flow-package evidence ID or SHA changed")

    @property
    def identity(self) -> dict[str, object]:
        return {
            "contract": FLOW_PACKAGE_EVIDENCE_VERSION,
            "dataset_id": self.dataset_id,
            "evaluation_id": self.evaluation_id,
            "selection_status": self.selection_status,
            "detector": self.detector,
            "underlyer": self.underlyer,
            "flow_observations": self.flow_observations,
            "cluster_observations": self.cluster_observations,
            "threshold_evidence": self.threshold_evidence,
            "reference_state_id": self.reference_state_id,
            "reference_state_sha256": self.reference_state_sha256,
            "rolling_state_id": self.rolling_state_id,
            "rolling_state_sha256": self.rolling_state_sha256,
            "package": self.package,
            "package_mark": self.package_mark,
            "package_leg_evidence": self.package_leg_evidence,
            "equity_spot_sha256": self.equity_spot_sha256,
            "model_version": self.model_version,
            "model_bundle_sha256": self.model_bundle_sha256,
            "valuation_policy_sha256": self.valuation_policy_sha256,
            "degraded_reason_codes": self.degraded_reason_codes,
            "publication_authorized": False,
            "delivery_authorized": False,
            "paper_fill_authorized": False,
            "execution_authorized": False,
        }


def build_flow_package_evidence(
    evaluation: AdvancedOptionsDetectorEvaluation,
    *,
    flow_observations: tuple[AdvancedOptionsFlowObservation, ...],
    cluster_observations: tuple[AdvancedOptionsClusterObservation, ...],
    threshold_evidence: AdvancedOptionsThresholdEvidence,
    reference_state: AdvancedOptionsReferenceState,
    rolling_state: AdvancedOptionsRollingState,
    package_mark: RealtimeOptionPackageMark | None = None,
    model_version: str | None = None,
    model_bundle_sha256: str | None = None,
    valuation_policy_sha256: str | None = None,
    degraded_reason_codes: tuple[str, ...] = (),
) -> AdvancedOptionsFlowPackageEvidence:
    package = evaluation.package
    if package is not None and package.event_id not in {
        row.event_id for row in flow_observations
    }:
        raise ValueError("selected package does not bind a flow event")
    leg_evidence: tuple[AdvancedOptionsPackageLegEvidence, ...] = ()
    equity_spot_sha256 = None
    if package is not None and package_mark is not None:
        mark_by_instrument = {
            row.instrument_id: row for row in package_mark.mark_evidence
        }
        spot = next(
            (
                row for row in rolling_state.underlying_ticks
                if row.ticker == evaluation.underlyer
            ),
            None,
        )
        if spot is None:
            raise ValueError("selected package Equity spot is unavailable")
        equity_spot_sha256 = spot.sha256
        rows = []
        for leg in package.legs:
            oi = next(
                (
                    row for row in reference_state.open_interest
                    if row.contract_id == leg.contract_id
                ),
                None,
            )
            volume = next(
                (
                    row for row in rolling_state.contract_volumes
                    if row.checkpoint.contract_ticker == leg.contract_ticker
                ),
                None,
            )
            valuation = next(
                (
                    row for row in reference_state.valuation_inputs
                    if row.ticker == evaluation.underlyer
                    and row.expiration_date == leg.expiration_date
                ),
                None,
            )
            hv = {
                row.window_sessions: row
                for row in reference_state.historical_volatility
                if row.ticker == evaluation.underlyer
            }
            mark = mark_by_instrument.get(leg.instrument_id)
            if (
                oi is None
                or volume is None
                or valuation is None
                or set(hv) != {20, 30}
                or mark is None
            ):
                raise ValueError("selected package leg evidence is incomplete")
            rows.append(AdvancedOptionsPackageLegEvidence(
                leg_index=leg.leg_index,
                instrument_id=leg.instrument_id,
                contract_id=leg.contract_id,
                catalog_version_id=leg.catalog_version_id,
                contract_ticker=leg.contract_ticker,
                mark_evidence_id=mark.evidence_id,
                open_interest_sha256=oi.sha256,
                volume_state_sha256=volume.sha256,
                rate_observation_ids=tuple(
                    row.observation_id
                    for row in valuation.rate_observations
                ),
                rate_payload_sha256=tuple(
                    row.payload_sha256
                    for row in valuation.rate_observations
                ),
                dividend_coverage_sha256=(
                    valuation.dividend_coverage.sha256
                ),
                hv20_sha256=hv[20].sha256,
                hv30_sha256=hv[30].sha256,
                iv_evidence_sha256=rolling_state.iv_surface.evidence_sha256,
                iv_reason_codes=rolling_state.iv_surface.reason_codes,
            ))
        leg_evidence = tuple(rows)
    identity = {
        "contract": FLOW_PACKAGE_EVIDENCE_VERSION,
        "dataset_id": evaluation.dataset_id,
        "evaluation_id": evaluation.evaluation_id,
        "selection_status": evaluation.selection_status,
        "detector": evaluation.detector.value,
        "underlyer": evaluation.underlyer,
        "flow_observations": flow_observations,
        "cluster_observations": cluster_observations,
        "threshold_evidence": threshold_evidence,
        "reference_state_id": reference_state.state_id,
        "reference_state_sha256": reference_state.state_sha256,
        "rolling_state_id": rolling_state.state_id,
        "rolling_state_sha256": rolling_state.state_sha256,
        "package": package,
        "package_mark": package_mark,
        "package_leg_evidence": leg_evidence,
        "equity_spot_sha256": equity_spot_sha256,
        "model_version": model_version,
        "model_bundle_sha256": model_bundle_sha256,
        "valuation_policy_sha256": valuation_policy_sha256,
        "degraded_reason_codes": _reasons(degraded_reason_codes),
        "publication_authorized": False,
        "delivery_authorized": False,
        "paper_fill_authorized": False,
        "execution_authorized": False,
    }
    digest = canonical_sha256(identity)
    return AdvancedOptionsFlowPackageEvidence(
        evidence_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-flow-package:{digest}",
        ),
        evidence_sha256=digest,
        version=FLOW_PACKAGE_EVIDENCE_VERSION,
        dataset_id=evaluation.dataset_id,
        evaluation_id=evaluation.evaluation_id,
        selection_status=evaluation.selection_status,
        detector=evaluation.detector.value,
        underlyer=evaluation.underlyer,
        flow_observations=flow_observations,
        cluster_observations=cluster_observations,
        threshold_evidence=threshold_evidence,
        reference_state_id=reference_state.state_id,
        reference_state_sha256=reference_state.state_sha256,
        rolling_state_id=rolling_state.state_id,
        rolling_state_sha256=rolling_state.state_sha256,
        package=package,
        package_mark=package_mark,
        package_leg_evidence=leg_evidence,
        equity_spot_sha256=equity_spot_sha256,
        model_version=model_version,
        model_bundle_sha256=model_bundle_sha256,
        valuation_policy_sha256=valuation_policy_sha256,
        degraded_reason_codes=degraded_reason_codes,
    )


def persist_flow_package_evidence_cursor(
    cursor,
    evidence: AdvancedOptionsFlowPackageEvidence,
) -> None:
    document = json.loads(canonical_json(evidence.identity))
    cursor.execute(
        "INSERT INTO rt.advanced_options_flow_package_evidence ("
        "evidence_id,evidence_sha256,evidence_version,dataset_id,evaluation_id,"
        "candidate_id,selection_status,reference_state_id,rolling_state_id,"
        "evidence_document) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(evidence.evidence_id),
            evidence.evidence_sha256,
            evidence.version,
            str(evidence.dataset_id),
            str(evidence.evaluation_id),
            (
                None
                if evidence.package is None
                else str(evidence.package.candidate_id)
            ),
            evidence.selection_status,
            str(evidence.reference_state_id),
            str(evidence.rolling_state_id),
            Json(document),
        ),
    )
    cursor.execute(
        "SELECT evidence_id,evidence_sha256,evidence_document FROM "
        "rt.advanced_options_flow_package_evidence WHERE evaluation_id=%s",
        (str(evidence.evaluation_id),),
    )
    stored = cursor.fetchone()
    if (
        stored is None
        or UUID(str(stored[0])) != evidence.evidence_id
        or str(stored[1]) != evidence.evidence_sha256
        or canonical_json(stored[2]) != canonical_json(document)
    ):
        raise ValueError("persisted flow-package evidence conflicts")


@dataclass(frozen=True, slots=True)
class AdvancedOptionsFlowPackageEvidenceBatch:
    dataset_id: UUID
    cycle_sha256: str
    evidences: tuple[AdvancedOptionsFlowPackageEvidence, ...]
    batch_sha256: str
    publication_authorized: bool = False
    delivery_authorized: bool = False
    execution_authorized: bool = False

    def __post_init__(self) -> None:
        rows = tuple(sorted(
            self.evidences,
            key=lambda row: str(row.evaluation_id),
        ))
        if (
            not rows
            or any(row.dataset_id != self.dataset_id for row in rows)
            or len({row.evaluation_id for row in rows}) != len(rows)
            or any((
                self.publication_authorized,
                self.delivery_authorized,
                self.execution_authorized,
            ))
        ):
            raise ValueError("flow-package evidence batch is invalid")
        object.__setattr__(
            self,
            "cycle_sha256",
            _sha256(self.cycle_sha256, "cycle_sha256"),
        )
        object.__setattr__(self, "evidences", rows)
        expected = canonical_sha256({
            "contract": "advanced-options-flow-package-evidence-batch-v1",
            "dataset_id": self.dataset_id,
            "cycle_sha256": self.cycle_sha256,
            "evidence_sha256": tuple(row.evidence_sha256 for row in rows),
            "publication_authorized": False,
            "delivery_authorized": False,
            "execution_authorized": False,
        })
        if self.batch_sha256 != expected:
            raise ValueError("flow-package evidence batch SHA changed")


def build_flow_package_evidence_batch(
    cycle: AdvancedOptionsCycleResult,
    evidences: tuple[AdvancedOptionsFlowPackageEvidence, ...],
) -> AdvancedOptionsFlowPackageEvidenceBatch:
    expected = {row.evaluation_id for row in cycle.evaluations}
    actual = {row.evaluation_id for row in evidences}
    if expected != actual:
        raise ValueError("evidence batch does not cover exact evaluations")
    selected = {
        row.evaluation_id
        for row in cycle.evaluations
        if row.selection_status == "SELECTED"
    }
    selected_evidence = {
        row.evaluation_id
        for row in evidences
        if row.selection_status == "SELECTED" and row.package is not None
    }
    if selected != selected_evidence:
        raise ValueError("selected evaluation package evidence is incomplete")
    rows = tuple(sorted(evidences, key=lambda row: str(row.evaluation_id)))
    digest = canonical_sha256({
        "contract": "advanced-options-flow-package-evidence-batch-v1",
        "dataset_id": cycle.dataset_id,
        "cycle_sha256": cycle.cycle_sha256,
        "evidence_sha256": tuple(row.evidence_sha256 for row in rows),
        "publication_authorized": False,
        "delivery_authorized": False,
        "execution_authorized": False,
    })
    return AdvancedOptionsFlowPackageEvidenceBatch(
        dataset_id=cycle.dataset_id,
        cycle_sha256=cycle.cycle_sha256,
        evidences=rows,
        batch_sha256=digest,
    )


def persist_advanced_options_cycle_with_evidence_cursor(
    cursor,
    *,
    cycle: AdvancedOptionsCycleResult,
    control_plane,
    evidence_batch: AdvancedOptionsFlowPackageEvidenceBatch,
    flow_identity=None,
) -> None:
    if (
        evidence_batch.dataset_id != cycle.dataset_id
        or evidence_batch.cycle_sha256 != cycle.cycle_sha256
        or {row.evaluation_id for row in evidence_batch.evidences}
        != {row.evaluation_id for row in cycle.evaluations}
    ):
        raise ValueError("cycle evidence batch identity changed")
    persist_advanced_options_cycle_cursor(
        cursor,
        cycle,
        control_plane=control_plane,
        flow_identity=flow_identity,
    )
    for evidence in evidence_batch.evidences:
        persist_flow_package_evidence_cursor(cursor, evidence)
