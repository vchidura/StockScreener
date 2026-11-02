"""Canonical PostgreSQL producer for one Advanced Options rolling generation."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2 import Error as PsycopgError
from psycopg2.extras import RealDictCursor

from .advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
    AdvancedOptionsEnrollmentPlan,
    advanced_options_enrollment_plan_payload,
)
from .advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    flow_contract_scope_matches_data_membership,
)
from .advanced_options_reference_repository import (
    decode_advanced_options_reference_state,
)
from .advanced_options_reference_state import (
    AdvancedOptionsReferenceState,
    REFERENCE_STATE_VERSION_V1,
)
from .advanced_options_rolling_repository import (
    persist_advanced_options_rolling_state_cursor,
)
from .advanced_options_rolling_state import (
    CANONICAL_ORDER_WINDOWS_SECONDS,
    AdvancedOptionsOrderWindowMember,
    AdvancedOptionsOrderWindowState,
    AdvancedOptionsRollingProducerBinding,
    AdvancedOptionsRollingState,
    AdvancedOptionsUnderlyingTick,
    RollingAvailability,
    build_advanced_options_rolling_state,
    build_underlying_tick,
    contract_volume_state,
)
from .advanced_options_underlying_snapshot import (
    UnderlyingSnapshotBinding,
    UnderlyingSnapshotTick,
    underlying_snapshot_binding_from_payload,
    underlying_snapshot_binding_payload,
)
from .contracts import AssetClass, canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .materialization import MaterializedQuote, MaterializedTrade
from .option_flow.records import VolumeCheckpoint
from .option_flow.volume import COMPLETE_BASELINES, BaselineKind
from .option_source_flow import (
    CANONICAL_OPTION_FLOW_OWNER,
    OptionSourceFlowIdentity,
)


ROLLING_PRODUCER_VERSION = "advanced-options-rolling-producer-v1"
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


class RollingProducerStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRollingProducerEvidence:
    member_kind: str
    member_key: str
    availability: RollingAvailability
    evidence_sha256: str
    observed_at: datetime | None = None
    known_at: datetime | None = None
    valid_until: datetime | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        kind = self.member_kind.strip().upper()
        key = self.member_key.strip()
        reasons = tuple(sorted({
            value.strip().upper()
            for value in self.reason_codes
            if value.strip()
        }))
        if not kind or not key:
            raise ValueError("rolling producer evidence identity is invalid")
        if (
            self.availability is RollingAvailability.READY
            and reasons
        ) or (
            self.availability is RollingAvailability.UNAVAILABLE
            and not reasons
        ):
            raise ValueError("rolling producer evidence availability is invalid")
        observed = (
            None
            if self.observed_at is None
            else _utc(self.observed_at, "observed_at")
        )
        known = (
            None
            if self.known_at is None
            else _utc(self.known_at, "known_at")
        )
        valid = (
            None
            if self.valid_until is None
            else _utc(self.valid_until, "valid_until")
        )
        if (
            observed is not None
            and known is not None
            and observed > known
        ) or (
            known is not None
            and valid is not None
            and known >= valid
        ):
            raise ValueError("rolling producer evidence clocks are not causal")
        object.__setattr__(self, "member_kind", kind)
        object.__setattr__(self, "member_key", key)
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsEquityLiveHeadBinding:
    cohort_id: UUID
    manifest_id: UUID
    manifest_sha256: str
    ingress_plan_id: UUID
    ingress_plan_sha256: str
    journal_generation: UUID
    stream_session_ids: tuple[UUID, ...]
    runtime_profile_sha256: str
    configuration_sha256: str
    runtime_policy_sha256: str
    journal_metadata_sha256: str
    shard_map_sha256: str
    session_date: date
    committed_offset: int
    observed_at: datetime
    valid_until: datetime
    binding_sha256: str = ""

    def __post_init__(self) -> None:
        sessions = tuple(sorted(set(self.stream_session_ids), key=str))
        observed = _utc(self.observed_at, "observed_at")
        valid = _utc(self.valid_until, "valid_until")
        for name in (
            "manifest_sha256",
            "ingress_plan_sha256",
            "runtime_profile_sha256",
            "configuration_sha256",
            "runtime_policy_sha256",
            "journal_metadata_sha256",
            "shard_map_sha256",
        ):
            object.__setattr__(
                self,
                name,
                _sha256(getattr(self, name), name),
            )
        if (
            any(value.int == 0 for value in (
                self.cohort_id,
                self.manifest_id,
                self.ingress_plan_id,
                self.journal_generation,
            ))
            or not sessions
            or any(value.int == 0 for value in sessions)
            or type(self.committed_offset) is not int
            or self.committed_offset <= 0
            or observed >= valid
            or not isinstance(self.session_date, date)
            or isinstance(self.session_date, datetime)
        ):
            raise ValueError("Advanced Equity live-head binding is invalid")
        object.__setattr__(self, "stream_session_ids", sessions)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "valid_until", valid)
        expected = canonical_sha256(
            advanced_options_equity_live_head_binding_payload(
                self,
                include_sha256=False,
            )
        )
        if self.binding_sha256 and self.binding_sha256 != expected:
            raise ValueError(
                "Advanced Equity live-head binding identity changed"
            )
        object.__setattr__(self, "binding_sha256", expected)


def advanced_options_equity_live_head_binding_payload(
    binding: AdvancedOptionsEquityLiveHeadBinding,
    *,
    include_sha256: bool = True,
) -> dict[str, object]:
    if not isinstance(binding, AdvancedOptionsEquityLiveHeadBinding):
        raise TypeError("Advanced Equity live-head binding is invalid")
    value = {
        "contract": "advanced-options-equity-live-head-binding-v1",
        "cohort_id": str(binding.cohort_id),
        "manifest_id": str(binding.manifest_id),
        "manifest_sha256": binding.manifest_sha256,
        "ingress_plan_id": str(binding.ingress_plan_id),
        "ingress_plan_sha256": binding.ingress_plan_sha256,
        "journal_generation": str(binding.journal_generation),
        "stream_session_ids": [
            str(value) for value in binding.stream_session_ids
        ],
        "runtime_profile_sha256": binding.runtime_profile_sha256,
        "configuration_sha256": binding.configuration_sha256,
        "runtime_policy_sha256": binding.runtime_policy_sha256,
        "journal_metadata_sha256": binding.journal_metadata_sha256,
        "shard_map_sha256": binding.shard_map_sha256,
        "session_date": binding.session_date.isoformat(),
        "committed_offset": binding.committed_offset,
        "observed_at": binding.observed_at.isoformat(),
        "valid_until": binding.valid_until.isoformat(),
    }
    if include_sha256:
        value["binding_sha256"] = binding.binding_sha256
    return value


def advanced_options_equity_live_head_binding_from_payload(
    value: object,
) -> AdvancedOptionsEquityLiveHeadBinding:
    if not isinstance(value, dict):
        raise TypeError("Advanced Equity live-head binding must be an object")
    return AdvancedOptionsEquityLiveHeadBinding(
        cohort_id=UUID(str(value["cohort_id"])),
        manifest_id=UUID(str(value["manifest_id"])),
        manifest_sha256=str(value["manifest_sha256"]),
        ingress_plan_id=UUID(str(value["ingress_plan_id"])),
        ingress_plan_sha256=str(value["ingress_plan_sha256"]),
        journal_generation=UUID(str(value["journal_generation"])),
        stream_session_ids=tuple(
            UUID(str(row)) for row in value["stream_session_ids"]
        ),
        runtime_profile_sha256=str(value["runtime_profile_sha256"]),
        configuration_sha256=str(value["configuration_sha256"]),
        runtime_policy_sha256=str(value["runtime_policy_sha256"]),
        journal_metadata_sha256=str(value["journal_metadata_sha256"]),
        shard_map_sha256=str(value["shard_map_sha256"]),
        session_date=date.fromisoformat(str(value["session_date"])),
        committed_offset=int(value["committed_offset"]),
        observed_at=datetime.fromisoformat(str(value["observed_at"])),
        valid_until=datetime.fromisoformat(str(value["valid_until"])),
        binding_sha256=str(value["binding_sha256"]),
    )


def advanced_options_underlying_price_binding_payload(
    binding: (
        AdvancedOptionsEquityLiveHeadBinding
        | UnderlyingSnapshotBinding
    ),
) -> dict[str, object]:
    if isinstance(binding, UnderlyingSnapshotBinding):
        return underlying_snapshot_binding_payload(binding)
    return advanced_options_equity_live_head_binding_payload(binding)


def advanced_options_underlying_price_binding_from_payload(
    value: object,
) -> (
    AdvancedOptionsEquityLiveHeadBinding
    | UnderlyingSnapshotBinding
):
    if (
        isinstance(value, dict)
        and value.get("contract")
        == "advanced-options-underlying-snapshot-binding-v1"
    ):
        return underlying_snapshot_binding_from_payload(value)
    return advanced_options_equity_live_head_binding_from_payload(value)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRollingProducerPlan:
    plan_id: UUID
    plan_sha256: str
    enrollment: AdvancedOptionsEnrollmentPlan
    contract_scope: AdvancedOptionsContractScope
    data_membership_sha256: str
    reference_state: AdvancedOptionsReferenceState
    source_flow: OptionSourceFlowIdentity
    owner_epoch: int
    market_state_committed_offset: int
    flow_committed_offset: int
    market_state_shard_ids: tuple[int, ...]
    flow_shard_ids: tuple[int, ...]
    market_state_shard_map_sha256: str
    flow_shard_map_sha256: str
    known_at: datetime
    valid_until: datetime
    maximum_underlying_age_ms: int
    maximum_option_spot_skew_ms: int
    maximum_cluster_members: int
    equity_live_head_binding: (
        AdvancedOptionsEquityLiveHeadBinding
        | UnderlyingSnapshotBinding
        | None
    ) = None
    provider_calls_authorized: bool = False
    runtime_actions_authorized: bool = False

    @property
    def control_plane(self):
        return self.enrollment.control_plane

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
class AdvancedOptionsRollingProducerPreflight:
    status: RollingProducerStatus
    plan_id: UUID
    preflight_sha256: str
    evidence: tuple[AdvancedOptionsRollingProducerEvidence, ...]
    state: AdvancedOptionsRollingState | None

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(sorted({
            reason
            for row in self.evidence
            for reason in row.reason_codes
        }))


class AdvancedOptionsRollingPreflightBlocked(ValueError):
    def __init__(
        self,
        result: AdvancedOptionsRollingProducerPreflight,
    ) -> None:
        self.result = result
        super().__init__(
            "Advanced Options rolling producer preflight is blocked: "
            + ",".join(result.reason_codes)
        )


def _plan_identity(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    contract_scope: AdvancedOptionsContractScope,
    data_membership_sha256: str,
    reference_state: AdvancedOptionsReferenceState,
    source_flow: OptionSourceFlowIdentity,
    owner_epoch: int,
    market_state_committed_offset: int,
    flow_committed_offset: int,
    market_state_shard_ids: tuple[int, ...],
    flow_shard_ids: tuple[int, ...],
    market_state_shard_map_sha256: str,
    flow_shard_map_sha256: str,
    known_at: datetime,
    valid_until: datetime,
    maximum_underlying_age_ms: int,
    maximum_option_spot_skew_ms: int,
    maximum_cluster_members: int,
    equity_live_head_binding: (
        AdvancedOptionsEquityLiveHeadBinding
        | UnderlyingSnapshotBinding
        | None
    ),
) -> dict[str, object]:
    control = enrollment.control_plane
    return {
        "contract": ROLLING_PRODUCER_VERSION,
        "enrollment_sha256": enrollment.enrollment_sha256,
        "cohort_id": control.cohort_id,
        "cohort_sha256": control.cohort_sha256,
        "manifest_id": control.manifest_id,
        "manifest_sha256": control.manifest_sha256,
        "profile_id": control.profile_id,
        "profile_sha256": control.profile_sha256,
        "slot_set_version": control.slot_set.version,
        "slot_set_sha256": control.slot_set_sha256,
        "slot_member_sha256": control.slot_set.member_sha256,
        "data_membership_sha256": data_membership_sha256,
        "contract_scope_sha256": contract_scope.sha256,
        "reference_state_id": reference_state.state_id,
        "reference_state_sha256": reference_state.state_sha256,
        "reference_generation": reference_state.reference_generation,
        "source_flow_id": source_flow.source_flow_id,
        "source_flow_sha256": source_flow.source_flow_sha256,
        "journal_generation": source_flow.journal_generation,
        "stream_session_ids": source_flow.stream_session_ids,
        "owner_epoch": owner_epoch,
        "market_state_committed_offset": (
            market_state_committed_offset
        ),
        "flow_committed_offset": flow_committed_offset,
        "market_state_shard_ids": market_state_shard_ids,
        "flow_shard_ids": flow_shard_ids,
        "market_state_shard_map_sha256": (
            market_state_shard_map_sha256
        ),
        "flow_shard_map_sha256": flow_shard_map_sha256,
        "known_at": known_at,
        "valid_until": valid_until,
        "maximum_underlying_age_ms": maximum_underlying_age_ms,
        "maximum_option_spot_skew_ms": maximum_option_spot_skew_ms,
        "maximum_cluster_members": maximum_cluster_members,
        "equity_live_head_binding": (
            None
            if equity_live_head_binding is None
            else advanced_options_underlying_price_binding_payload(
                equity_live_head_binding
            )
        ),
        "provider_calls_authorized": False,
        "runtime_actions_authorized": False,
    }


def _validate_plan(
    plan: AdvancedOptionsRollingProducerPlan,
) -> dict[str, object]:
    if not isinstance(plan, AdvancedOptionsRollingProducerPlan):
        raise TypeError("Advanced Options rolling producer plan is invalid")
    advanced_options_enrollment_plan_payload(plan.enrollment)
    control = plan.control_plane
    known = _utc(plan.known_at, "known_at")
    valid = _utc(plan.valid_until, "valid_until")
    market_state_shards = tuple(sorted(set(
        plan.market_state_shard_ids
    )))
    flow_shards = tuple(sorted(set(plan.flow_shard_ids)))
    if (
        plan.data_membership_sha256
        != plan.enrollment.activity_selection.selection_sha256
        or not flow_contract_scope_matches_data_membership(
            plan.contract_scope,
            plan.enrollment.activity_selection.contracts,
            session_date=control.session_date,
            manifest_sha256=control.manifest_sha256,
            slot_sha256=control.slot_set_sha256,
            flow_policy_sha256=control.pins.flow_policy_sha256,
        )
    ):
        raise ValueError("rolling producer contract scope changed")
    if (
        not isinstance(plan.reference_state, AdvancedOptionsReferenceState)
        or plan.reference_state.session_date != control.session_date
        or (
            plan.reference_state.data_membership_sha256
            != plan.data_membership_sha256
            and not (
                plan.reference_state.version
                == REFERENCE_STATE_VERSION_V1
                and plan.reference_state.data_membership_sha256 is None
                and tuple(plan.contract_scope.contract_map)
                == tuple(
                    row.contract_ticker
                    for row in plan.enrollment.activity_selection.contracts
                )
            )
        )
        or plan.reference_state.contract_scope_sha256
        != plan.contract_scope.sha256
        or plan.reference_state.underlyings
        != control.universe.option_underlyings
        or not (
            plan.reference_state.known_at
            <= known
            < plan.reference_state.valid_until
        )
        or valid > plan.reference_state.valid_until
    ):
        raise ValueError("rolling producer reference generation changed")
    if (
        not isinstance(plan.source_flow, OptionSourceFlowIdentity)
        or plan.source_flow.journal_generation
        != control.source_flow.journal_generation
        or plan.source_flow.stream_session_ids
        != control.source_flow.stream_session_ids
        or plan.source_flow.condition_semantics_sha256
        != control.pins.condition_semantics_sha256
        or (
            plan.source_flow.identity_version in (3, 4)
            and (
                plan.source_flow.advanced_flow_policy_sha256
                != plan.contract_scope.flow_policy_sha256
                or plan.source_flow.contract_scope_sha256
                != plan.contract_scope.sha256
            )
        )
    ):
        raise ValueError("rolling producer source-flow identity changed")
    if (
        type(plan.owner_epoch) is not int
        or plan.owner_epoch <= 0
        or type(plan.market_state_committed_offset) is not int
        or plan.market_state_committed_offset < 0
        or type(plan.flow_committed_offset) is not int
        or plan.flow_committed_offset < 0
        or not market_state_shards
        or market_state_shards != plan.market_state_shard_ids
        or market_state_shards
        != tuple(range(len(market_state_shards)))
        or not flow_shards
        or flow_shards != plan.flow_shard_ids
        or flow_shards != tuple(range(len(flow_shards)))
        or type(plan.maximum_underlying_age_ms) is not int
        or plan.maximum_underlying_age_ms <= 0
        or type(plan.maximum_option_spot_skew_ms) is not int
        or plan.maximum_option_spot_skew_ms < 0
        or type(plan.maximum_cluster_members) is not int
        or not 1 <= plan.maximum_cluster_members <= 100_000
        or not control.slot_set.effective_at <= known < valid
        or known > plan.enrollment.activity_selection.valid_until
        or valid > control.expires_at
        or plan.provider_calls_authorized
        or plan.runtime_actions_authorized
    ):
        raise ValueError("rolling producer clocks, bounds or authority changed")
    equity_binding = plan.equity_live_head_binding
    if isinstance(equity_binding, UnderlyingSnapshotBinding):
        if (
            tuple(row.ticker for row in equity_binding.prices)
            != control.universe.option_underlyings
        ):
            raise ValueError(
                "rolling underlying snapshot membership changed"
            )
        equity_binding.require_fresh(known)
    elif equity_binding is not None and (
        not isinstance(
            equity_binding,
            AdvancedOptionsEquityLiveHeadBinding,
        )
        or equity_binding.session_date != control.session_date
        or not equity_binding.observed_at <= known
        < equity_binding.valid_until
    ):
        raise ValueError(
            "rolling Advanced Equity live-head binding changed"
        )
    market_state_shard_map_sha256 = _sha256(
        plan.market_state_shard_map_sha256,
        "market_state_shard_map_sha256",
    )
    flow_shard_map_sha256 = _sha256(
        plan.flow_shard_map_sha256,
        "flow_shard_map_sha256",
    )
    identity = _plan_identity(
        enrollment=plan.enrollment,
        contract_scope=plan.contract_scope,
        data_membership_sha256=plan.data_membership_sha256,
        reference_state=plan.reference_state,
        source_flow=plan.source_flow,
        owner_epoch=plan.owner_epoch,
        market_state_committed_offset=(
            plan.market_state_committed_offset
        ),
        flow_committed_offset=plan.flow_committed_offset,
        market_state_shard_ids=market_state_shards,
        flow_shard_ids=flow_shards,
        market_state_shard_map_sha256=(
            market_state_shard_map_sha256
        ),
        flow_shard_map_sha256=flow_shard_map_sha256,
        known_at=known,
        valid_until=valid,
        maximum_underlying_age_ms=plan.maximum_underlying_age_ms,
        maximum_option_spot_skew_ms=(
            plan.maximum_option_spot_skew_ms
        ),
        maximum_cluster_members=plan.maximum_cluster_members,
        equity_live_head_binding=equity_binding,
    )
    expected_sha256 = canonical_sha256(identity)
    expected_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-rolling-producer:{expected_sha256}",
    )
    if (
        plan.plan_sha256 != expected_sha256
        or plan.plan_id != expected_id
    ):
        raise ValueError("rolling producer plan identity changed")
    return identity


def build_advanced_options_rolling_producer_plan(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    contract_scope: AdvancedOptionsContractScope,
    reference_state: AdvancedOptionsReferenceState,
    source_flow: OptionSourceFlowIdentity,
    owner_epoch: int,
    market_state_committed_offset: int,
    flow_committed_offset: int,
    market_state_shard_ids: tuple[int, ...],
    flow_shard_ids: tuple[int, ...],
    market_state_shard_map_sha256: str,
    flow_shard_map_sha256: str,
    known_at: datetime,
    valid_until: datetime,
    maximum_underlying_age_ms: int = 1_000,
    maximum_option_spot_skew_ms: int = 500,
    maximum_cluster_members: int = 10_000,
    equity_live_head_binding: (
        AdvancedOptionsEquityLiveHeadBinding
        | UnderlyingSnapshotBinding
        | None
    ) = None,
) -> AdvancedOptionsRollingProducerPlan:
    known = _utc(known_at, "known_at")
    valid = _utc(valid_until, "valid_until")
    data_membership_sha256 = (
        enrollment.activity_selection.selection_sha256
    )
    market_state_shards = tuple(market_state_shard_ids)
    flow_shards = tuple(flow_shard_ids)
    identity = _plan_identity(
        enrollment=enrollment,
        contract_scope=contract_scope,
        data_membership_sha256=data_membership_sha256,
        reference_state=reference_state,
        source_flow=source_flow,
        owner_epoch=owner_epoch,
        market_state_committed_offset=market_state_committed_offset,
        flow_committed_offset=flow_committed_offset,
        market_state_shard_ids=market_state_shards,
        flow_shard_ids=flow_shards,
        market_state_shard_map_sha256=(
            market_state_shard_map_sha256
        ),
        flow_shard_map_sha256=flow_shard_map_sha256,
        known_at=known,
        valid_until=valid,
        maximum_underlying_age_ms=maximum_underlying_age_ms,
        maximum_option_spot_skew_ms=maximum_option_spot_skew_ms,
        maximum_cluster_members=maximum_cluster_members,
        equity_live_head_binding=equity_live_head_binding,
    )
    digest = canonical_sha256(identity)
    plan = AdvancedOptionsRollingProducerPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-rolling-producer:{digest}",
        ),
        plan_sha256=digest,
        enrollment=enrollment,
        contract_scope=contract_scope,
        data_membership_sha256=data_membership_sha256,
        reference_state=reference_state,
        source_flow=source_flow,
        owner_epoch=owner_epoch,
        market_state_committed_offset=market_state_committed_offset,
        flow_committed_offset=flow_committed_offset,
        market_state_shard_ids=market_state_shards,
        flow_shard_ids=flow_shards,
        market_state_shard_map_sha256=(
            market_state_shard_map_sha256
        ),
        flow_shard_map_sha256=flow_shard_map_sha256,
        known_at=known,
        valid_until=valid,
        maximum_underlying_age_ms=maximum_underlying_age_ms,
        maximum_option_spot_skew_ms=maximum_option_spot_skew_ms,
        maximum_cluster_members=maximum_cluster_members,
        equity_live_head_binding=equity_live_head_binding,
    )
    _validate_plan(plan)
    return plan


def advanced_options_rolling_producer_plan_payload(
    plan: AdvancedOptionsRollingProducerPlan,
) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "plan_id": str(plan.plan_id),
        "plan_sha256": plan.plan_sha256,
        "identity": _validate_plan(plan),
        "effect_counts": plan.zero_effect_counts,
    }


def advanced_options_consumer_checkpoint_sha256(
    rows: tuple[Mapping[str, object], ...],
) -> str:
    return canonical_sha256(tuple(
        (
            str(row["consumer_name"]),
            str(row["journal_generation"]),
            int(row["shard_id"]),
            str(row["shard_map_sha256"]).strip(),
            int(row["owner_epoch"]),
            int(row["committed_offset"]),
            str(row["checkpoint_id"]),
            str(row["state_hash"]).strip(),
            row["committed_at"],
        )
        for row in rows
    ))


def advanced_options_unresolved_gaps_sha256(
    rows: tuple[Mapping[str, object], ...],
) -> str:
    return canonical_sha256(tuple(
        (
            str(row["gap_id"]),
            str(row["session_id"]),
            row["instrument_id"],
            row["channel"],
            row["gap_start"],
            row["gap_end"],
            str(row["journal_generation"]),
            int(row["last_durable_offset"]),
            str(row["reason"]),
            str(row["recovery_status"]),
        )
        for row in rows
    ))


def _evidence(
    kind: str,
    key: str,
    availability: RollingAvailability,
    value: object,
    *,
    observed_at: datetime | None = None,
    known_at: datetime | None = None,
    valid_until: datetime | None = None,
    reasons: tuple[str, ...] = (),
) -> AdvancedOptionsRollingProducerEvidence:
    return AdvancedOptionsRollingProducerEvidence(
        member_kind=kind,
        member_key=key,
        availability=availability,
        evidence_sha256=canonical_sha256(value),
        observed_at=observed_at,
        known_at=known_at,
        valid_until=valid_until,
        reason_codes=reasons,
    )


def _result(
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
    state: AdvancedOptionsRollingState | None,
) -> AdvancedOptionsRollingProducerPreflight:
    status = (
        RollingProducerStatus.READY
        if state is not None
        and all(
            row.availability is RollingAvailability.READY
            for row in evidence
        )
        else RollingProducerStatus.BLOCKED
    )
    identity = {
        "contract": ROLLING_PRODUCER_VERSION,
        "plan_id": plan.plan_id,
        "plan_sha256": plan.plan_sha256,
        "status": status.value,
        "evidence": tuple(evidence),
        "state_id": None if state is None else state.state_id,
        "state_sha256": None if state is None else state.state_sha256,
    }
    return AdvancedOptionsRollingProducerPreflight(
        status=status,
        plan_id=plan.plan_id,
        preflight_sha256=canonical_sha256(identity),
        evidence=tuple(evidence),
        state=state,
    )


def _active_scope(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
):
    control = plan.control_plane
    cursor.execute(
        "SELECT cohort.cohort_sha256,cohort.manifest_id::text,"
        "cohort.configuration_sha256,cohort.model_bundle_sha256,"
        "cohort.valuation_sha256,cohort.mode,"
        "manifest.manifest_sha256,manifest.universe_sha256,"
        "manifest.profile_id::text,manifest.manifest_kind,"
        "manifest.option_underlying_count,profile.profile_sha256,"
        "profile.provider_tier,current.version,current.activated_at,"
        "version.member_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "LEFT JOIN rt.slot_set_current AS current "
        "ON current.cohort_id=cohort.cohort_id "
        "LEFT JOIN rt.slot_set_versions AS version "
        "ON version.cohort_id=current.cohort_id "
        "AND version.version=current.version "
        "WHERE cohort.cohort_id=%s",
        (str(control.cohort_id),),
    )
    header = cursor.fetchone()
    expected = (
        control.cohort_sha256,
        str(control.manifest_id),
        (
            plan.enrollment.enrollment_sha256
            if control.stored_identity is None
            else control.stored_identity.configuration_sha256
        ),
        control.pins.model_bundle_sha256,
        control.pins.valuation_policy_sha256,
        "SHADOW" if control.stored_identity is None else control.stored_identity.mode,
        control.manifest_sha256,
        control.universe.sha256,
        str(control.profile_id),
        "MARKET_OPTIONS",
        len(control.universe.option_underlyings),
        control.profile_sha256,
        "MASSIVE_ADVANCED",
        control.slot_set.version,
        control.slot_set.effective_at,
        control.slot_set.member_sha256,
    )
    actual = None if header is None else tuple(header.values())
    if actual != expected:
        raise ValueError("stored rolling producer cohort or slot changed")
    evidence.append(_evidence(
        "CONTROL_PLANE",
        str(control.cohort_id),
        RollingAvailability.READY,
        actual,
        observed_at=control.slot_set.effective_at,
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    cursor.execute(
        "SELECT member.instrument_id,member.channel,member.purpose,"
        "member.priority,member.catalog_version_id,contract.contract_id,"
        "contract.contract_ticker,contract.underlying_security_id::text,"
        "version.expiration,version.eligibility,version.effective_from,"
        "version.effective_to,version.observed_at,version.payload_sha256 "
        "FROM rt.slot_set_current AS current "
        "JOIN rt.slot_set_members AS member "
        "ON member.cohort_id=current.cohort_id "
        "AND member.version=current.version "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=instrument.contract_id "
        "JOIN ref.option_contract_versions AS version "
        "ON version.version_id=member.catalog_version_id "
        "AND version.contract_id=contract.contract_id "
        "WHERE current.cohort_id=%s "
        "ORDER BY contract.contract_ticker,member.channel",
        (str(control.cohort_id),),
    )
    rows = tuple(cursor.fetchall())
    selected = {
        row.contract_ticker: row
        for row in plan.enrollment.activity_selection.contracts
    }
    grouped: dict[str, list] = {}
    for row in rows:
        grouped.setdefault(str(row["contract_ticker"]), []).append(row)
    if set(grouped) != set(selected):
        raise ValueError("active rolling contract membership changed")
    contracts = []
    for ticker in sorted(grouped):
        members = grouped[ticker]
        selected_row = selected[ticker]
        first = members[0]
        if (
            len(members) != 2
            or {str(row["channel"]) for row in members} != {"T", "Q"}
            or {str(row["purpose"]) for row in members} != {"PRIMARY"}
            or any(
                int(row["instrument_id"]) != selected_row.instrument_id
                or int(row["contract_id"]) != selected_row.contract_id
                or int(row["catalog_version_id"])
                != selected_row.catalog_version_id
                for row in members
            )
            or str(first["eligibility"]) != "VALIDATED_ACTIVE"
            or first["effective_from"] > plan.known_at
            or (
                first["effective_to"] is not None
                and first["effective_to"] <= plan.known_at
            )
            or first["observed_at"] > plan.known_at
        ):
            raise ValueError("active rolling contract identity changed")
        contracts.append(first)
    evidence.append(_evidence(
        "ACTIVE_CONTRACTS",
        str(control.cohort_id),
        RollingAvailability.READY,
        tuple(tuple(row.values()) for row in rows),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    security_by_ticker = dict(plan.enrollment.equity_plan.security_ids)
    expected_equities = tuple(
        (
            ticker,
            str(security_by_ticker[ticker]),
        )
        for ticker in control.universe.option_underlyings
    )
    cursor.execute(
        "SELECT revision.ticker,instrument.security_id::text,"
        "instrument.instrument_id "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN LATERAL ("
        "SELECT ticker FROM ref.security_revisions "
        "WHERE security_id=instrument.security_id "
        "AND effective_from<=%s ORDER BY effective_from DESC,"
        "observed_at DESC LIMIT 1"
        ") AS revision ON TRUE "
        "WHERE member.manifest_id=%s "
        "AND instrument.security_id=ANY(%s::uuid[]) "
        "ORDER BY revision.ticker",
        (
            plan.known_at,
            str(plan.enrollment.equity_plan.manifest_id),
            [value for _, value in expected_equities],
        ),
    )
    equities = tuple(cursor.fetchall())
    if tuple(
        (str(row["ticker"]), str(row["security_id"]))
        for row in equities
    ) != expected_equities:
        raise ValueError("rolling underlying Equity membership changed")
    return tuple(contracts), equities


def _reference(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> None:
    cursor.execute(
        "SELECT state_id::text,state_sha256,state_document "
        "FROM rt.advanced_options_reference_states "
        "WHERE reference_generation=%s",
        (plan.reference_state.reference_generation,),
    )
    row = cursor.fetchone()
    if row is None:
        evidence.append(_evidence(
            "REFERENCE_GENERATION",
            str(plan.reference_state.reference_generation),
            RollingAvailability.UNAVAILABLE,
            ("REFERENCE_GENERATION_ABSENT", plan.reference_state.state_sha256),
            known_at=plan.known_at,
            valid_until=plan.valid_until,
            reasons=("REFERENCE_GENERATION_ABSENT",),
        ))
        return
    stored = decode_advanced_options_reference_state(
        state_id=UUID(str(row["state_id"])),
        state_sha256=str(row["state_sha256"]),
        document=row["state_document"],
    )
    if stored != plan.reference_state:
        raise ValueError("stored rolling reference generation diverged")
    evidence.append(_evidence(
        "REFERENCE_GENERATION",
        str(stored.reference_generation),
        RollingAvailability.READY,
        (stored.state_id, stored.state_sha256),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))


def _source_flow(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> None:
    source = plan.source_flow
    cursor.execute(
        "SELECT source_flow_id::text,source_flow_sha256,"
        "source_cohort_id::text,journal_generation::text,"
        "stream_session_ids::text[],owner_name,"
        "condition_semantics_sha256,flow_policy_sha256,"
        "identity_version,advanced_flow_policy_sha256,"
        "contract_scope_sha256,provider_contract_sha256,"
        "condition_reference_sha256,exchange_reference_sha256 "
        "FROM rt.option_source_flows WHERE source_flow_id=%s",
        (str(source.source_flow_id),),
    )
    row = cursor.fetchone()
    if row is None:
        evidence.append(_evidence(
            "SOURCE_FLOW",
            str(source.source_flow_id),
            RollingAvailability.UNAVAILABLE,
            ("SOURCE_FLOW_IDENTITY_ABSENT", source.source_flow_sha256),
            known_at=plan.known_at,
            valid_until=plan.valid_until,
            reasons=("SOURCE_FLOW_IDENTITY_ABSENT",),
        ))
        return
    expected = (
        str(source.source_flow_id),
        source.source_flow_sha256,
        str(plan.control_plane.cohort_id),
        str(source.journal_generation),
        [str(value) for value in source.stream_session_ids],
        source.owner_name,
        source.condition_semantics_sha256,
        source.flow_policy_sha256,
        source.identity_version,
        source.advanced_flow_policy_sha256,
        source.contract_scope_sha256,
        source.provider_contract_sha256,
        source.condition_reference_sha256,
        source.exchange_reference_sha256,
    )
    actual = tuple(row.values())
    actual = (
        *actual[:4],
        [str(value) for value in actual[4]],
        *actual[5:],
    )
    if actual != expected:
        raise ValueError("stored rolling source-flow identity diverged")
    evidence.append(_evidence(
        "SOURCE_FLOW",
        str(source.source_flow_id),
        RollingAvailability.READY,
        actual,
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))


def _source_watermarks(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> str | None:
    cursor.execute(
        "SELECT session_id::text,manifest_id::text,owner_epoch,status,"
        "journal_generation::text,last_raw_offset,last_event_offset,"
        "event_time_watermark,heartbeat_at,started_at,subscribed_at,ended_at "
        "FROM rt.stream_sessions WHERE session_id=ANY(%s::uuid[]) "
        "ORDER BY session_id",
        ([str(value) for value in plan.source_flow.stream_session_ids],),
    )
    rows = tuple(cursor.fetchall())
    expected_sessions = tuple(
        str(value) for value in plan.source_flow.stream_session_ids
    )
    reasons = []
    if tuple(str(row["session_id"]) for row in rows) != expected_sessions:
        reasons.append("SOURCE_SESSION_MEMBERSHIP_INCOMPLETE")
    for row in rows:
        if (
            str(row["manifest_id"]) != str(plan.control_plane.manifest_id)
            or int(row["owner_epoch"]) != plan.owner_epoch
            or str(row["journal_generation"])
            != str(plan.source_flow.journal_generation)
        ):
            raise ValueError("rolling source session or generation drifted")
        if (
            str(row["status"]) not in {"LIVE", "STOPPED"}
            or int(row["last_event_offset"])
            != plan.market_state_committed_offset
            or int(row["last_raw_offset"]) < plan.flow_committed_offset
            or row["event_time_watermark"] is None
            or row["event_time_watermark"] > plan.known_at
            or row["heartbeat_at"] is None
            or row["heartbeat_at"] > plan.known_at
            or row["subscribed_at"] is None
            or row["subscribed_at"] > plan.known_at
            or (
                row["ended_at"] is not None
                and row["ended_at"] > plan.known_at
            )
        ):
            reasons.append("SOURCE_WATERMARK_OR_CLOCK_MISMATCH")
    digest = canonical_sha256(tuple(tuple(row.values()) for row in rows))
    evidence.append(_evidence(
        "SOURCE_WATERMARKS",
        str(plan.source_flow.journal_generation),
        (
            RollingAvailability.READY
            if not reasons
            else RollingAvailability.UNAVAILABLE
        ),
        tuple(tuple(row.values()) for row in rows),
        observed_at=max(
            (
                row["event_time_watermark"]
                for row in rows
                if row["event_time_watermark"] is not None
            ),
            default=None,
        ),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        reasons=tuple(reasons),
    ))
    return digest if not reasons else None


def _gaps(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> str | None:
    cursor.execute(
        "SELECT gap_id::text,session_id::text,instrument_id,channel,"
        "gap_start,gap_end,journal_generation::text,last_durable_offset,"
        "reason,recovery_status FROM rt.stream_gaps "
        "WHERE session_id=ANY(%s::uuid[]) "
        "AND recovery_status IN ('UNRESOLVED','WARMING') "
        "ORDER BY gap_start,gap_id",
        ([str(value) for value in plan.source_flow.stream_session_ids],),
    )
    rows = tuple(cursor.fetchall())
    if any(
        str(row["journal_generation"])
        != str(plan.source_flow.journal_generation)
        for row in rows
    ):
        raise ValueError("rolling unresolved gap generation drifted")
    digest = advanced_options_unresolved_gaps_sha256(rows)
    evidence.append(_evidence(
        "UNRESOLVED_GAPS",
        str(plan.source_flow.journal_generation),
        (
            RollingAvailability.READY
            if not rows
            else RollingAvailability.UNAVAILABLE
        ),
        tuple(tuple(row.values()) for row in rows),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        reasons=(
            ()
            if not rows
            else ("UNRESOLVED_OR_WARMING_STREAM_GAPS",)
        ),
    ))
    return digest if not rows else None


def _checkpoints(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> tuple[str, str] | None:
    cursor.execute(
        "SELECT consumer_name,journal_generation::text,shard_id,"
        "shard_map_sha256,owner_epoch,committed_offset,"
        "checkpoint_id::text,state_hash,committed_at "
        "FROM rt.consumer_checkpoints WHERE cohort_id=%s "
        "AND journal_generation=%s AND consumer_name=ANY(%s) "
        "ORDER BY consumer_name,shard_id",
        (
            str(plan.control_plane.cohort_id),
            str(plan.source_flow.journal_generation),
            [CANONICAL_MARKET_STATE_OWNER, CANONICAL_OPTION_FLOW_OWNER],
        ),
    )
    rows = tuple(cursor.fetchall())
    digests = {}
    blocked = False
    for owner, kind, reason in (
        (
            CANONICAL_MARKET_STATE_OWNER,
            "MARKET_STATE_CHECKPOINT",
            "MARKET_STATE_CHECKPOINT_INCOMPLETE",
        ),
        (
            CANONICAL_OPTION_FLOW_OWNER,
            "FLOW_CHECKPOINT",
            "FLOW_CHECKPOINT_INCOMPLETE",
        ),
    ):
        selected = tuple(
            row for row in rows if str(row["consumer_name"]) == owner
        )
        reasons = []
        expected_shard_ids = (
            plan.market_state_shard_ids
            if owner == CANONICAL_MARKET_STATE_OWNER
            else plan.flow_shard_ids
        )
        if (
            tuple(int(row["shard_id"]) for row in selected)
            != expected_shard_ids
        ):
            reasons.append(reason)
        for row in selected:
            expected_shard_map_sha256 = (
                plan.market_state_shard_map_sha256
                if owner == CANONICAL_MARKET_STATE_OWNER
                else plan.flow_shard_map_sha256
            )
            if (
                str(row["journal_generation"])
                != str(plan.source_flow.journal_generation)
                or str(row["shard_map_sha256"]).strip()
                != expected_shard_map_sha256
            ):
                raise ValueError("rolling consumer checkpoint scope drifted")
            if (
                int(row["owner_epoch"]) != plan.owner_epoch
                or int(row["committed_offset"])
                != (
                    plan.market_state_committed_offset
                    if owner == CANONICAL_MARKET_STATE_OWNER
                    else plan.flow_committed_offset
                )
            ):
                reasons.append("CONSUMER_CHECKPOINT_OFFSET_MISMATCH")
            if (
                row["committed_at"] < plan.control_plane.slot_set.effective_at
                or row["committed_at"] > plan.known_at
            ):
                reasons.append("CONSUMER_CHECKPOINT_CLOCK_INVALID")
        digest = advanced_options_consumer_checkpoint_sha256(selected)
        digests[owner] = digest
        blocked = blocked or bool(reasons)
        evidence.append(_evidence(
            kind,
            owner,
            (
                RollingAvailability.READY
                if not reasons
                else RollingAvailability.UNAVAILABLE
            ),
            tuple(tuple(row.values()) for row in selected),
            observed_at=max(
                (row["committed_at"] for row in selected),
                default=None,
            ),
            known_at=plan.known_at,
            valid_until=plan.valid_until,
            reasons=tuple(reasons),
        ))
    if blocked:
        return None
    return (
        digests[CANONICAL_MARKET_STATE_OWNER],
        digests[CANONICAL_OPTION_FLOW_OWNER],
    )


def _current_rows(cursor, instrument_ids: tuple[int, ...]):
    cursor.execute(
        "SELECT 'Q' AS kind,instrument_id,source_session,"
        "source_sequence,event_time_raw,event_time_unit,event_at,received_at,"
        "bid,ask,bid_size,ask_size,bid_exchange,ask_exchange,quality,"
        "reason_codes,session_id::text,journal_generation::text,shard_id,"
        "source_offset,payload_sha256,NULL::numeric AS price,"
        "NULL::numeric AS size,NULL::integer AS exchange,"
        "NULL::integer[] AS conditions,NULL::varchar AS eligibility,"
        "NULL::varchar AS eligibility_policy_version "
        "FROM rt.quote_current WHERE provider='massive' "
        "AND instrument_id=ANY(%s) "
        "UNION ALL "
        "SELECT 'T',instrument_id,source_session,source_sequence,"
        "event_time_raw,event_time_unit,event_at,received_at,"
        "NULL,NULL,NULL,NULL,NULL,NULL,NULL,reason_codes,"
        "session_id::text,journal_generation::text,shard_id,source_offset,"
        "payload_sha256,price,size,exchange,conditions,eligibility,"
        "eligibility_policy_version FROM rt.trade_current "
        "WHERE provider='massive' AND instrument_id=ANY(%s) "
        "ORDER BY kind,instrument_id",
        (list(instrument_ids), list(instrument_ids)),
    )
    return tuple(cursor.fetchall())


def _equity_live_head_source(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> None:
    binding = plan.equity_live_head_binding
    if binding is None:
        return
    if isinstance(binding, UnderlyingSnapshotBinding):
        binding.require_fresh(plan.known_at)
        evidence.append(_evidence(
            "UNDERLYING_SNAPSHOT_BINDING",
            binding.selection_sha256,
            RollingAvailability.READY,
            underlying_snapshot_binding_payload(binding),
            observed_at=max(
                row.market_time for row in binding.prices
            ),
            known_at=plan.known_at,
            valid_until=binding.valid_until,
        ))
        return
    cursor.execute(
        "SELECT ingress.plan_sha256,ingress.cohort_id::text,"
        "ingress.manifest_id::text,manifest.manifest_sha256,"
        "ingress.session_date,ingress.runtime_profile_sha256,"
        "ingress.configuration_sha256,ingress.runtime_policy_sha256,"
        "ingress.authorized_end_at,ingress.status,"
        "registry.journal_generation::text,"
        "registry.stream_session_id::text,registry.metadata_sha256,"
        "registry.shard_map_sha256,registry.bridge_committed_offset,"
        "registry.status AS registry_status,session.heartbeat_at "
        "FROM rt.advanced_ingress_plans AS ingress "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=ingress.manifest_id "
        "JOIN rt.advanced_journal_registry AS registry "
        "ON registry.plan_id=ingress.plan_id "
        "JOIN rt.stream_sessions AS session "
        "ON session.session_id=registry.stream_session_id "
        "AND session.journal_generation=registry.journal_generation "
        "WHERE ingress.plan_id=%s ORDER BY registry.stream_session_id",
        (str(binding.ingress_plan_id),),
    )
    rows = tuple(cursor.fetchall())
    reasons = []
    if len(rows) != len(binding.stream_session_ids):
        reasons.append("EQUITY_LIVE_HEAD_BINDING_MISSING")
    else:
        sessions = tuple(
            sorted(
                (UUID(str(row["stream_session_id"])) for row in rows),
                key=str,
            )
        )
        if sessions != binding.stream_session_ids:
            raise ValueError(
                "Advanced Equity stream-session binding drifted"
            )
        for row in rows:
            if (
                str(row["plan_sha256"]) != binding.ingress_plan_sha256
                or UUID(str(row["cohort_id"])) != binding.cohort_id
                or UUID(str(row["manifest_id"])) != binding.manifest_id
                or str(row["manifest_sha256"])
                != binding.manifest_sha256
                or row["session_date"] != binding.session_date
                or str(row["runtime_profile_sha256"])
                != binding.runtime_profile_sha256
                or str(row["configuration_sha256"])
                != binding.configuration_sha256
                or str(row["runtime_policy_sha256"])
                != binding.runtime_policy_sha256
                or row["authorized_end_at"] != binding.valid_until
                or str(row["journal_generation"])
                != str(binding.journal_generation)
                or str(row["metadata_sha256"])
                != binding.journal_metadata_sha256
                or str(row["shard_map_sha256"])
                != binding.shard_map_sha256
                or int(row["bridge_committed_offset"])
                != binding.committed_offset
                or row["heartbeat_at"] != binding.observed_at
                or str(row["status"]) not in {"ACTIVE", "COMPLETE"}
                or str(row["registry_status"])
                not in {"WRITING", "DRAINING", "DRAINED"}
            ):
                raise ValueError(
                    "Advanced Equity live-head source binding drifted"
                )
    evidence.append(_evidence(
        "EQUITY_LIVE_HEAD_BINDING",
        str(binding.ingress_plan_id),
        (
            RollingAvailability.READY
            if not reasons
            else RollingAvailability.UNAVAILABLE
        ),
        tuple(tuple(row.values()) for row in rows),
        observed_at=binding.observed_at,
        known_at=plan.known_at,
        valid_until=binding.valid_until,
        reasons=tuple(reasons),
    ))


def _market_state(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    contracts,
    equities,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> tuple[
    tuple[AdvancedOptionsUnderlyingTick | UnderlyingSnapshotTick, ...],
    str,
] | None:
    option_ids = tuple(sorted({
        int(row["instrument_id"]) for row in contracts
    }))
    equity_ids = tuple(int(row["instrument_id"]) for row in equities)
    equity_binding = plan.equity_live_head_binding
    snapshot_based = isinstance(
        equity_binding,
        UnderlyingSnapshotBinding,
    )
    rows = _current_rows(
        cursor,
        (
            option_ids
            if snapshot_based
            else tuple(sorted((*option_ids, *equity_ids)))
        ),
    )
    option_sessions = {
        str(value) for value in plan.source_flow.stream_session_ids
    }
    equity_sessions = (
        option_sessions
        if equity_binding is None or snapshot_based
        else {
            str(value) for value in equity_binding.stream_session_ids
        }
    )
    by_key = {
        (str(row["kind"]), int(row["instrument_id"])): row
        for row in rows
    }
    reasons = []
    expected_keys = {
        (kind, instrument_id)
        for kind in ("Q", "T")
        for instrument_id in (
            option_ids
            if snapshot_based
            else (*option_ids, *equity_ids)
        )
    }
    if set(by_key) != expected_keys:
        reasons.append("CANONICAL_CURRENT_HEADS_INCOMPLETE")
    for row in rows:
        instrument_id = int(row["instrument_id"])
        is_equity = instrument_id in equity_ids
        expected_generation = (
            plan.source_flow.journal_generation
            if not is_equity or equity_binding is None or snapshot_based
            else equity_binding.journal_generation
        )
        expected_sessions = (
            option_sessions
            if not is_equity
            else equity_sessions
        )
        expected_session_date = (
            plan.control_plane.session_date
            if not is_equity or equity_binding is None or snapshot_based
            else equity_binding.session_date
        )
        if (
            str(row["journal_generation"])
            != str(expected_generation)
            or str(row["session_id"]) not in expected_sessions
            or row["source_session"] != expected_session_date
        ):
            raise ValueError(
                "rolling current source session or generation drifted"
            )
        maximum_offset = (
            plan.flow_committed_offset
            if not is_equity or equity_binding is None or snapshot_based
            else equity_binding.committed_offset
        )
        maximum_clock = (
            plan.known_at
            if not is_equity or equity_binding is None or snapshot_based
            else equity_binding.observed_at
        )
        if (
            int(row["source_offset"]) > maximum_offset
            or row["received_at"] > maximum_clock
            or row["event_at"] > maximum_clock
        ):
            reasons.append("CANONICAL_CURRENT_HEAD_AHEAD_OF_CHECKPOINT")
        if (
            str(row["kind"]) == "Q"
            and (
                str(row["quality"]) != "GOOD"
                or tuple(row["reason_codes"])
                or row["bid"] is None
                or row["ask"] is None
            )
        ) or (
            str(row["kind"]) == "T"
            and (
                str(row["eligibility"]) != "INCLUDED"
                or tuple(row["reason_codes"])
                or row["price"] is None
                or row["size"] is None
                or row["exchange"] is None
            )
        ):
            reasons.append("CANONICAL_CURRENT_HEAD_INVALID")
    digest = (
        canonical_sha256({
            "option_heads": tuple(
                tuple(row.values()) for row in rows
            ),
            "underlying_snapshot": (
                underlying_snapshot_binding_payload(equity_binding)
            ),
        })
        if snapshot_based
        else canonical_sha256(
            tuple(tuple(row.values()) for row in rows)
        )
    )
    ticks: list[
        AdvancedOptionsUnderlyingTick | UnderlyingSnapshotTick
    ] = []
    if not reasons:
        option_time_by_security = {}
        for row in contracts:
            instrument_id = int(row["instrument_id"])
            times = (
                by_key[("Q", instrument_id)]["event_at"],
                by_key[("T", instrument_id)]["event_at"],
            )
            option_time_by_security[str(row["underlying_security_id"])] = max(
                times
            )
        for equity in equities:
            instrument_id = int(equity["instrument_id"])
            if snapshot_based:
                snapshot = equity_binding.price_for(
                    str(equity["ticker"])
                )
                ticks.append(UnderlyingSnapshotTick(
                    instrument_id=instrument_id,
                    evidence=snapshot,
                    selected_at=plan.known_at,
                    option_market_time=option_time_by_security[
                        str(equity["security_id"])
                    ],
                    maximum_price_age_seconds=(
                        equity_binding.maximum_price_age_seconds
                    ),
                ))
                continue
            quote_row = by_key[("Q", instrument_id)]
            trade_row = by_key[("T", instrument_id)]
            quote = MaterializedQuote(
                journal_generation=UUID(str(quote_row["journal_generation"])),
                stream_session_id=UUID(str(quote_row["session_id"])),
                instrument_id=instrument_id,
                provider_symbol=str(equity["ticker"]),
                source_session=quote_row["source_session"],
                source_sequence=quote_row["source_sequence"],
                source_timestamp=int(quote_row["event_time_raw"]),
                source_timestamp_unit=str(quote_row["event_time_unit"]),
                event_at=quote_row["event_at"],
                received_at=quote_row["received_at"],
                bid=quote_row["bid"],
                ask=quote_row["ask"],
                bid_size=quote_row["bid_size"],
                ask_size=quote_row["ask_size"],
                bid_exchange=quote_row["bid_exchange"],
                ask_exchange=quote_row["ask_exchange"],
                quality=str(quote_row["quality"]),
                reason_codes=tuple(quote_row["reason_codes"]),
                source_offset=int(quote_row["source_offset"]),
                payload_sha256=str(quote_row["payload_sha256"]).strip(),
            )
            trade = MaterializedTrade(
                journal_generation=UUID(str(trade_row["journal_generation"])),
                stream_session_id=UUID(str(trade_row["session_id"])),
                instrument_id=instrument_id,
                provider_symbol=str(equity["ticker"]),
                asset_class=AssetClass.EQUITY,
                source_session=trade_row["source_session"],
                source_sequence=trade_row["source_sequence"],
                source_timestamp=int(trade_row["event_time_raw"]),
                source_timestamp_unit=str(trade_row["event_time_unit"]),
                event_at=trade_row["event_at"],
                received_at=trade_row["received_at"],
                price=trade_row["price"],
                size=trade_row["size"],
                exchange=int(trade_row["exchange"]),
                conditions=tuple(trade_row["conditions"]),
                quality="VALID",
                reason_codes=(),
                eligibility=str(trade_row["eligibility"]),
                eligibility_reason_codes=tuple(trade_row["reason_codes"]),
                semantics_version=str(
                    trade_row["eligibility_policy_version"]
                ),
                source_offset=int(trade_row["source_offset"]),
                payload_sha256=str(trade_row["payload_sha256"]).strip(),
            )
            tick = build_underlying_tick(
                instrument_id=instrument_id,
                ticker=str(equity["ticker"]),
                session_date=plan.control_plane.session_date,
                quote=quote,
                trade=trade,
                option_market_time=option_time_by_security[
                    str(equity["security_id"])
                ],
                selected_at=plan.known_at,
                maximum_age_ms=plan.maximum_underlying_age_ms,
                maximum_option_spot_skew_ms=(
                    plan.maximum_option_spot_skew_ms
                ),
            )
            if tick.availability is not RollingAvailability.READY:
                reasons.extend(tick.reason_codes)
            ticks.append(tick)
    evidence.append(_evidence(
        "MARKET_STATE_HEADS",
        str(plan.source_flow.journal_generation),
        (
            RollingAvailability.READY
            if not reasons
            else RollingAvailability.UNAVAILABLE
        ),
        tuple(tuple(row.values()) for row in rows),
        observed_at=max((row["event_at"] for row in rows), default=None),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        reasons=tuple(reasons),
    ))
    if reasons:
        return None
    return tuple(ticks), digest


def _volumes(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    contracts,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
):
    tickers = tuple(sorted(
        str(row["contract_ticker"]) for row in contracts
    ))
    cursor.execute(
        "SELECT cohort_id::text,session_date,contract_ticker,"
        "cumulative_volume,last_source_seq,last_event_time,baseline_kind,"
        "snapshot_volume,snapshot_watermark_seq "
        "FROM rt.option_volume_checkpoints WHERE cohort_id=%s "
        "AND session_date=%s AND contract_ticker=ANY(%s) "
        "ORDER BY contract_ticker",
        (
            str(plan.control_plane.cohort_id),
            plan.control_plane.session_date,
            list(tickers),
        ),
    )
    rows = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT contract_ticker,max(source_seq) AS last_source_seq,"
        "max(event_time) AS last_event_time,max(source_offset) AS last_offset "
        "FROM rt.option_flow_events WHERE cohort_id=%s AND session_date=%s "
        "AND source_flow_id=%s AND contract_ticker=ANY(%s) "
        "GROUP BY contract_ticker ORDER BY contract_ticker",
        (
            str(plan.control_plane.cohort_id),
            plan.control_plane.session_date,
            str(plan.source_flow.source_flow_id),
            list(tickers),
        ),
    )
    event_rows = {
        str(row["contract_ticker"]): row for row in cursor.fetchall()
    }
    reasons = []
    if tuple(str(row["contract_ticker"]) for row in rows) != tickers:
        reasons.append("OPTION_VOLUME_CHECKPOINT_MEMBERSHIP_INCOMPLETE")
    states = []
    for row in rows:
        ticker = str(row["contract_ticker"])
        baseline = BaselineKind(str(row["baseline_kind"]))
        event = event_rows.get(ticker)
        if baseline not in COMPLETE_BASELINES:
            reasons.append("OPTION_VOLUME_CHECKPOINT_INCOMPLETE")
        if (
            event is None
            and (
                int(row["cumulative_volume"]) != 0
                or int(row["last_source_seq"]) != 0
                or row["last_event_time"] is not None
            )
        ) or (
            event is not None
            and (
                int(event["last_offset"]) > plan.flow_committed_offset
                or int(event["last_source_seq"])
                != int(row["last_source_seq"])
                or event["last_event_time"] != row["last_event_time"]
            )
        ):
            reasons.append("OPTION_VOLUME_CHECKPOINT_WATERMARK_MISMATCH")
        checkpoint = VolumeCheckpoint(
            cohort_id=UUID(str(row["cohort_id"])),
            session_date=row["session_date"],
            contract_ticker=ticker,
            cumulative_volume=int(row["cumulative_volume"]),
            last_source_seq=int(row["last_source_seq"]),
            last_event_time=row["last_event_time"],
            baseline_kind=baseline.value,
            snapshot_volume=row["snapshot_volume"],
            snapshot_watermark_seq=row["snapshot_watermark_seq"],
        )
        states.append(contract_volume_state(
            checkpoint,
            reconcile_evidence_sha256=(
                canonical_sha256({
                    "contract": "option-volume-reconcile-v1",
                    "source_flow_sha256": plan.source_flow.source_flow_sha256,
                    "contract_ticker": ticker,
                    "snapshot_volume": row["snapshot_volume"],
                    "snapshot_watermark_seq": row[
                        "snapshot_watermark_seq"
                    ],
                    "last_source_seq": row["last_source_seq"],
                    "last_event_time": row["last_event_time"],
                })
                if baseline is BaselineKind.SNAPSHOT_RECONCILED
                else None
            ),
        ))
    value = (
        tuple(tuple(row.values()) for row in rows),
        tuple(tuple(row.values()) for row in event_rows.values()),
    )
    digest = canonical_sha256(value)
    evidence.append(_evidence(
        "OPTION_VOLUME_CHECKPOINTS",
        str(plan.control_plane.cohort_id),
        (
            RollingAvailability.READY
            if not reasons
            else RollingAvailability.UNAVAILABLE
        ),
        value,
        observed_at=max(
            (
                row["last_event_time"]
                for row in rows
                if row["last_event_time"] is not None
            ),
            default=None,
        ),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        reasons=tuple(reasons),
    ))
    if reasons:
        return None
    return tuple(states), digest


def _order_windows(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
    evidence: list[AdvancedOptionsRollingProducerEvidence],
) -> AdvancedOptionsOrderWindowState | None:
    cursor.execute(
        "WITH latest AS ("
        "SELECT DISTINCT ON (cluster_id,profile) cluster_id,revision,"
        "session_date,profile,window_seconds,print_count,status,"
        "source_flow_id::text,condition_semantics_sha256,"
        "flow_policy_sha256,advanced_flow_policy_sha256,"
        "contract_scope_sha256,manifest_sha256,slot_sha256 "
        "FROM rt.option_flow_clusters WHERE cohort_id=%s "
        "AND session_date=%s AND source_flow_id=%s "
        "ORDER BY cluster_id,profile,revision DESC"
        ") SELECT * FROM latest ORDER BY window_seconds,cluster_id,profile",
        (
            str(plan.control_plane.cohort_id),
            plan.control_plane.session_date,
            str(plan.source_flow.source_flow_id),
        ),
    )
    headers = tuple(cursor.fetchall())
    cursor.execute(
        "WITH latest AS ("
        "SELECT DISTINCT ON (cluster_id,profile) cluster_id,revision,"
        "session_date,profile,window_seconds,status "
        "FROM rt.option_flow_clusters WHERE cohort_id=%s "
        "AND session_date=%s AND source_flow_id=%s "
        "ORDER BY cluster_id,profile,revision DESC"
        ") SELECT latest.cluster_id::text,latest.revision,"
        "latest.profile,latest.window_seconds,member.event_id::text,"
        "member.event_time,event.source_offset,event.journal_generation::text,"
        "event.source_flow_id::text FROM latest "
        "JOIN rt.option_flow_cluster_members AS member "
        "ON member.cluster_id=latest.cluster_id "
        "AND member.revision=latest.revision "
        "AND member.session_date=latest.session_date "
        "AND member.profile=latest.profile "
        "JOIN rt.option_flow_events AS event "
        "ON event.event_id=member.event_id "
        "AND event.event_time=member.event_time "
        "WHERE latest.status<>'RETRACTED' "
        "ORDER BY latest.window_seconds,event.source_offset,event.event_id",
        (
            str(plan.control_plane.cohort_id),
            plan.control_plane.session_date,
            str(plan.source_flow.source_flow_id),
        ),
    )
    members = tuple(cursor.fetchall())
    reasons = []
    member_counts: dict[tuple[str, int, str], int] = {}
    for row in members:
        key = (
            str(row["cluster_id"]),
            int(row["revision"]),
            str(row["profile"]),
        )
        member_counts[key] = member_counts.get(key, 0) + 1
        if (
            str(row["journal_generation"])
            != str(plan.source_flow.journal_generation)
            or str(row["source_flow_id"]) != str(plan.source_flow.source_flow_id)
        ):
            raise ValueError("rolling cluster source identity drifted")
        if int(row["source_offset"]) > plan.flow_committed_offset:
            reasons.append("CLUSTER_WINDOW_AHEAD_OF_CHECKPOINT")
    for row in headers:
        if int(row["window_seconds"]) not in CANONICAL_ORDER_WINDOWS_SECONDS:
            raise ValueError("rolling cluster window policy changed")
        if (
            str(row["condition_semantics_sha256"])
            != plan.source_flow.condition_semantics_sha256
            or str(row["source_flow_id"]) != str(plan.source_flow.source_flow_id)
            or (
                plan.source_flow.identity_version in (3, 4)
                and (
                    str(row["flow_policy_sha256"])
                    != plan.source_flow.flow_policy_sha256
                    or str(row["advanced_flow_policy_sha256"])
                    != plan.source_flow.advanced_flow_policy_sha256
                    or str(row["contract_scope_sha256"])
                    != plan.contract_scope.sha256
                    or str(row["manifest_sha256"])
                    != plan.control_plane.manifest_sha256
                    or str(row["slot_sha256"])
                    != plan.control_plane.slot_set_sha256
                )
            )
        ):
            raise ValueError("rolling cluster policy identity drifted")
        key = (
            str(row["cluster_id"]),
            int(row["revision"]),
            str(row["profile"]),
        )
        expected_members = (
            0 if str(row["status"]) == "RETRACTED" else int(row["print_count"])
        )
        if member_counts.get(key, 0) != expected_members:
            reasons.append("CLUSTER_WINDOW_MEMBERSHIP_INCOMPLETE")
    normalized = {}
    for row in members:
        key = (int(row["window_seconds"]), UUID(str(row["event_id"])))
        value = (
            int(row["source_offset"]),
            row["event_time"],
        )
        if key in normalized and normalized[key] != value:
            raise ValueError("rolling cluster member identity diverged")
        normalized[key] = value
    if len(normalized) > plan.maximum_cluster_members:
        reasons.append("CLUSTER_WINDOW_MEMBER_BOUND_EXCEEDED")
    value = (
        tuple(tuple(row.values()) for row in headers),
        tuple(tuple(row.values()) for row in members),
    )
    digest = canonical_sha256(value)
    evidence.append(_evidence(
        "CLUSTER_WINDOWS",
        str(plan.source_flow.source_flow_id),
        (
            RollingAvailability.READY
            if not reasons
            else RollingAvailability.UNAVAILABLE
        ),
        value,
        observed_at=max(
            (row["event_time"] for row in members),
            default=None,
        ),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        reasons=tuple(reasons),
    ))
    if reasons:
        return None
    return AdvancedOptionsOrderWindowState(
        owner_epoch=plan.owner_epoch,
        committed_offset=plan.flow_committed_offset,
        checkpoint_sha256=digest,
        windows_seconds=CANONICAL_ORDER_WINDOWS_SECONDS,
        members=tuple(
            AdvancedOptionsOrderWindowMember(
                event_id,
                source_offset,
                event_time,
                window_seconds,
            )
            for (
                window_seconds,
                event_id,
            ), (
                source_offset,
                event_time,
            ) in sorted(
                normalized.items(),
                key=lambda item: (
                    item[0][0],
                    item[1][0],
                    str(item[0][1]),
                ),
            )
        ),
        maximum_members=plan.maximum_cluster_members,
    )


def assemble_advanced_options_rolling_state_cursor(
    cursor,
    plan: AdvancedOptionsRollingProducerPlan,
) -> AdvancedOptionsRollingProducerPreflight:
    _validate_plan(plan)
    evidence: list[AdvancedOptionsRollingProducerEvidence] = []
    contracts, equities = _active_scope(cursor, plan, evidence)
    _reference(cursor, plan, evidence)
    _source_flow(cursor, plan, evidence)
    source_watermarks_sha256 = _source_watermarks(cursor, plan, evidence)
    unresolved_gaps_sha256 = _gaps(cursor, plan, evidence)
    checkpoint_hashes = _checkpoints(cursor, plan, evidence)
    _equity_live_head_source(cursor, plan, evidence)
    market_state = _market_state(
        cursor,
        plan,
        contracts,
        equities,
        evidence,
    )
    volumes = _volumes(cursor, plan, contracts, evidence)
    order_windows = _order_windows(cursor, plan, evidence)
    if (
        any(
            row.availability is RollingAvailability.UNAVAILABLE
            for row in evidence
        )
        or source_watermarks_sha256 is None
        or unresolved_gaps_sha256 is None
        or checkpoint_hashes is None
        or market_state is None
        or volumes is None
        or order_windows is None
    ):
        return _result(plan, evidence, None)
    market_checkpoint_sha256, flow_checkpoint_sha256 = checkpoint_hashes
    ticks, market_state_heads_sha256 = market_state
    contract_volumes, volume_checkpoint_sha256 = volumes
    binding = AdvancedOptionsRollingProducerBinding(
        producer_plan_id=plan.plan_id,
        producer_plan_sha256=plan.plan_sha256,
        reference_state_id=plan.reference_state.state_id,
        reference_state_sha256=plan.reference_state.state_sha256,
        reference_generation=plan.reference_state.reference_generation,
        source_flow_id=plan.source_flow.source_flow_id,
        source_flow_sha256=plan.source_flow.source_flow_sha256,
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        unresolved_gaps_sha256=unresolved_gaps_sha256,
        source_watermarks_sha256=source_watermarks_sha256,
        market_state_heads_sha256=market_state_heads_sha256,
        volume_checkpoint_sha256=volume_checkpoint_sha256,
    )
    state = build_advanced_options_rolling_state(
        cohort_id=plan.control_plane.cohort_id,
        session_date=plan.control_plane.session_date,
        journal_generation=plan.source_flow.journal_generation,
        owner_epoch=plan.owner_epoch,
        committed_offset=plan.flow_committed_offset,
        committed_at=plan.known_at,
        market_state_checkpoint_sha256=market_checkpoint_sha256,
        flow_checkpoint_sha256=flow_checkpoint_sha256,
        underlying_ticks=ticks,
        contract_volumes=contract_volumes,
        order_windows=order_windows,
        producer_binding=binding,
    )
    evidence.append(_evidence(
        "ROLLING_GENERATION",
        str(state.state_id),
        RollingAvailability.READY,
        (state.state_id, state.state_sha256, binding),
        observed_at=state.committed_at,
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    return _result(plan, evidence, state)


class PostgresAdvancedOptionsRollingProducer:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        plan: AdvancedOptionsRollingProducerPlan,
        *,
        maximum_attempts: int = 3,
        retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("Advanced Options rolling provider is invalid")
        _validate_plan(plan)
        if (
            type(maximum_attempts) is not int
            or maximum_attempts < 1
            or retry_initial_seconds < 0
            or not callable(wait)
        ):
            raise ValueError("Advanced Options rolling retry is invalid")
        self._provider = provider
        self._plan = plan
        self._maximum_attempts = maximum_attempts
        self._retry_initial_seconds = retry_initial_seconds
        self._wait = wait

    def _assemble(self, unit) -> AdvancedOptionsRollingProducerPreflight:
        with unit.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            return assemble_advanced_options_rolling_state_cursor(
                cursor,
                self._plan,
            )

    def preflight(self) -> AdvancedOptionsRollingProducerPreflight:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return self._assemble(unit)

    def produce(self) -> AdvancedOptionsRollingState:
        for attempt in range(1, self._maximum_attempts + 1):
            try:
                with self._provider.transaction(
                    APP_ROLE,
                    isolation_level="SERIALIZABLE",
                ) as unit:
                    result = self._assemble(unit)
                    if (
                        result.status is not RollingProducerStatus.READY
                        or result.state is None
                    ):
                        raise AdvancedOptionsRollingPreflightBlocked(result)
                    return persist_advanced_options_rolling_state_cursor(
                        unit.cursor,
                        result.state,
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
        raise AssertionError("unreachable rolling producer retry state")
