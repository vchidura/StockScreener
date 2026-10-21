"""Pure immutable contracts for a prospective Advanced Options control plane."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_equity_control_plane import AdvancedEquityControlPlanePlan
from .acquisition_ledger import ProviderRequestLane
from .advanced_options_reference_state import AdvancedOptionsReferenceState
from .contracts import (
    MarketChannel,
    MarketFeed,
    UniverseManifest,
    canonical_sha256,
)
from .option_session_manifest import (
    BoundedOptionManifestPolicy,
    OptionActivitySelectionRecord,
    option_activity_selection_from_payload,
    option_activity_selection_payload,
    validate_option_activity_selection,
)
from .option_source_flow import (
    CANONICAL_OPTION_FLOW_OWNER,
    OptionSourceFlowIdentity,
    build_option_source_flow_identity,
)
from .option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from .option_slot_set_repository import persist_and_activate_slot_set_cursor
from .orchestration_repository import (
    OPTIONS_CYCLE,
    OPTIONS_INPUT_READY,
    OPTIONS_PORTAL_READY,
    CycleReceiptSpec,
    build_options_receipt_spec,
)
from .preflight import ProductEntitlementProfile, ProviderTier, TierState
from .provider_budget import ProviderBudgetScope
from .tier_cohort import (
    TierCohortBinding,
    persist_tier_cohort_binding_cursor,
    product_capabilities,
    product_disabled_detectors,
)


ADVANCED_OPTIONS_PROVIDER = "massive"
ADVANCED_OPTIONS_ACCOUNT_ALIAS = "MASSIVE_ADVANCED"
ADVANCED_OPTIONS_COHORT_KIND = "MARKET_OPTIONS"
ADVANCED_OPTIONS_CHANNELS = (
    MarketChannel.TRADE,
    MarketChannel.QUOTE,
)
ADVANCED_OPTIONS_REST_LANES = frozenset({
    ProviderRequestLane.OPTIONS_CHAIN,
    ProviderRequestLane.OPTIONS_ALIGNMENT,
    ProviderRequestLane.REFERENCE,
})
PRESERVED_STAGE_CATALOGS = ("stages_v1", "stages_v2", "stages_v3")
ADVANCED_OPTIONS_ENROLLMENT_VERSION = "advanced-options-enrollment-plan-v1"
ADVANCED_OPTIONS_HISTORY_BINDING_VERSION = (
    "advanced-options-history-binding-v1"
)
ADVANCED_OPTIONS_ZERO_EFFECTS = (
    "provider",
    "service",
    "stages_v4",
    "publication",
    "release",
    "pointer",
    "outbox",
    "delivery",
    "repair",
    "backfill",
    "replay",
    "archive",
    "retention",
)

CANONICAL_SCHEDULER_OWNER = "advanced-scheduler"
CANONICAL_INGRESS_OWNER = "advanced-options-ingress-v1"
CANONICAL_MARKET_STATE_OWNER = "market-state-materializer-v1"
CANONICAL_FLOW_OWNER = CANONICAL_OPTION_FLOW_OWNER
CANONICAL_PUBLISHER_OWNER = "portal-publisher"
CANONICAL_RETENTION_OWNER = "realtime-v2-retention-service-v1"

_NIL_UUID = UUID(int=0)


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


def _positive_int(value: int, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedOptionsFeedScope:
    provider: str = ADVANCED_OPTIONS_PROVIDER
    feed: MarketFeed = MarketFeed.OPTIONS
    channels: tuple[MarketChannel, ...] = ADVANCED_OPTIONS_CHANNELS
    socket_owner: str = CANONICAL_INGRESS_OWNER

    def __post_init__(self) -> None:
        if self.provider != ADVANCED_OPTIONS_PROVIDER:
            raise ValueError("Advanced Options provider must be massive")
        if self.feed is not MarketFeed.OPTIONS:
            raise ValueError("Advanced Options requires the OPTIONS feed")
        if (
            len(self.channels) != len(ADVANCED_OPTIONS_CHANNELS)
            or set(self.channels) != set(ADVANCED_OPTIONS_CHANNELS)
        ):
            raise ValueError("Advanced Options requires exact T/Q channels")
        if self.socket_owner != CANONICAL_INGRESS_OWNER:
            raise ValueError("Advanced Options socket owner is not canonical")
        object.__setattr__(self, "channels", ADVANCED_OPTIONS_CHANNELS)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsSocketBudget:
    stream_sessions: int
    connection_attempts: int
    reconnects: int
    decoded_messages: int
    decoded_bytes: int

    def __post_init__(self) -> None:
        for name in (
            "stream_sessions",
            "connection_attempts",
            "decoded_messages",
            "decoded_bytes",
        ):
            _positive_int(getattr(self, name), name)
        if type(self.reconnects) is not int or self.reconnects < 0:
            raise ValueError("reconnects must be a nonnegative integer")
        if self.connection_attempts != self.stream_sessions + self.reconnects:
            raise ValueError(
                "connection attempts must equal stream sessions plus reconnects"
            )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsPolicyPins:
    condition_semantics_sha256: str
    flow_policy_sha256: str
    valuation_policy_sha256: str
    model_bundle_sha256: str
    catalog_sha256: str
    slot_set_policy_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "condition_semantics_sha256",
            "flow_policy_sha256",
            "valuation_policy_sha256",
            "model_bundle_sha256",
            "catalog_sha256",
            "slot_set_policy_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsOwnerNames:
    scheduler: str = CANONICAL_SCHEDULER_OWNER
    ingress: str = CANONICAL_INGRESS_OWNER
    market_state: str = CANONICAL_MARKET_STATE_OWNER
    flow: str = CANONICAL_FLOW_OWNER
    publisher: str = CANONICAL_PUBLISHER_OWNER
    retention: str = CANONICAL_RETENTION_OWNER

    def __post_init__(self) -> None:
        expected = (
            CANONICAL_SCHEDULER_OWNER,
            CANONICAL_INGRESS_OWNER,
            CANONICAL_MARKET_STATE_OWNER,
            CANONICAL_FLOW_OWNER,
            CANONICAL_PUBLISHER_OWNER,
            CANONICAL_RETENTION_OWNER,
        )
        if (
            self.scheduler,
            self.ingress,
            self.market_state,
            self.flow,
            self.publisher,
            self.retention,
        ) != expected:
            raise ValueError("Advanced Options owner names must be canonical")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRuntimeAuthorization:
    acquisition: bool = False
    o2_activation: bool = False
    publication: bool = False
    options_alerts_production: bool = False
    strategy_activation: bool = False
    execution: bool = False
    delivery: bool = False

    def __post_init__(self) -> None:
        values = (
            self.acquisition,
            self.o2_activation,
            self.publication,
            self.options_alerts_production,
            self.strategy_activation,
            self.execution,
            self.delivery,
        )
        if any(type(value) is not bool for value in values):
            raise TypeError("runtime authorization values must be booleans")

    @property
    def any_authorized(self) -> bool:
        return any((
            self.acquisition,
            self.o2_activation,
            self.publication,
            self.options_alerts_production,
            self.strategy_activation,
            self.execution,
            self.delivery,
        ))

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProspectiveReferenceIdentity:
    state_id: UUID
    state_sha256: str
    reference_generation: int
    session_date: date
    known_at: datetime
    valid_until: datetime
    underlyings: tuple[str, ...]
    open_interest_sha256: str

    def __post_init__(self) -> None:
        known = _utc(self.known_at, "known_at")
        valid = _utc(self.valid_until, "valid_until")
        underlyings = tuple(sorted({
            value.strip().upper()
            for value in self.underlyings
            if value.strip()
        }))
        identity = {
            "contract": "advanced-options-prospective-reference-v1",
            "reference_generation": self.reference_generation,
            "session_date": self.session_date,
            "known_at": known,
            "valid_until": valid,
            "underlyings": underlyings,
            "open_interest_sha256": self.open_interest_sha256,
        }
        expected_sha256 = canonical_sha256(identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-options-prospective-reference:"
                f"{expected_sha256}"
            ),
        )
        if (
            self.state_id != expected_id
            or self.state_sha256 != expected_sha256
            or type(self.reference_generation) is not int
            or self.reference_generation <= 0
            or not isinstance(self.session_date, date)
            or isinstance(self.session_date, datetime)
            or known >= valid
            or not underlyings
            or _sha256(
                self.open_interest_sha256,
                "open_interest_sha256",
            )
            != self.open_interest_sha256
        ):
            raise ValueError(
                "prospective Advanced Options reference identity is invalid"
            )
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "underlyings", underlyings)


def build_advanced_options_prospective_reference_identity(
    *,
    reference_generation: int,
    session_date: date,
    known_at: datetime,
    valid_until: datetime,
    underlyings: tuple[str, ...],
    open_interest_sha256: str,
) -> AdvancedOptionsProspectiveReferenceIdentity:
    known = _utc(known_at, "known_at")
    valid = _utc(valid_until, "valid_until")
    normalized_underlyings = tuple(sorted({
        value.strip().upper()
        for value in underlyings
        if value.strip()
    }))
    identity = {
        "contract": "advanced-options-prospective-reference-v1",
        "reference_generation": reference_generation,
        "session_date": session_date,
        "known_at": known,
        "valid_until": valid,
        "underlyings": normalized_underlyings,
        "open_interest_sha256": _sha256(
            open_interest_sha256,
            "open_interest_sha256",
        ),
    }
    digest = canonical_sha256(identity)
    return AdvancedOptionsProspectiveReferenceIdentity(
        state_id=uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-options-prospective-reference:"
                f"{digest}"
            ),
        ),
        state_sha256=digest,
        reference_generation=reference_generation,
        session_date=session_date,
        known_at=known,
        valid_until=valid,
        underlyings=normalized_underlyings,
        open_interest_sha256=str(identity["open_interest_sha256"]),
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReceiptInputs:
    cutoff: datetime
    reference_state: (
        AdvancedOptionsReferenceState
        | AdvancedOptionsProspectiveReferenceIdentity
    )
    equity_input_receipt_id: UUID
    equity_input_sha256: str
    discovery_batches_sha256: str
    open_interest_sha256: str
    consumer_checkpoints_sha256: str
    unresolved_gaps_sha256: str
    market_state_sha256: str
    source_clocks_sha256: str
    detector_outcomes_sha256: str
    projection_sha256: str
    build_id: str

    def __post_init__(self) -> None:
        cutoff = _utc(self.cutoff, "cutoff")
        object.__setattr__(self, "cutoff", cutoff)
        if (
            not isinstance(
                self.reference_state,
                (
                    AdvancedOptionsReferenceState,
                    AdvancedOptionsProspectiveReferenceIdentity,
                ),
            )
            or not isinstance(self.equity_input_receipt_id, UUID)
            or not (
                self.reference_state.known_at
                <= cutoff
                < self.reference_state.valid_until
            )
        ):
            raise ValueError("reference state or equity receipt identity is invalid")
        for name in (
            "equity_input_sha256",
            "discovery_batches_sha256",
            "open_interest_sha256",
            "consumer_checkpoints_sha256",
            "unresolved_gaps_sha256",
            "market_state_sha256",
            "source_clocks_sha256",
            "detector_outcomes_sha256",
            "projection_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        object.__setattr__(self, "build_id", _text(self.build_id, "build_id"))

    @property
    def reference_generation(self) -> int:
        return self.reference_state.reference_generation


AdvancedOptionsSourceFlowBinding = OptionSourceFlowIdentity


def build_advanced_options_source_flow_binding(
    *,
    journal_generation: UUID,
    stream_session_ids: tuple[UUID, ...],
    pins: AdvancedOptionsPolicyPins,
) -> AdvancedOptionsSourceFlowBinding:
    if not isinstance(pins, AdvancedOptionsPolicyPins):
        raise TypeError("source-flow policy pins are invalid")
    return build_option_source_flow_identity(
        journal_generation=journal_generation,
        stream_session_ids=stream_session_ids,
        condition_semantics_sha256=pins.condition_semantics_sha256,
        flow_policy_sha256=pins.flow_policy_sha256,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReceiptIdentity:
    receipt_id: UUID
    receipt_sha256: str
    spec: CycleReceiptSpec

    def __post_init__(self) -> None:
        if not isinstance(self.receipt_id, UUID):
            raise TypeError("receipt_id must be a UUID")
        object.__setattr__(
            self,
            "receipt_sha256",
            _sha256(self.receipt_sha256, "receipt_sha256"),
        )
        if not isinstance(self.spec, CycleReceiptSpec):
            raise TypeError("receipt spec is invalid")


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReceiptSet:
    options_input_ready: AdvancedOptionsReceiptIdentity
    options_cycle: AdvancedOptionsReceiptIdentity
    options_portal_ready: AdvancedOptionsReceiptIdentity

    @property
    def ids(self) -> tuple[UUID, UUID, UUID]:
        return (
            self.options_input_ready.receipt_id,
            self.options_cycle.receipt_id,
            self.options_portal_ready.receipt_id,
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsControlPlanePlan:
    plan_id: UUID
    plan_sha256: str
    session_date: date
    entitlement: ProductEntitlementProfile
    options_entitlement_evidence_sha256: str
    options_authorization_reference: str
    checked_at: datetime
    expires_at: datetime
    planned_at: datetime
    profile_id: UUID
    profile_sha256: str
    universe: UniverseManifest
    manifest_id: UUID
    manifest_sha256: str
    cohort_kind: str
    cohort_id: UUID
    cohort_sha256: str
    binding: TierCohortBinding
    manifest_policy: BoundedOptionManifestPolicy
    feed_scope: AdvancedOptionsFeedScope
    slot_set: SlotSetVersion
    slot_set_sha256: str
    pins: AdvancedOptionsPolicyPins
    socket_budget: AdvancedOptionsSocketBudget
    provider_budget: ProviderBudgetScope
    owners: AdvancedOptionsOwnerNames
    source_flow: AdvancedOptionsSourceFlowBinding
    receipts: AdvancedOptionsReceiptSet
    runtime_authorization: AdvancedOptionsRuntimeAuthorization
    stage_catalog_versions: tuple[str, ...] = PRESERVED_STAGE_CATALOGS
    runtime_enabled: bool = False
    options_cycle_bound: bool = False

    @property
    def capability_only(self) -> bool:
        return (
            self.entitlement.advanced_platform_ready
            and not self.runtime_authorization.any_authorized
            and not self.runtime_enabled
            and not self.options_cycle_bound
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsEnrollmentPlan:
    enrollment_id: UUID
    enrollment_sha256: str
    control_plane: AdvancedOptionsControlPlanePlan
    equity_plan: AdvancedEquityControlPlanePlan
    activity_selection: OptionActivitySelectionRecord
    equity_members_sha256: str

    @property
    def zero_effect_counts(self) -> dict[str, int]:
        return {name: 0 for name in ADVANCED_OPTIONS_ZERO_EFFECTS}


@dataclass(frozen=True, slots=True)
class AdvancedOptionsHistoryBinding:
    target_cohort_id: UUID
    source_cohort_id: UUID
    source_provider_tier: ProviderTier
    history_before: datetime
    source_manifest_sha256: str
    authorization_reference: str
    binding_sha256: str

    def __post_init__(self) -> None:
        before = _utc(self.history_before, "history_before")
        source_manifest = _sha256(
            self.source_manifest_sha256,
            "source_manifest_sha256",
        )
        reference = _text(
            self.authorization_reference,
            "authorization_reference",
        )
        if (
            self.target_cohort_id.int == 0
            or self.source_cohort_id.int == 0
            or self.target_cohort_id == self.source_cohort_id
            or self.source_provider_tier is not ProviderTier.MASSIVE_DEVELOPER
        ):
            raise ValueError(
                "Advanced Options history binding scope is invalid"
            )
        object.__setattr__(self, "history_before", before)
        object.__setattr__(self, "source_manifest_sha256", source_manifest)
        object.__setattr__(self, "authorization_reference", reference)
        if self.binding_sha256 != canonical_sha256(
            advanced_options_history_binding_payload(
                self,
                include_sha256=False,
            )
        ):
            raise ValueError("Advanced Options history binding identity changed")


def advanced_options_history_binding_payload(
    binding: AdvancedOptionsHistoryBinding,
    *,
    include_sha256: bool = True,
) -> dict[str, object]:
    if not isinstance(binding, AdvancedOptionsHistoryBinding):
        raise TypeError("Advanced Options history binding is invalid")
    payload = {
        "contract_version": ADVANCED_OPTIONS_HISTORY_BINDING_VERSION,
        "target_cohort_id": str(binding.target_cohort_id),
        "source_cohort_id": str(binding.source_cohort_id),
        "source_provider_tier": binding.source_provider_tier.value,
        "history_before": binding.history_before.isoformat(),
        "source_manifest_sha256": binding.source_manifest_sha256,
        "authorization_reference": binding.authorization_reference,
    }
    if include_sha256:
        payload["binding_sha256"] = binding.binding_sha256
    return payload


def build_advanced_options_history_binding(
    *,
    target_cohort_id: UUID,
    source_cohort_id: UUID,
    history_before: datetime,
    source_manifest_sha256: str,
    authorization_reference: str,
) -> AdvancedOptionsHistoryBinding:
    body = {
        "contract_version": ADVANCED_OPTIONS_HISTORY_BINDING_VERSION,
        "target_cohort_id": str(target_cohort_id),
        "source_cohort_id": str(source_cohort_id),
        "source_provider_tier": ProviderTier.MASSIVE_DEVELOPER.value,
        "history_before": _utc(
            history_before,
            "history_before",
        ).isoformat(),
        "source_manifest_sha256": _sha256(
            source_manifest_sha256,
            "source_manifest_sha256",
        ),
        "authorization_reference": _text(
            authorization_reference,
            "authorization_reference",
        ),
    }
    return AdvancedOptionsHistoryBinding(
        target_cohort_id=target_cohort_id,
        source_cohort_id=source_cohort_id,
        source_provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        history_before=datetime.fromisoformat(str(body["history_before"])),
        source_manifest_sha256=str(body["source_manifest_sha256"]),
        authorization_reference=str(body["authorization_reference"]),
        binding_sha256=canonical_sha256(body),
    )


def _slot_set_identity(slot_set: SlotSetVersion) -> dict[str, object]:
    return {
        "version": slot_set.version,
        "kind": slot_set.kind,
        "effective_at": slot_set.effective_at,
        "policy_sha256": slot_set.policy_sha256,
        "member_sha256": slot_set.member_sha256,
        "primary_contract_count": slot_set.primary_contract_count,
        "reserve_contract_count": slot_set.reserve_contract_count,
        "contract_count": slot_set.contract_count,
    }


def _receipt_identity(
    *,
    cohort_id: UUID,
    spec: CycleReceiptSpec,
) -> AdvancedOptionsReceiptIdentity:
    identity = {
        "contract": "advanced-options-receipt-v1",
        "cohort_id": cohort_id,
        "receipt_kind": spec.receipt_kind,
        "cutoff": spec.cutoff,
        "reference_generation": spec.reference_generation,
        "universe_sha256": spec.universe_sha256,
        "input_sha256": spec.input_sha256,
        "build_id": spec.build_id,
        "advance_release": spec.advance_release,
    }
    receipt_sha256 = canonical_sha256(identity)
    return AdvancedOptionsReceiptIdentity(
        receipt_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-receipt:{receipt_sha256}",
        ),
        receipt_sha256=receipt_sha256,
        spec=spec,
    )


def _validate_equity_source(plan: AdvancedEquityControlPlanePlan) -> None:
    if not isinstance(plan, AdvancedEquityControlPlanePlan):
        raise TypeError("equity control-plane source is invalid")
    if (
        plan.binding.provider_tier is not ProviderTier.MASSIVE_ADVANCED
        or plan.binding.options_tier is not None
        or plan.binding.options_enabled
        or plan.universe.option_underlyings
    ):
        raise ValueError(
            "Advanced Options must extend an Advanced Equity-only cohort"
        )


def _validate_slots(
    slot_set: SlotSetVersion,
    manifest_policy: BoundedOptionManifestPolicy,
) -> None:
    channels: dict[int, set[MarketChannel]] = {}
    for member in slot_set.members:
        if member.purpose not in {
            SlotSetPurpose.PRIMARY,
            SlotSetPurpose.RESERVE,
        }:
            raise ValueError(
                "initial Advanced Options slots must be primary or reserve"
            )
        channels.setdefault(member.instrument_id, set()).add(member.channel)
    if any(values != set(ADVANCED_OPTIONS_CHANNELS) for values in channels.values()):
        raise ValueError("every Advanced Options slot requires exact T/Q channels")
    if slot_set.primary_contract_count == 0:
        raise ValueError("Advanced Options requires primary contracts")
    if (
        manifest_policy.reserved_option_quote_contracts
        and slot_set.reserve_contract_count == 0
    ):
        raise ValueError("Advanced Options reserve slots are incomplete")
    if (
        slot_set.primary_contract_count
        > manifest_policy.primary_option_quote_contracts
        or slot_set.reserve_contract_count
        > manifest_policy.reserved_option_quote_contracts
        or slot_set.contract_count
        > manifest_policy.maximum_option_quote_contracts
    ):
        raise ValueError("Advanced Options slot allocation exceeds its policy")


def build_advanced_options_control_plane_plan(
    *,
    equity_plan: AdvancedEquityControlPlanePlan,
    entitlement: ProductEntitlementProfile,
    option_underlyings: tuple[str, ...],
    options_entitlement_evidence_sha256: str,
    options_authorization_reference: str,
    checked_at: datetime,
    expires_at: datetime,
    planned_at: datetime,
    manifest_policy: BoundedOptionManifestPolicy,
    slot_members: tuple[SlotSetMember, ...],
    slot_effective_at: datetime,
    feed_scope: AdvancedOptionsFeedScope,
    pins: AdvancedOptionsPolicyPins,
    socket_budget: AdvancedOptionsSocketBudget,
    provider_budget: ProviderBudgetScope,
    owners: AdvancedOptionsOwnerNames,
    journal_generation: UUID,
    stream_session_ids: tuple[UUID, ...],
    receipt_inputs: AdvancedOptionsReceiptInputs,
    runtime_authorization: AdvancedOptionsRuntimeAuthorization | None = None,
    authorized_cohort_id: UUID | None = None,
    authorized_plan_id: UUID | None = None,
) -> AdvancedOptionsControlPlanePlan:
    _validate_equity_source(equity_plan)
    if (
        not isinstance(entitlement, ProductEntitlementProfile)
        or entitlement.stocks_tier is not ProviderTier.MASSIVE_ADVANCED
        or entitlement.options_tier is not ProviderTier.MASSIVE_ADVANCED
        or not entitlement.advanced_platform_ready
    ):
        raise ValueError(
            "Advanced Options requires exact Stocks Advanced plus Options Advanced"
        )
    if not isinstance(feed_scope, AdvancedOptionsFeedScope):
        raise TypeError("Advanced Options feed scope is invalid")
    if not isinstance(manifest_policy, BoundedOptionManifestPolicy):
        raise TypeError("Advanced Options manifest policy is invalid")
    if not isinstance(pins, AdvancedOptionsPolicyPins):
        raise TypeError("Advanced Options policy pins are invalid")
    if not isinstance(socket_budget, AdvancedOptionsSocketBudget):
        raise TypeError("Advanced Options socket budget is invalid")
    if not isinstance(provider_budget, ProviderBudgetScope):
        raise TypeError("Advanced Options provider budget is invalid")
    if not isinstance(owners, AdvancedOptionsOwnerNames):
        raise TypeError("Advanced Options owner names are invalid")
    if not isinstance(receipt_inputs, AdvancedOptionsReceiptInputs):
        raise TypeError("Advanced Options receipt inputs are invalid")
    if (
        authorized_cohort_id is not None
        and (
            not isinstance(authorized_cohort_id, UUID)
            or authorized_cohort_id.int == 0
        )
    ):
        raise ValueError("authorized Advanced Options cohort is invalid")
    if (
        authorized_plan_id is not None
        and (
            not isinstance(authorized_plan_id, UUID)
            or authorized_plan_id.int == 0
        )
    ):
        raise ValueError("authorized Advanced Options plan is invalid")

    evidence_sha256 = _sha256(
        options_entitlement_evidence_sha256,
        "options_entitlement_evidence_sha256",
    )
    approval = _text(
        options_authorization_reference,
        "options_authorization_reference",
    )
    checked = _utc(checked_at, "checked_at")
    expires = _utc(expires_at, "expires_at")
    planned = _utc(planned_at, "planned_at")
    slot_effective = _utc(slot_effective_at, "slot_effective_at")
    cutoff = receipt_inputs.cutoff
    if not checked <= planned <= cutoff < expires:
        raise ValueError("Advanced Options entitlement is stale or out of scope")
    if not equity_plan.checked_at <= planned <= cutoff < equity_plan.expires_at:
        raise ValueError("Advanced Equity entitlement is stale for Options")
    if (
        equity_plan.session_date != cutoff.date()
        or slot_effective.date() != equity_plan.session_date
        or slot_effective > cutoff
    ):
        raise ValueError("Advanced Options session clocks do not match")
    if (
        provider_budget.account_alias != ADVANCED_OPTIONS_ACCOUNT_ALIAS
        or provider_budget.session_date != equity_plan.session_date
        or provider_budget.authorization_reference != approval
        or provider_budget.lanes != ADVANCED_OPTIONS_REST_LANES
        or provider_budget.authorized_until < cutoff
    ):
        raise ValueError(
            "Advanced Options REST budget is not the canonical provider-ledger scope"
        )

    authorization = (
        runtime_authorization
        if runtime_authorization is not None
        else AdvancedOptionsRuntimeAuthorization()
    )
    if not isinstance(authorization, AdvancedOptionsRuntimeAuthorization):
        raise TypeError("Advanced Options runtime authorization is invalid")
    if authorization.any_authorized:
        raise ValueError("P1 does not authorize Advanced Options runtime actions")

    universe = UniverseManifest(
        version=(
            f"market-options-{len(equity_plan.universe.equity_tickers)}-"
            f"{len(option_underlyings)}-v1"
        ),
        equity_tickers=equity_plan.universe.equity_tickers,
        option_underlyings=option_underlyings,
    )
    if not universe.option_underlyings:
        raise ValueError("Advanced Options requires exact underlying membership")
    if (
        receipt_inputs.reference_state.session_date
        != equity_plan.session_date
        or receipt_inputs.reference_state.underlyings
        != universe.option_underlyings
        or receipt_inputs.open_interest_sha256
        != receipt_inputs.reference_state.open_interest_sha256
    ):
        raise ValueError("Advanced Options Tier A reference state changed")

    profile_identity = {
        "contract": "advanced-options-profile-v1",
        "stocks_profile_sha256": equity_plan.profile_sha256,
        "stocks_tier": entitlement.stocks_tier,
        "options_tier": entitlement.options_tier,
        "options_entitlement_evidence_sha256": evidence_sha256,
        "options_authorization_reference": approval,
        "checked_at": checked,
        "expires_at": expires,
        "feed_scope_sha256": feed_scope.sha256,
    }
    profile_sha256 = canonical_sha256(profile_identity)
    profile_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-profile:{profile_sha256}",
    )
    manifest_identity = {
        "contract": "market-options-manifest-v1",
        "source_equity_manifest_sha256": equity_plan.manifest.sha256,
        "profile_sha256": profile_sha256,
        "universe": universe,
        "manifest_policy_sha256": manifest_policy.sha256,
    }
    manifest_sha256 = canonical_sha256(manifest_identity)
    manifest_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:market-options-manifest:{manifest_sha256}",
    )

    provisional_slot_set = SlotSetVersion(
        cohort_id=_NIL_UUID,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=slot_effective,
        policy_sha256=pins.slot_set_policy_sha256,
        members=slot_members,
    )
    _validate_slots(provisional_slot_set, manifest_policy)
    slot_identity = _slot_set_identity(provisional_slot_set)
    slot_set_sha256 = canonical_sha256(slot_identity)
    cohort_identity = {
        "contract": "advanced-options-cohort-v1",
        "cohort_kind": ADVANCED_OPTIONS_COHORT_KIND,
        "profile_id": profile_id,
        "manifest_id": manifest_id,
        "manifest_sha256": manifest_sha256,
        "universe_sha256": universe.sha256,
        "feed_scope_sha256": feed_scope.sha256,
        "manifest_policy_sha256": manifest_policy.sha256,
        "pins_sha256": pins.sha256,
        "socket_budget_sha256": socket_budget.sha256,
        "provider_budget_sha256": provider_budget.sha256,
        "owners_sha256": owners.sha256,
        "slot_set_sha256": slot_set_sha256,
        "effective_at": planned,
        "mode": "SHADOW",
    }
    cohort_sha256 = canonical_sha256(cohort_identity)
    cohort_id = authorized_cohort_id or uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-cohort:{cohort_sha256}",
    )
    if cohort_id == equity_plan.cohort_id:
        raise ValueError("Advanced Options cannot relabel the Equity-only cohort")
    slot_set = SlotSetVersion(
        cohort_id=cohort_id,
        version=provisional_slot_set.version,
        kind=provisional_slot_set.kind,
        effective_at=provisional_slot_set.effective_at,
        policy_sha256=provisional_slot_set.policy_sha256,
        members=provisional_slot_set.members,
    )

    capabilities = product_capabilities(
        entitlement.stocks_tier,
        entitlement.options_tier,
    )
    disabled_detectors = product_disabled_detectors(
        entitlement.stocks_tier,
        entitlement.options_tier,
    )
    binding = TierCohortBinding(
        cohort_id=cohort_id,
        profile_id=profile_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        ready_state=TierState.ADVANCED_READY,
        source_manifest_sha256=manifest_sha256,
        valuation_policy_sha256=pins.valuation_policy_sha256,
        capabilities=capabilities,
        disabled_detectors=disabled_detectors,
        effective_at=planned,
        stocks_tier=ProviderTier.MASSIVE_ADVANCED,
        options_tier=ProviderTier.MASSIVE_ADVANCED,
        options_enabled=True,
    )
    source_flow = build_advanced_options_source_flow_binding(
        journal_generation=journal_generation,
        stream_session_ids=stream_session_ids,
        pins=pins,
    )
    control_identity = {
        "contract": "advanced-options-control-plane-v1",
        "session_date": equity_plan.session_date,
        "profile_sha256": profile_sha256,
        "manifest_sha256": manifest_sha256,
        "cohort_sha256": cohort_sha256,
        "binding_sha256": binding.sha256,
        "feed_scope_sha256": feed_scope.sha256,
        "slot_set_sha256": slot_set_sha256,
        "pins_sha256": pins.sha256,
        "socket_budget_sha256": socket_budget.sha256,
        "provider_budget_sha256": provider_budget.sha256,
        "owners_sha256": owners.sha256,
        "source_flow_sha256": source_flow.source_flow_sha256,
        "runtime_authorization_sha256": authorization.sha256,
        "stage_catalog_versions": PRESERVED_STAGE_CATALOGS,
        "runtime_enabled": False,
        "options_cycle_bound": False,
    }
    control_sha256 = canonical_sha256(control_identity)

    input_sha256 = canonical_sha256({
        "contract": "options-input-ready-v1",
        "control_sha256": control_sha256,
        "cohort_id": cohort_id,
        "profile_sha256": profile_sha256,
        "manifest_sha256": manifest_sha256,
        "active_slot_set": slot_identity,
        "reference_state_id": receipt_inputs.reference_state.state_id,
        "reference_state_sha256": receipt_inputs.reference_state.state_sha256,
        "reference_known_at": receipt_inputs.reference_state.known_at,
        "reference_valid_until": receipt_inputs.reference_state.valid_until,
        "discovery_batches_sha256": receipt_inputs.discovery_batches_sha256,
        "catalog_sha256": pins.catalog_sha256,
        "open_interest_sha256": receipt_inputs.open_interest_sha256,
        "valuation_policy_sha256": pins.valuation_policy_sha256,
        "model_bundle_sha256": pins.model_bundle_sha256,
        "condition_semantics_sha256": pins.condition_semantics_sha256,
        "flow_policy_sha256": pins.flow_policy_sha256,
        "source_flow_sha256": source_flow.source_flow_sha256,
        "consumer_checkpoints_sha256": (
            receipt_inputs.consumer_checkpoints_sha256
        ),
        "unresolved_gaps_sha256": receipt_inputs.unresolved_gaps_sha256,
        "market_state_sha256": receipt_inputs.market_state_sha256,
        "source_clocks_sha256": receipt_inputs.source_clocks_sha256,
        "equity_input_receipt_id": receipt_inputs.equity_input_receipt_id,
        "equity_input_sha256": receipt_inputs.equity_input_sha256,
    })
    input_receipt = _receipt_identity(
        cohort_id=cohort_id,
        spec=build_options_receipt_spec(
            receipt_kind=OPTIONS_INPUT_READY,
            cutoff=cutoff,
            reference_generation=receipt_inputs.reference_generation,
            universe_sha256=universe.sha256,
            input_sha256=input_sha256,
            build_id=receipt_inputs.build_id,
        ),
    )
    cycle_sha256 = canonical_sha256({
        "contract": "options-cycle-v1",
        "options_input_ready_sha256": input_receipt.receipt_sha256,
        "detector_outcomes_sha256": receipt_inputs.detector_outcomes_sha256,
        "model_bundle_sha256": pins.model_bundle_sha256,
        "valuation_policy_sha256": pins.valuation_policy_sha256,
    })
    cycle_receipt = _receipt_identity(
        cohort_id=cohort_id,
        spec=build_options_receipt_spec(
            receipt_kind=OPTIONS_CYCLE,
            cutoff=cutoff,
            reference_generation=receipt_inputs.reference_generation,
            universe_sha256=universe.sha256,
            input_sha256=cycle_sha256,
            build_id=receipt_inputs.build_id,
        ),
    )
    portal_sha256 = canonical_sha256({
        "contract": "options-portal-ready-v1",
        "options_cycle_sha256": cycle_receipt.receipt_sha256,
        "projection_sha256": receipt_inputs.projection_sha256,
        "publisher_owner": owners.publisher,
    })
    portal_receipt = _receipt_identity(
        cohort_id=cohort_id,
        spec=build_options_receipt_spec(
            receipt_kind=OPTIONS_PORTAL_READY,
            cutoff=cutoff,
            reference_generation=receipt_inputs.reference_generation,
            universe_sha256=universe.sha256,
            input_sha256=portal_sha256,
            build_id=receipt_inputs.build_id,
        ),
    )
    receipts = AdvancedOptionsReceiptSet(
        input_receipt,
        cycle_receipt,
        portal_receipt,
    )
    plan_identity = {
        "control_sha256": control_sha256,
        "receipt_ids": receipts.ids,
        "receipt_sha256s": (
            input_receipt.receipt_sha256,
            cycle_receipt.receipt_sha256,
            portal_receipt.receipt_sha256,
        ),
    }
    if authorized_plan_id is not None:
        plan_identity["authorized_plan_id"] = authorized_plan_id
    plan_sha256 = canonical_sha256(plan_identity)
    return AdvancedOptionsControlPlanePlan(
        plan_id=authorized_plan_id or uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-control-plane:{plan_sha256}",
        ),
        plan_sha256=plan_sha256,
        session_date=equity_plan.session_date,
        entitlement=entitlement,
        options_entitlement_evidence_sha256=evidence_sha256,
        options_authorization_reference=approval,
        checked_at=checked,
        expires_at=expires,
        planned_at=planned,
        profile_id=profile_id,
        profile_sha256=profile_sha256,
        universe=universe,
        manifest_id=manifest_id,
        manifest_sha256=manifest_sha256,
        cohort_kind=ADVANCED_OPTIONS_COHORT_KIND,
        cohort_id=cohort_id,
        cohort_sha256=cohort_sha256,
        binding=binding,
        manifest_policy=manifest_policy,
        feed_scope=feed_scope,
        slot_set=slot_set,
        slot_set_sha256=slot_set_sha256,
        pins=pins,
        socket_budget=socket_budget,
        provider_budget=provider_budget,
        owners=owners,
        source_flow=source_flow,
        receipts=receipts,
        runtime_authorization=authorization,
    )


def _enrollment_body(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    equity_plan: AdvancedEquityControlPlanePlan,
    activity_selection: OptionActivitySelectionRecord,
    equity_members_sha256: str,
) -> dict[str, object]:
    slot_set = control_plane.slot_set
    return {
        "session_date": control_plane.session_date.isoformat(),
        "planned_at": control_plane.planned_at.isoformat(),
        "checked_at": control_plane.checked_at.isoformat(),
        "expires_at": control_plane.expires_at.isoformat(),
        "source_equity": {
            "cohort_id": str(equity_plan.cohort_id),
            "cohort_sha256": equity_plan.cohort_sha256,
            "manifest_id": str(equity_plan.manifest_id),
            "manifest_sha256": equity_plan.manifest.sha256,
            "profile_id": str(equity_plan.profile_id),
            "profile_sha256": equity_plan.profile_sha256,
            "members_sha256": equity_members_sha256,
        },
        "market_options": {
            "plan_id": str(control_plane.plan_id),
            "plan_sha256": control_plane.plan_sha256,
            "profile_id": str(control_plane.profile_id),
            "profile_sha256": control_plane.profile_sha256,
            "manifest_id": str(control_plane.manifest_id),
            "manifest_sha256": control_plane.manifest_sha256,
            "cohort_id": str(control_plane.cohort_id),
            "cohort_sha256": control_plane.cohort_sha256,
            "universe_sha256": control_plane.universe.sha256,
            "option_underlyings": list(
                control_plane.universe.option_underlyings
            ),
            "manifest_policy_sha256": control_plane.manifest_policy.sha256,
            "pins_sha256": control_plane.pins.sha256,
            "source_flow_sha256": (
                control_plane.source_flow.source_flow_sha256
            ),
            "options_input_sha256": (
                control_plane.receipts.options_input_ready.spec.input_sha256
            ),
        },
        "slot_set": {
            "version": slot_set.version,
            "kind": slot_set.kind.value,
            "effective_at": slot_set.effective_at.isoformat(),
            "policy_sha256": slot_set.policy_sha256,
            "member_sha256": slot_set.member_sha256,
            "slot_set_sha256": control_plane.slot_set_sha256,
            "primary_contract_count": slot_set.primary_contract_count,
            "reserve_contract_count": slot_set.reserve_contract_count,
            "contract_count": slot_set.contract_count,
            "quote_ceiling": (
                control_plane.manifest_policy.maximum_option_quote_contracts
            ),
        },
        "activity_selection": option_activity_selection_payload(
            activity_selection
        ),
        "runtime_authorized": False,
        "effect_counts": {
            name: 0 for name in ADVANCED_OPTIONS_ZERO_EFFECTS
        },
    }


def _equity_am_members_sha256(
    equity_plan: AdvancedEquityControlPlanePlan,
) -> str:
    return canonical_sha256({
        "manifest_id": equity_plan.manifest_id,
        "manifest_sha256": equity_plan.manifest.sha256,
        "members": tuple(
            (
                ticker,
                security_id,
                MarketChannel.MINUTE_AGGREGATE,
                1,
                SlotSetPurpose.PRIMARY,
                False,
            )
            for ticker, security_id in equity_plan.security_ids
        ),
    })


def _validated_enrollment_identity(
    plan: AdvancedOptionsEnrollmentPlan,
) -> dict[str, object]:
    if not isinstance(plan, AdvancedOptionsEnrollmentPlan):
        raise TypeError("Advanced Options enrollment plan is invalid")
    control = plan.control_plane
    equity = plan.equity_plan
    selection = plan.activity_selection
    if (
        not isinstance(control, AdvancedOptionsControlPlanePlan)
        or not isinstance(equity, AdvancedEquityControlPlanePlan)
        or control.session_date != equity.session_date
        or control.universe.equity_tickers != equity.universe.equity_tickers
        or control.cohort_id == equity.cohort_id
        or control.runtime_authorization.any_authorized
        or control.runtime_enabled
        or control.options_cycle_bound
    ):
        raise ValueError("Advanced Options enrollment source plans diverged")
    expected_equity_members_sha256 = _equity_am_members_sha256(equity)
    if plan.equity_members_sha256 != expected_equity_members_sha256:
        raise ValueError("Advanced Equity AM membership changed")
    validate_option_activity_selection(
        selection,
        required_at=control.planned_at,
        expected_underlyings=control.universe.option_underlyings,
        expected_quote_ceiling=(
            control.manifest_policy.maximum_option_quote_contracts
        ),
    )
    selected = {
        (row.instrument_id, row.catalog_version_id)
        for row in selection.contracts
    }
    slot_identities: dict[tuple[int, int], set[MarketChannel]] = {}
    priorities: dict[tuple[int, int], set[int]] = {}
    for member in control.slot_set.members:
        if member.purpose is not SlotSetPurpose.PRIMARY:
            raise ValueError(
                "prospective Options enrollment permits primary slots only"
            )
        identity = (member.instrument_id, member.catalog_version_id)
        slot_identities.setdefault(identity, set()).add(member.channel)
        priorities.setdefault(identity, set()).add(member.priority)
    if (
        set(slot_identities) != selected
        or any(
            channels != set(ADVANCED_OPTIONS_CHANNELS)
            for channels in slot_identities.values()
        )
        or any(len(values) != 1 for values in priorities.values())
        or control.slot_set.version != 1
        or control.slot_set.kind is not SlotSetKind.PRE_SESSION
        or control.slot_set.reserve_contract_count != 0
        or control.slot_set.primary_contract_count != len(selection.contracts)
    ):
        raise ValueError(
            "activity selection and primary T/Q slot identities diverged"
        )
    body = _enrollment_body(
        control_plane=control,
        equity_plan=equity,
        activity_selection=selection,
        equity_members_sha256=expected_equity_members_sha256,
    )
    enrollment_sha256 = canonical_sha256(body)
    enrollment_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-enrollment:{enrollment_sha256}",
    )
    if (
        plan.enrollment_sha256 != enrollment_sha256
        or plan.enrollment_id != enrollment_id
    ):
        raise ValueError("Advanced Options enrollment identity changed")
    return body


def build_advanced_options_enrollment_plan(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    equity_plan: AdvancedEquityControlPlanePlan,
    activity_selection: OptionActivitySelectionRecord,
) -> AdvancedOptionsEnrollmentPlan:
    equity_members_sha256 = _equity_am_members_sha256(equity_plan)
    body = _enrollment_body(
        control_plane=control_plane,
        equity_plan=equity_plan,
        activity_selection=activity_selection,
        equity_members_sha256=equity_members_sha256,
    )
    enrollment_sha256 = canonical_sha256(body)
    plan = AdvancedOptionsEnrollmentPlan(
        enrollment_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-enrollment:{enrollment_sha256}",
        ),
        enrollment_sha256=enrollment_sha256,
        control_plane=control_plane,
        equity_plan=equity_plan,
        activity_selection=activity_selection,
        equity_members_sha256=equity_members_sha256,
    )
    _validated_enrollment_identity(plan)
    return plan


def advanced_options_enrollment_plan_payload(
    plan: AdvancedOptionsEnrollmentPlan,
) -> dict[str, object]:
    body = _validated_enrollment_identity(plan)
    return {
        "contract_version": ADVANCED_OPTIONS_ENROLLMENT_VERSION,
        "enrollment_id": str(plan.enrollment_id),
        "enrollment_sha256": plan.enrollment_sha256,
        **body,
    }


def validate_advanced_options_enrollment_plan_payload(
    value: object,
) -> dict[str, object]:
    expected_fields = {
        "contract_version",
        "enrollment_id",
        "enrollment_sha256",
        "session_date",
        "planned_at",
        "checked_at",
        "expires_at",
        "source_equity",
        "market_options",
        "slot_set",
        "activity_selection",
        "runtime_authorized",
        "effect_counts",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise ValueError(
            "Advanced Options enrollment plan fields are not exact"
        )
    if value["contract_version"] != ADVANCED_OPTIONS_ENROLLMENT_VERSION:
        raise ValueError(
            "Advanced Options enrollment contract version is invalid"
        )
    source_equity = value["source_equity"]
    market_options = value["market_options"]
    slot_set = value["slot_set"]
    if (
        not isinstance(source_equity, dict)
        or set(source_equity) != {
            "cohort_id",
            "cohort_sha256",
            "manifest_id",
            "manifest_sha256",
            "profile_id",
            "profile_sha256",
            "members_sha256",
        }
        or not isinstance(market_options, dict)
        or set(market_options) != {
            "plan_id",
            "plan_sha256",
            "profile_id",
            "profile_sha256",
            "manifest_id",
            "manifest_sha256",
            "cohort_id",
            "cohort_sha256",
            "universe_sha256",
            "option_underlyings",
            "manifest_policy_sha256",
            "pins_sha256",
            "source_flow_sha256",
            "options_input_sha256",
        }
        or not isinstance(slot_set, dict)
        or set(slot_set) != {
            "version",
            "kind",
            "effective_at",
            "policy_sha256",
            "member_sha256",
            "slot_set_sha256",
            "primary_contract_count",
            "reserve_contract_count",
            "contract_count",
            "quote_ceiling",
        }
    ):
        raise ValueError(
            "Advanced Options enrollment identities are not exact"
        )
    planned_at = _utc(
        datetime.fromisoformat(
            str(value["planned_at"]).replace("Z", "+00:00")
        ),
        "planned_at",
    )
    option_underlyings = tuple(market_options["option_underlyings"])
    selection = option_activity_selection_from_payload(
        value["activity_selection"],
        required_at=planned_at,
        expected_underlyings=option_underlyings,
        expected_quote_ceiling=slot_set["quote_ceiling"],
    )
    if (
        value["runtime_authorized"] is not False
        or value["effect_counts"]
        != {name: 0 for name in ADVANCED_OPTIONS_ZERO_EFFECTS}
        or slot_set["version"] != 1
        or slot_set["kind"] != SlotSetKind.PRE_SESSION.value
        or slot_set["reserve_contract_count"] != 0
        or slot_set["primary_contract_count"] != len(selection.contracts)
        or slot_set["contract_count"] != len(selection.contracts)
    ):
        raise ValueError(
            "Advanced Options enrollment is not a zero-effect primary plan"
        )
    body = {
        key: value[key]
        for key in expected_fields
        if key not in {
            "contract_version",
            "enrollment_id",
            "enrollment_sha256",
        }
    }
    body["activity_selection"] = option_activity_selection_payload(selection)
    enrollment_sha256 = canonical_sha256(body)
    enrollment_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-enrollment:{enrollment_sha256}",
    )
    if (
        value["enrollment_sha256"] != enrollment_sha256
        or value["enrollment_id"] != str(enrollment_id)
    ):
        raise ValueError("Advanced Options enrollment payload diverged")
    return {
        "contract_version": ADVANCED_OPTIONS_ENROLLMENT_VERSION,
        "enrollment_id": str(enrollment_id),
        "enrollment_sha256": enrollment_sha256,
        **body,
    }


def _verify_source_equity_cursor(
    cursor,
    plan: AdvancedOptionsEnrollmentPlan,
) -> tuple[tuple[int, str, int, str, str], ...]:
    equity = plan.equity_plan
    cursor.execute(
        "SELECT cohort.cohort_sha256, cohort.manifest_id::text, "
        "manifest.manifest_sha256, manifest.manifest_kind, "
        "profile.profile_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "WHERE cohort.cohort_id=%s",
        (str(equity.cohort_id),),
    )
    if cursor.fetchone() != (
        equity.cohort_sha256,
        str(equity.manifest_id),
        equity.manifest.sha256,
        "EQUITY_VALIDATION",
        equity.profile_sha256,
    ):
        raise ValueError("source Advanced Equity cohort identity changed")
    cursor.execute(
        "SELECT member.instrument_id, member.channel, member.priority, "
        "member.purpose, instrument.security_id::text "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "WHERE member.manifest_id=%s AND member.option_underlying=false "
        "ORDER BY instrument.security_id, member.channel",
        (str(equity.manifest_id),),
    )
    rows = tuple(
        (
            int(instrument_id),
            str(channel),
            int(priority),
            str(purpose),
            str(security_id),
        )
        for instrument_id, channel, priority, purpose, security_id
        in cursor.fetchall()
    )
    security_ids = {str(value) for _, value in equity.security_ids}
    expected_members = {
        (
            MarketChannel.MINUTE_AGGREGATE.value,
            1,
            SlotSetPurpose.PRIMARY.value,
            str(security_id),
        )
        for _, security_id in equity.security_ids
    }
    if (
        len(rows) != len(equity.manifest.members)
        or {row[4] for row in rows} != security_ids
        or {(row[1], row[2], row[3], row[4]) for row in rows}
        != expected_members
    ):
        raise ValueError("source Advanced Equity AM membership changed")
    return rows


def _verify_activity_catalog_cursor(
    cursor,
    plan: AdvancedOptionsEnrollmentPlan,
) -> None:
    selection = plan.activity_selection
    cursor.execute(
        "SELECT instrument.instrument_id, contract.contract_id, "
        "version.version_id, contract.contract_ticker, "
        "contract.underlying_security_id::text, version.call_put, "
        "version.expiration, version.strike, version.exercise_style, "
        "version.multiplier, version.deliverables, version.eligibility, "
        "version.effective_from, version.effective_to, "
        "version.observed_at, version.payload_sha256 "
        "FROM ref.option_contract_versions AS version "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=version.contract_id "
        "JOIN rt.instruments AS instrument "
        "ON instrument.contract_id=contract.contract_id "
        "WHERE version.version_id=ANY(%s)",
        ([row.catalog_version_id for row in selection.contracts],),
    )
    stored = {
        int(row[2]): row
        for row in cursor.fetchall()
    }
    security_by_ticker = {
        ticker: str(security_id)
        for ticker, security_id in plan.equity_plan.security_ids
    }
    if len(stored) != len(selection.contracts):
        raise ValueError("activity selection catalog identities are incomplete")
    for selected in selection.contracts:
        row = stored[selected.catalog_version_id]
        actual = (
            int(row[0]),
            int(row[1]),
            int(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            row[6],
            Decimal(row[7]),
            str(row[8]),
            Decimal(row[9]),
            tuple(row[10]),
            str(row[11]),
            _utc(row[12], "catalog effective_from"),
            (
                None
                if row[13] is None
                else _utc(row[13], "catalog effective_to")
            ),
            _utc(row[14], "catalog observed_at"),
            str(row[15]),
        )
        expected = (
            selected.instrument_id,
            selected.contract_id,
            selected.catalog_version_id,
            selected.contract_ticker,
            security_by_ticker[selected.underlyer],
            selected.call_put,
            selected.expiration_date,
            selected.strike,
            selected.exercise_style,
            selected.multiplier,
            selected.deliverables,
            selected.eligibility,
            selected.catalog_effective_from,
            selected.catalog_effective_to,
            selected.catalog_observed_at,
            selected.catalog_payload_sha256,
        )
        if actual != expected:
            raise ValueError(
                "activity selection catalog identity changed or is adjusted"
            )


def persist_advanced_options_history_binding_cursor(
    cursor,
    binding: AdvancedOptionsHistoryBinding,
) -> None:
    advanced_options_history_binding_payload(binding)
    cursor.execute(
        "SELECT manifest.manifest_sha256,profile.provider_tier "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "WHERE cohort.cohort_id=%s",
        (str(binding.source_cohort_id),),
    )
    if cursor.fetchone() != (
        binding.source_manifest_sha256,
        binding.source_provider_tier.value,
    ):
        raise ValueError(
            "Advanced Options history source identity changed"
        )
    cursor.execute(
        "SELECT profile.provider_tier "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "WHERE cohort.cohort_id=%s",
        (str(binding.target_cohort_id),),
    )
    if cursor.fetchone() != (ProviderTier.MASSIVE_ADVANCED.value,):
        raise ValueError(
            "Advanced Options history target identity changed"
        )
    cursor.execute(
        "INSERT INTO rt.cohort_history_bindings ("
        "target_cohort_id,source_cohort_id,source_provider_tier,"
        "history_before,source_manifest_sha256,authorization_reference,"
        "binding_sha256) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (target_cohort_id) DO NOTHING",
        (
            str(binding.target_cohort_id),
            str(binding.source_cohort_id),
            binding.source_provider_tier.value,
            binding.history_before,
            binding.source_manifest_sha256,
            binding.authorization_reference,
            binding.binding_sha256,
        ),
    )
    cursor.execute(
        "SELECT source_cohort_id::text,source_provider_tier,"
        "history_before,source_manifest_sha256,authorization_reference,"
        "binding_sha256 FROM rt.cohort_history_bindings "
        "WHERE target_cohort_id=%s",
        (str(binding.target_cohort_id),),
    )
    if cursor.fetchone() != (
        str(binding.source_cohort_id),
        binding.source_provider_tier.value,
        binding.history_before,
        binding.source_manifest_sha256,
        binding.authorization_reference,
        binding.binding_sha256,
    ):
        raise ValueError(
            "persisted Advanced Options history binding diverged"
        )


def persist_advanced_options_enrollment_cursor(
    cursor,
    plan: AdvancedOptionsEnrollmentPlan,
    *,
    history_binding: AdvancedOptionsHistoryBinding | None = None,
) -> bool:
    _validated_enrollment_identity(plan)
    control = plan.control_plane
    if (
        history_binding is not None
        and history_binding.target_cohort_id != control.cohort_id
    ):
        raise ValueError(
            "Advanced Options history binding targets another cohort"
        )
    equity_rows = _verify_source_equity_cursor(cursor, plan)
    _verify_activity_catalog_cursor(cursor, plan)
    capabilities_sha256 = canonical_sha256(tuple(sorted(
        capability.value for capability in control.binding.capabilities
    )))
    cursor.execute(
        "INSERT INTO rt.capability_profiles ("
        "profile_id,config_sha256,schema_sha256,account_alias,"
        "capabilities_sha256,universe_sha256,checked_at,expires_at,"
        "status,profile_sha256,provider_tier,security_policy_sha256,"
        "preflight_mode,provider_requests,action_authorized) VALUES ("
        "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'PASS',%s,"
        "'MASSIVE_ADVANCED',%s,'PASSIVE',false,false) "
        "ON CONFLICT (profile_id) DO NOTHING",
        (
            str(control.profile_id),
            plan.enrollment_sha256,
            plan.equity_plan.schema_sha256,
            capabilities_sha256,
            control.universe.sha256,
            control.checked_at,
            control.expires_at,
            control.profile_sha256,
            plan.equity_plan.security_policy_sha256,
        ),
    )
    cursor.execute(
        "SELECT config_sha256,schema_sha256,account_alias,"
        "capabilities_sha256,universe_sha256,checked_at,expires_at,"
        "status,profile_sha256,provider_tier,security_policy_sha256,"
        "preflight_mode,provider_requests,action_authorized "
        "FROM rt.capability_profiles WHERE profile_id=%s",
        (str(control.profile_id),),
    )
    if cursor.fetchone() != (
        plan.enrollment_sha256,
        plan.equity_plan.schema_sha256,
        "MASSIVE_ADVANCED",
        capabilities_sha256,
        control.universe.sha256,
        control.checked_at,
        control.expires_at,
        "PASS",
        control.profile_sha256,
        "MASSIVE_ADVANCED",
        plan.equity_plan.security_policy_sha256,
        "PASSIVE",
        False,
        False,
    ):
        raise ValueError("persisted Advanced Options profile diverged")
    capability_evidence = (
        (
            "options_entitlement",
            "options:entitlement",
            {
                "authorization_reference": (
                    control.options_authorization_reference
                ),
                "evidence_sha256": (
                    control.options_entitlement_evidence_sha256
                ),
                "provider_requests_by_action": 0,
            },
        ),
        (
            "options_activity_selection",
            "options:activity-selection",
            {
                "authorization_reference": (
                    plan.activity_selection.authorization_reference
                ),
                "selection_sha256": (
                    plan.activity_selection.selection_sha256
                ),
                "source_sha256": plan.activity_selection.source_sha256,
                "source_clocks_sha256": (
                    plan.activity_selection.source_clocks_sha256
                ),
                "provider_requests_by_action": 0,
            },
        ),
    )
    for check_name, scope_key, evidence in capability_evidence:
        cursor.execute(
            "INSERT INTO rt.capability_checks ("
            "profile_id,check_name,scope_key,verdict,required,"
            "reason_codes,observed_at,expires_at,evidence) VALUES ("
            "%s,%s,%s,'PASS',true,ARRAY[]::text[],%s,%s,%s::jsonb) "
            "ON CONFLICT DO NOTHING",
            (
                str(control.profile_id),
                check_name,
                scope_key,
                control.checked_at,
                control.expires_at,
                json.dumps(evidence, sort_keys=True),
            ),
        )
        cursor.execute(
            "SELECT verdict,required,reason_codes,observed_at,expires_at,"
            "evidence FROM rt.capability_checks "
            "WHERE profile_id=%s AND check_name=%s AND scope_key=%s",
            (str(control.profile_id), check_name, scope_key),
        )
        if cursor.fetchone() != (
            "PASS",
            True,
            [],
            control.checked_at,
            control.expires_at,
            evidence,
        ):
            raise ValueError(
                "persisted Advanced Options capability evidence diverged"
            )
    cursor.execute(
        "INSERT INTO rt.subscription_manifests ("
        "manifest_id,manifest_sha256,universe_sha256,session_date,"
        "effective_at,profile_id,manifest_kind,stock_count,"
        "option_underlying_count,member_count,allocation_policy_sha256"
        ") VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',%s,%s,%s,%s) "
        "ON CONFLICT (manifest_id) DO NOTHING",
        (
            str(control.manifest_id),
            control.manifest_sha256,
            control.universe.sha256,
            control.session_date,
            control.planned_at,
            str(control.profile_id),
            len(control.universe.equity_tickers),
            len(control.universe.option_underlyings),
            len(equity_rows),
            control.manifest_policy.sha256,
        ),
    )
    cursor.execute(
        "SELECT manifest_sha256,universe_sha256,session_date,effective_at,"
        "profile_id::text,manifest_kind,stock_count,"
        "option_underlying_count,member_count,allocation_policy_sha256 "
        "FROM rt.subscription_manifests WHERE manifest_id=%s",
        (str(control.manifest_id),),
    )
    if cursor.fetchone() != (
        control.manifest_sha256,
        control.universe.sha256,
        control.session_date,
        control.planned_at,
        str(control.profile_id),
        "MARKET_OPTIONS",
        len(control.universe.equity_tickers),
        len(control.universe.option_underlyings),
        len(equity_rows),
        control.manifest_policy.sha256,
    ):
        raise ValueError("persisted MARKET_OPTIONS manifest diverged")
    underlying_security_ids = {
        str(security_id)
        for ticker, security_id in plan.equity_plan.security_ids
        if ticker in control.universe.option_underlyings
    }
    for instrument_id, channel, priority, purpose, security_id in equity_rows:
        cursor.execute(
            "INSERT INTO rt.subscription_members ("
            "manifest_id,instrument_id,channel,priority,purpose,"
            "option_underlying) VALUES (%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(control.manifest_id),
                instrument_id,
                channel,
                priority,
                purpose,
                security_id in underlying_security_ids,
            ),
        )
    cursor.execute(
        "SELECT instrument_id,channel,priority,purpose,option_underlying "
        "FROM rt.subscription_members WHERE manifest_id=%s "
        "ORDER BY instrument_id,channel",
        (str(control.manifest_id),),
    )
    expected_members = tuple(sorted(
        (
            instrument_id,
            channel,
            priority,
            purpose,
            security_id in underlying_security_ids,
        )
        for instrument_id, channel, priority, purpose, security_id
        in equity_rows
    ))
    stored_members = tuple(
        (
            int(instrument_id),
            str(channel),
            int(priority),
            str(purpose),
            bool(option_underlying),
        )
        for instrument_id, channel, priority, purpose, option_underlying
        in cursor.fetchall()
    )
    if stored_members != expected_members:
        raise ValueError("persisted MARKET_OPTIONS membership diverged")
    cursor.execute(
        "INSERT INTO rt.cohorts ("
        "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
        "model_bundle_sha256,valuation_sha256,effective_at,mode"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW') "
        "ON CONFLICT (cohort_id) DO NOTHING",
        (
            str(control.cohort_id),
            control.cohort_sha256,
            str(control.manifest_id),
            plan.enrollment_sha256,
            control.pins.model_bundle_sha256,
            control.pins.valuation_policy_sha256,
            control.planned_at,
        ),
    )
    cursor.execute(
        "SELECT cohort_sha256,manifest_id::text,configuration_sha256,"
        "model_bundle_sha256,valuation_sha256,effective_at,mode "
        "FROM rt.cohorts WHERE cohort_id=%s",
        (str(control.cohort_id),),
    )
    if cursor.fetchone() != (
        control.cohort_sha256,
        str(control.manifest_id),
        plan.enrollment_sha256,
        control.pins.model_bundle_sha256,
        control.pins.valuation_policy_sha256,
        control.planned_at,
        "SHADOW",
    ):
        raise ValueError("persisted MARKET_OPTIONS cohort diverged")
    persist_tier_cohort_binding_cursor(cursor, control.binding)
    moved = persist_and_activate_slot_set_cursor(
        cursor,
        control.slot_set,
        activated_at=control.slot_set.effective_at,
    )
    if history_binding is not None:
        persist_advanced_options_history_binding_cursor(
            cursor,
            history_binding,
        )
    return moved


def persist_advanced_options_enrollment(
    connection,
    plan: AdvancedOptionsEnrollmentPlan,
    *,
    history_binding: AdvancedOptionsHistoryBinding | None = None,
) -> bool:
    try:
        with connection.cursor() as cursor:
            moved = persist_advanced_options_enrollment_cursor(
                cursor,
                plan,
                history_binding=history_binding,
            )
        connection.commit()
        return moved
    except Exception:
        connection.rollback()
        raise
