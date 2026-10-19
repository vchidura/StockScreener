"""Pure immutable contracts for a prospective Advanced Options control plane."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
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
from .option_session_manifest import BoundedOptionManifestPolicy
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
class AdvancedOptionsReceiptInputs:
    cutoff: datetime
    reference_state: AdvancedOptionsReferenceState
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
                AdvancedOptionsReferenceState,
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
    cohort_id = uuid5(
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
    plan_sha256 = canonical_sha256({
        "control_sha256": control_sha256,
        "receipt_ids": receipts.ids,
        "receipt_sha256s": (
            input_receipt.receipt_sha256,
            cycle_receipt.receipt_sha256,
            portal_receipt.receipt_sha256,
        ),
    })
    return AdvancedOptionsControlPlanePlan(
        plan_id=uuid5(
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
