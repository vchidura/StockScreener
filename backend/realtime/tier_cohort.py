from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from options.domain import DataCapability

from .contracts import canonical_sha256
from .preflight import (
    DetectorFamily,
    CapabilityVerdict,
    ProductEntitlementProfile,
    ProviderTier,
    TierState,
    TierTransition,
    product_scoped_detector_availability,
)


DEVELOPER_CAPABILITIES = frozenset({
    DataCapability.CHAIN_SNAPSHOT,
    DataCapability.OPTION_TRADES,
    DataCapability.UNDERLYING_PRICE,
})
ADVANCED_CAPABILITIES = frozenset({
    DataCapability.CHAIN_SNAPSHOT,
    DataCapability.OPTION_TRADES,
    DataCapability.OPTION_QUOTES,
    DataCapability.UNDERLYING_PRICE,
    DataCapability.REAL_TIME,
})
DEVELOPER_DISABLED_DETECTORS = frozenset({
    DetectorFamily.O2,
    DetectorFamily.AGGRESSIVE_PREMIUM_BURST,
    DetectorFamily.PERSISTENT_REALTIME_FLOW,
    DetectorFamily.SWEEP_LIKE_CLUSTER,
})


def product_capabilities(
    stocks_tier: ProviderTier,
    options_tier: ProviderTier | None,
) -> frozenset[DataCapability]:
    capabilities = {DataCapability.UNDERLYING_PRICE}
    if stocks_tier is ProviderTier.MASSIVE_ADVANCED:
        capabilities.add(DataCapability.REAL_TIME)
    if options_tier is not None:
        capabilities.update({
            DataCapability.CHAIN_SNAPSHOT,
            DataCapability.OPTION_TRADES,
        })
        if options_tier is ProviderTier.MASSIVE_ADVANCED:
            capabilities.update({
                DataCapability.OPTION_QUOTES,
                DataCapability.REAL_TIME,
            })
    return frozenset(capabilities)


def product_disabled_detectors(
    stocks_tier: ProviderTier,
    options_tier: ProviderTier | None,
) -> frozenset[DetectorFamily]:
    profile = ProductEntitlementProfile(stocks_tier, options_tier)
    return frozenset(
        detector
        for detector in DetectorFamily
        if product_scoped_detector_availability(
            detector,
            profile,
        ).verdict is not CapabilityVerdict.PASS
    )


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _database_value(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (tuple, list)):
        return [
            _database_value(item)
            for item in value
        ]
    return value


@dataclass(frozen=True, slots=True)
class TierCohortBinding:
    cohort_id: UUID
    profile_id: UUID
    provider_tier: ProviderTier
    ready_state: TierState
    source_manifest_sha256: str
    valuation_policy_sha256: str
    capabilities: frozenset[DataCapability]
    disabled_detectors: frozenset[DetectorFamily]
    effective_at: datetime
    stocks_tier: ProviderTier | None = None
    options_tier: ProviderTier | None = None
    options_enabled: bool = True

    def __post_init__(self) -> None:
        effective = _utc(self.effective_at, "effective_at")
        stocks_tier = self.stocks_tier or self.provider_tier
        options_tier = (
            self.options_tier or self.provider_tier
            if self.options_enabled
            else None
        )
        if stocks_tier is not self.provider_tier:
            raise ValueError("the publication tier must match the stocks tier")
        expected_capabilities = product_capabilities(
            stocks_tier,
            options_tier,
        )
        expected_disabled = product_disabled_detectors(
            stocks_tier,
            options_tier,
        )
        if self.provider_tier is ProviderTier.MASSIVE_DEVELOPER:
            if self.ready_state is not TierState.DEVELOPER_READY:
                raise ValueError("Developer cohort must bind DEVELOPER_READY")
            if self.capabilities != expected_capabilities:
                raise ValueError(
                    "Developer cohort capabilities must match the reduced contract"
                )
            if self.disabled_detectors != expected_disabled:
                raise ValueError(
                    "Developer cohort disabled detectors do not match its product tiers"
                )
        else:
            if self.ready_state is not TierState.ADVANCED_READY:
                raise ValueError("Advanced cohort must bind ADVANCED_READY")
            if self.capabilities != expected_capabilities:
                raise ValueError(
                    "Advanced cohort capabilities do not match its product tiers"
                )
            if self.disabled_detectors != expected_disabled:
                raise ValueError(
                    "Advanced cohort disabled detectors do not match its product tiers"
                )
        object.__setattr__(
            self,
            "source_manifest_sha256",
            _sha256(self.source_manifest_sha256, "source_manifest_sha256"),
        )
        object.__setattr__(
            self,
            "valuation_policy_sha256",
            _sha256(self.valuation_policy_sha256, "valuation_policy_sha256"),
        )
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "stocks_tier", stocks_tier)
        object.__setattr__(self, "options_tier", options_tier)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class TierCohortActivation:
    activation_id: UUID
    previous_cohort_id: UUID | None
    target: TierCohortBinding
    transitions: tuple[TierTransition, ...]
    activated_at: datetime
    approval_reference: str
    dependencies_warm: bool
    activation_sha256: str


def build_initial_developer_activation(
    target: TierCohortBinding,
    *,
    activated_at: datetime,
    approval_reference: str,
    dependencies_warm: bool,
) -> TierCohortActivation:
    activated = _utc(activated_at, "activated_at")
    approval = approval_reference.strip()
    if target.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
        raise ValueError("initial reduced-capability activation must target Developer")
    if not approval:
        raise ValueError("initial Developer activation requires an approval reference")
    if not dependencies_warm:
        raise ValueError("initial Developer activation requires warmed dependencies")
    if activated < target.effective_at:
        raise ValueError("Developer activation cannot precede cohort effective time")
    identity = {
        "previous_cohort_id": None,
        "target_sha256": target.sha256,
        "transitions": (),
        "activated_at": activated,
        "approval_reference": approval,
        "dependencies_warm": True,
    }
    digest = canonical_sha256(identity)
    return TierCohortActivation(
        activation_id=uuid5(NAMESPACE_URL, f"realtime-tier-activation:{digest}"),
        previous_cohort_id=None,
        target=target,
        transitions=(),
        activated_at=activated,
        approval_reference=approval,
        dependencies_warm=True,
        activation_sha256=digest,
    )


def build_tier_cohort_switch(
    current: TierCohortBinding,
    target: TierCohortBinding,
    transitions: tuple[TierTransition, ...],
    *,
    activated_at: datetime,
) -> TierCohortActivation:
    activated = _utc(activated_at, "activated_at")
    if current.cohort_id == target.cohort_id or current.profile_id == target.profile_id:
        raise ValueError("tier switch requires a distinct cohort and capability profile")
    if current.provider_tier is target.provider_tier:
        raise ValueError("tier switch target must use a different provider tier")
    if len(transitions) < 2:
        raise ValueError("tier switch requires approval and warmed-ready transitions")
    if transitions[0].previous is not current.ready_state:
        raise ValueError("tier switch transition chain does not start at the active state")
    if transitions[-1].target is not target.ready_state:
        raise ValueError("tier switch transition chain does not reach the target ready state")
    if any(
        left.target is not right.previous
        for left, right in zip(transitions, transitions[1:])
    ):
        raise ValueError("tier switch transitions are not contiguous")
    approvals = tuple(
        transition.operator_approval_reference
        for transition in transitions
        if transition.operator_approved
    )
    if not approvals:
        raise ValueError("tier switch requires explicit operator approval")
    if not transitions[-1].dependencies_warm:
        raise ValueError("tier switch requires warmed target dependencies")
    if activated < max(target.effective_at, transitions[-1].transitioned_at):
        raise ValueError("tier switch activation precedes target readiness")
    identity = {
        "previous_cohort_id": current.cohort_id,
        "target_sha256": target.sha256,
        "transition_sha256": tuple(row.transition_sha256 for row in transitions),
        "activated_at": activated,
        "approval_reference": approvals[-1],
        "dependencies_warm": True,
    }
    digest = canonical_sha256(identity)
    return TierCohortActivation(
        activation_id=uuid5(NAMESPACE_URL, f"realtime-tier-activation:{digest}"),
        previous_cohort_id=current.cohort_id,
        target=target,
        transitions=transitions,
        activated_at=activated,
        approval_reference=approvals[-1],
        dependencies_warm=True,
        activation_sha256=digest,
    )


def persist_tier_cohort_binding_cursor(cursor, target: TierCohortBinding) -> None:
    if not isinstance(target, TierCohortBinding):
        raise TypeError("tier binding persistence requires a validated native binding")
    cursor.execute(
                "INSERT INTO rt.provider_tier_cohorts ("
                "cohort_id, profile_id, provider_tier, stocks_tier, options_tier, ready_state, "
                "source_manifest_sha256, valuation_policy_sha256, capabilities, "
                "disabled_detectors, effective_at, binding_sha256) VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (cohort_id) DO NOTHING",
                (
                    str(target.cohort_id),
                    str(target.profile_id),
                    target.provider_tier.value,
                    target.stocks_tier.value,
                    (
                        target.options_tier.value
                        if target.options_tier is not None
                        else None
                    ),
                    target.ready_state.value,
                    target.source_manifest_sha256,
                    target.valuation_policy_sha256,
                    sorted(value.value for value in target.capabilities),
                    sorted(value.value for value in target.disabled_detectors),
                    target.effective_at,
                    target.sha256,
                ),
            )
    cursor.execute(
                "SELECT profile_id, provider_tier, stocks_tier, options_tier, ready_state, source_manifest_sha256, "
                "valuation_policy_sha256, capabilities, disabled_detectors, effective_at, "
                "binding_sha256 FROM rt.provider_tier_cohorts WHERE cohort_id=%s",
                (str(target.cohort_id),),
            )
    binding = cursor.fetchone()
    expected_binding = (
                str(target.profile_id),
                target.provider_tier.value,
                target.stocks_tier.value,
                (
                    target.options_tier.value
                    if target.options_tier is not None
                    else None
                ),
                target.ready_state.value,
                target.source_manifest_sha256,
                target.valuation_policy_sha256,
                sorted(value.value for value in target.capabilities),
                sorted(value.value for value in target.disabled_detectors),
                target.effective_at,
                target.sha256,
            )
    normalized_binding = tuple(_database_value(value) for value in binding) if binding is not None else None
    if normalized_binding != expected_binding:
        raise ValueError("persisted tier cohort binding conflict")


def persist_tier_cohort_activation(
    connection,
    activation: TierCohortActivation,
) -> None:
    target = activation.target
    try:
        with connection.cursor() as cursor:
            persist_tier_cohort_binding_cursor(cursor, target)
            cursor.execute(
                "SELECT cohort_id, provider_tier, activation_id, effective_at "
                "FROM rt.portal_cohort_current WHERE scope='PLATFORM' FOR UPDATE"
            )
            current = cursor.fetchone()
            if activation.previous_cohort_id is None:
                if current is not None and str(current[0]) != str(target.cohort_id):
                    raise ValueError("initial Developer activation found an existing cohort")
            elif current is None or str(current[0]) != str(activation.previous_cohort_id):
                raise ValueError("tier switch previous cohort does not match current portal state")
            cursor.execute(
                "INSERT INTO rt.provider_tier_activations ("
                "activation_id, previous_cohort_id, target_cohort_id, provider_tier, "
                "transition_ids, activated_at, approval_reference, dependencies_warm, "
                "activation_sha256) VALUES (%s, %s, %s, %s, %s::uuid[], %s, %s, true, %s) "
                "ON CONFLICT (activation_id) DO NOTHING",
                (
                    str(activation.activation_id),
                    str(activation.previous_cohort_id)
                    if activation.previous_cohort_id is not None else None,
                    str(target.cohort_id),
                    target.provider_tier.value,
                    [str(row.transition_id) for row in activation.transitions],
                    activation.activated_at,
                    activation.approval_reference,
                    activation.activation_sha256,
                ),
            )
            cursor.execute(
                "SELECT previous_cohort_id, target_cohort_id, provider_tier, transition_ids::text[], "
                "activated_at, approval_reference, dependencies_warm, activation_sha256 "
                "FROM rt.provider_tier_activations WHERE activation_id=%s",
                (str(activation.activation_id),),
            )
            stored = cursor.fetchone()
            expected_activation = (
                str(activation.previous_cohort_id)
                if activation.previous_cohort_id is not None else None,
                str(target.cohort_id),
                target.provider_tier.value,
                [str(row.transition_id) for row in activation.transitions],
                activation.activated_at,
                activation.approval_reference,
                True,
                activation.activation_sha256,
            )
            normalized_activation = (
                tuple(_database_value(value) for value in stored)
                if stored is not None else None
            )
            if normalized_activation != expected_activation:
                raise ValueError("persisted tier cohort activation conflict")
            cursor.execute(
                "INSERT INTO rt.portal_cohort_current ("
                "scope, cohort_id, provider_tier, activation_id, effective_at) "
                "VALUES ('PLATFORM', %s, %s, %s, %s) ON CONFLICT (scope) DO UPDATE SET "
                "cohort_id=EXCLUDED.cohort_id, provider_tier=EXCLUDED.provider_tier, "
                "activation_id=EXCLUDED.activation_id, effective_at=EXCLUDED.effective_at, "
                "updated_at=clock_timestamp() WHERE "
                "rt.portal_cohort_current.cohort_id=%s "
                "AND rt.portal_cohort_current.effective_at < EXCLUDED.effective_at "
                "RETURNING activation_id",
                (
                    str(target.cohort_id),
                    target.provider_tier.value,
                    str(activation.activation_id),
                    activation.activated_at,
                    str(activation.previous_cohort_id)
                    if activation.previous_cohort_id is not None else str(target.cohort_id),
                ),
            )
            advanced = cursor.fetchone()
            if advanced is None and not (
                current is not None and str(current[2]) == str(activation.activation_id)
            ):
                raise ValueError("portal cohort pointer advancement was rejected")
        connection.commit()
    except Exception:
        connection.rollback()
        raise