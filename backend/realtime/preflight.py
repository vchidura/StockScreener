from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    CapabilityCheck,
    CapabilityVerdict,
    ReadinessAssessment,
    ReadinessStatus,
    SubscriptionManifest,
    UniverseManifest,
    assess_readiness,
    canonical_sha256,
)
from .migrations import EXPECTED_DATABASE_NAME, load_realtime_migrations


class ProviderTier(str, Enum):
    MASSIVE_ADVANCED = "MASSIVE_ADVANCED"
    MASSIVE_DEVELOPER = "MASSIVE_DEVELOPER"


EXPECTED_SCHEMA_VERSIONS = tuple(
    migration.version for migration in load_realtime_migrations()
)


class DetectorFamily(str, Enum):
    STOCK_INTRADAY = "STOCK_INTRADAY"
    STOCK_SWING = "STOCK_SWING"
    O1 = "O1"
    O2 = "O2"
    O3 = "O3"
    S1 = "S1"
    S2 = "S2"
    AGGRESSIVE_PREMIUM_BURST = "AGGRESSIVE_PREMIUM_BURST"
    PERSISTENT_REALTIME_FLOW = "PERSISTENT_REALTIME_FLOW"
    SWEEP_LIKE_CLUSTER = "SWEEP_LIKE_CLUSTER"


PORTABLE_DETECTORS = frozenset(
    {
        DetectorFamily.STOCK_INTRADAY,
        DetectorFamily.STOCK_SWING,
        DetectorFamily.O1,
        DetectorFamily.O3,
        DetectorFamily.S1,
        DetectorFamily.S2,
    }
)


class TierState(str, Enum):
    ADVANCED_READY = "ADVANCED_READY"
    ADVANCED_BLOCKED = "ADVANCED_BLOCKED"
    FALLBACK_PENDING_APPROVAL = "FALLBACK_PENDING_APPROVAL"
    DEVELOPER_WARMING = "DEVELOPER_WARMING"
    DEVELOPER_READY = "DEVELOPER_READY"
    ADVANCED_WARMING = "ADVANCED_WARMING"


@dataclass(frozen=True, slots=True)
class DetectorAvailability:
    detector: DetectorFamily
    tier: ProviderTier
    verdict: CapabilityVerdict
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProductEntitlementProfile:
    stocks_tier: ProviderTier | None
    options_tier: ProviderTier | None

    @property
    def advanced_platform_ready(self) -> bool:
        return (
            self.stocks_tier is ProviderTier.MASSIVE_ADVANCED
            and self.options_tier is ProviderTier.MASSIVE_ADVANCED
        )


@dataclass(frozen=True, slots=True)
class ProductScopedDetectorAvailability:
    detector: DetectorFamily
    verdict: CapabilityVerdict
    realtime_qualified: bool
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PassiveSchemaEvidence:
    database_name: str
    applied_versions: tuple[str, ...]
    schema_sha256: str
    security_policy_sha256: str
    role_audit_passed: bool

    def __post_init__(self) -> None:
        if not self.database_name.strip():
            raise ValueError("database_name cannot be blank")
        if not self.applied_versions:
            raise ValueError("applied_versions cannot be empty")
        for value in (self.schema_sha256, self.security_policy_sha256):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError("schema and security policy hashes must be SHA-256")


@dataclass(frozen=True, slots=True)
class PassiveHostEvidence:
    local_filesystem: bool
    journal_mode: str
    journal_synchronous: int
    planned_journal_bytes: int
    usable_disk_bytes: int
    clock_verified: bool

    def __post_init__(self) -> None:
        if self.planned_journal_bytes <= 0 or self.usable_disk_bytes <= 0:
            raise ValueError("journal and disk byte budgets must be positive")


@dataclass(frozen=True, slots=True)
class PassivePreflightConfiguration:
    provider_tier: ProviderTier
    account_alias: str
    universe: UniverseManifest
    expected_schema_sha256: str
    expected_security_policy_sha256: str
    advanced_subscription: SubscriptionManifest | None = None
    observation_only: bool = True
    publication_enabled: bool = False
    broker_enabled: bool = False
    profile_ttl_seconds: int = 3600

    def __post_init__(self) -> None:
        if not self.account_alias.strip():
            raise ValueError("account_alias cannot be blank")
        for value in (
            self.expected_schema_sha256,
            self.expected_security_policy_sha256,
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError("expected policy hashes must be SHA-256")
        if type(self.profile_ttl_seconds) is not int or self.profile_ttl_seconds <= 0:
            raise ValueError("profile_ttl_seconds must be positive")
        if self.provider_tier is ProviderTier.MASSIVE_ADVANCED:
            if self.advanced_subscription is None:
                raise ValueError("Advanced preflight requires a subscription manifest")
            if self.advanced_subscription.universe != self.universe:
                raise ValueError("Advanced subscription universe does not match preflight")
        elif self.advanced_subscription is not None:
            raise ValueError("Developer preflight cannot claim an Advanced subscription")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class PassivePreflightEvidence:
    schema: PassiveSchemaEvidence
    host: PassiveHostEvidence
    entitlement: CapabilityCheck | None = None


@dataclass(frozen=True, slots=True)
class PreflightProfile:
    profile_id: UUID
    profile_sha256: str
    configuration_sha256: str
    provider_tier: ProviderTier
    account_alias: str
    schema_sha256: str
    security_policy_sha256: str
    universe_sha256: str
    capabilities_sha256: str
    checked_at: datetime
    expires_at: datetime
    verdict: CapabilityVerdict
    readiness: ReadinessAssessment
    checks: tuple[CapabilityCheck, ...]
    provider_requests: bool = False
    action_authorized: bool = False


@dataclass(frozen=True, slots=True)
class TierTransition:
    transition_id: UUID
    previous: TierState
    target: TierState
    transitioned_at: datetime
    operator_approved: bool
    operator_approval_reference: str | None
    dependencies_warm: bool
    reason_code: str
    transition_sha256: str


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _check(
    name: str,
    verdict: CapabilityVerdict,
    now: datetime,
    expires_at: datetime,
    *reasons: str,
    required: bool = True,
) -> CapabilityCheck:
    return CapabilityCheck(
        check_name=name,
        scope_key="platform",
        verdict=verdict,
        observed_at=now,
        expires_at=expires_at,
        required=required,
        reason_codes=tuple(reasons),
        evidence_reference=f"passive:{name}",
    )


def _condition_check(
    name: str,
    passed: bool,
    now: datetime,
    expires_at: datetime,
    failure_reason: str,
) -> CapabilityCheck:
    return _check(
        name,
        CapabilityVerdict.PASS if passed else CapabilityVerdict.FAIL,
        now,
        expires_at,
        *((failure_reason,) if not passed else ()),
    )


def run_passive_preflight(
    configuration: PassivePreflightConfiguration,
    evidence: PassivePreflightEvidence,
    *,
    checked_at: datetime,
) -> PreflightProfile:
    now = _utc(checked_at, "checked_at")
    expires_at = now + timedelta(seconds=configuration.profile_ttl_seconds)
    checks = [
        _condition_check(
            "database_target",
            evidence.schema.database_name == EXPECTED_DATABASE_NAME,
            now,
            expires_at,
            "WRONG_DATABASE_TARGET",
        ),
        _condition_check(
            "schema_versions",
            evidence.schema.applied_versions == EXPECTED_SCHEMA_VERSIONS,
            now,
            expires_at,
            "SCHEMA_VERSION_MISMATCH",
        ),
        _condition_check(
            "schema_fingerprint",
            evidence.schema.schema_sha256 == configuration.expected_schema_sha256,
            now,
            expires_at,
            "SCHEMA_FINGERPRINT_MISMATCH",
        ),
        _condition_check(
            "security_policy",
            evidence.schema.security_policy_sha256
            == configuration.expected_security_policy_sha256
            and evidence.schema.role_audit_passed,
            now,
            expires_at,
            "SECURITY_POLICY_OR_GRANTS_MISMATCH",
        ),
        _condition_check(
            "journal_durability",
            evidence.host.local_filesystem
            and evidence.host.journal_mode.lower() == "wal"
            and evidence.host.journal_synchronous == 2,
            now,
            expires_at,
            "JOURNAL_DURABILITY_NOT_VERIFIED",
        ),
        _condition_check(
            "disk_capacity",
            evidence.host.planned_journal_bytes
            <= int(evidence.host.usable_disk_bytes * 0.70),
            now,
            expires_at,
            "DISK_HEADROOM_BELOW_30_PERCENT",
        ),
        _condition_check(
            "clock",
            evidence.host.clock_verified,
            now,
            expires_at,
            "CLOCK_NOT_VERIFIED",
        ),
        _condition_check(
            "action_mode",
            configuration.observation_only
            and not configuration.publication_enabled
            and not configuration.broker_enabled,
            now,
            expires_at,
            "PASSIVE_ACTION_BOUNDARY_VIOLATED",
        ),
    ]
    if evidence.entitlement is None:
        checks.append(
            _check(
                "entitlement",
                CapabilityVerdict.UNAVAILABLE,
                now,
                expires_at,
                "NO_ACTIVE_OR_RETAINED_ENTITLEMENT_EVIDENCE",
            )
        )
    else:
        checks.append(evidence.entitlement)
    checks.append(
        _check(
            "live_recency",
            CapabilityVerdict.UNAVAILABLE,
            now,
            expires_at,
            "PASSIVE_MODE_DOES_NOT_OBSERVE_LIVE_RECENCY",
        )
    )
    checks.append(
        _check(
            "active_probe",
            CapabilityVerdict.NOT_APPLICABLE,
            now,
            expires_at,
            "PASSIVE_MODE_NO_PROVIDER_ACCESS",
            required=False,
        )
    )
    check_tuple = tuple(checks)
    readiness = assess_readiness(check_tuple, now)
    effective = tuple(
        check.effective_verdict(now) for check in check_tuple if check.required
    )
    verdict = (
        CapabilityVerdict.FAIL
        if CapabilityVerdict.FAIL in effective
        else CapabilityVerdict.EXPIRED
        if CapabilityVerdict.EXPIRED in effective
        else CapabilityVerdict.UNAVAILABLE
        if CapabilityVerdict.UNAVAILABLE in effective
        else CapabilityVerdict.PASS
    )
    identity = {
        "configuration_sha256": configuration.sha256,
        "provider_tier": configuration.provider_tier,
        "checked_at": now,
        "checks": check_tuple,
    }
    profile_sha256 = canonical_sha256(identity)
    capabilities_sha256 = canonical_sha256(
        tuple(
            detector_availability(detector, configuration.provider_tier)
            for detector in DetectorFamily
        )
    )
    return PreflightProfile(
        profile_id=uuid5(NAMESPACE_URL, f"realtime-preflight:{profile_sha256}"),
        profile_sha256=profile_sha256,
        configuration_sha256=configuration.sha256,
        provider_tier=configuration.provider_tier,
        account_alias=configuration.account_alias.strip(),
        schema_sha256=evidence.schema.schema_sha256,
        security_policy_sha256=evidence.schema.security_policy_sha256,
        universe_sha256=configuration.universe.sha256,
        capabilities_sha256=capabilities_sha256,
        checked_at=now,
        expires_at=min(check.expires_at for check in check_tuple),
        verdict=verdict,
        readiness=readiness,
        checks=check_tuple,
    )


def detector_availability(
    detector: DetectorFamily,
    tier: ProviderTier,
) -> DetectorAvailability:
    if tier is ProviderTier.MASSIVE_ADVANCED:
        return DetectorAvailability(detector, tier, CapabilityVerdict.PASS, ())
    if detector is DetectorFamily.O2:
        return DetectorAvailability(
            detector,
            tier,
            CapabilityVerdict.UNAVAILABLE,
            ("DEVELOPER_O2_DISABLED_BY_POLICY",),
        )
    if detector in PORTABLE_DETECTORS:
        return DetectorAvailability(
            detector,
            tier,
            CapabilityVerdict.PASS,
            ("DEVELOPER_DELAYED_OR_SNAPSHOT_CONTRACT",),
        )
    return DetectorAvailability(
        detector,
        tier,
        CapabilityVerdict.UNAVAILABLE,
        ("ADVANCED_REALTIME_QUOTES_REQUIRED",),
    )


def product_scoped_detector_availability(
    detector: DetectorFamily,
    profile: ProductEntitlementProfile,
) -> ProductScopedDetectorAvailability:
    stocks = profile.stocks_tier
    options = profile.options_tier
    if detector in {DetectorFamily.STOCK_INTRADAY, DetectorFamily.STOCK_SWING}:
        if stocks is None:
            return ProductScopedDetectorAvailability(
                detector, CapabilityVerdict.UNAVAILABLE, False,
                ("STOCK_MARKET_DATA_UNAVAILABLE",),
            )
        realtime = stocks is ProviderTier.MASSIVE_ADVANCED
        return ProductScopedDetectorAvailability(
            detector, CapabilityVerdict.PASS, realtime,
            () if realtime else ("DEVELOPER_COMPLETED_BAR_CONTRACT",),
        )
    if detector in {
        DetectorFamily.AGGRESSIVE_PREMIUM_BURST,
        DetectorFamily.SWEEP_LIKE_CLUSTER,
    }:
        ready = options is ProviderTier.MASSIVE_ADVANCED
        return ProductScopedDetectorAvailability(
            detector,
            CapabilityVerdict.PASS if ready else CapabilityVerdict.UNAVAILABLE,
            ready,
            (
                ()
                if ready and profile.advanced_platform_ready
                else ("OPTIONS_ADVANCED_ONLY_SHADOW",)
                if ready
                else ("OPTIONS_ADVANCED_QUOTES_TRADES_REQUIRED",)
            ),
        )
    if detector in {DetectorFamily.O2, DetectorFamily.PERSISTENT_REALTIME_FLOW}:
        ready = profile.advanced_platform_ready
        reason = (
            "EQUITY_ADVANCED_SPOT_REQUIRED"
            if options is ProviderTier.MASSIVE_ADVANCED
            and stocks is not ProviderTier.MASSIVE_ADVANCED
            else "STOCKS_AND_OPTIONS_ADVANCED_REQUIRED"
        )
        return ProductScopedDetectorAvailability(
            detector,
            CapabilityVerdict.PASS if ready else CapabilityVerdict.UNAVAILABLE,
            ready,
            () if ready else (reason,),
        )
    if detector in {DetectorFamily.O1, DetectorFamily.O3, DetectorFamily.S1, DetectorFamily.S2}:
        if stocks is None or options is None:
            return ProductScopedDetectorAvailability(
                detector, CapabilityVerdict.UNAVAILABLE, False,
                ("STOCK_OR_OPTION_DEPENDENCY_UNAVAILABLE",),
            )
        realtime = profile.advanced_platform_ready
        reasons = () if realtime else (
            "PORTABLE_DELAYED_STOCK_CONTEXT",
            "ADVANCED_COHORT_NOT_QUALIFIED",
        )
        return ProductScopedDetectorAvailability(
            detector, CapabilityVerdict.PASS, realtime, reasons,
        )
    raise ValueError("unreviewed detector family")


_ALLOWED_TRANSITIONS = {
    (TierState.ADVANCED_READY, TierState.ADVANCED_BLOCKED): (False, False),
    (
        TierState.ADVANCED_BLOCKED,
        TierState.FALLBACK_PENDING_APPROVAL,
    ): (False, False),
    (
        TierState.FALLBACK_PENDING_APPROVAL,
        TierState.DEVELOPER_WARMING,
    ): (True, False),
    (TierState.DEVELOPER_WARMING, TierState.DEVELOPER_READY): (False, True),
    (TierState.DEVELOPER_READY, TierState.ADVANCED_WARMING): (True, False),
    (TierState.ADVANCED_WARMING, TierState.ADVANCED_READY): (False, True),
}


def transition_tier(
    previous: TierState,
    target: TierState,
    *,
    transitioned_at: datetime,
    reason_code: str,
    operator_approved: bool = False,
    operator_approval_reference: str | None = None,
    dependencies_warm: bool = False,
) -> TierTransition:
    stamp = _utc(transitioned_at, "transitioned_at")
    reason = reason_code.strip()
    if not reason:
        raise ValueError("tier transition reason_code cannot be blank")
    requirements = _ALLOWED_TRANSITIONS.get((previous, target))
    if requirements is None:
        raise ValueError("provider tier transition is not allowed")
    approval_required, warmup_required = requirements
    if approval_required and not operator_approved:
        raise ValueError("provider tier transition requires operator approval")
    approval_reference = (
        operator_approval_reference.strip()
        if operator_approval_reference is not None
        else None
    )
    if operator_approved and not approval_reference:
        raise ValueError("approved tier transition requires an approval reference")
    if not operator_approved and approval_reference is not None:
        raise ValueError("unapproved tier transition cannot carry an approval reference")
    if warmup_required and not dependencies_warm:
        raise ValueError("provider tier transition requires warmed dependencies")
    identity = {
        "previous": previous,
        "target": target,
        "transitioned_at": stamp,
        "operator_approved": operator_approved,
        "operator_approval_reference": approval_reference,
        "dependencies_warm": dependencies_warm,
        "reason_code": reason,
    }
    digest = canonical_sha256(identity)
    return TierTransition(
        transition_id=uuid5(NAMESPACE_URL, f"realtime-tier-transition:{digest}"),
        previous=previous,
        target=target,
        transitioned_at=stamp,
        operator_approved=operator_approved,
        operator_approval_reference=approval_reference,
        dependencies_warm=dependencies_warm,
        reason_code=reason,
        transition_sha256=digest,
    )