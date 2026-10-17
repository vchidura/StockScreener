from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from .preflight import ProviderTier, TierState
from .tier_cohort import (
    TierCohortBinding,
    persist_tier_cohort_binding_cursor,
    product_capabilities,
    product_disabled_detectors,
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(
        character not in "0123456789abcdef"
        for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedEquityControlPlanePlan:
    session_date: date
    universe: UniverseManifest
    security_ids: tuple[tuple[str, UUID], ...]
    entitlement_evidence_sha256: str
    authorization_reference: str
    checked_at: datetime
    expires_at: datetime
    schema_sha256: str
    security_policy_sha256: str
    profile_id: UUID
    profile_sha256: str
    configuration_sha256: str
    capabilities_sha256: str
    manifest_id: UUID
    manifest: SubscriptionManifest
    cohort_id: UUID
    cohort_sha256: str
    model_bundle_sha256: str
    valuation_sha256: str
    binding: TierCohortBinding


def build_advanced_equity_control_plane_plan(
    *,
    session_date: date,
    security_ids: dict[str, UUID],
    entitlement_evidence_sha256: str,
    authorization_reference: str,
    checked_at: datetime,
    expires_at: datetime,
    schema_sha256: str,
    security_policy_sha256: str,
    configuration_sha256: str | None = None,
) -> AdvancedEquityControlPlanePlan:
    symbols = tuple(sorted(security_ids))
    if not 1 <= len(symbols) <= 400:
        raise ValueError(
            "Advanced equity control plane requires 1-400 exact symbols"
        )
    checked = _utc(checked_at, "checked_at")
    expires = _utc(expires_at, "expires_at")
    if expires <= checked:
        raise ValueError("Advanced entitlement evidence must expire after check")
    evidence_sha = _sha256(
        entitlement_evidence_sha256,
        "entitlement_evidence_sha256",
    )
    schema = _sha256(schema_sha256, "schema_sha256")
    security_policy = _sha256(
        security_policy_sha256,
        "security_policy_sha256",
    )
    approval = authorization_reference.strip()
    if not approval:
        raise ValueError("Advanced control plane requires authorization")
    universe = UniverseManifest(
        version=f"advanced-equity-{len(symbols)}-am-v1",
        equity_tickers=symbols,
        option_underlyings=(),
    )
    capabilities = product_capabilities(
        ProviderTier.MASSIVE_ADVANCED,
        None,
    )
    disabled_detectors = product_disabled_detectors(
        ProviderTier.MASSIVE_ADVANCED,
        None,
    )
    configuration_sha256 = (
        _sha256(configuration_sha256, "configuration_sha256")
        if configuration_sha256 is not None
        else canonical_sha256({
            "version": "advanced-equity-control-v1",
            "universe_sha256": universe.sha256,
            "channels": ("AM",),
            "options_pipeline": "DISABLED",
            "authorization_reference": approval,
        })
    )
    capabilities_sha256 = canonical_sha256(
        tuple(sorted(value.value for value in capabilities))
    )
    profile_identity = {
        "configuration_sha256": configuration_sha256,
        "schema_sha256": schema,
        "security_policy_sha256": security_policy,
        "universe_sha256": universe.sha256,
        "capabilities_sha256": capabilities_sha256,
        "entitlement_evidence_sha256": evidence_sha,
        "checked_at": checked,
        "expires_at": expires,
    }
    profile_sha256 = canonical_sha256(profile_identity)
    profile_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-profile:{profile_sha256}",
    )
    members = tuple(
        SubscriptionMember(
            AssetClass.EQUITY,
            ticker,
            ticker,
            MarketChannel.MINUTE_AGGREGATE,
        )
        for ticker in symbols
    )
    allocation_sha256 = canonical_sha256({
        "version": "advanced-equity-am-allocation-v1",
        "profile_sha256": profile_sha256,
        "members": members,
    })
    manifest = SubscriptionManifest(
        version=f"advanced-equity-{len(symbols)}-am-manifest-v1",
        session_date=session_date,
        universe=universe,
        capability_profile_sha256=profile_sha256,
        allocation_policy_sha256=allocation_sha256,
        members=members,
        maximum_option_quote_contracts=0,
    )
    manifest_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-manifest:{manifest.sha256}",
    )
    model_bundle_sha256 = canonical_sha256(
        "advanced-equity-existing-model-bundle-v1"
    )
    valuation_sha256 = canonical_sha256(
        "advanced-equity-raw-am-valuation-v1"
    )
    cohort_identity = {
        "manifest_id": manifest_id,
        "configuration_sha256": configuration_sha256,
        "model_bundle_sha256": model_bundle_sha256,
        "valuation_sha256": valuation_sha256,
        "effective_at": checked,
        "mode": "SHADOW",
    }
    cohort_sha256 = canonical_sha256(cohort_identity)
    cohort_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-cohort:{cohort_sha256}",
    )
    binding = TierCohortBinding(
        cohort_id,
        profile_id,
        ProviderTier.MASSIVE_ADVANCED,
        TierState.ADVANCED_READY,
        manifest.sha256,
        valuation_sha256,
        capabilities,
        disabled_detectors,
        checked,
        options_enabled=False,
    )
    return AdvancedEquityControlPlanePlan(
        session_date=session_date,
        universe=universe,
        security_ids=tuple(sorted(security_ids.items())),
        entitlement_evidence_sha256=evidence_sha,
        authorization_reference=approval,
        checked_at=checked,
        expires_at=expires,
        schema_sha256=schema,
        security_policy_sha256=security_policy,
        profile_id=profile_id,
        profile_sha256=profile_sha256,
        configuration_sha256=configuration_sha256,
        capabilities_sha256=capabilities_sha256,
        manifest_id=manifest_id,
        manifest=manifest,
        cohort_id=cohort_id,
        cohort_sha256=cohort_sha256,
        model_bundle_sha256=model_bundle_sha256,
        valuation_sha256=valuation_sha256,
        binding=binding,
    )


def persist_advanced_equity_control_plane_cursor(
    cursor,
    plan: AdvancedEquityControlPlanePlan,
) -> None:
    cursor.execute(
        "INSERT INTO rt.capability_profiles ("
        "profile_id,config_sha256,schema_sha256,account_alias,"
        "capabilities_sha256,universe_sha256,checked_at,expires_at,"
        "status,profile_sha256,provider_tier,security_policy_sha256,"
        "preflight_mode,provider_requests,action_authorized) VALUES ("
        "%s,%s,%s,'massive-primary',%s,%s,%s,%s,'PASS',%s,"
        "'MASSIVE_ADVANCED',%s,'ACTIVE',true,true) "
        "ON CONFLICT (profile_id) DO NOTHING",
        (
            str(plan.profile_id),
            plan.configuration_sha256,
            plan.schema_sha256,
            plan.capabilities_sha256,
            plan.universe.sha256,
            plan.checked_at,
            plan.expires_at,
            plan.profile_sha256,
            plan.security_policy_sha256,
        ),
    )
    for check_name in ("entitlement", "live_recency", "active_probe"):
        cursor.execute(
            "INSERT INTO rt.capability_checks ("
            "profile_id,check_name,scope_key,verdict,required,"
            "reason_codes,observed_at,expires_at,evidence) VALUES ("
            "%s,%s,'stocks:SPY:AM','PASS',true,ARRAY[]::text[],"
            "%s,%s,%s::jsonb) ON CONFLICT DO NOTHING",
            (
                str(plan.profile_id),
                check_name,
                plan.checked_at,
                plan.expires_at,
                json.dumps({
                    "authorization_reference": (
                        plan.authorization_reference
                    ),
                    "evidence_sha256": (
                        plan.entitlement_evidence_sha256
                    ),
                }),
            ),
        )
    cursor.execute(
        "INSERT INTO rt.subscription_manifests ("
        "manifest_id,manifest_sha256,universe_sha256,session_date,"
        "effective_at,profile_id,manifest_kind,stock_count,"
        "option_underlying_count,member_count,allocation_policy_sha256"
        ") VALUES (%s,%s,%s,%s,%s,%s,'EQUITY_VALIDATION',%s,0,%s,%s) "
        "ON CONFLICT (manifest_id) DO NOTHING",
        (
            str(plan.manifest_id),
            plan.manifest.sha256,
            plan.universe.sha256,
            plan.session_date,
            plan.checked_at,
            str(plan.profile_id),
            len(plan.security_ids),
            len(plan.manifest.members),
            plan.manifest.allocation_policy_sha256,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.cohorts ("
        "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
        "model_bundle_sha256,valuation_sha256,effective_at,mode"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW') "
        "ON CONFLICT (cohort_id) DO NOTHING",
        (
            str(plan.cohort_id),
            plan.cohort_sha256,
            str(plan.manifest_id),
            plan.configuration_sha256,
            plan.model_bundle_sha256,
            plan.valuation_sha256,
            plan.checked_at,
        ),
    )
    for ticker, security_id in plan.security_ids:
        cursor.execute(
            "INSERT INTO rt.instruments (asset_class,security_id) "
            "VALUES ('EQUITY',%s) ON CONFLICT DO NOTHING",
            (str(security_id),),
        )
        cursor.execute(
            "SELECT instrument_id FROM rt.instruments "
            "WHERE asset_class='EQUITY' AND security_id=%s",
            (str(security_id),),
        )
        instrument = cursor.fetchone()
        if instrument is None:
            raise ValueError(
                "Advanced control plane instrument is unavailable"
            )
        cursor.execute(
            "INSERT INTO rt.subscription_members ("
            "manifest_id,instrument_id,channel,priority,purpose,"
            "option_underlying) VALUES (%s,%s,'AM',1,'PRIMARY',false) "
            "ON CONFLICT DO NOTHING",
            (str(plan.manifest_id), int(instrument[0])),
        )
    persist_tier_cohort_binding_cursor(cursor, plan.binding)
