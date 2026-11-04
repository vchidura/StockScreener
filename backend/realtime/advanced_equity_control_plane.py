from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    AssetClass,
    EQUITY_VALIDATION_UNIVERSE,
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


class _StoredIdentity:
    """A rebuilt universe or manifest carrying the hash its materialized cohort stored."""

    def __init__(self, value, sha256: str) -> None:
        self._value = value
        self.sha256 = sha256

    def __getattr__(self, name: str):
        return getattr(self._value, name)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _StoredIdentity)
            and self.sha256 == other.sha256
            and self._value == other._value
        )

    def __hash__(self) -> int:
        return hash(self.sha256)


def materialized_cohort_identity_payload(plan) -> dict[str, str] | None:
    """What a binding must carry to rebuild a cohort materialized from the desired state."""
    if not isinstance(plan.manifest, _StoredIdentity):
        return None
    return {
        "cohort_id": str(plan.cohort_id),
        "cohort_sha256": plan.cohort_sha256,
        "manifest_id": str(plan.manifest_id),
        "manifest_sha256": plan.manifest.sha256,
        "universe_sha256": plan.universe.sha256,
    }


def with_materialized_cohort_identity(plan, value: Mapping[str, str]):
    return replace(
        plan,
        cohort_id=UUID(value["cohort_id"]),
        cohort_sha256=value["cohort_sha256"],
        manifest_id=UUID(value["manifest_id"]),
        manifest=_StoredIdentity(plan.manifest, value["manifest_sha256"]),
        universe=_StoredIdentity(plan.universe, value["universe_sha256"]),
    )


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


def load_advanced_equity_control_plane_plan_cursor(
    cursor,
    cohort_id: UUID,
) -> AdvancedEquityControlPlanePlan:
    if not isinstance(cohort_id, UUID) or cohort_id.int == 0:
        raise ValueError("Advanced Equity source cohort is invalid")
    cursor.execute(
        "SELECT cohort.cohort_sha256,cohort.configuration_sha256,"
        "cohort.model_bundle_sha256,cohort.valuation_sha256,"
        "cohort.effective_at,cohort.mode,manifest.manifest_id::text,"
        "manifest.manifest_sha256,manifest.universe_sha256,"
        "manifest.session_date,manifest.effective_at AS manifest_effective_at,"
        "manifest.manifest_kind,manifest.allocation_policy_sha256,"
        "profile.profile_id::text,profile.config_sha256,"
        "profile.schema_sha256,profile.capabilities_sha256,"
        "profile.checked_at,profile.expires_at,profile.profile_sha256,"
        "profile.provider_tier,profile.security_policy_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "WHERE cohort.cohort_id=%s",
        (str(cohort_id),),
    )
    row = cursor.fetchone()
    if row is not None and not isinstance(row, Mapping):
        row = dict(zip(
            (description.name for description in cursor.description),
            row,
        ))
    if (
        row is None
        or str(row["manifest_kind"]) != "EQUITY_VALIDATION"
        or str(row["provider_tier"])
        != ProviderTier.MASSIVE_ADVANCED.value
        or str(row["mode"]) != "SHADOW"
    ):
        raise ValueError(
            "Advanced Equity source control plane is unavailable"
        )
    cursor.execute(
        "SELECT revision.ticker,instrument.security_id::text,"
        "member.channel,member.priority,member.purpose "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN LATERAL ("
        "SELECT candidate.ticker "
        "FROM ref.security_revisions AS candidate "
        "WHERE candidate.security_id=instrument.security_id "
        "AND candidate.effective_from<=%s "
        "AND candidate.observed_at<=%s "
        "ORDER BY candidate.effective_from DESC,"
        "candidate.observed_at DESC LIMIT 1"
        ") AS revision ON true "
        "WHERE member.manifest_id=%s "
        "AND member.option_underlying=false "
        "ORDER BY revision.ticker",
        (
            row["checked_at"],
            row["checked_at"],
            str(row["manifest_id"]),
        ),
    )
    member_columns = tuple(
        description.name for description in cursor.description
    )
    members = tuple(
        member
        if isinstance(member, Mapping)
        else dict(zip(member_columns, member))
        for member in cursor.fetchall()
    )
    if (
        not members
        or any(
            str(member["channel"])
            != MarketChannel.MINUTE_AGGREGATE.value
            or int(member["priority"]) != 1
            or str(member["purpose"]) != "PRIMARY"
            for member in members
        )
    ):
        raise ValueError(
            "Advanced Equity source membership is unavailable"
        )
    security_ids = {
        str(member["ticker"]): UUID(str(member["security_id"]))
        for member in members
    }
    if len(security_ids) != len(members):
        raise ValueError("Advanced Equity source membership is ambiguous")
    cursor.execute(
        "SELECT evidence FROM rt.capability_checks "
        "WHERE profile_id=%s AND check_name='entitlement' "
        "ORDER BY observed_at DESC LIMIT 1",
        (str(row["profile_id"]),),
    )
    capability = cursor.fetchone()
    if capability is not None and not isinstance(capability, Mapping):
        capability = dict(zip(
            (description.name for description in cursor.description),
            capability,
        ))
    evidence = None if capability is None else capability["evidence"]
    if (
        not isinstance(evidence, dict)
        or not str(evidence.get("authorization_reference", "")).strip()
        or len(str(evidence.get("evidence_sha256", ""))) != 64
    ):
        raise ValueError(
            "Advanced Equity source authorization is unavailable"
        )
    plan = build_advanced_equity_control_plane_plan(
        session_date=row["session_date"],
        security_ids=security_ids,
        entitlement_evidence_sha256=str(evidence["evidence_sha256"]),
        authorization_reference=str(
            evidence["authorization_reference"]
        ),
        checked_at=row["checked_at"],
        expires_at=row["expires_at"],
        schema_sha256=str(row["schema_sha256"]),
        security_policy_sha256=str(row["security_policy_sha256"]),
        configuration_sha256=str(row["config_sha256"]),
    )
    actual = (
        plan.cohort_sha256,
        plan.configuration_sha256,
        plan.model_bundle_sha256,
        plan.valuation_sha256,
        plan.checked_at,
        plan.manifest_id,
        plan.manifest.sha256,
        plan.universe.sha256,
        plan.session_date,
        plan.manifest.allocation_policy_sha256,
        plan.profile_id,
        plan.capabilities_sha256,
        plan.profile_sha256,
    )
    expected = (
        str(row["cohort_sha256"]),
        str(row["configuration_sha256"]),
        str(row["model_bundle_sha256"]),
        str(row["valuation_sha256"]),
        row["effective_at"],
        UUID(str(row["manifest_id"])),
        str(row["manifest_sha256"]),
        str(row["universe_sha256"]),
        row["session_date"],
        str(row["allocation_policy_sha256"]),
        UUID(str(row["profile_id"])),
        str(row["capabilities_sha256"]),
        str(row["profile_sha256"]),
    )
    if plan.cohort_id != cohort_id or actual != expected:
        # A session cohort materialized from the desired state has its own
        # manifest and effective time, which this rebuild cannot reproduce. Its
        # snapshot row is the durable proof of that identity; everything the
        # profile determines must still match.
        cursor.execute(
            "SELECT 1 FROM rt.realtime_desired_session_snapshots "
            "WHERE cohort_id=%s AND manifest_id=%s AND session_date=%s",
            (str(cohort_id), str(row["manifest_id"]), row["session_date"]),
        )
        if (
            cursor.fetchone() is None
            or plan.configuration_sha256 != str(row["configuration_sha256"])
            or plan.model_bundle_sha256 != str(row["model_bundle_sha256"])
            or plan.valuation_sha256 != str(row["valuation_sha256"])
            or plan.session_date != row["session_date"]
            or plan.profile_id != UUID(str(row["profile_id"]))
            or plan.capabilities_sha256 != str(row["capabilities_sha256"])
            or plan.profile_sha256 != str(row["profile_sha256"])
        ):
            raise ValueError(
                "Advanced Equity source control plane identity changed"
            )
        return replace(
            plan,
            cohort_id=cohort_id,
            cohort_sha256=str(row["cohort_sha256"]),
            manifest_id=UUID(str(row["manifest_id"])),
            manifest=_StoredIdentity(plan.manifest, str(row["manifest_sha256"])),
            universe=_StoredIdentity(plan.universe, str(row["universe_sha256"])),
        )
    return plan


def validate_active_target9_equity_source_cursor(
    cursor,
    plan: AdvancedEquityControlPlanePlan,
) -> None:
    if (
        not isinstance(plan, AdvancedEquityControlPlanePlan)
        or plan.universe.equity_tickers
        != EQUITY_VALIDATION_UNIVERSE.equity_tickers
    ):
        raise ValueError(
            "Advanced Options requires the exact target-9 Equity universe"
        )
    cursor.execute(
        "SELECT universe_target,provider_tier,feed,channels,status,"
        "configuration_sha256 "
        "FROM rt.advanced_ingress_runtime_profiles "
        "WHERE configuration_sha256=%s",
        (plan.configuration_sha256,),
    )
    row = cursor.fetchone()
    if row is not None and not isinstance(row, Mapping):
        row = dict(zip(
            (description.name for description in cursor.description),
            row,
        ))
    if (
        row is None
        or int(row["universe_target"]) != 9
        or str(row["provider_tier"])
        != ProviderTier.MASSIVE_ADVANCED.value
        or str(row["feed"]) != "STOCKS"
        or tuple(row["channels"]) != ("AM",)
        or str(row["status"]) != "ACTIVE"
        or str(row["configuration_sha256"])
        != plan.configuration_sha256
    ):
        raise ValueError(
            "Advanced Options target-9 Equity runtime profile is not active"
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
        instrument_id = ensure_equity_instrument_cursor(
            cursor,
            security_id,
        )
        cursor.execute(
            "INSERT INTO rt.subscription_members ("
            "manifest_id,instrument_id,channel,priority,purpose,"
            "option_underlying) VALUES (%s,%s,'AM',1,'PRIMARY',false) "
            "ON CONFLICT DO NOTHING",
            (str(plan.manifest_id), instrument_id),
        )
    persist_tier_cohort_binding_cursor(cursor, plan.binding)


def ensure_equity_instrument_cursor(
    cursor,
    security_id: UUID,
) -> int:
    if not isinstance(security_id, UUID) or security_id.int == 0:
        raise ValueError("Equity instrument security identity is invalid")
    cursor.execute(
        "INSERT INTO rt.instruments (asset_class,security_id) "
        "VALUES ('EQUITY',%s) ON CONFLICT DO NOTHING",
        (str(security_id),),
    )
    cursor.execute(
        "SELECT instrument_id FROM rt.instruments "
        "WHERE asset_class='EQUITY' AND security_id=%s "
        "ORDER BY instrument_id",
        (str(security_id),),
    )
    rows = tuple(cursor.fetchall())
    if len(rows) != 1:
        raise ValueError(
            "Advanced control plane instrument identity is ambiguous"
        )
    return int(rows[0][0])
