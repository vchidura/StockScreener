from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    EQUITY_VALIDATION_UNIVERSE,
    AssetClass,
    CapabilityVerdict,
    MarketChannel,
    ReadinessStatus,
    SubscriptionManifest,
    SubscriptionMember,
    canonical_sha256,
)
from .preflight import PreflightProfile, ProviderTier
from .reference_seed import ReferenceSeedManifest, SeedStatus


@dataclass(frozen=True, slots=True)
class EquityValidationControlPlanePlan:
    seed_id: UUID
    profile_id: UUID
    profile_sha256: str
    manifest_id: UUID
    manifest: SubscriptionManifest
    cohort_id: UUID
    cohort_sha256: str
    configuration_sha256: str
    model_bundle_sha256: str
    valuation_sha256: str
    effective_at: datetime
    security_ids: tuple[tuple[str, UUID], ...]


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("effective_at must be timezone-aware")
    return value.astimezone(timezone.utc)


def build_equity_validation_control_plane_plan(
    seed: ReferenceSeedManifest,
    profile: PreflightProfile,
    *,
    security_ids: Mapping[str, UUID],
    configuration_sha256: str,
    model_bundle_sha256: str,
    valuation_sha256: str,
    effective_at: datetime,
) -> EquityValidationControlPlanePlan:
    universe = EQUITY_VALIDATION_UNIVERSE
    if seed.status is not SeedStatus.READY or seed.universe_sha256 != universe.sha256:
        raise ValueError("equity validation requires its ready reference seed")
    if (
        profile.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
        or profile.verdict not in {CapabilityVerdict.FAIL, CapabilityVerdict.UNAVAILABLE}
        or profile.readiness.status is not ReadinessStatus.BLOCKED
        or profile.universe_sha256 != universe.sha256
    ):
        raise ValueError("equity validation requires a blocked Developer profile")
    if set(security_ids) != set(universe.equity_tickers):
        raise ValueError("equity validation security identities must cover the exact scope")
    effective = _utc(effective_at)
    if effective < seed.effective_at:
        raise ValueError("equity validation cohort cannot precede its reference seed")
    configuration_hash = _sha256(configuration_sha256, "configuration_sha256")
    model_hash = _sha256(model_bundle_sha256, "model_bundle_sha256")
    valuation_hash = _sha256(valuation_sha256, "valuation_sha256")
    members = tuple(
        SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
        for ticker in universe.equity_tickers
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        )
    )
    allocation_sha256 = canonical_sha256({
        "version": "equity-validation-all-channels-v1",
        "seed_sha256": seed.seed_sha256,
        "members": members,
    })
    manifest = SubscriptionManifest(
        version="equity-validation-manifest-v1",
        session_date=seed.session_date,
        universe=universe,
        capability_profile_sha256=profile.profile_sha256,
        allocation_policy_sha256=allocation_sha256,
        members=members,
        maximum_option_quote_contracts=0,
    )
    manifest_id = uuid5(NAMESPACE_URL, f"realtime-subscription:{manifest.sha256}")
    cohort_identity = {
        "manifest_id": manifest_id,
        "configuration_sha256": configuration_hash,
        "model_bundle_sha256": model_hash,
        "valuation_sha256": valuation_hash,
        "effective_at": effective,
        "mode": "SHADOW",
    }
    cohort_sha256 = canonical_sha256(cohort_identity)
    return EquityValidationControlPlanePlan(
        seed_id=seed.seed_id,
        profile_id=profile.profile_id,
        profile_sha256=profile.profile_sha256,
        manifest_id=manifest_id,
        manifest=manifest,
        cohort_id=uuid5(NAMESPACE_URL, f"realtime-shadow-cohort:{cohort_sha256}"),
        cohort_sha256=cohort_sha256,
        configuration_sha256=configuration_hash,
        model_bundle_sha256=model_hash,
        valuation_sha256=valuation_hash,
        effective_at=effective,
        security_ids=tuple(sorted(security_ids.items())),
    )


def persist_equity_validation_instruments(
    connection,
    plan: EquityValidationControlPlanePlan,
) -> Mapping[str, int]:
    instrument_ids = {}
    try:
        with connection.cursor() as cursor:
            for ticker, security_id in plan.security_ids:
                cursor.execute(
                    "INSERT INTO rt.instruments (asset_class, security_id) "
                    "VALUES ('EQUITY', %s) ON CONFLICT DO NOTHING",
                    (str(security_id),),
                )
                cursor.execute(
                    "SELECT instrument_id FROM rt.instruments WHERE security_id=%s",
                    (str(security_id),),
                )
                row = cursor.fetchone()
                if row is None or int(row[0]) <= 0:
                    raise ValueError("equity validation instrument persistence did not reconcile")
                instrument_ids[ticker] = int(row[0])
        if set(instrument_ids) != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers):
            raise ValueError("equity validation instrument identities are incomplete")
        connection.commit()
        return MappingProxyType(instrument_ids)
    except Exception:
        connection.rollback()
        raise


def persist_equity_validation_control_plane(
    connection,
    plan: EquityValidationControlPlanePlan,
    instrument_ids: Mapping[str, int],
) -> None:
    universe = EQUITY_VALIDATION_UNIVERSE
    if set(instrument_ids) != set(universe.equity_tickers):
        raise ValueError("equity validation control plane received mismatched instruments")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, profile_sha256 FROM rt.capability_profiles WHERE profile_id=%s",
                (str(plan.profile_id),),
            )
            profile = cursor.fetchone()
            if (
                profile is None
                or profile[0] not in {
                    CapabilityVerdict.FAIL.value,
                    CapabilityVerdict.UNAVAILABLE.value,
                }
                or profile[1] != plan.profile_sha256
            ):
                raise ValueError("equity validation profile is absent or not blocked")
            cursor.execute(
                "INSERT INTO rt.subscription_manifests ("
                "manifest_id, manifest_sha256, universe_sha256, session_date, effective_at, "
                "profile_id, stock_count, option_underlying_count, "
                "member_count, allocation_policy_sha256, "
                "manifest_kind) VALUES (%s, %s, %s, %s, %s, %s, 9, 0, 27, %s, "
                "'EQUITY_VALIDATION') ON CONFLICT (manifest_id) DO NOTHING",
                (
                    str(plan.manifest_id),
                    plan.manifest.sha256,
                    universe.sha256,
                    plan.manifest.session_date,
                    plan.effective_at,
                    str(plan.profile_id),
                    plan.manifest.allocation_policy_sha256,
                ),
            )
            cursor.execute(
                "SELECT manifest_sha256, manifest_kind FROM rt.subscription_manifests "
                "WHERE manifest_id=%s",
                (str(plan.manifest_id),),
            )
            if cursor.fetchone() != (plan.manifest.sha256, "EQUITY_VALIDATION"):
                raise ValueError("equity validation manifest identity conflict")
            for member in plan.manifest.members:
                cursor.execute(
                    "INSERT INTO rt.subscription_members ("
                    "manifest_id, instrument_id, channel, priority, purpose) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (
                        str(plan.manifest_id),
                        instrument_ids[member.provider_symbol],
                        member.channel.value,
                        member.priority,
                        member.purpose.value,
                    ),
                )
            cursor.execute(
                "INSERT INTO rt.cohorts ("
                "cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
                "model_bundle_sha256, valuation_sha256, effective_at, mode) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, 'SHADOW') "
                "ON CONFLICT (cohort_id) DO NOTHING",
                (
                    str(plan.cohort_id),
                    plan.cohort_sha256,
                    str(plan.manifest_id),
                    plan.configuration_sha256,
                    plan.model_bundle_sha256,
                    plan.valuation_sha256,
                    plan.effective_at,
                ),
            )
            cursor.execute(
                "SELECT cohort_sha256, mode FROM rt.cohorts WHERE cohort_id=%s",
                (str(plan.cohort_id),),
            )
            if cursor.fetchone() != (plan.cohort_sha256, "SHADOW"):
                raise ValueError("equity validation cohort identity conflict")
        connection.commit()
    except Exception:
        connection.rollback()
        raise