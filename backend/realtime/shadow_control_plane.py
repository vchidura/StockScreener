from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    CANARY_UNIVERSE,
    AssetClass,
    CapabilityVerdict,
    MarketChannel,
    ReadinessStatus,
    SubscriptionManifest,
    SubscriptionMember,
    canonical_sha256,
)
from .option_manifest_cohort_repository import (
    option_universe_manifest_id,
    persist_option_universe_manifest_cursor,
)
from .option_slot_set import option_manifest_members, pre_session_slot_set
from .option_slot_set_repository import persist_and_activate_slot_set_cursor
from .preflight import PreflightProfile, ProviderTier
from .reference_seed import ReferenceSeedManifest, SeedStatus


@dataclass(frozen=True, slots=True)
class ShadowInstrumentBinding:
    asset_class: AssetClass
    provider_symbol: str
    underlying: str
    security_id: UUID | None = None
    contract_id: int | None = None
    catalog_version_id: int | None = None


@dataclass(frozen=True, slots=True)
class ShadowControlPlanePlan:
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
    instruments: tuple[ShadowInstrumentBinding, ...]


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("effective_at must be timezone-aware")
    return value.astimezone(timezone.utc)


def build_shadow_control_plane_plan(
    seed: ReferenceSeedManifest,
    profile: PreflightProfile,
    *,
    security_ids: Mapping[str, UUID],
    option_contracts: Mapping[str, tuple[int, str, int]],
    configuration_sha256: str,
    model_bundle_sha256: str,
    valuation_sha256: str,
    effective_at: datetime,
) -> ShadowControlPlanePlan:
    if seed.status is not SeedStatus.READY or seed.universe_sha256 != CANARY_UNIVERSE.sha256:
        raise ValueError("SHADOW bootstrap requires the ready canary reference seed")
    if (
        profile.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
        or profile.verdict not in {CapabilityVerdict.FAIL, CapabilityVerdict.UNAVAILABLE}
        or profile.readiness.status is not ReadinessStatus.BLOCKED
        or profile.universe_sha256 != CANARY_UNIVERSE.sha256
    ):
        raise ValueError("SHADOW bootstrap requires a non-PASS blocked Developer profile")
    if set(security_ids) != set(CANARY_UNIVERSE.equity_tickers):
        raise ValueError("SHADOW security identities must cover the exact canary")
    if set(option_contracts) != set(CANARY_UNIVERSE.option_underlyings):
        raise ValueError("SHADOW option identities must cover the exact canary")
    effective = _utc(effective_at)
    if effective < seed.effective_at:
        raise ValueError("SHADOW cohort cannot precede its reference seed")
    model_hash = _sha256(model_bundle_sha256, "model_bundle_sha256")
    valuation_hash = _sha256(valuation_sha256, "valuation_sha256")
    configuration_hash = _sha256(configuration_sha256, "configuration_sha256")

    instruments = []
    members = []
    for ticker in CANARY_UNIVERSE.equity_tickers:
        instruments.append(
            ShadowInstrumentBinding(
                AssetClass.EQUITY,
                ticker,
                ticker,
                security_id=security_ids[ticker],
            )
        )
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        ):
            members.append(SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel))
    for ticker in CANARY_UNIVERSE.option_underlyings:
        contract_id, contract_ticker, catalog_version_id = option_contracts[ticker]
        instruments.append(
            ShadowInstrumentBinding(
                AssetClass.OPTION,
                contract_ticker,
                ticker,
                contract_id=contract_id,
                catalog_version_id=catalog_version_id,
            )
        )
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE):
            members.append(
                SubscriptionMember(
                    AssetClass.OPTION,
                    contract_ticker,
                    ticker,
                    channel,
                    catalog_version_id=catalog_version_id,
                )
            )
    allocation_sha256 = canonical_sha256(
        {
            "version": "developer-shadow-exact-canary-v1",
            "seed_sha256": seed.seed_sha256,
            "members": tuple(members),
        }
    )
    manifest = SubscriptionManifest(
        version="developer-shadow-canary-v1",
        session_date=seed.session_date,
        universe=CANARY_UNIVERSE,
        capability_profile_sha256=profile.profile_sha256,
        allocation_policy_sha256=allocation_sha256,
        members=tuple(members),
        maximum_option_quote_contracts=8,
    )
    manifest_id = option_universe_manifest_id(manifest)
    cohort_identity = {
        "manifest_id": manifest_id,
        "configuration_sha256": configuration_hash,
        "model_bundle_sha256": model_hash,
        "valuation_sha256": valuation_hash,
        "effective_at": effective,
        "mode": "SHADOW",
    }
    cohort_sha256 = canonical_sha256(cohort_identity)
    cohort_id = uuid5(NAMESPACE_URL, f"realtime-shadow-cohort:{cohort_sha256}")
    return ShadowControlPlanePlan(
        seed_id=seed.seed_id,
        profile_id=profile.profile_id,
        profile_sha256=profile.profile_sha256,
        manifest_id=manifest_id,
        manifest=manifest,
        cohort_id=cohort_id,
        cohort_sha256=cohort_sha256,
        configuration_sha256=configuration_hash,
        model_bundle_sha256=model_hash,
        valuation_sha256=valuation_hash,
        effective_at=effective,
        instruments=tuple(instruments),
    )


def persist_shadow_instruments(
    connection,
    plan: ShadowControlPlanePlan,
) -> Mapping[tuple[AssetClass, str], int]:
    instrument_ids = {}
    try:
        with connection.cursor() as cursor:
            for binding in plan.instruments:
                if binding.asset_class is AssetClass.EQUITY:
                    cursor.execute(
                        "INSERT INTO rt.instruments (asset_class, security_id) "
                        "VALUES ('EQUITY', %s) ON CONFLICT DO NOTHING",
                        (str(binding.security_id),),
                    )
                    cursor.execute(
                        "SELECT instrument_id FROM rt.instruments WHERE security_id=%s",
                        (str(binding.security_id),),
                    )
                else:
                    cursor.execute(
                        "INSERT INTO rt.instruments (asset_class, contract_id) "
                        "VALUES ('OPTION', %s) ON CONFLICT DO NOTHING",
                        (binding.contract_id,),
                    )
                    cursor.execute(
                        "SELECT instrument_id FROM rt.instruments WHERE contract_id=%s",
                        (binding.contract_id,),
                    )
                row = cursor.fetchone()
                if row is None or int(row[0]) <= 0:
                    raise ValueError("SHADOW instrument persistence did not reconcile")
                instrument_ids[(binding.asset_class, binding.provider_symbol)] = int(row[0])
        if len(instrument_ids) != 16 or len(set(instrument_ids.values())) != 16:
            raise ValueError("SHADOW instrument identities are not exact and unique")
        connection.commit()
        return MappingProxyType(instrument_ids)
    except Exception:
        connection.rollback()
        raise


def persist_shadow_control_plane(
    connection,
    plan: ShadowControlPlanePlan,
    instrument_ids: Mapping[tuple[AssetClass, str], int],
) -> None:
    if set(instrument_ids) != {
        (binding.asset_class, binding.provider_symbol) for binding in plan.instruments
    }:
        raise ValueError("SHADOW control plane received mismatched instrument identities")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, profile_sha256 FROM rt.capability_profiles "
                "WHERE profile_id=%s",
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
                raise ValueError("SHADOW passive profile is absent or not blocked")
            manifest = plan.manifest
            persist_option_universe_manifest_cursor(
                cursor,
                manifest,
                manifest_id=plan.manifest_id,
                profile_id=plan.profile_id,
                effective_at=plan.effective_at,
                equity_instrument_ids={
                    ticker: instrument_ids[(AssetClass.EQUITY, ticker)]
                    for ticker in CANARY_UNIVERSE.equity_tickers
                },
            )
            cursor.execute(
                "INSERT INTO rt.cohorts ("
                "cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
                "model_bundle_sha256, valuation_sha256, effective_at, mode) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, 'SHADOW') "
                "ON CONFLICT (cohort_id) DO NOTHING",
                (
                    str(plan.cohort_id), plan.cohort_sha256, str(plan.manifest_id),
                    plan.configuration_sha256, plan.model_bundle_sha256,
                    plan.valuation_sha256, plan.effective_at,
                ),
            )
            cursor.execute(
                "SELECT cohort_sha256, mode FROM rt.cohorts WHERE cohort_id=%s",
                (str(plan.cohort_id),),
            )
            stored_cohort = cursor.fetchone()
            if stored_cohort != (plan.cohort_sha256, "SHADOW"):
                raise ValueError("SHADOW cohort identity conflict")
            persist_and_activate_slot_set_cursor(
                cursor,
                pre_session_slot_set(
                    manifest,
                    cohort_id=plan.cohort_id,
                    instrument_ids={
                        member.provider_symbol:
                            instrument_ids[(AssetClass.OPTION, member.provider_symbol)]
                        for member in option_manifest_members(manifest)
                    },
                    effective_at=plan.effective_at,
                ),
                activated_at=plan.effective_at,
                allow_superseded=True,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise