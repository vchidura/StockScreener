from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import DEVELOPER_FORWARD_UNIVERSE, SubscriptionManifest, canonical_sha256
from .option_session_manifest import (
    ManifestSelectionStatus,
    SelectedContractManifestPlan,
)
from .option_slot_set import equity_manifest_members, option_universe_manifest_sha256
from .preflight import DetectorFamily, ProviderTier


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("option manifest cohort effective_at must be timezone-aware")
    return value.astimezone(timezone.utc)


def option_universe_manifest_id(manifest: SubscriptionManifest) -> UUID:
    return uuid5(NAMESPACE_URL, f"realtime-subscription:{option_universe_manifest_sha256(manifest)}")


def option_subscription_manifest_id(plan: SelectedContractManifestPlan) -> UUID | None:
    if plan.manifest is None:
        return None
    return option_universe_manifest_id(plan.manifest)


def persist_option_universe_manifest_cursor(
    cursor,
    manifest: SubscriptionManifest,
    *,
    manifest_id: UUID,
    profile_id: UUID,
    effective_at: datetime,
    equity_instrument_ids: Mapping[str, int],
) -> None:
    """Persist the equity-only manifest of a MARKET_OPTIONS universe and verify it by reading it back.

    `member_count` counts the stock channel members; the option contracts are not manifest members (they are
    persisted as a slot set under the cohort). The first writer's `effective_at` stays on a replayed manifest.
    """
    universe = manifest.universe
    equity_members = equity_manifest_members(manifest)
    stock_count = len(universe.equity_tickers)
    underlying_count = len(universe.option_underlyings)
    if not 1 <= underlying_count <= stock_count or len(equity_members) < stock_count:
        raise ValueError("option universe manifest counts are outside the control plane contract")
    if manifest_id != option_universe_manifest_id(manifest):
        raise ValueError("option universe manifest id does not match its identity")
    if set(equity_instrument_ids) != set(universe.equity_tickers):
        raise ValueError("option universe manifest equity identities are incomplete")
    option_underlyings = set(universe.option_underlyings)
    expected_members = tuple(sorted(
        (
            row.channel.value,
            equity_instrument_ids[row.provider_symbol],
            row.priority,
            row.purpose.value,
            row.provider_symbol in option_underlyings,
        )
        for row in equity_members
    ))
    expected_manifest = (
        option_universe_manifest_sha256(manifest),
        universe.sha256,
        str(profile_id),
        manifest.session_date,
        "MARKET_OPTIONS",
        stock_count,
        underlying_count,
        len(equity_members),
        manifest.allocation_policy_sha256,
    )
    cursor.execute(
        "INSERT INTO rt.subscription_manifests ("
        "manifest_id, manifest_sha256, universe_sha256, session_date, effective_at, profile_id, "
        "manifest_kind, stock_count, option_underlying_count, member_count, allocation_policy_sha256) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'MARKET_OPTIONS', %s, %s, %s, %s) "
        "ON CONFLICT (manifest_id) DO NOTHING",
        (
            str(manifest_id), expected_manifest[0], universe.sha256, manifest.session_date,
            _utc(effective_at), str(profile_id), stock_count, underlying_count,
            len(equity_members), manifest.allocation_policy_sha256,
        ),
    )
    cursor.execute(
        "SELECT manifest_sha256, universe_sha256, profile_id::text, session_date, manifest_kind, "
        "stock_count, option_underlying_count, member_count, allocation_policy_sha256 "
        "FROM rt.subscription_manifests WHERE manifest_id=%s",
        (str(manifest_id),),
    )
    stored_manifest = cursor.fetchone()
    if stored_manifest is None or tuple(stored_manifest) != expected_manifest:
        raise ValueError("persisted option subscription manifest identity conflict")
    for channel, instrument_id, priority, purpose, option_underlying in expected_members:
        cursor.execute(
            "INSERT INTO rt.subscription_members ("
            "manifest_id, instrument_id, channel, priority, purpose, option_underlying) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (str(manifest_id), instrument_id, channel, priority, purpose, option_underlying),
        )
    cursor.execute(
        "SELECT channel, instrument_id, priority, purpose, option_underlying "
        "FROM rt.subscription_members WHERE manifest_id=%s",
        (str(manifest_id),),
    )
    stored_members = tuple(sorted(
        (str(channel), int(instrument_id), int(priority), str(purpose), bool(option_underlying))
        for channel, instrument_id, priority, purpose, option_underlying in cursor.fetchall()
    ))
    if stored_members != expected_members:
        raise ValueError("persisted option subscription members are incomplete")


@dataclass(frozen=True, slots=True)
class DeveloperForwardCohortBinding:
    configuration_sha256: str
    model_bundle_sha256: str
    valuation_sha256: str
    universe_sha256: str = DEVELOPER_FORWARD_UNIVERSE.sha256

    def __post_init__(self):
        for digest in (self.configuration_sha256, self.model_bundle_sha256,
                self.valuation_sha256, self.universe_sha256):
            if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                raise ValueError("forward cohort binding requires reviewed SHA-256 identities")

    @classmethod
    def from_configuration(cls, configuration, universe=DEVELOPER_FORWARD_UNIVERSE):
        if set(configuration.settings.underlyers) != set(universe.equity_tickers):
            raise ValueError("forward configuration must cover the exact reviewed universe")
        return cls(configuration.configuration_sha256, canonical_sha256(dict(
            version="developer-forward-native-model-bundle-v1",
            configuration_sha256=configuration.configuration_sha256,
            developer_policy_sha256=configuration.developer_policy_sha256,
            strategy_policy_sha256=configuration.strategy_policy_sha256,
            gamma_policy_sha256=configuration.gamma_policy_sha256,
        )), configuration.valuation_policy_sha256, universe.sha256)


def _shadow_cohort_identity(manifest_id, configuration_sha256, model_bundle_sha256, valuation_sha256, effective_at):
    cohort_sha256 = canonical_sha256(dict(
        manifest_id=str(manifest_id), configuration_sha256=configuration_sha256,
        model_bundle_sha256=model_bundle_sha256, valuation_sha256=valuation_sha256,
        effective_at=_utc(effective_at), mode="SHADOW",
    ))
    return uuid5(NAMESPACE_URL, f"realtime-selected-manifest-shadow:{cohort_sha256}"), cohort_sha256


def developer_forward_cohort_id(plan, binding, *, effective_at):
    if (not isinstance(binding, DeveloperForwardCohortBinding) or plan.manifest is None
            or plan.status is not ManifestSelectionStatus.READY
            or plan.scope.universe.sha256 != binding.universe_sha256):
        raise ValueError("forward cohort identity requires a ready reviewed selected manifest")
    manifest_id = option_universe_manifest_id(plan.manifest)
    return _shadow_cohort_identity(manifest_id, binding.configuration_sha256, binding.model_bundle_sha256,
        binding.valuation_sha256, effective_at)[0]


def bind_ready_manifest_shadow_cohort_cursor(
    cursor,
    plan: SelectedContractManifestPlan,
    *,
    manifest_id: UUID,
    effective_at: datetime,
    cohort_binding: DeveloperForwardCohortBinding | None = None,
) -> UUID | None:
    if cohort_binding is not None and (not isinstance(cohort_binding, DeveloperForwardCohortBinding)
            or plan.scope.universe.sha256 != cohort_binding.universe_sha256):
        raise ValueError("forward cohort binding changed the reviewed universe")
    if plan.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
        return None
    if plan.status is not ManifestSelectionStatus.READY or plan.manifest is None:
        raise ValueError("Developer SHADOW cohort requires a ready selected manifest")
    if DetectorFamily.O2 not in plan.policy.unavailable_detectors:
        raise ValueError("Developer SHADOW cohort requires O2 to remain unavailable")
    effective = _utc(effective_at)
    if effective.date() != plan.session_date:
        raise ValueError("Developer SHADOW cohort must be prospective for its session")

    cursor.execute(
        "SELECT profile_sha256, config_sha256, provider_tier, universe_sha256, "
        "status, provider_requests, action_authorized FROM rt.capability_profiles "
        "WHERE profile_id=%s",
        (str(plan.scope.profile_id),),
    )
    profile = cursor.fetchone()
    if (
        profile is None
        or profile[0] != plan.scope.profile_sha256
        or profile[2] != ProviderTier.MASSIVE_DEVELOPER.value
        or profile[3] != plan.scope.universe.sha256
        or profile[4] not in {"FAIL", "UNAVAILABLE"}
        or profile[5] is not False
        or profile[6] is not False
    ):
        raise ValueError("Developer SHADOW cohort requires the blocked passive profile")
    configuration_sha256 = str(profile[1])
    model_bundle_sha256 = canonical_sha256({
        "version": "developer-selected-manifest-shadow-model-v1",
        "status": "NOT_MATERIALIZED",
        "required_detectors": tuple(
            detector.value for detector in plan.policy.required_detectors
        ),
        "unavailable_detectors": tuple(
            detector.value for detector in plan.policy.unavailable_detectors
        ),
    })
    valuation_sha256 = canonical_sha256({
        "version": "developer-selected-manifest-shadow-valuation-v2",
        "status": "NOT_MATERIALIZED",
        "manifest_sha256": option_universe_manifest_sha256(plan.manifest),
        "policy_sha256": plan.policy.sha256,
    })
    if cohort_binding is not None:
        configuration_sha256 = cohort_binding.configuration_sha256
        model_bundle_sha256 = cohort_binding.model_bundle_sha256
        valuation_sha256 = cohort_binding.valuation_sha256
    cohort_id, cohort_sha256 = _shadow_cohort_identity(
        manifest_id, configuration_sha256, model_bundle_sha256, valuation_sha256, effective,
    )
    cursor.execute(
        "INSERT INTO rt.cohorts ("
        "cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, 'SHADOW') "
        "ON CONFLICT (cohort_id) DO NOTHING",
        (
            str(cohort_id),
            cohort_sha256,
            str(manifest_id),
            configuration_sha256,
            model_bundle_sha256,
            valuation_sha256,
            effective,
        ),
    )
    cursor.execute(
        "SELECT cohort_sha256, manifest_id::text, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode "
        "FROM rt.cohorts WHERE cohort_id=%s",
        (str(cohort_id),),
    )
    stored = cursor.fetchone()
    if (
        stored is None
        or stored[:5] != (
            cohort_sha256,
            str(manifest_id),
            configuration_sha256,
            model_bundle_sha256,
            valuation_sha256,
        )
        or _utc(stored[5]) != effective
        or stored[6] != "SHADOW"
    ):
        raise ValueError("Developer SHADOW cohort identity conflict")
    return cohort_id