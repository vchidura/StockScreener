"""Immutable technical runtime profiles for Advanced equity ingress."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from .advanced_equity_scale import (
    AdvancedScalePolicy,
    load_advanced_scale_policy,
)
from .contracts import canonical_json, canonical_sha256
from .universe_limits import SCHEMA_MAX_UNIVERSE_NAMES, max_universe_names

# The pre-053 catalog's range: legacy exact-size profile SHAs exist only up to it (history, not a limit).
_LEGACY_MAX_TARGET = 400


ADVANCED_RUNTIME_PROFILE_STATUSES = frozenset(
    {"CANDIDATE", "ACTIVE", "SUPERSEDED"}
)


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_number(value: object, name: str) -> float:
    if type(value) not in {int, float} or value <= 0:
        raise ValueError(f"{name} must be positive")
    return float(value)


def _nonnegative_int(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedIngressTechnicalSettings:
    maximum_message_bytes: int
    buffer_event_limit: int
    buffer_byte_limit: int
    buffer_max_age_nanoseconds: int
    journal_batch_messages: int
    journal_batch_bytes: int
    bridge_batch_limit: int
    rest_aggregate_endpoint_class: str
    rest_eligibility_policy_version: str
    rest_provider_delay_seconds: int
    rest_publication_retry_grace_seconds: int
    rest_maximum_attempts: int
    websocket_open_timeout_seconds: int
    websocket_ping_interval_seconds: int
    websocket_ping_timeout_seconds: int
    websocket_close_timeout_seconds: int
    websocket_library_max_queue: int
    websocket_maximum_control_messages: int
    reconnect_initial_backoff_seconds: float
    reconnect_backoff_multiplier: float
    reconnect_maximum_backoff_seconds: float
    reconnect_jitter_ceiling_seconds: float
    heartbeat_interval_seconds: int
    stale_after_seconds: int
    controller_warmup_seconds: int
    stable_live_reset_seconds: int
    minimum_journal_free_bytes: int
    minimum_journal_free_percent: int
    scheduler_window_retry_max_attempts: int
    scheduler_window_retry_initial_backoff_seconds: int
    scheduler_window_retry_max_backoff_seconds: int
    ingress_poll_interval_seconds: float
    bridge_poll_interval_seconds: float
    journal_read_timeout_seconds: float
    database_statement_timeout_ms: int
    bridge_statement_timeout_ms: int
    shutdown_deadline_seconds: int
    drain_deadline_seconds: int

    def __post_init__(self) -> None:
        integer_fields = (
            "maximum_message_bytes",
            "buffer_event_limit",
            "buffer_byte_limit",
            "buffer_max_age_nanoseconds",
            "journal_batch_messages",
            "journal_batch_bytes",
            "bridge_batch_limit",
            "rest_publication_retry_grace_seconds",
            "rest_maximum_attempts",
            "websocket_open_timeout_seconds",
            "websocket_ping_interval_seconds",
            "websocket_ping_timeout_seconds",
            "websocket_close_timeout_seconds",
            "websocket_library_max_queue",
            "websocket_maximum_control_messages",
            "heartbeat_interval_seconds",
            "stale_after_seconds",
            "controller_warmup_seconds",
            "stable_live_reset_seconds",
            "minimum_journal_free_bytes",
            "minimum_journal_free_percent",
            "scheduler_window_retry_max_attempts",
            "scheduler_window_retry_initial_backoff_seconds",
            "scheduler_window_retry_max_backoff_seconds",
            "database_statement_timeout_ms",
            "bridge_statement_timeout_ms",
            "shutdown_deadline_seconds",
            "drain_deadline_seconds",
        )
        number_fields = (
            "reconnect_initial_backoff_seconds",
            "reconnect_backoff_multiplier",
            "reconnect_maximum_backoff_seconds",
            "reconnect_jitter_ceiling_seconds",
            "ingress_poll_interval_seconds",
            "bridge_poll_interval_seconds",
            "journal_read_timeout_seconds",
        )
        for name in integer_fields:
            _positive_int(getattr(self, name), name)
        _nonnegative_int(
            self.rest_provider_delay_seconds,
            "rest_provider_delay_seconds",
        )
        for name in number_fields:
            _positive_number(getattr(self, name), name)
        if (
            self.rest_aggregate_endpoint_class
            != "MASSIVE_STOCKS_AGGREGATES_1M"
            or self.rest_eligibility_policy_version
            != "advanced-real-time-closed-bar-v1"
            or self.rest_provider_delay_seconds != 0
            or self.rest_maximum_attempts != 2
        ):
            raise ValueError(
                "Advanced REST eligibility policy identity is invalid"
            )
        if not 1_024 <= self.maximum_message_bytes <= 2 * 1024 * 1024:
            raise ValueError("maximum_message_bytes is outside gateway bounds")
        if self.buffer_byte_limit < self.maximum_message_bytes:
            raise ValueError("buffer bytes must admit one maximum message")
        if self.journal_batch_messages > self.buffer_event_limit:
            raise ValueError("journal batch exceeds the event buffer")
        if self.journal_batch_bytes > self.buffer_byte_limit:
            raise ValueError("journal batch exceeds the byte buffer")
        if self.bridge_batch_limit > 1_000:
            raise ValueError("bridge batch limit exceeds its reviewed bound")
        if self.stale_after_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("stale threshold must exceed heartbeat interval")
        if self.minimum_journal_free_percent > 100:
            raise ValueError("journal free-percent threshold exceeds 100")
        if (
            self.scheduler_window_retry_max_backoff_seconds
            < self.scheduler_window_retry_initial_backoff_seconds
        ):
            raise ValueError("scheduler retry backoff settings are invalid")
        if (
            self.reconnect_maximum_backoff_seconds
            < self.reconnect_initial_backoff_seconds
            or self.reconnect_jitter_ceiling_seconds
            > self.reconnect_maximum_backoff_seconds
        ):
            raise ValueError("reconnect/backoff settings are inconsistent")


@dataclass(frozen=True, slots=True)
class AdvancedIngressRuntimeProfile:
    version: str
    universe_target: int
    provider_tier: str
    feed: str
    channels: tuple[str, ...]
    settings: AdvancedIngressTechnicalSettings

    def __post_init__(self) -> None:
        normalized_version = self.version.strip()
        if not normalized_version or len(normalized_version) > 96:
            raise ValueError("runtime profile version is invalid")
        object.__setattr__(self, "version", normalized_version)
        if not 1 <= self.universe_target <= SCHEMA_MAX_UNIVERSE_NAMES:
            raise ValueError(f"runtime profile target must be in [1, {SCHEMA_MAX_UNIVERSE_NAMES}]")
        if (
            self.provider_tier != "MASSIVE_ADVANCED"
            or (self.feed, self.channels)
            not in {
                ("STOCKS", ("AM",)),
                ("OPTIONS", ("T", "Q")),
            }
        ):
            raise ValueError(
                "runtime profile must use an exact Advanced product scope"
            )

    @property
    def identity(self) -> dict[str, object]:
        return {
            "contract": (
                "advanced-equity-ingress-runtime-profile-v1"
                if self.feed == "STOCKS"
                else "advanced-options-ingress-runtime-profile-v1"
            ),
            "version": self.version,
            "universe_target": self.universe_target,
            "provider_tier": self.provider_tier,
            "feed": self.feed,
            "channels": self.channels,
            "settings": self.settings,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.identity)

    def configuration_sha256(self, runtime_policy_sha256: str) -> str:
        return canonical_sha256({
            "contract": (
                "advanced-equity-ingress-configuration-v1"
                if self.feed == "STOCKS"
                else "advanced-options-ingress-configuration-v1"
            ),
            "runtime_profile_sha256": self.sha256,
            "runtime_policy_sha256": runtime_policy_sha256,
            "provider_tier": self.provider_tier,
            "feed": self.feed,
            "channels": self.channels,
        })


def _legacy_profile_for_target(
    catalog_version: str,
    legacy_profiles: tuple[AdvancedIngressRuntimeProfile, ...],
    universe_target: int,
) -> AdvancedIngressRuntimeProfile:
    """The pre-R62 identity of a size: the reviewed ladder profile, or an exact-size copy of the next one."""
    matches = tuple(
        profile
        for profile in legacy_profiles
        if profile.universe_target == universe_target
    )
    if len(matches) == 1:
        return matches[0]
    if matches:
        raise ValueError(
            "runtime profile catalog contains duplicate target profiles"
        )
    capacity = next(
        (
            profile
            for profile in legacy_profiles
            if profile.universe_target >= universe_target
        ),
        None,
    )
    if capacity is None:
        raise ValueError("runtime profile target exceeds reviewed capacity")
    return AdvancedIngressRuntimeProfile(
        version=(
            f"{catalog_version}-exact-{universe_target}"
            f"-capacity-{capacity.universe_target}"
        ),
        universe_target=universe_target,
        provider_tier=capacity.provider_tier,
        feed=capacity.feed,
        channels=capacity.channels,
        settings=capacity.settings,
    )


@dataclass(frozen=True, slots=True)
class AdvancedIngressRuntimeCatalog:
    """G102 R62 (Nov 15): one size-independent profile serves every universe size up to
    REALTIME_UNIVERSE_MAX_NAMES (`universe_limits`).

    The seven reviewed ladder profiles (1, 9, 25, 50, 100, 200, 400) carried identical settings and the controller
    satisfied the promotion ladder with back-dated approvals of its own, so per-size profiles guarded nothing while
    every new size needed a profile row the app role cannot write (R62: the Nov 13 activation crash-looped). New plans
    pin `profile`; the legacy ladder and its exact-size copies stay resolvable by SHA only, so plans, cohorts and
    capability profiles stored before the change keep loading.
    """

    version: str
    path: Path
    scale_policy: AdvancedScalePolicy
    profile: AdvancedIngressRuntimeProfile
    legacy_profiles: tuple[AdvancedIngressRuntimeProfile, ...]
    legacy_by_sha: Mapping[str, AdvancedIngressRuntimeProfile] = field(
        default_factory=dict, compare=False, repr=False,
    )

    @property
    def profiles(self) -> tuple[AdvancedIngressRuntimeProfile, ...]:
        """The profiles new plans may pin (registered ACTIVE by migration 053)."""
        return (self.profile,)

    def profile_for_target(
        self,
        universe_target: int,
    ) -> AdvancedIngressRuntimeProfile:
        maximum = max_universe_names()
        if type(universe_target) is not int or not 1 <= universe_target <= maximum:
            raise ValueError(f"runtime profile target must be in [1, {maximum}] (REALTIME_UNIVERSE_MAX_NAMES)")
        if universe_target > self.profile.universe_target:
            raise ValueError("runtime profile target exceeds reviewed capacity")
        return self.profile

    def profile_for_sha(
        self,
        profile_sha256: str,
    ) -> AdvancedIngressRuntimeProfile:
        if profile_sha256 == self.profile.sha256:
            return self.profile
        profile = self.legacy_by_sha.get(profile_sha256)
        if profile is None:
            raise ValueError("runtime profile SHA is unavailable")
        return profile


_CATALOG_KEYS = frozenset({
    "version",
    "ladder_policy_file",
    "profile",
    "legacy_profiles",
})
_PROFILE_KEYS = frozenset({
    "version",
    "universe_target",
    "provider_tier",
    "feed",
    "channels",
    "settings",
})
_SETTINGS_KEYS = frozenset(
    AdvancedIngressTechnicalSettings.__dataclass_fields__
)


def load_advanced_ingress_runtime_catalog(
    path: Path,
    *,
    expected_version: str | None = None,
) -> AdvancedIngressRuntimeCatalog:
    resolved = path.resolve()
    value = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or frozenset(value) != _CATALOG_KEYS:
        raise ValueError("Advanced ingress runtime catalog keys changed")
    version = str(value["version"]).strip()
    if not version or (
        expected_version is not None and version != expected_version
    ):
        raise ValueError("Advanced ingress runtime catalog version changed")
    scale_path = resolved.parent / str(value["ladder_policy_file"])
    scale_policy = load_advanced_scale_policy(scale_path)
    profile = _load_profile(value["profile"])
    raw_profiles = value["legacy_profiles"]
    if not isinstance(raw_profiles, list):
        raise ValueError("legacy runtime profiles must be a list")
    legacy = tuple(_load_profile(raw_profile) for raw_profile in raw_profiles)
    targets = tuple(item.universe_target for item in legacy)
    if (
        targets != scale_policy.universe_ladder
        or len({item.version for item in legacy}) != len(legacy)
        or len({item.sha256 for item in legacy}) != len(legacy)
    ):
        raise ValueError(
            "legacy runtime profiles must exactly cover the versioned scale ladder"
        )
    if (
        profile.feed != "STOCKS"
        or profile.channels != ("AM",)
        or profile.universe_target != SCHEMA_MAX_UNIVERSE_NAMES
        or profile.version in {item.version for item in legacy}
    ):
        raise ValueError(
            f"the size-independent runtime profile must be a new STOCKS AM profile covering 1..{SCHEMA_MAX_UNIVERSE_NAMES}"
        )
    legacy_by_sha: dict[str, AdvancedIngressRuntimeProfile] = {}
    for target in range(1, _LEGACY_MAX_TARGET + 1):
        item = _legacy_profile_for_target(version, legacy, target)
        if item.sha256 in legacy_by_sha or item.sha256 == profile.sha256:
            raise ValueError("runtime profile catalog has an ambiguous profile SHA")
        legacy_by_sha[item.sha256] = item
    return AdvancedIngressRuntimeCatalog(
        version,
        resolved,
        scale_policy,
        profile,
        legacy,
        legacy_by_sha,
    )


def _load_profile(raw_profile: object) -> AdvancedIngressRuntimeProfile:
    if (
        not isinstance(raw_profile, dict)
        or frozenset(raw_profile) != _PROFILE_KEYS
    ):
        raise ValueError("Advanced ingress runtime profile keys changed")
    settings = raw_profile["settings"]
    if (
        not isinstance(settings, dict)
        or frozenset(settings) != _SETTINGS_KEYS
    ):
        raise ValueError(
            "Advanced ingress technical setting keys changed"
        )
    channels = raw_profile["channels"]
    if not isinstance(channels, list):
        raise ValueError("runtime profile channels must be a list")
    return AdvancedIngressRuntimeProfile(
        version=str(raw_profile["version"]),
        universe_target=int(raw_profile["universe_target"]),
        provider_tier=str(raw_profile["provider_tier"]),
        feed=str(raw_profile["feed"]),
        channels=tuple(str(channel) for channel in channels),
        settings=AdvancedIngressTechnicalSettings(**settings),
    )


SIZE_INDEPENDENT_PROFILE_APPROVAL_REFERENCE = (
    "g102-r62-migration-053-size-independent-runtime-profile"
)


def size_independent_profile_registration(
    catalog: AdvancedIngressRuntimeCatalog,
) -> dict[str, object]:
    """The exact row migration 053 registers ACTIVE for `catalog.profile` (also used by the admin check script).

    The evidence hashes are deterministic: the profile's settings are the reviewed ladder's settings unchanged, which
    is the stability and capacity basis it inherits.
    """
    profile = catalog.profile
    policy = catalog.scale_policy.sha256
    reviewed = tuple(item.sha256 for item in catalog.legacy_profiles)
    if any(item.settings != profile.settings for item in catalog.legacy_profiles):
        raise ValueError(
            "the size-independent profile must keep the reviewed ladder settings"
        )
    return {
        "runtime_profile_sha256": profile.sha256,
        "runtime_profile_version": profile.version,
        "universe_target": profile.universe_target,
        "ladder_position": 0,
        "previous_universe_target": None,
        "provider_tier": profile.provider_tier,
        "feed": profile.feed,
        "channels": profile.channels,
        "profile_document": canonical_json(profile.identity),
        "configuration_sha256": profile.configuration_sha256(policy),
        "runtime_policy_sha256": policy,
        "stability_evidence_sha256": canonical_sha256({
            "contract": "size-independent-runtime-profile-stability-v1",
            "profile_sha256": profile.sha256,
            "reviewed_ladder_profile_sha256s": reviewed,
            "basis": "identical settings to every reviewed ladder profile",
        }),
        "capacity_evidence_sha256": canonical_sha256({
            "contract": "size-independent-runtime-profile-capacity-v1",
            "profile_sha256": profile.sha256,
            "covers_universe_targets": (1, profile.universe_target),
            "reviewed_capacity_profile_sha256": reviewed[-1],
        }),
        "activation_approval_reference": SIZE_INDEPENDENT_PROFILE_APPROVAL_REFERENCE,
    }
