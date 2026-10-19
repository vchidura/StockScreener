"""Immutable technical runtime profiles for Advanced equity ingress."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .advanced_equity_scale import (
    AdvancedScalePolicy,
    load_advanced_scale_policy,
)
from .contracts import canonical_sha256


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
            != "conservative-unverified-delay-v1"
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
        if not 1 <= self.universe_target <= 400:
            raise ValueError("runtime profile target must be in [1, 400]")
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


@dataclass(frozen=True, slots=True)
class AdvancedIngressRuntimeCatalog:
    version: str
    path: Path
    scale_policy: AdvancedScalePolicy
    profiles: tuple[AdvancedIngressRuntimeProfile, ...]

    def profile_for_target(
        self,
        universe_target: int,
    ) -> AdvancedIngressRuntimeProfile:
        matches = tuple(
            profile
            for profile in self.profiles
            if profile.universe_target == universe_target
        )
        if len(matches) != 1:
            raise ValueError(
                "runtime profile catalog must contain exactly one target profile"
            )
        return matches[0]

    def profile_for_sha(
        self,
        profile_sha256: str,
    ) -> AdvancedIngressRuntimeProfile:
        matches = tuple(
            profile
            for profile in self.profiles
            if profile.sha256 == profile_sha256
        )
        if len(matches) != 1:
            raise ValueError("runtime profile SHA is unavailable")
        return matches[0]


_CATALOG_KEYS = frozenset({
    "version",
    "ladder_policy_file",
    "profiles",
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
    raw_profiles = value["profiles"]
    if not isinstance(raw_profiles, list):
        raise ValueError("runtime profiles must be a list")
    profiles = []
    for raw_profile in raw_profiles:
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
        profiles.append(
            AdvancedIngressRuntimeProfile(
                version=str(raw_profile["version"]),
                universe_target=int(raw_profile["universe_target"]),
                provider_tier=str(raw_profile["provider_tier"]),
                feed=str(raw_profile["feed"]),
                channels=tuple(str(channel) for channel in channels),
                settings=AdvancedIngressTechnicalSettings(**settings),
            )
        )
    targets = tuple(profile.universe_target for profile in profiles)
    if (
        targets != scale_policy.universe_ladder
        or len({profile.version for profile in profiles}) != len(profiles)
        or len({profile.sha256 for profile in profiles}) != len(profiles)
    ):
        raise ValueError(
            "runtime profiles must exactly cover the versioned scale ladder"
        )
    return AdvancedIngressRuntimeCatalog(
        version,
        resolved,
        scale_policy,
        tuple(profiles),
    )
