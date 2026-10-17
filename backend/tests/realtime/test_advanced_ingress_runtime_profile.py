from dataclasses import replace
from pathlib import Path

from realtime.advanced_ingress_runtime_profile import (
    load_advanced_ingress_runtime_catalog,
)


POLICY = (
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_ingress_v1.json"
)


def test_runtime_catalog_exactly_covers_the_versioned_ladder():
    catalog = load_advanced_ingress_runtime_catalog(
        POLICY,
        expected_version="advanced-equity-ingress-v1",
    )

    assert tuple(
        profile.universe_target for profile in catalog.profiles
    ) == (1, 9, 25, 50, 100, 200, 400)
    assert tuple(
        profile.universe_target for profile in catalog.profiles
    ) == catalog.scale_policy.universe_ladder
    assert len({profile.sha256 for profile in catalog.profiles}) == 7


def test_profile_sha_contains_behavior_but_no_operational_scope():
    profile = load_advanced_ingress_runtime_catalog(
        POLICY
    ).profile_for_target(25)
    identity = profile.identity

    assert identity["universe_target"] == 25
    assert identity["feed"] == "STOCKS"
    assert identity["channels"] == ("AM",)
    assert set(identity) == {
        "contract",
        "version",
        "universe_target",
        "provider_tier",
        "feed",
        "channels",
        "settings",
    }
    serialized = repr(identity).lower()
    for excluded in (
        "secret",
        "cohort_id",
        "session_date",
        "authorization_id",
        "approved_byte",
        "approved_request",
    ):
        assert excluded not in serialized


def test_any_behavior_change_requires_a_new_profile_sha():
    profile = load_advanced_ingress_runtime_catalog(
        POLICY
    ).profile_for_target(1)
    changed = replace(
        profile,
        version="advanced-equity-ingress-1-v2",
        settings=replace(
            profile.settings,
            heartbeat_interval_seconds=21,
        ),
    )

    assert changed.sha256 != profile.sha256
    assert profile.configuration_sha256("a" * 64) != (
        changed.configuration_sha256("a" * 64)
    )
