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


def test_advanced_rest_eligibility_is_explicit_and_hash_bound():
    profile = load_advanced_ingress_runtime_catalog(
        POLICY
    ).profile_for_target(1)

    assert (
        profile.settings.rest_aggregate_endpoint_class
        == "MASSIVE_STOCKS_AGGREGATES_1M"
    )
    assert (
        profile.settings.rest_eligibility_policy_version
        == "conservative-unverified-delay-v1"
    )
    assert profile.settings.rest_provider_delay_seconds == 900
    changed = replace(
        profile,
        version="advanced-equity-ingress-1-provider-delay-test",
        settings=replace(
            profile.settings,
            rest_provider_delay_seconds=0,
        ),
    )

    assert changed.sha256 != profile.sha256
    assert changed.configuration_sha256("a" * 64) != (
        profile.configuration_sha256("a" * 64)
    )


def test_options_profile_reuses_settings_with_exact_tq_product_identity():
    equity = load_advanced_ingress_runtime_catalog(
        POLICY
    ).profile_for_target(9)
    options = replace(
        equity,
        version="advanced-options-ingress-9-v1",
        feed="OPTIONS",
        channels=("T", "Q"),
    )

    assert options.identity["contract"] == (
        "advanced-options-ingress-runtime-profile-v1"
    )
    assert options.sha256 != equity.sha256
    assert options.configuration_sha256("a" * 64) != (
        equity.configuration_sha256("a" * 64)
    )
