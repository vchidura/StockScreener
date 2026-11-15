from dataclasses import replace
from pathlib import Path

import pytest

from realtime.advanced_ingress_runtime_profile import (
    load_advanced_ingress_runtime_catalog,
)
from realtime.universe_limits import SCHEMA_MAX_UNIVERSE_NAMES


POLICY = (
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_ingress_v1.json"
)


def test_runtime_catalog_has_one_size_independent_profile_and_the_legacy_ladder():
    catalog = load_advanced_ingress_runtime_catalog(
        POLICY,
        expected_version="advanced-equity-ingress-v1",
    )

    assert catalog.profiles == (catalog.profile,)
    assert catalog.profile.version == "advanced-equity-ingress-v6"
    assert catalog.profile.universe_target == SCHEMA_MAX_UNIVERSE_NAMES
    assert (catalog.profile.feed, catalog.profile.channels) == ("STOCKS", ("AM",))
    assert tuple(
        profile.universe_target for profile in catalog.legacy_profiles
    ) == (1, 9, 25, 50, 100, 200, 400)
    assert tuple(
        profile.universe_target for profile in catalog.legacy_profiles
    ) == catalog.scale_policy.universe_ladder
    assert len({profile.sha256 for profile in catalog.legacy_profiles}) == 7
    # R62: the reviewed sizes never differed in behaviour; the size-independent profile keeps their settings.
    assert {profile.settings for profile in catalog.legacy_profiles} == {catalog.profile.settings}


@pytest.mark.parametrize("target", (1, 9, 10, 25, 37, 45, 50, 52, 100, 399, 400))
def test_every_size_uses_the_size_independent_profile(target):
    catalog = load_advanced_ingress_runtime_catalog(POLICY)

    assert catalog.profile_for_target(target) is catalog.profile
    assert catalog.profile_for_sha(catalog.profile.sha256) is catalog.profile


@pytest.mark.parametrize("target", (0, 401, 45.0, "45", True))
def test_sizes_outside_the_configured_maximum_are_refused(target, monkeypatch):
    monkeypatch.delenv("REALTIME_UNIVERSE_MAX_NAMES", raising=False)  # default 400
    catalog = load_advanced_ingress_runtime_catalog(POLICY)

    with pytest.raises(ValueError):
        catalog.profile_for_target(target)


def test_the_configured_maximum_moves_the_limit_without_a_new_profile(monkeypatch):
    catalog = load_advanced_ingress_runtime_catalog(POLICY)
    monkeypatch.setenv("REALTIME_UNIVERSE_MAX_NAMES", "600")
    assert catalog.profile_for_target(600) is catalog.profile
    with pytest.raises(ValueError, match="REALTIME_UNIVERSE_MAX_NAMES"):
        catalog.profile_for_target(601)
    monkeypatch.setenv("REALTIME_UNIVERSE_MAX_NAMES", "25")
    with pytest.raises(ValueError):
        catalog.profile_for_target(26)


@pytest.mark.parametrize(
    ("version", "sha_prefix"),
    (
        # The live rows (Nov 15): plans, cohorts and capability profiles stored before migration 053 pin these.
        ("advanced-equity-ingress-1-v5", "b604e574bcbc"),
        ("advanced-equity-ingress-9-v5", "0c5a018f8e60"),
        ("advanced-equity-ingress-25-v5", "84af4b94864b"),
        ("advanced-equity-ingress-v1-exact-45-capacity-50", "9917e7e7af3f"),
    ),
)
def test_legacy_profiles_stay_resolvable_by_sha(version, sha_prefix):
    catalog = load_advanced_ingress_runtime_catalog(POLICY)

    matches = [
        profile for sha, profile in catalog.legacy_by_sha.items()
        if sha.startswith(sha_prefix)
    ]
    assert [profile.version for profile in matches] == [version]
    assert catalog.profile_for_sha(matches[0].sha256) == matches[0]
    assert matches[0].settings == catalog.profile.settings


def test_legacy_exact_size_identity_is_unchanged():
    catalog = load_advanced_ingress_runtime_catalog(POLICY)
    exact = next(
        profile for profile in catalog.legacy_by_sha.values()
        if profile.version == "advanced-equity-ingress-v1-exact-37-capacity-50"
    )

    assert exact.universe_target == 37
    assert exact.settings == catalog.legacy_profiles[3].settings
    assert len(catalog.legacy_by_sha) == 400


def test_an_unknown_profile_sha_is_unavailable():
    catalog = load_advanced_ingress_runtime_catalog(POLICY)

    with pytest.raises(ValueError, match="unavailable"):
        catalog.profile_for_sha("0" * 64)


def test_profile_sha_contains_behavior_but_no_operational_scope():
    profile = load_advanced_ingress_runtime_catalog(
        POLICY
    ).profile_for_target(25)
    identity = profile.identity

    assert identity["universe_target"] == SCHEMA_MAX_UNIVERSE_NAMES
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
        == "advanced-real-time-closed-bar-v1"
    )
    assert profile.settings.rest_provider_delay_seconds == 0
    assert profile.settings.rest_publication_retry_grace_seconds == 10
    assert profile.settings.rest_maximum_attempts == 2
    changed = replace(
        profile,
        version="advanced-equity-ingress-1-retry-grace-test",
        settings=replace(
            profile.settings,
            rest_publication_retry_grace_seconds=11,
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
