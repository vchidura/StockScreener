from datetime import datetime, timedelta, timezone
from uuid import UUID
from unittest.mock import MagicMock

import pytest

from options.domain import DataCapability
from realtime.preflight import (
    DetectorFamily,
    ProviderTier,
    TierState,
    transition_tier,
)
from realtime.tier_cohort import (
    ADVANCED_CAPABILITIES,
    DEVELOPER_CAPABILITIES,
    DEVELOPER_DISABLED_DETECTORS,
    TierCohortBinding,
    build_initial_developer_activation,
    build_tier_cohort_switch,
    persist_tier_cohort_activation,
    product_capabilities,
    product_disabled_detectors,
    persist_tier_cohort_binding_cursor,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
DEVELOPER_COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
DEVELOPER_PROFILE = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
ADVANCED_COHORT = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
ADVANCED_PROFILE = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def _developer():
    return TierCohortBinding(
        DEVELOPER_COHORT,
        DEVELOPER_PROFILE,
        ProviderTier.MASSIVE_DEVELOPER,
        TierState.DEVELOPER_READY,
        "a" * 64,
        "b" * 64,
        DEVELOPER_CAPABILITIES,
        DEVELOPER_DISABLED_DETECTORS,
        NOW,
    )


def _advanced():
    return TierCohortBinding(
        ADVANCED_COHORT,
        ADVANCED_PROFILE,
        ProviderTier.MASSIVE_ADVANCED,
        TierState.ADVANCED_READY,
        "c" * 64,
        "d" * 64,
        ADVANCED_CAPABILITIES,
        frozenset(),
        NOW + timedelta(minutes=10),
    )


def test_initial_developer_activation_requires_exact_reduced_capabilities_and_warmup():
    developer = _developer()
    activation = build_initial_developer_activation(
        developer,
        activated_at=NOW + timedelta(minutes=1),
        approval_reference="approved-developer-bootstrap",
        dependencies_warm=True,
    )

    assert activation.previous_cohort_id is None
    assert activation.target.provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert DetectorFamily.O2 in activation.target.disabled_detectors

    with pytest.raises(ValueError, match="reduced contract"):
        TierCohortBinding(
            DEVELOPER_COHORT,
            DEVELOPER_PROFILE,
            ProviderTier.MASSIVE_DEVELOPER,
            TierState.DEVELOPER_READY,
            "a" * 64,
            "b" * 64,
            DEVELOPER_CAPABILITIES | {DataCapability.OPTION_QUOTES},
            DEVELOPER_DISABLED_DETECTORS,
            NOW,
        )
    equity_only = TierCohortBinding(
        ADVANCED_COHORT,
        ADVANCED_PROFILE,
        ProviderTier.MASSIVE_ADVANCED,
        TierState.ADVANCED_READY,
        "c" * 64,
        "d" * 64,
        product_capabilities(ProviderTier.MASSIVE_ADVANCED, None),
        product_disabled_detectors(ProviderTier.MASSIVE_ADVANCED, None),
        NOW,
        options_enabled=False,
    )
    assert equity_only.stocks_tier is ProviderTier.MASSIVE_ADVANCED
    assert equity_only.options_tier is None

    with pytest.raises(ValueError, match="publication tier"):
        TierCohortBinding(
            ADVANCED_COHORT,
            ADVANCED_PROFILE,
            ProviderTier.MASSIVE_ADVANCED,
            TierState.ADVANCED_READY,
            "c" * 64,
            "d" * 64,
            product_capabilities(ProviderTier.MASSIVE_DEVELOPER, None),
            product_disabled_detectors(ProviderTier.MASSIVE_DEVELOPER, None),
            NOW,
            stocks_tier=ProviderTier.MASSIVE_DEVELOPER,
            options_enabled=False,
        )


def test_developer_to_advanced_switch_requires_approval_warmup_and_new_identity():
    developer = _developer()
    advanced = _advanced()
    warming = transition_tier(
        TierState.DEVELOPER_READY,
        TierState.ADVANCED_WARMING,
        transitioned_at=NOW + timedelta(minutes=2),
        reason_code="ADVANCED_ENTITLEMENT_APPROVED",
        operator_approved=True,
        operator_approval_reference="approved-advanced-switch",
    )
    ready = transition_tier(
        TierState.ADVANCED_WARMING,
        TierState.ADVANCED_READY,
        transitioned_at=NOW + timedelta(minutes=11),
        reason_code="ADVANCED_DEPENDENCIES_WARM",
        dependencies_warm=True,
    )

    activation = build_tier_cohort_switch(
        developer,
        advanced,
        (warming, ready),
        activated_at=NOW + timedelta(minutes=12),
    )

    assert activation.previous_cohort_id == DEVELOPER_COHORT
    assert activation.target.cohort_id == ADVANCED_COHORT
    assert activation.approval_reference == "approved-advanced-switch"
    assert activation.dependencies_warm is True

    with pytest.raises(ValueError, match="approval and warmed-ready"):
        build_tier_cohort_switch(
            developer,
            advanced,
            (ready,),
            activated_at=NOW + timedelta(minutes=12),
        )
    with pytest.raises(ValueError, match="precedes target readiness"):
        build_tier_cohort_switch(
            developer,
            advanced,
            (warming, ready),
            activated_at=NOW + timedelta(minutes=10, seconds=30),
        )


def test_initial_developer_activation_persists_before_portal_pointer():
    activation = build_initial_developer_activation(
        _developer(),
        activated_at=NOW + timedelta(minutes=1),
        approval_reference="approved-developer-bootstrap",
        dependencies_warm=True,
    )
    target = activation.target
    binding = (
        target.profile_id,
        target.provider_tier.value,
        target.stocks_tier.value,
        target.options_tier.value,
        target.ready_state.value,
        target.source_manifest_sha256,
        target.valuation_policy_sha256,
        sorted(value.value for value in target.capabilities),
        sorted(value.value for value in target.disabled_detectors),
        target.effective_at,
        target.sha256,
    )
    stored_activation = (
        None,
        target.cohort_id,
        target.provider_tier.value,
        [],
        activation.activated_at,
        activation.approval_reference,
        True,
        activation.activation_sha256,
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        binding,
        None,
        stored_activation,
        (activation.activation_id,),
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_tier_cohort_activation(connection, activation)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    activation_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.provider_tier_activations")
    )
    pointer_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.portal_cohort_current")
    )
    assert activation_index < pointer_index
    assert connection.commit.call_count == 1


def test_native_tier_binding_can_be_persisted_without_platform_activation():
    target = _developer()
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        target.profile_id, target.provider_tier.value, target.stocks_tier.value,
        target.options_tier.value, target.ready_state.value, target.source_manifest_sha256,
        target.valuation_policy_sha256, sorted(value.value for value in target.capabilities),
        sorted(value.value for value in target.disabled_detectors), target.effective_at, target.sha256,
    )

    persist_tier_cohort_binding_cursor(cursor, target)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert len(statements) == 2
    assert statements[0].startswith("INSERT INTO rt.provider_tier_cohorts")
    assert statements[1].startswith("SELECT profile_id")
    assert all("provider_tier_activations" not in statement and "portal_cohort_current" not in statement for statement in statements)