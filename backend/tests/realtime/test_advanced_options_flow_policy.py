from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from realtime.advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
    DEFAULT_ADVANCED_OPTIONS_FLOW_POLICY_PATH,
    FlowDteLane,
    FlowProfileKind,
    FlowThresholdMode,
    FlowUniverseTier,
    load_advanced_options_flow_policy,
)


def _scope(
    policy,
    *,
    entry=AdvancedOptionsContractScopeEntry(
        "O:SPY261019C00650000",
        "SPY",
        date(2026, 10, 19),
        FlowUniverseTier.ETF,
        FlowDteLane.CORE_SWING,
    ),
    flow_policy_sha256=None,
):
    return AdvancedOptionsContractScope(
        version="advanced-options-contract-scope-v1",
        session_date=date(2026, 10, 12),
        manifest_sha256="1" * 64,
        slot_sha256="2" * 64,
        flow_policy_sha256=flow_policy_sha256 or policy.sha256,
        contracts=(entry,),
    )


def test_policy_freezes_reviewed_tiers_lanes_profiles_and_caps():
    policy = load_advanced_options_flow_policy()

    assert policy.version == "advanced-options-flow-v1"
    assert policy.tier_for("SPY") is FlowUniverseTier.ETF
    assert policy.tier_for("aapl") is FlowUniverseTier.CORE_MEGA_CAP
    assert policy.tier_for("TSLA") is (
        FlowUniverseTier.HIGH_BETA_HIGH_PREMIUM
    )
    assert policy.lane_for(0) is FlowDteLane.ULTRA_SHORT
    assert policy.lane_for(2) is FlowDteLane.ULTRA_SHORT
    assert policy.lane_for(3) is None
    assert policy.lane_for(4) is None
    assert policy.lane_for(5) is FlowDteLane.CORE_SWING
    assert policy.lane_for(21) is FlowDteLane.CORE_SWING
    assert policy.lane_for(22) is FlowDteLane.POSITION
    assert policy.lane_for(60) is FlowDteLane.POSITION
    assert policy.sha256 == (
        "a09fa7207c4cff7e028e54ae42ff87400ea1670247906bd2b4b4c3ebbd3d2833"
    )
    assert policy.candidate_caps.global_maximum == 20
    assert dict(policy.candidate_caps.profile_budgets) == {
        FlowProfileKind.ULTRA_SHORT: 4,
        FlowProfileKind.BLOCK: 6,
        FlowProfileKind.SWEEP: 6,
        FlowProfileKind.ACCUMULATION: 4,
    }


def test_tiered_profile_thresholds_match_the_reviewed_initial_policy():
    policy = load_advanced_options_flow_policy()
    ultra = policy.profile(FlowProfileKind.ULTRA_SHORT)
    block = policy.profile(FlowProfileKind.BLOCK)
    sweep = policy.profile(FlowProfileKind.SWEEP)
    accumulation = policy.profile(FlowProfileKind.ACCUMULATION)

    assert all(
        profile.threshold_mode is FlowThresholdMode.OR
        for profile in policy.profiles
    )
    assert ultra.threshold_for(FlowUniverseTier.ETF).minimum_contracts == 1000
    assert ultra.threshold_for(
        FlowUniverseTier.HIGH_BETA_HIGH_PREMIUM
    ).minimum_print_premium == Decimal("400000")
    assert ultra.window_for(FlowDteLane.ULTRA_SHORT) == 5
    assert ultra.retain_inside_spread_as_context
    assert block.threshold_for(
        FlowUniverseTier.CORE_MEGA_CAP
    ).minimum_print_premium == Decimal("150000")
    assert block.window_for(FlowDteLane.CORE_SWING) is None
    assert sweep.threshold_for(
        FlowUniverseTier.HIGH_BETA_HIGH_PREMIUM
    ).minimum_cluster_prints == 5
    assert sweep.threshold_for(
        FlowUniverseTier.HIGH_BETA_HIGH_PREMIUM
    ).minimum_cumulative_contracts is None
    assert sweep.window_for(FlowDteLane.CORE_SWING) == 5
    assert accumulation.threshold_for(
        FlowUniverseTier.ETF
    ).minimum_cumulative_contracts == 2000
    assert accumulation.window_for(FlowDteLane.CORE_SWING) == 180
    assert accumulation.window_for(FlowDteLane.POSITION) == 900
    assert policy.opening_overlay.minimum_prior_open_interest == 250
    assert policy.opening_overlay.unusual_volume_oi_ratio == 2.0
    assert policy.opening_overlay.high_conviction_volume_oi_ratio == 3.0


@pytest.mark.parametrize("kind", tuple(FlowProfileKind))
@pytest.mark.parametrize("tier", tuple(FlowUniverseTier))
def test_every_profile_tier_relationship_changes_policy_identity(kind, tier):
    policy = load_advanced_options_flow_policy()
    profile = policy.profile(kind)
    changed_profile = replace(
        profile,
        tier_thresholds=tuple(
            replace(row, minimum_contracts=row.minimum_contracts + 1)
            if row.tier is tier
            else row
            for row in profile.tier_thresholds
        ),
    )
    changed_policy = replace(
        policy,
        profiles=tuple(
            changed_profile if row.kind is kind else row
            for row in policy.profiles
        ),
    )

    assert changed_policy.sha256 != policy.sha256


@pytest.mark.parametrize(
    ("kind", "lane"),
    (
        (FlowProfileKind.ULTRA_SHORT, FlowDteLane.ULTRA_SHORT),
        (FlowProfileKind.SWEEP, FlowDteLane.CORE_SWING),
        (FlowProfileKind.ACCUMULATION, FlowDteLane.CORE_SWING),
        (FlowProfileKind.ACCUMULATION, FlowDteLane.POSITION),
    ),
)
def test_every_windowed_profile_lane_relationship_changes_policy_identity(
    kind,
    lane,
):
    policy = load_advanced_options_flow_policy()
    profile = policy.profile(kind)
    changed_profile = replace(
        profile,
        window_seconds_by_lane=tuple(
            (item_lane, seconds + 1 if item_lane is lane else seconds)
            for item_lane, seconds in profile.window_seconds_by_lane
        ),
    )
    changed_policy = replace(
        policy,
        profiles=tuple(
            changed_profile if row.kind is kind else row
            for row in policy.profiles
        ),
    )

    assert changed_policy.sha256 != policy.sha256


def test_every_universe_tier_membership_change_changes_policy_identity():
    policy = load_advanced_options_flow_policy()
    etf = next(
        row for row in policy.universe if row.tier is FlowUniverseTier.ETF
    )
    core_membership = next(
        row
        for row in policy.universe
        if row.tier is FlowUniverseTier.CORE_MEGA_CAP
    )
    changed_membership = replace(
        policy,
        universe=tuple(
            replace(
                row,
                underlyings=tuple(
                    value for value in row.underlyings if value != "IWM"
                ),
            )
            if row.tier is etf.tier
            else replace(
                row,
                underlyings=(*row.underlyings, "IWM"),
            )
            if row.tier is core_membership.tier
            else row
            for row in policy.universe
        ),
    )

    assert changed_membership.sha256 != policy.sha256


@pytest.mark.parametrize(
    ("kind", "invalid_lanes"),
    (
        (FlowProfileKind.ULTRA_SHORT, (FlowDteLane.CORE_SWING,)),
        (FlowProfileKind.BLOCK, (FlowDteLane.CORE_SWING,)),
        (FlowProfileKind.SWEEP, (FlowDteLane.POSITION,)),
        (FlowProfileKind.ACCUMULATION, (FlowDteLane.POSITION,)),
    ),
)
def test_profile_lane_mapping_is_exact(kind, invalid_lanes):
    profile = load_advanced_options_flow_policy().profile(kind)

    with pytest.raises(ValueError, match="DTE lanes"):
        replace(profile, dte_lanes=invalid_lanes)


def test_cumulative_contract_threshold_scope_is_exact():
    policy = load_advanced_options_flow_policy()
    ultra = policy.profile(FlowProfileKind.ULTRA_SHORT)
    sweep = policy.profile(FlowProfileKind.SWEEP)

    with pytest.raises(ValueError, match="cumulative contract threshold scope"):
        replace(
            ultra,
            tier_thresholds=tuple(
                replace(row, minimum_cumulative_contracts=None)
                if row.tier is FlowUniverseTier.ETF
                else row
                for row in ultra.tier_thresholds
            ),
        )
    with pytest.raises(ValueError, match="cumulative contract threshold scope"):
        replace(
            sweep,
            tier_thresholds=tuple(
                replace(row, minimum_cumulative_contracts=1)
                if row.tier is FlowUniverseTier.ETF
                else row
                for row in sweep.tier_thresholds
            ),
        )


def test_duplicate_directional_side_and_confidence_entries_fail_closed():
    policy = load_advanced_options_flow_policy()
    sweep = policy.profile(FlowProfileKind.SWEEP)

    with pytest.raises(ValueError, match="directional evidence"):
        replace(
            sweep,
            directional_side_classes=(
                *sweep.directional_side_classes,
                sweep.directional_side_classes[0],
            ),
        )
    with pytest.raises(ValueError, match="directional evidence"):
        replace(
            sweep,
            directional_confidences=(
                *sweep.directional_confidences,
                sweep.directional_confidences[0],
            ),
        )
    with pytest.raises(ValueError, match="opening overlay"):
        replace(
            policy.opening_overlay,
            directional_side_classes=(
                *policy.opening_overlay.directional_side_classes,
                policy.opening_overlay.directional_side_classes[0],
            ),
        )
    with pytest.raises(ValueError, match="opening overlay"):
        replace(
            policy.opening_overlay,
            directional_confidences=(
                *policy.opening_overlay.directional_confidences,
                policy.opening_overlay.directional_confidences[0],
            ),
        )


def test_contract_scope_binds_contract_manifest_slot_and_policy_identities():
    policy = load_advanced_options_flow_policy()
    scope = _scope(policy)

    scope.validate_policy(policy)
    assert scope.contract_map[
        "O:SPY261019C00650000"
    ] == scope.contracts[0]
    assert scope.sha256 != replace(
        scope,
        manifest_sha256="3" * 64,
    ).sha256
    assert scope.sha256 != replace(
        scope,
        slot_sha256="4" * 64,
    ).sha256
    assert scope.sha256 != replace(
        scope,
        session_date=date(2026, 10, 13),
    ).sha256


def test_contract_scope_rejects_duplicate_mismatched_and_unowned_contracts():
    policy = load_advanced_options_flow_policy()
    scope = _scope(policy)

    with pytest.raises(ValueError, match="scope is invalid"):
        replace(scope, contracts=(scope.contracts[0], scope.contracts[0]))
    with pytest.raises(ValueError, match="ownership"):
        replace(scope.contracts[0], underlyer="QQQ")
    with pytest.raises(ValueError, match="expiration"):
        replace(scope.contracts[0], expiration=date(2026, 10, 20))
    with pytest.raises(ValueError, match="policy identity"):
        _scope(policy, flow_policy_sha256="f" * 64).validate_policy(policy)
    with pytest.raises(ValueError, match="tier or DTE lane"):
        replace(
            scope,
            contracts=(
                replace(
                    scope.contracts[0],
                    dte_lane=FlowDteLane.POSITION,
                ),
            ),
        ).validate_policy(policy)


def test_invalid_scope_and_incomplete_cluster_shapes_fail_closed():
    policy = load_advanced_options_flow_policy()
    with pytest.raises(ValueError, match="outside"):
        policy.tier_for("META")
    with pytest.raises(ValueError, match="DTE"):
        policy.lane_for(61)

    sweep = policy.profile(FlowProfileKind.SWEEP)
    with pytest.raises(ValueError, match="cluster threshold"):
        replace(
            sweep,
            tier_thresholds=tuple(
                replace(row, minimum_cluster_prints=None)
                if row.tier is FlowUniverseTier.ETF
                else row
                for row in sweep.tier_thresholds
            ),
        )


def test_policy_loader_rejects_unknown_root_keys(tmp_path):
    text = DEFAULT_ADVANCED_OPTIONS_FLOW_POLICY_PATH.read_text(
        encoding="utf-8"
    )
    path = tmp_path / "changed.json"
    path.write_text(
        text.replace(
            '"version": "advanced-options-flow-v1",',
            '"version": "advanced-options-flow-v1",\n  "unexpected": true,',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="keys changed"):
        load_advanced_options_flow_policy(path)
