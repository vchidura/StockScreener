from pathlib import Path

from realtime.advanced_equity_scale import (
    AdvancedScaleAction,
    AdvancedScaleMetrics,
    load_advanced_scale_policy,
    recommend_advanced_scale,
)


POLICY = (
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_scale_v1.json"
)


def _metrics(**changes):
    values = {
        "metrics_complete": True,
        "current_target": 1,
        "stable_sessions": 0,
        "unresolved_gaps": 0,
        "quarantines": 0,
        "duplicate_bars": 0,
        "release_failures": 0,
        "p95_lag_ms": 1000,
        "p99_lag_ms": 2000,
        "maximum_buffer_utilization": 0.1,
        "memory_headroom_percent": 70,
        "database_headroom_percent": 70,
    }
    values.update(changes)
    return AdvancedScaleMetrics(**values)


def test_manual_ladder_holds_until_five_stable_sessions_then_moves_one_step():
    policy = load_advanced_scale_policy(POLICY)

    hold = recommend_advanced_scale(
        policy,
        _metrics(stable_sessions=4),
        desired_target=9,
    )
    up = recommend_advanced_scale(
        policy,
        _metrics(stable_sessions=5),
        desired_target=9,
    )

    assert hold.action is AdvancedScaleAction.HOLD
    assert hold.proposed_target == 1
    assert up.action is AdvancedScaleAction.UP
    assert up.proposed_target == 9
    assert up.requires_operator_approval


def test_correctness_or_capacity_failure_proposes_one_step_down():
    policy = load_advanced_scale_policy(POLICY)
    recommendation = recommend_advanced_scale(
        policy,
        _metrics(
            current_target=25,
            stable_sessions=5,
            unresolved_gaps=1,
            memory_headroom_percent=20,
        ),
        desired_target=100,
    )

    assert recommendation.action is AdvancedScaleAction.DOWN
    assert recommendation.proposed_target == 9
    assert set(recommendation.reason_codes) == {
        "UNRESOLVED_GAPS",
        "MEMORY_HEADROOM_LOW",
    }


def test_desired_target_decrease_moves_only_one_ladder_step():
    policy = load_advanced_scale_policy(POLICY)
    recommendation = recommend_advanced_scale(
        policy,
        _metrics(current_target=100, stable_sessions=5),
        desired_target=25,
    )

    assert recommendation.action is AdvancedScaleAction.DOWN
    assert recommendation.proposed_target == 50


def test_incomplete_metrics_never_propose_scale_up():
    policy = load_advanced_scale_policy(POLICY)
    recommendation = recommend_advanced_scale(
        policy,
        _metrics(metrics_complete=False, stable_sessions=99),
        desired_target=9,
    )

    assert recommendation.action is AdvancedScaleAction.HOLD
    assert recommendation.proposed_target == 1
    assert recommendation.reason_codes == ("METRICS_INCOMPLETE",)


def test_scale_up_never_skips_25_or_100_without_a_new_ladder_policy():
    policy = load_advanced_scale_policy(POLICY)

    from_one = recommend_advanced_scale(
        policy,
        _metrics(current_target=1, stable_sessions=5),
        desired_target=400,
    )
    from_nine = recommend_advanced_scale(
        policy,
        _metrics(current_target=9, stable_sessions=5),
        desired_target=400,
    )
    from_fifty = recommend_advanced_scale(
        policy,
        _metrics(current_target=50, stable_sessions=5),
        desired_target=400,
    )

    assert from_one.proposed_target == 9
    assert from_nine.proposed_target == 25
    assert from_fifty.proposed_target == 100
