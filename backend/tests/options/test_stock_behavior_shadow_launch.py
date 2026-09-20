from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from equity.behavior import DEFINITION_SHA256, OPTIONS_SWING_PROFILE
from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY
from options.stock_behavior_shadow_launch import (
    OptionStockBehaviorShadowLaunch,
    load_option_stock_behavior_shadow_launch,
)


BACKEND_DIR = Path(__file__).resolve().parents[2]


def launch(**updates):
    values = dict(
        schema_version="option_stock_behavior_shadow_launch_v1",
        launch_id="options-stock-behavior-test-v1",
        starts_at=datetime(2026, 9, 21, 13, 30, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 21, 20, 1, tzinfo=timezone.utc),
        underlyers=("AAPL", "SPY"), maximum_assessments=1000,
        maximum_payload_bytes=50_000_000, maximum_candidates_per_matrix=100,
        minimum_assessments_before_rate_stops=25,
        maximum_unavailable_fraction=0.5,
        maximum_p95_decision_lag_seconds=120,
        artifact_destination="backups/options-stock-behavior/test/status.json",
        detector_policy_sha256=STOCK_BEHAVIOR_GATE_POLICY.sha256,
        behavior_definition_sha256=DEFINITION_SHA256,
        behavior_policy_sha256=OPTIONS_SWING_PROFILE.sha256,
        assessment_only=True, execution_permission=False,
    )
    values.update(updates)
    return OptionStockBehaviorShadowLaunch(**values)


def test_shadow_launch_is_frozen_bounded_and_hash_stable():
    first = launch()
    second = launch()

    assert first == second
    assert first.sha256 == second.sha256
    with pytest.raises(ValidationError):
        first.maximum_assessments = 2


def test_continuous_shadow_launch_uses_rolling_window_without_end():
    continuous = launch(
        schema_version="option_stock_behavior_shadow_launch_v2",
        mode="CONTINUOUS_DEVELOPMENT",
        ends_at=None,
        usage_window_seconds=86400,
    )
    current = datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc)

    assert continuous.accepts(current) is True
    assert continuous.usage_bounds(current) == (current - timedelta(days=1), current)


def test_checked_in_continuous_development_manifest_is_current_and_non_executing():
    manifest = load_option_stock_behavior_shadow_launch(
        BACKEND_DIR / "research" / "options_stock_behavior_continuous_development_v1.json"
    )

    assert manifest.mode == "CONTINUOUS_DEVELOPMENT"
    assert manifest.ends_at is None
    assert manifest.usage_window_seconds == 86400
    assert manifest.assessment_only is True
    assert manifest.execution_permission is False
    assert STOCK_BEHAVIOR_GATE_POLICY.sha256 == "f3fddf6aadb5dc9688fbf3dcf4250113a64fb48b0a3b2d35b55400232fe7a8dd"
    assert DEFINITION_SHA256 == "d8c85fc91b69c83af5081d4f77877d8c93fa1653d98d444820b4a2e62390af30"
    assert OPTIONS_SWING_PROFILE.sha256 == "67d75381c5964d97a292c902345c999d4694965c60e16d0d046539e30dbfc39f"
    assert manifest.sha256 == "99c10a4fc93a94c630fdead85f7e52cf10cbe9f5047636097da6d230c6122930"


@pytest.mark.parametrize("updates", [
    {"schema_version": "option_stock_behavior_shadow_launch_v1",
     "mode": "CONTINUOUS_DEVELOPMENT", "ends_at": None},
    {"schema_version": "option_stock_behavior_shadow_launch_v2",
     "mode": "CONTINUOUS_DEVELOPMENT"},
])
def test_continuous_shadow_launch_rejects_incoherent_version_or_end(updates):
    with pytest.raises(ValidationError):
        launch(**updates)


@pytest.mark.parametrize("updates", [
    {"ends_at": datetime(2026, 9, 29, 13, 30, tzinfo=timezone.utc)},
    {"underlyers": ("AAPL", "AAPL")},
    {"maximum_assessments": 10,
     "minimum_assessments_before_rate_stops": 11},
    {"artifact_destination": "../outside.json"},
    {"detector_policy_sha256": "0" * 64},
    {"assessment_only": False},
    {"execution_permission": True},
])
def test_shadow_launch_rejects_unbounded_or_unregistered_contracts(updates):
    with pytest.raises(ValidationError):
        launch(**updates)