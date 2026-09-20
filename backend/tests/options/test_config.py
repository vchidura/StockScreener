import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import (
    load_developer_policy,
    load_option_runtime_configuration,
    load_valuation_policy,
)
from equity.behavior import DEFINITION_SHA256, OPTIONS_SWING_PROFILE
from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY


def _environment(**overrides: str) -> dict[str, str]:
    environment = {"POLYGON_API_KEY": "test-secret"}
    environment.update(overrides)
    return environment


def test_runtime_configuration_is_frozen_read_only_and_secret_safe():
    runtime = load_option_runtime_configuration(_environment(), BACKEND_DIR)

    assert runtime.settings.start_read_only is True
    assert runtime.settings.stock_behavior_shadow_enabled is False
    assert runtime.settings.package_assessments_enabled is False
    assert runtime.settings.baseline_alerts_enabled is False
    assert runtime.settings.baseline_alerts_effective_from is None
    assert runtime.settings.outcome_unavailable_evidence_enabled is False
    assert runtime.settings.underlyers == (
        "AAPL",
        "AMD",
        "AMZN",
        "GOOGL",
        "META",
        "MSFT",
        "NVDA",
        "PLTR",
        "SOFI",
        "TSLA",
        "SPY",
        "QQQ",
        "IWM",
    )
    assert runtime.settings.policy_file == BACKEND_DIR / "options/policies/developer_v1.json"
    assert runtime.settings.valuation_policy_file == (
        BACKEND_DIR / "options/policies/valuation_v1.json"
    )
    assert runtime.settings.settlement_valuation_policy_file == (
        BACKEND_DIR / "options/policies/settlement_valuation_v1.json"
    )
    assert "test-secret" not in repr(runtime)
    assert "polygon_api_key" not in runtime.metadata()
    assert len(runtime.policy_sha256) == 64
    assert runtime.developer_policy_sha256 == load_developer_policy(
        runtime.settings.policy_file
    ).sha256
    assert runtime.valuation_policy.policy_version == "option_valuation_v1"
    assert runtime.valuation_policy_sha256 == runtime.valuation_policy.policy_sha256
    assert runtime.settlement_valuation_policy.policy_version == (
        "option_settlement_valuation_v1"
    )
    assert runtime.settlement_valuation_policy.option_aggregates_adjusted is False
    assert runtime.policy.contract_filter.maximum_dte == 60
    assert runtime.settlement_valuation_policy_sha256 == (
        runtime.settlement_valuation_policy.policy_sha256
    )
    assert runtime.policy_sha256 != runtime.developer_policy_sha256
    assert len(runtime.configuration_sha256) == 64

    with pytest.raises(ValidationError):
        runtime.settings.start_read_only = False
    with pytest.raises(ValidationError):
        runtime.policy.contract_filter.maximum_dte = 30


def test_baseline_alerts_require_aware_effective_date_without_changing_source_identity():
    disabled = load_option_runtime_configuration(_environment(), BACKEND_DIR)
    with pytest.raises(ValueError, match="EFFECTIVE_FROM"):
        load_option_runtime_configuration(_environment(OPTION_BASELINE_ALERTS_ENABLED="true"), BACKEND_DIR)
    with pytest.raises(ValueError, match="timezone-aware"):
        load_option_runtime_configuration(_environment(OPTION_BASELINE_ALERTS_ENABLED="true",
            OPTION_BASELINE_ALERTS_EFFECTIVE_FROM="2026-09-21T13:30:00"), BACKEND_DIR)
    enabled = load_option_runtime_configuration(_environment(OPTION_BASELINE_ALERTS_ENABLED="true",
        OPTION_BASELINE_ALERTS_EFFECTIVE_FROM="2026-09-21T13:30:00Z"), BACKEND_DIR)
    assert enabled.settings.baseline_alerts_enabled
    assert enabled.configuration_sha256 == disabled.configuration_sha256
    assert enabled.strategy_policy_sha256 == disabled.strategy_policy_sha256


def test_stock_behavior_shadow_is_explicit_and_not_part_of_market_configuration_identity(
    tmp_path: Path,
):
    disabled = load_option_runtime_configuration(_environment(), BACKEND_DIR)
    with pytest.raises(ValueError, match="LAUNCH_FILE"):
        load_option_runtime_configuration(
            _environment(OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED="true"), BACKEND_DIR,
        )
    launch_path = tmp_path / "launch.json"
    launch_path.write_text(json.dumps({
        "schema_version": "option_stock_behavior_shadow_launch_v1",
        "launch_id": "options-stock-behavior-test-v1",
        "starts_at": "2026-09-21T13:30:00Z",
        "ends_at": "2026-09-21T20:30:00Z",
        "underlyers": list(disabled.settings.underlyers),
        "maximum_assessments": 1000,
        "maximum_payload_bytes": 50000000,
        "maximum_candidates_per_matrix": 100,
        "minimum_assessments_before_rate_stops": 25,
        "maximum_unavailable_fraction": 0.5,
        "maximum_p95_decision_lag_seconds": 120,
        "artifact_destination": "backups/options-stock-behavior/test/status.json",
        "detector_policy_sha256": STOCK_BEHAVIOR_GATE_POLICY.sha256,
        "behavior_definition_sha256": DEFINITION_SHA256,
        "behavior_policy_sha256": OPTIONS_SWING_PROFILE.sha256,
        "assessment_only": True,
        "execution_permission": False,
    }), encoding="utf-8")
    enabled = load_option_runtime_configuration(
        _environment(
            OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED="true",
            OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE=str(launch_path),
        ),
        BACKEND_DIR,
    )

    assert enabled.settings.stock_behavior_shadow_enabled is True
    assert enabled.stock_behavior_shadow_launch is not None
    assert enabled.stock_behavior_shadow_launch.launch_id == "options-stock-behavior-test-v1"
    assert len(enabled.stock_behavior_shadow_launch_sha256 or "") == 64
    assert enabled.configuration_sha256 == disabled.configuration_sha256


def test_package_assessment_switch_does_not_change_market_configuration_identity():
    disabled = load_option_runtime_configuration(_environment(), BACKEND_DIR)
    enabled = load_option_runtime_configuration(
        _environment(OPTION_PACKAGE_ASSESSMENTS_ENABLED="true"), BACKEND_DIR,
    )

    assert enabled.settings.package_assessments_enabled is True
    assert enabled.configuration_sha256 == disabled.configuration_sha256


def test_outcome_unavailable_switch_does_not_change_market_configuration_identity():
    disabled = load_option_runtime_configuration(_environment(), BACKEND_DIR)
    enabled = load_option_runtime_configuration(
        _environment(OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED="true"),
        BACKEND_DIR,
    )

    assert enabled.settings.outcome_unavailable_evidence_enabled is True
    assert enabled.configuration_sha256 == disabled.configuration_sha256


def test_policy_fingerprint_uses_canonical_validated_content(tmp_path: Path):
    original_path = BACKEND_DIR / "options/policies/developer_v1.json"
    payload = json.loads(original_path.read_text(encoding="utf-8"))
    reformatted_path = tmp_path / "policy.json"
    reformatted_path.write_text(json.dumps(payload, indent=4, sort_keys=True), encoding="utf-8")

    original = load_developer_policy(original_path)
    reformatted = load_developer_policy(reformatted_path)

    assert original.sha256 == reformatted.sha256


def test_valuation_policy_fingerprint_uses_canonical_validated_content(
    tmp_path: Path,
):
    original_path = BACKEND_DIR / "options/policies/valuation_v1.json"
    payload = json.loads(original_path.read_text(encoding="utf-8"))
    reformatted_path = tmp_path / "valuation.json"
    reformatted_path.write_text(
        json.dumps(payload, indent=4, sort_keys=True), encoding="utf-8"
    )

    original = load_valuation_policy(original_path)
    reformatted = load_valuation_policy(reformatted_path)

    assert original == reformatted
    assert original.policy_sha256 == reformatted.policy_sha256


def test_fixed_universe_rejects_overlap_and_wrong_cohort_size():
    with pytest.raises(ValidationError, match="overlap"):
        load_option_runtime_configuration(
            _environment(OPTION_FIXED_ETF_UNDERLYERS="SPY,QQQ,AAPL"), BACKEND_DIR
        )

    with pytest.raises(ValidationError, match="stock_universe_size"):
        load_option_runtime_configuration(
            _environment(OPTION_FIXED_STOCK_UNDERLYERS="AAPL,AMD"), BACKEND_DIR
        )