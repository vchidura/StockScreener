import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_developer_policy, load_option_runtime_configuration


def _environment(**overrides: str) -> dict[str, str]:
    environment = {"POLYGON_API_KEY": "test-secret"}
    environment.update(overrides)
    return environment


def test_runtime_configuration_is_frozen_read_only_and_secret_safe():
    runtime = load_option_runtime_configuration(_environment(), BACKEND_DIR)

    assert runtime.settings.start_read_only is True
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
    assert "test-secret" not in repr(runtime)
    assert "polygon_api_key" not in runtime.metadata()
    assert len(runtime.policy_sha256) == 64
    assert len(runtime.configuration_sha256) == 64

    with pytest.raises(ValidationError):
        runtime.settings.start_read_only = False
    with pytest.raises(ValidationError):
        runtime.policy.contract_filter.maximum_dte = 30


def test_policy_fingerprint_uses_canonical_validated_content(tmp_path: Path):
    original_path = BACKEND_DIR / "options/policies/developer_v1.json"
    payload = json.loads(original_path.read_text(encoding="utf-8"))
    reformatted_path = tmp_path / "policy.json"
    reformatted_path.write_text(json.dumps(payload, indent=4, sort_keys=True), encoding="utf-8")

    original = load_developer_policy(original_path)
    reformatted = load_developer_policy(reformatted_path)

    assert original.sha256 == reformatted.sha256


def test_fixed_universe_rejects_overlap_and_wrong_cohort_size():
    with pytest.raises(ValidationError, match="overlap"):
        load_option_runtime_configuration(
            _environment(OPTION_FIXED_ETF_UNDERLYERS="SPY,QQQ,AAPL"), BACKEND_DIR
        )

    with pytest.raises(ValidationError, match="stock_universe_size"):
        load_option_runtime_configuration(
            _environment(OPTION_FIXED_STOCK_UNDERLYERS="AAPL,AMD"), BACKEND_DIR
        )