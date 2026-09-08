from __future__ import annotations

import json
from pathlib import Path

import pytest

from options.config import (
    VolatilityForecastPolicy,
    load_volatility_forecast_policy,
)
from options.domain import VarianceEstimator, VarianceSource

POLICY_PATH = (
    Path(__file__).resolve().parents[2] / "options" / "policies" / "vol_forecast_policy_v1.json"
)

# Pinned so a specification change must be paired with a deliberate version bump, the
# same discipline the strategy policy hash is held to.
EXPECTED_SHA256 = "ebcda5516f9cf8acfb8d208211905201bf26a0cc29148519548da6a35d8d91ae"


def test_policy_loads():
    artifact = load_volatility_forecast_policy(POLICY_PATH)
    assert artifact.policy.forecast_policy_version == "vol_forecast_v1"
    assert artifact.path == POLICY_PATH


def test_policy_records_the_measured_winning_configuration():
    """These values are the outcome of the forecast study, not defaults."""
    policy = load_volatility_forecast_policy(POLICY_PATH).policy
    assert policy.variance_source is VarianceSource.INTRADAY_PATH
    assert policy.intraday_interval == "30m"
    assert policy.include_overnight_variance is False
    assert policy.horizon_sessions == 21
    assert policy.daily_estimator is VarianceEstimator.GARMAN_KLASS


def test_overnight_variance_cannot_be_summed_into_the_intraday_path():
    payload = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    payload["include_overnight_variance"] = True
    with pytest.raises(ValueError):
        VolatilityForecastPolicy.model_validate(payload)


def test_har_lags_must_be_increasing():
    payload = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    payload["har_lags"] = [5, 1, 22]
    with pytest.raises(ValueError):
        VolatilityForecastPolicy.model_validate(payload)


def test_har_lags_need_more_than_one_horizon():
    payload = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    payload["har_lags"] = [1]
    with pytest.raises(ValueError):
        VolatilityForecastPolicy.model_validate(payload)


def test_missing_policy_file_raises():
    with pytest.raises(ValueError):
        load_volatility_forecast_policy(POLICY_PATH.parent / "does_not_exist.json")


def test_policy_hash_is_pinned():
    artifact = load_volatility_forecast_policy(POLICY_PATH)
    assert artifact.sha256 == EXPECTED_SHA256, (
        "the forecast specification changed; bump forecast_policy_version and update "
        "this pin deliberately"
    )


def test_policy_hash_is_independent_of_the_runtime_configuration():
    """Revising the forecast must not touch ingestion or strategy identity."""
    from options.config import load_option_runtime_configuration

    configuration = load_option_runtime_configuration()
    artifact = load_volatility_forecast_policy(POLICY_PATH)
    assert artifact.sha256 != configuration.configuration_sha256
    assert artifact.sha256 != configuration.strategy_policy_sha256
    assert artifact.sha256 != configuration.gamma_policy_sha256
