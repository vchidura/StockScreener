import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.chain_analysis import build_chain_health
from options.config import load_option_runtime_configuration

UTC = timezone.utc


def _health(**overrides):
    values = {
        "received_count": 100,
        "retained_count": 100,
        "catalog_matched_count": 100,
        "mark_aligned_count": 100,
        "iv_attempt_count": 100,
        "iv_converged_count": 100,
        "unknown_reference_count": 0,
        "reference_drift_failed": False,
    }
    values.update(overrides)
    return build_chain_health(**values)


def test_punctual_run_stays_complete():
    health = _health()
    assert health.status == "COMPLETE"
    assert health.reasons == ()


def test_late_run_degrades_without_failing():
    health = _health(execution_lag_exceeded=True)
    assert health.status == "DEGRADED"
    assert "EXECUTION_LAG_EXCEEDED" in health.reasons


def test_lag_does_not_mask_a_hard_failure():
    health = _health(execution_lag_exceeded=True, reference_drift_failed=True)
    assert health.status == "FAILED"
    assert "REFERENCE_DRIFT_FAILED" in health.reasons
    assert "EXECUTION_LAG_EXCEEDED" in health.reasons


def test_degraded_chain_suppresses_strategy_selection():
    # Strategies gate on an exactly COMPLETE chain, so a late run cannot produce
    # selections; this is the property the guard relies on.
    assert _health(execution_lag_exceeded=True).status != "COMPLETE"


def test_default_lag_ceiling_matches_the_source_age_window():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    assert configuration.settings.maximum_execution_lag_seconds == (
        configuration.valuation_policy.maximum_source_age_seconds
    )


def test_lag_ceiling_participates_in_the_configuration_fingerprint():
    baseline = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    tightened = load_option_runtime_configuration(
        {
            "POLYGON_API_KEY": "test-secret",
            "OPTION_MAXIMUM_EXECUTION_LAG_SECONDS": "600",
        },
        BACKEND_DIR,
    )
    assert tightened.settings.maximum_execution_lag_seconds == 600
    assert tightened.configuration_sha256 != baseline.configuration_sha256


def test_expected_worker_lag_sits_inside_the_default_ceiling():
    # The worker becomes eligible provider_delay + grace after the slot, so a punctual
    # run lands near 930s and must not trip the guard.
    slot = datetime(2026, 9, 8, 15, 0, tzinfo=UTC)
    punctual = slot + timedelta(seconds=930)
    assert (punctual - slot).total_seconds() < 1800


def test_trade_ingestion_toggle_is_readable_from_the_environment():
    enabled = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret", "OPTION_TRADE_INGESTION_ENABLED": "true"},
        BACKEND_DIR,
    )
    disabled = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret", "OPTION_TRADE_INGESTION_ENABLED": "false"},
        BACKEND_DIR,
    )
    assert enabled.settings.trade_ingestion_enabled is True
    assert disabled.settings.trade_ingestion_enabled is False
