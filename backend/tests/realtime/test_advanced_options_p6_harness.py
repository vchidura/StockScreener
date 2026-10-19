from pathlib import Path

import pytest

from scripts.prepare_advanced_options_p6 import (
    DATABASE,
    ROLE_PREFIX,
    parse_args,
)
from scripts.evaluate_advanced_options_p6 import evaluate
from scripts.run_advanced_options_p6_faults import PROJECT
from tests.realtime.advanced_options_p6_harness import (
    BURSTS,
    FAULT_DUPLICATES,
    FAULT_GENERATION_MISMATCH,
    FAULT_OUT_OF_ORDER,
    PHASES,
)


def test_p6_scope_is_exact_and_provider_free():
    assert DATABASE == "test_realtime_v2_options_p6"
    assert ROLE_PREFIX == "test_rt_options_p6"
    assert PROJECT == "stockscreener-options-p6-20261018"
    assert PHASES == (
        ("calibration-8-160", 300, 250),
        ("calibration-25-500", 300, 500),
        ("qualification-50-1000", 1800, 1000),
    )
    assert BURSTS == ((300, 30, 5000), (900, 30, 5000), (1500, 30, 5000))
    assert (FAULT_DUPLICATES, FAULT_OUT_OF_ORDER, FAULT_GENERATION_MISMATCH) == (
        100,
        100,
        1,
    )


def test_prepare_requires_exact_disposable_names_and_execute(tmp_path):
    base = [
        "prepare",
        "--database",
        DATABASE,
        "--confirm-database-name",
        DATABASE,
        "--role-prefix",
        ROLE_PREFIX,
        "--secret-directory",
        str(tmp_path / "secrets"),
    ]
    with pytest.raises(SystemExit):
        parse_args(base)
    args = parse_args([*base, "--execute"])
    assert args.database == DATABASE
    with pytest.raises(SystemExit):
        parse_args([
            *base,
            "--database",
            "stocks_realtime_v2",
            "--execute",
        ])


def test_p6_compose_has_no_provider_or_production_target():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.options-p6.yml"
    ).read_text(encoding="utf-8")
    assert "provider_key" not in compose
    assert "POLYGON_API_KEY" not in compose
    assert "stocks_realtime_v2" not in compose
    assert "\n    ports:" not in compose
    assert "pull_policy: never" in compose
    assert "host.docker.internal" in compose
    assert "stockscreener-options-p6-20261018" in compose


def test_p6_evaluator_fails_closed_when_measurements_are_incomplete(
    monkeypatch,
):
    monkeypatch.setattr(
        "scripts.evaluate_advanced_options_p6._database_bytes",
        lambda: 1,
    )
    workload = {
        "duration_scale": 0.01,
        "logical_input_duration_seconds": 24,
        "accepted_per_second": 100,
        "drain_seconds": 0,
        "events": {
            "duplicates": 100,
            "out_of_order": 100,
            "generation_mismatch": 1,
        },
        "lag_ms": {"p95": 1, "p99": 2},
        "queue": {"maximum_depth": 1},
        "reader": {"failures": 0},
        "cycles": 0,
        "o2_reason": "O2_MODEL_NOT_ACCEPTED",
        "storage": {
            "journal_bytes": 1,
            "report_bytes_before_final_report": 1,
            "journal_free_bytes": 20 * 1_073_741_824,
            "report_free_bytes": 20 * 1_073_741_824,
            "journal_free_percent": 50,
            "report_free_percent": 50,
        },
        "orchestrator": {
            "forbidden_effect_counts": {
                "rt.portal_releases": 0,
            },
        },
        "provider_calls": 0,
        "publication_authorized": False,
        "archive_execution_authorized": False,
        "retention_execution_authorized": False,
        "execution_authorized": False,
        "delivery_authorized": False,
    }
    resource = {"faults": [], "resource_samples": []}
    result = evaluate(workload, resource)
    assert result["status"] == "FAIL"
    assert "full_duration_scale" in result["failures"]
    assert "all_faults_injected" in result["failures"]
