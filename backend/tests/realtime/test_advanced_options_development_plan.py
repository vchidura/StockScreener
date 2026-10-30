from pathlib import Path

import pytest

from realtime.advanced_options_development_fixture import (
    build_development_fixture_e,
)
from realtime.advanced_options_development_plan import (
    build_development_diagnostic_plan,
)


ENVIRONMENT = {
    "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
    "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
}


def _fixture():
    from datetime import date, datetime, timezone

    return build_development_fixture_e(
        session_date=date(2026, 10, 26),
        equity_cutoff=datetime(
            2026, 10, 26, 13, 45, tzinfo=timezone.utc
        ),
        authorization_reference="fixture-e-test",
        qualification_semantic_sha256="a" * 64,
        qualification_file_sha256="b" * 64,
    )


def _live_report():
    return {
        "contract": "advanced-options-v3-live-socket-canary-v1",
        "status": "PASS",
        "authority_consumed_for_diagnostic": True,
        "production_database_writes": 0,
        "publication": 0,
        "serving": 0,
        "contracts": [
            "O:QQQ261026C00705000",
            "O:QQQ261026P00705000",
            "O:SPY261026C00762000",
            "O:SPY261026P00762000",
        ],
        "semantic_sha256": "c" * 64,
    }


def test_development_plan_is_inert_projection_only(tmp_path):
    plan = build_development_diagnostic_plan(
        fixture_e=_fixture(),
        fixture_e_file_sha256="d" * 64,
        live_options_report=_live_report(),
        live_options_report_file_sha256="e" * 64,
        bound_journal_path=(
            tmp_path / "bound.sqlite"
        ).resolve(),
        bound_journal_sha256="f" * 64,
        authorization_reference="development-plan-test",
        environment=ENVIRONMENT,
    )

    assert plan["equity_stage_order"][-1] == "EQUITY_PROJECTION"
    assert plan["options_stage_order"][-1] == "OPTIONS_PROJECTION"
    assert not plan["release_allowed"]
    assert not plan["pointer_allowed"]
    assert not plan["read_pin_allowed"]
    assert not plan["normal_portal_serving_allowed"]
    assert not plan["database_execution_authorized"]
    assert not plan["service_execution_authorized"]


def test_development_plan_rejects_failed_live_options(tmp_path):
    live = _live_report()
    live["status"] = "FAIL"

    with pytest.raises(
        ValueError,
        match="source identity changed",
    ):
        build_development_diagnostic_plan(
            fixture_e=_fixture(),
            fixture_e_file_sha256="d" * 64,
            live_options_report=live,
            live_options_report_file_sha256="e" * 64,
            bound_journal_path=(tmp_path / "bound.sqlite").resolve(),
            bound_journal_sha256="f" * 64,
            authorization_reference="development-plan-test",
            environment=ENVIRONMENT,
        )
