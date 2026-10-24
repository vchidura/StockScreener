from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from realtime.advanced_options_control_plane import (
    advanced_options_enrollment_plan_payload,
    build_advanced_options_equity_input_binding,
    build_advanced_options_history_binding,
)
from realtime.contracts import DEVELOPER_FORWARD_UNIVERSE
from scripts import run_realtime_v2_options_enrollment as script
from tests.realtime.test_advanced_options_control_plane import (
    _enrollment_plan,
)


def _args(report, underlyings, *, execute=False):
    now = datetime.now(timezone.utc)
    values = [
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--session-date", now.date().isoformat(),
        "--deadline", (now + timedelta(minutes=4)).isoformat(),
        "--authorized-underlyings", *underlyings,
        "--authorization-reference", "test-authorization",
        "--report-path", str(report),
    ]
    if execute:
        values.append("--execute")
    return values


def test_authorized_universe_mismatch_blocks_before_environment_database_or_provider(
    tmp_path, monkeypatch
):
    report = tmp_path / "blocked.json"
    monkeypatch.setattr(
        script,
        "_configuration_environment",
        lambda: (_ for _ in ()).throw(AssertionError("environment must not load")),
    )
    monkeypatch.setattr(
        script,
        "_pool",
        lambda: (_ for _ in ()).throw(AssertionError("database must not open")),
    )
    underlyings = tuple(
        "META" if symbol == "TSLA" else symbol
        for symbol in DEVELOPER_FORWARD_UNIVERSE.option_underlyings
    )
    assert script.main(_args(report, underlyings, execute=True)) == 2
    stored = json.loads(report.read_text(encoding="utf-8"))
    assert stored["status"] == "BLOCKED"
    assert stored["reason_codes"] == ["AUTHORIZED_UNIVERSE_IDENTITY_MISMATCH"]
    assert stored["provider_requests"] == 0
    assert stored["response_bytes"] == 0
    assert stored["database_writes"] is False


def test_plan_retains_the_exact_canonical_scope_without_opening_runtime(tmp_path, monkeypatch):
    report = tmp_path / "plan.json"
    monkeypatch.setattr(
        script,
        "_pool",
        lambda: (_ for _ in ()).throw(AssertionError("plan must not open the database")),
    )
    assert script.main(
        _args(report, DEVELOPER_FORWARD_UNIVERSE.option_underlyings)
    ) == 0
    stored = json.loads(report.read_text(encoding="utf-8"))
    assert stored["status"] == "PLAN"
    assert tuple(stored["canonical_underlyings"]) == (
        DEVELOPER_FORWARD_UNIVERSE.option_underlyings
    )


def test_prospective_control_plane_plan_has_no_database_provider_or_execute_path(
    tmp_path,
    monkeypatch,
):
    prepared = tmp_path / "prepared.json"
    report = tmp_path / "report.json"
    prepared.write_text(
        json.dumps(
            advanced_options_enrollment_plan_payload(_enrollment_plan()),
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        script,
        "load_dotenv",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("PLAN loaded the environment")
        ),
    )
    monkeypatch.setattr(
        script,
        "_pool",
        lambda: (_ for _ in ()).throw(
            AssertionError("PLAN opened the database")
        ),
    )

    assert script.main([
        "--prospective-control-plane-plan",
        str(prepared),
        "--report-path",
        str(report),
    ]) == 0

    stored = json.loads(report.read_text(encoding="utf-8"))
    assert stored["status"] == "PLAN"
    assert stored["action"] == "PROSPECTIVE_MARKET_OPTIONS_ENROLLMENT"
    assert stored["provider_requests"] == stored["provider_sockets"] == 0
    assert stored["database_writes"] is False
    assert stored["production_authority"] is False
    assert stored["execution_available"] is False
    assert set(stored["effect_counts"].values()) == {0}


def test_frozen_advanced_plan_executes_without_developer_enrollment(
    monkeypatch,
):
    plan = _enrollment_plan()
    history = build_advanced_options_history_binding(
        target_cohort_id=plan.control_plane.cohort_id,
        source_cohort_id=plan.equity_plan.cohort_id,
        history_before=plan.control_plane.planned_at,
        source_manifest_sha256=plan.equity_plan.manifest.sha256,
        authorization_reference="p8-history",
    )
    equity_input = build_advanced_options_equity_input_binding(
        target_options_window_id=uuid4(),
        target_cohort_id=plan.control_plane.cohort_id,
        source_cohort_id=plan.equity_plan.cohort_id,
        source_cohort_sha256=plan.equity_plan.cohort_sha256,
        source_manifest_id=plan.equity_plan.manifest_id,
        source_manifest_sha256=plan.equity_plan.manifest.sha256,
        source_profile_id=plan.equity_plan.profile_id,
        source_profile_sha256=plan.equity_plan.profile_sha256,
        equity_window_id=uuid4(),
        equity_receipt_id=uuid4(),
        equity_cutoff=plan.control_plane.receipts.options_input_ready.spec.cutoff,
        reference_generation=1,
        universe_sha256=plan.equity_plan.universe.sha256,
        input_sha256="d" * 64,
        build_id="advanced-equity-test-v1",
        maximum_age_seconds=300,
        authorization_reference="p8-equity-input",
    )
    persisted = []
    monkeypatch.setattr(
        script,
        "persist_advanced_options_enrollment",
        lambda connection, frozen, *, history_binding,
        equity_input_binding: persisted.append(
            (
                connection,
                frozen,
                history_binding,
                equity_input_binding,
            )
        ) or True,
    )
    monkeypatch.setattr(
        script,
        "build_developer_forward_process_plan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Advanced execution used Developer enrollment")
        ),
    )

    connection = object()
    assert script.execute_frozen_advanced_options_enrollment(
        connection,
        plan,
        history,
        equity_input,
        database_name="stocks_realtime_v2",
        confirmed_database_name="stocks_realtime_v2",
    )
    assert persisted == [(connection, plan, history, equity_input)]
