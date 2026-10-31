import json
from datetime import date, datetime, timezone

import pytest

from realtime.advanced_realtime_daily_supervisor import (
    REQUIRED_DAILY_ARTIFACTS,
    build_daily_supervisor_plan,
    daily_artifact_paths,
    daily_supervisor_action,
    reconcile_daily_supervisor,
    scheduler_argv,
    session_for_observation,
    validated_daily_artifacts,
)
from realtime.advanced_options_v3_history import (
    ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA,
    advanced_options_v3_history_source_configuration,
)
from realtime.contracts import canonical_sha256


UTC = timezone.utc


def _coordinator_identity(session_date="2026-10-30"):
    value = {
        "schema": "advanced-realtime-v3-coordinator-identity-v1",
        "session_date": session_date,
    }
    value["semantic_sha256"] = canonical_sha256(value)
    return value


def _history_preflight(plan):
    source = advanced_options_v3_history_source_configuration()
    evidence = {
        "schema": ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA,
        "session_date": plan["session_date"],
        "source_configuration": source,
        "readiness": {
            "status": "READY",
            "required_through_session": plan["history_required_through"],
            "latest_covered_session": plan["history_required_through"],
        },
    }
    evidence["evidence_sha256"] = canonical_sha256(evidence)
    report = {
        "contract": "advanced-options-v3-history-preflight-v1",
        "status": "PASS",
        "source_configuration": source,
        "binding": {"evidence_sha256": evidence["evidence_sha256"]},
        "evidence": evidence,
        "effects": {
            "provider_requests": 0,
            "production_database_writes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    return report


def test_daily_plan_is_calendar_derived_and_release_is_cross_date():
    plan = build_daily_supervisor_plan(
        date(2026, 11, 27),
        release_source_sha256="a" * 64,
        release_image_id="sha256:" + "b" * 64,
    )
    assert plan["window_count"] == 42
    assert plan["market_capture_stop_utc"] == "2026-11-27T18:00:00+00:00"
    assert plan["history_required_through"] == "2026-11-25"
    assert plan["release"]["date_coupled"] is False


def test_daily_reconciliation_is_restart_stable_and_truthful():
    plan = build_daily_supervisor_plan(
        date(2026, 10, 30),
        release_source_sha256="a" * 64,
        release_image_id="sha256:" + "b" * 64,
    )
    observed = datetime(2026, 10, 30, 12, tzinfo=UTC)
    missing = reconcile_daily_supervisor(
        plan,
        artifacts={},
        prior_state=None,
        observed_at=observed,
    )
    assert missing["state"] == "PREPARATION_REQUIRED"
    assert missing["missing_artifacts"] == list(REQUIRED_DAILY_ARTIFACTS)
    ready = reconcile_daily_supervisor(
        plan,
        artifacts={name: True for name in REQUIRED_DAILY_ARTIFACTS},
        prior_state=missing,
        observed_at=observed,
    )
    assert ready["state"] == "READY_TO_LAUNCH"
    assert set(ready["effects"].values()) == {0}
    changed = dict(missing, plan_sha256="f" * 64)
    with pytest.raises(RuntimeError, match="state diverged"):
        reconcile_daily_supervisor(
            plan,
            artifacts={},
            prior_state=changed,
            observed_at=observed,
        )


def test_persistent_supervisor_rolls_after_decision_drain():
    assert session_for_observation(
        datetime(2026, 10, 30, 12, tzinfo=UTC)
    ) == date(2026, 10, 30)
    assert session_for_observation(
        datetime(2026, 10, 30, 21, tzinfo=UTC)
    ) == date(2026, 11, 2)
    assert session_for_observation(
        datetime(2026, 10, 31, 12, tzinfo=UTC)
    ) == date(2026, 11, 2)


def test_supervisor_action_is_deadline_and_restart_safe(tmp_path):
    plan = build_daily_supervisor_plan(
        date(2026, 10, 30),
        release_source_sha256="a" * 64,
        release_image_id="sha256:" + "b" * 64,
    )
    coordinator = tmp_path / "coordinator"
    missing = daily_supervisor_action(
        plan,
        artifacts={},
        coordinator_directory=coordinator,
        observed_at=datetime(2026, 10, 30, 12, tzinfo=UTC),
    )
    assert missing["action"] == "WAIT_FOR_PREPARATION"
    late = daily_supervisor_action(
        plan,
        artifacts={},
        coordinator_directory=coordinator,
        observed_at=datetime(2026, 10, 30, 13, 15, tzinfo=UTC),
    )
    assert late["action"] == "MISSED_PREPARATION_DEADLINE"
    ready = daily_supervisor_action(
        plan,
        artifacts={name: True for name in REQUIRED_DAILY_ARTIFACTS},
        coordinator_directory=coordinator,
        observed_at=datetime(2026, 10, 30, 12, tzinfo=UTC),
    )
    assert ready["action"] == "LAUNCH_SCHEDULER"

    coordinator.mkdir()
    (coordinator / "COORDINATOR.json").write_text(
        json.dumps(_coordinator_identity()),
        encoding="utf-8",
    )
    resumed = daily_supervisor_action(
        plan,
        artifacts={name: True for name in REQUIRED_DAILY_ARTIFACTS},
        coordinator_directory=coordinator,
        observed_at=datetime(2026, 10, 30, 14, tzinfo=UTC),
    )
    assert resumed["action"] == "RESUME_SCHEDULER"
    (coordinator / "CLEANUP.json").write_text("{}", encoding="utf-8")
    completed = daily_supervisor_action(
        plan,
        artifacts={},
        coordinator_directory=coordinator,
        observed_at=datetime(2026, 10, 30, 21, tzinfo=UTC),
    )
    assert completed["action"] == "AWAIT_NEXT_SESSION"


def test_daily_scheduler_command_uses_fixed_artifacts_and_resume(tmp_path):
    session_root = tmp_path / "sessions"
    paths = daily_artifact_paths(session_root, date(2026, 10, 30))
    coordinator = session_root / "2026-10-30" / "coordinator"
    command = scheduler_argv(
        python_executable=tmp_path / "python.exe",
        scheduler_script=tmp_path / "scheduler.py",
        database="stocks_realtime_v2",
        artifact_paths=paths,
        docker_executable=tmp_path / "docker.exe",
        coordinator_directory=coordinator,
    )
    assert "--history-preflight" not in command
    assert command[command.index("--acquisition-binding") + 1].endswith(
        "acquisition-binding.json"
    )
    assert "--resume" not in command
    assert command[command.index("--monitor-seconds") + 1] == "15"

    coordinator.mkdir(parents=True)
    (coordinator / "COORDINATOR.json").write_text(
        json.dumps(_coordinator_identity()),
        encoding="utf-8",
    )
    resumed = scheduler_argv(
        python_executable=tmp_path / "python.exe",
        scheduler_script=tmp_path / "scheduler.py",
        database="stocks_realtime_v2",
        artifact_paths=paths,
        docker_executable=tmp_path / "docker.exe",
        coordinator_directory=coordinator,
    )
    assert resumed[-1] == "--resume"


def test_daily_history_artifact_requires_exact_prior_session(tmp_path):
    plan = build_daily_supervisor_plan(
        date(2026, 10, 30),
        release_source_sha256="a" * 64,
        release_image_id="sha256:" + "b" * 64,
    )
    paths = daily_artifact_paths(tmp_path, date(2026, 10, 30))
    paths["history_preflight"].parent.mkdir()
    report = _history_preflight(plan)
    paths["history_preflight"].write_text(
        json.dumps(report),
        encoding="utf-8",
    )
    assert validated_daily_artifacts(
        plan,
        paths,
    )["history_preflight"] is True

    report["evidence"]["readiness"]["latest_covered_session"] = (
        "2026-10-28"
    )
    report["evidence"].pop("evidence_sha256")
    report["evidence"]["evidence_sha256"] = canonical_sha256(
        report["evidence"]
    )
    report["binding"]["evidence_sha256"] = report["evidence"][
        "evidence_sha256"
    ]
    report.pop("semantic_sha256")
    report["semantic_sha256"] = canonical_sha256(report)
    paths["history_preflight"].write_text(
        json.dumps(report),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="history preflight"):
        validated_daily_artifacts(plan, paths)
