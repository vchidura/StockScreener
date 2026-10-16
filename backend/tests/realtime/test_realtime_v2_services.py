import importlib

import pytest

script = importlib.import_module("scripts.run_realtime_v2_services")
BASE = [
    "--session-date", "2026-10-15",
    "--first-cutoff", "2026-10-15T18:30:00+00:00",
    "--window-count", "2",
    "--authorization-reference", "approved",
    "--run-directory", "backend\\.realtime-canary\\test-v2-services",
]


def test_service_launcher_builds_separate_bounded_processes(tmp_path):
    args = script.parse_args([
        *BASE[:-2],
        "--run-directory", str(tmp_path / "services"),
    ])
    commands = script.build_commands(args)

    assert set(commands) == {"api", "frontend", "finalizer", "scheduler"}
    assert args.maximum_provider_requests == 36
    assert "run_realtime_v2_developer_service.py" in " ".join(commands["scheduler"])
    assert "--continuous" in commands["finalizer"]
    assert "uvicorn" in commands["api"]
    assert "npm.cmd" in commands["frontend"]


def test_service_launcher_requires_authorized_execution(tmp_path):
    base = [
        "--session-date", "2026-10-15",
        "--first-cutoff", "2026-10-15T18:30:00+00:00",
        "--window-count", "1",
        "--run-directory", str(tmp_path / "services"),
    ]
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--execute"])


def test_service_launcher_builds_continuous_ingestion_command(tmp_path):
    args = script.parse_args([
        "--session-date", "2026-10-15",
        "--first-cutoff", "2026-10-15T18:30:00+00:00",
        "--continuous-ingestion",
        "--daily-request-ceiling", "126",
        "--authorization-reference", "approved",
        "--run-directory", str(tmp_path / "continuous"),
    ])
    scheduler = script.build_commands(args)["scheduler"]

    assert "run_realtime_v2_continuous_ingestion.py" in " ".join(scheduler)
    assert "--daily-request-ceiling" in scheduler
    assert args.maximum_provider_requests == 126


def test_service_launcher_builds_provider_free_stored_only_processes(tmp_path):
    args = script.parse_args([
        "--session-date", "2026-10-15",
        "--stored-only",
        "--run-directory", str(tmp_path / "stored-only"),
        "--execute",
    ])
    commands = script.build_commands(args)

    assert set(commands) == {"api", "frontend", "finalizer"}
    assert args.maximum_provider_requests == 0
    assert args.authorization_reference == ""


@pytest.mark.parametrize("argument,value", [
    ("--first-cutoff", "2026-10-15T18:30:00+00:00"),
    ("--window-count", "1"),
    ("--daily-request-ceiling", "18"),
])
def test_stored_only_rejects_scheduler_scope(tmp_path, argument, value):
    with pytest.raises(SystemExit):
        script.parse_args([
            "--session-date", "2026-10-15",
            "--stored-only",
            argument, value,
            "--run-directory", str(tmp_path / "stored-only"),
        ])
