import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from realtime.retention_lifecycle_executor import (
    STATUS_AMBIGUOUS,
    STATUS_CONSUMED,
    STATUS_LOCKED,
    STATUS_READY,
    FakeLifecycleCommandPort,
    ObservationSnapshotProvider,
    StopOnlyExecutionError,
    StopOnlyExecutor,
    load_exact_lifecycle_package,
)
from scripts import run_realtime_v2_retention_stop as stop_script
from scripts.run_realtime_v2_retention_stop import main as stop_main


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[3]
PACKAGE_PATH = (
    ROOT
    / "backend/.realtime-canary/"
    "options-owp5-retention-stop-authorization-20261022-02.json"
)
POST_STOP_PATH = (
    ROOT
    / "backend/.realtime-canary/"
    "options-owp5-retention-post-stop-contract-20261022-02.json"
)
READINESS_REPORT = (
    ROOT
    / "backend/.realtime-canary/"
    "options-owp5-execution-readiness-20261021.json"
)
ACTIVE = datetime(2026, 10, 22, 15, 35, tzinfo=UTC)


def _package():
    return load_exact_lifecycle_package(PACKAGE_PATH, POST_STOP_PATH)


def _fake_state(tmp_path, package=None):
    package = package or _package()
    path = tmp_path / "fake-service.json"
    path.write_text(
        json.dumps({
            "observation": copy.deepcopy(
                package["lifecycle_scope"]["observed_state"]
            ),
            "commands": [],
        }, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _executor(tmp_path):
    package = _package()
    state_path = _fake_state(tmp_path, package)
    lock_path = tmp_path / "stop.lock"
    receipt_path = tmp_path / "stop-receipt.json"
    port = FakeLifecycleCommandPort(state_path)
    executor = StopOnlyExecutor(
        port,
        ObservationSnapshotProvider(),
        repository_root=ROOT,
        readiness_report_path=READINESS_REPORT,
        lock_path=lock_path,
        receipt_path=receipt_path,
    )
    return package, port, executor, state_path, lock_path, receipt_path


def test_plan_preflight_and_status_are_zero_effect(tmp_path):
    package, port, executor, state_path, lock_path, receipt_path = (
        _executor(tmp_path)
    )
    before = hashlib.sha256(state_path.read_bytes()).hexdigest()

    plan = executor.plan(package)
    status = executor.status(package)
    preflight = executor.preflight(
        package,
        checked_at=datetime(2026, 10, 21, 8, 0, tzinfo=UTC),
    )

    assert plan["mode"] == "PLAN"
    assert status.state == STATUS_READY
    assert preflight["status"] == "PASS_STOP_ONLY_LIFECYCLE_PREFLIGHT"
    assert hashlib.sha256(state_path.read_bytes()).hexdigest() == before
    assert port.commands() == ()
    assert not lock_path.exists()
    assert not receipt_path.exists()


def test_exact_fake_stop_runs_two_commands_post_snapshot_and_refuses_reuse(
    tmp_path,
):
    package, port, executor, _state_path, lock_path, receipt_path = (
        _executor(tmp_path)
    )

    result = executor.execute(package, checked_at=ACTIVE)

    assert result["status"] == STATUS_CONSUMED
    assert result["post_stop"]["retention_stopped"]
    assert not result["post_stop"]["retention_resume_authorized"]
    assert port.commands() == (
        {
            "action": "SET_RESTART_POLICY_NO",
            "container_id": package[
                "lifecycle_scope"
            ]["container_identity"]["id"],
        },
        {
            "action": "STOP_CONTAINER",
            "container_id": package[
                "lifecycle_scope"
            ]["container_identity"]["id"],
            "timeout_seconds": 30,
        },
    )
    assert not lock_path.exists()
    assert receipt_path.exists()
    assert executor.status(package).state == STATUS_CONSUMED
    assert result["production_writes"] == 0
    assert result["provider_requests"] == 0
    assert result["start_restart_remove_kill_actions"] == 0
    with pytest.raises(StopOnlyExecutionError, match="STOP_NOT_READY"):
        executor.execute(package, checked_at=ACTIVE)


@pytest.mark.parametrize(
    "stage, expected_commands, expected_status",
    (
        ("AFTER_LOCK", 0, STATUS_LOCKED),
        ("AFTER_RECEIPT", 0, STATUS_AMBIGUOUS),
        ("AFTER_RESTART_POLICY", 1, STATUS_AMBIGUOUS),
        ("AFTER_STOP", 2, STATUS_AMBIGUOUS),
        ("BEFORE_POST_STOP_SNAPSHOT", 2, STATUS_AMBIGUOUS),
        ("AFTER_POST_STOP_SNAPSHOT", 2, STATUS_AMBIGUOUS),
    ),
)
def test_failure_boundaries_never_roll_forward_or_auto_restart(
    tmp_path,
    stage,
    expected_commands,
    expected_status,
):
    package, port, executor, _state_path, lock_path, receipt_path = (
        _executor(tmp_path)
    )

    def fault(current):
        if current == stage:
            raise RuntimeError(f"crash at {stage}")

    with pytest.raises(StopOnlyExecutionError):
        executor.execute(package, checked_at=ACTIVE, fault=fault)

    assert len(port.commands()) == expected_commands
    assert lock_path.exists()
    assert executor.status(package).state == expected_status
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert receipt["state"] == "AMBIGUOUS_STOP_REQUIRES_OPERATOR"
    with pytest.raises(StopOnlyExecutionError):
        executor.execute(package, checked_at=ACTIVE)


@pytest.mark.parametrize(
    "failure_action, expected_commands",
    (
        ("SET_RESTART_POLICY_NO", 1),
        ("STOP_CONTAINER", 2),
    ),
)
def test_command_port_failure_after_mutation_is_ambiguous_and_not_retried(
    tmp_path,
    failure_action,
    expected_commands,
):
    package, fake_port, _executor_value, _state, lock, receipt = (
        _executor(tmp_path)
    )

    class MutationThenFailPort:
        def observe(self):
            return fake_port.observe()

        def set_restart_policy_no(self, container_id):
            fake_port.set_restart_policy_no(container_id)
            if failure_action == "SET_RESTART_POLICY_NO":
                raise StopOnlyExecutionError(
                    "INJECTED_UPDATE_POST_INSPECT_FAILURE",
                    "normalized fake transcript failure",
                )

        def stop(self, container_id, timeout_seconds):
            fake_port.stop(container_id, timeout_seconds)
            if failure_action == "STOP_CONTAINER":
                raise StopOnlyExecutionError(
                    "INJECTED_STOP_POST_INSPECT_FAILURE",
                    "normalized fake transcript failure",
                )

    executor = StopOnlyExecutor(
        MutationThenFailPort(),
        ObservationSnapshotProvider(),
        repository_root=ROOT,
        readiness_report_path=READINESS_REPORT,
        lock_path=lock,
        receipt_path=receipt,
    )

    with pytest.raises(StopOnlyExecutionError, match="POST_INSPECT_FAILURE"):
        executor.execute(package, checked_at=ACTIVE)

    assert len(fake_port.commands()) == expected_commands
    assert lock.exists()
    assert json.loads(receipt.read_text(encoding="utf-8"))["state"] == (
        "AMBIGUOUS_STOP_REQUIRES_OPERATOR"
    )
    assert executor.status(package).state == STATUS_AMBIGUOUS
    with pytest.raises(StopOnlyExecutionError, match="STOP_NOT_READY"):
        executor.execute(package, checked_at=ACTIVE)
    assert len(fake_port.commands()) == expected_commands


def test_early_late_lock_and_identity_drift_are_refused(tmp_path):
    package, _port, executor, state_path, lock_path, _receipt_path = (
        _executor(tmp_path)
    )
    with pytest.raises(StopOnlyExecutionError, match="EARLY_STOP_WINDOW"):
        executor.execute(
            package,
            checked_at=datetime(2026, 10, 22, 15, 29, tzinfo=UTC),
        )
    with pytest.raises(StopOnlyExecutionError, match="MISSED_STOP_WINDOW"):
        executor.execute(
            package,
            checked_at=datetime(2026, 10, 22, 15, 40, tzinfo=UTC),
        )
    lock_path.write_text("occupied\n", encoding="ascii")
    assert executor.status(package).state == STATUS_LOCKED
    with pytest.raises(StopOnlyExecutionError, match="STOP_NOT_READY"):
        executor.execute(package, checked_at=ACTIVE)
    lock_path.unlink()

    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["observation"]["matching_containers"][0]["image_id"] = (
        "sha256:" + "0" * 64
    )
    state_path.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(StopOnlyExecutionError, match="STOP_NOT_READY"):
        executor.preflight(package, checked_at=ACTIVE)


def test_fake_port_exposes_no_forbidden_lifecycle_methods(tmp_path):
    port = FakeLifecycleCommandPort(_fake_state(tmp_path))
    for name in ("start", "restart", "remove", "kill"):
        assert not hasattr(port, name)


def test_cli_execute_requires_owned_fake_and_never_calls_docker(
    tmp_path,
    capsys,
):
    package = _package()
    state_path = _fake_state(tmp_path, package)
    arguments = [
        "--package", str(PACKAGE_PATH),
        "--post-stop-contract", str(POST_STOP_PATH),
        "--readiness-report", str(READINESS_REPORT),
        "--fake-service-state", str(state_path),
        "--lock-path", str(tmp_path / "cli.lock"),
        "--receipt-path", str(tmp_path / "cli-receipt.json"),
        "--checked-at", ACTIVE.isoformat(),
    ]
    with pytest.raises(ValueError, match="owned-fake-service"):
        stop_main([*arguments, "--execute"])

    assert stop_main([
        *arguments,
        "--owned-fake-service",
        "--execute",
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == STATUS_CONSUMED
    assert result["start_restart_remove_kill_actions"] == 0
    source = (
        ROOT
        / "backend/scripts/run_realtime_v2_retention_stop.py"
    ).read_text(encoding="utf-8")
    assert "subprocess" not in source
    assert "docker.exe" not in source


def test_cli_real_boundary_stays_inert_until_explicit_execute(
    tmp_path,
    capsys,
    monkeypatch,
):
    package = _package()
    state_path = _fake_state(tmp_path, package)
    fake_port = FakeLifecycleCommandPort(state_path)
    created = []

    class InjectedDockerPort:
        def __init__(self, **arguments):
            created.append(arguments)

        def observe(self):
            return fake_port.observe()

        def set_restart_policy_no(self, container_id):
            fake_port.set_restart_policy_no(container_id)

        def stop(self, container_id, timeout_seconds):
            fake_port.stop(container_id, timeout_seconds)

        def evidence(self):
            return ({
                "operation": "INJECTED_FAKE_SUBPROCESS_TRANSCRIPT",
                "raw_environment": False,
                "credentials": False,
            },)

    monkeypatch.setattr(
        stop_script,
        "DockerLifecycleCommandPort",
        InjectedDockerPort,
    )
    docker = (tmp_path / "docker.exe").resolve()
    docker.write_bytes(b"owned fake executable")
    arguments = [
        "--package", str(PACKAGE_PATH),
        "--post-stop-contract", str(POST_STOP_PATH),
        "--readiness-report", str(READINESS_REPORT),
        "--lock-path", str(tmp_path / "real-cli.lock"),
        "--receipt-path", str(tmp_path / "real-cli-receipt.json"),
        "--checked-at", ACTIVE.isoformat(),
        "--real-docker",
        "--docker-executable", str(docker),
        "--expected-docker-version", "29.8.1",
    ]

    assert stop_main([*arguments, "--plan"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["mode"] == "PLAN"
    assert fake_port.commands() == ()
    assert not (tmp_path / "real-cli.lock").exists()
    assert not (tmp_path / "real-cli-receipt.json").exists()

    assert stop_main([*arguments, "--execute"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == STATUS_CONSUMED
    assert result["docker_command_evidence"] == [{
        "operation": "INJECTED_FAKE_SUBPROCESS_TRANSCRIPT",
        "raw_environment": False,
        "credentials": False,
    }]
    assert len(created) == 2
    assert all(
        row["expected_container"]
        == package["lifecycle_scope"]["container_identity"]
        for row in created
    )
    assert (tmp_path / "real-cli-receipt.json").exists()
    assert not (tmp_path / "real-cli.lock").exists()
