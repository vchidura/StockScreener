import copy
import hashlib
import json
from pathlib import Path

import pytest

from realtime.retention_lifecycle_docker import (
    DOCKER_HOST,
    EXACT_CONTAINER_ID,
    MAX_CAPTURE_BYTES,
    CommandResult,
    DockerLifecycleCommandPort,
)
from realtime.retention_lifecycle_executor import (
    StopOnlyExecutionError,
    load_exact_lifecycle_package,
)


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
DOCKER_VERSION = "29.8.1"
ADMIN_SOURCE = "C:\\owned-fake\\realtime_v2_admin_dsn"
APP_SOURCE = "C:\\owned-fake\\realtime_v2_app_dsn"


def _package():
    return load_exact_lifecycle_package(PACKAGE_PATH, POST_STOP_PATH)


def _port_inputs():
    package = _package()
    baseline = copy.deepcopy(
        package["lifecycle_scope"]["observed_state"]
    )
    expected = copy.deepcopy(
        package["lifecycle_scope"]["container_identity"]
    )
    sources = {
        "/run/secrets/realtime_v2_admin_dsn": ADMIN_SOURCE,
        "/run/secrets/realtime_v2_app_dsn": APP_SOURCE,
    }
    for mount in expected["mounts"]:
        source = sources.get(mount["destination"])
        if source is not None:
            mount["source_path_sha256"] = hashlib.sha256(
                source.encode("utf-8")
            ).hexdigest()
    baseline["matching_containers"] = [copy.deepcopy(expected)]
    return baseline, expected


def _inspect(expected):
    labels = {
        f"com.docker.compose.{name.replace('_', '-')}": value
        for name, value in expected["compose_identity"].items()
    }
    labels["com.docker.compose.config-hash"] = (
        expected["compose_identity"]["config_hash"]
    )
    labels["com.docker.compose.container-number"] = (
        expected["compose_identity"]["container_number"]
    )
    mounts = []
    for mount in expected["mounts"]:
        raw = {
            "Type": mount["type"],
            "Destination": mount["destination"],
            "RW": mount["read_write"],
        }
        if mount["type"] == "volume":
            raw["Name"] = mount["name"]
        elif mount["destination"].endswith("admin_dsn"):
            raw["Source"] = ADMIN_SOURCE
        else:
            raw["Source"] = APP_SOURCE
        mounts.append(raw)
    return {
        "id": expected["id"],
        "name": f"/{expected['name']}",
        "image_id": expected["image_id"],
        "command": copy.deepcopy(expected["command"]),
        "entrypoint": expected["entrypoint"],
        "started_at_utc": expected["started_at_utc"],
        "status": expected["status"],
        "running": expected["running"],
        "health_status": expected["health"],
        "restart_policy": {
            "Name": expected["restart_policy"]["name"],
            "MaximumRetryCount": expected[
                "restart_policy"
            ]["maximum_retry_count"],
        },
        "labels": labels,
        "mounts": mounts,
    }


class TranscriptRunner:
    def __init__(self, inspect, *, version=DOCKER_VERSION):
        self.inspect = copy.deepcopy(inspect)
        self.version = version
        self.calls = []
        self.next_result = None
        self.update_output = EXACT_CONTAINER_ID
        self.stop_output = EXACT_CONTAINER_ID
        self.stop_changes_state = True

    def __call__(
        self,
        argv,
        *,
        cwd,
        env,
        timeout_seconds,
        max_capture_bytes,
    ):
        exact = tuple(argv)
        self.calls.append({
            "argv": exact,
            "cwd": cwd,
            "env": dict(env),
            "timeout_seconds": timeout_seconds,
            "max_capture_bytes": max_capture_bytes,
        })
        if self.next_result is not None:
            result = self.next_result
            self.next_result = None
            return CommandResult(argv=exact, **result)
        operation = exact[1]
        if operation == "version":
            stdout = json.dumps(self.version).encode("utf-8") + b"\n"
        elif operation == "inspect":
            stdout = json.dumps(
                self.inspect,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8") + b"\n"
        elif operation == "update":
            self.inspect["restart_policy"] = {
                "Name": "no",
                "MaximumRetryCount": 0,
            }
            stdout = self.update_output.encode("ascii") + b"\n"
        elif operation == "stop":
            if self.stop_changes_state:
                self.inspect["status"] = "exited"
                self.inspect["running"] = False
            stdout = self.stop_output.encode("ascii") + b"\n"
        else:
            raise AssertionError(f"forbidden transcript command: {operation}")
        return CommandResult(
            argv=exact,
            returncode=0,
            stdout=stdout,
            stderr=b"",
        )


def _adapter(tmp_path, runner=None):
    baseline, expected = _port_inputs()
    docker = (tmp_path / "docker.exe").resolve()
    docker.write_bytes(b"owned fake executable")
    transcript = runner or TranscriptRunner(_inspect(expected))
    return (
        DockerLifecycleCommandPort(
            baseline_observation=baseline,
            expected_container=expected,
            docker_executable=docker,
            expected_docker_version=DOCKER_VERSION,
            runner=transcript,
        ),
        transcript,
        docker,
    )


def test_exact_adapter_runs_only_version_inspect_update_and_stop(tmp_path):
    adapter, runner, docker = _adapter(tmp_path)

    running = adapter.observe()
    adapter.set_restart_policy_no(EXACT_CONTAINER_ID)
    adapter.stop(EXACT_CONTAINER_ID, 30)
    stopped = adapter.observe()

    assert running["matching_containers"][0]["running"] is True
    assert stopped["matching_containers"][0]["running"] is False
    assert stopped["matching_containers"][0]["restart_policy"] == {
        "name": "no",
        "maximum_retry_count": 0,
    }
    commands = [call["argv"][1] for call in runner.calls]
    assert commands == [
        "version",
        "inspect",
        "version",
        "inspect",
        "update",
        "inspect",
        "version",
        "inspect",
        "stop",
        "inspect",
        "version",
        "inspect",
    ]
    update = next(
        call["argv"] for call in runner.calls
        if call["argv"][1] == "update"
    )
    stop = next(
        call["argv"] for call in runner.calls
        if call["argv"][1] == "stop"
    )
    assert update == (
        str(docker),
        "update",
        "--restart=no",
        EXACT_CONTAINER_ID,
    )
    assert stop == (
        str(docker),
        "stop",
        "--time",
        "30",
        EXACT_CONTAINER_ID,
    )
    for call in runner.calls:
        assert call["cwd"] == docker.parent
        assert call["env"] == {"DOCKER_HOST": DOCKER_HOST}
        assert call["max_capture_bytes"] == MAX_CAPTURE_BYTES
    evidence = adapter.evidence()
    assert len(evidence) == len(runner.calls)
    assert all("stdout" not in row and "stderr" not in row for row in evidence)
    assert all(
        set(row) == {
            "operation",
            "argv_sha256",
            "returncode",
            "stdout_bytes",
            "stdout_sha256",
            "stderr_bytes",
            "stderr_sha256",
            "stdout_overflow",
            "stderr_overflow",
            "timed_out",
        }
        for row in evidence
    )


@pytest.mark.parametrize(
    "mutation, code",
    (
        (lambda row: row.update(id=EXACT_CONTAINER_ID[:-1]), "IDENTITY_DRIFT"),
        (lambda row: row.update(image_id="sha256:" + "0" * 64), "IDENTITY_DRIFT"),
        (lambda row: row["command"].append("--restart"), "IDENTITY_DRIFT"),
        (lambda row: row.update(started_at_utc="2026-10-20T00:00:00Z"), "IDENTITY_DRIFT"),
        (
            lambda row: row["labels"].update(
                {"com.docker.compose.service": "other"}
            ),
            "IDENTITY_DRIFT",
        ),
        (
            lambda row: row["mounts"][0].update(Destination="/other"),
            "IDENTITY_DRIFT",
        ),
        (lambda row: row.update(health_status=None), "FIELDS_AMBIGUOUS"),
    ),
)
def test_adapter_refuses_identity_health_label_and_mount_drift(
    tmp_path,
    mutation,
    code,
):
    baseline, expected = _port_inputs()
    inspect = _inspect(expected)
    mutation(inspect)
    docker = (tmp_path / "docker.exe").resolve()
    docker.write_bytes(b"owned fake executable")
    adapter = DockerLifecycleCommandPort(
        baseline_observation=baseline,
        expected_container=expected,
        docker_executable=docker,
        expected_docker_version=DOCKER_VERSION,
        runner=TranscriptRunner(inspect),
    )

    with pytest.raises(StopOnlyExecutionError, match=code):
        adapter.observe()


@pytest.mark.parametrize(
    "target",
    (
        "stock-screener-realtime-v2-retention",
        EXACT_CONTAINER_ID[:12],
        "0" * 64,
    ),
)
def test_adapter_refuses_names_prefixes_and_other_ids(tmp_path, target):
    adapter, runner, _docker = _adapter(tmp_path)

    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_CONTAINER_TARGET_REFUSED",
    ):
        adapter.set_restart_policy_no(target)
    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_CONTAINER_TARGET_REFUSED",
    ):
        adapter.stop(target, 30)

    assert runner.calls == []


def test_adapter_refuses_extra_timeout_and_has_no_other_lifecycle_verbs(
    tmp_path,
):
    adapter, runner, _docker = _adapter(tmp_path)

    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_STOP_TIMEOUT_REFUSED",
    ):
        adapter.stop(EXACT_CONTAINER_ID, 31)

    assert runner.calls == []
    for name in (
        "start",
        "restart",
        "remove",
        "kill",
        "rm",
        "pause",
        "unpause",
    ):
        assert not hasattr(adapter, name)


@pytest.mark.parametrize(
    "result",
    (
        {
            "returncode": 1,
            "stdout": b"",
            "stderr": b"synthetic failure",
        },
        {
            "returncode": 0,
            "stdout": b'"29.8.1"\n',
            "stderr": b"synthetic warning",
        },
        {
            "returncode": 0,
            "stdout": b'"29.8.1"\n',
            "stderr": b"",
            "stdout_overflow": True,
        },
        {
            "returncode": 0,
            "stdout": b'"29.8.1"\n',
            "stderr": b"",
            "timed_out": True,
        },
    ),
)
def test_adapter_normalizes_nonzero_stderr_overflow_and_timeout(
    tmp_path,
    result,
):
    baseline, expected = _port_inputs()
    runner = TranscriptRunner(_inspect(expected))
    runner.next_result = result
    docker = (tmp_path / "docker.exe").resolve()
    docker.write_bytes(b"owned fake executable")
    adapter = DockerLifecycleCommandPort(
        baseline_observation=baseline,
        expected_container=expected,
        docker_executable=docker,
        expected_docker_version=DOCKER_VERSION,
        runner=runner,
    )

    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_COMMAND_RESULT_AMBIGUOUS",
    ) as raised:
        adapter.observe()

    assert "synthetic" not in str(raised.value)
    assert "stdout_bytes=" in str(raised.value)
    assert "stderr_bytes=" in str(raised.value)


def test_adapter_refuses_version_drift_and_multiple_inspect_results(tmp_path):
    baseline, expected = _port_inputs()
    docker = (tmp_path / "docker.exe").resolve()
    docker.write_bytes(b"owned fake executable")
    version_drift = DockerLifecycleCommandPort(
        baseline_observation=baseline,
        expected_container=expected,
        docker_executable=docker,
        expected_docker_version=DOCKER_VERSION,
        runner=TranscriptRunner(_inspect(expected), version="29.8.2"),
    )
    with pytest.raises(StopOnlyExecutionError, match="DOCKER_VERSION_DRIFT"):
        version_drift.observe()

    runner = TranscriptRunner(_inspect(expected))
    adapter = DockerLifecycleCommandPort(
        baseline_observation=baseline,
        expected_container=expected,
        docker_executable=docker,
        expected_docker_version=DOCKER_VERSION,
        runner=runner,
    )
    adapter._preflight_executable()
    runner.next_result = {
        "returncode": 0,
        "stdout": b"{}\n{}\n",
        "stderr": b"",
    }
    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_INSPECT_OUTPUT_AMBIGUOUS",
    ):
        adapter._inspect()


def test_update_and_stop_ambiguous_results_are_fail_closed(tmp_path):
    adapter, runner, _docker = _adapter(tmp_path)
    runner.update_output = "stock-screener-realtime-v2-retention"
    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_UPDATE_OUTPUT_AMBIGUOUS",
    ):
        adapter.set_restart_policy_no(EXACT_CONTAINER_ID)

    adapter, runner, _docker = _adapter(tmp_path)
    adapter.set_restart_policy_no(EXACT_CONTAINER_ID)
    runner.stop_changes_state = False
    with pytest.raises(
        StopOnlyExecutionError,
        match="DOCKER_STOP_POSTCONDITION_FAILED",
    ):
        adapter.stop(EXACT_CONTAINER_ID, 30)
