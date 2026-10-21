"""Exact-ID Docker command boundary for the O-WP5 stop-only executor."""
from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, Sequence

from .retention_lifecycle_authorization import canonical_sha256
from .retention_lifecycle_executor import StopOnlyExecutionError


EXACT_CONTAINER_ID = (
    "6f2424ce65b3132ae8e5ec766a503919f2adc493fb4907f68eeffff67f2601ef"
)
DOCKER_HOST = "npipe:////./pipe/docker_engine"
MAX_CAPTURE_BYTES = 128 * 1024
INSPECT_TIMEOUT_SECONDS = 20
UPDATE_TIMEOUT_SECONDS = 30
STOP_TIMEOUT_SECONDS = 45
_DOCKER_VERSION_FORMAT = "{{json .Client.Version}}"
_DOCKER_INSPECT_FORMAT = (
    '{"id":{{json .Id}},'
    '"name":{{json .Name}},'
    '"image_id":{{json .Image}},'
    '"command":{{json .Config.Cmd}},'
    '"entrypoint":{{json .Config.Entrypoint}},'
    '"started_at_utc":{{json .State.StartedAt}},'
    '"status":{{json .State.Status}},'
    '"running":{{json .State.Running}},'
    '"health_status":{{json .State.Health.Status}},'
    '"restart_policy":{{json .HostConfig.RestartPolicy}},'
    '"labels":{{json .Config.Labels}},'
    '"mounts":{{json .Mounts}}}'
)
_COMPOSE_LABELS = {
    "config_hash": "com.docker.compose.config-hash",
    "container_number": "com.docker.compose.container-number",
    "image": "com.docker.compose.image",
    "oneoff": "com.docker.compose.oneoff",
    "project": "com.docker.compose.project",
    "service": "com.docker.compose.service",
    "version": "com.docker.compose.version",
}


@dataclass(frozen=True, slots=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: bytes
    stderr: bytes
    stdout_overflow: bool = False
    stderr_overflow: bool = False
    timed_out: bool = False


class CommandRunner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout_seconds: int,
        max_capture_bytes: int,
    ) -> CommandResult: ...


class BoundedSubprocessRunner:
    """Run one argv-only process while bounding both captured streams."""

    def __call__(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout_seconds: int,
        max_capture_bytes: int,
    ) -> CommandResult:
        exact_argv = tuple(argv)
        if (
            not exact_argv
            or any(not isinstance(value, str) or not value for value in exact_argv)
        ):
            raise ValueError("subprocess argv must contain only non-empty strings")
        process = subprocess.Popen(
            list(exact_argv),
            cwd=str(cwd),
            env=dict(env),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
        )
        stdout = bytearray()
        stderr = bytearray()
        overflow = {"stdout": False, "stderr": False}

        def read_stream(name: str, stream, sink: bytearray) -> None:
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    return
                remaining = max_capture_bytes - len(sink)
                if remaining > 0:
                    sink.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    overflow[name] = True

        stdout_thread = threading.Thread(
            target=read_stream,
            args=("stdout", process.stdout, stdout),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=read_stream,
            args=("stderr", process.stderr, stderr),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        timed_out = False
        try:
            returncode = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            returncode = process.wait()
        stdout_thread.join()
        stderr_thread.join()
        return CommandResult(
            argv=exact_argv,
            returncode=returncode,
            stdout=bytes(stdout),
            stderr=bytes(stderr),
            stdout_overflow=overflow["stdout"],
            stderr_overflow=overflow["stderr"],
            timed_out=timed_out,
        )


class DockerLifecycleCommandPort:
    """Docker port with only exact inspect, update-restart, and stop commands."""

    def __init__(
        self,
        *,
        baseline_observation: Mapping[str, object],
        expected_container: Mapping[str, object],
        docker_executable: Path,
        expected_docker_version: str,
        runner: CommandRunner | None = None,
    ) -> None:
        if not docker_executable.is_absolute():
            raise ValueError("docker executable path must be absolute")
        executable = docker_executable.resolve(strict=True)
        if not executable.is_file() or executable.name.casefold() != "docker.exe":
            raise ValueError("docker executable must be an exact docker.exe file")
        if (
            not expected_docker_version
            or expected_docker_version.strip() != expected_docker_version
            or len(expected_docker_version) > 64
            or any(
                not (character.isalnum() or character in ".+-")
                for character in expected_docker_version
            )
        ):
            raise ValueError("expected Docker version is not exact")
        baseline = copy.deepcopy(dict(baseline_observation))
        containers = baseline.get("matching_containers")
        expected = copy.deepcopy(dict(expected_container))
        if (
            not isinstance(containers, list)
            or len(containers) != 1
            or containers[0] != expected
            or expected.get("id") != EXACT_CONTAINER_ID
        ):
            raise ValueError("Docker port requires the exact frozen container")
        self._baseline = baseline
        self._expected = expected
        self._docker_executable = executable
        self._expected_docker_version = expected_docker_version
        self._runner = runner or BoundedSubprocessRunner()
        self._cwd = executable.parent
        self._env = {"DOCKER_HOST": DOCKER_HOST}
        self._evidence: list[dict[str, object]] = []

    def observe(self) -> Mapping[str, object]:
        self._preflight_executable()
        container = self._inspect()
        return self._observation(container)

    def set_restart_policy_no(self, container_id: str) -> None:
        self._require_exact_target(container_id)
        self._preflight_executable()
        before = self._inspect()
        self._require_state(
            before,
            running=True,
            status="running",
            restart_policy={
                "name": "unless-stopped",
                "maximum_retry_count": 0,
            },
            code="DOCKER_UPDATE_PRECONDITION_DRIFT",
        )
        result = self._invoke(
            "UPDATE_RESTART_POLICY_NO",
            ("update", "--restart=no", EXACT_CONTAINER_ID),
            timeout_seconds=UPDATE_TIMEOUT_SECONDS,
        )
        if self._single_line(result, "DOCKER_UPDATE_OUTPUT_AMBIGUOUS") != (
            EXACT_CONTAINER_ID
        ):
            raise StopOnlyExecutionError(
                "DOCKER_UPDATE_OUTPUT_AMBIGUOUS",
                self._result_detail("UPDATE_RESTART_POLICY_NO", result),
            )
        after = self._inspect()
        self._require_state(
            after,
            running=True,
            status="running",
            restart_policy={"name": "no", "maximum_retry_count": 0},
            code="DOCKER_UPDATE_POSTCONDITION_FAILED",
        )

    def stop(self, container_id: str, timeout_seconds: int) -> None:
        self._require_exact_target(container_id)
        if isinstance(timeout_seconds, bool) or timeout_seconds != 30:
            raise StopOnlyExecutionError(
                "DOCKER_STOP_TIMEOUT_REFUSED",
                "only the exact 30-second timeout is permitted",
            )
        self._preflight_executable()
        before = self._inspect()
        self._require_state(
            before,
            running=True,
            status="running",
            restart_policy={"name": "no", "maximum_retry_count": 0},
            code="DOCKER_STOP_PRECONDITION_DRIFT",
        )
        result = self._invoke(
            "STOP_CONTAINER",
            ("stop", "--time", "30", EXACT_CONTAINER_ID),
            timeout_seconds=STOP_TIMEOUT_SECONDS,
        )
        if self._single_line(result, "DOCKER_STOP_OUTPUT_AMBIGUOUS") != (
            EXACT_CONTAINER_ID
        ):
            raise StopOnlyExecutionError(
                "DOCKER_STOP_OUTPUT_AMBIGUOUS",
                self._result_detail("STOP_CONTAINER", result),
            )
        after = self._inspect()
        self._require_state(
            after,
            running=False,
            status="exited",
            restart_policy={"name": "no", "maximum_retry_count": 0},
            code="DOCKER_STOP_POSTCONDITION_FAILED",
        )

    def evidence(self) -> tuple[Mapping[str, object], ...]:
        return tuple(copy.deepcopy(self._evidence))

    def _preflight_executable(self) -> None:
        result = self._invoke(
            "DOCKER_VERSION",
            ("version", "--format", _DOCKER_VERSION_FORMAT),
            timeout_seconds=INSPECT_TIMEOUT_SECONDS,
        )
        line = self._single_line(result, "DOCKER_VERSION_OUTPUT_AMBIGUOUS")
        try:
            version = json.loads(line)
        except json.JSONDecodeError as error:
            raise StopOnlyExecutionError(
                "DOCKER_VERSION_OUTPUT_AMBIGUOUS",
                self._result_detail("DOCKER_VERSION", result),
            ) from error
        if version != self._expected_docker_version:
            raise StopOnlyExecutionError(
                "DOCKER_VERSION_DRIFT",
                f"expected={self._expected_docker_version}",
            )

    def _inspect(self) -> dict[str, object]:
        result = self._invoke(
            "INSPECT_CONTAINER",
            (
                "inspect",
                "--type",
                "container",
                "--format",
                _DOCKER_INSPECT_FORMAT,
                EXACT_CONTAINER_ID,
            ),
            timeout_seconds=INSPECT_TIMEOUT_SECONDS,
        )
        line = self._single_line(result, "DOCKER_INSPECT_OUTPUT_AMBIGUOUS")
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as error:
            raise StopOnlyExecutionError(
                "DOCKER_INSPECT_OUTPUT_AMBIGUOUS",
                self._result_detail("INSPECT_CONTAINER", result),
            ) from error
        if not isinstance(raw, dict):
            raise StopOnlyExecutionError(
                "DOCKER_INSPECT_CONTAINER_SET_AMBIGUOUS",
                "inspect did not return exactly one container object",
            )
        container = self._normalize_container(raw)
        immutable_fields = (
            "id",
            "name",
            "service",
            "image_id",
            "command",
            "command_sha256",
            "entrypoint",
            "started_at_utc",
            "health",
            "compose_identity",
            "mounts",
        )
        drift = [
            name
            for name in immutable_fields
            if container.get(name) != self._expected.get(name)
        ]
        if drift:
            raise StopOnlyExecutionError(
                "DOCKER_CONTAINER_IDENTITY_DRIFT",
                ",".join(drift),
            )
        return container

    def _normalize_container(
        self,
        raw: Mapping[str, object],
    ) -> dict[str, object]:
        required = {
            "id",
            "name",
            "image_id",
            "command",
            "entrypoint",
            "started_at_utc",
            "status",
            "running",
            "health_status",
            "restart_policy",
            "labels",
            "mounts",
        }
        if set(raw) != required:
            raise StopOnlyExecutionError(
                "DOCKER_INSPECT_FIELDS_AMBIGUOUS",
                "inspect fields are not exact",
            )
        labels = raw["labels"]
        raw_mounts = raw["mounts"]
        restart = raw["restart_policy"]
        health = raw["health_status"]
        if (
            not isinstance(labels, dict)
            or not isinstance(raw_mounts, list)
            or not isinstance(restart, dict)
            or not isinstance(health, str)
            or not health
        ):
            raise StopOnlyExecutionError(
                "DOCKER_INSPECT_FIELDS_AMBIGUOUS",
                "labels, mounts, restart policy, or health are unavailable",
            )
        command = raw["command"]
        if (
            not isinstance(command, list)
            or any(not isinstance(argument, str) for argument in command)
        ):
            raise StopOnlyExecutionError(
                "DOCKER_INSPECT_FIELDS_AMBIGUOUS",
                "container command is unavailable",
            )
        compose_identity = {
            name: labels.get(label)
            for name, label in _COMPOSE_LABELS.items()
        }
        mounts = sorted(
            (self._normalize_mount(mount) for mount in raw_mounts),
            key=lambda row: (str(row["destination"]), str(row["type"])),
        )
        normalized_restart = {
            "name": restart.get("Name"),
            "maximum_retry_count": restart.get("MaximumRetryCount"),
        }
        container = copy.deepcopy(self._expected)
        container.update({
            "id": raw["id"],
            "name": (
                raw["name"].lstrip("/")
                if isinstance(raw["name"], str)
                else raw["name"]
            ),
            "service": compose_identity["service"],
            "image_id": raw["image_id"],
            "command": command,
            "command_sha256": canonical_sha256(command),
            "entrypoint": raw["entrypoint"],
            "started_at_utc": raw["started_at_utc"],
            "status": raw["status"],
            "running": raw["running"],
            "health": health,
            "restart_policy": normalized_restart,
            "compose_identity": compose_identity,
            "mounts": mounts,
        })
        return container

    @staticmethod
    def _normalize_mount(value: object) -> dict[str, object]:
        if not isinstance(value, dict):
            raise StopOnlyExecutionError(
                "DOCKER_MOUNT_IDENTITY_DRIFT",
                "mount is not an object",
            )
        mount_type = value.get("Type")
        destination = value.get("Destination")
        read_write = value.get("RW")
        if (
            not isinstance(mount_type, str)
            or not isinstance(destination, str)
            or type(read_write) is not bool
        ):
            raise StopOnlyExecutionError(
                "DOCKER_MOUNT_IDENTITY_DRIFT",
                "mount fields are unavailable",
            )
        normalized: dict[str, object] = {
            "type": mount_type,
            "destination": destination,
            "read_write": read_write,
        }
        if mount_type == "volume":
            name = value.get("Name")
            if not isinstance(name, str) or not name:
                raise StopOnlyExecutionError(
                    "DOCKER_MOUNT_IDENTITY_DRIFT",
                    "volume name is unavailable",
                )
            normalized["name"] = name
        else:
            source = value.get("Source")
            if not isinstance(source, str) or not source:
                raise StopOnlyExecutionError(
                    "DOCKER_MOUNT_IDENTITY_DRIFT",
                    "mount source is unavailable",
                )
            normalized["source_path_sha256"] = hashlib.sha256(
                source.encode("utf-8")
            ).hexdigest()
        return normalized

    def _observation(
        self,
        container: Mapping[str, object],
    ) -> dict[str, object]:
        observation = copy.deepcopy(self._baseline)
        observation["matching_containers"] = [copy.deepcopy(dict(container))]
        return observation

    @staticmethod
    def _require_state(
        container: Mapping[str, object],
        *,
        running: bool,
        status: str,
        restart_policy: Mapping[str, object],
        code: str,
    ) -> None:
        if (
            container.get("running") is not running
            or container.get("status") != status
            or container.get("restart_policy") != restart_policy
        ):
            raise StopOnlyExecutionError(code, "exact lifecycle state changed")

    @staticmethod
    def _require_exact_target(container_id: str) -> None:
        if container_id != EXACT_CONTAINER_ID:
            raise StopOnlyExecutionError(
                "DOCKER_CONTAINER_TARGET_REFUSED",
                "only the exact full container ID is permitted",
            )

    def _invoke(
        self,
        operation: str,
        arguments: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> CommandResult:
        argv = (str(self._docker_executable), *arguments)
        try:
            result = self._runner(
                argv,
                cwd=self._cwd,
                env=self._env,
                timeout_seconds=timeout_seconds,
                max_capture_bytes=MAX_CAPTURE_BYTES,
            )
        except (OSError, ValueError) as error:
            raise StopOnlyExecutionError(
                "DOCKER_COMMAND_START_FAILED",
                f"operation={operation}; error_type={type(error).__name__}",
            ) from error
        self._evidence.append({
            "operation": operation,
            "argv_sha256": canonical_sha256(list(argv)),
            "returncode": result.returncode,
            "stdout_bytes": len(result.stdout),
            "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
            "stderr_bytes": len(result.stderr),
            "stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
            "stdout_overflow": result.stdout_overflow,
            "stderr_overflow": result.stderr_overflow,
            "timed_out": result.timed_out,
        })
        if (
            result.argv != argv
            or result.returncode != 0
            or result.stderr
            or result.stdout_overflow
            or result.stderr_overflow
            or result.timed_out
        ):
            raise StopOnlyExecutionError(
                "DOCKER_COMMAND_RESULT_AMBIGUOUS",
                self._result_detail(operation, result),
            )
        return result

    @staticmethod
    def _single_line(result: CommandResult, code: str) -> str:
        try:
            text = result.stdout.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise StopOnlyExecutionError(
                code,
                "stdout is not UTF-8",
            ) from error
        lines = text.splitlines()
        if (
            len(lines) != 1
            or not lines[0]
            or text not in {lines[0], f"{lines[0]}\n", f"{lines[0]}\r\n"}
        ):
            raise StopOnlyExecutionError(code, "stdout is not one exact line")
        return lines[0]

    @staticmethod
    def _result_detail(operation: str, result: CommandResult) -> str:
        return (
            f"operation={operation}; returncode={result.returncode}; "
            f"stdout_bytes={len(result.stdout)}; "
            f"stderr_bytes={len(result.stderr)}; "
            f"stdout_overflow={result.stdout_overflow}; "
            f"stderr_overflow={result.stderr_overflow}; "
            f"timed_out={result.timed_out}"
        )
