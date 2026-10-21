"""Exact Docker service boundary for the four Advanced Options P8 owners."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .advanced_options_control_plane import (
    CANONICAL_FLOW_OWNER,
    CANONICAL_INGRESS_OWNER,
    CANONICAL_MARKET_STATE_OWNER,
)
from .advanced_options_service import (
    AdvancedOptionsProcessKind,
    AdvancedOptionsProcessSpec,
)
from .retention_lifecycle_docker import (
    DOCKER_HOST,
    MAX_CAPTURE_BYTES,
    BoundedSubprocessRunner,
    CommandResult,
)


P8_COMPOSE_PROJECT = "alpha-research-realtime-v2"
P8_COMPOSE_PROFILE = "realtime-v2-options-p8"
P8_STOP_TIMEOUT_SECONDS = 60
P8_COMMAND_TIMEOUT_SECONDS = 90
P8_SERVICE_NAMES = {
    AdvancedOptionsProcessKind.INGRESS: (
        "realtime-v2-options-p8-ingress"
    ),
    AdvancedOptionsProcessKind.MARKET_STATE: (
        "realtime-v2-options-p8-market-state"
    ),
    AdvancedOptionsProcessKind.FLOW: "realtime-v2-options-p8-flow",
    AdvancedOptionsProcessKind.WINDOW: "realtime-v2-options-p8-window",
}
P8_COMPONENT_NAMES = {
    AdvancedOptionsProcessKind.INGRESS: "options-p8-ingress",
    AdvancedOptionsProcessKind.MARKET_STATE: "options-p8-market-state",
    AdvancedOptionsProcessKind.FLOW: "options-p8-flow",
    AdvancedOptionsProcessKind.WINDOW: "options-p8-window",
}
_BASE_ENVIRONMENT_NAMES = (
    "SystemRoot",
    "WINDIR",
    "TEMP",
    "TMP",
    "PATH",
)
_FORBIDDEN_ENVIRONMENT_TOKENS = (
    "BROKER",
    "DELIVERY",
    "TRADING",
    "EXECUTION",
    "POLYGON_API_KEY",
    "MASSIVE_API_KEY",
    "REALTIME_V2_APP_DSN",
    "REALTIME_V2_API_DSN",
    "REALTIME_V2_ADMIN_DSN",
)
_INSPECT_FORMAT = (
    '{"id":{{json .Id}},'
    '"command":{{json .Config.Cmd}},'
    '"entrypoint":{{json .Config.Entrypoint}},'
    '"running":{{json .State.Running}},'
    '"status":{{json .State.Status}},'
    '"health":{{json .State.Health.Status}},'
    '"restart":{{json .HostConfig.RestartPolicy}},'
    '"labels":{{json .Config.Labels}}}'
)


class P8DockerServiceError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


class P8CommandRunner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout_seconds: int,
        max_capture_bytes: int,
    ) -> CommandResult: ...


@dataclass(frozen=True, slots=True)
class P8DockerServiceHandle:
    kind: AdvancedOptionsProcessKind
    service_name: str
    container_id: str


def p8_container_command(
    kind: AdvancedOptionsProcessKind,
) -> tuple[str, ...]:
    if not isinstance(kind, AdvancedOptionsProcessKind):
        raise TypeError("P8 Docker service kind is invalid")
    component = P8_COMPONENT_NAMES[kind]
    arguments = [
        "python",
        "-B",
        "scripts/run_realtime_v2_container.py",
        "--component",
        component,
        "--app-dsn-file",
        "/run/secrets/realtime_v2_app_dsn",
    ]
    if kind is AdvancedOptionsProcessKind.INGRESS:
        arguments.extend([
            "--provider-key-file",
            "/run/secrets/realtime_v2_provider_key",
        ])
    arguments.extend([
        "--",
        "python",
        "-B",
        "scripts/run_advanced_options_p8_canary.py",
        "--docker-service-kind",
        kind.value,
        "--package",
        "/run/advanced-options-p8/package.json",
        "--execution-bundle",
        "/run/advanced-options-p8/execution-bundle.json",
    ])
    return tuple(arguments)


def p8_health_command(
    kind: AdvancedOptionsProcessKind,
) -> tuple[str, ...]:
    if not isinstance(kind, AdvancedOptionsProcessKind):
        raise TypeError("P8 Docker health kind is invalid")
    return (
        "python",
        "-B",
        "scripts/run_advanced_options_p8_canary.py",
        "--docker-health-kind",
        kind.value,
        "--package",
        "/run/advanced-options-p8/package.json",
        "--execution-bundle",
        "/run/advanced-options-p8/execution-bundle.json",
    )


def isolated_p8_compose_environment(
    base_environment: Mapping[str, str],
    *,
    backend_image: str,
    secret_directory: Path,
    package_path: Path,
    bundle_path: Path,
) -> dict[str, str]:
    image = backend_image.strip()
    image_name, separator, digest = image.rpartition("@sha256:")
    local_digest = (
        image.removeprefix("sha256:")
        if image.startswith("sha256:")
        else ""
    )
    if (
        (
            not image_name
            or separator != "@sha256:"
            or len(digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in digest
            )
        )
        and (
            len(local_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in local_digest
            )
        )
        or any(character.isspace() for character in image)
    ):
        raise ValueError("P8 backend image identity is invalid")
    if (
        image.startswith("sha256:")
        and image != f"sha256:{local_digest}"
    ):
        raise ValueError("P8 backend image identity is invalid")
    paths = {
        "REALTIME_V2_DOCKER_SECRET_DIR": secret_directory.resolve(),
        "REALTIME_V2_P8_PACKAGE_PATH": package_path.resolve(),
        "REALTIME_V2_P8_BUNDLE_PATH": bundle_path.resolve(),
    }
    if not package_path.resolve().is_file():
        raise ValueError("P8 package path must be an existing file")
    if not bundle_path.resolve().parent.is_dir():
        raise ValueError("P8 execution bundle parent is absent")
    if not secret_directory.resolve().is_dir():
        raise ValueError("P8 Docker secret directory is absent")
    if any(
        not (secret_directory.resolve() / name).is_file()
        for name in (
            "realtime_v2_app_dsn",
            "realtime_v2_provider_key",
        )
    ):
        raise ValueError("P8 Docker secret files are absent")
    result = {
        name: str(base_environment[name])
        for name in _BASE_ENVIRONMENT_NAMES
        if name in base_environment and str(base_environment[name])
    }
    result.update({
        "DOCKER_HOST": DOCKER_HOST,
        "REALTIME_V2_BACKEND_IMAGE": image,
        **{name: str(path) for name, path in paths.items()},
    })
    if any(
        token in name.upper()
        for name in result
        for token in _FORBIDDEN_ENVIRONMENT_TOKENS
    ):
        raise ValueError("P8 compose environment contains an unsafe value")
    return result


def validate_p8_service_membership(
    package: Mapping[str, object],
) -> None:
    owners = package.get("owners")
    rows = owners.get("processes") if isinstance(owners, Mapping) else None
    if not isinstance(rows, list) or len(rows) != 4:
        raise ValueError("P8 package must name exactly four resident owners")
    expected = [
        {
            "kind": kind.value,
            "owner_name": owner_name,
            "lease_resource_key": lease_key,
        }
        for kind, owner_name, lease_key in (
            (
                AdvancedOptionsProcessKind.INGRESS,
                CANONICAL_INGRESS_OWNER,
                "massive:options:websocket",
            ),
            (
                AdvancedOptionsProcessKind.MARKET_STATE,
                CANONICAL_MARKET_STATE_OWNER,
                (
                    f"advanced-options:{package['cohort']['cohort_id']}:"
                    f"{package['window']['session_date']}:market-state"
                ),
            ),
            (
                AdvancedOptionsProcessKind.FLOW,
                CANONICAL_FLOW_OWNER,
                (
                    f"advanced-options:{package['cohort']['cohort_id']}:"
                    f"{package['window']['session_date']}:flow"
                ),
            ),
            (
                AdvancedOptionsProcessKind.WINDOW,
                "advanced-options-window-v1",
                (
                    f"advanced-options:{package['cohort']['cohort_id']}:"
                    f"{package['window']['session_date']}:window"
                ),
            ),
        )
    ]
    projected = [
        {
            key: row.get(key)
            for key in ("kind", "owner_name", "lease_resource_key")
        }
        for row in rows
        if isinstance(row, Mapping)
    ]
    if (
        projected != expected
        or set(P8_SERVICE_NAMES) != set(AdvancedOptionsProcessKind)
        or len(set(P8_SERVICE_NAMES.values())) != 4
    ):
        raise ValueError("P8 Docker service ownership changed")


class AdvancedOptionsDockerServicePort:
    """AdvancedOptionsProcessPort backed by four exact Compose services."""

    def __init__(
        self,
        *,
        package: Mapping[str, object],
        repository_root: Path,
        docker_executable: Path,
        environment: Mapping[str, str],
        runner: P8CommandRunner | None = None,
    ) -> None:
        validate_p8_service_membership(package)
        root = repository_root.resolve()
        compose_file = (root / "docker-compose.realtime-v2.yml").resolve()
        if not compose_file.is_file():
            raise ValueError("P8 Docker compose file is absent")
        if not docker_executable.is_absolute():
            raise ValueError("P8 docker executable path must be absolute")
        executable = docker_executable.resolve(strict=True)
        if not executable.is_file() or executable.name.casefold() != "docker.exe":
            raise ValueError("P8 docker executable must be an exact docker.exe")
        environment_value = dict(environment)
        if environment_value.get("DOCKER_HOST") != DOCKER_HOST:
            raise ValueError("P8 Docker host is not exact")
        if any(
            name not in {
                *_BASE_ENVIRONMENT_NAMES,
                "DOCKER_HOST",
                "REALTIME_V2_BACKEND_IMAGE",
                "REALTIME_V2_DOCKER_SECRET_DIR",
                "REALTIME_V2_P8_PACKAGE_PATH",
                "REALTIME_V2_P8_BUNDLE_PATH",
            }
            for name in environment_value
        ):
            raise ValueError("P8 Docker environment membership expanded")
        self._root = root
        self._compose_file = compose_file
        self._docker = executable
        self._environment = environment_value
        self._runner = runner or BoundedSubprocessRunner()
        self._specs = {
            AdvancedOptionsProcessKind(str(row["kind"])): (
                str(row["owner_name"]),
                str(row["lease_resource_key"]),
            )
            for row in package["owners"]["processes"]
        }
        self._evidence: list[dict[str, object]] = []

    @property
    def service_names(
        self,
    ) -> Mapping[AdvancedOptionsProcessKind, str]:
        return dict(P8_SERVICE_NAMES)

    def _compose(self, *arguments: str) -> tuple[str, ...]:
        return (
            str(self._docker),
            "compose",
            "--file",
            str(self._compose_file),
            "--project-name",
            P8_COMPOSE_PROJECT,
            "--profile",
            P8_COMPOSE_PROFILE,
            *arguments,
        )

    def _invoke(
        self,
        operation: str,
        argv: Sequence[str],
        *,
        timeout_seconds: int = P8_COMMAND_TIMEOUT_SECONDS,
    ) -> CommandResult:
        result = self._runner(
            tuple(argv),
            cwd=self._root,
            env=self._environment,
            timeout_seconds=timeout_seconds,
            max_capture_bytes=MAX_CAPTURE_BYTES,
        )
        self._evidence.append({
            "operation": operation,
            "argv_sha256": hashlib.sha256(
                "\0".join(result.argv).encode("utf-8")
            ).hexdigest(),
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
            result.returncode != 0
            or result.stdout_overflow
            or result.stderr_overflow
            or result.timed_out
        ):
            raise P8DockerServiceError(
                f"{operation}_FAILED",
                (
                    f"returncode={result.returncode};"
                    f"stdout_bytes={len(result.stdout)};"
                    f"stderr_bytes={len(result.stderr)}"
                ),
            )
        return result

    @staticmethod
    def _single_line(result: CommandResult, code: str) -> str:
        try:
            text = result.stdout.decode("utf-8")
        except UnicodeDecodeError as error:
            raise P8DockerServiceError(code, "stdout is not UTF-8") from error
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) != 1:
            raise P8DockerServiceError(code, "expected exactly one output line")
        return lines[0]

    def _container_id(
        self,
        kind: AdvancedOptionsProcessKind,
    ) -> str:
        result = self._invoke(
            "P8_COMPOSE_PS",
            self._compose("ps", "--quiet", P8_SERVICE_NAMES[kind]),
        )
        value = self._single_line(result, "P8_CONTAINER_ID_AMBIGUOUS")
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise P8DockerServiceError(
                "P8_CONTAINER_ID_AMBIGUOUS",
                value[:80],
            )
        return value

    def _inspect(
        self,
        handle: P8DockerServiceHandle,
    ) -> dict[str, object]:
        result = self._invoke(
            "P8_DOCKER_INSPECT",
            (
                str(self._docker),
                "inspect",
                "--type",
                "container",
                "--format",
                _INSPECT_FORMAT,
                handle.container_id,
            ),
        )
        line = self._single_line(result, "P8_INSPECT_AMBIGUOUS")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise P8DockerServiceError(
                "P8_INSPECT_AMBIGUOUS",
                "inspect output is not JSON",
            ) from error
        if not isinstance(value, dict):
            raise P8DockerServiceError(
                "P8_INSPECT_AMBIGUOUS",
                "inspect output is not an object",
            )
        labels = value.get("labels")
        restart = value.get("restart")
        if (
            value.get("id") != handle.container_id
            or value.get("command") != list(
                p8_container_command(handle.kind)
            )
            or not isinstance(labels, dict)
            or labels.get("com.docker.compose.project")
            != P8_COMPOSE_PROJECT
            or labels.get("com.docker.compose.service")
            != handle.service_name
            or not isinstance(restart, dict)
            or restart.get("Name") != "no"
            or restart.get("MaximumRetryCount") != 0
            or value.get("status") not in {"created", "running", "exited"}
            or type(value.get("running")) is not bool
        ):
            raise P8DockerServiceError(
                "P8_CONTAINER_IDENTITY_DRIFT",
                handle.service_name,
            )
        return value

    def start(self, spec: AdvancedOptionsProcessSpec):
        if not isinstance(spec, AdvancedOptionsProcessSpec):
            raise TypeError("P8 Docker process spec is invalid")
        if (
            spec.owner_name,
            spec.lease_resource_key,
        ) != self._specs[spec.kind]:
            raise P8DockerServiceError(
                "P8_SERVICE_OWNER_IDENTITY_DRIFT",
                spec.kind.value,
            )
        required_paths = (
            Path(self._environment["REALTIME_V2_P8_PACKAGE_PATH"]),
            Path(self._environment["REALTIME_V2_P8_BUNDLE_PATH"]),
            (
                Path(self._environment["REALTIME_V2_DOCKER_SECRET_DIR"])
                / "realtime_v2_app_dsn"
            ),
            (
                Path(self._environment["REALTIME_V2_DOCKER_SECRET_DIR"])
                / "realtime_v2_provider_key"
            ),
        )
        if any(not path.resolve().is_file() for path in required_paths):
            raise P8DockerServiceError(
                "P8_SERVICE_INPUTS_UNAVAILABLE",
                spec.kind.value,
            )
        service = P8_SERVICE_NAMES[spec.kind]
        self._invoke(
            "P8_COMPOSE_UP",
            self._compose(
                "up",
                "--detach",
                "--no-deps",
                "--no-build",
                "--wait",
                "--wait-timeout",
                "60",
                service,
            ),
        )
        handle = P8DockerServiceHandle(
            spec.kind,
            service,
            self._container_id(spec.kind),
        )
        state = self._inspect(handle)
        if (
            state.get("running") is not True
            or state.get("status") != "running"
            or state.get("health") != "healthy"
        ):
            raise P8DockerServiceError(
                "P8_SERVICE_START_POSTCONDITION_FAILED",
                service,
            )
        return handle

    def alive(self, handle) -> bool:
        if not isinstance(handle, P8DockerServiceHandle):
            raise TypeError("P8 Docker service handle is invalid")
        state = self._inspect(handle)
        return (
            state.get("running") is True
            and state.get("status") == "running"
            and state.get("health") == "healthy"
        )

    def stop(self, handle, *, deadline_seconds: int) -> None:
        if not isinstance(handle, P8DockerServiceHandle):
            raise TypeError("P8 Docker service handle is invalid")
        if (
            isinstance(deadline_seconds, bool)
            or deadline_seconds != P8_STOP_TIMEOUT_SECONDS
        ):
            raise P8DockerServiceError(
                "P8_STOP_TIMEOUT_REFUSED",
                str(deadline_seconds),
            )
        before = self._inspect(handle)
        if (
            before.get("running") is not True
            and before.get("status") != "exited"
        ):
            raise P8DockerServiceError(
                "P8_STOP_PRECONDITION_FAILED",
                handle.service_name,
            )
        if before.get("running") is True:
            stop_result = self._invoke(
                "P8_DOCKER_STOP",
                (
                    str(self._docker),
                    "stop",
                    "--time",
                    str(P8_STOP_TIMEOUT_SECONDS),
                    handle.container_id,
                ),
            )
            if self._single_line(
                stop_result,
                "P8_STOP_OUTPUT_AMBIGUOUS",
            ) != handle.container_id:
                raise P8DockerServiceError(
                    "P8_STOP_OUTPUT_AMBIGUOUS",
                    handle.service_name,
                )
            stopped = self._inspect(handle)
            if (
                stopped.get("running") is not False
                or stopped.get("status") != "exited"
            ):
                raise P8DockerServiceError(
                    "P8_STOP_POSTCONDITION_FAILED",
                    handle.service_name,
                )
        remove_result = self._invoke(
            "P8_DOCKER_REMOVE",
            (
                str(self._docker),
                "rm",
                handle.container_id,
            ),
        )
        if self._single_line(
            remove_result,
            "P8_REMOVE_OUTPUT_AMBIGUOUS",
        ) != handle.container_id:
            raise P8DockerServiceError(
                "P8_REMOVE_OUTPUT_AMBIGUOUS",
                handle.service_name,
            )

    def evidence(self) -> tuple[Mapping[str, object], ...]:
        return tuple(dict(row) for row in self._evidence)
