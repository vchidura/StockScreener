"""Guarded stop-only executor over an injected lifecycle command boundary."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Protocol

from .retention_lifecycle_authorization import (
    canonical_sha256,
    lifecycle_status,
    validate_lifecycle_authorization,
    validate_post_stop_snapshot,
)


EXPECTED_PACKAGE_SHA256 = (
    "57856f54e4bfa228e70020ac7417df07cab12a79e29c45094798879059ab69fb"
)
EXPECTED_SCOPE_SHA256 = (
    "dfd3aa39f210fb7233d39929ccf25c6febc7ac3d0b8b8a2aa24a27868709b6d8"
)
EXPECTED_NONCE = "51f8c2a4-8d7e-4b39-9a61-0e5d3c7f8426"
EXPECTED_WINDOW_START = datetime(2026, 10, 22, 15, 30, tzinfo=timezone.utc)
EXPECTED_WINDOW_END = datetime(2026, 10, 22, 15, 40, tzinfo=timezone.utc)
EXPECTED_PACKAGE_FILE_SHA256 = (
    "9fe761580f6e876cfb4144a9c9b2a70ca452a5d71f84a66c8dff129e97816ddf"
)
EXPECTED_POST_STOP_FILE_SHA256 = (
    "aaa9b98525091e9abf0ad32c22c44cfb3a153474d585c5d5ac2d7b9ffce88b59"
)
STATUS_READY = "READY_FOR_EXPLICIT_STOP_AUTHORIZATION"
STATUS_LOCKED = "LOCKED_OR_INTERRUPTED"
STATUS_AMBIGUOUS = "AMBIGUOUS_PRIOR_STOP"
STATUS_CONSUMED = "STOP_CONSUMED_POST_SNAPSHOT_PASS"
_UTC = timezone.utc


class StopOnlyExecutionError(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class LifecycleCommandPort(Protocol):
    def observe(self) -> Mapping[str, object]: ...

    def set_restart_policy_no(self, container_id: str) -> None: ...

    def stop(self, container_id: str, timeout_seconds: int) -> None: ...


class PostStopSnapshotProvider(Protocol):
    def capture(
        self,
        package: Mapping[str, object],
        observation: Mapping[str, object],
        *,
        captured_at: datetime,
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class StopExecutionStatus:
    state: str
    detail: str
    lock_present: bool
    receipt: Mapping[str, object] | None
    lifecycle: Mapping[str, object] | None

    def as_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "detail": self.detail,
            "lock_present": self.lock_present,
            "receipt": dict(self.receipt) if self.receipt else None,
            "lifecycle": dict(self.lifecycle) if self.lifecycle else None,
        }


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(_UTC)


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def load_exact_lifecycle_package(
    package_path: Path,
    post_stop_path: Path,
) -> dict[str, object]:
    if hashlib.sha256(package_path.read_bytes()).hexdigest() != (
        EXPECTED_PACKAGE_FILE_SHA256
    ):
        raise ValueError("stop-only package file identity changed")
    if hashlib.sha256(post_stop_path.read_bytes()).hexdigest() != (
        EXPECTED_POST_STOP_FILE_SHA256
    ):
        raise ValueError("post-stop contract file identity changed")
    package = _json(package_path)
    contract = _json(post_stop_path)
    unsigned = dict(package)
    package_sha256 = unsigned.pop("package_sha256", None)
    window = package.get("window")
    if (
        package_sha256 != EXPECTED_PACKAGE_SHA256
        or canonical_sha256(unsigned) != EXPECTED_PACKAGE_SHA256
        or package.get("lifecycle_scope_sha256") != EXPECTED_SCOPE_SHA256
        or package.get("lifecycle_nonce") != EXPECTED_NONCE
        or not isinstance(window, dict)
        or datetime.fromisoformat(window["start_utc"]) != EXPECTED_WINDOW_START
        or datetime.fromisoformat(window["stop_utc"]) != EXPECTED_WINDOW_END
        or package.get("post_stop_contract") != contract
        or canonical_sha256(contract)
        != package.get("post_stop_contract_sha256")
    ):
        raise ValueError("stop-only package exact identity changed")
    return package


class ExclusiveStopLock:
    def __init__(self, path: Path, package: Mapping[str, object]) -> None:
        self.path = path.resolve()
        self.package = package

    def acquire(self, *, acquired_at: datetime) -> None:
        if not self.path.parent.is_dir():
            raise ValueError("stop lock parent must exist")
        value = {
            "schema": "options-retention-stop-lock-v1",
            "package_sha256": self.package["package_sha256"],
            "lifecycle_nonce": self.package["lifecycle_nonce"],
            "state": "LOCKED",
            "acquired_at_utc": _utc(
                acquired_at,
                "acquired_at",
            ).isoformat(),
        }
        try:
            with self.path.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as error:
            raise StopOnlyExecutionError(
                "STOP_LOCK_CONFLICT",
                str(self.path),
            ) from error

    def release(self) -> None:
        self.path.unlink()


class DurableStopReceipt:
    def __init__(self, path: Path, package: Mapping[str, object]) -> None:
        self.path = path.resolve()
        self.package = package

    def create(self, *, created_at: datetime) -> None:
        if not self.path.parent.is_dir():
            raise ValueError("stop receipt parent must exist")
        value = {
            "schema": "options-retention-stop-receipt-v1",
            "package_sha256": self.package["package_sha256"],
            "lifecycle_nonce": self.package["lifecycle_nonce"],
            "state": "STOP_ATTEMPT_STARTED",
            "created_at_utc": _utc(
                created_at,
                "created_at",
            ).isoformat(),
            "history": [],
        }
        try:
            self._write(value, exclusive=True)
        except FileExistsError as error:
            raise StopOnlyExecutionError(
                "STOP_RECEIPT_ALREADY_EXISTS",
                str(self.path),
            ) from error

    def read(self) -> dict[str, object]:
        value = _json(self.path)
        if (
            value.get("package_sha256") != self.package["package_sha256"]
            or value.get("lifecycle_nonce") != self.package["lifecycle_nonce"]
        ):
            raise StopOnlyExecutionError(
                "STOP_RECEIPT_IDENTITY_DRIFT",
                str(self.path),
            )
        return value

    def update(
        self,
        state: str,
        *,
        checked_at: datetime,
        detail: Mapping[str, object] | None = None,
    ) -> None:
        value = self.read()
        history = list(value.get("history") or [])
        history.append({
            "state": state,
            "checked_at_utc": _utc(
                checked_at,
                "checked_at",
            ).isoformat(),
            "detail": dict(detail or {}),
        })
        value.update({
            "state": state,
            "updated_at_utc": _utc(
                checked_at,
                "checked_at",
            ).isoformat(),
            "history": history,
        })
        self._write(value, exclusive=False)

    def _write(self, value: object, *, exclusive: bool) -> None:
        if exclusive:
            with self.path.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return
        temporary = self.path.with_name(
            f"{self.path.name}.{self.package['lifecycle_nonce']}.partial"
        )
        try:
            with temporary.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


class ObservationSnapshotProvider:
    """Build a fake/read-only post-stop snapshot from the injected observation."""

    def capture(
        self,
        package: Mapping[str, object],
        observation: Mapping[str, object],
        *,
        captured_at: datetime,
    ) -> Mapping[str, object]:
        contract = package["post_stop_contract"]
        container = observation["matching_containers"]
        if len(container) != 1:
            raise StopOnlyExecutionError(
                "POST_STOP_CONTAINER_AMBIGUOUS",
                str(len(container)),
            )
        actual = container[0]
        expected = contract["expected_container"]
        identity = {
            "matching_count": 1,
            **{
                key: actual[key]
                for key in (
                "id",
                "name",
                "service",
                "image_id",
                "command_sha256",
                "started_at_utc",
                "status",
                "running",
                "restart_policy",
                )
            },
        }
        if identity != expected:
            raise StopOnlyExecutionError(
                "POST_STOP_CONTAINER_DRIFT",
                json.dumps(identity, sort_keys=True),
            )
        return {
            "schema": contract["schema"],
            "lifecycle_package_sha256": package["package_sha256"],
            "captured_at_utc": _utc(
                captured_at,
                "captured_at",
            ).isoformat(),
            "container": identity,
            "database_snapshot": {
                "transaction_read_only": "on",
                "transaction_isolation": "repeatable read",
                "writes": 0,
                "ledger_rows": observation["ledger_rows"],
                "source_counts": observation["source_counts"],
            },
            "archive_inventory": observation["archive_inventory"],
            "lock_or_partial_files": observation["lock_or_partial_files"],
            "revision_classifications": contract[
                "expected_revision_classifications"
            ],
            "effects": contract["forbidden_effects"],
        }


class FakeLifecycleCommandPort:
    """Owned JSON-backed fake. It has no start/restart/remove/kill operations."""

    def __init__(self, state_path: Path) -> None:
        self.state_path = state_path.resolve()
        if not self.state_path.is_file():
            raise ValueError("fake lifecycle state file is unavailable")

    def _state(self):
        value = _json(self.state_path)
        if set(value) != {"observation", "commands"}:
            raise ValueError("fake lifecycle state fields are not exact")
        return value

    def _write(self, value):
        temporary = self.state_path.with_suffix(
            self.state_path.suffix + ".partial"
        )
        try:
            with temporary.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.state_path)
        finally:
            temporary.unlink(missing_ok=True)

    def observe(self) -> Mapping[str, object]:
        return self._state()["observation"]

    def commands(self) -> tuple[Mapping[str, object], ...]:
        return tuple(self._state()["commands"])

    def set_restart_policy_no(self, container_id: str) -> None:
        value = self._state()
        [container] = value["observation"]["matching_containers"]
        if container["id"] != container_id:
            raise StopOnlyExecutionError(
                "FAKE_CONTAINER_ID_DRIFT",
                container_id,
            )
        if container["restart_policy"] != {
            "name": "unless-stopped",
            "maximum_retry_count": 0,
        }:
            raise StopOnlyExecutionError(
                "FAKE_RESTART_POLICY_DRIFT",
                json.dumps(container["restart_policy"], sort_keys=True),
            )
        container["restart_policy"] = {
            "name": "no",
            "maximum_retry_count": 0,
        }
        value["commands"].append({
            "action": "SET_RESTART_POLICY_NO",
            "container_id": container_id,
        })
        self._write(value)

    def stop(self, container_id: str, timeout_seconds: int) -> None:
        value = self._state()
        [container] = value["observation"]["matching_containers"]
        if (
            container["id"] != container_id
            or timeout_seconds != 30
            or container["restart_policy"] != {
                "name": "no",
                "maximum_retry_count": 0,
            }
            or container["running"] is not True
        ):
            raise StopOnlyExecutionError(
                "FAKE_STOP_SCOPE_DRIFT",
                container_id,
            )
        container.update({
            "status": "exited",
            "running": False,
            "health": None,
        })
        value["commands"].append({
            "action": "STOP_CONTAINER",
            "container_id": container_id,
            "timeout_seconds": 30,
        })
        self._write(value)


class StopOnlyExecutor:
    def __init__(
        self,
        command_port: LifecycleCommandPort,
        snapshot_provider: PostStopSnapshotProvider,
        *,
        repository_root: Path,
        readiness_report_path: Path,
        lock_path: Path,
        receipt_path: Path,
    ) -> None:
        self._port = command_port
        self._snapshot_provider = snapshot_provider
        self._repository_root = repository_root.resolve()
        self._readiness_report_path = readiness_report_path.resolve()
        self._lock_path = lock_path.resolve()
        self._receipt_path = receipt_path.resolve()

    def _resources(self, package):
        return (
            ExclusiveStopLock(self._lock_path, package),
            DurableStopReceipt(self._receipt_path, package),
        )

    def plan(self, package: Mapping[str, object]) -> dict[str, object]:
        return {
            "mode": "PLAN",
            "package_sha256": package["package_sha256"],
            "lifecycle_scope_sha256": package["lifecycle_scope_sha256"],
            "lifecycle_nonce": package["lifecycle_nonce"],
            "window": package["window"],
            "operations": package["lifecycle_scope"][
                "allowed_lifecycle_actions"
            ],
            "post_stop": "READ_ONLY_FROZEN_CONTRACT",
            "service_lifecycle_actions": 0,
            "production_writes": 0,
            "provider_requests": 0,
        }

    def status(self, package: Mapping[str, object]) -> StopExecutionStatus:
        receipt = None
        lifecycle = None
        if self._receipt_path.exists():
            receipt = DurableStopReceipt(
                self._receipt_path,
                package,
            ).read()
            state = str(receipt["state"])
            if state == STATUS_CONSUMED:
                return StopExecutionStatus(
                    STATUS_CONSUMED,
                    "durable one-use stop receipt is complete",
                    self._lock_path.exists(),
                    receipt,
                    None,
                )
            return StopExecutionStatus(
                STATUS_AMBIGUOUS,
                f"durable stop receipt state is {state}",
                self._lock_path.exists(),
                receipt,
                None,
            )
        if self._lock_path.exists():
            return StopExecutionStatus(
                STATUS_LOCKED,
                "exclusive stop lock exists without a durable receipt",
                True,
                None,
                None,
            )
        observation = self._port.observe()
        lifecycle = lifecycle_status(package, observation)
        state = str(lifecycle["state"])
        return StopExecutionStatus(
            state,
            "current injected lifecycle observation",
            False,
            None,
            lifecycle,
        )

    def preflight(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> dict[str, object]:
        status = self.status(package)
        if status.state != STATUS_READY:
            raise StopOnlyExecutionError(
                "STOP_NOT_READY",
                status.state,
            )
        observation = self._port.observe()
        result = validate_lifecycle_authorization(
            package,
            readiness_report_path=self._readiness_report_path,
            current_observation=observation,
            repository_root=self._repository_root,
            expected_package_sha256=EXPECTED_PACKAGE_SHA256,
            now=checked_at,
            prior_stop_records=(),
        )
        return {
            "mode": "PREFLIGHT",
            **result,
            "stop_status": status.as_dict(),
            "service_lifecycle_actions": 0,
            "production_writes": 0,
            "provider_requests": 0,
        }

    def execute(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
        fault: Callable[[str], None] = lambda _stage: None,
    ) -> dict[str, object]:
        checked = _utc(checked_at, "checked_at")
        if not EXPECTED_WINDOW_START <= checked < EXPECTED_WINDOW_END:
            code = (
                "EARLY_STOP_WINDOW"
                if checked < EXPECTED_WINDOW_START
                else "MISSED_STOP_WINDOW"
            )
            raise StopOnlyExecutionError(code, checked.isoformat())
        self.preflight(package, checked_at=checked)
        lock, receipt = self._resources(package)
        lock.acquire(acquired_at=checked)
        phase = "AFTER_LOCK"
        try:
            fault(phase)
            receipt.create(created_at=checked)
            phase = "AFTER_RECEIPT"
            fault(phase)
            observation = self._port.observe()
            validate_lifecycle_authorization(
                package,
                readiness_report_path=self._readiness_report_path,
                current_observation=observation,
                repository_root=self._repository_root,
                expected_package_sha256=EXPECTED_PACKAGE_SHA256,
                now=checked,
                prior_stop_records=(),
            )
            [container] = observation["matching_containers"]
            container_id = container["id"]
            self._port.set_restart_policy_no(container_id)
            receipt.update(
                "RESTART_POLICY_DISABLED_STOP_PENDING",
                checked_at=checked,
                detail={"container_id": container_id},
            )
            phase = "AFTER_RESTART_POLICY"
            fault(phase)
            partial = lifecycle_status(package, self._port.observe())
            if partial["state"] != "PARTIAL_RESTART_POLICY_CHANGED":
                raise StopOnlyExecutionError(
                    "RESTART_POLICY_POSTCONDITION_FAILED",
                    str(partial),
                )
            self._port.stop(container_id, 30)
            receipt.update(
                "STOPPED_POST_SNAPSHOT_PENDING",
                checked_at=checked,
                detail={"container_id": container_id},
            )
            phase = "AFTER_STOP"
            fault(phase)
            stopped_observation = self._port.observe()
            if lifecycle_status(package, stopped_observation)["state"] != (
                "STOP_CONSUMED_POST_SNAPSHOT_REQUIRED"
            ):
                raise StopOnlyExecutionError(
                    "STOP_POSTCONDITION_FAILED",
                    container_id,
                )
            phase = "BEFORE_POST_STOP_SNAPSHOT"
            fault(phase)
            snapshot = self._snapshot_provider.capture(
                package,
                stopped_observation,
                captured_at=checked,
            )
            post_stop = validate_post_stop_snapshot(package, snapshot)
            receipt.update(
                STATUS_CONSUMED,
                checked_at=checked,
                detail={
                    "container_id": container_id,
                    "post_stop_status": post_stop["status"],
                },
            )
            phase = "AFTER_POST_STOP_SNAPSHOT"
            fault(phase)
            lock.release()
            return {
                "status": STATUS_CONSUMED,
                "package_sha256": package["package_sha256"],
                "lifecycle_nonce": package["lifecycle_nonce"],
                "commands": [
                    {
                        "action": "SET_RESTART_POLICY_NO",
                        "container_id": container_id,
                    },
                    {
                        "action": "STOP_CONTAINER",
                        "container_id": container_id,
                        "timeout_seconds": 30,
                    },
                ],
                "post_stop": post_stop,
                "production_writes": 0,
                "provider_requests": 0,
                "reconciliation_actions": 0,
                "archive_actions": 0,
                "delete_prune_actions": 0,
                "start_restart_remove_kill_actions": 0,
            }
        except BaseException as error:
            if receipt.path.exists():
                try:
                    receipt.update(
                        "AMBIGUOUS_STOP_REQUIRES_OPERATOR",
                        checked_at=checked,
                        detail={
                            "phase": phase,
                            "error_type": type(error).__name__,
                            "error": str(error)[:240],
                        },
                    )
                except BaseException:
                    pass
            if isinstance(error, StopOnlyExecutionError):
                raise
            raise StopOnlyExecutionError(
                "STOP_EXECUTION_INTERRUPTED",
                f"phase={phase}; {type(error).__name__}: {str(error)[:240]}",
            ) from error
