"""One-use guarded executor for an exact Advanced Options P8 canary."""
from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol
from uuid import UUID

from .advanced_options_control_plane import (
    AdvancedOptionsEquityInputBinding,
    AdvancedOptionsEnrollmentPlan,
    AdvancedOptionsHistoryBinding,
    advanced_options_equity_input_binding_payload,
    advanced_options_enrollment_plan_payload,
    advanced_options_history_binding_payload,
)
from .advanced_options_p8_canary_authorization import (
    PREPARED_AUTHORITIES,
    SOURCE_BINDING_PATHS,
    p8_canary_status,
    validate_p8_canary_authorization,
    validate_p8_live_observation,
    validate_p8_page_release_observation,
)
from .advanced_options_service import (
    OPTIONS_NON_SERVING_STAGE_ORDER,
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentLauncher,
)
from .advanced_options_underlying_snapshot import (
    UnderlyingSnapshotBinding,
    underlying_snapshot_binding_payload,
)
from .contracts import MarketChannel
from .option_slot_set import SlotSetPurpose
from .portal_projection_repository import (
    PortalReleasePointer,
    load_portal_release_pointer_cursor,
    restore_portal_release_pointer_cursor,
)
from .portal_publisher import (
    PortalDeliverySink,
    PortalOutboxMessage,
    PortalPublicationBatch,
    PortalPublicationConfig,
    run_portal_publication_once,
)
from .preflight import ProviderTier
from .stage_orchestrator import StageOrchestrator
from .worker_database import RealtimeProcessDatabase


STATUS_READY = "ACTIVE_UNCONSUMED"
STATUS_LOCKED = "LOCKED_UNCONSUMED"
STATUS_CONSUMED = "CONSUMED"
STATUS_AMBIGUOUS = "AMBIGUOUS_PRIOR_RUN"
RECEIPT_SCHEMA = "advanced-options-p8-consumption-receipt-v1"
LOCK_SCHEMA = "advanced-options-p8-execution-lock-v1"
POSTFLIGHT_ZERO_EFFECTS = (
    "dispatches",
    "deliveries",
    "broker_actions",
    "trades",
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _resolved_package_path(
    repository_root: Path,
    package: Mapping[str, object],
    key: str,
) -> Path:
    root = repository_root.resolve()
    value = package["cleanup"][key]
    path = (root / str(value)).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"P8 {key} escaped the repository root") from error
    if not path.parent.is_dir():
        raise ValueError(f"P8 {key} parent directory is absent")
    return path


class P8CanaryExecutionError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


@dataclass(frozen=True, slots=True)
class P8CanaryExecutionBinding:
    enrollment: AdvancedOptionsEnrollmentPlan
    history: AdvancedOptionsHistoryBinding
    equity_input: AdvancedOptionsEquityInputBinding
    window_id: UUID
    underlying_price_binding: UnderlyingSnapshotBinding | None = None

    def validate(
        self,
        package: Mapping[str, object],
        live_preflight: Mapping[str, object],
    ) -> None:
        if not isinstance(self.enrollment, AdvancedOptionsEnrollmentPlan):
            raise TypeError("P8 execution requires a typed enrollment plan")
        if not isinstance(self.history, AdvancedOptionsHistoryBinding):
            raise TypeError("P8 execution requires a typed history binding")
        if not isinstance(
            self.equity_input,
            AdvancedOptionsEquityInputBinding,
        ):
            raise TypeError(
                "P8 execution requires a typed Equity input binding"
            )
        if not isinstance(
            self.underlying_price_binding,
            UnderlyingSnapshotBinding,
        ):
            raise TypeError(
                "P8 execution requires frozen underlying snapshot prices"
            )
        enrollment = advanced_options_enrollment_plan_payload(
            self.enrollment
        )
        history = advanced_options_history_binding_payload(self.history)
        equity_input = advanced_options_equity_input_binding_payload(
            self.equity_input
        )
        underlying_prices = underlying_snapshot_binding_payload(
            self.underlying_price_binding
        )
        control = self.enrollment.control_plane
        observed = live_preflight["advanced_enrollment"]
        expected_enrollment = {
            "enrollment_id": enrollment["enrollment_id"],
            "enrollment_sha256": enrollment["enrollment_sha256"],
            "control_plane_plan_id": str(control.plan_id),
            "control_plane_plan_sha256": control.plan_sha256,
            "cohort_id": str(control.cohort_id),
            "cohort_sha256": control.cohort_sha256,
            "profile_id": str(control.profile_id),
            "profile_sha256": control.profile_sha256,
            "manifest_id": str(control.manifest_id),
            "manifest_sha256": control.manifest_sha256,
            "slot_set_sha256": control.slot_set_sha256,
            "selected_contracts": [
                row.contract_ticker
                for row in self.enrollment.activity_selection.contracts
            ],
        }
        selected = {
            (row.instrument_id, row.catalog_version_id)
            for row in self.enrollment.activity_selection.contracts
        }
        slot_channels: dict[tuple[int, int], set[MarketChannel]] = {}
        for member in control.slot_set.members:
            if member.purpose is not SlotSetPurpose.PRIMARY:
                raise ValueError("P8 permits primary option slots only")
            slot_channels.setdefault(
                (member.instrument_id, member.catalog_version_id),
                set(),
            ).add(member.channel)
        if (
            expected_enrollment != observed
            or str(control.plan_id)
            != package["orchestration"]["plan_id"]
            or str(control.cohort_id) != package["cohort"]["cohort_id"]
            or history != live_preflight["realized_history_binding"]
            or equity_input
            != live_preflight["realized_equity_input_binding"]
            or underlying_prices
            != live_preflight["realized_underlying_price_binding"]
            or self.history.target_cohort_id != control.cohort_id
            or self.equity_input.target_cohort_id != control.cohort_id
            or self.equity_input.target_options_window_id != self.window_id
            or self.equity_input.source_cohort_id
            != self.enrollment.equity_plan.cohort_id
            or list(control.universe.option_underlyings)
            != package["cohort"]["underlyings"]
            or set(slot_channels) != selected
            or any(
                channels
                != {MarketChannel.TRADE, MarketChannel.QUOTE}
                for channels in slot_channels.values()
            )
            or self.window_id.int == 0
        ):
            raise ValueError(
                "typed P8 enrollment, history or window binding diverged"
            )


class P8BindingCapturePort(Protocol):
    def capture(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> tuple[Mapping[str, object], P8CanaryExecutionBinding]: ...


class P8StoredReadPort(Protocol):
    def enable(
        self,
        *,
        cohort_id: UUID,
        release_id: UUID,
    ) -> Mapping[str, object]: ...

    def disable(
        self,
        *,
        cohort_id: UUID,
        release_id: UUID,
    ) -> None: ...


class P8CanaryLifecyclePort(Protocol):
    def capture_binding(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> tuple[Mapping[str, object], P8CanaryExecutionBinding]: ...

    def enroll(self, binding: P8CanaryExecutionBinding) -> None: ...

    def start_residents(
        self,
        binding: P8CanaryExecutionBinding,
        *,
        checked_at: datetime,
    ) -> tuple[str, ...]: ...

    def run_stages(
        self,
        binding: P8CanaryExecutionBinding,
    ) -> Mapping[str, object]: ...

    def publish(
        self,
        binding: P8CanaryExecutionBinding,
        stage_result: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> Mapping[str, object]: ...

    def enable_stored_reads(
        self,
        binding: P8CanaryExecutionBinding,
        publication: Mapping[str, object],
    ) -> Mapping[str, object]: ...

    def rollback(
        self,
        binding: P8CanaryExecutionBinding,
        publication: Mapping[str, object] | None,
    ) -> None: ...

    def stop_residents(
        self,
        binding: P8CanaryExecutionBinding,
    ) -> None: ...

    def cleanup(
        self,
        binding: P8CanaryExecutionBinding,
    ) -> Mapping[str, object]: ...

    def postflight(self) -> Mapping[str, object]: ...

    def close(self) -> None: ...


class P8ExecutionLock:
    def __init__(self, path: Path, package: Mapping[str, object]) -> None:
        self.path = path.resolve()
        self.package = package

    def acquire(self, *, acquired_at: datetime) -> None:
        value = {
            "schema": LOCK_SCHEMA,
            "state": "LOCKED_PREFLIGHT",
            "package_sha256": self.package["package_sha256"],
            "package_nonce": self.package["package_nonce"],
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
            raise P8CanaryExecutionError(
                "EXECUTION_LOCK_CONFLICT",
                str(self.path),
            ) from error

    def release(self) -> None:
        current = _json(self.path)
        if (
            current.get("package_sha256")
            != self.package["package_sha256"]
            or current.get("package_nonce") != self.package["package_nonce"]
        ):
            raise P8CanaryExecutionError(
                "EXECUTION_LOCK_IDENTITY_DRIFT",
                str(self.path),
            )
        self.path.unlink()


class P8ConsumptionReceipt:
    def __init__(self, path: Path, package: Mapping[str, object]) -> None:
        self.path = path.resolve()
        self.package = package

    def _write(
        self,
        value: Mapping[str, object],
        *,
        exclusive: bool = False,
    ) -> None:
        if exclusive:
            with self.path.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return
        temporary = self.path.with_name(
            f"{self.path.name}.{self.package['package_nonce']}.partial"
        )
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def create(self, *, created_at: datetime) -> None:
        value = {
            "schema": RECEIPT_SCHEMA,
            "state": "EXECUTION_STARTED",
            "package_sha256": self.package["package_sha256"],
            "package_nonce": self.package["package_nonce"],
            "authorization_reference": self.package[
                "authorization_reference"
            ],
            "created_at_utc": _utc(
                created_at,
                "created_at",
            ).isoformat(),
            "history": [],
        }
        try:
            self._write(value, exclusive=True)
        except FileExistsError as error:
            raise P8CanaryExecutionError(
                "CONSUMPTION_RECEIPT_ALREADY_EXISTS",
                str(self.path),
            ) from error

    def read(self) -> dict[str, object]:
        value = _json(self.path)
        if (
            value.get("package_sha256")
            != self.package["package_sha256"]
            or value.get("package_nonce") != self.package["package_nonce"]
        ):
            raise P8CanaryExecutionError(
                "CONSUMPTION_RECEIPT_IDENTITY_DRIFT",
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
        history = list(value["history"])
        history.append({
            "state": state,
            "checked_at_utc": _utc(
                checked_at,
                "checked_at",
            ).isoformat(),
            "detail": dict(detail or {}),
        })
        value.update(state=state, history=history)
        self._write(value)


@dataclass(frozen=True, slots=True)
class P8ExecutionStatus:
    state: str
    detail: str
    lock_present: bool
    receipt: Mapping[str, object] | None

    def as_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "detail": self.detail,
            "lock_present": self.lock_present,
            "receipt": self.receipt,
        }


class _DeniedDeliverySink:
    def deliver(self, message: PortalOutboxMessage):
        raise RuntimeError(
            f"P8 canary delivery is denied for {message.outbox_id}"
        )


class AdvancedOptionsP8LifecyclePort:
    """Concrete composition of enrollment, residents, stages and publication."""

    def __init__(
        self,
        *,
        capture: P8BindingCapturePort,
        enrollment_connection: object,
        resident_launcher: AdvancedOptionsResidentLauncher,
        orchestrator: StageOrchestrator,
        publisher_database: RealtimeProcessDatabase,
        publication_config: PortalPublicationConfig,
        publication_batch: Callable[
            [P8CanaryExecutionBinding, Mapping[str, object]],
            PortalPublicationBatch,
        ],
        publication_observation: Callable[
            [P8CanaryExecutionBinding, PortalReleasePointer, datetime],
            Mapping[str, object],
        ],
        stored_reads: P8StoredReadPort | None = None,
        delivery_sink: PortalDeliverySink | None = None,
        cleanup: Callable[
            [P8CanaryExecutionBinding], Mapping[str, object]
        ] = lambda _binding: {
            "temporary_files": [],
            "processes": [],
            "locks": [],
        },
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if publication_config.dispatch_authorized:
            raise ValueError("P8 publication must deny dispatch")
        if clock is not None and not callable(clock):
            raise TypeError("P8 publication clock is invalid")
        self._capture = capture
        self._enrollment_connection = enrollment_connection
        self._resident_launcher = resident_launcher
        self._orchestrator = orchestrator
        self._publisher_database = publisher_database
        self._publication_config = publication_config
        self._publication_batch = publication_batch
        self._publication_observation = publication_observation
        self._stored_reads = stored_reads
        self._delivery_sink = delivery_sink or _DeniedDeliverySink()
        self._cleanup = cleanup
        self._clock = clock
        self._previous_pointer: PortalReleasePointer | None = None
        self._published_pointer: PortalReleasePointer | None = None
        self._stored_reads_enabled = False
        self._resident_starts = 0
        self._resident_stops = 0
        self._stage_runs = 0
        self._publication_runs = 0

    def capture_binding(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> tuple[Mapping[str, object], P8CanaryExecutionBinding]:
        return self._capture.capture(package, checked_at=checked_at)

    def enroll(self, binding: P8CanaryExecutionBinding) -> None:
        from scripts.run_realtime_v2_options_enrollment import (
            execute_frozen_advanced_options_enrollment,
        )

        execute_frozen_advanced_options_enrollment(
            self._enrollment_connection,
            binding.enrollment,
            binding.history,
            binding.equity_input,
            database_name="stocks_realtime_v2",
            confirmed_database_name="stocks_realtime_v2",
        )

    def start_residents(
        self,
        binding: P8CanaryExecutionBinding,
        *,
        checked_at: datetime,
    ) -> tuple[str, ...]:
        health = self._resident_launcher.start(now=checked_at)
        self._resident_starts += 1
        if set(health) != set(AdvancedOptionsProcessKind):
            raise RuntimeError("P8 resident owner membership changed")
        return tuple(kind.value for kind in AdvancedOptionsProcessKind)

    def run_stages(
        self,
        binding: P8CanaryExecutionBinding,
    ) -> Mapping[str, object]:
        start = self._orchestrator.start(
            binding.enrollment.control_plane.plan_id
        )
        if start.enabled != frozenset(OPTIONS_NON_SERVING_STAGE_ORDER):
            raise RuntimeError("P8 stages_v4 membership changed")
        report = self._orchestrator.run_window(binding.window_id)
        self._stage_runs += 1
        return {
            "status": report.status,
            "window_state": report.window_state,
            "stage_order": list(OPTIONS_NON_SERVING_STAGE_ORDER),
        }

    def _pointer(self, *, for_update: bool = False):
        with self._publisher_database.transaction() as cursor:
            return load_portal_release_pointer_cursor(
                cursor,
                cohort_id=self._publication_config.cohort_id,
                provider_tier=self._publication_config.provider_tier,
                for_update=for_update,
            )

    def publish(
        self,
        binding: P8CanaryExecutionBinding,
        stage_result: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> Mapping[str, object]:
        effective_checked_at = (
            checked_at
            if self._clock is None
            else _utc(self._clock(), "publication clock")
        )
        if effective_checked_at < checked_at:
            raise RuntimeError("P8 publication clock moved backward")
        self._previous_pointer = self._pointer()
        batch = self._publication_batch(binding, stage_result)
        status = run_portal_publication_once(
            self._publisher_database,
            self._publication_config,
            batch,
            self._delivery_sink,
            checked_at=effective_checked_at,
        )
        if any(
            (
                status.claimed,
                status.delivered,
                status.retried,
                status.abandoned,
            )
        ):
            raise RuntimeError("P8 publication attempted delivery")
        self._published_pointer = self._pointer()
        if self._published_pointer is None:
            raise RuntimeError("P8 publication did not create a pointer")
        self._publication_runs += 1
        return self._publication_observation(
            binding,
            self._published_pointer,
            effective_checked_at,
        )

    def enable_stored_reads(
        self,
        binding: P8CanaryExecutionBinding,
        publication: Mapping[str, object],
    ) -> Mapping[str, object]:
        if self._stored_reads is None:
            raise RuntimeError("P8 stored-read port is unavailable")
        release_id = UUID(str(publication["release_id"]))
        result = self._stored_reads.enable(
            cohort_id=binding.enrollment.control_plane.cohort_id,
            release_id=release_id,
        )
        if (
            result.get("cohort_id")
            != str(binding.enrollment.control_plane.cohort_id)
            or result.get("release_id") != str(release_id)
            or result.get("verified") is not True
        ):
            raise RuntimeError(
                "P8 stored reads did not verify the exact cohort release"
            )
        self._stored_reads_enabled = True
        return result

    def rollback(
        self,
        binding: P8CanaryExecutionBinding,
        publication: Mapping[str, object] | None,
    ) -> None:
        if self._stored_reads_enabled and self._stored_reads is not None:
            self._stored_reads.disable(
                cohort_id=binding.enrollment.control_plane.cohort_id,
                release_id=UUID(str(publication["release_id"])),
            )
            self._stored_reads_enabled = False
        if publication is None or self._published_pointer is None:
            return
        with self._publisher_database.transaction() as cursor:
            restore_portal_release_pointer_cursor(
                cursor,
                cohort_id=binding.enrollment.control_plane.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                expected_current_release_id=self._published_pointer.release_id,
                previous=self._previous_pointer,
            )

    def stop_residents(
        self,
        binding: P8CanaryExecutionBinding,
    ) -> None:
        self._resident_launcher.stop()
        self._resident_stops += 1

    def cleanup(
        self,
        binding: P8CanaryExecutionBinding,
    ) -> Mapping[str, object]:
        return self._cleanup(binding)

    def postflight(self) -> Mapping[str, object]:
        return {
            "resident_starts": self._resident_starts,
            "resident_stops": self._resident_stops,
            "stage_runs": self._stage_runs,
            "publication_runs": self._publication_runs,
            "dispatches": 0,
            "deliveries": 0,
            "broker_actions": 0,
            "trades": 0,
        }


class P8CanaryExecutor:
    def __init__(
        self,
        port: P8CanaryLifecyclePort,
        *,
        repository_root: Path,
        qualification_report_path: Path,
        current_source_commit: str,
        expected_package_sha256: str,
        expected_qualification_semantic_sha256: str,
        expected_qualification_file_sha256: str,
        source_files: Sequence[str] = SOURCE_BINDING_PATHS,
    ) -> None:
        self._port = port
        self._repository_root = repository_root.resolve()
        self._qualification_report_path = qualification_report_path.resolve()
        self._current_source_commit = current_source_commit
        self._expected_package_sha256 = expected_package_sha256
        self._expected_qualification_semantic_sha256 = (
            expected_qualification_semantic_sha256
        )
        self._expected_qualification_file_sha256 = (
            expected_qualification_file_sha256
        )
        self._source_files = tuple(source_files)

    def _validate_package(
        self,
        package: Mapping[str, object],
    ) -> Mapping[str, object]:
        return validate_p8_canary_authorization(
            package,
            qualification_report_path=self._qualification_report_path,
            repository_root=self._repository_root,
            current_source_commit=self._current_source_commit,
            expected_package_sha256=self._expected_package_sha256,
            expected_qualification_semantic_sha256=(
                self._expected_qualification_semantic_sha256
            ),
            expected_qualification_file_sha256=(
                self._expected_qualification_file_sha256
            ),
            source_files=self._source_files,
        )

    def _resources(self, package):
        lock_path = _resolved_package_path(
            self._repository_root,
            package,
            "lock_path",
        )
        receipt_path = _resolved_package_path(
            self._repository_root,
            package,
            "consumption_receipt_path",
        )
        return (
            P8ExecutionLock(lock_path, package),
            P8ConsumptionReceipt(receipt_path, package),
        )

    def plan(self, package: Mapping[str, object]) -> dict[str, object]:
        self._validate_package(package)
        return {
            "mode": "PLAN",
            "package_sha256": package["package_sha256"],
            "package_nonce": package["package_nonce"],
            "window": package["window"],
            "v3_authority_classification": package[
                "v3_authority_classification"
            ],
            "operations": [
                "acquire_one_use_lock_and_receipt",
                "capture_and_validate_fresh_live_binding",
                "persist_advanced_enrollment_and_history_binding",
                "start_exact_four_residents",
                "run_post_e_late_canary_stages_v4",
                "publish_after_inert_late_canary_v4_handoff",
                "optionally_enable_exact_cohort_stored_reads",
                "stop_exact_four_residents",
            ],
            "dispatch_delivery_authorized": False,
            "broker_trading_authorized": False,
        }

    def status(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> P8ExecutionStatus:
        self._validate_package(package)
        lock, receipt = self._resources(package)
        if receipt.path.exists():
            value = receipt.read()
            state = str(value["state"])
            return P8ExecutionStatus(
                STATUS_CONSUMED if state == "CONSUMED_CLEAN" else STATUS_AMBIGUOUS,
                f"durable P8 receipt state is {state}",
                lock.path.exists(),
                value,
            )
        if lock.path.exists():
            return P8ExecutionStatus(
                STATUS_LOCKED,
                "execution lock exists without a durable receipt",
                True,
                None,
            )
        package_status = p8_canary_status(
            package,
            now=checked_at,
            prior_run_records=[],
        )
        return P8ExecutionStatus(
            str(package_status["state"]),
            str(package_status["detail"]),
            False,
            None,
        )

    def preflight(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ) -> dict[str, object]:
        self._validate_package(package)
        status = self.status(package, checked_at=checked_at)
        if status.state not in {
            "PREPARED_UNARMED_UNCONSUMED",
            STATUS_READY,
        }:
            raise P8CanaryExecutionError(
                "EXECUTION_NOT_READY",
                status.state,
            )
        return {
            "mode": "PREFLIGHT",
            "status": "PASS_P8_EXECUTOR_PREFLIGHT",
            "execution_status": status.as_dict(),
            "provider_requests": 0,
            "production_writes": 0,
            "service_lifecycle_actions": 0,
            "dispatches": 0,
            "deliveries": 0,
            "broker_actions": 0,
            "trades": 0,
        }

    def execute(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
        stored_reads_authorized: bool = False,
        fault: Callable[[str], None] = lambda _phase: None,
    ) -> dict[str, object]:
        checked = _utc(checked_at, "checked_at")
        start = datetime.fromisoformat(
            str(package["window"]["start_utc"])
        )
        stop = datetime.fromisoformat(
            str(package["window"]["stop_utc"])
        )
        if not start <= checked < stop:
            code = (
                "EARLY_EXECUTION_WINDOW"
                if checked < start
                else "MISSED_EXECUTION_WINDOW"
            )
            raise P8CanaryExecutionError(code, checked.isoformat())
        self.preflight(package, checked_at=checked)
        if stored_reads_authorized and not package["stored_reads"][
            "authority_prepared"
        ]:
            raise P8CanaryExecutionError(
                "STORED_READ_AUTHORITY_NOT_PREPARED"
            )
        lock, receipt = self._resources(package)
        lock.acquire(acquired_at=checked)
        receipt_created = False
        binding = None
        publication = None
        resident_start_attempted = False
        failure: BaseException | None = None
        result: dict[str, object] | None = None
        try:
            receipt.create(created_at=checked)
            receipt_created = True
            fault("AFTER_RECEIPT")
            observation, binding = self._port.capture_binding(
                package,
                checked_at=checked,
            )
            captured_at = _utc(
                datetime.fromisoformat(
                    str(observation["checked_at_utc"])
                ),
                "observation.checked_at_utc",
            )
            if captured_at < checked or not start <= captured_at < stop:
                raise P8CanaryExecutionError(
                    "LIVE_BINDING_OUTSIDE_EXECUTION_WINDOW",
                    captured_at.isoformat(),
                )
            checked = captured_at
            live = validate_p8_live_observation(
                package,
                observation,
                repository_root=self._repository_root,
                now=checked,
                prior_run_records=[],
                execution_context=True,
                stored_reads_authorized=stored_reads_authorized,
            )
            binding.validate(package, live)
            receipt.update(
                "LIVE_BINDING_VALIDATED",
                checked_at=checked,
                detail={
                    "enrollment_sha256": (
                        binding.enrollment.enrollment_sha256
                    ),
                    "history_binding_sha256": (
                        binding.history.binding_sha256
                    ),
                },
            )
            fault("AFTER_LIVE_BINDING")
            self._port.enroll(binding)
            receipt.update("ENROLLED", checked_at=checked)
            fault("AFTER_ENROLLMENT")
            resident_start_attempted = True
            residents = self._port.start_residents(
                binding,
                checked_at=checked,
            )
            if residents != tuple(
                kind.value for kind in AdvancedOptionsProcessKind
            ):
                raise RuntimeError("P8 resident lineup changed")
            receipt.update("RESIDENTS_STARTED", checked_at=checked)
            fault("AFTER_RESIDENT_START")
            stages = self._port.run_stages(binding)
            if (
                stages.get("status") != "FINISHED"
                or stages.get("window_state") != "COMPLETE"
                or stages.get("stage_order")
                != list(OPTIONS_NON_SERVING_STAGE_ORDER)
            ):
                raise RuntimeError(
                    "post-E late-canary P8 stages_v4 did not complete"
                )
            receipt.update("STAGES_COMPLETE", checked_at=checked)
            fault("AFTER_STAGES")
            publication = self._port.publish(
                binding,
                stages,
                checked_at=checked,
            )
            page_gate = validate_p8_page_release_observation(
                package,
                publication,
            )
            receipt.update(
                "RELEASE_PUBLISHED",
                checked_at=checked,
                detail={
                    "release_id": publication["release_id"],
                    "scope_sha256": publication["scope_sha256"],
                },
            )
            fault("AFTER_PUBLICATION")
            stored_read_result = None
            if stored_reads_authorized:
                stored_read_result = self._port.enable_stored_reads(
                    binding,
                    publication,
                )
                fault("AFTER_STORED_READS")
            postflight = dict(self._port.postflight())
            if (
                postflight.get("resident_starts") != 1
                or postflight.get("stage_runs") != 1
                or postflight.get("publication_runs") != 1
                or any(
                    postflight.get(name) != 0
                    for name in POSTFLIGHT_ZERO_EFFECTS
                )
            ):
                raise RuntimeError("P8 postflight effects diverged")
            result = {
                "status": "PASS_P8_CANARY_EXECUTION",
                "package_sha256": package["package_sha256"],
                "package_nonce": package["package_nonce"],
                "enrollment_sha256": binding.enrollment.enrollment_sha256,
                "history_binding_sha256": binding.history.binding_sha256,
                "release_id": publication["release_id"],
                "page_gate": page_gate,
                "stored_reads": stored_read_result,
                "postflight": postflight,
                "v3_authority_classification": package[
                    "v3_authority_classification"
                ],
            }
        except BaseException as error:
            failure = error

        if failure is not None and binding is not None:
            try:
                self._port.rollback(binding, publication)
            except BaseException as rollback_error:
                failure = P8CanaryExecutionError(
                    "ROLLBACK_FAILED",
                    (
                        f"{type(rollback_error).__name__}: "
                        f"{str(rollback_error)[:240]}"
                    ),
                )

        if binding is not None and resident_start_attempted:
            try:
                self._port.stop_residents(binding)
            except BaseException as stop_error:
                failure = P8CanaryExecutionError(
                    "MANDATORY_STOP_FAILED",
                    f"{type(stop_error).__name__}: {str(stop_error)[:240]}",
                )
            else:
                stopped_postflight = dict(self._port.postflight())
                if stopped_postflight.get("resident_stops") != 1:
                    failure = P8CanaryExecutionError(
                        "MANDATORY_STOP_POSTCONDITION_FAILED"
                    )
                elif result is not None:
                    result["postflight"] = stopped_postflight

        cleanup: Mapping[str, object] = {}
        if binding is not None:
            try:
                cleanup = self._port.cleanup(binding)
            except BaseException as cleanup_error:
                failure = P8CanaryExecutionError(
                    "CLEANUP_FAILED",
                    (
                        f"{type(cleanup_error).__name__}: "
                        f"{str(cleanup_error)[:240]}"
                    ),
                )

        close = getattr(self._port, "close", None)
        if callable(close):
            try:
                close()
            except BaseException as close_error:
                failure = P8CanaryExecutionError(
                    "RUNTIME_CLOSE_FAILED",
                    (
                        f"{type(close_error).__name__}: "
                        f"{str(close_error)[:240]}"
                    ),
                )

        if receipt_created:
            final_state = (
                "CONSUMED_CLEAN"
                if failure is None
                else "CONSUMED_ROLLED_BACK"
            )
            receipt.update(
                final_state,
                checked_at=checked,
                detail={
                    "error_type": (
                        None if failure is None else type(failure).__name__
                    ),
                    "cleanup": dict(cleanup),
                },
            )
            lock.release()
        elif lock.path.exists():
            lock.release()

        if failure is not None:
            if isinstance(failure, P8CanaryExecutionError):
                raise failure
            raise P8CanaryExecutionError(
                "EXECUTION_FAILED",
                f"{type(failure).__name__}: {str(failure)[:240]}",
            ) from failure
        if result is None:
            raise AssertionError("P8 execution completed without a result")
        result["cleanup"] = {
            **dict(cleanup),
            "v3_authority_classification": package[
                "v3_authority_classification"
            ],
        }
        return result
