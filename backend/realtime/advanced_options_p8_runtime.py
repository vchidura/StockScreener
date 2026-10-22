"""Repository-owned P8 runtime composition and Docker execution bundle."""
from __future__ import annotations

import json
import os
import signal
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2

from .advanced_options_control_plane import (
    advanced_options_enrollment_plan_payload,
    advanced_options_history_binding_payload,
)
from .advanced_options_p8_canary_authorization import canonical_sha256
from .advanced_options_p8_canary_executor import (
    AdvancedOptionsP8LifecyclePort,
    P8BindingCapturePort,
    P8CanaryExecutionBinding,
    P8StoredReadPort,
)
from .advanced_options_p8_docker import (
    AdvancedOptionsDockerServicePort,
    P8_SERVICE_NAMES,
    isolated_p8_compose_environment,
    validate_p8_service_membership,
)
from .advanced_options_service import (
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentLauncher,
    build_owp3_advanced_options_resident_plan,
)
from .advanced_options_underlying_snapshot import (
    underlying_snapshot_binding_payload,
)
from .portal_projection_repository import PortalReleasePointer
from .portal_publisher import (
    PortalDeliverySink,
    PortalPublicationBatch,
    PortalPublicationConfig,
)
from .preflight import ProviderTier
from .stage_orchestrator import StageOrchestrator
from .worker_database import RealtimeProcessDatabase


UTC = timezone.utc
P8_EXECUTION_BUNDLE_SCHEMA = "advanced-options-p8-execution-bundle-v2"
P8_EXECUTION_BUNDLE_MAX_BYTES = 1_048_576
P8_HEARTBEAT_INTERVAL_SECONDS = 10
P8_EFFECT_DENIALS = {
    "dispatch": False,
    "delivery": False,
    "broker": False,
    "trading": False,
}
P8_STATUS_SCHEMA = "advanced-options-p8-docker-service-status-v1"
P8_STATUS_MAXIMUM_AGE_SECONDS = 20


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _inside(root: Path, path: Path, name: str) -> Path:
    resolved_root = root.resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError(f"P8 {name} escaped the repository root") from error
    return resolved


class P8RuntimeComponentsFactory(Protocol):
    def __call__(
        self,
        *,
        package: Mapping[str, object],
        repository_root: Path,
        stored_reads_authorized: bool,
    ) -> "P8RuntimeComponents": ...


@dataclass(frozen=True, slots=True)
class P8RuntimeComponents:
    capture: P8BindingCapturePort
    enrollment_connection: object
    database: object
    orchestrator: StageOrchestrator
    publisher_database: RealtimeProcessDatabase
    publication_config: PortalPublicationConfig
    publication_batch: Callable[
        [P8CanaryExecutionBinding, Mapping[str, object]],
        PortalPublicationBatch,
    ]
    publication_observation: Callable[
        [P8CanaryExecutionBinding, PortalReleasePointer, datetime],
        Mapping[str, object],
    ]
    stored_reads: P8StoredReadPort | None = None
    delivery_sink: PortalDeliverySink | None = None
    cleanup: Callable[
        [P8CanaryExecutionBinding], Mapping[str, object]
    ] = lambda _binding: {
        "temporary_files": [],
        "processes": [],
        "locks": [],
    }
    close: Callable[[], None] = lambda: None
    clock: Callable[[], datetime] | None = None

    def __post_init__(self) -> None:
        if self.publication_config.dispatch_authorized:
            raise ValueError("P8 runtime components must deny dispatch")
        for value, name in (
            (self.publication_batch, "publication batch"),
            (self.publication_observation, "publication observation"),
            (self.cleanup, "cleanup"),
            (self.close, "close"),
        ):
            if not callable(value):
                raise TypeError(f"P8 {name} callback is invalid")
        if self.clock is not None and not callable(self.clock):
            raise TypeError("P8 runtime clock is invalid")


class P8ExecutionBundleStore:
    def __init__(
        self,
        path: Path,
        *,
        package: Mapping[str, object],
        package_path: Path,
        repository_root: Path,
    ) -> None:
        validate_p8_service_membership(package)
        self.path = _inside(
            repository_root,
            path,
            "execution bundle path",
        )
        self.package_path = _inside(
            repository_root,
            package_path,
            "package path",
        )
        if not self.package_path.is_file():
            raise ValueError("P8 package path is absent")
        if not self.path.parent.is_dir():
            raise ValueError("P8 execution bundle parent is absent")
        self.package = package

    def write(
        self,
        observation: Mapping[str, object],
        binding: P8CanaryExecutionBinding,
        *,
        written_at: datetime,
        production_binding: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        if self.path.exists():
            raise FileExistsError("P8 execution bundle must be new")
        if not isinstance(binding, P8CanaryExecutionBinding):
            raise TypeError("P8 execution bundle requires a typed binding")
        checked = _utc(written_at, "written_at")
        control = binding.enrollment.control_plane
        payload = {
            "schema": P8_EXECUTION_BUNDLE_SCHEMA,
            "package_sha256": self.package["package_sha256"],
            "package_nonce": self.package["package_nonce"],
            "package_path": str(self.package_path),
            "window": dict(self.package["window"]),
            "cohort_id": str(control.cohort_id),
            "plan_id": str(control.plan_id),
            "window_id": str(binding.window_id),
            "written_at_utc": checked.isoformat(),
            "services": [
                {
                    "kind": row["kind"],
                    "owner_name": row["owner_name"],
                    "lease_resource_key": row["lease_resource_key"],
                    "owner_id": str(_owner_ids(self.package)[
                        AdvancedOptionsProcessKind(str(row["kind"]))
                    ]),
                    "service_name": P8_SERVICE_NAMES[
                        AdvancedOptionsProcessKind(str(row["kind"]))
                    ],
                }
                for row in self.package["owners"]["processes"]
            ],
            "enrollment": advanced_options_enrollment_plan_payload(
                binding.enrollment
            ),
            "history_binding": advanced_options_history_binding_payload(
                binding.history
            ),
            "underlying_price_binding": (
                underlying_snapshot_binding_payload(
                    binding.underlying_price_binding
                )
            ),
            "production_binding": (
                None
                if production_binding is None
                else dict(production_binding)
            ),
            "live_observation_sha256": canonical_sha256(observation),
            "effect_authorizations": dict(P8_EFFECT_DENIALS),
        }
        payload["bundle_sha256"] = canonical_sha256(payload)
        encoded = (
            json.dumps(payload, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        if len(encoded) > P8_EXECUTION_BUNDLE_MAX_BYTES:
            raise ValueError("P8 execution bundle exceeded its byte ceiling")
        with self.path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        return payload

    def load(
        self,
        *,
        expected_kind: AdvancedOptionsProcessKind | None = None,
        checked_at: datetime | None = None,
    ) -> dict[str, object]:
        return validate_p8_execution_bundle(
            self.package_path,
            self.path,
            expected_kind=expected_kind,
            checked_at=checked_at,
        )


def validate_p8_execution_bundle(
    package_path: Path,
    bundle_path: Path,
    *,
    expected_kind: AdvancedOptionsProcessKind | None = None,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    package = _json(package_path.resolve())
    bundle_file = bundle_path.resolve()
    if (
        not bundle_file.is_file()
        or bundle_file.stat().st_size > P8_EXECUTION_BUNDLE_MAX_BYTES
    ):
        raise ValueError("P8 execution bundle is absent or oversized")
    bundle = _json(bundle_file)
    digest = bundle.get("bundle_sha256")
    unsigned = dict(bundle)
    unsigned.pop("bundle_sha256", None)
    services = bundle.get("services")
    if (
        bundle.get("schema") != P8_EXECUTION_BUNDLE_SCHEMA
        or digest != canonical_sha256(unsigned)
        or bundle.get("package_sha256") != package.get("package_sha256")
        or bundle.get("package_nonce") != package.get("package_nonce")
        or bundle.get("window") != package.get("window")
        or bundle.get("cohort_id") != package.get("cohort", {}).get(
            "cohort_id"
        )
        or bundle.get("plan_id") != package.get("orchestration", {}).get(
            "plan_id"
        )
        or bundle.get("effect_authorizations") != P8_EFFECT_DENIALS
        or not isinstance(services, list)
        or [row.get("kind") for row in services] != [
            kind.value for kind in AdvancedOptionsProcessKind
        ]
        or [row.get("service_name") for row in services] != [
            P8_SERVICE_NAMES[kind] for kind in AdvancedOptionsProcessKind
        ]
        or [row.get("owner_id") for row in services] != [
            str(_owner_ids(package)[kind])
            for kind in AdvancedOptionsProcessKind
        ]
    ):
        raise ValueError("P8 execution bundle identity changed")
    validate_p8_service_membership(package)
    if expected_kind is not None and (
        not isinstance(expected_kind, AdvancedOptionsProcessKind)
        or expected_kind.value not in {
            str(row.get("kind")) for row in services
        }
    ):
        raise ValueError("P8 execution bundle worker kind changed")
    if checked_at is not None:
        checked = _utc(checked_at, "checked_at")
        start = datetime.fromisoformat(str(bundle["window"]["start_utc"]))
        stop = datetime.fromisoformat(str(bundle["window"]["stop_utc"]))
        if not start <= checked < stop:
            raise ValueError("P8 Docker service is outside its exact window")
    return bundle


class _BundleCapture:
    def __init__(
        self,
        capture: P8BindingCapturePort,
        store: P8ExecutionBundleStore,
    ) -> None:
        self._capture = capture
        self._store = store

    def capture(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ):
        observation, binding = self._capture.capture(
            package,
            checked_at=checked_at,
        )
        self._store.write(
            observation,
            binding,
            written_at=checked_at,
            production_binding=getattr(
                self._capture,
                "production_binding_payload",
                None,
            ),
        )
        return observation, binding


class P8HeartbeatResidentLauncher:
    """Keep all four database leases healthy until mandatory stop."""

    def __init__(
        self,
        launcher: AdvancedOptionsResidentLauncher,
        bundle: P8ExecutionBundleStore,
        *,
        interval_seconds: int = P8_HEARTBEAT_INTERVAL_SECONDS,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (
            type(interval_seconds) is not int
            or interval_seconds <= 0
            or not callable(clock)
        ):
            raise ValueError("P8 heartbeat launcher configuration is invalid")
        self._launcher = launcher
        self._bundle = bundle
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure: BaseException | None = None

    def start(self, *, now: datetime):
        self._bundle.load(checked_at=now)
        health = self._launcher.start(now=now)

        def heartbeat() -> None:
            while not self._stop.wait(self._interval_seconds):
                try:
                    self._launcher.tick(now=self._clock())
                except BaseException as error:
                    self._failure = error
                    self._stop.set()

        self._thread = threading.Thread(
            target=heartbeat,
            name="advanced-options-p8-docker-heartbeat",
            daemon=True,
        )
        self._thread.start()
        return health

    def stop(self) -> None:
        self._stop.set()
        failure = self._failure
        if self._thread is not None:
            self._thread.join(timeout=self._interval_seconds + 1)
            if self._thread.is_alive():
                failure = RuntimeError(
                    "P8 resident heartbeat did not stop"
                )
        try:
            self._launcher.stop()
        except BaseException:
            raise
        if failure is not None:
            raise RuntimeError(
                "P8 resident heartbeat failed"
            ) from failure


class _ClosingLifecyclePort:
    def __init__(
        self,
        port: AdvancedOptionsP8LifecyclePort,
        close: Callable[[], None],
    ) -> None:
        self._port = port
        self._close = close
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._port, name)

    def cleanup(self, binding: P8CanaryExecutionBinding):
        try:
            return self._port.cleanup(binding)
        finally:
            self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()


def _owner_ids(package: Mapping[str, object]):
    nonce = UUID(str(package["package_nonce"]))
    return {
        kind: uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-options-p8-owner:"
                f"{nonce}:{kind.value}"
            ),
        )
        for kind in AdvancedOptionsProcessKind
    }


def _default_components_factory(
    *,
    package: Mapping[str, object],
    repository_root: Path,
    stored_reads_authorized: bool,
) -> P8RuntimeComponents:
    class Deferred:
        def __init__(self):
            self._components = None

        def get(self):
            if self._components is None:
                from .advanced_options_p8_production import (
                    build_advanced_options_p8_production_components,
                )

                self._components = (
                    build_advanced_options_p8_production_components(
                        package=package,
                        repository_root=repository_root,
                        stored_reads_authorized=stored_reads_authorized,
                    )
                )
            return self._components

        def close(self):
            if self._components is not None:
                self._components.close()

    deferred = Deferred()

    class Capture:
        def capture(self, *args, **kwargs):
            return deferred.get().capture.capture(*args, **kwargs)

        @property
        def production_binding_payload(self):
            return getattr(
                deferred.get().capture,
                "production_binding_payload",
                None,
            )

    class EnrollmentConnection:
        def cursor(self, *args, **kwargs):
            return deferred.get().enrollment_connection.cursor(
                *args,
                **kwargs,
            )

        def commit(self):
            return deferred.get().enrollment_connection.commit()

        def rollback(self):
            return deferred.get().enrollment_connection.rollback()

    class Database:
        def transaction(self, *args, **kwargs):
            return deferred.get().database.transaction(*args, **kwargs)

    class Orchestrator:
        def start(self, *args, **kwargs):
            return deferred.get().orchestrator.start(*args, **kwargs)

        def run_window(self, *args, **kwargs):
            return deferred.get().orchestrator.run_window(*args, **kwargs)

    class PublisherDatabase(RealtimeProcessDatabase):
        @property
        def process_plan(self):
            return deferred.get().publisher_database.process_plan

        def transaction(self, *args, **kwargs):
            return deferred.get().publisher_database.transaction(
                *args,
                **kwargs,
            )

    class StoredReads:
        def _port(self):
            port = deferred.get().stored_reads
            if port is None:
                raise RuntimeError(
                    "P8 stored-read persistence is unavailable"
                )
            return port

        def enable(self, **kwargs):
            return self._port().enable(**kwargs)

        def disable(self, **kwargs):
            return self._port().disable(**kwargs)

    publication_config = PortalPublicationConfig(
        cohort_id=UUID(str(package["cohort"]["cohort_id"])),
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        owner_id=uuid5(
            NAMESPACE_URL,
            f"p8-publisher:{package['package_nonce']}",
        ),
        owner_epoch=1,
        dispatch_authorized=False,
    )
    return P8RuntimeComponents(
        capture=Capture(),
        enrollment_connection=EnrollmentConnection(),
        database=Database(),
        orchestrator=Orchestrator(),
        publisher_database=object.__new__(PublisherDatabase),
        publication_config=publication_config,
        publication_batch=lambda *args, **kwargs: (
            deferred.get().publication_batch(*args, **kwargs)
        ),
        publication_observation=lambda *args, **kwargs: (
            deferred.get().publication_observation(*args, **kwargs)
        ),
        stored_reads=StoredReads(),
        cleanup=lambda binding: deferred.get().cleanup(binding),
        close=deferred.close,
        clock=lambda: datetime.now(UTC),
    )


def build_advanced_options_p8_runtime(
    *,
    package: Mapping[str, object],
    package_path: Path,
    repository_root: Path,
    components_factory: P8RuntimeComponentsFactory | None = None,
    docker_executable: Path | None = None,
    environment: Mapping[str, str] | None = None,
    command_runner=None,
    stored_reads_authorized: bool = False,
):
    """Compose the exact repository runtime; no external module is loadable."""
    if type(stored_reads_authorized) is not bool:
        raise TypeError("P8 stored-read authority must be boolean")
    validate_p8_service_membership(package)
    root = repository_root.resolve()
    bundle_path = _inside(
        root,
        (
            root / str(package["cleanup"]["lock_path"])
        ).with_suffix(".execution-bundle.json"),
        "execution bundle path",
    )
    bundle = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=root,
    )
    if bundle.path.exists():
        raise FileExistsError(
            "P8 execution bundle already exists; execution is ambiguous"
        )
    source_environment = dict(os.environ if environment is None else environment)
    executable_value = docker_executable or Path(
        source_environment.get(
            "REALTIME_V2_DOCKER_EXECUTABLE",
            "docker.exe",
        )
    )
    if not executable_value.is_absolute():
        raise ValueError("REALTIME_V2_DOCKER_EXECUTABLE must be absolute")
    compose_environment = isolated_p8_compose_environment(
        source_environment,
        backend_image=source_environment.get(
            "REALTIME_V2_BACKEND_IMAGE",
            "",
        ),
        secret_directory=Path(source_environment.get(
            "REALTIME_V2_DOCKER_SECRET_DIR",
            root / "backend" / ".realtime-canary" / "v2-docker-secrets",
        )),
        package_path=package_path,
        bundle_path=bundle.path,
    )
    processes = AdvancedOptionsDockerServicePort(
        package=package,
        repository_root=root,
        docker_executable=executable_value,
        environment=compose_environment,
        runner=command_runner,
    )
    plan = build_owp3_advanced_options_resident_plan(
        plan_id=UUID(str(package["orchestration"]["plan_id"])),
        cohort_id=UUID(str(package["cohort"]["cohort_id"])),
        session_date=date.fromisoformat(
            str(package["window"]["session_date"])
        ),
    )
    if [
        {
            "kind": row.kind.value,
            "owner_name": row.owner_name,
            "lease_resource_key": row.lease_resource_key,
        }
        for row in plan.processes
    ] != [
        {
            key: row[key]
            for key in ("kind", "owner_name", "lease_resource_key")
        }
        for row in package["owners"]["processes"]
    ]:
        raise ValueError("P8 resident plan and package owners diverged")
    components = (components_factory or _default_components_factory)(
        package=package,
        repository_root=root,
        stored_reads_authorized=stored_reads_authorized,
    )
    try:
        launcher = P8HeartbeatResidentLauncher(
            AdvancedOptionsResidentLauncher(
                plan,
                components.database,
                processes,
                owner_ids=_owner_ids(package),
                cleanup_host_artifacts=True,
            ),
            bundle,
        )
        port = AdvancedOptionsP8LifecyclePort(
            capture=_BundleCapture(components.capture, bundle),
            enrollment_connection=components.enrollment_connection,
            resident_launcher=launcher,
            orchestrator=components.orchestrator,
            publisher_database=components.publisher_database,
            publication_config=components.publication_config,
            publication_batch=components.publication_batch,
            publication_observation=components.publication_observation,
            stored_reads=components.stored_reads,
            delivery_sink=components.delivery_sink,
            cleanup=components.cleanup,
            clock=components.clock,
        )
    except Exception:
        components.close()
        raise
    return _ClosingLifecyclePort(port, components.close)


def validate_p8_docker_service_environment(
    kind: AdvancedOptionsProcessKind,
    environment: Mapping[str, str],
) -> None:
    if not isinstance(kind, AdvancedOptionsProcessKind):
        raise TypeError("P8 Docker service kind is invalid")
    if not str(environment.get("REALTIME_V2_APP_DSN", "")).strip():
        raise RuntimeError("P8 Docker service app DSN is unavailable")
    provider = str(environment.get("POLYGON_API_KEY", "")).strip()
    if kind is AdvancedOptionsProcessKind.INGRESS:
        if not provider:
            raise RuntimeError("P8 ingress provider credential is unavailable")
    elif provider:
        raise RuntimeError("P8 non-ingress service received a provider credential")
    forbidden = [
        name
        for name, value in environment.items()
        if value
        and any(
            token in name.upper()
            for token in ("BROKER", "DELIVERY", "TRADING", "EXECUTION")
        )
    ]
    if forbidden:
        raise RuntimeError(
            "P8 Docker service received forbidden authority: "
            + ",".join(sorted(forbidden))
        )


def _p8_status_path(
    kind: AdvancedOptionsProcessKind,
    environment: Mapping[str, str],
) -> Path:
    directory = Path(environment.get(
        "REALTIME_V2_P8_STATUS_DIR",
        "/var/lib/realtime-v2-reports/p8",
    ))
    return directory / f"{kind.value.casefold().replace('_', '-')}.json"


def _write_service_status(
    path: Path,
    *,
    kind: AdvancedOptionsProcessKind,
    bundle: Mapping[str, object],
    observed_at: datetime,
    state: str,
    owner_epoch: int | None = None,
    lease_until: datetime | None = None,
    role_evidence: Mapping[str, object] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": P8_STATUS_SCHEMA,
        "kind": kind.value,
        "service_name": P8_SERVICE_NAMES[kind],
        "package_sha256": bundle["package_sha256"],
        "package_nonce": bundle["package_nonce"],
        "bundle_sha256": bundle["bundle_sha256"],
        "window_id": bundle["window_id"],
        "observed_at_utc": _utc(
            observed_at,
            "observed_at",
        ).isoformat(),
        "state": state,
        "owner_epoch": owner_epoch,
        "lease_until_utc": (
            None
            if lease_until is None
            else _utc(lease_until, "lease_until").isoformat()
        ),
        "role_evidence": (
            None if role_evidence is None else dict(role_evidence)
        ),
        "effects": dict(P8_EFFECT_DENIALS),
    }
    temporary = path.with_suffix(".partial")
    with temporary.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def run_p8_docker_service(
    kind: AdvancedOptionsProcessKind,
    package_path: Path,
    bundle_path: Path,
    *,
    environment: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    wait: Callable[[float], None] = time.sleep,
    stop_event: threading.Event | None = None,
    role_runner: Callable[..., Mapping[str, object]] | None = None,
) -> int:
    """Run one real bounded role while continuously proving lease ownership."""
    service_environment = dict(
        os.environ if environment is None else environment
    )
    validate_p8_docker_service_environment(kind, service_environment)
    bundle = validate_p8_execution_bundle(
        package_path,
        bundle_path,
        expected_kind=kind,
        checked_at=clock(),
    )
    package = _json(package_path.resolve())
    stop = stop_event or threading.Event()
    if stop_event is None:
        signal.signal(signal.SIGINT, lambda *_args: stop.set())
        signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    status_path = _p8_status_path(kind, service_environment)
    window_stop = datetime.fromisoformat(str(bundle["window"]["stop_utc"]))
    service = next(
        row for row in bundle["services"] if row["kind"] == kind.value
    )
    if role_runner is None:
        from .advanced_options_p8_production import run_p8_production_role

        role_runner = run_p8_production_role
    role_result: dict[str, object] = {}
    role_failure: list[BaseException] = []

    def run_role() -> None:
        try:
            result = role_runner(
                kind=kind,
                package=package,
                bundle=bundle,
                environment=service_environment,
                stop_event=stop,
            )
            if not isinstance(result, Mapping):
                raise TypeError("P8 Docker role evidence must be an object")
            role_result.update(result)
        except BaseException as error:
            role_failure.append(error)
            stop.set()

    role_thread = threading.Thread(
        target=run_role,
        name=f"advanced-options-p8-{kind.value.casefold()}-role",
        daemon=True,
    )
    connection = psycopg2.connect(
        service_environment["REALTIME_V2_APP_DSN"],
        application_name=f"advanced_options_p8_{kind.value.casefold()}",
        options="-c timezone=UTC",
    )
    try:
        role_thread.start()
        while not stop.is_set():
            observed = _utc(clock(), "clock")
            if observed >= window_stop:
                stop.set()
                break
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT owner_id::text,owner_epoch,lease_until "
                        "FROM rt.worker_leases WHERE resource_key=%s",
                        (service["lease_resource_key"],),
                    )
                    lease = cursor.fetchone()
            if (
                lease is None
                or str(lease[0]) != service["owner_id"]
                or int(lease[1]) < 1
                or lease[2] <= observed
            ):
                raise RuntimeError(
                    f"P8 {kind.value} lease ownership changed"
                )
            _write_service_status(
                status_path,
                kind=kind,
                bundle=bundle,
                observed_at=observed,
                state=(
                    "ROLE_COMPLETE"
                    if not role_thread.is_alive()
                    else "ROLE_RUNNING"
                ),
                owner_epoch=int(lease[1]),
                lease_until=lease[2],
                role_evidence=role_result or None,
            )
            if role_failure:
                raise RuntimeError(
                    f"P8 {kind.value} role failed"
                ) from role_failure[0]
            stop.wait(min(
                5.0,
                max(0.0, (window_stop - observed).total_seconds()),
            ))
        role_thread.join(timeout=10)
        if role_thread.is_alive():
            raise RuntimeError(
                f"P8 {kind.value} role did not stop within its bound"
            )
        if role_failure:
            raise RuntimeError(
                f"P8 {kind.value} role failed"
            ) from role_failure[0]
        if (
            role_result.get("status") != "COMPLETE"
            or role_result.get("kind") != kind.value
            or role_result.get("effects") != P8_EFFECT_DENIALS
        ):
            raise RuntimeError(
                f"P8 {kind.value} role evidence is incomplete"
            )
        _write_service_status(
            status_path,
            kind=kind,
            bundle=bundle,
            observed_at=_utc(clock(), "clock"),
            state="STOPPED",
            role_evidence=role_result,
        )
    finally:
        connection.close()
    return 0


def check_p8_docker_service_health(
    kind: AdvancedOptionsProcessKind,
    package_path: Path,
    bundle_path: Path,
    *,
    environment: Mapping[str, str] | None = None,
    checked_at: datetime | None = None,
) -> Mapping[str, object]:
    service_environment = dict(
        os.environ if environment is None else environment
    )
    checked = _utc(
        checked_at or datetime.now(UTC),
        "checked_at",
    )
    bundle = validate_p8_execution_bundle(
        package_path,
        bundle_path,
        expected_kind=kind,
        checked_at=checked,
    )
    status = _json(_p8_status_path(kind, service_environment))
    observed = datetime.fromisoformat(str(status.get("observed_at_utc")))
    if (
        status.get("schema") != P8_STATUS_SCHEMA
        or status.get("kind") != kind.value
        or status.get("service_name") != P8_SERVICE_NAMES[kind]
        or status.get("package_sha256") != bundle["package_sha256"]
        or status.get("package_nonce") != bundle["package_nonce"]
        or status.get("bundle_sha256") != bundle["bundle_sha256"]
        or status.get("window_id") != bundle["window_id"]
        or status.get("state") not in {"ROLE_RUNNING", "ROLE_COMPLETE"}
        or type(status.get("owner_epoch")) is not int
        or status["owner_epoch"] < 1
        or datetime.fromisoformat(
            str(status.get("lease_until_utc"))
        ) <= checked
        or status.get("effects") != P8_EFFECT_DENIALS
        or observed > checked
        or (
            checked - observed
        ).total_seconds() > P8_STATUS_MAXIMUM_AGE_SECONDS
    ):
        raise RuntimeError("P8 Docker service health identity changed")
    return {
        "status": "READY",
        "kind": kind.value,
        "service_name": P8_SERVICE_NAMES[kind],
        "observed_at_utc": observed.isoformat(),
        "state": status["state"],
        "role_evidence": status.get("role_evidence"),
        "effects": dict(P8_EFFECT_DENIALS),
    }
