from __future__ import annotations

import copy
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.advanced_options_p8_canary_authorization import (
    canonical_sha256,
)
from realtime.advanced_options_p8_docker import (
    DOCKER_HOST,
    P8_COMPOSE_PROFILE,
    P8_COMPOSE_PROJECT,
    P8_COMPONENT_NAMES,
    P8_SERVICE_NAMES,
    AdvancedOptionsDockerServicePort,
    P8DockerServiceHandle,
    isolated_p8_compose_environment,
    p8_container_command,
    p8_health_command,
    validate_p8_service_membership,
)
from realtime.advanced_options_p8_runtime import (
    P8_EFFECT_DENIALS,
    P8_EXECUTION_BUNDLE_SCHEMA,
    P8_STATUS_SCHEMA,
    P8HeartbeatResidentLauncher,
    P8RuntimeComponents,
    build_advanced_options_p8_runtime,
    check_p8_docker_service_health,
    run_p8_docker_service,
    validate_p8_docker_service_environment,
    validate_p8_execution_bundle,
)
import realtime.advanced_options_p8_runtime as runtime_module
import realtime.advanced_options_p8_roles as roles_module
from realtime.advanced_options_service import (
    AdvancedOptionsProcessKind,
    AdvancedOptionsProcessSpec,
)
from realtime.advanced_gateway import AdvancedGateway, GatewayConfiguration
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.portal_publisher import PortalPublicationConfig
from realtime.preflight import ProviderTier
from realtime.retention_lifecycle_docker import CommandResult


UTC = timezone.utc
START = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
STOP = START + timedelta(minutes=10)
CONTAINER_ID = "a" * 64
ROOT = Path(__file__).resolve().parents[3]


def _package():
    cohort_id = "8bce9685-d379-5bdd-8aa5-b4990844380d"
    session_date = START.date().isoformat()
    prefix = f"advanced-options:{cohort_id}:{session_date}"
    return {
        "package_sha256": "1" * 64,
        "package_nonce": "9c05aeab-e50d-4504-b7dc-ae55e85f8813",
        "window": {
            "session_date": session_date,
            "start_utc": START.isoformat(),
            "stop_utc": STOP.isoformat(),
        },
        "cohort": {"cohort_id": cohort_id},
        "orchestration": {
            "plan_id": "2fef884f-18a1-5567-a319-fc4d5c558ec6",
        },
        "owners": {
            "processes": [
                {
                    "kind": "INGRESS",
                    "owner_name": "advanced-options-ingress-v1",
                    "lease_resource_key": "massive:options:websocket",
                },
                {
                    "kind": "MARKET_STATE",
                    "owner_name": "market-state-materializer-v1",
                    "lease_resource_key": f"{prefix}:market-state",
                },
                {
                    "kind": "FLOW",
                    "owner_name": "option_flow_stream_v1",
                    "lease_resource_key": f"{prefix}:flow",
                },
                {
                    "kind": "WINDOW",
                    "owner_name": "advanced-options-window-v1",
                    "lease_resource_key": f"{prefix}:window",
                },
            ],
        },
    }


def _write_bundle(tmp_path, package=None):
    package = package or _package()
    package_path = tmp_path / "package.json"
    bundle_path = tmp_path / "bundle.json"
    package_path.write_text(
        json.dumps(package, sort_keys=True),
        encoding="utf-8",
    )
    body = {
        "schema": P8_EXECUTION_BUNDLE_SCHEMA,
        "package_sha256": package["package_sha256"],
        "package_nonce": package["package_nonce"],
        "package_path": str(package_path),
        "window": package["window"],
        "cohort_id": package["cohort"]["cohort_id"],
        "plan_id": package["orchestration"]["plan_id"],
        "window_id": "11111111-2222-3333-4444-555555555555",
        "written_at_utc": START.isoformat(),
        "services": [
            {
                **row,
                "owner_id": str(uuid5(
                    NAMESPACE_URL,
                    (
                        "stockscreener:advanced-options-p8-owner:"
                        f"{package['package_nonce']}:{row['kind']}"
                    ),
                )),
                "service_name": P8_SERVICE_NAMES[
                    AdvancedOptionsProcessKind(row["kind"])
                ],
            }
            for row in package["owners"]["processes"]
        ],
        "enrollment": {},
        "history_binding": {},
        "live_observation_sha256": "2" * 64,
        "effect_authorizations": dict(P8_EFFECT_DENIALS),
    }
    body["bundle_sha256"] = canonical_sha256(body)
    bundle_path.write_text(
        json.dumps(body, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return package_path, bundle_path, body


def _service_block(text, service):
    marker = f"  {service}:\n"
    start = text.index(marker)
    lines = text[start:].splitlines(keepends=True)
    block = [lines[0]]
    for line in lines[1:]:
        if (
            line.startswith("  ")
            and not line.startswith("    ")
            and line.strip().endswith(":")
        ):
            break
        block.append(line)
    return "".join(block)


def test_compose_has_exact_four_p8_services_commands_and_isolated_secrets():
    text = (ROOT / "docker-compose.realtime-v2.yml").read_text(
        encoding="utf-8"
    )
    assert set(P8_SERVICE_NAMES) == set(AdvancedOptionsProcessKind)
    assert len(set(P8_SERVICE_NAMES.values())) == 4
    for kind in AdvancedOptionsProcessKind:
        block = _service_block(text, P8_SERVICE_NAMES[kind])
        assert f'profiles: ["{P8_COMPOSE_PROFILE}"]' in block
        assert 'restart: "no"' in block
        assert "- --docker-service-kind" in block
        assert f"- {kind.value}" in block
        assert "- --docker-health-kind" in block
        assert "- /run/advanced-options-p8/package.json" in block
        assert "- /run/advanced-options-p8/execution-bundle.json" in block
        assert "REALTIME_V2_OPTIONS_PIPELINE: MASSIVE_ADVANCED" in block
        assert "REALTIME_V2_EQUITY_PIPELINE: DISABLED" in block
        assert "- realtime_v2_app_dsn" in block
        assert (
            "- realtime_v2_provider_key" in block
        ) is (kind is AdvancedOptionsProcessKind.INGRESS)
        assert tuple(P8_COMPONENT_NAMES) == tuple(
            AdvancedOptionsProcessKind
        )
        assert p8_container_command(kind)[0:3] == (
            "python",
            "-B",
            "scripts/run_realtime_v2_container.py",
        )
        assert p8_health_command(kind)[0:3] == (
            "python",
            "-B",
            "scripts/run_advanced_options_p8_canary.py",
        )


def test_package_membership_and_compose_environment_are_exact(tmp_path):
    package = _package()
    validate_p8_service_membership(package)
    package_path, bundle_path, _ = _write_bundle(tmp_path, package)
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "realtime_v2_app_dsn").write_text(
        "fixture",
        encoding="utf-8",
    )
    (secrets / "realtime_v2_provider_key").write_text(
        "fixture",
        encoding="utf-8",
    )
    environment = isolated_p8_compose_environment(
        {
            "PATH": "retained",
            "POLYGON_API_KEY": "must-not-leak",
            "REALTIME_V2_APP_DSN": "must-not-leak",
            "BROKER_API_KEY": "must-not-leak",
        },
        backend_image="backend@sha256:" + "a" * 64,
        secret_directory=secrets,
        package_path=package_path,
        bundle_path=bundle_path,
    )
    assert environment == {
        "PATH": "retained",
        "DOCKER_HOST": DOCKER_HOST,
        "REALTIME_V2_BACKEND_IMAGE": "backend@sha256:" + "a" * 64,
        "REALTIME_V2_DOCKER_SECRET_DIR": str(secrets.resolve()),
        "REALTIME_V2_P8_PACKAGE_PATH": str(package_path.resolve()),
        "REALTIME_V2_P8_BUNDLE_PATH": str(bundle_path.resolve()),
    }
    changed = copy.deepcopy(package)
    changed["owners"]["processes"].pop()
    with pytest.raises(ValueError, match="exactly four"):
        validate_p8_service_membership(changed)


def test_compose_environment_accepts_local_immutable_image_id(tmp_path):
    package = _package()
    package_path, bundle_path, _ = _write_bundle(tmp_path, package)
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "realtime_v2_app_dsn").write_text(
        "fixture",
        encoding="utf-8",
    )
    (secrets / "realtime_v2_provider_key").write_text(
        "fixture",
        encoding="utf-8",
    )

    environment = isolated_p8_compose_environment(
        {},
        backend_image="sha256:" + "d" * 64,
        secret_directory=secrets,
        package_path=package_path,
        bundle_path=bundle_path,
    )

    assert environment["REALTIME_V2_BACKEND_IMAGE"] == (
        "sha256:" + "d" * 64
    )


@pytest.mark.parametrize(
    "mutation, match",
    (
        (
            lambda package, bundle: bundle.update(package_sha256="3" * 64),
            "identity",
        ),
        (
            lambda package, bundle: bundle["window"].update(
                stop_utc=(STOP + timedelta(minutes=1)).isoformat()
            ),
            "identity",
        ),
        (
            lambda package, bundle: bundle["services"].pop(),
            "identity",
        ),
    ),
)
def test_bundle_refuses_wrong_package_window_or_membership(
    tmp_path,
    mutation,
    match,
):
    package_path, bundle_path, bundle = _write_bundle(tmp_path)
    mutation(_package(), bundle)
    unsigned = dict(bundle)
    unsigned.pop("bundle_sha256", None)
    bundle["bundle_sha256"] = canonical_sha256(unsigned)
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    with pytest.raises(ValueError, match=match):
        validate_p8_execution_bundle(
            package_path,
            bundle_path,
            checked_at=START + timedelta(minutes=2),
        )


def test_bundle_refuses_wrong_kind_and_outside_window(tmp_path):
    package_path, bundle_path, _ = _write_bundle(tmp_path)
    with pytest.raises((TypeError, ValueError)):
        validate_p8_execution_bundle(
            package_path,
            bundle_path,
            expected_kind="INGRESS",
            checked_at=START + timedelta(minutes=2),
        )
    with pytest.raises(ValueError, match="outside"):
        validate_p8_execution_bundle(
            package_path,
            bundle_path,
            expected_kind=AdvancedOptionsProcessKind.INGRESS,
            checked_at=STOP,
        )


def test_service_environment_denies_provider_leak_and_execution_authority():
    validate_p8_docker_service_environment(
        AdvancedOptionsProcessKind.INGRESS,
        {
            "REALTIME_V2_APP_DSN": "fixture",
            "POLYGON_API_KEY": "fixture",
        },
    )
    with pytest.raises(RuntimeError, match="provider credential"):
        validate_p8_docker_service_environment(
            AdvancedOptionsProcessKind.FLOW,
            {
                "REALTIME_V2_APP_DSN": "fixture",
                "POLYGON_API_KEY": "leaked",
            },
        )
    with pytest.raises(RuntimeError, match="forbidden authority"):
        validate_p8_docker_service_environment(
            AdvancedOptionsProcessKind.WINDOW,
            {
                "REALTIME_V2_APP_DSN": "fixture",
                "BROKER_EXECUTION_ENABLED": "1",
            },
        )


def test_health_requires_fresh_lease_identity_and_zero_effects(tmp_path):
    package_path, bundle_path, bundle = _write_bundle(tmp_path)
    status_dir = tmp_path / "status"
    status_dir.mkdir()
    kind = AdvancedOptionsProcessKind.FLOW
    checked = START + timedelta(minutes=2)
    status_path = status_dir / "flow.json"
    status = {
        "schema": P8_STATUS_SCHEMA,
        "kind": kind.value,
        "service_name": P8_SERVICE_NAMES[kind],
        "package_sha256": bundle["package_sha256"],
        "package_nonce": bundle["package_nonce"],
        "bundle_sha256": bundle["bundle_sha256"],
        "window_id": bundle["window_id"],
        "observed_at_utc": checked.isoformat(),
        "state": "ROLE_RUNNING",
        "owner_epoch": 1,
        "lease_until_utc": (
            checked + timedelta(seconds=30)
        ).isoformat(),
        "role_evidence": None,
        "effects": dict(P8_EFFECT_DENIALS),
    }
    status_path.write_text(json.dumps(status), encoding="utf-8")
    environment = {"REALTIME_V2_P8_STATUS_DIR": str(status_dir)}

    result = check_p8_docker_service_health(
        kind,
        package_path,
        bundle_path,
        environment=environment,
        checked_at=checked,
    )

    assert result["status"] == "READY"
    assert result["effects"] == P8_EFFECT_DENIALS
    status["effects"]["broker"] = True
    status_path.write_text(json.dumps(status), encoding="utf-8")
    with pytest.raises(RuntimeError, match="health identity"):
        check_p8_docker_service_health(
            kind,
            package_path,
            bundle_path,
            environment=environment,
            checked_at=checked,
        )


class _LeaseCursor:
    def execute(self, statement, _parameters=()):
        assert statement.startswith(
            "SELECT owner_id::text,owner_epoch,lease_until"
        )

    @staticmethod
    def fetchone():
        return (
            str(uuid5(
                NAMESPACE_URL,
                (
                    "stockscreener:advanced-options-p8-owner:"
                    f"{_package()['package_nonce']}:FLOW"
                ),
            )),
            1,
            STOP,
        )


class _LeaseConnection:
    closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @staticmethod
    def cursor():
        class Manager:
            def __enter__(self):
                return _LeaseCursor()

            def __exit__(self, *_args):
                return False

        return Manager()

    def close(self):
        self.closed = True


def test_docker_service_invokes_real_role_boundary(tmp_path, monkeypatch):
    package_path, bundle_path, _bundle = _write_bundle(tmp_path)
    status = tmp_path / "status"
    status.mkdir()
    invoked = []

    def role_runner(**kwargs):
        invoked.append(kwargs["kind"])
        time.sleep(0.05)
        kwargs["stop_event"].set()
        return {
            "status": "COMPLETE",
            "kind": kwargs["kind"].value,
            "effects": dict(P8_EFFECT_DENIALS),
        }

    monkeypatch.setattr(
        runtime_module.psycopg2,
        "connect",
        lambda *_args, **_kwargs: _LeaseConnection(),
    )

    assert run_p8_docker_service(
        AdvancedOptionsProcessKind.FLOW,
        package_path,
        bundle_path,
        environment={
            "REALTIME_V2_APP_DSN": "fixture",
            "REALTIME_V2_P8_STATUS_DIR": str(status),
        },
        clock=lambda: START + timedelta(minutes=2),
        role_runner=role_runner,
    ) == 0
    assert invoked == [AdvancedOptionsProcessKind.FLOW]
    stored = json.loads((status / "flow.json").read_text(encoding="utf-8"))
    assert stored["state"] == "STOPPED"
    assert stored["role_evidence"]["status"] == "COMPLETE"


def test_production_role_refuses_missing_binding():
    with pytest.raises(RuntimeError, match="binding is unavailable"):
        roles_module.run_p8_role(
            kind=AdvancedOptionsProcessKind.INGRESS,
            package=_package(),
            bundle={"production_binding": None},
            environment={},
            stop_event=threading.Event(),
        )


def test_window_role_requires_dependencies_and_exact_serial_result(
    tmp_path,
    monkeypatch,
):
    status = tmp_path / "status"
    status.mkdir()
    for kind in (
        AdvancedOptionsProcessKind.INGRESS,
        AdvancedOptionsProcessKind.MARKET_STATE,
        AdvancedOptionsProcessKind.FLOW,
    ):
        (status / f"{kind.value.casefold().replace('_', '-')}.json").write_text(
            json.dumps({
                "state": "ROLE_COMPLETE",
                "role_evidence": {
                    "status": "COMPLETE",
                    "kind": kind.value,
                    "effects": dict(P8_EFFECT_DENIALS),
                },
            }),
            encoding="utf-8",
        )
    observed = {}

    def serial(**kwargs):
        observed.update(kwargs["dependency_evidence"])
        return {
            "status": "FINISHED",
            "window_state": "COMPLETE",
            "stage_order": [
                "OPTIONS_ACQUIRE",
                "OPTIONS_ADMISSION",
                "OPTIONS_FLOW",
                "OPTIONS_INPUT_GATE",
                "OPTIONS_CYCLE",
                "OPTIONS_PROJECTION",
                "OPTIONS_PUBLISH",
            ],
        }

    from realtime import advanced_options_p8_production as production

    monkeypatch.setattr(production, "run_p8_serial_window", serial)
    binding = SimpleNamespace(
        window_id=UUID("11111111-2222-3333-4444-555555555555"),
    )

    result = roles_module._run_window(
        package=_package(),
        bundle={},
        binding=binding,
        provider=object(),
        environment={"REALTIME_V2_P8_STATUS_DIR": str(status)},
        stop_event=threading.Event(),
    )

    assert result["status"] == "COMPLETE"
    assert set(observed) == {"INGRESS", "MARKET_STATE", "FLOW"}


def test_p8_options_gateway_subscriptions_remain_option_only():
    contracts = (
        SimpleNamespace(
            contract_ticker="O:QQQ261021P00500000",
            underlyer="QQQ",
            catalog_version_id=100,
        ),
        SimpleNamespace(
            contract_ticker="O:SPY261021C00600000",
            underlyer="SPY",
            catalog_version_id=101,
        ),
    )
    control = SimpleNamespace(
        session_date=START.date(),
        universe=SimpleNamespace(
            equity_tickers=("QQQ", "SPY"),
            option_underlyings=("QQQ", "SPY"),
        ),
        profile_sha256="1" * 64,
        slot_set_sha256="2" * 64,
    )
    binding = SimpleNamespace(
        enrollment=SimpleNamespace(
            control_plane=control,
            activity_selection=SimpleNamespace(
                contracts=contracts,
            ),
        ),
    )
    manifest = roles_module._manifest(binding)
    gateway = AdvancedGateway(
        GatewayConfiguration.options(),
        manifest,
        RawMessageBuffer(
            BufferLimits(10, 1024, 1_000_000_000),
            monotonic_ns=lambda: 1,
        ),
        api_key="fixture",
        monotonic_ns=lambda: 1,
    )

    assert gateway.subscriptions == (
        "T.O:QQQ261021P00500000",
        "T.O:SPY261021C00600000",
        "Q.O:QQQ261021P00500000",
        "Q.O:SPY261021C00600000",
    )
    assert all(not value.startswith("AM.") for value in gateway.subscriptions)


class _Launcher:
    def __init__(self):
        self.starts = 0
        self.ticks = 0
        self.stops = 0

    def start(self, *, now):
        self.starts += 1
        return {"started": now}

    def tick(self, *, now):
        self.ticks += 1

    def stop(self):
        self.stops += 1


class _Bundle:
    def __init__(self):
        self.loads = 0

    def load(self, **_kwargs):
        self.loads += 1
        return {}


def test_resident_launcher_mandatory_stop_joins_heartbeat():
    launcher = _Launcher()
    resident = P8HeartbeatResidentLauncher(
        launcher,
        _Bundle(),
        interval_seconds=1,
    )
    resident.start(now=START + timedelta(minutes=2))
    resident.stop()
    assert launcher.starts == launcher.stops == 1


class _Transcript:
    def __init__(self):
        self.calls = []
        self.running = True

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
        self.calls.append(exact)
        if exact[1] == "compose" and "ps" in exact:
            output = CONTAINER_ID + "\n"
        elif exact[1] == "inspect":
            kind = AdvancedOptionsProcessKind.INGRESS
            output = json.dumps({
                "id": CONTAINER_ID,
                "command": list(p8_container_command(kind)),
                "entrypoint": None,
                "running": self.running,
                "status": "running" if self.running else "exited",
                "health": "healthy",
                "restart": {"Name": "no", "MaximumRetryCount": 0},
                "labels": {
                    "com.docker.compose.project": P8_COMPOSE_PROJECT,
                    "com.docker.compose.service": P8_SERVICE_NAMES[kind],
                },
            }) + "\n"
        elif exact[1] == "stop":
            self.running = False
            output = CONTAINER_ID + "\n"
        elif exact[1] == "rm":
            output = CONTAINER_ID + "\n"
        else:
            output = ""
        return CommandResult(exact, 0, output.encode(), b"")


def test_docker_port_start_health_stop_and_cleanup_contract(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "docker-compose.realtime-v2.yml").write_text(
        "services: {}\n",
        encoding="utf-8",
    )
    docker = tmp_path / "docker.exe"
    docker.write_bytes(b"fake")
    package_path, bundle_path, _ = _write_bundle(root)
    secrets = root / "secrets"
    secrets.mkdir()
    (secrets / "realtime_v2_app_dsn").write_text(
        "fixture",
        encoding="utf-8",
    )
    (secrets / "realtime_v2_provider_key").write_text(
        "fixture",
        encoding="utf-8",
    )
    environment = isolated_p8_compose_environment(
        {},
        backend_image="backend@sha256:" + "b" * 64,
        secret_directory=secrets,
        package_path=package_path,
        bundle_path=bundle_path,
    )
    transcript = _Transcript()
    port = AdvancedOptionsDockerServicePort(
        package=_package(),
        repository_root=root,
        docker_executable=docker.resolve(),
        environment=environment,
        runner=transcript,
    )
    spec = AdvancedOptionsProcessSpec(
        AdvancedOptionsProcessKind.INGRESS,
        "advanced-options-ingress-v1",
        "massive:options:websocket",
    )
    handle = port.start(spec)
    assert port.alive(handle)
    port.stop(handle, deadline_seconds=60)
    assert [call[1] for call in transcript.calls].count("stop") == 1
    assert [call[1] for call in transcript.calls].count("rm") == 1
    assert all(
        "--profile" in call and P8_COMPOSE_PROFILE in call
        for call in transcript.calls
        if call[1] == "compose"
    )
    up = next(
        call for call in transcript.calls
        if call[1] == "compose" and "up" in call
    )
    assert "--no-build" in up
    assert "--no-deps" in up
    assert "--wait" in up
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
        for row in port.evidence()
    )


def test_repository_runtime_factory_wires_docker_without_starting_it(tmp_path):
    root = tmp_path / "repo"
    (root / "backend" / ".realtime-canary").mkdir(parents=True)
    (root / "docker-compose.realtime-v2.yml").write_text(
        "services: {}\n",
        encoding="utf-8",
    )
    package = _package()
    package["cleanup"] = {
        "lock_path": "backend/.realtime-canary/p8.lock",
    }
    package_path = root / "backend" / ".realtime-canary" / "package.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    secrets = root / "secrets"
    secrets.mkdir()
    (secrets / "realtime_v2_app_dsn").write_text(
        "fixture",
        encoding="utf-8",
    )
    (secrets / "realtime_v2_provider_key").write_text(
        "fixture",
        encoding="utf-8",
    )
    docker = tmp_path / "docker.exe"
    docker.write_bytes(b"fake")
    transcript = _Transcript()
    capture = SimpleNamespace(capture=lambda *_args, **_kwargs: None)
    components = P8RuntimeComponents(
        capture=capture,
        enrollment_connection=object(),
        database=object(),
        orchestrator=object(),
        publisher_database=object(),
        publication_config=PortalPublicationConfig(
            cohort_id=UUID(package["cohort"]["cohort_id"]),
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            owner_id=UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            owner_epoch=1,
            dispatch_authorized=False,
        ),
        publication_batch=lambda *_args: None,
        publication_observation=lambda *_args: {},
    )

    port = build_advanced_options_p8_runtime(
        package=package,
        package_path=package_path,
        repository_root=root,
        components_factory=lambda **_kwargs: components,
        docker_executable=docker.resolve(),
        environment={
            "REALTIME_V2_BACKEND_IMAGE": (
                "backend@sha256:" + "b" * 64
            ),
            "REALTIME_V2_DOCKER_SECRET_DIR": str(secrets),
        },
        command_runner=transcript,
    )

    assert port._port._publication_config.dispatch_authorized is False
    assert isinstance(
        port._port._resident_launcher,
        P8HeartbeatResidentLauncher,
    )
    assert transcript.calls == []
    assert not (
        root
        / "backend"
        / ".realtime-canary"
        / "p8.execution-bundle.json"
    ).exists()


def test_default_factory_defers_database_and_provider_components(
    tmp_path,
    monkeypatch,
):
    from realtime import advanced_options_p8_production as production

    root = tmp_path / "repo"
    (root / "backend" / ".realtime-canary").mkdir(parents=True)
    (root / "docker-compose.realtime-v2.yml").write_text(
        "services: {}\n",
        encoding="utf-8",
    )
    package = _package()
    package["cleanup"] = {
        "lock_path": "backend/.realtime-canary/p8.lock",
    }
    package_path = root / "backend" / ".realtime-canary" / "package.json"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    secrets = root / "secrets"
    secrets.mkdir()
    (secrets / "realtime_v2_app_dsn").write_text(
        "fixture",
        encoding="utf-8",
    )
    (secrets / "realtime_v2_provider_key").write_text(
        "fixture",
        encoding="utf-8",
    )
    docker = tmp_path / "docker.exe"
    docker.write_bytes(b"fake")
    monkeypatch.setattr(
        production,
        "build_advanced_options_p8_production_components",
        lambda **_kwargs: pytest.fail(
            "factory construction opened production components"
        ),
    )

    port = build_advanced_options_p8_runtime(
        package=package,
        package_path=package_path,
        repository_root=root,
        docker_executable=docker.resolve(),
        environment={
            "REALTIME_V2_BACKEND_IMAGE": (
                "backend@sha256:" + "c" * 64
            ),
            "REALTIME_V2_DOCKER_SECRET_DIR": str(secrets),
        },
        command_runner=_Transcript(),
    )

    assert port._closed is False
    port.close()
    assert port._closed is True


def test_production_live_state_uses_the_current_selection_capture():
    from realtime import advanced_options_p8_production as production

    class Cursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return []

    class Transaction:
        def __enter__(self):
            return SimpleNamespace(cursor=Cursor())

        def __exit__(self, *_args):
            return None

    class Provider:
        def transaction(self, *_args, **kwargs):
            assert kwargs == {
                "read_only": True,
                "dict_cursor": True,
                "isolation_level": "REPEATABLE READ",
            }
            return Transaction()

    capture = SimpleNamespace(
        selection=SimpleNamespace(valid_until=STOP),
    )
    live = object.__new__(production._P8ProductionCapture)
    live._provider = Provider()
    live._package = _package()

    assert live._live_state(START, capture) == (
        [],
        [],
        [],
        [],
        [],
        [],
    )
