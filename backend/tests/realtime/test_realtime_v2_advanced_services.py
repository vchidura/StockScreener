import importlib
from pathlib import Path
from types import SimpleNamespace

from realtime.equity_pipeline_config import RealtimeV2PipelineConfig
from realtime.advanced_realtime_v3_runtime import (
    ADVANCED_REALTIME_V3_COMPOSE_PROFILE,
    ADVANCED_REALTIME_V3_ROLES,
)


script = importlib.import_module("scripts.run_realtime_v2_advanced_services")
selector = importlib.import_module("scripts.run_realtime_v2_equity_services")
BASE = [
    "--runtime-mode",
    "legacy-diagnostic",
    "--cohort-id",
    "11111111-1111-1111-1111-111111111111",
    "--authorization-id",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "--universe-target",
    "1",
    "--session-date",
    "2026-10-16",
    "--first-cutoff",
    "2026-10-16T13:35:00+00:00",
    "--authorization-reference",
    "advanced-equity-v2-20261016",
    "--decoded-byte-ceiling",
    "10485760",
    "--stream-sessions",
    "3",
    "--connection-attempts",
    "3",
    "--rest-repair-requests",
    "0",
    "--rest-repair-decoded-bytes",
    "0",
    "--finality-delay-seconds",
    "5",
    "--run-directory",
    "advanced-services",
    "--execute",
]


def _config(pipeline):
    values = {
        "REALTIME_V2_EQUITY_PIPELINE": pipeline,
        "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
        "REALTIME_V2_APP_DSN": "postgresql://app:secret@db/stocks_realtime_v2",
        "REALTIME_V2_API_DSN": "postgresql://api:secret@db/stocks_realtime_v2",
        "POLYGON_API_KEY": "configured",
    }
    if pipeline == "MASSIVE_ADVANCED":
        values.update({
            "REALTIME_V2_ADVANCED_JOURNAL_DIR": "C:\\runtime\\advanced",
            "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH": str(
                Path(__file__).resolve().parents[2]
                / "realtime"
                / "policies"
                / "advanced_equity_ingress_v1.json"
            ),
            "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION": (
                "advanced-equity-ingress-v1"
            ),
        })
    return RealtimeV2PipelineConfig.from_mapping(values)


def test_advanced_supervisor_has_no_developer_rest_process(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args(BASE)
    commands = script.build_commands(args)

    assert set(commands) == {
        "api",
        "frontend",
        "finalizer",
        "bridge",
        "controller",
        "equity-ingress",
        "scheduler",
    }
    flattened = " ".join(
        value
        for command in commands.values()
        for value in command
    )
    assert "run_realtime_v2_advanced_controller.py" in flattened
    assert "run_realtime_v2_advanced_ingress_service.py" in flattened
    assert "run_realtime_v2_advanced_journal_replay.py" in flattened
    assert "run_realtime_v2_advanced_equity_scheduler.py" in flattened
    assert "developer" not in flattened.lower()
    assert "--publish" not in flattened
    scheduler = commands["scheduler"]
    assert scheduler[
        scheduler.index("--publication-mode") + 1
    ] == "projection-only"
    controller = commands["controller"]
    assert "--authorization-id" in controller
    assert "--universe-target" in controller
    assert "--cohort-id" not in controller
    assert "--authorization-reference" not in controller


def test_continuous_supervisor_has_same_resident_roles_without_daily_scope(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args([
        "--run-directory",
        "continuous-services",
        "--execute",
    ])
    commands = script.build_commands(args)

    assert args.runtime_mode == "continuous"
    assert args.authorization_id is None
    assert args.cohort_id is None
    assert args.session_date is None
    assert args.first_cutoff is None
    assert set(commands) == {
        role.name for role in ADVANCED_REALTIME_V3_ROLES
    }
    for role in ("controller", "equity-ingress", "scheduler"):
        assert "--runtime-mode" in commands[role]
        assert commands[role][
            commands[role].index("--runtime-mode") + 1
        ] == "continuous"
    flattened = " ".join(
        value
        for command in commands.values()
        for value in command
    )
    for selector in (
        "--authorization-id",
        "--cohort-id",
        "--session-date",
        "--first-cutoff",
    ):
        assert selector not in flattened
    assert "--desired-state" in commands["finalizer"]


def test_continuous_native_and_compose_own_the_same_c8_topology(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args([
        "--run-directory",
        "continuous-c8-services",
        "--retention-directory",
        str(tmp_path / "retention"),
        "--execute",
    ])
    commands = script.build_commands(args)
    expected = {role.name for role in ADVANCED_REALTIME_V3_ROLES}

    assert set(commands) == expected
    flattened = " ".join(
        value
        for command in commands.values()
        for value in command
    )
    for selector in (
        "--session-date",
        "--package",
        "--package-source",
        "--bundle",
        "--execution-bundle",
        "--binding",
        "--release",
        "--pointer",
    ):
        assert selector not in flattened

    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")
    service_by_role = {
        "capability-refresh": "realtime-v2-advanced-capability-refresh",
        "equity-preparation": "realtime-v2-advanced-preparation",
        "controller": "realtime-v2-advanced-controller",
        "equity-ingress": "realtime-v2-advanced-ingress",
        "options-lifecycle": "realtime-v3-options-lifecycle",
        "options-acquisition": "realtime-v3-options-acquisition",
        "bridge": "realtime-v2-advanced-bridge",
        "finalizer": "realtime-v2-advanced-finalizer",
        "scheduler": "realtime-v2-advanced-scheduler",
        "detectors": "realtime-v2-advanced-detectors",
        "canonical-stock-alerts": (
            "realtime-v2-canonical-stock-alerts"
        ),
        "publication-retry": "realtime-v3-publication-retry",
        "options-ingress": "realtime-v3-options-p8-ingress",
        "options-market-state": (
            "realtime-v3-options-p8-market-state"
        ),
        "options-flow": "realtime-v3-options-p8-flow",
        "options-window": "realtime-v3-options-p8-window",
        "retention": "realtime-v2-retention",
        "api": "realtime-v2-advanced-api",
        "frontend": "realtime-v2-advanced-frontend",
    }
    assert set(service_by_role) == expected
    for service in service_by_role.values():
        lines = compose.split(f"  {service}:", 1)[1].splitlines()
        block = "\n".join(
            line
            for line in lines
            if not (
                line.startswith("  ")
                and not line.startswith("    ")
                and line.rstrip().endswith(":")
            )
        )
        block = block.split("\n\n", 1)[0]
        assert ADVANCED_REALTIME_V3_COMPOSE_PROFILE in block
        assert (
            "restart: unless-stopped" in block
            or "<<: *backend-service" in block
        )
        for forbidden in (
            "REALTIME_V2_P8_PACKAGE_PATH",
            "REALTIME_V2_P8_BUNDLE_PATH",
            "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_PATH",
        ):
            assert forbidden not in block


def test_process_environments_scope_provider_and_database_secrets():
    environment = {
        "POLYGON_API_KEY": "provider",
        "REALTIME_V2_APP_DSN": "app",
        "REALTIME_V2_API_DSN": "api",
        "PATH": "retained",
    }

    controller = script.process_environment(environment, "controller")
    capability_refresh = script.process_environment(
        environment,
        "capability-refresh",
    )
    preparation = script.process_environment(
        environment,
        "equity-preparation",
    )
    ingress = script.process_environment(
        environment,
        "equity-ingress",
    )
    bridge = script.process_environment(environment, "bridge")
    api = script.process_environment(environment, "api")
    frontend = script.process_environment(environment, "frontend")

    assert set(controller) >= {
        "POLYGON_API_KEY",
        "REALTIME_V2_APP_DSN",
        "PATH",
    }
    assert "REALTIME_V2_API_DSN" not in controller
    assert set(ingress) >= {
        "POLYGON_API_KEY",
        "REALTIME_V2_APP_DSN",
        "PATH",
    }
    assert set(capability_refresh) >= {
        "POLYGON_API_KEY",
        "REALTIME_V2_APP_DSN",
        "PATH",
    }
    assert set(preparation) >= {
        "POLYGON_API_KEY",
        "REALTIME_V2_APP_DSN",
        "PATH",
    }
    assert set(bridge) >= {"REALTIME_V2_APP_DSN", "PATH"}
    assert "POLYGON_API_KEY" not in bridge
    assert "REALTIME_V2_API_DSN" not in bridge
    assert set(api) >= {"REALTIME_V2_API_DSN", "PATH"}
    assert "POLYGON_API_KEY" not in api
    assert "REALTIME_V2_APP_DSN" not in api
    assert frontend == {"PATH": "retained"}


def test_environment_selector_chooses_separate_supervisors():
    advanced_main = selector.selected_main(
        _config("MASSIVE_ADVANCED")
    )
    developer_main = selector.selected_main(
        _config("MASSIVE_DEVELOPER")
    )

    assert advanced_main.__module__.endswith(
        "run_realtime_v2_advanced_services"
    )
    assert developer_main.__module__.endswith(
        "run_realtime_v2_services"
    )


def test_capability_refresh_restart_backoff_is_bounded():
    assert [
        script._capability_restart_delay(attempt)
        for attempt in range(1, 9)
    ] == [1, 2, 4, 8, 16, 32, 60, 60]


def test_initial_transient_launch_failure_does_not_stop_other_roles():
    api = script._RuntimeRole("api")
    controller = script._RuntimeRole("controller")
    attempts = {"api": 0}
    running = object()

    def starter(name):
        if name == "api":
            attempts["api"] += 1
            if attempts["api"] == 1:
                raise OSError("temporary executable lock")
        return running

    script._schedule_role_start(api, now=10.0, starter=starter)
    script._schedule_role_start(
        controller,
        now=10.0,
        starter=starter,
    )

    assert api.state == "DEGRADED"
    assert api.child is None
    assert api.next_restart_at == 11.0
    assert controller.state == "RUNNING"
    assert controller.child is running

    script._schedule_role_start(api, now=11.0, starter=starter)

    assert api.state == "RUNNING"
    assert api.child is running
    assert api.restart_attempts == 1


def test_terminal_role_configuration_error_is_not_retried():
    role = script._RuntimeRole("retention")

    def invalid(_name):
        raise script._RoleConfigurationError(
            "retention archive root is invalid"
        )

    script._schedule_role_start(role, now=20.0, starter=invalid)

    assert role.state == "FAILED_TERMINAL"
    assert role.next_restart_at is None
    assert role.child is None
    assert role.restart_attempts == 1


def test_terminal_exit_code_is_separate_from_transient_restart():
    terminal = script._RuntimeRole("retention", child=object())
    transient = script._RuntimeRole("api", child=object())

    script._record_role_exit(
        terminal,
        exit_code=2,
        now=30.0,
        log_path=Path("retention.log"),
    )
    script._record_role_exit(
        transient,
        exit_code=1,
        now=30.0,
        log_path=Path("api.log"),
    )

    assert terminal.state == "FAILED_TERMINAL"
    assert terminal.next_restart_at is None
    assert transient.state == "DEGRADED"
    assert transient.next_restart_at == 31.0


def test_main_reaches_fenced_initial_scheduling_with_isolated_failure(
    tmp_path,
    monkeypatch,
):
    journal = tmp_path / "journal"
    journal.mkdir()
    policy = (
        Path(__file__).resolve().parents[2]
        / "realtime"
        / "policies"
        / "advanced_equity_ingress_v1.json"
    )
    environment = {
        "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
        "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
        "REALTIME_V2_APP_DSN": "app",
        "REALTIME_V2_API_DSN": "api",
        "REALTIME_V2_ADMIN_DSN": "admin",
        "POLYGON_API_KEY": "provider",
        "REALTIME_V2_ADVANCED_JOURNAL_DIR": str(journal),
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH": str(policy),
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION": (
            "advanced-equity-ingress-v1"
        ),
    }
    events = []
    statuses = []

    class Pool:
        def __init__(self, *_args, **_kwargs):
            events.append("pool")

        def closeall(self):
            events.append("pool-closed")

    class Provider:
        def __init__(self, *_args, **_kwargs):
            events.append("provider")

        def verify(self):
            events.append("provider-verified")

    class Heartbeat:
        def __init__(self, *_args, role_name, **_kwargs):
            self.role_name = role_name
            self.lost = False

        def start(self):
            events.append(("fence-start", self.role_name))

        def mark_running(self):
            events.append(("fence-running", self.role_name))

        def mark_state(self, state, health, **_kwargs):
            events.append(
                ("fence-state", self.role_name, state, health)
            )

        def close(self):
            events.append(("fence-close", self.role_name))

    class HostLease:
        def __init__(self, *_args, **_kwargs):
            pass

        def acquire(self):
            events.append("host-acquired")

        def release(self):
            events.append("host-released")

    class Output:
        def close(self):
            pass

    class Process:
        _next_pid = 100

        def __init__(self):
            self.pid = Process._next_pid
            Process._next_pid += 1

        def poll(self):
            return None

    def start(name, *_args, **_kwargs):
        events.append(("start", name))
        if name == "api":
            raise OSError("temporary API launch failure")
        return SimpleNamespace(
            name=name,
            process=Process(),
            output=Output(),
        )

    def stop(child):
        events.append(("stop", child.name))

    monkeypatch.setattr(
        script,
        "dotenv_values",
        lambda _path: environment,
    )
    monkeypatch.setattr(script.os, "environ", {})
    monkeypatch.setattr(script, "ThreadedConnectionPool", Pool)
    monkeypatch.setattr(script, "V2ConnectionProvider", Provider)
    monkeypatch.setattr(script, "PostgresLeaseHeartbeat", Heartbeat)
    monkeypatch.setattr(script, "HostProviderSocketLease", HostLease)
    monkeypatch.setattr(script, "_surviving_children", lambda _commands: ())
    monkeypatch.setattr(script, "_start", start)
    monkeypatch.setattr(script, "_stop", stop)
    monkeypatch.setattr(
        script,
        "_write_runtime_status",
        lambda _path, roles, *, status: statuses.append(
            (
                status,
                {
                    name: role.state
                    for name, role in roles.items()
                },
            )
        ),
    )
    monkeypatch.setattr(
        script.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    result = script.main([
        "--run-directory",
        str(tmp_path / "run"),
        "--retention-directory",
        str(tmp_path / "retention"),
        "--runtime-lock-path",
        str(tmp_path / "runtime.lock"),
        "--execute",
    ])

    assert result == 0
    assert "provider-verified" in events
    assert ("fence-start", "runtime") in events
    assert ("start", "api") in events
    assert ("start", "controller") in events
    initial = statuses[0]
    assert initial[0] == "DEGRADED"
    assert initial[1]["api"] == "DEGRADED"
    assert initial[1]["controller"] == "RUNNING"
    assert ("stop", "controller") in events
    assert "host-released" in events
    assert "pool-closed" in events
