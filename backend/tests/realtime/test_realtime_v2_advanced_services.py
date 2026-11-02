import importlib
import inspect
import os
import re
import shutil
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from options.calendar import OptionExchangeCalendar

from realtime.database_access_v2 import V2ConnectionProvider
from realtime.equity_pipeline_config import (
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
)
from realtime.advanced_equity_lifecycle import (
    AdvancedEquityLifecyclePhase,
)
from realtime.advanced_equity_preparation import EquityPreparationRunResult
from realtime.advanced_realtime_v3_runtime import (
    ADVANCED_REALTIME_V3_COMPOSE_PROFILE,
    ADVANCED_REALTIME_V3_ROLES,
)


script = importlib.import_module("scripts.run_realtime_v2_advanced_services")
selector = importlib.import_module("scripts.run_realtime_v2_equity_services")
equity_preparation = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_preparation"
)
options_lifecycle = importlib.import_module(
    "scripts.run_advanced_options_v3_lifecycle"
)
options_preparation = importlib.import_module(
    "realtime.advanced_options_automatic_preparation"
)
equity_scheduler = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_scheduler"
)
status_report = importlib.import_module(
    "scripts.report_advanced_realtime_v3_status"
)
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


def test_equity_preparation_default_environment_file_is_optional(tmp_path):
    args = equity_preparation.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--execute",
    ])

    assert args.env_file is None

    with pytest.raises(SystemExit):
        equity_preparation.parse_args([
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--env-file",
            str(tmp_path / "missing.env"),
            "--execute",
        ])


@pytest.mark.parametrize(
    ("status", "next_retry_after_seconds", "expected_wait"),
    (
        ("PROGRESSED", None, equity_preparation.COMPLETED_CHUNK_PAUSE_SECONDS),
        ("COMPLETE", None, equity_preparation.COMPLETED_CHUNK_PAUSE_SECONDS),
        ("IDLE", None, 15.0),
        ("LIVE_PRIORITY", None, 15.0),
        ("BLOCKED", None, 15.0),
        ("RETRY", 4.0, 4.0),
        ("RETRY", 60.0, 15.0),
    ),
)
def test_equity_preparation_waits_the_poll_only_without_completed_work(
    monkeypatch,
    status,
    next_retry_after_seconds,
    expected_wait,
):
    checked = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)
    stop = SimpleNamespace(
        waits=[],
        is_set=lambda: bool(stop.waits),
        set=lambda: None,
        wait=lambda seconds: stop.waits.append(seconds),
    )
    resident = SimpleNamespace(
        run_once=lambda: EquityPreparationRunResult(
            status,
            checked,
            next_retry_at=(
                None
                if next_retry_after_seconds is None
                else checked + timedelta(seconds=next_retry_after_seconds)
            ),
        )
    )
    monkeypatch.setattr(equity_preparation, "_now", lambda: checked)
    monkeypatch.setattr(
        equity_preparation,
        "threading",
        SimpleNamespace(Event=lambda: stop),
    )
    monkeypatch.setattr(
        equity_preparation,
        "RealtimeV2PipelineConfig",
        SimpleNamespace(
            from_mapping=lambda *_args, **_kwargs: SimpleNamespace(
                equity_pipeline=EquityPipeline.MASSIVE_ADVANCED,
                options_pipeline=OptionsPipeline.DISABLED,
                massive_api_key="configured",
            )
        ),
    )
    monkeypatch.setattr(
        equity_preparation,
        "_provider",
        lambda _config: (object(), SimpleNamespace(closeall=lambda: None)),
    )
    monkeypatch.setattr(
        equity_preparation,
        "BoundedPolygonEquityPreparationClient",
        lambda _key: object(),
    )
    monkeypatch.setattr(
        equity_preparation,
        "MassiveEquityPreparationSource",
        lambda _client: object(),
    )
    monkeypatch.setattr(
        equity_preparation,
        "AdvancedEquityPreparationResident",
        lambda *_args, **_kwargs: resident,
    )

    assert equity_preparation.main([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--execute",
    ]) == 0

    assert stop.waits == [expected_wait]


def test_options_lifecycle_stream_ceilings_are_configurable():
    environment = {
        "REALTIME_V3_OPTIONS_CONNECTION_ATTEMPT_CEILING": "24",
        "REALTIME_V3_OPTIONS_DECODED_MESSAGE_CEILING": "8000000",
        "REALTIME_V3_OPTIONS_DECODED_BYTE_CEILING": "8589934592",
        "REALTIME_V3_OPTIONS_ACTIVE_QUOTE_CONTRACT_BUDGET": "400",
        "REALTIME_V3_OPTIONS_PER_UNDERLYING_CONTRACT_LIMIT": "12",
        "REALTIME_V3_OPTIONS_ESTABLISHED_MEASURED_CAPACITY": "350",
        "REALTIME_V3_OPTIONS_EFFECTIVE_PROVIDER_PROJECT_CEILING": "500",
    }

    assert options_lifecycle._positive_environment_int(
        environment,
        "REALTIME_V3_OPTIONS_CONNECTION_ATTEMPT_CEILING",
        12,
    ) == 24
    assert options_lifecycle._positive_environment_int(
        environment,
        "REALTIME_V3_OPTIONS_DECODED_MESSAGE_CEILING",
        1_000_000,
    ) == 8_000_000
    with pytest.raises(RuntimeError):
        options_lifecycle._positive_environment_int(
            {"LIMIT": "0"},
            "LIMIT",
            1,
        )
    assert options_lifecycle._optional_positive_environment_int(
        environment,
        "REALTIME_V3_OPTIONS_ACTIVE_QUOTE_CONTRACT_BUDGET",
    ) == 400
    assert options_lifecycle._optional_positive_environment_int(
        {},
        "REALTIME_V3_OPTIONS_ACTIVE_QUOTE_CONTRACT_BUDGET",
    ) is None


@pytest.mark.parametrize(
    ("environment", "expected"),
    (
        ({}, 32),
        ({"REALTIME_V3_OPTIONS_EXACT_FOUR_COMPATIBILITY": "enabled"}, 4),
        (
            {"REALTIME_V3_OPTIONS_MAXIMUM_PROVIDER_REQUESTS_PER_UNDERLYING": ""},
            32,
        ),
        (
            {"REALTIME_V3_OPTIONS_MAXIMUM_PROVIDER_REQUESTS_PER_UNDERLYING": "20"},
            20,
        ),
    ),
)
def test_options_discovery_request_limit_defaults_by_compatibility_mode(
    environment,
    expected,
):
    assert (
        options_lifecycle._maximum_provider_requests_per_underlying(
            environment
        )
        == expected
    )


def test_options_discovery_budget_scales_with_the_configured_request_limit():
    session_date = date(2026, 11, 2)
    executor = options_preparation.PostgresAutomaticOptionsPreparationExecutor(
        V2ConnectionProvider.__new__(V2ConnectionProvider),
        api_key="configured",
        backend_image_id="sha256:" + "0" * 64,
        maximum_provider_requests_per_underlying=(
            options_lifecycle._maximum_provider_requests_per_underlying({})
        ),
        clock=lambda: datetime(2026, 11, 2, 14, 0, tzinfo=timezone.utc),
    )

    budget = executor.provider_budget(
        SimpleNamespace(
            stage="CATALOG_QUOTE",
            session_date=session_date,
            revision_id=UUID(int=1),
            ticker="SPY",
            option_underlyings=("QQQ", "SPY"),
        )
    )

    assert budget.scope.request_ceiling == 32 * 2 * 3
    assert len(budget.requests) == 32
    with pytest.raises(ValueError, match="configuration is invalid"):
        options_preparation.PostgresAutomaticOptionsPreparationExecutor(
            V2ConnectionProvider.__new__(V2ConnectionProvider),
            api_key="configured",
            backend_image_id="sha256:" + "0" * 64,
            maximum_provider_requests_per_underlying=33,
        )


def test_close_status_retains_current_processing_until_scheduler_deadline():
    session_date = date(2026, 11, 2)
    calendar = OptionExchangeCalendar("XNYS")
    checked_at = (
        calendar.session_close(session_date)
        + timedelta(minutes=2)
    )

    lifecycle = status_report.advanced_realtime_v3_status_lifecycle(
        checked_at
    )

    assert lifecycle.phase is AdvancedEquityLifecyclePhase.DRAINING
    assert lifecycle.processing_session_date == session_date
    assert lifecycle.preparation_session_date == date(2026, 11, 3)
    assert lifecycle.processing_deadline > checked_at
    assert (
        status_report.ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS
        == 300
    )

    after_deadline = (
        status_report.advanced_realtime_v3_status_lifecycle(
            lifecycle.processing_deadline + timedelta(microseconds=1)
        )
    )
    assert after_deadline.processing_session_date == date(2026, 11, 3)


def test_scheduler_options_lifecycle_and_status_share_runtime_timing():
    scheduler_args = equity_scheduler.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--continuous",
        "--execute",
    ])
    lifecycle_args = options_lifecycle.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--continuous",
        "--execute",
    ])

    assert scheduler_args.finality_delay_seconds == 5
    assert lifecycle_args.finality_delay_seconds == 5
    assert scheduler_args.completion_budget_seconds == 300
    assert lifecycle_args.completion_budget_seconds == 300


def test_compose_lifecycle_inherits_all_options_ceiling_configuration():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")

    ceiling_names = (
        "REALTIME_V3_OPTIONS_CONNECTION_ATTEMPT_CEILING",
        "REALTIME_V3_OPTIONS_DECODED_MESSAGE_CEILING",
        "REALTIME_V3_OPTIONS_DECODED_BYTE_CEILING",
        "REALTIME_V3_OPTIONS_ACTIVE_QUOTE_CONTRACT_BUDGET",
        "REALTIME_V3_OPTIONS_PER_UNDERLYING_CONTRACT_LIMIT",
        "REALTIME_V3_OPTIONS_ESTABLISHED_MEASURED_CAPACITY",
        "REALTIME_V3_OPTIONS_EFFECTIVE_PROVIDER_PROJECT_CEILING",
        "REALTIME_V3_OPTIONS_MAXIMUM_PROVIDER_REQUESTS_PER_UNDERLYING",
    )
    shared_environment = compose.split(
        "x-advanced-environment: &advanced-environment",
        1,
    )[1].split("x-downstream-labels:", 1)[0]
    lifecycle_service = compose.split(
        "  realtime-v3-options-lifecycle:",
        1,
    )[1].split("  realtime-v3-publication-retry:", 1)[0]
    script_source = inspect.getsource(options_lifecycle)

    assert "<<: *advanced-environment" in lifecycle_service
    for name in ceiling_names:
        assert name in shared_environment
        assert name in script_source


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
        "options-scheduler": "realtime-v3-options-scheduler",
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


def _compose_service_blocks():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")
    section = compose.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    names = re.findall(r"^  ([a-z0-9-]+):$", section, flags=re.MULTILINE)
    return {
        name: re.split(
            r"^  [a-z0-9-]+:$",
            section.split(f"  {name}:\n", 1)[1],
            maxsplit=1,
            flags=re.MULTILINE,
        )[0]
        for name in names
    }


def test_v3_profile_services_do_not_leak_through_compose_extends():
    # Compose merges `profiles` across `extends`, so each service extending a
    # V3-profile service must replace the list or it joins the V3 topology.
    blocks = _compose_service_blocks()

    def declares_v3(block):
        return bool(re.search(
            r"^    profiles: .*\b"
            + re.escape(ADVANCED_REALTIME_V3_COMPOSE_PROFILE),
            block,
            flags=re.MULTILINE,
        ))

    effective = set()
    extending = set()
    for name, block in blocks.items():
        if declares_v3(block):
            effective.add(name)
        extended = re.search(
            r"^    extends:\n      service: (\S+)$",
            block,
            flags=re.MULTILINE,
        )
        if extended is None or not declares_v3(blocks[extended.group(1)]):
            continue
        extending.add(name)
        if not re.search(r"^    profiles: !override \[", block, flags=re.MULTILINE):
            effective.add(name)

    assert extending == {
        "realtime-v2-options-p8-ingress",
        "realtime-v2-options-p8-market-state",
        "realtime-v2-options-p8-flow",
        "realtime-v2-options-p8-window",
    }
    assert not extending & effective
    assert len(effective) == len(ADVANCED_REALTIME_V3_ROLES) == 20


@pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="the Docker CLI is required to render Compose",
)
def test_rendered_v3_profile_has_exactly_the_runtime_role_services():
    root = Path(__file__).resolve().parents[3]
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "--file",
            str(root / "docker-compose.realtime-v2.yml"),
            "--profile",
            ADVANCED_REALTIME_V3_COMPOSE_PROFILE,
            "config",
            "--services",
        ],
        cwd=root,
        env={
            **{
                name: value
                for name, value in os.environ.items()
                if not name.startswith("COMPOSE_")
            },
            "REALTIME_V2_BACKEND_IMAGE": "sha256:" + "0" * 64,
            "REALTIME_V2_DOCKER_SECRET_DIR": str(root),
        },
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )

    assert completed.returncode == 0, completed.stderr[-2000:]
    services = set(completed.stdout.split())
    assert len(services) == len(ADVANCED_REALTIME_V3_ROLES) == 20
    assert not any(
        name.startswith("realtime-v2-options-p8") for name in services
    )


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
