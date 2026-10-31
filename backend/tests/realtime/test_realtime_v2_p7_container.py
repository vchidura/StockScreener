from __future__ import annotations

import importlib
import os
import re
import subprocess
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest


container = importlib.import_module("scripts.run_realtime_v2_container")
ingress = importlib.import_module(
    "scripts.run_realtime_v2_advanced_ingress_service"
)
health = importlib.import_module(
    "scripts.report_realtime_v2_advanced_health"
)
secrets = importlib.import_module(
    "scripts.prepare_realtime_v2_docker_secrets"
)

ROOT = Path(__file__).resolve().parents[3]
AUTHORIZATION = UUID("11111111-1111-1111-1111-111111111111")


def test_container_secret_loader_exposes_only_owned_values(tmp_path):
    app = tmp_path / "app"
    api = tmp_path / "api"
    provider = tmp_path / "provider"
    app.write_text("app-dsn", encoding="utf-8")
    api.write_text("api-dsn", encoding="utf-8")
    provider.write_text("provider-key", encoding="utf-8")

    controller = container.child_environment(
        "controller",
        {"app_dsn": app, "provider_key": provider},
        {"PATH": "retained"},
    )
    bridge = container.child_environment(
        "bridge",
        {"app_dsn": app},
        {"PATH": "retained"},
    )
    reader = container.child_environment(
        "api",
        {"api_dsn": api},
        {"PATH": "retained"},
    )

    assert controller == {
        "PATH": "retained",
        "REALTIME_V2_APP_DSN": "app-dsn",
        "POLYGON_API_KEY": "provider-key",
    }
    assert bridge == {
        "PATH": "retained",
        "REALTIME_V2_APP_DSN": "app-dsn",
    }
    assert reader == {
        "PATH": "retained",
        "REALTIME_V2_API_DSN": "api-dsn",
    }
    with pytest.raises(RuntimeError, match="secret file"):
        container.child_environment(
            "bridge",
            {"app_dsn": app},
            {"REALTIME_V2_APP_DSN": "ambient"},
        )


def test_container_cli_requires_exact_component_secret_set(tmp_path):
    app = tmp_path / "app"
    app.write_text("app", encoding="utf-8")

    args = container.parse_args([
        "--component",
        "bridge",
        "--app-dsn-file",
        str(app),
        "--",
        "python",
        "-V",
    ])
    assert args.command == ["python", "-V"]

    with pytest.raises(SystemExit):
        container.parse_args([
            "--component",
            "api",
            "--app-dsn-file",
            str(app),
            "--",
            "python",
        ])


def test_container_windows_child_uses_subprocess(monkeypatch):
    observed = {}

    def run(command, *, env, check):
        observed["command"] = command
        observed["environment"] = env
        observed["check"] = check
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(container.subprocess, "run", run)
    monkeypatch.setattr(
        container.os,
        "execvpe",
        lambda *_args: pytest.fail("Windows child used execvpe"),
    )

    assert container.run_child(
        ["python", "-V"],
        {"PATH": "retained"},
        windows=True,
    ) == 7
    assert observed == {
        "command": ["python", "-V"],
        "environment": {"PATH": "retained"},
        "check": False,
    }


def test_p8_executor_loads_only_owned_host_environment(tmp_path):
    environment_file = tmp_path / ".env"
    environment_file.write_text(
        "REALTIME_V2_APP_DSN=host-dsn\n"
        "POLYGON_API_KEY=provider-key\n"
        "BROKER_PASSWORD=forbidden-extra\n",
        encoding="utf-8",
    )
    args = container.parse_args([
        "--component",
        "options-p8-executor",
        "--environment-file",
        str(environment_file),
        "--",
        "python",
        "-V",
    ])

    environment = container.child_environment_from_file(
        args.component,
        args.environment_file,
        {"PATH": "retained"},
    )

    assert args.command == ["python", "-V"]
    assert environment == {
        "PATH": "retained",
        "REALTIME_V2_APP_DSN": "host-dsn",
        "POLYGON_API_KEY": "provider-key",
    }


def test_docker_secret_dsn_rewrites_only_loopback_host():
    rewritten = secrets._container_dsn(
        "postgresql://reader:secret@127.0.0.1:5432/stocks_realtime_v2",
        "host.docker.internal",
    )
    retained = secrets._container_dsn(
        "postgresql://reader:secret@database.internal:5432/stocks_realtime_v2",
        "host.docker.internal",
    )

    assert "host.docker.internal" in rewritten
    assert "127.0.0.1" not in rewritten
    assert "database.internal" in retained
    assert secrets.SECRET_VARIABLES["realtime_v2_admin_dsn"] == "REALTIME_V2_ADMIN_DSN"


def test_retention_admin_secret_requires_explicit_preparation_scope(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "REALTIME_V2_APP_DSN=dbname=stocks_realtime_v2 host=localhost user=bootstrap\n"
        "REALTIME_V2_ADMIN_DSN=dbname=stocks_realtime_v2 host=localhost user=bootstrap\n"
        "REALTIME_V2_API_DSN=dbname=stocks_realtime_v2 host=localhost user=bootstrap\n",
        encoding="utf-8",
    )
    ordinary = tmp_path / "ordinary"
    retention = tmp_path / "retention"
    assert secrets.main([
        "--env-file", str(env_file),
        "--output-directory", str(ordinary),
        "--execute",
    ]) == 0
    assert {path.name for path in ordinary.iterdir()} == {
        "realtime_v2_app_dsn",
        "realtime_v2_api_dsn",
    }
    assert secrets.main([
        "--env-file", str(env_file),
        "--output-directory", str(retention),
        "--include-admin-dsn",
        "--execute",
    ]) == 0
    assert {path.name for path in retention.iterdir()} == {
        "realtime_v2_admin_dsn",
        "realtime_v2_app_dsn",
        "realtime_v2_api_dsn",
    }


def test_retention_admin_secret_can_be_derived_and_persisted_without_outputting_it(tmp_path, capsys):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DB_HOST=127.0.0.1\n"
        "DB_PORT=5432\n"
        "POSTGRES_ADMIN_USER=bootstrap\n"
        "POSTGRES_ADMIN_PASSWORD=fixture-secret\n"
        "REALTIME_V2_APP_DSN=dbname=stocks_realtime_v2 host=127.0.0.1 user=bootstrap\n"
        "REALTIME_V2_API_DSN=dbname=stocks_realtime_v2 host=127.0.0.1 user=bootstrap\n",
        encoding="utf-8",
    )
    output = tmp_path / "secrets"
    assert secrets.main([
        "--env-file", str(env_file),
        "--output-directory", str(output),
        "--include-admin-dsn",
        "--derive-admin-dsn-from-bootstrap",
        "--persist-derived-admin-dsn",
        "--container-db-host", "127.0.0.1",
        "--execute",
    ]) == 0
    admin = secrets.parse_dsn(
        (output / "realtime_v2_admin_dsn").read_text(encoding="utf-8")
    )
    persisted = secrets.dotenv_values(env_file)
    assert admin["dbname"] == "stocks_realtime_v2"
    assert admin["user"] == "bootstrap"
    assert admin["password"] == "fixture-secret"
    assert persisted["REALTIME_V2_ADMIN_DSN"]
    assert "fixture-secret" not in capsys.readouterr().out


def test_ingress_service_child_has_one_plan_and_no_env_file():
    args = ingress.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--runtime-mode",
        "legacy-diagnostic",
        "--authorization-id",
        str(AUTHORIZATION),
        "--execute",
    ])
    command = ingress._child_command(args, AUTHORIZATION, 3)

    assert "--ingress-plan-id" in command
    assert "--runtime-path-attempt" in command
    assert "--env-file" not in command


def test_ingress_service_can_bootstrap_a_ready_plan_without_resetting_identity():
    provider = MagicMock()
    cursor = provider.transaction.return_value.__enter__.return_value.cursor
    cursor.fetchone.return_value = (str(AUTHORIZATION),)

    assert ingress._active_plan_id(provider, AUTHORIZATION, datetime.now(timezone.utc)) == AUTHORIZATION
    statement = cursor.execute.call_args.args[0]
    assert "status IN ('READY','ACTIVE')" in statement
    assert "authorized_end_at>%s" in statement


def test_continuous_ingress_selects_desired_plan_without_daily_identity():
    args = ingress.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--execute",
    ])
    provider = MagicMock()
    cursor = provider.transaction.return_value.__enter__.return_value.cursor
    cursor.fetchone.return_value = (str(AUTHORIZATION),)

    assert args.authorization_id is None
    assert ingress._active_plan_id(
        provider,
        None,
        datetime.now(timezone.utc),
    ) == AUTHORIZATION
    statement = cursor.execute.call_args.args[0]
    assert "realtime_desired_session_snapshots" in statement
    assert "LIMIT 1" in statement


def test_surviving_stream_owner_is_detected_before_spawn():
    provider = MagicMock()
    cursor = provider.transaction.return_value.__enter__.return_value.cursor
    heartbeat = datetime.now(timezone.utc)
    cursor.fetchone.return_value = (
        str(UUID(int=1)),
        str(AUTHORIZATION),
        str(UUID(int=2)),
        7,
        str(UUID(int=3)),
        heartbeat,
    )

    owner = ingress._active_stream_owner(
        provider,
        checked_at=heartbeat,
        stale_after_seconds=30,
    )

    assert owner["owner_epoch"] == 7
    assert owner["plan_id"] == AUTHORIZATION
    statement = cursor.execute.call_args.args[0]
    assert "ended_at IS NULL" in statement
    assert "heartbeat_at>%s" in statement


@pytest.mark.parametrize("needs_kill", [False, True])
def test_ingress_supervisor_enforces_absolute_deadline_without_a_stop_signal(needs_kill):
    deadline = datetime(2026, 10, 19, 13, 39, 30, tzinfo=timezone.utc)
    process = MagicMock()
    process.poll.return_value = None
    process.returncode = -9 if needs_kill else 0
    if needs_kill:
        process.wait.side_effect = [subprocess.TimeoutExpired("synthetic-child", 30), 0]

    result = ingress._wait_for_child(
        process, threading.Event(), poll_seconds=1, shutdown_seconds=30,
        deadline=deadline, utc_clock=lambda: deadline,
    )

    process.terminate.assert_called_once()
    assert process.kill.call_count == int(needs_kill)
    assert result == process.returncode


def test_health_publication_fails_closed_on_any_advanced_authority():
    cursor = MagicMock()
    cursor.fetchone.side_effect = ((0,), (0,), (0,))
    args = SimpleNamespace(cohort_id=AUTHORIZATION)

    state, detail = health._publication_health(cursor, args.cohort_id)

    assert state == "READY"
    assert detail == {
        "portal_releases": 0,
        "cohort_pointers": 0,
        "platform_pointers": 0,
    }

    cursor.fetchone.side_effect = ((1,), (1,), (0,))
    assert health._publication_health(
        cursor,
        args.cohort_id,
    )[0] == "DEGRADED"

    cursor.fetchone.side_effect = ((1,), (1,), (1,))
    assert health._publication_health(
        cursor,
        args.cohort_id,
    )[0] == "BLOCKED"


def test_bridge_health_reports_missing_overlay_schema():
    cursor = MagicMock()
    cursor.fetchone.return_value = (None,)

    state, detail = health._bridge_health(cursor)

    assert state == "BLOCKED"
    assert detail == {
        "reason": "ADVANCED_OVERLAY_SCHEMA_NOT_INSTALLED",
    }


def test_bridge_health_can_scope_unrelated_quarantine_by_cohort():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("rt.advanced_journal_registry",),
        (True,),
        (0, 1, 0),
        (0,),
    )
    cohort_id = UUID("22222222-2222-2222-2222-222222222222")

    state, detail = health._bridge_health(cursor, cohort_id)

    assert state == "READY"
    assert detail["quarantined_journals"] == 0
    assert detail["draining_journals"] == 1
    assert cursor.execute.call_args_list[2].args[1] == (str(cohort_id),)
    assert cursor.execute.call_args_list[3].args[1] == (str(cohort_id),)


def _continuous_health_args():
    return SimpleNamespace(
        runtime_mode="continuous",
        component="controller",
        authorization_id=None,
        cohort_id=None,
    )


def _health_scope_row(identity, session_date):
    return (
        str(UUID(int=identity)),
        str(UUID(int=identity + 10)),
        str(UUID(int=identity + 20)),
        session_date,
    )


def test_continuous_health_reports_stalled_today_before_future_plan(
    monkeypatch,
):
    checked = datetime(2026, 11, 2, 16, 0, tzinfo=timezone.utc)
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        _health_scope_row(1, date(2026, 11, 2)),
        None,
        _health_scope_row(2, date(2026, 11, 3)),
        (
            "READY",
            "ACTIVE",
            None,
            1,
            12,
            "LIVE",
            None,
            checked - timedelta(minutes=5),
            checked - timedelta(minutes=1),
            None,
            "WRITING",
            None,
            "journal",
            date(2026, 11, 2),
        ),
    )
    monkeypatch.setenv(
        "REALTIME_V2_ADVANCED_HEALTH_STALE_SECONDS",
        "60",
    )
    monkeypatch.setattr(
        health,
        "_capability_refresh_health",
        lambda *_args, **_kwargs: ("READY", {"status": "READY"}),
    )

    state, detail = health.collect_health(
        cursor,
        _continuous_health_args(),
        checked_at=checked,
    )

    assert state == "STALE"
    assert detail["acquisition_session"] == date(2026, 11, 2)
    assert detail["future_prepared_session"] == date(2026, 11, 3)
    assert detail["reason"] == "STREAM_HEARTBEAT_STALE"


def test_continuous_health_keeps_prior_drain_before_future_prep(
    monkeypatch,
):
    checked = datetime(2026, 11, 2, 21, 1, tzinfo=timezone.utc)
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        None,
        _health_scope_row(1, date(2026, 11, 2)),
        _health_scope_row(2, date(2026, 11, 3)),
    )
    monkeypatch.setattr(
        health,
        "_capability_refresh_health",
        lambda *_args, **_kwargs: ("READY", {"status": "READY"}),
    )

    state, detail = health.collect_health(
        cursor,
        _continuous_health_args(),
        checked_at=checked,
    )

    assert state == "IDLE"
    assert detail == {
        "reason": "PROCESSING_DRAIN",
        "processing_session": date(2026, 11, 2),
        "future_prepared_session": date(2026, 11, 3),
        "capability_refresh": {"status": "READY"},
    }


def test_continuous_health_reports_no_current_plan(monkeypatch):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (None, None, None)
    monkeypatch.setattr(
        health,
        "_capability_refresh_health",
        lambda *_args, **_kwargs: ("READY", {"status": "READY"}),
    )

    state, detail = health.collect_health(
        cursor,
        _continuous_health_args(),
        checked_at=datetime(
            2026, 11, 2, 16, 0, tzinfo=timezone.utc
        ),
    )

    assert state == "IDLE"
    assert detail == {
        "reason": "NO_CURRENT_PLAN",
        "capability_refresh": {"status": "READY"},
    }


@pytest.mark.parametrize(
    ("plan_status", "terminal", "stream_status", "expected"),
    (
        ("ACTIVE", None, "LIVE", "READY"),
        ("FAILED", "FAILED_INFRASTRUCTURE", None, "DEGRADED"),
        ("COMPLETE", "SESSION_COMPLETE", "STOPPED", "TERMINAL"),
    ),
)
def test_continuous_provider_health_distinguishes_runtime_states(
    plan_status,
    terminal,
    stream_status,
    expected,
):
    checked = datetime(2026, 11, 2, 16, 0, tzinfo=timezone.utc)
    plan = health.ContinuousHealthPlan(
        UUID(int=1),
        UUID(int=2),
        UUID(int=3),
        date(2026, 11, 2),
    )
    scope = health.ContinuousHealthScope(plan, None, None)
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        "READY",
        plan_status,
        terminal,
        1,
        12,
        stream_status,
        None,
        checked,
        checked,
        None,
        "WRITING",
        None,
        "journal",
        date(2026, 11, 2),
    )

    state, _detail = health._provider_plan_health(
        cursor,
        plan.plan_id,
        checked_at=checked,
        scope=scope,
    )

    assert state == expected


def test_capability_refresh_health_exposes_owner_and_retry_state():
    checked = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (
            str(UUID(int=1)),
            4,
            checked + timedelta(seconds=30),
            checked,
        ),
        (
            "DEGRADED",
            ["CAPABILITY_PROVIDER_UNAVAILABLE"],
            checked,
            {
                "last_success_at": "2026-11-09T11:00:00+00:00",
                "next_retry_at": "2026-11-09T12:00:15+00:00",
                "request_identity_sha256": "a" * 64,
            },
        ),
    )

    state, detail = health._capability_refresh_health(
        cursor,
        checked_at=checked,
    )

    assert state == "DEGRADED"
    assert detail["owner_epoch"] == 4
    assert detail["heartbeat_at"] == checked
    assert detail["last_success_at"] == "2026-11-09T11:00:00+00:00"
    assert detail["next_retry_at"] == "2026-11-09T12:00:15+00:00"


def test_capability_refresh_health_reports_ttl_waiting_not_failure():
    checked = datetime(2026, 11, 7, 16, 0, tzinfo=timezone.utc)
    next_check = "2026-11-09T19:00:00+00:00"
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (
            str(UUID(int=1)),
            4,
            checked + timedelta(seconds=30),
            checked,
        ),
        (
            "DEGRADED",
            ["CAPABILITY_PROBE_NOT_BEFORE"],
            checked,
            {
                "state": "WAITING",
                "last_success_at": None,
                "next_retry_at": next_check,
                "request_identity_sha256": "a" * 64,
            },
        ),
    )

    state, detail = health._capability_refresh_health(
        cursor,
        checked_at=checked,
    )

    assert state == "WAITING"
    assert detail["status"] == "WAITING"
    assert detail["next_retry_at"] == next_check


def test_scheduler_health_scopes_runtime_query_to_current_session():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        1,
        0,
        datetime(2026, 11, 2, 16, 0, tzinfo=timezone.utc),
    )

    state, detail = health._scheduler_health(
        cursor,
        cohort_id=UUID(int=1),
        session_date=date(2026, 11, 2),
    )

    assert state == "READY"
    assert detail["historical_failures_excluded"] is True
    statement = cursor.execute.call_args.args[0]
    assert "service.session_date=%s" in statement
    assert cursor.execute.call_args.args[1] == (
        str(UUID(int=1)),
        str(UUID(int=1)),
        date(2026, 11, 2),
    )


def _service_block(text, service):
    match = re.search(
        rf"^  {re.escape(service)}:\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:\n|^volumes:\n)",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None, service
    return match.group("body")


def test_compose_advanced_services_are_independent_and_secret_scoped():
    standalone = (
        ROOT / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")
    text = standalone
    services = {
        "realtime-v2-advanced-controller",
        "realtime-v2-advanced-capability-refresh",
        "realtime-v2-advanced-ingress",
        "realtime-v2-advanced-bridge",
        "realtime-v2-advanced-scheduler",
        "realtime-v2-advanced-finalizer",
        "realtime-v2-advanced-detectors",
        "realtime-v2-advanced-api",
        "realtime-v2-advanced-frontend",
    }
    blocks = {
        service: _service_block(text, service)
        for service in services
    }

    assert (
        "image: ${REALTIME_V2_BACKEND_IMAGE:"
        "?set REALTIME_V2_BACKEND_IMAGE to an approved immutable image}"
        in standalone
    )
    assert "alpha-research-realtime-v2-backend:latest" not in standalone
    for service in (
        "realtime-v2-advanced-controller",
        "realtime-v2-advanced-ingress",
        "realtime-v2-advanced-bridge",
    ):
        assert (
            "realtime_v2_advanced_journals:"
            "/var/lib/realtime-v2-advanced"
            in blocks[service]
        )
    assert "realtime-v2-advanced-provider" in blocks[
        "realtime-v2-advanced-controller"
    ]
    assert "realtime-v2-advanced-provider" in blocks[
        "realtime-v2-advanced-ingress"
    ]
    assert "realtime-v2-advanced-provider" in blocks[
        "realtime-v2-advanced-capability-refresh"
    ]
    for service in services - {
        "realtime-v2-advanced-controller",
        "realtime-v2-advanced-ingress",
        "realtime-v2-advanced-capability-refresh",
    }:
        assert "realtime-v2-advanced" in blocks[service]
    assert "realtime_v2_provider_key" in blocks[
        "realtime-v2-advanced-controller"
    ]
    assert "realtime_v2_provider_key" in blocks[
        "realtime-v2-advanced-ingress"
    ]
    assert "realtime_v2_provider_key" in blocks[
        "realtime-v2-advanced-capability-refresh"
    ]
    for service in services - {
        "realtime-v2-advanced-controller",
        "realtime-v2-advanced-ingress",
        "realtime-v2-advanced-capability-refresh",
    }:
        assert "realtime_v2_provider_key" not in blocks[service]
    for block in blocks.values():
        assert "env_file:" not in block
        assert "healthcheck:" in block
    for service in {
        "realtime-v2-advanced-controller",
        "realtime-v2-advanced-capability-refresh",
        "realtime-v2-advanced-ingress",
        "realtime-v2-advanced-bridge",
        "realtime-v2-advanced-scheduler",
        "realtime-v2-advanced-finalizer",
    }:
        block = blocks[service]
        for selector in (
            "REALTIME_V2_ADVANCED_AUTHORIZATION_ID",
            "REALTIME_V2_ADVANCED_COHORT_ID",
            "REALTIME_V2_ADVANCED_SESSION_DATE",
            "REALTIME_V2_ADVANCED_FIRST_CUTOFF",
            "REALTIME_V2_ADVANCED_AUTHORIZATION_REFERENCE",
        ):
            assert selector not in block
    assert all(
        "<<: *backend-service" in blocks[service]
        for service in services
        if service != "realtime-v2-advanced-frontend"
    )
    assert "restart: unless-stopped" in blocks[
        "realtime-v2-advanced-frontend"
    ]
    assert "VITE_API_BASE_URL: /api" in blocks[
        "realtime-v2-advanced-frontend"
    ]
    assert 'VITE_REALTIME_ONLY: "true"' in blocks[
        "realtime-v2-advanced-frontend"
    ]
    assert "VITE_REALTIME_COHORT_ID: ${REALTIME_V2_ADVANCED_COHORT_ID" in blocks[
        "realtime-v2-advanced-frontend"
    ]
    assert "extends:" not in standalone
    assert "services.backend.environment" not in standalone
    assert {
        f"  {service}:"
        for service in services
    } <= {
        line
        for line in standalone.splitlines()
        if line.startswith("  ") and line.endswith(":")
    }
    nginx = (
        ROOT / "frontend" / "nginx.realtime-v2.conf"
    ).read_text(encoding="utf-8")
    assert "resolver 127.0.0.11" in nginx
    assert "proxy_pass http://$realtime_v2_api" in nginx


@pytest.mark.parametrize(
    "relative",
    [
        "backend/scripts/run_realtime_v2_advanced_controller.py",
        "backend/scripts/run_realtime_v2_advanced_capability_refresh.py",
        "backend/scripts/run_realtime_v2_advanced_equity_ingress.py",
        "backend/scripts/run_realtime_v2_advanced_equity_scheduler.py",
        "backend/scripts/run_realtime_v2_advanced_equity_window.py",
        "backend/scripts/run_realtime_v2_advanced_journal_replay.py",
        "backend/scripts/run_realtime_v2_advanced_equity_bridge.py",
        "backend/scripts/run_realtime_v2_equity_interval_finalizer.py",
        "backend/scripts/run_realtime_v2_advanced_options_os_detectors.py",
        "backend/scripts/run_realtime_v2_portal_api.py",
        "backend/scripts/run_realtime_v2_retention_service.py",
    ],
)
def test_direct_container_entrypoints_do_not_load_dotenv(relative):
    source = (ROOT / relative).read_text(encoding="utf-8")

    assert "load_dotenv" not in source
    assert "dotenv_values" not in source
    assert 'default=BACKEND_DIR / ".env"' not in source
