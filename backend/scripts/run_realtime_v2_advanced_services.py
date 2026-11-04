#!/usr/bin/env python3
"""Run the consolidated long-lived Advanced Realtime V3 topology."""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import psutil
from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


WORKSPACE_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = WORKSPACE_DIR / "backend"
PYTHON = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
NPM = "npm.cmd"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
)
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    ADVANCED_REALTIME_V3_ROLES,
    ADVANCED_REALTIME_V3_RUNTIME_RESOURCE,
    advanced_realtime_v3_topology_payload,
    role_restart_delay,
)
from realtime.advanced_realtime_fencing import (  # noqa: E402
    DurableLeaseUnavailable,
    PostgresLeaseHeartbeat,
    RUNTIME_GROUP_RESOURCE,
    role_resource_key,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402
from realtime.provider_socket_lease import (  # noqa: E402
    HostProviderSocketLease,
)
from scripts.run_realtime_v2_services import (  # noqa: E402
    _start,
    _stop,
)
from scripts.run_realtime_v2_retention_service import (  # noqa: E402
    ARCHIVE_FIRST_RELATIONS,
    ELIGIBLE_RELATIONS,
    PARTITIONED_RELATIONS,
    SESSION_KINDS,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runtime-mode",
        choices=("continuous", "legacy-diagnostic"),
        default="continuous",
    )
    parser.add_argument("--cohort-id", type=UUID)
    parser.add_argument("--authorization-id", type=UUID)
    parser.add_argument("--universe-target", type=int)
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--first-cutoff", type=datetime.fromisoformat)
    parser.add_argument("--authorization-reference")
    parser.add_argument(
        "--decoded-byte-ceiling",
        type=int,
        default=1024 * 1024 * 1024,
    )
    parser.add_argument("--stream-sessions", type=int, default=4)
    parser.add_argument("--connection-attempts", type=int, default=12)
    parser.add_argument("--rest-repair-requests", type=int, default=0)
    parser.add_argument(
        "--rest-repair-decoded-bytes",
        type=int,
        default=0,
    )
    parser.add_argument("--finality-delay-seconds", type=int, default=5)
    parser.add_argument("--api-port", type=int, default=8002)
    parser.add_argument("--frontend-port", type=int, default=5174)
    parser.add_argument("--run-directory", type=Path)
    parser.add_argument("--retention-directory", type=Path)
    parser.add_argument("--runtime-lock-path", type=Path)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    explicit = (
        args.cohort_id,
        args.authorization_id,
        args.universe_target,
        args.session_date,
        args.first_cutoff,
        args.authorization_reference,
    )
    if args.runtime_mode == "continuous":
        if any(value is not None for value in explicit):
            parser.error(
                "continuous Advanced services derive cohort, authorization, "
                "session and cutoff; use --runtime-mode legacy-diagnostic "
                "for explicit scope"
            )
        if args.rest_repair_requests or args.rest_repair_decoded_bytes:
            parser.error(
                "continuous Advanced services derive repair budgets"
            )
    elif any(value is None for value in explicit):
        parser.error(
            "legacy diagnostic services require the complete explicit scope"
        )
    if args.first_cutoff is not None and (
        args.first_cutoff.tzinfo is None
        or args.first_cutoff.utcoffset() is None
    ):
        parser.error("--first-cutoff must be timezone-aware")
    if (
        args.authorization_reference is not None
        and not args.authorization_reference.strip()
    ):
        parser.error("--authorization-reference cannot be blank")
    if (
        args.decoded_byte_ceiling <= 0
        or args.stream_sessions <= 0
        or args.connection_attempts < args.stream_sessions
        or args.rest_repair_requests < 0
        or args.rest_repair_decoded_bytes < 0
        or (args.rest_repair_requests == 0)
        != (args.rest_repair_decoded_bytes == 0)
    ):
        parser.error("Advanced controller budgets are invalid")
    if not 1 <= args.finality_delay_seconds <= 60:
        parser.error("--finality-delay-seconds must be in [1, 60]")
    if args.api_port == args.frontend_port or min(
        args.api_port,
        args.frontend_port,
    ) <= 0:
        parser.error("API and frontend ports must be distinct positive values")
    if args.run_directory is None:
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        args.run_directory = (
            BACKEND_DIR
            / ".realtime-canary"
            / "v2-advanced-services"
            / stamp
        )
    args.run_directory = args.run_directory.resolve()
    if args.retention_directory is None:
        args.retention_directory = (
            Path(os.environ.get("LOCALAPPDATA", Path.home()))
            / "StockScreener"
            / "advanced-realtime-v3"
            / "retention"
        )
    args.retention_directory = args.retention_directory.resolve()
    if args.runtime_lock_path is None:
        args.runtime_lock_path = (
            Path(os.environ.get("LOCALAPPDATA", Path.home()))
            / "StockScreener"
            / "locks"
            / "advanced-realtime-v3-runtime.lock"
        )
    args.runtime_lock_path = args.runtime_lock_path.resolve()
    if args.run_directory.exists():
        parser.error("--run-directory must not already exist")
    if not args.execute:
        parser.error("Advanced services require --execute")
    return args


def build_commands(args):
    run = args.run_directory
    commands = {
        "api": [
            str(PYTHON),
            "-B",
            "-m",
            "uvicorn",
            "scripts.run_realtime_v2_portal_api:create_app",
            "--factory",
            "--app-dir",
            "backend",
            "--host",
            "127.0.0.1",
            "--port",
            str(args.api_port),
        ],
        "frontend": [
            NPM,
            "--prefix",
            "frontend",
            "run",
            "dev",
            "--",
            "--host",
            "127.0.0.1",
            "--port",
            str(args.frontend_port),
        ],
        "finalizer": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_equity_interval_finalizer.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--security-mode",
            "hardened",
            "--desired-state",
            "--continuous",
            "--poll-seconds",
            "1",
            "--claim-limit",
            "10",
            "--execute",
        ],
        "bridge": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_journal_replay.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--continuous",
            "--execute",
        ],
        "controller": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_controller.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--runtime-mode",
            args.runtime_mode,
            "--stream-sessions",
            str(args.stream_sessions),
            "--connection-attempts",
            str(args.connection_attempts),
            "--decoded-bytes",
            str(args.decoded_byte_ceiling),
            "--rest-repair-requests",
            str(args.rest_repair_requests),
            "--rest-repair-decoded-bytes",
            str(args.rest_repair_decoded_bytes),
            "--catch-up-mode",
            "AUTO" if args.runtime_mode == "continuous" else "DISABLED",
            "--external-ingress",
            "--execute",
        ],
        "capability-refresh": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_capability_refresh.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--execute",
        ],
        "equity-preparation": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_equity_preparation.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--execute",
        ],
        "equity-ingress": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_ingress_service.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--runtime-mode",
            args.runtime_mode,
            "--execute",
        ],
        "scheduler": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_equity_scheduler.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--runtime-mode",
            args.runtime_mode,
            "--finality-delay-seconds",
            str(args.finality_delay_seconds),
            "--publication-mode",
            "continuous-publication",
            "--report-directory",
            str(run / "window-reports"),
            "--continuous",
            "--execute",
        ],
        "detectors": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_advanced_options_os_detectors.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--dynamic-scope",
            "--continuous",
            "--execute",
        ],
        "canonical-stock-alerts": [
            str(PYTHON),
            "-B",
            "-u",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_canonical_stock_alerts.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--dynamic-scope",
            "--operation",
            "PRIMARY_SERVING",
            "--continuous",
            "--execute",
        ],
        "publication-retry": [
            str(PYTHON),
            "-B",
            "-u",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_portal_publication_retry.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--continuous",
            "--execute",
        ],
        "options-lifecycle": [
            str(PYTHON),
            "-B",
            "-u",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_advanced_options_v3_lifecycle.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--continuous",
            "--execute",
        ],
        "options-acquisition": [
            str(PYTHON),
            "-B",
            "-u",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_advanced_options_v3_acquisition.py"
            ),
            "run",
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--state-source",
            "postgres",
            "--continuous",
            "--execute",
        ],
        "options-scheduler": [
            str(PYTHON),
            "-B",
            str(BACKEND_DIR / "scripts" / "run_advanced_options_v3_scheduler.py"),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--continuous",
            "--execute",
        ],
        **{
            role: [
                str(PYTHON),
                "-B",
                str(
                    BACKEND_DIR
                    / "scripts"
                    / "run_advanced_options_v3_production.py"
                ),
                "--docker-service-kind",
                kind,
                "--state-source",
                "postgres",
                "--continuous",
            ]
            for role, kind in (
                ("options-ingress", "INGRESS"),
                ("options-market-state", "MARKET_STATE"),
                ("options-flow", "FLOW"),
                ("options-window", "WINDOW"),
            )
        },
        "retention": [
            str(PYTHON),
            "-B",
            str(
                BACKEND_DIR
                / "scripts"
                / "run_realtime_v2_retention_service.py"
            ),
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--archive-root",
            str(args.retention_directory / "archive"),
            "--status-path",
            str(args.retention_directory / "status.json"),
            *[
                value
                for relation in PARTITIONED_RELATIONS
                for value in ("--partition-relation", relation)
            ],
            *[
                value
                for relation in ELIGIBLE_RELATIONS
                for value in ("--retention-relation", relation)
            ],
            *[
                value
                for relation in ARCHIVE_FIRST_RELATIONS
                for value in ("--archive-relation", relation)
            ],
            *[
                value
                for kind in SESSION_KINDS
                for value in ("--archive-kind", kind)
            ],
            "--archive-build-id",
            "advanced-realtime-v3",
            "--archive-policy-id",
            "retention-policy-v1",
            "--authorization-reference",
            "advanced-realtime-v3-ordinary-maintenance",
            "--continuous",
            "--execute",
        ],
    }
    if args.runtime_mode == "legacy-diagnostic":
        for name in (
            "capability-refresh",
            "equity-preparation",
            "detectors",
            "canonical-stock-alerts",
            "publication-retry",
            "options-lifecycle",
            "options-acquisition",
            "options-scheduler",
            "options-ingress",
            "options-market-state",
            "options-flow",
            "options-window",
            "retention",
        ):
            commands.pop(name)
        controller_scope = (
            commands["controller"].index("--runtime-mode") + 2
        )
        commands["controller"][controller_scope:controller_scope] = [
            "--authorization-id",
            str(args.authorization_id),
            "--universe-target",
            str(args.universe_target),
        ]
        commands["equity-ingress"][-1:-1] = [
            "--authorization-id",
            str(args.authorization_id),
        ]
        scheduler_scope = (
            commands["scheduler"].index("--runtime-mode") + 2
        )
        commands["scheduler"][scheduler_scope:scheduler_scope] = [
            "--authorization-id",
            str(args.authorization_id),
            "--cohort-id",
            str(args.cohort_id),
            "--session-date",
            args.session_date.isoformat(),
            "--first-cutoff",
            args.first_cutoff.isoformat(),
            "--authorization-reference",
            args.authorization_reference,
        ]
        finalizer_execute = commands["finalizer"].index("--desired-state")
        commands["finalizer"][finalizer_execute:finalizer_execute + 1] = [
            "--cohort-id",
            str(args.cohort_id),
        ]
    if args.runtime_mode == "continuous":
        advanced_realtime_v3_topology_payload(commands)
    return commands


def process_environment(
    environment: dict[str, str],
    process_name: str,
) -> dict[str, str]:
    scoped = dict(environment)
    allowed = {
        "controller": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "capability-refresh": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "equity-preparation": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "equity-ingress": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "options-lifecycle": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "options-acquisition": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "bridge": {"REALTIME_V2_APP_DSN"},
        "finalizer": {"REALTIME_V2_APP_DSN"},
        "scheduler": {"REALTIME_V2_APP_DSN"},
        "detectors": {"REALTIME_V2_APP_DSN"},
        "canonical-stock-alerts": {"REALTIME_V2_APP_DSN"},
        "publication-retry": {"REALTIME_V2_APP_DSN"},
        "options-ingress": {"REALTIME_V2_APP_DSN"},
        "options-market-state": {"REALTIME_V2_APP_DSN"},
        "options-flow": {"REALTIME_V2_APP_DSN"},
        "options-window": {"REALTIME_V2_APP_DSN"},
        "retention": {
            "REALTIME_V2_APP_DSN",
            "REALTIME_V2_ADMIN_DSN",
        },
        "api": {"REALTIME_V2_API_DSN"},
        "frontend": set(),
    }
    if process_name not in allowed:
        raise ValueError("unknown Advanced service process")
    for name in (
        "POLYGON_API_KEY",
        "REALTIME_V2_APP_DSN",
        "REALTIME_V2_API_DSN",
        "REALTIME_V2_ADMIN_DSN",
    ):
        if name not in allowed[process_name]:
            scoped.pop(name, None)
    if process_name.startswith("options-"):
        scoped.update({
            "REALTIME_V2_EQUITY_PIPELINE": "DISABLED",
            "REALTIME_V2_OPTIONS_PIPELINE": "MASSIVE_ADVANCED",
            "REALTIME_V3_OPTIONS_STATE_SOURCE": "postgres",
            "REALTIME_V3_ACQUISITION_SPLIT": "ENABLED",
        })
    elif process_name not in {"api", "frontend", "retention"}:
        scoped.update({
            "REALTIME_V2_EQUITY_PIPELINE": "MASSIVE_ADVANCED",
            "REALTIME_V2_OPTIONS_PIPELINE": "DISABLED",
        })
    return scoped


def _capability_restart_delay(attempt: int) -> int:
    return role_restart_delay(attempt)


@dataclass(slots=True)
class _RuntimeRole:
    name: str
    child: object | None = None
    state: str = "STARTING"
    restart_attempts: int = 0
    next_restart_at: float | None = 0.0
    last_exit_code: int | None = None
    last_error: str | None = None


class _RoleConfigurationError(ValueError):
    pass


def _schedule_role_start(
    role: _RuntimeRole,
    *,
    now: float,
    starter,
) -> None:
    try:
        role.child = starter(role.name)
    except _RoleConfigurationError as error:
        role.restart_attempts += 1
        role.state = "FAILED_TERMINAL"
        role.next_restart_at = None
        role.last_error = str(error)
        return
    except OSError as error:
        role.restart_attempts += 1
        delay = role_restart_delay(role.restart_attempts)
        role.state = "DEGRADED"
        role.next_restart_at = now + delay
        role.last_error = (
            f"{type(error).__name__}: {error}; retrying in {delay}s"
        )
        return
    role.state = "RUNNING"
    role.next_restart_at = None
    role.last_error = None


def _record_role_exit(
    role: _RuntimeRole,
    *,
    exit_code: int,
    now: float,
    log_path: Path,
) -> None:
    role.child = None
    role.restart_attempts += 1
    role.last_exit_code = exit_code
    if exit_code in {2, 64, 78}:
        role.state = "FAILED_TERMINAL"
        role.next_restart_at = None
        role.last_error = (
            f"configuration exit code {exit_code}; see {log_path}"
        )
        return
    delay = role_restart_delay(role.restart_attempts)
    role.next_restart_at = now + delay
    role.state = "DEGRADED"
    role.last_error = (
        f"exit code {exit_code}; retrying in {delay}s; "
        f"see {log_path}"
    )


def _write_runtime_status(
    path: Path,
    roles: dict[str, _RuntimeRole],
    *,
    status: str,
) -> None:
    document = {
        "contract": "advanced-realtime-v3-native-status-v1",
        "status": status,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "process_id": os.getpid(),
        "roles": {
            name: {
                "state": role.state,
                "process_id": (
                    None
                    if role.child is None
                    else role.child.process.pid
                ),
                "restart_attempts": role.restart_attempts,
                "next_restart_monotonic": role.next_restart_at,
                "last_exit_code": role.last_exit_code,
                "last_error": role.last_error,
            }
            for name, role in sorted(roles.items())
        },
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, path)


def _surviving_children(commands: dict[str, list[str]]) -> tuple[dict, ...]:
    own_processes = {
        os.getpid(),
        *(parent.pid for parent in psutil.Process().parents()),
    }
    markers = {
        Path(part).name
        for command in commands.values()
        for part in command
        if part.endswith(".py")
    }
    survivors = []
    for process in psutil.process_iter(("pid", "cmdline", "create_time")):
        if process.pid in own_processes:
            continue
        try:
            command = tuple(process.info.get("cmdline") or ())
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
        matched = sorted(
            marker
            for marker in markers
            if any(Path(part).name == marker for part in command)
        )
        if matched:
            survivors.append({
                "process_id": process.pid,
                "created_at": process.info.get("create_time"),
                "components": matched,
            })
    return tuple(survivors)


def main(argv=None):
    args = parse_args(argv)
    file_environment = {
        key: value
        for key, value in dotenv_values(args.env_file).items()
        if value is not None
    }
    config = RealtimeV2PipelineConfig.from_mapping({
        **file_environment,
        **os.environ,
    })
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced supervisor requires Advanced equity and disabled options"
        )
    if (
        config.advanced_journal_dir is None
        or not config.advanced_journal_dir.is_dir()
    ):
        raise RuntimeError("Advanced journal directory is unavailable")
    commands = build_commands(args)
    plan = {
        "mode": args.runtime_mode,
        "pipeline": config.equity_pipeline.value,
        "options_pipeline": "MASSIVE_ADVANCED",
        "publication_enabled": True,
        "publication_mode": "DURABLE_RETRY_NO_DELIVERY",
        "topology": (
            advanced_realtime_v3_topology_payload(commands)
            if args.runtime_mode == "continuous"
            else None
        ),
        "commands": commands,
    }
    args.run_directory.mkdir(parents=True)
    args.retention_directory.mkdir(parents=True, exist_ok=True)
    (args.run_directory / "launch-plan.json").write_text(
        json.dumps(plan, indent=2) + "\n",
        encoding="utf-8",
    )
    environment = {**file_environment, **os.environ}
    environment["REALTIME_V2_ADVANCED_JOURNAL_DIR"] = str(
        config.advanced_journal_dir
    )
    environment["REALTIME_V2_P8_STATUS_DIR"] = str(
        args.run_directory / "options-status"
    )
    environment["REALTIME_V3_OPTIONS_ACQUISITION_STATUS_PATH"] = str(
        args.run_directory / "options-acquisition-status.json"
    )
    environment["REALTIME_V3_OPTIONS_ACQUISITION_LOCK_PATH"] = str(
        args.runtime_lock_path.with_name(
            "advanced-realtime-v3-options-provider.lock"
        )
    )
    frontend_environment = process_environment(environment, "frontend")
    frontend_environment.update({
        "VITE_API_PROXY_TARGET": f"http://127.0.0.1:{args.api_port}",
        "VITE_REALTIME_ONLY": "true",
    })
    roles = {
        name: _RuntimeRole(name)
        for name in commands
    }
    exit_code = 0
    status_path = args.run_directory / "status.json"
    runtime_lease = HostProviderSocketLease(
        args.runtime_lock_path,
        ADVANCED_REALTIME_V3_RUNTIME_RESOURCE,
        uuid4(),
    )
    fence_pool = ThreadedConnectionPool(
        1,
        8,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
    fence_provider = V2ConnectionProvider(
        {APP_ROLE: fence_pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    fence_provider.verify()
    runtime_owner_id = uuid4()
    runtime_stop = threading.Event()
    process_identity = f"native:{os.getpid()}:{args.run_directory}"
    group_fence = PostgresLeaseHeartbeat(
        fence_provider,
        resource_key=RUNTIME_GROUP_RESOURCE,
        owner_id=runtime_owner_id,
        runtime_kind="NATIVE",
        role_name="runtime",
        process_identity=process_identity,
        stop_event=runtime_stop,
    )
    role_fences: dict[str, PostgresLeaseHeartbeat] = {}

    def start_role(name: str):
        heartbeat = role_fences.get(name)
        if heartbeat is None:
            heartbeat = PostgresLeaseHeartbeat(
                fence_provider,
                resource_key=role_resource_key(name),
                owner_id=runtime_owner_id,
                runtime_kind="NATIVE",
                role_name=name,
                process_identity=process_identity,
            )
            try:
                heartbeat.start()
            except DurableLeaseUnavailable as error:
                raise OSError(str(error)) from error
            role_fences[name] = heartbeat
        try:
            role_environment = (
                frontend_environment
                if name == "frontend"
                else process_environment(environment, name)
            )
        except ValueError as error:
            raise _RoleConfigurationError(str(error)) from error
        child = _start(
            name,
            commands[name],
            environment=role_environment,
            run_directory=args.run_directory,
        )
        heartbeat.mark_running()
        return child

    def publish_role_state(role: _RuntimeRole) -> None:
        heartbeat = role_fences.get(role.name)
        if heartbeat is None or heartbeat.lost:
            return
        health = {
            "STARTING": "STARTING",
            "RUNNING": "HEALTHY",
            "DEGRADED": "DEGRADED",
            "FAILED_TERMINAL": "UNHEALTHY",
            "DRAINING": "DEGRADED",
            "STOPPED": "STOPPED",
        }[role.state]
        heartbeat.mark_state(
            role.state,
            health,
            detail={
                "restart_attempts": role.restart_attempts,
                "last_exit_code": role.last_exit_code,
                "last_error": role.last_error,
            },
        )

    try:
        group_fence.start()
        runtime_lease.acquire()
        survivors = _surviving_children(commands)
        if survivors:
            raise RuntimeError(
                "Advanced V3 surviving children require operator review: "
                + json.dumps(survivors, sort_keys=True)
            )
        group_fence.mark_running()
        ordered = [
            role.name
            for role in ADVANCED_REALTIME_V3_ROLES
            if role.name in commands
        ]
        ordered.extend(name for name in commands if name not in ordered)
        initial_now = time.monotonic()
        for name in ordered:
            _schedule_role_start(
                roles[name],
                now=initial_now,
                starter=start_role,
            )
            publish_role_state(roles[name])
        initial_status = (
            "DEGRADED"
            if any(
                role.state in {"DEGRADED", "FAILED_TERMINAL"}
                for role in roles.values()
            )
            else "RUNNING"
        )
        _write_runtime_status(
            status_path,
            roles,
            status=initial_status,
        )
        print(
            json.dumps(
                {
                    "status": "RUNNING",
                    "pipeline": "MASSIVE_ADVANCED",
                    "run_directory": str(args.run_directory),
                    "pids": {
                        name: role.child.process.pid
                        for name, role in roles.items()
                        if role.child is not None
                    },
                },
                indent=2,
            )
        )
        while True:
            if runtime_stop.is_set() or group_fence.lost:
                raise RuntimeError(
                    "Advanced V3 durable runtime fence was lost"
                )
            now = time.monotonic()
            for role in roles.values():
                heartbeat = role_fences.get(role.name)
                if heartbeat is not None and heartbeat.lost:
                    if role.child is not None:
                        _stop(role.child)
                        role.child = None
                    heartbeat.close()
                    role_fences.pop(role.name, None)
                    role.restart_attempts += 1
                    delay = role_restart_delay(
                        role.restart_attempts
                    )
                    role.state = "DEGRADED"
                    role.next_restart_at = now + delay
                    role.last_error = (
                        "durable role fence lost; "
                        f"retrying in {delay}s"
                    )
                    continue
                if (
                    role.child is None
                    and role.next_restart_at is not None
                    and now >= role.next_restart_at
                ):
                    _schedule_role_start(
                        role,
                        now=now,
                        starter=start_role,
                    )
                    publish_role_state(role)
                    continue
                if role.child is None:
                    continue
                result = role.child.process.poll()
                if result is None:
                    continue
                role.child.output.close()
                _record_role_exit(
                    role,
                    exit_code=result,
                    now=now,
                    log_path=(
                        args.run_directory / (role.name + ".log")
                    ),
                )
                print(
                    f"Advanced V3 {role.name} degraded: "
                    f"{role.last_error}",
                    file=sys.stderr,
                )
            overall = (
                "DEGRADED"
                if any(
                    role.state in {"DEGRADED", "FAILED_TERMINAL"}
                    for role in roles.values()
                )
                else "RUNNING"
            )
            _write_runtime_status(status_path, roles, status=overall)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Draining Advanced Realtime V3 services...")
    except Exception as error:
        print(
            f"Advanced V3 supervisor failed: {error}",
            file=sys.stderr,
        )
        exit_code = 1
    finally:
        _write_runtime_status(status_path, roles, status="DRAINING")
        for role in reversed(tuple(roles.values())):
            if role.child is not None:
                _stop(role.child)
                role.child = None
                role.state = "STOPPED"
            heartbeat = role_fences.pop(role.name, None)
            if heartbeat is not None:
                heartbeat.close()
        _write_runtime_status(status_path, roles, status="STOPPED")
        runtime_lease.release()
        group_fence.close()
        fence_pool.closeall()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
