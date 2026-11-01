#!/usr/bin/env python3
"""Supervise the unpublished Advanced equity V2 process graph."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values


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
from scripts.run_realtime_v2_services import (  # noqa: E402
    _start,
    _stop,
    _wait_http,
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
        "preparation": [
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
        "ingress": [
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
            "projection-only",
            "--report-directory",
            str(run / "window-reports"),
            "--continuous",
            "--execute",
        ],
    }
    if args.runtime_mode == "legacy-diagnostic":
        commands.pop("capability-refresh")
        commands.pop("preparation")
        controller_scope = (
            commands["controller"].index("--runtime-mode") + 2
        )
        commands["controller"][controller_scope:controller_scope] = [
            "--authorization-id",
            str(args.authorization_id),
            "--universe-target",
            str(args.universe_target),
        ]
        commands["ingress"][-1:-1] = [
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
        "preparation": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "ingress": {
            "POLYGON_API_KEY",
            "REALTIME_V2_APP_DSN",
        },
        "bridge": {"REALTIME_V2_APP_DSN"},
        "finalizer": {"REALTIME_V2_APP_DSN"},
        "scheduler": {"REALTIME_V2_APP_DSN"},
        "api": {"REALTIME_V2_API_DSN"},
        "frontend": set(),
    }
    if process_name not in allowed:
        raise ValueError("unknown Advanced service process")
    for name in (
        "POLYGON_API_KEY",
        "REALTIME_V2_APP_DSN",
        "REALTIME_V2_API_DSN",
    ):
        if name not in allowed[process_name]:
            scoped.pop(name, None)
    return scoped


def _capability_restart_delay(attempt: int) -> int:
    if type(attempt) is not int or attempt < 1:
        raise ValueError("capability restart attempt must be positive")
    return min(60, 2 ** min(attempt - 1, 6))


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
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
        "options_pipeline": config.options_pipeline.value,
        "publication_enabled": False,
        "publication_mode": "PROJECTION_ONLY",
        "commands": commands,
    }
    args.run_directory.mkdir(parents=True)
    (args.run_directory / "launch-plan.json").write_text(
        json.dumps(plan, indent=2) + "\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    frontend_environment = process_environment(environment, "frontend")
    frontend_environment.update({
        "VITE_API_PROXY_TARGET": f"http://127.0.0.1:{args.api_port}",
        "VITE_REALTIME_ONLY": "true",
    })
    if args.cohort_id is not None:
        frontend_environment["VITE_REALTIME_COHORT_ID"] = str(
            args.cohort_id
        )
    children = []
    exit_code = 0
    capability_restart_attempt = 0
    capability_restart_at = None
    try:
        for name in (
            "capability-refresh",
            "preparation",
            "controller",
            "ingress",
            "bridge",
            "finalizer",
            "scheduler",
            "api",
            "frontend",
        ):
            if name not in commands:
                continue
            children.append(
                _start(
                    name,
                    commands[name],
                    environment=(
                        frontend_environment
                        if name == "frontend"
                        else process_environment(environment, name)
                    ),
                    run_directory=args.run_directory,
                )
            )
        _wait_http(
            f"http://127.0.0.1:{args.api_port}/api/realtime/health"
        )
        _wait_http(f"http://127.0.0.1:{args.frontend_port}/")
        print(
            json.dumps(
                {
                    "status": "RUNNING_UNPUBLISHED",
                    "pipeline": "MASSIVE_ADVANCED",
                    "run_directory": str(args.run_directory),
                    "pids": {
                        child.name: child.process.pid
                        for child in children
                    },
                },
                indent=2,
            )
        )
        while True:
            if (
                "capability-refresh" in commands
                and not any(
                    child.name == "capability-refresh"
                    for child in children
                )
                and capability_restart_at is not None
                and time.monotonic() >= capability_restart_at
            ):
                children.append(
                    _start(
                        "capability-refresh",
                        commands["capability-refresh"],
                        environment=process_environment(
                            environment,
                            "capability-refresh",
                        ),
                        run_directory=args.run_directory,
                    )
                )
                capability_restart_at = None
            for child in tuple(children):
                result = child.process.poll()
                if result is None:
                    continue
                if child.name == "capability-refresh":
                    children.remove(child)
                    child.output.close()
                    capability_restart_attempt += 1
                    delay = _capability_restart_delay(
                        capability_restart_attempt
                    )
                    capability_restart_at = time.monotonic() + delay
                    print(
                        "Advanced capability refresh exited with "
                        f"code {result}; retrying in {delay}s",
                        file=sys.stderr,
                    )
                    continue
                if child.name == "scheduler" and result == 0:
                    children.remove(child)
                    child.output.close()
                    continue
                raise RuntimeError(
                    f"{child.name} exited unexpectedly with code {result}; "
                    f"see {args.run_directory / (child.name + '.log')}"
                )
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping Advanced V2 services...")
    except Exception as error:
        print(
            f"Advanced V2 supervisor failed: {error}",
            file=sys.stderr,
        )
        exit_code = 1
    finally:
        for child in reversed(children):
            _stop(child)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
