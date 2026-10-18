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
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--authorization-id", type=UUID, required=True)
    parser.add_argument("--universe-target", type=int, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--first-cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--decoded-byte-ceiling", type=int, required=True)
    parser.add_argument("--stream-sessions", type=int, required=True)
    parser.add_argument("--connection-attempts", type=int, required=True)
    parser.add_argument("--rest-repair-requests", type=int, required=True)
    parser.add_argument(
        "--rest-repair-decoded-bytes",
        type=int,
        required=True,
    )
    parser.add_argument("--finality-delay-seconds", type=int, required=True)
    parser.add_argument("--api-port", type=int, default=8002)
    parser.add_argument("--frontend-port", type=int, default=5174)
    parser.add_argument("--run-directory", type=Path)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if (
        args.first_cutoff.tzinfo is None
        or args.first_cutoff.utcoffset() is None
    ):
        parser.error("--first-cutoff must be timezone-aware")
    if not args.authorization_reference.strip():
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
    return {
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
            "--cohort-id",
            str(args.cohort_id),
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
            "--authorization-id",
            str(args.authorization_id),
            "--universe-target",
            str(args.universe_target),
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
            "--authorization-id",
            str(args.authorization_id),
            "--cohort-id",
            str(args.cohort_id),
            "--session-date",
            args.session_date.isoformat(),
            "--first-cutoff",
            args.first_cutoff.isoformat(),
            "--finality-delay-seconds",
            str(args.finality_delay_seconds),
            "--authorization-reference",
            args.authorization_reference,
            "--publication-mode",
            "projection-only",
            "--report-directory",
            str(run / "window-reports"),
            "--continuous",
            "--execute",
        ],
    }


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
        "mode": "EXECUTE",
        "pipeline": config.equity_pipeline.value,
        "options_pipeline": config.options_pipeline.value,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "first_cutoff": args.first_cutoff.isoformat(),
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
        "VITE_REALTIME_COHORT_ID": str(args.cohort_id),
    })
    children = []
    exit_code = 0
    try:
        for name in (
            "api",
            "finalizer",
            "frontend",
            "bridge",
            "controller",
            "scheduler",
        ):
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
            for child in tuple(children):
                result = child.process.poll()
                if result is None:
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
