#!/usr/bin/env python3
"""Launch the V2 API, frontend and finalizer, optionally with a Developer scheduler."""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.request import urlopen
from uuid import UUID

WORKSPACE_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = WORKSPACE_DIR / "backend"
PYTHON = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
NPM = "npm.cmd"
COHORT_DEFAULT = UUID("d1baee6f-4ab2-543c-8a86-ce10f8952e69")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-id", type=UUID, default=COHORT_DEFAULT)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--first-cutoff", type=datetime.fromisoformat)
    parser.add_argument("--window-count", type=int)
    parser.add_argument("--continuous-ingestion", action="store_true")
    parser.add_argument("--stored-only", action="store_true")
    parser.add_argument("--daily-request-ceiling", type=int)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--api-port", type=int, default=8002)
    parser.add_argument("--frontend-port", type=int, default=5174)
    parser.add_argument("--run-directory", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.stored_only and args.continuous_ingestion:
        parser.error("--stored-only and --continuous-ingestion are mutually exclusive")
    if args.stored_only:
        if any(value is not None for value in (
            args.first_cutoff, args.window_count, args.daily_request_ceiling
        )):
            parser.error(
                "stored-only mode does not accept cutoff, window-count or request ceiling"
            )
        args.maximum_provider_requests = 0
    elif args.continuous_ingestion:
        if args.first_cutoff is None:
            parser.error("continuous ingestion requires --first-cutoff")
        if args.window_count is not None:
            parser.error("continuous ingestion does not accept --window-count")
        if args.first_cutoff.tzinfo is None or args.first_cutoff.utcoffset() is None:
            parser.error("--first-cutoff must be timezone-aware")
        if args.daily_request_ceiling is None or args.daily_request_ceiling <= 0:
            parser.error("continuous ingestion requires --daily-request-ceiling")
        args.maximum_provider_requests = args.daily_request_ceiling
    else:
        if args.first_cutoff is None or args.window_count is None:
            parser.error("bounded ingestion requires --first-cutoff and --window-count")
        if args.first_cutoff.tzinfo is None or args.first_cutoff.utcoffset() is None:
            parser.error("--first-cutoff must be timezone-aware")
        if not 1 <= args.window_count <= 26:
            parser.error("--window-count must be in [1, 26]")
        args.maximum_provider_requests = args.window_count * 18
    if args.api_port == args.frontend_port or min(args.api_port, args.frontend_port) <= 0:
        parser.error("API and frontend ports must be distinct positive values")
    if (
        args.execute
        and not args.stored_only
        and not args.authorization_reference.strip()
    ):
        parser.error("--execute requires --authorization-reference")
    if not PYTHON.is_file():
        parser.error(f"repository interpreter is missing: {PYTHON}")
    if args.run_directory is None:
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        args.run_directory = (
            BACKEND_DIR / ".realtime-canary" / "v2-services" / stamp
        )
    args.run_directory = args.run_directory.resolve()
    if args.run_directory.exists():
        parser.error("--run-directory must not already exist")
    return args


def build_commands(args):
    run = args.run_directory
    scheduler_reports = run / "scheduler-reports"
    scheduler_cache = run / "scheduler-cache"
    commands = {
        "api": [
            str(PYTHON), "-B", "-m", "uvicorn",
            "scripts.run_realtime_v2_portal_api:create_app",
            "--factory", "--app-dir", "backend",
            "--host", "127.0.0.1", "--port", str(args.api_port),
        ],
        "finalizer": [
            str(PYTHON), "-B",
            str(BACKEND_DIR / "scripts" / "run_realtime_v2_equity_interval_finalizer.py"),
            "--database", "stocks_realtime_v2",
            "--confirm-database-name", "stocks_realtime_v2",
            "--security-mode", "hardened",
            "--cohort-id", str(args.cohort_id),
            "--continuous", "--poll-seconds", "1",
            "--claim-limit", "10", "--execute",
        ],
        "frontend": [
            NPM, "--prefix", "frontend", "run", "dev", "--",
            "--host", "127.0.0.1", "--port", str(args.frontend_port),
        ],
    }
    if not args.stored_only:
        commands["scheduler"] = [
            str(PYTHON), "-B",
            str(
                BACKEND_DIR / "scripts" / (
                    "run_realtime_v2_continuous_ingestion.py"
                    if args.continuous_ingestion
                    else "run_realtime_v2_developer_service.py"
                )
            ),
            "--database", "stocks_realtime_v2",
            "--confirm-database-name", "stocks_realtime_v2",
            "--cohort-id", str(args.cohort_id),
            *(
                [
                    "--authorized-session-date", args.session_date.isoformat(),
                    "--first-authorized-cutoff", args.first_cutoff.isoformat(),
                    "--daily-request-ceiling", str(args.daily_request_ceiling),
                ]
                if args.continuous_ingestion
                else [
                    "--session-date", args.session_date.isoformat(),
                    "--first-cutoff", args.first_cutoff.isoformat(),
                    "--window-count", str(args.window_count),
                    "--maximum-provider-requests", str(args.maximum_provider_requests),
                ]
            ),
            "--cache-root", str(scheduler_cache),
            "--report-directory", str(scheduler_reports),
            "--summary-report", str(run / "scheduler-summary.json"),
            "--authorization-reference", args.authorization_reference,
            "--execute",
        ]
    return commands


@dataclass
class Child:
    name: str
    process: subprocess.Popen
    output: object


def _start(name, command, *, environment, run_directory):
    output = (run_directory / f"{name}.log").open(
        "w", encoding="utf-8", buffering=1)
    creationflags = (
        subprocess.CREATE_NEW_PROCESS_GROUP
        if os.name == "nt" else 0
    )
    process = subprocess.Popen(
        command,
        cwd=WORKSPACE_DIR,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=output,
        stderr=subprocess.STDOUT,
        text=True,
        creationflags=creationflags,
    )
    return Child(name, process, output)


def _stop(child):
    if child.process.poll() is None:
        try:
            if os.name == "nt":
                child.process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                child.process.send_signal(signal.SIGINT)
            child.process.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            child.process.terminate()
            try:
                child.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.process.kill()
                child.process.wait(timeout=5)
    child.output.close()


def _wait_http(url, *, timeout=45):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=3) as response:
                if response.status == 200:
                    return
        except Exception as error:  # noqa: BLE001 - startup probe retains last failure
            last_error = error
        time.sleep(0.5)
    raise RuntimeError(f"service did not become ready: {url}") from last_error


def main(argv=None):
    args = parse_args(argv)
    commands = build_commands(args)
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "run_directory": str(args.run_directory),
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "stored_only": args.stored_only,
        "continuous_ingestion": args.continuous_ingestion,
        "first_cutoff": (
            args.first_cutoff.isoformat()
            if args.first_cutoff is not None else None
        ),
        "window_count": args.window_count,
        "maximum_provider_requests": args.maximum_provider_requests,
        "api_url": f"http://127.0.0.1:{args.api_port}",
        "portal_url": f"http://127.0.0.1:{args.frontend_port}",
        "commands": commands,
    }
    if not args.execute:
        print(json.dumps(plan, indent=2))
        return 0
    args.run_directory.mkdir(parents=True)
    (args.run_directory / "launch-plan.json").write_text(
        json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    environment = dict(os.environ)
    frontend_environment = dict(environment)
    frontend_environment.update({
        "VITE_API_PROXY_TARGET": f"http://127.0.0.1:{args.api_port}",
        "VITE_REALTIME_ONLY": "true",
        "VITE_REALTIME_COHORT_ID": str(args.cohort_id),
    })
    children = []
    exit_code = 0
    try:
        children.append(_start(
            "api", commands["api"],
            environment=environment, run_directory=args.run_directory))
        children.append(_start(
            "finalizer", commands["finalizer"],
            environment=environment, run_directory=args.run_directory))
        children.append(_start(
            "frontend", commands["frontend"],
            environment=frontend_environment, run_directory=args.run_directory))
        _wait_http(f"http://127.0.0.1:{args.api_port}/api/realtime/health")
        _wait_http(f"http://127.0.0.1:{args.frontend_port}/")
        if "scheduler" in commands:
            children.append(_start(
                "scheduler", commands["scheduler"],
                environment=environment, run_directory=args.run_directory))
        print(json.dumps({
            "status": "RUNNING",
            "portal_url": plan["portal_url"],
            "api_url": plan["api_url"],
            "run_directory": str(args.run_directory),
            "pids": {child.name: child.process.pid for child in children},
        }, indent=2))
        scheduler = next(
            (child for child in children if child.name == "scheduler"), None
        )
        while True:
            for child in children:
                result = child.process.poll()
                if result is None:
                    continue
                if child is scheduler and result == 0:
                    children.remove(child)
                    child.output.close()
                    print("Bounded scheduler completed; API, frontend and finalizer remain running.")
                    scheduler = None
                    break
                raise RuntimeError(
                    f"{child.name} exited unexpectedly with code {result}; "
                    f"see {args.run_directory / (child.name + '.log')}"
                )
            if scheduler is None:
                while True:
                    for child in children:
                        result = child.process.poll()
                        if result is not None:
                            raise RuntimeError(
                                f"{child.name} exited unexpectedly with code {result}; "
                                f"see {args.run_directory / (child.name + '.log')}"
                            )
                    time.sleep(1)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping V2 services...")
    except Exception as error:
        print(f"V2 service supervisor failed: {error}", file=sys.stderr)
        exit_code = 1
    finally:
        for child in reversed(children):
            _stop(child)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
