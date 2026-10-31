#!/usr/bin/env python3
"""Plan, reconcile, or run the persistent Advanced V3 daily supervisor."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_realtime_daily_supervisor import (  # noqa: E402
    REQUIRED_DAILY_ARTIFACTS,
    artifacts_from_paths,
    build_daily_supervisor_plan,
    daily_artifact_paths,
    daily_supervisor_action,
    reconcile_daily_supervisor,
    scheduler_argv,
    session_for_observation,
    validated_daily_artifacts,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "reconcile", "supervise"))
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--release-source-sha256", required=True)
    parser.add_argument("--release-image-id", required=True)
    parser.add_argument("--output", type=Path)
    for name in REQUIRED_DAILY_ARTIFACTS:
        parser.add_argument(f"--{name.replace('_', '-')}", type=Path)
    parser.add_argument("--prior-state", type=Path)
    parser.add_argument("--session-root", type=Path)
    parser.add_argument("--state-directory", type=Path)
    parser.add_argument("--database")
    parser.add_argument("--confirm-database-name")
    parser.add_argument("--docker-executable", type=Path)
    parser.add_argument(
        "--python-executable",
        type=Path,
        default=Path(sys.executable),
    )
    parser.add_argument(
        "--scheduler-script",
        type=Path,
        default=Path(__file__).with_name(
            "run_advanced_realtime_session_scheduler.py"
        ),
    )
    parser.add_argument("--supervisor-poll-seconds", type=float, default=30)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if len(args.release_source_sha256) != 64:
        parser.error("release source identity is invalid")
    if not args.release_image_id.startswith("sha256:"):
        parser.error("release image identity is invalid")
    if not args.execute:
        parser.error("supervisor requires --execute")
    if args.command in {"plan", "reconcile"}:
        if args.session_date is None or args.output is None:
            parser.error("plan/reconcile require --session-date and --output")
        args.output = args.output.resolve()
        if args.output.exists() or not args.output.parent.is_dir():
            parser.error("supervisor requires a new output")
        if args.command == "reconcile" and any(
            getattr(args, name) is None for name in REQUIRED_DAILY_ARTIFACTS
        ):
            parser.error("reconcile requires every daily artifact path")
    else:
        required = (
            args.session_root,
            args.state_directory,
            args.database,
            args.confirm_database_name,
            args.docker_executable,
        )
        if (
            any(value is None for value in required)
            or args.database != args.confirm_database_name
            or args.database != "stocks_realtime_v2"
            or args.supervisor_poll_seconds <= 0
            or not args.session_root.is_dir()
            or not args.state_directory.is_dir()
            or not args.docker_executable.is_file()
            or not args.python_executable.is_file()
            or not args.scheduler_script.is_file()
        ):
            parser.error(
                "supervise requires existing roots/executables and the exact "
                "confirmed target database"
            )
    return args


def _write_json(path: Path, value, *, exclusive: bool = False) -> None:
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    if exclusive:
        with path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    try:
        with temporary.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _supervise(args) -> int:
    while True:
        observed_at = datetime.now(timezone.utc)
        session_date = session_for_observation(observed_at)
        plan = build_daily_supervisor_plan(
            session_date,
            release_source_sha256=args.release_source_sha256,
            release_image_id=args.release_image_id,
        )
        paths = daily_artifact_paths(args.session_root, session_date)
        artifacts = validated_daily_artifacts(plan, paths)
        session_state = args.state_directory / session_date.isoformat()
        session_state.mkdir(exist_ok=True)
        reconciliation_path = session_state / "RECONCILIATION.json"
        prior = (
            json.loads(reconciliation_path.read_text(encoding="utf-8"))
            if reconciliation_path.is_file()
            else None
        )
        reconciliation = reconcile_daily_supervisor(
            plan,
            artifacts=artifacts,
            prior_state=prior,
            observed_at=observed_at,
        )
        _write_json(reconciliation_path, reconciliation)
        coordinator_directory = (
            args.session_root
            / session_date.isoformat()
            / "coordinator"
        )
        action = daily_supervisor_action(
            plan,
            artifacts=artifacts,
            coordinator_directory=coordinator_directory,
            observed_at=observed_at,
        )
        _write_json(session_state / "ACTION.json", action)
        if action["action"] in {"LAUNCH_SCHEDULER", "RESUME_SCHEDULER"}:
            command = scheduler_argv(
                python_executable=args.python_executable,
                scheduler_script=args.scheduler_script,
                database=args.database,
                artifact_paths=paths,
                docker_executable=args.docker_executable,
                coordinator_directory=coordinator_directory,
            )
            started_at = datetime.now(timezone.utc)
            completed = subprocess.run(command, check=False)
            receipt = {
                "schema": "advanced-realtime-v3-daily-supervisor-launch-v1",
                "session_date": session_date.isoformat(),
                "plan_sha256": plan["plan_sha256"],
                "action": action["action"],
                "started_at_utc": started_at.isoformat(),
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "returncode": completed.returncode,
                "coordinator_directory": str(
                    coordinator_directory.resolve()
                ),
            }
            _write_json(
                session_state / f"LAUNCH-{time.time_ns()}.json",
                receipt,
                exclusive=True,
            )
            if args.once:
                return completed.returncode
        elif args.once:
            return 0 if action["action"] == "AWAIT_NEXT_SESSION" else 2
        time.sleep(args.supervisor_poll_seconds)


def main(argv=None):
    args = parse_args(argv)
    if args.command == "supervise":
        return _supervise(args)
    plan = build_daily_supervisor_plan(
        args.session_date,
        release_source_sha256=args.release_source_sha256,
        release_image_id=args.release_image_id,
    )
    if args.command == "plan":
        report = plan
    else:
        paths = {
            name: getattr(args, name).resolve()
            for name in REQUIRED_DAILY_ARTIFACTS
        }
        prior = (
            None
            if args.prior_state is None
            else json.loads(args.prior_state.read_text(encoding="utf-8"))
        )
        report = reconcile_daily_supervisor(
            plan,
            artifacts=artifacts_from_paths(paths),
            prior_state=prior,
            observed_at=datetime.now(timezone.utc),
        )
    _write_json(args.output, report, exclusive=True)
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("state") != "PREPARATION_REQUIRED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
