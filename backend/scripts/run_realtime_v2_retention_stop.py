#!/usr/bin/env python3
"""Run the exact O-WP5 stop-only package through a selected command boundary."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.retention_lifecycle_executor import (  # noqa: E402
    FakeLifecycleCommandPort,
    ObservationSnapshotProvider,
    StopOnlyExecutor,
    load_exact_lifecycle_package,
)
from realtime.retention_lifecycle_docker import (  # noqa: E402
    DockerLifecycleCommandPort,
)


UTC = timezone.utc


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("checked_at must be timezone-aware")
    return parsed.astimezone(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--post-stop-contract", type=Path, required=True)
    parser.add_argument("--readiness-report", type=Path, required=True)
    parser.add_argument("--fake-service-state", type=Path)
    parser.add_argument("--lock-path", type=Path, required=True)
    parser.add_argument("--receipt-path", type=Path, required=True)
    parser.add_argument("--checked-at", type=_timestamp, required=True)
    boundary = parser.add_mutually_exclusive_group()
    boundary.add_argument("--owned-fake-service", action="store_true")
    boundary.add_argument("--real-docker", action="store_true")
    parser.add_argument("--docker-executable", type=Path)
    parser.add_argument("--expected-docker-version")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def _outside_repository(path: Path, name: str) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(REPOSITORY_ROOT)
    except ValueError:
        return resolved
    raise ValueError(f"{name} must be outside the repository")


def _command_port(args, package):
    if args.real_docker:
        if args.fake_service_state is not None:
            raise ValueError(
                "--real-docker refuses --fake-service-state"
            )
        if (
            args.docker_executable is None
            or args.expected_docker_version is None
        ):
            raise ValueError(
                "--real-docker requires --docker-executable and "
                "--expected-docker-version"
            )
        return DockerLifecycleCommandPort(
            baseline_observation=package[
                "lifecycle_scope"
            ]["observed_state"],
            expected_container=package[
                "lifecycle_scope"
            ]["container_identity"],
            docker_executable=args.docker_executable,
            expected_docker_version=args.expected_docker_version,
        )
    if args.docker_executable is not None or (
        args.expected_docker_version is not None
    ):
        raise ValueError(
            "Docker executable/version require --real-docker"
        )
    if args.fake_service_state is None:
        raise ValueError(
            "select --real-docker or provide --fake-service-state"
        )
    fake_state = _outside_repository(
        args.fake_service_state,
        "fake_service_state",
    )
    return FakeLifecycleCommandPort(fake_state)


def main(argv=None) -> int:
    args = parse_args(argv)
    package = load_exact_lifecycle_package(
        args.package.resolve(),
        args.post_stop_contract.resolve(),
    )
    lock_path = _outside_repository(args.lock_path, "lock_path")
    receipt_path = _outside_repository(args.receipt_path, "receipt_path")
    command_port = _command_port(args, package)
    executor = StopOnlyExecutor(
        command_port,
        ObservationSnapshotProvider(),
        repository_root=REPOSITORY_ROOT,
        readiness_report_path=args.readiness_report.resolve(),
        lock_path=lock_path,
        receipt_path=receipt_path,
    )
    if not any((args.preflight, args.status, args.execute)):
        args.plan = True
    if args.plan:
        payload = executor.plan(package)
    elif args.preflight:
        payload = executor.preflight(
            package,
            checked_at=args.checked_at,
        )
    elif args.status:
        payload = {
            "mode": "STATUS",
            **executor.status(package).as_dict(),
            "service_lifecycle_actions": 0,
            "production_writes": 0,
            "provider_requests": 0,
        }
    elif args.execute:
        if not args.real_docker and not args.owned_fake_service:
            raise ValueError(
                "--execute requires --owned-fake-service or --real-docker"
            )
        payload = executor.execute(
            package,
            checked_at=args.checked_at,
        )
    else:
        raise AssertionError("unreachable stop-only mode")
    if args.real_docker:
        payload = {
            **payload,
            "docker_command_evidence": [
                dict(row) for row in command_port.evidence()
            ],
        }
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
