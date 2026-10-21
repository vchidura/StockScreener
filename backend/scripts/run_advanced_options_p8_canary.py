#!/usr/bin/env python3
"""Guarded one-use Advanced Options P8 canary lifecycle CLI."""
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

from realtime.advanced_options_p8_canary_executor import (  # noqa: E402
    P8CanaryExecutor,
)
from realtime.advanced_options_p8_runtime import (  # noqa: E402
    build_advanced_options_p8_runtime,
    check_p8_docker_service_health,
    run_p8_docker_service,
)
from realtime.advanced_options_service import (  # noqa: E402
    AdvancedOptionsProcessKind,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--qualification-report", type=Path, required=True)
    parser.add_argument(
        "--expected-qualification-semantic-sha256",
        required=True,
    )
    parser.add_argument(
        "--expected-qualification-file-sha256",
        required=True,
    )
    parser.add_argument("--expected-package-sha256", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--checked-at")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--enable-stored-reads", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.enable_stored_reads and not args.execute:
        parser.error("--enable-stored-reads requires --execute")
    return args


def _json(path: Path):
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _write(path: Path | None, payload: object) -> None:
    if path is None:
        return
    target = path.resolve()
    if target.exists():
        raise FileExistsError("P8 executor output must be new")
    if not target.parent.is_dir():
        raise FileNotFoundError("P8 executor output parent is absent")
    with target.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


class _NoExecutionPort:
    def __getattr__(self, name):
        raise RuntimeError(
            f"P8 runtime port {name} is unavailable outside --execute"
        )


def _docker_service_main(argv) -> int | None:
    values = list(sys.argv[1:] if argv is None else argv)
    service_mode = "--docker-service-kind" in values
    health_mode = "--docker-health-kind" in values
    if not service_mode and not health_mode:
        return None
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        "--docker-service-kind",
        choices=tuple(kind.value for kind in AdvancedOptionsProcessKind),
    )
    modes.add_argument(
        "--docker-health-kind",
        choices=tuple(kind.value for kind in AdvancedOptionsProcessKind),
    )
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--execution-bundle", type=Path, required=True)
    args = parser.parse_args(values)
    raw_kind = args.docker_service_kind or args.docker_health_kind
    kind = AdvancedOptionsProcessKind(raw_kind)
    if service_mode:
        return run_p8_docker_service(
            kind,
            args.package.resolve(),
            args.execution_bundle.resolve(),
        )
    health = check_p8_docker_service_health(
        kind,
        args.package.resolve(),
        args.execution_bundle.resolve(),
    )
    print(json.dumps(health, indent=2, sort_keys=True))
    return 0


def main(argv=None) -> int:
    docker_result = _docker_service_main(argv)
    if docker_result is not None:
        return docker_result
    args = parse_args(argv)
    package = _json(args.package.resolve())
    port = (
        build_advanced_options_p8_runtime(
            package=package,
            package_path=args.package.resolve(),
            repository_root=REPOSITORY_ROOT,
            stored_reads_authorized=args.enable_stored_reads,
        )
        if args.execute
        else _NoExecutionPort()
    )
    executor = P8CanaryExecutor(
        port,
        repository_root=REPOSITORY_ROOT,
        qualification_report_path=args.qualification_report.resolve(),
        current_source_commit=args.source_commit,
        expected_package_sha256=args.expected_package_sha256,
        expected_qualification_semantic_sha256=(
            args.expected_qualification_semantic_sha256
        ),
        expected_qualification_file_sha256=(
            args.expected_qualification_file_sha256
        ),
    )
    checked_at = (
        datetime.fromisoformat(args.checked_at.replace("Z", "+00:00"))
        if args.checked_at
        else datetime.now(UTC)
    )
    if not any((args.preflight, args.status, args.execute)):
        args.plan = True
    if args.plan:
        result = executor.plan(package)
    elif args.preflight:
        result = executor.preflight(package, checked_at=checked_at)
    elif args.status:
        result = executor.status(
            package,
            checked_at=checked_at,
        ).as_dict()
    else:
        result = executor.execute(
            package,
            checked_at=checked_at,
            stored_reads_authorized=args.enable_stored_reads,
        )
    _write(args.output, result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
