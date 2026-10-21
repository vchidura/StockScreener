#!/usr/bin/env python3
"""Provider-free PLAN, preflight and status for the inert retention stop package."""
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

from realtime.retention_lifecycle_authorization import (  # noqa: E402
    canonical_sha256,
    lifecycle_status,
    validate_lifecycle_authorization,
)
from scripts.prepare_realtime_v2_retention_lifecycle_authorization import (  # noqa: E402
    capture_observation,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--post-stop-contract", type=Path, required=True)
    parser.add_argument("--readiness-report", type=Path, required=True)
    parser.add_argument("--expected-package-sha256", required=True)
    parser.add_argument("--docker", default="docker.exe")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--status", action="store_true")
    return parser.parse_args(argv)


def _json(path: Path):
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def main(argv=None) -> int:
    args = parse_args(argv)
    package = _json(args.package.resolve())
    post_stop = _json(args.post_stop_contract.resolve())
    if (
        package.get("package_sha256")
        != args.expected_package_sha256
        or package.get("post_stop_contract") != post_stop
        or canonical_sha256(post_stop)
        != package.get("post_stop_contract_sha256")
    ):
        raise ValueError("lifecycle package or post-stop contract identity changed")
    if not any((args.preflight, args.status)):
        args.plan = True
    if args.plan:
        payload = {
            "mode": "PLAN",
            "package_sha256": package["package_sha256"],
            "lifecycle_scope_sha256": package[
                "lifecycle_scope_sha256"
            ],
            "lifecycle_nonce": package["lifecycle_nonce"],
            "window": package["window"],
            "allowed_lifecycle_actions": package[
                "lifecycle_scope"
            ]["allowed_lifecycle_actions"],
            "authority": package["authority"],
            "service_lifecycle_actions": 0,
            "production_writes": 0,
            "provider_requests": 0,
        }
    else:
        now = datetime.now(UTC)
        observation = capture_observation(
            docker=args.docker,
            checked_at=now,
        )
        status = lifecycle_status(package, observation)
        if args.preflight:
            preflight = validate_lifecycle_authorization(
                package,
                readiness_report_path=args.readiness_report.resolve(),
                current_observation=observation,
                repository_root=REPOSITORY_ROOT,
                expected_package_sha256=args.expected_package_sha256,
                now=now,
                prior_stop_records=(),
            )
            payload = {
                "mode": "PREFLIGHT",
                **preflight,
                "lifecycle_status": status,
            }
        else:
            payload = {
                "mode": "STATUS",
                "package_sha256": package["package_sha256"],
                "lifecycle_status": status,
                "service_lifecycle_actions": 0,
                "production_writes": 0,
                "provider_requests": 0,
            }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
