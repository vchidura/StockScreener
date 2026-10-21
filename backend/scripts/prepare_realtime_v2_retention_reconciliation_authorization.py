#!/usr/bin/env python3
"""Prepare one exact append-only retention reconciliation authorization package."""
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

from realtime.retention_reconciliation_authorization import (  # noqa: E402
    build_reconciliation_authorization,
    validate_reconciliation_authorization,
)
from scripts.audit_realtime_v2_retention_archive import (  # noqa: E402
    build_retention_compatibility_plan,
)


UTC = timezone.utc


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("authorization timestamps must be timezone-aware")
    return parsed.astimezone(UTC)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--expected-plan-sha256", required=True)
    parser.add_argument("--expected-build-id", required=True)
    parser.add_argument("--expected-policy-id", required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--execution-nonce", required=True)
    parser.add_argument("--prepared-at", type=_timestamp, required=True)
    parser.add_argument("--window-start", type=_timestamp, required=True)
    parser.add_argument("--window-end", type=_timestamp, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--postflight-output", type=Path, required=True)
    return parser.parse_args(argv)


def _write_new(path: Path, value: object) -> None:
    if path.exists():
        raise FileExistsError(f"authorization artifact must be new: {path}")
    if not path.parent.is_dir():
        raise FileNotFoundError(
            f"authorization artifact parent is unavailable: {path.parent}"
        )
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main(argv=None) -> int:
    args = parse_args(argv)
    report_path = args.report.resolve()
    output = args.output.resolve()
    postflight_output = args.postflight_output.resolve()
    if output == postflight_output:
        raise ValueError("authorization and postflight outputs must differ")
    if output.exists() or postflight_output.exists():
        raise FileExistsError("authorization outputs must both be new")
    plan = build_retention_compatibility_plan(
        report_path,
        expected_report_sha256=args.expected_report_sha256,
        expected_build_id=args.expected_build_id,
        expected_policy_id=args.expected_policy_id,
    )
    if plan["frozen_plan_sha256"] != args.expected_plan_sha256:
        raise ValueError("frozen retention PLAN SHA-256 changed")
    package = build_reconciliation_authorization(
        plan,
        repository_root=REPOSITORY_ROOT,
        expected_report_sha256=args.expected_report_sha256,
        expected_plan_sha256=args.expected_plan_sha256,
        authorization_reference=args.authorization_reference,
        execution_nonce=args.execution_nonce,
        prepared_at=args.prepared_at,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    preflight = validate_reconciliation_authorization(
        package,
        current_plan=plan,
        report_path=report_path,
        repository_root=REPOSITORY_ROOT,
        expected_package_sha256=package["package_sha256"],
        now=args.prepared_at,
        prior_execution_records=(),
    )
    _write_new(output, package)
    _write_new(postflight_output, package["postflight_contract"])
    print(json.dumps({
        "status": "RECONCILIATION_AUTHORIZATION_PREPARED_NOT_EXECUTED",
        "authorization_reference": package["authorization_reference"],
        "execution_nonce": package["execution_nonce"],
        "package_sha256": package["package_sha256"],
        "postflight_contract_sha256": package[
            "postflight_contract_sha256"
        ],
        "window": package["window"],
        "preflight": preflight,
        "output": str(output),
        "postflight_output": str(postflight_output),
        "provider_requests": 0,
        "database_connections": 0,
        "database_writes": 0,
        "archive_writes": 0,
        "archive_deletes": 0,
        "retention_started": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
