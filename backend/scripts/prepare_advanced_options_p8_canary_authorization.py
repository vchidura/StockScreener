#!/usr/bin/env python3
"""Prepare one inert exact P8 live-provider/production canary package."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p8_canary_authorization import (  # noqa: E402
    build_p8_canary_authorization,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification-report", type=Path, required=True)
    parser.add_argument(
        "--expected-qualification-semantic-sha256",
        required=True,
    )
    parser.add_argument(
        "--expected-qualification-file-sha256",
        required=True,
    )
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--package-nonce", required=True)
    parser.add_argument("--prepared-at", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--supersedes-package-sha256")
    parser.add_argument("--prepare-stored-reads", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main(argv=None) -> int:
    args = parse_args(argv)
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("P8 canary package output must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError("P8 canary package output parent is absent")
    package = build_p8_canary_authorization(
        args.qualification_report.resolve(),
        repository_root=REPOSITORY_ROOT,
        source_commit=args.source_commit,
        authorization_reference=args.authorization_reference,
        package_nonce=args.package_nonce,
        prepared_at=_timestamp(args.prepared_at),
        window_start=_timestamp(args.window_start),
        window_end=_timestamp(args.window_end),
        expected_qualification_semantic_sha256=(
            args.expected_qualification_semantic_sha256
        ),
        expected_qualification_file_sha256=(
            args.expected_qualification_file_sha256
        ),
        supersedes_package_sha256=args.supersedes_package_sha256,
        stored_reads_prepared=args.prepare_stored_reads,
    )
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(package, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": "P8_CANARY_PACKAGE_PREPARED_UNARMED_UNCONSUMED",
        "output": str(output),
        "package_sha256": package["package_sha256"],
        "package_nonce": package["package_nonce"],
        "automatic_execution_available": False,
        "guarded_one_use_execution_available": True,
        "provider_requests": 0,
        "external_socket_connections": 0,
        "production_writes": 0,
        "service_lifecycle_actions": 0,
        "releases_created": 0,
        "pointers_changed": 0,
        "stored_read_flags_changed": 0,
        "dispatches": 0,
        "deliveries": 0,
        "broker_actions": 0,
        "trades": 0,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
