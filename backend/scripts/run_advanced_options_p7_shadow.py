#!/usr/bin/env python3
"""Run the exact approved Advanced Options P7 live shadow."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_executor import execute_shadow  # noqa: E402
from realtime.advanced_options_p7_shadow import (  # noqa: E402
    approved_authorization,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=BACKEND_DIR / ".env",
    )
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def _api_key(path: Path) -> str:
    values = dotenv_values(path)
    key = str(values.get("POLYGON_API_KEY") or "").strip()
    if not key or key.lower() in {
        "your_polygon_api_key_here",
        "replace_me",
    }:
        raise RuntimeError("P7 live shadow provider credential is unavailable")
    return key


def main(argv=None) -> int:
    args = parse_args(argv)
    manifest_path = args.manifest.resolve()
    approval_path = args.approval.resolve()
    plan = approved_authorization(
        manifest_path,
        approval_path,
        backend_dir=BACKEND_DIR,
    )
    if not args.execute:
        print(
            json.dumps(
                {
                    "status": "PLAN",
                    "manifest_id": plan["manifest_id"],
                    "plan_sha256": plan["plan_sha256"],
                    "window": plan["window"],
                    "live_execution_implemented": plan[
                        "live_execution_implemented"
                    ],
                    "provider_requests": 0,
                    "external_socket_connections": 0,
                    "database_connections": 0,
                    "database_ddl": 0,
                    "files_written": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    manifest_body = manifest_path.read_bytes()
    approval_body = approval_path.read_bytes()
    report = execute_shadow(
        plan,
        manifest_sha256=hashlib.sha256(manifest_body).hexdigest(),
        approval_sha256=hashlib.sha256(approval_body).hexdigest(),
        api_key=_api_key(args.env_file.resolve()),
        now=datetime.now(UTC),
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "manifest_id": report["manifest_id"],
                "error": report["error"],
                "evidence_directory": plan["evidence"]["directory"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "PASS_LIVE_SHADOW" else 2


if __name__ == "__main__":
    raise SystemExit(main())
