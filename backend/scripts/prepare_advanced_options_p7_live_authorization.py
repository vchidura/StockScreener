#!/usr/bin/env python3
"""Create one exact live authorization for an approved P7 manifest."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_shadow import (  # noqa: E402
    approved_authorization,
    build_live_authorization,
)


UTC = timezone.utc


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--manifest", type=Path, required=True)
    value.add_argument("--approval", type=Path, required=True)
    value.add_argument("--output", type=Path, required=True)
    return value


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    manifest_path = args.manifest.resolve()
    approval_path = args.approval.resolve()
    output = args.output.resolve()
    plan = approved_authorization(
        manifest_path,
        approval_path,
        backend_dir=BACKEND_DIR,
    )
    if output.exists():
        raise FileExistsError("P7 live authorization output must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError(
            "P7 live authorization output parent is absent"
        )
    authorization = build_live_authorization(
        plan,
        manifest_sha256=hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest(),
        approval_sha256=hashlib.sha256(
            approval_path.read_bytes()
        ).hexdigest(),
        authorized_at=datetime.now(UTC),
    )
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(authorization, indent=2, sort_keys=True) + "\n"
        )
    print(json.dumps({
        "status": "LIVE_AUTHORIZATION_PREPARED_NOT_EXECUTED",
        "manifest_id": plan["manifest_id"],
        "live_authorization_reference": authorization[
            "live_authorization_reference"
        ],
        "output": str(output),
        "provider_requests": 0,
        "external_socket_connections": 0,
        "database_connections": 0,
        "database_ddl": 0,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
