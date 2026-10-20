#!/usr/bin/env python3
"""Create the exact conditional owner approval for one P7 manifest."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_shadow import (  # noqa: E402
    APPROVAL_VERSION,
    OWNER_DECISION,
    validate_authorization,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--manifest", type=Path, required=True)
    value.add_argument("--authorization-reference", required=True)
    value.add_argument("--output", type=Path, required=True)
    return value


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    manifest_path = args.manifest.resolve()
    output = args.output.resolve()
    body = manifest_path.read_bytes()
    manifest = json.loads(body)
    plan = validate_authorization(manifest, backend_dir=BACKEND_DIR)
    reference = args.authorization_reference.strip()
    if reference != plan["authorization_reference"]:
        raise ValueError("P7 approval authorization reference changed")
    if output.exists():
        raise FileExistsError("P7 approval output must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError("P7 approval output parent is absent")
    approval = {
        "contract_version": APPROVAL_VERSION,
        "manifest_id": plan["manifest_id"],
        "manifest_sha256": hashlib.sha256(body).hexdigest(),
        "authorization_reference": reference,
        "owner_decision": OWNER_DECISION,
        "conditional_live_shadow_authorized": True,
        "provider_websocket_connection_attempts_authorized": 2,
        "provider_rest_requests_authorized": 0,
        "production_effects_authorized": False,
    }
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(approval, indent=2, sort_keys=True) + "\n"
        )
    print(json.dumps({
        "status": "APPROVAL_PREPARED_NOT_EXECUTED",
        "manifest_id": plan["manifest_id"],
        "manifest_sha256": approval["manifest_sha256"],
        "output": str(output),
        "provider_requests": 0,
        "external_socket_connections": 0,
        "database_connections": 0,
        "database_ddl": 0,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
