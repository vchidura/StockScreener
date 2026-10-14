#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.bootstrap_manifest import load_realtime_bootstrap_manifest
from realtime.bootstrap_reset import build_realtime_integration_reset_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan a disposable realtime integration database reset without I/O."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = load_realtime_bootstrap_manifest(Path(args.manifest).resolve())
    plan = build_realtime_integration_reset_plan(
        manifest,
        database=args.database,
        confirmation=args.confirm_database_name,
        role_prefix=args.role_prefix,
    )
    print(json.dumps({
        "mode": "RESET_PLAN",
        "database": plan.database,
        "role_prefix": plan.role_prefix,
        "manifest_sha256": plan.manifest_sha256,
        "confirmation_token": plan.confirmation_token,
        "actions": plan.actions,
        "preserved_roles": plan.preserved_roles,
        "rebuild_stages": plan.rebuild_stages,
        "plan_sha256": plan.sha256,
        "provider_requests_performed": plan.provider_requests,
        "database_connections": plan.database_connections,
        "database_writes": plan.database_writes,
        "values_exposed": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
