#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.bootstrap_checkpoint import (
    BootstrapStageState,
    checkpoint_json,
    create_bootstrap_checkpoint,
    load_bootstrap_checkpoint,
    next_bootstrap_stage,
    write_bootstrap_checkpoint,
)
from realtime.bootstrap_manifest import (
    BootstrapStageName,
    build_realtime_bootstrap_plan,
    load_realtime_bootstrap_manifest,
)
from realtime.bootstrap_runner import run_next_bootstrap_stage
from scripts.realtime_bootstrap_foundation import ScriptFoundationStageExecutor


DEFAULT_MANIFEST_PATH = (
    BACKEND_DIR / "realtime" / "manifests" / "integration_equity_nine_v1.json"
)
DEFAULT_CHECKPOINT_PATH = BACKEND_DIR / ".realtime-canary" / "bootstrap-checkpoint.json"
FOUNDATION_STAGES = frozenset({
    BootstrapStageName.PROVISION,
    BootstrapStageName.MIGRATIONS,
    BootstrapStageName.SECURITY,
})
FOUNDATION_EXECUTOR_FACTORY = ScriptFoundationStageExecutor
CLOCK = lambda: datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan and inspect the generic realtime fresh-database bootstrap."
    )
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST_PATH))
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--session-date", required=True, type=date.fromisoformat)
    parser.add_argument("--checkpoint-path", default=str(DEFAULT_CHECKPOINT_PATH))
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--initialize-checkpoint", action="store_true")
    operations.add_argument("--status", action="store_true")
    operations.add_argument("--execute-next-stage", action="store_true")
    return parser.parse_args()


def _plan_report(plan, manifest) -> dict[str, object]:
    return {
        "mode": "PLAN",
        "database": plan.database,
        "target_kind": manifest.target_kind.value,
        "role_prefix": plan.role_prefix,
        "session_date": plan.session_date.isoformat(),
        "manifest_version": manifest.version,
        "manifest_sha256": plan.manifest_sha256,
        "universe_version": manifest.universe.version,
        "universe_sha256": plan.universe_sha256,
        "equity_count": len(manifest.universe.equity_tickers),
        "option_underlying_count": len(manifest.universe.option_underlyings),
        "stages": [
            {
                "name": stage.name.value,
                "ordinal": stage.ordinal,
                "side_effect": stage.side_effect,
                "provider_request_limit": stage.provider_request_limit,
            }
            for stage in plan.stages
        ],
        "maximum_provider_requests": plan.maximum_provider_requests,
        "history_plan_sha256": plan.history.sha256,
        "history_policy_sha256": plan.history.policy_sha256,
        "expected_history_facts": plan.history.expected_total_facts,
        "plan_sha256": plan.sha256,
        "provider_requests_performed": 0,
        "database_connections": 0,
        "database_writes": 0,
        "values_exposed": False,
    }


def main() -> int:
    args = parse_args()
    manifest = load_realtime_bootstrap_manifest(Path(args.manifest).resolve())
    plan = build_realtime_bootstrap_plan(
        manifest,
        database=args.database,
        confirmation=args.confirm_database_name,
        role_prefix=args.role_prefix,
        session_date=args.session_date,
    )
    checkpoint_path = Path(args.checkpoint_path).resolve()
    if args.initialize_checkpoint:
        if checkpoint_path.exists():
            raise FileExistsError("bootstrap checkpoint already exists; use --status")
        checkpoint = create_bootstrap_checkpoint(
            plan,
            now=datetime.now(timezone.utc),
        )
        write_bootstrap_checkpoint(checkpoint_path, checkpoint)
        print(json.dumps({
            "mode": "INITIALIZE_CHECKPOINT",
            "database": plan.database,
            "manifest_sha256": plan.manifest_sha256,
            "plan_sha256": plan.sha256,
            "checkpoint_path": str(checkpoint_path),
            "checkpoint_sha256": checkpoint.checkpoint_sha256,
            "next_stage": next_bootstrap_stage(checkpoint).value,
            "provider_requests_performed": 0,
            "database_connections": 0,
            "database_writes": 0,
            "values_exposed": False,
        }, indent=2, sort_keys=True))
        return 0
    if args.status:
        checkpoint = load_bootstrap_checkpoint(checkpoint_path, plan)
        print(checkpoint_json(checkpoint), end="")
        return 0
    if args.execute_next_stage:
        checkpoint = load_bootstrap_checkpoint(checkpoint_path, plan)
        next_stage = next_bootstrap_stage(checkpoint)
        if next_stage is None:
            print(json.dumps({
                "mode": "EXECUTE_NEXT_STAGE",
                "status": "COMPLETE",
                "database": plan.database,
                "plan_sha256": plan.sha256,
                "checkpoint_sha256": checkpoint.checkpoint_sha256,
                "values_exposed": False,
            }, indent=2, sort_keys=True))
            return 0
        if next_stage not in FOUNDATION_STAGES:
            raise RuntimeError(
                f"bootstrap stage adapter is not implemented: {next_stage.value}"
            )
        executor = FOUNDATION_EXECUTOR_FACTORY()
        started_at = CLOCK()
        result = run_next_bootstrap_stage(
            plan,
            checkpoint,
            executor,
            started_at=started_at,
            finished_at=CLOCK(),
        )
        write_bootstrap_checkpoint(checkpoint_path, result.checkpoint)
        print(json.dumps({
            "mode": "EXECUTE_NEXT_STAGE",
            "database": plan.database,
            "plan_sha256": plan.sha256,
            "stage": result.stage.value if result.stage else None,
            "state": result.state.value if result.state else None,
            "failure_code": result.failure_code,
            "checkpoint_sha256": result.checkpoint.checkpoint_sha256,
            "next_stage": (
                next_bootstrap_stage(result.checkpoint).value
                if next_bootstrap_stage(result.checkpoint) else None
            ),
            "values_exposed": False,
        }, indent=2, sort_keys=True))
        return 1 if result.state is BootstrapStageState.FAILED else 0
    print(json.dumps(_plan_report(plan, manifest), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
