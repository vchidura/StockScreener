#!/usr/bin/env python3
"""Plan or run the post-session refresh of established Advanced members."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_preparation import (  # noqa: E402
    BoundedPolygonEquityPreparationClient,
    MassiveEquityPreparationSource,
)
from realtime.advanced_equity_rollover import (  # noqa: E402
    AdvancedEquityRolloverResident,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--maximum-actions", type=int, default=64)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced rollover targets only stocks_realtime_v2")
    if not 1 <= args.maximum_actions <= 256:
        parser.error("--maximum-actions must be in [1, 256]")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def _provider(config):
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _plan_report(plan, checked_at):
    if plan is None:
        return {"status": "NO_DESIRED_SOURCE_COHORT"}
    return {
        "checked_at": checked_at.isoformat(),
        "revision_id": str(plan.revision_id),
        "source_cohort_id": str(plan.source_cohort_id),
        "target_session_date": plan.target_session_date.isoformat(),
        "completed_session_date": plan.completed_session_date.isoformat(),
        "members": [member.ticker for member in plan.members],
        "work": [
            {
                "ticker": work.member.ticker,
                "action": work.action,
                "sessions": [
                    session.isoformat() for session in work.sessions
                ],
                "renew_identity": work.renew_identity,
            }
            for work in plan.work
        ],
    }


def _result_report(result):
    return {
        "status": result.status,
        "checked_at": result.checked_at.isoformat(),
        "action": result.action,
        "reason": result.reason,
        "remaining": result.remaining,
        "provider_requests": result.provider_requests,
        "response_bytes": result.response_bytes,
        "persisted_bars": result.persisted_bars,
        "persisted_reference_facts": result.persisted_reference_facts,
        "next_retry_at": (
            result.next_retry_at.isoformat() if result.next_retry_at is not None else None
        ),
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    env_file = args.env_file or BACKEND_DIR / ".env"
    file_environment = (
        {
            key: value
            for key, value in dotenv_values(env_file).items()
            if value is not None
        }
        if env_file.is_file()
        else {}
    )
    config = RealtimeV2PipelineConfig.from_mapping({
        **file_environment,
        **os.environ,
    },
        process=RealtimeV2Process.EQUITY_PREPARATION,
    )
    provider, pool = _provider(config)
    resident = AdvancedEquityRolloverResident(
        provider,
        MassiveEquityPreparationSource(
            BoundedPolygonEquityPreparationClient(config.massive_api_key)
        ),
        owner_id=uuid4(),
    )
    try:
        checked_at = datetime.now(UTC)
        report = {
            "mode": "PLAN" if args.plan else "EXECUTE",
            "plan": _plan_report(resident.plan(checked_at), checked_at),
        }
        exit_code = 0
        if args.execute:
            results = []
            for _action in range(args.maximum_actions):
                result = resident.run_once()
                results.append(_result_report(result))
                print(json.dumps(results[-1], sort_keys=True), flush=True)
                if result.status != "PROGRESSED":
                    break
            report["results"] = results
            report["provider_requests"] = sum(
                row["provider_requests"] for row in results
            )
            checked_at = datetime.now(UTC)
            report["plan_after"] = _plan_report(
                resident.plan(checked_at),
                checked_at,
            )
            exit_code = (
                0 if results[-1]["status"] == "IDLE"
                and "work" in report["plan_after"]
                and not report["plan_after"]["work"]
                else 2
            )
        else:
            report["provider_requests"] = 0
            report["database_writes"] = 0
        write_report(args.report_path, report)
        print(json.dumps(report["plan"], indent=2))
        return exit_code
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
