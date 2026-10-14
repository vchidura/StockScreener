#!/usr/bin/env python3
"""V2 stage orchestrator entry point (security deferred: connects as the database owner).

  plan        read-only: show the plan scope, principals, wired/unwired stages (and which wired stages
              fail closed for lack of a provider source) and readiness
  once        claim and run every window that is due, then exit
  continuous  poll windows and tick resident stages until interrupted

once and continuous mutate the database and need --execute. Only stocks_realtime_v2 or an
explicit test_realtime_v2_ database (with --allow-disposable) is accepted. No provider request is
made by this script and it configures no provider source: the stages that need one (REFERENCE and
EQUITY_INGEST) are wired but report BLOCKED_POLICY (REFERENCE_SOURCE_NOT_CONFIGURED /
EQUITY_SOURCE_NOT_CONFIGURED) until an authorized source is injected through realtime.runner_environment.
Only the OPTIONS_* stages are unwired; enabling one in a plan fails closed at start.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
    orchestrator_pool_size,
    pools_required,
    verify_server_connection_budget,
)
from realtime.migrations import (  # noqa: E402
    SECURITY_MODE_DEFERRED,
    SECURITY_MODE_HARDENED,
    SECURITY_MODES_V2,
    validate_target_database_v2,
)
from realtime.orchestration_repository import PostgresOrchestrationRepository  # noqa: E402
from realtime.runner_environment import RunnerEnvironment  # noqa: E402
from realtime.stage_graph import RESIDENT_SCHEDULES, STAGES_V1  # noqa: E402
from realtime.stage_orchestrator import StageOrchestrator  # noqa: E402
from realtime.stage_runners_v2 import (  # noqa: E402
    build_runners,
    fail_closed_stages,
    unwired_stages,
    wired_stages,
)
from realtime.v2_service import V2Service, window_specs_for_session  # noqa: E402

DEVELOPER_TIER = "MASSIVE_DEVELOPER"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=("plan", "once", "continuous"))
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--allow-disposable", action="store_true")
    parser.add_argument("--security-mode", choices=SECURITY_MODES_V2, default=SECURITY_MODE_DEFERRED)
    parser.add_argument("--plan-id", type=UUID, required=True)
    parser.add_argument("--completion-budget-seconds", type=int, default=60)
    parser.add_argument("--serving-lag-seconds", type=int, default=30)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--other-connections", type=int, default=8,
                        help="pool maximums of the other consumers (API, archive, maintenance) sharing the server")
    parser.add_argument("--reserved-connections", type=int, default=5)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=args.allow_disposable,
                                security_mode=args.security_mode)
    if args.security_mode == SECURITY_MODE_HARDENED:
        parser.error("hardened runtime credentials are provisioned in the final hardening phase")
    if args.mode != "plan" and not args.execute:
        parser.error(f"{args.mode} mutates the database and requires --execute")
    return args


def _pool(args: argparse.Namespace, maxconn: int) -> ThreadedConnectionPool:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("requires: " + ", ".join(missing))
    return ThreadedConnectionPool(
        1, maxconn, dbname=args.database, user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"], port=settings["DB_PORT"],
        application_name="run_realtime_v2_orchestrator", options="-c timezone=UTC")


def main(argv=None) -> int:
    args = parse_args(argv)
    config = V2AccessConfig(security_mode=args.security_mode, allow_disposable=args.allow_disposable)
    # A small probe pool reads the plan; the working pool is then sized from the plan and the whole
    # deployment is checked against the server's connection capacity before any stage can start.
    probe = _pool(args, 2)
    try:
        plan_id_enabled = PostgresOrchestrationRepository(
            V2ConnectionProvider({OWNER_POOL: probe}, config)).load_plan(args.plan_id).enabled_stages
        pool_max = orchestrator_pool_size(plan_id_enabled)
        budget = verify_server_connection_budget(
            probe, {"orchestrator": pool_max, "other_consumers": args.other_connections},
            reserved_connections=args.reserved_connections)
    finally:
        probe.closeall()
    pool = _pool(args, pool_max)
    try:
        provider = V2ConnectionProvider({OWNER_POOL: pool}, config)
        database = provider.verify()
        # No provider source is configured here, so REFERENCE and EQUITY_INGEST stay fail-closed.
        environment = RunnerEnvironment(provider)
        repository = PostgresOrchestrationRepository(provider)
        plan = repository.load_plan(args.plan_id)
        catalog = repository.load_catalog(plan.stage_catalog_version)
        enabled = plan.enabled_stages
        resident = sorted(name for name in enabled if STAGES_V1[name].schedule_kind in RESIDENT_SCHEDULES)
        report = {
            "database": database, "security_mode": args.security_mode, "plan_id": str(plan.plan_id),
            "session_date": plan.session_date.isoformat(), "catalog_matches_source": dict(catalog) == STAGES_V1,
            "enabled": sorted(enabled), "resident": resident, "pool_max": pool_max, "connection_ceiling": budget,
            "principal_pools": sorted(pools_required(enabled, config)),
            "wired": sorted(enabled & wired_stages()), "unwired": sorted(unwired_stages(enabled)),
            "fail_closed_without_source": fail_closed_stages(enabled, environment),
            "readiness": dict(repository.readiness(plan.cohort_id)),
        }
        if args.mode == "plan":
            print(json.dumps(report, indent=2, default=str))
            return 0 if report["catalog_matches_source"] and not report["unwired"] else 2
        orchestrator = StageOrchestrator(
            repository, build_runners(enabled, provider, environment=environment), database=provider)
        orchestrator.start(plan.plan_id)
        specs = window_specs_for_session(
            plan.cohort_id, plan.session_date, DEVELOPER_TIER,
            completion_budget_seconds=args.completion_budget_seconds, serving_lag_seconds=args.serving_lag_seconds)
        service = V2Service(orchestrator, repository, plan.plan_id, specs)
        if args.mode == "once":
            poll = service.poll_windows()
            print(json.dumps({"windows": [(r.window_id.hex, r.status, r.window_state) for r in poll.windows],
                              "window_errors": poll.window_errors,
                              "fail_closed_without_source": report["fail_closed_without_source"],
                              "at": datetime.now(timezone.utc).isoformat()},
                             indent=2))
            return 0 if not poll.window_errors else 1
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        service.run(stop, poll_seconds=args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
