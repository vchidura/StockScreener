#!/usr/bin/env python3
"""Run Advanced ingress attempts for the active durable plan in one container."""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_ingress_repository import (  # noqa: E402
    load_advanced_ingress_runtime_plan_cursor,
)
from realtime.advanced_ingress_plan import advanced_ingress_provider_deadline  # noqa: E402
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.run_realtime_v2_advanced_equity_ingress import (  # noqa: E402
    _derived_runtime_paths,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--authorization-id", type=UUID, required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced ingress service targets stocks_realtime_v2")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if not args.execute:
        parser.error("Advanced ingress service requires --execute")
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


def _active_plan_id(provider, authorization_id, checked_at):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT plan_id FROM rt.advanced_ingress_plans "
            "WHERE authorization_id=%s AND status IN ('READY','ACTIVE') "
            "AND authorized_start_at<=%s AND authorized_end_at>%s "
            "ORDER BY session_date DESC,created_at DESC LIMIT 1",
            (str(authorization_id), checked_at, checked_at),
        )
        row = unit.cursor.fetchone()
    return None if row is None else UUID(str(row[0]))


def _runtime_path_attempt(journal_root, runtime_scope):
    for attempt in range(1, 1_001):
        try:
            _derived_runtime_paths(
                journal_root,
                plan_id=runtime_scope.plan_id,
                session_date=runtime_scope.session_date,
                connection_attempt=runtime_scope.next_connection_attempt,
                runtime_path_attempt=attempt,
            )
        except RuntimeError:
            continue
        return attempt
    raise RuntimeError("Advanced ingress runtime path attempts are exhausted")


def _child_command(args, plan_id, runtime_path_attempt):
    return [
        sys.executable,
        "-B",
        str(
            BACKEND_DIR
            / "scripts"
            / "run_realtime_v2_advanced_equity_ingress.py"
        ),
        "--database",
        args.database,
        "--confirm-database-name",
        args.confirm_database_name,
        "--ingress-plan-id",
        str(plan_id),
        "--runtime-path-attempt",
        str(runtime_path_attempt),
        "--execute",
    ]


def _wait_for_child(
    process, stop, *, poll_seconds, shutdown_seconds, deadline=None, utc_clock=None,
):
    clock = utc_clock or (lambda: datetime.now(UTC))
    while process.poll() is None:
        remaining = None if deadline is None else (deadline - clock()).total_seconds()
        wait_seconds = poll_seconds if remaining is None else min(poll_seconds, max(0, remaining))
        if not stop.wait(wait_seconds) and (deadline is None or clock() < deadline):
            continue
        process.terminate()
        try:
            process.wait(timeout=shutdown_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=shutdown_seconds)
        break
    return int(process.returncode)


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.INGRESS,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced ingress service requires Advanced equity "
            "and disabled options"
        )
    journal_root = config.advanced_journal_dir
    if journal_root is None or not journal_root.is_dir():
        raise RuntimeError("Advanced ingress journal directory is unavailable")
    catalog = load_advanced_ingress_runtime_catalog(
        config.advanced_ingress_policy_path,
        expected_version=config.advanced_ingress_policy_version,
    )
    provider, pool = _provider(config)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        while not stop.is_set():
            plan_id = _active_plan_id(
                provider,
                args.authorization_id,
                datetime.now(UTC),
            )
            if plan_id is None:
                stop.wait(args.poll_seconds)
                continue
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                runtime_scope = load_advanced_ingress_runtime_plan_cursor(
                    unit.cursor,
                    plan_id,
                )
            if (
                runtime_scope.next_connection_attempt
                > runtime_scope.budgets.connection_attempts
            ):
                stop.wait(args.poll_seconds)
                continue
            profile = catalog.profile_for_sha(
                runtime_scope.runtime_profile_sha256
            )
            deadline = advanced_ingress_provider_deadline(
                runtime_scope.authorized_start_at,
                runtime_scope.authorized_end_at,
                drain_seconds=profile.settings.drain_deadline_seconds,
                shutdown_seconds=profile.settings.shutdown_deadline_seconds,
            )
            if datetime.now(UTC) >= deadline:
                stop.wait(args.poll_seconds)
                continue
            runtime_path_attempt = _runtime_path_attempt(
                journal_root,
                runtime_scope,
            )
            process = subprocess.Popen(
                _child_command(args, plan_id, runtime_path_attempt)
            )
            result = _wait_for_child(
                process,
                stop,
                poll_seconds=args.poll_seconds,
                shutdown_seconds=profile.settings.shutdown_deadline_seconds,
                deadline=deadline,
            )
            if stop.is_set():
                break
            exponent = max(
                0,
                runtime_scope.next_connection_attempt - 1,
            )
            delay = min(
                profile.settings.reconnect_maximum_backoff_seconds,
                profile.settings.reconnect_initial_backoff_seconds
                * (
                    profile.settings.reconnect_backoff_multiplier
                    ** exponent
                ),
            )
            if result == 0:
                delay = args.poll_seconds
            stop.wait(delay)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
