#!/usr/bin/env python3
"""Run durable off-hours preparation for desired Advanced Equity members."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_preparation import (  # noqa: E402
    AdvancedEquityPreparationResident,
    BoundedPolygonEquityPreparationClient,
    EquityPreparationLimits,
    MassiveEquityPreparationSource,
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


UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument("--maximum-concurrency", type=int, default=1)
    parser.add_argument(
        "--maximum-response-bytes-per-chunk",
        type=int,
        default=8 * 1024 * 1024,
    )
    parser.add_argument(
        "--maximum-response-bytes-per-member",
        type=int,
        default=2 * 1024 * 1024 * 1024,
    )
    parser.add_argument(
        "--maximum-requests-per-member",
        type=int,
        default=160,
    )
    parser.add_argument(
        "--maximum-account-requests",
        type=int,
        default=64_000,
    )
    parser.add_argument(
        "--maximum-account-response-bytes",
        type=int,
        default=512 * 1024 * 1024 * 1024,
    )
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--retry-initial-seconds", type=int, default=15)
    parser.add_argument("--retry-maximum-seconds", type=int, default=900)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error(
            "Advanced Equity preparation targets only stocks_realtime_v2"
        )
    if args.poll_seconds <= 0 or args.poll_seconds > 300:
        parser.error("preparation poll interval must be in (0, 300]")
    try:
        args.limits = EquityPreparationLimits(
            maximum_concurrency=args.maximum_concurrency,
            maximum_response_bytes_per_chunk=(
                args.maximum_response_bytes_per_chunk
            ),
            maximum_response_bytes_per_member=(
                args.maximum_response_bytes_per_member
            ),
            maximum_requests_per_member=(
                args.maximum_requests_per_member
            ),
            maximum_account_requests=(
                args.maximum_account_requests
            ),
            maximum_account_response_bytes=(
                args.maximum_account_response_bytes
            ),
            lease_seconds=args.lease_seconds,
            retry_initial_seconds=args.retry_initial_seconds,
            retry_maximum_seconds=args.retry_maximum_seconds,
        )
    except ValueError as error:
        parser.error(str(error))
    if not args.execute:
        parser.error("Advanced Equity preparation requires --execute")
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


def _payload(result) -> str:
    return json.dumps({
        "status": result.status,
        "checked_at": result.checked_at.isoformat(),
        "ticker": result.ticker,
        "stage": result.stage,
        "attempts": result.attempts,
        "next_retry_at": (
            None
            if result.next_retry_at is None
            else result.next_retry_at.isoformat()
        ),
        "reason": result.reason,
        "reconciliation_state": result.reconciliation_state,
        "provider_fetch_performed": (
            result.status in {"PROGRESSED", "COMPLETE"}
            and result.stage
            in {"REFERENCE", "ADJUSTED_DAILY", "NATIVE_INTRADAY"}
        ),
    }, sort_keys=True)


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.EQUITY_PREPARATION,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced Equity preparation requires Advanced equity "
            "and disabled options"
        )
    provider, pool = _provider(config)
    resident = AdvancedEquityPreparationResident(
        provider,
        MassiveEquityPreparationSource(
            BoundedPolygonEquityPreparationClient(
                config.massive_api_key
            )
        ),
        owner_id=uuid4(),
        limits=args.limits,
        clock=_now,
    )
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    try:
        while not stop.is_set():
            result = resident.run_once()
            print(_payload(result))
            if args.once:
                return 0 if result.status != "BLOCKED" else 1
            wait_seconds = args.poll_seconds
            if result.next_retry_at is not None:
                wait_seconds = min(
                    wait_seconds,
                    max(
                        0.0,
                        (result.next_retry_at - _now()).total_seconds(),
                    ),
                )
            stop.wait(wait_seconds)
    finally:
        pool.closeall()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
