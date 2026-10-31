#!/usr/bin/env python3
"""Run the bounded Advanced Equity capability refresh resident."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_capability_refresh import (  # noqa: E402
    CAPABILITY_REFRESH_RESOURCE,
    MASSIVE_STOCKS_CONNECTION_RESOURCE,
    AdvancedGatewayCapabilityRefreshSource,
    AdvancedCapabilityRefreshResident,
    load_capability_refresh_request_cursor,
    persist_capability_refresh_outcome_cursor,
)
from realtime.advanced_equity_lifecycle import (  # noqa: E402
    claim_lifecycle_lease_cursor,
    release_lifecycle_lease_cursor,
    renew_lifecycle_lease_cursor,
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


def _now():
    return datetime.now(UTC)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument(
        "--refresh-before-seconds",
        type=int,
        default=1800,
    )
    parser.add_argument(
        "--evidence-validity-seconds",
        type=int,
        default=86_400,
    )
    parser.add_argument("--retry-initial-seconds", type=int, default=15)
    parser.add_argument("--retry-maximum-seconds", type=int, default=300)
    parser.add_argument(
        "--probe-timeout-seconds",
        type=float,
        default=10.0,
    )
    parser.add_argument(
        "--probe-byte-ceiling",
        type=int,
        default=64 * 1024,
    )
    parser.add_argument("--lease-seconds", type=int, default=45)
    parser.add_argument(
        "--status-cadence-seconds",
        type=float,
        default=60.0,
    )
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error(
            "capability refresh targets only stocks_realtime_v2"
        )
    if (
        args.poll_seconds <= 0
        or args.refresh_before_seconds < 0
        or args.evidence_validity_seconds < 300
        or args.retry_initial_seconds <= 0
        or args.retry_maximum_seconds < args.retry_initial_seconds
        or not 0 < args.probe_timeout_seconds < args.lease_seconds
        or not 1024 <= args.probe_byte_ceiling <= 1024 * 1024
        or not 10 <= args.lease_seconds <= 3600
        or args.poll_seconds >= args.lease_seconds
        or args.status_cadence_seconds < args.poll_seconds
    ):
        parser.error("capability refresh timing is invalid")
    if not args.execute:
        parser.error("capability refresh requires --execute")
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


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.CAPABILITY_REFRESH,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "capability refresh requires Advanced equity "
            "and disabled options"
        )
    provider, pool = _provider(config)
    source = AdvancedGatewayCapabilityRefreshSource(
        api_key=config.massive_api_key,
        clock=_now,
        evidence_validity_seconds=args.evidence_validity_seconds,
        probe_timeout_seconds=args.probe_timeout_seconds,
        probe_byte_ceiling=args.probe_byte_ceiling,
    )
    resident = AdvancedCapabilityRefreshResident(
        source,
        retry_initial_seconds=args.retry_initial_seconds,
        retry_maximum_seconds=args.retry_maximum_seconds,
    )
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_args: stop.set())
    signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    lease = None
    probe_owner_id = uuid4()
    next_control_at = None
    try:
        owner_id = uuid4()
        with provider.transaction(APP_ROLE) as unit:
            lease = claim_lifecycle_lease_cursor(
                unit.cursor,
                owner_id=owner_id,
                checked_at=_now(),
                lease_seconds=args.lease_seconds,
                resource_key=CAPABILITY_REFRESH_RESOURCE,
            )
        if lease is None:
            raise RuntimeError(
                "another capability refresh resident is active"
            )
        while not stop.is_set():
            checked = _now()
            with provider.transaction(APP_ROLE) as unit:
                lease = renew_lifecycle_lease_cursor(
                    unit.cursor,
                    lease,
                    checked_at=checked,
                    lease_seconds=args.lease_seconds,
                )
            if lease is None:
                raise RuntimeError(
                    "capability refresh leadership was lost"
                )
            if (
                next_control_at is not None
                and checked < next_control_at
            ):
                stop.wait(args.poll_seconds)
                continue
            with provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                request = load_capability_refresh_request_cursor(
                    unit.cursor,
                    checked_at=checked,
                    refresh_before_seconds=(
                        args.refresh_before_seconds
                    ),
                    evidence_validity_seconds=(
                        args.evidence_validity_seconds
                    ),
                )
            if request is None:
                stop.wait(args.poll_seconds)
                continue
            connection_lease = None
            probe_due = resident.provider_probe_due(
                request,
                checked_at=checked,
            )
            if probe_due:
                with provider.transaction(APP_ROLE) as unit:
                    connection_lease = claim_lifecycle_lease_cursor(
                        unit.cursor,
                        owner_id=probe_owner_id,
                        checked_at=checked,
                        lease_seconds=args.lease_seconds,
                        resource_key=(
                            MASSIVE_STOCKS_CONNECTION_RESOURCE
                        ),
                    )
            if (
                probe_due
                and connection_lease is None
            ):
                outcome = resident.connection_unavailable(
                    request,
                    checked_at=checked,
                )
            else:
                try:
                    outcome = resident.step(
                        request,
                        checked_at=checked,
                    )
                finally:
                    if connection_lease is not None:
                        with provider.transaction(APP_ROLE) as unit:
                            release_lifecycle_lease_cursor(
                                unit.cursor,
                                connection_lease,
                            )
            with provider.transaction(APP_ROLE) as unit:
                persist_capability_refresh_outcome_cursor(
                    unit.cursor,
                    outcome,
                    owner_epoch=lease.owner_epoch,
                    lease_until=lease.lease_until,
                )
            print(json.dumps({
                "status": outcome.status,
                "checked_at": outcome.checked_at.isoformat(),
                "last_success_at": (
                    None
                    if outcome.last_success_at is None
                    else outcome.last_success_at.isoformat()
                ),
                "next_retry_at": (
                    None
                    if outcome.next_retry_at is None
                    else outcome.next_retry_at.isoformat()
                ),
                "reason_code": outcome.reason_code,
                "profile_id": str(outcome.profile_id),
                "provider_requests": (
                    None
                    if outcome.status == "DEGRADED"
                    else outcome.evidence.provider_requests
                    if outcome.evidence is not None
                    else 0
                ),
                "credentials_exposed": False,
            }, separators=(",", ":")))
            next_control_at = min(
                (
                    outcome.next_retry_at
                    if outcome.next_retry_at is not None
                    else checked
                    + timedelta(
                        seconds=args.status_cadence_seconds
                    )
                ),
                checked
                + timedelta(seconds=args.status_cadence_seconds),
            )
            stop.wait(args.poll_seconds)
        return 0
    finally:
        if lease is not None:
            with provider.transaction(APP_ROLE) as unit:
                release_lifecycle_lease_cursor(unit.cursor, lease)
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
