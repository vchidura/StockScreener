#!/usr/bin/env python3
"""Resume due C7 publication work without delivery or provider effects."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access import (  # noqa: E402
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
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
from realtime.portal_publisher import (  # noqa: E402
    PortalPublicationConfig,
    PortalPublicationRetryable,
    resume_portal_publication_once,
)
from realtime.preflight import ProviderTier  # noqa: E402
from realtime.worker_database import RealtimeProcessDatabase  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--claim-limit", type=int, default=16)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.poll_seconds <= 0
        or not 1 <= args.claim_limit <= 100
        or not args.continuous
        or not args.execute
    ):
        parser.error(
            "C7 publication retry requires bounded continuous execution"
        )
    return args


def _databases():
    config = RealtimeV2PipelineConfig.from_mapping({
        **{
            key: value
            for key, value in dotenv_values(
                BACKEND_DIR / ".env"
            ).items()
            if value is not None
        },
        **os.environ,
    },
        process=RealtimeV2Process.APP_WORKER,
    )
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
            statement_timeout_ms=300_000,
        ),
    )
    provider.verify()
    roles = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {
            RealtimeDatabaseRole.PUBLICATION: (
                f"{EXPECTED_V2_DATABASE_NAME}_app"
            ),
        },
    )
    publication = RealtimeProcessDatabase(
        RealtimeProcessDatabasePlan(
            "worker:portal_publisher:advanced_realtime_v3",
            RealtimeDatabaseRole.PUBLICATION,
            RealtimePoolBudget(0, 1),
        ),
        roles,
    )
    return provider, publication, pool


def load_due_publication_work_cursor(
    cursor,
    *,
    checked_at: datetime,
    limit: int,
) -> tuple[dict[str, object], ...]:
    cursor.execute(
        "SELECT product_key,source_receipt_id::text,cohort_id::text,"
        "provider_tier,release_class "
        "FROM rt.portal_publication_work "
        "WHERE (state IN ('PENDING','RETRY') "
        "AND next_attempt_at<=%s "
        "AND (lease_until IS NULL OR lease_until<=%s)) "
        "OR (state='CLAIMED' AND lease_until<=%s) "
        "ORDER BY next_attempt_at,cutoff,work_id LIMIT %s",
        (checked_at, checked_at, checked_at, limit),
    )
    return tuple({
        "product_key": str(row[0]),
        "source_receipt_id": UUID(str(row[1])),
        "cohort_id": UUID(str(row[2])),
        "provider_tier": ProviderTier(str(row[3])),
        "release_class": str(row[4]),
    } for row in cursor.fetchall())


def run_publication_retry_cycle(
    provider: V2ConnectionProvider,
    publication: RealtimeProcessDatabase,
    *,
    checked_at: datetime,
    claim_limit: int,
) -> dict[str, object]:
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        work = load_due_publication_work_cursor(
            unit.cursor,
            checked_at=checked_at,
            limit=claim_limit,
        )
    completed = 0
    deferred = 0
    terminal = []
    for item in work:
        try:
            result = resume_portal_publication_once(
                publication,
                PortalPublicationConfig(
                    cohort_id=item["cohort_id"],
                    provider_tier=item["provider_tier"],
                    owner_id=uuid5(
                        NAMESPACE_URL,
                        "advanced-realtime-v3-publication-retry:"
                        f"{item['cohort_id']}",
                    ),
                    owner_epoch=1,
                    dispatch_authorized=False,
                ),
                product_key=item["product_key"],
                source_receipt_id=item["source_receipt_id"],
                checked_at=checked_at,
                release_class=item["release_class"],
            )
            completed += result is not None
        except PortalPublicationRetryable:
            deferred += 1
        except ValueError as error:
            terminal.append({
                "source_receipt_id": str(item["source_receipt_id"]),
                "reason": str(error),
            })
    return {
        "status": "DEGRADED" if terminal else "READY",
        "checked_at_utc": checked_at.isoformat(),
        "due": len(work),
        "completed": completed,
        "deferred": deferred,
        "terminal": terminal,
        "provider_requests": 0,
        "delivery_effects": 0,
        "broker_effects": 0,
        "trading_effects": 0,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    provider, publication, pool = _databases()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        while not stop.is_set():
            report = run_publication_retry_cycle(
                provider,
                publication,
                checked_at=datetime.now(UTC),
                claim_limit=args.claim_limit,
            )
            print(json.dumps(report, sort_keys=True), flush=True)
            stop.wait(args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
