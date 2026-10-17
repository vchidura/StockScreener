#!/usr/bin/env python3
"""Mark the known unsealed Advanced 1m repair attempt ambiguous."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.acquisition_ledger import (  # noqa: E402
    ProviderRequestCompletion,
    ProviderRequestState,
    finish_provider_request_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import RealtimeV2PipelineConfig  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--requested-from", type=datetime.fromisoformat, required=True)
    parser.add_argument("--requested-to", type=datetime.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    args = parser.parse_args(argv)
    requested_from = args.requested_from.astimezone(timezone.utc)
    requested_to = args.requested_to.astimezone(timezone.utc)
    request_sha256 = canonical_sha256({
        "contract": "advanced-intraday-rest-repair-v1",
        "cohort_id": args.cohort_id,
        "ticker": "SPY",
        "interval": "1m",
        "requested_from": requested_from,
        "requested_to": requested_to,
        "authorization_reference": args.authorization_reference,
    })
    request_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-repair:{request_sha256}",
    )
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    pool = ThreadedConnectionPool(1, 1, dsn=config.app_dsn)
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix="stocks_realtime_v2",
        ),
    )
    provider.verify()
    try:
        with provider.transaction(APP_ROLE) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT state,reserved_at FROM rt.provider_request_ledger "
                "WHERE request_id=%s FOR UPDATE",
                (str(request_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] != "RESERVED":
                raise ValueError("unsealed repair reservation is unavailable")
            finish_provider_request_cursor(
                cursor,
                request_id,
                ProviderRequestCompletion(
                    ProviderRequestState.AMBIGUOUS,
                    started_at=row[1],
                    outcome=None,
                    failure_code="CLIENT_RESPONSE_NOT_SEALED",
                ),
            )
        print(json.dumps({
            "status": "AMBIGUOUS",
            "request_id": str(request_id),
            "provider_requests": 0,
            "database_writes": 1,
            "failure_code": "CLIENT_RESPONSE_NOT_SEALED",
        }, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
