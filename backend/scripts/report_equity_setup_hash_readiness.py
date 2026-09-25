#!/usr/bin/env python3
"""Verify current retained equity setup payload hashes for the Options universe."""
from __future__ import annotations

from datetime import datetime, timezone
from copy import deepcopy
import json
from pathlib import Path
import sys

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from equity.materialization import SETUP_VERSION  # noqa: E402
from equity.polygon import sha256_json  # noqa: E402
from options.repositories.alert_review_sources import configured_detector_launch  # noqa: E402


def signed_zero_recovery(payload, wanted_sha256):
    zero_paths = []

    def visit(value, path=()):
        if isinstance(value, dict):
            for key, item in value.items():
                visit(item, (*path, key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                visit(item, (*path, index))
        elif isinstance(value, float) and value == 0.0:
            zero_paths.append(path)

    visit(payload)
    for path in zero_paths:
        candidate = deepcopy(payload)
        parent = candidate
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = -0.0
        if sha256_json(candidate) == wanted_sha256:
            return {"zero_float_count": len(zero_paths), "recovered_path": list(path)}
    candidate = deepcopy(payload)
    for path in zero_paths:
        parent = candidate
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = -0.0
    return {"zero_float_count": len(zero_paths),
        "recovered_path": "ALL" if sha256_json(candidate) == wanted_sha256 else None}


def main() -> int:
    launch = configured_detector_launch()
    if launch is None:
        raise ValueError("detector launch is not configured")
    as_of = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cursor.execute("""
            SELECT DISTINCT ON(ticker,interval,direction)
                   evidence_id,ticker,interval,direction,market_time,observed_at,
                   valid_until,payload,payload_sha256,created_at
            FROM equity_evidence
            WHERE ticker=ANY(%s) AND evidence_type='TRADE_SETUP'
              AND source_name='EQUITY_SETUP' AND source_version=%s
              AND interval IN ('30m','1h')
              AND observed_at<=%s AND created_at<=%s
            ORDER BY ticker,interval,direction,market_time DESC,observed_at DESC
        """, (list(launch.underlyers), SETUP_VERSION, as_of, as_of))
        rows = [dict(row) for row in cursor.fetchall()]
    results = []
    for row in rows:
        computed = sha256_json(row["payload"])
        results.append({"evidence_id": str(row["evidence_id"]), "ticker": row["ticker"],
            "interval": row["interval"], "direction": row["direction"],
            "market_time": row["market_time"], "valid_until": row["valid_until"],
            "stored_sha256": row["payload_sha256"], "computed_sha256": computed,
            "hash_matches": computed == row["payload_sha256"],
            "signed_zero": None if computed == row["payload_sha256"] else
                signed_zero_recovery(row["payload"], row["payload_sha256"])})
    print(json.dumps({"version": "equity_setup_hash_readiness_v1", "mode": "READ_ONLY",
        "as_of": as_of, "dataset_id": launch.dataset_id, "underlyers": len(launch.underlyers),
        "setups": len(results), "matching": sum(row["hash_matches"] for row in results),
        "mismatching": sum(not row["hash_matches"] for row in results),
        "mismatches": [row for row in results if not row["hash_matches"]],
        "source_writes": 0}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
