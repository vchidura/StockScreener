#!/usr/bin/env python3
"""Find retained corporate actions that conflict with current ticker identities."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from scripts.run_corporate_action_worker import (  # noqa: E402
    LOOKBACK_DAYS,
    SPLIT_LOOKBACK_DAYS,
    active_security_ids,
)


def main() -> int:
    observed_at = datetime.now(timezone.utc)
    starts = {
        "SPLIT": observed_at.date() - timedelta(days=SPLIT_LOOKBACK_DAYS),
        "DIVIDEND": observed_at.date() - timedelta(days=LOOKBACK_DAYS),
    }
    start = min(starts.values())
    security_ids = active_security_ids(observed_at)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cursor.execute("""
            SELECT corporate_action_id,ticker,action_type,effective_date,
                   security_id,source_key,first_observed_at,created_at
            FROM equity_corporate_actions
            WHERE source='POLYGON_CORPORATE_ACTIONS_V1'
              AND ticker=ANY(%s) AND effective_date BETWEEN %s AND %s
            ORDER BY ticker,action_type,effective_date,source_key
        """, (sorted(security_ids), start, observed_at.date() + timedelta(days=365)))
        actions = [dict(row) for row in cursor.fetchall()]
    in_scope = [row for row in actions if row["effective_date"] >= starts[row["action_type"]]]
    conflicts = [dict(row, expected_security_id=str(security_ids[row["ticker"]]))
        for row in in_scope if str(row["security_id"]) != str(security_ids[row["ticker"]])]
    print(json.dumps({
        "version": "corporate_action_identity_conflicts_v1",
        "mode": "READ_ONLY",
        "as_of": observed_at.isoformat(),
        "window_start": {key: value.isoformat() for key, value in starts.items()},
        "active_tickers": len(security_ids),
        "retained_actions": len(in_scope),
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "source_writes": 0,
    }, indent=2, default=str))
    return 1 if conflicts else 0


if __name__ == "__main__":
    raise SystemExit(main())
