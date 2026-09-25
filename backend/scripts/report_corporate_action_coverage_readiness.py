#!/usr/bin/env python3
"""Report current response-bound corporate-action coverage without writes."""
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
from options.repositories.alert_review_sources import configured_detector_launch  # noqa: E402
from scripts.run_corporate_action_worker import LOOKBACK_DAYS, SPLIT_LOOKBACK_DAYS  # noqa: E402


def main() -> int:
    launch = configured_detector_launch()
    if launch is None:
        raise ValueError("detector launch is not configured")
    as_of = datetime.now(timezone.utc)
    expected_starts = {
        "SPLIT": as_of.date() - timedelta(days=SPLIT_LOOKBACK_DAYS),
        "DIVIDEND": as_of.date() - timedelta(days=LOOKBACK_DAYS),
    }
    tickers = sorted(launch.underlyers)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cursor.execute("""
            SELECT DISTINCT ON(ticker,action_type)
                   coverage_id,ticker,action_type,window_start,window_end,
                   first_observed_at,created_at,availability_mode,
                   response_action_count,response_sha256
            FROM equity_corporate_action_coverage
            WHERE source='POLYGON_CORPORATE_ACTIONS_V1'
              AND ticker=ANY(%s) AND first_observed_at<=%s AND created_at<=%s
            ORDER BY ticker,action_type,first_observed_at DESC,created_at DESC
        """, (tickers, as_of, as_of))
        rows = [dict(row) for row in cursor.fetchall()]
    latest = {(row["ticker"], row["action_type"]): row for row in rows}
    ready = []
    missing = []
    for ticker in tickers:
        reasons = []
        for action_type in ("SPLIT", "DIVIDEND"):
            row = latest.get((ticker, action_type))
            if row is None:
                reasons.append(f"{action_type}_COVERAGE_MISSING")
            elif row["window_start"] > expected_starts[action_type]:
                reasons.append(f"{action_type}_WINDOW_TOO_NARROW")
            elif row["availability_mode"] != "LIVE_OBSERVED":
                reasons.append(f"{action_type}_NOT_LIVE_OBSERVED")
            elif row["response_action_count"] is None or row["response_sha256"] is None:
                reasons.append(f"{action_type}_RESPONSE_UNBOUND")
        (missing if reasons else ready).append({"ticker": ticker, "reasons": reasons})
    observations = sorted({row["first_observed_at"].isoformat() for row in rows})
    print(json.dumps({
        "version": "corporate_action_coverage_readiness_v1",
        "mode": "READ_ONLY",
        "dataset_id": launch.dataset_id,
        "as_of": as_of.isoformat(),
        "lookback_days": {"SPLIT": SPLIT_LOOKBACK_DAYS, "DIVIDEND": LOOKBACK_DAYS},
        "expected_window_start": {key: value.isoformat() for key, value in expected_starts.items()},
        "underlyers": len(tickers),
        "ready_underlyers": len(ready),
        "blocked_underlyers": len(missing),
        "latest_observed_at": max(observations) if observations else None,
        "observation_count": len(observations),
        "blocked": missing,
        "coverage": rows,
        "source_writes": 0,
    }, indent=2, default=str))
    return 0 if not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())
