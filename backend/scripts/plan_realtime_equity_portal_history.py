#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan native-5m realtime portal history without I/O."
    )
    parser.add_argument("--session-date", required=True, type=date.fromisoformat)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_realtime_equity_portal_history_plan(args.session_date)
    print(json.dumps({
        "mode": "PLAN",
        "session_date": plan.session_date.isoformat(),
        "tickers": EQUITY_VALIDATION_UNIVERSE.equity_tickers,
        "native_interval": "5m",
        "native_one_minute_sessions": len(plan.one_minute_sessions),
        "native_adjusted_monthly_periods": len(plan.monthly_sessions),
        "native_adjusted_weekly_periods": len(plan.weekly_sessions),
        "weekly_start": plan.weekly_start.isoformat(),
        "weekly_end": plan.weekly_end.isoformat(),
        "adjusted_daily_sessions": len(plan.adjusted_sessions),
        "adjusted_start": plan.adjusted_sessions[0].isoformat(),
        "adjusted_end": plan.adjusted_sessions[-1].isoformat(),
        "raw_intraday_sessions": len(plan.raw_sessions),
        "raw_start": plan.raw_start.isoformat(),
        "raw_end": plan.raw_end.isoformat(),
        "one_minute_start": plan.one_minute_start.isoformat(),
        "one_minute_end": plan.one_minute_end.isoformat(),
        "monthly_start": plan.monthly_start.isoformat(),
        "monthly_end": plan.monthly_end.isoformat(),
        "expected_counts_per_ticker": [
            {"interval": interval, "basis": basis, "count": count}
            for interval, basis, count in plan.expected_counts_per_ticker
        ],
        "expected_provider_requests": plan.expected_provider_requests,
        "expected_facts_per_ticker": plan.expected_facts_per_ticker,
        "expected_total_facts": plan.expected_total_facts,
        "maximum_rows_per_provider_response": plan.maximum_rows_per_provider_response,
        "maximum_persist_batch": plan.maximum_persist_batch,
        "policy_sha256": plan.policy_sha256,
        "plan_sha256": plan.sha256,
        "provider_requests_performed": 0,
        "database_connections": 0,
        "database_writes": 0,
        "values_exposed": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
