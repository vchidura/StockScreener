#!/usr/bin/env python3
"""Report split-sized session-over-session gaps in a daily bar lineage.

An unadjusted lineage shows a false gap on every split date because the raw
prices rebase. Run this against `--adjusted` to confirm a split-adjusted
lineage is clean, and against the default to see the contamination it removes.
Gaps that coincide with a recorded `equity_corporate_actions` split are
reported separately from unexplained ones.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor


QUERY = """
WITH visible AS (
    SELECT DISTINCT ON (ticker, bar_start)
        ticker, bar_start, close_price
    FROM equity_bar_revisions
    WHERE interval = %(interval)s
      AND session_scope = 'RTH'
      AND adjusted = %(adjusted)s
      AND is_final = TRUE
      AND NOT (
          availability_mode = 'HISTORICAL_RECONSTRUCTED'
          AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
      )
    ORDER BY ticker, bar_start, created_at DESC
),
moves AS (
    SELECT
        ticker,
        bar_start,
        close_price
            / LAG(close_price) OVER (PARTITION BY ticker ORDER BY bar_start)
            - 1 AS pct_change
    FROM visible
)
SELECT
    moves.ticker,
    moves.bar_start::date AS session,
    moves.pct_change,
    action.split_from,
    action.split_to
FROM moves
LEFT JOIN equity_corporate_actions AS action
    ON action.ticker = moves.ticker
   AND action.action_type = 'SPLIT'
   AND action.effective_date = moves.bar_start::date
WHERE moves.pct_change <= %(down)s OR moves.pct_change >= %(up)s
ORDER BY moves.ticker, moves.bar_start
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--adjusted", action="store_true")
    parser.add_argument("--down", type=float, default=-0.38)
    parser.add_argument("--up", type=float, default=0.60)
    parser.add_argument("--limit", type=int, default=40)
    args = parser.parse_args()

    with get_db_cursor() as cursor:
        cursor.execute(
            QUERY,
            {
                "interval": args.interval,
                "adjusted": args.adjusted,
                "down": args.down,
                "up": args.up,
            },
        )
        rows = cursor.fetchall()

    explained = [row for row in rows if row["split_from"] is not None]
    unexplained = [row for row in rows if row["split_from"] is None]

    lineage = "adjusted" if args.adjusted else "unadjusted"
    print(f"lineage={lineage} interval={args.interval}")
    print(f"thresholds: <= {args.down:.0%} or >= {args.up:.0%}")
    print(f"total gaps: {len(rows)}")
    print(f"  coinciding with a recorded split: {len(explained)}")
    print(f"  unexplained: {len(unexplained)}")
    print(f"  distinct tickers: {len({row['ticker'] for row in rows})}")

    if unexplained:
        print("\nunexplained gaps:")
        for row in unexplained[: args.limit]:
            print(f"  {row['ticker']:<8} {row['session']} {row['pct_change']:+.2%}")
        if len(unexplained) > args.limit:
            print(f"  ... {len(unexplained) - args.limit} more")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
