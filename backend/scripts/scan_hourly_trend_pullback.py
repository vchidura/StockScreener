#!/usr/bin/env python3
"""Scan the latest regular-session 1h bar for structured pullback watches."""
from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_selected_tickers  # noqa: E402
from research.trend_pullback import build_trend_pullback_patterns, load_hourly_panel  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Latest hourly pullback watch scan")
    parser.add_argument("--tickers", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--lookback-days", type=int, default=120)
    parser.add_argument("--batch-size", type=int, default=10)
    args = parser.parse_args()

    tickers = ([ticker.strip().upper() for ticker in args.tickers.split(",")]
               if args.tickers else get_selected_tickers(active_only=True))
    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.now(tz="America/New_York")
    start = (end - pd.Timedelta(days=args.lookback_days)).date().isoformat()
    end_date = end.date().isoformat()
    latest_rows = []

    for offset in range(0, len(tickers), args.batch_size):
        batch = tickers[offset:offset + args.batch_size]
        panel = load_hourly_panel(start, end_date, batch)
        if panel.empty:
            continue
        patterns = build_trend_pullback_patterns(panel)
        latest = patterns["date"].max()
        current = patterns[(patterns["date"] == latest) & (patterns["direction"] != 0)].copy()
        if not current.empty:
            latest_rows.append(current)
        print("scanned {}/{} tickers".format(min(offset + len(batch), len(tickers)), len(tickers)),
              flush=True)
        del panel, patterns, current
        gc.collect()

    if not latest_rows:
        print("Latest hourly bar: 0 structured trigger(s)")
        return 0

    result = pd.concat(latest_rows, ignore_index=True)
    latest_time = result["date"].max()
    result = result[result["date"] == latest_time].copy()
    result["watch"] = result["direction"].map({1: "BULL_PULLBACK", -1: "BEAR_BOUNCE"})
    result["status"] = "UNVALIDATED_TIMING"
    columns = [
        "ticker", "watch", "trigger_candle", "close", "sma20", "sma50",
        "last_high", "prior_high", "last_low", "prior_low", "atr14", "status",
    ]
    print(f"Latest hourly bar {latest_time}: {len(result)} structured trigger(s)")
    print(result[columns].sort_values(["watch", "ticker"]).to_string(index=False))
    print("\nHourly pattern research did not demonstrate positive predictive alpha.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
