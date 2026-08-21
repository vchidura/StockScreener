#!/usr/bin/env python3
"""Scan today's structured trend-pullback setups; not a trading recommendation."""
from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor, get_selected_tickers  # noqa: E402
from research.features import load_daily_panel  # noqa: E402
from research.trend_pullback import build_trend_pullback_patterns  # noqa: E402


def _production_ranks(trade_date) -> dict[str, dict]:
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT ticker, side, decile, percentile, universe_size
            FROM cross_sectional_signals
            WHERE trade_date = %s AND model_version = 'xsmom-1.0'
        """, (trade_date,))
        return {row["ticker"]: dict(row) for row in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily structured pullback watch scan")
    parser.add_argument("--date", default=None)
    parser.add_argument("--tickers", default=None)
    parser.add_argument("--batch-size", type=int, default=10)
    args = parser.parse_args()

    tickers = ([ticker.strip().upper() for ticker in args.tickers.split(",")]
               if args.tickers else get_selected_tickers(active_only=True))
    signals: list[pd.DataFrame] = []
    scan_date = pd.Timestamp(args.date) if args.date else None

    for start in range(0, len(tickers), args.batch_size):
        batch = tickers[start:start + args.batch_size]
        panel = load_daily_panel(None, args.date, tickers=batch)
        if panel.empty:
            continue
        patterns = build_trend_pullback_patterns(panel)
        latest = scan_date if scan_date is not None else patterns["date"].max()
        current = patterns[(patterns["date"] == latest) & (patterns["direction"] != 0)].copy()
        if not current.empty:
            signals.append(current)
        print(
            "scanned {}/{} tickers".format(
                min(start + len(batch), len(tickers)), len(tickers)
            ),
            flush=True,
        )
        del panel, patterns, current
        gc.collect()

    if not signals:
        print("No structured trend-pullback triggers found.")
        return 0

    result = pd.concat(signals, ignore_index=True)
    trade_date = result["date"].max().date()
    ranks = _production_ranks(trade_date)
    rows = []
    for _, signal in result.iterrows():
        rank = ranks.get(signal["ticker"], {})
        rows.append({
            "ticker": signal["ticker"],
            "watch": "BULL_PULLBACK" if signal["direction"] == 1 else "BEAR_BOUNCE",
            "candle": signal["trigger_candle"],
            "close": round(float(signal["close"]), 2),
            "sma20": round(float(signal["sma20"]), 2),
            "sma50": round(float(signal["sma50"]), 2),
            "support_or_resistance": round(float(
                signal["prior_high"] if signal["direction"] == 1 else signal["prior_low"]
            ), 2),
            "xs_side": rank.get("side", "N/A"),
            "xs_decile": rank.get("decile"),
            "xs_percentile": round(float(rank["percentile"]), 3)
                if rank.get("percentile") is not None else None,
            "status": "UNVALIDATED_TIMING",
        })

    output = pd.DataFrame(rows).sort_values(["watch", "ticker"])
    print("{} structured trigger(s) for {}".format(len(output), trade_date))
    print(output.to_string(index=False))
    print("\nThese are watch candidates, not recommendations: the standalone pattern and")
    print("its xsmom overlay did not clear the predictive significance gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
