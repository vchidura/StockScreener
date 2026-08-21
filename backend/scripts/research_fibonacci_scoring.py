"""Compare Fibonacci scoring variants with point-in-time next-open outcomes.

Examples:
    python backend/scripts/research_fibonacci_scoring.py --start 2024-01-01
    python backend/scripts/research_fibonacci_scoring.py --start 2025-01-01 --tickers MU,AAPL
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_selected_tickers  # noqa: E402
from research.features import load_daily_panel  # noqa: E402
from research.fibonacci_scoring import (  # noqa: E402
    DEFAULT_HORIZONS,
    build_variant_signals,
    evaluate_variant_outcomes,
    summarize_variant_outcomes,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("fibonacci-scoring-research")
pd.set_option("display.width", 180)
pd.set_option("display.max_columns", 30)
pd.set_option("display.float_format", lambda value: f"{value:,.4f}")


def _parse_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _format_report(report: pd.DataFrame) -> pd.DataFrame:
    display = report.copy()
    display["direction"] = display["direction"].map({1: "LONG", -1: "SHORT"})
    for column in (
        "mean_net_return", "mean_net_alpha", "early_alpha", "late_alpha",
        "hit_rate", "mean_mae_pct", "mean_mfe_pct",
    ):
        display[column] = display[column] * 100
    return display


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Point-in-time Fibonacci scoring variant comparison"
    )
    parser.add_argument("--start", default="2024-01-01", help="First signal date")
    parser.add_argument("--end", default=None, help="Last loaded date (YYYY-MM-DD)")
    parser.add_argument("--tickers", default=None, help="Comma-separated ticker subset")
    parser.add_argument("--ticker-limit", type=int, default=None)
    parser.add_argument("--horizons", default="5,10,21")
    parser.add_argument("--proximity-pct", type=float, default=1.5)
    parser.add_argument("--cost-bps", type=float, default=4.0)
    parser.add_argument("--output", default=None, help="Optional JSON report path")
    args = parser.parse_args()

    horizons = tuple(int(value) for value in args.horizons.split(",") if value.strip())
    if not horizons or any(value <= 0 for value in horizons):
        parser.error("--horizons must contain positive integers")
    tickers = _parse_csv(args.tickers) or get_selected_tickers(True)
    if args.ticker_limit is not None:
        tickers = tickers[:max(0, args.ticker_limit)]
    if not tickers:
        logger.error("No tickers selected.")
        return 1

    logger.info("Loading daily panel | tickers=%s | signals from %s", len(tickers), args.start)
    panel = load_daily_panel(end=args.end, tickers=tickers)
    if panel.empty:
        logger.error("No daily data available.")
        return 1
    logger.info(
        "Replaying variants | rows=%s | dates=%s..%s",
        f"{len(panel):,}", panel["date"].min().date(), panel["date"].max().date(),
    )
    signals = build_variant_signals(
        panel,
        evaluation_start=args.start,
        proximity_pct=args.proximity_pct,
    )
    if signals.empty:
        logger.error("No Fibonacci proximity events found.")
        return 1
    outcomes = evaluate_variant_outcomes(
        panel,
        signals,
        horizons=horizons or DEFAULT_HORIZONS,
        round_trip_cost_bps=args.cost_bps,
    )
    report = summarize_variant_outcomes(outcomes, panel["date"].unique())

    print("\nFIBONACCI SCORING VARIANT STUDY")
    print("Entry: next session open | Exit: horizon close | Cost: "
          f"{args.cost_bps:.1f} bps round trip")
    print("Signals are point-in-time; multi-leg emits one capped directional vote and "
          "suppresses conflicting support/resistance proximity.\n")
    print(_format_report(report).to_string(index=False))
    print("\nPercent columns: mean_net_return, mean_net_alpha, early_alpha, late_alpha, "
          "hit_rate, mean_mae_pct, mean_mfe_pct.")

    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "parameters": {
                "start": args.start,
                "end": args.end,
                "tickers": len(tickers),
                "horizons": list(horizons),
                "proximity_pct": args.proximity_pct,
                "round_trip_cost_bps": args.cost_bps,
                "entry_model": "next_bar_open_v2",
            },
            "signal_counts": {
                str(key): int(value)
                for key, value in signals.groupby("variant").size().items()
            },
            "report": json.loads(report.to_json(orient="records", date_format="iso")),
        }
        output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info("Wrote %s", output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())