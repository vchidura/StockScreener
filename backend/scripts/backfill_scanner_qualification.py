"""Replay scanner events over a qualification window and evaluate due outcomes."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.scanner_events import evaluate_outcomes, qualification_backfill  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", choices=("1d", "1wk", "1h"), default="1d")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", default=None)
    parser.add_argument("--ticker-offset", type=int, default=0)
    parser.add_argument("--ticker-limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument(
        "--refresh-existing-only", action="store_true",
        help="Refresh metadata only for existing lifecycle keys",
    )
    parser.add_argument(
        "--scanners",
        default=None,
        help="Comma-separated scanner names; default replays every registered scanner",
    )
    parser.add_argument("--evaluate", action="store_true")
    parser.add_argument("--outcome-limit", type=int, default=5000)
    args = parser.parse_args()

    scanner_names = (
        {value.strip() for value in args.scanners.split(",") if value.strip()}
        if args.scanners else None
    )
    result = qualification_backfill(
        args.interval,
        args.start,
        args.end,
        ticker_offset=args.ticker_offset,
        ticker_limit=args.ticker_limit,
        scanner_names=scanner_names,
        batch_size=args.batch_size,
        refresh_existing_only=args.refresh_existing_only,
    )
    print("backfill", result, flush=True)

    if args.evaluate:
        total_inserted = 0
        while True:
            outcome_result = evaluate_outcomes(args.interval, limit=args.outcome_limit)
            total_inserted += int(outcome_result["inserted"])
            print("outcomes", outcome_result, flush=True)
            if not outcome_result["due"]:
                break
        print(f"outcomes_inserted={total_inserted}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())