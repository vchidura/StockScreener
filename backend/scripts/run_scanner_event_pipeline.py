#!/usr/bin/env python3
"""Capture shadow scanner events and incrementally evaluate due outcomes."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.scanner_events import (
    backfill_events, capture_events, ensure_tables, evaluate_outcomes,
    qualification_backfill, reset_stale_outcomes,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("scanner-events")


def run_pipeline(intervals: tuple[str, ...] = ("1d", "1h"),
                 as_of: str | None = None) -> dict:
    """Evaluate prior events first, then capture the latest completed bar."""
    ensure_tables()
    results = {}
    for interval in intervals:
        evaluated = evaluate_outcomes(interval)
        captured = capture_events(interval, as_of=as_of)
        results[interval] = {"evaluated": evaluated, "captured": captured}
        logger.info("[%s] evaluated=%s captured=%s", interval, evaluated, captured)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Shadow scanner event pipeline")
    parser.add_argument("--interval", choices=["1d", "1wk", "1h", "all"], default="all")
    parser.add_argument("--date", default=None, help="Latest eligible date YYYY-MM-DD")
    parser.add_argument("--capture-only", action="store_true")
    parser.add_argument("--evaluate-only", action="store_true")
    parser.add_argument(
        "--backfill-sessions", type=int, default=None,
        help="Replay the latest N complete sessions, then evaluate matured outcomes",
    )
    parser.add_argument(
        "--qualification-start", default=None,
        help="Replay from YYYY-MM-DD using point-in-time data availability and discovery cohorts",
    )
    parser.add_argument("--qualification-end", default=None, help="Optional YYYY-MM-DD end")
    parser.add_argument("--ticker-offset", type=int, default=0)
    parser.add_argument("--ticker-limit", type=int, default=None)
    parser.add_argument("--skip-evaluation", action="store_true")
    parser.add_argument("--evaluation-limit", type=int, default=5000)
    parser.add_argument("--drain-outcomes", action="store_true")
    parser.add_argument("--reset-stale-outcomes", action="store_true")
    args = parser.parse_args()
    if args.capture_only and args.evaluate_only:
        parser.error("--capture-only and --evaluate-only are mutually exclusive")
    if args.backfill_sessions is not None and (args.capture_only or args.evaluate_only or args.date):
        parser.error("--backfill-sessions cannot be combined with capture/evaluate-only or --date")
    if args.qualification_start and (
        args.backfill_sessions is not None or args.capture_only or args.evaluate_only or args.date
    ):
        parser.error("--qualification-start cannot be combined with other pipeline modes")
    if args.qualification_end and not args.qualification_start:
        parser.error("--qualification-end requires --qualification-start")
    if (args.ticker_offset or args.ticker_limit is not None or args.skip_evaluation) \
            and not args.qualification_start:
        parser.error("ticker chunk and skip-evaluation options require --qualification-start")
    if args.drain_outcomes and not args.evaluate_only:
        parser.error("--drain-outcomes requires --evaluate-only")
    if args.reset_stale_outcomes and not args.evaluate_only:
        parser.error("--reset-stale-outcomes requires --evaluate-only")
    intervals = ("1d", "1wk", "1h") if args.interval == "all" else (args.interval,)
    ensure_tables()
    for interval in intervals:
        if args.qualification_start:
            backfilled = qualification_backfill(
                interval, args.qualification_start, args.qualification_end,
                ticker_offset=args.ticker_offset, ticker_limit=args.ticker_limit,
            )
            evaluated = (
                {"skipped": True} if args.skip_evaluation else evaluate_outcomes(interval)
            )
            print(interval, {"qualified": backfilled, "evaluated": evaluated})
        elif args.backfill_sessions is not None:
            backfilled = backfill_events(interval, sessions=args.backfill_sessions)
            evaluated = evaluate_outcomes(interval)
            print(interval, {"backfilled": backfilled, "evaluated": evaluated})
        elif args.capture_only:
            print(interval, capture_events(interval, as_of=args.date))
        elif args.evaluate_only:
            if args.reset_stale_outcomes:
                print(interval, {"reset": reset_stale_outcomes(interval)}, flush=True)
            if args.drain_outcomes:
                total_due = total_inserted = batches = 0
                while True:
                    result = evaluate_outcomes(interval, limit=args.evaluation_limit)
                    total_due += result["due"]
                    total_inserted += result["inserted"]
                    batches += 1
                    print(interval, {"batch": batches, **result}, flush=True)
                    if result["due"] < args.evaluation_limit:
                        break
                print(interval, {
                    "batches": batches, "due": total_due,
                    "inserted": total_inserted, "complete": True,
                })
            else:
                print(interval, evaluate_outcomes(interval, limit=args.evaluation_limit))
        else:
            print(interval, {
                "evaluated": evaluate_outcomes(interval),
                "captured": capture_events(interval, as_of=args.date),
            })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
