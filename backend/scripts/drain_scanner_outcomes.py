#!/usr/bin/env python3
"""Drain a bounded number of derived scanner outcomes using the current entry model."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.scanner_events import evaluate_outcomes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", choices=["1d", "1h"], required=True)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--max-outcomes", type=int, default=10000)
    args = parser.parse_args()
    total_due = total_inserted = batches = 0
    while total_due < args.max_outcomes:
        limit = min(args.batch_size, args.max_outcomes - total_due)
        result = evaluate_outcomes(args.interval, limit=limit)
        batches += 1
        total_due += result["due"]
        total_inserted += result["inserted"]
        print({"batch": batches, **result}, flush=True)
        if result["due"] < limit:
            break
    print({
        "interval": args.interval, "batches": batches,
        "due": total_due, "inserted": total_inserted,
    }, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
