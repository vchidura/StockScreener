"""Measure how much the composite scanners duplicate each other.

Uses only event identity, never outcomes, so it cannot overfit the evaluation sample.
Overlapping detectors inflate the FDR family without adding independent hypotheses.
"""
from __future__ import annotations

import argparse
import sys
from itertools import combinations
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor  # noqa: E402


def load_signal_keys(interval: str) -> dict[str, set[tuple]]:
    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute("""
            SELECT scanner_name, ticker, trade_date, direction
            FROM scanner_events WHERE interval = %s
        """, (interval,))
        rows = cur.fetchall()
    keys: dict[str, set[tuple]] = {}
    for scanner_name, ticker, trade_date, direction in rows:
        keys.setdefault(scanner_name, set()).add((ticker, trade_date, direction))
    return keys


def report(interval: str) -> None:
    keys = load_signal_keys(interval)
    if not keys:
        print(f"[{interval}] no events")
        return

    print(f"\n=== {interval}: signal-day footprint ===")
    for name in sorted(keys, key=lambda n: -len(keys[n])):
        print(f"  {name:32s} {len(keys[name]):>7,} ticker/date/direction keys")

    print(f"\n=== {interval}: pairwise overlap ===")
    print(f"  {'scanner A':32s} {'scanner B':32s} {'shared':>7s} "
          f"{'jaccard':>8s} {'A covered':>10s} {'B covered':>10s}")
    pairs = []
    for left, right in combinations(sorted(keys), 2):
        shared = keys[left] & keys[right]
        if not shared:
            continue
        union = keys[left] | keys[right]
        pairs.append((
            len(shared) / len(union), left, right, len(shared),
            len(shared) / len(keys[left]), len(shared) / len(keys[right]),
        ))
    for jaccard, left, right, shared, left_pct, right_pct in sorted(pairs, reverse=True):
        print(f"  {left:32s} {right:32s} {shared:>7,} "
              f"{jaccard:>8.3f} {left_pct:>9.1%} {right_pct:>9.1%}")
    if not pairs:
        print("  (no shared signal days)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--intervals", default="1d,1h")
    args = parser.parse_args()
    for interval in [value.strip() for value in args.intervals.split(",") if value.strip()]:
        report(interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
