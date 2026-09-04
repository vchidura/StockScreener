#!/usr/bin/env python3
"""Independently check the composite scanners for lookahead and payload sanity.

Reimplementing `_level_context` would only prove a second author can repeat the
same mistake, so this verifies two properties the detectors must satisfy
regardless of how they are written.

Prefix invariance: recomputing the detectors on history truncated at bar T must
reproduce exactly the events stamped on bar T. Any read of a later bar - a
centred rolling window, an unshifted pivot confirmation, a level anchored to a
future swing - changes the result and is reported. This is what the 2-bar right
confirmation in `_confirmed_swings` makes worth testing.

Payload invariants: entry equals the signal bar close, the stop sits on the
losing side of entry, and the setup anchor never postdates the signal.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from research.composite_scanners import build_all_scanner_events  # noqa: E402


PANEL_QUERY = """
SELECT DISTINCT ON (ticker, bar_start)
    ticker,
    bar_start::date AS date,
    open_price AS open,
    high_price AS high,
    low_price AS low,
    close_price AS close,
    volume
FROM equity_bar_revisions
WHERE interval = '1d'
  AND session_scope = 'RTH'
  AND adjusted = %(adjusted)s
  AND is_final = TRUE
  AND ticker = ANY(%(tickers)s)
  AND NOT (
      availability_mode = 'HISTORICAL_RECONSTRUCTED'
      AND quality_codes @> ARRAY['GROUPED_DAILY_AGGREGATE']::TEXT[]
  )
ORDER BY ticker, bar_start, created_at DESC
"""

# Columns that must match exactly; `metadata` is compared separately as JSON.
COMPARE_COLUMNS = [
    "scanner_name", "ticker", "direction", "trigger_type", "setup_anchor",
    "entry_price", "atr_at_signal", "reference_level", "stop_price",
    "target_price",
]


def load_panel(tickers: list[str], adjusted: bool) -> pd.DataFrame:
    with get_db_cursor() as cursor:
        cursor.execute(PANEL_QUERY, {"tickers": tickers, "adjusted": adjusted})
        rows = cursor.fetchall()
    panel = pd.DataFrame([dict(row) for row in rows])
    if panel.empty:
        return panel
    panel["date"] = pd.to_datetime(panel["date"])
    for column in ("open", "high", "low", "close", "volume"):
        panel[column] = panel[column].astype(float)
    return panel.sort_values(["ticker", "date"]).reset_index(drop=True)


def candidate_tickers(limit: int) -> list[str]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT ticker, COUNT(*) AS sessions
            FROM equity_bar_revisions
            WHERE interval = '1d' AND adjusted = TRUE AND is_final = TRUE
              AND session_scope = 'RTH'
            GROUP BY ticker
            HAVING COUNT(*) >= 400
            ORDER BY ticker
            """
        )
        return [row["ticker"] for row in cursor.fetchall()][:limit]


def _key(row: pd.Series) -> tuple:
    return (row["scanner_name"], row["ticker"], int(row["direction"]))


def _normalize(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    return str(value)


def compare(full: pd.DataFrame, truncated: pd.DataFrame) -> list[str]:
    """Return human-readable differences between two event sets for one bar."""
    differences: list[str] = []
    full_index = {_key(row): row for _, row in full.iterrows()}
    truncated_index = {_key(row): row for _, row in truncated.iterrows()}

    for key in full_index.keys() - truncated_index.keys():
        differences.append(f"only_with_future_data:{key[0]}:{key[2]:+d}")
    for key in truncated_index.keys() - full_index.keys():
        differences.append(f"disappears_with_future_data:{key[0]}:{key[2]:+d}")

    for key in full_index.keys() & truncated_index.keys():
        left, right = full_index[key], truncated_index[key]
        for column in COMPARE_COLUMNS:
            if _normalize(left[column]) != _normalize(right[column]):
                differences.append(
                    f"field_changed:{key[0]}:{column}:"
                    f"{_normalize(left[column])}!={_normalize(right[column])}"
                )
        if json.dumps(left["metadata"], sort_keys=True, default=str) != json.dumps(
            right["metadata"], sort_keys=True, default=str
        ):
            differences.append(f"metadata_changed:{key[0]}")
    return differences


def check_invariants(events: pd.DataFrame, panel: pd.DataFrame) -> Counter:
    """Verify each event against the raw bar it claims to be stamped on."""
    failures: Counter = Counter()
    bars = panel.set_index(["ticker", "date"])
    for _, event in events.iterrows():
        scanner = event["scanner_name"]
        try:
            bar = bars.loc[(event["ticker"], pd.Timestamp(event["date"]))]
        except KeyError:
            failures[f"{scanner}:no_matching_bar"] += 1
            continue

        entry = _normalize(event["entry_price"])
        if entry is not None and entry != _normalize(bar["close"]):
            failures[f"{scanner}:entry_is_not_signal_bar_close"] += 1

        stop = event["stop_price"]
        if stop is not None and not pd.isna(stop) and entry is not None:
            direction = int(event["direction"])
            if direction == 1 and float(stop) >= float(entry):
                failures[f"{scanner}:long_stop_not_below_entry"] += 1
            if direction == -1 and float(stop) <= float(entry):
                failures[f"{scanner}:short_stop_not_above_entry"] += 1

        anchor = str(event["setup_anchor"])
        anchor_date = anchor.split(":")[-1][:10]
        try:
            if pd.Timestamp(anchor_date) > pd.Timestamp(event["date"]):
                failures[f"{scanner}:anchor_after_signal"] += 1
        except ValueError:
            pass
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=int, default=8)
    parser.add_argument("--samples", type=int, default=25,
                        help="signal bars per ticker to recompute on truncated history")
    parser.add_argument("--adjusted", action="store_true", default=True)
    parser.add_argument("--unadjusted", dest="adjusted", action="store_false")
    parser.add_argument("--seed", type=int, default=20260903)
    parser.add_argument("--ticker-list")
    args = parser.parse_args()

    random.seed(args.seed)
    tickers = (
        [value.strip().upper() for value in args.ticker_list.split(",")]
        if args.ticker_list else candidate_tickers(args.tickers)
    )
    print(f"tickers: {len(tickers)} -> {', '.join(tickers)}")

    total_events = 0
    total_checked = 0
    differences: Counter = Counter()
    invariant_failures: Counter = Counter()
    examples: list[str] = []

    for ticker in tickers:
        panel = load_panel([ticker], args.adjusted)
        if panel.empty:
            print(f"  {ticker}: no bars")
            continue
        events = build_all_scanner_events(panel, "1d")
        total_events += len(events)
        invariant_failures.update(check_invariants(events, panel))
        if events.empty:
            print(f"  {ticker}: {len(panel)} bars, 0 events")
            continue

        signal_dates = sorted({pd.Timestamp(value) for value in events["date"]})
        sample = (random.sample(signal_dates, args.samples)
                  if len(signal_dates) > args.samples else signal_dates)

        for signal_date in sample:
            prefix = panel[panel["date"] <= signal_date]
            recomputed = build_all_scanner_events(prefix, "1d")
            expected = events[pd.to_datetime(events["date"]) == signal_date]
            actual = (
                recomputed[pd.to_datetime(recomputed["date"]) == signal_date]
                if not recomputed.empty else recomputed
            )
            total_checked += 1
            for difference in compare(expected, actual):
                differences[difference.rsplit(":", 1)[0] if difference.startswith(
                    "field_changed") else difference] += 1
                if len(examples) < 15:
                    examples.append(
                        f"{ticker} {signal_date.date()} {difference}"
                    )

        print(f"  {ticker}: {len(panel)} bars, {len(events)} events, "
              f"{len(sample)} bars recomputed")

    print("\n--- prefix invariance ---")
    print(f"signal bars recomputed on truncated history: {total_checked}")
    if differences:
        print("MISMATCHES (possible lookahead):")
        for name, count in differences.most_common():
            print(f"  {name}: {count}")
        print("\nexamples:")
        for line in examples:
            print(f"  {line}")
    else:
        print("no differences: every sampled event reproduced without future bars")

    print("\n--- payload invariants ---")
    print(f"events checked: {total_events}")
    if invariant_failures:
        for name, count in invariant_failures.most_common():
            print(f"  {name}: {count}")
    else:
        print("no failures")

    return 1 if differences or invariant_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
