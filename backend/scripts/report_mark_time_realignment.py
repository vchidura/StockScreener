#!/usr/bin/env python3
"""Re-normalize persisted raw provider pages to measure a mark-time change offline.

Read-only: no provider calls, no writes. Replays the immutable raw pages through the
current `parse_polygon_snapshot` and compares the resulting option mark times against
what is persisted, so a normalization change can be validated against real historical
data without waiting for a live session.

Projected skew assumes a one-minute underlying bar exists for the mark's minute, which
is the aligned-bar rule normalization applies. Bar availability is reported separately
rather than assumed.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, register_uuid

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")
register_uuid()

from options.data.normalizer import parse_polygon_snapshot  # noqa: E402

UTC = timezone.utc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batches", type=int, default=40, help="Most recent batches.")
    parser.add_argument("--underlyer", default=None)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def _connect():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"], options="-c timezone=UTC",
    )


def _describe(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "median": round(statistics.median(ordered), 1),
        "p10": round(ordered[len(ordered) // 10], 1),
        "p90": round(ordered[(9 * len(ordered)) // 10], 1),
        "min": round(ordered[0], 1),
        "max": round(ordered[-1], 1),
    }


def main() -> int:
    args = _parse_args()
    connection = _connect()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    cursor.execute(
        """
        SELECT batch_id, underlying
        FROM option_ingestion_runs
        WHERE status = 'COMPLETE'
          AND (%s::text IS NULL OR underlying = %s::text)
        ORDER BY scheduled_cycle DESC
        LIMIT %s
        """,
        (args.underlyer, args.underlyer, args.batches),
    )
    batches = [dict(row) for row in cursor.fetchall()]
    if not batches:
        print("no complete batches found", file=sys.stderr)
        return 1

    shifts: list[float] = []
    projected_skew: list[float] = []
    old_skew: list[float] = []
    by_volume: dict[str, dict[str, int]] = defaultdict(
        lambda: {"rows": 0, "projected_pass": 0, "old_pass": 0}
    )
    release_stamp_rows = 0
    compared = 0
    missing_persisted = 0
    newest_bar_covered = 0
    newest_bar_checked = 0

    for batch in batches:
        cursor.execute(
            "SELECT response_gzip FROM option_raw_batch_pages "
            "WHERE batch_id = %s ORDER BY page_number",
            (batch["batch_id"],),
        )
        pages = cursor.fetchall()

        cursor.execute(
            """
            SELECT contract_ticker, mark_market_data_time, spot_market_data_time,
                   day_volume, model_mark
            FROM option_chain_snapshots
            WHERE batch_id = %s
            """,
            (batch["batch_id"],),
        )
        persisted = {row["contract_ticker"]: dict(row) for row in cursor.fetchall()}
        if not persisted:
            continue
        latest_bar = max(
            (row["spot_market_data_time"] for row in persisted.values()
             if row["spot_market_data_time"]),
            default=None,
        )

        observed_at = datetime.now(UTC)
        for page in pages:
            payload = json.loads(gzip.decompress(page["response_gzip"]))
            for item in payload.get("results") or []:
                try:
                    observation = parse_polygon_snapshot(item, observed_at)
                except ValueError:
                    continue
                if observation.mark_time_from_release_stamp:
                    release_stamp_rows += 1
                new_time = observation.option_mark_time
                row = persisted.get(observation.contract_ticker)
                if row is None or new_time is None:
                    missing_persisted += 1
                    continue
                old_time = row["mark_market_data_time"]
                spot_time = row["spot_market_data_time"]
                compared += 1
                shifts.append((old_time - new_time).total_seconds())

                # Aligned bar under the corrected clock is the mark's own minute.
                skew = new_time.second + new_time.microsecond / 1e6
                projected_skew.append(skew)
                if spot_time is not None:
                    old_skew.append((old_time - spot_time).total_seconds())

                volume = row["day_volume"]
                bucket = (
                    "null" if volume is None
                    else "1-99" if volume < 100
                    else "100-999" if volume < 1000
                    else "1000+"
                )
                by_volume[bucket]["rows"] += 1
                if skew <= 60:
                    by_volume[bucket]["projected_pass"] += 1
                if row["model_mark"] is not None:
                    by_volume[bucket]["old_pass"] += 1

                if latest_bar is not None:
                    newest_bar_checked += 1
                    if new_time <= latest_bar:
                        newest_bar_covered += 1

    connection.close()

    def rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "batches": len(batches),
        "rows_compared": compared,
        "rows_without_persisted_match": missing_persisted,
        "rows_using_release_stamp_fallback": release_stamp_rows,
        "mark_time_shift_seconds_old_minus_new": _describe(shifts),
        "persisted_skew_seconds": _describe(old_skew),
        "projected_skew_seconds": _describe(projected_skew),
        "projected_within_60s": rate(
            sum(1 for value in projected_skew if value <= 60), len(projected_skew)
        ),
        "mark_within_fetched_bar_range": rate(newest_bar_covered, newest_bar_checked),
        "pass_rate_by_volume": {
            bucket: {
                "rows": counts["rows"],
                "persisted_model_valid": rate(counts["old_pass"], counts["rows"]),
                "projected_within_60s": rate(counts["projected_pass"], counts["rows"]),
            }
            for bucket, counts in sorted(by_volume.items())
        },
    }

    text = json.dumps(report, indent=2, default=str, allow_nan=False)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
