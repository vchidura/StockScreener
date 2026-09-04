#!/usr/bin/env python3
"""Compute and atomically publish expensive canonical equity portal snapshots."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_connection, get_selected_tickers, get_tickers_overview
from equity.portal_snapshots import SNAPSHOT_TYPES, current, publish, source_manifest
from equity.portal_scanners import compute_default_scanner_snapshots
from equity.sector_research import latest_sector_performance, sector_intelligence
from screeners import analyze_market_regime, bulk_load_dataframes


LOCK_NAME = "stock-screener:equity-portal-snapshot-refresh"


class SourceGenerationChanged(RuntimeError):
    pass


def _compute_streak_snapshots() -> dict:
    import main as portal

    previous = portal.MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
    portal.MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED = False

    async def compute() -> dict:
        snapshots = {}
        policies = {
            "gaps": "STREAK_GAPS_5",
            "ma-crossover": "STREAK_MA_5",
            "momentum-pullback": "STREAK_MOMENTUM_5",
            "bearish-bounce": "STREAK_BEARISH_5",
            "fibonacci": "STREAK_FIBONACCI_5",
        }
        for strategy, snapshot_type in policies.items():
            snapshots[snapshot_type] = await portal.scan_streak_endpoint(
                strategy=strategy,
                days=5,
                tickers=None,
                short_period=9,
                long_period=21,
                refresh=False,
            )
        snapshots["STREAK_SUMMARY_3_5"] = await portal.scan_streak_summary_endpoint(
            days=3,
            fib_swing_pct=5.0,
            refresh=False,
        )
        return snapshots

    try:
        return asyncio.run(compute())
    finally:
        portal.MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED = previous


def refresh_once() -> dict:
    started = time.perf_counter()
    with get_db_connection() as lock_connection:
        lock_cursor = lock_connection.cursor()
        lock_cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (LOCK_NAME,))
        try:
            manifest = source_manifest()
            tickers = get_selected_tickers(True)
            overview = get_tickers_overview(tickers)
            frames = bulk_load_dataframes(["SPY", "QQQ"], 1600)
            scanner_frames = bulk_load_dataframes(tickers, 1100)
            payloads = {
                "TICKER_OVERVIEW": overview,
                "MARKET_REGIME": analyze_market_regime(
                    frames.get("SPY"), frames.get("QQQ")
                ),
                "SECTOR_PERFORMANCE_1": latest_sector_performance(1),
                "SECTOR_PERFORMANCE_5": latest_sector_performance(5),
                "SECTOR_PERFORMANCE_10": latest_sector_performance(10),
                "SECTOR_PERFORMANCE_21": latest_sector_performance(21),
                "SECTOR_INTELLIGENCE": sector_intelligence(5),
            }
            payloads.update(compute_default_scanner_snapshots(
                tickers, scanner_frames, frames
            ))
            payloads.update(_compute_streak_snapshots())
            if source_manifest() != manifest:
                raise SourceGenerationChanged(
                    "canonical bar source changed during portal refresh"
                )
            published = publish(payloads, manifest)
            projections = {key: current(key) for key in payloads}
            if any(not row or not row["is_fresh"] for row in projections.values()):
                raise RuntimeError("published equity portal snapshot is not fresh")
        finally:
            lock_cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (LOCK_NAME,))
            lock_cursor.close()
            lock_connection.commit()
    return {
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "published": published,
        "rows": {
            key: len(value) if isinstance(value, list) else 1
            for key, value in payloads.items()
        },
        "read_latency_ms": {
            key: value["read_latency_ms"] for key, value in projections.items()
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=int(os.getenv("EQUITY_PORTAL_SNAPSHOT_POLL_SECONDS", "60")),
    )
    args = parser.parse_args()
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")

    while True:
        projections = [current(snapshot_type) for snapshot_type in SNAPSHOT_TYPES]
        if any(row is None or not row["is_fresh"] for row in projections):
            try:
                result = refresh_once()
            except SourceGenerationChanged as exc:
                if not args.continuous:
                    raise
                print(json.dumps({"status": "RETRY", "reason": str(exc)}), flush=True)
            else:
                print(json.dumps(result, indent=2), flush=True)
        if not args.continuous:
            break
        time.sleep(args.poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())