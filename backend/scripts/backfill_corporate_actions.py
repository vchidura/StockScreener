#!/usr/bin/env python3
"""Backfill Polygon splits and dividends for the current universe.

Split lineage is required before any daily study: canonical bars are stored
unadjusted, so an unrecorded split reads as a catastrophic gap in every derived
interval.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.domain import BarAvailabilityMode, DecisionWatermark
from equity.polygon import PolygonEquityClient, normalize_corporate_actions
from equity.repositories import (
    EquityCorporateActionRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--start", required=True, help="YYYY-MM-DD")
    result.add_argument("--end", required=True, help="YYYY-MM-DD")
    result.add_argument("--dividends", action="store_true",
                        help="Also backfill dividends; splits alone fix adjustment")
    result.add_argument("--apply", action="store_true")
    result.add_argument("--output", type=Path)
    return result


def with_backoff(operation, *, attempts: int = 6):
    """Polygon's client raises on 429; retry with exponential backoff."""
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except requests.HTTPError as error:
            status = getattr(error.response, "status_code", None)
            if status not in (429, 502, 503, 504) or attempt == attempts:
                raise
            print(f"  polygon {status}; retrying in {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("unreachable")


def main() -> int:
    arguments = parser().parse_args()
    start = date.fromisoformat(arguments.start)
    end = date.fromisoformat(arguments.end)
    observed_at = datetime.now(timezone.utc)

    universe = EquityUniverseRepository().get_latest_as_of(
        DecisionWatermark(market_time=observed_at, observed_time=observed_at)
    )
    if universe is None:
        raise SystemExit("no live universe run is available")
    tickers = EquityUniverseRepository().member_tickers(universe["universe_run_id"])
    securities = EquityReferenceRepository().list_securities_as_of(
        tuple(sorted(tickers)), DecisionWatermark(observed_at, observed_at)
    )
    security_ids = {row.ticker: row.security_id for row in securities}

    client = PolygonEquityClient()
    print(f"fetching splits {start}..{end}", flush=True)
    split_rows = list(with_backoff(lambda: client.fetch_splits(start, end)))
    dividend_rows: list = []
    if arguments.dividends:
        print(f"fetching dividends {start}..{end}", flush=True)
        dividend_rows = list(with_backoff(lambda: client.fetch_dividends(start, end)))

    actions = (
        *normalize_corporate_actions(
            split_rows, security_ids=security_ids, action_type="SPLIT",
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        ),
        *normalize_corporate_actions(
            dividend_rows, security_ids=security_ids, action_type="DIVIDEND",
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        ),
    )
    splits_for_universe = [
        action for action in actions if action.action_type == "SPLIT"
    ]

    report = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "universe_members": len(tickers),
        "splits_fetched": len(split_rows),
        "dividends_fetched": len(dividend_rows),
        "actions_matched_to_universe": len(actions),
        "splits_matched_to_universe": len(splits_for_universe),
        "distinct_split_tickers": len({a.ticker for a in splits_for_universe}),
        "mode": "APPLY" if arguments.apply else "DRY_RUN",
    }
    if arguments.apply:
        report["inserted"] = EquityCorporateActionRepository().persist(actions)
    else:
        report["note"] = "nothing written; re-run with --apply"

    rendered = json.dumps(report, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
