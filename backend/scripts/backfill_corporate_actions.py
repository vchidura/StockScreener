#!/usr/bin/env python3
"""Backfill Polygon splits and dividends for the current universe.

Split lineage is required before any daily study: canonical bars are stored
unadjusted, so an unrecorded split reads as a catastrophic gap in every derived
interval.
"""
from __future__ import annotations

import argparse
import json
import os
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
from equity.polygon import PolygonEquityClient, normalize_corporate_actions, sha256_json
from equity.repositories import (
    EquityCorporateActionRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from scripts.run_corporate_action_worker import build_coverage


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--start", required=True, help="YYYY-MM-DD")
    result.add_argument("--end", required=True, help="YYYY-MM-DD")
    result.add_argument("--dividends", action="store_true",
                        help="Also backfill dividends; splits alone fix adjustment")
    result.add_argument("--ticker", action="append", default=[],
                        help="Restrict to tickers already present in the live universe; repeatable")
    result.add_argument("--apply", action="store_true")
    result.add_argument("--confirm-database-name",
                        help="Required with --apply and must exactly match DB_NAME")
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
    if end < start:
        raise SystemExit("end must not precede start")
    requested_tickers = tuple(dict.fromkeys(
        ticker.strip().upper() for ticker in arguments.ticker if ticker.strip()
    ))
    if any(not ticker.replace(".", "").replace("-", "").isalnum() for ticker in requested_tickers):
        raise SystemExit("explicit tickers must be valid symbols")
    database_name = os.getenv("DB_NAME", "").strip()
    if arguments.apply and (
        not arguments.confirm_database_name
        or arguments.confirm_database_name != database_name
    ):
        raise SystemExit("--apply requires --confirm-database-name matching DB_NAME")
    observed_at = datetime.now(timezone.utc)

    universe = EquityUniverseRepository().get_latest_as_of(
        DecisionWatermark(market_time=observed_at, observed_time=observed_at)
    )
    if universe is None:
        raise SystemExit("no live universe run is available")
    tickers = EquityUniverseRepository().member_tickers(universe["universe_run_id"])
    if requested_tickers:
        if not set(requested_tickers) <= set(tickers):
            raise SystemExit("explicit tickers must already belong to the live universe")
        tickers = requested_tickers
    securities = EquityReferenceRepository().list_securities_as_of(
        tuple(sorted(tickers)), DecisionWatermark(observed_at, observed_at)
    )
    security_ids = {row.ticker: row.security_id for row in securities}
    if set(security_ids) != set(tickers):
        raise SystemExit("exact current security references are required for every selected ticker")

    client = PolygonEquityClient()
    print(f"fetching splits {start}..{end}", flush=True)
    split_rows = list(with_backoff(lambda: client.fetch_splits(start, end)))
    dividend_rows: list = []
    if arguments.dividends:
        print(f"fetching dividends {start}..{end}", flush=True)
        dividend_rows = list(with_backoff(lambda: client.fetch_dividends(start, end)))

    split_actions = normalize_corporate_actions(
            split_rows, security_ids=security_ids, action_type="SPLIT",
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        )
    dividend_actions = normalize_corporate_actions(
            dividend_rows, security_ids=security_ids, action_type="DIVIDEND",
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        )
    actions = (*split_actions, *dividend_actions)

    report = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "universe_members": len(tickers),
        "selected_tickers": sorted(tickers),
        "security_reference_count": len(security_ids),
        "splits_fetched": len(split_rows),
        "split_response_sha256": sha256_json(split_rows),
        "dividends_fetched": len(dividend_rows),
        "actions_matched_to_universe": len(actions),
        "splits_matched_to_universe": len(split_actions),
        "distinct_split_tickers": len({a.ticker for a in split_actions}),
        "mode": "APPLY" if arguments.apply else "DRY_RUN",
    }
    if arguments.apply:
        repository = EquityCorporateActionRepository()
        all_coverage = build_coverage(
            security_ids,
            start=start,
            end=end,
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
            responses={"SPLIT": split_rows, "DIVIDEND": dividend_rows},
        )
        coverage = tuple(
            row for row in all_coverage
            if row.action_type == "SPLIT" or arguments.dividends
        )
        inserted_actions, inserted_coverage = repository.persist_observation(
            coverage, actions,
        )
        report["inserted"] = inserted_actions
        report["coverage_inserted"] = inserted_coverage
        report["coverage_rows"] = len(coverage)
        report["coverage_response_bound"] = all(
            row.security_id is not None and row.response_action_count is not None
            and row.response_sha256 is not None for row in coverage
        )
        report["action_ids"] = [str(row.corporate_action_id) for row in actions]
        report["coverage_ids"] = [str(row.coverage_id) for row in coverage]
        report["persisted_identity_sha256"] = sha256_json({
            "action_ids": report["action_ids"],
            "coverage_ids": report["coverage_ids"],
        })
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
