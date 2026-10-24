#!/usr/bin/env python3
"""Refresh live split and dividend facts for the active equity universe."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from equity.domain import (  # noqa: E402
    BarAvailabilityMode,
    DecisionWatermark,
)
from equity.corporate_action_coverage import (  # noqa: E402
    build_corporate_action_coverage as build_coverage,
)
from equity.leadership import try_advisory_leadership  # noqa: E402
from equity.polygon import PolygonEquityClient, normalize_corporate_actions  # noqa: E402
from equity.repositories import (  # noqa: E402
    EquityCorporateActionRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)


LOGGER = logging.getLogger("corporate-action-worker")
POLL_SECONDS = int(os.getenv("EQUITY_CORPORATE_ACTION_POLL_SECONDS", "21600"))
LOOKBACK_DAYS = int(os.getenv("EQUITY_CORPORATE_ACTION_LOOKBACK_DAYS", "30"))
SPLIT_LOOKBACK_DAYS = int(os.getenv("EQUITY_CORPORATE_ACTION_SPLIT_LOOKBACK_DAYS", "180"))
HORIZON_DAYS = int(os.getenv("EQUITY_CORPORATE_ACTION_HORIZON_DAYS", "365"))
RETRY_SECONDS = int(os.getenv("EQUITY_CORPORATE_ACTION_RETRY_SECONDS", "300"))
WORKER_LOCK_NAME = os.getenv(
    "EQUITY_CORPORATE_ACTION_WORKER_LOCK_NAME",
    "stock-screener:equity-corporate-action-worker",
)


@dataclass(frozen=True, slots=True)
class CorporateActionRefreshResult:
    universe_size: int
    split_rows: int
    dividend_rows: int
    normalized_actions: int
    inserted_actions: int
    inserted_coverage: int


def get_selected_tickers(*args, **kwargs):
    from database import get_selected_tickers as load_selected_tickers

    return load_selected_tickers(*args, **kwargs)


def active_security_ids(observed_at: datetime) -> dict[str, object]:
    context = DecisionWatermark(observed_at, observed_at)
    tickers = set(get_selected_tickers(active_only=True))
    universe_repository = EquityUniverseRepository()
    universe = universe_repository.get_latest_as_of(context)
    if universe is not None:
        tickers.update(
            universe_repository.member_tickers(universe["universe_run_id"])
        )
    references = EquityReferenceRepository().list_securities_as_of(
        sorted(tickers), context
    )
    return {row.ticker: row.security_id for row in references if row.active}


def refresh_corporate_actions(
    *,
    observed_at: datetime,
    client: PolygonEquityClient | None = None,
    repository: EquityCorporateActionRepository | None = None,
) -> CorporateActionRefreshResult:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    split_start = observed_at.date() - timedelta(days=SPLIT_LOOKBACK_DAYS)
    dividend_start = observed_at.date() - timedelta(days=LOOKBACK_DAYS)
    end = observed_at.date() + timedelta(days=HORIZON_DAYS)
    security_ids = active_security_ids(observed_at)
    if not security_ids:
        raise RuntimeError("active equity universe contains no security references")
    client = client or PolygonEquityClient()
    split_rows = client.fetch_splits(split_start, end)
    dividend_rows = client.fetch_dividends(dividend_start, end)
    actions = (
        *normalize_corporate_actions(
            split_rows,
            security_ids=security_ids,
            action_type="SPLIT",
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        ),
        *normalize_corporate_actions(
            dividend_rows,
            security_ids=security_ids,
            action_type="DIVIDEND",
            observed_at=observed_at,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        ),
    )
    repository = repository or EquityCorporateActionRepository()
    coverage = (
        *build_coverage(security_ids, start=split_start, end=end,
            observed_at=observed_at, responses={"SPLIT": split_rows},
            action_types=("SPLIT",)),
        *build_coverage(security_ids, start=dividend_start, end=end,
            observed_at=observed_at, responses={"DIVIDEND": dividend_rows},
            action_types=("DIVIDEND",)),
    )
    inserted, inserted_coverage = repository.persist_observation(coverage, actions)
    return CorporateActionRefreshResult(
        universe_size=len(security_ids),
        split_rows=len(split_rows),
        dividend_rows=len(dividend_rows),
        normalized_actions=len(actions),
        inserted_actions=inserted,
        inserted_coverage=inserted_coverage,
    )


def run_worker(*, once: bool = False) -> None:
    if POLL_SECONDS <= 0 or RETRY_SECONDS <= 0:
        raise ValueError("corporate-action poll and retry durations must be positive")
    if LOOKBACK_DAYS < 0 or SPLIT_LOOKBACK_DAYS < 0 or HORIZON_DAYS <= 0:
        raise ValueError("corporate-action date windows are invalid")
    while True:
        try:
            result = refresh_corporate_actions(
                observed_at=datetime.now(timezone.utc)
            )
            LOGGER.info(
                "corporate actions universe=%s splits=%s dividends=%s "
                "normalized=%s inserted=%s coverage_inserted=%s",
                result.universe_size,
                result.split_rows,
                result.dividend_rows,
                result.normalized_actions,
                result.inserted_actions,
                result.inserted_coverage,
            )
            wait_seconds = POLL_SECONDS
        except Exception:
            LOGGER.exception("corporate-action refresh failed")
            if once:
                raise
            wait_seconds = RETRY_SECONDS
        if once:
            return
        time.sleep(wait_seconds)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--once", action="store_true", help="Refresh one observation and exit."
    )
    return result


def main() -> int:
    args = parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )
    with try_advisory_leadership(WORKER_LOCK_NAME) as is_leader:
        if not is_leader:
            LOGGER.error("another corporate-action worker holds leadership")
            return 2
        run_worker(once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())