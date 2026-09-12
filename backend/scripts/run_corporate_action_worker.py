#!/usr/bin/env python3
"""Refresh live split and dividend facts for the active equity universe."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from database import get_selected_tickers  # noqa: E402
from equity.domain import (  # noqa: E402
    BarAvailabilityMode,
    DecisionWatermark,
    EquityCorporateActionCoverage,
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


def build_coverage(
    tickers,
    *,
    start,
    end,
    observed_at: datetime,
    availability_mode: BarAvailabilityMode = BarAvailabilityMode.LIVE_OBSERVED,
) -> tuple[EquityCorporateActionCoverage, ...]:
    rows = []
    for ticker in sorted(tickers):
        for action_type in ("SPLIT", "DIVIDEND"):
            payload = {
                "action_type": action_type,
                "ticker": ticker,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
            }
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            digest = hashlib.sha256(canonical.encode("ascii")).hexdigest()
            source_key = (
                f"polygon:{action_type.lower()}:{ticker}:{start}:{end}"
            )
            rows.append(EquityCorporateActionCoverage(
                coverage_id=uuid5(
                    NAMESPACE_URL,
                    f"equity-action-coverage:{source_key}:{observed_at.isoformat()}:{digest}",
                ),
                action_type=action_type,
                ticker=ticker,
                window_start=start,
                window_end=end,
                source="POLYGON_CORPORATE_ACTIONS_V1",
                source_key=source_key,
                first_observed_at=observed_at,
                availability_mode=availability_mode,
                replay_available_at=(
                    observed_at
                    if availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
                    else None
                ),
                payload_sha256=digest,
            ))
    return tuple(rows)


def refresh_corporate_actions(
    *,
    observed_at: datetime,
    client: PolygonEquityClient | None = None,
    repository: EquityCorporateActionRepository | None = None,
) -> CorporateActionRefreshResult:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    start = observed_at.date() - timedelta(days=LOOKBACK_DAYS)
    end = observed_at.date() + timedelta(days=HORIZON_DAYS)
    security_ids = active_security_ids(observed_at)
    if not security_ids:
        raise RuntimeError("active equity universe contains no security references")
    client = client or PolygonEquityClient()
    split_rows = client.fetch_splits(start, end)
    dividend_rows = client.fetch_dividends(start, end)
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
    inserted = repository.persist(actions)
    coverage = build_coverage(
        security_ids,
        start=start,
        end=end,
        observed_at=observed_at,
    )
    inserted_coverage = repository.persist_coverage(coverage, actions)
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
    if LOOKBACK_DAYS < 0 or HORIZON_DAYS <= 0:
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