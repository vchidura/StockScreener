#!/usr/bin/env python3
"""Materialize shared earnings and FOMC calendar facts for the equity universe."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from database import get_selected_tickers  # noqa: E402
from equity.domain import DecisionWatermark  # noqa: E402
from equity.leadership import try_advisory_leadership  # noqa: E402
from equity.repositories import (  # noqa: E402
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from market_events import (  # noqa: E402
    FederalReserveCalendarClient,
    FinnhubEarningsClient,
    PUBLIC_CALENDAR_SOURCE,
    PublicMarketEventMaterializer,
)
from security_types import is_earnings_applicable_security_type  # noqa: E402


LOGGER = logging.getLogger("market-event-worker")
POLL_SECONDS = int(os.getenv("MARKET_EVENT_POLL_SECONDS", "21600"))
LOOKBACK_DAYS = int(os.getenv("MARKET_EVENT_LOOKBACK_DAYS", "1"))
HORIZON_DAYS = int(os.getenv("MARKET_EVENT_HORIZON_DAYS", "45"))
WORKER_LOCK_NAME = os.getenv(
    "MARKET_EVENT_WORKER_LOCK_NAME", "stock-screener:market-event-worker"
)


def active_company_tickers(observed_at: datetime) -> tuple[str, ...]:
    context = DecisionWatermark(observed_at, observed_at)
    universe_repository = EquityUniverseRepository()
    universe = universe_repository.get_latest_as_of(context)
    tickers = set(get_selected_tickers(active_only=True))
    if universe is not None:
        tickers.update(
            universe_repository.member_tickers(universe["universe_run_id"])
        )
    references = EquityReferenceRepository().list_securities_as_of(
        sorted(tickers), context
    )
    security_types = {row.ticker: row.security_type for row in references}
    return tuple(sorted(
        ticker for ticker in tickers
        if is_earnings_applicable_security_type(security_types.get(ticker))
    ))


def build_materializer() -> PublicMarketEventMaterializer:
    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()
    return PublicMarketEventMaterializer(
        earnings_client=(
            FinnhubEarningsClient(finnhub_key) if finnhub_key else None
        ),
        fomc_client=FederalReserveCalendarClient(),
    )


def run_once() -> None:
    observed_at = datetime.now(timezone.utc)
    company_tickers = active_company_tickers(observed_at)
    if not company_tickers:
        raise RuntimeError("active equity universe contains no company securities")
    result = build_materializer().materialize(
        company_tickers,
        start=(observed_at - timedelta(days=LOOKBACK_DAYS)).date(),
        end=(observed_at + timedelta(days=HORIZON_DAYS)).date(),
        observed_at=observed_at,
    )
    LOGGER.info(
        "calendar source=%s companies=%s coverage=%s earnings=%s fomc=%s "
        "coverage_inserted=%s events_inserted=%s reasons=%s",
        result.source,
        result.universe_size,
        result.coverage_count,
        result.earnings_event_count,
        result.fomc_event_count,
        result.persisted.coverage_inserted if result.persisted else 0,
        result.persisted.events_inserted if result.persisted else 0,
        ",".join(result.reasons) or "NONE",
    )


def run_worker(*, once: bool = False) -> None:
    if POLL_SECONDS <= 0:
        raise ValueError("MARKET_EVENT_POLL_SECONDS must be positive")
    if LOOKBACK_DAYS < 0:
        raise ValueError("MARKET_EVENT_LOOKBACK_DAYS must not be negative")
    if HORIZON_DAYS < 3:
        raise ValueError("MARKET_EVENT_HORIZON_DAYS must be at least 3")
    while True:
        try:
            run_once()
        except Exception:
            LOGGER.exception("market-event materialization failed")
            if once:
                raise
        if once:
            return
        time.sleep(POLL_SECONDS)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--once", action="store_true", help="Materialize one observation and exit."
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
            LOGGER.error("another market-event worker holds leadership")
            return 2
        LOGGER.info(
            "worker started source=%s poll_seconds=%s lookback_days=%s horizon_days=%s",
            PUBLIC_CALENDAR_SOURCE,
            POLL_SECONDS,
            LOOKBACK_DAYS,
            HORIZON_DAYS,
        )
        run_worker(once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())