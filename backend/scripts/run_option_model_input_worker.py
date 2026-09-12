#!/usr/bin/env python3
"""Refresh official Treasury curve inputs without changing active option policy."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from equity.leadership import try_advisory_leadership  # noqa: E402
from http_client import get_session  # noqa: E402
from options.model_inputs import TREASURY_CURVE_URL, parse_treasury_curve  # noqa: E402
from options.repositories.model_inputs import OptionModelInputRepository  # noqa: E402


LOGGER = logging.getLogger("option-model-input-worker")
POLL_SECONDS = int(os.getenv("OPTION_MODEL_INPUT_POLL_SECONDS", "21600"))
RETRY_SECONDS = int(os.getenv("OPTION_MODEL_INPUT_RETRY_SECONDS", "300"))
WORKER_LOCK_NAME = os.getenv(
    "OPTION_MODEL_INPUT_WORKER_LOCK_NAME",
    "stock-screener:option-model-input-worker",
)


def refresh_rates(observed_at: datetime) -> tuple[int, int]:
    response = get_session().get(
        TREASURY_CURVE_URL.format(year=observed_at.year), timeout=(5, 30)
    )
    response.raise_for_status()
    rows = parse_treasury_curve(response.text, received_at=observed_at)
    latest_date = max(row.rate_date for row in rows)
    latest = tuple(row for row in rows if row.rate_date == latest_date)
    return len(latest), OptionModelInputRepository().persist_rates(latest)


def run_worker(*, once: bool = False) -> None:
    while True:
        try:
            received, inserted = refresh_rates(datetime.now(timezone.utc))
            LOGGER.info("Treasury rates received=%s inserted=%s", received, inserted)
            wait_seconds = POLL_SECONDS
        except Exception:
            LOGGER.exception("Treasury rate refresh failed")
            if once:
                raise
            wait_seconds = RETRY_SECONDS
        if once:
            return
        time.sleep(wait_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    with try_advisory_leadership(WORKER_LOCK_NAME) as is_leader:
        if not is_leader:
            LOGGER.error("another option model-input worker holds leadership")
            return 2
        run_worker(once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())