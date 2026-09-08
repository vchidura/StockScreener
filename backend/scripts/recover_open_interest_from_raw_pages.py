#!/usr/bin/env python3
"""Recover settled open interest from retained raw chain pages.

Open interest cannot be fetched for a past session at any entitlement tier, so the only
copies that exist for sessions already ingested are the gzipped provider responses in
`option_raw_batch_pages`. Those are retained provider evidence and survived the derived
layer purge, which means the sessions captured before the daily fact table existed are
still recoverable — but only until raw-page retention expires.

This replays those pages through the same parser the live pipeline uses and writes into
`option_daily_contract_facts`. It reads provider evidence and writes only the daily fact
table; nothing else is touched, and re-running is safe because the upsert fills gaps
rather than overwriting.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.data.normalizer import parse_polygon_snapshot  # noqa: E402
from options.repositories.daily_facts import (  # noqa: E402
    DailyOpenInterestRecord,
    OptionDailyFactRepository,
)

SQL_PAGES = """
    SELECT p.batch_id, p.page_number, p.response_gzip, p.received_at,
           r.underlying, r.scheduled_cycle
    FROM option_raw_batch_pages p
    JOIN option_ingestion_runs r ON r.batch_id = p.batch_id
    WHERE p.validation_status = 'VALID'
      AND (%s::text IS NULL OR r.underlying = %s)
    ORDER BY r.scheduled_cycle, p.batch_id, p.page_number
"""

SQL_CONTRACT_IDS = """
    SELECT contract_ticker, contract_id FROM option_contract_catalog
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyer", default=None, help="Restrict to one underlying.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the recovered facts. Without this the script only reports.",
    )
    return parser.parse_args()


def _results(response_gzip: memoryview | bytes) -> list[dict[str, Any]]:
    payload = json.loads(gzip.decompress(bytes(response_gzip)).decode("utf-8"))
    results = payload.get("results")
    return results if isinstance(results, list) else []


def main() -> int:
    args = _parse_args()
    calendar = OptionExchangeCalendar()

    with get_db_cursor() as cursor:
        cursor.execute(SQL_CONTRACT_IDS)
        contract_ids = {row["contract_ticker"]: row["contract_id"] for row in cursor.fetchall()}
        cursor.execute(SQL_PAGES, (args.underlyer, args.underlyer))
        pages = cursor.fetchall()

    print(f"{len(pages)} retained pages, {len(contract_ids)} catalogued contracts")
    if not pages:
        return 0

    # Keyed by (contract_id, settlement_session): one cycle's snapshot and the next
    # report the same settled figure, so the last observation of a session wins.
    recovered: dict[tuple[int, date], DailyOpenInterestRecord] = {}
    per_session: dict[date, set[int]] = defaultdict(set)
    unknown_contracts = 0
    missing_open_interest = 0
    parse_failures = 0

    for page in pages:
        observed_session = calendar.session_for_slot(
            page["scheduled_cycle"].astimezone(timezone.utc)
        )
        settlement_session = calendar.previous_session(observed_session)
        observed_at: datetime = page["received_at"]
        for item in _results(page["response_gzip"]):
            try:
                raw = parse_polygon_snapshot(item, observed_at)
            except ValueError:
                parse_failures += 1
                continue
            if raw.open_interest is None:
                missing_open_interest += 1
                continue
            contract_id = contract_ids.get(raw.contract_ticker)
            if contract_id is None:
                unknown_contracts += 1
                continue
            recovered[(contract_id, settlement_session)] = DailyOpenInterestRecord(
                contract_id=contract_id,
                settlement_session=settlement_session,
                underlying=page["underlying"],
                open_interest=raw.open_interest,
                observed_at=observed_at,
                observed_session=observed_session,
                batch_id=page["batch_id"],
            )
            per_session[settlement_session].add(contract_id)

    print(f"recoverable facts: {len(recovered)}")
    for session in sorted(per_session):
        print(f"  {session}  {len(per_session[session])} contracts")
    print(
        f"skipped: {missing_open_interest} without open interest,"
        f" {unknown_contracts} not in catalog, {parse_failures} unparseable"
    )

    if not args.apply:
        print("\nDry run. Re-run with --apply to write.")
        return 0

    written = OptionDailyFactRepository().persist_open_interest(list(recovered.values()))
    print(f"\nWrote {written} rows into option_daily_contract_facts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
