#!/usr/bin/env python3
"""Compare legacy and bulk canonical publication reads without writing data."""
from __future__ import annotations

import json
from pathlib import Path
import sys
from time import perf_counter

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from equity.domain import BarSessionScope, DecisionWatermark  # noqa: E402
from equity.repositories import EquityBarRepository  # noqa: E402
from scripts.run_equity_worker import get_selected_tickers  # noqa: E402


def main() -> int:
    tickers = tuple(get_selected_tickers(active_only=True))
    if not 1 <= len(tickers) <= 500 or len(set(tickers)) != len(tickers):
        raise ValueError("publication read diagnostic requires 1..500 distinct selected tickers")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '120s'")
        cursor.execute("""SELECT market_time,observed_at FROM equity_bar_publications
            WHERE interval='30m' AND session_scope='RTH' AND NOT adjusted
              AND status IN ('COMPLETE','DEGRADED')
            ORDER BY market_time DESC,observed_at DESC LIMIT 1""")
        source = cursor.fetchone()
    if source is None:
        raise ValueError("no retained 30m publication is available")
    watermark = DecisionWatermark(source["market_time"], source["observed_at"])
    repository = EquityBarRepository()

    started = perf_counter()
    legacy = {ticker: repository.list_final_as_of(ticker, "30m", watermark,
        limit=1, session_scope=BarSessionScope.RTH, adjusted=False) for ticker in tickers}
    legacy_seconds = perf_counter() - started

    started = perf_counter()
    bulk = repository.list_final_for_tickers_as_of(tickers, "30m", watermark,
        limit_per_ticker=1, session_scope=BarSessionScope.RTH, adjusted=False)
    bulk_seconds = perf_counter() - started

    started = perf_counter()
    exact = repository.list_final_at_watermark_for_tickers(tickers, "30m", watermark,
        session_scope=BarSessionScope.RTH, adjusted=False)
    exact_seconds = perf_counter() - started

    legacy_ids = {ticker: str(rows[-1].bar_revision_id) if rows else None for ticker, rows in legacy.items()}
    bulk_ids = {ticker: str(rows[-1].bar_revision_id) if rows else None for ticker, rows in bulk.items()}
    exact_ids = {ticker: str(exact[ticker].bar_revision_id) if ticker in exact else None for ticker in tickers}
    print(json.dumps(dict(version="equity_publication_read_latency_v1", mode="READ_ONLY",
        interval="30m", market_time=watermark.market_time, observed_time=watermark.observed_time,
        tickers=len(tickers), legacy_seconds=round(legacy_seconds, 4), bulk_seconds=round(bulk_seconds, 4),
        exact_seconds=round(exact_seconds, 4), exact_speedup=round(legacy_seconds / exact_seconds, 3) if exact_seconds else None,
        speedup=round(legacy_seconds / bulk_seconds, 3) if bulk_seconds else None,
        matching_revision_ids=legacy_ids == bulk_ids == exact_ids,
        legacy_available=sum(value is not None for value in legacy_ids.values()),
        bulk_available=sum(value is not None for value in bulk_ids.values()),
        exact_available=sum(value is not None for value in exact_ids.values()), source_writes=0),
        indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
