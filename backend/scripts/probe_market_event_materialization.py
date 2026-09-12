#!/usr/bin/env python3
"""Validate live market-event persistence in a transaction that is always rolled back."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import time


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env", override=True)

from database import get_db_connection  # noqa: E402
from market_events import (  # noqa: E402
    FederalReserveCalendarClient,
    FinnhubEarningsClient,
    PUBLIC_CALENDAR_SOURCE,
    PublicMarketEventMaterializer,
)
from options.repositories.market_events import OptionMarketEventRepository  # noqa: E402
from scripts.run_market_event_worker import (  # noqa: E402
    HORIZON_DAYS,
    LOOKBACK_DAYS,
    active_company_tickers,
)


class _NoCommitConnection:
    def __init__(self, connection) -> None:
        self.connection = connection

    @property
    def closed(self):
        return self.connection.closed

    def cursor(self, *args, **kwargs):
        return self.connection.cursor(*args, **kwargs)

    def commit(self):
        return None

    def rollback(self):
        self.connection.rollback()


def main() -> int:
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FINNHUB_API_KEY is not configured")
    observed_at = datetime.now(timezone.utc)
    start = (observed_at - timedelta(days=LOOKBACK_DAYS)).date()
    end = (observed_at + timedelta(days=HORIZON_DAYS)).date()
    companies = active_company_tickers(observed_at)
    persisted = None
    with get_db_connection() as connection:
        connection.rollback()
        wrapper = _NoCommitConnection(connection)

        @contextmanager
        def factory():
            yield wrapper

        repository = OptionMarketEventRepository(factory)
        materializer = PublicMarketEventMaterializer(
            earnings_client=FinnhubEarningsClient(api_key),
            fomc_client=FederalReserveCalendarClient(),
            repository=repository,
        )
        started = time.monotonic()
        try:
            persisted = materializer.materialize(
                companies,
                start=start,
                end=end,
                observed_at=observed_at,
            )
            elapsed = time.monotonic() - started
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_type, status, confidence, COUNT(*)
                    FROM option_market_events
                    WHERE source = %s AND first_observed_at = %s
                    GROUP BY event_type, status, confidence
                    ORDER BY event_type, status, confidence
                    """,
                    (PUBLIC_CALENDAR_SOURCE, observed_at),
                )
                event_groups = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT event_type, COUNT(*)
                    FROM option_event_calendar_coverage
                    WHERE source = %s AND first_observed_at = %s
                    GROUP BY event_type ORDER BY event_type
                    """,
                    (PUBLIC_CALENDAR_SOURCE, observed_at),
                )
                coverage_groups = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT
                        COUNT(DISTINCT affected_underlying)
                            FILTER (WHERE event_type = 'EARNINGS'),
                        COUNT(*) FILTER (WHERE source_observed_at IS NULL)
                    FROM option_event_calendar_coverage
                    WHERE source = %s AND first_observed_at = %s
                    """,
                    (PUBLIC_CALENDAR_SOURCE, observed_at),
                )
                earnings_coverage, null_coverage_observed = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE source_observed_at IS NULL),
                        COUNT(*) FILTER (
                            WHERE scheduled_time < %s OR scheduled_time > %s
                        )
                    FROM option_market_events
                    WHERE source = %s AND first_observed_at = %s
                    """,
                    (
                        datetime.combine(start, datetime.min.time(), timezone.utc)
                        - timedelta(hours=8),
                        datetime.combine(end, datetime.max.time(), timezone.utc)
                        + timedelta(hours=8),
                        PUBLIC_CALENDAR_SOURCE,
                        observed_at,
                    ),
                )
                null_event_observed, outside_horizon = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT source_key
                        FROM option_market_events
                        WHERE source = %s AND first_observed_at = %s
                        GROUP BY source_key HAVING COUNT(*) > 1
                    ) AS duplicates
                    """,
                    (PUBLIC_CALENDAR_SOURCE, observed_at),
                )
                duplicate_event_keys = cursor.fetchone()[0]
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT source_key
                        FROM option_event_calendar_coverage
                        WHERE source = %s AND first_observed_at = %s
                        GROUP BY source_key HAVING COUNT(*) > 1
                    ) AS duplicates
                    """,
                    (PUBLIC_CALENDAR_SOURCE, observed_at),
                )
                duplicate_coverage_keys = cursor.fetchone()[0]
            event_count = sum(row[3] for row in event_groups)
            coverage_count = sum(row[1] for row in coverage_groups)
            checks = {
                "no_reasons": not persisted.reasons,
                "earnings_coverage_complete": earnings_coverage == len(companies),
                "event_counts_match": event_count == (
                    persisted.earnings_event_count + persisted.fomc_event_count
                ),
                "coverage_counts_match": coverage_count == persisted.coverage_count,
                "insert_counts_match": (
                    persisted.persisted is not None
                    and persisted.persisted.events_inserted == event_count
                    and persisted.persisted.coverage_inserted == coverage_count
                ),
                "source_observation_complete": (
                    null_event_observed == 0 and null_coverage_observed == 0
                ),
                "unique_source_keys": (
                    duplicate_event_keys == 0 and duplicate_coverage_keys == 0
                ),
                "events_inside_horizon": outside_horizon == 0,
            }
            report = {
                "status": "PASS" if all(checks.values()) else "FAIL",
                "observed_at": observed_at,
                "elapsed_seconds": round(elapsed, 3),
                "universe_size": len(companies),
                "result": {
                    "earnings_events": persisted.earnings_event_count,
                    "fomc_events": persisted.fomc_event_count,
                    "coverage": persisted.coverage_count,
                    "reasons": persisted.reasons,
                },
                "event_groups": event_groups,
                "coverage_groups": coverage_groups,
                "earnings_coverage_underlyings": earnings_coverage,
                "checks": checks,
                "rollback_verified": False,
            }
        finally:
            connection.rollback()
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM option_market_events
                     WHERE source = %s AND first_observed_at = %s),
                    (SELECT COUNT(*) FROM option_event_calendar_coverage
                     WHERE source = %s AND first_observed_at = %s)
                """,
                (
                    PUBLIC_CALENDAR_SOURCE, observed_at,
                    PUBLIC_CALENDAR_SOURCE, observed_at,
                ),
            )
            remaining = cursor.fetchone()
    report["rollback_verified"] = remaining == (0, 0)
    if not report["rollback_verified"]:
        report["status"] = "FAIL"
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())