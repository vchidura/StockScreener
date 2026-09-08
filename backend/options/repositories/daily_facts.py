from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .base import PostgresRepository

OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT = "PROVIDER_CHAIN_SNAPSHOT"
MARK_SOURCE_DAILY_AGGREGATE = "PROVIDER_DAILY_AGGREGATE"


@dataclass(frozen=True, slots=True)
class DailyOpenInterestRecord:
    contract_id: int
    settlement_session: date
    underlying: str
    open_interest: int
    observed_at: datetime
    observed_session: date
    batch_id: Any | None = None


@dataclass(frozen=True, slots=True)
class DailyMarkRecord:
    contract_id: int
    settlement_session: date
    underlying: str
    close: Decimal
    observed_at: datetime
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    volume: int | None = None
    transaction_count: int | None = None


# Re-observing the same settlement session must not rewrite a value that is already
# recorded, because open interest for a closed session cannot legitimately change. The
# conflict clause therefore fills gaps only.
SQL_UPSERT_OPEN_INTEREST = """
INSERT INTO option_daily_contract_facts (
    contract_id, settlement_session, underlying,
    open_interest, open_interest_source, open_interest_observed_at,
    open_interest_observed_session, open_interest_batch_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (contract_id, settlement_session) DO UPDATE
SET open_interest = COALESCE(
        option_daily_contract_facts.open_interest, EXCLUDED.open_interest
    ),
    open_interest_source = COALESCE(
        option_daily_contract_facts.open_interest_source, EXCLUDED.open_interest_source
    ),
    open_interest_observed_at = COALESCE(
        option_daily_contract_facts.open_interest_observed_at,
        EXCLUDED.open_interest_observed_at
    ),
    open_interest_observed_session = COALESCE(
        option_daily_contract_facts.open_interest_observed_session,
        EXCLUDED.open_interest_observed_session
    ),
    open_interest_batch_id = COALESCE(
        option_daily_contract_facts.open_interest_batch_id,
        EXCLUDED.open_interest_batch_id
    ),
    open_interest_revised_value = CASE
        WHEN option_daily_contract_facts.open_interest IS NULL THEN NULL
        ELSE EXCLUDED.open_interest
    END,
    open_interest_revised_observed_at = CASE
        WHEN option_daily_contract_facts.open_interest IS NULL THEN NULL
        ELSE EXCLUDED.open_interest_observed_at
    END,
    open_interest_revision_count = CASE
        WHEN option_daily_contract_facts.open_interest IS NULL THEN 0
        ELSE option_daily_contract_facts.open_interest_revision_count + 1
    END,
    updated_at = now()
WHERE option_daily_contract_facts.open_interest IS NULL
   OR EXCLUDED.open_interest IS DISTINCT FROM COALESCE(
        option_daily_contract_facts.open_interest_revised_value,
        option_daily_contract_facts.open_interest
   )
"""

SQL_UPSERT_MARK = """
INSERT INTO option_daily_contract_facts (
    contract_id, settlement_session, underlying,
    mark_open, mark_high, mark_low, mark_close,
    mark_volume, mark_transaction_count, mark_source, mark_observed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (contract_id, settlement_session) DO UPDATE
SET mark_open = COALESCE(option_daily_contract_facts.mark_open, EXCLUDED.mark_open),
    mark_high = COALESCE(option_daily_contract_facts.mark_high, EXCLUDED.mark_high),
    mark_low = COALESCE(option_daily_contract_facts.mark_low, EXCLUDED.mark_low),
    mark_close = COALESCE(option_daily_contract_facts.mark_close, EXCLUDED.mark_close),
    mark_volume = COALESCE(
        option_daily_contract_facts.mark_volume, EXCLUDED.mark_volume
    ),
    mark_transaction_count = COALESCE(
        option_daily_contract_facts.mark_transaction_count,
        EXCLUDED.mark_transaction_count
    ),
    mark_source = COALESCE(
        option_daily_contract_facts.mark_source, EXCLUDED.mark_source
    ),
    mark_observed_at = COALESCE(
        option_daily_contract_facts.mark_observed_at, EXCLUDED.mark_observed_at
    ),
    updated_at = now()
WHERE option_daily_contract_facts.mark_close IS NULL
"""

SQL_SESSION_COVERAGE = """
SELECT settlement_session,
       count(*) AS contracts,
       count(open_interest) AS with_open_interest,
       count(mark_close) AS with_mark,
    sum(COALESCE(open_interest_revised_value, open_interest)) AS total_open_interest,
    sum(open_interest_revision_count) AS open_interest_revisions
FROM option_daily_contract_facts
WHERE underlying = %s
  AND settlement_session BETWEEN %s AND %s
GROUP BY settlement_session
ORDER BY settlement_session
"""

# The change-in-open-interest read. Volume counts opens, closes and round trips
# identically, so a rise in open interest is the only unambiguous evidence that
# contracts were opened rather than churned.
SQL_OPEN_INTEREST_CHANGE = """
SELECT current.contract_id,
       current.underlying,
       current.settlement_session,
       COALESCE(current.open_interest_revised_value, current.open_interest)
           AS open_interest,
       COALESCE(prior.open_interest_revised_value, prior.open_interest)
           AS prior_open_interest,
       COALESCE(current.open_interest_revised_value, current.open_interest)
           - COALESCE(prior.open_interest_revised_value, prior.open_interest)
           AS open_interest_change,
       current.open_interest_revision_count,
       prior.open_interest_revision_count AS prior_open_interest_revision_count
FROM option_daily_contract_facts current
JOIN option_daily_contract_facts prior
  ON prior.contract_id = current.contract_id
 AND prior.settlement_session = %s
WHERE current.underlying = %s
  AND current.settlement_session = %s
  AND current.open_interest IS NOT NULL
  AND prior.open_interest IS NOT NULL
ORDER BY abs(
    COALESCE(current.open_interest_revised_value, current.open_interest)
    - COALESCE(prior.open_interest_revised_value, prior.open_interest)
) DESC
LIMIT %s
"""

# Detector read. Joins the settled figures to the contract terms, taking the catalog
# version that was current at the settlement session rather than the newest one, so a
# later correction cannot retroactively change what a past session is said to describe.
#
# Session volume comes from whichever source is better. `mark_volume` is the settled
# figure from the daily aggregate; the chain snapshots carry a running intraday total,
# so their maximum for the session is a lower bound that the final print may exceed.
# `session_volume_is_final` reports which one was used, because a provisional volume
# understates the denominator of the opening ratio and so overstates opening activity.
SQL_OPEN_INTEREST_CHANGE_DETAIL = """
WITH observed_volume AS (
    SELECT contract_id, max(day_volume) AS session_volume
    FROM option_chain_snapshots
    WHERE underlying = %s
      AND day_volume IS NOT NULL
      AND (market_data_time AT TIME ZONE 'America/New_York')::date = %s
    GROUP BY contract_id
)
SELECT current.contract_id,
       current.underlying,
       version.contract_type,
       version.strike,
       version.expiration_date,
       current.settlement_session,
       prior.settlement_session AS prior_settlement_session,
       COALESCE(current.open_interest_revised_value, current.open_interest)
           AS open_interest,
       COALESCE(prior.open_interest_revised_value, prior.open_interest)
           AS prior_open_interest,
       current.open_interest_revision_count,
       prior.open_interest_revision_count AS prior_open_interest_revision_count,
       COALESCE(current.mark_volume, observed.session_volume) AS session_volume,
       (current.mark_volume IS NOT NULL) AS session_volume_is_final
FROM option_daily_contract_facts current
JOIN option_daily_contract_facts prior
  ON prior.contract_id = current.contract_id
 AND prior.settlement_session = %s
JOIN LATERAL (
    SELECT contract_type, strike, expiration_date
    FROM option_contract_catalog_versions
    WHERE contract_id = current.contract_id
      AND contract_type IS NOT NULL
      AND valid_from <= (current.settlement_session + INTERVAL '1 day')
    ORDER BY valid_from DESC
    LIMIT 1
) version ON TRUE
LEFT JOIN observed_volume observed ON observed.contract_id = current.contract_id
WHERE current.underlying = %s
  AND current.settlement_session = %s
  AND current.open_interest IS NOT NULL
  AND prior.open_interest IS NOT NULL
ORDER BY abs(
    COALESCE(current.open_interest_revised_value, current.open_interest)
    - COALESCE(prior.open_interest_revised_value, prior.open_interest)
) DESC
"""

# The two most recent settlement sessions that actually carry open interest. Cycles are
# missed, so the newest pair is not reliably yesterday and the day before.
SQL_LATEST_SESSION_PAIR = """
SELECT DISTINCT settlement_session
FROM option_daily_contract_facts
WHERE underlying = %s
  AND open_interest IS NOT NULL
ORDER BY settlement_session DESC
LIMIT 2
"""

READ_QUERIES: Mapping[str, str] = {
    "session_coverage": SQL_SESSION_COVERAGE,
    "open_interest_change": SQL_OPEN_INTEREST_CHANGE,
    "open_interest_change_detail": SQL_OPEN_INTEREST_CHANGE_DETAIL,
    "latest_session_pair": SQL_LATEST_SESSION_PAIR,
}


class OptionDailyFactRepository(PostgresRepository):
    """Retained provider facts at one row per contract per settlement session."""

    def persist_open_interest(self, records: Sequence[DailyOpenInterestRecord]) -> int:
        if not records:
            return 0
        with self._cursor() as cursor:
            cursor.executemany(
                SQL_UPSERT_OPEN_INTEREST,
                [
                    (
                        record.contract_id,
                        record.settlement_session,
                        record.underlying,
                        record.open_interest,
                        OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT,
                        record.observed_at,
                        record.observed_session,
                        record.batch_id,
                    )
                    for record in records
                ],
            )
        return len(records)

    def persist_marks(self, records: Sequence[DailyMarkRecord]) -> int:
        if not records:
            return 0
        with self._cursor() as cursor:
            cursor.executemany(
                SQL_UPSERT_MARK,
                [
                    (
                        record.contract_id,
                        record.settlement_session,
                        record.underlying,
                        record.open,
                        record.high,
                        record.low,
                        record.close,
                        record.volume,
                        record.transaction_count,
                        MARK_SOURCE_DAILY_AGGREGATE,
                        record.observed_at,
                    )
                    for record in records
                ],
            )
        return len(records)

    def session_coverage(
        self, underlying: str, start: date, end: date
    ) -> tuple[dict[str, Any], ...]:
        with self._cursor() as cursor:
            cursor.execute(SQL_SESSION_COVERAGE, (underlying, start, end))
            return tuple(dict(row) for row in cursor.fetchall())

    def open_interest_change(
        self,
        underlying: str,
        session: date,
        prior_session: date,
        limit: int = 100,
    ) -> tuple[dict[str, Any], ...]:
        with self._cursor() as cursor:
            cursor.execute(
                SQL_OPEN_INTEREST_CHANGE,
                (prior_session, underlying, session, limit),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def latest_session_pair(self, underlying: str) -> tuple[date, date] | None:
        with self._cursor() as cursor:
            cursor.execute(SQL_LATEST_SESSION_PAIR, (underlying,))
            sessions = [row["settlement_session"] for row in cursor.fetchall()]
        if len(sessions) < 2:
            return None
        return sessions[0], sessions[1]

    def open_interest_change_detail(
        self, underlying: str, session: date, prior_session: date
    ) -> tuple[dict[str, Any], ...]:
        with self._cursor() as cursor:
            cursor.execute(
                SQL_OPEN_INTEREST_CHANGE_DETAIL,
                (underlying, session, prior_session, underlying, session),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def explain(
        self, query_name: str, params: Sequence[Any], analyze: bool = True
    ) -> list[Any]:
        if query_name not in READ_QUERIES:
            raise KeyError(f"unknown read query: {query_name}")
        prefix = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" if analyze else "EXPLAIN (FORMAT JSON)"
        with self._cursor() as cursor:
            cursor.execute(f"{prefix} {READ_QUERIES[query_name]}", tuple(params))
            row = cursor.fetchone()
            return list(row.values())[0]
