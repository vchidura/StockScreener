from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from .base import PostgresRepository

OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT = "PROVIDER_CHAIN_SNAPSHOT"
MARK_SOURCE_DAILY_AGGREGATE = "PROVIDER_DAILY_AGGREGATE"
MARK_SOURCE_DAILY_AGGREGATE_UNADJUSTED = "PROVIDER_DAILY_AGGREGATE_UNADJUSTED"


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
    mark_source: str
    mark_adjusted: bool
    valuation_policy_version: str
    valuation_policy_sha256: str
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    volume: int | None = None
    transaction_count: int | None = None

    def __post_init__(self) -> None:
        if self.close <= 0:
            raise ValueError("daily mark close must be positive")
        if self.mark_adjusted:
            raise ValueError("nominal-strike option marks must be unadjusted")
        if self.mark_source != MARK_SOURCE_DAILY_AGGREGATE_UNADJUSTED:
            raise ValueError("daily mark source must identify unadjusted aggregates")
        if not self.mark_source.strip() or not self.valuation_policy_version.strip():
            raise ValueError("daily mark provenance cannot be blank")
        if len(self.valuation_policy_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.valuation_policy_sha256
        ):
            raise ValueError("valuation_policy_sha256 must be a SHA-256 digest")

    @property
    def payload_sha256(self) -> str:
        payload = {
            "contract_id": self.contract_id,
            "settlement_session": self.settlement_session.isoformat(),
            "underlying": self.underlying,
            "open": str(self.open) if self.open is not None else None,
            "high": str(self.high) if self.high is not None else None,
            "low": str(self.low) if self.low is not None else None,
            "close": str(self.close),
            "volume": self.volume,
            "transaction_count": self.transaction_count,
            "mark_source": self.mark_source,
            "mark_adjusted": self.mark_adjusted,
            "valuation_policy_version": self.valuation_policy_version,
            "valuation_policy_sha256": self.valuation_policy_sha256,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()

    @property
    def mark_revision_id(self) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            (
                f"option-daily-mark:{self.contract_id}:"
                f"{self.settlement_session}:{self.valuation_policy_sha256}"
            ),
        )


SQL_INSERT_MARK_REVISION = """
INSERT INTO option_daily_contract_mark_revisions (
    mark_revision_id, contract_id, settlement_session, underlying,
    mark_open, mark_high, mark_low, mark_close, mark_volume,
    mark_transaction_count, mark_source, mark_adjusted,
    mark_observed_at, valuation_policy_version,
    valuation_policy_sha256, payload_sha256
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (contract_id, settlement_session, valuation_policy_sha256) DO NOTHING
RETURNING mark_revision_id
"""


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
    mark_volume, mark_transaction_count, mark_source, mark_observed_at,
    mark_adjusted, mark_valuation_policy_version,
    mark_valuation_policy_sha256
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    mark_adjusted = COALESCE(
        option_daily_contract_facts.mark_adjusted, EXCLUDED.mark_adjusted
    ),
    mark_valuation_policy_version = COALESCE(
        option_daily_contract_facts.mark_valuation_policy_version,
        EXCLUDED.mark_valuation_policy_version
    ),
    mark_valuation_policy_sha256 = COALESCE(
        option_daily_contract_facts.mark_valuation_policy_sha256,
        EXCLUDED.mark_valuation_policy_sha256
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
        inserted = 0
        with self._cursor() as cursor:
            for record in records:
                cursor.execute(
                    SQL_INSERT_MARK_REVISION,
                    (
                        record.mark_revision_id,
                        record.contract_id,
                        record.settlement_session,
                        record.underlying,
                        record.open,
                        record.high,
                        record.low,
                        record.close,
                        record.volume,
                        record.transaction_count,
                        record.mark_source,
                        record.mark_adjusted,
                        record.observed_at,
                        record.valuation_policy_version,
                        record.valuation_policy_sha256,
                        record.payload_sha256,
                    ),
                )
                if cursor.fetchone():
                    inserted += 1
                    continue
                cursor.execute(
                    """
                    SELECT payload_sha256
                    FROM option_daily_contract_mark_revisions
                    WHERE contract_id = %s
                      AND settlement_session = %s
                      AND valuation_policy_sha256 = %s
                    """,
                    (
                        record.contract_id,
                        record.settlement_session,
                        record.valuation_policy_sha256,
                    ),
                )
                if cursor.fetchone()["payload_sha256"] != record.payload_sha256:
                    raise ValueError(
                        "daily mark policy key has different immutable payload"
                    )
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
                        record.mark_source,
                        record.observed_at,
                        record.mark_adjusted,
                        record.valuation_policy_version,
                        record.valuation_policy_sha256,
                    )
                    for record in records
                ],
            )
        return inserted

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
