from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from options.errors import DuplicateFactConflict
from options.event_calendar import OptionEventCalendarCoverage, OptionMarketEvent

from .base import ConnectionFactory, PostgresRepository


@dataclass(frozen=True, slots=True)
class OptionEventCalendarPersistResult:
    coverage_inserted: int
    events_inserted: int


class OptionMarketEventRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist_batch(
        self,
        coverage: Sequence[OptionEventCalendarCoverage],
        events: Sequence[OptionMarketEvent],
    ) -> OptionEventCalendarPersistResult:
        with self._cursor() as cursor:
            coverage_inserted = sum(
                self._persist_coverage(cursor, item) for item in coverage
            )
            events_inserted = sum(self._persist_event(cursor, item) for item in events)
        return OptionEventCalendarPersistResult(coverage_inserted, events_inserted)

    def latest_active_events(
        self,
        *,
        source: str,
        event_type: str,
        affected_underlyings: Sequence[str],
        window_start: datetime,
        window_end: datetime,
        observed_at: datetime,
    ) -> tuple[dict, ...]:
        if not affected_underlyings:
            return ()
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (source, source_key)
                        source_key, affected_underlying, scheduled_time,
                        confidence, status
                    FROM option_market_events
                    WHERE source = %s
                      AND event_type = %s
                      AND first_observed_at <= %s
                    ORDER BY source, source_key, first_observed_at DESC
                )
                SELECT source_key, affected_underlying, scheduled_time,
                       confidence, status
                FROM latest
                WHERE affected_underlying = ANY(%s)
                  AND scheduled_time BETWEEN %s AND %s
                  AND status IN ('SCHEDULED', 'REVISED')
                ORDER BY source_key
                """,
                (
                    source, event_type, observed_at,
                    list(affected_underlyings), window_start, window_end,
                ),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    @staticmethod
    def _persist_coverage(cursor, item: OptionEventCalendarCoverage) -> int:
        cursor.execute(
            """
            INSERT INTO option_event_calendar_coverage (
                coverage_id, event_type, affected_underlying,
                window_start, window_end, source, source_key,
                first_observed_at, source_observed_at, payload_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, source_key, first_observed_at) DO NOTHING
            RETURNING coverage_id
            """,
            (
                item.coverage_id, item.event_type.value, item.affected_underlying,
                item.window_start, item.window_end, item.source, item.source_key,
                item.first_observed_at, item.source_observed_at,
                item.payload_sha256,
            ),
        )
        if cursor.fetchone():
            return 1
        cursor.execute(
            """
            SELECT payload_sha256
            FROM option_event_calendar_coverage
            WHERE source = %s AND source_key = %s AND first_observed_at = %s
            """,
            (item.source, item.source_key, item.first_observed_at),
        )
        if cursor.fetchone()["payload_sha256"] != item.payload_sha256:
            raise DuplicateFactConflict("event coverage key has different immutable payload")
        return 0

    @staticmethod
    def _persist_event(cursor, item: OptionMarketEvent) -> int:
        cursor.execute(
            """
            INSERT INTO option_market_events (
                market_event_id, event_type, affected_underlying,
                scheduled_time, source, source_key, announcement_time,
                first_observed_at, source_observed_at,
                revised_observed_at, confidence,
                status, payload_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
            ON CONFLICT (source, source_key, first_observed_at) DO NOTHING
            RETURNING market_event_id
            """,
            (
                item.market_event_id, item.event_type.value,
                item.affected_underlying, item.scheduled_time, item.source,
                item.source_key, item.announcement_time, item.first_observed_at,
                item.source_observed_at,
                item.confidence.value, item.status.value, item.payload_sha256,
            ),
        )
        if cursor.fetchone():
            return 1
        cursor.execute(
            """
            SELECT payload_sha256
            FROM option_market_events
            WHERE source = %s AND source_key = %s AND first_observed_at = %s
            """,
            (item.source, item.source_key, item.first_observed_at),
        )
        if cursor.fetchone()["payload_sha256"] != item.payload_sha256:
            raise DuplicateFactConflict("market event key has different immutable payload")
        return 0