from __future__ import annotations

from datetime import datetime
from typing import Any, Collection

from psycopg2.extras import execute_values

from options.domain import (
    DecisionContext,
    OptionTradeCursor,
    OptionTradeEvent,
    TradeClassificationStatus,
)

from .base import ConnectionFactory, PostgresRepository


class OptionTradeRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist(self, events: Collection[OptionTradeEvent]) -> int:
        if not events:
            return 0
        ordered = tuple(events)
        with self._cursor() as cursor:
            partition_probes = {
                (event.sip_timestamp.year, event.sip_timestamp.month): event.sip_timestamp
                for event in ordered
            }
            for sip_timestamp in partition_probes.values():
                cursor.execute(
                    "SELECT option_market_data_partitions_ready(%s) AS ready",
                    (sip_timestamp,),
                )
                if not cursor.fetchone()["ready"]:
                    raise RuntimeError("option trade partition is missing")

            values = [
                (
                    event.trade_event_id,
                    event.provider,
                    event.contract_id,
                    event.contract_ticker,
                    event.underlyer,
                    event.sip_timestamp,
                    event.sequence_number,
                    event.participant_timestamp,
                    event.first_observed_at,
                    event.revised_observed_at,
                    event.exchange,
                    list(event.conditions),
                    event.correction,
                    event.provider_trade_id,
                    event.price,
                    event.size,
                    event.shares_per_contract,
                    event.notional,
                    event.payload_sha256,
                    event.raw_batch_id,
                    event.classification_status.value,
                )
                for event in ordered
            ]
            execute_values(
                cursor,
                """
                INSERT INTO option_trade_events (
                    trade_event_id, provider, contract_id, contract_ticker, underlying,
                    sip_timestamp, sequence_number, participant_timestamp,
                    first_observed_at, revised_observed_at, exchange, conditions,
                    correction, provider_trade_id, price, size, shares_per_contract,
                    notional, payload_sha256, raw_batch_id, classification_status
                ) VALUES %s
                ON CONFLICT (
                    provider, contract_id, sip_timestamp, sequence_number,
                    participant_timestamp, payload_sha256
                ) DO NOTHING
                """,
                values,
                page_size=2000,
            )
            return cursor.rowcount

    def advance_cursor(
        self,
        provider: str,
        contract_id: int,
        completed_sip_timestamp: datetime,
        completed_sequence_number: int,
        overlap_seconds: int,
        latest_complete_request_id: str | None,
    ) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_trade_cursors (
                    provider, contract_id, completed_sip_timestamp,
                    completed_sequence_number, overlap_seconds,
                    latest_complete_request_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider, contract_id) DO UPDATE
                SET completed_sip_timestamp = EXCLUDED.completed_sip_timestamp,
                    completed_sequence_number = EXCLUDED.completed_sequence_number,
                    overlap_seconds = EXCLUDED.overlap_seconds,
                    latest_complete_request_id = EXCLUDED.latest_complete_request_id,
                    updated_at = NOW()
                WHERE (
                    EXCLUDED.completed_sip_timestamp,
                    EXCLUDED.completed_sequence_number
                ) >= (
                    option_trade_cursors.completed_sip_timestamp,
                    option_trade_cursors.completed_sequence_number
                )
                """,
                (
                    provider,
                    contract_id,
                    completed_sip_timestamp,
                    completed_sequence_number,
                    overlap_seconds,
                    latest_complete_request_id,
                ),
            )
            return cursor.rowcount == 1

    def get_cursor(self, provider: str, contract_id: int) -> OptionTradeCursor | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    completed_sip_timestamp,
                    completed_sequence_number,
                    overlap_seconds
                FROM option_trade_cursors
                WHERE provider = %s AND contract_id = %s
                """,
                (provider, contract_id),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return OptionTradeCursor(
            sip_timestamp=row["completed_sip_timestamp"],
            sequence_number=row["completed_sequence_number"],
            overlap_seconds=row["overlap_seconds"],
        )

    def list_for_contract(
        self,
        contract_id: int,
        start_time: datetime,
        context: DecisionContext,
    ) -> tuple[OptionTradeEvent, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    trade_event_id, provider, contract_id, contract_ticker, underlying,
                    sip_timestamp,
                    sequence_number, participant_timestamp, first_observed_at,
                    revised_observed_at, exchange, conditions, correction,
                    provider_trade_id, price, size, shares_per_contract, notional,
                    payload_sha256, raw_batch_id, classification_status
                FROM option_trade_events
                WHERE contract_id = %s
                  AND sip_timestamp BETWEEN %s AND %s
                  AND COALESCE(revised_observed_at, first_observed_at) <= %s
                ORDER BY sip_timestamp, sequence_number, participant_timestamp NULLS FIRST
                """,
                (contract_id, start_time, context.market_time, context.observed_time),
            )
            rows = cursor.fetchall()
        return tuple(_trade_event(row) for row in rows)

    def list_for_underlyer(
        self,
        underlyer: str,
        start_time: datetime,
        context: DecisionContext,
    ) -> tuple[OptionTradeEvent, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    trade_event_id, provider, contract_id, contract_ticker, underlying,
                    sip_timestamp, sequence_number, participant_timestamp,
                    first_observed_at, revised_observed_at, exchange, conditions,
                    correction, provider_trade_id, price, size, shares_per_contract,
                    notional, payload_sha256, raw_batch_id, classification_status
                FROM option_trade_events
                WHERE underlying = %s
                  AND sip_timestamp BETWEEN %s AND %s
                  AND COALESCE(revised_observed_at, first_observed_at) <= %s
                  AND classification_status = 'INCLUDED'
                ORDER BY sip_timestamp, sequence_number,
                         participant_timestamp NULLS FIRST, trade_event_id
                """,
                (
                    underlyer,
                    start_time,
                    context.market_time,
                    context.observed_time,
                ),
            )
            rows = cursor.fetchall()
        return tuple(_trade_event(row) for row in rows)


def _trade_event(row: dict[str, Any]) -> OptionTradeEvent:
    return OptionTradeEvent(
        trade_event_id=row["trade_event_id"],
        provider=row["provider"],
        contract_id=row["contract_id"],
        contract_ticker=row["contract_ticker"],
        underlyer=row["underlying"],
        sip_timestamp=row["sip_timestamp"],
        sequence_number=row["sequence_number"],
        participant_timestamp=row["participant_timestamp"],
        first_observed_at=row["first_observed_at"],
        revised_observed_at=row["revised_observed_at"],
        exchange=row["exchange"],
        conditions=tuple(row["conditions"]),
        correction=row["correction"],
        provider_trade_id=row["provider_trade_id"],
        price=row["price"],
        size=row["size"],
        shares_per_contract=row["shares_per_contract"],
        notional=row["notional"],
        payload_sha256=row["payload_sha256"],
        raw_batch_id=row["raw_batch_id"],
        classification_status=TradeClassificationStatus(row["classification_status"]),
    )