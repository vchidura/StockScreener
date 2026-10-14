from __future__ import annotations

from contextlib import nullcontext
from uuid import UUID

from psycopg2.extras import execute_batch

from .materialization import (
    MaterializerCheckpoint,
    MaterializerShardMap,
    MaterializerShardState,
    restore_materializer_checkpoint,
)


MATERIALIZATION_BATCH_PAGE_SIZE = 100


class _StatementBatch:
    def __init__(self) -> None:
        self.statement: str | None = None
        self.parameters: list[tuple[object, ...]] = []

    def add(self, statement: str, parameters: tuple[object, ...]) -> None:
        if self.statement is None:
            self.statement = statement
        elif self.statement != statement:
            raise ValueError("materialization batch contains different statements")
        self.parameters.append(parameters)

    def flush(self, cursor, batch_executor) -> None:
        if self.statement is not None and self.parameters:
            batch_executor(
                cursor,
                self.statement,
                self.parameters,
                page_size=MATERIALIZATION_BATCH_PAGE_SIZE,
            )


def _persist_quote_materialization(
    connection,
    *,
    repository_cursor,
    provider: str,
    lease_resource_key: str,
    owner_id: UUID,
    checkpoint: MaterializerCheckpoint,
    state: MaterializerShardState,
    shard_map: MaterializerShardMap,
    dirty_instrument_ids: frozenset[int],
    batch_executor=execute_batch,
) -> None:
    provider_name = provider.strip().lower()
    resource_key = lease_resource_key.strip()
    if not provider_name or not resource_key:
        raise ValueError("provider and lease resource key cannot be blank")
    if not isinstance(dirty_instrument_ids, frozenset) or any(
        type(value) is not int or value <= 0 for value in dirty_instrument_ids
    ):
        raise ValueError("dirty instrument IDs must be a frozenset of positive integers")
    if any(
        shard_map.shard_for(instrument_id) != state.shard_id
        for instrument_id in dirty_instrument_ids
    ):
        raise ValueError("dirty instrument is assigned to a different shard")
    restored = restore_materializer_checkpoint(checkpoint, shard_map)
    if restored != state:
        raise ValueError("checkpoint payload does not match materializer state")
    lineage_rows = (*state.quotes, *state.trades, *state.quote_history)
    if any(
        row.journal_generation != checkpoint.journal_generation
        or row.source_offset > checkpoint.committed_offset
        for row in lineage_rows
    ):
        raise ValueError("materialized lineage is outside the checkpoint boundary")
    if (connection is None) == (repository_cursor is None):
        raise ValueError("exactly one connection or repository cursor is required")
    try:
        cursor_context = (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        )
        with cursor_context as cursor:
            cursor.execute(
                "SELECT owner_id, owner_epoch, lease_until > clock_timestamp() "
                "FROM rt.worker_leases WHERE resource_key=%s FOR UPDATE",
                (resource_key,),
            )
            lease = cursor.fetchone()
            if lease is None or str(lease[0]) != str(owner_id):
                raise ValueError("materializer lease owner does not match")
            if lease[1] != checkpoint.owner_epoch or not lease[2]:
                raise ValueError("materializer lease is stale or expired")
            quote_batch = _StatementBatch()
            for quote in state.quotes:
                if quote.instrument_id not in dirty_instrument_ids:
                    continue
                quote_batch.add(
                    "INSERT INTO rt.quote_current ("
                    "provider, instrument_id, source_session, source_sequence, "
                    "event_time_raw, event_time_unit, event_at, received_at, bid, ask, "
                    "bid_size, ask_size, bid_exchange, ask_exchange, quality, reason_codes, "
                    "session_id, journal_generation, shard_id, source_offset, payload_sha256"
                    ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (provider, instrument_id) DO UPDATE SET "
                    "source_session=EXCLUDED.source_session, "
                    "source_sequence=EXCLUDED.source_sequence, "
                    "event_time_raw=EXCLUDED.event_time_raw, "
                    "event_time_unit=EXCLUDED.event_time_unit, event_at=EXCLUDED.event_at, "
                    "received_at=EXCLUDED.received_at, bid=EXCLUDED.bid, ask=EXCLUDED.ask, "
                    "bid_size=EXCLUDED.bid_size, ask_size=EXCLUDED.ask_size, "
                    "bid_exchange=EXCLUDED.bid_exchange, ask_exchange=EXCLUDED.ask_exchange, "
                    "quality=EXCLUDED.quality, reason_codes=EXCLUDED.reason_codes, "
                    "session_id=EXCLUDED.session_id, "
                    "journal_generation=EXCLUDED.journal_generation, "
                    "shard_id=EXCLUDED.shard_id, source_offset=EXCLUDED.source_offset, "
                    "payload_sha256=EXCLUDED.payload_sha256, updated_at=clock_timestamp() "
                    "WHERE (rt.quote_current.journal_generation = EXCLUDED.journal_generation "
                    "AND rt.quote_current.source_offset < EXCLUDED.source_offset) "
                    "OR (rt.quote_current.journal_generation <> EXCLUDED.journal_generation "
                    "AND (rt.quote_current.source_session < EXCLUDED.source_session "
                    "OR (rt.quote_current.source_session = EXCLUDED.source_session AND ("
                    "(rt.quote_current.source_sequence IS NOT NULL "
                    "AND EXCLUDED.source_sequence IS NOT NULL "
                    "AND rt.quote_current.source_sequence < EXCLUDED.source_sequence) "
                    "OR ((rt.quote_current.source_sequence IS NULL "
                    "OR EXCLUDED.source_sequence IS NULL "
                    "OR rt.quote_current.source_sequence = EXCLUDED.source_sequence) "
                    "AND (rt.quote_current.event_at < EXCLUDED.event_at "
                    "OR (rt.quote_current.event_at = EXCLUDED.event_at "
                    "AND rt.quote_current.received_at < EXCLUDED.received_at)))))))",
                    (
                        provider_name,
                        quote.instrument_id,
                        quote.source_session,
                        quote.source_sequence,
                        quote.source_timestamp,
                        quote.source_timestamp_unit,
                        quote.event_at,
                        quote.received_at,
                        quote.bid,
                        quote.ask,
                        quote.bid_size,
                        quote.ask_size,
                        quote.bid_exchange,
                        quote.ask_exchange,
                        quote.quality,
                        list(quote.reason_codes),
                        str(quote.stream_session_id),
                        str(quote.journal_generation),
                        state.shard_id,
                        quote.source_offset,
                        quote.payload_sha256,
                    ),
                )
            quote_batch.flush(cursor, batch_executor)
            trade_batch = _StatementBatch()
            for trade in state.trades:
                if trade.instrument_id not in dirty_instrument_ids:
                    continue
                reasons = tuple(dict.fromkeys((
                    *trade.reason_codes,
                    *trade.eligibility_reason_codes,
                )))
                trade_batch.add(
                    "INSERT INTO rt.trade_current ("
                    "provider, instrument_id, source_session, source_sequence, "
                    "event_time_raw, event_time_unit, event_at, received_at, price, size, "
                    "exchange, conditions, eligibility, eligibility_policy_version, "
                    "reason_codes, session_id, journal_generation, shard_id, source_offset, "
                    "payload_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (provider, instrument_id) DO UPDATE SET "
                    "source_session=EXCLUDED.source_session, "
                    "source_sequence=EXCLUDED.source_sequence, "
                    "event_time_raw=EXCLUDED.event_time_raw, "
                    "event_time_unit=EXCLUDED.event_time_unit, event_at=EXCLUDED.event_at, "
                    "received_at=EXCLUDED.received_at, price=EXCLUDED.price, "
                    "size=EXCLUDED.size, exchange=EXCLUDED.exchange, "
                    "conditions=EXCLUDED.conditions, eligibility=EXCLUDED.eligibility, "
                    "eligibility_policy_version=EXCLUDED.eligibility_policy_version, "
                    "reason_codes=EXCLUDED.reason_codes, session_id=EXCLUDED.session_id, "
                    "journal_generation=EXCLUDED.journal_generation, "
                    "shard_id=EXCLUDED.shard_id, source_offset=EXCLUDED.source_offset, "
                    "payload_sha256=EXCLUDED.payload_sha256, updated_at=clock_timestamp() "
                    "WHERE (rt.trade_current.journal_generation = EXCLUDED.journal_generation "
                    "AND rt.trade_current.source_offset < EXCLUDED.source_offset) "
                    "OR (rt.trade_current.journal_generation <> EXCLUDED.journal_generation "
                    "AND (rt.trade_current.source_session < EXCLUDED.source_session "
                    "OR (rt.trade_current.source_session = EXCLUDED.source_session AND ("
                    "(rt.trade_current.source_sequence IS NOT NULL "
                    "AND EXCLUDED.source_sequence IS NOT NULL "
                    "AND rt.trade_current.source_sequence < EXCLUDED.source_sequence) "
                    "OR ((rt.trade_current.source_sequence IS NULL "
                    "OR EXCLUDED.source_sequence IS NULL "
                    "OR rt.trade_current.source_sequence = EXCLUDED.source_sequence) "
                    "AND (rt.trade_current.event_at < EXCLUDED.event_at "
                    "OR (rt.trade_current.event_at = EXCLUDED.event_at "
                    "AND rt.trade_current.received_at < EXCLUDED.received_at)))))))",
                    (
                        provider_name,
                        trade.instrument_id,
                        trade.source_session,
                        trade.source_sequence,
                        trade.source_timestamp,
                        trade.source_timestamp_unit,
                        trade.event_at,
                        trade.received_at,
                        trade.price,
                        trade.size,
                        trade.exchange,
                        list(trade.conditions),
                        trade.eligibility,
                        trade.semantics_version,
                        list(reasons),
                        str(trade.stream_session_id),
                        str(trade.journal_generation),
                        state.shard_id,
                        trade.source_offset,
                        trade.payload_sha256,
                    ),
                )
            trade_batch.flush(cursor, batch_executor)
            cursor.execute(
                "SELECT owner_epoch, committed_offset, checkpoint_id, state_hash "
                "FROM rt.consumer_checkpoints WHERE consumer_name=%s AND cohort_id=%s "
                "AND journal_generation=%s AND shard_id=%s FOR UPDATE",
                (
                    checkpoint.consumer_name,
                    str(checkpoint.cohort_id),
                    str(checkpoint.journal_generation),
                    checkpoint.shard_id,
                ),
            )
            current = cursor.fetchone()
            idempotent = current is not None and (
                current[0], current[1], str(current[2]), current[3]
            ) == (
                checkpoint.owner_epoch,
                checkpoint.committed_offset,
                str(checkpoint.checkpoint_id),
                checkpoint.state_hash,
            )
            if current is not None and current[0] > checkpoint.owner_epoch:
                raise ValueError("stale owner epoch cannot advance checkpoint")
            if current is not None and current[1] > checkpoint.committed_offset:
                raise ValueError("checkpoint offset cannot move backward")
            if current is not None and current[1] == checkpoint.committed_offset and not idempotent:
                raise ValueError("checkpoint coordinate has conflicting state")
            cursor.execute(
                "INSERT INTO rt.consumer_checkpoints ("
                "consumer_name, cohort_id, journal_generation, shard_id, shard_map_sha256, "
                "owner_epoch, committed_offset, checkpoint_id, state_codec, state_hash, "
                "state_payload, committed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s) ON CONFLICT (consumer_name, cohort_id, "
                "journal_generation, shard_id) DO UPDATE SET "
                "shard_map_sha256=EXCLUDED.shard_map_sha256, "
                "owner_epoch=EXCLUDED.owner_epoch, committed_offset=EXCLUDED.committed_offset, "
                "checkpoint_id=EXCLUDED.checkpoint_id, state_codec=EXCLUDED.state_codec, "
                "state_hash=EXCLUDED.state_hash, state_payload=EXCLUDED.state_payload, "
                "committed_at=EXCLUDED.committed_at, updated_at=clock_timestamp() "
                "WHERE rt.consumer_checkpoints.owner_epoch <= EXCLUDED.owner_epoch "
                "AND rt.consumer_checkpoints.committed_offset < EXCLUDED.committed_offset "
                "RETURNING checkpoint_id",
                (
                    checkpoint.consumer_name,
                    str(checkpoint.cohort_id),
                    str(checkpoint.journal_generation),
                    checkpoint.shard_id,
                    state.shard_map_sha256,
                    checkpoint.owner_epoch,
                    checkpoint.committed_offset,
                    str(checkpoint.checkpoint_id),
                    checkpoint.state_codec,
                    checkpoint.state_hash,
                    checkpoint.state_payload,
                    checkpoint.committed_at,
                ),
            )
            persisted = cursor.fetchone()
            if not idempotent and (
                persisted is None or str(persisted[0]) != str(checkpoint.checkpoint_id)
            ):
                raise ValueError("checkpoint advancement was rejected")
        if connection is not None:
            connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise


def persist_quote_materialization_cursor(
    cursor,
    *,
    provider: str,
    lease_resource_key: str,
    owner_id: UUID,
    checkpoint: MaterializerCheckpoint,
    state: MaterializerShardState,
    shard_map: MaterializerShardMap,
    dirty_instrument_ids: frozenset[int],
    batch_executor=execute_batch,
) -> None:
    _persist_quote_materialization(
        None,
        repository_cursor=cursor,
        provider=provider,
        lease_resource_key=lease_resource_key,
        owner_id=owner_id,
        checkpoint=checkpoint,
        state=state,
        shard_map=shard_map,
        dirty_instrument_ids=dirty_instrument_ids,
        batch_executor=batch_executor,
    )


def persist_quote_materialization(
    connection,
    *,
    provider: str,
    lease_resource_key: str,
    owner_id: UUID,
    checkpoint: MaterializerCheckpoint,
    state: MaterializerShardState,
    shard_map: MaterializerShardMap,
    dirty_instrument_ids: frozenset[int],
    batch_executor=execute_batch,
) -> None:
    _persist_quote_materialization(
        connection,
        repository_cursor=None,
        provider=provider,
        lease_resource_key=lease_resource_key,
        owner_id=owner_id,
        checkpoint=checkpoint,
        state=state,
        shard_map=shard_map,
        dirty_instrument_ids=dirty_instrument_ids,
        batch_executor=batch_executor,
    )