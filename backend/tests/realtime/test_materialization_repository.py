from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.materialization import (
    MaterializedQuote,
    MaterializedTrade,
    MaterializerShardMap,
    MaterializerShardState,
    build_materializer_checkpoint,
)
from realtime.materialization_repository import (
    persist_quote_materialization,
    persist_quote_materialization_cursor,
)
from realtime.contracts import AssetClass


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
OWNER = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
COHORT = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
GENERATION = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
SESSION = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")


def _state_and_checkpoint(epoch=3):
    shard_map = MaterializerShardMap("canary-v1", 1, ((1, 0),))
    quote = MaterializedQuote(
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=1,
        provider_symbol="SPY",
        source_session=date(2026, 10, 5),
        source_sequence=10,
        source_timestamp=1_000,
        source_timestamp_unit="ms",
        event_at=NOW,
        received_at=NOW,
        bid=Decimal("100"),
        ask=Decimal("101"),
        bid_size=10,
        ask_size=11,
        bid_exchange=1,
        ask_exchange=2,
        quality="GOOD",
        reason_codes=(),
        source_offset=1,
        payload_sha256="a" * 64,
    )
    trade = MaterializedTrade(
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=1,
        provider_symbol="SPY",
        asset_class=AssetClass.OPTION,
        source_session=date(2026, 10, 5),
        source_sequence=11,
        source_timestamp=1_001,
        source_timestamp_unit="ms",
        event_at=NOW,
        received_at=NOW,
        price=Decimal("101"),
        size=Decimal("5"),
        exchange=4,
        conditions=(208,),
        quality="UNCLASSIFIED",
        reason_codes=("CONDITION_POLICY_PENDING",),
        eligibility="INCLUDED",
        eligibility_reason_codes=(
            "PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE",
            "AGGRESSOR_SIDE_UNAVAILABLE",
        ),
        semantics_version="massive_options_conditions_v1",
        source_offset=1,
        payload_sha256="b" * 64,
    )
    state = MaterializerShardState(
        0,
        shard_map.sha256,
        1,
        (quote,),
        (trade,),
        (quote,),
    )
    checkpoint = build_materializer_checkpoint(
        state,
        consumer_name="quote-materializer",
        cohort_id=COHORT,
        journal_generation=GENERATION,
        owner_epoch=epoch,
        committed_at=NOW,
    )
    return shard_map, state, checkpoint


def _connection(*rows):
    cursor = MagicMock()
    cursor.fetchone.side_effect = rows
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def _batch_executor(cursor, statement, parameters, *, page_size):
    assert page_size == 100
    for row in parameters:
        cursor.execute(statement, row)


def test_quote_and_checkpoint_commit_atomically_after_active_lease_fence():
    shard_map, state, checkpoint = _state_and_checkpoint()
    connection, cursor = _connection(
        (str(OWNER), checkpoint.owner_epoch, True),
        None,
        (str(checkpoint.checkpoint_id),),
    )

    persist_quote_materialization(
        connection,
        provider="massive",
        lease_resource_key="materializer:quote:0",
        owner_id=OWNER,
        checkpoint=checkpoint,
        state=state,
        shard_map=shard_map,
        dirty_instrument_ids=frozenset({1}),
        batch_executor=_batch_executor,
    )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    quote_index = next(index for index, row in enumerate(statements) if "INSERT INTO rt.quote_current" in row)
    trade_index = next(index for index, row in enumerate(statements) if "INSERT INTO rt.trade_current" in row)
    checkpoint_index = next(index for index, row in enumerate(statements) if "INSERT INTO rt.consumer_checkpoints" in row)
    assert quote_index < checkpoint_index
    assert trade_index < checkpoint_index
    assert "source_offset < EXCLUDED.source_offset" in statements[quote_index]
    assert "journal_generation <> EXCLUDED.journal_generation" in statements[quote_index]
    assert "source_session < EXCLUDED.source_session" in statements[quote_index]
    assert "source_sequence < EXCLUDED.source_sequence" in statements[quote_index]
    assert "event_at < EXCLUDED.event_at" in statements[quote_index]
    assert "received_at < EXCLUDED.received_at" in statements[quote_index]
    assert "source_session < EXCLUDED.source_session" in statements[trade_index]
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_stale_or_expired_lease_rolls_back_before_projection_write():
    shard_map, state, checkpoint = _state_and_checkpoint()
    connection, cursor = _connection((str(OWNER), checkpoint.owner_epoch + 1, True))

    with pytest.raises(ValueError, match="stale or expired"):
        persist_quote_materialization(
            connection,
            provider="massive",
            lease_resource_key="materializer:quote:0",
            owner_id=OWNER,
            checkpoint=checkpoint,
            state=state,
            shard_map=shard_map,
            dirty_instrument_ids=frozenset({1}),
            batch_executor=_batch_executor,
        )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("INSERT INTO rt.quote_current" in row for row in statements)
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_conflicting_checkpoint_coordinate_rolls_back_projection_and_progress():
    shard_map, state, checkpoint = _state_and_checkpoint()
    connection, _ = _connection(
        (str(OWNER), checkpoint.owner_epoch, True),
        (checkpoint.owner_epoch, checkpoint.committed_offset, UUID(int=0), "f" * 64),
    )

    with pytest.raises(ValueError, match="conflicting state"):
        persist_quote_materialization(
            connection,
            provider="massive",
            lease_resource_key="materializer:quote:0",
            owner_id=OWNER,
            checkpoint=checkpoint,
            state=state,
            shard_map=shard_map,
            dirty_instrument_ids=frozenset({1}),
            batch_executor=_batch_executor,
        )

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_unknown_commit_retry_accepts_identical_checkpoint_identity():
    shard_map, state, checkpoint = _state_and_checkpoint()
    current = (
        checkpoint.owner_epoch,
        checkpoint.committed_offset,
        checkpoint.checkpoint_id,
        checkpoint.state_hash,
    )
    connection, _ = _connection(
        (str(OWNER), checkpoint.owner_epoch, True),
        current,
        None,
    )

    persist_quote_materialization(
        connection,
        provider="massive",
        lease_resource_key="materializer:quote:0",
        owner_id=OWNER,
        checkpoint=checkpoint,
        state=state,
        shard_map=shard_map,
        dirty_instrument_ids=frozenset({1}),
        batch_executor=_batch_executor,
    )

    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_cursor_level_materialization_leaves_transaction_to_worker():
    shard_map, state, checkpoint = _state_and_checkpoint()
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (str(OWNER), checkpoint.owner_epoch, True),
        None,
        (str(checkpoint.checkpoint_id),),
    )

    persist_quote_materialization_cursor(
        cursor,
        provider="massive",
        lease_resource_key="materializer:quote:0",
        owner_id=OWNER,
        checkpoint=checkpoint,
        state=state,
        shard_map=shard_map,
        dirty_instrument_ids=frozenset({1}),
        batch_executor=_batch_executor,
    )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("INSERT INTO rt.quote_current" in statement for statement in statements)
    assert any(
        "INSERT INTO rt.consumer_checkpoints" in statement
        for statement in statements
    )