from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from realtime.contracts import AssetClass, MarketChannel
from realtime.journal import NormalizedJournalRecord
from realtime.materialization import (
    AggressorSide,
    MaterializerShardMap,
    assess_trade_at_nbbo,
    build_materializer_checkpoint,
    dirty_materializer_instruments,
    initial_materializer_state,
    materialize_shard_batch,
    materialize_shard_fanout,
    restore_materializer_checkpoint,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
GENERATION = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _record(
    offset,
    instrument_id,
    shard_id,
    *,
    sequence=None,
    timestamp=None,
    event_kind=MarketChannel.QUOTE,
    event_at=None,
    received_at=None,
    price=None,
    bid=None,
    ask=None,
    conditions=(),
    asset_class=AssetClass.EQUITY,
    quality=None,
    reasons=None,
):
    event_time = event_at or NOW + timedelta(milliseconds=offset)
    receipt_time = received_at or event_time + timedelta(milliseconds=1)
    event_quality = quality or (
        "GOOD" if event_kind is MarketChannel.QUOTE else "UNCLASSIFIED"
    )
    event_reasons = reasons if reasons is not None else (
        () if event_kind is MarketChannel.QUOTE else ("CONDITION_POLICY_PENDING",)
    )
    return NormalizedJournalRecord(
        event_offset=offset,
        raw_offset=offset,
        event_index=0,
        normalizer_version="fixture-v1",
        journal_generation=GENERATION,
        stream_session_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        instrument_id=instrument_id,
        provider_symbol={1: "SPY", 2: "QQQ"}[instrument_id],
        underlying={1: "SPY", 2: "QQQ"}[instrument_id],
        asset_class=asset_class,
        shard_id=shard_id,
        event_kind=event_kind,
        source_session=date(2026, 10, 5),
        source_sequence=sequence,
        source_timestamp=timestamp if timestamp is not None else 1_000 + offset,
        source_timestamp_unit="ms",
        event_at=event_time,
        price=(price if event_kind is MarketChannel.TRADE else None),
        size=(Decimal("10") if event_kind is MarketChannel.TRADE else None),
        bid=(bid if bid is not None else Decimal("100") + offset)
        if event_kind is MarketChannel.QUOTE else None,
        ask=(ask if ask is not None else Decimal("101") + offset)
        if event_kind is MarketChannel.QUOTE else None,
        bid_size=10 if event_kind is MarketChannel.QUOTE else None,
        ask_size=11 if event_kind is MarketChannel.QUOTE else None,
        exchange=4 if event_kind is MarketChannel.TRADE else None,
        conditions=tuple(conditions),
        quality=event_quality,
        reason_codes=tuple(event_reasons),
        details_json=(
            '{"ask_exchange":2,"bid_exchange":1}'
            if event_kind is MarketChannel.QUOTE else "{}"
        ),
        payload_sha256=f"{offset:064x}",
        received_at=receipt_time,
        committed_at=receipt_time + timedelta(milliseconds=1),
    )


def _map():
    return MaterializerShardMap("canary-eight-v1", 2, ((1, 0), (2, 1)))


def test_shard_map_identity_is_deterministic_and_rejects_drift():
    first = _map()
    repeated = MaterializerShardMap("canary-eight-v1", 2, ((2, 1), (1, 0)))

    assert first.sha256 == repeated.sha256
    assert first.shard_for(1) == 0

    with pytest.raises(ValueError, match="unique"):
        MaterializerShardMap("canary-eight-v1", 2, ((1, 0), (1, 1)))
    with pytest.raises(ValueError, match="outside"):
        MaterializerShardMap("canary-eight-v1", 2, ((1, 2),))


def test_empty_shard_range_advances_and_restart_replay_is_deterministic():
    shard_map = _map()
    initial = initial_materializer_state(shard_map, 0)
    empty_for_shard = materialize_shard_batch(initial, shard_map, (_record(1, 2, 1),))

    assert empty_for_shard.scanned_offset == 1
    assert empty_for_shard.quotes == ()

    checkpoint = build_materializer_checkpoint(
        empty_for_shard,
        consumer_name="quote-materializer",
        cohort_id=COHORT,
        journal_generation=GENERATION,
        owner_epoch=1,
        committed_at=NOW,
    )
    restored = restore_materializer_checkpoint(checkpoint, shard_map)
    next_record = (_record(2, 1, 0),)
    resumed = materialize_shard_batch(restored, shard_map, next_record)
    uninterrupted = materialize_shard_batch(
        initial,
        shard_map,
        (_record(1, 2, 1), *next_record),
    )

    assert resumed == uninterrupted
    assert resumed.scanned_offset == 2
    assert resumed.quotes[0].received_at == NOW + timedelta(milliseconds=3)
    assert resumed.quotes[0].bid_exchange == 1
    assert resumed.quotes[0].ask_exchange == 2


def test_newer_invalid_quote_replaces_readiness_but_older_quote_does_not():
    shard_map = _map()
    state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (_record(1, 1, 0, sequence=10, timestamp=2_000),),
    )
    state = materialize_shard_batch(
        state,
        shard_map,
        (_record(2, 1, 0, sequence=9, timestamp=3_000),),
    )
    assert state.quotes[0].source_sequence == 10

    state = materialize_shard_batch(
        state,
        shard_map,
        (_record(3, 1, 0, sequence=11, quality="INVALID", reasons=("EMPTY_QUOTE",)),),
    )
    assert state.quotes[0].quality == "INVALID"
    assert state.quotes[0].reason_codes == ("EMPTY_QUOTE",)
    assert state.scanned_offset == 3


def test_checkpoint_rejects_hash_offset_map_and_size_drift():
    shard_map = _map()
    state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (_record(1, 1, 0),),
    )
    checkpoint = build_materializer_checkpoint(
        state,
        consumer_name="quote-materializer",
        cohort_id=COHORT,
        journal_generation=GENERATION,
        owner_epoch=2,
        committed_at=NOW,
    )

    with pytest.raises(ValueError, match="hash"):
        restore_materializer_checkpoint(
            replace(checkpoint, state_hash="f" * 64),
            shard_map,
        )
    with pytest.raises(ValueError, match="ahead"):
        restore_materializer_checkpoint(
            replace(checkpoint, committed_offset=2),
            shard_map,
        )
    with pytest.raises(ValueError, match="shard map drifted"):
        restore_materializer_checkpoint(
            checkpoint,
            MaterializerShardMap("changed", 2, ((1, 0), (2, 1))),
        )
    with pytest.raises(ValueError, match="byte limit"):
        restore_materializer_checkpoint(checkpoint, shard_map, maximum_bytes=1)


def test_replay_rejects_noncontiguous_offsets_and_shard_map_mismatch():
    shard_map = _map()
    initial = initial_materializer_state(shard_map, 0)

    with pytest.raises(ValueError, match="contiguous"):
        materialize_shard_batch(initial, shard_map, (_record(2, 1, 0),))
    with pytest.raises(ValueError, match="active map"):
        materialize_shard_batch(
            replace(initial, shard_map_sha256="f" * 64),
            shard_map,
            (_record(1, 1, 0),),
        )
    with pytest.raises(ValueError, match="does not match"):
        materialize_shard_batch(initial, shard_map, (_record(1, 1, 1),))


def test_trade_state_classifies_conditions_and_causal_nbbo_across_restart():
    shard_map = _map()
    state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (
            _record(1, 1, 0, bid=Decimal("100"), ask=Decimal("101")),
            _record(
                2,
                1,
                0,
                event_kind=MarketChannel.TRADE,
                asset_class=AssetClass.OPTION,
                price=Decimal("101"),
                conditions=(208,),
            ),
        ),
    )

    trade = state.trades[0]
    assert trade.eligibility == "INCLUDED"
    assert trade.conditions == (208,)
    assessment = assess_trade_at_nbbo(
        trade,
        state.quote_history,
        decision_at=trade.received_at,
    )
    assert assessment.side is AggressorSide.ASK
    assert assessment.quote_source_offset == 1

    checkpoint = build_materializer_checkpoint(
        state,
        consumer_name="trade-materializer",
        cohort_id=COHORT,
        journal_generation=GENERATION,
        owner_epoch=1,
        committed_at=NOW,
    )
    assert restore_materializer_checkpoint(checkpoint, shard_map) == state


def test_causal_nbbo_uses_bad_latest_quote_and_equal_time_is_ambiguous():
    shard_map = _map()
    state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (
            _record(1, 1, 0, bid=Decimal("100"), ask=Decimal("101")),
            _record(
                2,
                1,
                0,
                quality="INVALID",
                reasons=("EMPTY_QUOTE",),
                bid=Decimal("100"),
                ask=Decimal("101"),
            ),
            _record(
                3,
                1,
                0,
                event_kind=MarketChannel.TRADE,
                asset_class=AssetClass.OPTION,
                price=Decimal("101"),
                conditions=(208,),
            ),
        ),
    )

    invalid = assess_trade_at_nbbo(
        state.trades[0],
        state.quote_history,
        decision_at=state.trades[0].received_at,
    )
    assert invalid.side is AggressorSide.UNKNOWN
    assert invalid.reason_codes == ("CAUSAL_QUOTE_INVALID",)
    assert invalid.quote_source_offset == 2

    shared_time = NOW + timedelta(milliseconds=20)
    equal_state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (
            _record(1, 1, 0, event_at=shared_time),
            _record(
                2,
                1,
                0,
                event_kind=MarketChannel.TRADE,
                asset_class=AssetClass.OPTION,
                event_at=shared_time,
                received_at=shared_time + timedelta(milliseconds=2),
                price=Decimal("101"),
                conditions=(208,),
            ),
        ),
    )
    ambiguous = assess_trade_at_nbbo(
        equal_state.trades[0],
        equal_state.quote_history,
        decision_at=equal_state.trades[0].received_at,
    )
    assert ambiguous.side is AggressorSide.UNKNOWN
    assert ambiguous.reason_codes == ("EQUAL_TIMESTAMP_ORDER_AMBIGUOUS",)


def test_quote_history_is_bounded_and_allows_reviewed_reordering_window():
    shard_map = _map()
    trade_time = NOW + timedelta(milliseconds=100)
    trade_receipt = trade_time + timedelta(milliseconds=1)
    delayed_quote_receipt = trade_receipt + timedelta(milliseconds=20)
    state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (
            _record(
                1,
                1,
                0,
                event_kind=MarketChannel.TRADE,
                asset_class=AssetClass.OPTION,
                event_at=trade_time,
                received_at=trade_receipt,
                price=Decimal("101"),
                conditions=(208,),
            ),
            _record(
                2,
                1,
                0,
                event_at=trade_time - timedelta(milliseconds=10),
                received_at=delayed_quote_receipt,
                bid=Decimal("100"),
                ask=Decimal("101"),
            ),
            _record(3, 1, 0),
        ),
        maximum_quote_history_per_instrument=1,
    )

    assert [quote.source_offset for quote in state.quote_history] == [3]

    untrimmed = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (
            _record(
                1,
                1,
                0,
                event_kind=MarketChannel.TRADE,
                asset_class=AssetClass.OPTION,
                event_at=trade_time,
                received_at=trade_receipt,
                price=Decimal("101"),
                conditions=(208,),
            ),
            _record(
                2,
                1,
                0,
                event_at=trade_time - timedelta(milliseconds=10),
                received_at=delayed_quote_receipt,
                bid=Decimal("100"),
                ask=Decimal("101"),
            ),
        ),
    )
    reordered = assess_trade_at_nbbo(
        untrimmed.trades[0],
        untrimmed.quote_history,
        decision_at=delayed_quote_receipt,
    )
    assert reordered.side is AggressorSide.ASK
    assert reordered.quote_source_offset == 2


def test_equity_trade_condition_policy_remains_fail_closed():
    shard_map = _map()
    state = materialize_shard_batch(
        initial_materializer_state(shard_map, 0),
        shard_map,
        (_record(
            1,
            1,
            0,
            event_kind=MarketChannel.TRADE,
            asset_class=AssetClass.EQUITY,
            price=Decimal("101"),
            conditions=(208,),
        ),),
    )

    assert state.trades[0].eligibility == "UNKNOWN"
    assert state.trades[0].eligibility_reason_codes == (
        "EQUITY_TRADE_CONDITION_POLICY_PENDING",
    )


def test_dirty_instrument_diff_ignores_empty_scan_progress():
    shard_map = _map()
    initial = initial_materializer_state(shard_map, 0)
    quote_state = materialize_shard_batch(
        initial,
        shard_map,
        (_record(1, 1, 0),),
    )
    empty_progress = materialize_shard_batch(
        quote_state,
        shard_map,
        (_record(2, 2, 1),),
    )

    assert dirty_materializer_instruments(initial, quote_state) == frozenset({1})
    assert dirty_materializer_instruments(quote_state, empty_progress) == frozenset()


def test_one_read_fanout_partitions_once_and_advances_every_logical_shard(monkeypatch):
    shard_map = _map()
    states = tuple(
        initial_materializer_state(shard_map, shard_id)
        for shard_id in range(shard_map.shard_count)
    )
    shard_lookups = []
    original_shard_for = MaterializerShardMap.shard_for

    def counted_shard_for(self, instrument_id):
        shard_lookups.append(instrument_id)
        return original_shard_for(self, instrument_id)

    monkeypatch.setattr(MaterializerShardMap, "shard_for", counted_shard_for)

    advanced = materialize_shard_fanout(
        states,
        shard_map,
        (_record(1, 1, 0),),
    )

    assert shard_lookups == [1]
    assert [state.scanned_offset for state in advanced] == [1, 1]
    assert len(advanced[0].quotes) == 1
    assert advanced[1].quotes == ()