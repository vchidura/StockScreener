from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

import realtime.advanced_equity_journal_bridge as bridge
from realtime.contracts import AssetClass, MarketChannel
from realtime.database_access import RealtimeDatabaseRole
from realtime.journal import DecoderProgress, NormalizedJournalRecord, RawJournalReader
from realtime.stage_database_v2 import V2WorkerDatabase


NOW = datetime(2026, 10, 14, 20, 5, tzinfo=timezone.utc)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")
SECURITY = UUID("44444444-4444-4444-4444-444444444444")


class Database(V2WorkerDatabase):
    def __init__(self):
        self._process_plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)
        self.cursor = MagicMock()

    def transaction(self, **options):
        return nullcontext(self.cursor)


def config():
    return bridge.AdvancedEquityJournalBridgeConfig(
        cohort_id=COHORT,
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        owner_epoch=7,
        normalizer_version="massive_market_event_v1",
        shard_ids=(0, 1),
        batch_limit=10,
    )


def record(
    offset,
    *,
    shard=0,
    kind=MarketChannel.MINUTE_AGGREGATE,
    asset=AssetClass.EQUITY,
    quality="GOOD",
):
    end = NOW + timedelta(minutes=offset)
    start = end - timedelta(minutes=1)
    return NormalizedJournalRecord(
        event_offset=offset,
        raw_offset=offset,
        event_index=0,
        normalizer_version="massive_market_event_v1",
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=1,
        provider_symbol="SPY",
        underlying="SPY",
        asset_class=asset,
        shard_id=shard,
        event_kind=kind,
        source_session=date(2026, 10, 14),
        source_sequence=offset,
        source_timestamp=int(end.timestamp() * 1000),
        source_timestamp_unit="ms",
        event_at=end,
        price=Decimal("101") if kind is MarketChannel.TRADE else None,
        size=Decimal("1000"),
        bid=None,
        ask=None,
        bid_size=None,
        ask_size=None,
        exchange=None,
        conditions=(),
        quality=quality,
        reason_codes=() if quality == "GOOD" else ("INVALID_AM",),
        details_json=(
            '{"close":"101","end_timestamp":'
            + str(int(end.timestamp() * 1000))
            + ',"high":"102","low":"99","open":"100","start_timestamp":'
            + str(int(start.timestamp() * 1000))
            + ',"vwap":"100.5"}'
        ),
        payload_sha256=f"{offset:064x}",
        received_at=end + timedelta(milliseconds=1),
        committed_at=end + timedelta(milliseconds=2),
    )


def progress(offset, *, updated_at=None):
    return DecoderProgress(
        generation=GENERATION,
        normalizer_version="massive_market_event_v1",
        committed_raw_offset=offset,
        committed_event_offset=offset,
        updated_at=updated_at or NOW + timedelta(minutes=offset, milliseconds=3),
    )


def reader(records, decoder_progress):
    value = MagicMock(spec=RawJournalReader)
    value.decoder_progress.return_value = decoder_progress
    value.read_normalized_after.return_value = tuple(records)
    return value


def test_scan_is_bounded_and_advances_empty_configured_shards_through_batch():
    source = reader(
        (
            record(1, kind=MarketChannel.QUOTE),
            record(2),
            record(3, kind=MarketChannel.TRADE),
        ),
        progress(3),
    )

    batch = bridge.scan_advanced_equity_journal_batch(
        source, config(), {0: 0, 1: 0}
    )

    assert batch.scanned_through_offset == 3
    assert [row.event_offset for row in batch.processable_records] == [2]
    assert batch.target_offsets == {0: 3, 1: 3}
    assert batch.event_time_watermark == NOW + timedelta(minutes=3)
    source.read_normalized_after.assert_called_once_with(0, limit=10)


@pytest.mark.parametrize(
    "bad_record",
    [
        record(1, quality="INVALID"),
        record(1, shard=2),
    ],
)
def test_scan_refuses_relevant_invalid_am_finality(bad_record):
    with pytest.raises(ValueError, match="AM"):
        bridge.scan_advanced_equity_journal_batch(
            reader((bad_record,), progress(1)), config(), {0: 0, 1: 0}
        )


def test_process_builds_exact_finality_and_persists_bars_with_all_shard_checkpoints_atomically(
    monkeypatch,
):
    database = Database()
    source = reader((record(1),), progress(1))
    load = MagicMock(return_value={0: 0, 1: 0})
    verify_session = MagicMock()
    no_gaps = MagicMock()
    security_ids = MagicMock(return_value={1: SECURITY})
    checkpoints = MagicMock(return_value=2)
    persist = MagicMock()
    adapted = MagicMock(return_value=object())
    monkeypatch.setattr(bridge, "_load_checkpoint_offsets_cursor", load)
    monkeypatch.setattr(bridge, "_verify_stream_session_cursor", verify_session)
    monkeypatch.setattr(bridge, "_require_no_unresolved_am_gaps_cursor", no_gaps)
    monkeypatch.setattr(bridge, "_load_security_ids_cursor", security_ids)
    monkeypatch.setattr(bridge, "_persist_consumer_checkpoints_cursor", checkpoints)
    monkeypatch.setattr(bridge, "persist_realtime_equity_bars_cursor", persist)
    monkeypatch.setattr(bridge, "adapt_advanced_minute_aggregate", adapted)

    status = bridge.process_advanced_equity_journal_once(
        database, source, config(), checked_at=NOW + timedelta(minutes=2)
    )

    assert (status.scanned_records, status.persisted_bars, status.shards_advanced) == (
        1,
        1,
        2,
    )
    finality = adapted.call_args.kwargs["finality"]
    assert finality.journal_generation == GENERATION
    assert finality.stream_session_id == SESSION
    assert finality.owner_epoch == 7
    assert finality.normalized_through_offset == 1
    assert finality.event_time_watermark == NOW + timedelta(minutes=1)
    assert finality.proved_at == progress(1).updated_at
    assert finality.gap_free is True
    persist.assert_called_once_with(database.cursor, (adapted.return_value,))
    checkpoint_args = checkpoints.call_args
    assert checkpoint_args.args[0] is database.cursor
    assert checkpoint_args.kwargs["expected_offsets"] == {0: 0, 1: 0}
    assert checkpoint_args.kwargs["target_offsets"] == {0: 1, 1: 1}


def test_unresolved_relevant_gap_prevents_bar_and_checkpoint_advancement(monkeypatch):
    database = Database()
    source = reader((record(1),), progress(1))
    monkeypatch.setattr(
        bridge, "_load_checkpoint_offsets_cursor", MagicMock(return_value={0: 0, 1: 0})
    )
    monkeypatch.setattr(bridge, "_verify_stream_session_cursor", MagicMock())
    monkeypatch.setattr(
        bridge,
        "_require_no_unresolved_am_gaps_cursor",
        MagicMock(side_effect=ValueError("unresolved AM gap")),
    )
    persist = MagicMock()
    checkpoints = MagicMock()
    monkeypatch.setattr(bridge, "persist_realtime_equity_bars_cursor", persist)
    monkeypatch.setattr(bridge, "_persist_consumer_checkpoints_cursor", checkpoints)

    with pytest.raises(ValueError, match="unresolved AM gap"):
        bridge.process_advanced_equity_journal_once(
            database, source, config(), checked_at=NOW + timedelta(minutes=2)
        )

    persist.assert_not_called()
    checkpoints.assert_not_called()
