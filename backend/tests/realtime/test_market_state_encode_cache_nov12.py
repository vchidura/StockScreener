"""V3-1.6b (Nov 12): the cached market-state checkpoint encode gives the full encode's bytes on every pass."""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

import realtime.materialization as m
from realtime.contracts import AssetClass, MarketChannel
from realtime.journal import NormalizedJournalRecord

UTC = timezone.utc
NOW = datetime(2026, 11, 12, 14, 30, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
GENERATION = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
SYMBOLS = {1: "O:SPY261127C00600000", 2: "O:SPY261127P00590000", 3: "O:QQQ261127C00500000"}


@pytest.fixture(autouse=True)
def fresh_cache(monkeypatch):
    monkeypatch.setattr(m, "_ENCODE_CACHE", {"failed": False, "calls": 0, "verified": 0, "fragments": {}})
    monkeypatch.delenv(m.MARKET_STATE_ENCODE_SWITCH, raising=False)


def _record(offset, instrument_id, *, kind=MarketChannel.QUOTE, bid=None, ask=None, price=None, quality="GOOD",
            reasons=(), conditions=()):
    at = NOW + timedelta(milliseconds=offset)
    quote = kind is MarketChannel.QUOTE
    return NormalizedJournalRecord(
        event_offset=offset, raw_offset=offset, event_index=0, normalizer_version="fixture-v1",
        journal_generation=GENERATION, stream_session_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        instrument_id=instrument_id, provider_symbol=SYMBOLS[instrument_id], underlying=SYMBOLS[instrument_id][2:5],
        asset_class=AssetClass.OPTION, shard_id={1: 0, 2: 0, 3: 1}[instrument_id], event_kind=kind,
        source_session=date(2026, 11, 12), source_sequence=offset, source_timestamp=1_000 + offset,
        source_timestamp_unit="ms", event_at=at,
        price=price if not quote else None, size=Decimal("3") if not quote else None,
        bid=(bid if bid is not None else Decimal("1.00") + Decimal(offset) / 100) if quote else None,
        ask=(ask if ask is not None else Decimal("1.10") + Decimal(offset) / 100) if quote else None,
        bid_size=10 if quote else None, ask_size=11 if quote else None, exchange=None if quote else 4,
        conditions=tuple(conditions), quality=quality if quote else "UNCLASSIFIED",
        reason_codes=tuple(reasons) if quote else ("CONDITION_POLICY_PENDING",),
        details_json='{"ask_exchange":2,"bid_exchange":1}' if quote else "{}",
        payload_sha256=f"{offset:064x}", received_at=at + timedelta(milliseconds=2),
        committed_at=at + timedelta(milliseconds=3),
    )


def _map():
    return m.MaterializerShardMap("advanced-options-test-v1", 2, ((1, 0), (2, 0), (3, 1)))


def _passes():
    """Twelve passes of quotes and trades on three contracts in two shards, with Decimal text variants, an invalid
    quote with reasons and trades with conditions."""
    batches, offset = [], 0
    for index in range(12):
        batch = []
        for _ in range(3 + index % 4):
            offset += 1
            instrument = 1 + offset % 3
            if offset % 5 == 0:
                batch.append(_record(offset, instrument, kind=MarketChannel.TRADE, price=Decimal("1.05"),
                                     conditions=(208,)))
            elif offset % 7 == 0:
                batch.append(_record(offset, instrument, bid=Decimal("1.50"), ask=Decimal("1.5"),
                                     quality="BAD", reasons=("CROSSED_QUOTE",)))
            else:
                batch.append(_record(offset, instrument))
        batches.append(tuple(batch))
    return batches


def _encode(state, mode, monkeypatch):
    monkeypatch.setenv(m.MARKET_STATE_ENCODE_SWITCH, mode)
    return m.encode_materializer_state(state)


def test_cached_encode_equals_full_on_every_pass_and_checkpoint(monkeypatch):
    # The self-check is off here so every compared payload is the assembled one, never the full fallback.
    monkeypatch.setattr(m, "_ENCODE_VERIFY_FIRST", 0)
    monkeypatch.setattr(m, "_ENCODE_VERIFY_EVERY", 10 ** 9)
    shard_map = _map()
    states = [m.initial_materializer_state(shard_map, shard) for shard in (0, 1)]
    for batch in _passes():
        states = [m.materialize_shard_batch(state, shard_map, batch) for state in states]
        for state in states:
            full = _encode(state, "FULL", monkeypatch)
            assert _encode(state, "CACHED", monkeypatch) == full
            assert _encode(state, "cached", monkeypatch) == full  # warm, and the switch ignores case
            checkpoints = []
            for mode in ("FULL", "CACHED"):
                monkeypatch.setenv(m.MARKET_STATE_ENCODE_SWITCH, mode)
                checkpoints.append(m.build_materializer_checkpoint(
                    state, consumer_name="market-state-materializer-v1", cohort_id=COHORT,
                    journal_generation=GENERATION, owner_epoch=1, committed_at=NOW))
            assert checkpoints[0] == checkpoints[1]
            assert m.restore_materializer_checkpoint(checkpoints[1], shard_map) == state
    assert any(state.trades for state in states) and any(state.quote_history for state in states)
    assert m._ENCODE_CACHE["failed"] is False and m._ENCODE_CACHE["calls"] == 72 and m._ENCODE_CACHE["verified"] == 0


def test_self_check_runs_on_the_first_calls_and_every_nth(monkeypatch):
    monkeypatch.setattr(m, "_ENCODE_VERIFY_FIRST", 3)
    monkeypatch.setattr(m, "_ENCODE_VERIFY_EVERY", 5)
    shard_map = _map()
    state = m.materialize_shard_batch(m.initial_materializer_state(shard_map, 0), shard_map, (_record(1, 1),))
    for _ in range(12):
        _encode(state, "CACHED", monkeypatch)
    assert m._ENCODE_CACHE["calls"] == 12 and m._ENCODE_CACHE["verified"] == 3 + 2  # calls 1-3, 5 and 10


def test_cached_encode_encodes_only_new_quotes_and_trades(monkeypatch):
    monkeypatch.setattr(m, "_ENCODE_VERIFY_FIRST", 0)
    monkeypatch.setattr(m, "_ENCODE_VERIFY_EVERY", 10 ** 9)
    shard_map = _map()
    state = m.materialize_shard_batch(m.initial_materializer_state(shard_map, 0), shard_map,
                                      tuple(_record(offset, 1 + offset % 2) for offset in range(1, 9)))
    _encode(state, "CACHED", monkeypatch)
    calls = []
    original = m._quote_document
    monkeypatch.setattr(m, "_quote_document", lambda quote: calls.append(quote) or original(quote))
    following = m.materialize_shard_batch(state, shard_map, (_record(9, 1),))
    payload = _encode(following, "CACHED", monkeypatch)
    assert len(calls) == 1 and calls[0].source_offset == 9
    monkeypatch.setattr(m, "_quote_document", original)
    assert payload == _encode(following, "FULL", monkeypatch)


def test_equal_values_with_different_text_are_never_shared(monkeypatch):
    # Decimal("1.5") == Decimal("1.50") and the frozen quotes compare equal, but the JSON differs: the cache key is
    # the object, so the second quote is encoded from its own value.
    shard_map = _map()
    state = m.materialize_shard_batch(m.initial_materializer_state(shard_map, 0), shard_map,
                                      (_record(1, 1, bid=Decimal("1.5"), ask=Decimal("1.6")),))
    quote = state.quotes[0]
    twin = replace(quote, bid=Decimal("1.50"), ask=Decimal("1.60"))
    assert twin == quote and twin is not quote
    _encode(state, "CACHED", monkeypatch)
    other = replace(state, quotes=(twin,), quote_history=(twin,))
    cached = _encode(other, "CACHED", monkeypatch)
    assert cached == _encode(other, "FULL", monkeypatch)
    assert b'"bid":"1.50"' in cached


def test_a_difference_writes_the_full_bytes_reports_and_turns_the_cache_off(monkeypatch, capsys):
    shard_map = _map()
    state = m.materialize_shard_batch(m.initial_materializer_state(shard_map, 0), shard_map,
                                      tuple(_record(offset, 1) for offset in range(1, 4)))
    full = _encode(state, "FULL", monkeypatch)
    monkeypatch.setattr(m, "_encode_state_fragments", lambda state: b"{}")
    assert _encode(state, "CACHED", monkeypatch) == full
    assert m._ENCODE_CACHE["failed"] is True
    assert "MARKET_STATE_ENCODE_MISMATCH" in capsys.readouterr().err

    def refuse(state):
        raise AssertionError("the cache must stay off after a difference")

    monkeypatch.setattr(m, "_encode_state_fragments", refuse)
    assert _encode(state, "CACHED", monkeypatch) == full


def test_default_mode_encodes_as_before_and_keeps_no_cache(monkeypatch):
    shard_map = _map()
    state = m.materialize_shard_batch(m.initial_materializer_state(shard_map, 0), shard_map, (_record(1, 1),))
    assert m.market_state_encode_mode() == "FULL"
    m.encode_materializer_state(state)
    monkeypatch.setenv(m.MARKET_STATE_ENCODE_SWITCH, "ON")
    assert m.market_state_encode_mode() == "FULL"
    m.encode_materializer_state(state)
    assert m._ENCODE_CACHE["fragments"] == {} and m._ENCODE_CACHE["calls"] == 0


def test_byte_limit_applies_in_cached_mode(monkeypatch):
    shard_map = _map()
    state = m.materialize_shard_batch(m.initial_materializer_state(shard_map, 0), shard_map, (_record(1, 1),))
    monkeypatch.setenv(m.MARKET_STATE_ENCODE_SWITCH, "CACHED")
    with pytest.raises(ValueError, match="byte limit"):
        m.encode_materializer_state(state, maximum_bytes=10)
