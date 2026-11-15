"""V3-1.4 / R81 item 1 (Nov 15): market-state single pass.

The V3 market-state role builds one consumer per shard, and each materializes the whole shared batch for itself, so
N shards scan every record N times. `REALTIME_V3_MARKET_STATE_SINGLE_PASS=ON` computes every shard's next state with
one `materialize_shard_fanout` pass when the process owns every shard at one committed offset; the per-shard slices
(states, checkpoints, payloads, dirty sets, identities) must be identical to today's per-shard path."""
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from realtime import advanced_options_consumers as consumers
from realtime.advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
    market_state_single_pass_mode,
    plan_market_state_single_pass,
)
from realtime.advanced_options_ingress import AdvancedOptionsJournalSource
from realtime.contracts import MarketChannel
from realtime.materialization import MaterializerShardMap
from tests.realtime.test_advanced_options_consumers import NOW, _normalized_record, _source

INSTRUMENTS = (101, 102, 103, 104, 105, 106)


def _map(shard_count):
    return MaterializerShardMap(
        "advanced-options-market-state-v1",
        shard_count,
        tuple((instrument, index % shard_count) for index, instrument in enumerate(INSTRUMENTS)),
    )


def _records(shard_map, count=90):
    rows = []
    for offset in range(1, count + 1):
        instrument = INSTRUMENTS[(offset * 7) % len(INSTRUMENTS)]
        # Event times go back and forth so quote/trade replacement and history trimming are exercised.
        event_at = NOW + timedelta(milliseconds=(offset * 37) % 500 + offset)
        kind = MarketChannel.TRADE if offset % 4 == 0 else MarketChannel.QUOTE
        record = _normalized_record(
            offset, f"O:SPY{instrument}", kind, event_at,
            size=Decimal(offset % 5 + 1) if kind is MarketChannel.TRADE else None,
        )
        rows.append(replace(
            record,
            instrument_id=instrument,
            shard_id=shard_map.shard_for(instrument),
            bid=Decimal("1.00") + Decimal(offset % 4) / 100,
            ask=Decimal("1.10") + Decimal(offset % 3) / 100,
        ))
    return tuple(rows)


def _consumers(tmp_path, shard_map):
    control, source = _source(tmp_path)
    registration = replace(
        source.registration,
        shard_ids=tuple(range(shard_map.shard_count)),
        shard_map_sha256=shard_map.sha256,
    )
    source = AdvancedOptionsJournalSource(
        root=source.path.parent, registration=registration, source_flow=control.source_flow,
    )
    return {
        shard_id: AdvancedOptionsMarketStateConsumer(
            source=source, cohort_id=control.cohort_id, shard_map=shard_map, owner_epoch=1,
            owned_shard_ids=(shard_id,), clock=lambda: NOW,
        )
        for shard_id in range(shard_map.shard_count)
    }


def _slice_bytes(planned):
    return (
        planned.start_offset, planned.end_offset, planned.record_count, planned.states,
        tuple((c.shard_id, c.committed_offset, c.state_hash, c.state_payload) for c in planned.checkpoints),
        planned.dirty_instrument_ids, planned.state_sha256,
    )


def _run(tmp_path, shard_map, records, *, step, single_pass):
    """Drive the role's per-pass logic (shared read of `step` records) to the end; returns each pass's slices."""
    owned = _consumers(tmp_path, shard_map)
    passes = []
    while owned[0].committed_offset < records[-1].event_offset:
        offset = min(consumer.committed_offset for consumer in owned.values())
        shared = tuple(record for record in records if record.event_offset > offset)[:step]
        single, status = (
            plan_market_state_single_pass(owned, shard_map, shared) if single_pass else (None, None)
        )
        if single_pass:
            assert status == consumers.MARKET_STATE_SINGLE_PASS_APPLIED
        slices = []
        for shard_id, consumer in owned.items():
            if single is not None:
                planned = consumer.plan_slice(
                    maximum_records=10_000, records=single.records, next_states=(single.states[shard_id],),
                )
            else:
                planned = consumer.plan_slice(
                    maximum_records=10_000,
                    records=tuple(r for r in shared if r.event_offset > consumer.committed_offset),
                )
            consumer.commit_slice(planned)
            slices.append(_slice_bytes(planned))
        passes.append(tuple(slices))
    return owned, passes


def test_switch_values(monkeypatch):
    assert market_state_single_pass_mode({}) == "OFF"
    assert market_state_single_pass_mode(None) == "OFF"
    for value in ("", " off ", "OFF"):
        assert market_state_single_pass_mode({consumers.MARKET_STATE_SINGLE_PASS_SWITCH: value}) == "OFF"
    assert market_state_single_pass_mode({consumers.MARKET_STATE_SINGLE_PASS_SWITCH: " on "}) == "ON"
    with pytest.raises(ValueError, match=consumers.MARKET_STATE_SINGLE_PASS_SWITCH):
        market_state_single_pass_mode({consumers.MARKET_STATE_SINGLE_PASS_SWITCH: "yes"})


@pytest.mark.parametrize("shard_count", [1, 3])
@pytest.mark.parametrize("step", [1, 7, 90])
@pytest.mark.parametrize("hash_mode", ["CANONICAL", "CHECKPOINT"])
def test_single_pass_equals_per_shard_path(tmp_path, monkeypatch, shard_count, step, hash_mode):
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, hash_mode)
    shard_map = _map(shard_count)
    records = _records(shard_map)
    per_shard, per_shard_passes = _run(tmp_path / "a", shard_map, records, step=step, single_pass=False)
    single, single_passes = _run(tmp_path / "b", shard_map, records, step=step, single_pass=True)
    assert single_passes == per_shard_passes
    assert len(single_passes) == -(-len(records) // step)
    for shard_id in range(shard_count):
        assert single[shard_id].states == per_shard[shard_id].states
        assert single[shard_id].committed_offset == records[-1].event_offset
    if shard_count == 3:
        # every shard holds its own instruments only
        for shard_id in range(3):
            held = {quote.instrument_id for quote in single[shard_id].states[0].quotes}
            assert held and all(shard_map.shard_for(instrument) == shard_id for instrument in held)


def test_fallback_reasons(tmp_path):
    shard_map = _map(3)
    records = _records(shard_map, count=20)
    owned = _consumers(tmp_path, shard_map)
    assert plan_market_state_single_pass(owned, shard_map, None) == (None, "NO_SHARED_READ")
    assert plan_market_state_single_pass(owned, shard_map, ()) == (None, "IDLE")
    partial = {shard_id: owned[shard_id] for shard_id in (0, 2)}
    assert plan_market_state_single_pass(partial, shard_map, records) == (None, "NOT_ALL_SHARDS_OWNED")
    # shard 1 commits ahead of the others: offsets differ
    ahead = owned[1].plan_slice(maximum_records=10_000, records=records[:5])
    owned[1].commit_slice(ahead)
    assert plan_market_state_single_pass(owned, shard_map, records) == (None, "COMMITTED_OFFSETS_DIFFER")
    # a record whose shard does not match the map makes the fan-out fail: the per-shard path reports it per shard
    fresh = _consumers(tmp_path / "fresh", shard_map)
    broken = records[:3] + (replace(records[3], shard_id=(records[3].shard_id + 1) % 3),) + records[4:]
    single, status = plan_market_state_single_pass(fresh, shard_map, broken)
    assert single is None and status == "FANOUT_VALUEERROR"


def test_consumer_refuses_precomputed_states_that_do_not_match(tmp_path):
    shard_map = _map(3)
    records = _records(shard_map, count=12)
    owned = _consumers(tmp_path, shard_map)
    single, status = plan_market_state_single_pass(owned, shard_map, records)
    assert status == "APPLIED" and single.start_offset == 0
    with pytest.raises(ValueError, match="need the shared records"):
        owned[0].plan_slice(maximum_records=10_000, next_states=(single.states[0],))
    with pytest.raises(ValueError, match="do not match this consumer"):
        owned[0].plan_slice(maximum_records=10_000, records=single.records, next_states=(single.states[1],))
    with pytest.raises(ValueError, match="do not match this consumer"):
        owned[0].plan_slice(maximum_records=10_000, records=single.records[:6], next_states=(single.states[0],))
    planned = owned[0].plan_slice(maximum_records=10_000, records=single.records, next_states=(single.states[0],))
    assert planned.states == (single.states[0],) and planned.end_offset == records[-1].event_offset


def test_role_reads_the_switch_and_keeps_the_per_shard_path():
    """The role imports the switch and the fan-out planner; OFF leaves `single` unset, so every shard plans as before."""
    import inspect

    from realtime import advanced_options_p8_roles as roles

    source = inspect.getsource(roles._run_market_state)
    assert "market_state_single_pass_mode(environment)" in source
    assert "plan_market_state_single_pass(" in source
    assert "if single_pass and v3_loop and consumers:" in source
