"""V3-1.6 (Nov 12): the market-state role hashed every shard state's canonical JSON twice per pass (plan and commit),
42% of its busy samples on Nov 11 (V3 #2 profile). `REALTIME_V3_MARKET_STATE_HASH=CHECKPOINT` derives the slice
identity from the checkpoints instead; states, checkpoints and payloads must be the same in both modes."""
from dataclasses import replace

import pytest

from realtime import advanced_options_consumers as consumers
from realtime.advanced_options_consumers import AdvancedOptionsMarketStateConsumer
from realtime.materialization import restore_materializer_checkpoint
from tests.realtime.test_advanced_options_consumers import NOW, _shard_map, _source


def _consumer(control, source, shard_map, **kwargs):
    return AdvancedOptionsMarketStateConsumer(
        source=source, cohort_id=control.cohort_id, shard_map=shard_map, owner_epoch=1, clock=lambda: NOW, **kwargs,
    )


def _run(control, source, shard_map, *, step):
    """Plan and commit the whole journal in slices of `step` records; returns every slice and the consumer."""
    consumer, slices = _consumer(control, source, shard_map), []
    while consumer.committed_offset < source.last_event_offset():
        planned = consumer.plan_slice(maximum_records=step)
        consumer.commit_slice(planned)
        slices.append(planned)
    return consumer, slices


def test_default_mode_is_canonical(monkeypatch):
    monkeypatch.delenv(consumers.MARKET_STATE_HASH_SWITCH, raising=False)
    assert consumers.market_state_hash_mode() == "CANONICAL"
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, " checkpoint ")
    assert consumers.market_state_hash_mode() == "CHECKPOINT"
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "anything")
    assert consumers.market_state_hash_mode() == "CANONICAL"


@pytest.mark.parametrize("step", [1, 10])
def test_both_modes_give_identical_states_checkpoints_and_payloads(tmp_path, monkeypatch, step):
    control, source = _source(tmp_path)
    shard_map = _shard_map()
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "CANONICAL")
    canonical, canonical_slices = _run(control, source, shard_map, step=step)
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "CHECKPOINT")
    checkpoint, checkpoint_slices = _run(control, source, shard_map, step=step)

    assert checkpoint.states == canonical.states
    assert checkpoint.committed_offset == canonical.committed_offset == source.last_event_offset()
    assert [s.checkpoints for s in checkpoint_slices] == [s.checkpoints for s in canonical_slices]
    assert [c.state_payload for s in checkpoint_slices for c in s.checkpoints] == [
        c.state_payload for s in canonical_slices for c in s.checkpoints]
    # Only the in-memory slice identity differs (it is never stored).
    assert all(a.state_sha256 != b.state_sha256 for a, b in zip(checkpoint_slices, canonical_slices))


def test_checkpoint_mode_restart_matches_uninterrupted(tmp_path, monkeypatch):
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "CHECKPOINT")
    control, source = _source(tmp_path)
    shard_map = _shard_map()
    uninterrupted, slices = _run(control, source, shard_map, step=10)
    first = _consumer(control, source, shard_map)
    first_slice = first.plan_slice(maximum_records=1)
    first.commit_slice(first_slice)
    restarted = _consumer(control, source, shard_map, states=tuple(
        restore_materializer_checkpoint(checkpoint, shard_map) for checkpoint in first_slice.checkpoints))
    second = restarted.plan_slice(maximum_records=10)
    restarted.commit_slice(second)
    assert restarted.states == uninterrupted.states
    assert second.state_sha256 == slices[-1].state_sha256


def test_checkpoint_mode_refuses_a_slice_whose_checkpoints_changed(tmp_path, monkeypatch):
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "CHECKPOINT")
    control, source = _source(tmp_path)
    consumer = _consumer(control, source, _shard_map())
    planned = consumer.plan_slice(maximum_records=10)
    changed = replace(planned, checkpoints=tuple(
        replace(checkpoint, state_hash="0" * 64) for checkpoint in planned.checkpoints))
    with pytest.raises(ValueError, match="identity changed before commit"):
        consumer.commit_slice(changed)
    consumer.commit_slice(planned)
    assert consumer.committed_offset == planned.end_offset


def test_a_slice_planned_in_one_mode_is_refused_in_the_other(tmp_path, monkeypatch):
    """Switching modes mid-pass cannot commit a slice under the wrong identity: the next pass replans it."""
    control, source = _source(tmp_path)
    consumer = _consumer(control, source, _shard_map())
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "CANONICAL")
    planned = consumer.plan_slice(maximum_records=10)
    monkeypatch.setenv(consumers.MARKET_STATE_HASH_SWITCH, "CHECKPOINT")
    with pytest.raises(ValueError, match="identity changed before commit"):
        consumer.commit_slice(planned)
    consumer.commit_slice(consumer.plan_slice(maximum_records=10))
    assert consumer.committed_offset == source.last_event_offset()
