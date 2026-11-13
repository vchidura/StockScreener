"""V3-1.6c (Nov 12): the market-state persist step's full checkpoint round-trip check, sampled behind a switch.

Default (FULL) decodes and compares on every persist, as before. SAMPLED decodes on the first 20 persists and every
200th after; a mismatch on a checked persist still raises; the cheap lineage-boundary check runs every time."""
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import realtime.materialization_repository as repo

_spec = importlib.util.spec_from_file_location(
    "materialization_repository_fixtures", Path(__file__).with_name("test_materialization_repository.py")
)
_fixtures = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixtures)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setitem(repo._PERSIST_CHECK, "calls", 0)
    monkeypatch.setitem(repo._PERSIST_CHECK, "checked", 0)
    monkeypatch.delenv(repo.MARKET_STATE_PERSIST_CHECK_SWITCH, raising=False)


def _persist(state, checkpoint, shard_map):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (str(_fixtures.OWNER), checkpoint.owner_epoch, True),
        None,
        (str(checkpoint.checkpoint_id),),
    )
    repo.persist_quote_materialization_cursor(
        cursor, provider="massive", lease_resource_key="materializer:quote:0", owner_id=_fixtures.OWNER,
        checkpoint=checkpoint, state=state, shard_map=shard_map, dirty_instrument_ids=frozenset({1}),
        batch_executor=_fixtures._batch_executor,
    )
    return cursor


def _counting_restore(monkeypatch):
    calls = []
    original = repo.restore_materializer_checkpoint

    def counting(checkpoint, shard_map):
        calls.append(1)
        return original(checkpoint, shard_map)

    monkeypatch.setattr(repo, "restore_materializer_checkpoint", counting)
    return calls


def test_mode_defaults_to_full_and_only_sampled_turns_it_down():
    assert repo.market_state_persist_check_mode({}) == "FULL"
    assert repo.market_state_persist_check_mode({repo.MARKET_STATE_PERSIST_CHECK_SWITCH: "sampled"}) == "SAMPLED"
    assert repo.market_state_persist_check_mode({repo.MARKET_STATE_PERSIST_CHECK_SWITCH: "off"}) == "FULL"


def test_full_checks_every_persist(monkeypatch):
    shard_map, state, checkpoint = _fixtures._state_and_checkpoint()
    calls = _counting_restore(monkeypatch)
    for _ in range(25):
        _persist(state, checkpoint, shard_map)
    assert len(calls) == 25


def test_sampled_checks_the_first_twenty_and_every_two_hundredth(monkeypatch):
    monkeypatch.setenv(repo.MARKET_STATE_PERSIST_CHECK_SWITCH, "SAMPLED")
    shard_map, state, checkpoint = _fixtures._state_and_checkpoint()
    calls = _counting_restore(monkeypatch)
    for _ in range(400):
        cursor = _persist(state, checkpoint, shard_map)
    assert len(calls) == 20 + 2  # persists 1-20, 200 and 400
    assert repo._PERSIST_CHECK["checked"] == 22
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("INSERT INTO rt.consumer_checkpoints" in statement for statement in statements)


def test_sampled_mismatch_on_a_checked_persist_still_raises(monkeypatch):
    monkeypatch.setenv(repo.MARKET_STATE_PERSIST_CHECK_SWITCH, "SAMPLED")
    shard_map, state, checkpoint = _fixtures._state_and_checkpoint()
    _, other_state, _ = _fixtures._state_and_checkpoint(epoch=3)
    monkeypatch.setattr(repo, "restore_materializer_checkpoint", lambda checkpoint, shard_map: "not the state")
    with pytest.raises(ValueError, match="checkpoint payload does not match materializer state"):
        _persist(state, checkpoint, shard_map)


def test_sampled_still_enforces_the_lineage_boundary(monkeypatch):
    monkeypatch.setenv(repo.MARKET_STATE_PERSIST_CHECK_SWITCH, "SAMPLED")
    monkeypatch.setitem(repo._PERSIST_CHECK, "calls", 100)  # past the first 20, not a 200th
    shard_map, state, checkpoint = _fixtures._state_and_checkpoint()
    beyond = checkpoint.__class__(**{**{f: getattr(checkpoint, f) for f in checkpoint.__dataclass_fields__},
                                     "committed_offset": 0})
    with pytest.raises(ValueError):
        _persist(state, beyond, shard_map)
