from scripts.benchmark_realtime_checkpoint import checkpoint_bytes, checkpoint_fixture
from realtime.materialization import (
    MAX_CHECKPOINT_STATE_BYTES,
    restore_materializer_checkpoint,
)


def test_checkpoint_capacity_grows_with_history_and_is_deterministic():
    shallow = checkpoint_bytes(8, 1)
    repeated = checkpoint_bytes(8, 1)
    deeper = checkpoint_bytes(8, 8)

    assert shallow == repeated
    assert deeper > shallow > 0


def test_target_shard_checkpoint_fits_at_provisional_history_depth_only():
    assert checkpoint_bytes(125, 8) <= MAX_CHECKPOINT_STATE_BYTES
    assert checkpoint_bytes(125, 16) > MAX_CHECKPOINT_STATE_BYTES


def test_target_shard_benchmark_checkpoint_restores_exact_state():
    shard_map, state, checkpoint = checkpoint_fixture(125, 8)

    assert restore_materializer_checkpoint(checkpoint, shard_map) == state