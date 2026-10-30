from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.run_legacy_equity_backtests_recovery import _apply_track_policy, _split_shards


def test_split_shards_partitions_without_loss():
    shards = _split_shards([str(i) for i in range(10)], 3)
    assert [len(item) for item in shards] == [3, 3, 3, 1]
    assert [value for shard in shards for value in shard] == [str(i) for i in range(10)]


def test_apply_track_policy_retrospective_sets_visible_at_from_bar_end():
    bundle = {
        "bars": [
            {
                "bar_end": "2026-08-07T14:00:00+00:00",
                "execution_scale_observed_at": "2026-09-01T00:00:00+00:00",
                "execution_scale_created_at": "2026-09-01T00:00:00+00:00",
            }
        ]
    }
    config = {"provider_delay_minutes": 15}
    adjusted = _apply_track_policy(bundle, config, "RETROSPECTIVE_RECONSTRUCTED_RESEARCH")
    assert adjusted["bars"][0]["visible_at"] == "2026-08-07T14:15:00+00:00"
    assert adjusted["bars"][0]["execution_scale_observed_at"] == "2026-08-07T14:15:00+00:00"
