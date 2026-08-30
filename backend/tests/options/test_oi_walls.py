import sys
from decimal import Decimal
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import OiWallInput, detect_oi_wall_clusters


def test_zero_mad_has_no_default_wall_fallback():
    rows = tuple(OiWallInput(Decimal(index), 100) for index in range(1, 11))

    assert detect_oi_wall_clusters(rows, Decimal("5")) == ()


def test_strict_percentile_walls_cluster_only_when_adjacent_in_listed_order():
    rows = tuple(
        OiWallInput(Decimal(index), index * 10 if index <= 18 else (900 if index == 19 else 1000))
        for index in range(1, 21)
    )

    clusters = detect_oi_wall_clusters(rows, Decimal("10"))

    assert len(clusters) == 1
    cluster = clusters[0]
    assert cluster.member_strikes == (Decimal("19"), Decimal("20"))
    assert cluster.total_open_interest == 1900
    assert cluster.center_strike == Decimal("37100") / Decimal("1900")


def test_non_wall_listed_strike_splits_clusters():
    values = [10, 20, 30, 40, 50, 60, 70, 80, 900, 90, 1000]
    rows = tuple(
        OiWallInput(Decimal(index + 1), value)
        for index, value in enumerate(values)
    )

    clusters = detect_oi_wall_clusters(
        rows,
        Decimal("6"),
        percentile=0.8,
        minimum_robust_z=2.5,
    )

    assert [cluster.member_strikes for cluster in clusters] == [
        (Decimal("11"),),
        (Decimal("9"),),
    ]