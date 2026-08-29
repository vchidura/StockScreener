import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.price_structures import (
    _detect_structural_patterns,
    _is_active_pattern,
    _volume_pivot_zones,
    analyze_price_structures,
)


def frame_from_close(close: list[float], volume: list[float] | None = None) -> pd.DataFrame:
    values = np.asarray(close, dtype=float)
    return pd.DataFrame(
        {
            "open": values - 0.2,
            "high": values + 0.7,
            "low": values - 0.7,
            "close": values,
            "volume": volume if volume is not None else np.full(len(values), 1000.0),
        },
        index=pd.date_range("2026-01-01", periods=len(values), freq="D"),
    )


def pivot(index: int, price: float, pivot_type: str) -> dict:
    return {"index": index, "price": price, "type": pivot_type}


class PriceStructureTests(unittest.TestCase):
    def test_public_detector_confirms_double_top_only_after_neckline_break(self):
        close = [
            90, 91, 92, 94, 97, 100, 98, 95, 92, 90,
            92, 95, 98, 100.5, 98, 95, 91, 88, 89, 90,
            91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101,
        ]
        result = analyze_price_structures(frame_from_close(close))

        pattern = next(item for item in result["patterns"] if item["type"] == "DOUBLE_TOP")
        self.assertEqual(pattern["direction"], "BEARISH")
        self.assertEqual(pattern["status"], "CONFIRMED")
        self.assertLess(pattern["target"], pattern["neckline"])

        no_break = close.copy()
        no_break[17] = 91
        no_break[18] = 91
        result = analyze_price_structures(frame_from_close(no_break))
        self.assertFalse(any(item["type"] == "DOUBLE_TOP" for item in result["patterns"]))

    def test_detects_confirmed_double_bottom(self):
        frame = frame_from_close([
            105, 104, 103, 102, 101, 100, 103, 106, 109, 110,
            108, 105, 102, 101, 100.4, 103, 107, 111, 112, 113,
            114, 115, 116, 117, 118, 119, 118, 117, 116, 115,
        ])
        atr = pd.Series(2.0, index=frame.index)
        pivots = [pivot(5, 99.3, "low"), pivot(9, 110.7, "high"), pivot(14, 99.7, "low")]

        patterns = _detect_structural_patterns(frame, atr, pivots)

        pattern = next(item for item in patterns if item["type"] == "DOUBLE_BOTTOM")
        self.assertEqual(pattern["direction"], "BULLISH")
        self.assertGreater(pattern["target"], pattern["neckline"])

    def test_detects_head_and_shoulders_and_retires_target_hit(self):
        close = [
            95, 98, 101, 104, 105, 102, 99, 96, 95, 98,
            103, 108, 112, 108, 103, 98, 96, 99, 102, 105,
            103, 99, 96, 94, 93, 92, 91, 90, 89, 88,
        ]
        frame = frame_from_close(close)
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(4, 105.7, "high"), pivot(8, 94.3, "low"),
            pivot(12, 112.7, "high"), pivot(16, 95.3, "low"),
            pivot(19, 105.7, "high"),
        ]

        patterns = _detect_structural_patterns(frame, atr, pivots)
        pattern = next(item for item in patterns if item["type"] == "HEAD_AND_SHOULDERS")
        self.assertEqual(pattern["direction"], "BEARISH")
        self.assertAlmostEqual(pattern["neckline"], 94.8)

        target_hit = frame.copy()
        target_hit.iloc[-1, target_hit.columns.get_loc("low")] = pattern["target"] - 1
        retired = _detect_structural_patterns(target_hit, atr, pivots)
        self.assertFalse(any(item["type"] == "HEAD_AND_SHOULDERS" for item in retired))

    def test_volume_pivot_zone_requires_elevated_volume_and_tags_fibonacci(self):
        frame = frame_from_close(list(np.linspace(100, 108, 35)))
        frame.iloc[25, frame.columns.get_loc("low")] = 101.0
        frame.iloc[25, frame.columns.get_loc("volume")] = 1800.0
        atr = pd.Series(2.0, index=frame.index)

        zones = _volume_pivot_zones(
            frame,
            atr,
            [pivot(25, 101.0, "low")],
            [{"name": "61.8%", "price": 101.4}],
        )

        self.assertEqual(len(zones), 1)
        self.assertEqual(zones[0]["source"], "Volume Pivot")
        self.assertEqual(zones[0]["pivot_type"], "low")
        self.assertEqual(zones[0]["volume_ratio"], 1.8)
        self.assertEqual(zones[0]["fibonacci_levels"][0]["name"], "61.8%")
        self.assertIn("Pivot volume 1.80x its prior 20-bar median (+80%)", zones[0]["qualifier"])
        self.assertIn("near Fib 61.8%", zones[0]["qualifier"])

    def test_fibonacci_overlap_tolerance_is_capped_at_one_percent(self):
        frame = frame_from_close(list(np.linspace(100, 108, 35)))
        frame.iloc[25, frame.columns.get_loc("low")] = 101.0
        frame.iloc[25, frame.columns.get_loc("volume")] = 1800.0
        atr = pd.Series(40.0, index=frame.index)

        zones = _volume_pivot_zones(
            frame,
            atr,
            [pivot(25, 101.0, "low")],
            [{"name": "61.8%", "price": 108.0}],
        )

        self.assertEqual(len(zones), 1)
        self.assertEqual(zones[0]["fibonacci_levels"], [])

    def test_nonfinite_pattern_geometry_is_rejected(self):
        frame = frame_from_close([100.0] * 30)

        self.assertFalse(
            _is_active_pattern(frame, 10, "BEARISH", float("nan"), 105.0)
        )

    def test_duplicate_fibonacci_overlaps_are_collapsed(self):
        frame = frame_from_close(list(np.linspace(100, 108, 35)))
        frame.iloc[25, frame.columns.get_loc("low")] = 101.0
        frame.iloc[25, frame.columns.get_loc("volume")] = 1800.0
        atr = pd.Series(2.0, index=frame.index)

        zones = _volume_pivot_zones(
            frame,
            atr,
            [pivot(25, 101.0, "low")],
            [
                {"name": "61.8%", "price": 101.4},
                {"name": "61.8%", "price": 101.5},
            ],
        )

        self.assertEqual(zones[0]["fibonacci_levels"], [{"name": "61.8%", "price": 101.4}])

    def test_pattern_is_retired_when_confirmation_bar_reaches_target(self):
        frame = frame_from_close([100.0] * 30)
        frame.iloc[10, frame.columns.get_loc("low")] = 90.0

        self.assertFalse(
            _is_active_pattern(frame, 10, "BEARISH", 91.0, 105.0)
        )

    def test_pattern_is_retired_when_invalidation_is_crossed(self):
        frame = frame_from_close([100.0] * 30)
        frame.iloc[12, frame.columns.get_loc("close")] = 106.0

        self.assertFalse(
            _is_active_pattern(frame, 10, "BEARISH", 90.0, 105.0)
        )

    def test_broken_volume_demand_and_supply_zones_are_retired(self):
        demand_frame = frame_from_close(list(np.linspace(100, 108, 35)))
        demand_frame.iloc[25, demand_frame.columns.get_loc("low")] = 101.0
        demand_frame.iloc[25, demand_frame.columns.get_loc("volume")] = 1800.0
        demand_frame.iloc[30, demand_frame.columns.get_loc("close")] = 100.0
        atr = pd.Series(2.0, index=demand_frame.index)

        demand_zones = _volume_pivot_zones(
            demand_frame, atr, [pivot(25, 101.0, "low")], []
        )

        supply_frame = frame_from_close(list(np.linspace(100, 108, 35)))
        supply_frame.iloc[25, supply_frame.columns.get_loc("high")] = 110.0
        supply_frame.iloc[25, supply_frame.columns.get_loc("volume")] = 1800.0
        supply_frame.iloc[30, supply_frame.columns.get_loc("close")] = 111.0
        supply_zones = _volume_pivot_zones(
            supply_frame, atr, [pivot(25, 110.0, "high")], []
        )

        self.assertEqual(demand_zones, [])
        self.assertEqual(supply_zones, [])


if __name__ == "__main__":
    unittest.main()