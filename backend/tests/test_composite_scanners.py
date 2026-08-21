import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.composite_scanners import (
    build_composite_scanner_events,
    _compression_breakout_events,
    _failed_breakout_reversal_events,
    _point_in_time_vwap,
    _pullback_metadata,
)


def _failed_breakout_frame() -> pd.DataFrame:
    frame = pd.DataFrame({
        "ticker": ["TEST"] * 4,
        "date": pd.date_range("2026-01-05", periods=4, freq="B"),
        "open": [99.0, 99.4, 99.9, 100.3],
        "high": [99.8, 100.0, 100.5, 100.35],
        "low": [98.8, 99.2, 99.7, 99.55],
        "close": [99.4, 99.8, 100.25, 99.7],
        "volume": [100.0, 100.0, 140.0, 130.0],
        "atr_ref": [1.0] * 4,
        "volume_ratio": [1.0, 1.0, 1.4, 1.3],
        "range_ratio": [1.0, 0.8, 0.8, 0.8],
        "close_location": [0.6, 0.75, 0.69, 0.19],
        "last_high": [100.0] * 4,
        "last_high_index": [0] * 4,
        "last_low": [98.0] * 4,
        "last_low_index": [0] * 4,
    })
    return frame


class FailedBreakoutReversalTests(unittest.TestCase):
    def test_fresh_upside_break_then_next_bar_failure_emits_short(self):
        events = _failed_breakout_reversal_events(_failed_breakout_frame())

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["direction"], -1)
        self.assertEqual(events[0]["reference_level"], 100.0)
        metadata = json.loads(events[0]["metadata"])
        self.assertEqual(metadata["failed_side"], "upside")
        self.assertGreater(metadata["breakout_distance_atr"], 0)
        self.assertTrue(metadata["follow_through_failed"])
        self.assertEqual(metadata["follow_through_excursion_atr"], 0)

    def test_failure_must_close_back_inside_level(self):
        frame = _failed_breakout_frame()
        frame.loc[3, "close"] = 100.1
        frame.loc[3, "close_location"] = 0.3

        self.assertEqual(_failed_breakout_reversal_events(frame), [])

    def test_extended_breakout_is_not_low_follow_through(self):
        frame = _failed_breakout_frame()
        frame.loc[3, "high"] = 100.75

        events = _failed_breakout_reversal_events(frame)

        self.assertEqual(len(events), 1)
        metadata = json.loads(events[0]["metadata"])
        self.assertFalse(metadata["follow_through_failed"])
        self.assertAlmostEqual(metadata["follow_through_excursion_atr"], 0.25)


def _compression_frame() -> pd.DataFrame:
    rows = []
    for index, date in enumerate(pd.date_range("2026-01-05", periods=12, freq="B")):
        is_breakout = index == 11
        rows.append({
            "ticker": "TEST",
            "date": date,
            "open": 100.0 if not is_breakout else 100.1,
            "high": 100.3 if not is_breakout else 100.8,
            "low": 99.7 if not is_breakout else 99.5,
            "close": 100.05 if not is_breakout else 100.7,
            "volume": 100.0 if not is_breakout else 150.0,
            "atr_ref": 2.0 if index <= 1 else 1.0,
            "volume_ratio": 1.0 if not is_breakout else 1.5,
            "range_ratio": 0.6 if not is_breakout else 1.3,
            "close_location": 0.58 if not is_breakout else 0.92,
            "sma20": 100.0,
            "sma50": 99.0,
        })
    return pd.DataFrame(rows)


class CompressionBreakoutTests(unittest.TestCase):
    def test_contracting_channel_then_expansion_close_emits_long(self):
        events = _compression_breakout_events(_compression_frame())

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["direction"], 1)
        metadata = json.loads(events[0]["metadata"])
        self.assertLessEqual(metadata["compression_mean_range_atr"], 0.75)
        self.assertLessEqual(metadata["atr_contraction_ratio"], 0.9)

    def test_flat_atr_does_not_count_as_compression(self):
        frame = _compression_frame()
        frame["atr_ref"] = 1.0

        self.assertEqual(_compression_breakout_events(frame), [])

    def test_candidate_detectors_run_once_per_ticker(self):
        panel = _compression_frame()

        with patch(
            "research.composite_scanners._compression_breakout_events",
            return_value=[],
        ) as compression, patch(
            "research.composite_scanners._failed_breakout_reversal_events",
            return_value=[],
        ) as failed_breakout:
            build_composite_scanner_events(panel)

        self.assertEqual(compression.call_count, 1)
        self.assertEqual(failed_breakout.call_count, 1)


class PullbackMetadataTests(unittest.TestCase):
    def test_shape_and_volume_use_only_pivot_to_trigger_window(self):
        frame = pd.DataFrame({
            "date": pd.date_range("2026-01-05", periods=4, freq="B"),
            "open": [100.0, 102.0, 101.0, 100.0],
            "high": [101.0, 103.0, 102.0, 101.5],
            "low": [99.0, 101.0, 100.0, 99.5],
            "close": [100.5, 102.5, 100.5, 101.0],
            "volume": [200.0, 200.0, 100.0, 150.0],
            "atr_ref": [2.0] * 4,
            "last_high": [103.0] * 4,
            "last_high_index": [1] * 4,
            "last_low": [99.0] * 4,
            "last_low_index": [0] * 4,
            "prior_high": [99.5] * 4,
            "prior_low": [98.0] * 4,
            "sma20": [100.0] * 4,
            "sma50": [98.0] * 4,
            "vwap_ref": [100.0, 101.5, 101.0, 100.5],
            "vwap_basis": ["rolling_20_bar"] * 4,
            "close_location": [0.75, 0.75, 0.25, 0.75],
        })

        metadata = _pullback_metadata(frame, frame.iloc[3], 1)

        self.assertEqual(metadata["pullback_bars"], 2)
        self.assertAlmostEqual(metadata["pullback_depth_atr"], 1.75)
        self.assertAlmostEqual(metadata["pullback_volume_ratio"], 0.5)
        self.assertAlmostEqual(metadata["trigger_volume_ratio"], 1.5)
        self.assertTrue(metadata["vwap_reclaim"])
        self.assertAlmostEqual(metadata["overnight_gap_atr"], -0.25)
        self.assertGreaterEqual(metadata["level_cluster_count"], 2)

    def test_intraday_vwap_resets_at_each_session(self):
        frame = pd.DataFrame({
            "date": pd.to_datetime([
                "2026-01-05 09:30", "2026-01-05 10:30",
                "2026-01-06 09:30", "2026-01-06 10:30",
            ]),
            "high": [101.0, 103.0, 111.0, 113.0],
            "low": [99.0, 101.0, 109.0, 111.0],
            "close": [100.0, 102.0, 110.0, 112.0],
            "volume": [100.0, 100.0, 100.0, 100.0],
        })

        vwap, basis = _point_in_time_vwap(frame)

        self.assertEqual(basis, "session")
        self.assertAlmostEqual(vwap.iloc[0], 100.0)
        self.assertAlmostEqual(vwap.iloc[1], 101.0)
        self.assertAlmostEqual(vwap.iloc[2], 110.0)


if __name__ == "__main__":
    unittest.main()