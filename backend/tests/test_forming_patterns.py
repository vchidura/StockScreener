import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.forming_patterns import (
    _classify_convergence,
    _edge_metrics,
    _forming_convergences,
    _forming_cup_handle,
    _forming_flags,
    _forming_head_shoulders,
    _forming_triples,
    detect_forming_patterns,
    summarize_cross_frame_patterns,
)


def frame_from_close(close: list[float]) -> pd.DataFrame:
    values = np.asarray(close, dtype=float)
    return pd.DataFrame(
        {
            "open": values - 0.2,
            "high": values + 0.7,
            "low": values - 0.7,
            "close": values,
            "volume": np.full(len(values), 1000.0),
        },
        index=pd.date_range("2026-01-01", periods=len(values), freq="D", tz="UTC"),
    )


def pivot(index: int, price: float, pivot_type: str) -> dict:
    return {"index": index, "price": price, "type": pivot_type}


def pattern_record(pattern_type: str, bias: str) -> dict:
    return {
        "type": pattern_type,
        "name": pattern_type.replace("_", " ").title(),
        "bias": bias,
        "readiness": "NEAR_EDGE",
        "grade": "VALID_GEOMETRY",
        "edge_distance_atr": 0.5,
        "upper_touches": 2,
        "lower_touches": 2,
    }


class FormingPatternTests(unittest.TestCase):
    def test_finalized_worker_frame_keeps_the_newest_bar(self):
        source = frame_from_close(list(np.linspace(90, 100, 30)))

        with patch(
            "research.forming_patterns._confirmed_pivots", return_value=[]
        ) as pivots:
            detect_forming_patterns(
                source,
                input_includes_forming_bar=False,
            )

        completed = pivots.call_args.args[0]
        self.assertEqual(completed.index[-1], source.index[-1])

    def test_cross_frame_summary_counts_one_directional_vote_per_interval(self):
        rows = [
            {"interval": "1d", "pattern": {**pattern_record("RISING_WEDGE", "BEARISH"), "readiness": "FORMING"}},
            {"interval": "1d", "pattern": {**pattern_record("TRIPLE_TOP", "BEARISH"), "readiness": "AT_EDGE"}},
            {"interval": "30m", "pattern": pattern_record("BEAR_FLAG", "BEARISH")},
        ]

        summary = summarize_cross_frame_patterns(rows)

        self.assertEqual(summary["state"], "ALIGNED_BEARISH")
        self.assertEqual(summary["directional_frames"], 2)
        daily = next(frame for frame in summary["frames"] if frame["interval"] == "1d")
        self.assertEqual(daily["pattern_count"], 2)
        self.assertEqual(daily["primary_pattern_type"], "TRIPLE_TOP")

    def test_cross_frame_summary_distinguishes_countertrend_from_mixed(self):
        countertrend = summarize_cross_frame_patterns([
            {"interval": "1d", "pattern": pattern_record("FALLING_WEDGE", "BULLISH")},
            {"interval": "15m", "pattern": pattern_record("RISING_WEDGE", "BEARISH")},
            {"interval": "5m", "pattern": pattern_record("BEAR_FLAG", "BEARISH")},
        ])
        mixed = summarize_cross_frame_patterns([
            {"interval": "1wk", "pattern": pattern_record("FALLING_WEDGE", "BULLISH")},
            {"interval": "1d", "pattern": pattern_record("RISING_WEDGE", "BEARISH")},
        ])

        self.assertEqual(countertrend["state"], "COUNTERTREND")
        self.assertEqual(countertrend["dominant_bias"], "BULLISH")
        self.assertEqual(mixed["state"], "MIXED")

    def test_cross_frame_summary_marks_conflicting_labels_on_one_frame_mixed(self):
        summary = summarize_cross_frame_patterns([
            {"interval": "30m", "pattern": pattern_record("FALLING_WEDGE", "BULLISH")},
            {"interval": "30m", "pattern": pattern_record("RISING_WEDGE", "BEARISH")},
            {"interval": "15m", "pattern": pattern_record("BULL_FLAG", "BULLISH")},
        ])

        self.assertEqual(summary["state"], "MIXED")
        frame = next(item for item in summary["frames"] if item["interval"] == "30m")
        self.assertEqual(frame["bias"], "MIXED")

    def test_cross_frame_summary_does_not_promote_directional_alternate_over_neutral_primary(self):
        neutral = pattern_record("SYMMETRICAL_TRIANGLE", "NEUTRAL")
        neutral["readiness"] = "AT_EDGE"
        summary = summarize_cross_frame_patterns([
            {"interval": "30m", "pattern": neutral},
            {"interval": "30m", "pattern": pattern_record("FALLING_WEDGE", "BULLISH")},
            {"interval": "15m", "pattern": pattern_record("BULL_FLAG", "BULLISH")},
        ])

        self.assertEqual(summary["state"], "SINGLE_FRAME")
        frame = next(item for item in summary["frames"] if item["interval"] == "30m")
        self.assertEqual(frame["bias"], "NEUTRAL")

    def test_edge_readiness_uses_atr_normalized_distance(self):
        at_edge = _edge_metrics(100.0, 100.4, 2.0, "resistance")
        near_edge = _edge_metrics(100.0, 101.0, 2.0, "resistance")
        forming = _edge_metrics(100.0, 102.0, 2.0, "resistance")

        self.assertEqual(at_edge["readiness"], "AT_EDGE")
        self.assertEqual(near_edge["readiness"], "NEAR_EDGE")
        self.assertEqual(forming["readiness"], "FORMING")
        self.assertEqual(near_edge["edge_distance_atr"], 0.5)
        self.assertEqual(near_edge["edge_distance_pct"], 1.0)

    def test_classifies_triangle_and_wedge_slopes(self):
        cases = [
            ((0.0, 0.08), "ASCENDING_TRIANGLE"),
            ((-0.08, 0.0), "DESCENDING_TRIANGLE"),
            ((-0.06, 0.06), "SYMMETRICAL_TRIANGLE"),
            ((0.03, 0.08), "RISING_WEDGE"),
            ((-0.08, -0.03), "FALLING_WEDGE"),
        ]
        for slopes, expected in cases:
            with self.subTest(slopes=slopes):
                self.assertEqual(_classify_convergence(*slopes), expected)

    def test_detects_forming_head_and_shoulders_before_neckline_break(self):
        frame = frame_from_close([
            95, 98, 101, 104, 105, 102, 99, 96, 95, 98,
            103, 108, 112, 108, 103, 98, 96, 99, 102, 105,
            103, 100, 98, 97, 98, 99, 100, 101, 102, 103,
        ])
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(4, 105.7, "high"), pivot(8, 94.3, "low"),
            pivot(12, 112.7, "high"), pivot(16, 95.3, "low"),
            pivot(19, 105.7, "high"),
        ]

        patterns = _forming_head_shoulders(frame, atr, pivots)

        self.assertEqual(patterns[0]["type"], "HEAD_AND_SHOULDERS")
        self.assertEqual(patterns[0]["status"], "FORMING")
        self.assertEqual([line["role"] for line in patterns[0]["lines"]], ["neckline", "structure"])

    def test_fits_forming_ascending_triangle_boundaries(self):
        close = list(np.linspace(94.5, 98.7, 30))
        frame = frame_from_close(close)
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(5, 100.0, "high"), pivot(9, 90.0, "low"),
            pivot(13, 100.2, "high"), pivot(17, 93.0, "low"),
            pivot(21, 100.1, "high"), pivot(25, 96.0, "low"),
        ]

        patterns = _forming_convergences(frame, atr, pivots)

        pattern = next(item for item in patterns if item["type"] == "ASCENDING_TRIANGLE")
        self.assertGreaterEqual(pattern["upper_touches"], 2)
        self.assertGreaterEqual(pattern["lower_touches"], 2)
        self.assertGreater(pattern["contraction_pct"], 20)
        self.assertGreater(pattern["apex_bars_ahead"], 0)
        self.assertIsNone(pattern["flagpole_atr"])
        self.assertEqual([line["role"] for line in pattern["lines"]], ["resistance", "support"])

    def test_short_convergence_after_flagpole_is_classified_as_pennant(self):
        close = list(np.linspace(78, 96, 16)) + list(np.linspace(97, 97.3, 12))
        frame = frame_from_close(close)
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(16, 101.0, "high"), pivot(18, 93.0, "low"),
            pivot(20, 99.5, "high"), pivot(22, 95.0, "low"),
            pivot(24, 98.5, "high"), pivot(26, 96.0, "low"),
        ]

        patterns = _forming_convergences(frame, atr, pivots)

        pattern = next(item for item in patterns if item["type"] == "BULL_PENNANT")
        self.assertEqual(pattern["bias"], "BULLISH")
        self.assertGreaterEqual(pattern["flagpole_atr"], 3)
        self.assertEqual(_forming_flags(frame, atr, pivots), [])

    def test_parallel_pullback_after_impulse_is_bull_flag_not_pennant(self):
        close = list(np.linspace(80, 100, 12)) + [
            100, 98.5, 96.5, 98, 98.5, 96, 94.5,
            96, 96.5, 94, 92.5, 93, 92.5, 92,
        ]
        frame = frame_from_close(close)
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(12, 101.0, "high"), pivot(14, 96.0, "low"),
            pivot(16, 99.0, "high"), pivot(18, 94.0, "low"),
            pivot(20, 97.0, "high"), pivot(22, 92.0, "low"),
        ]

        flags = _forming_flags(frame, atr, pivots)

        pattern = next(item for item in flags if item["type"] == "BULL_FLAG")
        self.assertEqual(pattern["bias"], "BULLISH")
        self.assertGreaterEqual(pattern["flagpole_atr"], 3)
        self.assertEqual(
            [line["role"] for line in pattern["lines"]],
            ["flagpole", "resistance", "support"],
        )

    def test_broken_then_reentered_flag_stays_retired(self):
        close = list(np.linspace(80, 100, 12)) + [
            100, 98.5, 96.5, 98, 98.5, 96, 94.5,
            96, 96.5, 94, 92.5, 97, 92.5, 92,
        ]
        frame = frame_from_close(close)
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(12, 101.0, "high"), pivot(14, 96.0, "low"),
            pivot(16, 99.0, "high"), pivot(18, 94.0, "low"),
            pivot(20, 97.0, "high"), pivot(22, 92.0, "low"),
        ]

        self.assertEqual(_forming_flags(frame, atr, pivots), [])

    def test_convergence_stays_retired_after_break_and_reentry(self):
        close = list(np.linspace(94.5, 98.7, 30))
        close[27] = 102.0
        close[28] = 98.5
        close[29] = 98.7
        frame = frame_from_close(close)
        frame.iloc[27, frame.columns.get_loc("high")] = 102.7
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(5, 100.0, "high"), pivot(9, 90.0, "low"),
            pivot(13, 100.2, "high"), pivot(17, 93.0, "low"),
            pivot(21, 100.1, "high"), pivot(25, 96.0, "low"),
        ]

        self.assertEqual(_forming_convergences(frame, atr, pivots), [])

    def test_broken_then_reentered_head_and_shoulders_stays_retired(self):
        frame = frame_from_close([
            95, 98, 101, 104, 105, 102, 99, 96, 95, 98,
            103, 108, 112, 108, 103, 98, 96, 99, 102, 105,
            103, 100, 94, 98, 99, 100, 101, 102, 103, 104,
        ])
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(4, 105.7, "high"), pivot(8, 94.3, "low"),
            pivot(12, 112.7, "high"), pivot(16, 95.3, "low"),
            pivot(19, 105.7, "high"),
        ]

        self.assertEqual(_forming_head_shoulders(frame, atr, pivots), [])

    def test_head_and_shoulders_is_removed_after_neckline_break(self):
        frame = frame_from_close([
            95, 98, 101, 104, 105, 102, 99, 96, 95, 98,
            103, 108, 112, 108, 103, 98, 96, 99, 102, 105,
            103, 100, 98, 97, 94, 93, 92, 91, 90, 89,
        ])
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(4, 105.7, "high"), pivot(8, 94.3, "low"),
            pivot(12, 112.7, "high"), pivot(16, 95.3, "low"),
            pivot(19, 105.7, "high"),
        ]

        self.assertEqual(_forming_head_shoulders(frame, atr, pivots), [])

    def test_detects_cup_and_handle_geometry(self):
        frame = frame_from_close(list(np.linspace(110, 90, 20)) + list(np.linspace(91, 110, 20)) + [108, 105, 107, 108, 109])
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(0, 110.7, "high"), pivot(19, 89.3, "low"),
            pivot(39, 110.7, "high"), pivot(41, 104.3, "low"),
        ]

        patterns = _forming_cup_handle(frame, atr, pivots)

        self.assertEqual(patterns[0]["type"], "CUP_AND_HANDLE")
        self.assertEqual([line["role"] for line in patterns[0]["lines"]], ["rim", "cup", "handle"])

    def test_detects_forming_triple_top_and_bottom(self):
        frame = frame_from_close([
            95, 97, 99, 100, 99, 96, 92, 90, 92, 96,
            99, 100, 99, 96, 93, 91, 93, 97, 99, 100,
            98, 96, 95, 94, 95, 96, 97, 98, 97, 96,
        ])
        atr = pd.Series(2.0, index=frame.index)
        top_pivots = [
            pivot(3, 100.7, "high"), pivot(7, 89.3, "low"),
            pivot(11, 100.7, "high"), pivot(15, 90.3, "low"),
            pivot(19, 100.7, "high"),
        ]
        bottom_pivots = [
            pivot(3, 89.3, "low"), pivot(7, 100.7, "high"),
            pivot(11, 89.6, "low"), pivot(15, 99.7, "high"),
            pivot(19, 89.4, "low"),
        ]

        top = next(item for item in _forming_triples(frame, atr, top_pivots) if item["type"] == "TRIPLE_TOP")
        bottom = next(item for item in _forming_triples(frame, atr, bottom_pivots) if item["type"] == "TRIPLE_BOTTOM")

        self.assertEqual(top["bias"], "BEARISH")
        self.assertEqual(top["upper_touches"], 3)
        self.assertEqual(bottom["bias"], "BULLISH")
        self.assertEqual(bottom["lower_touches"], 3)

    def test_broken_then_reentered_triple_top_stays_retired(self):
        close = [
            95, 97, 99, 100, 99, 96, 92, 90, 92, 96,
            99, 100, 99, 96, 93, 91, 93, 97, 99, 100,
            98, 96, 88, 94, 95, 96, 97, 98, 97, 96,
        ]
        frame = frame_from_close(close)
        atr = pd.Series(2.0, index=frame.index)
        pivots = [
            pivot(3, 100.7, "high"), pivot(7, 89.3, "low"),
            pivot(11, 100.7, "high"), pivot(15, 90.3, "low"),
            pivot(19, 100.7, "high"),
        ]

        self.assertEqual(_forming_triples(frame, atr, pivots), [])

    def test_public_detector_ignores_changes_to_forming_bar(self):
        close = [100 + np.sin(index / 2) * (8 - index * 0.1) for index in range(40)]
        frame = frame_from_close(close)
        first = detect_forming_patterns(frame)
        frame.iloc[-1, frame.columns.get_loc("high")] = 1000
        frame.iloc[-1, frame.columns.get_loc("low")] = 1
        frame.iloc[-1, frame.columns.get_loc("close")] = 500

        self.assertEqual(detect_forming_patterns(frame), first)

    def test_public_detector_returns_at_most_three_unique_types(self):
        close = [100 + np.sin(index / 2) * (8 - index * 0.1) for index in range(60)]
        patterns = detect_forming_patterns(frame_from_close(close))

        self.assertLessEqual(len(patterns), 3)
        self.assertEqual(len({pattern["type"] for pattern in patterns}), len(patterns))


if __name__ == "__main__":
    unittest.main()