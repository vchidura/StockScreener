import unittest
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from screeners import (
    _describe_active_fibonacci_leg,
    _describe_confirmed_fibonacci_leg,
    _find_zigzag_pivots,
    calculate_fibonacci_swing_pct,
    scan_fibonacci,
)


class FibonacciSwingThresholdTests(unittest.TestCase):
    def test_scales_and_respects_interval_bounds(self):
        closes = np.linspace(100.0, 110.0, 60)
        frame = pd.DataFrame({
            "high": closes * 1.01,
            "low": closes * 0.99,
            "close": closes,
        })

        daily_threshold = calculate_fibonacci_swing_pct(frame, "1d")
        monthly_threshold = calculate_fibonacci_swing_pct(frame, "1mo")
        five_minute_threshold = calculate_fibonacci_swing_pct(frame, "5m")

        self.assertGreaterEqual(daily_threshold, 4.9)
        self.assertLessEqual(daily_threshold, 5.1)
        self.assertEqual(monthly_threshold, 8.0)
        self.assertEqual(five_minute_threshold, 3.0)

    def test_scanner_levels_reproduce_from_returned_swing(self):
        closes = np.concatenate([
            np.linspace(100.0, 120.0, 20),
            np.linspace(119.0, 102.0, 20),
            np.linspace(103.0, 115.0, 20),
        ])
        frame = pd.DataFrame({
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
        }, index=pd.date_range("2026-01-02", periods=len(closes), freq="B"))

        result = scan_fibonacci("TEST", frame, min_swing_pct=5.0)
        pivots = _find_zigzag_pivots(
            frame["high"].values,
            frame["low"].values,
            frame["close"].values,
            min_swing_pct=5.0,
        )

        self.assertIsNotNone(result)
        self.assertGreaterEqual(len(pivots), 3)
        confirmed_start, confirmed_end = pivots[-3], pivots[-2]
        developing = pivots[-1]
        swing_range = result["swing_high"] - result["swing_low"]
        expected_size = round(swing_range / result["swing_low"] * 100, 2)
        ratios = [0.236, 0.382, 0.500, 0.618, 0.786]
        if result["trend_direction"] == "uptrend_retracement":
            expected_levels = [
                round(result["swing_high"] - swing_range * ratio, 2)
                for ratio in ratios
            ]
        else:
            expected_levels = [
                round(result["swing_low"] + swing_range * ratio, 2)
                for ratio in ratios
            ]
        actual_levels = [
            result["fib_236"], result["fib_382"], result["fib_500"],
            result["fib_618"], result["fib_786"],
        ]
        expected_nearest = min(
            expected_levels, key=lambda level: abs(result["last_close"] - level)
        )
        expected_distance = round(
            (result["last_close"] - expected_nearest) / expected_nearest * 100, 2
        )

        self.assertEqual(result["swing_detection_pct"], 5.0)
        self.assertEqual(result["swing_basis"], "structural_confirmed_leg")
        self.assertEqual(
            {result["swing_low"], result["swing_high"]},
            {round(confirmed_start[1], 2), round(confirmed_end[1], 2)},
        )
        self.assertEqual(result["developing_pivot"]["type"], developing[2])
        self.assertEqual(result["developing_pivot"]["price"], round(developing[1], 2))
        active_leg = result["active_leg"]
        self.assertEqual(active_leg["status"], "provisional")
        self.assertEqual(active_leg["start"]["type"], confirmed_end[2])
        self.assertEqual(active_leg["start"]["price"], round(confirmed_end[1], 2))
        self.assertEqual(active_leg["end"]["type"], developing[2])
        self.assertEqual(active_leg["end"]["price"], round(developing[1], 2))
        self.assertNotEqual(active_leg["trend_direction"], result["trend_direction"])
        active_range = active_leg["end"]["price"] - active_leg["start"]["price"]
        if active_leg["trend_direction"] == "uptrend_retracement":
            expected_active_levels = [
                round(active_leg["end"]["price"] - active_range * ratio, 2)
                for ratio in ratios
            ]
            self.assertEqual(active_leg["level_role"], "provisional_support")
        else:
            active_range = abs(active_range)
            expected_active_levels = [
                round(active_leg["end"]["price"] + active_range * ratio, 2)
                for ratio in ratios
            ]
            self.assertEqual(active_leg["level_role"], "provisional_resistance")
        self.assertEqual(
            [level["price"] for level in active_leg["levels"]],
            expected_active_levels,
        )
        self.assertEqual(active_leg["current_state"]["id"], "unconfirmed_range")
        self.assertEqual(
            [scenario["id"] for scenario in active_leg["scenarios"]],
            [
                "continuation", "confirmation", "unconfirmed_range",
                "support_hold" if active_leg["trend_direction"] == "uptrend_retracement"
                    else "resistance_hold",
                "failure",
            ],
        )
        confirmation = active_leg["scenarios"][1]
        self.assertEqual(
            confirmation["trigger_price"], active_leg["confirmation"]["price"]
        )
        self.assertEqual(
            [level["price"] for level in confirmation["levels"]],
            expected_active_levels,
        )
        failure = active_leg["scenarios"][-1]
        self.assertEqual(failure["trigger_price"], active_leg["start"]["price"])
        self.assertEqual(
            [level["name"] for level in failure["levels"]],
            ["127.2%", "138.2%", "161.8%", "200%", "261.8%"],
        )
        self.assertEqual(result["swing_size_pct"], expected_size)
        self.assertEqual(actual_levels, expected_levels)
        self.assertEqual(result["nearest_level_price"], expected_nearest)
        self.assertEqual(result["distance_pct"], expected_distance)

    def test_confirmed_history_falls_back_when_latest_leg_is_invalidated(self):
        frame = pd.DataFrame(
            {
                "high": [101.0, 121.0, 111.0, 126.0],
                "low": [99.0, 119.0, 89.0, 124.0],
                "close": [100.0, 120.0, 90.0, 125.0],
            },
            index=pd.date_range("2026-01-02", periods=4, freq="B"),
        )
        up_leg = _describe_confirmed_fibonacci_leg(
            (0, 100.0, "low"), (1, 120.0, "high"), frame, 125.0
        )
        down_leg = _describe_confirmed_fibonacci_leg(
            (1, 120.0, "high"), (2, 90.0, "low"), frame, 125.0
        )

        self.assertEqual(up_leg["status"], "invalidated")
        self.assertEqual(up_leg["invalidation"]["date"], "2026-01-06")
        self.assertEqual(down_leg["status"], "invalidated")
        self.assertEqual(down_leg["invalidation"]["date"], "2026-01-07")

    def test_active_down_leg_scenarios_invert_conditions_and_levels(self):
        frame = pd.DataFrame(
            {"high": [121.0, 106.0], "low": [119.0, 99.0], "close": [120.0, 104.0]},
            index=pd.date_range("2026-01-02", periods=2, freq="B"),
        )

        active_leg = _describe_active_fibonacci_leg(
            (0, 120.0, "high"), (1, 100.0, "low"), frame, 104.0, 5.0
        )
        scenarios = {scenario["id"]: scenario for scenario in active_leg["scenarios"]}

        self.assertEqual(active_leg["level_role"], "provisional_resistance")
        self.assertEqual(scenarios["continuation"]["condition"], "below")
        self.assertEqual(scenarios["continuation"]["trigger_price"], 100.0)
        self.assertEqual(scenarios["confirmation"]["condition"], "at_or_above")
        self.assertEqual(scenarios["confirmation"]["trigger_price"], 105.0)
        self.assertEqual(scenarios["unconfirmed_range"]["lower_price"], 100.0)
        self.assertEqual(scenarios["unconfirmed_range"]["upper_price"], 105.0)
        self.assertEqual(scenarios["failure"]["condition"], "at_or_above")
        self.assertEqual(scenarios["failure"]["trigger_price"], 120.0)
        self.assertTrue(all(
            level["price"] > 120.0 for level in scenarios["failure"]["levels"]
        ))

    def test_active_leg_uses_displayed_cent_anchors_for_reproducible_levels(self):
        frame = pd.DataFrame(
            {"high": [350.0, 514.0], "low": [349.0, 500.0], "close": [349.2, 495.4]},
            index=pd.date_range("2026-01-02", periods=2, freq="B"),
        )

        active_leg = _describe_active_fibonacci_leg(
            (0, 349.204, "low"), (1, 513.725, "high"), frame, 495.4, 7.22
        )

        self.assertEqual(active_leg["start"]["price"], 349.20)
        self.assertEqual(active_leg["end"]["price"], 513.73)
        self.assertEqual(active_leg["confirmation"]["price"], 476.64)
        self.assertEqual(
            [level["price"] for level in active_leg["levels"]],
            [474.90, 450.88, 431.47, 412.05, 384.41],
        )


if __name__ == "__main__":
    unittest.main()