import unittest
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.fibonacci_scoring import (
    _daily_swing_thresholds,
    evaluate_variant_outcomes,
    select_variant_candidates,
)
from screeners import calculate_fibonacci_swing_pct


class FibonacciScoringResearchTests(unittest.TestCase):
    def test_optimized_thresholds_match_production_prefix_calculation(self):
        closes = np.linspace(80.0, 140.0, 90) + np.sin(np.arange(90)) * 4
        frame = pd.DataFrame({
            "high": closes * (1.01 + np.arange(90) % 4 / 1000),
            "low": closes * (0.99 - np.arange(90) % 3 / 1000),
            "close": closes,
        })
        optimized = _daily_swing_thresholds(frame)

        for position in (49, 50, 63, 79, 89):
            expected = calculate_fibonacci_swing_pct(
                frame.iloc[:position + 1], "1d"
            )
            self.assertEqual(float(optimized.iloc[position]), expected)

    def test_variants_select_latest_valid_and_cap_multi_leg_vote(self):
        pairs = [
            ((0, 70.0, "low"), (1, 100.0, "high")),
            ((2, 60.0, "low"), (3, 100.0, "high")),
            ((4, 110.0, "high"), (5, 80.0, "low")),
        ]
        legs = [
            {"status": "valid"},
            {"status": "valid"},
            {"status": "invalidated"},
        ]

        selected = select_variant_candidates(pairs, legs, 92.0, proximity_pct=2.0)

        self.assertEqual(selected["legacy_latest_completed"]["direction"], -1)
        self.assertEqual(selected["latest_valid_primary"]["direction"], 1)
        self.assertEqual(selected["multi_leg_capped"]["direction"], 1)
        self.assertEqual(selected["multi_leg_capped"]["leg_count"], 2)

        conflicting_legs = [{"status": "valid"}] * 3
        conflicted = select_variant_candidates(
            pairs, conflicting_legs, 92.0, proximity_pct=2.0
        )
        self.assertNotIn("multi_leg_capped", conflicted)

    def test_outcomes_enter_at_next_open_and_exit_at_horizon_close(self):
        panel = pd.DataFrame({
            "ticker": ["TEST"] * 4,
            "date": pd.date_range("2026-01-05", periods=4, freq="B"),
            "open": [10.0, 11.0, 11.5, 12.0],
            "high": [10.5, 12.0, 12.5, 12.5],
            "low": [9.5, 10.5, 11.0, 11.5],
            "close": [10.0, 11.5, 12.0, 12.2],
        })
        signals = pd.DataFrame([{
            "variant": "latest_valid_primary",
            "ticker": "TEST",
            "date": panel.iloc[0]["date"],
            "position": 0,
            "direction": 1,
            "level": "38.2%",
            "level_price": 10.0,
            "distance_pct": 0.0,
            "leg_count": 1,
            "swing_detection_pct": 5.0,
        }])

        result = evaluate_variant_outcomes(
            panel, signals, horizons=(2,), round_trip_cost_bps=0
        ).iloc[0]

        self.assertEqual(result["entry_price"], 11.0)
        self.assertEqual(result["exit_price"], 12.0)
        self.assertAlmostEqual(result["net_return"], 12.0 / 11.0 - 1)
        self.assertAlmostEqual(result["net_alpha"], 0.0)


if __name__ == "__main__":
    unittest.main()