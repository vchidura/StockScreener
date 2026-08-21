import sys
import unittest
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.scanner_calibration import walk_forward_calibration


class ScannerCalibrationTests(unittest.TestCase):
    def test_walk_forward_probabilities_use_only_prior_periods(self):
        returns = pd.Series([
            0.01 if index % 5 != 0 else -0.01
            for index in range(200)
        ])
        alpha = returns + pd.Series([
            (index % 3) * 0.0001 for index in range(200)
        ])

        result = walk_forward_calibration(returns, alpha)

        self.assertEqual(result["calibration_oos_periods"], 160)
        self.assertGreater(result["calibrated_win_probability"], 0.75)
        self.assertLess(result["brier_score"], 0.25)
        self.assertGreater(result["brier_skill_score_vs_50"], 0)
        self.assertEqual(
            sum(point["count"] for point in result["calibration_curve"]), 160
        )
        self.assertGreater(result["live_expected_alpha"], 0)

    def test_insufficient_history_does_not_emit_probability(self):
        result = walk_forward_calibration(
            pd.Series([0.01] * 40), pd.Series([0.01] * 40)
        )

        self.assertEqual(result["calibration_oos_periods"], 0)
        self.assertIsNone(result["calibrated_win_probability"])
        self.assertEqual(result["calibration_curve"], [])


if __name__ == "__main__":
    unittest.main()