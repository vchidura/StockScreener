import sys
import unittest
from unittest.mock import patch
import json
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

    def test_delayed_labels_and_equal_time_labels_are_not_training_data(self):
        times = pd.Series(pd.date_range("2026-09-01 14:30Z", periods=5, freq="30min"))
        returns = pd.Series([0.01, -0.01, 0.01, -0.01, 0.01])
        availability = times + pd.Timedelta(hours=1)

        result = walk_forward_calibration(
            returns, returns, min_train_periods=1,
            prediction_times=times, outcome_available_at=availability,
        )

        self.assertEqual(result["calibration_oos_periods"], 2)
        expected = (((11 / 21) ** 2) + ((11 / 22 - 1) ** 2)) / 2
        self.assertAlmostEqual(result["brier_score"], expected)

    def test_unavailable_prior_outcome_cannot_change_later_forecasts(self):
        times = pd.Series(pd.date_range("2026-09-01 14:30Z", periods=5, freq="30min"))
        returns = pd.Series([0.01, -0.01, 0.01, -0.01, 0.01])
        availability = times + pd.Timedelta(minutes=31)
        availability.iloc[0] = times.iloc[-1] + pd.Timedelta(hours=1)
        arguments = {
            "min_train_periods": 1,
            "prediction_times": times,
            "outcome_available_at": availability,
        }
        first = walk_forward_calibration(returns, returns, **arguments)
        changed = returns.copy()
        changed.iloc[0] = -0.01
        second = walk_forward_calibration(changed, changed, **arguments)

        self.assertEqual(first["calibration_oos_periods"], 2)
        self.assertEqual(first["brier_score"], second["brier_score"])
        self.assertEqual(first["calibration_curve"], second["calibration_curve"])

    def test_row_order_does_not_define_timestamped_training_windows(self):
        times = pd.Series(pd.date_range("2026-09-01", periods=100, tz="UTC"))
        returns = pd.Series([0.01 if index % 3 else -0.01 for index in range(100)])
        availability = times + pd.Timedelta(days=3)
        first = walk_forward_calibration(
            returns, returns, prediction_times=times, outcome_available_at=availability,
        )
        order = returns.sample(frac=1, random_state=7).index
        second = walk_forward_calibration(
            returns.loc[order], returns.loc[order],
            prediction_times=times.loc[order], outcome_available_at=availability.loc[order],
        )

        self.assertEqual(first["calibration_oos_periods"], 57)
        self.assertEqual(first["calibration_oos_periods"], second["calibration_oos_periods"])
        self.assertAlmostEqual(first["brier_score"], second["brier_score"])

    def test_no_known_training_labels_suppresses_calibration(self):
        times = pd.Series(pd.date_range("2026-09-01", periods=100, tz="UTC"))
        returns = pd.Series([0.01] * 100)
        result = walk_forward_calibration(
            returns, returns, prediction_times=times,
            outcome_available_at=times + pd.Timedelta(days=200),
        )

        self.assertEqual(result["calibration_oos_periods"], 0)
        self.assertIsNone(result["calibrated_win_probability"])

    def test_timestamp_contract_rejects_missing_naive_or_misaligned_values(self):
        returns = pd.Series([0.01, -0.01])
        times = pd.Series(pd.date_range("2026-09-01", periods=2, tz="UTC"))
        availability = times + pd.Timedelta(days=1)
        invalid_arguments = [
            {"prediction_times": times},
            {"outcome_available_at": availability},
            {"prediction_times": times.dt.tz_localize(None), "outcome_available_at": availability},
            {"prediction_times": times, "outcome_available_at": pd.Series([pd.NaT, availability.iloc[1]])},
            {"prediction_times": times, "outcome_available_at": times},
            {"prediction_times": times.set_axis([1, 2]), "outcome_available_at": availability},
        ]
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                walk_forward_calibration(returns, returns, **arguments)

    def test_calibration_curve_omits_unused_categorical_bins(self):
        returns = pd.Series([
            0.01 if index % 3 else -0.01 for index in range(100)
        ])
        categories = pd.Categorical(
            ["populated"] * 60,
            categories=["populated", "unused"],
        )

        with patch("research.scanner_calibration.pd.qcut", return_value=categories):
            result = walk_forward_calibration(returns, returns)

        self.assertEqual(len(result["calibration_curve"]), 1)
        self.assertEqual(result["calibration_curve"][0]["count"], 60)
        json.dumps(result, allow_nan=False)


if __name__ == "__main__":
    unittest.main()