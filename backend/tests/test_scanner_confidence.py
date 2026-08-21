import sys
import unittest
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.scanner_confidence import (
    _benjamini_hochberg,
    confidence_slices,
    expand_confidence_slices,
    summarize_confidence_slices,
)


class ScannerConfidenceTests(unittest.TestCase):
    def test_benjamini_hochberg_adjustment_is_monotonic(self):
        adjusted = _benjamini_hochberg(pd.Series([0.001, 0.02, 0.04]))

        self.assertAlmostEqual(adjusted.iloc[0], 0.003)
        self.assertAlmostEqual(adjusted.iloc[1], 0.03)
        self.assertAlmostEqual(adjusted.iloc[2], 0.04)

    def test_slice_must_improve_over_same_date_baseline(self):
        dates = pd.date_range("2026-01-05", periods=45, freq="B")
        rows = []
        for index, date in enumerate(dates):
            for ticker, alpha, aligned in (
                ("A1", 0.018 + (index % 3) * 0.001, True),
                ("A2", 0.019 + (index % 3) * 0.001, True),
                ("A3", 0.020 + (index % 3) * 0.001, True),
                ("B1", -0.011 + (index % 2) * 0.001, False),
                ("B2", -0.010 + (index % 2) * 0.001, False),
                ("B3", -0.009 + (index % 2) * 0.001, False),
            ):
                rows.append({
                    "scanner_name": "breakout_expansion",
                    "scanner_version": "1.0",
                    "interval": "1d",
                    "direction": 1,
                    "trigger_type": "swing_breakout",
                    "discovery_state": "CONTINUATION" if aligned else "NEUTRAL",
                    "metadata": {},
                    "horizon_bars": 1,
                    "signal_time": date.to_pydatetime(),
                    "trade_date": date.date(),
                    "ticker": ticker,
                    "net_return": alpha,
                    "net_alpha": alpha,
                })
        observations = pd.DataFrame(rows)
        observations["trade_date"] = pd.to_datetime(observations["trade_date"])
        calendar = {date.date(): index for index, date in enumerate(dates)}

        report = summarize_confidence_slices(observations, {"1d": calendar})
        aligned = report[report["slice_name"] == "discovery_aligned"].iloc[0]

        self.assertEqual(aligned["independent_periods"], 45)
        self.assertGreater(aligned["mean_net_alpha"], 0.018)
        self.assertGreater(aligned["mean_incremental_alpha"], 0.014)
        self.assertEqual(aligned["status"], "CONFIDENCE_PASS")
        self.assertEqual(aligned["robustness_status"], "ROBUST_PASS")

    def test_primary_baseline_uses_absolute_fdr_without_incremental_test(self):
        dates = pd.date_range("2026-01-05", periods=45, freq="B")
        observations = pd.DataFrame([{
            "scanner_name": "candidate",
            "scanner_version": "0.1-shadow",
            "interval": "1d",
            "direction": 1,
            "trigger_type": "candidate",
            "discovery_state": "NEUTRAL",
            "metadata": {},
            "horizon_bars": 1,
            "signal_time": date.to_pydatetime(),
            "trade_date": date,
            "ticker": ticker,
            "net_return": 0.01 + (index % 2) * 0.001,
            "net_alpha": 0.01 + (index % 2) * 0.001,
        } for index, date in enumerate(dates) for ticker in ("A", "B", "C")])
        calendar = {date.date(): index for index, date in enumerate(dates)}

        report = summarize_confidence_slices(observations, {"1d": calendar})
        baseline = report[report["slice_name"] == "baseline"].iloc[0]

        self.assertEqual(baseline["status"], "CONFIDENCE_PASS")
        self.assertLessEqual(baseline["absolute_fdr_q"], 0.05)
        self.assertTrue(pd.isna(baseline["incremental_fdr_q"]))
        self.assertEqual(baseline["robustness_status"], "ROBUST_PASS")

    def test_breakout_quality_and_discovery_alignment_are_predeclared(self):
        row = pd.Series({
            "scanner_name": "breakout_expansion",
            "direction": 1,
            "trigger_type": "swing_breakout",
            "discovery_state": "CONTINUATION",
            "xs_age_days": 1,
            "xs_side": "LONG",
            "xs_percentile": 0.95,
            "market_breadth": 0.7,
            "sector_breadth": 0.8,
            "market_volatility_percentile": 0.75,
            "metadata": {
                "volume_ratio": 1.7,
                "range_ratio": 1.8,
                "close_location": 0.9,
            },
        })

        slices = confidence_slices(row)

        self.assertIn("baseline", slices)
        self.assertIn("discovery_aligned", slices)
        self.assertIn("rank_actionable_aligned", slices)
        self.assertIn("rank_quintile_aligned", slices)
        self.assertIn("market_breadth_aligned", slices)
        self.assertIn("sector_breadth_aligned", slices)
        self.assertIn("market_sector_breadth_aligned", slices)
        self.assertIn("market_high_volatility", slices)
        self.assertIn("participation_both_1_50", slices)
        self.assertIn("breakout_extreme_close", slices)
        self.assertIn("breakout_high_quality", slices)

    def test_level_source_and_trigger_are_classified_without_duplicate_votes(self):
        row = pd.Series({
            "scanner_name": "level_retest_rejection",
            "direction": -1,
            "trigger_type": "fib_0.618:bearish_strong_close",
            "discovery_state": "NEUTRAL",
            "xs_age_days": 1,
            "xs_side": "LONG",
            "xs_percentile": 0.9,
            "metadata": {
                "level_source": "fib_0.618",
                "volume_ratio": 1.1,
                "range_ratio": 1.2,
            },
        })

        slices = confidence_slices(row)

        self.assertIn("level_fibonacci", slices)
        self.assertIn("trigger_bearish_strong_close", slices)
        self.assertNotIn("discovery_aligned", slices)
        self.assertNotIn("rank_actionable_aligned", slices)
        self.assertEqual(len(slices), len(set(slices)))

    def test_pullback_shape_and_volume_filters_are_predeclared(self):
        row = pd.Series({
            "scanner_name": "structured_trend_pullback",
            "direction": 1,
            "trigger_type": "bullish_strong_close",
            "discovery_state": "NEUTRAL",
            "metadata": {
                "pullback_bars": 4,
                "pullback_volume_ratio": 0.8,
                "trigger_volume_ratio": 1.3,
                "pullback_speed_atr_per_bar": 0.4,
                "swing_origin_distance_atr": 1.2,
                "vwap_reclaim": True,
                "pivot_age_bars": 12,
                "overnight_gap_atr": 0.1,
                "level_cluster_count": 2,
            },
        })

        slices = confidence_slices(row)

        self.assertIn("pullback_2_5_bars", slices)
        self.assertIn("pullback_volume_contraction", slices)
        self.assertIn("pullback_contract_then_expand", slices)
        self.assertIn("pullback_orderly_speed", slices)
        self.assertIn("pullback_near_swing_origin", slices)
        self.assertIn("pullback_vwap_reclaim", slices)
        self.assertIn("pivot_age_10_plus", slices)
        self.assertIn("small_overnight_gap", slices)
        self.assertIn("level_clustered", slices)

    def test_failed_breakout_filters_use_level_freshness_and_participation(self):
        row = pd.Series({
            "scanner_name": "failed_breakout_reversal",
            "direction": -1,
            "trigger_type": "upside_failure:close_back_inside",
            "discovery_state": "NEUTRAL",
            "metadata": {
                "prior_level_tests": 1,
                "breakout_volume_ratio": 1.4,
                "follow_through_failed": True,
            },
        })

        slices = confidence_slices(row)

        self.assertIn("failed_breakout_fresh_level", slices)
        self.assertIn("failed_breakout_participation", slices)
        self.assertIn("failed_breakout_low_follow_through", slices)

    def test_compression_quality_filters_are_predeclared(self):
        row = pd.Series({
            "scanner_name": "compression_breakout",
            "direction": 1,
            "trigger_type": "compressed_channel:expansion_close",
            "discovery_state": "NEUTRAL",
            "metadata": {
                "compression_band_atr": 1.8,
                "atr_contraction_ratio": 0.7,
                "trend_aligned": True,
            },
        })

        slices = confidence_slices(row)

        self.assertIn("compression_tight_band", slices)
        self.assertIn("compression_deep_atr_contraction", slices)
        self.assertIn("compression_trend_aligned", slices)

    def test_vectorized_expansion_matches_better_feature_slices(self):
        observations = pd.DataFrame([{
            "scanner_name": "structured_trend_pullback",
            "direction": 1,
            "trigger_type": "bullish_strong_close",
            "discovery_state": "NEUTRAL",
            "metadata": {
                "pullback_speed_atr_per_bar": 0.4,
                "swing_origin_distance_atr": 1.2,
                "vwap_reclaim": True,
                "pivot_age_bars": 12,
                "overnight_gap_atr": 0.1,
                "level_cluster_count": 2,
            },
        }])

        slices = set(expand_confidence_slices(observations)["slice_name"])

        self.assertTrue({
            "pullback_orderly_speed", "pullback_near_swing_origin",
            "pullback_vwap_reclaim", "pivot_age_10_plus",
            "small_overnight_gap", "level_clustered",
        }.issubset(slices))


if __name__ == "__main__":
    unittest.main()