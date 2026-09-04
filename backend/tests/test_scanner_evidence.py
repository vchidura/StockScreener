import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.scanner_research import (
    _qualification_row,
    latest_ticker_signals,
    qualification_report,
    recent_events,
)


class ScannerEvidenceTests(unittest.TestCase):
    def test_durable_gap_qualification_keeps_its_published_fdr_state(self):
        row = _qualification_row({
            "source_name": "GAP_BREAKAWAY_HOLD",
            "source_version": "gap_formation_v2",
            "interval": "1d",
            "direction": 1,
            "horizon_bars": 5,
            "outcome_policy_key": (
                "GAP_BREAKAWAY_HOLD:gap_formation_v2:1d:SIGNED:SECTOR_PRIMARY"
            ),
            "sample_size": 559,
            "independent_periods": 52,
            "mean_net_alpha": -0.012,
            "alpha_t_stat": -2.21,
            "alpha_fdr_q": 0.16,
            "qualification_state": "UNRANKED",
            "calibrated_probability": None,
            "probability_ci_low": None,
            "probability_ci_high": None,
            "brier_score": None,
            "brier_skill_score": None,
            "expected_calibration_error": None,
            "metrics": {
                "alpha_p_value": 0.027,
                "early_alpha": -0.017,
                "hit_rate": 0.47,
                "late_alpha": -0.007,
                "mean_mae_pct": -0.05,
                "mean_mfe_pct": 0.04,
                "mean_net_return": -0.006,
                "raw_pass": False,
                "stop_hit_rate": 0.18,
                "target_hit_rate": 0.04,
            },
        })

        self.assertEqual(row["scanner_name"], "gap_breakaway_hold")
        self.assertEqual(row["return_mode"], "DIRECTIONAL_HORIZON")
        self.assertEqual(row["events"], 559)
        self.assertEqual(row["independent_periods"], 52)
        self.assertEqual(row["alpha_fdr_q"], 0.16)
        self.assertEqual(row["evidence_status"], "UNRANKED")
        self.assertEqual(row["mean_net_return"], -0.006)
        self.assertEqual(row["hit_rate"], 0.47)
        self.assertEqual(row["mean_mae_pct"], -0.05)
        self.assertEqual(row["mean_mfe_pct"], 0.04)
        self.assertEqual(row["stop_hit_rate"], 0.18)
        self.assertEqual(row["target_hit_rate"], 0.04)

    def test_recommendation_plan_qualification_exposes_its_return_mode(self):
        row = _qualification_row({
            "source_name": "breakout_expansion",
            "source_version": "1.0",
            "interval": "1d",
            "direction": 1,
            "horizon_bars": 5,
            "outcome_policy_key": (
                "breakout_expansion:1.0:1d:RECOMMENDATION_PLAN:SECTOR_PRIMARY"
            ),
            "sample_size": 120,
            "independent_periods": 40,
            "mean_net_alpha": 0.01,
            "alpha_t_stat": 2.1,
            "alpha_fdr_q": 0.04,
            "qualification_state": "ROBUST_PASS",
            "calibrated_probability": None,
            "probability_ci_low": None,
            "probability_ci_high": None,
            "brier_score": None,
            "brier_skill_score": None,
            "expected_calibration_error": None,
            "metrics": {},
        })

        self.assertEqual(row["return_mode"], "RECOMMENDATION_PLAN")

    def test_qualification_reads_only_sector_primary_policies(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        manager = MagicMock()
        manager.__enter__.return_value = cursor

        with patch("equity.scanner_research.get_db_cursor", return_value=manager):
            qualification_report()

        sql = cursor.execute.call_args.args[0]
        self.assertIn("benchmark_policy->>'primary' = 'SECTOR'", sql)

    def test_signal_lists_require_an_actionable_trigger(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        manager = MagicMock()
        manager.__enter__.return_value = cursor

        with patch("equity.scanner_research.get_db_cursor", return_value=manager):
            latest_ticker_signals()
            latest_sql = cursor.execute.call_args.args[0]
            recent_events()
            recent_sql = cursor.execute.call_args.args[0]

        self.assertIn("evidence.payload ? 'trigger_type'", latest_sql)
        self.assertIn("evidence.payload ? 'trigger_type'", recent_sql)


if __name__ == "__main__":
    unittest.main()