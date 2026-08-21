import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.scanner_events import _apply_qualification_fdr


class ScannerEvidenceTests(unittest.TestCase):
    def test_raw_primary_pass_requires_fdr_to_be_robust(self):
        rows = [
            {
                "scanner_name": "strong",
                "qualification_status": "PRIMARY_PASS",
                "alpha_p_value": 0.001,
                "calibration_oos_periods": 120,
                "calibrated_win_probability": 0.60,
                "brier_score": 0.20,
                "expected_calibration_error": 0.04,
            },
            {
                "scanner_name": "weak",
                "qualification_status": "PRIMARY_PASS",
                "alpha_p_value": 0.04,
                "calibration_oos_periods": 120,
                "calibrated_win_probability": 0.60,
                "brier_score": 0.20,
                "expected_calibration_error": 0.04,
            },
            {
                "scanner_name": "failed",
                "qualification_status": "NOT_QUALIFIED",
                "alpha_p_value": 0.8,
            },
        ]

        report = {row["scanner_name"]: row for row in _apply_qualification_fdr(rows)}

        self.assertEqual(report["strong"]["evidence_status"], "ROBUST_PASS")
        self.assertEqual(
            report["strong"]["calibration_status"], "RESEARCH_CALIBRATED"
        )
        self.assertEqual(report["strong"]["calibrated_win_probability"], 0.60)
        self.assertEqual(report["weak"]["evidence_status"], "MONITOR_ONLY")
        self.assertEqual(report["weak"]["calibration_status"], "NOT_ELIGIBLE")
        self.assertIsNone(report["weak"]["calibrated_win_probability"])
        self.assertEqual(report["failed"]["evidence_status"], "UNRANKED")


if __name__ == "__main__":
    unittest.main()