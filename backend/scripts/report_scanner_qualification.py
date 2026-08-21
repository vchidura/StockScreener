#!/usr/bin/env python3
"""Print aggregate scanner qualification results from persisted outcomes."""
from __future__ import annotations

import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pandas as pd

from research.scanner_events import qualification_report

rows = qualification_report()
report = pd.DataFrame(rows)
if report.empty:
    raise SystemExit("No qualification outcomes")
columns = [
    "scanner_name", "scanner_version", "interval", "direction", "horizon_bars",
    "events", "independent_periods", "mean_net_alpha", "alpha_t_stat",
    "early_alpha", "late_alpha", "hit_rate", "qualification_status",
    "alpha_fdr_q", "evidence_status",
    "calibration_status", "calibration_oos_periods",
    "calibrated_win_probability", "calibrated_win_probability_ci_low",
    "calibrated_win_probability_ci_high", "brier_score",
    "brier_skill_score_vs_50", "expected_calibration_error",
    "live_expected_alpha", "live_expected_alpha_ci_low",
    "live_expected_alpha_ci_high",
]
for column in (
    "mean_net_alpha", "early_alpha", "late_alpha", "hit_rate",
    "calibrated_win_probability", "expected_calibration_error",
    "calibrated_win_probability_ci_low", "calibrated_win_probability_ci_high",
    "live_expected_alpha", "live_expected_alpha_ci_low",
    "live_expected_alpha_ci_high",
):
    report[column] = report[column] * 100
print(report[columns].to_string(index=False, float_format=lambda value: f"{value:.3f}"))
print("status_counts", report["qualification_status"].value_counts().to_dict())
print("evidence_counts", report["evidence_status"].value_counts().to_dict())
print("calibration_counts", report["calibration_status"].value_counts().to_dict())
