from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.discovery_states import _apply_position_overlay


def _candidate(**overrides) -> dict:
    row = {
        "close": 110.0,
        "sma20": 105.0,
        "sma50": 100.0,
        "recent_21d_percentile": 0.90,
        "recent_21d_return": 0.15,
        "recent_5d_return": 0.02,
        "rsi_14": 72.0,
        "distance_sma20_atr": 1.8,
        "up_days_10": 8.0,
        "down_days_10": 2.0,
        "close_strength": 0.55,
        "higher_swing_high": True,
        "higher_swing_low": True,
        "lower_swing_high": False,
        "lower_swing_low": False,
    }
    row.update(overrides)
    return row


class PositionOverlayTests(unittest.TestCase):
    def test_extended_uptrend_is_not_a_short_signal(self):
        row = _apply_position_overlay(pd.DataFrame([_candidate()])).iloc[0]
        self.assertEqual(row["trend_state"], "UPTREND")
        self.assertEqual(row["extension_risk"], "EXTENDED")
        self.assertEqual(row["reversal_trigger"], "NONE")
        self.assertIn("avoid chasing", row["position_guidance"])

    def test_decelerating_extended_uptrend_emits_early_bearish_warning(self):
        row = _apply_position_overlay(pd.DataFrame([
            _candidate(
                recent_5d_return=-0.03,
                rsi_14=68.5,
                distance_sma20_atr=1.3,
                up_days_10=5.0,
            )
        ])).iloc[0]
        self.assertEqual(row["trend_state"], "UPTREND")
        self.assertEqual(row["extension_risk"], "EXHAUSTION_WATCH")
        self.assertEqual(row["reversal_trigger"], "BEARISH_EARLY")
        self.assertIn("require confirmation before shorting", row["position_guidance"])

    def test_downtrend_rules_are_symmetric(self):
        row = _apply_position_overlay(pd.DataFrame([
            _candidate(
                close=90.0,
                sma20=95.0,
                sma50=100.0,
                recent_21d_percentile=0.10,
                recent_21d_return=-0.15,
                recent_5d_return=0.02,
                rsi_14=28.0,
                distance_sma20_atr=-1.8,
                up_days_10=2.0,
                down_days_10=8.0,
                close_strength=0.70,
                higher_swing_high=False,
                higher_swing_low=False,
                lower_swing_high=True,
                lower_swing_low=True,
            )
        ])).iloc[0]
        self.assertEqual(row["trend_state"], "DOWNTREND")
        self.assertEqual(row["extension_risk"], "EXHAUSTION_WATCH")
        self.assertEqual(row["reversal_trigger"], "BULLISH_EARLY")


if __name__ == "__main__":
    unittest.main()