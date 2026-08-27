import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main


def price_frame(index: pd.DatetimeIndex) -> pd.DataFrame:
    close = np.linspace(100.0, 130.0, len(index))
    return pd.DataFrame(
        {
            "open": close - 0.25,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": np.linspace(1_000_000.0, 1_200_000.0, len(index)),
        },
        index=index,
    )


class TradeSetupIntervalTests(unittest.IsolatedAsyncioTestCase):
    async def test_hourly_setup_propagates_interval_to_interval_aware_scanners(self):
        hourly = price_frame(pd.date_range("2026-07-01", periods=250, freq="h", tz="UTC"))
        daily = price_frame(pd.date_range("2025-09-01", periods=250, freq="B", tz="UTC"))

        with (
            patch.object(main, "scan_gap_strategies", return_value=[]),
            patch.object(main, "scan_fair_value_gaps", return_value=[]),
            patch.object(main, "scan_fibonacci", return_value=None),
            patch.object(main, "scan_moving_average_crossover", return_value=None) as ma_scan,
            patch.object(main, "scan_momentum_pullback", return_value=None) as pullback_scan,
            patch.object(main, "scan_bearish_bounce", return_value=None) as bounce_scan,
        ):
            result = await main._compute_trade_setup(
                "UNIT-HOURLY",
                "1h",
                True,
                {"daily": daily, "hourly": hourly},
            )

        self.assertEqual(result["interval"], "1h")
        self.assertEqual(result["ema_alignment"]["confirm_interval"], "1d")
        self.assertEqual(ma_scan.call_args.kwargs["interval"], "1h")
        self.assertEqual(pullback_scan.call_args.kwargs["interval"], "1h")
        self.assertEqual(bounce_scan.call_args.kwargs["interval"], "1h")

    async def test_monthly_setup_uses_latest_trading_date_and_weekly_confirmation(self):
        daily = price_frame(
            pd.date_range(end="2026-08-26", periods=4000, freq="B", tz="UTC")
        )

        with (
            patch.object(main, "scan_gap_strategies", return_value=[]),
            patch.object(main, "scan_fair_value_gaps", return_value=[]),
            patch.object(main, "scan_fibonacci", return_value=None),
            patch.object(main, "scan_moving_average_crossover", return_value=None) as ma_scan,
        ):
            result = await main._compute_trade_setup(
                "UNIT-MONTHLY",
                "1mo",
                True,
                {"daily": daily, "hourly": None},
            )

        self.assertEqual(result["interval"], "1mo")
        self.assertEqual(result["date"], "2026-08-26")
        self.assertEqual(result["ema_alignment"]["confirm_interval"], "1wk")
        self.assertEqual(ma_scan.call_args.kwargs["interval"], "1mo")


if __name__ == "__main__":
    unittest.main()