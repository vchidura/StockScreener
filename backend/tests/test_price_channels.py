import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.price_channels import _channel_candidates, detect_price_channel
import main


def channel_frame(length: int = 62) -> pd.DataFrame:
    positions = np.arange(length, dtype=float)
    close = 95 + 0.30 * positions
    return pd.DataFrame(
        {
            "open": close - 0.1,
            "high": close + 5,
            "low": close - 5,
            "close": close,
            "volume": np.full(length, 1000.0),
        },
        index=pd.date_range("2026-01-01", periods=length, freq="D", tz="UTC"),
    )


def pivot(index: int, price: float, pivot_type: str) -> dict:
    return {"index": index, "price": price, "type": pivot_type}


class PriceChannelTests(unittest.TestCase):
    def setUp(self):
        self.frame = channel_frame()
        self.atr = pd.Series(2.0, index=self.frame.index)
        self.pivots = [
            pivot(5, 100 + 0.30 * 5, "high"),
            pivot(12, 90 + 0.30 * 12, "low"),
            pivot(20, 100 + 0.30 * 20, "high"),
            pivot(27, 90 + 0.30 * 27, "low"),
            pivot(35, 100 + 0.30 * 35, "high"),
            pivot(42, 90 + 0.30 * 42, "low"),
            pivot(50, 100 + 0.30 * 50, "high"),
        ]

    def test_detects_parallel_rising_channel(self):
        channels = _channel_candidates(self.frame, self.atr, self.pivots)

        channel = channels[0]
        self.assertEqual(channel["type"], "RISING_CHANNEL")
        self.assertEqual(channel["bias"], "BULLISH")
        self.assertEqual(channel["grade"], "STRONG_GEOMETRY")
        self.assertEqual([line["role"] for line in channel["lines"]], ["resistance", "support"])
        self.assertGreaterEqual(channel["upper_touches"], 3)
        self.assertGreaterEqual(channel["lower_touches"], 3)

    def test_rejects_nonparallel_boundaries(self):
        pivots = [
            pivot(item["index"], item["price"], item["type"])
            for item in self.pivots
        ]
        for item in pivots:
            if item["type"] == "high":
                item["price"] -= 0.20 * item["index"]

        self.assertEqual(_channel_candidates(self.frame, self.atr, pivots), [])

    def test_detects_parallel_falling_channel(self):
        positions = np.arange(len(self.frame), dtype=float)
        falling = self.frame.copy()
        falling["close"] = 115 - 0.30 * positions
        falling["open"] = falling["close"] + 0.1
        falling["high"] = falling["close"] + 5
        falling["low"] = falling["close"] - 5
        pivots = [
            pivot(item["index"], 120 - 0.30 * item["index"], "high")
            if item["type"] == "high"
            else pivot(item["index"], 110 - 0.30 * item["index"], "low")
            for item in self.pivots
        ]

        channel = _channel_candidates(falling, self.atr, pivots)[0]

        self.assertEqual(channel["type"], "FALLING_CHANNEL")
        self.assertEqual(channel["bias"], "BEARISH")

    def test_broken_then_reentered_channel_stays_retired(self):
        broken = self.frame.copy()
        broken.iloc[55, broken.columns.get_loc("close")] = 80

        self.assertEqual(_channel_candidates(broken, self.atr, self.pivots), [])

    def test_public_detector_ignores_newest_forming_bar(self):
        first = detect_price_channel(self.frame)
        changed = self.frame.copy()
        changed.iloc[-1, changed.columns.get_loc("high")] = 1000
        changed.iloc[-1, changed.columns.get_loc("low")] = 1
        changed.iloc[-1, changed.columns.get_loc("close")] = 500

        self.assertEqual(detect_price_channel(changed), first)

    def test_public_detector_rejects_nonfinite_completed_prices(self):
        corrupted = self.frame.copy()
        corrupted.loc[corrupted.index[:-1], "close"] = np.inf

        self.assertIsNone(detect_price_channel(corrupted))


class PriceChannelEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_channel_endpoint_uses_shared_frame_and_completed_close(self):
        source = channel_frame(40)
        detected = {"type": "RISING_CHANNEL"}
        with (
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "_load_pattern_frames", return_value={"AAA": source}) as loader,
            patch.object(main, "detect_price_channel", return_value=detected),
        ):
            result = await main.get_price_channel("aaa", "1d")

        loader.assert_called_once_with(["AAA"], "1d")
        self.assertEqual(result["last_close"], round(float(source.iloc[-2]["close"]), 2))
        self.assertEqual(result["channel"], detected)

    async def test_channel_endpoint_excludes_one_minute_interval(self):
        operation = main.app.openapi()["paths"]["/api/stock/{ticker}/price-channel"]["get"]
        interval = next(parameter for parameter in operation["parameters"] if parameter["name"] == "interval")

        self.assertEqual(interval["schema"]["pattern"], "^(5m|15m|30m|1h|1d|1wk)$")


if __name__ == "__main__":
    unittest.main()