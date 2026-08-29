import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from main import _build_ticker_confluence_zones


def setup(interval: str, close: float, ema21: float, fib: float, pattern=None, zones=None):
    return {
        "last_close": close,
        "technicals": {
            "atr": 2.0,
            "ema8": close + 4,
            "ema21": ema21,
            "ema50": close - 5,
            "ma50": close - 6,
            "ma100": close - 10,
            "ma200": close - 15,
            "vwap": close + 3,
        },
        "targets": [],
        "stops": [],
        "zones": zones or [],
        "candlestick_patterns": [pattern] if pattern else [],
        "strategy_results": {
            "fibonacci": {
                "active_leg": {"levels": [{"name": "61.8%", "price": fib}]},
                "target_levels": [],
            }
        },
        "interval": interval,
    }


class TickerConfluenceTests(unittest.TestCase):
    def test_clusters_independent_sources_and_attaches_candle_confirmation(self):
        pattern = {
            "name": "Hammer",
            "direction": "BULLISH",
            "bar_time": "2026-08-25T00:00:00",
            "open": 99.8,
            "high": 100.8,
            "low": 99.2,
            "close": 100.4,
        }
        setups = {
            "1d": setup("1d", 101.0, 100.0, 100.2, pattern),
            "1h": setup("1h", 101.0, 100.1, 100.3),
        }

        zones = _build_ticker_confluence_zones(setups)
        zone = min(zones, key=lambda item: abs(item["midpoint"] - 100.1))

        self.assertEqual(zone["strength"], "STRONG_CONFLUENCE")
        self.assertEqual(zone["intervals"], ["1h", "1d"])
        self.assertIn("fibonacci", zone["families"])
        self.assertIn("moving_average", zone["families"])
        self.assertEqual(zone["confirmations"][0]["name"], "Hammer")
        self.assertEqual(zone["confirmations"][0]["interval"], "1d")

    def test_does_not_bridge_separate_zones_transitively(self):
        setups = {
            "1d": setup("1d", 100.0, 98.0, 98.5),
            "1h": setup("1h", 100.0, 99.0, 99.5),
        }

        zones = _build_ticker_confluence_zones(setups)
        nearby = [zone for zone in zones if 97.5 <= zone["midpoint"] <= 100.0]

        self.assertGreaterEqual(len(nearby), 2)

    def test_preserves_volume_pivot_family_and_fibonacci_overlap_evidence(self):
        volume_zone = {
            "name": "Volume pivot demand",
            "low": 99.9,
            "high": 100.1,
            "source": "Volume Pivot",
            "qualifier": "Pivot volume 1.80x its prior 20-bar median (+80%); near Fib 61.8%",
        }
        setups = {
            "1d": setup("1d", 101.0, 95.0, 100.0, zones=[volume_zone]),
        }

        zones = _build_ticker_confluence_zones(setups)
        zone = min(zones, key=lambda item: abs(item["midpoint"] - 100.0))
        volume_reference = next(
            reference for reference in zone["references"]
            if reference["family"] == "volume_pivot"
        )

        self.assertIn("fibonacci", zone["families"])
        self.assertIn("volume_pivot", zone["families"])
        self.assertEqual(zone["strength"], "CONFLUENCE")
        self.assertEqual(volume_reference["qualifier"], volume_zone["qualifier"])


if __name__ == "__main__":
    unittest.main()
