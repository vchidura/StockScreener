import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main


def frame(close: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [close - 1, close - 0.5],
            "high": [close + 1, close + 1],
            "low": [close - 2, close - 1],
            "close": [close - 0.5, close],
            "volume": [1000, 1100],
        },
        index=pd.date_range("2026-08-26", periods=2, freq="D", tz="UTC"),
    )


def pattern(name: str, grade: str, touches: int, readiness: str = "FORMING", edge_distance: float = 1.0) -> dict:
    return {
        "type": name.upper().replace(" ", "_"),
        "name": name,
        "status": "FORMING",
        "bias": "NEUTRAL",
        "grade": grade,
        "start_time": 1,
        "end_time": 2,
        "formation_bars": 20,
        "upper_touches": touches // 2,
        "lower_touches": touches - touches // 2,
        "contraction_pct": 40.0,
        "apex_bars_ahead": 8.0,
        "fit_error_atr": 0.2,
        "flagpole_atr": None,
        "invalidation_price": None,
        "readiness": readiness,
        "boundary_role": "resistance",
        "boundary_price": 101.0,
        "edge_distance_atr": edge_distance,
        "edge_distance_pct": 1.0,
        "lines": [],
    }


class PatternWatchEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_universe_scan_contract_excludes_removed_one_minute_interval(self):
        operation = main.app.openapi()["paths"]["/api/chart-patterns/scan"]["get"]
        interval = next(parameter for parameter in operation["parameters"] if parameter["name"] == "interval")

        self.assertEqual(interval["schema"]["pattern"], "^(5m|15m|30m|1h|1d|1wk)$")

    async def test_ticker_patterns_report_the_close_used_by_the_detector(self):
        source = frame(100.0)
        with (
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "_load_pattern_frames", return_value={"AAA": source}),
            patch.object(main, "detect_forming_patterns", return_value=[]),
        ):
            result = await main.get_chart_patterns("aaa", "1d")

        self.assertEqual(result["last_close"], 99.5)

    async def test_empty_active_universe_returns_empty_scan(self):
        with (
            patch.object(main, "get_selected_tickers", return_value=[]),
            patch.object(main, "clear_bulk_cache"),
            patch.object(main, "_invalidate_prefix"),
        ):
            result = await main.scan_chart_patterns("1d", 1000, True)

        self.assertEqual(result["scanned"], 0)
        self.assertEqual(result["matched_tickers"], 0)
        self.assertEqual(result["results"], [])
        self.assertIsNone(result["cross_frame"])

    async def test_daily_scan_returns_sector_metadata_and_geometry_order(self):
        frames = {"AAA": frame(100.0), "BBB": frame(200.0)}
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {"ticker": "AAA", "sector": "Technology"},
            {"ticker": "BBB", "sector": "Industrials"},
        ]

        @contextmanager
        def cursor_context():
            yield cursor

        def detect(source):
            if float(source.iloc[-1]["close"]) == 100.0:
                return [pattern("Ascending triangle", "VALID_GEOMETRY", 4)]
            return [pattern("Falling wedge", "STRONG_GEOMETRY", 6)]

        with (
            patch.object(main, "get_selected_tickers", return_value=["AAA", "BBB"]),
            patch.object(main, "bulk_load_dataframes", return_value=frames),
            patch.object(main, "detect_forming_patterns", side_effect=detect),
            patch.object(main, "detect_price_channel", return_value={"type": "RISING_CHANNEL"}),
            patch.object(main, "clear_bulk_cache"),
            patch.object(main, "_invalidate_prefix") as invalidate_prefix,
            patch("database.get_db_cursor", return_value=cursor_context()),
        ):
            result = await main.scan_chart_patterns("1d", 1000, True)

        invalidate_prefix.assert_has_calls([
            call(f"{main.CHART_PATTERN_SCAN_CACHE}_"),
            call(f"{main.CHART_PATTERN_DETAIL_CACHE}_"),
            call(f"{main.CHART_PATTERN_TICKER_CACHE}_"),
            call(f"{main.PRICE_CHANNEL_CACHE}_"),
        ])
        self.assertEqual(result["scanned"], 2)
        self.assertEqual(result["matched_tickers"], 2)
        self.assertEqual([row["ticker"] for row in result["results"]], ["BBB", "AAA"])
        self.assertEqual(result["results"][0]["sector"], "Industrials")
        self.assertEqual(result["results"][1]["last_close"], 99.5)
        self.assertTrue(all(row["channel"]["type"] == "RISING_CHANNEL" for row in result["results"]))

    async def test_intraday_scan_uses_bounded_bulk_frames(self):
        frames = {"AAA": frame(100.0)}
        cursor = MagicMock()
        cursor.fetchall.return_value = [{"ticker": "AAA", "sector": "Technology"}]

        @contextmanager
        def cursor_context():
            yield cursor

        with (
            patch.object(main, "get_selected_tickers", return_value=["AAA"]),
            patch.object(main, "_load_intraday_frames", return_value=frames) as loader,
            patch.object(main, "detect_forming_patterns", return_value=[]),
            patch.object(main, "detect_price_channel", return_value=None) as channel_detector,
            patch.object(main, "clear_bulk_cache"),
            patch.object(main, "_invalidate_prefix"),
            patch("database.get_db_cursor", return_value=cursor_context()),
        ):
            result = await main.scan_chart_patterns("15m", 1000, True)

        loader.assert_called_once_with(["AAA"], "15m", main.PATTERN_FRAME_ROWS)
        channel_detector.assert_not_called()
        self.assertEqual(result["scanned"], 1)

    async def test_weekly_pattern_frames_are_friday_anchored_and_bounded(self):
        source = pd.DataFrame(
            {
                "open": range(400), "high": range(1, 401),
                "low": range(400), "close": range(1, 401),
                "volume": [1000] * 400,
            },
            index=pd.date_range("2019-01-01", periods=400, freq="B", tz="UTC"),
        )
        with patch.object(main, "bulk_load_dataframes", return_value={"AAA": source}) as loader:
            frames = main._load_pattern_frames(["AAA"], "1wk")

        loader.assert_called_once_with(["AAA"], 2500)
        self.assertLessEqual(len(frames["AAA"]), main.PATTERN_FRAME_ROWS)
        self.assertTrue(all(timestamp.weekday() == 4 for timestamp in frames["AAA"].index))

    async def test_matched_ticker_count_precedes_result_limit(self):
        frames = {"AAA": frame(100.0), "BBB": frame(200.0)}
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {"ticker": "AAA", "sector": "Technology"},
            {"ticker": "BBB", "sector": "Industrials"},
        ]

        @contextmanager
        def cursor_context():
            yield cursor

        with (
            patch.object(main, "get_selected_tickers", return_value=["AAA", "BBB"]),
            patch.object(main, "bulk_load_dataframes", return_value=frames),
            patch.object(
                main,
                "detect_forming_patterns",
                return_value=[pattern("Ascending triangle", "VALID_GEOMETRY", 4)],
            ),
            patch.object(main, "clear_bulk_cache"),
            patch.object(main, "_invalidate_prefix"),
            patch("database.get_db_cursor", return_value=cursor_context()),
        ):
            result = await main.scan_chart_patterns("1d", 1, True)

        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["matched_tickers"], 2)

    async def test_readiness_sorts_before_geometry_grade(self):
        frames = {"AAA": frame(100.0), "BBB": frame(200.0)}
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {"ticker": "AAA", "sector": "Technology"},
            {"ticker": "BBB", "sector": "Industrials"},
        ]

        @contextmanager
        def cursor_context():
            yield cursor

        def detect(source):
            if float(source.iloc[-1]["close"]) == 100.0:
                return [pattern("Ascending triangle", "VALID_GEOMETRY", 4, "AT_EDGE", 0.1)]
            return [pattern("Falling wedge", "STRONG_GEOMETRY", 6, "FORMING", 1.0)]

        with (
            patch.object(main, "get_selected_tickers", return_value=["AAA", "BBB"]),
            patch.object(main, "bulk_load_dataframes", return_value=frames),
            patch.object(main, "detect_forming_patterns", side_effect=detect),
            patch.object(main, "clear_bulk_cache"),
            patch.object(main, "_invalidate_prefix"),
            patch("database.get_db_cursor", return_value=cursor_context()),
        ):
            result = await main.scan_chart_patterns("1d", 1000, True)

        self.assertEqual([row["ticker"] for row in result["results"]], ["AAA", "BBB"])

    async def test_ticker_scan_combines_all_intervals_and_preserves_row_frames(self):
        responses = []
        for interval in ("5m", "15m", "30m", "1h", "1d", "1wk"):
            patterns = []
            if interval == "15m":
                patterns = [pattern("Descending triangle", "VALID_GEOMETRY", 4, "NEAR_EDGE", 0.5)]
            elif interval == "30m":
                patterns = [pattern("Symmetrical triangle", "STRONG_GEOMETRY", 6, "AT_EDGE", 0.1)]
            responses.append({
                "ticker": "AAA",
                "interval": interval,
                "status": "FORMING_RESEARCH",
                "last_close": 100.0,
                "patterns": patterns,
                "computed_at": "2026-08-28T12:00:00+00:00",
            })
        cursor = MagicMock()
        cursor.fetchone.return_value = {"sector": "Technology"}

        @contextmanager
        def cursor_context():
            yield cursor

        detector = AsyncMock(side_effect=responses)
        channel_responses = [
            {
                "ticker": "AAA", "interval": interval,
                "status": "CHANNEL_RESEARCH", "last_close": 100.0,
                "channel": {"type": "RISING_CHANNEL"} if interval == "30m" else None,
                "computed_at": "2026-08-28T12:00:00+00:00",
            }
            for interval in ("5m", "15m", "30m", "1h", "1d", "1wk")
        ]
        channel_detector = AsyncMock(side_effect=channel_responses)
        with (
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "clear_bulk_cache") as clear_bulk_cache,
            patch.object(main, "_invalidate_prefix") as invalidate_prefix,
            patch.object(main, "get_chart_patterns", detector),
            patch.object(main, "get_price_channel", channel_detector),
            patch("database.get_db_cursor", return_value=cursor_context()),
        ):
            result = await main.scan_ticker_chart_patterns("aaa", refresh=True)

        self.assertEqual(detector.await_count, 6)
        self.assertEqual(
            detector.await_args_list,
            [call("AAA", interval, False) for interval in ("5m", "15m", "30m", "1h", "1d", "1wk")],
        )
        self.assertEqual(channel_detector.await_count, 6)
        self.assertEqual(
            channel_detector.await_args_list,
            [call("AAA", interval, False) for interval in ("5m", "15m", "30m", "1h", "1d", "1wk")],
        )
        clear_bulk_cache.assert_called_once_with()
        invalidate_prefix.assert_any_call(f"{main.CHART_PATTERN_TICKER_CACHE}_AAA")
        invalidate_prefix.assert_any_call(f"{main.CHART_PATTERN_DETAIL_CACHE}_AAA_")
        invalidate_prefix.assert_any_call(f"{main.PRICE_CHANNEL_CACHE}_AAA_")
        self.assertEqual(result["cross_frame"]["state"], "NEUTRAL")
        self.assertEqual(result["matched_tickers"], 1)
        self.assertEqual(
            [(row["interval"], row["pattern"]["readiness"]) for row in result["results"]],
            [("30m", "AT_EDGE"), ("15m", "NEAR_EDGE")],
        )
        self.assertIsNotNone(result["results"][0]["channel"])
        self.assertIsNone(result["results"][1]["channel"])
        self.assertTrue(all(row["sector"] == "Technology" for row in result["results"]))


if __name__ == "__main__":
    unittest.main()