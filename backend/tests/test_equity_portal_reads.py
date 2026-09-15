import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main


UTC = timezone.utc


def pattern():
    return {
        "type": "ASCENDING_TRIANGLE",
        "name": "Ascending triangle",
        "bias": "BULLISH",
        "readiness": "AT_EDGE",
        "grade": "STRONG_GEOMETRY",
        "edge_distance_atr": 0.2,
        "upper_touches": 3,
        "lower_touches": 3,
        "start_time": "2026-08-28T13:30:00+00:00",
    }


def report(interval="30m", ticker="AAPL", *, fresh=True, serveable=None):
    return {
        "analysis_run_id": "run-id",
        "computed_at": "2026-08-28T20:01:00+00:00",
        "expected_market_time": datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        "is_fresh": fresh,
        "is_serveable": fresh if serveable is None else serveable,
        "staleness_sessions": 0 if fresh else 9,
        "market_times": (datetime(2026, 8, 28, 20, 0, tzinfo=UTC),),
        "read_latency_ms": 1.5,
        "results": [{
            "ticker": ticker,
            "sector": "Technology",
            "interval": interval,
            "last_close": 100.0,
            "pattern": pattern(),
            "channel": {"type": "RISING_CHANNEL"},
        }],
        "scanned": 1,
        "channels": {ticker: {"type": "RISING_CHANNEL"}},
        "last_closes": {ticker: 100.0},
    }


def scanner_projection(payload):
    observed = datetime(2026, 8, 31, 2, 0, tzinfo=UTC)
    return {
        "snapshot_id": "snapshot-id",
        "payload": payload,
        "payload_sha256": "a" * 64,
        "generated_at": observed,
        "published_at": observed,
        "read_latency_ms": 1.25,
        "source_manifest": {"event_count": 10, "outcome_count": 30},
        "is_fresh": True,
        "is_serveable": True,
    }


class MaterializedPatternReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_latest_price_date_is_not_process_cached(self):
        with (
            patch.object(
                main, "get_latest_price_date",
                side_effect=("2026-08-31", "2026-09-01"),
            ) as read_date,
            patch.object(main, "_get_cached") as read_cache,
            patch.object(main, "_set_cached") as write_cache,
        ):
            first = await main.latest_price_date()
            second = await main.latest_price_date()

        self.assertEqual(first, {"latest_date": "2026-08-31"})
        self.assertEqual(second, {"latest_date": "2026-09-01"})
        self.assertEqual(read_date.call_count, 2)
        read_cache.assert_not_called()
        write_cache.assert_not_called()

    async def test_materialized_chart_uses_exact_projection_bars(self):
        projection = {
            "is_fresh": True,
            "is_serveable": True,
            "bars": ({
                "bar_start": datetime(2026, 8, 28, 19, 30, tzinfo=UTC),
                "open_price": 100,
                "high_price": 102,
                "low_price": 99,
                "close_price": 101,
                "volume": 1234,
            },),
        }
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(
                main, "current_chart_bar_projection", return_value=projection,
            ) as materialized,
            patch.object(main, "download_historical_data") as legacy,
            patch.object(main, "clear_bulk_cache"),
        ):
            result = await main.get_chart_data(
                "aapl", period="1mo", interval="30m", refresh=True,
            )

        self.assertEqual(result, [{
            "time": 1787945400,
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1234,
        }])
        materialized.assert_any_call("aapl", "30m", limit=390)
        legacy.assert_not_called()

    def test_display_candle_never_replaces_canonical_timestamp(self):
        canonical = ({"bar_start": datetime(2026, 9, 8, 13, 30, tzinfo=UTC)},)
        candidate = {
            "bar_start": canonical[0]["bar_start"],
            "derived": True,
            "provisional": True,
        }
        source = {"is_fresh": True, "bars": ()}
        with (
            patch.object(main, "current_chart_bar_projection", return_value=source),
            patch.object(main, "fold_latest_session_bars", return_value=(candidate,)),
        ):
            result = main._with_current_session_bars("AAPL", "1h", canonical)

        self.assertEqual(result, canonical)

    def test_display_candle_requires_fresh_5m_source(self):
        source = {"is_fresh": False, "is_serveable": True, "bars": ()}
        with (
            patch.object(main, "current_chart_bar_projection", return_value=source),
            patch.object(main, "fold_latest_session_bars") as fold,
        ):
            result = main._with_current_session_bars("AAPL", "1d", ())

        self.assertEqual(result, ())
        fold.assert_not_called()

    async def test_materialized_chart_serves_stale_bars_within_window(self):
        projection = {
            "is_fresh": False,
            "is_serveable": True,
            "bars": ({
                "bar_start": datetime(2026, 8, 31, 13, 30, tzinfo=UTC),
                "open_price": 100,
                "high_price": 102,
                "low_price": 99,
                "close_price": 101,
                "volume": 1234,
            },),
        }
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(
                main, "current_chart_bar_projection", return_value=projection,
            ),
            patch.object(main, "download_historical_data") as legacy,
            patch.object(main, "clear_bulk_cache"),
        ):
            result = await main.get_chart_data(
                "AAPL", period="5d", interval="15m", refresh=True,
            )

        self.assertEqual(len(result), 1)
        legacy.assert_not_called()

    async def test_display_derived_chart_row_exposes_provisional_metadata(self):
        projection = {
            "is_fresh": True,
            "is_serveable": True,
            "bars": ({
                "bar_start": datetime(2026, 9, 4, 13, 30, tzinfo=UTC),
                "bar_end": datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
                "open_price": 100,
                "high_price": 102,
                "low_price": 99,
                "close_price": 101,
                "volume": 1234,
            },),
        }
        derived = {
            "bar_start": datetime(2026, 9, 8, 13, 30, tzinfo=UTC),
            "bar_end": datetime(2026, 9, 8, 16, 0, tzinfo=UTC),
            "open_price": 103,
            "high_price": 106,
            "low_price": 102,
            "close_price": 105,
            "volume": 5678,
            "derived": True,
            "provisional": True,
            "source_interval": "5m",
        }
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(main, "current_chart_bar_projection", return_value=projection),
            patch.object(
                main, "_with_current_session_bars",
                return_value=projection["bars"] + (derived,),
            ),
            patch.object(main, "clear_bulk_cache"),
        ):
            result = await main.get_chart_data(
                "AAPL", period="1mo", interval="1d", refresh=True,
            )

        self.assertEqual(result[-1], {
            "time": 1788874200,
            "open": 103.0,
            "high": 106.0,
            "low": 102.0,
            "close": 105.0,
            "volume": 5678,
            "derived": True,
            "provisional": True,
            "source_interval": "5m",
            "available_through": 1788883200,
        })

    async def test_materialized_chart_fails_closed_when_expired(self):
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(
                main, "current_chart_bar_projection",
                return_value={"is_fresh": False, "is_serveable": False, "bars": ()},
            ),
            patch.object(main, "clear_bulk_cache"),
        ):
            with self.assertRaises(HTTPException) as error:
                await main.get_chart_data(
                    "AAPL", period="5d", interval="15m", refresh=True,
                )

        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual(error.exception.detail["reason"], "EXPIRED")

    async def test_materialized_hourly_chart_bypasses_response_cache(self):
        projection = {
            "is_fresh": True,
            "is_serveable": True,
            "bars": ({
                "bar_start": datetime(2026, 8, 31, 13, 30, tzinfo=UTC),
                "bar_end": datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
                "open_price": 100,
                "high_price": 102,
                "low_price": 99,
                "close_price": 101,
                "volume": 1234,
            },),
        }
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(main, "_get_cached", return_value=[{"time": 1}]) as cached,
            patch.object(
                main, "current_chart_bar_projection", return_value=projection,
            ) as materialized,
            patch.object(main, "_set_cached") as store_cache,
            patch.object(main, "download_historical_data") as legacy,
        ):
            result = await main.get_chart_data(
                "AAPL", period="3mo", interval="1h", refresh=False,
            )

        self.assertEqual(result[-1]["time"], 1788183000)
        materialized.assert_any_call("AAPL", "1h", limit=630)
        cached.assert_not_called()
        store_cache.assert_not_called()
        legacy.assert_not_called()

    async def test_pattern_detail_and_channel_use_only_current_projections(self):
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(main, "current_pattern_watch_projection", return_value=report()),
            patch.object(main, "_load_pattern_frames") as frame_loader,
            patch.object(main, "detect_forming_patterns") as detector,
            patch.object(main, "detect_price_channel") as channel_detector,
        ):
            patterns = await main.get_chart_patterns("aapl", "30m")
            channel = await main.get_price_channel("aapl", "30m")

        self.assertEqual(patterns["patterns"], [pattern()])
        self.assertEqual(patterns["read_source"], "MATERIALIZED_CURRENT_PROJECTION")
        self.assertEqual(channel["channel"], {"type": "RISING_CHANNEL"})
        frame_loader.assert_not_called()
        detector.assert_not_called()
        channel_detector.assert_not_called()

    async def test_pattern_scan_uses_projection_rows_and_fails_closed_when_stale(self):
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(main, "current_pattern_watch_projection", return_value=report()),
            patch.object(main, "_load_pattern_frames") as frame_loader,
        ):
            result = await main.scan_chart_patterns("30m", 1000)

        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["matched_tickers"], 1)
        self.assertEqual(result["results"][0]["ticker"], "AAPL")
        frame_loader.assert_not_called()

        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(
                main, "current_pattern_watch_projection",
                return_value=report(fresh=False),
            ),
        ):
            with self.assertRaises(HTTPException) as error:
                await main.scan_chart_patterns("30m", 1000)
        self.assertEqual(error.exception.status_code, 503)

class ScannerRetirementTests(unittest.TestCase):
    def test_research_routes_retired_but_shared_readers_remain(self):
        paths = {route.path for route in main.app.routes}
        for path in ("/api/scanner-events/qualification", "/api/scanner-events/latest-by-ticker",
                     "/api/scanner-events/summary", "/api/scanner-events/backlog",
                     "/api/scanner-events", "/api/stock/{ticker}/scanner-events"):
            self.assertNotIn(path, paths)
        for path in ("/api/scanner-events/sector-performance", "/api/scan/streak",
                     "/api/stocks/alert-view", "/api/stocks/screening/query"):
            self.assertIn(path, paths)


if __name__ == "__main__":
    unittest.main()