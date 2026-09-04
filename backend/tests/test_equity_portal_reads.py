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


def report(interval="30m", ticker="AAPL", *, fresh=True):
    return {
        "analysis_run_id": "run-id",
        "computed_at": "2026-08-28T20:01:00+00:00",
        "expected_market_time": datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        "is_fresh": fresh,
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
        materialized.assert_called_once_with("aapl", "30m", limit=390)
        legacy.assert_not_called()

    async def test_materialized_chart_fails_closed_when_stale(self):
        with (
            patch.object(main, "MATERIALIZED_PATTERN_WATCH_ENABLED", True),
            patch.object(
                main, "current_chart_bar_projection",
                return_value={"is_fresh": False, "bars": ()},
            ),
            patch.object(main, "clear_bulk_cache"),
        ):
            with self.assertRaises(HTTPException) as error:
                await main.get_chart_data(
                    "AAPL", period="5d", interval="15m", refresh=True,
                )

        self.assertEqual(error.exception.status_code, 503)

    async def test_materialized_hourly_chart_bypasses_response_cache(self):
        projection = {
            "is_fresh": True,
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
        materialized.assert_called_once_with("AAPL", "1h", limit=630)
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


class DurableScannerReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_uses_canonical_equity_research(self):
        rows = [{"interval": "1d", "independent_periods": 50}]
        with patch(
            "equity.scanner_research.event_summary", return_value=rows,
        ) as compute:
            result = await main.scanner_event_summary(
                interval="1d", discovery_state=None, min_periods=40,
            )

        self.assertEqual(result["results"], rows)
        self.assertEqual(result["read_source"], "CANONICAL_EQUITY_RESEARCH")
        compute.assert_called_once_with("1d", None, 40)

    async def test_qualification_uses_durable_canonical_revisions(self):
        rows = [{"interval": "1d", "evidence_status": "UNRANKED"}]
        with patch(
            "equity.scanner_research.qualification_report", return_value=rows,
        ) as compute:
            result = await main.scanner_event_qualification(interval="1d")

        self.assertEqual(result["results"], rows)
        self.assertEqual(result["read_source"], "CANONICAL_EQUITY_RESEARCH")
        compute.assert_called_once_with("1d")

    async def test_backlog_and_recent_events_use_canonical_tables(self):
        backlog = [{"interval": "1d", "horizon_bars": 5, "pending": 1}]
        events = [{"event_id": "evidence-id", "interval": "1d"}]
        with (
            patch(
                "equity.scanner_research.pending_outcome_counts",
                return_value=backlog,
            ),
            patch("equity.scanner_research.recent_events", return_value=events),
        ):
            backlog_result = await main.scanner_event_backlog()
            event_result = await main.scanner_events_endpoint(
                interval="1d", limit=100,
            )

        self.assertEqual(backlog_result["results"], backlog)
        self.assertEqual(event_result["results"], events)
        self.assertEqual(
            backlog_result["read_source"], "CANONICAL_EQUITY_RESEARCH"
        )
        self.assertEqual(
            event_result["read_source"], "CANONICAL_EQUITY_RESEARCH"
        )

    async def test_latest_and_ticker_history_use_original_canonical_evidence(self):
        latest = [{"ticker": "AAPL", "interval": "1h"}]
        history = [{"event_id": "evidence-id", "interval": "1d"}]
        with (
            patch("equity.scanner_research.latest_ticker_signals", return_value=latest) as current,
            patch("equity.scanner_research.ticker_events", return_value=history) as ticker,
        ):
            latest_result = await main.latest_ticker_scanner_signals(
                interval="1h", limit=500, sessions=10, hourly_sessions=2,
            )
            ticker_result = await main.ticker_scanner_events(
                "aapl", limit=100, daily_sessions=21, hourly_sessions=5,
            )

        self.assertEqual(latest_result["results"], latest)
        self.assertEqual(ticker_result["events"], history)
        self.assertEqual(
            latest_result["read_source"], "CANONICAL_EQUITY_RESEARCH"
        )
        self.assertEqual(
            ticker_result["read_source"], "CANONICAL_EQUITY_RESEARCH"
        )
        current.assert_called_once_with("1h", 500, 10, 2)
        ticker.assert_called_once_with("aapl", 100, 21, 5)


if __name__ == "__main__":
    unittest.main()