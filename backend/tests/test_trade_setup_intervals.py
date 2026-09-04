import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import numpy as np
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main


def materialized_projection(payload, *, fresh=True):
    market_time = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
    return {
        "payload": payload,
        "evidence_id": "evidence-id",
        "analysis_run_id": "run-id",
        "market_time": market_time,
        "observed_at": market_time,
        "published_at": market_time,
        "expected_market_time": market_time,
        "is_fresh": fresh,
        "read_latency_ms": 1.25,
        "staleness_seconds": 0 if fresh else 1800,
    }


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
    async def test_enabled_setup_intervals_bypass_request_time_computation(self):
        materialized = {
            interval: {"ticker": "AAPL", "interval": interval}
            for interval in ("30m", "1h", "1d", "1wk")
        }

        async def compute(_symbol, interval, _refresh, _shared_frames):
            return {"ticker": "AAPL", "interval": interval}

        def projection(_symbol, interval):
            return materialized_projection(materialized[interval])

        compute_mock = AsyncMock(side_effect=compute)
        with (
            patch.object(main, "MATERIALIZED_30M_SETUP_ENABLED", True),
            patch.object(main, "MATERIALIZED_1H_SETUP_ENABLED", True),
            patch.object(main, "MATERIALIZED_1D_SETUP_ENABLED", True),
            patch.object(main, "MATERIALIZED_1WK_SETUP_ENABLED", True),
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "bulk_load_dataframes", return_value={"AAPL": None}),
            patch.object(main, "get_hourly_data", return_value=[]),
            patch.object(main, "_compute_trade_setup", new=compute_mock),
            patch.object(main, "current_trade_setup_projection", side_effect=projection),
            patch.object(main, "_build_ticker_confluence_zones", return_value=[]),
        ):
            result = await main.get_multi_trade_setup("AAPL")

        self.assertEqual(set(result["setups"]), {"30m", "1h", "1d", "1wk", "1mo"})
        self.assertEqual(compute_mock.await_count, 1)
        self.assertEqual(compute_mock.await_args.args[1], "1mo")
        self.assertTrue(all(
            result["setup_sources"][interval] == "MATERIALIZED_CURRENT_PROJECTION"
            for interval in materialized
        ))
        self.assertEqual(result["setup_sources"]["1mo"], "LEGACY_REQUEST_TIME")

    async def test_multi_setup_omits_30m_when_materialized_gate_is_disabled(self):
        async def compute(_symbol, interval, _refresh, _shared_frames):
            return {"ticker": "AAPL", "interval": interval}

        with (
            patch.object(main, "MATERIALIZED_30M_SETUP_ENABLED", False),
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "bulk_load_dataframes", return_value={"AAPL": None}),
            patch.object(main, "get_hourly_data", return_value=[]),
            patch.object(main, "_compute_trade_setup", new=AsyncMock(side_effect=compute)),
            patch.object(main, "current_trade_setup_projection") as projection,
            patch.object(main, "_build_ticker_confluence_zones", return_value=[]),
        ):
            result = await main.get_multi_trade_setup("AAPL")

        self.assertNotIn("30m", result["setups"])
        self.assertNotIn("30m", result["errors"])
        projection.assert_not_called()

    async def test_multi_setup_reports_missing_enabled_projection_without_fallback(self):
        async def compute(_symbol, interval, _refresh, _shared_frames):
            return {"ticker": "AAPL", "interval": interval}

        with (
            patch.object(main, "MATERIALIZED_30M_SETUP_ENABLED", True),
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "bulk_load_dataframes", return_value={"AAPL": None}),
            patch.object(main, "get_hourly_data", return_value=[]),
            patch.object(main, "_compute_trade_setup", new=AsyncMock(side_effect=compute)),
            patch.object(main, "current_trade_setup_projection", return_value=None),
            patch.object(main, "_build_ticker_confluence_zones", return_value=[]),
        ):
            result = await main.get_multi_trade_setup("AAPL")

        self.assertNotIn("30m", result["setups"])
        self.assertEqual(
            result["errors"]["30m"],
            "Published materialized 30m setup unavailable",
        )

    async def test_multi_setup_reports_stale_projection_without_fallback(self):
        async def compute(_symbol, interval, _refresh, _shared_frames):
            return {"ticker": "AAPL", "interval": interval}

        with (
            patch.object(main, "MATERIALIZED_30M_SETUP_ENABLED", True),
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "bulk_load_dataframes", return_value={"AAPL": None}),
            patch.object(main, "get_hourly_data", return_value=[]),
            patch.object(main, "_compute_trade_setup", new=AsyncMock(side_effect=compute)),
            patch.object(
                main,
                "current_trade_setup_projection",
                return_value=materialized_projection(
                    {"ticker": "AAPL", "interval": "30m"}, fresh=False
                ),
            ),
            patch.object(main, "_build_ticker_confluence_zones", return_value=[]),
        ):
            result = await main.get_multi_trade_setup("AAPL")

        self.assertNotIn("30m", result["setups"])
        self.assertEqual(result["setup_read_metrics"]["30m"]["status"], "STALE")
        self.assertIn("is stale", result["errors"]["30m"])

    async def test_multi_setup_injects_exact_materialized_30m_projection(self):
        materialized = {"ticker": "AAPL", "interval": "30m", "last_close": 100.0}
        legacy = {
            interval: {"ticker": "AAPL", "interval": interval}
            for interval in ("1h", "1d", "1wk", "1mo")
        }

        async def compute(_symbol, interval, _refresh, _shared_frames):
            return legacy[interval]

        with (
            patch.object(main, "MATERIALIZED_30M_SETUP_ENABLED", True),
            patch.object(main, "_get_cached", return_value=None),
            patch.object(main, "_set_cached"),
            patch.object(main, "bulk_load_dataframes", return_value={"AAPL": None}),
            patch.object(main, "get_hourly_data", return_value=[]),
            patch.object(main, "_compute_trade_setup", new=AsyncMock(side_effect=compute)),
            patch.object(
                main,
                "current_trade_setup_projection",
                return_value=materialized_projection(materialized),
            ) as projection,
            patch.object(main, "_build_ticker_confluence_zones", return_value=[]),
        ):
            result = await main.get_multi_trade_setup("aapl")

        self.assertIs(result["setups"]["30m"], materialized)
        self.assertEqual(result["setup_sources"]["30m"], "MATERIALIZED_CURRENT_PROJECTION")
        self.assertEqual(
            {interval: result["setup_sources"][interval] for interval in legacy},
            {interval: "LEGACY_REQUEST_TIME" for interval in legacy},
        )
        self.assertNotIn("30m", result["errors"])
        projection.assert_called_once_with("AAPL", "30m")

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

    async def test_rich_setup_technical_snapshot_matches_golden_contract(self):
        hourly = price_frame(pd.date_range("2026-07-01", periods=250, freq="h", tz="UTC"))
        daily = price_frame(pd.date_range("2025-09-01", periods=250, freq="B", tz="UTC"))

        with (
            patch.object(main, "scan_gap_strategies", return_value=[]),
            patch.object(main, "scan_fair_value_gaps", return_value=[]),
            patch.object(main, "scan_fibonacci", return_value=None),
            patch.object(main, "scan_moving_average_crossover", return_value=None),
            patch.object(main, "scan_momentum_pullback", return_value=None),
            patch.object(main, "scan_bearish_bounce", return_value=None),
        ):
            result = await main._compute_trade_setup(
                "UNIT-GOLDEN", "1h", True, {"daily": daily, "hourly": hourly}
            )

        self.assertEqual(result["technicals"], {
            "rsi": 100.0, "rsi_state": "Overbought", "stoch_k": 72.0,
            "atr": 2.0, "atr_pct": 1.54, "ma10": 129.46, "ma20": 128.86,
            "ma50": 127.05, "ma100": 124.04, "ma200": 118.01,
            "ema8": 129.58, "ema21": 128.8, "ema50": 127.05,
            "vwap": 128.86, "price_vs_vwap": "Above",
            "dist_to_8ema": 0.33, "dist_to_21ema": 0.94,
            "trend_consistency": 100.0, "macd": 0.8434,
            "macd_signal": 0.8434, "macd_histogram": 0.0,
            "macd_histogram_previous": 0.0, "macd_state": "BULLISH_RISING",
            "adx": 100.0, "plus_di": 6.0, "minus_di": 0.0,
            "historical_volatility_pct": 0.0,
            "historical_volatility_percentile": 0.0,
            "historical_volatility_state": "QUIET", "relative_volume": 1.01,
            "volume_trend_ratio": 1.01, "volume_trend_pct": 0.5,
            "volume_trend_state": "STABLE", "volume_slope": 0.002,
            "volume_slope_state": "FLAT",
            "volume_sparkline": [1.0, 1.0, 1.0, 1.0, 1.0, 1.01, 1.01, 1.01],
            "cmf_20": 0.0, "volume_pressure": "BALANCED",
            "range_low": 99.0, "range_high": 131.0,
            "range_position_pct": 96.9,
        })
        self.assertEqual(result["candlestick_patterns"], [])
        self.assertEqual(result["ema_alignment"], {
            "primary": "Bullish Stack",
            "primary_detail": (
                "8 EMA > 21 EMA > 50 EMA — short-term momentum aligned with trend"
            ),
            "confirm_interval": "1d",
            "confirm": "Bullish",
            "confirm_ema8": 129.58,
            "confirm_ema21": 128.8,
            "multi_tf_agree": True,
        })
        self.assertEqual(result["golden_cross"], {
            "type": "Above (Bullish)",
            "bars_ago": None,
            "detail": "50 SMA above 200 SMA — bullish structure, no recent cross",
        })
        self.assertEqual(result["direction"], {
            "bias": "Bullish",
            "conviction": "High",
            "bull_signals": 3,
            "bear_signals": 1,
        })
        self.assertEqual(result["signals"], [
            "EMA Stack: 8 > 21 > 50 (bullish alignment)",
            "Price above VWAP(20) $128.86",
            "Multi-TF: 1d 8/21 EMA aligns bullish with 1h",
            "RSI: 100.0 (overbought — pullback risk)",
        ])
        self.assertEqual(result["entries"], [{
            "strategy": "8 EMA Retest",
            "condition": "Price at 8 EMA ($129.58) — pullback entry in trend",
            "price_zone": "$129.58",
            "zone_low": 129.58,
            "zone_high": 129.58,
            "strength": "Strong",
        }])
        self.assertEqual(result["targets"], [
            {"level": "Prior 249-Bar High", "price": 130.88, "source": "Price Action"},
            {"level": "ATR Target (2R)", "price": 134.0, "source": "ATR"},
        ])
        self.assertEqual(result["stops"], [
            {"level": "8 EMA", "price": 129.58, "source": "EMA"},
            {"level": "VWAP(20)", "price": 128.86, "source": "VWAP"},
            {"level": "ATR Stop (1R)", "price": 128.0, "source": "ATR"},
            {"level": "50 SMA", "price": 127.05, "source": "SMA"},
            {"level": "Prior 249-Bar Low", "price": 99.0, "source": "Price Action"},
        ])
        self.assertEqual(result["timing"], {
            "urgency": "Near-term",
            "detail": "RSI at extreme (100.0) — mean reversion likely within 1–5 days",
        })
        self.assertEqual(result["duration"], {
            "estimate": "Unknown",
            "detail": "Insufficient MA data to estimate duration",
        })
        self.assertEqual(result["confluence"], {"grade": "B+", "count": 4})
        self.assertEqual(result["structural_patterns"], [])


if __name__ == "__main__":
    unittest.main()