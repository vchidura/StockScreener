import sys
import unittest
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.xsmom import attach_xsmom_ranks, score_xsmom_cross_sections
from research.regime_context import replay_regime_context


class XsMomentumTests(unittest.TestCase):
    def test_rank_scoring_assigns_actionable_extreme_deciles(self):
        rows = []
        for index in range(20):
            rows.append({
                "date": pd.Timestamp("2025-01-02"),
                "ticker": f"T{index:02d}",
                "mom_12_1": float(index),
                "beta_60": 0.0,
                "liquidity": 0.0,
                "vol_21": 0.0,
                "sector": "TEST",
            })
        ranks = score_xsmom_cross_sections(pd.DataFrame(rows))
        by_ticker = ranks.set_index("ticker")
        self.assertEqual(by_ticker.loc["T19", "side"], "LONG")
        self.assertEqual(by_ticker.loc["T00", "side"], "SHORT")
        self.assertEqual((ranks["side"] == "LONG").sum(), 2)
        self.assertEqual((ranks["side"] == "SHORT").sum(), 2)

    def test_hourly_uses_prior_rank_while_daily_can_use_same_close(self):
        observations = pd.DataFrame([
            {"ticker": "AAA", "interval": "1d", "trade_date": "2025-01-03"},
            {"ticker": "AAA", "interval": "1h", "trade_date": "2025-01-03"},
        ])
        ranks = pd.DataFrame([
            {
                "date": "2025-01-02", "ticker": "AAA",
                "decile": 1, "percentile": 0.05, "universe_size": 100,
                "market_breadth": 0.3, "sector_breadth": 0.2,
                "market_volatility_percentile": 0.8,
            },
            {
                "date": "2025-01-03", "ticker": "AAA",
                "decile": 10, "percentile": 0.95, "universe_size": 100,
                "market_breadth": 0.7, "sector_breadth": 0.8,
                "market_volatility_percentile": 0.2,
            },
        ])
        attached = attach_xsmom_ranks(observations, ranks)
        self.assertEqual(attached.loc[0, "xs_side"], "LONG")
        self.assertEqual(attached.loc[0, "xs_age_days"], 0)
        self.assertEqual(attached.loc[1, "xs_side"], "SHORT")
        self.assertEqual(attached.loc[1, "xs_age_days"], 1)
        self.assertEqual(attached.loc[0, "market_breadth"], 0.7)
        self.assertEqual(attached.loc[1, "market_breadth"], 0.3)


class RegimeContextTests(unittest.TestCase):
    def test_breadth_uses_each_dates_available_closes(self):
        dates = pd.date_range("2025-01-02", periods=70, freq="B")
        rows = []
        for ticker, rising in (("AAA", True), ("BBB", False)):
            for index, date in enumerate(dates):
                close = 100.0 + index if rising else 100.0 - 0.5 * index
                rows.append({
                    "date": date,
                    "ticker": ticker,
                    "open": close,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                    "volume": 1000.0,
                })

        context = replay_regime_context(
            pd.DataFrame(rows), {"AAA": "TECH", "BBB": "FINANCE"}
        )
        latest = context[context["date"] == dates[-1]].set_index("ticker")

        self.assertEqual(latest.loc["AAA", "market_breadth"], 0.5)
        self.assertEqual(latest.loc["AAA", "sector_breadth"], 1.0)
        self.assertEqual(latest.loc["BBB", "sector_breadth"], 0.0)


if __name__ == "__main__":
    unittest.main()