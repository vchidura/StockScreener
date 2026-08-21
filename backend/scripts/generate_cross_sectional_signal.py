"""
Generate the validated cross-sectional momentum signal and persist it.

    python backend/scripts/generate_cross_sectional_signal.py
    python backend/scripts/generate_cross_sectional_signal.py --date 2026-08-07 --dry-run

Model (xsmom-1.0), as validated by run_alpha_research.py:
  feature   : mom_12_1 (12-month return skipping the most recent month)
  horizon   : 21 trading days
  neutralise: beta_60, liquidity, vol_21, and sector means
  actionable: decile 10 = LONG, decile 1 = SHORT, everything else FLAT

Out-of-sample, neutralised, net of 2bps: 23.0%/yr at Sharpe 1.32 versus 13.7%
at Sharpe 1.02 for equal-weight long-only. IC t-stat 1.71 over 26 independent
periods, so treat this as a monitored signal rather than a settled result.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pandas as pd  # noqa: E402

from database import get_db_cursor  # noqa: E402
from research.features import prepare_live_cross_section  # noqa: E402
from research.xsmom import (  # noqa: E402
    HORIZON_DAYS,
    MODEL_FEATURES,
    MODEL_VERSION,
    N_DECILES,
    score_xsmom_cross_sections,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("xs-signal")

MIN_PRIOR_UNIVERSE_RATIO = 0.90


def ensure_table() -> None:
    with get_db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cross_sectional_signals (
                signal_id       SERIAL PRIMARY KEY,
                trade_date      DATE         NOT NULL,
                ticker          VARCHAR(16)  NOT NULL,
                model_version   VARCHAR(32)  NOT NULL,
                horizon_days    SMALLINT     NOT NULL,
                raw_score       DOUBLE PRECISION,
                neutral_score   DOUBLE PRECISION,
                percentile      DOUBLE PRECISION,
                decile          SMALLINT,
                side            VARCHAR(5),
                universe_size   INTEGER      NOT NULL,
                created_at      TIMESTAMPTZ  DEFAULT NOW(),
                CONSTRAINT uq_xs_signal UNIQUE (trade_date, ticker, model_version)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_xs_signals_date "
                    "ON cross_sectional_signals (trade_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_xs_signals_ticker "
                    "ON cross_sectional_signals (ticker, trade_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_xs_signals_date_side "
                    "ON cross_sectional_signals (trade_date DESC, side)")


def compute_signal(as_of: str | None) -> pd.DataFrame:
    """Score the latest cross-section. Returns one row per ticker."""
    cross = prepare_live_cross_section(as_of, feature_cols=MODEL_FEATURES)
    if cross.empty:
        return cross

    cross = score_xsmom_cross_sections(cross)
    cross["universe_size"] = len(cross)
    return cross.reset_index(drop=True)


def persist(cross: pd.DataFrame) -> int:
    trade_date = cross["date"].iloc[0]
    if hasattr(trade_date, "date"):
        trade_date = trade_date.date()

    rows = [
        (
            r["date"].date() if hasattr(r["date"], "date") else r["date"],
            r["ticker"],
            MODEL_VERSION,
            HORIZON_DAYS,
            float(r["raw_score"]),
            float(r["neutral_score"]),
            float(r["percentile"]),
            int(r["decile"]),
            r["side"],
            int(r["universe_size"]),
        )
        for _, r in cross.iterrows()
    ]
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT universe_size
            FROM cross_sectional_signals
            WHERE model_version = %s AND trade_date < %s
            ORDER BY trade_date DESC
            LIMIT 1
        """, (MODEL_VERSION, trade_date))
        prior = cur.fetchone()
        if prior and len(rows) < prior["universe_size"] * MIN_PRIOR_UNIVERSE_RATIO:
            raise ValueError(
                f"Refusing to persist {len(rows)} names for {trade_date}: prior universe "
                f"was {prior['universe_size']} and minimum allowed is "
                f"{MIN_PRIOR_UNIVERSE_RATIO:.0%}"
            )

        cur.execute(
            "DELETE FROM cross_sectional_signals WHERE trade_date = %s AND model_version = %s",
            (trade_date, MODEL_VERSION),
        )
        cur.executemany("""
            INSERT INTO cross_sectional_signals (
                trade_date, ticker, model_version, horizon_days,
                raw_score, neutral_score, percentile, decile, side, universe_size
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, ticker, model_version) DO UPDATE SET
                raw_score     = EXCLUDED.raw_score,
                neutral_score = EXCLUDED.neutral_score,
                percentile    = EXCLUDED.percentile,
                decile        = EXCLUDED.decile,
                side          = EXCLUDED.side,
                universe_size = EXCLUDED.universe_size,
                created_at    = NOW()
        """, rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate cross-sectional momentum signal")
    parser.add_argument("--date", default=None, help="As-of date (YYYY-MM-DD); default latest")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    parser.add_argument("--top", type=int, default=10, help="Rows to print per side")
    args = parser.parse_args()

    cross = compute_signal(args.date)
    if cross.empty:
        logger.error("No signal produced; insufficient data.")
        return 1

    trade_date = cross["date"].iloc[0]
    longs = cross[cross["side"] == "LONG"]
    shorts = cross[cross["side"] == "SHORT"]
    logger.info("Signal %s | date=%s | universe=%s | LONG=%s | SHORT=%s",
                MODEL_VERSION, trade_date.date(), len(cross), len(longs), len(shorts))

    cols = ["ticker", "neutral_score", "percentile", "decile", "side"]
    print("\nTop LONG candidates (decile 10):")
    print(longs.head(args.top)[cols].to_string(index=False))
    print("\nTop SHORT candidates (decile 1):")
    print(shorts.tail(args.top)[cols].to_string(index=False))

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return 0

    ensure_table()
    written = persist(cross)
    logger.info("Persisted %s rows to cross_sectional_signals", written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
