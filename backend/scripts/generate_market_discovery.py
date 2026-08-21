#!/usr/bin/env python3
"""Generate and persist shadow market-discovery states."""
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
from research.discovery_states import (  # noqa: E402
    DISCOVERY_MODEL_VERSION,
    classify_discovery_states,
)
from research.features import load_daily_panel  # noqa: E402

logger = logging.getLogger("market-discovery")
MIN_PRIOR_UNIVERSE_RATIO = 0.90
CURRENT_LOOKBACK_CALENDAR_DAYS = 550
DISCOVERY_RETENTION_SESSIONS = 252


def ensure_table() -> None:
    with get_db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_discovery_states (
                discovery_id BIGSERIAL PRIMARY KEY,
                trade_date DATE NOT NULL,
                ticker VARCHAR(16) NOT NULL,
                model_version VARCHAR(32) NOT NULL,
                state VARCHAR(32) NOT NULL,
                validation_status VARCHAR(32) NOT NULL,
                activity_percentile DOUBLE PRECISION,
                echo_percentile DOUBLE PRECISION,
                older_momentum_percentile DOUBLE PRECISION,
                long_momentum_percentile DOUBLE PRECISION,
                recent_21d_percentile DOUBLE PRECISION,
                recent_21d_return DOUBLE PRECISION,
                recent_5d_return DOUBLE PRECISION,
                close_price DOUBLE PRECISION,
                sma_20 DOUBLE PRECISION,
                sma_50 DOUBLE PRECISION,
                higher_swing_high BOOLEAN,
                higher_swing_low BOOLEAN,
                evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (trade_date, ticker, model_version)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_discovery_date_state "
                    "ON market_discovery_states (trade_date DESC, state)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_discovery_ticker_date "
                    "ON market_discovery_states (ticker, trade_date DESC)")


def compute(as_of: str | None = None) -> pd.DataFrame:
    with get_db_cursor() as cur:
        if as_of:
            cur.execute(
                "SELECT MAX(datetime)::date AS latest FROM stock_prices_daily "
                "WHERE datetime::date <= %s",
                (as_of,),
            )
        else:
            cur.execute("SELECT MAX(datetime)::date AS latest FROM stock_prices_daily")
        row = cur.fetchone()
    latest = row["latest"] if row else None
    if latest is None:
        return pd.DataFrame()
    start = pd.Timestamp(latest) - pd.Timedelta(days=CURRENT_LOOKBACK_CALENDAR_DAYS)
    panel = load_daily_panel(start.date().isoformat(), latest.isoformat())
    return classify_discovery_states(panel)


def persist(states: pd.DataFrame) -> int:
    if states.empty:
        return 0
    trade_date = states["trade_date"].iloc[0]
    rows = [
        (
            row["trade_date"], row["ticker"], DISCOVERY_MODEL_VERSION,
            row["state"], row["validation_status"],
            _float_or_none(row["activity_percentile"]),
            _float_or_none(row["echo_percentile"]),
            _float_or_none(row["older_momentum_percentile"]),
            _float_or_none(row["long_momentum_percentile"]),
            _float_or_none(row["recent_21d_percentile"]),
            _float_or_none(row["recent_21d_return"]),
            _float_or_none(row["recent_5d_return"]),
            _float_or_none(row["close"]), _float_or_none(row["sma20"]),
            _float_or_none(row["sma50"]),
            bool(row["higher_swing_high"]) if pd.notna(row["higher_swing_high"]) else None,
            bool(row["higher_swing_low"]) if pd.notna(row["higher_swing_low"]) else None,
            row["evidence"],
        )
        for _, row in states.iterrows()
    ]
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS names
            FROM market_discovery_states
            WHERE model_version = %s AND trade_date = (
                SELECT MAX(trade_date) FROM market_discovery_states
                WHERE model_version = %s AND trade_date < %s
            )
        """, (DISCOVERY_MODEL_VERSION, DISCOVERY_MODEL_VERSION, trade_date))
        prior = cur.fetchone()
        if prior and prior["names"] and len(rows) < prior["names"] * MIN_PRIOR_UNIVERSE_RATIO:
            raise ValueError(
                f"Refusing discovery snapshot of {len(rows)} names; prior had {prior['names']}"
            )
        cur.execute(
            "DELETE FROM market_discovery_states WHERE trade_date=%s AND model_version=%s",
            (trade_date, DISCOVERY_MODEL_VERSION),
        )
        cur.executemany("""
            INSERT INTO market_discovery_states (
                trade_date, ticker, model_version, state, validation_status,
                activity_percentile, echo_percentile, older_momentum_percentile,
                long_momentum_percentile, recent_21d_percentile, recent_21d_return,
                recent_5d_return, close_price, sma_20, sma_50,
                higher_swing_high, higher_swing_low, evidence
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
            )
        """, rows)
        cur.execute("""
            WITH retained_dates AS (
                SELECT DISTINCT trade_date
                FROM market_discovery_states
                ORDER BY trade_date DESC
                LIMIT %s
            ), cutoff AS (
                SELECT MIN(trade_date) AS trade_date FROM retained_dates
            )
            DELETE FROM market_discovery_states
            WHERE trade_date < (SELECT trade_date FROM cutoff)
        """, (DISCOVERY_RETENTION_SESSIONS,))
    return len(rows)


def _float_or_none(value):
    return float(value) if pd.notna(value) else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate shadow market-discovery states")
    parser.add_argument("--date", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    states = compute(args.date)
    if states.empty:
        logger.error("No discovery states produced")
        return 1
    print("date={} names={}".format(states["trade_date"].iloc[0], len(states)))
    print(states["state"].value_counts().to_string())
    print("\nposition overlay")
    print(states.groupby(
        ["trend_state", "extension_risk", "reversal_trigger"], dropna=False
    ).size().sort_values(ascending=False).to_string())
    focus = states[
        (states["state"] != "NEUTRAL") | (states["extension_risk"] != "NORMAL")
    ]
    print("\n" + focus[[
        "ticker", "state", "trend_state", "extension_risk", "reversal_trigger",
        "validation_status", "activity_percentile", "echo_percentile",
        "recent_21d_percentile", "recent_21d_return",
    ]].to_string(index=False))
    if not args.dry_run:
        ensure_table()
        print("persisted", persist(states), "rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
