"""
Add more tickers to selected_tickers table.

Scans tickers already in the DB (stock_prices_hourly) that are NOT yet
in selected_tickers, ranks them by composite score, and adds the top N.

Unlike filter_top_tickers.py, this script:
  - Does NOT deactivate existing selected tickers
  - Includes ETFs (SPY, QQQ, etc.)
  - Uses a lower min avg volume threshold (100k) to capture more candidates

Ranking factors (weighted composite score):
  DB-only mode:
    1. Avg Daily Volume    (50%) - liquidity
    2. Avg Dollar Volume   (30%) - notional liquidity (price x volume)
    3. ATR% (volatility)   (20%) - prefer movers for swing/gap strategies

  Enriched mode (--enrich):
    1. Market Cap           (35%)
    2. Avg Daily Volume     (25%)
    3. Avg Dollar Volume    (15%)
    4. ATR%                 (10%)
    5. Institutional %      (10%)
    6. Beta                  (5%)

Hard filters:
  - Min avg daily volume: 100,000 shares
  - Min avg close price: $5 (avoid penny stocks)
  - Min data points: 20 trading days in last 90 days
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor, create_selected_tickers_table, get_selected_tickers

DEFAULT_ADD_N = 215
MIN_AVG_VOLUME = 100_000
MIN_AVG_PRICE = 5.0

WEIGHTS_DB_ONLY = {
    "avg_volume": 0.50,
    "dollar_volume": 0.30,
    "atr_pct": 0.20,
}

WEIGHTS_ENRICHED = {
    "market_cap": 0.35,
    "avg_volume": 0.25,
    "dollar_volume": 0.15,
    "atr_pct": 0.10,
    "inst_pct": 0.10,
    "beta": 0.05,
}


def fetch_unselected_ticker_metrics(existing: set) -> pd.DataFrame:
    """
    Query stock_prices_hourly for per-ticker metrics (last 90 days),
    excluding tickers already in selected_tickers.
    """
    query = """
        SELECT
            ticker,
            AVG(volume)::BIGINT                          AS avg_volume,
            AVG(close_price)                             AS avg_close,
            (AVG(close_price) * AVG(volume))::NUMERIC    AS dollar_volume,
            AVG(CASE WHEN close_price > 0
                     THEN (high - low) / close_price * 100
                     ELSE 0 END)                         AS atr_pct,
            COUNT(*)                                     AS data_days,
            MIN(datetime)                                AS first_date,
            MAX(datetime)                                AS last_date
        FROM stock_prices_hourly
        WHERE datetime >= NOW() - INTERVAL '90 days'
        GROUP BY ticker
        HAVING COUNT(*) >= 20
        ORDER BY ticker
    """
    with get_db_cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for col in ["avg_volume", "avg_close", "dollar_volume", "atr_pct"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Exclude already-selected tickers
    df = df[~df["ticker"].isin(existing)]

    # Hard filters
    df = df[df["avg_volume"] >= MIN_AVG_VOLUME]
    df = df[df["avg_close"] >= MIN_AVG_PRICE]

    return df.reset_index(drop=True)


def enrich_with_yfinance(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich DB metrics with yfinance .info: market_cap, beta, inst%."""
    import yfinance as yf

    market_caps, betas, inst_pcts = [], [], []

    total = len(df)
    for i, row in df.iterrows():
        ticker = row["ticker"]
        try:
            info = yf.Ticker(ticker).info
            market_caps.append(info.get("marketCap") or 0)
            betas.append(abs(info.get("beta") or 0))
            inst_pcts.append(info.get("heldPercentInstitutions") or 0)
        except Exception:
            market_caps.append(0)
            betas.append(0)
            inst_pcts.append(0)

        if (i + 1) % 25 == 0:
            print(f"    Enriched {i + 1}/{total} tickers...")
            time.sleep(0.3)

    df["market_cap"] = market_caps
    df["beta"] = betas
    df["inst_pct"] = inst_pcts

    # Drop tickers where yfinance returned no market cap
    df = df[df["market_cap"] > 0]
    return df.reset_index(drop=True)


def rank_tickers(df: pd.DataFrame, top_n: int, enriched: bool) -> pd.DataFrame:
    """Rank tickers by weighted composite score and return top N."""
    weights = WEIGHTS_ENRICHED if enriched else WEIGHTS_DB_ONLY

    for col in weights:
        if col not in df.columns:
            continue
        col_max = df[col].max()
        col_min = df[col].min()
        if col_max > col_min:
            df[f"{col}_score"] = (df[col] - col_min) / (col_max - col_min)
        else:
            df[f"{col}_score"] = 0.5

    df["composite_score"] = sum(
        df.get(f"{col}_score", 0) * w for col, w in weights.items()
    )

    df = df.sort_values("composite_score", ascending=False).head(top_n).reset_index(drop=True)
    return df


def add_selected_tickers(tickers: list) -> int:
    """ADD tickers to selected_tickers without deactivating existing ones."""
    create_selected_tickers_table()

    with get_db_cursor(dict_cursor=False) as cursor:
        added = 0
        for ticker in tickers:
            cursor.execute(
                """
                INSERT INTO selected_tickers (ticker, is_active)
                VALUES (%s, TRUE)
                ON CONFLICT (ticker) DO UPDATE SET is_active = TRUE
                """,
                (ticker,),
            )
            added += 1
    return added


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add more tickers to selected_tickers from DB pool (includes ETFs)"
    )
    parser.add_argument("--add", type=int, default=DEFAULT_ADD_N,
                        help=f"Number of new tickers to add (default: {DEFAULT_ADD_N})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results without updating DB")
    parser.add_argument("--enrich", action="store_true",
                        help="Enrich with yfinance .info (market cap, beta, inst%%). Slower.")
    args = parser.parse_args()

    # Get existing selected tickers
    existing = set(get_selected_tickers())
    print(f"Currently selected tickers: {len(existing)}")

    print(f"\nStep 1: Querying DB for unselected ticker metrics (last 90 days)...")
    print(f"  Hard filters: avg_volume >= {MIN_AVG_VOLUME:,}, avg_close >= ${MIN_AVG_PRICE}")
    print(f"  Including ETFs: YES")
    df = fetch_unselected_ticker_metrics(existing)
    print(f"  {len(df)} unselected tickers passed hard filters")

    if df.empty:
        print("No tickers passed filters. Exiting.")
        return

    # Detect ETFs in the pool
    known_etfs = {"SPY", "QQQ", "DIA", "IWM", "XLE", "XLF", "XLK", "SMH",
                  "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLC", "XLB",
                  "VTI", "VOO", "VEA", "VWO", "BND", "TLT", "GLD", "SLV",
                  "USO", "EEM", "EFA", "HYG", "LQD", "ARKK", "ARKG", "ARKW",
                  "SOXX", "IGV", "KWEB", "IBIT", "MSTR"}
    etfs_in_pool = df[df["ticker"].isin(known_etfs)]
    if len(etfs_in_pool) > 0:
        print(f"  ETFs in pool: {', '.join(sorted(etfs_in_pool['ticker'].tolist()))}")

    enriched = False
    if args.enrich:
        print(f"\nStep 2: Enriching {len(df)} tickers with yfinance .info...")
        df = enrich_with_yfinance(df)
        enriched = True
        print(f"  {len(df)} tickers have valid market cap data")

    step = "3" if enriched else "2"
    actual_add = min(args.add, len(df))
    weights = WEIGHTS_ENRICHED if enriched else WEIGHTS_DB_ONLY
    print(f"\nStep {step}: Ranking top {actual_add} by composite score...")
    print(f"  Weights: {weights}")

    ranked_df = rank_tickers(df, actual_add, enriched)

    # Print results
    print(f"\nTop {len(ranked_df)} NEW tickers to add:")
    print("-" * 100)

    if enriched:
        print(f"{'Rank':<6} {'Ticker':<8} {'Market Cap':>14} {'Avg Volume':>14} {'$Volume':>14} {'ATR%':>8} {'Score':>8}")
        print("-" * 100)
        for idx, row in ranked_df.iterrows():
            mcap = f"${row['market_cap'] / 1e9:.1f}B" if row['market_cap'] > 0 else "N/A"
            print(
                f"{idx + 1:<6} {row['ticker']:<8} "
                f"{mcap:>14} "
                f"{row['avg_volume'] / 1e6:>10.1f}M "
                f"{row['dollar_volume'] / 1e9:>10.1f}B "
                f"{row['atr_pct']:>8.2f} "
                f"{row['composite_score']:>8.4f}"
            )
    else:
        print(f"{'Rank':<6} {'Ticker':<8} {'Type':<6} {'Avg Volume':>14} {'$Volume':>14} {'ATR%':>8} {'Avg Close':>12} {'Score':>8}")
        print("-" * 100)
        for idx, row in ranked_df.iterrows():
            is_etf = "ETF" if row['ticker'] in known_etfs else "Stock"
            print(
                f"{idx + 1:<6} {row['ticker']:<8} {is_etf:<6} "
                f"{row['avg_volume'] / 1e6:>10.1f}M "
                f"{row['dollar_volume'] / 1e9:>10.1f}B "
                f"{row['atr_pct']:>8.2f} "
                f"${row['avg_close']:>10.2f} "
                f"{row['composite_score']:>8.4f}"
            )

    selected = ranked_df["ticker"].tolist()
    etf_count = sum(1 for t in selected if t in known_etfs)
    stock_count = len(selected) - etf_count
    print(f"\nSummary: {len(selected)} tickers ({stock_count} stocks + {etf_count} ETFs)")

    if args.dry_run:
        print(f"\n[DRY RUN] Would add {len(selected)} tickers to selected_tickers. No DB changes made.")
    else:
        step_n = "4" if enriched else "3"
        print(f"\nStep {step_n}: Adding to selected_tickers table...")
        count = add_selected_tickers(selected)
        print(f"  Done. {count} tickers added/activated.")
        print(f"  Total selected_tickers now: {len(existing) + count}")


if __name__ == "__main__":
    main()
