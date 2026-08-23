"""
Build the initial trading universe directly from Polygon.io (massive.com) reference
data — no pre-existing price history required (unlike add_more_tickers.py, which
needs stock_prices_hourly to already be populated).

Pipeline:
  1. List all active US common stocks (v3/reference/tickers, type=CS)
  2. Pull grouped daily bars (v2/aggs/grouped) for the last N trading days to
     compute average price/volume for every US stock in a handful of API calls
  3. Hard filter: min avg price, min avg daily volume
  4. For survivors, fetch market cap (v3/reference/tickers/{ticker}) and filter
  5. Rank by average dollar volume, keep the top --target-size
  6. Always add a curated list of major ETFs (bypass the stock filters — ETFs
     don't have a "market cap" in the same sense)
  7. Insert the final list into selected_tickers with metadata populated

Usage:
    python scripts/discover_universe_polygon.py --dry-run
    python scripts/discover_universe_polygon.py --target-size 200 \\
        --min-market-cap 300000000 --min-avg-volume 1000000 --min-price 5
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import requests

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

import os
from database import get_db_cursor, create_selected_tickers_table, migrate_selected_tickers_metadata

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
BASE_URL = "https://api.polygon.io"
REQUEST_TIMEOUT = 30

# Major ETFs added unconditionally, on top of --target-size stock picks
KNOWN_ETFS = {
    "SPY", "QQQ", "DIA", "IWM", "XLE", "XLF", "XLK", "SMH",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLC", "XLB",
    "VTI", "VOO", "VEA", "VWO", "BND", "TLT", "GLD", "SLV",
    "USO", "EEM", "EFA", "HYG", "LQD", "ARKK", "ARKG", "ARKW",
    "SOXX", "IGV", "KWEB", "IBIT",
}


def classify_market_cap(market_cap: float) -> str:
    if market_cap >= 200_000_000_000:
        return "Mega"
    elif market_cap >= 10_000_000_000:
        return "Large"
    elif market_cap >= 2_000_000_000:
        return "Mid"
    elif market_cap >= 300_000_000:
        return "Small"
    return "Micro"


def _require_key():
    if not POLYGON_API_KEY:
        print("ERROR: POLYGON_API_KEY not set in backend/.env")
        sys.exit(1)


def fetch_active_common_stocks() -> set[str]:
    """List every active US common stock ticker via v3/reference/tickers."""
    tickers: set[str] = set()
    url = f"{BASE_URL}/v3/reference/tickers"
    params = {"market": "stocks", "type": "CS", "active": "true", "limit": 1000, "apiKey": POLYGON_API_KEY}

    page = 0
    while url:
        page += 1
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print(f"  Reference tickers HTTP {resp.status_code}: {resp.text[:200]}")
            break
        payload = resp.json()
        for row in payload.get("results", []) or []:
            tickers.add(row["ticker"])
        print(f"  Page {page}: {len(tickers)} tickers so far...", end="\r")
        next_url = payload.get("next_url")
        url = next_url
        params = {"apiKey": POLYGON_API_KEY}

    print(f"\n  Total active US common stocks: {len(tickers)}")
    return tickers


def fetch_grouped_daily(trade_date: str) -> dict[str, dict]:
    """One call: close price + volume for every US stock on trade_date."""
    url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{trade_date}"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        return {}
    payload = resp.json()
    return {row["T"]: {"close": row["c"], "volume": row["v"]} for row in payload.get("results", []) or []}


def compute_avg_price_volume(lookback_days: int) -> dict[str, dict]:
    """Average close price and volume per ticker over the last `lookback_days` trading days."""
    accum: dict[str, dict] = {}
    counts: dict[str, int] = {}
    found_days = 0
    cursor_date = date.today() - timedelta(days=1)  # yesterday: today's session may be incomplete/delayed
    attempts = 0

    while found_days < lookback_days and attempts < lookback_days * 3:
        attempts += 1
        date_str = cursor_date.strftime("%Y-%m-%d")
        day_data = fetch_grouped_daily(date_str)
        cursor_date -= timedelta(days=1)
        if not day_data:
            continue  # weekend/holiday/no data
        found_days += 1
        print(f"  Grouped daily {date_str}: {len(day_data)} tickers ({found_days}/{lookback_days} days)")
        for ticker, row in day_data.items():
            a = accum.setdefault(ticker, {"close_sum": 0.0, "volume_sum": 0.0})
            a["close_sum"] += row["close"]
            a["volume_sum"] += row["volume"]
            counts[ticker] = counts.get(ticker, 0) + 1

    return {
        ticker: {
            "avg_price": a["close_sum"] / counts[ticker],
            "avg_volume": a["volume_sum"] / counts[ticker],
        }
        for ticker, a in accum.items()
    }


def fetch_market_cap(ticker: str) -> dict | None:
    """Ticker overview → market cap, sector, exchange."""
    url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    r = resp.json().get("results") or {}
    if "market_cap" not in r:
        return None
    return {
        "market_cap": r.get("market_cap"),
        "sector": r.get("sic_description"),
        "exchange": r.get("primary_exchange"),
    }


def main():
    parser = argparse.ArgumentParser(description="Build initial trading universe from Polygon reference data")
    parser.add_argument("--target-size", type=int, default=200)
    parser.add_argument("--min-market-cap", type=float, default=300_000_000)
    parser.add_argument("--min-avg-volume", type=float, default=1_000_000)
    parser.add_argument("--min-price", type=float, default=5.0)
    parser.add_argument("--lookback-days", type=int, default=20, help="Trading days used to average price/volume")
    parser.add_argument("--no-etfs", action="store_true", help="Skip adding the curated ETF list")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    _require_key()

    print(f"Step 1: Listing active US common stocks...")
    stock_universe = fetch_active_common_stocks()

    print(f"\nStep 2: Averaging price/volume over last {args.lookback_days} trading days...")
    stats = compute_avg_price_volume(args.lookback_days)

    print(f"\nStep 3: Applying hard filters (price >= ${args.min_price}, "
          f"avg volume >= {args.min_avg_volume:,.0f})...")
    survivors = [
        ticker for ticker in stock_universe
        if ticker in stats
        and stats[ticker]["avg_price"] >= args.min_price
        and stats[ticker]["avg_volume"] >= args.min_avg_volume
    ]
    print(f"  {len(survivors)} tickers pass price/volume filters")

    print(f"\nStep 4: Fetching market cap for {len(survivors)} survivors "
          f"(min ${args.min_market_cap:,.0f})...")
    enriched = []
    checked = 0
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fetch_market_cap, ticker): ticker for ticker in survivors}
        for future in as_completed(futures):
            ticker = futures[future]
            checked += 1
            info = future.result()
            if info and info.get("market_cap") and info["market_cap"] >= args.min_market_cap:
                enriched.append({
                    "ticker": ticker,
                    "avg_price": stats[ticker]["avg_price"],
                    "avg_volume": stats[ticker]["avg_volume"],
                    "dollar_volume": stats[ticker]["avg_price"] * stats[ticker]["avg_volume"],
                    **info,
                })
            if checked % 100 == 0:
                print(f"  Checked {checked}/{len(survivors)}, {len(enriched)} pass market cap filter so far...")

    print(f"  {len(enriched)} tickers pass all stock filters")

    enriched.sort(key=lambda r: r["dollar_volume"], reverse=True)
    final_stocks = enriched[: args.target_size]
    print(f"\nStep 5: Selected top {len(final_stocks)} stocks by average dollar volume")

    etfs = sorted(KNOWN_ETFS) if not args.no_etfs else []
    print(f"Step 6: Adding {len(etfs)} curated ETFs" if etfs else "Step 6: Skipping ETFs (--no-etfs)")

    if args.dry_run:
        print("\n--- DRY RUN: would insert into selected_tickers ---")
        for row in final_stocks[:20]:
            print(f"  {row['ticker']:6s} cap=${row['market_cap']:,.0f}  "
                  f"avg_vol={row['avg_volume']:,.0f}  avg_price=${row['avg_price']:.2f}  "
                  f"sector={row.get('sector')}")
        if len(final_stocks) > 20:
            print(f"  ... and {len(final_stocks) - 20} more")
        print(f"  Plus ETFs: {', '.join(etfs)}")
        return

    create_selected_tickers_table()
    migrate_selected_tickers_metadata()

    with get_db_cursor(dict_cursor=False) as cur:
        for row in final_stocks:
            cur.execute(
                """
                INSERT INTO selected_tickers
                    (ticker, is_active, asset_type, market_cap, market_cap_group,
                     avg_volume_90d, sector, exchange, metadata_updated)
                VALUES (%s, TRUE, 'Stock', %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    is_active = TRUE,
                    asset_type = 'Stock',
                    market_cap = EXCLUDED.market_cap,
                    market_cap_group = EXCLUDED.market_cap_group,
                    avg_volume_90d = EXCLUDED.avg_volume_90d,
                    sector = EXCLUDED.sector,
                    exchange = EXCLUDED.exchange,
                    metadata_updated = NOW()
                """,
                (
                    row["ticker"], row["market_cap"], classify_market_cap(row["market_cap"]),
                    int(row["avg_volume"]), row.get("sector"), row.get("exchange"),
                ),
            )
        for ticker in etfs:
            cur.execute(
                """
                INSERT INTO selected_tickers (ticker, is_active, asset_type, metadata_updated)
                VALUES (%s, TRUE, 'ETF', NOW())
                ON CONFLICT (ticker) DO UPDATE SET is_active = TRUE, asset_type = 'ETF', metadata_updated = NOW()
                """,
                (ticker,),
            )

    print(f"\nDone — inserted/updated {len(final_stocks)} stocks + {len(etfs)} ETFs into selected_tickers")


if __name__ == "__main__":
    main()
