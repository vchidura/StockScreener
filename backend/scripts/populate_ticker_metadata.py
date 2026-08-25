"""
Populate metadata columns in selected_tickers using Twelve Data API
and database-computed metrics.

Adds: asset_type, sector, industry, market_cap, market_cap_group,
      beta, avg_volume_90d, float_shares, short_percent,
      institutional_pct, dividend_yield, pe_ratio, exchange.

Sources:
  - Twelve Data /stocks and /etfs endpoints for reference data (type, exchange)
  - Twelve Data /profile endpoint for sector, industry, market cap, etc.
  - Database for avg_volume_90d (computed from stock_prices_hourly)

Usage:
  python scripts/populate_ticker_metadata.py            # all active tickers
  python scripts/populate_ticker_metadata.py --ticker AAPL,MSFT  # specific tickers
  python scripts/populate_ticker_metadata.py --force    # re-fetch even if already populated
  python scripts/populate_ticker_metadata.py --dry-run  # preview without writing
"""

import argparse
import os
import sys
import time
import json
from datetime import datetime
from pathlib import Path

import requests

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import (
    get_db_cursor,
    get_selected_tickers,
    create_selected_tickers_table,
    migrate_selected_tickers_metadata,
)
from http_client import get_session

TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
BASE_URL = "https://api.twelvedata.com"

# Known ETFs for fallback classification
KNOWN_ETFS = {
    "SPY", "QQQ", "DIA", "IWM", "XLE", "XLF", "XLK", "SMH",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLC", "XLB",
    "VTI", "VOO", "VEA", "VWO", "BND", "TLT", "GLD", "SLV",
    "USO", "EEM", "EFA", "HYG", "LQD", "ARKK", "ARKG", "ARKW",
    "SOXX", "IGV", "KWEB", "IBIT",
}


def classify_market_cap(market_cap: int) -> str:
    """Classify market cap into standard groups."""
    if market_cap >= 200_000_000_000:
        return "Mega"
    elif market_cap >= 10_000_000_000:
        return "Large"
    elif market_cap >= 2_000_000_000:
        return "Mid"
    elif market_cap >= 300_000_000:
        return "Small"
    else:
        return "Micro"


_TD_DISABLED = False
_TD_FAILURES = 0


def twelve_data_active() -> bool:
    return not _TD_DISABLED


def fetch_twelve_data_profile(ticker: str, retries: int = 2) -> dict:
    """Fetch profile from Twelve Data /profile endpoint."""
    global _TD_DISABLED, _TD_FAILURES
    if _TD_DISABLED:
        return {}

    url = f"{BASE_URL}/profile"
    params = {"symbol": ticker, "apikey": TWELVE_DATA_API_KEY}
    for attempt in range(retries + 1):
        try:
            resp = get_session().get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = 60
                print(f"    [RATE LIMIT] Waiting {wait}s...")
                time.sleep(wait)
                continue
            # 4xx other than 429 is permanent (e.g. /profile is paid-tier only).
            if 400 <= resp.status_code < 500:
                _TD_FAILURES += 1
                if _TD_FAILURES >= 3 and not _TD_DISABLED:
                    _TD_DISABLED = True
                    print(f"    [INFO] Twelve Data /profile returned "
                          f"{resp.status_code} repeatedly - using Yahoo only")
                return {}
            resp.raise_for_status()
            data = resp.json()
            if "code" in data and data["code"] != 200:
                return {}
            _TD_FAILURES = 0
            return data
        except Exception as e:
            if attempt < retries:
                time.sleep(5)
            else:
                print(f"    [WARN] Twelve Data error for {ticker}: {e}")
    return {}


def get_db_computed_metrics(ticker: str) -> dict:
    """Compute avg_volume and beta from DB price data."""
    with get_db_cursor() as cur:
        cur.execute(
            """
            WITH daily AS (
                SELECT close_price, volume,
                       LAG(close_price) OVER (ORDER BY datetime) AS prev_close
                FROM stock_prices_hourly
                WHERE ticker = %s AND datetime >= NOW() - INTERVAL '90 days'
                ORDER BY datetime
            )
            SELECT
                AVG(volume)::BIGINT AS avg_vol,
                STDDEV(CASE WHEN prev_close > 0
                    THEN (close_price - prev_close) / prev_close
                    ELSE NULL END) AS daily_std
            FROM daily
            WHERE prev_close IS NOT NULL
            """,
            (ticker,),
        )
        row = cur.fetchone()
        avg_vol = int(row["avg_vol"]) if row and row["avg_vol"] else 0
        daily_std = float(row["daily_std"]) if row and row["daily_std"] else None
        beta = None
        if daily_std and daily_std > 0:
            annual_vol = daily_std * (252 ** 0.5)
            beta = round(annual_vol / 0.16, 2)
        return {"avg_volume_90d": avg_vol, "beta": beta}


def safe_float(val):
    """Convert to float or None."""
    if val is None or val == "" or val == "null":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    """Convert to int or None."""
    if val is None or val == "" or val == "null":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


_YAHOO_SESSION = None
_YAHOO_CRUMB = None


def _yahoo_session():
    """Session carrying Yahoo's cookie + crumb. quoteSummary 401s without them."""
    global _YAHOO_SESSION, _YAHOO_CRUMB
    if _YAHOO_SESSION is not None:
        return _YAHOO_SESSION, _YAHOO_CRUMB

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    try:
        s.get("https://fc.yahoo.com", timeout=10)
        r = s.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        _YAHOO_CRUMB = r.text.strip() if r.status_code == 200 else None
    except Exception as exc:
        print(f"    [WARN] Yahoo crumb handshake failed: {exc}")
        _YAHOO_CRUMB = None
    _YAHOO_SESSION = s
    return _YAHOO_SESSION, _YAHOO_CRUMB


def fetch_yahoo_profile(ticker: str) -> dict:
    """Sector/industry via Yahoo direct. Twelve Data's /profile is 403 on the free tier."""
    session, crumb = _yahoo_session()

    if crumb:
        try:
            url = (f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
                   f"?modules=assetProfile%2CquoteType&crumb={crumb}")
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                res = (r.json().get("quoteSummary", {}).get("result") or [None])[0]
                if res:
                    prof = res.get("assetProfile") or {}
                    qt = (res.get("quoteType") or {}).get("quoteType", "")
                    return {
                        "sector": prof.get("sector") or None,
                        "industry": prof.get("industry") or None,
                        "type": "ETF" if str(qt).upper() in ("ETF", "MUTUALFUND") else "Stock",
                    }
        except Exception as exc:
            print(f"    [WARN] Yahoo profile error for {ticker}: {exc}")

    # Slower, but needs no crumb — used when the handshake goes stale.
    try:
        url = (f"https://query1.finance.yahoo.com/v1/finance/search"
               f"?q={ticker}&quotesCount=1&newsCount=0")
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            q = (r.json().get("quotes") or [{}])[0]
            qt = (q.get("quoteType") or "")
            return {
                "sector": q.get("sector") or None,
                "industry": q.get("industry") or None,
                "type": "ETF" if str(qt).upper() in ("ETF", "MUTUALFUND") else "Stock",
            }
    except Exception as exc:
        print(f"    [WARN] Yahoo search error for {ticker}: {exc}")
    return {}


def fetch_metadata(ticker: str) -> dict:
    """Fetch metadata from Yahoo (reference) + DB (computed metrics)."""
    # Yahoo first: it answers in ~50ms. Twelve Data's /profile is paid-tier only,
    # so on the free key it burns retries and rate-limit sleeps for nothing.
    profile = fetch_yahoo_profile(ticker)

    if not profile.get("sector"):
        td = fetch_twelve_data_profile(ticker)
        if td:
            profile = {**td, **{k: v for k, v in profile.items() if v}}

    # 2. DB computed metrics — avg_volume, approx beta
    db_metrics = get_db_computed_metrics(ticker)

    # If profile failed, still handle known ETFs with DB data
    if not profile:
        if ticker in KNOWN_ETFS:
            return {
                "asset_type": "ETF", "sector": None, "industry": None,
                "market_cap": None, "market_cap_group": None,
                "beta": db_metrics.get("beta"), "avg_volume_90d": db_metrics.get("avg_volume_90d"),
                "float_shares": None, "short_percent": None, "institutional_pct": None,
                "dividend_yield": None, "pe_ratio": None, "exchange": None,
            }
        # Still return DB metrics even without profile
        if db_metrics.get("avg_volume_90d"):
            return {
                "asset_type": "Stock", "sector": None, "industry": None,
                "market_cap": None, "market_cap_group": None,
                "beta": db_metrics.get("beta"), "avg_volume_90d": db_metrics.get("avg_volume_90d"),
                "float_shares": None, "short_percent": None, "institutional_pct": None,
                "dividend_yield": None, "pe_ratio": None, "exchange": None,
            }
        return {}

    # Determine asset type
    td_type = (profile or {}).get("type", "")
    if ticker in KNOWN_ETFS or td_type.lower() in ("etf", "fund"):
        asset_type = "ETF"
    else:
        asset_type = "Stock"

    sector = profile.get("sector") or None
    industry = profile.get("industry") or None
    exchange = profile.get("exchange") or None

    return {
        "asset_type": asset_type,
        "sector": sector if sector and sector != "N/A" else None,
        "industry": industry if industry and industry != "N/A" else None,
        "market_cap": None,   # Populated later via yfinance when rate limits reset
        "market_cap_group": None,
        "beta": db_metrics.get("beta"),
        "avg_volume_90d": db_metrics.get("avg_volume_90d"),
        "float_shares": None,
        "short_percent": None,
        "institutional_pct": None,
        "dividend_yield": None,
        "pe_ratio": None,
        "exchange": exchange,
    }


def update_ticker_metadata(ticker: str, meta: dict) -> bool:
    """Update metadata columns for a single ticker."""
    if not meta:
        return False

    set_clauses = []
    values = []
    for col, val in meta.items():
        set_clauses.append(f"{col} = %s")
        values.append(val)
    set_clauses.append("metadata_updated = %s")
    values.append(datetime.now())
    values.append(ticker)

    sql = f"UPDATE selected_tickers SET {', '.join(set_clauses)} WHERE ticker = %s"

    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(sql, values)
    return True


def get_tickers_needing_metadata(tickers: list, force: bool) -> list:
    """Return tickers that don't have metadata yet (or all if force=True)."""
    if force:
        return tickers
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT ticker FROM selected_tickers WHERE ticker = ANY(%s) AND metadata_updated IS NOT NULL",
            (tickers,),
        )
        already_done = {row["ticker"] for row in cursor.fetchall()}
    return [t for t in tickers if t not in already_done]


def main():
    parser = argparse.ArgumentParser(description="Populate ticker metadata from yfinance")
    parser.add_argument("--ticker", type=str, help="Comma-separated tickers (default: all active)")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if already populated")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    parser.add_argument("--batch-size", type=int, default=50, help="Pause every N tickers (default: 50)")
    args = parser.parse_args()

    # Ensure schema is up to date
    create_selected_tickers_table()
    migrate_selected_tickers_metadata()

    # Determine which tickers to process
    if args.ticker:
        tickers = [t.strip().upper() for t in args.ticker.split(",")]
    else:
        tickers = get_selected_tickers()

    tickers = get_tickers_needing_metadata(tickers, args.force)

    print(f"Tickers to process: {len(tickers)}")
    if not tickers:
        print("All tickers already have metadata. Use --force to re-fetch.")
        return

    import yfinance as yf  # noqa: F401 — validate import early

    success = 0
    failed = []
    for i, ticker in enumerate(tickers):
        meta = fetch_metadata(ticker)
        if meta:
            if args.dry_run:
                cap_str = f"${meta.get('market_cap', 0) / 1e9:.1f}B" if meta.get("market_cap") else "N/A"
                print(
                    f"  [{i+1}/{len(tickers)}] {ticker:<8} "
                    f"type={meta.get('asset_type', '?'):<6} "
                    f"sector={str(meta.get('sector', 'N/A')):<25} "
                    f"cap={cap_str:<10} "
                    f"group={str(meta.get('market_cap_group', 'N/A')):<6} "
                    f"beta={meta.get('beta', 'N/A')}"
                )
            else:
                update_ticker_metadata(ticker, meta)
                print(f"  [{i+1}/{len(tickers)}] {ticker:<8} ✓ {meta.get('asset_type', '?')} | {meta.get('sector', 'N/A')} | {meta.get('market_cap_group', 'N/A')}")
            success += 1
        else:
            failed.append(ticker)
            print(f"  [{i+1}/{len(tickers)}] {ticker:<8} ✗ no data")

        # Twelve Data free plan allows 8 calls/min; no need to wait once it is
        # disabled, since Yahoo is the only source being hit.
        if i + 1 < len(tickers) and twelve_data_active():
            time.sleep(8)

    print(f"\nDone: {success} updated, {len(failed)} failed")
    if failed:
        print(f"Failed tickers: {', '.join(failed)}")

    if not args.dry_run and success > 0:
        # Print summary stats
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE metadata_updated IS NOT NULL) AS has_meta,
                    COUNT(*) FILTER (WHERE metadata_updated IS NULL) AS missing_meta,
                    COUNT(DISTINCT sector) AS sectors,
                    COUNT(DISTINCT market_cap_group) AS cap_groups,
                    COUNT(*) FILTER (WHERE asset_type = 'ETF') AS etfs,
                    COUNT(*) FILTER (WHERE asset_type = 'Stock') AS stocks
                FROM selected_tickers WHERE is_active = TRUE
            """)
            s = cur.fetchone()
            print(f"\nTable stats: {s['has_meta']} with metadata, {s['missing_meta']} missing")
            print(f"  {s['stocks']} stocks, {s['etfs']} ETFs, {s['sectors']} sectors, {s['cap_groups']} cap groups")


if __name__ == "__main__":
    main()
