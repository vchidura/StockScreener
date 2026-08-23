"""Repopulate selected_tickers sector/industry from Polygon SIC codes.

The discovery script wrote Polygon's `sic_description` into `sector`, producing well over a
hundred industry-level buckets. This moves that text to `industry`, stores the raw `sic_code`,
and derives `sector` as one of the eleven GICS-style names. ETFs are labelled `ETF` rather
than assigned an operating sector, so baskets never enter sector neutralization or breadth.
"""
from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor, migrate_selected_tickers_metadata  # noqa: E402
from research.gics_sectors import (  # noqa: E402
    ETF_SECTOR, MANUAL_SECTORS, UNCLASSIFIED_SECTOR, sector_for_sic,
)

load_dotenv(BACKEND_DIR / ".env")
API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"
MAX_WORKERS = 20


def fetch_reference(ticker: str) -> dict:
    """Return sic_code and sic_description for one ticker."""
    try:
        response = requests.get(
            f"{BASE_URL}/v3/reference/tickers/{ticker}",
            params={"apiKey": API_KEY}, timeout=20,
        )
        if response.status_code != 200:
            return {"ticker": ticker, "name": None, "sic_code": None, "sic_description": None}
        result = response.json().get("results") or {}
        return {
            "ticker": ticker,
            "name": result.get("name"),
            "sic_code": result.get("sic_code"),
            "sic_description": result.get("sic_description"),
        }
    except requests.RequestException:
        return {"ticker": ticker, "name": None, "sic_code": None, "sic_description": None}


def load_universe() -> list[dict]:
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT ticker, asset_type, sector, industry
            FROM selected_tickers ORDER BY ticker
        """)
        return [dict(row) for row in cur.fetchall()]


def persist(rows: list[tuple]) -> int:
    with get_db_cursor() as cur:
        cur.executemany("""
            UPDATE selected_tickers
               SET sic_code = %s, industry = %s, sector = %s, metadata_updated = NOW()
             WHERE ticker = %s
        """, rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not API_KEY:
        print("POLYGON_API_KEY is required", file=sys.stderr)
        return 1

    migrate_selected_tickers_metadata()
    universe = load_universe()
    stocks = [row["ticker"] for row in universe if row["asset_type"] != "ETF"]
    etfs = [row for row in universe if row["asset_type"] == "ETF"]
    print(f"universe={len(universe)} stocks={len(stocks)} etfs={len(etfs)}", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        reference = list(pool.map(fetch_reference, stocks))

    updates: list[tuple] = []
    counts: dict[str, int] = {}
    overridden: list[tuple[str, str, str]] = []
    unmapped: list[tuple[str, object, object, object]] = []
    for row in reference:
        sector = sector_for_sic(row["sic_code"])
        if sector is None and row["ticker"] in MANUAL_SECTORS:
            sector = MANUAL_SECTORS[row["ticker"]]
            overridden.append((row["ticker"], row["name"] or "?", sector))
        if sector is None:
            sector = UNCLASSIFIED_SECTOR
            unmapped.append((
                row["ticker"], row["name"], row["sic_code"], row["sic_description"],
            ))
        counts[sector] = counts.get(sector, 0) + 1
        updates.append((row["sic_code"], row["sic_description"], sector, row["ticker"]))

    for row in etfs:
        # Keep any existing descriptive text; the basket gets no operating sector.
        updates.append((None, row["industry"] or row["sector"], ETF_SECTOR, row["ticker"]))
    counts[ETF_SECTOR] = len(etfs)

    print("\nsector distribution:")
    for sector, count in sorted(counts.items(), key=lambda item: -item[1]):
        print(f"  {count:>4}  {sector}")

    if overridden:
        print(f"\nmanual overrides ({len(overridden)}) — confirm each name matches:")
        for ticker, name, sector in overridden:
            print(f"  {ticker:<8} {sector:<24} {name}")

    if unmapped:
        print(f"\nunmapped ({len(unmapped)}):")
        for ticker, name, code, description in unmapped:
            print(f"  {ticker:<8} sic={code} {description} | {name}")

    if args.dry_run:
        print("\n[dry-run] no rows written")
        return 0

    print(f"\nupdated {persist(updates)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
