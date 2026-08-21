"""Cross-table data coverage checker — finds missing days and incomplete bars.

Reports:
  1. Daily: trading days with fewer tickers than expected
  2. Hourly: from its min date to now, days missing or with incomplete bars (expect 7/ticker)
  3. Intraday 5m: from its min date to now, days missing or with incomplete bars (expect 78/ticker)
  4. Cross-table: daily dates missing from hourly or 5m

Usage:
    python scripts/check_data_coverage.py              # full report
    python scripts/check_data_coverage.py --days 30    # only last 30 days
    python scripts/check_data_coverage.py --ticker AAPL  # single ticker detail
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor

EXPECTED_HOURLY_BARS = 7    # 9:30..15:30
EXPECTED_5M_BARS = 78       # 9:30..15:55


def check_daily(days: int = None):
    """Check daily table for missing tickers on trading days."""
    print("=" * 70)
    print("DAILY COVERAGE")
    print("=" * 70)

    cutoff = f"AND datetime >= now() - interval '{days} days'" if days else ""

    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(f"""
            SELECT MIN(datetime::date), MAX(datetime::date), COUNT(DISTINCT ticker),
                   COUNT(DISTINCT datetime::date)
            FROM stock_prices_daily WHERE 1=1 {cutoff}
        """)
        r = cur.fetchone()
        print(f"Range: {r[0]} to {r[1]} | {r[2]} tickers | {r[3]} trading days")

        # Find the typical ticker count (mode)
        cur.execute(f"""
            SELECT COUNT(DISTINCT ticker) as n
            FROM stock_prices_daily WHERE 1=1 {cutoff}
            GROUP BY datetime::date
            ORDER BY COUNT(*) DESC LIMIT 1
        """)
        typical = cur.fetchone()[0]
        print(f"Typical tickers/day: {typical}")

        # Days with significantly fewer tickers
        cur.execute(f"""
            SELECT datetime::date as dt, COUNT(DISTINCT ticker) as n
            FROM stock_prices_daily WHERE 1=1 {cutoff}
            GROUP BY dt
            HAVING COUNT(DISTINCT ticker) < %s * 0.95
            ORDER BY dt DESC
        """, (typical,))
        rows = cur.fetchall()
        if rows:
            print(f"\nDays with <95% ticker coverage ({len(rows)} days):")
            for r in rows[:15]:
                print(f"  {r[0]}: {r[1]}/{typical} tickers ({r[1]*100//typical}%)")
            if len(rows) > 15:
                print(f"  ...and {len(rows)-15} more")
        else:
            print("All trading days have full ticker coverage")


def check_hourly(days: int = None):
    """Check hourly table for missing days and incomplete bars."""
    print("\n" + "=" * 70)
    print("HOURLY COVERAGE (expect 7 bars/ticker/day)")
    print("=" * 70)

    cutoff = f"AND h.datetime >= now() - interval '{days} days'" if days else ""
    cutoff_simple = f"AND datetime >= now() - interval '{days} days'" if days else ""

    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(f"""
            SELECT MIN(datetime::date), MAX(datetime::date), COUNT(DISTINCT ticker),
                   COUNT(DISTINCT datetime::date)
            FROM stock_prices_hourly WHERE 1=1 {cutoff_simple}
        """)
        r = cur.fetchone()
        h_min, h_max = r[0], r[1]
        print(f"Range: {h_min} to {h_max} | {r[2]} tickers | {r[3]} trading days")

        # Trading days (from daily) that are missing from hourly
        cur.execute(f"""
            SELECT d.dt FROM (
                SELECT DISTINCT datetime::date as dt FROM stock_prices_daily
                WHERE datetime::date >= %s AND datetime::date <= %s {cutoff_simple.replace('datetime', 'datetime::date')}
            ) d
            LEFT JOIN (
                SELECT DISTINCT datetime::date as dt FROM stock_prices_hourly
                WHERE 1=1 {cutoff_simple}
            ) h ON d.dt = h.dt
            WHERE h.dt IS NULL
            ORDER BY d.dt DESC
        """, (h_min, h_max))
        missing = cur.fetchall()
        if missing:
            print(f"\nMissing hourly data for {len(missing)} trading days:")
            for r in missing[:15]:
                print(f"  {r[0]}")
            if len(missing) > 15:
                print(f"  ...and {len(missing)-15} more")
        else:
            print("No missing trading days")

        # Days with incomplete bars (avg bars/ticker != 7)
        cur.execute(f"""
            SELECT datetime::date as dt, COUNT(DISTINCT ticker) as tickers,
                   COUNT(*) as total_bars,
                   ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ticker), 0), 1) as avg_bars
            FROM stock_prices_hourly
            WHERE 1=1 {cutoff_simple}
            GROUP BY dt
            HAVING ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ticker), 0), 1) != {EXPECTED_HOURLY_BARS}
            ORDER BY dt DESC
        """)
        incomplete = cur.fetchall()
        if incomplete:
            print(f"\nDays with avg bars/ticker != {EXPECTED_HOURLY_BARS} ({len(incomplete)} days):")
            print(f"  {'Date':<12} {'Tickers':>8} {'Avg Bars':>10}")
            for r in incomplete[:15]:
                print(f"  {r[0]}   {r[1]:>6}     {r[3]}")
            if len(incomplete) > 15:
                print(f"  ...and {len(incomplete)-15} more")
        else:
            print(f"All days have {EXPECTED_HOURLY_BARS} bars/ticker")

        # Tickers with incomplete bars on recent days
        cur.execute(f"""
            SELECT datetime::date as dt, ticker, COUNT(*) as bars
            FROM stock_prices_hourly
            WHERE datetime >= now() - interval '10 days'
            GROUP BY dt, ticker
            HAVING COUNT(*) != {EXPECTED_HOURLY_BARS}
            ORDER BY dt DESC, bars ASC
            LIMIT 20
        """)
        bad_tickers = cur.fetchall()
        if bad_tickers:
            print(f"\nTickers with != {EXPECTED_HOURLY_BARS} bars (last 10 days):")
            for r in bad_tickers:
                print(f"  {r[0]} {r[1]}: {r[2]} bars")
        else:
            print(f"All tickers have exactly {EXPECTED_HOURLY_BARS} bars/day (last 10 days)")


def check_intraday_5m(days: int = None):
    """Check intraday 5m table for missing days and incomplete bars."""
    print("\n" + "=" * 70)
    print("INTRADAY 5m COVERAGE (expect 78 bars/ticker/day)")
    print("=" * 70)

    cutoff_simple = f"AND datetime >= now() - interval '{days} days'" if days else ""

    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(f"""
            SELECT MIN(datetime::date), MAX(datetime::date), COUNT(DISTINCT ticker),
                   COUNT(DISTINCT datetime::date)
            FROM stock_prices_intraday
            WHERE interval = '5m' {cutoff_simple.replace('AND', 'AND', 1)}
        """)
        r = cur.fetchone()
        i_min, i_max = r[0], r[1]
        print(f"Range: {i_min} to {i_max} | {r[2]} tickers | {r[3]} trading days")

        # Trading days missing from intraday
        cur.execute(f"""
            SELECT d.dt FROM (
                SELECT DISTINCT datetime::date as dt FROM stock_prices_daily
                WHERE datetime::date >= %s AND datetime::date <= %s
            ) d
            LEFT JOIN (
                SELECT DISTINCT datetime::date as dt FROM stock_prices_intraday
                WHERE interval = '5m' {cutoff_simple.replace('AND', 'AND', 1)}
            ) i ON d.dt = i.dt
            WHERE i.dt IS NULL
            ORDER BY d.dt DESC
        """, (i_min, i_max))
        missing = cur.fetchall()
        if missing:
            print(f"\nMissing 5m data for {len(missing)} trading days:")
            for r in missing[:15]:
                print(f"  {r[0]}")
            if len(missing) > 15:
                print(f"  ...and {len(missing)-15} more")
        else:
            print("No missing trading days")

        # Days with incomplete bars
        cur.execute(f"""
            SELECT datetime::date as dt, COUNT(DISTINCT ticker) as tickers,
                   COUNT(*) as total_bars,
                   ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ticker), 0), 1) as avg_bars
            FROM stock_prices_intraday
            WHERE interval = '5m' {cutoff_simple.replace('AND', 'AND', 1)}
            GROUP BY dt
            HAVING ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ticker), 0), 1) != {EXPECTED_5M_BARS}
            ORDER BY dt DESC
        """)
        incomplete = cur.fetchall()
        if incomplete:
            print(f"\nDays with avg bars/ticker != {EXPECTED_5M_BARS} ({len(incomplete)} days):")
            print(f"  {'Date':<12} {'Tickers':>8} {'Avg Bars':>10}")
            for r in incomplete[:15]:
                print(f"  {r[0]}   {r[1]:>6}     {r[3]}")
            if len(incomplete) > 15:
                print(f"  ...and {len(incomplete)-15} more")
        else:
            print(f"All days have {EXPECTED_5M_BARS} bars/ticker")

        # Tickers with incomplete bars on recent days
        cur.execute(f"""
            SELECT datetime::date as dt, ticker, COUNT(*) as bars
            FROM stock_prices_intraday
            WHERE interval = '5m' AND datetime >= now() - interval '10 days'
            GROUP BY dt, ticker
            HAVING COUNT(*) != {EXPECTED_5M_BARS}
            ORDER BY dt DESC, bars ASC
            LIMIT 20
        """)
        bad_tickers = cur.fetchall()
        if bad_tickers:
            print(f"\nTickers with != {EXPECTED_5M_BARS} bars (last 10 days):")
            for r in bad_tickers:
                print(f"  {r[0]} {r[1]}: {r[2]} bars")
        else:
            print(f"All tickers have exactly {EXPECTED_5M_BARS} bars/day (last 10 days)")


def check_cross_table(days: int = None):
    """Check cross-table: daily dates that are missing hourly or 5m data."""
    print("\n" + "=" * 70)
    print("CROSS-TABLE COVERAGE (last 20 trading days)")
    print("=" * 70)

    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute("""
            WITH daily_dates AS (
                SELECT datetime::date as dt, COUNT(DISTINCT ticker) as d_tickers
                FROM stock_prices_daily
                WHERE datetime >= now() - interval '25 days'
                GROUP BY dt
            ),
            hourly_dates AS (
                SELECT datetime::date as dt, COUNT(DISTINCT ticker) as h_tickers,
                       ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ticker), 0), 1) as h_bars
                FROM stock_prices_hourly
                WHERE datetime >= now() - interval '25 days'
                GROUP BY dt
            ),
            intraday_dates AS (
                SELECT datetime::date as dt, COUNT(DISTINCT ticker) as i_tickers,
                       ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ticker), 0), 1) as i_bars
                FROM stock_prices_intraday
                WHERE interval = '5m' AND datetime >= now() - interval '25 days'
                GROUP BY dt
            )
            SELECT d.dt,
                   d.d_tickers,
                   COALESCE(h.h_tickers, 0) as h_tickers,
                   COALESCE(h.h_bars, 0) as h_bars,
                   COALESCE(i.i_tickers, 0) as i_tickers,
                   COALESCE(i.i_bars, 0) as i_bars
            FROM daily_dates d
            LEFT JOIN hourly_dates h ON d.dt = h.dt
            LEFT JOIN intraday_dates i ON d.dt = i.dt
            ORDER BY d.dt DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"  {'Date':<12} {'Daily':>6} {'Hourly':>8} {'H Bars':>7} {'5m':>6} {'5m Bars':>8}")
        print(f"  {'─'*12} {'─'*6} {'─'*8} {'─'*7} {'─'*6} {'─'*8}")
        for r in rows:
            h_flag = " *" if r[2] == 0 or float(r[3]) != EXPECTED_HOURLY_BARS else ""
            i_flag = " *" if r[4] == 0 or float(r[5]) != EXPECTED_5M_BARS else ""
            print(f"  {r[0]}   {r[1]:>4}   {r[2]:>6}   {r[3]:>5}{h_flag:<2}  {r[4]:>4}   {r[5]:>6}{i_flag}")
        print("\n  * = missing or incomplete data")


def check_single_ticker(ticker: str, days: int = None):
    """Detailed coverage for a single ticker across all tables."""
    print("=" * 70)
    print(f"TICKER DETAIL: {ticker}")
    print("=" * 70)

    cutoff = f"AND datetime >= now() - interval '{days} days'" if days else ""

    with get_db_cursor(dict_cursor=False) as cur:
        for table, label in [
            ("stock_prices_daily", "Daily"),
            ("stock_prices_hourly", "Hourly"),
        ]:
            cur.execute(f"""
                SELECT MIN(datetime::date), MAX(datetime::date), COUNT(*),
                       COUNT(DISTINCT datetime::date)
                FROM {table}
                WHERE ticker = %s {cutoff}
            """, (ticker,))
            r = cur.fetchone()
            print(f"\n{label}: {r[0]} to {r[1]} | {r[2]} rows | {r[3]} trading days")

            # Per-day bar count
            cur.execute(f"""
                SELECT datetime::date as dt, COUNT(*) as bars
                FROM {table}
                WHERE ticker = %s {cutoff}
                GROUP BY dt ORDER BY dt DESC LIMIT 15
            """, (ticker,))
            for r in cur.fetchall():
                print(f"  {r[0]}: {r[1]} bars")

        # Intraday
        cur.execute(f"""
            SELECT MIN(datetime::date), MAX(datetime::date), COUNT(*),
                   COUNT(DISTINCT datetime::date)
            FROM stock_prices_intraday
            WHERE ticker = %s AND interval = '5m' {cutoff}
        """, (ticker,))
        r = cur.fetchone()
        print(f"\nIntraday 5m: {r[0]} to {r[1]} | {r[2]} rows | {r[3]} trading days")

        cur.execute(f"""
            SELECT datetime::date as dt, COUNT(*) as bars
            FROM stock_prices_intraday
            WHERE ticker = %s AND interval = '5m' {cutoff}
            GROUP BY dt ORDER BY dt DESC LIMIT 15
        """, (ticker,))
        for r in cur.fetchall():
            flag = "" if r[1] == EXPECTED_5M_BARS else f" (expect {EXPECTED_5M_BARS})"
            print(f"  {r[0]}: {r[1]} bars{flag}")


def main():
    parser = argparse.ArgumentParser(
        description="Cross-table data coverage checker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--days", type=int, default=None,
                        help="Limit to last N days (default: all data)")
    parser.add_argument("--ticker", type=str, default=None,
                        help="Single ticker detail view")
    args = parser.parse_args()

    if args.ticker:
        check_single_ticker(args.ticker.upper(), args.days)
        return

    check_daily(args.days)
    check_hourly(args.days)
    check_intraday_5m(args.days)
    check_cross_table(args.days)

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
