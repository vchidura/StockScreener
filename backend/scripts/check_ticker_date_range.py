import argparse
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect OHLCV rows for a ticker in a date range")
    parser.add_argument("--ticker", type=str, required=True)
    parser.add_argument("--start", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    ticker = args.ticker.upper()

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                ticker,
                datetime,
                open_price,
                high,
                low,
                close_price,
                volume
            FROM stock_prices_hourly
            WHERE ticker = %s
              AND datetime >= %s::date
              AND datetime < (%s::date + INTERVAL '1 day')
            ORDER BY datetime ASC
            """,
            (ticker, args.start, args.end),
        )
        rows = cursor.fetchall()

    if not rows:
        print(f"No rows found for {ticker} between {args.start} and {args.end}")
        return

    print(f"Rows found: {len(rows)}")
    print(f"Ticker: {ticker} | Range: {args.start} to {args.end}")
    print("-" * 115)
    print(f"{'datetime':<22} {'open':>10} {'high':>10} {'low':>10} {'close':>10} {'volume':>12}")
    print("-" * 115)

    for row in rows:
        print(
            f"{str(row['datetime']):<22} "
            f"{row['open_price']:>10.2f} "
            f"{row['high']:>10.2f} "
            f"{row['low']:>10.2f} "
            f"{row['close_price']:>10.2f} "
            f"{int(row['volume']):>12}"
        )


if __name__ == "__main__":
    main()
