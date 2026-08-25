import argparse
from datetime import timedelta
from pathlib import Path
import sys
import io
import os

import pandas as pd
import yfinance as yf
import requests

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor
from http_client import get_session


def get_db_daily(ticker: str, start: str, end: str) -> pd.DataFrame:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT datetime, open_price, high, low, close_price, volume
            FROM stock_prices_hourly
            WHERE ticker = %s
              AND datetime >= %s::date
              AND datetime < (%s::date + INTERVAL '1 day')
            ORDER BY datetime ASC
            """,
            (ticker, start, end),
        )
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["date", "db_close", "db_open", "db_high", "db_low", "db_volume"])

    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date

    # If multiple rows per day exist, take latest timestamp in that day
    daily = (
        df.sort_values("datetime")
        .groupby("date", as_index=False)
        .last()
        .rename(
            columns={
                "close_price": "db_close",
                "open_price": "db_open",
                "high": "db_high",
                "low": "db_low",
                "volume": "db_volume",
            }
        )
    )

    return daily[["date", "db_open", "db_high", "db_low", "db_close", "db_volume"]]


def get_yahoo_daily(ticker: str, start: str, end: str) -> pd.DataFrame:
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end) + timedelta(days=1)

    stock = yf.Ticker(ticker)
    df = stock.history(
        start=start_dt.strftime("%Y-%m-%d"),
        end=end_dt.strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
    )

    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "yf_close", "yf_adj_close"])

    data = df.reset_index()
    data["date"] = pd.to_datetime(data["Date"]).dt.date

    if "Adj Close" not in data.columns:
        data["Adj Close"] = data["Close"]

    return data[["date", "Close", "Adj Close"]].rename(
        columns={"Close": "yf_close", "Adj Close": "yf_adj_close"}
    )


def get_stooq_daily(ticker: str, start: str, end: str) -> pd.DataFrame:
    symbol = f"{ticker.lower()}.us"
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    try:
        response = get_session().get(url, timeout=20)
        response.raise_for_status()
        text = response.text.strip()
        if not text or text.lower().startswith("no data"):
            return pd.DataFrame(columns=["date", "stooq_close"])

        df = pd.read_csv(io.StringIO(text))
        if df.empty:
            return pd.DataFrame(columns=["date", "stooq_close"])

        df.columns = [c.strip().lower() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[(df["date"] >= pd.to_datetime(start).date()) & (df["date"] <= pd.to_datetime(end).date())]

        if "close" not in df.columns:
            return pd.DataFrame(columns=["date", "stooq_close"])

        return df[["date", "close"]].rename(columns={"close": "stooq_close"})
    except Exception:
        return pd.DataFrame(columns=["date", "stooq_close"])


def get_alpha_vantage_daily(ticker: str, start: str, end: str) -> pd.DataFrame:
    api_key = os.getenv("ALPHAVANTAGE_API_KEY", "demo")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "outputsize": "full",
        "apikey": api_key,
    }

    try:
        response = get_session().get(url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()

        series = payload.get("Time Series (Daily)")
        if not series:
            return pd.DataFrame(columns=["date", "av_close", "av_adj_close"])

        rows = []
        start_date = pd.to_datetime(start).date()
        end_date = pd.to_datetime(end).date()

        for dt, values in series.items():
            trade_date = pd.to_datetime(dt).date()
            if start_date <= trade_date <= end_date:
                rows.append(
                    {
                        "date": trade_date,
                        "av_close": float(values.get("4. close", "nan")),
                        "av_adj_close": float(values.get("5. adjusted close", "nan")),
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["date", "av_close", "av_adj_close"])

        return pd.DataFrame(rows).sort_values("date")
    except Exception:
        return pd.DataFrame(columns=["date", "av_close", "av_adj_close"])


def get_twelve_data_daily(ticker: str, start: str, end: str) -> pd.DataFrame:
    api_key = os.getenv("TWELVEDATA_API_KEY", "")
    if not api_key:
        return pd.DataFrame(columns=["date", "td_close"])

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": ticker,
        "interval": "1day",
        "outputsize": 5000,
        "start_date": start,
        "end_date": end,
        "apikey": api_key,
    }

    try:
        response = get_session().get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") == "error":
            print(f"Twelve Data error: {payload.get('message', '')}")
            return pd.DataFrame(columns=["date", "td_close"])

        values_list = payload.get("values")
        if not values_list:
            return pd.DataFrame(columns=["date", "td_close"])

        rows = []
        for item in values_list:
            rows.append({
                "date": pd.to_datetime(item["datetime"]).date(),
                "td_close": float(item["close"]),
            })

        return pd.DataFrame(rows).sort_values("date")
    except Exception as e:
        print(f"Twelve Data fetch error: {e}")
        return pd.DataFrame(columns=["date", "td_close"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare DB daily closes vs Yahoo/Stooq/TwelveData for a ticker/date range")
    parser.add_argument("--ticker", required=True, type=str)
    parser.add_argument("--start", required=True, type=str, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=str, help="YYYY-MM-DD")
    args = parser.parse_args()

    ticker = args.ticker.upper()

    db_df = get_db_daily(ticker, args.start, args.end)
    yf_df = get_yahoo_daily(ticker, args.start, args.end)
    stooq_df = get_stooq_daily(ticker, args.start, args.end)
    av_df = get_alpha_vantage_daily(ticker, args.start, args.end)
    td_df = get_twelve_data_daily(ticker, args.start, args.end)

    merged = (
        db_df.merge(yf_df, on="date", how="outer")
        .merge(stooq_df, on="date", how="outer")
        .merge(av_df, on="date", how="outer")
        .merge(td_df, on="date", how="outer")
    )
    merged = merged.sort_values("date")

    if merged.empty:
        print("No data found in requested range from DB/Yahoo/Stooq.")
        return

    merged["delta_db_vs_yf_close"] = merged["db_close"] - merged["yf_close"]
    merged["delta_db_vs_yf_adj"] = merged["db_close"] - merged["yf_adj_close"]
    merged["delta_db_vs_stooq"] = merged["db_close"] - merged["stooq_close"]
    merged["delta_db_vs_av_close"] = merged["db_close"] - merged["av_close"]
    merged["delta_db_vs_av_adj"] = merged["db_close"] - merged["av_adj_close"]
    merged["delta_db_vs_td"] = merged["db_close"] - merged["td_close"]

    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 30)

    print(f"Ticker={ticker} | Range={args.start}..{args.end}")
    print(merged[[
        "date",
        "db_close",
        "av_close",
        "td_close",
        "stooq_close",
        "delta_db_vs_av_close",
        "delta_db_vs_td",
        "delta_db_vs_stooq",
    ]].to_string(index=False, float_format=lambda x: f"{x:.4f}" if pd.notna(x) else ""))


if __name__ == "__main__":
    main()
