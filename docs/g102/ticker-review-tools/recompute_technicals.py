"""Recompute Timeframes metrics from stored bars on the copy and compare with what the live API served.

Read-only: the connection is opened readonly; bars only through realtime.equity_bar_repository.
Usage (inside the r94 image, worktree backend at /backend, served JSON at /live):
  python /tools/recompute_technicals.py AAPL NVDA ...
"""
import json
import math
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2

sys.path.insert(0, "/backend")
from equity.technicals import compute_trade_setup_technicals  # noqa: E402
from realtime.equity_bar_repository import load_bar_range_cursor  # noqa: E402

END = datetime(2026, 11, 11, 21, 0, tzinfo=timezone.utc)
START = datetime(2020, 1, 1, tzinfo=timezone.utc)
WINDOW = {"30m": 500, "1h": 500, "1d": 283, "1wk": 261, "1mo": 60}
BASIS = {"30m": "RAW_UNADJUSTED", "1h": "RAW_UNADJUSTED", "1d": "ADJUSTED", "1wk": "ADJUSTED", "1mo": "ADJUSTED"}
FIELDS = ("rsi", "atr", "atr_pct", "adx", "plus_di", "minus_di", "historical_volatility_pct",
          "historical_volatility_percentile", "relative_volume", "volume_trend_pct", "volume_slope", "cmf_20",
          "trend_consistency", "ema8", "ema21", "ema50", "vwap", "ma50", "ma200", "stoch_k", "macd")


def connect():
    password = open("/run/secrets/pw", encoding="utf-8").read().strip()
    connection = psycopg2.connect(host="g102-test-pg", dbname="test_realtime_v2_r92", user="postgres", password=password)
    connection.set_session(readonly=True)
    return connection


def instrument_ids(cursor, tickers):
    cursor.execute(
        "SELECT DISTINCT ON (r.ticker) r.ticker, i.instrument_id FROM ref.security_revisions r "
        "JOIN rt.instruments i ON i.security_id=r.security_id AND i.asset_class='EQUITY' "
        "WHERE r.ticker = ANY(%s) ORDER BY r.ticker, r.effective_from DESC", (list(tickers),))
    return dict(cursor.fetchall())


def frame_of(rows):
    return pd.DataFrame({
        "open": [float(r["open_price"]) for r in rows], "high": [float(r["high_price"]) for r in rows],
        "low": [float(r["low_price"]) for r in rows], "close": [float(r["close_price"]) for r in rows],
        "volume": [float(r["volume"]) for r in rows],
    }, index=pd.DatetimeIndex([r["bar_start"] for r in rows])).sort_index()


def wilder(values, period):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan)
    if len(values) < period:
        return out
    out[period - 1] = values[:period].mean()
    for i in range(period, len(values)):
        out[i] = (out[i - 1] * (period - 1) + values[i]) / period
    return out


def textbook(frame, interval):
    close, high, low, vol = (frame[c].to_numpy(float) for c in ("close", "high", "low", "volume"))
    delta = np.diff(close)
    gain, loss = np.clip(delta, 0, None), np.clip(-delta, 0, None)
    ag, al = wilder(gain, 14)[-1], wilder(loss, 14)[-1]
    rsi_wilder = 100 - 100 / (1 + ag / al) if al > 0 else 100.0
    tr = np.maximum(high[1:] - low[1:], np.maximum(abs(high[1:] - close[:-1]), abs(low[1:] - close[:-1])))
    atr_wilder = wilder(tr, 14)[-1]
    up, down = np.diff(high), -np.diff(low)
    pdm = np.where((up > down) & (up > 0), up, 0.0)
    mdm = np.where((down > up) & (down > 0), down, 0.0)
    str_, spdm, smdm = wilder(tr, 14), wilder(pdm, 14), wilder(mdm, 14)
    pdi, mdi = 100 * spdm / str_, 100 * smdm / str_
    dx = 100 * abs(pdi - mdi) / (pdi + mdi)
    adx = wilder(dx[~np.isnan(dx)], 14)[-1]
    bars_per_day = {"30m": 13, "1h": 7}.get(interval)
    per_year = {"1mo": 12, "1wk": 52, "1d": 252}.get(interval) or 252 * bars_per_day
    hv = float(np.std(np.diff(np.log(close))[-20:], ddof=1) * math.sqrt(per_year) * 100)
    result = dict(rsi_wilder=round(rsi_wilder, 1), atr_wilder=round(float(atr_wilder), 2),
                  adx_wilder=round(float(adx), 1), plus_di_wilder=round(float(pdi[-1]), 1),
                  minus_di_wilder=round(float(mdi[-1]), 1), hv20_bar_count_annualized=round(hv, 1))
    if bars_per_day:
        # Time-of-day RVOL: last bar against the same slot over the previous 20 sessions.
        local = frame.index.tz_convert("America/New_York")
        slot = (local.hour * 60 + local.minute)
        same = vol[:-1][slot[:-1] == slot[-1]][-20:]
        result["rvol_same_slot"] = round(float(vol[-1] / same.mean()), 2) if len(same) else None
        result["last_bar"] = f"{local[-1]:%Y-%m-%d %H:%M} ET"
        result["last_bar_vs_20bar_mean_raw"] = round(float(vol[-1] / vol[-21:-1].mean()), 2)
    return result


def main(tickers):
    connection = connect()
    cursor = connection.cursor()
    ids = instrument_ids(cursor, tickers)
    report = {}
    for ticker in tickers:
        served = json.load(open(f"/live/{ticker}.json"))["multi"]["setups"]
        for interval in ("30m", "1h", "1d", "1wk", "1mo"):
            rows = load_bar_range_cursor(cursor, instrument_ids=(ids[ticker],), interval=interval,
                                         basis=BASIS[interval], start=START, end=END)[ids[ticker]]
            rows = rows[-WINDOW[interval]:]
            frame = frame_of(rows)
            production = compute_trade_setup_technicals(frame, interval, input_includes_forming_bar=False).payload()
            setup = served.get(interval) or {}
            got = setup.get("technicals", {})
            diffs = {f: (got.get(f), production.get(f)) for f in FIELDS
                     if got.get(f) != production.get(f) and not (
                         isinstance(got.get(f), (int, float)) and isinstance(production.get(f), (int, float))
                         and abs(got[f] - production[f]) <= 1e-9)}
            report[f"{ticker} {interval}"] = dict(
                bars=len(rows), first=str(frame.index[0]), last=str(frame.index[-1]),
                served_last_close=setup.get("last_close"), stored_last_close=round(float(frame["close"].iloc[-1]), 2),
                served_vs_recomputed_diffs=diffs,
                served=dict((f, got.get(f)) for f in ("rsi", "atr", "adx", "plus_di", "minus_di",
                                                      "historical_volatility_pct", "relative_volume", "cmf_20")),
                textbook=textbook(frame, interval),
            )
    connection.rollback()
    json.dump(report, open("/out/recompute.json", "w"), indent=1, default=str)
    for key, value in report.items():
        print(key, value["bars"], value["last"], "served==stored close:",
              value["served_last_close"] == value["stored_last_close"],
              "diffs:", value["served_vs_recomputed_diffs"] or "none")


main(sys.argv[1:])
