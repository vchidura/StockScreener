"""Shared Screening features and retained legacy snapshot filtering."""

from __future__ import annotations

import math

from collections import Counter

import numpy as np

VERSION = "stock_discovery_v1"

SNAPSHOT_SOURCE = "STOCK_DISCOVERY"

ALERT_SOURCE = "STOCK_DISCOVERY_ALERT"

MARK_SOURCE = "STOCK_DISCOVERY_MARK"

NOTIONAL = 1000.0

COST_BPS = 10.0

def stock_features(frame, session, security_id):
    frame = frame.sort_values("session_date")
    row = dict(eligible=False, exclusion="INSUFFICIENT_HISTORY", price=None, momentum=None,
               change=None, relative_volume=None, dollar_volume=None, volatility=None,
               trend="UNAVAILABLE", state="UNAVAILABLE", break_side=0)
    if frame.empty:
        return row
    frame = frame.loc[frame.session_date <= session].tail(253)
    if frame.empty:
        return row
    row["price"] = float(frame.close.iloc[-1])
    if frame.session_date.iloc[-1] != session:
        return row | {"exclusion": "MISSING_LATEST_SESSION"}
    if len(frame) != 253:
        return row
    if not frame.security_id.astype(str).eq(str(security_id)).all():
        return row | {"exclusion": "IDENTITY_CHANGED"}
    if frame.ordinal.diff().iloc[1:].ne(1).any():
        return row | {"exclusion": "MISSING_HISTORY_SESSION"}
    values = frame[["open", "high", "low", "close", "volume"]].astype(float)
    if not np.isfinite(values).all().all() or (values[["open", "high", "low", "close"]] <= 0).any().any() or (values.volume < 0).any():
        return row | {"exclusion": "INVALID_PRICE"}
    close, high, low, volume = values.close, values.high, values.low, values.volume
    liquidity = float((frame.raw_close.astype(float) * frame.raw_volume.astype(float)).tail(20).median())
    row.update(momentum=float(close.iloc[-22] / close.iloc[-253] - 1),
               change=float(close.iloc[-1] / close.iloc[-2] - 1), dollar_volume=liquidity,
               relative_volume=float(volume.iloc[-1] / volume.iloc[-21:-1].mean()) if volume.iloc[-21:-1].mean() > 0 else None,
               volatility=float(close.pct_change(fill_method=None).tail(21).std() * math.sqrt(252)))
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    sma50 = close.rolling(50).mean()
    trend = "UP" if close.iloc[-1] > sma50.iloc[-1] and sma50.iloc[-1] > sma50.iloc[-11] else "DOWN" if close.iloc[-1] < sma50.iloc[-1] and sma50.iloc[-1] < sma50.iloc[-11] else "MIXED"
    recent = close.iloc[-1] / close.iloc[-6] - 1
    state = "PULLBACK" if trend == "UP" and recent < 0 else "BOUNCE" if trend == "DOWN" and recent > 0 else "TRENDING" if trend != "MIXED" else "MIXED"
    if trend == "UP" and close.iloc[-1] > high.iloc[-2] and close.iloc[-1] > ema20 and close.iloc[-2] < close.iloc[-6]:
        state = "RESUMING_UP"
    if trend == "DOWN" and close.iloc[-1] < low.iloc[-2] and close.iloc[-1] < ema20 and close.iloc[-2] > close.iloc[-6]:
        state = "RESUMING_DOWN"
    breakout = 1 if close.iloc[-1] > high.iloc[-21:-1].max() else -1 if close.iloc[-1] < low.iloc[-21:-1].min() else 0
    exclusion = "PRICE_BELOW_5" if row["price"] < 5 else "LOW_LIQUIDITY" if liquidity < 20_000_000 else ""
    return row | dict(eligible=not exclusion, exclusion=exclusion, trend=trend, state=state, break_side=breakout)

def filter_stocks(rows, *, search="", sector=None, side=None, state=None, eligible_only=True,
                  minimum_price=0, minimum_dollar_volume=0, minimum_relative_volume=0,
                  sort="RANK", offset=0, limit=100):
    sectors = sorted({row.get("sector") for row in rows if row.get("sector")})
    selected = [row for row in rows if (not eligible_only or row["eligible"])
                and (not search or search.upper() in (row["ticker"] + " " + (row.get("company_name") or "")).upper())
                and (not sector or row.get("sector") == sector) and (not side or row["side"] == side)
                and (not state or row["state"] == state)
                and (row["price"] or 0) >= minimum_price and (row["dollar_volume"] or 0) >= minimum_dollar_volume
                and (row["relative_volume"] or 0) >= minimum_relative_volume]
    field = {"RANK": "rank", "WEAKEST": "momentum", "CHANGE": "change", "VOLUME": "relative_volume", "LIQUIDITY": "dollar_volume", "VOLATILITY": "volatility"}[sort]
    descending = sort not in ("RANK", "WEAKEST")
    selected.sort(key=lambda row: (row.get(field) is None, (-row[field] if descending else row[field]) if row.get(field) is not None else 0, row["ticker"]))
    return dict(rows=selected[offset:offset + limit], total=len(selected), offset=offset, limit=limit, sectors=sectors,
                eligible_count=sum(row["eligible"] for row in rows), universe_count=len(rows),
                exclusion_counts=dict(Counter(row["exclusion"] for row in rows if not row["eligible"])))
