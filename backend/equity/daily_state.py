"""Pure daily screening state with the original stock_discovery_v1 contract."""
from __future__ import annotations

import math

import numpy as np


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