"""Point-in-time structured trend-pullback pattern detection."""
from __future__ import annotations

import numpy as np
import pandas as pd


def load_hourly_panel(
    start: str | None = None,
    end: str | None = None,
    tickers: list[str] | None = None,
) -> pd.DataFrame:
    """Load regular-session 1h bars in Eastern time, sorted by ticker and bar."""
    import sys
    from pathlib import Path

    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    from database import get_db_cursor

    clauses = []
    params: list = []
    if start:
        clauses.append("bar_start >= %s")
        params.append(start)
    if end:
        clauses.append("bar_start < (%s::date + INTERVAL '1 day')")
        params.append(end)
    if tickers:
        clauses.append("ticker = ANY(%s)")
        params.append(tickers)

    filters = f"AND {' AND '.join(clauses)}" if clauses else ""
    with get_db_cursor() as cur:
        cur.execute(
            f"""
                SELECT DISTINCT ON (ticker, bar_start)
                       ticker,
                       bar_start AT TIME ZONE 'America/New_York' AS date,
                       open_price AS open, high_price AS high, low_price AS low,
                       close_price AS close, volume
                FROM equity_bar_revisions
                WHERE interval = '1h'
                  AND session_scope = 'RTH'
                  AND adjusted = FALSE
                  AND is_final = TRUE
                  AND COALESCE(replay_available_at, system_observed_at) <= NOW()
                  {filters}
                ORDER BY ticker, bar_start,
                         CASE
                             WHEN source_kind = 'RECONCILED' THEN 0
                             WHEN source_kind = 'DERIVED' THEN 1
                             WHEN source_kind = 'NATIVE_REST' THEN 2
                             ELSE 3
                         END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
            """,
            params,
        )
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "volume"]
        )
    frame = pd.DataFrame([dict(row) for row in rows])
    frame["date"] = pd.to_datetime(frame["date"])
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    frame = frame[(frame[["open", "high", "low", "close"]] > 0).all(axis=1)]
    return frame.sort_values(["ticker", "date"]).reset_index(drop=True)


def _candles(frame: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    open_ = frame["open"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    prior_open = open_.shift(1)
    prior_close = close.shift(1)

    span = (high - low).replace(0, np.nan)
    body = (close - open_).abs()
    lower_wick = np.minimum(open_, close) - low
    upper_wick = high - np.maximum(open_, close)
    close_location = (close - low) / span

    out["bull_hammer"] = (
        (lower_wick >= 0.50 * span)
        & (upper_wick <= 0.25 * span)
        & (close_location >= 0.60)
    )
    out["bear_shooting_star"] = (
        (upper_wick >= 0.50 * span)
        & (lower_wick <= 0.25 * span)
        & (close_location <= 0.40)
    )
    out["bull_engulfing"] = (
        (close > open_)
        & (prior_close < prior_open)
        & (open_ <= prior_close)
        & (close >= prior_open)
    )
    out["bear_engulfing"] = (
        (close < open_)
        & (prior_close > prior_open)
        & (open_ >= prior_close)
        & (close <= prior_open)
    )
    out["bull_strong_close"] = (
        (close > open_) & (body >= 0.50 * span) & (close_location >= 0.75)
    )
    out["bear_strong_close"] = (
        (close < open_) & (body >= 0.50 * span) & (close_location <= 0.25)
    )
    return out.fillna(False)


def _confirmed_swings(
    high: np.ndarray, low: np.ndarray, left: int = 2, right: int = 2
) -> dict[str, np.ndarray]:
    """Latest two pivots known at each bar; pivots appear only after right-bar confirmation."""
    size = len(high)
    fields = {
        "last_high": np.full(size, np.nan),
        "prior_high": np.full(size, np.nan),
        "last_high_index": np.full(size, -1, dtype=int),
        "last_low": np.full(size, np.nan),
        "prior_low": np.full(size, np.nan),
        "last_low_index": np.full(size, -1, dtype=int),
    }
    last_high = prior_high = np.nan
    last_low = prior_low = np.nan
    last_high_index = last_low_index = -1

    for confirmation in range(size):
        pivot = confirmation - right
        if pivot >= left:
            high_window = high[pivot - left:pivot + right + 1]
            low_window = low[pivot - left:pivot + right + 1]
            if np.isfinite(high_window).all() and high[pivot] == high_window.max():
                prior_high, last_high = last_high, high[pivot]
                last_high_index = pivot
            if np.isfinite(low_window).all() and low[pivot] == low_window.min():
                prior_low, last_low = last_low, low[pivot]
                last_low_index = pivot

        fields["last_high"][confirmation] = last_high
        fields["prior_high"][confirmation] = prior_high
        fields["last_high_index"][confirmation] = last_high_index
        fields["last_low"][confirmation] = last_low
        fields["prior_low"][confirmation] = prior_low
        fields["last_low_index"][confirmation] = last_low_index
    return fields


def _bars_since(condition: pd.Series) -> pd.Series:
    result = np.full(len(condition), np.nan)
    elapsed: int | None = None
    for index, value in enumerate(condition.fillna(False).to_numpy(dtype=bool)):
        if value:
            elapsed = 0
        elif elapsed is not None:
            elapsed += 1
        if elapsed is not None:
            result[index] = elapsed
    return pd.Series(result, index=condition.index)


def build_trend_pullback_patterns(
    panel: pd.DataFrame,
    new_trend_days: int = 40,
    pullback_min_bars: int = 2,
    pullback_max_bars: int = 15,
    pivot_atr_break: float = 0.25,
    support_atr_distance: float = 0.25,
) -> pd.DataFrame:
    """Attach symmetric bull/bear setup and trigger flags to a daily OHLCV panel."""
    pieces: list[pd.DataFrame] = []
    for _, source in panel.groupby("ticker", sort=False):
        frame = source.sort_values("date").copy()
        close = frame["close"].astype(float)
        high = frame["high"].astype(float)
        low = frame["low"].astype(float)
        prior_close = close.shift(1)

        frame["sma20"] = close.rolling(20).mean()
        frame["sma50"] = close.rolling(50).mean()
        true_range = pd.concat(
            [high - low, (high - prior_close).abs(), (low - prior_close).abs()], axis=1
        ).max(axis=1)
        frame["atr14"] = true_range.ewm(alpha=1 / 14, adjust=False).mean()

        bull_cross = (frame["sma20"] > frame["sma50"]) & (
            frame["sma20"].shift(1) <= frame["sma50"].shift(1)
        )
        bear_cross = (frame["sma20"] < frame["sma50"]) & (
            frame["sma20"].shift(1) >= frame["sma50"].shift(1)
        )
        frame["bull_cross_age"] = _bars_since(bull_cross)
        frame["bear_cross_age"] = _bars_since(bear_cross)

        swings = _confirmed_swings(high.to_numpy(), low.to_numpy())
        for name, values in swings.items():
            frame[name] = values

        atr = frame["atr14"]
        frame["higher_swing_high"] = (
            frame["last_high"].notna()
            & frame["prior_high"].notna()
            & (frame["last_high"] >= frame["prior_high"] + pivot_atr_break * atr)
        )
        frame["lower_swing_high"] = (
            frame["last_high"].notna()
            & frame["prior_high"].notna()
            & (frame["last_high"] <= frame["prior_high"] - pivot_atr_break * atr)
        )
        frame["higher_swing_low"] = (
            frame["last_low"].notna()
            & frame["prior_low"].notna()
            & (frame["last_low"] >= frame["prior_low"] + pivot_atr_break * atr)
        )
        frame["lower_swing_low"] = (
            frame["last_low"].notna()
            & frame["prior_low"].notna()
            & (frame["last_low"] <= frame["prior_low"] - pivot_atr_break * atr)
        )

        index = np.arange(len(frame))
        bars_after_high = index - frame["last_high_index"].to_numpy()
        bars_after_low = index - frame["last_low_index"].to_numpy()
        support_distance = np.maximum(
            np.maximum(low.to_numpy() - frame["sma20"].to_numpy(), 0),
            np.maximum(frame["sma20"].to_numpy() - high.to_numpy(), 0),
        )
        touches_ma20 = support_distance <= support_atr_distance * atr.to_numpy()

        frame["bull_setup"] = (
            frame["bull_cross_age"].between(0, new_trend_days)
            & frame["higher_swing_high"]
            & pd.Series(bars_after_high, index=frame.index).between(
                pullback_min_bars, pullback_max_bars
            )
            & pd.Series(touches_ma20, index=frame.index)
            & (close >= frame["sma20"] - 0.10 * atr)
            & (close > frame["sma50"])
            & (close < frame["last_high"])
            & ((frame["last_high"] - low) >= 0.50 * atr)
        )
        frame["bear_setup"] = (
            frame["bear_cross_age"].between(0, new_trend_days)
            & frame["lower_swing_low"]
            & pd.Series(bars_after_low, index=frame.index).between(
                pullback_min_bars, pullback_max_bars
            )
            & pd.Series(touches_ma20, index=frame.index)
            & (close <= frame["sma20"] + 0.10 * atr)
            & (close < frame["sma50"])
            & (close > frame["last_low"])
            & ((high - frame["last_low"]) >= 0.50 * atr)
        )

        frame["bull_swing_support"] = (
            frame["prior_high"].notna()
            & (low <= frame["prior_high"] + support_atr_distance * atr)
            & (high >= frame["prior_high"] - support_atr_distance * atr)
            & (close >= frame["prior_high"] - 0.10 * atr)
        )
        frame["bear_swing_resistance"] = (
            frame["prior_low"].notna()
            & (high >= frame["prior_low"] - support_atr_distance * atr)
            & (low <= frame["prior_low"] + support_atr_distance * atr)
            & (close <= frame["prior_low"] + 0.10 * atr)
        )

        candles = _candles(frame)
        for column in candles:
            frame[column] = candles[column]
        frame["bull_trigger"] = frame["bull_setup"] & candles[
            ["bull_hammer", "bull_engulfing", "bull_strong_close"]
        ].any(axis=1)
        frame["bear_trigger"] = frame["bear_setup"] & candles[
            ["bear_shooting_star", "bear_engulfing", "bear_strong_close"]
        ].any(axis=1)
        frame["bull_reversal_trigger"] = frame["bull_setup"] & candles[
            ["bull_hammer", "bull_engulfing"]
        ].any(axis=1)
        frame["bear_reversal_trigger"] = frame["bear_setup"] & candles[
            ["bear_shooting_star", "bear_engulfing"]
        ].any(axis=1)
        frame["bull_swing_retest_trigger"] = (
            frame["bull_trigger"] & frame["bull_swing_support"]
        )
        frame["bear_swing_retest_trigger"] = (
            frame["bear_trigger"] & frame["bear_swing_resistance"]
        )
        frame["bull_strict_trigger"] = (
            frame["bull_reversal_trigger"] & frame["bull_swing_support"]
        )
        frame["bear_strict_trigger"] = (
            frame["bear_reversal_trigger"] & frame["bear_swing_resistance"]
        )
        frame["direction"] = np.select(
            [frame["bull_trigger"], frame["bear_trigger"]], [1, -1], default=0
        )
        frame["trigger_candle"] = np.select(
            [
                frame["bull_hammer"], frame["bull_engulfing"],
                frame["bull_strong_close"], frame["bear_shooting_star"],
                frame["bear_engulfing"], frame["bear_strong_close"],
            ],
            [
                "hammer", "bullish_engulfing", "bullish_strong_close",
                "shooting_star", "bearish_engulfing", "bearish_strong_close",
            ],
            default="",
        )
        pieces.append(frame)

    return pd.concat(pieces, ignore_index=True) if pieces else panel.copy()
