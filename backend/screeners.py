import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
import threading
import time as _time
from database import get_stock_data as db_get_stock_data, get_bulk_stock_data

logger = logging.getLogger("stock-screener-api")

# ── TTL cache for bulk_load_dataframes (avoids repeated heavy DB queries) ──
_bulk_cache: Dict[str, Tuple[float, Dict[str, pd.DataFrame]]] = {}
_bulk_cache_lock = threading.Lock()
_BULK_CACHE_TTL = 3600  # 1 hour


def get_data_from_db(ticker: str, days: int = 365) -> Optional[pd.DataFrame]:
    """Gets data from PostgreSQL database and converts to DataFrame."""
    try:
        rows = db_get_stock_data(ticker, days)
        if not rows:
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        logger.error("Error getting DB data for %s: %s", ticker, e)
        return None


def bulk_load_dataframes(tickers: List[str], days: int = 365, end_date: str = None) -> Dict[str, pd.DataFrame]:
    """Fetch all tickers in one DB query, return {ticker: DataFrame} dict.
    If end_date is provided (YYYY-MM-DD), data window ends on that date.
    Results are cached for 5 minutes to avoid redundant heavy DB queries."""
    cache_key = f"{days}|{end_date or 'latest'}"
    now = _time.time()
    with _bulk_cache_lock:
        if cache_key in _bulk_cache:
            ts, cached_frames = _bulk_cache[cache_key]
            if now - ts < _BULK_CACHE_TTL:
                # Return subset matching requested tickers
                hit = {t: cached_frames[t] for t in tickers if t in cached_frames}
                if len(hit) == len(tickers):
                    logger.info("bulk_load cache HIT | key=%s | tickers=%s", cache_key, len(hit))
                    return hit

    raw = get_bulk_stock_data(tickers, days, end_date=end_date)
    frames: Dict[str, pd.DataFrame] = {}
    for ticker, rows in raw.items():
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        if 'ticker' in df.columns:
            df = df.drop(columns=['ticker'])
        df.columns = [col.lower() for col in df.columns]
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = df[col].astype(float)
        frames[ticker] = df

    # Merge into cache (preserve other tickers already cached at this key)
    with _bulk_cache_lock:
        if cache_key in _bulk_cache:
            _, existing = _bulk_cache[cache_key]
            existing.update(frames)
            _bulk_cache[cache_key] = (now, existing)
        else:
            _bulk_cache[cache_key] = (now, dict(frames))
    logger.info("bulk_load cache MISS | key=%s | tickers=%s", cache_key, len(frames))
    return frames


def clear_bulk_cache():
    """Clear the bulk_load_dataframes cache (used by force refresh)."""
    with _bulk_cache_lock:
        _bulk_cache.clear()
    logger.info("bulk_load cache CLEARED (force refresh)")


def _compute_index_technicals(df: pd.DataFrame, ticker: str) -> Dict:
    """Compute comprehensive technicals for a market index (SPY, QQQ, etc.).

    Returns daily SMAs/EMAs, true weekly SMAs, MACD, RSI, and distances.
    """
    close = df["close"].astype(float)
    price = float(close.iloc[-1])

    # Daily SMAs
    sma20 = float(close.rolling(20).mean().iloc[-1])
    sma50 = float(close.rolling(50).mean().iloc[-1])
    sma200 = float(close.rolling(200).mean().iloc[-1])

    # Daily EMAs (9/21 — short-term momentum)
    ema9 = float(close.ewm(span=9, adjust=False).mean().iloc[-1])
    ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
    ema_bullish = ema9 > ema21

    # True weekly SMAs — resample to actual weeks
    weekly_close = close.resample("W-FRI").last().dropna()
    wsma50 = float(weekly_close.rolling(50).mean().iloc[-1]) if len(weekly_close) >= 50 else None
    wsma200 = float(weekly_close.rolling(200).mean().iloc[-1]) if len(weekly_close) >= 200 else None

    # MACD (12/26/9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_signal
    macd_val = float(macd_line.iloc[-1])
    macd_sig = float(macd_signal.iloc[-1])
    macd_h = float(macd_hist.iloc[-1])
    # MACD histogram trend (rising/falling)
    if len(macd_hist) >= 3:
        h_prev = float(macd_hist.iloc[-3])
        macd_hist_trend = "Rising" if macd_h > h_prev else "Falling"
    else:
        macd_hist_trend = "N/A"

    # RSI (14-period)
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    rsi = float(rsi_series.iloc[-1]) if not np.isnan(rsi_series.iloc[-1]) else 50.0

    # Distances
    dist_20 = round((price - sma20) / sma20 * 100, 2)
    dist_50 = round((price - sma50) / sma50 * 100, 2)
    dist_200 = round((price - sma200) / sma200 * 100, 2)
    ma_spread = round((sma50 - sma200) / sma200 * 100, 2)

    # 52-week high & drawdown
    high_52w = float(close.tail(252).max()) if len(close) >= 252 else float(close.max())
    drawdown = round((price - high_52w) / high_52w * 100, 2)

    # 20-day change
    chg_20d = round((price / float(close.iloc[-21]) - 1) * 100, 2) if len(close) >= 21 else 0.0

    golden_cross = sma50 > sma200

    return {
        "ticker": ticker,
        "price": round(price, 2),
        "sma_20": round(sma20, 2),
        "sma_50": round(sma50, 2),
        "sma_200": round(sma200, 2),
        "ema_9": round(ema9, 2),
        "ema_21": round(ema21, 2),
        "ema_bullish": ema_bullish,
        "wsma_50": round(wsma50, 2) if wsma50 else None,
        "wsma_200": round(wsma200, 2) if wsma200 else None,
        "macd": round(macd_val, 2),
        "macd_signal": round(macd_sig, 2),
        "macd_histogram": round(macd_h, 2),
        "macd_hist_trend": macd_hist_trend,
        "rsi": round(rsi, 1),
        "dist_from_20": dist_20,
        "dist_from_50": dist_50,
        "dist_from_200": dist_200,
        "ma_spread_50_200": ma_spread,
        "golden_cross": golden_cross,
        "drawdown_from_52w_high": drawdown,
        "chg_20d": chg_20d,
    }


def analyze_market_regime(spy_df: pd.DataFrame, qqq_df: pd.DataFrame = None) -> Dict:
    """Analyze SPY (+ optionally QQQ) to determine overall market regime.

    Uses 20/50/200 SMA structure, EMA 9/21, MACD, RSI to classify:
      Strong Bull / Bull / Caution / Bear Rally / Bear / Strong Bear

    When QQQ is provided, also checks for SPY-QQQ divergence.
    """
    if spy_df is None or len(spy_df) < 200:
        return {"regime": "Unknown", "description": "Insufficient SPY data",
                "caution_buy": False, "caution_sell": False, "spy": {}, "qqq": None}

    spy = _compute_index_technicals(spy_df, "SPY")

    price = spy["price"]
    sma200 = spy["sma_200"]
    rsi = spy["rsi"]
    above_200 = price > sma200
    above_20 = price > spy["sma_20"]
    golden_cross = spy["golden_cross"]
    death_cross = not golden_cross

    # Determine primary regime from SPY
    if above_200 and golden_cross and above_20:
        regime = "Strong Bull"
    elif above_200 and golden_cross and not above_20:
        regime = "Bull"
    elif above_200 and death_cross:
        regime = "Caution"
    elif not above_200 and above_20 and rsi > 40:
        regime = "Bear Rally"
    elif not above_200 and death_cross and rsi > 30:
        regime = "Bear"
    elif not above_200 and rsi <= 30:
        regime = "Strong Bear"
    else:
        regime = "Caution"

    caution_buy = regime in ("Bear", "Strong Bear", "Bear Rally")
    caution_sell = regime in ("Strong Bull", "Bull")

    # QQQ analysis for divergence detection
    qqq = None
    divergence = None
    if qqq_df is not None and len(qqq_df) >= 200:
        qqq = _compute_index_technicals(qqq_df, "QQQ")
        spy_bear = not above_200
        qqq_bear = qqq["price"] < qqq["sma_200"]
        spy_ema_bull = spy["ema_bullish"]
        qqq_ema_bull = qqq["ema_bullish"]
        if spy_bear and not qqq_bear:
            divergence = "QQQ stronger — tech leading"
        elif not spy_bear and qqq_bear:
            divergence = "QQQ weaker — tech lagging"
        elif spy_ema_bull and not qqq_ema_bull:
            divergence = "QQQ short-term weaker"
        elif not spy_ema_bull and qqq_ema_bull:
            divergence = "QQQ short-term stronger"

    descriptions = {
        "Strong Bull": "All MAs aligned bullish. Trend is strong.",
        "Bull": "Uptrend intact but short-term pullback underway.",
        "Caution": "Mixed signals. Trend may be shifting.",
        "Bear Rally": "Below 200 SMA but bouncing. Potential dead-cat bounce.",
        "Bear": "Downtrend confirmed. Use caution with buy signals.",
        "Strong Bear": "Oversold in downtrend. Reversal possible but risky.",
    }
    description = descriptions.get(regime, "Insufficient data to determine regime.")

    return {
        "regime": regime,
        "description": description,
        "caution_buy": caution_buy,
        "caution_sell": caution_sell,
        "divergence": divergence,
        "spy": spy,
        "qqq": qqq,
    }


def download_historical_data(
    ticker: str,
    period: str = "1y",
    interval: str = "1d",
    prefer_db: bool = True,
) -> Optional[pd.DataFrame]:
    """Reads historical data from the appropriate database table based on interval.
    
    Routes to:
      - stock_prices_daily for '1d', '1wk'
      - stock_prices_hourly for '1h' 
      - stock_prices_intraday for '1m', '5m', '15m', '30m'
    
    The prefer_db parameter is kept for API compatibility.
    Use update scripts to populate/refresh DB data.
    """
    # Map period to number of candles
    period_map = {
        "1d": 1, "5d": 5, "1mo": 30, "3mo": 90, 
        "6mo": 180, "1y": 365, "2y": 730, "4y": 1460, "5y": 1825
    }
    periods = period_map.get(period, 365)
    
    # Scale for intraday intervals (period_map values are in trading days)
    candles_per_day = {"1m": 390, "5m": 78, "15m": 26, "30m": 13, "1h": 7}
    if interval in candles_per_day:
        periods = periods * candles_per_day[interval]
    
    # For weekly, fetch more daily data to aggregate
    if interval == "1wk":
        periods = periods * 7
    
    try:
        if interval in ("1d", "1wk"):
            # Use existing daily data function
            df = get_data_from_db(ticker, periods)
        elif interval == "1h":
            # Use hourly data
            from database import get_hourly_data
            rows = get_hourly_data(ticker, periods)
            if not rows:
                return None
            df = pd.DataFrame(rows)
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df.set_index('datetime', inplace=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = df[col].astype(float)
        elif interval in ("15m", "30m"):
            # Use SQL-based aggregation from 5m candles
            from database import _aggregate_from_5m
            data = _aggregate_from_5m([ticker], interval, periods)
            rows = data.get(ticker)
            if not rows:
                return None
            df = pd.DataFrame(rows)
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df.set_index('datetime', inplace=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = df[col].astype(float)
        else:
            # Use intraday data (1m, 5m)
            from database import get_intraday_data
            rows = get_intraday_data(ticker, interval, periods)
            if not rows:
                return None
            df = pd.DataFrame(rows)
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df.set_index('datetime', inplace=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = df[col].astype(float)
        
        if df is None or df.empty:
            logger.warning("No DB data for %s (interval=%s, periods=%d)", ticker, interval, periods)
            return None
        
        # Aggregate to weekly if requested
        if interval == "1wk" and not df.empty:
            df = df.resample('W').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
        
        return df
        
    except Exception as e:
        logger.error("Error getting data for %s: %s", ticker, e)
        return None


def identify_gap_up(df: pd.DataFrame, gap_threshold: float = 0.01) -> List[Dict]:
    """Identifies gap-up patterns where current open > previous high (vectorized).
    Only includes gaps where the zone survived end-of-day (curr_low > prev_high)."""
    prev_high = df["high"].values[:-1]
    curr_open = df["open"].values[1:]
    curr_low = df["low"].values[1:]
    gap_pct = (curr_open - prev_high) / prev_high
    # Gap must open above prev high AND the day's low must stay above prev high
    # (otherwise the gap was filled on the same day — no valid zone exists)
    mask = (gap_pct >= gap_threshold) & (curr_low > prev_high)
    indices = np.where(mask)[0] + 1  # +1 to get actual df position

    gaps = []
    dates = df.index
    for idx in indices:
        gaps.append({
            "index": int(idx),
            "date": dates[idx],
            "prev_high": float(prev_high[idx - 1]),
            "gap_open": float(curr_open[idx - 1]),
            "gap_low": float(curr_low[idx - 1]),
            "gap_pct": round(float(gap_pct[idx - 1]) * 100, 2)
        })
    return gaps


def identify_gap_down(df: pd.DataFrame, gap_threshold: float = 0.01) -> List[Dict]:
    """Identifies gap-down patterns where current open < previous low (vectorized).
    Only includes gaps where the zone survived end-of-day (curr_high < prev_low)."""
    prev_low = df["low"].values[:-1]
    curr_open = df["open"].values[1:]
    curr_high = df["high"].values[1:]
    gap_pct = (prev_low - curr_open) / prev_low
    # Gap must open below prev low AND the day's high must stay below prev low
    # (otherwise the gap was filled on the same day — no valid zone exists)
    mask = (gap_pct >= gap_threshold) & (curr_high < prev_low)
    indices = np.where(mask)[0] + 1

    gaps = []
    dates = df.index
    for idx in indices:
        gaps.append({
            "index": int(idx),
            "date": dates[idx],
            "prev_low": float(prev_low[idx - 1]),
            "gap_open": float(curr_open[idx - 1]),
            "gap_high": float(curr_high[idx - 1]),
            "gap_pct": round(float(gap_pct[idx - 1]) * 100, 2)
        })
    return gaps


def check_gap_status(df: pd.DataFrame, gap_ups: List[Dict], gap_downs: List[Dict], entry_threshold: float = 0.02) -> List[Dict]:
    """
    Checks if the last close is near any unfilled gaps.
    Returns ALL relevant gap strategy signals as a list.
    Uses numpy arrays for fast min/max on post-gap slices.
    """
    if df.empty:
        return []

    last_close = float(df.iloc[-1]["close"])
    low_arr = df["low"].values
    high_arr = df["high"].values
    signals = []
    seen = set()  # deduplicate by (gap_type, gap_date)

    # Check gap-ups (potential support levels)
    for gap in reversed(gap_ups):
        gap_low = gap["gap_low"]      # curr_low at gap day
        gap_high = gap["prev_high"]    # prev_high
        gi = gap["index"]

        # Check if gap is still unfilled (price hasn't gone below gap_high)
        gap_filled = low_arr[gi:].min() <= gap_high

        if not gap_filled:
            # Price sitting inside an unfilled gap-up zone
            if last_close > gap_high and last_close < gap_low:
                key = ("Possible Downside (In Gap Up)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "Possible Downside (In Gap Up)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high,
                        "gap_high": gap_low,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })
            elif abs(last_close - gap_low) / gap_low <= entry_threshold:
                key = ("At Support (Unfilled Gap Up)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "At Support (Unfilled Gap Up)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high,
                        "gap_high": gap_low,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })
        else:
            # Gap was filled — check inside zone first, then edge proximity
            if last_close > gap_high and last_close < gap_low:
                key = ("Possible Downside (In Gap Up)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "Possible Downside (In Gap Up)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high,
                        "gap_high": gap_low,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })
            elif abs(last_close - gap_low) / gap_low <= entry_threshold:
                key = ("At Support (Filled Gap Up)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "At Support (Filled Gap Up)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high,
                        "gap_high": gap_low,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })

    # Check gap-downs (potential resistance levels)
    for gap in reversed(gap_downs):
        gap_high_val = gap["gap_high"]  # curr_high at gap day
        gap_low_val = gap["prev_low"]   # prev_low
        gi = gap["index"]

        gap_filled = high_arr[gi:].max() >= gap_low_val

        if not gap_filled:
            if last_close > gap_high_val and last_close < gap_low_val:
                key = ("Possible Upside (In Gap Down)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "Possible Upside (In Gap Down)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high_val,
                        "gap_high": gap_low_val,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })
            elif abs(last_close - gap_low_val) / gap_low_val <= entry_threshold:
                key = ("At Resistance (Unfilled Gap Down)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "At Resistance (Unfilled Gap Down)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high_val,
                        "gap_high": gap_low_val,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })
        else:
            # Gap was filled — check inside zone first, then edge proximity
            if last_close > gap_high_val and last_close < gap_low_val:
                key = ("Possible Upside (In Gap Down)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "Possible Upside (In Gap Down)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high_val,
                        "gap_high": gap_low_val,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })
            elif last_close <= gap_high_val and abs(last_close - gap_high_val) / gap_high_val <= entry_threshold:
                key = ("At Resistance (Filled Gap Down)", gap["date"])
                if key not in seen:
                    seen.add(key)
                    signals.append({
                        "gap_type": "At Resistance (Filled Gap Down)",
                        "gap_date": gap["date"],
                        "gap_low": gap_high_val,
                        "gap_high": gap_low_val,
                        "last_close": last_close,
                        "gap_pct": gap["gap_pct"],
                        "gap_index": gi,
                    })

    return signals


def scan_gap_strategies(ticker: str, df: pd.DataFrame = None, min_atr_ratio: float = 0.5) -> List[Dict]:
    """
    Scans a ticker for gap-based strategies.
    Returns a list of all relevant gap signals for the ticker.
    Filters out gaps smaller than min_atr_ratio × ATR(14) to exclude noise.
    """
    if df is None:
        df = download_historical_data(ticker, period="1y", interval="1d")
    
    if df is None or len(df) < 20:
        return []
    
    # Normalize column names
    df.columns = [col.lower() for col in df.columns]
    
    # Pre-compute ATR(14) series for gap size filtering
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    # Wilder's smoothing (EWM with alpha=1/14)
    atr_arr = np.empty(len(tr))
    atr_arr[0] = tr[0]
    alpha = 1.0 / 14
    for i in range(1, len(tr)):
        atr_arr[i] = alpha * tr[i] + (1 - alpha) * atr_arr[i - 1]
    # Pad index 0 so atr_lookup[i] corresponds to df row i (row 0 has no ATR)
    atr_lookup = np.concatenate(([np.nan], atr_arr))

    gap_ups = identify_gap_up(df, gap_threshold=0.01)
    gap_downs = identify_gap_down(df, gap_threshold=0.01)
    
    signals = check_gap_status(df, gap_ups, gap_downs)

    # Current bar O/H/L
    last_row = df.iloc[-1]
    current_open = round(float(last_row["open"]), 2)
    current_high = round(float(last_row["high"]), 2)
    current_low = round(float(last_row["low"]), 2)

    # Trend: price vs 50MA & 200MA
    close_series = df["close"]
    ma50 = float(close_series.rolling(50).mean().iloc[-1]) if len(df) >= 50 else None
    ma200 = float(close_series.rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
    last_close_val = float(last_row["close"])

    if ma50 is not None and ma200 is not None:
        if last_close_val > ma50 and ma50 > ma200:
            trend = "Bullish"
        elif last_close_val < ma50 and ma50 < ma200:
            trend = "Bearish"
        elif last_close_val > ma50:
            trend = "Neutral-Bullish"
        else:
            trend = "Neutral-Bearish"
    elif ma50 is not None:
        trend = "Bullish" if last_close_val > ma50 else "Bearish"
    else:
        trend = "N/A"

    close_arr = df["close"].values

    results = []
    for s in signals:
        gap_size = abs(s["gap_high"] - s["gap_low"])
        # Look up ATR at the gap date index
        gi = s.get("gap_index", None)
        if gi is not None and gi < len(atr_lookup):
            atr_at_gap = float(atr_lookup[gi])
        else:
            # Fallback: use current (last) ATR
            atr_at_gap = float(atr_lookup[-1])

        if atr_at_gap > 0:
            gap_atr_ratio = round(gap_size / atr_at_gap, 2)
        else:
            gap_atr_ratio = 0.0

        # Skip gaps smaller than min_atr_ratio × ATR (noise)
        if gap_atr_ratio < min_atr_ratio:
            continue

        is_inside = "In Gap" in s["gap_type"]

        # Determine entry direction for inside-gap signals
        # Scan backward to find the last close that was outside the gap zone
        entry_dir = None
        if is_inside:
            g_lo = s["gap_low"]
            g_hi = s["gap_high"]
            for i in range(len(close_arr) - 2, -1, -1):
                c_val = float(close_arr[i])
                if c_val <= g_lo:
                    entry_dir = "rally"   # came from below the gap
                    break
                elif c_val >= g_hi:
                    entry_dir = "drop"    # came from above the gap
                    break
            if entry_dir is None:
                # All history inside gap — fallback to position within zone
                entry_dir = "rally" if last_close_val < (g_lo + g_hi) / 2 else "drop"

        results.append({
            "ticker": ticker,
            "gap_type": s["gap_type"],
            "gap_low": round(s["gap_low"], 2),
            "gap_high": round(s["gap_high"], 2),
            "last_close": round(s["last_close"], 2),
            "current_open": current_open,
            "current_high": current_high,
            "current_low": current_low,
            "trend": trend,
            "gap_diff": round(gap_size, 2),
            "gap_pct": s["gap_pct"],
            "gap_atr_ratio": gap_atr_ratio,
            "gap_date": s["gap_date"].strftime("%Y-%m-%d") if hasattr(s["gap_date"], "strftime") else str(s["gap_date"]),
            "entry_direction": entry_dir,
        })
    return results


def scan_fair_value_gaps(ticker: str, df: pd.DataFrame = None, lookback: int = 50,
                         min_atr_ratio: float = 0.3) -> List[Dict]:
    """
    Scan for Fair Value Gaps (FVGs) — 3-candle imbalance zones.

    Bullish FVG: candle[i].low > candle[i-2].high  (gap between candle 1 high and candle 3 low)
    Bearish FVG: candle[i].high < candle[i-2].low  (gap between candle 1 low and candle 3 high)

    Returns unmitigated and recently-mitigated FVGs with streak analysis.
    """
    if df is None or len(df) < max(20, lookback):
        return []

    df.columns = [col.lower() for col in df.columns]

    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    o = df["open"].values
    v = df["volume"].values if "volume" in df.columns else np.zeros(len(df))
    dates = df.index

    # ATR(14) for filtering noise
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    atr_arr = np.empty(len(tr))
    atr_arr[0] = tr[0]
    alpha = 1.0 / 14
    for i in range(1, len(tr)):
        atr_arr[i] = alpha * tr[i] + (1 - alpha) * atr_arr[i - 1]
    atr_lookup = np.concatenate(([np.nan], atr_arr))

    # Trend: 50MA / 200MA
    close_series = df["close"]
    ma50 = float(close_series.rolling(50).mean().iloc[-1]) if len(df) >= 50 else None
    ma200 = float(close_series.rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
    last_close_val = float(df.iloc[-1]["close"])
    last_row = df.iloc[-1]

    if ma50 is not None and ma200 is not None:
        if last_close_val > ma50 and ma50 > ma200:
            trend = "Bullish"
        elif last_close_val < ma50 and ma50 < ma200:
            trend = "Bearish"
        elif last_close_val > ma50:
            trend = "Neutral-Bullish"
        else:
            trend = "Neutral-Bearish"
    elif ma50 is not None:
        trend = "Bullish" if last_close_val > ma50 else "Bearish"
    else:
        trend = "N/A"

    # Scan start: only look at last `lookback` bars
    start_idx = max(2, len(df) - lookback)
    fvgs = []

    for i in range(start_idx, len(df)):
        # Bullish FVG: candle i low > candle i-2 high (gap = demand zone)
        if l[i] > h[i - 2]:
            fvg_low = float(h[i - 2])
            fvg_high = float(l[i])
            fvg_size = fvg_high - fvg_low
            atr_val = float(atr_lookup[i]) if i < len(atr_lookup) and not np.isnan(atr_lookup[i]) else float(atr_lookup[-1])
            if atr_val > 0 and (fvg_size / atr_val) < min_atr_ratio:
                continue
            fvgs.append({
                "direction": "Bullish",
                "fvg_low": fvg_low,
                "fvg_high": fvg_high,
                "fvg_size": fvg_size,
                "atr_ratio": round(fvg_size / atr_val, 2) if atr_val > 0 else 0.0,
                "date": dates[i],
                "index": i,
                "candle_body": abs(float(c[i]) - float(o[i])),
                "volume": float(v[i]),
            })

        # Bearish FVG: candle i high < candle i-2 low (gap = supply zone)
        if h[i] < l[i - 2]:
            fvg_low = float(h[i])
            fvg_high = float(l[i - 2])
            fvg_size = fvg_high - fvg_low
            atr_val = float(atr_lookup[i]) if i < len(atr_lookup) and not np.isnan(atr_lookup[i]) else float(atr_lookup[-1])
            if atr_val > 0 and (fvg_size / atr_val) < min_atr_ratio:
                continue
            fvgs.append({
                "direction": "Bearish",
                "fvg_low": fvg_low,
                "fvg_high": fvg_high,
                "fvg_size": fvg_size,
                "atr_ratio": round(fvg_size / atr_val, 2) if atr_val > 0 else 0.0,
                "date": dates[i],
                "index": i,
                "candle_body": abs(float(c[i]) - float(o[i])),
                "volume": float(v[i]),
            })

    # Check mitigation status and build results
    results = []
    bull_streak = 0
    bear_streak = 0
    bull_unmitigated = 0
    bear_unmitigated = 0

    for fvg in fvgs:
        gi = fvg["index"]
        fvg_low = fvg["fvg_low"]
        fvg_high = fvg["fvg_high"]
        midpoint = (fvg_low + fvg_high) / 2

        # Check if mitigated: price entered the FVG zone after formation
        if fvg["direction"] == "Bullish":
            # Mitigated if any subsequent low went into or below the zone
            post_lows = l[gi + 1:] if gi + 1 < len(l) else np.array([])
            if len(post_lows) > 0 and post_lows.min() <= fvg_high:
                # Partially mitigated if only touched top, fully if went through midpoint
                if post_lows.min() <= midpoint:
                    status = "Mitigated"
                else:
                    status = "Partially Mitigated"
            else:
                status = "Unmitigated"
                bull_unmitigated += 1
        else:
            # Mitigated if any subsequent high went into or above the zone
            post_highs = h[gi + 1:] if gi + 1 < len(h) else np.array([])
            if len(post_highs) > 0 and post_highs.max() >= fvg_low:
                if post_highs.max() >= midpoint:
                    status = "Mitigated"
                else:
                    status = "Partially Mitigated"
            else:
                status = "Unmitigated"
                bear_unmitigated += 1

        # Proximity: how close is last_close to the FVG zone?
        if last_close_val >= fvg_low and last_close_val <= fvg_high:
            proximity = "Inside"
        elif fvg["direction"] == "Bullish":
            dist = (last_close_val - fvg_high) / fvg_high if last_close_val > fvg_high else (fvg_low - last_close_val) / fvg_low
            proximity = "Near" if dist < 0.02 else "Away"
        else:
            dist = (fvg_low - last_close_val) / fvg_low if last_close_val < fvg_low else (last_close_val - fvg_high) / fvg_high
            proximity = "Near" if dist < 0.02 else "Away"

        # Trend alignment
        aligned = (fvg["direction"] == "Bullish" and trend in ("Bullish", "Neutral-Bullish")) or \
                  (fvg["direction"] == "Bearish" and trend in ("Bearish", "Neutral-Bearish"))

        fvg_pct = round((fvg["fvg_size"] / fvg_low) * 100, 2) if fvg_low > 0 else 0.0

        results.append({
            "ticker": ticker,
            "fvg_type": f"{fvg['direction']} FVG",
            "status": status,
            "fvg_low": round(fvg_low, 2),
            "fvg_high": round(fvg_high, 2),
            "fvg_size": round(fvg["fvg_size"], 2),
            "fvg_pct": fvg_pct,
            "atr_ratio": fvg["atr_ratio"],
            "last_close": round(last_close_val, 2),
            "current_open": round(float(last_row["open"]), 2),
            "current_high": round(float(last_row["high"]), 2),
            "current_low": round(float(last_row["low"]), 2),
            "proximity": proximity,
            "trend": trend,
            "trend_aligned": aligned,
            "gap_date": fvg["date"].strftime("%Y-%m-%d") if hasattr(fvg["date"], "strftime") else str(fvg["date"]),
        })

    # Streak analysis: count consecutive same-direction FVGs from most recent
    if fvgs:
        last_dir = fvgs[-1]["direction"]
        streak_count = 0
        for fvg in reversed(fvgs):
            if fvg["direction"] == last_dir:
                streak_count += 1
            else:
                break
        streak_dir = last_dir
    else:
        streak_count = 0
        streak_dir = None

    # Attach streak info to each result
    for r in results:
        r["streak_count"] = streak_count
        r["streak_direction"] = streak_dir
        r["bull_unmitigated"] = bull_unmitigated
        r["bear_unmitigated"] = bear_unmitigated
        r["total_fvgs"] = len(fvgs)

    return results


def scan_moving_average_crossover(ticker: str, df: pd.DataFrame = None, short_period: int = 9, long_period: int = 21, interval: str = "1d") -> Optional[Dict]:
    """
    Scans for moving average crossover signals with enriched context.

    Returns signal type, MA spread, days since last crossover,
    price change since crossover, and price distance from MAs.
    Also detects important markers: Golden Cross, Death Cross,
    price relative to key moving averages.

    Interval-aware:
      1d  – weekly resampling + SMA 200 + weekly 50W/200W markers
      1h  – daily resampling (as higher-TF) + SMA 200 (200-bar)
      5m/15m/30m – crossover only, no higher-TF resampling
    """
    if df is None:
        df = download_historical_data(ticker, period="2y", interval="1d")

    if df is None or len(df) < long_period + 5:
        return None

    df.columns = [col.lower() for col in df.columns]

    short_ma = df["close"].rolling(window=short_period).mean()
    long_ma = df["close"].rolling(window=long_period).mean()

    last_short = short_ma.iloc[-1]
    last_long = long_ma.iloc[-1]
    last_close = float(df.iloc[-1]["close"])

    # --- Detect crossover by scanning backward ---
    # diff > 0 means short above long
    diff = short_ma - long_ma
    valid = diff.dropna()
    if len(valid) < 2:
        return None

    currently_above = valid.iloc[-1] > 0

    # Walk backward to find the last sign change (crossover day)
    days_since_cross = 0
    crossover_date = None
    crossover_price = None
    for i in range(len(valid) - 1, 0, -1):
        prev_above = valid.iloc[i - 1] > 0
        curr_above = valid.iloc[i] > 0
        if prev_above != curr_above:
            days_since_cross = len(valid) - 1 - i
            crossover_date = valid.index[i]
            crossover_price = float(df.loc[valid.index[i], "close"])
            break

    # Classify signal
    if days_since_cross == 0 and crossover_date is not None:
        signal = "Bullish Crossover" if currently_above else "Bearish Crossover"
    elif days_since_cross <= 5 and crossover_date is not None:
        signal = "Recent Bullish" if currently_above else "Recent Bearish"
    elif currently_above:
        signal = "Above MA"
    else:
        signal = "Below MA"

    # --- Enriched metrics ---
    ma_spread_pct = round((last_short - last_long) / last_long * 100, 2)
    price_vs_short_pct = round((last_close - last_short) / last_short * 100, 2)
    price_vs_long_pct = round((last_close - last_long) / last_long * 100, 2)

    price_change_since_cross = None
    if crossover_price and crossover_price > 0:
        price_change_since_cross = round((last_close - crossover_price) / crossover_price * 100, 2)

    # --- Higher-TF MAs (interval-dependent) ---
    weekly_short_ma = None
    weekly_long_ma = None
    weekly_spread_pct = None
    weekly_signal = None

    if interval == "1d":
        # Weekly resampling from daily data
        wk = df["close"].resample("W-FRI").last().dropna()
        if len(wk) >= long_period + 2:
            wk_short = wk.rolling(window=short_period).mean()
            wk_long = wk.rolling(window=long_period).mean()
            if not (pd.isna(wk_short.iloc[-1]) or pd.isna(wk_long.iloc[-1])):
                weekly_short_ma = round(float(wk_short.iloc[-1]), 2)
                weekly_long_ma = round(float(wk_long.iloc[-1]), 2)
                weekly_spread_pct = round((weekly_short_ma - weekly_long_ma) / weekly_long_ma * 100, 2)
                wk_diff = wk_short - wk_long
                wk_valid = wk_diff.dropna()
                if len(wk_valid) >= 2:
                    wk_above_now = wk_valid.iloc[-1] > 0
                    wk_above_prev = wk_valid.iloc[-2] > 0
                    if wk_above_now != wk_above_prev:
                        weekly_signal = "W-Bullish Cross" if wk_above_now else "W-Bearish Cross"
                    elif wk_above_now:
                        weekly_signal = "W-Above"
                    else:
                        weekly_signal = "W-Below"

    elif interval == "1h":
        # Daily resampling from hourly data as higher-TF
        daily = df["close"].resample("D").last().dropna()
        if len(daily) >= long_period + 2:
            d_short = daily.rolling(window=short_period).mean()
            d_long = daily.rolling(window=long_period).mean()
            if not (pd.isna(d_short.iloc[-1]) or pd.isna(d_long.iloc[-1])):
                weekly_short_ma = round(float(d_short.iloc[-1]), 2)
                weekly_long_ma = round(float(d_long.iloc[-1]), 2)
                weekly_spread_pct = round((weekly_short_ma - weekly_long_ma) / weekly_long_ma * 100, 2)
                d_diff = d_short - d_long
                d_valid = d_diff.dropna()
                if len(d_valid) >= 2:
                    d_above_now = d_valid.iloc[-1] > 0
                    d_above_prev = d_valid.iloc[-2] > 0
                    if d_above_now != d_above_prev:
                        weekly_signal = "D-Bullish Cross" if d_above_now else "D-Bearish Cross"
                    elif d_above_now:
                        weekly_signal = "D-Above"
                    else:
                        weekly_signal = "D-Below"
    # 5m/15m/30m: no higher-TF resampling

    # --- Important markers ---
    markers = []

    # Golden Cross / Death Cross (50/200 SMA) — only for daily/hourly with enough bars
    if len(df) >= 205 and interval in ("1d", "1h"):
        sma50 = df["close"].rolling(window=50).mean()
        sma200 = df["close"].rolling(window=200).mean()
        diff_gd = sma50 - sma200
        valid_gd = diff_gd.dropna()
        if len(valid_gd) >= 2:
            gd_above = valid_gd.iloc[-1] > 0
            # Find how many days since last 50/200 cross
            gd_days = None
            for j in range(len(valid_gd) - 1, 0, -1):
                if (valid_gd.iloc[j - 1] > 0) != (valid_gd.iloc[j] > 0):
                    gd_days = len(valid_gd) - 1 - j
                    break
            if gd_days is not None and gd_days <= 10:
                markers.append("Golden Cross" if gd_above else "Death Cross")
            elif gd_above:
                markers.append("Above 200 SMA")
            else:
                markers.append("Below 200 SMA")

            # Price vs 200 SMA
            last_sma200 = float(sma200.iloc[-1])
            pct_from_200 = (last_close - last_sma200) / last_sma200 * 100
            if abs(pct_from_200) <= 2:
                markers.append("Near 200 SMA")

            # Price vs 50 SMA
            last_sma50 = float(sma50.iloc[-1])
            pct_from_50 = (last_close - last_sma50) / last_sma50 * 100
            if abs(pct_from_50) <= 1:
                markers.append("Near 50 SMA")

    # Weekly 50W / 200W SMA markers — only for daily interval
    if interval == "1d":
        wk = df["close"].resample("W-FRI").last().dropna()
        if len(wk) >= 52:
            wk_sma50 = wk.rolling(window=50).mean()
            if not pd.isna(wk_sma50.iloc[-1]):
                last_wk_sma50 = float(wk_sma50.iloc[-1])
                pct_from_50w = (last_close - last_wk_sma50) / last_wk_sma50 * 100
                if abs(pct_from_50w) <= 2:
                    markers.append("Near 50W SMA")
                elif pct_from_50w > 0:
                    markers.append("Above 50W SMA")
                else:
                    markers.append("Below 50W SMA")
        if len(wk) >= 202:
            wk_sma200 = wk.rolling(window=200).mean()
            if not pd.isna(wk_sma200.iloc[-1]):
                last_wk_sma200 = float(wk_sma200.iloc[-1])
                pct_from_200w = (last_close - last_wk_sma200) / last_wk_sma200 * 100
                if abs(pct_from_200w) <= 3:
                    markers.append("Near 200W SMA")
                elif pct_from_200w > 0:
                    markers.append("Above 200W SMA")
                else:
                    markers.append("Below 200W SMA")

    return {
        "ticker": ticker,
        "signal": signal,
        "short_ma": round(last_short, 2),
        "long_ma": round(last_long, 2),
        "last_close": round(last_close, 2),
        "ma_spread_pct": ma_spread_pct,
        "price_vs_short_pct": price_vs_short_pct,
        "price_vs_long_pct": price_vs_long_pct,
        "days_since_cross": days_since_cross if crossover_date else None,
        "crossover_date": crossover_date.strftime("%Y-%m-%d") if crossover_date and hasattr(crossover_date, "strftime") else None,
        "price_at_cross": round(crossover_price, 2) if crossover_price else None,
        "price_change_since_cross_pct": price_change_since_cross,
        "weekly_short_ma": weekly_short_ma,
        "weekly_long_ma": weekly_long_ma,
        "weekly_spread_pct": weekly_spread_pct,
        "weekly_signal": weekly_signal,
        "markers": markers,
        "date": df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1]),
    }


def scan_momentum_pullback(ticker: str, df: pd.DataFrame = None, interval: str = "1d") -> Optional[Dict]:
    """
    Elite Momentum Pullback scanner.

    3-pillar methodology:
    1. Trend Anchor   – EMA stack ≥2/4 pairs aligned +
                        higher-TF confirmation OR price above SMA 200.
    2. Pullback Zone  – Slow Stochastic %K < 40, ADX 15-55,
                        price within 2 ATR of EMA 21, RSI 30-60.
    3. Entry Quality  – Composite score (A+/A/B+/B/C) based on stochastics
                        depth, EMA 21 proximity, stack alignment, relative
                        volume, and ADX strength.

    Interval-aware:
      1d  – weekly resampling + SMA 200, min 210 bars
      1h  – daily resampling + SMA 200 (200-bar), min 200 bars
      15m/30m/5m – EMA stack only (no higher-TF), min 100 bars
    """
    if df is None:
        df = download_historical_data(ticker, period="2y", interval="1d")

    # ── Interval-specific thresholds ──
    if interval in ("5m", "15m", "30m"):
        min_bars = 100
    elif interval == "1h":
        min_bars = 200
    else:
        min_bars = 210

    if df is None or len(df) < min_bars:
        return None

    df.columns = [col.lower() for col in df.columns]
    close = df["close"]
    high = df["high"]
    low = df["low"]
    last_close = float(close.iloc[-1])

    # ── EMAs ──
    ema8 = close.ewm(span=8, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema34 = close.ewm(span=34, adjust=False).mean()
    ema55 = close.ewm(span=55, adjust=False).mean()
    ema89 = close.ewm(span=89, adjust=False).mean()

    daily_stack = (
        float(ema8.iloc[-1]) > float(ema21.iloc[-1]) > float(ema34.iloc[-1])
        > float(ema55.iloc[-1]) > float(ema89.iloc[-1])
    )

    # Count how many of the 5 EMAs are in order (for partial scoring)
    daily_emas = [float(ema8.iloc[-1]), float(ema21.iloc[-1]),
                  float(ema34.iloc[-1]), float(ema55.iloc[-1]),
                  float(ema89.iloc[-1])]
    stack_count = sum(1 for i in range(len(daily_emas) - 1)
                      if daily_emas[i] > daily_emas[i + 1])  # max 4

    # ── Higher-TF confirmation (interval-dependent) ──
    weekly_stack = False
    above_sma200 = False
    sma200_val = None

    if interval == "1d":
        # Weekly EMAs (resample daily → weekly)
        wk = close.resample("W-FRI").last().dropna()
        if len(wk) < 35:
            return None
        wk_ema8 = wk.ewm(span=8, adjust=False).mean()
        wk_ema21 = wk.ewm(span=21, adjust=False).mean()
        wk_ema34 = wk.ewm(span=34, adjust=False).mean()
        weekly_stack = (
            float(wk_ema8.iloc[-1]) > float(wk_ema21.iloc[-1])
            > float(wk_ema34.iloc[-1])
        )
        sma200 = close.rolling(window=200).mean()
        sma200_val = float(sma200.iloc[-1])
        above_sma200 = last_close > sma200_val

    elif interval == "1h":
        # Daily EMAs (resample hourly → daily) as higher-TF
        daily = close.resample("D").last().dropna()
        if len(daily) >= 10:
            d_ema8 = daily.ewm(span=8, adjust=False).mean()
            d_ema21 = daily.ewm(span=21, adjust=False).mean()
            d_ema34 = daily.ewm(span=34, adjust=False).mean()
            weekly_stack = (
                float(d_ema8.iloc[-1]) > float(d_ema21.iloc[-1])
                > float(d_ema34.iloc[-1])
            )
        if len(close) >= 200:
            sma200 = close.rolling(window=200).mean()
            sma200_val = float(sma200.iloc[-1])
            above_sma200 = last_close > sma200_val

    else:
        # 5m/15m/30m – no higher-TF resampling: use EMA stack alone as trend
        # weekly_stack stays False, above_sma200 stays False
        pass

    # ── Trend Anchor pass ──
    if interval in ("5m", "15m", "30m"):
        # For short intraday: just require strong EMA stacking (≥3 pairs)
        trend_pass = stack_count >= 3
    else:
        # Daily/hourly: at least 2 EMA pairs + higher-TF or SMA 200
        trend_pass = stack_count >= 2 and (weekly_stack or above_sma200)
    if not trend_pass:
        return None

    # ── Slow Stochastic (14, 3, 3) ──
    low14 = low.rolling(window=14).min()
    high14 = high.rolling(window=14).max()
    raw_k = (close - low14) / (high14 - low14) * 100
    slow_k = raw_k.rolling(window=3).mean()
    slow_d = slow_k.rolling(window=3).mean()
    stoch_k = float(slow_k.iloc[-1])
    stoch_d = float(slow_d.iloc[-1])

    # ── ADX (14-period, Wilder's smoothing) ──
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr14 = tr.ewm(alpha=1/14, adjust=False).mean()

    plus_dm = high.diff().clip(lower=0)
    minus_dm = (-low.diff()).clip(lower=0)
    # Zero out when the other is larger
    plus_dm = plus_dm.where(plus_dm > minus_dm, 0.0)
    minus_dm = minus_dm.where(minus_dm > plus_dm, 0.0)

    plus_di = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    adx = dx.ewm(alpha=1/14, adjust=False).mean()
    adx_val = float(adx.iloc[-1])

    # ── ATR (14) and Rubber-Band Rule ──
    atr_val = float(atr14.iloc[-1])
    ema21_val = float(ema21.iloc[-1])
    dist_to_ema21 = abs(last_close - ema21_val)

    # ── Pullback Zone check ──
    stoch_pullback = stoch_k < 40
    adx_healthy = 15 <= adx_val <= 55
    rubber_band = dist_to_ema21 <= 2 * atr_val

    pullback_pass = stoch_pullback and adx_healthy and rubber_band
    if not pullback_pass:
        return None

    # ── Relative volume (20-day avg) ──
    avg_vol_20 = df["volume"].rolling(window=20).mean()
    last_vol = float(df["volume"].iloc[-1])
    avg_vol = float(avg_vol_20.iloc[-1])
    rel_volume = round(last_vol / avg_vol, 2) if avg_vol > 0 else 0

    # ── RSI (14-period) ──
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = float(rsi_series.iloc[-1])

    # ── RSI gate: avoid oversold falling knives and overbought stocks ──
    if not (30 <= rsi_val <= 60):
        return None

    # ── Entry Quality Score ──
    # Stochastics depth: lower %K = deeper pullback = better (0-30 range ideal)
    stoch_score = max(0, min(100, (40 - stoch_k) / 40 * 100)) * 0.30

    # EMA 21 proximity: closer = better
    ema21_pct = dist_to_ema21 / ema21_val * 100 if ema21_val > 0 else 100
    proximity_score = max(0, min(100, (1 - ema21_pct / 3) * 100)) * 0.25

    # Stack alignment: 4/4 = perfect
    stack_score = (stack_count / 4) * 100 * 0.20

    # Relative volume
    vol_score = min(100, rel_volume * 50) * 0.15

    # ADX sweet spot (30-40 is ideal)
    adx_diff = abs(adx_val - 35)
    adx_score = max(0, min(100, (1 - adx_diff / 25) * 100)) * 0.10

    total_score = stoch_score + proximity_score + stack_score + vol_score + adx_score
    total_score = round(total_score, 1)

    if total_score >= 90:
        grade = "A+"
    elif total_score >= 80:
        grade = "A"
    elif total_score >= 70:
        grade = "B+"
    elif total_score >= 60:
        grade = "B"
    else:
        grade = "C"

    return {
        "ticker": ticker,
        "last_close": round(last_close, 2),
        "grade": grade,
        "score": total_score,
        "interval": interval,
        # Trend Anchor
        "daily_stack": daily_stack,
        "stack_count": stack_count,
        "weekly_stack": weekly_stack,
        "above_sma200": above_sma200,
        "sma200": round(sma200_val, 2) if sma200_val is not None else None,
        # Pullback Zone
        "stoch_k": round(stoch_k, 2),
        "stoch_d": round(stoch_d, 2),
        "adx": round(adx_val, 2),
        "atr": round(atr_val, 2),
        "ema21": round(ema21_val, 2),
        "dist_to_ema21_pct": round(ema21_pct, 2),
        "rubber_band": rubber_band,
        # Volume
        "rel_volume": rel_volume,
        "volume": int(last_vol),
        "avg_volume": int(avg_vol),
        "rsi": round(rsi_val, 2),
        "date": df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1]),
    }


def scan_bearish_bounce(ticker: str, df: pd.DataFrame = None, interval: str = "1d") -> Optional[Dict]:
    """
    Bearish Bounce scanner — mirror of Momentum Pullback.

    Finds stocks in confirmed downtrends that are bouncing up toward
    resistance, offering short/exit opportunities.

    3-pillar methodology:
    1. Trend Anchor   – Inverted EMA stack ≥2/4 pairs (89>55>34>21>8) +
                        higher-TF confirmation OR price below SMA 200.
    2. Bounce Zone    – Slow Stochastic %K > 60, ADX 15-55,
                        price within 2 ATR of EMA 21, RSI 40-70.
    3. Entry Quality  – Composite score (A+/A/B+/B/C) based on stochastic
                        overbought depth, EMA 21 proximity, stack alignment,
                        relative volume, and ADX strength.

    Interval-aware:
      1d  – weekly resampling + SMA 200, min 210 bars
      1h  – daily resampling + SMA 200 (200-bar), min 200 bars
      15m/30m/5m – EMA stack only (no higher-TF), min 100 bars
    """
    if df is None:
        df = download_historical_data(ticker, period="2y", interval="1d")

    # ── Interval-specific thresholds ──
    if interval in ("5m", "15m", "30m"):
        min_bars = 100
    elif interval == "1h":
        min_bars = 200
    else:
        min_bars = 210

    if df is None or len(df) < min_bars:
        return None

    df.columns = [col.lower() for col in df.columns]
    close = df["close"]
    high = df["high"]
    low = df["low"]
    last_close = float(close.iloc[-1])

    # ── EMAs ──
    ema8 = close.ewm(span=8, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema34 = close.ewm(span=34, adjust=False).mean()
    ema55 = close.ewm(span=55, adjust=False).mean()
    ema89 = close.ewm(span=89, adjust=False).mean()

    daily_stack = (
        float(ema89.iloc[-1]) > float(ema55.iloc[-1]) > float(ema34.iloc[-1])
        > float(ema21.iloc[-1]) > float(ema8.iloc[-1])
    )

    # Count how many of the 5 EMAs are in inverted order (bearish)
    daily_emas = [float(ema8.iloc[-1]), float(ema21.iloc[-1]),
                  float(ema34.iloc[-1]), float(ema55.iloc[-1]),
                  float(ema89.iloc[-1])]
    stack_count = sum(1 for i in range(len(daily_emas) - 1)
                      if daily_emas[i] < daily_emas[i + 1])  # max 4 (bearish)

    # ── Higher-TF confirmation (interval-dependent) ──
    weekly_stack = False
    below_sma200 = False
    sma200_val = None

    if interval == "1d":
        # Weekly EMAs (resample daily → weekly)
        wk = close.resample("W-FRI").last().dropna()
        if len(wk) < 35:
            return None
        wk_ema8 = wk.ewm(span=8, adjust=False).mean()
        wk_ema21 = wk.ewm(span=21, adjust=False).mean()
        wk_ema34 = wk.ewm(span=34, adjust=False).mean()
        weekly_stack = (
            float(wk_ema34.iloc[-1]) > float(wk_ema21.iloc[-1])
            > float(wk_ema8.iloc[-1])
        )
        sma200 = close.rolling(window=200).mean()
        sma200_val = float(sma200.iloc[-1])
        below_sma200 = last_close < sma200_val

    elif interval == "1h":
        # Daily EMAs (resample hourly → daily) as higher-TF
        daily = close.resample("D").last().dropna()
        if len(daily) >= 10:
            d_ema8 = daily.ewm(span=8, adjust=False).mean()
            d_ema21 = daily.ewm(span=21, adjust=False).mean()
            d_ema34 = daily.ewm(span=34, adjust=False).mean()
            weekly_stack = (
                float(d_ema34.iloc[-1]) > float(d_ema21.iloc[-1])
                > float(d_ema8.iloc[-1])
            )
        if len(close) >= 200:
            sma200 = close.rolling(window=200).mean()
            sma200_val = float(sma200.iloc[-1])
            below_sma200 = last_close < sma200_val

    else:
        # 5m/15m/30m – no higher-TF resampling
        pass

    # ── Trend Anchor pass ──
    if interval in ("5m", "15m", "30m"):
        # For short intraday: just require strong inverted EMA stacking (≥3 pairs)
        trend_pass = stack_count >= 3
    else:
        # Daily/hourly: at least 2 EMA pairs inverted + higher-TF or below SMA 200
        trend_pass = stack_count >= 2 and (weekly_stack or below_sma200)
    if not trend_pass:
        return None

    # ── Slow Stochastic (14, 3, 3) ──
    low14 = low.rolling(window=14).min()
    high14 = high.rolling(window=14).max()
    raw_k = (close - low14) / (high14 - low14) * 100
    slow_k = raw_k.rolling(window=3).mean()
    slow_d = slow_k.rolling(window=3).mean()
    stoch_k = float(slow_k.iloc[-1])
    stoch_d = float(slow_d.iloc[-1])

    # ── ADX (14-period, Wilder's smoothing) ──
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr14 = tr.ewm(alpha=1/14, adjust=False).mean()

    plus_dm = high.diff().clip(lower=0)
    minus_dm = (-low.diff()).clip(lower=0)
    plus_dm = plus_dm.where(plus_dm > minus_dm, 0.0)
    minus_dm = minus_dm.where(minus_dm > plus_dm, 0.0)

    plus_di = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    adx = dx.ewm(alpha=1/14, adjust=False).mean()
    adx_val = float(adx.iloc[-1])

    # ── ATR (14) and Rubber-Band Rule ──
    atr_val = float(atr14.iloc[-1])
    ema21_val = float(ema21.iloc[-1])
    dist_to_ema21 = abs(last_close - ema21_val)

    # ── Bounce Zone check (bearish mirror) ──
    stoch_bounce = stoch_k > 60
    adx_healthy = 15 <= adx_val <= 55
    rubber_band = dist_to_ema21 <= 2 * atr_val

    bounce_pass = stoch_bounce and adx_healthy and rubber_band
    if not bounce_pass:
        return None

    # ── Relative volume (20-day avg) ──
    avg_vol_20 = df["volume"].rolling(window=20).mean()
    last_vol = float(df["volume"].iloc[-1])
    avg_vol = float(avg_vol_20.iloc[-1])
    rel_volume = round(last_vol / avg_vol, 2) if avg_vol > 0 else 0

    # ── RSI (14-period) ──
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14).mean()
    rs = gain / loss
    rsi_series = 100 - (100 / (1 + rs))
    rsi_val = float(rsi_series.iloc[-1])

    # ── RSI gate: avoid overbought blowoffs and deeply oversold stocks ──
    if not (40 <= rsi_val <= 70):
        return None

    # ── Entry Quality Score (bearish mirror) ──
    # Stochastics height: higher %K = stronger bounce into resistance = better short
    stoch_score = max(0, min(100, (stoch_k - 60) / 40 * 100)) * 0.30

    # EMA 21 proximity: closer = better
    ema21_pct = dist_to_ema21 / ema21_val * 100 if ema21_val > 0 else 100
    proximity_score = max(0, min(100, (1 - ema21_pct / 3) * 100)) * 0.25

    # Stack alignment: 4/4 = perfect bearish alignment
    stack_score = (stack_count / 4) * 100 * 0.20

    # Relative volume
    vol_score = min(100, rel_volume * 50) * 0.15

    # ADX sweet spot (30-40 is ideal)
    adx_diff = abs(adx_val - 35)
    adx_score = max(0, min(100, (1 - adx_diff / 25) * 100)) * 0.10

    total_score = stoch_score + proximity_score + stack_score + vol_score + adx_score
    total_score = round(total_score, 1)

    if total_score >= 90:
        grade = "A+"
    elif total_score >= 80:
        grade = "A"
    elif total_score >= 70:
        grade = "B+"
    elif total_score >= 60:
        grade = "B"
    else:
        grade = "C"

    return {
        "ticker": ticker,
        "last_close": round(last_close, 2),
        "grade": grade,
        "score": total_score,
        "interval": interval,
        # Trend Anchor (bearish)
        "daily_stack": daily_stack,
        "stack_count": stack_count,
        "weekly_stack": weekly_stack,
        "below_sma200": below_sma200,
        "sma200": round(sma200_val, 2) if sma200_val is not None else None,
        # Bounce Zone
        "stoch_k": round(stoch_k, 2),
        "stoch_d": round(stoch_d, 2),
        "adx": round(adx_val, 2),
        "atr": round(atr_val, 2),
        "ema21": round(ema21_val, 2),
        "dist_to_ema21_pct": round(ema21_pct, 2),
        "rubber_band": rubber_band,
        # Volume
        "rel_volume": rel_volume,
        "volume": int(last_vol),
        "avg_volume": int(avg_vol),
        "rsi": round(rsi_val, 2),
        "date": df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1]),
    }


# ---------------------------------------------------------------------------
# Fibonacci Retracement Scanner (Zigzag Pivot Method)
# ---------------------------------------------------------------------------

_FIBONACCI_SWING_BOUNDS = {
    "1d": (3.0, 12.0),
    "1h": (1.5, 8.0),
    "30m": (1.0, 6.0),
    "15m": (0.75, 5.0),
    "5m": (0.5, 3.0),
}


def calculate_fibonacci_swing_pct(df: pd.DataFrame, interval: str = "1d") -> float:
    """Return a robust ZigZag reversal threshold for the ticker and interval."""
    minimum, maximum = _FIBONACCI_SWING_BOUNDS.get(
        interval, _FIBONACCI_SWING_BOUNDS["1d"]
    )
    if df is None or len(df) < 2 or not {"high", "low", "close"}.issubset(df.columns):
        return minimum

    previous_close = pd.to_numeric(df["close"], errors="coerce").shift(1)
    true_ranges = pd.concat([
        (pd.to_numeric(df["high"], errors="coerce")
         - pd.to_numeric(df["low"], errors="coerce")).abs(),
        (pd.to_numeric(df["high"], errors="coerce") - previous_close).abs(),
        (pd.to_numeric(df["low"], errors="coerce") - previous_close).abs(),
    ], axis=1).max(axis=1)
    true_range_pct = (true_ranges / previous_close.replace(0, np.nan) * 100).dropna()
    if true_range_pct.empty:
        return minimum

    typical_range_pct = float(true_range_pct.tail(50).median())
    return round(float(np.clip(typical_range_pct * 2.5, minimum, maximum)), 2)

def _find_zigzag_pivots(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                        min_swing_pct: float = 5.0) -> list:
    """Find alternating swing high/low pivots using zigzag algorithm.

    Returns list of (index, price, type) where type is 'high' or 'low'.
    Pivots must alternate and the swing between them must be >= min_swing_pct.
    """
    if len(closes) < 10:
        return []

    threshold = min_swing_pct / 100.0
    pivots = []

    # Seed with initial extremes
    first_hi_idx = int(np.argmax(highs[:20]))
    first_lo_idx = int(np.argmin(lows[:20]))

    if first_hi_idx < first_lo_idx:
        pivots.append((first_hi_idx, float(highs[first_hi_idx]), 'high'))
    else:
        pivots.append((first_lo_idx, float(lows[first_lo_idx]), 'low'))

    for i in range(pivots[0][0] + 1, len(closes)):
        last_type = pivots[-1][2]
        last_price = pivots[-1][1]
        last_idx = pivots[-1][0]

        if last_type == 'high':
            # Looking for a swing low
            if float(lows[i]) < last_price * (1 - threshold):
                segment_low_idx = last_idx + int(np.argmin(lows[last_idx + 1:i + 1])) + 1
                pivots.append((segment_low_idx, float(lows[segment_low_idx]), 'low'))
            elif float(highs[i]) > last_price:
                pivots[-1] = (i, float(highs[i]), 'high')
        else:
            # Looking for a swing high
            if float(highs[i]) > last_price * (1 + threshold):
                segment_hi_idx = last_idx + int(np.argmax(highs[last_idx + 1:i + 1])) + 1
                pivots.append((segment_hi_idx, float(highs[segment_hi_idx]), 'high'))
            elif float(lows[i]) < last_price:
                pivots[-1] = (i, float(lows[i]), 'low')

    return pivots


def _describe_active_fibonacci_leg(start_pivot, end_pivot, df: pd.DataFrame,
                                   last_close: float, min_swing_pct: float) -> Dict:
    """Describe the current significant ZigZag leg without treating it as confirmed."""
    start_idx, start_price, start_type = start_pivot
    end_idx, end_price, end_type = end_pivot
    start_price = round(float(start_price), 2)
    end_price = round(float(end_price), 2)
    swing_high = max(start_price, end_price)
    swing_low = min(start_price, end_price)
    swing_range = swing_high - swing_low
    trend_direction = (
        "uptrend_retracement" if end_type == "high" else "downtrend_retracement"
    )
    ratios = [0.236, 0.382, 0.500, 0.618, 0.786]
    names = ["23.6%", "38.2%", "50.0%", "61.8%", "78.6%"]
    if trend_direction == "uptrend_retracement":
        prices = [round(swing_high - swing_range * ratio, 2) for ratio in ratios]
        raw_retracement_pct = (swing_high - last_close) / swing_range * 100
        confirmation_price = round(end_price * (1 - min_swing_pct / 100), 2)
        confirmation_condition = "at_or_below"
        level_role = "provisional_support"
    else:
        prices = [round(swing_low + swing_range * ratio, 2) for ratio in ratios]
        raw_retracement_pct = (last_close - swing_low) / swing_range * 100
        confirmation_price = round(end_price * (1 + min_swing_pct / 100), 2)
        confirmation_condition = "at_or_above"
        level_role = "provisional_resistance"

    nearest_idx = min(
        range(len(prices)), key=lambda index: abs(last_close - prices[index])
    )
    nearest_price = prices[nearest_idx]
    extension_names = ["127.2%", "161.8%"]
    extension_ratios = [1.272, 1.618]
    if trend_direction == "uptrend_retracement":
        failure_condition = "at_or_below"
        extensions = [
            round(swing_high - swing_range * ratio, 2)
            for ratio in extension_ratios
        ]
        scenarios = [
            {
                "id": "continuation",
                "title": "Active up-leg continues",
                "condition": "above",
                "trigger_price": round(end_price, 2),
                "detail": "A higher high replaces the developing pivot and recalculates provisional supports.",
                "levels": [],
            },
            {
                "id": "confirmation",
                "title": "Developing high confirms",
                "condition": confirmation_condition,
                "trigger_price": confirmation_price,
                "detail": "The active up-leg becomes confirmed; its retracement supports become stable references.",
                "levels": [
                    {"name": name, "price": price}
                    for name, price in zip(names, prices)
                ],
            },
            {
                "id": "unconfirmed_range",
                "title": "Pullback remains unconfirmed",
                "condition": "between",
                "lower_price": confirmation_price,
                "upper_price": round(end_price, 2),
                "detail": "No new pivot is confirmed while price remains between the reversal boundary and developing high.",
                "levels": [],
            },
            {
                "id": "support_hold",
                "title": "Confirmed pullback finds support",
                "condition": "after_confirmation",
                "detail": "After high confirmation, a developing low may form around one of these supports; a threshold rally would confirm that low.",
                "levels": [
                    {"name": name, "price": price}
                    for name, price in zip(names, prices)
                ],
            },
            {
                "id": "failure",
                "title": "Active bullish leg fails",
                "condition": failure_condition,
                "trigger_price": round(start_price, 2),
                "detail": "A break of the active-leg origin completes a 100% retracement; extensions are references, not predicted targets.",
                "levels": [
                    {"name": name, "price": price}
                    for name, price in zip(extension_names, extensions)
                ],
            },
        ]
    else:
        failure_condition = "at_or_above"
        extensions = [
            round(swing_low + swing_range * ratio, 2)
            for ratio in extension_ratios
        ]
        scenarios = [
            {
                "id": "continuation",
                "title": "Active down-leg continues",
                "condition": "below",
                "trigger_price": round(end_price, 2),
                "detail": "A lower low replaces the developing pivot and recalculates provisional resistances.",
                "levels": [],
            },
            {
                "id": "confirmation",
                "title": "Developing low confirms",
                "condition": confirmation_condition,
                "trigger_price": confirmation_price,
                "detail": "The active down-leg becomes confirmed; its retracement resistances become stable references.",
                "levels": [
                    {"name": name, "price": price}
                    for name, price in zip(names, prices)
                ],
            },
            {
                "id": "unconfirmed_range",
                "title": "Bounce remains unconfirmed",
                "condition": "between",
                "lower_price": round(end_price, 2),
                "upper_price": confirmation_price,
                "detail": "No new pivot is confirmed while price remains between the developing low and reversal boundary.",
                "levels": [],
            },
            {
                "id": "resistance_hold",
                "title": "Confirmed bounce meets resistance",
                "condition": "after_confirmation",
                "detail": "After low confirmation, a developing high may form around one of these resistances; a threshold decline would confirm that high.",
                "levels": [
                    {"name": name, "price": price}
                    for name, price in zip(names, prices)
                ],
            },
            {
                "id": "failure",
                "title": "Active bearish leg fails",
                "condition": failure_condition,
                "trigger_price": round(start_price, 2),
                "detail": "A break of the active-leg origin completes a 100% retracement; extensions are references, not predicted targets.",
                "levels": [
                    {"name": name, "price": price}
                    for name, price in zip(extension_names, extensions)
                ],
            },
        ]

    def pivot_payload(index, price, pivot_type):
        pivot_date = df.index[index]
        return {
            "type": pivot_type,
            "price": round(price, 2),
            "date": pivot_date.strftime("%Y-%m-%d")
                if hasattr(pivot_date, "strftime") else str(pivot_date)[:10],
        }

    return {
        "status": "provisional",
        "level_role": level_role,
        "trend_direction": trend_direction,
        "start": pivot_payload(start_idx, start_price, start_type),
        "end": pivot_payload(end_idx, end_price, end_type),
        "swing_range": round(swing_range, 2),
        "swing_size_pct": round(swing_range / swing_low * 100, 2),
        "retracement_pct": round(max(0, raw_retracement_pct), 2),
        "levels": [
            {"name": name, "price": price}
            for name, price in zip(names, prices)
        ],
        "nearest_level": names[nearest_idx],
        "nearest_level_price": nearest_price,
        "distance_pct": round(
            (last_close - nearest_price) / nearest_price * 100, 2
        ) if nearest_price > 0 else 0.0,
        "confirmation": {
            "condition": confirmation_condition,
            "price": confirmation_price,
            "reversal_pct": round(min_swing_pct, 2),
        },
        "current_state": {
            "id": "unconfirmed_range",
            "detail": scenarios[2]["detail"],
        },
        "scenarios": scenarios,
    }


def _describe_confirmed_fibonacci_leg(start_pivot, end_pivot, df: pd.DataFrame,
                                      last_close: float) -> Dict:
    """Describe a completed leg and whether later price action invalidated it."""
    start_idx, start_price, start_type = start_pivot
    end_idx, end_price, end_type = end_pivot
    start_price = round(float(start_price), 2)
    end_price = round(float(end_price), 2)
    swing_high = max(start_price, end_price)
    swing_low = min(start_price, end_price)
    swing_range = swing_high - swing_low
    names = ["23.6%", "38.2%", "50.0%", "61.8%", "78.6%"]
    ratios = [0.236, 0.382, 0.500, 0.618, 0.786]
    later = df.iloc[end_idx + 1:]

    if end_type == "high":
        trend_direction = "uptrend_retracement"
        level_role = "confirmed_support"
        prices = [round(swing_high - swing_range * ratio, 2) for ratio in ratios]
        invalidation_condition = "below"
        invalidation_price = start_price
        invalidating = later[pd.to_numeric(later["low"], errors="coerce") < start_price]
    else:
        trend_direction = "downtrend_retracement"
        level_role = "confirmed_resistance"
        prices = [round(swing_low + swing_range * ratio, 2) for ratio in ratios]
        invalidation_condition = "above"
        invalidation_price = start_price
        invalidating = later[pd.to_numeric(later["high"], errors="coerce") > start_price]

    invalidated = not invalidating.empty
    nearest_idx = min(
        range(len(prices)), key=lambda index: abs(last_close - prices[index])
    )

    def pivot_payload(index, price, pivot_type):
        pivot_date = df.index[index]
        return {
            "type": pivot_type,
            "price": price,
            "date": pivot_date.strftime("%Y-%m-%d")
                if hasattr(pivot_date, "strftime") else str(pivot_date)[:10],
        }

    invalidated_at = None
    if invalidated:
        invalidated_index = invalidating.index[0]
        invalidated_at = invalidated_index.strftime("%Y-%m-%d") \
            if hasattr(invalidated_index, "strftime") else str(invalidated_index)[:10]

    return {
        "status": "invalidated" if invalidated else "valid",
        "level_role": level_role,
        "trend_direction": trend_direction,
        "start": pivot_payload(start_idx, start_price, start_type),
        "end": pivot_payload(end_idx, end_price, end_type),
        "swing_range": round(swing_range, 2),
        "swing_size_pct": round(swing_range / swing_low * 100, 2),
        "levels": [
            {"name": name, "price": price}
            for name, price in zip(names, prices)
        ],
        "nearest_level": names[nearest_idx],
        "nearest_level_price": prices[nearest_idx],
        "distance_pct": round(
            (last_close - prices[nearest_idx]) / prices[nearest_idx] * 100, 2
        ) if prices[nearest_idx] > 0 else 0.0,
        "invalidation": {
            "condition": invalidation_condition,
            "price": invalidation_price,
            "date": invalidated_at,
        },
    }


def scan_fibonacci(ticker: str, df: pd.DataFrame = None,
                   min_swing_pct: float = 5.0) -> Optional[Dict]:
    """Scan a ticker for Fibonacci retracement levels using zigzag pivots."""
    if df is None:
        df = download_historical_data(ticker, days=1100)
    if df is None or len(df) < 50:
        return None

    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    last_close = float(closes[-1])

    pivots = _find_zigzag_pivots(highs, lows, closes, min_swing_pct)
    if len(pivots) < 3:
        return None

    # The final ZigZag point is still developing. Keep completed legs as
    # history and use the newest one not invalidated by later price action.
    confirmed_pairs = list(zip(pivots[:-2], pivots[1:-1]))
    confirmed_legs = [
        _describe_confirmed_fibonacci_leg(start, end, df, last_close)
        for start, end in confirmed_pairs
    ]
    primary_index = next(
        (index for index in range(len(confirmed_legs) - 1, -1, -1)
         if confirmed_legs[index]["status"] == "valid"),
        len(confirmed_legs) - 1,
    )
    p1, p2 = confirmed_pairs[primary_index]
    developing_pivot = pivots[-1]
    p1_idx, p1_price, p1_type = p1
    p2_idx, p2_price, p2_type = p2
    developing_idx, developing_price, developing_type = developing_pivot
    p1_price = round(float(p1_price), 2)
    p2_price = round(float(p2_price), 2)
    developing_price = round(float(developing_price), 2)

    swing_high = max(p1_price, p2_price)
    swing_low = min(p1_price, p2_price)
    swing_range = swing_high - swing_low

    if swing_range <= 0 or swing_low <= 0:
        return None

    swing_size_pct = round(swing_range / swing_low * 100, 2)

    # Determine trend direction based on which pivot is more recent
    if p2_type == 'high':
        trend_direction = "uptrend_retracement"
    else:
        trend_direction = "downtrend_retracement"

    # Compute Fibonacci levels
    fib_ratios = [0.236, 0.382, 0.500, 0.618, 0.786]
    fib_names = ["23.6%", "38.2%", "50.0%", "61.8%", "78.6%"]

    if trend_direction == "uptrend_retracement":
        fib_levels = [round(swing_high - swing_range * r, 2) for r in fib_ratios]
    else:
        fib_levels = [round(swing_low + swing_range * r, 2) for r in fib_ratios]

    # --- BOTH sets of Fibonacci levels (support & resistance) ---
    # Support levels: retracement from swing high (potential support during decline)
    support_fibs = [{"name": n, "price": round(swing_high - swing_range * r, 2)}
                    for n, r in zip(fib_names, fib_ratios)]
    # Resistance levels: retracement from swing low (potential resistance during rally)
    resistance_fibs = [{"name": n, "price": round(swing_low + swing_range * r, 2)}
                       for n, r in zip(fib_names, fib_ratios)]

    # Find nearest Fibonacci level
    nearest_idx = 0
    nearest_dist = abs(last_close - fib_levels[0])
    for i, level in enumerate(fib_levels):
        d = abs(last_close - level)
        if d < nearest_dist:
            nearest_dist = d
            nearest_idx = i

    nearest_level_name = fib_names[nearest_idx]
    nearest_level_price = fib_levels[nearest_idx]
    distance_pct = round((last_close - nearest_level_price) / nearest_level_price * 100, 2) if nearest_level_price > 0 else 0.0

    # Retracement percentage
    if trend_direction == "uptrend_retracement":
        retracement_pct = round((swing_high - last_close) / swing_range * 100, 2)
    else:
        retracement_pct = round((last_close - swing_low) / swing_range * 100, 2)

    # Determine signal
    proximity_threshold = 1.5
    if abs(distance_pct) <= proximity_threshold:
        signal = f"Near Fib {nearest_level_name}"
    elif trend_direction == "uptrend_retracement" and last_close < fib_levels[-1]:
        signal = "Below All Levels"
    elif trend_direction == "downtrend_retracement" and last_close > fib_levels[-1]:
        signal = "Above All Levels"
    else:
        signal = "Between Levels"

    # --- Build zone ---
    # Zone: use primary direction fib levels + swing boundaries
    all_levels = [("Swing High", round(swing_high, 2))]
    for name, price in zip(fib_names, fib_levels):
        all_levels.append((name, price))
    all_levels.append(("Swing Low", round(swing_low, 2)))
    all_levels.sort(key=lambda x: x[1], reverse=True)

    zone = "Unknown"
    for i in range(len(all_levels) - 1):
        upper_name, upper_price = all_levels[i]
        lower_name, lower_price = all_levels[i + 1]
        if lower_price <= last_close <= upper_price:
            zone = f"{lower_name} \u2013 {upper_name}"
            break
    else:
        if last_close > all_levels[0][1]:
            zone = f"Above {all_levels[0][0]}"
        elif last_close < all_levels[-1][1]:
            zone = f"Below {all_levels[-1][0]}"

    # --- Support & Resistance targets from dual fib sets ---
    # Support targets: support fib levels below current price (nearest first)
    support_targets = [{"level": s["name"], "price": s["price"],
                        "pct": round((last_close - s["price"]) / last_close * 100, 2)}
                       for s in sorted(support_fibs, key=lambda x: x["price"], reverse=True)
                       if s["price"] < last_close][:3]
    # Resistance targets: resistance fib levels above current price (nearest first)
    resistance_targets = [{"level": r["name"], "price": r["price"],
                           "pct": round((r["price"] - last_close) / last_close * 100, 2)}
                          for r in sorted(resistance_fibs, key=lambda x: x["price"])
                          if r["price"] > last_close][:3]

    # Nearest from each set (closest by absolute distance)
    ns = min(support_fibs, key=lambda s: abs(last_close - s["price"]))
    nearest_support = {"name": ns["name"], "price": ns["price"],
                       "distance_pct": round((last_close - ns["price"]) / ns["price"] * 100, 2) if ns["price"] > 0 else 0}
    nr = min(resistance_fibs, key=lambda r: abs(last_close - r["price"]))
    nearest_resistance = {"name": nr["name"], "price": nr["price"],
                          "distance_pct": round((last_close - nr["price"]) / nr["price"] * 100, 2) if nr["price"] > 0 else 0}

    # Fibonacci extensions (both directions)
    ext_ratios = [1.272, 1.618]
    ext_names = ["127.2%", "161.8%"]
    upside_extensions = [{"level": n, "price": round(swing_low + swing_range * r, 2)}
                         for n, r in zip(ext_names, ext_ratios)]
    downside_extensions = [{"level": n, "price": round(swing_high - swing_range * r, 2)}
                           for n, r in zip(ext_names, ext_ratios)]

    # Resolve swing dates
    swing_high_date = df.index[p1_idx if p1_type == 'high' else p2_idx]
    swing_low_date = df.index[p1_idx if p1_type == 'low' else p2_idx]
    developing_date = df.index[developing_idx]
    active_start_price = round(float(pivots[-2][1]), 2)
    developing_move_pct = round(
        abs(developing_price - active_start_price) / active_start_price * 100, 2
    ) if active_start_price > 0 else 0.0
    active_leg = _describe_active_fibonacci_leg(
        pivots[-2], developing_pivot, df, last_close, min_swing_pct
    )
    confirmed_history = []
    for index in range(len(confirmed_legs) - 1, max(-1, len(confirmed_legs) - 7), -1):
        leg = dict(confirmed_legs[index])
        leg["is_primary"] = index == primary_index
        confirmed_history.append(leg)

    return {
        "ticker": ticker,
        "signal": signal,
        "swing_basis": "latest_valid_confirmed_leg",
        "swing_detection_pct": round(min_swing_pct, 2),
        "trend_direction": trend_direction,
        "last_close": round(last_close, 2),
        "swing_high": round(swing_high, 2),
        "swing_low": round(swing_low, 2),
        "swing_high_date": swing_high_date.strftime("%Y-%m-%d") if hasattr(swing_high_date, "strftime") else str(swing_high_date)[:10],
        "swing_low_date": swing_low_date.strftime("%Y-%m-%d") if hasattr(swing_low_date, "strftime") else str(swing_low_date)[:10],
        "developing_pivot": {
            "type": developing_type,
            "price": round(developing_price, 2),
            "date": developing_date.strftime("%Y-%m-%d") if hasattr(developing_date, "strftime") else str(developing_date)[:10],
            "move_pct_from_confirmed": developing_move_pct,
        },
        "active_leg": active_leg,
        "confirmed_legs": confirmed_history,
        "swing_size_pct": swing_size_pct,
        "retracement_pct": round(max(0, min(retracement_pct, 100)), 2),
        "fib_236": fib_levels[0],
        "fib_382": fib_levels[1],
        "fib_500": fib_levels[2],
        "fib_618": fib_levels[3],
        "fib_786": fib_levels[4],
        "nearest_level": nearest_level_name,
        "nearest_level_price": nearest_level_price,
        "distance_pct": distance_pct,
        "zone": zone,
        "support_fibs": support_fibs,
        "resistance_fibs": resistance_fibs,
        "nearest_support": nearest_support,
        "nearest_resistance": nearest_resistance,
        "support_targets": support_targets,
        "resistance_targets": resistance_targets,
        "upside_extensions": upside_extensions,
        "downside_extensions": downside_extensions,
        "date": df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1]),
    }
