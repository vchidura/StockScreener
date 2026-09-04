"""Pure technical-analysis primitives shared by setup workers and legacy parity."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class TradeSetupTechnicals:
    close: np.ndarray
    high: np.ndarray
    low: np.ndarray
    volume: np.ndarray
    ema8: np.ndarray
    ema21: np.ndarray
    ema50: np.ndarray
    ema8_value: float
    ema21_value: float
    ema50_value: float
    ema_bullish_stack: bool
    ema_bearish_stack: bool
    ema_alignment: str
    ema_alignment_detail: str
    distance_to_ema8: float
    distance_to_ema21: float
    rsi: float
    rsi_state: str
    atr14: float
    atr_pct: float
    stochastic_k: float
    vwap: float
    price_vs_vwap: str
    ma10: float | None
    ma20: float | None
    ma50: float | None
    ma100: float | None
    ma200: float | None
    macd: float
    macd_signal: float
    macd_histogram: float
    macd_histogram_previous: float
    macd_state: str
    adx: float | None
    plus_di: float | None
    minus_di: float | None
    historical_volatility_pct: float | None
    historical_volatility_percentile: float | None
    historical_volatility_state: str
    relative_volume: float | None
    volume_trend_ratio: float | None
    volume_trend_pct: float | None
    volume_trend_state: str
    volume_slope: float | None
    volume_slope_state: str
    volume_sparkline: tuple[float, ...]
    cmf_20: float | None
    volume_pressure: str
    range_low: float
    range_high: float
    range_position_pct: float
    trend_consistency: float

    def payload(self) -> dict[str, Any]:
        return {
            "rsi": self.rsi,
            "rsi_state": self.rsi_state,
            "stoch_k": self.stochastic_k,
            "atr": round(self.atr14, 2),
            "atr_pct": self.atr_pct,
            "ma10": round(self.ma10, 2) if self.ma10 else None,
            "ma20": round(self.ma20, 2) if self.ma20 else None,
            "ma50": round(self.ma50, 2) if self.ma50 else None,
            "ma100": round(self.ma100, 2) if self.ma100 else None,
            "ma200": round(self.ma200, 2) if self.ma200 else None,
            "ema8": self.ema8_value,
            "ema21": self.ema21_value,
            "ema50": self.ema50_value,
            "vwap": self.vwap,
            "price_vs_vwap": self.price_vs_vwap,
            "dist_to_8ema": self.distance_to_ema8,
            "dist_to_21ema": self.distance_to_ema21,
            "trend_consistency": self.trend_consistency,
            "macd": self.macd,
            "macd_signal": self.macd_signal,
            "macd_histogram": self.macd_histogram,
            "macd_histogram_previous": self.macd_histogram_previous,
            "macd_state": self.macd_state,
            "adx": self.adx,
            "plus_di": self.plus_di,
            "minus_di": self.minus_di,
            "historical_volatility_pct": self.historical_volatility_pct,
            "historical_volatility_percentile": self.historical_volatility_percentile,
            "historical_volatility_state": self.historical_volatility_state,
            "relative_volume": self.relative_volume,
            "volume_trend_ratio": (
                round(self.volume_trend_ratio, 2)
                if self.volume_trend_ratio is not None else None
            ),
            "volume_trend_pct": self.volume_trend_pct,
            "volume_trend_state": self.volume_trend_state,
            "volume_slope": self.volume_slope,
            "volume_slope_state": self.volume_slope_state,
            "volume_sparkline": list(self.volume_sparkline),
            "cmf_20": self.cmf_20,
            "volume_pressure": self.volume_pressure,
            "range_low": round(self.range_low, 2),
            "range_high": round(self.range_high, 2),
            "range_position_pct": self.range_position_pct,
        }


@dataclass(frozen=True, slots=True)
class EmaConfirmation:
    alignment: str | None
    ema8: float | None
    ema21: float | None


def detect_setup_candlesticks(
    frame: pd.DataFrame,
    *,
    input_includes_forming_bar: bool = True,
) -> list[dict[str, Any]]:
    completed_index = len(frame) - 2 if input_includes_forming_bar else len(frame) - 1
    if completed_index < 1:
        return []
    patterns = []
    previous_bar = frame.iloc[completed_index - 1]
    completed_bar = frame.iloc[completed_index]
    candle_open = float(completed_bar["open"])
    candle_high = float(completed_bar["high"])
    candle_low = float(completed_bar["low"])
    candle_close = float(completed_bar["close"])
    previous_open = float(previous_bar["open"])
    previous_close = float(previous_bar["close"])
    candle_range = max(candle_high - candle_low, 1e-9)
    candle_body = abs(candle_close - candle_open)
    upper_wick = candle_high - max(candle_open, candle_close)
    lower_wick = min(candle_open, candle_close) - candle_low

    def record(name: str, direction: str) -> None:
        pattern_time = frame.index[completed_index]
        patterns.append({
            "name": name,
            "direction": direction,
            "bar_time": (
                pattern_time.isoformat()
                if hasattr(pattern_time, "isoformat") else str(pattern_time)
            ),
            "open": round(candle_open, 2),
            "high": round(candle_high, 2),
            "low": round(candle_low, 2),
            "close": round(candle_close, 2),
        })

    if (
        previous_close < previous_open and candle_close > candle_open
        and candle_open <= previous_close and candle_close >= previous_open
    ):
        record("Bullish engulfing", "BULLISH")
    if (
        previous_close > previous_open and candle_close < candle_open
        and candle_open >= previous_close and candle_close <= previous_open
    ):
        record("Bearish engulfing", "BEARISH")
    if (
        lower_wick >= max(candle_body * 2, candle_range * 0.45)
        and upper_wick <= candle_range * 0.2
    ):
        record("Hammer", "BULLISH")
    if (
        upper_wick >= max(candle_body * 2, candle_range * 0.45)
        and lower_wick <= candle_range * 0.2
    ):
        record("Shooting star", "BEARISH")
    if candle_body <= candle_range * 0.1:
        record("Doji", "NEUTRAL")
    if completed_index >= 2:
        first_bar = frame.iloc[completed_index - 2]
        first_open = float(first_bar["open"])
        first_close = float(first_bar["close"])
        first_body = abs(first_close - first_open)
        middle_body = abs(previous_close - previous_open)
        first_midpoint = (first_open + first_close) / 2
        if (
            first_close < first_open and middle_body <= first_body * 0.5
            and candle_close > candle_open and candle_close >= first_midpoint
        ):
            record("Morning star", "BULLISH")
        if (
            first_close > first_open and middle_body <= first_body * 0.5
            and candle_close < candle_open and candle_close <= first_midpoint
        ):
            record("Evening star", "BEARISH")
    return patterns


def detect_golden_cross(technicals: TradeSetupTechnicals) -> dict[str, Any] | None:
    close = technicals.close
    ma50 = technicals.ma50
    ma200 = technicals.ma200
    if ma50 is None or ma200 is None or len(close) < 201:
        return None
    ma50_values = np.array([
        float(np.mean(close[max(0, index - 49):index + 1]))
        for index in range(max(0, len(close) - 20), len(close))
    ])
    ma200_values = np.array([
        float(np.mean(close[max(0, index - 199):index + 1]))
        for index in range(max(0, len(close) - 20), len(close))
    ])
    result = None
    for index in range(1, len(ma50_values)):
        bars_ago = len(ma50_values) - 1 - index
        if (
            ma50_values[index] > ma200_values[index]
            and ma50_values[index - 1] <= ma200_values[index - 1]
        ):
            result = {
                "type": "Golden Cross",
                "bars_ago": bars_ago,
                "detail": f"50 SMA crossed above 200 SMA {bars_ago} bars ago — bullish",
            }
        elif (
            ma50_values[index] < ma200_values[index]
            and ma50_values[index - 1] >= ma200_values[index - 1]
        ):
            result = {
                "type": "Death Cross",
                "bars_ago": bars_ago,
                "detail": f"50 SMA crossed below 200 SMA {bars_ago} bars ago — bearish",
            }
    if result is not None:
        return result
    if ma50 > ma200:
        return {
            "type": "Above (Bullish)",
            "bars_ago": None,
            "detail": "50 SMA above 200 SMA — bullish structure, no recent cross",
        }
    return {
        "type": "Below (Bearish)",
        "bars_ago": None,
        "detail": "50 SMA below 200 SMA — bearish structure, no recent cross",
    }


def compute_ema_confirmation(frame: pd.DataFrame | None) -> EmaConfirmation:
    if frame is None or len(frame) < 21:
        return EmaConfirmation(None, None, None)
    close = frame["close"].values.astype(float)
    ema8 = exponential_moving_average(close, 8)
    ema21 = exponential_moving_average(close, 21)
    alignment = (
        "Bullish" if ema8[-1] > ema21[-1]
        else "Bearish" if ema8[-1] < ema21[-1]
        else "Neutral"
    )
    return EmaConfirmation(
        alignment=alignment,
        ema8=round(float(ema8[-1]), 2),
        ema21=round(float(ema21[-1]), 2),
    )


def assess_momentum(technicals: TradeSetupTechnicals) -> dict[str, str]:
    last_close = float(technicals.close[-1])
    ma50 = technicals.ma50
    ma200 = technicals.ma200
    if ma50 and ma200:
        if last_close > ma50 and ma50 > ma200:
            state = "Strong Uptrend"
            detail = "Price > 50MA > 200MA — full bullish alignment"
        elif last_close > ma50 and ma50 < ma200:
            state = "Recovery"
            detail = (
                "Price above 50MA but 50MA still below 200MA — early recovery or bear rally"
            )
        elif last_close < ma50 and ma50 > ma200:
            state = "Weakening"
            detail = (
                "Price fell below 50MA but MAs still bullish — pullback or trend break starting"
            )
        elif last_close < ma50 and ma50 < ma200:
            state = "Strong Downtrend"
            detail = "Price < 50MA < 200MA — full bearish alignment"
        else:
            state = "Sideways"
            detail = "MAs tangled — no clear directional trend"
    elif ma50:
        state = "Uptrend" if last_close > ma50 else "Downtrend"
        detail = (
            f"Price {'above' if last_close > ma50 else 'below'} 50MA "
            "(insufficient data for 200MA)"
        )
    else:
        state = "Insufficient Data"
        detail = "Not enough history to assess trend structure"
    return {"state": state, "detail": detail}


def exponential_moving_average(values: np.ndarray, span: int) -> np.ndarray:
    series = np.asarray(values, dtype=float)
    if series.ndim != 1 or len(series) == 0:
        raise ValueError("values must be a nonempty one-dimensional array")
    if span <= 0:
        raise ValueError("span must be positive")
    alpha = 2.0 / (span + 1)
    result = np.empty_like(series, dtype=float)
    result[0] = series[0]
    for index in range(1, len(series)):
        result[index] = alpha * series[index] + (1 - alpha) * result[index - 1]
    return result


def compute_trade_setup_technicals(
    frame: pd.DataFrame,
    interval: str,
    *,
    input_includes_forming_bar: bool = True,
) -> TradeSetupTechnicals:
    if frame is None or len(frame) < 2:
        raise ValueError("at least two price bars are required")
    missing = {"close", "high", "low"} - set(frame.columns)
    if missing:
        raise ValueError(f"missing price columns: {sorted(missing)}")
    close = frame["close"].values.astype(float)
    high = frame["high"].values.astype(float)
    low = frame["low"].values.astype(float)
    volume = (
        frame["volume"].values.astype(float)
        if "volume" in frame.columns else np.zeros(len(close))
    )
    last_close = float(close[-1])

    ema8 = exponential_moving_average(close, 8)
    ema21 = exponential_moving_average(close, 21)
    ema50 = exponential_moving_average(close, 50)
    ema12 = exponential_moving_average(close, 12)
    ema26 = exponential_moving_average(close, 26)
    macd_values = ema12 - ema26
    macd_signal_values = exponential_moving_average(macd_values, 9)
    macd_histogram_values = macd_values - macd_signal_values
    ema8_value = round(float(ema8[-1]), 2)
    ema21_value = round(float(ema21[-1]), 2)
    ema50_value = round(float(ema50[-1]), 2)
    macd = round(float(macd_values[-1]), 4)
    macd_signal = round(float(macd_signal_values[-1]), 4)
    macd_histogram = round(float(macd_histogram_values[-1]), 4)
    macd_histogram_previous = round(float(macd_histogram_values[-2]), 4)
    if macd >= macd_signal:
        macd_state = (
            "BULLISH_RISING"
            if macd_histogram >= macd_histogram_previous else "BULLISH_FADING"
        )
    else:
        macd_state = (
            "BEARISH_FALLING"
            if macd_histogram <= macd_histogram_previous else "BEARISH_IMPROVING"
        )

    ema_bullish_stack = bool(ema8[-1] > ema21[-1] > ema50[-1])
    ema_bearish_stack = bool(ema8[-1] < ema21[-1] < ema50[-1])
    if ema_bullish_stack:
        ema_alignment = "Bullish Stack"
        ema_alignment_detail = (
            "8 EMA > 21 EMA > 50 EMA — short-term momentum aligned with trend"
        )
    elif ema_bearish_stack:
        ema_alignment = "Bearish Stack"
        ema_alignment_detail = (
            "8 EMA < 21 EMA < 50 EMA — bearish momentum aligned with downtrend"
        )
    elif ema8[-1] > ema21[-1]:
        ema_alignment = "Short-term Bullish"
        ema_alignment_detail = (
            "8 EMA > 21 EMA but not fully stacked — short-term momentum up, trend uncertain"
        )
    elif ema8[-1] < ema21[-1]:
        ema_alignment = "Short-term Bearish"
        ema_alignment_detail = (
            "8 EMA < 21 EMA but not fully stacked — short-term weakness, trend uncertain"
        )
    else:
        ema_alignment = "Neutral"
        ema_alignment_detail = "EMAs converging — no clear alignment"

    distance_to_ema8 = round((last_close - ema8[-1]) / ema8[-1] * 100, 2)
    distance_to_ema21 = round((last_close - ema21[-1]) / ema21[-1] * 100, 2)
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    average_gain = np.mean(gain[-14:])
    average_loss = np.mean(loss[-14:])
    rsi = (
        round(100 - 100 / (1 + average_gain / average_loss), 1)
        if average_loss > 0 else 100.0
    )
    if rsi < 30:
        rsi_state = "Oversold"
    elif rsi > 70:
        rsi_state = "Overbought"
    elif rsi < 40:
        rsi_state = "Weak"
    elif rsi > 60:
        rsi_state = "Strong"
    else:
        rsi_state = "Neutral"

    true_range = np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])),
    )
    atr14 = (
        float(np.mean(true_range[-14:]))
        if len(true_range) >= 14 else float(np.mean(true_range))
    )
    atr_pct = round(atr14 / last_close * 100, 2)

    if len(close) >= 17:
        highs_14 = np.array([
            np.max(high[index - 14:index]) for index in range(14, len(high) + 1)
        ])
        lows_14 = np.array([
            np.min(low[index - 14:index]) for index in range(14, len(low) + 1)
        ])
        raw_k = np.where(
            highs_14 - lows_14 > 0,
            (close[13:] - lows_14) / (highs_14 - lows_14) * 100,
            50,
        )
        stochastic_k = (
            round(float(np.mean(raw_k[-3:])), 1)
            if len(raw_k) >= 3 else round(float(raw_k[-1]), 1)
        )
    else:
        stochastic_k = 50.0

    vwap_window = min(20, len(close))
    if np.sum(volume[-vwap_window:]) > 0:
        vwap = round(float(
            np.sum(close[-vwap_window:] * volume[-vwap_window:])
            / np.sum(volume[-vwap_window:])
        ), 2)
    else:
        vwap = round(float(np.mean(close[-vwap_window:])), 2)
    price_vs_vwap = "Above" if last_close > vwap else "Below"
    ma10 = float(np.mean(close[-10:])) if len(close) >= 10 else None
    ma20 = float(np.mean(close[-20:])) if len(close) >= 20 else None
    ma50 = float(np.mean(close[-50:])) if len(close) >= 50 else None
    ma100 = float(np.mean(close[-100:])) if len(close) >= 100 else None
    ma200 = float(np.mean(close[-200:])) if len(close) >= 200 else None

    completed_slice = slice(None, -1) if input_includes_forming_bar else slice(None)
    completed_volume = volume[completed_slice]
    completed_high = high[completed_slice]
    completed_low = low[completed_slice]
    completed_close = close[completed_slice]
    plus_di_value = minus_di_value = adx_value = None
    if len(completed_close) >= 15:
        up_move = np.diff(completed_high)
        down_move = -np.diff(completed_low)
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        directional_tr = np.maximum(
            completed_high[1:] - completed_low[1:],
            np.maximum(
                np.abs(completed_high[1:] - completed_close[:-1]),
                np.abs(completed_low[1:] - completed_close[:-1]),
            ),
        )
        smoothed_tr = pd.Series(directional_tr).ewm(
            alpha=1 / 14, adjust=False
        ).mean().to_numpy()
        smoothed_plus_dm = pd.Series(plus_dm).ewm(
            alpha=1 / 14, adjust=False
        ).mean().to_numpy()
        smoothed_minus_dm = pd.Series(minus_dm).ewm(
            alpha=1 / 14, adjust=False
        ).mean().to_numpy()
        plus_di = np.divide(
            100 * smoothed_plus_dm, smoothed_tr,
            out=np.zeros_like(smoothed_plus_dm), where=smoothed_tr > 0,
        )
        minus_di = np.divide(
            100 * smoothed_minus_dm, smoothed_tr,
            out=np.zeros_like(smoothed_minus_dm), where=smoothed_tr > 0,
        )
        directional_sum = plus_di + minus_di
        dx = np.divide(
            100 * np.abs(plus_di - minus_di), directional_sum,
            out=np.zeros_like(directional_sum), where=directional_sum > 0,
        )
        adx = pd.Series(dx).ewm(alpha=1 / 14, adjust=False).mean().to_numpy()
        plus_di_value = round(float(plus_di[-1]), 1)
        minus_di_value = round(float(minus_di[-1]), 1)
        adx_value = round(float(adx[-1]), 1)

    historical_volatility_pct = None
    historical_volatility_percentile = None
    historical_volatility_state = "UNAVAILABLE"
    if len(completed_close) >= 21 and np.all(completed_close > 0):
        periods_per_year = {
            "1mo": 12, "1wk": 52, "1d": 252, "1h": 252 * 6.5,
            "30m": 252 * 13, "15m": 252 * 26, "5m": 252 * 78,
        }.get(interval, 252)
        log_returns = pd.Series(np.diff(np.log(completed_close)))
        rolling_volatility = (
            log_returns.rolling(20).std(ddof=1)
            * math.sqrt(periods_per_year) * 100
        ).dropna()
        if not rolling_volatility.empty:
            volatility_history = rolling_volatility.tail(252)
            current_volatility = float(volatility_history.iloc[-1])
            volatility_percentile = float(
                (volatility_history <= current_volatility).mean() * 100
            )
            historical_volatility_pct = round(current_volatility, 1)
            historical_volatility_percentile = round(volatility_percentile, 0)
            if volatility_percentile >= 75:
                historical_volatility_state = "ELEVATED"
            elif volatility_percentile <= 25:
                historical_volatility_state = "QUIET"
            else:
                historical_volatility_state = "NORMAL"

    volume_20 = completed_volume[-20:]
    volume_5 = completed_volume[-5:]
    completed_volume_baseline = (
        float(np.mean(volume_20)) if len(volume_20) > 0 else 0.0
    )
    prior_completed_baseline = (
        float(np.mean(completed_volume[-21:-1]))
        if len(completed_volume) >= 21
        else float(np.mean(completed_volume[:-1]))
        if len(completed_volume) > 1 else 0.0
    )
    relative_volume = (
        round(float(completed_volume[-1]) / prior_completed_baseline, 2)
        if len(completed_volume) > 0 and prior_completed_baseline > 0 else None
    )
    volume_trend_ratio = (
        float(np.mean(volume_5)) / completed_volume_baseline
        if len(volume_5) > 0 and completed_volume_baseline > 0 else None
    )
    volume_trend_pct = (
        round((volume_trend_ratio - 1) * 100, 1)
        if volume_trend_ratio is not None else None
    )
    if volume_trend_pct is None:
        volume_trend_state = "UNAVAILABLE"
    elif volume_trend_pct >= 10:
        volume_trend_state = "EXPANDING"
    elif volume_trend_pct <= -10:
        volume_trend_state = "CONTRACTING"
    else:
        volume_trend_state = "STABLE"
    sparkline_volume = completed_volume[-8:]
    volume_sparkline = tuple(
        round(float(value) / completed_volume_baseline, 2)
        for value in sparkline_volume
    ) if completed_volume_baseline > 0 else ()
    if len(volume_sparkline) >= 2:
        x_values = np.arange(len(volume_sparkline), dtype=float)
        volume_slope = round(float(np.polyfit(x_values, volume_sparkline, 1)[0]), 3)
    else:
        volume_slope = None
    if volume_slope is None:
        volume_slope_state = "UNAVAILABLE"
    elif volume_slope >= 0.02:
        volume_slope_state = "RISING"
    elif volume_slope <= -0.02:
        volume_slope_state = "FALLING"
    else:
        volume_slope_state = "FLAT"

    cmf_window = min(20, len(completed_volume))
    if cmf_window > 0 and float(np.sum(completed_volume[-cmf_window:])) > 0:
        window_high = completed_high[-cmf_window:]
        window_low = completed_low[-cmf_window:]
        window_close = completed_close[-cmf_window:]
        window_volume = completed_volume[-cmf_window:]
        window_range = window_high - window_low
        multiplier = np.divide(
            ((window_close - window_low) - (window_high - window_close)),
            window_range,
            out=np.zeros_like(window_close, dtype=float),
            where=window_range > 0,
        )
        cmf_20 = round(float(
            np.sum(multiplier * window_volume) / np.sum(window_volume)
        ), 3)
    else:
        cmf_20 = None
    if cmf_20 is None:
        volume_pressure = "UNAVAILABLE"
    elif cmf_20 > 0.1:
        volume_pressure = "ACCUMULATION"
    elif cmf_20 < -0.1:
        volume_pressure = "DISTRIBUTION"
    else:
        volume_pressure = "BALANCED"
    range_window = min(252, len(close))
    range_low = float(np.min(low[-range_window:]))
    range_high = float(np.max(high[-range_window:]))
    range_position_pct = (
        round((last_close - range_low) / (range_high - range_low) * 100, 1)
        if range_high > range_low else 50.0
    )
    up_days = (
        sum(1 for index in range(-14, 0) if close[index] > close[index - 1])
        if len(close) >= 15 else 0
    )
    trend_consistency = round(max(up_days, 14 - up_days) / 14 * 100, 0)

    return TradeSetupTechnicals(
        close=close, high=high, low=low, volume=volume,
        ema8=ema8, ema21=ema21, ema50=ema50,
        ema8_value=ema8_value, ema21_value=ema21_value, ema50_value=ema50_value,
        ema_bullish_stack=ema_bullish_stack,
        ema_bearish_stack=ema_bearish_stack,
        ema_alignment=ema_alignment, ema_alignment_detail=ema_alignment_detail,
        distance_to_ema8=distance_to_ema8, distance_to_ema21=distance_to_ema21,
        rsi=rsi, rsi_state=rsi_state, atr14=atr14, atr_pct=atr_pct,
        stochastic_k=stochastic_k, vwap=vwap, price_vs_vwap=price_vs_vwap,
        ma10=ma10, ma20=ma20, ma50=ma50, ma100=ma100, ma200=ma200,
        macd=macd, macd_signal=macd_signal,
        macd_histogram=macd_histogram,
        macd_histogram_previous=macd_histogram_previous,
        macd_state=macd_state, adx=adx_value, plus_di=plus_di_value,
        minus_di=minus_di_value,
        historical_volatility_pct=historical_volatility_pct,
        historical_volatility_percentile=historical_volatility_percentile,
        historical_volatility_state=historical_volatility_state,
        relative_volume=relative_volume, volume_trend_ratio=volume_trend_ratio,
        volume_trend_pct=volume_trend_pct, volume_trend_state=volume_trend_state,
        volume_slope=volume_slope, volume_slope_state=volume_slope_state,
        volume_sparkline=volume_sparkline, cmf_20=cmf_20,
        volume_pressure=volume_pressure, range_low=range_low,
        range_high=range_high, range_position_pct=range_position_pct,
        trend_consistency=trend_consistency,
    )


def detect_level_retests(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    levels: Sequence[Mapping[str, Any]],
    *,
    lookback: int = 5,
    tolerance_pct: float = 0.5,
) -> list[dict[str, Any]]:
    close_values = np.asarray(close, dtype=float)
    high_values = np.asarray(high, dtype=float)
    low_values = np.asarray(low, dtype=float)
    if not (len(close_values) == len(high_values) == len(low_values)):
        raise ValueError("close, high, and low arrays must have equal length")
    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if tolerance_pct < 0:
        raise ValueError("tolerance_pct cannot be negative")

    retests = []
    count = len(close_values)
    lookback = min(lookback, count)
    seen_levels = set()
    for index in range(count - lookback, count):
        candle_high = float(high_values[index])
        candle_low = float(low_values[index])
        candle_close = float(close_values[index])
        for level in levels:
            level_price = float(level["price"])
            if level_price <= 0:
                continue
            tolerance = level_price * tolerance_pct / 100
            level_key = f'{level["name"]}_{level_price:.2f}'
            if level_key in seen_levels:
                continue
            touched = False
            touch_type = ""
            if abs(candle_low - level_price) <= tolerance:
                touched = True
                touch_type = "Low touched"
            elif abs(candle_high - level_price) <= tolerance:
                touched = True
                touch_type = "High touched"
            elif candle_low <= level_price <= candle_high:
                touched = True
                touch_type = "Pierced"
            if touched:
                bounce_pct = round(
                    (candle_close - level_price) / level_price * 100, 2
                )
                seen_levels.add(level_key)
                retests.append({
                    "level_name": level["name"],
                    "level_price": round(level_price, 2),
                    "source": level["source"],
                    "candle_high": round(candle_high, 2),
                    "candle_low": round(candle_low, 2),
                    "candle_close": round(candle_close, 2),
                    "touch_type": touch_type,
                    "held": abs(bounce_pct) >= 0.1,
                    "bounce_pct": bounce_pct,
                    "bars_ago": count - 1 - index,
                })
    return retests
