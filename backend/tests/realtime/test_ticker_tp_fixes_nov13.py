"""Ticker page fixes TP9, TP10, TP13 (user's go Nov 13 ~03:45 UTC; review G102_TICKER_PAGE_REVIEW_20261112)."""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from equity.technicals import compute_trade_setup_technicals
from equity.ticker_contracts import build_ticker_confluence_zones


def _frame(count, *, freq="MS", start="2021-01-01", seed=7, volume=None, index=None):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0.4, 3.0, count))
    high = close + rng.uniform(0.5, 4.0, count)
    low = close - rng.uniform(0.5, 4.0, count)
    if index is None:
        index = pd.date_range(start, periods=count, freq=freq, tz="UTC")
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close,
                         "volume": volume if volume is not None else np.full(count, 1000.0)}, index=index)


def _wilder_adx_reference(frame):
    high, low, close = (frame[key].to_numpy(dtype=float) for key in ("high", "low", "close"))
    up, down = np.diff(high), -np.diff(low)
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = np.maximum(high[1:] - low[1:], np.maximum(abs(high[1:] - close[:-1]), abs(low[1:] - close[:-1])))

    def smooth(values):  # Wilder: sum of the first 14, then S - S/14 + v
        out = [float(np.sum(values[:14]))]
        for value in values[14:]:
            out.append(out[-1] - out[-1] / 14 + value)
        return np.array(out)

    s_tr, s_plus, s_minus = smooth(tr), smooth(plus_dm), smooth(minus_dm)
    plus_di, minus_di = 100 * s_plus / s_tr, 100 * s_minus / s_tr
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = [float(np.mean(dx[:14]))]
    for value in dx[14:]:
        adx.append((adx[-1] * 13 + value) / 14)
    return round(adx[-1], 1), round(float(plus_di[-1]), 1), round(float(minus_di[-1]), 1)


def test_monthly_adx_and_di_follow_wilder_on_a_short_series():
    frame = _frame(60)
    technicals = compute_trade_setup_technicals(frame, "1mo", input_includes_forming_bar=False)
    assert (technicals.adx, technicals.plus_di, technicals.minus_di) == _wilder_adx_reference(frame)


def test_long_series_keep_their_values():
    frame = _frame(500, freq="30min", start="2026-06-01 13:30")
    technicals = compute_trade_setup_technicals(frame, "30m", input_includes_forming_bar=False)
    assert technicals.adx == _wilder_adx_reference(frame)[0]


def test_hourly_volatility_annualizes_with_seven_bars_per_session():
    frame = _frame(80, freq="h", start="2026-06-01 13:30")
    technicals = compute_trade_setup_technicals(frame, "1h", input_includes_forming_bar=False)
    returns = pd.Series(np.diff(np.log(frame["close"].to_numpy())))
    expected = returns.rolling(20).std(ddof=1).iloc[-1] * math.sqrt(252 * 7) * 100
    assert technicals.historical_volatility_pct == pytest.approx(round(expected, 1))


def _intraday_index(sessions):
    days = pd.bdate_range("2026-10-01", periods=sessions)
    stamps = []
    for day in days:
        opening = pd.Timestamp(day.date()).tz_localize("America/New_York") + pd.Timedelta(hours=9, minutes=30)
        stamps.extend(opening + pd.Timedelta(minutes=30 * slot) for slot in range(13))
    return pd.DatetimeIndex(stamps).tz_convert("UTC")


def test_intraday_relative_volume_compares_the_same_time_of_day_slot():
    index = _intraday_index(25)
    volume = np.array([3000.0 if stamp.tz_convert("America/New_York").strftime("%H:%M") == "15:30" else 1000.0
                       for stamp in index])
    frame = _frame(len(index), index=index, volume=volume)
    technicals = compute_trade_setup_technicals(frame, "30m", input_includes_forming_bar=False)
    # The closing-auction slot is always 3x the others: against its own slot it is normal (1.0), not 2.8x.
    assert technicals.relative_volume == pytest.approx(1.0)
    assert technicals.volume_trend_state == "STABLE"


def test_intraday_without_bar_times_keeps_the_previous_method():
    frame = _frame(60, freq="30min", start="2026-06-01 13:30").reset_index(drop=True)
    technicals = compute_trade_setup_technicals(frame, "30m", input_includes_forming_bar=False)
    assert technicals.relative_volume == pytest.approx(1.0)


def _setup(close, *, atr=2.0, targets=(), stops=(), zones=(), patterns=(), fib=()):
    return {"last_close": close, "technicals": {"atr": atr}, "targets": list(targets), "stops": list(stops),
            "zones": list(zones), "candlestick_patterns": list(patterns),
            "strategy_results": {"fibonacci": {"active_leg": {"levels": []},
                                               "target_levels": [{"name": name, "price": price} for name, price in fib]}}}


def test_atr_projections_are_not_zone_evidence():
    setups = {"1d": _setup(100.0, targets=[{"level": "ATR +2", "price": 104.0, "source": "ATR"},
                                           {"level": "Swing high", "price": 106.0, "source": "Price Action"}])}
    zones = build_ticker_confluence_zones(setups)
    assert [zone["midpoint"] for zone in zones] == [106.0]


def test_same_price_references_of_one_family_collapse():
    setups = {"1d": _setup(100.0, targets=[{"level": "127.2%", "price": 104.0, "source": "Fib Extension"}],
                           fib=[("127.2%", 104.0)])}
    (zone,) = build_ticker_confluence_zones(setups)
    assert len(zone["references"]) == 1 and zone["strength"] == "SINGLE_REFERENCE"


def test_zone_width_is_capped():
    # Price 100, daily ATR 2: cap = max(1.0, 1.0) = 1.0. Four references 0.3 apart chain across 0.9 then split.
    targets = [{"level": f"L{index}", "price": 103.0 + 0.3 * index, "source": "Price Action"} for index in range(6)]
    zones = build_ticker_confluence_zones({"1d": _setup(100.0, targets=targets)})
    assert all(zone["high"] - zone["low"] <= 1.0 + 1e-9 for zone in zones)
    assert len(zones) >= 2


def test_candle_confirmation_only_from_the_zones_own_timeframe():
    pattern = {"name": "Doji", "direction": "NEUTRAL", "bar_time": "2026-11-12T19:30:00", "open": 104.0,
               "high": 104.2, "low": 103.8, "close": 104.0}
    setups = {"1wk": _setup(100.0, targets=[{"level": "Weekly high", "price": 104.0, "source": "Price Action"}]),
              "1h": _setup(100.0, patterns=[pattern])}
    setups["1d"] = _setup(100.0)
    (zone,) = [zone for zone in build_ticker_confluence_zones(setups) if zone["midpoint"] == 104.0]
    assert zone["intervals"] == ["1wk"] and zone["confirmations"] == []
