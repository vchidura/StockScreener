"""Point-in-time comparison of Fibonacci signal-scoring variants."""
from __future__ import annotations

from collections.abc import Iterable
import logging

import numpy as np
import pandas as pd

from screeners import (
    _find_zigzag_pivots,
)


FIBONACCI_RATIOS = (0.236, 0.382, 0.500, 0.618, 0.786)
FIBONACCI_NAMES = ("23.6%", "38.2%", "50.0%", "61.8%", "78.6%")
VARIANTS = ("legacy_latest_completed", "latest_valid_primary", "multi_leg_capped")
DEFAULT_HORIZONS = (5, 10, 21)
ROUND_TRIP_COST_BPS = 4.0
logger = logging.getLogger(__name__)


def _daily_swing_thresholds(frame: pd.DataFrame) -> pd.Series:
    """Match the production 2.5x rolling median true-range percentage rule."""
    previous_close = pd.to_numeric(frame["close"], errors="coerce").shift(1)
    true_range = pd.concat([
        pd.to_numeric(frame["high"], errors="coerce")
        - pd.to_numeric(frame["low"], errors="coerce"),
        (pd.to_numeric(frame["high"], errors="coerce") - previous_close).abs(),
        (pd.to_numeric(frame["low"], errors="coerce") - previous_close).abs(),
    ], axis=1).max(axis=1)
    true_range_pct = true_range / previous_close.replace(0, np.nan) * 100
    return (true_range_pct.rolling(50, min_periods=1).median() * 2.5).clip(
        lower=3.0, upper=12.0
    ).round(2).fillna(3.0)


def _leg_validity(
    confirmed_pairs: list[tuple], highs: np.ndarray, lows: np.ndarray,
) -> list[dict]:
    """Return path-dependent validity without building UI-oriented leg details."""
    legs = []
    for start, end in confirmed_pairs:
        start_price = round(float(start[1]), 2)
        later_highs = highs[int(end[0]) + 1:]
        later_lows = lows[int(end[0]) + 1:]
        if end[2] == "high":
            invalidated = bool(len(later_lows) and np.nanmin(later_lows) < start_price)
        else:
            invalidated = bool(len(later_highs) and np.nanmax(later_highs) > start_price)
        legs.append({"status": "invalidated" if invalidated else "valid"})
    return legs


def _near_leg(pair: tuple, last_close: float, proximity_pct: float) -> dict | None:
    """Return one directional candidate when price is near the pair's closest level."""
    start, end = pair
    start_price = round(float(start[1]), 2)
    end_price = round(float(end[1]), 2)
    swing_high = max(start_price, end_price)
    swing_low = min(start_price, end_price)
    swing_range = swing_high - swing_low
    if swing_range <= 0 or swing_low <= 0:
        return None

    if end[2] == "high":
        direction = 1
        levels = [swing_high - swing_range * ratio for ratio in FIBONACCI_RATIOS]
    else:
        direction = -1
        levels = [swing_low + swing_range * ratio for ratio in FIBONACCI_RATIOS]
    nearest_index = min(
        range(len(levels)), key=lambda index: abs(last_close - levels[index])
    )
    level_price = round(float(levels[nearest_index]), 2)
    distance_pct = (last_close - level_price) / level_price * 100
    if abs(distance_pct) > proximity_pct:
        return None
    return {
        "direction": direction,
        "level": FIBONACCI_NAMES[nearest_index],
        "level_price": level_price,
        "distance_pct": round(distance_pct, 4),
    }


def select_variant_candidates(
    confirmed_pairs: list[tuple],
    confirmed_legs: list[dict],
    last_close: float,
    proximity_pct: float = 1.5,
) -> dict[str, dict]:
    """Select legacy, latest-valid, and conflict-capped multi-leg candidates."""
    if not confirmed_pairs or len(confirmed_pairs) != len(confirmed_legs):
        return {}

    candidates: dict[str, dict] = {}
    legacy = _near_leg(confirmed_pairs[-1], last_close, proximity_pct)
    if legacy:
        candidates["legacy_latest_completed"] = {**legacy, "leg_count": 1}

    valid_indices = [
        index for index, leg in enumerate(confirmed_legs)
        if leg.get("status") == "valid"
    ]
    if not valid_indices:
        return candidates

    primary_index = valid_indices[-1]
    primary = _near_leg(confirmed_pairs[primary_index], last_close, proximity_pct)
    if primary:
        candidates["latest_valid_primary"] = {**primary, "leg_count": 1}

    valid_near = []
    for index in valid_indices:
        candidate = _near_leg(confirmed_pairs[index], last_close, proximity_pct)
        if candidate:
            valid_near.append(candidate)
    directions = {candidate["direction"] for candidate in valid_near}
    if len(directions) == 1:
        direction = directions.pop()
        aligned = [
            candidate for candidate in valid_near
            if candidate["direction"] == direction
        ]
        if primary is not None or len(aligned) >= 2:
            nearest = min(aligned, key=lambda item: abs(item["distance_pct"]))
            candidates["multi_leg_capped"] = {
                **nearest,
                "leg_count": len(aligned),
            }
    return candidates


def build_ticker_variant_signals(
    frame: pd.DataFrame,
    ticker: str,
    evaluation_start: str | pd.Timestamp | None = None,
    min_history: int = 50,
    lookback_bars: int = 1600,
    proximity_pct: float = 1.5,
) -> pd.DataFrame:
    """Replay all variants using only bars available through each signal date."""
    ordered = frame.sort_values("date").reset_index(drop=True).copy()
    ordered["date"] = pd.to_datetime(ordered["date"])
    columns = [
        "variant", "ticker", "date", "position", "direction", "level",
        "level_price", "distance_pct", "leg_count", "swing_detection_pct",
    ]
    if len(ordered) < min_history:
        return pd.DataFrame(columns=columns)

    highs = ordered["high"].to_numpy(dtype=float)
    lows = ordered["low"].to_numpy(dtype=float)
    closes = ordered["close"].to_numpy(dtype=float)
    thresholds = _daily_swing_thresholds(ordered).to_numpy(dtype=float)

    start_position = min_history - 1
    if evaluation_start is not None:
        requested = pd.Timestamp(evaluation_start)
        positions = np.flatnonzero(ordered["date"].to_numpy() >= requested.to_datetime64())
        if len(positions) == 0:
            return pd.DataFrame(columns=columns)
        start_position = max(start_position, int(positions[0]))

    rows = []
    for position in range(start_position, len(ordered) - 1):
        history_start = max(0, position - lookback_bars + 1)
        history_highs = highs[history_start:position + 1]
        history_lows = lows[history_start:position + 1]
        history_closes = closes[history_start:position + 1]
        threshold = float(thresholds[position])
        pivots = _find_zigzag_pivots(
            history_highs,
            history_lows,
            history_closes,
            threshold,
        )
        if len(pivots) < 3:
            continue
        confirmed_pairs = list(zip(pivots[:-2], pivots[1:-1]))
        last_close = float(history_closes[-1])
        confirmed_legs = _leg_validity(
            confirmed_pairs, history_highs, history_lows
        )
        selected = select_variant_candidates(
            confirmed_pairs, confirmed_legs, last_close, proximity_pct
        )
        for variant, candidate in selected.items():
            rows.append({
                "variant": variant,
                "ticker": ticker,
                "date": ordered.iloc[position]["date"],
                "position": position,
                **candidate,
                "swing_detection_pct": threshold,
            })
    return pd.DataFrame(rows, columns=columns)


def build_variant_signals(
    panel: pd.DataFrame,
    evaluation_start: str | pd.Timestamp | None = None,
    min_history: int = 50,
    lookback_bars: int = 1600,
    proximity_pct: float = 1.5,
) -> pd.DataFrame:
    """Build point-in-time variant signals for a long-format daily panel."""
    batches = []
    grouped = panel.groupby("ticker", sort=True)
    ticker_count = panel["ticker"].nunique()
    for ticker_index, (ticker, frame) in enumerate(grouped, start=1):
        signals = build_ticker_variant_signals(
            frame,
            str(ticker),
            evaluation_start=evaluation_start,
            min_history=min_history,
            lookback_bars=lookback_bars,
            proximity_pct=proximity_pct,
        )
        if not signals.empty:
            batches.append(signals)
        if ticker_index % 25 == 0 or ticker_index == ticker_count:
            logger.info(
                "Fibonacci replay progress | tickers=%s/%s | signals=%s",
                ticker_index, ticker_count,
                sum(len(batch) for batch in batches),
            )
    if not batches:
        return pd.DataFrame(columns=[
            "variant", "ticker", "date", "position", "direction", "level",
            "level_price", "distance_pct", "leg_count", "swing_detection_pct",
        ])
    return pd.concat(batches, ignore_index=True)


def evaluate_variant_outcomes(
    panel: pd.DataFrame,
    signals: pd.DataFrame,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    round_trip_cost_bps: float = ROUND_TRIP_COST_BPS,
) -> pd.DataFrame:
    """Measure signals from the next session open through each horizon close."""
    columns = [
        "variant", "ticker", "date", "direction", "horizon_bars",
        "entry_price", "exit_price", "net_return", "net_alpha", "mae_pct",
        "mfe_pct", "leg_count",
    ]
    if signals.empty:
        return pd.DataFrame(columns=columns)

    ordered = panel.sort_values(["ticker", "date"]).copy()
    ordered["date"] = pd.to_datetime(ordered["date"])
    ticker_frames = {
        str(ticker): frame.reset_index(drop=True)
        for ticker, frame in ordered.groupby("ticker", sort=False)
    }
    horizons = tuple(sorted({int(value) for value in horizons if int(value) > 0}))
    benchmark: dict[tuple[pd.Timestamp, int], float] = {}
    for horizon in horizons:
        work = ordered[["ticker", "date", "open", "close"]].copy()
        grouped = work.groupby("ticker", sort=False)
        work["entry_open"] = grouped["open"].shift(-1)
        work["exit_close"] = grouped["close"].shift(-horizon)
        work["return"] = work["exit_close"] / work["entry_open"] - 1
        values = work.groupby("date")["return"].mean()
        benchmark.update({(pd.Timestamp(date), horizon): float(value)
                          for date, value in values.dropna().items()})

    cost = float(round_trip_cost_bps) / 10_000.0
    rows = []
    for signal in signals.itertuples(index=False):
        frame = ticker_frames.get(str(signal.ticker))
        if frame is None:
            continue
        position = int(signal.position)
        for horizon in horizons:
            if position + horizon >= len(frame):
                continue
            bars = frame.iloc[position + 1:position + horizon + 1]
            entry = float(bars.iloc[0]["open"])
            exit_price = float(bars.iloc[-1]["close"])
            if not np.isfinite(entry) or entry <= 0 or not np.isfinite(exit_price):
                continue
            raw_return = exit_price / entry - 1
            direction = int(signal.direction)
            signed_return = direction * raw_return
            market_return = benchmark.get((pd.Timestamp(signal.date), horizon))
            if market_return is None or not np.isfinite(market_return):
                continue
            signed_benchmark = direction * market_return
            favorable = (
                float(bars["high"].max()) / entry - 1
                if direction == 1 else
                1 - float(bars["low"].min()) / entry
            )
            adverse = (
                float(bars["low"].min()) / entry - 1
                if direction == 1 else
                1 - float(bars["high"].max()) / entry
            )
            rows.append({
                "variant": signal.variant,
                "ticker": signal.ticker,
                "date": pd.Timestamp(signal.date),
                "direction": direction,
                "horizon_bars": horizon,
                "entry_price": entry,
                "exit_price": exit_price,
                "net_return": signed_return - cost,
                "net_alpha": signed_return - signed_benchmark - cost,
                "mae_pct": adverse,
                "mfe_pct": favorable,
                "leg_count": int(signal.leg_count),
            })
    return pd.DataFrame(rows, columns=columns)


def summarize_variant_outcomes(
    outcomes: pd.DataFrame,
    calendar_dates: Iterable,
) -> pd.DataFrame:
    """Apply the scanner framework's horizon-spaced portfolio qualification."""
    columns = [
        "variant", "direction", "horizon_bars", "events",
        "independent_periods", "mean_net_return", "mean_net_alpha",
        "alpha_t_stat", "early_alpha", "late_alpha", "hit_rate",
        "mean_mae_pct", "mean_mfe_pct", "qualification_status",
    ]
    if outcomes.empty:
        return pd.DataFrame(columns=columns)

    portfolios = outcomes.groupby(
        ["variant", "direction", "horizon_bars", "date"], as_index=False
    ).agg(
        net_return=("net_return", "mean"),
        net_alpha=("net_alpha", "mean"),
        mae_pct=("mae_pct", "mean"),
        mfe_pct=("mfe_pct", "mean"),
        names=("ticker", "count"),
    )
    calendar = {
        pd.Timestamp(value): index
        for index, value in enumerate(sorted(pd.to_datetime(list(calendar_dates))))
    }
    rows = []
    for key, group in portfolios.groupby(
        ["variant", "direction", "horizon_bars"], sort=True
    ):
        horizon = int(key[2])
        selected = []
        last_ordinal = -horizon
        for row in group.sort_values("date").itertuples(index=False):
            ordinal = calendar.get(pd.Timestamp(row.date))
            if ordinal is not None and ordinal - last_ordinal >= horizon:
                selected.append(row)
                last_ordinal = ordinal
        sample = pd.DataFrame(selected)
        periods = len(sample)
        alpha = pd.to_numeric(sample.get("net_alpha"), errors="coerce").dropna()
        alpha_std = float(alpha.std(ddof=1)) if len(alpha) > 1 else np.nan
        alpha_t = (
            float(alpha.mean() / (alpha_std / np.sqrt(len(alpha))))
            if len(alpha) > 1 and np.isfinite(alpha_std) and alpha_std > 0 else None
        )
        midpoint = max(1, periods // 2)
        mean_alpha = float(alpha.mean()) if len(alpha) else None
        early = float(sample.iloc[:midpoint]["net_alpha"].mean()) if periods else None
        late = float(sample.iloc[midpoint:]["net_alpha"].mean()) if periods > 1 else None
        events = int(group["names"].sum())
        qualified = (
            events >= 100 and periods >= 40
            and mean_alpha is not None and mean_alpha > 0
            and alpha_t is not None and alpha_t > 2
            and early is not None and early > 0
            and late is not None and late > 0
        )
        rows.append({
            "variant": key[0],
            "direction": int(key[1]),
            "horizon_bars": horizon,
            "events": events,
            "independent_periods": periods,
            "mean_net_return": float(sample["net_return"].mean()) if periods else None,
            "mean_net_alpha": mean_alpha,
            "alpha_t_stat": alpha_t,
            "early_alpha": early,
            "late_alpha": late,
            "hit_rate": float((sample["net_return"] > 0).mean()) if periods else None,
            "mean_mae_pct": float(sample["mae_pct"].mean()) if periods else None,
            "mean_mfe_pct": float(sample["mfe_pct"].mean()) if periods else None,
            "qualification_status": "PRIMARY_PASS" if qualified else "NOT_QUALIFIED",
        })
    return pd.DataFrame(rows, columns=columns)