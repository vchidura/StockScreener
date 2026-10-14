"""Pure gap occurrence and lifecycle calculations shared with legacy screeners."""
from typing import Dict, List

import numpy as np
import pandas as pd


def _gap_candidate_mask(df: pd.DataFrame, session_start_only: bool) -> np.ndarray:
    mask = np.ones(max(len(df) - 1, 0), dtype=bool)
    if session_start_only and len(df) > 1:
        dates = pd.DatetimeIndex(pd.to_datetime(df.index)).normalize()
        mask &= np.asarray(dates[1:] != dates[:-1])
    return mask


def identify_gap_up(df: pd.DataFrame, gap_threshold: float = 0.01, *,
                    session_start_only: bool = False) -> List[Dict]:
    prev_high = df["high"].values[:-1]
    prev_close = df["close"].values[:-1]
    curr_open = df["open"].values[1:]
    curr_low = df["low"].values[1:]
    gap_pct = (curr_open - prev_high) / prev_high
    indices = np.where((gap_pct >= gap_threshold) & _gap_candidate_mask(df, session_start_only))[0] + 1
    return [{
        "index": int(idx), "date": df.index[idx],
        "prev_high": float(prev_high[idx - 1]),
        "previous_close": float(prev_close[idx - 1]),
        "gap_open": float(curr_open[idx - 1]),
        "gap_low": float(curr_low[idx - 1]),
        "gap_pct": round(float(gap_pct[idx - 1]) * 100, 2),
        "range_gap_survived": bool(curr_low[idx - 1] > prev_high[idx - 1]),
    } for idx in indices]


def identify_gap_down(df: pd.DataFrame, gap_threshold: float = 0.01, *,
                      session_start_only: bool = False) -> List[Dict]:
    prev_low = df["low"].values[:-1]
    prev_close = df["close"].values[:-1]
    curr_open = df["open"].values[1:]
    curr_high = df["high"].values[1:]
    gap_pct = (prev_low - curr_open) / prev_low
    indices = np.where((gap_pct >= gap_threshold) & _gap_candidate_mask(df, session_start_only))[0] + 1
    return [{
        "index": int(idx), "date": df.index[idx],
        "prev_low": float(prev_low[idx - 1]),
        "previous_close": float(prev_close[idx - 1]),
        "gap_open": float(curr_open[idx - 1]),
        "gap_high": float(curr_high[idx - 1]),
        "gap_pct": round(float(gap_pct[idx - 1]) * 100, 2),
        "range_gap_survived": bool(curr_high[idx - 1] < prev_low[idx - 1]),
    } for idx in indices]


def _gap_lifecycle(df: pd.DataFrame, gap: Dict, direction: str) -> Dict:
    gap_index, gap_open, previous_close = gap["index"], gap["gap_open"], gap["previous_close"]
    if direction == "UP":
        path = df["low"].values[gap_index:]
        fill_progress = gap_open - float(np.min(path))
        filled_offsets = np.where(path <= previous_close)[0]
        opening_gap = gap_open - previous_close
    else:
        path = df["high"].values[gap_index:]
        fill_progress = float(np.max(path)) - gap_open
        filled_offsets = np.where(path >= previous_close)[0]
        opening_gap = previous_close - gap_open
    fill_pct = round(max(0.0, min(100.0, fill_progress / opening_gap * 100)), 2)
    first_fill_index = gap_index + int(filled_offsets[0]) if len(filled_offsets) else None
    lifecycle = ("SAME_SESSION_FADE" if first_fill_index == gap_index else "FILLED"
                 if first_fill_index is not None else "PARTIALLY_FILLED" if fill_pct > 0 else "OPEN")
    latest_close = float(df.iloc[-1]["close"])
    if first_fill_index is not None and (
        direction == "UP" and latest_close < previous_close
        or direction == "DOWN" and latest_close > previous_close
    ):
        lifecycle = "FAILED"
    first_fill_date = df.index[first_fill_index] if first_fill_index is not None else None
    normalized_dates = pd.DatetimeIndex(pd.to_datetime(df.index[gap_index:])).normalize()
    return {
        "gap_direction": direction, "gap_lifecycle": lifecycle, "fill_pct": fill_pct,
        "first_fill_date": first_fill_date.strftime("%Y-%m-%d") if hasattr(first_fill_date, "strftime") else None,
        "fill_target": round(float(previous_close), 2),
        "gap_age_bars": len(df) - 1 - gap_index,
        "gap_age_sessions": max(len(normalized_dates.unique()) - 1, 0),
        "range_gap_survived": gap["range_gap_survived"],
    }
