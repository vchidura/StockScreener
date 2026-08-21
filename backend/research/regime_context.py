"""Point-in-time market and sector context for scanner qualification studies."""
from __future__ import annotations

import numpy as np
import pandas as pd

from research.features import load_sector_map


def replay_regime_context(panel: pd.DataFrame,
                          sectors: dict[str, str] | None = None) -> pd.DataFrame:
    """Build daily breadth and volatility context using only bars through each date."""
    columns = [
        "date", "ticker", "market_breadth", "sector_breadth",
        "market_volatility_percentile",
    ]
    if panel.empty:
        return pd.DataFrame(columns=columns)

    frame = panel.sort_values(["ticker", "date"]).copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    grouped = frame.groupby("ticker", group_keys=False)
    frame["return_1"] = grouped["close"].pct_change()
    frame["sma50"] = grouped["close"].transform(
        lambda values: values.rolling(50, min_periods=50).mean()
    )
    frame = frame.dropna(subset=["sma50"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)

    sector_map = sectors if sectors is not None else load_sector_map()
    frame["sector"] = (
        frame["ticker"].map(sector_map).fillna("UNKNOWN")
        if sector_map else "UNKNOWN"
    )
    frame["above_sma50"] = frame["close"] > frame["sma50"]
    frame["market_breadth"] = frame.groupby("date")["above_sma50"].transform("mean")
    frame["sector_breadth"] = frame.groupby(
        ["date", "sector"]
    )["above_sma50"].transform("mean")

    market_return = frame.groupby("date")["return_1"].mean().sort_index()
    market_volatility = market_return.rolling(21, min_periods=21).std()
    volatility_percentile = market_volatility.rolling(
        252, min_periods=60
    ).apply(
        lambda values: pd.Series(values).rank(pct=True).iloc[-1],
        raw=False,
    )
    frame["market_volatility_percentile"] = frame["date"].map(
        volatility_percentile
    )
    frame = frame.replace([np.inf, -np.inf], np.nan)
    return frame[columns].reset_index(drop=True)