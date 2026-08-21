"""Shared live and point-in-time replay logic for the xsmom-1.0 signal."""
from __future__ import annotations

import numpy as np
import pandas as pd

from research.evaluate import neutralize
from research.features import (
    RISK_COLUMNS,
    add_market_beta,
    build_features,
    cross_sectional_zscore,
    load_sector_map,
)


MODEL_VERSION = "xsmom-1.0"
MODEL_FEATURES = ["mom_12_1"]
MODEL_WEIGHTS = {"mom_12_1": 1.0}
HORIZON_DAYS = 21
N_DECILES = 10


def score_xsmom_cross_sections(cross: pd.DataFrame) -> pd.DataFrame:
    """Score prepared feature cross-sections using the production model contract."""
    if cross.empty:
        return cross.copy()

    out = cross.copy()
    raw_score = pd.Series(0.0, index=out.index)
    for feature in MODEL_FEATURES:
        standardized = out.groupby("date")[feature].transform(
            lambda values: (values - values.mean()) / values.std()
        )
        raw_score = raw_score + standardized * MODEL_WEIGHTS[feature]
    out["raw_score"] = raw_score
    out = neutralize(
        out, "raw_score", factor_cols=RISK_COLUMNS,
        group_col="sector", out_col="neutral_score",
    ).dropna(subset=["neutral_score"])
    out["percentile"] = out.groupby("date")["neutral_score"].rank(pct=True)
    out["decile"] = out.groupby("date")["neutral_score"].transform(
        lambda values: pd.qcut(
            values.rank(method="first"), N_DECILES,
            labels=False, duplicates="drop",
        ) + 1
    ).astype("Int64")
    out["side"] = "FLAT"
    out.loc[out["decile"] == N_DECILES, "side"] = "LONG"
    out.loc[out["decile"] == 1, "side"] = "SHORT"
    return out.sort_values(["date", "neutral_score"], ascending=[True, False])


def replay_xsmom_ranks(panel: pd.DataFrame, min_names: int = 50) -> pd.DataFrame:
    """Build all historical ranks from daily bars using only data available by each close."""
    columns = [
        "date", "ticker", "raw_score", "neutral_score",
        "percentile", "decile", "side", "universe_size",
    ]
    if panel.empty:
        return pd.DataFrame(columns=columns)

    featured = add_market_beta(build_features(panel))
    sectors = load_sector_map()
    featured["sector"] = (
        featured["ticker"].map(sectors).fillna("UNKNOWN")
        if sectors else "UNKNOWN"
    )
    needed = MODEL_FEATURES + [
        column for column in RISK_COLUMNS if column not in MODEL_FEATURES
    ]
    featured = featured.dropna(subset=needed)
    counts = featured.groupby("date")["ticker"].transform("size")
    featured = featured[counts >= min_names]
    if featured.empty:
        return pd.DataFrame(columns=columns)

    prepared = cross_sectional_zscore(featured, needed).dropna(subset=needed)
    ranks = score_xsmom_cross_sections(prepared)
    ranks["universe_size"] = ranks.groupby("date")["ticker"].transform("size")
    return ranks[columns].reset_index(drop=True)


def attach_xsmom_ranks(observations: pd.DataFrame,
                       ranks: pd.DataFrame) -> pd.DataFrame:
    """Attach the latest rank available when each scanner signal was formed."""
    out = observations.copy()
    for column in (
        "xs_trade_date", "xs_side", "xs_decile", "xs_percentile",
        "xs_universe_size", "xs_age_days", "market_breadth",
        "sector_breadth", "market_volatility_percentile",
    ):
        if column in out:
            out = out.drop(columns=column)
    if out.empty or ranks.empty:
        out["xs_trade_date"] = pd.NaT
        out["xs_side"] = None
        out["xs_decile"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
        out["xs_percentile"] = np.nan
        out["xs_universe_size"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
        out["xs_age_days"] = np.nan
        out["market_breadth"] = np.nan
        out["sector_breadth"] = np.nan
        out["market_volatility_percentile"] = np.nan
        return out

    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.normalize()
    rank_frame = ranks.copy()
    rank_frame["date"] = pd.to_datetime(rank_frame["date"]).dt.normalize()
    available_dates = np.array(
        sorted(rank_frame["date"].dropna().unique()), dtype="datetime64[ns]"
    )
    event_dates = out["trade_date"].to_numpy(dtype="datetime64[ns]")
    same_close_available = out["interval"].isin(["1d", "1wk"]).to_numpy()
    positions = np.empty(len(out), dtype=int)
    positions[same_close_available] = np.searchsorted(
        available_dates, event_dates[same_close_available], side="right"
    ) - 1
    positions[~same_close_available] = np.searchsorted(
        available_dates, event_dates[~same_close_available], side="left"
    ) - 1
    valid = positions >= 0
    effective_dates = np.full(len(out), np.datetime64("NaT"), dtype="datetime64[ns]")
    effective_dates[valid] = available_dates[positions[valid]]
    out["xs_trade_date"] = effective_dates
    out["_rank_order"] = np.arange(len(out))

    rank_frame = rank_frame.rename(columns={
        "date": "xs_trade_date",
        "side": "xs_side",
        "decile": "xs_decile",
        "percentile": "xs_percentile",
        "universe_size": "xs_universe_size",
    })
    rank_columns = [
        column for column in (
            "xs_trade_date", "ticker", "xs_side", "xs_decile",
            "xs_percentile", "xs_universe_size", "market_breadth",
            "sector_breadth", "market_volatility_percentile",
        ) if column in rank_frame
    ]
    out = out.merge(
        rank_frame[rank_columns],
        on=["xs_trade_date", "ticker"], how="left",
    ).sort_values("_rank_order").drop(columns="_rank_order")
    if "xs_side" not in out:
        out["xs_side"] = "FLAT"
        out.loc[out["xs_decile"] == N_DECILES, "xs_side"] = "LONG"
        out.loc[out["xs_decile"] == 1, "xs_side"] = "SHORT"
        out.loc[out["xs_decile"].isna(), "xs_side"] = None
    if "xs_universe_size" not in out:
        out["xs_universe_size"] = pd.Series(
            pd.NA, index=out.index, dtype="Int64"
        )
    out["xs_age_days"] = (
        out["trade_date"] - out["xs_trade_date"]
    ).dt.days
    return out.reset_index(drop=True)