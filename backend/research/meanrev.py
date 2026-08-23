"""Shared live and point-in-time replay logic for the meanrev-1.0-shadow signal.

UNVALIDATED. This is a shadow scaffold, not a promoted model: it has not been
run through run_alpha_research.py's 5-gate contract (see docs/SIGNAL_RESEARCH.md
Section 3). Do not treat its output as a trading signal. It exists so a second
model can be exercised end-to-end (scoring, persistence, portal query) under
its own `model_version` without touching `xsmom-1.0`'s rows, mirroring the
`discovery-1.0-shadow` / `extension-0.1-shadow` pattern already used elsewhere.

Promotion path if this is ever validated: rerun run_alpha_research.py with
these features, confirm it prints ALPHA (not UNDERPOWERED/RISK_EXPOSURE/
NO_SIGNAL), then update MODEL_WEIGHTS below in a reviewed commit and drop the
"-shadow" suffix from MODEL_VERSION.
"""
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

MODEL_VERSION = "meanrev-1.0-shadow"
# rev_5 is the negated 5-day return (high = price fell = reversal-up candidate).
# rsi_14 is raw RSI; negative weight so low RSI (oversold) also scores LONG.
MODEL_FEATURES = ["rev_5", "rsi_14"]
MODEL_WEIGHTS = {"rev_5": 1.0, "rsi_14": -1.0}
HORIZON_DAYS = 5
N_DECILES = 10


def score_meanrev_cross_sections(cross: pd.DataFrame) -> pd.DataFrame:
    """Score prepared feature cross-sections using the shadow model contract."""
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


def replay_meanrev_ranks(panel: pd.DataFrame, min_names: int = 50) -> pd.DataFrame:
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
    ranks = score_meanrev_cross_sections(prepared)
    ranks["universe_size"] = ranks.groupby("date")["ticker"].transform("size")
    return ranks[columns].reset_index(drop=True)
