"""Daily market-discovery states: continuation and reversal are separate lanes."""
from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd

from research.evaluate import neutralize
from research.features import RISK_COLUMNS, add_market_beta, build_features, load_sector_map
from research.trend_pullback import build_trend_pullback_patterns

DISCOVERY_MODEL_VERSION = "discovery-1.0-shadow"
POSITION_OVERLAY_VERSION = "extension-0.1-shadow"
MIN_LATEST_COVERAGE_RATIO = 0.90
ACTIVITY_COLUMNS = ["volume_ratio", "vol_ratio", "range_pct"]
REQUIRED_FEATURES = [
    "mom_12_6", "mom_12_1", "rev_5", "rev_21", "dist_ma50",
    "volume_ratio", "vol_ratio", "range_pct", *RISK_COLUMNS,
]


def _evidence_json(payload: dict) -> str:
    """PostgreSQL json rejects the NaN/Infinity literals json.dumps emits by default."""
    return json.dumps({
        key: None if isinstance(value, float) and not math.isfinite(value) else value
        for key, value in payload.items()
    })


def _percentile(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame[column].rank(pct=True, method="average")


def _prepare_discovery_panel(
    panel: pd.DataFrame, full_history: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if panel.empty:
        return panel, panel

    featured = build_features(panel)
    grouped = featured.groupby("ticker", group_keys=False)
    previous_close = grouped["close"].shift(1)
    true_range = pd.concat([
        (featured["high"] - featured["low"]).abs(),
        (featured["high"] - previous_close).abs(),
        (featured["low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    featured["atr_14"] = true_range.groupby(featured["ticker"]).transform(
        lambda values: values.rolling(14).mean()
    )
    featured["up_days_10"] = grouped["ret_1"].transform(
        lambda values: values.gt(0).rolling(10).sum()
    )
    featured["down_days_10"] = grouped["ret_1"].transform(
        lambda values: values.lt(0).rolling(10).sum()
    )
    featured = add_market_beta(featured)
    sectors = load_sector_map()
    featured["sector"] = (
        featured["ticker"].map(sectors).fillna("UNKNOWN") if sectors else "UNKNOWN"
    )
    structure_panel = (
        panel if full_history else panel.groupby("ticker", group_keys=False).tail(80).copy()
    )
    structured = build_trend_pullback_patterns(structure_panel)[[
        "ticker", "date", "sma20", "sma50", "higher_swing_high",
        "higher_swing_low", "lower_swing_high", "lower_swing_low",
    ]]
    structured = structured.sort_values(["ticker", "date"])
    structured["prior_sma20"] = structured.groupby("ticker")["sma20"].shift(5)
    featured = featured.merge(structured, on=["ticker", "date"], how="left")
    featured["distance_sma20_atr"] = (
        (featured["close"] - featured["sma20"])
        / featured["atr_14"].replace(0, np.nan)
    )
    featured = featured.dropna(subset=REQUIRED_FEATURES)
    return featured, structured


def _apply_position_overlay(cross: pd.DataFrame) -> pd.DataFrame:
    """Describe extension and reversal risk without changing directional state."""
    result = cross.copy()
    uptrend = (
        (result["close"] > result["sma20"])
        & (result["sma20"] > result["sma50"])
    )
    downtrend = (
        (result["close"] < result["sma20"])
        & (result["sma20"] < result["sma50"])
    )
    result["trend_state"] = np.select(
        [uptrend, downtrend], ["UPTREND", "DOWNTREND"], default="NEUTRAL"
    )

    strong_up_move = (
        (result["recent_21d_percentile"] >= 0.80)
        & (result["recent_21d_return"] > 0)
    )
    strong_down_move = (
        (result["recent_21d_percentile"] <= 0.20)
        & (result["recent_21d_return"] < 0)
    )
    up_extension_votes = (
        (result["rsi_14"] >= 68).astype(int)
        + (result["distance_sma20_atr"] >= 1.50).astype(int)
        + (result["up_days_10"] >= 7).astype(int)
    )
    down_extension_votes = (
        (result["rsi_14"] <= 32).astype(int)
        + (result["distance_sma20_atr"] <= -1.50).astype(int)
        + (result["down_days_10"] >= 7).astype(int)
    )
    extended_up = uptrend & strong_up_move & (up_extension_votes >= 1)
    extended_down = downtrend & strong_down_move & (down_extension_votes >= 1)

    lower_high = result["lower_swing_high"].astype("boolean").fillna(False)
    lower_low = result["lower_swing_low"].astype("boolean").fillna(False)
    higher_high = result["higher_swing_high"].astype("boolean").fillna(False)
    higher_low = result["higher_swing_low"].astype("boolean").fillna(False)
    weakening_up = (
        (result["recent_5d_return"] <= 0)
        | (result["close_strength"] <= 0.35)
        | lower_high
    )
    weakening_down = (
        (result["recent_5d_return"] >= 0)
        | (result["close_strength"] >= 0.65)
        | higher_low
    )
    exhausted_up = extended_up & weakening_up
    exhausted_down = extended_down & weakening_down
    bearish_confirmed = strong_up_move & (result["close"] < result["sma20"]) & lower_high & lower_low
    bullish_confirmed = strong_down_move & (result["close"] > result["sma20"]) & higher_high & higher_low

    result["extension_risk"] = np.select(
        [exhausted_up | exhausted_down, extended_up | extended_down],
        ["EXHAUSTION_WATCH", "EXTENDED"],
        default="NORMAL",
    )
    result["reversal_trigger"] = np.select(
        [bearish_confirmed, bullish_confirmed, exhausted_up, exhausted_down],
        ["BEARISH_CONFIRMED", "BULLISH_CONFIRMED", "BEARISH_EARLY", "BULLISH_EARLY"],
        default="NONE",
    )

    guidance = pd.Series(
        "No directional edge from the extension overlay.", index=result.index
    )
    guidance.loc[uptrend] = "Uptrend intact; continuation is eligible, but use normal entry discipline."
    guidance.loc[downtrend] = "Downtrend intact; bearish continuation is eligible, but avoid chasing weakness."
    guidance.loc[extended_up] = "Uptrend extended; avoid chasing and wait for a pullback or fresh base."
    guidance.loc[extended_down] = "Downtrend extended; avoid a new short and wait for a bounce or fresh breakdown."
    guidance.loc[exhausted_up] = "Uptrend is losing momentum; avoid new longs and require confirmation before shorting."
    guidance.loc[exhausted_down] = "Downtrend is losing momentum; avoid new shorts and require confirmation before buying."
    guidance.loc[bearish_confirmed] = "Bearish reversal structure confirmed after an extended advance; manage against the failed high."
    guidance.loc[bullish_confirmed] = "Bullish reversal structure confirmed after an extended decline; manage against the failed low."
    result["position_guidance"] = guidance
    result["extension_score"] = np.select(
        [strong_up_move, strong_down_move], [up_extension_votes, -down_extension_votes],
        default=0,
    ).astype(int)
    return result


def _classify_discovery_date(featured: pd.DataFrame, latest_date) -> pd.DataFrame:
    cross = featured[featured["date"] == latest_date].copy()
    if cross.empty:
        return cross

    cross["activity_percentile"] = cross[ACTIVITY_COLUMNS].rank(pct=True).mean(axis=1)
    cross["recent_5d_return"] = -cross["rev_5"]
    cross["recent_21d_return"] = -cross["rev_21"]
    cross["recent_5d_percentile"] = _percentile(cross, "recent_5d_return")
    cross["recent_21d_percentile"] = _percentile(cross, "recent_21d_return")
    cross["older_momentum_percentile"] = _percentile(cross, "mom_12_6")
    cross["long_momentum_percentile"] = _percentile(cross, "mom_12_1")

    eligible = cross[cross["activity_percentile"] > 0.50].copy()
    if not eligible.empty:
        eligible = neutralize(
            eligible, "mom_12_6", factor_cols=RISK_COLUMNS,
            group_col="sector", out_col="echo_score",
        )
        eligible["echo_percentile"] = _percentile(eligible, "echo_score")
        cross = cross.merge(
            eligible[["ticker", "echo_score", "echo_percentile"]], on="ticker", how="left"
        )
    else:
        cross["echo_score"] = np.nan
        cross["echo_percentile"] = np.nan

    weak_old = (
        (cross["older_momentum_percentile"] <= 0.40)
        | (cross["long_momentum_percentile"] <= 0.40)
    )
    strong_recent = (
        (cross["recent_21d_percentile"] >= 0.80)
        & (cross["recent_5d_return"] > 0)
    )
    active = cross["activity_percentile"] > 0.50
    improving = (cross["close"] > cross["sma20"]) & (cross["sma20"] > cross["prior_sma20"])
    higher_high = cross["higher_swing_high"].astype("boolean").fillna(False)
    higher_low = cross["higher_swing_low"].astype("boolean").fillna(False)
    lower_high = cross["lower_swing_high"].astype("boolean").fillna(False)
    lower_low = cross["lower_swing_low"].astype("boolean").fillna(False)
    confirmed = improving & (cross["sma20"] > cross["sma50"]) & higher_high & higher_low

    bearish_recent = (
        (cross["recent_21d_percentile"] <= 0.20)
        & (cross["recent_5d_return"] < 0)
    )
    bearish_structure = (
        (cross["close"] < cross["sma20"])
        & (cross["sma20"] < cross["sma50"])
        & lower_high
        & lower_low
    )

    cross = _apply_position_overlay(cross)

    continuation = active & (cross["echo_percentile"] >= 0.90)
    reversal_watch = weak_old & strong_recent & improving
    emerging = reversal_watch & active
    confirmed_reversal = emerging & confirmed
    bullish_conflict = continuation & bearish_recent
    laggard = weak_old & bearish_recent & bearish_structure

    cross["state"] = "NEUTRAL"
    cross.loc[laggard, "state"] = "LAGGARD"
    cross.loc[continuation, "state"] = "CONTINUATION"
    cross.loc[bullish_conflict, "state"] = "CONFLICT"
    cross.loc[reversal_watch, "state"] = "REVERSAL_WATCH"
    cross.loc[emerging, "state"] = "EMERGING_REVERSAL"
    cross.loc[confirmed_reversal, "state"] = "REVERSAL_CONFIRMED"

    cross["validation_status"] = np.where(
        cross["state"] == "CONTINUATION", "CANDIDATE_ALPHA", "DISCOVERY_ONLY"
    )
    cross["evidence"] = cross.apply(
        lambda row: _evidence_json({
            "activity_pct": round(float(row["activity_percentile"]), 4),
            "echo_pct": round(float(row["echo_percentile"]), 4)
                if pd.notna(row["echo_percentile"]) else None,
            "older_momentum_pct": round(float(row["older_momentum_percentile"]), 4),
            "long_momentum_pct": round(float(row["long_momentum_percentile"]), 4),
            "recent_21d_pct": round(float(row["recent_21d_percentile"]), 4),
            "recent_21d_return": round(float(row["recent_21d_return"]), 6),
            "recent_5d_return": round(float(row["recent_5d_return"]), 6),
            "above_rising_sma20": bool(improving.loc[row.name]),
            "sma20_above_sma50": bool(row["sma20"] > row["sma50"]),
            "higher_high": bool(row["higher_swing_high"]),
            "higher_low": bool(row["higher_swing_low"]),
            "position_overlay_version": POSITION_OVERLAY_VERSION,
            "trend_state": row["trend_state"],
            "extension_risk": row["extension_risk"],
            "reversal_trigger": row["reversal_trigger"],
            "position_guidance": row["position_guidance"],
            "extension_score": int(row["extension_score"]),
            "distance_sma20_atr": round(float(row["distance_sma20_atr"]), 4),
            "rsi_14": round(float(row["rsi_14"]), 2),
            "volume_ratio": round(float(row["volume_ratio"]), 4),
            "close_strength": round(float(row["close_strength"]), 4),
            "up_days_10": int(row["up_days_10"]),
            "down_days_10": int(row["down_days_10"]),
        }),
        axis=1,
    )
    cross["model_version"] = DISCOVERY_MODEL_VERSION
    cross["trade_date"] = pd.to_datetime(cross["date"]).dt.date
    return cross.sort_values(
        ["state", "recent_21d_percentile", "echo_percentile"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def classify_discovery_states(panel: pd.DataFrame) -> pd.DataFrame:
    """Classify the latest complete cross-section without assigning trade actions."""
    featured, _ = _prepare_discovery_panel(panel)
    if featured.empty:
        return featured

    recent_counts = featured.groupby("date")["ticker"].nunique().sort_index().tail(10)
    peak_names = int(recent_counts.max())
    complete_dates = recent_counts[
        recent_counts >= peak_names * MIN_LATEST_COVERAGE_RATIO
    ].index
    if complete_dates.empty:
        return featured.iloc[0:0]
    latest_date = complete_dates.max()
    return _classify_discovery_date(featured, latest_date)


def classify_discovery_history(panel: pd.DataFrame, dates: list) -> pd.DataFrame:
    """Classify requested dates using only bars available on or before each date."""
    featured, _ = _prepare_discovery_panel(panel, full_history=True)
    if featured.empty:
        return featured
    available = set(pd.to_datetime(featured["date"]).dt.date)
    frames = [
        _classify_discovery_date(featured, pd.Timestamp(value))
        for value in dates if pd.Timestamp(value).date() in available
    ]
    populated = [frame for frame in frames if not frame.empty]
    return pd.concat(populated, ignore_index=True) if populated else featured.iloc[0:0]
