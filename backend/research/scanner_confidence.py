"""Evaluate predeclared scanner confidence filters against persisted outcomes."""
from __future__ import annotations

import json
from statistics import NormalDist

import numpy as np
import pandas as pd


ALIGNED_LONG_STATES = {"CONTINUATION", "EMERGING_REVERSAL", "REVERSAL_CONFIRMED"}
ALIGNED_SHORT_STATES = {"CONFLICT", "LAGGARD"}
SESSION_TIMEZONE = "America/New_York"


def _session_hour(value) -> int | None:
    """Hourly signal_time is stored UTC; slices are declared in exchange local time."""
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return int(timestamp.tz_convert(SESSION_TIMEZONE).hour)


def _metadata(value) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return {}


def _finite(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def confidence_slices(row: pd.Series) -> list[str]:
    """Return fixed, interpretable filters applicable to one scanner event."""
    scanner = str(row["scanner_name"])
    direction = int(row["direction"])
    trigger = str(row.get("trigger_type") or "")
    state = str(row.get("discovery_state") or "").upper()
    metadata = _metadata(row.get("metadata"))
    volume_ratio = _finite(metadata.get("volume_ratio"))
    range_ratio = _finite(metadata.get("range_ratio"))
    pivot_age = _finite(metadata.get("pivot_age_bars"))
    overnight_gap = _finite(metadata.get("overnight_gap_atr"))
    cluster_count = _finite(metadata.get("level_cluster_count"))
    slices = ["baseline"]

    if pivot_age is not None and pivot_age >= 10:
        slices.append("pivot_age_10_plus")
    if overnight_gap is not None and abs(overnight_gap) <= 0.25:
        slices.append("small_overnight_gap")
    if cluster_count is not None and cluster_count >= 2:
        slices.append("level_clustered")

    if str(row.get("interval") or "") == "1h":
        session_hour = _session_hour(row.get("signal_time"))
        if session_hour in (9, 10):
            slices.append("hour_am_open")
        elif session_hour in (11, 12, 13):
            slices.append("hour_midday")
        elif session_hour in (14, 15):
            slices.append("hour_pm_close")

    aligned = (
        direction == 1 and state in ALIGNED_LONG_STATES
    ) or (
        direction == -1 and state in ALIGNED_SHORT_STATES
    )
    if aligned:
        slices.append("discovery_aligned")

    rank_age = _finite(row.get("xs_age_days"))
    rank_side = str(row.get("xs_side") or "").upper()
    rank_percentile = _finite(row.get("xs_percentile"))
    market_breadth = _finite(row.get("market_breadth"))
    sector_breadth = _finite(row.get("sector_breadth"))
    volatility_percentile = _finite(row.get("market_volatility_percentile"))
    rank_fresh = rank_age is not None and 0 <= rank_age <= 7
    if rank_fresh and (
        (direction == 1 and rank_side == "LONG")
        or (direction == -1 and rank_side == "SHORT")
    ):
        slices.append("rank_actionable_aligned")
    if rank_fresh and rank_percentile is not None and (
        (direction == 1 and rank_percentile >= 0.80)
        or (direction == -1 and rank_percentile <= 0.20)
    ):
        slices.append("rank_quintile_aligned")
    market_aligned = rank_fresh and market_breadth is not None and (
        (direction == 1 and market_breadth >= 0.60)
        or (direction == -1 and market_breadth <= 0.40)
    )
    sector_aligned = rank_fresh and sector_breadth is not None and (
        (direction == 1 and sector_breadth >= 0.60)
        or (direction == -1 and sector_breadth <= 0.40)
    )
    if market_aligned:
        slices.append("market_breadth_aligned")
    if sector_aligned:
        slices.append("sector_breadth_aligned")
    if market_aligned and sector_aligned:
        slices.append("market_sector_breadth_aligned")

    # Same point-in-time attachment and staleness rule as market/sector breadth.
    trend_state = str(row.get("trend_state") or "").upper()
    if rank_fresh and (
        (direction == 1 and trend_state == "UPTREND")
        or (direction == -1 and trend_state == "DOWNTREND")
    ):
        slices.append("daily_trend_aligned")

    if rank_fresh and volatility_percentile is not None:
        if volatility_percentile <= 0.35:
            slices.append("market_low_volatility")
        if volatility_percentile >= 0.65:
            slices.append("market_high_volatility")

    if volume_ratio is not None and range_ratio is not None:
        if volume_ratio >= 1.25 and range_ratio >= 1.25:
            slices.append("participation_both_1_25")
        if volume_ratio >= 1.50 and range_ratio >= 1.50:
            slices.append("participation_both_1_50")

    if scanner == "breakout_expansion":
        close_location = _finite(metadata.get("close_location"))
        extreme_close = close_location is not None and (
            (direction == 1 and close_location >= 0.85)
            or (direction == -1 and close_location <= 0.15)
        )
        if extreme_close:
            slices.append("breakout_extreme_close")
        if (extreme_close and volume_ratio is not None and range_ratio is not None
                and volume_ratio >= 1.50 and range_ratio >= 1.50):
            slices.append("breakout_high_quality")

    if scanner == "structured_trend_pullback":
        pullback_bars = _finite(metadata.get("pullback_bars"))
        pullback_volume = _finite(metadata.get("pullback_volume_ratio"))
        trigger_volume = _finite(metadata.get("trigger_volume_ratio"))
        pullback_speed = _finite(metadata.get("pullback_speed_atr_per_bar"))
        swing_origin_distance = _finite(metadata.get("swing_origin_distance_atr"))
        if pullback_bars is not None and 2 <= pullback_bars <= 5:
            slices.append("pullback_2_5_bars")
        if pullback_volume is not None and pullback_volume <= 0.85:
            slices.append("pullback_volume_contraction")
        if (pullback_volume is not None and trigger_volume is not None
                and pullback_volume <= 0.85 and trigger_volume >= 1.25):
            slices.append("pullback_contract_then_expand")
        if pullback_speed is not None and pullback_speed <= 0.50:
            slices.append("pullback_orderly_speed")
        if swing_origin_distance is not None and swing_origin_distance <= 1.50:
            slices.append("pullback_near_swing_origin")
        if metadata.get("vwap_reclaim") is True:
            slices.append("pullback_vwap_reclaim")

    if scanner == "compression_breakout":
        compression_band = _finite(metadata.get("compression_band_atr"))
        atr_contraction = _finite(metadata.get("atr_contraction_ratio"))
        if compression_band is not None and compression_band <= 2.0:
            slices.append("compression_tight_band")
        if atr_contraction is not None and atr_contraction <= 0.75:
            slices.append("compression_deep_atr_contraction")
        if metadata.get("trend_aligned") is True:
            slices.append("compression_trend_aligned")

    if scanner == "failed_breakout_reversal":
        prior_tests = _finite(metadata.get("prior_level_tests"))
        breakout_volume = _finite(metadata.get("breakout_volume_ratio"))
        if prior_tests is not None and prior_tests <= 1:
            slices.append("failed_breakout_fresh_level")
        if breakout_volume is not None and breakout_volume >= 1.25:
            slices.append("failed_breakout_participation")
        if metadata.get("follow_through_failed") is True:
            slices.append("failed_breakout_low_follow_through")

    if scanner == "level_retest_rejection":
        source = str(metadata.get("level_source") or "")
        family = "fibonacci" if source.startswith("fib_") else source
        if family in {"gap", "fvg", "fibonacci"}:
            slices.append(f"level_{family}")

    candle = trigger.rsplit(":", 1)[-1]
    if candle in {
        "hammer", "bullish_engulfing", "bullish_strong_close",
        "shooting_star", "bearish_engulfing", "bearish_strong_close",
    }:
        slices.append(f"trigger_{candle}")
    return slices


def expand_confidence_slices(observations: pd.DataFrame) -> pd.DataFrame:
    """Expand each event outcome into its applicable predeclared study slices."""
    if observations.empty:
        return observations.assign(slice_name=pd.Series(dtype="object"))

    metadata = observations["metadata"].map(_metadata)
    volume_ratio = metadata.map(lambda value: _finite(value.get("volume_ratio")))
    range_ratio = metadata.map(lambda value: _finite(value.get("range_ratio")))
    close_location = metadata.map(lambda value: _finite(value.get("close_location")))
    pivot_age = pd.to_numeric(
        metadata.map(lambda value: _finite(value.get("pivot_age_bars"))),
        errors="coerce",
    )
    overnight_gap = pd.to_numeric(
        metadata.map(lambda value: _finite(value.get("overnight_gap_atr"))),
        errors="coerce",
    )
    cluster_count = pd.to_numeric(
        metadata.map(lambda value: _finite(value.get("level_cluster_count"))),
        errors="coerce",
    )
    scanner = observations["scanner_name"].astype(str)
    direction = pd.to_numeric(observations["direction"], errors="coerce")
    state = observations["discovery_state"].fillna("").astype(str).str.upper()
    trigger = observations["trigger_type"].fillna("").astype(str)
    frames = [observations.assign(slice_name="baseline")]

    def append(mask: pd.Series, slice_name: str) -> None:
        if bool(mask.any()):
            frames.append(observations.loc[mask].assign(slice_name=slice_name))

    append(pivot_age >= 10, "pivot_age_10_plus")
    append(overnight_gap.abs() <= 0.25, "small_overnight_gap")
    append(cluster_count >= 2, "level_clustered")

    session_hour = pd.to_datetime(
        observations.get("signal_time", pd.Series(pd.NaT, index=observations.index)),
        utc=True, errors="coerce",
    ).dt.tz_convert(SESSION_TIMEZONE).dt.hour
    hourly = observations.get(
        "interval", pd.Series("", index=observations.index)
    ).astype(str) == "1h"
    append(hourly & session_hour.isin([9, 10]), "hour_am_open")
    append(hourly & session_hour.isin([11, 12, 13]), "hour_midday")
    append(hourly & session_hour.isin([14, 15]), "hour_pm_close")

    aligned = (
        (direction == 1) & state.isin(ALIGNED_LONG_STATES)
    ) | (
        (direction == -1) & state.isin(ALIGNED_SHORT_STATES)
    )
    append(aligned, "discovery_aligned")

    missing_numeric = pd.Series(np.nan, index=observations.index, dtype=float)
    rank_age = pd.to_numeric(
        observations.get("xs_age_days", missing_numeric), errors="coerce"
    )
    rank_side = observations.get(
        "xs_side", pd.Series("", index=observations.index)
    ).fillna("").astype(str).str.upper()
    rank_percentile = pd.to_numeric(
        observations.get("xs_percentile", missing_numeric), errors="coerce"
    )
    market_breadth = pd.to_numeric(
        observations.get("market_breadth", missing_numeric), errors="coerce"
    )
    sector_breadth = pd.to_numeric(
        observations.get("sector_breadth", missing_numeric), errors="coerce"
    )
    volatility_percentile = pd.to_numeric(
        observations.get("market_volatility_percentile", missing_numeric),
        errors="coerce",
    )
    rank_fresh = rank_age.between(0, 7, inclusive="both")
    append(
        rank_fresh & (
            ((direction == 1) & (rank_side == "LONG"))
            | ((direction == -1) & (rank_side == "SHORT"))
        ),
        "rank_actionable_aligned",
    )
    append(
        rank_fresh & (
            ((direction == 1) & (rank_percentile >= 0.80))
            | ((direction == -1) & (rank_percentile <= 0.20))
        ),
        "rank_quintile_aligned",
    )
    market_aligned = rank_fresh & (
        ((direction == 1) & (market_breadth >= 0.60))
        | ((direction == -1) & (market_breadth <= 0.40))
    )
    sector_aligned = rank_fresh & (
        ((direction == 1) & (sector_breadth >= 0.60))
        | ((direction == -1) & (sector_breadth <= 0.40))
    )
    append(market_aligned, "market_breadth_aligned")
    append(sector_aligned, "sector_breadth_aligned")
    append(
        market_aligned & sector_aligned,
        "market_sector_breadth_aligned",
    )
    trend_state = observations.get(
        "trend_state", pd.Series("", index=observations.index)
    ).fillna("").astype(str).str.upper()
    append(
        rank_fresh & (
            ((direction == 1) & (trend_state == "UPTREND"))
            | ((direction == -1) & (trend_state == "DOWNTREND"))
        ),
        "daily_trend_aligned",
    )
    append(
        rank_fresh & (volatility_percentile <= 0.35),
        "market_low_volatility",
    )
    append(
        rank_fresh & (volatility_percentile >= 0.65),
        "market_high_volatility",
    )
    both_1_25 = (volume_ratio >= 1.25) & (range_ratio >= 1.25)
    both_1_50 = (volume_ratio >= 1.50) & (range_ratio >= 1.50)
    append(both_1_25, "participation_both_1_25")
    append(both_1_50, "participation_both_1_50")

    breakout = scanner == "breakout_expansion"
    extreme_close = breakout & (
        ((direction == 1) & (close_location >= 0.85))
        | ((direction == -1) & (close_location <= 0.15))
    )
    append(extreme_close, "breakout_extreme_close")
    append(extreme_close & both_1_50, "breakout_high_quality")

    pullback = scanner == "structured_trend_pullback"
    pullback_bars = metadata.map(
        lambda value: _finite(value.get("pullback_bars"))
    )
    pullback_volume = metadata.map(
        lambda value: _finite(value.get("pullback_volume_ratio"))
    )
    trigger_volume = metadata.map(
        lambda value: _finite(value.get("trigger_volume_ratio"))
    )
    pullback_speed = pd.to_numeric(metadata.map(
        lambda value: _finite(value.get("pullback_speed_atr_per_bar"))
    ), errors="coerce")
    swing_origin_distance = pd.to_numeric(metadata.map(
        lambda value: _finite(value.get("swing_origin_distance_atr"))
    ), errors="coerce")
    vwap_reclaim = metadata.map(lambda value: value.get("vwap_reclaim") is True)
    append(
        pullback & pullback_bars.between(2, 5, inclusive="both"),
        "pullback_2_5_bars",
    )
    append(
        pullback & (pullback_volume <= 0.85),
        "pullback_volume_contraction",
    )
    append(
        pullback & (pullback_volume <= 0.85) & (trigger_volume >= 1.25),
        "pullback_contract_then_expand",
    )
    append(
        pullback & (pullback_speed <= 0.50),
        "pullback_orderly_speed",
    )
    append(
        pullback & (swing_origin_distance <= 1.50),
        "pullback_near_swing_origin",
    )
    append(pullback & vwap_reclaim, "pullback_vwap_reclaim")

    compression = scanner == "compression_breakout"
    compression_band = pd.to_numeric(metadata.map(
        lambda value: _finite(value.get("compression_band_atr"))
    ), errors="coerce")
    atr_contraction = pd.to_numeric(metadata.map(
        lambda value: _finite(value.get("atr_contraction_ratio"))
    ), errors="coerce")
    trend_aligned = metadata.map(lambda value: value.get("trend_aligned") is True)
    append(compression & (compression_band <= 2.0), "compression_tight_band")
    append(
        compression & (atr_contraction <= 0.75),
        "compression_deep_atr_contraction",
    )
    append(compression & trend_aligned, "compression_trend_aligned")

    failed_breakout = scanner == "failed_breakout_reversal"
    prior_tests = metadata.map(
        lambda value: _finite(value.get("prior_level_tests"))
    )
    breakout_volume = metadata.map(
        lambda value: _finite(value.get("breakout_volume_ratio"))
    )
    follow_through_failed = metadata.map(
        lambda value: value.get("follow_through_failed") is True
    )
    append(
        failed_breakout & (prior_tests <= 1),
        "failed_breakout_fresh_level",
    )
    append(
        failed_breakout & (breakout_volume >= 1.25),
        "failed_breakout_participation",
    )
    append(
        failed_breakout & follow_through_failed,
        "failed_breakout_low_follow_through",
    )

    level_retest = scanner == "level_retest_rejection"
    level_source = metadata.map(lambda value: str(value.get("level_source") or ""))
    level_family = level_source.where(
        ~level_source.str.startswith("fib_"), "fibonacci"
    )
    for family in ("gap", "fvg", "fibonacci"):
        append(level_retest & (level_family == family), f"level_{family}")

    candle = trigger.str.rsplit(":", n=1).str[-1]
    for candle_name in (
        "hammer", "bullish_engulfing", "bullish_strong_close",
        "shooting_star", "bearish_engulfing", "bearish_strong_close",
    ):
        append(candle == candle_name, f"trigger_{candle_name}")
    return pd.concat(frames, ignore_index=True)


def _t_stat(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    standard_deviation = numeric.std(ddof=1)
    if len(numeric) < 2 or not np.isfinite(standard_deviation) or standard_deviation <= 0:
        return None
    return float(numeric.mean() / (standard_deviation / np.sqrt(len(numeric))))


def _normal_p_value(t_stat: float | None) -> float | None:
    """Two-sided normal approximation; qualification samples require >=40 periods."""
    if t_stat is None or not np.isfinite(t_stat):
        return None
    return float(2 * (1 - NormalDist().cdf(abs(float(t_stat)))))


def _benjamini_hochberg(values: pd.Series) -> pd.Series:
    """Control false-discovery rate across the predeclared slice family."""
    result = pd.Series(np.nan, index=values.index, dtype=float)
    valid = pd.to_numeric(values, errors="coerce").dropna().sort_values()
    count = len(valid)
    if not count:
        return result
    adjusted = pd.Series(index=valid.index, dtype=float)
    running = 1.0
    for rank in range(count, 0, -1):
        index = valid.index[rank - 1]
        running = min(running, float(valid.iloc[rank - 1]) * count / rank)
        adjusted.loc[index] = min(1.0, running)
    result.loc[adjusted.index] = adjusted
    return result


def _select_independent(group: pd.DataFrame, horizon: int,
                        calendar: dict) -> pd.DataFrame:
    selected = []
    last_ordinal = -horizon
    for row_index, row in group.sort_values("signal_time").iterrows():
        key = pd.Timestamp(row["trade_date"]).date() \
            if row["interval"] in ("1d", "1wk") \
            else pd.Timestamp(row["signal_time"])
        if isinstance(key, pd.Timestamp) and key.tzinfo is not None:
            key = key.tz_convert("UTC").to_pydatetime()
        ordinal = calendar.get(key)
        if ordinal is not None and ordinal - last_ordinal >= horizon:
            selected.append(row_index)
            last_ordinal = ordinal
    return group.loc[selected].sort_values("signal_time")


def summarize_confidence_slices(observations: pd.DataFrame,
                                calendars: dict[str, dict]) -> pd.DataFrame:
    """Summarize absolute and paired incremental alpha for every fixed slice."""
    columns = [
        "scanner_name", "scanner_version", "interval", "direction",
        "horizon_bars", "slice_name", "events", "independent_periods",
        "mean_net_alpha", "alpha_t_stat", "early_alpha", "late_alpha",
        "hit_rate", "mean_incremental_alpha", "incremental_t_stat",
        "early_incremental_alpha", "late_incremental_alpha",
        "early_alpha_t_stat", "late_alpha_t_stat",
        "early_incremental_t_stat", "late_incremental_t_stat",
        "absolute_p_value", "incremental_p_value", "absolute_fdr_q",
        "incremental_fdr_q", "status", "robustness_status",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    expanded = expand_confidence_slices(observations)
    group_columns = [
        "scanner_name", "scanner_version", "interval", "direction",
        "horizon_bars", "slice_name", "signal_time", "trade_date",
    ]
    portfolios = expanded.groupby(group_columns, as_index=False).agg(
        net_alpha=("net_alpha", "mean"),
        net_return=("net_return", "mean"),
        names=("ticker", "nunique"),
    )
    baseline = portfolios[portfolios["slice_name"] == "baseline"].rename(
        columns={"net_alpha": "baseline_alpha"}
    )
    baseline_keys = [
        "scanner_name", "scanner_version", "interval", "direction",
        "horizon_bars", "signal_time", "trade_date",
    ]
    portfolios = portfolios.merge(
        baseline[baseline_keys + ["baseline_alpha"]], on=baseline_keys, how="left"
    )
    portfolios["incremental_alpha"] = (
        portfolios["net_alpha"] - portfolios["baseline_alpha"]
    )

    result = []
    summary_keys = [
        "scanner_name", "scanner_version", "interval", "direction",
        "horizon_bars", "slice_name",
    ]
    for key, group in portfolios.groupby(summary_keys, sort=True):
        interval = str(key[2])
        sample = _select_independent(
            group, int(key[4]), calendars.get(interval, {})
        )
        periods = len(sample)
        midpoint = max(1, periods // 2)
        early = sample.iloc[:midpoint]
        late = sample.iloc[midpoint:]
        alpha = pd.to_numeric(sample["net_alpha"], errors="coerce")
        incremental = pd.to_numeric(sample["incremental_alpha"], errors="coerce")
        events = int(group["names"].sum())
        mean_alpha = float(alpha.mean()) if periods else None
        mean_incremental = float(incremental.mean()) if periods else None
        alpha_t = _t_stat(alpha)
        incremental_t = _t_stat(incremental)
        early_alpha = float(early["net_alpha"].mean()) if len(early) else None
        late_alpha = float(late["net_alpha"].mean()) if len(late) else None
        early_incremental = float(early["incremental_alpha"].mean()) if len(early) else None
        late_incremental = float(late["incremental_alpha"].mean()) if len(late) else None
        early_alpha_t = _t_stat(early["net_alpha"])
        late_alpha_t = _t_stat(late["net_alpha"])
        early_incremental_t = _t_stat(early["incremental_alpha"])
        late_incremental_t = _t_stat(late["incremental_alpha"])
        absolute_pass = (
            events >= 100 and periods >= 40
            and mean_alpha is not None and mean_alpha > 0
            and alpha_t is not None and alpha_t > 2
            and early_alpha is not None and early_alpha > 0
            and late_alpha is not None and late_alpha > 0
        )
        incremental_pass = key[5] == "baseline" or (
            mean_incremental is not None and mean_incremental > 0
            and incremental_t is not None and incremental_t > 2
            and early_incremental is not None and early_incremental > 0
            and late_incremental is not None and late_incremental > 0
        )
        result.append({
            "scanner_name": key[0],
            "scanner_version": key[1],
            "interval": interval,
            "direction": int(key[3]),
            "horizon_bars": int(key[4]),
            "slice_name": key[5],
            "events": events,
            "independent_periods": periods,
            "mean_net_alpha": mean_alpha,
            "alpha_t_stat": alpha_t,
            "early_alpha": early_alpha,
            "late_alpha": late_alpha,
            "hit_rate": float((sample["net_return"] > 0).mean()) if periods else None,
            "mean_incremental_alpha": mean_incremental,
            "incremental_t_stat": incremental_t,
            "early_incremental_alpha": early_incremental,
            "late_incremental_alpha": late_incremental,
            "early_alpha_t_stat": early_alpha_t,
            "late_alpha_t_stat": late_alpha_t,
            "early_incremental_t_stat": early_incremental_t,
            "late_incremental_t_stat": late_incremental_t,
            "absolute_p_value": _normal_p_value(alpha_t),
            "incremental_p_value": _normal_p_value(incremental_t),
            "absolute_fdr_q": None,
            "incremental_fdr_q": None,
            "status": "CONFIDENCE_PASS" if absolute_pass and incremental_pass
                else "NOT_QUALIFIED",
            "robustness_status": "NOT_ROBUST",
        })
    report = pd.DataFrame(result, columns=columns)
    baseline_mask = report["slice_name"] == "baseline"
    candidate_mask = report["slice_name"] != "baseline"
    report.loc[baseline_mask, "absolute_fdr_q"] = _benjamini_hochberg(
        report.loc[baseline_mask, "absolute_p_value"]
    )
    report.loc[candidate_mask, "absolute_fdr_q"] = _benjamini_hochberg(
        report.loc[candidate_mask, "absolute_p_value"]
    )
    report.loc[candidate_mask, "incremental_fdr_q"] = _benjamini_hochberg(
        report.loc[candidate_mask, "incremental_p_value"]
    )
    robust_baseline = (
        baseline_mask
        & (report["status"] == "CONFIDENCE_PASS")
        & (report["absolute_fdr_q"] <= 0.05)
    )
    robust_filter = (
        candidate_mask
        &
        (report["status"] == "CONFIDENCE_PASS")
        & (report["absolute_fdr_q"] <= 0.05)
        & (report["incremental_fdr_q"] <= 0.05)
    )
    report.loc[robust_baseline | robust_filter, "robustness_status"] = "ROBUST_PASS"
    return report