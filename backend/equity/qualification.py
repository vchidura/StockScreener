"""Qualification and calibration of versioned equity research outcomes."""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import NormalDist
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

import numpy as np
import pandas as pd
import exchange_calendars

from research.scanner_calibration import walk_forward_calibration
from research.scanner_confidence import _benjamini_hochberg

from .polygon import canonical_json, sha256_json


@dataclass(frozen=True, slots=True)
class QualificationRevision:
    qualification_revision_id: UUID
    source_name: str
    source_version: str
    interval: str | None
    direction: int | None
    horizon_key: str
    outcome_policy_key: str
    evaluation_version: str
    qualification_state: str
    effective_from: datetime
    sample_size: int
    independent_periods: int
    mean_net_alpha: float | None
    alpha_t_stat: float | None
    alpha_fdr_q: float | None
    calibrated_probability: float | None
    probability_ci_low: float | None
    probability_ci_high: float | None
    brier_score: float | None
    brier_skill_score: float | None
    expected_calibration_error: float | None
    report_identity: str
    metrics_json: str


def qualify_outcomes(
    observations: pd.DataFrame,
    *,
    effective_from: datetime,
    evaluation_version: str = "equity_qualification_v1",
    minimum_events: int = 100,
    minimum_independent_periods: int = 40,
    research_scope: str = "EQUITY_SIGNAL",
    publication_metadata: Mapping[str, Any] | None = None,
) -> tuple[QualificationRevision, ...]:
    if effective_from.tzinfo is None or effective_from.utcoffset() is None:
        raise ValueError("effective_from must be timezone-aware")
    if observations.empty:
        return ()
    if research_scope not in ("EQUITY_SIGNAL", "OPTION_CONDITIONING"):
        raise ValueError("research_scope is invalid")
    metadata = dict(publication_metadata or {})
    required = {
        "ticker", "source_name", "source_version", "interval", "direction",
        "horizon_key", "horizon_bars", "policy_key", "signal_time",
        "net_return", "net_alpha",
    }
    missing = required - set(observations.columns)
    if missing:
        raise ValueError(f"qualification observations are missing: {sorted(missing)}")
    frame = observations.copy()
    frame["signal_time"] = pd.to_datetime(frame["signal_time"], utc=True)
    for column in ("net_return", "net_alpha", "mae_pct", "mfe_pct"):
        if column not in frame:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "first_hit" in frame:
        frame["stop_first"] = frame["first_hit"].eq("STOP").astype(float)
        frame["target_first"] = frame["first_hit"].eq("TARGET").astype(float)
    else:
        frame["stop_first"] = np.nan
        frame["target_first"] = np.nan
    if "has_bracket" in frame:
        frame["stop_first"] = frame["stop_first"].where(frame["has_bracket"])
        frame["target_first"] = frame["target_first"].where(frame["has_bracket"])
    for column in ("stop_hit", "target_hit"):
        values = (
            frame[column].map(lambda value: float(value) if pd.notna(value) else np.nan)
            if column in frame else np.nan
        )
        if "has_bracket" in frame:
            values = pd.Series(values, index=frame.index).where(frame["has_bracket"])
        frame[f"{column}_rate"] = values
    if "sector_net_alpha" not in frame:
        frame["sector_net_alpha"] = np.nan
    frame["sector_net_alpha"] = pd.to_numeric(
        frame["sector_net_alpha"], errors="coerce"
    )
    if "primary_benchmark" not in frame:
        frame["primary_benchmark"] = "MARKET"
    frame["primary_benchmark"] = frame["primary_benchmark"].fillna("MARKET")
    invalid_benchmarks = set(frame["primary_benchmark"]) - {"MARKET", "SECTOR"}
    if invalid_benchmarks:
        raise ValueError(f"invalid primary benchmarks: {sorted(invalid_benchmarks)}")
    frame["primary_alpha"] = frame["net_alpha"].where(
        frame["primary_benchmark"].eq("MARKET"),
        frame["sector_net_alpha"],
    )
    frame = frame.dropna(subset=["net_return", "primary_alpha"])
    if frame.empty:
        return ()
    cohort_sha256 = _cohort_sha256(frame)

    grouping = [
        "source_name", "source_version", "interval", "direction",
        "horizon_key", "horizon_bars", "policy_key",
    ]
    candidates = []
    for key, group in frame.groupby(grouping, dropna=False, sort=True):
        portfolio = group.groupby("signal_time", as_index=False).agg(
            net_return=("net_return", "mean"),
            market_net_alpha=("net_alpha", "mean"),
            sector_net_alpha=("sector_net_alpha", "mean"),
            primary_alpha=("primary_alpha", "mean"),
            mae_pct=("mae_pct", "mean"),
            mfe_pct=("mfe_pct", "mean"),
            stop_first=("stop_first", "mean"),
            target_first=("target_first", "mean"),
            stop_hit_rate=("stop_hit_rate", "mean"),
            target_hit_rate=("target_hit_rate", "mean"),
            names=("ticker", "nunique"),
        )
        independent = _independent_periods(
            portfolio, int(key[5]), interval=None if pd.isna(key[2]) else str(key[2])
        )
        periods = len(independent)
        midpoint = max(1, periods // 2)
        early_alpha = _mean(independent.iloc[:midpoint]["primary_alpha"])
        late_alpha = _mean(independent.iloc[midpoint:]["primary_alpha"])
        mean_alpha = _mean(independent["primary_alpha"])
        alpha_t = _t_stat(independent["primary_alpha"])
        mean_return = _mean(independent["net_return"])
        return_t = _t_stat(independent["net_return"])
        early_return = _mean(independent.iloc[:midpoint]["net_return"])
        late_return = _mean(independent.iloc[midpoint:]["net_return"])
        hit_rate = _mean((independent["net_return"] > 0).astype(float))
        mean_mae = _mean(independent["mae_pct"])
        mean_mfe = _mean(independent["mfe_pct"])
        stop_first_rate = _mean(independent["stop_first"])
        target_first_rate = _mean(independent["target_first"])
        stop_hit_rate = _mean(independent["stop_hit_rate"])
        target_hit_rate = _mean(independent["target_hit_rate"])
        raw_pass = (
            int(group["ticker"].count()) >= minimum_events
            and periods >= minimum_independent_periods
            and mean_return is not None and mean_return > 0
            and return_t is not None and return_t > 2
            and early_return is not None and early_return > 0
            and late_return is not None and late_return > 0
            and mean_alpha is not None and mean_alpha > 0
            and alpha_t is not None and alpha_t > 2
            and early_alpha is not None and early_alpha > 0
            and late_alpha is not None and late_alpha > 0
        )
        calibration = walk_forward_calibration(
            independent["net_return"], independent["primary_alpha"]
        )
        wins = int((independent["net_return"] > 0).sum())
        hit_ci_low, hit_ci_high = _wilson_interval(wins, periods)
        signal_times = pd.to_datetime(group["signal_time"], errors="coerce").dropna()
        candidates.append({
            "key": key,
            "sample_size": int(group["ticker"].count()),
            "independent_periods": periods,
            "distinct_tickers": int(group["ticker"].nunique()),
            "top5_concentration": _concentration(group["ticker"]),
            "hit_rate_ci_low": hit_ci_low,
            "hit_rate_ci_high": hit_ci_high,
            "sector_alpha_t_stat": _t_stat(independent["sector_net_alpha"]),
            "first_signal_time": (
                signal_times.min().isoformat() if not signal_times.empty else None
            ),
            "last_signal_time": (
                signal_times.max().isoformat() if not signal_times.empty else None
            ),
            "mean_net_return": mean_return,
            "net_return_t_stat": return_t,
            "net_return_p_value": _normal_p_value(return_t),
            "early_net_return": early_return,
            "late_net_return": late_return,
            "mean_net_alpha": mean_alpha,
            "mean_market_net_alpha": _mean(independent["market_net_alpha"]),
            "mean_sector_net_alpha": _mean(independent["sector_net_alpha"]),
            "primary_benchmark": _single_value(group["primary_benchmark"]),
            "alpha_t_stat": alpha_t,
            "alpha_p_value": _normal_p_value(alpha_t),
            "early_alpha": early_alpha,
            "late_alpha": late_alpha,
            "hit_rate": hit_rate,
            "mean_mae_pct": mean_mae,
            "mean_mfe_pct": mean_mfe,
            "stop_first_rate": stop_first_rate,
            "target_first_rate": target_first_rate,
            "stop_hit_rate": stop_hit_rate,
            "target_hit_rate": target_hit_rate,
            "raw_pass": raw_pass,
            "calibration": calibration,
        })

    p_values = pd.Series(
        [row["alpha_p_value"] for row in candidates], dtype=float
    )
    adjusted = _benjamini_hochberg(p_values)
    prepared = []
    for position, row in enumerate(candidates):
        key = row["key"]
        fdr_q = _finite_or_none(adjusted.iloc[position])
        state = (
            "ROBUST_PASS" if row["raw_pass"] and fdr_q is not None and fdr_q <= 0.05
            else "MONITOR_ONLY" if row["raw_pass"]
            else "UNRANKED"
        )
        calibration = row["calibration"]
        metrics = {
            "alpha_p_value": row["alpha_p_value"],
            "calibration_curve": calibration["calibration_curve"],
            "early_alpha": row["early_alpha"],
            "early_net_return": row["early_net_return"],
            "hit_rate": row["hit_rate"],
            "late_alpha": row["late_alpha"],
            "late_net_return": row["late_net_return"],
            "live_expected_alpha": calibration["live_expected_alpha"],
            "live_expected_alpha_ci_high": calibration["live_expected_alpha_ci_high"],
            "live_expected_alpha_ci_low": calibration["live_expected_alpha_ci_low"],
            "mean_mae_pct": row["mean_mae_pct"],
            "mean_mfe_pct": row["mean_mfe_pct"],
            "mean_market_net_alpha": row["mean_market_net_alpha"],
            "mean_net_return": row["mean_net_return"],
            "mean_sector_net_alpha": row["mean_sector_net_alpha"],
            "net_return_t_stat": row["net_return_t_stat"],
            "net_return_p_value": row["net_return_p_value"],
            "primary_benchmark": row["primary_benchmark"],
            "raw_pass": row["raw_pass"],
            "stop_first_rate": row["stop_first_rate"],
            "stop_hit_rate": row["stop_hit_rate"],
            "target_first_rate": row["target_first_rate"],
            "target_hit_rate": row["target_hit_rate"],
            "cohort_sha256": cohort_sha256,
            "distinct_tickers": row["distinct_tickers"],
            "top5_concentration": row["top5_concentration"],
            "hit_rate_ci_low": row["hit_rate_ci_low"],
            "hit_rate_ci_high": row["hit_rate_ci_high"],
            "sector_alpha_t_stat": row["sector_alpha_t_stat"],
            "first_signal_time": row["first_signal_time"],
            "last_signal_time": row["last_signal_time"],
            "minimum_events": minimum_events,
            "minimum_independent_periods": minimum_independent_periods,
            "publication_metadata": metadata,
            "qualification_metrics_version": "equity_qualification_metrics_v3",
            "research_scope": research_scope,
        }
        prepared.append({
            "key": [_json_scalar(value) for value in key],
            "source_name": str(key[0]),
            "source_version": str(key[1]),
            "interval": None if pd.isna(key[2]) else str(key[2]),
            "direction": None if pd.isna(key[3]) else int(key[3]),
            "horizon_key": str(key[4]),
            "outcome_policy_key": str(key[6]),
            "evaluation_version": evaluation_version,
            "qualification_state": state,
            "effective_from": effective_from.astimezone(timezone.utc),
            "sample_size": row["sample_size"],
            "independent_periods": row["independent_periods"],
            "mean_net_alpha": row["mean_net_alpha"],
            "alpha_t_stat": row["alpha_t_stat"],
            "alpha_fdr_q": fdr_q,
            "calibrated_probability": calibration["calibrated_win_probability"],
            "probability_ci_low": calibration["calibrated_win_probability_ci_low"],
            "probability_ci_high": calibration["calibrated_win_probability_ci_high"],
            "brier_score": calibration["brier_score"],
            "brier_skill_score": calibration["brier_skill_score_vs_50"],
            "expected_calibration_error": calibration["expected_calibration_error"],
            "metrics": metrics,
        })

    report_identity = _report_identity(
        prepared,
        effective_from=effective_from,
        evaluation_version=evaluation_version,
        minimum_events=minimum_events,
        minimum_independent_periods=minimum_independent_periods,
        publication_metadata=metadata,
        research_scope=research_scope,
    )
    return _prepared_revisions(prepared, report_identity)


def qualify_option_conditioning(
    observations: pd.DataFrame,
    *,
    effective_from: datetime,
    evaluation_version: str = "option_conditioning_v1",
    minimum_events: int = 100,
    minimum_independent_periods: int = 40,
    publication_metadata: Mapping[str, Any] | None = None,
) -> tuple[QualificationRevision, ...]:
    if effective_from.tzinfo is None or effective_from.utcoffset() is None:
        raise ValueError("effective_from must be timezone-aware")
    if observations.empty:
        return ()
    required = {
        "ticker", "source_name", "source_version", "interval", "direction",
        "horizon_key", "horizon_bars", "policy_key", "signal_time",
        "conditioned_return", "control_return",
    }
    missing = required - set(observations.columns)
    if missing:
        raise ValueError(
            f"option conditioning observations are missing: {sorted(missing)}"
        )
    metadata = dict(publication_metadata or {})
    frame = observations.copy()
    frame["signal_time"] = pd.to_datetime(frame["signal_time"], utc=True)
    for column in ("conditioned_return", "control_return"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["conditioned_return", "control_return"])
    if frame.empty:
        return ()
    frame["incremental_return"] = (
        frame["conditioned_return"] - frame["control_return"]
    )
    cohort_sha256 = _cohort_sha256(frame)
    grouping = [
        "source_name", "source_version", "interval", "direction",
        "horizon_key", "horizon_bars", "policy_key",
    ]
    candidates = []
    for key, group in frame.groupby(grouping, dropna=False, sort=True):
        portfolio = group.groupby("signal_time", as_index=False).agg(
            conditioned_return=("conditioned_return", "mean"),
            control_return=("control_return", "mean"),
            incremental_return=("incremental_return", "mean"),
            names=("ticker", "nunique"),
        )
        independent = _independent_periods(
            portfolio, int(key[5]),
            interval=None if pd.isna(key[2]) else str(key[2]),
        )
        periods = len(independent)
        midpoint = max(1, periods // 2)
        incremental = independent["incremental_return"]
        mean_incremental = _mean(incremental)
        incremental_t = _t_stat(incremental)
        early_incremental = _mean(incremental.iloc[:midpoint])
        late_incremental = _mean(incremental.iloc[midpoint:])
        raw_pass = (
            int(group["ticker"].count()) >= minimum_events
            and periods >= minimum_independent_periods
            and mean_incremental is not None and mean_incremental > 0
            and incremental_t is not None and incremental_t > 2
            and early_incremental is not None and early_incremental > 0
            and late_incremental is not None and late_incremental > 0
        )
        calibration = walk_forward_calibration(
            independent["conditioned_return"], incremental
        )
        candidates.append({
            "key": key,
            "sample_size": int(group["ticker"].count()),
            "independent_periods": periods,
            "conditioned_mean_return": _mean(independent["conditioned_return"]),
            "control_mean_return": _mean(independent["control_return"]),
            "incremental_mean_return": mean_incremental,
            "incremental_t_stat": incremental_t,
            "incremental_p_value": _normal_p_value(incremental_t),
            "early_incremental_return": early_incremental,
            "late_incremental_return": late_incremental,
            "raw_pass": raw_pass,
            "calibration": calibration,
        })
    adjusted = _benjamini_hochberg(pd.Series(
        [row["incremental_p_value"] for row in candidates], dtype=float
    ))
    prepared = []
    for position, row in enumerate(candidates):
        key = row["key"]
        fdr_q = _finite_or_none(adjusted.iloc[position])
        state = (
            "ROBUST_PASS"
            if row["raw_pass"] and fdr_q is not None and fdr_q <= 0.05
            else "MONITOR_ONLY" if row["raw_pass"]
            else "UNRANKED"
        )
        calibration = row["calibration"]
        option_metrics = {
            "conditioned_mean_return": row["conditioned_mean_return"],
            "control_mean_return": row["control_mean_return"],
            "early_incremental_return": row["early_incremental_return"],
            "incremental_mean_return": row["incremental_mean_return"],
            "incremental_p_value": row["incremental_p_value"],
            "late_incremental_return": row["late_incremental_return"],
        }
        metrics = {
            "calibration_curve": calibration["calibration_curve"],
            "cohort_sha256": cohort_sha256,
            "minimum_events": minimum_events,
            "minimum_independent_periods": minimum_independent_periods,
            "option_conditioning": option_metrics,
            "publication_metadata": metadata,
            "qualification_metrics_version": "option_conditioning_metrics_v1",
            "raw_pass": row["raw_pass"],
            "research_scope": "OPTION_CONDITIONING",
        }
        prepared.append({
            "key": [_json_scalar(value) for value in key],
            "source_name": str(key[0]),
            "source_version": str(key[1]),
            "interval": None if pd.isna(key[2]) else str(key[2]),
            "direction": None if pd.isna(key[3]) else int(key[3]),
            "horizon_key": str(key[4]),
            "outcome_policy_key": str(key[6]),
            "evaluation_version": evaluation_version,
            "qualification_state": state,
            "effective_from": effective_from.astimezone(timezone.utc),
            "sample_size": row["sample_size"],
            "independent_periods": row["independent_periods"],
            "mean_net_alpha": row["incremental_mean_return"],
            "alpha_t_stat": row["incremental_t_stat"],
            "alpha_fdr_q": fdr_q,
            "calibrated_probability": calibration["calibrated_win_probability"],
            "probability_ci_low": calibration["calibrated_win_probability_ci_low"],
            "probability_ci_high": calibration["calibrated_win_probability_ci_high"],
            "brier_score": calibration["brier_score"],
            "brier_skill_score": calibration["brier_skill_score_vs_50"],
            "expected_calibration_error": calibration["expected_calibration_error"],
            "metrics": metrics,
        })
    report_identity = _report_identity(
        prepared,
        effective_from=effective_from,
        evaluation_version=evaluation_version,
        minimum_events=minimum_events,
        minimum_independent_periods=minimum_independent_periods,
        publication_metadata=metadata,
        research_scope="OPTION_CONDITIONING",
    )
    return _prepared_revisions(prepared, report_identity)


def _cohort_sha256(frame: pd.DataFrame) -> str:
    identity_columns = [
        column for column in (
            "subject_evidence_id", "outcome_id", "ticker", "source_name",
            "source_version", "interval", "direction", "horizon_key",
            "horizon_bars", "policy_key", "signal_time", "net_return",
            "net_alpha", "mae_pct", "mfe_pct", "first_hit", "stop_hit",
            "target_hit", "has_bracket", "conditioned_return",
            "control_return", "incremental_return", "sector_net_alpha",
            "primary_benchmark",
        )
        if column in frame.columns
    ]
    sort_columns = [
        column for column in (
            "source_name", "source_version", "interval", "direction",
            "horizon_key", "signal_time", "ticker", "subject_evidence_id",
            "outcome_id",
        )
        if column in frame.columns
    ]
    digest = hashlib.sha256()
    ordered = frame.sort_values(sort_columns, kind="stable")
    for values in ordered[identity_columns].itertuples(index=False, name=None):
        record = {
            column: _identity_scalar(value)
            for column, value in zip(identity_columns, values)
        }
        digest.update(canonical_json(record).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _report_identity(
    prepared: list[dict[str, Any]],
    *,
    effective_from: datetime,
    evaluation_version: str,
    minimum_events: int,
    minimum_independent_periods: int,
    publication_metadata: Mapping[str, Any],
    research_scope: str,
) -> str:
    return sha256_json({
        "cells": [
            {
                key: _identity_scalar(value)
                for key, value in row.items()
                if key not in ("effective_from", "key")
            } | {"key": row["key"]}
            for row in prepared
        ],
        "effective_from": effective_from.astimezone(timezone.utc).isoformat(),
        "evaluation_version": evaluation_version,
        "minimum_events": minimum_events,
        "minimum_independent_periods": minimum_independent_periods,
        "publication_metadata": dict(publication_metadata),
        "research_scope": research_scope,
    })


def _prepared_revisions(
    prepared: list[dict[str, Any]],
    report_identity: str,
) -> tuple[QualificationRevision, ...]:
    revisions = []
    for row in prepared:
        identity = sha256_json({
            "key": row["key"],
            "report_identity": report_identity,
        })
        revisions.append(QualificationRevision(
            qualification_revision_id=uuid5(
                NAMESPACE_URL, f"equity-qualification:{identity}"
            ),
            source_name=row["source_name"],
            source_version=row["source_version"],
            interval=row["interval"],
            direction=row["direction"],
            horizon_key=row["horizon_key"],
            outcome_policy_key=row["outcome_policy_key"],
            evaluation_version=row["evaluation_version"],
            qualification_state=row["qualification_state"],
            effective_from=row["effective_from"],
            sample_size=row["sample_size"],
            independent_periods=row["independent_periods"],
            mean_net_alpha=row["mean_net_alpha"],
            alpha_t_stat=row["alpha_t_stat"],
            alpha_fdr_q=row["alpha_fdr_q"],
            calibrated_probability=row["calibrated_probability"],
            probability_ci_low=row["probability_ci_low"],
            probability_ci_high=row["probability_ci_high"],
            brier_score=row["brier_score"],
            brier_skill_score=row["brier_skill_score"],
            expected_calibration_error=row["expected_calibration_error"],
            report_identity=report_identity,
            metrics_json=canonical_json(row["metrics"]),
        ))
    return tuple(revisions)


def _identity_scalar(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _independent_periods(
    portfolio: pd.DataFrame,
    horizon_bars: int,
    *,
    interval: str | None = None,
) -> pd.DataFrame:
    ordered = portfolio.sort_values("signal_time").reset_index(drop=True)
    if ordered.empty:
        return ordered
    if interval == "1d":
        signal_dates = pd.to_datetime(ordered["signal_time"], utc=True).dt.date
        calendar = exchange_calendars.get_calendar("XNYS")
        sessions = calendar.sessions_in_range(
            pd.Timestamp(signal_dates.min()), pd.Timestamp(signal_dates.max())
        )
        session_ordinals = {
            pd.Timestamp(session).date(): ordinal
            for ordinal, session in enumerate(sessions)
        }
        try:
            ordinals = [session_ordinals[value] for value in signal_dates]
        except KeyError as exc:
            raise ValueError("daily signal_time must fall on an XNYS session") from exc
    elif interval in ("5m", "15m", "30m", "1h"):
        ordinals = _intraday_bar_ordinals(ordered["signal_time"], interval)
    else:
        unique_times = {
            timestamp: ordinal
            for ordinal, timestamp in enumerate(sorted(ordered["signal_time"].unique()))
        }
        ordinals = [unique_times[value] for value in ordered["signal_time"]]
    selected = []
    last_ordinal = -horizon_bars
    for index, ordinal in enumerate(ordinals):
        if ordinal - last_ordinal >= horizon_bars:
            selected.append(index)
            last_ordinal = ordinal
    return ordered.loc[selected].reset_index(drop=True)


def _intraday_bar_ordinals(
    signal_times: pd.Series,
    interval: str,
) -> list[int]:
    interval_minutes = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}
    minutes = interval_minutes[interval]
    timestamps = pd.to_datetime(signal_times, utc=True)
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = calendar.sessions_in_range(
        pd.Timestamp(timestamps.min().date()),
        pd.Timestamp(timestamps.max().date()),
    )
    bar_ordinals = {}
    ordinal = 0
    for session in sessions:
        session_open = calendar.session_open(session)
        session_close = calendar.session_close(session)
        boundary = session_open + pd.Timedelta(minutes=minutes)
        while boundary < session_close:
            bar_ordinals[boundary] = ordinal
            ordinal += 1
            boundary += pd.Timedelta(minutes=minutes)
        bar_ordinals[session_close] = ordinal
        ordinal += 1
    try:
        return [bar_ordinals[timestamp] for timestamp in timestamps]
    except KeyError as exc:
        raise ValueError(
            f"{interval} signal_time must equal an XNYS bar boundary"
        ) from exc


def _wilson_interval(
    successes: int, trials: int, z: float = 1.959963984540054
) -> tuple[float | None, float | None]:
    """Wilson score interval; Wald degenerates at the small period counts here."""
    if trials <= 0:
        return None, None
    proportion = successes / trials
    denominator = 1 + z * z / trials
    centre = (proportion + z * z / (2 * trials)) / denominator
    margin = (
        z * math.sqrt(proportion * (1 - proportion) / trials
                      + z * z / (4 * trials * trials)) / denominator
    )
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _concentration(tickers: pd.Series, top: int = 5) -> float | None:
    counts = tickers.value_counts()
    total = int(counts.sum())
    if total <= 0:
        return None
    return float(counts.iloc[:top].sum() / total)


def _t_stat(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    standard_deviation = float(numeric.std(ddof=1))
    if not math.isfinite(standard_deviation) or standard_deviation <= 0:
        return None
    return float(numeric.mean() / (standard_deviation / np.sqrt(len(numeric))))


def _normal_p_value(t_stat: float | None) -> float | None:
    if t_stat is None or not math.isfinite(t_stat):
        return None
    return float(2 * (1 - NormalDist().cdf(abs(t_stat))))


def _mean(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.mean()) if len(numeric) else None


def _finite_or_none(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _json_scalar(value):
    if pd.isna(value):
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return value


def _single_value(values: pd.Series) -> Any:
    unique = tuple(pd.unique(values.dropna()))
    if len(unique) != 1:
        raise ValueError("qualification group mixes benchmark policies")
    return _identity_scalar(unique[0])