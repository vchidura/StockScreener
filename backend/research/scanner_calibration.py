"""Walk-forward reliability diagnostics for independent scanner periods."""
from __future__ import annotations

import numpy as np
import pandas as pd


PRIOR_STRENGTH = 20.0
MIN_TRAIN_PERIODS = 40


def walk_forward_calibration(
    net_returns: pd.Series,
    net_alpha: pd.Series,
    min_train_periods: int = MIN_TRAIN_PERIODS,
    *,
    prediction_times: pd.Series | None = None,
    outcome_available_at: pd.Series | None = None,
) -> dict:
    """Use strictly earlier available labels, or legacy preordered independent periods."""
    if (prediction_times is None) != (outcome_available_at is None):
        raise ValueError("prediction_times and outcome_available_at must be supplied together")
    frame = pd.DataFrame({
        "net_return": pd.to_numeric(net_returns, errors="coerce"),
        "net_alpha": pd.to_numeric(net_alpha, errors="coerce"),
    })
    if prediction_times is not None:
        if not all(
            values.index.equals(net_returns.index)
            for values in (net_alpha, prediction_times, outcome_available_at)
        ):
            raise ValueError("calibration returns and timestamps must have matching indexes")
        frame["prediction_time"] = _utc_timestamps(prediction_times, "prediction_times")
        frame["outcome_available_at"] = _utc_timestamps(
            outcome_available_at, "outcome_available_at"
        )
        if (frame["outcome_available_at"] <= frame["prediction_time"]).any():
            raise ValueError("outcome_available_at must be after its prediction_time")
    frame = frame.dropna()
    periods = len(frame)
    outcomes = (frame["net_return"].to_numpy(dtype=float) > 0).astype(float)
    if prediction_times is not None:
        availability = frame["outcome_available_at"].array.asi8
        available_order = np.argsort(availability, kind="stable")
        known_counts = np.searchsorted(
            availability[available_order], frame["prediction_time"].array.asi8,
            side="left",
        )
        cumulative_wins = np.r_[0.0, np.cumsum(outcomes[available_order])]
    else:
        known_counts = np.arange(periods)
        cumulative_wins = np.r_[0.0, np.cumsum(outcomes)]
    eligible = known_counts >= min_train_periods
    if not eligible.any():
        return {
            "calibration_oos_periods": 0,
            "calibrated_win_probability": None,
            "calibrated_win_probability_ci_low": None,
            "calibrated_win_probability_ci_high": None,
            "brier_score": None,
            "brier_skill_score_vs_50": None,
            "expected_calibration_error": None,
            "calibration_curve": [],
            "live_expected_alpha": None,
            "live_expected_alpha_ci_low": None,
            "live_expected_alpha_ci_high": None,
        }

    prior_wins = PRIOR_STRENGTH / 2.0
    predicted = (
        (cumulative_wins[known_counts[eligible]] + prior_wins)
        / (known_counts[eligible] + PRIOR_STRENGTH)
    )
    observed = outcomes[eligible]
    brier_score = float(np.mean((predicted - observed) ** 2))
    curve_frame = pd.DataFrame({"predicted": predicted, "observed": observed})
    unique_predictions = int(curve_frame["predicted"].nunique())
    bins = min(5, unique_predictions)
    if bins > 1:
        curve_frame["bin"] = pd.qcut(
            curve_frame["predicted"], bins, duplicates="drop"
        )
    else:
        curve_frame["bin"] = "all"
    curve = []
    for _, group in curve_frame.groupby("bin", observed=True, sort=True):
        if group.empty:
            continue
        curve.append({
            "count": int(len(group)),
            "mean_predicted": float(group["predicted"].mean()),
            "observed_frequency": float(group["observed"].mean()),
            "minimum_prediction": float(group["predicted"].min()),
            "maximum_prediction": float(group["predicted"].max()),
        })
    calibration_error = float(sum(
        point["count"] / len(curve_frame)
        * abs(point["mean_predicted"] - point["observed_frequency"])
        for point in curve
    ))

    total_wins = float(outcomes.sum())
    posterior_alpha = total_wins + prior_wins
    posterior_beta = periods - total_wins + prior_wins
    posterior_total = posterior_alpha + posterior_beta
    win_probability = posterior_alpha / posterior_total
    posterior_standard_error = np.sqrt(
        posterior_alpha * posterior_beta
        / (posterior_total ** 2 * (posterior_total + 1.0))
    )
    alpha = frame["net_alpha"]
    alpha_standard_error = (
        float(alpha.std(ddof=1) / np.sqrt(periods))
        if periods > 1 and alpha.std(ddof=1) > 0 else None
    )
    mean_alpha = float(alpha.mean())
    return {
        "calibration_oos_periods": int(len(observed)),
        "calibrated_win_probability": float(win_probability),
        "calibrated_win_probability_ci_low": float(max(
            0.0, win_probability - 1.96 * posterior_standard_error
        )),
        "calibrated_win_probability_ci_high": float(min(
            1.0, win_probability + 1.96 * posterior_standard_error
        )),
        "brier_score": brier_score,
        "brier_skill_score_vs_50": float(1.0 - brier_score / 0.25),
        "expected_calibration_error": calibration_error,
        "calibration_curve": curve,
        "live_expected_alpha": mean_alpha,
        "live_expected_alpha_ci_low": (
            mean_alpha - 1.96 * alpha_standard_error
            if alpha_standard_error is not None else None
        ),
        "live_expected_alpha_ci_high": (
            mean_alpha + 1.96 * alpha_standard_error
            if alpha_standard_error is not None else None
        ),
    }


def _utc_timestamps(values: pd.Series, name: str) -> pd.Series:
    if values.isna().any():
        raise ValueError(f"{name} must contain known timezone-aware timestamps")
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        return values.astype("datetime64[ns, UTC]")
    timestamps = [pd.Timestamp(value) for value in values]
    if any(pd.isna(value) or value.tzinfo is None for value in timestamps):
        raise ValueError(f"{name} must contain known timezone-aware timestamps")
    return pd.Series(
        pd.to_datetime(timestamps, utc=True), index=values.index,
        dtype="datetime64[ns, UTC]",
    )