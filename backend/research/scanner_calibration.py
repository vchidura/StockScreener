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
) -> dict:
    """Evaluate expanding base-rate forecasts without using future outcomes."""
    frame = pd.DataFrame({
        "net_return": pd.to_numeric(net_returns, errors="coerce"),
        "net_alpha": pd.to_numeric(net_alpha, errors="coerce"),
    }).dropna()
    periods = len(frame)
    if periods <= min_train_periods:
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

    outcomes = (frame["net_return"].to_numpy(dtype=float) > 0).astype(float)
    predictions = []
    realized = []
    prior_wins = PRIOR_STRENGTH / 2.0
    for position in range(min_train_periods, periods):
        wins = float(outcomes[:position].sum())
        predictions.append((wins + prior_wins) / (position + PRIOR_STRENGTH))
        realized.append(outcomes[position])

    predicted = np.asarray(predictions, dtype=float)
    observed = np.asarray(realized, dtype=float)
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