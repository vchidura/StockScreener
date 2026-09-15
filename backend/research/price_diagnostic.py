"""Frozen causal scoring retained for the enrolled paper observer; no fitting."""

import numpy as np

def score_causal_ridge(group, model):
    if group.empty or set(group["session"]) != {model["session"]} or not group["feature_ready"].all():
        raise ValueError("ridge prediction requires one decision's unchanged feature-ready universe")
    values = group[model["features"]].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("feature-ready candidate has invalid ridge features")
    scores = ((values - np.array(model["feature_mean"])) / np.array(model["feature_scale"])) @ np.array(model["coefficients"]) + model["label_mean"]
    if not np.isfinite(scores).all():
        raise ValueError("ridge prediction produced invalid scores")
    return group.assign(ridge_score=scores)
