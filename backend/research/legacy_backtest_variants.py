"""Variant configuration catalog for legacy equity backtest continuation."""
from __future__ import annotations

import copy
from typing import Any


def _clone(config: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(config)


def _apply_label(config: dict[str, Any], label: str) -> dict[str, Any]:
    config["variant_policy_version"] = label
    config["classification"] = f"LEGACY_AUTONOMOUS_BACKTEST::{label}"
    return config


def control_config(base: dict[str, Any]) -> dict[str, Any]:
    return _apply_label(_clone(base), "legacy_controls_v1")


def r2_ablations(base: dict[str, Any]) -> list[dict[str, Any]]:
    first = _apply_label(_clone(base), "r2_ablation_rs75_v1")
    first["models"]["resumption"]["rs_percentile_threshold"] = 0.75
    return [first]


def b3_ablations(base: dict[str, Any]) -> list[dict[str, Any]]:
    first = _apply_label(_clone(base), "b3_ablation_no_chase_v1")
    first["models"]["acceptance"]["chase_atr"] = 0.0
    return [first]


def f2_ablations(base: dict[str, Any]) -> list[dict[str, Any]]:
    first = _apply_label(_clone(base), "f2_ablation_deadline2_v1")
    first["models"]["failure"]["deadline_bars"] = 2
    return [first]


def sw3_structural_controls(base: dict[str, Any]) -> list[dict[str, Any]]:
    first = _apply_label(_clone(base), "sw3_structural_hourly_context_v1")
    first["models"]["resumption"]["time_cap_minutes"] = 300
    first["models"]["acceptance"]["time_cap_minutes"] = 180
    first["models"]["failure"]["time_cap_minutes"] = 90
    return [first]


def lane_variants(base: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    variants: list[tuple[str, str, dict[str, Any]]] = [("controls_r2_b3_f2", "control", control_config(base))]
    variants.extend(("controls_r2_b3_f2", "r2_ablation", item) for item in r2_ablations(base))
    variants.extend(("controls_r2_b3_f2", "b3_ablation", item) for item in b3_ablations(base))
    variants.extend(("controls_r2_b3_f2", "f2_ablation", item) for item in f2_ablations(base))
    variants.extend(("sw3_r_b_f", "sw3_structural_control", item) for item in sw3_structural_controls(base))
    return variants
