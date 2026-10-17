from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .contracts import canonical_sha256
from .equity_pipeline_config import AdvancedScaleControl


class AdvancedScaleAction(str, Enum):
    UP = "UP"
    HOLD = "HOLD"
    DOWN = "DOWN"


@dataclass(frozen=True, slots=True)
class AdvancedScaleThresholds:
    maximum_unresolved_gaps: int
    maximum_quarantines: int
    maximum_duplicate_bars: int
    maximum_release_failures: int
    maximum_p95_lag_ms: int
    maximum_p99_lag_ms: int
    maximum_buffer_utilization: float
    minimum_memory_headroom_percent: int
    minimum_database_headroom_percent: int

    def __post_init__(self) -> None:
        nonnegative = (
            self.maximum_unresolved_gaps,
            self.maximum_quarantines,
            self.maximum_duplicate_bars,
            self.maximum_release_failures,
            self.maximum_p95_lag_ms,
            self.maximum_p99_lag_ms,
        )
        if (
            any(type(value) is not int or value < 0 for value in nonnegative)
            or not 0 < self.maximum_buffer_utilization < 1
            or not 1 <= self.minimum_memory_headroom_percent <= 100
            or not 1 <= self.minimum_database_headroom_percent <= 100
            or self.maximum_p95_lag_ms > self.maximum_p99_lag_ms
        ):
            raise ValueError("Advanced scale thresholds are invalid")


@dataclass(frozen=True, slots=True)
class AdvancedScalePolicy:
    version: str
    scale_control: AdvancedScaleControl
    universe_ladder: tuple[int, ...]
    stable_sessions_before_up: int
    thresholds: AdvancedScaleThresholds

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedScaleMetrics:
    metrics_complete: bool
    current_target: int
    stable_sessions: int
    unresolved_gaps: int
    quarantines: int
    duplicate_bars: int
    release_failures: int
    p95_lag_ms: int
    p99_lag_ms: int
    maximum_buffer_utilization: float
    memory_headroom_percent: int
    database_headroom_percent: int

    def __post_init__(self) -> None:
        if (
            type(self.metrics_complete) is not bool
            or type(self.current_target) is not int
            or self.current_target <= 0
            or type(self.stable_sessions) is not int
            or self.stable_sessions < 0
        ):
            raise ValueError("Advanced scale metric identity is invalid")


@dataclass(frozen=True, slots=True)
class AdvancedScaleRecommendation:
    action: AdvancedScaleAction
    current_target: int
    desired_target: int
    proposed_target: int
    reason_codes: tuple[str, ...]
    policy_sha256: str
    requires_operator_approval: bool = True


_POLICY_KEYS = frozenset({
    "version",
    "scale_control",
    "universe_ladder",
    "stable_sessions_before_up",
    "thresholds",
})
_THRESHOLD_KEYS = frozenset({
    "maximum_unresolved_gaps",
    "maximum_quarantines",
    "maximum_duplicate_bars",
    "maximum_release_failures",
    "maximum_p95_lag_ms",
    "maximum_p99_lag_ms",
    "maximum_buffer_utilization",
    "minimum_memory_headroom_percent",
    "minimum_database_headroom_percent",
})


def load_advanced_scale_policy(path: Path) -> AdvancedScalePolicy:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or frozenset(value) != _POLICY_KEYS:
        raise ValueError("Advanced scale policy keys changed")
    thresholds = value["thresholds"]
    if (
        not isinstance(thresholds, dict)
        or frozenset(thresholds) != _THRESHOLD_KEYS
    ):
        raise ValueError("Advanced scale threshold keys changed")
    ladder = tuple(int(item) for item in value["universe_ladder"])
    if (
        not ladder
        or ladder[0] != 1
        or ladder != tuple(sorted(set(ladder)))
        or ladder[-1] > 400
    ):
        raise ValueError("Advanced scale ladder is invalid")
    stable_sessions = int(value["stable_sessions_before_up"])
    if stable_sessions <= 0:
        raise ValueError("Advanced scale stability gate must be positive")
    return AdvancedScalePolicy(
        version=str(value["version"]),
        scale_control=AdvancedScaleControl(str(value["scale_control"])),
        universe_ladder=ladder,
        stable_sessions_before_up=stable_sessions,
        thresholds=AdvancedScaleThresholds(**thresholds),
    )


def recommend_advanced_scale(
    policy: AdvancedScalePolicy,
    metrics: AdvancedScaleMetrics,
    *,
    desired_target: int,
) -> AdvancedScaleRecommendation:
    if (
        metrics.current_target not in policy.universe_ladder
        or desired_target not in policy.universe_ladder
    ):
        raise ValueError("Advanced scale target is outside the approved ladder")
    thresholds = policy.thresholds
    failures = []
    if not metrics.metrics_complete:
        failures.append("METRICS_INCOMPLETE")
    checks = (
        (
            metrics.unresolved_gaps
            > thresholds.maximum_unresolved_gaps,
            "UNRESOLVED_GAPS",
        ),
        (
            metrics.quarantines > thresholds.maximum_quarantines,
            "QUARANTINED_EVENTS",
        ),
        (
            metrics.duplicate_bars > thresholds.maximum_duplicate_bars,
            "DUPLICATE_BARS",
        ),
        (
            metrics.release_failures
            > thresholds.maximum_release_failures,
            "RELEASE_FAILURES",
        ),
        (
            metrics.p95_lag_ms > thresholds.maximum_p95_lag_ms,
            "P95_LAG_EXCEEDED",
        ),
        (
            metrics.p99_lag_ms > thresholds.maximum_p99_lag_ms,
            "P99_LAG_EXCEEDED",
        ),
        (
            metrics.maximum_buffer_utilization
            > thresholds.maximum_buffer_utilization,
            "BUFFER_UTILIZATION_EXCEEDED",
        ),
        (
            metrics.memory_headroom_percent
            < thresholds.minimum_memory_headroom_percent,
            "MEMORY_HEADROOM_LOW",
        ),
        (
            metrics.database_headroom_percent
            < thresholds.minimum_database_headroom_percent,
            "DATABASE_HEADROOM_LOW",
        ),
    )
    failures.extend(code for failed, code in checks if failed)
    index = policy.universe_ladder.index(metrics.current_target)
    if failures:
        proposed = (
            policy.universe_ladder[index - 1]
            if index > 0
            else metrics.current_target
        )
        return AdvancedScaleRecommendation(
            AdvancedScaleAction.DOWN if index > 0 else AdvancedScaleAction.HOLD,
            metrics.current_target,
            desired_target,
            proposed,
            tuple(failures),
            policy.sha256,
        )
    if metrics.current_target > desired_target:
        return AdvancedScaleRecommendation(
            AdvancedScaleAction.DOWN,
            metrics.current_target,
            desired_target,
            policy.universe_ladder[index - 1],
            ("TARGET_REDUCED",),
            policy.sha256,
        )
    if metrics.current_target < desired_target:
        if metrics.stable_sessions < policy.stable_sessions_before_up:
            return AdvancedScaleRecommendation(
                AdvancedScaleAction.HOLD,
                metrics.current_target,
                desired_target,
                metrics.current_target,
                ("STABILITY_GATE_NOT_MET",),
                policy.sha256,
            )
        return AdvancedScaleRecommendation(
            AdvancedScaleAction.UP,
            metrics.current_target,
            desired_target,
            min(policy.universe_ladder[index + 1], desired_target),
            ("STABILITY_GATE_MET",),
            policy.sha256,
        )
    return AdvancedScaleRecommendation(
        AdvancedScaleAction.HOLD,
        metrics.current_target,
        desired_target,
        metrics.current_target,
        ("AT_DESIRED_TARGET",),
        policy.sha256,
    )
