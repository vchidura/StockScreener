from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class IvContextStatistics:
    current_comparable_iv: float | None
    lookback_start_date: date | None
    lookback_end_date: date | None
    sample_count: int
    coverage_fraction: float
    minimum_iv: float | None
    maximum_iv: float | None
    range_position_rank: float | None
    empirical_percentile: float | None
    null_reason_codes: tuple[str, ...]


def summarize_iv_context(
    current_iv: float | None,
    history: tuple[tuple[date, float], ...],
    *,
    lookback_sessions: int,
    minimum_sample_sessions: int,
    minimum_coverage_fraction: float,
    expected_sessions: tuple[date, ...] | None = None,
) -> IvContextStatistics:
    if lookback_sessions <= 0 or minimum_sample_sessions <= 0:
        raise ValueError("IV context session requirements must be positive")
    if not 0 < minimum_coverage_fraction <= 1:
        raise ValueError("minimum coverage fraction must be in (0, 1]")
    if expected_sessions is not None:
        if len(expected_sessions) != lookback_sessions:
            raise ValueError("expected sessions must match the lookback length")
        if tuple(sorted(set(expected_sessions))) != expected_sessions:
            raise ValueError("expected sessions must be unique and ordered")
        expected = frozenset(expected_sessions)
        history = tuple(row for row in history if row[0] in expected)
    valid = tuple(
        (session, value) for session, value in history
        if value > 0 and math.isfinite(value)
    )[-lookback_sessions:]
    values = [value for _, value in valid]
    sample_count = len(values)
    coverage = min(sample_count / lookback_sessions, 1.0)
    reasons = []
    if current_iv is None or current_iv <= 0 or not math.isfinite(current_iv):
        reasons.append("CURRENT_COMPARABLE_IV_UNAVAILABLE")
    if sample_count < minimum_sample_sessions:
        reasons.append("INSUFFICIENT_IV_SAMPLE_SESSIONS")
    if coverage < minimum_coverage_fraction:
        reasons.append("INSUFFICIENT_IV_COVERAGE")
    minimum = min(values) if values else None
    maximum = max(values) if values else None
    range_rank = None
    percentile = None
    if not reasons and current_iv is not None and minimum is not None and maximum is not None:
        range_rank = (
            0.5 if math.isclose(maximum, minimum)
            else min(max((current_iv - minimum) / (maximum - minimum), 0.0), 1.0)
        )
        percentile = sum(value <= current_iv for value in values) / sample_count
    return IvContextStatistics(
        current_comparable_iv=current_iv,
        lookback_start_date=(
            expected_sessions[0]
            if expected_sessions is not None
            else (valid[0][0] if valid else None)
        ),
        lookback_end_date=(
            expected_sessions[-1]
            if expected_sessions is not None
            else (valid[-1][0] if valid else None)
        ),
        sample_count=sample_count,
        coverage_fraction=coverage,
        minimum_iv=minimum,
        maximum_iv=maximum,
        range_position_rank=range_rank,
        empirical_percentile=percentile,
        null_reason_codes=tuple(reasons),
    )