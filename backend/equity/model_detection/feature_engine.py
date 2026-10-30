"""Pure point-in-time reference features with explicit readiness reasons."""
from datetime import timedelta
from decimal import Context, Decimal, localcontext
from typing import Protocol

from .contracts import CanonicalInput, FeatureSnapshot, Readiness
from .feature_definitions import FEATURE_DEFINITIONS, FEATURE_VERSION
from .policies import BreakoutPolicy


class FeatureCalculator(Protocol):
    def __call__(self, packet: CanonicalInput, policy: BreakoutPolicy) -> FeatureSnapshot: ...


def compute_features(packet: CanonicalInput, policy: BreakoutPolicy) -> FeatureSnapshot:
    reasons: list[str] = []
    values: dict[str, Decimal | bool | None] = {item.name: None for item in FEATURE_DEFINITIONS}
    selected = packet.bars[-(policy.lookback + 1):]
    if len(selected) < policy.lookback + 1:
        reasons.append("INSUFFICIENT_COMPLETED_BARS")
    if any(bar.interval != policy.interval for bar in packet.bars):
        reasons.append("INTERVAL_MISMATCH")
    duration = {"1m": 60, "5m": 300, "30m": 1800, "1h": 3600, "1d": 86400}[policy.interval]
    # Calendar/session-aware daily continuity is intentionally a future policy concern.
    if policy.interval != "1d" and any((bar.clocks.market_end - bar.clocks.market_start).total_seconds() != duration for bar in selected):
        reasons.append("BAR_DURATION_MISMATCH")
    if any(left.clocks.market_end != right.clocks.market_start for left, right in zip(selected, selected[1:])):
        reasons.append("NONCONTIGUOUS_WINDOW")
    observations = (*selected, *packet.events, packet.context)
    if any(item.clocks.received_at > packet.decision_at or item.clocks.available_at > packet.decision_at for item in observations):
        reasons.append("INPUT_NOT_YET_AVAILABLE")
    if selected and packet.market_cutoff - selected[-1].clocks.market_end > timedelta(seconds=policy.max_age_seconds):
        reasons.append("STALE_COMPLETED_BAR")
    if packet.context.market_trend is None:
        reasons.append("MISSING_MARKET_CONTEXT")
    if packet.context.sector_trend is None:
        reasons.append("MISSING_SECTOR_CONTEXT")
    if packet.context.event_risk is None:
        reasons.append("MISSING_EVENT_CONTEXT")
    # Do not compute even partial values from late or incomplete observations.
    if not reasons:
        history, latest = selected[:-1], selected[-1]
        with localcontext(Context(prec=34)):
            baseline = sum((bar.volume for bar in history), Decimal(0)) / Decimal(policy.lookback)
            if baseline <= 0:
                reasons.append("ZERO_VOLUME_BASELINE")
            else:
                values.update(range_high=max(bar.high for bar in history), range_low=min(bar.low for bar in history),
                              close=latest.close, volume_multiple=latest.volume / baseline,
                              market_trend=packet.context.market_trend, sector_trend=packet.context.sector_trend,
                              event_risk=packet.context.event_risk)
    return FeatureSnapshot(FEATURE_VERSION, packet.observation_sha256, values,
                           Readiness.UNAVAILABLE if reasons else Readiness.READY, tuple(reasons))
