"""Versioned, fail-closed classification of Polygon/Massive option trades."""
from __future__ import annotations

from dataclasses import dataclass

from options.domain import TradeClassificationStatus

TRADE_SEMANTICS_VERSION = "massive_options_conditions_v1"

CANCELED_CONDITIONS = frozenset({201, 203, 205, 207})
EXCLUDED_CONDITIONS = frozenset({202, 204, 206})
INCLUDED_CONDITIONS = frozenset({
    208, 209, 210, 219,
    227, 228, 229, 230, 231, 232, 233, 234, 235, 236,
    237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248,
})
KNOWN_CONDITIONS = CANCELED_CONDITIONS | EXCLUDED_CONDITIONS | INCLUDED_CONDITIONS


@dataclass(frozen=True, slots=True)
class OptionTradeClassification:
    status: TradeClassificationStatus
    reasons: tuple[str, ...]
    semantics_version: str = TRADE_SEMANTICS_VERSION


def classify_option_trade(
    conditions: tuple[int, ...], correction: int | None
) -> OptionTradeClassification:
    """Classify aggregation eligibility without inferring buyer/seller direction."""
    if correction not in (None, 0):
        return OptionTradeClassification(
            TradeClassificationStatus.UNKNOWN,
            ("CORRECTION_SEMANTICS_UNAVAILABLE",),
        )
    if not conditions:
        return OptionTradeClassification(
            TradeClassificationStatus.UNKNOWN,
            ("TRADE_CONDITION_MISSING",),
        )
    unknown = sorted(set(conditions) - KNOWN_CONDITIONS)
    if unknown:
        return OptionTradeClassification(
            TradeClassificationStatus.UNKNOWN,
            tuple(f"UNKNOWN_TRADE_CONDITION_{code}" for code in unknown),
        )
    if set(conditions) & CANCELED_CONDITIONS:
        return OptionTradeClassification(
            TradeClassificationStatus.CANCELED,
            ("PROVIDER_CANCELED_CONDITION",),
        )
    if set(conditions) & EXCLUDED_CONDITIONS:
        return OptionTradeClassification(
            TradeClassificationStatus.EXCLUDED,
            ("PROVIDER_NON_VOLUME_CONDITION",),
        )
    return OptionTradeClassification(
        TradeClassificationStatus.INCLUDED,
        ("PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE", "AGGRESSOR_SIDE_UNAVAILABLE"),
    )
