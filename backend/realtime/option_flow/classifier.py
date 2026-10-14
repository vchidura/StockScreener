from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import NamedTuple

from .conditions import ConditionVerdict
from .quote_cache import QuoteLookup, QuoteView

AGGRESSION_VERSION = "aggression_v1"


class SideClass(str, Enum):
    """Values equal the database labels."""

    AT_OR_ABOVE_ASK = "AT_OR_ABOVE_ASK"
    AT_OR_BELOW_BID = "AT_OR_BELOW_BID"
    INSIDE_SPREAD = "INSIDE_SPREAD"
    QUOTE_CROSSED_OR_LOCKED = "QUOTE_CROSSED_OR_LOCKED"
    QUOTE_STALE = "QUOTE_STALE"
    NO_QUOTE = "NO_QUOTE"
    # The contract is not on the quote list, so no quote can ever exist for it.
    AGGRESSION_UNAVAILABLE_NO_QUOTE = "AGGRESSION_UNAVAILABLE_NO_QUOTE"
    EXCLUDED_COMPLEX = "EXCLUDED_COMPLEX"
    NOT_CLASSIFIED = "NOT_CLASSIFIED"

    @property
    def has_side(self) -> bool:
        return self in _SIDED


_SIDED = frozenset({SideClass.AT_OR_ABOVE_ASK, SideClass.AT_OR_BELOW_BID, SideClass.INSIDE_SPREAD})


class SideConfidence(str, Enum):
    """Values equal the database labels: LOW_CONFIDENCE is stored as LOW, UNKNOWN as NONE."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW_CONFIDENCE = "LOW"
    UNKNOWN = "NONE"


@dataclass(frozen=True, slots=True)
class AggressionPolicy:
    version: str = AGGRESSION_VERSION
    fresh_quote_ms: int = 1_000
    stale_quote_ms: int = 5_000

    def __post_init__(self) -> None:
        if not 0 <= self.fresh_quote_ms <= self.stale_quote_ms:
            raise ValueError("fresh_quote_ms must not exceed stale_quote_ms")


class AggressionResult(NamedTuple):
    side_class: SideClass
    confidence: SideConfidence
    position: Decimal | None
    reasons: tuple[str, ...]
    quote: QuoteView | None


_NONE = SideConfidence.UNKNOWN
_EXCLUDED = AggressionResult(SideClass.EXCLUDED_COMPLEX, _NONE, None, ("MULTI_LEG_PRINT_EXCLUDED",), None)
_UNAVAILABLE = AggressionResult(SideClass.AGGRESSION_UNAVAILABLE_NO_QUOTE, _NONE, None,
                                ("CONTRACT_NOT_ON_QUOTE_LIST",), None)
_NOT_CLASSIFIED = AggressionResult(SideClass.NOT_CLASSIFIED, _NONE, None, ("CONDITION_NOT_CLASSIFIED",), None)
_FOUR_PLACES = Decimal("0.0001")


def classify_aggression(
    price: float,
    lookup: QuoteLookup | None,
    *,
    verdict: ConditionVerdict,
    quote_listed: bool,
    policy: AggressionPolicy,
) -> AggressionResult:
    """Place a print against the quote that was current at its time. Never forces a side.

    The class says where the price sits relative to the quote, not who initiated the trade.
    Venue plays no part. Multi-leg prints report a net price and are excluded.
    """
    if not verdict.known or not verdict.volume_eligible:
        return _NOT_CLASSIFIED
    if verdict.is_multi_leg:
        return _EXCLUDED
    if not quote_listed:
        return _UNAVAILABLE
    if lookup is None or lookup.quote is None:
        reason = "NO_QUOTE_CACHED" if lookup is None else (lookup.reason or "NO_QUOTE_CACHED")
        return AggressionResult(SideClass.NO_QUOTE, _NONE, None, (reason,), None)
    quote = lookup.quote
    if quote.ask <= 0:
        return AggressionResult(SideClass.NO_QUOTE, _NONE, None, ("QUOTE_HAS_NO_OFFER",), quote)
    if quote.bid >= quote.ask:
        reason = "QUOTE_LOCKED" if quote.bid == quote.ask else "QUOTE_CROSSED"
        return AggressionResult(SideClass.QUOTE_CROSSED_OR_LOCKED, _NONE, None, (reason,), quote)
    age = lookup.age_ms or 0
    if age > policy.stale_quote_ms:
        return AggressionResult(SideClass.QUOTE_STALE, _NONE, None, ("QUOTE_OLDER_THAN_STALE_LIMIT",), quote)
    reasons: list[str] = []
    confidence = SideConfidence.HIGH if age <= policy.fresh_quote_ms else SideConfidence.MEDIUM
    if lookup.same_ms:
        confidence = SideConfidence.LOW_CONFIDENCE
        reasons.append("QUOTE_IN_SAME_MILLISECOND")
    if verdict.is_cross or verdict.is_floor:
        confidence = SideConfidence.LOW_CONFIDENCE
        reasons.append("NEGOTIATED_PRINT_NOT_AGGRESSOR_EVIDENCE")
    position: Decimal | None = None
    if price >= quote.ask:
        side = SideClass.AT_OR_ABOVE_ASK
    elif quote.bid > 0 and price <= quote.bid:
        side = SideClass.AT_OR_BELOW_BID
    else:
        side = SideClass.INSIDE_SPREAD
        position = Decimal(repr((price - quote.bid) / (quote.ask - quote.bid))).quantize(_FOUR_PLACES)
    return AggressionResult(side, confidence, position, tuple(reasons), quote)
