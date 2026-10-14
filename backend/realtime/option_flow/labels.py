from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import NamedTuple

from .classifier import SideClass, SideConfidence
from .volume import COMPLETE_BASELINES, BaselineKind

OPENING_INFERENCE_VERSION = "opening_inference_v1"
HEURISTIC_REASON = "HEURISTIC_UNCALIBRATED_V1"


class OpeningLabel(str, Enum):
    NOT_ASSESSED = "NOT_ASSESSED"
    LIKELY_OPENING = "LIKELY_OPENING"
    # Reserved: this inference version has no evidence that argues against opening flow.
    UNLIKELY_OPENING = "UNLIKELY_OPENING"
    INDETERMINATE = "INDETERMINATE"


@dataclass(frozen=True, slots=True)
class PriorOpenInterest:
    """Open interest published before the session. It cannot be backdated into a session."""

    open_interest: int
    as_of: date

    def __post_init__(self) -> None:
        if type(self.open_interest) is not int or self.open_interest < 0:
            raise ValueError("open_interest must be a nonnegative integer")


class OiAssessment(NamedTuple):
    prior_open_interest: int | None
    open_interest_as_of: date | None
    volume_exceeds_prior_oi: bool
    reasons: tuple[str, ...]


_NO_OI = OiAssessment(None, None, False, ("NO_PRIOR_OPEN_INTEREST",))
_OI_NOT_PRIOR = OiAssessment(None, None, False, ("OPEN_INTEREST_NOT_PRIOR_TO_SESSION",))


def assess_volume_vs_oi(
    prior: PriorOpenInterest | None,
    session_date: date,
    baseline: BaselineKind,
    cumulative_after: int | None,
) -> OiAssessment:
    """VOLUME_EXCEEDS_PRIOR_OI is a fact about the data, never a conclusion about intent.

    It needs a prior open interest from an earlier date, a complete volume baseline and a
    cumulative volume strictly above that open interest (the table CHECKs enforce the same).
    """
    if prior is None:
        return _NO_OI
    if prior.as_of >= session_date:
        return _OI_NOT_PRIOR
    if baseline not in COMPLETE_BASELINES or cumulative_after is None:
        return OiAssessment(prior.open_interest, prior.as_of, False, ("VOLUME_BASELINE_NOT_COMPLETE",))
    return OiAssessment(prior.open_interest, prior.as_of, cumulative_after > prior.open_interest, ())


@dataclass(frozen=True, slots=True)
class OpeningInferencePolicy:
    minimum_volume_oi_ratio: float
    minimum_print_notional: Decimal
    version: str = OPENING_INFERENCE_VERSION
    likely_threshold: float = 0.6
    ratio_weight: float = 0.5
    aggression_weight: float = 0.3
    notional_weight: float = 0.2

    def __post_init__(self) -> None:
        if self.minimum_volume_oi_ratio <= 0 or self.minimum_print_notional <= 0:
            raise ValueError("opening inference thresholds must be positive")
        if abs(self.ratio_weight + self.aggression_weight + self.notional_weight - 1.0) > 1e-9:
            raise ValueError("opening inference weights must sum to 1")
        if not 0 < self.likely_threshold <= 1:
            raise ValueError("likely_threshold must be in (0, 1]")


class OpeningAssessment(NamedTuple):
    label: OpeningLabel
    score: float | None
    reasons: tuple[str, ...]


_NOT_ASSESSED = OpeningAssessment(OpeningLabel.NOT_ASSESSED, None, ())


def infer_opening(
    oi: OiAssessment,
    cumulative_after: int | None,
    side_class: SideClass,
    confidence: SideConfidence,
    notional: Decimal,
    policy: OpeningInferencePolicy,
) -> OpeningAssessment:
    """A separate, scored inference. It exists only on top of the factual flag.

    The score is an uncalibrated heuristic, not a probability: its weights and threshold were
    chosen by hand and have not been fitted or validated against outcomes, so 0.8 does not mean
    an 80% chance of anything. Same-day round trips can push volume above open interest
    without any net opening, so the result is evidence with a score, labelled LIKELY_OPENING or
    INDETERMINATE. Every assessed result carries its reasons plus HEURISTIC_UNCALIBRATED_V1;
    NOT_ASSESSED carries neither a score nor reasons.
    """
    if not oi.volume_exceeds_prior_oi or oi.prior_open_interest is None or cumulative_after is None:
        return _NOT_ASSESSED
    reasons = ["VOLUME_EXCEEDS_PRIOR_OI"]
    ratio = cumulative_after / oi.prior_open_interest if oi.prior_open_interest > 0 else float("inf")
    if policy.minimum_volume_oi_ratio <= 1 or ratio >= policy.minimum_volume_oi_ratio:
        ratio_score = 1.0
        reasons.append("RATIO_AT_OR_ABOVE_THRESHOLD")
    else:
        ratio_score = max(0.0, (ratio - 1.0) / (policy.minimum_volume_oi_ratio - 1.0))
        reasons.append("RATIO_BELOW_THRESHOLD")
    firm = confidence in (SideConfidence.HIGH, SideConfidence.MEDIUM)
    if side_class in (SideClass.AT_OR_ABOVE_ASK, SideClass.AT_OR_BELOW_BID):
        aggression_score = 1.0 if firm else 0.5
        reasons.append("PRICE_AT_OR_THROUGH_QUOTE")
    elif side_class is SideClass.INSIDE_SPREAD:
        aggression_score = 0.3 if firm else 0.15
        reasons.append("PRICE_INSIDE_SPREAD")
    else:
        aggression_score = 0.0
        reasons.append("NO_SIDE_EVIDENCE")
    notional_score = min(1.0, float(notional / (2 * policy.minimum_print_notional)))
    score = round(
        policy.ratio_weight * ratio_score
        + policy.aggression_weight * aggression_score
        + policy.notional_weight * notional_score,
        4,
    )
    label = OpeningLabel.LIKELY_OPENING if score >= policy.likely_threshold else OpeningLabel.INDETERMINATE
    reasons.append(HEURISTIC_REASON)
    return OpeningAssessment(label, score, tuple(reasons))
