"""Change in open interest between two settlement sessions.

Volume counts opens, closes and round trips identically, which is why
`VOLUME_OI_ANOMALY` states in its own registry entry that volume above open interest
does not imply opening flow. A rise in open interest is different in kind: it is the
only unambiguous evidence that contracts were opened rather than churned.

Two things this deliberately does not claim:

Direction. Open interest rises whether the initiator bought or sold, so a call build is
not a bullish signal. Separating those needs the national best bid and offer at trade
time, which the current entitlement does not carry.

Attribution. Open interest is one net figure across the whole market for a contract. A
dealer hedge, an index rebalance and a single large directional position are
indistinguishable in it.

What it does establish is that net new positions exist, which nothing else in the
platform can currently state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum

# Below this prior level a percentage change is arithmetic noise: a contract going from
# 2 to 8 open contracts is a 300% rise and means nothing.
MINIMUM_PRIOR_OPEN_INTEREST = 100

# Share of session volume explained by the open-interest move. Above the upper bound the
# session was predominantly opening; below the lower bound it was predominantly churn.
PREDOMINANTLY_OPENING_SHARE = 0.70
PREDOMINANTLY_CHURN_SHARE = 0.30


class OpenInterestFlow(str, Enum):
    OPENING = "OPENING"
    UNWINDING = "UNWINDING"
    UNCHANGED = "UNCHANGED"


class VolumeConfirmation(str, Enum):
    PREDOMINANTLY_OPENING = "PREDOMINANTLY_OPENING"
    MIXED = "MIXED"
    PREDOMINANTLY_CHURN = "PREDOMINANTLY_CHURN"
    VOLUME_UNAVAILABLE = "VOLUME_UNAVAILABLE"
    NO_VOLUME_RECORDED = "NO_VOLUME_RECORDED"


@dataclass(frozen=True, slots=True)
class OpenInterestObservation:
    contract_id: int
    underlying: str
    contract_type: str
    strike: Decimal
    expiration_date: date
    settlement_session: date
    prior_settlement_session: date
    open_interest: int
    prior_open_interest: int
    session_volume: int | None = None
    # False when the volume is a running intraday total rather than the settled figure.
    session_volume_is_final: bool = True
    # Exchange sessions from the prior settlement to this one; 1 when consecutive.
    # Calendar-day arithmetic cannot supply this, because a weekend gap and a missed
    # session are indistinguishable by date distance alone.
    sessions_spanned: int = 1
    open_interest_revision_count: int = 0
    prior_open_interest_revision_count: int = 0


@dataclass(frozen=True, slots=True)
class OpenInterestChange:
    contract_id: int
    underlying: str
    contract_type: str
    strike: Decimal
    expiration_date: date
    settlement_session: date
    prior_settlement_session: date
    open_interest: int
    prior_open_interest: int
    change: int
    change_fraction: float | None
    flow: OpenInterestFlow
    confirmation: VolumeConfirmation
    opening_share: float | None
    session_volume_is_final: bool
    sessions_spanned: int
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UnderlyingOpenInterestSummary:
    underlying: str
    settlement_session: date
    prior_settlement_session: date
    contract_count: int
    call_change: int
    put_change: int
    opening_contract_count: int
    unwinding_contract_count: int
    total_open_interest: int
    prior_total_open_interest: int


def classify_open_interest_change(
    observation: OpenInterestObservation,
    minimum_prior_open_interest: int = MINIMUM_PRIOR_OPEN_INTEREST,
) -> OpenInterestChange:
    change = observation.open_interest - observation.prior_open_interest
    reasons: list[str] = []

    if observation.settlement_session <= observation.prior_settlement_session:
        raise ValueError("settlement_session must follow prior_settlement_session")
    if observation.sessions_spanned < 1:
        raise ValueError("sessions_spanned must be at least 1")
    if (
        observation.open_interest_revision_count < 0
        or observation.prior_open_interest_revision_count < 0
    ):
        raise ValueError("open interest revision counts cannot be negative")
    if observation.open_interest_revision_count:
        reasons.append("OPEN_INTEREST_REVISED")
    if observation.prior_open_interest_revision_count:
        reasons.append("PRIOR_OPEN_INTEREST_REVISED")

    change_fraction: float | None = None
    if observation.prior_open_interest >= minimum_prior_open_interest:
        change_fraction = change / observation.prior_open_interest
    else:
        reasons.append("PRIOR_OPEN_INTEREST_BELOW_FLOOR")

    if change > 0:
        flow = OpenInterestFlow.OPENING
    elif change < 0:
        flow = OpenInterestFlow.UNWINDING
    else:
        flow = OpenInterestFlow.UNCHANGED

    opening_share: float | None = None
    if observation.session_volume is None:
        confirmation = VolumeConfirmation.VOLUME_UNAVAILABLE
    elif observation.session_volume <= 0:
        confirmation = VolumeConfirmation.NO_VOLUME_RECORDED
        if change != 0:
            # Exercise, assignment and expiration all move open interest without a print.
            reasons.append("OPEN_INTEREST_MOVED_WITHOUT_VOLUME")
    else:
        opening_share = min(abs(change) / observation.session_volume, 1.0)
        if abs(change) > observation.session_volume:
            reasons.append("CHANGE_EXCEEDS_SESSION_VOLUME")
        if not observation.session_volume_is_final:
            # A running intraday total understates the denominator, so the share is an
            # upper bound and churn can be mistaken for opening.
            reasons.append("PROVISIONAL_SESSION_VOLUME")
        if opening_share >= PREDOMINANTLY_OPENING_SHARE:
            confirmation = VolumeConfirmation.PREDOMINANTLY_OPENING
        elif opening_share <= PREDOMINANTLY_CHURN_SHARE:
            confirmation = VolumeConfirmation.PREDOMINANTLY_CHURN
        else:
            confirmation = VolumeConfirmation.MIXED

    # Across a gap the move covers several sessions at once, and the volume recorded for
    # this session cannot account for the ones in between.
    if observation.sessions_spanned > 1:
        reasons.append("MULTI_SESSION_OPEN_INTEREST_CHANGE")
        if observation.session_volume is not None:
            confirmation = VolumeConfirmation.VOLUME_UNAVAILABLE
            opening_share = None

    return OpenInterestChange(
        contract_id=observation.contract_id,
        underlying=observation.underlying,
        contract_type=observation.contract_type,
        strike=observation.strike,
        expiration_date=observation.expiration_date,
        settlement_session=observation.settlement_session,
        prior_settlement_session=observation.prior_settlement_session,
        open_interest=observation.open_interest,
        prior_open_interest=observation.prior_open_interest,
        change=change,
        change_fraction=change_fraction,
        flow=flow,
        confirmation=confirmation,
        opening_share=opening_share,
        session_volume_is_final=observation.session_volume_is_final,
        sessions_spanned=observation.sessions_spanned,
        reasons=tuple(reasons),
    )


def rank_open_interest_builds(
    changes: tuple[OpenInterestChange, ...],
    minimum_absolute_change: int = 0,
    limit: int | None = None,
) -> tuple[OpenInterestChange, ...]:
    """Largest absolute moves first, so unwinding ranks alongside opening."""
    ranked = sorted(
        (change for change in changes if abs(change.change) >= minimum_absolute_change),
        key=lambda item: (-abs(item.change), item.expiration_date, item.strike),
    )
    return tuple(ranked if limit is None else ranked[:limit])


def summarize_underlying(
    underlying: str,
    changes: tuple[OpenInterestChange, ...],
) -> UnderlyingOpenInterestSummary | None:
    scoped = tuple(change for change in changes if change.underlying == underlying)
    if not scoped:
        return None
    return UnderlyingOpenInterestSummary(
        underlying=underlying,
        settlement_session=scoped[0].settlement_session,
        prior_settlement_session=scoped[0].prior_settlement_session,
        contract_count=len(scoped),
        call_change=sum(item.change for item in scoped if item.contract_type == "CALL"),
        put_change=sum(item.change for item in scoped if item.contract_type == "PUT"),
        opening_contract_count=sum(
            1 for item in scoped if item.flow is OpenInterestFlow.OPENING
        ),
        unwinding_contract_count=sum(
            1 for item in scoped if item.flow is OpenInterestFlow.UNWINDING
        ),
        total_open_interest=sum(item.open_interest for item in scoped),
        prior_total_open_interest=sum(item.prior_open_interest for item in scoped),
    )
