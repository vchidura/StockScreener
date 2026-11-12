"""O3 input producer for the live (advanced) Options window: credit-vertical candidates (matrix R44).

Design: `docs/g102/G102_O3_INPUTS_DESIGN.md`. This module is the first night's part (design steps 1, 2, 6 and 7): from
the window's watched contracts and the cutoff's quotes it builds the credit verticals O3 may judge, picks each one's
activity leg, applies O3's per-leg floors, bounds the count per underlying, and names the legs whose marks O3 needs.
It is pure (no database, no clock) and not wired into the window yet; steps 3-5 (stock trend decision, structural
invalidation, persistence of the two-leg package and the CONFIRMED `ADVANCED_OPTIONS_O3_INPUT` event) come next, all
behind `REALTIME_V3_O3_INPUTS` on the Options window (off by default: O3 stays BLOCKED as today).

Rules mirror what O3 itself checks (`options/credit_detection.py`, policy `o3_options_first_credit_v2`) and how the
C6 allocation reserves SPREAD_RANGE_LOCATOR pair groups (`advanced_options_activity_momentum_c6.pair_groups`): two
adjacent strikes of one underlying, expiry and side at 7-30 DTE, width at most `maximum_center_distance_fraction` of
spot. Calls: the lower strike is the short anchor, the higher the protective wing (bearish CALL_CREDIT_VERTICAL). Puts:
the higher strike is the short anchor, the lower the wing (bullish PUT_CREDIT_VERTICAL). A pair whose legs are not both
watched cannot be marked and is never built.
"""
from __future__ import annotations

import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable, Mapping, Sequence

from .contracts import canonical_sha256

O3_INPUTS_SWITCH = "REALTIME_V3_O3_INPUTS"
O3_INPUTS_CONTRACT = "advanced-options-o3-inputs-v1"


def o3_inputs_enabled() -> bool:
    """`ON` turns the producer on in the Options window; anything else (the default) keeps O3 as today."""
    return os.environ.get(O3_INPUTS_SWITCH, "OFF").strip().upper() == "ON"


@dataclass(frozen=True, slots=True)
class O3InputPolicy:
    """The parts of `o3_options_first_credit_v2` and the strategy's `spreads` block this step applies."""

    minimum_entry_dte: int = 7
    maximum_entry_dte: int = 30
    minimum_leg_open_interest: int = 500
    minimum_leg_day_volume: int = 50
    maximum_center_distance_fraction: Decimal = Decimal("0.02")
    maximum_candidates_per_underlying: int = 2

    @classmethod
    def from_strategy_document(cls, document: Mapping[str, object], *, maximum_per_underlying: int = 2):
        credit = dict(document.get("credit") or {})
        spreads = dict(document.get("spreads") or {})
        return cls(
            minimum_entry_dte=int(credit["minimum_entry_dte"]),
            maximum_entry_dte=int(credit["maximum_entry_dte"]),
            minimum_leg_open_interest=int(credit["minimum_leg_open_interest"]),
            minimum_leg_day_volume=int(credit["minimum_leg_day_volume"]),
            maximum_center_distance_fraction=Decimal(str(spreads.get("maximum_center_distance_fraction", "0.02"))),
            maximum_candidates_per_underlying=maximum_per_underlying,
        )


@dataclass(frozen=True, slots=True)
class WatchedContract:
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    underlying: str
    call_put: str  # "C" or "P"
    strike: Decimal
    expiration: date
    multiplier: int

    def __post_init__(self) -> None:
        if self.call_put not in ("C", "P"):
            raise ValueError("watched contract side must be C or P")
        if not self.strike.is_finite() or self.strike <= 0 or self.multiplier <= 0:
            raise ValueError("watched contract strike and multiplier must be positive")


@dataclass(frozen=True, slots=True)
class LegQuote:
    """One contract's state at the cutoff: the newest two-sided quote at or before it, and its day activity."""

    instrument_id: int
    bid: Decimal | None
    ask: Decimal | None
    market_time: datetime | None
    day_volume: int | None
    open_interest: int | None

    @property
    def midpoint(self) -> Decimal | None:
        if self.bid is None or self.ask is None or not self.bid.is_finite() or not self.ask.is_finite():
            return None
        if self.bid <= 0 or self.ask < self.bid:
            return None
        return (self.bid + self.ask) / 2


@dataclass(frozen=True, slots=True)
class CreditVertical:
    """A priced credit vertical. Money is per share (the package's leg marks); totals multiply by the multiplier."""

    underlying: str
    expiration: date
    structure_type: str  # PUT_CREDIT_VERTICAL or CALL_CREDIT_VERTICAL
    direction: int  # +1 bullish (put credit), -1 bearish (call credit)
    short: WatchedContract
    long: WatchedContract
    short_mark: Decimal
    long_mark: Decimal
    net_credit: Decimal
    width: Decimal
    maximum_loss: Decimal
    breakeven: Decimal
    return_on_risk: Decimal
    activity_instrument_id: int
    mark_market_time: datetime
    identity_sha256: str
    rank: int = 0

    @property
    def leg_instrument_ids(self) -> tuple[int, int]:
        return self.short.instrument_id, self.long.instrument_id

    @property
    def credit_to_width(self) -> Decimal:
        return self.net_credit / self.width


def credit_vertical_pairs(
    contracts: Iterable[WatchedContract],
    *,
    session_date: date,
    spot_by_underlying: Mapping[str, Decimal],
    policy: O3InputPolicy,
) -> tuple[tuple[WatchedContract, WatchedContract], ...]:
    """(short, long) pairs by C6's SPREAD_RANGE_LOCATOR pair rule; an underlying without a spot builds none."""
    buckets: dict[tuple[str, date, str], list[WatchedContract]] = defaultdict(list)
    for row in contracts:
        dte = (row.expiration - session_date).days
        if policy.minimum_entry_dte <= dte <= policy.maximum_entry_dte:
            buckets[(row.underlying, row.expiration, row.call_put)].append(row)
    pairs = []
    for (underlying, _expiration, side), rows in sorted(buckets.items()):
        spot = spot_by_underlying.get(underlying)
        if spot is None or not spot.is_finite() or spot <= 0:
            continue
        ordered = sorted(rows, key=lambda row: (row.strike, row.contract_ticker))
        for lower, upper in zip(ordered, ordered[1:]):
            width = upper.strike - lower.strike
            if width <= 0 or width / spot > policy.maximum_center_distance_fraction:
                continue
            if lower.multiplier != upper.multiplier:
                continue
            pairs.append((lower, upper) if side == "C" else (upper, lower))
    return tuple(pairs)


def activity_leg(short: WatchedContract, long: WatchedContract, quotes: Mapping[int, LegQuote]) -> int | None:
    """The leg with the highest day volume / open interest (ties: lower contract id), as the developer path picks."""
    scored = []
    for leg in (short, long):
        quote = quotes.get(leg.instrument_id)
        if quote is None or not quote.open_interest or quote.day_volume is None:
            continue
        scored.append((Decimal(quote.day_volume) / Decimal(quote.open_interest), -leg.contract_id, leg.instrument_id))
    return max(scored)[2] if scored else None


def price_credit_vertical(
    short: WatchedContract,
    long: WatchedContract,
    quotes: Mapping[int, LegQuote],
    *,
    cutoff: datetime,
    policy: O3InputPolicy,
) -> CreditVertical | str:
    """The priced vertical, or the reason it is not built (a stable code; never raises on market data)."""
    legs = (quotes.get(short.instrument_id), quotes.get(long.instrument_id))
    if any(quote is None or quote.midpoint is None or quote.market_time is None for quote in legs):
        return "O3_LEG_MARK_UNAVAILABLE"
    if any(quote.market_time > cutoff for quote in legs):
        return "O3_LEG_MARK_AFTER_CUTOFF"
    if any((quote.open_interest or 0) < policy.minimum_leg_open_interest
           or (quote.day_volume or 0) < policy.minimum_leg_day_volume for quote in legs):
        return "O3_LEG_ACTIVITY_BELOW_POLICY"
    short_mark, long_mark = legs[0].midpoint, legs[1].midpoint
    net_credit = short_mark - long_mark
    width = abs(short.strike - long.strike)
    if net_credit <= 0:
        return "O3_NON_POSITIVE_CREDIT"
    maximum_loss = width - net_credit
    if maximum_loss <= 0:
        return "O3_CREDIT_NOT_BOUNDED"
    activity = activity_leg(short, long, quotes)
    if activity is None:
        return "O3_ACTIVITY_LEG_UNAVAILABLE"
    put = short.call_put == "P"
    structure = "PUT_CREDIT_VERTICAL" if put else "CALL_CREDIT_VERTICAL"
    identity = canonical_sha256({
        "contract": O3_INPUTS_CONTRACT,
        "structure": structure,
        "underlying": short.underlying,
        "expiration": short.expiration.isoformat(),
        "short": short.contract_ticker,
        "long": long.contract_ticker,
        "short_mark": str(short_mark),
        "long_mark": str(long_mark),
        "cutoff": cutoff.isoformat(),
    })
    return CreditVertical(
        underlying=short.underlying,
        expiration=short.expiration,
        structure_type=structure,
        direction=1 if put else -1,
        short=short,
        long=long,
        short_mark=short_mark,
        long_mark=long_mark,
        net_credit=net_credit,
        width=width,
        maximum_loss=maximum_loss,
        breakeven=short.strike - net_credit if put else short.strike + net_credit,
        return_on_risk=net_credit / maximum_loss,
        activity_instrument_id=activity,
        mark_market_time=max(quote.market_time for quote in legs),
        identity_sha256=identity,
    )


@dataclass(frozen=True, slots=True)
class O3CandidateSelection:
    candidates: tuple[CreditVertical, ...]
    rejections: tuple[tuple[str, int], ...]  # (reason, count), sorted

    @property
    def judged_instrument_ids(self) -> frozenset[int]:
        """Design step 6: O3's marks gate covers only these legs, not every watched contract."""
        return frozenset(leg for row in self.candidates for leg in row.leg_instrument_ids)


def select_o3_candidates(
    contracts: Sequence[WatchedContract],
    quotes: Mapping[int, LegQuote],
    *,
    session_date: date,
    cutoff: datetime,
    spot_by_underlying: Mapping[str, Decimal],
    policy: O3InputPolicy = O3InputPolicy(),
) -> O3CandidateSelection:
    """Design steps 1, 2 and 7: every buildable credit vertical, best `maximum_candidates_per_underlying` per
    underlying by credit per unit of width (ties: higher return on risk, then identity), ranked from 1."""
    rejections: Counter[str] = Counter()
    built: dict[str, list[CreditVertical]] = defaultdict(list)
    for short, long in credit_vertical_pairs(
        contracts, session_date=session_date, spot_by_underlying=spot_by_underlying, policy=policy,
    ):
        priced = price_credit_vertical(short, long, quotes, cutoff=cutoff, policy=policy)
        if isinstance(priced, str):
            rejections[priced] += 1
        else:
            built[priced.underlying].append(priced)
    selected = []
    for underlying in sorted(built):
        ordered = sorted(
            built[underlying],
            key=lambda row: (-row.credit_to_width, -row.return_on_risk, row.identity_sha256),
        )
        if len(ordered) > policy.maximum_candidates_per_underlying:
            rejections["O3_CANDIDATE_BOUND"] += len(ordered) - policy.maximum_candidates_per_underlying
        for rank, row in enumerate(ordered[: policy.maximum_candidates_per_underlying], start=1):
            selected.append(_ranked(row, rank))
    return O3CandidateSelection(tuple(selected), tuple(sorted(rejections.items())))


def _ranked(row: CreditVertical, rank: int) -> CreditVertical:
    from dataclasses import replace

    return replace(row, rank=rank)
