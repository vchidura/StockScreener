from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from options.domain import ContractType

from .domain import CandidateLeg, OptionSide


ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class PayoffSummary:
    net_premium: Decimal
    maximum_profit: Decimal | None
    maximum_loss: Decimal | None
    breakevens: tuple[Decimal, ...]
    bounded_maximum_loss: bool


def terminal_profit(legs: tuple[CandidateLeg, ...], terminal_spot: Decimal) -> Decimal:
    if not legs:
        raise ValueError("terminal payoff requires at least one leg")
    if terminal_spot < 0:
        raise ValueError("terminal_spot cannot be negative")
    profit = ZERO
    for leg in legs:
        intrinsic = (
            max(terminal_spot - leg.strike, ZERO)
            if leg.contract_type is ContractType.CALL
            else max(leg.strike - terminal_spot, ZERO)
        )
        direction = Decimal(1) if leg.side is OptionSide.BUY else Decimal(-1)
        profit += (
            direction
            * (intrinsic - leg.model_mark)
            * leg.ratio
            * leg.multiplier
        )
    return profit


def evaluate_terminal_payoff(legs: tuple[CandidateLeg, ...]) -> PayoffSummary:
    if not legs:
        raise ValueError("terminal payoff requires at least one leg")
    if len({leg.expiration_date for leg in legs}) != 1:
        raise ValueError("terminal payoff legs must share one expiration")
    strikes = tuple(sorted({leg.strike for leg in legs}))
    breakpoints = (ZERO, *strikes)
    profits = tuple(terminal_profit(legs, spot) for spot in breakpoints)
    upper_slope = sum(
        (Decimal(1) if leg.side is OptionSide.BUY else Decimal(-1))
        * leg.ratio
        * leg.multiplier
        for leg in legs
        if leg.contract_type is ContractType.CALL
    )
    bounded_loss = upper_slope >= 0
    maximum_loss = -min(profits) if bounded_loss and min(profits) < 0 else ZERO
    maximum_profit = None if upper_slope > 0 else max(profits)
    net_premium = sum(
        (Decimal(1) if leg.side is OptionSide.SELL else Decimal(-1))
        * leg.model_mark
        * leg.ratio
        * leg.multiplier
        for leg in legs
    )
    breakevens: set[Decimal] = set()
    for left, right in zip(breakpoints, breakpoints[1:]):
        left_profit = terminal_profit(legs, left)
        right_profit = terminal_profit(legs, right)
        if left_profit == 0:
            breakevens.add(left)
        if right_profit == 0:
            breakevens.add(right)
        if left_profit * right_profit < 0:
            slope = (right_profit - left_profit) / (right - left)
            breakevens.add(left - left_profit / slope)
    tail_start = strikes[-1]
    tail_profit = terminal_profit(legs, tail_start)
    if upper_slope != 0:
        tail_root = tail_start - tail_profit / upper_slope
        if tail_root >= tail_start:
            breakevens.add(tail_root)
    return PayoffSummary(
        net_premium=net_premium,
        maximum_profit=maximum_profit,
        maximum_loss=maximum_loss if bounded_loss else None,
        breakevens=tuple(sorted(breakevens)),
        bounded_maximum_loss=bounded_loss,
    )