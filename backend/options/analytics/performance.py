"""Realized profit and loss aggregation for daily manual review.

The outcome rows already carry entry and exit premium, gross and net profit, the cost
estimate and the capital at risk. What they do not carry is a judgement about whether a
number is worth reading, and that is what this adds.

Three honesty rules are built in rather than left to the reader:

A win rate on its own says nothing, because a structure that wins often and loses large
and one that loses often and wins large can share it. Win rate is therefore always
reported next to mean return and profit factor.

A small sample is labelled, not shown as though it were a measurement. Overlapping
horizons mean one candidate contributes several rows, so distinct sessions are counted
separately from raw observations.

Profit measured under two different valuation policies is not comparable. A summary that
spans more than one policy hash says so, because the mark definition changed underneath it.

The raw totals are also not differenced against anything. `compare_against_baseline` is
what separates selection from market direction, by pairing each candidate with the naive
at-the-money structure of the same session, expiration and side.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from math import erf, isfinite, sqrt
from typing import Mapping

import numpy as np

# A Newey-West corrected test of a mean differential, which is exactly what a paired
# advantage over a baseline needs when the measurement windows overlap.
from options.analytics.realized_volatility import diebold_mariano

# Below this a cell is a handful of trades, not a measurement.
MINIMUM_OUTCOMES_FOR_READING = 30
# Matches the events floor the equity confidence gates already use.
MINIMUM_OUTCOMES_FOR_CONFIDENCE = 100
# Overlapping windows collapse to one cross-sectional observation per session.
MINIMUM_INDEPENDENT_SESSIONS = 40


class PerformanceVerdict(str, Enum):
    INSUFFICIENT = "INSUFFICIENT"
    INDICATIVE = "INDICATIVE"
    MEASURABLE = "MEASURABLE"


@dataclass(frozen=True, slots=True)
class OutcomeRow:
    strategy_name: str
    underlying: str
    measurement_type: str
    session_date: date
    net_pnl: Decimal | None
    net_return: Decimal | None
    capital_at_risk: Decimal | None
    estimated_cost: Decimal | None
    availability_flag: str
    valuation_policy_sha256: str | None


@dataclass(frozen=True, slots=True)
class PerformanceSummary:
    label: str
    observations: int
    priced_observations: int
    independent_sessions: int
    win_rate: float | None
    mean_net_return: float | None
    median_net_return: float | None
    total_net_pnl: Decimal
    total_estimated_cost: Decimal
    mean_capital_at_risk: float | None
    profit_factor: float | None
    largest_gain: Decimal | None
    largest_loss: Decimal | None
    verdict: PerformanceVerdict
    valuation_policies: tuple[str, ...]
    reasons: tuple[str, ...]


def _verdict(priced: int, sessions: int) -> PerformanceVerdict:
    if priced < MINIMUM_OUTCOMES_FOR_READING:
        return PerformanceVerdict.INSUFFICIENT
    if priced < MINIMUM_OUTCOMES_FOR_CONFIDENCE or sessions < MINIMUM_INDEPENDENT_SESSIONS:
        return PerformanceVerdict.INDICATIVE
    return PerformanceVerdict.MEASURABLE


def summarize_outcomes(label: str, rows: tuple[OutcomeRow, ...]) -> PerformanceSummary:
    priced = [row for row in rows if row.net_pnl is not None]
    sessions = {row.session_date for row in priced}
    policies = tuple(
        sorted({row.valuation_policy_sha256 for row in priced if row.valuation_policy_sha256})
    )

    reasons: list[str] = []
    if len(rows) != len(priced):
        reasons.append("UNPRICED_OUTCOMES_EXCLUDED")
    if len(policies) > 1:
        # The mark definition changed inside the window, so these are not one series.
        reasons.append("MULTIPLE_VALUATION_POLICIES")
    if priced and len(sessions) < MINIMUM_INDEPENDENT_SESSIONS:
        reasons.append("FEW_INDEPENDENT_SESSIONS")
    # These totals are raw, not differenced against the naive structure. A positive number
    # here can still be direction rather than selection; the baseline section is what
    # separates them.
    reasons.append("NOT_BASELINE_ADJUSTED")

    if not priced:
        return PerformanceSummary(
            label=label,
            observations=len(rows),
            priced_observations=0,
            independent_sessions=0,
            win_rate=None,
            mean_net_return=None,
            median_net_return=None,
            total_net_pnl=Decimal("0"),
            total_estimated_cost=Decimal("0"),
            mean_capital_at_risk=None,
            profit_factor=None,
            largest_gain=None,
            largest_loss=None,
            verdict=PerformanceVerdict.INSUFFICIENT,
            valuation_policies=policies,
            reasons=tuple(reasons),
        )

    pnl = [row.net_pnl for row in priced]
    returns = [float(row.net_return) for row in priced if row.net_return is not None]
    capital = [float(row.capital_at_risk) for row in priced if row.capital_at_risk is not None]
    gains = sum((value for value in pnl if value > 0), Decimal("0"))
    losses = sum((-value for value in pnl if value < 0), Decimal("0"))

    return PerformanceSummary(
        label=label,
        observations=len(rows),
        priced_observations=len(priced),
        independent_sessions=len(sessions),
        win_rate=sum(1 for value in pnl if value > 0) / len(pnl),
        mean_net_return=statistics.fmean(returns) if returns else None,
        median_net_return=statistics.median(returns) if returns else None,
        total_net_pnl=sum(pnl, Decimal("0")),
        total_estimated_cost=sum(
            (row.estimated_cost for row in priced if row.estimated_cost is not None),
            Decimal("0"),
        ),
        mean_capital_at_risk=statistics.fmean(capital) if capital else None,
        profit_factor=float(gains / losses) if losses > 0 else None,
        largest_gain=max(pnl),
        largest_loss=min(pnl),
        verdict=_verdict(len(priced), len(sessions)),
        valuation_policies=policies,
        reasons=tuple(reasons),
    )


def group_by(
    rows: tuple[OutcomeRow, ...], key: str
) -> dict[str, tuple[OutcomeRow, ...]]:
    grouped: dict[str, list[OutcomeRow]] = {}
    for row in rows:
        grouped.setdefault(str(getattr(row, key)), []).append(row)
    return {name: tuple(items) for name, items in sorted(grouped.items())}


@dataclass(frozen=True, slots=True)
class BaselinePair:
    """One candidate and the naive structure it must beat to have earned anything.

    The baseline is the same session, same expiration, same side, at the money, with no
    selection applied. Pairing within a session is what removes market direction: both
    legs of the pair experience the same move, so the difference isolates selection.
    """

    strategy_name: str
    session_date: date
    measurement_type: str
    strategy_return: float
    baseline_return: float
    horizon_sessions: int = 1

    @property
    def advantage(self) -> float:
        return self.strategy_return - self.baseline_return

    def __post_init__(self) -> None:
        if self.horizon_sessions < 1:
            raise ValueError("horizon_sessions must be positive")


@dataclass(frozen=True, slots=True)
class BaselineStructureLeg:
    leg_index: int
    side: str
    ratio: int
    multiplier: int
    contract_type: str
    strike: Decimal
    entry_mark: Decimal
    exit_mark: Decimal

    def __post_init__(self) -> None:
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("baseline leg side is invalid")
        if self.contract_type not in {"CALL", "PUT"}:
            raise ValueError("baseline leg contract type is invalid")
        if self.ratio < 1 or self.multiplier < 1:
            raise ValueError("baseline leg ratio and multiplier must be positive")
        if self.strike <= 0 or self.entry_mark <= 0 or self.exit_mark <= 0:
            raise ValueError("baseline leg prices and strike must be positive")


@dataclass(frozen=True, slots=True)
class StructureBaselineResult:
    entry_net_premium: Decimal
    exit_net_premium: Decimal
    gross_pnl: Decimal
    estimated_cost: Decimal
    net_pnl: Decimal
    capital_at_risk: Decimal
    net_return: float


def _terminal_profit(
    legs: tuple[BaselineStructureLeg, ...], terminal_spot: Decimal
) -> Decimal:
    profit = Decimal("0")
    for leg in legs:
        intrinsic = (
            max(terminal_spot - leg.strike, Decimal("0"))
            if leg.contract_type == "CALL"
            else max(leg.strike - terminal_spot, Decimal("0"))
        )
        direction = Decimal("1") if leg.side == "BUY" else Decimal("-1")
        profit += (
            direction
            * (intrinsic - leg.entry_mark)
            * leg.ratio
            * leg.multiplier
        )
    return profit


def evaluate_structure_baseline(
    legs: tuple[BaselineStructureLeg, ...],
    *,
    commission_per_contract_per_side: Decimal = Decimal("0.65"),
) -> StructureBaselineResult | None:
    """Return a bounded package's outcome under the same accounting as live outcomes."""
    if not legs:
        return None
    if len({leg.leg_index for leg in legs}) != len(legs):
        raise ValueError("baseline leg indexes must be unique")
    ordered = tuple(sorted(legs, key=lambda leg: leg.leg_index))
    entry_net_premium = sum(
        (
            Decimal("1") if leg.side == "SELL" else Decimal("-1")
        ) * leg.entry_mark * leg.ratio * leg.multiplier
        for leg in ordered
    )
    exit_net_premium = sum(
        (
            Decimal("1") if leg.side == "SELL" else Decimal("-1")
        ) * leg.exit_mark * leg.ratio * leg.multiplier
        for leg in ordered
    )
    breakpoints = (Decimal("0"), *sorted({leg.strike for leg in ordered}))
    profits = tuple(_terminal_profit(ordered, spot) for spot in breakpoints)
    upper_slope = sum(
        (Decimal("1") if leg.side == "BUY" else Decimal("-1"))
        * leg.ratio
        * leg.multiplier
        for leg in ordered
        if leg.contract_type == "CALL"
    )
    if upper_slope < 0:
        return None
    minimum_profit = min(profits)
    capital_at_risk = -minimum_profit if minimum_profit < 0 else Decimal("0")
    if capital_at_risk <= 0:
        return None
    gross_pnl = entry_net_premium - exit_net_premium
    estimated_cost = (
        commission_per_contract_per_side
        * Decimal("2")
        * sum(leg.ratio for leg in ordered)
    )
    net_pnl = gross_pnl - estimated_cost
    return StructureBaselineResult(
        entry_net_premium=entry_net_premium,
        exit_net_premium=exit_net_premium,
        gross_pnl=gross_pnl,
        estimated_cost=estimated_cost,
        net_pnl=net_pnl,
        capital_at_risk=capital_at_risk,
        net_return=float(net_pnl / capital_at_risk),
    )


@dataclass(frozen=True, slots=True)
class BaselineComparison:
    label: str
    pairs: int
    # Pairs are collapsed to one observation per (session, horizon) before testing, so
    # this - not `pairs` - is the sample the statistic is computed on.
    period_observations: int
    independent_sessions: int
    measurement_types: tuple[str, ...]
    mean_strategy_return: float | None
    mean_baseline_return: float | None
    mean_advantage: float | None
    median_advantage: float | None
    beat_baseline_rate: float | None
    statistic: float
    p_value: float
    verdict: PerformanceVerdict
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OptionConfidenceSummary:
    strategy_name: str
    measurement_type: str
    events: int
    independent_periods: int
    mean_strategy_return: float | None
    strategy_t_stat: float | None
    early_strategy_return: float | None
    late_strategy_return: float | None
    mean_advantage: float | None
    advantage_t_stat: float | None
    early_advantage: float | None
    late_advantage: float | None
    strategy_p_value: float | None
    advantage_p_value: float | None
    strategy_fdr_q: float | None
    advantage_fdr_q: float | None
    status: str
    robustness_status: str


def _t_stat(values: tuple[float, ...]) -> float | None:
    if len(values) < 2:
        return None
    standard_deviation = statistics.stdev(values)
    if standard_deviation <= 0:
        return None
    return statistics.fmean(values) / (standard_deviation / sqrt(len(values)))


def _normal_p_value(t_stat: float | None) -> float | None:
    if t_stat is None or not isfinite(t_stat):
        return None
    return 2.0 * (1.0 - 0.5 * (1.0 + erf(abs(t_stat) / sqrt(2.0))))


def _benjamini_hochberg(values: tuple[float | None, ...]) -> tuple[float | None, ...]:
    indexed = [(index, value) for index, value in enumerate(values) if value is not None]
    if not indexed:
        return tuple(None for _ in values)
    ranked = sorted(indexed, key=lambda item: item[1])
    adjusted: dict[int, float] = {}
    running = 1.0
    count = len(ranked)
    for rank_index in range(count - 1, -1, -1):
        original_index, value = ranked[rank_index]
        rank = rank_index + 1
        running = min(running, value * count / rank)
        adjusted[original_index] = min(running, 1.0)
    return tuple(adjusted.get(index) for index in range(len(values)))


def summarize_option_confidence(
    pairs: tuple[BaselinePair, ...],
    *,
    session_ordinals: Mapping[date, int] | None = None,
    minimum_events: int = MINIMUM_OUTCOMES_FOR_CONFIDENCE,
    minimum_independent_periods: int = MINIMUM_INDEPENDENT_SESSIONS,
) -> tuple[OptionConfidenceSummary, ...]:
    """Apply the equity confidence contract to paired option advantages."""
    grouped: dict[tuple[str, str], list[BaselinePair]] = {}
    for pair in pairs:
        grouped.setdefault((pair.strategy_name, pair.measurement_type), []).append(pair)

    interim: list[dict[str, object]] = []
    for (strategy_name, measurement_type), group in sorted(grouped.items()):
        by_session: dict[date, list[BaselinePair]] = {}
        for pair in group:
            by_session.setdefault(pair.session_date, []).append(pair)
        session_portfolios = [
            (
                session,
                statistics.fmean(pair.strategy_return for pair in values),
                statistics.fmean(pair.advantage for pair in values),
                max(pair.horizon_sessions for pair in values),
            )
            for session, values in sorted(by_session.items())
        ]
        ordinals = session_ordinals or {
            session: index
            for index, (session, _, _, _) in enumerate(session_portfolios)
        }
        independent = []
        last_selected_ordinal = -10**9
        for portfolio in session_portfolios:
            ordinal = ordinals.get(portfolio[0])
            if ordinal is None:
                raise ValueError("session_ordinals is missing an observed session")
            if ordinal - last_selected_ordinal < portfolio[3]:
                continue
            independent.append(portfolio)
            last_selected_ordinal = ordinal

        strategy_returns = tuple(value[1] for value in independent)
        advantages = tuple(value[2] for value in independent)
        midpoint = max(1, len(independent) // 2)
        early_strategy = strategy_returns[:midpoint]
        late_strategy = strategy_returns[midpoint:]
        early_advantage = advantages[:midpoint]
        late_advantage = advantages[midpoint:]
        mean_strategy = statistics.fmean(strategy_returns) if strategy_returns else None
        mean_advantage = statistics.fmean(advantages) if advantages else None
        strategy_t = _t_stat(strategy_returns)
        advantage_t = _t_stat(advantages)
        confidence_pass = (
            len(group) >= minimum_events
            and len(independent) >= minimum_independent_periods
            and mean_strategy is not None
            and mean_strategy > 0
            and strategy_t is not None
            and strategy_t > 2
            and bool(late_strategy)
            and statistics.fmean(early_strategy) > 0
            and statistics.fmean(late_strategy) > 0
            and mean_advantage is not None
            and mean_advantage > 0
            and advantage_t is not None
            and advantage_t > 2
            and bool(late_advantage)
            and statistics.fmean(early_advantage) > 0
            and statistics.fmean(late_advantage) > 0
        )
        interim.append({
            "strategy_name": strategy_name,
            "measurement_type": measurement_type,
            "events": len(group),
            "independent_periods": len(independent),
            "mean_strategy_return": mean_strategy,
            "strategy_t_stat": strategy_t,
            "early_strategy_return": (
                statistics.fmean(early_strategy) if early_strategy else None
            ),
            "late_strategy_return": (
                statistics.fmean(late_strategy) if late_strategy else None
            ),
            "mean_advantage": mean_advantage,
            "advantage_t_stat": advantage_t,
            "early_advantage": (
                statistics.fmean(early_advantage) if early_advantage else None
            ),
            "late_advantage": (
                statistics.fmean(late_advantage) if late_advantage else None
            ),
            "strategy_p_value": _normal_p_value(strategy_t),
            "advantage_p_value": _normal_p_value(advantage_t),
            "status": "CONFIDENCE_PASS" if confidence_pass else "NOT_QUALIFIED",
        })

    strategy_q = _benjamini_hochberg(
        tuple(row["strategy_p_value"] for row in interim)
    )
    advantage_q = _benjamini_hochberg(
        tuple(row["advantage_p_value"] for row in interim)
    )
    results = []
    for index, row in enumerate(interim):
        robust = (
            row["status"] == "CONFIDENCE_PASS"
            and strategy_q[index] is not None
            and strategy_q[index] <= 0.05
            and advantage_q[index] is not None
            and advantage_q[index] <= 0.05
        )
        results.append(
            OptionConfidenceSummary(
                **row,
                strategy_fdr_q=strategy_q[index],
                advantage_fdr_q=advantage_q[index],
                robustness_status="ROBUST_PASS" if robust else "NOT_ROBUST",
            )
        )
    return tuple(results)


def compare_against_baseline(
    label: str,
    pairs: tuple[BaselinePair, ...],
    horizon_sessions: int = 1,
) -> BaselineComparison:
    """Paired advantage over the naive structure, with overlap-corrected significance.

    Candidates measured on overlapping windows produce autocorrelated differences, so the
    variance is Newey-West corrected rather than treated as independent.
    """
    if not pairs:
        return BaselineComparison(
            label=label,
            pairs=0,
            period_observations=0,
            independent_sessions=0,
            measurement_types=(),
            mean_strategy_return=None,
            mean_baseline_return=None,
            mean_advantage=None,
            median_advantage=None,
            beat_baseline_rate=None,
            statistic=float("nan"),
            p_value=float("nan"),
            verdict=PerformanceVerdict.INSUFFICIENT,
            reasons=("NO_BASELINE_PAIRS",),
        )

    by_period: dict[tuple[date, str], list[BaselinePair]] = {}
    for pair in pairs:
        by_period.setdefault(
            (pair.session_date, pair.measurement_type), []
        ).append(pair)
    portfolios = [
        (
            statistics.fmean(pair.strategy_return for pair in period),
            statistics.fmean(pair.baseline_return for pair in period),
        )
        for _, period in sorted(by_period.items())
    ]
    strategy = [portfolio[0] for portfolio in portfolios]
    baseline = [portfolio[1] for portfolio in portfolios]
    advantages = [strategy_return - baseline_return for strategy_return, baseline_return in portfolios]
    sessions = {pair.session_date for pair in pairs}
    measurement_types = tuple(sorted({pair.measurement_type for pair in pairs}))

    if len(measurement_types) == 1:
        lag = max(horizon_sessions - 1, 0)
        statistic, p_value = diebold_mariano(
            np.asarray(baseline, dtype=float), np.asarray(strategy, dtype=float), lag
        )
    else:
        statistic = p_value = float("nan")

    reasons: list[str] = []
    if len(sessions) < MINIMUM_INDEPENDENT_SESSIONS:
        reasons.append("FEW_INDEPENDENT_SESSIONS")
    if statistics.fmean(advantages) <= 0:
        reasons.append("NO_ADVANTAGE_OVER_BASELINE")
    if len(portfolios) < 3:
        # The Newey-West test needs dispersion across periods; two portfolio
        # observations cannot supply it, which is why the statistic is undefined.
        reasons.append("TOO_FEW_PERIODS_TO_TEST")
    if len(measurement_types) > 1:
        reasons.append("MIXED_MEASUREMENT_TYPES")

    return BaselineComparison(
        label=label,
        pairs=len(pairs),
        period_observations=len(portfolios),
        independent_sessions=len(sessions),
        measurement_types=measurement_types,
        mean_strategy_return=statistics.fmean(strategy),
        mean_baseline_return=statistics.fmean(baseline),
        mean_advantage=statistics.fmean(advantages),
        median_advantage=statistics.median(advantages),
        beat_baseline_rate=sum(1 for value in advantages if value > 0) / len(advantages),
        statistic=statistic,
        p_value=p_value,
        verdict=_verdict(len(pairs), len(sessions)),
        reasons=tuple(reasons),
    )
