from __future__ import annotations

import random
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from options.analytics.performance import (
    MINIMUM_INDEPENDENT_SESSIONS,
    MINIMUM_OUTCOMES_FOR_CONFIDENCE,
    MINIMUM_OUTCOMES_FOR_READING,
    BaselinePair,
    BaselineStructureLeg,
    OutcomeRow,
    PerformanceVerdict,
    compare_against_baseline,
    evaluate_structure_baseline,
    group_by,
    summarize_outcomes,
    summarize_option_confidence,
)
from scripts import report_option_performance as performance_report


def _baseline_leg(
    index: int,
    side: str,
    strike: str,
    entry: str,
    exit_mark: str,
    *,
    contract_type: str = "CALL",
    ratio: int = 1,
) -> BaselineStructureLeg:
    return BaselineStructureLeg(
        leg_index=index,
        side=side,
        ratio=ratio,
        multiplier=100,
        contract_type=contract_type,
        strike=Decimal(strike),
        entry_mark=Decimal(entry),
        exit_mark=Decimal(exit_mark),
    )


def test_long_call_baseline_uses_premium_at_risk():
    result = evaluate_structure_baseline(
        (_baseline_leg(0, "BUY", "100", "2.00", "3.00"),)
    )
    assert result is not None
    assert result.capital_at_risk == Decimal("200.00")
    assert result.gross_pnl == Decimal("100.00")
    assert result.net_return == pytest.approx(98.7 / 200)


def test_call_debit_vertical_preserves_bounded_package_risk():
    result = evaluate_structure_baseline(
        (
            _baseline_leg(0, "BUY", "100", "3.00", "4.00"),
            _baseline_leg(1, "SELL", "105", "1.00", "1.50"),
        )
    )
    assert result is not None
    assert result.entry_net_premium == Decimal("-200.00")
    assert result.capital_at_risk == Decimal("200.00")
    assert result.gross_pnl == Decimal("50.00")


def test_put_credit_vertical_preserves_bounded_package_risk():
    result = evaluate_structure_baseline(
        (
            _baseline_leg(
                0, "SELL", "100", "3.00", "2.00", contract_type="PUT"
            ),
            _baseline_leg(
                1, "BUY", "95", "1.00", "0.50", contract_type="PUT"
            ),
        )
    )
    assert result is not None
    assert result.entry_net_premium == Decimal("200.00")
    assert result.capital_at_risk == Decimal("300.00")
    assert result.gross_pnl == Decimal("50.00")


def test_iron_condor_baseline_is_one_coherent_four_leg_package():
    result = evaluate_structure_baseline(
        (
            _baseline_leg(0, "BUY", "90", "0.50", "0.25", contract_type="PUT"),
            _baseline_leg(1, "SELL", "95", "1.50", "0.75", contract_type="PUT"),
            _baseline_leg(2, "SELL", "105", "1.50", "0.75"),
            _baseline_leg(3, "BUY", "110", "0.50", "0.25"),
        )
    )
    assert result is not None
    assert result.entry_net_premium == Decimal("200.00")
    assert result.capital_at_risk == Decimal("300.00")
    assert result.gross_pnl == Decimal("100.00")


def test_unbounded_short_call_has_no_baseline_return():
    result = evaluate_structure_baseline(
        (_baseline_leg(0, "SELL", "100", "2.00", "1.00"),)
    )
    assert result is None


def test_option_confidence_requires_enough_independent_periods():
    pairs = tuple(_pair(0.08, 0.02, day) for day in range(120))
    pairs = tuple(
        replace(pair, horizon_sessions=10)
        for pair in pairs
    )
    summary = summarize_option_confidence(pairs)[0]
    assert summary.events == 120
    assert summary.independent_periods == 12
    assert summary.status == "NOT_QUALIFIED"


def test_option_confidence_uses_market_session_distance_for_sparse_signals():
    pairs = tuple(
        replace(_pair(0.08, 0.02, day), horizon_sessions=5)
        for day in range(0, 120, 10)
    )
    ordinals = {pair.session_date: index * 10 for index, pair in enumerate(pairs)}
    summary = summarize_option_confidence(
        pairs,
        session_ordinals=ordinals,
        minimum_events=1,
        minimum_independent_periods=1,
    )[0]
    assert summary.independent_periods == len(pairs)


def test_option_confidence_requires_both_halves_positive():
    pairs = tuple(
        _pair(0.10, 0.02, day) if day < 60 else _pair(-0.04, 0.02, day)
        for day in range(120)
    )
    summary = summarize_option_confidence(pairs)[0]
    assert summary.early_advantage > 0
    assert summary.late_advantage < 0
    assert summary.status == "NOT_QUALIFIED"


def test_option_confidence_reaches_robust_pass_for_stable_incremental_edge():
    generator = random.Random(29)
    pairs = []
    for day in range(140):
        baseline = generator.gauss(0.01, 0.01)
        advantage = generator.gauss(0.025, 0.006)
        pairs.append(_pair(baseline + advantage, baseline, day))
    summary = summarize_option_confidence(tuple(pairs))[0]
    assert summary.independent_periods == 140
    assert summary.strategy_t_stat > 2
    assert summary.advantage_t_stat > 2
    assert summary.status == "CONFIDENCE_PASS"
    assert summary.strategy_fdr_q <= 0.05
    assert summary.advantage_fdr_q <= 0.05
    assert summary.robustness_status == "ROBUST_PASS"


def _row(
    net_pnl: str | None = "10",
    session: date = date(2026, 9, 1),
    net_return: str | None = "0.05",
    policy: str | None = "a" * 64,
    strategy: str = "INCOME_WHEEL",
    underlying: str = "SPY",
    measurement: str = "CLOSE",
) -> OutcomeRow:
    return OutcomeRow(
        strategy_name=strategy,
        underlying=underlying,
        measurement_type=measurement,
        session_date=session,
        net_pnl=Decimal(net_pnl) if net_pnl is not None else None,
        net_return=Decimal(net_return) if net_return is not None else None,
        capital_at_risk=Decimal("500"),
        estimated_cost=Decimal("1.30"),
        availability_flag="RESEARCH_DELAYED_PROXY",
        valuation_policy_sha256=policy,
    )


def _many(count: int, net_pnl: str = "10", start_day: int = 1) -> tuple[OutcomeRow, ...]:
    return tuple(
        _row(net_pnl=net_pnl, session=date(2026, 1, 1) + timedelta(days=index))
        for index in range(count)
    )


def test_empty_input_is_insufficient_not_zero():
    summary = summarize_outcomes("ALL", ())
    assert summary.verdict is PerformanceVerdict.INSUFFICIENT
    assert summary.win_rate is None
    assert summary.total_net_pnl == Decimal("0")


def test_small_sample_is_labelled_rather_than_reported():
    summary = summarize_outcomes("ALL", _many(MINIMUM_OUTCOMES_FOR_READING - 1))
    assert summary.verdict is PerformanceVerdict.INSUFFICIENT


def test_moderate_sample_is_indicative():
    summary = summarize_outcomes("ALL", _many(MINIMUM_OUTCOMES_FOR_READING + 5))
    assert summary.verdict is PerformanceVerdict.INDICATIVE


def test_large_sample_across_enough_sessions_is_measurable():
    summary = summarize_outcomes("ALL", _many(MINIMUM_OUTCOMES_FOR_CONFIDENCE + 10))
    assert summary.independent_sessions >= MINIMUM_INDEPENDENT_SESSIONS
    assert summary.verdict is PerformanceVerdict.MEASURABLE


def test_many_observations_on_few_sessions_stay_indicative():
    """Overlapping horizons inflate the count without adding independent evidence."""
    rows = tuple(_row(session=date(2026, 9, 1)) for _ in range(MINIMUM_OUTCOMES_FOR_CONFIDENCE + 50))
    summary = summarize_outcomes("ALL", rows)
    assert summary.priced_observations > MINIMUM_OUTCOMES_FOR_CONFIDENCE
    assert summary.independent_sessions == 1
    assert summary.verdict is PerformanceVerdict.INDICATIVE
    assert "FEW_INDEPENDENT_SESSIONS" in summary.reasons


def test_win_rate_and_mean_return_are_both_reported():
    """A high win rate with a negative mean is the classic short-premium shape."""
    rows = tuple(_row(net_pnl="1", net_return="0.01") for _ in range(9))
    rows += (_row(net_pnl="-50", net_return="-0.50"),)
    summary = summarize_outcomes("ALL", rows)
    assert summary.win_rate == pytest.approx(0.9)
    assert summary.mean_net_return < 0
    assert summary.total_net_pnl == Decimal("-41")


def test_profit_factor_uses_gross_gains_over_gross_losses():
    rows = (_row(net_pnl="30"), _row(net_pnl="-10"), _row(net_pnl="-5"))
    summary = summarize_outcomes("ALL", rows)
    assert summary.profit_factor == pytest.approx(30 / 15)


def test_profit_factor_is_none_without_losses():
    summary = summarize_outcomes("ALL", (_row(net_pnl="30"), _row(net_pnl="10")))
    assert summary.profit_factor is None


def test_unpriced_outcomes_are_excluded_and_flagged():
    rows = (_row(net_pnl="10"), _row(net_pnl=None))
    summary = summarize_outcomes("ALL", rows)
    assert summary.observations == 2
    assert summary.priced_observations == 1
    assert "UNPRICED_OUTCOMES_EXCLUDED" in summary.reasons


def test_mixed_valuation_policies_are_flagged():
    """Profit measured under two mark definitions is not one series."""
    rows = (_row(policy="a" * 64), _row(policy="b" * 64))
    summary = summarize_outcomes("ALL", rows)
    assert len(summary.valuation_policies) == 2
    assert "MULTIPLE_VALUATION_POLICIES" in summary.reasons


def test_single_valuation_policy_is_not_flagged():
    summary = summarize_outcomes("ALL", (_row(), _row()))
    assert "MULTIPLE_VALUATION_POLICIES" not in summary.reasons


def test_raw_totals_declare_they_are_not_baseline_adjusted():
    summary = summarize_outcomes("ALL", _many(MINIMUM_OUTCOMES_FOR_CONFIDENCE + 10))
    assert "NOT_BASELINE_ADJUSTED" in summary.reasons


def test_largest_gain_and_loss_are_reported():
    rows = (_row(net_pnl="120"), _row(net_pnl="-80"), _row(net_pnl="5"))
    summary = summarize_outcomes("ALL", rows)
    assert summary.largest_gain == Decimal("120")
    assert summary.largest_loss == Decimal("-80")


def test_grouping_splits_by_the_requested_key():
    rows = (
        _row(strategy="INCOME_WHEEL"),
        _row(strategy="SPREAD_RANGE_LOCATOR"),
        _row(strategy="INCOME_WHEEL"),
    )
    grouped = group_by(rows, "strategy_name")
    assert set(grouped) == {"INCOME_WHEEL", "SPREAD_RANGE_LOCATOR"}
    assert len(grouped["INCOME_WHEEL"]) == 2


def _pair(strategy_return: float, baseline_return: float, day: int) -> BaselinePair:
    return BaselinePair(
        strategy_name="DIRECTIONAL_LONG_PREMIUM",
        session_date=date(2026, 1, 1) + timedelta(days=day),
        measurement_type="CLOSE",
        strategy_return=strategy_return,
        baseline_return=baseline_return,
    )


def test_no_pairs_is_insufficient():
    comparison = compare_against_baseline("ALL", ())
    assert comparison.verdict is PerformanceVerdict.INSUFFICIENT
    assert "NO_BASELINE_PAIRS" in comparison.reasons
    assert comparison.period_observations == 0


def test_many_pairs_on_one_session_yield_too_few_periods_to_test():
    """Pairs are collapsed per (session, horizon), so a large pair count is not power.

    One session with two horizons gives two portfolio observations, which cannot support
    the test. Reporting the pair count alone next to a NaN p-value reads as a defect.
    """
    pairs = tuple(
        BaselinePair(
            strategy_name="SPREAD_RANGE_LOCATOR",
            session_date=date(2026, 9, 8),
            measurement_type=horizon,
            strategy_return=-0.07 + index * 1e-4,
            baseline_return=0.04,
        )
        for horizon in ("15MIN", "30MIN")
        for index in range(400)
    )
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.pairs == 800
    assert comparison.period_observations == 2
    assert comparison.independent_sessions == 1
    assert comparison.p_value != comparison.p_value  # NaN
    assert "TOO_FEW_PERIODS_TO_TEST" in comparison.reasons
    assert "MIXED_MEASUREMENT_TYPES" in comparison.reasons
    # The beat rate is over periods, which is why it lands on an exact 0% or 100%.
    assert comparison.beat_baseline_rate in (0.0, 1.0)


def test_enough_periods_produces_a_real_p_value():
    generator = random.Random(29)
    pairs = tuple(
        BaselinePair(
            strategy_name="SPREAD_RANGE_LOCATOR",
            session_date=date(2026, 1, 1) + timedelta(days=day),
            measurement_type="CLOSE",
            strategy_return=generator.gauss(0.03, 0.01),
            baseline_return=0.0,
        )
        for day in range(60)
    )
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.period_observations == 60
    assert comparison.p_value == comparison.p_value
    assert "TOO_FEW_PERIODS_TO_TEST" not in comparison.reasons


def test_mixed_measurement_types_never_publish_inferential_statistics():
    pairs = tuple(
        BaselinePair(
            strategy_name="SPREAD_RANGE_LOCATOR",
            session_date=date(2026, 1, 1) + timedelta(days=day),
            measurement_type=horizon,
            strategy_return=0.03 + day / 100000,
            baseline_return=0.0,
        )
        for day in range(60)
        for horizon in ("15MIN", "30MIN", "60MIN")
    )
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.period_observations == 180
    assert comparison.independent_sessions == 60
    assert comparison.measurement_types == ("15MIN", "30MIN", "60MIN")
    assert comparison.statistic != comparison.statistic
    assert comparison.p_value != comparison.p_value
    assert "MIXED_MEASUREMENT_TYPES" in comparison.reasons
    payload = performance_report._baseline_payload(comparison)
    assert payload["statistic"] is None
    assert payload["p_value"] is None
    json.dumps(payload, allow_nan=False)


def test_report_scope_reaches_every_query(monkeypatch, capsys):
    executions = []

    class Cursor:
        def execute(self, query, parameters):
            executions.append((query, parameters))

        def fetchall(self):
            return []

    @contextmanager
    def cursor_context():
        yield Cursor()

    monkeypatch.setattr(performance_report, "get_db_cursor", cursor_context)
    monkeypatch.setattr(
        performance_report,
        "_parse_args",
        lambda: SimpleNamespace(
            days=1,
            strategy="DIRECTIONAL_LONG_PREMIUM",
            horizon="15MIN",
            output_dir=Path("unused"),
            no_write=True,
        ),
    )

    assert performance_report.main() == 0
    capsys.readouterr()
    by_query = {query: parameters for query, parameters in executions}
    assert by_query[performance_report.SQL_COHORT][0:2] == ("15MIN", "15MIN")
    assert by_query[performance_report.SQL_COHORT][-2:] == (
        "DIRECTIONAL_LONG_PREMIUM",
        "DIRECTIONAL_LONG_PREMIUM",
    )
    assert by_query[performance_report.SQL_AVAILABILITY][-4:] == (
        "DIRECTIONAL_LONG_PREMIUM",
        "DIRECTIONAL_LONG_PREMIUM",
        "15MIN",
        "15MIN",
    )
    assert by_query[performance_report.SQL_SUPPRESSIONS][-2:] == (
        "DIRECTIONAL_LONG_PREMIUM",
        "DIRECTIONAL_LONG_PREMIUM",
    )


def test_a_rising_market_lifting_both_legs_shows_no_advantage():
    """The failure this exists to catch: profit that is direction, not selection."""
    generator = random.Random(7)
    pairs = tuple(
        _pair(move + 0.01, move + 0.01, day)
        for day, move in enumerate(generator.uniform(0.02, 0.20) for _ in range(120))
    )
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.mean_strategy_return > 0
    assert comparison.mean_advantage == pytest.approx(0.0, abs=1e-12)
    assert "NO_ADVANTAGE_OVER_BASELINE" in comparison.reasons


def test_genuine_selection_shows_a_positive_advantage():
    generator = random.Random(11)
    pairs = []
    for day in range(120):
        move = generator.uniform(-0.10, 0.10)
        pairs.append(_pair(move + 0.03 + generator.gauss(0, 0.005), move, day))
    comparison = compare_against_baseline("ALL", tuple(pairs))
    assert comparison.mean_advantage == pytest.approx(0.03, abs=0.005)
    assert comparison.beat_baseline_rate > 0.9
    assert comparison.p_value < 0.01
    assert "NO_ADVANTAGE_OVER_BASELINE" not in comparison.reasons


def test_losing_to_the_baseline_is_reported_as_such():
    generator = random.Random(13)
    pairs = tuple(
        _pair(move - 0.02, move, day)
        for day, move in enumerate(generator.uniform(0.0, 0.10) for _ in range(120))
    )
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.mean_advantage < 0
    assert "NO_ADVANTAGE_OVER_BASELINE" in comparison.reasons


def test_few_sessions_keeps_the_verdict_below_measurable():
    pairs = tuple(_pair(0.05, 0.01, 0) for _ in range(MINIMUM_OUTCOMES_FOR_CONFIDENCE + 20))
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.independent_sessions == 1
    assert comparison.verdict is PerformanceVerdict.INDICATIVE
    assert "FEW_INDEPENDENT_SESSIONS" in comparison.reasons


def test_same_session_contracts_cannot_manufacture_significance():
    pairs = tuple(
        _pair(0.10 + index / 10000, 0.01, 0)
        for index in range(200)
    )
    comparison = compare_against_baseline("ALL", pairs)
    assert comparison.pairs == 200
    assert comparison.independent_sessions == 1
    assert comparison.p_value != comparison.p_value


def test_overlapping_horizons_widen_the_p_value():
    """Correlated windows carry less evidence than their count suggests.

    The advantage itself must be autocorrelated for the correction to bite; a constant
    edge has no dispersion and the test statistic is degenerate.
    """
    generator = random.Random(17)
    shocks = [generator.gauss(0, 0.02) for _ in range(200)]
    smoothed_advantage = [
        sum(shocks[max(index - 9, 0) : index + 1]) / min(index + 1, 10)
        for index in range(200)
    ]
    pairs = []
    for index in range(200):
        baseline = generator.gauss(0, 0.05)
        pairs.append(_pair(baseline + 0.004 + smoothed_advantage[index], baseline, index))
    independent = compare_against_baseline("ALL", tuple(pairs), horizon_sessions=1)
    overlapping = compare_against_baseline("ALL", tuple(pairs), horizon_sessions=10)
    assert overlapping.p_value > independent.p_value
