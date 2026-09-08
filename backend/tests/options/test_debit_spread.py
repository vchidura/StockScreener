import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.chain_analysis import ChainHealth
from options.config import load_strategy_policy
from options.domain import ContractType, ExerciseStyle, MarkSource, OptionContractSnapshot
from options.strategies.context import StrategyContextSnapshot
from options.strategies.domain import CandidateKind, StrategyContextStatus
from options.strategies.engine import OptionStrategyEngine

UTC = timezone.utc
MARKET_TIME = datetime(2026, 9, 4, 17, 0, tzinfo=UTC)
OBSERVED_TIME = datetime(2026, 9, 4, 17, 15, tzinfo=UTC)
BATCH = uuid4()
POLICY_PATH = BACKEND_DIR / "options" / "policies" / "strategy_v1.json"
SPOT = Decimal("100")


def _policy():
    artifact = load_strategy_policy(POLICY_PATH)
    return artifact.policy, artifact.sha256


def _snapshot(
    contract_id,
    contract_type,
    strike,
    *,
    dte,
    delta,
    mark,
    iv=0.30,
    open_interest=5000,
    day_volume=2000,
):
    expiration = MARKET_TIME.date() + timedelta(days=dte)
    strike = Decimal(str(strike))
    mark = Decimal(str(mark))
    return OptionContractSnapshot(
        snapshot_id=uuid4(), contract_id=contract_id,
        contract_ticker=f"O:SPY{contract_id:08d}", underlyer="SPY", provider="polygon",
        contract_type=contract_type, expiration_date=expiration,
        expiration_cutoff=datetime(
            expiration.year, expiration.month, expiration.day, 20, 0, tzinfo=UTC
        ),
        calendar_dte=dte, time_to_expiration_years=max(dte, 1) / 365,
        strike=strike, shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN, spot=SPOT,
        spot_market_data_time=MARKET_TIME, bid=None, ask=None, midpoint=None,
        display_mark=mark, model_mark=mark,
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=day_volume, open_interest=open_interest,
        market_data_time=MARKET_TIME, first_observed_at=OBSERVED_TIME,
        revised_observed_at=None, local_iv=iv, local_gamma=0.02, local_delta=delta,
        local_theta_per_day=-0.01, local_vega_per_vol_point=0.1,
        local_rho_per_rate_point=0.001, intrinsic_value=Decimal("0"),
        extrinsic_value=mark,
        single_contract_breakeven=(
            strike + mark if contract_type is ContractType.CALL else strike - mark
        ),
        provider_iv=None, provider_gamma=None, risk_free_rate=0.04, dividend_yield=0.0,
        iv_converged=True, iv_solver="NEWTON", iv_iteration_count=3,
        iv_price_error=1e-9, iv_failure_reason=None, model_version="bs-1",
        quality_flags=(), batch_id=BATCH, raw_payload_sha256="a" * 64,
        normalized_payload_sha256="b" * 64, revision=1,
    )


def _context(matrix_id, direction=None):
    return StrategyContextSnapshot(
        context_snapshot_id=uuid4(), matrix_id=matrix_id, underlyer="SPY",
        market_data_time=MARKET_TIME, observed_time=OBSERVED_TIME,
        status=StrategyContextStatus.COMPLETE, daily_close=None, daily_ema_50=None,
        daily_input_bars=0, hourly_close=None, hourly_ema_20=None, hourly_input_bars=0,
        trend_state="NEUTRAL", earnings_blackout_state="UNAVAILABLE",
        fed_blackout_state="UNAVAILABLE", quote_spread_state="UNAVAILABLE",
        reason_codes=(), source_bar_keys=(), policy_version="phase2",
        policy_sha256="d" * 64,
        equity_context_snapshot_id=uuid4() if direction else None,
        equity_context_status="COMPLETE" if direction else None,
        qualified_direction=direction,
    )


def _health(count):
    return ChainHealth(
        received_count=count, retained_count=count, catalog_coverage_fraction=1.0,
        mark_alignment_fraction=1.0, iv_convergence_fraction=1.0,
        unknown_reference_fraction=0.0, rejection_fraction=0.0,
        status="COMPLETE", reasons=(),
    )


def _scan(snapshots, direction=None):
    policy, sha = _policy()
    engine = OptionStrategyEngine(policy, sha)
    matrix_id = uuid4()
    result = engine.scan(
        matrix_id, snapshots, _health(len(snapshots)), (),
        _context(matrix_id, direction), (), (),
    )
    return tuple(
        candidate for candidate in result.candidates
        if candidate.strategy_name == "DIRECTIONAL_DEBIT_SPREAD"
    )


def _selected(snapshots, direction=None):
    return [c for c in _scan(snapshots, direction) if c.status.value == "SELECTED"]


# Spot 100, 30 DTE at 30% IV implies a ~8.6% one-sigma move. The 100/103 call spread
# costs 1.30, breaks even at 101.30 (1.3%), and pays 1.70 on a 3% move.
def _call_spread():
    return (
        _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.55, mark=2.5),
        _snapshot(2, ContractType.CALL, 103, dte=30, delta=0.35, mark=1.2),
    )


def _put_spread():
    return (
        _snapshot(3, ContractType.PUT, 100, dte=30, delta=-0.55, mark=2.5),
        _snapshot(4, ContractType.PUT, 97, dte=30, delta=-0.35, mark=1.2),
    )


def test_emits_both_call_and_put_debit_verticals():
    selected = _selected(_call_spread() + _put_spread())
    structures = {candidate.structure_type.value for candidate in selected}
    assert structures == {"CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL"}


def test_structure_is_a_two_leg_debit_with_bounded_loss():
    candidate = _selected(_call_spread())[0]
    assert candidate.candidate_kind is CandidateKind.MULTI_LEG
    assert candidate.structure_risk_class.value == "PREMIUM_AT_RISK_DEBIT"
    assert len(candidate.legs) == 2
    assert candidate.legs[0].side.value == "BUY"
    assert candidate.legs[1].side.value == "SELL"
    assert candidate.net_premium < 0
    assert candidate.maximum_loss == Decimal("130")
    assert candidate.maximum_profit == Decimal("170")
    assert candidate.capital_at_risk == Decimal("130")


def test_short_wing_is_always_further_out_of_the_money():
    for candidate in _selected(_call_spread() + _put_spread()):
        long_leg, short_leg = candidate.legs
        if long_leg.contract_type is ContractType.CALL:
            assert short_leg.strike > long_leg.strike
        else:
            assert short_leg.strike < long_leg.strike


def test_return_on_risk_floor_rejects_a_spread_that_pays_too_little():
    policy, _ = _policy()
    assert policy.debit_spread.minimum_return_on_risk == 1.0
    # 100/101 for 0.60 risks 60 to make 40, a return on risk of 0.67.
    thin = (
        _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.55, mark=2.5),
        _snapshot(2, ContractType.CALL, 101, dte=30, delta=0.40, mark=1.9),
    )
    assert _selected(thin) == []


def test_breakeven_beyond_the_implied_move_is_rejected():
    # A 106/109 spread needs a 7.2% move to break even against a 8.6% implied move,
    # a ratio of 0.84 versus the 0.75 ceiling.
    far = (
        _snapshot(1, ContractType.CALL, 106, dte=30, delta=0.50, mark=2.0),
        _snapshot(2, ContractType.CALL, 109, dte=30, delta=0.30, mark=0.8),
    )
    assert _selected(far) == []


def test_unreachable_profit_target_is_rejected():
    # 10 DTE at 15% IV implies a 2.5% move; a short strike 4% away only pays in full
    # on a 1.6 sigma move even though the breakeven itself is close.
    unreachable = (
        _snapshot(1, ContractType.CALL, 101, dte=10, delta=0.50, mark=0.50, iv=0.15),
        _snapshot(2, ContractType.CALL, 104, dte=10, delta=0.20, mark=0.05, iv=0.15),
    )
    assert _selected(unreachable) == []
    reachable = (
        _snapshot(1, ContractType.CALL, 101, dte=10, delta=0.50, mark=0.50, iv=0.15),
        _snapshot(2, ContractType.CALL, 102, dte=10, delta=0.35, mark=0.20, iv=0.15),
    )
    assert _selected(reachable)


def test_width_band_is_enforced_from_policy():
    policy, _ = _policy()
    assert policy.debit_spread.minimum_width_fraction == 0.005
    narrow = (
        _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.55, mark=2.5),
        _snapshot(2, ContractType.CALL, "100.2", dte=30, delta=0.52, mark=2.4),
    )
    assert _selected(narrow) == []


def test_long_leg_delta_band_is_enforced_from_policy():
    policy, _ = _policy()
    assert policy.debit_spread.minimum_long_absolute_delta == 0.45
    assert policy.debit_spread.maximum_long_absolute_delta == 0.70
    shallow = (
        _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.20, mark=2.5),
        _snapshot(2, ContractType.CALL, 103, dte=30, delta=0.10, mark=1.2),
    )
    assert _selected(shallow) == []


def test_both_legs_must_clear_the_liquidity_floors():
    thin_short = (
        _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.55, mark=2.5),
        _snapshot(
            2, ContractType.CALL, 103, dte=30, delta=0.35, mark=1.2,
            open_interest=10, day_volume=1,
        ),
    )
    assert _selected(thin_short) == []


def test_qualified_equity_direction_filters_the_side():
    selected = _selected(_call_spread() + _put_spread(), direction="BEARISH")
    assert {c.structure_type.value for c in selected} == {"PUT_DEBIT_VERTICAL"}


def test_each_lane_and_side_is_capped_independently():
    policy, _ = _policy()
    assert policy.debit_spread.maximum_candidates_per_lane_side == 1
    crowded = _call_spread() + (
        _snapshot(5, ContractType.CALL, 99, dte=30, delta=0.60, mark=3.1),
        _snapshot(6, ContractType.CALL, 102, dte=30, delta=0.40, mark=1.6),
    )
    selected = _selected(crowded)
    assert len(selected) == 1


def test_profit_target_must_sit_inside_the_implied_move():
    policy, _ = _policy()
    assert policy.debit_spread.maximum_target_expected_move_ratio == 1.0


def test_ranking_prefers_the_most_leverage_with_a_reachable_target():
    # Both spreads qualify; 100/105 risks the same kind of dollar for more payoff.
    wider = _call_spread() + (
        _snapshot(5, ContractType.CALL, 105, dte=30, delta=0.25, mark=0.6),
    )
    candidate = _selected(wider)[0]
    assert candidate.primary_metric_name == "return_on_risk"
    assert [str(leg.strike) for leg in candidate.legs] == ["100", "105"]
    assert candidate.return_on_risk == pytest.approx(310 / 190)


def test_primary_metric_is_the_return_on_risk():
    candidate = _selected(_call_spread())[0]
    assert candidate.primary_metric_value == pytest.approx(170 / 130)
    assert candidate.rank_components["breakeven_expected_move_ratio"] == pytest.approx(
        0.1511, abs=1e-3
    )


def test_evidence_records_the_spread_arithmetic():
    evidence = _selected(_call_spread())[0].primary_evidence
    assert evidence["long_strike"] == "100"
    assert evidence["short_strike"] == "103"
    assert evidence["breakeven"] == "101.3"
    assert evidence["net_debit_per_contract"] == "130.0"
    assert evidence["required_move_fraction"] == pytest.approx(0.013)
    assert evidence["expected_move_fraction"] == pytest.approx(0.086, abs=1e-3)
    assert evidence["target_expected_move_ratio"] < 1.0
    assert evidence["directional_thesis"] == "BULLISH"
    assert evidence["iv_context"] is None
    assert evidence["iv_context_reason"] == "INSUFFICIENT_COMPLETED_SESSION_HISTORY"


def test_rejection_is_reason_coded():
    candidates = _scan((
        _snapshot(1, ContractType.CALL, 130, dte=30, delta=0.10, mark=0.2),
    ))
    assert candidates[0].status.value == "SUPPRESSED"
    assert "NO_DEBIT_SPREAD_WITHIN_EXPECTED_MOVE" in candidates[0].reason_codes


def test_candidates_remain_blocked_by_the_gate_ledger():
    candidate = _selected(_call_spread())[0]
    assert candidate.execution_eligibility is None
    assert "QUOTE_LIQUIDITY_NOT_AVAILABLE" in candidate.reason_codes
    assert "PAPER_RISK_ENGINE_NOT_IMPLEMENTED" in candidate.reason_codes


def test_never_emits_a_credit_structure():
    selected = _selected(_call_spread() + _put_spread())
    assert selected
    for candidate in selected:
        assert candidate.net_premium < 0
        assert candidate.maximum_loss is not None and candidate.maximum_loss > 0
        assert candidate.maximum_profit is not None and candidate.maximum_profit > 0
