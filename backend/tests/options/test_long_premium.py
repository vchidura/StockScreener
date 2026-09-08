import sys
from datetime import date, datetime, timedelta, timezone
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
from options.strategies.domain import StrategyContextStatus
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
    return OptionContractSnapshot(
        snapshot_id=uuid4(), contract_id=contract_id,
        contract_ticker=f"O:SPY{contract_id:08d}", underlyer="SPY", provider="polygon",
        contract_type=contract_type, expiration_date=expiration,
        expiration_cutoff=datetime(
            expiration.year, expiration.month, expiration.day, 20, 0, tzinfo=UTC
        ),
        calendar_dte=dte, time_to_expiration_years=max(dte, 1) / 365,
        strike=Decimal(str(strike)), shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN, spot=SPOT,
        spot_market_data_time=MARKET_TIME, bid=None, ask=None, midpoint=None,
        display_mark=Decimal(str(mark)), model_mark=Decimal(str(mark)),
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=day_volume, open_interest=open_interest,
        market_data_time=MARKET_TIME, first_observed_at=OBSERVED_TIME,
        revised_observed_at=None, local_iv=iv, local_gamma=0.02, local_delta=delta,
        local_theta_per_day=-0.01, local_vega_per_vol_point=0.1,
        local_rho_per_rate_point=0.001, intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal(str(mark)),
        single_contract_breakeven=(
            Decimal(str(strike)) + Decimal(str(mark))
            if contract_type is ContractType.CALL
            else Decimal(str(strike)) - Decimal(str(mark))
        ),
        provider_iv=None, provider_gamma=None, risk_free_rate=0.04, dividend_yield=0.0,
        iv_converged=True, iv_solver="NEWTON", iv_iteration_count=3,
        iv_price_error=1e-9, iv_failure_reason=None, model_version="bs-1",
        quality_flags=(), batch_id=BATCH, raw_payload_sha256="a" * 64,
        normalized_payload_sha256="b" * 64, revision=1,
    )


def _context(matrix_id):
    return StrategyContextSnapshot(
        context_snapshot_id=uuid4(), matrix_id=matrix_id, underlyer="SPY",
        market_data_time=MARKET_TIME, observed_time=OBSERVED_TIME,
        status=StrategyContextStatus.COMPLETE, daily_close=None, daily_ema_50=None,
        daily_input_bars=0, hourly_close=None, hourly_ema_20=None, hourly_input_bars=0,
        trend_state="NEUTRAL", earnings_blackout_state="UNAVAILABLE",
        fed_blackout_state="UNAVAILABLE", quote_spread_state="UNAVAILABLE",
        reason_codes=(), source_bar_keys=(), policy_version="phase2",
        policy_sha256="d" * 64,
    )


def _health(count):
    return ChainHealth(
        received_count=count, retained_count=count, catalog_coverage_fraction=1.0,
        mark_alignment_fraction=1.0, iv_convergence_fraction=1.0,
        unknown_reference_fraction=0.0, rejection_fraction=0.0,
        status="COMPLETE", reasons=(),
    )


def _scan(snapshots):
    policy, sha = _policy()
    engine = OptionStrategyEngine(policy, sha)
    matrix_id = uuid4()
    result = engine.scan(
        matrix_id, snapshots, _health(len(snapshots)), (), _context(matrix_id), (), ()
    )
    return tuple(
        candidate for candidate in result.candidates
        if candidate.strategy_name == "DIRECTIONAL_LONG_PREMIUM"
    )


def _selected(snapshots):
    return [c for c in _scan(snapshots) if c.status.value == "SELECTED"]


# A 30-day 0.45-delta call at 2.00 breaks even 2% away against a ~8.6% implied move.
def _viable_call(contract_id=1, dte=30):
    return _snapshot(contract_id, ContractType.CALL, 100, dte=dte, delta=0.45, mark=2.0)


def _viable_put(contract_id=2, dte=30):
    return _snapshot(contract_id, ContractType.PUT, 100, dte=dte, delta=-0.45, mark=2.0)


def test_emits_both_long_calls_and_long_puts():
    selected = _selected((_viable_call(), _viable_put()))
    structures = {candidate.structure_type.value for candidate in selected}
    assert structures == {"LONG_CALL", "LONG_PUT"}


def test_delta_band_is_enforced_from_policy():
    policy, _ = _policy()
    below = _snapshot(3, ContractType.CALL, 100, dte=30, delta=0.10, mark=2.0)
    above = _snapshot(4, ContractType.CALL, 100, dte=30, delta=0.90, mark=2.0)
    assert policy.long_premium.minimum_absolute_delta == 0.30
    assert policy.long_premium.maximum_absolute_delta == 0.60
    assert _selected((below, above)) == []


def test_breakeven_beyond_the_implied_move_is_rejected():
    # A far OTM call needs a move well past one sigma to break even.
    far = _snapshot(5, ContractType.CALL, 130, dte=30, delta=0.35, mark=6.0)
    assert _selected((far,)) == []


def test_rejection_is_reason_coded():
    far = _snapshot(5, ContractType.CALL, 130, dte=30, delta=0.35, mark=6.0)
    candidates = _scan((far,))
    assert candidates[0].status.value == "SUPPRESSED"
    assert "NO_LONG_PREMIUM_WITHIN_EXPECTED_MOVE" in candidates[0].reason_codes


def test_dte_lanes_are_labelled_and_separated():
    lanes = {}
    for contract_id, dte in ((1, 5), (2, 14), (3, 30)):
        selected = _selected((_viable_call(contract_id, dte=dte),))
        assert selected, dte
        lanes[dte] = selected[0].primary_evidence["dte_lane"]
    assert lanes == {5: "NEAR", 14: "SHORT", 30: "MEDIUM"}


def test_each_lane_and_side_is_capped_independently():
    snapshots = (
        _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.45, mark=2.0),
        _snapshot(2, ContractType.CALL, 101, dte=30, delta=0.42, mark=2.1),
        _snapshot(3, ContractType.PUT, 100, dte=30, delta=-0.45, mark=2.0),
        _snapshot(4, ContractType.CALL, 100, dte=5, delta=0.45, mark=0.8),
    )
    selected = _selected(snapshots)
    keys = {
        (c.primary_evidence["dte_lane"], c.structure_type.value) for c in selected
    }
    assert len(selected) == len(keys)


def test_liquidity_floors_exclude_thin_contracts():
    thin = _snapshot(
        6, ContractType.CALL, 100, dte=30, delta=0.45, mark=2.0,
        open_interest=10, day_volume=5,
    )
    assert _selected((thin,)) == []


def test_evidence_records_the_breakeven_arithmetic():
    evidence = _selected((_viable_call(),))[0].primary_evidence
    assert evidence["breakeven"] == "102.0"
    assert evidence["required_move_fraction"] == pytest.approx(0.02)
    assert evidence["expected_move_fraction"] > evidence["required_move_fraction"]
    assert evidence["breakeven_expected_move_ratio"] < 1.0
    assert evidence["directional_thesis"] == "BULLISH"


def test_iv_context_is_reported_unavailable_rather_than_ignored():
    evidence = _selected((_viable_call(),))[0].primary_evidence
    assert evidence["iv_context"] is None
    assert evidence["iv_context_reason"] == "INSUFFICIENT_COMPLETED_SESSION_HISTORY"


def test_ranking_prefers_the_cheapest_breakeven_relative_to_implied_move():
    near = _snapshot(1, ContractType.CALL, 100, dte=30, delta=0.45, mark=1.0)
    far = _snapshot(2, ContractType.CALL, 100, dte=30, delta=0.45, mark=3.0)
    selected = _selected((far, near))
    assert selected[0].legs[0].contract_id == 1


def test_candidates_remain_blocked_by_the_gate_ledger():
    candidate = _selected((_viable_call(),))[0]
    assert candidate.execution_eligibility is None
    assert "QUOTE_LIQUIDITY_NOT_AVAILABLE" in candidate.reason_codes
    assert "PAPER_RISK_ENGINE_NOT_IMPLEMENTED" in candidate.reason_codes


def test_zero_dte_is_left_to_the_squeeze_module():
    policy, _ = _policy()
    assert policy.long_premium.minimum_dte >= 1
    same_day = _snapshot(7, ContractType.CALL, 100, dte=0, delta=0.45, mark=0.4)
    assert _selected((same_day,)) == []
