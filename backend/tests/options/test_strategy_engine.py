import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import numpy as np

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.chain_analysis import build_chain_health
from options.config import load_option_runtime_configuration
from options.domain import (
    ContractType,
    ExerciseStyle,
    MarkSource,
    OptionContractSnapshot,
    OptionTradeEvent,
    TradeClassificationStatus,
)
from options.strategies.domain import CandidateStatus, StrategyContextSnapshot, StrategyContextStatus
from options.strategies.engine import (
    OptionStrategyEngine,
    _quadratic_coefficient_payload,
)


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
OBSERVED_TIME = MARKET_TIME + timedelta(minutes=15)
EXPIRATION = date(2026, 9, 18)
HASH = "a" * 64


def snapshot(
    contract_id: int,
    strike: str,
    mark: str,
    local_iv: float,
    contract_type: ContractType = ContractType.PUT,
) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        snapshot_id=uuid4(),
        contract_id=contract_id,
        contract_ticker=f"O:SPY260918P{contract_id:08d}",
        underlyer="SPY",
        provider="polygon",
        contract_type=contract_type,
        expiration_date=EXPIRATION,
        expiration_cutoff=datetime(2026, 9, 18, 20, 0, tzinfo=UTC),
        calendar_dte=21,
        time_to_expiration_years=21 / 365,
        strike=Decimal(strike),
        shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN,
        spot=Decimal("100"),
        spot_market_data_time=MARKET_TIME,
        bid=None,
        ask=None,
        midpoint=None,
        display_mark=Decimal(mark),
        model_mark=Decimal(mark),
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=50,
        open_interest=200,
        market_data_time=MARKET_TIME,
        first_observed_at=OBSERVED_TIME,
        revised_observed_at=None,
        local_iv=local_iv,
        local_gamma=0.02,
        local_delta=-0.30 if contract_type is ContractType.PUT else 0.30,
        local_theta_per_day=-0.05,
        local_vega_per_vol_point=0.10,
        local_rho_per_rate_point=-0.02,
        intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal(mark),
        single_contract_breakeven=Decimal(strike) - Decimal(mark),
        provider_iv=None,
        provider_gamma=None,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        iv_converged=True,
        iv_solver="NEWTON",
        iv_iteration_count=4,
        iv_price_error=1e-8,
        iv_failure_reason=None,
        model_version="black_scholes_european_v1",
        quality_flags=(),
        batch_id=uuid4(),
        raw_payload_sha256=HASH,
        normalized_payload_sha256=HASH,
    )


def strategy_context(matrix_id):
    return StrategyContextSnapshot(
        context_snapshot_id=uuid4(),
        matrix_id=matrix_id,
        underlyer="SPY",
        market_data_time=MARKET_TIME,
        observed_time=OBSERVED_TIME,
        status=StrategyContextStatus.DEGRADED,
        daily_close=Decimal("100"),
        daily_ema_50=Decimal("99"),
        daily_input_bars=100,
        hourly_close=Decimal("100"),
        hourly_ema_20=Decimal("99.5"),
        hourly_input_bars=20,
        trend_state="BULLISH",
        earnings_blackout_state="NOT_APPLICABLE",
        fed_blackout_state="UNAVAILABLE",
        quote_spread_state="NOT_AVAILABLE",
        reason_codes=("FED_CALENDAR_UNAVAILABLE",),
        source_bar_keys=("daily:2026-08-28", "hourly:2026-08-28T19:00:00Z"),
        policy_version="developer_v2",
        policy_sha256=HASH,
    )


def test_healthy_matrix_selects_deterministic_wheel_candidates_and_scenarios():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()
    rows = (
        snapshot(1, "95", "2.50", 0.35),
        snapshot(2, "90", "1.25", 0.30),
        snapshot(3, "97", "3.00", 0.40),
        snapshot(4, "85", "0.75", 0.25),
    )
    health = build_chain_health(
        received_count=4,
        retained_count=4,
        catalog_matched_count=4,
        mark_aligned_count=4,
        iv_attempt_count=4,
        iv_converged_count=4,
        unknown_reference_count=0,
        reference_drift_failed=False,
    )

    first = engine.scan(matrix_id, rows, health, (), strategy_context(matrix_id))
    second = engine.scan(matrix_id, tuple(reversed(rows)), health, (), strategy_context(matrix_id))
    first_wheel = [item for item in first.candidates if item.strategy_name == "INCOME_WHEEL"]
    second_wheel = [item for item in second.candidates if item.strategy_name == "INCOME_WHEEL"]

    assert [item.status for item in first_wheel] == [CandidateStatus.SELECTED] * 3
    assert [item.candidate_id for item in first_wheel] == [item.candidate_id for item in second_wheel]
    assert [item.legs[0].contract_id for item in first_wheel] == [3, 1, 2]
    assert all(item.execution_eligibility is None for item in first_wheel)
    assert all("FED_CALENDAR_UNAVAILABLE" in item.reason_codes for item in first_wheel)
    assert len([item for item in first.scenarios if item.candidate_id == first_wheel[0].candidate_id]) == 35

    with pytest.raises(ValueError, match="future-visible"):
        replace(
            first_wheel[0],
            market_data_time=MARKET_TIME - timedelta(seconds=1),
        )


def test_smile_fit_coefficients_match_durable_object_contract():
    payload = _quadratic_coefficient_payload(np.asarray([1.5, -0.25, 0.3]))

    assert payload == {
        "quadratic": 1.5,
        "linear": -0.25,
        "intercept": 0.3,
    }


def test_failed_chain_persists_one_suppression_for_each_registered_strategy():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()
    rows = (snapshot(1, "95", "2.50", 0.35),)
    health = build_chain_health(
        received_count=1,
        retained_count=1,
        catalog_matched_count=1,
        mark_aligned_count=0,
        iv_attempt_count=0,
        iv_converged_count=0,
        unknown_reference_count=0,
        reference_drift_failed=False,
    )

    result = engine.scan(matrix_id, rows, health, (), strategy_context(matrix_id))

    assert len(result.candidates) == 6
    assert all(item.status is CandidateStatus.SUPPRESSED for item in result.candidates)
    assert all("DATA_QUALITY_GATE_FAILED" in item.reason_codes for item in result.candidates)
    assert result.scenarios == ()


def test_sweep_like_cluster_is_research_only_and_preserves_event_keys():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()
    call = snapshot(10, "105", "1.50", 0.30, ContractType.CALL)
    health = build_chain_health(
        received_count=1,
        retained_count=1,
        catalog_matched_count=1,
        mark_aligned_count=1,
        iv_attempt_count=1,
        iv_converged_count=1,
        unknown_reference_count=0,
        reference_drift_failed=False,
    )
    trades = tuple(
        OptionTradeEvent(
            trade_event_id=uuid4(),
            provider="polygon",
            contract_id=call.contract_id,
            contract_ticker=call.contract_ticker,
            underlyer="SPY",
            sip_timestamp=MARKET_TIME - timedelta(seconds=120 - index * 10),
            sequence_number=index,
            participant_timestamp=None,
            first_observed_at=OBSERVED_TIME,
            revised_observed_at=None,
            exchange=1 + index % 2,
            conditions=(),
            correction=None,
            price=Decimal("5"),
            size=100,
            shares_per_contract=100,
            notional=Decimal("50000"),
            payload_sha256=f"{index:064x}",
            raw_batch_id=uuid4(),
            classification_status=TradeClassificationStatus.INCLUDED,
        )
        for index in range(10)
    )

    result = engine.scan(
        matrix_id,
        (call,),
        health,
        (),
        strategy_context(matrix_id),
        trades,
    )
    sweep = [item for item in result.candidates if item.strategy_name == "SWEEP_LIKE_CLUSTER"]

    assert len(sweep) == 1
    assert sweep[0].status is CandidateStatus.SELECTED
    assert sweep[0].candidate_kind.value == "RESEARCH_ONLY"
    assert sweep[0].execution_eligibility is None
    assert sweep[0].primary_evidence["qualifying_print_count"] == 10
    assert len(sweep[0].primary_evidence["contributing_event_keys"]) == 10
    assert sweep[0].primary_evidence["aggressor_side"] is None


def test_linked_equity_context_routes_gamma_to_qualified_direction():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()
    call = replace(
        snapshot(20, "100", "1.50", 0.30, ContractType.CALL),
        calendar_dte=0, local_gamma=0.10, day_volume=400, open_interest=100,
    )
    put = replace(
        snapshot(21, "100", "1.50", 0.30, ContractType.PUT),
        calendar_dte=0, local_gamma=0.10, day_volume=400, open_interest=100,
    )
    context = replace(
        strategy_context(matrix_id),
        equity_context_snapshot_id=uuid4(),
        equity_context_status="COMPLETE",
        qualified_direction="BULLISH",
    )

    candidates = engine._gamma_squeeze(matrix_id, (call, put), context)

    selected = [item for item in candidates if item.status is CandidateStatus.SELECTED]
    assert len(selected) == 1
    assert selected[0].legs[0].contract_type is ContractType.CALL
    assert selected[0].primary_evidence["directional_thesis"] == "BULLISH"


def test_linked_bearish_context_suppresses_income_wheel():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()
    context = replace(
        strategy_context(matrix_id),
        equity_context_snapshot_id=uuid4(),
        equity_context_status="COMPLETE",
        qualified_direction="BEARISH",
    )

    candidates = engine._income_wheel(
        matrix_id,
        (snapshot(30, "95", "2.50", 0.35),),
        context,
    )

    assert len(candidates) == 1
    assert candidates[0].status is CandidateStatus.SUPPRESSED
    assert "QUALIFIED_EQUITY_DIRECTION_OPPOSES_STRATEGY" in candidates[0].reason_codes


def test_linked_context_without_qualified_direction_fails_closed():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()
    context = replace(
        strategy_context(matrix_id),
        equity_context_snapshot_id=uuid4(),
        equity_context_status="DEGRADED",
        qualified_direction=None,
    )

    candidates = engine._income_wheel(
        matrix_id,
        (snapshot(40, "95", "2.50", 0.35),),
        context,
    )

    assert candidates[0].status is CandidateStatus.SUPPRESSED
    assert "QUALIFIED_EQUITY_DIRECTION_UNAVAILABLE" in candidates[0].reason_codes