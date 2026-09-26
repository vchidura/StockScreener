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
from options.strategies.domain import CandidateStatus, StrategyContextSnapshot, StrategyContextStatus, StructureType
from options.strategies.engine import (
    OptionStrategyEngine,
    _quadratic_coefficient_payload,
)
from options.strategies.registry import STRATEGY_REGISTRY, build_discovery_catalog, discovery_categories


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
OBSERVED_TIME = MARKET_TIME + timedelta(minutes=15)
EXPIRATION = date(2026, 9, 18)
HASH = "a" * 64


def test_forward_admission_is_versioned_and_never_resets_source_age():
    from types import SimpleNamespace
    from options.config import load_strategy_policy, StrategyPolicy

    legacy = load_strategy_policy(BACKEND_DIR / "options/policies/strategy_v1.json")
    forward = load_strategy_policy(BACKEND_DIR / "options/policies/strategy_technical_forward_v1.json")
    assert legacy.sha256 == "c4a46268cbef4cb44994fe5524dc2499ef90d36853cd3d50b07a3126a31e6bc6"
    assert forward.sha256 != legacy.sha256
    old = OptionStrategyEngine(legacy.policy, legacy.sha256)
    new = OptionStrategyEngine(forward.policy, forward.sha256)
    clock = datetime(2026, 9, 18, 15, 0, tzinfo=UTC)
    context = SimpleNamespace(market_data_time=clock, observed_time=clock + timedelta(minutes=20))
    legs = (SimpleNamespace(source_market_time=clock - timedelta(minutes=2)),)
    assert old._candidate_deadline(context, "DIRECTIONAL_LONG_PREMIUM", legs) == clock + timedelta(minutes=15)
    assert new._candidate_deadline(context, "DIRECTIONAL_LONG_PREMIUM", legs) == clock + timedelta(minutes=28)
    context.observed_time += timedelta(minutes=30)
    assert new._candidate_deadline(context, "DIRECTIONAL_LONG_PREMIUM", legs) == clock + timedelta(minutes=28)
    assert new._candidate_deadline(context, "INCOME_WHEEL", legs) == clock + timedelta(minutes=15)
    context.market_data_time = datetime(2026, 9, 18, 19, 50, tzinfo=UTC)
    assert new._candidate_deadline(context, "DIRECTIONAL_LONG_PREMIUM", (SimpleNamespace(source_market_time=context.market_data_time),)) == datetime(2026, 9, 18, 20, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="distinct strategy"):
        StrategyPolicy.model_validate({**forward.policy.model_dump(), "strategy_version": "phase2_v4"})


def test_o1_participation_anchor_policy_has_a_distinct_version():
    from options.config import load_strategy_policy, StrategyPolicy

    anchored = load_strategy_policy(
        BACKEND_DIR / "options/policies/strategy_technical_forward_v2.json"
    )

    assert anchored.policy.strategy_version == "phase2_v6_o1_activity_anchor"
    assert anchored.policy.forward_admission is not None
    assert anchored.policy.participation_anchor.minimum_volume_oi_ratio == 3.0
    assert anchored.policy.participation_anchor.source_basis == (
        "CHAIN_VOLUME_OI_REVALIDATED_BY_DATED_OI"
    )
    with pytest.raises(ValueError, match="participation anchoring"):
        StrategyPolicy.model_validate({
            **anchored.policy.model_dump(),
            "strategy_version": "phase2_v5_technical_forward",
        })


def test_o3_credit_policy_is_distinct_and_preserves_o1_anchor():
    from options.config import load_strategy_policy, StrategyPolicy

    v1 = load_strategy_policy(BACKEND_DIR / "options/policies/strategy_o3_credit_v1.json")
    policy = load_strategy_policy(BACKEND_DIR / "options/policies/strategy_o3_credit_v2.json")
    assert v1.sha256 == "0850c29d76ebd29055cb9b6532f0462b06bb1c4f188c11fd0c1ace390f3b83e9"
    assert v1.policy.strategy_version == "phase2_v7_o3_credit"
    assert v1.policy.credit.maximum_source_age_seconds is None
    assert policy.policy.strategy_version == "phase2_v8_o3_credit_timing"
    assert policy.policy.participation_anchor.minimum_volume_oi_ratio == 3.0
    assert policy.policy.credit.minimum_entry_dte == 7
    assert policy.policy.credit.maximum_entry_dte == 30
    assert policy.policy.credit.maximum_source_age_seconds == 1800
    assert policy.policy.credit.source_session_cap == "SOURCE_SESSION_CLOSE"
    assert policy.policy.credit.pricing_basis == "ORIGINAL_COHERENT_MODEL_MARKS"
    assert policy.policy.credit.output == "QUALIFIED_INDICATIVE_RESEARCH"
    with pytest.raises(ValueError, match="O3 credit"):
        StrategyPolicy.model_validate({**policy.policy.model_dump(), "strategy_version": "phase2_v6_o1_activity_anchor"})


def test_o3_credit_deadline_uses_source_time_and_session_close():
    from types import SimpleNamespace
    from options.config import load_strategy_policy

    v1 = load_strategy_policy(BACKEND_DIR / "options/policies/strategy_o3_credit_v1.json")
    v2 = load_strategy_policy(BACKEND_DIR / "options/policies/strategy_o3_credit_v2.json")
    old = OptionStrategyEngine(v1.policy, v1.sha256)
    new = OptionStrategyEngine(v2.policy, v2.sha256)
    source_time = datetime(2026, 9, 25, 18, 30, tzinfo=UTC)
    context = SimpleNamespace(market_data_time=source_time, observed_time=source_time + timedelta(minutes=15, seconds=22))
    legs = (SimpleNamespace(source_market_time=source_time),)
    assert old._candidate_deadline(context, "SPREAD_RANGE_LOCATOR", legs, StructureType.PUT_CREDIT_VERTICAL) == source_time + timedelta(minutes=15)
    assert new._candidate_deadline(context, "SPREAD_RANGE_LOCATOR", legs, StructureType.PUT_CREDIT_VERTICAL) == source_time + timedelta(minutes=30)
    context.observed_time += timedelta(minutes=10)
    assert new._candidate_deadline(context, "SPREAD_RANGE_LOCATOR", legs, StructureType.CALL_CREDIT_VERTICAL) == source_time + timedelta(minutes=30)
    assert new._candidate_deadline(context, "SPREAD_RANGE_LOCATOR", legs, StructureType.IRON_CONDOR) == source_time + timedelta(minutes=15)
    closing_time = datetime(2026, 9, 25, 19, 50, tzinfo=UTC)
    context.market_data_time = closing_time
    legs = (SimpleNamespace(source_market_time=closing_time),)
    assert new._candidate_deadline(context, "SPREAD_RANGE_LOCATOR", legs, StructureType.CALL_CREDIT_VERTICAL) == datetime(2026, 9, 25, 20, 0, tzinfo=UTC)


def test_discovery_catalog_covers_registered_models_without_execution_claims():
    catalog = build_discovery_catalog()
    assert catalog["version"] == "option_discovery_v1"
    assert [model["id"] for model in catalog["models"]] == [
        registration.strategy_name for registration in STRATEGY_REGISTRY
    ]
    categories = {category["id"] for category in catalog["categories"]}
    observations = {
        model["id"] for model in catalog["models"]
        if model["output_kind"] == "OBSERVATION"
    }
    assert observations == {
        "SWEEP_LIKE_CLUSTER", "VOLUME_OI_ANOMALY", "VOLATILITY_SMILE_DISTORTION",
    }
    for model in catalog["models"]:
        assert "execution_eligibility" not in model
        assert "success_probability" not in model
        for structure in model["structures"]:
            assert structure["category_ids"]
            assert set(structure["category_ids"]) <= categories


@pytest.mark.parametrize(("structure", "categories"), [
    (StructureType.CASH_SECURED_PUT, ("INCOME",)),
    (StructureType.PUT_CREDIT_VERTICAL, ("DEFINED_RISK_INCOME",)),
    (StructureType.CALL_CREDIT_VERTICAL, ("DEFINED_RISK_INCOME",)),
    (StructureType.IRON_CONDOR, ("DEFINED_RISK_INCOME", "NEUTRAL_VOL")),
    (StructureType.CALL_BUTTERFLY, ("NEUTRAL_VOL",)),
    (StructureType.PUT_BUTTERFLY, ("NEUTRAL_VOL",)),
    (StructureType.CALL_DEBIT_VERTICAL, ("MOMENTUM",)),
    (StructureType.PUT_DEBIT_VERTICAL, ("MOMENTUM",)),
])
def test_discovery_categories_follow_structure_not_broad_persona(structure, categories):
    assert discovery_categories(structure) == categories


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
        replace(snapshot(1, "95", "2.50", 0.35), calendar_dte=22),
        replace(snapshot(2, "90", "1.25", 0.30), calendar_dte=22),
        replace(snapshot(3, "97", "3.00", 0.40), calendar_dte=22),
        replace(snapshot(4, "85", "0.75", 0.25), calendar_dte=22),
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

    # One suppression per registered strategy, so adding a module cannot silently
    # bypass the shared chain-health gate.
    assert len(result.candidates) == len(STRATEGY_REGISTRY)
    assert {item.strategy_name for item in result.candidates} == {
        registration.strategy_name for registration in STRATEGY_REGISTRY
    }
    assert all(item.status is CandidateStatus.SUPPRESSED for item in result.candidates)
    assert all("DATA_QUALITY_GATE_FAILED" in item.reason_codes for item in result.candidates)
    assert result.scenarios == ()
    assert {candidate_id for candidate_id, _ in result.candidate_gate_ledgers} == {
        candidate.candidate_id for candidate in result.candidates
    }
    assert all(
        len(ledger.results) == 6
        for _, ledger in result.candidate_gate_ledgers
    )


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


def test_income_wheel_rejects_entry_at_or_below_exit_dte():
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test"}, BACKEND_DIR)
    engine = OptionStrategyEngine(
        configuration.strategy_policy,
        configuration.strategy_policy_sha256,
    )
    matrix_id = uuid4()

    candidates = engine._income_wheel(
        matrix_id,
        (snapshot(35, "95", "2.50", 0.35),),
        strategy_context(matrix_id),
    )

    assert len(candidates) == 1
    assert candidates[0].status is CandidateStatus.SUPPRESSED
    assert "NO_WHEEL_CONTRACT_ABOVE_EXIT_DTE" in candidates[0].reason_codes


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