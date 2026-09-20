from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from equity.behavior import StockBehaviorSnapshot
from options.domain import ContractType
from options.outcome_contracts import (
    PACKAGE_ASSESSMENT_POLICY,
    CalibrationEvidence,
    OptionOutcomeBinding,
    OptionOutcomeEstimate,
    OptionPackageAssessment,
    PackageRewardRisk,
    OptionPackageCostPolicy,
    OptionOutcomeAvailabilityPolicy,
    assess_option_outcome_measurement,
    assess_option_package,
    bind_option_outcome,
    build_option_outcome_binding,
    build_package_reward_risk,
)
from options.strategies.domain import (
    CandidateKind,
    CandidateStatus,
    OptionCandidate,
    OptionSide,
    StructureRiskClass,
    StructureType,
)
from options.strategies.payoff import evaluate_terminal_payoff
from options.strategies.scenarios import build_scenario_grid
from options.config import load_option_runtime_configuration
from test_equity_behavior import NOW, snapshot_payload
from test_strategy_payoff import leg


def estimate_payload():
    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    package_legs = tuple(replace(
        item, expiration_date=date(2026, 10, 16),
        source_market_time=snapshot.market_time,
        valuation_policy_version="option_valuation_v1",
        valuation_policy_sha256="e" * 64,
    ) for item in (
        leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),
        leg(1, "105", "2", OptionSide.SELL, ContractType.CALL),
    ))
    package_assessment = assess_option_package(
        package_candidate(package_legs, StructureType.CALL_DEBIT_VERTICAL),
        valuation_policy_sha256="e" * 64,
    )
    package = package_assessment.package
    binding = OptionOutcomeBinding(candidate_id=package.candidate_id,
        candidate_identity_sha256=package.candidate_identity_sha256,
        option_matrix_id=package.option_matrix_id, option_source_evidence_sha256="b" * 64,
        strategy_name=package.strategy_name, strategy_version=package.strategy_version,
        strategy_policy_sha256=package.strategy_policy_sha256, structure=package.structure,
        security_id=snapshot.security_id,
        contract_ids=tuple(row.contract_id for row in package.ordered_legs),
        package_terms_sha256=package.sha256, package_terms=package,
        valuation_policy_sha256="e" * 64, cost_policy_sha256="f" * 64, outcome_policy_sha256="a" * 64,
        management_policy_sha256="b" * 64, calibration_scope_sha256="5" * 64,
        market_time=package.market_time,
        source_available_at=NOW, decision_at=NOW, planned_entry_at=NOW + timedelta(seconds=5),
        entry_deadline=NOW + timedelta(minutes=2), planned_exit_at=NOW + timedelta(days=4),
        earliest_expiration_at=datetime(2026, 10, 16, 20, 0, tzinfo=timezone.utc),
        holding_sessions=2)
    cost_policy = OptionPackageCostPolicy(commission_per_contract_per_side=Decimal("0"))
    economics = PackageRewardRisk(basis="INDICATIVE_MODEL", scenario_sha256="c" * 64,
        net_premium=package.net_premium, capital_at_risk=package.capital_at_risk,
        maximum_loss=package.maximum_loss,
        maximum_profit_status="BOUNDED", maximum_profit=Decimal("200"), target_profit=Decimal("50"),
        stop_loss=Decimal("25"), estimated_cost=Decimal("0"),
        cost_policy_version=cost_policy.version, cost_policy_sha256="f" * 64,
        slippage_status="UNAVAILABLE", costs_included=True)
    estimate = OptionOutcomeEstimate(binding=binding, stock_context_id=snapshot.snapshot_id,
        stock_payload_sha256=snapshot.sha256, stock_policy_sha256=snapshot.policy_sha256, economics=economics)
    return snapshot, estimate.model_dump(mode="json")


def calibration_payload(binding, stock_policy):
    return CalibrationEvidence(
        **{key: binding[key] for key in ("strategy_name", "strategy_version", "strategy_policy_sha256", "structure", "holding_sessions",
            "valuation_policy_sha256", "cost_policy_sha256", "outcome_policy_sha256", "management_policy_sha256")},
        model_id="option_test", model_version="test_v1", model_sha256="0" * 64, report_id=uuid4(), report_sha256="1" * 64,
        training_dataset_sha256="2" * 64, validation_dataset_sha256="3" * 64, calibration_policy_sha256="4" * 64,
        scope_sha256="5" * 64, behavior_policy_sha256=stock_policy, target="NET_PROFIT_AT_PLANNED_EXIT",
        outcome_basis="INDICATIVE_OPTION_MARKS", training_window_end=NOW - timedelta(days=300),
        training_outcomes_available_at=NOW - timedelta(days=297),
        validation_window_start=NOW - timedelta(days=290), validation_window_end=NOW - timedelta(days=20),
        outcomes_available_at=NOW - timedelta(days=10), model_fitted_at=NOW - timedelta(days=295),
        report_available_at=NOW - timedelta(days=4), effective_from=NOW - timedelta(days=3), valid_until=NOW + timedelta(days=3),
        sample_count=100, independent_periods=40, minimum_samples=100, minimum_independent_periods=40,
        brier_score=.20, baseline_brier_score=.25, calibration_error=.05, acceptance="ACCEPTED_OUT_OF_SAMPLE",
    ).model_dump(mode="json")


def test_reward_risk_does_not_manufacture_probability():
    snapshot, payload = estimate_payload()
    estimate = OptionOutcomeEstimate.model_validate(payload)
    assert bind_option_outcome(snapshot, estimate) is estimate
    assert estimate.economics.terminal_reward_risk == Decimal("2") / Decimal("3")
    assert estimate.economics.target_stop_reward_risk == 2
    assert estimate.probability is None and estimate.execution_permission is False
    payload["probability"] = .6
    with pytest.raises(ValidationError):
        OptionOutcomeEstimate.model_validate(payload)


@pytest.mark.parametrize("mutation", [None, "horizon", "policy", "scope", "late", "same_data", "short_sample", "bad_interval", "stock_probability", "missing_report", "fit_after_validation"])
def test_calibration_requires_matching_option_scope_and_oos_availability(mutation):
    snapshot, payload = estimate_payload()
    report = calibration_payload(payload["binding"], snapshot.policy_sha256)
    payload.update(probability_status="CALIBRATED", probability=.6, probability_interval_low=.5,
                   probability_interval_high=.7, interval_confidence=.95, interval_method="session_block_bootstrap_v1",
                   calibration=report, unavailable_reasons=[])
    if mutation == "horizon": report["holding_sessions"] = 5
    if mutation == "policy": report["cost_policy_sha256"] = "0" * 64
    if mutation == "scope": report["scope_sha256"] = "0" * 64
    if mutation == "late": report["effective_from"] = (NOW + timedelta(days=1)).isoformat()
    if mutation == "same_data": report["validation_dataset_sha256"] = report["training_dataset_sha256"]
    if mutation == "short_sample": report["independent_periods"] = 10
    if mutation == "bad_interval": payload["probability_interval_low"] = .65
    if mutation == "stock_probability": report["outcome_basis"] = "UNDERLYING_STOCK_WIN_RATE"
    if mutation == "missing_report": payload["calibration"] = None
    if mutation == "fit_after_validation": report["model_fitted_at"] = (NOW - timedelta(days=5)).isoformat()
    if mutation:
        with pytest.raises(ValidationError):
            OptionOutcomeEstimate.model_validate(payload)
    else:
        estimate = OptionOutcomeEstimate.model_validate(payload)
        with pytest.raises(ValueError, match="independently resolved"):
            bind_option_outcome(snapshot, estimate)
        assert bind_option_outcome(snapshot, estimate, verified_calibration=CalibrationEvidence.model_validate(report)).probability == .6
        assert estimate.research_only is True and estimate.execution_permission is False


def test_estimate_cannot_bind_another_stock_snapshot_or_extend_expiry():
    snapshot, payload = estimate_payload()
    payload["stock_context_id"] = uuid4()
    with pytest.raises(ValueError, match="exact stock"):
        bind_option_outcome(snapshot, OptionOutcomeEstimate.model_validate(payload))
    _, payload = estimate_payload()
    payload["binding"]["planned_exit_at"] = payload["binding"]["earliest_expiration_at"]
    with pytest.raises(ValidationError):
        OptionOutcomeEstimate.model_validate(payload)


def test_outcome_binding_rejects_package_identity_or_leg_order_drift():
    _, payload = estimate_payload()
    payload["binding"]["contract_ids"].reverse()
    with pytest.raises(ValidationError, match="contract order"):
        OptionOutcomeEstimate.model_validate(payload)

    _, payload = estimate_payload()
    payload["binding"]["package_terms_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="package hash"):
        OptionOutcomeEstimate.model_validate(payload)


@pytest.mark.parametrize(("field", "value", "message"), [
    ("net_premium", "-299", "net premium"),
    ("capital_at_risk", "299", "capital"),
    ("maximum_loss", "299", "maximum loss"),
    ("maximum_profit", "199", "maximum profit"),
])
def test_outcome_estimate_rejects_economics_drift_from_package(field, value, message):
    _, payload = estimate_payload()
    payload["economics"][field] = value

    with pytest.raises(ValidationError, match=message):
        OptionOutcomeEstimate.model_validate(payload)


def test_outcome_horizon_cannot_be_calendar_days_disguised_as_trading_sessions():
    _, payload = estimate_payload()
    payload["binding"]["planned_exit_at"] = (NOW + timedelta(days=3)).isoformat()
    with pytest.raises(ValidationError, match="trading-session horizon"):
        OptionOutcomeEstimate.model_validate(payload)


def test_unbounded_profit_is_not_a_numeric_reward_risk():
    _, payload = estimate_payload()
    economics = payload["economics"]
    economics.update(maximum_profit_status="UNBOUNDED", maximum_profit=None)
    assert PackageRewardRisk.model_validate(economics).terminal_reward_risk is None
    economics["stop_loss"] = None
    with pytest.raises(ValidationError):
        PackageRewardRisk.model_validate(economics)


def package_candidate(legs, structure):
    policy_sha256 = "e" * 64
    legs = tuple(replace(
        item,
        valuation_policy_version="option_valuation_v1",
        valuation_policy_sha256=policy_sha256,
    ) for item in legs)
    payoff = evaluate_terminal_payoff(legs)
    return OptionCandidate(
        candidate_id=uuid4(), identity_sha256="a" * 64, matrix_id=uuid4(),
        strategy_name="WP6_PACKAGE_TEST", strategy_version="strategy_v1",
        underlyer="TEST",
        candidate_kind=(CandidateKind.SINGLE_CONTRACT if len(legs) == 1
                        else CandidateKind.MULTI_LEG),
        strategy_archetype="REWARD_RISK", persona_tags=("RESEARCH",),
        structure_type=structure,
        structure_risk_class=StructureRiskClass.PREMIUM_AT_RISK_DEBIT,
        expiration_date=legs[0].expiration_date, rank=1,
        status=CandidateStatus.SELECTED, primary_metric_name=None,
        primary_metric_value=None, rank_components={}, primary_evidence={},
        legs=legs, net_premium=payoff.net_premium, collateral_required=None,
        capital_at_risk=payoff.maximum_loss,
        maximum_profit=payoff.maximum_profit, maximum_loss=payoff.maximum_loss,
        return_on_collateral=None, return_on_risk=None,
        breakevens=payoff.breakevens, execution_eligibility=None,
        reason_codes=(), management_policy_version=None, management_policy={},
        policy_sha256="b" * 64, model_version="black_scholes_european_v1",
        context_snapshot_id=None, iv_context_id=None,
        market_data_time=legs[0].source_market_time,
        observed_time=legs[0].source_market_time + timedelta(seconds=1),
        valid_until=legs[0].source_market_time + timedelta(minutes=15),
    )


@pytest.mark.parametrize(("structure", "legs"), [
    (StructureType.LONG_CALL,
         (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),)),
    (StructureType.LONG_PUT,
     (leg(0, "100", "5", OptionSide.BUY, ContractType.PUT),)),
    (StructureType.CALL_DEBIT_VERTICAL,
         (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),
            leg(1, "105", "2", OptionSide.SELL, ContractType.CALL))),
        (StructureType.PUT_DEBIT_VERTICAL,
         (leg(0, "100", "5", OptionSide.BUY, ContractType.PUT),
            leg(1, "95", "2", OptionSide.SELL, ContractType.PUT))),
    (StructureType.PUT_CREDIT_VERTICAL,
         (leg(0, "100", "3", OptionSide.SELL, ContractType.PUT),
            leg(1, "95", "1", OptionSide.BUY, ContractType.PUT))),
            (StructureType.CALL_CREDIT_VERTICAL,
             (leg(0, "100", "3", OptionSide.SELL, ContractType.CALL),
              leg(1, "105", "1", OptionSide.BUY, ContractType.CALL))),
    (StructureType.IRON_CONDOR,
         (leg(0, "95", "2", OptionSide.BUY, ContractType.PUT),
            leg(1, "100", "4", OptionSide.SELL, ContractType.PUT),
            leg(2, "110", "3", OptionSide.SELL, ContractType.CALL),
            leg(3, "116", "1", OptionSide.BUY, ContractType.CALL))),
    (StructureType.CALL_BUTTERFLY,
         (leg(0, "95", "7", OptionSide.BUY, ContractType.CALL),
            leg(1, "100", "4", OptionSide.SELL, ContractType.CALL, ratio=2),
            leg(2, "105", "2", OptionSide.BUY, ContractType.CALL))),
            (StructureType.PUT_BUTTERFLY,
             (leg(0, "105", "7", OptionSide.BUY, ContractType.PUT),
              leg(1, "100", "4", OptionSide.SELL, ContractType.PUT, ratio=2),
              leg(2, "95", "2", OptionSide.BUY, ContractType.PUT))),
])
def test_package_assessment_reconciles_supported_structure_economics(structure, legs):
    candidate = package_candidate(legs, structure)

    assessment = assess_option_package(candidate, valuation_policy_sha256="e" * 64)

    assert assessment.status == "READY"
    assert assessment.package is not None
    assert assessment.package.net_premium == candidate.net_premium
    assert assessment.package.maximum_profit == candidate.maximum_profit
    assert assessment.package.maximum_loss == candidate.maximum_loss
    assert assessment.package.breakevens == candidate.breakevens
    assert assessment.package_terms_sha256 == assessment.package.sha256
    assert assessment.assessed_at == candidate.observed_time
    assert assessment.valuation_policy_sha256 == "e" * 64
    assert tuple(row.contract_id for row in assessment.package.ordered_legs) == tuple(
        row.contract_id for row in candidate.legs
    )
    assert all(row.time_to_expiration_years > 0 for row in assessment.package.ordered_legs)
    assert all(row.entry_iv > 0 for row in assessment.package.ordered_legs)
    assert assessment.package.ordered_legs[0].risk_free_rate == candidate.legs[0].risk_free_rate
    assert assessment.package.ordered_legs[0].dividend_yield == candidate.legs[0].dividend_yield
    assert assessment.execution_permission is False


def test_package_assessment_retains_unbounded_loss_without_inventing_capital():
    candidate = package_candidate(
        (leg(0, "100", "5", OptionSide.SELL, ContractType.CALL),),
        StructureType.LONG_CALL,
    )

    assessment = assess_option_package(candidate, valuation_policy_sha256="e" * 64)

    assert assessment.status == "READY"
    assert assessment.package.maximum_loss_status == "UNBOUNDED"
    assert assessment.package.maximum_loss is None
    assert assessment.package.capital_at_risk is None


def test_package_assessment_marks_stored_economics_mismatch_unavailable():
    candidate = package_candidate(
        (leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),),
        StructureType.LONG_CALL,
    )
    candidate = replace(candidate, maximum_loss=Decimal("499"))

    assessment = assess_option_package(candidate, valuation_policy_sha256="e" * 64)

    assert assessment.status == "UNAVAILABLE"
    assert assessment.package is None
    assert assessment.reason_codes == ("CANDIDATE_MAXIMUM_LOSS_MISMATCH",)


def test_scenario_grid_records_exact_ordered_valuation_inputs():
    candidate = package_candidate(
        (
            leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),
            leg(1, "105", "2", OptionSide.SELL, ContractType.CALL),
        ),
        StructureType.CALL_DEBIT_VERTICAL,
    )
    policy = load_option_runtime_configuration().strategy_policy.scenarios

    scenarios = build_scenario_grid(candidate, policy)

    assert scenarios
    assumptions = scenarios[0].assumptions
    assert assumptions["entry_net_premium"] == str(candidate.net_premium)
    assert [row["contract_id"] for row in assumptions["valuation_inputs"]] == [
        leg.contract_id for leg in candidate.legs
    ]
    assert all(
        row["valuation_policy_sha256"] == "e" * 64
        for row in assumptions["valuation_inputs"]
    )
    assert assumptions["quote_liquidity"] == "NOT_AVAILABLE"


def test_package_reward_risk_applies_commission_and_never_invents_slippage():
    candidate = package_candidate(
        (
            leg(0, "100", "5", OptionSide.BUY, ContractType.CALL),
            leg(1, "105", "2", OptionSide.SELL, ContractType.CALL),
        ),
        StructureType.CALL_DEBIT_VERTICAL,
    )
    package = assess_option_package(
        candidate, valuation_policy_sha256="e" * 64,
    ).package
    cost_policy = OptionPackageCostPolicy(
        commission_per_contract_per_side=Decimal("0.65")
    )

    economics = build_package_reward_risk(
        package, scenario_sha256="a" * 64, cost_policy=cost_policy,
        target_profit=Decimal("50"), stop_loss=Decimal("25"),
    )

    assert economics.estimated_cost == Decimal("2.60")
    assert economics.maximum_profit == Decimal("197.40")
    assert economics.maximum_loss == Decimal("302.60")
    assert economics.capital_at_risk == Decimal("302.60")
    assert economics.slippage_status == "UNAVAILABLE"
    assert economics.cost_policy_sha256 == cost_policy.sha256
    assert economics.costs_included is True


def test_outcome_measurement_marks_complete_coherent_batch_ready():
    batch_id = uuid4()
    assessment = assess_option_outcome_measurement(
        candidate_id=uuid4(), candidate_identity_sha256="a" * 64,
        valuation_policy_sha256="b" * 64,
        measurement_type="30MIN", checkpoint_at=NOW,
        evaluated_at=NOW + timedelta(minutes=30),
        required_contract_ids=(1, 2), observed_contract_ids=(1, 2),
        source_batch_id=batch_id,
    )

    assert assessment.status == "READY"
    assert assessment.source_batch_id == batch_id
    assert assessment.missing_contract_ids == ()
    assert assessment.execution_permission is False


def test_outcome_measurement_transitions_missing_leg_from_pending_to_unavailable():
    policy = OptionOutcomeAvailabilityPolicy()
    common = dict(
        candidate_id=uuid4(), candidate_identity_sha256="a" * 64,
        valuation_policy_sha256="b" * 64,
        measurement_type="30MIN", checkpoint_at=NOW,
        required_contract_ids=(1, 2), observed_contract_ids=(1,),
        source_batch_id=None, policy=policy,
    )

    pending = assess_option_outcome_measurement(
        **common, evaluated_at=policy.deadline(NOW) - timedelta(seconds=1),
    )
    unavailable = assess_option_outcome_measurement(
        **common, evaluated_at=policy.deadline(NOW),
    )

    assert pending.status == "PENDING"
    assert pending.reason_codes == ("COHERENT_PACKAGE_MARKS_PENDING",)
    assert unavailable.status == "UNAVAILABLE"
    assert unavailable.evaluated_at == policy.deadline(NOW)
    assert unavailable.missing_contract_ids == (2,)
    assert unavailable.reason_codes == ("COHERENT_PACKAGE_MARKS_UNAVAILABLE",)


def test_outcome_binding_builder_uses_ready_package_identity_and_order():
    snapshot, payload = estimate_payload()
    package = OptionPackageAssessment.model_validate({
        "candidate_id": payload["binding"]["candidate_id"],
        "candidate_identity_sha256": payload["binding"]["candidate_identity_sha256"],
        "option_matrix_id": payload["binding"]["option_matrix_id"],
        "valuation_policy_sha256": payload["binding"]["valuation_policy_sha256"],
        "assessment_policy_version": "option_package_assessment_policy_v1",
        "assessment_policy_sha256": PACKAGE_ASSESSMENT_POLICY.sha256,
        "assessed_at": payload["binding"]["package_terms"]["observed_at"],
        "status": "READY",
        "package": payload["binding"]["package_terms"],
        "package_terms_sha256": payload["binding"]["package_terms_sha256"],
    })
    original = payload["binding"]

    built = build_option_outcome_binding(
        package, security_id=snapshot.security_id,
        option_source_evidence_sha256=original["option_source_evidence_sha256"],
        cost_policy_sha256=original["cost_policy_sha256"],
        outcome_policy_sha256=original["outcome_policy_sha256"],
        management_policy_sha256=original["management_policy_sha256"],
        calibration_scope_sha256=original["calibration_scope_sha256"],
        source_available_at=original["source_available_at"],
        decision_at=original["decision_at"],
        planned_entry_at=original["planned_entry_at"],
        entry_deadline=original["entry_deadline"],
        planned_exit_at=original["planned_exit_at"],
        earliest_expiration_at=original["earliest_expiration_at"],
        holding_sessions=original["holding_sessions"],
    )

    assert built.package_terms_sha256 == package.package.sha256
    assert built.contract_ids == tuple(
        leg.contract_id for leg in package.package.ordered_legs
    )

    unavailable = package.model_copy(
        update={"status": "UNAVAILABLE", "package": None,
                "package_terms_sha256": None, "reason_codes": ("MISSING",)}
    )
    with pytest.raises(ValueError, match="READY package"):
        build_option_outcome_binding(
            unavailable, security_id=snapshot.security_id,
            option_source_evidence_sha256="a" * 64,
            cost_policy_sha256="b" * 64, outcome_policy_sha256="c" * 64,
            management_policy_sha256="d" * 64,
            calibration_scope_sha256="e" * 64,
            source_available_at=NOW, decision_at=NOW,
            planned_entry_at=NOW + timedelta(seconds=1),
            entry_deadline=NOW + timedelta(minutes=1),
            planned_exit_at=NOW + timedelta(days=4),
            earliest_expiration_at=datetime(2026, 10, 16, 20, tzinfo=timezone.utc),
            holding_sessions=2,
        )