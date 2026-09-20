"""Evidence-bound option outcome estimates, never execution or calibration by assertion."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars
from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, DEFINITION_SHA256, Name, Sha256, StockBehaviorSnapshot
from options.strategies.domain import CandidateStatus, OptionCandidate
from options.strategies.payoff import evaluate_terminal_payoff


Money = Annotated[Decimal, Field(allow_inf_nan=False)]
Probability = Annotated[float, Field(strict=True, ge=0, le=1)]
OutcomeMeasurement = Literal["15MIN", "30MIN", "60MIN", "CLOSE", "NEXT_OPEN"]


class OptionPackageLegTerms(Contract):
    leg_index: int = Field(strict=True, ge=0, le=3)
    snapshot_id: UUID
    contract_id: int = Field(strict=True, gt=0)
    contract_ticker: Name
    side: Literal["BUY", "SELL"]
    ratio: int = Field(strict=True, gt=0)
    multiplier: int = Field(strict=True, gt=0)
    expiration_date: date
    strike: Money = Field(gt=0)
    contract_type: Literal["CALL", "PUT"]
    spot: Money = Field(gt=0)
    entry_mark: Money = Field(gt=0)
    time_to_expiration_years: float = Field(strict=True, gt=0)
    risk_free_rate: float = Field(strict=True, ge=-0.10, le=1)
    dividend_yield: float = Field(strict=True, ge=0, le=1)
    entry_iv: float = Field(strict=True, gt=0)
    source_market_time: AwareDatetime
    mark_source: Name
    model_version: Name
    valuation_policy_version: Name
    valuation_policy_sha256: Sha256


class OptionPackageTerms(Contract):
    schema_version: Literal["option_package_terms_v1"] = "option_package_terms_v1"
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    option_matrix_id: UUID
    underlyer: Name
    strategy_name: Name
    strategy_version: Name
    strategy_policy_sha256: Sha256
    structure: Name
    ordered_legs: tuple[OptionPackageLegTerms, ...] = Field(min_length=1, max_length=4)
    entry_basis: Literal["CANDIDATE_MODEL_MARKS"] = "CANDIDATE_MODEL_MARKS"
    market_time: AwareDatetime
    observed_at: AwareDatetime
    valid_until: AwareDatetime | None = None
    net_premium: Money
    collateral_required: Money | None = Field(default=None, ge=0)
    capital_at_risk: Money | None = Field(default=None, gt=0)
    maximum_loss_status: Literal["BOUNDED", "UNBOUNDED", "UNAVAILABLE"]
    maximum_loss: Money | None = Field(default=None, gt=0)
    maximum_profit_status: Literal["BOUNDED", "UNBOUNDED", "UNAVAILABLE"]
    maximum_profit: Money | None = Field(default=None, ge=0)
    breakevens: tuple[Money, ...]
    research_only: Literal[True] = True
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_package(self):
        if tuple(leg.leg_index for leg in self.ordered_legs) != tuple(
            range(len(self.ordered_legs))
        ):
            raise ValueError("package legs must retain contiguous candidate order")
        if len({leg.contract_id for leg in self.ordered_legs}) != len(self.ordered_legs):
            raise ValueError("package contract IDs must be distinct")
        if len({leg.expiration_date for leg in self.ordered_legs}) != 1:
            raise ValueError("package payoff requires one expiration")
        if any(leg.source_market_time > self.market_time for leg in self.ordered_legs):
            raise ValueError("package legs cannot be future-visible at market_time")
        if self.observed_at < self.market_time:
            raise ValueError("package observation cannot precede market_time")
        if self.valid_until is not None and self.valid_until <= self.market_time:
            raise ValueError("package valid_until must follow market_time")
        if (self.maximum_loss_status == "BOUNDED") != (self.maximum_loss is not None):
            raise ValueError("bounded maximum loss requires a positive value")
        if (self.maximum_profit_status == "BOUNDED") != (self.maximum_profit is not None):
            raise ValueError("bounded maximum profit requires a value")
        return self


class OptionPackageAssessmentPolicy(Contract):
    version: Literal["option_package_assessment_policy_v1"] = (
        "option_package_assessment_policy_v1"
    )
    usage_window_seconds: int = Field(default=86400, strict=True, ge=3600, le=604800)
    maximum_assessments: int = Field(default=50000, strict=True, ge=1, le=1000000)
    maximum_payload_bytes: int = Field(
        default=500000000, strict=True, ge=1000000, le=5000000000
    )
    maximum_candidates_per_matrix: int = Field(default=1000, strict=True, ge=1, le=1000)
    minimum_assessments_before_rate_pause: int = Field(
        default=100, strict=True, ge=1, le=10000
    )
    maximum_unavailable_fraction: float = Field(default=0.25, strict=True, ge=0, le=1)

    @model_validator(mode="after")
    def validate_limits(self):
        if self.minimum_assessments_before_rate_pause > self.maximum_assessments:
            raise ValueError("package rate-pause minimum exceeds assessment limit")
        return self


PACKAGE_ASSESSMENT_POLICY = OptionPackageAssessmentPolicy()


class OptionPackageAssessment(Contract):
    schema_version: Literal["option_package_assessment_v1"] = (
        "option_package_assessment_v1"
    )
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    option_matrix_id: UUID
    valuation_policy_sha256: Sha256
    assessment_policy_version: Name
    assessment_policy_sha256: Sha256
    assessed_at: AwareDatetime
    status: Literal["READY", "UNAVAILABLE"]
    package: OptionPackageTerms | None = None
    package_terms_sha256: Sha256 | None = None
    reason_codes: tuple[Name, ...] = ()
    research_only: Literal[True] = True
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_assessment(self):
        if (self.status == "READY") != (self.package is not None):
            raise ValueError("ready package assessment requires package terms")
        if (self.status == "READY") != (self.package_terms_sha256 is not None):
            raise ValueError("ready package assessment requires package hash")
        if self.package is not None and self.package_terms_sha256 != self.package.sha256:
            raise ValueError("package assessment hash does not match package terms")
        if self.status == "READY" and self.reason_codes:
            raise ValueError("ready package assessment cannot carry reasons")
        if self.status == "UNAVAILABLE" and not self.reason_codes:
            raise ValueError("unavailable package assessment requires reasons")
        return self


def assess_option_package(
    candidate: OptionCandidate,
    *,
    valuation_policy_sha256: str,
    policy: OptionPackageAssessmentPolicy = PACKAGE_ASSESSMENT_POLICY,
) -> OptionPackageAssessment:
    identity = dict(
        candidate_id=candidate.candidate_id,
        candidate_identity_sha256=candidate.identity_sha256,
    )
    assessment_fields = dict(
        **identity,
        option_matrix_id=candidate.matrix_id,
        valuation_policy_sha256=valuation_policy_sha256,
        assessment_policy_version=policy.version,
        assessment_policy_sha256=policy.sha256,
        assessed_at=candidate.observed_time,
    )
    reasons = []
    if candidate.status is not CandidateStatus.SELECTED:
        reasons.append("CANDIDATE_NOT_SELECTED")
    if not candidate.legs:
        reasons.append("PACKAGE_LEGS_UNAVAILABLE")
    if any(
        leg.valuation_policy_sha256 != valuation_policy_sha256
        or leg.valuation_policy_version is None
        for leg in candidate.legs
    ):
        reasons.append("LEG_VALUATION_POLICY_MISMATCH")
    if reasons:
        return OptionPackageAssessment(
            **assessment_fields, status="UNAVAILABLE", reason_codes=tuple(reasons),
        )
    payoff = evaluate_terminal_payoff(candidate.legs)
    expected = {
        "net_premium": payoff.net_premium,
        "maximum_profit": payoff.maximum_profit,
        "maximum_loss": payoff.maximum_loss,
        "breakevens": payoff.breakevens,
    }
    actual = {name: getattr(candidate, name) for name in expected}
    mismatches = tuple(
        f"CANDIDATE_{name.upper()}_MISMATCH"
        for name, value in expected.items()
        if actual[name] != value
    )
    if candidate.capital_at_risk != payoff.maximum_loss:
        mismatches += ("CANDIDATE_CAPITAL_AT_RISK_MISMATCH",)
    if mismatches:
        return OptionPackageAssessment(
            **assessment_fields, status="UNAVAILABLE", reason_codes=mismatches,
        )
    package = OptionPackageTerms(
        **identity,
        option_matrix_id=candidate.matrix_id,
        underlyer=candidate.underlyer,
        strategy_name=candidate.strategy_name,
        strategy_version=candidate.strategy_version,
        strategy_policy_sha256=candidate.policy_sha256,
        structure=candidate.structure_type.value,
        ordered_legs=tuple(
            OptionPackageLegTerms(
                leg_index=leg.leg_index, snapshot_id=leg.snapshot_id,
                contract_id=leg.contract_id, contract_ticker=leg.contract_ticker,
                side=leg.side.value, ratio=leg.ratio, multiplier=leg.multiplier,
                expiration_date=leg.expiration_date, strike=leg.strike,
                contract_type=leg.contract_type.value, spot=leg.spot,
                entry_mark=leg.model_mark,
                time_to_expiration_years=leg.time_to_expiration_years,
                risk_free_rate=leg.risk_free_rate,
                dividend_yield=leg.dividend_yield,
                entry_iv=leg.local_iv,
                source_market_time=leg.source_market_time,
                mark_source=leg.mark_source, model_version=leg.model_version,
                valuation_policy_version=leg.valuation_policy_version,
                valuation_policy_sha256=leg.valuation_policy_sha256,
            )
            for leg in candidate.legs
        ),
        market_time=candidate.market_data_time,
        observed_at=candidate.observed_time,
        valid_until=candidate.valid_until,
        net_premium=payoff.net_premium,
        collateral_required=candidate.collateral_required,
        capital_at_risk=candidate.capital_at_risk,
        maximum_loss_status="BOUNDED" if payoff.bounded_maximum_loss else "UNBOUNDED",
        maximum_loss=payoff.maximum_loss,
        maximum_profit_status="BOUNDED" if payoff.maximum_profit is not None else "UNBOUNDED",
        maximum_profit=payoff.maximum_profit,
        breakevens=payoff.breakevens,
    )
    return OptionPackageAssessment(
        **assessment_fields, status="READY", package=package,
        package_terms_sha256=package.sha256,
    )


class OptionOutcomeBinding(Contract):
    schema_version: Literal["option_outcome_binding_v2"] = "option_outcome_binding_v2"
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    option_matrix_id: UUID
    option_source_evidence_sha256: Sha256
    strategy_name: Name
    strategy_version: Name
    strategy_policy_sha256: Sha256
    structure: Name
    security_id: UUID
    contract_ids: tuple[Annotated[int, Field(strict=True, gt=0)], ...] = Field(min_length=1, max_length=4)
    package_terms_sha256: Sha256
    valuation_policy_sha256: Sha256
    cost_policy_sha256: Sha256
    outcome_policy_sha256: Sha256
    management_policy_sha256: Sha256
    calibration_scope_sha256: Sha256
    package_terms: OptionPackageTerms
    market_time: AwareDatetime
    source_available_at: AwareDatetime
    decision_at: AwareDatetime
    planned_entry_at: AwareDatetime
    entry_deadline: AwareDatetime
    planned_exit_at: AwareDatetime
    earliest_expiration_at: AwareDatetime
    holding_sessions: int = Field(strict=True, ge=1, le=252)
    holding_clock: Literal["XNYS_SESSIONS_FROM_ENTRY"] = "XNYS_SESSIONS_FROM_ENTRY"

    @model_validator(mode="after")
    def validate_identity_and_horizon(self):
        if len(set(self.contract_ids)) != len(self.contract_ids):
            raise ValueError("contract IDs must be distinct and leg-ordered")
        package = self.package_terms
        expected = {
            "candidate_id": package.candidate_id,
            "candidate_identity_sha256": package.candidate_identity_sha256,
            "option_matrix_id": package.option_matrix_id,
            "strategy_name": package.strategy_name,
            "strategy_version": package.strategy_version,
            "strategy_policy_sha256": package.strategy_policy_sha256,
            "structure": package.structure,
            "market_time": package.market_time,
        }
        for field, value in expected.items():
            if getattr(self, field) != value:
                raise ValueError(f"outcome binding {field} does not match package terms")
        if self.contract_ids != tuple(leg.contract_id for leg in package.ordered_legs):
            raise ValueError("outcome binding contract order does not match package terms")
        if self.package_terms_sha256 != package.sha256:
            raise ValueError("outcome binding package hash does not match package terms")
        expiration_dates = {leg.expiration_date for leg in package.ordered_legs}
        expiration_date = next(iter(expiration_dates))
        if self.earliest_expiration_at.astimezone(
            ZoneInfo("America/New_York")
        ).date() != expiration_date:
            raise ValueError("outcome binding expiration does not match package terms")
        if not self.market_time <= self.source_available_at <= self.decision_at < self.planned_entry_at <= self.entry_deadline < self.planned_exit_at < self.earliest_expiration_at:
            raise ValueError("source, decision, entry, exit and expiry clocks must be causal")
        calendar = exchange_calendars.get_calendar("XNYS")
        entry_session = self.planned_entry_at.astimezone(ZoneInfo("America/New_York")).date()
        if not calendar.is_session(str(entry_session)):
            raise ValueError("planned entry requires an exchange session")
        exit_session = calendar.session_offset(str(entry_session), self.holding_sessions)
        if not calendar.session_open(entry_session) <= self.planned_entry_at < calendar.session_close(entry_session):
            raise ValueError("planned entry must be inside the regular exchange session")
        if not calendar.session_open(exit_session) <= self.planned_exit_at <= calendar.session_close(exit_session):
            raise ValueError("planned exit must match the declared trading-session horizon")
        return self


def build_option_outcome_binding(
    assessment: OptionPackageAssessment,
    *,
    security_id: UUID,
    option_source_evidence_sha256: str,
    cost_policy_sha256: str,
    outcome_policy_sha256: str,
    management_policy_sha256: str,
    calibration_scope_sha256: str,
    source_available_at,
    decision_at,
    planned_entry_at,
    entry_deadline,
    planned_exit_at,
    earliest_expiration_at,
    holding_sessions: int,
) -> OptionOutcomeBinding:
    if assessment.status != "READY" or assessment.package is None:
        raise ValueError("outcome binding requires a READY package assessment")
    package = assessment.package
    return OptionOutcomeBinding(
        candidate_id=package.candidate_id,
        candidate_identity_sha256=package.candidate_identity_sha256,
        option_matrix_id=package.option_matrix_id,
        option_source_evidence_sha256=option_source_evidence_sha256,
        strategy_name=package.strategy_name,
        strategy_version=package.strategy_version,
        strategy_policy_sha256=package.strategy_policy_sha256,
        structure=package.structure,
        security_id=security_id,
        contract_ids=tuple(leg.contract_id for leg in package.ordered_legs),
        package_terms_sha256=package.sha256,
        valuation_policy_sha256=assessment.valuation_policy_sha256,
        cost_policy_sha256=cost_policy_sha256,
        outcome_policy_sha256=outcome_policy_sha256,
        management_policy_sha256=management_policy_sha256,
        calibration_scope_sha256=calibration_scope_sha256,
        package_terms=package,
        market_time=package.market_time,
        source_available_at=source_available_at,
        decision_at=decision_at,
        planned_entry_at=planned_entry_at,
        entry_deadline=entry_deadline,
        planned_exit_at=planned_exit_at,
        earliest_expiration_at=earliest_expiration_at,
        holding_sessions=holding_sessions,
    )


class PackageRewardRisk(Contract):
    basis: Literal["INDICATIVE_MODEL", "QUOTE_SCENARIO"]
    scenario_sha256: Sha256
    units: Literal["USD_PER_PACKAGE"] = "USD_PER_PACKAGE"
    net_premium: Money
    capital_at_risk: Money = Field(gt=0)
    maximum_loss: Money = Field(gt=0)
    maximum_profit_status: Literal["BOUNDED", "UNBOUNDED", "UNAVAILABLE"]
    maximum_profit: Money | None = Field(default=None, ge=0)
    target_profit: Money | None = Field(default=None, gt=0)
    stop_loss: Money | None = Field(default=None, gt=0)
    estimated_cost: Money = Field(ge=0)
    cost_policy_version: Name
    cost_policy_sha256: Sha256
    slippage_status: Literal["APPLIED", "UNAVAILABLE"]
    costs_included: bool = Field(strict=True)
    fill_guaranteed: Literal[False] = False
    stop_fill_guaranteed: Literal[False] = False

    @model_validator(mode="after")
    def validate_economics(self):
        if (self.maximum_profit_status == "BOUNDED") != (self.maximum_profit is not None):
            raise ValueError("bounded maximum profit requires a value; unavailable or unbounded does not mean zero")
        if (self.target_profit is None) != (self.stop_loss is None):
            raise ValueError("target/stop reward-risk requires both explicit scenario amounts")
        if self.stop_loss is not None and self.stop_loss > self.maximum_loss:
            raise ValueError("scenario stop loss exceeds maximum modeled package loss")
        if self.target_profit is not None and self.maximum_profit is not None and self.target_profit > self.maximum_profit:
            raise ValueError("scenario target exceeds bounded package profit")
        if not self.costs_included:
            raise ValueError("package reward/risk must include its declared cost estimate")
        return self

    @property
    def terminal_reward_risk(self) -> Decimal | None:
        return self.maximum_profit / self.maximum_loss if self.maximum_profit is not None else None

    @property
    def target_stop_reward_risk(self) -> Decimal | None:
        return self.target_profit / self.stop_loss if self.target_profit is not None else None


class OptionPackageCostPolicy(Contract):
    version: Literal["option_package_cost_v1"] = "option_package_cost_v1"
    commission_per_contract_per_side: Money = Field(ge=0)
    round_trip_sides: Literal[2] = 2
    slippage_model: Literal["UNAVAILABLE_WITHOUT_EXECUTABLE_QUOTES"] = (
        "UNAVAILABLE_WITHOUT_EXECUTABLE_QUOTES"
    )


class OptionOutcomeAvailabilityPolicy(Contract):
    version: Literal["option_outcome_availability_v1"] = (
        "option_outcome_availability_v1"
    )
    maximum_mark_lag_seconds: int = Field(default=900, strict=True, gt=0)
    provider_delay_seconds: int = Field(default=900, strict=True, ge=0)
    completion_grace_seconds: int = Field(default=900, strict=True, ge=0)
    maximum_candidates_per_run: int = Field(default=200, strict=True, ge=1, le=1000)

    def deadline(self, checkpoint_at) -> object:
        return checkpoint_at + timedelta(
            seconds=(
                self.maximum_mark_lag_seconds
                + self.provider_delay_seconds
                + self.completion_grace_seconds
            )
        )


class OptionOutcomeMeasurementAssessment(Contract):
    schema_version: Literal["option_outcome_measurement_assessment_v1"] = (
        "option_outcome_measurement_assessment_v1"
    )
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    valuation_policy_sha256: Sha256
    measurement_type: OutcomeMeasurement
    checkpoint_at: AwareDatetime
    evaluated_at: AwareDatetime
    availability_deadline: AwareDatetime
    required_contract_ids: tuple[int, ...] = Field(min_length=1, max_length=4)
    observed_contract_ids: tuple[int, ...] = ()
    missing_contract_ids: tuple[int, ...] = ()
    source_batch_id: UUID | None = None
    status: Literal["READY", "PENDING", "UNAVAILABLE"]
    availability_policy_version: Name
    availability_policy_sha256: Sha256
    reason_codes: tuple[Name, ...] = ()
    research_only: Literal[True] = True
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_measurement(self):
        if len(set(self.required_contract_ids)) != len(self.required_contract_ids):
            raise ValueError("required outcome contract IDs must be distinct")
        if set(self.observed_contract_ids) - set(self.required_contract_ids):
            raise ValueError("observed outcome contracts must belong to the package")
        expected_missing = tuple(
            contract_id for contract_id in self.required_contract_ids
            if contract_id not in set(self.observed_contract_ids)
        )
        if self.missing_contract_ids != expected_missing:
            raise ValueError("missing outcome contract IDs are not exact")
        if not self.checkpoint_at <= self.evaluated_at:
            raise ValueError("outcome measurement cannot be evaluated before checkpoint")
        if self.availability_deadline < self.checkpoint_at:
            raise ValueError("outcome availability deadline precedes checkpoint")
        if self.status == "READY":
            if self.missing_contract_ids or self.source_batch_id is None or self.reason_codes:
                raise ValueError("ready outcome measurement requires one complete batch")
        elif self.status == "PENDING":
            if not self.missing_contract_ids or self.source_batch_id is not None:
                raise ValueError("pending outcome measurement requires missing legs")
            if self.evaluated_at >= self.availability_deadline:
                raise ValueError("expired pending outcome measurement must be unavailable")
            if self.reason_codes != ("COHERENT_PACKAGE_MARKS_PENDING",):
                raise ValueError("pending outcome measurement reason is invalid")
        else:
            if not self.missing_contract_ids or self.source_batch_id is not None:
                raise ValueError("unavailable outcome measurement requires missing legs")
            if self.evaluated_at < self.availability_deadline:
                raise ValueError("outcome measurement is unavailable only after deadline")
            if self.reason_codes != ("COHERENT_PACKAGE_MARKS_UNAVAILABLE",):
                raise ValueError("unavailable outcome measurement reason is invalid")
        return self


def assess_option_outcome_measurement(
    *,
    candidate_id: UUID,
    candidate_identity_sha256: str,
    valuation_policy_sha256: str,
    measurement_type: OutcomeMeasurement,
    checkpoint_at,
    evaluated_at,
    required_contract_ids: tuple[int, ...],
    observed_contract_ids: tuple[int, ...],
    source_batch_id: UUID | None,
    policy: OptionOutcomeAvailabilityPolicy | None = None,
) -> OptionOutcomeMeasurementAssessment:
    policy = policy or OptionOutcomeAvailabilityPolicy()
    observed = set(observed_contract_ids)
    missing = tuple(
        contract_id for contract_id in required_contract_ids
        if contract_id not in observed
    )
    deadline = policy.deadline(checkpoint_at)
    if not missing:
        status = "READY"
        reasons = ()
    elif evaluated_at < deadline:
        status = "PENDING"
        reasons = ("COHERENT_PACKAGE_MARKS_PENDING",)
        source_batch_id = None
    else:
        status = "UNAVAILABLE"
        reasons = ("COHERENT_PACKAGE_MARKS_UNAVAILABLE",)
        source_batch_id = None
        evaluated_at = deadline
    return OptionOutcomeMeasurementAssessment(
        candidate_id=candidate_id,
        candidate_identity_sha256=candidate_identity_sha256,
        valuation_policy_sha256=valuation_policy_sha256,
        measurement_type=measurement_type,
        checkpoint_at=checkpoint_at,
        evaluated_at=evaluated_at,
        availability_deadline=deadline,
        required_contract_ids=required_contract_ids,
        observed_contract_ids=observed_contract_ids,
        missing_contract_ids=missing,
        source_batch_id=source_batch_id,
        status=status,
        availability_policy_version=policy.version,
        availability_policy_sha256=policy.sha256,
        reason_codes=reasons,
    )


def build_package_reward_risk(
    package: OptionPackageTerms,
    *,
    scenario_sha256: str,
    cost_policy: OptionPackageCostPolicy,
    target_profit: Decimal | None = None,
    stop_loss: Decimal | None = None,
) -> PackageRewardRisk:
    if (
        package.maximum_loss is None
        or package.capital_at_risk is None
        or package.maximum_loss_status != "BOUNDED"
    ):
        raise ValueError("package reward/risk requires bounded maximum loss")
    contracts = sum(leg.ratio for leg in package.ordered_legs)
    estimated_cost = (
        cost_policy.commission_per_contract_per_side
        * cost_policy.round_trip_sides
        * contracts
    )
    maximum_profit = (
        max(package.maximum_profit - estimated_cost, Decimal("0"))
        if package.maximum_profit is not None else None
    )
    return PackageRewardRisk(
        basis="INDICATIVE_MODEL",
        scenario_sha256=scenario_sha256,
        net_premium=package.net_premium,
        capital_at_risk=package.capital_at_risk + estimated_cost,
        maximum_loss=package.maximum_loss + estimated_cost,
        maximum_profit_status=package.maximum_profit_status,
        maximum_profit=maximum_profit,
        target_profit=target_profit,
        stop_loss=stop_loss,
        estimated_cost=estimated_cost,
        cost_policy_version=cost_policy.version,
        cost_policy_sha256=cost_policy.sha256,
        slippage_status="UNAVAILABLE",
        costs_included=True,
    )


class CalibrationEvidence(Contract):
    model_id: Name
    model_version: Name
    model_sha256: Sha256
    report_id: UUID
    report_sha256: Sha256
    training_dataset_sha256: Sha256
    validation_dataset_sha256: Sha256
    calibration_policy_sha256: Sha256
    scope_sha256: Sha256
    behavior_definition_sha256: Literal[DEFINITION_SHA256] = DEFINITION_SHA256
    behavior_policy_sha256: Sha256
    strategy_name: Name
    strategy_version: Name
    strategy_policy_sha256: Sha256
    structure: Name
    holding_sessions: int = Field(strict=True, ge=1, le=252)
    valuation_policy_sha256: Sha256
    cost_policy_sha256: Sha256
    outcome_policy_sha256: Sha256
    management_policy_sha256: Sha256
    target: Literal["NET_PROFIT_AT_PLANNED_EXIT", "TARGET_BEFORE_STOP_WITHIN_HOLD"]
    outcome_basis: Literal["INDICATIVE_OPTION_MARKS", "QUOTE_PAPER_FILLS"]
    training_window_end: AwareDatetime
    training_outcomes_available_at: AwareDatetime
    validation_window_start: AwareDatetime
    validation_window_end: AwareDatetime
    outcomes_available_at: AwareDatetime
    model_fitted_at: AwareDatetime
    report_available_at: AwareDatetime
    effective_from: AwareDatetime
    valid_until: AwareDatetime
    sample_count: int = Field(strict=True, gt=0)
    independent_periods: int = Field(strict=True, gt=0)
    minimum_samples: int = Field(strict=True, gt=0)
    minimum_independent_periods: int = Field(strict=True, ge=2)
    brier_score: Probability
    baseline_brier_score: Probability
    calibration_error: Probability
    acceptance: Literal["ACCEPTED_OUT_OF_SAMPLE"]

    @model_validator(mode="after")
    def validate_calibration_scope(self):
        if not self.training_window_end <= self.training_outcomes_available_at <= self.model_fitted_at < self.validation_window_start <= self.validation_window_end <= self.outcomes_available_at <= self.report_available_at <= self.effective_from < self.valid_until:
            raise ValueError("calibration windows and actual outcome/report availability must be ordered")
        if self.training_dataset_sha256 == self.validation_dataset_sha256:
            raise ValueError("calibration cannot claim the same training and validation dataset")
        if not self.minimum_independent_periods <= self.independent_periods <= self.sample_count or self.sample_count < self.minimum_samples:
            raise ValueError("calibration sample does not meet its declared policy minima")
        return self


class OptionOutcomeEstimate(Contract):
    schema_version: Literal["option_outcome_estimate_v1"] = "option_outcome_estimate_v1"
    binding: OptionOutcomeBinding
    stock_context_id: UUID
    stock_payload_sha256: Sha256
    stock_definition_sha256: Literal[DEFINITION_SHA256] = DEFINITION_SHA256
    stock_policy_sha256: Sha256
    economics: PackageRewardRisk
    probability_status: Literal["UNAVAILABLE", "CALIBRATED"] = "UNAVAILABLE"
    probability: Probability | None = None
    probability_interval_low: Probability | None = None
    probability_interval_high: Probability | None = None
    interval_method: Name | None = None
    interval_confidence: Probability | None = None
    calibration: CalibrationEvidence | None = None
    unavailable_reasons: tuple[Name, ...] = ("NO_MATCHING_OUT_OF_SAMPLE_OPTION_CALIBRATION",)
    research_only: Literal[True] = True
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_probability(self):
        package = self.binding.package_terms
        economics = self.economics
        if economics.net_premium != package.net_premium:
            raise ValueError("outcome economics net premium does not match package terms")
        if economics.capital_at_risk != package.capital_at_risk + economics.estimated_cost:
            raise ValueError("outcome economics capital does not match package terms")
        if economics.maximum_loss != package.maximum_loss + economics.estimated_cost:
            raise ValueError("outcome economics maximum loss does not match package terms")
        if economics.maximum_profit_status != package.maximum_profit_status:
            raise ValueError("outcome economics profit status does not match package terms")
        expected_profit = (
            max(package.maximum_profit - economics.estimated_cost, Decimal("0"))
            if package.maximum_profit is not None else None
        )
        if economics.maximum_profit != expected_profit:
            raise ValueError("outcome economics maximum profit does not match package terms")
        if economics.cost_policy_sha256 != self.binding.cost_policy_sha256:
            raise ValueError("outcome economics cost policy does not match binding")
        values = (self.probability, self.probability_interval_low, self.probability_interval_high,
                  self.interval_method, self.interval_confidence, self.calibration)
        if self.probability_status == "UNAVAILABLE":
            if any(value is not None for value in values) or not self.unavailable_reasons:
                raise ValueError("unavailable probability must be null with reasons, never delta or a score")
            return self
        if any(value is None for value in values) or self.unavailable_reasons:
            raise ValueError("calibrated probability requires report evidence and uncertainty")
        if not self.probability_interval_low <= self.probability <= self.probability_interval_high or not 0 < self.interval_confidence < 1:
            raise ValueError("probability interval/confidence is invalid")
        calibration = self.calibration
        for field in ("strategy_name", "strategy_version", "strategy_policy_sha256", "structure", "holding_sessions",
                      "valuation_policy_sha256", "cost_policy_sha256", "outcome_policy_sha256", "management_policy_sha256"):
            if getattr(calibration, field) != getattr(self.binding, field):
                raise ValueError(f"calibration {field} does not match the option outcome contract")
        if calibration.behavior_policy_sha256 != self.stock_policy_sha256 or calibration.behavior_definition_sha256 != self.stock_definition_sha256:
            raise ValueError("calibration does not match stock behavior policy")
        if calibration.scope_sha256 != self.binding.calibration_scope_sha256:
            raise ValueError("calibration population/applicability scope mismatch")
        if not calibration.effective_from <= self.binding.decision_at < calibration.valid_until or not self.economics.costs_included:
            raise ValueError("calibration must be effective at decision and net of declared costs")
        if calibration.outcome_basis == "QUOTE_PAPER_FILLS" and self.economics.basis != "QUOTE_SCENARIO":
            raise ValueError("fill-calibrated results cannot be applied to indicative-only economics")
        if calibration.target == "TARGET_BEFORE_STOP_WITHIN_HOLD" and self.economics.target_stop_reward_risk is None:
            raise ValueError("first-hit probability requires defined target and stop")
        return self


def bind_option_outcome(
    snapshot: StockBehaviorSnapshot, estimate: OptionOutcomeEstimate, *,
    verified_calibration: CalibrationEvidence | None = None,
) -> OptionOutcomeEstimate:
    binding = estimate.binding
    if (estimate.stock_context_id != snapshot.snapshot_id or estimate.stock_payload_sha256 != snapshot.sha256
            or estimate.stock_definition_sha256 != snapshot.definition_sha256 or estimate.stock_policy_sha256 != snapshot.policy_sha256
            or snapshot.security_id != binding.security_id):
        raise ValueError("option estimate does not bind the exact stock snapshot")
    if snapshot.market_time > binding.market_time or not snapshot.available_at <= binding.decision_at < snapshot.valid_until:
        raise ValueError("stock snapshot is not usable at option decision")
    if snapshot.assess_at(binding.decision_at).data_status != "READY" or snapshot.availability_mode != "PROSPECTIVE_RECEIPT":
        raise ValueError("option estimate requires ready prospective stock evidence")
    if estimate.probability_status == "CALIBRATED" and verified_calibration != estimate.calibration:
        raise ValueError("calibrated probability requires independently resolved calibration evidence")
    return estimate