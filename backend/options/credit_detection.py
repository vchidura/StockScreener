from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256
from options.dual_origin import LatestCompletedSignalDecision
from options.domain import OptionContractSnapshot
from options.strategies.domain import OptionCandidate, StructureRiskClass, StructureType, canonical_json


O3_CREDIT_POLICY = {
    "version": "o3_options_first_credit_v2",
    "detector_id": "O3",
    "origin": "OPTIONS_FIRST",
    "structures": ["PUT_CREDIT_VERTICAL", "CALL_CREDIT_VERTICAL"],
    "direction_mapping": {"1": "PUT_CREDIT_VERTICAL", "-1": "CALL_CREDIT_VERTICAL"},
    "minimum_entry_dte": 7,
    "maximum_entry_dte": 30,
    "minimum_leg_open_interest": 500,
    "minimum_leg_day_volume": 50,
    "minimum_exit_dte": 3,
    "stop_loss_multiple": "2.0",
    "take_profit_fraction": "0.5",
    "maximum_source_age_seconds": 1800,
    "source_session_cap": "SOURCE_SESSION_CLOSE",
    "pricing_basis": "ORIGINAL_COHERENT_MODEL_MARKS",
    "output": "QUALIFIED_INDICATIVE_RESEARCH",
    "publication_permission": False,
    "execution_permission": False,
}
O3_CREDIT_POLICY_SHA256 = hashlib.sha256(canonical_json(O3_CREDIT_POLICY).encode("ascii")).hexdigest()


class O3CreditLeg(Contract):
    leg_index: int = Field(strict=True, ge=0)
    snapshot_id: UUID
    contract_id: int = Field(strict=True, gt=0)
    contract_ticker: Name
    side: Literal["BUY", "SELL"]
    ratio: int = Field(strict=True, gt=0)
    multiplier: Literal[100]
    expiration_date: date
    strike: Decimal
    spot: Decimal
    model_mark: Decimal
    local_iv: float
    local_delta: float
    local_gamma: float
    local_theta_per_day: float
    local_vega_per_vol_point: float
    local_rho_per_rate_point: float
    day_volume: int | None
    open_interest: int | None
    bid: Decimal | None
    ask: Decimal | None
    source_market_time: AwareDatetime
    mark_source: Name
    valuation_policy_version: Name
    valuation_policy_sha256: Sha256


class O3CreditObservation(Contract):
    schema_version: Literal["option_o3_credit_observation_v1"] = "option_o3_credit_observation_v1"
    detector_id: Literal["O3"] = "O3"
    origin: Literal["OPTIONS_FIRST"] = "OPTIONS_FIRST"
    policy_version: Literal["o3_options_first_credit_v2"] = "o3_options_first_credit_v2"
    policy_sha256: Sha256 = O3_CREDIT_POLICY_SHA256
    scheduled_cycle: AwareDatetime
    decision_at: AwareDatetime
    valid_until: AwareDatetime
    matrix_id: UUID
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    underlyer: Name
    direction: Literal[-1, 1]
    candidate_rank: int = Field(strict=True, gt=0)
    structure_type: Literal["PUT_CREDIT_VERTICAL", "CALL_CREDIT_VERTICAL"]
    activity_episode_id: UUID
    activity_contract_id: int = Field(strict=True, gt=0)
    stock_decision_sha256: Sha256
    legs: tuple[O3CreditLeg, O3CreditLeg]
    net_credit: Decimal
    maximum_profit: Decimal
    maximum_loss: Decimal
    breakeven: Decimal
    return_on_risk: Decimal
    structural_invalidation: Decimal
    disposition: Literal["QUALIFIED_INDICATIVE"] = "QUALIFIED_INDICATIVE"
    pricing_basis: Literal["ORIGINAL_COHERENT_MODEL_MARKS"] = "ORIGINAL_COHERENT_MODEL_MARKS"
    probability: None = None
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @property
    def recurrence_sha256(self) -> str:
        return hashlib.sha256(canonical_json({
            "policy": self.policy_sha256,
            "candidate": self.candidate_identity_sha256,
            "activity_episode": str(self.activity_episode_id),
            "stock_decision": self.stock_decision_sha256,
        }).encode("ascii")).hexdigest()

    @model_validator(mode="after")
    def validate_observation(self):
        expected = "PUT_CREDIT_VERTICAL" if self.direction == 1 else "CALL_CREDIT_VERTICAL"
        if self.policy_sha256 != O3_CREDIT_POLICY_SHA256 or self.structure_type != expected:
            raise ValueError("unsupported O3 credit policy or direction mapping")
        if not self.scheduled_cycle <= self.decision_at < self.valid_until:
            raise ValueError("O3 observation clocks are not causal")
        if tuple(leg.leg_index for leg in self.legs) != (0, 1):
            raise ValueError("O3 legs must be exact and ordered")
        short = next((leg for leg in self.legs if leg.side == "SELL"), None)
        long = next((leg for leg in self.legs if leg.side == "BUY"), None)
        if short is None or long is None or short.ratio != 1 or long.ratio != 1:
            raise ValueError("O3 requires one short and one long 1:1 leg")
        if (self.structure_type == "PUT_CREDIT_VERTICAL" and not long.strike < short.strike
                or self.structure_type == "CALL_CREDIT_VERTICAL" and not short.strike < long.strike):
            raise ValueError("O3 credit wing ordering is invalid")
        if (self.direction == 1 and short.strike > self.structural_invalidation
                or self.direction == -1 and short.strike < self.structural_invalidation):
            raise ValueError("O3 short strike is inside structural invalidation")
        values = (self.net_credit, self.maximum_profit, self.maximum_loss, self.breakeven, self.return_on_risk)
        if any(not value.is_finite() or value <= 0 for value in values):
            raise ValueError("O3 economics must be finite and positive")
        if self.maximum_profit != self.net_credit or self.return_on_risk != self.maximum_profit / self.maximum_loss:
            raise ValueError("O3 payoff arithmetic mismatch")
        return self


def assess_o3_credit(
    candidate: OptionCandidate,
    decision: LatestCompletedSignalDecision,
    *,
    activity_contract_id: int,
    snapshots: tuple[OptionContractSnapshot, ...],
    structural_invalidation: Decimal,
    scheduled_cycle: datetime,
) -> O3CreditObservation:
    if decision.detector_id != "O1" or decision.origin != "OPTIONS_FIRST" or decision.disposition != "CONFIRMED":
        raise ValueError("O3 requires exact confirmed options-first stock direction")
    expected = StructureType.PUT_CREDIT_VERTICAL if decision.direction == 1 else StructureType.CALL_CREDIT_VERTICAL
    if (candidate.strategy_name != "SPREAD_RANGE_LOCATOR" or candidate.structure_type is not expected
            or candidate.structure_risk_class is not StructureRiskClass.DEFINED_RISK_CREDIT
            or candidate.net_premium is None or candidate.net_premium <= 0):
        raise ValueError("O3 candidate does not match confirmed credit direction")
    if candidate.maximum_profit is None or candidate.maximum_loss is None or len(candidate.breakevens) != 1:
        raise ValueError("O3 requires bounded payoff and one breakeven")
    if activity_contract_id not in {leg.contract_id for leg in candidate.legs}:
        raise ValueError("O3 activity trigger must bind one exact package contract")
    by_snapshot = {snapshot.snapshot_id: snapshot for snapshot in snapshots}
    if set(by_snapshot) != {leg.snapshot_id for leg in candidate.legs}:
        raise ValueError("O3 requires every exact retained package snapshot")
    dte = (candidate.expiration_date - candidate.market_data_time.date()).days
    if not O3_CREDIT_POLICY["minimum_entry_dte"] <= dte <= O3_CREDIT_POLICY["maximum_entry_dte"]:
        raise ValueError("O3 entry DTE is outside policy")
    if any((snapshot.open_interest or 0) < O3_CREDIT_POLICY["minimum_leg_open_interest"]
            or (snapshot.day_volume or 0) < O3_CREDIT_POLICY["minimum_leg_day_volume"] for snapshot in snapshots):
        raise ValueError("O3 leg activity is below policy")
    legs = tuple(O3CreditLeg(leg_index=leg.leg_index, snapshot_id=leg.snapshot_id, contract_id=leg.contract_id,
        contract_ticker=leg.contract_ticker, side=leg.side.value, ratio=leg.ratio, multiplier=leg.multiplier,
        expiration_date=leg.expiration_date, strike=leg.strike, spot=leg.spot, model_mark=leg.model_mark,
        local_iv=leg.local_iv, local_delta=leg.local_delta, local_gamma=leg.local_gamma,
        local_theta_per_day=leg.local_theta_per_day, local_vega_per_vol_point=leg.local_vega_per_vol_point,
        local_rho_per_rate_point=leg.local_rho_per_rate_point,
        day_volume=by_snapshot[leg.snapshot_id].day_volume,
        open_interest=by_snapshot[leg.snapshot_id].open_interest,
        bid=by_snapshot[leg.snapshot_id].bid, ask=by_snapshot[leg.snapshot_id].ask,
        source_market_time=leg.source_market_time, mark_source=leg.mark_source,
        valuation_policy_version=leg.valuation_policy_version,
        valuation_policy_sha256=leg.valuation_policy_sha256) for leg in candidate.legs)
    valid_until = min(candidate.valid_until, decision.valid_until)
    return O3CreditObservation(scheduled_cycle=scheduled_cycle, decision_at=decision.decision_at,
        valid_until=valid_until, matrix_id=candidate.matrix_id, candidate_id=candidate.candidate_id,
        candidate_identity_sha256=candidate.identity_sha256, underlyer=candidate.underlyer,
        direction=decision.direction, candidate_rank=candidate.rank, structure_type=candidate.structure_type.value,
        activity_episode_id=decision.activity.episode_id, activity_contract_id=activity_contract_id,
        stock_decision_sha256=decision.sha256, legs=legs, net_credit=candidate.net_premium,
        maximum_profit=candidate.maximum_profit, maximum_loss=candidate.maximum_loss,
        breakeven=candidate.breakevens[0], return_on_risk=candidate.maximum_profit / candidate.maximum_loss,
        structural_invalidation=structural_invalidation)