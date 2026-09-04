from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from options.domain import ContractType


class OptionSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class CandidateStatus(str, Enum):
    SELECTED = "SELECTED"
    SUPPRESSED = "SUPPRESSED"
    REJECTED = "REJECTED"


class CandidateKind(str, Enum):
    RESEARCH_ONLY = "RESEARCH_ONLY"
    SINGLE_CONTRACT = "SINGLE_CONTRACT"
    MULTI_LEG = "MULTI_LEG"


class StructureRiskClass(str, Enum):
    RESEARCH_CONTEXT = "RESEARCH_CONTEXT"
    CASH_SECURED = "CASH_SECURED"
    DEFINED_RISK_CREDIT = "DEFINED_RISK_CREDIT"
    PREMIUM_AT_RISK_DEBIT = "PREMIUM_AT_RISK_DEBIT"


class StructureType(str, Enum):
    CASH_SECURED_PUT = "CASH_SECURED_PUT"
    LONG_CALL = "LONG_CALL"
    LONG_PUT = "LONG_PUT"
    CALL_DEBIT_VERTICAL = "CALL_DEBIT_VERTICAL"
    PUT_DEBIT_VERTICAL = "PUT_DEBIT_VERTICAL"
    PUT_CREDIT_VERTICAL = "PUT_CREDIT_VERTICAL"
    CALL_CREDIT_VERTICAL = "CALL_CREDIT_VERTICAL"
    IRON_CONDOR = "IRON_CONDOR"
    CALL_BUTTERFLY = "CALL_BUTTERFLY"
    PUT_BUTTERFLY = "PUT_BUTTERFLY"
    SWEEP_LIKE_CLUSTER = "SWEEP_LIKE_CLUSTER"
    VOLUME_OI_ANOMALY = "VOLUME_OI_ANOMALY"
    VOLATILITY_DISTORTION = "VOLATILITY_DISTORTION"


class ExecutionEligibility(str, Enum):
    PAPER_PROXY = "PAPER_PROXY"
    LIVE_CANDIDATE = "LIVE_CANDIDATE"


class StrategyContextStatus(str, Enum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class StrategyContextSnapshot:
    context_snapshot_id: UUID
    matrix_id: UUID
    underlyer: str
    market_data_time: datetime
    observed_time: datetime
    status: StrategyContextStatus
    daily_close: Decimal | None
    daily_ema_50: Decimal | None
    daily_input_bars: int
    hourly_close: Decimal | None
    hourly_ema_20: Decimal | None
    hourly_input_bars: int
    trend_state: str | None
    earnings_blackout_state: str
    fed_blackout_state: str
    quote_spread_state: str
    reason_codes: tuple[str, ...]
    source_bar_keys: tuple[str, ...]
    policy_version: str
    policy_sha256: str
    equity_context_snapshot_id: UUID | None = None
    equity_context_status: str | None = None
    qualified_direction: str | None = None
    company_name: str | None = None
    market_cap: Decimal | None = None
    shares_outstanding: Decimal | None = None
    free_float: Decimal | None = None
    dividend_yield: float | None = None
    enterprise_value: Decimal | None = None
    ebitda: Decimal | None = None
    operating_income: Decimal | None = None
    free_cash_flow: Decimal | None = None
    equity_reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.underlyer.strip():
            raise ValueError("underlyer cannot be blank")
        market_time = _as_utc(self.market_data_time, "market_data_time")
        observed_time = _as_utc(self.observed_time, "observed_time")
        if observed_time < market_time:
            raise ValueError("observed_time cannot precede market_data_time")
        if self.daily_input_bars < 0 or self.hourly_input_bars < 0:
            raise ValueError("context input counts cannot be negative")
        for value, name in (
            (self.daily_close, "daily_close"),
            (self.daily_ema_50, "daily_ema_50"),
            (self.hourly_close, "hourly_close"),
            (self.hourly_ema_20, "hourly_ema_20"),
            (self.market_cap, "market_cap"),
            (self.shares_outstanding, "shares_outstanding"),
            (self.free_float, "free_float"),
            (self.enterprise_value, "enterprise_value"),
            (self.ebitda, "ebitda"),
            (self.operating_income, "operating_income"),
            (self.free_cash_flow, "free_cash_flow"),
        ):
            _finite_decimal(value, name)
        if self.dividend_yield is not None:
            _finite_float(self.dividend_yield, "dividend_yield")
            if not 0 <= self.dividend_yield <= 1:
                raise ValueError("dividend_yield must be in [0, 1]")
        if self.qualified_direction not in (None, "BULLISH", "BEARISH", "NEUTRAL"):
            raise ValueError("qualified_direction is invalid")
        if type(self.equity_reason_codes) is not tuple:
            raise TypeError("equity_reason_codes must be a tuple")
        object.__setattr__(self, "market_data_time", market_time)
        object.__setattr__(self, "observed_time", observed_time)


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _finite_float(value: float | None, name: str) -> None:
    if value is not None and (type(value) is not float or not math.isfinite(value)):
        raise TypeError(f"{name} must be a finite float")


def _finite_decimal(value: Decimal | None, name: str) -> None:
    if value is not None and (type(value) is not Decimal or not value.is_finite()):
        raise TypeError(f"{name} must be a finite Decimal")


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def candidate_identity(
    matrix_id: UUID,
    strategy_name: str,
    strategy_version: str,
    structure_type: StructureType,
    ordered_contract_ids: tuple[int, ...],
    trigger_type: str,
) -> tuple[UUID, str]:
    payload = canonical_json(
        {
            "matrix_id": str(matrix_id),
            "ordered_contract_ids": ordered_contract_ids,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "structure_type": structure_type.value,
            "trigger_type": trigger_type,
        }
    )
    digest = hashlib.sha256(payload.encode("ascii")).hexdigest()
    return uuid5(NAMESPACE_URL, f"option-candidate:{digest}"), digest


def signal_identity_sha256(
    underlyer: str,
    strategy_name: str,
    strategy_version: str,
    policy_sha256: str,
    structure_type: StructureType,
    ordered_legs: tuple[tuple[int, str, int, int], ...],
) -> str:
    payload = canonical_json(
        {
            "underlyer": underlyer,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "policy_sha256": policy_sha256,
            "structure_type": structure_type.value,
            "ordered_legs": ordered_legs,
        }
    )
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class CandidateLeg:
    leg_index: int
    snapshot_id: UUID
    contract_id: int
    contract_ticker: str
    side: OptionSide
    ratio: int
    multiplier: int
    expiration_date: date
    strike: Decimal
    contract_type: ContractType
    spot: Decimal
    time_to_expiration_years: float
    risk_free_rate: float
    dividend_yield: float
    model_mark: Decimal
    local_iv: float
    local_delta: float
    local_gamma: float
    local_theta_per_day: float
    local_vega_per_vol_point: float
    local_rho_per_rate_point: float
    source_market_time: datetime
    mark_source: str
    model_version: str
    quality_flags: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.leg_index < 0:
            raise ValueError("leg_index cannot be negative")
        if self.contract_id <= 0 or self.ratio <= 0 or self.multiplier <= 0:
            raise ValueError("contract_id, ratio, and multiplier must be positive")
        if not self.contract_ticker.strip() or not self.mark_source.strip() or not self.model_version.strip():
            raise ValueError("contract_ticker, mark_source, and model_version cannot be blank")
        for value, name in (
            (self.strike, "strike"),
            (self.spot, "spot"),
            (self.model_mark, "model_mark"),
        ):
            _finite_decimal(value, name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        for value, name in (
            (self.local_iv, "local_iv"),
            (self.time_to_expiration_years, "time_to_expiration_years"),
            (self.risk_free_rate, "risk_free_rate"),
            (self.dividend_yield, "dividend_yield"),
            (self.local_delta, "local_delta"),
            (self.local_gamma, "local_gamma"),
            (self.local_theta_per_day, "local_theta_per_day"),
            (self.local_vega_per_vol_point, "local_vega_per_vol_point"),
            (self.local_rho_per_rate_point, "local_rho_per_rate_point"),
        ):
            _finite_float(value, name)
        if self.time_to_expiration_years <= 0:
            raise ValueError("time_to_expiration_years must be positive")
        object.__setattr__(
            self,
            "source_market_time",
            _as_utc(self.source_market_time, "source_market_time"),
        )
        if type(self.quality_flags) is not tuple:
            raise TypeError("quality_flags must be a tuple")


@dataclass(frozen=True, slots=True)
class OptionCandidate:
    candidate_id: UUID
    identity_sha256: str
    matrix_id: UUID
    strategy_name: str
    strategy_version: str
    underlyer: str
    candidate_kind: CandidateKind
    strategy_archetype: str
    persona_tags: tuple[str, ...]
    structure_type: StructureType
    structure_risk_class: StructureRiskClass
    expiration_date: date | None
    rank: int
    status: CandidateStatus
    primary_metric_name: str | None
    primary_metric_value: float | None
    rank_components: Mapping[str, object]
    primary_evidence: Mapping[str, object]
    legs: tuple[CandidateLeg, ...]
    net_premium: Decimal | None
    collateral_required: Decimal | None
    capital_at_risk: Decimal | None
    maximum_profit: Decimal | None
    maximum_loss: Decimal | None
    return_on_collateral: float | None
    return_on_risk: float | None
    breakevens: tuple[Decimal, ...]
    execution_eligibility: ExecutionEligibility | None
    reason_codes: tuple[str, ...]
    management_policy_version: str | None
    management_policy: Mapping[str, object]
    policy_sha256: str
    model_version: str
    context_snapshot_id: UUID | None
    iv_context_id: UUID | None
    market_data_time: datetime
    observed_time: datetime
    valid_until: datetime | None

    def __post_init__(self) -> None:
        if len(self.identity_sha256) != 64:
            raise ValueError("identity_sha256 must be a SHA-256 hex digest")
        if not self.strategy_name.strip() or not self.strategy_version.strip():
            raise ValueError("strategy name and version cannot be blank")
        if not self.underlyer.strip() or not self.strategy_archetype.strip():
            raise ValueError("underlyer and strategy_archetype cannot be blank")
        if self.rank <= 0:
            raise ValueError("rank must be positive")
        if not self.persona_tags:
            raise ValueError("persona_tags cannot be empty")
        if tuple(leg.leg_index for leg in self.legs) != tuple(range(len(self.legs))):
            raise ValueError("candidate legs must have contiguous ordered indexes")
        if self.candidate_kind is CandidateKind.RESEARCH_ONLY and self.legs:
            raise ValueError("research-only candidates cannot contain signal legs")
        if self.candidate_kind is not CandidateKind.RESEARCH_ONLY and not self.legs:
            raise ValueError("contract candidates require at least one leg")
        if self.status is not CandidateStatus.SELECTED and self.execution_eligibility:
            raise ValueError("non-selected candidates cannot be execution eligible")
        if self.candidate_kind is CandidateKind.RESEARCH_ONLY and self.execution_eligibility:
            raise ValueError("research-only candidates cannot be execution eligible")
        if self.maximum_loss is not None and self.maximum_loss <= 0:
            raise ValueError("maximum_loss must be positive")
        for value, name in (
            (self.net_premium, "net_premium"),
            (self.collateral_required, "collateral_required"),
            (self.capital_at_risk, "capital_at_risk"),
            (self.maximum_profit, "maximum_profit"),
            (self.maximum_loss, "maximum_loss"),
        ):
            _finite_decimal(value, name)
        for value, name in (
            (self.primary_metric_value, "primary_metric_value"),
            (self.return_on_collateral, "return_on_collateral"),
            (self.return_on_risk, "return_on_risk"),
        ):
            _finite_float(value, name)
        market_time = _as_utc(self.market_data_time, "market_data_time")
        observed_time = _as_utc(self.observed_time, "observed_time")
        if observed_time < market_time:
            raise ValueError("observed_time cannot precede market_data_time")
        valid_until = (
            _as_utc(self.valid_until, "valid_until")
            if self.valid_until is not None
            else None
        )
        if valid_until is not None and valid_until <= market_time:
            raise ValueError("valid_until must follow market_data_time")
        if any(leg.source_market_time > market_time for leg in self.legs):
            raise ValueError("candidate legs cannot be future-visible at market_data_time")
        object.__setattr__(self, "market_data_time", market_time)
        object.__setattr__(self, "observed_time", observed_time)
        object.__setattr__(self, "valid_until", valid_until)


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    scenario_result_id: UUID
    candidate_id: UUID
    scenario_key: str
    spot_shock_fraction: float
    iv_shock_fraction: float
    time_fraction_remaining: float
    repriced_value: Decimal | None
    profit_loss: Decimal | None
    delta: float | None
    gamma: float | None
    theta_per_day: float | None
    vega_per_vol_point: float | None
    terminal: bool
    assumptions: Mapping[str, object]
    quality_flags: tuple[str, ...]
    model_version: str
    policy_sha256: str

    def __post_init__(self) -> None:
        if not 0 <= self.time_fraction_remaining <= 1:
            raise ValueError("time_fraction_remaining must be in [0, 1]")
        for value, name in (
            (self.spot_shock_fraction, "spot_shock_fraction"),
            (self.iv_shock_fraction, "iv_shock_fraction"),
            (self.time_fraction_remaining, "time_fraction_remaining"),
            (self.delta, "delta"),
            (self.gamma, "gamma"),
            (self.theta_per_day, "theta_per_day"),
            (self.vega_per_vol_point, "vega_per_vol_point"),
        ):
            _finite_float(value, name)
        _finite_decimal(self.repriced_value, "repriced_value")
        _finite_decimal(self.profit_loss, "profit_loss")