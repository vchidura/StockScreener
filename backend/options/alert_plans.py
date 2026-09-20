from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, Field, model_validator
from equity.behavior import Contract, Name, Sha256

from options.alert_qualification import (
    RETAINED_EVIDENCE_VERSION,
    AlertCapability, qualify_option_alert, retained_alert_evidence,
    retained_leg, retained_time, validate_alert_package,
)
from options.strategies.domain import StructureType


PLAN_VERSION = "option_alert_plan_v1"


class TechnicalExitPolicy(Contract):
    version: Literal["option_technical_exit_policy_v1"] = "option_technical_exit_policy_v1"
    hard_stop_fraction: Literal["0.35"] = "0.35"
    hard_profit_fraction: Literal["0.50"] = "0.50"
    maximum_hold_seconds: Literal[7200] = 7200
    minimum_exit_dte: Literal[1] = 1
    minimum_reward_risk: Literal["1"] = "1"
    structure_buffer_atr: Literal["0.10"] = "0.10"
    stop_trigger: Literal["FINALIZED_SOURCE_INTERVAL_CLOSE"] = "FINALIZED_SOURCE_INTERVAL_CLOSE"
    target_trigger: Literal["FINALIZED_SOURCE_INTERVAL_CLOSE"] = "FINALIZED_SOURCE_INTERVAL_CLOSE"
    level_updates: Literal["FROZEN_AT_ADMISSION"] = "FROZEN_AT_ADMISSION"
    execution_permission: Literal[False] = False


TECHNICAL_EXIT_POLICY = TechnicalExitPolicy()


class TechnicalLevel(Contract):
    level_id: Name
    role: Literal["INVALIDATION", "OPPOSING_STRUCTURE", "FIBONACCI"]
    price: Decimal = Field(gt=0, allow_inf_nan=False)


class TechnicalExitEvidence(Contract):
    schema_version: Literal["option_technical_levels_v1"] = "option_technical_levels_v1"
    security_id: UUID
    underlyer: Name
    direction: Literal[-1, 1]
    source_kind: Literal["STOCK_SETUP", "STRUCTURE_SNAPSHOT"]
    source_policy_sha256: Sha256
    source_payload_sha256: Sha256
    source_revision_ids: tuple[UUID, ...] = Field(min_length=1, max_length=1000)
    interval: Literal["30m", "1h"]
    market_time: AwareDatetime
    available_at: AwareDatetime
    received_at: AwareDatetime
    valid_until: AwareDatetime
    atr: Decimal = Field(gt=0, allow_inf_nan=False)
    levels: tuple[TechnicalLevel, ...] = Field(min_length=2, max_length=128)
    price_basis: Literal["RAW_ACTION_GATED"] = "RAW_ACTION_GATED"
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"

    @model_validator(mode="after")
    def validate_source(self):
        if not self.market_time <= self.available_at <= self.received_at < self.valid_until:
            raise ValueError("technical levels require causal unexpired source receipts")
        if len(set(self.source_revision_ids)) != len(self.source_revision_ids):
            raise ValueError("technical level revisions must be distinct")
        if len({level.level_id for level in self.levels}) != len(self.levels):
            raise ValueError("technical levels require distinct identities")
        if sum(level.role == "INVALIDATION" for level in self.levels) != 1:
            raise ValueError("technical levels require one original structural invalidation")
        return self


def technical_exit_terms(*, evidence, trusted_source_policy_sha256, security_id, underlyer,
                         direction, stock_entry, entry_debit, decision_at, exit_deadline, session_close):
    evidence = TechnicalExitEvidence.model_validate_json(evidence.canonical_json())
    if (evidence.source_policy_sha256 != trusted_source_policy_sha256
            or (evidence.security_id, evidence.underlyer, evidence.direction) != (security_id, underlyer, direction)
            or not evidence.received_at <= decision_at < evidence.valid_until):
        raise ValueError("technical levels do not match the trusted decision source")
    if (any(value.utcoffset() is None for value in (decision_at, exit_deadline, session_close))
            or not decision_at < exit_deadline <= min(session_close,
                decision_at + timedelta(seconds=TECHNICAL_EXIT_POLICY.maximum_hold_seconds))):
        raise ValueError("technical management exceeds session or elapsed hold cutoff")
    if any(type(value) is not Decimal or not value.is_finite() or value <= 0 for value in (stock_entry, entry_debit)):
        raise ValueError("technical management requires positive exact entry prices")
    invalidation = next(level for level in evidence.levels if level.role == "INVALIDATION")
    buffer = evidence.atr * Decimal(TECHNICAL_EXIT_POLICY.structure_buffer_atr)
    stop = invalidation.price if evidence.source_kind == "STOCK_SETUP" else invalidation.price - direction * buffer
    candidates = [level for level in evidence.levels if level.role == "OPPOSING_STRUCTURE"
        and direction * (level.price - stock_entry) > 0]
    if not candidates or direction * (stock_entry - invalidation.price) <= 0 or stop <= 0:
        raise ValueError("technical management requires an intact directional structural bracket")
    target = min(candidates, key=lambda level: (direction * (level.price - stock_entry), level.level_id))
    risk, reward = direction * (stock_entry - stop), direction * (target.price - stock_entry)
    if reward < risk * Decimal(TECHNICAL_EXIT_POLICY.minimum_reward_risk):
        raise ValueError("nearest structural target has insufficient reward to risk")
    confluence = sorted(level.level_id for level in evidence.levels
        if level.role == "FIBONACCI" and abs(level.price - target.price) <= buffer)
    return dict(version=TECHNICAL_EXIT_POLICY.version, policy_sha256=TECHNICAL_EXIT_POLICY.sha256,
        evidence_sha256=evidence.sha256, evidence=evidence.model_dump(mode="json"),
        underlying_entry=str(stock_entry), underlying_stop=str(stop), underlying_target=str(target.price),
        stop_level_id=invalidation.level_id, target_level_id=target.level_id,
        fibonacci_confluence=confluence, reward_risk=str(reward / risk),
        hard_stop_package_value=str(entry_debit * (1 - Decimal(TECHNICAL_EXIT_POLICY.hard_stop_fraction))),
        hard_profit_package_value=str(entry_debit * (1 + Decimal(TECHNICAL_EXIT_POLICY.hard_profit_fraction))),
        exit_deadline=exit_deadline.isoformat(), source_interval=evidence.interval,
        technical_trigger="FINALIZED_SOURCE_INTERVAL_CLOSE", level_updates="FROZEN_AT_ADMISSION",
        package_values_basis="PLANNED_ENTRY_DEBIT_NOT_FILL", execution_permission=False)


@dataclass(frozen=True, slots=True)
class AlertManagementPolicy:
    policy_version: str
    strategy_name: str
    stop_loss_fraction: Decimal
    take_profit_fraction: Decimal
    maximum_hold_seconds: int
    minimum_exit_dte: int = 1

    def __post_init__(self):
        if not self.policy_version.strip() or self.strategy_name not in {"DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD"}:
            raise ValueError("explicit management supports versioned long-premium and debit-spread strategies")
        for value in (self.stop_loss_fraction, self.take_profit_fraction):
            if type(value) is not Decimal or not value.is_finite() or value <= 0:
                raise ValueError("management fractions must be positive finite Decimals")
        if self.stop_loss_fraction >= 1:
            raise ValueError("debit stop must be below the full premium loss")
        if type(self.maximum_hold_seconds) is not int or not 0 < self.maximum_hold_seconds <= 366 * 86400:
            raise ValueError("maximum hold must be positive elapsed seconds within 366 days")
        if type(self.minimum_exit_dte) is not int or self.minimum_exit_dte < 1:
            raise ValueError("initial managed debit plans must exit before expiration day")

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_version": self.policy_version, "strategy_name": self.strategy_name,
            "stop_loss_fraction": str(self.stop_loss_fraction),
            "take_profit_fraction": str(self.take_profit_fraction),
            "maximum_hold_seconds": self.maximum_hold_seconds,
            "exit_dte": self.minimum_exit_dte,
            "basis": "ENTRY_DEBIT", "hold_clock": "ELAPSED_SECONDS",
            "validation_status": "DEVELOPMENT_POLICY_NOT_CALIBRATED",
        }

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_canonical(self.to_dict()).encode("ascii")).hexdigest()


def estimate_technical_package_values(candidate, terms, *, decision_at, exit_deadline):
    from math import isfinite
    from options.analytics.greeks import price_and_greeks
    from options.strategies.domain import OptionSide

    scenarios = []
    for label in ("underlying_stop", "underlying_target"):
        spot = float(terms[label])
        for scenario_at in (decision_at, exit_deadline):
            elapsed_years = (scenario_at - candidate.market_data_time).total_seconds() / (365 * 86400)
            for iv_change in (-0.20, 0.0, 0.20):
                value = Decimal("0")
                for leg in candidate.legs:
                    maturity = leg.time_to_expiration_years - elapsed_years
                    if maturity <= 0 or leg.local_iv is None or not isfinite(leg.local_iv) or leg.local_iv <= 0:
                        raise ValueError("technical repricing requires unexpired exact IV inputs")
                    price, *_ = price_and_greeks(leg.contract_type, spot=spot, strike=float(leg.strike),
                        maturity=maturity, rate=leg.risk_free_rate, dividend=leg.dividend_yield,
                        volatility=leg.local_iv * (1 + iv_change))
                    if not isfinite(price) or price < 0:
                        raise ValueError("technical repricing returned an invalid price")
                    value += Decimal(str(price)) * leg.ratio * leg.multiplier * (1 if leg.side is OptionSide.BUY else -1)
                scenarios.append(dict(level=label, underlying_price=str(terms[label]), scenario_at=scenario_at.isoformat(),
                    iv_relative_change=iv_change, package_value=str(value), basis="MODEL_SCENARIO_NOT_FILL_OR_FORECAST"))
    return scenarios


class TechnicalExitObservation(Contract):
    plan_sha256: Sha256
    security_id: UUID
    market_time: AwareDatetime
    recorded_at: AwareDatetime
    received_at: AwareDatetime
    underlying_close: Decimal | None = Field(default=None, gt=0, allow_inf_nan=False)
    stock_interval: Literal["30m", "1h"] | None = None
    stock_bar_revision_id: UUID | None = None
    package_value: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    leg_snapshot_ids: tuple[UUID, ...] = Field(default=(), max_length=2)
    price_basis: Literal["RAW_ACTION_GATED"] = "RAW_ACTION_GATED"
    finalized: Literal[True] = True

    @model_validator(mode="after")
    def validate_observation(self):
        if not self.market_time <= self.recorded_at <= self.received_at:
            raise ValueError("exit observation receipts must be causal")
        if (self.underlying_close is not None) != (self.stock_bar_revision_id is not None and self.stock_interval is not None):
            raise ValueError("technical close requires its exact finalized interval and bar")
        if (self.package_value is not None) != bool(self.leg_snapshot_ids) or len(set(self.leg_snapshot_ids)) != len(self.leg_snapshot_ids):
            raise ValueError("package observation requires exact distinct leg snapshots")
        return self


def assess_technical_exit(plan, observation, *, as_of, maximum_source_age_seconds):
    if type(maximum_source_age_seconds) is not int or not 0 < maximum_source_age_seconds <= 1800:
        raise ValueError("exit assessment requires an explicit bounded source-age policy")
    observation = TechnicalExitObservation.model_validate_json(observation.canonical_json())
    payload = json.loads(plan.payload_json)
    if payload.get("version") != "dual_origin_indicative_plan_v2":
        raise ValueError("technical exit assessment requires the technical plan version")
    technical = payload["management_policy"]["technical_exit"]
    evidence = TechnicalExitEvidence.model_validate(technical["evidence"])
    from options.calendar import OptionExchangeCalendar

    expected = technical_exit_terms(evidence=evidence, trusted_source_policy_sha256=evidence.source_policy_sha256,
        security_id=evidence.security_id, underlyer=evidence.underlyer, direction=payload["direction"],
        stock_entry=Decimal(technical["underlying_entry"]), entry_debit=Decimal(payload["entry_limit"]),
        decision_at=retained_time(payload["decision_at"]), exit_deadline=retained_time(payload["exit_deadline"]),
        session_close=OptionExchangeCalendar().session_close(retained_time(payload["source_market_time"]).astimezone(ZoneInfo("America/New_York")).date()))
    if any(technical.get(key) != value for key, value in expected.items()):
        raise ValueError("technical exit plan has altered policy or levels")
    if (observation.plan_sha256 != plan.sha256 or observation.security_id != evidence.security_id
            or as_of.utcoffset() is None or observation.received_at > as_of
            or observation.market_time < retained_time(payload["planned_entry_at"])):
        raise ValueError("exit observation does not match original plan identity or clocks")
    reasons, missing = [], []
    fresh = 0 <= (as_of - observation.market_time).total_seconds() <= maximum_source_age_seconds
    if observation.underlying_close is None or not fresh or observation.stock_interval != technical["source_interval"]:
        missing.append("TECHNICAL_CLOSE_UNAVAILABLE")
    else:
        direction = payload["direction"]
        if direction * (observation.underlying_close - Decimal(technical["underlying_stop"])) <= 0:
            reasons.append("TECHNICAL_STOP")
        if direction * (observation.underlying_close - Decimal(technical["underlying_target"])) >= 0:
            reasons.append("TECHNICAL_TARGET")
    if observation.package_value is None or not fresh:
        missing.append("PACKAGE_MARK_UNAVAILABLE")
    else:
        if observation.package_value <= Decimal(technical["hard_stop_package_value"]):
            reasons.append("HARD_PREMIUM_STOP")
        if observation.package_value >= Decimal(technical["hard_profit_package_value"]):
            reasons.append("HARD_PREMIUM_TARGET")
    if as_of >= retained_time(payload["exit_deadline"]):
        reasons.append("TIME_LIMIT")
    return dict(status="EXIT_TRIGGERED" if reasons else "UNAVAILABLE" if missing else "MONITOR",
        reasons=reasons, unavailable_inputs=missing, plan_sha256=plan.sha256,
        observation_sha256=observation.sha256, assessed_at=as_of.isoformat(),
        trigger_basis="OBSERVED_FINALIZED_CLOSE_OR_PACKAGE_MARK_NOT_INTRABAR_ORDER",
        fill=None, execution_permission=False)


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return retained_time(value).isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"unsupported frozen value: {type(value).__name__}")


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False, default=_json_value)


@dataclass(frozen=True, slots=True)
class FrozenOptionAlertPlan:
    payload_json: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.payload_json.encode("ascii")).hexdigest()

    @property
    def plan_id(self) -> UUID:
        return uuid5(NAMESPACE_URL, f"option-alert-plan:{self.sha256}")

    def to_dict(self) -> dict[str, object]:
        return {"plan_id": str(self.plan_id), "plan_sha256": self.sha256, "plan": json.loads(self.payload_json)}


def _plan_management(
    candidate: Mapping[str, object], decision_at: datetime, exit_deadline: datetime | None,
    management_policy: AlertManagementPolicy | None,
):
    original_management = candidate.get("management_policy") or {}
    if management_policy is not None:
        if management_policy.strategy_name != candidate["strategy_name"] or Decimal(str(candidate["net_premium"])) >= 0:
            raise ValueError("management policy must match the candidate's debit strategy")
        if any(key in original_management for key in ("stop_loss_fraction", "stop_loss_multiple", "take_profit_fraction", "exit_dte")):
            raise ValueError("explicit policy cannot overwrite original management limits")
        if exit_deadline is not None and exit_deadline > decision_at + timedelta(seconds=management_policy.maximum_hold_seconds):
            raise ValueError("exit deadline exceeds the management hold cap")
        management = management_policy.to_dict()
        management_version = management_policy.policy_version
    else:
        management = original_management
        management_version = candidate.get("management_policy_version")
    if not management or not management_version:
        raise ValueError("a frozen original management policy is required")
    try:
        target = Decimal(str(management["take_profit_fraction"]))
        stop_key = "stop_loss_multiple" if "stop_loss_multiple" in management else "stop_loss_fraction"
        stop = Decimal(str(management[stop_key]))
        valid_stop = stop > 1 if stop_key == "stop_loss_multiple" else 0 < stop < 1
        if not target.is_finite() or target <= 0 or not stop.is_finite() or not valid_stop:
            raise ValueError("invalid management limits")
        if Decimal(str(candidate["net_premium"])) > 0 and target > 1:
            raise ValueError("credit take-profit fraction exceeds original credit")
    except (KeyError, ArithmeticError) as failure:
        raise ValueError("complete stop and take-profit management rules are required") from failure
    return management, management_version


def freeze_indicative_alert_plan(
    detail: Mapping[str, object],
    *,
    decision_at: datetime,
    entry_deadline: datetime,
    exit_deadline: datetime,
    entry_limit: Decimal,
    management_policy: AlertManagementPolicy | None = None,
) -> FrozenOptionAlertPlan:
    decision_at = retained_time(decision_at)
    entry_deadline = retained_time(entry_deadline)
    exit_deadline = retained_time(exit_deadline)
    if type(entry_limit) is not Decimal or not entry_limit.is_finite() or entry_limit <= 0:
        raise ValueError("entry limit must be a positive finite package-dollar Decimal")
    candidate = detail["candidate"]
    if candidate.get("candidate_kind") == "RESEARCH_ONLY":
        raise ValueError("observation-only findings cannot become package plans")
    if not decision_at < entry_deadline < exit_deadline:
        raise ValueError("plan deadlines must follow the actual decision in order")
    if not candidate.get("valid_until") or entry_deadline > retained_time(candidate["valid_until"]):
        raise ValueError("entry deadline exceeds the original candidate validity window")
    snapshots = detail.get("source_snapshots", ())
    if not snapshots or exit_deadline >= min(retained_time(row["expiration_cutoff"]) for row in snapshots):
        raise ValueError("initial plans must exit before every contract expiration cutoff")
    legs = tuple(retained_leg(row) for row in detail.get("legs", ()))
    structure = StructureType(candidate["structure_type"])
    validate_alert_package(structure, legs)
    evidence = retained_alert_evidence(detail, decision_at, holding_until=exit_deadline)
    qualification = qualify_option_alert(candidate["strategy_name"], structure, evidence, decision_at)
    indicative = next(row for row in qualification["assessments"] if row["capability"] == AlertCapability.INDICATIVE.value)
    if indicative["status"] != "SATISFIED":
        raise ValueError(f"indicative requirements are not satisfied: {', '.join(indicative['blocking_inputs'])}")
    original_management = candidate.get("management_policy") or {}
    management, management_version = _plan_management(candidate, decision_at, exit_deadline, management_policy)
    for field in ("policy_sha256", "identity_sha256"):
        digest = candidate.get(field)
        if not isinstance(digest, str) or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError(f"invalid original {field}")
    net_premium = Decimal(str(candidate["net_premium"]))
    is_credit = net_premium > 0
    if is_credit and entry_limit < net_premium or not is_credit and entry_limit > -net_premium:
        raise ValueError("entry limit must not worsen the frozen indicative entry basis")
    if "exit_dte" in management:
        exit_dte = management["exit_dte"]
        if type(exit_dte) is not int or exit_dte < 0:
            raise ValueError("exit-DTE policy must be a nonnegative integer")
        remaining_dte = (legs[0].expiration_date - exit_deadline.astimezone(ZoneInfo("America/New_York")).date()).days
        if remaining_dte < exit_dte:
            raise ValueError("exit deadline violates the original exit-DTE policy")
    management_limits = None
    if management_policy is not None:
        maximum_profit = candidate.get("maximum_profit")
        target_profit = entry_limit * management_policy.take_profit_fraction
        if maximum_profit is not None:
            profit_at_limit = Decimal(str(maximum_profit)) + (-net_premium - entry_limit)
            if target_profit > profit_at_limit:
                raise ValueError("profit target exceeds the package's bounded maximum profit")
        management_limits = {
            "stop_package_value": str(entry_limit * (1 - management_policy.stop_loss_fraction)),
            "take_profit_package_value": str(entry_limit + target_profit),
            "units": "USD_PER_PACKAGE", "basis": "PLANNED_ENTRY_LIMIT_NOT_FILL",
            "stop_fill_guaranteed": False,
        }
    leg_payload = [{
        "index": leg.leg_index, "snapshot_id": str(leg.snapshot_id), "contract_id": leg.contract_id,
        "contract_ticker": leg.contract_ticker, "side": leg.side.value, "ratio": leg.ratio,
        "multiplier": leg.multiplier, "expiration_date": leg.expiration_date.isoformat(),
        "strike": str(leg.strike), "contract_type": leg.contract_type.value,
        "entry_model_mark": str(leg.model_mark), "source_market_time": leg.source_market_time.isoformat(),
        "valuation_policy_version": leg.valuation_policy_version,
        "valuation_policy_sha256": leg.valuation_policy_sha256, "model_version": leg.model_version,
    } for leg in legs]
    family = {
        "underlying": candidate["underlying"], "strategy": candidate["strategy_name"],
        "strategy_version": candidate["strategy_version"], "strategy_policy_sha256": candidate["policy_sha256"],
        "structure": structure.value,
        "legs": [{key: leg[key] for key in ("index", "contract_id", "side", "ratio", "multiplier", "expiration_date", "strike", "contract_type")} for leg in leg_payload],
        "entry_deadline": entry_deadline, "exit_deadline": exit_deadline,
        "management_policy": management,
    }
    return FrozenOptionAlertPlan(_canonical({
        "version": PLAN_VERSION, "state": "UNPUBLISHED_INDICATIVE_PLAN",
        "evidence_adapter_version": RETAINED_EVIDENCE_VERSION,
        "package_family_sha256": hashlib.sha256(_canonical(family).encode("ascii")).hexdigest(),
        "candidate_id": str(candidate["candidate_id"]), "candidate_identity_sha256": candidate["identity_sha256"],
        "matrix_id": str(candidate["matrix_id"]), "decision_evidence_id": str(candidate["decision_evidence_id"]),
        "underlying": candidate["underlying"], "strategy": candidate["strategy_name"],
        "strategy_version": candidate["strategy_version"], "strategy_policy_sha256": candidate["policy_sha256"],
        "structure": structure.value, "legs": leg_payload,
        "source_market_time": retained_time(candidate["market_data_time"]),
        "source_observed_at": retained_time(candidate["observed_time"]), "decision_at": decision_at,
        "original_valid_until": retained_time(candidate["valid_until"]),
        "entry_deadline": entry_deadline, "exit_deadline": exit_deadline,
        "entry_limit": str(entry_limit), "entry_limit_kind": "MINIMUM_CREDIT" if is_credit else "MAXIMUM_DEBIT",
        "entry_limit_units": "USD_PER_PACKAGE", "original_economics": {
            key: candidate.get(key) for key in ("net_premium", "maximum_profit", "maximum_loss", "capital_at_risk", "collateral_required", "breakevens")
        },
        "management_policy_version": management_version,
        "management_policy": management, "qualification": qualification,
        **({"management_source": "EXPLICIT_ALERT_POLICY", "management_policy_sha256": management_policy.sha256,
            "management_limits": management_limits, "original_management_policy": original_management,
            "original_management_policy_version": candidate.get("management_policy_version")} if management_policy else {}),
        "source_evidence_sha256": hashlib.sha256(_canonical(detail).encode("ascii")).hexdigest(),
        "source_evidence": detail, "published_at": None, "fill": None,
        "execution_permission": False, "paper_position_created": False,
        "limitations": ["INDICATIVE_MARKS_NOT_EXECUTABLE_QUOTES", "NO_FILL_OR_PUBLICATION", "EVENT_HORIZON_AND_ASSIGNMENT_NOT_QUALIFIED"],
    }))


def preview_indicative_alert_plan(
    detail: Mapping[str, object],
    *,
    decision_at: datetime,
    entry_deadline: datetime | None = None,
    exit_deadline: datetime | None = None,
    entry_limit: Decimal | None = None,
    management_policy: AlertManagementPolicy | None = None,
) -> dict[str, object]:
    decision_at = retained_time(decision_at)
    candidate = detail["candidate"]
    blockers: list[dict[str, object]] = []
    for name, value in (("entry_deadline", entry_deadline), ("exit_deadline", exit_deadline), ("entry_limit", entry_limit)):
        if value is None:
            blockers.append({"code": "PLAN_TERM_REQUIRED", "field": name})
    holding_until = None
    if exit_deadline is not None:
        exit_deadline = retained_time(exit_deadline)
        if decision_at < exit_deadline <= decision_at + timedelta(days=366):
            holding_until = exit_deadline
        else:
            blockers.append({"code": "EXIT_HORIZON_INVALID", "field": "exit_deadline"})
    original_deadline = candidate.get("valid_until")
    if original_deadline is None:
        blockers.append({"code": "ORIGINAL_ENTRY_WINDOW_UNAVAILABLE", "field": "entry_deadline"})
    elif retained_time(original_deadline) <= decision_at:
        blockers.append({"code": "ORIGINAL_ENTRY_WINDOW_ELAPSED", "field": "entry_deadline"})
    if retained_time(candidate["observed_time"]) > decision_at:
        blockers.append({"code": "CANDIDATE_NOT_YET_AVAILABLE"})
        holding_until = None
    evidence = retained_alert_evidence(detail, decision_at, **({"holding_until": holding_until} if holding_until else {}))
    qualification = qualify_option_alert(
        candidate["strategy_name"], StructureType(candidate["structure_type"]), evidence, decision_at,
    )
    indicative = next(row for row in qualification["assessments"] if row["capability"] == AlertCapability.INDICATIVE.value)
    observation_only = candidate.get("candidate_kind") == "RESEARCH_ONLY" or indicative["status"] == "NOT_APPLICABLE"
    if observation_only:
        blockers.append({"code": "OBSERVATION_ONLY_NOT_A_PACKAGE"})
    elif indicative["status"] != "SATISFIED":
        blockers.append({"code": "INDICATIVE_REQUIREMENTS_NOT_SATISFIED", "inputs": indicative["blocking_inputs"]})
    if not observation_only:
        try:
            _plan_management(candidate, decision_at, exit_deadline, management_policy)
        except (ValueError, KeyError, TypeError, ArithmeticError) as error:
            blockers.append({"code": "MANAGEMENT_POLICY_REJECTED", "message": str(error)})
    plan_preview = None
    if not blockers:
        try:
            frozen = freeze_indicative_alert_plan(
                detail, decision_at=decision_at, entry_deadline=entry_deadline,
                exit_deadline=exit_deadline, entry_limit=entry_limit, management_policy=management_policy,
            )
            plan_preview = frozen.to_dict()
            plan_preview["plan"].pop("source_evidence")
            plan_preview["source_evidence_omitted"] = True
        except (ValueError, KeyError, TypeError, ArithmeticError) as error:
            blockers.append({"code": "FROZEN_PLAN_REJECTED", "message": str(error)})
    return json.loads(_canonical({
        "version": "option_alert_preview_v1", "assessment_only": True,
        "status": "NOT_APPLICABLE" if observation_only else "BLOCKED" if blockers else "INDICATIVE_PLAN_VALID",
        "assessed_at": decision_at, "candidate_id": str(candidate["candidate_id"]),
        "underlying": candidate["underlying"], "strategy": candidate["strategy_name"],
        "strategy_version": candidate["strategy_version"], "strategy_policy_sha256": candidate["policy_sha256"],
        "matrix_id": str(candidate["matrix_id"]), "structure": candidate["structure_type"],
        "source_market_time": candidate["market_data_time"], "source_observed_at": candidate["observed_time"],
        "original_valid_until": original_deadline,
        "original_package": {"legs": detail.get("legs", ()), "economics": {
            key: candidate.get(key) for key in ("net_premium", "maximum_profit", "maximum_loss", "capital_at_risk", "collateral_required", "breakevens")
        }},
        "requested_terms": {"entry_deadline": entry_deadline, "exit_deadline": exit_deadline,
                            "entry_limit": entry_limit, "entry_limit_units": "USD_PER_PACKAGE"},
        "original_management_policy": candidate.get("management_policy"),
        "original_management_policy_version": candidate.get("management_policy_version"),
        "proposed_management_policy": management_policy.to_dict() if management_policy else None,
        "proposed_management_policy_sha256": management_policy.sha256 if management_policy else None,
        "qualification": qualification, "blockers": blockers, "plan_preview": plan_preview,
        "evidence_adapter_version": RETAINED_EVIDENCE_VERSION,
        "persisted": False, "publication_permission": False, "publication_state_checked": False,
        "execution_permission": False, "paper_position_created": False, "fill": None,
        "limitations": ["READ_ONLY_PREVIEW_NOT_PUBLICATION", "INDICATIVE_MARKS_NOT_EXECUTABLE_QUOTES",
                        "PUBLICATION_DUPLICATES_AND_CURRENT_GATES_NOT_CHECKED", "MANAGEMENT_RULES_NOT_CALIBRATED"],
    }))