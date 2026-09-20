from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar
from options.config import ValuationPolicy, load_valuation_policy
from options.domain import MarkSource
from options.strategies.domain import OptionSide


MEASUREMENT_TYPES = frozenset(("15MIN", "30MIN", "60MIN", "CLOSE", "NEXT_OPEN"))
CURRENT_MARK_MEASUREMENT = "CURRENT"


@dataclass(frozen=True, slots=True)
class OptionOutcomeLeg:
    contract_id: int
    side: OptionSide
    ratio: int
    multiplier: int
    entry_mark: Decimal
    exit_mark: Decimal
    source_snapshot_id: UUID
    source_batch_id: UUID
    source_market_time: datetime
    source_observed_time: datetime
    entry_mark_source: MarkSource
    exit_mark_source: MarkSource
    entry_valuation_policy_sha256: str
    exit_valuation_policy_sha256: str

    def __post_init__(self) -> None:
        if self.contract_id <= 0 or self.ratio <= 0 or self.multiplier <= 0:
            raise ValueError("leg identifiers, ratio, and multiplier must be positive")
        if self.entry_mark <= 0 or self.exit_mark <= 0:
            raise ValueError("entry and exit marks must be positive")
        for value in (
            self.entry_valuation_policy_sha256,
            self.exit_valuation_policy_sha256,
        ):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValueError("valuation policy hashes must be SHA-256 digests")
        object.__setattr__(
            self, "source_market_time",
            _utc(self.source_market_time, "source_market_time"),
        )
        object.__setattr__(
            self, "source_observed_time",
            _utc(self.source_observed_time, "source_observed_time"),
        )


@dataclass(frozen=True, slots=True)
class OptionDecayOutcome:
    outcome_id: UUID
    event_id: UUID | None
    candidate_id: UUID
    measurement_type: str
    market_time: datetime
    observed_time: datetime
    entry_net_premium: Decimal
    exit_net_premium: Decimal
    gross_pnl: Decimal
    estimated_cost: Decimal
    net_pnl: Decimal
    capital_at_risk: Decimal
    net_return: Decimal
    availability_flag: str
    quality_flags: tuple[str, ...]
    valuation_policy_version: str
    valuation_policy_sha256: str
    source_snapshot_ids: tuple[UUID, ...]
    source_batch_id: UUID


def configured_valuation_policy() -> ValuationPolicy:
    backend_dir = Path(__file__).resolve().parent.parent
    path = Path(
        os.getenv("OPTION_VALUATION_POLICY_FILE", "options/policies/valuation_v1.json")
    )
    if not path.is_absolute():
        path = backend_dir / path
    return load_valuation_policy(path.resolve())


def delayed_proxy_commission_policy() -> ValuationPolicy:
    return configured_valuation_policy()


def measurement_checkpoints(
    market_time: datetime,
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> dict[str, datetime]:
    market_utc = _utc(market_time, "market_time")
    exchange = calendar or OptionExchangeCalendar()
    session = exchange.session_for_market_time(market_utc)
    session_close = exchange.session_close(session)
    next_open = exchange.next_session_open(session)
    result = {
        "15MIN": market_utc + timedelta(minutes=15),
        "30MIN": market_utc + timedelta(minutes=30),
        "60MIN": market_utc + timedelta(minutes=60),
        "NEXT_OPEN": next_open,
    }
    if session_close > market_utc:
        result["CLOSE"] = session_close
    return result


def evaluate_delayed_proxy_outcome(
    *,
    candidate_id: UUID,
    event_id: UUID | None,
    measurement_type: str,
    market_time: datetime,
    observed_time: datetime,
    capital_at_risk: Decimal,
    legs: tuple[OptionOutcomeLeg, ...],
    policy: ValuationPolicy,
) -> OptionDecayOutcome:
    if measurement_type not in MEASUREMENT_TYPES | {CURRENT_MARK_MEASUREMENT}:
        raise ValueError("measurement_type is invalid")
    market_utc = _utc(market_time, "market_time")
    observed_utc = _utc(observed_time, "observed_time")
    if observed_utc < market_utc:
        raise ValueError("observed_time cannot precede market_time")
    if capital_at_risk <= 0:
        raise ValueError("capital_at_risk must be positive")
    if not legs:
        raise ValueError("option outcome requires at least one leg")
    for leg in legs:
        if (
            leg.entry_valuation_policy_sha256 != policy.policy_sha256
            or leg.exit_valuation_policy_sha256 != policy.policy_sha256
        ):
            raise ValueError("outcome marks must match the valuation policy")
        if leg.entry_mark_source not in policy.allowed_entry_mark_sources:
            raise ValueError("entry mark source is not allowed by the valuation policy")
        if leg.exit_mark_source not in policy.allowed_exit_mark_sources:
            raise ValueError("exit mark source is not allowed by the valuation policy")
    batch_ids = {leg.source_batch_id for leg in legs}
    if len(batch_ids) != 1:
        raise ValueError("option outcome legs must use one coherent source batch")

    entry_net_premium = _package_premium(legs, entry=True)
    exit_net_premium = _package_premium(legs, entry=False)
    gross_pnl = entry_net_premium - exit_net_premium
    contract_sides = sum(leg.ratio for leg in legs)
    estimated_cost = (
        policy.commission_per_contract_per_side
        * Decimal(2)
        * Decimal(contract_sides)
    )
    net_pnl = gross_pnl - estimated_cost
    net_return = net_pnl / capital_at_risk
    source_snapshot_ids = tuple(leg.source_snapshot_id for leg in legs)
    identity = hashlib.sha256(_canonical_json({
        "candidate_id": str(candidate_id),
        "measurement_type": measurement_type,
        "policy_sha256": policy.policy_sha256,
        "source_snapshot_ids": [str(value) for value in source_snapshot_ids],
    }).encode("ascii")).hexdigest()
    return OptionDecayOutcome(
        outcome_id=uuid5(NAMESPACE_URL, f"option-decay-outcome:{identity}"),
        event_id=event_id,
        candidate_id=candidate_id,
        measurement_type=measurement_type,
        market_time=market_utc,
        observed_time=observed_utc,
        entry_net_premium=entry_net_premium,
        exit_net_premium=exit_net_premium,
        gross_pnl=gross_pnl,
        estimated_cost=estimated_cost,
        net_pnl=net_pnl,
        capital_at_risk=capital_at_risk,
        net_return=net_return,
        availability_flag="RESEARCH_DELAYED_PROXY",
        quality_flags=(
            "COMMISSION_ONLY_COST_MODEL",
            policy.primary_model_mark_source.value,
            "QUOTE_LIQUIDITY_NOT_AVAILABLE",
        ),
        valuation_policy_version=policy.policy_version,
        valuation_policy_sha256=policy.policy_sha256,
        source_snapshot_ids=source_snapshot_ids,
        source_batch_id=next(iter(batch_ids)),
    )


def review_retained_plan_mark(plan: dict, retained: dict | None, *, checked_at: datetime, policy: ValuationPolicy) -> dict:
    from options.alert_qualification import retained_time

    missing = dict(status="UNAVAILABLE", reason="CURRENT_MARK_NOT_RECORDED", market_time=None,
        observed_time=None, age_seconds=None, signed_package_mark=None, package_price=None,
        gross_pnl=None, price_return=None, estimated_cost=None, net_pnl=None, net_return=None,
        price_return_basis="ABS_ORIGINAL_NET_PREMIUM", net_return_basis="ORIGINAL_CAPITAL_AT_RISK",
        basis="ORIGINAL_CANDIDATE_MARKS_NOT_PLANNED_LIMIT_OR_FILL", slippage="UNAVAILABLE",
        execution_permission=False)
    if retained is None:
        return missing
    try:
        if (str(retained["candidate_id"]) != plan["candidate_id"]
            or retained["candidate_identity"] != plan["candidate_identity_sha256"]
            or str(retained["matrix_id"]) != plan["matrix_id"]
            or retained["valuation_policy_sha256"] != policy.policy_sha256):
            raise ValueError("identity")
        originals = plan["legs"]
        marks = retained["legs"]
        if not originals or len(originals) != len(marks) or len({leg["contract_id"] for leg in marks}) != len(marks):
            raise ValueError("incomplete legs")
        outcome_legs = []
        for original, mark in zip(originals, marks):
            if any(str(original[key]) != str(mark[key]) for key in ("contract_id", "contract_ticker", "side", "ratio", "multiplier", "expiration_date", "contract_type")):
                raise ValueError("leg identity")
            if Decimal(str(original["strike"])) != Decimal(str(mark["strike"])) or Decimal(str(original["entry_model_mark"])) != Decimal(str(mark["entry_mark"])):
                raise ValueError("leg basis")
            if mark["entry_valuation_policy_sha256"] != original["valuation_policy_sha256"] or mark["snapshot_multiplier"] != original["multiplier"]:
                raise ValueError("leg valuation identity")
            market_time = retained_time(mark["source_market_time"])
            receipt = retained_time(mark["source_observed_time"])
            if not retained_time(plan["source_market_time"]) <= market_time <= receipt <= checked_at:
                raise ValueError("causal clocks")
            if retained_time(mark["revised_observed_at"] or receipt) > retained_time(retained["observed_time"]):
                raise ValueError("later revision")
            if (market_time - retained_time(original["source_market_time"])).total_seconds() < 0:
                raise ValueError("pre-entry mark")
            for value in (original["entry_model_mark"], mark["exit_mark"], mark["entry_mark"]):
                if not Decimal(str(value)).is_finite():
                    raise ValueError("nonfinite mark")
            outcome_legs.append(OptionOutcomeLeg(contract_id=original["contract_id"], side=OptionSide(original["side"]),
                ratio=original["ratio"], multiplier=original["multiplier"], entry_mark=Decimal(str(original["entry_model_mark"])),
                exit_mark=Decimal(str(mark["exit_mark"])), source_snapshot_id=UUID(str(mark["snapshot_id"])),
                source_batch_id=UUID(str(mark["batch_id"])), source_market_time=market_time, source_observed_time=receipt,
                entry_mark_source=MarkSource(mark["entry_mark_source"]), exit_mark_source=MarkSource(mark["exit_mark_source"]),
                entry_valuation_policy_sha256=original["valuation_policy_sha256"], exit_valuation_policy_sha256=mark["valuation_policy_sha256"]))
        market_time = retained_time(retained["market_time"])
        receipt = retained_time(retained["observed_time"])
        if market_time != max(leg.source_market_time for leg in outcome_legs) or receipt != max(leg.source_observed_time for leg in outcome_legs):
            raise ValueError("aggregate clocks")
        if not receipt <= retained_time(retained["updated_at"]) <= checked_at:
            raise ValueError("recording clock")
        if {str(leg.source_snapshot_id) for leg in outcome_legs} != {str(value) for value in retained["source_snapshot_ids"]} or len(retained["source_snapshot_ids"]) != len(outcome_legs):
            raise ValueError("snapshot binding")
        capital = Decimal(str(plan["original_economics"]["capital_at_risk"]))
        if not capital.is_finite():
            raise ValueError("capital")
        outcome = evaluate_delayed_proxy_outcome(candidate_id=UUID(plan["candidate_id"]), event_id=None,
            measurement_type="CURRENT", market_time=market_time, observed_time=receipt,
            capital_at_risk=capital, legs=tuple(outcome_legs), policy=policy)
        if outcome.source_batch_id != UUID(str(retained["source_batch_id"])):
            raise ValueError("batch identity")
        if outcome.entry_net_premium != Decimal(str(plan["original_economics"]["net_premium"])):
            raise ValueError("entry premium")
        for field in ("entry_net_premium", "exit_net_premium", "gross_pnl", "estimated_cost", "net_pnl", "capital_at_risk", "net_return"):
            stored = Decimal(str(retained[field]))
            if not stored.is_finite() or abs(stored - getattr(outcome, field)) > Decimal("0.00000002"):
                raise ValueError("stored arithmetic")
        oldest_mark = min(leg.source_market_time for leg in outcome_legs)
        age = (checked_at - oldest_mark).total_seconds()
        multipliers = {leg.multiplier for leg in outcome_legs}
        return {**missing, "status": "FRESH" if age <= policy.maximum_source_age_seconds else "STALE", "reason": None,
            "market_time": market_time, "observed_time": receipt, "oldest_leg_market_time": oldest_mark,
            "age_seconds": age, "maximum_age_seconds": policy.maximum_source_age_seconds,
            "signed_package_mark": outcome.exit_net_premium,
            "package_price": outcome.exit_net_premium / next(iter(multipliers)) if len(multipliers) == 1 else None,
            "gross_pnl": outcome.gross_pnl, "price_return": outcome.gross_pnl / abs(outcome.entry_net_premium) if outcome.entry_net_premium else None,
            "estimated_cost": outcome.estimated_cost, "net_pnl": outcome.net_pnl, "net_return": outcome.net_return,
            "valuation_policy_sha256": policy.policy_sha256, "source_snapshot_ids": retained["source_snapshot_ids"],
            "after_exit_deadline": market_time >= retained_time(plan["exit_deadline"]) if plan.get("exit_deadline") else None}
    except (ValueError, TypeError, KeyError, ArithmeticError):
        return {**missing, "reason": "CURRENT_MARK_BINDING_INVALID"}


def review_retained_candidate_mark(candidate: dict, retained: dict | None, *, checked_at: datetime, policy: ValuationPolicy) -> dict:
    basis = dict(candidate_id=str(candidate["candidate_id"]), candidate_identity_sha256=candidate["candidate_identity"],
        matrix_id=str(candidate["matrix_id"]), source_market_time=candidate["market_data_time"],
        original_economics={key: candidate.get(key) for key in ("net_premium", "capital_at_risk")},
        legs=[{**leg, "entry_model_mark": leg.get("model_mark")} for leg in candidate.get("legs", ())])
    return review_retained_plan_mark(basis, retained, checked_at=checked_at, policy=policy)


def _package_premium(
    legs: tuple[OptionOutcomeLeg, ...],
    *,
    entry: bool,
) -> Decimal:
    return sum(
        (
            Decimal(1) if leg.side is OptionSide.SELL else Decimal(-1)
        )
        * (leg.entry_mark if entry else leg.exit_mark)
        * leg.ratio
        * leg.multiplier
        for leg in legs
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)