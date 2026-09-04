from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar
from options.strategies.domain import OptionSide


MEASUREMENT_TYPES = frozenset(("15MIN", "30MIN", "60MIN", "CLOSE", "NEXT_OPEN"))


@dataclass(frozen=True, slots=True)
class OptionOutcomePolicy:
    policy_version: str
    commission_per_contract_per_side: Decimal
    policy_sha256: str

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("policy_version cannot be blank")
        if self.commission_per_contract_per_side < 0:
            raise ValueError("commission cannot be negative")
        if len(self.policy_sha256) != 64:
            raise ValueError("policy_sha256 must be a SHA-256 digest")


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

    def __post_init__(self) -> None:
        if self.contract_id <= 0 or self.ratio <= 0 or self.multiplier <= 0:
            raise ValueError("leg identifiers, ratio, and multiplier must be positive")
        if self.entry_mark <= 0 or self.exit_mark <= 0:
            raise ValueError("entry and exit marks must be positive")
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


def delayed_proxy_commission_policy(
    commission_per_contract_per_side: Decimal = Decimal("0.65"),
) -> OptionOutcomePolicy:
    payload = {
        "commission_per_contract_per_side": str(
            commission_per_contract_per_side
        ),
        "mark_model": "DEVELOPER_ALIGNED_AGG_CLOSE_PACKAGE",
        "policy_version": "option_delayed_proxy_commission_v1",
        "slippage_model": "UNAVAILABLE",
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("ascii")).hexdigest()
    return OptionOutcomePolicy(
        policy_version=payload["policy_version"],
        commission_per_contract_per_side=commission_per_contract_per_side,
        policy_sha256=digest,
    )


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
    policy: OptionOutcomePolicy,
) -> OptionDecayOutcome:
    if measurement_type not in MEASUREMENT_TYPES:
        raise ValueError("measurement_type is invalid")
    market_utc = _utc(market_time, "market_time")
    observed_utc = _utc(observed_time, "observed_time")
    if observed_utc < market_utc:
        raise ValueError("observed_time cannot precede market_time")
    if capital_at_risk <= 0:
        raise ValueError("capital_at_risk must be positive")
    if not legs:
        raise ValueError("option outcome requires at least one leg")
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
            "DEVELOPER_ALIGNED_AGG_CLOSE",
            "QUOTE_LIQUIDITY_NOT_AVAILABLE",
        ),
        valuation_policy_version=policy.policy_version,
        valuation_policy_sha256=policy.policy_sha256,
        source_snapshot_ids=source_snapshot_ids,
        source_batch_id=next(iter(batch_ids)),
    )


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