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