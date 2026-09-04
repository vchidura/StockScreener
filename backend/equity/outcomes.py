"""Versioned causal outcome evaluation for persisted equity evidence."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd

from .domain import EquityBarRevision, EquityEvidence
from .polygon import canonical_json, sha256_json


@dataclass(frozen=True, slots=True)
class OutcomePolicy:
    outcome_policy_id: UUID
    policy_key: str
    policy_version: str
    evidence_type: str
    source_name: str | None
    source_version: str | None
    interval: str | None
    direction_contract: str
    eligibility_transition: str
    entry_model: str
    horizons_json: str
    cost_model_json: str
    benchmark_policy_json: str
    ambiguity_policy: str
    success_definition_json: str
    missingness_policy_json: str
    independence_policy_json: str
    effective_from: datetime
    effective_to: datetime | None
    policy_sha256: str

    def __post_init__(self) -> None:
        if self.ambiguity_policy not in (
            "CONSERVATIVE_STOP_FIRST",
            "TARGET_FIRST_SENSITIVITY",
            "EXCLUDE_AMBIGUOUS_SENSITIVITY",
        ):
            raise ValueError("invalid ambiguity policy")
        horizons = json.loads(self.horizons_json)
        if not isinstance(horizons, dict) or not horizons:
            raise ValueError("horizons_json must be a non-empty JSON object")
        if any(
            not isinstance(key, str) or not isinstance(value, int) or value <= 0
            for key, value in horizons.items()
        ):
            raise ValueError("horizons_json must map names to positive bar counts")
        for value, name, expected in (
            (self.cost_model_json, "cost_model_json", dict),
            (self.benchmark_policy_json, "benchmark_policy_json", dict),
            (self.success_definition_json, "success_definition_json", dict),
            (self.missingness_policy_json, "missingness_policy_json", dict),
            (self.independence_policy_json, "independence_policy_json", dict),
        ):
            parsed = json.loads(value)
            if not isinstance(parsed, expected):
                raise ValueError(f"{name} has an invalid JSON shape")
        effective_from = _utc(self.effective_from, "effective_from")
        effective_to = _utc(self.effective_to, "effective_to") if self.effective_to else None
        if effective_to is not None and effective_to <= effective_from:
            raise ValueError("effective_to must follow effective_from")
        if len(self.policy_sha256) != 64:
            raise ValueError("policy_sha256 must be a SHA-256 digest")
        object.__setattr__(self, "effective_from", effective_from)
        object.__setattr__(self, "effective_to", effective_to)


@dataclass(frozen=True, slots=True)
class ResearchOutcome:
    outcome_id: UUID
    subject_evidence_id: UUID
    outcome_policy_id: UUID
    horizon_key: str
    outcome_revision: int
    entry_status: str
    signal_time: datetime
    confirmation_bar_id: UUID | None
    confirmation_bar_end: datetime | None
    entry_bar_id: UUID | None
    entry_time: datetime | None
    entry_price: Decimal | None
    exit_bar_id: UUID | None
    exit_time: datetime | None
    exit_price: Decimal | None
    gross_return: float | None
    signed_return: float | None
    estimated_cost: float | None
    net_return: float | None
    market_benchmark_ticker: str | None
    market_return: float | None
    sector_benchmark_ticker: str | None
    sector_return: float | None
    net_alpha: float | None
    sector_net_alpha: float | None
    mae_pct: float | None
    mfe_pct: float | None
    mae_r: float | None
    mfe_r: float | None
    stop_hit: bool | None
    target_hit: bool | None
    first_hit: str | None
    outcome_category: str
    is_stale: bool
    outcome_available_at: datetime
    quality_codes: tuple[str, ...]
    path_bar_ids: tuple[UUID, ...]
    benchmark_bar_ids: tuple[UUID, ...]
    supersedes_outcome_id: UUID | None = None


def default_directional_policy(
    *,
    source_name: str,
    source_version: str,
    interval: str,
    horizons: dict[str, int],
    effective_from: datetime,
    round_trip_cost_bps: float = 4.0,
    sector_benchmark: bool = True,
    primary_benchmark: str = "MARKET",
    policy_version: str = "directional_outcome_v1",
) -> OutcomePolicy:
    if primary_benchmark not in ("MARKET", "SECTOR"):
        raise ValueError("primary_benchmark must be MARKET or SECTOR")
    if primary_benchmark == "SECTOR" and not sector_benchmark:
        raise ValueError("SECTOR primary benchmark requires sector benchmarking")
    payload = {
        "ambiguity_policy": "CONSERVATIVE_STOP_FIRST",
        "benchmark": {
            "market": "SPY",
            "primary": primary_benchmark,
            "sector": sector_benchmark,
            "sector_mapping": "sic_gics_sector_etf_v1",
        },
        "cost": {"round_trip_bps": round_trip_cost_bps},
        "direction_contract": "SIGNED",
        "eligibility_transition": "MATCH",
        "entry_model": "NEXT_ACTIONABLE_BAR_OPEN_V1",
        "evidence_type": "SCANNER_RESULT",
        "horizons": horizons,
        "independence": {"spacing": "HORIZON_BARS"},
        "missingness": {"retain_unavailable": True},
        "source_name": source_name,
        "source_version": source_version,
        "success": {"category": "NET_RETURN_POSITIVE"},
    }
    digest = sha256_json(payload)
    policy_key = f"{source_name}:{source_version}:{interval}:SIGNED"
    if primary_benchmark == "SECTOR":
        policy_key += ":SECTOR_PRIMARY"
    identity = digest if policy_version == "directional_outcome_v1" else f"{policy_version}:{digest}"
    return OutcomePolicy(
        outcome_policy_id=uuid5(NAMESPACE_URL, f"equity-outcome-policy:{identity}"),
        policy_key=policy_key,
        policy_version=policy_version,
        evidence_type="SCANNER_RESULT",
        source_name=source_name,
        source_version=source_version,
        interval=interval,
        direction_contract="SIGNED",
        eligibility_transition="MATCH",
        entry_model="NEXT_ACTIONABLE_BAR_OPEN_V1",
        horizons_json=canonical_json(horizons),
        cost_model_json=canonical_json({"round_trip_bps": round_trip_cost_bps}),
        benchmark_policy_json=canonical_json({
            "market": "SPY", "primary": primary_benchmark,
            "sector": sector_benchmark,
            "sector_mapping": "sic_gics_sector_etf_v1",
        }),
        ambiguity_policy="CONSERVATIVE_STOP_FIRST",
        success_definition_json=canonical_json({"category": "NET_RETURN_POSITIVE"}),
        missingness_policy_json=canonical_json({"retain_unavailable": True}),
        independence_policy_json=canonical_json({"spacing": "HORIZON_BARS"}),
        effective_from=effective_from,
        effective_to=None,
        policy_sha256=digest,
    )


def recommendation_plan_policy(
    *,
    source_name: str,
    source_version: str,
    interval: str,
    horizons: dict[str, int],
    effective_from: datetime,
    round_trip_cost_bps: float = 4.0,
    sector_benchmark: bool = True,
    primary_benchmark: str = "MARKET",
    policy_version: str = "recommendation_plan_v1",
) -> OutcomePolicy:
    if primary_benchmark not in ("MARKET", "SECTOR"):
        raise ValueError("primary_benchmark must be MARKET or SECTOR")
    if primary_benchmark == "SECTOR" and not sector_benchmark:
        raise ValueError("SECTOR primary benchmark requires sector benchmarking")
    success = {
        "category": "PLAN_NET_RETURN_POSITIVE",
        "exit_model": "FIRST_STOP_TARGET_OR_HORIZON_CLOSE",
        "required_payload_fields": ["stop_price", "target_price"],
    }
    payload = {
        "ambiguity_policy": "CONSERVATIVE_STOP_FIRST",
        "benchmark": {
            "market": "SPY",
            "primary": primary_benchmark,
            "sector": sector_benchmark,
            "sector_mapping": "sic_gics_sector_etf_v1",
        },
        "cost": {"round_trip_bps": round_trip_cost_bps},
        "direction_contract": "SIGNED",
        "eligibility_transition": "MATCH_WITH_BRACKET",
        "entry_model": "NEXT_ACTIONABLE_BAR_OPEN_V1",
        "evidence_type": "SCANNER_RESULT",
        "horizons": horizons,
        "independence": {"spacing": "HORIZON_BARS"},
        "missingness": {"retain_unavailable": True},
        "source_name": source_name,
        "source_version": source_version,
        "success": success,
    }
    digest = sha256_json(payload)
    policy_key = f"{source_name}:{source_version}:{interval}:RECOMMENDATION_PLAN"
    if primary_benchmark == "SECTOR":
        policy_key += ":SECTOR_PRIMARY"
    return OutcomePolicy(
        outcome_policy_id=uuid5(
            NAMESPACE_URL,
            f"equity-outcome-policy:{policy_version}:{digest}",
        ),
        policy_key=policy_key,
        policy_version=policy_version,
        evidence_type="SCANNER_RESULT",
        source_name=source_name,
        source_version=source_version,
        interval=interval,
        direction_contract="SIGNED",
        eligibility_transition="MATCH_WITH_BRACKET",
        entry_model="NEXT_ACTIONABLE_BAR_OPEN_V1",
        horizons_json=canonical_json(horizons),
        cost_model_json=canonical_json({"round_trip_bps": round_trip_cost_bps}),
        benchmark_policy_json=canonical_json({
            "market": "SPY", "primary": primary_benchmark,
            "sector": sector_benchmark,
            "sector_mapping": "sic_gics_sector_etf_v1",
        }),
        ambiguity_policy="CONSERVATIVE_STOP_FIRST",
        success_definition_json=canonical_json(success),
        missingness_policy_json=canonical_json({"retain_unavailable": True}),
        independence_policy_json=canonical_json({"spacing": "HORIZON_BARS"}),
        effective_from=effective_from,
        effective_to=None,
        policy_sha256=digest,
    )


def evaluate_directional_outcome(
    subject: EquityEvidence,
    policy: OutcomePolicy,
    horizon_key: str,
    bars: tuple[EquityBarRevision, ...],
    *,
    market_bars: tuple[EquityBarRevision, ...] = (),
    sector_bars: tuple[EquityBarRevision, ...] = (),
    market_benchmark_ticker: str | None = None,
    sector_benchmark_ticker: str | None = None,
    outcome_revision: int = 1,
) -> ResearchOutcome:
    if subject.direction not in (-1, 1):
        raise ValueError("directional outcome requires signed subject direction")
    horizons = json.loads(policy.horizons_json)
    if horizon_key not in horizons:
        raise ValueError(f"unknown horizon: {horizon_key}")
    horizon_bars = int(horizons[horizon_key])
    if horizon_bars <= 0:
        raise ValueError("horizon bars must be positive")
    path = tuple(
        row for row in sorted(bars, key=lambda item: item.bar_start)
        if row.is_final and row.bar_start > subject.observed_at
    )[:horizon_bars]
    payload = json.loads(subject.payload_json)
    success = json.loads(policy.success_definition_json)
    recommendation_plan = (
        success.get("exit_model") == "FIRST_STOP_TARGET_OR_HORIZON_CLOSE"
    )
    if not path:
        return _unavailable(
            subject, policy, horizon_key, outcome_revision, path,
            market_benchmark_ticker=market_benchmark_ticker,
            sector_benchmark_ticker=sector_benchmark_ticker,
        )
    entry = path[0]
    if policy.interval == "1d" and entry.session_date != _next_daily_entry_session(
        subject.observed_at
    ):
        return _unavailable(
            subject, policy, horizon_key, outcome_revision, path,
            market_benchmark_ticker=market_benchmark_ticker,
            sector_benchmark_ticker=sector_benchmark_ticker,
            quality_code="NEXT_DAILY_ENTRY_SESSION_MISSING",
        )
    entry_price = entry.open_price
    stop = _decimal(payload.get("stop_price"))
    target = _decimal(payload.get("target_price"))
    invalid_bracket = (
        stop is None
        or target is None
        or subject.direction * (entry_price - stop) <= 0
        or subject.direction * (target - entry_price) <= 0
    )
    if (recommendation_plan or payload.get("require_entry_between_stop_target")) \
            and invalid_bracket:
        return _not_triggered(
            subject, policy, horizon_key, outcome_revision, entry,
            quality_code=(
                "PLAN_BRACKET_INVALID"
                if recommendation_plan else "ENTRY_OUTSIDE_BRACKET"
            ),
            market_benchmark_ticker=market_benchmark_ticker,
            sector_benchmark_ticker=sector_benchmark_ticker,
        )
    plan_exit = (
        _recommendation_plan_exit(
            path, subject.direction, stop, target, policy.ambiguity_policy
        )
        if recommendation_plan else None
    )
    if plan_exit is None and len(path) < horizon_bars:
        return _unavailable(
            subject, policy, horizon_key, outcome_revision, path,
            market_benchmark_ticker=market_benchmark_ticker,
            sector_benchmark_ticker=sector_benchmark_ticker,
        )
    if plan_exit is None:
        evaluated_path = path
        exit_bar = path[-1]
        exit_price = exit_bar.close_price
    else:
        evaluated_path, exit_bar, exit_price = plan_exit
    stop_hit, target_hit, first_hit = _first_hit(
        evaluated_path, subject.direction, stop, target
    )
    gross_return = float(exit_price / entry_price - Decimal("1"))
    signed_return = subject.direction * gross_return
    cost = float(json.loads(policy.cost_model_json).get("round_trip_bps", 0)) / 10_000
    net_return = signed_return - cost
    market_return = _benchmark_return(market_bars, entry.bar_start, exit_bar.bar_end)
    sector_return = _benchmark_return(sector_bars, entry.bar_start, exit_bar.bar_end)
    net_alpha = (
        net_return - subject.direction * market_return
        if market_return is not None else None
    )
    sector_net_alpha = (
        net_return - subject.direction * sector_return
        if sector_return is not None else None
    )
    favorable = max(
        float(row.high_price / entry_price - Decimal("1"))
        if subject.direction == 1
        else float(Decimal("1") - row.low_price / entry_price)
        for row in evaluated_path
    )
    adverse = min(
        float(row.low_price / entry_price - Decimal("1"))
        if subject.direction == 1
        else float(Decimal("1") - row.high_price / entry_price)
        for row in evaluated_path
    )
    risk_fraction = (
        float(abs(entry_price - stop) / entry_price)
        if stop is not None and entry_price > 0 else None
    )
    mae_r = adverse / risk_fraction if risk_fraction else None
    mfe_r = favorable / risk_fraction if risk_fraction else None
    category = _category(net_return, first_hit, policy.ambiguity_policy)
    quality_codes = (
        ("SAME_BAR_PATH_AMBIGUOUS",) if first_hit == "SAME_BAR" else ()
    )
    identity = sha256_json({
        "horizon": horizon_key,
        "path": [str(row.bar_revision_id) for row in evaluated_path],
        "policy": str(policy.outcome_policy_id),
        "revision": outcome_revision,
        "subject": str(subject.evidence_id),
    })
    return ResearchOutcome(
        outcome_id=uuid5(NAMESPACE_URL, f"equity-outcome:{identity}"),
        subject_evidence_id=subject.evidence_id,
        outcome_policy_id=policy.outcome_policy_id,
        horizon_key=horizon_key,
        outcome_revision=outcome_revision,
        entry_status="ENTERED",
        signal_time=subject.observed_at,
        confirmation_bar_id=None,
        confirmation_bar_end=None,
        entry_bar_id=entry.bar_revision_id,
        entry_time=entry.bar_start,
        entry_price=entry_price,
        exit_bar_id=exit_bar.bar_revision_id,
        exit_time=exit_bar.bar_end,
        exit_price=exit_price,
        gross_return=gross_return,
        signed_return=signed_return,
        estimated_cost=cost,
        net_return=net_return,
        market_benchmark_ticker=market_benchmark_ticker,
        market_return=market_return,
        sector_benchmark_ticker=sector_benchmark_ticker,
        sector_return=sector_return,
        net_alpha=net_alpha,
        sector_net_alpha=sector_net_alpha,
        mae_pct=adverse,
        mfe_pct=favorable,
        mae_r=mae_r,
        mfe_r=mfe_r,
        stop_hit=stop_hit,
        target_hit=target_hit,
        first_hit=first_hit,
        outcome_category=category,
        is_stale=False,
        outcome_available_at=exit_bar.system_observed_at,
        quality_codes=quality_codes,
        path_bar_ids=tuple(row.bar_revision_id for row in evaluated_path),
        benchmark_bar_ids=tuple(dict.fromkeys(
            row.bar_revision_id for row in (*market_bars, *sector_bars)
        )),
    )


def _not_triggered(
    subject: EquityEvidence,
    policy: OutcomePolicy,
    horizon_key: str,
    revision: int,
    entry: EquityBarRevision,
    quality_code: str = "ENTRY_OUTSIDE_BRACKET",
    market_benchmark_ticker: str | None = None,
    sector_benchmark_ticker: str | None = None,
) -> ResearchOutcome:
    identity = sha256_json({
        "entry": str(entry.bar_revision_id),
        "horizon": horizon_key,
        "not_triggered": True,
        "policy": str(policy.outcome_policy_id),
        "revision": revision,
        "subject": str(subject.evidence_id),
    })
    return ResearchOutcome(
        outcome_id=uuid5(NAMESPACE_URL, f"equity-outcome:{identity}"),
        subject_evidence_id=subject.evidence_id,
        outcome_policy_id=policy.outcome_policy_id,
        horizon_key=horizon_key,
        outcome_revision=revision,
        entry_status="NOT_TRIGGERED",
        signal_time=subject.observed_at,
        confirmation_bar_id=None,
        confirmation_bar_end=None,
        entry_bar_id=entry.bar_revision_id,
        entry_time=entry.bar_start,
        entry_price=entry.open_price,
        exit_bar_id=None,
        exit_time=None,
        exit_price=None,
        gross_return=None,
        signed_return=None,
        estimated_cost=None,
        net_return=None,
        market_benchmark_ticker=market_benchmark_ticker,
        market_return=None,
        sector_benchmark_ticker=sector_benchmark_ticker,
        sector_return=None,
        net_alpha=None,
        sector_net_alpha=None,
        mae_pct=None,
        mfe_pct=None,
        mae_r=None,
        mfe_r=None,
        stop_hit=None,
        target_hit=None,
        first_hit=None,
        outcome_category="NOT_ENTERED",
        is_stale=False,
        outcome_available_at=entry.system_observed_at,
        quality_codes=(quality_code,),
        path_bar_ids=(entry.bar_revision_id,),
        benchmark_bar_ids=(),
    )


def _unavailable(
    subject: EquityEvidence,
    policy: OutcomePolicy,
    horizon_key: str,
    revision: int,
    path: tuple[EquityBarRevision, ...],
    market_benchmark_ticker: str | None = None,
    sector_benchmark_ticker: str | None = None,
    quality_code: str = "OUTCOME_HORIZON_INCOMPLETE",
) -> ResearchOutcome:
    identity = sha256_json({
        "horizon": horizon_key,
        "policy": str(policy.outcome_policy_id),
        "revision": revision,
        "subject": str(subject.evidence_id),
        "unavailable": True,
    })
    return ResearchOutcome(
        outcome_id=uuid5(NAMESPACE_URL, f"equity-outcome:{identity}"),
        subject_evidence_id=subject.evidence_id,
        outcome_policy_id=policy.outcome_policy_id,
        horizon_key=horizon_key,
        outcome_revision=revision,
        entry_status="UNAVAILABLE",
        signal_time=subject.observed_at,
        confirmation_bar_id=None,
        confirmation_bar_end=None,
        entry_bar_id=None,
        entry_time=None,
        entry_price=None,
        exit_bar_id=None,
        exit_time=None,
        exit_price=None,
        gross_return=None,
        signed_return=None,
        estimated_cost=None,
        net_return=None,
        market_benchmark_ticker=market_benchmark_ticker,
        market_return=None,
        sector_benchmark_ticker=sector_benchmark_ticker,
        sector_return=None,
        net_alpha=None,
        sector_net_alpha=None,
        mae_pct=None,
        mfe_pct=None,
        mae_r=None,
        mfe_r=None,
        stop_hit=None,
        target_hit=None,
        first_hit=None,
        outcome_category="UNAVAILABLE",
        is_stale=False,
        outcome_available_at=(
            path[-1].system_observed_at if path else subject.observed_at
        ),
        quality_codes=(quality_code,),
        path_bar_ids=tuple(row.bar_revision_id for row in path),
        benchmark_bar_ids=(),
    )


def _first_hit(
    bars: tuple[EquityBarRevision, ...],
    direction: int,
    stop: Decimal | None,
    target: Decimal | None,
) -> tuple[bool, bool, str]:
    if stop is None or target is None:
        return False, False, "NONE"
    stop_hit = target_hit = False
    first = "NONE"
    for bar in bars:
        bar_stop = bar.low_price <= stop if direction == 1 else bar.high_price >= stop
        bar_target = bar.high_price >= target if direction == 1 else bar.low_price <= target
        stop_hit = stop_hit or bar_stop
        target_hit = target_hit or bar_target
        if first == "NONE" and (bar_stop or bar_target):
            first = "SAME_BAR" if bar_stop and bar_target else "STOP" if bar_stop else "TARGET"
    return stop_hit, target_hit, first


def _recommendation_plan_exit(
    bars: tuple[EquityBarRevision, ...],
    direction: int,
    stop: Decimal | None,
    target: Decimal | None,
    ambiguity_policy: str,
) -> tuple[tuple[EquityBarRevision, ...], EquityBarRevision, Decimal] | None:
    if stop is None or target is None:
        return None
    for index, bar in enumerate(bars):
        bar_stop = bar.low_price <= stop if direction == 1 else bar.high_price >= stop
        bar_target = bar.high_price >= target if direction == 1 else bar.low_price <= target
        if not bar_stop and not bar_target:
            continue
        use_target = bar_target and not bar_stop
        if bar_stop and bar_target:
            use_target = ambiguity_policy == "TARGET_FIRST_SENSITIVITY"
        exit_price = target if use_target else _stop_fill(bar, direction, stop)
        return bars[:index + 1], bar, exit_price
    return None


def _stop_fill(
    bar: EquityBarRevision,
    direction: int,
    stop: Decimal,
) -> Decimal:
    if direction == 1 and bar.open_price < stop:
        return bar.open_price
    if direction == -1 and bar.open_price > stop:
        return bar.open_price
    return stop


def _category(net_return: float, first_hit: str, ambiguity_policy: str) -> str:
    if first_hit == "SAME_BAR":
        if ambiguity_policy == "CONSERVATIVE_STOP_FIRST":
            return "LOSS"
        if ambiguity_policy == "TARGET_FIRST_SENSITIVITY":
            return "WIN"
        return "AMBIGUOUS_SAME_BAR"
    if first_hit == "STOP":
        return "LOSS"
    if first_hit == "TARGET":
        return "WIN"
    return "WIN" if net_return > 0 else "LOSS"


def _benchmark_return(
    bars: tuple[EquityBarRevision, ...],
    entry_time: datetime,
    exit_time: datetime,
) -> float | None:
    path = tuple(
        row for row in sorted(bars, key=lambda item: item.bar_start)
        if row.bar_start >= entry_time and row.bar_end <= exit_time
    )
    if not path:
        return None
    return float(path[-1].close_price / path[0].open_price - Decimal("1"))


def _decimal(value) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    return result if result.is_finite() else None


def _next_daily_entry_session(observed_at: datetime):
    observed_utc = _utc(observed_at, "observed_at")
    calendar = exchange_calendars.get_calendar("XNYS")
    observed_date = pd.Timestamp(observed_utc.date())
    if not calendar.is_session(observed_date):
        return calendar.date_to_session(
            observed_date, direction="next"
        ).date()
    session_open = calendar.session_open(observed_date).to_pydatetime()
    if observed_utc < session_open:
        return observed_date.date()
    return calendar.next_session(observed_date).date()


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)