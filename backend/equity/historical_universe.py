"""Causal, signal-agnostic historical research-universe selection."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from statistics import median
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd

from .polygon import sha256_json


@dataclass(frozen=True, slots=True)
class HistoricalUniversePolicy:
    policy_version: str = "liquid_us_common_stocks_v2"
    market: str = "stocks"
    security_types: tuple[str, ...] = ("CS",)
    active_only: bool = True
    lookback_sessions: int = 20
    minimum_coverage_ratio: float = 0.90
    minimum_price: Decimal = Decimal("5")
    minimum_median_dollar_volume: Decimal = Decimal("20000000")

    def __post_init__(self) -> None:
        if not self.policy_version:
            raise ValueError("policy_version is required")
        if self.lookback_sessions <= 0:
            raise ValueError("lookback_sessions must be positive")
        if not 0 < self.minimum_coverage_ratio <= 1:
            raise ValueError("minimum_coverage_ratio must be in (0, 1]")
        if self.minimum_price < 0 or self.minimum_median_dollar_volume < 0:
            raise ValueError("eligibility thresholds cannot be negative")
        normalized_types = tuple(sorted({value.upper() for value in self.security_types}))
        if not normalized_types:
            raise ValueError("at least one security type is required")
        object.__setattr__(self, "security_types", normalized_types)

    @property
    def policy_sha256(self) -> str:
        return sha256_json({
            "active_only": self.active_only,
            "lookback_sessions": self.lookback_sessions,
            "market": self.market,
            "minimum_coverage_ratio": self.minimum_coverage_ratio,
            "minimum_median_dollar_volume": str(self.minimum_median_dollar_volume),
            "minimum_price": str(self.minimum_price),
            "policy_version": self.policy_version,
            "security_types": self.security_types,
        })


@dataclass(frozen=True, slots=True)
class HistoricalUniverseMember:
    ticker: str
    reference_payload: Mapping[str, Any]
    latest_price: Decimal
    median_dollar_volume: Decimal
    observed_sessions: int
    required_sessions: int


@dataclass(frozen=True, slots=True)
class HistoricalUniverseSelection:
    signal_date: date
    members: tuple[HistoricalUniverseMember, ...]
    reference_count: int
    exclusion_counts: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class HistoricalSessionPlan:
    warmup_sessions: tuple[date, ...]
    research_sessions: tuple[date, ...]

    @property
    def all_sessions(self) -> tuple[date, ...]:
        return self.warmup_sessions + self.research_sessions


def historical_session_plan(
    *,
    end_date: date,
    research_sessions: int,
    warmup_sessions: int,
    calendar_name: str = "XNYS",
) -> HistoricalSessionPlan:
    if research_sessions <= 0:
        raise ValueError("research_sessions must be positive")
    if warmup_sessions < 0:
        raise ValueError("warmup_sessions cannot be negative")
    calendar = exchange_calendars.get_calendar(calendar_name)
    end_session = calendar.date_to_session(pd.Timestamp(end_date), direction="previous")
    total = research_sessions + warmup_sessions
    sessions = calendar.sessions_window(end_session, -total)
    dates = tuple(pd.Timestamp(value).date() for value in sessions)
    return HistoricalSessionPlan(
        warmup_sessions=dates[:warmup_sessions],
        research_sessions=dates[warmup_sessions:],
    )


def historical_universe_run_id(
    *,
    signal_date: date,
    policy_sha256: str,
    source_request_sha256: str,
    member_tickers: Sequence[str],
) -> UUID:
    identity = sha256_json({
        "members": sorted({ticker.upper() for ticker in member_tickers}),
        "policy_sha256": policy_sha256,
        "signal_date": signal_date.isoformat(),
        "source_request_sha256": source_request_sha256,
    })
    return uuid5(NAMESPACE_URL, f"historical-research-universe:{identity}")


def grouped_daily_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result = {}
    for row in rows:
        ticker = str(row.get("T") or row.get("ticker") or "")
        if not ticker:
            continue
        result[ticker] = {
            "close": row.get("c") if "c" in row else row.get("close"),
            "volume": row.get("v") if "v" in row else row.get("volume"),
        }
    return result


def select_historical_members(
    reference_rows: Sequence[Mapping[str, Any]],
    daily_history: Mapping[date, Mapping[str, Mapping[str, Any]]],
    *,
    signal_date: date,
    prior_sessions: Sequence[date],
    policy: HistoricalUniversePolicy,
) -> HistoricalUniverseSelection:
    """Select one session's members using only bars before ``signal_date``."""
    ordered_sessions = tuple(sorted(set(prior_sessions)))
    if len(ordered_sessions) > policy.lookback_sessions:
        ordered_sessions = ordered_sessions[-policy.lookback_sessions:]
    if any(session >= signal_date for session in ordered_sessions):
        raise ValueError("prior_sessions must precede signal_date")
    required_sessions = math.ceil(
        policy.lookback_sessions * policy.minimum_coverage_ratio
    )
    exclusions = {
        "INSUFFICIENT_HISTORY": 0,
        "LOW_DOLLAR_VOLUME": 0,
        "LOW_PRICE": 0,
        "UNSUPPORTED_SECURITY_TYPE": 0,
    }
    references = {}
    for row in reference_rows:
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            references[ticker] = row

    members = []
    for ticker, reference in sorted(references.items()):
        security_type = str(reference.get("type") or "").upper()
        if security_type not in policy.security_types or (
            policy.active_only and not bool(reference.get("active", False))
        ):
            exclusions["UNSUPPORTED_SECURITY_TYPE"] += 1
            continue
        observations = []
        for session in ordered_sessions:
            row = daily_history.get(session, {}).get(ticker)
            if not row:
                continue
            close = _decimal(row.get("close") if "close" in row else row.get("c"))
            volume = _decimal(row.get("volume") if "volume" in row else row.get("v"))
            if close is None or close <= 0 or volume is None or volume < 0:
                continue
            observations.append((close, close * volume))
        if len(observations) < required_sessions:
            exclusions["INSUFFICIENT_HISTORY"] += 1
            continue
        latest_price = observations[-1][0]
        if latest_price < policy.minimum_price:
            exclusions["LOW_PRICE"] += 1
            continue
        median_dollar_volume = Decimal(median(value for _, value in observations))
        if median_dollar_volume < policy.minimum_median_dollar_volume:
            exclusions["LOW_DOLLAR_VOLUME"] += 1
            continue
        members.append(HistoricalUniverseMember(
            ticker=ticker,
            reference_payload=reference,
            latest_price=latest_price,
            median_dollar_volume=median_dollar_volume,
            observed_sessions=len(observations),
            required_sessions=required_sessions,
        ))
    members.sort(key=lambda row: (-row.median_dollar_volume, row.ticker))
    return HistoricalUniverseSelection(
        signal_date=signal_date,
        members=tuple(members),
        reference_count=len(references),
        exclusion_counts=exclusions,
    )


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None