"""Decode already-extracted, frozen records. This module never opens artifacts.

    The wire schema is the canonical representation of ``CanonicalInput``.
    Research extraction owns clock reconstruction and adjustment normalization.
    V2 materialization owns qualification and source-coordinate retention.
"""
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping
from uuid import UUID

from .contracts import (
    AdjustmentBasis, AdjustmentLineage, CanonicalInput, CompletedBar,
    CompletedEvent, FrozenContext, ModelIdentity, ObservationClocks, Phase,
    PriorState, SecurityIdentity, SourceIdentity,
)


def _clock(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _clocks(row: Mapping[str, Any]) -> ObservationClocks:
    return ObservationClocks(**{key: _clock(row[key]) for key in ("market_start", "market_end", "available_at", "received_at")})


def _source(row: Mapping[str, Any]) -> SourceIdentity:
    return SourceIdentity(UUID(row["security_id"]), row["source_id"], row["revision"], row["payload_sha256"])


def _adjustment(row: Mapping[str, Any]) -> AdjustmentLineage:
    return AdjustmentLineage(AdjustmentBasis(row["basis"]), row["action_sha256"], row["version"])


def _decimal(value: Any) -> Decimal:
    # Binary floats and bools must never silently become prices or thresholds.
    if type(value) not in (str, int, Decimal):
        raise ValueError("wire numbers must be decimal strings or integers")
    return Decimal(value)


def decode_input(row: Mapping[str, Any]) -> CanonicalInput:
    """Validate the same wire schema at both adapter boundaries; no policy edits."""
    security = row["security"]
    context = row["context"]
    prior = row.get("prior_state")
    bars = tuple(CompletedBar(_source(bar["source"]), _clocks(bar["clocks"]), _adjustment(bar["adjustment"]),
                              bar["interval"], *(_decimal(bar[name]) for name in ("open", "high", "low", "close", "volume")),
                              completed=bar["completed"]) for bar in row["bars"])
    events = tuple(CompletedEvent(_source(event["source"]), _clocks(event["clocks"]), event["event_kind"],
                                  event["value"], event["completed"]) for event in row["events"])
    return CanonicalInput(
        SecurityIdentity(UUID(security["security_id"]), security["ticker"], security["exchange"], security["currency"]),
        _adjustment(row["adjustment"]), ModelIdentity(**row["model"]), bars, events,
        FrozenContext(_source(context["source"]), _clocks(context["clocks"]),
                      _decimal(context["market_trend"]) if context["market_trend"] is not None else None,
                      _decimal(context["sector_trend"]) if context["sector_trend"] is not None else None,
                      context["event_risk"]),
        _clock(row["market_cutoff"]), _clock(row["decision_at"]),
        PriorState(prior["state_key"], prior["revision"], Phase(prior["phase"]), prior["trigger_identity"],
                   _clock(prior["valid_until"]) if prior["valid_until"] is not None else None,
                   prior["last_input_sha256"]) if prior is not None else None,
    )
