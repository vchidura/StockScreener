"""Immutable contracts and the versioned, storage-independent identity codec."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Mapping
from uuid import UUID


CODEC_VERSION = "equity_model_canonical_v1"


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def sha256(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("identity must be a lowercase SHA-256")
    return value


def nonempty(value: str) -> None:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError("identity must be a nonempty, trimmed string")


def finite(value: Decimal, *, positive: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("numeric values must be finite Decimals")
    if positive and value <= 0:
        raise ValueError("price/threshold must be positive")


def canonical_value(value: object) -> object:
    if isinstance(value, Enum):
        return canonical_value(value.value)
    if isinstance(value, datetime):
        return utc(value).isoformat().replace("+00:00", "Z")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        finite(value)
        rendered = format(value, "f")
        return (rendered.rstrip("0").rstrip(".") if "." in rendered else rendered) if value else "0"
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: canonical_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical object keys must be strings")
        return {key: canonical_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [canonical_value(item) for item in value]
    if type(value) in (str, int, bool, type(None)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json(value: object) -> str:
    return json.dumps(canonical_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


def freeze(value: object) -> object:
    """Copy JSON-compatible containers; frozen dataclasses never retain caller dicts."""
    canonical_value(value)
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in value)
    return value


class AdjustmentBasis(str, Enum):
    RAW = "RAW"
    SPLIT_ADJUSTED = "SPLIT_ADJUSTED"


class Disposition(str, Enum):
    MATCH = "MATCH"
    NO_MATCH = "NO_MATCH"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


class Direction(int, Enum):
    LONG = 1
    SHORT = -1


class Readiness(str, Enum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


class GateStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNAVAILABLE = "UNAVAILABLE"
    # Optional context with no data: it does not stop a match, and it never reads as a pass.
    NOT_APPLICABLE = "NOT_APPLICABLE"


class Phase(str, Enum):
    IDLE = "IDLE"
    ACTIVE = "ACTIVE"


class Transition(str, Enum):
    HOLD = "HOLD"
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    REPLACE = "REPLACE"
    EXPIRE = "EXPIRE"


@dataclass(frozen=True, slots=True)
class SecurityIdentity:
    security_id: UUID
    ticker: str
    exchange: str
    currency: str

    def __post_init__(self) -> None:
        if not isinstance(self.security_id, UUID):
            raise ValueError("security_id must be a UUID")
        for value in (self.ticker, self.exchange, self.currency):
            nonempty(value)


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    security_id: UUID
    source_id: str
    revision: str
    payload_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.security_id, UUID):
            raise ValueError("source security_id must be a UUID")
        nonempty(self.source_id)
        nonempty(self.revision)
        sha256(self.payload_sha256)


@dataclass(frozen=True, slots=True)
class ObservationClocks:
    market_start: datetime
    market_end: datetime
    available_at: datetime
    received_at: datetime

    def __post_init__(self) -> None:
        for name in ("market_start", "market_end", "available_at", "received_at"):
            object.__setattr__(self, name, utc(getattr(self, name)))
        if not self.market_start <= self.market_end <= self.available_at <= self.received_at:
            raise ValueError("observation clocks are not causal")


@dataclass(frozen=True, slots=True)
class AdjustmentLineage:
    basis: AdjustmentBasis
    action_sha256: str
    version: str

    def __post_init__(self) -> None:
        if not isinstance(self.basis, AdjustmentBasis):
            raise ValueError("unsupported adjustment basis")
        sha256(self.action_sha256)
        nonempty(self.version)


@dataclass(frozen=True, slots=True)
class CompletedBar:
    source: SourceIdentity
    clocks: ObservationClocks
    adjustment: AdjustmentLineage
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    completed: bool = True

    def __post_init__(self) -> None:
        if self.completed is not True or self.clocks.market_start >= self.clocks.market_end:
            raise ValueError("only completed, nonempty bars are supported")
        if self.interval not in {"1m", "5m", "30m", "1h", "1d"}:
            raise ValueError("unsupported bar interval")
        for price in (self.open, self.high, self.low, self.close):
            finite(price, positive=True)
        finite(self.volume)
        if self.volume < 0 or not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("invalid OHLCV geometry")


@dataclass(frozen=True, slots=True)
class CompletedEvent:
    source: SourceIdentity
    clocks: ObservationClocks
    event_kind: str
    value: str
    completed: bool = True

    def __post_init__(self) -> None:
        nonempty(self.event_kind)
        nonempty(self.value)
        if self.completed is not True:
            raise ValueError("only completed events are supported")


@dataclass(frozen=True, slots=True)
class FrozenContext:
    source: SourceIdentity
    clocks: ObservationClocks
    market_trend: Decimal | None
    sector_trend: Decimal | None
    event_risk: bool | None

    def __post_init__(self) -> None:
        for value in (self.market_trend, self.sector_trend):
            if value is not None:
                finite(value)
        if self.event_risk is not None and type(self.event_risk) is not bool:
            raise ValueError("event_risk must be bool or None")


@dataclass(frozen=True, slots=True)
class ModelIdentity:
    model_id: str
    version: str
    style: str
    origin: str
    policy_sha256: str

    def __post_init__(self) -> None:
        for value in (self.model_id, self.version, self.style, self.origin):
            nonempty(value)
        sha256(self.policy_sha256)


@dataclass(frozen=True, slots=True)
class PriorState:
    state_key: str
    revision: int
    phase: Phase
    trigger_identity: str | None = None
    valid_until: datetime | None = None
    last_input_sha256: str | None = None

    def __post_init__(self) -> None:
        sha256(self.state_key)
        if type(self.revision) is not int or self.revision < 0 or not isinstance(self.phase, Phase):
            raise ValueError("invalid prior state")
        if self.trigger_identity is not None:
            sha256(self.trigger_identity)
        if self.last_input_sha256 is not None:
            sha256(self.last_input_sha256)
        if self.valid_until is not None:
            object.__setattr__(self, "valid_until", utc(self.valid_until))
        if self.phase is Phase.ACTIVE and (self.trigger_identity is None or self.valid_until is None):
            raise ValueError("active state requires trigger and expiry")
        if self.phase is Phase.IDLE and (self.trigger_identity is not None or self.valid_until is not None):
            raise ValueError("idle state cannot carry an active trigger")


@dataclass(frozen=True, slots=True)
class CanonicalInput:
    security: SecurityIdentity
    adjustment: AdjustmentLineage
    model: ModelIdentity
    bars: tuple[CompletedBar, ...]
    events: tuple[CompletedEvent, ...]
    context: FrozenContext
    market_cutoff: datetime
    decision_at: datetime
    prior_state: PriorState | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "bars", tuple(self.bars))
        object.__setattr__(self, "events", tuple(self.events))
        object.__setattr__(self, "market_cutoff", utc(self.market_cutoff))
        object.__setattr__(self, "decision_at", utc(self.decision_at))
        if self.market_cutoff > self.decision_at:
            raise ValueError("market cutoff exceeds decision clock")
        observations = (*self.bars, *self.events, self.context)
        if any(item.source.security_id != self.security.security_id for item in observations):
            raise ValueError("mixed security identities")
        if any(bar.adjustment != self.adjustment for bar in self.bars):
            raise ValueError("mixed adjustment/action lineage")
        if any(item.clocks.market_end > self.market_cutoff for item in observations):
            raise ValueError("observation exceeds market cutoff")
        ends = tuple(bar.clocks.market_end for bar in self.bars)
        if ends != tuple(sorted(set(ends))):
            raise ValueError("bars must be unique and ordered by market end")
        if any(left.clocks.market_end > right.clocks.market_start for left, right in zip(self.bars, self.bars[1:])):
            raise ValueError("bars overlap")
        source_ids = tuple((item.source.source_id, item.source.revision) for item in observations)
        if len(set(source_ids)) != len(source_ids):
            raise ValueError("duplicate source identities")
        if tuple(self.events) != tuple(sorted(self.events, key=lambda item: (item.clocks.market_end, item.source.source_id))):
            raise ValueError("events must be ordered by market end and source identity")

    @property
    def observation_sha256(self) -> str:
        """Causal observations, excluding prior state; used for retry detection."""
        return canonical_sha256({field.name: getattr(self, field.name) for field in fields(self) if field.name != "prior_state"})

    @property
    def input_sha256(self) -> str:
        return canonical_sha256({"codec": CODEC_VERSION, "input": self})


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    version: str
    input_sha256: str
    values: Mapping[str, Decimal | bool | None]
    readiness: Readiness
    missing_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        nonempty(self.version)
        sha256(self.input_sha256)
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))
        object.__setattr__(self, "missing_reasons", tuple(self.missing_reasons))
        for value in self.values.values():
            if value is not None and type(value) is not bool:
                finite(value)
        if not isinstance(self.readiness, Readiness) or (self.readiness is Readiness.READY) != (not self.missing_reasons):
            raise ValueError("readiness must agree with missing/error reasons")

    @property
    def feature_sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class GateValue:
    name: str
    value: Decimal | bool | None
    threshold: Decimal | bool | None
    status: GateStatus

    def __post_init__(self) -> None:
        nonempty(self.name)
        if not isinstance(self.status, GateStatus):
            raise ValueError("invalid gate status")
        for value in (self.value, self.threshold):
            if value is not None and type(value) is not bool:
                finite(value)


@dataclass(frozen=True, slots=True)
class SuggestedGeometry:
    trigger: Decimal
    stop: Decimal
    target: Decimal
    direction: Direction
    valid_from: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        for value in (self.trigger, self.stop, self.target):
            finite(value, positive=True)
        object.__setattr__(self, "valid_from", utc(self.valid_from))
        object.__setattr__(self, "valid_until", utc(self.valid_until))
        if not isinstance(self.direction, Direction) or self.valid_until <= self.valid_from:
            raise ValueError("invalid geometry validity/direction")
        if self.direction * (self.trigger - self.stop) <= 0 or self.direction * (self.target - self.trigger) <= 0:
            raise ValueError("invalid suggested risk/reward geometry")


@dataclass(frozen=True, slots=True)
class StateDecision:
    state_key: str
    revision: int
    phase: Phase
    transition: Transition
    trigger_identity: str | None
    valid_until: datetime | None
    last_input_sha256: str | None

    def __post_init__(self) -> None:
        self.as_prior()
        if not isinstance(self.transition, Transition):
            raise ValueError("invalid state transition")

    def as_prior(self) -> PriorState:
        return PriorState(self.state_key, self.revision, self.phase, self.trigger_identity, self.valid_until, self.last_input_sha256)


@dataclass(frozen=True, slots=True)
class Decision:
    model: ModelIdentity
    input_sha256: str
    features: FeatureSnapshot
    disposition: Disposition
    gates: tuple[GateValue, ...]
    direction: Direction | None
    trigger_identity: str | None
    state: StateDecision
    geometry: SuggestedGeometry | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        sha256(self.input_sha256)
        object.__setattr__(self, "gates", tuple(self.gates))
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))
        if not isinstance(self.disposition, Disposition):
            raise ValueError("invalid disposition")
        if self.disposition is Disposition.MATCH:
            if self.direction is None or self.geometry is None or self.trigger_identity is None:
                raise ValueError("match requires direction, geometry and trigger")
            sha256(self.trigger_identity)
            if self.direction != self.geometry.direction or not self.gates or any(gate.status not in {GateStatus.PASS, GateStatus.NOT_APPLICABLE} for gate in self.gates):
                raise ValueError("inconsistent match geometry or gates")
        elif self.direction is not None or self.geometry is not None or self.trigger_identity is not None:
            raise ValueError("non-match cannot suggest a trade")

    @property
    def decision_sha256(self) -> str:
        return canonical_sha256(self)

    @property
    def valid_until(self) -> datetime | None:
        return self.geometry.valid_until if self.geometry else None
