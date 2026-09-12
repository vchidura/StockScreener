from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5


class OptionMarketEventType(str, Enum):
    EARNINGS = "EARNINGS"
    FED_RATE_DECISION = "FED_RATE_DECISION"


class OptionMarketEventConfidence(str, Enum):
    CONFIRMED = "CONFIRMED"
    ESTIMATED = "ESTIMATED"
    UNKNOWN = "UNKNOWN"


class OptionMarketEventStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    COMPLETED = "COMPLETED"
    CANCELED = "CANCELED"
    REVISED = "REVISED"


@dataclass(frozen=True, slots=True)
class OptionMarketEvent:
    market_event_id: UUID
    event_type: OptionMarketEventType
    affected_underlying: str | None
    scheduled_time: datetime
    source: str
    source_key: str
    announcement_time: datetime | None
    first_observed_at: datetime
    source_observed_at: datetime
    confidence: OptionMarketEventConfidence
    status: OptionMarketEventStatus
    payload_sha256: str

    def __post_init__(self) -> None:
        _validate_scope(self.event_type, self.affected_underlying)
        _non_empty(self.source, "source")
        _non_empty(self.source_key, "source_key")
        scheduled = _utc(self.scheduled_time, "scheduled_time")
        observed = _utc(self.first_observed_at, "first_observed_at")
        source_observed = _utc(self.source_observed_at, "source_observed_at")
        announced = (
            _utc(self.announcement_time, "announcement_time")
            if self.announcement_time is not None
            else None
        )
        if announced is not None and announced > observed:
            raise ValueError("announcement_time cannot follow first_observed_at")
        _sha256(self.payload_sha256)
        object.__setattr__(self, "scheduled_time", scheduled)
        object.__setattr__(self, "first_observed_at", observed)
        object.__setattr__(self, "source_observed_at", source_observed)
        object.__setattr__(self, "announcement_time", announced)


@dataclass(frozen=True, slots=True)
class OptionEventCalendarCoverage:
    coverage_id: UUID
    event_type: OptionMarketEventType
    affected_underlying: str | None
    window_start: datetime
    window_end: datetime
    source: str
    source_key: str
    first_observed_at: datetime
    source_observed_at: datetime
    payload_sha256: str

    def __post_init__(self) -> None:
        _validate_scope(self.event_type, self.affected_underlying)
        _non_empty(self.source, "source")
        _non_empty(self.source_key, "source_key")
        start = _utc(self.window_start, "window_start")
        end = _utc(self.window_end, "window_end")
        observed = _utc(self.first_observed_at, "first_observed_at")
        source_observed = _utc(self.source_observed_at, "source_observed_at")
        if end <= start:
            raise ValueError("coverage window_end must follow window_start")
        _sha256(self.payload_sha256)
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        object.__setattr__(self, "first_observed_at", observed)
        object.__setattr__(self, "source_observed_at", source_observed)


@dataclass(frozen=True, slots=True)
class OptionEventCalendarBatch:
    source: str
    observed_at: datetime
    coverage: tuple[OptionEventCalendarCoverage, ...]
    events: tuple[OptionMarketEvent, ...]


def parse_event_calendar_document(
    payload: Mapping[str, Any],
    *,
    received_at: datetime,
) -> OptionEventCalendarBatch:
    if payload.get("schema_version") != 1:
        raise ValueError("event calendar schema_version must be 1")
    source = str(payload.get("source") or "").strip().lower()
    _non_empty(source, "source")
    source_observed_at = _parse_datetime(payload.get("observed_at"), "observed_at")
    received_at = _utc(received_at, "received_at")
    coverage = tuple(
        _parse_coverage(row, source, source_observed_at, received_at)
        for row in _rows(payload, "coverage")
    )
    events = tuple(
        _parse_event(row, source, source_observed_at, received_at)
        for row in _rows(payload, "events")
    )
    for name, rows in (("coverage", coverage), ("events", events)):
        keys = [row.source_key for row in rows]
        if len(keys) != len(set(keys)):
            raise ValueError(f"{name} source_key values must be unique per document")
    return OptionEventCalendarBatch(source, received_at, coverage, events)


def _parse_coverage(
    row: Mapping[str, Any], source: str,
    source_observed_at: datetime, received_at: datetime,
) -> OptionEventCalendarCoverage:
    event_type = OptionMarketEventType(str(row.get("event_type") or ""))
    underlying = _underlying(row.get("affected_underlying"))
    source_key = str(row.get("source_key") or "").strip()
    canonical = _canonical_json(dict(row))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return OptionEventCalendarCoverage(
        coverage_id=uuid5(
            NAMESPACE_URL,
            f"option-event-coverage:{source}:{source_key}:{received_at.isoformat()}:{digest}",
        ),
        event_type=event_type,
        affected_underlying=underlying,
        window_start=_parse_datetime(row.get("window_start"), "window_start"),
        window_end=_parse_datetime(row.get("window_end"), "window_end"),
        source=source,
        source_key=source_key,
        first_observed_at=received_at,
        source_observed_at=source_observed_at,
        payload_sha256=digest,
    )


def _parse_event(
    row: Mapping[str, Any], source: str,
    source_observed_at: datetime, received_at: datetime,
) -> OptionMarketEvent:
    event_type = OptionMarketEventType(str(row.get("event_type") or ""))
    underlying = _underlying(row.get("affected_underlying"))
    source_key = str(row.get("source_key") or "").strip()
    canonical = _canonical_json(dict(row))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return OptionMarketEvent(
        market_event_id=uuid5(
            NAMESPACE_URL,
            f"option-market-event:{source}:{source_key}:{received_at.isoformat()}:{digest}",
        ),
        event_type=event_type,
        affected_underlying=underlying,
        scheduled_time=_parse_datetime(row.get("scheduled_time"), "scheduled_time"),
        source=source,
        source_key=source_key,
        announcement_time=(
            _parse_datetime(row["announcement_time"], "announcement_time")
            if row.get("announcement_time") is not None
            else None
        ),
        first_observed_at=received_at,
        source_observed_at=source_observed_at,
        confidence=OptionMarketEventConfidence(
            str(row.get("confidence") or "UNKNOWN")
        ),
        status=OptionMarketEventStatus(str(row.get("status") or "SCHEDULED")),
        payload_sha256=digest,
    )


def _rows(payload: Mapping[str, Any], name: str) -> tuple[Mapping[str, Any], ...]:
    value = payload.get(name, ())
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ValueError(f"{name} must be an array of objects")
    return tuple(value)


def _underlying(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized or None


def _validate_scope(
    event_type: OptionMarketEventType, affected_underlying: str | None
) -> None:
    if event_type is OptionMarketEventType.EARNINGS and not affected_underlying:
        raise ValueError("earnings facts require affected_underlying")
    if event_type is OptionMarketEventType.FED_RATE_DECISION and affected_underlying:
        raise ValueError("Fed facts must not have affected_underlying")


def _parse_datetime(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO-8601 timestamp")
    return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")), name)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _non_empty(value: str, name: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} cannot be blank")


def _sha256(value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("payload_sha256 must be a SHA-256 digest")


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)