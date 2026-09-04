"""Immutable equity facts shared by scanners, the portal, and option analysis."""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from uuid import UUID


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class EvidenceType(str, Enum):
    FEATURE_SNAPSHOT = "FEATURE_SNAPSHOT"
    FUNDAMENTAL_SNAPSHOT = "FUNDAMENTAL_SNAPSHOT"
    SCANNER_RESULT = "SCANNER_RESULT"
    REGIME_SIGNAL = "REGIME_SIGNAL"
    PATTERN_OBSERVATION = "PATTERN_OBSERVATION"
    PRICE_CHANNEL = "PRICE_CHANNEL"
    TRADE_SETUP = "TRADE_SETUP"
    RANGE_FORECAST = "RANGE_FORECAST"
    MARKET_REGIME = "MARKET_REGIME"


class EvidenceRole(str, Enum):
    REGIME = "REGIME"
    DIRECTION = "DIRECTION"
    LOCATION = "LOCATION"
    TRIGGER = "TRIGGER"
    PARTICIPATION = "PARTICIPATION"
    RISK = "RISK"
    SETUP = "SETUP"


class LifecycleStatus(str, Enum):
    SNAPSHOT = "SNAPSHOT"
    MATCH = "MATCH"
    FORMING = "FORMING"
    AT_EDGE = "AT_EDGE"
    CONFIRMED = "CONFIRMED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    CONFLICTED = "CONFLICTED"
    UNAVAILABLE = "UNAVAILABLE"


class QualityState(str, Enum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    STALE = "STALE"
    FAILED = "FAILED"


class ContextStatus(str, Enum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    CONFLICTED = "CONFLICTED"
    UNAVAILABLE = "UNAVAILABLE"
    FAILED = "FAILED"


class BarSourceKind(str, Enum):
    NATIVE_REST = "NATIVE_REST"
    REALTIME_STREAM = "REALTIME_STREAM"
    DERIVED = "DERIVED"
    RECONCILED = "RECONCILED"


class BarAvailabilityMode(str, Enum):
    LIVE_OBSERVED = "LIVE_OBSERVED"
    HISTORICAL_RECONSTRUCTED = "HISTORICAL_RECONSTRUCTED"


class BarSessionScope(str, Enum):
    RTH = "RTH"
    EXTENDED = "EXTENDED"
    FULL_DAY = "FULL_DAY"


@dataclass(frozen=True, slots=True)
class DecisionWatermark:
    market_time: datetime
    observed_time: datetime

    def __post_init__(self) -> None:
        market_time = _as_utc(self.market_time, "market_time")
        observed_time = _as_utc(self.observed_time, "observed_time")
        if observed_time < market_time:
            raise ValueError("observed_time cannot precede market_time")
        object.__setattr__(self, "market_time", market_time)
        object.__setattr__(self, "observed_time", observed_time)


@dataclass(frozen=True, slots=True)
class SecurityReferenceRevision:
    security_revision_id: UUID
    security_id: UUID
    ticker: str
    active: bool
    company_name: str | None
    security_type: str | None
    cik: str | None
    composite_figi: str | None
    share_class_figi: str | None
    primary_exchange: str | None
    sic_code: str | None
    sic_description: str | None
    sector: str | None
    industry: str | None
    list_date: date | None
    delisted_date: date | None
    weighted_shares: Decimal | None
    free_float: Decimal | None
    free_float_percent: float | None
    market_cap: Decimal | None
    source: str
    effective_from: datetime
    observed_at: datetime
    payload_sha256: str
    raw_payload_json: str

    def __post_init__(self) -> None:
        _non_empty(self.ticker, "ticker")
        _non_empty(self.source, "source")
        effective_from = _as_utc(self.effective_from, "effective_from")
        observed_at = _as_utc(self.observed_at, "observed_at")
        if observed_at < effective_from:
            raise ValueError("observed_at cannot precede effective_from")
        _sha256(self.payload_sha256, "payload_sha256")
        _json_object(self.raw_payload_json, "raw_payload_json")
        for value, name in (
            (self.weighted_shares, "weighted_shares"),
            (self.free_float, "free_float"),
            (self.market_cap, "market_cap"),
        ):
            _optional_nonnegative_decimal(value, name)
        if self.free_float_percent is not None and not 0 <= self.free_float_percent <= 100:
            raise ValueError("free_float_percent must be in [0, 100]")
        object.__setattr__(self, "ticker", self.ticker.upper())
        object.__setattr__(self, "effective_from", effective_from)
        object.__setattr__(self, "observed_at", observed_at)


@dataclass(frozen=True, slots=True)
class EquityCorporateAction:
    corporate_action_id: UUID
    security_id: UUID
    ticker: str
    action_type: str
    effective_date: date
    declaration_date: date | None
    ex_date: date | None
    record_date: date | None
    pay_date: date | None
    cash_amount: Decimal | None
    split_from: Decimal | None
    split_to: Decimal | None
    new_ticker: str | None
    source: str
    source_key: str
    first_observed_at: datetime
    revised_observed_at: datetime | None
    payload_sha256: str
    raw_payload_json: str
    availability_mode: BarAvailabilityMode
    replay_available_at: datetime | None

    def __post_init__(self) -> None:
        _non_empty(self.ticker, "ticker")
        _non_empty(self.source, "source")
        _non_empty(self.source_key, "source_key")
        if self.action_type not in (
            "SPLIT", "DIVIDEND", "SYMBOL_CHANGE", "SPINOFF", "MERGER", "OTHER",
        ):
            raise ValueError("invalid corporate action type")
        observed = _as_utc(self.first_observed_at, "first_observed_at")
        replay = (
            _as_utc(self.replay_available_at, "replay_available_at")
            if self.replay_available_at else None
        )
        if self.availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED:
            if replay is None or replay > observed:
                raise ValueError("reconstructed corporate action requires replay availability")
        elif replay is not None:
            raise ValueError("live corporate action cannot set replay availability")
        _sha256(self.payload_sha256, "payload_sha256")
        _json_object(self.raw_payload_json, "raw_payload_json")
        object.__setattr__(self, "ticker", self.ticker.upper())
        object.__setattr__(self, "first_observed_at", observed)
        object.__setattr__(self, "replay_available_at", replay)


@dataclass(frozen=True, slots=True)
class FundamentalReport:
    fundamental_report_id: UUID
    security_id: UUID
    security_revision_id: UUID | None
    cik: str | None
    accession_number: str | None
    form_type: str | None
    timeframe: str
    fiscal_year: int | None
    fiscal_quarter: int | None
    period_end: date
    filing_date: date | None
    availability_time: datetime
    observed_at: datetime
    source: str
    source_key: str
    metrics_json: str
    raw_payload_json: str
    payload_sha256: str
    quality_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.timeframe not in ("quarterly", "annual", "trailing_twelve_months"):
            raise ValueError("timeframe is invalid")
        if self.fiscal_quarter is not None and not 1 <= self.fiscal_quarter <= 4:
            raise ValueError("fiscal_quarter must be in [1, 4]")
        _non_empty(self.source, "source")
        _non_empty(self.source_key, "source_key")
        availability_time = _as_utc(self.availability_time, "availability_time")
        observed_at = _as_utc(self.observed_at, "observed_at")
        if observed_at < availability_time:
            raise ValueError("observed_at cannot precede availability_time")
        _json_object(self.metrics_json, "metrics_json")
        _json_object(self.raw_payload_json, "raw_payload_json")
        _sha256(self.payload_sha256, "payload_sha256")
        if type(self.quality_codes) is not tuple:
            raise TypeError("quality_codes must be a tuple")
        object.__setattr__(self, "availability_time", availability_time)
        object.__setattr__(self, "observed_at", observed_at)


@dataclass(frozen=True, slots=True)
class EquityBarRevision:
    bar_revision_id: UUID
    security_id: UUID
    ticker: str
    interval: str
    session_date: date
    bar_start: datetime
    bar_end: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    vwap: Decimal | None
    transaction_count: int | None
    source_kind: BarSourceKind
    availability_mode: BarAvailabilityMode
    is_final: bool
    system_observed_at: datetime
    replay_available_at: datetime | None
    adjusted: bool
    payload_sha256: str
    quality_codes: tuple[str, ...] = ()
    provider_published_at: datetime | None = None
    ingestion_segment_id: UUID | None = None
    source_bar_revision_ids: tuple[UUID, ...] = ()
    supersedes_bar_revision_id: UUID | None = None
    reconciliation_status: str | None = None
    session_scope: BarSessionScope = BarSessionScope.RTH

    def __post_init__(self) -> None:
        _non_empty(self.ticker, "ticker")
        if self.interval not in {"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"}:
            raise ValueError(f"unsupported interval: {self.interval}")
        bar_start = _as_utc(self.bar_start, "bar_start")
        bar_end = _as_utc(self.bar_end, "bar_end")
        observed_at = _as_utc(self.system_observed_at, "system_observed_at")
        replay_available_at = (
            _as_utc(self.replay_available_at, "replay_available_at")
            if self.replay_available_at is not None else None
        )
        provider_published_at = _optional_utc(
            self.provider_published_at, "provider_published_at"
        )
        if bar_end <= bar_start:
            raise ValueError("bar_end must be later than bar_start")
        if self.is_final and observed_at < bar_end:
            raise ValueError("a final bar cannot be observed before bar_end")
        for value, name in (
            (self.open_price, "open_price"),
            (self.high_price, "high_price"),
            (self.low_price, "low_price"),
            (self.close_price, "close_price"),
        ):
            _positive_decimal(value, name)
        _optional_nonnegative_decimal(self.volume, "volume")
        if self.high_price < max(self.open_price, self.close_price, self.low_price):
            raise ValueError("high_price violates OHLC bounds")
        if self.low_price > min(self.open_price, self.close_price, self.high_price):
            raise ValueError("low_price violates OHLC bounds")
        if self.vwap is not None:
            _positive_decimal(self.vwap, "vwap")
        if self.transaction_count is not None and self.transaction_count < 0:
            raise ValueError("transaction_count cannot be negative")
        _sha256(self.payload_sha256, "payload_sha256")
        if type(self.quality_codes) is not tuple:
            raise TypeError("quality_codes must be a tuple")
        if type(self.source_bar_revision_ids) is not tuple:
            raise TypeError("source_bar_revision_ids must be a tuple")
        if not isinstance(self.session_scope, BarSessionScope):
            raise TypeError("session_scope must be a BarSessionScope")
        object.__setattr__(self, "ticker", self.ticker.upper())
        object.__setattr__(self, "bar_start", bar_start)
        object.__setattr__(self, "bar_end", bar_end)
        object.__setattr__(self, "system_observed_at", observed_at)
        object.__setattr__(self, "replay_available_at", replay_available_at)
        object.__setattr__(self, "provider_published_at", provider_published_at)


@dataclass(frozen=True, slots=True)
class EquityEvidence:
    evidence_id: UUID
    evidence_key: str
    lifecycle_key: str | None
    evidence_type: EvidenceType
    evidence_role: EvidenceRole
    security_id: UUID
    ticker: str
    interval: str | None
    direction: int | None
    lifecycle_status: LifecycleStatus
    strength: float | None
    market_time: datetime
    observed_at: datetime
    valid_until: datetime | None
    source_name: str
    source_version: str
    payload_schema_version: str
    analysis_run_id: UUID | None
    latest_bar_revision_id: UUID | None
    security_revision_id: UUID | None
    fundamental_report_ids: tuple[UUID, ...]
    source_revision_ids: tuple[UUID, ...]
    quality_state: QualityState
    quality_codes: tuple[str, ...]
    qualification_revision_id: UUID | None
    payload_json: str
    payload_sha256: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.evidence_key, "evidence_key"),
            (self.ticker, "ticker"),
            (self.source_name, "source_name"),
            (self.source_version, "source_version"),
            (self.payload_schema_version, "payload_schema_version"),
        ):
            _non_empty(value, name)
        if self.direction not in (None, -1, 0, 1):
            raise ValueError("direction must be -1, 0, 1, or None")
        if self.strength is not None and (
            type(self.strength) is not float or not math.isfinite(self.strength)
            or not 0 <= self.strength <= 1
        ):
            raise ValueError("strength must be a finite float in [0, 1]")
        market_time = _as_utc(self.market_time, "market_time")
        observed_at = _as_utc(self.observed_at, "observed_at")
        valid_until = (
            _as_utc(self.valid_until, "valid_until")
            if self.valid_until is not None else None
        )
        if observed_at < market_time:
            raise ValueError("observed_at cannot precede market_time")
        if valid_until is not None and valid_until <= market_time:
            raise ValueError("valid_until must be later than market_time")
        _json_object(self.payload_json, "payload_json")
        _sha256(self.payload_sha256, "payload_sha256")
        for value, name in (
            (self.fundamental_report_ids, "fundamental_report_ids"),
            (self.source_revision_ids, "source_revision_ids"),
            (self.quality_codes, "quality_codes"),
        ):
            if type(value) is not tuple:
                raise TypeError(f"{name} must be a tuple")
        object.__setattr__(self, "ticker", self.ticker.upper())
        object.__setattr__(self, "market_time", market_time)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "valid_until", valid_until)


@dataclass(frozen=True, slots=True)
class EquityContextSnapshot:
    equity_context_snapshot_id: UUID
    security_id: UUID
    ticker: str
    strategy_horizon: str
    market_time: datetime
    observed_at: datetime
    valid_until: datetime | None
    status: ContextStatus
    universe_run_id: UUID | None
    security_revision_id: UUID | None
    fundamental_snapshot_id: UUID | None
    regime_state: str | None
    ema_direction: str | None
    qualified_direction: str | None
    direction_qualification_id: UUID | None
    direction_evidence_id: UUID | None
    direction_horizon: str | None
    direction_valid_until: datetime | None
    trigger_state: str | None
    trigger_valid_until: datetime | None
    range_forecast_id: UUID | None
    range_lower: Decimal | None
    range_upper: Decimal | None
    range_valid_until: datetime | None
    market_cap: Decimal | None
    shares_outstanding: Decimal | None
    free_float: Decimal | None
    dividend_yield: float | None
    enterprise_value: Decimal | None
    ebitda: Decimal | None
    operating_income: Decimal | None
    free_cash_flow: Decimal | None
    risk_levels_json: str
    conflict_state_json: str
    stale_components_json: str
    reason_codes: tuple[str, ...]
    summary_json: str
    context_policy_version: str
    context_policy_sha256: str

    def __post_init__(self) -> None:
        _non_empty(self.ticker, "ticker")
        _non_empty(self.strategy_horizon, "strategy_horizon")
        _non_empty(self.context_policy_version, "context_policy_version")
        market_time = _as_utc(self.market_time, "market_time")
        observed_at = _as_utc(self.observed_at, "observed_at")
        if observed_at < market_time:
            raise ValueError("observed_at cannot precede market_time")
        valid_until = _optional_utc(self.valid_until, "valid_until")
        direction_valid_until = _optional_utc(
            self.direction_valid_until, "direction_valid_until"
        )
        trigger_valid_until = _optional_utc(self.trigger_valid_until, "trigger_valid_until")
        range_valid_until = _optional_utc(self.range_valid_until, "range_valid_until")
        if self.qualified_direction is not None and (
            self.direction_qualification_id is None or self.direction_evidence_id is None
        ):
            raise ValueError("qualified direction requires qualification and evidence IDs")
        for value, name in (
            (self.ema_direction, "ema_direction"),
            (self.qualified_direction, "qualified_direction"),
        ):
            if value not in (None, "BULLISH", "BEARISH", "NEUTRAL"):
                raise ValueError(f"{name} has an invalid direction")
        if self.range_lower is not None and self.range_upper is not None \
                and self.range_upper < self.range_lower:
            raise ValueError("range_upper cannot be below range_lower")
        for value, name in (
            (self.risk_levels_json, "risk_levels_json"),
            (self.conflict_state_json, "conflict_state_json"),
            (self.summary_json, "summary_json"),
        ):
            _json_object(value, name)
        _json_array(self.stale_components_json, "stale_components_json")
        _sha256(self.context_policy_sha256, "context_policy_sha256")
        if type(self.reason_codes) is not tuple:
            raise TypeError("reason_codes must be a tuple")
        object.__setattr__(self, "ticker", self.ticker.upper())
        object.__setattr__(self, "market_time", market_time)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "valid_until", valid_until)
        object.__setattr__(self, "direction_valid_until", direction_valid_until)
        object.__setattr__(self, "trigger_valid_until", trigger_valid_until)
        object.__setattr__(self, "range_valid_until", range_valid_until)


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _optional_utc(value: datetime | None, name: str) -> datetime | None:
    return _as_utc(value, name) if value is not None else None


def _non_empty(value: str, name: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} cannot be blank")


def _sha256(value: str, name: str) -> None:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def _positive_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be a finite positive Decimal")


def _optional_nonnegative_decimal(value: Decimal | None, name: str) -> None:
    if value is not None and (
        type(value) is not Decimal or not value.is_finite() or value < 0
    ):
        raise ValueError(f"{name} must be a finite non-negative Decimal")


def _json_object(value: str, name: str) -> None:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must contain a JSON object")


def _json_array(value: str, name: str) -> None:
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        raise ValueError(f"{name} must contain a JSON array")