from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import TypeVar
from uuid import UUID

from .errors import ContextViolation


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class ContractType(str, Enum):
    CALL = "CALL"
    PUT = "PUT"


class AssetType(str, Enum):
    STOCK = "STOCK"
    ETF = "ETF"


class OptionUniverseMode(str, Enum):
    FIXED = "fixed"
    RANKED = "ranked"


class UniverseRunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class AnalysisStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    INCOMPLETE_MATRIX = "INCOMPLETE_MATRIX"
    REFERENCE_DRIFT_FAILED = "REFERENCE_DRIFT_FAILED"
    MODEL_QUALITY_FAILED = "MODEL_QUALITY_FAILED"
    FAILED = "FAILED"


class RawFileEventType(str, Enum):
    SNAPSHOT = "SNAPSHOT"
    TRADE = "TRADE"
    QUOTE = "QUOTE"


class DataCapability(str, Enum):
    CHAIN_SNAPSHOT = "CHAIN_SNAPSHOT"
    OPTION_TRADES = "OPTION_TRADES"
    OPTION_QUOTES = "OPTION_QUOTES"
    UNDERLYING_PRICE = "UNDERLYING_PRICE"
    REAL_TIME = "REAL_TIME"


class ManifestCreationStatus(str, Enum):
    WRITING = "WRITING"
    COMPLETE = "COMPLETE"
    MISSING = "MISSING"
    CORRUPT = "CORRUPT"
    DELETED = "DELETED"


class RetentionScopeType(str, Enum):
    OBJECT = "OBJECT"
    TABLE = "TABLE"
    PARTITION = "PARTITION"
    FILE = "FILE"
    DECISION = "DECISION"
    INCIDENT = "INCIDENT"


class ExerciseStyle(str, Enum):
    AMERICAN = "AMERICAN"
    EUROPEAN = "EUROPEAN"


class MarkSource(str, Enum):
    DEVELOPER_ALIGNED_AGG_CLOSE = "DEVELOPER_ALIGNED_AGG_CLOSE"
    ADVANCED_NBBO_MIDPOINT = "ADVANCED_NBBO_MIDPOINT"
    BROKER_NBBO_MIDPOINT = "BROKER_NBBO_MIDPOINT"
    DISPLAY_DAY_CLOSE = "DISPLAY_DAY_CLOSE"
    DISPLAY_DAY_VWAP = "DISPLAY_DAY_VWAP"


class DataQualityFlag(str, Enum):
    MISSING_DAY_VOLUME = "MISSING_DAY_VOLUME"
    MISSING_OPEN_INTEREST = "MISSING_OPEN_INTEREST"
    STALE_MARK = "STALE_MARK"
    FALLBACK_MARK = "FALLBACK_MARK"
    NON_CONVERGED_IV = "NON_CONVERGED_IV"
    MISSING_FIELD = "MISSING_FIELD"
    BELOW_INTRINSIC_MARK = "BELOW_INTRINSIC_MARK"
    OPTION_SPOT_SKEW = "OPTION_SPOT_SKEW"
    MISSING_MARK_TIMESTAMP = "MISSING_MARK_TIMESTAMP"
    MISSING_ALIGNED_SPOT = "MISSING_ALIGNED_SPOT"
    NON_POSITIVE_MARK = "NON_POSITIVE_MARK"
    DIVIDEND_YIELD_DEFAULTED = "DIVIDEND_YIELD_DEFAULTED"
    REFERENCE_PENDING = "REFERENCE_PENDING"
    UNKNOWN_TRADE_CONDITION = "UNKNOWN_TRADE_CONDITION"
    UNKNOWN_CORRECTION = "UNKNOWN_CORRECTION"


class BatchStatus(str, Enum):
    FETCHING = "FETCHING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


class PageValidationStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"


class WorkStage(str, Enum):
    NORMALIZE = "NORMALIZE"
    ANALYZE = "ANALYZE"
    STRATEGY = "STRATEGY"
    ARCHIVE = "ARCHIVE"
    TRADE_BACKFILL = "TRADE_BACKFILL"
    CLASSIFY_TRADES = "CLASSIFY_TRADES"


class WorkStatus(str, Enum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    RETRY = "RETRY"
    COMPLETED = "COMPLETED"
    TERMINAL_FAILED = "TERMINAL_FAILED"


class NewSeriesState(str, Enum):
    UNKNOWN_REFERENCE = "UNKNOWN_REFERENCE"
    REFERENCE_PENDING = "REFERENCE_PENDING"
    VALIDATED_ACTIVE = "VALIDATED_ACTIVE"
    REJECTED_UNSUPPORTED = "REJECTED_UNSUPPORTED"
    REFERENCE_UNAVAILABLE = "REFERENCE_UNAVAILABLE"
    WATCHLIST_ACTIVE = "WATCHLIST_ACTIVE"


_NEW_SERIES_TRANSITIONS = {
    NewSeriesState.UNKNOWN_REFERENCE: frozenset({NewSeriesState.REFERENCE_PENDING}),
    NewSeriesState.REFERENCE_PENDING: frozenset(
        {
            NewSeriesState.VALIDATED_ACTIVE,
            NewSeriesState.REJECTED_UNSUPPORTED,
            NewSeriesState.REFERENCE_UNAVAILABLE,
        }
    ),
    NewSeriesState.REFERENCE_UNAVAILABLE: frozenset({NewSeriesState.REFERENCE_PENDING}),
    NewSeriesState.VALIDATED_ACTIVE: frozenset({NewSeriesState.WATCHLIST_ACTIVE}),
    NewSeriesState.REJECTED_UNSUPPORTED: frozenset(),
    NewSeriesState.WATCHLIST_ACTIVE: frozenset(),
}


def is_new_series_transition_allowed(
    current: NewSeriesState,
    target: NewSeriesState,
) -> bool:
    return target in _NEW_SERIES_TRANSITIONS[current]


def reference_drift_failed(
    unknown_reference_count: int,
    received_contract_count: int,
    *,
    maximum_unknown_references: int,
    maximum_unknown_reference_fraction: Decimal,
) -> bool:
    if unknown_reference_count < 0 or received_contract_count < 0:
        raise ValueError("reference counts cannot be negative")
    if maximum_unknown_references <= 0:
        raise ValueError("maximum_unknown_references must be positive")
    if not Decimal("0") < maximum_unknown_reference_fraction <= Decimal("1"):
        raise ValueError("maximum_unknown_reference_fraction must be in (0, 1]")
    if received_contract_count == 0:
        return unknown_reference_count > 0
    threshold = min(
        Decimal(maximum_unknown_references),
        Decimal(received_contract_count) * maximum_unknown_reference_fraction,
    )
    return Decimal(unknown_reference_count) > threshold


class TradeClassificationStatus(str, Enum):
    PENDING = "PENDING"
    INCLUDED = "INCLUDED"
    EXCLUDED = "EXCLUDED"
    SUPERSEDED = "SUPERSEDED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"


class TradeSemanticsBehavior(str, Enum):
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"
    SUPERSEDE = "SUPERSEDE"
    CANCEL = "CANCEL"
    UNKNOWN = "UNKNOWN"


class CatalogEligibility(str, Enum):
    VALIDATED_ACTIVE = "VALIDATED_ACTIVE"
    REJECTED_UNSUPPORTED = "REJECTED_UNSUPPORTED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class ContractReferenceValidation:
    contract_type: ContractType | None
    exercise_style: ExerciseStyle | None
    eligibility_status: CatalogEligibility
    exclusion_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NewSeriesResolution:
    discovery_id: int
    state: NewSeriesState
    contract_id: int | None
    activate_after: datetime | None

    def __post_init__(self) -> None:
        if self.discovery_id <= 0:
            raise ValueError("discovery_id must be positive")
        if self.contract_id is not None and self.contract_id <= 0:
            raise ValueError("contract_id must be positive")
        if self.activate_after is not None:
            object.__setattr__(
                self,
                "activate_after",
                _as_utc(self.activate_after, "activate_after"),
            )


@dataclass(frozen=True, slots=True)
class OptionUniverseCandidate:
    run_id: UUID
    ticker: str
    asset_type: AssetType
    raw_metrics_json: str
    component_ranks_json: str
    total_score: float | None
    eligible: bool
    exclusion_reasons: tuple[str, ...]
    candidate_rank: int | None
    first_observed_at: datetime

    def __post_init__(self) -> None:
        _non_empty(self.ticker, "ticker")
        for value, name in (
            (self.raw_metrics_json, "raw_metrics_json"),
            (self.component_ranks_json, "component_ranks_json"),
        ):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{name} must contain valid JSON") from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"{name} must contain a JSON object")
        _optional_float(self.total_score, "total_score")
        if type(self.exclusion_reasons) is not tuple:
            raise TypeError("exclusion_reasons must be a tuple")
        if self.candidate_rank is not None and self.candidate_rank <= 0:
            raise ValueError("candidate_rank must be positive")
        object.__setattr__(
            self,
            "first_observed_at",
            _as_utc(self.first_observed_at, "first_observed_at"),
        )


@dataclass(frozen=True, slots=True)
class OptionUniverseMember:
    effective_from: date
    ticker: str
    asset_type: AssetType
    source_run_id: UUID
    member_rank: int | None
    score: float | None
    activated_at: datetime
    deactivated_at: datetime | None
    first_observed_at: datetime

    def __post_init__(self) -> None:
        _non_empty(self.ticker, "ticker")
        if self.member_rank is not None and self.member_rank <= 0:
            raise ValueError("member_rank must be positive")
        _optional_float(self.score, "score")
        activated_at = _as_utc(self.activated_at, "activated_at")
        deactivated_at = (
            _as_utc(self.deactivated_at, "deactivated_at")
            if self.deactivated_at is not None
            else None
        )
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        if deactivated_at is not None and deactivated_at <= activated_at:
            raise ValueError("deactivated_at must be later than activated_at")
        if first_observed_at < activated_at:
            raise ValueError("first_observed_at cannot precede activated_at")
        object.__setattr__(self, "activated_at", activated_at)
        object.__setattr__(self, "deactivated_at", deactivated_at)
        object.__setattr__(self, "first_observed_at", first_observed_at)


@dataclass(frozen=True, slots=True)
class OptionAnalysisRun:
    matrix_id: UUID
    batch_id: UUID
    underlyer: str
    context: DecisionContext
    status: AnalysisStatus
    received_contract_count: int
    eligible_contract_count: int
    unknown_reference_count: int
    iv_attempt_count: int
    iv_converged_count: int
    iv_convergence_fraction: float | None
    quality_reasons: tuple[str, ...]
    chain_health_json: str
    policy_version: str
    policy_sha256: str
    model_version: str
    started_at: datetime
    completed_at: datetime | None

    def __post_init__(self) -> None:
        _non_empty(self.underlyer, "underlyer")
        _non_empty(self.policy_version, "policy_version")
        _non_empty(self.model_version, "model_version")
        _sha256(self.policy_sha256, "policy_sha256")
        for value, name in (
            (self.received_contract_count, "received_contract_count"),
            (self.eligible_contract_count, "eligible_contract_count"),
            (self.unknown_reference_count, "unknown_reference_count"),
            (self.iv_attempt_count, "iv_attempt_count"),
            (self.iv_converged_count, "iv_converged_count"),
        ):
            if value < 0:
                raise ValueError(f"{name} cannot be negative")
        if self.eligible_contract_count > self.received_contract_count:
            raise ValueError("eligible_contract_count cannot exceed received_contract_count")
        if self.iv_converged_count > self.iv_attempt_count:
            raise ValueError("iv_converged_count cannot exceed iv_attempt_count")
        _optional_float(self.iv_convergence_fraction, "iv_convergence_fraction")
        if self.iv_convergence_fraction is not None and not 0 <= self.iv_convergence_fraction <= 1:
            raise ValueError("iv_convergence_fraction must be in [0, 1]")
        if type(self.quality_reasons) is not tuple:
            raise TypeError("quality_reasons must be a tuple")
        try:
            chain_health = json.loads(self.chain_health_json)
        except json.JSONDecodeError as exc:
            raise ValueError("chain_health_json must contain valid JSON") from exc
        if not isinstance(chain_health, dict):
            raise ValueError("chain_health_json must contain a JSON object")
        started_at = _as_utc(self.started_at, "started_at")
        completed_at = (
            _as_utc(self.completed_at, "completed_at")
            if self.completed_at is not None
            else None
        )
        if completed_at is not None and completed_at < started_at:
            raise ValueError("completed_at cannot precede started_at")
        if self.status is AnalysisStatus.RUNNING and completed_at is not None:
            raise ValueError("a running analysis cannot have completed_at")
        if self.status not in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING) and completed_at is None:
            raise ValueError("a terminal analysis requires completed_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)


@dataclass(frozen=True, slots=True)
class OptionExpirationAnalytics:
    matrix_id: UUID
    expiration_date: date
    fractional_maturity_years: float
    forward_price: Decimal | None
    atm_iv: float | None
    call_25_delta_iv: float | None
    put_25_delta_iv: float | None
    call_skew_25_delta: float | None
    put_skew_25_delta: float | None
    risk_reversal_25_delta: float | None
    interpolation_diagnostics_json: str
    put_volume: int | None
    call_volume: int | None
    put_open_interest: int | None
    call_open_interest: int | None
    breadth: int | None
    concentration_metrics_json: str
    wall_clusters_json: str
    term_change: float | None
    term_slope: float | None
    quality_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _optional_float(self.fractional_maturity_years, "fractional_maturity_years")
        if self.fractional_maturity_years < 0:
            raise ValueError("fractional_maturity_years cannot be negative")
        _optional_decimal(self.forward_price, "forward_price", allow_zero=False)
        for value, name in (
            (self.atm_iv, "atm_iv"),
            (self.call_25_delta_iv, "call_25_delta_iv"),
            (self.put_25_delta_iv, "put_25_delta_iv"),
            (self.call_skew_25_delta, "call_skew_25_delta"),
            (self.put_skew_25_delta, "put_skew_25_delta"),
            (self.risk_reversal_25_delta, "risk_reversal_25_delta"),
            (self.term_change, "term_change"),
            (self.term_slope, "term_slope"),
        ):
            _optional_float(value, name)
        for value, name in (
            (self.put_volume, "put_volume"),
            (self.call_volume, "call_volume"),
            (self.put_open_interest, "put_open_interest"),
            (self.call_open_interest, "call_open_interest"),
            (self.breadth, "breadth"),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{name} cannot be negative")
        for value, name, expected_type in (
            (self.interpolation_diagnostics_json, "interpolation_diagnostics_json", dict),
            (self.concentration_metrics_json, "concentration_metrics_json", dict),
            (self.wall_clusters_json, "wall_clusters_json", list),
        ):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{name} must contain valid JSON") from exc
            if not isinstance(parsed, expected_type):
                raise ValueError(f"{name} has the wrong JSON shape")
        if type(self.quality_reasons) is not tuple:
            raise TypeError("quality_reasons must be a tuple")


@dataclass(frozen=True, slots=True)
class RawFileManifest:
    file_id: UUID
    event_type: RawFileEventType
    market_date: date
    underlyer: str
    market_hour: int
    object_key: str
    schema_version: int
    row_count: int
    minimum_source_time: datetime | None
    maximum_source_time: datetime | None
    byte_size: int
    payload_sha256: str
    creation_status: ManifestCreationStatus
    retention_class: str

    def __post_init__(self) -> None:
        _non_empty(self.underlyer, "underlyer")
        _non_empty(self.object_key, "object_key")
        _non_empty(self.retention_class, "retention_class")
        if not 0 <= self.market_hour <= 23:
            raise ValueError("market_hour must be in [0, 23]")
        if self.schema_version <= 0 or self.row_count < 0 or self.byte_size < 0:
            raise ValueError("manifest size, rows, or schema version is invalid")
        minimum_source_time = (
            _as_utc(self.minimum_source_time, "minimum_source_time")
            if self.minimum_source_time is not None
            else None
        )
        maximum_source_time = (
            _as_utc(self.maximum_source_time, "maximum_source_time")
            if self.maximum_source_time is not None
            else None
        )
        if (
            minimum_source_time is not None
            and maximum_source_time is not None
            and maximum_source_time < minimum_source_time
        ):
            raise ValueError("maximum_source_time cannot precede minimum_source_time")
        _sha256(self.payload_sha256, "payload_sha256")
        object.__setattr__(self, "minimum_source_time", minimum_source_time)
        object.__setattr__(self, "maximum_source_time", maximum_source_time)


@dataclass(frozen=True, slots=True)
class RetentionHold:
    hold_id: UUID
    scope_type: RetentionScopeType
    selector_json: str
    reason: str
    actor: str
    created_at: datetime
    expires_at: datetime | None

    def __post_init__(self) -> None:
        _non_empty(self.reason, "reason")
        _non_empty(self.actor, "actor")
        try:
            selector = json.loads(self.selector_json)
        except json.JSONDecodeError as exc:
            raise ValueError("selector_json must contain valid JSON") from exc
        if not isinstance(selector, dict) or not selector:
            raise ValueError("selector_json must contain a non-empty JSON object")
        created_at = _as_utc(self.created_at, "created_at")
        expires_at = (
            _as_utc(self.expires_at, "expires_at")
            if self.expires_at is not None
            else None
        )
        if expires_at is not None and expires_at <= created_at:
            raise ValueError("expires_at must be later than created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "expires_at", expires_at)


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _positive_decimal(value: Decimal, name: str, *, allow_zero: bool = False) -> None:
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite() or (value < 0 if allow_zero else value <= 0):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be a finite {qualifier} Decimal")


def _optional_decimal(value: Decimal | None, name: str, *, allow_zero: bool = True) -> None:
    if value is not None:
        _positive_decimal(value, name, allow_zero=allow_zero)


def _optional_signed_decimal(value: Decimal | None, name: str) -> None:
    if value is not None and (type(value) is not Decimal or not value.is_finite()):
        raise TypeError(f"{name} must be a finite Decimal")


def _optional_float(value: float | None, name: str) -> None:
    if value is not None and (type(value) is not float or not math.isfinite(value)):
        raise TypeError(f"{name} must be a finite float")


def _sha256(value: str, name: str) -> None:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def _non_empty(value: str, name: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{name} cannot be blank")


def _unique_enum_tuple(value: tuple[_EnumT, ...], name: str) -> None:
    if type(value) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if len(value) != len(set(value)):
        raise ValueError(f"{name} must not contain duplicates")


@dataclass(frozen=True, slots=True)
class DecisionContext:
    market_time: datetime
    observed_time: datetime

    def __post_init__(self) -> None:
        market_time = _as_utc(self.market_time, "market_time")
        observed_time = _as_utc(self.observed_time, "observed_time")
        if observed_time < market_time:
            raise ValueError("observed_time cannot precede market_time")
        object.__setattr__(self, "market_time", market_time)
        object.__setattr__(self, "observed_time", observed_time)

    def require_available(self, market_data_time: datetime, first_observed_at: datetime) -> None:
        market_data_time = _as_utc(market_data_time, "market_data_time")
        first_observed_at = _as_utc(first_observed_at, "first_observed_at")
        if market_data_time > self.market_time or first_observed_at > self.observed_time:
            raise ContextViolation("record is not available within the decision context")


@dataclass(frozen=True, slots=True)
class MarketTimestamp:
    market_data_time: datetime
    first_observed_at: datetime
    revised_observed_at: datetime | None = None

    def __post_init__(self) -> None:
        market_data_time = _as_utc(self.market_data_time, "market_data_time")
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        revised_observed_at = (
            _as_utc(self.revised_observed_at, "revised_observed_at")
            if self.revised_observed_at is not None
            else None
        )
        if first_observed_at < market_data_time:
            raise ValueError("first_observed_at cannot precede market_data_time")
        if revised_observed_at is not None and revised_observed_at < first_observed_at:
            raise ValueError("revised_observed_at cannot precede first_observed_at")
        object.__setattr__(self, "market_data_time", market_data_time)
        object.__setattr__(self, "first_observed_at", first_observed_at)
        object.__setattr__(self, "revised_observed_at", revised_observed_at)

    @property
    def available_at(self) -> datetime:
        return self.revised_observed_at or self.first_observed_at

    @property
    def data_delay_seconds(self) -> float:
        return (self.first_observed_at - self.market_data_time).total_seconds()

    def require_available(self, context: DecisionContext) -> None:
        context.require_available(self.market_data_time, self.available_at)


@dataclass(frozen=True, slots=True)
class SpotPrice:
    underlyer: str
    provider: str
    price: Decimal
    market_data_time: datetime
    first_observed_at: datetime

    def __post_init__(self) -> None:
        _non_empty(self.underlyer, "underlyer")
        _non_empty(self.provider, "provider")
        _positive_decimal(self.price, "price")
        market_data_time = _as_utc(self.market_data_time, "market_data_time")
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        if first_observed_at < market_data_time:
            raise ValueError("first_observed_at cannot precede market_data_time")
        object.__setattr__(self, "market_data_time", market_data_time)
        object.__setattr__(self, "first_observed_at", first_observed_at)


@dataclass(frozen=True, slots=True)
class OptionTradeCursor:
    sip_timestamp: datetime
    sequence_number: int
    overlap_seconds: int

    def __post_init__(self) -> None:
        if self.sequence_number < 0 or self.overlap_seconds < 0:
            raise ValueError("trade cursor sequence and overlap cannot be negative")
        object.__setattr__(
            self,
            "sip_timestamp",
            _as_utc(self.sip_timestamp, "sip_timestamp"),
        )


@dataclass(frozen=True, slots=True)
class OptionTradeFetchResult:
    raw_batch_id: UUID
    events: tuple[OptionTradeEvent, ...]
    request_ids: tuple[str, ...]
    complete: bool
    terminal_page_received: bool

    def __post_init__(self) -> None:
        if type(self.events) is not tuple or type(self.request_ids) is not tuple:
            raise TypeError("trade fetch events and request_ids must be tuples")
        if self.complete and not self.terminal_page_received:
            raise ValueError("a complete trade fetch requires a terminal page")


@dataclass(frozen=True, slots=True)
class OptionContractReference:
    contract_ticker: str
    underlyer: str
    asset_type: AssetType
    provider: str
    provider_version: str | None
    provider_contract_type: str
    expiration_date: date
    strike: Decimal
    provider_exercise_style: str
    shares_per_contract: int
    primary_exchange: str | None
    correction: str | None
    additional_underlyings_json: str
    adjustment_metadata_json: str
    changes_deliverables: bool
    valid_from: datetime
    valid_to: datetime | None
    first_observed_at: datetime
    revised_observed_at: datetime | None
    refreshed_at: datetime
    payload_sha256: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.contract_ticker, "contract_ticker"),
            (self.underlyer, "underlyer"),
            (self.provider, "provider"),
            (self.provider_contract_type, "provider_contract_type"),
            (self.provider_exercise_style, "provider_exercise_style"),
        ):
            _non_empty(value, name)
        _positive_decimal(self.strike, "strike")
        if self.shares_per_contract <= 0:
            raise ValueError("shares_per_contract must be positive")
        try:
            additional_underlyings = json.loads(self.additional_underlyings_json)
            adjustment_metadata = json.loads(self.adjustment_metadata_json)
        except json.JSONDecodeError as exc:
            raise ValueError("reference JSON fields must contain valid JSON") from exc
        if not isinstance(additional_underlyings, list):
            raise ValueError("additional_underlyings_json must contain a JSON array")
        if not isinstance(adjustment_metadata, dict):
            raise ValueError("adjustment_metadata_json must contain a JSON object")
        _sha256(self.payload_sha256, "payload_sha256")

        valid_from = _as_utc(self.valid_from, "valid_from")
        valid_to = _as_utc(self.valid_to, "valid_to") if self.valid_to is not None else None
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        revised_observed_at = (
            _as_utc(self.revised_observed_at, "revised_observed_at")
            if self.revised_observed_at is not None
            else None
        )
        refreshed_at = _as_utc(self.refreshed_at, "refreshed_at")
        if valid_to is not None and valid_to <= valid_from:
            raise ValueError("valid_to must be later than valid_from")
        if first_observed_at < valid_from:
            raise ValueError("first_observed_at cannot precede valid_from")
        if revised_observed_at is not None and revised_observed_at < first_observed_at:
            raise ValueError("revised_observed_at cannot precede first_observed_at")
        if refreshed_at < first_observed_at:
            raise ValueError("refreshed_at cannot precede first_observed_at")
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_to", valid_to)
        object.__setattr__(self, "first_observed_at", first_observed_at)
        object.__setattr__(self, "revised_observed_at", revised_observed_at)
        object.__setattr__(self, "refreshed_at", refreshed_at)


def validate_standard_contract(
    reference: OptionContractReference,
    *,
    required_exercise_style: ExerciseStyle = ExerciseStyle.AMERICAN,
    required_shares_per_contract: int = 100,
) -> ContractReferenceValidation:
    contract_type_value = reference.provider_contract_type.strip().upper()
    contract_type = (
        ContractType(contract_type_value)
        if contract_type_value in {member.value for member in ContractType}
        else None
    )
    exercise_style_value = reference.provider_exercise_style.strip().upper()
    exercise_style = (
        ExerciseStyle(exercise_style_value)
        if exercise_style_value in {member.value for member in ExerciseStyle}
        else None
    )
    reasons: list[str] = []
    if contract_type is None:
        reasons.append("UNSUPPORTED_CONTRACT_TYPE")
    if exercise_style is not required_exercise_style:
        reasons.append("UNSUPPORTED_EXERCISE_STYLE")
    if reference.shares_per_contract != required_shares_per_contract:
        reasons.append("UNSUPPORTED_MULTIPLIER")
    if json.loads(reference.additional_underlyings_json) or reference.changes_deliverables:
        reasons.append("ADJUSTED_CONTRACT")
    return ContractReferenceValidation(
        contract_type=contract_type,
        exercise_style=exercise_style,
        eligibility_status=(
            CatalogEligibility.VALIDATED_ACTIVE
            if not reasons
            else CatalogEligibility.REJECTED_UNSUPPORTED
        ),
        exclusion_reasons=tuple(reasons),
    )


@dataclass(frozen=True, slots=True)
class OptionContractCatalogEntry:
    contract_id: int
    contract_ticker: str
    underlyer: str
    asset_type: AssetType
    provider: str
    provider_version: str | None
    contract_type: ContractType
    expiration_date: date
    strike: Decimal
    exercise_style: ExerciseStyle
    shares_per_contract: int
    primary_exchange: str | None
    eligibility_status: CatalogEligibility
    exclusion_reasons: tuple[str, ...]
    valid_from: datetime
    valid_to: datetime | None
    first_observed_at: datetime
    revised_observed_at: datetime | None
    payload_sha256: str

    def __post_init__(self) -> None:
        if self.contract_id <= 0:
            raise ValueError("contract_id must be positive")
        for value, name in (
            (self.contract_ticker, "contract_ticker"),
            (self.underlyer, "underlyer"),
            (self.provider, "provider"),
        ):
            _non_empty(value, name)
        _positive_decimal(self.strike, "strike")
        if self.shares_per_contract <= 0:
            raise ValueError("shares_per_contract must be positive")
        if type(self.exclusion_reasons) is not tuple:
            raise TypeError("exclusion_reasons must be a tuple")
        _sha256(self.payload_sha256, "payload_sha256")

        valid_from = _as_utc(self.valid_from, "valid_from")
        valid_to = _as_utc(self.valid_to, "valid_to") if self.valid_to is not None else None
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        revised_observed_at = (
            _as_utc(self.revised_observed_at, "revised_observed_at")
            if self.revised_observed_at is not None
            else None
        )
        if valid_to is not None and valid_to <= valid_from:
            raise ValueError("valid_to must be later than valid_from")
        if first_observed_at < valid_from:
            raise ValueError("first_observed_at cannot precede valid_from")
        if revised_observed_at is not None and revised_observed_at < first_observed_at:
            raise ValueError("revised_observed_at cannot precede first_observed_at")
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_to", valid_to)
        object.__setattr__(self, "first_observed_at", first_observed_at)
        object.__setattr__(self, "revised_observed_at", revised_observed_at)

    def require_available(self, context: DecisionContext) -> None:
        context.require_available(
            self.valid_from, self.revised_observed_at or self.first_observed_at
        )


@dataclass(frozen=True, slots=True)
class OptionContractSnapshot:
    snapshot_id: UUID
    contract_id: int
    contract_ticker: str
    underlyer: str
    provider: str
    contract_type: ContractType
    expiration_date: date
    expiration_cutoff: datetime
    calendar_dte: int
    time_to_expiration_years: float
    strike: Decimal
    shares_per_contract: int
    exercise_style: ExerciseStyle
    spot: Decimal
    spot_market_data_time: datetime
    bid: Decimal | None
    ask: Decimal | None
    midpoint: Decimal | None
    display_mark: Decimal | None
    model_mark: Decimal | None
    mark_market_data_time: datetime
    mark_source: MarkSource
    day_volume: int | None
    open_interest: int | None
    market_data_time: datetime
    first_observed_at: datetime
    revised_observed_at: datetime | None
    local_iv: float | None
    local_gamma: float | None
    local_delta: float | None
    local_theta_per_day: float | None
    local_vega_per_vol_point: float | None
    local_rho_per_rate_point: float | None
    intrinsic_value: Decimal | None
    extrinsic_value: Decimal | None
    single_contract_breakeven: Decimal | None
    provider_iv: float | None
    provider_gamma: float | None
    risk_free_rate: float
    dividend_yield: float
    iv_converged: bool
    iv_solver: str | None
    iv_iteration_count: int
    iv_price_error: float | None
    iv_failure_reason: str | None
    model_version: str
    quality_flags: tuple[DataQualityFlag, ...]
    batch_id: UUID
    raw_payload_sha256: str
    normalized_payload_sha256: str
    revision: int = 1

    def __post_init__(self) -> None:
        if self.contract_id <= 0:
            raise ValueError("contract_id must be positive")
        for value, name in (
            (self.contract_ticker, "contract_ticker"),
            (self.underlyer, "underlyer"),
            (self.provider, "provider"),
        ):
            _non_empty(value, name)
        _positive_decimal(self.strike, "strike")
        _positive_decimal(self.spot, "spot")
        if self.calendar_dte < 0 or self.time_to_expiration_years <= 0:
            raise ValueError("snapshot maturity must be positive and unexpired")
        if self.shares_per_contract <= 0:
            raise ValueError("shares_per_contract must be positive")
        for value, name in (
            (self.bid, "bid"),
            (self.ask, "ask"),
            (self.midpoint, "midpoint"),
            (self.display_mark, "display_mark"),
            (self.intrinsic_value, "intrinsic_value"),
            (self.single_contract_breakeven, "single_contract_breakeven"),
        ):
            _optional_decimal(value, name)
        _optional_signed_decimal(self.extrinsic_value, "extrinsic_value")
        _optional_decimal(self.model_mark, "model_mark", allow_zero=False)
        if self.day_volume is not None and self.day_volume < 0:
            raise ValueError("day_volume cannot be negative")
        if self.open_interest is not None and self.open_interest < 0:
            raise ValueError("open_interest cannot be negative")
        for value, name in (
            (self.local_iv, "local_iv"),
            (self.local_gamma, "local_gamma"),
            (self.local_delta, "local_delta"),
            (self.local_theta_per_day, "local_theta_per_day"),
            (self.local_vega_per_vol_point, "local_vega_per_vol_point"),
            (self.local_rho_per_rate_point, "local_rho_per_rate_point"),
            (self.provider_iv, "provider_iv"),
            (self.provider_gamma, "provider_gamma"),
            (self.risk_free_rate, "risk_free_rate"),
            (self.dividend_yield, "dividend_yield"),
            (self.iv_price_error, "iv_price_error"),
        ):
            _optional_float(value, name)
        if self.iv_iteration_count < 0:
            raise ValueError("iv_iteration_count cannot be negative")
        if self.iv_converged and (self.local_iv is None or self.iv_solver is None):
            raise ValueError("converged IV requires local_iv and iv_solver")
        if not self.iv_converged and self.iv_failure_reason is None:
            raise ValueError("non-converged IV requires iv_failure_reason")
        _non_empty(self.model_version, "model_version")
        if self.revision <= 0:
            raise ValueError("revision must be positive")
        _unique_enum_tuple(self.quality_flags, "quality_flags")
        _sha256(self.raw_payload_sha256, "raw_payload_sha256")
        _sha256(self.normalized_payload_sha256, "normalized_payload_sha256")

        market_data_time = _as_utc(self.market_data_time, "market_data_time")
        expiration_cutoff = _as_utc(self.expiration_cutoff, "expiration_cutoff")
        spot_market_data_time = _as_utc(self.spot_market_data_time, "spot_market_data_time")
        mark_market_data_time = _as_utc(self.mark_market_data_time, "mark_market_data_time")
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        revised_observed_at = (
            _as_utc(self.revised_observed_at, "revised_observed_at")
            if self.revised_observed_at is not None
            else None
        )
        if first_observed_at < max(
            market_data_time, spot_market_data_time, mark_market_data_time
        ):
            raise ValueError("first_observed_at cannot precede a source timestamp")
        if revised_observed_at is not None and revised_observed_at < first_observed_at:
            raise ValueError("revised_observed_at cannot precede first_observed_at")
        object.__setattr__(self, "market_data_time", market_data_time)
        object.__setattr__(self, "expiration_cutoff", expiration_cutoff)
        object.__setattr__(self, "spot_market_data_time", spot_market_data_time)
        object.__setattr__(self, "mark_market_data_time", mark_market_data_time)
        object.__setattr__(self, "first_observed_at", first_observed_at)
        object.__setattr__(self, "revised_observed_at", revised_observed_at)

    @property
    def data_delay_seconds(self) -> float:
        return (self.first_observed_at - self.market_data_time).total_seconds()

    def require_available(self, context: DecisionContext) -> None:
        context.require_available(
            self.market_data_time, self.revised_observed_at or self.first_observed_at
        )


@dataclass(frozen=True, slots=True)
class OptionTradeEvent:
    trade_event_id: UUID
    provider: str
    contract_id: int
    contract_ticker: str
    underlyer: str
    sip_timestamp: datetime
    sequence_number: int
    participant_timestamp: datetime | None
    first_observed_at: datetime
    revised_observed_at: datetime | None
    exchange: int | None
    conditions: tuple[int, ...]
    correction: int | None
    price: Decimal
    size: int
    shares_per_contract: int
    notional: Decimal
    payload_sha256: str
    raw_batch_id: UUID
    classification_status: TradeClassificationStatus = TradeClassificationStatus.PENDING
    provider_trade_id: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.provider, "provider"),
            (self.contract_ticker, "contract_ticker"),
            (self.underlyer, "underlyer"),
        ):
            _non_empty(value, name)
        if self.contract_id <= 0:
            raise ValueError("contract_id must be positive")
        if self.sequence_number < 0:
            raise ValueError("sequence_number cannot be negative")
        if type(self.conditions) is not tuple:
            raise TypeError("conditions must be a tuple")
        if self.size <= 0 or self.shares_per_contract <= 0:
            raise ValueError("size and shares_per_contract must be positive")
        _positive_decimal(self.price, "price")
        _positive_decimal(self.notional, "notional")
        if self.notional != self.price * self.size * self.shares_per_contract:
            raise ValueError("notional must equal price * size * shares_per_contract")
        _sha256(self.payload_sha256, "payload_sha256")

        sip_timestamp = _as_utc(self.sip_timestamp, "sip_timestamp")
        participant_timestamp = (
            _as_utc(self.participant_timestamp, "participant_timestamp")
            if self.participant_timestamp is not None
            else None
        )
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        revised_observed_at = (
            _as_utc(self.revised_observed_at, "revised_observed_at")
            if self.revised_observed_at is not None
            else None
        )
        if first_observed_at < sip_timestamp:
            raise ValueError("first_observed_at cannot precede sip_timestamp")
        if revised_observed_at is not None and revised_observed_at < first_observed_at:
            raise ValueError("revised_observed_at cannot precede first_observed_at")
        object.__setattr__(self, "sip_timestamp", sip_timestamp)
        object.__setattr__(self, "participant_timestamp", participant_timestamp)
        object.__setattr__(self, "first_observed_at", first_observed_at)
        object.__setattr__(self, "revised_observed_at", revised_observed_at)

    @property
    def event_key(self) -> tuple[str, str, datetime, int, datetime | None, str]:
        return (
            self.provider,
            self.contract_ticker,
            self.sip_timestamp,
            self.sequence_number,
            self.participant_timestamp,
            self.payload_sha256,
        )

    def require_available(self, context: DecisionContext) -> None:
        context.require_available(
            self.sip_timestamp, self.revised_observed_at or self.first_observed_at
        )


@dataclass(frozen=True, slots=True)
class ProviderTradeSemantics:
    provider: str
    semantics_version: str
    condition_code: int | None
    correction_code: int | None
    behavior: TradeSemanticsBehavior
    contributes_volume: bool
    contributes_notional: bool
    effective_from: datetime
    effective_to: datetime | None
    first_observed_at: datetime
    configuration_sha256: str

    def __post_init__(self) -> None:
        _non_empty(self.provider, "provider")
        _non_empty(self.semantics_version, "semantics_version")
        if self.condition_code is None and self.correction_code is None:
            raise ValueError("trade semantics require a condition or correction code")
        effective_from = _as_utc(self.effective_from, "effective_from")
        effective_to = (
            _as_utc(self.effective_to, "effective_to")
            if self.effective_to is not None
            else None
        )
        first_observed_at = _as_utc(self.first_observed_at, "first_observed_at")
        if effective_to is not None and effective_to <= effective_from:
            raise ValueError("effective_to must be later than effective_from")
        if first_observed_at < effective_from:
            raise ValueError("first_observed_at cannot precede effective_from")
        _sha256(self.configuration_sha256, "configuration_sha256")
        object.__setattr__(self, "effective_from", effective_from)
        object.__setattr__(self, "effective_to", effective_to)
        object.__setattr__(self, "first_observed_at", first_observed_at)


@dataclass(frozen=True, slots=True)
class RawBatchPage:
    batch_id: UUID
    page_number: int
    row_count: int
    response_bytes: bytes
    payload_sha256: str
    received_at: datetime
    terminal: bool
    validation_status: PageValidationStatus
    request_filter_sha256: str
    request_cursor_sha256: str | None = None
    request_id: str | None = None
    next_cursor_sha256: str | None = None
    request_metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.page_number <= 0:
            raise ValueError("page_number must be positive")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        if type(self.response_bytes) is not bytes:
            raise TypeError("response_bytes must be bytes")
        if type(self.request_metadata) is not tuple:
            raise TypeError("request_metadata must be a tuple")
        _sha256(self.payload_sha256, "payload_sha256")
        _sha256(self.request_filter_sha256, "request_filter_sha256")
        if self.request_cursor_sha256 is not None:
            _sha256(self.request_cursor_sha256, "request_cursor_sha256")
        if self.next_cursor_sha256 is not None:
            _sha256(self.next_cursor_sha256, "next_cursor_sha256")
        if self.terminal and self.next_cursor_sha256 is not None:
            raise ValueError("a terminal page cannot contain a next cursor")
        object.__setattr__(self, "received_at", _as_utc(self.received_at, "received_at"))


@dataclass(frozen=True, slots=True)
class RawOptionBatch:
    batch_id: UUID
    provider: str
    underlyer: str
    scheduled_cycle: datetime
    request_filter_sha256: str
    policy_sha256: str
    status: BatchStatus
    pages: tuple[RawBatchPage, ...]
    started_at: datetime
    completed_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        _non_empty(self.provider, "provider")
        _non_empty(self.underlyer, "underlyer")
        _sha256(self.request_filter_sha256, "request_filter_sha256")
        _sha256(self.policy_sha256, "policy_sha256")
        if type(self.pages) is not tuple:
            raise TypeError("pages must be a tuple")
        scheduled_cycle = _as_utc(self.scheduled_cycle, "scheduled_cycle")
        started_at = _as_utc(self.started_at, "started_at")
        completed_at = (
            _as_utc(self.completed_at, "completed_at")
            if self.completed_at is not None
            else None
        )
        if any(page.batch_id != self.batch_id for page in self.pages):
            raise ValueError("every page must belong to this batch")
        if tuple(page.page_number for page in self.pages) != tuple(range(1, len(self.pages) + 1)):
            raise ValueError("batch pages must form a contiguous one-based sequence")
        terminal_pages = [page for page in self.pages if page.terminal]
        if self.status is BatchStatus.COMPLETE:
            if not self.pages or terminal_pages != [self.pages[-1]]:
                raise ValueError("a complete batch must end with exactly one terminal page")
            if any(page.validation_status is not PageValidationStatus.VALID for page in self.pages):
                raise ValueError("a complete batch cannot contain invalid pages")
            if completed_at is None:
                raise ValueError("a complete batch requires completed_at")
            if self.failure_reason is not None:
                raise ValueError("a complete batch cannot have a failure_reason")
        if self.status in (BatchStatus.FAILED, BatchStatus.QUARANTINED) and not self.failure_reason:
            raise ValueError("failed and quarantined batches require a failure_reason")
        if completed_at is not None and completed_at < started_at:
            raise ValueError("completed_at cannot precede started_at")
        object.__setattr__(self, "scheduled_cycle", scheduled_cycle)
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)

    @property
    def complete(self) -> bool:
        return self.status is BatchStatus.COMPLETE

    @property
    def row_count(self) -> int:
        return sum(page.row_count for page in self.pages)


@dataclass(frozen=True, slots=True)
class DurableWorkItem:
    work_id: UUID
    stage: WorkStage
    subject_id: str
    business_key: str
    status: WorkStatus
    attempt_count: int
    maximum_attempts: int
    created_at: datetime
    next_attempt_at: datetime
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_error: str | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        _non_empty(self.subject_id, "subject_id")
        _non_empty(self.business_key, "business_key")
        if self.attempt_count < 0 or self.maximum_attempts <= 0:
            raise ValueError("work attempt counts are invalid")
        if self.attempt_count > self.maximum_attempts:
            raise ValueError("attempt_count cannot exceed maximum_attempts")
        created_at = _as_utc(self.created_at, "created_at")
        next_attempt_at = _as_utc(self.next_attempt_at, "next_attempt_at")
        lease_expires_at = (
            _as_utc(self.lease_expires_at, "lease_expires_at")
            if self.lease_expires_at is not None
            else None
        )
        completed_at = (
            _as_utc(self.completed_at, "completed_at")
            if self.completed_at is not None
            else None
        )
        if self.status is WorkStatus.CLAIMED:
            if not self.lease_owner or lease_expires_at is None:
                raise ValueError("claimed work requires a lease owner and expiry")
        elif self.lease_owner is not None or lease_expires_at is not None:
            raise ValueError("only claimed work may have a lease")
        if self.status is WorkStatus.COMPLETED and completed_at is None:
            raise ValueError("completed work requires completed_at")
        if self.status is WorkStatus.TERMINAL_FAILED and not self.last_error:
            raise ValueError("terminally failed work requires last_error")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "next_attempt_at", next_attempt_at)
        object.__setattr__(self, "lease_expires_at", lease_expires_at)
        object.__setattr__(self, "completed_at", completed_at)