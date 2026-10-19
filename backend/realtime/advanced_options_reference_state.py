"""Immutable Tier A reference state for one Advanced Options session."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import math
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from options.model_inputs import (
    ExpirationValuationCandidate,
    RiskFreeRateObservation,
)

from .contracts import canonical_sha256
from .dividend_reference import DividendReferenceWindow
from .reference_facts import ReferenceFact, ReferenceFactRelation


REFERENCE_STATE_VERSION = "advanced-options-reference-state-v1"


class ReferenceAvailability(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"


class EquityMetricKind(str, Enum):
    SHORT_INTEREST_PERCENT = "SHORT_INTEREST_PERCENT"
    TOTAL_FLOAT = "TOTAL_FLOAT"
    AVERAGE_DAILY_VOLUME_30 = "AVERAGE_DAILY_VOLUME_30"


def _utc(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty text")
    return value.strip()


def _ticker(value: str) -> str:
    ticker = _text(value, "ticker").upper()
    if ticker != value.strip():
        raise ValueError("ticker must already be uppercase")
    return ticker


def _decimal(
    value: Decimal,
    name: str,
    *,
    positive: bool = False,
    allow_negative: bool = False,
) -> Decimal:
    if (
        not isinstance(value, Decimal)
        or not value.is_finite()
        or (not allow_negative and value < 0)
        or (positive and value <= 0)
    ):
        raise ValueError(f"{name} must be finite")
    return value


def _reason_codes(values: tuple[str, ...]) -> tuple[str, ...]:
    reasons = tuple(sorted({_text(value, "reason_code").upper() for value in values}))
    return reasons


@dataclass(frozen=True, slots=True)
class AdvancedOptionsOpenInterestReference:
    contract_id: int
    contract_ticker: str
    underlyer: str
    availability: ReferenceAvailability
    known_at: datetime
    baseline_id: UUID | None = None
    settlement_session: date | None = None
    observed_session: date | None = None
    open_interest: int | None = None
    source: str | None = None
    date_basis: str | None = None
    observed_at: datetime | None = None
    received_at: datetime | None = None
    payload_sha256: str | None = None
    supersedes_baseline_id: UUID | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.contract_id) is not int or self.contract_id <= 0:
            raise ValueError("contract_id must be positive")
        ticker = _ticker(self.contract_ticker)
        underlyer = _ticker(self.underlyer)
        if not ticker.startswith(f"O:{underlyer}"):
            raise ValueError("OI contract ownership is inconsistent")
        known = _utc(self.known_at, "known_at")
        reasons = _reason_codes(self.reason_codes)
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "reason_codes", reasons)
        if self.availability is ReferenceAvailability.UNAVAILABLE:
            if any((
                self.baseline_id,
                self.settlement_session,
                self.observed_session,
                self.open_interest is not None,
                self.source,
                self.date_basis,
                self.observed_at,
                self.received_at,
                self.payload_sha256,
                self.supersedes_baseline_id,
            )) or not reasons:
                raise ValueError(
                    "unavailable OI must not contain a value or factual provenance"
                )
            return
        if self.availability is not ReferenceAvailability.AVAILABLE:
            raise ValueError("OI availability is invalid")
        if (
            not isinstance(self.baseline_id, UUID)
            or not isinstance(self.settlement_session, date)
            or not isinstance(self.observed_session, date)
            or self.settlement_session >= self.observed_session
            or type(self.open_interest) is not int
            or self.open_interest < 0
            or reasons
        ):
            raise ValueError("available OI identity or value is invalid")
        source = _text(self.source, "source")
        date_basis = _text(self.date_basis, "date_basis")
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        if not observed <= received <= known:
            raise ValueError("OI clocks are not causal")
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "date_basis", date_basis)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsAdjustedDailyBarIdentity:
    bar_revision_id: UUID
    security_id: UUID
    ticker: str
    session_date: date
    adjusted_close: Decimal
    market_close_at: datetime
    known_at: datetime
    adjustment_lineage_sha256: str
    finality_sha256: str
    payload_sha256: str
    finalized: bool = True

    def __post_init__(self) -> None:
        if (
            not isinstance(self.bar_revision_id, UUID)
            or not isinstance(self.security_id, UUID)
            or not isinstance(self.session_date, date)
            or self.finalized is not True
        ):
            raise ValueError("adjusted daily bar identity is invalid")
        market_close = _utc(self.market_close_at, "market_close_at")
        known = _utc(self.known_at, "known_at")
        if market_close > known or market_close.date() != self.session_date:
            raise ValueError("adjusted daily bar clocks are not causal")
        object.__setattr__(self, "ticker", _ticker(self.ticker))
        object.__setattr__(
            self,
            "adjusted_close",
            _decimal(self.adjusted_close, "adjusted_close", positive=True),
        )
        object.__setattr__(self, "market_close_at", market_close)
        object.__setattr__(self, "known_at", known)
        for name in (
            "adjustment_lineage_sha256",
            "finality_sha256",
            "payload_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsHistoricalVolatilityFact:
    security_id: UUID
    ticker: str
    window_sessions: int
    annualized_volatility: Decimal
    estimator: str
    estimator_version: str
    market_cutoff: datetime
    known_at: datetime
    payload_sha256: str
    bars: tuple[AdvancedOptionsAdjustedDailyBarIdentity, ...]

    def __post_init__(self) -> None:
        bars = tuple(sorted(self.bars, key=lambda row: row.session_date))
        cutoff = _utc(self.market_cutoff, "market_cutoff")
        known = _utc(self.known_at, "known_at")
        ticker = _ticker(self.ticker)
        if (
            not isinstance(self.security_id, UUID)
            or self.window_sessions not in (20, 30)
            or len(bars) != self.window_sessions + 1
            or len({row.session_date for row in bars}) != len(bars)
            or any(
                row.security_id != self.security_id
                or row.ticker != ticker
                or row.known_at > known
                or row.market_close_at > cutoff
                for row in bars
            )
            or cutoff > known
            or self.estimator != "CLOSE_TO_CLOSE"
            or self.estimator_version != "realized-volatility-close-v1"
        ):
            raise ValueError("historical volatility membership or clocks are invalid")
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(
            self,
            "annualized_volatility",
            _decimal(self.annualized_volatility, "annualized_volatility"),
        )
        expected = _close_to_close_hv(bars, self.window_sessions)
        if self.annualized_volatility != expected:
            raise ValueError("historical volatility does not match its bar membership")
        object.__setattr__(self, "market_cutoff", cutoff)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )
        object.__setattr__(self, "bars", bars)

    @property
    def bar_membership_sha256(self) -> str:
        return canonical_sha256(tuple(row.sha256 for row in self.bars))

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def _close_to_close_hv(
    bars: tuple[AdvancedOptionsAdjustedDailyBarIdentity, ...],
    window_sessions: int,
) -> Decimal:
    variances = []
    for previous, current in zip(bars, bars[1:]):
        if (current.session_date - previous.session_date).days > 7:
            raise ValueError("historical volatility bars contain a session gap")
        log_return = math.log(
            float(current.adjusted_close / previous.adjusted_close)
        )
        variances.append(max(log_return * log_return, 1e-10))
    if len(variances) != window_sessions:
        raise ValueError("historical volatility return membership is incomplete")
    return Decimal(repr(math.sqrt(
        sum(variances) / len(variances) * 252
    )))


def build_historical_volatility_fact(
    *,
    security_id: UUID,
    ticker: str,
    window_sessions: int,
    market_cutoff: datetime,
    known_at: datetime,
    bars: tuple[AdvancedOptionsAdjustedDailyBarIdentity, ...],
) -> AdvancedOptionsHistoricalVolatilityFact:
    ordered = tuple(sorted(bars, key=lambda row: row.session_date))
    volatility = _close_to_close_hv(ordered, window_sessions)
    payload_sha256 = canonical_sha256({
        "contract": "advanced-options-historical-volatility-v1",
        "security_id": security_id,
        "ticker": ticker,
        "window_sessions": window_sessions,
        "annualized_volatility": volatility,
        "estimator": "CLOSE_TO_CLOSE",
        "estimator_version": "realized-volatility-close-v1",
        "market_cutoff": market_cutoff,
        "known_at": known_at,
        "bar_membership": tuple(row.sha256 for row in ordered),
    })
    return AdvancedOptionsHistoricalVolatilityFact(
        security_id=security_id,
        ticker=ticker,
        window_sessions=window_sessions,
        annualized_volatility=volatility,
        estimator="CLOSE_TO_CLOSE",
        estimator_version="realized-volatility-close-v1",
        market_cutoff=market_cutoff,
        known_at=known_at,
        payload_sha256=payload_sha256,
        bars=ordered,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRateReference:
    observation_id: UUID
    rate_date: date
    tenor_days: int
    annual_rate: Decimal
    source: str
    source_key: str
    observed_at: datetime
    received_at: datetime
    payload_sha256: str

    def __post_init__(self) -> None:
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        if (
            not isinstance(self.observation_id, UUID)
            or not isinstance(self.rate_date, date)
            or type(self.tenor_days) is not int
            or self.tenor_days <= 0
            or observed > received
        ):
            raise ValueError("risk-free-rate reference is invalid")
        object.__setattr__(
            self,
            "annual_rate",
            _decimal(self.annual_rate, "annual_rate", allow_negative=True),
        )
        object.__setattr__(self, "source", _text(self.source, "source"))
        object.__setattr__(
            self,
            "source_key",
            _text(self.source_key, "source_key"),
        )
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCorporateActionIdentity:
    action_id: UUID
    payload_sha256: str
    observed_at: datetime
    received_at: datetime

    def __post_init__(self) -> None:
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        if not isinstance(self.action_id, UUID) or observed > received:
            raise ValueError("corporate-action identity is invalid")
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDividendCoverageReference:
    coverage_id: UUID
    security_id: UUID
    ticker: str
    window_start: date
    window_end: date
    status: str
    observed_at: datetime
    received_at: datetime
    payload_sha256: str
    response_sha256: str
    actions: tuple[AdvancedOptionsCorporateActionIdentity, ...] = ()

    def __post_init__(self) -> None:
        actions = tuple(sorted(self.actions, key=lambda row: str(row.action_id)))
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        status = _text(self.status, "status").upper()
        if (
            not isinstance(self.coverage_id, UUID)
            or not isinstance(self.security_id, UUID)
            or not isinstance(self.window_start, date)
            or not isinstance(self.window_end, date)
            or self.window_end < self.window_start
            or status not in {"AVAILABLE", "KNOWN_NO_ANNOUNCED_DIVIDEND"}
            or (status == "KNOWN_NO_ANNOUNCED_DIVIDEND" and actions)
            or observed > received
            or any(row.received_at != received for row in actions)
        ):
            raise ValueError("dividend coverage reference is invalid")
        object.__setattr__(self, "ticker", _ticker(self.ticker))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )
        object.__setattr__(
            self,
            "response_sha256",
            _sha256(self.response_sha256, "response_sha256"),
        )
        object.__setattr__(self, "actions", actions)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsValuationReference:
    security_id: UUID
    ticker: str
    valuation_date: date
    expiration_date: date
    risk_free_rate: Decimal
    dividend_yield: Decimal
    rate_observations: tuple[AdvancedOptionsRateReference, ...]
    dividend_coverage: AdvancedOptionsDividendCoverageReference
    model_version: str
    model_bundle_sha256: str
    valuation_policy_sha256: str
    known_at: datetime

    def __post_init__(self) -> None:
        rates = tuple(sorted(
            self.rate_observations,
            key=lambda row: (row.tenor_days, str(row.observation_id)),
        ))
        known = _utc(self.known_at, "known_at")
        ticker = _ticker(self.ticker)
        if (
            not isinstance(self.security_id, UUID)
            or not isinstance(self.valuation_date, date)
            or not isinstance(self.expiration_date, date)
            or self.expiration_date <= self.valuation_date
            or not rates
            or len({row.observation_id for row in rates}) != len(rates)
            or any(
                row.rate_date > self.valuation_date
                or row.received_at > known
                for row in rates
            )
            or self.dividend_coverage.security_id != self.security_id
            or self.dividend_coverage.ticker != ticker
            or self.dividend_coverage.window_start > self.valuation_date
            or self.dividend_coverage.window_end < self.expiration_date
            or self.dividend_coverage.received_at > known
        ):
            raise ValueError("valuation reference identity or coverage is invalid")
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(
            self,
            "risk_free_rate",
            _decimal(
                self.risk_free_rate,
                "risk_free_rate",
                allow_negative=True,
            ),
        )
        dividend_yield = _decimal(self.dividend_yield, "dividend_yield")
        if dividend_yield > 1:
            raise ValueError("dividend_yield must be at most one")
        object.__setattr__(self, "dividend_yield", dividend_yield)
        object.__setattr__(self, "rate_observations", rates)
        object.__setattr__(
            self,
            "model_version",
            _text(self.model_version, "model_version"),
        )
        for name in ("model_bundle_sha256", "valuation_policy_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        object.__setattr__(self, "known_at", known)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsEquityMetric:
    security_id: UUID
    ticker: str
    metric_kind: EquityMetricKind
    as_of_session: date
    availability: ReferenceAvailability
    known_at: datetime
    value: Decimal | None = None
    source: str | None = None
    source_key: str | None = None
    observed_at: datetime | None = None
    received_at: datetime | None = None
    payload_sha256: str | None = None
    calculation_version: str | None = None
    member_sessions: tuple[date, ...] = ()
    member_sha256: str | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        known = _utc(self.known_at, "known_at")
        reasons = _reason_codes(self.reason_codes)
        sessions = tuple(sorted(self.member_sessions))
        if (
            not isinstance(self.security_id, UUID)
            or not isinstance(self.as_of_session, date)
            or len(sessions) != len(set(sessions))
        ):
            raise ValueError("equity metric identity is invalid")
        object.__setattr__(self, "ticker", _ticker(self.ticker))
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "member_sessions", sessions)
        object.__setattr__(self, "reason_codes", reasons)
        if self.availability is ReferenceAvailability.UNAVAILABLE:
            if any((
                self.value is not None,
                self.source,
                self.source_key,
                self.observed_at,
                self.received_at,
                self.payload_sha256,
                self.calculation_version,
                sessions,
                self.member_sha256,
            )) or not reasons:
                raise ValueError(
                    "unavailable equity metric must not contain a default value"
                )
            return
        if self.availability is not ReferenceAvailability.AVAILABLE or reasons:
            raise ValueError("equity metric availability is invalid")
        value = _decimal(self.value, "value")
        source = _text(self.source, "source")
        source_key = _text(self.source_key, "source_key")
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        if not observed <= received <= known:
            raise ValueError("equity metric clocks are not causal")
        if self.metric_kind is EquityMetricKind.AVERAGE_DAILY_VOLUME_30:
            if (
                len(sessions) != 30
                or sessions[-1] != self.as_of_session
                or not self.calculation_version
                or self.member_sha256 is None
            ):
                raise ValueError("ADV30 membership is incomplete")
        elif sessions or self.member_sha256 or self.calculation_version:
            raise ValueError("point-in-time equity metric cannot carry bar membership")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )
        if self.calculation_version is not None:
            object.__setattr__(
                self,
                "calculation_version",
                _text(self.calculation_version, "calculation_version"),
            )
        if self.member_sha256 is not None:
            object.__setattr__(
                self,
                "member_sha256",
                _sha256(self.member_sha256, "member_sha256"),
            )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReferenceState:
    state_id: UUID
    state_sha256: str
    version: str
    reference_generation: int
    session_date: date
    known_at: datetime
    valid_until: datetime
    reference_policy_sha256: str
    contract_scope_sha256: str
    underlyings: tuple[str, ...]
    open_interest: tuple[AdvancedOptionsOpenInterestReference, ...]
    historical_volatility: tuple[AdvancedOptionsHistoricalVolatilityFact, ...]
    valuation_inputs: tuple[AdvancedOptionsValuationReference, ...]
    equity_metrics: tuple[AdvancedOptionsEquityMetric, ...]

    def __post_init__(self) -> None:
        known = _utc(self.known_at, "known_at")
        valid = _utc(self.valid_until, "valid_until")
        underlyings = tuple(sorted({_ticker(value) for value in self.underlyings}))
        oi = tuple(sorted(self.open_interest, key=lambda row: row.contract_id))
        volatility = tuple(sorted(
            self.historical_volatility,
            key=lambda row: (row.ticker, row.window_sessions),
        ))
        valuations = tuple(sorted(
            self.valuation_inputs,
            key=lambda row: (row.ticker, row.expiration_date),
        ))
        metrics = tuple(sorted(
            self.equity_metrics,
            key=lambda row: (row.ticker, row.metric_kind.value),
        ))
        if (
            self.version != REFERENCE_STATE_VERSION
            or type(self.reference_generation) is not int
            or self.reference_generation <= 0
            or not isinstance(self.session_date, date)
            or known >= valid
            or not underlyings
            or not oi
            or not volatility
            or not valuations
            or not metrics
            or len({row.contract_id for row in oi}) != len(oi)
            or any(
                row.underlyer not in underlyings
                or row.known_at > known
                or (
                    row.availability is ReferenceAvailability.AVAILABLE
                    and row.observed_session != self.session_date
                )
                for row in oi
            )
            or any(
                row.ticker not in underlyings
                or row.known_at > known
                or row.bars[-1].session_date >= self.session_date
                for row in volatility
            )
            or any(
                row.ticker not in underlyings
                or row.known_at > known
                or row.valuation_date != self.session_date
                for row in valuations
            )
            or any(
                row.ticker not in underlyings
                or row.known_at > known
                or row.as_of_session >= self.session_date
                for row in metrics
            )
        ):
            raise ValueError("Advanced Options reference state is incomplete")
        for underlyer in underlyings:
            if {
                row.window_sessions
                for row in volatility
                if row.ticker == underlyer
            } != {20, 30}:
                raise ValueError("HV20/HV30 reference membership is incomplete")
            if not any(row.ticker == underlyer for row in valuations):
                raise ValueError("valuation reference membership is incomplete")
            if {
                row.metric_kind
                for row in metrics
                if row.ticker == underlyer
            } != set(EquityMetricKind):
                raise ValueError("causal equity metric membership is incomplete")
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(
            self,
            "reference_policy_sha256",
            _sha256(self.reference_policy_sha256, "reference_policy_sha256"),
        )
        object.__setattr__(
            self,
            "contract_scope_sha256",
            _sha256(self.contract_scope_sha256, "contract_scope_sha256"),
        )
        object.__setattr__(self, "underlyings", underlyings)
        object.__setattr__(self, "open_interest", oi)
        object.__setattr__(self, "historical_volatility", volatility)
        object.__setattr__(self, "valuation_inputs", valuations)
        object.__setattr__(self, "equity_metrics", metrics)
        expected_sha256 = canonical_sha256(self.identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-reference-state:{expected_sha256}",
        )
        if self.state_sha256 != expected_sha256 or self.state_id != expected_id:
            raise ValueError("reference state ID or SHA does not match its identity")

    @property
    def identity(self) -> dict[str, object]:
        return {
            "contract": REFERENCE_STATE_VERSION,
            "reference_generation": self.reference_generation,
            "session_date": self.session_date,
            "known_at": self.known_at,
            "valid_until": self.valid_until,
            "reference_policy_sha256": self.reference_policy_sha256,
            "contract_scope_sha256": self.contract_scope_sha256,
            "underlyings": self.underlyings,
            "open_interest": self.open_interest,
            "historical_volatility": self.historical_volatility,
            "valuation_inputs": self.valuation_inputs,
            "equity_metrics": self.equity_metrics,
        }

    @property
    def member_identities(self) -> tuple[tuple[str, str, str], ...]:
        members = [
            ("OPEN_INTEREST", str(row.contract_id), row.sha256)
            for row in self.open_interest
        ]
        members.extend(
            (
                "HISTORICAL_VOLATILITY",
                f"{row.ticker}:HV{row.window_sessions}",
                row.sha256,
            )
            for row in self.historical_volatility
        )
        members.extend(
            (
                "VALUATION_INPUT",
                f"{row.ticker}:{row.expiration_date.isoformat()}",
                row.sha256,
            )
            for row in self.valuation_inputs
        )
        members.extend(
            (
                "EQUITY_METRIC",
                f"{row.ticker}:{row.metric_kind.value}",
                row.sha256,
            )
            for row in self.equity_metrics
        )
        return tuple(sorted(members))

    @property
    def open_interest_sha256(self) -> str:
        return canonical_sha256(tuple(row.sha256 for row in self.open_interest))


def open_interest_reference_from_fact(
    fact: ReferenceFact,
    *,
    contract_ticker: str,
    underlyer: str,
    known_at: datetime,
) -> AdvancedOptionsOpenInterestReference:
    if (
        not isinstance(fact, ReferenceFact)
        or fact.relation is not ReferenceFactRelation.OI_BASELINE
    ):
        raise ValueError("OI reference requires a ref.oi_baselines fact")
    values = dict(fact.values)
    return AdvancedOptionsOpenInterestReference(
        contract_id=int(values["contract_id"]),
        contract_ticker=contract_ticker,
        underlyer=underlyer,
        availability=ReferenceAvailability.AVAILABLE,
        known_at=known_at,
        baseline_id=UUID(str(values["baseline_id"])),
        settlement_session=values["settlement_session"],
        observed_session=values["observed_session"],
        open_interest=values["open_interest"],
        source=values["source"],
        date_basis=values["date_basis"],
        observed_at=values["observed_at"],
        received_at=values["received_at"],
        payload_sha256=values["payload_sha256"],
        supersedes_baseline_id=(
            None
            if values["supersedes_baseline_id"] is None
            else UUID(str(values["supersedes_baseline_id"]))
        ),
    )


def rate_reference_from_observation(
    observation: RiskFreeRateObservation,
) -> AdvancedOptionsRateReference:
    if (
        not isinstance(observation, RiskFreeRateObservation)
        or observation.source_observed_at is None
        or observation.availability_mode != "LIVE_OBSERVED"
        or observation.replay_available_at is not None
    ):
        raise ValueError("risk-free-rate observation provenance is unavailable")
    return AdvancedOptionsRateReference(
        observation_id=observation.rate_observation_id,
        rate_date=observation.rate_date,
        tenor_days=observation.tenor_days,
        annual_rate=Decimal(repr(observation.annual_rate)),
        source=observation.source,
        source_key=observation.source_key,
        observed_at=observation.source_observed_at,
        received_at=observation.first_observed_at,
        payload_sha256=observation.payload_sha256,
    )


def dividend_coverage_reference_from_window(
    window: DividendReferenceWindow,
) -> AdvancedOptionsDividendCoverageReference:
    if (
        not isinstance(window, DividendReferenceWindow)
        or window.status not in {"AVAILABLE", "KNOWN_NO_ANNOUNCED_DIVIDEND"}
        or window.coverage_id is None
        or window.coverage_observed_at is None
        or window.checked_at is None
        or window.coverage_payload_sha256 is None
        or window.response_sha256 is None
    ):
        raise ValueError("dividend coverage is unavailable")
    actions = tuple(
        AdvancedOptionsCorporateActionIdentity(
            action_id=row.action_id,
            payload_sha256=row.payload_sha256,
            observed_at=row.observed_at,
            received_at=row.received_at,
        )
        for row in window.dividends
    )
    return AdvancedOptionsDividendCoverageReference(
        coverage_id=window.coverage_id,
        security_id=window.security_id,
        ticker=window.ticker,
        window_start=window.window_start,
        window_end=window.window_end,
        status=window.status,
        observed_at=window.coverage_observed_at,
        received_at=window.checked_at,
        payload_sha256=window.coverage_payload_sha256,
        response_sha256=window.response_sha256,
        actions=actions,
    )


def valuation_reference_from_candidate(
    candidate: ExpirationValuationCandidate,
    *,
    security_id: UUID,
    ticker: str,
    curve: tuple[RiskFreeRateObservation, ...],
    dividend_coverage: AdvancedOptionsDividendCoverageReference,
    model_version: str,
    model_bundle_sha256: str,
    valuation_policy_sha256: str,
) -> AdvancedOptionsValuationReference:
    if not isinstance(candidate, ExpirationValuationCandidate):
        raise TypeError("valuation candidate is invalid")
    rates = tuple(rate_reference_from_observation(row) for row in curve)
    if (
        tuple(row.observation_id for row in rates)
        != candidate.rate_observation_ids
        or tuple(row.payload_sha256 for row in rates)
        != candidate.rate_payload_sha256
        or dividend_coverage.payload_sha256
        != candidate.dividend_payload_sha256
    ):
        raise ValueError("valuation candidate provenance changed")
    return AdvancedOptionsValuationReference(
        security_id=security_id,
        ticker=ticker,
        valuation_date=candidate.valuation_date,
        expiration_date=candidate.expiration_date,
        risk_free_rate=Decimal(repr(candidate.risk_free_rate)),
        dividend_yield=Decimal(repr(candidate.dividend_yield)),
        rate_observations=rates,
        dividend_coverage=dividend_coverage,
        model_version=model_version,
        model_bundle_sha256=model_bundle_sha256,
        valuation_policy_sha256=valuation_policy_sha256,
        known_at=candidate.selected_at,
    )


def build_advanced_options_reference_state(
    *,
    reference_generation: int,
    session_date: date,
    known_at: datetime,
    valid_until: datetime,
    reference_policy_sha256: str,
    contract_scope_sha256: str,
    underlyings: tuple[str, ...],
    open_interest: tuple[AdvancedOptionsOpenInterestReference, ...],
    historical_volatility: tuple[AdvancedOptionsHistoricalVolatilityFact, ...],
    valuation_inputs: tuple[AdvancedOptionsValuationReference, ...],
    equity_metrics: tuple[AdvancedOptionsEquityMetric, ...],
) -> AdvancedOptionsReferenceState:
    normalized_underlyings = tuple(sorted({_ticker(value) for value in underlyings}))
    normalized_oi = tuple(sorted(open_interest, key=lambda row: row.contract_id))
    normalized_volatility = tuple(sorted(
        historical_volatility,
        key=lambda row: (row.ticker, row.window_sessions),
    ))
    normalized_valuations = tuple(sorted(
        valuation_inputs,
        key=lambda row: (row.ticker, row.expiration_date),
    ))
    normalized_metrics = tuple(sorted(
        equity_metrics,
        key=lambda row: (row.ticker, row.metric_kind.value),
    ))
    identity = {
        "contract": REFERENCE_STATE_VERSION,
        "reference_generation": reference_generation,
        "session_date": session_date,
        "known_at": known_at,
        "valid_until": valid_until,
        "reference_policy_sha256": reference_policy_sha256,
        "contract_scope_sha256": contract_scope_sha256,
        "underlyings": normalized_underlyings,
        "open_interest": normalized_oi,
        "historical_volatility": normalized_volatility,
        "valuation_inputs": normalized_valuations,
        "equity_metrics": normalized_metrics,
    }
    state_sha256 = canonical_sha256(identity)
    return AdvancedOptionsReferenceState(
        state_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-reference-state:{state_sha256}",
        ),
        state_sha256=state_sha256,
        version=REFERENCE_STATE_VERSION,
        reference_generation=reference_generation,
        session_date=session_date,
        known_at=known_at,
        valid_until=valid_until,
        reference_policy_sha256=reference_policy_sha256,
        contract_scope_sha256=contract_scope_sha256,
        underlyings=normalized_underlyings,
        open_interest=normalized_oi,
        historical_volatility=normalized_volatility,
        valuation_inputs=normalized_valuations,
        equity_metrics=normalized_metrics,
    )


class AdvancedOptionsReferenceStateRepository(Protocol):
    def put(
        self,
        state: AdvancedOptionsReferenceState,
    ) -> AdvancedOptionsReferenceState:
        ...

    def load_generation(
        self,
        reference_generation: int,
    ) -> AdvancedOptionsReferenceState | None:
        ...

    def load_for_session(
        self,
        session_date: date,
        *,
        known_at: datetime,
    ) -> AdvancedOptionsReferenceState | None:
        ...

    def freeze_for_cycle(
        self,
        state: AdvancedOptionsReferenceState,
        *,
        cycle_id: UUID,
        consumed_at: datetime,
    ) -> None:
        ...


class InMemoryAdvancedOptionsReferenceStateRepository:
    """Source-only adapter that enforces immutable generations and session freezes."""

    def __init__(self) -> None:
        self._states: dict[int, AdvancedOptionsReferenceState] = {}
        self._session_state: dict[date, UUID] = {}
        self._cycle_state: dict[UUID, UUID] = {}

    def put(
        self,
        state: AdvancedOptionsReferenceState,
    ) -> AdvancedOptionsReferenceState:
        if not isinstance(state, AdvancedOptionsReferenceState):
            raise TypeError("reference state is invalid")
        existing = self._states.get(state.reference_generation)
        if existing is not None:
            if existing != state:
                raise ValueError("reference generation is immutable")
            return existing
        if self._states and state.reference_generation <= max(self._states):
            raise ValueError("new reference generation must advance")
        self._states[state.reference_generation] = state
        return state

    def load_generation(
        self,
        reference_generation: int,
    ) -> AdvancedOptionsReferenceState | None:
        return self._states.get(reference_generation)

    def load_for_session(
        self,
        session_date: date,
        *,
        known_at: datetime,
    ) -> AdvancedOptionsReferenceState | None:
        cutoff = _utc(known_at, "known_at")
        frozen_id = self._session_state.get(session_date)
        candidates = tuple(
            state
            for state in self._states.values()
            if state.session_date == session_date
            and state.known_at <= cutoff < state.valid_until
            and (frozen_id is None or state.state_id == frozen_id)
        )
        return (
            None
            if not candidates
            else max(candidates, key=lambda state: state.reference_generation)
        )

    def freeze_for_cycle(
        self,
        state: AdvancedOptionsReferenceState,
        *,
        cycle_id: UUID,
        consumed_at: datetime,
    ) -> None:
        consumed = _utc(consumed_at, "consumed_at")
        if (
            not isinstance(cycle_id, UUID)
            or self._states.get(state.reference_generation) != state
            or not state.known_at <= consumed < state.valid_until
        ):
            raise ValueError("reference state cannot be consumed")
        frozen = self._session_state.get(state.session_date)
        if frozen is not None and frozen != state.state_id:
            raise ValueError("consumed session reference state is frozen")
        cycle_state = self._cycle_state.get(cycle_id)
        if cycle_state is not None and cycle_state != state.state_id:
            raise ValueError("cycle reference state is immutable")
        self._session_state[state.session_date] = state.state_id
        self._cycle_state[cycle_id] = state.state_id
