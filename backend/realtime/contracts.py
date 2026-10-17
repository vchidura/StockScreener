from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Mapping
from uuid import UUID


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_EQUITY_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")


class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    OPTION = "OPTION"


class MarketChannel(str, Enum):
    TRADE = "T"
    QUOTE = "Q"
    MINUTE_AGGREGATE = "AM"


class MarketFeed(str, Enum):
    STOCKS = "STOCKS"
    OPTIONS = "OPTIONS"


class SubscriptionPurpose(str, Enum):
    PRIMARY = "PRIMARY"
    RETAINED_ALERT = "RETAINED_ALERT"
    POSITION = "POSITION"


class CapabilityVerdict(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNAVAILABLE = "UNAVAILABLE"
    EXPIRED = "EXPIRED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ReadinessStatus(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _non_empty(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} cannot be blank")
    return normalized


def _sha256(value: str, name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


_JSON_SCALARS = frozenset({str, int, float, bool, type(None)})


def _canonical_value(value: object) -> object:
    # Exact-type fast path: the generic branches below would return these unchanged (scalars) or
    # rebuild them the same way (dict, list, tuple), but isinstance() against typing.Mapping and
    # is_dataclass() dominated hashing time on multi-megabyte bodies. Output is byte-identical.
    kind = type(value)
    if kind in _JSON_SCALARS:
        return value
    if kind is dict:
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if kind is list or kind is tuple:
        return [_canonical_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _utc(value, "canonical datetime").isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical Decimal must be finite")
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonical_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json(value: object) -> str:
    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


def _equity_symbols(
    values: tuple[str, ...],
    name: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    normalized = tuple(sorted(value.strip().upper() for value in values))
    if not normalized and not allow_empty:
        raise ValueError(f"{name} cannot be empty")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{name} cannot contain duplicates")
    if any(not _EQUITY_SYMBOL_PATTERN.fullmatch(value) for value in normalized):
        raise ValueError(f"{name} contains an invalid symbol")
    return normalized


@dataclass(frozen=True, slots=True)
class UniverseManifest:
    version: str
    equity_tickers: tuple[str, ...]
    option_underlyings: tuple[str, ...]

    def __post_init__(self) -> None:
        version = _non_empty(self.version, "version")
        equities = _equity_symbols(self.equity_tickers, "equity_tickers")
        underlyings = _equity_symbols(
            self.option_underlyings,
            "option_underlyings",
            allow_empty=True,
        )
        if not set(underlyings).issubset(equities):
            raise ValueError("option underlyings must belong to the equity universe")
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "equity_tickers", equities)
        object.__setattr__(self, "option_underlyings", underlyings)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


CANARY_UNIVERSE = UniverseManifest(
    version="advanced-realtime-canary-v1",
    equity_tickers=("SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "TSLA"),
    option_underlyings=("SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "TSLA"),
)

EQUITY_VALIDATION_UNIVERSE = UniverseManifest(
    version="realtime-equity-validation-v1",
    equity_tickers=(
        "SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "NFLX"
    ),
    option_underlyings=(),
)

DEVELOPER_FORWARD_UNIVERSE = UniverseManifest(
    version="developer-forward-nine-v1",
    equity_tickers=EQUITY_VALIDATION_UNIVERSE.equity_tickers,
    option_underlyings=EQUITY_VALIDATION_UNIVERSE.equity_tickers,
)


@dataclass(frozen=True, slots=True)
class SubscriptionMember:
    asset_class: AssetClass
    provider_symbol: str
    underlying: str
    channel: MarketChannel
    priority: int = 100
    purpose: SubscriptionPurpose = SubscriptionPurpose.PRIMARY
    catalog_version_id: int | None = None

    def __post_init__(self) -> None:
        symbol = _non_empty(self.provider_symbol, "provider_symbol").upper()
        underlying = _non_empty(self.underlying, "underlying").upper()
        if "*" in symbol or "," in symbol:
            raise ValueError("subscription members must identify one explicit symbol")
        if not _EQUITY_SYMBOL_PATTERN.fullmatch(underlying):
            raise ValueError("underlying contains an invalid symbol")
        if type(self.priority) is not int or not 1 <= self.priority <= 32767:
            raise ValueError("priority must be an integer in [1, 32767]")
        if self.catalog_version_id is not None and (
            type(self.catalog_version_id) is not int or self.catalog_version_id <= 0
        ):
            raise ValueError("catalog_version_id must be a positive integer")
        if self.asset_class is AssetClass.EQUITY:
            if symbol != underlying:
                raise ValueError("equity provider symbol must equal its underlying")
            if self.channel not in {
                MarketChannel.TRADE,
                MarketChannel.QUOTE,
                MarketChannel.MINUTE_AGGREGATE,
            }:
                raise ValueError("equity channel is not supported")
            if self.catalog_version_id is not None:
                raise ValueError("equity membership cannot use an option catalog version")
        else:
            if not symbol.startswith("O:"):
                raise ValueError("option provider symbols must retain the O: prefix")
            if self.channel not in {MarketChannel.TRADE, MarketChannel.QUOTE}:
                raise ValueError("option channel is not supported")
            if self.catalog_version_id is None:
                raise ValueError("option membership requires a catalog version")
        object.__setattr__(self, "provider_symbol", symbol)
        object.__setattr__(self, "underlying", underlying)

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.asset_class.value, self.provider_symbol, self.channel.value


@dataclass(frozen=True, slots=True)
class SubscriptionManifest:
    version: str
    session_date: date
    universe: UniverseManifest
    capability_profile_sha256: str
    allocation_policy_sha256: str
    members: tuple[SubscriptionMember, ...]
    maximum_option_quote_contracts: int = 1000

    def __post_init__(self) -> None:
        version = _non_empty(self.version, "version")
        profile_hash = _sha256(
            self.capability_profile_sha256, "capability_profile_sha256"
        )
        allocation_hash = _sha256(
            self.allocation_policy_sha256, "allocation_policy_sha256"
        )
        if not self.members:
            raise ValueError("subscription manifest cannot be empty")
        option_budget_minimum = 0 if not self.universe.option_underlyings else 1
        if type(self.maximum_option_quote_contracts) is not int or not (
            option_budget_minimum <= self.maximum_option_quote_contracts <= 1000
        ):
            raise ValueError(
                "maximum option quote contracts must match the universe kind"
            )
        identities = tuple(member.identity for member in self.members)
        if len(identities) != len(set(identities)):
            raise ValueError("subscription membership cannot contain duplicates")
        self._validate_membership()
        option_quotes = {
            member.provider_symbol
            for member in self.members
            if member.asset_class is AssetClass.OPTION
            and member.channel is MarketChannel.QUOTE
        }
        if len(option_quotes) > self.maximum_option_quote_contracts:
            raise ValueError("option quote subscription exceeds the connection budget")
        option_trades = {
            member.provider_symbol
            for member in self.members
            if member.asset_class is AssetClass.OPTION
            and member.channel is MarketChannel.TRADE
        }
        if option_trades != option_quotes:
            raise ValueError("initial option trade and quote contract sets must match")
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "capability_profile_sha256", profile_hash)
        object.__setattr__(self, "allocation_policy_sha256", allocation_hash)

    def _validate_membership(self) -> None:
        stock_channels: dict[str, set[MarketChannel]] = {
            ticker: set() for ticker in self.universe.equity_tickers
        }
        option_underlyings: set[str] = set()
        for member in self.members:
            if member.asset_class is AssetClass.EQUITY:
                if member.underlying not in stock_channels:
                    raise ValueError("equity member is outside the universe")
                stock_channels[member.underlying].add(member.channel)
            else:
                if member.underlying not in self.universe.option_underlyings:
                    raise ValueError("option member is outside the option universe")
                option_underlyings.add(member.underlying)
        if any(not channels for channels in stock_channels.values()):
            raise ValueError("every equity requires at least one explicit channel")
        if option_underlyings != set(self.universe.option_underlyings):
            raise ValueError("every option underlying requires admitted contracts")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class RawMessageEnvelope:
    stream_session_id: UUID
    message_number: int
    feed: MarketFeed
    received_at: datetime
    received_monotonic_ns: int
    payload: bytes
    schema_version: int = 1

    def __post_init__(self) -> None:
        if type(self.message_number) is not int or self.message_number < 0:
            raise ValueError("message_number must be a nonnegative integer")
        if type(self.received_monotonic_ns) is not int or self.received_monotonic_ns < 0:
            raise ValueError("received_monotonic_ns must be a nonnegative integer")
        if type(self.payload) is not bytes or not self.payload:
            raise ValueError("payload must be nonempty immutable bytes")
        if type(self.schema_version) is not int or self.schema_version <= 0:
            raise ValueError("schema_version must be a positive integer")
        object.__setattr__(
            self, "received_at", _utc(self.received_at, "received_at")
        )

    @property
    def byte_length(self) -> int:
        return len(self.payload)


@dataclass(frozen=True, slots=True)
class CapabilityCheck:
    check_name: str
    scope_key: str
    verdict: CapabilityVerdict
    observed_at: datetime
    expires_at: datetime
    required: bool = True
    reason_codes: tuple[str, ...] = ()
    evidence_reference: str | None = None

    def __post_init__(self) -> None:
        check_name = _non_empty(self.check_name, "check_name")
        scope_key = _non_empty(self.scope_key, "scope_key")
        observed_at = _utc(self.observed_at, "observed_at")
        expires_at = _utc(self.expires_at, "expires_at")
        reasons = tuple(_non_empty(reason, "reason_code") for reason in self.reason_codes)
        if expires_at <= observed_at:
            raise ValueError("expires_at must follow observed_at")
        if self.verdict is CapabilityVerdict.PASS and reasons:
            raise ValueError("passing checks cannot carry failure reasons")
        if self.verdict is not CapabilityVerdict.PASS and not reasons:
            raise ValueError("non-passing checks require a reason code")
        if self.verdict is CapabilityVerdict.NOT_APPLICABLE and self.required:
            raise ValueError("required checks cannot be marked NOT_APPLICABLE")
        if self.evidence_reference is not None:
            object.__setattr__(
                self,
                "evidence_reference",
                _non_empty(self.evidence_reference, "evidence_reference"),
            )
        object.__setattr__(self, "check_name", check_name)
        object.__setattr__(self, "scope_key", scope_key)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "reason_codes", reasons)

    def effective_verdict(self, as_of: datetime) -> CapabilityVerdict:
        evaluated_at = _utc(as_of, "as_of")
        if (
            evaluated_at >= self.expires_at
            and self.verdict is not CapabilityVerdict.NOT_APPLICABLE
        ):
            return CapabilityVerdict.EXPIRED
        return self.verdict


@dataclass(frozen=True, slots=True)
class ReadinessAssessment:
    status: ReadinessStatus
    evaluated_at: datetime
    reason_codes: tuple[str, ...]
    expires_at: datetime


def assess_readiness(
    checks: tuple[CapabilityCheck, ...], as_of: datetime
) -> ReadinessAssessment:
    evaluated_at = _utc(as_of, "as_of")
    if not checks:
        raise ValueError("readiness requires at least one capability check")
    identities = tuple((check.check_name, check.scope_key) for check in checks)
    if len(identities) != len(set(identities)):
        raise ValueError("capability checks must have unique names and scopes")
    required_blockers: list[str] = []
    optional_failures: list[str] = []
    for check in checks:
        verdict = check.effective_verdict(evaluated_at)
        if verdict in {CapabilityVerdict.PASS, CapabilityVerdict.NOT_APPLICABLE}:
            continue
        reason = check.reason_codes[0] if verdict is check.verdict else "CHECK_EXPIRED"
        scoped_reason = f"{check.check_name}:{check.scope_key}:{reason}"
        if check.required:
            required_blockers.append(scoped_reason)
        else:
            optional_failures.append(scoped_reason)
    status = (
        ReadinessStatus.BLOCKED
        if required_blockers
        else ReadinessStatus.DEGRADED
        if optional_failures
        else ReadinessStatus.READY
    )
    return ReadinessAssessment(
        status=status,
        evaluated_at=evaluated_at,
        reason_codes=tuple(sorted(required_blockers + optional_failures)),
        expires_at=min(check.expires_at for check in checks),
    )