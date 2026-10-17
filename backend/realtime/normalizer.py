from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .contracts import AssetClass, MarketChannel, MarketFeed, canonical_sha256
from .journal import RawJournalRecord


NORMALIZER_VERSION = "massive_market_event_v2"
ET = ZoneInfo("America/New_York")


class MarketEventQuality(str, Enum):
    GOOD = "GOOD"
    ONE_SIDED = "ONE_SIDED"
    LOCKED = "LOCKED"
    CROSSED = "CROSSED"
    INVALID = "INVALID"
    UNCLASSIFIED = "UNCLASSIFIED"


@dataclass(frozen=True, slots=True)
class InstrumentAdmission:
    instrument_id: int
    provider_symbol: str
    underlying: str
    asset_class: AssetClass
    shard_id: int

    def __post_init__(self) -> None:
        symbol = self.provider_symbol.strip().upper()
        underlying = self.underlying.strip().upper()
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("instrument_id must be a positive integer")
        if not symbol or not underlying:
            raise ValueError("admission symbols cannot be blank")
        if type(self.shard_id) is not int or self.shard_id < 0:
            raise ValueError("shard_id must be a nonnegative integer")
        if self.asset_class is AssetClass.OPTION and not symbol.startswith("O:"):
            raise ValueError("option admission must retain the O: prefix")
        if self.asset_class is AssetClass.EQUITY and symbol != underlying:
            raise ValueError("equity admission symbol must equal its underlying")
        object.__setattr__(self, "provider_symbol", symbol)
        object.__setattr__(self, "underlying", underlying)

    @property
    def feed(self) -> MarketFeed:
        return (
            MarketFeed.STOCKS
            if self.asset_class is AssetClass.EQUITY
            else MarketFeed.OPTIONS
        )


@dataclass(frozen=True, slots=True)
class NormalizedMarketEvent:
    raw_offset: int
    event_index: int
    normalizer_version: str
    instrument_id: int
    provider_symbol: str
    underlying: str
    asset_class: AssetClass
    shard_id: int
    event_kind: MarketChannel
    source_session: date
    source_sequence: int | None
    source_timestamp: int
    source_timestamp_unit: str
    event_at: datetime
    price: Decimal | None
    size: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    bid_size: int | None
    ask_size: int | None
    exchange: int | None
    conditions: tuple[int, ...]
    quality: MarketEventQuality
    reason_codes: tuple[str, ...]
    details_json: str
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class QuarantinedMarketEvent:
    raw_offset: int
    event_index: int
    normalizer_version: str
    reason_code: str
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class NormalizationBatch:
    raw_offset: int
    normalizer_version: str
    events: tuple[NormalizedMarketEvent, ...]
    quarantines: tuple[QuarantinedMarketEvent, ...]


class _EventError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _payload_hash(value: object) -> str:
    return canonical_sha256(_json_value(value))


def _decimal(row: Mapping[str, Any], name: str, *, required: bool) -> Decimal | None:
    value = row.get(name)
    if value is None:
        if required:
            raise _EventError(f"MISSING_{name.upper()}")
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise _EventError(f"INVALID_{name.upper()}") from None
    if not parsed.is_finite():
        raise _EventError(f"INVALID_{name.upper()}")
    return parsed


def _integer(row: Mapping[str, Any], name: str, *, required: bool) -> int | None:
    value = row.get(name)
    if value is None:
        if required:
            raise _EventError(f"MISSING_{name.upper()}")
        return None
    if type(value) is not int:
        raise _EventError(f"INVALID_{name.upper()}")
    return value


def _conditions(row: Mapping[str, Any]) -> tuple[int, ...]:
    value = row.get("c", [])
    if type(value) is int:
        return (value,)
    if not isinstance(value, list) or any(type(item) is not int for item in value):
        raise _EventError("INVALID_CONDITIONS")
    return tuple(value)


def _event_time(timestamp: int) -> datetime:
    try:
        return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        raise _EventError("INVALID_SOURCE_TIMESTAMP") from None


def _quote_quality(
    bid: Decimal | None,
    ask: Decimal | None,
    bid_size: int | None,
    ask_size: int | None,
) -> tuple[MarketEventQuality, tuple[str, ...]]:
    if bid is None and ask is None:
        return MarketEventQuality.INVALID, ("EMPTY_QUOTE",)
    if bid is None or ask is None:
        return MarketEventQuality.ONE_SIDED, ("MISSING_QUOTE_SIDE",)
    if bid <= 0 or ask <= 0:
        return MarketEventQuality.INVALID, ("NON_POSITIVE_QUOTE_PRICE",)
    if bid_size is None or ask_size is None or bid_size <= 0 or ask_size <= 0:
        return MarketEventQuality.INVALID, ("MISSING_OR_NON_POSITIVE_QUOTE_SIZE",)
    if ask < bid:
        return MarketEventQuality.CROSSED, ("CROSSED_MARKET",)
    if ask == bid:
        return MarketEventQuality.LOCKED, ("LOCKED_MARKET",)
    return MarketEventQuality.GOOD, ()


def _normalize_event(
    raw: RawJournalRecord,
    event_index: int,
    row: Mapping[str, Any],
    admissions: Mapping[str, InstrumentAdmission],
) -> NormalizedMarketEvent:
    event_code = str(row.get("ev") or "").upper()
    try:
        event_kind = MarketChannel(event_code)
    except ValueError:
        raise _EventError("UNSUPPORTED_EVENT_TYPE") from None
    symbol = str(row.get("sym") or "").strip().upper()
    admission = admissions.get(symbol)
    if admission is None:
        raise _EventError("INSTRUMENT_NOT_ADMITTED")
    if admission.feed is not raw.feed:
        raise _EventError("INSTRUMENT_FEED_MISMATCH")
    if raw.feed is MarketFeed.OPTIONS and event_kind is MarketChannel.MINUTE_AGGREGATE:
        raise _EventError("CHANNEL_NOT_SUPPORTED_FOR_FEED")
    source_sequence = _integer(row, "q", required=False)
    price = size = bid = ask = None
    bid_size = ask_size = exchange = None
    conditions: tuple[int, ...] = ()
    details: dict[str, object] = {}
    if event_kind is MarketChannel.QUOTE:
        source_timestamp = _integer(row, "t", required=True)
        bid = _decimal(row, "bp", required=False)
        ask = _decimal(row, "ap", required=False)
        bid_size = _integer(row, "bs", required=False)
        ask_size = _integer(row, "as", required=False)
        quality, reasons = _quote_quality(bid, ask, bid_size, ask_size)
        details = {
            "ask_exchange": _integer(row, "ax", required=False),
            "bid_exchange": _integer(row, "bx", required=False),
            "participant_timestamp": _integer(row, "pt", required=False),
            "tape": _integer(row, "z", required=False),
        }
    elif event_kind is MarketChannel.TRADE:
        source_timestamp = _integer(row, "t", required=True)
        price = _decimal(row, "p", required=True)
        size = _decimal(row, "ds", required=False) or _decimal(
            row, "s", required=True
        )
        exchange = _integer(row, "x", required=True)
        conditions = _conditions(row)
        if price <= 0 or size <= 0:
            quality = MarketEventQuality.INVALID
            reasons = ("NON_POSITIVE_TRADE",)
        else:
            quality = MarketEventQuality.UNCLASSIFIED
            reasons = ("CONDITION_POLICY_PENDING",)
        details = {
            "participant_timestamp": _integer(row, "pt", required=False),
            "tape": _integer(row, "z", required=False),
            "trade_id": str(row.get("i")) if row.get("i") is not None else None,
            "trf_id": _integer(row, "trfi", required=False),
            "trf_timestamp": _integer(row, "trft", required=False),
        }
    else:
        start_timestamp = _integer(row, "s", required=True)
        source_timestamp = _integer(row, "e", required=False) or start_timestamp + 60_000
        open_price = _decimal(row, "o", required=True)
        high = _decimal(row, "h", required=True)
        low = _decimal(row, "l", required=True)
        close = _decimal(row, "c", required=True)
        integer_volume = _decimal(row, "v", required=False)
        fractional_volume = _decimal(row, "dv", required=False)
        volume = (
            fractional_volume
            if fractional_volume is not None
            else integer_volume
        )
        if volume is None:
            raise _EventError("MISSING_VOLUME")
        vwap = _decimal(row, "vw", required=False)
        if (
            min(open_price, high, low, close) <= 0
            or volume < 0
            or high < max(open_price, close)
            or low > min(open_price, close)
        ):
            quality = MarketEventQuality.INVALID
            reasons = ("INVALID_AGGREGATE_VALUES",)
        else:
            quality = MarketEventQuality.GOOD
            reasons = ()
        price = close
        size = volume
        details = {
            "close": str(close),
            "end_timestamp": source_timestamp,
            "high": str(high),
            "low": str(low),
            "open": str(open_price),
            "provider_fractional_volume": (
                str(fractional_volume)
                if fractional_volume is not None
                else None
            ),
            "provider_integer_volume": (
                str(integer_volume)
                if integer_volume is not None
                else None
            ),
            "start_timestamp": start_timestamp,
            "vwap": str(vwap) if vwap is not None else None,
        }
    event_at = _event_time(source_timestamp)
    if event_at > raw.received_at:
        quality = MarketEventQuality.INVALID
        reasons = tuple(dict.fromkeys((*reasons, "SOURCE_TIME_AFTER_RECEIPT")))
    return NormalizedMarketEvent(
        raw_offset=raw.raw_offset,
        event_index=event_index,
        normalizer_version=NORMALIZER_VERSION,
        instrument_id=admission.instrument_id,
        provider_symbol=admission.provider_symbol,
        underlying=admission.underlying,
        asset_class=admission.asset_class,
        shard_id=admission.shard_id,
        event_kind=event_kind,
        source_session=event_at.astimezone(ET).date(),
        source_sequence=source_sequence,
        source_timestamp=source_timestamp,
        source_timestamp_unit="ms",
        event_at=event_at,
        price=price,
        size=size,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        exchange=exchange,
        conditions=conditions,
        quality=quality,
        reason_codes=reasons,
        details_json=json.dumps(details, sort_keys=True, separators=(",", ":")),
        payload_sha256=_payload_hash(row),
    )


def normalize_raw_message(
    raw: RawJournalRecord,
    admissions: Mapping[str, InstrumentAdmission],
    *,
    maximum_events: int = 10_000,
) -> NormalizationBatch:
    if not 1 <= maximum_events <= 100_000:
        raise ValueError("maximum_events must be in [1, 100000]")
    normalized_admissions = {
        symbol.strip().upper(): admission for symbol, admission in admissions.items()
    }
    try:
        payload = json.loads(
            raw.payload,
            parse_float=Decimal,
            parse_int=int,
        )
    except (json.JSONDecodeError, UnicodeDecodeError):
        return NormalizationBatch(
            raw_offset=raw.raw_offset,
            normalizer_version=NORMALIZER_VERSION,
            events=(),
            quarantines=(
                QuarantinedMarketEvent(
                    raw.raw_offset,
                    -1,
                    NORMALIZER_VERSION,
                    "INVALID_JSON",
                    hashlib.sha256(raw.payload).hexdigest(),
                ),
            ),
        )
    if not isinstance(payload, list):
        return NormalizationBatch(
            raw_offset=raw.raw_offset,
            normalizer_version=NORMALIZER_VERSION,
            events=(),
            quarantines=(
                QuarantinedMarketEvent(
                    raw.raw_offset,
                    -1,
                    NORMALIZER_VERSION,
                    "INVALID_MESSAGE_SHAPE",
                    _payload_hash(payload),
                ),
            ),
        )
    if len(payload) > maximum_events:
        return NormalizationBatch(
            raw_offset=raw.raw_offset,
            normalizer_version=NORMALIZER_VERSION,
            events=(),
            quarantines=(
                QuarantinedMarketEvent(
                    raw.raw_offset,
                    -1,
                    NORMALIZER_VERSION,
                    "MESSAGE_EVENT_LIMIT_EXCEEDED",
                    _payload_hash(payload),
                ),
            ),
        )
    events: list[NormalizedMarketEvent] = []
    quarantines: list[QuarantinedMarketEvent] = []
    for event_index, row in enumerate(payload):
        if not isinstance(row, dict):
            quarantines.append(
                QuarantinedMarketEvent(
                    raw.raw_offset,
                    event_index,
                    NORMALIZER_VERSION,
                    "INVALID_EVENT_SHAPE",
                    _payload_hash(row),
                )
            )
            continue
        try:
            events.append(
                _normalize_event(raw, event_index, row, normalized_admissions)
            )
        except _EventError as error:
            quarantines.append(
                QuarantinedMarketEvent(
                    raw.raw_offset,
                    event_index,
                    NORMALIZER_VERSION,
                    error.reason_code,
                    _payload_hash(row),
                )
            )
    return NormalizationBatch(
        raw_offset=raw.raw_offset,
        normalizer_version=NORMALIZER_VERSION,
        events=tuple(events),
        quarantines=tuple(quarantines),
    )