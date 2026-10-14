from __future__ import annotations

from bisect import bisect_left
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from options.data.trade_classification import (
    TRADE_SEMANTICS_VERSION,
    classify_option_trade,
)

from .contracts import AssetClass, MarketChannel, canonical_sha256
from .journal import NormalizedJournalRecord


MATERIALIZER_STATE_CODEC = "realtime-materializer-shard-json-v2"
MAX_CHECKPOINT_STATE_BYTES = 1_048_576
MAX_QUOTE_HISTORY_PER_INSTRUMENT = 8


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _stamp(value: datetime) -> str:
    return _utc(value, "checkpoint datetime").isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class MaterializerShardMap:
    version: str
    shard_count: int
    assignments: tuple[tuple[int, int], ...]

    def __post_init__(self) -> None:
        version = self.version.strip()
        if not version:
            raise ValueError("shard map version cannot be blank")
        if type(self.shard_count) is not int or self.shard_count <= 0:
            raise ValueError("shard count must be a positive integer")
        ordered = tuple(sorted(self.assignments))
        instrument_ids = tuple(instrument_id for instrument_id, _ in ordered)
        if not ordered or any(type(value) is not int for row in ordered for value in row):
            raise ValueError("shard map requires integer assignments")
        if any(instrument_id <= 0 for instrument_id in instrument_ids):
            raise ValueError("shard map instrument IDs must be positive")
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("shard map instrument IDs must be unique")
        if any(not 0 <= shard_id < self.shard_count for _, shard_id in ordered):
            raise ValueError("shard assignment is outside the configured shard count")
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "assignments", ordered)

    @property
    def sha256(self) -> str:
        return canonical_sha256({
            "version": self.version,
            "shard_count": self.shard_count,
            "assignments": self.assignments,
        })

    def shard_for(self, instrument_id: int) -> int:
        index = bisect_left(self.assignments, (instrument_id, -1))
        if index < len(self.assignments):
            candidate, shard_id = self.assignments[index]
            if candidate == instrument_id:
                return shard_id
        raise ValueError(f"instrument {instrument_id} is absent from the shard map")


@dataclass(frozen=True, slots=True)
class MaterializedQuote:
    journal_generation: UUID
    stream_session_id: UUID
    instrument_id: int
    provider_symbol: str
    source_session: date
    source_sequence: int | None
    source_timestamp: int
    source_timestamp_unit: str
    event_at: datetime
    received_at: datetime
    bid: Decimal | None
    ask: Decimal | None
    bid_size: int | None
    ask_size: int | None
    bid_exchange: int | None
    ask_exchange: int | None
    quality: str
    reason_codes: tuple[str, ...]
    source_offset: int
    payload_sha256: str

    def __post_init__(self) -> None:
        symbol = self.provider_symbol.strip().upper()
        unit = self.source_timestamp_unit.strip()
        quality = self.quality.strip().upper()
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("quote instrument ID must be positive")
        if not symbol or not unit or not quality:
            raise ValueError("quote symbol, timestamp unit and quality cannot be blank")
        if self.source_sequence is not None and type(self.source_sequence) is not int:
            raise ValueError("quote source sequence must be an integer when present")
        if type(self.source_timestamp) is not int or self.source_timestamp < 0:
            raise ValueError("quote source timestamp must be nonnegative")
        if type(self.source_offset) is not int or self.source_offset <= 0:
            raise ValueError("quote source offset must be positive")
        for value, name in ((self.bid, "bid"), (self.ask, "ask")):
            if value is not None and not value.is_finite():
                raise ValueError(f"quote {name} must be finite")
        for value, name in ((self.bid_size, "bid_size"), (self.ask_size, "ask_size")):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"quote {name} must be a nonnegative integer")
        for value, name in (
            (self.bid_exchange, "bid_exchange"),
            (self.ask_exchange, "ask_exchange"),
        ):
            if value is not None and type(value) is not int:
                raise ValueError(f"quote {name} must be an integer")
        reasons = tuple(reason.strip() for reason in self.reason_codes)
        if any(not reason for reason in reasons):
            raise ValueError("quote reason codes cannot be blank")
        object.__setattr__(self, "provider_symbol", symbol)
        object.__setattr__(self, "source_timestamp_unit", unit)
        object.__setattr__(self, "event_at", _utc(self.event_at, "event_at"))
        object.__setattr__(self, "received_at", _utc(self.received_at, "received_at"))
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "payload_sha256", _sha256(self.payload_sha256, "payload_sha256"))


@dataclass(frozen=True, slots=True)
class MaterializedTrade:
    journal_generation: UUID
    stream_session_id: UUID
    instrument_id: int
    provider_symbol: str
    asset_class: AssetClass
    source_session: date
    source_sequence: int | None
    source_timestamp: int
    source_timestamp_unit: str
    event_at: datetime
    received_at: datetime
    price: Decimal
    size: Decimal
    exchange: int
    conditions: tuple[int, ...]
    quality: str
    reason_codes: tuple[str, ...]
    eligibility: str
    eligibility_reason_codes: tuple[str, ...]
    semantics_version: str
    source_offset: int
    payload_sha256: str

    def __post_init__(self) -> None:
        symbol = self.provider_symbol.strip().upper()
        unit = self.source_timestamp_unit.strip()
        quality = self.quality.strip().upper()
        eligibility = self.eligibility.strip().upper()
        semantics = self.semantics_version.strip()
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("trade instrument ID must be positive")
        if not symbol or not unit or not quality or not eligibility or not semantics:
            raise ValueError("trade identity fields cannot be blank")
        if self.source_sequence is not None and type(self.source_sequence) is not int:
            raise ValueError("trade source sequence must be an integer when present")
        if type(self.source_timestamp) is not int or self.source_timestamp < 0:
            raise ValueError("trade source timestamp must be nonnegative")
        if type(self.source_offset) is not int or self.source_offset <= 0:
            raise ValueError("trade source offset must be positive")
        if not self.price.is_finite() or not self.size.is_finite():
            raise ValueError("trade price and size must be finite")
        if type(self.exchange) is not int:
            raise ValueError("trade exchange must be an integer")
        if any(type(condition) is not int for condition in self.conditions):
            raise ValueError("trade conditions must be integers")
        reasons = tuple(reason.strip() for reason in self.reason_codes)
        eligibility_reasons = tuple(
            reason.strip() for reason in self.eligibility_reason_codes
        )
        if any(not reason for reason in (*reasons, *eligibility_reasons)):
            raise ValueError("trade reason codes cannot be blank")
        object.__setattr__(self, "provider_symbol", symbol)
        object.__setattr__(self, "source_timestamp_unit", unit)
        object.__setattr__(self, "event_at", _utc(self.event_at, "event_at"))
        object.__setattr__(self, "received_at", _utc(self.received_at, "received_at"))
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "eligibility", eligibility)
        object.__setattr__(self, "eligibility_reason_codes", eligibility_reasons)
        object.__setattr__(self, "semantics_version", semantics)
        object.__setattr__(self, "payload_sha256", _sha256(self.payload_sha256, "payload_sha256"))


class AggressorSide(str, Enum):
    ASK = "ASK"
    BID = "BID"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class TradeQuoteAssessment:
    trade_source_offset: int
    side: AggressorSide
    decision_at: datetime
    quote_source_offset: int | None
    quote_payload_sha256: str | None
    quote_age_ms: int | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.trade_source_offset <= 0:
            raise ValueError("assessment trade offset must be positive")
        decision = _utc(self.decision_at, "decision_at")
        reasons = tuple(reason.strip() for reason in self.reason_codes)
        if any(not reason for reason in reasons):
            raise ValueError("assessment reason codes cannot be blank")
        if self.side is AggressorSide.UNKNOWN and not reasons:
            raise ValueError("unknown aggressor side requires a reason")
        if self.side is not AggressorSide.UNKNOWN and reasons:
            raise ValueError("classified aggressor side cannot have reasons")
        if (self.quote_source_offset is None) != (self.quote_payload_sha256 is None):
            raise ValueError("assessment quote identity must be complete")
        if self.quote_payload_sha256 is not None:
            _sha256(self.quote_payload_sha256, "quote_payload_sha256")
        if self.quote_age_ms is not None and self.quote_age_ms < 0:
            raise ValueError("assessment quote age cannot be negative")
        object.__setattr__(self, "decision_at", decision)
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class MaterializerShardState:
    shard_id: int
    shard_map_sha256: str
    scanned_offset: int
    quotes: tuple[MaterializedQuote, ...] = ()
    trades: tuple[MaterializedTrade, ...] = ()
    quote_history: tuple[MaterializedQuote, ...] = ()

    def __post_init__(self) -> None:
        if type(self.shard_id) is not int or self.shard_id < 0:
            raise ValueError("state shard ID must be nonnegative")
        if type(self.scanned_offset) is not int or self.scanned_offset < 0:
            raise ValueError("state scanned offset must be nonnegative")
        quote_ids = tuple(quote.instrument_id for quote in self.quotes)
        if len(quote_ids) != len(set(quote_ids)):
            raise ValueError("state may contain one current quote per instrument")
        trade_ids = tuple(trade.instrument_id for trade in self.trades)
        if len(trade_ids) != len(set(trade_ids)):
            raise ValueError("state may contain one current trade per instrument")
        object.__setattr__(
            self,
            "shard_map_sha256",
            _sha256(self.shard_map_sha256, "shard_map_sha256"),
        )
        object.__setattr__(
            self,
            "quotes",
            tuple(sorted(self.quotes, key=lambda quote: quote.instrument_id)),
        )
        object.__setattr__(
            self,
            "trades",
            tuple(sorted(self.trades, key=lambda trade: trade.instrument_id)),
        )
        object.__setattr__(
            self,
            "quote_history",
            tuple(sorted(self.quote_history, key=lambda quote: quote.source_offset)),
        )


@dataclass(frozen=True, slots=True)
class MaterializerCheckpoint:
    checkpoint_id: UUID
    consumer_name: str
    cohort_id: UUID
    journal_generation: UUID
    shard_id: int
    owner_epoch: int
    committed_offset: int
    state_codec: str
    state_hash: str
    state_payload: bytes
    committed_at: datetime

    def __post_init__(self) -> None:
        consumer = self.consumer_name.strip()
        if not consumer:
            raise ValueError("checkpoint consumer name cannot be blank")
        if type(self.shard_id) is not int or self.shard_id < 0:
            raise ValueError("checkpoint shard ID must be nonnegative")
        if type(self.owner_epoch) is not int or self.owner_epoch <= 0:
            raise ValueError("checkpoint owner epoch must be positive")
        if type(self.committed_offset) is not int or self.committed_offset < 0:
            raise ValueError("checkpoint offset must be nonnegative")
        if self.state_codec != MATERIALIZER_STATE_CODEC:
            raise ValueError("checkpoint state codec is unsupported")
        if not isinstance(self.state_payload, bytes) or not self.state_payload:
            raise ValueError("checkpoint state payload must be nonempty bytes")
        object.__setattr__(self, "consumer_name", consumer)
        object.__setattr__(self, "state_hash", _sha256(self.state_hash, "state_hash"))
        object.__setattr__(self, "committed_at", _utc(self.committed_at, "committed_at"))


def initial_materializer_state(
    shard_map: MaterializerShardMap,
    shard_id: int,
) -> MaterializerShardState:
    if type(shard_id) is not int or not 0 <= shard_id < shard_map.shard_count:
        raise ValueError("initial state shard is outside the shard map")
    return MaterializerShardState(shard_id, shard_map.sha256, 0)


def _quote_from_record(record: NormalizedJournalRecord) -> MaterializedQuote:
    try:
        details = json.loads(record.details_json)
    except json.JSONDecodeError as exc:
        raise ValueError("normalized quote details are invalid JSON") from exc
    if not isinstance(details, dict):
        raise ValueError("normalized quote details must be an object")
    return MaterializedQuote(
        journal_generation=record.journal_generation,
        stream_session_id=record.stream_session_id,
        instrument_id=record.instrument_id,
        provider_symbol=record.provider_symbol,
        source_session=record.source_session,
        source_sequence=record.source_sequence,
        source_timestamp=record.source_timestamp,
        source_timestamp_unit=record.source_timestamp_unit,
        event_at=record.event_at,
        received_at=record.received_at,
        bid=record.bid,
        ask=record.ask,
        bid_size=record.bid_size,
        ask_size=record.ask_size,
        bid_exchange=details.get("bid_exchange"),
        ask_exchange=details.get("ask_exchange"),
        quality=record.quality,
        reason_codes=record.reason_codes,
        source_offset=record.event_offset,
        payload_sha256=record.payload_sha256,
    )


def _replace_quote(current: MaterializedQuote, candidate: MaterializedQuote) -> bool:
    if candidate.source_session != current.source_session:
        return candidate.source_session > current.source_session
    if (
        candidate.source_sequence is not None
        and current.source_sequence is not None
        and candidate.source_sequence != current.source_sequence
    ):
        return candidate.source_sequence > current.source_sequence
    if candidate.source_timestamp_unit != current.source_timestamp_unit:
        raise ValueError("quote source timestamp unit drifted within a session")
    if candidate.source_timestamp != current.source_timestamp:
        return candidate.source_timestamp > current.source_timestamp
    if candidate.payload_sha256 == current.payload_sha256:
        return False
    return candidate.source_offset > current.source_offset


def _trade_from_record(record: NormalizedJournalRecord) -> MaterializedTrade:
    if record.price is None or record.size is None or record.exchange is None:
        raise ValueError("normalized trade is missing price, size or exchange")
    if record.quality == "INVALID":
        eligibility = "INVALID"
        eligibility_reasons = record.reason_codes
        semantics_version = "invalid_trade_v1"
    elif record.asset_class is AssetClass.OPTION:
        classification = classify_option_trade(record.conditions, None)
        eligibility = classification.status.value
        eligibility_reasons = classification.reasons
        semantics_version = TRADE_SEMANTICS_VERSION
    else:
        eligibility = "UNKNOWN"
        eligibility_reasons = ("EQUITY_TRADE_CONDITION_POLICY_PENDING",)
        semantics_version = "equity_trade_condition_policy_pending_v1"
    return MaterializedTrade(
        journal_generation=record.journal_generation,
        stream_session_id=record.stream_session_id,
        instrument_id=record.instrument_id,
        provider_symbol=record.provider_symbol,
        asset_class=record.asset_class,
        source_session=record.source_session,
        source_sequence=record.source_sequence,
        source_timestamp=record.source_timestamp,
        source_timestamp_unit=record.source_timestamp_unit,
        event_at=record.event_at,
        received_at=record.received_at,
        price=record.price,
        size=record.size,
        exchange=record.exchange,
        conditions=record.conditions,
        quality=record.quality,
        reason_codes=record.reason_codes,
        eligibility=eligibility,
        eligibility_reason_codes=eligibility_reasons,
        semantics_version=semantics_version,
        source_offset=record.event_offset,
        payload_sha256=record.payload_sha256,
    )


def _replace_trade(current: MaterializedTrade, candidate: MaterializedTrade) -> bool:
    if candidate.source_session != current.source_session:
        return candidate.source_session > current.source_session
    if (
        candidate.source_sequence is not None
        and current.source_sequence is not None
        and candidate.source_sequence != current.source_sequence
    ):
        return candidate.source_sequence > current.source_sequence
    if candidate.source_timestamp_unit != current.source_timestamp_unit:
        raise ValueError("trade source timestamp unit drifted within a session")
    if candidate.source_timestamp != current.source_timestamp:
        return candidate.source_timestamp > current.source_timestamp
    if candidate.payload_sha256 == current.payload_sha256:
        return False
    return candidate.source_offset > current.source_offset


def _trim_quote_history(
    history: list[MaterializedQuote],
    maximum_per_instrument: int,
) -> list[MaterializedQuote]:
    by_instrument: dict[int, list[MaterializedQuote]] = {}
    for quote in history:
        by_instrument.setdefault(quote.instrument_id, []).append(quote)
    retained = []
    for quotes in by_instrument.values():
        retained.extend(
            sorted(quotes, key=lambda quote: quote.source_offset)[-maximum_per_instrument:]
        )
    return sorted(retained, key=lambda quote: quote.source_offset)


def assess_trade_at_nbbo(
    trade: MaterializedTrade,
    quote_history: tuple[MaterializedQuote, ...],
    *,
    decision_at: datetime,
    maximum_quote_age_ms: int = 1_000,
    maximum_reordering_ms: int = 50,
) -> TradeQuoteAssessment:
    decision = _utc(decision_at, "decision_at")
    if decision < trade.received_at:
        raise ValueError("trade decision cannot precede receipt")
    if maximum_quote_age_ms <= 0 or maximum_reordering_ms < 0:
        raise ValueError("trade quote timing limits are invalid")
    if trade.eligibility != "INCLUDED":
        return TradeQuoteAssessment(
            trade.source_offset,
            AggressorSide.UNKNOWN,
            decision,
            None,
            None,
            None,
            ("TRADE_NOT_ELIGIBLE",),
        )
    candidates = tuple(
        quote
        for quote in quote_history
        if quote.instrument_id == trade.instrument_id
        and quote.source_session == trade.source_session
        and quote.event_at <= trade.event_at
        and quote.received_at <= decision
        and (
            quote.received_at <= trade.received_at
            or (quote.received_at - trade.received_at).total_seconds() * 1000
            <= maximum_reordering_ms
        )
    )
    if not candidates:
        return TradeQuoteAssessment(
            trade.source_offset,
            AggressorSide.UNKNOWN,
            decision,
            None,
            None,
            None,
            ("CAUSAL_QUOTE_UNAVAILABLE",),
        )
    quote = max(
        candidates,
        key=lambda row: (row.event_at, row.received_at, row.source_offset),
    )
    age_ms = int((trade.event_at - quote.event_at).total_seconds() * 1000)
    quote_identity = (quote.source_offset, quote.payload_sha256, age_ms)
    if quote.event_at == trade.event_at:
        reason = "EQUAL_TIMESTAMP_ORDER_AMBIGUOUS"
    elif age_ms > maximum_quote_age_ms:
        reason = "CAUSAL_QUOTE_TOO_OLD"
    elif quote.quality != "GOOD":
        reason = f"CAUSAL_QUOTE_{quote.quality}"
    elif (
        quote.bid is None
        or quote.ask is None
        or quote.bid_size is None
        or quote.ask_size is None
        or quote.bid <= 0
        or quote.ask <= 0
        or quote.bid_size <= 0
        or quote.ask_size <= 0
    ):
        reason = "CAUSAL_QUOTE_UNUSABLE"
    else:
        side = (
            AggressorSide.ASK
            if trade.price >= quote.ask
            else AggressorSide.BID
            if trade.price <= quote.bid
            else AggressorSide.UNKNOWN
        )
        if side is not AggressorSide.UNKNOWN:
            return TradeQuoteAssessment(
                trade.source_offset,
                side,
                decision,
                *quote_identity,
                (),
            )
        reason = "TRADE_INSIDE_NBBO"
    return TradeQuoteAssessment(
        trade.source_offset,
        AggressorSide.UNKNOWN,
        decision,
        *quote_identity,
        (reason,),
    )


def _validate_materializer_state(
    state: MaterializerShardState,
    shard_map: MaterializerShardMap,
) -> None:
    if state.shard_map_sha256 != shard_map.sha256:
        raise ValueError("materializer state shard map does not match the active map")
    if not 0 <= state.shard_id < shard_map.shard_count:
        raise ValueError("materializer state shard is outside the active map")


def _materialize_partitioned_shard_batch(
    state: MaterializerShardState,
    records: tuple[NormalizedJournalRecord, ...],
    *,
    scanned_offset: int,
    maximum_quote_history_per_instrument: int,
) -> MaterializerShardState:
    quotes = {quote.instrument_id: quote for quote in state.quotes}
    trades = {trade.instrument_id: trade for trade in state.trades}
    quote_history = list(state.quote_history)
    for record in records:
        if record.event_kind is MarketChannel.QUOTE:
            candidate = _quote_from_record(record)
            quote_history.append(candidate)
            current = quotes.get(record.instrument_id)
            if current is None or _replace_quote(current, candidate):
                quotes[record.instrument_id] = candidate
        elif record.event_kind is MarketChannel.TRADE:
            candidate_trade = _trade_from_record(record)
            current_trade = trades.get(record.instrument_id)
            if current_trade is None or _replace_trade(current_trade, candidate_trade):
                trades[record.instrument_id] = candidate_trade
    return MaterializerShardState(
        shard_id=state.shard_id,
        shard_map_sha256=state.shard_map_sha256,
        scanned_offset=scanned_offset,
        quotes=tuple(quotes.values()),
        trades=tuple(trades.values()),
        quote_history=tuple(_trim_quote_history(
            quote_history,
            maximum_quote_history_per_instrument,
        )),
    )


def materialize_shard_batch(
    state: MaterializerShardState,
    shard_map: MaterializerShardMap,
    records: tuple[NormalizedJournalRecord, ...],
    *,
    maximum_quote_history_per_instrument: int = MAX_QUOTE_HISTORY_PER_INSTRUMENT,
) -> MaterializerShardState:
    _validate_materializer_state(state, shard_map)
    if not records:
        return state
    if not 1 <= maximum_quote_history_per_instrument <= 4_096:
        raise ValueError("quote history limit must be in [1, 4096]")
    expected_offset = state.scanned_offset + 1
    partition = []
    for record in records:
        if record.event_offset != expected_offset:
            raise ValueError("materializer replay must scan contiguous event offsets")
        expected_offset += 1
        assigned_shard = shard_map.shard_for(record.instrument_id)
        if record.shard_id != assigned_shard:
            raise ValueError("normalized event shard does not match the active shard map")
        if record.shard_id == state.shard_id:
            partition.append(record)
    return _materialize_partitioned_shard_batch(
        state,
        tuple(partition),
        scanned_offset=records[-1].event_offset,
        maximum_quote_history_per_instrument=maximum_quote_history_per_instrument,
    )


def dirty_materializer_instruments(
    previous: MaterializerShardState,
    current: MaterializerShardState,
) -> frozenset[int]:
    if (
        previous.shard_id != current.shard_id
        or previous.shard_map_sha256 != current.shard_map_sha256
        or current.scanned_offset < previous.scanned_offset
    ):
        raise ValueError("materializer state diff lineage is incompatible")
    previous_quotes = {row.instrument_id: row for row in previous.quotes}
    current_quotes = {row.instrument_id: row for row in current.quotes}
    previous_trades = {row.instrument_id: row for row in previous.trades}
    current_trades = {row.instrument_id: row for row in current.trades}
    identities = set(previous_quotes) | set(current_quotes) | set(previous_trades) | set(current_trades)
    return frozenset(
        instrument_id
        for instrument_id in identities
        if previous_quotes.get(instrument_id) != current_quotes.get(instrument_id)
        or previous_trades.get(instrument_id) != current_trades.get(instrument_id)
    )


def materialize_shard_fanout(
    states: tuple[MaterializerShardState, ...],
    shard_map: MaterializerShardMap,
    records: tuple[NormalizedJournalRecord, ...],
    *,
    maximum_quote_history_per_instrument: int = MAX_QUOTE_HISTORY_PER_INSTRUMENT,
) -> tuple[MaterializerShardState, ...]:
    if len(states) != shard_map.shard_count:
        raise ValueError("fan-out requires one state for every logical shard")
    if {state.shard_id for state in states} != set(range(shard_map.shard_count)):
        raise ValueError("fan-out shard state identities are incomplete")
    if len({state.scanned_offset for state in states}) != 1:
        raise ValueError("fan-out shard checkpoints must share one scanned offset")
    ordered = tuple(sorted(states, key=lambda state: state.shard_id))
    for state in ordered:
        _validate_materializer_state(state, shard_map)
    if not records:
        return ordered
    if not 1 <= maximum_quote_history_per_instrument <= 4_096:
        raise ValueError("quote history limit must be in [1, 4096]")
    expected_offset = ordered[0].scanned_offset + 1
    partitions: list[list[NormalizedJournalRecord]] = [
        [] for _ in range(shard_map.shard_count)
    ]
    for record in records:
        if record.event_offset != expected_offset:
            raise ValueError("materializer replay must scan contiguous event offsets")
        expected_offset += 1
        assigned_shard = shard_map.shard_for(record.instrument_id)
        if record.shard_id != assigned_shard:
            raise ValueError("normalized event shard does not match the active shard map")
        partitions[assigned_shard].append(record)
    scanned_offset = records[-1].event_offset
    return tuple(
        _materialize_partitioned_shard_batch(
            state,
            tuple(partitions[state.shard_id]),
            scanned_offset=scanned_offset,
            maximum_quote_history_per_instrument=maximum_quote_history_per_instrument,
        )
        for state in ordered
    )


def _quote_document(quote: MaterializedQuote) -> dict[str, object]:
    return {
        "journal_generation": str(quote.journal_generation),
        "stream_session_id": str(quote.stream_session_id),
        "instrument_id": quote.instrument_id,
        "provider_symbol": quote.provider_symbol,
        "source_session": quote.source_session.isoformat(),
        "source_sequence": quote.source_sequence,
        "source_timestamp": quote.source_timestamp,
        "source_timestamp_unit": quote.source_timestamp_unit,
        "event_at": _stamp(quote.event_at),
        "received_at": _stamp(quote.received_at),
        "bid": str(quote.bid) if quote.bid is not None else None,
        "ask": str(quote.ask) if quote.ask is not None else None,
        "bid_size": quote.bid_size,
        "ask_size": quote.ask_size,
        "bid_exchange": quote.bid_exchange,
        "ask_exchange": quote.ask_exchange,
        "quality": quote.quality,
        "reason_codes": list(quote.reason_codes),
        "source_offset": quote.source_offset,
        "payload_sha256": quote.payload_sha256,
    }


def _trade_document(trade: MaterializedTrade) -> dict[str, object]:
    return {
        "journal_generation": str(trade.journal_generation),
        "stream_session_id": str(trade.stream_session_id),
        "instrument_id": trade.instrument_id,
        "provider_symbol": trade.provider_symbol,
        "asset_class": trade.asset_class.value,
        "source_session": trade.source_session.isoformat(),
        "source_sequence": trade.source_sequence,
        "source_timestamp": trade.source_timestamp,
        "source_timestamp_unit": trade.source_timestamp_unit,
        "event_at": _stamp(trade.event_at),
        "received_at": _stamp(trade.received_at),
        "price": str(trade.price),
        "size": str(trade.size),
        "exchange": trade.exchange,
        "conditions": list(trade.conditions),
        "quality": trade.quality,
        "reason_codes": list(trade.reason_codes),
        "eligibility": trade.eligibility,
        "eligibility_reason_codes": list(trade.eligibility_reason_codes),
        "semantics_version": trade.semantics_version,
        "source_offset": trade.source_offset,
        "payload_sha256": trade.payload_sha256,
    }


def encode_materializer_state(
    state: MaterializerShardState,
    *,
    maximum_bytes: int = MAX_CHECKPOINT_STATE_BYTES,
) -> bytes:
    if maximum_bytes <= 0:
        raise ValueError("checkpoint byte limit must be positive")
    document = {
        "codec": MATERIALIZER_STATE_CODEC,
        "shard_id": state.shard_id,
        "shard_map_sha256": state.shard_map_sha256,
        "scanned_offset": state.scanned_offset,
        "quotes": [_quote_document(quote) for quote in state.quotes],
        "trades": [_trade_document(trade) for trade in state.trades],
        "quote_history": [_quote_document(quote) for quote in state.quote_history],
    }
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    if len(payload) > maximum_bytes:
        raise ValueError("materializer checkpoint exceeds its byte limit")
    return payload


def _parse_decimal(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"checkpoint {name} is invalid") from None
    if not parsed.is_finite():
        raise ValueError(f"checkpoint {name} is invalid")
    return parsed


def decode_materializer_state(
    payload: bytes,
    shard_map: MaterializerShardMap,
    *,
    expected_shard_id: int,
    maximum_bytes: int = MAX_CHECKPOINT_STATE_BYTES,
) -> MaterializerShardState:
    if not isinstance(payload, bytes) or not payload:
        raise ValueError("materializer checkpoint payload must be nonempty bytes")
    if len(payload) > maximum_bytes:
        raise ValueError("materializer checkpoint exceeds its byte limit")
    try:
        document = json.loads(payload.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("materializer checkpoint is not canonical JSON") from exc
    if not isinstance(document, dict) or set(document) != {
        "codec", "shard_id", "shard_map_sha256", "scanned_offset", "quotes",
        "trades", "quote_history",
    }:
        raise ValueError("materializer checkpoint has an invalid shape")
    if document["codec"] != MATERIALIZER_STATE_CODEC:
        raise ValueError("materializer checkpoint codec is unsupported")
    if document["shard_id"] != expected_shard_id:
        raise ValueError("materializer checkpoint shard identity drifted")
    if document["shard_map_sha256"] != shard_map.sha256:
        raise ValueError("materializer checkpoint shard map drifted")
    rows = document["quotes"]
    history_rows = document["quote_history"]
    trade_rows = document["trades"]
    if not isinstance(rows, list) or not isinstance(history_rows, list):
        raise ValueError("materializer checkpoint quotes must be arrays")
    if not isinstance(trade_rows, list):
        raise ValueError("materializer checkpoint trades must be an array")
    expected_fields = {
        "journal_generation", "stream_session_id",
        "instrument_id", "provider_symbol", "source_session", "source_sequence",
        "source_timestamp", "source_timestamp_unit", "event_at", "received_at",
        "bid", "ask", "bid_size", "ask_size", "quality", "reason_codes",
        "bid_exchange", "ask_exchange", "source_offset", "payload_sha256",
    }
    quotes = []
    quote_history = []
    for row, target in (
        *((row, quotes) for row in rows),
        *((row, quote_history) for row in history_rows),
    ):
        if not isinstance(row, dict) or set(row) != expected_fields:
            raise ValueError("materializer checkpoint quote has an invalid shape")
        quote = MaterializedQuote(
            journal_generation=UUID(row["journal_generation"]),
            stream_session_id=UUID(row["stream_session_id"]),
            instrument_id=row["instrument_id"],
            provider_symbol=row["provider_symbol"],
            source_session=date.fromisoformat(row["source_session"]),
            source_sequence=row["source_sequence"],
            source_timestamp=row["source_timestamp"],
            source_timestamp_unit=row["source_timestamp_unit"],
            event_at=datetime.fromisoformat(row["event_at"].replace("Z", "+00:00")),
            received_at=datetime.fromisoformat(row["received_at"].replace("Z", "+00:00")),
            bid=_parse_decimal(row["bid"], "bid"),
            ask=_parse_decimal(row["ask"], "ask"),
            bid_size=row["bid_size"],
            ask_size=row["ask_size"],
            bid_exchange=row["bid_exchange"],
            ask_exchange=row["ask_exchange"],
            quality=row["quality"],
            reason_codes=tuple(row["reason_codes"]),
            source_offset=row["source_offset"],
            payload_sha256=row["payload_sha256"],
        )
        if shard_map.shard_for(quote.instrument_id) != expected_shard_id:
            raise ValueError("checkpoint quote is assigned to a different shard")
        target.append(quote)
    trade_fields = {
        "journal_generation", "stream_session_id", "instrument_id", "provider_symbol",
        "asset_class",
        "source_session", "source_sequence", "source_timestamp", "source_timestamp_unit",
        "event_at", "received_at", "price", "size", "exchange", "conditions", "quality",
        "reason_codes", "eligibility", "eligibility_reason_codes", "semantics_version",
        "source_offset", "payload_sha256",
    }
    trades = []
    for row in trade_rows:
        if not isinstance(row, dict) or set(row) != trade_fields:
            raise ValueError("materializer checkpoint trade has an invalid shape")
        trade = MaterializedTrade(
            journal_generation=UUID(row["journal_generation"]),
            stream_session_id=UUID(row["stream_session_id"]),
            instrument_id=row["instrument_id"],
            provider_symbol=row["provider_symbol"],
            asset_class=AssetClass(row["asset_class"]),
            source_session=date.fromisoformat(row["source_session"]),
            source_sequence=row["source_sequence"],
            source_timestamp=row["source_timestamp"],
            source_timestamp_unit=row["source_timestamp_unit"],
            event_at=datetime.fromisoformat(row["event_at"].replace("Z", "+00:00")),
            received_at=datetime.fromisoformat(row["received_at"].replace("Z", "+00:00")),
            price=_parse_decimal(row["price"], "price"),
            size=_parse_decimal(row["size"], "size"),
            exchange=row["exchange"],
            conditions=tuple(row["conditions"]),
            quality=row["quality"],
            reason_codes=tuple(row["reason_codes"]),
            eligibility=row["eligibility"],
            eligibility_reason_codes=tuple(row["eligibility_reason_codes"]),
            semantics_version=row["semantics_version"],
            source_offset=row["source_offset"],
            payload_sha256=row["payload_sha256"],
        )
        if shard_map.shard_for(trade.instrument_id) != expected_shard_id:
            raise ValueError("checkpoint trade is assigned to a different shard")
        trades.append(trade)
    return MaterializerShardState(
        shard_id=document["shard_id"],
        shard_map_sha256=document["shard_map_sha256"],
        scanned_offset=document["scanned_offset"],
        quotes=tuple(quotes),
        trades=tuple(trades),
        quote_history=tuple(quote_history),
    )


def build_materializer_checkpoint(
    state: MaterializerShardState,
    *,
    consumer_name: str,
    cohort_id: UUID,
    journal_generation: UUID,
    owner_epoch: int,
    committed_at: datetime,
    maximum_bytes: int = MAX_CHECKPOINT_STATE_BYTES,
) -> MaterializerCheckpoint:
    payload = encode_materializer_state(state, maximum_bytes=maximum_bytes)
    state_hash = hashlib.sha256(payload).hexdigest()
    checkpoint_id = uuid5(
        NAMESPACE_URL,
        f"realtime-checkpoint:{consumer_name.strip()}:{cohort_id}:"
        f"{journal_generation}:{state.shard_id}:{state.scanned_offset}:{state_hash}",
    )
    return MaterializerCheckpoint(
        checkpoint_id=checkpoint_id,
        consumer_name=consumer_name,
        cohort_id=cohort_id,
        journal_generation=journal_generation,
        shard_id=state.shard_id,
        owner_epoch=owner_epoch,
        committed_offset=state.scanned_offset,
        state_codec=MATERIALIZER_STATE_CODEC,
        state_hash=state_hash,
        state_payload=payload,
        committed_at=committed_at,
    )


def restore_materializer_checkpoint(
    checkpoint: MaterializerCheckpoint,
    shard_map: MaterializerShardMap,
    *,
    maximum_bytes: int = MAX_CHECKPOINT_STATE_BYTES,
) -> MaterializerShardState:
    digest = hashlib.sha256(checkpoint.state_payload).hexdigest()
    if digest != checkpoint.state_hash:
        raise ValueError("materializer checkpoint payload hash does not match")
    state = decode_materializer_state(
        checkpoint.state_payload,
        shard_map,
        expected_shard_id=checkpoint.shard_id,
        maximum_bytes=maximum_bytes,
    )
    if state.scanned_offset != checkpoint.committed_offset:
        raise ValueError("checkpoint offset is ahead of its state payload")
    return state