"""Resumable engine state in rt.consumer_checkpoints.

A checkpoint is the engine state at a quiescent point (every queue drained, no pending gap) plus the
source offset it covers. It is written inside the writer's flush transaction, so the rows a flush
persists and the checkpoint that covers them commit or roll back together: persisted rows never
extend past the checkpoint offset. One row per engine shard (the table caps a payload at 1 MiB), all
written together with the same sequence number.

What is serialized is everything the next message can depend on: the volume accumulator per contract
and session including the retained print history (duplicate detection, cancel matching, reconcile)
and the events a cancel will supersede, the quote cache, the cluster state (open windows, members,
heaps, watermark), the shard clocks and offsets, the quote universe and the engine counters. The open
interest map is reference data and is reloaded, not serialized.

A checkpoint is trusted only when every check passes: codec, content hash, schema version, structural
layout of the engine classes, policy hash, runtime configuration hash, shard map, complete and
consistent shard set, and the restored state digest equals the captured one. Anything else raises
CheckpointRejected with a stable code and the caller starts WARMING; nothing is ever half restored
into a live engine (the caller builds a fresh engine for the fallback).
"""
from __future__ import annotations

import hashlib
import json
import time
import types
import typing
import zlib
from collections import OrderedDict
from dataclasses import dataclass, fields
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence
from uuid import UUID, uuid5

from psycopg2 import Binary

from ..contracts import canonical_sha256
from .clusters import ClusterEngine, _State as ClusterState
from .engine import DETECTOR_VERSION, EngineStats, OptionFlowEngine, _Shard
from .quote_cache import QuoteCache, QuoteView
from .records import FlowEvent
from .volume import BaselineKind, ContractVolume, PrintRecord, VolumeBook

CHECKPOINT_SCHEMA_VERSION = 1
CODEC = "option_flow_state_v1+json+zlib"
SHARD_SCHEME = "crc32_underlyer_v1"
CHECKPOINT_NAMESPACE = UUID("c3d1b7a4-52e0-4f6a-9b18-0a7e6d2f4c55")
# rt.consumer_checkpoints rejects a payload over 1 MiB; stay under it with margin.
DEFAULT_MAX_STATE_BYTES = 1_000_000
_MAX_DECOMPRESSED_BYTES = 128 * 1024 * 1024
_COMPRESSION_LEVEL = 3

# Every attribute of these state holders, either serialized or derived on restore. The layout hash is
# computed from the attribute names that actually exist, so adding state to any of these classes
# changes the hash (older checkpoints are rejected, never half restored) and fails the structure test
# until the new attribute is serialized here and listed below.
SERIALIZED_ATTRIBUTES: Mapping[str, frozenset[str]] = {
    "VolumeBook": frozenset({"_sessions", "_new_floor", "_dirty"}),
    "VolumeBook.derived": frozenset({"_cohort_id", "_config", "_cache_date", "_cache"}),
    "ContractVolume": frozenset({"contract", "session_date", "cumulative", "baseline", "reason", "max_seq",
                                 "last_time_ms", "snapshot_volume", "snapshot_watermark", "history", "order",
                                 "evicted_max_seq", "gap_end_ms"}),
    "PrintRecord": frozenset({"seq", "time_ms", "exchange", "price", "size", "counted", "covered", "active",
                              "is_cancel", "orig_seq", "orig_size", "event"}),
    "QuoteCache": frozenset({"_by_contract", "evicted_contracts", "out_of_order", "duplicates", "too_old",
                             "invalidations"}),
    "QuoteCache.derived": frozenset({"_config"}),
    "QuoteView": frozenset({"bid", "ask", "bid_size", "ask_size", "time_ms", "seq", "bid_exchange",
                            "ask_exchange"}),
    "ClusterEngine": frozenset({"_states", "_open", "_purge", "_max_time_ms", "_watermark_ms",
                                "late_beyond_retention", "late_revisions", "finalized"}),
    "ClusterEngine.derived": frozenset({"_config", "_cohort_id", "_provider_tier", "_policy_sha256", "_clock"}),
    "ClusterState": frozenset({"cluster_id", "contract", "underlyer", "side", "window_seconds", "start_ms",
                               "end_ms", "session_date", "members", "revision", "emitted", "final",
                               "retracted", "span_ms"}),
    "Shard": frozenset({"last_session", "last_time_ms", "next_checkpoint_ms", "last_offset", "reported_offset"}),
    "Shard.derived": frozenset({"index", "queue", "pending", "book", "quotes", "clusters"}),
}


class CheckpointRejected(Exception):
    """A stored checkpoint cannot be trusted. `code` is stable and goes into readiness reasons;
    `offset` is the covered offset the rows claim, when it could be read."""

    def __init__(self, code: str, detail: str = "", *, offset: int | None = None) -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail
        self.offset = offset


class CheckpointTooLarge(RuntimeError):
    """A shard's serialized state exceeds the size bound; nothing was written."""

    def __init__(self, shard: int, size: int, limit: int) -> None:
        super().__init__(f"option flow state of shard {shard} is {size} bytes, over the {limit} byte bound")
        self.shard = shard
        self.size = size
        self.limit = limit


class CheckpointRegression(RuntimeError):
    """The stored checkpoint covers a later offset than the one being written."""


class EngineNotQuiescent(RuntimeError):
    """State can only be captured with every queue drained and no pending gap."""


@dataclass(frozen=True, slots=True)
class CheckpointRow:
    shard_id: int
    committed_offset: int
    shard_map_sha256: str
    owner_epoch: int
    state_codec: str
    state_hash: str
    state_payload: bytes


@dataclass(frozen=True, slots=True)
class RestoredCheckpoint:
    """`offset` is the offset the stored STATE covers (resume reading after it); `durable_offset` is the
    offset the persisted rows cover (at least `offset`: between full checkpoints a flush only advances the
    row offset). Messages in (offset, durable_offset] are replayed to rebuild state, and their rows
    already exist, so the replay must reproduce them exactly."""

    offset: int
    sequence: int
    extras: Mapping[str, Any]
    payload_bytes: int
    durable_offset: int = 0

    def __post_init__(self) -> None:
        if self.durable_offset < self.offset:
            object.__setattr__(self, "durable_offset", self.offset)


@dataclass(frozen=True, slots=True)
class CheckpointWrite:
    offset: int
    sequence: int
    payload_bytes: int
    uncompressed_bytes: int
    seconds: float
    largest_row_bytes: int = 0


def shard_map_sha256(shard_count: int) -> str:
    return canonical_sha256({"scheme": SHARD_SCHEME, "shard_count": shard_count})


def runtime_config_sha256(engine: OptionFlowEngine) -> str:
    """The runtime knobs the policy hash does not cover but the serialized state depends on."""
    config = engine.config
    return canonical_sha256({
        "shard_count": config.shard_count,
        "provider_tier": config.provider_tier,
        "history_per_contract": config.volume.history_per_contract,
        "initial_baseline": config.volume.initial_baseline.value,
        "quote_cache": [config.quote_cache.max_quotes_per_contract, config.quote_cache.max_contracts],
        "checkpoint_interval_ms": config.checkpoint_interval_ms,
        "journal_generation": config.journal_generation,
        "cohort_id": config.cohort_id,
    })


def layout_sha256(engine: OptionFlowEngine) -> str:
    """Hash of the structure of the state holders; see SERIALIZED_ATTRIBUTES."""
    shard = engine._shards[0]
    return canonical_sha256({
        "schema": CHECKPOINT_SCHEMA_VERSION,
        "detector_version": DETECTOR_VERSION,
        "flow_event": [field.name for field in fields(FlowEvent)],
        "engine_stats": [field.name for field in fields(EngineStats)],
        "contract_volume": list(ContractVolume.__slots__),
        "print_record": list(PrintRecord.__slots__),
        "quote_view": list(QuoteView._fields),
        "cluster_state": list(ClusterState.__slots__),
        "shard": list(_Shard.__slots__),
        "quote_cache": list(QuoteCache.__slots__),
        "volume_book": sorted(vars(shard.book)),
        "cluster_engine": sorted(vars(shard.clusters)),
    })


# --- value coding -------------------------------------------------------------------------------

def _encode(value: Any) -> Any:
    kind = type(value)
    if value is None or kind is int or kind is str or kind is bool or kind is float:
        return value
    if kind is UUID:
        return str(value)
    if kind is datetime or kind is date:
        return value.isoformat()
    if kind is Decimal:
        return str(value)
    if kind is tuple:
        return list(value)
    raise TypeError(f"unsupported checkpoint value: {kind.__name__}")


def _decoder(hint: Any):
    origin = typing.get_origin(hint)
    if origin is typing.Union or origin is types.UnionType:
        inner = _decoder(next(arg for arg in typing.get_args(hint) if arg is not type(None)))
        return lambda value: None if value is None else inner(value)
    if hint is UUID:
        return UUID
    if hint is datetime:
        return datetime.fromisoformat
    if hint is date:
        return date.fromisoformat
    if hint is Decimal:
        return Decimal
    if origin is tuple:
        return tuple
    return lambda value: value


_EVENT_FIELDS = tuple(field.name for field in fields(FlowEvent))
_EVENT_DECODERS = tuple(_decoder(typing.get_type_hints(FlowEvent)[name]) for name in _EVENT_FIELDS)


def _event_out(event: FlowEvent | None) -> list | None:
    if event is None:
        return None
    return [_encode(getattr(event, name)) for name in _EVENT_FIELDS]


def _event_in(values: Sequence[Any] | None) -> FlowEvent | None:
    if values is None:
        return None
    if len(values) != len(_EVENT_FIELDS):
        raise ValueError("stored event does not match the event layout")
    return FlowEvent(**{name: decode(value) for name, decode, value in zip(_EVENT_FIELDS, _EVENT_DECODERS, values)})


# --- capture ------------------------------------------------------------------------------------

def _require_quiescent(engine: OptionFlowEngine) -> None:
    for shard in engine._shards:
        if shard.queue or shard.pending is not None:
            raise EngineNotQuiescent("drain the engine (process) before capturing its state")


def _record_out(record: PrintRecord) -> list:
    return [record.seq, record.time_ms, record.is_cancel, record.exchange, record.price, record.size,
            record.counted, record.covered, record.active, record.orig_seq, record.orig_size,
            _event_out(record.event)]


def _contract_out(cv: ContractVolume) -> list:
    return [cv.contract, cv.cumulative, cv.baseline.value, cv.reason, cv.max_seq, cv.last_time_ms,
            cv.snapshot_volume, cv.snapshot_watermark, cv.gap_end_ms, cv.evicted_max_seq,
            [_record_out(cv.history[key]) for key in cv.order]]


def _book_out(book: VolumeBook) -> dict:
    return {
        "sessions": [[session.isoformat(), [_contract_out(cv) for cv in contracts.values()]]
                     for session, contracts in book._sessions.items()],
        "floors": [[session.isoformat(), reason, end_ms] for session, (reason, end_ms) in book._new_floor.items()],
        "dirty": [[session.isoformat(), contract] for session, contract in book._dirty],
    }


def _quotes_out(cache: QuoteCache) -> dict:
    return {
        "contracts": [[symbol, [list(view) for view in history]] for symbol, history in cache._by_contract.items()],
        "counters": [cache.evicted_contracts, cache.out_of_order, cache.duplicates, cache.too_old,
                     cache.invalidations],
    }


def _clusters_out(engine: ClusterEngine) -> dict:
    states = []
    for key, state in engine._states.items():
        states.append([
            list(key), str(state.cluster_id), state.underlyer, state.end_ms, state.session_date.isoformat(),
            state.revision, state.emitted, state.final, state.retracted,
            None if state.span_ms is None else list(state.span_ms),
            [[member_key[0], member_key[1], member[0], str(member[1]), member[2], member[3], member[4]]
             for member_key, member in state.members.items()],
        ])
    return {
        "states": states,
        "open": [list(item) for item in engine._open],
        "purge": [[due, list(key)] for due, key in engine._purge],
        "scalars": [engine._max_time_ms, engine._watermark_ms, engine.late_beyond_retention,
                    engine.late_revisions, engine.finalized],
    }


def _shard_out(shard: _Shard) -> dict:
    return {
        "book": _book_out(shard.book),
        "quotes": _quotes_out(shard.quotes),
        "clusters": _clusters_out(shard.clusters),
        "clock": [None if shard.last_session is None else shard.last_session.isoformat(), shard.last_time_ms,
                  shard.next_checkpoint_ms, shard.last_offset, shard.reported_offset],
    }


def _shard_digest(shard: _Shard) -> str:
    return canonical_sha256([shard.book.digest(), shard.clusters.digest_state()])


def _engine_out(engine: OptionFlowEngine) -> dict:
    stats = engine.stats
    values = {field.name: getattr(stats, field.name) for field in fields(EngineStats)}
    values["unknown_condition_codes"] = sorted(values["unknown_condition_codes"].items())
    values["rejected_by_reason"] = sorted(values["rejected_by_reason"].items())
    universe = engine._quote_universe
    return {"quote_universe": None if universe is None else sorted(universe), "stats": values}


# --- encode / decode ----------------------------------------------------------------------------

def _document_bytes(document: Mapping[str, Any]) -> bytes:
    return json.dumps(document, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def encode_checkpoint(
    engine: OptionFlowEngine, *, consumer_name: str, cohort_id: UUID, generation: UUID, offset: int,
    sequence: int, extras: Mapping[str, Any], owner_epoch: int = 1,
    max_state_bytes: int = DEFAULT_MAX_STATE_BYTES,
) -> tuple[list[CheckpointRow], int]:
    """Serialize the engine into one row per shard. Returns the rows and the uncompressed byte total.

    Raises EngineNotQuiescent or CheckpointTooLarge; never writes a partial set."""
    if type(offset) is not int or offset < 0:
        raise ValueError("a checkpoint offset must be a nonnegative integer")
    _require_quiescent(engine)
    shard_count = len(engine._shards)
    common = {
        "schema": CHECKPOINT_SCHEMA_VERSION,
        "layout": layout_sha256(engine),
        "policy": engine.policy_sha256,
        "runtime": runtime_config_sha256(engine),
        "consumer": consumer_name,
        "cohort": str(cohort_id),
        "generation": str(generation),
        "shards": shard_count,
        "offset": offset,
        "sequence": sequence,
    }
    rows: list[CheckpointRow] = []
    raw_total = 0
    for index, shard in enumerate(engine._shards):
        document: dict[str, Any] = {"header": dict(common, shard=index, digest=_shard_digest(shard)),
                                    "state": _shard_out(shard)}
        if index == 0:
            document["engine"] = _engine_out(engine)
            document["extras"] = dict(extras)
        raw = _document_bytes(document)
        payload = zlib.compress(raw, _COMPRESSION_LEVEL)
        if len(payload) > max_state_bytes:
            raise CheckpointTooLarge(index, len(payload), max_state_bytes)
        raw_total += len(raw)
        rows.append(CheckpointRow(index, offset, shard_map_sha256(shard_count), owner_epoch, CODEC,
                                  hashlib.sha256(raw).hexdigest(), payload))
    return rows, raw_total


def _decode_row(row: CheckpointRow) -> dict:
    if row.state_codec != CODEC:
        raise CheckpointRejected("CHECKPOINT_BAD_CODEC", row.state_codec, offset=row.committed_offset)
    try:
        inflater = zlib.decompressobj()
        raw = inflater.decompress(bytes(row.state_payload), _MAX_DECOMPRESSED_BYTES)
        if inflater.unconsumed_tail or not inflater.eof:
            raise ValueError("checkpoint payload is truncated or too large")
    except (zlib.error, ValueError) as error:
        raise CheckpointRejected("CHECKPOINT_UNREADABLE", type(error).__name__, offset=row.committed_offset) from None
    if hashlib.sha256(raw).hexdigest() != row.state_hash:
        raise CheckpointRejected("CHECKPOINT_HASH_MISMATCH", f"shard {row.shard_id}", offset=row.committed_offset)
    try:
        document = json.loads(raw)
    except ValueError as error:
        raise CheckpointRejected("CHECKPOINT_UNREADABLE", type(error).__name__, offset=row.committed_offset) from None
    if not isinstance(document, dict) or not isinstance(document.get("header"), dict):
        raise CheckpointRejected("CHECKPOINT_UNREADABLE", "not a checkpoint document", offset=row.committed_offset)
    return document


def _restore_contract(item: list, session: date) -> ContractVolume:
    (contract, cumulative, baseline, reason, max_seq, last_time_ms, snapshot_volume, snapshot_watermark,
     gap_end_ms, evicted_max_seq, history) = item
    cv = ContractVolume(contract, session, BaselineKind(baseline), reason, gap_end_ms)
    cv.cumulative = cumulative
    cv.max_seq = max_seq
    cv.last_time_ms = last_time_ms
    cv.snapshot_volume = snapshot_volume
    cv.snapshot_watermark = snapshot_watermark
    cv.evicted_max_seq = evicted_max_seq
    for (seq, time_ms, is_cancel, exchange, price, size, counted, covered, active, orig_seq, orig_size,
         event) in history:
        record = PrintRecord(seq, time_ms, exchange, price, size, counted, covered, is_cancel)
        record.active = active
        record.orig_seq = orig_seq
        record.orig_size = orig_size
        record.event = _event_in(event)
        key = (seq, time_ms, is_cancel)
        cv.history[key] = record
        cv.order.append(key)
    return cv


def _restore_book(book: VolumeBook, data: Mapping[str, Any]) -> None:
    sessions: dict[date, dict[str, ContractVolume]] = {}
    for session_text, contracts in data["sessions"]:
        session = date.fromisoformat(session_text)
        sessions[session] = {item[0]: _restore_contract(item, session) for item in contracts}
    book._sessions = sessions
    book._new_floor = {date.fromisoformat(session): (reason, end_ms) for session, reason, end_ms in data["floors"]}
    book._dirty = {}
    for session_text, contract in data["dirty"]:
        session = date.fromisoformat(session_text)
        book._dirty[(session, contract)] = sessions[session][contract]
    book._cache_date = None
    book._cache = {}


def _restore_quotes(cache: QuoteCache, data: Mapping[str, Any]) -> None:
    cache._by_contract = OrderedDict(
        (symbol, [QuoteView(*view) for view in history]) for symbol, history in data["contracts"])
    (cache.evicted_contracts, cache.out_of_order, cache.duplicates, cache.too_old,
     cache.invalidations) = data["counters"]


def _restore_clusters(engine: ClusterEngine, data: Mapping[str, Any]) -> None:
    states = {}
    for (key, cluster_id, underlyer, end_ms, session, revision, emitted, final, retracted, span,
         members) in data["states"]:
        contract, side, window_seconds, start_ms = key
        state = ClusterState(UUID(cluster_id), contract, underlyer, side, window_seconds, start_ms, end_ms,
                             date.fromisoformat(session))
        state.revision, state.emitted, state.final, state.retracted = revision, emitted, final, retracted
        state.span_ms = None if span is None else (span[0], span[1])
        for seq, time_ms, size, premium, exchange, member_time, bucket in members:
            state.members[(seq, time_ms)] = (size, Decimal(premium), exchange, member_time, bucket)
        states[(contract, side, window_seconds, start_ms)] = state
    engine._states = states
    engine._open = [tuple(item) for item in data["open"]]
    engine._purge = [(due, tuple(key)) for due, key in data["purge"]]
    (engine._max_time_ms, engine._watermark_ms, engine.late_beyond_retention, engine.late_revisions,
     engine.finalized) = data["scalars"]


def _restore_shard(shard: _Shard, data: Mapping[str, Any]) -> None:
    _restore_book(shard.book, data["book"])
    _restore_quotes(shard.quotes, data["quotes"])
    _restore_clusters(shard.clusters, data["clusters"])
    last_session, shard.last_time_ms, shard.next_checkpoint_ms, shard.last_offset, shard.reported_offset = (
        data["clock"])
    shard.last_session = None if last_session is None else date.fromisoformat(last_session)


def restore_checkpoint(
    engine: OptionFlowEngine, rows: Sequence[CheckpointRow], *, consumer_name: str, cohort_id: UUID,
    generation: UUID, source_tail: int | None = None,
) -> RestoredCheckpoint:
    """Restore a FRESH engine from the stored shard rows or raise CheckpointRejected.

    On rejection the engine may be partly written: discard it and build a new one."""
    shard_count = len(engine._shards)
    if len(rows) != shard_count or sorted(row.shard_id for row in rows) != list(range(shard_count)):
        raise CheckpointRejected("CHECKPOINT_INCOMPLETE_SET", f"{len(rows)} rows for {shard_count} shards",
                                 offset=max((row.committed_offset for row in rows), default=None))
    ordered = sorted(rows, key=lambda row: row.shard_id)
    offsets = {row.committed_offset for row in ordered}
    claimed = max(offsets)
    expected_map = shard_map_sha256(shard_count)
    if any(row.shard_map_sha256 != expected_map for row in ordered):
        raise CheckpointRejected("CHECKPOINT_SHARD_MAP_MISMATCH", offset=claimed)
    documents = [_decode_row(row) for row in ordered]
    layout, runtime = layout_sha256(engine), runtime_config_sha256(engine)
    for row, document in zip(ordered, documents):
        header = document["header"]
        if header.get("schema") != CHECKPOINT_SCHEMA_VERSION or header.get("layout") != layout:
            raise CheckpointRejected("CHECKPOINT_VERSION_MISMATCH", offset=claimed)
        if header.get("policy") != engine.policy_sha256:
            raise CheckpointRejected("CHECKPOINT_POLICY_MISMATCH", offset=claimed)
        if header.get("runtime") != runtime:
            raise CheckpointRejected("CHECKPOINT_CONFIG_MISMATCH", offset=claimed)
        if (header.get("consumer"), header.get("cohort"), header.get("generation"), header.get("shards")) != (
                consumer_name, str(cohort_id), str(generation), shard_count) or header.get("shard") != row.shard_id:
            raise CheckpointRejected("CHECKPOINT_IDENTITY_MISMATCH", offset=claimed)
    sequences = {document["header"].get("sequence") for document in documents}
    header_offsets = {document["header"].get("offset") for document in documents}
    state_offset = next(iter(header_offsets))
    if (len(offsets) != 1 or len(sequences) != 1 or len(header_offsets) != 1 or type(state_offset) is not int
            or state_offset > claimed):
        raise CheckpointRejected("CHECKPOINT_INCOMPLETE_SET", "shard rows disagree on offset or sequence",
                                 offset=claimed)
    if source_tail is not None and claimed > source_tail:
        raise CheckpointRejected("CHECKPOINT_AHEAD_OF_SOURCE", f"checkpoint {claimed}, source {source_tail}",
                                 offset=claimed)
    try:
        for shard, document in zip(engine._shards, documents):
            _restore_shard(shard, document["state"])
            if _shard_digest(shard) != document["header"]["digest"]:
                raise CheckpointRejected("CHECKPOINT_STATE_DIGEST_MISMATCH", f"shard {shard.index}", offset=claimed)
        engine_block = documents[0]["engine"]
        universe = engine_block["quote_universe"]
        engine._quote_universe = None if universe is None else frozenset(universe)
        stats = dict(engine_block["stats"])
        stats["unknown_condition_codes"] = {int(code): count for code, count in stats["unknown_condition_codes"]}
        stats["rejected_by_reason"] = {reason: count for reason, count in stats["rejected_by_reason"]}
        engine.stats = EngineStats(**stats)
        extras = documents[0]["extras"]
    except CheckpointRejected:
        raise
    except (KeyError, TypeError, ValueError, IndexError) as error:
        raise CheckpointRejected("CHECKPOINT_UNREADABLE", f"restore failed: {type(error).__name__}",
                                 offset=claimed) from None
    return RestoredCheckpoint(state_offset, int(sequences.pop()), extras,
                              sum(len(row.state_payload) for row in rows), claimed)


# --- persistence --------------------------------------------------------------------------------

_COLUMNS = ("consumer_name", "cohort_id", "journal_generation", "shard_id", "shard_map_sha256", "owner_epoch",
            "committed_offset", "checkpoint_id", "state_codec", "state_hash", "state_payload", "committed_at")
SAVE_SQL = (
    "INSERT INTO rt.consumer_checkpoints (" + ", ".join(_COLUMNS) + ") "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp()) "
    "ON CONFLICT (consumer_name, cohort_id, journal_generation, shard_id) DO UPDATE SET "
    "shard_map_sha256 = EXCLUDED.shard_map_sha256, owner_epoch = EXCLUDED.owner_epoch, "
    "committed_offset = EXCLUDED.committed_offset, checkpoint_id = EXCLUDED.checkpoint_id, "
    "state_codec = EXCLUDED.state_codec, state_hash = EXCLUDED.state_hash, "
    "state_payload = EXCLUDED.state_payload, committed_at = EXCLUDED.committed_at, "
    "updated_at = clock_timestamp() "
    "WHERE %s OR EXCLUDED.committed_offset >= rt.consumer_checkpoints.committed_offset"
)
LOAD_SQL = (
    "SELECT shard_id, committed_offset, shard_map_sha256, owner_epoch, state_codec, state_hash, state_payload "
    "FROM rt.consumer_checkpoints WHERE consumer_name = %s AND cohort_id = %s AND journal_generation = %s "
    "ORDER BY shard_id"
)
# Between full checkpoints a flush only advances the durable row offset (no payload is rewritten).
ADVANCE_SQL = (
    "UPDATE rt.consumer_checkpoints SET committed_offset = %s, updated_at = clock_timestamp() "
    "WHERE consumer_name = %s AND cohort_id = %s AND journal_generation = %s AND committed_offset <= %s"
)


def save_checkpoint_rows(cursor, rows: Sequence[CheckpointRow], *, consumer_name: str, cohort_id: UUID,
                         generation: UUID, allow_regression: bool = False) -> None:
    """Upsert the shard rows. A row that would move the stored offset backwards is refused (the
    regression guard) unless allow_regression, used once after a rejected checkpoint."""
    for row in rows:
        identity = f"{consumer_name}|{cohort_id}|{generation}|{row.shard_id}|{row.committed_offset}|{row.state_hash}"
        cursor.execute(SAVE_SQL, (
            consumer_name, str(cohort_id), str(generation), row.shard_id, row.shard_map_sha256, row.owner_epoch,
            row.committed_offset, str(uuid5(CHECKPOINT_NAMESPACE, identity)), row.state_codec, row.state_hash,
            Binary(row.state_payload), allow_regression))
        if cursor.rowcount != 1:
            raise CheckpointRegression(
                f"shard {row.shard_id} checkpoint at offset {row.committed_offset} is older than the stored one")


def load_checkpoint_rows(cursor, *, consumer_name: str, cohort_id: UUID, generation: UUID) -> list[CheckpointRow]:
    cursor.execute(LOAD_SQL, (consumer_name, str(cohort_id), str(generation)))
    return [CheckpointRow(int(shard), int(offset), str(shard_map).strip(), int(epoch), str(codec),
                          str(state_hash).strip(), bytes(payload))
            for shard, offset, shard_map, epoch, codec, state_hash, payload in cursor.fetchall()]


class CheckpointStore:
    """Writes checkpoints for one (consumer, cohort, journal generation) through a cursor.

    The consumer calls `write` (the full state) or `advance` (only the durable row offset) from inside the
    writer's flush transaction. `sequence` increases with every full write. After a rejected checkpoint set
    `allow_regression` once so the first write may replace a stored checkpoint that is ahead of the source,
    and `must_write_full` so a stale payload is replaced instead of merely advanced."""

    def __init__(self, *, consumer_name: str, cohort_id: UUID, generation: UUID, owner_epoch: int = 1,
                 max_state_bytes: int = DEFAULT_MAX_STATE_BYTES, sequence: int = 0,
                 allow_regression: bool = False, must_write_full: bool = False) -> None:
        if owner_epoch < 1 or max_state_bytes < 1 or sequence < 0:
            raise ValueError("owner epoch, size bound and sequence must be positive")
        self.consumer_name = consumer_name
        self.cohort_id = cohort_id
        self.generation = generation
        self.owner_epoch = owner_epoch
        self.max_state_bytes = max_state_bytes
        self.sequence = sequence
        self.allow_regression = allow_regression
        self.must_write_full = must_write_full
        self.last_write: CheckpointWrite | None = None

    def load(self, cursor) -> list[CheckpointRow]:
        return load_checkpoint_rows(cursor, consumer_name=self.consumer_name, cohort_id=self.cohort_id,
                                    generation=self.generation)

    def restore(self, engine: OptionFlowEngine, rows: Sequence[CheckpointRow],
                *, source_tail: int | None) -> RestoredCheckpoint:
        restored = restore_checkpoint(engine, rows, consumer_name=self.consumer_name, cohort_id=self.cohort_id,
                                      generation=self.generation, source_tail=source_tail)
        self.sequence = max(self.sequence, restored.sequence)
        return restored

    def advance(self, cursor, *, offset: int, shard_count: int) -> bool:
        """Move the durable row offset of a complete stored checkpoint forward. False when there is nothing
        to advance (no complete set, or a stored offset ahead of this one): write a full checkpoint."""
        if self.must_write_full:
            return False
        cursor.execute(ADVANCE_SQL, (offset, self.consumer_name, str(self.cohort_id), str(self.generation), offset))
        return cursor.rowcount == shard_count

    def write(self, cursor, engine: OptionFlowEngine, *, offset: int, extras: Mapping[str, Any]) -> CheckpointWrite:
        started = time.perf_counter()
        rows, raw_total = encode_checkpoint(
            engine, consumer_name=self.consumer_name, cohort_id=self.cohort_id, generation=self.generation,
            offset=offset, sequence=self.sequence + 1, extras=extras, owner_epoch=self.owner_epoch,
            max_state_bytes=self.max_state_bytes)
        save_checkpoint_rows(cursor, rows, consumer_name=self.consumer_name, cohort_id=self.cohort_id,
                             generation=self.generation, allow_regression=self.allow_regression)
        self.sequence += 1
        self.allow_regression = False
        self.must_write_full = False
        self.last_write = CheckpointWrite(offset, self.sequence, sum(len(row.state_payload) for row in rows),
                                          raw_total, time.perf_counter() - started,
                                          max(len(row.state_payload) for row in rows))
        return self.last_write


def baseline_counts(engine: OptionFlowEngine) -> dict[str, int]:
    """Contracts per accumulator baseline across every session still held (WARMING, INCOMPLETE, ...)."""
    counts: dict[str, int] = {}
    for shard in engine._shards:
        for contracts in shard.book._sessions.values():
            for cv in contracts.values():
                counts[cv.baseline.value] = counts.get(cv.baseline.value, 0) + 1
    return counts


def prune_sessions(engine: OptionFlowEngine, keep_from: date) -> int:
    """Drop the accumulators of sessions before `keep_from` (their rows are persisted); returns how many
    sessions were dropped. Without it a stage that lives across days would carry, and checkpoint, every day."""
    dropped = 0
    for shard in engine._shards:
        before = len(shard.book._sessions)
        shard.book.drop_sessions_before(keep_from)
        dropped = max(dropped, before - len(shard.book._sessions))
    return dropped
