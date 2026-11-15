"""V3-1.5 (Nov 15): the Options window's quote set as of its cutoff, read from the session journal.

Why: the mark producer reads `rt.quote_current`, the market-state head at the moment the window runs (40-78 s after
the cutoff). A contract that quoted after the cutoff then has a head past the window's barrier and is left unmarked
(`OPTION_NBBO_AHEAD_OF_ROLLING_CHECKPOINT`), and the quote age is measured against the host's receive clock (R34, R35).

What this module reads: for each selected contract, the quote head the market-state materializer held when it had
scanned exactly up to the window's **barrier offset**, i.e. the newest quote at or before that normalized event offset
by the materializer's own rule (`materialization._replace_quote`: session, wrap-aware provider sequence, source
timestamp, offset). The row has the shape and the value types of an `rt.quote_current` row, so a contract that did
not quote between the barrier and the window's run gives a field-for-field identical row (and an identical mark).

The barrier offset (checked Nov 15 in `advanced_options_p8_roles._snapshot_v3_resident_windows` and
`advanced_options_p8_window.build_and_run_p8_serial_window`): the market-state member of
`rt.advanced_options_resident_completions` records `ending_offset` = `evidence.normalized_end_offset` =
`RawJournalReader.offsets_at(options_cutoff)[1]`, the last normalized event before the frame holding the first
credible event later than the cutoff. The window passes that same `normalized_end_offset` as
`market_state_committed_offset` to the mark plan, which is today's `head_offset_limit`. One value, three names.

Read path: one read-only SQLite connection per window (WAL-aware, `query_only`), per contract one indexed walk of
`idx_normalized_event_instrument_time (instrument_id, source_timestamp, event_offset)` backwards from the newest event,
skipping events past the barrier (about a minute of that contract's events when the window runs on time), keeping the
newest `candidate_limit` quotes by source time, folded in offset order with the materializer's replace rule. Exact
unless more than `candidate_limit` quotes at or before the barrier carry newer source times than the materializer's
head (a provider sequence running against its own timestamps); the replay checks this against stored marks.
"""
from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from uuid import UUID

from .contracts import AssetClass, MarketChannel
from .journal import NormalizedJournalRecord, _connect, _parse_stamp
from .materialization import MaterializedQuote, _quote_from_record, _replace_quote


MARKS_JOURNAL_UNAVAILABLE = "MARKS_JOURNAL_UNAVAILABLE"
DEFAULT_CANDIDATE_LIMIT = 32
# A window on time reads ~1 s of index per 100 contracts (Nov 12 replay: 72 contracts 0.8-1.1 s with a 120 s walk).
DEFAULT_TIME_BUDGET_SECONDS = 10.0
# rt.quote_current.bid/ask are numeric(20,8): the row read back from Postgres carries exactly 8 decimals.
_QUOTE_PRICE_QUANTUM = Decimal("0.00000001")
# The quote columns of the mark producer's slot/quote join, in its SELECT order.
QUOTE_COLUMNS = (
    "source_session",
    "source_sequence",
    "event_time_raw",
    "event_time_unit",
    "event_at",
    "received_at",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "bid_exchange",
    "ask_exchange",
    "quality",
    "reason_codes",
    "session_id",
    "journal_generation",
    "shard_id",
    "source_offset",
    "payload_sha256",
)
_CANDIDATES_SQL = (
    "SELECT normalized_events.*,"
    "raw_messages.journal_generation AS raw_journal_generation,"
    "raw_messages.stream_session_id AS raw_stream_session_id,"
    "raw_messages.received_at AS raw_received_at "
    "FROM normalized_events INDEXED BY idx_normalized_event_instrument_time "
    "JOIN raw_messages USING (raw_offset) "
    "WHERE normalized_events.instrument_id=? AND normalized_events.event_kind='Q' "
    "AND normalized_events.event_offset<=? {bound}"
    "ORDER BY normalized_events.source_timestamp DESC,normalized_events.event_offset DESC LIMIT ?"
)


class CutoffQuotesUnavailable(RuntimeError):
    """The journal could not give the cutoff quote set; the caller falls back to today's read."""


@dataclass(frozen=True, slots=True)
class CutoffQuoteRead:
    barrier_offset: int
    quotes: Mapping[int, Mapping[str, object]]
    read_ms: float
    candidates_examined: int = 0
    instruments_without_quote: tuple[int, ...] = field(default=())


def _record(row) -> NormalizedJournalRecord:
    """The same record `RawJournalReader.read_normalized_after` builds from one row."""
    return NormalizedJournalRecord(
        event_offset=row["event_offset"],
        raw_offset=row["raw_offset"],
        event_index=row["event_index"],
        normalizer_version=row["normalizer_version"],
        journal_generation=UUID(row["raw_journal_generation"]),
        stream_session_id=UUID(row["raw_stream_session_id"]),
        instrument_id=row["instrument_id"],
        provider_symbol=row["provider_symbol"],
        underlying=row["underlying"],
        asset_class=AssetClass(row["asset_class"]),
        shard_id=row["shard_id"],
        event_kind=MarketChannel(row["event_kind"]),
        source_session=date.fromisoformat(row["source_session"]),
        source_sequence=row["source_sequence"],
        source_timestamp=row["source_timestamp"],
        source_timestamp_unit=row["source_timestamp_unit"],
        event_at=_parse_stamp(row["event_at"]),
        price=Decimal(row["price_text"]) if row["price_text"] else None,
        size=Decimal(row["size_text"]) if row["size_text"] else None,
        bid=Decimal(row["bid_text"]) if row["bid_text"] else None,
        ask=Decimal(row["ask_text"]) if row["ask_text"] else None,
        bid_size=row["bid_size"],
        ask_size=row["ask_size"],
        exchange=row["exchange"],
        conditions=tuple(json.loads(row["conditions_json"])),
        quality=row["quality"],
        reason_codes=tuple(json.loads(row["reason_codes_json"])),
        details_json=row["details_json"],
        payload_sha256=row["payload_sha256"],
        received_at=_parse_stamp(row["raw_received_at"]),
        committed_at=_parse_stamp(row["committed_at"]),
    )


def _price(value: Decimal | None) -> Decimal | None:
    return None if value is None else value.quantize(_QUOTE_PRICE_QUANTUM, rounding=ROUND_HALF_UP)


def quote_current_row(
    quote: MaterializedQuote,
    *,
    shard_id: int,
    registered_stream_session_id: UUID,
) -> dict[str, object]:
    """The `rt.quote_current` columns market-state would write for this head (materialization_repository), with the
    types the mark producer's query returns. Market state stores the registered session id, not the per-attempt id
    the raw frame carries (`AdvancedOptionsJournalSource.read_normalized_after`)."""
    return {
        "source_session": quote.source_session,
        "source_sequence": quote.source_sequence,
        "event_time_raw": quote.source_timestamp,
        "event_time_unit": quote.source_timestamp_unit,
        "event_at": quote.event_at,
        "received_at": quote.received_at,
        "bid": _price(quote.bid),
        "ask": _price(quote.ask),
        "bid_size": quote.bid_size,
        "ask_size": quote.ask_size,
        "bid_exchange": quote.bid_exchange,
        "ask_exchange": quote.ask_exchange,
        "quality": quote.quality,
        "reason_codes": list(quote.reason_codes),
        "session_id": str(registered_stream_session_id),
        "journal_generation": str(quote.journal_generation),
        "shard_id": shard_id,
        "source_offset": quote.source_offset,
        "payload_sha256": quote.payload_sha256,
    }


def head_at_barrier(records: Iterable[NormalizedJournalRecord]) -> tuple[MaterializedQuote, int] | None:
    """Fold quote records in offset order with the materializer's replace rule; returns (head, its shard)."""
    head = None
    shard = None
    for record in sorted(records, key=lambda item: item.event_offset):
        candidate = _quote_from_record(record)
        if head is None or _replace_quote(head, candidate):
            head = candidate
            shard = record.shard_id
    return None if head is None else (head, shard)


def read_cutoff_quotes(
    path: str | Path,
    *,
    instrument_ids: Iterable[int],
    barrier_offset: int,
    journal_generation: UUID,
    registered_stream_session_id: UUID,
    allowed_stream_session_ids: frozenset[UUID] | None = None,
    normalizer_version: str | None = None,
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT,
    maximum_source_timestamp_ms: int | None = None,
    timeout_seconds: float = 5.0,
    time_budget_seconds: float | None = DEFAULT_TIME_BUDGET_SECONDS,
    connection: sqlite3.Connection | None = None,
) -> CutoffQuoteRead:
    """Every selected contract's quote head as of `barrier_offset`, keyed by instrument id (absent: no quote).

    Raises CutoffQuotesUnavailable for a missing or unreadable journal or one whose identity does not match; the
    window then keeps today's read with MARKS_JOURNAL_UNAVAILABLE. `maximum_source_timestamp_ms` bounds the backward
    walk (replays long after the cutoff only; quotes at or before the barrier stamped later than the bound are
    non-credible stamps, see `RawJournalReader.offsets_at`). `connection` lets a replay reuse one connection.
    `time_budget_seconds` aborts a read that runs longer (a window running long after its cutoff walks every event
    since): the window then keeps today's read (MARKS_JOURNAL_UNAVAILABLE:READ_TIME_BUDGET_EXCEEDED)."""
    if type(barrier_offset) is not int or barrier_offset < 0:
        raise ValueError("cutoff quote barrier offset must be a nonnegative integer")
    if type(candidate_limit) is not int or not 1 <= candidate_limit <= 1_000:
        raise ValueError("cutoff quote candidate limit must be in [1, 1000]")
    identifiers = tuple(sorted({int(value) for value in instrument_ids}))
    started = time.perf_counter()
    journal = Path(path)
    owned = connection is None
    over_budget = False

    def _budget_check() -> int:
        nonlocal over_budget
        over_budget = time.perf_counter() - started > time_budget_seconds
        return 1 if over_budget else 0

    try:
        if owned:
            if not journal.is_file():
                raise CutoffQuotesUnavailable(f"{MARKS_JOURNAL_UNAVAILABLE}:JOURNAL_FILE_MISSING")
            connection = _connect(journal, timeout_seconds=timeout_seconds, read_only=True)
        if time_budget_seconds is not None:
            connection.set_progress_handler(_budget_check, 10_000)
        metadata = connection.execute(
            "SELECT journal_generation FROM journal_metadata WHERE singleton=1"
        ).fetchone()
        if metadata is None or UUID(str(metadata[0])) != journal_generation:
            raise CutoffQuotesUnavailable(f"{MARKS_JOURNAL_UNAVAILABLE}:JOURNAL_GENERATION_MISMATCH")
        statement = _CANDIDATES_SQL.format(
            bound="" if maximum_source_timestamp_ms is None else "AND normalized_events.source_timestamp<=? "
        )
        quotes: dict[int, dict[str, object]] = {}
        missing = []
        examined = 0
        for instrument_id in identifiers:
            parameters = (
                (instrument_id, barrier_offset, candidate_limit)
                if maximum_source_timestamp_ms is None
                else (instrument_id, barrier_offset, maximum_source_timestamp_ms, candidate_limit)
            )
            records = tuple(_record(row) for row in connection.execute(statement, parameters).fetchall())
            examined += len(records)
            for record in records:
                if (
                    record.journal_generation != journal_generation
                    or record.asset_class is not AssetClass.OPTION
                    or (
                        allowed_stream_session_ids is not None
                        and record.stream_session_id not in allowed_stream_session_ids
                    )
                    or (normalizer_version is not None and record.normalizer_version != normalizer_version)
                ):
                    raise CutoffQuotesUnavailable(f"{MARKS_JOURNAL_UNAVAILABLE}:JOURNAL_IDENTITY_MISMATCH")
            folded = head_at_barrier(records)
            if folded is None:
                missing.append(instrument_id)
                continue
            head, shard = folded
            quotes[instrument_id] = quote_current_row(
                head,
                shard_id=shard,
                registered_stream_session_id=registered_stream_session_id,
            )
    except CutoffQuotesUnavailable:
        raise
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError) as error:
        if over_budget:
            raise CutoffQuotesUnavailable(
                f"{MARKS_JOURNAL_UNAVAILABLE}:READ_TIME_BUDGET_EXCEEDED:{time_budget_seconds}s"
            ) from error
        raise CutoffQuotesUnavailable(
            f"{MARKS_JOURNAL_UNAVAILABLE}:{type(error).__name__}:{error}"[:300]
        ) from error
    finally:
        if connection is not None:
            if time_budget_seconds is not None:
                connection.set_progress_handler(None, 0)
            if owned:
                connection.close()
    return CutoffQuoteRead(
        barrier_offset=barrier_offset,
        quotes=quotes,
        read_ms=round((time.perf_counter() - started) * 1000.0, 3),
        candidates_examined=examined,
        instruments_without_quote=tuple(missing),
    )
