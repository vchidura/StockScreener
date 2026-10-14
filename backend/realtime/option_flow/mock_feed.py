"""A deterministic scripted feed that speaks the journal read protocol (no socket, no file, no clock).

`MockOptionFeed` mimics what the OPTIONS_FLOW consumer reads from RawJournalReader: records with a
monotonically increasing `raw_offset`, the journal `generation`, a `stream_session_id`, a per-session
`message_number` and the raw frame `payload` bytes (a JSON array of provider messages, as the gateway
journals them). It exists so the consumer, the checkpoint/restore path and the divergence policy can be
proven offline and on a disposable database without a gateway, websocket or provider.

Scripting helpers cover trades, quotes, cancels, control frames, undecodable frames and the three source
discontinuities the consumer must turn into gaps: a gateway disconnect (new stream session), lost
frames (message numbers jump) and a hole in the offsets. `divergent_replay` returns a copy in which
chosen frames carry different field values for the same (contract, sequence, time) identities.
`generate_messages` builds a seeded busy stream for throughput and checkpoint-size measurements.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid5

_NAMESPACE = UUID("9d2c4f60-3b7a-4e1d-8a55-61c0e2b7d913")
MAX_READ_LIMIT = 10_000


@dataclass(frozen=True, slots=True)
class MockJournalRecord:
    """The fields of RawJournalRecord the consumer uses."""

    raw_offset: int
    generation: UUID
    stream_session_id: UUID
    message_number: int
    payload: bytes


def scripted_trade(symbol: str, seq: int, time_ms: int, price: float, size: int, *, conditions: Sequence[int],
                   exchange: int | None = 302) -> dict[str, Any]:
    message: dict[str, Any] = {"ev": "T", "sym": symbol, "p": price, "s": size, "c": list(conditions),
                               "t": time_ms, "q": seq}
    if exchange is not None:
        message["x"] = exchange
    return message


def scripted_quote(symbol: str, seq: int, time_ms: int, bid: float, ask: float, *, bid_size: int = 10,
                   ask_size: int = 12) -> dict[str, Any]:
    return {"ev": "Q", "sym": symbol, "bx": 301, "ax": 302, "bp": bid, "ap": ask, "bs": bid_size,
            "as": ask_size, "t": time_ms, "q": seq}


def scripted_cancel(original: Mapping[str, Any], *, cancel_condition: int) -> dict[str, Any]:
    """A cancel that echoes the original's sequence, time, exchange, price and size."""
    return dict(original, c=[cancel_condition])


class MockOptionFeed:
    """Append-only scripted journal. Offsets start at 1 and only grow."""

    def __init__(self, generation: UUID | None = None, *, seed: str = "mock-option-feed") -> None:
        self.seed = seed
        self.generation = generation or uuid5(_NAMESPACE, f"{seed}|generation")
        self._records: list[MockJournalRecord] = []
        self._session_index = 0
        self._next_message_number = 0
        self._next_offset = 1
        self.reads = 0

    # --- the read protocol -----------------------------------------------------------------------

    def read_after(self, raw_offset: int, *, limit: int = 1000) -> tuple[MockJournalRecord, ...]:
        if type(raw_offset) is not int or raw_offset < 0:
            raise ValueError("raw_offset must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= MAX_READ_LIMIT:
            raise ValueError("limit must be in [1, 10000]")
        self.reads += 1
        selected = []
        for record in self._records:
            if record.raw_offset > raw_offset:
                selected.append(record)
                if len(selected) == limit:
                    break
        return tuple(selected)

    def last_offset(self) -> int:
        return self._records[-1].raw_offset if self._records else 0

    @property
    def records(self) -> tuple[MockJournalRecord, ...]:
        return tuple(self._records)

    # --- scripting -------------------------------------------------------------------------------

    @property
    def stream_session_id(self) -> UUID:
        return uuid5(_NAMESPACE, f"{self.generation}|session|{self._session_index}")

    def append_payload(self, payload: bytes) -> int:
        """Journal raw bytes as one frame (a decodable or an undecodable one). Returns its offset."""
        if type(payload) is not bytes or not payload:
            raise ValueError("a frame payload must be nonempty bytes")
        record = MockJournalRecord(self._next_offset, self.generation, self.stream_session_id,
                                   self._next_message_number, payload)
        self._records.append(record)
        self._next_offset += 1
        self._next_message_number += 1
        return record.raw_offset

    def append_frame(self, messages: Sequence[Mapping[str, Any]]) -> int:
        return self.append_payload(json.dumps(list(messages), separators=(",", ":")).encode("ascii"))

    def add_trade(self, symbol: str, seq: int, time_ms: int, price: float, size: int, *, conditions: Sequence[int],
                  exchange: int | None = 302) -> int:
        return self.append_frame([scripted_trade(symbol, seq, time_ms, price, size, conditions=conditions,
                                                 exchange=exchange)])

    def add_quote(self, symbol: str, seq: int, time_ms: int, bid: float, ask: float, **sizes: int) -> int:
        return self.append_frame([scripted_quote(symbol, seq, time_ms, bid, ask, **sizes)])

    def add_cancel(self, original: Mapping[str, Any], *, cancel_condition: int) -> int:
        return self.append_frame([scripted_cancel(original, cancel_condition=cancel_condition)])

    def add_status(self, status: str = "success", message: str = "subscribed") -> int:
        """A provider control frame; it carries no market data and must be skipped."""
        return self.append_frame([{"ev": "status", "status": status, "message": message}])

    def add_undecodable(self) -> int:
        """A frame that is not valid JSON: any trade in it is unknown, so the consumer records a gap."""
        return self.append_payload(b"{not json")

    def disconnect(self) -> None:
        """The gateway reconnected: the next frame comes from a new stream session, numbered from zero."""
        self._session_index += 1
        self._next_message_number = 0

    def lose_messages(self, count: int) -> None:
        """Frames the gateway admitted but the journal never received: message numbers jump."""
        if count < 1:
            raise ValueError("count must be positive")
        self._next_message_number += count

    def skip_offsets(self, count: int) -> None:
        """A hole in the journal offsets (retention trimmed or rows lost)."""
        if count < 1:
            raise ValueError("count must be positive")
        self._next_offset += count

    # --- derived feeds ---------------------------------------------------------------------------

    def copy(self) -> MockOptionFeed:
        clone = MockOptionFeed(self.generation, seed=self.seed)
        clone._records = list(self._records)
        clone._session_index = self._session_index
        clone._next_message_number = self._next_message_number
        clone._next_offset = self._next_offset
        return clone

    def truncated(self, last_offset: int) -> MockOptionFeed:
        """The same feed as it was at `last_offset` (a journal that is behind a stored checkpoint). New frames
        continue that moment's stream session and message numbering."""
        clone = self.copy()
        clone._records = [record for record in self._records if record.raw_offset <= last_offset]
        clone._next_offset = last_offset + 1
        if clone._records:
            tail = clone._records[-1]
            clone._next_message_number = tail.message_number + 1
            clone._session_index = next(
                index for index in range(self._session_index + 1)
                if uuid5(_NAMESPACE, f"{self.generation}|session|{index}") == tail.stream_session_id)
        else:
            clone._next_message_number = 0
            clone._session_index = 0
        return clone

    def divergent_replay(self, changes: Mapping[int, Mapping[str, Any]]) -> MockOptionFeed:
        """A copy whose frames at the given offsets carry different field values (for example `{"s": 999}`)
        for the same (contract, sequence, time): a replay that no longer matches what was stored."""
        clone = self.copy()
        replaced = []
        for record in self._records:
            override = changes.get(record.raw_offset)
            if override is None:
                replaced.append(record)
                continue
            messages = [dict(message, **override) if message.get("ev") in ("T", "Q") else message
                        for message in json.loads(record.payload)]
            replaced.append(MockJournalRecord(
                record.raw_offset, record.generation, record.stream_session_id, record.message_number,
                json.dumps(messages, separators=(",", ":")).encode("ascii")))
        missing = set(changes) - {record.raw_offset for record in replaced}
        if missing:
            raise ValueError(f"no frame at offsets {sorted(missing)}")
        clone._records = replaced
        return clone

    def extend(self, messages: Iterable[Mapping[str, Any]], *, frame_size: int = 1) -> None:
        """Append provider messages, `frame_size` per frame."""
        if frame_size < 1:
            raise ValueError("frame_size must be positive")
        batch: list[Mapping[str, Any]] = []
        for message in messages:
            batch.append(message)
            if len(batch) == frame_size:
                self.append_frame(batch)
                batch = []
        if batch:
            self.append_frame(batch)


def generate_messages(
    *, contracts: Sequence[str], count: int, base_ms: int, rate_per_second: int = 1_000, seed: int = 11,
    regular_condition: int, quoted: Iterable[str] = (), big_fraction: float = 0.02,
) -> list[dict[str, Any]]:
    """A seeded stream of quotes and trades over `contracts`, sequences strictly increasing per contract."""
    rng = random.Random(seed)
    quoted_list = sorted(set(quoted))
    sequences: dict[tuple[str, str], int] = {}
    messages: list[dict[str, Any]] = []
    for index in range(count):
        time_ms = base_ms + index * 1000 // rate_per_second
        if quoted_list and rng.random() < 0.3:
            symbol = rng.choice(quoted_list)
            sequences[("Q", symbol)] = sequences.get(("Q", symbol), 0) + 1
            bid = round(rng.uniform(0.5, 20.0), 2)
            messages.append(scripted_quote(symbol, sequences[("Q", symbol)], time_ms, bid, round(bid + 0.05, 2)))
            continue
        symbol = rng.choice(contracts)
        sequences[("T", symbol)] = sequences.get(("T", symbol), 0) + 1
        big = rng.random() < big_fraction
        price = round(rng.uniform(1.0, 20.0), 2) if big else round(rng.uniform(0.05, 4.0), 2)
        size = rng.randint(500, 2000) if big else rng.randint(1, 50)
        messages.append(scripted_trade(symbol, sequences[("T", symbol)], time_ms, price, size,
                                       conditions=(regular_condition,), exchange=300 + rng.randrange(6)))
    return messages
