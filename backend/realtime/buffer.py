from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Callable
from uuid import UUID, uuid4

from .contracts import RawMessageEnvelope


@dataclass(frozen=True, slots=True)
class BufferLimits:
    maximum_events: int
    maximum_bytes: int
    maximum_age_ns: int

    def __post_init__(self) -> None:
        if type(self.maximum_events) is not int or self.maximum_events <= 0:
            raise ValueError("maximum_events must be a positive integer")
        if type(self.maximum_bytes) is not int or self.maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be a positive integer")
        if type(self.maximum_age_ns) is not int or self.maximum_age_ns <= 0:
            raise ValueError("maximum_age_ns must be a positive integer")


class BufferFull(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class BufferClaim:
    claim_id: UUID
    messages: tuple[RawMessageEnvelope, ...]
    payload_bytes: int


@dataclass(frozen=True, slots=True)
class BufferSnapshot:
    queued_events: int
    claimed_events: int
    admitted_events: int
    payload_bytes: int
    oldest_age_ns: int
    full_count: int
    maximum_admitted_events: int
    maximum_payload_bytes: int
    maximum_oldest_age_ns: int


class RawMessageBuffer:
    def __init__(
        self,
        limits: BufferLimits,
        *,
        monotonic_ns: Callable[[], int],
    ) -> None:
        self.limits = limits
        self._monotonic_ns = monotonic_ns
        self._queued: deque[RawMessageEnvelope] = deque()
        self._claim: BufferClaim | None = None
        self._payload_bytes = 0
        self._full_count = 0
        self._maximum_admitted_events = 0
        self._maximum_payload_bytes = 0
        self._maximum_oldest_age_ns = 0
        self._lock = threading.Lock()

    def put_nowait(self, message: RawMessageEnvelope) -> None:
        now = self._monotonic_ns()
        with self._lock:
            age = self._oldest_age_ns(now)
            if age > self.limits.maximum_age_ns:
                self._reject("MAXIMUM_AGE_EXCEEDED")
            admitted_events = len(self._queued) + (
                len(self._claim.messages) if self._claim is not None else 0
            )
            if admitted_events >= self.limits.maximum_events:
                self._reject("MAXIMUM_EVENTS_EXCEEDED")
            if message.byte_length > self.limits.maximum_bytes:
                self._reject("MESSAGE_EXCEEDS_BYTE_BUDGET")
            if self._payload_bytes + message.byte_length > self.limits.maximum_bytes:
                self._reject("MAXIMUM_BYTES_EXCEEDED")
            self._queued.append(message)
            self._payload_bytes += message.byte_length
            self._maximum_admitted_events = max(
                self._maximum_admitted_events,
                admitted_events + 1,
            )
            self._maximum_payload_bytes = max(
                self._maximum_payload_bytes,
                self._payload_bytes,
            )
            self._maximum_oldest_age_ns = max(
                self._maximum_oldest_age_ns,
                self._oldest_age_ns(now),
            )

    def _reject(self, reason: str) -> None:
        self._full_count += 1
        raise BufferFull(reason)

    def claim_nowait(
        self,
        *,
        maximum_events: int,
        maximum_bytes: int,
    ) -> BufferClaim | None:
        if type(maximum_events) is not int or maximum_events <= 0:
            raise ValueError("maximum_events must be a positive integer")
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be a positive integer")
        with self._lock:
            if self._claim is not None:
                raise RuntimeError("a raw-message buffer claim is already active")
            selected: list[RawMessageEnvelope] = []
            payload_bytes = 0
            while self._queued and len(selected) < maximum_events:
                message = self._queued[0]
                if selected and payload_bytes + message.byte_length > maximum_bytes:
                    break
                if not selected and message.byte_length > maximum_bytes:
                    raise ValueError("claim byte budget cannot fit the oldest message")
                selected.append(self._queued.popleft())
                payload_bytes += message.byte_length
            if not selected:
                return None
            claim = BufferClaim(uuid4(), tuple(selected), payload_bytes)
            self._claim = claim
            return claim

    def acknowledge(self, claim_id: UUID) -> None:
        with self._lock:
            claim = self._matching_claim(claim_id)
            self._payload_bytes -= claim.payload_bytes
            self._claim = None

    def release(self, claim_id: UUID) -> None:
        with self._lock:
            claim = self._matching_claim(claim_id)
            self._queued.extendleft(reversed(claim.messages))
            self._claim = None

    def _matching_claim(self, claim_id: UUID) -> BufferClaim:
        if self._claim is None or self._claim.claim_id != claim_id:
            raise ValueError("claim does not own the active raw-message batch")
        return self._claim

    def snapshot(self) -> BufferSnapshot:
        now = self._monotonic_ns()
        with self._lock:
            claimed_events = len(self._claim.messages) if self._claim else 0
            oldest_age_ns = self._oldest_age_ns(now)
            self._maximum_oldest_age_ns = max(
                self._maximum_oldest_age_ns,
                oldest_age_ns,
            )
            return BufferSnapshot(
                queued_events=len(self._queued),
                claimed_events=claimed_events,
                admitted_events=len(self._queued) + claimed_events,
                payload_bytes=self._payload_bytes,
                oldest_age_ns=oldest_age_ns,
                full_count=self._full_count,
                maximum_admitted_events=self._maximum_admitted_events,
                maximum_payload_bytes=self._maximum_payload_bytes,
                maximum_oldest_age_ns=self._maximum_oldest_age_ns,
            )

    def _oldest_age_ns(self, now: int) -> int:
        oldest = None
        if self._claim is not None:
            oldest = self._claim.messages[0].received_monotonic_ns
        if self._queued:
            queued_oldest = self._queued[0].received_monotonic_ns
            oldest = queued_oldest if oldest is None else min(oldest, queued_oldest)
        return max(0, now - oldest) if oldest is not None else 0