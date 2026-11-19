"""G102 R93 (Nov 18 night): the Options window reads its inputs in a REPEATABLE READ READ ONLY snapshot.

Why: the window's producers assemble their state inside the SERIALIZABLE transaction that writes it (reference state,
rolling state, cutoff marks), the native input load reads every lane's inputs (O1 events, O1 debit vertical legs, OS
bars, O3) in a SERIALIZABLE READ ONLY transaction, and the pre-stage reads (up to 10,000 flow events for the evidence,
the Equity receipt, the runtime policy) run at the database default isolation (SERIALIZABLE). Postgres takes one SIRead
predicate lock per tuple / page read by a serializable transaction and keeps them past commit while overlapping
serializable transactions run: one window connection took 591-7,149 SIReadLocks in the first ~20 s after its release on
Nov 16 (live max_pred_locks_per_transaction 64; Nov 13 crashed at ~38,600, R71). R71 fixed the same shape for Stock
Alerts (b80fc4e): build in a REPEATABLE READ READ ONLY snapshot, keep SERIALIZABLE only for the short write.

`REALTIME_V3_WINDOW_INPUT_SNAPSHOT` on the Options window (`realtime-v3-options-p8-window`):
| OFF (default, unset, blank) | today, byte for byte: every transaction as before |
| ON | reference / rolling / mark `produce()`: assemble in REPEATABLE READ READ ONLY (the preflight's own transaction
       shape), then one short SERIALIZABLE transaction that persists the assembled state (the persist functions keep
       their own conflict checks: an existing row that differs still raises, a diverged rolling checkpoint is still
       AdvancedOptionsWindowRetryDiverged); native `load()`: REPEATABLE READ READ ONLY instead of SERIALIZABLE READ ONLY;
       the window's other read-only reads at the default isolation (flow evidence, Equity receipt, runtime policy,
       reference generation): REPEATABLE READ READ ONLY |
Anything else and the window service refuses to start (never silently OFF).

What does not change: what is read (same SQL, same plans, same bounds: the inputs of a window are bounded by its cutoff,
committed offsets and receipts), what is written, the retry policy (40001 / 40P01, same attempts and back-off around
the assemble + write), every stage after the claim (claims, gates, cycle writes, publication keep their isolation).
A snapshot read cannot fail with a serialization error, so ON also removes those retries from the reads.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Mapping

WINDOW_INPUT_SNAPSHOT_SETTING = "REALTIME_V3_WINDOW_INPUT_SNAPSHOT"
SNAPSHOT_ISOLATION = "REPEATABLE READ"
_VALUES = ("OFF", "ON")


def window_input_snapshot_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """`REALTIME_V3_WINDOW_INPUT_SNAPSHOT`; unset or blank is OFF. Anything but OFF or ON raises."""
    raw = (os.environ if environ is None else environ).get(WINDOW_INPUT_SNAPSHOT_SETTING)
    value = (raw or "").strip().upper() or "OFF"
    if value not in _VALUES:
        raise ValueError(f"{WINDOW_INPUT_SNAPSHOT_SETTING} must be OFF or ON, not {raw!r}")
    return value == "ON"


class SnapshotReadProvider:
    """The window's provider with read-only transactions at the default isolation moved to REPEATABLE READ (ON only).
    Writes, and reads that name their isolation, pass through unchanged."""

    def __init__(self, provider) -> None:
        self._provider = provider

    @property
    def wrapped(self):
        return self._provider

    @contextmanager
    def transaction(self, role, *args, read_only: bool = False, isolation_level: str | None = None, **kwargs):
        if read_only and isolation_level is None:
            isolation_level = SNAPSHOT_ISOLATION
        with self._provider.transaction(
            role, *args, read_only=read_only, isolation_level=isolation_level, **kwargs,
        ) as unit:
            yield unit

    def __getattr__(self, name):
        return getattr(self._provider, name)


def window_input_reads(provider, environ: Mapping[str, str] | None = None):
    """The provider the window's pre-stage reads use: itself when OFF (today), the snapshot proxy when ON."""
    if not window_input_snapshot_enabled(environ):
        return provider
    if isinstance(provider, SnapshotReadProvider):
        return provider
    return SnapshotReadProvider(provider)


def snapshot_then_write(provider, role, assemble, persist):
    """ON's shape for a producer's `produce()`: `assemble(unit)` in one REPEATABLE READ READ ONLY transaction, then
    `persist(unit, assembled)` in one SERIALIZABLE transaction. Returns what `persist` returns. Exceptions propagate
    (the caller's retry loop decides)."""
    with provider.transaction(role, read_only=True, isolation_level=SNAPSHOT_ISOLATION) as unit:
        assembled = assemble(unit)
    with provider.transaction(role, isolation_level="SERIALIZABLE") as unit:
        return persist(unit, assembled)


__all__ = [
    "SNAPSHOT_ISOLATION", "SnapshotReadProvider", "WINDOW_INPUT_SNAPSHOT_SETTING", "snapshot_then_write",
    "window_input_reads", "window_input_snapshot_enabled",
]
