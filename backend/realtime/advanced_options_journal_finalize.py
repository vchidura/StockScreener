"""R54 (Nov 13 night): finalize an Options journal registration after its session.

Diagnosis (G102 evidence, Nov 12 night): the V3 ingress role seals the Options journal at the decision stop with the
last progress offsets it read (27-204 events short of the journal's end: the acquisition status says COMPLETE ~90 s
later, after its integrity check), hashes the main SQLite file only (`path.read_bytes()`, without the WAL), and nothing
ever marks an Options journal DRAINED (Equity's bridge does it for Stocks). Consumers are not short: market-state and
Flow checkpoints reach the true end every day.

This module does the missing step outside the session path, so the live roles are unchanged: read the journal's own end
(read-only, WAL-aware), hash main file + WAL like Equity (`journal_files_sha256`), compare the consumers
(`market-state-materializer-v1` >= the last normalized event offset, `option_flow_stream_v1` >= the last raw offset),
then re-seal with the exact end (the registry's seal keeps the greater offsets) and mark DRAINED. Planning is read-only;
applying writes the registry (an operator action: past sessions need the user's yes).
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Mapping
from uuid import UUID

from .advanced_journal_registry import mark_advanced_journal_drained_cursor, seal_advanced_journal_cursor
from .journal import journal_files_sha256

MARKET_STATE_CONSUMER = "market-state-materializer-v1"
FLOW_CONSUMER = "option_flow_stream_v1"
OPTIONS_JOURNAL_PREFIX = "options-acquisition/"


@dataclass(frozen=True, slots=True)
class JournalEnd:
    raw_offset: int
    event_offset: int
    files_sha256: str


def read_journal_end(path: str | Path, *, with_hash: bool = True) -> JournalEnd:
    """The last raw and normalized offsets of a journal (read-only, WAL-aware) and its main + WAL hash."""
    journal = Path(path)
    if not journal.is_file():
        raise FileNotFoundError(journal)
    connection = sqlite3.connect(f"file:{journal}?mode=ro", uri=True)
    try:
        raw = connection.execute("SELECT COALESCE(max(raw_offset),0) FROM raw_messages").fetchone()[0]
        event = connection.execute("SELECT COALESCE(max(event_offset),0) FROM normalized_events").fetchone()[0]
    finally:
        connection.close()
    return JournalEnd(int(raw), int(event), journal_files_sha256(journal) if with_hash else "")


@dataclass(frozen=True, slots=True)
class OptionsJournalRegistration:
    journal_generation: UUID
    cohort_id: UUID
    session_date: date
    relative_path: str
    status: str
    raw_committed_offset: int
    normalized_committed_offset: int
    sealed_files_sha256: str | None


@dataclass(frozen=True, slots=True)
class FinalizePlan:
    registration: OptionsJournalRegistration
    end: JournalEnd | None
    market_state_offset: int | None
    flow_offset: int | None
    action: str  # NONE | SEAL_AND_DRAIN | DRAIN | SEAL_ONLY | REFUSE
    reasons: tuple[str, ...]

    def summary(self) -> dict:
        row = self.registration
        return {
            "journal_generation": str(row.journal_generation), "session_date": row.session_date.isoformat(),
            "status": row.status, "sealed": [row.raw_committed_offset, row.normalized_committed_offset],
            "sealed_sha256": (row.sealed_files_sha256 or "")[:12] or None,
            "end": None if self.end is None else [self.end.raw_offset, self.end.event_offset],
            "end_sha256": None if self.end is None else self.end.files_sha256[:12],
            "consumers": {"flow_raw": self.flow_offset, "market_state_normalized": self.market_state_offset},
            "action": self.action, "reasons": list(self.reasons),
        }


def plan_finalize(registration: OptionsJournalRegistration, end: JournalEnd | None,
                  consumers: Mapping[str, int]) -> FinalizePlan:
    """What finalizing would do, from the registration, the journal's end and the consumer checkpoints (pure)."""
    market_state, flow = consumers.get(MARKET_STATE_CONSUMER), consumers.get(FLOW_CONSUMER)

    def plan(action, *reasons):
        return FinalizePlan(registration, end, market_state, flow, action, tuple(reasons))

    if registration.status == "DRAINED":
        return plan("NONE", "ALREADY_DRAINED")
    if registration.status not in {"WRITING", "DRAINING"}:
        return plan("REFUSE", f"STATUS_{registration.status}")
    if end is None:
        return plan("REFUSE", "JOURNAL_FILE_UNAVAILABLE")
    if end.raw_offset <= 0 or end.event_offset <= 0:
        return plan("REFUSE", "JOURNAL_EMPTY")
    if end.raw_offset < registration.raw_committed_offset or end.event_offset < registration.normalized_committed_offset:
        return plan("REFUSE", "JOURNAL_SHORTER_THAN_REGISTRY")
    exact = (
        registration.status == "DRAINING"
        and registration.raw_committed_offset == end.raw_offset
        and registration.normalized_committed_offset == end.event_offset
        and registration.sealed_files_sha256 == end.files_sha256
    )
    reasons = [] if exact else [
        "SEAL_SHORT" if (registration.raw_committed_offset, registration.normalized_committed_offset)
        != (end.raw_offset, end.event_offset) else "SEAL_HASH_MAIN_FILE_ONLY"
    ]
    if registration.status == "WRITING":
        reasons = ["NEVER_SEALED"]
    if market_state is None or flow is None or market_state < end.event_offset or flow < end.raw_offset:
        return plan("NONE" if exact else "SEAL_ONLY", *reasons, "CONSUMERS_BEHIND")
    return plan("DRAIN" if exact else "SEAL_AND_DRAIN", *reasons)


def load_options_registrations_cursor(cursor, *, generations=(), since: date | None = None):
    """Options registrations (tuple cursor), newest last."""
    where, params = ["relative_path LIKE %s"], [OPTIONS_JOURNAL_PREFIX + "%"]
    if generations:
        where.append("journal_generation=ANY(%s::uuid[])")
        params.append([str(value) for value in generations])
    if since is not None:
        where.append("session_date>=%s")
        params.append(since)
    cursor.execute(
        "SELECT journal_generation::text,cohort_id::text,session_date,relative_path,status,raw_committed_offset,"
        "normalized_committed_offset,sealed_files_sha256 FROM rt.advanced_journal_registry WHERE "
        + " AND ".join(where) + " ORDER BY session_date,journal_generation",
        tuple(params),
    )
    return tuple(
        OptionsJournalRegistration(UUID(str(row[0])), UUID(str(row[1])), row[2], str(row[3]), str(row[4]),
                                   int(row[5] or 0), int(row[6] or 0), None if row[7] is None else str(row[7]))
        for row in cursor.fetchall()
    )


def consumer_offsets_cursor(cursor, registration: OptionsJournalRegistration) -> dict[str, int]:
    cursor.execute(
        "SELECT consumer_name,max(committed_offset) FROM rt.consumer_checkpoints "
        "WHERE cohort_id=%s AND journal_generation=%s AND consumer_name=ANY(%s) GROUP BY consumer_name",
        (str(registration.cohort_id), str(registration.journal_generation), [MARKET_STATE_CONSUMER, FLOW_CONSUMER]),
    )
    return {str(name): int(offset) for name, offset in cursor.fetchall()}


def apply_finalize_cursor(cursor, plan: FinalizePlan, *, checked_at: datetime) -> str:
    """Write the plan (tuple cursor, caller's transaction): re-seal with the exact end, then mark DRAINED."""
    if plan.action not in {"SEAL_AND_DRAIN", "DRAIN", "SEAL_ONLY"}:
        return plan.action
    generation = plan.registration.journal_generation
    if plan.action in {"SEAL_AND_DRAIN", "SEAL_ONLY"}:
        seal_advanced_journal_cursor(
            cursor, generation, raw_committed_offset=plan.end.raw_offset,
            normalized_committed_offset=plan.end.event_offset, sealed_files_sha256=plan.end.files_sha256,
            checked_at=checked_at,
        )
    if plan.action in {"SEAL_AND_DRAIN", "DRAIN"}:
        mark_advanced_journal_drained_cursor(
            cursor, generation, bridge_committed_offset=int(plan.market_state_offset), checked_at=checked_at,
        )
    return plan.action
