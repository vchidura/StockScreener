"""R54 (Nov 13): Options journal registrations are re-sealed at the journal's own end (main + WAL hash) and marked
DRAINED once market-state and Flow have consumed it; the plan is read-only, the apply writes seal then drain."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from realtime.advanced_options_journal_finalize import (
    FLOW_CONSUMER,
    MARKET_STATE_CONSUMER,
    JournalEnd,
    OptionsJournalRegistration,
    apply_finalize_cursor,
    plan_finalize,
    read_journal_end,
)
from realtime.journal import journal_files_sha256

NOW = datetime(2026, 11, 13, 3, 0, tzinfo=timezone.utc)


def _journal(tmp_path, raw=5, events=9):
    path = tmp_path / "journal.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("CREATE TABLE raw_messages (raw_offset INTEGER PRIMARY KEY, payload BLOB)")
    connection.execute("CREATE TABLE normalized_events (event_offset INTEGER PRIMARY KEY, payload BLOB)")
    connection.executemany("INSERT INTO raw_messages VALUES (?, ?)", [(n, b"r") for n in range(1, raw + 1)])
    connection.executemany("INSERT INTO normalized_events VALUES (?, ?)", [(n, b"e") for n in range(1, events + 1)])
    connection.commit()
    return path, connection  # keep open: the rows stay in the WAL like a live journal


def _registration(status="DRAINING", raw=4, norm=7, sha="a" * 64):
    return OptionsJournalRegistration(uuid4(), uuid4(), date(2026, 11, 12), "options-acquisition/x/journal.sqlite",
                                      status, raw, norm, sha)


def test_journal_end_reads_the_wal_and_hashes_main_and_wal(tmp_path):
    path, connection = _journal(tmp_path)
    try:
        end = read_journal_end(path)
        assert (end.raw_offset, end.event_offset) == (5, 9)
        assert end.files_sha256 == journal_files_sha256(path)
        assert (tmp_path / "journal.sqlite-wal").stat().st_size > 0
    finally:
        connection.close()


def test_short_seal_is_resealed_and_drained_when_consumers_are_at_the_end():
    end = JournalEnd(5, 9, "b" * 64)
    plan = plan_finalize(_registration(), end, {MARKET_STATE_CONSUMER: 9, FLOW_CONSUMER: 5})
    assert (plan.action, plan.reasons) == ("SEAL_AND_DRAIN", ("SEAL_SHORT",))
    exact = plan_finalize(_registration(raw=5, norm=9, sha="b" * 64), end, {MARKET_STATE_CONSUMER: 9, FLOW_CONSUMER: 5})
    assert exact.action == "DRAIN"
    main_only = plan_finalize(_registration(raw=5, norm=9), end, {MARKET_STATE_CONSUMER: 9, FLOW_CONSUMER: 5})
    assert (main_only.action, main_only.reasons) == ("SEAL_AND_DRAIN", ("SEAL_HASH_MAIN_FILE_ONLY",))


@pytest.mark.parametrize(("registration", "end", "consumers", "action", "reason"), [
    (_registration(), JournalEnd(5, 9, "b" * 64), {MARKET_STATE_CONSUMER: 8, FLOW_CONSUMER: 5}, "SEAL_ONLY",
     "CONSUMERS_BEHIND"),
    (_registration(raw=5, norm=9, sha="b" * 64), JournalEnd(5, 9, "b" * 64), {FLOW_CONSUMER: 5}, "NONE",
     "CONSUMERS_BEHIND"),
    (_registration(status="WRITING", raw=3, norm=3, sha=None), JournalEnd(5, 9, "b" * 64),
     {MARKET_STATE_CONSUMER: 9, FLOW_CONSUMER: 5}, "SEAL_AND_DRAIN", "NEVER_SEALED"),
    (_registration(status="DRAINED"), JournalEnd(5, 9, "b" * 64), {}, "NONE", "ALREADY_DRAINED"),
    (_registration(status="QUARANTINED"), JournalEnd(5, 9, "b" * 64), {}, "REFUSE", "STATUS_QUARANTINED"),
    (_registration(raw=6), JournalEnd(5, 9, "b" * 64), {}, "REFUSE", "JOURNAL_SHORTER_THAN_REGISTRY"),
    (_registration(), None, {}, "REFUSE", "JOURNAL_FILE_UNAVAILABLE"),
])
def test_plan_rules(registration, end, consumers, action, reason):
    plan = plan_finalize(registration, end, consumers)
    assert plan.action == action and reason in plan.reasons


class _Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return ("ok",)


def test_apply_seals_with_the_exact_end_then_marks_drained():
    end = JournalEnd(5, 9, "b" * 64)
    plan = plan_finalize(_registration(), end, {MARKET_STATE_CONSUMER: 9, FLOW_CONSUMER: 5})
    cursor = _Cursor()
    assert apply_finalize_cursor(cursor, plan, checked_at=NOW) == "SEAL_AND_DRAIN"
    (seal_sql, seal_params), (drain_sql, drain_params) = cursor.statements
    assert "SET status='DRAINING'" in seal_sql and seal_params[:3] == (5, 9, "b" * 64)
    assert "SET status='DRAINED'" in drain_sql and drain_params[0] == 9
    nothing = _Cursor()
    assert apply_finalize_cursor(nothing, plan_finalize(_registration(status="DRAINED"), end, {}), checked_at=NOW) == "NONE"
    assert nothing.statements == []
