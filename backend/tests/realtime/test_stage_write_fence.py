"""R1: ordinary stage writes are fenced to their stage run (offline; the DB scenario is in
test_stage_orchestrator.py)."""
from __future__ import annotations

import threading
from contextlib import nullcontext
from datetime import datetime, timezone
from uuid import uuid4

import psycopg2
import pytest

from realtime.database_access_v2 import (
    FENCE_SQLSTATE, OWNER_POOL, StageRunFence, V2AccessConfig, V2ConnectionProvider, WriteFenceRejected,
)
from realtime.security_v2 import EXECUTE_GRANTS
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageCancelled, StageContext
from tests.realtime.test_normalization_diagnostics_and_access_v2 import _Pool

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


class _Rejected(psycopg2.Error):
    pgcode = FENCE_SQLSTATE


class _OtherError(psycopg2.Error):
    pgcode = "40001"


class _RaisingCursor:
    def __init__(self, error):
        self.error, self.statements = error, []

    def execute(self, statement, parameters=()):
        self.statements.append((statement, parameters))
        raise self.error


def test_a_fenced_transaction_checks_the_run_as_its_last_statement_then_commits():
    pool = _Pool([])
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig())
    run = uuid4()
    with provider.transaction(fence=StageRunFence(run)) as unit:
        unit.cursor.execute("INSERT INTO probe VALUES (1)")
    assert pool.connection.statements[-1] == ("SELECT rt.fence_stage_run(%s)", (str(run),))
    assert pool.connection.statements[-2][0] == "INSERT INTO probe VALUES (1)"
    assert pool.connection.committed == 1 and pool.connection.rolled_back == 0


def test_a_rejected_fence_rolls_everything_back_and_never_commits():
    pool = _Pool([])
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig())

    class Rejecting:
        def verify(self, cursor):
            raise WriteFenceRejected("stage run is not RUNNING")

    with pytest.raises(WriteFenceRejected):
        with provider.transaction(fence=Rejecting()) as unit:
            unit.cursor.execute("INSERT INTO probe VALUES (1)")
    assert pool.connection.committed == 0 and pool.connection.rolled_back == 1


def test_a_body_failure_skips_the_fence_and_rolls_back():
    pool = _Pool([])
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig())
    with pytest.raises(RuntimeError, match="boom"):
        with provider.transaction(fence=StageRunFence(uuid4())):
            raise RuntimeError("boom")
    assert not any("fence_stage_run" in text for text, _ in pool.connection.statements)
    assert pool.connection.committed == 0 and pool.connection.rolled_back == 1


def test_a_read_only_transaction_cannot_carry_a_fence():
    provider = V2ConnectionProvider({OWNER_POOL: _Pool([])}, V2AccessConfig())
    with pytest.raises(ValueError, match="no writes to fence"):
        with provider.transaction(read_only=True, fence=StageRunFence(uuid4())):
            pass


def test_only_the_fence_sqlstate_becomes_a_write_rejection():
    run = uuid4()
    with pytest.raises(WriteFenceRejected, match=str(run)):
        StageRunFence(run).verify(_RaisingCursor(_Rejected("not running")))
    with pytest.raises(_OtherError):
        StageRunFence(run).verify(_RaisingCursor(_OtherError("serialization failure")))


class _Provider:
    def __init__(self):
        self.calls = []

    def transaction(self, role="app", **options):
        self.calls.append((role, options))
        return nullcontext()


def _context(provider, *, fenced=True, cancelled=None, stage="EQUITY_INGEST"):
    return StageContext(None, None, STAGES_V1[stage], uuid4(), 1, NOW, provider, cancelled, fenced=fenced)


def test_a_window_stage_write_carries_the_stage_run_fence_and_reads_do_not():
    provider = _Provider()
    context = _context(provider)
    with context.transaction():
        pass
    with context.transaction(read_only=True):
        pass
    (_, write), (_, read) = provider.calls
    assert write == {"dict_cursor": False, "isolation_level": None, "fence": StageRunFence(context.stage_run_id)}
    assert read == {"read_only": True, "dict_cursor": False, "isolation_level": None}


def test_every_write_of_a_window_stage_stays_fenced_whatever_the_work_area():
    provider = _Provider()
    context = _context(provider, stage="EQUITY_EVIDENCE")
    with context.transaction(dict_cursor=True, isolation_level="REPEATABLE READ"):
        pass
    role, options = provider.calls[0]
    assert role == "app"
    assert options == {"dict_cursor": True, "isolation_level": "REPEATABLE READ",
                       "fence": StageRunFence(context.stage_run_id)}


def test_a_resident_stage_has_no_stage_run_to_fence():
    provider = _Provider()
    with _context(provider, fenced=False, stage="REFERENCE").transaction():
        pass
    assert provider.calls[0][1] == {"dict_cursor": False, "isolation_level": None, "fence": None}


def test_a_cancelled_stage_cannot_open_a_write_transaction_but_may_still_read():
    provider, cancelled = _Provider(), threading.Event()
    context = _context(provider, cancelled=cancelled)
    cancelled.set()
    with pytest.raises(StageCancelled):
        context.transaction()
    assert provider.calls == []
    with context.transaction(read_only=True):
        pass
    assert len(provider.calls) == 1


def test_the_fence_function_is_granted_to_the_app_role_only():
    assert EXECUTE_GRANTS["rt.fence_stage_run(uuid)"] == ("app",)
