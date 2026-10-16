"""The V2 stage database adapter (offline fakes plus an opt-in DB variant on the real V2 schema)."""
from __future__ import annotations

import threading
from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from realtime.database_access import RealtimeDatabaseRole
from realtime.stage_database_v2 import V2StageDatabase, V2WorkerDatabase, stage_databases
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageCancelled, StageContext
from realtime.worker_database import RealtimeProcessDatabase

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


class _Provider:
    def __init__(self):
        self.calls = []

    def transaction(self, role="app", **options):
        self.calls.append((role, options))
        return nullcontext(SimpleNamespace(cursor="the-cursor"))


def _context(provider, *, stage="EQUITY_EVIDENCE", cancelled=None):
    return StageContext(None, None, STAGES_V1[stage], uuid4(), 1, NOW, provider, cancelled, fenced=True)


def test_the_adapter_is_a_process_database_with_the_requested_role():
    database = V2StageDatabase(_context(_Provider()), RealtimeDatabaseRole.PUBLICATION)
    assert isinstance(database, RealtimeProcessDatabase)
    assert database.process_plan.role is RealtimeDatabaseRole.PUBLICATION
    with pytest.raises(TypeError):
        V2StageDatabase(_context(_Provider()), "COMPUTE")


def test_transactions_map_the_v1_arguments_onto_the_fenced_stage_context():
    provider = _Provider()
    compute, publication = stage_databases(_context(provider), RealtimeDatabaseRole.COMPUTE,
                                           RealtimeDatabaseRole.PUBLICATION)
    with compute.transaction(dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
        assert cursor == "the-cursor"
    with publication.transaction(read_only=True):
        pass
    (write_role, write), (read_role, read) = provider.calls
    assert write_role == read_role == "app"
    assert write["dict_cursor"] is True and write["isolation_level"] == "REPEATABLE READ"
    assert write["fence"] is not None
    assert read["read_only"] is True and "fence" not in read


def test_a_cancelled_stage_cannot_write_through_the_adapter():
    cancelled = threading.Event()
    database = V2StageDatabase(_context(_Provider(), cancelled=cancelled), RealtimeDatabaseRole.COMPUTE)
    cancelled.set()
    with pytest.raises(StageCancelled):
        with database.transaction():
            pass
    with database.transaction(read_only=True):
        pass


def test_the_logical_role_label_is_wiring_only_and_never_selects_a_database_role():
    provider = _Provider()
    for role in RealtimeDatabaseRole:
        database = V2StageDatabase(_context(provider), role)
        assert database.process_plan.role is role
        with database.transaction(read_only=True):
            pass
    assert len(provider.calls) == len(RealtimeDatabaseRole)
    assert {call[0] for call in provider.calls} == {"app"}  # the only database role a stage ever uses


def test_a_resident_options_stage_runs_its_transactions_unfenced_as_app():
    provider = _Provider()
    context = StageContext(None, None, STAGES_V1["OPTIONS_FLOW"], uuid4(), 1, NOW, provider, None, fenced=False)
    compute, ingress = stage_databases(context, RealtimeDatabaseRole.OPTIONS_COMPUTE, RealtimeDatabaseRole.OPTIONS_INGRESS)
    with compute.transaction():
        pass
    with ingress.transaction(read_only=True):
        pass
    (write_role, write), (read_role, read) = provider.calls
    assert write_role == read_role == "app"
    assert write["fence"] is None and read["read_only"] is True
    assert STAGES_V1["OPTIONS_FLOW"].lane == "options"


def test_external_job_workers_use_app_without_a_stage_fence():
    provider = _Provider()
    database = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
    assert isinstance(database, RealtimeProcessDatabase)
    with database.transaction(dict_cursor=True) as cursor:
        assert cursor == "the-cursor"
    role, options = provider.calls[0]
    assert role == "app"
    assert options["dict_cursor"] is True
    assert "fence" not in options
