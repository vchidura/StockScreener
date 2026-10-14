"""The V2 stage database adapter (offline fakes plus an opt-in DB variant on the real V2 schema)."""
from __future__ import annotations

import threading
from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from realtime.database_access import RealtimeDatabaseRole
from realtime.stage_database_v2 import V2StageDatabase, stage_databases
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageCancelled, StageContext
from realtime.worker_database import RealtimeProcessDatabase

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


class _Provider:
    def __init__(self):
        self.calls = []

    def transaction(self, role, profile, **options):
        self.calls.append((role, profile, options))
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
    (write_role, profile, write), (read_role, _, read) = provider.calls
    assert (write_role, profile) == ("compute", "runtime_data")
    assert write["dict_cursor"] is True and write["isolation_level"] == "REPEATABLE READ"
    assert write["fence"] is not None
    assert read_role == "publication" and read["read_only"] is True and "fence" not in read


def test_a_cancelled_stage_cannot_write_through_the_adapter():
    cancelled = threading.Event()
    database = V2StageDatabase(_context(_Provider(), cancelled=cancelled), RealtimeDatabaseRole.COMPUTE)
    cancelled.set()
    with pytest.raises(StageCancelled):
        with database.transaction():
            pass
    with database.transaction(read_only=True):
        pass


def test_every_stage_role_maps_to_a_known_capability_role():
    from realtime.relation_registry import CAPABILITY_ROLES

    assert {role.value.lower() for role in RealtimeDatabaseRole} <= set(CAPABILITY_ROLES)


def test_the_options_roles_are_runtime_roles_the_options_principal_is_a_member_of():
    from realtime.database_access import ROLE_PRINCIPALS, RealtimeDatabasePrincipal
    from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
    from realtime.relation_registry import PRINCIPAL_PROFILES

    for role in (RealtimeDatabaseRole.OPTIONS_INGRESS, RealtimeDatabaseRole.OPTIONS_COMPUTE):
        assert ROLE_PRINCIPALS[role] is RealtimeDatabasePrincipal.RUNTIME
        assert role.value.lower() in PRINCIPAL_PROFILES["runtime_options"]
        assert role.value.lower() not in PRINCIPAL_PROFILES["runtime_data"]
    provider = V2ConnectionProvider({OWNER_POOL: object()}, V2AccessConfig(allow_disposable=True))
    for capability_role in ("options_ingress", "options_compute"):
        provider._check_role(capability_role, "runtime_options")
    assert provider.role_violations == []
    provider._check_role("options_compute", "runtime_data")  # the data principal is not a member: recorded
    assert [violation.capability_role for violation in provider.role_violations] == ["options_compute"]


def test_a_resident_options_stage_runs_its_transactions_unfenced_under_the_options_roles():
    provider = _Provider()
    context = StageContext(None, None, STAGES_V1["OPTIONS_FLOW"], uuid4(), 1, NOW, provider, None, fenced=False)
    compute, ingress = stage_databases(context, RealtimeDatabaseRole.OPTIONS_COMPUTE, RealtimeDatabaseRole.OPTIONS_INGRESS)
    with compute.transaction():
        pass
    with ingress.transaction(read_only=True):
        pass
    (write_role, profile, write), (read_role, _, read) = provider.calls
    assert (write_role, profile, read_role) == ("options_compute", "runtime_options", "options_ingress")
    assert write["fence"] is None and read["read_only"] is True
    assert STAGES_V1["OPTIONS_FLOW"].capability_role == "options_compute"
