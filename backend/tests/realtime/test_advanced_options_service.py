import os
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from psycopg2 import Error as PsycopgError

from test_advanced_options_cycle import NOW
from realtime.advanced_options_service import (
    OPTIONS_NON_SERVING_STAGE_ORDER,
    OPTIONS_RESIDENT_STAGE_SUBSET,
    OPTIONS_V3_STAGE_ORDER,
    OPTIONS_V3_STAGE_SUBSET,
    PASSING_P7_MANIFEST_ID,
    PASSING_P7_MANIFEST_SHA256,
    PASSING_P7_REPORT_SHA256,
    PASSING_P7_SOURCE_COMMIT,
    AdvancedOptionsHostFencedLeasePort,
    AdvancedOptionsPostgresLeasePort,
    AdvancedOptionsProcessHealth,
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentSupervisor,
    AdvancedOptionsSubprocessPort,
    build_advanced_options_non_serving_qualification_plan,
    build_advanced_options_v3_service_plan,
    build_owp3_advanced_options_resident_plan,
    build_provider_free_advanced_options_resident_plan,
    evaluate_advanced_options_process_health,
)
from realtime.provider_socket_lease import MASSIVE_OPTIONS_SOCKET_RESOURCE


class _Leases:
    def __init__(self):
        self.released = []
        self.renewed = True

    def acquire(self, _key, _owner, *, lease_seconds, now):
        assert lease_seconds == 30
        return 1

    def renew(self, _key, _owner, _epoch, *, lease_seconds, now):
        return self.renewed

    def release(self, key, owner, epoch):
        self.released.append((key, owner, epoch))


class _Processes:
    def __init__(self):
        self.handles = {}
        self.stopped = []

    def start(self, spec):
        handle = object()
        self.handles[handle] = True
        return handle

    def alive(self, handle):
        return self.handles[handle]

    def stop(self, handle, *, deadline_seconds):
        self.stopped.append((handle, deadline_seconds))


def test_provider_free_resident_plan_has_exact_four_process_ownership():
    plan = build_provider_free_advanced_options_resident_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
    )

    assert tuple(row.kind for row in plan.processes) == tuple(
        AdvancedOptionsProcessKind
    )
    assert OPTIONS_NON_SERVING_STAGE_ORDER == (
        "OPTIONS_ACQUIRE",
        "OPTIONS_ADMISSION",
        "OPTIONS_FLOW",
        "OPTIONS_INPUT_GATE",
        "OPTIONS_CYCLE",
        "OPTIONS_PROJECTION",
        "OPTIONS_PUBLISH",
    )
    assert plan.policy.stage_subset == OPTIONS_RESIDENT_STAGE_SUBSET
    assert plan.policy.heartbeat_interval_seconds == 10
    assert plan.policy.stale_after_seconds == 30
    assert plan.policy.restart_delay_seconds(1) == 1
    assert plan.policy.restart_delay_seconds(10) == 30
    assert (
        plan.processes[0].lease_resource_key
        == MASSIVE_OPTIONS_SOCKET_RESOURCE
    )
    assert not plan.activation_authorized
    assert plan.p7_manifest_id is None


def test_resident_health_fails_closed_for_stale_or_exhausted_owner():
    plan = build_provider_free_advanced_options_resident_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
    )
    healthy = AdvancedOptionsProcessHealth(
        AdvancedOptionsProcessKind.INGRESS,
        uuid4(),
        1,
        NOW - timedelta(seconds=5),
        0,
        None,
        None,
    )
    stale = AdvancedOptionsProcessHealth(
        AdvancedOptionsProcessKind.INGRESS,
        healthy.owner_id,
        1,
        NOW - timedelta(seconds=31),
        5,
        NOW - timedelta(minutes=1),
        NOW + timedelta(seconds=30),
    )

    assert evaluate_advanced_options_process_health(
        healthy,
        plan.policy,
        checked_at=NOW,
    ).status == "READY"
    verdict = evaluate_advanced_options_process_health(
        stale,
        plan.policy,
        checked_at=NOW,
    )
    assert verdict.status == "UNAVAILABLE"
    assert verdict.reason_codes == (
        "HEARTBEAT_STALE",
        "RESTART_BUDGET_EXHAUSTED",
        "RESTART_BACKOFF_ACTIVE",
    )


def test_authorized_supervisor_owns_four_leases_and_bounded_restart():
    provider_free = build_provider_free_advanced_options_resident_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
    )
    plan = replace(
        provider_free,
        p7_manifest_id=uuid4(),
        p7_manifest_sha256="b" * 64,
        p7_report_sha256="a" * 64,
        p7_source_commit="c" * 40,
        activation_authorized=True,
    )
    leases = _Leases()
    processes = _Processes()
    supervisor = AdvancedOptionsResidentSupervisor(
        plan,
        leases,
        processes,
        owner_ids={kind: uuid4() for kind in AdvancedOptionsProcessKind},
    )

    supervisor.tick(now=NOW)

    assert all(
        verdict.status == "READY"
        for verdict in supervisor.health(checked_at=NOW).values()
    )
    ingress = next(iter(processes.handles))
    processes.handles[ingress] = False
    supervisor.tick(now=NOW + timedelta(seconds=1))
    assert len(leases.released) == 1
    assert len(processes.handles) == 4
    supervisor.tick(now=NOW + timedelta(seconds=1, milliseconds=500))
    assert len(processes.handles) == 4
    supervisor.tick(now=NOW + timedelta(seconds=2))
    assert len(processes.handles) == 5
    supervisor.stop()
    assert len(leases.released) == 5


def test_supervisor_stops_an_owner_immediately_after_lease_loss():
    plan = build_owp3_advanced_options_resident_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
    )
    leases = _Leases()
    processes = _Processes()
    supervisor = AdvancedOptionsResidentSupervisor(
        plan,
        leases,
        processes,
        owner_ids={kind: uuid4() for kind in AdvancedOptionsProcessKind},
    )
    supervisor.tick(now=NOW)
    leases.renewed = False

    supervisor.tick(now=NOW + timedelta(seconds=1))

    assert len(processes.stopped) == 4
    assert len(leases.released) == 4
    assert all(
        verdict.reason_codes == ("PROCESS_NOT_RUNNING",)
        for verdict in supervisor.health(
            checked_at=NOW + timedelta(seconds=1),
        ).values()
    )


def test_non_serving_qualification_plan_freezes_full_inert_v4_subset():
    plan = build_advanced_options_non_serving_qualification_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
        config_sha256="c" * 64,
        deadline=NOW + timedelta(hours=1),
    )

    assert plan.service_plan.stage_catalog_version == "stages_v4"
    assert plan.service_plan.enabled_stages == OPTIONS_RESIDENT_STAGE_SUBSET
    assert plan.p7_manifest_id == PASSING_P7_MANIFEST_ID
    assert plan.p7_manifest_sha256 == PASSING_P7_MANIFEST_SHA256
    assert plan.p7_report_sha256 == PASSING_P7_REPORT_SHA256
    assert plan.p7_source_commit == PASSING_P7_SOURCE_COMMIT
    assert plan.cohort_kind == "MARKET_OPTIONS"
    assert not any(plan.effect_authorizations.values())


def test_v3_service_plan_is_prospective_and_keeps_v4_plan_unchanged():
    plan = build_advanced_options_v3_service_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
        config_sha256="d" * 64,
        deadline=NOW + timedelta(hours=1),
    )

    assert plan.stage_catalog_version == "stages_v5"
    assert plan.enabled_stages == OPTIONS_V3_STAGE_SUBSET
    assert OPTIONS_V3_STAGE_ORDER == (
        "OPTIONS_ACQUIRE",
        "OPTIONS_ADMISSION",
        "OPTIONS_REFERENCE",
        "OPTIONS_FLOW",
        "OPTIONS_INPUT_GATE",
        "OPTIONS_CYCLE",
        "OPTIONS_PROJECTION",
        "OPTIONS_PUBLISH",
    )
    assert OPTIONS_NON_SERVING_STAGE_ORDER == (
        "OPTIONS_ACQUIRE",
        "OPTIONS_ADMISSION",
        "OPTIONS_FLOW",
        "OPTIONS_INPUT_GATE",
        "OPTIONS_CYCLE",
        "OPTIONS_PROJECTION",
        "OPTIONS_PUBLISH",
    )


def test_owp3_resident_plan_is_bound_to_the_passing_p7_evidence():
    plan = build_owp3_advanced_options_resident_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
    )

    assert plan.activation_authorized
    assert plan.p7_manifest_id == PASSING_P7_MANIFEST_ID
    assert plan.p7_manifest_sha256 == PASSING_P7_MANIFEST_SHA256
    assert plan.p7_report_sha256 == PASSING_P7_REPORT_SHA256
    assert plan.p7_source_commit == PASSING_P7_SOURCE_COMMIT


def test_host_socket_fence_releases_database_lease_and_owned_artifacts(
    tmp_path,
):
    postgres = _Leases()
    first_owner = uuid4()
    second_owner = uuid4()
    path = tmp_path / "options.lock"
    first = AdvancedOptionsHostFencedLeasePort(
        postgres,
        socket_lock_path=path,
        cleanup_host_artifacts=True,
    )
    second = AdvancedOptionsHostFencedLeasePort(
        postgres,
        socket_lock_path=path,
        cleanup_host_artifacts=True,
    )

    assert first.acquire(
        MASSIVE_OPTIONS_SOCKET_RESOURCE,
        first_owner,
        lease_seconds=30,
        now=NOW,
    ) == 1
    assert second.acquire(
        MASSIVE_OPTIONS_SOCKET_RESOURCE,
        second_owner,
        lease_seconds=30,
        now=NOW,
    ) is None
    assert postgres.released == [
        (MASSIVE_OPTIONS_SOCKET_RESOURCE, second_owner, 1)
    ]

    first.release(MASSIVE_OPTIONS_SOCKET_RESOURCE, first_owner, 1)

    assert not path.exists()
    assert not Path(f"{path}.json").exists()


class _RetryablePostgresError(PsycopgError):
    @property
    def pgcode(self):
        return "40001"


class _LeaseCursor:
    def __init__(self):
        self.rowcount = 1
        self.statement = None

    def execute(self, statement, _parameters):
        self.statement = statement

    @staticmethod
    def fetchone():
        return (7,)


class _RetryingDatabase:
    def __init__(self):
        self.attempts = 0
        self.cursor = _LeaseCursor()

    @contextmanager
    def transaction(self):
        self.attempts += 1
        if self.attempts == 1:
            raise _RetryablePostgresError("serialization")
        yield SimpleNamespace(cursor=self.cursor)


def test_postgres_lease_retries_only_a_rolled_back_transaction():
    database = _RetryingDatabase()
    waits = []
    port = AdvancedOptionsPostgresLeasePort(
        database,
        retry_initial_seconds=0.25,
        wait=waits.append,
    )

    epoch = port.acquire(
        "advanced-options:test",
        uuid4(),
        lease_seconds=30,
        now=NOW,
    )

    assert epoch == 7
    assert database.attempts == 2
    assert waits == [0.25]
    assert "ON CONFLICT" in database.cursor.statement


def test_subprocess_port_stops_all_four_isolated_owners(tmp_path):
    plan = build_provider_free_advanced_options_resident_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
    )
    command = (
        sys.executable,
        "-B",
        "-c",
        "import time; time.sleep(60)",
    )
    environment = {
        name: os.environ[name]
        for name in ("SystemRoot", "WINDIR", "TEMP", "TMP")
        if name in os.environ
    }
    port = AdvancedOptionsSubprocessPort(
        {kind: command for kind in AdvancedOptionsProcessKind},
        cwd=tmp_path,
        environment=environment,
    )
    handles = [port.start(spec) for spec in plan.processes]
    try:
        assert all(port.alive(handle) for handle in handles)
    finally:
        for handle in handles:
            port.stop(handle, deadline_seconds=5)

    assert all(not port.alive(handle) for handle in handles)
