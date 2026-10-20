from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from test_advanced_options_cycle import NOW
from realtime.advanced_options_service import (
    OPTIONS_RESIDENT_STAGE_SUBSET,
    AdvancedOptionsProcessHealth,
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentSupervisor,
    build_advanced_options_non_serving_qualification_plan,
    build_provider_free_advanced_options_resident_plan,
    evaluate_advanced_options_process_health,
)


class _Leases:
    def __init__(self):
        self.released = []

    def acquire(self, _key, _owner, *, lease_seconds, now):
        assert lease_seconds == 30
        return 1

    def renew(self, _key, _owner, _epoch, *, lease_seconds, now):
        return True

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
    assert plan.policy.stage_subset == OPTIONS_RESIDENT_STAGE_SUBSET
    assert plan.policy.heartbeat_interval_seconds == 10
    assert plan.policy.stale_after_seconds == 30
    assert plan.policy.restart_delay_seconds(1) == 1
    assert plan.policy.restart_delay_seconds(10) == 30
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
        p7_report_sha256="a" * 64,
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


def test_non_serving_qualification_plan_freezes_full_inert_v4_subset():
    plan = build_advanced_options_non_serving_qualification_plan(
        plan_id=uuid4(),
        cohort_id=uuid4(),
        session_date=NOW.date(),
        config_sha256="c" * 64,
        deadline=NOW + timedelta(hours=1),
        p7_manifest_id=uuid4(),
        p7_report_sha256="d" * 64,
    )

    assert plan.service_plan.stage_catalog_version == "stages_v4"
    assert plan.service_plan.enabled_stages == OPTIONS_RESIDENT_STAGE_SUBSET
    assert not plan.publication_authorized
    assert not plan.delivery_authorized
