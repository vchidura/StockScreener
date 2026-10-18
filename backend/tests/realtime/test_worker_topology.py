from datetime import datetime, timedelta, timezone
from uuid import UUID

from realtime.database_access import (
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    planned_process_connection_ceiling,
)
from realtime.preflight import ProviderTier, TierState
from realtime.tier_cohort import (
    ADVANCED_CAPABILITIES,
    DEVELOPER_CAPABILITIES,
    DEVELOPER_DISABLED_DETECTORS,
    TierCohortBinding,
    product_capabilities,
    product_disabled_detectors,
)
from realtime.worker_topology import (
    API_READER_POOL_BUDGET,
    RUNTIME_WORKER_POOL_BUDGET,
    EquityRolloutMode,
    RealtimeWorkerKind,
    WorkerLane,
    build_api_reader_database_plan,
    build_equity_rollout_plan,
    build_tier_worker_plan,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def _developer():
    return TierCohortBinding(
        UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        ProviderTier.MASSIVE_DEVELOPER,
        TierState.DEVELOPER_READY,
        "a" * 64,
        "b" * 64,
        DEVELOPER_CAPABILITIES,
        DEVELOPER_DISABLED_DETECTORS,
        NOW,
    )


def _advanced():
    return TierCohortBinding(
        UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        ProviderTier.MASSIVE_ADVANCED,
        TierState.ADVANCED_READY,
        "c" * 64,
        "d" * 64,
        ADVANCED_CAPABILITIES,
        frozenset(),
        NOW + timedelta(minutes=10),
    )


def _advanced_equity_only():
    return TierCohortBinding(
        UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        ProviderTier.MASSIVE_ADVANCED,
        TierState.ADVANCED_READY,
        "e" * 64,
        "f" * 64,
        product_capabilities(ProviderTier.MASSIVE_ADVANCED, None),
        product_disabled_detectors(ProviderTier.MASSIVE_ADVANCED, None),
        NOW + timedelta(minutes=10),
        options_enabled=False,
    )


def _kinds(plan, cohort_id):
    return {
        row.kind
        for row in plan.assignments
        if row.cohort_id == cohort_id
    }


def test_developer_active_uses_rest_acquisition_without_advanced_hot_workers():
    developer = _developer()
    plan = build_tier_worker_plan(active=developer)
    kinds = _kinds(plan, developer.cohort_id)

    assert RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION in kinds
    assert RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION in kinds
    assert RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY not in kinds
    assert RealtimeWorkerKind.FAST_MATERIALIZER not in kinds
    assert sum(row.publishes for row in plan.assignments) == 1


def test_advanced_warms_in_parallel_without_replacing_developer_publisher():
    developer = _developer()
    advanced = _advanced()
    plan = build_tier_worker_plan(active=developer, warming=advanced)

    assert RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION in _kinds(
        plan, developer.cohort_id
    )
    assert RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION in _kinds(
        plan, developer.cohort_id
    )
    assert RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY in _kinds(
        plan, advanced.cohort_id
    )
    publisher = next(row for row in plan.assignments if row.publishes)
    assert publisher.cohort_id == developer.cohort_id
    assert all(
        not row.publishes
        for row in plan.assignments
        if row.cohort_id == advanced.cohort_id
    )
    assert RealtimeWorkerKind.DETECTOR_RUNNER not in _kinds(
        plan,
        advanced.cohort_id,
    )


def test_advanced_only_rehearsal_has_no_serving_publisher():
    advanced = _advanced()
    plan = build_tier_worker_plan(active=advanced)
    kinds = _kinds(plan, advanced.cohort_id)

    assert RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION not in kinds
    assert RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION not in kinds
    assert RealtimeWorkerKind.ADVANCED_STOCK_GATEWAY in kinds
    assert RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY in kinds
    assert RealtimeWorkerKind.DETECTOR_RUNNER not in kinds
    assert not any(row.publishes for row in plan.assignments)
    assert plan.active_cohort_id is None
    assert plan.warming_cohort_id == advanced.cohort_id
    assert all(
        row.lane is WorkerLane.HOT
        for row in plan.assignments
        if row.kind in {
            RealtimeWorkerKind.ADVANCED_STOCK_GATEWAY,
            RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY,
            RealtimeWorkerKind.DECODER,
            RealtimeWorkerKind.NORMALIZED_DISPATCHER,
            RealtimeWorkerKind.FAST_MATERIALIZER,
        }
    )


def test_advanced_equity_only_does_not_start_any_options_worker():
    advanced = _advanced_equity_only()
    plan = build_tier_worker_plan(active=advanced)
    kinds = _kinds(plan, advanced.cohort_id)

    assert RealtimeWorkerKind.ADVANCED_STOCK_GATEWAY in kinds
    assert RealtimeWorkerKind.ADVANCED_OPTION_GATEWAY not in kinds
    assert RealtimeWorkerKind.DEVELOPER_OPTION_ACQUISITION not in kinds
    bridge = next(
        row
        for row in plan.assignments
        if row.kind is RealtimeWorkerKind.RAW_JOURNAL_BRIDGE
    )
    assert bridge.database_role is RealtimeDatabaseRole.COMPUTE


def test_canonical_rollout_modes_keep_developer_as_only_publisher():
    developer = _developer()
    advanced = _advanced()

    developer_only = build_equity_rollout_plan(
        EquityRolloutMode.DEVELOPER_ONLY,
        developer=developer,
    )
    advanced_only = build_equity_rollout_plan(
        EquityRolloutMode.ADVANCED_ONLY_REHEARSAL,
        advanced=advanced,
    )
    shadow = build_equity_rollout_plan(
        EquityRolloutMode.DEVELOPER_ACTIVE_ADVANCED_SHADOW,
        developer=developer,
        advanced=advanced,
    )

    assert {
        row.cohort_id
        for row in developer_only.assignments
        if row.publishes
    } == {developer.cohort_id}
    assert not any(row.publishes for row in advanced_only.assignments)
    assert {
        row.cohort_id
        for row in shadow.assignments
        if row.publishes
    } == {developer.cohort_id}
    assert RealtimeWorkerKind.DETECTOR_RUNNER not in _kinds(
        shadow,
        advanced.cohort_id,
    )


def test_worker_database_plans_use_one_runtime_connection_and_minimum_roles():
    developer = _developer()
    plan = build_tier_worker_plan(active=developer)
    assignments = {row.kind: row for row in plan.assignments}

    assert RUNTIME_WORKER_POOL_BUDGET.minimum_connections == 1
    assert RUNTIME_WORKER_POOL_BUDGET.maximum_connections == 1
    assert assignments[
        RealtimeWorkerKind.DEVELOPER_STOCK_ACQUISITION
    ].database_role is RealtimeDatabaseRole.INGRESS
    assert assignments[
        RealtimeWorkerKind.DETECTOR_RUNNER
    ].database_role is RealtimeDatabaseRole.COMPUTE
    assert assignments[
        RealtimeWorkerKind.PORTAL_PUBLISHER
    ].database_role is RealtimeDatabaseRole.PUBLICATION
    assert all(
        process_plan.principal is RealtimeDatabasePrincipal.RUNTIME
        for process_plan in plan.database_plans
    )


def test_worker_and_api_budgets_have_a_preconstruction_aggregate_ceiling():
    plan = build_tier_worker_plan(active=_developer(), warming=_advanced())
    api_plan = build_api_reader_database_plan()

    assert API_READER_POOL_BUDGET.minimum_connections == 2
    assert API_READER_POOL_BUDGET.maximum_connections == 20
    assert api_plan.role is RealtimeDatabaseRole.READER
    assert api_plan.principal is RealtimeDatabasePrincipal.API
    assert planned_process_connection_ceiling(
        (*plan.database_plans, api_plan),
        server_max_connections=60,
        reserved_connections=20,
    ) == 37