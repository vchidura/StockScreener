from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from realtime.contracts import CANARY_UNIVERSE
from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.option_manifest_repository import option_manifest_selection_id
from realtime.option_pre_session_producer import OptionPreSessionCollection
from realtime.option_pre_session_target import (
    OptionPreSessionTargetPlan,
    TargetOptionPreSessionRunner,
    load_option_pre_session_request_cursor,
    persist_option_pre_session_package_cursor,
)
import realtime.option_pre_session_target as option_pre_session_target
from realtime.preflight import ProviderTier
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_option_pre_session_producer import (
    FakeSource,
    _packages,
    _security_id,
)
from tests.realtime.test_option_session_manifest import (
    MARKET_TIME,
    SESSION_DATE,
    _candidates,
    _developer_policy,
    _scope,
)


def _target_plan():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    return OptionPreSessionTargetPlan(
        scope=scope,
        policy=_developer_policy(),
        session_date=SESSION_DATE,
        scheduled_cycle=MARKET_TIME,
        effective_at=MARKET_TIME - timedelta(minutes=30),
        maximum_provider_requests=320,
        maximum_response_bytes=8 * 67_108_864,
    )


def _request_for(plan):
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        plan.scope.profile_sha256,
        plan.scope.provider_tier.value,
        plan.scope.universe.sha256,
    )
    cursor.fetchall.return_value = [
        (ticker, str(_security_id(ticker)))
        for ticker in plan.scope.universe.option_underlyings
    ]
    return load_option_pre_session_request_cursor(cursor, plan)


def _collection(plan, *, omit_last=False):
    candidates = _candidates(
        plan.scope.universe,
        plan.policy.target_dtes,
        plan.policy.strikes_per_side_per_expiry,
    )
    packages = _packages(plan.scope, plan.policy, candidates)
    if omit_last:
        packages = packages[:-1]
    return OptionPreSessionCollection(
        packages,
        len(packages),
        sum(package.evidence.response_bytes for package in packages),
    )


def _wired_database(role):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    database = RealtimeProcessDatabase.wire(
        RealtimeProcessDatabasePlan(
            f"worker:option_pre_session:{role.value.lower()}",
            role,
            RealtimePoolBudget(1, 1),
        ),
        principal_pool=pool,
        role_name=f"stocks_realtime_{role.value.lower()}",
    )
    cursor.reset_mock()
    connection.reset_mock()
    pool.reset_mock()
    cursor.fetchone.side_effect = None
    connection.closed = False
    return database, connection, cursor


def test_target_loader_reconciles_profile_and_exact_security_identities():
    plan = _target_plan()
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        plan.scope.profile_sha256,
        plan.scope.provider_tier.value,
        plan.scope.universe.sha256,
    )
    cursor.fetchall.return_value = [
        (ticker, str(_security_id(ticker)))
        for ticker in plan.scope.universe.option_underlyings
    ]

    request = load_option_pre_session_request_cursor(cursor, plan)

    assert request.scope == plan.scope
    assert request.underlying_security_ids == tuple(
        (ticker, _security_id(ticker))
        for ticker in plan.scope.universe.option_underlyings
    )
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("FROM rt.capability_profiles" in row for row in statements)
    assert any("FROM ref.security_revisions" in row for row in statements)


def test_runner_uses_read_only_loader_then_atomic_references_and_control_stages():
    plan = _target_plan()
    collection = _collection(plan)
    source = FakeSource(collection)
    references, references_connection, references_cursor = _wired_database(
        RealtimeDatabaseRole.REFERENCES
    )
    control, control_connection, control_cursor = _wired_database(
        RealtimeDatabaseRole.CONTROL
    )
    events = []

    def loader(cursor, active_plan):
        assert cursor is references_cursor
        events.append("LOAD")
        return _request_for(active_plan)

    def package_repository(cursor, package):
        assert cursor is references_cursor
        events.append(f"DISCOVERY:{package.evidence.result.batch.underlyer}")

    def selection_repository(cursor, selection, *, effective_at):
        assert cursor is control_cursor
        assert effective_at == plan.effective_at
        events.append(f"SELECTION:{selection.status.value}")
        return option_manifest_selection_id(selection)

    runner = TargetOptionPreSessionRunner(
        references,
        control,
        source,
        package_repository=package_repository,
        selection_repository=selection_repository,
        loader=loader,
    )

    result = runner.run_once(plan)

    assert result.plan.selected_contract_count == 128
    assert events[0] == "LOAD"
    assert events[1:-1] == [
        f"DISCOVERY:{ticker}" for ticker in plan.scope.universe.option_underlyings
    ]
    assert events[-1] == "SELECTION:READY"
    assert references_connection.commit.call_count == 2
    assert control_connection.commit.call_count == 1
    reference_statements = [
        call.args[0] for call in references_cursor.execute.call_args_list
        if isinstance(call.args[0], str)
    ]
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY" in reference_statements


def test_partial_collection_fails_after_loader_before_write_transactions():
    plan = _target_plan()
    references, references_connection, _ = _wired_database(
        RealtimeDatabaseRole.REFERENCES
    )
    control, control_connection, _ = _wired_database(RealtimeDatabaseRole.CONTROL)
    package_repository = MagicMock()
    selection_repository = MagicMock()
    runner = TargetOptionPreSessionRunner(
        references,
        control,
        FakeSource(_collection(plan, omit_last=True)),
        package_repository=package_repository,
        selection_repository=selection_repository,
        loader=lambda cursor, active_plan: _request_for(active_plan),
    )

    with pytest.raises(ValueError, match="exact option universe"):
        runner.run_once(plan)

    package_repository.assert_not_called()
    selection_repository.assert_not_called()
    assert references_connection.commit.call_count == 1
    assert control_connection.commit.call_count == 0


def test_references_failure_rolls_back_and_never_opens_control_stage():
    plan = _target_plan()
    references, references_connection, _ = _wired_database(
        RealtimeDatabaseRole.REFERENCES
    )
    control, control_connection, _ = _wired_database(RealtimeDatabaseRole.CONTROL)
    selection_repository = MagicMock()
    runner = TargetOptionPreSessionRunner(
        references,
        control,
        FakeSource(_collection(plan)),
        package_repository=MagicMock(side_effect=RuntimeError("reference write failed")),
        selection_repository=selection_repository,
        loader=lambda cursor, active_plan: _request_for(active_plan),
    )

    with pytest.raises(RuntimeError, match="reference write failed"):
        runner.run_once(plan)

    assert references_connection.commit.call_count == 1
    assert references_connection.rollback.call_count == 1
    assert control_connection.commit.call_count == 0
    selection_repository.assert_not_called()


def test_control_failure_rolls_back_after_complete_references_commit():
    plan = _target_plan()
    references, references_connection, _ = _wired_database(
        RealtimeDatabaseRole.REFERENCES
    )
    control, control_connection, _ = _wired_database(RealtimeDatabaseRole.CONTROL)
    package_repository = MagicMock()
    runner = TargetOptionPreSessionRunner(
        references,
        control,
        FakeSource(_collection(plan)),
        package_repository=package_repository,
        selection_repository=MagicMock(side_effect=RuntimeError("control write failed")),
        loader=lambda cursor, active_plan: _request_for(active_plan),
    )

    with pytest.raises(RuntimeError, match="control write failed"):
        runner.run_once(plan)

    assert package_repository.call_count == 8
    assert references_connection.commit.call_count == 2
    assert references_connection.rollback.call_count == 0
    assert control_connection.commit.call_count == 0
    assert control_connection.rollback.call_count == 1


def test_runner_rejects_wrong_database_roles():
    references, _, _ = _wired_database(RealtimeDatabaseRole.REFERENCES)
    control, _, _ = _wired_database(RealtimeDatabaseRole.CONTROL)
    wrong, _, _ = _wired_database(RealtimeDatabaseRole.COMPUTE)
    source = FakeSource(_collection(_target_plan()))

    with pytest.raises(ValueError, match="references database role"):
        TargetOptionPreSessionRunner(wrong, control, source)
    with pytest.raises(ValueError, match="control database role"):
        TargetOptionPreSessionRunner(references, wrong, source)


def test_default_references_repository_persists_catalog_versions_and_instruments(
    monkeypatch,
):
    package = _collection(_target_plan()).packages[0]
    cursor = MagicMock()
    discovery_calls = []
    monkeypatch.setattr(
        option_pre_session_target,
        "persist_realtime_option_discovery_cursor",
        lambda active_cursor, evidence: discovery_calls.append(
            (active_cursor, evidence)
        ),
    )
    fetches = []
    for index, candidate in enumerate(package.candidates, start=1):
        catalog = candidate.catalog
        fetches.extend((
            (
                str(package.evidence.underlying_security_id),
                catalog.provider,
                catalog.contract_ticker,
            ),
            (catalog.contract_id, catalog.payload_sha256),
            (index,),
        ))
    cursor.fetchone.side_effect = fetches

    persist_option_pre_session_package_cursor(cursor, package)

    assert discovery_calls == [(cursor, package.evidence)]
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert sum("INSERT INTO ref.option_contracts" in row for row in statements) == 16
    assert sum("INSERT INTO ref.option_contract_versions" in row for row in statements) == 16
    assert sum("INSERT INTO rt.instruments" in row for row in statements) == 16
