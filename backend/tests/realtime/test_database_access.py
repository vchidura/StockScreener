from unittest.mock import MagicMock

import pytest

from realtime.database_access import (
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
    planned_connection_ceiling,
    planned_process_connection_ceiling,
    wire_process_connection_provider,
)


def _pool_and_connection():
    cursor = MagicMock()
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    return pool, connection, cursor


def test_compute_transaction_uses_runtime_pool_local_role_and_one_commit():
    pool, connection, cursor = _pool_and_connection()
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {RealtimeDatabaseRole.COMPUTE: "stocks_realtime_compute"},
    )

    with provider.transaction(RealtimeDatabaseRole.COMPUTE) as unit_of_work:
        assert unit_of_work.connection is connection
        assert unit_of_work.cursor is cursor
        assert unit_of_work.principal is RealtimeDatabasePrincipal.RUNTIME
        assert not unit_of_work.read_only

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION READ WRITE"
    assert str(statements[1]).startswith("Composed(")
    assert "set_config('statement_timeout'" in statements[2]
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0
    pool.putconn.assert_called_once_with(connection, close=False)


def test_reader_transaction_uses_api_pool_and_cannot_be_read_write():
    pool, connection, cursor = _pool_and_connection()
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.API: pool},
        {RealtimeDatabaseRole.READER: "stocks_realtime_reader"},
    )

    with provider.transaction(
        RealtimeDatabaseRole.READER,
        dict_cursor=True,
    ) as unit_of_work:
        assert unit_of_work.read_only

    assert cursor.execute.call_args_list[0].args[0] == "SET TRANSACTION READ ONLY"
    with pytest.raises(ValueError, match="must be read-only"):
        with provider.transaction(RealtimeDatabaseRole.READER, read_only=False):
            pass


def test_reader_repeatable_read_is_configured_before_role_or_queries():
    pool, _, cursor = _pool_and_connection()
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.API: pool},
        {RealtimeDatabaseRole.READER: "stocks_realtime_reader"},
    )

    with provider.transaction(
        RealtimeDatabaseRole.READER,
        isolation_level="REPEATABLE READ",
    ):
        pass

    assert cursor.execute.call_args_list[0].args[0] == (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    )


def test_transaction_failure_rolls_back_and_returns_connection():
    pool, connection, _ = _pool_and_connection()
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {RealtimeDatabaseRole.PUBLICATION: "stocks_realtime_publication"},
    )

    with pytest.raises(RuntimeError, match="failed"):
        with provider.transaction(RealtimeDatabaseRole.PUBLICATION):
            raise RuntimeError("failed")

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1
    pool.putconn.assert_called_once_with(connection, close=False)


def test_unconfigured_role_is_denied_before_connection_checkout():
    pool, _, _ = _pool_and_connection()
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {RealtimeDatabaseRole.COMPUTE: "stocks_realtime_compute"},
    )

    with pytest.raises(ValueError, match="not configured"):
        with provider.transaction(RealtimeDatabaseRole.PUBLICATION):
            pass

    pool.getconn.assert_not_called()


def test_connection_ceiling_is_explicit_and_fails_before_pool_construction():
    counts = {
        RealtimeDatabasePrincipal.RUNTIME: 6,
        RealtimeDatabasePrincipal.API: 1,
    }
    budgets = {
        RealtimeDatabasePrincipal.RUNTIME: RealtimePoolBudget(1, 2),
        RealtimeDatabasePrincipal.API: RealtimePoolBudget(1, 4),
    }

    assert planned_connection_ceiling(
        counts,
        budgets,
        server_max_connections=30,
        reserved_connections=10,
    ) == 16
    with pytest.raises(ValueError, match="exceeds PostgreSQL capacity"):
        planned_connection_ceiling(
            counts,
            budgets,
            server_max_connections=20,
            reserved_connections=5,
        )


def test_per_process_budgets_are_summed_before_pool_wiring():
    plans = (
        RealtimeProcessDatabasePlan(
            "fast-materializer:active",
            RealtimeDatabaseRole.COMPUTE,
            RealtimePoolBudget(1, 1),
        ),
        RealtimeProcessDatabasePlan(
            "portal-publisher:active",
            RealtimeDatabaseRole.PUBLICATION,
            RealtimePoolBudget(1, 1),
        ),
        RealtimeProcessDatabasePlan(
            "portal-api",
            RealtimeDatabaseRole.READER,
            RealtimePoolBudget(2, 20),
        ),
    )

    assert planned_process_connection_ceiling(
        plans,
        server_max_connections=40,
        reserved_connections=10,
    ) == 22
    with pytest.raises(ValueError, match="exceeds PostgreSQL capacity"):
        planned_process_connection_ceiling(
            plans,
            server_max_connections=30,
            reserved_connections=10,
        )


def test_process_plan_wires_only_its_injected_principal_and_role():
    pool, connection, cursor = _pool_and_connection()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    plan = RealtimeProcessDatabasePlan(
        "fast-materializer:active",
        RealtimeDatabaseRole.COMPUTE,
        RealtimePoolBudget(1, 1),
    )
    provider = wire_process_connection_provider(
        plan,
        principal_pool=pool,
        role_name="stocks_realtime_compute",
    )

    with provider.transaction(plan.role) as unit_of_work:
        assert unit_of_work.principal is RealtimeDatabasePrincipal.RUNTIME

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION READ ONLY"
    assert statements[2] == "SELECT current_database()"
    assert "FROM rt.database_identity" in statements[4]
    assert cursor.execute.call_count == 8
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 1
    assert pool.putconn.call_count == 2


@pytest.mark.parametrize(
    ("rows", "message"),
    (
        ((("stocks_db_fresh",),), "wrong database"),
        (
            (
                ("stocks_realtime_v1",),
                ("wrong_lineage", "stocks_realtime_v1"),
            ),
            "identity does not match",
        ),
    ),
)
def test_process_pool_wiring_rejects_wrong_database_or_lineage(rows, message):
    pool, connection, cursor = _pool_and_connection()
    cursor.fetchone.side_effect = rows
    plan = RealtimeProcessDatabasePlan(
        "fast-materializer:active",
        RealtimeDatabaseRole.COMPUTE,
        RealtimePoolBudget(1, 1),
    )

    with pytest.raises(RuntimeError, match=message):
        wire_process_connection_provider(
            plan,
            principal_pool=pool,
            role_name="stocks_realtime_compute",
        )

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1
    pool.putconn.assert_called_once_with(connection, close=False)