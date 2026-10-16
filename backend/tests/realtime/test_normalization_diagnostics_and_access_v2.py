"""RT-05 diagnostics persistence and V2 database access (offline fakes plus an opt-in DB variant)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
import pytest

from realtime.database_access_v2 import (
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
    pools_required,
    verify_pool_target_v2,
)
from realtime.normalization_diagnostics_repository import (
    diagnostic_id,
    load_normalization_diagnostics,
    persist_normalization_diagnostics,
)
from realtime.option_pre_session_producer import PreSessionNormalizationDiagnostics
from realtime.stage_graph import STAGES_V1

from v2_database import db_tests_enabled, disposable_v2_database, insert_cohort

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


def _diagnostic(candidates=0, reasons=(("OPTION_SPOT_SKEW", 4132),), **overrides):
    values = dict(
        underlyer="SPY", bar_acquisition="FETCHED", requested_from=NOW - timedelta(hours=6), requested_to=NOW,
        returned_from=NOW - timedelta(hours=6), returned_to=NOW, bar_count=380, pages_fetched=3,
        pagination_complete=True, price_basis="RAW_FINALIZED_MINUTE_CLOSE_V1", observation_min=NOW - timedelta(hours=5),
        observation_max=NOW - timedelta(minutes=2), raw_rows=4132, usable_mark_count=4132, candidates=candidates,
        rejection_counts=tuple(reasons))
    values.update(overrides)
    return PreSessionNormalizationDiagnostics(**values)


class _Cursor:
    def __init__(self, rowcounts):
        self.calls, self._rowcounts, self.rowcount = [], list(rowcounts), 0

    def execute(self, statement, parameters=()):
        self.calls.append((statement, parameters))
        self.rowcount = self._rowcounts.pop(0) if self._rowcounts else 1


class _Pool:
    def __init__(self, rows, closed=False):
        self.rows, self.returned = rows, []
        self.connection = _Connection(rows)

    def getconn(self):
        return self.connection

    def putconn(self, connection, close=False):
        self.returned.append(connection)


class _Connection:
    closed = False

    def __init__(self, rows):
        self.rows, self.statements, self.committed, self.rolled_back = list(rows), [], 0, 0

    def cursor(self):
        return _ConnectionCursor(self)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


class _ConnectionCursor:
    def __init__(self, connection):
        self.connection = connection
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, parameters=()):
        text = statement if isinstance(statement, str) else str(statement)
        self.connection.statements.append((text, parameters))
        self._result = next((rows for marker, rows in self.connection.rows if marker in text), [])

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result or [])


def test_diagnostic_identity_is_stable_and_depends_on_the_stage_run_and_content():
    cohort, run = uuid4(), uuid4()
    base = _diagnostic()
    assert diagnostic_id(cohort, run, base) == diagnostic_id(cohort, run, _diagnostic())
    assert diagnostic_id(cohort, run, base) != diagnostic_id(cohort, uuid4(), base)
    assert diagnostic_id(cohort, run, base) != diagnostic_id(cohort, run, _diagnostic(raw_rows=1))


def test_persist_writes_each_diagnostic_once_with_bounded_content():
    cursor = _Cursor([1, 0])
    cohort, run, request = uuid4(), uuid4(), uuid4()
    written = persist_normalization_diagnostics(
        cursor, cohort, [_diagnostic(), _diagnostic()], created_at=NOW, stage_run_id=run,
        request_ids_by_underlyer={"SPY": [request]})
    assert written == 1  # the second insert conflicted and was skipped
    statement, parameters = cursor.calls[0]
    assert "ON CONFLICT (diagnostic_id, created_at) DO NOTHING" in statement
    assert parameters[3] == "SPY" and parameters[4] == [str(request)]
    assert parameters[-1] == NOW and parameters[-3].adapted == {"OPTION_SPOT_SKEW": 4132}
    assert parameters[-2].adapted == []


def test_persist_requires_an_aware_clock_and_never_stores_a_url_or_credential():
    with pytest.raises(ValueError):
        persist_normalization_diagnostics(_Cursor([]), uuid4(), [_diagnostic()], created_at=datetime(2026, 10, 12))
    with pytest.raises(ValueError, match="rejection reasons"):
        _diagnostic(reasons=())
    cursor = _Cursor([1])
    persist_normalization_diagnostics(cursor, uuid4(), [_diagnostic()], created_at=NOW)
    flattened = repr(cursor.calls).lower()
    assert "apikey" not in flattened and "http" not in flattened


def test_access_config_validates_modes():
    assert V2AccessConfig().security_mode == "deferred"
    with pytest.raises(ValueError):
        V2AccessConfig(security_mode="relaxed")
    with pytest.raises(ValueError, match="role prefix"):
        V2AccessConfig(security_mode="hardened")
    assert V2AccessConfig(security_mode="hardened", role_prefix="stocks_realtime_v2").role_prefix


def test_pools_required_follows_enabled_stages_and_security_mode():
    equity = {name for name, stage in STAGES_V1.items() if stage.domain != "options"}
    assert pools_required(equity, V2AccessConfig()) == frozenset({OWNER_POOL})
    hardened = V2AccessConfig(security_mode="hardened", role_prefix="stocks_realtime_v2")
    # One app pool serves every stage, Options included; the api pool belongs to the HTTP process.
    assert pools_required(equity, hardened) == frozenset({"app"})
    assert pools_required(STAGES_V1, hardened) == frozenset({"app"})
    assert pools_required(set(), hardened) == frozenset() == pools_required(set(), V2AccessConfig())


def test_connection_budget_enforces_the_headroom_over_the_whole_deployment():
    from realtime.database_access_v2 import (
        orchestrator_pool_size,
        plan_connection_budget,
        verify_server_connection_budget,
    )

    assert plan_connection_budget({"orchestrator": 6, "other": 8}, server_max_connections=100,
                                  reserved_connections=5) == 66  # 70% of 95 usable, rounded down
    with pytest.raises(ValueError, match="exceed"):
        plan_connection_budget({"orchestrator": 60, "other": 7}, server_max_connections=100, reserved_connections=5)
    for bad in ({}, {"a": 0}):
        with pytest.raises(ValueError):
            plan_connection_budget(bad, server_max_connections=100, reserved_connections=5)
    with pytest.raises(ValueError):
        plan_connection_budget({"a": 1}, server_max_connections=10, reserved_connections=10)
    assert orchestrator_pool_size(STAGES_V1) == 6  # two lanes, two resident loops, coordinator and heartbeat
    equity = {name for name, stage in STAGES_V1.items() if stage.domain != "options"}
    assert orchestrator_pool_size(equity) == 4
    pool = _Pool([("max_connections", [(100, 3)])])
    assert verify_server_connection_budget(pool, {"orchestrator": 6}, reserved_connections=5) == 66
    with pytest.raises(ValueError):
        verify_server_connection_budget(_Pool([("max_connections", [(20, 3)])]), {"orchestrator": 11},
                                        reserved_connections=5)


def test_provider_pool_shape_is_validated_per_mode():
    config = V2AccessConfig()
    hardened = V2AccessConfig(security_mode="hardened", role_prefix="stocks_realtime_v2")
    with pytest.raises(ValueError):
        V2ConnectionProvider({}, config)
    with pytest.raises(ValueError, match="single owner pool"):
        V2ConnectionProvider({"app": object()}, config)
    with pytest.raises(ValueError, match="must not hold an owner pool"):
        V2ConnectionProvider({OWNER_POOL: object()}, hardened)
    with pytest.raises(ValueError, match="unknown"):
        V2ConnectionProvider({"nobody": object()}, config)
    with pytest.raises(ValueError, match="unknown"):
        V2ConnectionProvider({"runtime_data": object()}, hardened)
    assert V2ConnectionProvider({"app": object(), "api": object()}, hardened)


def test_deferred_transaction_uses_the_owner_pool_without_switching_roles():
    pool = _Pool([])
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig())
    with provider.transaction() as unit:
        unit.cursor.execute("SELECT 1")
    statements = [text for text, _ in pool.connection.statements]
    assert not any("SET LOCAL ROLE" in text for text in statements)
    assert pool.connection.committed == 1 and unit.role == "app"
    with provider.transaction("api", read_only=True) as unit:
        assert unit.role == "api"
    with pytest.raises(ValueError, match="unknown database role"):
        with provider.transaction("compute"):
            pass
    with pytest.raises(ValueError, match="read-only"):
        with provider.transaction("api"):
            pass


def test_hardened_transaction_sets_the_app_or_api_role_on_its_own_pool():
    app_pool, api_pool = _Pool([]), _Pool([])
    config = V2AccessConfig(security_mode="hardened", role_prefix="stocks_realtime_v2")
    provider = V2ConnectionProvider({"app": app_pool, "api": api_pool}, config)
    with provider.transaction(read_only=True):
        pass
    statements = [text for text, _ in app_pool.connection.statements]
    assert any("SET LOCAL ROLE" in text and "stocks_realtime_v2_app" in text for text in statements)
    assert any("READ ONLY" in text for text in statements)
    with provider.transaction("api", read_only=True):
        pass
    assert any("SET LOCAL ROLE" in text and "stocks_realtime_v2_api" in text
               for text, _ in api_pool.connection.statements)
    assert not any("stocks_realtime_v2_api" in text for text, _ in app_pool.connection.statements)
    with pytest.raises(ValueError, match="read-only"):
        with provider.transaction("api"):
            pass
    only_app = V2ConnectionProvider({"app": _Pool([])}, config)
    with pytest.raises(RuntimeError, match="no v2 pool"):
        with only_app.transaction("api", read_only=True):
            pass


def test_pool_verification_gates_on_database_identity_and_security_phase():
    def pool_for(database, identity, phase):
        return _Pool([("current_database", [(database,)]), ("rt.database_identity", identity),
                      ("security_phase_events", [(phase,)] if phase else [])])

    good = [("advanced_realtime_v2", "test_realtime_v2_x")]
    config = V2AccessConfig(allow_disposable=True)
    assert verify_pool_target_v2(pool_for("test_realtime_v2_x", good, "DEFERRED"), config) == "test_realtime_v2_x"
    with pytest.raises(RuntimeError, match="wrong database"):
        verify_pool_target_v2(pool_for("stocks_realtime_v1", good, "DEFERRED"), config)
    with pytest.raises(RuntimeError, match="wrong database"):
        verify_pool_target_v2(pool_for("test_realtime_v2_x", good, "DEFERRED"), V2AccessConfig())
    with pytest.raises(RuntimeError, match="identity"):
        verify_pool_target_v2(pool_for("test_realtime_v2_x", [("advanced_realtime_v1", "test_realtime_v2_x")],
                                       "DEFERRED"), config)
    with pytest.raises(RuntimeError, match="requires security phase DEFERRED"):
        verify_pool_target_v2(pool_for("test_realtime_v2_x", good, "AUDITED"), config)
    hardened = V2AccessConfig(security_mode="hardened", role_prefix="stocks_realtime_v2", allow_disposable=True)
    with pytest.raises(RuntimeError, match="requires security phase AUDITED"):
        verify_pool_target_v2(pool_for("test_realtime_v2_x", good, "APPLIED"), hardened)
    assert verify_pool_target_v2(pool_for("test_realtime_v2_x", good, "AUDITED"), hardened)


def test_a_hardened_pool_verifies_under_the_role_it_logs_in_as():
    good = [("advanced_realtime_v2", "test_realtime_v2_x")]
    hardened = V2AccessConfig(security_mode="hardened", role_prefix="stocks_realtime_v2", allow_disposable=True)

    def verified_statements(role):
        pool = _Pool([("current_database", [("test_realtime_v2_x",)]), ("rt.database_identity", good),
                      ("security_phase_events", [("AUDITED",)])])
        verify_pool_target_v2(pool, hardened, role)
        return [text for text, _ in pool.connection.statements if "SET LOCAL ROLE" in str(text)]

    assert any("stocks_realtime_v2_app" in str(text) for text in verified_statements("app"))
    assert any("stocks_realtime_v2_api" in str(text) for text in verified_statements("api"))
    # Without a role (or in deferred mode) no role is assumed.
    assert verified_statements(None) == []
    deferred = V2AccessConfig(allow_disposable=True)
    pool = _Pool([("current_database", [("test_realtime_v2_x",)]), ("rt.database_identity", good),
                  ("security_phase_events", [("DEFERRED",)])])
    verify_pool_target_v2(pool, deferred, "app")
    assert not any("SET LOCAL ROLE" in str(text) for text, _ in pool.connection.statements)


@pytest.fixture(scope="module")
def database():
    if not db_tests_enabled():
        pytest.skip("DB-backed variant is opt-in (STOCKS_V2_DB_TESTS=1, no offline I/O guard)")
    with disposable_v2_database("test_realtime_v2_b2") as pool:
        yield pool


def test_partition_maintenance_creates_ahead_and_alarms_when_creation_is_missed(database):
    from datetime import date as date_type

    from realtime.partition_maintenance import ensure_partitions_and_check, partition_alarms

    provider = V2ConnectionProvider({OWNER_POOL: database}, V2AccessConfig(allow_disposable=True))
    today = date_type.today()
    with provider.transaction() as unit:
        created, alarms = ensure_partitions_and_check(unit.cursor, today)
    assert created >= 0 and alarms == []
    with provider.transaction(read_only=True) as unit:
        missed = partition_alarms(unit.cursor, today + timedelta(days=30))
    from realtime.relation_registry import PARTITIONED_RELATIONS

    assert len(missed) == len(PARTITIONED_RELATIONS) and all(alarm.days_ahead < 7 for alarm in missed)


def test_diagnostics_round_trip_on_the_real_table_and_database_rules_hold(database):
    pool = database
    provider = V2ConnectionProvider({OWNER_POOL: pool}, V2AccessConfig(allow_disposable=True))
    assert provider.verify().startswith("test_realtime_v2_")
    with provider.transaction() as unit:
        cohort = insert_cohort(unit.cursor, NOW)
    created_at = datetime.now(timezone.utc)
    with provider.transaction() as unit:
        assert persist_normalization_diagnostics(unit.cursor, cohort, [_diagnostic()], created_at=created_at) == 1
    with provider.transaction() as unit:
        assert persist_normalization_diagnostics(unit.cursor, cohort, [_diagnostic()], created_at=created_at) == 0
    with provider.transaction(read_only=True) as unit:
        rows = load_normalization_diagnostics(unit.cursor, cohort, "spy")
    assert len(rows) == 1
    assert rows[0]["rejection_counts"] == {"OPTION_SPOT_SKEW": 4132} and rows[0]["pages_fetched"] == 3
    assert rows[0]["bar_acquisition"] == "FETCHED" and rows[0]["usable_mark_count"] == 4132
    # The database rejects a zero-candidate row without reasons even if the Python guard is bypassed.
    with pytest.raises(psycopg2.errors.CheckViolation):
        with provider.transaction() as unit:
            unit.cursor.execute(
                "INSERT INTO rt.normalization_diagnostics (diagnostic_id, cohort_id, underlyer, bar_count, "
                "pages_fetched, pagination_complete, bar_acquisition, price_basis, raw_rows, usable_mark_count, "
                "candidates) VALUES (%s,%s,'SPY',1,1,true,'FETCHED','NOT_APPLICABLE',1,1,0)",
                (str(uuid4()), str(cohort)))
    with pytest.raises(psycopg2.errors.RaiseException):
        with provider.transaction() as unit:
            unit.cursor.execute("UPDATE rt.normalization_diagnostics SET raw_rows = raw_rows + 1")
