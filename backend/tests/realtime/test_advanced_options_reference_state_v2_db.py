"""P6D3 migration 028 qualification on one exact disposable V2 database."""
from __future__ import annotations

from contextlib import closing, contextmanager
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from psycopg2 import errors
from psycopg2.pool import ThreadedConnectionPool

import test_advanced_options_flow_persistence_v2_db as base
from advanced_options_reference_support import KNOWN, SESSION, reference_state
from option_flow_support import CALL, advanced_contract_scope, advanced_policy
from realtime.advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
)
from realtime.option_source_flow import (
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
)
from realtime.relation_registry import API_ROLE, APP_ROLE
from realtime.security_v2 import build_role_names_v2, harden_database_v2
from v2_database import _connect, _settings, requires_v2_database


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_options_p6d3"
ROLE_PREFIX = "test_rt_options_p6d3"


@pytest.fixture(scope="module")
def p6d3_database():
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    migrations = tuple(
        migration
        for migration in base.load_realtime_migrations(
            base.V2_MIGRATIONS_DIR
        )
        if migration.version <= "028_advanced_options_reference_state"
    )
    assert migrations[-1].version == "028_advanced_options_reference_state"
    original = base.DATABASE, base.ROLE_PREFIX
    base.DATABASE, base.ROLE_PREFIX = DATABASE, ROLE_PREFIX
    pool = None
    try:
        base._assert_target_absent(settings, roles)
        staged_versions = base._create_staged_database(settings, migrations)
        argv = [
            "--database", DATABASE,
            "--confirm-database-name", DATABASE,
            "--allow-disposable",
            "--resume",
            "--execute",
        ]
        resume_report = base._run_provision(argv)
        replay_report = base._run_provision(argv)
        with closing(_connect(settings, DATABASE)) as connection:
            roles_created, security_audit = harden_database_v2(
                connection,
                roles,
            )
        pool = ThreadedConnectionPool(
            1,
            8,
            dbname=DATABASE,
            user=settings["POSTGRES_ADMIN_USER"],
            password=settings["POSTGRES_ADMIN_PASSWORD"],
            host=settings["DB_HOST"],
            port=settings["DB_PORT"],
            options="-c timezone=UTC",
        )
        database = base.P6D1Database(
            pool,
            roles,
            migrations,
            staged_versions,
            resume_report,
            replay_report,
            tuple(roles_created),
            tuple(security_audit),
            UUID(int=0),
        )
        database.cohort_id = base._seed(database)
        yield database
    finally:
        if pool is not None:
            pool.closeall()
        base._drop_target(settings, roles)
        base.DATABASE, base.ROLE_PREFIX = original


class _AppProvider:
    def __init__(self, database):
        self.database = database

    @contextmanager
    def transaction(self, *, read_only=False):
        with base._transaction(
            self.database,
            APP_ROLE,
            read_only=read_only,
        ) as cursor:
            yield SimpleNamespace(cursor=cursor)


@pytest.fixture(scope="module")
def stored_reference_state(p6d3_database):
    policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    with base._transaction(p6d3_database, APP_ROLE) as cursor:
        persist_option_flow_policy_cursor(cursor, policy)
        persist_option_contract_scope_cursor(cursor, scope)
    repository = PostgresAdvancedOptionsReferenceStateRepository(
        _AppProvider(p6d3_database)
    )
    state = reference_state(contract_scope_sha256=scope.sha256)
    assert repository.put(state) == state
    assert repository.put(state) == state
    return repository, state, scope


def test_p6d3db_01_migration_applies_once_and_hardens(p6d3_database):
    migration = p6d3_database.migrations[-1]
    assert p6d3_database.resume_report["applied_now"] == [
        migration.version
    ]
    assert p6d3_database.replay_report["applied_now"] == []
    assert all(
        status.startswith("PASS")
        for _, status in p6d3_database.security_audit
    )


def test_p6d3db_02_repository_round_trip_is_exact(
    p6d3_database,
    stored_reference_state,
):
    repository, state, _ = stored_reference_state

    assert repository.load_generation(7) == state
    assert repository.load_for_session(SESSION, known_at=KNOWN) == state
    with base._transaction(
        p6d3_database,
        APP_ROLE,
        read_only=True,
    ) as cursor:
        cursor.execute(
            "SELECT member_count FROM "
            "rt.advanced_options_reference_states"
        )
        assert cursor.fetchone()[0] == len(state.member_identities)
        cursor.execute(
            "SELECT count(*) FROM "
            "rt.advanced_options_reference_state_members"
        )
        assert cursor.fetchone()[0] == len(state.member_identities)


def test_p6d3db_03_generation_and_session_freeze_are_immutable(
    p6d3_database,
    stored_reference_state,
):
    repository, state, scope = stored_reference_state
    cycle = UUID("77777777-7777-7777-7777-777777777777")
    corrected = reference_state(
        generation=8,
        short_interest=Decimal("0.09"),
        contract_scope_sha256=scope.sha256,
    )

    repository.freeze_for_cycle(
        state,
        cycle_id=cycle,
        consumed_at=KNOWN,
    )
    repository.freeze_for_cycle(
        state,
        cycle_id=cycle,
        consumed_at=KNOWN,
    )
    repository.put(corrected)

    assert repository.load_generation(8) == corrected
    assert repository.load_for_session(SESSION, known_at=KNOWN) == state
    with pytest.raises(
        ValueError,
        match="session reference state is frozen",
    ):
        repository.freeze_for_cycle(
            corrected,
            cycle_id=UUID("88888888-8888-8888-8888-888888888888"),
            consumed_at=KNOWN,
        )
    with base._transaction(
        p6d3_database,
        APP_ROLE,
        read_only=True,
    ) as cursor:
        cursor.execute(
            "SELECT count(*) FROM "
            "rt.advanced_options_reference_session_freezes"
        )
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            "SELECT count(*) FROM "
            "rt.advanced_options_reference_cycle_bindings"
        )
        assert cursor.fetchone()[0] == 1


def test_p6d3db_04_divergence_immutability_and_api_denial(
    p6d3_database,
    stored_reference_state,
):
    repository, _, scope = stored_reference_state
    with pytest.raises(ValueError, match="persisted reference state conflicts"):
        repository.put(reference_state(
            short_interest=Decimal("0.09"),
            contract_scope_sha256=scope.sha256,
        ))
    with pytest.raises(errors.RaiseException):
        with base._transaction(p6d3_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE rt.advanced_options_reference_states "
                "SET valid_until=valid_until + interval '1 second'"
            )
    with pytest.raises(errors.InsufficientPrivilege):
        with base._transaction(
            p6d3_database,
            API_ROLE,
            read_only=True,
        ) as cursor:
            cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_reference_states"
            )
