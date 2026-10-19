"""P6D4 migration 029 qualification on one exact disposable V2 database."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from psycopg2 import errors
from psycopg2.pool import ThreadedConnectionPool

import test_advanced_options_flow_persistence_v2_db as base
from advanced_options_rolling_support import NOW, rolling_state
from realtime.advanced_options_rolling_repository import (
    PostgresAdvancedOptionsRollingStateRepository,
)
from realtime.relation_registry import API_ROLE, APP_ROLE
from realtime.security_v2 import build_role_names_v2, harden_database_v2
from v2_database import _connect, _settings, requires_v2_database


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_options_p6d4"
ROLE_PREFIX = "test_rt_options_p6d4"
GENERATION = UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture(scope="module")
def p6d4_database():
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    migrations = tuple(
        migration
        for migration in base.load_realtime_migrations(
            base.V2_MIGRATIONS_DIR
        )
        if migration.version
        <= "029_advanced_options_rolling_checkpoints"
    )
    assert migrations[-1].version == (
        "029_advanced_options_rolling_checkpoints"
    )
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
def rolling_repository(p6d4_database):
    return PostgresAdvancedOptionsRollingStateRepository(
        _AppProvider(p6d4_database)
    )


def _state(database, **changes):
    return rolling_state(
        cohort_id=database.cohort_id,
        journal_generation=GENERATION,
        **changes,
    )


def test_p6d4db_01_migration_applies_once_and_hardens(p6d4_database):
    migration = p6d4_database.migrations[-1]
    assert p6d4_database.resume_report["applied_now"] == [
        migration.version
    ]
    assert p6d4_database.replay_report["applied_now"] == []
    assert all(
        status.startswith("PASS")
        for _, status in p6d4_database.security_audit
    )


def test_p6d4db_02_repository_replay_and_restart_are_exact(
    p6d4_database,
    rolling_repository,
):
    state = _state(p6d4_database)

    assert rolling_repository.put(state) == state
    assert rolling_repository.put(state) == state
    assert rolling_repository.load_state(state.state_id) == state
    assert rolling_repository.load_latest(
        cohort_id=state.cohort_id,
        session_date=state.session_date,
        journal_generation=state.journal_generation,
    ) == state


def test_p6d4db_03_latest_orders_owner_epoch_then_offset(
    p6d4_database,
    rolling_repository,
):
    second = _state(
        p6d4_database,
        committed_offset=12,
        committed_at=NOW + timedelta(seconds=1),
    )
    next_owner = _state(
        p6d4_database,
        owner_epoch=4,
        committed_offset=13,
        committed_at=NOW + timedelta(seconds=2),
    )

    rolling_repository.put(second)
    rolling_repository.put(next_owner)

    assert rolling_repository.load_latest(
        cohort_id=next_owner.cohort_id,
        session_date=next_owner.session_date,
        journal_generation=next_owner.journal_generation,
    ) == next_owner


def test_p6d4db_04_divergence_immutability_and_api_denial(
    p6d4_database,
    rolling_repository,
):
    with pytest.raises(
        ValueError,
        match="persisted rolling checkpoint conflicts",
    ):
        rolling_repository.put(_state(
            p6d4_database,
            flow_checkpoint_sha256="9" * 64,
        ))
    with pytest.raises(errors.RaiseException):
        with base._transaction(p6d4_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE rt.advanced_options_rolling_checkpoints "
                "SET committed_at=committed_at + interval '1 second'"
            )
    with pytest.raises(errors.InsufficientPrivilege):
        with base._transaction(
            p6d4_database,
            API_ROLE,
            read_only=True,
        ) as cursor:
            cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_rolling_checkpoints"
            )
