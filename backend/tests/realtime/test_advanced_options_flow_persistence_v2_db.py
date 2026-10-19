"""P6D1 flow-evidence persistence on one exact disposable V2 database."""
from __future__ import annotations

import io
import json
from contextlib import closing, contextmanager, redirect_stdout
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg2 import errors, sql
from psycopg2.pool import ThreadedConnectionPool

from option_flow_support import (
    CALL,
    JOURNAL_GENERATION,
    T0,
    advanced_contract_scope,
    advanced_policy,
    config,
    quote,
    run,
    trade,
)
from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations
from realtime.option_flow.engine import OptionFlowEngine
from realtime.option_flow.records import EngineOutput
from realtime.option_flow.writer import (
    OptionFlowReplayDivergence,
    OptionFlowWriter,
)
from realtime.option_source_flow import (
    build_option_source_flow_identity,
    persist_option_source_flow_cursor,
)
from realtime.relation_registry import API_ROLE, APP_ROLE
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts import provision_realtime_v2_database as provision
from v2_database import _connect, _settings, requires_v2_database


pytestmark = requires_v2_database

UTC = timezone.utc
SESSION = date(2026, 10, 12)
NOW = datetime(2026, 10, 12, 15, 0, tzinfo=UTC)
DATABASE = "test_realtime_v2_options_p6d1"
ROLE_PREFIX = "test_rt_options_p6d1"
MANIFEST_SHA256 = "1" * 64
SLOT_SHA256 = "2" * 64
STREAM = UUID("61616161-7171-8181-9191-a1a1a1a1a1a1")


@dataclass
class P6D1Database:
    pool: ThreadedConnectionPool
    roles: object
    migrations: tuple
    staged_versions: tuple[str, ...]
    resume_report: dict[str, object]
    replay_report: dict[str, object]
    roles_created: tuple[str, ...]
    security_audit: tuple[tuple[str, str], ...]
    cohort_id: UUID


def _digest() -> str:
    return uuid4().hex + uuid4().hex


def _run_provision(argv: list[str]) -> dict[str, object]:
    output = io.StringIO()
    with redirect_stdout(output):
        result = provision.main(argv)
    report = json.loads(output.getvalue())
    if result != 0:
        raise RuntimeError(
            f"V2 provisioning failed with exit status {result}: {report}"
        )
    return report


def _assert_target_absent(settings, roles) -> None:
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datname=%s",
                (DATABASE,),
            )
            databases = cursor.fetchall()
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                (list(roles.all_names),),
            )
            role_rows = cursor.fetchall()
    if databases or role_rows:
        raise RuntimeError(
            "P6D1 disposable target is not clean: "
            f"databases={databases}, roles={role_rows}"
        )


def _create_staged_database(settings, migrations) -> tuple[str, ...]:
    with closing(_connect(settings, "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'"
                ).format(sql.Identifier(DATABASE))
            )
    staged = migrations[:-1]
    with closing(_connect(settings, DATABASE)) as connection:
        for migration in staged:
            with connection.cursor() as cursor:
                cursor.execute(migration.sql)
                cursor.execute(
                    "INSERT INTO rt.schema_migrations "
                    "(version,checksum_sha256) VALUES (%s,%s)",
                    (migration.version, migration.checksum_sha256),
                )
            connection.commit()
    return tuple(migration.version for migration in staged)


def _drop_target(settings, roles) -> None:
    with closing(_connect(settings, "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname=%s AND pid<>pg_backend_pid()",
                (DATABASE,),
            )
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(DATABASE)
                )
            )
            drop_disposable_roles_v2(connection, roles)
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datname=%s",
                (DATABASE,),
            )
            databases = cursor.fetchall()
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                (list(roles.all_names),),
            )
            role_rows = cursor.fetchall()
    assert databases == []
    assert role_rows == []


@contextmanager
def _transaction(
    database: P6D1Database,
    role: str | None = None,
    *,
    read_only: bool = False,
):
    connection = database.pool.getconn()
    try:
        with connection.cursor() as cursor:
            if read_only:
                cursor.execute("SET TRANSACTION READ ONLY")
            if role is not None:
                cursor.execute(
                    sql.SQL("SET LOCAL ROLE {}").format(
                        sql.Identifier(database.roles.name(role))
                    )
                )
            yield cursor
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        database.pool.putconn(connection)


def _seed(database: P6D1Database) -> UUID:
    profile_id = uuid4()
    manifest_id = uuid4()
    cohort_id = uuid4()
    with _transaction(database, APP_ROLE) as cursor:
        cursor.execute(
            "INSERT INTO rt.capability_profiles ("
            "profile_id,config_sha256,schema_sha256,account_alias,"
            "capabilities_sha256,universe_sha256,checked_at,expires_at,"
            "status,profile_sha256,provider_tier,security_policy_sha256,"
            "preflight_mode,provider_requests,action_authorized) VALUES "
            "(%s,%s,%s,'p6d1-fixture',%s,%s,%s,%s,'PASS',%s,"
            "'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
            (
                str(profile_id),
                _digest(),
                _digest(),
                _digest(),
                _digest(),
                NOW,
                NOW + timedelta(hours=1),
                _digest(),
                _digest(),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.subscription_manifests ("
            "manifest_id,manifest_sha256,universe_sha256,session_date,"
            "effective_at,profile_id,manifest_kind,stock_count,"
            "option_underlying_count,member_count,"
            "allocation_policy_sha256) VALUES "
            "(%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',9,9,9,%s)",
            (
                str(manifest_id),
                MANIFEST_SHA256,
                _digest(),
                SESSION,
                NOW,
                str(profile_id),
                _digest(),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.cohorts ("
            "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
            "model_bundle_sha256,valuation_sha256,effective_at,mode) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (
                str(cohort_id),
                _digest(),
                str(manifest_id),
                _digest(),
                _digest(),
                _digest(),
                NOW,
            ),
        )
        for relation in (
            "rt.option_flow_events",
            "rt.option_flow_event_profiles",
            "rt.option_flow_clusters",
            "rt.option_flow_cluster_members",
        ):
            cursor.execute(
                "SELECT rt.ensure_partitions(%s,%s)",
                (relation, SESSION),
            )
    return cohort_id


@pytest.fixture(scope="module")
def p6d1_database():
    assert DATABASE.startswith("test_realtime_v2_")
    assert ROLE_PREFIX.startswith("test_rt_")
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    migrations = tuple(
        migration
        for migration in load_realtime_migrations(V2_MIGRATIONS_DIR)
        if migration.version
        <= "026_advanced_options_flow_evidence"
    )
    assert migrations[-1].version == "026_advanced_options_flow_evidence"
    _assert_target_absent(settings, roles)
    pool = None
    try:
        staged_versions = _create_staged_database(settings, migrations)
        argv = [
            "--database",
            DATABASE,
            "--confirm-database-name",
            DATABASE,
            "--allow-disposable",
            "--resume",
            "--execute",
        ]
        resume_report = _run_provision(argv)
        replay_report = _run_provision(argv)
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
        database = P6D1Database(
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
        database.cohort_id = _seed(database)
        yield database
    finally:
        if pool is not None:
            pool.closeall()
        _drop_target(settings, roles)


def _advanced_output(database: P6D1Database):
    flow_policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    base = config(
        cohort_id=database.cohort_id,
        shard_count=1,
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=JOURNAL_GENERATION,
        stream_session_ids=(STREAM,),
        condition_semantics_sha256=base.semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=flow_policy.sha256,
        contract_scope_sha256=scope.sha256,
    )
    engine = OptionFlowEngine(replace(base, source_flow=source_flow))
    messages = [quote(seq=1, time_ms=T0 - 100)]
    messages += [
        trade(
            seq=index + 1,
            time_ms=T0 + index * 80,
            price=2.0,
            size=50,
            exchange=302 + index % 3,
        )
        for index in range(50)
    ]
    output = run(engine, messages, final=False)
    output.extend(engine.feed(
        trade(
            seq=100,
            time_ms=T0 + 4_500,
            price=2.0,
            size=50,
            exchange=302,
            conditions=(9011,),
        ),
        100,
    ))
    output.extend(engine.flush(final=True))
    return source_flow, scope, output


def _write_output(database: P6D1Database, output: EngineOutput):
    connection = database.pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SET LOCAL ROLE {}").format(
                    sql.Identifier(database.roles.name(APP_ROLE))
                )
            )
        writer = OptionFlowWriter(connection)
        writer.add(output, journal_generation=JOURNAL_GENERATION)
        return writer.flush()
    finally:
        database.pool.putconn(connection)


def test_p6d1p_01_migration_026_applies_once_and_hardens_exact_roles(
    p6d1_database,
):
    migration = p6d1_database.migrations[-1]
    assert p6d1_database.staged_versions == tuple(
        row.version for row in p6d1_database.migrations[:-1]
    )
    assert p6d1_database.resume_report["applied_now"] == [
        migration.version
    ]
    assert p6d1_database.replay_report["applied_now"] == []
    assert sorted(p6d1_database.roles_created) == sorted(
        p6d1_database.roles.all_names
    )
    assert all(
        status.startswith("PASS")
        for _, status in p6d1_database.security_audit
    )
    with _transaction(p6d1_database, read_only=True) as cursor:
        cursor.execute(
            "SELECT checksum_sha256 FROM rt.schema_migrations "
            "WHERE version=%s",
            (migration.version,),
        )
        assert cursor.fetchone() == (migration.checksum_sha256,)
        cursor.execute(
            "SELECT rolname,rolcanlogin FROM pg_roles "
            "WHERE rolname=ANY(%s) ORDER BY rolname",
            (list(p6d1_database.roles.all_names),),
        )
        assert cursor.fetchall() == [
            (name, False)
            for name in sorted(p6d1_database.roles.all_names)
        ]


def test_p6d1p_02_writer_persists_v3_profiles_members_and_replay(
    p6d1_database,
):
    source_flow, scope, output = _advanced_output(p6d1_database)
    with _transaction(p6d1_database, APP_ROLE) as cursor:
        persist_option_source_flow_cursor(
            cursor,
            source_flow,
            source_cohort_id=p6d1_database.cohort_id,
            contract_scope=scope,
        )
    report = _write_output(p6d1_database, output)
    assert report.events_inserted == len(output.events)
    assert report.clusters_inserted == len(output.clusters)

    with _transaction(p6d1_database, API_ROLE, read_only=True) as cursor:
        cursor.execute(
            "SELECT count(*) FROM rt.option_flow_events "
            "WHERE lineage_version=3"
        )
        event_count = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM rt.option_flow_event_profiles")
        profile_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*),count(DISTINCT event_id) "
            "FROM rt.option_flow_cluster_members"
        )
        member_count, distinct_events = cursor.fetchone()
        cursor.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema='rt' "
            "AND table_name='option_flow_cluster_members' "
            "AND column_name IN ('size','premium','cumulative_volume')"
        )
        copied_volume_columns = cursor.fetchone()[0]
    assert event_count == len(output.events)
    assert profile_count == sum(
        len(row.retention_thresholds) for row in output.events
    )
    assert member_count > 0
    assert distinct_events > 0
    assert copied_volume_columns == 0

    replay = _write_output(p6d1_database, output)
    assert replay.events_inserted == 0
    assert replay.clusters_inserted == 0

    event = output.events[0]
    changed = replace(
        event.retention_thresholds[0],
        minimum_contracts=(
            event.retention_thresholds[0].minimum_contracts + 1
        ),
    )
    divergent = EngineOutput()
    divergent.events.append(replace(
        event,
        retention_thresholds=(
            changed,
            *event.retention_thresholds[1:],
        ),
    ))
    with pytest.raises(OptionFlowReplayDivergence) as raised:
        _write_output(p6d1_database, divergent)
    assert raised.value.table == "rt.option_flow_event_profiles"


def test_p6d1p_03_dataset_identity_constraints_and_role_denials(
    p6d1_database,
):
    with _transaction(p6d1_database, APP_ROLE, read_only=True) as cursor:
        cursor.execute(
            "SELECT source_flow_id,advanced_flow_policy_sha256,"
            "contract_scope_sha256 FROM rt.option_source_flows "
            "WHERE identity_version=3"
        )
        source_flow_id, policy_sha256, scope_sha256 = cursor.fetchone()
    dataset_id = uuid4()
    with _transaction(p6d1_database, APP_ROLE) as cursor:
        cursor.execute(
            "INSERT INTO rt.detector_datasets ("
            "dataset_id,cohort_id,provider_tier,dataset_key,session_date,"
            "status,detector_ids,source_manifest_sha256,started_at,"
            "completed_at,summary,flow_identity_version,source_flow_id,"
            "advanced_flow_policy_sha256,contract_scope_sha256,slot_sha256,"
            "runtime_policy_sha256) VALUES "
            "(%s,%s,'MASSIVE_ADVANCED','p6d1-flow',%s,'COMPLETE',"
            "ARRAY['O1'],%s,%s,%s,'{}'::jsonb,2,%s,%s,%s,%s,%s)",
            (
                str(dataset_id),
                str(p6d1_database.cohort_id),
                SESSION,
                MANIFEST_SHA256,
                NOW,
                NOW,
                str(source_flow_id),
                policy_sha256,
                scope_sha256,
                SLOT_SHA256,
                "e" * 64,
            ),
        )
    with _transaction(p6d1_database, API_ROLE, read_only=True) as cursor:
        cursor.execute(
            "SELECT flow_identity_version,source_flow_id,"
            "contract_scope_sha256 FROM rt.detector_datasets "
            "WHERE dataset_id=%s",
            (str(dataset_id),),
        )
        assert cursor.fetchone() == (
            2,
            source_flow_id,
            scope_sha256,
        )

    with pytest.raises(errors.InsufficientPrivilege):
        with _transaction(p6d1_database, API_ROLE) as cursor:
            cursor.execute(
                "INSERT INTO rt.option_flow_contract_scopes ("
                "contract_scope_sha256,scope_version,session_date,"
                "manifest_sha256,slot_sha256,"
                "advanced_flow_policy_sha256) VALUES "
                "(%s,'advanced-options-contract-scope-v1',%s,%s,%s,%s)",
                (
                    "f" * 64,
                    SESSION,
                    MANIFEST_SHA256,
                    SLOT_SHA256,
                    policy_sha256,
                ),
            )
    with pytest.raises(errors.RaiseException, match="immutable"):
        with _transaction(p6d1_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE rt.option_flow_contract_scopes "
                "SET slot_sha256=slot_sha256 "
                "WHERE contract_scope_sha256=%s",
                (scope_sha256,),
            )


def test_p6d1p_04_profile_lane_constraint_fails_closed(p6d1_database):
    with _transaction(p6d1_database, APP_ROLE, read_only=True) as cursor:
        cursor.execute(
            "SELECT event_id,event_time,session_date "
            "FROM rt.option_flow_events WHERE lineage_version=3 LIMIT 1"
        )
        event_id, event_time, session_date = cursor.fetchone()
    with pytest.raises(errors.CheckViolation):
        with _transaction(p6d1_database, APP_ROLE) as cursor:
            cursor.execute(
                "INSERT INTO rt.option_flow_event_profiles ("
                "event_id,event_time,session_date,profile,universe_tier,"
                "dte_lane,threshold_mode,minimum_contracts,"
                "minimum_print_premium,contracts_matched,premium_matched,"
                "minimum_cluster_prints,minimum_distinct_exchanges,"
                "minimum_cluster_premium,minimum_cumulative_contracts,"
                "window_seconds) VALUES "
                "(%s,%s,%s,'ULTRA_SHORT','ETF','CORE_SWING','OR',"
                "1000,500000,true,false,10,3,1000000,5000,5)",
                (str(event_id), event_time, session_date),
            )
