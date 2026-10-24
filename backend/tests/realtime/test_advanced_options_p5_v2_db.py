"""Advanced Options P5 qualification on one staged, hardened disposable V2 database."""
from __future__ import annotations

import io
import json
from contextlib import closing, contextmanager, redirect_stdout
from dataclasses import dataclass, replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import psycopg2
import pytest
from psycopg2 import errors, sql
from psycopg2.pool import ThreadedConnectionPool

from realtime.advanced_options_close import (
    ADVANCED_OPTIONS_ARCHIVE_KINDS,
    build_advanced_options_retention_handoff,
)
from realtime.advanced_options_cycle import (
    O2_MODEL_NOT_ACCEPTED,
    load_advanced_options_cycle_cursor,
    persist_advanced_options_cycle_cursor,
    run_advanced_options_cycle,
)
from realtime.advanced_options_input import (
    build_advanced_options_input_closure,
    build_advanced_options_input_gate_plan,
    reconstruct_options_input_ready_receipt,
)
from realtime.advanced_options_projection import (
    OPTIONS_SNAPSHOT_TYPES,
    AdvancedOptionsPublicationAuthorization,
    OPTIONS_PROJECTION_TYPES,
    build_advanced_options_projection_batch,
    load_advanced_options_projection_batch_cursor,
    persist_advanced_options_projection_cursor,
    read_advanced_options_projection_cursor,
)
from realtime.advanced_options_native_adapters import (
    AdvancedOptionsLaneInputs,
    AdvancedOptionsNativeInputBundle,
)
from realtime.advanced_options_package_evidence import (
    build_flow_package_evidence_batch,
)
from realtime.advanced_options_postgres import (
    AdvancedOptionsPostgresRuntimeScope,
    PostgresAdvancedOptionsCanonicalRuntimePort,
)
from realtime.advanced_options_runtime import (
    AdvancedOptionsInputStageData,
    AdvancedOptionsPublishStageData,
    AdvancedOptionsRuntimeBinding,
)
from realtime.advanced_options_producers import (
    build_advanced_options_producer_plan,
)
from realtime.advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
)
from realtime.advanced_options_rolling_repository import (
    PostgresAdvancedOptionsRollingStateRepository,
)
from realtime.database_access_v2 import (
    APP_ROLE as V2_APP_ROLE,
    V2ConnectionProvider,
)
from realtime.database_access import (
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
)
from realtime.database_access_v2 import V2AccessConfig, verify_pool_target_v2
from realtime.materialization_jobs import (
    BarrierOutcome,
    JobBarrierVerdict,
    enqueue_materialization_job_cursor,
    evaluate_job_barrier_cursor,
    open_job_barrier_cursor,
    record_job_barrier_outcome_cursor,
)
from realtime.migrations import (
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
)
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from realtime.option_slot_set_repository import (
    persist_and_activate_slot_set_cursor,
)
from realtime.option_discovery_repository import (
    RealtimeOptionDiscoveryEvidence,
    persist_realtime_option_discovery_cursor,
)
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_source_flow import (
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from realtime.orchestration_repository import (
    CycleReceiptSpec,
    OPTIONS_CYCLE,
    OPTIONS_INPUT_READY,
    OPTIONS_PORTAL_READY,
)
from realtime.options_v2_runtime import OptionsPreSessionStageBinding
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    PortalReleaseReceipt,
    load_portal_release_pointer_cursor,
    persist_portal_snapshot_cursor,
    publish_portal_release_cursor,
)
from realtime.portal_publisher import (
    CanonicalPortalPublisher,
    PortalPublicationConfig,
)
from realtime.preflight import DetectorFamily, ProviderTier
from realtime.relation_registry import APP_ROLE, API_ROLE, ROLES
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.session_archive import ArchiveIdentity
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5
from realtime.worker_database import RealtimeProcessDatabase
from scripts import provision_realtime_v2_database as provision
from test_advanced_options_close import NOW as CLOSE_NOW, _coverage
from test_advanced_options_control_plane import (
    CUTOFF,
    EXPIRES,
    PLANNED,
    SESSION,
    _plan as _control_plane,
)
from test_advanced_options_cycle import NOW, _complete, _reference_repository
from test_advanced_options_input import EQUITY_WINDOW, WINDOW, _closure
from test_advanced_options_producers import (
    _clocks,
    _discovery_scope,
    _limits,
)
from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from test_option_discovery_repository import _result as _discovery_result
from option_flow_support import advanced_contract_scope, advanced_policy
from v2_database import _connect, _settings, requires_v2_database


pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_options_p5"
ROLE_PREFIX = "test_rt_options_p5"
CONFIGURATION_SHA256 = "c" * 64
RUNTIME_POLICY_SHA256 = "d" * 64
PROFILE_SHA256 = "e" * 64
AUTHORIZATION_SCOPE_SHA256 = "f" * 64
SERVICE_PLAN_ID = UUID("10101010-2020-3030-4040-505050505050")
OPTION_CONTRACTS = (
    (50_001, 60_001, 201, "O:SPYP5A"),
    (50_002, 60_002, 202, "O:SPYP5B"),
)


@dataclass
class P5Database:
    pool: ThreadedConnectionPool
    roles: object
    control: object
    migrations: tuple
    staged_versions: tuple[str, ...]
    resume_report: dict[str, object]
    replay_report: dict[str, object]
    roles_created: tuple[str, ...]
    security_audit: tuple[tuple[str, str], ...]


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
            "P5 disposable target is not clean: "
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
    staged = tuple(
        migration
        for migration in migrations
        if migration.version != "025_realtime_advanced_options_control_plane"
    )
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
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT version FROM rt.schema_migrations ORDER BY version"
            )
            versions = tuple(row[0] for row in cursor.fetchall())
            cursor.execute(
                "SELECT count(*) FROM rt.stage_catalog "
                "WHERE version='stages_v4'"
            )
            if cursor.fetchone()[0] != 0:
                raise RuntimeError("stages_v4 exists before migration 025")
        connection.rollback()
    return versions


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
    assert databases == [], f"disposable database was left behind: {databases}"
    assert role_rows == [], f"disposable roles were left behind: {role_rows}"


@contextmanager
def _transaction(
    database: P5Database,
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


def _seed_control_plane(database: P5Database) -> None:
    control = database.control
    securities = (
        (UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), "AAPL", 100, False),
        (UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), "SPY", 101, True),
    )
    with _transaction(database, APP_ROLE) as cursor:
        cursor.execute(
            "INSERT INTO rt.capability_profiles ("
            "profile_id,config_sha256,schema_sha256,account_alias,"
            "capabilities_sha256,universe_sha256,checked_at,expires_at,status,"
            "profile_sha256,provider_tier,security_policy_sha256,preflight_mode,"
            "provider_requests,action_authorized) VALUES ("
            "%s,%s,%s,'p5-fixture',%s,%s,%s,%s,'PASS',%s,"
            "'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
            (
                str(control.profile_id),
                CONFIGURATION_SHA256,
                _digest(),
                _digest(),
                control.universe.sha256,
                PLANNED,
                EXPIRES,
                control.profile_sha256,
                _digest(),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.subscription_manifests ("
            "manifest_id,manifest_sha256,universe_sha256,session_date,"
            "effective_at,profile_id,manifest_kind,stock_count,"
            "option_underlying_count,member_count,allocation_policy_sha256) "
            "VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',2,1,2,%s)",
            (
                str(control.manifest_id),
                control.manifest_sha256,
                control.universe.sha256,
                SESSION,
                PLANNED,
                str(control.profile_id),
                control.manifest_policy.sha256,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.cohorts ("
            "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
            "model_bundle_sha256,valuation_sha256,effective_at,mode) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (
                str(control.cohort_id),
                control.cohort_sha256,
                str(control.manifest_id),
                CONFIGURATION_SHA256,
                control.pins.model_bundle_sha256,
                control.pins.valuation_policy_sha256,
                PLANNED,
            ),
        )
        for security_id, ticker, instrument_id, option_underlying in securities:
            cursor.execute(
                "INSERT INTO ref.securities ("
                "security_id,asset_type,source_origin,created_at) "
                "VALUES (%s,'STOCK','p5-fixture',%s)",
                (str(security_id), PLANNED - timedelta(days=30)),
            )
            cursor.execute(
                "INSERT INTO ref.security_revisions ("
                "revision_id,security_id,ticker,calendar_id,effective_from,"
                "observed_at,payload_sha256) VALUES (%s,%s,%s,'XNYS',%s,%s,%s)",
                (
                    str(uuid4()),
                    str(security_id),
                    ticker,
                    PLANNED - timedelta(days=30),
                    PLANNED - timedelta(days=30),
                    _digest(),
                ),
            )
            cursor.execute(
                "INSERT INTO rt.instruments ("
                "instrument_id,asset_class,security_id) "
                "VALUES (%s,'EQUITY',%s)",
                (instrument_id, str(security_id)),
            )
            cursor.execute(
                "INSERT INTO rt.subscription_members ("
                "manifest_id,instrument_id,channel,priority,purpose,"
                "option_underlying) VALUES (%s,%s,'AM',100,'PRIMARY',%s)",
                (
                    str(control.manifest_id),
                    instrument_id,
                    option_underlying,
                ),
            )
        for contract_id, version_id, instrument_id, ticker in OPTION_CONTRACTS:
            cursor.execute(
                "INSERT INTO ref.option_contracts ("
                "contract_id,underlying_security_id,provider,contract_ticker,"
                "first_observed_at,source_origin) VALUES "
                "(%s,%s,'massive',%s,%s,'p5-fixture')",
                (
                    contract_id,
                    str(securities[1][0]),
                    ticker,
                    PLANNED,
                ),
            )
            cursor.execute(
                "INSERT INTO ref.option_contract_versions ("
                "version_id,contract_id,call_put,expiration,strike,"
                "exercise_style,multiplier,eligibility,effective_from,"
                "observed_at,payload_sha256) VALUES "
                "(%s,%s,'C',%s,500,'AMERICAN',100,'VALIDATED_ACTIVE',"
                "%s,%s,%s)",
                (
                    version_id,
                    contract_id,
                    SESSION + timedelta(days=30),
                    PLANNED - timedelta(days=1),
                    PLANNED - timedelta(days=1),
                    _digest(),
                ),
            )
            cursor.execute(
                "INSERT INTO rt.instruments ("
                "instrument_id,asset_class,contract_id) "
                "VALUES (%s,'OPTION',%s)",
                (instrument_id, contract_id),
            )
        cursor.execute(
            "INSERT INTO rt.service_plans ("
            "plan_id,cohort_id,session_date,stage_catalog_version,"
            "enabled_stages,config_sha256,deadline,scope_reference) "
            "VALUES (%s,%s,%s,'stages_v1',ARRAY['REFERENCE'],%s,%s,%s)",
            (
                str(SERVICE_PLAN_ID),
                str(control.cohort_id),
                SESSION,
                _digest(),
                CUTOFF + timedelta(hours=2),
                "advanced-options-p5-fixture",
            ),
        )
        cursor.execute(
            "INSERT INTO rt.window_runs ("
            "window_id,plan_id,cohort_id,provider_tier,window_key,"
            "release_at,cutoff,deadline) VALUES "
            "(%s,%s,%s,'MASSIVE_ADVANCED','advanced-options-p5',%s,%s,%s)",
            (
                str(WINDOW),
                str(SERVICE_PLAN_ID),
                str(control.cohort_id),
                CUTOFF + timedelta(minutes=1),
                CUTOFF,
                CUTOFF + timedelta(hours=1),
            ),
        )
        cursor.execute(
            "SELECT rt.ensure_partitions(%s,%s)",
            ("rt.portal_snapshot_hot", SESSION),
        )


@pytest.fixture(scope="module")
def p5_database():
    assert DATABASE.startswith("test_realtime_v2_")
    assert ROLE_PREFIX.startswith("test_rt_")
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    migrations = tuple(
        migration
        for migration in load_realtime_migrations(V2_MIGRATIONS_DIR)
        if migration.version
        <= "025_realtime_advanced_options_control_plane"
    )
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
        database = P5Database(
            pool=pool,
            roles=roles,
            control=_control_plane(),
            migrations=migrations,
            staged_versions=staged_versions,
            resume_report=resume_report,
            replay_report=replay_report,
            roles_created=tuple(roles_created),
            security_audit=tuple(security_audit),
        )
        _seed_control_plane(database)
        yield database
    finally:
        if pool is not None:
            pool.closeall()
        _drop_target(settings, roles)


def _counts(cursor) -> dict[str, int]:
    relations = (
        "rt.portal_releases",
        "rt.portal_release_current",
        "rt.alert_outbox",
        "rt.session_archives",
        "rt.archive_segments",
    )
    result = {}
    for relation in relations:
        cursor.execute(f"SELECT count(*) FROM {relation}")
        result[relation] = int(cursor.fetchone()[0])
    return result


def _insert_receipt(cursor, database: P5Database, receipt) -> UUID:
    receipt_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:p5-receipt:{WINDOW}:{receipt.receipt_kind}",
    )
    cursor.execute(
        "INSERT INTO rt.cycle_receipts ("
        "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
        "reference_generation,universe_sha256,input_sha256,build_id) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (window_id,receipt_kind) DO NOTHING",
        (
            str(receipt_id),
            str(WINDOW),
            str(database.control.cohort_id),
            receipt.receipt_kind,
            receipt.cutoff,
            receipt.reference_generation,
            receipt.universe_sha256,
            receipt.input_sha256,
            receipt.build_id,
        ),
    )
    cursor.execute(
        "SELECT receipt_id,input_sha256 FROM rt.cycle_receipts "
        "WHERE window_id=%s AND receipt_kind=%s",
        (str(WINDOW), receipt.receipt_kind),
    )
    stored = cursor.fetchone()
    assert stored == (receipt_id, receipt.input_sha256)
    return receipt_id


def _cycle_result():
    control = _control_plane()
    gate = build_advanced_options_input_gate_plan(
        _closure(),
        window_id=WINDOW,
        equity_window_id=EQUITY_WINDOW,
        instrument_ids={"SPY": 101},
        build_id="p4-build",
        available_at=NOW,
    )
    executors = {
        detector: (lambda _context: _complete())
        for detector in control.manifest_policy.required_detectors
        if detector is not DetectorFamily.O2
    }
    return control, gate, executors


def test_p5_01_forward_migrations_apply_once_on_024_baseline(
    p5_database,
):
    migration_025 = p5_database.migrations[-1]
    current_migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    expected_applied = [
        migration.version
        for migration in current_migrations
        if migration.sequence >= migration_025.sequence
    ]
    assert migration_025.version == (
        "025_realtime_advanced_options_control_plane"
    )
    assert p5_database.staged_versions == tuple(
        migration.version for migration in p5_database.migrations[:-1]
    )
    assert p5_database.resume_report["applied_now"] == expected_applied
    assert p5_database.replay_report["applied_now"] == []
    assert p5_database.resume_report["structure"]["failures"] == []
    with _transaction(p5_database, read_only=True) as cursor:
        cursor.execute(
            "SELECT checksum_sha256 FROM rt.schema_migrations "
            "WHERE version=%s",
            (migration_025.version,),
        )
        assert cursor.fetchone() == (migration_025.checksum_sha256,)
        cursor.execute(
            "SELECT count(*) FROM rt.stage_catalog WHERE version='stages_v4'"
        )
        assert cursor.fetchone()[0] == 14
        cursor.execute(
            "SELECT count(*) FROM rt.service_plans "
            "WHERE stage_catalog_version='stages_v4'"
        )
        assert cursor.fetchone()[0] == 0


def test_p5_02_options_scope_slots_and_receipts_are_database_enforced(
    p5_database,
):
    members = tuple(
        SlotSetMember(
            instrument_id,
            channel,
            (
                SlotSetPurpose.PRIMARY
                if index == 0
                else SlotSetPurpose.RESERVE
            ),
            100,
            version_id,
        )
        for index, (_, version_id, instrument_id, _) in enumerate(
            OPTION_CONTRACTS
        )
        for channel in p5_database.control.feed_scope.channels
    )
    slot_set = SlotSetVersion(
        p5_database.control.cohort_id,
        1,
        SlotSetKind.PRE_SESSION,
        PLANNED,
        p5_database.control.pins.slot_set_policy_sha256,
        members,
    )
    with _transaction(p5_database, APP_ROLE) as cursor:
        assert persist_and_activate_slot_set_cursor(
            cursor,
            slot_set,
            activated_at=PLANNED,
        )

    with pytest.raises(errors.RaiseException, match="exact T/Q contracts"):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "INSERT INTO rt.slot_set_versions ("
                "cohort_id,version,kind,effective_at,policy_sha256,"
                "member_sha256,primary_contract_count,reserve_contract_count) "
                "VALUES (%s,2,'REFRESH',%s,%s,%s,1,0)",
                (
                    str(p5_database.control.cohort_id),
                    PLANNED + timedelta(minutes=1),
                    "1" * 64,
                    "2" * 64,
                ),
            )
            cursor.execute(
                "INSERT INTO rt.slot_set_members ("
                "cohort_id,version,instrument_id,channel,purpose,priority,"
                "catalog_version_id) VALUES (%s,2,201,'T','PRIMARY',100,60001)",
                (str(p5_database.control.cohort_id),),
            )

    with pytest.raises(errors.CheckViolation):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "INSERT INTO rt.slot_set_versions ("
                "cohort_id,version,kind,effective_at,policy_sha256,"
                "member_sha256,primary_contract_count,reserve_contract_count) "
                "VALUES (%s,2,'REFRESH',%s,%s,%s,1001,0)",
                (
                    str(p5_database.control.cohort_id),
                    PLANNED + timedelta(minutes=1),
                    "1" * 64,
                    "2" * 64,
                ),
            )

    with pytest.raises(errors.CheckViolation):
        with _transaction(p5_database) as cursor:
            cursor.execute(
                "INSERT INTO rt.advanced_ingress_runtime_profiles ("
                "runtime_profile_sha256,runtime_profile_version,"
                "universe_target,ladder_position,provider_tier,feed,channels,"
                "profile_document,configuration_sha256,runtime_policy_sha256,"
                "status) VALUES (%s,'p5-invalid-options',2,0,"
                "'MASSIVE_ADVANCED','OPTIONS',ARRAY['T'],'{}'::jsonb,%s,%s,"
                "'CANDIDATE')",
                ("1" * 64, "2" * 64, RUNTIME_POLICY_SHA256),
            )

    with _transaction(p5_database) as cursor:
        cursor.execute(
            "INSERT INTO rt.advanced_ingress_runtime_profiles ("
            "runtime_profile_sha256,runtime_profile_version,universe_target,"
            "ladder_position,provider_tier,feed,channels,profile_document,"
            "configuration_sha256,runtime_policy_sha256,status) VALUES "
            "(%s,'p5-options-v1',2,0,'MASSIVE_ADVANCED','OPTIONS',"
            "ARRAY['T','Q'],'{}'::jsonb,%s,%s,'CANDIDATE')",
            (PROFILE_SHA256, CONFIGURATION_SHA256, RUNTIME_POLICY_SHA256),
        )
        cursor.execute(
            "UPDATE rt.advanced_ingress_runtime_profiles SET "
            "status='ACTIVE',stability_evidence_sha256=%s,"
            "capacity_evidence_sha256=%s,"
            "activation_approval_reference='advanced-options-p5',"
            "activated_at=%s WHERE runtime_profile_sha256=%s",
            ("3" * 64, "4" * 64, PLANNED, PROFILE_SHA256),
        )

    with pytest.raises(errors.CheckViolation):
        with _transaction(p5_database) as cursor:
            cursor.execute(
                "INSERT INTO rt.advanced_stream_authorizations ("
                "authorization_id,authorization_reference,scope_sha256,"
                "provider,feed,channel,allowed_universe_targets,valid_from,"
                "valid_through,maximum_daily_stream_sessions,"
                "maximum_daily_connection_attempts,"
                "maximum_daily_decoded_bytes,"
                "maximum_daily_rest_repair_requests,"
                "maximum_daily_rest_repair_decoded_bytes,"
                "runtime_policy_sha256,approval_source,approval_reference,"
                "status) VALUES (%s,'p5-invalid-channel',%s,'massive',"
                "'OPTIONS','T',ARRAY[2],%s,%s,1,3,1000000,0,0,%s,"
                "'operator','advanced-options-p5','READY')",
                (
                    str(uuid4()),
                    "5" * 64,
                    SESSION,
                    SESSION,
                    RUNTIME_POLICY_SHA256,
                ),
            )

    authorization_id = uuid4()
    with _transaction(p5_database) as cursor:
        cursor.execute(
            "INSERT INTO rt.advanced_stream_authorizations ("
            "authorization_id,authorization_reference,scope_sha256,"
            "provider,feed,channel,allowed_universe_targets,valid_from,"
            "valid_through,maximum_daily_stream_sessions,"
            "maximum_daily_connection_attempts,maximum_daily_decoded_bytes,"
            "maximum_daily_rest_repair_requests,"
            "maximum_daily_rest_repair_decoded_bytes,runtime_policy_sha256,"
            "approval_source,approval_reference,status) VALUES "
            "(%s,'advanced-options-p5',%s,'massive','OPTIONS','T,Q',"
            "ARRAY[2],%s,%s,1,3,1000000,0,0,%s,'operator',"
            "'advanced-options-p5','READY')",
            (
                str(authorization_id),
                AUTHORIZATION_SCOPE_SHA256,
                SESSION,
                SESSION,
                RUNTIME_POLICY_SHA256,
            ),
        )

    with _transaction(p5_database, APP_ROLE) as cursor:
        cursor.execute(
            "INSERT INTO rt.advanced_ingress_plans ("
            "plan_id,plan_sha256,authorization_id,"
            "authorization_scope_sha256,cohort_id,manifest_id,"
            "cohort_selection_reference,session_date,universe_target,mode,"
            "runtime_profile_version,runtime_profile_sha256,"
            "configuration_sha256,runtime_policy_sha256,"
            "authorized_start_at,authorized_end_at,maximum_stream_sessions,"
            "maximum_connection_attempts,maximum_decoded_bytes,"
            "maximum_rest_repair_requests,"
            "maximum_rest_repair_decoded_bytes,status) VALUES "
            "(%s,%s,%s,%s,%s,%s,'advanced-options-p5',%s,2,'SHADOW',"
            "'p5-options-v1',%s,%s,%s,%s,%s,1,3,1000000,0,0,'READY')",
            (
                str(uuid4()),
                "6" * 64,
                str(authorization_id),
                AUTHORIZATION_SCOPE_SHA256,
                str(p5_database.control.cohort_id),
                str(p5_database.control.manifest_id),
                SESSION,
                PROFILE_SHA256,
                CONFIGURATION_SHA256,
                RUNTIME_POLICY_SHA256,
                PLANNED,
                EXPIRES,
            ),
        )
        cursor.execute(
            "SELECT feed,channel FROM rt.advanced_stream_authorizations "
            "WHERE authorization_id=%s",
            (str(authorization_id),),
        )
        assert cursor.fetchone() == ("OPTIONS", "T,Q")


def test_p5_02b_postgres_port_reconstructs_exact_canonical_input(
    p5_database,
):
    port_plan_id = UUID("22222222-3333-4444-5555-666666666666")
    port_window_id = UUID("77777777-8888-9999-aaaa-bbbbbbbbbbbb")
    slot_members = tuple(
        SlotSetMember(
            instrument_id,
            channel,
            (
                SlotSetPurpose.PRIMARY
                if index == 0
                else SlotSetPurpose.RESERVE
            ),
            100,
            version_id,
        )
        for index, (_, version_id, instrument_id, _) in enumerate(
            OPTION_CONTRACTS
        )
        for channel in p5_database.control.feed_scope.channels
    )
    control = _control_plane(slot_members=slot_members)
    base_result = _discovery_result()
    discovery = RealtimeOptionDiscoveryEvidence(
        RealtimeOptionDiscoveryScope(
            ProviderTier.MASSIVE_ADVANCED,
            control.profile_id,
            control.profile_sha256,
            control.universe,
        ),
        UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        replace(
            base_result,
            batch=replace(
                base_result.batch,
                underlyer="SPY",
                scheduled_cycle=CUTOFF,
                policy_sha256=control.manifest_policy.sha256,
            ),
        ),
    )
    flow_policy = advanced_policy()
    contract_scope = replace(
        advanced_contract_scope(
            ("O:SPY261120C00500000",),
            session_date=SESSION,
            flow_policy=flow_policy,
        ),
        manifest_sha256=control.manifest_sha256,
        slot_sha256=control.slot_set.member_sha256,
    )
    reference = reference_state(
        contract_scope_sha256=contract_scope.sha256,
    )
    rolling = rolling_state(
        cohort_id=control.cohort_id,
        journal_generation=control.source_flow.journal_generation,
    )
    equity_receipt = CycleReceiptSpec(
        "INPUT_READY",
        CUTOFF,
        reference.reference_generation,
        control.universe.sha256,
        "9" * 64,
        "equity-build",
    )
    with _transaction(p5_database, APP_ROLE) as cursor:
        cursor.execute(
            "INSERT INTO rt.cohorts ("
            "cohort_id,cohort_sha256,manifest_id,configuration_sha256,"
            "model_bundle_sha256,valuation_sha256,effective_at,mode) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (
                str(control.cohort_id),
                control.cohort_sha256,
                str(control.manifest_id),
                CONFIGURATION_SHA256,
                control.pins.model_bundle_sha256,
                control.pins.valuation_policy_sha256,
                PLANNED,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.service_plans ("
            "plan_id,cohort_id,session_date,stage_catalog_version,"
            "enabled_stages,config_sha256,deadline,scope_reference) "
            "VALUES (%s,%s,%s,'stages_v1',ARRAY['REFERENCE'],%s,%s,%s)",
            (
                str(port_plan_id),
                str(control.cohort_id),
                SESSION,
                CONFIGURATION_SHA256,
                CUTOFF + timedelta(hours=2),
                "advanced-options-postgres-port",
            ),
        )
        cursor.execute(
            "INSERT INTO rt.window_runs ("
            "window_id,plan_id,cohort_id,provider_tier,window_key,"
            "release_at,cutoff,deadline) VALUES "
            "(%s,%s,%s,'MASSIVE_ADVANCED',"
            "'advanced-options-postgres-port',%s,%s,%s)",
            (
                str(port_window_id),
                str(port_plan_id),
                str(control.cohort_id),
                CUTOFF + timedelta(minutes=1),
                CUTOFF,
                CUTOFF + timedelta(hours=1),
            ),
        )
        persist_and_activate_slot_set_cursor(
            cursor,
            control.slot_set,
            activated_at=PLANNED,
        )
        persist_option_flow_policy_cursor(cursor, flow_policy)
        persist_option_contract_scope_cursor(cursor, contract_scope)
        cursor.execute(
            "SELECT rt.ensure_partitions(%s,%s)",
            (
                "rt.option_discovery_pages",
                discovery.result.batch.completed_at.date(),
            ),
        )
        persist_realtime_option_discovery_cursor(cursor, discovery)
        cursor.execute(
            "INSERT INTO rt.cycle_receipts ("
            "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
            "reference_generation,universe_sha256,input_sha256,build_id"
            ") VALUES (%s,%s,%s,'INPUT_READY',%s,%s,%s,%s,%s)",
            (
                str(uuid4()),
                str(port_window_id),
                str(control.cohort_id),
                equity_receipt.cutoff,
                equity_receipt.reference_generation,
                equity_receipt.universe_sha256,
                equity_receipt.input_sha256,
                equity_receipt.build_id,
            ),
        )

    provider = V2ConnectionProvider(
        {V2_APP_ROLE: p5_database.pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=ROLE_PREFIX,
            allow_disposable=True,
        ),
    )
    provider.verify()
    PostgresAdvancedOptionsReferenceStateRepository(provider).put(reference)
    PostgresAdvancedOptionsRollingStateRepository(provider).put(rolling)
    native = AdvancedOptionsNativeInputBundle(
        o1=AdvancedOptionsLaneInputs(),
        o3=AdvancedOptionsLaneInputs(),
    )
    scope = AdvancedOptionsPostgresRuntimeScope(
        control_plane=control,
        producer_plan=build_advanced_options_producer_plan(
            control_plane=control,
            discovery_scope=_discovery_scope(control),
            provider_budget=control.provider_budget,
            clocks=_clocks(control),
            endpoint_response_byte_limits=_limits(),
        ),
        code_sha256="6" * 64,
        native_inputs_loader=lambda _context: native,
        evidence_builder=lambda cycle: build_flow_package_evidence_batch(
            cycle,
            (),
        ),
        close_data_loader=lambda _context, _projection: pytest.fail(
            "input qualification attempted close"
        ),
    )
    port = PostgresAdvancedOptionsCanonicalRuntimePort(provider, scope)
    context = SimpleNamespace(
        plan=SimpleNamespace(
            cohort_id=control.cohort_id,
            session_date=control.session_date,
            config_sha256=CONFIGURATION_SHA256,
        ),
        window=SimpleNamespace(
            window_id=port_window_id,
            cutoff=CUTOFF,
        ),
    )

    facts = port.load_input_facts(context)

    assert facts.control_plane == control
    assert facts.cycle_input.cohort_id == control.cohort_id
    assert facts.cycle_input.slot_set_version == control.slot_set.version
    assert facts.reference_state == reference
    assert facts.rolling_state == rolling
    assert facts.equity_input_receipt == equity_receipt
    assert facts.instrument_ids == {"SPY": 101}

    closure = build_advanced_options_input_closure(
        control_plane=facts.control_plane,
        producer_plan=facts.producer_plan,
        cycle_input=facts.cycle_input,
        reference_state=facts.reference_state,
        rolling_state=facts.rolling_state,
        reference_policy_sha256=facts.reference_policy_sha256,
        oi_facts_sha256=facts.oi_facts_sha256,
        market_state_checkpoint_sha256=(
            facts.market_state_checkpoint_sha256
        ),
        flow_checkpoint_sha256=facts.flow_checkpoint_sha256,
        unresolved_gaps_sha256=facts.unresolved_gaps_sha256,
        market_state_heads_sha256=facts.market_state_heads_sha256,
        equity_input_receipt_id=facts.equity_input_receipt_id,
        equity_input_sha256=facts.equity_input_sha256,
    )
    gate = build_advanced_options_input_gate_plan(
        closure,
        window_id=port_window_id,
        equity_window_id=facts.equity_window_id,
        instrument_ids=facts.instrument_ids,
        build_id="advanced-options-postgres-port-v1",
        available_at=facts.available_at,
    )
    verdict = JobBarrierVerdict(
        gate.barrier_id,
        "COMPLETE",
        len(gate.barrier_members),
        tuple(row.member_key for row in gate.barrier_members),
        (),
        (),
    )
    input_stage = AdvancedOptionsInputStageData(
        control,
        gate,
        verdict,
        facts.reference_state.reference_generation,
        "postgres-input",
    )
    options_receipt = CycleReceiptSpec(
        OPTIONS_INPUT_READY,
        CUTOFF,
        facts.reference_state.reference_generation,
        control.universe.sha256,
        gate.closure_sha256,
        "advanced-options-postgres-port-v1",
    )
    with _transaction(p5_database, APP_ROLE) as cursor:
        cursor.execute(
            "INSERT INTO rt.cycle_receipts ("
            "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
            "reference_generation,universe_sha256,input_sha256,build_id"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                str(uuid4()),
                str(port_window_id),
                str(control.cohort_id),
                options_receipt.receipt_kind,
                options_receipt.cutoff,
                options_receipt.reference_generation,
                options_receipt.universe_sha256,
                options_receipt.input_sha256,
                options_receipt.build_id,
            ),
        )

    cycle_facts = port.load_cycle_facts(context, input_stage)

    assert cycle_facts.input_receipt == options_receipt
    assert cycle_facts.native_inputs == native
    assert cycle_facts.reference_repository.load_generation(
        reference.reference_generation
    ) == reference


def test_p5_03_source_flow_lineage_is_idempotent_immutable_and_fk_bound(
    p5_database,
):
    source_flow = p5_database.control.source_flow
    with _transaction(p5_database, APP_ROLE) as cursor:
        persist_option_source_flow_cursor(
            cursor,
            source_flow,
            source_cohort_id=p5_database.control.cohort_id,
        )
        persist_option_source_flow_cursor(
            cursor,
            source_flow,
            source_cohort_id=p5_database.control.cohort_id,
        )
        cursor.execute(
            "SELECT count(*) FROM rt.option_source_flows "
            "WHERE source_flow_id=%s",
            (str(source_flow.source_flow_id),),
        )
        assert cursor.fetchone()[0] == 1

    with pytest.raises(errors.RaiseException, match="stream sessions"):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "INSERT INTO rt.option_source_flows ("
                "source_flow_id,source_flow_sha256,source_cohort_id,"
                "journal_generation,stream_session_ids,owner_name,"
                "condition_semantics_sha256,flow_policy_sha256) VALUES "
                "(%s,%s,%s,%s,%s::uuid[],'option_flow_stream_v1',%s,%s)",
                (
                    str(uuid4()),
                    "7" * 64,
                    str(p5_database.control.cohort_id),
                    str(uuid4()),
                    [str(source_flow.stream_session_ids[0])] * 2,
                    source_flow.condition_semantics_sha256,
                    source_flow.flow_policy_sha256,
                ),
            )

    with pytest.raises(errors.RaiseException, match="immutable"):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE rt.option_source_flows SET owner_name=owner_name "
                "WHERE source_flow_id=%s",
                (str(source_flow.source_flow_id),),
            )

    expected_constraints = {
        "option_flow_events_source_flow_fkey": (
            "source_flow_id",
            "condition_semantics_sha256",
            "flow_policy_sha256",
        ),
        "option_flow_clusters_source_flow_fkey": (
            "source_flow_id",
            "condition_semantics_sha256",
            "flow_policy_sha256",
        ),
        "detector_event_inputs_source_flow_fkey": (
            "source_flow_id",
            "source_cohort_id",
        ),
    }
    with _transaction(p5_database, APP_ROLE, read_only=True) as cursor:
        for name, fragments in expected_constraints.items():
            cursor.execute(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname=%s",
                (name,),
            )
            definition = cursor.fetchone()[0]
            assert all(fragment in definition for fragment in fragments)
        cursor.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname='rt' "
            "AND indexname='uq_rt_option_flow_event_source_policy'"
        )
        index = cursor.fetchone()[0]
        assert all(
            fragment in index
            for fragment in (
                "source_flow_id",
                "lineage_version = ANY",
                "ARRAY[2, 3, 4]",
            )
        )


def test_p5_04_input_and_cycle_persist_under_hardened_app_role(p5_database):
    control, gate, executors = _cycle_result()
    with _transaction(p5_database, APP_ROLE) as cursor:
        for job in gate.jobs:
            enqueue_materialization_job_cursor(cursor, job)
        open_job_barrier_cursor(
            cursor,
            barrier_id=gate.barrier_id,
            cohort_id=control.cohort_id,
            window_id=gate.window_id,
            barrier_kind=OPTIONS_INPUT_READY,
            universe_sha256=control.universe.sha256,
            members=gate.barrier_members,
        )
        cursor.execute(
            "UPDATE rt.materialization_jobs SET status='COMPLETE' "
            "WHERE job_id=ANY(%s::uuid[])",
            ([str(job.job_id) for job in gate.jobs],),
        )
        for member in gate.barrier_members:
            record_job_barrier_outcome_cursor(
                cursor,
                barrier_id=gate.barrier_id,
                member_key=member.member_key,
                outcome=BarrierOutcome.COMPLETE,
                outcome_input_sha256=member.expected_input_sha256,
            )
        verdict = evaluate_job_barrier_cursor(cursor, gate.barrier_id)
        input_receipt = reconstruct_options_input_ready_receipt(
            gate,
            verdict,
            cutoff=NOW,
            reference_generation=7,
            build_id="p4-build",
        )
        assert input_receipt.receipt_kind == OPTIONS_INPUT_READY
        assert not input_receipt.advance_release
        _insert_receipt(cursor, p5_database, input_receipt)
        cycle = run_advanced_options_cycle(
            control_plane=control,
            input_gate=gate,
            input_receipt=input_receipt,
            executors=executors,
            cutoff=NOW,
            reference_generation=7,
            reference_repository=_reference_repository(),
            build_id="p4-build",
        )
        assert cycle.receipt.receipt_kind == OPTIONS_CYCLE
        assert not cycle.receipt.advance_release
        persist_advanced_options_cycle_cursor(
            cursor,
            cycle,
            control_plane=control,
        )
        persist_advanced_options_cycle_cursor(
            cursor,
            cycle,
            control_plane=control,
        )
        _insert_receipt(cursor, p5_database, cycle.receipt)
        cursor.execute(
            "SELECT summary FROM rt.detector_datasets WHERE dataset_id=%s",
            (str(cycle.dataset_id),),
        )
        summary = cursor.fetchone()[0]
        o2 = next(
            row for row in summary["lanes"]
            if row["detector"] == DetectorFamily.O2.value
        )
        assert o2["state"] == "UNAVAILABLE"
        assert o2["reason_codes"] == [O2_MODEL_NOT_ACCEPTED]
        assert summary["o2_activation"] is False
        cursor.execute(
            "SELECT count(*) FROM rt.detector_datasets WHERE dataset_id=%s",
            (str(cycle.dataset_id),),
        )
        assert cursor.fetchone()[0] == 1


def test_p5_05_projection_is_stored_readable_and_never_publishes(
    p5_database,
):
    control, gate, executors = _cycle_result()
    verdict = SimpleNamespace(
        barrier_id=gate.barrier_id,
        state="COMPLETE",
        expected_count=len(gate.barrier_members),
        complete_members=tuple(
            member.member_key for member in gate.barrier_members
        ),
        failed_members=(),
        open_members=(),
    )
    input_receipt = reconstruct_options_input_ready_receipt(
        gate,
        verdict,
        cutoff=NOW,
        reference_generation=7,
        build_id="p4-build",
    )
    cycle = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=input_receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p4-build",
    )
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    with _transaction(p5_database, APP_ROLE) as cursor:
        before = _counts(cursor)
        persist_advanced_options_projection_cursor(cursor, batch)
        _insert_receipt(cursor, p5_database, batch.portal_ready_receipt)
        after = _counts(cursor)
        assert after == before
        cursor.execute(
            "SELECT count(*) FROM rt.snapshot_catalog "
            "WHERE cohort_id=%s AND snapshot_type=ANY(%s)",
            (
                str(control.cohort_id),
                list(OPTIONS_PROJECTION_TYPES),
            ),
        )
        assert cursor.fetchone()[0] == len(OPTIONS_PROJECTION_TYPES)
    with _transaction(p5_database, API_ROLE, read_only=True) as cursor:
        for snapshot in batch.snapshots:
            result = read_advanced_options_projection_cursor(
                cursor,
                cohort_id=control.cohort_id,
                snapshot_type=snapshot.snapshot_type,
                available_by=NOW,
            )
            assert result["available"]
            assert result["data"] == snapshot.payload
            assert result["provider_fetch_performed"] is False
            assert result["detector_work_performed"] is False
            assert result["publication_performed"] is False


def test_p5_05b_v5_restart_publish_replay_and_rollback(p5_database):
    control, gate, executors = _cycle_result()
    with _transaction(p5_database, APP_ROLE) as cursor:
        for job in gate.jobs:
            enqueue_materialization_job_cursor(cursor, job)
        open_job_barrier_cursor(
            cursor,
            barrier_id=gate.barrier_id,
            cohort_id=control.cohort_id,
            window_id=gate.window_id,
            barrier_kind=OPTIONS_INPUT_READY,
            universe_sha256=control.universe.sha256,
            members=gate.barrier_members,
        )
        cursor.execute(
            "UPDATE rt.materialization_jobs SET status='COMPLETE' "
            "WHERE job_id=ANY(%s::uuid[])",
            ([str(job.job_id) for job in gate.jobs],),
        )
        for member in gate.barrier_members:
            record_job_barrier_outcome_cursor(
                cursor,
                barrier_id=gate.barrier_id,
                member_key=member.member_key,
                outcome=BarrierOutcome.COMPLETE,
                outcome_input_sha256=member.expected_input_sha256,
            )
        verdict = evaluate_job_barrier_cursor(
            cursor,
            gate.barrier_id,
        )
        initial_input_receipt = (
            reconstruct_options_input_ready_receipt(
                gate,
                verdict,
                cutoff=NOW,
                reference_generation=7,
                build_id="p4-build",
            )
        )
        _insert_receipt(
            cursor,
            p5_database,
            initial_input_receipt,
        )
        initial_cycle = run_advanced_options_cycle(
            control_plane=control,
            input_gate=gate,
            input_receipt=initial_input_receipt,
            executors=executors,
            cutoff=NOW,
            reference_generation=7,
            reference_repository=_reference_repository(),
            build_id="p4-build",
        )
        persist_advanced_options_cycle_cursor(
            cursor,
            initial_cycle,
            control_plane=control,
        )
        _insert_receipt(
            cursor,
            p5_database,
            initial_cycle.receipt,
        )
        initial_projection = build_advanced_options_projection_batch(
            control_plane=control,
            cycle=initial_cycle,
            generated_at=NOW,
            build_id="p4-build",
        )
        persist_advanced_options_projection_cursor(
            cursor,
            initial_projection,
        )
        _insert_receipt(
            cursor,
            p5_database,
            initial_projection.portal_ready_receipt,
        )

    def receipt(kind):
        with _transaction(
            p5_database,
            APP_ROLE,
            read_only=True,
        ) as cursor:
            cursor.execute(
                "SELECT receipt_id,cutoff,reference_generation,"
                "universe_sha256,input_sha256,build_id "
                "FROM rt.cycle_receipts WHERE window_id=%s "
                "AND receipt_kind=%s",
                (str(WINDOW), kind),
            )
            row = cursor.fetchone()
        assert row is not None
        return row[0], CycleReceiptSpec(
            kind,
            row[1],
            int(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
        )

    input_receipt_id, input_receipt = receipt(OPTIONS_INPUT_READY)
    _cycle_receipt_id, cycle_receipt = receipt(OPTIONS_CYCLE)
    portal_receipt_id, portal_receipt = receipt(OPTIONS_PORTAL_READY)
    input_stage = AdvancedOptionsInputStageData(
        control,
        gate,
        verdict,
        input_receipt.reference_generation,
        "rehydrated-input",
    )
    with _transaction(
        p5_database,
        APP_ROLE,
        read_only=True,
    ) as cursor:
        cycle = load_advanced_options_cycle_cursor(
            cursor,
            control_plane=control,
            input_gate=input_stage.input_gate,
            input_receipt=input_receipt,
            cycle_receipt=cycle_receipt,
        )
        projection = load_advanced_options_projection_batch_cursor(
            cursor,
            control_plane=control,
            cycle=cycle,
            portal_ready_receipt=portal_receipt,
        )
    assert cycle.cycle_sha256 == cycle_receipt.input_sha256
    assert projection.batch_sha256 == portal_receipt.input_sha256
    assert tuple(
        row.snapshot_type for row in projection.snapshots
    ) == OPTIONS_SNAPSHOT_TYPES

    equity_snapshot = build_portal_snapshot(
        cohort_id=control.cohort_id,
        provider_tier=control.binding.provider_tier,
        snapshot_type="TICKER_WORKSPACE_SUMMARY_V1",
        snapshot_key="PLATFORM",
        source_generation=input_receipt.reference_generation,
        source_manifest={"lane": "EQUITY"},
        payload={"lane": "EQUITY"},
        generated_at=NOW - timedelta(seconds=1),
    )
    with _transaction(p5_database, APP_ROLE) as cursor:
        persist_portal_snapshot_cursor(cursor, equity_snapshot)
        prior_release = publish_portal_release_cursor(
            cursor,
            PortalReleaseReceipt(
                input_receipt_id,
                control.cohort_id,
                control.binding.provider_tier,
                input_receipt.cutoff,
                input_receipt.universe_sha256,
            ),
            (equity_snapshot,),
        )
    provider = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: p5_database.pool},
        {
            RealtimeDatabaseRole.PUBLICATION: (
                p5_database.roles.name(APP_ROLE)
            ),
        },
    )
    publisher_database = RealtimeProcessDatabase(
        RealtimeProcessDatabasePlan(
            "worker:portal_publisher:v5_qualification",
            RealtimeDatabaseRole.PUBLICATION,
            RealtimePoolBudget(0, 1),
        ),
        provider,
    )
    config = PortalPublicationConfig(
        control.cohort_id,
        control.binding.provider_tier,
        uuid4(),
        1,
        dispatch_authorized=False,
    )
    authority = AdvancedOptionsPublicationAuthorization(
        uuid4(),
        control.cohort_id,
        control.binding.provider_tier,
        projection.batch_sha256,
        portal_receipt_id,
        "OWNED_DISPOSABLE_V5_PUBLICATION",
    )
    context = SimpleNamespace(
        plan=SimpleNamespace(
            plan_id=control.plan_id,
            cohort_id=control.cohort_id,
            session_date=control.session_date,
            stage_catalog_version=STAGE_CATALOG_VERSION_V5,
        ),
        window=SimpleNamespace(
            plan_id=control.plan_id,
            cohort_id=control.cohort_id,
            cutoff=portal_receipt.cutoff,
        ),
        started_at=portal_receipt.cutoff,
    )

    def binding(canonical_publisher):
        return AdvancedOptionsRuntimeBinding(
            build_id=portal_receipt.build_id,
            pre_session=OptionsPreSessionStageBinding(
                SimpleNamespace()
            ),
            input_loader=lambda _context: input_stage,
            cycle_loader=lambda _context: None,
            projection_loader=lambda _context: None,
            publish_loader=lambda _context: (
                AdvancedOptionsPublishStageData(
                    control,
                    projection,
                    "v5-canonical-publication",
                )
            ),
            catalog_version=STAGE_CATALOG_VERSION_V5,
            reference_loader=lambda _context: None,
            publication_authority_loader=(
                lambda _context, _handoff: authority
            ),
            canonical_publisher=canonical_publisher,
        )

    pointer_reads = 0

    def fail_after_publish(cursor, **values):
        nonlocal pointer_reads
        pointer_reads += 1
        if pointer_reads == 2:
            return None
        return load_portal_release_pointer_cursor(cursor, **values)

    failing = CanonicalPortalPublisher(
        publisher_database,
        config,
        pointer_loader=fail_after_publish,
    )
    with pytest.raises(
        ValueError,
        match="did not advance its pointer",
    ):
        binding(failing.publish).publish(context)
    with _transaction(
        p5_database,
        APP_ROLE,
        read_only=True,
    ) as cursor:
        after_failure = load_portal_release_pointer_cursor(
            cursor,
            cohort_id=control.cohort_id,
            provider_tier=control.binding.provider_tier,
        )
    assert after_failure is not None
    assert after_failure.release_id == prior_release.release_id

    canonical = CanonicalPortalPublisher(
        publisher_database,
        config,
    )
    runtime = binding(canonical.publish)
    first = runtime.publish(context)
    replay = runtime.publish(context)

    assert first.release_id == first.pointer_release_id
    assert replay.release_id == first.release_id
    assert not first.dispatch_authorized
    with _transaction(
        p5_database,
        APP_ROLE,
        read_only=True,
    ) as cursor:
        cursor.execute(
            "SELECT manifest FROM rt.portal_releases "
            "WHERE release_id=%s",
            (str(first.release_id),),
        )
        manifest = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM rt.portal_releases "
            "WHERE receipt_id=%s",
            (str(portal_receipt_id),),
        )
        release_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM rt.detector_datasets "
            "WHERE dataset_id=%s",
            (str(cycle.dataset_id),),
        )
        dataset_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM rt.snapshot_catalog "
            "WHERE snapshot_id=ANY(%s::uuid[])",
            ([str(row.snapshot_id) for row in projection.snapshots],),
        )
        snapshot_count = cursor.fetchone()[0]
    identities = {
        (row["snapshot_type"], row["snapshot_key"])
        for row in manifest["snapshots"]
    }
    assert (
        equity_snapshot.snapshot_type,
        equity_snapshot.snapshot_key,
    ) in identities
    assert release_count == 1
    assert dataset_count == 1
    assert snapshot_count == len(projection.snapshots)


def test_p5_06_close_handoff_has_no_archive_or_retention_effect(
    p5_database,
    tmp_path,
):
    with _transaction(p5_database, APP_ROLE, read_only=True) as cursor:
        before = _counts(cursor)
    control, coverage = _coverage(tmp_path)
    handoff = build_advanced_options_retention_handoff(
        control_plane=control,
        close_coverage=coverage,
        archive_identity=ArchiveIdentity("p4-build", "options-close-v1"),
        archives_verified=True,
        open_alert_ids=(),
        journal_retention_expires_at=CLOSE_NOW - timedelta(seconds=1),
        checked_at=CLOSE_NOW,
    )
    assert coverage.status == "COMPLETE"
    assert handoff.required_archive_kinds == ADVANCED_OPTIONS_ARCHIVE_KINDS
    assert handoff.raw_journal_removal_eligible
    assert not handoff.archive_execution_authorized
    assert not handoff.retention_execution_authorized
    assert not handoff.journal_deletion_authorized
    with _transaction(p5_database, APP_ROLE, read_only=True) as cursor:
        assert _counts(cursor) == before


def test_p5_07_hardened_roles_and_forbidden_effects_are_enforced(
    p5_database,
):
    assert sorted(p5_database.roles_created) == sorted(
        p5_database.roles.all_names
    )
    assert p5_database.security_audit
    assert all(
        status.startswith("PASS")
        for _, status in p5_database.security_audit
    )
    config = V2AccessConfig(
        security_mode="hardened",
        role_prefix=ROLE_PREFIX,
        allow_disposable=True,
    )
    assert verify_pool_target_v2(
        p5_database.pool,
        config,
        APP_ROLE,
    ) == DATABASE
    assert verify_pool_target_v2(
        p5_database.pool,
        config,
        API_ROLE,
    ) == DATABASE
    with _transaction(p5_database) as cursor:
        cursor.execute(
            "SELECT rolname,rolcanlogin FROM pg_roles "
            "WHERE rolname=ANY(%s) ORDER BY rolname",
            (list(p5_database.roles.all_names),),
        )
        role_rows = cursor.fetchall()
        assert len(role_rows) == len(ROLES)
        assert not any(can_login for _, can_login in role_rows)
        memberships = []
        for member in ROLES:
            for role in ROLES:
                if member != role:
                    cursor.execute(
                        "SELECT pg_has_role(%s,%s,'MEMBER')",
                        (
                            p5_database.roles.name(member),
                            p5_database.roles.name(role),
                        ),
                    )
                    memberships.append(cursor.fetchone()[0])
        assert not any(memberships)

    source_flow_id = p5_database.control.source_flow.source_flow_id
    with pytest.raises(errors.InsufficientPrivilege):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute("SELECT count(*) FROM rt.schema_migrations")
    with pytest.raises(errors.InsufficientPrivilege):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "INSERT INTO rt.advanced_stream_authorizations DEFAULT VALUES"
            )
    with _transaction(p5_database, APP_ROLE, read_only=True) as cursor:
        cursor.execute(
            "SELECT count(*) FROM rt.option_source_flows "
            "WHERE source_flow_id=%s",
            (str(source_flow_id),),
        )
        assert cursor.fetchone()[0] == 1
    with pytest.raises(errors.RaiseException, match="immutable"):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE rt.option_source_flows SET owner_name=owner_name "
                "WHERE source_flow_id=%s",
                (str(source_flow_id),),
            )
    with pytest.raises(errors.InsufficientPrivilege):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute(
                "DELETE FROM rt.snapshot_catalog WHERE cohort_id=%s",
                (str(p5_database.control.cohort_id),),
            )
    with pytest.raises(errors.InsufficientPrivilege):
        with _transaction(p5_database, APP_ROLE) as cursor:
            cursor.execute("CREATE TABLE rt.p5_out_of_role (value integer)")
    with pytest.raises(errors.ReadOnlySqlTransaction):
        with _transaction(
            p5_database,
            API_ROLE,
            read_only=True,
        ) as cursor:
            cursor.execute(
                "INSERT INTO rt.alert_outbox DEFAULT VALUES"
            )
    with _transaction(p5_database) as cursor:
        cursor.execute("SELECT to_regclass('rt.p5_out_of_role')")
        assert cursor.fetchone()[0] is None
