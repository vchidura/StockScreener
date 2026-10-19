"""P6D2 migration 027 qualification on one exact disposable V2 database."""
from __future__ import annotations

import json
from contextlib import closing
from uuid import UUID

import pytest
from psycopg2 import errors
from psycopg2.pool import ThreadedConnectionPool

import test_advanced_options_flow_persistence_v2_db as base
from option_flow_support import (
    CALL,
    JOURNAL_GENERATION,
    advanced_contract_scope,
    advanced_policy,
    config,
)
from realtime.advanced_options_provider_contract import (
    DEFAULT_OPTIONS_EXCHANGE_REFERENCE_PATH,
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    parse_exchange_reference,
)
from realtime.option_flow.conditions import (
    DEFAULT_LIVE_CONDITION_PAYLOAD_PATH,
    DEFAULT_LIVE_CONDITION_REVIEW_PATH,
    load_semantics,
    parse_payload,
)
from realtime.option_flow.writer import write_condition_reference
from realtime.option_source_flow import (
    build_option_source_flow_identity,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from realtime.relation_registry import API_ROLE, APP_ROLE
from realtime.security_v2 import (
    build_role_names_v2,
    harden_database_v2,
)
from v2_database import _connect, _settings, requires_v2_database


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_options_p6d2"
ROLE_PREFIX = "test_rt_options_p6d2"
STREAM = UUID("62626262-7272-8282-9292-a2a2a2a2a2a2")


@pytest.fixture(scope="module")
def p6d2_database():
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    migrations = tuple(
        migration
        for migration in base.load_realtime_migrations(
            base.V2_MIGRATIONS_DIR
        )
        if migration.version
        <= "027_advanced_options_provider_references"
    )
    assert migrations[-1].version == (
        "027_advanced_options_provider_references"
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
            1, 8, dbname=DATABASE,
            user=settings["POSTGRES_ADMIN_USER"],
            password=settings["POSTGRES_ADMIN_PASSWORD"],
            host=settings["DB_HOST"], port=settings["DB_PORT"],
            options="-c timezone=UTC",
        )
        database = base.P6D1Database(
            pool, roles, migrations, staged_versions,
            resume_report, replay_report, tuple(roles_created),
            tuple(security_audit), UUID(int=0),
        )
        database.cohort_id = base._seed(database)
        yield database
    finally:
        if pool is not None:
            pool.closeall()
        base._drop_target(settings, roles)
        base.DATABASE, base.ROLE_PREFIX = original


def test_p6d2db_01_migration_applies_once_and_hardens(p6d2_database):
    migration = p6d2_database.migrations[-1]
    assert p6d2_database.resume_report["applied_now"] == [
        migration.version
    ]
    assert p6d2_database.replay_report["applied_now"] == []
    assert all(
        status.startswith("PASS")
        for _, status in p6d2_database.security_audit
    )


def test_p6d2db_02_policy_references_and_v4_identity(p6d2_database):
    policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    conditions = parse_payload(json.loads(
        DEFAULT_LIVE_CONDITION_PAYLOAD_PATH.read_text(encoding="utf-8")
    ))
    exchanges = parse_exchange_reference(json.loads(
        DEFAULT_OPTIONS_EXCHANGE_REFERENCE_PATH.read_text(encoding="utf-8")
    ))
    engine_config = config(
        cohort_id=p6d2_database.cohort_id,
        advanced_flow_policy=policy,
        contract_scope=scope,
        exchange_reference=exchanges,
    )
    source = build_option_source_flow_identity(
        journal_generation=JOURNAL_GENERATION,
        stream_session_ids=(STREAM,),
        condition_semantics_sha256=engine_config.semantics.semantics_sha256,
        flow_policy_sha256=engine_config.policy_sha256(),
        advanced_flow_policy_sha256=policy.sha256,
        contract_scope_sha256=scope.sha256,
        provider_contract_sha256=MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        condition_reference_sha256=conditions.payload_sha256,
        exchange_reference_sha256=exchanges.sha256,
    )
    with base._transaction(p6d2_database, APP_ROLE) as cursor:
        persist_option_flow_policy_cursor(cursor, policy)
        persist_option_source_flow_cursor(
            cursor, source,
            source_cohort_id=p6d2_database.cohort_id,
            contract_scope=scope,
        )
        for row in exchanges.exchanges:
            cursor.execute(
                "INSERT INTO ref.exchange_codes (provider,asset_class,"
                "exchange_id,name,mic,exchange_type,fetched_at,payload_sha256,"
                "acronym,locale,operating_mic,participant_id,url,"
                "provider_contract_sha256,exchange_reference_sha256) VALUES "
                "(%s,'options',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    "massive", row.exchange_id, row.name, row.mic,
                    row.exchange_type, exchanges.fetched_at,
                    exchanges.payload_sha256, row.acronym, row.locale,
                    row.operating_mic, row.participant_id, row.url,
                    MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
                    exchanges.sha256,
                ),
            )
    with base._transaction(
        p6d2_database, APP_ROLE, read_only=True
    ) as cursor:
        cursor.execute("SELECT count(*) FROM ref.exchange_codes")
        assert cursor.fetchone()[0] == 21
        cursor.execute(
            "SELECT identity_version,condition_reference_sha256,"
            "exchange_reference_sha256 FROM rt.option_source_flows"
        )
        assert cursor.fetchone() == (
            4, conditions.payload_sha256, exchanges.sha256
        )
    with pytest.raises(errors.InsufficientPrivilege):
        with base._transaction(
            p6d2_database, API_ROLE, read_only=True
        ) as cursor:
            cursor.execute("SELECT count(*) FROM ref.exchange_codes")


def test_p6d2db_03_legacy_v3_writer_still_persists(p6d2_database):
    source, scope, output = base._advanced_output(p6d2_database)
    with base._transaction(p6d2_database, APP_ROLE) as cursor:
        persist_option_source_flow_cursor(
            cursor, source,
            source_cohort_id=p6d2_database.cohort_id,
            contract_scope=scope,
        )
    report = base._write_output(p6d2_database, output)
    assert report.events_inserted == len(output.events)
    assert report.clusters_inserted == len(output.clusters)


def test_p6d2db_04_semantics_v3_is_idempotent_and_immutable(
    p6d2_database,
):
    semantics = load_semantics(
        DEFAULT_LIVE_CONDITION_PAYLOAD_PATH,
        DEFAULT_LIVE_CONDITION_REVIEW_PATH,
    )
    connection = p6d2_database.pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                base.sql.SQL("SET LOCAL ROLE {}").format(
                    base.sql.Identifier(
                        p6d2_database.roles.name(APP_ROLE)
                    )
                )
            )
        assert write_condition_reference(connection, semantics) == (33, 33)
        with connection.cursor() as cursor:
            cursor.execute(
                base.sql.SQL("SET LOCAL ROLE {}").format(
                    base.sql.Identifier(
                        p6d2_database.roles.name(APP_ROLE)
                    )
                )
            )
        assert write_condition_reference(connection, semantics) == (0, 0)
    finally:
        p6d2_database.pool.putconn(connection)
    with base._transaction(
        p6d2_database, APP_ROLE, read_only=True
    ) as cursor:
        cursor.execute(
            "SELECT count(*),min(provider_contract_sha256),"
            "max(provider_contract_sha256) "
            "FROM ref.trade_condition_codes WHERE provider='massive'"
        )
        assert cursor.fetchone() == (
            33,
            MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
            MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        )
        cursor.execute(
            "SELECT count(*) FROM ref.option_condition_semantics "
            "WHERE semantics_version='massive_options_conditions_v3'"
        )
        assert cursor.fetchone()[0] == 33
    with pytest.raises(errors.RaiseException, match="immutable"):
        with base._transaction(p6d2_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE ref.option_condition_semantics "
                "SET review_note=review_note "
                "WHERE semantics_version='massive_options_conditions_v3'"
            )
