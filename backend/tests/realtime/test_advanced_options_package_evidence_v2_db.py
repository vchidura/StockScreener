"""P6D5 migration 030 qualification on one exact disposable V2 database."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from psycopg2 import errors
from psycopg2.extras import Json
from psycopg2.pool import ThreadedConnectionPool

import test_advanced_options_flow_persistence_v2_db as base
from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from option_flow_support import advanced_contract_scope, advanced_policy
from realtime.advanced_options_cycle import AdvancedOptionsDetectorEvaluation
from realtime.advanced_options_package_evidence import (
    build_cluster_observation,
    build_flow_observation,
    build_flow_package_evidence,
    persist_flow_package_evidence_cursor,
)
from realtime.advanced_options_reference_repository import (
    PostgresAdvancedOptionsReferenceStateRepository,
)
from realtime.advanced_options_rolling_repository import (
    PostgresAdvancedOptionsRollingStateRepository,
)
from realtime.option_portal_repository import (
    persist_realtime_option_package_cursor,
)
from realtime.option_source_flow import (
    persist_option_contract_scope_cursor,
    persist_option_flow_policy_cursor,
)
from realtime.preflight import DetectorFamily
from realtime.relation_registry import API_ROLE, APP_ROLE
from realtime.security_v2 import build_role_names_v2, harden_database_v2
from test_advanced_options_package_evidence import (
    DATASET,
    EVALUATION,
    EVENT,
    GENERATION,
    NOW,
    SESSION,
    _cluster,
    _flow_event,
    _package_and_mark,
    _thresholds,
)
from v2_database import _connect, _settings, requires_v2_database


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_options_p6d5"
ROLE_PREFIX = "test_rt_options_p6d5"
SECURITY = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
DETECTOR_SHA256 = "a" * 64


@pytest.fixture(scope="module")
def p6d5_database():
    settings = _settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    migrations = base.load_realtime_migrations(base.V2_MIGRATIONS_DIR)
    assert migrations[-1].version == (
        "030_advanced_options_flow_package_evidence"
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


def _seed_existing_owners(database):
    policy = advanced_policy()
    scope = advanced_contract_scope(
        (
            "O:SPY261120C00500000",
            "O:SPY261120P00450000",
        ),
        session_date=SESSION,
        flow_policy=policy,
    )
    reference = reference_state(contract_scope_sha256=scope.sha256)
    rolling = rolling_state(
        cohort_id=database.cohort_id,
        journal_generation=GENERATION,
    )
    package, mark, valuation_policy = _package_and_mark(database.cohort_id)
    event = _flow_event(database.cohort_id)
    flow = build_flow_observation(
        event,
        reference_state=reference,
        rolling_state=rolling,
    )
    cluster = build_cluster_observation(
        _cluster(event),
        events={EVENT: event},
        watermark=NOW,
        rolling_state=rolling,
    )
    evaluation = AdvancedOptionsDetectorEvaluation(
        EVALUATION,
        DATASET,
        EVENT,
        DetectorFamily.O1,
        "SPY",
        "SELECTED",
        NOW,
        "f" * 64,
        {"native": "O1"},
        package,
    )
    evidence = build_flow_package_evidence(
        evaluation,
        flow_observations=(flow,),
        cluster_observations=(cluster,),
        threshold_evidence=_thresholds(),
        reference_state=reference,
        rolling_state=rolling,
        package_mark=mark,
        model_version="black-scholes-merton-v1",
        model_bundle_sha256="1" * 64,
        valuation_policy_sha256=valuation_policy.policy_sha256,
        degraded_reason_codes=("IV_SURFACE_UNAVAILABLE",),
    )
    with base._transaction(database, APP_ROLE) as cursor:
        persist_option_flow_policy_cursor(cursor, policy)
        persist_option_contract_scope_cursor(cursor, scope)
        cursor.execute(
            "SELECT manifest.profile_id,manifest.manifest_sha256 "
            "FROM rt.cohorts AS cohort JOIN rt.subscription_manifests "
            "AS manifest USING(manifest_id) WHERE cohort.cohort_id=%s",
            (str(database.cohort_id),),
        )
        profile_id, manifest_sha256 = cursor.fetchone()
        cursor.execute(
            "INSERT INTO rt.provider_tier_cohorts ("
            "cohort_id,profile_id,provider_tier,stocks_tier,options_tier,"
            "ready_state,source_manifest_sha256,valuation_policy_sha256,"
            "capabilities,disabled_detectors,effective_at,binding_sha256"
            ") VALUES (%s,%s,'MASSIVE_ADVANCED','MASSIVE_ADVANCED',"
            "'MASSIVE_ADVANCED','ADVANCED_READY',%s,%s,%s,%s,%s,%s)",
            (
                str(database.cohort_id),
                str(profile_id),
                manifest_sha256,
                valuation_policy.policy_sha256,
                ["OPTIONS_FLOW"],
                ["O2"],
                NOW,
                "2" * 64,
            ),
        )
        cursor.execute(
            "INSERT INTO ref.securities "
            "(security_id,asset_type,source_origin,created_at) "
            "VALUES (%s,'ETF','p6d5-fixture',%s)",
            (str(SECURITY), NOW),
        )
        cursor.execute(
            "INSERT INTO rt.instruments "
            "(instrument_id,asset_class,security_id) "
            "VALUES (101,'EQUITY',%s)",
            (str(SECURITY),),
        )
        for leg in package.legs:
            cursor.execute(
                "INSERT INTO ref.option_contracts ("
                "contract_id,underlying_security_id,provider,contract_ticker,"
                "first_observed_at,source_origin) VALUES "
                "(%s,%s,'massive',%s,%s,'p6d5-fixture')",
                (
                    leg.contract_id,
                    str(SECURITY),
                    leg.contract_ticker,
                    NOW,
                ),
            )
            cursor.execute(
                "INSERT INTO ref.option_contract_versions ("
                "version_id,contract_id,call_put,expiration,strike,"
                "exercise_style,multiplier,eligibility,effective_from,"
                "observed_at,payload_sha256) VALUES "
                "(%s,%s,%s,%s,%s,'AMERICAN',%s,'VALIDATED_ACTIVE',"
                "%s,%s,%s)",
                (
                    leg.catalog_version_id,
                    leg.contract_id,
                    leg.contract_type.value[0],
                    leg.expiration_date,
                    leg.strike,
                    leg.multiplier,
                    NOW - timedelta(days=1),
                    NOW - timedelta(days=1),
                    uuid4().hex + uuid4().hex,
                ),
            )
            cursor.execute(
                "INSERT INTO rt.instruments "
                "(instrument_id,asset_class,contract_id) "
                "VALUES (%s,'OPTION',%s)",
                (leg.instrument_id, leg.contract_id),
            )
        cursor.execute(
            "INSERT INTO rt.detector_versions ("
            "detector_sha256,detector_id,version,code_sha256,policy_sha256,"
            "requirements,output_kind) VALUES "
            "(%s,%s,'p6d5-v1',%s,%s,%s,'SIGNAL')",
            (
                DETECTOR_SHA256,
                DetectorFamily.O1.value,
                "3" * 64,
                "4" * 64,
                Json({}),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.detector_events ("
            "event_id,dedup_sha256,cohort_id,provider_tier,detector_sha256,"
            "instrument_id,trigger_identity,trigger_at,decision_at,"
            "disposition,reason_codes,readiness_epoch,evidence_sha256,"
            "payload_schema,payload) VALUES "
            "(%s,%s,%s,'MASSIVE_ADVANCED',%s,101,'p6d5-flow',%s,%s,"
            "'OBSERVED',ARRAY[]::text[],1,%s,'p6d5-v1',%s)",
            (
                str(EVENT),
                "5" * 64,
                str(database.cohort_id),
                DETECTOR_SHA256,
                NOW,
                NOW,
                "6" * 64,
                Json({}),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.detector_datasets ("
            "dataset_id,cohort_id,provider_tier,dataset_key,session_date,"
            "status,detector_ids,source_manifest_sha256,started_at,"
            "completed_at,summary) VALUES "
            "(%s,%s,'MASSIVE_ADVANCED','p6d5-dataset',%s,'COMPLETE',"
            "%s,%s,%s,%s,%s)",
            (
                str(DATASET),
                str(database.cohort_id),
                SESSION,
                [DetectorFamily.O1.value],
                manifest_sha256,
                NOW,
                NOW,
                Json({}),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.detector_evaluations ("
            "evaluation_id,dataset_id,event_id,detector_id,underlyer,"
            "selection_status,observed_at,payload_sha256,payload) "
            "VALUES (%s,%s,%s,%s,'SPY','SELECTED',%s,%s,%s)",
            (
                str(EVALUATION),
                str(DATASET),
                str(EVENT),
                DetectorFamily.O1.value,
                NOW,
                "f" * 64,
                Json({"native": "O1"}),
            ),
        )
        persist_realtime_option_package_cursor(cursor, package)
    PostgresAdvancedOptionsReferenceStateRepository(
        _AppProvider(database)
    ).put(reference)
    PostgresAdvancedOptionsRollingStateRepository(
        _AppProvider(database)
    ).put(rolling)
    return evidence, reference, rolling, evaluation, mark, valuation_policy


@pytest.fixture(scope="module")
def stored_evidence(p6d5_database):
    values = _seed_existing_owners(p6d5_database)
    evidence = values[0]
    with base._transaction(p6d5_database, APP_ROLE) as cursor:
        persist_flow_package_evidence_cursor(cursor, evidence)
        persist_flow_package_evidence_cursor(cursor, evidence)
    return values


def test_p6d5db_01_migration_applies_once_and_hardens(p6d5_database):
    migration = p6d5_database.migrations[-1]
    assert p6d5_database.resume_report["applied_now"] == [
        migration.version
    ]
    assert p6d5_database.replay_report["applied_now"] == []
    assert all(
        status.startswith("PASS")
        for _, status in p6d5_database.security_audit
    )


def test_p6d5db_02_exact_evidence_replay(p6d5_database, stored_evidence):
    evidence = stored_evidence[0]
    with base._transaction(
        p6d5_database,
        APP_ROLE,
        read_only=True,
    ) as cursor:
        cursor.execute(
            "SELECT evidence_id,evidence_sha256,selection_status,"
            "candidate_id,evidence_document FROM "
            "rt.advanced_options_flow_package_evidence"
        )
        row = cursor.fetchone()
    assert UUID(str(row[0])) == evidence.evidence_id
    assert str(row[1]) == evidence.evidence_sha256
    assert row[2] == "SELECTED"
    assert UUID(str(row[3])) == evidence.package.candidate_id
    assert row[4]["execution_authorized"] is False


def test_p6d5db_03_divergence_and_immutability(
    p6d5_database,
    stored_evidence,
):
    (
        evidence,
        reference,
        rolling,
        evaluation,
        mark,
        valuation_policy,
    ) = stored_evidence
    divergent = build_flow_package_evidence(
        evaluation,
        flow_observations=evidence.flow_observations,
        cluster_observations=evidence.cluster_observations,
        threshold_evidence=evidence.threshold_evidence,
        reference_state=reference,
        rolling_state=rolling,
        package_mark=mark,
        model_version=evidence.model_version,
        model_bundle_sha256=evidence.model_bundle_sha256,
        valuation_policy_sha256=valuation_policy.policy_sha256,
        degraded_reason_codes=(
            "IV_SURFACE_UNAVAILABLE",
            "REFERENCE_DEGRADED",
        ),
    )
    with pytest.raises(
        ValueError,
        match="persisted flow-package evidence conflicts",
    ):
        with base._transaction(p6d5_database, APP_ROLE) as cursor:
            persist_flow_package_evidence_cursor(cursor, divergent)
    with pytest.raises(errors.RaiseException):
        with base._transaction(p6d5_database, APP_ROLE) as cursor:
            cursor.execute(
                "UPDATE rt.advanced_options_flow_package_evidence "
                "SET selection_status='OBSERVATION'"
            )


def test_p6d5db_04_api_denial(p6d5_database, stored_evidence):
    with pytest.raises(errors.InsufficientPrivilege):
        with base._transaction(
            p6d5_database,
            API_ROLE,
            read_only=True,
        ) as cursor:
            cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_flow_package_evidence"
            )
