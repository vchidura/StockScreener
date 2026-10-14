#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import load_realtime_migrations, validate_target_database
from realtime.security import apply_database_security, build_role_names, create_disposable_roles, drop_disposable_roles
from scripts.provision_realtime_database import _admin_settings, _connection_settings
from realtime.contracts import canonical_sha256
from realtime.forward_ingestion import OfflineForwardPublication
from realtime.forward_reader import ForwardPublicationUnavailable, load_forward_release_cursor
from options.calendar import OptionExchangeCalendar
from realtime.forward_storage import (
    claim_forward_work_cursor, complete_forward_work_cursor, encode_forward_inputs,
    enqueue_forward_work_cursor, publish_forward_release_cursor, reconcile_forward_work_cursor,
)


ACCEPTANCE_DATABASE = "test_realtime_developer_forward_20261008"
ACCEPTANCE_ROLE_PREFIX = "test_rt_forward_20261008"


def validate_acceptance_scope(database, confirmation):
    validate_target_database(database, confirmation, allow_disposable=True)
    if database != ACCEPTANCE_DATABASE:
        raise ValueError("acceptance is authorized only for the exact disposable database")
    return database


def parse_args():
    parser = argparse.ArgumentParser(description="Explicitly authorized disposable Developer PostgreSQL acceptance; no providers/services")
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--report-path", required=True, type=Path)
    args = parser.parse_args()
    validate_acceptance_scope(args.database, args.confirm_database_name)
    report = args.report_path.resolve()
    if not report.is_relative_to(BACKEND_DIR.parent) or report.exists():
        raise ValueError("acceptance report must be a new workspace-local file")
    return args


def _connect(settings, database):
    return psycopg2.connect(**dict(_connection_settings(settings, database),
        application_name="disposable_forward_acceptance", connect_timeout=5))


@contextmanager
def _role_cursor(settings, role):
    with closing(_connect(settings, ACCEPTANCE_DATABASE)) as connection:
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout='10s'")
                cursor.execute("SET LOCAL lock_timeout='2s'")
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def _expect_denied(settings, role, statement):
    try:
        with _role_cursor(settings, role) as cursor:
            cursor.execute(statement)
    except psycopg2.errors.InsufficientPrivilege:
        return
    raise AssertionError("role separation did not deny the forbidden operation")


def _seed(connection, roles):
    test_dir = BACKEND_DIR / "tests" / "realtime"
    if str(test_dir) not in sys.path:
        sys.path.insert(0, str(test_dir))
    from test_forward_ingestion import cycle

    configuration, carried, _ = cycle.__wrapped__()
    with connection.cursor() as cursor:
        cursor.execute("SELECT clock_timestamp()")
        now = cursor.fetchone()[0]
    calendar = OptionExchangeCalendar()
    cutoff = calendar.latest_delayed_slot(now, publication_grace=timedelta(0))
    if cutoff is None:
        cutoff = calendar.session_close(calendar.latest_completed_session(now))
    selection = replace(carried.selection, session_date=cutoff.date())
    cohort = uuid5(NAMESPACE_URL, "disposable-postgresql-forward-acceptance")
    scope = replace(carried, cohort_id=cohort, selection=selection, cutoff=cutoff,
        available_after=now - timedelta(seconds=2), deadline=now + timedelta(seconds=90))
    profile = selection.scope.profile_id
    manifest = uuid5(cohort, "test-manifest")
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(roles.owner)))
        cursor.execute("INSERT INTO rt.capability_profiles(profile_id,profile_sha256,provider_tier,security_policy_sha256,"
            "preflight_mode,provider_requests,action_authorized,config_sha256,schema_sha256,account_alias,"
            "capabilities_sha256,universe_sha256,checked_at,expires_at,status) "
            "VALUES (%s,%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false,%s,%s,'DISPOSABLE_FIXTURE',%s,%s,%s,%s,'PASS')",
            (str(profile), selection.scope.profile_sha256, "e" * 64, scope.configuration_sha256, "e" * 64,
             "e" * 64, selection.scope.universe.sha256, now, now + timedelta(hours=1)))
        cursor.execute("INSERT INTO rt.subscription_manifests(manifest_id,manifest_sha256,universe_sha256,session_date,"
            "effective_at,profile_id,stock_count,option_underlying_count,option_quote_contract_count,member_count,allocation_policy_sha256) "
            "VALUES (%s,%s,%s,%s,%s,%s,8,8,16,56,%s)", (str(manifest), selection.manifest.sha256,
                selection.scope.universe.sha256, cutoff.date(), cutoff, str(profile), "f" * 64))
        cursor.execute("INSERT INTO rt.cohorts(cohort_id,cohort_sha256,manifest_id,configuration_sha256,model_bundle_sha256,"
            "valuation_sha256,effective_at,mode) VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
            (str(cohort), canonical_sha256((str(cohort), "fixture")), str(manifest), scope.configuration_sha256,
             "e" * 64, configuration.valuation_policy_sha256, cutoff))
        cursor.execute("INSERT INTO rt.provider_tier_cohorts(cohort_id,profile_id,provider_tier,stocks_tier,options_tier,"
            "ready_state,source_manifest_sha256,valuation_policy_sha256,capabilities,disabled_detectors,effective_at,binding_sha256) "
            "VALUES (%s,%s,'MASSIVE_DEVELOPER','MASSIVE_DEVELOPER','MASSIVE_DEVELOPER','DEVELOPER_READY',%s,%s,"
            "ARRAY['DISPOSABLE_ACCEPTANCE'],ARRAY['O2'],%s,%s)", (str(cohort), str(profile), selection.manifest.sha256,
                configuration.valuation_policy_sha256, cutoff, canonical_sha256((str(cohort), "tier-fixture"))))
        for symbol in scope.underlyings:
            identity = uuid5(cohort, symbol)
            cursor.execute("INSERT INTO ref.securities(security_id,asset_type,source_origin,created_at) VALUES (%s,'ETF','DISPOSABLE_FIXTURE',%s)",
                (str(identity), cutoff))
            cursor.execute("INSERT INTO ref.security_revisions(revision_id,security_id,ticker,calendar_id,effective_from,observed_at,payload_sha256) "
                "VALUES (%s,%s,%s,'XNYS',%s,%s,%s)", (str(uuid5(identity, "revision")), str(identity), symbol,
                    cutoff - timedelta(days=1), cutoff - timedelta(days=1), "a" * 64))
        for catalog in scope.catalog:
            identity = uuid5(cohort, catalog.underlyer)
            cursor.execute("INSERT INTO ref.option_contracts(contract_id,underlying_security_id,provider,contract_ticker,first_observed_at,source_origin) "
                "VALUES (%s,%s,'polygon',%s,%s,'DISPOSABLE_FIXTURE')", (catalog.contract_id, str(identity), catalog.contract_ticker, cutoff - timedelta(days=1)))
            cursor.execute("INSERT INTO ref.option_contract_versions(version_id,contract_id,call_put,expiration,strike,exercise_style,multiplier,"
                "eligibility,effective_from,observed_at,payload_sha256) VALUES (%s,%s,%s,%s,%s,'AMERICAN',100,'VALIDATED_ACTIVE',%s,%s,%s)",
                (catalog.contract_id, catalog.contract_id, catalog.contract_type.value[0], catalog.expiration_date,
                    catalog.strike, cutoff - timedelta(days=1), cutoff - timedelta(days=1), catalog.payload_sha256))
    connection.commit()
    return scope


def _insert_input(settings, roles, scope, suffix):
    identity = uuid5(scope.cohort_id, f"input:{suffix}")
    payload, digest = encode_forward_inputs(scope, ())
    with _role_cursor(settings, roles.ingress) as cursor:
        cursor.execute("INSERT INTO rt.developer_forward_inputs(input_id,cohort_id,provider_tier,scope_sha256,selection_sha256,"
            "configuration_sha256,reference_sha256,action_sha256,cutoff,available_after,deadline,received_at,payload_text,payload_sha256) "
            "VALUES (%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp(),%s,%s)",
            (str(identity), str(scope.cohort_id), scope.sha256, scope.selection.selection_sha256, scope.configuration_sha256,
             scope.reference_sha256, scope.action_sha256, scope.cutoff, scope.available_after, scope.deadline, payload, digest))
    with _role_cursor(settings, roles.compute) as cursor:
        enqueue_forward_work_cursor(cursor, identity)
    return identity


def _accept(connection, settings, roles, checks):
    checks.append("DISPOSABLE_ROLES_NOLOGIN_AND_EXPLICIT_GRANTS")
    scope = _seed(connection, roles)
    checks.append("CURRENT_SCHEMA_FIXTURE_SEEDED")
    identity = _insert_input(settings, roles, scope, "first")
    checks.append("INGRESS_INPUT_AND_COMPUTE_ENQUEUE")
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FILTER (WHERE rolcanlogin),count(*) FROM pg_roles WHERE rolname=ANY(%s)",
            (list(roles.all_names),))
        login_count, role_count = cursor.fetchone()
        if login_count or role_count != len(roles.all_names):
            raise AssertionError("disposable role catalog does not match NOLOGIN scope")
    connection.rollback()
    _expect_denied(settings, roles.reader, "SELECT payload_text FROM rt.developer_forward_inputs LIMIT 0")
    _expect_denied(settings, roles.ingress, "INSERT INTO rt.developer_forward_artifacts SELECT * FROM rt.developer_forward_artifacts WHERE false")
    _expect_denied(settings, roles.compute, "UPDATE rt.developer_forward_current SET cutoff=cutoff WHERE false")
    checks.append("INGRESS_COMPUTE_PUBLICATION_READER_SEPARATION")
    owner = uuid5(scope.cohort_id, "worker")
    with _role_cursor(settings, roles.compute) as cursor:
        claim = claim_forward_work_cursor(cursor, cohort_id=scope.cohort_id, owner_id=owner, lease_seconds=60)
        with _role_cursor(settings, roles.compute) as competitor:
            if claim_forward_work_cursor(competitor, cohort_id=scope.cohort_id, owner_id=uuid5(owner, "simultaneous")) is not None:
                raise AssertionError("SKIP LOCKED did not isolate simultaneous claims")
    checks.append("SIMULTANEOUS_SKIP_LOCKED_CLAIMS")
    if claim is None or claim.input_id != identity:
        raise AssertionError("real PostgreSQL did not claim the accepted input")
    with _role_cursor(settings, roles.compute) as cursor:
        if claim_forward_work_cursor(cursor, cohort_id=scope.cohort_id, owner_id=uuid5(owner, "competitor")) is not None:
            raise AssertionError("active lease was claimed by a second owner")
    checks.append("DATABASE_CLOCK_CLAIM_ACTIVE_LEASE_FENCE")
    try:
        with _role_cursor(settings, roles.compute) as cursor:
            complete_forward_work_cursor(cursor, replace(claim, owner_epoch=claim.owner_epoch + 1), scope, dict(MATRICES=()))
    except ValueError:
        checks.append("STALE_OWNER_EPOCH_REJECTED")
    else:
        raise AssertionError("stale owner completed the input")
    envelope = OfflineForwardPublication(scope.cohort_id, scope.selection.scope.provider_tier, scope.sha256,
        "disposable-acceptance", json.dumps(dict(native_cycles=[], available=False, reason="SYNTHETIC_STORAGE_ACCEPTANCE_ONLY", data={})))
    artifacts = dict(MATRICES=(), CONTEXT=None, PACKAGES=(), FLOW=envelope, SCREENER=envelope, ALERTS=envelope)
    try:
        with _role_cursor(settings, roles.compute) as cursor:
            complete_forward_work_cursor(cursor, claim, scope, dict(MATRICES=()))
            raise RuntimeError("intentional disposable rollback")
    except RuntimeError:
        pass
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(roles.owner)))
        cursor.execute("SELECT count(*) FROM rt.developer_forward_artifacts WHERE input_id=%s", (str(identity),))
        if cursor.fetchone()[0] != 0:
            raise AssertionError("failed compute transaction retained partial artifacts")
    connection.rollback()
    checks.append("COMPUTE_ARTIFACT_AND_COMPLETION_ROLLBACK")
    with _role_cursor(settings, roles.compute) as cursor:
        complete_forward_work_cursor(cursor, claim, scope, artifacts)
    valid_until = scope.deadline + timedelta(minutes=5)
    with _role_cursor(settings, roles.publication) as cursor:
        release = publish_forward_release_cursor(cursor, identity, valid_until=valid_until)
    with _role_cursor(settings, roles.publication) as cursor:
        if publish_forward_release_cursor(cursor, identity, valid_until=valid_until) != release:
            raise AssertionError("release retry changed immutable identity")
    checks.append("ATOMIC_RELEASE_AND_IMMUTABLE_RETRY")
    older_scope = replace(scope, cutoff=scope.cutoff - timedelta(minutes=15))
    older_identity = _insert_input(settings, roles, older_scope, "older")
    with _role_cursor(settings, roles.compute) as cursor:
        older_claim = claim_forward_work_cursor(cursor, cohort_id=scope.cohort_id, owner_id=owner, lease_seconds=60)
        older_envelope = replace(envelope, scope_sha256=older_scope.sha256)
        complete_forward_work_cursor(cursor, older_claim, older_scope,
            dict(MATRICES=(), CONTEXT=None, PACKAGES=(), FLOW=older_envelope, SCREENER=older_envelope, ALERTS=older_envelope))
    try:
        with _role_cursor(settings, roles.publication) as cursor:
            publish_forward_release_cursor(cursor, older_identity, valid_until=valid_until)
    except ValueError:
        checks.append("OLDER_RELEASE_POINTER_REPLACEMENT_ROLLED_BACK")
    else:
        raise AssertionError("older source cutoff replaced newer current release")
    with _role_cursor(settings, roles.reader) as cursor:
        cursor.execute("SELECT clock_timestamp() AS checked_at")
        now = cursor.fetchone()["checked_at"]
        artifact, retained = load_forward_release_cursor(cursor, cohort_id=scope.cohort_id, available_by=now, artifact_kind="FLOW")
        if artifact != envelope or str(retained["release_id"]) != str(release):
            raise AssertionError("stored role reader changed retained artifact identity")
    checks.append("STORED_READER_HASH_REFERENCE_TIER_AND_CLOCK_CHECKS")
    try:
        with _role_cursor(settings, roles.reader) as cursor:
            load_forward_release_cursor(cursor, cohort_id=scope.cohort_id, available_by=valid_until, artifact_kind="FLOW")
    except ForwardPublicationUnavailable:
        checks.append("EXPIRED_RELEASE_NOT_SERVED")
    else:
        raise AssertionError("expired release remained serveable")
    with _role_cursor(settings, roles.compute) as cursor:
        cursor.execute("SELECT count(*) AS count FROM rt.developer_forward_artifacts WHERE input_id=%s", (str(identity),))
        if cursor.fetchone()["count"] != 6:
            raise AssertionError("release artifact group is not complete")
    try:
        with _role_cursor(settings, roles.compute) as cursor:
            cursor.execute("UPDATE rt.developer_forward_artifacts SET artifact_key=artifact_key WHERE input_id=%s", (str(identity),))
    except psycopg2.Error:
        checks.append("IMMUTABLE_ARTIFACT_MUTATION_REFUSED")
    else:
        raise AssertionError("immutable artifact update succeeded")
    return checks


def main():
    args = parse_args()
    roles = build_role_names(ACCEPTANCE_ROLE_PREFIX)
    load_dotenv(BACKEND_DIR / ".env", override=False)
    settings = _admin_settings()
    created_database, created_roles = False, False
    report = dict(database=args.database, scope="DISPOSABLE_ONLY", provider_calls=False,
        existing_database_changes=False, service_actions=False, checks=[], status="NOT_STARTED", cleanup="NOT_STARTED")
    with closing(_connect(settings, "postgres")) as administrator:
        administrator.autocommit = True
        try:
            with administrator.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)", (args.database,))
                if cursor.fetchone()[0]:
                    raise ValueError("disposable database already exists; refusing ownership or cleanup")
                cursor.execute("SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)", (list(roles.all_names),))
                if cursor.fetchall():
                    raise ValueError("disposable roles already exist; refusing ownership or cleanup")
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(args.database)))
                created_database = True
            with closing(_connect(settings, args.database)) as connection:
                for migration in load_realtime_migrations():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL statement_timeout='30s'")
                        cursor.execute("SET LOCAL lock_timeout='5s'")
                        cursor.execute(migration.sql)
                        cursor.execute("INSERT INTO rt.schema_migrations(version,checksum_sha256) VALUES (%s,%s)",
                            (migration.version, migration.checksum_sha256))
                    connection.commit()
                report["checks"].append("MIGRATIONS_000_THROUGH_018")
                create_disposable_roles(connection, roles)
                connection.commit()
                created_roles = True
                apply_database_security(connection, roles)
                with connection.cursor() as cursor:
                    cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO CURRENT_USER").format(sql.Identifier(args.database)))
                connection.commit()
                _accept(connection, settings, roles, report["checks"])
                report["status"] = "PASS"
        except Exception as error:
            report["status"] = "FAIL"
            report["error_type"] = type(error).__name__
            report["sqlstate"] = getattr(error, "pgcode", None)
            if isinstance(error, ForwardPublicationUnavailable):
                report["serving_rejection"] = str(error)
            diagnostics = getattr(error, "diag", None)
            if diagnostics is not None:
                report["constraint"] = diagnostics.constraint_name
                report["relation"] = diagnostics.table_name
                report["column"] = diagnostics.column_name
                if report["sqlstate"] == "42501":
                    report["permission_failure"] = diagnostics.message_primary
        finally:
            try:
                if created_database:
                    with administrator.cursor() as cursor:
                        cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(args.database)))
                if created_roles:
                    drop_disposable_roles(administrator, roles)
                if created_database or created_roles:
                    with administrator.cursor() as cursor:
                        cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s),"
                            "(SELECT count(*) FROM pg_roles WHERE rolname=ANY(%s))", (args.database, list(roles.all_names)))
                        exists, role_count = cursor.fetchone()
                        if exists or role_count:
                            raise RuntimeError("disposable cleanup catalog verification failed")
                    report["cleanup_verified"] = True
                report["cleanup"] = "COMPLETE"
            except Exception as cleanup_error:
                report["cleanup"] = "FAILED"
                report["cleanup_error_type"] = type(cleanup_error).__name__
            args.report_path.parent.mkdir(parents=True, exist_ok=True)
            args.report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" and report["cleanup"] == "COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())