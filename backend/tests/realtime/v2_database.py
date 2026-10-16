"""Opt-in disposable V2 database for DB-backed tests (never stocks_realtime_v1 or stocks_realtime_v2).

Set STOCKS_V2_DB_TESTS=1 and run pytest WITHOUT the offline I/O guard plugin. The database is
created from template0, the sql_v2 baseline is applied as its owner (security deferred), and the
database is dropped unless an explicit caller preserves it for a post-test validation.
"""
from __future__ import annotations

import os
import sys
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations  # noqa: E402
from scripts.validate_realtime_v2_schema import _connect, _settings  # noqa: E402


def db_tests_enabled() -> bool:
    return os.getenv("STOCKS_V2_DB_TESTS") == "1" and getattr(psycopg2.connect, "__name__", "") != "reject_io"


requires_v2_database = pytest.mark.skipif(
    not db_tests_enabled(), reason="set STOCKS_V2_DB_TESTS=1 and run without the offline I/O guard plugin")


def _digest() -> str:
    return uuid4().hex + uuid4().hex


def insert_cohort(cursor, now: datetime) -> UUID:
    """Insert a capability profile, manifest and cohort with unique hashes; return the cohort id."""
    profile, manifest, cohort = uuid4(), uuid4(), uuid4()
    cursor.execute(
        "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
        "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, "
        "provider_tier, security_policy_sha256, preflight_mode, provider_requests, action_authorized) "
        "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
        (str(profile), _digest(), _digest(), _digest(), _digest(), now, now + timedelta(hours=1), _digest(),
         _digest()))
    cursor.execute(
        "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, session_date, "
        "effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
        "allocation_policy_sha256) VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',400,50,400,%s)",
        (str(manifest), _digest(), _digest(), now.date(), now, str(profile), _digest()))
    cursor.execute(
        "INSERT INTO rt.cohorts (cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode) VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
        (str(cohort), _digest(), str(manifest), _digest(), _digest(), _digest(), now))
    return cohort


def insert_service_plan(cursor, cohort: UUID, enabled_stages, now: datetime) -> UUID:
    plan = uuid4()
    cursor.execute(
        "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, enabled_stages, "
        "config_sha256, deadline, scope_reference) VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,'fixture scope')",
        (str(plan), str(cohort), now.date(), sorted(enabled_stages), _digest(), now + timedelta(hours=8)))
    return plan


def drop_database_if_exists(name: str) -> None:
    if not name.startswith("test_realtime_v2_"):
        raise ValueError("only test_realtime_v2_ databases can be dropped")
    with closing(_connect(_settings(), "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                           "WHERE datname=%s AND pid <> pg_backend_pid()", (name,))
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))


@contextmanager
def disposable_v2_database(name: str, *, preserve: bool = False):
    """Yield a freshly provisioned database pool; normally drop the database afterwards."""
    if not name.startswith("test_realtime_v2_"):
        raise ValueError("disposable database names must start with test_realtime_v2_")
    if type(preserve) is not bool:
        raise TypeError("preserve must be a boolean")
    settings = _settings()
    created = False
    pool = None
    try:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=%s)", (name,))
                if cursor.fetchone()[0]:
                    raise RuntimeError("disposable database already exists; refusing ownership")
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'")
                               .format(sql.Identifier(name)))
        created = True
        with closing(_connect(settings, name)) as connection:
            for migration in load_realtime_migrations(V2_MIGRATIONS_DIR):
                with connection.cursor() as cursor:
                    cursor.execute(migration.sql)
                    cursor.execute("INSERT INTO rt.schema_migrations (version, checksum_sha256) VALUES (%s, %s)",
                                   (migration.version, migration.checksum_sha256))
                connection.commit()
        pool = ThreadedConnectionPool(
            1, 8, dbname=name, user=settings["POSTGRES_ADMIN_USER"],
            password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"], port=settings["DB_PORT"],
            options="-c timezone=UTC")
        yield pool
    finally:
        if pool is not None:
            pool.closeall()
        if created and not preserve:
            with closing(_connect(settings, "postgres")) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                   "WHERE datname=%s AND pid <> pg_backend_pid()", (name,))
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
