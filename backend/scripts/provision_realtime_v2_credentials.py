#!/usr/bin/env python3
"""Provision persistent V2 app/api LOGIN principals without changing capability roles."""
from __future__ import annotations

import argparse
import json
import secrets
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from dotenv import set_key
from psycopg2 import sql
from psycopg2.extensions import make_dsn
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    verify_pool_target_v2,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.security_v2 import build_role_names_v2  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402

APP_LOGIN = "stocks_realtime_v2_app_login"
API_LOGIN = "stocks_realtime_v2_api_login"
APP_DSN_NAME = "REALTIME_V2_APP_DSN"
API_DSN_NAME = "REALTIME_V2_API_DSN"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("V2 credentials target only stocks_realtime_v2")
    if args.execute and not args.authorization_reference.strip():
        parser.error("--execute requires --authorization-reference")
    return args


def _principal(cursor, name, member_of, password, database):
    cursor.execute(
        "SELECT rolcanlogin,rolinherit,rolsuper,rolcreatedb,rolcreaterole "
        "FROM pg_roles WHERE rolname=%s",
        (name,),
    )
    existing = cursor.fetchone()
    created = False
    if existing is None:
        cursor.execute(
            sql.SQL("CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %s")
            .format(sql.Identifier(name)),
            (password,),
        )
        created = True
    elif tuple(existing) != (True, False, False, False, False):
        raise RuntimeError(f"existing V2 login principal violates policy: {name}")
    else:
        cursor.execute(
            sql.SQL("ALTER ROLE {} PASSWORD %s").format(sql.Identifier(name)),
            (password,),
        )
    cursor.execute(
        sql.SQL("GRANT {} TO {}").format(
            sql.Identifier(member_of),
            sql.Identifier(name),
        )
    )
    cursor.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(database),
            sql.Identifier(name),
        )
    )
    return created


def _dsn(settings, database, user, password):
    return make_dsn(
        dbname=database,
        user=user,
        password=password,
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        application_name=f"{user}_pool",
        options="-c timezone=UTC",
    )


def _verify_dsn(dsn, role):
    pool = ThreadedConnectionPool(1, 1, dsn=dsn)
    try:
        return verify_pool_target_v2(
            pool,
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
            ),
            role,
        )
    finally:
        pool.closeall()


def main(argv=None):
    args = parse_args(argv)
    roles = build_role_names_v2(EXPECTED_V2_DATABASE_NAME)
    report = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "principals": [APP_LOGIN, API_LOGIN],
        "capability_roles": [roles.name(APP_ROLE), roles.name(API_ROLE)],
        "env_names": [APP_DSN_NAME, API_DSN_NAME],
        "values_exposed": False,
    }
    if not args.execute:
        print(json.dumps({**report, "database_writes": 0, "env_writes": 0}, sort_keys=True))
        return 0
    settings = admin_settings(BACKEND_DIR / ".env")
    app_password = secrets.token_urlsafe(48)
    api_password = secrets.token_urlsafe(48)
    with closing(connect("postgres", settings, application_name="provision_v2_credentials")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            created_app = _principal(
                cursor, APP_LOGIN, roles.name(APP_ROLE), app_password, args.database)
            created_api = _principal(
                cursor, API_LOGIN, roles.name(API_ROLE), api_password, args.database)
    app_dsn = _dsn(settings, args.database, APP_LOGIN, app_password)
    api_dsn = _dsn(settings, args.database, API_LOGIN, api_password)
    set_key(str(args.env_file), APP_DSN_NAME, app_dsn, quote_mode="always")
    set_key(str(args.env_file), API_DSN_NAME, api_dsn, quote_mode="always")
    if _verify_dsn(app_dsn, APP_ROLE) != args.database or _verify_dsn(api_dsn, API_ROLE) != args.database:
        raise RuntimeError("V2 persistent credential verification failed")
    print(json.dumps({
        **report,
        "status": "CREDENTIALS_READY",
        "created": {APP_LOGIN: created_app, API_LOGIN: created_api},
        "verified_roles": [APP_ROLE, API_ROLE],
        "database_writes": 2,
        "env_writes": 2,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
