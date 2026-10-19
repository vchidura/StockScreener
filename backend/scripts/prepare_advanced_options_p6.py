#!/usr/bin/env python3
"""Prepare, inspect, or clean the exact disposable Advanced Options P6 database and secrets."""
from __future__ import annotations

import argparse
import io
import json
import secrets
import shutil
import sys
from contextlib import closing, redirect_stdout
from pathlib import Path

from psycopg2 import sql
from psycopg2.extensions import make_dsn

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.security_v2 import (  # noqa: E402
    APP_ROLE,
    API_ROLE,
    build_role_names_v2,
    drop_disposable_roles_v2,
)
from scripts import provision_realtime_v2_database as provision  # noqa: E402


DATABASE = "test_realtime_v2_options_p6"
ROLE_PREFIX = "test_rt_options_p6"
APP_LOGIN = f"{ROLE_PREFIX}_app_login"
API_LOGIN = f"{ROLE_PREFIX}_api_login"
SECRET_FILES = {
    APP_ROLE: "realtime_v2_app_dsn",
    API_ROLE: "realtime_v2_api_dsn",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "status", "cleanup"))
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", required=True)
    parser.add_argument("--secret-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.database != DATABASE or args.confirm_database_name != DATABASE:
        parser.error(f"P6 database must be exactly {DATABASE}")
    if args.role_prefix != ROLE_PREFIX:
        parser.error(f"P6 role prefix must be exactly {ROLE_PREFIX}")
    args.secret_directory = args.secret_directory.resolve()
    if args.action != "status" and not args.execute:
        parser.error(f"{args.action} requires --execute")
    return args


def _cluster_state(settings, roles) -> dict[str, object]:
    names = [*roles.all_names, APP_LOGIN, API_LOGIN]
    with closing(provision._connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datname=%s",
                (DATABASE,),
            )
            database_exists = cursor.fetchone() is not None
            cursor.execute(
                "SELECT rolname,rolcanlogin FROM pg_roles "
                "WHERE rolname=ANY(%s) ORDER BY rolname",
                (names,),
            )
            role_rows = [
                {"name": name, "login": bool(can_login)}
                for name, can_login in cursor.fetchall()
            ]
    return {
        "database_exists": database_exists,
        "roles": role_rows,
    }


def _create_login(cursor, name: str, member_of: str, password: str) -> None:
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,))
    if cursor.fetchone() is not None:
        raise RuntimeError(f"P6 login role already exists: {name}")
    cursor.execute(
        sql.SQL(
            "CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE PASSWORD %s"
        ).format(sql.Identifier(name)),
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
            sql.Identifier(DATABASE),
            sql.Identifier(name),
        )
    )


def _container_dsn(settings, user: str, password: str) -> str:
    return make_dsn(
        dbname=DATABASE,
        user=user,
        password=password,
        host="host.docker.internal",
        port=settings["DB_PORT"],
        application_name=f"{user}_p6",
        options="-c timezone=UTC",
    )


def _write_secret(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)


def _prepare(args, settings, roles) -> dict[str, object]:
    state = _cluster_state(settings, roles)
    if state["database_exists"] or state["roles"]:
        raise RuntimeError(f"P6 disposable cluster state is not clean: {state}")
    if args.secret_directory.exists():
        raise RuntimeError("P6 secret directory must not already exist")
    argv = [
        "--database",
        DATABASE,
        "--confirm-database-name",
        DATABASE,
        "--allow-disposable",
        "--security-mode",
        "hardened",
        "--execute",
    ]
    output = io.StringIO()
    with redirect_stdout(output):
        result = provision.main(argv)
    report = json.loads(output.getvalue())
    if result != 0:
        raise RuntimeError(f"P6 database provisioning failed: {report}")
    app_password = secrets.token_urlsafe(48)
    api_password = secrets.token_urlsafe(48)
    with closing(provision._connect(settings, "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            _create_login(
                cursor,
                APP_LOGIN,
                roles.name(APP_ROLE),
                app_password,
            )
            _create_login(
                cursor,
                API_LOGIN,
                roles.name(API_ROLE),
                api_password,
            )
    args.secret_directory.mkdir(parents=True)
    try:
        _write_secret(
            args.secret_directory / SECRET_FILES[APP_ROLE],
            _container_dsn(settings, APP_LOGIN, app_password),
        )
        _write_secret(
            args.secret_directory / SECRET_FILES[API_ROLE],
            _container_dsn(settings, API_LOGIN, api_password),
        )
    except BaseException:
        shutil.rmtree(args.secret_directory, ignore_errors=True)
        raise
    return {
        "status": "P6_PREPARED",
        "database": DATABASE,
        "role_prefix": ROLE_PREFIX,
        "security_phase": report["security_phase"],
        "migration_count": len(report["applied_now"]),
        "secret_files": sorted(SECRET_FILES.values()),
        "provider_secret_created": False,
        "values_exposed": False,
    }


def _cleanup(args, settings, roles) -> dict[str, object]:
    with closing(provision._connect(settings, "postgres")) as connection:
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
            for login in (APP_LOGIN, API_LOGIN):
                cursor.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(
                        sql.Identifier(login)
                    )
                )
            drop_disposable_roles_v2(connection, roles)
    if args.secret_directory.exists():
        expected = {
            args.secret_directory / name for name in SECRET_FILES.values()
        }
        actual = set(args.secret_directory.iterdir())
        if actual - expected:
            raise RuntimeError(
                "P6 secret directory contains unexpected paths; refusing cleanup"
            )
        for path in actual:
            path.unlink()
        args.secret_directory.rmdir()
    state = _cluster_state(settings, roles)
    if state["database_exists"] or state["roles"]:
        raise RuntimeError(f"P6 cleanup verification failed: {state}")
    return {
        "status": "P6_CLEAN",
        "database_absent": True,
        "roles_absent": True,
        "secret_directory_absent": not args.secret_directory.exists(),
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = provision._settings()
    roles = build_role_names_v2(ROLE_PREFIX)
    if args.action == "prepare":
        report = _prepare(args, settings, roles)
    elif args.action == "cleanup":
        report = _cleanup(args, settings, roles)
    else:
        report = {
            "status": "P6_STATUS",
            **_cluster_state(settings, roles),
            "secret_directory_exists": args.secret_directory.exists(),
        }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
