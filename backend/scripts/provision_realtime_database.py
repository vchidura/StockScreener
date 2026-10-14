#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import secrets
import stat
import sys
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

import psycopg2
from dotenv import dotenv_values, load_dotenv
from psycopg2 import sql


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import validate_target_database
from realtime.security import RealtimeRoleNames, build_role_names


DEFAULT_ENV_FILE = BACKEND_DIR / ".env"
LOCAL_SECRET_FILE = BACKEND_DIR / ".env.realtime.local"
PRINCIPAL_PASSWORD_NAMES = {
    "migrator": "REALTIME_MIGRATOR_PASSWORD",
    "runtime": "REALTIME_RUNTIME_PASSWORD",
    "api": "REALTIME_API_PASSWORD",
}
PRINCIPAL_DSN_NAMES = {
    "migrator": "REALTIME_MIGRATOR_DSN",
    "runtime": "REALTIME_RUNTIME_DSN",
    "api": "REALTIME_API_DSN",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Provision the isolated Advanced realtime database and principals."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE,
        help="Explicit admin environment source; does not copy or regenerate credentials")
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--initialize-local-secrets", action="store_true")
    operations.add_argument("--apply", action="store_true")
    operations.add_argument("--status", action="store_true")
    operations.add_argument("--unlock-runtime", action="store_true")
    operations.add_argument("--unlock-api", action="store_true")
    operations.add_argument("--lock-runtime", action="store_true")
    operations.add_argument("--lock-principals", action="store_true")
    args = parser.parse_args()
    validate_target_database(args.database, args.confirm_database_name)
    build_role_names(args.role_prefix)
    return args


def _admin_settings(env_file: Path | None = None) -> dict[str, str]:
    environment_file = env_file if env_file is not None else DEFAULT_ENV_FILE
    if not environment_file.is_file():
        raise FileNotFoundError("realtime admin environment file is missing")
    load_dotenv(environment_file, override=True)
    names = (
        "DB_HOST",
        "DB_PORT",
        "POSTGRES_ADMIN_USER",
        "POSTGRES_ADMIN_PASSWORD",
    )
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("realtime provisioning requires: " + ", ".join(missing))
    return settings


def _principal_passwords(env_file: Path | None = None) -> dict[str, str]:
    environment_file = env_file if env_file is not None else DEFAULT_ENV_FILE
    if not environment_file.is_file():
        raise FileNotFoundError("realtime principal environment file is missing")
    load_dotenv(environment_file, override=True)
    passwords = {
        suffix: os.getenv(variable_name, "").strip()
        for suffix, variable_name in PRINCIPAL_PASSWORD_NAMES.items()
    }
    missing = [
        PRINCIPAL_PASSWORD_NAMES[suffix]
        for suffix, value in passwords.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            "realtime principal credentials require: " + ", ".join(missing)
        )
    return passwords


def _connection_settings(settings: dict[str, str], database: str) -> dict[str, str]:
    return {
        "dbname": database,
        "user": settings["POSTGRES_ADMIN_USER"],
        "password": settings["POSTGRES_ADMIN_PASSWORD"],
        "host": settings["DB_HOST"],
        "port": settings["DB_PORT"],
        "application_name": "provision_realtime_database",
        "options": "-c timezone=UTC",
    }


def _maintenance_connection(settings: dict[str, str]):
    return psycopg2.connect(**_connection_settings(settings, "postgres"))


def _provisioning_status(
    settings: dict[str, str], database: str, roles: RealtimeRoleNames,
    env_file: Path = DEFAULT_ENV_FILE,
) -> dict[str, object]:
    with closing(_maintenance_connection(settings)) as connection:
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (database,),
            )
            database_exists = bool(cursor.fetchone()[0])
            cursor.execute(
                "SELECT rolname, rolcanlogin, rolinherit FROM pg_roles "
                "WHERE rolname = ANY(%s) ORDER BY rolname",
                (list(roles.all_names),),
            )
            existing_roles = tuple(
                {
                    "name": name,
                    "login": bool(can_login),
                    "inherit": bool(inherit),
                }
                for name, can_login, inherit in cursor.fetchall()
            )
    expected_secret_names = frozenset(PRINCIPAL_PASSWORD_NAMES.values()) | frozenset(
        PRINCIPAL_DSN_NAMES.values()
    )
    secret_names = (
        frozenset(dotenv_values(env_file))
        if env_file.is_file()
        else frozenset()
    )
    return {
        "database": database,
        "database_exists": database_exists,
        "existing_roles": existing_roles,
        "local_secret_file_exists": env_file.is_file(),
        "local_secret_names": tuple(sorted(secret_names)),
        "missing_local_secret_names": tuple(
            sorted(expected_secret_names - secret_names)
        ),
    }


def _write_local_secrets(
    settings: dict[str, str], database: str, roles: RealtimeRoleNames
) -> None:
    passwords = {
        suffix: secrets.token_urlsafe(32) for suffix in PRINCIPAL_PASSWORD_NAMES
    }
    lines = [
        "# Generated locally for the isolated Advanced realtime canary.",
        "# Do not commit or print these values.",
    ]
    for suffix, variable_name in PRINCIPAL_PASSWORD_NAMES.items():
        lines.append(f"{variable_name}={passwords[suffix]}")
    for suffix, variable_name in PRINCIPAL_DSN_NAMES.items():
        principal = getattr(roles, suffix)
        dsn = (
            f"postgresql://{quote(principal, safe='')}:{quote(passwords[suffix], safe='')}"
            f"@{settings['DB_HOST']}:{settings['DB_PORT']}/{database}"
            f"?application_name=advanced_realtime_{suffix}"
        )
        lines.append(f"{variable_name}={dsn}")
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    descriptor = os.open(
        LOCAL_SECRET_FILE,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        stat.S_IRUSR | stat.S_IWUSR,
    )
    with os.fdopen(descriptor, "wb") as secret_file:
        secret_file.write(payload)


def _create_principals(
    settings: dict[str, str], roles: RealtimeRoleNames, passwords: dict[str, str]
) -> None:
    with closing(_maintenance_connection(settings)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname",
                (list(roles.all_names),),
            )
            existing = tuple(row[0] for row in cursor.fetchall())
            if existing:
                raise RuntimeError(
                    "realtime role already exists; refusing ownership: "
                    + ", ".join(existing)
                )
            for suffix, password in passwords.items():
                inheritance = sql.SQL(" NOINHERIT") if suffix == "runtime" else sql.SQL("")
                cursor.execute(
                    sql.SQL("CREATE ROLE {} LOGIN{} PASSWORD %s").format(
                        sql.Identifier(getattr(roles, suffix)),
                        inheritance,
                    ),
                    (password,),
                )
        connection.commit()


def _drop_new_principals(
    settings: dict[str, str], roles: RealtimeRoleNames
) -> None:
    with closing(_maintenance_connection(settings)) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for suffix in reversed(tuple(PRINCIPAL_PASSWORD_NAMES)):
                cursor.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(
                        sql.Identifier(getattr(roles, suffix))
                    )
                )


def _create_database(settings: dict[str, str], database: str) -> None:
    with closing(_maintenance_connection(settings)) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (database,),
            )
            if cursor.fetchone()[0]:
                raise RuntimeError("realtime database already exists; refusing ownership")
            cursor.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(
                    sql.Identifier(database)
                )
            )


def _lock_principals(settings: dict[str, str], roles: RealtimeRoleNames) -> None:
    with closing(_maintenance_connection(settings)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname",
                (list(roles.principal_names),),
            )
            existing = tuple(row[0] for row in cursor.fetchall())
            if existing != tuple(sorted(roles.principal_names)):
                raise RuntimeError("realtime principal set is incomplete; refusing lock")
            for name in roles.principal_names:
                cursor.execute(
                    sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(name))
                )
        connection.commit()


def _lock_principal(settings: dict[str, str], roles: RealtimeRoleNames, principal: str) -> None:
    if principal not in roles.principal_names:
        raise ValueError("realtime lock principal is invalid")
    with closing(_maintenance_connection(settings)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname",
                (list(roles.principal_names),))
            existing = tuple(row[0] for row in cursor.fetchall())
            if existing != tuple(sorted(roles.principal_names)):
                raise RuntimeError("realtime principal set is incomplete; refusing lock")
            cursor.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(principal)))
        connection.commit()


def _unlock_principal(
    settings: dict[str, str],
    roles: RealtimeRoleNames,
    principal: str,
) -> None:
    if principal not in roles.principal_names:
        raise ValueError("realtime unlock principal is invalid")
    with closing(_maintenance_connection(settings)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rolname, rolcanlogin FROM pg_roles "
                "WHERE rolname = ANY(%s) ORDER BY rolname",
                (list(roles.principal_names),),
            )
            states = {name: bool(can_login) for name, can_login in cursor.fetchall()}
            if set(states) != set(roles.principal_names):
                raise RuntimeError("realtime principal set is incomplete; refusing unlock")
            if any(states.values()):
                raise RuntimeError("realtime principal already has LOGIN; refusing unlock")
            cursor.execute(
                sql.SQL("ALTER ROLE {} LOGIN NOINHERIT").format(
                    sql.Identifier(principal)
                )
            )
        connection.commit()


def main() -> int:
    args = parse_args()
    roles = build_role_names(args.role_prefix)
    if not (
        args.initialize_local_secrets
        or args.apply
        or args.status
        or args.unlock_runtime
        or args.unlock_api
        or args.lock_runtime
        or args.lock_principals
    ):
        print(
            json.dumps(
                {
                    "database": args.database,
                    "role_prefix": args.role_prefix,
                    "mode": "PLAN",
                    "principal_roles": roles.principal_names,
                    "secret_file": str(LOCAL_SECRET_FILE),
                    "actions": ("CREATE_LOGIN_PRINCIPALS", "CREATE_DATABASE"),
                },
                indent=2,
            )
        )
        print("No database, role, or credential changes made.")
        return 0

    settings = _admin_settings(args.env_file)
    if args.initialize_local_secrets:
        _write_local_secrets(settings, args.database, roles)
        print(
            json.dumps(
                {
                    "status": "LOCAL_SECRETS_INITIALIZED",
                    "secret_file": str(LOCAL_SECRET_FILE),
                    "secret_names": tuple(PRINCIPAL_PASSWORD_NAMES.values())
                    + tuple(PRINCIPAL_DSN_NAMES.values()),
                },
                indent=2,
            )
        )
        return 0
    if args.status:
        print(json.dumps(_provisioning_status(settings, args.database, roles, args.env_file), indent=2))
        return 0
    if args.lock_principals:
        _lock_principals(settings, roles)
        print(
            json.dumps(
                {
                    "status": "PRINCIPALS_LOCKED",
                    "database": args.database,
                    "principal_roles": roles.principal_names,
                },
                indent=2,
            )
        )
        return 0
    if args.lock_runtime:
        _lock_principal(settings, roles, roles.runtime)
        print(json.dumps({"status": "RUNTIME_LOCKED", "database": args.database,
            "runtime_principal": roles.runtime,
            "other_principals_unchanged": (roles.migrator, roles.api)}, indent=2))
        return 0
    if args.unlock_runtime:
        _unlock_principal(settings, roles, roles.runtime)
        print(
            json.dumps(
                {
                    "status": "RUNTIME_UNLOCKED",
                    "database": args.database,
                    "runtime_principal": roles.runtime,
                    "other_principals_locked": (roles.migrator, roles.api),
                },
                indent=2,
            )
        )
        return 0
    if args.unlock_api:
        _unlock_principal(settings, roles, roles.api)
        print(
            json.dumps(
                {
                    "status": "API_UNLOCKED",
                    "database": args.database,
                    "api_principal": roles.api,
                    "other_principals_locked": (roles.migrator, roles.runtime),
                },
                indent=2,
            )
        )
        return 0

    state = _provisioning_status(settings, args.database, roles, args.env_file)
    if state["database_exists"] or state["existing_roles"]:
        raise RuntimeError("realtime provisioning target is not clean; refusing apply")
    passwords = _principal_passwords(args.env_file)
    _create_principals(settings, roles, passwords)
    try:
        _create_database(settings, args.database)
    except Exception:
        _drop_new_principals(settings, roles)
        raise
    print(
        json.dumps(
            {
                "status": "PROVISIONED",
                "database": args.database,
                "principal_roles": roles.principal_names,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())