#!/usr/bin/env python3
"""Write process-scoped Docker secret files without printing secret values."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import dotenv_values, set_key
from psycopg2.extensions import make_dsn, parse_dsn


SECRET_VARIABLES = {
    "realtime_v2_app_dsn": "REALTIME_V2_APP_DSN",
    "realtime_v2_admin_dsn": "REALTIME_V2_ADMIN_DSN",
    "realtime_v2_api_dsn": "REALTIME_V2_API_DSN",
    "realtime_v2_provider_key": "POLYGON_API_KEY",
}
EXPECTED_DATABASE = "stocks_realtime_v2"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--include-provider-key", action="store_true")
    parser.add_argument("--include-admin-dsn", action="store_true")
    parser.add_argument("--derive-admin-dsn-from-bootstrap", action="store_true")
    parser.add_argument("--persist-derived-admin-dsn", action="store_true")
    parser.add_argument(
        "--container-db-host",
        default="host.docker.internal",
    )
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.env_file = args.env_file.resolve()
    args.output_directory = args.output_directory.resolve()
    if not args.env_file.is_file():
        parser.error("--env-file must exist")
    if not args.execute:
        parser.error("secret preparation requires --execute")
    if args.derive_admin_dsn_from_bootstrap and not args.include_admin_dsn:
        parser.error("--derive-admin-dsn-from-bootstrap requires --include-admin-dsn")
    if args.persist_derived_admin_dsn and not args.derive_admin_dsn_from_bootstrap:
        parser.error("--persist-derived-admin-dsn requires --derive-admin-dsn-from-bootstrap")
    return args


def _container_dsn(value: str, host: str) -> str:
    parameters = parse_dsn(value)
    if parameters.get("host") in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        parameters["host"] = host
    return make_dsn(**parameters)


def _write_secret(path: Path, value: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        os.write(descriptor, value.encode("utf-8"))
    finally:
        os.close(descriptor)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.output_directory.exists():
        raise RuntimeError("Docker secret output directory must be new")
    values = dotenv_values(args.env_file)
    derived_admin_dsn = None
    if args.derive_admin_dsn_from_bootstrap:
        names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
        bootstrap = {name: str(values.get(name) or "").strip() for name in names}
        missing = [name for name, value in bootstrap.items() if not value]
        if missing:
            raise RuntimeError("admin DSN derivation requires: " + ", ".join(missing))
        derived_admin_dsn = make_dsn(
            dbname=EXPECTED_DATABASE,
            host=bootstrap["DB_HOST"],
            port=bootstrap["DB_PORT"],
            user=bootstrap["POSTGRES_ADMIN_USER"],
            password=bootstrap["POSTGRES_ADMIN_PASSWORD"],
        )
    selected = {
        name: variable
        for name, variable in SECRET_VARIABLES.items()
        if (
            name != "realtime_v2_provider_key"
            or args.include_provider_key
        )
        and (
            name != "realtime_v2_admin_dsn"
            or args.include_admin_dsn
        )
    }
    resolved = {}
    for name, variable in selected.items():
        value = str(values.get(variable) or "").strip()
        if name == "realtime_v2_admin_dsn" and derived_admin_dsn is not None:
            value = derived_admin_dsn
        if not value:
            raise RuntimeError(f"{variable} is unavailable")
        if name in {
            "realtime_v2_app_dsn",
            "realtime_v2_admin_dsn",
            "realtime_v2_api_dsn",
        }:
            value = _container_dsn(value, args.container_db_host)
        resolved[name] = value
    args.output_directory.mkdir(parents=True, mode=0o700)
    try:
        for name, value in resolved.items():
            _write_secret(args.output_directory / name, value)
        if args.persist_derived_admin_dsn:
            set_key(
                str(args.env_file),
                "REALTIME_V2_ADMIN_DSN",
                derived_admin_dsn,
                quote_mode="always",
            )
    except Exception:
        for name in resolved:
            path = args.output_directory / name
            if path.exists():
                path.unlink()
        args.output_directory.rmdir()
        raise
    print(
        "prepared Docker secrets: "
        + ", ".join(sorted(resolved))
    )
    if args.persist_derived_admin_dsn:
        print("updated environment keys: REALTIME_V2_ADMIN_DSN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
