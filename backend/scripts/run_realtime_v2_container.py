#!/usr/bin/env python3
"""Load only a component's Docker secrets, then replace this process."""
from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

from dotenv import dotenv_values


OWNED_SECRETS = {
    "controller": frozenset({"app_dsn", "provider_key"}),
    "ingress": frozenset({"app_dsn", "provider_key"}),
    "bridge": frozenset({"app_dsn"}),
    "scheduler": frozenset({"app_dsn"}),
    "finalizer": frozenset({"app_dsn"}),
    "detectors": frozenset({"app_dsn"}),
    "canonical-stock-alerts": frozenset({"app_dsn"}),
    "options-v3-acquisition": frozenset({
        "app_dsn",
        "provider_key",
    }),
    "options-p8-ingress": frozenset({"app_dsn"}),
    "options-p8-market-state": frozenset({"app_dsn"}),
    "options-p8-flow": frozenset({"app_dsn"}),
    "options-p8-window": frozenset({"app_dsn"}),
    "options-p8-executor": frozenset({"app_dsn", "provider_key"}),
    "api": frozenset({"api_dsn"}),
}
SECRET_ENVIRONMENT = {
    "app_dsn": "REALTIME_V2_APP_DSN",
    "api_dsn": "REALTIME_V2_API_DSN",
    "provider_key": "POLYGON_API_KEY",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--component",
        choices=tuple(OWNED_SECRETS),
        required=True,
    )
    parser.add_argument("--app-dsn-file", type=Path)
    parser.add_argument("--api-dsn-file", type=Path)
    parser.add_argument("--provider-key-file", type=Path)
    parser.add_argument("--environment-file", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a child command is required after --")
    paths = {
        "app_dsn": args.app_dsn_file,
        "api_dsn": args.api_dsn_file,
        "provider_key": args.provider_key_file,
    }
    owned = OWNED_SECRETS[args.component]
    supplied = {
        name for name, path in paths.items() if path is not None
    }
    if args.environment_file is not None:
        if args.component != "options-p8-executor":
            parser.error(
                "--environment-file is reserved for options-p8-executor"
            )
        if supplied:
            parser.error(
                "--environment-file cannot be combined with secret files"
            )
        args.environment_file = args.environment_file.resolve()
        args.secret_paths = {}
    else:
        if supplied != set(owned):
            parser.error(
                f"{args.component} requires exactly: "
                f"{', '.join(sorted(owned))}"
            )
        args.secret_paths = {
            name: paths[name].resolve()
            for name in owned
        }
    return args


def _secret(path: Path, name: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError(f"{name} secret file is unavailable") from error
    if not value:
        raise RuntimeError(f"{name} secret file is empty")
    return value


def child_environment(component, secret_paths, base_environment):
    owned = OWNED_SECRETS[component]
    environment = dict(base_environment)
    for secret_name, variable in SECRET_ENVIRONMENT.items():
        if variable in environment:
            raise RuntimeError(
                f"{variable} must be supplied only through a secret file"
            )
        if secret_name in owned:
            environment[variable] = _secret(
                secret_paths[secret_name],
                secret_name,
            )
    return environment


def child_environment_from_file(component, path, base_environment):
    environment = dict(base_environment)
    owned = OWNED_SECRETS[component]
    values = dotenv_values(path)
    for secret_name in owned:
        variable = SECRET_ENVIRONMENT[secret_name]
        if variable in environment:
            raise RuntimeError(
                f"{variable} must be supplied only through the environment file"
            )
        value = str(values.get(variable) or "").strip()
        if not value:
            raise RuntimeError(
                f"{variable} is absent from the environment file"
            )
        environment[variable] = value
    return environment


def run_child(command, environment, *, windows: bool | None = None) -> int:
    use_subprocess = os.name == "nt" if windows is None else windows
    if use_subprocess:
        return subprocess.run(
            command,
            env=environment,
            check=False,
        ).returncode
    os.execvpe(command[0], command, environment)
    raise RuntimeError("container child replacement unexpectedly returned")


def main(argv=None) -> int:
    args = parse_args(argv)
    environment = (
        child_environment_from_file(
            args.component,
            args.environment_file,
            os.environ,
        )
        if args.environment_file is not None
        else child_environment(
            args.component,
            args.secret_paths,
            os.environ,
        )
    )
    return run_child(args.command, environment)


if __name__ == "__main__":
    raise SystemExit(main())
