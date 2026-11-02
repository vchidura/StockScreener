#!/usr/bin/env python3
"""Load only a component's Docker secrets, then replace this process."""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_realtime_fencing import (
    PostgresLeaseHeartbeat,
    RUNTIME_GROUP_RESOURCE,
    role_resource_key,
)
from realtime.database_access_v2 import (
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME


OWNED_SECRETS = {
    "controller": frozenset({"app_dsn", "provider_key"}),
    "capability-refresh": frozenset({"app_dsn", "provider_key"}),
    "preparation": frozenset({"app_dsn", "provider_key"}),
    "ingress": frozenset({"app_dsn", "provider_key"}),
    "bridge": frozenset({"app_dsn"}),
    "scheduler": frozenset({"app_dsn"}),
    "finalizer": frozenset({"app_dsn"}),
    "detectors": frozenset({"app_dsn"}),
    "canonical-stock-alerts": frozenset({"app_dsn"}),
    "publication-retry": frozenset({"app_dsn"}),
    "options-lifecycle": frozenset({"app_dsn", "provider_key"}),
    "options-scheduler": frozenset({"app_dsn", "provider_key"}),
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
FENCED_ROLE_NAMES = {
    "controller": "controller",
    "capability-refresh": "capability-refresh",
    "preparation": "equity-preparation",
    "ingress": "equity-ingress",
    "bridge": "bridge",
    "scheduler": "scheduler",
    "finalizer": "finalizer",
    "detectors": "detectors",
    "canonical-stock-alerts": "canonical-stock-alerts",
    "publication-retry": "publication-retry",
    "options-lifecycle": "options-lifecycle",
    "options-scheduler": "options-scheduler",
    "options-v3-acquisition": "options-acquisition",
    "options-p8-ingress": "options-ingress",
    "options-p8-market-state": "options-market-state",
    "options-p8-flow": "options-flow",
    "options-p8-window": "options-window",
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


def run_fenced_child(
    component,
    command,
    environment,
    *,
    process_factory=subprocess.Popen,
    clock=time.monotonic,
) -> int:
    role_name = FENCED_ROLE_NAMES[component]
    pool = ThreadedConnectionPool(
        1,
        3 if component == "options-lifecycle" else 2,
        dsn=environment["REALTIME_V2_APP_DSN"],
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    stop = threading.Event()
    owner_id = uuid4()
    identity = (
        f"compose:{component}:{os.getpid()}:{environment.get('HOSTNAME','')}"
    )
    group = (
        PostgresLeaseHeartbeat(
            provider,
            resource_key=RUNTIME_GROUP_RESOURCE,
            owner_id=owner_id,
            runtime_kind="COMPOSE",
            role_name="runtime",
            process_identity=identity,
            stop_event=stop,
        )
        if component == "options-lifecycle"
        else None
    )
    role = PostgresLeaseHeartbeat(
        provider,
        resource_key=role_resource_key(role_name),
        owner_id=owner_id,
        runtime_kind="COMPOSE",
        role_name=role_name,
        process_identity=identity,
        stop_event=stop,
    )
    process = None

    def request_stop(*_unused):
        stop.set()
        if process is not None and process.poll() is None:
            process.terminate()

    prior_int = signal.getsignal(signal.SIGINT)
    prior_term = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        if group is not None:
            group.start()
        role.start()
        process = process_factory(command, env=environment)
        if group is not None:
            group.mark_running()
        role.mark_running()
        while process.poll() is None:
            if stop.wait(0.5):
                if process.poll() is None:
                    process.terminate()
                break
        return process.wait()
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        role.close()
        if group is not None:
            group.close()
        pool.closeall()
        signal.signal(signal.SIGINT, prior_int)
        signal.signal(signal.SIGTERM, prior_term)


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
    if args.component in FENCED_ROLE_NAMES:
        return run_fenced_child(
            args.component,
            args.command,
            environment,
        )
    return run_child(args.command, environment)


if __name__ == "__main__":
    raise SystemExit(main())
