#!/usr/bin/env python3
"""Prepare or run receipt-free Advanced Options session acquisition."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_acquisition_runtime import (  # noqa: E402
    acquisition_binding_from_payload,
    acquisition_binding_payload,
    acquisition_health,
    build_acquisition_binding,
    run_acquisition,
)
from realtime.advanced_realtime_fencing import (  # noqa: E402
    DurableLeaseUnavailable,
    OPTIONS_PHYSICAL_SOCKET_RESOURCE,
    PostgresLeaseHeartbeat,
)
from realtime.advanced_options_p8_runtime import (  # noqa: E402
    LegacyArtifactSessionStateAdapter,
)
from realtime.advanced_options_session_state import (  # noqa: E402
    LEGACY_DIAGNOSTIC_STATE_SOURCE,
    POSTGRES_STATE_SOURCE,
    select_options_session_state,
)
from realtime.advanced_options_v3_authority import (  # noqa: E402
    session_acquisition_authorization_from_payload,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)


UTC = timezone.utc


def _read(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain an object")
    return value


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--pre-session-authority", type=Path, required=True)
    prepare.add_argument("--source-sha256", required=True)
    prepare.add_argument("--backend-image-id", required=True)
    prepare.add_argument("--connection-attempts", type=int, required=True)
    prepare.add_argument("--decoded-messages", type=int, required=True)
    prepare.add_argument("--decoded-bytes", type=int, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--execute", action="store_true")

    for name in ("run", "health"):
        command = commands.add_parser(name)
        command.add_argument("--database", required=True)
        command.add_argument("--confirm-database-name", required=True)
        command.add_argument(
            "--state-source",
            choices=(
                POSTGRES_STATE_SOURCE,
                LEGACY_DIAGNOSTIC_STATE_SOURCE,
            ),
            default=POSTGRES_STATE_SOURCE,
        )
        command.add_argument("--binding", type=Path)
        command.add_argument("--status-path", type=Path)
        if name == "run":
            command.add_argument("--journal-root", type=Path)
            command.add_argument("--lock-path", type=Path)
            command.add_argument("--continuous", action="store_true")
            command.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in (
        "pre_session_authority",
        "binding",
        "status_path",
        "journal_root",
        "lock_path",
        "output",
    ):
        value = getattr(args, name, None)
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    if args.command == "prepare":
        if (
            not args.pre_session_authority.is_file()
            or args.output.exists()
            or not args.execute
        ):
            parser.error(
                "prepare requires an existing authority, new output and "
                "--execute"
            )
        return args
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or (
            args.state_source == POSTGRES_STATE_SOURCE
            and args.binding is not None
        )
        or (
            args.state_source == LEGACY_DIAGNOSTIC_STATE_SOURCE
            and (
                args.binding is None
                or not args.binding.is_file()
            )
        )
    ):
        parser.error("Options acquisition runtime scope is invalid")
    if args.state_source == POSTGRES_STATE_SOURCE:
        journal_root = Path(
            os.environ.get(
                "REALTIME_V2_ADVANCED_JOURNAL_DIR",
                BACKEND_DIR / ".realtime-canary" / "advanced-journals",
            )
        )
        status_path = Path(
            os.environ.get(
                "REALTIME_V3_OPTIONS_ACQUISITION_STATUS_PATH",
                journal_root / "options-acquisition-status.json",
            )
        )
        lock_path = Path(
            os.environ.get(
                "REALTIME_V3_OPTIONS_ACQUISITION_LOCK_PATH",
                journal_root / "options-acquisition-provider.lock",
            )
        )
        args.status_path = args.status_path or status_path
        if args.command == "run":
            args.journal_root = args.journal_root or journal_root
            args.lock_path = args.lock_path or lock_path
    if args.status_path is None:
        parser.error("Options acquisition status path is unavailable")
    args.status_path = args.status_path.resolve()
    if args.command == "run":
        args.journal_root = args.journal_root.resolve()
        args.lock_path = args.lock_path.resolve()
    if args.command == "run" and (
        not args.continuous or not args.execute
    ):
        parser.error("run requires --continuous --execute")
    return args


def _binding(path: Path):
    return LegacyArtifactSessionStateAdapter(
        acquisition_binding_path=path,
        json_loader=_read,
        acquisition_parser=acquisition_binding_from_payload,
    ).load_acquisition_binding()


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.environ.get("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    qualification = (
        os.environ.get("REALTIME_V3_VALIDATION_MODE")
        == "DEVELOPMENT_DIAGNOSTIC"
        and os.environ.get(
            "REALTIME_V3_PROVIDER_FREE_QUALIFICATION"
        )
        == "ENABLED"
    )
    role_prefix = (
        os.environ.get("REALTIME_V2_ROLE_PREFIX", "").strip()
        if qualification
        else EXPECTED_V2_DATABASE_NAME
    )
    if not role_prefix:
        raise RuntimeError("Options acquisition role prefix is unavailable")
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=role_prefix,
            statement_timeout_ms=300_000,
            allow_disposable=qualification,
        ),
    )
    provider.verify()
    return provider, pool, qualification


class _FixtureConnection:
    def __init__(self, binding, *, now: datetime | None = None):
        observed_at = datetime.now(UTC) if now is None else now
        now_ms = int(observed_at.timestamp() * 1000)
        incoming = [
            [{"ev": "status", "status": "auth_success"}],
            [{"ev": "status", "status": "success"}],
        ]
        for index, ticker in enumerate(binding.contract_tickers, start=1):
            incoming.append([
                {
                    "ev": "T",
                    "sym": ticker,
                    "p": 1.0 + index / 10,
                    "s": 10,
                    "x": 302,
                    "t": now_ms,
                    "q": index * 2 - 1,
                },
                {
                    "ev": "Q",
                    "sym": ticker,
                    "bp": 1.0,
                    "ap": 1.1,
                    "bs": 10,
                    "as": 12,
                    "t": now_ms,
                    "q": index * 2,
                },
            ])
        self.incoming = deque(
            json.dumps(row, separators=(",", ":")).encode("ascii")
            for row in incoming
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def send(self, _payload):
        return None

    async def recv(self, *, decode=None):
        del decode
        return self.incoming.popleft()

    async def close(self, *, code, reason):
        del code, reason


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.command == "prepare":
        authority_document = _read(args.pre_session_authority)
        authority = session_acquisition_authorization_from_payload(
            authority_document["session_acquisition"]
        )
        binding = build_acquisition_binding(
            source_sha256=args.source_sha256,
            backend_image_id=args.backend_image_id,
            authority=authority,
            contract_tickers=tuple(
                str(value)
                for value in authority_document["admitted_contracts"]
            ),
            connection_attempt_ceiling=args.connection_attempts,
            decoded_message_ceiling=args.decoded_messages,
            decoded_byte_ceiling=args.decoded_bytes,
        )
        _write(args.output, acquisition_binding_payload(binding))
        print(json.dumps({
            "status": "PREPARED",
            "binding_id": str(binding.binding_id),
            "binding_sha256": binding.binding_sha256,
            "owner_id": str(binding.owner_id),
            "journal_generation": str(
                binding.authority.journal_generation
            ),
            "provider_effects": 0,
            "production_effects": 0,
            "output": str(args.output),
        }, indent=2, sort_keys=True))
        return 0
    state_provider = None
    pool = None
    diagnostic = False
    if (
        args.state_source == POSTGRES_STATE_SOURCE
        or args.command == "run"
    ):
        provider, pool, diagnostic = _provider()
        if args.state_source == POSTGRES_STATE_SOURCE:
            state_provider = provider
    if args.command == "health":
        try:
            state = select_options_session_state(
                environment=os.environ,
                provider=state_provider,
                acquisition_binding_path=(
                    args.binding
                    if args.state_source
                    == LEGACY_DIAGNOSTIC_STATE_SOURCE
                    else None
                ),
                checked_at=datetime.now(UTC),
                state_source=args.state_source,
            )
            try:
                binding = state.load_acquisition_binding()
            except RuntimeError as error:
                if (
                    str(error)
                    != "current PostgreSQL Options session "
                    "state is unavailable"
                ):
                    raise
                print(json.dumps({
                    "status": "READY_WAITING_FOR_SESSION",
                    "provider_effects": 0,
                }, indent=2, sort_keys=True))
                return 0
            diagnostic_clock = (
                binding.authority.acquisition_start
                + timedelta(seconds=1)
                if diagnostic
                else None
            )
            print(json.dumps(
                acquisition_health(
                    args.status_path,
                    binding,
                    checked_at=diagnostic_clock or datetime.now(UTC),
                ),
                indent=2,
                sort_keys=True,
            ))
            return 0
        finally:
            if pool is not None:
                pool.closeall()
    if pool is None:
        raise RuntimeError("Options acquisition database is unavailable")
    stop = threading.Event()
    active_stop = [None]
    physical_owner_id = uuid4()

    def request_stop(*_unused):
        stop.set()
        if active_stop[0] is not None:
            active_stop[0].set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.environ.get("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if diagnostic:
        api_key = "provider-free-diagnostic"
    try:
        while not stop.is_set():
            state = select_options_session_state(
                environment=os.environ,
                provider=state_provider,
                acquisition_binding_path=(
                    args.binding
                    if args.state_source
                    == LEGACY_DIAGNOSTIC_STATE_SOURCE
                    else None
                ),
                checked_at=datetime.now(UTC),
                state_source=args.state_source,
            )
            try:
                binding = state.load_acquisition_binding()
            except RuntimeError as error:
                if (
                    str(error)
                    != "current PostgreSQL Options session "
                    "state is unavailable"
                ):
                    raise
                print(json.dumps({
                    "status": "READY_WAITING_FOR_SESSION",
                    "provider_effects": 0,
                }, sort_keys=True), flush=True)
                stop.wait(2.0)
                continue
            selection_plan = state.load_c6_selection_plan()
            diagnostic_clock = (
                binding.authority.acquisition_start
                + timedelta(seconds=1)
                if diagnostic
                else None
            )
            session_stop = threading.Event()
            active_stop[0] = session_stop
            physical_fence = PostgresLeaseHeartbeat(
                provider,
                resource_key=OPTIONS_PHYSICAL_SOCKET_RESOURCE,
                owner_id=physical_owner_id,
                stop_event=session_stop,
            )
            try:
                physical_fence.start()
            except DurableLeaseUnavailable as error:
                active_stop[0] = None
                print(json.dumps({
                    "status": "READY_WAITING_FOR_PHYSICAL_OWNER",
                    "reason": str(error),
                    "provider_effects": 0,
                }, sort_keys=True), flush=True)
                stop.wait(2.0)
                continue
            try:
                report = run_acquisition(
                    binding,
                    provider=provider,
                    api_key=api_key,
                    journal_root=args.journal_root,
                    status_path=args.status_path,
                    lock_path=args.lock_path,
                    stop_event=session_stop,
                    connection_factory=(
                        (
                            lambda _: _FixtureConnection(
                                binding,
                                now=diagnostic_clock,
                            )
                        )
                        if diagnostic
                        else None
                    ),
                    clock=(
                        (lambda: diagnostic_clock)
                        if diagnostic_clock is not None
                        else (lambda: datetime.now(UTC))
                    ),
                    diagnostic=diagnostic,
                    selection_plan=selection_plan,
                )
            finally:
                physical_fence.close()
                active_stop[0] = None
            print(json.dumps(report, indent=2, sort_keys=True))
            if args.state_source == LEGACY_DIAGNOSTIC_STATE_SOURCE:
                break
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
