#!/usr/bin/env python3
"""Validate three V3 windows on host using captured provider inputs and disposable DB."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import time
from collections import deque
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
TEST_SUPPORT = BACKEND_DIR / "tests" / "realtime"
for path in (BACKEND_DIR, TEST_SUPPORT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from realtime.advanced_equity_ingress import (  # noqa: E402
    AdvancedEquityJournalPump,
)
from realtime.advanced_options_acquisition_runtime import (  # noqa: E402
    acquisition_binding_payload,
    run_acquisition,
)
from realtime.advanced_options_service import (  # noqa: E402
    AdvancedOptionsProcessKind,
)
from realtime.advanced_realtime_session_operations import (  # noqa: E402
    build_diagnostic_authorization,
    diagnostic_authorization_payload,
)
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import (  # noqa: E402
    AssetClass,
    MarketFeed,
    RawMessageEnvelope,
    canonical_sha256,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.normalizer import InstrumentAdmission  # noqa: E402
from realtime.security_v2 import (  # noqa: E402
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from scripts import qualify_advanced_options_v3_docker as qualification  # noqa: E402
from scripts.validate_realtime_v2_schema import _connect  # noqa: E402
from v2_database import disposable_v2_database  # noqa: E402


UTC = timezone.utc


def _replay_clock(session_date):
    """Return an authority-valid logical clock for historical replay."""
    if not isinstance(session_date, date):
        raise TypeError("replay session date is invalid")
    return datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        13,
        10,
        tzinfo=UTC,
    )


def _run_replay_acquisition(binding, *, replay_clock, **kwargs):
    if replay_clock.date() != binding.authority.session_date:
        raise ValueError("replay clock is outside acquisition session")
    return run_acquisition(
        binding,
        clock=lambda: replay_clock,
        **kwargs,
    )


def _read_bound(path, expected_sha256):
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != expected_sha256:
        raise ValueError(f"artifact file identity changed: {path}")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise TypeError(f"artifact must be a JSON object: {path}")
    semantic = value.get("semantic_sha256")
    document = dict(value)
    document.pop("semantic_sha256", None)
    if not isinstance(semantic, str) or canonical_sha256(document) != semantic:
        raise ValueError(f"artifact semantic identity changed: {path}")
    return value


def _write(path, value):
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


class ArtifactConnection:
    def __init__(self, frames):
        self.incoming = deque([
            json.dumps(
                [{"ev": "status", "status": "auth_success"}],
                separators=(",", ":"),
            ).encode("ascii"),
            json.dumps(
                [{"ev": "status", "status": "success"}],
                separators=(",", ":"),
            ).encode("ascii"),
            *frames,
        ])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def send(self, _payload):
        return None

    async def recv(self, *, decode=None):
        del decode
        return self.incoming.popleft()

    async def close(self, *, code, reason):
        del code, reason


def _frames(value, name):
    rows = value[name]["frames"]
    return [
        base64.b64decode(row["payload_base64"], validate=True)
        for row in rows
    ]


def _equity_journal(websocket, output):
    buffer = RawMessageBuffer(
        BufferLimits(100, 64 * 1024 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    admissions = {
        ticker: InstrumentAdmission(
            instrument_id=index,
            provider_symbol=ticker,
            underlying=ticker,
            asset_class=AssetClass.EQUITY,
            shard_id=0,
        )
        for index, ticker in enumerate(
            sorted({
                symbol.split(".", 1)[1]
                for symbol in websocket["equity"]["subscriptions"]
            }),
            start=1,
        )
    }
    now_ns = time.monotonic_ns()
    for index, row in enumerate(websocket["equity"]["frames"]):
        buffer.put_nowait(RawMessageEnvelope(
            stream_session_id=UUID(row["stream_session_id"]),
            message_number=int(row["message_number"]),
            feed=MarketFeed.STOCKS,
            received_at=datetime.fromisoformat(row["received_at_utc"]),
            received_monotonic_ns=now_ns + index,
            payload=base64.b64decode(
                row["payload_base64"],
                validate=True,
            ),
        ))
    generation = uuid5(
        NAMESPACE_URL,
        f"host-equity-journal:{websocket['semantic_sha256']}",
    )
    journal = output / "equity-journal.sqlite"
    pump = AdvancedEquityJournalPump(
        buffer,
        journal,
        generation,
        admissions,
        maximum_batch_messages=100,
        maximum_batch_bytes=64 * 1024 * 1024,
    )
    pump.start()
    deadline = time.monotonic() + 30
    while buffer.snapshot().queued_events and time.monotonic() < deadline:
        time.sleep(0.05)
    pump.request_stop()
    progress = pump.join(30)
    if (
        progress.last_raw_offset <= 0
        or progress.last_event_offset <= 0
    ):
        raise RuntimeError("captured Equity frames did not ingest")
    return {
        "journal_generation": str(generation),
        "journal_path": str(journal),
        "journal_file_sha256": hashlib.sha256(
            journal.read_bytes()
        ).hexdigest(),
        "last_raw_offset": progress.last_raw_offset,
        "last_event_offset": progress.last_event_offset,
        "admitted_market_messages": progress.admitted_market_messages,
        "admitted_market_bytes": progress.admitted_market_bytes,
    }


def _contract_rows(contracts):
    rows = []
    for ticker in contracts:
        strike = str(int(ticker[-8:]) / 1000)
        rows.append((ticker, ticker[11], strike))
    return tuple(rows)


def _safe_environment(environment):
    return {
        name: value
        for name, value in environment.items()
        if name.upper() not in {"POLYGON_API_KEY", "MASSIVE_API_KEY"}
        and not any(
            token in name.upper()
            for token in ("BROKER", "DELIVERY", "TRADING", "EXECUTION")
        )
    }


def _role_command(kind, package, bundle):
    return [
        sys.executable,
        "-B",
        str(BACKEND_DIR / "scripts" / "run_advanced_options_v3_production.py"),
        "--docker-service-kind",
        kind.value,
        "--package",
        str(package),
        "--execution-bundle",
        str(bundle),
    ]


def _start_process(name, command, output, environment, *, suffix=""):
    stem = name.casefold() + suffix
    stdout = (output / f"{stem}.out").open("wb")
    stderr = (output / f"{stem}.err").open("wb")
    process = subprocess.Popen(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        stdout=stdout,
        stderr=stderr,
    )
    return process, (stdout, stderr)


def _processes(output, environment, package, bundle, seed, diagnostic):
    safe_environment = _safe_environment(environment)
    commands = {
        kind.value: _role_command(kind, package, bundle)
        for kind in AdvancedOptionsProcessKind
    }
    commands["CANONICAL_STOCK_ALERTS"] = [
        sys.executable,
        "-B",
        "-u",
        str(BACKEND_DIR / "scripts" / "run_realtime_v2_canonical_stock_alerts.py"),
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--cohort-id",
        seed["cohort_id"],
        "--session-date",
        environment["REALTIME_V2_ADVANCED_SESSION_DATE"],
        "--operation",
        "DIAGNOSTIC_NON_SERVING",
        "--diagnostic-authorization",
        str(diagnostic),
        "--continuous",
        "--execute",
    ]
    processes = {}
    handles = []
    for name, command in commands.items():
        process, opened = _start_process(
            name,
            command,
            output,
            safe_environment,
        )
        processes[name] = process
        handles.extend(opened)
    return processes, handles


def _stop_processes(processes, handles):
    for process in processes.values():
        if process.poll() is None:
            process.terminate()
    for process in processes.values():
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    for handle in handles:
        handle.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--websocket-capture", type=Path, required=True)
    parser.add_argument("--websocket-file-sha256", required=True)
    parser.add_argument("--rest-summary", type=Path, required=True)
    parser.add_argument("--rest-file-sha256", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in ("websocket_capture", "rest_summary", "output_directory"):
        setattr(args, name, getattr(args, name).resolve())
    if (
        not args.websocket_capture.is_file()
        or not args.rest_summary.is_file()
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.execute
    ):
        parser.error("host pipeline requires exact artifacts and new output")
    return args


def execute(args):
    websocket = _read_bound(
        args.websocket_capture,
        args.websocket_file_sha256,
    )
    rest = _read_bound(args.rest_summary, args.rest_file_sha256)
    if (
        websocket.get("status") != "PASS"
        or rest.get("status") != "PASS"
        or rest.get("provider_requests") != 3
        or len(rest.get("cycles", ())) != 3
    ):
        raise ValueError("provider artifacts are incomplete")
    contracts = tuple(
        sorted(rest["cycles"][0]["contracts"])
    )
    if any(
        tuple(sorted(row["contracts"])) != contracts
        for row in rest["cycles"]
    ):
        raise ValueError("REST contract membership changed")
    args.output_directory.mkdir()
    equity = _equity_journal(websocket, args.output_directory)
    suffix = uuid4().hex[:12]
    database = f"test_realtime_v2_v3host_{suffix}"
    roles = build_role_names_v2(f"test_rt_v3host_{suffix}")
    settings = qualification._settings()
    login = roles.prefix + "_worker"
    login_owned = False
    roles_owned = False
    worker_pool = None
    processes = {}
    handles = []
    durable = None
    acquisition_report = None
    restart_evidence = None
    started_at = datetime.now(UTC)
    try:
        with disposable_v2_database(database) as admin_pool:
            seed = qualification._seed(
                admin_pool,
                session_date=args.session_date,
                suffix=suffix,
            )
            qualification._seed_acquisition_catalog(
                admin_pool,
                contracts=_contract_rows(contracts),
                expiration=args.session_date,
            )
            connection = admin_pool.getconn()
            try:
                _, security = harden_database_v2(connection, roles)
                roles_owned = True
                if any(
                    not verdict.startswith("PASS")
                    for _, verdict in security
                ):
                    raise RuntimeError("host disposable security audit failed")
            finally:
                admin_pool.putconn(connection)
            login, host_dsn = qualification._worker_login(
                settings,
                database,
                roles,
            )
            login_owned = True
            worker_pool = ThreadedConnectionPool(
                1,
                10,
                dsn=host_dsn,
            )
            provider = V2ConnectionProvider(
                {APP_ROLE: worker_pool},
                V2AccessConfig(
                    security_mode="hardened",
                    role_prefix=roles.prefix,
                    allow_disposable=True,
                ),
            )
            provider.verify()
            source_identity = canonical_sha256({
                "websocket": websocket["semantic_sha256"],
                "rest": rest["semantic_sha256"],
            })
            runtime_identity = "sha256:" + hashlib.sha256(
                Path(sys.executable).read_bytes()
            ).hexdigest()
            helper_args = SimpleNamespace(
                session_date=args.session_date,
                qualification_semantic_sha256=source_identity,
                qualification_file_sha256=canonical_sha256({
                    "websocket_file": args.websocket_file_sha256,
                    "rest_file": args.rest_file_sha256,
                }),
                image_id=runtime_identity,
            )
            replay_clock = _replay_clock(args.session_date)
            options_frames = _frames(websocket, "options")
            binding = qualification._qualification_acquisition_binding(
                seed,
                helper_args,
                contract_tickers=contracts,
                now=replay_clock,
                connection_attempt_ceiling=1,
                decoded_message_ceiling=2 + len(options_frames),
                decoded_byte_ceiling=32 * 1024 * 1024,
            )
            package, bundle = qualification._qualification_artifacts(
                seed,
                helper_args,
                binding,
            )
            qualification._seed_service_leases(admin_pool, bundle)
            package_path = args.output_directory / "package.json"
            bundle_path = args.output_directory / "execution-bundle.json"
            binding_path = args.output_directory / "acquisition-binding.json"
            downstream = args.output_directory / "downstream-bindings"
            status_dir = args.output_directory / "p8-status"
            journal_root = args.output_directory / "journals"
            downstream.mkdir()
            status_dir.mkdir()
            _write(package_path, package)
            _write(bundle_path, bundle)
            _write(binding_path, acquisition_binding_payload(binding))
            acquisition_report = _run_replay_acquisition(
                binding,
                replay_clock=replay_clock,
                provider=provider,
                api_key="artifact-replay-no-provider",
                journal_root=journal_root,
                status_path=args.output_directory
                / "options-acquisition-status.json",
                lock_path=args.output_directory / "options.lock",
                stop_event=__import__("threading").Event(),
                connection_factory=lambda _url: ArtifactConnection(
                    options_frames
                ),
                diagnostic=False,
            )
            diagnostic = build_diagnostic_authorization(
                authorization_reference=f"host-pipeline:{suffix}",
                owner=f"stockscreener-v3host-{suffix}",
                session_date=args.session_date,
                cohort_id=UUID(seed["cohort_id"]),
                source_sha256=source_identity,
                backend_image_id=runtime_identity,
                input_evidence_sha256=args.websocket_file_sha256,
            )
            diagnostic_path = args.output_directory / "diagnostic.json"
            _write(
                diagnostic_path,
                diagnostic_authorization_payload(diagnostic),
            )
            environment = {
                **os.environ,
                "REALTIME_V2_APP_DSN": host_dsn,
                "REALTIME_V2_ROLE_PREFIX": roles.prefix,
                "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
                "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
                "REALTIME_V3_ACQUISITION_SPLIT": "ENABLED",
                "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH": str(
                    binding_path
                ),
                "REALTIME_V3_OPTIONS_ACQUISITION_STATUS_PATH": str(
                    args.output_directory
                    / "options-acquisition-status.json"
                ),
                "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_DIR": str(
                    downstream
                ),
                "REALTIME_V2_ADVANCED_JOURNAL_DIR": str(journal_root),
                "REALTIME_V2_P8_STATUS_DIR": str(status_dir),
                "REALTIME_V2_ADVANCED_COHORT_ID": seed["cohort_id"],
                "REALTIME_V2_ADVANCED_SESSION_DATE": (
                    args.session_date.isoformat()
                ),
                "REALTIME_V3_DIAGNOSTIC_AUTHORIZATION_ID": str(
                    diagnostic.authorization_id
                ),
            }
            processes, handles = _processes(
                args.output_directory,
                environment,
                package_path,
                bundle_path,
                seed,
                diagnostic_path,
            )
            time.sleep(2)
            cutoffs = [
                datetime.fromisoformat(row["boundary_utc"])
                for row in rest["cycles"]
            ]
            qualification._attach_qualification_downstream_bindings(
                downstream,
                bundle,
                seed,
                helper_args,
                equity_source_sha256=args.websocket_file_sha256,
                cycle_cutoffs=cutoffs,
                options_activity_sha256s=[
                    row["file_sha256"] for row in rest["cycles"]
                ],
            )
            deadline = time.monotonic() + 240
            last_error = None
            while time.monotonic() < deadline:
                failed = {
                    name: process.returncode
                    for name, process in processes.items()
                    if process.poll() not in (None, 0)
                }
                if failed:
                    raise RuntimeError(
                        f"host resident process failed: {failed}"
                    )
                try:
                    durable = qualification._durable_evidence(
                        admin_pool,
                        seed,
                    )
                    break
                except RuntimeError as error:
                    last_error = error
                    time.sleep(1)
            if durable is None:
                raise RuntimeError(
                    "host resident barrier did not complete"
                ) from last_error
            window_status_path = status_dir / "window.json"
            before_restart = json.loads(
                window_status_path.read_text(encoding="utf-8")
            )
            window_process = processes["WINDOW"]
            if window_process.poll() is None:
                window_process.terminate()
                window_process.wait(timeout=15)
            process, opened = _start_process(
                "WINDOW",
                _role_command(
                    AdvancedOptionsProcessKind.WINDOW,
                    package_path,
                    bundle_path,
                ),
                args.output_directory,
                _safe_environment(environment),
                suffix="-restart",
            )
            processes["WINDOW"] = process
            handles.extend(opened)
            restart_deadline = time.monotonic() + 90
            after_restart = None
            while time.monotonic() < restart_deadline:
                if window_status_path.is_file():
                    candidate = json.loads(
                        window_status_path.read_text(encoding="utf-8")
                    )
                    qualification_evidence = (
                        candidate.get("role_evidence") or {}
                    ).get("qualification") or {}
                    if qualification_evidence.get("rehydrated") is True:
                        after_restart = candidate
                        break
                if process.poll() not in (None, 0):
                    raise RuntimeError(
                        "restarted WINDOW host process failed"
                    )
                time.sleep(0.5)
            if after_restart is None:
                raise RuntimeError(
                    "WINDOW host restart did not rehydrate"
                )
            restart_evidence = {
                "before_state": before_restart.get("state"),
                "before_owner_epoch": before_restart.get("owner_epoch"),
                "after_state": after_restart.get("state"),
                "after_owner_epoch": after_restart.get("owner_epoch"),
                "rehydrated": True,
                "same_bundle": (
                    before_restart.get("bundle_sha256")
                    == after_restart.get("bundle_sha256")
                ),
            }
            _stop_processes(processes, handles)
            processes = {}
            handles = []
            worker_pool.closeall()
            worker_pool = None
    finally:
        if processes:
            _stop_processes(processes, handles)
        if worker_pool is not None:
            worker_pool.closeall()
        if login_owned:
            qualification._drop_login(settings, login)
        if roles_owned:
            with closing(_connect(settings, "postgres")) as connection:
                drop_disposable_roles_v2(connection, roles)
                connection.commit()
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)",
                (database,),
            )
            database_exists = bool(cursor.fetchone()[0])
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname LIKE %s",
                (roles.prefix + "%",),
            )
            remaining_roles = [row[0] for row in cursor.fetchall()]
    if database_exists or remaining_roles:
        raise RuntimeError("host disposable cleanup is not empty")
    report = {
        "schema": "advanced-realtime-v3-host-pipeline-validation-v1",
        "status": "PASS",
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "provider_inputs": {
            "websocket_capture_file_sha256": args.websocket_file_sha256,
            "websocket_capture_semantic_sha256": (
                websocket["semantic_sha256"]
            ),
            "rest_summary_file_sha256": args.rest_file_sha256,
            "rest_summary_semantic_sha256": rest["semantic_sha256"],
        },
        "equity_ingress": equity,
        "options_acquisition": acquisition_report,
        "durable_database_barrier": durable,
        "host_restart": restart_evidence,
        "cleanup": {
            "database_exists": database_exists,
            "remaining_roles": remaining_roles,
            "owned_processes": [],
        },
        "effects": {
            "provider_requests_during_replay": 0,
            "provider_websocket_sessions_during_replay": 0,
            "production_database_writes": 0,
            "production_service_actions": 0,
            "docker_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    path = args.output_directory / "host-pipeline-report.json"
    _write(path, report)
    return report, path


def main(argv=None):
    report, path = execute(parse_args(argv))
    print(json.dumps({
        "status": report["status"],
        "semantic_sha256": report["semantic_sha256"],
        "output": str(path),
        "effects": report["effects"],
        "cleanup": report["cleanup"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
