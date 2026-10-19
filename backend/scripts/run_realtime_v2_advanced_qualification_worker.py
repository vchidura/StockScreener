#!/usr/bin/env python3
"""Measure one synthetic, projection-only V3 window in an isolated Docker process."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import psutil
import requests
from psycopg2.extensions import parse_dsn
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.advanced_gateway import AdvancedGateway, GatewayConfiguration  # noqa: E402
from realtime.advanced_ingress_runtime_profile import load_advanced_ingress_runtime_catalog  # noqa: E402
from realtime.advanced_scheduler import ADVANCED_WINDOW_BUILD_ID, discover_advanced_window_recovery_cursor  # noqa: E402
from realtime.bridge import drain_raw_buffer_once  # noqa: E402
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import AssetClass, MarketChannel, SubscriptionManifest, SubscriptionMember, UniverseManifest  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_live024_compatibility import (  # noqa: E402
    CONTRACT_ID,
    ENABLED_STAGES,
    LIVE024_REGISTRY_SHA256,
    LIVE024_SCHEMA_SHA256,
    RUNTIME_PATHS,
    live024_migration_rows,
    source_bundle_identity,
)
from realtime.equity_interval_finalizer import enqueue_five_minute_finalizer_jobs_cursor, run_five_minute_finalizer_once  # noqa: E402
from realtime.equity_worker_service import EquityWorkerConfig  # noqa: E402
from realtime.journal import RawJournalReader, RawJournalWriter  # noqa: E402
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION, normalize_raw_message  # noqa: E402
from realtime.orchestration_repository import PostgresOrchestrationRepository, WindowSpec  # noqa: E402
from realtime.runner_environment import RunnerEnvironment  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from realtime.stage_graph import STAGES_V3  # noqa: E402
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome  # noqa: E402
from realtime.stage_runners_equity import FINALIZER_POLICY_SHA256, reuse_advanced_input_ready_runner  # noqa: E402
from realtime.stage_runners_v2 import build_runners  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc
ENABLED = frozenset(ENABLED_STAGES)
PROHIBITED_ENV = (
    "POLYGON_API_KEY", "FINNHUB_API_KEY", "MASSIVE_API_KEY",
    "REALTIME_V2_APP_DSN", "REALTIME_V2_API_DSN", "REALTIME_V2_ADMIN_DSN",
    "POSTGRES_ADMIN_PASSWORD",
)


def reject_provider_io(*_args, **_kwargs):
    raise RuntimeError("provider I/O is prohibited in isolated qualification")


class ReplayConnection:
    def __init__(self, payloads):
        self.incoming = deque((
            b'[{"ev":"status","status":"auth_success"}]',
            b'[{"ev":"status","status":"success"}]',
            *payloads,
        ))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def send(self, _payload):
        pass

    async def recv(self, *, decode):
        if decode is not False:
            raise RuntimeError("qualification must use the byte gateway")
        return self.incoming.popleft()

    async def close(self, **_kwargs):
        pass


def validate_scope(manifest, dsn):
    database = manifest["database"]
    if not database.startswith("test_realtime_v2_p8q_") or parse_dsn(dsn).get("dbname") != database:
        raise ValueError("qualification refuses non-owned disposable targets")
    if manifest["symbols"] != ["SPY"] or manifest["enabled_stages"] != sorted(ENABLED):
        raise ValueError("qualification must remain exact SPY projection-only")
    if (
        manifest.get("universe_target") != 1
        or manifest.get("maximum_stream_sessions") != 2
        or manifest.get("maximum_connection_attempts") != 2
        or manifest.get("maximum_decoded_bytes") != 10 * 1024 * 1024
        or manifest.get("maximum_rest_repair_requests") != 0
        or manifest.get("provider_requests_authorized") != 0
        or manifest.get("publication_authorized") is not False
    ):
        raise ValueError("qualification budget or product scope is not exact")
    if (
        manifest.get("contract_id") != CONTRACT_ID
        or manifest.get("schema_sha256") != LIVE024_SCHEMA_SHA256
        or manifest.get("registry_sha256") != LIVE024_REGISTRY_SHA256
        or tuple(manifest.get("migrations") or ()) != live024_migration_rows()
    ):
        raise ValueError("qualification does not bind the exact live-024 contract")
    if not manifest["role_prefix"].startswith("test_rt_p8q_"):
        raise ValueError("qualification role prefix is not disposable")
    if parse_dsn(dsn).get("user") != manifest["role_prefix"] + "_worker":
        raise ValueError("qualification refuses an unowned login credential")
    if any(os.getenv(name) for name in PROHIBITED_ENV):
        raise RuntimeError("qualification received an unowned credential")


def validate_image_paths(root):
    if any(root.glob(".env*")) or any(
        (root / name).exists() for name in (".realtime-canary", "tests", "backups", ".cache")
    ):
        raise RuntimeError("qualification image contains an unowned path")


def snapshot(provider):
    relations = (
        "rt.bar_catalog", "rt.equity_evidence", "rt.screening_generations",
        "rt.materialization_jobs", "rt.portal_releases", "rt.portal_release_current",
        "rt.release_current", "rt.portal_cohort_current", "rt.alert_outbox", "rt.detector_events",
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        counts = {}
        for relation in relations:
            unit.cursor.execute(f"SELECT count(*) FROM {relation}")
            counts[relation] = int(unit.cursor.fetchone()[0])
        unit.cursor.execute(
            "SELECT count(*) FROM rt.stage_runs "
            "WHERE stage_id IN ('STOCK_ALERTS','STRATEGY_PUBLISH','PORTAL_PUBLISH')"
        )
        counts["prohibited_stage_runs"] = int(unit.cursor.fetchone()[0])
    for relation in relations[4:]:
        if counts[relation]:
            raise RuntimeError(f"qualification unexpectedly produced {relation}")
    if counts["prohibited_stage_runs"]:
        raise RuntimeError("qualification ran an unapproved stage")
    return counts


def negative_runtime_cases(provider, manifest, window):
    def rejected(cursor, statement, parameters=()):
        cursor.execute("SAVEPOINT live024_negative")
        try:
            cursor.execute(statement, parameters)
        except Exception as error:
            cursor.execute("ROLLBACK TO SAVEPOINT live024_negative")
            cursor.execute("RELEASE SAVEPOINT live024_negative")
            return {
                "rejected": True,
                "error_type": type(error).__name__,
                "pgcode": getattr(error, "pgcode", None),
            }
        cursor.execute("ROLLBACK TO SAVEPOINT live024_negative")
        cursor.execute("RELEASE SAVEPOINT live024_negative")
        raise RuntimeError("live-024 negative operation unexpectedly succeeded")

    with provider.transaction(APP_ROLE) as unit:
        ddl = rejected(
            unit.cursor,
            "CREATE TABLE rt.live024_forbidden_runtime_ddl (id integer)",
        )
        options_receipt = rejected(
            unit.cursor,
            "INSERT INTO rt.cycle_receipts ("
            "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
            "reference_generation,universe_sha256,input_sha256,build_id"
            ") VALUES (%s,%s,%s,'OPTIONS_INPUT_READY',%s,1,%s,%s,%s)",
            (
                str(uuid4()),
                str(window.window_id),
                manifest["cohort_id"],
                datetime.fromisoformat(manifest["cutoff"]),
                "a" * 64,
                "b" * 64,
                "live024-negative",
            ),
        )
        counter_reset = rejected(
            unit.cursor,
            "UPDATE rt.advanced_ingress_plan_usage "
            "SET connection_attempt_count=connection_attempt_count-1 "
            "WHERE plan_id=%s",
            (manifest["ingress_plan_id"],),
        )
        unit.cursor.execute(
            "SELECT has_table_privilege(%s,'rt.capability_profiles','INSERT'),"
            "has_schema_privilege(%s,'rt','CREATE')",
            (
                manifest["role_prefix"] + "_api",
                manifest["role_prefix"] + "_api",
            ),
        )
        api_insert, api_create = unit.cursor.fetchone()
    if api_insert or api_create:
        raise RuntimeError("live-024 API role has a forbidden write or DDL privilege")
    return {
        "runtime_ddl": ddl,
        "new_options_receipt": options_receipt,
        "counter_reset": counter_reset,
        "api_write_and_ddl_rejected": True,
    }


def replay(provider, manifest, output, settings):
    cohort_id = UUID(manifest["cohort_id"])
    instrument_id = manifest["instrument_id"]
    cutoff = datetime.fromisoformat(manifest["cutoff"])
    gateway_manifest = SubscriptionManifest(
        version="p8-isolated-synthetic-v1", session_date=cutoff.date(),
        universe=UniverseManifest("p8-isolated-synthetic-v1", ("SPY",), ()),
        capability_profile_sha256="a" * 64, allocation_policy_sha256="b" * 64,
        members=(SubscriptionMember(AssetClass.EQUITY, "SPY", "SPY", MarketChannel.MINUTE_AGGREGATE),),
    )
    database = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
    observed = []
    for generation in manifest["generations"]:
        payloads = tuple(value.encode("ascii") for value in generation["payloads"])
        buffer = RawMessageBuffer(
            BufferLimits(settings.buffer_event_limit, settings.buffer_byte_limit, settings.buffer_max_age_nanoseconds),
            monotonic_ns=time.monotonic_ns,
        )
        gateway = AdvancedGateway(
            GatewayConfiguration.stocks(), gateway_manifest, buffer,
            api_key="synthetic-not-a-provider-key",
            connection_factory=lambda _configuration: ReplayConnection(payloads),
            utc_clock=lambda: cutoff + timedelta(seconds=1),
            monotonic_ns=time.monotonic_ns,
            session_id_factory=lambda: UUID(generation["session_id"]),
        )
        started = time.perf_counter()
        result = asyncio.run(gateway.run_session(maximum_market_messages=len(payloads)))
        path = output / f"{generation['journal_generation']}.sqlite"
        with RawJournalWriter(path, generation=UUID(generation["journal_generation"])) as writer:
            while drain_raw_buffer_once(
                buffer, writer, maximum_events=settings.journal_batch_messages,
                maximum_bytes=settings.journal_batch_bytes,
            ) is not None:
                pass
            for raw in RawJournalReader(path).read_after(0):
                batch = normalize_raw_message(raw, {
                    "SPY": InstrumentAdmission(instrument_id, "SPY", "SPY", AssetClass.EQUITY, 0),
                })
                if batch.quarantines or any(event.event_kind is not MarketChannel.MINUTE_AGGREGATE for event in batch.events):
                    raise RuntimeError("synthetic qualification input left the AM scope")
                writer.commit_normalized(batch)
        bridged = process_advanced_equity_journal_once(
            database, RawJournalReader(path),
            AdvancedEquityJournalBridgeConfig(
                cohort_id, UUID(generation["journal_generation"]), UUID(generation["session_id"]),
                generation["owner_epoch"], NORMALIZER_VERSION, (0,), settings.bridge_batch_limit,
            ),
            checked_at=datetime.now(UTC),
        )
        if bridged.committed_offset != len(payloads):
            raise RuntimeError("qualification bridge did not drain every durable offset")
        observed.append({
            "frames": len(payloads), "payload_bytes": sum(map(len, payloads)),
            "admitted_frames": result.accepted_messages, "persisted_bars": bridged.persisted_bars,
            "committed_offset": bridged.committed_offset,
            "seconds": time.perf_counter() - started,
        })
    return observed


def run_window(provider, manifest, window, *, recovery=None):
    environment = RunnerEnvironment(
        provider, build_id=ADVANCED_WINDOW_BUILD_ID, freeze_heap=True,
        input_known_by=None if recovery is None else recovery.input_known_by,
    )
    runners = dict(build_runners(ENABLED, provider, environment=environment))
    if recovery is not None:
        runners["EQUITY_MATERIALIZE"] = reuse_advanced_input_ready_runner(
            environment, receipt=recovery.input_receipt,
            input_known_by=recovery.input_known_by,
            expected_gate_sha256=recovery.input_gate_sha256,
        )
        runners["EQUITY_PROJECTION"] = lambda _context: StageOutcome.complete(receipt=recovery.portal_receipt)
    timings = {}
    for stage_id, runner in tuple(runners.items()):
        def timed(context, execute=runner, stage=stage_id):
            started = time.perf_counter()
            try:
                return execute(context)
            finally:
                timings[stage] = time.perf_counter() - started
        runners[stage_id] = timed
    repository = PostgresOrchestrationRepository(provider)
    orchestrator = StageOrchestrator(
        repository, runners, database=provider, stages=STAGES_V3,
        catalog_version="stages_v3", max_lane_seconds=300,
    )
    orchestrator.start(UUID(manifest["plan_id"]))
    started = time.perf_counter()
    result = orchestrator.run_window(window.window_id)
    if result.window_state != "COMPLETE":
        states = repository.stage_states(window.window_id)
        detail = {
            stage_id: {
                "state": record.state,
                "reason_codes": record.reason_codes,
                "attempt": record.attempt,
            }
            for stage_id, record in sorted(states.items())
        }
        raise RuntimeError(
            "qualification window did not complete: "
            f"{result.window_state}; lanes={result.lane_errors}; "
            f"stages={detail}"
        )
    return {"seconds": time.perf_counter() - started, "stage_seconds": timings}


def finalize(provider, manifest, window):
    compute = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
    with compute.transaction() as cursor:
        enqueue_five_minute_finalizer_jobs_cursor(
            cursor, cohort_id=UUID(manifest["cohort_id"]), window_id=window.window_id,
            instrument_ids=(manifest["instrument_id"],), checked_at=datetime.now(UTC),
            policy_sha256=FINALIZER_POLICY_SHA256, build_id=ADVANCED_WINDOW_BUILD_ID,
            barrier_id=uuid5(NAMESPACE_URL, f"stockscreener:equity-5m-finalizer:{window.window_id}"),
        )
    result = run_five_minute_finalizer_once(
        compute, EquityWorkerConfig(
            cohort_id=UUID(manifest["cohort_id"]), job_kind="EQUITY_INTERVAL_FINALIZE",
            lease_owner=uuid4(), claim_limit=1,
        ), checked_at=datetime.now(UTC),
    )
    if result.completed != 1 or result.failed or result.blocked or result.retried:
        raise RuntimeError("qualification interval finalization failed")


def main(argv=None):
    import resource

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=("cold", "restart"), required=True)
    args = parser.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    dsn = args.dsn_file.read_text(encoding="utf-8").strip()
    validate_scope(manifest, dsn)
    validate_image_paths(BACKEND_DIR)
    remaining = (datetime.fromisoformat(manifest["workload_stop_at"]) - datetime.now(UTC)).total_seconds()
    if not 0 < remaining <= 600:
        raise ValueError("qualification workload has no valid remaining time")
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_REAL, remaining)
    requests.sessions.Session.request = reject_provider_io
    import websockets.asyncio.client
    websockets.asyncio.client.connect = reject_provider_io
    runtime_catalog = load_advanced_ingress_runtime_catalog(
        BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json",
    )
    profile = runtime_catalog.profile_for_target(1)
    runtime_identity = source_bundle_identity(BACKEND_DIR, RUNTIME_PATHS)
    if manifest.get("runtime_bundle_sha256") != runtime_identity["sha256"]:
        raise RuntimeError("qualification image source differs from the frozen plan")
    started = time.perf_counter()
    cpu_started = time.process_time()
    pool = ThreadedConnectionPool(1, 8, dsn=dsn, options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened", role_prefix=manifest["role_prefix"],
                allow_disposable=True, statement_timeout_ms=30_000,
            ),
        )
        provider.verify()
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute("SELECT current_database(),session_user,current_user")
            database, session_user, role = unit.cursor.fetchone()
            unit.cursor.execute(
                "SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=session_user"
            )
            if any(unit.cursor.fetchone()):
                raise RuntimeError("qualification worker has excessive cluster privileges")
        before = snapshot(provider)
        repository = PostgresOrchestrationRepository(provider)
        cutoff = datetime.fromisoformat(manifest["cutoff"])
        release = datetime.fromisoformat(manifest["release_at"])
        spec = WindowSpec(
            UUID(manifest["cohort_id"]), "MASSIVE_ADVANCED", manifest["window_key"],
            release, cutoff, datetime.fromisoformat(manifest["deadline"]),
        )
        window = repository.ensure_window(UUID(manifest["plan_id"]), spec)
        metrics = {}
        if args.phase == "cold":
            metrics["negative_cases"] = negative_runtime_cases(
                provider,
                manifest,
                window,
            )
            metrics["journal_generations"] = replay(provider, manifest, args.output, profile.settings)
            finalize(provider, manifest, window)
            metrics["cold_window"] = run_window(provider, manifest, window)
        else:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                recovery = discover_advanced_window_recovery_cursor(
                    unit.cursor, cohort_id=spec.cohort_id, cutoff=cutoff, checked_at=datetime.now(UTC),
                )
            if not recovery.complete or recovery.input_receipt is None or recovery.portal_receipt is None:
                raise RuntimeError("restart lost exact input/portal receipts")
            retry = repository.ensure_window(
                UUID(manifest["plan_id"]),
                WindowSpec(spec.cohort_id, spec.provider_tier, manifest["window_key"] + "-retry1",
                           release, cutoff, spec.deadline),
            )
            metrics["receipt_reuse"] = run_window(provider, manifest, retry, recovery=recovery)
        after = snapshot(provider)
        if args.phase == "restart" and after != before:
            raise RuntimeError("restart changed retained bar/evidence/job/publication counts")
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT receipt_kind,input_sha256,universe_sha256,build_id "
                "FROM rt.cycle_receipts WHERE window_id=%s ORDER BY receipt_kind",
                (str(window.window_id),),
            )
            receipts = unit.cursor.fetchall()
        if [row[0] for row in receipts] != ["INPUT_READY", "PORTAL_READY"]:
            raise RuntimeError("qualification lacks exact input and portal receipts")
        metrics.update(
            status="PASS_PROVIDER_FREE", phase=args.phase, database=database,
            session_user=session_user, effective_role=role,
            elapsed_seconds=time.perf_counter() - started,
            process_cpu_seconds=time.process_time() - cpu_started,
            process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            process_final_rss_bytes=psutil.Process().memory_info().rss,
            runtime_profile_sha256=profile.sha256, receipts=receipts,
            runtime_policy_sha256=runtime_catalog.scale_policy.sha256,
            counts_before=before, counts_after=after,
            provider_requests=0, publication_performed=False, live_qualified=False,
            image_secret_paths_absent=True,
            contract_id=CONTRACT_ID,
            schema_sha256=LIVE024_SCHEMA_SHA256,
            registry_sha256=LIVE024_REGISTRY_SHA256,
            runtime_bundle_sha256=runtime_identity["sha256"],
        )
        for name in ("memory.peak", "memory.max"):
            counter = Path("/sys/fs/cgroup") / name
            value = counter.read_text(encoding="ascii").strip() if counter.is_file() else None
            metrics["cgroup_" + name.replace(".", "_")] = int(value) if value and value.isdigit() else None
        metrics["cgroup_metrics_available"] = all(
            metrics["cgroup_" + name] is not None for name in ("memory_peak", "memory_max")
        )
        write_report(args.output / f"{args.phase}.json", metrics)
        print(json.dumps({"status": metrics["status"], "phase": args.phase, "seconds": metrics["elapsed_seconds"]}))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
