#!/usr/bin/env python3
"""Run an isolated, resource-bounded, provider-free Advanced V3 Docker qualification."""
from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from psycopg2 import sql
from psycopg2.extensions import make_dsn


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.advanced_equity_control_plane import (  # noqa: E402
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
)
from realtime.advanced_equity_journal_bridge import AdvancedEquityJournalBridgeConfig  # noqa: E402
from realtime.advanced_ingress_plan import (  # noqa: E402
    AdvancedIngressBudgets,
    AdvancedIngressPlanRequest,
    build_advanced_stream_authorization,
)
from realtime.advanced_ingress_repository import (  # noqa: E402
    activate_advanced_runtime_profile_cursor,
    create_advanced_stream_authorization_cursor,
    prepare_advanced_ingress_plan_cursor,
    register_advanced_runtime_profile_cursor,
    resolve_advanced_ingress_plan_cursor,
)
from realtime.advanced_ingress_runtime_profile import load_advanced_ingress_runtime_catalog  # noqa: E402
from realtime.equity_live024_compatibility import (  # noqa: E402
    CONTRACT_ID,
    ENABLED_STAGES,
    LIVE024_REGISTRY_SHA256,
    LIVE024_SCHEMA_SHA256,
    acl_inventory_cursor,
    contract_metadata,
    live024_acl_contract_inventory,
    live024_schema_contract_inventory,
    load_live_metadata_capture,
    schema_inventory_cursor,
)
from realtime.normalizer import NORMALIZER_VERSION  # noqa: E402
from realtime.orchestration_repository import PostgresOrchestrationRepository  # noqa: E402
from realtime.security_v2 import (  # noqa: E402
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_equity_live024_database_v2,
)
from scripts.prepare_realtime_v2_docker_secrets import _container_dsn, _write_secret  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc
CPUS = 2
MEMORY_BYTES = 2 * 1024**3
MAXIMUM_SECONDS = 600
ENABLED = frozenset(ENABLED_STAGES)
SIBLINGS = (
    "stock-screener-realtime-v2-advanced-api",
    "stock-screener-realtime-v2-advanced-frontend",
    "stock-screener-realtime-v2-advanced-bridge",
    "stock-screener-realtime-v2-advanced-finalizer",
    "stock-screener-realtime-v2-retention",
)


def qualification_plan(run_id, *, fixture_session_date=None):
    suffix = run_id.hex
    metadata = contract_metadata(BACKEND_DIR)
    return {
        "contract_version": "advanced-equity-live024-isolated-docker-v1",
        "contract_id": CONTRACT_ID,
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "migrations": metadata["migrations"],
        "registry_sha256": LIVE024_REGISTRY_SHA256,
        "security_policy_sha256": metadata["security_policy_sha256"],
        "operator_bundle_sha256": metadata["operator"]["sha256"],
        "runtime_bundle_sha256": metadata["runtime"]["sha256"],
        "run_id": str(run_id),
        "image": f"alpha-research-realtime-v2-p8-qualification:{suffix}",
        "container": f"stock-screener-realtime-v2-p8-qualification-{suffix}",
        "database": f"test_realtime_v2_p8q_{suffix}",
        "role_prefix": f"test_rt_p8q_{suffix[:16]}",
        "cpus": CPUS,
        "memory_bytes": MEMORY_BYTES,
        "maximum_workload_seconds_after_build": MAXIMUM_SECONDS,
        "symbols": ["SPY"],
        "universe_target": 1,
        "enabled_stages": sorted(ENABLED),
        "maximum_stream_sessions": 2,
        "maximum_connection_attempts": 2,
        "maximum_decoded_bytes": 10 * 1024 * 1024,
        "maximum_rest_repair_requests": 0,
        "provider_requests_authorized": 0,
        "live_writes_authorized": False,
        "live_profile_activation_authorized": False,
        "publication_authorized": False,
        "fixture_session_date": (
            None
            if fixture_session_date is None
            else fixture_session_date.isoformat()
        ),
    }


def validate_qualification_plan(plan):
    expected = {
        "contract_id": CONTRACT_ID,
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "migrations": contract_metadata(BACKEND_DIR)["migrations"],
        "registry_sha256": LIVE024_REGISTRY_SHA256,
        "symbols": ["SPY"],
        "universe_target": 1,
        "enabled_stages": sorted(ENABLED),
        "maximum_stream_sessions": 2,
        "maximum_connection_attempts": 2,
        "maximum_decoded_bytes": 10 * 1024 * 1024,
        "maximum_rest_repair_requests": 0,
        "provider_requests_authorized": 0,
        "live_writes_authorized": False,
        "live_profile_activation_authorized": False,
        "publication_authorized": False,
        "cpus": 2,
        "memory_bytes": 2 * 1024**3,
        "maximum_workload_seconds_after_build": 600,
    }
    differences = {
        name: {"expected": value, "actual": plan.get(name)}
        for name, value in expected.items()
        if plan.get(name) != value
    }
    if differences:
        raise ValueError(
            "qualification plan exceeds the exact live-024 scope: "
            + ", ".join(sorted(differences))
        )


def durable_counter_snapshot(pool, plan_id):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT connection_attempt_count,stream_session_count,"
                "decoded_byte_count,rest_repair_request_count,"
                "rest_repair_decoded_byte_count,latest_owner_epoch "
                "FROM rt.advanced_ingress_plan_usage WHERE plan_id=%s",
                (str(plan_id),),
            )
            usage = cursor.fetchone()
            cursor.execute(
                "SELECT session_id,connection_attempt,received_frame_count,"
                "received_byte_count,admitted_market_message_count,"
                "admitted_market_byte_count,last_raw_offset,last_event_offset "
                "FROM rt.stream_sessions WHERE advanced_ingress_plan_id=%s "
                "ORDER BY session_id",
                (str(plan_id),),
            )
            sessions = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT journal_generation,raw_committed_offset,"
                "normalized_committed_offset,bridge_committed_offset "
                "FROM rt.advanced_journal_registry WHERE plan_id=%s "
                "ORDER BY journal_generation",
                (str(plan_id),),
            )
            journals = tuple(cursor.fetchall())
    finally:
        pool.putconn(connection)
    return {
        "usage": None if usage is None else tuple(map(str, usage)),
        "sessions": tuple(tuple(map(str, row)) for row in sessions),
        "journals": tuple(tuple(map(str, row)) for row in journals),
    }


def receipt_equality(pool, plan_id, window_key):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT run.window_key,receipt.receipt_kind,"
                "receipt.input_sha256,receipt.universe_sha256,receipt.build_id "
                "FROM rt.window_runs AS run "
                "JOIN rt.cycle_receipts AS receipt USING(window_id) "
                "WHERE run.plan_id=%s AND run.window_key=ANY(%s) "
                "ORDER BY run.window_key,receipt.receipt_kind",
                (str(plan_id), [window_key, window_key + "-retry1"]),
            )
            rows = tuple(cursor.fetchall())
    finally:
        pool.putconn(connection)
    grouped = {}
    for key, *identity in rows:
        grouped.setdefault(str(key), []).append(tuple(map(str, identity)))
    original = tuple(grouped.get(window_key, ()))
    retry = tuple(grouped.get(window_key + "-retry1", ()))
    if (
        tuple(row[0] for row in original) != ("INPUT_READY", "PORTAL_READY")
        or retry != original
    ):
        raise RuntimeError("recreated worker receipts are not field-for-field equal")
    return {"original": original, "retry": retry, "equal": True}


def docker(*arguments, timeout=120):
    result = subprocess.run(
        ["docker", *map(str, arguments)], capture_output=True, text=True,
        timeout=timeout, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Docker {arguments[0]} failed ({result.returncode}): {result.stderr[-1500:]}")
    return result.stdout.strip()


def inspect_container(name):
    result = subprocess.run(
        ["docker", "inspect", name], capture_output=True, text=True, timeout=15, check=False,
    )
    if result.returncode:
        if "no such object" in result.stderr.lower() or "no such container" in result.stderr.lower():
            return None
        raise RuntimeError(f"Cannot determine qualification container state: {result.stderr[-500:]}")
    if not result.stdout.strip():
        raise RuntimeError("Docker returned unknown empty container state")
    return json.loads(result.stdout)[0]


def remove_owned_container(plan):
    info = inspect_container(plan["container"])
    if info is None:
        return
    labels = info["Config"].get("Labels")
    if not isinstance(labels, dict) or labels.get("qualification.run_id") != plan["run_id"]:
        raise RuntimeError("refusing to stop a container not owned by this qualification")
    if info["State"]["Running"]:
        docker("kill", info["Id"], timeout=30)
    docker("rm", info["Id"], timeout=30)
    if inspect_container(plan["container"]) is not None:
        raise RuntimeError("qualification container removal did not persist")


def require_new_image(image):
    result = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True, text=True,
        timeout=15, check=False,
    )
    if result.returncode == 0:
        raise RuntimeError("candidate image tag already exists; refusing replacement")
    if "no such image" not in result.stderr.lower() and "no such object" not in result.stderr.lower():
        raise RuntimeError("cannot establish that the candidate image tag is new")


def sibling_state():
    result = {}
    for name in SIBLINGS:
        info = inspect_container(name)
        result[name] = None if info is None else {
            "id": info["Id"], "started_at": info["State"]["StartedAt"],
            "running": info["State"]["Running"],
            "health": info["State"].get("Health", {}).get("Status"),
        }
    return result


def container_arguments(plan, manifest_path, secret_path, output, phase):
    return [
        "create", "--name", plan["container"],
        "--label", f"qualification.run_id={plan['run_id']}",
        "--cpus", str(CPUS), "--memory", str(MEMORY_BYTES),
        "--memory-swap", str(MEMORY_BYTES), "--pids-limit", "128",
        "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--tmpfs", "/tmp:rw,nosuid,size=268435456",
        "--env", "PYTHONDONTWRITEBYTECODE=1",
        "--env", "OPENBLAS_NUM_THREADS=2", "--env", "OMP_NUM_THREADS=2",
        "--env", "MKL_NUM_THREADS=2", "--env", "NUMEXPR_MAX_THREADS=2",
        "--mount", f"type=bind,source={manifest_path},target=/qualification.json,readonly",
        "--mount", f"type=bind,source={secret_path},target=/run/secrets/qualification_dsn,readonly",
        "--mount", f"type=bind,source={output},target=/qualification-output",
        plan["image"], "python", "-B", "scripts/run_realtime_v2_advanced_qualification_worker.py",
        "--manifest", "/qualification.json", "--dsn-file", "/run/secrets/qualification_dsn",
        "--output", "/qualification-output", "--phase", phase,
    ]


def run_phase(plan, manifest, secret, output, phase, deadline):
    if time.monotonic() >= deadline:
        raise TimeoutError("qualification workload deadline exhausted before container startup")
    container_id = docker(*container_arguments(plan, manifest, secret, output, phase))
    if len(container_id) != 64:
        raise RuntimeError("Docker did not return an exact qualification container identity")
    info = inspect_container(plan["container"])
    if (
        info is None or info["Id"] != container_id
        or info["HostConfig"]["Memory"] != MEMORY_BYTES
        or info["HostConfig"]["NanoCpus"] != CPUS * 1_000_000_000
    ):
        raise RuntimeError("qualification container resource/identity contract differs")
    docker("start", container_id)
    while True:
        info = inspect_container(plan["container"])
        if info is None:
            raise RuntimeError("qualification container vanished before its result was captured")
        if not info["State"]["Running"]:
            break
        if time.monotonic() >= deadline:
            docker("kill", container_id, timeout=30)
            raise TimeoutError("qualification workload exceeded its 600-second deadline")
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    captured = subprocess.run(
        ["docker", "logs", container_id], capture_output=True, text=True,
        timeout=30, check=True,
    )
    logs = captured.stdout + captured.stderr
    (output / f"{phase}.log").write_text(logs, encoding="utf-8")
    if info["State"]["ExitCode"] != 0:
        raise RuntimeError(
            f"qualification {phase} failed: exit={info['State']['ExitCode']}, "
            f"oom={info['State']['OOMKilled']}; {logs[-2000:]}"
        )
    path = output / f"{phase}.json"
    if not path.is_file():
        raise RuntimeError("completed qualification container did not persist its report")
    report = json.loads(path.read_text(encoding="utf-8"))
    if report["status"] != "PASS_PROVIDER_FREE" or report["database"] != plan["database"]:
        raise RuntimeError("qualification worker report does not prove the requested scope")
    for name in (
        "contract_id",
        "schema_sha256",
        "registry_sha256",
        "runtime_bundle_sha256",
    ):
        if report.get(name) != plan[name]:
            raise RuntimeError(f"qualification worker {name} differs from the frozen source")
    report["container_id"] = container_id
    report["image_id"] = info["Image"]
    report["docker_memory_limit_bytes"] = info["HostConfig"]["Memory"]
    report["docker_nano_cpus"] = info["HostConfig"]["NanoCpus"]
    remove_owned_container(plan)
    return report


def seed_workload(pool, *, session_date):
    support_path = BACKEND_DIR / "tests" / "realtime"
    if not support_path.is_dir():
        raise RuntimeError("qualification setup requires the source checkout")
    sys.path.insert(0, str(support_path))
    from tests.realtime import test_v2_pipeline_rehearsal_db as fixture
    from realtime.contracts import canonical_sha256

    _, session_close = fixture.support.session_bounds(session_date)
    world = fixture.Rehearsal(
        pool,
        session_date,
        tickers=("SPY",),
        started_at=session_close + fixture.PROVIDER_DELAY,
    )
    reference = world.deferred.tick_reference(world.opened - timedelta(minutes=15))
    if reference != "COMPLETE":
        raise RuntimeError(f"synthetic reference preparation failed: {reference}")
    cutoff = world.closed - timedelta(minutes=15)
    seeded = fixture._seed_advanced_rehearsal(
        world, cutoff=cutoff, enabled=ENABLED, stage_catalog_version="stages_v3",
    )
    metadata_evidence = exercise_stock_metadata_contract(world)
    generations = []
    payloads = tuple(
        payload for payload in fixture._advanced_payloads(cutoff, tickers=("SPY",))
        if json.loads(payload)[0]["ev"] == "AM"
    )
    for index, own_payloads in enumerate((payloads, tuple(payload.replace(b'"v":1000', b'"v":1001') for payload in payloads[-2:]))):
        session_id = seeded.session_id if index == 0 else uuid4()
        generation = uuid4()
        fields = {
            "session_id": str(session_id), "journal_generation": str(generation),
            "owner_epoch": 7 + index, "payloads": [payload.decode("ascii") for payload in own_payloads],
        }
        frames, byte_count = len(own_payloads), sum(map(len, own_payloads))
        bridge_config = AdvancedEquityJournalBridgeConfig(
            seeded.cohort_id, generation, session_id, 7 + index,
            NORMALIZER_VERSION, (0,),
        )
        if index:
            world.admin(
                "INSERT INTO rt.stream_sessions (session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
                "started_at,subscribed_at,status,advanced_ingress_plan_id) "
                "VALUES (%s,'synthetic-advanced','STOCKS',%s,%s,8,%s,%s,'LIVE',%s)",
                (str(session_id), str(seeded.manifest_id), str(uuid4()),
                 cutoff - timedelta(minutes=2), cutoff - timedelta(seconds=90), str(seeded.ingress_plan_id)),
            )
        world.admin(
            "UPDATE rt.stream_sessions SET status=%s,ended_at=%s,journal_generation=%s,"
            "normalizer_version=%s,authorization_reference='p8-isolated-synthetic',"
            "budget_scope_sha256=%s,connection_attempt=%s,connection_ceiling=2,"
            "decoded_byte_ceiling=1000000,received_frame_count=%s,received_byte_count=%s,"
            "admitted_market_message_count=%s,admitted_market_byte_count=%s,last_raw_offset=%s,"
            "last_event_offset=%s,event_time_watermark=%s,heartbeat_at=%s WHERE session_id=%s",
            ("STOPPED" if index == 0 else "LIVE", cutoff if index == 0 else None,
             str(generation), NORMALIZER_VERSION, "a" * 64, index + 1, frames, byte_count,
             frames, byte_count, frames, frames, cutoff, cutoff, str(session_id)),
        )
        world.admin(
            "INSERT INTO rt.advanced_journal_registry (journal_generation,plan_id,stream_session_id,"
            "cohort_id,manifest_id,session_date,owner_epoch,relative_path,path_sha256,metadata_sha256,"
            "journal_codec,journal_schema_version,normalizer_version,shard_ids,shard_map_sha256,status,"
            "raw_committed_offset,normalized_committed_offset,bridge_committed_offset) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'sqlite-wal',1,%s,ARRAY[0],%s,'WRITING',%s,%s,0)",
            (str(generation), str(seeded.ingress_plan_id), str(session_id), str(seeded.cohort_id),
             str(seeded.manifest_id), session_date, 7 + index, f"p8q/{generation}.sqlite",
             canonical_sha256(str(generation)), canonical_sha256(fields), NORMALIZER_VERSION,
             bridge_config.shard_map_sha256, frames, frames),
        )
        generations.append(fields)
    facts = fixture._advanced_history_facts(
        world, cohort_id=seeded.cohort_id, cutoff=cutoff,
        stream_session_id=seeded.session_id, journal_generation=uuid4(),
    )
    fixture._persist_advanced_history(world, facts)
    return world, seeded, cutoff, generations, len(facts), metadata_evidence


def exercise_stock_metadata_contract(world):
    runtime_catalog = load_advanced_ingress_runtime_catalog(
        BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json",
    )
    profile = runtime_catalog.profile_for_target(1)
    runtime_policy_sha256 = runtime_catalog.scale_policy.sha256
    configuration_sha256 = profile.configuration_sha256(
        runtime_policy_sha256
    )
    checked_at = world.opened - timedelta(hours=1)
    expires_at = world.closed + timedelta(hours=1)
    security_policy_sha256 = contract_metadata(BACKEND_DIR)[
        "security_policy_sha256"
    ]
    control = build_advanced_equity_control_plane_plan(
        session_date=world.session,
        security_ids={"SPY": world.security_ids["SPY"]},
        entitlement_evidence_sha256="e" * 64,
        authorization_reference="live024-disposable-metadata",
        checked_at=checked_at,
        expires_at=expires_at,
        schema_sha256=LIVE024_SCHEMA_SHA256,
        security_policy_sha256=security_policy_sha256,
        configuration_sha256=configuration_sha256,
    )
    budgets = AdvancedIngressBudgets(2, 2, 10 * 1024 * 1024, 0, 0)
    authorization = build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(1,),
        valid_from=world.session,
        valid_through=world.session,
        maximum_budgets=budgets,
        runtime_policy_sha256=runtime_policy_sha256,
        approval_source="disposable-qualification",
        approval_reference="live024-disposable-only",
    )
    with world.cursor() as cursor:
        persist_advanced_equity_control_plane_cursor(cursor, control)
        registered = register_advanced_runtime_profile_cursor(
            cursor,
            profile,
            runtime_policy_sha256=runtime_policy_sha256,
            ladder_position=0,
            previous_universe_target=None,
        )
        activated = activate_advanced_runtime_profile_cursor(
            cursor,
            profile.sha256,
            stability_evidence_sha256="a" * 64,
            capacity_evidence_sha256="b" * 64,
            approval_reference="live024-disposable-only",
            activated_at=checked_at,
        )
        stored_authorization = create_advanced_stream_authorization_cursor(
            cursor,
            authorization,
            promotion_ladder=(1,),
        )
        ingress = resolve_advanced_ingress_plan_cursor(
            cursor,
            AdvancedIngressPlanRequest(
                stored_authorization.authorization_id,
                world.session,
                1,
                budgets,
                preferred_cohort_id=control.cohort_id,
                cohort_selection_reference="live024-disposable-only",
            ),
            runtime_catalog=runtime_catalog,
        )
        prepared = prepare_advanced_ingress_plan_cursor(
            cursor,
            ingress,
            checked_at=world.opened,
        )
    if (
        registered.status != "CANDIDATE"
        or activated.status != "ACTIVE"
        or prepared.plan_id != ingress.plan_id
    ):
        raise RuntimeError("real stock metadata operations did not persist exactly")
    return {
        "profile_id": str(control.profile_id),
        "cohort_id": str(control.cohort_id),
        "authorization_id": str(stored_authorization.authorization_id),
        "ingress_plan_id": str(prepared.plan_id),
        "schema_sha256": control.schema_sha256,
        "runtime_profile_sha256": profile.sha256,
        "runtime_policy_sha256": runtime_policy_sha256,
        "status": "PASS_REAL_STOCK_METADATA",
    }


def workload_manifest(plan, world, seeded, cutoff, generations, history_rows, stop_at):
    release = datetime.now(UTC) - timedelta(seconds=1)
    return {
        **plan, "cohort_id": str(seeded.cohort_id), "plan_id": str(seeded.plan_id),
        "ingress_plan_id": str(seeded.ingress_plan_id),
        "manifest_id": str(seeded.manifest_id),
        "session_date": world.session.isoformat(),
        "instrument_id": world.instrument_ids["SPY"], "cutoff": cutoff.isoformat(),
        "release_at": release.isoformat(), "deadline": (release + timedelta(seconds=300)).isoformat(),
        "workload_stop_at": stop_at.isoformat(),
        "window_key": f"advanced-{cutoff:%Y%m%dT%H%M%SZ}",
        "generations": generations, "history_rows": history_rows,
        "fixture_control_plane": "SYNTHETIC_DISPOSABLE_ONLY_NOT_LIVE_ACTIVATION_EVIDENCE",
    }


def create_worker_login(settings, database, roles):
    from v2_database import _connect

    if not database.startswith("test_realtime_v2_p8q_") or not roles.prefix.startswith("test_rt_p8q_"):
        raise ValueError("qualification login requires an owned disposable database and roles")
    login = roles.prefix + "_worker"
    password = secrets.token_urlsafe(32)
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %s VALID UNTIL %s")
                .format(sql.Identifier(login)),
                (password, (datetime.now(UTC) + timedelta(minutes=20)).isoformat()),
            )
            cursor.execute(
                sql.SQL("GRANT {} TO {}").format(sql.Identifier(roles.name(APP_ROLE)), sql.Identifier(login))
            )
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(login))
            )
        connection.commit()
    return login, make_dsn(
        dbname=database, user=login, password=password,
        host=settings["DB_HOST"], port=settings["DB_PORT"],
    )


def execute(plan, output, *, retry_report=None, reuse_image_report=None):
    validate_qualification_plan(plan)
    if not plan.get("fixture_session_date"):
        raise ValueError("qualification requires an explicit fixture session date")
    if (
        not plan.get("live_metadata_report_sha256")
        or not plan.get("live_schema_inventory_sha256")
        or not plan.get("live_acl_inventory_sha256")
    ):
        raise ValueError("qualification lacks a bound compatible live-024 capture")
    stop_at = None
    previous = None
    previous_path = retry_report or reuse_image_report
    if previous_path is not None:
        previous = json.loads(previous_path.read_text(encoding="utf-8"))
        if (
            previous["status"] != "FAILED"
            or previous["contract_version"] != plan["contract_version"]
            or previous["cpus"] != CPUS or previous["memory_bytes"] != MEMORY_BYTES
            or previous["provider_requests"] != 0
            or (retry_report is not None and "retry_of" in previous)
        ):
            raise ValueError("only one same-scope failed qualification may be recovered")
        if retry_report is not None:
            prior_input = json.loads((previous_path.parent / "input.json").read_text(encoding="utf-8"))
            stop_at = datetime.fromisoformat(prior_input["workload_stop_at"])
            if stop_at <= datetime.now(UTC):
                raise TimeoutError("original qualification deadline expired; new authorization is required")
        elif previous["cleanup_verified"] is not True:
            raise RuntimeError("fresh-window image reuse requires verified prior cleanup")
        if inspect_container(previous["container"]) is not None or (previous_path.parent / "qualification_dsn").exists():
            raise RuntimeError("prior qualification resource/secret cleanup is not established")
        if docker("image", "inspect", previous["image"], "--format", "{{.Id}}") != previous["image_id"]:
            raise RuntimeError("prior candidate image identity changed")
        plan = {**plan, "image": previous["image"]}
        if retry_report is not None:
            plan.update(retry_of=str(retry_report), original_workload_stop_at=stop_at.isoformat())
        else:
            plan["reused_image_report"] = str(reuse_image_report)
    output.mkdir(parents=True, exist_ok=False)
    write_report(output / "manifest.json", {**plan, "status": "PREPARING"})
    siblings_before = sibling_state()
    if previous is None:
        require_new_image(plan["image"])
        build_log = output / "build.log"
        with build_log.open("w", encoding="utf-8") as log:
            subprocess.run(
                ["docker", "build", "-f", str(BACKEND_DIR / "Dockerfile"),
                 "-t", plan["image"], str(BACKEND_DIR)],
                stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1200,
            )
    image_id = docker("image", "inspect", plan["image"], "--format", "{{.Id}}")
    if not image_id.startswith("sha256:"):
        raise RuntimeError("candidate image build produced unknown image identity")
    sys.path.insert(0, str(BACKEND_DIR / "tests" / "realtime"))
    from v2_database import _connect, _settings, disposable_live024_database

    settings = _settings()
    if previous is not None:
        previous_roles = build_role_names_v2(previous["role_prefix"])
        with closing(_connect(settings, "postgres")) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)", (previous["database"],))
                if cursor.fetchone()[0]:
                    raise RuntimeError("prior qualification database still exists")
                cursor.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                    ([*previous_roles.all_names, previous["role_prefix"] + "_worker"],),
                )
                if cursor.fetchall():
                    raise RuntimeError("prior qualification roles still exist")
    roles = build_role_names_v2(plan["role_prefix"])
    login = plan["role_prefix"] + "_worker"
    roles_owned = False
    login_owned = False
    secret = output / "qualification_dsn"
    input_path = output / "input.json"
    data = output / "workload"
    data.mkdir()
    results = []
    counter_snapshots = []
    database_contract = None
    metadata_evidence = None
    receipt_proof = None
    started = time.monotonic()
    remaining = MAXIMUM_SECONDS if stop_at is None else (stop_at - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise TimeoutError("original qualification deadline expired before setup")
    deadline = started + remaining
    completed = False
    try:
        with closing(_connect(settings, "postgres")) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)", ([*roles.all_names, login],))
                if cursor.fetchall():
                    raise RuntimeError("qualification roles already exist; refusing ownership")
        with disposable_live024_database(plan["database"]) as pool:
            (
                world,
                seeded,
                cutoff,
                generations,
                history_rows,
                metadata_evidence,
            ) = seed_workload(
                pool,
                session_date=date.fromisoformat(plan["fixture_session_date"]),
            )
            connection = pool.getconn()
            try:
                roles_owned = True
                _created, audit = harden_equity_live024_database_v2(
                    connection,
                    roles,
                )
                if not all(state.startswith("PASS") for _, state in audit):
                    raise RuntimeError("disposable qualification hardening failed")
                with connection.cursor() as cursor:
                    raw_schema_inventory = schema_inventory_cursor(cursor)
                with connection.cursor() as cursor:
                    raw_acl_inventory = acl_inventory_cursor(
                        cursor,
                        role_names={
                            suffix: roles.name(suffix)
                            for suffix in ("admin", "app", "api")
                        },
                    )
                schema_inventory = live024_schema_contract_inventory(
                    raw_schema_inventory
                )
                acl_inventory = live024_acl_contract_inventory(
                    raw_acl_inventory
                )
                if (
                    schema_inventory["sha256"]
                    != plan["live_schema_inventory_sha256"]
                    or acl_inventory["sha256"]
                    != plan["live_acl_inventory_sha256"]
                ):
                    raise RuntimeError(
                        "disposable schema or ACL differs from the bound live-024 capture"
                    )
                database_contract = {
                    "schema_inventory_sha256": schema_inventory["sha256"],
                    "acl_inventory_sha256": acl_inventory["sha256"],
                    "raw_schema_inventory_sha256": raw_schema_inventory["sha256"],
                    "raw_acl_inventory_sha256": raw_acl_inventory["sha256"],
                    "live_inventory_match": True,
                    "security_audit": audit,
                }
            finally:
                pool.putconn(connection)
            login, local_dsn = create_worker_login(settings, plan["database"], roles)
            login_owned = True
            dsn = _container_dsn(local_dsn, "host.docker.internal")
            _write_secret(secret, dsn)
            workload = workload_manifest(
                plan, world, seeded, cutoff, generations, history_rows,
                datetime.now(UTC) + timedelta(seconds=max(0, deadline - time.monotonic())),
            )
            workload["deadline"] = min(
                datetime.fromisoformat(workload["deadline"]),
                datetime.fromisoformat(workload["workload_stop_at"]),
            ).isoformat()
            write_report(input_path, workload)
            provider = V2ConnectionProvider(
                {APP_ROLE: pool},
                V2AccessConfig(
                    security_mode="hardened", role_prefix=roles.prefix, allow_disposable=True,
                ),
            )
            provider.verify()
            PostgresOrchestrationRepository(provider).upsert_readiness(
                seeded.cohort_id, "REFERENCE", "stages_v3", "READY", (),
                "synthetic-existing-seed", datetime.now(UTC),
            )
            for phase in ("cold", "restart"):
                results.append(run_phase(plan, input_path, secret, data, phase, deadline))
                counter_snapshots.append(
                    durable_counter_snapshot(pool, seeded.ingress_plan_id)
                )
            if results[0]["container_id"] == results[1]["container_id"]:
                raise RuntimeError("qualification did not recreate the worker process")
            if counter_snapshots[1] != counter_snapshots[0]:
                raise RuntimeError("recreated worker changed durable ingress counters")
            receipt_proof = receipt_equality(
                pool,
                seeded.plan_id,
                workload["window_key"],
            )
            if time.monotonic() > deadline:
                raise TimeoutError("qualification workload exceeded approved duration")
            completed = True
    finally:
        container_removed = False
        try:
            remove_owned_container(plan)
            container_removed = True
        finally:
            if secret.exists():
                secret.unlink()
            with closing(_connect(settings, "postgres")) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    if login_owned:
                        cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(login)))
                if roles_owned:
                    drop_disposable_roles_v2(connection, roles)
                with connection.cursor() as cursor:
                    cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)", (plan["database"],))
                    database_left = bool(cursor.fetchone()[0])
                    cursor.execute("SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)", ([*roles.all_names, login],))
                    roles_left = [row[0] for row in cursor.fetchall()]
            cleanup_verified = (
                not database_left and not roles_left and not secret.exists() and container_removed
            )
            siblings_after = sibling_state()
            sibling_identities_unchanged = all(
                (None if before is None else (before["id"], before["started_at"]))
                == (None if siblings_after[name] is None else (
                    siblings_after[name]["id"], siblings_after[name]["started_at"],
                ))
                for name, before in siblings_before.items()
            )
            write_report(output / "result.json", {
                **plan, "image_id": image_id,
                "status": "PASS_EQUITY_LIVE024_COMPATIBILITY"
                if completed and cleanup_verified and sibling_identities_unchanged else "FAILED",
                "elapsed_workload_seconds": time.monotonic() - started,
                "phases": results, "live_qualified": False, "production_ready": False,
                "runtime_profile_sha256": (
                    results[0]["runtime_profile_sha256"] if results else None
                ),
                "runtime_policy_sha256": (
                    results[0]["runtime_policy_sha256"] if results else None
                ),
                "live_profile_activated": False, "provider_requests": 0,
                "live_database_writes": 0, "publication_performed": False,
                "cleanup_verified": cleanup_verified,
                **(database_contract or {}),
                "metadata_evidence": metadata_evidence,
                "counter_snapshots": counter_snapshots,
                "counter_preservation_verified": (
                    len(counter_snapshots) == 2
                    and counter_snapshots[0] == counter_snapshots[1]
                ),
                "receipt_equality": receipt_proof,
                "database_left": database_left, "roles_left": roles_left,
                "siblings_before": siblings_before, "siblings_after": siblings_after,
                "sibling_identities_unchanged": sibling_identities_unchanged,
                "control_plane": "SYNTHETIC_FIXTURE_IN_DISPOSABLE_DATABASE_ONLY",
                "not_qualified": [
                    "Live provider sockets/entitlement, cumulative live budgets and real recovery",
                    "Sustained rate/headroom, stable market sessions, target 9 or larger",
                    "Controller/scheduler process faults and automatic next-session lifecycle",
                    "Advanced alerts/publication, real profile activation or platform promotion",
                ],
            })
            if not cleanup_verified and container_removed:
                raise RuntimeError("qualification cleanup did not verify absent owned resources")
            if not sibling_identities_unchanged:
                raise RuntimeError("existing sibling identities changed; isolation is not certified")
    report = json.loads((output / "result.json").read_text(encoding="utf-8"))
    cold_seconds = report["phases"][0]["cold_window"]["seconds"]
    report["measured_cold_window_deadline_ratio"] = 300 / cold_seconds
    report["ratio_scope"] = "One synthetic window / 300-second production completion budget; not sustained-rate headroom"
    write_report(output / "result.json", report)
    print(json.dumps({
        "status": report["status"], "report": str(output / "result.json"),
        "cold_window_seconds": cold_seconds, "workload_seconds": report["elapsed_workload_seconds"],
        "live_qualified": False,
    }))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--run-id", type=UUID, default=None)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--live-metadata-report", type=Path)
    parser.add_argument("--live-metadata-report-sha256")
    parser.add_argument("--fixture-session-date", type=date.fromisoformat)
    reuse = parser.add_mutually_exclusive_group()
    reuse.add_argument("--retry-report", type=Path)
    reuse.add_argument("--reuse-image-report", type=Path)
    args = parser.parse_args(argv)
    plan = qualification_plan(
        args.run_id or uuid4(),
        fixture_session_date=args.fixture_session_date,
    )
    if (args.live_metadata_report is None) != (
        args.live_metadata_report_sha256 is None
    ):
        parser.error(
            "--live-metadata-report and its SHA256 must be supplied together"
        )
    if args.live_metadata_report is not None:
        capture, capture_sha256, inventories = load_live_metadata_capture(
            args.live_metadata_report.resolve(),
            args.live_metadata_report_sha256,
        )
        plan.update({
            "live_metadata_report": str(args.live_metadata_report.resolve()),
            "live_metadata_report_sha256": capture_sha256,
            "live_schema_inventory_sha256": inventories[
                "schema_inventory_sha256"
            ],
            "live_acl_inventory_sha256": inventories[
                "acl_inventory_sha256"
            ],
            "live_capture_at": capture["captured_at"],
        })
    elif args.execute:
        parser.error("--execute requires a bound compatible live metadata report")
    if args.execute and args.fixture_session_date is None:
        parser.error("--execute requires --fixture-session-date")
    if args.output_directory.exists():
        parser.error("--output-directory must be new")
    if not args.execute:
        previous_path = args.retry_report or args.reuse_image_report
        if previous_path is not None:
            previous = json.loads(previous_path.read_text(encoding="utf-8"))
            plan["image"] = previous["image"]
            plan["reused_image_report"] = str(previous_path)
        print(json.dumps({**plan, "status": "PLAN"}, indent=2))
        return 0
    return execute(
        plan, args.output_directory.resolve(), retry_report=args.retry_report,
        reuse_image_report=args.reuse_image_report,
    )


if __name__ == "__main__":
    raise SystemExit(main())
