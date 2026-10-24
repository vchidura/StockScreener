#!/usr/bin/env python3
"""Qualify the real Advanced Options V3 production roles in owned Docker."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from psycopg2 import sql
from psycopg2.extensions import make_dsn


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
TEST_SUPPORT = BACKEND_DIR / "tests" / "realtime"
for path in (BACKEND_DIR, TEST_SUPPORT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.advanced_options_control_plane import (  # noqa: E402
    CANONICAL_FLOW_OWNER,
    CANONICAL_INGRESS_OWNER,
    CANONICAL_MARKET_STATE_OWNER,
)
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.security_v2 import (  # noqa: E402
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5  # noqa: E402
from realtime.advanced_options_p8_docker import P8_SERVICE_NAMES  # noqa: E402
from realtime.advanced_options_p8_runtime import (  # noqa: E402
    P8_EFFECT_DENIALS,
    P8_EXECUTION_BUNDLE_SCHEMA,
)
from realtime.advanced_options_service import (  # noqa: E402
    AdvancedOptionsProcessKind,
    OPTIONS_V3_STAGE_ORDER,
)
from realtime.advanced_options_v3_deployment import (  # noqa: E402
    ADVANCED_OPTIONS_V3_PROFILE_SHA256,
    ADVANCED_OPTIONS_V3_QUALIFICATION_STATE,
    advanced_options_v3_profile_payload,
)
from scripts.prepare_realtime_v2_docker_secrets import (  # noqa: E402
    _container_dsn,
    _write_secret,
)
from scripts.validate_realtime_v2_schema import _connect, _settings  # noqa: E402
from v2_database import disposable_v2_database  # noqa: E402


UTC = timezone.utc
SERVICES = {
    "INGRESS": "realtime-v2-options-p8-ingress",
    "MARKET_STATE": "realtime-v2-options-p8-market-state",
    "FLOW": "realtime-v2-options-p8-flow",
    "WINDOW": "realtime-v2-options-p8-window",
}
QUALIFICATION_SCHEMA = (
    "advanced-options-v3-production-role-qualification-v1"
)
ZERO_EFFECTS = {
    "provider_rest": 0,
    "provider_websocket": 0,
    "production_database_writes": 0,
    "production_service_actions": 0,
    "publication": 0,
    "stored_read_changes": 0,
    "retention_actions": 0,
    "delivery": 0,
    "trading": 0,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--qualification-semantic-sha256", required=True)
    parser.add_argument("--qualification-file-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.output_directory = args.output_directory.resolve()
    if (
        not args.image_id.startswith("sha256:")
        or len(args.image_id) != 71
        or any(
            character not in "0123456789abcdef"
            for character in args.image_id.removeprefix("sha256:")
        )
    ):
        parser.error("--image-id must be an immutable local sha256 ID")
    for name in (
        "qualification_semantic_sha256",
        "qualification_file_sha256",
    ):
        value = getattr(args, name)
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            parser.error(f"--{name.replace('_', '-')} must be a SHA-256")
    if args.output_directory.exists():
        parser.error("--output-directory must be new")
    if not args.execute:
        parser.error("Docker qualification requires explicit --execute")
    return args


def _run(
    argv,
    *,
    environment=None,
    timeout=120,
    check=True,
):
    completed = subprocess.run(
        [str(value) for value in argv],
        cwd=REPOSITORY_ROOT,
        env=None if environment is None else environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if check and completed.returncode != 0:
        stdout = completed.stdout.decode("utf-8", errors="replace")
        stderr = completed.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(
            f"command failed ({completed.returncode}): "
            + (stdout + "\n" + stderr)[-4000:]
        )
    return completed


def _docker(*args, environment=None, timeout=120) -> str:
    result = _run(
        ("docker", *args),
        environment=environment,
        timeout=timeout,
    )
    return result.stdout.decode("utf-8").strip()


def _write_json(path: Path, value) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)


def _seed(pool, *, session_date: date, suffix: str):
    now = datetime.now(UTC)
    profile_id = uuid4()
    manifest_id = uuid4()
    cohort_id = uuid4()
    plan_id = uuid4()
    window_ids = tuple(uuid4() for _ in range(3))
    journal_generation = uuid4()
    digest = lambda name: hashlib.sha256(
        f"{suffix}:{name}".encode("utf-8")
    ).hexdigest()
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO rt.capability_profiles ("
                    "profile_id,config_sha256,schema_sha256,account_alias,"
                    "capabilities_sha256,universe_sha256,checked_at,"
                    "expires_at,status,profile_sha256,provider_tier,"
                    "security_policy_sha256,preflight_mode,"
                    "provider_requests,action_authorized) VALUES ("
                    "%s,%s,%s,'v3-production-qualification',%s,%s,%s,%s,'PASS',"
                    "%s,'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
                    (
                        str(profile_id),
                        digest("config"),
                        digest("schema"),
                        digest("capabilities"),
                        digest("universe"),
                        now,
                        now + timedelta(hours=1),
                        digest("profile"),
                        digest("security"),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.subscription_manifests ("
                    "manifest_id,manifest_sha256,universe_sha256,"
                    "session_date,effective_at,profile_id,manifest_kind,"
                    "stock_count,option_underlying_count,member_count,"
                    "allocation_policy_sha256) VALUES ("
                    "%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',9,2,9,%s)",
                    (
                        str(manifest_id),
                        digest("manifest"),
                        digest("universe"),
                        session_date,
                        now,
                        str(profile_id),
                        digest("allocation"),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.cohorts ("
                    "cohort_id,cohort_sha256,manifest_id,"
                    "configuration_sha256,model_bundle_sha256,"
                    "valuation_sha256,effective_at,mode) VALUES ("
                    "%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
                    (
                        str(cohort_id),
                        digest("cohort"),
                        str(manifest_id),
                        digest("configuration"),
                        digest("model"),
                        digest("valuation"),
                        now,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.service_plans ("
                    "plan_id,cohort_id,session_date,"
                    "stage_catalog_version,enabled_stages,config_sha256,"
                    "deadline,scope_reference) VALUES ("
                    "%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        str(plan_id),
                        str(cohort_id),
                        session_date,
                        STAGE_CATALOG_VERSION_V5,
                        list(OPTIONS_V3_STAGE_ORDER),
                        digest("service"),
                        now + timedelta(minutes=10),
                        f"OWNED_V3_DOCKER_REHEARSAL:{suffix}",
                    ),
                )
                for index, window_id in enumerate(
                    window_ids,
                    start=1,
                ):
                    cutoff = now + timedelta(seconds=index)
                    cursor.execute(
                        "INSERT INTO rt.window_runs ("
                        "window_id,plan_id,cohort_id,provider_tier,"
                        "window_key,release_at,cutoff,deadline,state) "
                        "VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,"
                        "'PENDING')",
                        (
                            str(window_id),
                            str(plan_id),
                            str(cohort_id),
                            f"v3-production-qualification-{suffix}-{index}",
                            cutoff,
                            cutoff,
                            now + timedelta(minutes=10),
                        ),
                    )
    finally:
        pool.putconn(connection)
    return {
        "cohort_id": str(cohort_id),
        "plan_id": str(plan_id),
        "window_id": str(window_ids[0]),
        "window_ids": [str(value) for value in window_ids],
        "journal_generation": str(journal_generation),
        "seeded_at_utc": now.isoformat(),
    }


def _worker_login(settings, database: str, roles):
    login = roles.prefix + "_worker"
    password = secrets.token_urlsafe(32)
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE INHERIT PASSWORD %s VALID UNTIL %s"
                ).format(sql.Identifier(login)),
                (
                    password,
                    (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
                ),
            )
            cursor.execute(
                sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(roles.name(APP_ROLE)),
                    sql.Identifier(login),
                )
            )
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database),
                    sql.Identifier(login),
                )
            )
        connection.commit()
    return login, make_dsn(
        dbname=database,
        user=login,
        password=password,
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        options=f"-c role={roles.name(APP_ROLE)} -c timezone=UTC",
    )


def _drop_login(settings, login: str) -> None:
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(
                    sql.Identifier(login)
                )
            )
        connection.commit()


def _override() -> dict[str, object]:
    return {
        "services": {
            service: {
                "environment": {
                    "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
                },
                "healthcheck": {
                    "interval": "2s",
                    "timeout": "5s",
                    "retries": 45,
                    "start_period": "2s",
                },
            }
            for service in SERVICES.values()
        },
    }


def _expected_command(role: str) -> list[str]:
    component = {
        "INGRESS": "options-p8-ingress",
        "MARKET_STATE": "options-p8-market-state",
        "FLOW": "options-p8-flow",
        "WINDOW": "options-p8-window",
    }[role]
    command = [
        "python",
        "-B",
        "scripts/run_realtime_v2_container.py",
        "--component",
        component,
        "--app-dsn-file",
        "/run/secrets/realtime_v2_app_dsn",
    ]
    if role == "INGRESS":
        command.extend([
            "--provider-key-file",
            "/run/secrets/realtime_v2_provider_key",
        ])
    command.extend([
        "--",
        "python",
        "-B",
        "scripts/run_advanced_options_v3_production.py",
        "--docker-service-kind",
        role,
        "--package",
        "/run/advanced-options-p8/package.json",
        "--execution-bundle",
        "/run/advanced-options-p8/execution-bundle.json",
    ])
    return command


def _compose_args(
    override_path: Path,
    project: str,
    *arguments: str,
):
    return (
        "docker",
        "compose",
        "--file",
        str(REPOSITORY_ROOT / "docker-compose.realtime-v2.yml"),
        "--file",
        str(override_path),
        "--project-name",
        project,
        "--profile",
        "realtime-v3-options",
        *arguments,
    )


def _project_containers(project: str):
    value = _docker(
        "ps",
        "-a",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--format",
        "{{.ID}}",
    )
    return tuple(row for row in value.splitlines() if row.strip())


def _service_container_id(
    environment,
    override_path,
    project,
    service,
) -> str:
    container_id = _run(
        _compose_args(override_path, project, "ps", "-q", service),
        environment=environment,
    ).stdout.decode("utf-8").strip()
    if len(container_id) != 64:
        raise RuntimeError(f"{service} container identity is ambiguous")
    return container_id


def _capture_service_states(
    environment,
    override_path,
    project,
    services,
    path,
) -> None:
    rows = []
    for service in services:
        try:
            container_id = _service_container_id(
                environment,
                override_path,
                project,
                service,
            )
            inspected = json.loads(_docker("inspect", container_id))
            state = (
                inspected[0]["State"]
                if isinstance(inspected, list) and len(inspected) == 1
                else {"error": "ambiguous inspect result"}
            )
            rows.append({
                "service": service,
                "container_id": container_id,
                "state": state,
            })
        except (RuntimeError, KeyError, TypeError, ValueError) as error:
            rows.append({
                "service": service,
                "error": str(error),
            })
    _write_json(path, rows)


def _inspect_service(environment, override_path, project, service, role):
    container_id = _service_container_id(
        environment,
        override_path,
        project,
        service,
    )
    value = json.loads(_docker("inspect", container_id))
    if not isinstance(value, list) or len(value) != 1:
        raise RuntimeError(f"{service} inspect result is ambiguous")
    row = value[0]
    expected = _expected_command(role)
    if (
        row["Image"] != environment["REALTIME_V2_BACKEND_IMAGE"]
        or row["Config"]["Cmd"] != expected
        or row["HostConfig"]["RestartPolicy"]["Name"] != "no"
        or row["State"]["Health"]["Status"] != "healthy"
        or row["Config"]["Labels"]["com.docker.compose.project"] != project
        or row["Config"]["Labels"]["com.docker.compose.service"] != service
    ):
        raise RuntimeError(f"{service} runtime identity changed")
    return {
        "role": role,
        "service": service,
        "container_id": container_id,
        "image_id": row["Image"],
        "command": row["Config"]["Cmd"],
        "health": row["State"]["Health"]["Status"],
        "restart": row["HostConfig"]["RestartPolicy"],
        "stop_timeout_seconds": row["Config"].get("StopTimeout", 60),
        "secrets": sorted(
            mount["Source"].split("/")[-1]
            for mount in row["Mounts"]
            if mount["Destination"].startswith("/run/secrets/")
        ),
    }


def _role_status(container_id: str, role: str) -> dict[str, object]:
    filename = role.casefold().replace("_", "-") + ".json"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        value = json.loads(_docker(
            "exec",
            container_id,
            "cat",
            f"/var/lib/realtime-v2-reports/p8/{filename}",
        ))
        evidence = value.get("role_evidence")
        if (
            value.get("state") == "ROLE_COMPLETE"
            and value.get("kind") == role
            and isinstance(evidence, dict)
            and evidence.get("status") == "COMPLETE"
            and evidence.get("kind") == role
            and evidence.get("effects") == P8_EFFECT_DENIALS
        ):
            return value
        time.sleep(0.1)
    raise RuntimeError(f"{role} production role evidence is incomplete")


def _qualification_artifacts(seed, args):
    now = datetime.now(UTC)
    nonce = uuid4()
    cohort_id = seed["cohort_id"]
    session_date = args.session_date.isoformat()
    prefix = f"advanced-options:{cohort_id}:{session_date}"
    owners = [
        {
            "kind": AdvancedOptionsProcessKind.INGRESS.value,
            "owner_name": CANONICAL_INGRESS_OWNER,
            "lease_resource_key": "massive:options:websocket",
        },
        {
            "kind": AdvancedOptionsProcessKind.MARKET_STATE.value,
            "owner_name": CANONICAL_MARKET_STATE_OWNER,
            "lease_resource_key": f"{prefix}:market-state",
        },
        {
            "kind": AdvancedOptionsProcessKind.FLOW.value,
            "owner_name": CANONICAL_FLOW_OWNER,
            "lease_resource_key": f"{prefix}:flow",
        },
        {
            "kind": AdvancedOptionsProcessKind.WINDOW.value,
            "owner_name": "advanced-options-window-v1",
            "lease_resource_key": f"{prefix}:window",
        },
    ]
    qualification = {
        "schema": QUALIFICATION_SCHEMA,
        "cohort_id": cohort_id,
        "window_ids": list(seed["window_ids"]),
        "journal_generation": seed["journal_generation"],
        "contracts": [
            "O:QQQ261023C00350000",
            "O:QQQ261023P00350000",
            "O:SPY261023C00600000",
            "O:SPY261023P00600000",
        ],
        "channels": ["T", "Q"],
        "flow_release_at_utc": (
            now + timedelta(seconds=60)
        ).isoformat(),
        "qualification_semantic_sha256": (
            args.qualification_semantic_sha256
        ),
        "qualification_file_sha256": (
            args.qualification_file_sha256
        ),
        "acceptance": {
            "window_count": 3,
            "window_2": "DEFERRED_THEN_COMPLETE",
            "window_3": {
                "O1": "COMPLETE_SELECTED",
                "O3": "BLOCKED",
            },
            "restart": "REHYDRATED",
            "publication": {
                "pointer_advances": 1,
                "idempotent": True,
                "rollback": True,
                "duplicate_alerts": 0,
                "duplicate_publications": 0,
            },
        },
    }
    orchestration = {
        "plan_id": seed["plan_id"],
        "catalog_version": STAGE_CATALOG_VERSION_V5,
        "stage_order": list(OPTIONS_V3_STAGE_ORDER),
    }
    package = {
        "schema": "advanced-options-v3-production-qualification-package-v1",
        "package_nonce": str(nonce),
        "window": {
            "session_date": session_date,
            "start_utc": (now - timedelta(minutes=1)).isoformat(),
            "stop_utc": (now + timedelta(minutes=10)).isoformat(),
        },
        "cohort": {
            "cohort_id": cohort_id,
            "underlyings": ["QQQ", "SPY"],
        },
        "orchestration": orchestration,
        "owners": {"processes": owners},
        "equity_input_binding": {
            "symbols": list(
                advanced_options_v3_profile_payload()["equity"]["symbols"]
            ),
        },
        "v3_deployment": {
            "profile": advanced_options_v3_profile_payload(),
            "profile_sha256": ADVANCED_OPTIONS_V3_PROFILE_SHA256,
            "state": ADVANCED_OPTIONS_V3_QUALIFICATION_STATE,
            "window_role_authorized": True,
            "qualification_verdict": "PASS",
        },
        "v3_authorities": None,
        "provider_free_qualification": qualification,
        "effects": dict(ZERO_EFFECTS),
    }
    package["package_sha256"] = canonical_sha256(package)
    owner_ids = {
        row["kind"]: uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-options-p8-owner:"
                f"{nonce}:{row['kind']}"
            ),
        )
        for row in owners
    }
    bundle = {
        "schema": P8_EXECUTION_BUNDLE_SCHEMA,
        "package_sha256": package["package_sha256"],
        "package_nonce": str(nonce),
        "package_path": "/run/advanced-options-p8/package.json",
        "window": dict(package["window"]),
        "cohort_id": cohort_id,
        "plan_id": seed["plan_id"],
        "window_id": seed["window_id"],
        "orchestration": orchestration,
        "v3_authorities": None,
        "written_at_utc": now.isoformat(),
        "services": [
            {
                **row,
                "owner_id": str(owner_ids[row["kind"]]),
                "service_name": P8_SERVICE_NAMES[
                    AdvancedOptionsProcessKind(row["kind"])
                ],
            }
            for row in owners
        ],
        "enrollment": {},
        "history_binding": {},
        "equity_input_binding": dict(
            package["equity_input_binding"]
        ),
        "underlying_price_binding": {},
        "production_binding": {
            "control": {"option_underlyings": ["QQQ", "SPY"]},
        },
        "live_observation_sha256": canonical_sha256({
            "transport": "LOOPBACK_WS",
            "provider_effects": 0,
        }),
        "effect_authorizations": dict(P8_EFFECT_DENIALS),
        "provider_free_qualification": qualification,
    }
    bundle["bundle_sha256"] = canonical_sha256(bundle)
    return package, bundle


def _seed_service_leases(pool, bundle) -> None:
    lease_until = datetime.now(UTC) + timedelta(minutes=15)
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                for service in bundle["services"]:
                    cursor.execute(
                        "INSERT INTO rt.worker_leases ("
                        "resource_key,owner_id,owner_epoch,lease_until) "
                        "VALUES (%s,%s,1,%s)",
                        (
                            service["lease_resource_key"],
                            service["owner_id"],
                            lease_until,
                        ),
                    )
    finally:
        pool.putconn(connection)


def _durable_evidence(pool, seed):
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT target_window_id::text,member,status,"
                    "ending_offset,evidence_sha256,"
                    "evidence_document FROM "
                    "rt.advanced_options_resident_completions "
                    "WHERE target_window_id=ANY(%s::uuid[]) "
                    "ORDER BY target_window_id,member",
                    (seed["window_ids"],),
                )
                residents = cursor.fetchall()
                cursor.execute(
                    "SELECT service.stage_catalog_version,"
                    "service.enabled_stages,run.state,run.window_id::text "
                    "FROM rt.window_runs AS run "
                    "JOIN rt.service_plans AS service USING(plan_id) "
                    "WHERE run.window_id=ANY(%s::uuid[]) "
                    "ORDER BY run.cutoff",
                    (seed["window_ids"],),
                )
                windows = cursor.fetchall()
        if (
            len(residents) != 9
            or {
                (row[0], row[1])
                for row in residents
            }
            != {
                (window_id, member)
                for window_id in seed["window_ids"]
                for member in {"INGRESS", "MARKET_STATE", "FLOW"}
            }
            or any(row[2] != "COMPLETE" for row in residents)
            or len(windows) != 3
            or any(
                row[:3]
                != (
                    STAGE_CATALOG_VERSION_V5,
                    list(OPTIONS_V3_STAGE_ORDER),
                    "COMPLETE",
                )
                for row in windows
            )
        ):
            raise RuntimeError(
                "durable V5 production qualification barrier is incomplete"
            )
        return {
            "residents": [
                {
                    "window_id": row[0],
                    "member": row[1],
                    "status": row[2],
                    "ending_offset": int(row[3]),
                    "evidence_sha256": row[4],
                    "evidence_document": row[5],
                }
                for row in residents
            ],
            "windows": [
                {
                    "catalog_version": row[0],
                    "stage_order": row[1],
                    "state": row[2],
                    "window_id": row[3],
                }
                for row in windows
            ],
            "window": {
                "catalog_version": windows[-1][0],
                "stage_order": windows[-1][1],
                "state": windows[-1][2],
                "window_id": windows[-1][3],
            },
            "filesystem_status_authority": False,
        }
    finally:
        pool.putconn(connection)


def execute(args) -> dict[str, object]:
    suffix = uuid4().hex[:12]
    output = args.output_directory
    output.mkdir(parents=True)
    database = f"test_realtime_v2_v3dr_{suffix}"
    roles = build_role_names_v2(f"test_rt_v3dr_{suffix}")
    login = roles.prefix + "_worker"
    project = f"stockscreener-v3dr-{suffix}"
    container_prefix = f"stockscreener-v3dr-{suffix}"
    journal_volume = f"{project}-journals"
    report_volume = f"{project}-reports"
    network = f"{project}-network"
    secrets_dir = output / "secrets"
    package_path = output / "package.json"
    bundle_path = output / "execution-bundle.json"
    override_path = output / "compose.override.json"
    settings = _settings()
    cleanup = {
        "containers": None,
        "database": None,
        "roles": None,
        "secrets": None,
        "volumes": None,
        "network": None,
    }
    compose_environment = None
    services = []
    durable = None
    resident_identity_before = {}
    resident_identity_after = {}
    started_at = datetime.now(UTC)
    roles_owned = False
    login_owned = False
    try:
        _docker("image", "inspect", args.image_id)
        with disposable_v2_database(database) as pool:
            seed = _seed(
                pool,
                session_date=args.session_date,
                suffix=suffix,
            )
            connection = pool.getconn()
            try:
                _, security = harden_database_v2(connection, roles)
                roles_owned = True
                if any(not verdict.startswith("PASS") for _, verdict in security):
                    raise RuntimeError("disposable V3 security audit failed")
            finally:
                pool.putconn(connection)
            login, host_dsn = _worker_login(
                settings,
                database,
                roles,
            )
            login_owned = True
            secrets_dir.mkdir()
            _write_secret(
                secrets_dir / "realtime_v2_app_dsn",
                _container_dsn(host_dsn, "host.docker.internal"),
            )
            _write_secret(
                secrets_dir / "realtime_v2_provider_key",
                "provider-free-loopback-not-a-provider-credential",
            )
            package, bundle = _qualification_artifacts(seed, args)
            _seed_service_leases(pool, bundle)
            _write_json(package_path, package)
            _write_json(bundle_path, bundle)
            _write_json(override_path, _override())
            compose_environment = {
                **os.environ,
                "REALTIME_V2_BACKEND_IMAGE": args.image_id,
                "REALTIME_V2_DOCKER_SECRET_DIR": str(secrets_dir),
                "REALTIME_V2_P8_PACKAGE_PATH": str(package_path),
                "REALTIME_V2_P8_BUNDLE_PATH": str(bundle_path),
                "REALTIME_V2_P8_CONTAINER_PREFIX": container_prefix,
                "REALTIME_V2_ADVANCED_JOURNALS_VOLUME_NAME": (
                    journal_volume
                ),
                "REALTIME_V2_ADVANCED_REPORTS_VOLUME_NAME": report_volume,
                "REALTIME_V2_APPLICATION_NETWORK_NAME": network,
            }
            resident_services = {
                role: service
                for role, service in SERVICES.items()
                if role != "WINDOW"
            }
            resident_command = _compose_args(
                override_path,
                project,
                "up",
                "--detach",
                "--no-build",
                *resident_services.values(),
            )
            try:
                _run(
                    resident_command,
                    environment=compose_environment,
                    timeout=120,
                )
                resident_identity_before = {
                    role: _service_container_id(
                        compose_environment,
                        override_path,
                        project,
                        service,
                    )
                    for role, service in resident_services.items()
                }
                _run(
                    _compose_args(
                        override_path,
                        project,
                        "up",
                        "--detach",
                        "--no-build",
                        SERVICES["WINDOW"],
                    ),
                    environment=compose_environment,
                    timeout=120,
                )
                _run(
                    _compose_args(
                        override_path,
                        project,
                        "up",
                        "--detach",
                        "--no-build",
                        "--wait",
                        "--wait-timeout",
                        "120",
                        *SERVICES.values(),
                    ),
                    environment=compose_environment,
                    timeout=150,
                )
            except Exception as error:
                logs = _run(
                    _compose_args(
                        override_path,
                        project,
                        "logs",
                        "--no-color",
                        "--timestamps",
                    ),
                    environment=compose_environment,
                    timeout=30,
                    check=False,
                )
                failure_log = output / "startup-failure.log"
                failure_log.write_bytes(
                    logs.stdout + b"\n" + logs.stderr
                )
                _capture_service_states(
                    compose_environment,
                    override_path,
                    project,
                    SERVICES.values(),
                    output / "startup-failure-states.json",
                )
                raise RuntimeError(
                    "V3 production role Docker startup failed; "
                    f"captured logs at {failure_log}"
                ) from error
            resident_rows = [
                _inspect_service(
                    compose_environment,
                    override_path,
                    project,
                    service,
                    role,
                )
                for role, service in resident_services.items()
            ]
            services = resident_rows + [
                _inspect_service(
                    compose_environment,
                    override_path,
                    project,
                    SERVICES["WINDOW"],
                    "WINDOW",
                )
            ]
            resident_identity_after = {
                role: _inspect_service(
                    compose_environment,
                    override_path,
                    project,
                    service,
                    role,
                )["container_id"]
                for role, service in resident_services.items()
            }
            if resident_identity_after != resident_identity_before:
                raise RuntimeError(
                    "V3 WINDOW start recreated a resident container"
                )
            window_container_before = services[-1]["container_id"]
            first_window_status = _role_status(
                window_container_before,
                "WINDOW",
            )
            durable = _durable_evidence(pool, seed)
            try:
                _run(
                    _compose_args(
                        override_path,
                        project,
                        "restart",
                        SERVICES["WINDOW"],
                    ),
                    environment=compose_environment,
                    timeout=90,
                )
                _run(
                    _compose_args(
                        override_path,
                        project,
                        "up",
                        "--detach",
                        "--no-build",
                        "--wait",
                        "--wait-timeout",
                        "90",
                        SERVICES["WINDOW"],
                    ),
                    environment=compose_environment,
                    timeout=120,
                )
            except RuntimeError as error:
                logs = _run(
                    _compose_args(
                        override_path,
                        project,
                        "logs",
                        "--no-color",
                        "--timestamps",
                        SERVICES["WINDOW"],
                    ),
                    environment=compose_environment,
                    timeout=30,
                    check=False,
                )
                failure_log = output / "window-restart-failure.log"
                failure_log.write_bytes(
                    logs.stdout + b"\n" + logs.stderr
                )
                _capture_service_states(
                    compose_environment,
                    override_path,
                    project,
                    (SERVICES["WINDOW"],),
                    output / "window-restart-failure-state.json",
                )
                raise RuntimeError(
                    "V3 WINDOW restart failed; captured logs at "
                    f"{failure_log}"
                ) from error
            window_container_after = _service_container_id(
                compose_environment,
                override_path,
                project,
                SERVICES["WINDOW"],
            )
            restarted_window_status = _role_status(
                window_container_after,
                "WINDOW",
            )
            if (
                window_container_after != window_container_before
                or first_window_status["role_evidence"][
                    "qualification"
                ]["deferred_attempts"] < 1
                or first_window_status["role_evidence"][
                    "qualification"
                ]["rehydrated"]
                or not restarted_window_status["role_evidence"][
                    "qualification"
                ]["rehydrated"]
            ):
                raise RuntimeError(
                    "V3 WINDOW defer/restart qualification changed"
                )
            _run(
                _compose_args(
                    override_path,
                    project,
                    "stop",
                    "--timeout",
                    "60",
                    *SERVICES.values(),
                ),
                environment=compose_environment,
                timeout=90,
            )
            _run(
                _compose_args(
                    override_path,
                    project,
                    "down",
                    "--volumes",
                    "--remove-orphans",
                ),
                environment=compose_environment,
                timeout=90,
            )
    finally:
        if compose_environment is not None and _project_containers(project):
            _run(
                _compose_args(
                    override_path,
                    project,
                    "down",
                    "--volumes",
                    "--remove-orphans",
                ),
                environment=compose_environment,
                timeout=90,
                check=False,
            )
        if secrets_dir.exists():
            shutil.rmtree(secrets_dir)
        if login_owned:
            _drop_login(settings, login)
        if roles_owned:
            with closing(_connect(settings, "postgres")) as connection:
                drop_disposable_roles_v2(connection, roles)
                connection.commit()
        with closing(_connect(settings, "postgres")) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_database "
                    "WHERE datname=%s)",
                    (database,),
                )
                cleanup["database"] = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                    ([*roles.all_names, login],),
                )
                cleanup["roles"] = [
                    row[0] for row in cursor.fetchall()
                ]
        cleanup["containers"] = list(_project_containers(project))
        cleanup["secrets"] = secrets_dir.exists()
        cleanup["volumes"] = [
            value
            for value in (journal_volume, report_volume)
            if _run(
                (
                    "docker",
                    "volume",
                    "inspect",
                    value,
                ),
                check=False,
            ).returncode
            == 0
        ]
        cleanup["network"] = (
            _run(
                ("docker", "network", "inspect", network),
                check=False,
            ).returncode
            == 0
        )
    if any((
        cleanup["database"],
        cleanup["roles"],
        cleanup["containers"],
        cleanup["secrets"],
        cleanup["volumes"],
        cleanup["network"],
    )):
        raise RuntimeError(
            f"V3 production role Docker cleanup is not empty: {cleanup}"
        )
    window_order = [
        row["window_id"] for row in durable["windows"]
    ]
    resident_offsets = {
        member: [
            next(
                row["ending_offset"]
                for row in durable["residents"]
                if row["window_id"] == window_id
                and row["member"] == member
            )
            for window_id in window_order
        ]
        for member in ("INGRESS", "MARKET_STATE", "FLOW")
    }
    report = {
        "contract": (
            "advanced-options-v3-production-role-docker-"
            "qualification-report-v1"
        ),
        "verdict": "PASS",
        "started_at_utc": started_at,
        "completed_at_utc": datetime.now(UTC),
        "scope": {
            "owned_project": project,
            "owned_database": database,
            "owned_roles": list(roles.all_names) + [login],
            "profile": "realtime-v3-options",
            "execution_flag": "--no-build",
            "image_id": args.image_id,
        },
        "package": {
            "path": str(package_path),
            "sha256": package["package_sha256"],
            "file_sha256": hashlib.sha256(
                package_path.read_bytes()
            ).hexdigest(),
        },
        "bundle": {
            "path": str(bundle_path),
            "sha256": bundle["bundle_sha256"],
            "file_sha256": hashlib.sha256(
                bundle_path.read_bytes()
            ).hexdigest(),
        },
        "entrypoint": {
            "script": "scripts/run_advanced_options_v3_production.py",
            "service_flag": "--docker-service-kind",
            "health_flag": "--docker-health-kind",
            "provider_free_injection": (
                "REALTIME_V3_PROVIDER_FREE_QUALIFICATION=ENABLED"
            ),
            "rehearsal_process_used": False,
        },
        "services": services,
        "single_bundle_handoff": {
            "resident_roles_started_first": list(
                resident_identity_before
            ),
            "window_started_after_residents": True,
            "resident_container_ids_before": resident_identity_before,
            "resident_container_ids_after": resident_identity_after,
            "resident_recreation_count": 0,
            "window_count": len(window_order),
            "resident_offsets": resident_offsets,
            "resident_offsets_monotonic": all(
                values == sorted(values)
                and len(set(values)) == len(values)
                for values in resident_offsets.values()
            ),
            "acceptance": bundle[
                "provider_free_qualification"
            ]["acceptance"],
            "bundle_sha256": bundle["bundle_sha256"],
            "first_window_evidence": first_window_status[
                "role_evidence"
            ]["qualification"],
            "restart_window_evidence": restarted_window_status[
                "role_evidence"
            ]["qualification"],
            "window_container_id_before_restart": (
                window_container_before
            ),
            "window_container_id_after_restart": (
                window_container_after
            ),
        },
        "bound_provider_free_qualification": {
            "semantic_sha256": args.qualification_semantic_sha256,
            "file_sha256": args.qualification_file_sha256,
        },
        "durable_database_barrier": durable,
        "effects": dict(ZERO_EFFECTS),
        "cleanup": cleanup,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    report_path = output / (
        "advanced-options-v3-production-role-docker-qualification-"
        f"{report['semantic_sha256'][:16]}.json"
    )
    _write_json(report_path, report)
    encoded = report_path.read_bytes()
    return {
        "verdict": "PASS",
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "image_id": args.image_id,
        "report": str(report_path),
        "cleanup": cleanup,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    result = execute(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
