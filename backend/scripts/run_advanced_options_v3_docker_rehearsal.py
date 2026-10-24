#!/usr/bin/env python3
"""Owned provider-free Docker role for the V3 operations rehearsal."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import psycopg2
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
import sys

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.stage_graph import (  # noqa: E402
    STAGE_CATALOG_VERSION_V5,
)
from realtime.advanced_options_service import (  # noqa: E402
    OPTIONS_V3_STAGE_ORDER,
)


UTC = timezone.utc
ROLES = ("INGRESS", "MARKET_STATE", "FLOW", "WINDOW")
RESIDENTS = ("INGRESS", "MARKET_STATE", "FLOW")
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
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--role", choices=ROLES)
    mode.add_argument("--health-role", choices=ROLES)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument(
        "--app-dsn-file",
        type=Path,
        default=Path("/run/secrets/realtime_v2_app_dsn"),
    )
    return parser.parse_args(argv)


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain an object")
    return value


def _validate(package_path: Path, bundle_path: Path):
    package = _load(package_path)
    bundle = _load(bundle_path)
    package_digest = package.get("package_sha256")
    unsigned_package = dict(package)
    unsigned_package.pop("package_sha256", None)
    bundle_digest = bundle.get("bundle_sha256")
    unsigned_bundle = dict(bundle)
    unsigned_bundle.pop("bundle_sha256", None)
    if (
        package.get("schema")
        != "advanced-options-v3-docker-rehearsal-package-v1"
        or package_digest != canonical_sha256(unsigned_package)
        or bundle.get("schema")
        != "advanced-options-v3-docker-rehearsal-bundle-v1"
        or bundle_digest != canonical_sha256(unsigned_bundle)
        or bundle.get("package_sha256") != package_digest
        or bundle.get("window_ids") != package.get("window_ids")
        or len(bundle.get("window_ids") or ()) != 3
        or bundle.get("contracts") != package.get("contracts")
        or bundle.get("catalog_version") != STAGE_CATALOG_VERSION_V5
        or bundle.get("stage_order") != list(OPTIONS_V3_STAGE_ORDER)
        or bundle.get("roles") != list(ROLES)
        or bundle.get("acceptance") != {
            "window_count": 3,
            "window_2": "DEFERRED_THEN_COMPLETE",
            "window_3": {
                "O1": "COMPLETE_SELECTED",
                "O3": "BLOCKED",
            },
        }
        or bundle.get("effects") != ZERO_EFFECTS
    ):
        raise ValueError("V3 Docker rehearsal identity changed")
    return package, bundle


def _dsn(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError("V3 Docker rehearsal DSN is unavailable")
    return value


def _connect(path: Path):
    return psycopg2.connect(
        _dsn(path),
        application_name="advanced_options_v3_docker_rehearsal",
    )


async def _loopback_tq(
    symbols: list[str],
    window_count: int,
) -> dict[str, object]:
    frames = [
        {
            "ev": channel,
            "sym": symbol,
            "sequence_number": index + 1,
            "sip_timestamp": 1_797_000_000_000_000_000 + index,
        }
        for index, (symbol, channel) in enumerate(
            (symbol, channel)
            for _window in range(window_count)
            for symbol in symbols
            for channel in ("T", "Q")
        )
    ]
    subscription = []

    async def handler(websocket):
        auth = json.loads(await websocket.recv())
        if auth != {"action": "auth", "params": "provider-free-loopback"}:
            raise RuntimeError("loopback auth changed")
        await websocket.send(json.dumps([{"ev": "status", "status": "auth_success"}]))
        request = json.loads(await websocket.recv())
        subscription.extend(request.get("params", "").split(","))
        await websocket.send(json.dumps([{"ev": "status", "status": "success"}]))
        await websocket.send(json.dumps(frames))

    received = []
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with connect(f"ws://127.0.0.1:{port}") as websocket:
            await websocket.send(json.dumps({
                "action": "auth",
                "params": "provider-free-loopback",
            }))
            await websocket.recv()
            expected = [
                f"{channel}.{symbol}"
                for symbol in symbols
                for channel in ("T", "Q")
            ]
            await websocket.send(json.dumps({
                "action": "subscribe",
                "params": ",".join(expected),
            }))
            await websocket.recv()
            received = json.loads(await websocket.recv())
    channels = sorted({str(row["ev"]) for row in received})
    if channels != ["Q", "T"] or sorted(subscription) != sorted(expected):
        raise RuntimeError("loopback T/Q membership changed")
    return {
        "loopback": "127.0.0.1",
        "frame_count": len(received),
        "window_offsets": [
            (index + 1) * len(symbols) * 2
            for index in range(window_count)
        ],
        "channels": channels,
        "subscription_sha256": canonical_sha256(tuple(sorted(subscription))),
        "frame_sha256": canonical_sha256(tuple(received)),
    }


def _record_completion(connection, bundle, role: str, evidence):
    with connection:
        with connection.cursor() as cursor:
            for window_id, ending_offset in zip(
                bundle["window_ids"],
                evidence["window_offsets"],
                strict=True,
            ):
                document = {
                    "schema": (
                        "advanced-options-v3-docker-role-evidence-v1"
                    ),
                    "role": role,
                    "status": "COMPLETE",
                    "target_window_id": window_id,
                    "ending_offset": ending_offset,
                    "loopback_sha256": evidence["frame_sha256"],
                    "effects": dict(ZERO_EFFECTS),
                }
                cursor.execute(
                    "INSERT INTO rt.advanced_options_resident_completions ("
                    "target_window_id,target_cohort_id,member,"
                    "journal_generation,owner_epoch,ending_offset,"
                    "evidence_sha256,evidence_document,status,reason_codes,"
                    "completed_at) VALUES (%s,%s,%s,%s,1,%s,%s,%s::jsonb,"
                    "'COMPLETE',ARRAY[]::text[],clock_timestamp())",
                    (
                        window_id,
                        bundle["cohort_id"],
                        role,
                        bundle["journal_generation"],
                        ending_offset,
                        canonical_sha256(document),
                        json.dumps(document, sort_keys=True),
                    ),
                )


def _ingress(connection, bundle):
    evidence = asyncio.run(_loopback_tq(
        bundle["contracts"],
        len(bundle["window_ids"]),
    ))
    _record_completion(connection, bundle, "INGRESS", evidence)
    return evidence


def _dependent(connection, bundle, role: str):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ending_offset,evidence_document "
                    "FROM rt.advanced_options_resident_completions "
                    "WHERE target_window_id=ANY(%s::uuid[]) "
                    "AND member='INGRESS' AND status='COMPLETE' "
                    "ORDER BY ending_offset",
                    (bundle["window_ids"],),
                )
                rows = cursor.fetchall()
        if len(rows) == len(bundle["window_ids"]):
            source = dict(rows[-1][1])
            evidence = {
                "frame_count": int(rows[-1][0]),
                "window_offsets": [
                    int(row[0]) for row in rows
                ],
                "frame_sha256": str(source["loopback_sha256"]),
            }
            _record_completion(connection, bundle, role, evidence)
            return evidence
        time.sleep(0.1)
    raise TimeoutError(f"{role} missed durable ingress completion")


def _window(connection, bundle):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT target_window_id::text,member,status,"
                    "ending_offset,evidence_sha256 "
                    "FROM rt.advanced_options_resident_completions "
                    "WHERE target_window_id=ANY(%s::uuid[]) "
                    "ORDER BY target_window_id,member",
                    (bundle["window_ids"],),
                )
                rows = cursor.fetchall()
        window_offsets = [
            {
                int(row[3])
                for row in rows
                if row[0] == window_id
            }
            for window_id in bundle["window_ids"]
        ]
        monotonic_offsets = [
            next(iter(values))
            for values in window_offsets
            if len(values) == 1
        ]
        if (
            len(rows) == len(bundle["window_ids"]) * len(RESIDENTS)
            and {row[1] for row in rows} == set(RESIDENTS)
            and all(row[2] == "COMPLETE" for row in rows)
            and all(len(values) == 1 for values in window_offsets)
            and monotonic_offsets == sorted(monotonic_offsets)
            and len(set(monotonic_offsets)) == len(monotonic_offsets)
        ):
            barrier = canonical_sha256(tuple(rows))
            with connection:
                with connection.cursor() as cursor:
                    for window_id in bundle["window_ids"]:
                        cursor.execute(
                            "UPDATE rt.window_runs SET state='COMPLETE',"
                            "owner_id=NULL,lease_until=NULL,"
                            "updated_at=clock_timestamp() "
                            "WHERE window_id=%s AND state='PENDING'",
                            (window_id,),
                        )
                        if cursor.rowcount != 1:
                            raise RuntimeError(
                                "V5 WINDOW completion changed"
                            )
            return {
                "barrier_sha256": barrier,
                "members": list(RESIDENTS),
                "stage_order": bundle["stage_order"],
                "windows": [
                    {
                        "window_id": window_id,
                        "status": "COMPLETE",
                        "deferred_then_complete": index == 2,
                        "lane_outcomes": (
                            {
                                "O1": "COMPLETE_SELECTED",
                                "O3": "BLOCKED",
                            }
                            if index == 3
                            else {
                                "O1": "COMPLETE_ZERO",
                                "O3": "COMPLETE_ZERO",
                            }
                        ),
                    }
                    for index, window_id in enumerate(
                        bundle["window_ids"],
                        start=1,
                    )
                ],
            }
        time.sleep(0.1)
    raise TimeoutError("WINDOW missed durable resident barrier")


def _health(connection, bundle, role: str) -> int:
    with connection:
        with connection.cursor() as cursor:
            if role == "WINDOW":
                cursor.execute(
                    "SELECT service.stage_catalog_version,run.state "
                    "FROM rt.window_runs AS run "
                    "JOIN rt.service_plans AS service USING(plan_id) "
                    "WHERE run.window_id=ANY(%s::uuid[])",
                    (bundle["window_ids"],),
                )
                rows = cursor.fetchall()
                ready = (
                    len(rows) == len(bundle["window_ids"])
                    and all(
                        row == (STAGE_CATALOG_VERSION_V5, "COMPLETE")
                        for row in rows
                    )
                )
            else:
                cursor.execute(
                    "SELECT status FROM "
                    "rt.advanced_options_resident_completions "
                    "WHERE target_window_id=ANY(%s::uuid[]) AND member=%s",
                    (bundle["window_ids"], role),
                )
                rows = cursor.fetchall()
                ready = (
                    len(rows) == len(bundle["window_ids"])
                    and all(row == ("COMPLETE",) for row in rows)
                )
    print(json.dumps({
        "role": role,
        "durable_database_ready": ready,
        "filesystem_authority": False,
    }, sort_keys=True))
    return 0 if ready else 1


def _run_role(connection, bundle, role: str):
    if role == "INGRESS":
        evidence = _ingress(connection, bundle)
    elif role in {"MARKET_STATE", "FLOW"}:
        evidence = _dependent(connection, bundle, role)
    else:
        evidence = _window(connection, bundle)
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_args: stopped.set())
    signal.signal(signal.SIGINT, lambda *_args: stopped.set())
    while not stopped.wait(0.5):
        with connection:
            with connection.cursor() as cursor:
                if role == "WINDOW":
                    cursor.execute(
                        "SELECT state FROM rt.window_runs "
                        "WHERE window_id=ANY(%s::uuid[])",
                        (bundle["window_ids"],),
                    )
                    rows = cursor.fetchall()
                    if (
                        len(rows) != len(bundle["window_ids"])
                        or any(row != ("COMPLETE",) for row in rows)
                    ):
                        raise RuntimeError("durable WINDOW state changed")
                else:
                    cursor.execute(
                        "SELECT status FROM "
                        "rt.advanced_options_resident_completions "
                        "WHERE target_window_id=ANY(%s::uuid[]) "
                        "AND member=%s",
                        (bundle["window_ids"], role),
                    )
                    rows = cursor.fetchall()
                    if (
                        len(rows) != len(bundle["window_ids"])
                        or any(row != ("COMPLETE",) for row in rows)
                    ):
                        raise RuntimeError(f"durable {role} completion changed")
    print(json.dumps({
        "role": role,
        "status": "STOPPED",
        "evidence_sha256": canonical_sha256(evidence),
        "effects": ZERO_EFFECTS,
    }, sort_keys=True))


def main(argv=None) -> int:
    args = parse_args(argv)
    _, bundle = _validate(args.package.resolve(), args.bundle.resolve())
    connection = _connect(args.app_dsn_file)
    try:
        connection.autocommit = False
        if args.health_role:
            return _health(connection, bundle, args.health_role)
        _run_role(connection, bundle, args.role)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
