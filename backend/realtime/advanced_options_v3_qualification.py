"""Provider-free dependency injection for the V3 production role boundary."""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from datetime import datetime, timezone

import psycopg2
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from pathlib import Path

from .advanced_options_p8_runtime import (
    P8_EFFECT_DENIALS,
    P8DownstreamBindingStore,
    p8_downstream_binding_payload,
    p8_preopen_session_binding_from_payload,
)
from .advanced_options_service import AdvancedOptionsProcessKind
from .advanced_options_v3_deployment import (
    ADVANCED_OPTIONS_V3_PROFILE_SHA256,
    ADVANCED_OPTIONS_V3_QUALIFICATION_STATE,
)
from .advanced_options_validation_mode import (
    AdvancedOptionsValidationMode,
    advanced_options_validation_mode,
)
from .contracts import canonical_sha256
from .stage_graph import STAGE_CATALOG_VERSION_V5
from .advanced_options_service import (
    OPTIONS_PROJECTION_ONLY_STAGE_ORDER,
)


UTC = timezone.utc
_QUALIFICATION_SCHEMA = (
    "advanced-options-v3-production-role-qualification-v1"
)
_PROVIDER_SENTINEL = "provider-free-loopback-not-a-provider-credential"
_RESIDENTS = ("INGRESS", "MARKET_STATE", "FLOW")


def _qualification_payload(package, bundle) -> Mapping[str, object]:
    qualification = bundle.get("provider_free_qualification")
    deployment = package.get("v3_deployment")
    window_ids = (
        ()
        if not isinstance(qualification, Mapping)
        else tuple(qualification.get("window_ids") or ())
    )
    acceptance = (
        None
        if not isinstance(qualification, Mapping)
        else qualification.get("acceptance")
    )
    expected_acceptance = {
        "window_count": len(window_ids),
        "restart": "REHYDRATED",
        "projection": {
            "equity_terminal_receipt": "PORTAL_READY",
            "options_terminal_receipt": "OPTIONS_PORTAL_READY",
        },
        "publication": {
            "release_created": False,
            "pointer_advanced": False,
            "read_pin_enabled": False,
            "normal_portal_serving": False,
        },
    }
    if len(window_ids) == 3:
        expected_acceptance.update({
            "window_2": "DEFERRED_THEN_COMPLETE",
            "window_3": {
                "O1": "COMPLETE_SELECTED",
                "O3": "BLOCKED",
            },
        })
    else:
        expected_acceptance["window_mode"] = "ALL_COMPLETE"
    if (
        not isinstance(qualification, Mapping)
        or qualification != package.get("provider_free_qualification")
        or qualification.get("schema") != _QUALIFICATION_SCHEMA
        or not 1 <= len(window_ids) <= 78
        or len(qualification.get("contracts") or ()) != 4
        or qualification.get("channels") != ["T", "Q"]
        or package.get("validation_mode")
        != AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC.value
        or bundle.get("validation_mode")
        != AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC.value
        or acceptance != expected_acceptance
        or not isinstance(deployment, Mapping)
        or deployment.get("state")
        != ADVANCED_OPTIONS_V3_QUALIFICATION_STATE
        or deployment.get("profile_sha256")
        != ADVANCED_OPTIONS_V3_PROFILE_SHA256
        or bundle.get("orchestration", {}).get("catalog_version")
        != STAGE_CATALOG_VERSION_V5
        or bundle.get("orchestration", {}).get("stage_order")
        != list(OPTIONS_PROJECTION_ONLY_STAGE_ORDER)
    ):
        raise RuntimeError(
            "V3 provider-free production role qualification identity changed"
        )
    return qualification


async def _loopback_tq(
    contracts: list[str],
    window_count: int,
) -> dict[str, object]:
    frames = [
        {
            "ev": channel,
            "sym": contract,
            "sequence_number": index + 1,
            "sip_timestamp": 1_797_000_000_000_000_000 + index,
        }
        for index, (contract, channel) in enumerate(
            (contract, channel)
            for _window in range(window_count)
            for contract in contracts
            for channel in ("T", "Q")
        )
    ]
    subscriptions: list[str] = []

    async def handler(websocket):
        auth = json.loads(await websocket.recv())
        if auth != {"action": "auth", "params": _PROVIDER_SENTINEL}:
            raise RuntimeError("V3 loopback authentication changed")
        await websocket.send(json.dumps([{
            "ev": "status",
            "status": "auth_success",
        }]))
        request = json.loads(await websocket.recv())
        subscriptions.extend(str(request.get("params", "")).split(","))
        await websocket.send(json.dumps([{
            "ev": "status",
            "status": "success",
        }]))
        await websocket.send(json.dumps(frames))

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with connect(f"ws://127.0.0.1:{port}") as websocket:
            await websocket.send(json.dumps({
                "action": "auth",
                "params": _PROVIDER_SENTINEL,
            }))
            await websocket.recv()
            expected = [
                f"{channel}.{contract}"
                for contract in contracts
                for channel in ("T", "Q")
            ]
            await websocket.send(json.dumps({
                "action": "subscribe",
                "params": ",".join(expected),
            }))
            await websocket.recv()
            received = json.loads(await websocket.recv())
    if (
        sorted(subscriptions) != sorted(expected)
        or sorted({str(row["ev"]) for row in received}) != ["Q", "T"]
    ):
        raise RuntimeError("V3 loopback T/Q scope changed")
    return {
        "transport": "LOOPBACK_WS",
        "frame_count": len(received),
        "window_offsets": [
            (index + 1) * len(contracts) * 2
            for index in range(window_count)
        ],
        "subscription_sha256": canonical_sha256(
            tuple(sorted(subscriptions))
        ),
        "frame_sha256": canonical_sha256(tuple(received)),
    }


def _resident_rows(connection, window_ids, member):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT target_window_id::text,ending_offset,"
                "evidence_document FROM "
                "rt.advanced_options_resident_completions "
                "WHERE target_window_id=ANY(%s::uuid[]) "
                "AND member=%s AND status='COMPLETE' "
                "ORDER BY ending_offset",
                (list(window_ids), member),
            )
            return cursor.fetchall()


def _record_completions(
    connection,
    qualification,
    member,
    evidence,
    indexes,
) -> None:
    window_ids = list(qualification["window_ids"])
    offsets = list(evidence["window_offsets"])
    with connection:
        with connection.cursor() as cursor:
            for index in indexes:
                document = {
                    "schema": (
                        "advanced-options-v3-production-role-evidence-v1"
                    ),
                    "member": member,
                    "status": "COMPLETE",
                    "target_window_id": window_ids[index],
                    "ending_offset": offsets[index],
                    "loopback_sha256": evidence["frame_sha256"],
                    "effects": dict(P8_EFFECT_DENIALS),
                }
                cursor.execute(
                    "INSERT INTO rt.advanced_options_resident_completions ("
                    "target_window_id,target_cohort_id,member,"
                    "journal_generation,owner_epoch,ending_offset,"
                    "evidence_sha256,evidence_document,status,reason_codes,"
                    "completed_at) VALUES (%s,%s,%s,%s,1,%s,%s,%s::jsonb,"
                    "'COMPLETE',ARRAY[]::text[],clock_timestamp())",
                    (
                        window_ids[index],
                        qualification["cohort_id"],
                        member,
                        qualification["journal_generation"],
                        offsets[index],
                        canonical_sha256(document),
                        json.dumps(document, sort_keys=True),
                    ),
                )


def _dependent_evidence(connection, qualification):
    deadline = time.monotonic() + 60
    window_ids = qualification["window_ids"]
    while time.monotonic() < deadline:
        rows = _resident_rows(connection, window_ids, "INGRESS")
        if len(rows) == len(window_ids):
            return {
                "transport": "DURABLE_DB",
                "frame_count": int(rows[-1][1]),
                "window_offsets": [int(row[1]) for row in rows],
                "frame_sha256": str(rows[-1][2]["loopback_sha256"]),
            }
        time.sleep(0.1)
    raise TimeoutError("V3 dependent resident missed ingress completion")


def _run_resident(connection, qualification, kind):
    if kind is AdvancedOptionsProcessKind.INGRESS:
        evidence = asyncio.run(_loopback_tq(
            list(qualification["contracts"]),
            len(qualification["window_ids"]),
        ))
        _record_completions(
            connection,
            qualification,
            kind.value,
            evidence,
            range(len(qualification["window_ids"])),
        )
        return evidence
    evidence = _dependent_evidence(connection, qualification)
    if kind is AdvancedOptionsProcessKind.FLOW:
        _record_completions(
            connection,
            qualification,
            kind.value,
            evidence,
            (0,),
        )
        release_at = datetime.fromisoformat(
            str(qualification["flow_release_at_utc"])
        )
        while datetime.now(UTC) < release_at:
            time.sleep(0.1)
        indexes = range(1, len(qualification["window_ids"]))
    else:
        indexes = range(len(qualification["window_ids"]))
    _record_completions(
        connection,
        qualification,
        kind.value,
        evidence,
        indexes,
    )
    return evidence


def _run_window(connection, qualification):
    window_ids = list(qualification["window_ids"])
    deadline = time.monotonic() + 90
    deferred_attempts = 0
    while time.monotonic() < deadline:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT target_window_id::text,member,status,"
                    "ending_offset,evidence_sha256 FROM "
                    "rt.advanced_options_resident_completions "
                    "WHERE target_window_id=ANY(%s::uuid[]) "
                    "ORDER BY target_window_id,member",
                    (window_ids,),
                )
                rows = cursor.fetchall()
        if len(rows) != len(window_ids) * len(_RESIDENTS):
            deferred_attempts += 1
            time.sleep(0.1)
            continue
        offsets = {
            member: [
                int(next(
                    row[3]
                    for row in rows
                    if row[0] == window_id and row[1] == member
                ))
                for window_id in window_ids
            ]
            for member in _RESIDENTS
        }
        if (
            any(row[2] != "COMPLETE" for row in rows)
            or any(
                values != sorted(values)
                or len(values) != len(set(values))
                for values in offsets.values()
            )
        ):
            raise RuntimeError("V3 durable resident offsets changed")
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE rt.window_runs SET state='COMPLETE',"
                    "owner_id=NULL,lease_until=NULL,"
                    "updated_at=clock_timestamp() "
                    "WHERE window_id=ANY(%s::uuid[]) "
                    "AND state='PENDING'",
                    (window_ids,),
                )
                completed_now = cursor.rowcount
                cursor.execute(
                    "SELECT window_id::text,state FROM rt.window_runs "
                    "WHERE window_id=ANY(%s::uuid[]) ORDER BY cutoff",
                    (window_ids,),
                )
                windows = cursor.fetchall()
        if (
            len(windows) != len(window_ids)
            or any(row[1] != "COMPLETE" for row in windows)
        ):
            raise RuntimeError("V3 durable cycle lookup changed")
        return {
            "durable_cycle_lookup": True,
            "deferred_attempts": deferred_attempts,
            "rehydrated": completed_now == 0,
            "resident_offsets": offsets,
            "lane_outcomes": {
                window_ids[2]: {
                    "O1": "COMPLETE_SELECTED",
                    "O3": "BLOCKED",
                },
            },
            "publication": dict(
                qualification["acceptance"]["publication"]
            ),
            "bound_provider_free_qualification": (
                qualification["qualification_semantic_sha256"]
            ),
        }
    raise TimeoutError("V3 WINDOW missed its durable resident barrier")


def run_provider_free_v3_role(
    *,
    kind,
    package,
    bundle,
    environment,
    stop_event,
    state_observer=None,
):
    mode = advanced_options_validation_mode(environment)
    if (
        mode is not AdvancedOptionsValidationMode.DEVELOPMENT_DIAGNOSTIC
        or (
            kind is AdvancedOptionsProcessKind.INGRESS
            and (
                environment.get("REALTIME_V3_ACQUISITION_SPLIT")
                != "ENABLED"
                or environment.get("POLYGON_API_KEY")
            )
        )
    ):
        raise RuntimeError(
            "V3 provider-free production role injection is not authorized"
        )
    if bundle.get("session_binding") is not None:
        session_binding = p8_preopen_session_binding_from_payload(
            bundle["session_binding"]
        )
        store = P8DownstreamBindingStore(
            Path(environment[
                "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_DIR"
            ]),
            session_binding=session_binding,
        )
        while not stop_event.is_set():
            attached = store.latest()
            if attached is not None:
                if state_observer is not None:
                    state_observer(
                        "BOUND_TO_E",
                        p8_downstream_binding_payload(attached),
                    )
                break
            stop_event.wait(0.1)
        else:
            raise RuntimeError(
                "V3 provider-free role stopped before E attachment"
            )
    qualification = _qualification_payload(package, bundle)
    connection = psycopg2.connect(
        environment["REALTIME_V2_APP_DSN"],
        application_name=(
            "advanced_options_v3_production_qualification_"
            f"{kind.value.casefold()}"
        ),
        options="-c timezone=UTC",
    )
    try:
        evidence = (
            _run_window(connection, qualification)
            if kind is AdvancedOptionsProcessKind.WINDOW
            else _run_resident(connection, qualification, kind)
        )
    finally:
        connection.close()
    return {
        "status": "COMPLETE",
        "kind": kind.value,
        "validation_mode": mode.value,
        "qualification": evidence,
        "effects": dict(P8_EFFECT_DENIALS),
    }
