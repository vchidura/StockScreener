"""Provider-free contracts and preflight for a bounded Advanced Options P7 shadow."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256
from .migrations import V2_MIGRATIONS_DIR, load_realtime_migrations


UTC = timezone.utc
CONTRACT_VERSION = "advanced-options-p7-shadow-v1"
APPROVAL_VERSION = "advanced-options-p7-shadow-approval-v1"
OWNER_DECISION = "approve_bounded_advanced_options_p7_shadow"
PROVIDER_ENDPOINT = "wss://socket.massive.com/options"
EVIDENCE_FILES = (
    "authorization.json",
    "source-hashes.json",
    "subscription-transcript.jsonl",
    "telemetry.jsonl",
    "provider-observations.json",
    "loss-accounting.json",
    "database-audit.json",
    "no-effects.json",
    "cleanup.json",
    "report.json",
)
PROHIBITED_ENV = (
    "POLYGON_API_KEY",
    "MASSIVE_API_KEY",
    "REALTIME_V2_ADMIN_DSN",
    "REALTIME_V2_APP_DSN",
    "REALTIME_V2_API_DSN",
    "DATABASE_URL",
)
SOURCE_BINDING_PATHS = (
    "provider_endpoints.py",
    "realtime/advanced_gateway.py",
    "realtime/buffer.py",
    "realtime/advanced_options_ingress.py",
    "realtime/advanced_options_consumers.py",
    "realtime/advanced_options_provider_contract.py",
    "realtime/advanced_options_control_plane.py",
    "realtime/advanced_options_p7_shadow.py",
    "realtime/advanced_options_p7_executor.py",
    "realtime/option_flow/consumer.py",
    "realtime/option_flow/engine.py",
    "realtime/option_flow/writer.py",
    "realtime/security_v2.py",
    "scripts/prepare_advanced_options_p7_shadow.py",
    "scripts/run_advanced_options_p7_shadow.py",
    "tests/realtime/advanced_options_reference_support.py",
    "tests/realtime/option_flow_runner_support.py",
    "tests/realtime/option_flow_support.py",
    "tests/realtime/test_option_flow_runner_v2_db.py",
    "tests/realtime/v2_database.py",
)
EXPECTED_P7_MIGRATIONS = (
    "025_realtime_advanced_options_control_plane",
    "026_advanced_options_flow_evidence",
    "027_advanced_options_provider_references",
    "028_advanced_options_reference_state",
    "029_advanced_options_rolling_checkpoints",
    "030_advanced_options_flow_package_evidence",
)
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_OPTION_PATTERN = re.compile(
    r"^O:(?P<underlyer>[A-Z][A-Z0-9.-]{0,14})"
    r"\d{6}[CP]\d{8}$"
)


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: str, name: str) -> datetime:
    text = _text(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{name} must be an ISO timestamp") from None
    return _utc(parsed, name)


def _contract(value: str, underlyer: str) -> str:
    ticker = _text(value, f"{underlyer} contract")
    match = _OPTION_PATTERN.fullmatch(ticker)
    if match is None or match.group("underlyer") != underlyer:
        raise ValueError(
            f"{underlyer} contract must be one explicit {underlyer} option"
        )
    if "*" in ticker:
        raise ValueError("P7 shadow contracts cannot contain wildcards")
    return ticker


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        raise ValueError(f"P7 source binding is missing: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_bindings(backend_dir: Path) -> list[dict[str, str]]:
    root = backend_dir.resolve()
    return [
        {
            "path": relative,
            "sha256": _sha256_file(root / Path(relative)),
        }
        for relative in SOURCE_BINDING_PATHS
    ]


def _migration_bindings() -> list[dict[str, str]]:
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    if (
        len(migrations) != 31
        or migrations[-1].sequence != 30
        or tuple(row.version for row in migrations[-6:])
        != EXPECTED_P7_MIGRATIONS
    ):
        raise ValueError("P7 requires the exact realtime migration lineage 000-030")
    return [
        {
            "version": row.version,
            "checksum_sha256": row.checksum_sha256,
        }
        for row in migrations
    ]


def build_authorization(
    *,
    run_id: UUID,
    authorization_reference: str,
    baseline_head: str,
    start_at: datetime,
    controlled_reconnect_at: datetime,
    stop_at: datetime,
    spy_contract: str,
    qqq_contract: str,
    backend_dir: Path,
    artifact_root: Path,
) -> dict[str, object]:
    if not isinstance(run_id, UUID):
        raise TypeError("run_id must be a UUID")
    reference = _text(authorization_reference, "authorization_reference")
    head = _text(baseline_head, "baseline_head").lower()
    if _COMMIT_PATTERN.fullmatch(head) is None:
        raise ValueError("baseline_head must be a 40-character commit digest")
    start = _utc(start_at, "start_at")
    reconnect = _utc(controlled_reconnect_at, "controlled_reconnect_at")
    stop = _utc(stop_at, "stop_at")
    if reconnect - start != timedelta(minutes=5):
        raise ValueError("controlled reconnect must be exactly five minutes after start")
    if stop - start != timedelta(minutes=10):
        raise ValueError("P7 shadow window must be exactly ten minutes")
    if start.date() != stop.date():
        raise ValueError("P7 shadow must remain inside one UTC session date")
    contracts = {
        "SPY": _contract(spy_contract, "SPY"),
        "QQQ": _contract(qqq_contract, "QQQ"),
    }
    if len(set(contracts.values())) != 2:
        raise ValueError("P7 shadow requires two distinct contracts")
    date_token = start.strftime("%Y%m%d")
    evidence_directory = (
        artifact_root.resolve()
        / f"advanced-options-p7-{start.strftime('%Y%m%dT%H%M%SZ')}"
    )
    database = f"test_realtime_v2_options_p7_{date_token}"
    role_prefix = f"test_rt_options_p7_{date_token}"
    roles = [
        f"{role_prefix}_admin",
        f"{role_prefix}_app",
        f"{role_prefix}_api",
    ]
    identity: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "state": "AWAITING_OWNER_APPROVAL",
        "run_id": str(run_id),
        "authorization_reference": reference,
        "baseline_head": head,
        "window": {
            "session_date": start.date().isoformat(),
            "start_utc": start.isoformat().replace("+00:00", "Z"),
            "controlled_reconnect_utc": reconnect.isoformat().replace(
                "+00:00", "Z"
            ),
            "stop_utc": stop.isoformat().replace("+00:00", "Z"),
            "missed_window_policy": "EXPIRE_WITHOUT_ROLLOVER",
        },
        "provider": {
            "provider": "massive",
            "account_alias": "MASSIVE_ADVANCED",
            "product": "US_OPTIONS_ADVANCED_REALTIME",
            "endpoint": PROVIDER_ENDPOINT,
            "feed": "OPTIONS",
            "channels": ["T", "Q"],
            "rest_requests_authorized": 0,
            "incremental_purchase_authorized": False,
        },
        "scope": {
            "process_owners": 1,
            "underlyings": ["SPY", "QQQ"],
            "contracts": [contracts["SPY"], contracts["QQQ"]],
            "subscriptions": [
                f"T.{contracts['SPY']}",
                f"Q.{contracts['SPY']}",
                f"T.{contracts['QQQ']}",
                f"Q.{contracts['QQQ']}",
            ],
            "wildcard_subscriptions": False,
            "contract_substitution": False,
            "scope_expansion": False,
        },
        "socket_ceiling": {
            "stream_sessions": 1,
            "connection_attempts": 2,
            "reconnects": 1,
            "auth_messages": 2,
            "subscription_messages": 2,
            "websocket_frames": 25_000,
            "decoded_market_messages": 100_000,
            "inbound_decoded_bytes": 32 * 1024 * 1024,
            "outbound_bytes": 64 * 1024,
            "maximum_frame_bytes": 512 * 1024,
            "library_max_queue": 8,
            "open_timeout_seconds": 10,
            "ping_interval_seconds": 20,
            "ping_timeout_seconds": 10,
            "close_timeout_seconds": 5,
        },
        "application_bounds": {
            "buffer_events": 2_048,
            "buffer_bytes": 32 * 1024 * 1024,
            "buffer_maximum_age_seconds": 60,
            "journal_batch_messages": 250,
            "journal_batch_bytes": 4 * 1024 * 1024,
            "abort_buffer_events": 1_536,
            "abort_buffer_bytes": 24 * 1024 * 1024,
            "abort_oldest_age_seconds": 5,
            "abort_postgresql_flush_seconds": 2,
            "final_drain_seconds": 30,
        },
        "database": {
            "name": database,
            "role_prefix": role_prefix,
            "roles": roles,
            "security_mode": "hardened",
            "production_ddl_authorized": False,
        },
        "services": {
            "process_count": 1,
            "owners": [
                "advanced-options-ingress-v1",
                "market-state-materializer-v1",
                "option_flow_stream_v1",
            ],
            "production_services_authorized": False,
            "stages_v4_selected": False,
        },
        "production_authority": {
            "production_acquisition": False,
            "production_runtime_activation": False,
            "publication": False,
            "release": False,
            "pointer_advance": False,
            "outbox_dispatch": False,
            "alert_delivery": False,
            "trade_execution": False,
            "repair": False,
            "backfill": False,
            "historical_replay": False,
            "archive": False,
            "retention_execution": False,
            "application_data_delete": False,
        },
        "source_bindings": _source_bindings(backend_dir),
        "migrations": _migration_bindings(),
        "evidence": {
            "directory": str(evidence_directory),
            "files": list(EVIDENCE_FILES),
            "create_exclusive": True,
            "retain_raw_provider_payloads": False,
        },
        "cleanup": {
            "database": database,
            "roles": roles,
            "temporary_directory": str(evidence_directory / "temporary"),
            "journal_files": [
                str(evidence_directory / "temporary" / "options.sqlite"),
                str(evidence_directory / "temporary" / "options.sqlite-wal"),
                str(evidence_directory / "temporary" / "options.sqlite-shm"),
            ],
            "application_data_delete_authorized": False,
            "owned_disposable_cleanup_required": True,
        },
        "provider_requests_authorized": 0,
        "database_connections_authorized": 0,
        "database_ddl_authorized": 0,
        "files_written_by_preflight": 0,
        "live_shadow_authority": {
            "provider_websocket_connection_attempts": 2,
            "provider_rest_requests": 0,
            "disposable_database_ddl": True,
            "run_owned_local_files": True,
            "production_database_connections": 0,
            "production_effects": False,
        },
        "live_execution_implemented": True,
    }
    plan_sha256 = canonical_sha256(identity)
    return {
        **identity,
        "manifest_id": str(
            uuid5(NAMESPACE_URL, f"{CONTRACT_VERSION}:{plan_sha256}")
        ),
        "plan_sha256": plan_sha256,
    }


def validate_authorization(
    manifest: Mapping[str, object],
    *,
    backend_dir: Path,
) -> dict[str, object]:
    if not isinstance(manifest, dict):
        raise TypeError("P7 authorization manifest must be a JSON object")
    required = {
        "run_id",
        "authorization_reference",
        "baseline_head",
        "window",
        "scope",
        "evidence",
    }
    missing = required - set(manifest)
    if missing:
        raise ValueError(
            "P7 authorization manifest is missing: " + ",".join(sorted(missing))
        )
    window = manifest["window"]
    scope = manifest["scope"]
    evidence = manifest["evidence"]
    if not all(isinstance(value, dict) for value in (window, scope, evidence)):
        raise TypeError("P7 authorization sections must be JSON objects")
    contracts = scope.get("contracts")
    if not isinstance(contracts, list) or len(contracts) != 2:
        raise ValueError("P7 authorization requires exactly two contracts")
    directory = evidence.get("directory")
    if not isinstance(directory, str):
        raise TypeError("P7 evidence directory must be text")
    expected = build_authorization(
        run_id=UUID(str(manifest["run_id"])),
        authorization_reference=str(manifest["authorization_reference"]),
        baseline_head=str(manifest["baseline_head"]),
        start_at=_timestamp(str(window.get("start_utc")), "start_utc"),
        controlled_reconnect_at=_timestamp(
            str(window.get("controlled_reconnect_utc")),
            "controlled_reconnect_utc",
        ),
        stop_at=_timestamp(str(window.get("stop_utc")), "stop_utc"),
        spy_contract=str(contracts[0]),
        qqq_contract=str(contracts[1]),
        backend_dir=backend_dir,
        artifact_root=Path(directory).resolve().parent,
    )
    if manifest != expected:
        raise ValueError(
            "P7 authorization manifest changed or its source bindings drifted"
        )
    return expected


def approved_authorization(
    manifest_path: Path,
    approval_path: Path,
    *,
    backend_dir: Path,
) -> dict[str, object]:
    body = manifest_path.read_bytes()
    manifest = json.loads(body)
    approval = json.loads(approval_path.read_text(encoding="utf-8"))
    expected_approval_keys = {
        "contract_version",
        "manifest_id",
        "manifest_sha256",
        "authorization_reference",
        "owner_decision",
        "conditional_live_shadow_authorized",
        "provider_websocket_connection_attempts_authorized",
        "provider_rest_requests_authorized",
        "production_effects_authorized",
    }
    if not isinstance(approval, dict) or set(approval) != expected_approval_keys:
        raise ValueError("P7 shadow approval fields are not exact")
    if (
        approval["contract_version"] != APPROVAL_VERSION
        or approval["manifest_id"] != manifest.get("manifest_id")
        or approval["authorization_reference"]
        != manifest.get("authorization_reference")
        or approval["manifest_sha256"]
        != hashlib.sha256(body).hexdigest()
        or approval["owner_decision"] != OWNER_DECISION
        or approval["conditional_live_shadow_authorized"] is not True
        or approval["provider_websocket_connection_attempts_authorized"] != 2
        or approval["provider_rest_requests_authorized"] != 0
        or approval["production_effects_authorized"] is not False
    ):
        raise ValueError("P7 shadow lacks exact conditional owner approval")
    return validate_authorization(manifest, backend_dir=backend_dir)


def provider_free_preflight(
    manifest: Mapping[str, object],
    *,
    backend_dir: Path,
    now: datetime,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    plan = validate_authorization(manifest, backend_dir=backend_dir)
    current = _utc(now, "now")
    start = _timestamp(
        str(plan["window"]["start_utc"]),
        "start_utc",
    )
    if current >= start:
        raise ValueError("P7 shadow authorization window is no longer prospective")
    environment = os.environ if environ is None else environ
    present = sorted(name for name in PROHIBITED_ENV if environment.get(name))
    if present:
        raise RuntimeError(
            "provider-free P7 preflight received prohibited credentials: "
            + ",".join(present)
        )
    evidence_directory = Path(str(plan["evidence"]["directory"]))
    if evidence_directory.exists():
        raise FileExistsError("P7 evidence directory must be new")
    return {
        "status": "PASS_PROVIDER_FREE_PREFLIGHT",
        "manifest_id": plan["manifest_id"],
        "plan_sha256": plan["plan_sha256"],
        "authorization_reference": plan["authorization_reference"],
        "window_status": "PROSPECTIVE",
        "provider_requests": 0,
        "external_socket_connections": 0,
        "database_connections": 0,
        "database_ddl": 0,
        "files_written": 0,
        "live_execution_available": bool(
            plan["live_execution_implemented"]
        ),
        "publication_performed": False,
        "release_performed": False,
        "pointer_advanced": False,
        "execution_performed": False,
        "delivery_performed": False,
        "repair_performed": False,
        "backfill_performed": False,
        "archive_retention_performed": False,
        "application_data_deleted": False,
        "evidence_files": plan["evidence"]["files"],
        "cleanup_targets": plan["cleanup"],
    }
