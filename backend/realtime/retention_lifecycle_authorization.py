"""Inert stop-only lifecycle authorization for the O-WP5 retention owner."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence
from uuid import UUID


LIFECYCLE_SCHEMA = "options-retention-lifecycle-authorization-v1"
POST_STOP_SCHEMA = "options-retention-post-stop-snapshot-v1"
OBSERVATION_SCHEMA = "options-retention-lifecycle-observation-v1"
READINESS_REPORT_SHA256 = (
    "4da89de05cb37eabe562792c37c671b57e4ecb0422042d320aaf9748c1cb7139"
)
SERVICE_NAME = "realtime-v2-retention"
CONTAINER_NAME = "stock-screener-realtime-v2-retention"
CLASSIFICATIONS = {
    "COMPLETE_VALID",
    "PARTIAL_AMBIGUOUS",
    "ORPHANED_FILES",
    "LEDGER_ONLY",
    "DIVERGENT",
}
MISSED_WINDOW_POLICY = "EXPIRE_WITHOUT_ROLLOVER_OR_REUSE"
SOURCE_FILES = (
    "backend/realtime/retention_lifecycle_authorization.py",
    "backend/scripts/audit_realtime_v2_retention_lifecycle.py",
    "backend/scripts/prepare_realtime_v2_retention_lifecycle_authorization.py",
)
_SHA256_CHARACTERS = frozenset("0123456789abcdef")
_UTC = timezone.utc


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a SHA-256 digest")
    normalized = value.strip().lower()
    if (
        len(normalized) != 64
        or any(character not in _SHA256_CHARACTERS for character in normalized)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return normalized


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(_UTC)


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO timestamp") from error
    return _utc(parsed, name)


def _nonce(value: object) -> str:
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError("lifecycle nonce must be a UUID") from error
    if parsed.version != 4:
        raise ValueError("lifecycle nonce must be a UUID4")
    return str(parsed)


def _load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _source_identity(
    repository_root: Path,
    source_files: Sequence[str],
) -> dict[str, object]:
    root = repository_root.resolve()
    selected = tuple(sorted(set(source_files)))
    if not selected or selected != tuple(source_files):
        raise ValueError("lifecycle source files must be unique and sorted")
    files = {}
    for relative in selected:
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ValueError("lifecycle source file escaped repository root") from error
        if not path.is_file():
            raise FileNotFoundError(relative)
        files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "files": files,
        "source_sha256": canonical_sha256(files),
    }


def classify_revision(evidence: Mapping[str, object]) -> str:
    """Classify one data/manifest pair and its ledger state without repair."""
    files = evidence.get("files")
    source_counts = evidence.get("source_counts")
    manifest_counts = evidence.get("manifest_relation_counts")
    file_counts = evidence.get("file_relation_counts")
    expected_relations = evidence.get("expected_relations")
    ledger = evidence.get("matching_ledger_rows")
    if (
        not isinstance(files, dict)
        or not isinstance(source_counts, dict)
        or not isinstance(manifest_counts, dict)
        or not isinstance(file_counts, dict)
        or not isinstance(expected_relations, list)
        or not isinstance(ledger, list)
    ):
        return "PARTIAL_AMBIGUOUS"
    data_present = files.get("data_present") is True
    manifest_present = files.get("manifest_present") is True
    pair_present = data_present and manifest_present
    pair_absent = not data_present and not manifest_present
    exact_counts = (
        set(source_counts) == set(expected_relations)
        and set(manifest_counts) == set(expected_relations)
        and set(file_counts) == set(expected_relations)
        and source_counts == manifest_counts == file_counts
    )
    internally_valid = (
        evidence.get("integrity_status") == "PASS"
        and evidence.get("content_matches") is True
        and exact_counts
    )
    if pair_absent and len(ledger) == 1:
        return "LEDGER_ONLY"
    if pair_present and internally_valid and not ledger:
        return "ORPHANED_FILES"
    if pair_present and internally_valid and len(ledger) == 1:
        row = ledger[0]
        if (
            isinstance(row, dict)
            and row.get("sha256") == files.get("data_sha256")
            and row.get("build_id") == evidence.get("build_id")
            and row.get("row_count") == sum(file_counts.values())
        ):
            return "COMPLETE_VALID"
        return "DIVERGENT"
    if data_present != manifest_present:
        return "PARTIAL_AMBIGUOUS"
    if pair_absent and not ledger:
        return "PARTIAL_AMBIGUOUS"
    return "DIVERGENT"


def _normalize_observation(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("lifecycle observation must be a JSON object")
    required = {
        "schema",
        "checked_at_utc",
        "matching_containers",
        "archive_inventory",
        "source_counts",
        "ledger_rows",
        "revision_evidence",
        "lock_or_partial_files",
        "package_target_files",
    }
    if set(value) != required or value.get("schema") != OBSERVATION_SCHEMA:
        raise ValueError("lifecycle observation fields are not exact")
    _timestamp(value["checked_at_utc"], "checked_at_utc")
    containers = value["matching_containers"]
    inventory = value["archive_inventory"]
    revisions = value["revision_evidence"]
    if (
        not isinstance(containers, list)
        or not isinstance(inventory, list)
        or not isinstance(revisions, list)
    ):
        raise ValueError("lifecycle observation collections are invalid")
    normalized = json.loads(json.dumps(value, sort_keys=True))
    normalized["archive_inventory"] = sorted(
        normalized["archive_inventory"],
        key=lambda row: row["path"],
    )
    normalized["ledger_rows"] = sorted(
        normalized["ledger_rows"],
        key=lambda row: (row["kind"], row["sha256"], row["build_id"]),
    )
    normalized["revision_evidence"] = sorted(
        normalized["revision_evidence"],
        key=lambda row: row["kind"],
    )
    normalized["lock_or_partial_files"] = sorted(
        normalized["lock_or_partial_files"]
    )
    normalized["package_target_files"] = sorted(
        normalized["package_target_files"]
    )
    for row in normalized["revision_evidence"]:
        row["classification"] = classify_revision(row)
        if row["classification"] not in CLASSIFICATIONS:
            raise AssertionError("unreachable revision classification")
    return normalized


def _assert_running_identity(observation: Mapping[str, object]) -> dict[str, object]:
    containers = observation["matching_containers"]
    if len(containers) != 1:
        raise ValueError("lifecycle authorization requires exactly one matching container")
    container = containers[0]
    required = {
        "id",
        "name",
        "service",
        "image_id",
        "image_created_at",
        "image_reference",
        "command",
        "command_sha256",
        "entrypoint",
        "started_at_utc",
        "status",
        "running",
        "health",
        "restart_policy",
        "compose_identity",
        "mounts",
        "live_source_hashes",
    }
    if set(container) != required:
        raise ValueError("container identity fields are not exact")
    if (
        container["name"] != CONTAINER_NAME
        or container["service"] != SERVICE_NAME
        or container["status"] != "running"
        or container["running"] is not True
        or container["health"] != "healthy"
        or container["restart_policy"] != {
            "name": "unless-stopped",
            "maximum_retry_count": 0,
        }
        or container["command_sha256"] != canonical_sha256(
            container["command"]
        )
    ):
        raise ValueError("observed retention container is not the exact running owner")
    _sha256(container["id"], "container id")
    _sha256(container["image_id"].removeprefix("sha256:"), "image id")
    _timestamp(container["started_at_utc"], "container started_at")
    return container


def _post_stop_contract(
    observation: Mapping[str, object],
    *,
    lifecycle_scope_sha256: str,
) -> dict[str, object]:
    container = observation["matching_containers"][0]
    return {
        "schema": POST_STOP_SCHEMA,
        "mode": "READ_ONLY_POST_STOP_SNAPSHOT",
        "lifecycle_scope_sha256": lifecycle_scope_sha256,
        "expected_container": {
            "matching_count": 1,
            "id": container["id"],
            "name": container["name"],
            "service": container["service"],
            "image_id": container["image_id"],
            "command_sha256": container["command_sha256"],
            "started_at_utc": container["started_at_utc"],
            "status": "exited",
            "running": False,
            "restart_policy": {
                "name": "no",
                "maximum_retry_count": 0,
            },
        },
        "expected_database_snapshot": {
            "transaction_read_only": "on",
            "transaction_isolation": "repeatable read",
            "writes": 0,
            "ledger_rows": observation["ledger_rows"],
            "source_counts": observation["source_counts"],
        },
        "expected_archive_inventory": observation["archive_inventory"],
        "expected_lock_or_partial_files": [],
        "expected_originals": [
            row for row in observation["archive_inventory"]
            if row["path"] in {
                "2026-10-19/option_flow_events.jsonl.gz",
                "2026-10-19/option_flow_events.manifest.json",
                "2026-10-19/option_flow_clusters.jsonl.gz",
                "2026-10-19/option_flow_clusters.manifest.json",
            }
        ],
        "expected_revision_classifications": {
            row["kind"]: row["classification"]
            for row in observation["revision_evidence"]
        },
        "forbidden_effects": {
            "container_start": 0,
            "container_restart": 0,
            "container_remove": 0,
            "database_writes": 0,
            "archive_writes": 0,
            "archive_deletes": 0,
            "partition_prunes": 0,
            "evidence_rewrites": 0,
            "retention_resume": 0,
            "provider_requests": 0,
        },
        "success_state": "PASS_RETENTION_STOPPED_SNAPSHOT_REQUIRES_NEW_PACKAGE",
    }


def build_lifecycle_authorization(
    readiness_report_path: Path,
    observation: Mapping[str, object],
    *,
    repository_root: Path,
    source_files: Sequence[str] = SOURCE_FILES,
    authorization_reference: str,
    lifecycle_nonce: UUID | str,
    prepared_at: datetime,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, object]:
    report_sha256 = hashlib.sha256(
        readiness_report_path.read_bytes()
    ).hexdigest()
    if report_sha256 != READINESS_REPORT_SHA256:
        raise ValueError("execution-readiness report identity changed")
    report = _load_json(readiness_report_path)
    if (
        report.get("status") != "BLOCKED_LIVE_STATE_DRIFT"
        or report.get("package_still_usable") is not False
        or report.get("production_execution_authorized") is not False
    ):
        raise ValueError("readiness blocker state changed")
    normalized = _normalize_observation(observation)
    container = _assert_running_identity(normalized)
    if normalized["lock_or_partial_files"]:
        raise ValueError("lifecycle package cannot be prepared with lock/partial files")
    reference = authorization_reference.strip()
    if not reference or len(reference) > 160:
        raise ValueError("authorization_reference must be 1 to 160 characters")
    nonce = _nonce(lifecycle_nonce)
    prepared = _utc(prepared_at, "prepared_at")
    start = _utc(window_start, "window_start")
    stop = _utc(window_end, "window_end")
    if start <= prepared or stop <= start:
        raise ValueError("lifecycle stop window must be prospective")
    if (stop - start).total_seconds() != 600:
        raise ValueError("lifecycle stop window must be exactly ten minutes")
    source_identity = _source_identity(repository_root, source_files)
    scope = {
        "readiness_report_sha256": report_sha256,
        "source_identity": source_identity,
        "observed_state": normalized,
        "container_identity": container,
        "allowed_lifecycle_actions": [
            {
                "action": "SET_RESTART_POLICY_NO",
                "container_id": container["id"],
            },
            {
                "action": "STOP_CONTAINER",
                "container_id": container["id"],
                "timeout_seconds": 30,
            },
        ],
    }
    scope_sha256 = canonical_sha256(scope)
    package: dict[str, object] = {
        "schema": LIFECYCLE_SCHEMA,
        "state": "AUTHORIZED_FOR_ONE_STOP_ONLY_WINDOW",
        "authorization_reference": reference,
        "prepared_at_utc": prepared.isoformat(),
        "lifecycle_nonce": nonce,
        "window": {
            "start_utc": start.isoformat(),
            "stop_utc": stop.isoformat(),
            "missed_window_policy": MISSED_WINDOW_POLICY,
            "rollover_authorized": False,
            "reuse_authorized": False,
        },
        "lifecycle_scope": scope,
        "lifecycle_scope_sha256": scope_sha256,
        "authority": {
            "matching_container_count": 1,
            "disable_automatic_restart": True,
            "stop_exact_container": True,
            "start_authorized": False,
            "restart_authorized": False,
            "resume_authorized": False,
            "container_remove_authorized": False,
            "reconciliation_authorized": False,
            "archive_write_authorized": False,
            "database_write_authorized": False,
            "delete_authorized": False,
            "prune_authorized": False,
            "evidence_rewrite_authorized": False,
        },
    }
    post_stop = _post_stop_contract(
        normalized,
        lifecycle_scope_sha256=scope_sha256,
    )
    package["post_stop_contract"] = post_stop
    package["post_stop_contract_sha256"] = canonical_sha256(post_stop)
    package["package_sha256"] = canonical_sha256(package)
    return package


def validate_lifecycle_authorization(
    package: object,
    *,
    readiness_report_path: Path,
    current_observation: Mapping[str, object],
    repository_root: Path,
    expected_package_sha256: str,
    now: datetime,
    prior_stop_records: Sequence[Mapping[str, object]] | None,
    source_files: Sequence[str] = SOURCE_FILES,
) -> dict[str, object]:
    if not isinstance(package, dict):
        raise TypeError("lifecycle authorization must be a JSON object")
    expected_sha256 = _sha256(
        expected_package_sha256,
        "expected_package_sha256",
    )
    unsigned = dict(package)
    actual_sha256 = unsigned.pop("package_sha256", None)
    if (
        actual_sha256 != expected_sha256
        or canonical_sha256(unsigned) != expected_sha256
    ):
        raise ValueError("lifecycle authorization package hash changed")
    window = package.get("window")
    if not isinstance(window, dict):
        raise ValueError("lifecycle authorization window is unavailable")
    current_for_comparison = json.loads(json.dumps(current_observation))
    try:
        current_for_comparison["checked_at_utc"] = package[
            "lifecycle_scope"
        ]["observed_state"]["checked_at_utc"]
    except (KeyError, TypeError) as error:
        raise ValueError(
            "lifecycle authorization baseline observation is unavailable"
        ) from error
    expected = build_lifecycle_authorization(
        readiness_report_path,
        current_for_comparison,
        repository_root=repository_root,
        source_files=source_files,
        authorization_reference=str(
            package.get("authorization_reference", "")
        ),
        lifecycle_nonce=str(package.get("lifecycle_nonce", "")),
        prepared_at=_timestamp(
            package.get("prepared_at_utc"),
            "prepared_at_utc",
        ),
        window_start=_timestamp(window.get("start_utc"), "window.start_utc"),
        window_end=_timestamp(window.get("stop_utc"), "window.stop_utc"),
    )
    if package != expected:
        raise ValueError("service, source, file, ledger or authority identity drifted")
    current = _utc(now, "now")
    prepared = _timestamp(package["prepared_at_utc"], "prepared_at_utc")
    start = _timestamp(window["start_utc"], "window.start_utc")
    stop = _timestamp(window["stop_utc"], "window.stop_utc")
    if current < prepared:
        raise ValueError("lifecycle authorization predates validation clock")
    if current >= stop:
        raise ValueError("lifecycle authorization expired without rollover")
    if prior_stop_records is None:
        raise ValueError("prior lifecycle stop state is ambiguous")
    if prior_stop_records:
        if any(
            row.get("lifecycle_nonce") == package["lifecycle_nonce"]
            for row in prior_stop_records
            if isinstance(row, Mapping)
        ):
            raise ValueError("lifecycle authorization was already used")
        raise ValueError("prior lifecycle stop is ambiguous")
    return {
        "status": "PASS_STOP_ONLY_LIFECYCLE_PREFLIGHT",
        "package_sha256": expected_sha256,
        "lifecycle_scope_sha256": package["lifecycle_scope_sha256"],
        "post_stop_contract_sha256": package[
            "post_stop_contract_sha256"
        ],
        "window_status": "PROSPECTIVE" if current < start else "ACTIVE",
        "matching_containers": 1,
        "production_writes": 0,
        "service_lifecycle_actions": 0,
        "provider_requests": 0,
    }


def lifecycle_status(
    package: Mapping[str, object],
    observation: Mapping[str, object],
) -> dict[str, object]:
    normalized = _normalize_observation(observation)
    containers = normalized["matching_containers"]
    if len(containers) != 1:
        return {
            "state": "AMBIGUOUS_CONTAINER_SET",
            "matching_containers": len(containers),
        }
    expected = package["lifecycle_scope"]["container_identity"]
    container = containers[0]
    identity_fields = (
        "id",
        "name",
        "service",
        "image_id",
        "command_sha256",
        "started_at_utc",
    )
    if any(container.get(name) != expected.get(name) for name in identity_fields):
        return {"state": "SERVICE_IDENTITY_DRIFT"}
    if (
        container["running"] is True
        and container["status"] == "running"
        and container["restart_policy"] == {
            "name": "unless-stopped",
            "maximum_retry_count": 0,
        }
    ):
        return {"state": "READY_FOR_EXPLICIT_STOP_AUTHORIZATION"}
    if (
        container["running"] is False
        and container["status"] == "exited"
        and container["restart_policy"] == {
            "name": "no",
            "maximum_retry_count": 0,
        }
    ):
        return {"state": "STOP_CONSUMED_POST_SNAPSHOT_REQUIRED"}
    if (
        container["running"] is True
        and container["restart_policy"].get("name") == "no"
    ):
        return {"state": "PARTIAL_RESTART_POLICY_CHANGED"}
    return {"state": "AMBIGUOUS_LIFECYCLE_STATE"}


def validate_post_stop_snapshot(
    package: Mapping[str, object],
    snapshot: object,
) -> dict[str, object]:
    if not isinstance(snapshot, dict):
        raise TypeError("post-stop snapshot must be a JSON object")
    contract = package.get("post_stop_contract")
    if (
        not isinstance(contract, dict)
        or canonical_sha256(contract)
        != package.get("post_stop_contract_sha256")
    ):
        raise ValueError("post-stop contract identity changed")
    required = {
        "schema",
        "lifecycle_package_sha256",
        "captured_at_utc",
        "container",
        "database_snapshot",
        "archive_inventory",
        "lock_or_partial_files",
        "revision_classifications",
        "effects",
    }
    if set(snapshot) != required or snapshot["schema"] != POST_STOP_SCHEMA:
        raise ValueError("post-stop snapshot fields are not exact")
    _timestamp(snapshot["captured_at_utc"], "captured_at_utc")
    if snapshot["lifecycle_package_sha256"] != package["package_sha256"]:
        raise ValueError("post-stop lifecycle package identity changed")
    expected_container = contract["expected_container"]
    if snapshot["container"] != expected_container:
        raise ValueError("post-stop container state or restart policy drifted")
    if snapshot["database_snapshot"] != contract["expected_database_snapshot"]:
        raise ValueError("post-stop database isolation or ledger state drifted")
    if snapshot["archive_inventory"] != contract["expected_archive_inventory"]:
        raise ValueError("post-stop archive inventory drifted")
    if snapshot["lock_or_partial_files"] != []:
        raise ValueError("post-stop lock or partial files remain")
    if (
        snapshot["revision_classifications"]
        != contract["expected_revision_classifications"]
    ):
        raise ValueError("post-stop revision classification drifted")
    if snapshot["effects"] != contract["forbidden_effects"]:
        raise ValueError("post-stop forbidden effects changed")
    return {
        "status": contract["success_state"],
        "lifecycle_package_sha256": package["package_sha256"],
        "post_stop_contract_sha256": package[
            "post_stop_contract_sha256"
        ],
        "retention_stopped": True,
        "retention_resume_authorized": False,
        "reconciliation_authorized": False,
    }
