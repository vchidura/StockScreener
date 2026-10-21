"""Provider-free authorization and postflight contracts for archive reconciliation."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence
from uuid import UUID

from .session_archive import (
    ARCHIVE_MANIFEST_CONTRACT,
    ARCHIVE_SCHEMA_VERSION,
    KIND_RELATIONS,
    ArchiveIdentity,
    archive_file_names,
    build_archive,
    render_manifest,
)


AUTHORIZATION_SCHEMA = "options-retention-reconciliation-authorization-v1"
POSTFLIGHT_SCHEMA = "options-retention-reconciliation-postflight-v1"
POSTFLIGHT_OBSERVATION_SCHEMA = (
    "options-retention-reconciliation-postflight-observation-v1"
)
EXECUTION_STATE = "AUTHORIZED_FOR_ONE_APPEND_ONLY_RECONCILIATION"
MISSED_WINDOW_POLICY = "EXPIRE_WITHOUT_ROLLOVER_OR_REUSE"
SOURCE_FILES = (
    "backend/realtime/advanced_options_close.py",
    "backend/realtime/retention_reconciliation_authorization.py",
    "backend/realtime/session_archive.py",
    "backend/realtime/session_archive_repository.py",
    "backend/realtime/session_archive_worker.py",
    "backend/scripts/audit_realtime_v2_retention_archive.py",
    "backend/scripts/prepare_realtime_v2_retention_reconciliation_authorization.py",
    "docker-compose.realtime-v2.yml",
)
_FLOW_KINDS = ("OPTION_FLOW_EVENTS", "OPTION_FLOW_CLUSTERS")
_SHA256_CHARACTERS = frozenset("0123456789abcdef")
_UTC = timezone.utc


def _canonical_sha256(value: object) -> str:
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
        raise ValueError("execution_nonce must be a UUID") from error
    if parsed.version != 4:
        raise ValueError("execution_nonce must be a UUID4")
    return str(parsed)


def _source_code_identity(
    repository_root: Path,
    source_files: Sequence[str],
) -> dict[str, object]:
    root = repository_root.resolve()
    if not root.is_dir():
        raise ValueError("repository_root must be an existing directory")
    normalized = tuple(sorted(set(source_files)))
    if not normalized or normalized != tuple(source_files):
        raise ValueError("authorization source files must be unique and sorted")
    files: dict[str, str] = {}
    for relative in normalized:
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ValueError("authorization source file escaped repository_root") from error
        if not path.is_file():
            raise FileNotFoundError(f"authorization source file is unavailable: {relative}")
        files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "files": files,
        "source_sha256": _canonical_sha256(files),
    }


def _weekly_partition(relation: str, session_date: date) -> str:
    lower = session_date.fromordinal(
        session_date.toordinal() - session_date.weekday()
    )
    return f"{relation}_p{lower:%Y%m%d}"


def _revision_scopes(
    plan: Mapping[str, object],
) -> tuple[dict[str, object], ...]:
    source_report = plan.get("source_report")
    rows = plan.get("reconciliation")
    if (
        not isinstance(source_report, dict)
        or not isinstance(rows, list)
        or len(rows) != len(_FLOW_KINDS)
    ):
        raise ValueError("frozen retention PLAN is incomplete")
    try:
        session_date = date.fromisoformat(str(source_report["session_date"]))
    except (KeyError, ValueError) as error:
        raise ValueError("frozen retention PLAN session date is invalid") from error
    by_kind = {
        row.get("kind"): row
        for row in rows
        if isinstance(row, dict) and row.get("kind") in _FLOW_KINDS
    }
    if set(by_kind) != set(_FLOW_KINDS):
        raise ValueError("frozen retention PLAN kind coverage is incomplete")

    scopes = []
    for kind in _FLOW_KINDS:
        row = by_kind[kind]
        target = row.get("target_manifest")
        if not isinstance(target, dict):
            raise ValueError(f"{kind} target manifest is unavailable")
        build_id = str(target.get("build_id", "")).strip()
        policy_id = str(target.get("policy_id", "")).strip()
        identity = ArchiveIdentity(build_id, policy_id)
        built = build_archive(kind, session_date, ())
        relation_counts = {
            relation: 0
            for relation in KIND_RELATIONS[kind]
        }
        if (
            row.get("action") != "APPEND_CONTENT_ADDRESSED_REVISION"
            or row.get("preserve_original_bytes_and_timestamps") is not True
            or target.get("archive_manifest_contract")
            != ARCHIVE_MANIFEST_CONTRACT
            or target.get("content_sha256") != built.content_sha256
            or target.get("relation_counts") != relation_counts
            or target.get("partition_coverage") != built.partition_coverage
            or target.get("source_identity") != built.source_identity
        ):
            raise ValueError(f"{kind} frozen reconciliation scope drifted")
        data_file, manifest_file = archive_file_names(
            kind,
            content_sha256=built.content_sha256,
            identity_sha256=identity.sha256,
        )
        manifest_bytes = render_manifest(
            built,
            file_name=data_file,
            identity=identity,
        )
        preserved_data_file = str(row.get("preserved_file", "")).strip()
        preserved_manifest_file = str(row.get("preserved_manifest", "")).strip()
        if (
            not preserved_data_file
            or not preserved_manifest_file
            or preserved_data_file == data_file
            or preserved_manifest_file == manifest_file
        ):
            raise ValueError(f"{kind} append-only filenames are not distinct")
        relations = tuple(KIND_RELATIONS[kind])
        preserved_data_sha256 = _sha256(
            target.get("file_sha256"),
            f"{kind} preserved file_sha256",
        )
        scopes.append({
            "kind": kind,
            "relations": list(relations),
            "partitions": [
                _weekly_partition(relation, session_date)
                for relation in relations
            ],
            "source_counts": relation_counts,
            "gzip_relation_counts": relation_counts,
            "flow_identity": built.source_identity,
            "preserved": {
                "data_file": f"{session_date.isoformat()}/{preserved_data_file}",
                "data_sha256": preserved_data_sha256,
                "manifest_file": (
                    f"{session_date.isoformat()}/{preserved_manifest_file}"
                ),
                "preserve_bytes": True,
                "preserve_timestamps": True,
            },
            "append_only_revision": {
                "data_file": f"{session_date.isoformat()}/{data_file}",
                "data_sha256": built.sha256,
                "content_sha256": built.content_sha256,
                "manifest_file": f"{session_date.isoformat()}/{manifest_file}",
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "archive_manifest_contract": ARCHIVE_MANIFEST_CONTRACT,
                "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
                "archive_identity_sha256": identity.sha256,
                "build_id": identity.build_id,
                "policy_id": identity.policy_id,
                "row_count": 0,
                "relation_counts": relation_counts,
                "partition_coverage": built.partition_coverage,
                "source_identity": built.source_identity,
                "write_mode": "CREATE_NEW_EXCLUSIVE",
            },
        })
    return tuple(scopes)


def _postflight_contract(
    *,
    authorization_scope_sha256: str,
    source_report_sha256: str,
    frozen_plan_sha256: str,
    session_date: str,
    revisions: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    originals = []
    expected_revisions = []
    ledger_rows = []
    for revision in revisions:
        preserved = revision["preserved"]
        append_only = revision["append_only_revision"]
        originals.append({
            "kind": revision["kind"],
            "data_file": preserved["data_file"],
            "data_sha256": preserved["data_sha256"],
            "manifest_file": preserved["manifest_file"],
            "bytes_unchanged": True,
            "timestamps_unchanged": True,
        })
        expected_revisions.append({
            "kind": revision["kind"],
            "data_file": append_only["data_file"],
            "data_sha256": append_only["data_sha256"],
            "content_sha256": append_only["content_sha256"],
            "manifest_file": append_only["manifest_file"],
            "manifest_sha256": append_only["manifest_sha256"],
            "archive_manifest_contract": append_only[
                "archive_manifest_contract"
            ],
            "archive_schema_version": append_only["archive_schema_version"],
            "archive_identity_sha256": append_only[
                "archive_identity_sha256"
            ],
            "build_id": append_only["build_id"],
            "policy_id": append_only["policy_id"],
            "row_count": 0,
            "relation_counts": append_only["relation_counts"],
            "partition_coverage": append_only["partition_coverage"],
            "source_identity": append_only["source_identity"],
        })
        ledger_rows.append({
            "session_date": session_date,
            "kind": revision["kind"],
            "uri_relative": append_only["data_file"],
            "sha256": append_only["data_sha256"],
            "row_count": 0,
            "schema_version": append_only["archive_schema_version"],
            "build_id": append_only["build_id"],
        })
    return {
        "schema": POSTFLIGHT_SCHEMA,
        "mode": "READ_ONLY_POSTFLIGHT",
        "authorization_scope_sha256": authorization_scope_sha256,
        "source_report_sha256": source_report_sha256,
        "frozen_plan_sha256": frozen_plan_sha256,
        "expected_originals": originals,
        "expected_revisions": expected_revisions,
        "expected_database": {
            "inserted_session_archive_rows": ledger_rows,
            "unexpected_rows": 0,
            "updates": 0,
            "deletes": 0,
            "ddl": 0,
        },
        "expected_filesystem": {
            "unexpected_files": [],
            "deleted_files": [],
            "rewritten_files": [],
            "cleanup_paths_remaining": [],
        },
        "expected_services": {
            "retention_before": "STOPPED",
            "retention_after": "STOPPED",
            "archive_before": "STOPPED",
            "archive_after": "STOPPED",
            "starts": 0,
            "restarts": 0,
        },
        "expected_external_effects": {
            "provider_requests": 0,
            "publication_actions": 0,
            "delivery_actions": 0,
            "repair_actions": 0,
            "backfill_actions": 0,
            "production_replay_actions": 0,
        },
        "success_state": {
            "status": "PASS_RECONCILIATION_POSTFLIGHT_RETENTION_STOPPED",
            "retention_stopped": True,
            "retention_resume_authorized": False,
        },
    }


def build_reconciliation_authorization(
    plan: Mapping[str, object],
    *,
    repository_root: Path,
    source_files: Sequence[str] = SOURCE_FILES,
    expected_report_sha256: str,
    expected_plan_sha256: str,
    authorization_reference: str,
    execution_nonce: UUID | str,
    prepared_at: datetime,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, object]:
    """Build one immutable authorization package; this function performs no execution."""
    report_sha256 = _sha256(
        expected_report_sha256,
        "expected_report_sha256",
    )
    plan_sha256 = _sha256(expected_plan_sha256, "expected_plan_sha256")
    reference = authorization_reference.strip()
    if not reference or len(reference) > 160:
        raise ValueError("authorization_reference must be 1 to 160 characters")
    nonce = _nonce(execution_nonce)
    prepared = _utc(prepared_at, "prepared_at")
    start = _utc(window_start, "window_start")
    stop = _utc(window_end, "window_end")
    if start <= prepared or stop <= start:
        raise ValueError("reconciliation authorization window must be prospective")
    if (stop - start).total_seconds() != 600:
        raise ValueError("reconciliation authorization window must be exactly ten minutes")
    if (
        plan.get("status") != "PASS"
        or plan.get("mode") != "PLAN"
        or plan.get("safe_to_resume_retention") is not False
        or plan.get("frozen_plan_sha256") != plan_sha256
    ):
        raise ValueError("retention PLAN identity or stopped state changed")
    source_report = plan.get("source_report")
    if (
        not isinstance(source_report, dict)
        or source_report.get("sha256") != report_sha256
    ):
        raise ValueError("retention report identity changed")
    revisions = _revision_scopes(plan)
    source_code = _source_code_identity(repository_root, source_files)
    session_date = str(source_report["session_date"])
    allowed_relations = sorted({
        relation
        for revision in revisions
        for relation in revision["relations"]
    })
    allowed_partitions = sorted({
        partition
        for revision in revisions
        for partition in revision["partitions"]
    })
    flattened_files = sorted(
        (
            item
            for revision in revisions
            for item in (
            {
                "path": revision["preserved"]["data_file"],
                "access": "READ_PRESERVE",
            },
            {
                "path": revision["preserved"]["manifest_file"],
                "access": "READ_PRESERVE",
            },
            {
                "path": revision["append_only_revision"]["data_file"],
                "access": "CREATE_NEW_EXCLUSIVE",
            },
            {
                "path": revision["append_only_revision"]["manifest_file"],
                "access": "CREATE_NEW_EXCLUSIVE",
            },
            )
        ),
        key=lambda item: (item["path"], item["access"]),
    )
    scope = {
        "session_date": session_date,
        "read_relations": allowed_relations,
        "read_partitions": allowed_partitions,
        "files": flattened_files,
        "database_writes": [{
            "relation": "rt.session_archives",
            "operation": "INSERT_EXACT",
            "maximum_rows": 2,
        }],
        "database_updates_authorized": 0,
        "database_deletes_authorized": 0,
        "database_ddl_authorized": 0,
    }
    authorization_scope = {
        "source_report_sha256": report_sha256,
        "frozen_plan_sha256": plan_sha256,
        "source_code": source_code,
        "archive_identity": {
            "build_ids": sorted({
                revision["append_only_revision"]["build_id"]
                for revision in revisions
            }),
            "policy_ids": sorted({
                revision["append_only_revision"]["policy_id"]
                for revision in revisions
            }),
            "archive_identity_sha256": sorted({
                revision["append_only_revision"]["archive_identity_sha256"]
                for revision in revisions
            }),
            "source_flow_ids": [],
            "journal_generations": [],
        },
        "scope": scope,
        "revisions": list(revisions),
    }
    authorization_scope_sha256 = _canonical_sha256(authorization_scope)
    payload: dict[str, object] = {
        "schema": AUTHORIZATION_SCHEMA,
        "state": EXECUTION_STATE,
        "authorization_reference": reference,
        "prepared_at_utc": prepared.isoformat(),
        "execution_nonce": nonce,
        "window": {
            "start_utc": start.isoformat(),
            "stop_utc": stop.isoformat(),
            "missed_window_policy": MISSED_WINDOW_POLICY,
            "rollover_authorized": False,
            "reuse_authorized": False,
        },
        "authorization_scope": authorization_scope,
        "authorization_scope_sha256": authorization_scope_sha256,
        "authority": {
            "append_only_reconciliation_authorized": True,
            "archive_file_create_authorized": 4,
            "session_archive_ledger_insert_rows_authorized": 2,
            "original_file_rewrite_authorized": False,
            "original_timestamp_rewrite_authorized": False,
            "relation_expansion_authorized": False,
            "file_expansion_authorized": False,
            "archive_delete_authorized": False,
            "partition_prune_authorized": False,
            "retention_start_authorized": False,
            "archive_service_start_authorized": False,
            "service_restart_authorized": False,
        },
    }
    postflight = _postflight_contract(
        authorization_scope_sha256=authorization_scope_sha256,
        source_report_sha256=report_sha256,
        frozen_plan_sha256=plan_sha256,
        session_date=session_date,
        revisions=revisions,
    )
    payload["postflight_contract"] = postflight
    payload["postflight_contract_sha256"] = _canonical_sha256(postflight)
    payload["package_sha256"] = _canonical_sha256(payload)
    return payload


def validate_reconciliation_authorization(
    package: object,
    *,
    current_plan: Mapping[str, object],
    report_path: Path,
    repository_root: Path,
    expected_package_sha256: str,
    now: datetime,
    prior_execution_records: Sequence[Mapping[str, object]] | None,
    source_files: Sequence[str] = SOURCE_FILES,
) -> dict[str, object]:
    """Fail closed unless the package still exactly matches source, evidence and time."""
    if not isinstance(package, dict):
        raise TypeError("reconciliation authorization package must be a JSON object")
    expected_sha256 = _sha256(
        expected_package_sha256,
        "expected_package_sha256",
    )
    actual_sha256 = package.get("package_sha256")
    unsigned = dict(package)
    unsigned.pop("package_sha256", None)
    if (
        actual_sha256 != expected_sha256
        or _canonical_sha256(unsigned) != expected_sha256
    ):
        raise ValueError("reconciliation authorization package hash changed")
    report_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    scope = package.get("authorization_scope")
    window = package.get("window")
    if not isinstance(scope, dict) or not isinstance(window, dict):
        raise ValueError("reconciliation authorization fields are incomplete")
    if report_sha256 != scope.get("source_report_sha256"):
        raise ValueError("reconciliation source report drifted")
    expected = build_reconciliation_authorization(
        current_plan,
        repository_root=repository_root,
        source_files=source_files,
        expected_report_sha256=report_sha256,
        expected_plan_sha256=str(scope.get("frozen_plan_sha256", "")),
        authorization_reference=str(
            package.get("authorization_reference", "")
        ),
        execution_nonce=str(package.get("execution_nonce", "")),
        prepared_at=_timestamp(package.get("prepared_at_utc"), "prepared_at_utc"),
        window_start=_timestamp(window.get("start_utc"), "window.start_utc"),
        window_end=_timestamp(window.get("stop_utc"), "window.stop_utc"),
    )
    if package != expected:
        raise ValueError(
            "reconciliation authorization field, source or scope drifted"
        )
    current = _utc(now, "now")
    prepared = _timestamp(package["prepared_at_utc"], "prepared_at_utc")
    start = _timestamp(window["start_utc"], "window.start_utc")
    stop = _timestamp(window["stop_utc"], "window.stop_utc")
    if current < prepared:
        raise ValueError("reconciliation authorization predates validation clock")
    if current >= stop:
        raise ValueError("reconciliation authorization expired without rollover")
    if prior_execution_records is None:
        raise ValueError("prior reconciliation execution state is ambiguous")
    if not isinstance(prior_execution_records, Sequence):
        raise TypeError("prior_execution_records must be an explicit sequence")
    if prior_execution_records:
        if any(
            record.get("execution_nonce") == package["execution_nonce"]
            for record in prior_execution_records
            if isinstance(record, Mapping)
        ):
            raise ValueError("reconciliation authorization was already used")
        raise ValueError("prior reconciliation execution is ambiguous")
    return {
        "status": "PASS_RECONCILIATION_AUTHORIZATION_PREFLIGHT",
        "package_sha256": expected_sha256,
        "authorization_scope_sha256": package[
            "authorization_scope_sha256"
        ],
        "postflight_contract_sha256": package[
            "postflight_contract_sha256"
        ],
        "window_status": "PROSPECTIVE" if current < start else "ACTIVE",
        "execution_nonce": package["execution_nonce"],
        "prior_execution_records": 0,
        "provider_requests": 0,
        "database_connections": 0,
        "database_writes": 0,
        "archive_writes": 0,
        "archive_deletes": 0,
        "retention_started": False,
    }


def validate_postflight_observation(
    package: Mapping[str, object],
    observation: object,
) -> dict[str, object]:
    """Validate one read-only postflight result and keep retention stopped."""
    if not isinstance(observation, dict):
        raise TypeError("postflight observation must be a JSON object")
    required = {
        "schema",
        "authorization_package_sha256",
        "mode",
        "observed_at_utc",
        "originals",
        "revisions",
        "database",
        "filesystem",
        "services",
        "external_effects",
        "cleanup",
    }
    if set(observation) != required:
        raise ValueError("postflight observation fields are not exact")
    if (
        observation["schema"] != POSTFLIGHT_OBSERVATION_SCHEMA
        or observation["mode"] != "READ_ONLY"
        or observation["authorization_package_sha256"]
        != package.get("package_sha256")
    ):
        raise ValueError("postflight observation identity changed")
    _timestamp(observation["observed_at_utc"], "observed_at_utc")
    contract = package.get("postflight_contract")
    if (
        not isinstance(contract, dict)
        or _canonical_sha256(contract)
        != package.get("postflight_contract_sha256")
    ):
        raise ValueError("postflight contract identity changed")

    originals = observation["originals"]
    if not isinstance(originals, list):
        raise ValueError("postflight original evidence is unavailable")
    expected_originals = {
        row["kind"]: row
        for row in contract["expected_originals"]
    }
    actual_originals = {
        row.get("kind"): row
        for row in originals
        if isinstance(row, dict)
    }
    if set(actual_originals) != set(expected_originals):
        raise ValueError("postflight original evidence coverage changed")
    for kind, expected in expected_originals.items():
        actual = actual_originals[kind]
        if (
            set(actual) != {
                "kind",
                "data_file",
                "data_sha256_before",
                "data_sha256_after",
                "data_mtime_ns_before",
                "data_mtime_ns_after",
                "manifest_file",
                "manifest_sha256_before",
                "manifest_sha256_after",
                "manifest_mtime_ns_before",
                "manifest_mtime_ns_after",
            }
            or actual["data_file"] != expected["data_file"]
            or actual["manifest_file"] != expected["manifest_file"]
            or actual["data_sha256_before"] != expected["data_sha256"]
            or actual["data_sha256_after"] != expected["data_sha256"]
            or type(actual["data_mtime_ns_before"]) is not int
            or type(actual["data_mtime_ns_after"]) is not int
            or actual["data_mtime_ns_before"]
            != actual["data_mtime_ns_after"]
            or actual["manifest_sha256_before"]
            != actual["manifest_sha256_after"]
            or type(actual["manifest_mtime_ns_before"]) is not int
            or type(actual["manifest_mtime_ns_after"]) is not int
            or actual["manifest_mtime_ns_before"]
            != actual["manifest_mtime_ns_after"]
        ):
            raise ValueError("postflight original bytes or timestamps changed")
        _sha256(actual["manifest_sha256_before"], "original manifest sha256")

    revisions = observation["revisions"]
    if revisions != contract["expected_revisions"]:
        raise ValueError(
            "postflight revision checksum, identity or companion coverage changed"
        )
    if observation["database"] != contract["expected_database"]:
        raise ValueError("postflight database rows or mutation counts changed")
    if observation["filesystem"] != contract["expected_filesystem"]:
        raise ValueError("postflight filesystem scope or cleanup changed")
    if observation["services"] != contract["expected_services"]:
        raise ValueError("postflight service state changed")
    if observation["external_effects"] != contract["expected_external_effects"]:
        raise ValueError("postflight external effects changed")
    if observation["cleanup"] != {
        "temporary_files": [],
        "temporary_databases": [],
        "temporary_roles": [],
    }:
        raise ValueError("postflight cleanup is incomplete")
    success = contract["success_state"]
    return {
        **success,
        "authorization_package_sha256": package["package_sha256"],
        "postflight_contract_sha256": package[
            "postflight_contract_sha256"
        ],
        "observed_at_utc": observation["observed_at_utc"],
    }
