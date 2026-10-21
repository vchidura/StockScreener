#!/usr/bin/env python3
"""Read-only reconciliation of retained V2 session archives."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from realtime.relation_registry import ADMIN_ROLE  # noqa: E402
from realtime.session_archive import (  # noqa: E402
    ARCHIVE_MANIFEST_CONTRACT,
    OPTION_FLOW_CLUSTERS,
    OPTION_FLOW_EVENTS,
    ArchiveVerificationError,
    KIND_RELATIONS,
    archive_directory,
    verify_archive,
)
from realtime.session_archive_repository import (  # noqa: E402
    PostgresSessionArchiveRepository,
)
from scripts.run_realtime_v2_retention_service import (  # noqa: E402
    RoleConnectionFactory,
    _validated_dsn,
)


KINDS = (OPTION_FLOW_EVENTS, OPTION_FLOW_CLUSTERS)
PLAN_SCHEMA = "options-retention-compatibility-plan-v1"
_PRIMARY_RELATIONS = {
    OPTION_FLOW_EVENTS: "rt.option_flow_events",
    OPTION_FLOW_CLUSTERS: "rt.option_flow_clusters",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database")
    parser.add_argument("--confirm-database-name")
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=Path("/var/lib/realtime-v2-retention/archive"),
    )
    parser.add_argument(
        "--admin-dsn-file",
        type=Path,
        default=Path("/run/secrets/realtime_v2_admin_dsn"),
    )
    parser.add_argument("--kind", action="append", choices=KINDS)
    parser.add_argument("--plan-report", type=Path)
    parser.add_argument("--expected-report-sha256")
    parser.add_argument("--expected-build-id")
    parser.add_argument("--expected-policy-id")
    return parser.parse_args(argv)


def _file_counts(
    path: Path,
    *,
    kind: str,
) -> tuple[dict[str, int], int, str, str]:
    payload = path.read_bytes()
    content = gzip.decompress(payload)
    counts = Counter({relation: 0 for relation in KIND_RELATIONS[kind]})
    rows = 0
    for line in content.splitlines():
        parsed = json.loads(line.decode("ascii"))
        counts[str(parsed["relation"])] += 1
        rows += 1
    return (
        dict(sorted(counts.items())),
        rows,
        hashlib.sha256(payload).hexdigest(),
        hashlib.sha256(content).hexdigest(),
    )


def inspect_manifest(
    manifest_path: Path,
    *,
    kind: str,
    session_date: date,
    current_counts: dict[str, int],
    require_retention_compatibility: bool = False,
) -> dict[str, object]:
    result: dict[str, object] = {
        "manifest": manifest_path.name,
        "current_source_counts": current_counts,
    }
    try:
        manifest = json.loads(manifest_path.read_text(encoding="ascii"))
        if not isinstance(manifest, dict):
            raise ValueError("manifest must be a JSON object")
        file_name = manifest.get("file")
        if not isinstance(file_name, str) or not file_name.strip():
            raise ValueError("manifest file identity is unavailable")
        data_path = manifest_path.parent / file_name
        result.update({
            "file": data_path.name,
            "build_id": manifest.get("build_id"),
            "policy_id": manifest.get("policy_id"),
            "manifest_relation_counts": manifest.get("relation_counts"),
        })
        file_counts, row_count, file_sha256, content_sha256 = (
            _file_counts(data_path, kind=kind)
        )
        result.update({
            "file_relation_counts": file_counts,
            "file_row_count": row_count,
            "file_sha256": file_sha256,
            "content_sha256": content_sha256,
            "manifest_matches_file_counts": (
                manifest.get("relation_counts") == file_counts
            ),
            "current_source_matches_file_counts": (
                current_counts == file_counts
            ),
        })
        verify_archive(
            data_path,
            manifest_path,
            kind=kind,
            session_date=session_date,
            require_retention_compatibility=require_retention_compatibility,
        )
        result["integrity_status"] = "PASS"
    except ArchiveVerificationError as error:
        result["integrity_status"] = "FAIL"
        result["verification_code"] = error.code.value
        result["verification_detail"] = error.detail
    except Exception as error:  # noqa: BLE001
        result["integrity_status"] = "FAIL"
        result["verification_code"] = type(error).__name__
        result["verification_detail"] = str(error)
    return result


def _sha256(value: str, name: str) -> str:
    normalized = value.strip().lower()
    if (
        len(normalized) != 64
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return normalized


def _load_report(path: Path) -> tuple[dict[str, object], str]:
    payload = path.read_bytes()
    report_sha256 = hashlib.sha256(payload).hexdigest()
    encoding = "utf-16" if payload.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8"
    try:
        report = json.loads(payload.decode(encoding))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("retention reconciliation report is not valid JSON") from error
    if not isinstance(report, dict):
        raise ValueError("retention reconciliation report must be a JSON object")
    return report, report_sha256


def _exact_counts(
    value: object,
    *,
    kind: str,
    name: str,
) -> dict[str, int]:
    expected = KIND_RELATIONS[kind]
    if (
        not isinstance(value, dict)
        or set(value) != set(expected)
        or any(type(value[relation]) is not int or value[relation] < 0 for relation in expected)
    ):
        raise ValueError(f"{kind} {name} does not cover every primary and companion relation")
    return {relation: int(value[relation]) for relation in expected}


def build_retention_compatibility_plan(
    report_path: Path,
    *,
    expected_report_sha256: str,
    expected_build_id: str,
    expected_policy_id: str,
) -> dict[str, object]:
    """Freeze an append-only reconciliation plan without database or archive writes."""
    expected_sha256 = _sha256(
        expected_report_sha256,
        "expected_report_sha256",
    )
    build_id = expected_build_id.strip()
    policy_id = expected_policy_id.strip()
    if not build_id or not policy_id:
        raise ValueError("expected build and policy identities cannot be blank")
    report, actual_sha256 = _load_report(report_path)
    if actual_sha256 != expected_sha256:
        raise ValueError("retention reconciliation report SHA-256 differs")
    expected_zero_effects = {
        "provider_requests": 0,
        "database_writes": 0,
        "archive_writes": 0,
        "archive_deletes": 0,
    }
    if any(report.get(name) != value for name, value in expected_zero_effects.items()):
        raise ValueError("retention reconciliation report records a prohibited effect")
    if (
        report.get("status") != "RECONCILIATION_REQUIRED"
        or report.get("safe_to_resume_retention") is not False
        or report.get("next_action")
        != "REVIEW_NEW_CONTENT_ADDRESSED_ARCHIVE_REVISION"
    ):
        raise ValueError("retention reconciliation report is not the stopped v3 state")
    rows = report.get("kinds")
    if not isinstance(rows, list):
        raise ValueError("retention reconciliation report kinds are unavailable")
    by_kind = {
        row.get("kind"): row
        for row in rows
        if isinstance(row, dict) and row.get("kind") in KINDS
    }
    if set(by_kind) != set(KINDS) or len(rows) != len(KINDS):
        raise ValueError("retention reconciliation report kind coverage is incomplete")

    reconciliation = []
    for kind in KINDS:
        row = by_kind[kind]
        current_counts = _exact_counts(
            row.get("current_source_counts"),
            kind=kind,
            name="current source counts",
        )
        if any(current_counts.values()):
            raise ValueError(f"{kind} source is no longer the frozen zero-row state")
        manifests = row.get("manifests")
        ledgers = row.get("ledger_records")
        if (
            not isinstance(manifests, list)
            or len(manifests) != 1
            or not isinstance(manifests[0], dict)
            or not isinstance(ledgers, list)
            or len(ledgers) != 1
            or not isinstance(ledgers[0], dict)
        ):
            raise ValueError(f"{kind} preserved manifest or ledger evidence is ambiguous")
        manifest = manifests[0]
        ledger = ledgers[0]
        file_counts = _exact_counts(
            manifest.get("file_relation_counts"),
            kind=kind,
            name="archive file counts",
        )
        primary_relation = _PRIMARY_RELATIONS[kind]
        if (
            file_counts != current_counts
            or manifest.get("current_source_matches_file_counts") is not True
            or manifest.get("manifest_matches_file_counts") is not False
            or manifest.get("manifest_relation_counts") != {primary_relation: 0}
            or manifest.get("integrity_status") != "FAIL"
            or manifest.get("verification_code") != "RELATION_COUNT_MISMATCH"
            or manifest.get("build_id") != build_id
            or manifest.get("policy_id") != policy_id
            or manifest.get("file_row_count") != 0
            or row.get("ready") is not False
            or row.get("exact_current_archive_count") != 0
        ):
            raise ValueError(f"{kind} preserved mismatch is not the expected companion omission")
        file_sha256 = _sha256(str(manifest.get("file_sha256", "")), "file_sha256")
        content_sha256 = _sha256(
            str(manifest.get("content_sha256", "")),
            "content_sha256",
        )
        if (
            ledger.get("build_id") != build_id
            or ledger.get("row_count") != 0
            or ledger.get("sha256") != file_sha256
        ):
            raise ValueError(f"{kind} ledger identity diverges from preserved archive evidence")
        partition_coverage = {
            "complete": True,
            "relation_counts": current_counts,
            "relations": list(KIND_RELATIONS[kind]),
            "session_date": report.get("session_date"),
        }
        reconciliation.append({
            "kind": kind,
            "action": "APPEND_CONTENT_ADDRESSED_REVISION",
            "preserved_file": manifest.get("file"),
            "preserved_manifest": manifest.get("manifest"),
            "preserve_original_bytes_and_timestamps": True,
            "target_manifest": {
                "archive_manifest_contract": ARCHIVE_MANIFEST_CONTRACT,
                "build_id": build_id,
                "policy_id": policy_id,
                "content_sha256": content_sha256,
                "file_sha256": file_sha256,
                "relation_counts": current_counts,
                "partition_coverage": partition_coverage,
                "source_identity": {
                    "complete": True,
                    "identified_rows": 0,
                    "journal_generations": [],
                    "primary_relation": primary_relation,
                    "primary_rows": 0,
                    "source_flow_ids": [],
                },
            },
        })

    plan: dict[str, object] = {
        "schema": PLAN_SCHEMA,
        "mode": "PLAN",
        "status": "PASS",
        "source_report": {
            "file": report_path.name,
            "sha256": actual_sha256,
            "session_date": report.get("session_date"),
        },
        "owners": {
            "archive_worker": "realtime.session_archive_worker.SessionArchiveWorker",
            "retention_owner": "retention-production-v1",
            "options_sweeper_added": False,
        },
        "reconciliation": reconciliation,
        "preflight": {
            "current_source_zero_rows": True,
            "current_gzip_zero_rows": True,
            "primary_and_companion_relations_complete": True,
            "append_only_revision_required": True,
            "malformed_divergent_or_incomplete_candidate_fails_closed": True,
        },
        "authorization": {
            "archive_execution_authorized": False,
            "reconciliation_execution_authorized": False,
            "retention_execution_authorized": False,
            "deletion_authorized": False,
        },
        **expected_zero_effects,
        "safe_to_resume_retention": False,
        "next_authorization_boundary": (
            "review and explicitly authorize one append-only reconciliation; "
            "retention remains a separate later authorization"
        ),
    }
    canonical = json.dumps(
        plan,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    plan["frozen_plan_sha256"] = hashlib.sha256(canonical).hexdigest()
    return plan


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.plan_report is not None:
        required = {
            "--expected-report-sha256": args.expected_report_sha256,
            "--expected-build-id": args.expected_build_id,
            "--expected-policy-id": args.expected_policy_id,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(
                "provider-free retention plan requires: " + ", ".join(missing)
            )
        plan = build_retention_compatibility_plan(
            args.plan_report,
            expected_report_sha256=args.expected_report_sha256,
            expected_build_id=args.expected_build_id,
            expected_policy_id=args.expected_policy_id,
        )
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0
    if not args.database or not args.confirm_database_name or args.session_date is None:
        raise ValueError(
            "read-only archive audit requires --database, "
            "--confirm-database-name and --session-date"
        )
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        raise ValueError("retention archive audit targets only stocks_realtime_v2")
    archive_root = args.archive_root.resolve()
    if not archive_root.is_dir():
        raise ValueError("retention archive root is unavailable")
    admin_dsn = _validated_dsn(
        args.admin_dsn_file,
        args.database,
        "admin_dsn",
    )
    connect = RoleConnectionFactory(
        admin_dsn,
        args.database,
        ADMIN_ROLE,
        "audit_realtime_v2_retention_archive",
    )
    repository = PostgresSessionArchiveRepository(
        connect,
        statement_timeout="30s",
    )
    selected = tuple(args.kind or KINDS)
    records = tuple(repository.list_archives(args.session_date))
    directory = archive_directory(archive_root, args.session_date)
    results = []
    for kind in selected:
        current_counts = dict(repository.count_rows(args.session_date, kind))
        manifests = []
        for path in sorted(directory.glob("*.manifest.json")):
            try:
                document = json.loads(path.read_text(encoding="ascii"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if (
                isinstance(document, dict)
                and document.get("archive_kind") == kind
            ):
                manifests.append(inspect_manifest(
                    path,
                    kind=kind,
                    session_date=args.session_date,
                    current_counts=current_counts,
                    require_retention_compatibility=True,
                ))
        ledgers = [
            {
                "uri": row.uri,
                "sha256": row.sha256,
                "row_count": row.row_count,
                "build_id": row.build_id,
                "verified_at": row.verified_at.isoformat(),
            }
            for row in records
            if row.kind == kind
        ]
        exact = [
            row for row in manifests
            if row.get("integrity_status") == "PASS"
            and row.get("current_source_matches_file_counts") is True
        ]
        results.append({
            "kind": kind,
            "current_source_counts": current_counts,
            "manifests": manifests,
            "ledger_records": ledgers,
            "exact_current_archive_count": len(exact),
            "ready": len(exact) == 1,
        })
    ready = all(row["ready"] for row in results)
    report = {
        "status": "READY" if ready else "RECONCILIATION_REQUIRED",
        "database": args.database,
        "session_date": args.session_date.isoformat(),
        "archive_root": str(archive_root),
        "kinds": results,
        "provider_requests": 0,
        "database_writes": 0,
        "archive_writes": 0,
        "archive_deletes": 0,
        "safe_to_resume_retention": ready,
        "next_action": (
            "NONE"
            if ready
            else "REVIEW_NEW_CONTENT_ADDRESSED_ARCHIVE_REVISION"
        ),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
