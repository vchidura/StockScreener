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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
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


def main(argv=None) -> int:
    args = parse_args(argv)
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
