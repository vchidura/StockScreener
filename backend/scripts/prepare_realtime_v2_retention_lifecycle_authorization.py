#!/usr/bin/env python3
"""Prepare one inert stop-only authorization for the exact retention container."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.retention_lifecycle_authorization import (  # noqa: E402
    CONTAINER_NAME,
    OBSERVATION_SCHEMA,
    SERVICE_NAME,
    build_lifecycle_authorization,
    canonical_sha256,
    validate_lifecycle_authorization,
)


UTC = timezone.utc
ARCHIVE_ROOT = "/var/lib/realtime-v2-retention/archive"
RETENTION_ROOT = "/var/lib/realtime-v2-retention"
LIVE_SOURCE_PATHS = (
    "/app/realtime/retention_maintenance.py",
    "/app/realtime/session_archive.py",
    "/app/realtime/session_archive_repository.py",
    "/app/realtime/session_archive_worker.py",
    "/app/scripts/audit_realtime_v2_retention_archive.py",
    "/app/scripts/run_realtime_v2_retention_service.py",
)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("lifecycle timestamps must be timezone-aware")
    return parsed.astimezone(UTC)


def _run(arguments: list[str], *, binary: bool = False):
    completed = subprocess.run(
        arguments,
        check=True,
        capture_output=True,
        text=not binary,
    )
    return completed.stdout


def _docker_json(docker: str, *arguments: str):
    return json.loads(_run([docker, *arguments]))


def _container_matches(docker: str) -> list[str]:
    output = _run([
        docker,
        "ps",
        "-a",
        "--filter",
        f"label=com.docker.compose.service={SERVICE_NAME}",
        "--no-trunc",
        "--format",
        "{{.ID}}",
    ])
    return [line.strip() for line in output.splitlines() if line.strip()]


def _hashes(docker: str, container: str, paths: tuple[str, ...]):
    output = _run([docker, "exec", container, "sha256sum", *paths])
    rows = {}
    for line in output.splitlines():
        digest, path = line.split(maxsplit=1)
        rows[path.strip()] = digest
    return rows


def _archive_inventory(docker: str, container: str):
    output = _run(
        [
            docker,
            "exec",
            container,
            "find",
            f"{ARCHIVE_ROOT}/2026-10-19",
            "-maxdepth",
            "1",
            "-type",
            "f",
            "-print0",
        ],
        binary=True,
    )
    paths = tuple(sorted(
        value.decode("utf-8")
        for value in output.split(b"\x00")
        if value
    ))
    digests = _hashes(docker, container, paths)
    rows = []
    for path in paths:
        stat = _run([
            docker,
            "exec",
            container,
            "stat",
            "-c",
            "%s|%Y",
            path,
        ]).strip()
        size, mtime = stat.split("|", 1)
        rows.append({
            "path": path.removeprefix(f"{ARCHIVE_ROOT}/"),
            "sha256": digests[path],
            "bytes": int(size),
            "mtime_epoch_seconds": int(mtime),
        })
    return rows


def _live_audit(docker: str, container: str):
    return json.loads(_run([
        docker,
        "exec",
        container,
        "python",
        "-B",
        "scripts/audit_realtime_v2_retention_archive.py",
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--session-date",
        "2026-10-19",
        "--archive-root",
        ARCHIVE_ROOT,
        "--admin-dsn-file",
        "/run/secrets/realtime_v2_admin_dsn",
    ]))


def _lock_or_partial_files(docker: str, container: str):
    output = _run(
        [
            docker,
            "exec",
            container,
            "find",
            RETENTION_ROOT,
            "-maxdepth",
            "3",
            "-type",
            "f",
            "-print0",
        ],
        binary=True,
    )
    paths = [
        value.decode("utf-8")
        for value in output.split(b"\x00")
        if value
    ]
    return sorted(
        path.removeprefix(f"{RETENTION_ROOT}/")
        for path in paths
        if (
            path.endswith(".partial")
            or path.endswith(".lock")
            or "c9d6025a-57e4-4c65-a792-9e21d2f06731" in path
        )
    )


def capture_observation(
    *,
    docker: str,
    checked_at: datetime,
) -> dict[str, object]:
    matches = _container_matches(docker)
    containers = []
    inventory = []
    source_counts = {}
    ledger_rows = []
    revision_evidence = []
    lock_or_partial = []
    target_files = []
    for container_id in matches:
        [raw] = _docker_json(docker, "inspect", container_id)
        [image] = _docker_json(docker, "image", "inspect", raw["Image"])
        labels = raw["Config"].get("Labels") or {}
        mounts = []
        for mount in raw.get("Mounts") or []:
            row = {
                "type": mount["Type"],
                "destination": mount["Destination"],
                "read_write": bool(mount["RW"]),
            }
            if mount["Type"] == "volume":
                row["name"] = mount["Name"]
            else:
                row["source_path_sha256"] = hashlib.sha256(
                    mount["Source"].encode("utf-8")
                ).hexdigest()
            mounts.append(row)
        command = raw["Config"].get("Cmd")
        live_hashes = _hashes(
            docker,
            container_id,
            LIVE_SOURCE_PATHS,
        )
        containers.append({
            "id": raw["Id"],
            "name": raw["Name"].lstrip("/"),
            "service": labels.get("com.docker.compose.service"),
            "image_id": raw["Image"],
            "image_created_at": image["Created"],
            "image_reference": (
                (image.get("RepoDigests") or image.get("RepoTags") or [""])[0]
            ),
            "command": command,
            "command_sha256": canonical_sha256(command),
            "entrypoint": raw["Config"].get("Entrypoint"),
            "started_at_utc": raw["State"]["StartedAt"],
            "status": raw["State"]["Status"],
            "running": bool(raw["State"]["Running"]),
            "health": (raw["State"].get("Health") or {}).get("Status"),
            "restart_policy": {
                "name": raw["HostConfig"]["RestartPolicy"]["Name"],
                "maximum_retry_count": raw[
                    "HostConfig"
                ]["RestartPolicy"]["MaximumRetryCount"],
            },
            "compose_identity": {
                "config_hash": labels.get("com.docker.compose.config-hash"),
                "container_number": labels.get(
                    "com.docker.compose.container-number"
                ),
                "image": labels.get("com.docker.compose.image"),
                "oneoff": labels.get("com.docker.compose.oneoff"),
                "project": labels.get("com.docker.compose.project"),
                "service": labels.get("com.docker.compose.service"),
                "version": labels.get("com.docker.compose.version"),
            },
            "mounts": sorted(
                mounts,
                key=lambda row: (row["destination"], row["type"]),
            ),
            "live_source_hashes": dict(sorted(live_hashes.items())),
        })
        if raw["Name"].lstrip("/") == CONTAINER_NAME:
            inventory = _archive_inventory(docker, container_id)
            audit = _live_audit(docker, container_id)
            lock_or_partial = _lock_or_partial_files(
                docker,
                container_id,
            )
            for kind_row in audit["kinds"]:
                source_counts.update(kind_row["current_source_counts"])
                ledger_rows.extend({
                    **row,
                    "kind": kind_row["kind"],
                } for row in kind_row["ledger_records"])
                expected_relations = sorted(
                    kind_row["current_source_counts"]
                )
                for manifest in kind_row["manifests"]:
                    if (
                        ".f58e881ede46a814.manifest.json"
                        not in manifest["manifest"]
                    ):
                        continue
                    data_path = (
                        f"2026-10-19/{manifest['file']}"
                    )
                    manifest_path = (
                        f"2026-10-19/{manifest['manifest']}"
                    )
                    by_path = {
                        row["path"]: row for row in inventory
                    }
                    matching_ledger = [
                        row for row in kind_row["ledger_records"]
                        if row["uri"].endswith("/" + manifest["file"])
                    ]
                    revision_evidence.append({
                        "kind": kind_row["kind"],
                        "build_id": manifest["build_id"],
                        "expected_relations": expected_relations,
                        "source_counts": manifest["current_source_counts"],
                        "manifest_relation_counts": manifest[
                            "manifest_relation_counts"
                        ],
                        "file_relation_counts": manifest[
                            "file_relation_counts"
                        ],
                        "integrity_status": manifest["integrity_status"],
                        "content_matches": (
                            manifest[
                                "current_source_matches_file_counts"
                            ] is True
                            and manifest[
                                "manifest_matches_file_counts"
                            ] is True
                        ),
                        "files": {
                            "data_present": data_path in by_path,
                            "manifest_present": manifest_path in by_path,
                            "data_path": data_path,
                            "manifest_path": manifest_path,
                            "data_sha256": by_path[data_path]["sha256"],
                            "manifest_sha256": by_path[
                                manifest_path
                            ]["sha256"],
                        },
                        "matching_ledger_rows": matching_ledger,
                    })
            target_files = sorted(
                row["path"] for row in inventory
                if "7efa97861b737880" in row["path"]
            )
    return {
        "schema": OBSERVATION_SCHEMA,
        "checked_at_utc": checked_at.astimezone(UTC).isoformat(),
        "matching_containers": sorted(
            containers,
            key=lambda row: row["id"],
        ),
        "archive_inventory": inventory,
        "source_counts": dict(sorted(source_counts.items())),
        "ledger_rows": ledger_rows,
        "revision_evidence": revision_evidence,
        "lock_or_partial_files": lock_or_partial,
        "package_target_files": target_files,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readiness-report", type=Path, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--lifecycle-nonce", required=True)
    parser.add_argument("--prepared-at", type=_timestamp, required=True)
    parser.add_argument("--window-start", type=_timestamp, required=True)
    parser.add_argument("--window-end", type=_timestamp, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--post-stop-output", type=Path, required=True)
    parser.add_argument("--docker", default="docker.exe")
    return parser.parse_args(argv)


def _write_new(path: Path, value: object):
    if path.exists():
        raise FileExistsError(f"lifecycle artifact must be new: {path}")
    if not path.parent.is_dir():
        raise FileNotFoundError(path.parent)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main(argv=None) -> int:
    args = parse_args(argv)
    output = args.output.resolve()
    post_stop_output = args.post_stop_output.resolve()
    if output == post_stop_output:
        raise ValueError("lifecycle outputs must differ")
    observation = capture_observation(
        docker=args.docker,
        checked_at=args.prepared_at,
    )
    package = build_lifecycle_authorization(
        args.readiness_report.resolve(),
        observation,
        repository_root=REPOSITORY_ROOT,
        authorization_reference=args.authorization_reference,
        lifecycle_nonce=args.lifecycle_nonce,
        prepared_at=args.prepared_at,
        window_start=args.window_start,
        window_end=args.window_end,
    )
    preflight = validate_lifecycle_authorization(
        package,
        readiness_report_path=args.readiness_report.resolve(),
        current_observation=observation,
        repository_root=REPOSITORY_ROOT,
        expected_package_sha256=package["package_sha256"],
        now=args.prepared_at,
        prior_stop_records=(),
    )
    _write_new(output, package)
    _write_new(post_stop_output, package["post_stop_contract"])
    print(json.dumps({
        "status": "STOP_ONLY_LIFECYCLE_PACKAGE_PREPARED_NOT_EXECUTED",
        "package_sha256": package["package_sha256"],
        "lifecycle_scope_sha256": package["lifecycle_scope_sha256"],
        "post_stop_contract_sha256": package[
            "post_stop_contract_sha256"
        ],
        "lifecycle_nonce": package["lifecycle_nonce"],
        "window": package["window"],
        "revision_classifications": package[
            "post_stop_contract"
        ]["expected_revision_classifications"],
        "preflight": preflight,
        "service_lifecycle_actions": 0,
        "production_writes": 0,
        "provider_requests": 0,
        "output": str(output),
        "post_stop_output": str(post_stop_output),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
