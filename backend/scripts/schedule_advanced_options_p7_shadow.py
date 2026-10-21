#!/usr/bin/env python3
"""Arm one exact Advanced Options P7 live shadow and expire if missed."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_shadow import (  # noqa: E402
    LIVE_AUTHORIZATION_VERSION,
    approved_authorization,
    build_live_authorization,
    provider_free_preflight,
)
from realtime.provider_socket_lease import (  # noqa: E402
    HostProviderSocketLease,
    ProviderSocketLeaseUnavailable,
    default_provider_lock_path,
)
from scripts import run_advanced_options_p7_shadow as live  # noqa: E402


UTC = timezone.utc
LIVE_AUTH_VERSION = LIVE_AUTHORIZATION_VERSION
DEFAULT_PROVIDER_LOCK_PATH = (
    default_provider_lock_path()
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--manifest", type=Path, required=True)
    value.add_argument("--approval", type=Path, required=True)
    value.add_argument("--live-authorization", type=Path, required=True)
    value.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    value.add_argument("--status-path", type=Path, required=True)
    value.add_argument("--lock-path", type=Path, required=True)
    value.add_argument(
        "--provider-lock-path",
        type=Path,
        default=DEFAULT_PROVIDER_LOCK_PATH,
    )
    return value


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("P7 scheduler timestamp must be aware")
    return parsed.astimezone(UTC)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_live_authorization(
    authorization: object,
    *,
    manifest_path: Path,
    approval_path: Path,
    plan: dict[str, object],
) -> dict[str, object]:
    if not isinstance(authorization, dict):
        raise TypeError("P7 live authorization must be a JSON object")
    authorized_at = authorization.get("authorized_at_utc")
    if not isinstance(authorized_at, str):
        raise ValueError("P7 live authorization fields are not exact")
    expected = build_live_authorization(
        plan,
        manifest_sha256=_sha256(manifest_path),
        approval_sha256=_sha256(approval_path),
        authorized_at=_timestamp(authorized_at),
    )
    if authorization != expected:
        raise ValueError("P7 live authorization scope changed")
    return authorization


def _write_status(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, path)


def _wait_until(start: datetime) -> None:
    while True:
        remaining = (start - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            return
        time.sleep(
            min(
                30.0 if remaining > 5 else 0.05,
                max(0.01, remaining),
            )
        )


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    manifest_path = args.manifest.resolve()
    approval_path = args.approval.resolve()
    live_authorization_path = args.live_authorization.resolve()
    status_path = args.status_path.resolve()
    lock_path = args.lock_path.resolve()
    provider_lock_path = args.provider_lock_path.resolve()
    plan = approved_authorization(
        manifest_path,
        approval_path,
        backend_dir=BACKEND_DIR,
    )
    authorization = _validate_live_authorization(
        json.loads(live_authorization_path.read_text(encoding="utf-8")),
        manifest_path=manifest_path,
        approval_path=approval_path,
        plan=plan,
    )
    start = _timestamp(plan["window"]["start_utc"])
    stop = _timestamp(plan["window"]["stop_utc"])
    now = datetime.now(UTC)
    if now >= start:
        _write_status(status_path, {
            "status": "EXPIRED",
            "checked_at": now,
            "manifest_id": plan["manifest_id"],
            "reason": "MISSED_ARM_WINDOW",
            "provider_requests": 0,
            "external_socket_connections": 0,
        })
        return 2
    if lock_path.exists():
        raise FileExistsError("P7 scheduler lock already exists")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps({
                "manifest_id": plan["manifest_id"],
                "live_authorization_reference": (
                    authorization["live_authorization_reference"]
                ),
                "scheduler_pid": os.getpid(),
                "armed_at": now.isoformat(),
            }, indent=2, sort_keys=True) + "\n"
        )
    provider_free_preflight(
        plan,
        backend_dir=BACKEND_DIR,
        now=now,
        environ={},
    )
    provider_lease = HostProviderSocketLease(
        provider_lock_path,
        plan["provider"]["connection_lease_resource_key"],
        UUID(str(plan["run_id"])),
    )
    try:
        provider_lease.acquire(now=now)
    except ProviderSocketLeaseUnavailable:
        _write_status(status_path, {
            "status": "BLOCKED",
            "manifest_id": plan["manifest_id"],
            "checked_at": datetime.now(UTC),
            "reason": "OPTIONS_SOCKET_LEASE_UNAVAILABLE",
            "provider_requests": 0,
            "external_socket_connections": 0,
            "database_connections": 0,
        })
        return 2
    try:
        _write_status(status_path, {
            "status": "ARMED",
            "manifest_id": plan["manifest_id"],
            "live_authorization_reference": (
                authorization["live_authorization_reference"]
            ),
            "scheduler_pid": os.getpid(),
            "armed_at": now,
            "start_utc": start,
            "stop_utc": stop,
            "provider_socket_lease": provider_lease.evidence(),
            "provider_requests": 0,
            "external_socket_connections": 0,
            "database_connections": 0,
        })
        _wait_until(start - timedelta(milliseconds=250))
        plan = approved_authorization(
            manifest_path,
            approval_path,
            backend_dir=BACKEND_DIR,
        )
        provider_free_preflight(
            plan,
            backend_dir=BACKEND_DIR,
            now=datetime.now(UTC),
            environ={},
        )
        _wait_until(start)
        _write_status(status_path, {
            "status": "RUNNING",
            "manifest_id": plan["manifest_id"],
            "started_at": datetime.now(UTC),
            "stop_utc": stop,
            "provider_socket_lease": provider_lease.evidence(),
        })
        try:
            result = live.main(
                [
                    "--manifest",
                    str(manifest_path),
                    "--approval",
                    str(approval_path),
                    "--env-file",
                    str(args.env_file.resolve()),
                    "--execute",
                ],
                provider_lease=provider_lease,
            )
        except Exception as error:
            _write_status(status_path, {
                "status": "FAILED",
                "manifest_id": plan["manifest_id"],
                "failed_at": datetime.now(UTC),
                "error": f"{type(error).__name__}:{error}",
                "provider_socket_lease": provider_lease.evidence(),
            })
            raise
        _write_status(status_path, {
            "status": "COMPLETED" if result == 0 else "FAILED",
            "manifest_id": plan["manifest_id"],
            "completed_at": datetime.now(UTC),
            "exit_code": result,
            "evidence_directory": plan["evidence"]["directory"],
            "provider_socket_lease": provider_lease.evidence(),
        })
        return result
    finally:
        provider_lease.release(now=datetime.now(UTC))


if __name__ == "__main__":
    raise SystemExit(main())
