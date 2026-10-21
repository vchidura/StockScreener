#!/usr/bin/env python3
"""Plan, inspect or execute only the frozen O-WP5 append-only reconciliation."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from realtime.relation_registry import ADMIN_ROLE  # noqa: E402
from realtime.retention_reconciliation_authorization import (  # noqa: E402
    validate_reconciliation_authorization,
)
from realtime.retention_reconciliation_executor import (  # noqa: E402
    EXPECTED_PACKAGE_SHA256,
    PostgresReconciliationRepository,
    ReconciliationExecutor,
    ReconciliationPostflightRunner,
    ExecutionLock,
    _revision_rows,
    load_exact_package,
)
from scripts.audit_realtime_v2_retention_archive import (  # noqa: E402
    build_retention_compatibility_plan,
)
from scripts.run_realtime_v2_retention_service import (  # noqa: E402
    RoleConnectionFactory,
    _validated_dsn,
)


UTC = timezone.utc
REPORT_SHA256 = (
    "387d7a09c3073a294133162449876bda49a193c6e83f7699857bb1d135097cad"
)
PLAN_SHA256 = (
    "7605568c00244513ff25ed7a360e2d6e1031088748298b6225a34fc9fb75ec20"
)
BUILD_ID = "retention-production-v1"
POLICY_ID = "retention-policy-v1"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--postflight-contract", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--lock-path", type=Path, required=True)
    parser.add_argument("--service-state-file", type=Path, required=True)
    parser.add_argument("--database")
    parser.add_argument("--confirm-database-name")
    parser.add_argument("--admin-dsn-file", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--postflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def _service_probe(path: Path):
    resolved = path.resolve()

    def probe() -> dict[str, object]:
        value = json.loads(resolved.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("service state file must contain a JSON object")
        return value

    return probe


def _connection(args: argparse.Namespace):
    required = {
        "--database": args.database,
        "--confirm-database-name": args.confirm_database_name,
        "--admin-dsn-file": args.admin_dsn_file,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError("database mode requires: " + ", ".join(missing))
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        raise ValueError("reconciliation execution targets only stocks_realtime_v2")
    dsn = _validated_dsn(
        args.admin_dsn_file,
        args.database,
        "admin_dsn",
    )
    return RoleConnectionFactory(
        dsn,
        args.database,
        ADMIN_ROLE,
        "run_realtime_v2_retention_reconciliation",
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    package_path = args.package.resolve()
    postflight_path = args.postflight_contract.resolve()
    report_path = args.report.resolve()
    package = load_exact_package(package_path, postflight_path)
    plan = build_retention_compatibility_plan(
        report_path,
        expected_report_sha256=REPORT_SHA256,
        expected_build_id=BUILD_ID,
        expected_policy_id=POLICY_ID,
    )
    if plan["frozen_plan_sha256"] != PLAN_SHA256:
        raise ValueError("frozen retention PLAN identity changed")
    prepared_at = datetime.fromisoformat(
        str(package["prepared_at_utc"])
    ).astimezone(UTC)
    validate_reconciliation_authorization(
        package,
        current_plan=plan,
        report_path=report_path,
        repository_root=REPOSITORY_ROOT,
        expected_package_sha256=EXPECTED_PACKAGE_SHA256,
        now=prepared_at,
        prior_execution_records=(),
    )
    archive_root = args.archive_root.resolve()
    lock_path = args.lock_path.resolve()
    service_probe = _service_probe(args.service_state_file)
    if not any((
        args.preflight,
        args.status,
        args.postflight,
        args.execute,
    )):
        args.plan = True
    if args.plan:
        if not archive_root.is_dir():
            raise ValueError("archive_root must be an existing directory")
        executor = ReconciliationExecutor(
            None,
            repository_root=REPOSITORY_ROOT,
            archive_root=archive_root,
            report_path=report_path,
            lock_path=lock_path,
            service_probe=service_probe,
        )
        print(json.dumps(executor.plan(package), indent=2, sort_keys=True))
        return 0

    connect = _connection(args)
    repository = PostgresReconciliationRepository(connect)
    executor = ReconciliationExecutor(
        repository,
        repository_root=REPOSITORY_ROOT,
        archive_root=archive_root,
        report_path=report_path,
        lock_path=lock_path,
        service_probe=service_probe,
    )
    now = datetime.now(UTC)
    if args.status:
        payload = {
            "mode": "STATUS",
            **executor.status(package, checked_at=now).as_dict(),
            "database_writes": 0,
            "archive_writes": 0,
            "archive_deletes": 0,
            "retention_started": False,
        }
    elif args.preflight:
        payload = executor.preflight(package, plan, checked_at=now)
    elif args.postflight:
        lock = ExecutionLock(lock_path, package)
        rows = _revision_rows(package, archive_root, now)
        status = executor.status(package, checked_at=now)
        if not status.database.ledger_rows or len(rows) != 2:
            raise ValueError("postflight requires the exact committed receipt")
        payload = ReconciliationPostflightRunner(
            repository,
            archive_root,
            lock,
            service_probe,
        ).run(package, observed_at=now)
    elif args.execute:
        payload = executor.execute(package, plan, checked_at=now)
    else:
        raise AssertionError("unreachable reconciliation mode")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
