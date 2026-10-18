#!/usr/bin/env python3
"""Run the provider-free V2 retention maintenance service.

PLAN is the default and performs no file or database I/O. --status reads only the
bounded status artifact. --once/--continuous require --execute, exact production
database confirmation, explicit ordinary/archive relation and archive-kind scopes,
separate app/admin DSN secret files, and an authorization reference.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Callable

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn


def _repository_root(backend_dir: Path) -> Path:
    backend_dir = backend_dir.resolve()
    checkout_root = backend_dir.parent
    if (checkout_root / "docker-compose.realtime-v2.yml").is_file():
        return checkout_root
    return backend_dir


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = _repository_root(BACKEND_DIR)
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from realtime.partition_maintenance import (  # noqa: E402
    ensure_selected_partitions_and_check,
    partition_creation_plan,
    partition_alarms,
)
from realtime.partition_retention import (  # noqa: E402
    CATALOG_RULES,
    ELIGIBLE_RELATIONS,
    PartitionRetentionExecutor,
    PostgresPartitionRetentionRepository,
)
from realtime.relation_registry import (  # noqa: E402
    ADMIN_ROLE,
    APP_ROLE,
    PARTITIONED_RELATIONS,
)
from realtime.retention_maintenance import (  # noqa: E402
    ARCHIVE_FIRST_RELATIONS,
    HEALTH_BLOCKED,
    HEALTH_DEGRADED,
    STATUS_ALARM,
    RetentionMaintenanceCycle,
    RetentionMaintenanceRequest,
    assess_retention_health,
    read_retention_status,
)
from realtime.security_v2 import build_role_names_v2  # noqa: E402
from realtime.session_archive import SESSION_KINDS, ArchiveIdentity  # noqa: E402
from realtime.session_archive_repository import PostgresSessionArchiveRepository  # noqa: E402
from realtime.session_archive_worker import SessionArchiveWorker  # noqa: E402

UTC = timezone.utc
LOCK_NAME = "realtime-v2-retention-service-v1"
DEFAULT_INTERVAL_SECONDS = 3600
DEFAULT_MAXIMUM_STATUS_AGE_SECONDS = 7200
DEFAULT_FAILURE_THRESHOLD = 2


def _now() -> datetime:
    return datetime.now(UTC)


def _outside_repository(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    resolved = path.resolve()
    if resolved == REPOSITORY_ROOT or REPOSITORY_ROOT in resolved.parents:
        raise ValueError(f"{name} must be outside the repository")
    return resolved


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--archive-root", type=Path)
    parser.add_argument("--status-path", type=Path, required=True)
    parser.add_argument("--app-dsn-file", type=Path)
    parser.add_argument("--admin-dsn-file", type=Path)
    parser.add_argument("--partition-relation", action="append", choices=PARTITIONED_RELATIONS)
    parser.add_argument("--retention-relation", action="append", choices=ELIGIBLE_RELATIONS)
    parser.add_argument("--archive-relation", action="append", choices=ARCHIVE_FIRST_RELATIONS)
    parser.add_argument("--archive-kind", action="append", choices=SESSION_KINDS)
    parser.add_argument("--archive-build-id")
    parser.add_argument("--archive-policy-id")
    parser.add_argument("--authorization-reference")
    parser.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--statement-timeout-seconds", type=int, default=300)
    parser.add_argument(
        "--maximum-status-age-seconds",
        type=int,
        default=DEFAULT_MAXIMUM_STATUS_AGE_SECONDS,
    )
    parser.add_argument("--failure-threshold", type=int, default=DEFAULT_FAILURE_THRESHOLD)
    parser.add_argument("--fail-on-degraded", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    try:
        validate_target_database_v2(
            args.database,
            args.confirm_database_name,
            security_mode=SECURITY_MODE_HARDENED,
        )
        if args.database != EXPECTED_V2_DATABASE_NAME:
            raise ValueError("the retention service targets only stocks_realtime_v2")
        args.status_path = _outside_repository(args.status_path, "status_path")
        if args.interval_seconds <= 0 or args.statement_timeout_seconds <= 0:
            raise ValueError("interval and statement timeout must be positive")
        if args.maximum_status_age_seconds <= 0 or args.failure_threshold <= 0:
            raise ValueError("health thresholds must be positive")
        if args.status:
            if args.execute:
                raise ValueError("--status cannot execute maintenance")
            return args
        if args.check and args.execute:
            raise ValueError("--check is read-only and cannot use --execute")
        required_scope = {
            "--archive-root": args.archive_root,
            "--partition-relation": args.partition_relation,
            "--retention-relation": args.retention_relation,
            "--archive-relation": args.archive_relation,
            "--archive-kind": args.archive_kind,
            "--archive-build-id": args.archive_build_id,
            "--archive-policy-id": args.archive_policy_id,
        }
        missing_scope = [name for name, value in required_scope.items() if not value]
        if missing_scope:
            raise ValueError("maintenance plan requires: " + ", ".join(missing_scope))
        args.archive_root = _outside_repository(args.archive_root, "archive_root")
        if args.status_path == args.archive_root:
            raise ValueError("status_path and archive_root must be different")
        args.partition_relation = tuple(sorted(set(args.partition_relation)))
        args.retention_relation = tuple(sorted(set(args.retention_relation)))
        args.archive_relation = tuple(sorted(set(args.archive_relation)))
        args.archive_kind = tuple(sorted(set(args.archive_kind)))
        identity = ArchiveIdentity(args.archive_build_id.strip(), args.archive_policy_id.strip())
        args.archive_build_id = identity.build_id
        args.archive_policy_id = identity.policy_id
        if args.execute or args.check:
            if not (args.once or args.continuous):
                if args.execute:
                    raise ValueError("--execute requires --once or --continuous")
            execution = {
                "--app-dsn-file": args.app_dsn_file,
                "--admin-dsn-file": args.admin_dsn_file,
                "--authorization-reference": args.authorization_reference,
            }
            missing_execution = [name for name, value in execution.items() if not value]
            if missing_execution:
                raise ValueError("execution requires: " + ", ".join(missing_execution))
            args.app_dsn_file = args.app_dsn_file.resolve()
            args.admin_dsn_file = args.admin_dsn_file.resolve()
            if args.app_dsn_file == args.admin_dsn_file:
                raise ValueError("app and admin DSNs require separate secret files")
            args.authorization_reference = args.authorization_reference.strip()
            if not 1 <= len(args.authorization_reference) <= 200:
                raise ValueError("authorization reference must be 1 to 200 characters")
        elif args.once or args.continuous:
            raise ValueError("--once/--continuous require --execute")
    except ValueError as error:
        parser.error(str(error))
    return args


def _read_secret(path: Path, name: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError(f"{name} secret file is unavailable") from error
    if not value:
        raise RuntimeError(f"{name} secret file is empty")
    return value


def _validated_dsn(path: Path, database: str, name: str) -> str:
    dsn = _read_secret(path, name)
    try:
        target = parse_dsn(dsn).get("dbname")
    except psycopg2.Error as error:
        raise RuntimeError(f"{name} secret is not a valid PostgreSQL DSN") from error
    if target != database:
        raise RuntimeError(f"{name} DSN must explicitly target {database}")
    return dsn


@dataclass(frozen=True, slots=True)
class RoleConnectionFactory:
    dsn: str
    database: str
    role: str
    application_name: str
    connector: Callable[..., object] = psycopg2.connect

    def __post_init__(self) -> None:
        build_role_names_v2(self.database).name(self.role)

    def __call__(self):
        connection = self.connector(
            self.dsn,
            application_name=self.application_name,
            options="-c timezone=UTC",
        )
        connection.autocommit = True
        expected_role = build_role_names_v2(self.database).name(self.role)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(expected_role)))
                cursor.execute(
                    "SELECT current_database()::text,current_role::text,"
                    "(SELECT phase FROM rt.security_phase_events ORDER BY event_id DESC LIMIT 1)"
                )
                database, role, phase = cursor.fetchone()
            if (database, role, phase) != (self.database, expected_role, "AUDITED"):
                raise RuntimeError(
                    "retention connection identity/security phase mismatch"
                )
            connection.autocommit = False
            return connection
        except Exception:
            connection.close()
            raise


class AppPartitionMaintenance:
    def __init__(
        self,
        connect: Callable[[], object],
        relations: tuple[str, ...],
    ) -> None:
        self._connect = connect
        self._relations = relations

    def __call__(self, today: date):
        with closing(self._connect()) as connection:
            try:
                with connection.cursor() as cursor:
                    result = ensure_selected_partitions_and_check(
                        cursor,
                        today,
                        self._relations,
                    )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise


@contextmanager
def retention_leadership(connect: Callable[[], object]):
    with closing(connect()) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0))",
                (LOCK_NAME,),
            )
            if not cursor.fetchone()[0]:
                raise RuntimeError("another retention service owns leadership")
        try:
            yield
        finally:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                    (LOCK_NAME,),
                )


def build_request(
    args: argparse.Namespace,
    checked_at: datetime,
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> RetentionMaintenanceRequest:
    session = (calendar or OptionExchangeCalendar()).latest_completed_session(checked_at)
    return RetentionMaintenanceRequest(
        as_of_date=checked_at.astimezone(UTC).date(),
        as_of_session=session,
        archive_session_dates=(session,),
        partition_relations=args.partition_relation,
        retention_relations=args.retention_relation,
        archive_relations=args.archive_relation,
        archive_kinds=args.archive_kind,
    )


def _plan(args: argparse.Namespace, checked_at: datetime) -> dict[str, object]:
    request = build_request(args, checked_at)
    return {
        "mode": "PLAN",
        "database": args.database,
        "security_mode": SECURITY_MODE_HARDENED,
        "request": request.as_dict(),
        "archive_root": str(args.archive_root),
        "status_path": str(args.status_path),
        "archive_build_id": args.archive_build_id,
        "archive_policy_id": args.archive_policy_id,
        "interval_seconds": args.interval_seconds,
        "leadership_lock": LOCK_NAME,
        "requires_app_role": True,
        "requires_admin_role": True,
        "provider_requests": 0,
        "database_changes": False,
    }


def _status(args: argparse.Namespace, checked_at: datetime) -> tuple[dict[str, object], int]:
    status = read_retention_status(args.status_path)
    health = assess_retention_health(
        status,
        checked_at=checked_at,
        maximum_age_seconds=args.maximum_status_age_seconds,
        failure_threshold=args.failure_threshold,
    )
    payload = {
        "component": "retention",
        **health.as_dict(),
        "provider_requests": 0,
        "database_access": False,
    }
    failed = health.state == HEALTH_BLOCKED or (
        health.state == HEALTH_DEGRADED and args.fail_on_degraded
    )
    return payload, 1 if failed else 0


def _read_partition_state(
    connect: Callable[[], object],
    today: date,
    relations: tuple[str, ...],
):
    with closing(connect()) as connection:
        connection.set_session(readonly=True, autocommit=False)
        try:
            with connection.cursor() as cursor:
                creation = partition_creation_plan(cursor, today, relations)
                alarms = tuple(partition_alarms(cursor, today))
                return creation, alarms
        finally:
            connection.rollback()


def run_preflight(
    args: argparse.Namespace,
    app_connect: RoleConnectionFactory,
    admin_connect: RoleConnectionFactory,
    *,
    checked_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
    retention: PartitionRetentionExecutor | None = None,
    archive: SessionArchiveWorker | None = None,
    impact_repository: PostgresPartitionRetentionRepository | None = None,
) -> tuple[dict[str, object], int]:
    request = build_request(args, checked_at, calendar=calendar)
    timeout = f"{args.statement_timeout_seconds}s"
    repository = impact_repository or PostgresPartitionRetentionRepository(
        admin_connect,
        statement_timeout=timeout,
    )
    ordinary = retention or PartitionRetentionExecutor(
        repository,
        clock=lambda: checked_at,
    )
    archive_worker = archive or SessionArchiveWorker(
        PostgresSessionArchiveRepository(
            admin_connect,
            statement_timeout=timeout,
        ),
        args.archive_root,
        ArchiveIdentity(args.archive_build_id, args.archive_policy_id),
        kinds=args.archive_kind,
        clock=lambda: checked_at,
    )
    with closing(app_connect()):
        pass
    with closing(admin_connect()):
        pass
    with retention_leadership(admin_connect):
        creation_plan, health_alarms = _read_partition_state(
            app_connect,
            checked_at.astimezone(UTC).date(),
            request.partition_relations,
        )
        retention_report = ordinary.run(
            execute=False,
            as_of_date=request.as_of_date,
            relations=request.retention_relations,
        )
        archive_report = archive_worker.plan_archived_partitions(
            as_of_session=request.as_of_session,
            relations=request.archive_relations,
            health_date=checked_at.astimezone(UTC).date(),
        )
        session_archive_report = archive_worker.plan_session_archive(
            request.as_of_session,
            request.archive_kinds,
        )
        partition_impacts = tuple(
            repository.partition_impact(partition)
            for partition in retention_report.would_drop
        )
        catalog_impacts = tuple(
            repository.catalog_prune_impact(
                catalog.catalog,
                CATALOG_RULES[catalog.catalog],
                datetime.combine(catalog.before, time(0), tzinfo=UTC),
            )
            for catalog in retention_report.catalogs
        )
    ok = (
        not health_alarms
        and retention_report.ok
        and archive_report.ok
        and session_archive_report.ok
    )
    payload = {
        "mode": "CHECK",
        "checked_at": checked_at.astimezone(UTC).isoformat(),
        "database": args.database,
        "authorization_reference": args.authorization_reference,
        "request": request.as_dict(),
        "app_role_verified": True,
        "admin_role_verified": True,
        "security_phase": "AUDITED",
        "leadership_available": True,
        "partition_health_alarms": [
            {
                "relation": alarm.relation_name,
                "horizon_date": alarm.horizon_date.isoformat() if alarm.horizon_date else None,
                "days_ahead": alarm.days_ahead,
            }
            for alarm in health_alarms
        ],
        "partition_creation": creation_plan.as_dict(),
        "ordinary_retention": retention_report.as_dict(),
        "ordinary_impact": {
            "partitions": [impact.as_dict() for impact in partition_impacts],
            "catalogs": [impact.as_dict() for impact in catalog_impacts],
        },
        "archive_retention": archive_report.as_dict(),
        "session_archive": session_archive_report.as_dict(),
        "ok": ok,
        "database_changes": False,
        "archive_writes": 0,
        "provider_requests": 0,
    }
    return payload, 0 if ok else 1


def run_cycles(
    cycle: RetentionMaintenanceCycle,
    args: argparse.Namespace,
    *,
    stop: threading.Event,
    clock: Callable[[], datetime] = _now,
    calendar: OptionExchangeCalendar | None = None,
    emit: Callable[[str], None] = print,
) -> int:
    exit_code = 0
    while not stop.is_set():
        status = cycle.run(build_request(args, clock(), calendar=calendar), execute=True)
        emit(json.dumps(status.as_dict(), sort_keys=True))
        if status.state == STATUS_ALARM:
            exit_code = 1
        if args.once or stop.wait(args.interval_seconds):
            return exit_code
    return exit_code


def _build_cycle(
    args: argparse.Namespace,
    app_connect: RoleConnectionFactory,
    admin_connect: RoleConnectionFactory,
    *,
    clock: Callable[[], datetime] = _now,
) -> RetentionMaintenanceCycle:
    timeout = f"{args.statement_timeout_seconds}s"
    archive_repository = PostgresSessionArchiveRepository(
        admin_connect,
        statement_timeout=timeout,
    )
    return RetentionMaintenanceCycle(
        AppPartitionMaintenance(app_connect, args.partition_relation),
        PartitionRetentionExecutor(
            PostgresPartitionRetentionRepository(
                admin_connect,
                statement_timeout=timeout,
            ),
            clock=clock,
        ),
        SessionArchiveWorker(
            archive_repository,
            args.archive_root,
            ArchiveIdentity(args.archive_build_id, args.archive_policy_id),
            kinds=args.archive_kind,
            clock=clock,
        ),
        args.status_path,
        clock=clock,
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    checked_at = _now()
    if args.status:
        payload, code = _status(args, checked_at)
        print(json.dumps(payload, sort_keys=True))
        return code
    if not args.execute and not args.check:
        print(json.dumps(_plan(args, checked_at), indent=2, sort_keys=True))
        print("No database or file changes made. Add --once/--continuous --execute.")
        return 0

    app_dsn = _validated_dsn(args.app_dsn_file, args.database, "app_dsn")
    admin_dsn = _validated_dsn(args.admin_dsn_file, args.database, "admin_dsn")
    app_connect = RoleConnectionFactory(
        app_dsn,
        args.database,
        APP_ROLE,
        "run_realtime_v2_retention_app",
    )
    admin_connect = RoleConnectionFactory(
        admin_dsn,
        args.database,
        ADMIN_ROLE,
        "run_realtime_v2_retention_admin",
    )
    if args.check:
        payload, code = run_preflight(
            args,
            app_connect,
            admin_connect,
            checked_at=checked_at,
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return code
    with closing(app_connect()):
        pass
    with closing(admin_connect()):
        pass
    cycle = _build_cycle(args, app_connect, admin_connect)
    stop = threading.Event()
    if args.continuous:
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
    with retention_leadership(admin_connect):
        return run_cycles(cycle, args, stop=stop)


if __name__ == "__main__":
    raise SystemExit(main())
