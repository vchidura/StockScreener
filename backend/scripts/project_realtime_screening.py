"""Explicit, provider-free offline materialization into the isolated realtime target."""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager, nullcontext
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extensions import parse_dsn


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access import RealtimeDatabaseRole, RealtimePoolBudget, RealtimeProcessDatabasePlan
from realtime.migrations import EXPECTED_DATABASE_NAME, plan_realtime_migrations
from realtime.screening_repository import materialize_realtime_screening
from realtime.ticker_repository import materialize_realtime_ticker_workspace
from realtime.stock_alert_repository import materialize_realtime_stock_alerts
from realtime.dashboard_repository import materialize_realtime_dashboard
from realtime.strategy_repository import materialize_due_realtime_strategies
from realtime.security import ALL_DATABASE_TABLES, build_role_names
from realtime.worker_database import RealtimeProcessDatabase
from scripts import provision_realtime_database as provisioning


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--cutoff",
        type=lambda value: datetime.now(timezone.utc) if value == "now" else datetime.fromisoformat(value),
        required=True,
    )
    generation = parser.add_mutually_exclusive_group(required=True)
    generation.add_argument("--source-generation", type=int)
    generation.add_argument("--current-generation", action="store_true")
    parser.add_argument("--surface", choices=["screening", "ticker", "alerts", "dashboard", "strategies"], default="screening")
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--confirm-database-name", choices=[EXPECTED_DATABASE_NAME])
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--temporary-runtime-login", action="store_true")
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args(argv)
    if args.cutoff.tzinfo is None or args.cutoff.utcoffset() is None:
        parser.error("A timezone-aware cutoff is required")
    if args.source_generation is not None and args.source_generation < 1:
        parser.error("Source generation must be positive")
    if args.current_generation and args.surface not in {"ticker", "strategies"}:
        parser.error("Current-generation resolution is ticker/strategies-only")
    if args.execute and args.confirm_database_name != EXPECTED_DATABASE_NAME:
        parser.error("Execution requires exact database-name confirmation")
    if args.temporary_runtime_login and (args.surface != "ticker" or not args.current_generation or args.report_path is None):
        parser.error("Temporary runtime access requires ticker/current-generation publication and a new report")
    return args


def _current_source_generation(database, *, cohort_id, session, cutoff):
    with database.transaction(
        read_only=True, dict_cursor=True, isolation_level="REPEATABLE READ",
    ) as cursor:
        cursor.execute(
            "SELECT generation.session_date, generation.manifest->>'source_generation' AS source_generation "
            "FROM rt.screening_current AS current "
            "JOIN rt.screening_generations AS generation ON generation.generation_id=current.generation_id "
            "AND generation.cohort_id=current.cohort_id "
            "WHERE current.cohort_id=%s AND generation.status='COMPLETE' AND generation.generated_at<=%s",
            (str(cohort_id), cutoff),
        )
        row = cursor.fetchone()
    if row is None or row["session_date"] != session:
        raise RuntimeError("Current screening generation does not match the portal session")
    source_generation = int(row["source_generation"])
    if source_generation < 1:
        raise RuntimeError("Current screening source generation is invalid")
    return source_generation


@contextmanager
def _temporary_runtime_login(args, report):
    settings = provisioning._admin_settings(args.env_file)
    roles = build_role_names(args.role_prefix)
    with closing(psycopg2.connect(**provisioning._connection_settings(settings, EXPECTED_DATABASE_NAME))) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='10s'")
            cursor.execute("SELECT current_database(),lineage_name FROM rt.database_identity")
            if cursor.fetchone() != (EXPECTED_DATABASE_NAME, "advanced_realtime_v1"):
                raise RuntimeError("Publication target identity mismatch")
            cursor.execute("SELECT version,checksum_sha256 FROM rt.schema_migrations ORDER BY version")
            if plan_realtime_migrations(EXPECTED_DATABASE_NAME, dict(cursor.fetchall())).pending:
                raise RuntimeError("Publication target has pending migrations")
            cursor.execute("SELECT n.nspname||'.'||c.relname FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace "
                           "WHERE n.nspname IN ('ref','rt') AND c.relkind IN ('r','p')")
            if {row[0] for row in cursor.fetchall()} != set(ALL_DATABASE_TABLES):
                raise RuntimeError("Publication target relation inventory mismatch")
            cursor.execute("SELECT rolname,rolcanlogin,rolinherit FROM pg_roles WHERE rolname=ANY(%s)",
                           (list(roles.principal_names),))
            states = {name: (login, inherit) for name, login, inherit in cursor.fetchall()}
            if (states.get(roles.api) != (True, False) or states.get(roles.runtime) != (False, False)
                    or states.get(roles.migrator, (True,))[0]):
                raise RuntimeError("Publication requires API-only principal state")
            cursor.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE usename=%s", (roles.runtime,))
            if cursor.fetchone()[0]:
                raise RuntimeError("Publication refuses concurrent runtime sessions")
        connection.rollback()
        connection.set_session(readonly=False, autocommit=True)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("ALTER ROLE {} LOGIN NOINHERIT").format(sql.Identifier(roles.runtime)))
            report["temporary_runtime_login"] = True
            yield
        finally:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(roles.runtime)))
                cursor.execute("SELECT rolname,rolcanlogin,rolinherit FROM pg_roles WHERE rolname=ANY(%s)",
                               (list(roles.principal_names),))
                states = {name: (login, inherit) for name, login, inherit in cursor.fetchall()}
            restored = (states.get(roles.api) == (True, False) and states.get(roles.runtime) == (False, False)
                        and not states.get(roles.migrator, (True,))[0])
            report["api_only_roles_restored"] = restored
            if not restored:
                raise RuntimeError("Publication principal restoration failed")


def _execute_projection(args):
    if not args.env_file.is_file():
        raise FileNotFoundError("Realtime screening environment file is missing")
    load_dotenv(args.env_file, override=True)
    dsn = os.getenv("REALTIME_RUNTIME_DSN", "").strip()
    if not dsn:
        raise RuntimeError("Realtime screening projection requires REALTIME_RUNTIME_DSN")
    if parse_dsn(dsn).get("dbname") != EXPECTED_DATABASE_NAME:
        raise ValueError("Realtime screening DSN must explicitly target stocks_realtime_v1")
    roles = build_role_names(args.role_prefix)
    principal_pool = pool.SimpleConnectionPool(1, 1, dsn=dsn)
    try:
        database = RealtimeProcessDatabase.wire(
            RealtimeProcessDatabasePlan("screening-projection:offline", RealtimeDatabaseRole.COMPUTE, RealtimePoolBudget(1, 1)),
            principal_pool=principal_pool, role_name=roles.compute, statement_timeout_ms=60_000,
        )
        publication_database = RealtimeProcessDatabase.wire(
            RealtimeProcessDatabasePlan(
                "ticker-publication:offline",
                RealtimeDatabaseRole.PUBLICATION,
                RealtimePoolBudget(1, 1),
            ),
            principal_pool=principal_pool,
            role_name=roles.publication,
            statement_timeout_ms=60_000,
        )
        if args.surface == "strategies":
            outcomes = materialize_due_realtime_strategies(database, publication_database,
                cohort_id=args.cohort_id, available_by=args.cutoff)
            return dict(strategy_outcomes=list(outcomes), history_mode="ROLLING_STORED_SESSION_OUTCOMES")
        source_generation = (
            _current_source_generation(
                database, cohort_id=args.cohort_id, session=args.session, cutoff=args.cutoff,
            )
            if args.current_generation else args.source_generation
        )
        scope = dict(
            cohort_id=args.cohort_id, session=args.session, cutoff=args.cutoff,
            source_generation=source_generation,
        )
        if args.surface == "dashboard":
            snapshot = materialize_realtime_dashboard(database, publication_database, **scope)
            print(f"STORED_DASHBOARD: session={args.session} snapshot={snapshot.snapshot_id}")
        elif args.surface == "alerts":
            snapshot = materialize_realtime_stock_alerts(database, publication_database, **scope)
            print(f"STORED_ALERTS: session={args.session} snapshot={snapshot.snapshot_id} status={snapshot.payload['shadow']['status']}")
        elif args.surface == "ticker":
            snapshots = materialize_realtime_ticker_workspace(
                database, publication_database, **scope,
            )
            print(f"STORED_TICKER: session={args.session} snapshots={len(snapshots)}")
            return dict(source_generation=source_generation, ticker_snapshots=len(snapshots),
                        snapshot_ids=[str(snapshot.snapshot_id) for snapshot in snapshots])
        else:
            generation = materialize_realtime_screening(database, **scope)
            print(f"STORED_SCREENING: generation={generation.generation_id} session={generation.session_date} rows={len(generation.rows)}")
    finally:
        principal_pool.closeall()
    return 0


def main(argv=None):
    args = parse_args(argv)
    if not args.execute:
        print(f"NOT_EXECUTED: provider-free {args.surface} projection; explicit target-write authorization required")
        return 0
    path = args.report_path.resolve() if args.report_path else None
    if path is not None and (path.exists() or not path.is_relative_to(BACKEND_DIR.parent)):
        raise ValueError("Publication report must be a new workspace-local file")
    report = dict(state="RUNNING", database=EXPECTED_DATABASE_NAME, cohort_id=str(args.cohort_id),
                  session=str(args.session), surface=args.surface, cutoff=args.cutoff.isoformat(),
                  started_at=datetime.now(timezone.utc).isoformat(), provider_requests=0, values_exposed=False)
    if path is not None:
        from scripts.realtime_script_support import write_report as _write_report
        _write_report(path, report)
    try:
        with _temporary_runtime_login(args, report) if args.temporary_runtime_login else nullcontext():
            result = _execute_projection(args)
            if isinstance(result, dict):
                report.update(result)
        report["state"] = "COMPLETE"
    except Exception as error:
        report.update(state="FAILED", error_type=type(error).__name__)
        message = str(error)
        for name in ("REALTIME_RUNTIME_DSN", "POSTGRES_ADMIN_PASSWORD", "REALTIME_API_DSN"):
            secret = os.getenv(name, "").strip()
            if secret:
                message = message.replace(secret, "[REDACTED]")
        report["error_message"] = message
        raise RuntimeError(message) from None
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        if path is not None:
            _write_report(path, report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
