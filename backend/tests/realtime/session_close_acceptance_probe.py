"""Fresh-process, provider-free audit measurement; disposable databases only."""
from __future__ import annotations

from time import perf_counter, process_time

STARTED = perf_counter()
CPU_STARTED = process_time()

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

METRICS_MARKER = "CLOSE_ACCEPTANCE_METRICS "


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--known-by", type=datetime.fromisoformat, required=True)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expect-truncated-refusal", action="store_true")
    args = parser.parse_args(argv)
    if (
        not args.database.startswith("test_realtime_v2_")
        or args.confirm_database_name != args.database
    ):
        parser.error("acceptance measurements require a confirmed disposable database")
    expected_prefix = "test_rt_" + args.database[len("test_realtime_v2_"):]
    if args.role_prefix != expected_prefix:
        parser.error("role prefix must match the disposable database")
    if args.known_by.tzinfo is None or args.known_by.utcoffset() is None:
        parser.error("--known-by must be timezone-aware")
    if args.persist != args.execute:
        parser.error("persistence requires --persist and --execute together")
    if args.expect_truncated_refusal and not args.persist:
        parser.error("expected persistence refusal requires --persist")
    return args


def main(argv=None):
    args = parse_args(argv)
    import psutil
    from psycopg2.extensions import cursor as PgCursor
    from psycopg2.pool import SimpleConnectionPool

    from realtime.database_access_v2 import (
        API_ROLE, APP_ROLE, V2AccessConfig, V2ConnectionProvider,
    )
    from realtime.session_close_coverage import (
        audit_session_close_cursor, persist_session_close_coverage_cursor,
    )
    from scripts.validate_realtime_v2_schema import _settings

    sql_calls = []

    class MeasuredCursor(PgCursor):
        def execute(self, query, parameters=None):
            started = perf_counter()
            try:
                return super().execute(query, parameters)
            finally:
                text = query.decode("utf-8") if isinstance(query, bytes) else str(query)
                sql_calls.append((
                    "historical" if "JOIN rt.bar_revisions AS revision" in text else "other",
                    perf_counter() - started,
                ))

    process = psutil.Process()
    rss_before = process.memory_info().rss
    settings = _settings()
    role = APP_ROLE if args.persist else API_ROLE
    pool = SimpleConnectionPool(
        1, 1, dbname=args.database, user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], options="-c timezone=UTC",
        cursor_factory=MeasuredCursor,
    )
    persistence = None
    persistence_seconds = 0.0
    outcome = "MEASURED"
    refusal = None
    try:
        provider = V2ConnectionProvider(
            {role: pool},
            V2AccessConfig(
                security_mode="hardened", role_prefix=args.role_prefix,
                allow_disposable=True, statement_timeout_ms=5_000,
            ),
        )
        assert provider.verify() == args.database
        with provider.transaction(
            role, read_only=True, isolation_level="REPEATABLE READ"
        ) as unit:
            coverage = audit_session_close_cursor(
                unit.cursor, cohort_id=args.cohort_id,
                provider_tier="MASSIVE_DEVELOPER", session_date=args.session_date,
                known_by=args.known_by,
            )
        if args.persist:
            started = perf_counter()
            try:
                with provider.transaction(APP_ROLE) as unit:
                    persistence = persist_session_close_coverage_cursor(
                        unit.cursor, coverage
                    )
            except ValueError as exc:
                if (
                    not args.expect_truncated_refusal
                    or str(exc) != "truncated close coverage cannot be persisted"
                ):
                    raise
                outcome, refusal = "REFUSED_TRUNCATED_EVIDENCE", str(exc)
            finally:
                persistence_seconds = perf_counter() - started
            if args.expect_truncated_refusal and refusal is None:
                raise AssertionError("truncated evidence unexpectedly persisted")
    finally:
        pool.closeall()
    report = {
        **coverage.as_dict(),
        "outcome": outcome,
        "refusal": refusal,
        "audit_id": str(persistence.audit_id) if persistence else None,
        "current_audit_id": str(persistence.current_audit_id) if persistence else None,
        "current_status": persistence.current_status if persistence else None,
        "pointer_advanced": persistence.pointer_advanced if persistence else False,
        "persist_requested": args.persist,
        "database_writes": None,
        "database_write_accounting": "parent compares retained table rows",
    }
    report_json = json.dumps(report, sort_keys=True)
    print(report_json, flush=True)
    memory = process.memory_info()
    metrics = {
        "pre_exit_seconds": perf_counter() - STARTED,
        "process_cpu_seconds": process_time() - CPU_STARTED,
        "coverage_seconds": coverage.query_seconds,
        "persistence_seconds": persistence_seconds,
        "rss_before_connection_bytes": rss_before,
        "rss_after_report_bytes": memory.rss,
        "peak_working_set_bytes": memory.peak_wset,
        "report_bytes": len(report_json.encode("utf-8")),
        "sql_calls": len(sql_calls),
        "sql_execute_seconds": sum(seconds for _, seconds in sql_calls),
        "historical_queries": sum(kind == "historical" for kind, _ in sql_calls),
        "hardened_role": role,
        "login_model": "admin connection with transaction-local NOLOGIN role",
    }
    print(METRICS_MARKER + json.dumps(metrics, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
