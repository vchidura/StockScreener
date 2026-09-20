#!/usr/bin/env python3
"""Read-only public PostgreSQL readiness report for stock behavior migration 044."""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from equity.behavior import StockBehaviorSnapshot
from equity.behavior_sources import snapshot_uses_current_source_policies
from equity.domain import DecisionWatermark
from equity.repositories import EquityEvidenceRepository

MIGRATION_VERSION = "044_equity_stock_behavior_contract"
ADJUSTED_SCHEMA_MIGRATION_VERSION = "045_stock_behavior_adjusted_schema_identifier"
ADJUSTED_EVIDENCE_SCHEMA = "stock_behavior_adjusted_1d_v1"
TABLES = (
    "equity_context_snapshots",
    "equity_context_evidence",
    "equity_evidence",
)
BEHAVIOR_COLUMNS = (
    "context_kind",
    "behavior_schema_version",
    "behavior_definition_sha256",
    "behavior_computed_at",
    "behavior_payload_text",
    "behavior_payload_sha256",
)
EXPECTED_CONSTRAINT = "ck_equity_context_behavior_contract"
EXPECTED_INDEX = "idx_equity_behavior_asof"
EXPECTED_TRIGGERS = (
    "trg_guard_equity_behavior_context",
    "trg_guard_equity_behavior_evidence_link",
    "trg_guard_equity_behavior_source_evidence",
    "trg_guard_equity_behavior_link_truncate",
    "trg_guard_equity_behavior_source_truncate",
    "trg_guard_equity_behavior_truncate",
)
EXPECTED_FUNCTIONS = (
    "guard_equity_behavior_context",
    "guard_equity_behavior_evidence_link",
    "guard_equity_behavior_source_evidence",
    "guard_equity_behavior_evidence_truncate",
    "guard_equity_behavior_truncate",
)


def _settings() -> dict[str, str]:
    names = (
        "DB_NAME", "DB_USER", "DB_HOST", "DB_PORT",
        "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD",
    )
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("migration readiness review requires: " + ", ".join(missing))
    return settings


def _fetch_all(cursor, statement: str, parameters=()) -> list[dict]:
    cursor.execute(statement, parameters)
    return [dict(row) for row in cursor.fetchall()]


def _relation_counts(cursor) -> list[dict]:
    counts = []
    for table in TABLES:
        cursor.execute(sql.SQL("SELECT COUNT(*) AS exact_rows FROM public.{}").format(
            sql.Identifier(table)
        ))
        counts.append({"table": table, "exact_rows": int(cursor.fetchone()["exact_rows"])})
    return counts


def review() -> dict:
    settings = _settings()
    with closing(psycopg2.connect(
        dbname=settings["DB_NAME"], user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], options="-c timezone=UTC",
        application_name="stock_behavior_migration_readiness",
        cursor_factory=RealDictCursor,
    )) as connection:
        connection.set_session(
            readonly=True, autocommit=False, isolation_level="REPEATABLE READ",
        )
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute(
                "SELECT current_database() AS database, current_setting('server_version') AS server_version, "
                "pg_is_in_recovery() AS in_recovery, "
                "current_setting('transaction_read_only')::boolean AS read_only, "
                "clock_timestamp() AS reviewed_at"
            )
            server = dict(cursor.fetchone())
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version = %s) AS registered",
                (MIGRATION_VERSION,),
            )
            migration = {"version": MIGRATION_VERSION, "registered": bool(cursor.fetchone()["registered"])}
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version = %s) AS registered",
                (ADJUSTED_SCHEMA_MIGRATION_VERSION,),
            )
            adjusted_schema_migration = {
                "version": ADJUSTED_SCHEMA_MIGRATION_VERSION,
                "registered": bool(cursor.fetchone()["registered"]),
            }
            columns = _fetch_all(cursor, """
                SELECT attribute.attname AS column_name,
                       format_type(attribute.atttypid, attribute.atttypmod) AS data_type,
                       attribute.attnotnull AS not_null,
                       attribute.atthasmissing AS has_missing_value,
                       attribute.attmissingval::text AS missing_value,
                       pg_get_expr(default_value.adbin, default_value.adrelid) AS default_expression
                FROM pg_attribute AS attribute
                LEFT JOIN pg_attrdef AS default_value
                  ON default_value.adrelid = attribute.attrelid
                 AND default_value.adnum = attribute.attnum
                WHERE attribute.attrelid = 'public.equity_context_snapshots'::regclass
                  AND attribute.attname = ANY(%s::text[]) AND NOT attribute.attisdropped
                ORDER BY attribute.attnum
            """, (list(BEHAVIOR_COLUMNS),))
            relations = _fetch_all(cursor, """
                SELECT relation.relname AS table,
                       pg_total_relation_size(relation.oid) AS total_bytes,
                       pg_size_pretty(pg_total_relation_size(relation.oid)) AS total_size,
                       relation.reltuples::bigint AS estimated_rows,
                       statistics.n_live_tup::bigint AS estimated_live_rows,
                       statistics.n_dead_tup::bigint AS estimated_dead_rows,
                       statistics.last_analyze, statistics.last_autoanalyze
                FROM pg_class AS relation
                JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                LEFT JOIN pg_stat_user_tables AS statistics ON statistics.relid = relation.oid
                WHERE namespace.nspname = 'public' AND relation.relname = ANY(%s::text[])
                ORDER BY relation.relname
            """, (list(TABLES),))
            exact_counts = _relation_counts(cursor)
            constraints = _fetch_all(cursor, """
                SELECT constraint_name.conname AS name,
                       constraint_name.convalidated AS validated,
                       pg_get_constraintdef(constraint_name.oid) AS definition
                FROM pg_constraint AS constraint_name
                WHERE constraint_name.conrelid = 'public.equity_context_snapshots'::regclass
                  AND constraint_name.conname = %s
            """, (EXPECTED_CONSTRAINT,))
            indexes = _fetch_all(cursor, """
                SELECT indexes.indexname AS name, indexes.indexdef AS definition,
                       catalog.indisvalid AS valid, catalog.indisready AS ready
                FROM pg_indexes AS indexes
                JOIN pg_class AS relation ON relation.relname = indexes.indexname
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                 AND namespace.nspname = indexes.schemaname
                JOIN pg_index AS catalog ON catalog.indexrelid = relation.oid
                WHERE indexes.schemaname = 'public' AND indexes.indexname = %s
            """, (EXPECTED_INDEX,))
            triggers = _fetch_all(cursor, """
                SELECT trigger_name.tgname AS name, relation.relname AS table,
                       trigger_name.tgenabled AS enabled
                FROM pg_trigger AS trigger_name
                JOIN pg_class AS relation ON relation.oid = trigger_name.tgrelid
                JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'public' AND NOT trigger_name.tgisinternal
                  AND trigger_name.tgname = ANY(%s::text[])
                ORDER BY trigger_name.tgname
            """, (list(EXPECTED_TRIGGERS),))
            functions = _fetch_all(cursor, """
                SELECT procedure.proname AS name,
                       pg_get_function_identity_arguments(procedure.oid) AS arguments
                FROM pg_proc AS procedure
                JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
                WHERE namespace.nspname = 'public' AND procedure.proname = ANY(%s::text[])
                ORDER BY procedure.proname
            """, (list(EXPECTED_FUNCTIONS),))
            cursor.execute("""
                SELECT COALESCE(
                    position(%s in pg_get_functiondef(procedure.oid)) > 0,
                    FALSE
                ) AS matches
                FROM pg_proc AS procedure
                JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
                WHERE namespace.nspname = 'public'
                  AND procedure.proname = 'guard_equity_behavior_evidence_link'
            """, (ADJUSTED_EVIDENCE_SCHEMA,))
            link_function_row = cursor.fetchone()
            link_function_matches_contract = bool(
                link_function_row and link_function_row["matches"]
            )
            locks = _fetch_all(cursor, """
                SELECT activity.pid, activity.usename, activity.application_name, activity.state,
                       activity.wait_event_type, activity.wait_event,
                       activity.xact_start, activity.query_start,
                       relation.relname AS table, lock.mode, lock.granted
                FROM pg_locks AS lock
                JOIN pg_class AS relation ON relation.oid = lock.relation
                JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                JOIN pg_stat_activity AS activity ON activity.pid = lock.pid
                WHERE namespace.nspname = 'public' AND relation.relname = ANY(%s::text[])
                  AND activity.pid <> pg_backend_pid()
                ORDER BY activity.xact_start NULLS LAST, activity.pid, relation.relname, lock.mode
            """, (list(TABLES),))
            runtime_grants = _fetch_all(cursor, """
                SELECT table_name,
                       has_table_privilege(%s, format('public.%%I', table_name), 'SELECT') AS can_select,
                       has_table_privilege(%s, format('public.%%I', table_name), 'INSERT') AS can_insert,
                       has_table_privilege(%s, format('public.%%I', table_name), 'UPDATE') AS can_update,
                       has_table_privilege(%s, format('public.%%I', table_name), 'DELETE') AS can_delete
                FROM unnest(%s::text[]) AS tables(table_name)
                ORDER BY table_name
            """, (
                settings["DB_USER"], settings["DB_USER"], settings["DB_USER"],
                settings["DB_USER"], list(TABLES),
            ))
            cursor.execute("SELECT encode(sha256(convert_to('{}', 'UTF8')), 'hex') AS digest")
            sha256_available = len(cursor.fetchone()["digest"]) == 64
            found_columns = {row["column_name"] for row in columns}
            if found_columns == set(BEHAVIOR_COLUMNS):
                cursor.execute("""
                    SELECT COUNT(*) FILTER (WHERE context_kind = 'LEGACY') AS legacy_rows,
                           COUNT(*) FILTER (WHERE context_kind = 'STOCK_BEHAVIOR') AS behavior_rows,
                           COUNT(*) FILTER (
                               WHERE context_kind NOT IN ('LEGACY', 'STOCK_BEHAVIOR')
                           ) AS unexpected_kind_rows,
                           COUNT(*) FILTER (
                               WHERE context_kind = 'LEGACY' AND (
                                   behavior_schema_version IS NOT NULL
                                   OR behavior_definition_sha256 IS NOT NULL
                                   OR behavior_computed_at IS NOT NULL
                                   OR behavior_payload_text IS NOT NULL
                                   OR behavior_payload_sha256 IS NOT NULL
                               )
                           ) AS legacy_rows_with_behavior_fields
                    FROM public.equity_context_snapshots
                """)
                context_population = {
                    key: int(value) for key, value in dict(cursor.fetchone()).items()
                }
                cursor.execute("""
                    SELECT source_name, COUNT(*) AS rows
                    FROM public.equity_evidence
                    WHERE source_name = ANY(%s::text[])
                    GROUP BY source_name ORDER BY source_name
                """, ([
                    "STOCK_BEHAVIOR_ADJUSTED_DAILY", "STOCK_BEHAVIOR_RAW_SOURCE",
                ],))
                behavior_source_evidence = [
                    {"source_name": row["source_name"], "rows": int(row["rows"])}
                    for row in cursor.fetchall()
                ]
                if context_population["behavior_rows"]:
                    cursor.execute("""
                        SELECT COUNT(*) AS rows
                        FROM public.equity_context_evidence AS link
                        JOIN public.equity_context_snapshots AS context
                          USING (equity_context_snapshot_id)
                        WHERE context.context_kind = 'STOCK_BEHAVIOR'
                    """)
                    behavior_links = int(cursor.fetchone()["rows"])
                    cursor.execute("""
                           SELECT context.ticker, context.security_id,
                               context.equity_context_snapshot_id,
                               context.market_time, context.observed_at, context.valid_until,
                               context.created_at, context.behavior_definition_sha256,
                               context.context_policy_sha256,
                               COUNT(link.evidence_id) AS link_count,
                               COUNT(*) FILTER (
                                   WHERE evidence.source_name = 'STOCK_BEHAVIOR_ADJUSTED_DAILY'
                               ) AS adjusted_links,
                               COUNT(*) FILTER (
                                   WHERE evidence.source_name = 'STOCK_BEHAVIOR_RAW_SOURCE'
                               ) AS raw_links,
                               ARRAY_AGG(evidence.interval ORDER BY evidence.interval, evidence.evidence_id)
                                   AS linked_intervals,
                               context.behavior_payload_text
                        FROM public.equity_context_snapshots AS context
                        JOIN public.equity_context_evidence AS link
                          USING (equity_context_snapshot_id)
                        JOIN public.equity_evidence AS evidence USING (evidence_id)
                        WHERE context.context_kind = 'STOCK_BEHAVIOR'
                        GROUP BY context.equity_context_snapshot_id
                        ORDER BY context.ticker, context.market_time DESC, context.observed_at DESC
                    """)
                    behavior_context_details = []
                    latest_context_by_ticker = {}
                    for row in cursor.fetchall():
                        payload = json.loads(row.pop("behavior_payload_text"))
                        snapshot = StockBehaviorSnapshot.model_validate(payload)
                        components = payload.get("components") or []
                        row["link_count"] = int(row["link_count"])
                        row["adjusted_links"] = int(row["adjusted_links"])
                        row["raw_links"] = int(row["raw_links"])
                        row["relative_strength_status"] = next((
                            component.get("status") for component in components
                            if component.get("factor") == "RELATIVE_STRENGTH"
                        ), "NOT_PRESENT")
                        row["payload_source_count"] = len({
                            source["evidence_id"]
                            for component in components
                            for source in component.get("sources") or []
                        })
                        row["current_source_policy"] = snapshot_uses_current_source_policies(
                            snapshot
                        )
                        detail = dict(row)
                        behavior_context_details.append(detail)
                        latest_context_by_ticker.setdefault(row["ticker"], detail)
                else:
                    behavior_links = 0
                    behavior_context_details = []
                    latest_context_by_ticker = {}
            else:
                context_population = None
                behavior_source_evidence = None
                behavior_links = None
                behavior_context_details = None
                latest_context_by_ticker = None
        connection.rollback()

    found_triggers = {row["name"] for row in triggers}
    found_functions = {row["name"] for row in functions}
    present_objects = bool(columns or constraints or indexes or triggers or functions)
    blockers = []
    if migration["registered"] and found_columns != set(BEHAVIOR_COLUMNS):
        blockers.append("MIGRATION_REGISTERED_WITH_INCOMPLETE_COLUMNS")
    if migration["registered"] and len(constraints) != 1:
        blockers.append("MIGRATION_REGISTERED_WITHOUT_BEHAVIOR_CONSTRAINT")
    if migration["registered"] and found_triggers != set(EXPECTED_TRIGGERS):
        blockers.append("MIGRATION_REGISTERED_WITH_INCOMPLETE_TRIGGERS")
    if migration["registered"] and found_functions != set(EXPECTED_FUNCTIONS):
        blockers.append("MIGRATION_REGISTERED_WITH_INCOMPLETE_FUNCTIONS")
    if migration["registered"] and not link_function_matches_contract:
        blockers.append("BEHAVIOR_LINK_FUNCTION_CONTRACT_MISMATCH")
    if adjusted_schema_migration["registered"] and not link_function_matches_contract:
        blockers.append("ADJUSTED_SCHEMA_MIGRATION_CONTRACT_MISMATCH")
    if not migration["registered"] and present_objects:
        blockers.append("UNREGISTERED_PARTIAL_OR_MANUAL_BEHAVIOR_SCHEMA")
    if found_columns and found_columns != set(BEHAVIOR_COLUMNS):
        blockers.append("PARTIAL_BEHAVIOR_COLUMN_SET")
    if found_triggers and found_triggers != set(EXPECTED_TRIGGERS):
        blockers.append("PARTIAL_BEHAVIOR_TRIGGER_SET")
    if found_functions and found_functions != set(EXPECTED_FUNCTIONS):
        blockers.append("PARTIAL_BEHAVIOR_FUNCTION_SET")
    if not sha256_available:
        blockers.append("SERVER_SHA256_FUNCTION_UNAVAILABLE")
    if context_population and (
        context_population["unexpected_kind_rows"]
        or context_population["legacy_rows_with_behavior_fields"]
    ):
        blockers.append("EXISTING_CONTEXT_ROWS_VIOLATE_BEHAVIOR_ISOLATION")
    missing_runtime_grants = [
        row["table_name"] for row in runtime_grants
        if not row["can_select"] or not row["can_insert"]
    ]
    if missing_runtime_grants:
        blockers.append("RUNTIME_ROLE_MISSING_REQUIRED_TABLE_GRANTS")
    active_writers = [
        row for row in locks
        if row["granted"] and row["mode"] in {
            "RowExclusiveLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock",
        }
    ]
    return {
        "operation": "READ_ONLY_MIGRATION_READINESS_REVIEW",
        "database_mutations": False,
        "server": server,
        "migration": migration,
        "adjusted_schema_migration": adjusted_schema_migration,
        "schema": {
            "expected_columns": BEHAVIOR_COLUMNS, "columns": columns,
            "constraints": constraints, "indexes": indexes,
            "triggers": triggers, "functions": functions,
            "link_function_matches_adjusted_schema": link_function_matches_contract,
        },
        "relations": relations,
        "exact_counts": exact_counts,
        "population": {
            "contexts": context_population,
            "behavior_source_evidence": behavior_source_evidence,
            "behavior_links": behavior_links,
            "behavior_context_details": behavior_context_details,
            "latest_behavior_contexts": (
                list(latest_context_by_ticker.values())
                if latest_context_by_ticker is not None else None
            ),
        },
        "relation_locks": locks,
        "active_writer_locks": active_writers,
        "runtime_role": settings["DB_USER"],
        "runtime_grants": runtime_grants,
        "sha256_available": sha256_available,
        "blocker_codes": list(dict.fromkeys(blockers)),
        "ready_for_bounded_stage_1": not blockers and not migration["registered"],
        "stage_status": {
            "stage_1_additive_installed": (
                migration["registered"] and found_columns == set(BEHAVIOR_COLUMNS)
                and len(constraints) == 1
                and found_triggers == set(EXPECTED_TRIGGERS)
                and found_functions == set(EXPECTED_FUNCTIONS)
            ),
            "stage_2_constraint_validated": bool(
                constraints and constraints[0]["validated"]
            ),
            "stage_3_index_present": bool(indexes),
            "stage_3_index_valid": bool(indexes and indexes[0]["valid"]),
            "stage_3_index_ready": bool(indexes and indexes[0]["ready"]),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cohort-only", action="store_true",
        help="Emit only dated behavior population and per-context link details.",
    )
    arguments = parser.parse_args()
    payload = review()
    if arguments.cohort_only:
        repository = EquityEvidenceRepository()
        population = payload["population"]
        all_details = population["behavior_context_details"] or []
        latest_details = population["latest_behavior_contexts"] or []
        for detail in latest_details:
            decision_at = max(detail["observed_at"], detail["created_at"])
            detail["operational_reader_available"] = repository.get_behavior_as_of(
                detail["security_id"],
                DecisionWatermark(detail["market_time"], decision_at),
                profile="OPTIONS_SWING_V1",
                definition_sha256=detail["behavior_definition_sha256"],
                policy_sha256=detail["context_policy_sha256"],
            ) is not None
        compact_latest = [{
            key: detail[key] for key in (
                "ticker", "equity_context_snapshot_id", "market_time", "observed_at",
                "link_count", "adjusted_links", "raw_links", "linked_intervals",
                "payload_source_count", "relative_strength_status",
                "current_source_policy", "operational_reader_available",
            )
        } for detail in latest_details]
        payload = {
            "operation": payload["operation"],
            "database_mutations": payload["database_mutations"],
            "reviewed_at": payload["server"]["reviewed_at"],
            "exact_counts": payload["exact_counts"],
            "population": {
                "contexts": population["contexts"],
                "behavior_source_evidence": population["behavior_source_evidence"],
                "behavior_links": population["behavior_links"],
                "all_context_summary": {
                    "rows": len(all_details),
                    "current_source_policy": sum(
                        detail["current_source_policy"] for detail in all_details
                    ),
                    "superseded_source_policy": sum(
                        not detail["current_source_policy"] for detail in all_details
                    ),
                },
                "latest_context_summary": {
                    "rows": len(latest_details),
                    "links": sum(detail["link_count"] for detail in latest_details),
                    "current_source_policy": sum(
                        detail["current_source_policy"] for detail in latest_details
                    ),
                    "operational_reader_available": sum(
                        detail["operational_reader_available"] for detail in latest_details
                    ),
                },
                "latest_behavior_contexts": compact_latest,
            },
            "blocker_codes": payload["blocker_codes"],
        }
    print(json.dumps(payload, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())