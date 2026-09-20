#!/usr/bin/env python3
"""Check or concurrently create the public stock behavior as-of index as stage 3."""
from __future__ import annotations

import argparse
import json
import os
from contextlib import closing
from pathlib import Path
from time import perf_counter

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BACKEND_DIR / ".env")

MIGRATION_VERSION = "044_equity_stock_behavior_contract"
CONSTRAINT_NAME = "ck_equity_context_behavior_contract"
INDEX_NAME = "idx_equity_behavior_asof"
BEHAVIOR_COLUMNS = (
    "context_kind",
    "behavior_schema_version",
    "behavior_definition_sha256",
    "behavior_computed_at",
    "behavior_payload_text",
    "behavior_payload_sha256",
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--confirm-index-name",
        help="Required with --apply and must equal the exact stage-3 index name.",
    )
    return parser.parse_args()


def _settings() -> dict[str, str]:
    names = (
        "DB_NAME", "DB_HOST", "DB_PORT",
        "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD",
    )
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("concurrent index creation requires: " + ", ".join(missing))
    return settings


def _state(cursor) -> dict:
    cursor.execute("""
        SELECT EXISTS (
                   SELECT 1 FROM public.schema_migrations WHERE version = %s
               ) AS migration_registered,
               (
                   SELECT convalidated FROM pg_constraint
                   WHERE conrelid = 'public.equity_context_snapshots'::regclass
                     AND conname = %s
               ) AS constraint_validated,
               (
                   SELECT COUNT(*) FROM pg_attribute
                   WHERE attrelid = 'public.equity_context_snapshots'::regclass
                     AND attname = ANY(%s::text[]) AND NOT attisdropped
               ) AS behavior_column_count
    """, (MIGRATION_VERSION, CONSTRAINT_NAME, list(BEHAVIOR_COLUMNS)))
    state = dict(cursor.fetchone())
    state["behavior_column_count"] = int(state["behavior_column_count"])
    cursor.execute("""
        SELECT relation.oid IS NOT NULL AS index_present,
               catalog.indisvalid AS index_valid,
               catalog.indisready AS index_ready,
               CASE WHEN relation.oid IS NULL THEN NULL
                    ELSE pg_relation_size(relation.oid) END AS index_bytes,
               CASE WHEN relation.oid IS NULL THEN NULL
                    ELSE pg_get_indexdef(relation.oid) END AS index_definition
        FROM (SELECT to_regclass(%s) AS oid) AS selected
        LEFT JOIN pg_class AS relation ON relation.oid = selected.oid
        LEFT JOIN pg_index AS catalog ON catalog.indexrelid = relation.oid
    """, (f"public.{INDEX_NAME}",))
    state.update(dict(cursor.fetchone()))
    if state["behavior_column_count"] != len(BEHAVIOR_COLUMNS):
        return state
    cursor.execute("""
        SELECT COUNT(*) FILTER (WHERE context_kind = 'STOCK_BEHAVIOR') AS behavior_rows,
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
    state.update({key: int(value) for key, value in dict(cursor.fetchone()).items()})
    return state


def _require_stage_3_ready(state: dict) -> None:
    if not state["migration_registered"]:
        raise RuntimeError("stage 1 migration is not registered")
    if state["constraint_validated"] is not True:
        raise RuntimeError("stage 2 constraint is not validated")
    if state["behavior_column_count"] != len(BEHAVIOR_COLUMNS):
        raise RuntimeError("stage 1 behavior columns are incomplete")
    if state["index_present"]:
        raise RuntimeError("stage 3 index already exists; inspect it before any action")
    if any(state[name] for name in (
        "behavior_rows", "unexpected_kind_rows", "legacy_rows_with_behavior_fields",
    )):
        raise RuntimeError("stage 3 requires unchanged legacy-only context population")


def main() -> int:
    arguments = _arguments()
    if arguments.apply and arguments.confirm_index_name != INDEX_NAME:
        raise RuntimeError(f"--apply requires --confirm-index-name {INDEX_NAME}")
    settings = _settings()
    started = perf_counter()
    with closing(psycopg2.connect(
        dbname=settings["DB_NAME"], user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], options="-c timezone=UTC",
        application_name="stock_behavior_concurrent_index",
        cursor_factory=RealDictCursor,
    )) as connection:
        connection.set_session(readonly=not arguments.apply, autocommit=arguments.apply)
        with connection.cursor() as cursor:
            cursor.execute("SET lock_timeout = '2s'")
            cursor.execute("SET statement_timeout = '5min'")
            before = _state(cursor)
            _require_stage_3_ready(before)
            applied = False
            if arguments.apply:
                cursor.execute("""
                    CREATE INDEX CONCURRENTLY idx_equity_behavior_asof
                    ON public.equity_context_snapshots
                    (security_id, strategy_horizon, behavior_definition_sha256,
                     context_policy_sha256, market_time DESC, observed_at DESC)
                    WHERE context_kind = 'STOCK_BEHAVIOR'
                """)
                applied = True
            after = _state(cursor)
            if arguments.apply and not (after["index_valid"] and after["index_ready"]):
                raise RuntimeError("behavior index is not valid and ready")
            cursor.execute("SELECT clock_timestamp() AS checked_at")
            checked_at = cursor.fetchone()["checked_at"]
        if not arguments.apply:
            connection.rollback()
    print(json.dumps({
        "operation": "CREATE_STOCK_BEHAVIOR_INDEX_CONCURRENTLY",
        "apply_requested": arguments.apply,
        "applied": applied,
        "before": before,
        "after": after,
        "checked_at": checked_at,
        "elapsed_seconds": round(perf_counter() - started, 3),
        "autocommit": arguments.apply,
    }, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())