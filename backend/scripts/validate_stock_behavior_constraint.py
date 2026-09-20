#!/usr/bin/env python3
"""Check or validate the public stock behavior CHECK constraint as deployment stage 2."""
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
        "--confirm-constraint-name",
        help="Required with --apply and must equal the exact stage-2 constraint name.",
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
        raise RuntimeError("constraint validation requires: " + ", ".join(missing))
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
               to_regclass(%s) IS NOT NULL AS stage_3_index_present,
               (
                   SELECT COUNT(*) FROM pg_attribute
                   WHERE attrelid = 'public.equity_context_snapshots'::regclass
                     AND attname = ANY(%s::text[]) AND NOT attisdropped
               ) AS behavior_column_count
    """, (
        MIGRATION_VERSION, CONSTRAINT_NAME, f"public.{INDEX_NAME}",
        list(BEHAVIOR_COLUMNS),
    ))
    state = dict(cursor.fetchone())
    state["behavior_column_count"] = int(state["behavior_column_count"])
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


def _require_stage_2_ready(state: dict) -> None:
    if not state["migration_registered"]:
        raise RuntimeError("stage 1 migration is not registered")
    if state["constraint_validated"] is None:
        raise RuntimeError("behavior constraint is missing")
    if state["behavior_column_count"] != len(BEHAVIOR_COLUMNS):
        raise RuntimeError("stage 1 behavior columns are incomplete")
    if state["stage_3_index_present"]:
        raise RuntimeError("stage 3 index already exists")
    if any(state[name] for name in (
        "behavior_rows", "unexpected_kind_rows", "legacy_rows_with_behavior_fields",
    )):
        raise RuntimeError("stage 2 requires unchanged legacy-only context population")


def main() -> int:
    arguments = _arguments()
    if arguments.apply and arguments.confirm_constraint_name != CONSTRAINT_NAME:
        raise RuntimeError(f"--apply requires --confirm-constraint-name {CONSTRAINT_NAME}")
    settings = _settings()
    started = perf_counter()
    with closing(psycopg2.connect(
        dbname=settings["DB_NAME"], user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], options="-c timezone=UTC",
        application_name="stock_behavior_constraint_validation",
        cursor_factory=RealDictCursor,
    )) as connection:
        connection.set_session(readonly=not arguments.apply, autocommit=False)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '2s'")
                cursor.execute("SET LOCAL statement_timeout = '5min'")
                before = _state(cursor)
                _require_stage_2_ready(before)
                applied = False
                if arguments.apply and not before["constraint_validated"]:
                    cursor.execute(
                        "ALTER TABLE public.equity_context_snapshots "
                        "VALIDATE CONSTRAINT ck_equity_context_behavior_contract"
                    )
                    applied = True
                after = _state(cursor)
                if arguments.apply and not after["constraint_validated"]:
                    raise RuntimeError("behavior constraint was not validated")
                cursor.execute("SELECT clock_timestamp() AS checked_at")
                checked_at = cursor.fetchone()["checked_at"]
    print(json.dumps({
        "operation": "VALIDATE_STOCK_BEHAVIOR_CONSTRAINT",
        "apply_requested": arguments.apply,
        "applied": applied,
        "before": before,
        "after": after,
        "checked_at": checked_at,
        "elapsed_seconds": round(perf_counter() - started, 3),
        "stage_3_untouched": not after["stage_3_index_present"],
    }, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())