#!/usr/bin/env python3
"""Purge the derived option layer so it can be rebuilt under corrected mark-time semantics.

Deletes only *derived* rows. Immutable provider evidence — raw batch pages, ingestion
runs, the contract catalog, universe runs, and trade events — is retained, so a
re-normalization replay remains possible later.

Dry run by default. Deleting requires both --apply and --confirm PURGE, and the whole
deletion runs in one transaction so a failure leaves the database untouched.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

# Ordered children first so every foreign key is satisfied at each step.
DERIVED_TABLES: tuple[str, ...] = (
    "option_candidate_execution_gates",
    "option_recommendation_validity_current",
    "option_recommendation_validity_events",
    "option_signal_current_marks",
    "option_signal_decay_outcomes",
    "option_signal_occurrences",
    "option_signal_legs",
    "option_signal_events",
    "option_scenario_results",
    "option_signal_suppressions",
    "option_candidate_legs",
    "option_strategy_candidates",
    "option_decision_evidence",
    "option_gamma_profiles",
    "option_volatility_surfaces",
    "option_flow_windows",
    "option_iv_context_snapshots",
    "option_expiration_analytics",
    "option_context_snapshots",
    "option_analysis_runs",
    "option_chain_snapshots",
    "option_snapshot_fact_keys",
    "option_strategy_registry",
)

# Provider evidence and identity that must survive the purge.
RETAINED_TABLES: tuple[str, ...] = (
    "option_raw_batch_pages",
    "option_ingestion_runs",
    "option_contract_catalog",
    "option_contract_catalog_versions",
    "option_contract_discoveries",
    "option_daily_contract_facts",
    "option_universe_runs",
    "option_universe_members",
    "option_universe_candidates",
    "option_trade_events",
    "option_trade_cursors",
    "option_trade_watchlist",
    "option_provider_trade_semantics",
)

# Work items are keyed to superseded batches and matrices; only these stages are cleared.
WORK_STAGES: tuple[str, ...] = ("NORMALIZE", "STRATEGY")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Execute the deletion.")
    parser.add_argument("--confirm", default="", help="Must be PURGE when applying.")
    return parser.parse_args()


def _connect():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"], options="-c timezone=UTC",
    )


def _counts(cursor, tables: tuple[str, ...]) -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    for table in tables:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (f"public.{table}",))
        if not cursor.fetchone()["present"]:
            result[table] = None
            continue
        cursor.execute(f"SELECT count(*) AS n FROM {table}")
        result[table] = int(cursor.fetchone()["n"])
    return result


def main() -> int:
    args = _parse_args()
    connection = _connect()
    connection.autocommit = False
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    before_derived = _counts(cursor, DERIVED_TABLES)
    before_retained = _counts(cursor, RETAINED_TABLES)
    cursor.execute(
        "SELECT count(*) AS n FROM option_work_items WHERE stage = ANY(%s)",
        (list(WORK_STAGES),),
    )
    before_work = int(cursor.fetchone()["n"])

    print("=== derived rows to delete ===")
    total = 0
    for table, count in before_derived.items():
        state = "absent" if count is None else f"{count:>9,}"
        print(f"  {table:<45} {state}")
        total += count or 0
    print(f"  {'option_work_items (' + '/'.join(WORK_STAGES) + ')':<45} {before_work:>9,}")
    print(f"  {'TOTAL':<45} {total + before_work:>9,}")

    print("\n=== retained provider evidence ===")
    for table, count in before_retained.items():
        state = "absent" if count is None else f"{count:>9,}"
        print(f"  {table:<45} {state}")

    if not args.apply:
        print("\nDRY RUN. Nothing was deleted. Re-run with --apply --confirm PURGE.")
        connection.rollback()
        connection.close()
        return 0

    if args.confirm != "PURGE":
        print("\nRefusing to delete: --confirm PURGE is required.", file=sys.stderr)
        connection.rollback()
        connection.close()
        return 2

    print("\n=== deleting ===")
    try:
        for table in DERIVED_TABLES:
            if before_derived[table] is None:
                continue
            cursor.execute(f"DELETE FROM {table}")
            print(f"  {table:<45} {cursor.rowcount:>9,} deleted")
        cursor.execute(
            "DELETE FROM option_work_items WHERE stage = ANY(%s)", (list(WORK_STAGES),)
        )
        print(f"  {'option_work_items':<45} {cursor.rowcount:>9,} deleted")

        after_derived = _counts(cursor, DERIVED_TABLES)
        after_retained = _counts(cursor, RETAINED_TABLES)
        residue = {
            table: count for table, count in after_derived.items() if count
        }
        if residue:
            raise RuntimeError(f"derived rows survived the purge: {residue}")
        lost = {
            table: (before_retained[table], after_retained[table])
            for table in RETAINED_TABLES
            if before_retained[table] != after_retained[table]
        }
        if lost:
            raise RuntimeError(f"retained tables changed: {lost}")
    except Exception as exc:
        connection.rollback()
        connection.close()
        print(f"\nROLLED BACK: {exc}", file=sys.stderr)
        return 1

    connection.commit()
    print("\n=== verification ===")
    print("  all derived tables empty")
    print("  all retained tables unchanged")
    print("\nPURGE_COMMITTED")
    connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
