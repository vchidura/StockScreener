#!/usr/bin/env python3
"""Read-only boundary audit for the canonical V2-10 target."""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from pathlib import Path

from psycopg2.extras import RealDictCursor

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402

COUNT_RELATIONS = (
    "rt.capability_profiles",
    "rt.capability_checks",
    "rt.seed_manifests",
    "rt.seed_manifest_members",
    "rt.seed_current",
    "rt.subscription_manifests",
    "rt.subscription_members",
    "rt.cohorts",
    "rt.provider_request_ledger",
    "rt.acquisition_batches",
    "rt.acquisition_facts",
    "rt.acquisition_watermarks",
    "rt.materialization_jobs",
    "rt.bar_catalog",
    "rt.bar_revisions",
    "rt.bar_current",
    "rt.cycle_receipts",
    "rt.screening_generations",
    "rt.portal_releases",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("the V2-10 boundary audit targets only stocks_realtime_v2")
    return args


def main(argv=None):
    args = parse_args(argv)
    settings = admin_settings(BACKEND_DIR / ".env")
    with closing(connect(args.database, settings, application_name="audit_v2_backfill_boundary")) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT set_config('statement_timeout', '30s', true)")
                cursor.execute(
                    "SELECT current_database()::text AS database,"
                    "current_setting('transaction_read_only') AS read_only,"
                    "pg_database_size(current_database()) AS database_bytes"
                )
                control = dict(cursor.fetchone())
                counts = {}
                for relation in COUNT_RELATIONS:
                    cursor.execute(f"SELECT count(*) AS count FROM {relation}")
                    counts[relation] = int(cursor.fetchone()["count"])
                cursor.execute(
                    "SELECT COUNT(DISTINCT ticker) AS ticker_count,COUNT(*) AS revision_count,"
                    "COUNT(*) FILTER (WHERE active AND effective_from<=clock_timestamp() "
                    "AND (effective_to IS NULL OR effective_to>clock_timestamp()) "
                    "AND observed_at<=clock_timestamp()) AS currently_valid_revisions,"
                    "MAX(effective_from) AS latest_effective_from,"
                    "MAX(observed_at) AS latest_observed_at "
                    "FROM ref.security_revisions WHERE ticker=ANY(%s)",
                    (list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),),
                )
                identities = dict(cursor.fetchone())
                cursor.execute(
                    "SELECT MAX(valid_until) AS latest_valid_until,"
                    "COUNT(*) FILTER (WHERE status='READY' AND valid_until>clock_timestamp()) AS current_ready "
                    "FROM rt.seed_manifests"
                )
                seeds = dict(cursor.fetchone())
                cursor.execute(
                    "SELECT MAX(expires_at) AS latest_expires_at,"
                    "COUNT(*) FILTER (WHERE expires_at>clock_timestamp()) AS current_profiles "
                    "FROM rt.capability_profiles"
                )
                profiles = dict(cursor.fetchone())
                cursor.execute(
                    "SELECT endpoint_class,state,outcome,COUNT(*) AS count "
                    "FROM rt.provider_request_ledger GROUP BY endpoint_class,state,outcome "
                    "ORDER BY endpoint_class,state,outcome"
                )
                requests = tuple(dict(row) for row in cursor.fetchall())
                cursor.execute(
                    "SELECT job_kind,status,COUNT(*) AS count,"
                    "COUNT(*) FILTER (WHERE failure_code IS NOT NULL) AS failures "
                    "FROM rt.materialization_jobs GROUP BY job_kind,status "
                    "ORDER BY job_kind,status"
                )
                jobs = tuple(dict(row) for row in cursor.fetchall())
                cursor.execute(
                    "SELECT MIN(batch.created_at) AS acquisition_started_at,"
                    "MAX(batch.created_at) AS acquisition_completed_at,"
                    "MIN(job.created_at) AS jobs_started_at,"
                    "MAX(job.updated_at) AS jobs_completed_at,"
                    "MIN(bar.created_at) AS bars_started_at,"
                    "MAX(bar.completed_at) AS bars_completed_at "
                    "FROM rt.acquisition_batches AS batch "
                    "CROSS JOIN (SELECT MIN(created_at) AS created_at,MAX(updated_at) AS updated_at "
                    "FROM rt.materialization_jobs) AS job "
                    "CROSS JOIN (SELECT MIN(created_at) AS created_at,MAX(created_at) AS completed_at "
                    "FROM rt.bar_revisions) AS bar "
                    "GROUP BY job.created_at,job.updated_at,bar.created_at,bar.completed_at"
                )
                timings = dict(cursor.fetchone() or {})
                cursor.execute(
                    "SELECT window_key,state,owner_epoch,release_at,cutoff,deadline "
                    "FROM rt.window_runs ORDER BY cutoff,window_key"
                )
                windows = tuple(dict(row) for row in cursor.fetchall())
                cursor.execute(
                    "SELECT window_run.window_key,run.stage_id,run.state,run.attempt,run.reason_codes "
                    "FROM rt.stage_runs AS run JOIN rt.window_runs AS window_run USING(window_id) "
                    "ORDER BY window_run.cutoff,run.stage_id,run.attempt"
                )
                stage_runs = tuple(dict(row) for row in cursor.fetchall())
                cursor.execute(
                    "SELECT window_run.window_key,receipt.receipt_kind,receipt.cutoff,"
                    "receipt.reference_generation,receipt.build_id "
                    "FROM rt.cycle_receipts AS receipt "
                    "JOIN rt.window_runs AS window_run USING(window_id) "
                    "ORDER BY receipt.cutoff,window_run.window_key,receipt.receipt_kind"
                )
                receipts = tuple(dict(row) for row in cursor.fetchall())
        finally:
            connection.rollback()
    print(json.dumps({
        "state": "COMPLETE",
        "control": control,
        "counts": counts,
        "identities": identities,
        "seeds": seeds,
        "profiles": profiles,
        "provider_request_groups": requests,
        "materialization_job_groups": jobs,
        "timings": timings,
        "windows": windows,
        "stage_runs": stage_runs,
        "receipts": receipts,
        "provider_requests": counts["rt.provider_request_ledger"],
        "database_writes": 0,
    }, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
