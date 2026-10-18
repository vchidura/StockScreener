#!/usr/bin/env python3
"""Report stored-only Advanced equity warming state."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file),
        process=RealtimeV2Process.APP_WORKER,
    )
    pool = ThreadedConnectionPool(1, 1, dsn=config.app_dsn)
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
        ),
    )
    provider.verify()
    try:
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT profile.provider_tier,profile.status,"
                "profile.expires_at,manifest.stock_count,"
                "manifest.member_count,cohort.mode,"
                "tier.stocks_tier,tier.options_tier,tier.ready_state "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                "JOIN rt.capability_profiles AS profile USING(profile_id) "
                "JOIN rt.provider_tier_cohorts AS tier USING(cohort_id) "
                "WHERE cohort.cohort_id=%s",
                (str(args.cohort_id),),
            )
            identity = cursor.fetchone()
            if identity is None:
                raise ValueError("Advanced warming cohort is unavailable")
            counts = {}
            queries = {
                "portal_pointers": (
                    "SELECT count(*) FROM rt.portal_cohort_current "
                    "WHERE cohort_id=%s"
                ),
                "bar_revisions": (
                    "SELECT count(*) FROM rt.bar_revisions "
                    "WHERE cohort_id=%s"
                ),
                "stream_sessions": (
                    "SELECT count(*) FROM rt.stream_sessions AS session "
                    "JOIN rt.cohorts AS cohort USING(manifest_id) "
                    "WHERE cohort.cohort_id=%s"
                ),
                "consumer_checkpoints": (
                    "SELECT count(*) FROM rt.consumer_checkpoints "
                    "WHERE cohort_id=%s"
                ),
                "portal_releases": (
                    "SELECT count(*) FROM rt.portal_releases "
                    "WHERE cohort_id=%s"
                ),
            }
            for name, query in queries.items():
                cursor.execute(query, (str(args.cohort_id),))
                counts[name] = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT session.status,session.failure_code,"
                "session.received_frame_count,session.received_byte_count,"
                "session.admitted_market_message_count,"
                "session.admitted_market_byte_count,"
                "session.last_raw_offset,session.last_event_offset,"
                "session.event_time_watermark "
                "FROM rt.stream_sessions AS session "
                "JOIN rt.cohorts AS cohort USING(manifest_id) "
                "WHERE cohort.cohort_id=%s "
                "ORDER BY session.started_at DESC LIMIT 1",
                (str(args.cohort_id),),
            )
            latest_session = cursor.fetchone()
            cursor.execute(
                "SELECT reason,recovery_status,last_durable_offset "
                "FROM rt.stream_gaps AS gap "
                "JOIN rt.stream_sessions AS session USING(session_id) "
                "JOIN rt.cohorts AS cohort USING(manifest_id) "
                "WHERE cohort.cohort_id=%s "
                "ORDER BY gap.recorded_at DESC LIMIT 1",
                (str(args.cohort_id),),
            )
            latest_gap = cursor.fetchone()
            cursor.execute(
                "SELECT window_run.window_key,window_run.state,run.stage_id,"
                "run.attempt,run.state,run.reason_codes "
                "FROM rt.window_runs AS window_run "
                "JOIN rt.stage_runs AS run USING(window_id) "
                "WHERE window_run.cohort_id=%s "
                "ORDER BY window_run.updated_at DESC,"
                "run.stage_id,run.attempt "
                "LIMIT 20",
                (str(args.cohort_id),),
            )
            recent_stages = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT job_id,job_kind,interval,status,failure_code,"
                "primary_window_id,bundle_sha256,policy_sha256,build_id "
                "FROM rt.materialization_jobs WHERE cohort_id=%s "
                "ORDER BY updated_at DESC,job_id LIMIT 20",
                (str(args.cohort_id),),
            )
            recent_jobs = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT ARRAY(SELECT jsonb_object_keys("
                "snapshot.payload->'chart_refs')),"
                "snapshot.payload->'missing_producers' "
                "FROM rt.snapshot_catalog AS catalog "
                "JOIN rt.portal_snapshot_hot AS snapshot "
                "ON snapshot.snapshot_id=catalog.snapshot_id "
                "AND snapshot.generated_at=catalog.generated_at "
                "WHERE catalog.cohort_id=%s "
                "AND catalog.snapshot_type='TICKER_WORKSPACE_V1' "
                "AND catalog.snapshot_key='SPY' "
                "ORDER BY catalog.generated_at DESC,catalog.snapshot_id DESC "
                "LIMIT 1",
                (str(args.cohort_id),),
            )
            workspace_availability = cursor.fetchone()
        report = {
            "status": "WARMING",
            "cohort_id": str(args.cohort_id),
            "provider_tier": identity[0],
            "profile_status": identity[1],
            "profile_expires_at": identity[2].isoformat(),
            "stock_count": int(identity[3]),
            "member_count": int(identity[4]),
            "cohort_mode": identity[5],
            "stocks_tier": identity[6],
            "options_tier": identity[7],
            "ready_state": identity[8],
            **counts,
            "latest_session": (
                None
                if latest_session is None
                else {
                    "status": latest_session[0],
                    "failure_code": latest_session[1],
                    "received_frames": int(latest_session[2]),
                    "received_bytes": int(latest_session[3]),
                    "market_messages": int(latest_session[4]),
                    "market_bytes": int(latest_session[5]),
                    "last_raw_offset": int(latest_session[6]),
                    "last_event_offset": int(latest_session[7]),
                    "event_time_watermark": (
                        latest_session[8].isoformat()
                        if latest_session[8] is not None
                        else None
                    ),
                }
            ),
            "latest_gap": (
                None
                if latest_gap is None
                else {
                    "reason": latest_gap[0],
                    "recovery_status": latest_gap[1],
                    "last_durable_offset": int(latest_gap[2]),
                }
            ),
            "recent_stages": [
                {
                    "window_key": row[0],
                    "window_state": row[1],
                    "stage_id": row[2],
                    "attempt": int(row[3]),
                    "stage_state": row[4],
                    "reason_codes": tuple(row[5] or ()),
                }
                for row in recent_stages
            ],
            "recent_jobs": [
                {
                    "job_id": str(row[0]),
                    "job_kind": row[1],
                    "interval": row[2],
                    "status": row[3],
                    "failure_code": row[4],
                    "primary_window_id": (
                        str(row[5]) if row[5] is not None else None
                    ),
                    "bundle_sha256": row[6],
                    "policy_sha256": row[7],
                    "build_id": row[8],
                }
                for row in recent_jobs
            ],
            "workspace_availability": (
                None
                if workspace_availability is None
                else {
                    "chart_refs": tuple(
                        workspace_availability[0] or ()
                    ),
                    "missing_producers": (
                        workspace_availability[1] or {}
                    ),
                }
            ),
            "provider_requests": 0,
            "database_writes": 0,
            "credentials_exposed": False,
        }
        print(json.dumps(report, indent=2))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
