#!/usr/bin/env python3
"""Bind an Advanced cohort to immutable prior-session Developer history."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-cohort-id", type=UUID, required=True)
    parser.add_argument("--source-cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.target_cohort_id == args.source_cohort_id:
        parser.error("target and source cohorts must differ")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(args.env_file)
    )
    pool = ThreadedConnectionPool(1, 2, dsn=config.app_dsn)
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
                "SELECT manifest.manifest_sha256,profile.provider_tier,"
                "manifest.stock_count,array_agg(DISTINCT reference.ticker "
                "ORDER BY reference.ticker) "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                "JOIN rt.capability_profiles AS profile USING(profile_id) "
                "JOIN rt.subscription_members AS member USING(manifest_id) "
                "JOIN rt.instruments AS instrument USING(instrument_id) "
                "JOIN ref.security_revisions AS reference USING(security_id) "
                "WHERE cohort.cohort_id=%s AND member.channel='AM' "
                "AND reference.active "
                "AND reference.effective_from<=clock_timestamp() "
                "AND (reference.effective_to IS NULL "
                "OR reference.effective_to>clock_timestamp()) "
                "GROUP BY manifest.manifest_sha256,profile.provider_tier,"
                "manifest.stock_count",
                (str(args.target_cohort_id),),
            )
            target = cursor.fetchone()
            cursor.execute(
                "SELECT manifest.manifest_sha256,profile.provider_tier,"
                "array_agg(DISTINCT reference.ticker "
                "ORDER BY reference.ticker) "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                "JOIN rt.capability_profiles AS profile USING(profile_id) "
                "JOIN rt.subscription_members AS member USING(manifest_id) "
                "JOIN rt.instruments AS instrument USING(instrument_id) "
                "JOIN ref.security_revisions AS reference USING(security_id) "
                "WHERE cohort.cohort_id=%s AND member.channel='AM' "
                "AND reference.active "
                "GROUP BY manifest.manifest_sha256,profile.provider_tier",
                (str(args.source_cohort_id),),
            )
            source = cursor.fetchone()
            if (
                target is None
                or target[1] != "MASSIVE_ADVANCED"
                or len(set(target[3])) != int(target[2])
                or source is None
                or source[1] != "MASSIVE_DEVELOPER"
                or not set(target[3]).issubset(set(source[2]))
            ):
                target_summary = (
                    None
                    if target is None
                    else {
                        "tier": target[1],
                        "stock_count": int(target[2]),
                        "distinct_members": len(set(target[3])),
                    }
                )
                source_summary = (
                    None
                    if source is None
                    else {
                        "tier": source[1],
                        "distinct_members": len(set(source[2])),
                    }
                )
                raise ValueError(
                    "Advanced history binding cohort membership is invalid: "
                    f"target={target_summary} source={source_summary}"
                )
            history_before = (
                OptionExchangeCalendar()
                .session_open(args.session_date)
                .astimezone(timezone.utc)
            )
            cursor.execute(
                "SELECT count(DISTINCT revision.session_date) FILTER ("
                "WHERE revision.interval='1d' "
                "AND revision.adjustment_basis='ADJUSTED'),"
                "count(DISTINCT revision.session_date) FILTER ("
                "WHERE revision.interval='1d' "
                "AND revision.adjustment_basis='RAW_UNADJUSTED'),"
                "count(*) FILTER (WHERE revision.interval='5m'),"
                "count(*) FILTER (WHERE revision.interval='1h') "
                "FROM rt.bar_current AS current "
                "JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.observed_at=current.observed_at "
                "WHERE current.cohort_id=%s "
                "AND current.provider_tier='MASSIVE_DEVELOPER' "
                "AND current.instrument_id IN ("
                "SELECT member.instrument_id "
                "FROM rt.subscription_members AS member "
                "JOIN rt.cohorts AS cohort USING(manifest_id) "
                "JOIN rt.instruments AS instrument USING(instrument_id) "
                "JOIN ref.security_revisions AS reference USING(security_id) "
                "WHERE cohort.cohort_id=%s AND member.channel='AM' "
                "AND reference.ticker=ANY(%s)) "
                "AND revision.bar_end<%s",
                (
                    str(args.source_cohort_id),
                    str(args.source_cohort_id),
                    list(target[3]),
                    history_before,
                ),
            )
            coverage = tuple(int(value) for value in cursor.fetchone())
        if (
            coverage[0] < 283
            or coverage[1] < 90
            or coverage[2] < 1
            or coverage[3] < 1
        ):
            raise ValueError(
                "Developer source cohort lacks required retained portal history"
            )
        identity = {
            "contract": "advanced-cohort-history-binding-v1",
            "target_cohort_id": args.target_cohort_id,
            "source_cohort_id": args.source_cohort_id,
            "source_provider_tier": "MASSIVE_DEVELOPER",
            "history_before": history_before,
            "source_manifest_sha256": source[0],
            "symbols": tuple(target[3]),
            "authorization_reference": args.authorization_reference,
        }
        binding_sha256 = canonical_sha256(identity)
        if args.execute:
            with provider.transaction(APP_ROLE) as unit:
                cursor = unit.cursor
                cursor.execute(
                    "INSERT INTO rt.cohort_history_bindings ("
                    "target_cohort_id,source_cohort_id,"
                    "source_provider_tier,history_before,"
                    "source_manifest_sha256,authorization_reference,"
                    "binding_sha256) VALUES ("
                    "%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s) "
                    "ON CONFLICT (target_cohort_id) DO NOTHING",
                    (
                        str(args.target_cohort_id),
                        str(args.source_cohort_id),
                        history_before,
                        source[0],
                        args.authorization_reference,
                        binding_sha256,
                    ),
                )
                cursor.execute(
                    "SELECT binding_sha256 FROM rt.cohort_history_bindings "
                    "WHERE target_cohort_id=%s",
                    (str(args.target_cohort_id),),
                )
                stored = cursor.fetchone()
                if stored is None or stored[0] != binding_sha256:
                    raise ValueError(
                        "Advanced history binding persistence conflict"
                    )
        report = {
            "mode": "EXECUTE" if args.execute else "PLAN",
            "status": "BOUND" if args.execute else "PLANNED",
            "target_cohort_id": str(args.target_cohort_id),
            "source_cohort_id": str(args.source_cohort_id),
            "symbols": tuple(target[3]),
            "history_before": history_before.isoformat(),
            "source_manifest_sha256": source[0],
            "binding_sha256": binding_sha256,
            "coverage": {
                "adjusted_daily_sessions": coverage[0],
                "raw_daily_sessions": coverage[1],
                "five_minute_bars": coverage[2],
                "one_hour_bars": coverage[3],
            },
            "provider_requests": 0,
            "publication_advanced": False,
            "platform_pointer_changed": False,
            "credentials_exposed": False,
        }
        if args.execute:
            write_report(args.report_path, report)
        print(json.dumps(report, indent=2))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
