#!/usr/bin/env python3
"""Execute the authorized V2-10 exact nine-ticker bulk backfill.

The one-shot operation copies the retained V1 control closure, reads retained bars in a
REPEATABLE READ/READ ONLY transaction, makes only the separately authorized 36 provider
requests, validates exact history and lineage, then persists through acquisition batches,
facts, watermarks, jobs and the ordinary immutable bar repository.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_bulk_backfill import (  # noqa: E402
    build_equity_bulk_backfill_plan,
    execute_equity_bulk_backfill,
)
from realtime.equity_v2_backfill_source import (  # noqa: E402
    APPROVED_REQUEST_CEILING,
    APPROVED_SESSION_DATE,
    APPROVED_TICKERS,
    AccountingPolygonTransport,
    collect_approved_provider_gaps,
    load_retained_v1_facts,
    load_retained_v1_lineage_support,
    validate_exact_backfill_facts,
)
from realtime.equity_validation_source import (  # noqa: E402
    EquityValidationIdentity,
    EquityValidationSourceLimits,
    PolygonEquityValidationHistorySource,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from realtime.ticker_projection import _security  # noqa: E402
from realtime.v2_backfill_bootstrap import copy_v2_backfill_control_plane  # noqa: E402
from scripts.realtime_script_support import PacedPolygonTransport, write_report  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402

SOURCE_DATABASE = "stocks_realtime_v1"
MINIMUM_REQUEST_INTERVAL_SECONDS = 0.5


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=lambda value: datetime.strptime(value, "%Y-%m-%d").date(),
                        required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--provider-authorization-reference", default="")
    parser.add_argument("--cache-directory", type=Path)
    parser.add_argument("--output")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("the V2-10 bulk backfill targets only stocks_realtime_v2")
    if args.session_date != APPROVED_SESSION_DATE:
        parser.error(f"the approved V2-10 session date is {APPROVED_SESSION_DATE}")
    if args.execute and (
        not args.authorization_reference.strip()
        or not args.provider_authorization_reference.strip()
        or args.cache_directory is None
    ):
        parser.error("--execute requires authorization references and --cache-directory")
    return args


def _load_target_identities(connection, *, cohort_id: UUID, selected_at: datetime):
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            "SELECT DISTINCT ON (revision.ticker) revision.*,"
            "revision.revision_id AS security_revision_id,instrument.instrument_id "
            "FROM ref.security_revisions AS revision "
            "JOIN rt.instruments AS instrument USING (security_id) "
            "WHERE revision.ticker=ANY(%s) AND revision.effective_from<=%s "
            "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
            "AND revision.observed_at<=%s AND revision.active "
            "ORDER BY revision.ticker,revision.observed_at DESC,revision.revision_id DESC",
            (list(APPROVED_TICKERS), selected_at, selected_at, selected_at),
        )
        rows = tuple(cursor.fetchall())
    identities = {
        row["ticker"]: EquityValidationIdentity(
            row["ticker"],
            _security(row),
            row["instrument_id"],
        )
        for row in rows
    }
    if set(identities) != set(APPROVED_TICKERS):
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT ticker,COUNT(*) AS revisions,"
                "COUNT(*) FILTER (WHERE active) AS active_revisions,"
                "COUNT(*) FILTER (WHERE effective_from<=%s "
                "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s) AS dated_revisions,"
                "MAX(effective_from) AS latest_effective_from,MAX(observed_at) AS latest_observed_at "
                "FROM ref.security_revisions WHERE ticker=ANY(%s) "
                "GROUP BY ticker ORDER BY ticker",
                (selected_at, selected_at, selected_at, list(APPROVED_TICKERS)),
            )
            diagnostics = tuple(dict(row) for row in cursor.fetchall())
        raise ValueError(
            "V2-10 target equity identities are incomplete: "
            f"found={sorted(identities)} expected={sorted(APPROVED_TICKERS)} "
            f"diagnostics={diagnostics}"
        )
    return tuple(identities[ticker] for ticker in APPROVED_TICKERS)


def _pool(settings, database):
    return ThreadedConnectionPool(
        1,
        2,
        dbname=database,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        application_name="run_realtime_v2_bulk_backfill",
        options="-c timezone=UTC",
    )


def _target_control_exists(connection, cohort_id: UUID) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM rt.cohorts WHERE cohort_id=%s)",
            (str(cohort_id),),
        )
        return bool(cursor.fetchone()[0])


def main(argv=None) -> int:
    args = parse_args(argv)
    plan_report = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "source": SOURCE_DATABASE,
        "target": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "tickers": list(APPROVED_TICKERS),
        "provider_tier": "MASSIVE_DEVELOPER",
        "provider_request_ceiling": APPROVED_REQUEST_CEILING,
        "provider_in_flight": 1,
        "minimum_request_interval_seconds": MINIMUM_REQUEST_INTERVAL_SECONDS,
    }
    if not args.execute:
        print(json.dumps({**plan_report, "database_writes": 0, "provider_requests": 0}, sort_keys=True))
        return 0
    settings = admin_settings(BACKEND_DIR / ".env")
    load_dotenv(BACKEND_DIR / ".env", override=True)
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("V2-10 bulk backfill requires POLYGON_API_KEY")
    started_at = datetime.now(timezone.utc)
    with closing(connect(args.database, settings, application_name="v2_backfill_control_status")) as target:
        control_exists = _target_control_exists(target, args.cohort_id)
        target.rollback()
    if control_exists:
        control = {"status": "EXISTING", "cohort_id": str(args.cohort_id)}
    else:
        with (
            closing(connect(SOURCE_DATABASE, settings, application_name="v2_backfill_control_source")) as source,
            closing(connect(args.database, settings, application_name="v2_backfill_control_target")) as target,
        ):
            control = copy_v2_backfill_control_plane(
                source,
                target,
                cohort_id=args.cohort_id,
            )
    with closing(connect(args.database, settings, application_name="v2_backfill_identities")) as target:
        identities = _load_target_identities(
            target,
            cohort_id=args.cohort_id,
            selected_at=started_at,
        )
        target.rollback()
    with closing(connect(SOURCE_DATABASE, settings, application_name="v2_backfill_retained_bars")) as source:
        source.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with source.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT set_config('statement_timeout', '120s', true)")
                retained = load_retained_v1_facts(
                    cursor,
                    cohort_id=args.cohort_id,
                    session_date=args.session_date,
                )
                lineage_support = load_retained_v1_lineage_support(cursor, retained)
        finally:
            source.rollback()
    transport = AccountingPolygonTransport(
        PacedPolygonTransport(
            RequestsPolygonHttpTransport(api_key, timeout_seconds=30),
            minimum_interval_seconds=MINIMUM_REQUEST_INTERVAL_SECONDS,
        ),
        cohort_id=args.cohort_id,
        clock=lambda: datetime.now(timezone.utc),
        cache_directory=args.cache_directory,
    )
    provider_source = PolygonEquityValidationHistorySource(
        transport,
        identities,
        cohort_id=args.cohort_id,
        limits=EquityValidationSourceLimits(
            maximum_requests=APPROVED_REQUEST_CEILING,
            maximum_pages_per_request=1,
            maximum_retries_per_request=0,
            maximum_workers=1,
        ),
    )
    provider = collect_approved_provider_gaps(provider_source)
    facts = (*retained, *provider.facts)
    coverage = validate_exact_backfill_facts(
        facts,
        session_date=args.session_date,
        lineage_support=lineage_support,
    )
    backfill_plan = build_equity_bulk_backfill_plan(
        (*facts, *lineage_support),
        cohort_id=args.cohort_id,
        tickers=APPROVED_TICKERS,
    )
    pool = _pool(settings, args.database)
    try:
        connection_provider = V2ConnectionProvider(
            {OWNER_POOL: pool},
            V2AccessConfig(
                security_mode="deferred",
                statement_timeout_ms=120_000,
            ),
        )
        connection_provider.verify()
        result = execute_equity_bulk_backfill(
            V2WorkerDatabase(connection_provider, RealtimeDatabaseRole.INGRESS),
            V2WorkerDatabase(connection_provider, RealtimeDatabaseRole.COMPUTE),
            backfill_plan,
            recorded_at=datetime.now(timezone.utc),
            request_evidence_by_segment=provider.request_evidence_by_segment,
        )
    finally:
        pool.closeall()
    completed_at = datetime.now(timezone.utc)
    report = {
        **plan_report,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": (completed_at - started_at).total_seconds(),
        "control": control,
        "retained_fact_count": len(retained),
        "lineage_support_fact_count": len(lineage_support),
        "provider_fact_count": len(provider.facts),
        "logical_provider_requests": provider.logical_provider_requests,
        "provider_requests": provider.external_provider_requests,
        "provider_cache_hits": provider.cache_hits,
        "provider_response_bytes": provider.response_bytes,
        "coverage": coverage,
        "backfill": asdict(result),
    }
    if args.output:
        output = Path(args.output).resolve()
        if output.exists():
            raise ValueError("V2-10 backfill report already exists")
        write_report(output, json.loads(json.dumps(report, default=str)))
        report["output"] = str(output)
    print(json.dumps({
        "state": "COMPLETE",
        "target": args.database,
        "cohort_id": str(args.cohort_id),
        "facts": coverage["facts"],
        "chunks": result.chunks,
        "provider_requests": provider.external_provider_requests,
        "logical_provider_requests": provider.logical_provider_requests,
        "provider_cache_hits": provider.cache_hits,
        "provider_response_bytes": provider.response_bytes,
        "duration_seconds": report["duration_seconds"],
        "output": report.get("output"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
