#!/usr/bin/env python3
"""Provider-free replacement of retained current weekly pointers from exact adjusted daily bars."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_bulk_backfill import (  # noqa: E402
    build_equity_bulk_backfill_plan,
    execute_equity_bulk_backfill,
)
from realtime.equity_interval_derivation import derive_realtime_interval_facts  # noqa: E402
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan  # noqa: E402
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.preflight import ProviderTier  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from realtime.ticker_projection import _fact, _security  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("weekly repair targets only stocks_realtime_v2")
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if args.execute and not args.authorization_reference.strip():
        parser.error("--execute requires --authorization-reference")
    return args


def _pool(dsn):
    return ThreadedConnectionPool(1, 2, dsn=dsn)


def main(argv=None):
    args = parse_args(argv)
    portal_plan = build_realtime_equity_portal_history_plan(args.session_date)
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "interval": "1wk",
        "provider_requests": 0,
    }
    if not args.execute:
        print(json.dumps({**plan, "database_writes": 0}, sort_keys=True))
        return 0
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("weekly repair requires REALTIME_V2_APP_DSN")
    pool = _pool(dsn)
    observed_at = datetime.now(timezone.utc)
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=300_000,
            ),
        )
        provider.verify()
        with provider.transaction(read_only=True, dict_cursor=True) as unit:
            unit.cursor.execute(
                "SELECT DISTINCT ON (reference.ticker) reference.*,"
                "reference.revision_id AS security_revision_id "
                "FROM ref.security_revisions AS reference "
                "WHERE reference.ticker=ANY(%s) AND reference.active "
                "AND reference.effective_from<=%s AND reference.effective_to>%s "
                "ORDER BY reference.ticker,reference.observed_at DESC,reference.revision_id DESC",
                (list(EQUITY_VALIDATION_UNIVERSE.equity_tickers), observed_at, observed_at),
            )
            securities = {
                row["ticker"]: _security(row)
                for row in unit.cursor.fetchall()
            }
            unit.cursor.execute(
                "SELECT revision.bar_revision_id,current.cohort_id,revision.security_id,"
                "revision.instrument_id,revision.ticker,revision.interval,revision.session_date,"
                "revision.session_scope,revision.adjustment_basis,revision.bar_start,revision.bar_end,"
                "revision.open_price AS open,revision.high_price AS high,revision.low_price AS low,"
                "revision.close_price AS close,revision.volume,revision.vwap,revision.transaction_count,"
                "revision.source_kind,revision.availability_mode,revision.is_final,revision.observed_at,"
                "revision.replay_available_at,revision.provider_published_at,revision.ingestion_segment_id,"
                "revision.source_revision_ids,revision.payload_sha256 "
                "FROM rt.bar_current AS current JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id AND revision.observed_at=current.observed_at "
                "WHERE current.cohort_id=%s AND revision.ticker=ANY(%s) "
                "AND revision.interval='1d' AND revision.adjustment_basis='ADJUSTED' "
                "AND revision.session_date=ANY(%s) "
                "ORDER BY revision.ticker,revision.bar_start",
                (
                    str(args.cohort_id),
                    list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
                    list(portal_plan.adjusted_sessions),
                ),
            )
            inputs = SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER)
            daily = tuple(_fact(row, inputs) for row in unit.cursor.fetchall())
        if (
            set(securities) != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
            or len(daily) != 283 * len(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        ):
            raise ValueError("weekly repair inputs are incomplete")
        repaired = []
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
            ticker_daily = tuple(
                fact for fact in daily if fact.revision.ticker == ticker
            )
            segment = uuid5(
                NAMESPACE_URL,
                f"v2-10-weekly-repair:{args.cohort_id}:{ticker}:{args.session_date}",
            )
            repaired.extend(derive_realtime_interval_facts(
                securities[ticker],
                ticker_daily,
                target_interval="1wk",
                observed_at=observed_at,
                ingestion_segment_id=segment,
            ))
        candidate_count = len(repaired)
        with provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT bar_revision_id FROM rt.bar_catalog "
                "WHERE bar_revision_id=ANY(%s::uuid[])",
                ([str(fact.revision.bar_revision_id) for fact in repaired],),
            )
            existing = {str(row[0]) for row in unit.cursor.fetchall()}
        repaired = [
            fact for fact in repaired
            if str(fact.revision.bar_revision_id) not in existing
        ]
        if not repaired:
            raise ValueError("weekly repair produced no new immutable identities")
        backfill_plan = build_equity_bulk_backfill_plan(
            repaired,
            cohort_id=args.cohort_id,
            tickers=EQUITY_VALIDATION_UNIVERSE.equity_tickers,
        )
        result = execute_equity_bulk_backfill(
            V2WorkerDatabase(provider, RealtimeDatabaseRole.INGRESS),
            V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
            backfill_plan,
            recorded_at=observed_at,
        )
        report = {
            **plan,
            "status": "COMPLETE",
            "daily_inputs": len(daily),
            "weekly_candidates": candidate_count,
            "weekly_existing": len(existing),
            "weekly_revisions": len(repaired),
            "backfill": {
                "chunks": result.chunks,
                "facts": result.facts,
                "jobs_completed": result.jobs_completed,
                "bars_materialized": result.bars_materialized,
                "external_provider_requests": result.external_provider_requests,
            },
        }
        write_report(args.report_path.resolve(), report)
        print(json.dumps({
            "state": "COMPLETE",
            "weekly_revisions": len(repaired),
            "chunks": result.chunks,
            "provider_requests": 0,
            "report_path": str(args.report_path.resolve()),
        }, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
