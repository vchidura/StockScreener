#!/usr/bin/env python3
"""Copy one exact adjusted-daily session from retained V1 into V2."""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_bar_adapter import RealtimeEquityBarFact  # noqa: E402
from realtime.equity_bar_repository import (  # noqa: E402
    persist_realtime_equity_bars_cursor,
)
from realtime.equity_interval_finalizer import _bar_revision  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_DATABASE_NAME,
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database,
    validate_target_database_v2,
)
from realtime.preflight import ProviderTier  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    admin_settings,
    connect,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", required=True)
    parser.add_argument("--confirm-source-database-name", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database(
        args.source_database,
        args.confirm_source_database_name,
    )
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if (
        args.source_database != EXPECTED_DATABASE_NAME
        or args.database != EXPECTED_V2_DATABASE_NAME
    ):
        parser.error("adjusted history copy requires V1 source and V2 target")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def _source_rows(args, settings):
    with closing(
        connect(
            args.source_database,
            settings,
            application_name="repair_v2_adjusted_from_v1",
        )
    ) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            cursor.execute("SET LOCAL statement_timeout='30s'")
            cursor.execute(
                "SELECT revision.*,current.cohort_id,"
                "current.provider_tier "
                "FROM rt.bar_current AS current "
                "JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.cohort_id=current.cohort_id "
                "AND revision.provider_tier=current.provider_tier "
                "WHERE current.cohort_id=%s "
                "AND current.provider_tier='MASSIVE_DEVELOPER' "
                "AND revision.ticker=ANY(%s) "
                "AND revision.interval='1d' "
                "AND revision.adjustment_basis='ADJUSTED' "
                "AND revision.session_date=%s AND revision.is_final "
                "ORDER BY revision.ticker",
                (
                    str(args.cohort_id),
                    list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
                    args.session_date,
                ),
            )
            rows = tuple(dict(row) for row in cursor.fetchall())
        connection.rollback()
    return rows


def _validate_source_rows(rows):
    tickers = tuple(row["ticker"] for row in rows)
    if (
        len(rows) != len(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        or set(tickers)
        != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        or len(set(tickers)) != len(tickers)
        or any(
            row["provider_tier"] != "MASSIVE_DEVELOPER"
            or row["interval"] != "1d"
            or row["adjustment_basis"] != "ADJUSTED"
            or not row["is_final"]
            for row in rows
        )
    ):
        raise ValueError(
            "retained adjusted-daily source is not exact: "
            f"rows={len(rows)} tickers={sorted(set(tickers))} "
            f"tiers={sorted({row['provider_tier'] for row in rows})} "
            f"intervals={sorted({row['interval'] for row in rows})} "
            f"bases={sorted({row['adjustment_basis'] for row in rows})} "
            f"final={sorted({bool(row['is_final']) for row in rows})}"
        )


def _target_members(provider, cohort_id):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT instrument.instrument_id,instrument.security_id "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "WHERE cohort.cohort_id=%s AND member.channel='AM' "
            "AND instrument.asset_class='EQUITY' "
            "ORDER BY instrument.instrument_id",
            (str(cohort_id),),
        )
        return tuple(unit.cursor.fetchall())


def _facts(rows, cohort_id, target_members):
    expected = {
        (int(instrument_id), str(security_id))
        for instrument_id, security_id in target_members
    }
    actual = {
        (int(row["instrument_id"]), str(row["security_id"]))
        for row in rows
    }
    if actual != expected or len(actual) != len(rows):
        raise ValueError("retained adjusted-daily source changed target membership")
    return tuple(
        RealtimeEquityBarFact(
            cohort_id=cohort_id,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            instrument_id=int(row["instrument_id"]),
            revision=_bar_revision(row),
        )
        for row in rows
    )


def main(argv=None):
    args = parse_args(argv)
    settings = admin_settings(BACKEND_DIR / ".env")
    rows = _source_rows(args, settings)
    _validate_source_rows(rows)
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("adjusted history copy requires REALTIME_V2_APP_DSN")
    pool = SimpleConnectionPool(1, 1, dsn=dsn, options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode=SECURITY_MODE_HARDENED,
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=30_000,
            ),
        )
        provider.verify()
        members = _target_members(provider, args.cohort_id)
        facts = _facts(rows, args.cohort_id, members)
        identity = canonical_sha256({
            "version": "v2-adjusted-daily-retained-copy-v1",
            "source_database": args.source_database,
            "target_database": args.database,
            "cohort_id": args.cohort_id,
            "session_date": args.session_date,
            "authorization_reference": args.authorization_reference,
            "revisions": tuple(
                (
                    fact.instrument_id,
                    fact.revision.ticker,
                    fact.revision.bar_revision_id,
                    fact.revision.payload_sha256,
                    fact.revision.system_observed_at,
                )
                for fact in facts
            ),
        })
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT bar_revision_id FROM rt.bar_catalog "
                "WHERE bar_revision_id=ANY(%s::uuid[])",
                (
                    [
                        str(fact.revision.bar_revision_id)
                        for fact in facts
                    ],
                ),
            )
            existing = {
                str(row[0]) for row in unit.cursor.fetchall()
            }
        copied = 0
        if args.execute:
            with provider.transaction(APP_ROLE) as unit:
                persist_realtime_equity_bars_cursor(
                    unit.cursor,
                    facts,
                )
            copied = len(facts) - len(existing)
        report = {
            "status": "COMPLETE",
            "mode": "EXECUTE" if args.execute else "PLAN",
            "source_database": args.source_database,
            "database": args.database,
            "cohort_id": str(args.cohort_id),
            "session_date": str(args.session_date),
            "tickers": list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
            "source_revisions": len(facts),
            "new_revisions": int(copied),
            "preexisting_revisions": len(existing),
            "identity_sha256": identity,
            "provider_requests": 0,
            "database_writes": int(bool(args.execute)),
            "release_advanced": False,
        }
        write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
