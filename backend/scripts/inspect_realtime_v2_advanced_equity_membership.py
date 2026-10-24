#!/usr/bin/env python3
"""Read-only diagnosis of one Advanced Equity AM ingress membership."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_ingress_repository import (  # noqa: E402
    load_advanced_ingress_runtime_plan_cursor,
)
from realtime.contracts import (  # noqa: E402
    EQUITY_VALIDATION_UNIVERSE,
    canonical_sha256,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--plan-id", type=UUID, required=True)
    parser.add_argument("--observed-at", type=datetime.fromisoformat)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    args.observed_at = (
        datetime.now(UTC)
        if args.observed_at is None
        else args.observed_at.astimezone(UTC)
    )
    args.output = args.output.resolve()
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.output.exists()
    ):
        parser.error("Advanced Equity membership inspection is invalid")
    return args


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(values.get("REALTIME_V2_APP_DSN") or "").strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def main(argv=None):
    args = parse_args(argv)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            plan = load_advanced_ingress_runtime_plan_cursor(
                unit.cursor,
                args.plan_id,
            )
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT member.instrument_id,instrument.security_id::text,"
                "member.channel,reference.ticker,reference.active,"
                "reference.effective_from,reference.effective_to,"
                "reference.observed_at,reference.revision_id::text "
                "FROM rt.subscription_members AS member "
                "JOIN rt.instruments AS instrument USING(instrument_id) "
                "LEFT JOIN LATERAL ("
                "SELECT revision.ticker,revision.active,"
                "revision.effective_from,revision.effective_to,"
                "revision.observed_at,revision.revision_id "
                "FROM ref.security_revisions AS revision "
                "WHERE revision.security_id=instrument.security_id "
                "AND revision.observed_at<=%s "
                "ORDER BY revision.observed_at DESC,"
                "revision.revision_id DESC LIMIT 1"
                ") AS reference ON true "
                "WHERE member.manifest_id=%s AND member.channel='AM' "
                "ORDER BY member.instrument_id",
                (args.observed_at, str(plan.manifest_id)),
            )
            members = [dict(row) for row in unit.cursor.fetchall()]
        expected = set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        actual = {
            str(row["ticker"])
            for row in members
            if row["ticker"] is not None
        }
        current = {
            str(row["ticker"])
            for row in members
            if row["ticker"] is not None
            and row["active"] is True
            and row["effective_from"] <= args.observed_at
            and (
                row["effective_to"] is None
                or row["effective_to"] > args.observed_at
            )
        }
        report = {
            "contract": (
                "advanced-equity-ingress-membership-inspection-v1"
            ),
            "mode": "READ_ONLY",
            "observed_at_utc": args.observed_at,
            "plan_id": str(plan.plan_id),
            "cohort_id": str(plan.cohort_id),
            "manifest_id": str(plan.manifest_id),
            "universe_target": plan.universe_target,
            "expected_symbols": sorted(expected),
            "manifest_symbols": sorted(actual),
            "current_symbols": sorted(current),
            "missing_manifest_symbols": sorted(expected - actual),
            "missing_current_symbols": sorted(expected - current),
            "unexpected_symbols": sorted(actual - expected),
            "members": members,
            "status": (
                "PASS"
                if current == expected
                and len(members) == plan.universe_target
                else "BLOCKED"
            ),
            "effects": {
                "provider_requests": 0,
                "provider_websocket": 0,
                "database_writes": 0,
                "service_actions": 0,
            },
        }
        report["semantic_sha256"] = canonical_sha256(report)
        encoded = (
            json.dumps(
                report,
                indent=2,
                sort_keys=True,
                default=str,
            )
            + "\n"
        ).encode("utf-8")
        with args.output.open("xb") as stream:
            stream.write(encoded)
        print(json.dumps({
            "status": report["status"],
            "missing_manifest_symbols": (
                report["missing_manifest_symbols"]
            ),
            "missing_current_symbols": (
                report["missing_current_symbols"]
            ),
            "semantic_sha256": report["semantic_sha256"],
            "file_sha256": hashlib.sha256(encoded).hexdigest(),
            "output": str(args.output),
        }, indent=2, sort_keys=True))
        return 0 if report["status"] == "PASS" else 1
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
