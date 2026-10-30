#!/usr/bin/env python3
"""Audit exact QQQ/SPY raw daily membership for one V3 H cutoff."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import date, datetime, timezone
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
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)


TICKERS = ("QQQ", "SPY")
SESSION_COUNT = 15


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--known-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.known_at.tzinfo is None
        or args.known_at.utcoffset() is None
        or args.output.exists()
    ):
        parser.error("V3 H membership audit scope is invalid")
    args.known_at = args.known_at.astimezone(timezone.utc)
    args.output = args.output.resolve()
    return args


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
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


def _expected_sessions(session_date: date) -> tuple[date, ...]:
    calendar = OptionExchangeCalendar()
    prior = calendar.previous_session(session_date)
    values = [prior]
    while len(values) < SESSION_COUNT:
        values.append(calendar.previous_session(values[-1]))
    return tuple(reversed(values))


def _row(raw) -> dict[str, object]:
    return {
        key: (
            value.isoformat()
            if isinstance(value, (date, datetime))
            else [str(item) for item in value]
            if isinstance(value, list)
            else str(value)
            if isinstance(value, UUID)
            else value
        )
        for key, value in dict(raw).items()
    }


def _summarize(rows, expected):
    result = {}
    for ticker in TICKERS:
        selected = tuple(
            row for row in rows if row["ticker"] == ticker
        )
        counts = Counter(row["session_date"] for row in selected)
        result[ticker] = {
            "member_count": len(selected),
            "expected_member_count": len(expected),
            "missing_sessions": [
                value.isoformat() for value in expected
                if counts[value] == 0
            ],
            "duplicate_sessions": {
                value.isoformat(): count
                for value, count in sorted(counts.items())
                if count > 1
            },
            "unexpected_sessions": [
                value.isoformat() for value in sorted(counts)
                if value not in expected
            ],
            "members": [_row(row) for row in selected],
        }
    return result


def main(argv=None) -> int:
    args = parse_args(argv)
    expected = _expected_sessions(args.session_date)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT revision.ticker,revision.session_date,"
                "current.instrument_id,current.provider_tier,current.interval,"
                "current.bar_start,current.session_scope,"
                "current.adjustment_basis,"
                "current.bar_revision_id::text,current.observed_at,"
                "revision.security_id::text,revision.source_kind,"
                "revision.availability_mode,revision.is_final,"
                "revision.bar_end,revision.created_at,"
                "revision.replay_available_at,"
                "revision.provider_published_at,"
                "revision.payload_sha256,revision.quality_codes,"
                "revision.ingestion_segment_id::text,"
                "revision.source_revision_ids::text[] "
                "FROM rt.bar_current AS current "
                "JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.observed_at=current.observed_at "
                "WHERE current.cohort_id=%s "
                "AND current.interval='1d' "
                "AND current.adjustment_basis='RAW_UNADJUSTED' "
                "AND revision.is_final "
                "AND revision.ticker=ANY(%s) "
                "AND revision.session_date=ANY(%s) "
                "AND revision.observed_at<=%s "
                "ORDER BY revision.ticker,revision.session_date,"
                "revision.source_kind,current.observed_at",
                (
                    str(args.cohort_id),
                    list(TICKERS),
                    list(expected),
                    args.known_at,
                ),
            )
            rows = tuple(unit.cursor.fetchall())
    finally:
        pool.closeall()
    summary = _summarize(rows, expected)
    status = (
        "PASS"
        if all(
            value["member_count"] == value["expected_member_count"]
            and not value["missing_sessions"]
            and not value["duplicate_sessions"]
            and not value["unexpected_sessions"]
            for value in summary.values()
        )
        else "BLOCKED"
    )
    report = {
        "contract": "advanced-options-v3-history-membership-audit-v1",
        "mode": "READ_ONLY",
        "status": status,
        "database": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "known_at_utc": args.known_at.isoformat(),
        "availability_modes": "ALL_CURRENT",
        "expected_sessions": [
            value.isoformat() for value in expected
        ],
        "symbols": summary,
        "effects": {
            "provider_requests": 0,
            "production_database_writes": 0,
            "service_actions": 0,
            "publication": 0,
            "stored_read_changes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    body = (
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({
        "status": status,
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(body).hexdigest(),
        "output": str(args.output),
        "duplicates": {
            ticker: value["duplicate_sessions"]
            for ticker, value in summary.items()
        },
    }, indent=2, sort_keys=True))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
