#!/usr/bin/env python3
"""Read-only target-9 daily/hourly scanner warmup and lineage audit."""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from uuid import UUID

import exchange_calendars
import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_realtime_session_scheduler import (  # noqa: E402
    ADVANCED_SESSION_EQUITY_SYMBOLS,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import validate_target_database_v2  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    admin_settings,
    connect,
    read_only_transaction,
    require_new_workspace_report,
)


DAILY_REQUIRED_BARS = 210
HOURLY_REQUIRED_BARS = 200


_COVERAGE_SQL = """
WITH members AS (
    SELECT DISTINCT instrument.instrument_id,instrument.security_id
    FROM rt.cohorts AS cohort
    JOIN rt.subscription_members AS member USING(manifest_id)
    JOIN rt.instruments AS instrument USING(instrument_id)
    WHERE cohort.cohort_id=%s
      AND member.channel='AM'
      AND instrument.asset_class='EQUITY'
),
names AS (
    SELECT DISTINCT ON (members.instrument_id)
        members.instrument_id,revision.ticker
    FROM members
    JOIN ref.security_revisions AS revision
      ON revision.security_id=members.security_id
    ORDER BY members.instrument_id,revision.observed_at DESC,
             revision.revision_id DESC
)
SELECT names.ticker,current.interval,current.adjustment_basis,
       COUNT(*) AS bar_count,
       MIN(revision.session_date) AS first_session,
       MAX(revision.session_date) AS latest_session,
       MAX(revision.bar_end) AS latest_bar_end,
       ARRAY_AGG(DISTINCT revision.source_kind
                 ORDER BY revision.source_kind) AS source_kinds,
       ARRAY_AGG(DISTINCT revision.availability_mode
                 ORDER BY revision.availability_mode) AS availability_modes
FROM names
JOIN rt.bar_current AS current
  ON current.cohort_id=%s
 AND current.instrument_id=names.instrument_id
JOIN rt.bar_revisions AS revision
  ON revision.bar_revision_id=current.bar_revision_id
 AND revision.observed_at=current.observed_at
WHERE revision.is_final
  AND revision.session_scope='RTH'
  AND revision.session_date<=%s
  AND (
      (current.interval='1d' AND current.adjustment_basis='ADJUSTED')
      OR
      (current.interval='1h' AND current.adjustment_basis='RAW_UNADJUSTED')
  )
GROUP BY names.ticker,current.interval,current.adjustment_basis
ORDER BY names.ticker,current.interval
"""


_HOURLY_LINEAGE_SQL = """
WITH members AS (
    SELECT DISTINCT instrument.instrument_id,instrument.security_id
    FROM rt.cohorts AS cohort
    JOIN rt.subscription_members AS member USING(manifest_id)
    JOIN rt.instruments AS instrument USING(instrument_id)
    WHERE cohort.cohort_id=%s
      AND member.channel='AM'
      AND instrument.asset_class='EQUITY'
),
names AS (
    SELECT DISTINCT ON (members.instrument_id)
        members.instrument_id,revision.ticker
    FROM members
    JOIN ref.security_revisions AS revision
      ON revision.security_id=members.security_id
    ORDER BY members.instrument_id,revision.observed_at DESC,
             revision.revision_id DESC
),
parents AS (
    SELECT names.ticker,current.instrument_id,
           revision.bar_revision_id,revision.bar_start,revision.bar_end,
           revision.source_revision_ids
    FROM names
    JOIN rt.bar_current AS current
      ON current.cohort_id=%s
     AND current.instrument_id=names.instrument_id
     AND current.interval='1h'
     AND current.adjustment_basis='RAW_UNADJUSTED'
    JOIN rt.bar_revisions AS revision
      ON revision.bar_revision_id=current.bar_revision_id
     AND revision.observed_at=current.observed_at
    WHERE revision.is_final
      AND revision.session_scope='RTH'
      AND revision.session_date BETWEEN %s AND %s
)
SELECT parents.ticker,
       COUNT(DISTINCT parents.bar_revision_id) AS parent_count,
       COUNT(source_id) AS source_link_count,
       COUNT(DISTINCT parents.bar_revision_id) FILTER (
           WHERE CARDINALITY(parents.source_revision_ids)
                 <> EXTRACT(EPOCH FROM
                    (parents.bar_end-parents.bar_start))/300
       ) AS source_count_mismatch,
       COUNT(*) FILTER (
           WHERE source_id IS NULL
              OR source.bar_revision_id IS NULL
              OR source.interval<>'5m'
              OR source.adjustment_basis<>'RAW_UNADJUSTED'
              OR NOT source.is_final
              OR source.instrument_id<>parents.instrument_id
              OR source.bar_start<parents.bar_start
              OR source.bar_end>parents.bar_end
       ) AS invalid_source_links
FROM parents
LEFT JOIN LATERAL
     UNNEST(parents.source_revision_ids) AS source_id ON true
LEFT JOIN LATERAL (
    SELECT candidate.*
    FROM rt.bar_revisions AS candidate
    WHERE candidate.bar_revision_id=source_id
    ORDER BY candidate.observed_at DESC
    LIMIT 1
) AS source ON true
GROUP BY parents.ticker
ORDER BY parents.ticker
"""


def _rows(cursor, statement, parameters):
    cursor.execute(statement, parameters)
    columns = tuple(column.name for column in cursor.description)
    return tuple(
        dict(row)
        if hasattr(row, "keys")
        else dict(zip(columns, row))
        for row in cursor.fetchall()
    )


def _failures(coverage, lineage, *, through_session: date) -> tuple[str, ...]:
    by_ticker = {}
    for row in coverage:
        by_ticker.setdefault(str(row["ticker"]), {})[str(row["interval"])] = row
    lineage_by_ticker = {
        str(row["ticker"]): row
        for row in lineage
    }
    failures = []
    if tuple(sorted(by_ticker)) != tuple(sorted(ADVANCED_SESSION_EQUITY_SYMBOLS)):
        failures.append("TARGET9_MEMBERSHIP_CHANGED")
    for ticker in ADVANCED_SESSION_EQUITY_SYMBOLS:
        daily = by_ticker.get(ticker, {}).get("1d")
        hourly = by_ticker.get(ticker, {}).get("1h")
        hourly_lineage = lineage_by_ticker.get(ticker)
        if (
            daily is None
            or int(daily["bar_count"]) < DAILY_REQUIRED_BARS
            or daily["latest_session"] != through_session
            or list(daily["source_kinds"]) != ["NATIVE_REST"]
            or list(daily["availability_modes"])
            != ["HISTORICAL_RECONSTRUCTED"]
        ):
            failures.append(f"{ticker}:DAILY_WARMUP_OR_LINEAGE")
        if (
            hourly is None
            or int(hourly["bar_count"]) < HOURLY_REQUIRED_BARS
            or hourly["latest_session"] != through_session
            or list(hourly["source_kinds"]) != ["DERIVED"]
            or list(hourly["availability_modes"])
            != ["HISTORICAL_RECONSTRUCTED"]
            or hourly_lineage is None
            or int(hourly_lineage["parent_count"]) < HOURLY_REQUIRED_BARS
            or int(hourly_lineage["source_count_mismatch"]) != 0
            or int(hourly_lineage["invalid_source_links"]) != 0
        ):
            failures.append(f"{ticker}:HOURLY_WARMUP_OR_LINEAGE")
    return tuple(failures)


def build_report(cursor, control, *, cohort_id: UUID, through_session: date):
    calendar = exchange_calendars.get_calendar("XNYS")
    hourly_lineage_start = calendar.sessions_window(
        pd.Timestamp(through_session),
        -30,
    )[0].date()
    cursor.execute("SET LOCAL statement_timeout = '120s'")
    coverage = _rows(
        cursor,
        _COVERAGE_SQL,
        (str(cohort_id), str(cohort_id), through_session),
    )
    lineage = _rows(
        cursor,
        _HOURLY_LINEAGE_SQL,
        (
            str(cohort_id),
            str(cohort_id),
            hourly_lineage_start,
            through_session,
        ),
    )
    failures = _failures(
        coverage,
        lineage,
        through_session=through_session,
    )
    report = {
        "contract": "target9-scanner-warmup-lineage-audit-v1",
        "verdict": "PASS" if not failures else "FAIL",
        "cohort_id": str(cohort_id),
        "through_session": through_session,
        "daily_required_bars": DAILY_REQUIRED_BARS,
        "hourly_required_bars": HOURLY_REQUIRED_BARS,
        "hourly_lineage_start": hourly_lineage_start,
        "coverage": coverage,
        "hourly_lineage": lineage,
        "failures": failures,
        "controls": {
            "database": control["database"],
            "transaction_read_only": control["read_only"],
            "transaction_isolation": control["isolation"],
            "checked_at": control["checked_at"],
            "provider_requests": 0,
            "database_writes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    return json.loads(json.dumps(report, default=str))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--through-session", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=BACKEND_DIR / ".env",
    )
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
    )
    args.report_path = require_new_workspace_report(
        args.report_path,
        WORKSPACE_DIR,
    )
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = admin_settings(args.env_file)
    with closing(connect(
        args.database,
        settings,
        application_name="audit_realtime_v2_target9_scanner_warmup",
    )) as connection:
        with read_only_transaction(
            connection,
            args.database,
        ) as (cursor, control):
            report = build_report(
                cursor,
                control,
                cohort_id=args.cohort_id,
                through_session=args.through_session,
            )
    write_report(args.report_path, report)
    print(json.dumps(report, default=str, sort_keys=True))
    return 0 if report["verdict"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
