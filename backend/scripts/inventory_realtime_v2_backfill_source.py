#!/usr/bin/env python3
"""Inventory retained V1 bars for the exact V2 nine-ticker history contract.

The source is always stocks_realtime_v1 and the transaction is REPEATABLE READ, READ ONLY.
No provider is constructed or called and no database row is written. Coverage is checked per
ticker, interval, adjustment basis and expected XNYS session, including derived-bar lineage.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from contextlib import closing
from datetime import date
from pathlib import Path

import exchange_calendars
from psycopg2.extras import RealDictCursor

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan  # noqa: E402
from realtime.migrations import EXPECTED_DATABASE_NAME, validate_target_database  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402

STATEMENT_TIMEOUT = "120s"


def _session_bar_count(session_date: date, interval: str) -> int:
    if interval in {"1d", "1wk", "1mo"}:
        return 1
    calendar = exchange_calendars.get_calendar("XNYS")
    minutes = int(
        (calendar.session_close(str(session_date)) - calendar.session_open(str(session_date))).total_seconds() // 60
    )
    width = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}[interval]
    return math.ceil(minutes / width) if interval == "1h" else minutes // width


def expected_session_counts(session_date: date) -> dict[tuple[str, str], dict[date, int]]:
    plan = build_realtime_equity_portal_history_plan(session_date)
    sessions = {
        ("1d", "ADJUSTED"): plan.adjusted_sessions,
        ("1wk", "ADJUSTED"): plan.weekly_sessions,
        ("1mo", "ADJUSTED"): plan.monthly_sessions,
        ("1m", "RAW_UNADJUSTED"): plan.one_minute_sessions,
        ("5m", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("15m", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("30m", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("1h", "RAW_UNADJUSTED"): plan.raw_sessions,
        ("1d", "RAW_UNADJUSTED"): plan.raw_sessions,
    }
    return {
        family: {day: _session_bar_count(day, family[0]) for day in days}
        for family, days in sessions.items()
    }


def coverage_report(rows, *, session_date: date) -> dict:
    expected = expected_session_counts(session_date)
    observed = defaultdict(dict)
    lineage = defaultdict(lambda: {
        "derived_rows": 0,
        "derived_without_sources": 0,
        "native_without_segment": 0,
        "missing_source_revisions": 0,
    })
    for row in rows:
        ticker = str(row["ticker"])
        family = (str(row["interval"]), str(row["adjustment_basis"]))
        observed[(ticker, family)][row["session_date"]] = int(row["bar_count"])
        facts = lineage[ticker]
        for key in facts:
            facts[key] += int(row[key])
    ticker_reports = {}
    for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
        families = {}
        for family, sessions in expected.items():
            actual = observed[(ticker, family)]
            missing = [day.isoformat() for day in sessions if actual.get(day, 0) == 0]
            incomplete = [
                {
                    "session_date": day.isoformat(),
                    "actual_count": actual[day],
                    "expected_count": count,
                }
                for day, count in sessions.items()
                if 0 < actual.get(day, 0) != count
            ]
            scoped_rows = sum(actual.get(day, 0) for day in sessions)
            families[f"{family[0]}:{family[1]}"] = {
                "expected_rows": sum(sessions.values()),
                "actual_rows": scoped_rows,
                "missing_sessions": missing,
                "incomplete_sessions": incomplete,
                "extra_sessions": sorted(day.isoformat() for day in set(actual) - set(sessions)),
            }
        lineage_ok = not any(
            lineage[ticker][key]
            for key in ("derived_without_sources", "native_without_segment", "missing_source_revisions")
        )
        complete = lineage_ok and all(
            not family["missing_sessions"]
            and not family["incomplete_sessions"]
            and family["actual_rows"] == family["expected_rows"]
            for family in families.values()
        )
        ticker_reports[ticker] = {
            "complete": complete,
            "families": families,
            "lineage": dict(lineage[ticker]),
        }
    return {
        "complete": all(report["complete"] for report in ticker_reports.values()),
        "expected_rows": sum(
            sum(sessions.values()) for sessions in expected.values()
        ) * len(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
        "actual_rows": sum(
            family["actual_rows"]
            for report in ticker_reports.values()
            for family in report["families"].values()
        ),
        "source_rows_in_date_envelope": sum(int(row["bar_count"]) for row in rows),
        "tickers": ticker_reports,
    }


def _candidate_cohorts(cursor, *, start_date: date, end_date: date) -> tuple[dict, ...]:
    cursor.execute(
        "SELECT current.cohort_id::text AS cohort_id, COUNT(*) AS retained_rows, "
        "COUNT(DISTINCT revision.ticker) AS ticker_count, "
        "MIN(revision.observed_at) AS first_observed_at, MAX(revision.observed_at) AS last_observed_at "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.cohort_id=current.cohort_id AND revision.provider_tier=current.provider_tier "
        "WHERE revision.ticker=ANY(%s) AND revision.session_date BETWEEN %s AND %s "
        "AND revision.is_final AND current.provider_tier='MASSIVE_DEVELOPER' "
        "GROUP BY current.cohort_id ORDER BY ticker_count DESC, retained_rows DESC, current.cohort_id",
        (list(EQUITY_VALIDATION_UNIVERSE.equity_tickers), start_date, end_date),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _coverage_rows(cursor, *, cohort_id: str, start_date: date, end_date: date) -> tuple[dict, ...]:
    cursor.execute(
        "SELECT revision.ticker,revision.interval,revision.adjustment_basis,revision.session_date,"
        "COUNT(*) AS bar_count,"
        "COUNT(*) FILTER (WHERE revision.source_kind='DERIVED') AS derived_rows,"
        "COUNT(*) FILTER (WHERE revision.source_kind='DERIVED' "
        "AND cardinality(revision.source_revision_ids)=0) AS derived_without_sources,"
        "COUNT(*) FILTER (WHERE revision.source_kind='NATIVE_REST' "
        "AND revision.ingestion_segment_id IS NULL) AS native_without_segment,"
        "COALESCE(SUM((SELECT COUNT(*) FROM unnest(revision.source_revision_ids) AS source_id "
        "WHERE NOT EXISTS (SELECT 1 FROM rt.bar_revisions AS source "
        "WHERE source.bar_revision_id=source_id))),0) AS missing_source_revisions "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.cohort_id=current.cohort_id AND revision.provider_tier=current.provider_tier "
        "WHERE current.cohort_id=%s AND current.provider_tier='MASSIVE_DEVELOPER' "
        "AND revision.ticker=ANY(%s) AND revision.session_date BETWEEN %s AND %s "
        "AND revision.is_final "
        "GROUP BY revision.ticker,revision.interval,revision.adjustment_basis,revision.session_date "
        "ORDER BY revision.ticker,revision.interval,revision.adjustment_basis,revision.session_date",
        (cohort_id, list(EQUITY_VALIDATION_UNIVERSE.equity_tickers), start_date, end_date),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _reference_rows(cursor) -> tuple[dict, ...]:
    cursor.execute(
        "SELECT revision.ticker,COUNT(DISTINCT revision.security_id) AS security_count,"
        "COUNT(DISTINCT instrument.instrument_id) AS instrument_count,"
        "MIN(revision.effective_from) AS first_effective_at,MAX(revision.observed_at) AS last_observed_at "
        "FROM ref.security_revisions AS revision "
        "JOIN rt.instruments AS instrument ON instrument.security_id=revision.security_id "
        "AND instrument.asset_class='EQUITY' "
        "WHERE revision.ticker=ANY(%s) GROUP BY revision.ticker ORDER BY revision.ticker",
        (list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),),
    )
    return tuple(dict(row) for row in cursor.fetchall())


def _control_report(cursor, *, cohort_ids: list[str]) -> dict:
    cursor.execute(
        "SELECT cohort.cohort_id::text,cohort.manifest_id::text,cohort.effective_at,cohort.mode,"
        "manifest.session_date,manifest.effective_at AS manifest_effective_at,manifest.profile_id::text,"
        "manifest.universe_sha256,profile.provider_tier,profile.status AS profile_status,"
        "profile.checked_at,profile.expires_at,profile.provider_requests,profile.action_authorized "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
        "JOIN rt.capability_profiles AS profile USING (profile_id) "
        "WHERE cohort.cohort_id::text=ANY(%s) ORDER BY cohort.cohort_id",
        (cohort_ids,),
    )
    cohorts = tuple(dict(row) for row in cursor.fetchall())
    cursor.execute(
        "SELECT seed.seed_id::text,seed.seed_sha256,seed.session_date,seed.status,"
        "seed.created_at,seed.effective_at,seed.valid_until,current.scope,current.generation "
        "FROM rt.seed_manifests AS seed "
        "LEFT JOIN rt.seed_current AS current USING (seed_id) "
        "WHERE seed.universe_sha256=%s ORDER BY current.generation DESC NULLS LAST,"
        "seed.effective_at DESC LIMIT 5",
        (EQUITY_VALIDATION_UNIVERSE.sha256,),
    )
    seeds = tuple(dict(row) for row in cursor.fetchall())
    return {"cohorts": cohorts, "seeds": seeds}


def build_report(cursor, *, session_date: date) -> dict:
    expected = expected_session_counts(session_date)
    all_days = [day for sessions in expected.values() for day in sessions]
    start_date, end_date = min(all_days), max(all_days)
    candidates = _candidate_cohorts(cursor, start_date=start_date, end_date=end_date)
    cohort_reports = []
    for candidate in candidates:
        coverage = coverage_report(
            _coverage_rows(
                cursor,
                cohort_id=candidate["cohort_id"],
                start_date=start_date,
                end_date=end_date,
            ),
            session_date=session_date,
        )
        cohort_reports.append({**candidate, "coverage": coverage})
    references = _reference_rows(cursor)
    exact_reference = (
        {row["ticker"] for row in references} == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        and all(int(row["security_count"]) == 1 and int(row["instrument_count"]) == 1 for row in references)
    )
    complete = [row for row in cohort_reports if row["coverage"]["complete"]]
    control = _control_report(
        cursor,
        cohort_ids=[row["cohort_id"] for row in candidates],
    )
    return {
        "status": "SUFFICIENT" if exact_reference and len(complete) == 1 else "INSUFFICIENT",
        "database": EXPECTED_DATABASE_NAME,
        "session_date": session_date.isoformat(),
        "universe_version": EQUITY_VALIDATION_UNIVERSE.version,
        "universe_sha256": EQUITY_VALIDATION_UNIVERSE.sha256,
        "tickers": list(EQUITY_VALIDATION_UNIVERSE.equity_tickers),
        "required_start_date": start_date.isoformat(),
        "required_end_date": end_date.isoformat(),
        "reference_exact": exact_reference,
        "reference_rows": references,
        "complete_cohort_ids": [row["cohort_id"] for row in complete],
        "cohorts": cohort_reports,
        "control": control,
        "provider_requests": 0,
        "database_writes": 0,
    }


def gap_summary(report: dict) -> tuple[dict, ...]:
    grouped = defaultdict(list)
    for cohort in report["cohorts"]:
        for ticker, ticker_report in cohort["coverage"]["tickers"].items():
            for family, coverage in ticker_report["families"].items():
                if coverage["missing_sessions"] or coverage["incomplete_sessions"]:
                    missing = tuple(coverage["missing_sessions"])
                    key = (
                        cohort["cohort_id"],
                        family,
                        missing,
                        tuple(
                            (
                                row["session_date"],
                                row["actual_count"],
                                row["expected_count"],
                            )
                            for row in coverage["incomplete_sessions"]
                        ),
                    )
                    grouped[key].append(ticker)
    summaries = []
    for (cohort_id, family, missing, incomplete), tickers in grouped.items():
        summaries.append({
            "cohort_id": cohort_id,
            "family": family,
            "tickers": sorted(tickers),
            "missing_session_count": len(missing),
            "missing_first": missing[0] if missing else None,
            "missing_last": missing[-1] if missing else None,
            "missing_sessions": list(missing) if len(missing) <= 10 else None,
            "incomplete_sessions": [
                {"session_date": day, "actual_count": actual, "expected_count": expected}
                for day, actual, expected in incomplete
            ],
        })
    return tuple(sorted(summaries, key=lambda row: (row["family"], row["cohort_id"])))


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    validate_target_database(args.database, args.confirm_database_name)
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = admin_settings(BACKEND_DIR / ".env")
    with closing(connect(args.database, settings, application_name="inventory_v2_backfill_source")) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT set_config('statement_timeout', %s, true)", (STATEMENT_TIMEOUT,))
                cursor.execute(
                    "SELECT current_database()::text AS database,"
                    "current_setting('transaction_read_only') AS read_only,"
                    "current_setting('transaction_isolation') AS isolation"
                )
                identity = cursor.fetchone()
                if (
                    identity["database"],
                    identity["read_only"],
                    identity["isolation"],
                ) != (EXPECTED_DATABASE_NAME, "on", "repeatable read"):
                    raise RuntimeError("retained-source database/read-only identity mismatch")
                report = build_report(cursor, session_date=args.session_date)
        finally:
            connection.rollback()
    if args.output:
        output = Path(args.output).resolve()
        if output.exists():
            raise ValueError("retained-source inventory output already exists")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, default=str, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "state": "COMPLETE",
        "status": report["status"],
        "database": report["database"],
        "session_date": report["session_date"],
        "complete_cohort_ids": report["complete_cohort_ids"],
        "candidate_cohorts": len(report["cohorts"]),
        "reference_exact": report["reference_exact"],
        "control": report["control"],
        "gaps": gap_summary(report),
        "output": str(Path(args.output).resolve()) if args.output else None,
        "provider_requests": 0,
        "database_writes": 0,
    }, default=str, sort_keys=True))
    return 0 if report["status"] == "SUFFICIENT" else 2


if __name__ == "__main__":
    raise SystemExit(main())
