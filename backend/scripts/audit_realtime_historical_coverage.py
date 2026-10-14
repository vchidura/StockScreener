#!/usr/bin/env python3
"""Read-only V2 historical coverage audit for the realtime equity universe.

Reports, per ticker, whether reference identity, corporate actions, earnings, open interest, the option
catalog and the retained current bars (rt.bar_current with its catalog) cover the required history, plus the
global FOMC/treasury/seed references. One REPEATABLE READ read-only transaction with a statement timeout;
no provider request and no write. Targets stocks_realtime_v2 or an explicit test_realtime_v2_ database
(--allow-disposable).

The v1 `immutable_selection` section (option selection audits and decoded staged forward packages) is not ported:
it read v1 forward artifacts that no longer exist. Every other v1 report key keeps its name.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

import exchange_calendars

BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import DEVELOPER_FORWARD_UNIVERSE  # noqa: E402
from realtime.equity_portal_history import REALTIME_EQUITY_PORTAL_HISTORY_POLICY  # noqa: E402
from realtime.migrations import validate_target_database_v2  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    admin_settings, connect, query_rows, read_only_transaction, require_new_workspace_report,
)

_POLICY = REALTIME_EQUITY_PORTAL_HISTORY_POLICY
BAR_FAMILIES = (
    ("raw_final_1m", "1m", "RAW_UNADJUSTED", "RTH", _POLICY.native_one_minute_sessions),
    ("raw_final_30m", "30m", "RAW_UNADJUSTED", "RTH", _POLICY.raw_intraday_sessions),
    ("raw_final_daily", "1d", "RAW_UNADJUSTED", None, _POLICY.raw_intraday_sessions),
    ("adjusted_final_daily", "1d", "ADJUSTED", None, _POLICY.adjusted_daily_sessions),
)
_SECURITY_MAP = "WITH security_map AS (SELECT DISTINCT security_id,ticker FROM ref.security_revisions WHERE ticker=ANY(%s)) "
# Current final bars of the requested tickers inside the date window; the cohort filter is optional.
_FINAL_BARS = (
    "FROM rt.bar_current AS bar "
    "JOIN rt.bar_catalog AS catalog ON catalog.bar_revision_id=bar.bar_revision_id AND catalog.cohort_id=bar.cohort_id "
    "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=bar.bar_revision_id AND revision.observed_at=bar.observed_at "
    "JOIN rt.instruments AS instrument ON instrument.instrument_id=bar.instrument_id "
    "JOIN security_map AS member ON member.security_id=instrument.security_id "
    "WHERE revision.is_final AND catalog.session_date BETWEEN %s AND %s "
    "AND (%s::uuid IS NULL OR bar.cohort_id=%s::uuid) "
)


def _iso(value):
    return value.isoformat() if value is not None else None


def _compact_dates(values) -> tuple[dict, ...]:
    ordered = tuple(sorted(set(values)))
    if not ordered:
        return ()
    ranges = []
    first = previous = ordered[0]
    for value in ordered[1:]:
        if (value - previous).days <= 3:
            previous = value
            continue
        ranges.append({"start": first.isoformat(), "end": previous.isoformat()})
        first = previous = value
    ranges.append({"start": first.isoformat(), "end": previous.isoformat()})
    return tuple(ranges)


def _expected_sessions(start_date: date, end_date: date) -> tuple[date, ...]:
    calendar = exchange_calendars.get_calendar("XNYS")
    return tuple(value.date() for value in calendar.sessions_in_range(start_date, end_date))


def _required_start_date(end_date: date) -> date:
    calendar = exchange_calendars.get_calendar("XNYS")
    completed = calendar.sessions[calendar.sessions <= datetime.combine(end_date, datetime.min.time())]
    required = _POLICY.adjusted_daily_sessions
    if len(completed) < required:
        raise ValueError("calendar does not contain the required adjusted daily history")
    return completed[-required].date()


def _expected_bar_count(session_date: date, interval: str) -> int:
    if interval == "1d":
        return 1
    calendar = exchange_calendars.get_calendar("XNYS")
    minutes = int((calendar.session_close(session_date) - calendar.session_open(session_date)).total_seconds() // 60)
    return math.ceil(minutes / {"1m": 1, "30m": 30}[interval])


def _bar_gap_report(session_counts, expected_sessions, *, interval, adjustment_basis, session_scope):
    counts = {
        row["session_date"]: int(row["bar_count"])
        for row in session_counts
        if row["interval"] == interval
        and row["adjustment_basis"] == adjustment_basis
        and (session_scope is None or row["session_scope"] == session_scope)
    }
    missing = tuple(session for session in expected_sessions if counts.get(session, 0) == 0)
    incomplete = tuple(
        {
            "session_date": session.isoformat(),
            "actual_count": counts[session],
            "expected_count": _expected_bar_count(session, interval),
        }
        for session in expected_sessions
        if 0 < counts.get(session, 0) < _expected_bar_count(session, interval)
    )
    return {
        "covered_session_count": len(expected_sessions) - len(missing),
        "expected_session_count": len(expected_sessions),
        "missing_session_count": len(missing),
        "missing_session_ranges": _compact_dates(missing),
        "incomplete_sessions": incomplete,
    }


def _reference_queries(cursor, tickers, start_date, end_date):
    names = list(tickers)
    window = (names, start_date, end_date)
    return {
        "security_rows": query_rows(cursor, """
            SELECT revision.ticker,COUNT(*) AS revision_count,
                COUNT(DISTINCT revision.security_id) AS security_count,
                MIN(revision.effective_from) AS first_effective_at,
                MAX(revision.effective_from) AS latest_effective_at,
                MAX(revision.observed_at) AS latest_observed_at,
                ARRAY_AGG(DISTINCT security.source_origin ORDER BY security.source_origin) AS sources
            FROM ref.security_revisions AS revision
            JOIN ref.securities AS security USING (security_id)
            WHERE revision.ticker=ANY(%s)
            GROUP BY revision.ticker ORDER BY revision.ticker
        """, (names,)),
        "action_rows": query_rows(cursor, """
            SELECT revision.ticker,revision.action_type,revision.source,COUNT(*) AS revision_count,
                MIN(revision.effective_date) AS first_effective_date,
                MAX(revision.effective_date) AS latest_effective_date,
                MAX(revision.received_at) AS latest_received_at
            FROM ref.corporate_action_revisions AS revision
            WHERE revision.ticker=ANY(%s) AND revision.effective_date BETWEEN %s AND %s
            GROUP BY revision.ticker,revision.action_type,revision.source
            ORDER BY revision.ticker,revision.action_type,revision.source
        """, window),
        "action_coverage_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,coverage.action_type,coverage.source,COUNT(*) AS coverage_count,
                MIN(coverage.window_start) AS first_window_start,
                MAX(coverage.window_end) AS latest_window_end,
                MAX(coverage.received_at) AS latest_received_at,
                SUM(coverage.response_action_count) AS response_action_count
            FROM ref.corporate_action_coverage AS coverage
            JOIN security_map AS member USING (security_id)
            WHERE coverage.window_end >= %s AND coverage.window_start <= %s
            GROUP BY member.ticker,coverage.action_type,coverage.source
            ORDER BY member.ticker,coverage.action_type,coverage.source
        """, (names, start_date, end_date)),
        "earnings_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,event.source,COUNT(*) AS revision_count,
                MIN(event.event_date) AS first_event_date,MAX(event.event_date) AS latest_event_date,
                MAX(event.received_at) AS latest_received_at,MAX(event.valid_until) AS latest_valid_until
            FROM ref.market_event_revisions AS event
            JOIN security_map AS member USING (security_id)
            WHERE event.event_type='EARNINGS' AND event.event_date BETWEEN %s AND %s
            GROUP BY member.ticker,event.source ORDER BY member.ticker,event.source
        """, (names, start_date, end_date)),
        "earnings_coverage_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,coverage.source,coverage.verdict,COUNT(*) AS coverage_count,
                MIN(coverage.window_start) AS first_window_start,MAX(coverage.window_end) AS latest_window_end,
                MAX(coverage.checked_at) AS latest_checked_at,MAX(coverage.valid_until) AS latest_valid_until
            FROM ref.market_event_coverage AS coverage
            JOIN security_map AS member USING (security_id)
            WHERE coverage.event_type='EARNINGS' AND coverage.window_end >= %s AND coverage.window_start <= %s
            GROUP BY member.ticker,coverage.source,coverage.verdict
            ORDER BY member.ticker,coverage.source,coverage.verdict
        """, (names, start_date, end_date)),
        "oi_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,baseline.source,baseline.date_basis,COUNT(*) AS baseline_count,
                COUNT(DISTINCT baseline.contract_id) AS contract_count,
                MIN(baseline.settlement_session) AS first_settlement_session,
                MAX(baseline.settlement_session) AS latest_settlement_session,
                MAX(baseline.received_at) AS latest_received_at
            FROM ref.oi_baselines AS baseline
            JOIN ref.option_contracts AS contract USING (contract_id)
            JOIN security_map AS member ON member.security_id=contract.underlying_security_id
            WHERE baseline.settlement_session BETWEEN %s AND %s
            GROUP BY member.ticker,baseline.source,baseline.date_basis
            ORDER BY member.ticker,baseline.source,baseline.date_basis
        """, (names, start_date, end_date)),
        "catalog_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,contract.provider,contract.source_origin,version.eligibility,
                COUNT(DISTINCT contract.contract_id) AS contract_count,COUNT(*) AS version_count,
                MIN(contract.first_observed_at) AS first_contract_observed_at,
                MIN(version.effective_from) AS first_version_effective_at,
                MAX(version.effective_from) AS latest_version_effective_at,
                MAX(version.observed_at) AS latest_version_observed_at,
                MIN(version.expiration) AS first_expiration,MAX(version.expiration) AS latest_expiration
            FROM ref.option_contracts AS contract
            JOIN ref.option_contract_versions AS version USING (contract_id)
            JOIN security_map AS member ON member.security_id=contract.underlying_security_id
            GROUP BY member.ticker,contract.provider,contract.source_origin,version.eligibility
            ORDER BY member.ticker,contract.provider,contract.source_origin,version.eligibility
        """, (names,)),
        "fomc_rows": query_rows(cursor, """
            SELECT source,COUNT(*) AS revision_count,MIN(event_date) AS first_event_date,
                MAX(event_date) AS latest_event_date,MAX(received_at) AS latest_received_at,
                MAX(valid_until) AS latest_valid_until
            FROM ref.market_event_revisions WHERE event_type='FED_RATE_DECISION'
                AND event_date BETWEEN %s AND %s GROUP BY source ORDER BY source
        """, (start_date, end_date)),
        "fomc_coverage_rows": query_rows(cursor, """
            SELECT source,verdict,COUNT(*) AS coverage_count,MIN(window_start) AS first_window_start,
                MAX(window_end) AS latest_window_end,MAX(checked_at) AS latest_checked_at,
                MAX(valid_until) AS latest_valid_until
            FROM ref.market_event_coverage WHERE event_type='FED_RATE_DECISION'
                AND window_end >= %s AND window_start <= %s
            GROUP BY source,verdict ORDER BY source,verdict
        """, (start_date, end_date)),
        "rate_rows": query_rows(cursor, """
            SELECT source,COUNT(*) AS observation_count,COUNT(DISTINCT rate_date) AS curve_date_count,
                COUNT(DISTINCT tenor_days) AS tenor_count,MIN(rate_date) AS first_rate_date,
                MAX(rate_date) AS latest_rate_date,MAX(received_at) AS latest_received_at
            FROM ref.risk_free_rate_observations WHERE rate_date BETWEEN %s AND %s
            GROUP BY source ORDER BY source
        """, (start_date, end_date)),
        "seed_rows": query_rows(cursor, """
            SELECT current.generation,current.effective_at,manifest.seed_id::text,manifest.session_date,
                manifest.status,manifest.reason_codes,manifest.member_count,manifest.valid_until,
                manifest.source_generations
            FROM rt.seed_current AS current JOIN rt.seed_manifests AS manifest USING (seed_id)
            WHERE current.scope='MARKET_REFERENCES'
        """),
    }


def _acquisition_queries(cursor, tickers, start_date, end_date, cohort_id):
    names = list(tickers)
    cohort = str(cohort_id) if cohort_id is not None else None
    window = (names, start_date, end_date, cohort, cohort)
    return {
        "bar_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,bar.interval,bar.adjustment_basis,bar.session_scope,bar.source_kind,
                revision.availability_mode,bar.provider_tier,COUNT(*) AS revision_count,
                COUNT(DISTINCT bar.bar_start) AS bar_count,COUNT(DISTINCT catalog.session_date) AS session_count,
                MIN(catalog.session_date) AS first_session,MAX(catalog.session_date) AS latest_session,
                MIN(bar.bar_start) AS first_bar_start,MAX(bar.bar_end) AS latest_bar_end,
                MAX(bar.observed_at) AS latest_observed_at
        """ + _FINAL_BARS + """
            GROUP BY member.ticker,bar.interval,bar.adjustment_basis,bar.session_scope,bar.source_kind,
                revision.availability_mode,bar.provider_tier
            ORDER BY member.ticker,bar.interval,bar.adjustment_basis,bar.session_scope,bar.source_kind,
                revision.availability_mode,bar.provider_tier
        """, window),
        "bar_session_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,bar.interval,bar.adjustment_basis,bar.session_scope,catalog.session_date,
                COUNT(DISTINCT bar.bar_start) AS bar_count
        """ + _FINAL_BARS + """
            AND bar.interval=ANY(%s)
            GROUP BY member.ticker,bar.interval,bar.adjustment_basis,bar.session_scope,catalog.session_date
            ORDER BY member.ticker,bar.interval,bar.adjustment_basis,bar.session_scope,catalog.session_date
        """, (*window, ["1m", "30m", "1d"])),
        "option_observation_rows": query_rows(cursor, _SECURITY_MAP + """
            SELECT member.ticker,batch.provider,batch.fact_schema_version,COUNT(*) AS observation_count,
                COUNT(DISTINCT instrument.contract_id) AS contract_count,
                MIN(fact.market_time) AS first_market_time,MAX(fact.market_time) AS latest_market_time,
                MAX(fact.observed_at) AS latest_observed_at,MAX(batch.completed_at) AS latest_batch_completed_at
            FROM rt.acquisition_facts AS fact
            JOIN rt.acquisition_batches AS batch USING (acquisition_batch_id)
            JOIN rt.instruments AS instrument USING (instrument_id)
            JOIN ref.option_contracts AS contract ON contract.contract_id=instrument.contract_id
            JOIN security_map AS member ON member.security_id=contract.underlying_security_id
            WHERE batch.fact_kind='OPTION_SNAPSHOT'
                AND fact.market_time >= %s::date AND fact.market_time < (%s::date + 1)
            GROUP BY member.ticker,batch.provider,batch.fact_schema_version
            ORDER BY member.ticker,batch.provider,batch.fact_schema_version
        """, (names, start_date, end_date)),
        "discovery_rows": query_rows(cursor, """
            SELECT underlyer,provider_tier,COUNT(*) AS batch_count,SUM(page_count) AS page_count,
                SUM(row_count) AS row_count,SUM(response_bytes) AS response_bytes,
                MIN(scheduled_cycle) AS first_scheduled_cycle,MAX(scheduled_cycle) AS latest_scheduled_cycle,
                MAX(completed_at) AS latest_completed_at,MAX(created_at) AS latest_created_at
            FROM rt.option_discovery_batches
            WHERE underlyer=ANY(%s) AND scheduled_cycle >= %s::date AND scheduled_cycle < (%s::date + 1)
            GROUP BY underlyer,provider_tier ORDER BY underlyer,provider_tier
        """, (names, start_date, end_date)),
    }


def _ticker_report(ticker, data, gaps, start_date, end_date):
    def rows(name):
        return tuple(row for row in data[name] if row["ticker"] == ticker)

    security, action_coverage, earnings_coverage = rows("security_rows"), rows("action_coverage_rows"), rows("earnings_coverage_rows")
    catalog, oi, observations = rows("catalog_rows"), rows("oi_rows"), rows("option_observation_rows")
    blockers = []
    if not security:
        blockers.append("SECURITY_IDENTITY_MISSING")
    elif min(row["first_effective_at"] for row in security).date() > start_date:
        blockers.append("DATED_SECURITY_IDENTITY_COVERAGE_GAP")
    if any(not any(
        row["action_type"] == action_type
        and row["first_window_start"] <= start_date
        and row["latest_window_end"] >= end_date
        for row in action_coverage
    ) for action_type in ("SPLIT", "DIVIDEND")):
        blockers.append("CORPORATE_ACTION_COVERAGE_GAP")
    for family, gap in gaps.items():
        if gap["missing_session_count"] or gap["incomplete_sessions"]:
            blockers.append(f"{family.upper()}_COVERAGE_GAP")
    if not any(row["first_window_start"] <= start_date and row["latest_window_end"] >= end_date
               for row in earnings_coverage):
        blockers.append("HISTORICAL_EARNINGS_COVERAGE_GAP")
    if not catalog:
        blockers.append("OPTION_CATALOG_MISSING")
    elif min(row["first_version_effective_at"] for row in catalog).date() > start_date:
        blockers.append("OPTION_CATALOG_HISTORY_GAP")
    if not oi:
        blockers.append("SETTLED_OPEN_INTEREST_MISSING")
    elif min(row["first_settlement_session"] for row in oi) > start_date:
        blockers.append("SETTLED_OPEN_INTEREST_HISTORY_GAP")
    if not observations:
        blockers.append("HISTORICAL_OPTION_OBSERVATIONS_MISSING")
    return {
        "ticker": ticker,
        "security_identities": security,
        "corporate_actions": rows("action_rows"),
        "corporate_action_coverage": action_coverage,
        "bar_coverage": rows("bar_rows"),
        "bar_gaps": gaps,
        "earnings": rows("earnings_rows"),
        "earnings_coverage": earnings_coverage,
        "settled_open_interest": oi,
        "option_catalog": catalog,
        "historical_option_observations": observations,
        "current_chain_discovery": tuple(row for row in data["discovery_rows"] if row["underlyer"] == ticker),
        "downstream_blockers": tuple(blockers),
    }


def build_report(cursor, control, *, start_date: date, end_date: date, tickers=None, cohort_id: UUID | None = None):
    tickers = tuple(tickers or DEVELOPER_FORWARD_UNIVERSE.equity_tickers)
    data = {**_reference_queries(cursor, tickers, start_date, end_date),
            **_acquisition_queries(cursor, tickers, start_date, end_date, cohort_id)}
    expected_sessions = _expected_sessions(start_date, end_date)
    by_ticker = []
    for ticker in tickers:
        ticker_sessions = tuple(row for row in data["bar_session_rows"] if row["ticker"] == ticker)
        gaps = {
            name: _bar_gap_report(
                ticker_sessions, expected_sessions[-required_sessions:],
                interval=interval, adjustment_basis=adjustment_basis, session_scope=session_scope,
            )
            for name, interval, adjustment_basis, session_scope, required_sessions in BAR_FAMILIES
        }
        by_ticker.append(_ticker_report(ticker, data, gaps, start_date, end_date))
    global_blockers = []
    if not any(row["first_window_start"] <= start_date and row["latest_window_end"] >= end_date
               for row in data["fomc_coverage_rows"]):
        global_blockers.append("HISTORICAL_FOMC_COVERAGE_GAP")
    if not any(row["first_rate_date"] <= start_date and row["latest_rate_date"] >= end_date
               for row in data["rate_rows"]):
        global_blockers.append("TREASURY_CURVE_HISTORY_GAP")
    status = "GAPS_FOUND" if any(row["downstream_blockers"] for row in by_ticker) or global_blockers else "COVERED"
    return {
        "status": status,
        "scope": {
            "tickers": tickers,
            "calendar": "XNYS",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "cohort_id": str(cohort_id) if cohort_id is not None else None,
            "expected_sessions": tuple(value.isoformat() for value in expected_sessions),
            "bar_requirements": {
                name: {
                    "interval": interval,
                    "adjustment_basis": adjustment_basis,
                    "session_scope": session_scope,
                    "required_sessions": required_sessions,
                }
                for name, interval, adjustment_basis, session_scope, required_sessions in BAR_FAMILIES
            },
            "portal_history_policy_sha256": _POLICY.sha256,
        },
        "controls": {
            "database": control["database"],
            "lineage": control["lineage"],
            "transaction_read_only": control["read_only"],
            "transaction_isolation": control["isolation"],
            "checked_at": _iso(control["checked_at"]),
            "database_writes": 0,
            "provider_requests": 0,
            "services_started": 0,
            "principals_changed": 0,
            "values_exposed": False,
        },
        "per_ticker": tuple(by_ticker),
        "global_references": {
            "fomc": data["fomc_rows"],
            "fomc_coverage": data["fomc_coverage_rows"],
            "treasury_curves": data["rate_rows"],
            "current_reference_generation": data["seed_rows"],
            "downstream_blockers": tuple(global_blockers),
        },
    }


def run_audit(connection, *, database: str, start_date: date, end_date: date, tickers=None,
              cohort_id: UUID | None = None):
    with read_only_transaction(connection, database) as (cursor, control):
        return build_report(cursor, control, start_date=start_date, end_date=end_date, tickers=tickers,
                            cohort_id=cohort_id)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Read-only V2 realtime historical coverage audit.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--allow-disposable", action="store_true")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--cohort-id", type=UUID)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=args.allow_disposable)
    if args.start_date is None:
        args.start_date = _required_start_date(args.end_date)
    if args.end_date < args.start_date:
        parser.error("end-date must not precede start-date")
    return args


def main(argv=None):
    args = parse_args(argv)
    report_path = require_new_workspace_report(args.report_path, WORKSPACE_DIR)
    settings = admin_settings(args.env_file)
    with closing(connect(args.database, settings, application_name="audit_realtime_historical_coverage")) as connection:
        report = run_audit(connection, database=args.database, start_date=args.start_date,
                           end_date=args.end_date, cohort_id=args.cohort_id)
    write_report(report_path, json.loads(json.dumps(report, default=str)))
    print(json.dumps({"state": "COMPLETE", "status": report["status"], "database": args.database,
                      "report_path": str(report_path), "provider_requests": 0, "database_writes": 0}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
