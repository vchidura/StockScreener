#!/usr/bin/env python3
"""Report exact provider-free V2 nine-ticker coverage through one XNYS session."""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import exchange_calendars
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc
EXACT_INTERVAL_MINUTES = {"1m": 1, "5m": 5}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("nine-ticker status targets only stocks_realtime_v2")
    if args.report_path is not None:
        args.report_path = args.report_path.resolve()
        if args.report_path.exists():
            parser.error("--report-path must be new")
    return args


def expected_starts(opened_at, closed_at, width_minutes):
    starts = []
    current = opened_at
    width = timedelta(minutes=width_minutes)
    while current + width <= closed_at:
        starts.append(current)
        current += width
    return tuple(starts)


def coalesced_missing_ranges(expected, observed, width_minutes):
    missing = [value for value in expected if value not in observed]
    if not missing:
        return ()
    width = timedelta(minutes=width_minutes)
    ranges = []
    start = previous = missing[0]
    for value in missing[1:]:
        if value == previous + width:
            previous = value
            continue
        ranges.append((start, previous + width))
        start = previous = value
    ranges.append((start, previous + width))
    return tuple(ranges)


def _iso(value):
    return None if value is None else value.astimezone(UTC).isoformat()


def _identity_rows(
    cursor,
    cohort_id,
    opened_at,
    closed_at,
    known_by,
    current_at,
):
    cursor.execute(
        "SELECT instrument.instrument_id,latest.ticker,"
        "latest.effective_from,latest.effective_to,latest.observed_at,"
        "latest.active,session_identity.ticker,current_identity.ticker "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_members AS member USING(manifest_id) "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "LEFT JOIN LATERAL ("
        "SELECT revision.ticker,revision.effective_from,"
        "revision.effective_to,revision.observed_at,revision.active "
        "FROM ref.security_revisions AS revision "
        "WHERE revision.security_id=instrument.security_id "
        "ORDER BY revision.observed_at DESC,revision.revision_id DESC LIMIT 1"
        ") AS latest ON true "
        "LEFT JOIN LATERAL ("
        "SELECT revision.ticker FROM ref.security_revisions AS revision "
        "WHERE revision.security_id=instrument.security_id "
        "AND revision.effective_from<=%s "
        "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.effective_from DESC,revision.observed_at DESC,"
        "revision.revision_id DESC LIMIT 1"
        ") AS session_identity ON true "
        "LEFT JOIN LATERAL ("
        "SELECT revision.ticker FROM ref.security_revisions AS revision "
        "WHERE revision.security_id=instrument.security_id "
        "AND revision.effective_from<=%s "
        "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.effective_from DESC,revision.observed_at DESC,"
        "revision.revision_id DESC LIMIT 1"
        ") AS current_identity ON true "
        "WHERE cohort.cohort_id=%s AND instrument.asset_class='EQUITY' "
        "AND member.channel='AM' "
        "ORDER BY instrument.instrument_id",
        (
            closed_at,
            opened_at,
            known_by,
            current_at,
            current_at,
            current_at,
            str(cohort_id),
        ),
    )
    return tuple(cursor.fetchall())


def collect_status(cursor, *, cohort_id, session_date, checked_at):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(
        session_date.isoformat(),
        direction="none",
    )
    opened_at = calendar.session_open(session).to_pydatetime()
    closed_at = calendar.session_close(session).to_pydatetime()
    historical_known_by = closed_at + timedelta(minutes=17)
    identities = _identity_rows(
        cursor,
        cohort_id,
        opened_at,
        closed_at,
        historical_known_by,
        checked_at,
    )
    if not identities:
        raise ValueError("cohort has no equity members")

    cursor.execute(
        "SELECT manifest.stock_count,profile.provider_tier,profile.status,"
        "profile.expires_at,manifest.session_date "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "WHERE cohort.cohort_id=%s",
        (str(cohort_id),),
    )
    scope = cursor.fetchone()
    if scope is None or scope[1] != "MASSIVE_DEVELOPER":
        raise ValueError("cohort is not the Developer equity cohort")
    if len(identities) != int(scope[0]):
        raise ValueError("cohort membership is not exact")

    instrument_ids = [int(row[0]) for row in identities]
    cursor.execute(
        "SELECT current.instrument_id,current.interval,revision.bar_start,"
        "revision.bar_end,revision.ticker "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.cohort_id=%s "
        "AND current.provider_tier='MASSIVE_DEVELOPER' "
        "AND current.instrument_id=ANY(%s) "
        "AND current.interval=ANY(%s) "
        "AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.session_date=%s AND revision.is_final "
        "ORDER BY current.instrument_id,current.interval,revision.bar_start",
        (
            str(cohort_id),
            instrument_ids,
            list(EXACT_INTERVAL_MINUTES),
            session_date,
        ),
    )
    observed = defaultdict(set)
    bar_tickers = {}
    for instrument_id, interval, bar_start, _bar_end, ticker in cursor.fetchall():
        observed[(int(instrument_id), str(interval))].add(bar_start)
        bar_tickers[int(instrument_id)] = str(ticker)

    cursor.execute(
        "SELECT current.instrument_id,current.interval,"
        "current.adjustment_basis,MAX(current.bar_end),COUNT(*) "
        "FROM rt.bar_current AS current WHERE current.cohort_id=%s "
        "AND current.provider_tier='MASSIVE_DEVELOPER' "
        "AND current.instrument_id=ANY(%s) "
        "GROUP BY current.instrument_id,current.interval,"
        "current.adjustment_basis "
        "ORDER BY current.instrument_id,current.interval,"
        "current.adjustment_basis",
        (str(cohort_id), instrument_ids),
    )
    latest = defaultdict(dict)
    for instrument_id, interval, basis, bar_end, count in cursor.fetchall():
        latest[int(instrument_id)][f"{interval}:{basis}"] = {
            "latest_bar_end": _iso(bar_end),
            "current_rows": int(count),
        }

    cursor.execute(
        "SELECT instrument_id,interval,source_kind,terminal_state,"
        "last_complete_bar_end,last_examined_bar_end,reason_codes,updated_at "
        "FROM rt.acquisition_watermarks WHERE cohort_id=%s "
        "AND instrument_id=ANY(%s) "
        "ORDER BY instrument_id,interval,source_kind",
        (str(cohort_id), instrument_ids),
    )
    watermarks = defaultdict(list)
    for row in cursor.fetchall():
        watermarks[int(row[0])].append({
            "interval": row[1],
            "source_kind": row[2],
            "terminal_state": row[3],
            "last_complete_bar_end": _iso(row[4]),
            "last_examined_bar_end": _iso(row[5]),
            "reason_codes": tuple(row[6] or ()),
            "updated_at": _iso(row[7]),
        })

    expected_by_interval = {
        interval: expected_starts(opened_at, closed_at, width)
        for interval, width in EXACT_INTERVAL_MINUTES.items()
    }
    tickers = {}
    total_missing = {interval: 0 for interval in EXACT_INTERVAL_MINUTES}
    for (
        instrument_id,
        latest_ticker,
        latest_effective_from,
        latest_effective_to,
        latest_observed_at,
        latest_active,
        session_ticker,
        current_ticker,
    ) in identities:
        instrument_id = int(instrument_id)
        ticker = (
            str(latest_ticker)
            if latest_ticker is not None
            else bar_tickers.get(instrument_id, f"instrument:{instrument_id}")
        )
        intervals = {}
        for interval, width in EXACT_INTERVAL_MINUTES.items():
            expected = expected_by_interval[interval]
            own_observed = observed[(instrument_id, interval)]
            ranges = coalesced_missing_ranges(expected, own_observed, width)
            missing_count = len(expected) - len(own_observed & set(expected))
            total_missing[interval] += missing_count
            intervals[interval] = {
                "expected_bars": len(expected),
                "observed_bars": len(own_observed & set(expected)),
                "missing_bars": missing_count,
                "missing_ranges": [
                    {"start": _iso(start), "end": _iso(end)}
                    for start, end in ranges
                ],
            }
        tickers[ticker] = {
            "instrument_id": instrument_id,
            "latest_identity": {
                "effective_from": _iso(latest_effective_from),
                "effective_to": _iso(latest_effective_to),
                "observed_at": _iso(latest_observed_at),
                "active": bool(latest_active),
            },
            "session_identity": session_ticker,
            "current_identity": current_ticker,
            "intervals": intervals,
            "latest": latest[instrument_id],
            "watermarks": watermarks[instrument_id],
        }

    cursor.execute(
        "SELECT generation.session_date,generation.generated_at,"
        "generation.expected_members,generation.actual_members,"
        "generation.status FROM rt.screening_current AS current "
        "JOIN rt.screening_generations AS generation USING(generation_id) "
        "WHERE current.cohort_id=%s",
        (str(cohort_id),),
    )
    screening = cursor.fetchone()
    cursor.execute(
        "SELECT release.cutoff,release.session_date,release.updated_at "
        "FROM rt.portal_release_current AS release WHERE cohort_id=%s",
        (str(cohort_id),),
    )
    release = cursor.fetchone()

    exact_complete = all(value == 0 for value in total_missing.values())
    session_identity_complete = all(row[6] is not None for row in identities)
    current_identity_complete = all(row[7] is not None for row in identities)
    reasons = []
    if not exact_complete:
        reasons.append("LAST_SESSION_EXACT_BAR_GAPS")
    if not session_identity_complete:
        reasons.append("SESSION_REFERENCE_IDENTITY_INCOMPLETE")
    if not current_identity_complete:
        reasons.append("CURRENT_REFERENCE_IDENTITY_EXPIRED")
    if screening is None or screening[0] < session_date:
        reasons.append("SCREENING_BEHIND_LAST_SESSION")
    if release is None or release[0].date() < session_date:
        reasons.append("PORTAL_RELEASE_BEHIND_LAST_SESSION")
    return {
        "status": "READY" if not reasons else "DEGRADED",
        "reason_codes": reasons,
        "cohort_id": str(cohort_id),
        "provider_tier": scope[1],
        "profile_status": scope[2],
        "profile_expires_at": _iso(scope[3]),
        "manifest_session_date": str(scope[4]),
        "session_date": str(session_date),
        "session_open": _iso(opened_at),
        "session_close": _iso(closed_at),
        "checked_at": _iso(checked_at),
        "member_count": len(identities),
        "total_missing": total_missing,
        "session_identity_complete": session_identity_complete,
        "current_identity_complete": current_identity_complete,
        "tickers": tickers,
        "screening_current": (
            None
            if screening is None
            else {
                "session_date": str(screening[0]),
                "generated_at": _iso(screening[1]),
                "expected_members": int(screening[2]),
                "actual_members": int(screening[3]),
                "status": screening[4],
            }
        ),
        "portal_release_current": (
            None
            if release is None
            else {
                "cutoff": _iso(release[0]),
                "session_date": (
                    None if release[1] is None else str(release[1])
                ),
                "updated_at": _iso(release[2]),
            }
        ),
        "provider_requests": 0,
        "database_writes": 0,
    }


def main(argv=None):
    args = parse_args(argv)
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("nine-ticker status requires REALTIME_V2_APP_DSN")
    pool = SimpleConnectionPool(
        1,
        1,
        dsn=dsn,
        options="-c timezone=UTC",
    )
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
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            report = collect_status(
                unit.cursor,
                cohort_id=args.cohort_id,
                session_date=args.session_date,
                checked_at=datetime.now(UTC),
            )
        serializable = json.loads(json.dumps(report, default=str))
        if args.report_path is not None:
            write_report(args.report_path, serializable)
        print(json.dumps(serializable, indent=2, sort_keys=True))
        return 0 if report["status"] == "READY" else 2
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
