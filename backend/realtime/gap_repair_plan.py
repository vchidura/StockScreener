"""Provider-free explicit repair planning for retained 1m/5m session gaps."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import UUID

from .session_close_coverage import five_minute_session


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("gap inspection clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _coalesce(rows, *, width: timedelta):
    ordered = sorted(rows)
    ranges = []
    current = []
    for row in ordered:
        if current and (
            row[0] != current[-1][0]
            or row[1] != current[-1][1]
            or row[2] != current[-1][2] + width
        ):
            ranges.append(_range(current, width))
            current = []
        current.append(row)
    if current:
        ranges.append(_range(current, width))
    return tuple(ranges)


def _range(rows, width):
    first, last = rows[0], rows[-1]
    return {
        "instrument_id": first[0],
        "ticker": first[1],
        "requested_from": first[2],
        "requested_to": last[2] + width,
        "missing_members": len(rows),
    }


def plan_gap_repair_cursor(
    cursor,
    *,
    cohort_id: UUID,
    sessions: tuple[date, ...],
    known_by: datetime,
    intervals: tuple[str, ...] = ("1m", "5m"),
    detail_limit: int = 2_000,
) -> dict:
    if (
        not isinstance(cohort_id, UUID)
        or not sessions
        or tuple(sorted(set(sessions))) != sessions
        or not intervals
        or set(intervals) - {"1m", "5m"}
        or type(detail_limit) is not int
        or detail_limit < 1
    ):
        raise ValueError("gap repair plan scope is invalid")
    inspected = _utc(known_by)
    cursor.execute(
        "SELECT DISTINCT member.instrument_id,reference.ticker "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_members AS member USING(manifest_id) "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "JOIN LATERAL (SELECT revision.ticker "
        "FROM ref.security_revisions AS revision "
        "WHERE revision.security_id=instrument.security_id "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.observed_at DESC,revision.revision_id DESC LIMIT 1"
        ") AS reference ON true "
        "WHERE cohort.cohort_id=%s AND instrument.asset_class='EQUITY' "
        "AND member.channel='AM' ORDER BY member.instrument_id",
        (inspected, str(cohort_id)),
    )
    members = tuple((int(row[0]), str(row[1])) for row in cursor.fetchall())
    if not members or len({row[0] for row in members}) != len(members):
        raise ValueError("gap repair cohort membership is unavailable or ambiguous")

    plans = []
    total_missing = 0
    request_scopes = set()
    for session_date in sessions:
        session = five_minute_session(session_date)
        for interval in intervals:
            minutes = 1 if interval == "1m" else 5
            width = timedelta(minutes=minutes)
            expected_per_instrument = int(
                (session.closed_at - session.opened_at) / width
            )
            cursor.execute(
                "WITH expected AS ("
                "SELECT instrument_id,generate_series(%s::timestamptz,"
                "%s::timestamptz-%s::interval,%s::interval) AS bar_start "
                "FROM unnest(%s::bigint[]) AS member(instrument_id)) "
                "SELECT expected.instrument_id,expected.bar_start "
                "FROM expected LEFT JOIN rt.bar_current AS current "
                "ON current.cohort_id=%s "
                "AND current.instrument_id=expected.instrument_id "
                "AND current.interval=%s "
                "AND current.bar_start=expected.bar_start "
                "AND current.bar_end=expected.bar_start+%s::interval "
                "AND current.session_scope='RTH' "
                "AND current.adjustment_basis='RAW_UNADJUSTED' "
                "LEFT JOIN rt.bar_revisions AS revision "
                "ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.observed_at=current.observed_at "
                "AND revision.cohort_id=current.cohort_id "
                "AND revision.instrument_id=current.instrument_id "
                "AND revision.interval=current.interval "
                "AND revision.is_final "
                "WHERE revision.bar_revision_id IS NULL "
                "ORDER BY expected.instrument_id,expected.bar_start",
                (
                    session.opened_at,
                    session.closed_at,
                    f"{minutes} minutes",
                    f"{minutes} minutes",
                    [row[0] for row in members],
                    str(cohort_id),
                    interval,
                    f"{minutes} minutes",
                ),
            )
            tickers = dict(members)
            missing = tuple(
                (int(row[0]), tickers[int(row[0])], row[1])
                for row in cursor.fetchall()
            )
            ranges = _coalesce(missing, width=width)
            total_missing += len(missing)
            if interval == "1m":
                for instrument_id, ticker, _bar_start in missing:
                    request_scopes.add(
                        (session_date, instrument_id, ticker)
                    )
            plans.append({
                "session_date": session_date,
                "interval": interval,
                "expected_per_instrument": expected_per_instrument,
                "expected_members": len(members) * expected_per_instrument,
                "missing_members": len(missing),
                "missing_range_count": len(ranges),
                "missing_ranges": ranges[:detail_limit],
                "missing_ranges_truncated": len(ranges) > detail_limit,
                "executor": (
                    "ADVANCED_HISTORICAL_1M"
                    if interval == "1m"
                    else "LOCAL_DERIVATION_FROM_RAW_1M"
                ),
            })
    return {
        "status": "GAPS_FOUND" if total_missing else "COMPLETE",
        "cohort_id": str(cohort_id),
        "sessions": sessions,
        "members": len(members),
        "intervals": intervals,
        "inspected_at": inspected,
        "plans": plans,
        "missing_members": total_missing,
        "full_session_request_scopes": len(request_scopes),
        "automatic_execution": False,
        "execution_authorized": False,
        "provider_requests": 0,
        "database_writes": 0,
    }
