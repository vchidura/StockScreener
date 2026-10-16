"""Provider-free V2 session-close coverage over current five-minute bars."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_values

from .contracts import canonical_sha256


FIVE_MINUTES = timedelta(minutes=5)
DEFAULT_MEMBER_LIMIT = 50_000
DEFAULT_MISSING_RANGE_LIMIT = 2_000
DEFAULT_PROVIDER_DELAY_SECONDS = 900
DEFAULT_FINALIZATION_GRACE_SECONDS = 30
_PROVIDER_TIERS = frozenset({"MASSIVE_ADVANCED", "MASSIVE_DEVELOPER"})


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class FiveMinuteSession:
    session_date: date
    opened_at: datetime
    closed_at: datetime
    bar_starts: tuple[datetime, ...]

    @property
    def expected_per_instrument(self) -> int:
        return len(self.bar_starts)


@dataclass(frozen=True, slots=True)
class SessionCloseEligibilityPolicy:
    provider_delay_seconds: int = DEFAULT_PROVIDER_DELAY_SECONDS
    finalization_grace_seconds: int = DEFAULT_FINALIZATION_GRACE_SECONDS
    version: str = "v2-session-close-eligibility-v1"

    def __post_init__(self) -> None:
        if (
            type(self.provider_delay_seconds) is not int
            or self.provider_delay_seconds < 0
            or type(self.finalization_grace_seconds) is not int
            or self.finalization_grace_seconds < 0
            or not self.version.strip()
        ):
            raise ValueError("session-close eligibility policy is invalid")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def eligible_at(self, session: FiveMinuteSession) -> datetime:
        return session.closed_at + timedelta(
            seconds=self.provider_delay_seconds + self.finalization_grace_seconds
        )


class SessionCloseNotEligible(ValueError):
    def __init__(self, eligible_at: datetime) -> None:
        self.eligible_at = _utc(eligible_at, "eligible_at")
        super().__init__(
            "session close coverage is not provider-qualified until "
            + self.eligible_at.isoformat()
        )


def five_minute_session(
    session_date: date,
    *,
    calendar=None,
) -> FiveMinuteSession:
    """Return exact XNYS five-minute RTH bar starts, including early closes."""
    import exchange_calendars

    if not isinstance(session_date, date):
        raise TypeError("session_date must be a date")
    calendar = calendar or exchange_calendars.get_calendar("XNYS")
    session_key = session_date.isoformat()
    if not calendar.is_session(session_key):
        raise ValueError(f"{session_date} is not an XNYS session")
    opened = _utc(
        calendar.session_open(session_key).to_pydatetime(), "session open"
    )
    closed = _utc(
        calendar.session_close(session_key).to_pydatetime(), "session close"
    )
    duration = closed - opened
    if duration <= timedelta(0) or duration % FIVE_MINUTES:
        raise ValueError("XNYS session length is not divisible by five minutes")
    count = int(duration / FIVE_MINUTES)
    return FiveMinuteSession(
        session_date,
        opened,
        closed,
        tuple(opened + index * FIVE_MINUTES for index in range(count)),
    )


@dataclass(frozen=True, slots=True)
class EquityCloseMember:
    instrument_id: int
    ticker: str

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("close member instrument_id must be positive")
        ticker = self.ticker.strip().upper()
        if not ticker:
            raise ValueError("close member ticker cannot be blank")
        object.__setattr__(self, "ticker", ticker)


@dataclass(frozen=True, slots=True)
class MissingFiveMinuteRange:
    instrument_id: int
    ticker: str
    requested_from: datetime
    requested_to: datetime
    missing_members: int

    def __post_init__(self) -> None:
        start = _utc(self.requested_from, "requested_from")
        end = _utc(self.requested_to, "requested_to")
        if (
            type(self.instrument_id) is not int
            or self.instrument_id <= 0
            or not self.ticker.strip()
            or end <= start
            or type(self.missing_members) is not int
            or self.missing_members <= 0
            or end - start != self.missing_members * FIVE_MINUTES
        ):
            raise ValueError("missing five-minute range is invalid")
        object.__setattr__(self, "ticker", self.ticker.strip().upper())
        object.__setattr__(self, "requested_from", start)
        object.__setattr__(self, "requested_to", end)


def missing_five_minute_ranges(
    missing: Sequence[tuple[int, str, datetime]],
) -> tuple[MissingFiveMinuteRange, ...]:
    """Coalesce consecutive missing bar starts per instrument."""
    ordered = sorted(
        (
            int(instrument_id),
            str(ticker).strip().upper(),
            _utc(bar_start, "missing bar_start"),
        )
        for instrument_id, ticker, bar_start in missing
    )
    if len({(row[0], row[2]) for row in ordered}) != len(ordered):
        raise ValueError("missing close members must be unique")
    ranges: list[MissingFiveMinuteRange] = []
    current: list[tuple[int, str, datetime]] = []
    for row in ordered:
        if current and (
            row[0] != current[-1][0]
            or row[1] != current[-1][1]
            or row[2] != current[-1][2] + FIVE_MINUTES
        ):
            ranges.append(_missing_range(current))
            current = []
        current.append(row)
    if current:
        ranges.append(_missing_range(current))
    return tuple(ranges)


def _missing_range(rows: Sequence[tuple[int, str, datetime]]) -> MissingFiveMinuteRange:
    first, last = rows[0], rows[-1]
    return MissingFiveMinuteRange(
        first[0],
        first[1],
        first[2],
        last[2] + FIVE_MINUTES,
        len(rows),
    )


@dataclass(frozen=True, slots=True)
class SessionCloseCoverage:
    cohort_id: UUID
    provider_tier: str
    cohort_effective_at: datetime
    manifest_id: UUID
    manifest_session_date: date
    manifest_sha256: str
    universe_sha256: str
    session: FiveMinuteSession
    eligibility_policy: SessionCloseEligibilityPolicy
    eligible_at: datetime
    known_by: datetime
    instrument_count: int
    expected_members: int
    complete_members: int
    missing_members: int
    missing_ranges: tuple[MissingFiveMinuteRange, ...]
    missing_range_count: int
    missing_ranges_truncated: bool
    expected_manifest_sha256: str
    input_sha256: str
    coverage_sha256: str
    query_seconds: float

    @property
    def status(self) -> str:
        return "COMPLETE" if self.missing_members == 0 else "INCOMPLETE"

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "cohort_id": str(self.cohort_id),
            "provider_tier": self.provider_tier,
            "cohort_effective_at": self.cohort_effective_at.isoformat(),
            "manifest_id": str(self.manifest_id),
            "manifest_session_date": self.manifest_session_date.isoformat(),
            "manifest_sha256": self.manifest_sha256,
            "universe_sha256": self.universe_sha256,
            "session_date": self.session.session_date.isoformat(),
            "session_open": self.session.opened_at.isoformat(),
            "session_close": self.session.closed_at.isoformat(),
            "eligible_at": self.eligible_at.isoformat(),
            "provider_delay_seconds": self.eligibility_policy.provider_delay_seconds,
            "finalization_grace_seconds": self.eligibility_policy.finalization_grace_seconds,
            "eligibility_policy_sha256": self.eligibility_policy.sha256,
            "known_by": self.known_by.isoformat(),
            "instrument_count": self.instrument_count,
            "expected_per_instrument": self.session.expected_per_instrument,
            "expected_members": self.expected_members,
            "complete_members": self.complete_members,
            "missing_members": self.missing_members,
            "missing_range_count": self.missing_range_count,
            "missing_ranges_truncated": self.missing_ranges_truncated,
            "missing_ranges": [
                {
                    "instrument_id": row.instrument_id,
                    "ticker": row.ticker,
                    "requested_from": row.requested_from.isoformat(),
                    "requested_to": row.requested_to.isoformat(),
                    "missing_members": row.missing_members,
                }
                for row in self.missing_ranges
            ],
            "expected_manifest_sha256": self.expected_manifest_sha256,
            "input_sha256": self.input_sha256,
            "coverage_sha256": self.coverage_sha256,
            "query_seconds": round(self.query_seconds, 6),
            "provider_requests": 0,
            "database_writes": 0,
        }


_MEMBERS_SQL = """
WITH equity_members AS (
    SELECT DISTINCT member.instrument_id
    FROM rt.cohorts AS cohort
    JOIN rt.subscription_members AS member
      ON member.manifest_id = cohort.manifest_id
    JOIN rt.instruments AS instrument
      ON instrument.instrument_id = member.instrument_id
    WHERE cohort.cohort_id = %s
      AND instrument.asset_class = 'EQUITY'
)
SELECT cohort.manifest_id::text, manifest.manifest_sha256,
       manifest.universe_sha256, cohort.effective_at,
       manifest.session_date, manifest.stock_count,
       instrument.instrument_id,
       identity.ticker
FROM rt.cohorts AS cohort
JOIN rt.subscription_manifests AS manifest
  ON manifest.manifest_id = cohort.manifest_id
JOIN equity_members AS member ON TRUE
JOIN rt.instruments AS instrument
  ON instrument.instrument_id = member.instrument_id
LEFT JOIN LATERAL (
    SELECT revision.ticker
    FROM ref.security_revisions AS revision
    WHERE revision.security_id = instrument.security_id
      AND revision.effective_from <= %s
      AND (revision.effective_to IS NULL OR revision.effective_to > %s)
      AND revision.observed_at <= %s
    ORDER BY revision.effective_from DESC,
             revision.observed_at DESC,
             revision.revision_id DESC
    LIMIT 1
) AS identity ON TRUE
WHERE cohort.cohort_id = %s
ORDER BY instrument.instrument_id
"""

_COVERAGE_MEMBERS_SQL = """
WITH expected AS (
    SELECT instrument_id,
           generate_series(%s::timestamptz,
                           %s::timestamptz - interval '5 minutes',
                           interval '5 minutes') AS bar_start
    FROM unnest(%s::bigint[]) AS member(instrument_id)
)
SELECT expected.instrument_id, expected.bar_start,
       current.bar_revision_id::text, current.observed_at, current.updated_at
FROM expected
LEFT JOIN rt.bar_current AS current
  ON current.cohort_id = %s
 AND current.provider_tier = %s
 AND current.instrument_id = expected.instrument_id
 AND current.interval = '5m'
 AND current.bar_start = expected.bar_start
 AND current.bar_end = expected.bar_start + interval '5 minutes'
 AND current.session_scope = 'RTH'
 AND current.adjustment_basis = 'RAW_UNADJUSTED'
ORDER BY expected.instrument_id, expected.bar_start
LIMIT %s
"""

_HISTORICAL_MEMBERS_SQL = """
WITH expected AS (
    SELECT * FROM unnest(%s::bigint[], %s::timestamptz[])
      AS member(instrument_id, bar_start)
)
SELECT expected.instrument_id, expected.bar_start, selected.bar_revision_id::text
FROM expected
LEFT JOIN LATERAL (
    SELECT revision.bar_revision_id
    FROM rt.bar_catalog AS catalog
    JOIN rt.bar_revisions AS revision
      ON revision.bar_revision_id = catalog.bar_revision_id
     AND revision.observed_at = catalog.observed_at
     AND revision.cohort_id = catalog.cohort_id
     AND revision.provider_tier = catalog.provider_tier
     AND revision.instrument_id = catalog.instrument_id
     AND revision.interval = catalog.interval
     AND revision.bar_start = catalog.bar_start
     AND revision.bar_end = catalog.bar_end
    WHERE catalog.cohort_id = %s
      AND catalog.provider_tier = %s
      AND catalog.instrument_id = expected.instrument_id
      AND catalog.interval = '5m'
      AND catalog.bar_start = expected.bar_start
      AND catalog.bar_end = expected.bar_start + interval '5 minutes'
      AND catalog.observed_at <= %s
      AND revision.observed_at >= %s
      AND revision.session_scope = 'RTH'
      AND revision.adjustment_basis = 'RAW_UNADJUSTED'
      AND revision.is_final
      AND revision.created_at <= %s
      AND (revision.replay_available_at IS NULL OR revision.replay_available_at <= %s)
      AND (revision.provider_published_at IS NULL OR revision.provider_published_at <= %s)
    ORDER BY {source_precedence} DESC,
             revision.observed_at DESC, revision.created_at DESC,
             revision.bar_revision_id
    LIMIT 1
) AS selected ON TRUE
ORDER BY expected.instrument_id, expected.bar_start
"""


def audit_session_close_cursor(
    cursor,
    *,
    cohort_id: UUID,
    provider_tier: str,
    session_date: date,
    known_by: datetime,
    calendar=None,
    eligibility_policy: SessionCloseEligibilityPolicy | None = None,
    member_limit: int = DEFAULT_MEMBER_LIMIT,
    missing_range_limit: int = DEFAULT_MISSING_RANGE_LIMIT,
) -> SessionCloseCoverage:
    """Audit current five-minute identities as known, without provider I/O or writes."""
    if not isinstance(cohort_id, UUID):
        raise TypeError("cohort_id must be a UUID")
    tier = provider_tier.strip().upper()
    if tier not in _PROVIDER_TIERS:
        raise ValueError("provider_tier is unsupported")
    known = _utc(known_by, "known_by")
    policy = eligibility_policy or SessionCloseEligibilityPolicy()
    if not isinstance(policy, SessionCloseEligibilityPolicy):
        raise TypeError("eligibility_policy is invalid")
    if (
        type(member_limit) is not int
        or not 1 <= member_limit <= 100_000
        or type(missing_range_limit) is not int
        or not 1 <= missing_range_limit <= member_limit
    ):
        raise ValueError("close coverage detail limits are invalid")
    session = five_minute_session(session_date, calendar=calendar)
    eligible_at = policy.eligible_at(session)
    if known < eligible_at:
        raise SessionCloseNotEligible(eligible_at)

    started = time.perf_counter()
    cursor.execute(
        _MEMBERS_SQL,
        (
            str(cohort_id),
            session.closed_at,
            session.opened_at,
            known,
            str(cohort_id),
        ),
    )
    rows = tuple(cursor.fetchall())
    if not rows:
        raise ValueError("cohort has no equity subscription members")
    manifest_identity = {
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            row[3],
            row[4],
            int(row[5]),
        )
        for row in rows
    }
    if len(manifest_identity) != 1:
        raise ValueError("cohort manifest identity is ambiguous")
    (
        manifest_id,
        manifest_sha,
        universe_sha,
        cohort_effective_at,
        manifest_session,
        stock_count,
    ) = next(iter(manifest_identity))
    cohort_effective = _utc(cohort_effective_at, "cohort effective_at")
    if cohort_effective > session.opened_at or manifest_session > session_date:
        raise ValueError("cohort was not effective for the audited session")
    if any(row[7] is None for row in rows):
        raise ValueError("cohort equity security identity is incomplete")
    members = tuple(
        EquityCloseMember(int(row[6]), str(row[7])) for row in rows
    )
    if (
        len(members) != stock_count
        or len({row.instrument_id for row in members}) != len(members)
        or len({row.ticker for row in members}) != len(members)
    ):
        raise ValueError("cohort equity member count or identity is inconsistent")

    instrument_ids = [row.instrument_id for row in members]
    expected_members = len(members) * session.expected_per_instrument
    if expected_members > member_limit:
        raise ValueError("close coverage member detail exceeds its bound")
    coverage_parameters = (
        session.opened_at,
        session.closed_at,
        instrument_ids,
        str(cohort_id),
        tier,
    )
    cursor.execute(
        _COVERAGE_MEMBERS_SQL,
        (*coverage_parameters, member_limit + 1),
    )
    current_rows = tuple(cursor.fetchall())
    if len(current_rows) != expected_members:
        raise ValueError("close coverage member query is incomplete or duplicated")
    coverage_rows = [
        (
            instrument_id, bar_start,
            revision_id
            if observed_at is not None and observed_at <= known and updated_at <= known
            else None,
        )
        for instrument_id, bar_start, revision_id, observed_at, updated_at in current_rows
    ]
    historical_members = [
        (int(instrument_id), bar_start)
        for instrument_id, bar_start, _revision_id, observed_at, updated_at in current_rows
        if observed_at is not None and (observed_at > known or updated_at > known)
    ]
    if historical_members:
        from .screening_repository import source_precedence_case

        cursor.execute(
            _HISTORICAL_MEMBERS_SQL.format(
                source_precedence=source_precedence_case(False)
            ),
            (
                [instrument_id for instrument_id, _ in historical_members],
                [bar_start for _, bar_start in historical_members],
                str(cohort_id), tier, known, session.opened_at, known, known, known,
            ),
        )
        historical_rows = tuple(cursor.fetchall())
        historical = {
            (int(instrument_id), bar_start): revision_id
            for instrument_id, bar_start, revision_id in historical_rows
        }
        if (
            len(historical_rows) != len(historical_members)
            or set(historical) != set(historical_members)
        ):
            raise ValueError("historical close coverage is incomplete or duplicated")
        coverage_rows = [
            (instrument_id, bar_start, historical[(instrument_id, bar_start)])
            if (instrument_id, bar_start) in historical
            else (instrument_id, bar_start, revision_id)
            for instrument_id, bar_start, revision_id in coverage_rows
        ]
    tickers = {row.instrument_id: row.ticker for row in members}
    input_members = tuple(
        (
            int(instrument_id),
            tickers[int(instrument_id)],
            bar_start,
            str(bar_revision_id) if bar_revision_id is not None else None,
        )
        for instrument_id, bar_start, bar_revision_id in coverage_rows
    )
    input_sha = canonical_sha256({
        "version": "v2-session-close-input-v1",
        "members": input_members,
    })
    missing_rows = tuple(
        (instrument_id, bar_start)
        for instrument_id, bar_start, bar_revision_id in coverage_rows
        if bar_revision_id is None
    )
    missing_members = len(missing_rows)
    complete_members = expected_members - missing_members
    ranges = missing_five_minute_ranges(
        (
            int(instrument_id),
            tickers[int(instrument_id)],
            bar_start,
        )
        for instrument_id, bar_start in missing_rows
    )
    shown_ranges = ranges[:missing_range_limit]
    expected_manifest_sha = canonical_sha256(
        {
            "version": "v2-session-close-five-minute-members-v1",
            "cohort_id": cohort_id,
            "provider_tier": tier,
            "cohort_effective_at": cohort_effective,
            "manifest_session_date": manifest_session,
            "session_date": session_date,
            "opened_at": session.opened_at,
            "closed_at": session.closed_at,
            "members": tuple(
                (member.instrument_id, member.ticker) for member in members
            ),
            "bar_starts": session.bar_starts,
        }
    )
    coverage_sha = canonical_sha256(
        {
            "version": "v2-session-close-coverage-v2",
            "expected_manifest_sha256": expected_manifest_sha,
            "eligibility_policy_sha256": policy.sha256,
            "input_sha256": input_sha,
            "complete_members": complete_members,
            "missing_members": missing_members,
            "missing_ranges": ranges,
        }
    )
    return SessionCloseCoverage(
        cohort_id=cohort_id,
        provider_tier=tier,
        cohort_effective_at=cohort_effective,
        manifest_id=UUID(manifest_id),
        manifest_session_date=manifest_session,
        manifest_sha256=manifest_sha,
        universe_sha256=universe_sha,
        session=session,
        eligibility_policy=policy,
        eligible_at=eligible_at,
        known_by=known,
        instrument_count=len(members),
        expected_members=expected_members,
        complete_members=complete_members,
        missing_members=missing_members,
        missing_ranges=shown_ranges,
        missing_range_count=len(ranges),
        missing_ranges_truncated=len(ranges) > len(shown_ranges),
        expected_manifest_sha256=expected_manifest_sha,
        input_sha256=input_sha,
        coverage_sha256=coverage_sha,
        query_seconds=time.perf_counter() - started,
    )


def session_close_audit_id(coverage: SessionCloseCoverage) -> UUID:
    if not isinstance(coverage, SessionCloseCoverage):
        raise TypeError("coverage must be a SessionCloseCoverage")
    return uuid5(
        NAMESPACE_URL,
        f"v2-session-close-audit:{coverage.coverage_sha256}",
    )


@dataclass(frozen=True, slots=True)
class SessionClosePersistenceResult:
    audit_id: UUID
    current_audit_id: UUID
    current_status: str
    pointer_advanced: bool


def persist_session_close_coverage_cursor(
    cursor,
    coverage: SessionCloseCoverage,
) -> SessionClosePersistenceResult:
    """Persist immutable audit/anomaly evidence and advance its bounded pointer."""
    if not isinstance(coverage, SessionCloseCoverage):
        raise TypeError("coverage must be a SessionCloseCoverage")
    if coverage.missing_ranges_truncated:
        raise ValueError("truncated close coverage cannot be persisted")
    if sum(row.missing_members for row in coverage.missing_ranges) != coverage.missing_members:
        raise ValueError("close coverage ranges do not account for every missing member")
    audit_id = session_close_audit_id(coverage)
    values = (
        str(audit_id),
        str(coverage.cohort_id),
        coverage.provider_tier,
        coverage.session.session_date,
        "5m",
        coverage.cohort_effective_at,
        str(coverage.manifest_id),
        coverage.manifest_sha256,
        coverage.universe_sha256,
        coverage.session.opened_at,
        coverage.session.closed_at,
        coverage.eligible_at,
        coverage.eligibility_policy.sha256,
        coverage.known_by,
        coverage.instrument_count,
        coverage.session.expected_per_instrument,
        coverage.expected_members,
        coverage.complete_members,
        0,
        coverage.missing_members,
        coverage.missing_range_count,
        coverage.status,
        coverage.expected_manifest_sha256,
        coverage.input_sha256,
        coverage.coverage_sha256,
        max(0, round(coverage.query_seconds * 1_000_000)),
    )
    cursor.execute(
        "INSERT INTO rt.session_close_audits ("
        "audit_id, cohort_id, provider_tier, session_date, interval, "
        "cohort_effective_at, manifest_id, manifest_sha256, universe_sha256, "
        "session_open, session_close, eligible_at, eligibility_policy_sha256, "
        "known_by, instrument_count, "
        "expected_per_instrument, expected_members, complete_members, "
        "no_trade_members, missing_members, missing_range_count, status, "
        "expected_manifest_sha256, input_sha256, coverage_sha256, query_microseconds"
        ") VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s,%s,%s,%s,%s,%s"
        ") ON CONFLICT (audit_id) DO NOTHING",
        values,
    )
    if coverage.missing_ranges:
        execute_values(
            cursor,
            "INSERT INTO rt.session_close_anomalies ("
            "audit_id, instrument_id, ticker, reason_code, requested_from, "
            "requested_to, missing_members"
            ") VALUES %s ON CONFLICT DO NOTHING",
            [
                (
                    str(audit_id),
                    row.instrument_id,
                    row.ticker,
                    "FINAL_BAR_UNAVAILABLE",
                    row.requested_from,
                    row.requested_to,
                    row.missing_members,
                )
                for row in coverage.missing_ranges
            ],
            page_size=500,
        )
    cursor.execute(
        "SELECT audit_id::text, cohort_id::text, provider_tier, session_date, "
        "manifest_id::text, expected_members, complete_members, missing_members, "
        "missing_range_count, status, eligible_at, eligibility_policy_sha256, "
        "expected_manifest_sha256, input_sha256, coverage_sha256, known_by "
        "FROM rt.session_close_audits WHERE audit_id=%s",
        (str(audit_id),),
    )
    stored = cursor.fetchone()
    expected = (
        str(audit_id),
        str(coverage.cohort_id),
        coverage.provider_tier,
        coverage.session.session_date,
        str(coverage.manifest_id),
        coverage.expected_members,
        coverage.complete_members,
        coverage.missing_members,
        coverage.missing_range_count,
        coverage.status,
        coverage.eligible_at,
        coverage.eligibility_policy.sha256,
        coverage.expected_manifest_sha256,
        coverage.input_sha256,
        coverage.coverage_sha256,
    )
    if stored is None or tuple(stored[:-1]) != expected:
        raise ValueError("persisted session-close audit identity conflict")
    stored_known_by = stored[-1]
    cursor.execute(
        "SELECT count(*), COALESCE(sum(missing_members), 0) "
        "FROM rt.session_close_anomalies WHERE audit_id=%s",
        (str(audit_id),),
    )
    anomaly_count, missing_count = cursor.fetchone()
    if (
        int(anomaly_count) != coverage.missing_range_count
        or int(missing_count) != coverage.missing_members
    ):
        raise ValueError("persisted session-close anomalies are incomplete")
    cursor.execute(
        "INSERT INTO rt.session_close_current ("
        "cohort_id, provider_tier, session_date, audit_id, known_by, status"
        ") VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (cohort_id, provider_tier, session_date) DO UPDATE SET "
        "audit_id=EXCLUDED.audit_id, known_by=EXCLUDED.known_by, "
        "status=EXCLUDED.status, updated_at=clock_timestamp() "
        "WHERE rt.session_close_current.known_by <= EXCLUDED.known_by "
        "AND rt.session_close_current.audit_id IS DISTINCT FROM EXCLUDED.audit_id "
        "AND NOT (rt.session_close_current.status='COMPLETE' "
        "AND EXCLUDED.status='INCOMPLETE')",
        (
            str(coverage.cohort_id),
            coverage.provider_tier,
            coverage.session.session_date,
            str(audit_id),
            stored_known_by,
            coverage.status,
        ),
    )
    pointer_advanced = cursor.rowcount > 0
    cursor.execute(
        "SELECT audit_id::text, known_by, status "
        "FROM rt.session_close_current "
        "WHERE cohort_id=%s AND provider_tier=%s AND session_date=%s",
        (
            str(coverage.cohort_id),
            coverage.provider_tier,
            coverage.session.session_date,
        ),
    )
    current = cursor.fetchone()
    if current is None:
        raise ValueError("session-close current pointer is unavailable")
    current_id, _current_known_by, current_status = current
    return SessionClosePersistenceResult(
        audit_id,
        UUID(str(current_id)),
        str(current_status),
        pointer_advanced,
    )
