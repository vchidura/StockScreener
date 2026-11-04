"""Which stored publication the portal serves when a reader names no cohort.

A cohort remains the integrity identity of every stored row and every explicit
read. Each session has its own cohort, so "latest" is resolved here from durable
state instead of being compiled into a client. The pointer moves only when a
release exists, so a new session never blanks the prior session's publication.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID


PORTAL_IDENTITY_CONTRACT = "realtime-portal-current-v1"
PORTAL_IDENTITY_SESSION_LIMIT = 32
CURRENT = "CURRENT"
PRIOR_SESSION = "PRIOR_SESSION"
AWAITING_FIRST_RELEASE = "AWAITING_FIRST_RELEASE"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PortalPublicationIdentity:
    cohort_id: UUID
    session_date: date
    status: str
    latest_session_date: date
    release_id: UUID | None = None
    cutoff: datetime | None = None

    def payload(self) -> dict[str, object]:
        return {
            "cohort_id": str(self.cohort_id),
            "session_date": self.session_date.isoformat(),
            "status": self.status,
            "latest_session_date": self.latest_session_date.isoformat(),
            "release_id": (
                None if self.release_id is None else str(self.release_id)
            ),
            "cutoff": None if self.cutoff is None else self.cutoff.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class PortalIdentity:
    equity: PortalPublicationIdentity | None
    options: PortalPublicationIdentity | None
    session_cohorts: Mapping[str, UUID]

    def payload(
        self,
        available_by: datetime,
        *,
        options_reads_enabled: bool,
    ) -> dict[str, object]:
        return {
            "contract": PORTAL_IDENTITY_CONTRACT,
            "available_by": available_by.isoformat(),
            "equity": None if self.equity is None else self.equity.payload(),
            "options": (
                None
                if self.options is None or not options_reads_enabled
                else self.options.payload()
            ),
            "options_reads_enabled": options_reads_enabled,
            "session_cohorts": {
                session: str(cohort)
                for session, cohort in sorted(self.session_cohorts.items())
            },
            "provider_fetch_performed": False,
        }


def _row(row, names: tuple[str, ...]) -> dict[str, object]:
    if isinstance(row, Mapping):
        return {name: row[name] for name in names}
    return dict(zip(names, row, strict=True))


_EQUITY_NAMES = (
    "cohort_id",
    "session_date",
    "release_id",
    "cutoff",
    "release_session_date",
)
_OPTIONS_NAMES = ("cohort_id", "release_id", "cutoff", "session_date")


def load_portal_identity_cursor(
    cursor,
    *,
    available_by: datetime,
) -> PortalIdentity:
    """Only desired-state cohorts are served; one-shot and diagnostic cohorts are
    reachable solely by naming them."""
    available = _utc(available_by, "available_by")
    cursor.execute(
        "SELECT snapshot.cohort_id::text AS cohort_id,snapshot.session_date,"
        "pointer.release_id::text AS release_id,pointer.cutoff,"
        "pointer.session_date AS release_session_date "
        "FROM rt.realtime_desired_session_snapshots AS snapshot "
        "LEFT JOIN rt.portal_release_current AS pointer "
        "ON pointer.cohort_id=snapshot.cohort_id "
        "AND pointer.provider_tier='MASSIVE_ADVANCED' "
        "AND pointer.cutoff<=%s "
        "WHERE snapshot.materialized_at<=%s "
        "ORDER BY snapshot.session_date DESC,snapshot.materialized_at DESC,"
        "snapshot.cohort_id LIMIT %s",
        (available, available, PORTAL_IDENTITY_SESSION_LIMIT),
    )
    rows = tuple(_row(row, _EQUITY_NAMES) for row in cursor.fetchall())
    session_cohorts: dict[str, UUID] = {}
    for row in rows:
        session_cohorts.setdefault(
            row["session_date"].isoformat(),
            UUID(str(row["cohort_id"])),
        )
    equity = None
    if rows:
        latest = rows[0]["session_date"]
        published = next(
            (row for row in rows if row["release_id"] is not None),
            None,
        )
        equity = (
            PortalPublicationIdentity(
                cohort_id=UUID(str(rows[0]["cohort_id"])),
                session_date=latest,
                status=AWAITING_FIRST_RELEASE,
                latest_session_date=latest,
            )
            if published is None
            else PortalPublicationIdentity(
                cohort_id=UUID(str(published["cohort_id"])),
                session_date=published["release_session_date"],
                status=(
                    CURRENT
                    if published["session_date"] == latest
                    else PRIOR_SESSION
                ),
                latest_session_date=latest,
                release_id=UUID(str(published["release_id"])),
                cutoff=_utc(published["cutoff"], "release cutoff"),
            )
        )
    # Options readers require an enabled read pin, so only a pinned release
    # can be served; the pin is also what the Options API role may read.
    cursor.execute(
        "SELECT pin.cohort_id::text AS cohort_id,"
        "pin.release_id::text AS release_id,pin.release_cutoff AS cutoff,"
        "release.session_date "
        "FROM rt.advanced_options_read_pins AS pin "
        "JOIN rt.portal_releases AS release "
        "ON release.release_id=pin.release_id "
        "AND release.cutoff=pin.release_cutoff "
        "WHERE pin.enabled AND pin.release_cutoff<=%s "
        "ORDER BY pin.release_cutoff DESC,pin.cohort_id LIMIT 1",
        (available,),
    )
    pinned = cursor.fetchone()
    options = None
    if pinned is not None:
        row = _row(pinned, _OPTIONS_NAMES)
        options = PortalPublicationIdentity(
            cohort_id=UUID(str(row["cohort_id"])),
            session_date=row["session_date"],
            status=CURRENT,
            latest_session_date=row["session_date"],
            release_id=UUID(str(row["release_id"])),
            cutoff=_utc(row["cutoff"], "Options release cutoff"),
        )
    return PortalIdentity(equity, options, session_cohorts)


class PortalIdentityRepository:
    def __init__(self, cursor_factory: Callable) -> None:
        if not callable(cursor_factory):
            raise TypeError("portal identity cursor factory is invalid")
        self._cursor_factory = cursor_factory

    def current(self, *, available_by: datetime) -> PortalIdentity:
        with self._cursor_factory() as cursor:
            return load_portal_identity_cursor(
                cursor,
                available_by=available_by,
            )
