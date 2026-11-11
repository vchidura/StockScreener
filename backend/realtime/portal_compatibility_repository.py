from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from threading import Lock
from typing import Callable
from uuid import UUID

import exchange_calendars

from .contracts import CANARY_UNIVERSE
from .contracts import canonical_sha256
from .advanced_options_read_pin import (
    load_enabled_advanced_options_release_cursor,
)
from .equity_bar_repository import load_bar_heads_cursor
from .screening_projection import REALTIME_FIELDS, REALTIME_GAP_CATALOG, SCREENING_BASIS
from . import screening_references
from .ticker_references import BarLineageChanged
from equity.calendar import latest_expected_market_time, staleness_state
from research.screening import (
    DEFERRED, GAP_VERSION, HOURLY_CATALOG, PATTERNS, VERSION, GapPredicate, digest,
    evaluate_gaps, query_generation, supported_hourly_contract,
)


_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,31}$")
RELEASE_CACHE_SIZE = 8
VERIFIED_GENERATION_CACHE_SIZE = 8
VERIFICATION_CHUNK_MEMBERS = 50
RELEASE_MANIFEST_VERSION = "realtime-portal-release-v1"
DEVELOPER_DELAY_MINUTES = 15

# Snapshot identity columns come from the catalog; the body is read from whichever
# retention class holds it. The alias keeps the v1 column names for the readers.
SNAPSHOT_RELATION = (
    "(SELECT catalog.snapshot_id, catalog.cohort_id, catalog.provider_tier, "
    "catalog.snapshot_class, catalog.snapshot_type, catalog.snapshot_key, "
    "catalog.session_date, catalog.source_generation, "
    "catalog.manifest_sha256 AS source_manifest_sha256, catalog.payload_sha256, "
    "catalog.generated_at, catalog.valid_until, "
    "COALESCE(hot.source_manifest, kept.source_manifest) AS source_manifest, "
    "COALESCE(hot.payload, kept.payload) AS payload "
    "FROM rt.snapshot_catalog AS catalog "
    "LEFT JOIN rt.portal_snapshot_hot AS hot ON catalog.snapshot_class='EPHEMERAL_PORTAL' "
    "AND hot.snapshot_id=catalog.snapshot_id AND hot.generated_at=catalog.generated_at "
    "LEFT JOIN rt.portal_snapshot_kept AS kept ON catalog.snapshot_class='SESSION_CLOSE' "
    "AND kept.snapshot_id=catalog.snapshot_id AND kept.session_date=catalog.session_date"
    ") AS snapshot"
)
SNAPSHOT_IDENTITY_RELATION = "rt.snapshot_catalog AS snapshot"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def action_evidence_until(
    generated_at: datetime,
    read_at: datetime,
    read_mode: str = "current",
) -> datetime:
    """The end of the period whose corporate actions can make a publication stale.

    A publication reports its session, and only the next session's publications
    can absorb actions recorded after that session closed. Without this bound,
    the nightly reference refresh would take every current read down until the
    next open. Retained reads still see every later action, to warn about them.
    """
    if read_mode == "retained":
        return read_at
    calendar = exchange_calendars.get_calendar("XNYS")
    session = _utc(generated_at, "generated_at").date().isoformat()
    if not calendar.is_session(session):
        return read_at
    return min(read_at, calendar.session_close(session).to_pydatetime())


def _row(value) -> dict[str, object] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    raise TypeError("realtime compatibility repositories require mapping cursors")


class BoundedCache:
    """Small thread-safe LRU for immutable values keyed by identity."""

    def __init__(self, limit: int):
        if limit <= 0:
            raise ValueError("cache limit must be positive")
        self._limit = limit
        self._guard = Lock()
        self._items: OrderedDict = OrderedDict()

    def get(self, key):
        with self._guard:
            value = self._items.get(key)
            if value is not None:
                self._items.move_to_end(key)
            return value

    def put(self, key, value) -> None:
        with self._guard:
            self._items[key] = value
            self._items.move_to_end(key)
            while len(self._items) > self._limit:
                self._items.popitem(last=False)

    def __len__(self) -> int:
        with self._guard:
            return len(self._items)


# R33 (Nov 10): a read-time Options alert mark older than this reads STALE (after the close every mark ages past it).
ADVANCED_CURRENT_MARK_STALE_SECONDS = 900
# R33: an alert's market facts at its trigger never change; once complete they are kept by evaluation id.
_ALERT_SOURCE_FACTS = BoundedCache(4096)


def _mark_market_time(value) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value, "mark market_time") if value.tzinfo else None
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _age_stored_mark(row, cutoff: datetime) -> None:
    """R33: a kept mark's age is measured at the read, not left as it was when the window stored it."""
    mark = row.get("current_mark")
    if not isinstance(mark, Mapping) or mark.get("status") not in ("FRESH", "STALE"):
        return
    market_time = _mark_market_time(mark.get("market_time"))
    if market_time is None:
        return
    age = max(0, int((cutoff - market_time).total_seconds()))
    stale = age > ADVANCED_CURRENT_MARK_STALE_SECONDS
    row["current_mark"] = {
        **mark,
        "age_seconds": age,
        "maximum_age_seconds": ADVANCED_CURRENT_MARK_STALE_SECONDS,
        "status": "STALE" if stale else mark.get("status"),
        "reason": "CURRENT_MARK_OLDER_THAN_15_MINUTES" if stale else mark.get("reason"),
    }


def _fill_calendar_dte(row, original) -> None:
    """R33: the alert's calendar days to expiration from its session, when the package does not carry it."""
    if original.get("calendar_dte") is not None:
        return
    try:
        expiration = date.fromisoformat(str(original.get("expiration_date")))
        session = date.fromisoformat(str(row.get("session_date")))
    except ValueError:
        return
    if expiration >= session:
        original["calendar_dte"] = (expiration - session).days


def _apply_source_facts(legs, facts) -> None:
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        if leg.get("spot") is None and facts.get("spot") is not None:
            leg["spot"] = facts["spot"]
            leg["spot_market_data_time"] = facts["spot_time"]
        contract = facts["contracts"].get(str(leg.get("contract_ticker")))
        if contract is not None:
            if leg.get("day_volume") is None and contract["day_volume"] is not None:
                leg["day_volume"] = contract["day_volume"]
            if leg.get("open_interest") is None and contract["open_interest"] is not None:
                leg["open_interest"] = contract["open_interest"]
            leg.setdefault("source_event_time", contract["event_time"])
        leg["market_facts_basis"] = "TRIGGER_FLOW_EVENT_AND_1M_CLOSE"


@dataclass(frozen=True, slots=True)
class ReleaseView:
    release_id: UUID
    cohort_id: UUID
    provider_tier: str
    release_class: str
    session_date: date
    cutoff: datetime
    entries: Mapping[tuple[str, str], Mapping[str, object]]
    publication_generation: int | None = None
    desired_snapshot_sha256: str | None = None
    membership_sha256: str | None = None
    deliverable: bool = True
    publication_disposition: str = "CURRENT"

    def entry(self, snapshot_type: str, snapshot_key: str) -> Mapping[str, object] | None:
        return self.entries.get((snapshot_type, snapshot_key))

    def entries_of(self, snapshot_type: str) -> tuple[Mapping[str, object], ...]:
        return tuple(
            entry for (kind, _), entry in sorted(self.entries.items()) if kind == snapshot_type
        )


def load_release(cursor, release_id, cutoff, cache: BoundedCache | None = None) -> ReleaseView:
    key = str(release_id)
    cached = cache.get(key) if cache is not None else None
    if cached is not None:
        return cached
    cursor.execute(
        "SELECT release_id, cohort_id, provider_tier, release_class, session_date, cutoff, "
        "manifest, manifest_sha256 FROM rt.portal_releases WHERE release_id=%s AND cutoff=%s",
        (key, cutoff),
    )
    row = _row(cursor.fetchone())
    if row is None:
        raise ValueError("Portal release is not retained")
    manifest = row["manifest"]
    if (
        canonical_sha256(manifest) != row["manifest_sha256"]
        or manifest.get("version") != RELEASE_MANIFEST_VERSION
        or manifest.get("release_class") != row["release_class"]
    ):
        raise ValueError("Portal release identity/checksum drift")
    entries = {
        (entry["snapshot_type"], entry["snapshot_key"]): dict(entry)
        for entry in manifest["snapshots"]
    }
    if len(entries) != len(manifest["snapshots"]):
        raise ValueError("Portal release names a snapshot identity twice")
    view = ReleaseView(
        release_id=UUID(str(row["release_id"])), cohort_id=UUID(str(row["cohort_id"])),
        provider_tier=row["provider_tier"], release_class=row["release_class"],
        session_date=row["session_date"], cutoff=row["cutoff"], entries=entries,
        publication_generation=manifest.get("publication_generation"),
        desired_snapshot_sha256=manifest.get(
            "desired_snapshot_sha256"
        ),
        membership_sha256=manifest.get("membership_sha256"),
        deliverable=bool(manifest.get("deliverable", True)),
        publication_disposition=manifest.get(
            "publication_disposition",
            "CURRENT",
        ),
    )
    if cache is not None:
        cache.put(key, view)
    return view


def load_current_release(cursor, cohort_id, cache: BoundedCache | None = None) -> ReleaseView | None:
    """The release the single atomic pointer names for the cohort, or None."""
    cursor.execute(
        "SELECT release_id, cutoff, session_date, provider_tier FROM rt.portal_release_current "
        "WHERE cohort_id=%s ORDER BY cutoff DESC LIMIT 1",
        (str(cohort_id),),
    )
    pointer = _row(cursor.fetchone())
    if pointer is None:
        return None
    return load_release(cursor, pointer["release_id"], pointer["cutoff"], cache)


def load_session_close_releases(cursor, cohort_id, session_date: date) -> tuple[ReleaseView, ...]:
    """Frozen session-close releases of one session, newest first."""
    cursor.execute(
        "SELECT release_id, cutoff FROM rt.portal_releases WHERE cohort_id=%s "
        "AND release_class='SESSION_CLOSE' AND session_date=%s ORDER BY cutoff DESC, release_id",
        (str(cohort_id), session_date),
    )
    rows = tuple(_row(row) for row in cursor.fetchall())
    return tuple(load_release(cursor, row["release_id"], row["cutoff"]) for row in rows)


def availability_epoch(available_by: datetime, provider_tier: str, interval: str = "1d") -> str:
    delay = timedelta(minutes=DEVELOPER_DELAY_MINUTES if provider_tier == "MASSIVE_DEVELOPER" else 0)
    return latest_expected_market_time(_utc(available_by, "available_by") - delay, interval).isoformat()


def portal_etag(release_id, epoch: str) -> str:
    return '"' + sha256(f"{release_id}:{epoch}".encode("utf-8")).hexdigest()[:32] + '"'


def _active_cohort(
    cursor,
    available_by: datetime,
    cohort_id: UUID | None = None,
) -> dict[str, object] | None:
    if cohort_id is None:
        cursor.execute(
            "SELECT cohort_id, provider_tier, effective_at FROM rt.portal_cohort_current "
            "WHERE scope='PLATFORM' AND effective_at<=%s",
            (available_by,),
        )
    else:
        cursor.execute(
            "SELECT generation.cohort_id, generation.provider_tier, "
            "generation.generated_at AS effective_at "
            "FROM rt.screening_current AS current "
            "JOIN rt.screening_generations AS generation "
            "ON generation.generation_id=current.generation_id "
            "WHERE current.cohort_id=%s "
            "AND generation.cohort_id=current.cohort_id "
            "AND generation.status='COMPLETE' AND generation.generated_at<=%s",
            (str(cohort_id), available_by),
        )
        active = _row(cursor.fetchone())
        if active is not None:
            return active
        # An Options cohort publishes releases without a screening generation.
        cursor.execute(
            "SELECT cohort_id, provider_tier, updated_at AS effective_at "
            "FROM rt.portal_release_current "
            "WHERE cohort_id=%s AND cutoff<=%s "
            "ORDER BY cutoff DESC LIMIT 1",
            (str(cohort_id), available_by),
        )
    return _row(cursor.fetchone())


def _unavailable_options_data_tier(active: dict[str, object] | None) -> str:
    return (
        "REAL-TIME ADVANCED RESEARCH DATA"
        if active is not None and active["provider_tier"] == "MASSIVE_ADVANCED"
        else "15-MINUTE DELAYED RESEARCH DATA"
    )


def _retained_options_envelope(
    result: Mapping[str, object],
    release: Mapping[str, object],
) -> dict[str, object]:
    return {
        **result,
        "release_id": release["release_id"],
        "release_cutoff": release["cutoff"].isoformat(),
        "retained_session_date": release["retained_session_date"],
        "valid_until": release["valid_until"],
        "checked_at": release["checked_at"],
        "stale": release["stale"],
        "staleness_seconds": release["staleness_seconds"],
        "serving_latest_available": release["serving_latest_available"],
    }


class _RealtimeCompatibilityRepository:
    def __init__(
        self,
        cursor_factory: Callable,
        *,
        transaction_configured: bool = False,
        require_read_pin: bool = False,
        reads_enabled: bool = True,
    ):
        self._cursor_factory = cursor_factory
        self._transaction_configured = transaction_configured
        if type(require_read_pin) is not bool:
            raise TypeError("Options read-pin policy is invalid")
        if type(reads_enabled) is not bool:
            raise TypeError("Options stored-read flag is invalid")
        self._require_read_pin = require_read_pin
        self._reads_enabled = reads_enabled
        self._release_cache = BoundedCache(RELEASE_CACHE_SIZE)

    def _cursor(self):
        return self._cursor_factory()

    def _begin(self, cursor) -> None:
        if not self._transaction_configured:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")

    def _release(self, cursor, cohort_id) -> ReleaseView | None:
        if not self._reads_enabled:
            return None
        if self._require_read_pin:
            release_pin = load_enabled_advanced_options_release_cursor(
                cursor,
                cohort_id=cohort_id,
            )
            if release_pin is None:
                return None
            release_id, release_cutoff = release_pin
            release = load_release(
                cursor,
                release_id,
                release_cutoff,
                cache=self._release_cache,
            )
        else:
            release = load_current_release(
                cursor,
                cohort_id,
                self._release_cache,
            )
        if (
            release is not None
            and release.cohort_id != UUID(str(cohort_id))
        ):
            raise ValueError("Portal release cohort identity mismatch")
        return release

    def _options_read_context(
        self,
        cursor,
        *,
        cohort_id: UUID,
        cutoff: datetime,
        session_date: date | None = None,
    ) -> tuple[
        dict[str, object] | None,
        ReleaseView | None,
        str | None,
    ]:
        if not self._reads_enabled:
            return None, None, "ADVANCED_OPTIONS_V2_DISABLED"
        if self._require_read_pin:
            release = self._release(cursor, cohort_id)
            if release is None:
                return (
                    None,
                    None,
                    "ADVANCED_OPTIONS_READ_PIN_UNAVAILABLE",
                )
            if (
                release.provider_tier != "MASSIVE_ADVANCED"
                or release.cutoff > cutoff
            ):
                raise ValueError(
                    "Advanced Options read pin release identity mismatch"
                )
            if (
                session_date is not None
                and release.session_date != session_date
            ):
                return (
                    {
                        "cohort_id": cohort_id,
                        "provider_tier": release.provider_tier,
                        "effective_at": release.cutoff,
                    },
                    release,
                    "OPTIONS_SESSION_RELEASE_UNAVAILABLE",
                )
            return (
                {
                    "cohort_id": cohort_id,
                    "provider_tier": release.provider_tier,
                    "effective_at": release.cutoff,
                },
                release,
                None,
            )
        active = _active_cohort(cursor, cutoff, cohort_id)
        if active is None:
            return None, None, "ACTIVE_COHORT_UNAVAILABLE"
        release = (
            self._release(cursor, cohort_id)
            if active["provider_tier"] == "MASSIVE_ADVANCED"
            else None
        )
        if (
            session_date is not None
            and release is not None
            and release.session_date != session_date
        ):
            return active, release, "OPTIONS_SESSION_RELEASE_UNAVAILABLE"
        return active, release, None

    def _latest_release_entries(
        self,
        cursor,
        cohort_id,
        snapshot_types,
        snapshot_key,
        cutoff,
    ):
        current = self._release(cursor, cohort_id)
        types = tuple(snapshot_types)
        entries = tuple(
            entry
            for snapshot_type in types
            if (entry := current.entry(snapshot_type, snapshot_key) if current is not None else None)
            is not None
        )
        if entries:
            return entries
        cursor.execute(
            "SELECT release_id, cutoff FROM rt.portal_releases "
            "WHERE cohort_id=%s AND cutoff<=%s "
            "ORDER BY session_date DESC, cutoff DESC, release_id LIMIT 32",
            (str(cohort_id), cutoff),
        )
        for row in cursor.fetchall():
            if current is not None and str(row["release_id"]) == str(current.release_id):
                continue
            release = load_release(
                cursor,
                row["release_id"],
                row["cutoff"],
                cache=self._release_cache,
            )
            entries = tuple(
                entry
                for snapshot_type in types
                if (entry := release.entry(snapshot_type, snapshot_key)) is not None
            )
            if entries:
                return entries
        return ()

    def _advanced_options_page(
        self,
        cursor,
        *,
        cohort_id: UUID,
        available_by: datetime,
        artifact_kind: str,
        session_date: date | None = None,
    ):
        release = self._release(cursor, cohort_id)
        if release is None:
            return None
        if (
            session_date is not None
            and release.session_date != session_date
        ):
            return None
        from .advanced_options_projection import (
            load_advanced_options_page_release_cursor,
        )

        return load_advanced_options_page_release_cursor(
            cursor,
            cohort_id=cohort_id,
            available_by=available_by,
            artifact_kind=artifact_kind,
            release_id=release.release_id,
            release_cutoff=release.cutoff,
        )

    def release_etag(
        self,
        *,
        available_by: datetime,
        cohort_id: UUID,
        interval: str = "1d",
    ) -> str | None:
        """ETag from the current release id and the availability epoch: the response can
        only change when the release rotates or the expected market time advances."""
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            release = self._release(cursor, cohort_id)
        if release is None:
            return None
        return portal_etag(
            release.release_id, availability_epoch(cutoff, release.provider_tier, interval)
        )


class RealtimeOptionFlowRepository(_RealtimeCompatibilityRepository):
    def flow(
        self,
        *,
        available_by: datetime,
        cohort_id: UUID | None = None,
        underlyer: str | None = None,
        session_date: date | None = None,
        read_mode: str = "current",
    ) -> dict[str, object]:
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Options Flow requires an explicit realtime cohort")
        if read_mode not in {"current", "retained"}:
            raise ValueError("Invalid Options Flow read mode")
        symbol = underlyer.strip().upper() if underlyer else None
        if symbol is not None and not _TICKER_PATTERN.fullmatch(symbol):
            raise ValueError("Invalid Options Flow underlying")
        with self._cursor() as cursor:
            self._begin(cursor)
            active, resolved_release, reason = self._options_read_context(
                cursor,
                cohort_id=cohort_id,
                cutoff=cutoff,
                session_date=session_date,
            )
            if active is not None and reason is None:
                cursor.execute(
                    "SELECT manifest.option_underlying_count "
                    "FROM rt.cohorts AS cohort "
                    "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                    "WHERE cohort.cohort_id=%s AND cohort.created_at<=%s "
                    "AND cohort.effective_at<=%s AND manifest.created_at<=%s",
                    (str(cohort_id), cutoff, cutoff, cutoff),
                )
                scope = _row(cursor.fetchone())
                if scope is None:
                    raise ValueError("Options Flow cohort manifest identity is unavailable")
                reason = (
                    "EQUITY_ONLY_COHORT"
                    if int(scope["option_underlying_count"]) == 0
                    else "NO_COMPLETE_MATRIX"
                )
                if int(scope["option_underlying_count"]) > 0:
                    if active["provider_tier"] == "MASSIVE_ADVANCED":
                        from .advanced_options_projection import (
                            load_advanced_options_page_release_cursor,
                        )

                        retained = (
                            None
                            if resolved_release is None
                            else load_advanced_options_page_release_cursor(
                                cursor,
                                cohort_id=cohort_id,
                                available_by=cutoff,
                                artifact_kind="FLOW",
                                release_id=resolved_release.release_id,
                                release_cutoff=resolved_release.cutoff,
                            )
                        )
                        if retained is not None:
                            publication, release = retained
                            return _flow_with_breadth(
                                cursor,
                                _retained_options_envelope(
                                    publication.read(
                                    underlyer=symbol,
                                    cohort_id=cohort_id,
                                    provider_tier=publication.provider_tier,
                                    scope_sha256=release["scope_sha256"],
                                    ),
                                    release,
                                ),
                                cutoff=cutoff,
                            )
                    else:
                        from .forward_reader import (
                            load_forward_release_cursor,
                        )

                        retained = load_forward_release_cursor(
                            cursor,
                            cohort_id=cohort_id,
                            available_by=cutoff,
                            artifact_kind="FLOW",
                        )
                    if retained is not None:
                        publication, release = retained
                        publication = (
                            (
                                next(iter(publication.values()), None)
                                if symbol is None
                                else publication.get(symbol)
                            )
                            if isinstance(publication, dict)
                            else publication
                        )
                        if publication is None:
                            from .forward_reader import ForwardPublicationUnavailable
                            raise ForwardPublicationUnavailable("Requested Flow underlying is outside the approved release")
                        result = publication.read(cohort_id=cohort_id, provider_tier=publication.provider_tier,
                            scope_sha256=release["scope_sha256"])
                        if (
                            symbol is not None
                            and symbol != result["data"]["selected"]
                        ):
                            from .forward_reader import ForwardPublicationUnavailable
                            raise ForwardPublicationUnavailable("Requested underlying Flow profile is not retained in this release")
                        return result
        return {
            "available": False,
            "reason": reason,
            "data_tier": _unavailable_options_data_tier(active),
            "generated_at": cutoff,
            "as_of": None,
            "observed_at": None,
            "policy_sha256": None,
            "model_version": None,
            "data": {
                "underlyers": [],
                "selected": symbol,
                "session_date": str(session_date) if session_date else None,
            },
        }


def _optional_read(read: str, *, served: bool, source: str, fallback_source: str, reason_code: str) -> dict:
    """INV-9 (Nov 10 review R7): what an optional read served, in the response. AVAILABLE when the read succeeded;
    FALLBACK with a reason code when it failed and its fallback is served (the error itself goes to the log)."""
    return {
        "read": read,
        "status": "AVAILABLE" if served else "FALLBACK",
        "reason_code": None if served else reason_code,
        "serving_source": source if served else fallback_source,
    }


def _with_optional_reads(envelope, *reads: dict):
    """Adds `data.optional_reads` to a page envelope; an envelope without a data object is returned unchanged."""
    if not isinstance(envelope, Mapping) or not isinstance(envelope.get("data"), Mapping):
        return envelope
    data = envelope["data"]
    return {**envelope, "data": {**data, "optional_reads": [*(data.get("optional_reads") or ()), *reads]}}


def _log_optional_read_failure(message: str) -> None:
    import logging

    logging.getLogger(__name__).warning(message, exc_info=True)


def _flow_with_breadth(cursor, envelope, *, cutoff):
    """Nov 10 (user): Flow and Activity show the full listed chain's volume and open interest from breadth in place of
    the streamed contracts; any failure serves the release as published and says so (INV-9)."""
    from .advanced_options_breadth import load_breadth_screener_rows_cursor
    from .option_flow_breadth import breadth_flow_data

    data = envelope.get("data") if isinstance(envelope, Mapping) else None
    if not isinstance(data, Mapping) or not data.get("underlyers"):
        return envelope
    saved = False
    try:
        session_value = data.get("session_date") or envelope.get("retained_session_date")
        session_date = session_value if isinstance(session_value, date) else date.fromisoformat(str(session_value))
        underlyings = tuple(str(item["underlying"]) for item in data["underlyers"])
        cursor.execute("SAVEPOINT options_flow_breadth")
        saved = True
        rows, captures = load_breadth_screener_rows_cursor(
            cursor, session_date=session_date, underlyings=underlyings, available_by=cutoff,
        )
        cursor.execute("RELEASE SAVEPOINT options_flow_breadth")
        served = {**envelope, "data": breadth_flow_data(data, rows, captures)}
        # R48: the selected ticker served from the streamed rows (no COMPLETE capture) is reported as the fallback.
        selected_breadth = not (served["data"].get("breadth_fallback") or {}).get("selected")
        return _with_optional_reads(served, _optional_read(
            "flow_breadth", served=selected_breadth, source="BREADTH", fallback_source="RELEASE_ROWS",
            reason_code="BREADTH_UNAVAILABLE"))
    except Exception:  # noqa: BLE001 - breadth never fails the page; the response and the log say it fell back
        if saved:
            try:
                cursor.execute("ROLLBACK TO SAVEPOINT options_flow_breadth")
            except Exception:  # noqa: BLE001
                _log_optional_read_failure("Options Flow breadth savepoint rollback failed")
        _log_optional_read_failure("Options Flow breadth unavailable; serving the release's rows")
        return _with_optional_reads(envelope, _optional_read(
            "flow_breadth", served=False, source="BREADTH", fallback_source="RELEASE_ROWS",
            reason_code="BREADTH_UNAVAILABLE"))


def _option_breadth(cursor, publication, *, cutoff):
    """G75: the chain's breadth rows for the Screener at the read time, or None (the published rows alone) when they
    cannot be read; breadth never fails the page. The caller reports which one was served (INV-9)."""
    from .advanced_options_breadth import load_breadth_screener_rows_cursor

    payload = getattr(publication, "_payload", None) or {}
    try:
        session_date = date.fromisoformat(str(payload["session_date"]))
        underlyings = tuple(str(item) for item in payload["underlyers"])
    except (KeyError, TypeError, ValueError):
        _log_optional_read_failure("Options breadth scope missing from the release; serving the release's rows")
        return None
    saved = False
    try:
        cursor.execute("SAVEPOINT options_breadth")
        saved = True
        result = load_breadth_screener_rows_cursor(
            cursor, session_date=session_date, underlyings=underlyings, available_by=cutoff,
        )
        cursor.execute("RELEASE SAVEPOINT options_breadth")
        return result
    except Exception:  # noqa: BLE001
        if saved:
            try:
                cursor.execute("ROLLBACK TO SAVEPOINT options_breadth")
            except Exception:  # noqa: BLE001
                _log_optional_read_failure("Options breadth savepoint rollback failed")
        _log_optional_read_failure("Options breadth unavailable; serving the release's rows")
        return None


class RealtimeOptionScreenerRepository(_RealtimeCompatibilityRepository):
    def eligible_chain(
        self,
        request,
        *,
        available_by: datetime,
        cohort_id: UUID | None = None,
    ) -> dict[str, object]:
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Options Screener requires an explicit realtime cohort")
        with self._cursor() as cursor:
            self._begin(cursor)
            active, resolved_release, reason = self._options_read_context(
                cursor,
                cohort_id=cohort_id,
                cutoff=cutoff,
                session_date=request.session_date,
            )
            if active is not None and reason is None:
                cursor.execute(
                    "SELECT manifest.option_underlying_count "
                    "FROM rt.cohorts AS cohort "
                    "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                    "WHERE cohort.cohort_id=%s AND cohort.created_at<=%s "
                    "AND cohort.effective_at<=%s AND manifest.created_at<=%s",
                    (str(cohort_id), cutoff, cutoff, cutoff),
                )
                scope = _row(cursor.fetchone())
                if scope is None:
                    raise ValueError("Options Screener cohort manifest identity is unavailable")
                reason = (
                    "EQUITY_ONLY_COHORT"
                    if int(scope["option_underlying_count"]) == 0
                    else "NO_ACTIVE_POLICY_ELIGIBLE_CHAIN"
                )
                if int(scope["option_underlying_count"]) > 0:
                    if active["provider_tier"] == "MASSIVE_ADVANCED":
                        from .advanced_options_projection import (
                            load_advanced_options_page_release_cursor,
                        )

                        retained = (
                            None
                            if resolved_release is None
                            else load_advanced_options_page_release_cursor(
                                cursor,
                                cohort_id=cohort_id,
                                available_by=cutoff,
                                artifact_kind="SCREENER",
                                release_id=resolved_release.release_id,
                                release_cutoff=resolved_release.cutoff,
                            )
                        )
                    else:
                        from .forward_reader import (
                            load_forward_release_cursor,
                        )

                        retained = load_forward_release_cursor(
                            cursor,
                            cohort_id=cohort_id,
                            available_by=cutoff,
                            artifact_kind="SCREENER",
                        )
                    if retained is not None:
                        publication, release = retained
                        breadth = (
                            _option_breadth(cursor, publication, cutoff=cutoff)
                            if active["provider_tier"] == "MASSIVE_ADVANCED"
                            else None
                        )
                        result = publication.query(
                            request,
                            cohort_id=cohort_id,
                            provider_tier=publication.provider_tier,
                            scope_sha256=release["scope_sha256"],
                            **({} if breadth is None else {
                                "breadth_rows": breadth[0], "breadth_coverage": breadth[1],
                            }),
                        )
                        if active["provider_tier"] != "MASSIVE_ADVANCED":
                            return result
                        return _with_optional_reads(
                            _retained_options_envelope(result, release),
                            _optional_read(
                                "screener_breadth", served=breadth is not None, source="BREADTH",
                                fallback_source="RELEASE_ROWS", reason_code="BREADTH_UNAVAILABLE",
                            ),
                        )
        requested = [request.underlyer] if request.underlyer else []
        return {
            "available": False,
            "reason": reason,
            "data_tier": _unavailable_options_data_tier(active),
            "generated_at": cutoff,
            "as_of": None,
            "observed_at": None,
            "policy_sha256": None,
            "model_version": None,
            "data": {
                "rows": [], "total": 0, "coverage": [],
                "requested_underlyers": requested,
                "missing_underlyers": requested,
                "coverage_status": "MISSING",
                "selection_basis": "LATEST_COMPLETE_MATRIX_PER_UNDERLYING",
                "serving_mode": "CURRENT_POLICY",
                "session_date": str(request.session_date) if request.session_date else None,
                "limit": request.limit, "offset": request.offset,
                "filters": [item.model_dump() for item in request.filters],
                "quote_liquidity": "NOT_AVAILABLE",
                "execution_mode": "READ_ONLY_RESEARCH",
            },
        }


class RealtimeOptionsAlertsPageRepository(_RealtimeCompatibilityRepository):
    _REASONS = {
        "datasets": "DETECTOR_DATASET_INDEX_UNAVAILABLE",
        "schedule": "DETECTOR_SCHEDULE_UNAVAILABLE",
        "evaluations": "DETECTOR_EVALUATION_UNAVAILABLE",
        "detector_runs": "DETECTOR_ALERTS_UNAVAILABLE",
        "candidate": "CANDIDATE_UNAVAILABLE",
        "quotes": "ADVANCED_QUOTES_UNAVAILABLE",
        "preview": "ASSESSMENT_UNAVAILABLE",
    }

    @staticmethod
    def _advanced_session_alerts(cursor, publication, *, cohort_id, cutoff):
        """G102 A0 (G100): the session's alerts from `rt.alert_current` (one per detector and contract, first
        triggered by the release cutoff). None when they cannot be read: the release's own rows are served then, as
        before, and the caller says so (INV-9). A savepoint keeps a failed lookup from aborting the read's
        transaction."""
        from .advanced_options_projection import load_options_session_alert_rows_cursor

        saved = False
        try:
            cursor.execute("SAVEPOINT options_session_alerts")
            saved = True
            rows = load_options_session_alert_rows_cursor(
                cursor, cohort_id=cohort_id, session_date=publication.session_date, cutoff=cutoff,
            )
            cursor.execute("RELEASE SAVEPOINT options_session_alerts")
            return rows
        except Exception:  # noqa: BLE001 - the session list is additive; never fail the alert page for it
            if saved:
                try:
                    cursor.execute("ROLLBACK TO SAVEPOINT options_session_alerts")
                except Exception:  # noqa: BLE001
                    _log_optional_read_failure("Options session alerts savepoint rollback failed")
            _log_optional_read_failure("Options session alerts unavailable; serving the release's rows")
            return None

    def _advanced_current_marks_reported(self, cursor, envelope, *, cutoff):
        """`_advanced_current_marks` under a savepoint, reporting what was served (INV-9): read-time marks from the
        stored quotes, or the release's marks when the quote read fails."""
        saved = False
        try:
            cursor.execute("SAVEPOINT options_current_marks")
            saved = True
            marked = self._advanced_current_marks(cursor, envelope, cutoff=cutoff)
            cursor.execute("RELEASE SAVEPOINT options_current_marks")
            served = True
        except Exception:  # noqa: BLE001 - marks are optional; never fail the alert list
            if saved:
                try:
                    cursor.execute("ROLLBACK TO SAVEPOINT options_current_marks")
                except Exception:  # noqa: BLE001
                    _log_optional_read_failure("Options alert current marks savepoint rollback failed")
            _log_optional_read_failure("Advanced Options alert current marks unavailable; serving the release's marks")
            marked, served = envelope, False
        return _with_optional_reads(marked, _optional_read(
            "current_marks", served=served, source="STORED_QUOTES", fallback_source="RELEASE_MARKS",
            reason_code="CURRENT_MARKS_UNAVAILABLE"))

    def _advanced_current_marks(self, cursor, envelope, *, cutoff):
        """Options alerts v1 (Nov 6, G78 principle): each alert's current price from the stored Options quotes.

        Advanced has no package-mark producer, so every Advanced alert showed its current mark as unavailable. For
        each row with an original package, the exit premium is the sum of the legs' latest stored NBBO midpoints
        (`rt.quote_current`, recorded by the read time) with the package sign rule (SELL +, BUY -); gross P/L is entry
        minus exit premium and Price P/L is gross over capital at risk, in the same fields a persisted mark carries.

        R33 (Nov 10): a mark the window stored (the session list keeps the one from the window that selected the
        alert) is replaced whenever the stored quotes are newer; before, a stored FRESH mark was never refreshed and
        read "0.0 hours old" an hour later. Age is from the quote's market time at the read; older than 15 minutes is
        STALE. The window's estimated cost is kept, so net P/L follows the new gross. A failed or incomplete lookup
        leaves the row's mark as it was, aged at the read."""
        from decimal import Decimal, InvalidOperation

        rows = (envelope.get("data") or {}).get("rows") or []
        pending = []
        contract_ids = set()
        for row in rows:
            original = row.get("original_package") or {}
            legs = original.get("legs") or []
            if not legs:
                continue
            pending.append((row, original, legs))
            contract_ids.update(int(leg["contract_id"]) for leg in legs)
        if not contract_ids:
            return envelope
        cursor.execute(
            "SELECT contract.contract_id, quote.bid, quote.ask, quote.event_at, quote.received_at "
            "FROM rt.quote_current AS quote "
            "JOIN rt.instruments AS instrument USING (instrument_id) "
            "JOIN ref.option_contracts AS contract USING (contract_id) "
            "WHERE quote.provider='massive' AND quote.quality='GOOD' "
            "AND contract.contract_id=ANY(%s::bigint[]) "
            "AND quote.event_at<=%s AND quote.received_at<=%s",
            (sorted(contract_ids), cutoff, cutoff),
        )
        quotes = {int(item["contract_id"]): item for item in (_row(value) for value in cursor.fetchall())}
        for row, original, legs in pending:
            try:
                exit_premium = Decimal(0)
                market_times, observed_times = [], []
                for leg in legs:
                    quote = quotes.get(int(leg["contract_id"]))
                    if quote is None or quote["bid"] is None or quote["ask"] is None:
                        raise InvalidOperation("missing quote")
                    bid, ask = Decimal(str(quote["bid"])), Decimal(str(quote["ask"]))
                    if bid <= 0 or ask < bid:
                        raise InvalidOperation("unusable quote")
                    sign = Decimal(1) if str(leg["side"]).upper() == "SELL" else Decimal(-1)
                    exit_premium += sign * (bid + ask) / 2 * int(leg["ratio"]) * int(leg["multiplier"])
                    market_times.append(_utc(quote["event_at"], "quote event_at"))
                    observed_times.append(_utc(quote["received_at"], "quote received_at"))
                entry_premium = Decimal(str(original["net_premium"]))
                capital = Decimal(str(original["capital_at_risk"]))
                if capital <= 0:
                    raise InvalidOperation("capital at risk")
            except (InvalidOperation, KeyError, TypeError, ValueError):
                _age_stored_mark(row, cutoff)
                continue
            stored = row.get("current_mark") if isinstance(row.get("current_mark"), Mapping) else {}
            market_time, observed_time = min(market_times), max(observed_times)
            stored_time = _mark_market_time(stored.get("market_time"))
            if stored.get("status") in ("FRESH", "STALE") and stored_time is not None and stored_time >= market_time:
                _age_stored_mark(row, cutoff)
                continue
            gross = entry_premium - exit_premium
            try:
                cost = None if stored.get("estimated_cost") is None else Decimal(str(stored["estimated_cost"]))
            except InvalidOperation:
                cost = None
            net = None if cost is None else gross - cost
            age = max(0, int((cutoff - market_time).total_seconds()))
            stale = age > ADVANCED_CURRENT_MARK_STALE_SECONDS
            mark = {
                **stored,
                "status": "STALE" if stale else "FRESH",
                "reason": "CURRENT_MARK_OLDER_THAN_15_MINUTES" if stale else None,
                "market_time": market_time.isoformat(),
                "observed_time": observed_time.isoformat(),
                "oldest_leg_market_time": market_time.isoformat(),
                "age_seconds": age,
                "maximum_age_seconds": ADVANCED_CURRENT_MARK_STALE_SECONDS,
                "signed_package_mark": str(exit_premium),
                "package_price": str(abs(exit_premium)),
                "gross_pnl": str(gross),
                "price_return": str(gross / capital),
                "estimated_cost": None if cost is None else str(cost),
                "net_pnl": None if net is None else str(net),
                "net_return": None if net is None else str(net / capital),
                "basis": "STORED_NBBO_MIDPOINT",
                "price_return_basis": "GROSS_PNL_OVER_CAPITAL_AT_RISK",
                "net_return_basis": None if net is None else (stored.get("net_return_basis") or "NET_PNL_OVER_CAPITAL_AT_RISK"),
                "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
            }
            # The window's mark evidence ids name the older quotes, not these.
            mark.pop("source_snapshot_ids", None)
            row["current_mark"] = mark
        return envelope

    def _advanced_source_facts_reported(self, cursor, envelope, *, cutoff):
        """`_advanced_source_facts` under a savepoint, reporting what was served (INV-9)."""
        saved = False
        try:
            cursor.execute("SAVEPOINT options_source_facts")
            saved = True
            filled = self._advanced_source_facts(cursor, envelope, cutoff=cutoff)
            cursor.execute("RELEASE SAVEPOINT options_source_facts")
            served = True
        except Exception:  # noqa: BLE001 - source facts are optional; never fail the alert list
            if saved:
                try:
                    cursor.execute("ROLLBACK TO SAVEPOINT options_source_facts")
                except Exception:  # noqa: BLE001
                    _log_optional_read_failure("Options alert source facts savepoint rollback failed")
            _log_optional_read_failure("Advanced Options alert source facts unavailable; serving the alert rows")
            filled, served = envelope, False
        return _with_optional_reads(filled, _optional_read(
            "source_facts", served=served, source="TRIGGER_EVENTS", fallback_source="ALERT_ROWS",
            reason_code="SOURCE_FACTS_UNAVAILABLE"))

    def _advanced_source_facts(self, cursor, envelope, *, cutoff):
        """R33 (Nov 10): the market facts at each alert's trigger that its package legs do not carry.

        O1 packages carry the contract and its entry mark only, so Stock at source, % OTM, Volume and OI read
        Unavailable. Each comes from what was stored at the trigger: the contract's day volume and prior open
        interest from the alert's own flow event at or before the trigger (`rt.option_flow_events`), and the stock
        from the last realtime 1-minute close at or before the trigger, known by then (`rt.bar_revisions`). DTE is
        the calendar days from the alert's session to expiration. Values the legs already carry are kept; a value
        not found stays unavailable. Complete facts are kept by evaluation id (they never change)."""
        rows = (envelope.get("data") or {}).get("rows") or []
        wanted = []
        for row in rows:
            original = row.get("original_package")
            if not isinstance(original, dict) or not original.get("legs"):
                continue
            _fill_calendar_dte(row, original)
            if all(
                isinstance(leg, Mapping) and leg.get("spot") is not None and leg.get("day_volume") is not None
                and leg.get("open_interest") is not None
                for leg in original["legs"]
            ):
                continue
            wanted.append(row)
        if not wanted:
            return envelope
        facts, missing = {}, []
        for row in wanted:
            key = str(row.get("evaluation_id") or "")
            cached = _ALERT_SOURCE_FACTS.get(key) if key else None
            if cached is None:
                missing.append(row)
            else:
                facts[key] = cached
        if missing:
            for key, value in self._load_source_facts(cursor, missing, cutoff=cutoff).items():
                facts[key] = value
                if value["complete"]:
                    _ALERT_SOURCE_FACTS.put(key, value)
        for row in wanted:
            value = facts.get(str(row.get("evaluation_id") or ""))
            if value is not None:
                _apply_source_facts(row["original_package"]["legs"], value)
        return envelope

    def _load_source_facts(self, cursor, rows, *, cutoff):
        triggers = {}
        for row in rows:
            at = _mark_market_time(row.get("triggered_at"))
            if at is not None and row.get("evaluation_id") and row.get("underlyer"):
                triggers[str(row["evaluation_id"])] = (row, at)
        if not triggers:
            return {}
        event_ids = sorted({
            str(event_id) for row, _ in triggers.values() for event_id in (row.get("event_ids") or ())
        })
        events = {}
        if event_ids:
            earliest = min(at for _, at in triggers.values()) - timedelta(days=1)
            cursor.execute(
                "SELECT event_id,contract_ticker,event_time,cumulative_volume_after,prior_open_interest "
                "FROM rt.option_flow_events "
                "WHERE event_id=ANY(%s::uuid[]) AND event_time>=%s AND event_time<=%s",
                (event_ids, earliest, cutoff),
            )
            for item in (_row(value) for value in cursor.fetchall()):
                events[str(item["event_id"])] = item
        keys = sorted({(str(row["underlyer"]), at) for row, at in triggers.values()}, key=lambda item: (item[0], item[1]))
        cursor.execute(
            "SELECT t.ticker,t.at,b.close_price,b.bar_end "
            "FROM unnest(%s::text[],%s::timestamptz[]) AS t(ticker,at) "
            "CROSS JOIN LATERAL (SELECT close_price,bar_end FROM rt.bar_revisions AS r "
            "WHERE r.ticker=t.ticker AND r.interval='1m' AND r.source_kind='REALTIME_STREAM' "
            "AND r.bar_end<=t.at AND r.bar_end>t.at-interval '15 minutes' "
            "AND r.observed_at<=t.at AND r.observed_at>t.at-interval '1 hour' "
            "ORDER BY r.bar_end DESC,r.observed_at DESC LIMIT 1) AS b",
            ([ticker for ticker, _ in keys], [at for _, at in keys]),
        )
        spots = {}
        for item in (_row(value) for value in cursor.fetchall()):
            spots[(str(item["ticker"]), _utc(item["at"], "spot at"))] = item
        result = {}
        for key, (row, at) in triggers.items():
            contracts = {}
            for event_id in row.get("event_ids") or ():
                event = events.get(str(event_id))
                if event is None:
                    continue
                event_time = _utc(event["event_time"], "flow event_time")
                if event_time > at:
                    continue
                ticker = str(event["contract_ticker"])
                if ticker in contracts and contracts[ticker]["event_time"] >= event_time.isoformat():
                    continue
                contracts[ticker] = {
                    "event_time": event_time.isoformat(),
                    "day_volume": None if event["cumulative_volume_after"] is None else int(event["cumulative_volume_after"]),
                    "open_interest": None if event["prior_open_interest"] is None else int(event["prior_open_interest"]),
                }
            spot = spots.get((str(row["underlyer"]), at))
            legs = (row.get("original_package") or {}).get("legs") or ()
            result[key] = {
                "spot": None if spot is None else format(spot["close_price"], "f"),
                "spot_time": None if spot is None else _utc(spot["bar_end"], "spot bar_end").isoformat(),
                "contracts": contracts,
                "complete": spot is not None and all(
                    isinstance(leg, Mapping) and str(leg.get("contract_ticker")) in contracts
                    and contracts[str(leg.get("contract_ticker"))]["day_volume"] is not None
                    and contracts[str(leg.get("contract_ticker"))]["open_interest"] is not None
                    for leg in legs
                ),
            }
        return result

    def _retained_marks(self, cursor, publication, envelope, *, cutoff, cohort_id):
        import json
        from contextlib import nullcontext
        from psycopg2 import DatabaseError
        from options.config import ValuationPolicy
        from options.outcomes import review_retained_candidate_mark
        from .option_portal_repository import RealtimeOptionPortalMarkRepository

        if not envelope["available"] or not envelope["data"].get("rows"):
            return envelope
        stored = json.loads(publication.payload_json)
        if "valuation_policy" not in stored:
            return envelope
        policy = ValuationPolicy.model_validate(stored["valuation_policy"])
        if policy.policy_sha256 != stored["valuation_policy_sha256"]:
            raise ValueError("retained Alerts mark policy identity mismatch")
        rows = envelope["data"]["rows"]
        candidates = tuple(UUID(row["candidate_id"]) for row in rows if row.get("candidate_id"))
        if not candidates:
            return envelope
        mark_reason = "CURRENT_MARK_STORAGE_UNAVAILABLE"
        try:
            marks = RealtimeOptionPortalMarkRepository(lambda: nullcontext(cursor), transaction_configured=True).retained_plan_marks(
                candidates, available_by=cutoff, valuation_policy_sha256=policy.policy_sha256,
                cohort_id=cohort_id, provider_tier=publication.provider_tier)
        except DatabaseError:
            marks = dict(ready=False, rows={})
            mark_reason = "CURRENT_MARK_READ_UNAVAILABLE"
        for row in rows:
            if not row.get("candidate_id") or not row.get("original_package"):
                continue
            detail = stored.get("candidate_details", {}).get(row["candidate_id"])
            identity = detail["candidate"]["candidate_identity"] if detail else (row.get("observation") or {}).get("candidate_identity_sha256")
            if identity is None:
                continue
            original = row["original_package"]
            basis = dict(original, candidate_id=row["candidate_id"], matrix_id=row["matrix_id"], candidate_identity=identity)
            row["current_mark"] = review_retained_candidate_mark(basis, marks["rows"].get(row["candidate_id"]),
                checked_at=cutoff, policy=policy)
            if not marks["ready"]:
                row["current_mark"]["reason"] = mark_reason
        envelope["generated_at"] = cutoff
        return envelope

    def read(
        self,
        contract: str,
        *,
        available_by: datetime,
        cohort_id: UUID | None = None,
        requested_contract_ids: Sequence[int] = (),
        candidate_id: UUID | None = None,
        preview_request=None,
        query=None,
    ) -> dict[str, object]:
        if contract not in self._REASONS:
            raise ValueError("Unsupported Options Alerts contract")
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Options Alerts requires an explicit realtime cohort")
        requested_session = (query or {}).get("session_date")
        with self._cursor() as cursor:
            self._begin(cursor)
            active, resolved_release, reason = self._options_read_context(
                cursor,
                cohort_id=cohort_id,
                cutoff=cutoff,
                session_date=requested_session,
            )
            if active is not None and reason is None:
                cursor.execute(
                    "SELECT manifest.option_underlying_count "
                    "FROM rt.cohorts AS cohort "
                    "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                    "WHERE cohort.cohort_id=%s AND cohort.created_at<=%s "
                    "AND cohort.effective_at<=%s AND manifest.created_at<=%s",
                    (str(cohort_id), cutoff, cutoff, cutoff),
                )
                scope = _row(cursor.fetchone())
                if scope is None:
                    raise ValueError("Options Alerts cohort manifest identity is unavailable")
                reason = (
                    "EQUITY_ONLY_COHORT"
                    if int(scope["option_underlying_count"]) == 0
                    else self._REASONS[contract]
                )
                if int(scope["option_underlying_count"]) > 0:
                    advanced = (
                        active["provider_tier"] == "MASSIVE_ADVANCED"
                    )
                    if advanced:
                        from .advanced_options_projection import (
                            load_advanced_options_page_release_cursor,
                        )

                        retained = (
                            None
                            if resolved_release is None
                            else load_advanced_options_page_release_cursor(
                                cursor,
                                cohort_id=cohort_id,
                                available_by=cutoff,
                                artifact_kind="ALERTS",
                                release_id=resolved_release.release_id,
                                release_cutoff=resolved_release.cutoff,
                            )
                        )
                    else:
                        from .forward_reader import (
                            load_forward_release_cursor,
                        )

                        retained = load_forward_release_cursor(
                            cursor,
                            cohort_id=cohort_id,
                            available_by=cutoff,
                            artifact_kind="ALERTS",
                        )
                    if retained is not None:
                        publication, release = retained
                        identity = dict(cohort_id=cohort_id, provider_tier=publication.provider_tier, scope_sha256=release["scope_sha256"])
                        if contract == "datasets":
                            result = publication.datasets(**identity)
                            return (
                                _retained_options_envelope(result, release)
                                if advanced
                                else result
                            )
                        if contract == "schedule":
                            result = publication.schedule(
                                checked_at=cutoff,
                                **identity,
                            )
                            return (
                                _retained_options_envelope(result, release)
                                if advanced
                                else result
                            )
                        if contract == "evaluations":
                            from .option_alert_projection import DetectorEvaluationQuery
                            result = publication.evaluations(
                                DetectorEvaluationQuery(**(query or {})),
                                **identity,
                            )
                            return (
                                _retained_options_envelope(result, release)
                                if advanced
                                else result
                            )
                        if contract == "detector_runs":
                            from .option_alert_projection import DetectorRunQuery
                            request = DetectorRunQuery(**(query or {}))
                            if advanced:
                                session_rows = self._advanced_session_alerts(
                                    cursor, publication, cohort_id=cohort_id,
                                    cutoff=min(cutoff, resolved_release.cutoff),
                                )
                                from zoneinfo import ZoneInfo

                                # R45: Day History withholds the latest run only for the current session.
                                current_session_date = cutoff.astimezone(ZoneInfo("America/New_York")).date().isoformat()
                                envelope = _with_optional_reads(
                                    publication.detector_runs(
                                        request, session_rows=session_rows,
                                        current_session_date=current_session_date, **identity,
                                    ),
                                    _optional_read(
                                        "session_alerts", served=session_rows is not None, source="SESSION_LIST",
                                        fallback_source="RELEASE_ROWS", reason_code="SESSION_ALERTS_UNAVAILABLE",
                                    ),
                                )
                                envelope = self._advanced_current_marks_reported(cursor, envelope, cutoff=cutoff)
                                envelope = self._advanced_source_facts_reported(cursor, envelope, cutoff=cutoff)
                            else:
                                envelope = publication.detector_runs(request, **identity)
                            return (
                                _retained_options_envelope(
                                    envelope,
                                    release,
                                )
                                if advanced
                                else self._retained_marks(
                                    cursor,
                                    publication,
                                    envelope,
                                    cutoff=cutoff,
                                    cohort_id=cohort_id,
                                )
                            )
                        if contract == "candidate":
                            if candidate_id is None:
                                raise ValueError("Candidate identity is required")
                            result = publication.candidate(
                                candidate_id,
                                **identity,
                            )
                            return (
                                _retained_options_envelope(result, release)
                                if advanced
                                else result
                            )
                        if contract == "preview":
                            if candidate_id is None or preview_request is None:
                                raise ValueError("Preview requires exact candidate identity and request")
                            result = publication.preview(
                                candidate_id,
                                assessed_at=cutoff,
                                request=preview_request,
                                **identity,
                            )
                            return (
                                _retained_options_envelope(result, release)
                                if advanced
                                else result
                            )
                        result = publication.unavailable(
                            contract,
                            requested_contract_ids=requested_contract_ids,
                            **identity,
                        )
                        return (
                            _retained_options_envelope(result, release)
                            if advanced
                            else result
                        )
        if contract == "datasets":
            data = {
                "storage_ready": False, "datasets": [], "dataset_sessions": {},
                "sessions": [], "session_datasets": {}, "current_session_date": None,
                "current_dataset_id": None, "default_dataset_id": None,
                "configured_dataset_id": None, "configured_effective_from": None,
            }
        elif contract == "quotes":
            ids = list(requested_contract_ids)
            data = {
                "quotes": [], "requested_contract_ids": ids,
                "missing_contract_ids": ids,
                "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
                "provider_fetch_performed": False,
                "execution_permission": False,
            }
        else:
            data = {}
        return {
            "available": False, "reason": reason,
            "data_tier": _unavailable_options_data_tier(active),
            "generated_at": cutoff, "as_of": None, "observed_at": None,
            "policy_sha256": None, "model_version": None, "data": data,
        }


class RealtimeTickerScreenerRepository(_RealtimeCompatibilityRepository):
    def ticker_page(
        self,
        ticker: str,
        *,
        available_by: datetime,
        cohort_id: UUID | None = None,
    ) -> dict[str, object] | None:
        symbol = ticker.strip().upper()
        cutoff = _utc(available_by, "available_by")
        if not _TICKER_PATTERN.fullmatch(symbol):
            raise ValueError("invalid ticker page symbol")
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                return None
            active_cohort_id = UUID(str(active["cohort_id"]))
            release = self._release(cursor, active_cohort_id)
            entry = release.entry("TICKER_PAGE", symbol) if release is not None else None
            if entry is None:
                return None
            cursor.execute(
                "SELECT snapshot.snapshot_id, snapshot.payload, snapshot.generated_at, "
                "snapshot.valid_until, snapshot.source_generation, "
                "snapshot.source_manifest_sha256 FROM " + SNAPSHOT_RELATION + " "
                "WHERE snapshot.snapshot_id=%s AND snapshot.cohort_id=%s "
                "AND snapshot.snapshot_type='TICKER_PAGE' AND snapshot.snapshot_key=%s "
                "AND snapshot.generated_at<=%s",
                (str(entry["snapshot_id"]), str(active_cohort_id), symbol, cutoff),
            )
            snapshot = _row(cursor.fetchone())
        if snapshot is None or snapshot["payload"] is None:
            return None
        valid_until = snapshot.get("valid_until")
        serveable = valid_until is None or cutoff < valid_until
        return {
            **snapshot,
            "payload": dict(snapshot["payload"]),
            "provider_tier": active["provider_tier"],
            "is_fresh": serveable,
            "is_serveable": serveable,
            "provider_fetch_performed": False,
        }

    def build_latest(
        self,
        session_date: date | None = None,
        *,
        available_by: datetime,
        allow_degraded: bool = False,
        cohort_id: UUID | None = None,
    ) -> dict[str, object]:
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                return {"available": False, "reason": "ACTIVE_COHORT_UNAVAILABLE", "rows": []}
            cursor.execute(
                "SELECT generation.generation_id, generation.session_date, "
                "generation.status, generation.generated_at, generation.manifest, "
                "generation.expected_members, generation.actual_members "
                "FROM rt.screening_current AS current JOIN rt.screening_generations AS generation "
                "USING (generation_id) WHERE current.cohort_id=%s "
                "AND (%s::date IS NULL OR generation.session_date=%s) "
                "AND generation.generated_at<=%s",
                (str(active["cohort_id"]), session_date, session_date, cutoff),
            )
            generation = _row(cursor.fetchone())
            if generation is None:
                return {"available": False, "reason": "SCREENING_GENERATION_UNAVAILABLE", "rows": []}
            if generation["status"] != "COMPLETE" and not allow_degraded:
                return {
                    "available": False,
                    "reason": "SCREENING_GENERATION_INCOMPLETE",
                    "rows": [],
                    "generation_id": generation["generation_id"],
                }
            cursor.execute(
                "SELECT security_id, ticker, eligible, payload FROM rt.screening_rows "
                "WHERE generation_id=%s ORDER BY ticker",
                (str(generation["generation_id"]),),
            )
            rows = [dict(row["payload"], security_id=row["security_id"],
                         ticker=row["ticker"], eligible=row["eligible"])
                    for row in cursor.fetchall()]
        return {
            "available": True,
            "reason": None,
            "rows": rows,
            "manifest": generation["manifest"],
            "generation_id": generation["generation_id"],
            "session_date": generation["session_date"],
            "generated_at": generation["generated_at"],
            "provider_tier": active["provider_tier"],
            "provider_fetch_performed": False,
        }


class ScreeningPublicationUnavailable(LookupError):
    pass


class ScreeningQueryRejected(ValueError):
    pass


class RealtimeScreeningRepository(_RealtimeCompatibilityRepository):
    """Stored-only screening reader. A stored generation names its bars by lineage head; the first load of a
    generation recomputes every member's heads over the bars `rt.bar_current` now holds and fails closed
    (`ScreeningPublicationUnavailable`, republish required) when any differ. `bar_loader` reads those bars
    (None: only the stored identity and hash checks run, for offline fixtures with no bar tables). A verified
    generation is remembered by id (a bounded cache), so a bar revised after its first load is noticed when
    the entry leaves the cache or the process restarts, as for the Ticker Workspace snapshot cache."""

    def __init__(self, cursor_factory, *, transaction_configured=False, bar_loader=load_bar_heads_cursor):
        super().__init__(cursor_factory, transaction_configured=transaction_configured)
        self._bar_loader = bar_loader
        self._verified = BoundedCache(VERIFIED_GENERATION_CACHE_SIZE)

    def _bars(self, cursor, record, payload, security_ids=None, *, with_sources=False):
        # G102 A4: the members' bars by instrument from the head (G83: the cohort's own bars held 7 of 283).
        targets = screening_references.bar_targets(payload, security_ids)
        return self._bar_loader(cursor, targets=targets, with_sources=with_sources)

    def _verify_bars(self, cursor, record, payload):
        key = str(record["generation_id"])
        if (
            self._bar_loader is None or not screening_references.is_compact(payload)
            or self._verified.get(key) is not None
        ):
            return
        members = list(payload["lineage"])
        try:
            # One statement per chunk of members keeps each lookup far inside the statement timeout however
            # large the cohort is (a member's heads cover about 330 bars).
            for offset in range(0, len(members), VERIFICATION_CHUNK_MEMBERS):
                chunk = set(members[offset:offset + VERIFICATION_CHUNK_MEMBERS])
                screening_references.verify_bar_lineage(payload, self._bars(cursor, record, payload, chunk), chunk)
        except BarLineageChanged as error:
            raise ScreeningPublicationUnavailable("Screening bar lineage changed; republish required") from error
        self._verified.put(key, True)

    def _cohort(self, cursor, cutoff, cohort_id):
        active = _active_cohort(cursor, cutoff, cohort_id)
        if active is None:
            raise ScreeningPublicationUnavailable("Awaiting first daily screening publication")
        return str(active["cohort_id"])

    def _index(self, cursor, cohort, cutoff):
        cursor.execute(
            "SELECT generation_id, session_date, generated_at, manifest "
            "FROM rt.screening_generations WHERE cohort_id=%s AND status='COMPLETE' "
            "AND generated_at<=%s AND manifest->>'version'=%s "
            "ORDER BY session_date DESC, generated_at DESC, generation_id LIMIT 120",
            (cohort, cutoff, VERSION),
        )
        return [dict(row) for row in cursor.fetchall()]

    def catalog(self, *, available_by, cohort_id=None):
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            publications = self._index(cursor, str(active["cohort_id"]), cutoff) if active else []
        latest = publications[0]["manifest"] if publications else {}
        if publications and (latest.get("field_contract") != REALTIME_FIELDS or latest.get("screening_basis") != SCREENING_BASIS):
            raise ValueError("Stored screening catalog contract is incompatible")
        return dict(
            version=VERSION, fields=REALTIME_FIELDS, patterns=PATTERNS, deferred=DEFERRED,
            gaps=REALTIME_GAP_CATALOG if latest.get("gap_contract") == REALTIME_GAP_CATALOG else None,
            hourly=HOURLY_CATALOG if supported_hourly_contract(latest.get("hourly_contract")) else None,
            publications=[dict(generation=p["manifest"]["generation"], session=str(p["session_date"]),
                               observed_at=p["generated_at"].isoformat(),
                               fields=list(p["manifest"]["field_coverage"])) for p in publications],
            status="READY" if publications else "AWAITING_FIRST_PUBLICATION",
        )

    def _load(self, cursor, cohort, cutoff, *, generation=None, session=None, verify_bars=True):
        cursor.execute(
            "SELECT generation_id, cohort_id, provider_tier, session_date, generated_at, manifest, "
            "source_manifest_sha256, expected_members, actual_members "
            "FROM rt.screening_generations WHERE cohort_id=%s AND status='COMPLETE' "
            "AND generated_at<=%s AND manifest->>'version'=%s "
            "AND (%s::text IS NULL OR manifest->>'generation'=%s) "
            "AND (%s::date IS NULL OR session_date=%s) "
            "ORDER BY session_date DESC, generated_at DESC, generation_id LIMIT 1",
            (cohort, cutoff, VERSION, generation, generation, session, session),
        )
        record = cursor.fetchone()
        if record is None:
            raise ScreeningPublicationUnavailable("Requested screening publication is unavailable")
        manifest = dict(record["manifest"])
        conflicts = {
            "manifest_sha256": canonical_sha256(manifest) != record["source_manifest_sha256"],
            "cohort_id": manifest.get("cohort_id") != cohort,
            "provider_tier": manifest.get("provider_tier") != record["provider_tier"],
            "session": manifest.get("session") != str(record["session_date"]),
            "observed_at": (not isinstance(manifest.get("observed_at"), str)
                or datetime.fromisoformat(manifest["observed_at"]) != record["generated_at"]),
            "screening_basis": manifest.get("screening_basis") != SCREENING_BASIS,
            "field_contract": manifest.get("field_contract") != REALTIME_FIELDS,
        }
        mismatches = sorted(name for name, failed in conflicts.items() if failed)
        if mismatches:
            raise ValueError(f"Stored screening generation lineage/contract conflict: {mismatches}")
        cursor.execute(
            "SELECT security_id, ticker, eligible, payload_sha256, payload FROM rt.screening_rows "
            "WHERE generation_id=%s ORDER BY ticker, security_id",
            (str(record["generation_id"]),),
        )
        records = cursor.fetchall()
        rows = []
        for stored in records:
            row = dict(stored["payload"])
            if (row.get("security_id") != str(stored["security_id"]) or row.get("ticker") != stored["ticker"]
                or row.get("eligible") != stored["eligible"] or canonical_sha256(row) != stored["payload_sha256"]):
                raise ValueError("Stored screening row identity/hash conflict")
            rows.append(row)
        if (not 0 < len(rows) <= 1000 or len(rows) != record["expected_members"]
            or len(rows) != record["actual_members"] or len({r["security_id"] for r in rows}) != len(rows)):
            raise ValueError("Stored screening generation membership is incomplete")
        payload = dict(manifest, rows=rows)
        if digest({key: value for key, value in payload.items() if key != "generation"}) != payload["generation"]:
            raise ValueError("Stored screening generation payload hash conflict")
        if verify_bars:
            self._verify_bars(cursor, record, payload)
        return payload

    def query(self, request, *, available_by, cohort_id=None, read_mode="current"):
        if read_mode not in {"current", "retained"}:
            raise ScreeningQueryRejected("Unsupported screening read mode")
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            generation = self._load(
                cursor,
                cohort,
                cutoff,
                generation=request.generation,
                verify_bars=read_mode == "current",
            )
            try:
                previous = self._load(
                    cursor, cohort, cutoff, session=generation["previous_expected_session"], verify_bars=False,
                )
            except ScreeningPublicationUnavailable:
                previous = None
        try:
            result = query_generation(generation, request, previous)
        except ValueError as error:
            raise ScreeningQueryRejected(str(error)) from error
        expected = latest_expected_market_time(cutoff, "1d")
        actual = datetime.fromisoformat(generation["market_time"])
        state = staleness_state(actual, expected, is_fresh=actual == expected)
        result.update(
            stale=state["status"] != "READY",
            status=state["status"],
            expected_market_time=expected.isoformat(),
            staleness_sessions=state["staleness_sessions"],
            max_serveable_stale_sessions=state["max_serveable_stale_sessions"],
            read_mode=read_mode.upper(),
            provider_fetch_performed=False,
        )
        delay = 15 if generation["provider_tier"] == "MASSIVE_DEVELOPER" else 0
        expected_hourly = latest_expected_market_time(cutoff - timedelta(minutes=delay), "1h")
        hourly = generation["hourly_source"]
        result["hourly_stale"] = not hourly.get("market_time") or datetime.fromisoformat(hourly["market_time"]) < expected_hourly
        result["hourly_expected_market_time"] = expected_hourly.isoformat()
        result["hourly_provider_delay_minutes"] = delay
        return result

    def status(self, *, available_by, cohort_id=None, read_mode="retained"):
        if read_mode not in {"current", "retained"}:
            raise ScreeningQueryRejected("Unsupported screening read mode")
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            generation = self._load(
                cursor,
                cohort,
                cutoff,
                verify_bars=read_mode == "current",
            )
        actual = datetime.fromisoformat(generation["market_time"])
        expected = latest_expected_market_time(cutoff, "1d")
        state = staleness_state(actual, expected, is_fresh=actual == expected)
        stale = state["status"] != "READY"
        return {
            "status": state["status"],
            "checked_at": cutoff.isoformat(),
            "max_serveable_stale_sessions": state["max_serveable_stale_sessions"],
            "staleness_sessions": state["staleness_sessions"],
            "last_market_time": generation["market_time"],
            "oldest_stale_market_time": generation["market_time"] if stale else None,
            "stale_intervals": ["1d"] if stale else [],
            "pipeline_active": False,
            "active_intervals": [],
            "source": VERSION,
            "serving_latest_available": True,
            "retained_setup_generated_at": generation["observed_at"],
            "read_mode": read_mode.upper(),
        }

    def _snapshot_row(self, request, available_by, cohort_id, read_mode):
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            generation = self._load(
                cursor,
                cohort,
                cutoff,
                generation=request.generation,
                verify_bars=read_mode == "current",
            )
        row = next((r for r in generation["rows"] if r["security_id"] == request.security_id), None)
        if row is None:
            raise ScreeningPublicationUnavailable("Requested security is not in this publication")
        return generation, row

    def gap_details(self, request, *, available_by, cohort_id=None, read_mode="current"):
        if read_mode not in {"current", "retained"}:
            raise ScreeningQueryRejected("Unsupported screening read mode")
        generation, row = self._snapshot_row(request, available_by, cohort_id, read_mode)
        if generation.get("gap_contract") != REALTIME_GAP_CATALOG:
            raise ValueError("Stored daily gap contract is incompatible")
        context = row["gaps"]
        state, episodes, reason = evaluate_gaps(context, request.gap or GapPredicate())
        return dict(generation=request.generation, security_id=request.security_id, session=generation["session"],
                    source_cutoff=generation["source_cutoff"], status=context["status"], match_state=state,
                    reason=reason, episode_count=len(context["episodes"]), matching_episodes=episodes,
                    contract=generation["gap_contract"])

    def hourly_details(self, request, *, available_by, cohort_id=None, read_mode="current"):
        if read_mode not in {"current", "retained"}:
            raise ScreeningQueryRejected("Unsupported screening read mode")
        generation, row = self._snapshot_row(request, available_by, cohort_id, read_mode)
        if not supported_hourly_contract(generation.get("hourly_contract")):
            raise ValueError("Stored hourly contract is incompatible")
        # G102 A2: a member listed in source_unavailable_members has a placeholder row and no bar lineage; the
        # lineage object stays present (the Screener drawer reads lineage.bars) and lineage_status says why it is empty.
        # STORED: an older generation's embedded lineage; VERIFIED / CHANGED: composed from current bars.
        lineage, lineage_status = generation["hourly_lineage"].get(request.security_id), "STORED"
        if lineage is None:
            lineage = dict(bars=[], selected_bar_id=None, reconstructed_from_30m={}, derived_source_revision_ids={})
            lineage_status = "UNAVAILABLE"
        elif screening_references.is_compact(generation):
            lineage, lineage_status = self._compose_hourly_lineage(
                generation, request.security_id, available_by, cohort_id,
            )
        return dict(generation=request.generation, security_id=request.security_id, source=generation["hourly_source"],
                    values=row["hourly"]["values"], missing=row["hourly"]["missing"], lineage=lineage,
                    lineage_status=lineage_status)

    def _compose_hourly_lineage(self, generation, security_id, available_by, cohort_id):
        """The per-bar hourly lineage, recomposed from the member's current bars and verified against the stored
        heads (the generation keeps the head, not the list), with its status. G102 A2 (G83 class): when a bar
        changed after publication, the lineage over the current bars is returned with status CHANGED; the
        hourly values are the stored ones either way."""
        if self._bar_loader is None:
            raise ScreeningPublicationUnavailable("Hourly lineage is composed from the bar tables, which are not attached")
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            record = dict(cohort_id=cohort, provider_tier=generation["provider_tier"])
            rows = self._bars(cursor, record, generation, {security_id}, with_sources=True)
        try:
            return screening_references.compose_hourly_lineage(generation, security_id, rows), "VERIFIED"
        except BarLineageChanged:
            return screening_references.hourly_lineage_of(generation, security_id, rows), "CHANGED"


class RealtimeStockAlertRepository(_RealtimeCompatibilityRepository):
    def alert_view(
        self,
        *,
        available_by: datetime,
        view: str = "latest",
        session_date: date | None = None,
        offset: int = 0,
        limit: int = 100,
        cohort_id: UUID | None = None,
    ) -> dict[str, object]:
        cutoff = _utc(available_by, "available_by")
        if view not in {"latest", "history", "open"}:
            raise ValueError("unsupported stock alert view")
        if offset < 0 or not 1 <= limit <= 200:
            raise ValueError("invalid stock alert page bounds")
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                return {"available": False, "reason": "ACTIVE_COHORT_UNAVAILABLE", "alerts": []}
            conditions = ["cohort_id=%s", "alert_kind='STOCK'", "as_of<=%s"]
            parameters: list[object] = [str(active["cohort_id"]), cutoff]
            if session_date is not None:
                conditions.extend((
                    "as_of >= %s::date AT TIME ZONE 'America/New_York'",
                    "as_of < (%s::date + 1) AT TIME ZONE 'America/New_York'",
                ))
                parameters.extend((session_date, session_date))
            if view == "open":
                conditions.append("lifecycle='OPEN'")
            cursor.execute(
                "SELECT projection, as_of, valid_until, lifecycle FROM rt.alert_current "
                f"WHERE {' AND '.join(conditions)} ORDER BY as_of DESC, alert_id "
                "OFFSET %s LIMIT %s",
                (*parameters, offset, limit),
            )
            rows = tuple(cursor.fetchall())
            alerts = [
                dict(row["projection"], as_of=row["as_of"],
                     valid_until=row["valid_until"], lifecycle=row["lifecycle"])
                for row in rows
            ]
            release = self._release(cursor, UUID(str(active["cohort_id"])))
            entry = release.entry("STOCK_ALERT_CONTEXT", "PLATFORM") if release is not None else None
            context = None
            if entry is not None:
                cursor.execute(
                    "SELECT snapshot.payload FROM " + SNAPSHOT_RELATION + " "
                    "WHERE snapshot.snapshot_id=%s AND snapshot.cohort_id=%s "
                    "AND snapshot.snapshot_type='STOCK_ALERT_CONTEXT' "
                    "AND snapshot.snapshot_key='PLATFORM' AND snapshot.generated_at<=%s",
                    (str(entry["snapshot_id"]), str(active["cohort_id"]), cutoff),
                )
                context = _row(cursor.fetchone())
            context = context if context is not None and context["payload"] is not None else {"payload": {}}
        payload = dict(context["payload"])
        return {
            "available": True,
            "reason": None,
            "alerts": alerts,
            "schedule_streams": payload.get("schedule_streams", []),
            "sessions": payload.get("sessions", []),
            "observations": payload.get("observations", []),
            "view": view,
            "offset": offset,
            "limit": limit,
            "provider_tier": active["provider_tier"],
            "source": "REALTIME",
            "provider_fetch_performed": False,
        }


class RealtimeOptionAlertRepository(_RealtimeCompatibilityRepository):
    def option_alert_evaluations(
        self,
        *,
        available_by: datetime,
        dataset_id: UUID | None = None,
        detector: str | None = None,
        underlyer: str | None = None,
        offset: int = 0,
        limit: int = 100,
        cohort_id: UUID | None = None,
    ) -> dict[str, object]:
        cutoff = _utc(available_by, "available_by")
        if detector not in {None, "O1", "O2", "O3", "OS1", "OS2"}:
            raise ValueError("unsupported option detector")
        if offset < 0 or not 1 <= limit <= 200:
            raise ValueError("invalid option evaluation page bounds")
        symbol = underlyer.strip().upper() if underlyer else None
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                return self._unavailable("ACTIVE_COHORT_UNAVAILABLE", cutoff)
            dataset_query = (
                "SELECT dataset_id, dataset_key, session_date, status, detector_ids, "
                "started_at, completed_at, summary, source_manifest_sha256, "
                "flow_identity_version, source_flow_id, "
                "advanced_flow_policy_sha256, contract_scope_sha256, "
                "slot_sha256, runtime_policy_sha256 "
                "FROM rt.detector_datasets WHERE cohort_id=%s AND started_at<=%s"
            )
            dataset_parameters: tuple[object, ...] = (
                str(active["cohort_id"]), cutoff,
            )
            if dataset_id is not None:
                dataset_query += " AND dataset_id=%s"
                dataset_parameters += (str(dataset_id),)
            dataset_query += " ORDER BY started_at DESC LIMIT 1"
            cursor.execute(dataset_query, dataset_parameters)
            dataset = _row(cursor.fetchone())
            if dataset is None:
                return self._unavailable("DETECTOR_DATASET_UNAVAILABLE", cutoff)
            evaluation_conditions = ["dataset_id=%s"]
            evaluation_parameters: list[object] = [str(dataset["dataset_id"])]
            if detector is not None:
                evaluation_conditions.append("detector_id=%s")
                evaluation_parameters.append(detector)
            if symbol is not None:
                evaluation_conditions.append("underlyer=%s")
                evaluation_parameters.append(symbol)
            cursor.execute(
                "SELECT evaluation_id, event_id, detector_id, underlyer, selection_status, "
                "observed_at, payload FROM rt.detector_evaluations WHERE "
                f"{' AND '.join(evaluation_conditions)} "
                "ORDER BY observed_at DESC, evaluation_id OFFSET %s LIMIT %s",
                (*evaluation_parameters, offset, limit),
            )
            evaluations = [dict(row["payload"], **{
                key: row[key] for key in (
                    "evaluation_id", "event_id", "detector_id", "underlyer",
                    "selection_status", "observed_at",
                )
            }) for row in cursor.fetchall()]
        generated_at = dataset["completed_at"] or dataset["started_at"]
        return {
            "available": dataset["status"] in {"COMPLETE", "DEGRADED"},
            "reason": None if dataset["status"] in {"COMPLETE", "DEGRADED"}
            else "DETECTOR_DATASET_INCOMPLETE",
            "data_tier": active["provider_tier"],
            "generated_at": generated_at,
            "as_of": generated_at,
            "observed_at": max((row["observed_at"] for row in evaluations), default=None),
            "policy_sha256": dataset["source_manifest_sha256"],
            "model_version": dataset["dataset_key"],
            "data": {
                "dataset": dataset,
                "evaluations": evaluations,
                "management_status_ready": False,
                "provider_fetch_performed": False,
            },
        }

    def option_detector_alerts(self, **kwargs) -> dict[str, object]:
        envelope = self.option_alert_evaluations(**kwargs)
        if envelope["data"] is not None:
            envelope["data"] = {
                "dataset": envelope["data"]["dataset"],
                "alerts": envelope["data"]["evaluations"],
                "management_status_ready": envelope["data"]["management_status_ready"],
                "provider_fetch_performed": False,
            }
        return envelope

    @staticmethod
    def _unavailable(reason: str, generated_at: datetime) -> dict[str, object]:
        return {
            "available": False,
            "reason": reason,
            "data_tier": "UNAVAILABLE",
            "generated_at": generated_at,
            "as_of": None,
            "observed_at": None,
            "policy_sha256": None,
            "model_version": None,
            "data": None,
        }