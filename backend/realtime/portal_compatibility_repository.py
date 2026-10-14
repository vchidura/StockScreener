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

from .contracts import CANARY_UNIVERSE
from .contracts import canonical_sha256
from .equity_bar_repository import load_current_bar_heads_cursor
from .screening_projection import REALTIME_FIELDS, REALTIME_GAP_CATALOG, SCREENING_BASIS
from . import screening_references
from .ticker_references import BarLineageChanged
from equity.calendar import latest_expected_market_time
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


@dataclass(frozen=True, slots=True)
class ReleaseView:
    release_id: UUID
    cohort_id: UUID
    provider_tier: str
    release_class: str
    session_date: date
    cutoff: datetime
    entries: Mapping[tuple[str, str], Mapping[str, object]]

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
    return _row(cursor.fetchone())


class _RealtimeCompatibilityRepository:
    def __init__(
        self,
        cursor_factory: Callable,
        *,
        transaction_configured: bool = False,
    ):
        self._cursor_factory = cursor_factory
        self._transaction_configured = transaction_configured
        self._release_cache = BoundedCache(RELEASE_CACHE_SIZE)

    def _cursor(self):
        return self._cursor_factory()

    def _begin(self, cursor) -> None:
        if not self._transaction_configured:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")

    def _release(self, cursor, cohort_id) -> ReleaseView | None:
        return load_current_release(cursor, cohort_id, self._release_cache)

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
    ) -> dict[str, object]:
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Options Flow requires an explicit realtime cohort")
        symbol = underlyer.strip().upper() if underlyer else "SPY"
        if not _TICKER_PATTERN.fullmatch(symbol):
            raise ValueError("Invalid Options Flow underlying")
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                reason = "ACTIVE_COHORT_UNAVAILABLE"
            else:
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
                    from .forward_reader import load_forward_release_cursor
                    retained = load_forward_release_cursor(cursor, cohort_id=cohort_id, available_by=cutoff, artifact_kind="FLOW")
                    if retained is not None:
                        publication, release = retained
                        publication = publication.get(symbol) if isinstance(publication, dict) else publication
                        if publication is None:
                            from .forward_reader import ForwardPublicationUnavailable
                            raise ForwardPublicationUnavailable("Requested Flow underlying is outside the approved release")
                        result = publication.read(cohort_id=cohort_id, provider_tier=publication.provider_tier,
                            scope_sha256=release["scope_sha256"])
                        if symbol != result["data"]["selected"]:
                            from .forward_reader import ForwardPublicationUnavailable
                            raise ForwardPublicationUnavailable("Requested underlying Flow profile is not retained in this release")
                        return result
        return {
            "available": False,
            "reason": reason,
            "data_tier": "15-MINUTE DELAYED RESEARCH DATA",
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
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                reason = "ACTIVE_COHORT_UNAVAILABLE"
            else:
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
                    from .forward_reader import load_forward_release_cursor
                    retained = load_forward_release_cursor(cursor, cohort_id=cohort_id, available_by=cutoff, artifact_kind="SCREENER")
                    if retained is not None:
                        publication, release = retained
                        return publication.query(request, cohort_id=cohort_id, provider_tier=publication.provider_tier,
                            scope_sha256=release["scope_sha256"])
        requested = [request.underlyer] if request.underlyer else []
        return {
            "available": False,
            "reason": reason,
            "data_tier": "15-MINUTE DELAYED RESEARCH DATA",
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
        with self._cursor() as cursor:
            self._begin(cursor)
            active = _active_cohort(cursor, cutoff, cohort_id)
            if active is None:
                reason = "ACTIVE_COHORT_UNAVAILABLE"
            else:
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
                    from .forward_reader import load_forward_release_cursor
                    retained = load_forward_release_cursor(cursor, cohort_id=cohort_id, available_by=cutoff, artifact_kind="ALERTS")
                    if retained is not None:
                        publication, release = retained
                        identity = dict(cohort_id=cohort_id, provider_tier=publication.provider_tier, scope_sha256=release["scope_sha256"])
                        if contract == "datasets":
                            return publication.datasets(**identity)
                        if contract == "schedule":
                            return publication.schedule(checked_at=cutoff, **identity)
                        if contract == "evaluations":
                            from .option_alert_projection import DetectorEvaluationQuery
                            return publication.evaluations(DetectorEvaluationQuery(**(query or {})), **identity)
                        if contract == "detector_runs":
                            from .option_alert_projection import DetectorRunQuery
                            envelope = publication.detector_runs(DetectorRunQuery(**(query or {})), **identity)
                            return self._retained_marks(cursor, publication, envelope, cutoff=cutoff, cohort_id=cohort_id)
                        if contract == "candidate":
                            if candidate_id is None:
                                raise ValueError("Candidate identity is required")
                            return publication.candidate(candidate_id, **identity)
                        if contract == "preview":
                            if candidate_id is None or preview_request is None:
                                raise ValueError("Preview requires exact candidate identity and request")
                            return publication.preview(candidate_id, assessed_at=cutoff, request=preview_request, **identity)
                        return publication.unavailable(contract, requested_contract_ids=requested_contract_ids, **identity)
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
            "data_tier": "15-MINUTE DELAYED RESEARCH DATA",
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

    def __init__(self, cursor_factory, *, transaction_configured=False, bar_loader=load_current_bar_heads_cursor):
        super().__init__(cursor_factory, transaction_configured=transaction_configured)
        self._bar_loader = bar_loader
        self._verified = BoundedCache(VERIFIED_GENERATION_CACHE_SIZE)

    def _bars(self, cursor, record, payload, security_ids=None, *, with_sources=False):
        targets = screening_references.bar_targets(payload, security_ids)
        return self._bar_loader(
            cursor, cohort_id=UUID(str(record["cohort_id"])), provider_tier=record["provider_tier"],
            targets=targets, with_sources=with_sources,
        )

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

    def query(self, request, *, available_by, cohort_id=None):
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            generation = self._load(cursor, cohort, cutoff, generation=request.generation)
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
        result["stale"] = datetime.fromisoformat(generation["market_time"]) < latest_expected_market_time(cutoff, "1d")
        delay = 15 if generation["provider_tier"] == "MASSIVE_DEVELOPER" else 0
        expected_hourly = latest_expected_market_time(cutoff - timedelta(minutes=delay), "1h")
        hourly = generation["hourly_source"]
        result["hourly_stale"] = not hourly.get("market_time") or datetime.fromisoformat(hourly["market_time"]) < expected_hourly
        result["hourly_expected_market_time"] = expected_hourly.isoformat()
        result["hourly_provider_delay_minutes"] = delay
        return result

    def _snapshot_row(self, request, available_by, cohort_id):
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            generation = self._load(cursor, cohort, cutoff, generation=request.generation)
        row = next((r for r in generation["rows"] if r["security_id"] == request.security_id), None)
        if row is None:
            raise ScreeningPublicationUnavailable("Requested security is not in this publication")
        return generation, row

    def gap_details(self, request, *, available_by, cohort_id=None):
        generation, row = self._snapshot_row(request, available_by, cohort_id)
        if generation.get("gap_contract") != REALTIME_GAP_CATALOG:
            raise ValueError("Stored daily gap contract is incompatible")
        context = row["gaps"]
        state, episodes, reason = evaluate_gaps(context, request.gap or GapPredicate())
        return dict(generation=request.generation, security_id=request.security_id, session=generation["session"],
                    source_cutoff=generation["source_cutoff"], status=context["status"], match_state=state,
                    reason=reason, episode_count=len(context["episodes"]), matching_episodes=episodes,
                    contract=generation["gap_contract"])

    def hourly_details(self, request, *, available_by, cohort_id=None):
        generation, row = self._snapshot_row(request, available_by, cohort_id)
        if not supported_hourly_contract(generation.get("hourly_contract")):
            raise ValueError("Stored hourly contract is incompatible")
        lineage = generation["hourly_lineage"][request.security_id]
        if screening_references.is_compact(generation):
            lineage = self._compose_hourly_lineage(generation, request.security_id, available_by, cohort_id)
        return dict(generation=request.generation, security_id=request.security_id, source=generation["hourly_source"],
                    values=row["hourly"]["values"], missing=row["hourly"]["missing"], lineage=lineage)

    def _compose_hourly_lineage(self, generation, security_id, available_by, cohort_id):
        """The per-bar hourly lineage, recomposed from the member's current bars and verified against the
        stored heads (the generation keeps the head, not the list)."""
        if self._bar_loader is None:
            raise ScreeningPublicationUnavailable("Hourly lineage is composed from the bar tables, which are not attached")
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            cohort = self._cohort(cursor, cutoff, cohort_id)
            record = dict(cohort_id=cohort, provider_tier=generation["provider_tier"])
            try:
                return screening_references.compose_hourly_lineage(
                    generation, security_id,
                    self._bars(cursor, record, generation, {security_id}, with_sources=True),
                )
            except BarLineageChanged as error:
                raise ScreeningPublicationUnavailable("Screening bar lineage changed; republish required") from error


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
        if detector not in {None, "O1", "O2", "O3", "S1", "S2"}:
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
                "started_at, completed_at, summary, source_manifest_sha256 "
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