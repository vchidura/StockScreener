"""R78 (Nov 14): the daily products job - the daily strategies and the Dashboard built outside the Equity window.

Switch ``REALTIME_V2_DAILY_PRODUCTS`` (read by the Equity scheduler, the window and the job):

- unset or ``OFF``: today's behaviour. The window builds the Dashboard (first window, cold windows, the close); no job.
- ``STRATEGIES``: the job runs and publishes the daily strategies only; the window still builds the Dashboard. A safe
  first step: it changes no window and fills ``/api/strategies/daily`` (Strategy Overview, Stock Persistence and the
  six strategy pages' preferred read).
- ``JOB``: the job publishes the strategies and the Dashboard; the window never builds the Dashboard (one loader slot,
  no ~90 s cold PORTAL_PUBLISH).

What the job does for (cohort, completed session):

1. Loads a narrow input (`load_daily_products_input`: adjusted daily, raw daily, 20 sessions of raw 1h; derived bars
   accepted by their stored lineage) in one read-only REPEATABLE READ transaction.
2. Idempotency: the input's identity (cohort, session, members, actions, coverage, per-ticker bar lineage heads and
   the product set; not the clock) is stamped on every product. When the cohort's current release already holds
   products with that identity, the job ends ``ALREADY_STORED`` without computing anything.
3. Computes the products: ``DAILY_STRATEGY_HISTORY_V1`` (the session and the four before it, re-evaluated from the
   retained bars and labelled RECONSTRUCTED: cohorts are per session, so a rolling history of stored evaluations
   never holds more than one session) and, in JOB mode, ``DASHBOARD_V1`` (same document as the window's).
4. Writes the strategy evidence (append-only COMPUTE rows), then publishes in ONE publication transaction: the
   strategy snapshot (KEPT), the Dashboard (HOT, as a window's) and an INTRADAY release attached to the cohort's
   current release (its cutoff, receipt and desired-state identity), so the products become current at once and
   every later window release carries them forward (a window never republishes them in this mode).
   - At the session close (the products of the cohort's own session, on its closing release) the Dashboard is also
     KEPT and a SESSION_CLOSE release names the kept Dashboard and the strategy snapshot, as the window's close and
     the strategy producer did (review P2-3, Nov 15).

Publication generation (review P1, Nov 15): the publisher orders a cohort's releases by (session, cutoff,
generation), and the job attaches to the current release's cutoff, so the job's generation must not be below the
current release's. A window's generation is the number of READY reference seeds (`reference_generation`), not the
cohort's own counter, so the job's generation is above all of: the cohort's stored snapshots and features, the
current release, and the READY seeds. Prepared products outranked by the current release (a reference refresh
after the preparation) are not released; the run computes again instead.

Prepared before the open (``prepare=True``): a new session's cohort exists from activation (~15 minutes before the
open) but has no release until its first window publishes, and a release must name a window receipt
(rt.portal_releases.receipt_id references rt.cycle_receipts). So the job computes everything at activation, writes the
evidence and stores the snapshots (no release: nothing is visible), and after the first window a publish-only run
finds the stored products by their input identity and names them in a release (no computing). The cutoff of a run
before the open is the cohort's effective time (the open): the input loader reads a cohort only once it is effective.

A late job never replaces a newer publication: under the release lock the job refuses (``SUPERSEDED``) when the
current release already holds a product of a later session, or of the same session generated later. A failure
writes nothing to the release: the previous publications stay served, and the caller raises a fault.
"""
from __future__ import annotations

import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from uuid import UUID

import exchange_calendars

from .corporate_action_drift import corporate_action_content, corporate_action_coverage_content
from .contracts import canonical_sha256
from .dashboard_projection import DASHBOARD_KEY, DASHBOARD_VERSION, produce_realtime_dashboard
from .dashboard_repository import persist_realtime_dashboard_cursor
from .database_access import RealtimeDatabaseRole
from .lineage_head import inputs_bar_lineage_sha256
from .portal_compatibility_repository import SNAPSHOT_RELATION
from .portal_projection import build_portal_snapshot
from .portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE, PortalReleaseReceipt, publish_portal_release_cursor,
)
from .preflight import ProviderTier
from .screening_repository import load_daily_products_input
from .setup_summary_projection import setup_executor
from .strategy_projection import (
    HISTORY_VERSION, STRATEGY_KEY, STRATEGY_VERSION, DailyStrategyProjection, project_realtime_daily_strategies,
    project_realtime_strategy_history,
)
from .strategy_repository import persist_strategy_snapshot_cursor, write_strategy_evidence
from .worker_database import RealtimeProcessDatabase

DAILY_PRODUCTS_SWITCH = "REALTIME_V2_DAILY_PRODUCTS"
MODE_OFF = "OFF"
MODE_STRATEGIES = "STRATEGIES"
MODE_JOB = "JOB"
MODES = (MODE_OFF, MODE_STRATEGIES, MODE_JOB)
DAILY_PRODUCTS_VERSION = "realtime-daily-products-v1"
# The manifest key every job product carries (identity of the input it was built from, the job's mode and version).
DAILY_PRODUCTS_MARKER = "daily_products"
# R78 (Nov 14 live check): which strategy release the job publishes. 5 (default): DAILY_STRATEGY_HISTORY_V1, the session
# and the four before it (Stock Persistence's grid; ~80% of the job and a ~32 MB release read in ~3 s). 1:
# DAILY_STRATEGIES_V1, the latest session only (~4.6 MB, a fifth of the strategy time); Stock Persistence then shows the
# latest session, until its redesign (R79).
STRATEGY_SESSIONS_SWITCH = "REALTIME_V2_DAILY_STRATEGY_SESSIONS"


def strategy_sessions(environ=None) -> int:
    value = (os.environ if environ is None else environ).get(STRATEGY_SESSIONS_SWITCH, "").strip()
    return 1 if value == "1" else 5


def products_for(mode: str, sessions: int = 5) -> tuple:
    strategy = (HISTORY_VERSION if sessions == 5 else STRATEGY_VERSION, STRATEGY_KEY)
    return (strategy,) if mode == MODE_STRATEGIES else (strategy, (DASHBOARD_VERSION, DASHBOARD_KEY))


PRODUCTS = {mode: products_for(mode) for mode in (MODE_STRATEGIES, MODE_JOB)}  # the default (5 sessions)

ALREADY_STORED = "ALREADY_STORED"
PUBLISHED = "PUBLISHED"
PREPARED = "PREPARED"  # computed and stored before the open, not yet named in a release
ALREADY_PREPARED = "ALREADY_PREPARED"
SUPERSEDED = "SUPERSEDED"
NO_RELEASE = "NO_RELEASE"
COMPUTED = "COMPUTED"  # dry run: computed, nothing written
# The products' generation is below the current release's (the publisher would record them HISTORICAL_STALE and keep
# the pointer): nothing is written; the run computes again above it.
OUTRANKED = "OUTRANKED"


def daily_products_mode(environ=None) -> str:
    """The switch value; anything else than STRATEGIES or JOB is OFF (today's behaviour)."""
    value = (os.environ if environ is None else environ).get(DAILY_PRODUCTS_SWITCH, "").strip().upper()
    return value if value in (MODE_STRATEGIES, MODE_JOB) else MODE_OFF


def window_builds_dashboard(environ=None) -> bool:
    """False only in JOB mode: the window then publishes no Dashboard (the job does)."""
    return daily_products_mode(environ) != MODE_JOB


class DailyProductsUnavailable(RuntimeError):
    """The job cannot run yet (no current release for the cohort, or the session is not complete)."""


@dataclass(frozen=True)
class DailyProductsOutcome:
    status: str
    cohort_id: str
    session: str
    mode: str
    input_sha256: str | None = None
    snapshots: dict = field(default_factory=dict)
    release_id: str | None = None
    release_cutoff: str | None = None
    timings: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    reason: str | None = None

    def report(self) -> dict:
        return dict(status=self.status, cohort_id=self.cohort_id, session=self.session, mode=self.mode,
                    input_sha256=self.input_sha256, snapshots=self.snapshots, release_id=self.release_id,
                    release_cutoff=self.release_cutoff, timings=self.timings, counts=self.counts, reason=self.reason)


def daily_products_input_sha256(inputs, mode: str, sessions: int = 5) -> str:
    """What the products depend on, without the clock: two runs over the same members, actions, coverage and bars
    produce the same products (up to their generated_at), so the second one is ALREADY_STORED.

    Nov 16: actions and coverage enter by content (`corporate_action_content`, `corporate_action_coverage_content`),
    not by row ids and arrival times. The nightly coverage refresh re-inserts identical revisions and re-checks coverage
    with an unchanged provider response; by row identity every refresh was a new input, so each scheduler restart
    recomputed the previous session's products (Nov 16: source generation 20 -> 21 -> 22 with nothing changed). The
    identity format changes once with this: the first run after the deploy computes again, then refreshes keep it."""
    return canonical_sha256(dict(
        version=DAILY_PRODUCTS_VERSION, mode=mode, products=[list(item) for item in products_for(mode, sessions)],
        cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier, session=inputs.session,
        source_manifest_sha256=inputs.source_manifest_sha256, members=inputs.members,
        actions=corporate_action_content(inputs.actions), coverage=corporate_action_coverage_content(inputs.coverage),
        bar_lineage=inputs_bar_lineage_sha256(inputs),
        source_lineage=getattr(inputs, "source_lineage", "LOADED"),
    ))


def next_source_generation_cursor(cursor, cohort_id) -> int:
    """Above every generation the job's release competes with: the cohort's stored snapshots and features (the
    strategy producer's rule, `materialize_due_realtime_strategies`), its current release, and the READY reference
    seeds. The last one is what a window's snapshots carry (`stage_runners_equity.reference_generation`), so a product
    prepared before the cohort's first window still ranks above that window's release (review P1, Nov 15: the cohort's
    own counter alone gave 1 before the first window, the window 13)."""
    cursor.execute(
        "SELECT GREATEST((SELECT COALESCE(max(source_generation),0) FROM rt.snapshot_catalog WHERE cohort_id=%s),"
        "(SELECT COALESCE(max(generation),0) FROM rt.feature_snapshots WHERE cohort_id=%s),"
        "(SELECT COALESCE(max(publication_generation),0) FROM rt.portal_release_current WHERE cohort_id=%s),"
        "(SELECT count(*) FROM rt.seed_manifests WHERE status='READY'))+1 AS generation",
        (str(cohort_id), str(cohort_id), str(cohort_id)),
    )
    return int(cursor.fetchone()["generation"])


def _generation(snapshots) -> int:
    """The release generation the publisher derives from these snapshots (`_publication_identity`)."""
    return max(snapshot.source_generation for snapshot in snapshots)


def _current_generation(current) -> int:
    return 0 if current is None else int(current["publication_generation"] or 0)


def _current_release_cursor(cursor, cohort_id):
    cursor.execute(
        "SELECT pointer.release_id, pointer.cutoff, pointer.session_date, pointer.provider_tier, "
        "COALESCE(pointer.publication_generation, release.publication_generation, 0) AS publication_generation, "
        "release.receipt_id, release.universe_sha256, release.desired_revision_id, "
        "release.desired_snapshot_sha256, release.membership_sha256, release.manifest "
        "FROM rt.portal_release_current AS pointer JOIN rt.portal_releases AS release "
        "ON release.release_id=pointer.release_id AND release.cutoff=pointer.cutoff "
        "WHERE pointer.cohort_id=%s ORDER BY pointer.cutoff DESC LIMIT 1",
        (str(cohort_id),),
    )
    return cursor.fetchone()


def stored_daily_products_cursor(cursor, cohort_id, mode: str, sessions: int = 5) -> dict:
    """The job products named by the cohort's current release: {(type, key): (entry, marker)}; marker None for an
    entry that is not a job product (the window's Dashboard)."""
    current = _current_release_cursor(cursor, cohort_id)
    if current is None:
        return {}
    wanted = set(products_for(mode, sessions))
    entries = {(entry["snapshot_type"], entry["snapshot_key"]): entry for entry in current["manifest"]["snapshots"]
               if (entry["snapshot_type"], entry["snapshot_key"]) in wanted}
    if not entries:
        return {}
    cursor.execute(
        "SELECT snapshot.snapshot_id, snapshot.source_manifest->%s AS marker, snapshot.payload->>'session' AS session "
        "FROM " + SNAPSHOT_RELATION + " WHERE snapshot.snapshot_id=ANY(%s::uuid[])",
        (DAILY_PRODUCTS_MARKER, [entry["snapshot_id"] for entry in entries.values()]),
    )
    stored = {str(row["snapshot_id"]): (row["marker"], row["session"]) for row in cursor.fetchall()}
    return {identity: (entry, *stored.get(str(entry["snapshot_id"]), (None, None)))
            for identity, entry in entries.items()}


def prepared_daily_products_cursor(cursor, cohort_id, mode: str, input_sha256: str, sessions: int = 5) -> dict:
    """Products already stored for this input identity (a run before the open), rebuilt from their stored rows and
    proven equal to them (same snapshot id and hashes). Empty unless every product of the mode is there."""
    wanted = set(products_for(mode, sessions))
    cursor.execute(
        "SELECT snapshot.snapshot_id, snapshot.cohort_id, snapshot.provider_tier, snapshot.snapshot_type, "
        "snapshot.snapshot_key, snapshot.source_generation, snapshot.source_manifest_sha256, snapshot.payload_sha256, "
        "snapshot.generated_at, snapshot.valid_until, snapshot.source_manifest, snapshot.payload FROM "
        + SNAPSHOT_RELATION + " WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=ANY(%s) "
        "AND snapshot.source_manifest->%s->>'input_sha256'=%s AND snapshot.payload IS NOT NULL "
        "ORDER BY snapshot.generated_at DESC, snapshot.source_generation DESC, snapshot.snapshot_id",
        (str(cohort_id), sorted({kind for kind, _key in wanted}), DAILY_PRODUCTS_MARKER, input_sha256),
    )
    found = {}
    for row in cursor.fetchall():
        identity = (row["snapshot_type"], row["snapshot_key"])
        if identity not in wanted or identity in found:
            continue
        snapshot = build_portal_snapshot(
            cohort_id=UUID(str(row["cohort_id"])), provider_tier=ProviderTier(row["provider_tier"]),
            snapshot_type=row["snapshot_type"], snapshot_key=row["snapshot_key"],
            source_generation=int(row["source_generation"]), source_manifest=row["source_manifest"],
            payload=row["payload"], generated_at=row["generated_at"], valid_until=row["valid_until"],
        )
        if (str(snapshot.snapshot_id) != str(row["snapshot_id"])
                or snapshot.source_manifest_sha256 != row["source_manifest_sha256"]
                or snapshot.payload_sha256 != row["payload_sha256"]):
            raise ValueError("Prepared daily product identity/hash drift")
        found[identity] = snapshot
    return found if len(found) == len(wanted) else {}


def _with_marker(snapshot, marker):
    manifest = dict(snapshot.source_manifest)
    manifest[DAILY_PRODUCTS_MARKER] = marker
    return build_portal_snapshot(
        cohort_id=snapshot.cohort_id, provider_tier=snapshot.provider_tier, snapshot_type=snapshot.snapshot_type,
        snapshot_key=snapshot.snapshot_key, source_generation=snapshot.source_generation, source_manifest=manifest,
        payload=snapshot.payload, generated_at=snapshot.generated_at, valid_until=snapshot.valid_until,
    )


@dataclass
class DailyProducts:
    inputs: object
    input_sha256: str
    mode: str
    strategy: object = None  # DailyStrategyProjection (snapshot carries the marker)
    dashboard: object = None
    timings: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    prepared: bool = False  # found stored (evidence already written); only the release is left
    sessions: int = 5  # the strategy release: 5 = history, 1 = latest session


def _check_database(database, role):
    if not isinstance(database, RealtimeProcessDatabase) or database.process_plan.role is not role:
        raise ValueError(f"Daily products require the target-local {role.name} role")


@contextmanager
def _setup_workers(workers: int):
    """`workers` > 1: the per-member setup evidence of every evaluation runs on that many spawned processes (spawn, not
    fork: no copy of this process). 1: in this process, as the window does."""
    if workers <= 1:
        yield None
        return
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        with setup_executor(pool):
            yield pool


def compute_daily_products(compute_database, *, cohort_id: UUID, session: date, cutoff: datetime, mode: str,
                           source_closure: bool = False, force: bool = False, workers: int = 1,
                           sessions: int | None = None, clock=time.perf_counter):
    """Returns (DailyProducts, None) or (None, ALREADY_STORED outcome)."""
    _check_database(compute_database, RealtimeDatabaseRole.COMPUTE)
    if mode not in PRODUCTS:
        raise ValueError("Daily products run only in STRATEGIES or JOB mode")
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("Daily products cutoff must be timezone-aware")
    cutoff = cutoff.astimezone(timezone.utc)
    sessions = strategy_sessions() if sessions is None else sessions
    products_wanted = products_for(mode, sessions)
    timings, counts = {}, {"strategy_sessions": sessions}
    started = clock()
    with compute_database.transaction(read_only=True, dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
        generation = next_source_generation_cursor(cursor, cohort_id)
        inputs = load_daily_products_input(cursor, cohort_id=cohort_id, session=session, cutoff=cutoff,
                                           source_generation=generation, source_closure=source_closure)
        timings["load_seconds"] = round(clock() - started, 2)
        counts.update(members=len(inputs.members), bars=len(inputs.bars), source_generation=generation)
        input_sha256 = daily_products_input_sha256(inputs, mode, sessions)
        stored = stored_daily_products_cursor(cursor, cohort_id, mode, sessions=sessions)
        prepared = {} if force else prepared_daily_products_cursor(cursor, cohort_id, mode, input_sha256,
                                                                   sessions=sessions)
        current_generation = _current_generation(_current_release_cursor(cursor, cohort_id)) if prepared else 0
    if prepared and _generation(prepared.values()) < current_generation:
        # Prepared before a window that published at a higher generation (review P1): releasing them would be
        # HISTORICAL_STALE. Compute again (the generation above is above the current release); they stay unreleased.
        counts["prepared_outranked"] = dict(prepared=_generation(prepared.values()), current=current_generation)
        prepared = {}
    if not force and len(stored) == len(products_wanted) and all(
            marker is not None and marker.get("input_sha256") == input_sha256 for _entry, marker, _session in stored.values()):
        return None, DailyProductsOutcome(
            ALREADY_STORED, str(cohort_id), str(session), mode, input_sha256=input_sha256,
            snapshots={f"{kind}:{key}": entry["snapshot_id"] for (kind, key), (entry, _m, _s) in stored.items()},
            timings=timings, counts=counts)
    if prepared:
        counts["prepared_found"] = True
        timings["compute_seconds"] = round(clock() - started, 2)
        return DailyProducts(inputs, input_sha256, mode, timings=timings, counts=counts, prepared=True,
                             sessions=sessions, strategy=DailyStrategyProjection(prepared[products_wanted[0]], ()),
                             dashboard=prepared.get((DASHBOARD_VERSION, DASHBOARD_KEY))), None
    marker = dict(version=DAILY_PRODUCTS_VERSION, input_sha256=input_sha256, mode=mode, strategy_sessions=sessions,
                  source_lineage=getattr(inputs, "source_lineage", "LOADED"))
    products = DailyProducts(inputs, input_sha256, mode, timings=timings, counts=counts, sessions=sessions)
    counts["workers"] = max(1, workers)
    with _setup_workers(workers):
        step = clock()
        strategy = (project_realtime_strategy_history(inputs) if sessions == 5
                    else project_realtime_daily_strategies(inputs))
        products.strategy = replace(strategy, snapshot=_with_marker(strategy.snapshot, marker))
        timings["strategies_seconds"] = round(clock() - step, 2)
        counts["strategy_materializations"] = len(strategy.materializations)
        if mode == MODE_JOB:
            step = clock()
            latest = (strategy.snapshot.source_manifest["history"][-1] if sessions == 5
                      else dict(payload=strategy.snapshot.payload, source_manifest=strategy.snapshot.source_manifest))
            setups = (latest["payload"]["setup_summary"]["results"], latest["source_manifest"]["setup_evidence"],
                      latest["source_manifest"]["missing_setup_confirmations"])
            products.dashboard = _with_marker(produce_realtime_dashboard(inputs, setups=setups), marker)
            timings["dashboard_seconds"] = round(clock() - step, 2)
    timings["compute_seconds"] = round(clock() - started, 2)
    return products, None


def _snapshots(products: DailyProducts):
    return tuple(snapshot for snapshot in (products.strategy.snapshot, products.dashboard) if snapshot is not None)


def _session_of(snapshot) -> str:
    return str(snapshot.payload["session"])


def publish_daily_products_cursor(cursor, products: DailyProducts):
    """One publication transaction: refuse when superseded, store the products, release them on the current pointer.
    Returns (status, release, reason)."""
    snapshots = _snapshots(products)
    cohort_id = products.inputs.cohort_id
    tier = products.inputs.provider_tier
    cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                   (f"portal-release:{cohort_id}:{tier.value}",))
    current = _current_release_cursor(cursor, cohort_id)
    if current is None:
        return NO_RELEASE, None, "COHORT_HAS_NO_CURRENT_RELEASE"
    if current["provider_tier"] != tier.value:
        raise ValueError("Daily products tier differs from the cohort's current release")
    stored = stored_daily_products_cursor(cursor, cohort_id, products.mode, sessions=products.sessions)
    for snapshot in snapshots:
        entry, _marker, session = stored.get((snapshot.snapshot_type, snapshot.snapshot_key), (None, None, None))
        if entry is None:
            continue
        ours = (_session_of(snapshot), snapshot.generated_at)
        theirs = (session or "", datetime.fromisoformat(entry["generated_at"]))
        if theirs > ours:
            return SUPERSEDED, None, f"NEWER_{snapshot.snapshot_type}_IS_CURRENT"
    ours, theirs = _generation(snapshots), _current_generation(current)
    if ours < theirs:
        # At the current cutoff the publisher orders by generation: this release would not become current.
        return OUTRANKED, None, f"GENERATION_{ours}_BELOW_CURRENT_{theirs}"
    session_close = closes_session(current, products.inputs.session)
    persist_strategy_snapshot_cursor(cursor, products.strategy.snapshot)
    kept = ()
    if products.dashboard is not None:
        _hot, kept = persist_realtime_dashboard_cursor(cursor, products.dashboard, session_close=session_close)
    receipt = PortalReleaseReceipt(
        UUID(str(current["receipt_id"])), UUID(str(cohort_id)), ProviderTier(current["provider_tier"]),
        current["cutoff"], current["universe_sha256"],
        desired_revision_id=(UUID(str(current["desired_revision_id"]))
                             if current["desired_revision_id"] is not None else None),
        desired_snapshot_sha256=current["desired_snapshot_sha256"],
        membership_sha256=current["membership_sha256"],
    )
    release = publish_portal_release_cursor(cursor, receipt, snapshots, session_date=current["session_date"])
    if not release.pointer_swapped:
        raise ValueError(f"Daily products release did not become current ({release.publication_disposition})")
    if session_close:
        # The frozen copy of the closed session's products (review P2-3): the window's close kept its Dashboard and
        # the strategy producer published its close snapshot in a SESSION_CLOSE release; HOT rows expire.
        publish_portal_release_cursor(cursor, receipt, (products.strategy.snapshot, *kept),
                                      release_class=RELEASE_CLASS_SESSION_CLOSE, session_date=products.inputs.session)
    return PUBLISHED, release, None


def closes_session(current, session) -> bool:
    """The products describe the cohort's own session and attach to its closing release (AFTER_CLOSE). A new cohort's
    products (SESSION_COHORT, PRE_OPEN) describe the session before the cohort's: never a close."""
    if str(current["session_date"]) != str(session):
        return False
    close = exchange_calendars.get_calendar("XNYS").session_close(str(session)).to_pydatetime()
    return current["cutoff"] >= close


def store_daily_products_cursor(cursor, products: DailyProducts) -> None:
    """Stores the products without naming them in a release (a run before the open): invisible to every reader until
    the publish-only run releases them."""
    persist_strategy_snapshot_cursor(cursor, products.strategy.snapshot)
    if products.dashboard is not None:
        persist_realtime_dashboard_cursor(cursor, products.dashboard, session_close=False)


def default_cutoff_cursor(cursor, cohort_id, now: datetime) -> datetime:
    """Now, or the cohort's effective time when that is later (a run before the open: the cohort exists from activation
    but is effective from the open, and the input loader reads it only as of an effective cutoff)."""
    cursor.execute("SELECT effective_at, created_at FROM rt.cohorts WHERE cohort_id=%s", (str(cohort_id),))
    row = cursor.fetchone()
    if row is None:
        raise ValueError("Daily products cohort is unavailable")
    effective, created = (row["effective_at"], row["created_at"]) if isinstance(row, dict) else (row[0], row[1])
    # The loader also requires the cohort to have been created by the cutoff (a late activation: Nov 13 14:35).
    return max(now, effective.astimezone(timezone.utc), created.astimezone(timezone.utc))


def run_daily_products(compute_database, publication_database, *, cohort_id: UUID, session: date,
                       cutoff: datetime | None = None, mode: str, dry_run: bool = False,
                       source_closure: bool = False, force: bool = False, workers: int = 1,
                       prepare: bool = False, clock=time.perf_counter):
    """The whole job for one (cohort, completed session). Raises on failure (nothing released). `prepare`: compute and
    store, no release (PREPARED); otherwise publish (stored prepared products are released without computing)."""
    _check_database(publication_database, RealtimeDatabaseRole.PUBLICATION)
    if cutoff is None:
        with compute_database.transaction(read_only=True, dict_cursor=True) as cursor:
            cutoff = default_cutoff_cursor(cursor, cohort_id, datetime.now(timezone.utc).replace(microsecond=0))
    products, stored = compute_daily_products(compute_database, cohort_id=cohort_id, session=session, cutoff=cutoff,
                                              mode=mode, source_closure=source_closure, force=force,
                                              workers=workers, clock=clock)
    if stored is not None:
        return stored
    snapshots = {f"{s.snapshot_type}:{s.snapshot_key}": str(s.snapshot_id) for s in _snapshots(products)}
    if prepare and products.prepared:
        return DailyProductsOutcome(ALREADY_PREPARED, str(cohort_id), str(session), mode,
                                    input_sha256=products.input_sha256, snapshots=snapshots,
                                    timings=products.timings, counts=products.counts)
    if dry_run:
        return DailyProductsOutcome(COMPUTED, str(cohort_id), str(session), mode, input_sha256=products.input_sha256,
                                    snapshots=snapshots, timings=products.timings, counts=products.counts)
    if not products.prepared:
        step = clock()
        products.counts["strategy_evidence_writes"] = write_strategy_evidence(compute_database, products.strategy)
        products.timings["evidence_seconds"] = round(clock() - step, 2)
    step = clock()
    if prepare:
        with publication_database.transaction(dict_cursor=True) as cursor:
            store_daily_products_cursor(cursor, products)
        products.timings["store_seconds"] = round(clock() - step, 2)
        return DailyProductsOutcome(PREPARED, str(cohort_id), str(session), mode, input_sha256=products.input_sha256,
                                    snapshots=snapshots, timings=products.timings, counts=products.counts)
    with publication_database.transaction(dict_cursor=True) as cursor:
        status, release, reason = publish_daily_products_cursor(cursor, products)
    if status == OUTRANKED and products.prepared:
        # A window published above the prepared products between the load and the release (review P1): compute
        # again, once, above the current release.
        outranked = reason
        products, _stored = compute_daily_products(compute_database, cohort_id=cohort_id, session=session,
                                                   cutoff=cutoff, mode=mode, source_closure=source_closure,
                                                   force=True, workers=workers, clock=clock)
        products.counts["prepared_outranked"] = outranked
        snapshots = {f"{s.snapshot_type}:{s.snapshot_key}": str(s.snapshot_id) for s in _snapshots(products)}
        step = clock()
        products.counts["strategy_evidence_writes"] = write_strategy_evidence(compute_database, products.strategy)
        products.timings["evidence_seconds"] = round(clock() - step, 2)
        step = clock()
        with publication_database.transaction(dict_cursor=True) as cursor:
            status, release, reason = publish_daily_products_cursor(cursor, products)
    if status == OUTRANKED:
        raise ValueError(f"Daily products generation is below the current release ({reason})")
    products.timings["publish_seconds"] = round(clock() - step, 2)
    return DailyProductsOutcome(
        status, str(cohort_id), str(session), mode, input_sha256=products.input_sha256,
        snapshots=snapshots if status == PUBLISHED else {},
        release_id=None if release is None else str(release.release_id),
        release_cutoff=None if release is None else release.cutoff.isoformat(),
        timings=products.timings, counts=products.counts, reason=reason)
