"""Bounded target-local producer and stored-only Ticker Workspace reader."""
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import hashlib
from pathlib import Path
from threading import Lock
from uuid import UUID

from equity.calendar import latest_expected_market_time, staleness_state
from equity.domain import EvidenceType
from equity.materialization import CHANNEL_VERSION, FEATURE_VERSION, PATTERN_VERSION, SETUP_VERSION
from equity.ticker_contracts import PATTERN_WATCH_INTERVALS

from .contracts import canonical_sha256
from .database_access import RealtimeDatabaseRole
from .equity_bar_repository import load_current_bar_groups_cursor
from .equity_evidence_repository import (
    EvidenceRef,
    RealtimeEquityMaterializationWrite,
    RealtimeFeatureDefinition,
    load_release_evidence_bodies_cursor,
    persist_realtime_equity_materialization_batch_cursor,
    resolve_release_evidence_refs_cursor,
    verify_realtime_equity_materializations_persisted_cursor,
)
from .lineage_head import inputs_sha256, mapping_identity, reference_set, require_member_set, workspace_set
from .portal_compatibility_repository import (
    SNAPSHOT_IDENTITY_RELATION, SNAPSHOT_RELATION, BoundedCache, _RealtimeCompatibilityRepository,
    _active_cohort, _utc, _TICKER_PATTERN,
)
from .portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE, RETENTION_HOT_AND_KEPT, persist_portal_snapshot_cursor,
    persist_screening_generation_cursor,
    portal_session_date, publish_portal_release_cursor, session_close_snapshot,
)
from .screening_repository import load_realtime_screening_input
from .ticker_projection import TICKER_WORKSPACE_VERSION, TickerProjectionState, project_realtime_ticker_workspace
from .ticker_projection import TICKER_WORKSPACE_SUMMARY_VERSION, produce_realtime_ticker_workspace_summary
from .ticker_projection import PATTERN_WATCH_PUBLICATION_VERSION, produce_realtime_pattern_watch_publication
from .ticker_references import BarLineageChanged, chart_group_ranges, compose_charts, evidence_expectations
from .worker_database import RealtimeProcessDatabase


PERIOD_DAYS = {
    "1d": 1, "5d": 5, "1mo": 30, "3mo": 90, "6mo": 180,
    "1y": 365, "2y": 730, "4y": 1460, "5y": 1825,
}
TICKER_EVIDENCE_CHUNK_SIZE = 100
TICKER_SNAPSHOT_CACHE_SIZE = 512
# One entry is one interval of one workspace snapshot: up to about 5,900 points (5m), so the entry bound is
# also the memory bound.
TICKER_CHART_CACHE_SIZE = 128
PINNED_WORKSPACE_NOTICE = (
    "The pinned Ticker Workspace is older than the retained hot window; "
    "showing the current view."
)
_CURRENT_SCREENING_JOIN = (
    "JOIN rt.screening_current AS screening_current ON screening_current.cohort_id=cohort.cohort_id "
    "JOIN rt.screening_generations AS screening ON screening.generation_id=screening_current.generation_id "
    "AND screening.cohort_id=screening_current.cohort_id AND screening.status='COMPLETE' "
)


class TickerPublicationUnavailable(LookupError):
    pass


_TECHNICAL_SOURCE_CONTRACTS = {
    "30m": ("RAW_UNADJUSTED", "1h", "DERIVED_SESSION_HOURLY"),
    "1h": ("RAW_UNADJUSTED", "1d", "ADJUSTED"),
    "1d": ("ADJUSTED", "1h", "RAW_UNADJUSTED"),
    "1wk": ("ADJUSTED", "1d", "ADJUSTED"),
    "1mo": ("ADJUSTED", "1wk", "ADJUSTED"),
}


def _ticker_feature_definition(result, history_policy_sha256, *, pattern_watch=False):
    primary_basis, confirmation_interval, confirmation_basis = (
        _TECHNICAL_SOURCE_CONTRACTS[result.interval]
    )
    dependency_spec = {
        "version": "realtime-ticker-technical-input-v1",
        "primary_interval": result.interval,
        "primary_price_basis": primary_basis,
        "primary_window": 500 if result.interval in {"30m", "1h"} else "RETAINED",
        "confirmation_interval": confirmation_interval,
        "confirmation_price_basis": confirmation_basis,
        "history_policy_sha256": history_policy_sha256,
        "setup_version": SETUP_VERSION,
    }
    if pattern_watch:
        dependency_spec = {
            "version": "realtime-pattern-watch-input-v1",
            "primary_interval": result.interval,
            "primary_price_basis": primary_basis,
            "primary_window": 300,
            "confirmation_interval": "1h" if result.interval == "30m" else None,
            "confirmation_price_basis": "DERIVED_SESSION_HOURLY" if result.interval == "30m" else None,
            "history_policy_sha256": history_policy_sha256,
            "pattern_version": PATTERN_VERSION,
            "channel_version": CHANNEL_VERSION,
        }
    policy_sha256 = canonical_sha256(dependency_spec)
    code_sha256 = hashlib.sha256(
        Path(__file__).parents[1].joinpath("equity", "materialization.py").read_bytes()
    ).hexdigest()
    feature = next(
        row for row in result.materialization.evidence
        if row.evidence_type is EvidenceType.FEATURE_SNAPSHOT
    )
    bundle_sha256 = canonical_sha256({
        "bundle_version": FEATURE_VERSION,
        "feature_schema_version": feature.payload_schema_version,
        "dependency_spec": dependency_spec,
        "policy_sha256": policy_sha256,
        "code_sha256": code_sha256,
    })
    return RealtimeFeatureDefinition(
        bundle_sha256=bundle_sha256,
        bundle_version=FEATURE_VERSION,
        feature_schema_version=feature.payload_schema_version,
        dependency_spec=dependency_spec,
        policy_sha256=policy_sha256,
        code_sha256=code_sha256,
    )


def _verify_ticker_evidence_parity(projection):
    # The stored index references exactly the evidence the cycle persists: nothing missing, nothing extra.
    embedded = {}
    for snapshot in projection.snapshots:
        for rows in snapshot.payload["evidence_index"].values():
            for row in rows:
                identity = (row["evidence_type"], row["payload_sha256"])
                previous = embedded.setdefault(row["evidence_id"], identity)
                if previous != identity:
                    raise ValueError("Ticker embedded evidence identity conflict")
    expected = {
        str(row.evidence_id): (row.evidence_type.value, row.payload_sha256)
        for result in (*projection.materializations, *projection.pattern_materializations)
        for row in result.materialization.evidence
    }
    if not expected or embedded != expected:
        raise ValueError("Ticker persisted evidence does not match portal evidence")


def _evidence_items(payload):
    """Evidence references of a workspace payload: the compact index, or the rows a payload published
    before the move to references still embeds."""
    index = payload.get("evidence_index")
    return payload["evidence"] if index is None else index


def _embedded_setup_refs(payload, *, cohort_id, frames=None):
    embedded = {item["evidence_id"]: item for rows in _evidence_items(payload).values() for item in rows}
    metrics = payload["multi"]["setup_read_metrics"]
    selected_frames = tuple(frames if frames is not None else metrics)
    refs = {}
    for frame in selected_frames:
        metric = metrics[frame]
        evidence = embedded.get(metric["evidence_id"])
        if (evidence is None or evidence["evidence_type"] != EvidenceType.TRADE_SETUP.value
                or evidence.get("market_time", metric["market_time"]) != metric["market_time"]
                or evidence.get("observed_at", metric["observed_at"]) != metric["observed_at"]):
            raise ValueError("Release-selected setup evidence metadata changed")
        refs[frame] = EvidenceRef(UUID(str(cohort_id)), UUID(metric["evidence_id"]),
                                  EvidenceType.TRADE_SETUP, _stamp(metric["market_time"]),
                                  evidence["payload_sha256"])
    return refs, metrics


def _setup_ref_mapping(ref, payload_sha256, metric):
    return dict(cohort_id=str(ref.cohort_id), evidence_id=str(ref.evidence_id),
                evidence_type=ref.evidence_type.value, market_time=ref.market_time.isoformat(),
                payload_sha256=payload_sha256, observed_at=metric["observed_at"],
                published_at=metric["published_at"])


def _release_setup_refs(cursor, payload, *, cohort_id, security_id, cutoff, frames=None):
    refs, metrics = _embedded_setup_refs(payload, cohort_id=cohort_id, frames=frames)
    resolved = resolve_release_evidence_refs_cursor(
        cursor, tuple(refs.values()), security_id=UUID(str(security_id)), known_by=cutoff,
    )
    return {frame: _setup_ref_mapping(ref, resolved[ref.evidence_id].payload_sha256, metrics[frame])
            for frame, ref in refs.items()}


def _verify_setup_bodies(payload, bodies):
    multi = payload["multi"]
    for frame, setup in multi["setups"].items():
        metric = multi["setup_read_metrics"][frame]
        body = bodies.get(UUID(metric["evidence_id"]))
        if (body is None or body["market_time"] != _stamp(metric["market_time"])
                or body["observed_at"] != _stamp(metric["observed_at"])
                or {key: value for key, value in setup.items() if key != "computed_at"} != body["payload"]):
            raise ValueError("Release-selected setup evidence metadata changed")


def _verified_evidence_bodies(cursor, manifest, payload, *, cohort_id, cutoff):
    """Every evidence row the stored index pins, read back and verified: id, type and payload hash against the
    index, the payload against its stored digest, and each setup against the `multi` section it was published
    in. `compose_evidence` rebuilds the embedded mapping from them. Nothing reads the bodies, so the reader
    verifies them at first load and does not keep them."""
    index = payload["evidence_index"]
    stored = load_release_evidence_bodies_cursor(
        cursor, evidence_expectations(index), cohort_id=cohort_id,
        security_id=UUID(str(manifest["reference"]["security_id"])), provider_tier=manifest["provider_tier"],
        known_by=cutoff,
    )
    _verify_setup_bodies(payload, stored)
    return stored


def _chart_source(manifest, *, cohort_id, snapshot_id):
    """What a chart read needs to compose one interval later: where its bars are and the lineage head to verify
    them against."""
    reference = manifest["reference"]
    return dict(
        cohort_id=str(cohort_id), snapshot_id=str(snapshot_id), provider_tier=manifest["provider_tier"],
        instrument_id=reference["instrument_id"], security_id=str(reference["security_id"]),
        ticker=reference["ticker"], bar_lineage=manifest["bar_lineage"],
        history_source_cohort_id=manifest.get(
            "history_source_cohort_id"
        ),
        history_source_provider_tier=manifest.get(
            "history_source_provider_tier"
        ),
        history_before=manifest.get("history_before"),
    )


def _session_close_summary(summary, snapshots):
    # Embeds each workspace's exact setup reference so the kept summary never resolves
    # a per-ticker workspace or an evidence row when it is read.
    setup_refs = {}
    for snapshot in snapshots:
        refs, metrics = _embedded_setup_refs(
            snapshot.payload, cohort_id=snapshot.cohort_id, frames=("1d",),
        )
        setup_refs[snapshot.snapshot_key] = _setup_ref_mapping(
            refs["1d"], refs["1d"].payload_sha256, metrics["1d"],
        )
    return session_close_snapshot(
        summary,
        source_manifest=dict(summary.source_manifest, setup_evidence_set=mapping_identity(setup_refs)),
        payload=dict(summary.payload, setup_evidence_refs=setup_refs),
    )


@dataclass(frozen=True, slots=True)
class TickerCycle:
    """The in-process result of the evidence half of a ticker cycle: what publishing needs."""

    inputs: object
    projection: object
    summary: object
    pattern_publication: object


class CycleRehydrationMismatch(ValueError):
    """The cycle rebuilt from durable data is not the one the EQUITY_CYCLE receipt names."""


def _project_ticker_cycle(database: RealtimeProcessDatabase, *, state, loader, scope):
    """Read-only half shared by compute and rehydration: load the inputs and project the cycle."""
    load_inputs = loader.load if loader is not None else load_realtime_screening_input
    with database.transaction(
        read_only=True, dict_cursor=True, isolation_level="REPEATABLE READ",
    ) as cursor:
        inputs = load_inputs(cursor, **scope, include_portal_history=True)
        market_events, market_event_coverage = _load_ticker_calendar_inputs(
            cursor, inputs.members, inputs.cutoff,
        )
        fundamental_reports, fundamental_coverage = _load_ticker_fundamental_inputs(
            cursor, inputs.members, inputs.cutoff,
        )
        projection = project_realtime_ticker_workspace(
            inputs, market_events=market_events, market_event_coverage=market_event_coverage,
            fundamental_reports=fundamental_reports, fundamental_coverage=fundamental_coverage,
            state=state,
        )
        snapshots = projection.snapshots
        summary = produce_realtime_ticker_workspace_summary(inputs, snapshots, projection.summary_charts)
        pattern_publication = produce_realtime_pattern_watch_publication(inputs, snapshots)
    return inputs, projection, summary, pattern_publication


def _materialization_write_groups(projection, inputs):
    """The evidence writes of a cycle: the workspace materializations (they move the evidence-current pointer),
    then the Pattern Watch ones (they do not)."""
    history_policy_sha256 = projection.snapshots[0].source_manifest["history_policy_sha256"]
    for results, pattern_watch in ((projection.materializations, False), (projection.pattern_materializations, True)):
        yield tuple(
            RealtimeEquityMaterializationWrite(
                result=result,
                definition=_ticker_feature_definition(result, history_policy_sha256, pattern_watch=pattern_watch),
                generation=inputs.source_generation,
                update_evidence_current=not pattern_watch,
            )
            for result in results
        )


def compute_realtime_ticker_cycle(
    compute_database: RealtimeProcessDatabase, *, state: TickerProjectionState | None = None,
    loader=None, **scope,
) -> TickerCycle:
    """EQUITY_EVIDENCE half: load inputs, project (incrementally with `state`) and persist the
    evidence. Nothing is published. `loader` is the runner's long-lived `ScreeningInputLoader`
    (delta loads); without it every retained bar is re-read. `scope` carries cohort_id, session, cutoff (the
    market boundary), source_generation and optionally `known_by` (when a bar must have been recorded, not
    before the cutoff; default the cutoff), all forwarded unchanged to the loader."""
    if not isinstance(compute_database, RealtimeProcessDatabase) or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Realtime ticker production requires the target-local COMPUTE role")
    inputs, projection, summary, pattern_publication = _project_ticker_cycle(
        compute_database, state=state, loader=loader, scope=scope,
    )
    snapshots = projection.snapshots
    _verify_ticker_evidence_parity(projection)
    for writes in _materialization_write_groups(projection, inputs):
        for offset in range(0, len(writes), TICKER_EVIDENCE_CHUNK_SIZE):
            with compute_database.transaction() as cursor:
                persist_realtime_equity_materialization_batch_cursor(
                    cursor, writes[offset:offset + TICKER_EVIDENCE_CHUNK_SIZE],
                )
    generation = projection.screening_generation
    if generation is not None:
        _require_workspace_generation(inputs, snapshots, generation)
        with compute_database.transaction() as cursor:
            persist_screening_generation_cursor(cursor, generation)
        # Persisted: do not keep its megabytes of lineage alive until the publication half runs.
        projection = replace(projection, screening_generation=None)
    return TickerCycle(inputs, projection, summary, pattern_publication)


def _require_workspace_generation(inputs, snapshots, generation):
    """The screening generation the compute stage persists is the one the cycle's workspaces name: it must be
    COMPLETE for exactly this cohort, tier and session, generated no later than the cutoff, and its manifest
    generation digest must be the `context.cohort.generation` every workspace embeds. The reader's current
    mode (and the screening readers) resolve the cohort through it."""
    if (
        generation.status != "COMPLETE" or generation.cohort_id != inputs.cohort_id
        or generation.provider_tier != inputs.provider_tier or generation.session_date != inputs.session
        or generation.generated_at > inputs.cutoff or len(generation.rows) != len(inputs.members)
        or generation.expected_members != len(inputs.members)
        or any(snapshot.payload["context"]["cohort"]["generation"] != generation.manifest["generation"]
               for snapshot in snapshots)
    ):
        raise ValueError("Ticker screening generation does not match the cycle's workspaces")


def rehydrate_realtime_ticker_cycle(
    publication_database: RealtimeProcessDatabase, *, input_sha256: str, universe_sha256: str, **scope,
) -> TickerCycle:
    """PORTAL_PUBLISH recovery when the in-process hand-off is gone (a restart between the two stages).

    Rebuilds the cycle EQUITY_EVIDENCE computed from durable rows only, read-only under the PUBLICATION
    role: a stateless full load (the same `scope`, in particular `known_by` = the window deadline, so a bar
    recorded after the window is invisible) and a cold projection. It is deterministic, and the durable
    state proves it: the rebuilt input and universe identities must equal the ones the EQUITY_CYCLE receipt
    names, and every evidence row the rebuilt materializations name must be persisted with the identity they
    imply (a different build or policy would name evidence that was never written). Otherwise
    CycleRehydrationMismatch: fail closed, nothing is published from a cycle the evidence stage did not
    compute. Nothing is written. The cost is a cold projection (about 1.3 s per ticker on the quiet
    reference machine); the long-lived loader and projection state are deliberately not touched, since the
    window may be older than what they hold."""
    if not isinstance(publication_database, RealtimeProcessDatabase) or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION:
        raise ValueError("Realtime ticker rehydration requires the target-local PUBLICATION role")
    inputs, projection, summary, pattern_publication = _project_ticker_cycle(
        publication_database, state=None, loader=None, scope=scope,
    )
    if inputs_sha256(inputs) != input_sha256 or reference_set(inputs.members)["sha256"] != universe_sha256:
        raise CycleRehydrationMismatch("the rebuilt cycle is not the one the EQUITY_CYCLE receipt names")
    try:
        with publication_database.transaction(read_only=True, dict_cursor=True) as cursor:
            for writes in _materialization_write_groups(projection, inputs):
                verify_realtime_equity_materializations_persisted_cursor(cursor, writes)
    except ValueError as error:
        raise CycleRehydrationMismatch(str(error)) from error
    generation = projection.screening_generation
    if generation is not None:
        _require_workspace_generation(inputs, projection.snapshots, generation)
        projection = replace(projection, screening_generation=None)
    return TickerCycle(inputs, projection, summary, pattern_publication)


def publish_realtime_ticker_cycle(
    publication_database: RealtimeProcessDatabase, cycle: TickerCycle, *, receipt=None, session_close=False,
):
    """PORTAL_PUBLISH half: persist the cycle's snapshots and publish the release for `receipt`."""
    if not isinstance(publication_database, RealtimeProcessDatabase) or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION:
        raise ValueError("Realtime ticker persistence requires the target-local PUBLICATION role")
    with publication_database.transaction() as cursor:
        published, kept = persist_realtime_ticker_cycle_cursor(
            cursor,
            cycle,
            session_close=session_close,
        )
        if receipt is not None:
            publish_portal_release_cursor(cursor, receipt, published)
            if session_close:
                publish_portal_release_cursor(
                    cursor, receipt, kept, release_class=RELEASE_CLASS_SESSION_CLOSE,
                    session_date=portal_session_date(cycle.summary),
                )
    return cycle.projection.snapshots


def persist_realtime_ticker_cycle_cursor(cursor, cycle: TickerCycle, *, session_close=False):
    snapshots, summary = cycle.projection.snapshots, cycle.summary
    pattern_publication = cycle.pattern_publication
    for snapshot in snapshots:
        persist_portal_snapshot_cursor(cursor, snapshot)
    if session_close:
        kept = (
            persist_portal_snapshot_cursor(
                cursor,
                summary,
                retention=RETENTION_HOT_AND_KEPT,
                kept_snapshot=_session_close_summary(summary, snapshots),
            ),
            persist_portal_snapshot_cursor(
                cursor,
                pattern_publication,
                retention=RETENTION_HOT_AND_KEPT,
            ),
        )
    else:
        persist_portal_snapshot_cursor(cursor, summary)
        persist_portal_snapshot_cursor(cursor, pattern_publication)
        kept = ()
    return (*snapshots, summary, pattern_publication), kept


def materialize_realtime_ticker_workspace(
    compute_database: RealtimeProcessDatabase,
    publication_database: RealtimeProcessDatabase,
    *,
    receipt=None,
    session_close=False,
    state: TickerProjectionState | None = None,
    **scope,
):
    """Evidence then publication in one call. `state` is the runner's long-lived incremental state;
    omitting it recomputes everything."""
    if not isinstance(compute_database, RealtimeProcessDatabase) or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Realtime ticker production requires the target-local COMPUTE role")
    if not isinstance(publication_database, RealtimeProcessDatabase) or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION:
        raise ValueError("Realtime ticker persistence requires the target-local PUBLICATION role")
    cycle = compute_realtime_ticker_cycle(compute_database, state=state, **scope)
    return publish_realtime_ticker_cycle(
        publication_database, cycle, receipt=receipt, session_close=session_close,
    )


def _load_ticker_calendar_inputs(cursor, members, cutoff):
    security_ids = [str(member["security_id"]) for member in members]
    cursor.execute(
        "SELECT DISTINCT ON (source, event_key) revision_id, security_id, event_key, event_type, "
        "event_date, announced_at, event_timezone, time_precision, status, source, observed_at, "
        "received_at, valid_until, payload_sha256 FROM ref.market_event_revisions AS event "
        "WHERE (security_id IS NULL OR security_id=ANY(%s::uuid[])) AND observed_at<=%s "
        "AND received_at<=%s AND valid_until>%s AND NOT EXISTS (SELECT 1 FROM ref.market_event_revisions AS newer "
        "WHERE newer.supersedes_revision_id=event.revision_id AND newer.observed_at<=%s) "
        "ORDER BY source, event_key, observed_at DESC, revision_id",
        (security_ids, cutoff, cutoff, cutoff, cutoff),
    )
    events = tuple(dict(row) for row in cursor.fetchall())
    cursor.execute(
        "SELECT DISTINCT ON (source, event_type, security_id, window_start, window_end) "
        "coverage_id, security_id, event_type, window_start, window_end, source, verdict, "
        "checked_at, valid_until, evidence_sha256 FROM ref.market_event_coverage "
        "WHERE (security_id IS NULL OR security_id=ANY(%s::uuid[])) AND checked_at<=%s "
        "AND valid_until>%s ORDER BY source, event_type, security_id, window_start, window_end, "
        "checked_at DESC, coverage_id",
        (security_ids, cutoff, cutoff),
    )
    return events, tuple(dict(row) for row in cursor.fetchall())


def _load_ticker_fundamental_inputs(cursor, members, cutoff):
    security_ids = [str(member["security_id"]) for member in members]
    cursor.execute(
        "SELECT report_id, security_id, security_revision_id, cik, accession_number, form_type, "
        "timeframe, fiscal_year, fiscal_quarter, period_end, filing_date, availability_time, "
        "observed_at, source, source_key, metrics, payload_sha256, quality_codes "
        "FROM ref.fundamental_reports WHERE security_id=ANY(%s::uuid[]) "
        "AND availability_time<=%s AND observed_at<=%s "
        "ORDER BY security_id, availability_time DESC, period_end DESC, report_id",
        (security_ids, cutoff, cutoff),
    )
    reports = tuple(dict(row) for row in cursor.fetchall())
    cursor.execute(
        "SELECT DISTINCT ON (security_id, source, window_start, window_end) coverage_id, "
        "security_id, window_start, window_end, source, report_count, response_sha256, "
        "checked_at, valid_until FROM ref.fundamental_report_coverage "
        "WHERE security_id=ANY(%s::uuid[]) AND checked_at<=%s AND valid_until>%s "
        "ORDER BY security_id, source, window_start, window_end, checked_at DESC, coverage_id",
        (security_ids, cutoff, cutoff),
    )
    return reports, tuple(dict(row) for row in cursor.fetchall())


def _stamp(value):
    return _utc(value if isinstance(value, datetime) else datetime.fromisoformat(value), "stored ticker clock")


def _availability(market_time, cutoff, interval):
    expected = latest_expected_market_time(cutoff - timedelta(minutes=15), interval)
    actual = _stamp(market_time)
    if actual > expected:
        raise TickerPublicationUnavailable(
            "Ticker source is ahead of the provider-qualified watermark: "
            f"interval={interval} actual={actual.isoformat()} expected={expected.isoformat()}"
        )
    state = staleness_state(actual, expected, is_fresh=actual == expected)
    return dict(state, expected_market_time=expected.isoformat())


def _reference_matches(reference, rows):
    return (len(rows) == 1
            and str(rows[0]["revision_id"]) == str(reference["security_revision_id"])
            and rows[0]["payload_sha256"] == reference["payload_sha256"])


def _retained_read_state(reference_current, action_drift):
    warnings = {}
    if not reference_current:
        warnings["reference"] = "Security reference is not current; displaying the retained publication."
    if action_drift:
        warnings["actions"] = "Corporate-action evidence changed; displaying the retained publication."
    return dict(read_mode="RETAINED", execution_permission=False, provider_fetch_performed=False,
                reference_status="CURRENT" if reference_current else "UNAVAILABLE",
                action_status="CHANGED" if action_drift else "CURRENT",
                current_data_available=reference_current and not action_drift, section_warnings=warnings)


def _reference_set_matches(expected, rows):
    return sorted((str(item["security_id"]), item["ticker"], str(item["security_revision_id"]), item["payload_sha256"])
                  for item in expected) == sorted((str(item["security_id"]), item["ticker"], str(item["revision_id"]), item["payload_sha256"])
                                                 for item in rows)


def _with_retained_state(payload, read_state):
    if read_state is None:
        return payload
    result = deepcopy(payload)
    result["_retained_read_state"] = read_state
    return result


def _read_availability(market_time, cutoff, interval, read_state):
    state = _availability(market_time, cutoff, interval)
    if read_state is not None and not read_state["current_data_available"]:
        state["status"] = "UNAVAILABLE"
    return state


class RealtimeTickerWorkspaceRepository(_RealtimeCompatibilityRepository):
    def __init__(self, cursor_factory, *, transaction_configured=False):
        super().__init__(cursor_factory, transaction_configured=transaction_configured)
        self._cache_guard = Lock()
        self._snapshot_cache = BoundedCache(TICKER_SNAPSHOT_CACHE_SIZE)
        self._chart_cache = BoundedCache(TICKER_CHART_CACHE_SIZE)
        self._snapshot_load_locks = {}

    def _load_lock(self, key):
        with self._cache_guard:
            return self._snapshot_load_locks.setdefault(key, Lock())

    def _chart(self, payload, interval):
        """One interval's chart points, composed from `rt.bar_current` as of the release lineage head and
        verified against it. Composed points live only in `_chart_cache`, keyed by snapshot and interval."""
        source = payload["_chart_source"]
        key = (source["cohort_id"], source["ticker"], source["snapshot_id"], interval)
        points = self._chart_cache.get(key)
        if points is None:
            with self._load_lock(key[:2] + key[3:]):
                points = self._chart_cache.get(key)
                if points is None:
                    points = self._load_chart(source, payload["chart_refs"][interval], payload["chart_market_times"], interval)
                    self._chart_cache.put(key, points)
        return points

    def _load_chart(self, source, group_key, chart_market_times, interval):
        manifest = dict(bar_lineage=source["bar_lineage"])
        refs = {interval: group_key}
        with self._cursor() as cursor:
            self._begin(cursor)
            rows = load_current_bar_groups_cursor(
                cursor, cohort_id=UUID(source["cohort_id"]), provider_tier=source["provider_tier"],
                instrument_id=source["instrument_id"], security_id=UUID(source["security_id"]),
                ticker=source["ticker"], groups=chart_group_ranges(manifest, refs),
                history_source_cohort_id=(
                    UUID(source["history_source_cohort_id"])
                    if source.get("history_source_cohort_id")
                    else None
                ),
                history_source_provider_tier=source.get(
                    "history_source_provider_tier"
                ),
                history_before=(
                    datetime.fromisoformat(source["history_before"])
                    if source.get("history_before")
                    else None
                ),
            )
        try:
            return compose_charts(manifest, refs, chart_market_times, rows)[interval]
        except BarLineageChanged as error:
            raise TickerPublicationUnavailable("Ticker bar lineage changed; republish required") from error

    def _current_workspaces(self, cursor, cohort_id, cutoff):
        release = self._release(cursor, cohort_id)
        entries = release.entries_of(TICKER_WORKSPACE_VERSION) if release is not None else ()
        return tuple(entry for entry in entries if datetime.fromisoformat(entry["generated_at"]) <= cutoff)

    def list_tickers(self, *, available_by, cohort_id=None):
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Realtime ticker catalog requires an explicit cohort")
        with self._cursor() as cursor:
            self._begin(cursor)
            tickers = tuple(entry["snapshot_key"] for entry in self._current_workspaces(cursor, cohort_id, cutoff))
        if not tickers:
            raise TickerPublicationUnavailable("Ticker catalog has no stored publication for this cohort")
        if len(tickers) != len(set(tickers)) or any(not _TICKER_PATTERN.fullmatch(ticker) for ticker in tickers):
            raise ValueError("Ticker catalog stored identity drift")
        return tickers

    def trading_sessions(self, *, interval, limit, available_by, cohort_id=None):
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Realtime trading sessions require an explicit cohort")
        if interval not in {"1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"}:
            raise ValueError("Realtime trading session interval is unsupported")
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("Realtime trading session limit must be in [1, 2000]")
        with self._cursor() as cursor:
            self._begin(cursor)
            cursor.execute(
                "SELECT DISTINCT session_date FROM rt.bar_catalog "
                "WHERE cohort_id=%s AND interval=%s AND observed_at<=%s "
                "ORDER BY session_date DESC LIMIT %s",
                (str(cohort_id), interval, cutoff, limit),
            )
            sessions = tuple(row["session_date"] for row in cursor.fetchall())
        if not sessions:
            raise TickerPublicationUnavailable(
                "Ticker Workspace has no retained trading sessions for this interval"
            )
        ordered = tuple(reversed(sessions))
        return {
            "interval": interval,
            "sessions": [session.isoformat() for session in ordered],
            "data_as_of": ordered[-1].isoformat(),
            "data_interval": interval,
            "provider_fetch_performed": False,
        }

    def materialization_status(self, *, available_by, cohort_id=None):
        cutoff = _utc(available_by, "available_by")
        if cohort_id is None:
            raise ValueError("Realtime ticker freshness requires an explicit cohort")
        with self._cursor() as cursor:
            self._begin(cursor)
            entries = self._current_workspaces(cursor, cohort_id, cutoff)
            rows = ()
            if entries:
                cursor.execute(
                    "SELECT snapshot.snapshot_key, snapshot.generated_at, "
                    "snapshot.payload->'chart_market_times' AS chart_market_times "
                    "FROM " + SNAPSHOT_RELATION + " "
                    "JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
                    "JOIN rt.screening_current AS screening_current ON screening_current.cohort_id=cohort.cohort_id "
                    "JOIN rt.screening_generations AS screening ON screening.generation_id=screening_current.generation_id "
                    "AND screening.cohort_id=screening_current.cohort_id AND screening.status='COMPLETE' "
                    "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s "
                    "AND snapshot.snapshot_id=ANY(%s::uuid[]) "
                    "AND snapshot.generated_at<=%s AND snapshot.provider_tier=screening.provider_tier "
                    "ORDER BY snapshot.snapshot_key",
                    (str(cohort_id), TICKER_WORKSPACE_VERSION,
                     [str(entry["snapshot_id"]) for entry in entries], cutoff),
                )
                rows = tuple(dict(row) for row in cursor.fetchall())
        tickers = tuple(row["snapshot_key"] for row in rows)
        if not rows:
            raise TickerPublicationUnavailable("Ticker Workspace has no stored publication for this cohort")
        if len(tickers) != len(set(tickers)) or any(not _TICKER_PATTERN.fullmatch(ticker) for ticker in tickers):
            raise ValueError("Ticker freshness stored identity drift")
        intervals = set(rows[0]["chart_market_times"])
        for row in rows[1:]:
            intervals.intersection_update(row["chart_market_times"])
        if not intervals:
            raise TickerPublicationUnavailable("Ticker Workspace has no common retained market interval")
        interval_states = {}
        for interval in sorted(intervals):
            actual = min(_stamp(row["chart_market_times"][interval]) for row in rows)
            state = _availability(actual, cutoff, interval)
            interval_states[interval] = dict(
                market_time=actual.isoformat(),
                published_at=min(_stamp(row["generated_at"]) for row in rows).isoformat(),
                **state,
            )
        rank = {"READY": 0, "STALE": 1, "EXPIRED": 2, "UNAVAILABLE": 3}
        status = max(interval_states.values(), key=lambda state: rank[state["status"]])["status"]
        stale_intervals = [
            interval for interval, state in interval_states.items()
            if state["status"] != "READY"
        ]
        stale_times = [
            _stamp(interval_states[interval]["market_time"])
            for interval in stale_intervals
        ]
        return dict(
            status=status,
            checked_at=cutoff.isoformat(),
            max_serveable_stale_sessions=max(
                state["max_serveable_stale_sessions"] for state in interval_states.values()
            ),
            staleness_sessions=max(state["staleness_sessions"] for state in interval_states.values()),
            last_market_time=max(
                _stamp(state["market_time"]) for state in interval_states.values()
            ).isoformat(),
            oldest_stale_market_time=min(stale_times).isoformat() if stale_times else None,
            stale_intervals=stale_intervals,
            pipeline_active=False,
            active_intervals=[],
            active_runs=[],
            intervals=interval_states,
            source=TICKER_WORKSPACE_VERSION,
            ticker_count=len(tickers),
            serving_latest_available=True,
        )

    def _summary_snapshot(self, cursor, *, cutoff, cohort_id, snapshot_id, snapshot_type, read_mode="current"):
        if read_mode not in {"current", "retained"}:
            raise ValueError("Unsupported ticker read mode")
        if cohort_id is None:
            raise ValueError("Realtime ticker workspace summary requires an explicit cohort")
        selected = str(snapshot_id) if snapshot_id is not None else None
        current_release = None
        if selected is None:
            current_release = self._release(cursor, cohort_id)
            entry = current_release.entry(snapshot_type, "PLATFORM") if current_release is not None else None
            if entry is None:
                raise TickerPublicationUnavailable("Ticker Workspace summary has no stored publication for this cohort")
            selected = str(entry["snapshot_id"])
        cursor.execute(
            "SELECT snapshot.*, "
            "manifest.manifest_sha256 AS cohort_manifest_sha256,"
            "history_binding.binding_sha256 AS history_binding_sha256 "
            "FROM " + SNAPSHOT_RELATION + " "
            "JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
            "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
            "LEFT JOIN rt.cohort_history_bindings AS history_binding "
            "ON history_binding.target_cohort_id=cohort.cohort_id "
            "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s AND snapshot.snapshot_key='PLATFORM' "
            "AND snapshot.generated_at<=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
            "AND manifest.created_at<=%s AND snapshot.snapshot_id=%s",
            (str(cohort_id), snapshot_type, cutoff, cutoff, cutoff, cutoff, selected),
        )
        row = cursor.fetchone()
        if row is None or row["payload"] is None:
            raise TickerPublicationUnavailable("Ticker Workspace summary has no stored publication for this cohort")
        if snapshot_id is not None and str(row["snapshot_id"]) != str(snapshot_id):
            raise ValueError("Ticker summary does not match the requested snapshot")
        self_contained = row.get("snapshot_class") == "SESSION_CLOSE"
        manifest, payload = row["source_manifest"], row["payload"]
        expected_source_manifest_sha256 = (
            canonical_sha256({
                "manifest_sha256": row["cohort_manifest_sha256"],
                "history_binding_sha256": row[
                    "history_binding_sha256"
                ],
            })
            if row["history_binding_sha256"] is not None
            else row["cohort_manifest_sha256"]
        )
        if (
            canonical_sha256(manifest) != row["source_manifest_sha256"]
            or canonical_sha256(payload) != row["payload_sha256"]
            or manifest["version"] != snapshot_type or payload["version"] != snapshot_type
            or str(manifest["cohort_id"]) != str(cohort_id)
            or str(payload["cohort_id"]) != str(cohort_id)
            or manifest["source_manifest_sha256"]
            != expected_source_manifest_sha256
            or manifest["provider_tier"] != row["provider_tier"]
            or manifest["source_generation"] != row["source_generation"]
            or payload["provider_fetch_performed"] is not False
            or payload["execution_permission"] is not False
        ):
            raise ValueError("Ticker Workspace summary identity/checksum drift")
        # The manifest is a constant-size head; the member lists live in the payload and must match it.
        require_member_set(manifest["reference_set"], reference_set(payload["references"]),
                           name="Ticker Workspace summary references")
        require_member_set(manifest["workspace_set"], workspace_set(payload["workspaces"]),
                           name="Ticker Workspace summary workspaces")
        if not self_contained:
            # A hot summary names its workspaces by id and payload digest, so they are resolved;
            # a session-close summary keeps those digests and never resolves a workspace.
            cursor.execute(
                "SELECT snapshot.snapshot_key AS ticker, snapshot.snapshot_id, snapshot.payload_sha256 "
                "FROM " + SNAPSHOT_IDENTITY_RELATION + " WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s "
                "AND snapshot.snapshot_id=ANY(%s::uuid[]) AND snapshot.generated_at<=%s ORDER BY snapshot.snapshot_key",
                (str(cohort_id), TICKER_WORKSPACE_VERSION,
                 [item["snapshot_id"] for item in payload["workspaces"]], cutoff),
            )
            workspaces = [dict(ticker=item["ticker"], snapshot_id=str(item["snapshot_id"]),
                               payload_sha256=item["payload_sha256"]) for item in cursor.fetchall()]
            if workspaces != payload["workspaces"]:
                raise TickerPublicationUnavailable("Ticker Workspace summary is behind current ticker publications")
            if current_release is not None and read_mode != "retained" and sorted(
                (entry["snapshot_key"], str(entry["snapshot_id"]))
                for entry in current_release.entries_of(TICKER_WORKSPACE_VERSION)
            ) != sorted((item["ticker"], str(item["snapshot_id"])) for item in payload["workspaces"]):
                raise TickerPublicationUnavailable("Ticker Workspace summary is behind current ticker publications")
        ids = [str(reference["security_id"]) for reference in payload["references"]]
        reference_cutoff = row["generated_at"] if read_mode == "retained" else cutoff
        cursor.execute(
            "SELECT security_id, ticker, revision_id, payload_sha256 FROM ref.security_revisions "
            "WHERE security_id=ANY(%s::uuid[]) AND effective_from<=%s "
            "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
            "ORDER BY ticker, revision_id",
            (ids, reference_cutoff, reference_cutoff, reference_cutoff),
        )
        current_references = [dict(item) for item in cursor.fetchall()]
        expected_references = sorted(payload["references"], key=lambda item: (item["ticker"], str(item["security_revision_id"])))
        if not self_contained and not _reference_set_matches(expected_references, current_references):
            raise TickerPublicationUnavailable("Ticker Workspace summary reference identity changed; republish required")
        if read_mode == "retained":
            cursor.execute(
                "SELECT security_id, ticker, revision_id, payload_sha256 FROM ref.security_revisions "
                "WHERE security_id=ANY(%s::uuid[]) AND effective_from<=%s "
                "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                "ORDER BY ticker, revision_id",
                (ids, cutoff, cutoff, cutoff),
            )
            reference_current = _reference_set_matches(expected_references, cursor.fetchall())
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions "
            "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) "
            "OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
            "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) AS action_drift",
            (ids, row["generated_at"], cutoff, ids, row["generated_at"], cutoff),
        )
        action_drift = cursor.fetchone()["action_drift"]
        if action_drift and read_mode != "retained" and not self_contained:
            raise TickerPublicationUnavailable("Ticker Workspace summary action evidence changed; republish required")
        if read_mode == "retained":
            row = dict(row, _retained_read_state=_retained_read_state(reference_current, action_drift))
        return row

    def workspace_publication(self, *, available_by, cohort_id=None, snapshot_id=None, read_mode="current"):
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            row = self._summary_snapshot(cursor, cutoff=cutoff, cohort_id=cohort_id,
                                         snapshot_id=snapshot_id, snapshot_type=TICKER_WORKSPACE_SUMMARY_VERSION,
                                         read_mode=read_mode)
            manifest, payload = row["source_manifest"], row["payload"]
            workspaces = payload["workspaces"]
            if row.get("snapshot_class") == "SESSION_CLOSE":
                setup_evidence_refs = deepcopy(payload.get("setup_evidence_refs", {}))
                require_member_set(manifest.get("setup_evidence_set", mapping_identity({})),
                                   mapping_identity(setup_evidence_refs),
                                   name="Ticker Workspace summary setup evidence")
            else:
                cursor.execute(
                    "SELECT snapshot.snapshot_key AS ticker, snapshot.payload->'multi' AS multi, "
                    "COALESCE(snapshot.payload->'evidence_index', snapshot.payload->'evidence') AS evidence, "
                    "snapshot.source_manifest->'reference' AS reference "
                    "FROM " + SNAPSHOT_RELATION + " "
                    "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s AND snapshot.snapshot_id=ANY(%s::uuid[])",
                    (str(cohort_id), TICKER_WORKSPACE_VERSION, [item["snapshot_id"] for item in workspaces]),
                )
                members = cursor.fetchall()
                if len(members) != len(workspaces):
                    raise ValueError("Ticker summary member evidence is missing")
                setup_evidence_refs = {item["ticker"]: _release_setup_refs(
                    cursor, item, cohort_id=cohort_id, security_id=item["reference"]["security_id"], cutoff=cutoff,
                    frames=("1d",),
                )["1d"] for item in members}
        read_state = row.get("_retained_read_state")
        availability_interval = (
            "1d"
            if row.get("snapshot_class") == "SESSION_CLOSE"
            else "5m"
        )
        state = _read_availability(
            payload["market_time"],
            cutoff,
            availability_interval,
            read_state,
        )
        publication = dict(
            cohort_id=str(cohort_id),
            snapshot_id=str(row["snapshot_id"]), payload_sha256=row["payload_sha256"],
            status=state["status"], session=payload["session"],
            market_time=payload["market_time"], generated_at=payload["generated_at"],
            read_at=cutoff.isoformat(), expected_market_time=state["expected_market_time"],
            scope_note="Retained Ticker Workspace summary; Dashboard-only scanner fields are unavailable.",
            price_basis="ADJUSTED_DAILY_WITH_RETAINED_QUOTE",
            missing_producers=payload["missing_producers"],
            strategy_ticker_available=False,
        )
        if read_state is not None:
            publication.update(read_state)
            publication["current_data_available"] = read_state["current_data_available"] and state["status"] == "READY"
        setup = deepcopy(payload["setup"])
        setup.update(is_fresh=state["status"] == "READY",
                     staleness_sessions=state["staleness_sessions"], publication=publication)
        return dict(overview=deepcopy(payload["overview"]), setup=setup, publication=publication,
                    workspaces=deepcopy(payload["workspaces"]), setup_evidence_refs=setup_evidence_refs)

    def scan_patterns(self, *, interval, limit, available_by, cohort_id=None, snapshot_id=None, read_mode="current"):
        if interval not in PATTERN_WATCH_INTERVALS:
            raise ValueError("Unsupported Pattern Watch interval")
        if not 1 <= limit <= 1500:
            raise ValueError("Pattern Watch limit must be within [1, 1500]")
        cutoff = _utc(available_by, "available_by")
        with self._cursor() as cursor:
            self._begin(cursor)
            row = self._summary_snapshot(cursor, cutoff=cutoff, cohort_id=cohort_id,
                                         snapshot_id=snapshot_id, snapshot_type=PATTERN_WATCH_PUBLICATION_VERSION,
                                         read_mode=read_mode)
            view = row["payload"]["views"].get(interval)
            if view is None or view["status"] != "COMPLETE":
                raise TickerPublicationUnavailable(f"Pattern Watch {interval} has no complete stored interval publication")
            read_state = row.get("_retained_read_state")
            state = _read_availability(view["market_time"], cutoff, interval, read_state)
            for reference in row["payload"]["references"]:
                if view["evidence_storage"] == "RELEASE_ONLY" or row.get("snapshot_class") == "SESSION_CLOSE":
                    continue
                refs = view["analysis_refs"].get(reference["ticker"], ())
                if sum(item["evidence_type"] == EvidenceType.FEATURE_SNAPSHOT.value for item in refs) != 1:
                    raise ValueError("Pattern Watch completed analysis receipt is missing")
                resolve_release_evidence_refs_cursor(cursor, tuple(EvidenceRef(
                    UUID(item["cohort_id"]), UUID(item["evidence_id"]), EvidenceType(item["evidence_type"]),
                    _stamp(item["market_time"]), item["payload_sha256"],
                ) for item in refs), security_id=UUID(str(reference["security_id"])), known_by=cutoff)
        result = deepcopy(view)
        result["results"] = result["results"][:limit]
        result.update(status=state["status"], read_source=PATTERN_WATCH_PUBLICATION_VERSION,
                      provider_fetch_performed=False, publication=dict(
                          cohort_id=str(cohort_id), snapshot_id=str(row["snapshot_id"]),
                          source_generation=row["source_generation"], generated_at=row["generated_at"].isoformat(),
                          market_time=view["market_time"], read_at=cutoff.isoformat(), status=state["status"],
                          workspaces=deepcopy(row["payload"]["workspaces"]),
                      ))
        if read_state is not None:
            result["publication"].update(read_state)
            result["publication"]["current_data_available"] = read_state["current_data_available"] and state["status"] == "READY"
        return result

    def _snapshot_identity(self, symbol, cutoff, cohort_id, snapshot_id=None, read_mode="current"):
        with self._cursor() as cursor:
            self._begin(cursor)
            if cohort_id is None:
                active = _active_cohort(cursor, cutoff)
                if active is None:
                    raise TickerPublicationUnavailable("Ticker active cohort is unavailable")
                cohort_id = UUID(str(active["cohort_id"]))
            notice = None
            selected = str(snapshot_id) if snapshot_id is not None else None
            if selected is not None:
                cursor.execute(
                    "SELECT snapshot_id, cohort_id, snapshot_type, snapshot_key FROM rt.snapshot_catalog "
                    "WHERE snapshot_id=%s",
                    (selected,),
                )
                pinned = cursor.fetchone()
                if pinned is None:
                    # Per-ticker workspaces are hot-only; a pin that left the hot window reads the current view.
                    notice = dict(code="PINNED_WORKSPACE_EXPIRED", message=PINNED_WORKSPACE_NOTICE,
                                  requested_snapshot_id=selected)
                    selected = None
            if selected is None:
                release = self._release(cursor, cohort_id)
                entry = release.entry(TICKER_WORKSPACE_VERSION, symbol) if release is not None else None
                if entry is None:
                    raise TickerPublicationUnavailable("Ticker Workspace has no stored publication for this cohort")
                selected = str(entry["snapshot_id"])
            statement = (
                "SELECT snapshot.snapshot_id, snapshot.payload_sha256, snapshot.source_manifest_sha256, "
                "snapshot.generated_at, snapshot.provider_tier, snapshot.source_generation, "
                "manifest.manifest_sha256 AS cohort_manifest_sha256, "
                "history_binding.binding_sha256 AS history_binding_sha256, "
                "snapshot.source_manifest->'reference' AS reference, "
                "snapshot.source_manifest->>'version' AS manifest_version, "
                "snapshot.source_manifest->>'cohort_id' AS manifest_cohort_id, "
                "snapshot.source_manifest->>'source_manifest_sha256' AS manifest_cohort_sha256, "
                "snapshot.source_manifest->>'history_binding_sha256' "
                "AS manifest_history_binding_sha256, "
                "snapshot.source_manifest->>'provider_tier' AS manifest_provider_tier, "
                "(snapshot.source_manifest->>'source_generation')::integer AS manifest_source_generation "
                "FROM " + SNAPSHOT_RELATION + " "
                "JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
                "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                "LEFT JOIN rt.cohort_history_bindings AS history_binding "
                "ON history_binding.target_cohort_id=cohort.cohort_id "
                + (_CURRENT_SCREENING_JOIN if read_mode == "current" else "") +
                "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s AND snapshot.snapshot_key=%s "
                "AND snapshot.generated_at<=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
                "AND manifest.created_at<=%s "
            )
            parameters = (str(cohort_id), TICKER_WORKSPACE_VERSION, symbol, cutoff, cutoff, cutoff, cutoff)
            if read_mode == "current":
                statement += "AND screening.generated_at<=%s AND snapshot.provider_tier=screening.provider_tier "
                parameters += (cutoff,)
            statement += " AND snapshot.snapshot_id=%s"
            parameters += (selected,)
            cursor.execute(statement, parameters)
            row = cursor.fetchone()
            if row is None:
                raise TickerPublicationUnavailable("Ticker Workspace has no stored publication for this cohort")
            if str(row["snapshot_id"]) != selected:
                raise ValueError("Ticker publication does not match the requested snapshot")
            reference = row["reference"]
            expected_source_manifest_sha256 = (
                canonical_sha256({
                    "manifest_sha256": row[
                        "cohort_manifest_sha256"
                    ],
                    "history_binding_sha256": row[
                        "history_binding_sha256"
                    ],
                })
                if row["history_binding_sha256"] is not None
                else row["cohort_manifest_sha256"]
            )
            if (
                row["manifest_version"] != TICKER_WORKSPACE_VERSION
                or reference["ticker"] != symbol
                or row["manifest_cohort_sha256"]
                != expected_source_manifest_sha256
                or row["manifest_history_binding_sha256"]
                != row["history_binding_sha256"]
                or str(row["manifest_cohort_id"]) != str(cohort_id)
                or row["manifest_provider_tier"] != row["provider_tier"]
                or row["manifest_source_generation"] != row["source_generation"]
            ):
                raise ValueError("Ticker stored publication identity drift")
            reference_cutoff = row["generated_at"] if read_mode == "retained" else cutoff
            cursor.execute(
                "SELECT revision_id, payload_sha256 FROM ref.security_revisions "
                "WHERE security_id=%s AND ticker=%s AND effective_from<=%s "
                "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                "ORDER BY effective_from DESC, observed_at DESC, revision_id",
                (str(reference["security_id"]), symbol, reference_cutoff, reference_cutoff, reference_cutoff),
            )
            references = cursor.fetchall()
            if not _reference_matches(reference, references):
                raise TickerPublicationUnavailable("Ticker reference identity changed; republish required")
            if read_mode == "retained":
                cursor.execute(
                    "SELECT revision_id, payload_sha256 FROM ref.security_revisions "
                    "WHERE security_id=%s AND ticker=%s AND effective_from<=%s "
                    "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                    "ORDER BY effective_from DESC, observed_at DESC, revision_id",
                    (str(reference["security_id"]), symbol, cutoff, cutoff, cutoff),
                )
                reference_current = _reference_matches(reference, cursor.fetchall())
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions "
                "WHERE security_id=%s AND received_at>%s AND received_at<=%s) "
                "OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
                "WHERE security_id=%s AND received_at>%s AND received_at<=%s) AS action_drift",
                (str(reference["security_id"]), row["generated_at"], cutoff,
                 str(reference["security_id"]), row["generated_at"], cutoff),
            )
            action_drift = cursor.fetchone()["action_drift"]
            if action_drift and read_mode != "retained":
                raise TickerPublicationUnavailable("Ticker action evidence changed; republish required")
        identity = (
            str(row["snapshot_id"]), row["source_manifest_sha256"], row["payload_sha256"],
        )
        read_state = _retained_read_state(reference_current, action_drift) if read_mode == "retained" else None
        return identity, UUID(str(cohort_id)), read_state, notice

    def _snapshot(self, ticker, cutoff, cohort_id, snapshot_id=None, read_mode="current"):
        symbol = ticker.strip().upper()
        if not _TICKER_PATTERN.fullmatch(symbol):
            raise ValueError("Invalid ticker workspace symbol")
        identity, resolved_cohort_id, read_state, notice = self._snapshot_identity(
            symbol, cutoff, cohort_id, snapshot_id, read_mode)
        cache_key = (str(resolved_cohort_id), symbol, identity[0])
        cached = self._snapshot_cache.get(cache_key)
        if cached is None or cached[0] != identity:
            with self._load_lock(cache_key[:2]):
                cached = self._snapshot_cache.get(cache_key)
                if cached is None or cached[0] != identity:
                    cached = (identity, self._load_snapshot(
                        symbol, cutoff, resolved_cohort_id, UUID(identity[0]), read_mode))
                    self._snapshot_cache.put(cache_key, cached)
        result = _with_retained_state(cached[1], read_state)
        return dict(result, _pin_notice=notice) if notice is not None else result

    def _load_snapshot(self, symbol, cutoff, cohort_id, snapshot_id, read_mode="current"):
        with self._cursor() as cursor:
            self._begin(cursor)
            statement = (
                "SELECT snapshot.*, "
                "manifest.manifest_sha256 AS cohort_manifest_sha256,"
                "history_binding.binding_sha256 "
                "AS history_binding_sha256 "
                "FROM " + SNAPSHOT_RELATION + " "
                "JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
                "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                "LEFT JOIN rt.cohort_history_bindings AS history_binding "
                "ON history_binding.target_cohort_id=cohort.cohort_id "
                + (_CURRENT_SCREENING_JOIN if read_mode == "current" else "") +
                "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s AND snapshot.snapshot_key=%s "
                "AND snapshot.generated_at<=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
                "AND manifest.created_at<=%s "
            )
            parameters = (str(cohort_id), TICKER_WORKSPACE_VERSION, symbol, cutoff, cutoff, cutoff, cutoff)
            if read_mode == "current":
                statement += "AND screening.generated_at<=%s AND snapshot.provider_tier=screening.provider_tier "
                parameters += (cutoff,)
            statement += "AND snapshot.snapshot_id=%s"
            cursor.execute(statement, (*parameters, str(snapshot_id)))
            row = cursor.fetchone()
            if row is None or row["payload"] is None:
                raise TickerPublicationUnavailable("Ticker Workspace has no stored publication for this cohort")
            if str(row["snapshot_id"]) != str(snapshot_id):
                raise ValueError("Ticker payload does not match its selected snapshot")
            manifest, payload = row["source_manifest"], row["payload"]
            expected_source_manifest_sha256 = (
                canonical_sha256({
                    "manifest_sha256": row[
                        "cohort_manifest_sha256"
                    ],
                    "history_binding_sha256": row[
                        "history_binding_sha256"
                    ],
                })
                if row["history_binding_sha256"] is not None
                else row["cohort_manifest_sha256"]
            )
            if (
                canonical_sha256(manifest) != row["source_manifest_sha256"]
                or canonical_sha256(payload) != row["payload_sha256"]
                or manifest["version"] != TICKER_WORKSPACE_VERSION or payload["version"] != TICKER_WORKSPACE_VERSION
                or payload["ticker"] != symbol or manifest["reference"]["ticker"] != symbol
                or manifest["source_manifest_sha256"]
                != expected_source_manifest_sha256
                or str(manifest["cohort_id"]) != str(cohort_id)
                or manifest["provider_tier"] != row["provider_tier"]
                or manifest["source_generation"] != row["source_generation"]
                or payload["provider_fetch_performed"] is not False or payload["execution_permission"] is not False
            ):
                raise ValueError("Ticker stored publication identity/checksum drift")
            reference = manifest["reference"]
            reference_cutoff = row["generated_at"] if read_mode == "retained" else cutoff
            cursor.execute(
                "SELECT revision_id, payload_sha256 FROM ref.security_revisions "
                "WHERE security_id=%s AND ticker=%s AND effective_from<=%s "
                "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                "ORDER BY effective_from DESC, observed_at DESC, revision_id",
                (str(reference["security_id"]), symbol, reference_cutoff, reference_cutoff, reference_cutoff),
            )
            references = cursor.fetchall()
            if not _reference_matches(reference, references):
                raise TickerPublicationUnavailable("Ticker reference identity changed; republish required")
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions "
                "WHERE security_id=%s AND received_at>%s AND received_at<=%s) "
                "OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
                "WHERE security_id=%s AND received_at>%s AND received_at<=%s) AS action_drift",
                (str(reference["security_id"]), row["generated_at"], cutoff,
                 str(reference["security_id"]), row["generated_at"], cutoff),
            )
            if cursor.fetchone()["action_drift"] and read_mode != "retained":
                raise TickerPublicationUnavailable("Ticker action evidence changed; republish required")
            evidence_refs = _release_setup_refs(cursor, payload, cohort_id=cohort_id,
                                                security_id=reference["security_id"], cutoff=cutoff)
            if "chart_refs" in payload:
                _verified_evidence_bodies(cursor, manifest, payload, cohort_id=cohort_id, cutoff=cutoff)
        result = deepcopy(payload)
        if "chart_refs" in payload:
            result["_chart_source"] = _chart_source(manifest, cohort_id=cohort_id, snapshot_id=snapshot_id)
        result["evidence_refs"] = evidence_refs
        result["context"]["cohort"].update(
            snapshot_id=str(row["snapshot_id"]), payload_sha256=row["payload_sha256"],
        )
        return result

    def read(self, ticker, contract, *, available_by, cohort_id=None, interval="1d", period="1y", report_limit=12,
             days_forward=30, snapshot_id=None, read_mode="current"):
        cutoff = _utc(available_by, "available_by")
        if read_mode not in {"current", "retained"}:
            raise ValueError("Unsupported ticker read mode")
        if snapshot_id is not None and cohort_id is None:
            raise ValueError("Pinned ticker reads require an explicit cohort")
        payload = self._snapshot(ticker, cutoff, cohort_id, snapshot_id, read_mode)
        read_state = payload.get("_retained_read_state")
        if contract == "publication":
            context = payload["context"]["cohort"]
            quote = payload["quote"]
            quote_interval = "5m" if quote["source"] == "5m" else "1d"
            state = _read_availability(
                quote["as_of"],
                cutoff,
                quote_interval,
                read_state,
            )
            serving = deepcopy(read_state) if read_state is not None else {}
            if serving:
                serving["current_data_available"] = serving["current_data_available"] and state["status"] == "READY"
                if _availability(quote["as_of"], cutoff, quote_interval)["status"] != "READY":
                    serving["section_warnings"]["market"] = "Market data is not current; displaying the retained publication."
            publication = dict(
                cohort_id=str(cohort_id or context.get("cohort_id")), snapshot_id=context["snapshot_id"],
                payload_sha256=context["payload_sha256"], ticker=payload["ticker"],
                market_time=payload["quote"]["as_of"],
                generated_at=payload["multi"]["as_of"], read_at=cutoff.isoformat(),
                evidence_refs=deepcopy(payload["evidence_refs"]), **state, **serving)
            notice = payload.get("_pin_notice")
            if notice is not None:
                publication.update(notice=notice["message"], notice_code=notice["code"],
                                   requested_snapshot_id=notice["requested_snapshot_id"])
            return publication
        if contract == "profile":
            result = deepcopy(payload["profile"])
            result["fundamental_reports"] = result["fundamental_reports"][:report_limit]
            return result
        if contract == "calendar":
            result = deepcopy(payload["calendar"])
            horizon = cutoff + timedelta(days=days_forward)
            result["data"]["events"] = [
                row for row in result["data"]["events"]
                if _stamp(row["scheduled_time"]) <= horizon
            ]
            return result
        if contract in {"chart", "patterns", "channel"}:
            by_reference = contract == "chart" and "chart_refs" in payload
            values = payload["chart_refs"] if by_reference else payload[
                {"chart": "charts", "patterns": "patterns", "channel": "channels"}[contract]]
            if interval not in values:
                raise TickerPublicationUnavailable(payload["missing_producers"].get(f"chart:{interval}", "RETAINED_INTERVAL_UNAVAILABLE"))
            _read_availability(payload["chart_market_times"][interval], cutoff, interval, read_state)
            result = self._chart(payload, interval) if by_reference else values[interval]
            if contract != "chart":
                return result
            if period not in PERIOD_DAYS:
                raise ValueError("Unsupported ticker chart period")
            boundary = datetime.fromtimestamp(result[-1]["time"], cutoff.tzinfo) - timedelta(days=PERIOD_DAYS[period])
            boundary = boundary.replace(hour=0, minute=0, second=0, microsecond=0)
            return [point for point in result if point["time"] >= boundary.timestamp()]
        if contract == "quote":
            quote = payload["quote"]
            _read_availability(quote["as_of"], cutoff, "5m" if quote["source"] == "5m" else "1d", read_state)
            return quote
        if contract == "multi":
            result = deepcopy(payload["multi"])
            for frame in tuple(result["setups"]):
                metrics = result["setup_read_metrics"][frame]
                metrics.update(_read_availability(metrics["market_time"], cutoff, frame, read_state))
            if set(result["setups"]) != set(payload["multi"]["setup_sources"]):
                raise ValueError("Ticker setup source identity drift")
            return result
        if contract == "cross_patterns":
            for frame in payload["patterns"]:
                _read_availability(payload["chart_market_times"][frame], cutoff, frame, read_state)
            return payload["cross_patterns"]
        if contract == "context":
            result = deepcopy(payload["context"])
            state = _read_availability(result["cohort"]["market_time"], cutoff, "1d", read_state)
            result["cohort"]["status"] = result["status"] = state["status"]
            if result["rows"][0]["daily"]["status"] == "READY":
                result["rows"][0]["daily"]["status"] = state["status"]
            for frame, value in result["rows"][0]["frames"].items():
                if value["status"] == "READY":
                    try:
                        value["status"] = _read_availability(value["market_time"], cutoff, frame, read_state)["status"]
                    except TickerPublicationUnavailable as error:
                        value.update(status="UNAVAILABLE", reason=str(error), trend=None, momentum=None, close=None)
            return result
        raise ValueError("Unsupported ticker stored contract")
