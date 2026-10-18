"""Evidence-first persistence and stored-only reads of independent daily releases."""
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from uuid import UUID

import exchange_calendars

from equity.calendar import latest_expected_market_time, staleness_state
from equity.domain import EvidenceType
from equity.portal_scanners import DAILY_SCANNER_MINIMUM_BARS, HOURLY_SCANNER_MINIMUM_BARS, HOURLY_SCANNER_CONTRACTS
from equity.ticker_contracts import setup_summary_row

from .contracts import canonical_json, canonical_sha256
from .dashboard_projection import DAILY_SCANNER_CONTRACTS, SCANNER_BASIS
from .database_access import RealtimeDatabaseRole
from .equity_evidence_repository import (EvidenceRef, RealtimeEquityMaterializationWrite,
    RealtimeFeatureDefinition, persist_realtime_equity_materialization_batch_cursor,
    resolve_release_evidence_refs_cursor)
from .portal_compatibility_repository import (
    SNAPSHOT_RELATION, _RealtimeCompatibilityRepository, _utc,
)
from .portal_projection_repository import (
    RETENTION_KEPT, persist_portal_snapshot_cursor, portal_session_date, publish_portal_release_cursor,
    RELEASE_CLASS_SESSION_CLOSE,
)
from .preflight import ProviderTier
from .screening_repository import load_realtime_screening_input
from .strategy_projection import (STRATEGY_KEY, STRATEGY_VERSION, HISTORY_VERSION, RECONSTRUCTED_VERSION,
    ROLLING_VERSION, HOURLY_ROLLING_VERSION, project_realtime_daily_strategies,
    project_realtime_strategy_history, project_realtime_rolling_strategies, strategy_action_context,
    strategy_reference_identity)
from .strategy_persistence import build_strategy_persistence
from .hourly_strategy_projection import HOURLY_VERSION, HOURLY_HISTORY_VERSION, HOURLY_RECONSTRUCTED_VERSION, HOURLY_BASIS, split_coverage_ref
from .worker_database import RealtimeProcessDatabase


STRATEGY_VALIDATION_CACHE_SIZE = 16


class StrategyPublicationUnavailable(LookupError):
    pass


class StrategyQueryRejected(ValueError):
    pass


def _strategy_definition(result, manifest):
    feature = next(row for row in result.materialization.evidence if row.evidence_type is EvidenceType.FEATURE_SNAPSHOT)
    interval = manifest.get("interval", "1d")
    dependency = dict(version=manifest["version"], primary_interval=interval,
        primary_window="EXACT_RETAINED_DAILY" if interval == "1d" else "EXACT_FINALIZED_XNYS_RTH_SLOTS",
        scanner_basis=manifest["scanner_basis"], minimum_bars=DAILY_SCANNER_MINIMUM_BARS if interval == "1d" else HOURLY_SCANNER_MINIMUM_BARS,
        defaults=dict(fvg_lookback=50, ma_short=9, ma_long=21, fibonacci_swing_pct=5.0),
        confirmation="EXACT_LAST_20_DAILY_SESSIONS_1H_OR_UNAVAILABLE")
    if interval == "1h":
        dependency.update(defaults=dict(fvg_lookback=50, ma_short=9, ma_long=21,
            fibonacci_swing_policy="INTERVAL_AWARE_ATR_STRUCTURAL_CONFIRMED_LEG"), confirmation="COMPLETED_RAW_DAILY_ONLY_OR_UNAVAILABLE")
    if "action_policy" in manifest:
        dependency["action_policy"] = manifest["action_policy"]
    policy = canonical_sha256(dependency)
    code = canonical_sha256(manifest["code_sha256"])
    return RealtimeFeatureDefinition(canonical_sha256((feature.source_version, dependency, policy, code)),
        feature.source_version, feature.payload_schema_version, dependency, policy, code)


def _stored_clock_matches(value, encoded):
    return _utc(value, "stored strategy clock") == _utc(datetime.fromisoformat(encoded), "strategy manifest clock")


def _strategy_materialization_writes(projection):
    manifest = projection.snapshot.source_manifest
    evaluations = {_utc(datetime.fromisoformat(item["market_time"]), "strategy evaluation cutoff"): item
                   for item in _evaluation_manifests(manifest)}
    writes = []
    for result in projection.materializations:
        cutoff = result.materialization.evidence[0].market_time
        own = evaluations[cutoff]
        writes.append(RealtimeEquityMaterializationWrite(result, _strategy_definition(result, own),
            own["source_generation"], update_evidence_current=False))
    return tuple(writes)


def load_retained_strategy_history_cursor(cursor, inputs, *, interval="1d"):
    calendar = exchange_calendars.get_calendar("XNYS")
    first = str(calendar.session_offset(str(inputs.session), -4).date())
    versions = [STRATEGY_VERSION, HISTORY_VERSION, ROLLING_VERSION] if interval == "1d" else [HOURLY_VERSION, HOURLY_HISTORY_VERSION, HOURLY_ROLLING_VERSION]
    cursor.execute("SELECT DISTINCT ON (snapshot.session_date, snapshot.snapshot_type) snapshot.snapshot_id, "
        "snapshot.cohort_id, snapshot.provider_tier, snapshot.snapshot_type, snapshot.source_generation, "
        "snapshot.source_manifest, snapshot.source_manifest_sha256, snapshot.payload, snapshot.payload_sha256, "
        "snapshot.generated_at FROM " + SNAPSHOT_RELATION + " "
        "WHERE snapshot.cohort_id=%s AND snapshot.provider_tier=%s AND snapshot.snapshot_key=%s "
        "AND snapshot.snapshot_class='SESSION_CLOSE' AND snapshot.snapshot_type=ANY(%s) "
        "AND snapshot.generated_at<=%s AND snapshot.session_date BETWEEN %s AND %s "
        "ORDER BY snapshot.session_date, snapshot.snapshot_type, snapshot.generated_at DESC, "
        "snapshot.source_generation DESC, snapshot.snapshot_id LIMIT 15",
        (str(inputs.cohort_id), inputs.provider_tier.value, STRATEGY_KEY, versions, inputs.cutoff, first, str(inputs.session)))
    selected = {}
    for stored in cursor.fetchall():
        manifest, payload = stored["source_manifest"], stored["payload"]
        if (canonical_sha256(manifest) != stored["source_manifest_sha256"] or canonical_sha256(payload) != stored["payload_sha256"]
                or str(stored["cohort_id"]) != str(inputs.cohort_id) or stored["provider_tier"] != inputs.provider_tier.value
                or stored["snapshot_type"] not in versions or manifest["version"] != stored["snapshot_type"]
                or manifest["source_generation"] != stored["source_generation"]
                or manifest["source_manifest_sha256"] != inputs.source_manifest_sha256
                or manifest["cohort_id"] != str(inputs.cohort_id) or manifest["provider_tier"] != inputs.provider_tier.value
                or not _stored_clock_matches(stored["generated_at"], manifest["generated_at"])
                or _utc(stored["generated_at"], "retained release generation") > inputs.cutoff):
            raise ValueError("Rolling strategy source release identity/hash/availability drift")
        _validate_strategy_snapshot(manifest, payload)
        entries = manifest.get("history", [dict(session=payload["session"], source_manifest=manifest, payload=payload)])
        origin = dict(snapshot_id=str(stored["snapshot_id"]), source_manifest_sha256=stored["source_manifest_sha256"],
            payload_sha256=stored["payload_sha256"], source_generation=stored["source_generation"],
            generated_at=manifest["generated_at"])
        for entry in entries:
            session = entry["session"]
            if not first <= session < str(inputs.session):
                continue
            close = calendar.session_close(session).to_pydatetime()
            if _utc(datetime.fromisoformat(entry["payload"]["market_time"]), "retained session cutoff") != close:
                continue
            rank = (_utc(datetime.fromisoformat(entry["payload"]["generated_at"]), "retained outcome clock"), stored["source_generation"])
            if session not in selected or rank > selected[session][0]:
                copied = deepcopy(entry)
                copied["source_release"] = origin
                selected[session] = (rank, copied)
    history = tuple(selected[session][1] for session in sorted(selected))
    for item in history:
        _validate_durable(cursor, item["source_manifest"], known_by=inputs.recorded_by)
    return history


def _validate_rolling_sources(cursor, manifest, *, known_by):
    for item in manifest["history"][:-1]:
        ref = item.get("source_release")
        if ref is None:
            raise ValueError("Rolling retained outcome requires an immutable source release")
        cursor.execute("SELECT snapshot.snapshot_id, snapshot.cohort_id, snapshot.source_generation, "
            "snapshot.source_manifest, snapshot.source_manifest_sha256, snapshot.payload, "
            "snapshot.payload_sha256, snapshot.generated_at FROM " + SNAPSHOT_RELATION + " "
            "WHERE snapshot.snapshot_id=%s AND snapshot.cohort_id=%s AND snapshot.generated_at<=%s",
            (ref["snapshot_id"], manifest["cohort_id"], known_by))
        stored = cursor.fetchone()
        if (stored is None or str(stored["snapshot_id"]) != ref["snapshot_id"]
                or str(stored["cohort_id"]) != manifest["cohort_id"] or stored["source_generation"] != ref["source_generation"]
                or stored["source_manifest_sha256"] != ref["source_manifest_sha256"]
                or stored["payload_sha256"] != ref["payload_sha256"]
                or canonical_sha256(stored["source_manifest"]) != ref["source_manifest_sha256"]
                or canonical_sha256(stored["payload"]) != ref["payload_sha256"]
                or not _stored_clock_matches(stored["generated_at"], ref["generated_at"])):
            raise ValueError("Rolling strategy immutable source release drift")
        entries = stored["source_manifest"].get("history", [dict(session=stored["payload"]["session"],
            source_manifest=stored["source_manifest"], payload=stored["payload"])])
        original = next((entry for entry in entries if entry["session"] == item["session"]), None)
        if (original is None
                or canonical_json(original["source_manifest"]) != canonical_json(item["source_manifest"])
                or canonical_json(original["payload"]) != canonical_json(item["payload"])):
            raise ValueError("Rolling strategy copied session differs from immutable source release")


def materialize_due_realtime_strategies(compute_database, publication_database, *, cohort_id, available_by,
                                       intervals=("1d", "1h"), receipt=None, known_by=None):
    """Publish the strategy snapshot due at `available_by` for each interval. `available_by` is the market
    cutoff; `known_by` (not before it; default the cutoff) is when a bar must have been recorded, as for the
    screening input, and bounds the recording clocks the retained-snapshot validation reads."""
    cutoff = _utc(available_by, "strategy producer cutoff")
    recorded_by = cutoff if known_by is None else _utc(known_by, "strategy producer known_by")
    if recorded_by < cutoff:
        raise ValueError("Strategy producer known_by must not precede the cutoff")
    if not intervals or len(set(intervals)) != len(intervals) or set(intervals) - {"1d", "1h"}:
        raise ValueError("Strategy producer requires unique supported intervals")
    with compute_database.transaction(read_only=True, dict_cursor=True) as cursor:
        cursor.execute("SELECT profile.provider_tier FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
            "JOIN rt.capability_profiles AS profile ON profile.profile_id=manifest.profile_id "
            "WHERE cohort.cohort_id=%s AND cohort.effective_at<=%s AND cohort.created_at<=%s "
            "AND manifest.created_at<=%s AND profile.created_at<=%s", (str(cohort_id), cutoff, cutoff, cutoff, cutoff))
        profile = cursor.fetchone()
    if profile is None:
        raise ValueError("Strategy producer cohort/tier is unavailable")
    tier = ProviderTier(profile["provider_tier"])
    delay = timedelta(minutes=15 if tier is ProviderTier.MASSIVE_DEVELOPER else 0)
    calendar = exchange_calendars.get_calendar("XNYS")
    outcomes = []
    for interval in intervals:
        market_time = latest_expected_market_time(cutoff - delay, interval)
        session = calendar.date_to_session(str(market_time.date()), direction="previous").date()
        version = ROLLING_VERSION if interval == "1d" else HOURLY_ROLLING_VERSION
        with compute_database.transaction(read_only=True, dict_cursor=True) as cursor:
            cursor.execute("SELECT snapshot.snapshot_id, snapshot.source_manifest, snapshot.payload, "
                "snapshot.source_manifest_sha256, snapshot.payload_sha256 FROM " + SNAPSHOT_RELATION + " "
                "WHERE snapshot.cohort_id=%s AND snapshot.provider_tier=%s AND snapshot.snapshot_type=%s "
                "AND snapshot.snapshot_key=%s AND snapshot.session_date=%s AND snapshot.generated_at<=%s "
                "AND (snapshot.payload->>'market_time')::timestamptz=%s "
                "ORDER BY snapshot.generated_at DESC, snapshot.source_generation DESC, snapshot.snapshot_id LIMIT 1",
                (str(cohort_id), tier.value, version, STRATEGY_KEY, session, cutoff, market_time))
            stored = cursor.fetchone()
            if stored is not None:
                if (canonical_sha256(stored["source_manifest"]) != stored["source_manifest_sha256"]
                        or canonical_sha256(stored["payload"]) != stored["payload_sha256"]
                        or stored["source_manifest"]["cohort_id"] != str(cohort_id)
                        or stored["source_manifest"]["version"] != version):
                    raise ValueError("Strategy producer retained cutoff identity/hash drift")
                _validate_strategy_snapshot(stored["source_manifest"], stored["payload"])
                _validate_rolling_sources(cursor, stored["source_manifest"], known_by=recorded_by)
                for manifest in _evaluation_manifests(stored["source_manifest"]):
                    _validate_durable(cursor, manifest, known_by=recorded_by)
                outcomes.append(dict(interval=interval, market_time=market_time.isoformat(),
                    status="ALREADY_STORED", snapshot_id=str(stored["snapshot_id"]),
                    analysis=_analysis_statuses(stored["payload"])))
                continue
            cursor.execute("SELECT GREATEST((SELECT COALESCE(max(source_generation),0) FROM rt.snapshot_catalog "
                "WHERE cohort_id=%s),(SELECT COALESCE(max(generation),0) FROM rt.feature_snapshots WHERE cohort_id=%s))+1 AS generation",
                (str(cohort_id), str(cohort_id)))
            generation = int(cursor.fetchone()["generation"])
        snapshot = materialize_realtime_daily_strategies(compute_database, publication_database,
            cohort_id=cohort_id, session=session, cutoff=cutoff, source_generation=generation,
            rolling_history=True, interval=interval, market_time=market_time if interval == "1h" else None,
            receipt=receipt, **({} if known_by is None else {"known_by": recorded_by}))
        outcomes.append(dict(interval=interval, market_time=market_time.isoformat(),
            status="PUBLISHED", snapshot_id=str(snapshot.snapshot_id), analysis=_analysis_statuses(snapshot.payload)))
    return tuple(outcomes)


def _analysis_statuses(payload) -> tuple[str, ...]:
    """The distinct coverage statuses of the snapshot's scanner analyses (COMPLETE, COMPLETE_EMPTY, PARTIAL,
    INSUFFICIENT_HISTORY, UNAVAILABLE): a snapshot whose members could not be evaluated is still a valid,
    published snapshot, so the caller must be able to see it."""
    analysis = payload.get("analysis") if isinstance(payload, Mapping) else None
    if not isinstance(analysis, Mapping):
        return ()
    return tuple(sorted({str(item["status"]) for item in analysis.values() if isinstance(item, Mapping)
                         and "status" in item}))


def materialize_realtime_daily_strategies(compute_database, publication_database, *, expected_snapshot_id=None, progress=None,
                                        reconstruct_history=False, rolling_history=False, interval="1d", market_time=None,
                                        receipt=None, **scope):
    if interval not in {"1d", "1h"} or interval == "1d" and market_time is not None:
        raise ValueError("Strategy production requires an explicit supported interval/cutoff")
    if rolling_history and reconstruct_history:
        raise ValueError("Rolling production and reconstruction are separate modes")
    if (not isinstance(compute_database, RealtimeProcessDatabase)
            or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE):
        raise ValueError("Strategy production requires the target-local COMPUTE role")
    if (not isinstance(publication_database, RealtimeProcessDatabase)
            or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION):
        raise ValueError("Strategy persistence requires the target-local PUBLICATION role")
    with compute_database.transaction(read_only=True, dict_cursor=True, isolation_level="REPEATABLE READ") as cursor:
        inputs = load_realtime_screening_input(cursor, **scope, include_portal_history=True, include_strategy_action_details=True)
        if rolling_history:
            retained = load_retained_strategy_history_cursor(cursor, inputs, interval=interval)
            projection = project_realtime_rolling_strategies(inputs, retained_history=retained, interval=interval, market_time=market_time)
        else:
            projection = (project_realtime_strategy_history(inputs, interval=interval, market_time=market_time) if reconstruct_history
                else project_realtime_daily_strategies(inputs, **({"interval": interval, "market_time": market_time} if interval == "1h" else {})))
    return persist_realtime_strategy_projection(compute_database, publication_database, projection,
        expected_snapshot_id=expected_snapshot_id, progress=progress, receipt=receipt)


def persist_realtime_strategy_projection(compute_database, publication_database, projection, *,
                                        expected_snapshot_id=None, progress=None, receipt=None):
    if (not isinstance(compute_database, RealtimeProcessDatabase)
            or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE
            or not isinstance(publication_database, RealtimeProcessDatabase)
            or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION):
        raise ValueError("Strategy persistence requires target-local COMPUTE and PUBLICATION roles")
    snapshot = projection.snapshot
    _validate_strategy_snapshot(snapshot.source_manifest, snapshot.payload)
    if expected_snapshot_id is not None and str(snapshot.snapshot_id) != str(expected_snapshot_id):
        raise ValueError("Strategy inputs changed after the checked release; refusing evidence writes")
    writes = _strategy_materialization_writes(projection)
    with compute_database.transaction() as cursor:
        persist_realtime_equity_materialization_batch_cursor(cursor, writes)
    if progress is not None:
        progress.update(target_writes=bool(writes), strategy_evidence_committed=True,
            strategy_evidence_members=len(writes), release_generated_at=snapshot.generated_at.isoformat())
    with publication_database.transaction(dict_cursor=True) as cursor:
        _validate_strategy_snapshot(snapshot.source_manifest, snapshot.payload)
        # The snapshot is known at its own `generated_at` (its evidence is observed then): a producer running
        # before that instant (a window's deadline) must not fail the durability check on the wall clock.
        durable_by = max(datetime.now(timezone.utc), snapshot.generated_at)
        if snapshot.snapshot_type in {ROLLING_VERSION, HOURLY_ROLLING_VERSION}:
            _validate_rolling_sources(cursor, snapshot.source_manifest, known_by=durable_by)
        for manifest in _evaluation_manifests(snapshot.source_manifest):
            _validate_durable(cursor, manifest, known_by=durable_by)
        persist_portal_snapshot_cursor(cursor, snapshot, retention=RETENTION_KEPT)
        if receipt is not None:
            publish_portal_release_cursor(cursor, receipt, (snapshot,))
            session = portal_session_date(snapshot)
            if datetime.fromisoformat(snapshot.payload["market_time"]) == exchange_calendars.get_calendar(
                    "XNYS").session_close(str(session)).to_pydatetime():
                publish_portal_release_cursor(cursor, receipt, (snapshot,),
                    release_class=RELEASE_CLASS_SESSION_CLOSE, session_date=session)
    return snapshot


def _evaluation_manifests(manifest):
    return [item["source_manifest"] for item in manifest["history"]] if manifest["version"] in {
        HISTORY_VERSION, HOURLY_HISTORY_VERSION, ROLLING_VERSION, HOURLY_ROLLING_VERSION} else [manifest]


def _validate_strategy_snapshot(manifest, payload):
    _validate_release(manifest, payload)
    if manifest["version"] in {ROLLING_VERSION, HOURLY_ROLLING_VERSION}:
        _validate_rolling_snapshot(manifest, payload)
        return
    if manifest["version"] not in {HISTORY_VERSION, HOURLY_HISTORY_VERSION}:
        return
    interval = manifest.get("interval", "1d")
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [str(session.date()) for session in calendar.sessions_in_range(
        calendar.session_offset(payload["session"], -4), payload["session"])]
    history = manifest["history"]
    generations = [item["source_manifest"]["source_generation"] for item in history]
    if (any(type(generation) is not int or generation <= 0 for generation in generations)
            or len(set(generations)) != len(generations) or generations != sorted(generations)
            or manifest["source_generation"] != generations[-1]):
        raise ValueError("Strategy reconstructed feature generations must be distinct and ordered")
    interpretation = "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
    if (manifest["evaluation_sessions"] != sessions or payload["evaluation_sessions"] != sessions
            or [item["session"] for item in history] != sessions
            or manifest["interpretation"] != interpretation or payload["interpretation"] != interpretation
            or manifest["reconstruction_at"] != payload["generated_at"]
            or payload["reconstruction_at"] != payload["generated_at"]):
        raise ValueError("Strategy reconstruction session/clock/provenance drift")
    for item in history:
        own, data = item["source_manifest"], item["payload"]
        _validate_release(own, data)
        expected_market_time = (datetime.fromisoformat(payload["market_time"]) if interval == "1h" and item is history[-1]
                    else calendar.session_close(item["session"]).to_pydatetime())
        if (own["version"] != (RECONSTRUCTED_VERSION if interval == "1d" else HOURLY_RECONSTRUCTED_VERSION) or own["session"] != item["session"]
                or data["session"] != item["session"] or own["tickers"] != manifest["tickers"]
                or own["references"] != manifest["references"] or own["cohort_id"] != manifest["cohort_id"]
                or own["generated_at"] != payload["generated_at"] or data["generated_at"] != payload["generated_at"]
                or datetime.fromisoformat(data["market_time"]) != expected_market_time):
            raise ValueError("Strategy reconstructed member/session/window drift")
    if (payload["persistence"] != build_strategy_persistence(history, manifest["tickers"], sessions, interval=interval)
            or any(history[-1]["payload"][key] != payload[key] for key in ("scanners", "analysis", "setup_summary", "overview"))):
        raise ValueError("Strategy reconstructed recurrence/last-session parity drift")


def _validate_rolling_snapshot(manifest, payload):
    interval = manifest.get("interval", "1d")
    versions = {STRATEGY_VERSION, RECONSTRUCTED_VERSION} if interval == "1d" else {HOURLY_VERSION, HOURLY_RECONSTRUCTED_VERSION}
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [str(session.date()) for session in calendar.sessions_in_range(
        calendar.session_offset(payload["session"], -4), payload["session"])]
    history = manifest["history"]
    observed = [item["session"] for item in history]
    provenance = {item["session"]: "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
        if "RECONSTRUCTED" in item["source_manifest"]["version"] else "STORED_EVALUATION" for item in history}
    if (not history or len(history) > 5 or observed != sorted(set(observed)) or observed[-1] != sessions[-1]
            or set(observed) - set(sessions) or manifest["evaluation_sessions"] != sessions
            or payload["evaluation_sessions"] != sessions or manifest["observed_sessions"] != observed
            or payload["observed_sessions"] != observed or manifest["session_provenance"] != provenance
            or payload["session_provenance"] != provenance
            or manifest["interpretation"] != "STORED_SESSION_OUTCOMES_WITH_EXPLICIT_PROVENANCE"
            or payload["interpretation"] != manifest["interpretation"]):
        raise ValueError("Rolling strategy session/coverage/provenance drift")
    for item in history:
        own, data = item["source_manifest"], item["payload"]
        _validate_release(own, data)
        expected = _utc(datetime.fromisoformat(payload["market_time"]), "rolling cutoff") if item is history[-1] else calendar.session_close(item["session"]).to_pydatetime()
        market = _utc(datetime.fromisoformat(data["market_time"]), "retained evaluation cutoff")
        if (own["version"] not in versions or own.get("interval", "1d") != interval
                or own["session"] != item["session"] or data["session"] != item["session"]
                or market != expected
                or _utc(datetime.fromisoformat(data["generated_at"]), "retained observation") > _utc(datetime.fromisoformat(payload["generated_at"]), "rolling observation")
                or own["generated_at"] != data["generated_at"] or own["market_time"] != data["market_time"]
                or own["cohort_id"] != manifest["cohort_id"] or own["provider_tier"] != manifest["provider_tier"]
                or own["source_manifest_sha256"] != manifest["source_manifest_sha256"]
                or own["tickers"] != manifest["tickers"] or own["scanner_basis"] != manifest["scanner_basis"]
                or any(strategy_reference_identity(reference) != strategy_reference_identity(current)
                    for reference, current in zip(own["references"], manifest["references"]))):
            raise ValueError("Rolling strategy retained evaluation identity/clock drift")
    latest = history[-1]
    if (any(latest["payload"][key] != payload[key] for key in ("scanners", "analysis", "setup_summary", "overview", "market_time"))
            or manifest.get("evaluation_generated_at", manifest["generated_at"]) != latest["source_manifest"]["generated_at"]
            or payload.get("evaluation_generated_at", payload["generated_at"]) != latest["payload"]["generated_at"]
            or any(latest["source_manifest"][key] != manifest[key] for key in ("references", "source_windows", "receipts", "evidence_refs"))
            or payload["persistence"] != build_strategy_persistence(history, manifest["tickers"], sessions, interval=interval)):
        raise ValueError("Rolling strategy stored outcome/recurrence parity drift")


def _validate_release(manifest, payload):
    interval = manifest.get("interval", "1d")
    evaluation_generated_at = payload.get("evaluation_generated_at", payload["generated_at"])
    if (("evaluation_generated_at" in manifest or "evaluation_generated_at" in payload)
            and (manifest["version"] not in {ROLLING_VERSION, HOURLY_ROLLING_VERSION}
                or manifest.get("evaluation_generated_at") != payload.get("evaluation_generated_at")
                or _utc(datetime.fromisoformat(evaluation_generated_at), "strategy evaluation observation")
                    > _utc(datetime.fromisoformat(payload["generated_at"]), "strategy publication"))):
        raise ValueError("Strategy evaluation/publication clock drift")
    if interval == "1h":
        from .hourly_strategy_projection import session_hourly_slots
        market_time = _utc(datetime.fromisoformat(payload["market_time"]), "hourly strategy market time")
        slot = next((slot for slot in session_hourly_slots(exchange_calendars.get_calendar("XNYS"), payload["session"])
                     if slot[1] == market_time), None)
        if slot is None or manifest["slot_minutes"] != int((slot[1] - slot[0]).total_seconds() / 60):
            raise ValueError("Hourly strategy finalized slot/partial-close drift")
    basis = SCANNER_BASIS if interval == "1d" else HOURLY_BASIS
    minimum = DAILY_SCANNER_MINIMUM_BARS if interval == "1d" else HOURLY_SCANNER_MINIMUM_BARS
    tickers = manifest["tickers"]
    unavailable = manifest.get("unavailable_members", {})
    if interval == "1h" and (set(manifest["split_coverage_refs"]) - set(manifest["tickers"])
            or any(not manifest["split_coverage_refs"].get(ticker) for ticker in manifest["source_windows"] if ticker not in unavailable)):
        raise ValueError("Hourly strategy retained action coverage is unavailable")
    if set(unavailable) - set(tickers):
        raise ValueError("Strategy unavailable members extend beyond the exact cohort")
    references = {row["ticker"]: row for row in manifest["references"]}
    action_context = manifest.get("action_context", {})
    policy = "DAILY_STRATEGY_ACTION_CONTEXT_V1" if interval == "1d" else "HOURLY_STRATEGY_ACTION_CONTEXT_V1"
    if (set(action_context) - set(tickers) or (action_context and manifest.get("action_policy") != policy)):
        raise ValueError("Strategy action context scope/policy drift")
    for ticker, context in action_context.items():
        refs = context["action_refs"]
        excluded = any(row["handling"] == "CORPORATE_ACTION_REVIEW" for row in refs)
        if (not refs or context["reason"] != ("CORPORATE_ACTION_REVIEW" if excluded else None)
                or (unavailable.get(ticker) != context if excluded else ticker in unavailable and interval == "1d")
                or any(row["security_id"] != str(references[ticker]["security_id"])
                    or row["action_type"] == "SPLIT" and interval == "1d" or row["effective_date"] > payload["session"]
                    or len(row["payload_sha256"]) != 64 or row["handling"] not in {
                        "CORPORATE_ACTION_REVIEW", "RECURRING_CASH_DIVIDEND_PRICE_SERIES_UNCHANGED"} for row in refs)):
            raise ValueError("Strategy member action evidence drift")
    if set(unavailable) - set(action_context) and interval == "1d":
        raise ValueError("Strategy excluded member requires pinned action context")
    if (not tickers or tickers != sorted(set(tickers))
            or [row["ticker"] for row in manifest["references"]] != tickers
            or manifest["expected_members"] != len(tickers)
            or manifest["scanner_basis"] != basis or payload["scanner_basis"] != basis
            or set(manifest["scanner_contracts"]) != set(minimum)
            or set(payload["scanners"]) != set(manifest["scanner_contracts"])
            or set(payload["analysis"]) != set(manifest["scanner_contracts"])
            or set(manifest["receipts"]) != set(tickers) or set(manifest["evidence_refs"]) != set(tickers)
            or payload["provider_fetch_performed"] is not False or payload["execution_permission"] is not False):
        raise ValueError("Strategy release membership/basis/contract drift")
    for contract, required in minimum.items():
        daily_contract = next((key for key, value in HOURLY_SCANNER_CONTRACTS.items() if value == contract), contract)
        scanner, analysis = payload["scanners"][contract], payload["analysis"][contract]
        rows = scanner["results"]
        if (scanner["scan_datetime"] != evaluation_generated_at or scanner["interval"] != interval
                or scanner["total_scanned"] != len(tickers) or scanner["total_signals"] != len(rows)
                or any(row["ticker"] not in tickers for row in rows)
                or [row["ticker"] for row in analysis["members"]] != tickers):
            raise ValueError("Strategy scanner identity/count/clock drift")
        evaluated = 0
        for member in analysis["members"]:
            ticker, count = member["ticker"], member["available_bars"]
            context = action_context.get(ticker)
            if member.get("corporate_actions", []) != (context["action_refs"] if context else []):
                raise ValueError("Strategy analysis action context drift")
            matches = [row for row in rows if row["ticker"] == ticker]
            status = ("UNAVAILABLE" if count == 0 else "INSUFFICIENT_HISTORY" if count < required
                      else "COMPLETE" if matches else "COMPLETE_EMPTY")
            prefix = "DAILY" if interval == "1d" else "HOURLY"
            reason = f"{prefix}_INPUT_MISSING" if count == 0 else f"{prefix}_WARMUP_NOT_MET" if count < required else None
            if ticker in unavailable:
                status, reason = "UNAVAILABLE", unavailable[ticker]["reason"]
                if count != unavailable[ticker]["available_bars"] or matches:
                    raise ValueError("Strategy excluded member history/result drift")
            if (type(count) is not int or count < 0 or member["required_bars"] != required
                    or member["status"] != status or member["reason"] != reason
                    or member["total_signals"] != len(matches) or count < required and matches):
                raise ValueError("Strategy warm-up/analysis drift")
            evaluated += status in {"COMPLETE", "COMPLETE_EMPTY"}
            receipt = manifest["receipts"][ticker].get(contract)
            ref = manifest["evidence_refs"][ticker].get(contract)
            window = manifest["source_windows"].get(ticker)
            if count == 0 or ticker in unavailable:
                if receipt is not None or ref is not None or window is not None:
                    raise ValueError("Unavailable strategy input cannot fabricate an evidence reference")
            elif (receipt is None or ref is None or window is None
                    or receipt["analysis"] != member or receipt["contract"] != contract
                    or receipt["results"] != matches or receipt["scanner_basis"] != basis
                    or receipt["input_sha256"] != window["input_sha256"]
                    or len(window["primary_bar_revision_ids"]) != count
                    or len(set(window["primary_bar_revision_ids"])) != count
                    or ref["cohort_id"] != manifest["cohort_id"]
                    or ref["evidence_type"] != EvidenceType.SCANNER_RESULT.value
                    or ref["market_time"] != payload["market_time"]
                    or ref["payload_sha256"] != canonical_sha256(receipt)):
                raise ValueError("Strategy receipt/row/window parity drift")
            if receipt is not None and manifest["version"] in {RECONSTRUCTED_VERSION, HOURLY_RECONSTRUCTED_VERSION} and (
                    receipt.get("interpretation") != "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
                    or receipt.get("reconstruction_at") != payload["generated_at"]):
                raise ValueError("Strategy reconstructed receipt provenance drift")
        status = ("PARTIAL" if 0 < evaluated < len(tickers) else "COMPLETE" if evaluated and rows
                  else "COMPLETE_EMPTY" if evaluated else "INSUFFICIENT_HISTORY"
                  if any(row["status"] == "INSUFFICIENT_HISTORY" for row in analysis["members"]) else "UNAVAILABLE")
        if (analysis["total_evaluated"] != evaluated or analysis["total_unavailable"] != len(tickers) - evaluated
                or analysis["status"] != status):
            raise ValueError("Strategy analysis coverage drift")
        if (daily_contract == "SCAN_FVG_1D_50" and scanner["lookback"] != 50
            or daily_contract == "SCAN_MA_1D_9_21" and (scanner["short_period"], scanner["long_period"]) != (9, 21)
            or daily_contract == "SCAN_FIBONACCI_1D_5" and (scanner["min_swing_pct"] != 5.0 if interval == "1d"
                else scanner["min_swing_pct"] is not None or scanner["swing_policy"] != HOURLY_BASIS["fibonacci"])):
            raise ValueError("Strategy default drift")
        discriminator = {"SCAN_GAPS_1D": "gap_type", "SCAN_FVG_1D_50": "fvg_type",
                         "SCAN_MA_1D_9_21": "signal"}.get(daily_contract)
        if discriminator:
            groups = scanner["results_by_signal" if discriminator == "signal" else "results_by_type"]
            if (set(groups) != {row[discriminator] for row in rows} or any(
                    sorted(canonical_json(row) for row in group) != sorted(canonical_json(row) for row in rows
                    if row[discriminator] == label) for label, group in groups.items())):
                raise ValueError("Strategy stored group drift")
    summary = payload["setup_summary"]
    evidence = manifest["setup_evidence"]
    expected_tickers = [ticker for ticker in tickers if manifest["receipts"][ticker]]
    if (summary["interval"] != interval or summary["source"] != "MATERIALIZED_CURRENT_PROJECTION"
            or summary["computed_at"] != evaluation_generated_at or summary["market_time"] != payload["market_time"]
            or summary["count"] != len(expected_tickers)
            or [row["ticker"] for row in summary["results"]] != expected_tickers
            or [row["ticker"] for row in evidence] != expected_tickers
            or any(row["market_time"] != payload["market_time"] or row["observed_at"] != evaluation_generated_at
                   or canonical_sha256(row["payload"]) != row["payload_sha256"]
                   or setup_summary_row(row) != flattened for row, flattened in zip(evidence, summary["results"]))):
        raise ValueError("Strategy setup evidence/row parity drift")


def _validate_durable(cursor, manifest, *, known_by):
    interval = manifest.get("interval", "1d")
    if interval == "1h":
        coverage = [ref for refs in manifest["split_coverage_refs"].values() for ref in refs]
        cursor.execute("SELECT coverage_id,security_id,action_type,window_start,window_end,response_action_count, "
            "response_sha256,payload_sha256 FROM ref.corporate_action_coverage WHERE coverage_id=ANY(%s::uuid[]) AND received_at<=%s",
            ([ref["coverage_id"] for ref in coverage], known_by))
        stored = {str(row["coverage_id"]): split_coverage_ref(row) for row in cursor.fetchall()}
        if len(stored) != len(coverage) or any(stored.get(ref["coverage_id"]) != ref for ref in coverage):
            raise ValueError("Hourly strategy durable split coverage identity/hash drift")
    for ticker, context in manifest.get("action_context", {}).items():
        cursor.execute("SELECT action_id,security_id,ticker,action_type,effective_date,payload_sha256, "
            "cash_amount::text,ex_date,raw_payload FROM ref.corporate_action_revisions "
            "WHERE action_id=ANY(%s::uuid[]) AND observed_at<=%s AND received_at<=%s",
            ([row["action_id"] for row in context["action_refs"]], known_by, known_by))
        stored = {str(row["action_id"]): row for row in cursor.fetchall()}
        if len(stored) != len(context["action_refs"]) or any(
                ref["action_id"] not in stored or stored[ref["action_id"]]["ticker"] != ticker
                or strategy_action_context(stored[ref["action_id"]]) != ref for ref in context["action_refs"]):
            raise ValueError("Strategy durable corporate action identity/hash/metadata drift")
    for reference in manifest["references"]:
        ticker = reference["ticker"]
        refs = manifest["evidence_refs"][ticker]
        setup = next((row for row in manifest["setup_evidence"] if row["ticker"] == ticker), None)
        selected = list(refs.values())
        if setup:
            selected.append(dict(cohort_id=manifest["cohort_id"], evidence_id=setup["evidence_id"],
                evidence_type=EvidenceType.TRADE_SETUP.value, market_time=setup["market_time"], payload_sha256=setup["payload_sha256"]))
        exact = tuple(EvidenceRef(UUID(row["cohort_id"]), UUID(row["evidence_id"]), EvidenceType(row["evidence_type"]),
                                 datetime.fromisoformat(row["market_time"]), row["payload_sha256"]) for row in selected)
        resolve_release_evidence_refs_cursor(cursor, exact, security_id=UUID(str(reference["security_id"])), known_by=known_by)
        if not exact:
            continue
        cursor.execute(
            "SELECT evidence.*, source_window.primary_bar_revision_ids, source_window.confirmation_bar_revision_ids, "
            "source_window.source_window_sha256, source_window.security_id AS window_security_id, "
            "source_window.instrument_id AS window_instrument_id, source_window.provider_tier AS window_provider_tier, "
            "source_window.interval AS window_interval, source_window.market_time AS window_market_time, "
            "source_window.observed_at AS window_observed_at FROM rt.equity_evidence AS evidence "
            "JOIN rt.equity_evidence_source_windows AS source_window ON source_window.cohort_id=evidence.cohort_id "
            "AND source_window.source_window_id=evidence.source_window_id "
            "WHERE evidence.cohort_id=%s AND evidence.security_id=%s AND evidence.evidence_id=ANY(%s::uuid[]) "
            "AND evidence.observed_at<=%s AND evidence.durable_at<=%s AND source_window.durable_at<=%s",
            (manifest["cohort_id"], str(reference["security_id"]), [str(ref.evidence_id) for ref in exact], known_by, known_by, known_by))
        stored = {str(row["evidence_id"]): row for row in cursor.fetchall()}
        if len(stored) != len(exact):
            raise ValueError("Strategy durable receipt/source window is missing")
        window = manifest["source_windows"][ticker]
        for item in selected:
            row = stored[item["evidence_id"]]
            contract = next((key for key, ref in refs.items() if ref["evidence_id"] == item["evidence_id"]), None)
            receipt = manifest["receipts"][ticker][contract] if contract else setup["payload"]
            if (canonical_sha256(row["payload"]) != item["payload_sha256"] or row["payload"] != receipt
                    or row["ticker"] != ticker or row["interval"] != interval
                    or row["provider_tier"] != manifest["provider_tier"]
                    or row["instrument_id"] != reference["instrument_id"]
                    or str(row["window_security_id"]) != str(reference["security_id"])
                    or row["window_instrument_id"] != reference["instrument_id"]
                    or row["window_provider_tier"] != manifest["provider_tier"]
                    or row["window_interval"] != interval
                    or not _stored_clock_matches(row["window_market_time"], manifest["market_time"])
                    or not _stored_clock_matches(row["window_observed_at"], manifest["generated_at"])
                    or str(row["latest_bar_revision_id"]) != window["primary_bar_revision_ids"][-1]
                    or str(row["analysis_run_id"]) != setup["analysis_run_id"]
                    or str(row["security_revision_id"]) != str(reference["security_revision_id"])
                    or not _stored_clock_matches(row["observed_at"], manifest["generated_at"])
                    or row["source_version"] != (manifest["version"] if contract else setup["source_version"])
                    or row["source_name"] != (contract if contract else "EQUITY_SETUP")
                    or row["source_window_role"] != ("PRIMARY" if contract else None)
                    or row["source_window_sha256"] != window["input_sha256"]
                    or row["materialization_input_sha256"] != window["input_sha256"]
                    or [str(value) for value in row["primary_bar_revision_ids"]] != window["primary_bar_revision_ids"]
                    or [str(value) for value in row["confirmation_bar_revision_ids"]] != window["confirmation_bar_revision_ids"]):
                raise ValueError("Strategy immutable durable receipt identity/basis/window drift")
        cursor.execute(
            "SELECT dependency.dependency_order, dependency.dependency_evidence_id, evidence.payload, "
            "evidence.payload_sha256, evidence.materialization_input_sha256 FROM rt.equity_evidence_dependencies AS dependency "
            "JOIN rt.equity_evidence AS evidence ON evidence.cohort_id=dependency.cohort_id "
            "AND evidence.evidence_id=dependency.dependency_evidence_id WHERE dependency.cohort_id=%s "
            "AND dependency.evidence_id=%s AND evidence.security_id=%s AND evidence.observed_at<=%s "
            "AND evidence.durable_at<=%s ORDER BY dependency.dependency_order",
            (manifest["cohort_id"], setup["evidence_id"], str(reference["security_id"]), known_by, known_by))
        dependencies = cursor.fetchall()
        if ([str(row["dependency_evidence_id"]) for row in dependencies] != setup["dependency_evidence_ids"]
                or [row["dependency_order"] for row in dependencies] != list(range(len(dependencies)))
                or any(row["payload_sha256"] != setup["dependency_payload_sha256"][str(row["dependency_evidence_id"])]
                    or canonical_sha256(row["payload"]) != row["payload_sha256"]
                    or row["materialization_input_sha256"] != window["input_sha256"] for row in dependencies)):
            raise ValueError("Strategy setup durable dependency identity/order/hash drift")


def _reference_matches(reference, revisions):
    return (len(revisions) == 1
            and str(revisions[0]["revision_id"]) == str(reference["security_revision_id"])
            and revisions[0]["payload_sha256"] == reference["payload_sha256"])


class RealtimeDailyStrategyRepository(_RealtimeCompatibilityRepository):
    def __init__(self, cursor_factory, *, transaction_configured=False):
        super().__init__(cursor_factory, transaction_configured=transaction_configured)
        self._validation_guard = Lock()
        self._validated_releases = OrderedDict()

    def _validate_immutable_release(self, cursor, stored, *, cutoff, read_mode):
        manifest, payload = stored["source_manifest"], stored["payload"]
        # A session-window release is validated by the hashes of its own embedded
        # content; it never resolves a hot snapshot, bar or evidence row.
        self_contained = stored.get("snapshot_class") == "SESSION_CLOSE"

        def validate():
            _validate_strategy_snapshot(manifest, payload)
            if self_contained:
                return
            if manifest["version"] in {ROLLING_VERSION, HOURLY_ROLLING_VERSION}:
                _validate_rolling_sources(cursor, manifest, known_by=cutoff)
            for evaluation in _evaluation_manifests(manifest):
                _validate_durable(cursor, evaluation, known_by=cutoff)

        if read_mode != "retained" and not self_contained:
            validate()
            return
        key = (str(stored["cohort_id"]), str(stored["snapshot_id"]),
               stored["source_manifest_sha256"], stored["payload_sha256"])
        with self._validation_guard:
            entry = self._validated_releases.get(key)
            if entry is None:
                entry = dict(lock=Lock(), known_by=None)
                self._validated_releases[key] = entry
                if len(self._validated_releases) > STRATEGY_VALIDATION_CACHE_SIZE:
                    self._validated_releases.popitem(last=False)
            self._validated_releases.move_to_end(key)
        with entry["lock"]:
            if entry["known_by"] is not None and cutoff >= entry["known_by"]:
                dependencies = entry["dependencies"]
                if dependencies:
                    cursor.execute(
                        "SELECT dependency.evidence_id,COUNT(*) AS dependency_count "
                        "FROM rt.equity_evidence_dependencies AS dependency "
                        "JOIN rt.equity_evidence AS evidence ON evidence.cohort_id=dependency.cohort_id "
                        "AND evidence.evidence_id=dependency.dependency_evidence_id "
                        "WHERE dependency.cohort_id=%s AND dependency.evidence_id=ANY(%s::uuid[]) "
                        "AND evidence.observed_at<=%s AND evidence.durable_at<=%s GROUP BY dependency.evidence_id",
                        (str(stored["cohort_id"]), list(dependencies), cutoff, cutoff),
                    )
                    counts = {str(row["evidence_id"]): row["dependency_count"] for row in cursor.fetchall()}
                    if any(counts.get(identity, 0) != count for identity, count in dependencies.items()):
                        raise ValueError("Strategy cached dependency membership drift")
                return
            validate()
            entry["dependencies"] = {} if self_contained else {
                setup["evidence_id"]: len(setup["dependency_evidence_ids"])
                for evaluation in _evaluation_manifests(manifest) for setup in evaluation["setup_evidence"]}
            entry["known_by"] = cutoff

    def read(self, *, available_by, cohort_id, snapshot_id=None, strategy=None, ticker=None, view=None,
             evaluation_session=None, interval="1d", read_mode="current", session_date=None):
        cutoff = _utc(available_by, "available_by")
        if read_mode not in {"current", "retained"}:
            raise StrategyQueryRejected("Unsupported strategy read mode")
        if interval not in {"1d", "1h"}:
            raise StrategyQueryRejected("Strategy releases support only 1d/1h")
        versions = [STRATEGY_VERSION, HISTORY_VERSION, ROLLING_VERSION] if interval == "1d" else [HOURLY_VERSION, HOURLY_HISTORY_VERSION, HOURLY_ROLLING_VERSION]
        contracts = DAILY_SCANNER_CONTRACTS if interval == "1d" else {name: HOURLY_SCANNER_CONTRACTS[key] for name, key in DAILY_SCANNER_CONTRACTS.items()}
        if view is not None and (view not in {"evaluations", "persistence", "sessions"} or strategy is not None or ticker is not None):
            raise StrategyQueryRejected("Unsupported or mixed daily strategy view")
        if session_date is not None:
            try:
                selected = date.fromisoformat(session_date)
            except (TypeError, ValueError) as error:
                raise StrategyQueryRejected("Strategy session requires an ISO trading date") from error
            if str(selected) != session_date or not exchange_calendars.get_calendar("XNYS").is_session(session_date):
                raise StrategyQueryRejected("Strategy session must be an XNYS trading date")
            if ticker is not None or view == "sessions":
                raise StrategyQueryRejected("Strategy session cannot be mixed with ticker research or session listing")
        if cohort_id is None or strategy is not None and strategy not in DAILY_SCANNER_CONTRACTS:
            raise StrategyQueryRejected("Daily strategies require an explicit cohort and supported default strategy")
        if ticker is not None and snapshot_id is None:
            raise StrategyQueryRejected("Daily research requires an explicit release pin")
        if evaluation_session is not None and ticker is None:
            raise StrategyQueryRejected("Exact evaluation session requires pinned ticker research")
        with self._cursor() as cursor:
            self._begin(cursor)
            pinned = str(snapshot_id) if snapshot_id else None
            if pinned is None:
                release = self._release(cursor, cohort_id)
                entries = [entry for entry in (release.entry(version, STRATEGY_KEY) for version in versions)
                           if entry is not None] if release is not None else []
                if not entries:
                    raise StrategyPublicationUnavailable("No retained independent daily strategy release for this exact pin/cohort")
                pinned = str(min(entries, key=lambda entry: (
                    -datetime.fromisoformat(entry["generated_at"]).timestamp(),
                    -int(entry["source_generation"]), str(entry["snapshot_id"])))["snapshot_id"])
            cursor.execute(
                "SELECT snapshot.*, manifest.manifest_sha256 AS cohort_manifest_sha256, manifest.stock_count "
                "FROM " + SNAPSHOT_RELATION + " JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
                "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=ANY(%s) AND snapshot.snapshot_key=%s "
                "AND snapshot.generated_at<=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s AND manifest.created_at<=%s "
                "AND snapshot.snapshot_id=%s",
                (str(cohort_id), versions, STRATEGY_KEY, cutoff, cutoff, cutoff, cutoff, pinned))
            stored = cursor.fetchone()
            if stored is None or stored["payload"] is None:
                raise StrategyPublicationUnavailable("No retained independent daily strategy release for this exact pin/cohort")
            manifest, payload = stored["source_manifest"], stored["payload"]
            checks = dict(
                source_checksum=canonical_sha256(manifest) == stored["source_manifest_sha256"],
                payload_checksum=canonical_sha256(payload) == stored["payload_sha256"],
                cohort_manifest=manifest["source_manifest_sha256"] == stored["cohort_manifest_sha256"],
                provider_tier=manifest["provider_tier"] == stored["provider_tier"],
                source_generation=manifest["source_generation"] == stored["source_generation"],
                member_count=manifest["expected_members"] == stored["stock_count"],
                cohort=str(stored["cohort_id"]) == str(cohort_id) == manifest["cohort_id"],
                exact_pin=snapshot_id is None or str(stored["snapshot_id"]) == str(snapshot_id),
                payload_identity=all(manifest[key] == payload[key] for key in ("version", "cohort_id", "session", "market_time", "generated_at")),
                release_version=manifest["version"] in versions and manifest["version"] == stored["snapshot_type"],
                interval=manifest.get("interval", "1d") == interval,
                generated_clock=_stored_clock_matches(stored["generated_at"], manifest["generated_at"]),
            )
            failed = [name for name, matched in checks.items() if not matched]
            if failed:
                raise ValueError("Strategy publication identity/checksum/clock drift: " + ", ".join(failed))
            self._validate_immutable_release(cursor, stored, cutoff=cutoff, read_mode=read_mode)
            reference_current = True
            reference_cutoff = stored["generated_at"] if read_mode == "retained" else cutoff
            for reference in manifest["references"]:
                cursor.execute("SELECT revision_id, payload_sha256 FROM ref.security_revisions WHERE security_id=%s AND ticker=%s "
                    "AND effective_from<=%s AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                    "ORDER BY effective_from DESC, observed_at DESC, revision_id",
                    (str(reference["security_id"]), reference["ticker"], reference_cutoff, reference_cutoff, reference_cutoff))
                revisions = cursor.fetchall()
                if not _reference_matches(reference, revisions):
                    raise StrategyPublicationUnavailable("Strategy reference changed; new reviewed release required")
                if read_mode == "retained":
                    cursor.execute("SELECT revision_id, payload_sha256 FROM ref.security_revisions WHERE security_id=%s AND ticker=%s "
                        "AND effective_from<=%s AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
                        "ORDER BY effective_from DESC, observed_at DESC, revision_id",
                        (str(reference["security_id"]), reference["ticker"], cutoff, cutoff, cutoff))
                    reference_current = _reference_matches(reference, cursor.fetchall()) and reference_current
            ids = [str(row["security_id"]) for row in manifest["references"]]
            cursor.execute("SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions WHERE security_id=ANY(%s::uuid[]) "
                "AND received_at>%s AND received_at<=%s) OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
                "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) AS action_drift",
                (ids, stored["generated_at"], cutoff, ids, stored["generated_at"], cutoff))
            action_drift = cursor.fetchone()["action_drift"]
            if action_drift and read_mode != "retained":
                raise StrategyPublicationUnavailable("Strategy action evidence changed; new reviewed release required")
        release_manifest, release_payload = manifest, payload
        if session_date is not None:
            selected = next((item for item in manifest.get("history", []) if item["session"] == session_date), None)
            if selected is not None:
                manifest, payload = selected["source_manifest"], selected["payload"]
            elif payload["session"] != session_date:
                raise StrategyPublicationUnavailable("No stored strategy evaluation for the requested trading session")
        delay = timedelta(minutes=15 if stored["provider_tier"] == "MASSIVE_DEVELOPER" else 0)
        expected = latest_expected_market_time(cutoff - delay, interval)
        actual = _utc(datetime.fromisoformat(payload["market_time"]), "strategy market time")
        if actual > expected:
            raise StrategyPublicationUnavailable("Strategy release is ahead of the provider-qualified watermark")
        state = staleness_state(actual, expected, is_fresh=actual == expected)
        if stored["valid_until"] is not None and cutoff >= stored["valid_until"]:
            state = dict(state, status="EXPIRED")
        if read_mode == "retained" and (not reference_current or action_drift) and state["status"] == "READY":
            state = dict(state, status="STALE")
        publication = dict(cohort_id=str(cohort_id), snapshot_id=str(stored["snapshot_id"]), release_type=release_manifest["version"],
            payload_sha256=stored["payload_sha256"], strategy_ticker_available=True, status=state["status"],
            session=payload["session"], market_time=payload["market_time"], generated_at=release_payload["generated_at"],
            evaluation_generated_at=payload.get("evaluation_generated_at", payload["generated_at"]),
            read_at=cutoff.isoformat(), expected_market_time=expected.isoformat(), scope_note=payload["scope_note"],
            staleness_sessions=state["staleness_sessions"], max_serveable_stale_sessions=state["max_serveable_stale_sessions"],
            price_basis=payload["price_basis"], missing_producers=payload["missing_producers"])
        if read_mode == "retained":
            publication.update(read_mode="RETAINED", reference_status="CURRENT" if reference_current else "UNAVAILABLE",
                action_status="CHANGED" if action_drift else "CURRENT", execution_permission=False, provider_fetch_performed=False,
                current_data_available=reference_current and not action_drift and state["status"] == "READY")
        result = dict(publication=publication)
        if session_date is not None:
            publication["evaluation_session"] = session_date
        if interval == "1h":
            publication["interval"] = interval
        if view == "sessions":
            result.update(interval=interval, sessions=release_payload.get("observed_sessions",
                release_payload.get("evaluation_sessions", [release_payload["session"]])),
                data_as_of=release_payload["market_time"], data_interval=interval)
        elif view == "evaluations":
            result.update(scanner_basis=payload["scanner_basis"], members=manifest["tickers"],
                observed_sessions=[payload["session"]], recurrence_available=False,
                scanners=payload["scanners"], analysis=payload["analysis"], evidence_refs=manifest["evidence_refs"])
        elif view == "persistence":
            calendar = exchange_calendars.get_calendar("XNYS")
            sessions = [str(session.date()) for session in calendar.sessions_in_range(
                calendar.session_offset(payload["session"], -4), payload["session"])]
            history = [item for item in release_manifest.get("history", []) if item["session"] in sessions]
            selected_rolling = session_date is not None and release_manifest["version"] in {ROLLING_VERSION, HOURLY_ROLLING_VERSION}
            result.update(scanner_basis=payload["scanner_basis"], members=manifest["tickers"],
                evaluation_sessions=sessions, observed_sessions=[item["session"] for item in history] if selected_rolling
                    else payload.get("observed_sessions", payload.get("evaluation_sessions", [payload["session"]])),
                interpretation=release_payload.get("interpretation", "RETAINED_LATEST_EVALUATION_ONLY") if selected_rolling
                    else payload.get("interpretation", "RETAINED_LATEST_EVALUATION_ONLY"),
                session_provenance={session: provenance for session, provenance in release_payload.get("session_provenance", {}).items()
                    if session in sessions},
                reconstruction_at=release_payload.get("reconstruction_at"),
                persistence=build_strategy_persistence(history, manifest["tickers"], sessions, interval=interval)
                    if selected_rolling else payload.get("persistence"),
                scanners=payload["scanners"], analysis=payload["analysis"], evidence_refs=manifest["evidence_refs"])
        elif ticker is not None:
            ticker = ticker.upper()
            if ticker not in manifest["tickers"]:
                raise StrategyQueryRejected("Daily research ticker is outside this exact release")
            if evaluation_session is not None:
                item = next((item for item in manifest.get("history", []) if item["session"] == evaluation_session), None)
                if item is None:
                    raise StrategyQueryRejected("Requested evaluation session is not retained in this release")
                manifest, payload = item["source_manifest"], item["payload"]
                result.update(evaluation_session=evaluation_session,
                    evaluation_market_time=payload["market_time"],
                    interpretation="RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
                        if "RECONSTRUCTED" in manifest["version"] else "STORED_EVALUATION")
            if payload.get("interpretation"):
                result["interpretation"] = payload["interpretation"]
            names = dict(gaps="gaps", fvg="fvg", ma="ma-crossover", momentum="momentum-pullback",
                         bearish="bearish-bounce", fibonacci="fibonacci")
            result.update(ticker=ticker, cohort_count=len(manifest["tickers"]), scanner_basis=payload["scanner_basis"],
                overview=next((row for row in payload["overview"] if row["ticker"] == ticker), None),
                setup=next((row for row in payload["setup_summary"]["results"] if row["ticker"] == ticker), None),
                evidence_refs=manifest["evidence_refs"][ticker],
                analysis={name: next(row for row in payload["analysis"][contracts[contract]]["members"]
                    if row["ticker"] == ticker) for name, contract in names.items()},
                scans={name: [row for row in payload["scanners"][contracts[contract]]["results"]
                    if row["ticker"] == ticker] for name, contract in names.items()})
        elif strategy is not None:
            def scanner(contract):
                key = contracts[contract]
                return dict(payload["scanners"][key], publication=publication, scanner_basis=payload["scanner_basis"],
                            analysis=payload["analysis"][key])
            result["data"] = scanner(strategy)
            if strategy == "gaps":
                result["fvgData"] = scanner("fvg")
        return deepcopy(result)