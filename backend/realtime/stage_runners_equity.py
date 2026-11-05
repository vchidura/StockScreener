"""V1 compatibility and V2/V3 split runners for canonical equity input and portal projection.

The V1 EQUITY_EVIDENCE runner retains its combined bar-drain/projection EQUITY_CYCLE contract. V2/V3
EQUITY_MATERIALIZE drains bars, completes exact per-ticker 5m finalizer jobs and seals INPUT_READY;
EQUITY_PROJECTION consumes it and seals PORTAL_READY. Alerts can publish into the INPUT_READY release
without waiting for projection. PORTAL_PUBLISH requires PORTAL_READY as proof but merges its snapshots
into that same INPUT_READY release. A missing in-process projection is rebuilt from durable rows and
checked against PORTAL_READY before publication. No runner calls a provider.

Cancellation: every write goes through the stage's fenced transaction, so a stage the coordinator
abandoned cannot persist anything further (StageCancelled / WriteFenceRejected). The runners also check
`context.is_cancelled()` between phases (and the bar drain between jobs) to stop early without doing the work.
"""
from __future__ import annotations

import gc
import logging
from datetime import datetime, timedelta, timezone
from typing import NamedTuple
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars

from .database_access import RealtimeDatabaseRole
from .database_access_v2 import WriteFenceRejected
from .advanced_scheduler import (
    bind_advanced_input_gate_build_id,
    split_advanced_input_gate_build_id,
)
from .contracts import canonical_sha256
from .dashboard_repository import (
    compute_realtime_dashboard,
    persist_realtime_dashboard_cursor,
)
from .equity_bar_drain import EQUITY_BAR_MATERIALIZATION_FAILED, drain_equity_bar_jobs, lease_owner_for
from .equity_interval_finalizer import (
    FINALIZER_POLICY_SHA256,
    FiveMinuteFinalizerInputPending,
    enqueue_five_minute_finalizer_jobs_cursor,
    run_five_minute_finalizer_once,
)
from .equity_interval_derivation_worker import (
    enqueue_interval_derivation_jobs_cursor,
    run_interval_derivation_worker_once,
)
from .equity_worker_service import (
    EQUITY_INTERVAL_DERIVE_JOB,
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
)
from .lineage_head import inputs_sha256, reference_set
from .materialization_jobs import evaluate_job_barrier_cursor
from .orchestration_repository import CycleReceiptSpec
from .portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE,
    PortalReleaseReceipt,
    portal_session_date,
    publish_portal_release_cursor,
)
from .preflight import ProviderTier
from .runner_environment import RunnerEnvironment
from .screening_repository import _BAR_VISIBLE_AT, BarClocks
from .reference_continuity import (
    load_reference_continuity_rows_cursor,
)
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageCancelled, StageContext, StageOutcome, StageRunner
from .stage_runners_advanced_equity import advanced_input_gate_identity
from .ticker_repository import (
    CycleRehydrationMismatch,
    TickerProjectionCancelled,
    compute_realtime_ticker_cycle,
    persist_realtime_ticker_cycle_cursor,
    rehydrate_realtime_ticker_cycle,
)

logger = logging.getLogger(__name__)

EQUITY_CYCLE = "EQUITY_CYCLE"
INPUT_READY = "INPUT_READY"
PORTAL_READY = "PORTAL_READY"
REFERENCE_GENERATION_UNAVAILABLE = "REFERENCE_GENERATION_UNAVAILABLE"
EQUITY_CYCLE_RECEIPT_MISSING = "EQUITY_CYCLE_RECEIPT_MISSING"
INPUT_READY_RECEIPT_MISSING = "INPUT_READY_RECEIPT_MISSING"
INPUT_READY_REUSE_MISMATCH = "INPUT_READY_REUSE_MISMATCH"
INPUT_READY_PRODUCER_UNAVAILABLE = "INPUT_READY_PRODUCER_UNAVAILABLE"
PORTAL_READY_PRODUCER_UNAVAILABLE = "PORTAL_READY_PRODUCER_UNAVAILABLE"
ADVANCED_INPUT_GATE_DIGEST_CHANGED = "ADVANCED_INPUT_GATE_DIGEST_CHANGED"
CYCLE_REHYDRATION_MISMATCH = "CYCLE_REHYDRATION_MISMATCH"
PUBLICATION_RECEIPT_SCOPE_MISMATCH = "PUBLICATION_RECEIPT_SCOPE_MISMATCH"
STAGE_CANCELLED = "STAGE_CANCELLED"
FINALIZER_INPUT_PENDING = "FINALIZER_INPUT_PENDING"
FINALIZER_BARRIER_FAILED = "FINALIZER_BARRIER_FAILED"
DERIVATION_FAILED = "DERIVATION_FAILED"
CLOSING_DAILY_INPUTS_PENDING = "CLOSING_DAILY_INPUTS_PENDING"


def reference_generation(database: V2StageDatabase, cutoff) -> int:
    """Owner decision (2026-10-12): the generation every snapshot and receipt carries is the number
    of READY reference seed manifests created at or before the window cutoff. Monotonic, derived from
    the REFERENCE stage's own refreshes, and 0 (no READY seed) blocks the runner."""
    with database.transaction(read_only=True) as cursor:
        cursor.execute("SELECT count(*) FROM rt.seed_manifests WHERE status = 'READY' AND created_at <= %s",
                       (cutoff,))
        return int(cursor.fetchone()[0])


def _is_session_close(context: StageContext) -> bool:
    calendar = exchange_calendars.get_calendar("XNYS")
    close = calendar.session_close(str(context.plan.session_date)).to_pydatetime()
    return context.window.cutoff == close


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def input_ready_identity(
    database: V2StageDatabase,
    *,
    cohort_id,
    cutoff,
    known_by,
    reference_generation,
    advanced_input_gate_sha256: str | None = None,
    allow_late_identity_continuity: bool = False,
):
    with database.transaction(read_only=True) as cursor:
        cursor.execute(
            "SELECT cohort.cohort_sha256, cohort.configuration_sha256, cohort.model_bundle_sha256, "
            "cohort.valuation_sha256, manifest.manifest_id, manifest.manifest_sha256, "
            "manifest.universe_sha256, manifest.allocation_policy_sha256, manifest.stock_count, "
            "profile.profile_id, profile.profile_sha256, profile.config_sha256, profile.schema_sha256, "
            "profile.capabilities_sha256, profile.provider_tier, profile.security_policy_sha256 "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
            "JOIN rt.capability_profiles AS profile USING (profile_id) "
            "WHERE cohort.cohort_id=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
            "AND manifest.created_at<=%s AND profile.created_at<=%s",
            (str(cohort_id), cutoff, cutoff, cutoff, cutoff),
        )
        scope = cursor.fetchone()
        if scope is None:
            raise ValueError("INPUT_READY requires the exact cohort, manifest and profile")
        cursor.execute(
            "SELECT DISTINCT ON (instrument.instrument_id) "
            "instrument.instrument_id, reference.security_id, reference.ticker, "
            "reference.revision_id, reference.payload_sha256 "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument USING (instrument_id) "
            "JOIN ref.security_revisions AS reference USING (security_id) "
            "WHERE member.manifest_id=%s AND instrument.asset_class='EQUITY' AND member.channel='AM' "
            "AND reference.effective_from<=%s "
            "AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
            "AND reference.observed_at<=%s "
            "ORDER BY instrument.instrument_id,"
            "reference.observed_at DESC,reference.revision_id DESC",
            (str(scope[4]), cutoff, cutoff, cutoff),
        )
        members = tuple(cursor.fetchall())
        cursor.execute(
            "SELECT seed_id, seed_sha256, universe_sha256, source_generations "
            "FROM rt.seed_manifests WHERE status='READY' AND created_at<=%s "
            "ORDER BY created_at, seed_id",
            (
                known_by
                if allow_late_identity_continuity
                else cutoff,
            ),
        )
        seeds = tuple(cursor.fetchall())
        if scope[14] == "MASSIVE_ADVANCED":
            cursor.execute(
                "SELECT session_id, provider, feed, manifest_id, owner_id,"
                "owner_epoch,started_at,status,journal_generation,"
                "event_time_watermark "
                "FROM rt.stream_sessions WHERE manifest_id=%s AND started_at<=%s "
                "ORDER BY started_at, session_id",
                (str(scope[4]), known_by),
            )
            stream_sessions = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT consumer_name, journal_generation, shard_id, shard_map_sha256, owner_epoch, "
                "committed_offset,checkpoint_id,state_hash,"
                "convert_from(state_payload,'UTF8')::jsonb->>'stream_session_id',"
                "(convert_from(state_payload,'UTF8')::jsonb"
                "->>'event_time_watermark')::timestamptz "
                "FROM rt.consumer_checkpoints "
                "WHERE cohort_id=%s "
                "ORDER BY consumer_name, journal_generation, shard_id",
                (str(cohort_id),),
            )
            checkpoints = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT gap.gap_id,gap.session_id,gap.instrument_id,"
                "gap.channel,gap.gap_start,gap.gap_end,"
                "gap.journal_generation,gap.shard_id,"
                "gap.last_durable_offset,gap.reason,gap.recovery_status "
                "FROM rt.stream_gaps AS gap "
                "JOIN rt.stream_sessions AS gap_session "
                "ON gap_session.session_id=gap.session_id "
                "WHERE gap.session_id=ANY(%s::uuid[]) "
                "AND gap.gap_start<%s "
                "AND COALESCE("
                "gap.gap_end,gap_session.ended_at,%s)>%s "
                "AND gap.recovery_status NOT IN ('REPAIRED','WAIVED') "
                "ORDER BY gap.gap_start,gap.gap_id",
                (
                    [str(row[0]) for row in stream_sessions],
                    cutoff,
                    cutoff,
                    cutoff - timedelta(minutes=5),
                ),
            )
            gaps = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT DISTINCT ON ("
                "revision.instrument_id,revision.interval,"
                "revision.adjustment_basis,revision.session_scope"
                ") "
                "revision.instrument_id, revision.interval, revision.bar_revision_id, "
                "revision.payload_sha256, revision.bar_end, revision.journal_generation, "
                "revision.stream_session_id, revision.source_offset, revision.source_revision_ids, "
                "revision.adjustment_basis,revision.session_scope "
                "FROM rt.bar_revisions AS revision "
                "WHERE revision.cohort_id=%s "
                "AND revision.provider_tier='MASSIVE_ADVANCED' "
                "AND revision.interval IN ('1m','5m','15m','30m','1h','1d') "
                "AND revision.bar_end<=%s "
                "AND revision.created_at<=%s "
                "ORDER BY revision.instrument_id,revision.interval,"
                "revision.adjustment_basis,revision.session_scope,"
                "revision.bar_end DESC,revision.observed_at DESC,"
                "revision.bar_revision_id DESC",
                (str(cohort_id), cutoff, known_by),
            )
            bar_heads = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT instrument_id,last_complete_bar_end,"
                "last_examined_bar_end,terminal_state,reason_codes "
                "FROM rt.acquisition_watermarks "
                "WHERE cohort_id=%s AND interval='5m' "
                "AND source_kind='DERIVED' "
                "AND last_examined_bar_end>=%s "
                "ORDER BY instrument_id",
                (str(cohort_id), cutoff),
            )
            finalizer_progress = tuple(cursor.fetchall())
            sources = {
                "stream_sessions": stream_sessions,
                "checkpoints": checkpoints,
                "gaps": gaps,
                "bar_heads": bar_heads,
                "finalizer_progress": finalizer_progress,
            }
        else:
            cursor.execute(
                "SELECT watermark.instrument_id, watermark.interval, watermark.source_kind, "
                "watermark.last_complete_bar_end, watermark.overlap_policy, watermark.terminal_state, "
                "watermark.reason_codes, watermark.request_id, request.request_sha256, request.state, "
                "request.outcome, request.completeness, request.requested_from, request.requested_to, "
                "request.price_basis, batch.acquisition_batch_id, batch.batch_payload_sha256 "
                "FROM rt.acquisition_watermarks AS watermark "
                "LEFT JOIN rt.provider_request_ledger AS request USING (request_id) "
                "LEFT JOIN rt.acquisition_batches AS batch USING (request_id) "
                "WHERE watermark.cohort_id=%s AND watermark.last_complete_bar_end<=%s "
                "ORDER BY watermark.instrument_id, watermark.interval, watermark.source_kind, "
                "batch.acquisition_batch_id",
                (str(cohort_id), cutoff),
            )
            sources = tuple(cursor.fetchall())
    identity_continuity = ()
    if (
        scope[14] == "MASSIVE_DEVELOPER"
        and len(members) != scope[8]
        and allow_late_identity_continuity
    ):
        with database.transaction(
            read_only=True,
            dict_cursor=True,
        ) as cursor:
            continuity_rows, identity_continuity = (
                load_reference_continuity_rows_cursor(
                    cursor,
                    manifest_id=scope[4],
                    cutoff=cutoff,
                    known_by=known_by,
                )
            )
        members = tuple(
            (
                row["instrument_id"],
                row["security_id"],
                row["ticker"],
                row["revision_id"],
                row["payload_sha256"],
            )
            for row in continuity_rows
        )
    if len(members) != scope[8] or len({row[0] for row in members}) != len(members):
        raise ValueError("INPUT_READY requires the exact manifest members")
    if len(seeds) != reference_generation:
        raise ValueError("INPUT_READY reference generation count changed")
    member_instruments = {row[0] for row in members}
    if scope[14] == "MASSIVE_ADVANCED":
        if not _is_sha256(advanced_input_gate_sha256):
            raise ValueError(
                "INPUT_READY requires the exact Advanced input-gate digest"
            )
        sessions = sources["stream_sessions"]
        checkpoints = sources["checkpoints"]
        gaps = sources["gaps"]
        heads = sources["bar_heads"]
        finalizer_progress = sources["finalizer_progress"]
        if not sessions or not checkpoints:
            raise ValueError("INPUT_READY requires Advanced stream sessions and checkpoints")
        if gaps:
            raise ValueError("INPUT_READY has unresolved stream gaps")
        session_by_id = {
            str(row[0]): row
            for row in sessions
            if row[8] is not None
            and row[9] is not None
            and row[9] >= cutoff
        }
        checkpoint_coverage = {}
        covered_checkpoints = []
        for row in checkpoints:
            own_session = session_by_id.get(str(row[8]))
            if (
                own_session is None
                or str(row[1]) != str(own_session[8])
                or int(row[4]) != int(own_session[5])
                or row[9] is None
                or row[9] < cutoff
            ):
                continue
            covered_checkpoints.append(row)
            key = (str(row[1]), str(row[8]))
            checkpoint_coverage[key] = max(
                int(row[5]),
                checkpoint_coverage.get(key, 0),
            )
        if not checkpoint_coverage or any(
            row[1] == "1m"
            and (
                row[5] is None
                or row[6] is None
                or row[7] is None
                or int(row[7])
                > checkpoint_coverage.get(
                    (str(row[5]), str(row[6])),
                    -1,
                )
            )
            for row in heads
        ):
            raise ValueError("INPUT_READY Advanced bar heads drift from stream checkpoints")
        no_trade_instruments = {
            row[0]
            for row in finalizer_progress
            if row[1] is not None
            and row[1] >= cutoff
            and row[2] >= cutoff
            and row[3] == "COMPLETE"
            and "FINALIZER_NO_TRADE" in tuple(row[4] or ())
        }
        for interval in ("1m", "5m"):
            covered = {
                row[0] for row in heads if row[1] == interval
            } | no_trade_instruments
            if covered != member_instruments:
                raise ValueError(f"INPUT_READY requires exact Advanced {interval} finalizer heads")
        identity_sources = {
            "stream_sessions": tuple(
                (*row[:7], row[8])
                for row in stream_sessions
            ),
            "checkpoints": tuple(covered_checkpoints),
            "gaps": gaps,
            "bar_heads": heads,
            "finalizer_no_trade_instruments": tuple(
                sorted(no_trade_instruments)
            ),
        }
    else:
        if advanced_input_gate_sha256 is not None:
            raise ValueError(
                "Developer INPUT_READY cannot bind an Advanced input gate"
            )
        if {row[0] for row in sources} != member_instruments:
            raise ValueError("INPUT_READY requires one or more source watermarks for every exact manifest member")
        if any(
            row[5] == "GAP"
            or row[2] != "DERIVED" and (
                row[7] is None
                or row[9] != "COMPLETE"
                or row[10] != "OK"
                or row[11] != "COMPLETE"
                or row[15] is None
                or row[16] is None
            )
            for row in sources
        ):
            raise ValueError("INPUT_READY source coverage is incomplete or unsealed")
        identity_sources = sources
    universe_sha256 = scope[6]
    input_sha256 = canonical_sha256({
        "contract": (
            "advanced-stream-input-ready-v1"
            if scope[14] == "MASSIVE_ADVANCED"
            else "developer-rest-input-ready-v2"
        ),
        "cohort_id": cohort_id,
        "cutoff": cutoff,
        "known_by": known_by,
        "reference_generation": reference_generation,
        "scope": scope,
        "members": members,
        "identity_continuity": identity_continuity,
        "reference_seeds": seeds,
        "sources": identity_sources,
        "advanced_input_gate_sha256": advanced_input_gate_sha256,
    })
    return universe_sha256, input_sha256


def finalize_five_minute_inputs(
    database: V2StageDatabase,
    context: StageContext,
    *,
    build_id: str,
) -> tuple[str, tuple[str, ...]]:
    plan, window = context.plan, context.window
    if context.is_cancelled():
        raise StageCancelled("five-minute finalization was cancelled")
    with database.transaction(read_only=True) as cursor:
        cursor.execute(
            "SELECT instrument.instrument_id FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING (manifest_id) "
            "JOIN rt.instruments AS instrument USING (instrument_id) "
            "WHERE cohort.cohort_id=%s AND instrument.asset_class='EQUITY' "
            "AND member.channel='AM' ORDER BY instrument.instrument_id",
            (str(plan.cohort_id),),
        )
        instrument_ids = tuple(int(row[0]) for row in cursor.fetchall())
    if not instrument_ids:
        raise ValueError("five-minute finalization requires exact equity members")
    checked_at = max(context.started_at, window.cutoff).astimezone(timezone.utc)
    barrier_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:equity-5m-finalizer:{window.window_id}",
    )
    try:
        with database.transaction() as cursor:
            enqueue_five_minute_finalizer_jobs_cursor(
                cursor,
                cohort_id=plan.cohort_id,
                window_id=window.window_id,
                instrument_ids=instrument_ids,
                checked_at=checked_at,
                policy_sha256=FINALIZER_POLICY_SHA256,
                build_id=build_id,
                barrier_id=barrier_id,
            )
    except FiveMinuteFinalizerInputPending:
        return "PENDING", (FINALIZER_INPUT_PENDING,)

    config = EquityWorkerConfig(
        cohort_id=plan.cohort_id,
        job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
        lease_owner=lease_owner_for(context.stage_run_id),
        claim_limit=min(100, len(instrument_ids)),
    )
    reasons: list[str] = []
    for _ in range((len(instrument_ids) + config.claim_limit - 1) // config.claim_limit + 1):
        if context.is_cancelled():
            raise StageCancelled("five-minute finalization was cancelled")
        status = run_five_minute_finalizer_once(
            database,
            config,
            checked_at=checked_at,
        )
        reasons.extend(status.reason_codes)
        if status.claimed == 0:
            break
    if context.is_cancelled():
        raise StageCancelled("five-minute finalization was cancelled")
    with database.transaction() as cursor:
        verdict = evaluate_job_barrier_cursor(cursor, barrier_id)
    if verdict.state == "COMPLETE":
        return "COMPLETE", tuple(dict.fromkeys(reasons))
    if verdict.state == "OPEN":
        return "PENDING", (FINALIZER_INPUT_PENDING, *dict.fromkeys(reasons))
    return "FAILED", (FINALIZER_BARRIER_FAILED, *dict.fromkeys(reasons))


def _cohort_equity_instrument_ids(cursor, cohort_id) -> tuple[int, ...]:
    """Requires a dictionary cursor."""
    cursor.execute(
        "SELECT instrument.instrument_id "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_members AS member USING(manifest_id) "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "WHERE cohort.cohort_id=%s "
        "AND instrument.asset_class='EQUITY' "
        "AND member.channel='AM' ORDER BY instrument.instrument_id",
        (str(cohort_id),),
    )
    return tuple(
        int(row["instrument_id"])
        for row in cursor.fetchall()
    )


def closing_daily_inputs_visible(
    database: V2StageDatabase,
    context: StageContext,
    *,
    known_by,
) -> bool:
    """Whether every member's own-session daily bars are recorded by known_by.

    Screening reads them at the close, and a retry reuses the known_by of the
    first INPUT_READY. Issuing it earlier would leave the window unable to ever
    see them, since neither bar can exist when the close is first processed.
    """
    plan = context.plan
    with database.transaction(read_only=True, dict_cursor=True) as cursor:
        instrument_ids = _cohort_equity_instrument_ids(cursor, plan.cohort_id)
        cursor.execute(
            "SELECT count(DISTINCT (instrument_id,adjustment_basis)) AS visible "
            "FROM rt.bar_revisions "
            "WHERE cohort_id=%s AND provider_tier='MASSIVE_ADVANCED' "
            "AND instrument_id=ANY(%s::bigint[]) AND interval='1d' "
            "AND session_scope='RTH' AND session_date=%s AND is_final "
            "AND adjustment_basis IN ('ADJUSTED','RAW_UNADJUSTED') AND "
            + _BAR_VISIBLE_AT,
            (
                str(plan.cohort_id),
                list(instrument_ids),
                plan.session_date,
                *BarClocks(context.window.cutoff, known_by).params(),
            ),
        )
        return int(cursor.fetchone()["visible"]) == 2 * len(instrument_ids)


def finalize_higher_interval_inputs(
    database: V2StageDatabase,
    context: StageContext,
) -> tuple[str, tuple[str, ...]]:
    plan, window = context.plan, context.window
    if context.is_cancelled():
        raise StageCancelled("interval derivation was cancelled")
    with database.transaction(dict_cursor=True) as cursor:
        instrument_ids = _cohort_equity_instrument_ids(cursor, plan.cohort_id)
        # This stage just recorded the 5m bars, after it started. Reading them
        # at its start would leave the session's last bars underived for good,
        # because no window follows the close.
        cursor.execute("SELECT clock_timestamp() AS checked_at")
        checked_at = max(
            context.started_at,
            window.cutoff,
            cursor.fetchone()["checked_at"],
        )
        enqueue_interval_derivation_jobs_cursor(
            cursor,
            cohort_id=plan.cohort_id,
            instrument_ids=instrument_ids,
            session_date=plan.session_date,
            checked_at=checked_at,
        )
    config = EquityWorkerConfig(
        cohort_id=plan.cohort_id,
        job_kind=EQUITY_INTERVAL_DERIVE_JOB,
        lease_owner=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-derive:{context.stage_run_id}",
        ),
        claim_limit=100,
    )
    reasons = []
    failed = False
    for _ in range(100):
        if context.is_cancelled():
            raise StageCancelled("interval derivation was cancelled")
        status = run_interval_derivation_worker_once(
            database,
            config,
            checked_at=checked_at,
        )
        reasons.extend(status.reason_codes)
        if status.failed or status.blocked:
            failed = True
        if status.claimed == 0:
            break
    else:
        return "FAILED", (DERIVATION_FAILED, "DERIVATION_DRAIN_LIMIT")
    if failed:
        return "FAILED", (
            DERIVATION_FAILED,
            *dict.fromkeys(reasons),
        )
    return "COMPLETE", tuple(dict.fromkeys(reasons))


def equity_materialize_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        try:
            bars = drain_equity_bar_jobs(
                compute, cohort_id=plan.cohort_id, lease_owner=lease_owner_for(context.stage_run_id),
                should_stop=context.is_cancelled,
            )
            if bars.unsuccessful:
                return StageOutcome.failed(EQUITY_BAR_MATERIALIZATION_FAILED)
            generation = reference_generation(compute, window.cutoff)
            if generation < 1:
                return StageOutcome.blocked(REFERENCE_GENERATION_UNAVAILABLE)
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            finalizer_state, finalizer_reasons = finalize_five_minute_inputs(
                compute,
                context,
                build_id=environment.build_id,
            )
            if finalizer_state == "PENDING":
                return StageOutcome.blocked(*finalizer_reasons)
            if finalizer_state == "FAILED":
                return StageOutcome.failed(*finalizer_reasons)
            derivation_reasons = ()
            if (
                getattr(
                    context.window,
                    "provider_tier",
                    ProviderTier.MASSIVE_DEVELOPER.value,
                )
                == ProviderTier.MASSIVE_ADVANCED.value
            ):
                derivation_state, derivation_reasons = (
                    finalize_higher_interval_inputs(compute, context)
                )
                if derivation_state == "FAILED":
                    return StageOutcome.failed(*derivation_reasons)
            input_known_by = environment.known_by(window)
            if (
                getattr(
                    context.window,
                    "provider_tier",
                    ProviderTier.MASSIVE_DEVELOPER.value,
                )
                == ProviderTier.MASSIVE_ADVANCED.value
            ):
                if (
                    _is_session_close(context)
                    and not closing_daily_inputs_visible(
                        compute,
                        context,
                        known_by=input_known_by,
                    )
                ):
                    return StageOutcome.blocked(CLOSING_DAILY_INPUTS_PENDING)
            gate_digest = (
                None
                if getattr(
                    context.plan,
                    "stage_catalog_version",
                    "stages_v1",
                )
                != "stages_v3"
                else advanced_input_gate_identity(compute, context)
            )
            universe_sha256, input_sha256 = input_ready_identity(
                compute,
                cohort_id=plan.cohort_id,
                cutoff=window.cutoff,
                known_by=input_known_by,
                reference_generation=generation,
                advanced_input_gate_sha256=gate_digest,
            )
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        return StageOutcome.complete(degraded=(
            *bars.degraded_reasons(),
            *finalizer_reasons,
            *derivation_reasons,
        ), receipt=CycleReceiptSpec(
            INPUT_READY,
            window.cutoff,
            generation,
            universe_sha256=universe_sha256,
            input_sha256=input_sha256,
            build_id=(
                bind_advanced_input_gate_build_id(
                    environment.build_id,
                    gate_digest,
                )
                if gate_digest is not None
                else environment.build_id
            ),
            known_by=input_known_by,
        ))

    return run


def reuse_advanced_input_ready_runner(
    environment: RunnerEnvironment,
    *,
    receipt: CycleReceiptSpec,
    input_known_by,
    expected_gate_sha256: str,
) -> StageRunner:
    if receipt.receipt_kind != INPUT_READY:
        raise ValueError("Advanced input reuse requires an INPUT_READY receipt")
    if not _is_sha256(expected_gate_sha256):
        raise ValueError("Advanced input reuse requires an exact gate digest")

    def run(context: StageContext) -> StageOutcome:
        try:
            producer_build_id, gate_digest = (
                split_advanced_input_gate_build_id(receipt.build_id)
            )
        except ValueError:
            logger.exception(
                "Advanced INPUT_READY stored producer identity is invalid"
            )
            return StageOutcome.failed(INPUT_READY_REUSE_MISMATCH)
        if (
            receipt.cutoff != context.window.cutoff
            or receipt.known_by != input_known_by
            or input_known_by.tzinfo is None
            or input_known_by.utcoffset() is None
            or input_known_by < receipt.cutoff
            or gate_digest != expected_gate_sha256
        ):
            logger.error(
                "Advanced INPUT_READY stored recovery scope mismatch: "
                "cutoff=%s/%s known_by=%s gate=%s/%s",
                receipt.cutoff,
                context.window.cutoff,
                input_known_by,
                gate_digest,
                expected_gate_sha256,
            )
            return StageOutcome.failed(INPUT_READY_REUSE_MISMATCH)
        if producer_build_id != environment.build_id:
            logger.warning(
                "Advanced INPUT_READY producer %s is unavailable in build %s",
                producer_build_id,
                environment.build_id,
            )
            return StageOutcome.failed(
                INPUT_READY_PRODUCER_UNAVAILABLE
            )
        return StageOutcome.complete(
            receipt=receipt,
            evidence_ref=f"reused-input-ready:{receipt.input_sha256}",
        )

    return run


def reuse_advanced_portal_ready_runner(
    environment: RunnerEnvironment,
    *,
    receipt: CycleReceiptSpec,
) -> StageRunner:
    if receipt.receipt_kind != PORTAL_READY:
        raise ValueError("Advanced portal reuse requires a PORTAL_READY receipt")

    def run(context: StageContext) -> StageOutcome:
        if (
            receipt.cutoff != context.window.cutoff
            or receipt.build_id != environment.build_id
        ):
            return StageOutcome.failed(
                PORTAL_READY_PRODUCER_UNAVAILABLE
                if receipt.build_id != environment.build_id
                else INPUT_READY_REUSE_MISMATCH
            )
        return StageOutcome.complete(
            receipt=receipt,
            evidence_ref=f"reused-portal-ready:{receipt.input_sha256}",
        )

    return run


def equity_evidence_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        try:
            bars = drain_equity_bar_jobs(
                compute, cohort_id=plan.cohort_id, lease_owner=lease_owner_for(context.stage_run_id),
                should_stop=context.is_cancelled,
            )
            if bars.unsuccessful:
                logger.warning("equity bar materialization: %s job(s) failed for good", bars.unsuccessful)
                return StageOutcome.failed(EQUITY_BAR_MATERIALIZATION_FAILED)
            generation = reference_generation(compute, window.cutoff)
            if generation < 1:
                return StageOutcome.blocked(REFERENCE_GENERATION_UNAVAILABLE)
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            input_known_by = environment.known_by(window)
            cycle = compute_realtime_ticker_cycle(
                compute, state=environment.ticker_state, loader=environment.screening_loader,
                cohort_id=plan.cohort_id, session=plan.session_date,
                cutoff=window.cutoff, known_by=input_known_by,
                source_generation=generation,
                should_stop=context.is_cancelled,
            )
        except (
            StageCancelled,
            TickerProjectionCancelled,
            WriteFenceRejected,
        ):
            return StageOutcome.failed(STAGE_CANCELLED)
        if context.is_cancelled():
            return StageOutcome.failed(STAGE_CANCELLED)
        if environment.freeze_heap and not environment.heap_frozen:
            gc.collect()
            gc.freeze()
            environment.heap_frozen = True
        environment.handoff.put(plan.cohort_id, window.cutoff, cycle)
        return StageOutcome.complete(degraded=bars.degraded_reasons(), receipt=CycleReceiptSpec(
            EQUITY_CYCLE, window.cutoff, generation,
            universe_sha256=reference_set(cycle.inputs.members)["sha256"],
            input_sha256=inputs_sha256(cycle.inputs), build_id=environment.build_id,
            known_by=input_known_by,
        ))

    return run


class _WindowReceipt(NamedTuple):
    release: PortalReleaseReceipt
    reference_generation: int
    input_sha256: str
    known_by: datetime


def _window_cycle_receipt(
    database: V2StageDatabase,
    window,
    *,
    receipt_kind: str = EQUITY_CYCLE,
    receipt_window_id: UUID | None = None,
) -> _WindowReceipt | None:
    with database.transaction(read_only=True, dict_cursor=True) as cursor:
        cursor.execute(
            "SELECT receipt.receipt_id, receipt.cohort_id, receipt.cutoff, receipt.universe_sha256, "
            "receipt.reference_generation, receipt.input_sha256, "
            "window_run.provider_tier,receipt.known_by "
            "FROM rt.cycle_receipts AS receipt "
            "JOIN rt.window_runs AS window_run ON window_run.window_id = receipt.window_id "
            "WHERE receipt.window_id = %s AND receipt.receipt_kind = %s",
            (str(receipt_window_id or window.window_id), receipt_kind),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    if row["known_by"] is None:
        raise ValueError(
            f"{receipt_kind} receipt lacks its persisted visibility seal"
        )
    return _WindowReceipt(
        PortalReleaseReceipt(row["receipt_id"], row["cohort_id"], ProviderTier(row["provider_tier"]),
                             row["cutoff"], row["universe_sha256"],
                             source_receipt_sha256=row["input_sha256"],
                             source_generation=int(row["reference_generation"])),
        int(row["reference_generation"]), row["input_sha256"], row["known_by"],
    )


def _window_receipt(
    database: V2StageDatabase,
    window,
    *,
    receipt_kind: str = EQUITY_CYCLE,
) -> PortalReleaseReceipt | None:
    """Return the requested canonical release identity for alert or strategy work."""
    cycle_receipt = _window_cycle_receipt(database, window, receipt_kind=receipt_kind)
    return None if cycle_receipt is None else cycle_receipt.release


def equity_projection_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        receipt = _window_cycle_receipt(compute, window, receipt_kind=INPUT_READY)
        if receipt is None:
            return StageOutcome.failed(INPUT_READY_RECEIPT_MISSING)
        try:
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            cycle = compute_realtime_ticker_cycle(
                compute,
                state=environment.ticker_state,
                loader=environment.screening_loader,
                cohort_id=plan.cohort_id,
                session=plan.session_date,
                cutoff=window.cutoff,
                known_by=receipt.known_by,
                source_generation=receipt.reference_generation,
                allow_late_identity_continuity=(
                    environment.allow_late_identity_continuity
                ),
                should_stop=context.is_cancelled,
            )
        except (
            StageCancelled,
            TickerProjectionCancelled,
            WriteFenceRejected,
        ):
            return StageOutcome.failed(STAGE_CANCELLED)
        if context.is_cancelled():
            return StageOutcome.failed(STAGE_CANCELLED)
        if environment.freeze_heap and not environment.heap_frozen:
            gc.collect()
            gc.freeze()
            environment.heap_frozen = True
        environment.handoff.put(plan.cohort_id, window.cutoff, cycle)
        return StageOutcome.complete(receipt=CycleReceiptSpec(
            PORTAL_READY,
            window.cutoff,
            receipt.reference_generation,
            universe_sha256=reference_set(cycle.inputs.members)["sha256"],
            input_sha256=inputs_sha256(cycle.inputs),
            build_id=environment.build_id,
            known_by=receipt.known_by,
        ))

    return run


def publish_realtime_portal_cycle_atomic(
    publication: V2StageDatabase,
    cycle,
    dashboard,
    *,
    receipt: PortalReleaseReceipt,
    session_close: bool,
):
    with publication.transaction() as cursor:
        ticker_published, ticker_kept = persist_realtime_ticker_cycle_cursor(
            cursor,
            cycle,
            session_close=session_close,
        )
        dashboard_published = dashboard_kept = ()
        if dashboard is not None:
            dashboard_published, dashboard_kept = (
                persist_realtime_dashboard_cursor(
                    cursor,
                    dashboard,
                    session_close=session_close,
                )
            )
        publish_portal_release_cursor(
            cursor,
            receipt,
            (*ticker_published, *dashboard_published),
        )
        if session_close:
            publish_portal_release_cursor(
                cursor,
                receipt,
                (*ticker_kept, *dashboard_kept),
                release_class=RELEASE_CLASS_SESSION_CLOSE,
                session_date=portal_session_date(cycle.summary),
            )


def portal_publish_runner(
    environment: RunnerEnvironment,
    *,
    receipt_window_id: UUID | None = None,
) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        publication = V2StageDatabase(context, RealtimeDatabaseRole.PUBLICATION)
        try:
            is_split_graph = getattr(
                plan,
                "stage_catalog_version",
                "stages_v1",
            ) in {"stages_v2", "stages_v3"}
            projection_receipt = _window_cycle_receipt(
                publication,
                window,
                receipt_kind=(
                    PORTAL_READY if is_split_graph else EQUITY_CYCLE
                ),
                receipt_window_id=receipt_window_id,
            )
            if projection_receipt is None:
                return StageOutcome.failed(EQUITY_CYCLE_RECEIPT_MISSING)
            release_receipt = (
                _window_cycle_receipt(
                    publication,
                    window,
                    receipt_kind=INPUT_READY,
                    receipt_window_id=receipt_window_id,
                )
                if is_split_graph else projection_receipt
            )
            if release_receipt is None:
                return StageOutcome.failed(INPUT_READY_RECEIPT_MISSING)
            if (
                projection_receipt.release.cohort_id != plan.cohort_id
                or projection_receipt.release.cutoff != window.cutoff
                or projection_receipt.release.provider_tier.value
                != getattr(
                    window,
                    "provider_tier",
                    projection_receipt.release.provider_tier.value,
                )
                or release_receipt.release.cohort_id != plan.cohort_id
                or release_receipt.release.cutoff != window.cutoff
                or release_receipt.release.provider_tier
                is not projection_receipt.release.provider_tier
                or release_receipt.reference_generation
                != projection_receipt.reference_generation
                or release_receipt.known_by != projection_receipt.known_by
            ):
                return StageOutcome.failed(PUBLICATION_RECEIPT_SCOPE_MISMATCH)
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            cycle = environment.handoff.peek(plan.cohort_id, window.cutoff)
            if cycle is None:
                # The process restarted (or the bounded hand-off evicted the cycle) after EQUITY_EVIDENCE
                # committed: rebuild the cycle from durable rows, proven equal to the receipt.
                logger.warning("portal publish: cycle hand-off missing for window %s, rehydrating", window.window_id)
                try:
                    cycle = rehydrate_realtime_ticker_cycle(
                        publication, input_sha256=projection_receipt.input_sha256,
                        universe_sha256=projection_receipt.release.universe_sha256, cohort_id=plan.cohort_id,
                        session=plan.session_date, cutoff=window.cutoff,
                        known_by=projection_receipt.known_by,
                        source_generation=projection_receipt.reference_generation,
                        allow_late_identity_continuity=(
                            environment.allow_late_identity_continuity
                        ),
                        should_stop=context.is_cancelled,
                    )
                except CycleRehydrationMismatch:
                    logger.error("portal publish: the rehydrated cycle does not match the receipt of window %s",
                                 window.window_id)
                    return StageOutcome.failed(CYCLE_REHYDRATION_MISMATCH)
                except TickerProjectionCancelled:
                    return StageOutcome.failed(STAGE_CANCELLED)
                if context.is_cancelled():
                    return StageOutcome.failed(STAGE_CANCELLED)
            session_close = _is_session_close(context)
            dashboard = (
                compute_realtime_dashboard(
                    V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE),
                    cohort_id=plan.cohort_id,
                    session=plan.session_date,
                    cutoff=window.cutoff,
                    known_by=projection_receipt.known_by,
                    source_generation=projection_receipt.reference_generation,
                    allow_late_identity_continuity=(
                        environment.allow_late_identity_continuity
                    ),
                )
                if is_split_graph else None
            )
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            publish_realtime_portal_cycle_atomic(
                publication,
                cycle,
                dashboard,
                receipt=release_receipt.release,
                session_close=session_close,
            )
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        # Consumed only after publication succeeded, so a retried attempt can publish again.
        environment.handoff.discard(plan.cohort_id, window.cutoff)
        return StageOutcome.complete()

    return run
