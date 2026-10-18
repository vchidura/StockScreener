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
from datetime import datetime, timezone
from typing import NamedTuple
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars

from .database_access import RealtimeDatabaseRole
from .database_access_v2 import WriteFenceRejected
from .advanced_scheduler import bind_advanced_input_gate_build_id
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
from .reference_continuity import (
    load_reference_continuity_rows_cursor,
)
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageCancelled, StageContext, StageOutcome, StageRunner
from .stage_runners_advanced_equity import advanced_input_gate_identity
from .ticker_repository import (
    CycleRehydrationMismatch,
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
ADVANCED_INPUT_GATE_DIGEST_CHANGED = "ADVANCED_INPUT_GATE_DIGEST_CHANGED"
CYCLE_REHYDRATION_MISMATCH = "CYCLE_REHYDRATION_MISMATCH"
PUBLICATION_RECEIPT_SCOPE_MISMATCH = "PUBLICATION_RECEIPT_SCOPE_MISMATCH"
STAGE_CANCELLED = "STAGE_CANCELLED"
FINALIZER_INPUT_PENDING = "FINALIZER_INPUT_PENDING"
FINALIZER_BARRIER_FAILED = "FINALIZER_BARRIER_FAILED"
DERIVATION_FAILED = "DERIVATION_FAILED"


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
            "SELECT instrument.instrument_id, reference.security_id, reference.ticker, "
            "reference.revision_id, reference.payload_sha256 "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument USING (instrument_id) "
            "JOIN ref.security_revisions AS reference USING (security_id) "
            "WHERE member.manifest_id=%s AND instrument.asset_class='EQUITY' AND member.channel='AM' "
            "AND reference.effective_from<=%s "
            "AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
            "AND reference.observed_at<=%s ORDER BY instrument.instrument_id",
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
                "WHERE cohort_id=%s AND committed_at<=%s "
                "ORDER BY consumer_name, journal_generation, shard_id",
                (str(cohort_id), known_by),
            )
            checkpoints = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT gap_id, session_id, instrument_id, channel, gap_start, gap_end, journal_generation, "
                "shard_id, last_durable_offset, reason, recovery_status FROM rt.stream_gaps "
                "WHERE session_id=ANY(%s::uuid[]) AND gap_start<=%s "
                "AND recovery_status NOT IN ('REPAIRED','WAIVED') ORDER BY gap_start, gap_id",
                ([str(row[0]) for row in stream_sessions], cutoff),
            )
            gaps = tuple(cursor.fetchall())
            cursor.execute(
                "SELECT DISTINCT ON (current.instrument_id, current.interval) "
                "current.instrument_id, current.interval, revision.bar_revision_id, "
                "revision.payload_sha256, revision.bar_end, revision.journal_generation, "
                "revision.stream_session_id, revision.source_offset, revision.source_revision_ids "
                "FROM rt.bar_current AS current "
                "JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=current.bar_revision_id "
                "AND revision.observed_at=current.observed_at "
                "WHERE current.cohort_id=%s AND current.provider_tier='MASSIVE_ADVANCED' "
                "AND current.interval IN ('1m','5m','15m','30m','1h','1d') "
                "AND revision.bar_end<=%s "
                "AND revision.created_at<=%s ORDER BY current.instrument_id, current.interval, "
                "revision.bar_end DESC, revision.observed_at DESC",
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
        "sources": sources,
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
        status = run_five_minute_finalizer_once(
            database,
            config,
            checked_at=checked_at,
        )
        reasons.extend(status.reason_codes)
        if status.claimed == 0:
            break
    with database.transaction() as cursor:
        verdict = evaluate_job_barrier_cursor(cursor, barrier_id)
    if verdict.state == "COMPLETE":
        return "COMPLETE", tuple(dict.fromkeys(reasons))
    if verdict.state == "OPEN":
        return "PENDING", (FINALIZER_INPUT_PENDING, *dict.fromkeys(reasons))
    return "FAILED", (FINALIZER_BARRIER_FAILED, *dict.fromkeys(reasons))


def finalize_higher_interval_inputs(
    database: V2StageDatabase,
    context: StageContext,
) -> tuple[str, tuple[str, ...]]:
    plan, window = context.plan, context.window
    with database.transaction(dict_cursor=True) as cursor:
        cursor.execute(
            "SELECT instrument.instrument_id "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "WHERE cohort.cohort_id=%s "
            "AND instrument.asset_class='EQUITY' "
            "AND member.channel='AM' ORDER BY instrument.instrument_id",
            (str(plan.cohort_id),),
        )
        instrument_ids = tuple(
            int(row["instrument_id"])
            for row in cursor.fetchall()
        )
        enqueue_interval_derivation_jobs_cursor(
            cursor,
            cohort_id=plan.cohort_id,
            instrument_ids=instrument_ids,
            session_date=plan.session_date,
            checked_at=max(context.started_at, window.cutoff),
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
        status = run_interval_derivation_worker_once(
            database,
            config,
            checked_at=max(context.started_at, window.cutoff),
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
                known_by=environment.known_by(window),
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
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        try:
            gate_digest = advanced_input_gate_identity(compute, context)
            if gate_digest != expected_gate_sha256:
                return StageOutcome.failed(
                    ADVANCED_INPUT_GATE_DIGEST_CHANGED
                )
            universe_sha256, input_sha256 = input_ready_identity(
                compute,
                cohort_id=context.plan.cohort_id,
                cutoff=context.window.cutoff,
                known_by=input_known_by,
                reference_generation=receipt.reference_generation,
                advanced_input_gate_sha256=gate_digest,
            )
        except ValueError:
            return StageOutcome.failed(INPUT_READY_REUSE_MISMATCH)
        if (
            receipt.cutoff != context.window.cutoff
            or receipt.universe_sha256 != universe_sha256
            or receipt.input_sha256 != input_sha256
            or receipt.build_id
            != bind_advanced_input_gate_build_id(
                environment.build_id,
                gate_digest,
            )
        ):
            return StageOutcome.failed(INPUT_READY_REUSE_MISMATCH)
        return StageOutcome.complete(
            receipt=receipt,
            evidence_ref=f"reused-input-ready:{input_sha256}",
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
            cycle = compute_realtime_ticker_cycle(
                compute, state=environment.ticker_state, loader=environment.screening_loader,
                cohort_id=plan.cohort_id, session=plan.session_date,
                cutoff=window.cutoff, known_by=environment.known_by(window),
                source_generation=generation,
            )
        except (StageCancelled, WriteFenceRejected):
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
            "window_run.provider_tier, window_run.deadline FROM rt.cycle_receipts AS receipt "
            "JOIN rt.window_runs AS window_run ON window_run.window_id = receipt.window_id "
            "WHERE receipt.window_id = %s AND receipt.receipt_kind = %s",
            (str(receipt_window_id or window.window_id), receipt_kind),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return _WindowReceipt(
        PortalReleaseReceipt(row["receipt_id"], row["cohort_id"], ProviderTier(row["provider_tier"]),
                             row["cutoff"], row["universe_sha256"]),
        int(row["reference_generation"]), row["input_sha256"], row["deadline"],
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
                known_by=environment.known_by(window),
                source_generation=receipt.reference_generation,
                allow_late_identity_continuity=(
                    environment.allow_late_identity_continuity
                ),
            )
        except (StageCancelled, WriteFenceRejected):
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
                    )
                except CycleRehydrationMismatch:
                    logger.error("portal publish: the rehydrated cycle does not match the receipt of window %s",
                                 window.window_id)
                    return StageOutcome.failed(CYCLE_REHYDRATION_MISMATCH)
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
