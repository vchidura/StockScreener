"""EQUITY_EVIDENCE and PORTAL_PUBLISH runners: the ticker-workspace cycle split over two stages.

EQUITY_EVIDENCE (compute) first materializes the acquisition batches EQUITY_INGEST recorded into
rt.bar_* (a bounded drain, equity_bar_drain.py: only `compute` may write bars), then loads the cohort's
inputs, projects them incrementally and persists the evidence, and reports an EQUITY_CYCLE receipt.
PORTAL_PUBLISH (publication) is dispatched from that durable receipt and publishes the cycle's snapshots
and release. No provider is called by either.

Cancellation: every write goes through the stage's fenced transaction, so a stage the coordinator
abandoned cannot persist anything further (StageCancelled / WriteFenceRejected). The runners also check
`context.is_cancelled()` between phases (and the bar drain between jobs) to stop early without doing the work.
"""
from __future__ import annotations

import logging

import exchange_calendars

from .database_access import RealtimeDatabaseRole
from .database_access_v2 import WriteFenceRejected
from .equity_bar_drain import EQUITY_BAR_MATERIALIZATION_FAILED, drain_equity_bar_jobs, lease_owner_for
from .lineage_head import inputs_sha256, reference_set
from .orchestration_repository import CycleReceiptSpec
from .portal_projection_repository import PortalReleaseReceipt
from .preflight import ProviderTier
from .runner_environment import RunnerEnvironment
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageCancelled, StageContext, StageOutcome, StageRunner
from .ticker_repository import compute_realtime_ticker_cycle, publish_realtime_ticker_cycle

logger = logging.getLogger(__name__)

EQUITY_CYCLE = "EQUITY_CYCLE"
REFERENCE_GENERATION_UNAVAILABLE = "REFERENCE_GENERATION_UNAVAILABLE"
EQUITY_CYCLE_RECEIPT_MISSING = "EQUITY_CYCLE_RECEIPT_MISSING"
CYCLE_HANDOFF_UNAVAILABLE = "CYCLE_HANDOFF_UNAVAILABLE"
STAGE_CANCELLED = "STAGE_CANCELLED"


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
                cutoff=window.cutoff, known_by=window.deadline, source_generation=generation,
            )
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        environment.handoff.put(plan.cohort_id, window.cutoff, cycle)
        return StageOutcome.complete(degraded=bars.degraded_reasons(), receipt=CycleReceiptSpec(
            EQUITY_CYCLE, window.cutoff, generation,
            universe_sha256=reference_set(cycle.inputs.members)["sha256"],
            input_sha256=inputs_sha256(cycle.inputs), build_id=environment.build_id,
        ))

    return run


def _window_receipt(database: V2StageDatabase, window) -> PortalReleaseReceipt | None:
    with database.transaction(read_only=True, dict_cursor=True) as cursor:
        cursor.execute(
            "SELECT receipt.receipt_id, receipt.cohort_id, receipt.cutoff, receipt.universe_sha256, "
            "window_run.provider_tier FROM rt.cycle_receipts AS receipt "
            "JOIN rt.window_runs AS window_run ON window_run.window_id = receipt.window_id "
            "WHERE receipt.window_id = %s AND receipt.receipt_kind = %s", (str(window.window_id), EQUITY_CYCLE))
        row = cursor.fetchone()
    if row is None:
        return None
    return PortalReleaseReceipt(row["receipt_id"], row["cohort_id"], ProviderTier(row["provider_tier"]),
                                row["cutoff"], row["universe_sha256"])


def portal_publish_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        publication = V2StageDatabase(context, RealtimeDatabaseRole.PUBLICATION)
        try:
            receipt = _window_receipt(publication, window)
            if receipt is None:
                return StageOutcome.failed(EQUITY_CYCLE_RECEIPT_MISSING)
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            cycle = environment.handoff.peek(plan.cohort_id, window.cutoff)
            if cycle is None:
                return StageOutcome.failed(CYCLE_HANDOFF_UNAVAILABLE)
            publish_realtime_ticker_cycle(
                publication, cycle, receipt=receipt, session_close=_is_session_close(context),
            )
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        # Consumed only after publication succeeded, so a retried attempt can publish again.
        environment.handoff.discard(plan.cohort_id, window.cutoff)
        return StageOutcome.complete()

    return run
