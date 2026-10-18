"""STOCK_ALERTS and STRATEGY_PUBLISH runners dispatched from the canonical input receipt.

STOCK_ALERTS computes the alert snapshot read-only under the compute role and publishes it under the
publication role (the V2 registry lets only `publication` insert portal snapshots). STRATEGY_PUBLISH
runs the ported strategy producer once per interval; the producer already writes its evidence under
compute and its snapshots and releases under publication. No provider is called by either.

Cancellation follows the equity runners: every write goes through the stage's fenced transaction
(StageCancelled / WriteFenceRejected), and `context.is_cancelled()` is polled between phases.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping

from .database_access import RealtimeDatabaseRole
from .database_access_v2 import WriteFenceRejected
from .runner_environment import RunnerEnvironment
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageCancelled, StageContext, StageOutcome, StageRunner
from .stage_runners_equity import (
    EQUITY_CYCLE,
    EQUITY_CYCLE_RECEIPT_MISSING,
    INPUT_READY,
    INPUT_READY_RECEIPT_MISSING,
    REFERENCE_GENERATION_UNAVAILABLE,
    STAGE_CANCELLED,
    _is_session_close,
    _window_receipt,
    reference_generation,
)
from .stock_alert_repository import (
    RetainedForwardState,
    compute_realtime_stock_alerts,
    publish_realtime_stock_alerts,
)
from .strategy_repository import materialize_due_realtime_strategies

logger = logging.getLogger(__name__)

STOCK_ALERTS_RETAINED_FORWARD_STATE = "STOCK_ALERTS_RETAINED_FORWARD_STATE"
STRATEGY_INTERVALS = ("1d", "1h")
# Analysis coverage statuses of a strategy snapshot that mean its members were evaluated (an evaluation without
# any setup is COMPLETE_EMPTY); everything else (PARTIAL, INSUFFICIENT_HISTORY, UNAVAILABLE) degrades the stage.
EVALUATED_ANALYSIS_STATUSES = frozenset({"COMPLETE", "COMPLETE_EMPTY"})


def _input_receipt(database, plan, window):
    is_split_graph = getattr(
        plan,
        "stage_catalog_version",
        "stages_v1",
    ) in {"stages_v2", "stages_v3"}
    return (
        _window_receipt(
            database,
            window,
            receipt_kind=INPUT_READY if is_split_graph else EQUITY_CYCLE,
        ),
        (
            INPUT_READY_RECEIPT_MISSING
            if is_split_graph
            else EQUITY_CYCLE_RECEIPT_MISSING
        ),
    )


def _missing_producers(snapshot) -> tuple[str, ...]:
    payload = getattr(snapshot, "payload", None)
    missing = payload.get("missing_producers") if isinstance(payload, Mapping) else None
    if not isinstance(missing, Mapping):
        return ()
    return tuple(f"MISSING_PRODUCER:{name}" for name in sorted(missing))


def stock_alerts_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        publication = V2StageDatabase(context, RealtimeDatabaseRole.PUBLICATION)
        try:
            receipt, missing_reason = _input_receipt(compute, plan, window)
            if receipt is None:
                return StageOutcome.failed(missing_reason)
            generation = reference_generation(compute, window.cutoff)
            if generation < 1:
                return StageOutcome.blocked(REFERENCE_GENERATION_UNAVAILABLE)
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            try:
                snapshot = compute_realtime_stock_alerts(
                    compute, cohort_id=plan.cohort_id, session=plan.session_date, cutoff=window.cutoff,
                    known_by=environment.known_by(window), source_generation=generation,
                )
            except RetainedForwardState:
                return StageOutcome.blocked(STOCK_ALERTS_RETAINED_FORWARD_STATE)
            if context.is_cancelled():
                return StageOutcome.failed(STAGE_CANCELLED)
            publish_realtime_stock_alerts(
                publication, snapshot, receipt=receipt, session_close=_is_session_close(context),
            )
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        return StageOutcome.complete(degraded=_missing_producers(snapshot))

    return run


def strategy_publish_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        plan, window = context.plan, context.window
        compute = V2StageDatabase(context, RealtimeDatabaseRole.COMPUTE)
        publication = V2StageDatabase(context, RealtimeDatabaseRole.PUBLICATION)
        statuses: list[str] = []
        failures: list[str] = []
        unevaluated: list[str] = []
        try:
            receipt, missing_reason = _input_receipt(publication, plan, window)
            if receipt is None:
                return StageOutcome.failed(missing_reason)
            for interval in STRATEGY_INTERVALS:
                if context.is_cancelled():
                    return StageOutcome.failed(STAGE_CANCELLED)
                try:
                    # Strategies subtract the provider delay from this sealed input visibility clock.
                    outcomes = materialize_due_realtime_strategies(
                        compute, publication, cohort_id=plan.cohort_id,
                        available_by=environment.known_by(window),
                        intervals=(interval,), receipt=receipt,
                    )
                except (StageCancelled, WriteFenceRejected):
                    raise
                except Exception as error:  # noqa: BLE001 - one interval must not stop the other
                    logger.warning("strategy interval %s failed: %s", interval, type(error).__name__,
                                   exc_info=True)
                    failures.append(f"STRATEGY_{interval.upper()}_FAILED:{type(error).__name__}")
                    continue
                statuses.extend(f"{item['interval']}={item['status']}" for item in outcomes)
                unevaluated.extend(
                    f"STRATEGY_{interval.upper()}_{status}" for item in outcomes
                    for status in item.get("analysis", ()) if status not in EVALUATED_ANALYSIS_STATUSES)
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        if len(failures) == len(STRATEGY_INTERVALS):
            return StageOutcome.failed(*failures)
        # ALREADY_STORED (a retained cutoff) is a success. A failed interval degrades the stage, and so does a
        # snapshot whose members could not be (fully) evaluated: it is valid and published, but not what the
        # stage exists to produce, and the stage must not look healthy.
        return StageOutcome.complete(degraded=(*failures, *dict.fromkeys(unevaluated)),
                                     evidence_ref=";".join(statuses) or None)

    return run
