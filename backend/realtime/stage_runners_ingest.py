"""REFERENCE and EQUITY_INGEST runners: the two stages that call a provider in production.

Both get their source from the RunnerEnvironment and fail closed while it is not configured: the stage
is BLOCKED_POLICY and no provider call, client construction or SDK import happens here. REFERENCE runs
the reference seed service and persists the manifest under the references role. EQUITY_INGEST is the
equity half of the developer acquisition producer (the OPTIONS_ACQUIRE stage owns options): it collects
bars from the injected source, checks exact universe coverage and the producer's request/response
bounds, then records the provider ledger and persists the acquisition batches and watermarks under the
ingress role. Turning those batches into rt.bar_revisions needs the compute role, so it is the first
phase of the EQUITY_EVIDENCE runner (equity_bar_drain.py).

Every write goes through the stage's transaction (a window stage's is fenced: StageCancelled /
WriteFenceRejected), and `context.is_cancelled()` is polled between phases.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

from .acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
    build_developer_acquisition_batch,
    persist_developer_acquisition_batch_cursor,
)
from .acquisition_ledger import (
    RequestCompleteness,
    advance_acquisition_watermark_cursor,
    record_provider_request_cursor,
)
from .acquisition_producer import (
    MAX_DEVELOPER_CANARY_REQUESTS,
    MAX_DEVELOPER_CANARY_RESPONSE_BYTES,
    DeveloperSourceResult,
    developer_equity_watermarks,
)
from .contracts import UniverseManifest
from .database_access import RealtimeDatabaseRole
from .database_access_v2 import WriteFenceRejected
from .reference_repository import persist_reference_result_cursor
from .reference_seed import SeedStatus
from .reference_service import ProviderEntitlementUnavailable, ReferenceSeedRequest
from .runner_environment import RunnerEnvironment
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageCancelled, StageContext, StageOutcome, StageRunner
from .stage_runners_equity import STAGE_CANCELLED
from .worker_database import RealtimeProcessDatabase

REFERENCE_SOURCE_NOT_CONFIGURED = "REFERENCE_SOURCE_NOT_CONFIGURED"
EQUITY_SOURCE_NOT_CONFIGURED = "EQUITY_SOURCE_NOT_CONFIGURED"
UNIVERSE_NOT_CONFIGURED = "UNIVERSE_NOT_CONFIGURED"
PROVIDER_ENTITLEMENT_UNAVAILABLE = "PROVIDER_ENTITLEMENT_UNAVAILABLE"
EQUITY_SOURCE_RESULT_INVALID = "EQUITY_SOURCE_RESULT_INVALID"
EQUITY_COVERAGE_MISMATCH = "EQUITY_COVERAGE_MISMATCH"
EQUITY_REQUEST_BOUND_EXCEEDED = "EQUITY_REQUEST_BOUND_EXCEEDED"
EQUITY_RESPONSE_BOUND_EXCEEDED = "EQUITY_RESPONSE_BOUND_EXCEEDED"
EQUITY_WATERMARK_NOT_ADVANCED = "EQUITY_WATERMARK_NOT_ADVANCED"

# Same clocks the reference bootstrap uses: effective one minute after creation, valid for the six-hour refresh.
SEED_EFFECTIVE_DELAY = timedelta(minutes=1)
SEED_VALIDITY = timedelta(hours=6)


def _next_seed_generation(cursor) -> int:
    """Owner decision (2026-10-12): the generation snapshots and receipts carry is the number of READY
    seed manifests, so the seed about to be persisted must raise that count by exactly one."""
    cursor.execute("SELECT count(*) FROM rt.seed_manifests WHERE status = 'READY'")
    return int(cursor.fetchone()[0]) + 1


def reference_block_reason(environment: RunnerEnvironment) -> str | None:
    """Why REFERENCE would be BLOCKED_POLICY with this environment (None: it can run)."""
    if environment.reference_service is None:
        return REFERENCE_SOURCE_NOT_CONFIGURED
    if environment.universe is None:
        return UNIVERSE_NOT_CONFIGURED
    return None


def equity_block_reason(environment: RunnerEnvironment) -> str | None:
    """Why EQUITY_INGEST would be BLOCKED_POLICY with this environment (None: it can run)."""
    source = environment.equity_source
    if source is None or getattr(source, "fact_kind", None) is not DeveloperAcquisitionFactKind.EQUITY_BAR:
        return EQUITY_SOURCE_NOT_CONFIGURED
    if environment.universe is None:
        return UNIVERSE_NOT_CONFIGURED
    return None


def reference_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        blocked = reference_block_reason(environment)
        if blocked is not None:
            return StageOutcome.blocked(blocked)
        service, universe = environment.reference_service, environment.universe
        if context.is_cancelled():
            return StageOutcome.failed(STAGE_CANCELLED)
        now = context.started_at
        request = ReferenceSeedRequest(
            universe, context.plan.session_date, now, now + SEED_EFFECTIVE_DELAY, now + SEED_VALIDITY,
        )
        try:
            result = service.run_once(request)
        except ProviderEntitlementUnavailable:
            return StageOutcome.blocked(PROVIDER_ENTITLEMENT_UNAVAILABLE)
        if context.is_cancelled():
            return StageOutcome.failed(STAGE_CANCELLED)
        manifest = result.manifest
        references = V2StageDatabase(context, RealtimeDatabaseRole.REFERENCES)
        try:
            with references.transaction() as cursor:
                cursor.execute("SELECT 1 FROM rt.seed_manifests WHERE seed_id = %s", (str(manifest.seed_id),))
                if cursor.fetchone() is None:
                    persist_reference_result_cursor(
                        cursor, result, generation=_next_seed_generation(cursor), scope=environment.reference_scope,
                    )
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        if manifest.status is SeedStatus.READY:
            return StageOutcome.complete(evidence_ref=str(manifest.seed_id))
        # An UNAVAILABLE manifest is persisted as evidence but never becomes current: report why.
        return StageOutcome("BLOCKED_POLICY", manifest.reason_codes, None, str(manifest.seed_id))

    return run


@dataclass(frozen=True, slots=True)
class EquityAcquisitionRequest:
    """What an equity bar source is asked for. Duck-compatible with DeveloperAcquisitionRequest
    (cohort_id, universe, requested_at) but not tied to the canary universe; the ledger ids let a
    source link its provider requests to the stage run."""

    cohort_id: UUID
    universe: UniverseManifest
    requested_at: datetime
    window_id: UUID | None = None
    stage_run_id: UUID | None = None

    def __post_init__(self) -> None:
        if self.requested_at.tzinfo is None or self.requested_at.utcoffset() is None:
            raise ValueError("requested_at must be timezone-aware")
        object.__setattr__(self, "requested_at", self.requested_at.astimezone(timezone.utc))


@dataclass(frozen=True, slots=True)
class EquityAcquisitionResult:
    acquisition_batch_ids: tuple[UUID, ...]
    fact_count: int
    provider_requests: int
    response_bytes: int
    watermarks_advanced: int
    unadvanced_batches: int


class EquityAcquisitionRejected(ValueError):
    """The source result broke a contract; `code` is the stage reason code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _checkpoint(should_stop: Callable[[], bool]) -> None:
    if should_stop():
        raise StageCancelled("equity acquisition was abandoned")


def acquire_equity_bars(
    database: RealtimeProcessDatabase,
    source,
    request: EquityAcquisitionRequest,
    *,
    should_stop: Callable[[], bool] = lambda: False,
    repository=persist_developer_acquisition_batch_cursor,
    request_repository=record_provider_request_cursor,
    watermark_repository=advance_acquisition_watermark_cursor,
) -> EquityAcquisitionResult:
    """Equity-only counterpart of DeveloperAcquisitionProducer.run_once, under the ingress role.

    `source` is any object with `fact_kind` EQUITY_BAR and `collect(request)` returning a
    DeveloperSourceResult (a DeveloperAcquisitionSource). The source is called once, its result is
    validated and the batches are built before anything is written; `should_stop` is polled between
    those phases (a stop raises StageCancelled)."""
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("equity acquisition database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.INGRESS:
        raise ValueError("equity acquisition requires the ingress database role")
    if source.fact_kind is not DeveloperAcquisitionFactKind.EQUITY_BAR:
        raise ValueError("equity acquisition source has the wrong fact kind")
    _checkpoint(should_stop)
    result = source.collect(request)
    _checkpoint(should_stop)
    if not isinstance(result, DeveloperSourceResult) or any(
        batch.fact_kind is not DeveloperAcquisitionFactKind.EQUITY_BAR for batch in result.batches
    ):
        raise EquityAcquisitionRejected(EQUITY_SOURCE_RESULT_INVALID)
    symbols = {
        fact.revision.ticker
        for batch in result.batches
        for fact in batch.facts
        if isinstance(fact, DeveloperEquityBarAcquisitionFact)
    }
    if symbols != set(request.universe.equity_tickers):
        raise EquityAcquisitionRejected(EQUITY_COVERAGE_MISMATCH)
    if result.provider_requests > MAX_DEVELOPER_CANARY_REQUESTS:
        raise EquityAcquisitionRejected(EQUITY_REQUEST_BOUND_EXCEEDED)
    if result.response_bytes > MAX_DEVELOPER_CANARY_RESPONSE_BYTES:
        raise EquityAcquisitionRejected(EQUITY_RESPONSE_BOUND_EXCEEDED)
    try:
        batches = tuple(
            build_developer_acquisition_batch(
                cohort_id=request.cohort_id,
                provider=result.provider,
                provider_batch_id=batch.provider_batch_id,
                started_at=batch.started_at,
                completed_at=batch.completed_at,
                facts=batch.facts,
                request_id=batch.request_id,
            )
            for batch in result.batches
        )
    except ValueError as error:
        raise EquityAcquisitionRejected(EQUITY_SOURCE_RESULT_INVALID) from error
    if result.requests:
        # Request evidence is its own transaction so a failed handoff keeps the pacing record.
        with database.transaction() as cursor:
            for item in result.requests:
                request_repository(cursor, item.reservation, item.completion)
        _checkpoint(should_stop)
    complete_requests = frozenset(
        item.reservation.request_id
        for item in result.requests
        if item.completion.completeness is RequestCompleteness.COMPLETE
    )
    advanced = unadvanced = 0
    with database.transaction() as cursor:
        for batch in batches:
            repository(cursor, batch)
            # RT-03: only a fully read request may advance an acquisition watermark.
            if batch.request_id in complete_requests:
                for watermark in developer_equity_watermarks(batch, request_id=batch.request_id):
                    watermark_repository(cursor, watermark)
                    advanced += 1
            else:
                unadvanced += 1
    return EquityAcquisitionResult(
        acquisition_batch_ids=tuple(batch.acquisition_batch_id for batch in batches),
        fact_count=sum(len(batch.facts) for batch in batches),
        provider_requests=result.provider_requests,
        response_bytes=result.response_bytes,
        watermarks_advanced=advanced,
        unadvanced_batches=unadvanced,
    )


def equity_ingest_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        blocked = equity_block_reason(environment)
        if blocked is not None:
            return StageOutcome.blocked(blocked)
        source, universe = environment.equity_source, environment.universe
        request = EquityAcquisitionRequest(
            context.plan.cohort_id, universe, context.window.cutoff,
            window_id=context.window.window_id, stage_run_id=context.stage_run_id,
        )
        ingress = V2StageDatabase(context, RealtimeDatabaseRole.INGRESS)
        try:
            result = acquire_equity_bars(ingress, source, request, should_stop=context.is_cancelled)
        except EquityAcquisitionRejected as rejected:
            return StageOutcome.failed(rejected.code)
        except (StageCancelled, WriteFenceRejected):
            return StageOutcome.failed(STAGE_CANCELLED)
        return StageOutcome.complete(
            degraded=(EQUITY_WATERMARK_NOT_ADVANCED,) if result.unadvanced_batches else (),
        )

    return run
