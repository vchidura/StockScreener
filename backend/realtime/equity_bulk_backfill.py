"""V2-10 equity history bulk backfill through the acquisition, batch and fact path."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import islice
from typing import Iterable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from .acquisition_consumer import (
    DEVELOPER_EQUITY_ACQUISITION_JOB, enqueue_pending_developer_acquisition_jobs,
)
from .acquisition_handoff import (
    MAX_DEVELOPER_ACQUISITION_FACTS,
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
)
from .acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
)
from .acquisition_producer import CompletedDeveloperSourceBatch, DeveloperSourceResult
from .contracts import UniverseManifest, canonical_sha256
from .database_access import RealtimeDatabaseRole
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_bar_drain import drain_equity_bar_jobs
from .preflight import ProviderTier
from .stage_runners_ingest import EquityAcquisitionRequest, acquire_equity_bars
from .worker_database import RealtimeProcessDatabase

RETAINED_BACKFILL_PROVIDER = "MASSIVE_RETAINED_V1"
RETAINED_BACKFILL_ACCOUNT = "LOCAL_RETAINED_V1"
RETAINED_BACKFILL_ENDPOINT = "RETAINED_DB_BACKFILL"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _chunks(values: Sequence, size: int) -> Iterable[tuple]:
    iterator = iter(values)
    while rows := tuple(islice(iterator, size)):
        yield rows


@dataclass(frozen=True, slots=True)
class EquityBulkBackfillChunk:
    ticker: str
    interval: str
    adjustment_basis: str
    source_segment_id: UUID
    provider_batch_id: UUID
    facts: tuple[DeveloperEquityBarAcquisitionFact, ...]
    closure_sha256: str

    def __post_init__(self) -> None:
        if not 1 <= len(self.facts) <= MAX_DEVELOPER_ACQUISITION_FACTS:
            raise ValueError("equity bulk chunk requires 1-1000 facts")
        identities = {
            (
                fact.revision.ticker,
                fact.revision.interval,
                "ADJUSTED" if fact.revision.adjusted else "RAW_UNADJUSTED",
                fact.revision.ingestion_segment_id,
                fact.provider_batch_id,
            )
            for fact in self.facts
        }
        if identities != {
            (
                self.ticker,
                self.interval,
                self.adjustment_basis,
                self.source_segment_id,
                self.provider_batch_id,
            )
        }:
            raise ValueError("equity bulk chunk crosses its ticker, interval, basis or batch")
        if self.closure_sha256 != canonical_sha256({
            "ticker": self.ticker,
            "interval": self.interval,
            "adjustment_basis": self.adjustment_basis,
            "source_segment_id": self.source_segment_id,
            "provider_batch_id": self.provider_batch_id,
            "bars": tuple(
                (fact.revision.bar_revision_id, fact.revision.payload_sha256)
                for fact in self.facts
            ),
        }):
            raise ValueError("equity bulk chunk closure hash mismatch")


@dataclass(frozen=True, slots=True)
class EquityBulkBackfillPlan:
    cohort_id: UUID
    tickers: tuple[str, ...]
    chunks: tuple[EquityBulkBackfillChunk, ...]
    fact_count: int
    sha256: str


@dataclass(frozen=True, slots=True)
class EquityBulkBackfillResult:
    plan_sha256: str
    chunks: int
    facts: int
    acquisition_batches: int
    retained_source_ledger_rows: int
    external_provider_requests: int
    watermarks_advanced: int
    jobs_enqueued: int
    jobs_completed: int
    bars_materialized: int


def build_equity_bulk_backfill_plan(
    facts: Sequence[RealtimeEquityBarFact],
    *,
    cohort_id: UUID,
    tickers: Sequence[str],
    maximum_chunk_facts: int = MAX_DEVELOPER_ACQUISITION_FACTS,
) -> EquityBulkBackfillPlan:
    rows = tuple(facts)
    expected_tickers = tuple(ticker.strip().upper() for ticker in tickers)
    if (
        not rows
        or not expected_tickers
        or len(expected_tickers) != len(set(expected_tickers))
        or not 1 <= maximum_chunk_facts <= MAX_DEVELOPER_ACQUISITION_FACTS
    ):
        raise ValueError("equity bulk backfill scope is invalid")
    if any(
        not isinstance(fact, RealtimeEquityBarFact)
        or fact.cohort_id != cohort_id
        or fact.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
        for fact in rows
    ):
        raise ValueError("equity bulk backfill facts changed cohort or provider tier")
    actual_tickers = {fact.revision.ticker for fact in rows}
    if actual_tickers != set(expected_tickers):
        raise ValueError("equity bulk backfill requires exact ticker coverage")
    revision_ids = [fact.revision.bar_revision_id for fact in rows]
    if len(revision_ids) != len(set(revision_ids)):
        raise ValueError("equity bulk backfill contains duplicate bar revisions")
    grouped = defaultdict(list)
    for fact in rows:
        source_segment_id = fact.revision.ingestion_segment_id
        if source_segment_id is None:
            raise ValueError("equity bulk backfill requires source segment lineage")
        grouped[(
            fact.revision.ticker,
            fact.revision.interval,
            "ADJUSTED" if fact.revision.adjusted else "RAW_UNADJUSTED",
            source_segment_id,
        )].append(fact)
    chunks = []
    for (ticker, interval, basis, source_segment_id), group in sorted(
        grouped.items(),
        key=lambda item: (
            0 if item[1][0].revision.source_kind.value == "NATIVE_REST" else 1,
            *(str(value) for value in item[0]),
        ),
    ):
        ordered = tuple(sorted(
            group,
            key=lambda fact: (
                fact.revision.bar_start,
                fact.revision.bar_revision_id,
            ),
        ))
        for index, source_rows in enumerate(_chunks(ordered, maximum_chunk_facts)):
            source_identity = canonical_sha256(tuple(
                (
                    fact.revision.bar_revision_id,
                    fact.revision.ingestion_segment_id,
                    fact.revision.payload_sha256,
                )
                for fact in source_rows
            ))
            provider_batch_id = uuid5(
                NAMESPACE_URL,
                f"v2-retained-backfill:{cohort_id}:{ticker}:{interval}:{basis}:{index}:{source_identity}",
            )
            handoff = tuple(
                DeveloperEquityBarAcquisitionFact(
                    fact.instrument_id,
                    fact.revision,
                    provider_batch_id,
                )
                for fact in source_rows
            )
            closure = canonical_sha256({
                "ticker": ticker,
                "interval": interval,
                "adjustment_basis": basis,
                "source_segment_id": source_segment_id,
                "provider_batch_id": provider_batch_id,
                "bars": tuple(
                    (fact.revision.bar_revision_id, fact.revision.payload_sha256)
                    for fact in handoff
                ),
            })
            chunks.append(EquityBulkBackfillChunk(
                ticker,
                interval,
                basis,
                source_segment_id,
                provider_batch_id,
                handoff,
                closure,
            ))
    identity = {
        "cohort_id": cohort_id,
        "tickers": expected_tickers,
        "chunks": tuple(chunk.closure_sha256 for chunk in chunks),
    }
    return EquityBulkBackfillPlan(
        cohort_id,
        expected_tickers,
        tuple(chunks),
        len(rows),
        canonical_sha256(identity),
    )


def _recorded_retained_read(
    chunk: EquityBulkBackfillChunk,
    *,
    cohort_id: UUID,
    recorded_at: datetime,
    retained_lineage: str = "V1",
) -> RecordedProviderRequest:
    namespace = (
        "v2-retained-backfill-request" if retained_lineage == "V1"
        else "v2-retained-target-request"
    )
    request_id = uuid5(
        NAMESPACE_URL,
        f"{namespace}:{cohort_id}:{chunk.closure_sha256}",
    )
    reservation = ProviderRequestReservation(
        request_id=request_id,
        account_alias=f"LOCAL_RETAINED_{retained_lineage}",
        lane=ProviderRequestLane.EQUITY_HISTORY,
        endpoint_class=RETAINED_BACKFILL_ENDPOINT,
        request_sha256=chunk.closure_sha256,
        reserved_at=recorded_at,
        cohort_id=cohort_id,
    )
    completion = ProviderRequestCompletion(
        state=ProviderRequestState.COMPLETE,
        started_at=recorded_at,
        completed_at=recorded_at,
        outcome=ProviderRequestOutcome.OK,
        response_bytes=0,
        row_count=len(chunk.facts),
        pages_fetched=1,
        requested_from=min(fact.revision.bar_start for fact in chunk.facts),
        requested_to=max(fact.revision.bar_end for fact in chunk.facts),
        price_basis=PriceBasis.NOT_APPLICABLE,
        completeness=RequestCompleteness.COMPLETE,
    )
    return RecordedProviderRequest(reservation, completion)


class RetainedEquityBulkSource:
    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(
        self,
        chunk: EquityBulkBackfillChunk,
        *,
        cohort_id: UUID,
        recorded_at: datetime,
        request_evidence: RecordedProviderRequest | None = None,
        external_provider_request: bool = False,
        retained_lineage: str = "V1",
    ) -> None:
        self.chunk = chunk
        self.cohort_id = cohort_id
        self.recorded_at = _utc(recorded_at, "recorded_at")
        self.request_evidence = request_evidence
        self.external_provider_request = external_provider_request
        if retained_lineage not in {"V1", "V2"}:
            raise ValueError("retained backfill lineage must be V1 or V2")
        self.retained_lineage = retained_lineage

    def collect(self, request: EquityAcquisitionRequest) -> DeveloperSourceResult:
        if (
            request.cohort_id != self.cohort_id
            or request.universe.equity_tickers != (self.chunk.ticker,)
            or request.universe.option_underlyings
        ):
            raise ValueError("retained equity bulk source request changed scope")
        ledger = self.request_evidence or _recorded_retained_read(
            self.chunk,
            cohort_id=self.cohort_id,
            recorded_at=self.recorded_at,
            retained_lineage=self.retained_lineage,
        )
        observed = tuple(fact.observed_at for fact in self.chunk.facts)
        batch = CompletedDeveloperSourceBatch(
            fact_kind=DeveloperAcquisitionFactKind.EQUITY_BAR,
            provider_batch_id=self.chunk.provider_batch_id,
            started_at=min(observed),
            completed_at=max(observed),
            facts=self.chunk.facts,
            request_id=ledger.reservation.request_id,
        )
        return DeveloperSourceResult(
            provider=f"MASSIVE_RETAINED_{self.retained_lineage}",
            batches=(batch,),
            provider_requests=1 if self.external_provider_request else 0,
            response_bytes=ledger.completion.response_bytes or 0,
            requests=(ledger,),
        )


def execute_equity_bulk_backfill(
    ingress_database: RealtimeProcessDatabase,
    compute_database: RealtimeProcessDatabase,
    plan: EquityBulkBackfillPlan,
    *,
    recorded_at: datetime,
    request_evidence_by_segment: Mapping[UUID | tuple[UUID, str, str], RecordedProviderRequest] | None = None,
    retained_lineage: str = "V1",
) -> EquityBulkBackfillResult:
    if (
        ingress_database.process_plan.role is not RealtimeDatabaseRole.INGRESS
        or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE
    ):
        raise ValueError("equity bulk backfill requires ingress and compute database roles")
    recorded = _utc(recorded_at, "recorded_at")
    evidence_by_segment = dict(request_evidence_by_segment or {})
    batch_ids = []
    watermarks = 0
    external_request_ids = set()
    retained_chunks = 0
    for chunk in plan.chunks:
        evidence = evidence_by_segment.get(
            (chunk.source_segment_id, chunk.ticker, chunk.interval),
            evidence_by_segment.get(chunk.source_segment_id),
        )
        if evidence is None:
            retained_chunks += 1
        else:
            external_request_ids.add(evidence.reservation.request_id)
        universe = UniverseManifest(
            version=f"v2-retained-backfill-{chunk.ticker.lower()}-v1",
            equity_tickers=(chunk.ticker,),
            option_underlyings=(),
        )
        result = acquire_equity_bars(
            ingress_database,
            RetainedEquityBulkSource(
                chunk,
                cohort_id=plan.cohort_id,
                recorded_at=recorded,
                request_evidence=evidence,
                external_provider_request=evidence is not None,
                retained_lineage=retained_lineage,
            ),
            EquityAcquisitionRequest(
                plan.cohort_id,
                universe,
                recorded,
            ),
        )
        batch_ids.extend(result.acquisition_batch_ids)
        watermarks += result.watermarks_advanced
        if result.unadvanced_batches:
            raise RuntimeError("equity bulk backfill left a watermark unadvanced")
    jobs = []
    for group in _chunks(tuple(batch_ids), 100):
        jobs.extend(enqueue_pending_developer_acquisition_jobs(
            compute_database,
            cohort_id=plan.cohort_id,
            fact_kind=DeveloperAcquisitionFactKind.EQUITY_BAR,
            acquisition_batch_ids=group,
            limit=len(group),
        ))
    lease_owner = uuid5(NAMESPACE_URL, f"v2-retained-backfill-lease:{plan.sha256}")
    completed = materialized = 0
    for group in _chunks(tuple(batch_ids), 100):
        drained = drain_equity_bar_jobs(
            compute_database,
            cohort_id=plan.cohort_id,
            lease_owner=lease_owner,
            max_jobs=len(group),
            acquisition_batch_ids=tuple(group),
        )
        if drained.unsuccessful or drained.retried:
            raise RuntimeError("retained backfill materialization did not complete cleanly")
        with compute_database.transaction(read_only=True) as cursor:
            cursor.execute(
                "SELECT source_revision_id,status FROM rt.materialization_jobs "
                "WHERE cohort_id=%s AND job_kind=%s "
                "AND source_revision_id=ANY(%s::uuid[])",
                (
                    str(plan.cohort_id), DEVELOPER_EQUITY_ACQUISITION_JOB,
                    [str(value) for value in group],
                ),
            )
            outcomes = tuple(cursor.fetchall())
        if (
            {UUID(str(row[0])) for row in outcomes} != set(group)
            or any(row[1] != "COMPLETE" for row in outcomes)
        ):
            raise RuntimeError("owned backfill jobs are not all complete")
        completed += drained.completed
        materialized += drained.bars
    return EquityBulkBackfillResult(
        plan_sha256=plan.sha256,
        chunks=len(plan.chunks),
        facts=plan.fact_count,
        acquisition_batches=len(batch_ids),
        retained_source_ledger_rows=retained_chunks,
        external_provider_requests=len(external_request_ids),
        watermarks_advanced=watermarks,
        jobs_enqueued=len(jobs),
        jobs_completed=completed,
        bars_materialized=materialized,
    )
