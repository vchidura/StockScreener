from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID

from .acquisition_handoff import (
    DeveloperAcquisitionBatch,
    DeveloperAcquisitionFact,
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
    DeveloperOptionSnapshotAcquisitionFact,
    build_developer_acquisition_batch,
    persist_developer_acquisition_batch_cursor,
)
from .acquisition_ledger import (
    AcquisitionWatermark,
    RecordedProviderRequest,
    RequestCompleteness,
    advance_acquisition_watermark_cursor,
    record_provider_request_cursor,
)
from .contracts import CANARY_UNIVERSE, UniverseManifest
from .database_access import RealtimeDatabaseRole
from .worker_database import RealtimeProcessDatabase


MAX_DEVELOPER_SOURCE_BATCHES = 100
MAX_DEVELOPER_SOURCE_FACTS = 2_400
MAX_DEVELOPER_CANARY_REQUESTS = 88
MAX_DEVELOPER_CANARY_RESPONSE_BYTES = 32 * 1024 * 1024
DEVELOPER_ACQUISITION_OVERLAP_POLICY = "developer_rest_overlap_v1"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class DeveloperAcquisitionRequest:
    cohort_id: UUID
    universe: UniverseManifest
    requested_at: datetime

    def __post_init__(self) -> None:
        if self.universe != CANARY_UNIVERSE:
            raise ValueError("Developer acquisition requires the reviewed canary universe")
        object.__setattr__(
            self,
            "requested_at",
            _utc(self.requested_at, "requested_at"),
        )


@dataclass(frozen=True, slots=True)
class CompletedDeveloperSourceBatch:
    fact_kind: DeveloperAcquisitionFactKind
    provider_batch_id: UUID
    started_at: datetime
    completed_at: datetime
    facts: tuple[DeveloperAcquisitionFact, ...]
    request_id: UUID | None = None

    def __post_init__(self) -> None:
        started = _utc(self.started_at, "started_at")
        completed = _utc(self.completed_at, "completed_at")
        if completed < started:
            raise ValueError("Developer source batch completes before it starts")
        if not self.facts:
            raise ValueError("Developer source batch cannot be empty")
        expected_type = (
            DeveloperEquityBarAcquisitionFact
            if self.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR
            else DeveloperOptionSnapshotAcquisitionFact
        )
        if any(not isinstance(fact, expected_type) for fact in self.facts):
            raise ValueError("Developer source batch fact kind is inconsistent")
        if any(fact.provider_batch_id != self.provider_batch_id for fact in self.facts):
            raise ValueError("Developer source batch identity is inconsistent")
        if max(fact.observed_at for fact in self.facts) > completed:
            raise ValueError("Developer source batch completed before observation")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "completed_at", completed)


@dataclass(frozen=True, slots=True)
class DeveloperSourceResult:
    provider: str
    batches: tuple[CompletedDeveloperSourceBatch, ...]
    provider_requests: int
    response_bytes: int
    requests: tuple[RecordedProviderRequest, ...] = ()

    def __post_init__(self) -> None:
        provider = self.provider.strip()
        if not provider:
            raise ValueError("Developer acquisition provider cannot be blank")
        if not self.batches:
            raise ValueError("Developer source result must contain completed batches")
        if len(self.batches) > MAX_DEVELOPER_SOURCE_BATCHES:
            raise ValueError("Developer source result exceeds the batch bound")
        if sum(len(batch.facts) for batch in self.batches) > MAX_DEVELOPER_SOURCE_FACTS:
            raise ValueError("Developer source result exceeds the fact bound")
        identities = tuple(
            (batch.fact_kind, batch.provider_batch_id) for batch in self.batches
        )
        if len(identities) != len(set(identities)):
            raise ValueError("Developer source result contains duplicate batches")
        if self.provider_requests < 0 or self.response_bytes < 0:
            raise ValueError("Developer source counters cannot be negative")
        recorded = tuple(item.reservation.request_id for item in self.requests)
        if len(recorded) != len(set(recorded)):
            raise ValueError("Developer source result repeats a provider request")
        object.__setattr__(self, "provider", provider)


class DeveloperAcquisitionSource(Protocol):
    fact_kind: DeveloperAcquisitionFactKind

    def collect(self, request: DeveloperAcquisitionRequest) -> DeveloperSourceResult:
        ...


@dataclass(frozen=True, slots=True)
class DeveloperAcquisitionProducerResult:
    acquisition_batch_ids: tuple[UUID, ...]
    equity_fact_count: int
    option_fact_count: int
    provider_requests: int
    response_bytes: int

    def __post_init__(self) -> None:
        if not self.acquisition_batch_ids:
            raise ValueError("Developer producer must persist acquisition batches")
        if len(self.acquisition_batch_ids) != len(set(self.acquisition_batch_ids)):
            raise ValueError("Developer producer batch IDs must be unique")
        if min(
            self.equity_fact_count,
            self.option_fact_count,
            self.provider_requests,
            self.response_bytes,
        ) < 0:
            raise ValueError("Developer producer counters cannot be negative")


def developer_equity_watermarks(
    batch: DeveloperAcquisitionBatch,
    *,
    request_id: UUID | None = None,
) -> tuple[AcquisitionWatermark, ...]:
    """Latest complete bar end per instrument, interval and source of an equity batch."""
    if batch.fact_kind is not DeveloperAcquisitionFactKind.EQUITY_BAR:
        raise ValueError("watermarks derive from equity bar batches only")
    latest: dict[tuple[int, str, str], datetime] = {}
    for fact in batch.facts:
        if not isinstance(fact, DeveloperEquityBarAcquisitionFact):
            continue
        key = (fact.instrument_id, fact.revision.interval, fact.revision.source_kind.value)
        latest[key] = max(latest.get(key, fact.revision.bar_end), fact.revision.bar_end)
    return tuple(
        AcquisitionWatermark(
            cohort_id=batch.cohort_id,
            instrument_id=instrument_id,
            interval=interval,
            source_kind=source_kind,
            last_complete_bar_end=bar_end,
            overlap_policy=DEVELOPER_ACQUISITION_OVERLAP_POLICY,
            request_id=request_id,
        )
        for (instrument_id, interval, source_kind), bar_end in sorted(latest.items())
    )


class DeveloperAcquisitionProducer:
    def __init__(
        self,
        equity_source: DeveloperAcquisitionSource,
        option_source: DeveloperAcquisitionSource,
    ) -> None:
        if equity_source.fact_kind is not DeveloperAcquisitionFactKind.EQUITY_BAR:
            raise ValueError("Developer equity source has the wrong fact kind")
        if option_source.fact_kind is not DeveloperAcquisitionFactKind.OPTION_SNAPSHOT:
            raise ValueError("Developer option source has the wrong fact kind")
        self.equity_source = equity_source
        self.option_source = option_source

    @staticmethod
    def _require_source_kind(
        result: DeveloperSourceResult,
        expected: DeveloperAcquisitionFactKind,
    ) -> None:
        if any(batch.fact_kind is not expected for batch in result.batches):
            raise ValueError("Developer source result changed its fact kind")

    @staticmethod
    def _require_exact_coverage(
        equity: DeveloperSourceResult,
        options: DeveloperSourceResult,
        universe: UniverseManifest,
    ) -> None:
        equity_symbols = {
            fact.revision.ticker
            for batch in equity.batches
            for fact in batch.facts
            if isinstance(fact, DeveloperEquityBarAcquisitionFact)
        }
        option_underlyings = {
            fact.snapshot.underlyer
            for batch in options.batches
            for fact in batch.facts
            if isinstance(fact, DeveloperOptionSnapshotAcquisitionFact)
        }
        if equity_symbols != set(universe.equity_tickers):
            raise ValueError("Developer equity source does not cover the exact canary")
        if option_underlyings != set(universe.option_underlyings):
            raise ValueError("Developer option source does not cover the exact canary")

    def run_once(
        self,
        database: RealtimeProcessDatabase,
        request: DeveloperAcquisitionRequest,
        *,
        repository=persist_developer_acquisition_batch_cursor,
        request_repository=record_provider_request_cursor,
        watermark_repository=advance_acquisition_watermark_cursor,
    ) -> DeveloperAcquisitionProducerResult:
        if not isinstance(database, RealtimeProcessDatabase):
            raise TypeError("Developer producer database is invalid")
        if database.process_plan.role is not RealtimeDatabaseRole.INGRESS:
            raise ValueError("Developer producer requires the ingress database role")
        if not isinstance(request, DeveloperAcquisitionRequest):
            raise TypeError("Developer producer request is invalid")
        equity = self.equity_source.collect(request)
        options = self.option_source.collect(request)
        if not isinstance(equity, DeveloperSourceResult) or not isinstance(
            options, DeveloperSourceResult
        ):
            raise TypeError("Developer source returned an invalid result")
        self._require_source_kind(equity, DeveloperAcquisitionFactKind.EQUITY_BAR)
        self._require_source_kind(options, DeveloperAcquisitionFactKind.OPTION_SNAPSHOT)
        self._require_exact_coverage(equity, options, request.universe)
        provider_requests = equity.provider_requests + options.provider_requests
        response_bytes = equity.response_bytes + options.response_bytes
        if provider_requests > MAX_DEVELOPER_CANARY_REQUESTS:
            raise ValueError("Developer acquisition exceeds the request bound")
        if response_bytes > MAX_DEVELOPER_CANARY_RESPONSE_BYTES:
            raise ValueError("Developer acquisition exceeds the response-byte bound")
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
            for result in (equity, options)
            for batch in result.batches
        )
        ledger = tuple(item for result in (equity, options) for item in result.requests)
        if ledger:
            # Request evidence is its own transaction so a failed handoff keeps the pacing record.
            with database.transaction() as cursor:
                for item in ledger:
                    request_repository(cursor, item.reservation, item.completion)
        complete_requests = frozenset(
            item.reservation.request_id
            for item in ledger
            if item.completion.completeness is RequestCompleteness.COMPLETE
        )
        with database.transaction() as cursor:
            for batch in batches:
                repository(cursor, batch)
                # RT-03: only a fully read request may advance an acquisition watermark.
                if (
                    batch.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR
                    and batch.request_id in complete_requests
                ):
                    for watermark in developer_equity_watermarks(
                        batch, request_id=batch.request_id,
                    ):
                        watermark_repository(cursor, watermark)
        return DeveloperAcquisitionProducerResult(
            acquisition_batch_ids=tuple(batch.acquisition_batch_id for batch in batches),
            equity_fact_count=sum(len(batch.facts) for batch in equity.batches),
            option_fact_count=sum(len(batch.facts) for batch in options.batches),
            provider_requests=provider_requests,
            response_bytes=response_bytes,
        )