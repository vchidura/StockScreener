"""Bounded exact-nine Developer source for one V2-10 five-minute window."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import BarAvailabilityMode
from equity.polygon import normalize_native_bars

from .acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
)
from .acquisition_producer import CompletedDeveloperSourceBatch, DeveloperSourceResult
from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from .equity_bar_adapter import adapt_developer_equity_bar
from .equity_interval_derivation import derive_realtime_interval_facts
from .equity_v2_backfill_source import AccountingPolygonTransport, _accounting_transport
from .equity_validation_source import PolygonEquityValidationHistorySource

WINDOW_MINUTES = 5
EXPECTED_PROVIDER_REQUESTS = 18


class PolygonEquityV2WindowSource:
    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(
        self,
        source: PolygonEquityValidationHistorySource,
        *,
        cutoff: datetime,
        window_start: datetime | None = None,
        existing_revision_ids: frozenset[UUID] = frozenset(),
        segment_identity_suffix: str = "",
    ) -> None:
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("V2 window cutoff must be timezone-aware")
        if source.universe != EQUITY_VALIDATION_UNIVERSE:
            raise ValueError("V2 window source requires the exact validation universe")
        self.source = source
        self.cutoff = cutoff.astimezone(timezone.utc)
        self.window_start = (
            window_start.astimezone(timezone.utc)
            if window_start is not None
            and window_start.tzinfo is not None
            and window_start.utcoffset() is not None
            else self.cutoff - timedelta(minutes=WINDOW_MINUTES)
            if window_start is None
            else None
        )
        if (
            self.window_start is None
            or self.window_start >= self.cutoff
            or int((self.cutoff - self.window_start).total_seconds()) % 300
        ):
            raise ValueError("V2 window start must define complete five-minute windows")
        self.existing_revision_ids = frozenset(existing_revision_ids)
        self.segment_identity_suffix = segment_identity_suffix.strip()

    def _collect_interval(self, identity, interval, width):
        start = self.window_start
        start_ms = int(start.timestamp() * 1000)
        cutoff_ms = int(self.cutoff.timestamp() * 1000)
        accounting = _accounting_transport(self.source)
        before = len(accounting.requests)
        rows = self.source._get(
            f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/"
            f"{width}/minute/{start_ms}/{cutoff_ms}",
            {"adjusted": "false", "sort": "asc", "limit": "50000"},
            interval_minutes=width,
        )
        if len(accounting.requests) != before + 1:
            raise RuntimeError("V2 window interval did not use exactly one provider request")
        evidence = accounting.requests[-1]
        segment = uuid5(
            NAMESPACE_URL,
            f"v2-developer-window:{self.source.cohort_id}:{identity.ticker}:{interval}:"
            f"{start.isoformat()}:{self.cutoff.isoformat()}:{canonical_sha256(rows)}:"
            f"{self.segment_identity_suffix}",
        )
        completed_at = evidence.completion.completed_at
        assert completed_at is not None
        revisions = normalize_native_bars(
            identity.security.security_id,
            identity.ticker,
            interval,
            rows,
            observed_at=completed_at,
            adjusted=False,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
            ingestion_segment_id=segment,
        )
        complete_revisions = tuple(
            revision
            for revision in revisions
            if revision.bar_start >= start and revision.bar_end <= self.cutoff
        )
        minutes = int((self.cutoff - start).total_seconds() // 60)
        expected = minutes if interval == "1m" else minutes // 5
        if (
            len(complete_revisions) != expected
            or complete_revisions[0].bar_start != start
            or complete_revisions[-1].bar_end != self.cutoff
        ):
            raise ValueError(
                f"V2 window {interval} coverage is incomplete for {identity.ticker}"
            )
        revisions = tuple(
            revision for revision in complete_revisions
            if revision.bar_revision_id not in self.existing_revision_ids
        )
        if not revisions:
            return None, evidence, complete_revisions
        facts = tuple(
            DeveloperEquityBarAcquisitionFact(identity.instrument_id, revision)
            for revision in revisions
        )
        started_at = evidence.completion.started_at or evidence.reservation.reserved_at
        return CompletedDeveloperSourceBatch(
            fact_kind=DeveloperAcquisitionFactKind.EQUITY_BAR,
            provider_batch_id=segment,
            started_at=started_at,
            completed_at=completed_at,
            facts=facts,
            request_id=evidence.reservation.request_id,
        ), evidence, complete_revisions

    def collect(self, request) -> DeveloperSourceResult:
        if (
            request.cohort_id != self.source.cohort_id
            or request.universe != EQUITY_VALIDATION_UNIVERSE
            or request.requested_at != self.cutoff
        ):
            raise ValueError("V2 window acquisition request changed approved scope")
        batches = []
        requests = []
        for identity in self.source.identities:
            five_revisions = ()
            five_evidence = None
            for interval, width in (("1m", 1), ("5m", 5)):
                batch, evidence, complete = self._collect_interval(identity, interval, width)
                if batch is not None:
                    batches.append(batch)
                requests.append(evidence)
                if interval == "5m":
                    five_revisions = complete
                    five_evidence = evidence
            assert five_evidence is not None
            five_completed_at = five_evidence.completion.completed_at
            assert five_completed_at is not None
            five_facts = tuple(
                adapt_developer_equity_bar(
                    revision,
                    cohort_id=self.source.cohort_id,
                    instrument_id=identity.instrument_id,
                )
                for revision in five_revisions
            )
            for interval in ("15m", "30m", "1h"):
                segment = uuid5(
                    NAMESPACE_URL,
                    f"v2-developer-window-derived:{self.source.cohort_id}:"
                    f"{identity.ticker}:{interval}:{self.window_start.isoformat()}:"
                    f"{self.cutoff.isoformat()}:{self.segment_identity_suffix}",
                )
                derived = derive_realtime_interval_facts(
                    identity.security,
                    five_facts,
                    target_interval=interval,
                    observed_at=five_completed_at,
                    ingestion_segment_id=segment,
                )
                derived = tuple(
                    fact for fact in derived
                    if fact.revision.bar_revision_id not in self.existing_revision_ids
                )
                if not derived:
                    continue
                started_at = (
                    five_evidence.completion.started_at
                    or five_evidence.reservation.reserved_at
                )
                completed_at = five_completed_at
                batches.append(CompletedDeveloperSourceBatch(
                    fact_kind=DeveloperAcquisitionFactKind.EQUITY_BAR,
                    provider_batch_id=segment,
                    started_at=started_at,
                    completed_at=completed_at,
                    facts=tuple(
                        DeveloperEquityBarAcquisitionFact(
                            fact.instrument_id,
                            fact.revision,
                        )
                        for fact in derived
                    ),
                    request_id=five_evidence.reservation.request_id,
                ))
        if len(requests) != EXPECTED_PROVIDER_REQUESTS:
            raise RuntimeError("V2 window provider request count changed")
        return DeveloperSourceResult(
            provider="MASSIVE",
            batches=tuple(batches),
            provider_requests=len(requests),
            response_bytes=sum(
                evidence.completion.response_bytes or 0
                for evidence in requests
            ),
            requests=tuple(requests),
        )
