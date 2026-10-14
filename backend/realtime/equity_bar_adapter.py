from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)

from .contracts import AssetClass, MarketChannel
from .journal import NormalizedJournalRecord
from .preflight import ProviderTier


@dataclass(frozen=True, slots=True)
class RealtimeEquityBarFact:
    cohort_id: UUID
    provider_tier: ProviderTier
    instrument_id: int
    revision: EquityBarRevision
    journal_generation: UUID | None = None
    stream_session_id: UUID | None = None
    source_offset: int | None = None

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("realtime bar instrument ID must be positive")
        journal_values = (
            self.journal_generation,
            self.stream_session_id,
            self.source_offset,
        )
        if self.revision.source_kind in {
            BarSourceKind.DERIVED,
            BarSourceKind.RECONCILED,
        }:
            if not self.revision.source_bar_revision_ids:
                raise ValueError("derived bar requires source revision lineage")
            if any(value is not None for value in journal_values):
                raise ValueError("derived bar cannot carry journal lineage")
            if self.revision.ingestion_segment_id is None:
                raise ValueError("derived bar requires a materialization segment")
        elif self.provider_tier is ProviderTier.MASSIVE_ADVANCED:
            if self.revision.source_kind is not BarSourceKind.REALTIME_STREAM:
                raise ValueError("Advanced bar must use realtime stream source kind")
            if any(value is None for value in journal_values):
                raise ValueError("Advanced bar requires complete journal lineage")
            if self.revision.ingestion_segment_id is not None:
                raise ValueError("Advanced stream bar cannot use a REST ingestion segment")
        else:
            if self.revision.source_kind is not BarSourceKind.NATIVE_REST:
                raise ValueError("Developer bar must use native REST source kind")
            if any(value is not None for value in journal_values):
                raise ValueError("Developer REST bar cannot carry journal lineage")
            if self.revision.ingestion_segment_id is None:
                raise ValueError("Developer REST bar requires an ingestion segment")


def adapt_developer_equity_bar(
    revision: EquityBarRevision,
    *,
    cohort_id: UUID,
    instrument_id: int,
) -> RealtimeEquityBarFact:
    if not revision.is_final:
        raise ValueError("Developer completed-bar adapter requires a final bar")
    return RealtimeEquityBarFact(
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        instrument_id=instrument_id,
        revision=revision,
    )


def adapt_advanced_minute_aggregate(
    record: NormalizedJournalRecord,
    *,
    cohort_id: UUID,
    security_id: UUID,
) -> RealtimeEquityBarFact:
    if (
        record.asset_class is not AssetClass.EQUITY
        or record.event_kind is not MarketChannel.MINUTE_AGGREGATE
        or record.quality != "GOOD"
    ):
        raise ValueError("Advanced bar adapter requires a good equity AM event")
    try:
        details = json.loads(record.details_json)
        start_timestamp = int(details["start_timestamp"])
        end_timestamp = int(details["end_timestamp"])
        open_price = Decimal(str(details["open"]))
        high_price = Decimal(str(details["high"]))
        low_price = Decimal(str(details["low"]))
        close_price = Decimal(str(details["close"]))
        vwap = (
            Decimal(str(details["vwap"]))
            if details.get("vwap") is not None
            else None
        )
    except (KeyError, TypeError, ValueError, ArithmeticError, json.JSONDecodeError) as exc:
        raise ValueError("Advanced AM details are invalid") from exc
    if end_timestamp <= start_timestamp or end_timestamp != record.source_timestamp:
        raise ValueError("Advanced AM source window is inconsistent")
    bar_start = datetime.fromtimestamp(start_timestamp / 1000, tz=timezone.utc)
    bar_end = datetime.fromtimestamp(end_timestamp / 1000, tz=timezone.utc)
    if bar_end != record.event_at or record.received_at < bar_end:
        raise ValueError("Advanced AM clocks violate finality")
    if record.size is None:
        raise ValueError("Advanced AM volume is unavailable")
    revision_id = uuid5(
        NAMESPACE_URL,
        f"realtime-equity-bar:{cohort_id}:{record.instrument_id}:1m:"
        f"{bar_start.isoformat()}:{record.journal_generation}:{record.event_offset}:"
        f"{record.payload_sha256}",
    )
    revision = EquityBarRevision(
        bar_revision_id=revision_id,
        security_id=security_id,
        ticker=record.provider_symbol,
        interval="1m",
        session_date=record.source_session,
        bar_start=bar_start,
        bar_end=bar_end,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=record.size,
        vwap=vwap,
        transaction_count=None,
        source_kind=BarSourceKind.REALTIME_STREAM,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True,
        system_observed_at=record.received_at,
        replay_available_at=None,
        adjusted=False,
        payload_sha256=record.payload_sha256,
        quality_codes=("MASSIVE_ADVANCED_AM_V1",),
        provider_published_at=record.event_at,
        session_scope=BarSessionScope.RTH,
    )
    return RealtimeEquityBarFact(
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        instrument_id=record.instrument_id,
        revision=revision,
        journal_generation=record.journal_generation,
        stream_session_id=record.stream_session_id,
        source_offset=record.event_offset,
    )


def adapt_derived_equity_bar(
    revision: EquityBarRevision,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    instrument_id: int,
) -> RealtimeEquityBarFact:
    if revision.source_kind is not BarSourceKind.DERIVED or not revision.is_final:
        raise ValueError("derived adapter requires a final derived bar")
    return RealtimeEquityBarFact(
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        instrument_id=instrument_id,
        revision=revision,
    )


def adapt_reconciled_equity_bar(
    revision: EquityBarRevision,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    instrument_id: int,
) -> RealtimeEquityBarFact:
    if revision.source_kind is not BarSourceKind.RECONCILED or not revision.is_final:
        raise ValueError("reconciled adapter requires a final reconciled bar")
    return RealtimeEquityBarFact(
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        instrument_id=instrument_id,
        revision=revision,
    )