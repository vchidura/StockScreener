from __future__ import annotations

from datetime import datetime
from typing import Sequence
from uuid import UUID

from equity.derivation import derive_canonical_bars, derive_five_minute_bars, derive_minute_bars
from equity.domain import SecurityReferenceRevision

from .equity_bar_adapter import RealtimeEquityBarFact, adapt_derived_equity_bar


REALTIME_DERIVATION_SOURCES = {
    "5m": ("1m", False),
    "15m": ("5m", False),
    "30m": ("5m", False),
    "1h": ("5m", False),
    "1d": ("5m", False),
    "1wk": ("1d", True),
    "1mo": ("1d", True),
}


def derive_realtime_interval_facts(
    security: SecurityReferenceRevision,
    source_facts: Sequence[RealtimeEquityBarFact],
    *,
    target_interval: str,
    observed_at: datetime,
    ingestion_segment_id: UUID,
) -> tuple[RealtimeEquityBarFact, ...]:
    source_contract = REALTIME_DERIVATION_SOURCES.get(target_interval)
    if source_contract is None:
        raise ValueError(f"unsupported realtime derived interval: {target_interval}")
    source_interval, adjusted = source_contract
    candidates = tuple(
        fact for fact in source_facts
        if fact.revision.interval == source_interval
        and fact.revision.adjusted is adjusted
    )
    if not candidates:
        return ()
    identities = {
        (fact.cohort_id, fact.provider_tier, fact.instrument_id)
        for fact in candidates
    }
    if len(identities) != 1:
        raise ValueError("realtime derivation cannot mix cohort, tier or instrument")
    if any(
        fact.revision.security_id != security.security_id
        or fact.revision.ticker != security.ticker
        for fact in candidates
    ):
        raise ValueError("realtime derivation source identity changed")
    cohort_id, provider_tier, instrument_id = next(iter(identities))
    revisions = tuple(fact.revision for fact in candidates)
    derive = (
        derive_minute_bars
        if source_interval == "1m"
        else derive_five_minute_bars
        if source_interval == "5m"
        else derive_canonical_bars
    )
    derived = derive(
            security,
            revisions,
            target_interval=target_interval,
            observed_at=observed_at,
            ingestion_segment_id=ingestion_segment_id,
    )
    return tuple(
        adapt_derived_equity_bar(
            revision,
            cohort_id=cohort_id,
            provider_tier=provider_tier,
            instrument_id=instrument_id,
        )
        for revision in derived
    )
