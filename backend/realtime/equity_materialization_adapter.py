from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import UUID

from equity.domain import SecurityReferenceRevision
from equity.materialization import MaterializationResult, materialize_equity_evidence

from .contracts import canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact
from .preflight import ProviderTier


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class RealtimeEquityMaterializationResult:
    cohort_id: UUID
    provider_tier: ProviderTier
    instrument_id: int
    interval: str
    bar_revision_ids: tuple[UUID, ...]
    confirmation_bar_revision_ids: tuple[UUID, ...]
    input_sha256: str
    materialization: MaterializationResult


def materialize_realtime_equity_evidence(
    *,
    analysis_run_id: UUID,
    security: SecurityReferenceRevision,
    bars: Sequence[RealtimeEquityBarFact],
    confirmation_bars: Sequence[RealtimeEquityBarFact] = (),
    observed_at: datetime,
    fundamental_metrics: Mapping[str, Any] | None = None,
    fundamental_report_ids: Sequence[UUID] = (),
    robust_qualifications: Mapping[
        tuple[str, str, str | None, int | None], UUID
    ] | None = None,
    level_directions: Sequence[int] = (),
) -> RealtimeEquityMaterializationResult:
    rows = tuple(bars)
    confirmations = tuple(confirmation_bars)
    if not rows:
        raise ValueError("realtime equity materialization requires bars")
    identities = {
        (row.cohort_id, row.provider_tier, row.instrument_id)
        for row in (*rows, *confirmations)
    }
    if len(identities) != 1:
        raise ValueError("realtime equity materialization cannot mix cohort/tier/instrument")
    cohort_id, provider_tier, instrument_id = next(iter(identities))
    revisions = tuple(sorted((row.revision for row in rows), key=lambda row: row.bar_start))
    confirmation_revisions = tuple(
        sorted((row.revision for row in confirmations), key=lambda row: row.bar_start)
    )
    interval = revisions[0].interval
    if any(
        row.security_id != security.security_id
        or row.ticker != security.ticker
        or row.interval != interval
        or not row.is_final
        for row in revisions
    ):
        raise ValueError("realtime equity bars do not match the materialization security")
    observed = _utc(observed_at, "observed_at")
    if revisions[-1].bar_end > observed:
        raise ValueError("realtime equity materialization cannot use an unfinished window")
    input_identity = {
        "cohort_id": cohort_id,
        "provider_tier": provider_tier,
        "instrument_id": instrument_id,
        "security_revision_id": security.security_revision_id,
        "interval": interval,
        "bar_revisions": tuple(
            (row.bar_revision_id, row.payload_sha256) for row in revisions
        ),
        "confirmation_revisions": tuple(
            (row.bar_revision_id, row.payload_sha256)
            for row in confirmation_revisions
        ),
        "observed_at": observed,
    }
    materialization = materialize_equity_evidence(
        analysis_run_id=analysis_run_id,
        security=security,
        interval=interval,
        bars=revisions,
        confirmation_bars=confirmation_revisions,
        observed_at=observed,
        fundamental_metrics=fundamental_metrics,
        fundamental_report_ids=fundamental_report_ids,
        robust_qualifications=robust_qualifications,
        level_directions=level_directions,
    )
    primary_ids = tuple(row.bar_revision_id for row in revisions)
    confirmation_ids = tuple(row.bar_revision_id for row in confirmation_revisions)
    if interval == "30m" and not confirmation_ids:
        derived_ids = next((tuple(row.source_revision_ids) for row in materialization.evidence
                            if row.source_name == "EMA_CONFIRMATION"), ())
        if derived_ids and derived_ids != primary_ids:
            by_id = {row.bar_revision_id: row for row in revisions}
            if len(set(derived_ids)) != len(derived_ids) or any(value not in by_id for value in derived_ids):
                raise ValueError("derived confirmation lineage is outside primary bars")
            confirmation_ids = derived_ids
            input_identity["derived_confirmation_revisions"] = tuple(
                (value, by_id[value].payload_sha256) for value in derived_ids
            )
    return RealtimeEquityMaterializationResult(
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        instrument_id=instrument_id,
        interval=interval,
        bar_revision_ids=primary_ids,
        confirmation_bar_revision_ids=confirmation_ids,
        input_sha256=canonical_sha256(input_identity),
        materialization=materialization,
    )