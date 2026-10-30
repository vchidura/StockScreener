"""Canonical OS1/OS2 evaluator binding over Advanced Equity bar inputs."""
from __future__ import annotations

from datetime import datetime
from typing import Callable, Mapping
from uuid import UUID

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)
from options.stock_bar_detection import (
    ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY_SHA256,
)

from .advanced_equity_detector_consumer import (
    AdvancedEquityDetectorConsumerConfig,
    AdvancedEquityDetectorDecision,
    AdvancedEquityDetectorSourceRecord,
)
from .advanced_equity_detector_shadow import detector_shadow_input_sha256
from .detector_adapters import run_canonical_stock_signal_adapter
from .detector_evidence import RealtimeDetectorInput
from .preflight import ProviderTier


DETECTORS = ("OS1", "OS2")
DETECTOR_VERSION = "advanced-options-canonical-stock-signal-shadow-v1"
PAYLOAD_SCHEMA = "advanced_options_canonical_stock_signal_shadow_v1"


def _uuid_array(value) -> tuple[UUID, ...]:
    if value is None:
        return ()
    values = (
        tuple(item for item in value.strip("{}").split(",") if item)
        if isinstance(value, str)
        else tuple(value)
    )
    return tuple(UUID(str(item)) for item in values)


def advanced_options_bar_revision_from_row(
    row,
) -> tuple[EquityBarRevision, datetime]:
    if isinstance(row, Mapping):
        row = tuple(row.values())
    return (
        EquityBarRevision(
            bar_revision_id=UUID(str(row[0])),
            security_id=UUID(str(row[1])),
            ticker=str(row[2]),
            interval=str(row[3]),
            session_date=row[4],
            bar_start=row[5],
            bar_end=row[6],
            open_price=row[7],
            high_price=row[8],
            low_price=row[9],
            close_price=row[10],
            volume=row[11],
            vwap=row[12],
            transaction_count=row[13],
            source_kind=BarSourceKind(str(row[14])),
            availability_mode=BarAvailabilityMode(str(row[15])),
            is_final=bool(row[16]),
            system_observed_at=row[17],
            replay_available_at=row[18],
            adjusted=str(row[19]) == "ADJUSTED",
            payload_sha256=str(row[20]),
            quality_codes=tuple(row[21] or ()),
            provider_published_at=row[22],
            ingestion_segment_id=(
                None if row[23] is None else UUID(str(row[23]))
            ),
            source_bar_revision_ids=_uuid_array(row[24]),
            supersedes_bar_revision_id=(
                None if row[25] is None else UUID(str(row[25]))
            ),
            session_scope=BarSessionScope(str(row[26])),
        ),
        row[27],
    )


def _load_bars_cursor(
    cursor,
    record: AdvancedEquityDetectorSourceRecord,
) -> tuple[tuple[EquityBarRevision, datetime], ...]:
    columns = (
        "revision.bar_revision_id,revision.security_id,revision.ticker,"
        "revision.interval,revision.session_date,revision.bar_start,"
        "revision.bar_end,revision.open_price,revision.high_price,"
        "revision.low_price,revision.close_price,revision.volume,"
        "revision.vwap,revision.transaction_count,revision.source_kind,"
        "revision.availability_mode,revision.is_final,revision.observed_at,"
        "revision.replay_available_at,revision.adjustment_basis,"
        "revision.payload_sha256,revision.quality_codes,"
        "revision.provider_published_at,revision.ingestion_segment_id,"
        "revision.source_revision_ids,revision.supersedes_revision_id,"
        "revision.session_scope,revision.created_at "
    )
    cursor.execute(
        "SELECT " + columns
        + "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "AND revision.cohort_id=current.cohort_id "
        "AND revision.provider_tier=current.provider_tier "
        "AND revision.instrument_id=current.instrument_id "
        "AND revision.interval=current.interval "
        "AND revision.bar_start=current.bar_start "
        "AND revision.session_scope=current.session_scope "
        "AND revision.adjustment_basis=current.adjustment_basis "
        "LEFT JOIN rt.cohort_history_bindings AS binding "
        "ON binding.target_cohort_id=%s "
        "WHERE ((revision.cohort_id=%s "
        "AND revision.availability_mode='LIVE_OBSERVED') "
        "OR (revision.cohort_id=binding.source_cohort_id "
        "AND revision.provider_tier=binding.source_provider_tier "
        "AND binding.created_at<=%s "
        "AND revision.bar_end<binding.history_before "
        "AND revision.availability_mode='HISTORICAL_RECONSTRUCTED')) "
        "AND revision.security_id=%s "
        "AND revision.interval='1d' AND revision.is_final "
        "AND revision.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.session_scope='RTH' "
        "AND revision.session_date<%s "
        "AND revision.observed_at<=%s AND revision.created_at<=%s "
        "AND (revision.replay_available_at IS NULL "
        "OR revision.replay_available_at<=%s) "
        "AND (revision.provider_published_at IS NULL "
        "OR revision.provider_published_at<=%s) "
        "ORDER BY revision.bar_end DESC LIMIT 15",
        (
            str(record.cohort_id),
            str(record.cohort_id),
            record.observed_at,
            str(record.security_id),
            record.bar_end.date(),
            record.observed_at,
            record.observed_at,
            record.observed_at,
            record.observed_at,
        ),
    )
    daily = tuple(reversed(tuple(
        advanced_options_bar_revision_from_row(row)
        for row in cursor.fetchall()
    )))
    cursor.execute(
        "SELECT " + columns
        + "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "AND revision.cohort_id=current.cohort_id "
        "AND revision.provider_tier=current.provider_tier "
        "AND revision.instrument_id=current.instrument_id "
        "AND revision.interval=current.interval "
        "AND revision.bar_start=current.bar_start "
        "AND revision.session_scope=current.session_scope "
        "AND revision.adjustment_basis=current.adjustment_basis "
        "WHERE revision.cohort_id=%s AND revision.security_id=%s "
        "AND revision.interval='5m' AND revision.is_final "
        "AND revision.availability_mode='LIVE_OBSERVED' "
        "AND revision.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.session_scope='RTH' "
        "AND revision.session_date=%s AND revision.bar_end<=%s "
        "ORDER BY revision.bar_end DESC LIMIT 3",
        (
            str(record.cohort_id),
            str(record.security_id),
            record.bar_end.date(),
            record.bar_end,
        ),
    )
    intraday = tuple(
        reversed(tuple(
            advanced_options_bar_revision_from_row(row)
            for row in cursor.fetchall()
        ))
    )
    return (*daily, *intraday)


def _retained_bar_input(
    revision: EquityBarRevision,
    created_at: datetime,
) -> RealtimeDetectorInput:
    retained = {
        "bar_revision_id": str(revision.bar_revision_id),
        "ticker": revision.ticker,
        "interval": revision.interval,
        "session_date": revision.session_date.isoformat(),
        "bar_start": revision.bar_start.isoformat(),
        "bar_end": revision.bar_end.isoformat(),
        "open": str(revision.open_price),
        "high": str(revision.high_price),
        "low": str(revision.low_price),
        "close": str(revision.close_price),
        "volume": str(revision.volume),
        "vwap": None if revision.vwap is None else str(revision.vwap),
        "transaction_count": revision.transaction_count,
        "payload_sha256": revision.payload_sha256,
        "quality_codes": list(revision.quality_codes),
        "created_at": created_at.isoformat(),
    }
    return RealtimeDetectorInput(
        input_role="HISTORY_BAR",
        input_key=str(revision.bar_revision_id),
        source_kind="BAR_REVISION",
        input_sha256=revision.payload_sha256,
        available_at=revision.bar_end,
        received_at=max(revision.system_observed_at, created_at),
        retained_input=retained,
        bar_revision_id=revision.bar_revision_id,
    )


def os1_os2_evaluator_cursor(
    cursor,
    *,
    detector_id: str,
    session_close: datetime,
    decision_at: Callable[[], datetime],
) -> Callable[
    [AdvancedEquityDetectorSourceRecord, tuple[RealtimeDetectorInput, ...]],
    AdvancedEquityDetectorDecision,
]:
    if detector_id not in DETECTORS:
        raise ValueError("Advanced Options stock-bar detector must be OS1 or OS2")

    def evaluate(
        record: AdvancedEquityDetectorSourceRecord,
        source_inputs: tuple[RealtimeDetectorInput, ...],
    ) -> AdvancedEquityDetectorDecision:
        bars = _load_bars_cursor(cursor, record)
        history_inputs = tuple(
            _retained_bar_input(revision, created_at)
            for revision, created_at in bars
            if revision.bar_revision_id != record.bar_revision_id
        )
        inputs = (*source_inputs, *history_inputs)
        adapter = run_canonical_stock_signal_adapter(
            detector_id=detector_id,
            security_id=record.security_id,
            underlyer=record.ticker,
            bars=bars,
            cohort_id=record.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            input_sha256=detector_shadow_input_sha256(inputs),
            market_cutoff=record.bar_end,
            decision_at=decision_at(),
            session_close=session_close,
        )
        signals = tuple(adapter.output)
        if len(signals) > 1:
            raise ValueError("canonical stock detector returned multiple signals")
        signal = signals[0] if signals else None
        payload = (
            signal.model_dump(mode="json")
            if signal is not None
            else {
                "detector_id": detector_id,
                "ticker": record.ticker,
                "market_cutoff": record.bar_end.isoformat(),
                "disposition": "COMPLETE_ZERO",
            }
        )
        normalized_adapter = type(adapter)(
            adapter_id=adapter.adapter_id,
            cohort_id=adapter.cohort_id,
            provider_tier=adapter.provider_tier,
            input_sha256=adapter.input_sha256,
            status=adapter.status,
            output=payload,
            reason_codes=adapter.reason_codes,
        )
        disposition = (
            signal.disposition if signal is not None else "COMPLETE_ZERO"
        )
        return AdvancedEquityDetectorDecision(
            adapter=normalized_adapter,
            trigger_identity=(
                f"{detector_id}:{record.security_id}:"
                f"{record.bar_revision_id}"
            ),
            valid_until=(
                None if signal is None else signal.valid_until
            ),
            disposition=disposition,
            direction=None if signal is None else signal.direction,
            reason_codes=adapter.reason_codes,
            payload_schema=PAYLOAD_SCHEMA,
            payload=payload,
            state_key=f"{detector_id}:{record.security_id}",
            state_revision=record.source_offset,
            state_payload={
                "detector_id": detector_id,
                "last_bar_revision_id": str(record.bar_revision_id),
                "last_signal_id": (
                    None if signal is None else str(signal.signal_id)
                ),
                "disposition": disposition,
            },
            additional_inputs=history_inputs,
        )

    return evaluate


def os1_os2_requirements(detector_id: str) -> dict[str, object]:
    if detector_id not in DETECTORS:
        raise ValueError("Advanced Options stock-bar detector must be OS1 or OS2")
    return {
        "detector_id": detector_id,
        "policy_sha256": ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY_SHA256,
        "trigger_interval": "5m",
        "daily_history_bars": 15,
        "intraday_history_bars": 2 if detector_id == "OS1" else 3,
        "completed_bars_only": True,
        "execution_permission": False,
    }


def build_os1_os2_consumer_config(
    *,
    detector_id: str,
    cohort_id: UUID,
    journal_generation: UUID,
    instrument_id: int,
    shard_map_sha256: str,
    owner_id: UUID,
    owner_epoch: int,
    lease_resource_key: str,
    code_sha256: str,
    readiness_epoch: int,
) -> AdvancedEquityDetectorConsumerConfig:
    requirements = os1_os2_requirements(detector_id)
    return AdvancedEquityDetectorConsumerConfig(
        cohort_id=cohort_id,
        journal_generation=journal_generation,
        instrument_id=instrument_id,
        shard_id=0,
        shard_map_sha256=shard_map_sha256,
        owner_id=owner_id,
        owner_epoch=owner_epoch,
        lease_resource_key=lease_resource_key,
        detector_id=f"ADVANCED_OPTIONS_{detector_id}",
        detector_version=DETECTOR_VERSION,
        code_sha256=code_sha256,
        policy_sha256=ADVANCED_OPTIONS_STOCK_BAR_SIGNAL_POLICY_SHA256,
        requirements=requirements,
        output_kind="SIGNAL",
        readiness_epoch=readiness_epoch,
        interval="5m",
    )
