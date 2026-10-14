from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping
from uuid import UUID

import pandas as pd

from options.alert_qualification import (
    AlertInput,
    AlertInputEvidence,
    qualify_option_alert as _qualify_option_alert,
)
from options.credit_detection import assess_o3_credit as _assess_o3_credit
from options.dual_origin import (
    ActivitySource,
    StockFirstPolicy,
    assess_stock_first as _assess_stock_first,
    detect_option_participation as _detect_option_participation,
)
from options.strategies.domain import StructureType
from options.surface_detection import assess_surface_first as _assess_surface_first
from options.stock_bar_detection import detect_canonical_stock_signals
from research.stock_idea_forward import (
    alert_selection_cohort as _alert_selection_cohort,
    decision_candidate as _decision_candidate,
)
from research.stock_idea_swing import (
    bind_swing_candidate as _bind_swing_candidate,
    preconfirmation_path as _preconfirmation_path,
)
from research.strategy_v2 import StrategyConfig, detect as _detect_stock_intraday

from .preflight import ProviderTier


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class StableDetectorAdapterResult:
    adapter_id: str
    cohort_id: UUID
    provider_tier: ProviderTier
    input_sha256: str
    status: str
    output: Any
    reason_codes: tuple[str, ...] = ()
    publication_permission: bool = False
    execution_permission: bool = False


def _result(
    adapter_id: str,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    output: Any,
    *,
    status: str = "EXECUTED",
    reason_codes: tuple[str, ...] = (),
) -> StableDetectorAdapterResult:
    if not adapter_id.strip():
        raise ValueError("adapter ID cannot be blank")
    if not isinstance(provider_tier, ProviderTier):
        raise TypeError("provider tier must be a ProviderTier")
    if not _SHA256_PATTERN.fullmatch(input_sha256):
        raise ValueError("adapter input SHA-256 is invalid")
    if status not in {"EXECUTED", "UNAVAILABLE"}:
        raise ValueError("unsupported adapter status")
    return StableDetectorAdapterResult(
        adapter_id=adapter_id,
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        input_sha256=input_sha256,
        status=status,
        output=output,
        reason_codes=reason_codes,
    )


def run_stock_intraday_adapter(
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    frame: pd.DataFrame,
    config: StrategyConfig,
) -> StableDetectorAdapterResult:
    return _result(
        "stock-intraday-quality-v2",
        cohort_id,
        provider_tier,
        input_sha256,
        _detect_stock_intraday(frame, config),
    )


def run_stock_forward_adapter(
    candidate,
    state: dict,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    boundary: datetime,
    cutoff: datetime,
    actual_time: datetime,
    config: dict,
) -> StableDetectorAdapterResult:
    output = _decision_candidate(
        candidate,
        state,
        boundary=boundary,
        cutoff=cutoff,
        actual_time=actual_time,
        config=config,
    )
    return _result(
        "stock-intraday-quality-v2",
        cohort_id,
        provider_tier,
        input_sha256,
        output,
    )


def run_stock_cohort_adapter(
    state: dict,
    boundary: datetime,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
) -> StableDetectorAdapterResult:
    return _result(
        "stock-intraday-quality-v2",
        cohort_id,
        provider_tier,
        input_sha256,
        _alert_selection_cohort(state, boundary),
    )


def run_stock_swing_adapter(
    daily,
    confirmation,
    cutoff: datetime,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
) -> StableDetectorAdapterResult:
    return _result(
        "stock-swing-v1",
        cohort_id,
        provider_tier,
        input_sha256,
        _bind_swing_candidate(daily, confirmation, cutoff),
    )


def run_stock_swing_path_adapter(
    daily,
    state: dict,
    boundary: datetime,
    cutoff: datetime,
    config: dict,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
) -> StableDetectorAdapterResult:
    return _result(
        "stock-swing-v1",
        cohort_id,
        provider_tier,
        input_sha256,
        _preconfirmation_path(daily, state, boundary, cutoff, config),
    )


def run_o1_adapter(
    source: ActivitySource,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    market_cutoff: datetime,
    decision_at: datetime,
    calendar=None,
) -> StableDetectorAdapterResult:
    return _result(
        "options-o1-participation",
        cohort_id,
        provider_tier,
        input_sha256,
        _detect_option_participation(
            source,
            market_cutoff=market_cutoff,
            decision_at=decision_at,
            calendar=calendar,
        ),
    )


def run_o2_adapter(
    source,
    stock,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    market_cutoff: datetime,
    decision_at: datetime,
    context_until: datetime | None = None,
    event_detail: dict | None = None,
    calendar=None,
) -> StableDetectorAdapterResult:
    if provider_tier is ProviderTier.MASSIVE_DEVELOPER:
        return _result(
            "options-o2-surface",
            cohort_id,
            provider_tier,
            input_sha256,
            None,
            status="UNAVAILABLE",
            reason_codes=("DETECTOR_DISABLED_FOR_TIER",),
        )
    output = _assess_surface_first(
        source,
        stock,
        market_cutoff=market_cutoff,
        decision_at=decision_at,
        context_until=context_until,
        event_detail=event_detail,
        calendar=calendar,
    )
    return _result(
        "options-o2-surface",
        cohort_id,
        provider_tier,
        input_sha256,
        output,
    )


def run_o3_adapter(
    candidate,
    decision,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    activity_contract_id: int,
    snapshots: tuple,
    structural_invalidation: Decimal,
    scheduled_cycle: datetime,
) -> StableDetectorAdapterResult:
    output = _assess_o3_credit(
        candidate,
        decision,
        activity_contract_id=activity_contract_id,
        snapshots=snapshots,
        structural_invalidation=structural_invalidation,
        scheduled_cycle=scheduled_cycle,
    )
    return _result(
        "options-o3-credit",
        cohort_id,
        provider_tier,
        input_sha256,
        output,
    )


def run_stock_first_adapter(
    setup,
    source: ActivitySource | None,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    policy: StockFirstPolicy,
    trusted_source_policy,
    market_cutoff: datetime,
    decision_at: datetime,
    calendar=None,
) -> StableDetectorAdapterResult:
    output = _assess_stock_first(
        setup,
        source,
        policy=policy,
        trusted_source_policy=trusted_source_policy,
        market_cutoff=market_cutoff,
        decision_at=decision_at,
        calendar=calendar,
    )
    return _result(
        "options-s1-s2-stock-first",
        cohort_id,
        provider_tier,
        input_sha256,
        output,
    )


def run_canonical_stock_signal_adapter(
    *,
    detector_id: str,
    security_id: UUID,
    underlyer: str,
    bars: tuple,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
    market_cutoff: datetime,
    decision_at: datetime,
    session_close: datetime,
) -> StableDetectorAdapterResult:
    if detector_id not in {"S1", "S2"}:
        raise ValueError("canonical stock adapter requires S1 or S2")
    signals = detect_canonical_stock_signals(
        security_id=security_id,
        underlyer=underlyer,
        bars=bars,
        market_cutoff=market_cutoff,
        decision_at=decision_at,
        session_close=session_close,
    )
    return _result(
        "canonical-stock-bar-signal-v1",
        cohort_id,
        provider_tier,
        input_sha256,
        tuple(signal for signal in signals if signal.detector_id == detector_id),
    )


def run_option_qualification_adapter(
    strategy: str,
    structure: StructureType,
    evidence: Mapping[AlertInput, AlertInputEvidence],
    decision_at: datetime,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    input_sha256: str,
) -> StableDetectorAdapterResult:
    return _result(
        "options-alert-qualification",
        cohort_id,
        provider_tier,
        input_sha256,
        _qualify_option_alert(strategy, structure, evidence, decision_at),
    )