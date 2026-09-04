"""Resolve point-in-time equity evidence into one option-facing context."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .domain import (
    ContextStatus,
    DecisionWatermark,
    EquityContextSnapshot,
    EquityEvidence,
    EvidenceRole,
    EvidenceType,
    LifecycleStatus,
    QualityState,
    SecurityReferenceRevision,
)
from .polygon import canonical_json, sha256_json


def build_equity_context(
    *,
    security: SecurityReferenceRevision,
    strategy_horizon: str,
    watermark: DecisionWatermark,
    evidence: Iterable[EquityEvidence],
    robust_qualification_ids: frozenset[UUID],
    context_policy_version: str,
    context_policy_sha256: str,
    universe_run_id: UUID | None = None,
    use_security_reference_metrics: bool = True,
) -> tuple[EquityContextSnapshot, tuple[tuple[UUID, EvidenceRole], ...]]:
    visible = tuple(
        row for row in evidence
        if row.ticker == security.ticker
        and row.market_time <= watermark.market_time
        and row.observed_at <= watermark.observed_time
        and (row.valid_until is None or row.valid_until > watermark.market_time)
        and row.quality_state not in (QualityState.FAILED, QualityState.STALE)
    )
    latest_by_type: dict[EvidenceType, EquityEvidence] = {}
    for row in sorted(visible, key=lambda item: (item.market_time, item.observed_at)):
        latest_by_type[row.evidence_type] = row

    feature = latest_by_type.get(EvidenceType.FEATURE_SNAPSHOT)
    fundamental = latest_by_type.get(EvidenceType.FUNDAMENTAL_SNAPSHOT)
    range_forecast = latest_by_type.get(EvidenceType.RANGE_FORECAST)
    setup = latest_by_type.get(EvidenceType.TRADE_SETUP)
    feature_payload = _payload(feature)
    fundamental_payload = _payload(fundamental)
    setup_payload = _payload(setup)
    range_payload = _payload(range_forecast)

    qualified = [
        row for row in visible
        if row.evidence_role is EvidenceRole.DIRECTION
        and row.direction in (-1, 1)
        and row.qualification_revision_id in robust_qualification_ids
    ]
    qualified.sort(key=lambda row: (row.market_time, row.observed_at), reverse=True)
    direction_values = {row.direction for row in qualified}
    direction_evidence = qualified[0] if len(direction_values) == 1 else None
    qualified_direction = (
        "BULLISH" if direction_evidence and direction_evidence.direction == 1
        else "BEARISH" if direction_evidence else None
    )

    trigger_rows = [
        row for row in visible
        if row.evidence_role is EvidenceRole.TRIGGER
        and row.lifecycle_status in (LifecycleStatus.AT_EDGE, LifecycleStatus.CONFIRMED)
    ]
    trigger_rows.sort(key=lambda row: (row.market_time, row.observed_at), reverse=True)
    trigger = trigger_rows[0] if trigger_rows else None
    conflict_reasons = []
    advisory_conflicts = []
    if len(direction_values) > 1:
        conflict_reasons.append("QUALIFIED_DIRECTION_CONFLICT")
    if setup and setup.lifecycle_status is LifecycleStatus.CONFLICTED:
        if setup.qualification_revision_id in robust_qualification_ids:
            conflict_reasons.append("QUALIFIED_SETUP_CONFLICT")
        else:
            advisory_conflicts.append("UNQUALIFIED_SETUP_CONFLICT")

    missing = []
    if feature is None:
        missing.append("FEATURE_CONTEXT_UNAVAILABLE")
    if direction_evidence is None and not conflict_reasons:
        missing.append("QUALIFIED_DIRECTION_UNAVAILABLE")
    status = (
        ContextStatus.CONFLICTED if conflict_reasons
        else ContextStatus.UNAVAILABLE if feature is None
        else ContextStatus.DEGRADED if missing or advisory_conflicts
        else ContextStatus.COMPLETE
    )
    reasons = tuple(dict.fromkeys((*conflict_reasons, *advisory_conflicts, *missing)))
    links = tuple(
        (row.evidence_id, row.evidence_role)
        for row in sorted(visible, key=lambda item: (item.evidence_role.value, item.evidence_id.hex))
    )
    link_ids = [str(evidence_id) for evidence_id, _ in links]
    context_identity = sha256_json({
        "context_policy_sha256": context_policy_sha256,
        "evidence_ids": link_ids,
        "market_time": watermark.market_time.isoformat(),
        "observed_time": watermark.observed_time.isoformat(),
        "strategy_horizon": strategy_horizon,
        "ticker": security.ticker,
    })
    context = EquityContextSnapshot(
        equity_context_snapshot_id=uuid5(
            NAMESPACE_URL, f"equity-context:{context_identity}"
        ),
        security_id=security.security_id,
        ticker=security.ticker,
        strategy_horizon=strategy_horizon,
        market_time=watermark.market_time,
        observed_at=watermark.observed_time,
        valid_until=_minimum_valid_until(visible),
        status=status,
        universe_run_id=universe_run_id,
        security_revision_id=security.security_revision_id,
        fundamental_snapshot_id=fundamental.evidence_id if fundamental else None,
        regime_state=_string(feature_payload.get("regime_state")),
        ema_direction=_string(feature_payload.get("ema_direction")),
        qualified_direction=qualified_direction,
        direction_qualification_id=(
            direction_evidence.qualification_revision_id if direction_evidence else None
        ),
        direction_evidence_id=(direction_evidence.evidence_id if direction_evidence else None),
        direction_horizon=(direction_evidence.interval if direction_evidence else None),
        direction_valid_until=(direction_evidence.valid_until if direction_evidence else None),
        trigger_state=(trigger.lifecycle_status.value if trigger else None),
        trigger_valid_until=(trigger.valid_until if trigger else None),
        range_forecast_id=(range_forecast.evidence_id if range_forecast else None),
        range_lower=_decimal(range_payload.get("lower")),
        range_upper=_decimal(range_payload.get("upper")),
        range_valid_until=(range_forecast.valid_until if range_forecast else None),
        market_cap=_decimal(
            fundamental_payload.get("market_cap")
            or (security.market_cap if use_security_reference_metrics else None)
        ),
        shares_outstanding=_decimal(
            fundamental_payload.get("shares_outstanding")
            or (security.weighted_shares if use_security_reference_metrics else None)
        ),
        free_float=_decimal(
            fundamental_payload.get("free_float")
            or (security.free_float if use_security_reference_metrics else None)
        ),
        dividend_yield=_float(fundamental_payload.get("dividend_yield")),
        enterprise_value=_decimal(fundamental_payload.get("enterprise_value")),
        ebitda=_decimal(fundamental_payload.get("ebitda")),
        operating_income=_decimal(fundamental_payload.get("operating_income")),
        free_cash_flow=_decimal(fundamental_payload.get("free_cash_flow")),
        risk_levels_json=canonical_json(setup_payload.get("risk_levels") or {}),
        conflict_state_json=canonical_json({
            "advisory_reasons": advisory_conflicts,
            "reasons": conflict_reasons,
        }),
        stale_components_json=canonical_json([]),
        reason_codes=reasons,
        summary_json=canonical_json({
            "company_name": security.company_name,
            "evidence_count": len(visible),
            "setup_direction": setup_payload.get("direction_state"),
            "trigger_evidence_id": str(trigger.evidence_id) if trigger else None,
        }),
        context_policy_version=context_policy_version,
        context_policy_sha256=context_policy_sha256,
    )
    return context, links


def _payload(evidence: EquityEvidence | None) -> dict[str, Any]:
    return json.loads(evidence.payload_json) if evidence is not None else {}


def _minimum_valid_until(evidence: Iterable[EquityEvidence]) -> datetime | None:
    values = [row.valid_until for row in evidence if row.valid_until is not None]
    return min(values) if values else None


def _string(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    return result if result.is_finite() else None


def _float(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if result == result else None