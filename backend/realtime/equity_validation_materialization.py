from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import EvidenceType, SecurityReferenceRevision
from equity.materialization import FEATURE_VERSION

from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256
from .equity_evidence_repository import (
    RealtimeFeatureDefinition,
    persist_realtime_equity_materialization_cursor,
)
from .equity_materialization_adapter import (
    RealtimeEquityMaterializationResult,
    materialize_realtime_equity_evidence,
)
from .equity_validation_history import EquityValidationHistoryPackage
from .portal_projection import (
    RealtimePortalSnapshot,
    RealtimeScreeningGeneration,
    build_portal_snapshot,
    build_screening_generation,
)
from .portal_projection_repository import (
    persist_portal_snapshot_cursor,
    persist_screening_generation_cursor,
    publish_portal_release_cursor,
)
from .preflight import ProviderTier


@dataclass(frozen=True, slots=True)
class EquityValidationComputedValues:
    features: tuple[RealtimeEquityMaterializationResult, ...]
    screening: RealtimeScreeningGeneration
    portal_snapshots: tuple[RealtimePortalSnapshot, ...]


@dataclass(frozen=True, slots=True)
class EquityValidationStoredValues:
    feature_snapshot_ids: tuple[UUID, ...]
    screening_generation_id: UUID
    portal_snapshot_ids: tuple[UUID, ...]


def equity_validation_feature_definition(
    backend_dir: Path,
    *,
    history_policy_sha256: str,
    interval: str = "30m",
) -> RealtimeFeatureDefinition:
    source_contracts = {
        "30m": ("RAW_ACTION_GATED", "1h", "DERIVED_SESSION_HOURLY"),
        "1h": ("RAW_ACTION_GATED", "1d", "RAW_DERIVED_FROM_30M"),
        "1d": ("PROVIDER_SPLIT_ADJUSTED", "1h", "RAW_ACTION_GATED"),
    }
    if interval not in source_contracts:
        raise ValueError("equity validation feature interval is unsupported")
    primary_basis, confirmation_interval, confirmation_basis = source_contracts[interval]
    materializer_path = backend_dir / "equity" / "materialization.py"
    code_sha256 = hashlib.sha256(materializer_path.read_bytes()).hexdigest()
    dependency_spec = {
        "version": "equity-validation-feature-input-v2",
        "manifest_kind": "EQUITY_VALIDATION",
        "universe_sha256": EQUITY_VALIDATION_UNIVERSE.sha256,
        "option_underlyings": (),
        "primary_interval": interval,
        "primary_price_basis": primary_basis,
        "confirmation_interval": confirmation_interval,
        "confirmation_price_basis": confirmation_basis,
        "history_policy_sha256": history_policy_sha256,
    }
    policy_sha256 = canonical_sha256(dependency_spec)
    bundle_sha256 = canonical_sha256({
        "bundle_version": FEATURE_VERSION,
        "feature_schema_version": "1.0",
        "dependency_spec": dependency_spec,
        "policy_sha256": policy_sha256,
        "code_sha256": code_sha256,
    })
    return RealtimeFeatureDefinition(
        bundle_sha256=bundle_sha256,
        bundle_version=FEATURE_VERSION,
        feature_schema_version="1.0",
        dependency_spec=dependency_spec,
        policy_sha256=policy_sha256,
        code_sha256=code_sha256,
    )


def produce_equity_validation_values(
    *,
    cohort_id: UUID,
    source_manifest_sha256: str,
    source_generation: int,
    checked_at: datetime,
    history: EquityValidationHistoryPackage,
    securities: Mapping[str, SecurityReferenceRevision],
) -> EquityValidationComputedValues:
    tickers = EQUITY_VALIDATION_UNIVERSE.equity_tickers
    if set(securities) != set(tickers):
        raise ValueError("equity validation securities must cover the exact scope")
    features = []
    screening_rows = []
    ticker_snapshots = []
    stock_observations = []
    for ticker in tickers:
        ticker_facts = tuple(
            row for row in history.facts if row.revision.ticker == ticker
        )
        adjusted_daily = tuple(
            row for row in ticker_facts
            if row.revision.interval == "1d" and row.revision.adjusted
        )
        raw_daily = tuple(
            row for row in ticker_facts
            if row.revision.interval == "1d" and not row.revision.adjusted
        )
        hourly = tuple(
            row for row in ticker_facts
            if row.revision.interval == "1h" and not row.revision.adjusted
        )
        thirty = tuple(
            row for row in ticker_facts
            if row.revision.interval == "30m" and not row.revision.adjusted
        )
        interval_inputs = {
            "30m": (thirty, ()),
            "1h": (hourly, raw_daily),
            "1d": (adjusted_daily, hourly),
        }
        ticker_features = {}
        technical_intervals = {}
        for interval, (primary, confirmation) in interval_inputs.items():
            analysis_run_id = uuid5(
                NAMESPACE_URL,
                f"equity-validation-feature:{cohort_id}:{ticker}:{interval}:"
                f"{source_manifest_sha256}:{source_generation}:{history.evidence_sha256}",
            )
            feature = materialize_realtime_equity_evidence(
                analysis_run_id=analysis_run_id,
                security=securities[ticker],
                bars=primary,
                confirmation_bars=confirmation,
                observed_at=checked_at,
            )
            features.append(feature)
            ticker_features[interval] = feature
            feature_evidence = next(
                row
                for row in feature.materialization.evidence
                if row.evidence_type is EvidenceType.FEATURE_SNAPSHOT
            )
            technical_intervals[interval] = json.loads(feature_evidence.payload_json)
        technicals = technical_intervals["30m"]
        detectors = {
            "S1": {
                "status": "UNAVAILABLE",
                "reason_codes": ["HISTORICAL_BOOTSTRAP_NO_LIVE_BARS"],
            },
            "S2": {
                "status": "UNAVAILABLE",
                "reason_codes": ["HISTORICAL_BOOTSTRAP_NO_LIVE_BARS"],
            },
            "O1": {
                "status": "UNAVAILABLE",
                "reason_codes": ["EQUITY_VALIDATION_NO_OPTIONS"],
            },
            "O2": {
                "status": "UNAVAILABLE",
                "reason_codes": ["EQUITY_VALIDATION_NO_OPTIONS"],
            },
            "O3": {
                "status": "UNAVAILABLE",
                "reason_codes": ["EQUITY_VALIDATION_NO_OPTIONS"],
            },
        }
        payload = {
            "ticker": ticker,
            "technicals": technicals,
            "technical_intervals": technical_intervals,
            "detectors": detectors,
            "validation_scope": "EQUITY_ONLY",
            "split_safe": True,
            "adjusted_discontinuities": [
                {
                    "session_date": row.session_date.isoformat(),
                    "return_fraction": row.return_fraction,
                    "split_coincident": row.split_coincident,
                }
                for row in history.adjusted_discontinuities
                if row.ticker == ticker
            ],
            "provider_fetch_performed": False,
        }
        source_manifest = {
            "source_manifest_sha256": source_manifest_sha256,
            "history_evidence_sha256": history.evidence_sha256,
            "split_action_count": sum(
                row.ticker == ticker for row in history.split_actions
            ),
            "split_coincident_discontinuity_count": sum(
                row.ticker == ticker and row.split_coincident
                for row in history.adjusted_discontinuities
            ),
            "feature_input_sha256s": {
                interval: ticker_features[interval].input_sha256
                for interval in ("30m", "1h", "1d")
            },
            "bar_revision_ids": {
                interval: [
                    str(value) for value in ticker_features[interval].bar_revision_ids
                ]
                for interval in ("30m", "1h", "1d")
            },
        }
        ticker_snapshots.append(build_portal_snapshot(
            cohort_id=cohort_id,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            snapshot_type="TICKER_PAGE",
            snapshot_key=ticker,
            source_generation=source_generation,
            source_manifest=source_manifest,
            payload=payload,
            generated_at=checked_at,
        ))
        screening_rows.append((
            securities[ticker].security_id,
            ticker,
            False,
            payload,
        ))
        for detector_id in ("S1", "S2"):
            stock_observations.append({
                "ticker": ticker,
                "detector_id": detector_id,
                "selected": False,
                "payload": detectors[detector_id],
            })
    screening = build_screening_generation(
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        session_date=checked_at.date(),
        expected_members=len(tickers),
        generated_at=checked_at,
        manifest={
            "source_manifest_sha256": source_manifest_sha256,
            "source_generation": source_generation,
            "manifest_kind": "EQUITY_VALIDATION",
        },
        rows=tuple(screening_rows),
    )
    context = build_portal_snapshot(
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type="STOCK_ALERT_CONTEXT",
        snapshot_key="PLATFORM",
        source_generation=source_generation,
        source_manifest={
            "source_manifest_sha256": source_manifest_sha256,
            "history_evidence_sha256": history.evidence_sha256,
        },
        payload={
            "schedule_streams": [],
            "sessions": [checked_at.date().isoformat()],
            "observations": stock_observations,
            "validation_scope": "EQUITY_ONLY",
            "provider_fetch_performed": False,
        },
        generated_at=checked_at,
    )
    return EquityValidationComputedValues(
        features=tuple(features),
        screening=screening,
        portal_snapshots=(*ticker_snapshots, context),
    )


def persist_equity_validation_values(
    compute_database,
    publication_database,
    values: EquityValidationComputedValues,
    definitions: Mapping[str, RealtimeFeatureDefinition],
    *,
    generation: int,
    receipt=None,
    release_repository=publish_portal_release_cursor,
) -> EquityValidationStoredValues:
    with compute_database.transaction() as cursor:
        feature_ids = tuple(
            persist_realtime_equity_materialization_cursor(
                cursor,
                feature,
                definitions[feature.interval],
                generation=generation,
            )
            for feature in values.features
        )
        persist_screening_generation_cursor(cursor, values.screening)
    with publication_database.transaction() as cursor:
        for snapshot in values.portal_snapshots:
            persist_portal_snapshot_cursor(cursor, snapshot)
        if receipt is not None:
            release_repository(cursor, receipt, values.portal_snapshots)
    return EquityValidationStoredValues(
        feature_snapshot_ids=feature_ids,
        screening_generation_id=values.screening.generation_id,
        portal_snapshot_ids=tuple(row.snapshot_id for row in values.portal_snapshots),
    )