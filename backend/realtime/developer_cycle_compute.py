from __future__ import annotations

import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Callable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import EvidenceType, SecurityReferenceRevision

from .contracts import CANARY_UNIVERSE, canonical_sha256
from .detector_adapters import StableDetectorAdapterResult
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_evidence_repository import RealtimeFeatureDefinition
from .equity_evidence_repository import persist_realtime_equity_materialization_cursor
from .equity_materialization_adapter import (
    RealtimeEquityMaterializationResult,
    materialize_realtime_equity_evidence,
)
from .portal_projection import (
    RealtimeDetectorDataset,
    RealtimePortalSnapshot,
    RealtimeScreeningGeneration,
    build_detector_dataset,
    build_portal_snapshot,
    build_screening_generation,
)
from .portal_projection_repository import (
    persist_detector_dataset_cursor,
    persist_portal_snapshot_cursor,
    persist_screening_generation_cursor,
    publish_portal_release_cursor,
)
from .preflight import DetectorFamily, ProviderTier
from .database_access import RealtimeDatabaseRole
from .worker_database import RealtimeProcessDatabase


DEVELOPER_DETECTORS = (
    DetectorFamily.O1.value,
    DetectorFamily.O2.value,
    DetectorFamily.O3.value,
    DetectorFamily.S1.value,
    DetectorFamily.S2.value,
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _plain(value):
    if hasattr(value, "model_dump"):
        return _plain(value.model_dump(mode="json"))
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_plain(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


@dataclass(frozen=True, slots=True)
class DeveloperDetectorInvocation:
    detector_id: str
    input_sha256: str
    execute: Callable[[], StableDetectorAdapterResult]

    def __post_init__(self) -> None:
        if self.detector_id not in DEVELOPER_DETECTORS:
            raise ValueError("Developer cycle detector ID is unsupported")
        if len(self.input_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.input_sha256
        ):
            raise ValueError("Developer cycle detector input SHA-256 is invalid")
        if not callable(self.execute):
            raise TypeError("Developer cycle detector invocation is not callable")


@dataclass(frozen=True, slots=True)
class DeveloperTickerComputeInput:
    ticker: str
    security: SecurityReferenceRevision
    bars: tuple[RealtimeEquityBarFact, ...]
    confirmation_bars: tuple[RealtimeEquityBarFact, ...]
    detectors: tuple[DeveloperDetectorInvocation, ...]

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        if ticker not in CANARY_UNIVERSE.equity_tickers:
            raise ValueError("Developer cycle ticker is outside the reviewed universe")
        if self.security.ticker != ticker:
            raise ValueError("Developer cycle security ticker changed")
        if not self.bars:
            raise ValueError("Developer cycle ticker requires materialization bars")
        if {row.revision.ticker for row in (*self.bars, *self.confirmation_bars)} != {
            ticker
        }:
            raise ValueError("Developer cycle bars changed ticker identity")
        detector_ids = tuple(row.detector_id for row in self.detectors)
        if set(detector_ids) != set(DEVELOPER_DETECTORS) or len(detector_ids) != len(
            DEVELOPER_DETECTORS
        ):
            raise ValueError("Developer cycle detector coverage is not exact")
        object.__setattr__(self, "ticker", ticker)


@dataclass(frozen=True, slots=True)
class DeveloperCycleComputePlan:
    cohort_id: UUID
    source_manifest_sha256: str
    source_generation: int
    checked_at: datetime
    feature_definition: RealtimeFeatureDefinition
    tickers: tuple[DeveloperTickerComputeInput, ...]

    def __post_init__(self) -> None:
        if len(self.source_manifest_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.source_manifest_sha256
        ):
            raise ValueError("Developer cycle source manifest SHA-256 is invalid")
        if type(self.source_generation) is not int or self.source_generation <= 0:
            raise ValueError("Developer cycle source generation must be positive")
        if {row.ticker for row in self.tickers} != set(
            CANARY_UNIVERSE.equity_tickers
        ) or len(self.tickers) != len(CANARY_UNIVERSE.equity_tickers):
            raise ValueError("Developer cycle compute plan requires the exact eight tickers")
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))


@dataclass(frozen=True, slots=True)
class DeveloperCycleComputedValues:
    features: tuple[RealtimeEquityMaterializationResult, ...]
    screening: RealtimeScreeningGeneration
    detector_dataset: RealtimeDetectorDataset
    portal_snapshots: tuple[RealtimePortalSnapshot, ...]

    def __post_init__(self) -> None:
        if (
            len(self.features) != len(CANARY_UNIVERSE.equity_tickers)
            or len({row.instrument_id for row in self.features}) != len(self.features)
            or len(self.screening.rows) != len(CANARY_UNIVERSE.equity_tickers)
            or len(self.detector_dataset.evaluations)
            != len(CANARY_UNIVERSE.equity_tickers) * len(DEVELOPER_DETECTORS)
        ):
            raise ValueError("Developer cycle computed coverage is incomplete")
        ticker_pages = {
            row.snapshot_key
            for row in self.portal_snapshots
            if row.snapshot_type == "TICKER_PAGE"
        }
        contexts = tuple(
            row for row in self.portal_snapshots
            if row.snapshot_type == "STOCK_ALERT_CONTEXT"
            and row.snapshot_key == "PLATFORM"
        )
        if (
            ticker_pages != set(CANARY_UNIVERSE.equity_tickers)
            or len(self.portal_snapshots) != len(ticker_pages) + 1
            or len(contexts) != 1
        ):
            raise ValueError("Developer cycle portal coverage is incomplete")


@dataclass(frozen=True, slots=True)
class DeveloperCycleStoredValues:
    feature_snapshot_ids: tuple[UUID, ...]
    screening_generation_id: UUID
    detector_dataset_id: UUID
    portal_snapshot_ids: tuple[UUID, ...]


def _selected(payload: Mapping[str, object]) -> bool:
    disposition = str(
        payload.get("selection_status")
        or payload.get("disposition")
        or payload.get("status")
        or ""
    ).upper()
    return disposition in {"SELECTED", "DETECTED", "CONFIRMED", "READY"}


def produce_developer_cycle_values(
    plan: DeveloperCycleComputePlan,
) -> DeveloperCycleComputedValues:
    checked_at = plan.checked_at
    features = []
    screening_rows = []
    evaluations = []
    ticker_snapshots = []
    stock_observations = []
    for ticker_input in sorted(plan.tickers, key=lambda row: row.ticker):
        analysis_run_id = uuid5(
            NAMESPACE_URL,
            f"developer-cycle-feature:{plan.cohort_id}:{ticker_input.ticker}:"
            f"{plan.source_manifest_sha256}:{plan.source_generation}",
        )
        feature = materialize_realtime_equity_evidence(
            analysis_run_id=analysis_run_id,
            security=ticker_input.security,
            bars=ticker_input.bars,
            confirmation_bars=ticker_input.confirmation_bars,
            observed_at=checked_at,
        )
        features.append(feature)
        feature_evidence = next(
            row
            for row in feature.materialization.evidence
            if row.evidence_type is EvidenceType.FEATURE_SNAPSHOT
        )
        technicals = json.loads(feature_evidence.payload_json)
        detector_payloads = {}
        selected_stock = False
        for invocation in sorted(
            ticker_input.detectors,
            key=lambda row: row.detector_id,
        ):
            result = invocation.execute()
            if not isinstance(result, StableDetectorAdapterResult):
                raise TypeError("Developer cycle detector returned an invalid result")
            if (
                result.cohort_id != plan.cohort_id
                or result.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
                or result.input_sha256 != invocation.input_sha256
            ):
                raise ValueError("Developer cycle detector identity changed")
            if invocation.detector_id == DetectorFamily.O2.value and (
                result.status != "UNAVAILABLE"
                or result.reason_codes != ("DETECTOR_DISABLED_FOR_TIER",)
            ):
                raise ValueError("Developer cycle O2 must remain unavailable")
            output = _plain(result.output)
            payload = {
                "adapter_id": result.adapter_id,
                "status": result.status,
                "reason_codes": list(result.reason_codes),
                "output": output,
                "input_sha256": result.input_sha256,
            }
            detector_payloads[invocation.detector_id] = payload
            selected = result.status == "EXECUTED" and (
                _selected(output)
                if isinstance(output, Mapping)
                else bool(output)
                if isinstance(output, list)
                else False
            )
            if invocation.detector_id in {
                DetectorFamily.S1.value,
                DetectorFamily.S2.value,
            }:
                selected_stock = selected_stock or selected
                stock_observations.append({
                    "ticker": ticker_input.ticker,
                    "detector_id": invocation.detector_id,
                    "selected": selected,
                    "payload": payload,
                })
            event_id = uuid5(
                NAMESPACE_URL,
                f"developer-cycle-evaluation:{plan.cohort_id}:"
                f"{ticker_input.ticker}:{invocation.detector_id}:"
                f"{invocation.input_sha256}",
            )
            evaluations.append((
                event_id,
                invocation.detector_id,
                ticker_input.ticker,
                "SELECTED" if selected else "OBSERVATION",
                checked_at,
                payload,
            ))
        ticker_payload = {
            "ticker": ticker_input.ticker,
            "technicals": technicals,
            "detectors": detector_payloads,
            "provider_fetch_performed": False,
        }
        source_manifest = {
            "source_manifest_sha256": plan.source_manifest_sha256,
            "feature_input_sha256": feature.input_sha256,
            "bar_revision_ids": [str(value) for value in feature.bar_revision_ids],
        }
        ticker_snapshots.append(build_portal_snapshot(
            cohort_id=plan.cohort_id,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            snapshot_type="TICKER_PAGE",
            snapshot_key=ticker_input.ticker,
            source_generation=plan.source_generation,
            source_manifest=source_manifest,
            payload=ticker_payload,
            generated_at=checked_at,
        ))
        screening_rows.append((
            ticker_input.security.security_id,
            ticker_input.ticker,
            selected_stock,
            ticker_payload,
        ))
    screening = build_screening_generation(
        cohort_id=plan.cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        session_date=checked_at.date(),
        expected_members=len(CANARY_UNIVERSE.equity_tickers),
        generated_at=checked_at,
        manifest={
            "source_manifest_sha256": plan.source_manifest_sha256,
            "source_generation": plan.source_generation,
        },
        rows=tuple(screening_rows),
    )
    dataset = build_detector_dataset(
        cohort_id=plan.cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        dataset_key=f"developer-cycle-{checked_at.date().isoformat()}",
        session_date=checked_at.date(),
        detector_ids=DEVELOPER_DETECTORS,
        source_manifest_sha256=plan.source_manifest_sha256,
        started_at=checked_at,
        completed_at=checked_at,
        summary={
            "evaluated": len(evaluations),
            "tickers": len(plan.tickers),
            "o2": "UNAVAILABLE",
        },
        evaluations=tuple(evaluations),
    )
    context = build_portal_snapshot(
        cohort_id=plan.cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type="STOCK_ALERT_CONTEXT",
        snapshot_key="PLATFORM",
        source_generation=plan.source_generation,
        source_manifest={
            "source_manifest_sha256": plan.source_manifest_sha256,
            "detector_dataset_id": str(dataset.dataset_id),
        },
        payload={
            "schedule_streams": [],
            "sessions": [checked_at.date().isoformat()],
            "observations": stock_observations,
            "provider_fetch_performed": False,
        },
        generated_at=checked_at,
    )
    return DeveloperCycleComputedValues(
        features=tuple(features),
        screening=screening,
        detector_dataset=dataset,
        portal_snapshots=(*ticker_snapshots, context),
    )


def persist_developer_cycle_values(
    compute_database: RealtimeProcessDatabase,
    publication_database: RealtimeProcessDatabase,
    plan: DeveloperCycleComputePlan,
    values: DeveloperCycleComputedValues,
    *,
    feature_repository=persist_realtime_equity_materialization_cursor,
    screening_repository=persist_screening_generation_cursor,
    detector_repository=persist_detector_dataset_cursor,
    portal_repository=persist_portal_snapshot_cursor,
    receipt=None,
    release_repository=publish_portal_release_cursor,
) -> DeveloperCycleStoredValues:
    if (
        not isinstance(compute_database, RealtimeProcessDatabase)
        or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE
    ):
        raise ValueError("Developer cycle value persistence requires COMPUTE")
    if (
        not isinstance(publication_database, RealtimeProcessDatabase)
        or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION
    ):
        raise ValueError("Developer cycle portal persistence requires PUBLICATION")
    if not isinstance(plan, DeveloperCycleComputePlan) or not isinstance(
        values, DeveloperCycleComputedValues
    ):
        raise TypeError("Developer cycle value persistence inputs are invalid")
    if any(row.cohort_id != plan.cohort_id for row in values.features):
        raise ValueError("Developer cycle feature cohort identity changed")
    if (
        values.screening.cohort_id != plan.cohort_id
        or values.detector_dataset.cohort_id != plan.cohort_id
        or any(row.cohort_id != plan.cohort_id for row in values.portal_snapshots)
    ):
        raise ValueError("Developer cycle projection cohort identity changed")

    with compute_database.transaction() as cursor:
        feature_snapshot_ids = tuple(
            feature_repository(
                cursor,
                feature,
                plan.feature_definition,
                generation=plan.source_generation,
            )
            for feature in values.features
        )
        screening_repository(cursor, values.screening)
        detector_repository(cursor, values.detector_dataset)
    with publication_database.transaction() as cursor:
        for snapshot in values.portal_snapshots:
            portal_repository(cursor, snapshot)
        if receipt is not None:
            release_repository(cursor, receipt, values.portal_snapshots)
    return DeveloperCycleStoredValues(
        feature_snapshot_ids=feature_snapshot_ids,
        screening_generation_id=values.screening.generation_id,
        detector_dataset_id=values.detector_dataset.dataset_id,
        portal_snapshot_ids=tuple(row.snapshot_id for row in values.portal_snapshots),
    )