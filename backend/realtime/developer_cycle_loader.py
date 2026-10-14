from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID

from equity.domain import SecurityReferenceRevision
from options.domain import OptionContractSnapshot

from .acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    developer_equity_bar_facts,
    developer_option_snapshots,
    load_developer_acquisition_batch_cursor,
)
from .contracts import CANARY_UNIVERSE
from .database_access import RealtimeDatabaseRole
from .developer_cycle_compute import (
    DeveloperCycleComputePlan,
    DeveloperDetectorInvocation,
    DeveloperTickerComputeInput,
)
from .equity_evidence_repository import RealtimeFeatureDefinition
from .worker_database import RealtimeProcessDatabase


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Developer cycle load clock must be timezone-aware")
    return value.astimezone(timezone.utc)


class DeveloperDetectorBindingFactory(Protocol):
    def __call__(
        self,
        *,
        cohort_id: UUID,
        ticker: str,
        security: SecurityReferenceRevision,
        bars: tuple,
        option_snapshots: tuple[OptionContractSnapshot, ...],
        split_clear: bool,
        checked_at: datetime,
    ) -> tuple[DeveloperDetectorInvocation, ...]: ...


@dataclass(frozen=True, slots=True)
class DeveloperCycleLoadRequest:
    cohort_id: UUID
    source_manifest_sha256: str
    source_generation: int
    checked_at: datetime
    acquisition_batch_ids: tuple[UUID, ...]
    required_intervals: tuple[str, ...]
    primary_interval: str
    feature_definition: RealtimeFeatureDefinition

    def __post_init__(self) -> None:
        if len(self.source_manifest_sha256) != 64 or any(
            value not in "0123456789abcdef" for value in self.source_manifest_sha256
        ):
            raise ValueError("Developer cycle load manifest SHA-256 is invalid")
        if type(self.source_generation) is not int or self.source_generation <= 0:
            raise ValueError("Developer cycle load generation must be positive")
        if (
            not self.acquisition_batch_ids
            or len(self.acquisition_batch_ids) > 100
            or len(self.acquisition_batch_ids) != len(set(self.acquisition_batch_ids))
        ):
            raise ValueError("Developer cycle load batch IDs are invalid")
        intervals = tuple(sorted(set(value.strip() for value in self.required_intervals)))
        if not intervals or any(not value for value in intervals):
            raise ValueError("Developer cycle load intervals are invalid")
        primary = self.primary_interval.strip()
        if primary not in intervals:
            raise ValueError("Developer cycle primary interval is not required")
        object.__setattr__(self, "required_intervals", intervals)
        object.__setattr__(self, "primary_interval", primary)
        object.__setattr__(self, "checked_at", _utc(self.checked_at))


def _security(row) -> SecurityReferenceRevision:
    (
        revision_id,
        security_id,
        ticker,
        asset_type,
        exchange,
        effective_from,
        observed_at,
        payload_sha256,
    ) = row
    return SecurityReferenceRevision(
        security_revision_id=UUID(str(revision_id)),
        security_id=UUID(str(security_id)),
        ticker=str(ticker),
        active=True,
        company_name=str(ticker),
        security_type=str(asset_type),
        cik=None,
        composite_figi=None,
        share_class_figi=None,
        primary_exchange=str(exchange) if exchange is not None else None,
        sic_code=None,
        sic_description=None,
        sector=None,
        industry=None,
        list_date=None,
        delisted_date=None,
        weighted_shares=None,
        free_float=None,
        free_float_percent=None,
        market_cap=None,
        source="REALTIME_REFERENCE_V1",
        effective_from=effective_from,
        observed_at=observed_at,
        payload_sha256=str(payload_sha256),
        raw_payload_json="{}",
    )


def load_developer_cycle_compute_plan(
    database: RealtimeProcessDatabase,
    request: DeveloperCycleLoadRequest,
    detector_factory: DeveloperDetectorBindingFactory,
) -> DeveloperCycleComputePlan:
    if (
        not isinstance(database, RealtimeProcessDatabase)
        or database.process_plan.role is not RealtimeDatabaseRole.COMPUTE
    ):
        raise ValueError("Developer cycle loading requires COMPUTE")
    if not isinstance(request, DeveloperCycleLoadRequest):
        raise TypeError("Developer cycle load request is invalid")
    if not callable(detector_factory):
        raise TypeError("Developer cycle detector factory is invalid")
    with database.transaction(
        read_only=True,
        isolation_level="REPEATABLE READ",
    ) as cursor:
        batches = tuple(
            load_developer_acquisition_batch_cursor(cursor, batch_id)
            for batch_id in request.acquisition_batch_ids
        )
        if any(batch.cohort_id != request.cohort_id for batch in batches):
            raise ValueError("Developer cycle acquisition batches crossed cohorts")
        cursor.execute(
            "SELECT revision.revision_id, revision.security_id, revision.ticker, "
            "security.asset_type, revision.exchange, revision.effective_from, "
            "revision.observed_at, revision.payload_sha256 "
            "FROM ref.security_revisions AS revision "
            "JOIN ref.securities AS security ON security.security_id=revision.security_id "
            "WHERE revision.ticker=ANY(%s) AND revision.effective_from<=%s "
            "AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
            "ORDER BY revision.ticker",
            (
                list(CANARY_UNIVERSE.equity_tickers),
                request.checked_at,
                request.checked_at,
            ),
        )
        securities = {
            security.ticker: security
            for security in (_security(row) for row in cursor.fetchall())
        }
        coverage_start = min(
            fact.revision.session_date
            for batch in batches
            if batch.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR
            for fact in batch.facts
        )
        cursor.execute(
            "SELECT coverage.security_id::text FROM ref.corporate_action_coverage AS coverage "
            "WHERE coverage.action_type='SPLIT' AND coverage.window_start<=%s "
            "AND coverage.window_end>=%s AND coverage.received_at<=%s "
            "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS action "
            "WHERE action.security_id=coverage.security_id AND action.action_type='SPLIT' "
            "AND action.effective_date BETWEEN %s AND %s AND action.observed_at<=%s)",
            (
                coverage_start,
                request.checked_at.date(),
                request.checked_at,
                coverage_start,
                request.checked_at.date(),
                request.checked_at,
            ),
        )
        split_clear_security_ids = {
            UUID(str(row[0])) for row in cursor.fetchall()
        }
    if set(securities) != set(CANARY_UNIVERSE.equity_tickers):
        raise ValueError("Developer cycle security coverage is incomplete")

    bars_by_ticker = {ticker: [] for ticker in CANARY_UNIVERSE.equity_tickers}
    options_by_ticker = {ticker: [] for ticker in CANARY_UNIVERSE.option_underlyings}
    for batch in batches:
        if batch.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR:
            for fact in developer_equity_bar_facts(batch):
                if fact.revision.ticker not in bars_by_ticker:
                    raise ValueError("Developer cycle bar is outside the reviewed universe")
                bars_by_ticker[fact.revision.ticker].append(fact)
        else:
            for snapshot in developer_option_snapshots(batch).values():
                if snapshot.underlyer not in options_by_ticker:
                    raise ValueError("Developer cycle option is outside the reviewed universe")
                options_by_ticker[snapshot.underlyer].append(snapshot)
    ticker_inputs = []
    for ticker in CANARY_UNIVERSE.equity_tickers:
        bars = tuple(sorted(
            bars_by_ticker[ticker],
            key=lambda row: (row.revision.interval, row.revision.bar_start),
        ))
        intervals = {row.revision.interval for row in bars}
        if not set(request.required_intervals).issubset(intervals):
            raise ValueError(f"Developer cycle lookback coverage is incomplete for {ticker}")
        snapshots = tuple(sorted(
            options_by_ticker[ticker],
            key=lambda row: (row.expiration_date, row.contract_ticker),
        ))
        if not snapshots:
            raise ValueError(f"Developer cycle option coverage is incomplete for {ticker}")
        primary_interval = request.primary_interval
        primary_bars = tuple(
            row for row in bars if row.revision.interval == primary_interval
        )
        confirmation_interval = {
            "1h": "1d",
            "1d": "1h",
        }.get(primary_interval)
        confirmation_bars = tuple(
            row for row in bars if row.revision.interval == confirmation_interval
        )
        ticker_inputs.append(DeveloperTickerComputeInput(
            ticker=ticker,
            security=securities[ticker],
            bars=primary_bars,
            confirmation_bars=confirmation_bars,
            detectors=detector_factory(
                cohort_id=request.cohort_id,
                ticker=ticker,
                security=securities[ticker],
                bars=bars,
                option_snapshots=snapshots,
                split_clear=(
                    securities[ticker].security_id in split_clear_security_ids
                ),
                checked_at=request.checked_at,
            ),
        ))
    return DeveloperCycleComputePlan(
        cohort_id=request.cohort_id,
        source_manifest_sha256=request.source_manifest_sha256,
        source_generation=request.source_generation,
        checked_at=request.checked_at,
        feature_definition=request.feature_definition,
        tickers=tuple(ticker_inputs),
    )