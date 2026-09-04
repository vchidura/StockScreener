"""Bounded one-shot orchestration for equity materialization."""
from __future__ import annotations

import json
import hashlib
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import requests
from research.gics_sectors import (
    resolve_benchmark_ticker,
    sector_benchmark_ticker,
)

from .context import build_equity_context
from .derivation import DERIVATION_SOURCES, derive_canonical_bars
from .domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    DecisionWatermark,
    EquityEvidence,
    EvidenceType,
    SecurityReferenceRevision,
)
from .materialization import (
    CHANNEL_VERSION,
    CONFIRMATION_VERSION,
    FEATURE_VERSION,
    FUNDAMENTAL_SNAPSHOT_VERSION,
    PATTERN_VERSION,
    PERSISTED_CONFIRMATION_VERSIONS,
    PORTAL_STRATEGY_VERSION,
    SCANNER_INTERVALS,
    SCANNER_BUNDLE_VERSION,
    SETUP_INTERVALS,
    SETUP_VERSION,
    materialize_equity_evidence,
)
from .polygon import (
    PolygonEquityClient,
    canonical_json,
    normalize_fundamental_reports,
    normalize_native_bars,
    normalize_security_reference,
    sha256_json,
)
from .outcomes import OutcomePolicy, evaluate_directional_outcome
from .qualification import qualify_outcomes
from .repositories import (
    EquityAnalysisRepository,
    EquityBarRepository,
    EquityEvidenceRepository,
    EquityIngestionRepository,
    EquityOutcomeRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from .stream import reconcile_interval_bar


REFERENCE_POLICY_VERSION = "polygon_reference_v1"
CONTEXT_POLICY_VERSION = "equity_context_v2"
MODEL_BUNDLE_VERSION = "equity_materialization_v16"
BAR_SELECTION_POLICY_VERSION = "equity_bar_selection_v1"
BAR_SELECTION_POLICY = {
    "intraday": ["RECONCILED", "NATIVE_REST", "REALTIME_STREAM", "DERIVED"],
    "higher": ["RECONCILED", "DERIVED", "NATIVE_REST", "REALTIME_STREAM"],
    "minimum_coverage": 0.95,
    "session_scope": "RTH",
    "adjusted": False,
}
BAR_SELECTION_POLICY_SHA256 = sha256_json(BAR_SELECTION_POLICY)
_BATCH_SIZE = 50
_MINIMUM_BARS = {
    "5m": 50, "15m": 40, "30m": 40, "1h": 40, "1d": 50, "1wk": 40,
}
_BAR_LIMIT = {
    "5m": 400, "15m": 400, "30m": 400, "1h": 500, "1d": 1600,
    "1wk": 400, "1mo": 240,
}
_DERIVATION_SOURCE_LIMIT = {"1h": 20, "1d": 20, "1wk": 10, "1mo": 40}


@dataclass(frozen=True, slots=True)
class ReferenceRefreshResult:
    universe_run_id: UUID
    revisions: tuple[SecurityReferenceRevision, ...]
    missing_tickers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IntervalIngestionResult:
    ingestion_segment_id: UUID
    interval: str
    bar_count: int
    inserted_count: int
    missing_tickers: tuple[str, ...]
    failed_tickers: tuple[str, ...] = ()
    fetch_seconds: float = 0.0
    persist_seconds: float = 0.0
    elapsed_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class FundamentalRefreshResult:
    available: bool
    reports_written: int
    reason: str | None = None


class IngestionCoverageError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReconciliationRunResult:
    ingestion_segment_id: UUID | None
    interval: str
    pending_count: int
    native_count: int
    reconciled_count: int
    inserted_count: int
    status_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class AnalysisRunResult:
    analysis_run_id: UUID
    interval: str
    status: str
    completed: int
    insufficient: int
    failed: int
    evidence_count: int
    inserted_evidence_count: int
    context_count: int


@dataclass(frozen=True, slots=True)
class BarPublicationResult:
    publication_id: UUID
    interval: str
    status: str
    selected: int
    missing: int
    failed: int


@dataclass(frozen=True, slots=True)
class OutcomeRunResult:
    policy_key: str
    horizon_key: str
    due: int
    persisted: int
    pending: int


@dataclass(frozen=True, slots=True)
class QualificationRunResult:
    observations: int
    revisions: int
    robust_passes: int


class EquityMaterializationService:
    def __init__(
        self,
        client: PolygonEquityClient,
        *,
        reference_repository: EquityReferenceRepository | None = None,
        universe_repository: EquityUniverseRepository | None = None,
        ingestion_repository: EquityIngestionRepository | None = None,
        bar_repository: EquityBarRepository | None = None,
        analysis_repository: EquityAnalysisRepository | None = None,
        evidence_repository: EquityEvidenceRepository | None = None,
        outcome_repository: EquityOutcomeRepository | None = None,
        native_fetch_workers: int = 1,
        outcome_path_cache_size: int = 0,
        outcome_path_prefetch_limit: int | None = None,
    ) -> None:
        if native_fetch_workers <= 0:
            raise ValueError("native_fetch_workers must be positive")
        if outcome_path_cache_size < 0:
            raise ValueError("outcome_path_cache_size cannot be negative")
        if outcome_path_prefetch_limit is not None \
                and outcome_path_prefetch_limit <= 0:
            raise ValueError("outcome_path_prefetch_limit must be positive")
        self.client = client
        self.native_fetch_workers = native_fetch_workers
        self.outcome_path_cache_size = outcome_path_cache_size
        self.outcome_path_prefetch_limit = outcome_path_prefetch_limit
        self._outcome_path_cache = {}
        self.reference_repository = reference_repository or EquityReferenceRepository()
        self.universe_repository = universe_repository or EquityUniverseRepository()
        self.ingestion_repository = ingestion_repository or EquityIngestionRepository()
        self.bar_repository = bar_repository or EquityBarRepository()
        self.analysis_repository = analysis_repository or EquityAnalysisRepository()
        self.evidence_repository = evidence_repository or EquityEvidenceRepository()
        self.outcome_repository = outcome_repository or EquityOutcomeRepository()

    def refresh_reference(
        self,
        tickers: Sequence[str],
        *,
        observed_at: datetime,
        as_of_date: date | None = None,
        include_float: bool = True,
    ) -> ReferenceRefreshResult:
        normalized_tickers = tuple(dict.fromkeys(ticker.upper() for ticker in tickers))
        float_by_ticker: dict[str, dict[str, Any]] = {}
        if include_float:
            for batch in _batches(normalized_tickers):
                for row in self.client.fetch_float(batch):
                    float_by_ticker[str(row.get("ticker") or "").upper()] = row
        revisions = []
        missing = []
        for ticker in normalized_tickers:
            payload = self.client.fetch_ticker_overview(ticker, as_of_date=as_of_date)
            if payload is None:
                missing.append(ticker)
                continue
            revisions.append(normalize_security_reference(
                payload,
                observed_at=observed_at,
                source_as_of_date=as_of_date,
                float_payload=float_by_ticker.get(ticker),
            ))
        revisions_tuple = tuple(revisions)
        self.reference_repository.persist_security_revisions(revisions_tuple)
        self.reference_repository.update_selected_ticker_projection(revisions_tuple)
        policy_sha256 = sha256_json({
            "include_float": include_float,
            "source": REFERENCE_POLICY_VERSION,
            "tickers": normalized_tickers,
        })
        effective_from = datetime.combine(
            as_of_date or _utc(observed_at).date(), time.min, tzinfo=timezone.utc
        )
        universe_run_id = uuid5(
            NAMESPACE_URL,
            f"equity-universe:{effective_from.isoformat()}:{policy_sha256}",
        )
        self.universe_repository.persist_complete_run(
            universe_run_id=universe_run_id,
            source="POLYGON_REFERENCE",
            mode="FIXED",
            effective_from=effective_from,
            observed_at=_utc(observed_at),
            policy_version=REFERENCE_POLICY_VERSION,
            policy_sha256=policy_sha256,
            members=revisions_tuple,
            configuration={
                "missing_tickers": missing,
                "requested_tickers": list(normalized_tickers),
            },
        )
        return ReferenceRefreshResult(
            universe_run_id=universe_run_id,
            revisions=revisions_tuple,
            missing_tickers=tuple(missing),
        )

    def refresh_fundamentals(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        observed_at: datetime,
    ) -> FundamentalRefreshResult:
        total = 0
        by_ticker = {row.ticker: row for row in securities}
        for batch in _batches(tuple(by_ticker)):
            try:
                income = self.client.fetch_income_statements(batch)
                balance = self.client.fetch_balance_sheets(batch)
                cash_flow = self.client.fetch_cash_flow_statements(batch)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 403:
                    return FundamentalRefreshResult(
                        available=False,
                        reports_written=total,
                        reason="POLYGON_FINANCIALS_ENTITLEMENT_UNAVAILABLE",
                    )
                raise
            filing_dates = [
                date.fromisoformat(str(row["filing_date"])[:10])
                for row in (*income, *balance, *cash_flow)
                if row.get("filing_date")
            ]
            filing_start = min(filing_dates) if filing_dates else None
            filings = self.client.fetch_filing_index(
                batch,
                filing_date_gte=filing_start,
            )
            reports = []
            for ticker in batch:
                reports.extend(normalize_fundamental_reports(
                    by_ticker[ticker],
                    income_rows=income,
                    balance_rows=balance,
                    cash_flow_rows=cash_flow,
                    filing_rows=filings,
                    observed_at=observed_at,
                ))
            total += self.reference_repository.persist_fundamental_reports(reports)
        return FundamentalRefreshResult(available=True, reports_written=total)

    def ingest_native_interval(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        interval: str,
        start: date,
        end: date,
        observed_at: datetime,
        availability_mode: BarAvailabilityMode = BarAvailabilityMode.LIVE_OBSERVED,
    ) -> IntervalIngestionResult:
        started_at = time_module.perf_counter()
        observed_utc = _utc(observed_at)
        segment_id = uuid5(
            NAMESPACE_URL,
            f"equity-segment:polygon:{interval}:{start}:{end}:{observed_utc.isoformat()}",
        )
        self.ingestion_repository.start_segment(
            ingestion_segment_id=segment_id,
            provider="polygon",
            provider_mode=(
                "REPLAY" if availability_mode is BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
                else "REST"
            ),
            dataset="EQUITY_BARS",
            interval=interval,
            requested_from=datetime.combine(start, time.min, tzinfo=timezone.utc),
            requested_to=datetime.combine(end, time.max, tzinfo=timezone.utc),
            observed_at=observed_utc,
        )
        bar_count = 0
        inserted_count = 0
        missing = []
        failed: dict[str, Exception] = {}
        payload_manifest = []
        market_watermark = None
        fetch_started_at = time_module.perf_counter()
        persist_seconds = 0.0
        thread_state = threading.local()

        def fetch_security(
            security: SecurityReferenceRevision,
        ) -> tuple[SecurityReferenceRevision, tuple]:
            client = self.client
            if self.native_fetch_workers > 1:
                client = getattr(thread_state, "client", None)
                if client is None:
                    fork = getattr(self.client, "fork", None)
                    if fork is None:
                        raise TypeError(
                            "parallel native ingestion requires a client with fork()"
                        )
                    client = fork()
                    thread_state.client = client
            raw_rows = client.fetch_native_bars(
                security.ticker, interval, start, end, adjusted=False
            )
            return security, normalize_native_bars(
                security.security_id,
                security.ticker,
                interval,
                raw_rows,
                observed_at=observed_utc,
                adjusted=False,
                availability_mode=availability_mode,
                ingestion_segment_id=segment_id,
            )

        futures = {}
        try:
            with ThreadPoolExecutor(
                max_workers=min(self.native_fetch_workers, max(len(securities), 1)),
                thread_name_prefix="equity-native-fetch",
            ) as executor:
                futures = {
                    executor.submit(fetch_security, security): security
                    for security in securities
                }
                completed = as_completed(futures)
                for future in completed:
                    security = futures[future]
                    try:
                        security, bars = future.result()
                    except Exception as exc:
                        failed[security.ticker] = exc
                        continue
                    if not bars:
                        missing.append(security.ticker)
                        continue
                    bar_count += len(bars)
                    persist_started_at = time_module.perf_counter()
                    inserted_count += self.bar_repository.persist(bars)
                    persist_seconds += time_module.perf_counter() - persist_started_at
                    payload_manifest.extend(
                        (
                            row.ticker,
                            row.interval,
                            row.bar_start.isoformat(),
                            row.payload_sha256,
                        )
                        for row in bars
                    )
                    latest_bar_end = max(row.bar_end for row in bars)
                    market_watermark = max(
                        value for value in (market_watermark, latest_bar_end)
                        if value is not None
                    )
        except Exception as exc:
            for future in futures:
                future.cancel()
            self.ingestion_repository.complete_segment(
                segment_id,
                status="FAILED",
                market_watermark=market_watermark,
                record_count=bar_count,
                gap_details={"failure_type": type(exc).__name__},
                completed_at=datetime.now(timezone.utc),
            )
            raise
        fetch_seconds = time_module.perf_counter() - fetch_started_at - persist_seconds
        unavailable = tuple(sorted({*missing, *failed}))
        if failed and len(failed) == len(securities):
            first_error = next(iter(failed.values()))
            self.ingestion_repository.complete_segment(
                segment_id,
                status="FAILED",
                market_watermark=market_watermark,
                record_count=bar_count,
                gap_details={"failure_type": type(first_error).__name__},
                completed_at=datetime.now(timezone.utc),
            )
            raise first_error
        unavailable_fraction = len(unavailable) / len(securities) if securities else 1.0
        terminal_status = (
            "FAILED" if unavailable_fraction > 0.05
            else "DEGRADED" if unavailable else "COMPLETE"
        )
        elapsed_seconds = time_module.perf_counter() - started_at
        self.ingestion_repository.complete_segment(
            segment_id,
            status=terminal_status,
            market_watermark=market_watermark,
            record_count=bar_count,
            checksum_sha256=sha256_json(sorted(payload_manifest)),
            gap_details={
                "failed_tickers": {
                    ticker: type(error).__name__ for ticker, error in sorted(failed.items())
                },
                "fetch_workers": self.native_fetch_workers,
                "missing_tickers": sorted(missing),
                "timings_seconds": {
                    "elapsed": round(elapsed_seconds, 3),
                    "fetch_and_normalize": round(fetch_seconds, 3),
                    "persist": round(persist_seconds, 3),
                },
            },
            completed_at=datetime.now(timezone.utc),
        )
        if terminal_status == "FAILED":
            raise IngestionCoverageError(
                f"native {interval} ingestion coverage failed: "
                f"{len(unavailable)}/{len(securities)} tickers unavailable"
            )
        return IntervalIngestionResult(
            ingestion_segment_id=segment_id,
            interval=interval,
            bar_count=bar_count,
            inserted_count=inserted_count,
            missing_tickers=unavailable,
            failed_tickers=tuple(sorted(failed)),
            fetch_seconds=fetch_seconds,
            persist_seconds=persist_seconds,
            elapsed_seconds=elapsed_seconds,
        )

    def reconcile_stream_interval(
        self,
        *,
        interval: str,
        observed_at: datetime,
        limit: int = 5000,
    ) -> ReconciliationRunResult:
        observed_utc = _utc(observed_at)
        pending = self.bar_repository.list_pending_reconciliation(
            interval, available_by=observed_utc, limit=limit
        )
        if not pending:
            return ReconciliationRunResult(
                ingestion_segment_id=None,
                interval=interval,
                pending_count=0,
                native_count=0,
                reconciled_count=0,
                inserted_count=0,
                status_counts=(),
            )
        segment_id = uuid5(
            NAMESPACE_URL,
            "equity-reconciliation:"
            + sha256_json({
                "interval": interval,
                "observed_at": observed_utc.isoformat(),
                "stream_bar_ids": [str(row.bar_revision_id) for row in pending],
            }),
        )
        requested_from = min(row.bar_start for row in pending)
        requested_to = max(row.bar_end for row in pending)
        self.ingestion_repository.start_segment(
            ingestion_segment_id=segment_id,
            provider="polygon",
            provider_mode="REST",
            dataset="EQUITY_BAR_RECONCILIATION",
            interval=interval,
            requested_from=requested_from,
            requested_to=requested_to,
            observed_at=observed_utc,
        )
        try:
            native_by_key = {}
            pending_keys = {
                (row.ticker, row.bar_start): row for row in pending
            }
            pending_by_ticker: dict[str, list[EquityBarRevision]] = {}
            for row in pending:
                pending_by_ticker.setdefault(row.ticker, []).append(row)
            for ticker, stream_bars in pending_by_ticker.items():
                raw_rows = self.client.fetch_native_bars(
                    ticker,
                    interval,
                    min(row.session_date for row in stream_bars),
                    max(row.session_date for row in stream_bars),
                    adjusted=False,
                )
                native_rows = normalize_native_bars(
                    stream_bars[0].security_id,
                    ticker,
                    interval,
                    raw_rows,
                    observed_at=observed_utc,
                    adjusted=False,
                    availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                    ingestion_segment_id=segment_id,
                )
                for native in native_rows:
                    key = (native.ticker, native.bar_start)
                    if key in pending_keys:
                        native_by_key[key] = native
            native = tuple(native_by_key.values())
            reconciled = tuple(
                reconcile_interval_bar(
                    stream_bar=stream_bar,
                    native_bar=native_by_key.get((stream_bar.ticker, stream_bar.bar_start)),
                    observed_at=observed_utc,
                )
                for stream_bar in pending
            )
            inserted = self.bar_repository.persist((*native, *reconciled))
            status_counts: dict[str, int] = {}
            for row in reconciled:
                status = row.reconciliation_status or "UNKNOWN"
                status_counts[status] = status_counts.get(status, 0) + 1
            terminal_status = (
                "DEGRADED" if status_counts.get("NATIVE_MISSING") else "COMPLETE"
            )
            self.ingestion_repository.complete_segment(
                segment_id,
                status=terminal_status,
                market_watermark=max(row.bar_end for row in reconciled),
                record_count=len(reconciled),
                checksum_sha256=sha256_json([
                    row.payload_sha256 for row in reconciled
                ]),
                gap_details={"reconciliation_status_counts": status_counts},
                completed_at=datetime.now(timezone.utc),
            )
            return ReconciliationRunResult(
                ingestion_segment_id=segment_id,
                interval=interval,
                pending_count=len(pending),
                native_count=len(native),
                reconciled_count=len(reconciled),
                inserted_count=inserted,
                status_counts=tuple(sorted(status_counts.items())),
            )
        except Exception as exc:
            self.ingestion_repository.complete_segment(
                segment_id,
                status="FAILED",
                market_watermark=None,
                record_count=0,
                gap_details={"failure_type": type(exc).__name__},
                completed_at=datetime.now(timezone.utc),
            )
            raise

    def publish_canonical_interval(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        interval: str,
        watermark: DecisionWatermark,
    ) -> BarPublicationResult:
        selected = {}
        failures = {}
        manifest = []
        for security in securities:
            try:
                bars = self.bar_repository.list_final_as_of(
                    security.ticker,
                    interval,
                    watermark,
                    limit=1,
                    session_scope=BarSessionScope.RTH,
                    adjusted=False,
                )
            except Exception as exc:
                failures[security.ticker] = type(exc).__name__
                manifest.append({
                    "failure": type(exc).__name__,
                    "ticker": security.ticker,
                })
                continue
            bar = bars[-1] if bars and bars[-1].bar_end == watermark.market_time else None
            if bar is not None:
                selected[security.ticker] = bar
            manifest.append({
                "bar_revision_id": str(bar.bar_revision_id) if bar is not None else None,
                "ticker": security.ticker,
            })

        input_sha256 = sha256_json({
            "interval": interval,
            "market_time": watermark.market_time.isoformat(),
            "members": manifest,
            "policy": BAR_SELECTION_POLICY_SHA256,
        })
        output_sha256 = sha256_json({
            "failed": sorted(failures.items()),
            "selected": sorted(
                (ticker, str(bar.bar_revision_id)) for ticker, bar in selected.items()
            ),
        })
        business_key = ":".join((
            interval,
            watermark.market_time.isoformat(),
            BAR_SELECTION_POLICY_VERSION,
            input_sha256,
        ))
        publication_id = uuid5(NAMESPACE_URL, f"equity-bar-publication:{business_key}")
        publication = self.bar_repository.publish_canonical_cohort(
            publication_id=publication_id,
            business_key=business_key,
            interval=interval,
            market_time=watermark.market_time,
            observed_at=watermark.observed_time,
            session_scope=BarSessionScope.RTH,
            adjusted=False,
            selection_policy_version=BAR_SELECTION_POLICY_VERSION,
            selection_policy_sha256=BAR_SELECTION_POLICY_SHA256,
            input_sha256=input_sha256,
            output_sha256=output_sha256,
            members=securities,
            selected=selected,
            failure_reasons=failures,
            minimum_coverage=BAR_SELECTION_POLICY["minimum_coverage"],
        )
        return BarPublicationResult(
            publication_id=publication["publication_id"],
            interval=interval,
            status=publication["status"],
            selected=publication["selected_members"],
            missing=publication["missing_members"],
            failed=publication["failed_members"],
        )

    def derive_interval(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        target_interval: str,
        watermark: DecisionWatermark,
        include_history: bool = False,
        source_limit_per_ticker: int | None = None,
    ) -> IntervalIngestionResult:
        if target_interval not in DERIVATION_SOURCES:
            raise ValueError(f"unsupported derived interval: {target_interval}")
        started_at = time_module.perf_counter()
        source_interval = DERIVATION_SOURCES[target_interval]
        if source_limit_per_ticker is not None and source_limit_per_ticker <= 0:
            raise ValueError("source_limit_per_ticker must be positive")
        source_limit = source_limit_per_ticker
        if source_limit is None and not include_history:
            source_limit = _DERIVATION_SOURCE_LIMIT[target_interval]
        segment_id = uuid5(
            NAMESPACE_URL,
            "equity-derived-segment:"
            + sha256_json({
                "interval": target_interval,
                "market_time": watermark.market_time.isoformat(),
                "observed_time": watermark.observed_time.isoformat(),
                "include_history": include_history,
                "source_interval": source_interval,
                "source_limit_per_ticker": source_limit,
                "tickers": sorted(row.ticker for row in securities),
            }),
        )
        self.ingestion_repository.start_segment(
            ingestion_segment_id=segment_id,
            provider="internal",
            provider_mode="DERIVED",
            dataset="EQUITY_DERIVED_BARS",
            interval=target_interval,
            requested_from=None,
            requested_to=watermark.market_time,
            observed_at=watermark.observed_time,
        )
        requested_from = None
        market_watermark = None
        bar_count = 0
        inserted_count = 0
        persist_seconds = 0.0
        missing = []
        failed = {}
        checksum = hashlib.sha256()
        batches = (
            _batches(securities)
            if include_history else (tuple(securities),)
        )
        try:
            for batch in batches:
                source_by_ticker = self.bar_repository.list_final_for_tickers_as_of(
                    tuple(row.ticker for row in batch),
                    source_interval,
                    watermark,
                    limit_per_ticker=source_limit,
                    session_scope=BarSessionScope.RTH,
                    adjusted=False,
                )
                source_rows = tuple(
                    bar for bars in source_by_ticker.values() for bar in bars
                )
                batch_requested_from = min(
                    (bar.bar_start for bar in source_rows), default=None
                )
                if batch_requested_from is not None:
                    requested_from = min(
                        value for value in (requested_from, batch_requested_from)
                        if value is not None
                    )
                derived = []
                for security in batch:
                    try:
                        candidates = derive_canonical_bars(
                            security,
                            source_by_ticker.get(security.ticker, ()),
                            target_interval=target_interval,
                            observed_at=watermark.observed_time,
                            ingestion_segment_id=segment_id,
                        )
                        selected_candidates = (
                            candidates
                            if include_history
                            else tuple(
                                bar for bar in reversed(candidates)
                                if bar.bar_end == watermark.market_time
                            )[:1]
                        )
                        if not selected_candidates:
                            missing.append(security.ticker)
                        else:
                            derived.extend(selected_candidates)
                    except Exception as exc:
                        failed[security.ticker] = type(exc).__name__
                persist_started_at = time_module.perf_counter()
                inserted_count += self.bar_repository.persist(derived)
                persist_seconds += time_module.perf_counter() - persist_started_at
                bar_count += len(derived)
                for bar in sorted(
                    derived,
                    key=lambda row: (row.ticker, row.bar_start, row.bar_revision_id.hex),
                ):
                    checksum.update(canonical_json((
                        bar.ticker, str(bar.bar_revision_id), bar.payload_sha256,
                    )).encode("utf-8"))
                    checksum.update(b"\n")
                batch_watermark = max(
                    (bar.bar_end for bar in derived), default=None
                )
                if batch_watermark is not None:
                    market_watermark = max(
                        value for value in (market_watermark, batch_watermark)
                        if value is not None
                    )
        except Exception as exc:
            self.ingestion_repository.complete_segment(
                segment_id,
                status="FAILED",
                requested_from=requested_from,
                market_watermark=market_watermark,
                record_count=bar_count,
                gap_details={"failure_type": type(exc).__name__},
                completed_at=datetime.now(timezone.utc),
            )
            raise
        unavailable = tuple(sorted({*missing, *failed}))
        coverage = (
            (len(securities) - len(unavailable)) / len(securities)
            if securities else 0.0
        )
        status = (
            "COMPLETE" if not unavailable
            else "DEGRADED" if coverage >= 0.95
            else "FAILED"
        )
        elapsed_seconds = time_module.perf_counter() - started_at
        self.ingestion_repository.complete_segment(
            segment_id,
            status=status,
            requested_from=requested_from,
            market_watermark=market_watermark,
            record_count=bar_count,
            checksum_sha256=checksum.hexdigest(),
            gap_details={
                "failed_tickers": failed,
                "missing_tickers": sorted(missing),
                "source_interval": source_interval,
                "include_history": include_history,
                "timings_seconds": {
                    "elapsed": round(elapsed_seconds, 3),
                    "persist": round(persist_seconds, 3),
                },
            },
            completed_at=datetime.now(timezone.utc),
        )
        if status == "FAILED":
            raise RuntimeError(
                f"derived {target_interval} coverage failed: "
                f"{len(unavailable)}/{len(securities)} tickers unavailable"
            )
        return IntervalIngestionResult(
            ingestion_segment_id=segment_id,
            interval=target_interval,
            bar_count=bar_count,
            inserted_count=inserted_count,
            missing_tickers=unavailable,
            failed_tickers=tuple(sorted(failed)),
            persist_seconds=persist_seconds,
            elapsed_seconds=elapsed_seconds,
        )

    def reconcile_derived_monthly(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        watermark: DecisionWatermark,
    ) -> ReconciliationRunResult:
        month_end = watermark.market_time.date()
        month_start = date(month_end.year, month_end.month, 1)
        native_ingestion = self.ingest_native_interval(
            securities,
            interval="1mo",
            start=month_start,
            end=month_end,
            observed_at=watermark.observed_time,
        )
        tickers = tuple(row.ticker for row in securities)
        derived = self.bar_repository.list_source_at_market_time(
            tickers,
            "1mo",
            watermark.market_time,
            source_kind=BarSourceKind.DERIVED,
            observed_by=watermark.observed_time,
        )
        native = self.bar_repository.list_source_at_market_time(
            tickers,
            "1mo",
            watermark.market_time,
            source_kind=BarSourceKind.NATIVE_REST,
            observed_by=watermark.observed_time,
        )
        segment_id = uuid5(
            NAMESPACE_URL,
            "equity-monthly-reconciliation:"
            + sha256_json({
                "market_time": watermark.market_time.isoformat(),
                "derived": sorted(str(row.bar_revision_id) for row in derived.values()),
                "native": sorted(str(row.bar_revision_id) for row in native.values()),
            }),
        )
        self.ingestion_repository.start_segment(
            ingestion_segment_id=segment_id,
            provider="internal",
            provider_mode="DERIVED",
            dataset="EQUITY_BAR_RECONCILIATION",
            interval="1mo",
            requested_from=watermark.market_time,
            requested_to=watermark.market_time,
            observed_at=watermark.observed_time,
        )
        reconciled = []
        statuses = {}
        for ticker in tickers:
            if ticker not in derived and ticker not in native:
                continue
            row = reconcile_interval_bar(
                stream_bar=derived.get(ticker),
                native_bar=native.get(ticker),
                observed_at=watermark.observed_time,
                prefer_stream=True,
            )
            reconciled.append(row)
            status = row.reconciliation_status or "UNKNOWN"
            statuses[status] = statuses.get(status, 0) + 1
        inserted = self.bar_repository.persist(reconciled)
        missing = len(securities) - len(reconciled)
        status = (
            "COMPLETE" if not missing
            else "DEGRADED" if len(reconciled) / max(len(securities), 1) >= 0.95
            else "FAILED"
        )
        self.ingestion_repository.complete_segment(
            segment_id,
            status=status,
            market_watermark=watermark.market_time if reconciled else None,
            record_count=len(reconciled),
            checksum_sha256=sha256_json(sorted(
                (row.ticker, str(row.bar_revision_id), row.payload_sha256)
                for row in reconciled
            )),
            gap_details={
                "native_ingestion_segment_id": str(native_ingestion.ingestion_segment_id),
                "reconciliation_status_counts": statuses,
                "missing_tickers": sorted(set(tickers) - set(derived) - set(native)),
            },
            completed_at=datetime.now(timezone.utc),
        )
        if status == "FAILED":
            raise RuntimeError(
                f"monthly reconciliation coverage failed: {missing}/{len(securities)} missing"
            )
        return ReconciliationRunResult(
            ingestion_segment_id=segment_id,
            interval="1mo",
            pending_count=len(derived),
            native_count=len(native),
            reconciled_count=len(reconciled),
            inserted_count=inserted,
            status_counts=tuple(sorted(statuses.items())),
        )

    def materialize_interval(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        universe_run_id: UUID,
        interval: str,
        watermark: DecisionWatermark,
        run_purpose: str = "ORIGINAL",
        current_ratios: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> AnalysisRunResult:
        robust_qualifications = self.evidence_repository.robust_qualifications_as_of(
            watermark,
            interval=interval,
            horizon_key=_qualification_horizon(interval),
        )
        robust_ids = frozenset(robust_qualifications.values())
        prepared = []
        for security in securities:
            try:
                bars = self.bar_repository.list_final_as_of(
                    security.ticker,
                    interval,
                    watermark,
                    limit=_BAR_LIMIT.get(interval, 400),
                )
                confirmation_interval = {
                    "1h": "1d", "1d": "1h", "1wk": "1d", "1mo": "1wk",
                }.get(interval)
                confirmation_bars = (
                    self.bar_repository.list_final_as_of(
                        security.ticker,
                        confirmation_interval,
                        watermark,
                        limit=_BAR_LIMIT.get(confirmation_interval, 400),
                    )
                    if confirmation_interval else ()
                )
                reports = self.reference_repository.list_fundamentals_as_of(
                    security.security_id, watermark, limit=8
                )
                preparation_error = None
            except Exception as exc:
                bars = ()
                confirmation_bars = ()
                reports = ()
                preparation_error = exc
            prepared.append((
                security, bars, confirmation_bars, reports, preparation_error,
            ))
        model_bundle = {
            "bundle": MODEL_BUNDLE_VERSION,
            "channel": CHANNEL_VERSION,
            "confirmation": {
                "derived_1h": CONFIRMATION_VERSION,
                "persisted": PERSISTED_CONFIRMATION_VERSIONS,
            },
            "context": CONTEXT_POLICY_VERSION,
            "feature": FEATURE_VERSION,
            "fundamental": FUNDAMENTAL_SNAPSHOT_VERSION,
            "pattern": PATTERN_VERSION,
            "portal_strategy": PORTAL_STRATEGY_VERSION,
            "scanner": SCANNER_BUNDLE_VERSION,
            "setup": SETUP_VERSION,
        }
        model_sha256 = sha256_json(model_bundle)
        input_sha256 = sha256_json({
            "interval": interval,
            "market_time": watermark.market_time.isoformat(),
            "members": [
                {
                    "bar_revision_ids": [str(row.bar_revision_id) for row in bars],
                    "confirmation_bar_revision_ids": [
                        str(row.bar_revision_id) for row in confirmation_bars
                    ],
                    "fundamental_report_ids": [
                        str(row["fundamental_report_id"]) for row in reports
                    ],
                    "preparation_error": (
                        type(preparation_error).__name__ if preparation_error else None
                    ),
                    "ratio_input": _fingerprint_value(
                        (current_ratios or {}).get(security.ticker)
                        if run_purpose != "REPLAY" else None
                    ),
                    "security_revision_id": str(security.security_revision_id),
                    "ticker": security.ticker,
                }
                for (
                    security, bars, confirmation_bars, reports, preparation_error,
                ) in prepared
            ],
            "qualifications": sorted(
                (
                    source_name,
                    source_version,
                    registered_interval or "",
                    "" if direction is None else str(direction),
                    str(qualification_id),
                )
                for (
                    source_name, source_version, registered_interval, direction,
                ), qualification_id in robust_qualifications.items()
            ),
            "universe_run_id": str(universe_run_id),
        })
        business_key = ":".join([
            run_purpose,
            interval,
            watermark.market_time.isoformat(),
            str(universe_run_id),
            model_sha256,
            input_sha256,
        ])
        run_id = uuid5(NAMESPACE_URL, f"equity-analysis:{business_key}")
        run_record = self.analysis_repository.start_run(
            analysis_run_id=run_id,
            business_key=business_key,
            run_purpose=run_purpose,
            interval=interval,
            market_time=watermark.market_time,
            observed_at=watermark.observed_time,
            universe_run_id=universe_run_id,
            model_bundle_version=MODEL_BUNDLE_VERSION,
            model_bundle_sha256=model_sha256,
            input_sha256=input_sha256,
            members=securities,
        )
        if not run_record.get("was_created", True):
            return AnalysisRunResult(
                analysis_run_id=run_record["analysis_run_id"],
                interval=interval,
                status=run_record["status"],
                completed=run_record.get("completed_members", 0),
                insufficient=run_record.get("insufficient_members", 0),
                failed=run_record.get("failed_members", 0),
                evidence_count=0,
                inserted_evidence_count=0,
                context_count=0,
            )
        run_watermark = DecisionWatermark(
            watermark.market_time,
            run_record["observed_at"],
        )
        completed = insufficient = failed = evidence_count = 0
        inserted_evidence_count = context_count = 0
        pending_projections: list[dict[str, Any]] = []
        output_manifest = []
        for (
            security, bars, confirmation_bars, reports, preparation_error,
        ) in prepared:
            if preparation_error is not None:
                failed += 1
                self.analysis_repository.complete_member(
                    run_id, security.security_id, status="FAILED",
                    latest_bar_revision_id=None, source_bar_count=0,
                    evidence_count=0, failure_reason=str(preparation_error),
                )
                output_manifest.append({
                    "failure_type": type(preparation_error).__name__,
                    "status": "FAILED",
                    "ticker": security.ticker,
                })
                continue
            minimum_bars = _MINIMUM_BARS.get(interval, 40)
            latest_reaches_watermark = bool(
                bars and bars[-1].bar_end == watermark.market_time
            )
            if len(bars) < minimum_bars or not latest_reaches_watermark:
                insufficient += 1
                failure_reason = (
                    "latest finalized interval bar is unavailable"
                    if not latest_reaches_watermark
                    else "minimum finalized history is unavailable"
                )
                self.analysis_repository.complete_member(
                    run_id, security.security_id, status="INSUFFICIENT_DATA",
                    latest_bar_revision_id=(bars[-1].bar_revision_id if bars else None),
                    source_bar_count=len(bars), evidence_count=0,
                    failure_reason=failure_reason,
                )
                output_manifest.append({
                    "bar_revision_ids": [str(row.bar_revision_id) for row in bars],
                    "reason": failure_reason,
                    "status": "INSUFFICIENT_DATA",
                    "ticker": security.ticker,
                })
                continue
            try:
                metrics = _fundamental_metrics(
                    security,
                    reports,
                    (current_ratios or {}).get(security.ticker)
                    if run_purpose != "REPLAY" else None,
                    include_reference_metrics=(run_purpose != "REPLAY"),
                )
                report_ids = tuple(row["fundamental_report_id"] for row in reports)
                result = materialize_equity_evidence(
                    analysis_run_id=run_id,
                    security=security,
                    interval=interval,
                    bars=bars,
                    confirmation_bars=confirmation_bars,
                    observed_at=run_watermark.observed_time,
                    fundamental_metrics=metrics,
                    fundamental_report_ids=report_ids,
                    robust_qualifications=robust_qualifications,
                )
                persisted = self.evidence_repository.persist(result.evidence)
                evidence_count += len(result.evidence)
                inserted_evidence_count += persisted
                previous_visible = self.evidence_repository.list_as_of(
                    security.ticker, run_watermark
                )
                all_visible = _context_evidence(
                    previous_visible, result.evidence, interval=interval
                )
                context, links = build_equity_context(
                    security=security,
                    strategy_horizon=_strategy_horizon(interval),
                    watermark=run_watermark,
                    evidence=all_visible,
                    robust_qualification_ids=robust_ids,
                    context_policy_version=CONTEXT_POLICY_VERSION,
                    context_policy_sha256=sha256_json({
                        "model": CONTEXT_POLICY_VERSION,
                        "staleness": {"5m": 600, "15m": 1800, "30m": 3600, "1h": 7200},
                    }),
                    universe_run_id=universe_run_id,
                    use_security_reference_metrics=(run_purpose != "REPLAY"),
                )
                self.evidence_repository.persist_context(context, links)
                context_count += 1
                self.analysis_repository.complete_member(
                    run_id, security.security_id, status="COMPLETE",
                    latest_bar_revision_id=bars[-1].bar_revision_id,
                    source_bar_count=len(bars), evidence_count=len(result.evidence),
                )
                completed += 1
                pending_projections.extend({
                    "ticker": security.ticker,
                    "interval_key": interval,
                    "projection_type": row.evidence_type.value,
                    "source_name": row.source_name,
                    "evidence_id": row.evidence_id,
                    "equity_context_snapshot_id": None,
                    "market_time": row.market_time,
                    "observed_at": row.observed_at,
                    "payload": json.loads(row.payload_json),
                } for row in result.evidence)
                pending_projections.append({
                    "ticker": security.ticker,
                    "interval_key": interval,
                    "projection_type": "EQUITY_CONTEXT",
                    "source_name": CONTEXT_POLICY_VERSION,
                    "evidence_id": None,
                    "equity_context_snapshot_id": context.equity_context_snapshot_id,
                    "market_time": context.market_time,
                    "observed_at": context.observed_at,
                    "payload": json.loads(context.summary_json),
                })
                output_manifest.append({
                    "context_id": str(context.equity_context_snapshot_id),
                    "evidence_ids": sorted(str(row.evidence_id) for row in result.evidence),
                    "status": "COMPLETE",
                    "ticker": security.ticker,
                })
            except Exception as exc:
                failed += 1
                self.analysis_repository.complete_member(
                    run_id, security.security_id, status="FAILED",
                    latest_bar_revision_id=bars[-1].bar_revision_id,
                    source_bar_count=len(bars), evidence_count=0,
                    failure_reason=str(exc),
                )
                output_manifest.append({
                    "failure_type": type(exc).__name__,
                    "status": "FAILED",
                    "ticker": security.ticker,
                })
        output_sha256 = sha256_json(sorted(output_manifest, key=lambda row: row["ticker"]))
        run = self.analysis_repository.publish_run(
            run_id,
            output_sha256=output_sha256,
            projections=pending_projections if run_purpose == "ORIGINAL" else (),
        )
        return AnalysisRunResult(
            analysis_run_id=run_id,
            interval=interval,
            status=run["status"],
            completed=completed,
            insufficient=insufficient,
            failed=failed,
            evidence_count=evidence_count,
            inserted_evidence_count=inserted_evidence_count,
            context_count=context_count,
        )

    def evaluate_directional_outcomes(
        self,
        policy: OutcomePolicy,
        horizon_key: str,
        *,
        available_by: datetime,
        market_benchmark: str = "SPY",
        finalize_unavailable: bool = False,
        signal_observed_through: datetime | None = None,
        prospective_only: bool = False,
        historical_reconstructed_only: bool = False,
        adjusted: bool = False,
        subject_evidence_ids: Sequence[UUID] = (),
        limit: int = 5000,
    ) -> OutcomeRunResult:
        available_utc = _utc(available_by)
        self.outcome_repository.persist_policy(policy)
        subjects = self.outcome_repository.list_pending_directional_subjects(
            policy,
            horizon_key,
            available_by=available_utc,
            signal_observed_through=(
                _utc(signal_observed_through)
                if signal_observed_through is not None else None
            ),
            prospective_only=prospective_only,
            subject_evidence_ids=subject_evidence_ids,
            limit=limit,
        )
        outcomes = []
        pending = 0
        revision_context = self.outcome_repository.outcome_revision_context(
            tuple(subject.evidence_id for subject in subjects),
            policy,
            horizon_key,
        )
        horizon_count = int(json.loads(policy.horizons_json)[horizon_key])
        benchmark_policy = json.loads(policy.benchmark_policy_json)
        for subject in subjects:
            interval = policy.interval or subject.interval or "30m"
            bars = self._outcome_path(
                subject.ticker,
                interval,
                after=subject.observed_at,
                available_by=available_utc,
                limit=horizon_count,
                historical_reconstructed_only=historical_reconstructed_only,
                adjusted=adjusted,
            )
            market_ticker = resolve_benchmark_ticker(
                subject.ticker,
                str(benchmark_policy.get("market") or market_benchmark),
            )
            benchmark_bars = self._outcome_path(
                market_ticker,
                interval,
                after=subject.observed_at,
                available_by=available_utc,
                limit=horizon_count,
                historical_reconstructed_only=historical_reconstructed_only,
                adjusted=adjusted,
            )
            sector_ticker = None
            sector_bars = ()
            if benchmark_policy.get("sector"):
                reference = (
                    self.reference_repository.get_security_revision(
                        subject.security_revision_id
                    )
                    if subject.security_revision_id else None
                )
                sector_ticker = resolve_benchmark_ticker(
                    subject.ticker,
                    sector_benchmark_ticker(
                        reference.sector if reference is not None else None
                    ),
                )
                sector_bars = (
                    benchmark_bars
                    if sector_ticker == market_ticker else
                    self._outcome_path(
                        sector_ticker,
                        interval,
                        after=subject.observed_at,
                        available_by=available_utc,
                        limit=horizon_count,
                        historical_reconstructed_only=historical_reconstructed_only,
                        adjusted=adjusted,
                    )
                )
            outcome_revision, supersedes_outcome_id = revision_context.get(
                subject.evidence_id, (1, None)
            )
            outcome = evaluate_directional_outcome(
                subject,
                policy,
                horizon_key,
                bars,
                market_bars=benchmark_bars,
                sector_bars=sector_bars,
                market_benchmark_ticker=market_ticker,
                sector_benchmark_ticker=sector_ticker,
                outcome_revision=outcome_revision,
            )
            if supersedes_outcome_id is not None:
                outcome = replace(
                    outcome,
                    supersedes_outcome_id=supersedes_outcome_id,
                )
            if outcome.entry_status == "UNAVAILABLE" and not finalize_unavailable:
                pending += 1
                continue
            outcomes.append(outcome)
        persisted = self.outcome_repository.persist_outcomes(outcomes)
        return OutcomeRunResult(
            policy_key=policy.policy_key,
            horizon_key=horizon_key,
            due=len(subjects),
            persisted=persisted,
            pending=pending,
        )

    def _outcome_path(
        self,
        ticker: str,
        interval: str,
        *,
        after: datetime,
        available_by: datetime,
        limit: int,
        historical_reconstructed_only: bool,
        adjusted: bool = False,
    ):
        fetch_limit = max(limit, self.outcome_path_prefetch_limit or limit)
        cache_key = (
            ticker, interval, after, available_by,
            historical_reconstructed_only, adjusted,
        )
        cached = self._outcome_path_cache.get(cache_key)
        if cached is not None and cached[0] >= limit:
            return cached[1][:limit]
        path = tuple(self.bar_repository.list_final_after(
            ticker,
            interval,
            after=after,
            available_by=available_by,
            limit=fetch_limit,
            historical_reconstructed_only=historical_reconstructed_only,
            adjusted=adjusted,
        ))
        if self.outcome_path_cache_size:
            if len(self._outcome_path_cache) >= self.outcome_path_cache_size:
                self._outcome_path_cache.clear()
            self._outcome_path_cache[cache_key] = (fetch_limit, path)
        return path[:limit]

    def clear_outcome_path_cache(self) -> None:
        self._outcome_path_cache.clear()

    def qualify_materialized_outcomes(
        self,
        *,
        available_by: datetime,
        interval: str | None = None,
        evaluation_version: str = "equity_qualification_v1",
        source_names: Sequence[str] = (),
    ) -> QualificationRunResult:
        import pandas as pd

        rows = self.outcome_repository.qualification_observations(
            available_by=_utc(available_by),
            interval=interval,
            source_names=source_names,
        )
        revisions = qualify_outcomes(
            pd.DataFrame(rows),
            effective_from=_utc(available_by),
            evaluation_version=evaluation_version,
        )
        written = self.outcome_repository.persist_qualification_revisions(revisions)
        return QualificationRunResult(
            observations=len(rows),
            revisions=written,
            robust_passes=sum(
                row.qualification_state == "ROBUST_PASS" for row in revisions
            ),
        )

    def replay_interval_range(
        self,
        securities: Sequence[SecurityReferenceRevision],
        *,
        universe_run_id: UUID,
        interval: str,
        start: datetime,
        end: datetime,
        available_by: datetime,
        max_runs: int = 10000,
    ) -> tuple[AnalysisRunResult, ...]:
        if max_runs <= 0:
            raise ValueError("max_runs must be positive")
        market_times = self.bar_repository.common_market_times(
            tuple(row.ticker for row in securities),
            interval,
            start=_utc(start),
            end=_utc(end),
            available_by=_utc(available_by),
            limit=max_runs,
        )
        return tuple(
            self.materialize_interval(
                securities,
                universe_run_id=universe_run_id,
                interval=interval,
                watermark=DecisionWatermark(market_time, market_time),
                run_purpose="REPLAY",
                current_ratios=None,
            )
            for market_time in market_times
        )


def _fundamental_metrics(
    security: SecurityReferenceRevision,
    reports: Sequence[Mapping[str, Any]],
    ratio: Mapping[str, Any] | None,
    *,
    include_reference_metrics: bool = True,
) -> dict[str, Any]:
    latest = reports[0] if reports else {}
    ratio = ratio or {}
    long_debt = _decimal(latest.get("long_term_debt")) or Decimal("0")
    current_debt = _decimal(latest.get("current_debt")) or Decimal("0")
    cash = _decimal(latest.get("cash_and_equivalents")) or Decimal("0")
    market_cap = _decimal(ratio.get("market_cap")) or (
        security.market_cap if include_reference_metrics else None
    )
    enterprise_value = (
        _decimal(ratio.get("enterprise_value"))
        or (market_cap + long_debt + current_debt - cash if market_cap is not None else None)
    )
    metrics = {
        "current_ratio": ratio.get("current"),
        "debt_to_equity": ratio.get("debt_to_equity"),
        "dividend_yield": ratio.get("dividend_yield"),
        "ebitda": latest.get("ebitda"),
        "enterprise_value": enterprise_value,
        "ev_to_ebitda": ratio.get("ev_to_ebitda"),
        "free_cash_flow": latest.get("free_cash_flow") or ratio.get("free_cash_flow"),
        "free_float": security.free_float if include_reference_metrics else None,
        "market_cap": market_cap,
        "operating_income": latest.get("operating_income"),
        "price_to_earnings": ratio.get("price_to_earnings"),
        "quick_ratio": ratio.get("quick"),
        "return_on_assets": ratio.get("return_on_assets"),
        "return_on_equity": ratio.get("return_on_equity"),
        "shares_outstanding": (
            security.weighted_shares if include_reference_metrics else None
        ),
    }
    return {key: value for key, value in metrics.items() if value is not None}


def _strategy_horizon(interval: str) -> str:
    return {
        "5m": "INTRADAY_5M",
        "15m": "INTRADAY_15M",
        "30m": "INTRADAY_30M",
        "1h": "SWING_1H",
        "1d": "SWING_1D",
    }.get(interval, interval.upper())


def _context_evidence(
    previous: Sequence[EquityEvidence],
    current: Sequence[EquityEvidence],
    *,
    interval: str,
) -> tuple[EquityEvidence, ...]:
    current_ids = {row.evidence_id for row in current}
    return tuple(
        row for row in previous
        if row.evidence_id not in current_ids
        and row.interval != interval
        and _evidence_allowed_by_interval(row)
    ) + tuple(current)


def _fingerprint_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _fingerprint_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, (date, datetime, Decimal, UUID)):
        return str(value)
    if isinstance(value, float) and (
        value != value or value in (float("inf"), float("-inf"))
    ):
        return None
    return value


def _evidence_allowed_by_interval(evidence: EquityEvidence) -> bool:
    if evidence.evidence_type is EvidenceType.SCANNER_RESULT:
        return evidence.interval in SCANNER_INTERVALS
    if evidence.evidence_type is EvidenceType.TRADE_SETUP:
        return evidence.interval in SETUP_INTERVALS
    if evidence.evidence_type is EvidenceType.FUNDAMENTAL_SNAPSHOT:
        return evidence.source_version == FUNDAMENTAL_SNAPSHOT_VERSION
    return True


def _qualification_horizon(interval: str) -> str:
    return {
        "5m": "15m",
        "15m": "30m",
        "30m": "60m",
        "1h": "7h",
        "1d": "5d",
    }.get(interval, interval)


def _batches(values: Sequence[str]) -> Iterable[tuple[str, ...]]:
    for offset in range(0, len(values), _BATCH_SIZE):
        yield tuple(values[offset:offset + _BATCH_SIZE])


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    return result if result.is_finite() else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)