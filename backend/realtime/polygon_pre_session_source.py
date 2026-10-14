from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Protocol

from options.analytics.marks import UnderlyingMinuteBar, fresh_mark_window
from options.calendar import OptionExchangeCalendar
from options.data.normalizer import (
    DeveloperNormalizationInput,
    DeveloperOptionNormalizer,
    parse_polygon_snapshot,
)
from options.data.polygon_developer import (
    ADJUSTED_START_PRICE_BASIS,
    RAW_FINALIZED_PRICE_BASIS,
    MinuteBarCoverage,
)
from options.domain import DataQualityFlag, OptionContractCatalogEntry
from options.model_inputs import (
    DefaultedExpirationValuationInput,
    ExpirationValuationCandidate,
)

from .option_discovery_repository import RealtimeOptionDiscoveryEvidence
from .option_pre_session_producer import (
    CompletedOptionDiscoveryPackage,
    OptionPreSessionCollection,
    OptionPreSessionRequest,
    PreSessionNormalizationDiagnostics,
)
from .option_session_manifest import OptionManifestCandidate
from .polygon_option_discovery import PolygonOptionDiscoverySource


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _counters(provider_requests: int, response_bytes: int, name: str) -> None:
    if (
        type(provider_requests) is not int
        or provider_requests < 0
        or type(response_bytes) is not int
        or response_bytes < 0
    ):
        raise ValueError(f"{name} counters cannot be negative")


@dataclass(frozen=True, slots=True)
class PreSessionSpotResult:
    price: Decimal
    provider_requests: int
    response_bytes: int

    def __post_init__(self) -> None:
        if not self.price.is_finite() or self.price <= 0:
            raise ValueError("pre-session spot must be positive and finite")
        _counters(self.provider_requests, self.response_bytes, "pre-session spot")


@dataclass(frozen=True, slots=True)
class AdmittedOptionCatalogEntry:
    catalog_version_id: int
    catalog: OptionContractCatalogEntry

    def __post_init__(self) -> None:
        if type(self.catalog_version_id) is not int or self.catalog_version_id <= 0:
            raise ValueError("pre-session catalog version must be positive")


@dataclass(frozen=True, slots=True)
class PreSessionCatalogResult:
    entries: tuple[AdmittedOptionCatalogEntry, ...]
    provider_requests: int
    response_bytes: int

    def __post_init__(self) -> None:
        tickers = tuple(row.catalog.contract_ticker for row in self.entries)
        contract_ids = tuple(row.catalog.contract_id for row in self.entries)
        version_ids = tuple(row.catalog_version_id for row in self.entries)
        if (
            len(tickers) != len(set(tickers))
            or len(contract_ids) != len(set(contract_ids))
            or len(version_ids) != len(set(version_ids))
        ):
            raise ValueError("pre-session catalog identities must be unique")
        _counters(self.provider_requests, self.response_bytes, "pre-session catalog")


@dataclass(frozen=True, slots=True)
class PreSessionBarsResult:
    bars: tuple[UnderlyingMinuteBar, ...]
    provider_requests: int
    response_bytes: int
    coverage: MinuteBarCoverage | None = None

    def __post_init__(self) -> None:
        _counters(self.provider_requests, self.response_bytes, "pre-session bars")


def _diagnostics(underlyer, bar_acquisition, coverage, bars_result, mark_times, raw_rows,
                 usable_mark_count, candidates, rejected_counts) -> PreSessionNormalizationDiagnostics:
    counts = tuple(rejected_counts)
    if not candidates and not counts:
        # Never leave a zero-candidate underlyer without a reason.
        counts = (("NO_RAW_OBSERVATIONS", 1),) if not raw_rows else (
            ("NO_CANDIDATE_REASON_RECORDED", len(raw_rows)),)
    return PreSessionNormalizationDiagnostics(
        underlyer=underlyer,
        bar_acquisition=bar_acquisition,
        requested_from=coverage.requested_from if coverage else None,
        requested_to=coverage.requested_to if coverage else None,
        returned_from=coverage.returned_from if coverage else None,
        returned_to=coverage.returned_to if coverage else None,
        bar_count=coverage.bar_count if coverage else len(bars_result.bars),
        pages_fetched=coverage.pages_fetched if coverage else 0,
        pagination_complete=coverage.pagination_complete if coverage else True,
        price_basis=coverage.price_basis if coverage else "NOT_APPLICABLE",
        observation_min=min(mark_times) if mark_times else None,
        observation_max=max(mark_times) if mark_times else None,
        raw_rows=len(raw_rows),
        usable_mark_count=usable_mark_count,
        candidates=len(candidates),
        rejection_counts=counts,
    )


class PreSessionSpotSource(Protocol):
    def __call__(self, underlyer: str, as_of: datetime) -> PreSessionSpotResult:
        ...


class PreSessionCatalogSource(Protocol):
    def __call__(
        self,
        underlyer: str,
        as_of: date,
        expiration_through: date,
        strike_min: Decimal,
        strike_max: Decimal,
    ) -> PreSessionCatalogResult:
        ...


class PreSessionBarsSource(Protocol):
    def __call__(
        self,
        underlyer: str,
        start: datetime,
        end: datetime,
    ) -> PreSessionBarsResult:
        ...


def _raw_observations(discovery_result):
    rows = []
    for page in discovery_result.batch.pages:
        payload = json.loads(page.response_bytes)
        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError("pre-session discovery page lost its results array")
        rows.extend(
            parse_polygon_snapshot(row, page.received_at)
            for row in results
        )
    return tuple(rows)


class PolygonOptionPreSessionSource:
    def __init__(
        self,
        discovery_source: PolygonOptionDiscoverySource,
        normalizer: DeveloperOptionNormalizer,
        *,
        spot_source: PreSessionSpotSource,
        catalog_source: PreSessionCatalogSource,
        bars_source: PreSessionBarsSource,
        risk_free_rate: float | None = None,
        dividend_yields: Mapping[str, float] | None = None,
        valuation_source: Callable | None = None,
        calendar: OptionExchangeCalendar | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        discovery_metrics: Callable[[], tuple[int, int]] | None = None,
    ) -> None:
        if ((valuation_source is None and (risk_free_rate is None or dividend_yields is None
                or set(dividend_yields) != set(discovery_source.scope.universe.option_underlyings)))
                or (valuation_source is not None and (risk_free_rate is not None or dividend_yields is not None))):
            raise ValueError("pre-session dividend yields must cover the exact universe")
        self.discovery_source = discovery_source
        self.normalizer = normalizer
        self.spot_source = spot_source
        self.catalog_source = catalog_source
        self.bars_source = bars_source
        self.risk_free_rate = float(risk_free_rate) if risk_free_rate is not None else None
        self.dividend_yields = ({ticker: float(value) for ticker, value in dividend_yields.items()}
            if dividend_yields is not None else None)
        self.valuation_source = valuation_source
        self.calendar = calendar or OptionExchangeCalendar()
        self.clock = clock
        self.discovery_metrics = discovery_metrics

    def _collect_underlyer(
        self,
        request: OptionPreSessionRequest,
        underlyer: str,
    ) -> tuple[CompletedOptionDiscoveryPackage, int, int]:
        spot = self.spot_source(underlyer, request.scheduled_cycle)
        discovery_before = (
            self.discovery_metrics() if self.discovery_metrics is not None else None
        )
        discovery = self.discovery_source.discover(
            underlyer,
            request.scheduled_cycle,
            spot.price,
        )
        discovery_after = (
            self.discovery_metrics() if self.discovery_metrics is not None else None
        )
        evidence = RealtimeOptionDiscoveryEvidence(
            request.scope,
            request.security_id_by_underlyer[underlyer],
            discovery,
        )
        catalog_result = self.catalog_source(
            underlyer,
            evidence.expiration_from,
            evidence.expiration_through,
            evidence.strike_min,
            evidence.strike_max,
        )
        if any(row.catalog.underlyer != underlyer for row in catalog_result.entries):
            raise ValueError("pre-session catalog changed the reviewed underlying")
        catalogs = {
            row.catalog.contract_ticker: row for row in catalog_result.entries
        }
        raw_rows = _raw_observations(discovery)
        raw_tickers = {row.contract_ticker for row in raw_rows}
        if raw_tickers - set(catalogs):
            raise ValueError("pre-session discovery contains unadmitted catalog identities")
        mark_times = tuple(
            row.option_mark_time for row in raw_rows
            if row.option_mark_time is not None
        )
        valuation_policy = getattr(self.normalizer, "valuation_policy", None)
        maximum_source_age = (
            valuation_policy.maximum_source_age_seconds if valuation_policy is not None else 1800
        )
        preselect_at = _utc(self.clock(), "clock")
        usable_window = fresh_mark_window(mark_times, preselect_at, maximum_source_age)
        earliest_usable = preselect_at - timedelta(seconds=maximum_source_age)
        usable_mark_count = sum(1 for mark in mark_times if earliest_usable <= mark <= preselect_at)
        if usable_window is not None:
            # Only causally usable marks bound the read; stale rows stay auditable but cannot
            # widen it. Freshness is rechecked at normalization with its own later clock.
            bars_result = self.bars_source(
                underlyer,
                usable_window[0] - timedelta(minutes=1),
                usable_window[1],
            )
            bar_acquisition = "FETCHED"
        else:
            bars_result = PreSessionBarsResult((), 0, 0)
            bar_acquisition = "SKIPPED_NO_USABLE_MARKS"
        coverage = bars_result.coverage
        if coverage is not None:
            expected_basis = (
                RAW_FINALIZED_PRICE_BASIS
                if valuation_policy is not None and valuation_policy.underlying_price_basis
                == RAW_FINALIZED_PRICE_BASIS else ADJUSTED_START_PRICE_BASIS
            )
            if coverage.price_basis != expected_basis:
                raise ValueError("pre-session bar price basis does not match the valuation policy")
            if not coverage.pagination_complete:
                raise ValueError("pre-session underlying bars were incomplete")
        valuation_candidates = (self.valuation_source(underlyer,
            request.security_id_by_underlyer[underlyer], spot.price, request.session_date,
            tuple(sorted({row.catalog.expiration_date for row in catalog_result.entries})))
            if self.valuation_source is not None else None)
        if valuation_candidates is not None and set(valuation_candidates) != {
                row.catalog.expiration_date for row in catalog_result.entries}:
            raise ValueError("pre-session expiration valuation coverage is incomplete")
        if valuation_candidates is not None and any(not isinstance(row,
                (ExpirationValuationCandidate, DefaultedExpirationValuationInput))
                for row in valuation_candidates.values()):
            raise ValueError("pre-session expiration valuation input type is invalid")
        normalized_observed_at = _utc(self.clock(), "clock")
        inputs = tuple(
            DeveloperNormalizationInput(
                raw=raw,
                catalog=catalogs[raw.contract_ticker].catalog,
                underlying_bars=bars_result.bars,
                expiration_cutoff=self.calendar.expiration_cutoff(
                    catalogs[raw.contract_ticker].catalog.expiration_date
                ),
                risk_free_rate=(valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date].risk_free_rate
                    if valuation_candidates is not None else self.risk_free_rate),
                dividend_yield=(valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date].dividend_yield
                    if valuation_candidates is not None else self.dividend_yields[underlyer]),
                dividend_cash_flows=(valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date].dividend_cash_flows
                    if valuation_candidates is not None and isinstance(
                        valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date],
                        ExpirationValuationCandidate) else ()),
                dividend_coverage_available=(valuation_candidates is not None and isinstance(
                    valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date],
                    ExpirationValuationCandidate)),
                input_quality_flags=((DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,)
                    if valuation_candidates is not None and isinstance(
                        valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date],
                        DefaultedExpirationValuationInput) else ()),
                normalized_observed_at=normalized_observed_at,
                valuation_candidate=(valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date]
                    if valuation_candidates is not None and isinstance(
                        valuation_candidates[catalogs[raw.contract_ticker].catalog.expiration_date],
                        ExpirationValuationCandidate) else None),
            )
            for raw in raw_rows
        )
        normalized = self.normalizer.normalize(discovery.batch.batch_id, inputs)
        candidates = tuple(
            OptionManifestCandidate(
                catalog_version_id=catalogs[snapshot.contract_ticker].catalog_version_id,
                catalog=catalogs[snapshot.contract_ticker].catalog,
                snapshot=snapshot,
                discovery_evidence_sha256=evidence.evidence_sha256,
            )
            for snapshot in normalized.matrix_snapshots
        )
        package = CompletedOptionDiscoveryPackage(
            evidence,
            candidates,
            _diagnostics(
                underlyer, bar_acquisition, coverage, bars_result, mark_times, raw_rows,
                usable_mark_count, candidates, normalized.rejected_counts,
            ),
        )
        if discovery_before is None or discovery_after is None:
            discovery_requests = len(discovery.batch.pages)
            discovery_bytes = evidence.response_bytes
        else:
            discovery_requests = discovery_after[0] - discovery_before[0]
            discovery_bytes = discovery_after[1] - discovery_before[1]
            _counters(discovery_requests, discovery_bytes, "pre-session discovery")
        return (
            package,
            discovery_requests
            + spot.provider_requests
            + catalog_result.provider_requests
            + bars_result.provider_requests,
            discovery_bytes
            + spot.response_bytes
            + catalog_result.response_bytes
            + bars_result.response_bytes,
        )

    def collect_underlyer(self, request: OptionPreSessionRequest, underlyer: str):
        if not isinstance(request, OptionPreSessionRequest):
            raise TypeError("pre-session source request is invalid")
        if request.scope != self.discovery_source.scope or underlyer not in request.scope.universe.option_underlyings:
            raise ValueError("pre-session staged source scope/underlying drifted")
        return self._collect_underlyer(request, underlyer)

    def account_counters(self):
        return self.discovery_metrics() if self.discovery_metrics is not None else (0, 0)

    def collect(self, request: OptionPreSessionRequest) -> OptionPreSessionCollection:
        if not isinstance(request, OptionPreSessionRequest):
            raise TypeError("pre-session source request is invalid")
        if request.scope != self.discovery_source.scope:
            raise ValueError("pre-session source scope drifted")
        packages = []
        provider_requests = 0
        response_bytes = 0
        for underlyer in request.scope.universe.option_underlyings:
            package, requests, byte_count = self.collect_underlyer(request, underlyer)
            packages.append(package)
            provider_requests += requests
            response_bytes += byte_count
            if provider_requests > request.maximum_provider_requests:
                raise ValueError("pre-session source exceeded the request bound")
            if response_bytes > request.maximum_response_bytes:
                raise ValueError("pre-session source exceeded the response-byte bound")
        return OptionPreSessionCollection(
            tuple(packages),
            provider_requests,
            response_bytes,
        )
