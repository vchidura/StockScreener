from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import quote, urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)
from equity.derivation import derive_canonical_bars
from equity.polygon import normalize_native_bars
from options.analytics.marks import UnderlyingMinuteBar
from options.data.normalizer import (
    DeveloperNormalizationInput,
    DeveloperOptionNormalizer,
    parse_polygon_snapshot,
)
from options.data.polygon_http import PolygonHttpTransport
from options.domain import OptionContractCatalogEntry
from provider_endpoints import REST_BASE_URL, REST_HOSTS

from .acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
    DeveloperOptionSnapshotAcquisitionFact,
)
from .acquisition_producer import (
    CompletedDeveloperSourceBatch,
    DeveloperAcquisitionRequest,
    DeveloperSourceResult,
    MAX_DEVELOPER_SOURCE_FACTS,
)
from .contracts import CANARY_UNIVERSE, canonical_json, canonical_sha256
from .polygon_option_discovery import (
    PolygonAdvancedOptionDiscoverySource,
    PolygonDeveloperOptionDiscoveryBinding,
    PolygonDeveloperOptionDiscoverySource,
    PolygonOptionDiscoveryBinding,
    PolygonOptionDiscoverySource,
)


POLYGON_DEVELOPER_PROVIDER = "polygon"
POLYGON_DEVELOPER_EQUITY_ADAPTER_VERSION = "polygon_developer_equity_v1"
POLYGON_DEVELOPER_OPTION_ADAPTER_VERSION = "polygon_developer_option_v1"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PolygonDeveloperSourceLimits:
    maximum_requests: int = 24
    maximum_response_bytes: int = 2 * 1024 * 1024
    maximum_total_bytes: int = 32 * 1024 * 1024
    maximum_rows: int = 1_000
    maximum_equity_rows_per_ticker: int = 125

    def __post_init__(self) -> None:
        values = (
            self.maximum_requests,
            self.maximum_response_bytes,
            self.maximum_total_bytes,
            self.maximum_rows,
            self.maximum_equity_rows_per_ticker,
        )
        if any(type(value) is not int or value <= 0 for value in values):
            raise ValueError("Polygon Developer source limits must be positive integers")
        if self.maximum_response_bytes > self.maximum_total_bytes:
            raise ValueError("Polygon response limit exceeds its total byte limit")
        if self.maximum_equity_rows_per_ticker * len(
            CANARY_UNIVERSE.equity_tickers
        ) > self.maximum_rows:
            raise ValueError("Polygon equity row limits exceed the source row bound")


@dataclass(frozen=True, slots=True)
class DeveloperEquityIdentity:
    ticker: str
    security_id: UUID
    instrument_id: int

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        if ticker not in CANARY_UNIVERSE.equity_tickers:
            raise ValueError("Developer equity identity is outside the canary")
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("Developer equity instrument ID must be positive")
        object.__setattr__(self, "ticker", ticker)


@dataclass(frozen=True, slots=True)
class DeveloperOptionIdentity:
    instrument_id: int
    catalog: OptionContractCatalogEntry
    expiration_cutoff: datetime

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("Developer option instrument ID must be positive")
        if self.catalog.underlyer not in CANARY_UNIVERSE.option_underlyings:
            raise ValueError("Developer option identity is outside the canary")
        object.__setattr__(
            self,
            "expiration_cutoff",
            _utc(self.expiration_cutoff, "expiration_cutoff"),
        )


class _BoundedPolygonDeveloperSource:
    def __init__(
        self,
        transport: PolygonHttpTransport,
        *,
        base_url: str,
        limits: PolygonDeveloperSourceLimits,
        clock: Callable[[], datetime],
        bar_window: Callable[[datetime], tuple[int, int]] | None = None,
    ) -> None:
        parsed = urlsplit(base_url.rstrip("/"))
        if parsed.scheme != "https" or parsed.hostname not in REST_HOSTS:
            raise ValueError("Polygon Developer source requires the approved HTTPS host")
        self.transport = transport
        self.base_url = base_url.rstrip("/")
        self.limits = limits
        self.clock = clock
        self.bar_window = bar_window
        self._requests = 0
        self._response_bytes = 0
        self._rows = 0

    def _begin(self) -> None:
        self._requests = 0
        self._response_bytes = 0
        self._rows = 0

    def _request(
        self,
        path: str,
        params: Mapping[str, str],
    ) -> tuple[tuple[dict[str, object], ...], str, datetime]:
        if self._requests >= self.limits.maximum_requests:
            raise ValueError("Polygon Developer source exceeded its request bound")
        url = f"{self.base_url}/{path.lstrip('/')}"
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname not in REST_HOSTS:
            raise ValueError("Polygon Developer request escaped the approved host")
        response = self.transport.get(url, params)
        self._requests += 1
        response_bytes = len(response.body)
        if response_bytes > self.limits.maximum_response_bytes:
            raise ValueError("Polygon Developer response exceeded its byte bound")
        self._response_bytes += response_bytes
        if self._response_bytes > self.limits.maximum_total_bytes:
            raise ValueError("Polygon Developer source exceeded its total byte bound")
        if response.status_code != 200:
            raise ValueError(
                f"Polygon Developer request failed with HTTP {response.status_code}"
            )
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("Polygon Developer response is not valid UTF-8 JSON") from error
        if not isinstance(payload, dict) or str(payload.get("status") or "").upper() not in {
            "OK",
            "DELAYED",
        }:
            raise ValueError("Polygon Developer response status is invalid")
        if payload.get("next_url"):
            raise ValueError("Polygon Developer canary response requires pagination")
        results = payload.get("results")
        if not isinstance(results, list) or any(not isinstance(row, dict) for row in results):
            raise ValueError("Polygon Developer response has an invalid results array")
        self._rows += len(results)
        if self._rows > self.limits.maximum_rows:
            raise ValueError("Polygon Developer source exceeded its row bound")
        return (
            tuple(dict(row) for row in results),
            hashlib.sha256(response.body).hexdigest(),
            _utc(self.clock(), "clock"),
        )

    @property
    def counters(self) -> tuple[int, int]:
        return self._requests, self._response_bytes

    def _minute_aggregate_path(
        self,
        ticker: str,
        requested_at: datetime,
        *,
        lookback_days: int,
    ) -> str:
        if self.bar_window is None:
            start = (requested_at - timedelta(days=lookback_days)).date().isoformat()
            end = requested_at.date().isoformat()
        else:
            start_ms, end_ms = self.bar_window(requested_at)
            if (
                type(start_ms) is not int
                or type(end_ms) is not int
                or start_ms < 0
                or end_ms < start_ms
                or end_ms - start_ms > 60_000
            ):
                raise ValueError("Polygon completed-minute window is invalid")
            start, end = str(start_ms), str(end_ms)
        return (
            f"/v2/aggs/ticker/{quote(ticker, safe='')}/range/1/minute/"
            f"{start}/{end}"
        )


class PolygonDeveloperEquitySource(_BoundedPolygonDeveloperSource):
    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(
        self,
        transport: PolygonHttpTransport,
        identities: tuple[DeveloperEquityIdentity, ...],
        *,
        base_url: str = REST_BASE_URL,
        limits: PolygonDeveloperSourceLimits = PolygonDeveloperSourceLimits(),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        lookback_days: int = 4,
        bar_window: Callable[[datetime], tuple[int, int]] | None = None,
    ) -> None:
        if {identity.ticker for identity in identities} != set(
            CANARY_UNIVERSE.equity_tickers
        ):
            raise ValueError("Polygon equity identities must cover the exact canary")
        if len({identity.instrument_id for identity in identities}) != len(identities):
            raise ValueError("Polygon equity instrument IDs must be unique")
        if type(lookback_days) is not int or not 1 <= lookback_days <= 10:
            raise ValueError("Polygon equity lookback must be in [1, 10]")
        super().__init__(
            transport,
            base_url=base_url,
            limits=limits,
            clock=clock,
            bar_window=bar_window,
        )
        self.identities = tuple(sorted(identities, key=lambda row: row.ticker))
        self.lookback_days = lookback_days

    def collect(self, request: DeveloperAcquisitionRequest) -> DeveloperSourceResult:
        if request.universe != CANARY_UNIVERSE:
            raise ValueError("Polygon equity request changed the reviewed universe")
        self._begin()
        batches = []
        for identity in self.identities:
            started_at = _utc(self.clock(), "clock")
            results, response_sha256, completed_at = self._request(
                self._minute_aggregate_path(
                    identity.ticker,
                    request.requested_at,
                    lookback_days=self.lookback_days,
                ),
                {
                    "adjusted": "false",
                    "sort": "asc",
                    "limit": str(self.limits.maximum_equity_rows_per_ticker),
                },
            )
            if len(results) > self.limits.maximum_equity_rows_per_ticker:
                raise ValueError("Polygon equity response exceeded its ticker row bound")
            batch_id = uuid5(
                NAMESPACE_URL,
                f"polygon-developer-equity:{identity.ticker}:"
                f"{request.requested_at.isoformat()}:{response_sha256}",
            )
            facts = []
            for index, row in enumerate(results):
                try:
                    bar_start = datetime.fromtimestamp(
                        int(row["t"]) / 1000,
                        tz=timezone.utc,
                    )
                    bar_end = bar_start + timedelta(minutes=1)
                    if bar_end > request.requested_at:
                        continue
                    row_sha256 = canonical_sha256(row)
                    revision = EquityBarRevision(
                        bar_revision_id=uuid5(
                            NAMESPACE_URL,
                            f"polygon-developer-bar:{batch_id}:{index}:{row_sha256}",
                        ),
                        security_id=identity.security_id,
                        ticker=identity.ticker,
                        interval="1m",
                        session_date=bar_start.date(),
                        bar_start=bar_start,
                        bar_end=bar_end,
                        open_price=Decimal(str(row["o"])),
                        high_price=Decimal(str(row["h"])),
                        low_price=Decimal(str(row["l"])),
                        close_price=Decimal(str(row["c"])),
                        volume=Decimal(str(row.get("v") or 0)),
                        vwap=(
                            Decimal(str(row["vw"]))
                            if row.get("vw") is not None
                            else None
                        ),
                        transaction_count=(
                            int(row["n"]) if isinstance(row.get("n"), int) else None
                        ),
                        source_kind=BarSourceKind.NATIVE_REST,
                        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                        is_final=True,
                        system_observed_at=completed_at,
                        replay_available_at=None,
                        adjusted=False,
                        payload_sha256=row_sha256,
                        quality_codes=(
                            POLYGON_DEVELOPER_EQUITY_ADAPTER_VERSION,
                            f"SOURCE_RESPONSE_SHA256:{response_sha256}",
                        ),
                        provider_published_at=bar_end,
                        ingestion_segment_id=batch_id,
                        session_scope=BarSessionScope.RTH,
                    )
                except (KeyError, TypeError, ValueError, ArithmeticError) as error:
                    raise ValueError("Polygon equity aggregate row is invalid") from error
                facts.append(
                    DeveloperEquityBarAcquisitionFact(identity.instrument_id, revision)
                )
            if not facts:
                raise ValueError("Polygon equity source returned no completed bars")
            batches.append(CompletedDeveloperSourceBatch(
                self.fact_kind,
                batch_id,
                started_at,
                completed_at,
                tuple(facts),
            ))
        requests, response_bytes = self.counters
        return DeveloperSourceResult(
            POLYGON_DEVELOPER_PROVIDER,
            tuple(batches),
            requests,
            response_bytes,
        )


class PolygonDeveloperStrategyBarSource(_BoundedPolygonDeveloperSource):
    fact_kind = DeveloperAcquisitionFactKind.EQUITY_BAR

    def __init__(
        self,
        transport: PolygonHttpTransport,
        identities: tuple[DeveloperEquityIdentity, ...],
        *,
        base_url: str = REST_BASE_URL,
        limits: PolygonDeveloperSourceLimits = PolygonDeveloperSourceLimits(
            maximum_rows=8_000,
            maximum_equity_rows_per_ticker=1_000,
        ),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        lookback_days: int = 90,
        retained_daily_bars: int = 50,
        retained_hourly_bars: int = 40,
        retained_thirty_minute_bars: int = 202,
    ) -> None:
        if {identity.ticker for identity in identities} != set(
            CANARY_UNIVERSE.equity_tickers
        ):
            raise ValueError("Polygon strategy bars require the exact canary")
        retained = (
            retained_daily_bars,
            retained_hourly_bars,
            retained_thirty_minute_bars,
        )
        if (
            type(lookback_days) is not int
            or not 30 <= lookback_days <= 365
            or any(type(value) is not int or value <= 0 for value in retained)
            or sum(retained) * len(identities) > MAX_DEVELOPER_SOURCE_FACTS
        ):
            raise ValueError("Polygon strategy lookback bounds are invalid")
        super().__init__(
            transport,
            base_url=base_url,
            limits=limits,
            clock=clock,
        )
        self.identities = tuple(sorted(identities, key=lambda row: row.ticker))
        self.lookback_days = lookback_days
        self.retained_daily_bars = retained_daily_bars
        self.retained_hourly_bars = retained_hourly_bars
        self.retained_thirty_minute_bars = retained_thirty_minute_bars

    @staticmethod
    def _security(identity: DeveloperEquityIdentity, effective_at: datetime):
        return SecurityReferenceRevision(
            security_revision_id=uuid5(
                NAMESPACE_URL,
                f"developer-strategy-reference:{identity.security_id}:{identity.ticker}",
            ),
            security_id=identity.security_id,
            ticker=identity.ticker,
            active=True,
            company_name=identity.ticker,
            security_type="ETF" if identity.ticker in {"SPY", "QQQ", "IWM"} else "STOCK",
            cik=None,
            composite_figi=None,
            share_class_figi=None,
            primary_exchange=None,
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
            source="MASSIVE_STRATEGY_LOOKBACK_V1",
            effective_from=effective_at - timedelta(days=1),
            observed_at=effective_at,
            payload_sha256=canonical_sha256({
                "security_id": identity.security_id,
                "ticker": identity.ticker,
                "source": "MASSIVE_STRATEGY_LOOKBACK_V1",
            }),
            raw_payload_json="{}",
        )

    def collect(self, request: DeveloperAcquisitionRequest) -> DeveloperSourceResult:
        if request.universe != CANARY_UNIVERSE:
            raise ValueError("Polygon strategy request changed the reviewed universe")
        self._begin()
        batches = []
        start = (request.requested_at - timedelta(days=self.lookback_days)).date()
        end = request.requested_at.date()
        for identity in self.identities:
            started_at = _utc(self.clock(), "clock")
            results, response_sha256, completed_at = self._request(
                f"/v2/aggs/ticker/{quote(identity.ticker, safe='')}/range/30/minute/"
                f"{start.isoformat()}/{end.isoformat()}",
                {
                    "adjusted": "false",
                    "sort": "asc",
                    "limit": "50000",
                },
            )
            batch_id = uuid5(
                NAMESPACE_URL,
                f"polygon-developer-strategy-bars:{identity.ticker}:"
                f"{request.requested_at.isoformat()}:{response_sha256}",
            )
            security = self._security(identity, started_at)
            thirty = normalize_native_bars(
                identity.security_id,
                identity.ticker,
                "30m",
                results,
                observed_at=completed_at,
                adjusted=False,
                availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
                ingestion_segment_id=batch_id,
            )
            hourly = derive_canonical_bars(
                security,
                thirty,
                target_interval="1h",
                observed_at=completed_at,
                ingestion_segment_id=batch_id,
            )
            daily = derive_canonical_bars(
                security,
                thirty,
                target_interval="1d",
                observed_at=completed_at,
                ingestion_segment_id=batch_id,
            )
            if (
                len(thirty) < self.retained_thirty_minute_bars
                or len(hourly) < self.retained_hourly_bars
                or len(daily) < self.retained_daily_bars
            ):
                raise ValueError(
                    f"Polygon strategy lookback coverage is incomplete for {identity.ticker}"
                )
            retained = (
                *daily[-self.retained_daily_bars:],
                *hourly[-self.retained_hourly_bars:],
                *thirty[-self.retained_thirty_minute_bars:],
            )
            facts = tuple(
                DeveloperEquityBarAcquisitionFact(identity.instrument_id, revision)
                for revision in retained
            )
            batches.append(CompletedDeveloperSourceBatch(
                self.fact_kind,
                batch_id,
                started_at,
                completed_at,
                facts,
            ))
        requests, response_bytes = self.counters
        return DeveloperSourceResult(
            POLYGON_DEVELOPER_PROVIDER,
            tuple(batches),
            requests,
            response_bytes,
        )


class PolygonDeveloperOptionSource(_BoundedPolygonDeveloperSource):
    fact_kind = DeveloperAcquisitionFactKind.OPTION_SNAPSHOT

    def __init__(
        self,
        transport: PolygonHttpTransport,
        identities: tuple[DeveloperOptionIdentity, ...],
        normalizer: DeveloperOptionNormalizer,
        *,
        risk_free_rate: float,
        dividend_yields: Mapping[str, float],
        base_url: str = REST_BASE_URL,
        limits: PolygonDeveloperSourceLimits = PolygonDeveloperSourceLimits(),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        bar_window: Callable[[datetime], tuple[int, int]] | None = None,
    ) -> None:
        underlyings = {identity.catalog.underlyer for identity in identities}
        if underlyings != set(CANARY_UNIVERSE.option_underlyings):
            raise ValueError("Polygon option identities must cover the exact canary")
        if len({identity.instrument_id for identity in identities}) != len(identities):
            raise ValueError("Polygon option instrument IDs must be unique")
        if len({identity.catalog.contract_id for identity in identities}) != len(identities):
            raise ValueError("Polygon option contract IDs must be unique")
        if set(dividend_yields) != set(CANARY_UNIVERSE.option_underlyings):
            raise ValueError("Polygon option dividend yields must cover the exact canary")
        super().__init__(
            transport,
            base_url=base_url,
            limits=limits,
            clock=clock,
            bar_window=bar_window,
        )
        self.identities = tuple(
            sorted(identities, key=lambda row: row.catalog.contract_ticker)
        )
        self.normalizer = normalizer
        self.risk_free_rate = float(risk_free_rate)
        self.dividend_yields = {
            ticker: float(value) for ticker, value in dividend_yields.items()
        }

    def _identities(self, underlyer: str) -> tuple[DeveloperOptionIdentity, ...]:
        return tuple(
            identity
            for identity in self.identities
            if identity.catalog.underlyer == underlyer
        )

    def collect(self, request: DeveloperAcquisitionRequest) -> DeveloperSourceResult:
        if request.universe != CANARY_UNIVERSE:
            raise ValueError("Polygon option request changed the reviewed universe")
        self._begin()
        batches = []
        for underlyer in request.universe.option_underlyings:
            started_at = _utc(self.clock(), "clock")
            identities = self._identities(underlyer)
            expiration_dates = tuple(
                identity.catalog.expiration_date for identity in identities
            )
            snapshot_results, snapshot_response_sha256, snapshot_observed_at = self._request(
                f"/v3/snapshot/options/{quote(underlyer, safe='')}",
                {
                    "expiration_date.gte": min(expiration_dates).isoformat(),
                    "expiration_date.lte": max(expiration_dates).isoformat(),
                    "limit": "250",
                    "sort": "ticker",
                    "order": "asc",
                },
            )
            by_ticker = {
                str(row.get("details", {}).get("ticker") or ""): row
                for row in snapshot_results
                if isinstance(row.get("details"), dict)
            }
            missing = {
                identity.catalog.contract_ticker for identity in identities
            } - set(by_ticker)
            if missing:
                raise ValueError("Polygon option response omitted an admitted contract")
            raw_by_ticker = {
                identity.catalog.contract_ticker: parse_polygon_snapshot(
                    by_ticker[identity.catalog.contract_ticker],
                    snapshot_observed_at,
                )
                for identity in identities
            }
            mark_times = tuple(
                raw.option_mark_time for raw in raw_by_ticker.values()
                if raw.option_mark_time is not None
            )
            if len(mark_times) != len(raw_by_ticker):
                raise ValueError("Polygon admitted option is missing its mark timestamp")
            bar_results, bar_response_sha256, bar_observed_at = self._request(
                self._minute_aggregate_path(
                    underlyer,
                    min(mark_times),
                    lookback_days=4,
                ),
                {
                    "adjusted": "false",
                    "sort": "asc",
                    "limit": str(self.limits.maximum_equity_rows_per_ticker),
                },
            )
            underlying_bars = []
            for row in bar_results:
                try:
                    bar_start = datetime.fromtimestamp(
                        int(row["t"]) / 1000,
                        tz=timezone.utc,
                    )
                    market_time = bar_start + timedelta(minutes=1)
                    if market_time > request.requested_at:
                        continue
                    raw_payload_text = canonical_json(row)
                    underlying_bars.append(UnderlyingMinuteBar(
                        close=Decimal(str(row["c"])),
                        market_data_time=market_time,
                        bar_start=bar_start,
                        raw_payload_text=raw_payload_text,
                    ))
                except (KeyError, TypeError, ValueError, ArithmeticError) as error:
                    raise ValueError("Polygon underlying aggregate row is invalid") from error
            if not underlying_bars:
                raise ValueError("Polygon option source returned no underlying bars")
            batch_id = uuid5(
                NAMESPACE_URL,
                f"polygon-developer-option:{underlyer}:"
                f"{request.requested_at.isoformat()}:{bar_response_sha256}:"
                f"{snapshot_response_sha256}",
            )
            normalized_inputs = tuple(
                DeveloperNormalizationInput(
                    raw=raw_by_ticker[identity.catalog.contract_ticker],
                    catalog=identity.catalog,
                    underlying_bars=tuple(underlying_bars),
                    expiration_cutoff=identity.expiration_cutoff,
                    risk_free_rate=self.risk_free_rate,
                    dividend_yield=self.dividend_yields[underlyer],
                    normalized_observed_at=max(
                        bar_observed_at,
                        snapshot_observed_at,
                    ),
                )
                for identity in identities
            )
            normalized = self.normalizer.normalize(batch_id, normalized_inputs)
            snapshots_by_contract = {
                snapshot.contract_id: snapshot for snapshot in normalized.snapshots
            }
            expected_contracts = {
                identity.catalog.contract_id for identity in identities
            }
            if set(snapshots_by_contract) != expected_contracts:
                raise ValueError(
                    "Polygon option normalization omitted an admitted contract: "
                    f"{dict(normalized.rejected_counts)}"
                )
            instrument_by_contract = {
                identity.catalog.contract_id: identity.instrument_id
                for identity in identities
            }
            facts = tuple(
                DeveloperOptionSnapshotAcquisitionFact(
                    instrument_by_contract[snapshot.contract_id],
                    snapshot,
                )
                for snapshot in normalized.snapshots
            )
            completed_at = max(bar_observed_at, snapshot_observed_at)
            batches.append(CompletedDeveloperSourceBatch(
                self.fact_kind,
                batch_id,
                started_at,
                completed_at,
                facts,
            ))
        requests, response_bytes = self.counters
        return DeveloperSourceResult(
            POLYGON_DEVELOPER_PROVIDER,
            tuple(batches),
            requests,
            response_bytes,
        )
