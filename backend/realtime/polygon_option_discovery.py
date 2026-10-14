from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from datetime import timedelta
import json
from urllib.parse import parse_qs, quote, urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import OptionRuntimeConfiguration
from provider_endpoints import REST_BASE_URL, REST_HOSTS
from options.data.polygon_chain_discovery import (
    PolygonOptionChainDiscoveryAdapter,
    PolygonOptionChainDiscoveryLimits,
    PolygonOptionChainDiscoveryResult,
    build_polygon_option_chain_discovery_plan,
)
from options.data.polygon_http import PolygonHttpTransport

from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .preflight import ProviderTier


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class PolygonOptionDiscoverySource:
    def __init__(
        self,
        transport: PolygonHttpTransport,
        configuration: OptionRuntimeConfiguration,
        scope: RealtimeOptionDiscoveryScope,
        *,
        base_url: str = REST_BASE_URL,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        parsed = urlsplit(base_url.rstrip("/"))
        if parsed.scheme != "https" or parsed.hostname not in REST_HOSTS:
            raise ValueError("Polygon option discovery requires the approved HTTPS host")
        capacity = configuration.policy.capacity
        self.configuration = configuration
        self.scope = scope
        self.clock = clock
        self.adapter = PolygonOptionChainDiscoveryAdapter(
            transport.get,
            base_url=base_url,
            limits=PolygonOptionChainDiscoveryLimits(
                maximum_pages=capacity.maximum_pages_per_batch,
                maximum_rows=capacity.maximum_contracts_per_batch,
                maximum_page_bytes=capacity.maximum_page_bytes,
                maximum_batch_bytes=capacity.maximum_batch_bytes,
            ),
            clock=clock,
        )

    def discover(
        self,
        underlyer: str,
        as_of: datetime,
        spot_price: Decimal,
    ) -> PolygonOptionChainDiscoveryResult:
        underlyer = underlyer.strip().upper()
        if underlyer not in self.scope.universe.option_underlyings:
            raise ValueError("Polygon option discovery changed the reviewed universe")
        contract_filter = self.configuration.policy.contract_filter
        plan = build_polygon_option_chain_discovery_plan(
            underlyer,
            as_of,
            spot_price,
            maximum_dte=contract_filter.maximum_dte,
            acquisition_maximum_dte=(
                self.configuration.settings.acquisition_maximum_dte
            ),
            strike_corridor_fraction=contract_filter.strike_corridor_fraction,
        )
        batch_id = uuid5(
            NAMESPACE_URL,
            f"polygon-option-discovery:{self.scope.sha256}:{underlyer}:"
            f"{plan.scheduled_cycle.isoformat()}:{plan.request_filter_sha256}",
        )
        return self.adapter.discover(
            plan,
            batch_id=batch_id,
            policy_sha256=self.configuration.policy_sha256,
            started_at=_utc(self.clock(), "clock"),
        )


class PolygonDeveloperOptionDiscoverySource(PolygonOptionDiscoverySource):
    def __init__(
        self,
        transport: PolygonHttpTransport,
        configuration: OptionRuntimeConfiguration,
        scope: RealtimeOptionDiscoveryScope,
        **kwargs,
    ) -> None:
        if scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
            raise ValueError("Developer discovery requires the Developer tier")
        super().__init__(transport, configuration, scope, **kwargs)


class PolygonAdvancedOptionDiscoverySource(PolygonOptionDiscoverySource):
    def __init__(
        self,
        transport: PolygonHttpTransport,
        configuration: OptionRuntimeConfiguration,
        scope: RealtimeOptionDiscoveryScope,
        **kwargs,
    ) -> None:
        if scope.provider_tier is not ProviderTier.MASSIVE_ADVANCED:
            raise ValueError("Advanced discovery requires the Advanced tier")
        super().__init__(transport, configuration, scope, **kwargs)


class PolygonOptionDiscoveryBinding:
    def __init__(
        self,
        source: PolygonOptionDiscoverySource,
        persist: Callable[
            [RealtimeOptionDiscoveryScope, UUID, PolygonOptionChainDiscoveryResult],
            object,
        ],
    ) -> None:
        self.source = source
        self.persist = persist

    def run(
        self,
        underlying_security_id: UUID,
        underlyer: str,
        as_of: datetime,
        spot_price: Decimal,
    ) -> PolygonOptionChainDiscoveryResult:
        result = self.source.discover(underlyer, as_of, spot_price)
        self.persist(self.source.scope, underlying_security_id, result)
        return result


class PolygonDeveloperOptionDiscoveryBinding(PolygonOptionDiscoveryBinding):
    pass


@dataclass(frozen=True, slots=True)
class DeveloperTradeCollectionPolicy:
    enabled: bool = False
    maximum_contracts_per_underlying: int = 2
    maximum_contracts_total: int = 18
    maximum_pages_per_contract: int = 2
    maximum_rows_per_page: int = 1000
    maximum_page_bytes: int = 1_048_576
    window_minutes: int = 15

    def __post_init__(self):
        if (type(self.enabled) is not bool or not 1 <= self.maximum_contracts_per_underlying <= 2
                or not 1 <= self.maximum_contracts_total <= 18 or not 1 <= self.maximum_pages_per_contract <= 2
                or not 1 <= self.maximum_rows_per_page <= 1000 or not 1 <= self.maximum_page_bytes <= 1_048_576
                or not 1 <= self.window_minutes <= 15):
            raise ValueError("Developer trade collection exceeds its reviewed bounds")

    @property
    def sha256(self):
        from .contracts import canonical_sha256
        return canonical_sha256(self)


class PolygonDeveloperTradeWindowSource:
    def __init__(self, transport, policy: DeveloperTradeCollectionPolicy, *, clock):
        self.transport, self.policy, self.clock = transport, policy, clock
        self.contract_count = 0

    def collect(self, scope, matrix):
        from options.data.polygon_developer import PolygonDeveloperEngine, validate_polygon_next_page
        from options.orchestration import select_option_trade_watchlist
        from .contracts import canonical_sha256
        from .option_package_context import ForwardTradePage, ForwardTradeWindowWithPages, retained_trade_context

        if not self.policy.enabled:
            return None
        if matrix.ingress.scope != scope or scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
            raise ValueError("Developer trade collection matrix/tier scope drift")
        catalog = {row.contract_ticker: row for row in scope.catalog if row.underlyer == matrix.analysis.underlying.underlyer}
        selected = select_option_trade_watchlist(matrix.snapshots, catalog, limit=self.policy.maximum_contracts_per_underlying)
        if not selected:
            return None
        if self.contract_count + len(selected) > self.policy.maximum_contracts_total:
            raise ValueError("Developer trade contract budget exhausted")
        self.contract_count += len(selected)
        start = scope.cutoff - timedelta(minutes=self.policy.window_minutes)
        events, pages, complete = [], [], True
        for contract in selected:
            path = f"/v3/trades/{quote(contract.contract_ticker, safe=':')}"
            filters = {"timestamp.gte": str(int(start.timestamp()) * 1_000_000_000),
                "timestamp.lte": str(int(scope.cutoff.timestamp()) * 1_000_000_000),
                "limit": str(self.policy.maximum_rows_per_page), "sort": "timestamp", "order": "asc"}
            url, params, seen = f"{REST_BASE_URL}{path}", filters, set()
            for page_index in range(self.policy.maximum_pages_per_contract):
                request_id = canonical_sha256((url, params))
                if request_id in seen:
                    raise ValueError("Developer trade pagination repeated a request")
                seen.add(request_id)
                if not scope.available_after <= self.clock() < scope.deadline:
                    raise ValueError("Developer trade request is outside the original release/deadline window")
                response = self.transport.get(url, params)
                if response.status_code != 200 or len(response.body) > self.policy.maximum_page_bytes:
                    raise ValueError("Developer trade response status or page byte limit failed")
                received = self.clock()
                if not scope.available_after <= received <= scope.deadline:
                    raise ValueError("Developer trade collection exceeded original deadline")
                payload = json.loads(response.body)
                rows = payload.get("results")
                if not isinstance(rows, list) or len(rows) > self.policy.maximum_rows_per_page or not all(isinstance(row, dict) for row in rows):
                    raise ValueError("Developer trade results exceed the exact row/schema bounds")
                next_url = payload.get("next_url")
                if next_url and "apiKey" in parse_qs(urlsplit(next_url).query):
                    raise ValueError("credential-bearing trade pagination cannot be retained")
                next_page = validate_polygon_next_page(next_url, path, filters, base_url=REST_BASE_URL) if next_url else None
                pages.append(ForwardTradePage(contract.contract_id, path, request_id, response.body, received,
                    canonical_sha256(next_page[2]) if next_page else None))
                batch = uuid5(NAMESPACE_URL, f"retained-trades:{scope.sha256}:{contract.contract_id}:{request_id}")
                events.extend(PolygonDeveloperEngine._trade_event(contract, batch, row, received, provider=contract.provider) for row in rows)
                if not next_page:
                    break
                if page_index + 1 == self.policy.maximum_pages_per_contract:
                    complete = False
                url, params = next_page[:2]
        recorded = self.clock()
        receipt = ForwardTradeWindowWithPages(scope.sha256, matrix.matrix_id, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER,
            start, scope.cutoff, max(page.received_at for page in pages), recorded,
            tuple(row.contract_id for row in selected), tuple(events), "COMPLETE" if complete else "PARTIAL",
            tuple(pages), self.policy.sha256)
        retained_trade_context(scope, matrix, receipt, selected_at=recorded)
        return receipt