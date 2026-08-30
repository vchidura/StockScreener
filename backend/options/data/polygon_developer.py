from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Collection, Mapping
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from queue import Queue
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo

import requests

from options.config import OptionRuntimeConfiguration
from options.domain import (
    AssetType,
    BatchStatus,
    OptionContractCatalogEntry,
    DataCapability,
    OptionContractReference,
    OptionTradeCursor,
    OptionTradeEvent,
    OptionTradeFetchResult,
    PageValidationStatus,
    RawBatchPage,
    RawOptionBatch,
    SpotPrice,
    WorkStage,
)
from options.errors import OptionProviderError, ProviderErrorCategory
from options.repositories.ingestion import OptionIngestionRepository
from options.repositories.trades import OptionTradeRepository
from options.analytics.marks import UnderlyingMinuteBar

from .base import BaseDataEngine
from .base import validate_developer_capabilities
from .polygon_http import (
    PolygonHttpResponse,
    PolygonHttpTransport,
    PolygonRateLimitGate,
    RequestsPolygonHttpTransport,
)
from .trades_base import OptionsTradeSource


ET = ZoneInfo("America/New_York")


class PolygonDeveloperEngine(BaseDataEngine, OptionsTradeSource):
    _CAPABILITIES = frozenset(
        {
            DataCapability.CHAIN_SNAPSHOT,
            DataCapability.OPTION_TRADES,
            DataCapability.UNDERLYING_PRICE,
        }
    )

    def __init__(
        self,
        configuration: OptionRuntimeConfiguration,
        ingestion_repository: OptionIngestionRepository,
        trade_repository: OptionTradeRepository | None = None,
        *,
        transport: PolygonHttpTransport | None = None,
        rate_limit_gate: PolygonRateLimitGate | None = None,
        base_url: str = "https://api.polygon.io",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.configuration = configuration
        self.ingestion_repository = ingestion_repository
        self.trade_repository = trade_repository
        self.base_url = base_url.rstrip("/")
        self._base = urlsplit(self.base_url)
        if self._base.scheme != "https" or not self._base.hostname:
            raise ValueError("Polygon base URL must be HTTPS with a hostname")
        api_key = configuration.settings.polygon_api_key.get_secret_value()
        self.transport = transport or RequestsPolygonHttpTransport(
            api_key,
            timeout_seconds=configuration.policy.provider_requests.request_timeout_seconds,
        )
        self.rate_limit_gate = rate_limit_gate or PolygonRateLimitGate()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def capabilities(self) -> frozenset[DataCapability]:
        return self._CAPABILITIES

    def probe_developer_capabilities(
        self,
        underlyer: str,
        contract_ticker: str,
        as_of: datetime,
    ) -> frozenset[DataCapability]:
        as_of = _as_utc(as_of, "as_of")
        observed: set[DataCapability] = set()
        chain_path = f"/v3/snapshot/options/{quote(underlyer, safe='')}"
        chain, _ = self._request_json(
            f"{self.base_url}{chain_path}",
            {"limit": "1"},
        )
        self._validate_provider_status(chain, {"OK", "DELAYED"}, "option chain")
        if isinstance(chain.get("results"), list):
            observed.add(DataCapability.CHAIN_SNAPSHOT)
        trade_path = f"/v3/trades/{quote(contract_ticker, safe=':')}"
        trades, _ = self._request_json(
            f"{self.base_url}{trade_path}",
            {"limit": "1", "sort": "timestamp", "order": "desc"},
        )
        self._validate_provider_status(trades, {"OK", "DELAYED"}, "option trades")
        if isinstance(trades.get("results"), list):
            observed.add(DataCapability.OPTION_TRADES)
        quote_path = f"/v3/quotes/{quote(contract_ticker, safe=':')}"
        quote_response = self._request_response(
            f"{self.base_url}{quote_path}",
            {"limit": "1"},
            allowed_status_codes=frozenset({403}),
        )
        if quote_response.status_code == 200:
            observed.add(DataCapability.OPTION_QUOTES)
        elif quote_response.status_code != 403:
            self._raise_for_response(quote_response)
        self.get_spot_price(underlyer, as_of)
        observed.add(DataCapability.UNDERLYING_PRICE)
        validate_developer_capabilities(observed)
        return frozenset(observed)

    def get_spot_price(self, underlyer: str, as_of: datetime) -> SpotPrice:
        as_of = _as_utc(as_of, "as_of")
        start_date = (as_of - timedelta(days=4)).date().isoformat()
        end_date = as_of.date().isoformat()
        path = (
            f"/v2/aggs/ticker/{quote(underlyer, safe='')}/range/1/minute/"
            f"{start_date}/{end_date}"
        )
        payload, _ = self._request_json(
            f"{self.base_url}{path}",
            {"adjusted": "true", "sort": "asc", "limit": "50000"},
        )
        self._validate_provider_status(payload, {"OK", "DELAYED"}, "underlying aggregates")
        results = payload.get("results")
        if not isinstance(results, list):
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon aggregate response is missing a results array",
            )
        eligible = [
            row
            for row in results
            if isinstance(row, dict)
            and isinstance(row.get("t"), int)
            and _from_milliseconds(row["t"]) <= as_of
        ]
        if not eligible:
            raise OptionProviderError(
                ProviderErrorCategory.MISSING_SPOT,
                "Polygon returned no positive underlying mark at or before the requested time",
            )
        row = max(eligible, key=lambda value: value["t"])
        try:
            price = Decimal(str(row["c"]))
        except (KeyError, ValueError, TypeError) as exc:
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon aggregate close is missing or invalid",
            ) from exc
        observed_at = _as_utc(self.clock(), "clock")
        return SpotPrice(
            underlyer=underlyer,
            provider=self.configuration.settings.underlying_data_provider,
            price=price,
            market_data_time=_from_milliseconds(row["t"]),
            first_observed_at=observed_at,
        )

    def get_underlying_minute_bars(
        self,
        underlyer: str,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[UnderlyingMinuteBar, ...]:
        start_time = _as_utc(start_time, "start_time")
        end_time = _as_utc(end_time, "end_time")
        if end_time < start_time:
            raise ValueError("end_time cannot precede start_time")
        path = (
            f"/v2/aggs/ticker/{quote(underlyer, safe='')}/range/1/minute/"
            f"{start_time.date().isoformat()}/{end_time.date().isoformat()}"
        )
        payload, _ = self._request_json(
            f"{self.base_url}{path}",
            {"adjusted": "true", "sort": "asc", "limit": "50000"},
        )
        self._validate_provider_status(payload, {"OK", "DELAYED"}, "underlying aggregates")
        results = payload.get("results")
        if not isinstance(results, list):
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon aggregate response is missing a results array",
            )
        bars: dict[datetime, UnderlyingMinuteBar] = {}
        for row in results:
            if not isinstance(row, dict) or not isinstance(row.get("t"), int):
                continue
            market_time = _from_milliseconds(row["t"])
            if not start_time <= market_time <= end_time:
                continue
            try:
                bar = UnderlyingMinuteBar(
                    close=Decimal(str(row["c"])),
                    market_data_time=market_time,
                )
            except (KeyError, TypeError, ValueError):
                continue
            bars[market_time] = bar
        return tuple(bars[key] for key in sorted(bars))

    def get_option_reference(
        self,
        contract_ticker: str,
        as_of: date,
        asset_type: AssetType,
    ) -> OptionContractReference:
        path = f"/v3/reference/options/contracts/{quote(contract_ticker, safe=':')}"
        payload, _ = self._request_json(
            f"{self.base_url}{path}",
            {"as_of": as_of.isoformat()},
        )
        self._validate_provider_status(payload, {"OK"}, "option reference")
        result = payload.get("results")
        if not isinstance(result, dict):
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon option reference response is missing a results object",
            )
        observed_at = _as_utc(self.clock(), "clock")
        return self._reference_from_result(
            result,
            contract_ticker,
            asset_type,
            observed_at,
            as_of,
        )

    def list_option_references(
        self,
        underlyer: str,
        as_of: date,
        expiration_through: date,
        asset_type: AssetType,
        strike_min: Decimal | None = None,
        strike_max: Decimal | None = None,
    ) -> tuple[OptionContractReference, ...]:
        filters = {
            "underlying_ticker": underlyer,
            "expiration_date.gte": as_of.isoformat(),
            "expiration_date.lte": expiration_through.isoformat(),
            "expired": "false",
            "limit": "1000",
            "sort": "ticker",
            "order": "asc",
        }
        if strike_min is not None:
            filters["strike_price.gte"] = str(strike_min)
        if strike_max is not None:
            filters["strike_price.lte"] = str(strike_max)
        path = "/v3/reference/options/contracts"
        url = f"{self.base_url}{path}"
        params = filters
        seen_requests: set[str] = set()
        references: list[OptionContractReference] = []
        page_count = 0
        byte_count = 0
        while True:
            request_identity = _sha256_json({"url": url, "params": sorted(params.items())})
            if request_identity in seen_requests:
                raise OptionProviderError(
                    ProviderErrorCategory.PAGINATION,
                    "Polygon option-reference pagination repeated a request",
                )
            seen_requests.add(request_identity)
            payload, response = self._request_json(url, params)
            self._validate_provider_status(payload, {"OK"}, "option references")
            results = payload.get("results")
            if not isinstance(results, list) or not all(isinstance(row, dict) for row in results):
                raise OptionProviderError(
                    ProviderErrorCategory.SCHEMA,
                    "Polygon option-reference response has an invalid results array",
                )
            observed_at = _as_utc(self.clock(), "clock")
            references.extend(
                self._reference_from_result(
                    row,
                    str(row.get("ticker") or ""),
                    asset_type,
                    observed_at,
                    as_of,
                )
                for row in results
            )
            page_count += 1
            byte_count += len(response.body)
            if page_count > self.configuration.policy.capacity.maximum_pages_per_batch:
                raise OptionProviderError(
                    ProviderErrorCategory.RESPONSE_LIMIT,
                    "Polygon option references exceeded the page cap",
                )
            if len(references) > self.configuration.policy.capacity.maximum_contracts_per_batch:
                raise OptionProviderError(
                    ProviderErrorCategory.RESPONSE_LIMIT,
                    "Polygon option references exceeded the contract cap",
                )
            if byte_count > self.configuration.policy.capacity.maximum_batch_bytes:
                raise OptionProviderError(
                    ProviderErrorCategory.RESPONSE_LIMIT,
                    "Polygon option references exceeded the batch byte cap",
                )
            next_url = payload.get("next_url")
            if not next_url:
                return tuple(references)
            url, params, _ = self._validate_next_page(str(next_url), path, filters)

    @staticmethod
    def _reference_from_result(
        result: dict[str, Any],
        requested_ticker: str,
        asset_type: AssetType,
        observed_at: datetime,
        as_of: date,
    ) -> OptionContractReference:
        try:
            expiration_date = date.fromisoformat(str(result["expiration_date"]))
            strike = Decimal(str(result["strike_price"]))
            shares_per_contract = int(result["shares_per_contract"])
            underlyer = str(result["underlying_ticker"])
            provider_contract_type = str(result["contract_type"])
        except (KeyError, ValueError, TypeError) as exc:
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon option reference identity is missing or invalid",
            ) from exc
        list_date = result.get("list_date")
        valid_from = (
            datetime.combine(date.fromisoformat(str(list_date)), time.min, tzinfo=timezone.utc)
            if list_date
            else datetime.combine(as_of, time.min, tzinfo=timezone.utc)
        )
        additional = result.get("additional_underlyings") or []
        adjustment_metadata = {
            key: result[key]
            for key in ("cfi", "correction")
            if result.get(key) is not None
        }
        return OptionContractReference(
            contract_ticker=str(result.get("ticker") or requested_ticker),
            underlyer=underlyer,
            asset_type=asset_type,
            provider="polygon",
            provider_version=(
                str(result["correction"]) if result.get("correction") is not None else None
            ),
            provider_contract_type=provider_contract_type,
            expiration_date=expiration_date,
            strike=strike,
            provider_exercise_style=str(result.get("exercise_style") or "UNKNOWN"),
            shares_per_contract=shares_per_contract,
            primary_exchange=(
                str(result["primary_exchange"])
                if result.get("primary_exchange") is not None
                else None
            ),
            correction=(
                str(result["correction"]) if result.get("correction") is not None else None
            ),
            additional_underlyings_json=_canonical_json_text(additional),
            adjustment_metadata_json=_canonical_json_text(adjustment_metadata),
            changes_deliverables=bool(additional),
            valid_from=valid_from,
            valid_to=None,
            first_observed_at=observed_at,
            revised_observed_at=None,
            refreshed_at=observed_at,
            payload_sha256=_sha256_json(result),
        )

    def get_option_chain(
        self,
        underlyer: str,
        as_of: datetime,
        expiration_through: date,
        strike_min: Decimal,
        strike_max: Decimal,
    ) -> RawOptionBatch:
        as_of = _as_utc(as_of, "as_of")
        if strike_min <= 0 or strike_max < strike_min:
            raise ValueError("option chain strike bounds are invalid")
        market_date = as_of.astimezone(ET).date()
        filters = {
            "expiration_date.gte": market_date.isoformat(),
            "expiration_date.lte": expiration_through.isoformat(),
            "strike_price.gte": str(strike_min),
            "strike_price.lte": str(strike_max),
            "limit": "250",
            "sort": "ticker",
            "order": "asc",
        }
        request_filter_sha256 = _sha256_json(filters)
        started_at = _as_utc(self.clock(), "clock")
        initial_batch = RawOptionBatch(
            batch_id=uuid4(),
            provider="polygon",
            underlyer=underlyer,
            scheduled_cycle=as_of,
            request_filter_sha256=request_filter_sha256,
            policy_sha256=self.configuration.policy_sha256,
            status=BatchStatus.FETCHING,
            pages=(),
            started_at=started_at,
        )
        batch_id = self.ingestion_repository.begin_batch(
            initial_batch,
            self._asset_type(underlyer),
            self.configuration.policy.policy_version,
            self.configuration.configuration_sha256,
        )
        if batch_id != initial_batch.batch_id:
            existing_batch = self.ingestion_repository.load_batch(batch_id)
            if existing_batch is not None and existing_batch.complete:
                return existing_batch
        path = f"/v3/snapshot/options/{quote(underlyer, safe='')}"
        url = f"{self.base_url}{path}"
        params = filters
        request_cursor_sha256 = None
        pages: list[RawBatchPage] = []
        seen_requests: set[str] = set()
        try:
            while True:
                request_identity = _sha256_json(
                    {"url": url, "params": sorted(params.items())}
                )
                if request_identity in seen_requests:
                    raise OptionProviderError(
                        ProviderErrorCategory.PAGINATION,
                        "Polygon option-chain pagination repeated a request",
                    )
                seen_requests.add(request_identity)
                response = self._request_response(url, params)
                try:
                    payload = self._decode_json(response)
                except OptionProviderError:
                    pages.append(
                        self._persist_invalid_page(
                            batch_id,
                            len(pages) + 1,
                            response,
                            request_filter_sha256,
                            request_cursor_sha256,
                            url,
                            params,
                        )
                    )
                    raise
                results = payload.get("results")
                if not isinstance(results, list) or not all(
                    isinstance(row, dict) for row in results
                ):
                    pages.append(
                        self._persist_invalid_page(
                            batch_id,
                            len(pages) + 1,
                            response,
                            request_filter_sha256,
                            request_cursor_sha256,
                            url,
                            params,
                        )
                    )
                    raise OptionProviderError(
                        ProviderErrorCategory.SCHEMA,
                        "Polygon option-chain response has an invalid results array",
                    )
                page_number = len(pages) + 1
                next_url = payload.get("next_url")
                terminal = not next_url
                next_cursor_sha256 = None
                next_request = None
                validation_status = PageValidationStatus.VALID
                try:
                    if next_url:
                        next_request = self._validate_next_page(
                            str(next_url), path, filters
                        )
                        next_cursor_sha256 = _sha256_text(next_request[2])
                except OptionProviderError:
                    validation_status = PageValidationStatus.INVALID
                page = RawBatchPage(
                    batch_id=batch_id,
                    page_number=page_number,
                    row_count=len(results),
                    response_bytes=response.body,
                    payload_sha256=_sha256_bytes(response.body),
                    received_at=_as_utc(self.clock(), "clock"),
                    terminal=terminal,
                    validation_status=validation_status,
                    request_filter_sha256=request_filter_sha256,
                    request_cursor_sha256=request_cursor_sha256,
                    request_id=(
                        str(payload["request_id"])
                        if payload.get("request_id") is not None
                        else None
                    ),
                    next_cursor_sha256=next_cursor_sha256,
                    request_metadata=tuple(
                        sorted(
                            [("path", urlsplit(url).path)]
                            + [(key, value) for key, value in params.items() if key != "apiKey"]
                        )
                    ),
                )
                self.ingestion_repository.persist_page(page)
                pages.append(page)
                if validation_status is PageValidationStatus.INVALID:
                    raise OptionProviderError(
                        ProviderErrorCategory.PAGINATION,
                        "Polygon option-chain next page failed validation",
                    )
                self._check_fetch_caps(pages)
                if terminal:
                    break
                url, params, cursor = next_request
                request_cursor_sha256 = _sha256_text(cursor)

            self.ingestion_repository.complete_batch(
                batch_id,
                uuid4(),
                f"normalize:{batch_id}",
                self.configuration.policy.capacity.maximum_work_attempts,
                self.configuration.policy.capacity.maximum_pages_per_batch,
                self.configuration.policy.capacity.maximum_contracts_per_batch,
                self.configuration.policy.capacity.maximum_batch_bytes,
            )
        except OptionProviderError as exc:
            failure_status = (
                BatchStatus.QUARANTINED
                if exc.category
                in {
                    ProviderErrorCategory.REQUEST,
                    ProviderErrorCategory.SCHEMA,
                    ProviderErrorCategory.PAGINATION,
                    ProviderErrorCategory.RESPONSE_LIMIT,
                }
                else BatchStatus.FAILED
            )
            self.ingestion_repository.fail_batch(
                batch_id, failure_status, exc.category.value, str(exc)
            )
            raise

        return RawOptionBatch(
            batch_id=batch_id,
            provider="polygon",
            underlyer=underlyer,
            scheduled_cycle=as_of,
            request_filter_sha256=request_filter_sha256,
            policy_sha256=self.configuration.policy_sha256,
            status=BatchStatus.COMPLETE,
            pages=tuple(pages),
            started_at=started_at,
            completed_at=_as_utc(self.clock(), "clock"),
        )

    def stream_market_data(
        self,
        underlyers: Collection[str],
        output: Queue[RawOptionBatch],
        stop_event: threading.Event,
    ) -> None:
        for underlyer in underlyers:
            if stop_event.is_set():
                return
            as_of = _as_utc(self.clock(), "clock")
            spot = self.get_spot_price(underlyer, as_of)
            corridor = self.configuration.policy.contract_filter.strike_corridor_fraction
            batch = self.get_option_chain(
                underlyer,
                as_of,
                as_of.astimezone(ET).date()
                + timedelta(days=self.configuration.policy.contract_filter.maximum_dte),
                spot.price * (Decimal("1") - corridor),
                spot.price * (Decimal("1") + corridor),
            )
            output.put(batch)

    def get_option_trades(
        self,
        contract: OptionContractCatalogEntry,
        start_time: datetime,
        end_time: datetime,
        cursor: OptionTradeCursor | None,
    ) -> OptionTradeFetchResult:
        if self.trade_repository is None:
            raise RuntimeError("option trade repository is required for trade ingestion")
        start_time = _as_utc(start_time, "start_time")
        end_time = _as_utc(end_time, "end_time")
        if end_time < start_time:
            raise ValueError("end_time cannot precede start_time")
        request_start = start_time
        if cursor is not None:
            overlap_start = cursor.sip_timestamp - timedelta(seconds=cursor.overlap_seconds)
            request_start = max(start_time, overlap_start)
        filters = {
            "timestamp.gte": str(_to_nanoseconds(request_start)),
            "timestamp.lte": str(_to_nanoseconds(end_time)),
            "limit": "50000",
            "sort": "timestamp",
            "order": "asc",
        }
        request_filter_sha256 = _sha256_json(filters)
        started_at = _as_utc(self.clock(), "clock")
        initial_batch = RawOptionBatch(
            batch_id=uuid4(),
            provider="polygon",
            underlyer=contract.underlyer,
            scheduled_cycle=end_time,
            request_filter_sha256=request_filter_sha256,
            policy_sha256=self.configuration.policy_sha256,
            status=BatchStatus.FETCHING,
            pages=(),
            started_at=started_at,
        )
        batch_id = self.ingestion_repository.begin_batch(
            initial_batch,
            contract.asset_type,
            self.configuration.policy.policy_version,
            self.configuration.configuration_sha256,
        )
        path = f"/v3/trades/{quote(contract.contract_ticker, safe=':')}"
        url = f"{self.base_url}{path}"
        params = filters
        request_cursor_sha256 = None
        pages: list[RawBatchPage] = []
        events: list[OptionTradeEvent] = []
        request_ids: list[str] = []
        seen_requests: set[str] = set()
        try:
            while True:
                request_identity = _sha256_json(
                    {"url": url, "params": sorted(params.items())}
                )
                if request_identity in seen_requests:
                    raise OptionProviderError(
                        ProviderErrorCategory.PAGINATION,
                        "Polygon option-trade pagination repeated a request",
                    )
                seen_requests.add(request_identity)
                response = self._request_response(url, params)
                try:
                    payload = self._decode_json(response)
                except OptionProviderError:
                    pages.append(
                        self._persist_invalid_page(
                            batch_id,
                            len(pages) + 1,
                            response,
                            request_filter_sha256,
                            request_cursor_sha256,
                            url,
                            params,
                        )
                    )
                    raise
                results = payload.get("results")
                if not isinstance(results, list) or not all(
                    isinstance(row, dict) for row in results
                ):
                    pages.append(
                        self._persist_invalid_page(
                            batch_id,
                            len(pages) + 1,
                            response,
                            request_filter_sha256,
                            request_cursor_sha256,
                            url,
                            params,
                        )
                    )
                    raise OptionProviderError(
                        ProviderErrorCategory.SCHEMA,
                        "Polygon option-trade response has an invalid results array",
                    )
                next_url = payload.get("next_url")
                terminal = not next_url
                next_cursor_sha256 = None
                next_request = None
                validation_status = PageValidationStatus.VALID
                try:
                    if next_url:
                        next_request = self._validate_next_page(
                            str(next_url), path, filters
                        )
                        next_cursor_sha256 = _sha256_text(next_request[2])
                except OptionProviderError:
                    validation_status = PageValidationStatus.INVALID
                received_at = _as_utc(self.clock(), "clock")
                page = RawBatchPage(
                    batch_id=batch_id,
                    page_number=len(pages) + 1,
                    row_count=len(results),
                    response_bytes=response.body,
                    payload_sha256=_sha256_bytes(response.body),
                    received_at=received_at,
                    terminal=terminal,
                    validation_status=validation_status,
                    request_filter_sha256=request_filter_sha256,
                    request_cursor_sha256=request_cursor_sha256,
                    request_id=(
                        str(payload["request_id"])
                        if payload.get("request_id") is not None
                        else None
                    ),
                    next_cursor_sha256=next_cursor_sha256,
                    request_metadata=tuple(
                        sorted(
                            [("path", urlsplit(url).path)]
                            + [(key, value) for key, value in params.items() if key != "apiKey"]
                        )
                    ),
                )
                self.ingestion_repository.persist_page(page)
                pages.append(page)
                if page.request_id:
                    request_ids.append(page.request_id)
                if validation_status is PageValidationStatus.INVALID:
                    raise OptionProviderError(
                        ProviderErrorCategory.PAGINATION,
                        "Polygon option-trade next page failed validation",
                    )
                page_events = tuple(
                    self._trade_event(contract, batch_id, row, received_at)
                    for row in results
                )
                self.trade_repository.persist(page_events)
                events.extend(page_events)
                self._check_trade_fetch_caps(pages)
                if terminal:
                    break
                url, params, next_cursor = next_request
                request_cursor_sha256 = _sha256_text(next_cursor)

            self.ingestion_repository.complete_batch(
                batch_id,
                uuid4(),
                f"classify-trades:{batch_id}",
                self.configuration.policy.capacity.maximum_work_attempts,
                self.configuration.policy.capacity.maximum_pages_per_batch,
                self.configuration.policy.capacity.maximum_trade_events_per_request,
                self.configuration.policy.capacity.maximum_batch_bytes,
                WorkStage.CLASSIFY_TRADES,
            )
            if events:
                watermark = max(
                    events,
                    key=lambda event: (event.sip_timestamp, event.sequence_number),
                )
                overlap_seconds = cursor.overlap_seconds if cursor else 30
                self.trade_repository.advance_cursor(
                    "polygon",
                    contract.contract_id,
                    watermark.sip_timestamp,
                    watermark.sequence_number,
                    overlap_seconds,
                    request_ids[-1] if request_ids else None,
                )
        except OptionProviderError as exc:
            failure_status = (
                BatchStatus.QUARANTINED
                if exc.category
                in {
                    ProviderErrorCategory.REQUEST,
                    ProviderErrorCategory.SCHEMA,
                    ProviderErrorCategory.PAGINATION,
                    ProviderErrorCategory.RESPONSE_LIMIT,
                }
                else BatchStatus.FAILED
            )
            self.ingestion_repository.fail_batch(
                batch_id, failure_status, exc.category.value, str(exc)
            )
            raise
        return OptionTradeFetchResult(
            raw_batch_id=batch_id,
            events=tuple(events),
            request_ids=tuple(request_ids),
            complete=True,
            terminal_page_received=True,
        )

    def stream_option_trades(
        self,
        contracts: Collection[OptionContractCatalogEntry],
        output: Queue[OptionTradeEvent],
        stop_event: threading.Event,
    ) -> None:
        if self.trade_repository is None:
            raise RuntimeError("option trade repository is required for trade ingestion")
        for contract in contracts:
            if stop_event.is_set():
                return
            end_time = _as_utc(self.clock(), "clock")
            cursor = self.trade_repository.get_cursor("polygon", contract.contract_id)
            start_time = (
                cursor.sip_timestamp - timedelta(seconds=cursor.overlap_seconds)
                if cursor
                else end_time - timedelta(seconds=self.configuration.settings.poll_seconds)
            )
            result = self.get_option_trades(contract, start_time, end_time, cursor)
            for event in result.events:
                if stop_event.is_set():
                    return
                output.put(event)

    def _request_json(
        self,
        url: str,
        params: Mapping[str, str],
    ) -> tuple[dict[str, Any], PolygonHttpResponse]:
        response = self._request_response(url, params)
        return self._decode_json(response), response

    def _request_response(
        self,
        url: str,
        params: Mapping[str, str],
        *,
        allowed_status_codes: frozenset[int] = frozenset(),
    ) -> PolygonHttpResponse:
        self._validate_url(url)
        rate_limit_retries = 0
        while True:
            self.rate_limit_gate.wait()
            try:
                response = self.transport.get(url, params)
            except requests.RequestException as exc:
                raise OptionProviderError(
                    ProviderErrorCategory.TRANSIENT,
                    "Polygon request failed after transport retries",
                ) from exc
            if response.status_code != 429:
                break
            retry_after = _retry_after_seconds(response.header("Retry-After"))
            retry_after = (
                retry_after
                if retry_after is not None
                else self.configuration.policy.provider_requests.default_retry_after_seconds
            )
            if (
                rate_limit_retries
                >= self.configuration.policy.provider_requests.maximum_rate_limit_retries
            ):
                raise OptionProviderError(
                    ProviderErrorCategory.RATE_LIMIT,
                    "Polygon rate limit retry budget was exhausted",
                    status_code=429,
                    retry_after_seconds=retry_after,
                )
            rate_limit_retries += 1
            self.rate_limit_gate.defer(retry_after)
        if response.status_code not in allowed_status_codes:
            self._raise_for_response(response)
        if len(response.body) > self.configuration.policy.capacity.maximum_page_bytes:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon response exceeded the per-page byte cap",
            )
        return response

    @staticmethod
    def _decode_json(response: PolygonHttpResponse) -> dict[str, Any]:
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon response was not valid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon response root must be an object",
            )
        return payload

    @staticmethod
    def _raise_for_response(response: PolygonHttpResponse) -> None:
        if response.status_code in (401, 403):
            raise OptionProviderError(
                ProviderErrorCategory.AUTHORIZATION,
                f"Polygon authorization failed with HTTP {response.status_code}",
                status_code=response.status_code,
            )
        if response.status_code >= 500:
            raise OptionProviderError(
                ProviderErrorCategory.TRANSIENT,
                f"Polygon request failed with HTTP {response.status_code}",
                status_code=response.status_code,
            )
        if response.status_code != 200:
            raise OptionProviderError(
                ProviderErrorCategory.REQUEST,
                f"Polygon request failed with HTTP {response.status_code}",
                status_code=response.status_code,
            )

    def _validate_next_page(
        self,
        next_url: str,
        expected_path: str,
        expected_filters: Mapping[str, str],
    ) -> tuple[str, dict[str, str], str]:
        parsed = self._validate_url(next_url)
        if parsed.path != expected_path:
            raise OptionProviderError(
                ProviderErrorCategory.PAGINATION,
                "Polygon pagination changed the endpoint path",
            )
        query = parse_qs(parsed.query, keep_blank_values=True)
        allowed_keys = set(expected_filters) | {"cursor", "apiKey"}
        if set(query) - allowed_keys or any(len(values) != 1 for values in query.values()):
            raise OptionProviderError(
                ProviderErrorCategory.PAGINATION,
                "Polygon pagination contained unexpected or repeated parameters",
            )
        cursor_values = query.get("cursor")
        if not cursor_values or len(cursor_values) != 1 or not cursor_values[0]:
            raise OptionProviderError(
                ProviderErrorCategory.PAGINATION,
                "Polygon pagination omitted a unique cursor",
            )
        for key, expected in expected_filters.items():
            if key in query and query[key] != [expected]:
                raise OptionProviderError(
                    ProviderErrorCategory.PAGINATION,
                    "Polygon pagination changed request filters",
                )
        params = {
            key: values[0]
            for key, values in query.items()
            if key != "apiKey" and len(values) == 1
        }
        return f"{self.base_url}{parsed.path}", params, cursor_values[0]

    def _validate_url(self, url: str):
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.hostname.lower() != self._base.hostname.lower()
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise OptionProviderError(
                ProviderErrorCategory.PAGINATION,
                "Polygon URL failed HTTPS host validation",
            )
        return parsed

    def _check_fetch_caps(self, pages: list[RawBatchPage]) -> None:
        capacity = self.configuration.policy.capacity
        if len(pages) > capacity.maximum_pages_per_batch:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon chain exceeded the page cap",
            )
        if sum(page.row_count for page in pages) > capacity.maximum_contracts_per_batch:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon chain exceeded the contract cap",
            )
        if sum(len(page.response_bytes) for page in pages) > capacity.maximum_batch_bytes:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon chain exceeded the batch byte cap",
            )

    @staticmethod
    def _validate_provider_status(
        payload: Mapping[str, Any],
        allowed: set[str],
        endpoint_name: str,
    ) -> None:
        status = payload.get("status")
        if status is not None and str(status).upper() not in allowed:
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                f"Polygon {endpoint_name} returned an unexpected provider status",
            )

    def _check_trade_fetch_caps(self, pages: list[RawBatchPage]) -> None:
        capacity = self.configuration.policy.capacity
        if len(pages) > capacity.maximum_pages_per_batch:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon trade request exceeded the page cap",
            )
        if sum(page.row_count for page in pages) > capacity.maximum_trade_events_per_request:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon trade request exceeded the event cap",
            )
        if sum(len(page.response_bytes) for page in pages) > capacity.maximum_batch_bytes:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon trade request exceeded the batch byte cap",
            )

    def _persist_invalid_page(
        self,
        batch_id,
        page_number: int,
        response: PolygonHttpResponse,
        request_filter_sha256: str,
        request_cursor_sha256: str | None,
        url: str,
        params: Mapping[str, str],
    ) -> RawBatchPage:
        page = RawBatchPage(
            batch_id=batch_id,
            page_number=page_number,
            row_count=0,
            response_bytes=response.body,
            payload_sha256=_sha256_bytes(response.body),
            received_at=_as_utc(self.clock(), "clock"),
            terminal=False,
            validation_status=PageValidationStatus.INVALID,
            request_filter_sha256=request_filter_sha256,
            request_cursor_sha256=request_cursor_sha256,
            request_metadata=tuple(
                sorted(
                    [("path", urlsplit(url).path)]
                    + [(key, value) for key, value in params.items() if key != "apiKey"]
                )
            ),
        )
        self.ingestion_repository.persist_page(page)
        return page

    @staticmethod
    def _trade_event(
        contract: OptionContractCatalogEntry,
        batch_id,
        row: dict[str, Any],
        observed_at: datetime,
    ) -> OptionTradeEvent:
        try:
            sip_timestamp = _from_nanoseconds(int(row["sip_timestamp"]))
            participant_timestamp = (
                _from_nanoseconds(int(row["participant_timestamp"]))
                if row.get("participant_timestamp") is not None
                else None
            )
            sequence_number = int(row["sequence_number"])
            price = Decimal(str(row["price"]))
            size = int(row["size"])
            conditions = tuple(int(value) for value in (row.get("conditions") or []))
            correction = int(row["correction"]) if row.get("correction") is not None else None
            exchange = int(row["exchange"]) if row.get("exchange") is not None else None
        except (KeyError, TypeError, ValueError) as exc:
            raise OptionProviderError(
                ProviderErrorCategory.SCHEMA,
                "Polygon option trade identity or economics are invalid",
            ) from exc
        payload_sha256 = _sha256_json(row)
        event_identity = _canonical_json_text(
            {
                "provider": "polygon",
                "contract_ticker": contract.contract_ticker,
                "sip_timestamp": row["sip_timestamp"],
                "sequence_number": sequence_number,
                "participant_timestamp": row.get("participant_timestamp"),
                "payload_sha256": payload_sha256,
            }
        )
        return OptionTradeEvent(
            trade_event_id=uuid5(NAMESPACE_URL, event_identity),
            provider="polygon",
            contract_id=contract.contract_id,
            contract_ticker=contract.contract_ticker,
            underlyer=contract.underlyer,
            sip_timestamp=sip_timestamp,
            sequence_number=sequence_number,
            participant_timestamp=participant_timestamp,
            first_observed_at=observed_at,
            revised_observed_at=None,
            exchange=exchange,
            conditions=conditions,
            correction=correction,
            price=price,
            size=size,
            shares_per_contract=contract.shares_per_contract,
            notional=price * size * contract.shares_per_contract,
            payload_sha256=payload_sha256,
            raw_batch_id=batch_id,
            provider_trade_id=(str(row["id"]) if row.get("id") is not None else None),
        )

    def _asset_type(self, underlyer: str) -> AssetType:
        if underlyer in self.configuration.settings.fixed_etf_underlyers:
            return AssetType.ETF
        if underlyer in self.configuration.settings.fixed_stock_underlyers:
            return AssetType.STOCK
        raise ValueError(f"underlyer is not in the configured universe: {underlyer}")


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _from_milliseconds(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000, tz=timezone.utc)


def _from_nanoseconds(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc)


def _to_nanoseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000_000)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _canonical_json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_json(value: object) -> str:
    return _sha256_text(_canonical_json_text(value))


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None