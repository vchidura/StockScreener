from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from options.domain import (
    BatchStatus,
    PageValidationStatus,
    RawBatchPage,
    RawOptionBatch,
)
from options.errors import OptionProviderError, ProviderErrorCategory

from .polygon_http import PolygonHttpResponse


ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class PolygonOptionChainDiscoveryLimits:
    maximum_pages: int
    maximum_rows: int
    maximum_page_bytes: int
    maximum_batch_bytes: int

    def __post_init__(self) -> None:
        values = (
            self.maximum_pages,
            self.maximum_rows,
            self.maximum_page_bytes,
            self.maximum_batch_bytes,
        )
        if any(type(value) is not int or value <= 0 for value in values):
            raise ValueError("option-chain discovery limits must be positive integers")
        if self.maximum_page_bytes > self.maximum_batch_bytes:
            raise ValueError("option-chain page limit exceeds the batch byte limit")


@dataclass(frozen=True, slots=True)
class PolygonOptionChainExcludedRange:
    field: str
    comparison: str
    boundary: str


@dataclass(frozen=True, slots=True)
class PolygonOptionChainIncludedRange:
    field: str
    comparison: str
    boundary: str


@dataclass(frozen=True, slots=True)
class PolygonOptionChainDiscoveryPlan:
    underlyer: str
    scheduled_cycle: datetime
    expiration_from: date
    expiration_through: date
    strike_min: Decimal
    strike_max: Decimal
    path: str
    filters: tuple[tuple[str, str], ...]
    request_filter_sha256: str
    included_ranges: tuple[PolygonOptionChainIncludedRange, ...]
    excluded_ranges: tuple[PolygonOptionChainExcludedRange, ...]

    @classmethod
    def build(
        cls,
        underlyer: str,
        as_of: datetime,
        expiration_through: date,
        strike_min: Decimal,
        strike_max: Decimal,
    ) -> "PolygonOptionChainDiscoveryPlan":
        as_of = _as_utc(as_of, "as_of")
        underlyer = underlyer.strip().upper()
        if not underlyer:
            raise ValueError("option-chain underlyer cannot be blank")
        if strike_min <= 0 or strike_max < strike_min:
            raise ValueError("option chain strike bounds are invalid")
        expiration_from = as_of.astimezone(ET).date()
        filters = {
            "expiration_date.gte": expiration_from.isoformat(),
            "expiration_date.lte": expiration_through.isoformat(),
            "strike_price.gte": str(strike_min),
            "strike_price.lte": str(strike_max),
            "limit": "250",
            "sort": "ticker",
            "order": "asc",
        }
        return cls(
            underlyer=underlyer,
            scheduled_cycle=as_of,
            expiration_from=expiration_from,
            expiration_through=expiration_through,
            strike_min=strike_min,
            strike_max=strike_max,
            path=f"/v3/snapshot/options/{quote(underlyer, safe='')}",
            filters=tuple(filters.items()),
            request_filter_sha256=_sha256_json(filters),
            included_ranges=(
                PolygonOptionChainIncludedRange(
                    "expiration_date", "gte", expiration_from.isoformat()
                ),
                PolygonOptionChainIncludedRange(
                    "expiration_date", "lte", expiration_through.isoformat()
                ),
                PolygonOptionChainIncludedRange("strike_price", "gte", str(strike_min)),
                PolygonOptionChainIncludedRange("strike_price", "lte", str(strike_max)),
            ),
            excluded_ranges=(
                PolygonOptionChainExcludedRange(
                    "expiration_date", "lt", expiration_from.isoformat()
                ),
                PolygonOptionChainExcludedRange(
                    "expiration_date", "gt", expiration_through.isoformat()
                ),
                PolygonOptionChainExcludedRange("strike_price", "lt", str(strike_min)),
                PolygonOptionChainExcludedRange("strike_price", "gt", str(strike_max)),
            ),
        )

    @property
    def request_filters(self) -> dict[str, str]:
        return dict(self.filters)


def build_polygon_option_chain_discovery_plan(
    underlyer: str,
    as_of: datetime,
    spot_price: Decimal,
    *,
    maximum_dte: int,
    acquisition_maximum_dte: int,
    strike_corridor_fraction: Decimal,
    expiration_anchor: date | None = None,
) -> PolygonOptionChainDiscoveryPlan:
    as_of = _as_utc(as_of, "as_of")
    if spot_price <= 0:
        raise ValueError("option-chain discovery spot must be positive")
    if type(maximum_dte) is not int or maximum_dte <= 0:
        raise ValueError("option-chain maximum DTE must be positive")
    if type(acquisition_maximum_dte) is not int or acquisition_maximum_dte <= 0:
        raise ValueError("option-chain acquisition maximum DTE must be positive")
    if not Decimal("0") < strike_corridor_fraction < Decimal("1"):
        raise ValueError("option-chain strike corridor must be in (0, 1)")
    expiration_anchor = expiration_anchor or as_of.astimezone(ET).date()
    expiration_through = expiration_anchor + timedelta(
        days=max(maximum_dte, acquisition_maximum_dte)
    )
    return PolygonOptionChainDiscoveryPlan.build(
        underlyer,
        as_of,
        expiration_through,
        spot_price * (Decimal("1") - strike_corridor_fraction),
        spot_price * (Decimal("1") + strike_corridor_fraction),
    )


@dataclass(frozen=True, slots=True)
class PolygonOptionChainDiscoveryResult:
    batch: RawOptionBatch
    rows: tuple[dict[str, Any], ...]
    included_ranges: tuple[PolygonOptionChainIncludedRange, ...]
    excluded_ranges: tuple[PolygonOptionChainExcludedRange, ...]

    def __post_init__(self) -> None:
        if not self.batch.complete:
            raise ValueError("option-chain discovery requires a complete raw batch")
        if self.batch.row_count != len(self.rows):
            raise ValueError("option-chain discovery row count does not match its batch")


class PolygonOptionChainDiscoveryAdapter:
    def __init__(
        self,
        http_get: Callable[[str, Mapping[str, str]], PolygonHttpResponse],
        *,
        base_url: str,
        limits: PolygonOptionChainDiscoveryLimits,
        clock: Callable[[], datetime],
        page_sink: Callable[[RawBatchPage], object] = lambda page: None,
        completion_sink: Callable[[], object] = lambda: None,
    ) -> None:
        base_url = base_url.rstrip("/")
        parsed = urlsplit(base_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("Polygon base URL must be HTTPS with a hostname")
        self.http_get = http_get
        self.base_url = base_url
        self._base = parsed
        self.limits = limits
        self.clock = clock
        self.page_sink = page_sink
        self.completion_sink = completion_sink

    def discover(
        self,
        plan: PolygonOptionChainDiscoveryPlan,
        *,
        batch_id: UUID,
        policy_sha256: str,
        started_at: datetime,
    ) -> PolygonOptionChainDiscoveryResult:
        url = f"{self.base_url}{plan.path}"
        params = plan.request_filters
        request_cursor_sha256 = None
        pages: list[RawBatchPage] = []
        rows: list[dict[str, Any]] = []
        seen_requests: set[str] = set()
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
            response = self.http_get(url, params)
            self._validate_response(response)
            try:
                payload = self._decode_json(response)
            except OptionProviderError:
                page = self._invalid_page(
                    batch_id,
                    len(pages) + 1,
                    response,
                    plan.request_filter_sha256,
                    request_cursor_sha256,
                    url,
                    params,
                )
                self.page_sink(page)
                pages.append(page)
                raise
            results = payload.get("results")
            if not isinstance(results, list) or not all(
                isinstance(row, dict) for row in results
            ):
                page = self._invalid_page(
                    batch_id,
                    len(pages) + 1,
                    response,
                    plan.request_filter_sha256,
                    request_cursor_sha256,
                    url,
                    params,
                )
                self.page_sink(page)
                pages.append(page)
                raise OptionProviderError(
                    ProviderErrorCategory.SCHEMA,
                    "Polygon option-chain response has an invalid results array",
                )
            next_url = payload.get("next_url")
            terminal = not next_url
            next_cursor_sha256 = None
            next_request = None
            validation_status = PageValidationStatus.VALID
            try:
                if next_url:
                    next_request = self._validate_next_page(
                        str(next_url), plan.path, plan.request_filters
                    )
                    next_cursor_sha256 = _sha256_text(next_request[2])
            except OptionProviderError:
                validation_status = PageValidationStatus.INVALID
            page = RawBatchPage(
                batch_id=batch_id,
                page_number=len(pages) + 1,
                row_count=len(results),
                response_bytes=response.body,
                payload_sha256=_sha256_bytes(response.body),
                received_at=_as_utc(self.clock(), "clock"),
                terminal=terminal,
                validation_status=validation_status,
                request_filter_sha256=plan.request_filter_sha256,
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
                        + [
                            (key, value)
                            for key, value in params.items()
                            if key != "apiKey"
                        ]
                    )
                ),
            )
            self.page_sink(page)
            pages.append(page)
            rows.extend(dict(row) for row in results)
            if validation_status is PageValidationStatus.INVALID:
                raise OptionProviderError(
                    ProviderErrorCategory.PAGINATION,
                    "Polygon option-chain next page failed validation",
                )
            self._check_caps(pages)
            if terminal:
                break
            url, params, cursor = next_request
            request_cursor_sha256 = _sha256_text(cursor)

        self.completion_sink()
        batch = RawOptionBatch(
            batch_id=batch_id,
            provider="polygon",
            underlyer=plan.underlyer,
            scheduled_cycle=plan.scheduled_cycle,
            request_filter_sha256=plan.request_filter_sha256,
            policy_sha256=policy_sha256,
            status=BatchStatus.COMPLETE,
            pages=tuple(pages),
            started_at=_as_utc(started_at, "started_at"),
            completed_at=_as_utc(self.clock(), "clock"),
        )
        return PolygonOptionChainDiscoveryResult(
            batch,
            tuple(rows),
            plan.included_ranges,
            plan.excluded_ranges,
        )

    def _validate_response(self, response: PolygonHttpResponse) -> None:
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
        if len(response.body) > self.limits.maximum_page_bytes:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon response exceeded the per-page byte cap",
            )

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

    def _check_caps(self, pages: list[RawBatchPage]) -> None:
        if len(pages) > self.limits.maximum_pages:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon chain exceeded the page cap",
            )
        if sum(page.row_count for page in pages) > self.limits.maximum_rows:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon chain exceeded the contract cap",
            )
        if sum(len(page.response_bytes) for page in pages) > self.limits.maximum_batch_bytes:
            raise OptionProviderError(
                ProviderErrorCategory.RESPONSE_LIMIT,
                "Polygon chain exceeded the batch byte cap",
            )

    def _invalid_page(
        self,
        batch_id: UUID,
        page_number: int,
        response: PolygonHttpResponse,
        request_filter_sha256: str,
        request_cursor_sha256: str | None,
        url: str,
        params: Mapping[str, str],
    ) -> RawBatchPage:
        return RawBatchPage(
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


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return _sha256_bytes(encoded)