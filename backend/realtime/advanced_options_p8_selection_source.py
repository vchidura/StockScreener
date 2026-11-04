"""Bounded Advanced-only Massive activity selection for one P8 canary."""
from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol
from urllib.parse import quote, urlsplit

from options.data.polygon_http import PolygonHttpTransport

from .advanced_options_p7_selection import (
    P7TransportCandidate,
    P7TransportSelection,
    P7TransportSelectionPolicy,
    P7TransportSelectionStatus,
    select_p7_transport_contracts,
)
from .contracts import canonical_sha256


MASSIVE_OPTIONS_SNAPSHOT_BASE_URL = "https://api.massive.com"
P8_EXACT_CONTRACT_REQUEST_COUNT = 1
P8_SELECTION_MAXIMUM_DECODED_BYTES = 100_000_000
P8_SELECTION_MAXIMUM_RESULTS_PER_UNDERLYING = 250
P8_EXACT_CONTRACT_RESULT_COUNT = 4
P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS = 5
SEALED_CAPTURE_GENERALIZED_C6_PROFILE = "GENERALIZED_C6"
SEALED_CAPTURE_EXACT_FOUR_PROFILE = "EXACT_FOUR_QQQ_SPY_V1"
SEALED_CAPTURE_COMPATIBILITY_PROFILES = frozenset({
    SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
    SEALED_CAPTURE_EXACT_FOUR_PROFILE,
})
P8_SELECTION_POLICY = P7TransportSelectionPolicy(
    version="advanced-options-p8-same-day-activity-v1",
    maximum_dte=0,
)
_OPTION_TICKER = re.compile(
    r"^O:([A-Z][A-Z0-9.-]{0,14})\d{6}([CP])\d{8}$"
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: object, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"{name} must be numeric") from error
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _nanoseconds(value: object, name: str) -> datetime:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive nanosecond timestamp")
    return datetime.fromtimestamp(
        value / 1_000_000_000,
        tz=timezone.utc,
    )


class P8SelectionSpotPort(Protocol):
    def load_spots(
        self,
        *,
        underlyings: tuple[str, ...],
        available_before: datetime,
    ) -> Mapping[str, Decimal]: ...


@dataclass(frozen=True, slots=True)
class P8ActivitySelectionCapture:
    selection: P7TransportSelection
    request_count: int
    decoded_bytes: int
    pages: int
    pagination_requests: int
    source_snapshot_at: datetime
    evidence: Mapping[str, object]
    selected_snapshots: Mapping[str, Mapping[str, object]]

    def __post_init__(self) -> None:
        if (
            self.selection.status is not P7TransportSelectionStatus.READY
            or type(self.request_count) is not int
            or self.request_count <= 0
            or not 0 < self.decoded_bytes
            <= P8_SELECTION_MAXIMUM_DECODED_BYTES
            or self.pages != self.request_count
            or type(self.pagination_requests) is not int
            or not 0
            <= self.pagination_requests
            < self.request_count
            or set(self.selected_snapshots) != set(self.selection.contracts)
        ):
            raise ValueError("P8 activity selection capture is incomplete")
        object.__setattr__(
            self,
            "source_snapshot_at",
            _utc(self.source_snapshot_at, "source_snapshot_at"),
        )


@dataclass(frozen=True, slots=True)
class P8SelectionProviderAttempt:
    """One real provider request, whether or not its capture succeeded."""

    http_status: int | None
    response_bytes: int
    row_count: int = 0


class P8SelectionNoUsableContracts(ValueError):
    """Every returned row was rejected; only the reasons are retained."""

    def __init__(
        self,
        underlyer: str,
        *,
        row_count: int,
        page_count: int,
        rejections: Mapping[str, int],
    ) -> None:
        self.underlyer = underlyer
        self.row_count = row_count
        self.page_count = page_count
        self.rejections = dict(rejections)
        histogram = ",".join(
            f"{reason}={count}"
            for reason, count in sorted(
                self.rejections.items(),
                key=lambda item: (-item[1], item[0]),
            )
        )
        self.reason_code = (
            f"NO_USABLE_CONTRACTS:{underlyer}:rows={row_count}"
            + (f":{histogram}" if histogram else "")
        )
        super().__init__(
            f"P8 Massive selection has no usable contracts for {underlyer}: "
            f"pages={page_count} rows={row_count}"
            + (f" {histogram}" if histogram else "")
        )


def _snapshot_rejection_reason(row: Mapping[str, object]) -> str | None:
    """Why a chain row cannot be a candidate; the provider marks every
    snapshot field optional, so an absent field rejects only its own row."""
    details = row.get("details")
    quote_row = row.get("last_quote")
    day_row = row.get("day", row.get("session"))
    underlying = row.get("underlying_asset")
    for value, reason in (
        (details, "MISSING_DETAILS"),
        (quote_row, "MISSING_LAST_QUOTE"),
        (day_row, "MISSING_DAY"),
        (underlying, "MISSING_UNDERLYING"),
    ):
        if not isinstance(value, Mapping):
            return reason
    if str(details.get("contract_type", "")).strip().lower() not in {
        "call",
        "put",
    }:
        return "UNKNOWN_CONTRACT_TYPE"
    if (
        details.get("expiration_date") is None
        or details.get("strike_price") is None
    ):
        return "INCOMPLETE_DETAILS"
    if quote_row.get("bid") is None or quote_row.get("ask") is None:
        return "INCOMPLETE_QUOTE"
    bid = _decimal(quote_row["bid"], "last_quote.bid")
    ask = _decimal(quote_row["ask"], "last_quote.ask")
    if bid < 0:
        return "NEGATIVE_BID"
    if ask <= 0:
        return "NON_POSITIVE_ASK"
    if ask < bid:
        return "CROSSED_QUOTE"
    if not quote_row.get("last_updated"):
        return "INCOMPLETE_QUOTE"
    if underlying.get("price", underlying.get("value")) is None:
        return "MISSING_UNDERLYING_PRICE"
    raw_open_interest = row.get("open_interest")
    if (
        int(day_row.get("volume") or 0) < 0
        or raw_open_interest is not None
        and int(raw_open_interest) < 0
    ):
        return "NEGATIVE_ACTIVITY"
    return None


def _candidate_from_snapshot(
    row: Mapping[str, object],
    *,
    session_date: date,
) -> P7TransportCandidate | None:
    if _snapshot_rejection_reason(row) is not None:
        return None
    details = row["details"]
    quote_row = row["last_quote"]
    day_row = row.get("day", row.get("session"))
    underlying = row["underlying_asset"]
    ticker = str(
        details.get("ticker", row.get("ticker", ""))
    ).strip().upper()
    option_type = {
        "call": "C",
        "put": "P",
    }[str(details["contract_type"]).strip().lower()]
    expiration = date.fromisoformat(str(details["expiration_date"]))
    bid = _decimal(quote_row["bid"], "last_quote.bid")
    ask = _decimal(quote_row["ask"], "last_quote.ask")
    day_volume = int(day_row.get("volume") or 0)
    raw_open_interest = row.get("open_interest")
    open_interest = (
        None
        if raw_open_interest is None
        else int(raw_open_interest)
    )
    return P7TransportCandidate(
        contract_ticker=ticker,
        underlyer=str(underlying.get("ticker", "")),
        expiration_date=expiration,
        option_type=option_type,
        strike=_decimal(
            details["strike_price"],
            "details.strike_price",
        ),
        spot=_decimal(
            underlying.get("price", underlying.get("value")),
            "underlying_asset.price",
        ),
        bid=bid,
        ask=ask,
        day_volume=day_volume,
        open_interest=open_interest,
        calendar_dte=(expiration - session_date).days,
        market_data_time=_nanoseconds(
            quote_row.get("last_updated"),
            "last_quote.last_updated",
        ),
        source_sha256=canonical_sha256(row),
    )


def _select_exact_call_put_contracts(
    candidates: tuple[P7TransportCandidate, ...],
    *,
    reviewed_at: datetime,
    policy: P7TransportSelectionPolicy,
) -> P7TransportSelection:
    configured_pairs = {
        (underlyer, option_type)
        for underlyer in policy.underlyings
        for option_type in ("C", "P")
    }
    observed_pairs = {
        (row.underlyer, row.option_type) for row in candidates
    }
    legacy_exact_four = (
        policy == P8_SELECTION_POLICY
        and len(candidates) == P8_EXACT_CONTRACT_RESULT_COUNT
    )
    if (
        legacy_exact_four
        and observed_pairs != configured_pairs
        or not legacy_exact_four
        and {
            row.underlyer for row in candidates
        }
        != set(policy.underlyings)
    ):
        raise RuntimeError(
            "P8 configured exact-contract sides are incomplete"
        )
    if legacy_exact_four:
        for option_type in ("C", "P"):
            selection = select_p7_transport_contracts(
                tuple(
                    row
                    for row in candidates
                    if row.option_type == option_type
                ),
                reviewed_at=reviewed_at,
                policy=policy,
            )
            if (
                selection.status
                is not P7TransportSelectionStatus.READY
            ):
                raise RuntimeError(
                    "P8 Massive exact-contract activity blocked: "
                    + ",".join(selection.reason_codes)
                )
    selected_rows = tuple(sorted(
        candidates,
        key=lambda row: (
            row.underlyer,
            row.option_type,
            row.contract_ticker,
        ),
    ))
    source_sha256 = canonical_sha256(tuple(
        (
            row.contract_ticker,
            row.market_data_time,
            row.source_sha256,
        )
        for row in selected_rows
    ))
    valid_until = min(
        row.market_data_time
        + timedelta(seconds=policy.maximum_snapshot_age_seconds)
        for row in selected_rows
    )
    selection_sha256 = canonical_sha256({
        "status": P7TransportSelectionStatus.READY,
        "policy_sha256": policy.sha256,
        "reviewed_at": reviewed_at,
        "valid_until": valid_until,
        "source_sha256": source_sha256,
        "selected": selected_rows,
        "reason_codes": (),
    })
    return P7TransportSelection(
        P7TransportSelectionStatus.READY,
        policy,
        reviewed_at,
        valid_until,
        selected_rows,
        source_sha256,
        selection_sha256,
        (),
    )


def p8_activity_selection_capture_from_sealed(
    value: Mapping[str, object],
    *,
    compatibility_profile: str | None = None,
) -> P8ActivitySelectionCapture:
    recorded_profile = value.get("compatibility_profile")
    resolved_profile = (
        recorded_profile
        if compatibility_profile is None
        else compatibility_profile
    )
    if (
        value.get("contract")
        != "advanced-options-v3-pre-session-capture-v1"
        or resolved_profile not in SEALED_CAPTURE_COMPATIBILITY_PROFILES
        or recorded_profile is not None
        and recorded_profile != resolved_profile
        or type(value.get("provider_requests")) is not int
        or value["provider_requests"] <= 0
        or type(value.get("decoded_bytes")) is not int
        or not 0 < value["decoded_bytes"]
        <= P8_SELECTION_MAXIMUM_DECODED_BYTES
        or not isinstance(value.get("responses"), list)
        or not isinstance(value.get("selected_snapshots"), Mapping)
        or not value["selected_snapshots"]
    ):
        raise ValueError("sealed P8 activity selection scope changed")
    session_date = date.fromisoformat(str(value["session_date"]))
    reviewed_at = _utc(
        datetime.fromisoformat(
            str(value["captured_at_utc"]).replace("Z", "+00:00")
        ),
        "captured_at_utc",
    )
    if reviewed_at.date() != session_date:
        raise ValueError("sealed P8 activity selection session changed")
    responses = value["responses"]
    if (
        any(not isinstance(row, Mapping) for row in responses)
        or sum(
            int(row.get("page_count", 1)) for row in responses
        )
        != value["provider_requests"]
    ):
        raise ValueError("sealed P8 response membership changed")
    recorded_underlyings = value.get("configured_underlyings")
    if recorded_underlyings is None:
        configured_underlyings = tuple(sorted({
            str(row.get("underlyer", "")).strip().upper()
            for row in responses
        }))
    elif (
        not isinstance(recorded_underlyings, list)
        or not recorded_underlyings
    ):
        raise ValueError("sealed P8 configured underlyings changed")
    else:
        configured_underlyings = tuple(
            str(underlying).strip().upper()
            for underlying in recorded_underlyings
        )
    if (
        not configured_underlyings
        or len(configured_underlyings)
        != len(set(configured_underlyings))
        or any(not underlying for underlying in configured_underlyings)
    ):
        raise ValueError("sealed P8 configured underlyings changed")
    snapshots = value["selected_snapshots"]
    candidates = []
    selected_snapshots = {}
    for ticker, row in sorted(snapshots.items()):
        if not isinstance(row, Mapping):
            raise TypeError("sealed P8 option snapshot must be an object")
        candidate = _candidate_from_snapshot(
            row,
            session_date=session_date,
        )
        if (
            candidate is None
            or candidate.contract_ticker != str(ticker).strip().upper()
        ):
            raise ValueError("sealed P8 option snapshot identity changed")
        candidates.append(candidate)
        selected_snapshots[candidate.contract_ticker] = row
    pairs = {
        (row.underlyer, row.option_type)
        for row in candidates
    }
    underlyings = tuple(sorted({
        row.underlyer for row in candidates
    }))
    response_underlyings = {
        str(row.get("underlyer", "")).strip().upper()
        for row in responses
    }
    expected_exact_pairs = {
        (underlying, option_type)
        for underlying in P8_SELECTION_POLICY.underlyings
        for option_type in ("C", "P")
    }
    if (
        not pairs
        or not set(underlyings).issubset(configured_underlyings)
        or response_underlyings != set(configured_underlyings)
    ):
        raise ValueError("sealed P8 call/put membership changed")
    if resolved_profile == SEALED_CAPTURE_EXACT_FOUR_PROFILE:
        if (
            configured_underlyings != P8_SELECTION_POLICY.underlyings
            or len(candidates) != P8_EXACT_CONTRACT_RESULT_COUNT
            or pairs != expected_exact_pairs
        ):
            raise ValueError(
                "sealed P8 explicit exact-four membership changed"
            )
        policy = P8_SELECTION_POLICY
    else:
        policy = P7TransportSelectionPolicy(
            version="advanced-options-c6-configured-capture-v1",
            underlyings=configured_underlyings,
            minimum_dte=0,
            maximum_dte=60,
        )
    latest_market_time = max(
        row.market_data_time for row in candidates
    )
    if latest_market_time > reviewed_at:
        if (
            latest_market_time - reviewed_at
        ).total_seconds() > (
            P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS
        ):
            raise ValueError(
                "sealed P8 provider clock skew exceeded"
            )
        reviewed_at = latest_market_time
    if resolved_profile == SEALED_CAPTURE_EXACT_FOUR_PROFILE:
        for option_type in ("C", "P"):
            selection = select_p7_transport_contracts(
                tuple(
                    row
                    for row in candidates
                    if row.option_type == option_type
                ),
                reviewed_at=reviewed_at,
                policy=policy,
            )
            if (
                selection.status
                is not P7TransportSelectionStatus.READY
            ):
                raise ValueError(
                    "sealed P8 activity selection is no longer eligible"
                )
    selected = tuple(sorted(
        candidates,
        key=lambda row: (
            row.underlyer,
            row.option_type,
            row.contract_ticker,
        ),
    ))
    source_sha256 = canonical_sha256(tuple(
        (
            row.contract_ticker,
            row.market_data_time,
            row.source_sha256,
        )
        for row in sorted(
            selected,
            key=lambda row: (
                row.underlyer,
                row.contract_ticker,
                row.market_data_time,
            ),
        )
    ))
    valid_until = min(
        row.market_data_time
        + timedelta(
            seconds=policy.maximum_snapshot_age_seconds
        )
        for row in selected
    )
    selection_sha256 = canonical_sha256({
        "status": P7TransportSelectionStatus.READY,
        "policy_sha256": policy.sha256,
        "reviewed_at": reviewed_at,
        "valid_until": valid_until,
        "source_sha256": source_sha256,
        "selected": selected,
        "reason_codes": (),
    })
    selection = P7TransportSelection(
        P7TransportSelectionStatus.READY,
        policy,
        reviewed_at,
        valid_until,
        selected,
        source_sha256,
        selection_sha256,
        (),
    )
    source_snapshot_at = min(
        row.market_data_time for row in selected
    )
    evidence = {
        "schema": "advanced-options-p8-activity-selection-v1",
        "compatibility_profile": resolved_profile,
        "configured_underlyings": list(configured_underlyings),
        "source_snapshot_at_utc": source_snapshot_at.isoformat(),
        "selected_at_utc": reviewed_at.isoformat(),
        "valid_until_utc": valid_until.isoformat(),
        "policy_sha256": policy.sha256,
        "source_snapshot_sha256": canonical_sha256(responses),
        "request_count": value["provider_requests"],
        "decoded_bytes": value["decoded_bytes"],
        "pages": value["provider_requests"],
        "pagination_requests": (
            value["provider_requests"] - len(responses)
        ),
        "sealed_capture_semantic_sha256": value.get("semantic_sha256"),
        "selected": [
            {
                "underlyer": row.underlyer,
                "contract_ticker": row.contract_ticker,
            }
            for row in selected
        ],
    }
    evidence["selection_sha256"] = canonical_sha256(evidence)
    return P8ActivitySelectionCapture(
        selection=selection,
        request_count=value["provider_requests"],
        decoded_bytes=value["decoded_bytes"],
        pages=value["provider_requests"],
        pagination_requests=(
            value["provider_requests"] - len(responses)
        ),
        source_snapshot_at=source_snapshot_at,
        evidence=evidence,
        selected_snapshots=selected_snapshots,
    )


class MassiveAdvancedOptionsSelectionSource:
    """Exactly one filtered option-chain snapshot request per QQQ/SPY."""

    def __init__(
        self,
        transport: PolygonHttpTransport,
        spots: P8SelectionSpotPort,
        *,
        policy: P7TransportSelectionPolicy | None = None,
        base_url: str = MASSIVE_OPTIONS_SNAPSHOT_BASE_URL,
        clock: Callable[[], datetime] | None = None,
        maximum_response_decoded_bytes: int = (
            P8_SELECTION_MAXIMUM_DECODED_BYTES
        ),
        maximum_total_decoded_bytes: int = (
            P8_SELECTION_MAXIMUM_DECODED_BYTES
        ),
        maximum_provider_requests: int | None = None,
        deadline: datetime | None = None,
        retain_candidate_pool: bool = False,
        query_minimum_dte: int | None = None,
        query_maximum_dte: int | None = None,
        query_maximum_moneyness_fraction: Decimal | None = None,
    ) -> None:
        if not callable(getattr(transport, "get", None)):
            raise TypeError("P8 Massive transport is invalid")
        if not callable(getattr(spots, "load_spots", None)):
            raise TypeError("P8 selection spot port is invalid")
        normalized_base = base_url.rstrip("/")
        if normalized_base not in {
            "https://api.massive.com",
            "https://api.polygon.io",
        }:
            raise ValueError("P8 Massive base URL is not approved")
        if (
            type(maximum_response_decoded_bytes) is not int
            or type(maximum_total_decoded_bytes) is not int
            or not 0
            < maximum_response_decoded_bytes
            <= maximum_total_decoded_bytes
            <= P8_SELECTION_MAXIMUM_DECODED_BYTES
        ):
            raise ValueError(
                "P8 selection decoded-byte ceilings are invalid"
            )
        selected_policy = policy or P8_SELECTION_POLICY
        minimum_dte = (
            selected_policy.minimum_dte
            if query_minimum_dte is None
            else query_minimum_dte
        )
        maximum_dte = (
            selected_policy.maximum_dte
            if query_maximum_dte is None
            else query_maximum_dte
        )
        maximum_moneyness = (
            selected_policy.maximum_moneyness_fraction
            if query_maximum_moneyness_fraction is None
            else query_maximum_moneyness_fraction
        )
        request_ceiling = (
            len(selected_policy.underlyings)
            if maximum_provider_requests is None
            else maximum_provider_requests
        )
        if (
            type(request_ceiling) is not int
            or request_ceiling < len(selected_policy.underlyings)
            or type(retain_candidate_pool) is not bool
            or type(minimum_dte) is not int
            or type(maximum_dte) is not int
            or not 0 <= minimum_dte <= maximum_dte <= 60
            or not Decimal("0")
            < maximum_moneyness
            <= Decimal("0.25")
        ):
            raise ValueError(
                "P8 selection provider request ceiling is invalid"
            )
        self._transport = transport
        self._spots = spots
        self._policy = selected_policy
        self._base_url = normalized_base
        self._clock = clock
        self._maximum_response_decoded_bytes = (
            maximum_response_decoded_bytes
        )
        self._maximum_total_decoded_bytes = (
            maximum_total_decoded_bytes
        )
        self._maximum_provider_requests = request_ceiling
        self._retain_candidate_pool = retain_candidate_pool
        self._query_minimum_dte = minimum_dte
        self._query_maximum_dte = maximum_dte
        self._query_maximum_moneyness_fraction = maximum_moneyness
        self._deadline = (
            None if deadline is None else _utc(deadline, "deadline")
        )
        self._provider_attempts: list[P8SelectionProviderAttempt] = []

    @property
    def provider_attempts(self) -> tuple[P8SelectionProviderAttempt, ...]:
        """Every request the latest capture really made, even if it failed."""
        return tuple(self._provider_attempts)

    def _request(
        self,
        underlyer: str,
        *,
        spot: Decimal,
        session_date: date,
        maximum_requests: int | None = None,
        maximum_total_decoded_bytes: int | None = None,
    ) -> tuple[
        tuple[P7TransportCandidate, ...],
        int,
        datetime,
        dict,
        dict[str, Mapping[str, object]],
    ]:
        policy = self._policy
        lower = spot * (
            Decimal("1") - self._query_maximum_moneyness_fraction
        )
        upper = spot * (
            Decimal("1") + self._query_maximum_moneyness_fraction
        )
        request_limit = (
            self._maximum_provider_requests
            if maximum_requests is None
            else maximum_requests
        )
        byte_limit = (
            self._maximum_total_decoded_bytes
            if maximum_total_decoded_bytes is None
            else maximum_total_decoded_bytes
        )
        if (
            type(request_limit) is not int
            or request_limit <= 0
            or type(byte_limit) is not int
            or byte_limit <= 0
        ):
            raise ValueError("P8 pagination budgets are invalid")
        next_url = (
            f"{self._base_url}/v3/snapshot/options/"
            f"{quote(underlyer, safe='')}"
        )
        next_params = {
            "expiration_date.gte": (
                session_date
                + timedelta(days=self._query_minimum_dte)
            ).isoformat(),
            "expiration_date.lte": (
                session_date
                + timedelta(days=self._query_maximum_dte)
            ).isoformat(),
            "strike_price.gte": str(lower),
            "strike_price.lte": str(upper),
            "limit": str(P8_SELECTION_MAXIMUM_RESULTS_PER_UNDERLYING),
            "sort": "ticker",
            "order": "asc",
        }
        candidates: list[P7TransportCandidate] = []
        snapshots: dict[str, Mapping[str, object]] = {}
        snapshot_clocks: list[datetime] = []
        pages = []
        page_received_times = []
        seen_urls = set()
        decoded_bytes = 0
        while next_url is not None:
            if len(pages) >= request_limit:
                raise ValueError(
                    "P8 Massive selection provider request ceiling "
                    "exceeded during pagination"
                )
            if self._deadline is not None:
                now = (
                    datetime.now(timezone.utc)
                    if self._clock is None
                    else _utc(self._clock(), "selection clock")
                )
                if now > self._deadline:
                    raise TimeoutError(
                        "P8 Massive selection pagination deadline expired"
                    )
            if next_url in seen_urls:
                raise ValueError(
                    "P8 Massive selection pagination cycle detected"
                )
            seen_urls.add(next_url)
            parsed = urlsplit(next_url)
            if (
                parsed.scheme != "https"
                or f"{parsed.scheme}://{parsed.netloc}"
                not in {
                    "https://api.massive.com",
                    "https://api.polygon.io",
                }
                or parsed.fragment
                or not parsed.path.startswith(
                    "/v3/snapshot/options/"
                )
            ):
                raise ValueError(
                    "P8 Massive selection next_url is not approved"
                )
            try:
                response = self._transport.get(next_url, next_params)
            except Exception:
                self._provider_attempts.append(P8SelectionProviderAttempt(
                    getattr(self._transport, "http_status", None),
                    getattr(self._transport, "response_bytes", 0),
                ))
                raise
            self._provider_attempts.append(P8SelectionProviderAttempt(
                response.status_code,
                len(response.body),
            ))
            if response.status_code != 200:
                raise RuntimeError(
                    f"P8 Massive selection failed for {underlyer}: "
                    f"HTTP {response.status_code}"
                )
            response_bytes = len(response.body)
            if not 0 < response_bytes <= (
                self._maximum_response_decoded_bytes
            ):
                raise ValueError(
                    "P8 Massive selection response decoded-byte ceiling "
                    "exceeded"
                )
            decoded_bytes += response_bytes
            if decoded_bytes > byte_limit:
                raise ValueError(
                    "P8 Massive selection exceeded decoded-byte ceiling"
                )
            try:
                payload = json.loads(response.body)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError(
                    "P8 Massive selection returned invalid JSON for "
                    f"{underlyer}"
                ) from error
            page_received_at = (
                datetime.now(timezone.utc)
                if self._clock is None
                else _utc(self._clock(), "selection clock")
            )
            if (
                not isinstance(payload, dict)
                or payload.get("status") not in {"OK", "DELAYED"}
                or not isinstance(payload.get("results"), list)
                or len(payload["results"])
                > P8_SELECTION_MAXIMUM_RESULTS_PER_UNDERLYING
            ):
                raise ValueError(
                    "P8 Massive selection scope changed for "
                    f"{underlyer}"
                )
            self._provider_attempts[-1] = replace(
                self._provider_attempts[-1],
                row_count=len(payload["results"]),
            )
            for row in payload["results"]:
                if not isinstance(row, dict):
                    raise TypeError(
                        "P8 option snapshot row must be an object"
                    )
                candidate = _candidate_from_snapshot(
                    row,
                    session_date=session_date,
                )
                if candidate is None:
                    continue
                if candidate.contract_ticker in snapshots:
                    raise ValueError(
                        "P8 Massive pagination repeated a contract"
                    )
                snapshot_clocks.append(candidate.market_data_time)
                candidates.append(candidate)
                snapshots[candidate.contract_ticker] = row
            if (
                snapshot_clocks
                and max(snapshot_clocks) - page_received_at
                > timedelta(
                    seconds=(
                        P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS
                    )
                )
            ):
                raise ValueError("P8 provider clock skew exceeded")
            pages.append(payload)
            page_received_times.append(page_received_at)
            raw_next_url = payload.get("next_url")
            next_url = (
                None
                if raw_next_url in {None, ""}
                else str(raw_next_url)
            )
            next_params = {}
        if not candidates or not snapshot_clocks:
            rows = tuple(
                row
                for page in pages
                for row in page["results"]
            )
            raise P8SelectionNoUsableContracts(
                underlyer,
                row_count=len(rows),
                page_count=len(pages),
                rejections=Counter(
                    _snapshot_rejection_reason(row) for row in rows
                ),
            )
        aggregate = {
            "status": "OK",
            "request_id": [
                page.get("request_id") for page in pages
            ],
            "results": [
                row
                for page in pages
                for row in page["results"]
            ],
            "page_count": len(pages),
            "pagination_requests": max(0, len(pages) - 1),
            "pages_sha256": canonical_sha256(pages),
            "received_at_utc": max(
                page_received_times
            ).isoformat(),
        }
        return (
            tuple(candidates),
            decoded_bytes,
            max(snapshot_clocks),
            aggregate,
            snapshots,
        )

    def capture(
        self,
        *,
        reviewed_at: datetime,
        session_date: date,
    ) -> P8ActivitySelectionCapture:
        self._provider_attempts = []
        reviewed = _utc(reviewed_at, "reviewed_at")
        if reviewed.date() != session_date:
            raise ValueError("P8 selection session date changed")
        spots = self._spots.load_spots(
            underlyings=self._policy.underlyings,
            available_before=reviewed,
        )
        if set(spots) != set(self._policy.underlyings):
            raise ValueError("P8 retained spot membership changed")
        candidates: list[P7TransportCandidate] = []
        decoded_bytes = 0
        snapshot_clocks: list[datetime] = []
        responses = []
        snapshots: dict[str, Mapping[str, object]] = {}
        request_count = 0
        pagination_requests = 0
        for underlyer in self._policy.underlyings:
            rows, byte_count, snapshot_at, payload, raw_snapshots = self._request(
                underlyer,
                spot=_decimal(spots[underlyer], f"{underlyer} spot"),
                session_date=session_date,
                maximum_requests=(
                    self._maximum_provider_requests - request_count
                ),
                maximum_total_decoded_bytes=(
                    self._maximum_total_decoded_bytes - decoded_bytes
                ),
            )
            page_count = int(payload["page_count"])
            request_count += page_count
            pagination_requests += int(
                payload["pagination_requests"]
            )
            candidates.extend(rows)
            decoded_bytes += byte_count
            snapshot_clocks.append(snapshot_at)
            responses.append({
                "underlyer": underlyer,
                "request_id": payload.get("request_id"),
                "response_sha256": canonical_sha256(payload),
                "row_count": len(payload["results"]),
                "page_count": page_count,
            })
            snapshots.update(raw_snapshots)
            if decoded_bytes > self._maximum_total_decoded_bytes:
                raise ValueError(
                    "P8 Massive selection exceeded decoded-byte ceiling"
                )
        local_completed_at = (
            reviewed
            if self._clock is None
            else max(reviewed, _utc(self._clock(), "selection clock"))
        )
        latest_snapshot_at = max(snapshot_clocks)
        if (
            latest_snapshot_at - local_completed_at
        ).total_seconds() > P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS:
            raise ValueError("P8 provider clock skew exceeded")
        selected_at = max(local_completed_at, latest_snapshot_at)
        if self._retain_candidate_pool:
            selected_rows = tuple(sorted(
                candidates,
                key=lambda row: (
                    row.underlyer,
                    row.expiration_date,
                    row.option_type,
                    row.strike,
                    row.contract_ticker,
                ),
            ))
            missing = tuple(
                underlyer
                for underlyer in self._policy.underlyings
                if not any(
                    row.underlyer == underlyer
                    for row in selected_rows
                )
            )
            if missing:
                raise RuntimeError(
                    "P8 Massive C6 candidate pool is unavailable: "
                    + ",".join(missing)
                )
            source_sha256 = canonical_sha256(tuple(
                (
                    row.contract_ticker,
                    row.market_data_time,
                    row.source_sha256,
                )
                for row in selected_rows
            ))
            reason_codes = ("C6_CANDIDATE_POOL_RETAINED",)
            valid_until = selected_at + timedelta(
                seconds=self._policy.maximum_snapshot_age_seconds
            )
            selection = P7TransportSelection(
                P7TransportSelectionStatus.READY,
                self._policy,
                selected_at,
                valid_until,
                selected_rows,
                source_sha256,
                canonical_sha256({
                    "status": P7TransportSelectionStatus.READY,
                    "policy_sha256": self._policy.sha256,
                    "reviewed_at": selected_at,
                    "valid_until": valid_until,
                    "source_sha256": source_sha256,
                    "selected": selected_rows,
                    "reason_codes": reason_codes,
                    "selection_mode": "C6_CANDIDATE_POOL",
                }),
                reason_codes,
            )
        else:
            selection = select_p7_transport_contracts(
                tuple(candidates),
                reviewed_at=selected_at,
                policy=self._policy,
            )
            if selection.status is not P7TransportSelectionStatus.READY:
                raise RuntimeError(
                    "P8 Massive activity selection blocked: "
                    + ",".join(selection.reason_codes)
                )
        source_snapshot_at = min(snapshot_clocks)
        evidence = {
            "schema": (
                "advanced-options-c6-candidate-pool-v1"
                if self._retain_candidate_pool
                else "advanced-options-p8-activity-selection-v1"
            ),
            "source_snapshot_at_utc": source_snapshot_at.isoformat(),
            "selected_at_utc": selected_at.isoformat(),
            "valid_until_utc": selection.valid_until.isoformat(),
            "policy_sha256": self._policy.sha256,
            "source_snapshot_sha256": canonical_sha256(responses),
            "request_count": request_count,
            "decoded_bytes": decoded_bytes,
            "pages": request_count,
            "pagination_requests": pagination_requests,
            "candidate_count": len(selection.selected),
            "query": {
                "minimum_dte": self._query_minimum_dte,
                "maximum_dte": self._query_maximum_dte,
                "maximum_moneyness_fraction": str(
                    self._query_maximum_moneyness_fraction
                ),
            },
            "selected": [
                {
                    "underlyer": row.underlyer,
                    "contract_ticker": row.contract_ticker,
                }
                for row in selection.selected
            ],
        }
        evidence["selection_sha256"] = canonical_sha256(evidence)
        return P8ActivitySelectionCapture(
            selection=selection,
            request_count=request_count,
            decoded_bytes=decoded_bytes,
            pages=request_count,
            pagination_requests=pagination_requests,
            source_snapshot_at=source_snapshot_at,
            evidence=evidence,
            selected_snapshots={
                row.contract_ticker: snapshots[row.contract_ticker]
                for row in selection.selected
            },
        )


def combine_exact_contract_captures(
    captures: tuple[P8ActivitySelectionCapture, ...],
    *,
    contract_tickers: tuple[str, ...],
) -> P8ActivitySelectionCapture:
    tickers = tuple(sorted(contract_tickers))
    selected = tuple(row for capture in captures for row in capture.selection.selected)
    if (
        not captures or not tickers
        or len(set(tickers)) != len(tickers)
        or len(selected) != len(tickers)
        or {row.contract_ticker for row in selected} != set(tickers)
    ):
        raise ValueError("batched exact-contract capture membership changed")
    if len(captures) == 1:
        return captures[0]
    reviewed_at = max(capture.selection.reviewed_at for capture in captures)
    if any(
        capture.selection.valid_until is None
        or capture.selection.valid_until <= reviewed_at
        for capture in captures
    ):
        raise ValueError("batched exact-contract capture clocks do not overlap")
    underlyings = tuple(sorted({row.underlyer for row in selected}))
    policy = (
        P8_SELECTION_POLICY
        if underlyings == P8_SELECTION_POLICY.underlyings
        else P7TransportSelectionPolicy(
            version="advanced-options-c6-configured-capture-v1",
            underlyings=underlyings, minimum_dte=0, maximum_dte=60,
        )
    )
    selection = _select_exact_call_put_contracts(
        selected, reviewed_at=reviewed_at, policy=policy,
    )
    batches = [
        {
            "selection_sha256": capture.evidence["selection_sha256"],
            "source_snapshot_sha256": capture.evidence["source_snapshot_sha256"],
            "selected_at_utc": capture.selection.reviewed_at.isoformat(),
            "contracts": sorted(capture.selection.contracts),
        }
        for capture in captures
    ]
    request_count = sum(capture.request_count for capture in captures)
    decoded_bytes = sum(capture.decoded_bytes for capture in captures)
    source_snapshot_at = min(capture.source_snapshot_at for capture in captures)
    evidence = {
        "schema": "advanced-options-p8-exact-contract-batches-v1",
        "endpoint": "/v3/snapshot",
        "source_snapshot_at_utc": source_snapshot_at.isoformat(),
        "selected_at_utc": reviewed_at.isoformat(),
        "valid_until_utc": selection.valid_until.isoformat(),
        "policy_sha256": policy.sha256,
        "source_snapshot_sha256": canonical_sha256(batches),
        "batches": batches,
        "request_count": request_count, "decoded_bytes": decoded_bytes,
        "pages": request_count, "pagination_requests": 0, "contracts": list(tickers),
    }
    evidence["selection_sha256"] = canonical_sha256(evidence)
    return P8ActivitySelectionCapture(
        selection=selection, request_count=request_count,
        decoded_bytes=decoded_bytes, pages=request_count, pagination_requests=0,
        source_snapshot_at=source_snapshot_at, evidence=evidence,
        selected_snapshots={
            ticker: raw for capture in captures
            for ticker, raw in capture.selected_snapshots.items()
        },
    )


class MassiveAdvancedOptionsExactContractSource:
    """One unified snapshot request for configured admitted contracts."""

    def __init__(
        self,
        transport: PolygonHttpTransport,
        *,
        policy: P7TransportSelectionPolicy | None = None,
        base_url: str = MASSIVE_OPTIONS_SNAPSHOT_BASE_URL,
        clock: Callable[[], datetime] | None = None,
        maximum_decoded_bytes: int = (
            P8_SELECTION_MAXIMUM_DECODED_BYTES
        ),
    ) -> None:
        if not callable(getattr(transport, "get", None)):
            raise TypeError("P8 Massive transport is invalid")
        normalized_base = base_url.rstrip("/")
        if normalized_base not in {
            "https://api.massive.com",
            "https://api.polygon.io",
        }:
            raise ValueError("P8 Massive base URL is not approved")
        if (
            type(maximum_decoded_bytes) is not int
            or maximum_decoded_bytes <= 0
        ):
            raise ValueError(
                "exact-contract response byte ceiling must be positive"
            )
        self._transport = transport
        self._policy = policy or P8_SELECTION_POLICY
        self._base_url = normalized_base
        self._clock = clock
        self._maximum_decoded_bytes = maximum_decoded_bytes

    def capture(
        self,
        *,
        contract_tickers: tuple[str, ...],
        reviewed_at: datetime,
        session_date: date,
    ) -> P8ActivitySelectionCapture:
        reviewed = _utc(reviewed_at, "reviewed_at")
        tickers = tuple(sorted(
            str(value).strip().upper()
            for value in contract_tickers
        ))
        memberships = {
            (
                match.group(1),
                match.group(2),
            )
            for ticker in tickers
            if (match := _OPTION_TICKER.fullmatch(ticker)) is not None
        }
        underlyings = tuple(sorted({
            underlyer for underlyer, _ in memberships
        }))
        if (
            reviewed.date() != session_date
            or not tickers
            or len(set(tickers)) != len(tickers)
            or not memberships
            or not underlyings
        ):
            raise ValueError(
                "P8 exact-contract request membership changed"
            )
        response = self._transport.get(
            f"{self._base_url}/v3/snapshot",
            {
                "ticker.any_of": ",".join(tickers),
                "limit": str(len(tickers)),
                "sort": "ticker",
                "order": "asc",
            },
        )
        if response.status_code != 200:
            raise RuntimeError(
                "P8 Massive exact-contract snapshot failed: "
                f"HTTP {response.status_code}"
            )
        if not 0 < len(response.body) <= self._maximum_decoded_bytes:
            raise ValueError(
                "P8 Massive exact-contract decoded-byte ceiling exceeded"
            )
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                "P8 Massive exact-contract snapshot returned invalid JSON"
            ) from error
        if (
            not isinstance(payload, dict)
            or payload.get("status") not in {"OK", "DELAYED"}
            or payload.get("next_url")
            or not isinstance(payload.get("results"), list)
            or len(payload["results"]) != len(tickers)
        ):
            raise ValueError(
                "P8 Massive exact-contract scope or pagination changed"
            )
        candidates: list[P7TransportCandidate] = []
        snapshots: dict[str, Mapping[str, object]] = {}
        snapshot_clocks: list[datetime] = []
        for row in payload["results"]:
            if not isinstance(row, dict):
                raise TypeError(
                    "P8 exact-contract snapshot row must be an object"
                )
            candidate = _candidate_from_snapshot(
                row,
                session_date=session_date,
            )
            if candidate is None:
                raise ValueError(
                    "P8 exact-contract snapshot row is incomplete"
                )
            candidates.append(candidate)
            snapshots[candidate.contract_ticker] = row
            snapshot_clocks.append(candidate.market_data_time)
        if set(snapshots) != set(tickers):
            raise ValueError(
                "P8 Massive exact-contract response membership changed"
            )
        local_completed_at = (
            reviewed
            if self._clock is None
            else max(
                reviewed,
                _utc(self._clock(), "selection clock"),
            )
        )
        latest_snapshot_at = max(snapshot_clocks)
        if (
            latest_snapshot_at - local_completed_at
        ).total_seconds() > P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS:
            raise ValueError("P8 provider clock skew exceeded")
        selected_at = max(local_completed_at, latest_snapshot_at)
        selection_policy = (
            self._policy
            if self._policy.underlyings == underlyings
            else P7TransportSelectionPolicy(
                version=(
                    "advanced-options-c6-configured-capture-v1"
                ),
                underlyings=underlyings,
                minimum_dte=0,
                maximum_dte=60,
            )
        )
        selection = _select_exact_call_put_contracts(
            tuple(candidates),
            reviewed_at=selected_at,
            policy=selection_policy,
        )
        if (
            set(selection.contracts) != set(tickers)
        ):
            raise RuntimeError(
                "P8 Massive exact-contract response selection changed"
            )
        evidence = {
            "schema": (
                "advanced-options-p8-exact-contract-snapshot-v1"
            ),
            "endpoint": "/v3/snapshot",
            "source_snapshot_at_utc": min(
                snapshot_clocks
            ).isoformat(),
            "selected_at_utc": selected_at.isoformat(),
            "valid_until_utc": selection.valid_until.isoformat(),
            "policy_sha256": selection_policy.sha256,
            "source_snapshot_sha256": canonical_sha256(payload),
            "request_id": payload.get("request_id"),
            "request_count": P8_EXACT_CONTRACT_REQUEST_COUNT,
            "decoded_bytes": len(response.body),
            "pages": P8_EXACT_CONTRACT_REQUEST_COUNT,
            "pagination_requests": 0,
            "contracts": list(tickers),
        }
        evidence["selection_sha256"] = canonical_sha256(evidence)
        return P8ActivitySelectionCapture(
            selection=selection,
            request_count=P8_EXACT_CONTRACT_REQUEST_COUNT,
            decoded_bytes=len(response.body),
            pages=P8_EXACT_CONTRACT_REQUEST_COUNT,
            pagination_requests=0,
            source_snapshot_at=min(snapshot_clocks),
            evidence=evidence,
            selected_snapshots={
                ticker: snapshots[ticker] for ticker in tickers
            },
        )
