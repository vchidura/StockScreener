"""Bounded Advanced-only Massive activity selection for one P8 canary."""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol
from urllib.parse import quote

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
P8_SELECTION_REQUEST_COUNT = 2
P8_SELECTION_MAXIMUM_DECODED_BYTES = 100_000_000
P8_SELECTION_MAXIMUM_RESULTS_PER_UNDERLYING = 250
P8_SELECTION_MAXIMUM_PROVIDER_CLOCK_SKEW_SECONDS = 5
P8_SELECTION_POLICY = P7TransportSelectionPolicy(
    version="advanced-options-p8-same-day-activity-v1",
    maximum_dte=0,
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
            or self.request_count != P8_SELECTION_REQUEST_COUNT
            or not 0 < self.decoded_bytes
            <= P8_SELECTION_MAXIMUM_DECODED_BYTES
            or self.pages != P8_SELECTION_REQUEST_COUNT
            or self.pagination_requests != 0
            or set(self.selected_snapshots) != set(self.selection.contracts)
        ):
            raise ValueError("P8 activity selection capture is incomplete")
        object.__setattr__(
            self,
            "source_snapshot_at",
            _utc(self.source_snapshot_at, "source_snapshot_at"),
        )


def _candidate_from_snapshot(
    row: Mapping[str, object],
    *,
    session_date: date,
) -> P7TransportCandidate | None:
    details = row.get("details")
    quote_row = row.get("last_quote")
    day_row = row.get("day")
    underlying = row.get("underlying_asset")
    if not all(
        isinstance(value, Mapping)
        for value in (details, quote_row, day_row, underlying)
    ):
        return None
    ticker = str(details.get("ticker", "")).strip().upper()
    option_type = {
        "call": "C",
        "put": "P",
    }.get(str(details.get("contract_type", "")).strip().lower())
    if option_type is None:
        return None
    expiration = date.fromisoformat(str(details["expiration_date"]))
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
        bid=_decimal(quote_row["bid"], "last_quote.bid"),
        ask=_decimal(quote_row["ask"], "last_quote.ask"),
        day_volume=int(day_row.get("volume", 0)),
        open_interest=int(row.get("open_interest", 0)),
        calendar_dte=(expiration - session_date).days,
        market_data_time=_nanoseconds(
            quote_row.get("last_updated"),
            "last_quote.last_updated",
        ),
        source_sha256=canonical_sha256(row),
    )


def p8_activity_selection_capture_from_sealed(
    value: Mapping[str, object],
) -> P8ActivitySelectionCapture:
    if (
        value.get("contract")
        != "advanced-options-v3-pre-session-capture-v1"
        or value.get("provider_requests") != P8_SELECTION_REQUEST_COUNT
        or type(value.get("decoded_bytes")) is not int
        or not 0 < value["decoded_bytes"]
        <= P8_SELECTION_MAXIMUM_DECODED_BYTES
        or not isinstance(value.get("responses"), list)
        or len(value["responses"]) != P8_SELECTION_REQUEST_COUNT
        or not isinstance(value.get("selected_snapshots"), Mapping)
        or len(value["selected_snapshots"]) != 4
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
        or tuple(row.get("underlyer") for row in responses)
        != P8_SELECTION_POLICY.underlyings
    ):
        raise ValueError("sealed P8 response membership changed")
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
    if pairs != {
        ("QQQ", "C"),
        ("QQQ", "P"),
        ("SPY", "C"),
        ("SPY", "P"),
    }:
        raise ValueError("sealed P8 call/put membership changed")
    for option_type in ("C", "P"):
        selection = select_p7_transport_contracts(
            tuple(
                row
                for row in candidates
                if row.option_type == option_type
            ),
            reviewed_at=reviewed_at,
            policy=P8_SELECTION_POLICY,
        )
        if selection.status is not P7TransportSelectionStatus.READY:
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
            seconds=P8_SELECTION_POLICY.maximum_snapshot_age_seconds
        )
        for row in selected
    )
    selection_sha256 = canonical_sha256({
        "status": P7TransportSelectionStatus.READY,
        "policy_sha256": P8_SELECTION_POLICY.sha256,
        "reviewed_at": reviewed_at,
        "valid_until": valid_until,
        "source_sha256": source_sha256,
        "selected": selected,
        "reason_codes": (),
    })
    selection = P7TransportSelection(
        P7TransportSelectionStatus.READY,
        P8_SELECTION_POLICY,
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
        "source_snapshot_at_utc": source_snapshot_at.isoformat(),
        "selected_at_utc": reviewed_at.isoformat(),
        "valid_until_utc": valid_until.isoformat(),
        "policy_sha256": P8_SELECTION_POLICY.sha256,
        "source_snapshot_sha256": canonical_sha256(responses),
        "request_count": P8_SELECTION_REQUEST_COUNT,
        "decoded_bytes": value["decoded_bytes"],
        "pages": P8_SELECTION_REQUEST_COUNT,
        "pagination_requests": 0,
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
        request_count=P8_SELECTION_REQUEST_COUNT,
        decoded_bytes=value["decoded_bytes"],
        pages=P8_SELECTION_REQUEST_COUNT,
        pagination_requests=0,
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
        self._transport = transport
        self._spots = spots
        self._policy = policy or P8_SELECTION_POLICY
        self._base_url = normalized_base
        self._clock = clock

    def _request(
        self,
        underlyer: str,
        *,
        spot: Decimal,
        session_date: date,
    ) -> tuple[
        tuple[P7TransportCandidate, ...],
        int,
        datetime,
        dict,
        dict[str, Mapping[str, object]],
    ]:
        policy = self._policy
        lower = spot * (Decimal("1") - policy.maximum_moneyness_fraction)
        upper = spot * (Decimal("1") + policy.maximum_moneyness_fraction)
        response = self._transport.get(
            (
                f"{self._base_url}/v3/snapshot/options/"
                f"{quote(underlyer, safe='')}"
            ),
            {
                "expiration_date.gte": (
                    session_date
                    + timedelta(days=policy.minimum_dte)
                ).isoformat(),
                "expiration_date.lte": (
                    session_date
                    + timedelta(days=policy.maximum_dte)
                ).isoformat(),
                "strike_price.gte": str(lower),
                "strike_price.lte": str(upper),
                "limit": str(P8_SELECTION_MAXIMUM_RESULTS_PER_UNDERLYING),
                "sort": "ticker",
                "order": "asc",
            },
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"P8 Massive selection failed for {underlyer}: "
                f"HTTP {response.status_code}"
            )
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                f"P8 Massive selection returned invalid JSON for {underlyer}"
            ) from error
        if (
            not isinstance(payload, dict)
            or payload.get("status") not in {"OK", "DELAYED"}
            or payload.get("next_url")
            or not isinstance(payload.get("results"), list)
            or len(payload["results"])
            > P8_SELECTION_MAXIMUM_RESULTS_PER_UNDERLYING
        ):
            raise ValueError(
                f"P8 Massive selection scope or pagination changed for {underlyer}"
            )
        candidates: list[P7TransportCandidate] = []
        snapshots: dict[str, Mapping[str, object]] = {}
        snapshot_clocks: list[datetime] = []
        for row in payload["results"]:
            if not isinstance(row, dict):
                raise TypeError("P8 option snapshot row must be an object")
            candidate = _candidate_from_snapshot(
                row,
                session_date=session_date,
            )
            if candidate is None:
                continue
            snapshot_clocks.append(candidate.market_data_time)
            candidates.append(candidate)
            snapshots[candidate.contract_ticker] = row
        if not candidates or not snapshot_clocks:
            raise ValueError(
                f"P8 Massive selection has no usable contracts for {underlyer}"
            )
        return (
            tuple(candidates),
            len(response.body),
            max(snapshot_clocks),
            payload,
            snapshots,
        )

    def capture(
        self,
        *,
        reviewed_at: datetime,
        session_date: date,
    ) -> P8ActivitySelectionCapture:
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
        for underlyer in self._policy.underlyings:
            rows, byte_count, snapshot_at, payload, raw_snapshots = self._request(
                underlyer,
                spot=_decimal(spots[underlyer], f"{underlyer} spot"),
                session_date=session_date,
            )
            candidates.extend(rows)
            decoded_bytes += byte_count
            snapshot_clocks.append(snapshot_at)
            responses.append({
                "underlyer": underlyer,
                "request_id": payload.get("request_id"),
                "response_sha256": canonical_sha256(payload),
                "row_count": len(payload["results"]),
            })
            snapshots.update(raw_snapshots)
            if decoded_bytes > P8_SELECTION_MAXIMUM_DECODED_BYTES:
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
            "schema": "advanced-options-p8-activity-selection-v1",
            "source_snapshot_at_utc": source_snapshot_at.isoformat(),
            "selected_at_utc": selected_at.isoformat(),
            "valid_until_utc": selection.valid_until.isoformat(),
            "policy_sha256": self._policy.sha256,
            "source_snapshot_sha256": canonical_sha256(responses),
            "request_count": P8_SELECTION_REQUEST_COUNT,
            "decoded_bytes": decoded_bytes,
            "pages": P8_SELECTION_REQUEST_COUNT,
            "pagination_requests": 0,
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
            request_count=P8_SELECTION_REQUEST_COUNT,
            decoded_bytes=decoded_bytes,
            pages=P8_SELECTION_REQUEST_COUNT,
            pagination_requests=0,
            source_snapshot_at=source_snapshot_at,
            evidence=evidence,
            selected_snapshots={
                row.contract_ticker: snapshots[row.contract_ticker]
                for row in selection.selected
            },
        )
