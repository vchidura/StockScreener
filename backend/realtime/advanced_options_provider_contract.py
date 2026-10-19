"""Documentation-pinned Massive Options fields and exchange reference contract."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .contracts import canonical_sha256


PROVIDER_CONTRACT_VERSION = "advanced-options-provider-contract-v1"
EXCHANGE_REFERENCE_VERSION = "advanced-options-exchanges-v1"
PROVENANCE_LIVE_CAPTURE = "LIVE_CAPTURED"
PROVENANCE_SYNTHETIC = "SYNTHETIC_SCHEMA_FIXTURE_NOT_LIVE_CAPTURED"
_PROVENANCES = frozenset({
    PROVENANCE_LIVE_CAPTURE,
    PROVENANCE_SYNTHETIC,
})
_TRADE_FIELDS = ("sym", "x", "p", "s", "c", "t", "q")
_QUOTE_FIELDS = (
    "sym",
    "bx",
    "ax",
    "bp",
    "ap",
    "bs",
    "as",
    "t",
    "q",
)
_CONDITION_RESPONSE_FIELDS = (
    "abbreviation",
    "asset_class",
    "data_types",
    "description",
    "exchange",
    "id",
    "legacy",
    "name",
    "sip_mapping",
    "type",
    "update_rules",
)
_EXCHANGE_RESPONSE_FIELDS = (
    "acronym",
    "asset_class",
    "id",
    "locale",
    "mic",
    "name",
    "operating_mic",
    "participant_id",
    "type",
    "url",
)
REFERENCE_DIRECTORY = (
    Path(__file__).resolve().parent / "policies" / "references"
)
DEFAULT_OPTIONS_EXCHANGE_REFERENCE_PATH = (
    REFERENCE_DIRECTORY / "massive_options_exchanges_20261019.json"
)


def _text(value: object, name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty text")
    return value.strip()


def _sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("fetched_at must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("fetched_at must be an ISO timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("fetched_at must be timezone-aware")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProviderContract:
    version: str
    provider: str
    asset_class: str
    locale: str
    trade_event: str
    trade_fields: tuple[str, ...]
    quote_event: str
    quote_fields: tuple[str, ...]
    condition_endpoint: str
    condition_parameters: tuple[tuple[str, str], ...]
    condition_response_fields: tuple[str, ...]
    exchange_endpoint: str
    exchange_parameters: tuple[tuple[str, str], ...]
    exchange_response_fields: tuple[str, ...]
    trade_documentation_url: str
    quote_documentation_url: str
    forbidden_dependencies: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            self.version != PROVIDER_CONTRACT_VERSION
            or self.provider != "massive"
            or self.asset_class != "options"
            or self.locale != "us"
            or self.trade_event != "T"
            or self.trade_fields != _TRADE_FIELDS
            or self.quote_event != "Q"
            or self.quote_fields != _QUOTE_FIELDS
            or self.condition_endpoint != "/v3/reference/conditions"
            or self.condition_parameters
            != (("asset_class", "options"), ("data_type", "trade"))
            or self.condition_response_fields
            != _CONDITION_RESPONSE_FIELDS
            or self.exchange_endpoint != "/v3/reference/exchanges"
            or self.exchange_parameters
            != (("asset_class", "options"), ("locale", "us"))
            or self.exchange_response_fields != _EXCHANGE_RESPONSE_FIELDS
            or self.trade_documentation_url
            != "https://massive.com/docs/websocket/options/trades"
            or self.quote_documentation_url
            != "https://massive.com/docs/websocket/options/quotes"
            or self.forbidden_dependencies != ("m",)
        ):
            raise ValueError("Advanced Options provider contract changed")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def fields_for_event(self, event: str) -> tuple[str, ...]:
        if event == self.trade_event:
            return self.trade_fields
        if event == self.quote_event:
            return self.quote_fields
        raise ValueError("unsupported Massive Options event")

    def recognized_payload(
        self,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        event = payload.get("ev")
        fields = self.fields_for_event(str(event))
        missing = tuple(name for name in fields if name not in payload)
        if missing:
            raise ValueError(
                "Massive Options payload is missing pinned fields: "
                + ",".join(missing)
            )
        return {"ev": event, **{name: payload[name] for name in fields}}


MASSIVE_OPTIONS_PROVIDER_CONTRACT = AdvancedOptionsProviderContract(
    version=PROVIDER_CONTRACT_VERSION,
    provider="massive",
    asset_class="options",
    locale="us",
    trade_event="T",
    trade_fields=_TRADE_FIELDS,
    quote_event="Q",
    quote_fields=_QUOTE_FIELDS,
    condition_endpoint="/v3/reference/conditions",
    condition_parameters=(("asset_class", "options"), ("data_type", "trade")),
    condition_response_fields=_CONDITION_RESPONSE_FIELDS,
    exchange_endpoint="/v3/reference/exchanges",
    exchange_parameters=(("asset_class", "options"), ("locale", "us")),
    exchange_response_fields=_EXCHANGE_RESPONSE_FIELDS,
    trade_documentation_url=(
        "https://massive.com/docs/websocket/options/trades"
    ),
    quote_documentation_url=(
        "https://massive.com/docs/websocket/options/quotes"
    ),
    forbidden_dependencies=("m",),
)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsExchange:
    exchange_id: int
    acronym: str | None
    asset_class: str
    locale: str
    mic: str | None
    name: str
    operating_mic: str | None
    participant_id: str | None
    exchange_type: str
    url: str | None

    def __post_init__(self) -> None:
        if type(self.exchange_id) is not int or self.exchange_id < 0:
            raise ValueError("exchange id must be a nonnegative integer")
        if self.asset_class != "options" or self.locale != "us":
            raise ValueError("exchange is outside the reviewed Options US scope")
        for name in ("acronym", "mic", "operating_mic", "participant_id", "url"):
            object.__setattr__(
                self,
                name,
                _text(getattr(self, name), name, optional=True),
            )
        object.__setattr__(self, "name", _text(self.name, "name"))
        object.__setattr__(
            self,
            "exchange_type",
            _text(self.exchange_type, "exchange_type"),
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsExchangeReference:
    version: str
    provider_contract_sha256: str
    provenance: str
    fetched_at: datetime
    payload_sha256: str
    exchanges: tuple[AdvancedOptionsExchange, ...]

    def __post_init__(self) -> None:
        rows = tuple(sorted(self.exchanges, key=lambda row: row.exchange_id))
        ids = tuple(row.exchange_id for row in rows)
        if (
            self.version != EXCHANGE_REFERENCE_VERSION
            or self.provenance not in _PROVENANCES
            or not isinstance(self.fetched_at, datetime)
            or self.fetched_at.tzinfo is None
            or self.fetched_at.utcoffset() is None
            or not rows
            or len(ids) != len(set(ids))
        ):
            raise ValueError("Advanced Options exchange reference is invalid")
        object.__setattr__(
            self,
            "provider_contract_sha256",
            _sha256(
                self.provider_contract_sha256,
                "provider_contract_sha256",
            ),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )
        object.__setattr__(
            self,
            "fetched_at",
            self.fetched_at.astimezone(timezone.utc),
        )
        object.__setattr__(self, "exchanges", rows)

    @property
    def is_live_capture(self) -> bool:
        return self.provenance == PROVENANCE_LIVE_CAPTURE

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def entry_for(self, exchange_id: int) -> AdvancedOptionsExchange:
        for row in self.exchanges:
            if row.exchange_id == exchange_id:
                return row
        raise ValueError("exchange id is outside the pinned reference")


def parse_exchange_reference(
    document: Mapping[str, object],
    *,
    provider_contract: AdvancedOptionsProviderContract = (
        MASSIVE_OPTIONS_PROVIDER_CONTRACT
    ),
) -> AdvancedOptionsExchangeReference:
    if not isinstance(provider_contract, AdvancedOptionsProviderContract):
        raise TypeError("provider contract is invalid")
    provenance = document.get("provenance")
    if provenance not in _PROVENANCES:
        raise ValueError("exchange reference provenance is invalid")
    request = document.get("request")
    if request != {"asset_class": "options", "locale": "us"}:
        raise ValueError("exchange reference request scope changed")
    raw_results = document.get("results")
    if not isinstance(raw_results, list) or not raw_results:
        raise ValueError("exchange reference results are missing")
    allowed = frozenset(provider_contract.exchange_response_fields)
    required = frozenset({
        "asset_class",
        "id",
        "locale",
        "name",
        "type",
    })
    rows: list[AdvancedOptionsExchange] = []
    canonical_results: list[dict[str, object]] = []
    for raw in raw_results:
        if not isinstance(raw, Mapping):
            raise ValueError("exchange reference row must be an object")
        keys = frozenset(raw)
        if keys - allowed or required - keys:
            raise ValueError("exchange reference row fields changed")
        normalized = {name: raw.get(name) for name in sorted(allowed)}
        rows.append(AdvancedOptionsExchange(
            exchange_id=raw["id"],
            acronym=raw.get("acronym"),
            asset_class=raw["asset_class"],
            locale=raw["locale"],
            mic=raw.get("mic"),
            name=raw["name"],
            operating_mic=raw.get("operating_mic"),
            participant_id=raw.get("participant_id"),
            exchange_type=raw["type"],
            url=raw.get("url"),
        ))
        canonical_results.append(normalized)
    payload_sha256 = canonical_sha256({
        "request": request,
        "results": sorted(
            canonical_results,
            key=lambda row: row["id"],
        ),
    })
    expected = document.get("payload_sha256")
    if expected is not None and expected != payload_sha256:
        raise ValueError("exchange reference payload hash changed")
    return AdvancedOptionsExchangeReference(
        version=EXCHANGE_REFERENCE_VERSION,
        provider_contract_sha256=provider_contract.sha256,
        provenance=provenance,
        fetched_at=_timestamp(document.get("fetched_at")),
        payload_sha256=payload_sha256,
        exchanges=tuple(rows),
    )


def load_default_options_exchange_reference() -> AdvancedOptionsExchangeReference:
    import json

    return parse_exchange_reference(json.loads(
        DEFAULT_OPTIONS_EXCHANGE_REFERENCE_PATH.read_text(encoding="utf-8")
    ))
