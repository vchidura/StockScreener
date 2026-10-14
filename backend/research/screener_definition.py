"""Provider-free screener definitions; compilation never executes a market query."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Literal, Mapping

from pydantic import Field, TypeAdapter, model_validator

from options.discovery import CONTRACT_FILTERS, ContractField, ContractRange, EligibleChainQuery
from research.screening import (
    FIELDS, VERSION, Predicate, Query, StrictModel, digest, predicate_hash,
)


DEFINITION_VERSION = "screener_definition_v1"
COMPILER_VERSION = "screener_compiler_v1"
MAX_DEFINITION_BYTES = 32_768
ScreenDomain = Literal["stock", "option_contract"]


class StockScreenDefinition(StrictModel):
    schema_version: Literal["screener_definition_v1"] = DEFINITION_VERSION
    domain: Literal["stock"]
    catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    predicate: Predicate = Field(default_factory=Predicate)
    sort: str = "ticker"
    descending: bool = False

    def native_query(self) -> Query:
        return Query(predicate=self.predicate, sort=self.sort, descending=self.descending)

    @model_validator(mode="after")
    def supported(self):
        self.native_query()
        return self


class OptionScreenDefinition(StrictModel):
    schema_version: Literal["screener_definition_v1"] = DEFINITION_VERSION
    domain: Literal["option_contract"]
    catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    underlyer: str | None = Field(default=None, max_length=15)
    contract_type: Literal["CALL", "PUT"] | None = None
    filters: tuple[ContractRange, ...] = Field(default=(), max_length=len(CONTRACT_FILTERS))
    sort: ContractField = "day_volume"
    descending: bool = True

    def native_query(self) -> EligibleChainQuery:
        return EligibleChainQuery(underlyer=self.underlyer, contract_type=self.contract_type,
            filters=self.filters, sort=self.sort, descending=self.descending)

    @model_validator(mode="after")
    def supported(self):
        self.native_query()
        return self


ScreenDefinition = Annotated[StockScreenDefinition | OptionScreenDefinition, Field(discriminator="domain")]
_DEFINITION_ADAPTER = TypeAdapter(ScreenDefinition)


@dataclass(frozen=True, slots=True)
class ScreenerCatalog:
    domain: ScreenDomain
    payload_json: str

    @property
    def sha256(self) -> str:
        return digest({"domain": self.domain, "catalog": self.document()})

    def document(self) -> dict:
        return json.loads(self.payload_json)


def build_screener_catalog(domain: ScreenDomain, published_catalog: Mapping) -> ScreenerCatalog:
    if domain == "stock":
        if published_catalog.get("version") != VERSION:
            raise ValueError("unsupported stock catalog version")
        fields = published_catalog["fields"]
        if set(fields) - set(FIELDS):
            raise ValueError("stock catalog contains unsupported fields")
        document = dict(version=VERSION, fields=fields, patterns=published_catalog.get("patterns", {}),
            gaps=published_catalog.get("gaps"), hourly=published_catalog.get("hourly"))
    elif domain == "option_contract":
        fields = published_catalog["contract_filters"]
        if set(fields) - set(CONTRACT_FILTERS):
            raise ValueError("option catalog contains unsupported fields")
        document = dict(version=DEFINITION_VERSION, fields=fields)
    else:
        raise ValueError("unsupported screener domain")
    return ScreenerCatalog(domain, json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False))


def screener_definition_schema() -> dict:
    return _DEFINITION_ADAPTER.json_schema()


def parse_screener_definition(payload: str | Mapping) -> StockScreenDefinition | OptionScreenDefinition:
    serialized = payload if isinstance(payload, str) else json.dumps(payload, allow_nan=False)
    if len(serialized.encode("utf-8")) > MAX_DEFINITION_BYTES:
        raise ValueError("screener definition exceeds byte budget")
    return _DEFINITION_ADAPTER.validate_json(serialized, strict=True)


@dataclass(frozen=True, slots=True)
class CompiledScreenerDefinition:
    domain: ScreenDomain
    definition_sha256: str
    catalog_sha256: str
    request_json: str
    compiler_version: str = COMPILER_VERSION

    def native_query(self) -> Query | EligibleChainQuery:
        contract = Query if self.domain == "stock" else EligibleChainQuery
        return contract.model_validate_json(self.request_json, strict=True)


def compile_screener_definition(
    definition: StockScreenDefinition | OptionScreenDefinition,
    catalog: ScreenerCatalog,
) -> CompiledScreenerDefinition:
    checked = parse_screener_definition(definition.model_dump_json())
    if checked.domain != catalog.domain or checked.catalog_sha256 != catalog.sha256:
        raise ValueError("screener catalog changed; explicit revalidation required")
    document = catalog.document()
    query = checked.native_query()
    if isinstance(checked, StockScreenDefinition):
        requested = {item.field for item in checked.predicate.filters} | ({checked.sort} - {"ticker"})
        if requested - set(document["fields"]):
            raise ValueError("stock fields are unavailable in this catalog")
        if {item.id for item in checked.predicate.patterns} - set(document["patterns"]):
            raise ValueError("stock patterns are unavailable in this catalog")
        for name in ("gap", "hourly"):
            predicate = getattr(checked.predicate, name)
            available = document["gaps" if name == "gap" else name]
            if predicate is not None and (not available or predicate.version != available.get("version")
                    or {item.field for item in predicate.filters} - set(available.get("fields", {}))):
                raise ValueError(f"stock {name} context is unavailable in this catalog")
        rules = predicate_hash(checked.predicate)
    else:
        if ({item.field for item in checked.filters} | {checked.sort}) - set(document["fields"]):
            raise ValueError("option fields are unavailable in this catalog")
        rules = dict(underlyer=checked.underlyer, contract_type=checked.contract_type,
            filters=sorted((item.model_dump() for item in checked.filters), key=lambda item: item["field"]))
    identity = digest(dict(version=DEFINITION_VERSION, compiler_version=COMPILER_VERSION,
        domain=checked.domain, catalog_sha256=catalog.sha256, rules=rules,
        sort=checked.sort, descending=checked.descending))
    return CompiledScreenerDefinition(checked.domain, identity, catalog.sha256, query.model_dump_json())