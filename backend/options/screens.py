"""Options screen definitions (schema options-screen-v1): the foundation for built-in, custom and, later, generated
screens.

A screen is a validated JSON document, not code: a rule tree over the contract fields (comparisons joined by all /
any / not), optional per-tier parameters the rule refers to by name, a sort and caps. The API evaluates the same
definition for every row it serves (published and breadth). A later natural-language layer produces this document and
nothing else, so it is checked by the same schema before it runs (docs/OPTIONS_SCREENS_FUTURE_DESIGN.md).

Built-in screens live in `options/policies/option_screens_v1.json`; each is identified by its id, version and the hash
of its canonical document.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Literal, Mapping, Sequence, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from options.discovery import CONTRACT_FILTERS

SCREEN_SCHEMA_VERSION = "options-screen-v1"
BUILTIN_SCREENS_PATH = Path(__file__).resolve().parent / "policies" / "option_screens_v1.json"
# Fields a rule may read: the published contract fields plus the values the evaluator derives for every row.
DERIVED_FIELDS = {
    "day_premium": "Day volume x option mark x 100 (USD); unavailable without both",
}
SCREEN_FIELDS = tuple(sorted(set(CONTRACT_FILTERS) | set(DERIVED_FIELDS)))
ScreenField = Literal[SCREEN_FIELDS]  # type: ignore[valid-type]
ScreenOperator = Literal[">=", ">", "<=", "<", "==", "!=", "in", "not_in"]
_ID = r"^[a-z0-9][a-z0-9-]{0,63}$"
_PARAMETER = r"^[a-z][a-z0-9_]{0,47}$"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Comparison(_Strict):
    field: ScreenField
    op: ScreenOperator
    value: float | tuple[float, ...] | None = None
    parameter: str | None = Field(default=None, pattern=_PARAMETER)

    @model_validator(mode="after")
    def one_operand(self):
        if (self.value is None) == (self.parameter is None):
            raise ValueError("a comparison takes exactly one of value or parameter")
        if self.op in ("in", "not_in") and not isinstance(self.value, tuple):
            raise ValueError("in / not_in compare against a list of values")
        if self.op not in ("in", "not_in") and isinstance(self.value, tuple):
            raise ValueError("only in / not_in take a list")
        return self


class AllOf(_Strict):
    all: tuple["Rule", ...] = Field(min_length=1, max_length=32)


class AnyOf(_Strict):
    any: tuple["Rule", ...] = Field(min_length=1, max_length=32)


class NotOf(_Strict):
    not_: "Rule" = Field(alias="not")
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


Rule = Union[Comparison, AllOf, AnyOf, NotOf]
AllOf.model_rebuild()
AnyOf.model_rebuild()
NotOf.model_rebuild()


class TierParameters(_Strict):
    """Per-tier values a rule refers to by name; an underlying outside the tier map is excluded, never guessed."""
    underlying_tiers: dict[str, str] = Field(min_length=1)
    values: dict[str, dict[str, float]] = Field(min_length=1)
    untiered: Literal["EXCLUDE"] = "EXCLUDE"

    @model_validator(mode="after")
    def complete(self):
        tiers = set(self.underlying_tiers.values())
        if tiers - set(self.values):
            raise ValueError(f"tiers without values: {sorted(tiers - set(self.values))}")
        names = {frozenset(values) for values in self.values.values()}
        if len(names) != 1:
            raise ValueError("every tier must define the same parameters")
        return self


class Caps(_Strict):
    per_underlying: int | None = Field(default=None, ge=1, le=500)
    total: int | None = Field(default=None, ge=1, le=500)


class ScreenDefinition(_Strict):
    schema_version: Literal["options-screen-v1"]
    id: str = Field(pattern=_ID)
    version: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=80)
    summary: str = Field(min_length=1, max_length=400)
    contract_type: Literal["CALL", "PUT"] | None = None
    rule: Rule
    parameters: TierParameters | None = None
    sort: ScreenField = "day_volume"
    descending: bool = True
    caps: Caps = Caps()
    source: str = Field(default="", max_length=200)

    @model_validator(mode="after")
    def parameters_resolve(self):
        named = set(_parameters_of(self.rule))
        available = set(next(iter(self.parameters.values.values()))) if self.parameters else set()
        if named - available:
            raise ValueError(f"rule refers to undefined parameters: {sorted(named - available)}")
        return self

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def _parameters_of(rule) -> list[str]:
    if isinstance(rule, Comparison):
        return [] if rule.parameter is None else [rule.parameter]
    if isinstance(rule, NotOf):
        return _parameters_of(rule.not_)
    return [name for child in (rule.all if isinstance(rule, AllOf) else rule.any) for name in _parameters_of(child)]


def _number(value) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def derive_screen_fields(row: Mapping[str, object]) -> dict[str, object]:
    """The derived values of one row (added to the row so they can be shown, sorted and filtered)."""
    volume = _number(row.get("day_volume"))
    mark = _number(row.get("mark")) or _number(row.get("model_mark"))
    premium = volume * mark * 100 if volume is not None and mark is not None and mark > 0 else None
    return {"day_premium": None if premium is None else format(premium.quantize(Decimal("0.01")), "f")}


def _holds(rule, row: Mapping[str, object], parameters: Mapping[str, float]) -> bool:
    if isinstance(rule, AllOf):
        return all(_holds(child, row, parameters) for child in rule.all)
    if isinstance(rule, AnyOf):
        return any(_holds(child, row, parameters) for child in rule.any)
    if isinstance(rule, NotOf):
        return not _holds(rule.not_, row, parameters)
    value = _number(row.get(rule.field))
    if value is None:
        return False  # a missing value never satisfies a comparison (and its negation reads as not satisfied too)
    if rule.op in ("in", "not_in"):
        member = value in {Decimal(str(item)) for item in rule.value}
        return member if rule.op == "in" else not member
    operand = Decimal(str(rule.value if rule.parameter is None else parameters[rule.parameter]))
    return {
        ">=": value >= operand, ">": value > operand, "<=": value <= operand, "<": value < operand,
        "==": value == operand, "!=": value != operand,
    }[rule.op]


def evaluate_screen(
    definition: ScreenDefinition, rows: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """The rows the screen selects, in its order and within its caps, and what it did (for the page's notes)."""
    selected, untiered = [], set()
    for row in rows:
        if definition.contract_type and row.get("contract_type") != definition.contract_type:
            continue
        underlying = str(row.get("underlying"))
        parameters: Mapping[str, float] = {}
        if definition.parameters is not None:
            tier = definition.parameters.underlying_tiers.get(underlying)
            if tier is None:
                untiered.add(underlying)
                continue
            parameters = definition.parameters.values[tier]
        enriched = {**row, **derive_screen_fields(row)}
        if definition.parameters is not None:
            enriched["screen_tier"] = definition.parameters.underlying_tiers[underlying]
        if _holds(definition.rule, enriched, parameters):
            selected.append(enriched)
    present = [row for row in selected if _number(row.get(definition.sort)) is not None]
    absent = [row for row in selected if _number(row.get(definition.sort)) is None]
    present.sort(key=lambda row: (_number(row[definition.sort]), str(row.get("contract_ticker"))),
                 reverse=definition.descending)
    ordered, per_underlying = [], {}
    for row in present + absent:
        count = per_underlying.get(row["underlying"], 0)
        if definition.caps.per_underlying is not None and count >= definition.caps.per_underlying:
            continue
        if definition.caps.total is not None and len(ordered) >= definition.caps.total:
            break
        per_underlying[row["underlying"]] = count + 1
        ordered.append(row)
    return ordered, {
        "id": definition.id, "version": definition.version, "sha256": definition.sha256,
        "matched": len(selected), "shown": len(ordered), "untiered_underlyings": sorted(untiered),
        "caps": definition.caps.model_dump(),
    }


@lru_cache(maxsize=1)
def builtin_screens() -> dict[str, ScreenDefinition]:
    document = json.loads(BUILTIN_SCREENS_PATH.read_text(encoding="utf-8"))
    screens = [ScreenDefinition.model_validate(item) for item in document["screens"]]
    if len({screen.id for screen in screens}) != len(screens):
        raise ValueError("built-in screen ids must be unique")
    return {screen.id: screen for screen in screens}


def rule_labels(rule, parameters: TierParameters | None = None) -> list[str]:
    """Plain lines describing a rule (the page shows them; per-tier values are spelled out)."""
    def label(node) -> str:
        if isinstance(node, Comparison):
            field = CONTRACT_FILTERS.get(node.field, {}).get("label") or node.field.replace("_", " ")
            if node.parameter is not None and parameters is not None:
                values = ", ".join(
                    f"{tier} {values[node.parameter]:g}" for tier, values in sorted(parameters.values.items())
                )
                return f"{field} {node.op} {values}"
            if isinstance(node.value, tuple):
                return f"{field} {'in' if node.op == 'in' else 'not in'} {', '.join(f'{v:g}' for v in node.value)}"
            return f"{field} {node.op} {node.value:g}"
        if isinstance(node, NotOf):
            return f"not ({label(node.not_)})"
        joiner = " and " if isinstance(node, AllOf) else " or "
        return joiner.join(label(child) for child in (node.all if isinstance(node, AllOf) else node.any))
    if isinstance(rule, AllOf):
        return [label(child) for child in rule.all]
    return [label(rule)]


def builtin_screen_catalog() -> list[dict[str, object]]:
    return [
        {
            "id": screen.id, "version": screen.version, "sha256": screen.sha256, "name": screen.name,
            "summary": screen.summary, "contract_type": screen.contract_type, "sort": screen.sort,
            "descending": screen.descending, "caps": screen.caps.model_dump(),
            "rules": rule_labels(screen.rule, screen.parameters),
            "tiers": None if screen.parameters is None else screen.parameters.underlying_tiers,
            "source": screen.source,
        }
        for screen in builtin_screens().values()
    ]
