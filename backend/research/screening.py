"""Versioned, pure daily screening contracts. No database or detector imports."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


VERSION = "stock_screener_workspace_v1"
UNIVERSE = "CONFIGURED_TRACKED_EQUITIES"
STATE_FIELDS = {"discovery_state", "discovery_trend"}
SHORT_MOMENTUM_FIELDS = {"momentum_6_1", "momentum_3_1"}
FIELD_SET_VERSION = "screening_fields_v3_momentum_horizons"
GAP_VERSION = "screening_daily_gap_context_v1"
GAP_MAX_AGE = 20
GAP_WINDOW = GAP_MAX_AGE + 2
HOURLY_VERSION = "screening_hourly_context_v1"


def field(label, group, unit, warmup, *, kind="number", options=None, source="canonical_daily_bars"):
    return dict(label=label, group=group, unit=unit, warmup_sessions=warmup, type=kind,
                options=options or [], operators=["range"] if kind == "number" else ["in"],
                source=source, interval="1d", price_basis="UNADJUSTED_ACTION_GATED",
                freshness="EXACT_COMPLETED_SESSION", versions=[VERSION])


FIELDS = {
    "instrument_type": field("Instrument type", "Universe", "category", 0, kind="category",
                             options=["CS", "ETF", "ETV"], source="dated_security_reference"),
    "price": field("Close", "Universe", "USD", 1),
    "volume": field("Session volume", "Activity", "shares", 1),
    "change": field("Session change", "Activity", "fraction", 2),
    "dollar_volume_20": field("Prior 20-session average dollar volume", "Activity", "USD/day", 21),
    "relative_volume_20": field("Volume / prior 20-session mean", "Activity", "ratio", 21),
    "vs_ema20": field("Close / EMA20 - 1", "Trend / momentum", "fraction", 20),
    "vs_ema50": field("Close / EMA50 - 1", "Trend / momentum", "fraction", 50),
    "vs_sma200": field("Close / SMA200 - 1", "Trend / momentum", "fraction", 200),
    "momentum_12_1": field("12-1 momentum", "Trend / momentum", "fraction", 253),
    "momentum_6_1": field("6-1 momentum", "Trend / momentum", "fraction", 127),
    "momentum_3_1": field("3-1 momentum", "Trend / momentum", "fraction", 64),
    "momentum_percentile": field("12-1 universe percentile", "Trend / momentum", "fraction", 253),
    "realized_volatility_21": field("21-session annualized realized volatility", "Volatility / structure", "fraction", 22),
    "vs_prior_high20": field("Close / prior 20-session high - 1", "Volatility / structure", "fraction", 21),
    "discovery_state": field("Daily discovery state", "Patterns / setups", "category", 253, kind="category",
                             options=["PULLBACK", "BOUNCE", "RESUMING_UP", "RESUMING_DOWN", "TRENDING", "MIXED"],
                             source="stock_discovery_v1.stock_features"),
    "discovery_trend": field("Daily discovery trend", "Trend / momentum", "category", 253, kind="category",
                             options=["UP", "DOWN", "MIXED"], source="stock_discovery_v1.stock_features"),
}
for name in STATE_FIELDS:
    FIELDS[name].update(introduced_in="screening_fields_v2_discovery_state", eligibility="253_VALID_SESSIONS_PRICE_5_MEDIAN_DOLLAR_VOLUME_20M",
                        interpretation="COMPLETED_SESSION_STATE_NOT_LIFECYCLE_OR_ALERT")
for name in SHORT_MOMENTUM_FIELDS:
    FIELDS[name].update(introduced_in=FIELD_SET_VERSION, skip_sessions=21,
                        lookback_sessions=FIELDS[name]["warmup_sessions"] - 1)
PATTERNS = {
    "bullish_engulfing": dict(label="Bullish engulfing", direction=1),
    "bearish_engulfing": dict(label="Bearish engulfing", direction=-1),
    "shooting_star": dict(label="Shooting star", direction=-1),
    "hammer": dict(label="Hammer", direction=1),
    "doji": dict(label="Doji", direction=0),
}
for spec in PATTERNS.values():
    spec.update(interval="1d", state="OCCURRENCE", warmup_sessions=2,
                source="equity.technicals.detect_setup_candlesticks", version=VERSION)

GAP_FIELDS = {
    "direction": field("Gap direction", "Daily gap context", "category", GAP_WINDOW, kind="category", options=["UP", "DOWN"]),
    "state": field("Gap fill state", "Daily gap context", "category", GAP_WINDOW, kind="category", options=["OPEN", "PARTIALLY_FILLED", "FILLED", "SAME_SESSION_FADE", "FAILED"]),
    "formation_age": field("Formation age", "Daily gap context", "sessions", GAP_WINDOW),
    "fill_fraction": field("Opening gap filled", "Daily gap context", "fraction", GAP_WINDOW),
    "price_location": field("Price vs gap zone", "Daily gap context", "category", GAP_WINDOW, kind="category", options=["ABOVE", "INSIDE", "BELOW"]),
    "distance_fraction": field("Distance to nearest zone edge", "Daily gap context", "fraction", GAP_WINDOW),
}
for spec in GAP_FIELDS.values():
    spec["source"] = GAP_VERSION
GAP_CATALOG = dict(version=GAP_VERSION, interval="1d", max_formation_age=GAP_MAX_AGE,
    warmup_sessions=GAP_WINDOW, formation_threshold=.01, fields=GAP_FIELDS,
    formation_rule="OPEN_BEYOND_PREVIOUS_HIGH_OR_LOW", fill_basis="OPEN_TO_PREVIOUS_CLOSE",
    zone_policy="FORMATION_RANGE_GAP_ELSE_OPEN_TO_PREVIOUS_CLOSE", coverage="ALL_FORMATIONS_IN_BOUNDED_WINDOW",
    selection="ANY_SINGLE_EPISODE_ALL_CONDITIONS", representative="NEAREST_MATCHING_EDGE_THEN_YOUNGEST_THEN_ID")

HOURLY_FIELDS = {
    "close": field("1h close", "Hourly context", "USD", 1),
    "change": field("1h last-bar change", "Hourly context", "fraction", 2),
    "vs_ema20": field("1h close / EMA20 - 1", "Hourly context", "fraction", 20),
    "ema20_change_3": field("1h EMA20 change over 3 bars", "Hourly context", "fraction", 23),
}
for spec in HOURLY_FIELDS.values():
    spec.update(interval="1h", source=HOURLY_VERSION, freshness="EXACT_COMPLETED_HOURLY_SLOT")
HOURLY_CATALOG = dict(version=HOURLY_VERSION, interval="1h", fields=HOURLY_FIELDS,
    session_scope="RTH", slot_policy="SESSION_OPEN_ANCHORED_INCLUDING_SHORT_FINAL_BAR",
    ema_policy="EXACT_WINDOW_SEEDED_EWM_ADJUST_FALSE", maximum_bars=23,
    capture="AT_DAILY_SNAPSHOT_CUTOFF_NOT_LIVE", comparison="NOT_ENABLED",
    missing_history_policy="RECONSTRUCT_COMPLETE_RETAINED_30M_ONLY_NEVER_LATEST_SLOT")

HOURLY_REFRESH_POLICY = "screening_hourly_refresh_v1"
REFRESHED_HOURLY_CATALOG = dict(HOURLY_CATALOG, capture="LATEST_COMPLETED_HOURLY_WITH_FROZEN_DAILY",
                              refresh_policy=HOURLY_REFRESH_POLICY)


def supported_hourly_contract(contract):
    return contract == HOURLY_CATALOG or contract == REFRESHED_HOURLY_CATALOG


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Filter(StrictModel):
    field: str = Field(max_length=64)
    min: float | None = None
    max: float | None = None
    values: list[str] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def valid_filter(self):
        spec = FIELDS.get(self.field)
        if spec is None:
            raise ValueError("Unsupported screening field")
        if spec["type"] == "number":
            if self.values or (self.min is None and self.max is None):
                raise ValueError("Numeric filter requires min or max, without values")
            if self.min is not None and self.max is not None and self.min > self.max:
                raise ValueError("Minimum must not exceed maximum")
        elif self.min is not None or self.max is not None or not self.values or any(value not in spec["options"] for value in self.values):
            raise ValueError("Unsupported category or numeric bounds on category")
        return self


class PatternFilter(StrictModel):
    id: str = Field(max_length=64)
    interval: Literal["1d"] = "1d"
    direction: Literal[-1, 0, 1]
    state: Literal["OCCURRENCE"] = "OCCURRENCE"

    @model_validator(mode="after")
    def supported(self):
        spec = PATTERNS.get(self.id)
        if spec is None or self.direction != spec["direction"]:
            raise ValueError("Unsupported pattern or direction")
        return self


class GapCondition(StrictModel):
    field: str = Field(max_length=64)
    min: float | None = None
    max: float | None = None
    values: list[str] = Field(default_factory=list, max_length=5)

    @model_validator(mode="after")
    def supported(self):
        spec = GAP_FIELDS.get(self.field)
        if spec is None:
            raise ValueError("Unsupported gap field")
        if spec["type"] == "category":
            if self.min is not None or self.max is not None or not self.values or any(value not in spec["options"] for value in self.values):
                raise ValueError("Unsupported gap category")
        else:
            if self.values or self.min is None and self.max is None:
                raise ValueError("Gap numeric condition requires bounds")
            if self.min is not None and self.max is not None and self.min > self.max:
                raise ValueError("Gap minimum must not exceed maximum")
            for bound in (self.min, self.max):
                if bound is not None and (bound < 0 or self.field == "formation_age" and (bound > GAP_MAX_AGE or not bound.is_integer()) or self.field == "fill_fraction" and bound > 1):
                    raise ValueError("Gap bound outside supported domain")
        return self


class GapPredicate(StrictModel):
    version: Literal["screening_daily_gap_context_v1"] = GAP_VERSION
    interval: Literal["1d"] = "1d"
    filters: list[GapCondition] = Field(default_factory=list, max_length=6)

    @model_validator(mode="after")
    def unique_fields(self):
        if len({item.field for item in self.filters}) != len(self.filters):
            raise ValueError("Duplicate gap field")
        return self


class HourlyCondition(StrictModel):
    field: str = Field(max_length=64)
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def supported(self):
        if self.field not in HOURLY_FIELDS or self.min is None and self.max is None:
            raise ValueError("Hourly condition requires a supported field and numeric bounds")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("Hourly minimum must not exceed maximum")
        return self


class HourlyPredicate(StrictModel):
    version: Literal["screening_hourly_context_v1"] = HOURLY_VERSION
    interval: Literal["1h"] = "1h"
    filters: list[HourlyCondition] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def unique_fields(self):
        if len({condition.field for condition in self.filters}) != len(self.filters):
            raise ValueError("Duplicate hourly field")
        return self


class Predicate(StrictModel):
    version: Literal["stock_screener_workspace_v1"] = VERSION
    universe: Literal["CONFIGURED_TRACKED_EQUITIES"] = UNIVERSE
    interval: Literal["1d"] = "1d"
    filters: list[Filter] = Field(default_factory=list, max_length=30)
    patterns: list[PatternFilter] = Field(default_factory=list, max_length=10)
    pattern_mode: Literal["ANY", "ALL", "NONE"] = "ANY"
    gap: GapPredicate | None = None
    hourly: HourlyPredicate | None = None

    @model_validator(mode="after")
    def unique_fields(self):
        if len({item.field for item in self.filters}) != len(self.filters):
            raise ValueError("Duplicate filter field")
        if len({item.id for item in self.patterns}) != len(self.patterns):
            raise ValueError("Duplicate pattern")
        return self


class Query(StrictModel):
    predicate: Predicate = Field(default_factory=Predicate)
    generation: str | None = Field(None, max_length=64)
    view: Literal["CURRENT"] = "CURRENT"
    new_only: bool = False
    sort: str = "ticker"
    descending: bool = False
    offset: int = Field(0, ge=0, le=10000)
    limit: int = Field(100, ge=1, le=200)

    @model_validator(mode="after")
    def supported_sort(self):
        if self.sort not in {"ticker", *FIELDS}:
            raise ValueError("Unsupported sort field")
        return self


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def predicate_hash(predicate):
    data = predicate.model_dump()
    data["filters"] = sorted((dict(item, values=sorted(set(item["values"]))) for item in data["filters"]), key=lambda item: item["field"])
    data["patterns"] = sorted(data["patterns"], key=lambda item: item["id"])
    if not data["patterns"]:
        data["pattern_mode"] = "ANY"
    if data["gap"] is None:
        data.pop("gap")
    else:
        data["gap"]["filters"] = sorted((dict(item, values=sorted(set(item["values"]))) for item in data["gap"]["filters"]), key=lambda item: item["field"])
    if data["hourly"] is None:
        data.pop("hourly")
    else:
        data["hourly"]["filters"] = sorted(data["hourly"]["filters"], key=lambda item: item["field"])
    return digest(data)


def combine(states, mode="ALL"):
    if mode == "NONE":
        result = combine(states, "ANY")
        return {"MATCH": "NO_MATCH", "NO_MATCH": "MATCH", "UNKNOWN": "UNKNOWN"}[result]
    decisive, default = ("NO_MATCH", "MATCH") if mode == "ALL" else ("MATCH", "NO_MATCH")
    return decisive if decisive in states else "UNKNOWN" if "UNKNOWN" in states else default


def evaluate_conditions(values, missing, conditions, specs):
    explanations = []
    for condition in conditions:
        value = values.get(condition.field)
        unknown = value is None or isinstance(value, float) and not math.isfinite(value)
        state = "UNKNOWN" if unknown else "MATCH"
        if not unknown:
            if getattr(condition, "values", None):
                state = "MATCH" if value in condition.values else "NO_MATCH"
            elif condition.min is not None and value < condition.min or condition.max is not None and value > condition.max:
                state = "NO_MATCH"
        explanations.append(dict(field=condition.field, value=value, state=state, unit=specs[condition.field]["unit"],
                                 condition=condition.model_dump(exclude_none=True),
                                 reason=missing.get(condition.field, "FIELD_UNAVAILABLE") if unknown else "IN_RANGE" if state == "MATCH" else "OUTSIDE_RANGE"))
    return explanations


def evaluate_gaps(context, predicate):
    if not context or context.get("version") != GAP_VERSION or context.get("status") != "READY":
        return "UNKNOWN", [], (context or {}).get("reason", "GAP_CONTEXT_UNAVAILABLE")
    matches, unknown = [], False
    for episode in context["episodes"]:
        conditions = evaluate_conditions(episode["values"], {}, predicate.filters, GAP_FIELDS)
        state = combine([condition["state"] for condition in conditions])
        if state == "MATCH":
            matches.append(episode)
        unknown |= state == "UNKNOWN"
    matches.sort(key=lambda episode: (episode["values"]["distance_fraction"], episode["values"]["formation_age"], episode["episode_id"]))
    return ("MATCH", matches, "MATCHING_GAP_EPISODE") if matches else ("UNKNOWN", [], "GAP_FIELDS_UNAVAILABLE") if unknown else ("NO_MATCH", [], "NO_MATCHING_GAP_IN_WINDOW")


def gap_summary(context, predicate):
    state, matches, reason = evaluate_gaps(context, predicate or GapPredicate())
    representative = matches[0] if matches else None
    return dict(status=(context or {}).get("status", "UNAVAILABLE"), reason=reason,
                episode_count=len((context or {}).get("episodes", [])), matching_count=len(matches) if state != "UNKNOWN" else None,
                representative={key: representative[key] for key in ("episode_id", "formation_session", "values")} if representative else None)


def evaluate(row, predicate):
    explanations = evaluate_conditions(row["values"], row.get("missing", {}), predicate.filters, FIELDS)
    if predicate.patterns:
        observations = []
        for selected in predicate.patterns:
            observation = row.get("patterns", {}).get(selected.id)
            state = "UNKNOWN" if observation is None else "MATCH" if observation["present"] else "NO_MATCH"
            observations.append(dict(field=selected.id, state=state, observation=observation))
        explanations.append(dict(field="patterns", state=combine([item["state"] for item in observations], predicate.pattern_mode),
                                 mode=predicate.pattern_mode, observations=observations))
    if predicate.gap is not None:
        state, matches, reason = evaluate_gaps(row.get("gaps"), predicate.gap)
        explanations.append(dict(field="gap", state=state, reason=reason,
                                 episode_ids=[episode["episode_id"] for episode in matches]))
    if predicate.hourly is not None:
        hourly = row.get("hourly", {})
        conditions = evaluate_conditions(hourly.get("values", {}), hourly.get("missing", {}), predicate.hourly.filters, HOURLY_FIELDS)
        explanations.append(dict(field="hourly", state=combine([condition["state"] for condition in conditions]),
                                 conditions=conditions))
    states = [item["state"] for item in explanations]
    if not row["eligible"]:
        states.append("UNKNOWN")
    return combine(states), explanations


def comparison_status(current, previous):
    if previous is None:
        return "PRIOR_SESSION_UNAVAILABLE"
    if not current.get("previous_expected_session") or previous.get("session") != current["previous_expected_session"]:
        return "PRIOR_SESSION_UNAVAILABLE"
    for key in ("version", "universe", "interval", "field_set_version", "contract_hash", "capture_mode"):
        if not current.get(key) or current[key] != previous.get(key):
            return "INCOMPATIBLE_PUBLICATIONS"
    if current["capture_mode"] != "RECONSTRUCTED_FROM_RETAINED_PUBLICATION":
        return "REVISION_POLICY_UNAVAILABLE"
    try:
        current_cutoff = datetime.fromisoformat(current["source_cutoff"])
        prior_cutoff = datetime.fromisoformat(previous["source_cutoff"])
        if current_cutoff.tzinfo is None or prior_cutoff.tzinfo is None or prior_cutoff >= current_cutoff:
            return "REVISION_POLICY_UNAVAILABLE"
    except (KeyError, ValueError, TypeError):
        return "REVISION_POLICY_UNAVAILABLE"
    contract = current.get("daily_state_contract")
    if contract != previous.get("daily_state_contract") or contract not in (None, "daily_state_extracted_v1"):
        return "INCOMPATIBLE_PUBLICATIONS"
    for name in ("technicals.py", "daily_state.py" if contract else "stock_discovery.py"):
        if not current.get("code_hashes", {}).get(name) or current["code_hashes"][name] != previous.get("code_hashes", {}).get(name):
            return "INCOMPATIBLE_PUBLICATIONS"
    return "READY"


def new_membership(row, prior, predicate, current_lineage, prior_lineage):
    if prior is None:
        return "UNAVAILABLE", "NO_PRIOR_MEMBERSHIP"
    if not row["eligible"] or not prior["eligible"]:
        return "UNAVAILABLE", "ELIGIBILITY_UNAVAILABLE"
    if row["values"].get("instrument_type") != prior["values"].get("instrument_type"):
        return "UNAVAILABLE", "INSTRUMENT_TYPE_CHANGED"
    state, explanations = evaluate(prior, predicate)
    if state == "UNKNOWN" or any(item["state"] == "UNKNOWN" or any(observation["state"] == "UNKNOWN" for observation in item.get("observations", [])) for item in explanations):
        return "UNAVAILABLE", "PRIOR_FIELDS_UNAVAILABLE"
    if not current_lineage or not prior_lineage or not row.get("source_bar_id") or not prior.get("source_bar_id"):
        return "UNAVAILABLE", "SOURCE_LINEAGE_UNAVAILABLE"
    warmup = max([GAP_WINDOW if predicate.gap is not None else 2] + [FIELDS[condition.field]["warmup_sessions"] for condition in predicate.filters])
    current_bars, prior_bars = current_lineage.get("bars", []), prior_lineage.get("bars", [])
    if not current_bars or not prior_bars or current_bars[-1] != row["source_bar_id"] or prior_bars[-1] != prior["source_bar_id"]:
        return "UNAVAILABLE", "SOURCE_LINEAGE_UNAVAILABLE"
    if current_bars[-warmup:-1] != prior_bars[-(warmup - 1):] or set(current_lineage.get("actions", [])) - set(prior_lineage.get("actions", [])):
        return "UNAVAILABLE", "SOURCE_HISTORY_CHANGED"
    return ("NEW", "PRIOR_NONMATCH") if state == "NO_MATCH" else ("NOT_NEW", "PRIOR_MATCH")


def query_generation(generation, request, previous=None):
    if generation["version"] != VERSION:
        raise ValueError("Unsupported publication version")
    if request.generation and request.generation != generation["generation"]:
        raise ValueError("Requested publication unavailable")
    requested_fields = {condition.field for condition in request.predicate.filters} | {request.sort}
    unsupported = (requested_fields & (STATE_FIELDS | SHORT_MOMENTUM_FIELDS)) - set(generation.get("field_coverage", {}))
    if unsupported:
        raise ValueError("Fields unavailable in this publication: " + ", ".join(sorted(unsupported)))
    if request.predicate.gap is not None and generation.get("gap_contract", {}).get("version") != GAP_VERSION:
        raise ValueError("Daily gap context unavailable in this publication")
    if request.predicate.hourly is not None and not supported_hourly_contract(generation.get("hourly_contract")):
        raise ValueError("Hourly context unavailable in this publication")
    availability = comparison_status(generation, previous)
    if request.predicate.hourly is not None:
        availability = "HOURLY_COMPARISON_NOT_ENABLED"
    if availability == "READY" and request.predicate.gap is not None and (generation.get("gap_contract") != previous.get("gap_contract") or generation.get("gap_code_hashes") != previous.get("gap_code_hashes")):
        availability = "INCOMPATIBLE_PUBLICATIONS"
    prior_rows = {row["security_id"]: row for row in previous["rows"]} if availability == "READY" else {}
    matches, unknown, nonmatches = [], 0, 0
    for row in generation["rows"]:
        state, explanations = evaluate(row, request.predicate)
        if state == "MATCH":
            membership, reason = new_membership(row, prior_rows.get(row["security_id"]), request.predicate,
                generation.get("lineage", {}).get(row["security_id"]), (previous or {}).get("lineage", {}).get(row["security_id"])) if availability == "READY" else ("UNAVAILABLE", availability)
            visible = {name: value for name, value in row.items() if name != "gaps"}
            if "gaps" in row:
                visible["gap_summary"] = gap_summary(row["gaps"], request.predicate.gap)
            matches.append(dict(visible, explanations=explanations, new_status=membership, new_reason=reason))
        elif state == "UNKNOWN":
            unknown += 1
        else:
            nonmatches += 1
    matched_count = len(matches)
    new_count = sum(row["new_status"] == "NEW" for row in matches) if availability == "READY" else None
    unavailable_count = sum(row["new_status"] == "UNAVAILABLE" for row in matches)
    if request.new_only:
        matches = [row for row in matches if row["new_status"] == "NEW"]
    matches.sort(key=lambda row: row["security_id"])
    key = lambda row: row["ticker"] if request.sort == "ticker" else row["values"].get(request.sort)
    known = [row for row in matches if key(row) is not None]
    missing = [row for row in matches if key(row) is None]
    known.sort(key=key, reverse=request.descending)
    return dict(generation={name: value for name, value in generation.items() if name not in {"rows", "lineage", "hourly_lineage"}},
                rows=(known + missing)[request.offset:request.offset + request.limit], matched_count=matched_count,
                unknown_count=unknown, nonmatch_count=nonmatches, universe_count=len(generation["rows"]),
                predicate_hash=predicate_hash(request.predicate), offset=request.offset, limit=request.limit,
                result_count=len(matches), new_only=request.new_only,
                comparison=dict(status=availability, current_session=generation.get("session"),
                    previous_session=generation.get("previous_expected_session"),
                    previous_generation=previous["generation"] if availability == "READY" else None,
                    new_count=new_count, unavailable_count=unavailable_count),
                change_status=availability, view="CURRENT")