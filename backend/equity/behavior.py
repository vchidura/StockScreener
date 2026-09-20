"""Versioned stock behavior evidence; no computation, persistence or trading authority."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


SCHEMA_VERSION = "stock_behavior_v1"
ADJUSTED_DAILY_EVIDENCE_SOURCE = "STOCK_BEHAVIOR_ADJUSTED_DAILY"
ADJUSTED_DAILY_EVIDENCE_VERSION = "provider_adjusted_daily_history_v1"
ADJUSTED_DAILY_EVIDENCE_SCHEMA = "stock_behavior_adjusted_1d_v1"
RAW_FEATURE_EVIDENCE_SOURCE = "STOCK_BEHAVIOR_RAW_SOURCE"
RAW_FEATURE_EVIDENCE_VERSION = "behavior_feature_source_v1"
RAW_FEATURE_EVIDENCE_SCHEMA = "stock_behavior_raw_source_v1"
BEHAVIOR_SOURCE_EVIDENCE_CONTRACTS = frozenset({
    (ADJUSTED_DAILY_EVIDENCE_SOURCE, ADJUSTED_DAILY_EVIDENCE_VERSION, ADJUSTED_DAILY_EVIDENCE_SCHEMA),
    (RAW_FEATURE_EVIDENCE_SOURCE, RAW_FEATURE_EVIDENCE_VERSION, RAW_FEATURE_EVIDENCE_SCHEMA),
})
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Name = Annotated[str, Field(min_length=1, max_length=80, pattern=r"^\S+$")]
Interval = Literal["1d", "1h", "30m"]
Factor = Literal["TREND", "MOMENTUM", "VOLATILITY", "PARTICIPATION", "LOCATION", "RELATIVE_STRENGTH"]
Status = Literal["READY", "STALE", "INSUFFICIENT_HISTORY", "UNAVAILABLE", "NOT_APPLICABLE"]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False, validate_default=True)

    @field_validator("*", mode="after")
    @classmethod
    def utc_datetimes(cls, value):
        return value.astimezone(timezone.utc) if isinstance(value, datetime) else value

    def canonical_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("ascii")).hexdigest()


class MetricDefinition(Contract):
    metric_id: Name
    factor: Factor
    unit: Literal["USD", "FRACTION", "RATIO", "OSCILLATOR_0_100", "ATR", "ATR_PER_BAR", "ANNUAL_VOL_FRACTION", "USD_PER_SESSION"]
    formula_id: Name
    lookback_bars: int = Field(strict=True, gt=0)
    minimum_samples: int = Field(strict=True, gt=0)
    intervals: tuple[Interval, ...]
    minimum: float | None = None
    maximum: float | None = None


METRICS_V1 = (
    MetricDefinition(metric_id="ema50_slope10_atr", factor="TREND", unit="ATR_PER_BAR", formula_id="ema50_delta10_over_10_prior_wilder14_v1", lookback_bars=200, minimum_samples=200, intervals=("1d", "1h", "30m")),
    MetricDefinition(metric_id="adx14", factor="TREND", unit="OSCILLATOR_0_100", formula_id="technical_ewm_dm14_v1", lookback_bars=200, minimum_samples=200, intervals=("1d", "1h", "30m"), minimum=0, maximum=100),
    MetricDefinition(metric_id="return5", factor="MOMENTUM", unit="FRACTION", formula_id="close_ratio5_minus1_v1", lookback_bars=6, minimum_samples=6, intervals=("1d", "1h", "30m"), minimum=-1),
    MetricDefinition(metric_id="momentum_change5", factor="MOMENTUM", unit="FRACTION", formula_id="nonoverlap_return5_difference_v1", lookback_bars=11, minimum_samples=11, intervals=("1d", "1h", "30m")),
    MetricDefinition(metric_id="atr14_fraction", factor="VOLATILITY", unit="FRACTION", formula_id="wilder14_seed_mean_tr_over_close_v1", lookback_bars=200, minimum_samples=200, intervals=("1d", "1h", "30m"), minimum=0),
    MetricDefinition(metric_id="rv20_cc_annual", factor="VOLATILITY", unit="ANNUAL_VOL_FRACTION", formula_id="log_return20_std_ddof1_sqrt252_v1", lookback_bars=21, minimum_samples=21, intervals=("1d",), minimum=0),
    MetricDefinition(metric_id="rv20_percentile252", factor="VOLATILITY", unit="FRACTION", formula_id="rv20_rank_prior252_midrank_v1", lookback_bars=273, minimum_samples=273, intervals=("1d",), minimum=0, maximum=1),
    MetricDefinition(metric_id="compression_tr5_20", factor="VOLATILITY", unit="RATIO", formula_id="prior_tr5_mean_over_prior_tr20_mean_v1", lookback_bars=22, minimum_samples=22, intervals=("1d", "1h", "30m"), minimum=0),
    MetricDefinition(metric_id="daily_rvol20", factor="PARTICIPATION", unit="RATIO", formula_id="daily_volume_over_prior20_mean_v1", lookback_bars=21, minimum_samples=21, intervals=("1d",), minimum=0),
    MetricDefinition(metric_id="median_dollar_volume20", factor="PARTICIPATION", unit="USD_PER_SESSION", formula_id="prior20_median_close_times_volume_v1", lookback_bars=21, minimum_samples=21, intervals=("1d",), minimum=0),
    MetricDefinition(metric_id="extension_ema21_atr", factor="LOCATION", unit="ATR", formula_id="close_minus_ema21_over_prior_wilder14_v1", lookback_bars=200, minimum_samples=200, intervals=("1d", "1h", "30m")),
    MetricDefinition(metric_id="prior_range20_position", factor="LOCATION", unit="RATIO", formula_id="close_minus_prior_low20_over_prior_width20_v1", lookback_bars=21, minimum_samples=21, intervals=("1d", "1h", "30m")),
    MetricDefinition(metric_id="excess_return20", factor="RELATIVE_STRENGTH", unit="FRACTION", formula_id="paired_close_return20_difference_v1", lookback_bars=21, minimum_samples=21, intervals=("1d",)),
)
METRICS = METRICS_V1
DEFINITION_V1_SHA256 = hashlib.sha256(json.dumps(
    [metric.model_dump(mode="json") for metric in METRICS_V1], sort_keys=True, separators=(",", ":"), allow_nan=False,
).encode("ascii")).hexdigest()
DEFINITION_SHA256 = DEFINITION_V1_SHA256
BEHAVIOR_CATALOGS = MappingProxyType({("stock_behavior_v1", DEFINITION_V1_SHA256): METRICS_V1})


class BehaviorProfile(Contract):
    name: Name
    definition_sha256: Sha256
    required_metrics: tuple[tuple[Name, tuple[Name, ...]], ...]


OPTIONS_SWING_PROFILE = BehaviorProfile(
    name="OPTIONS_SWING_V1", definition_sha256=DEFINITION_SHA256,
    required_metrics=tuple((f"TREND.{interval}", ("ema50_slope10_atr", "adx14")) for interval in ("1d", "1h", "30m")),
)
BEHAVIOR_PROFILES = MappingProxyType({OPTIONS_SWING_PROFILE.name: OPTIONS_SWING_PROFILE})
BEHAVIOR_PROFILE_VERSIONS = MappingProxyType({
    (OPTIONS_SWING_PROFILE.name, OPTIONS_SWING_PROFILE.definition_sha256,
     OPTIONS_SWING_PROFILE.sha256): OPTIONS_SWING_PROFILE,
})


def resolve_behavior_profile(name: str, definition_sha256: str, policy_sha256: str) -> BehaviorProfile:
    try:
        return BEHAVIOR_PROFILE_VERSIONS[(name, definition_sha256, policy_sha256)]
    except KeyError as exc:
        raise ValueError("an exact registered behavior profile/definition/policy is required") from exc


class BehaviorMetric(Contract):
    definition: MetricDefinition
    interval: Interval
    status: Status
    value: float | None = Field(default=None, strict=True)
    sample_count: int = Field(strict=True, ge=0)
    reason_codes: tuple[Name, ...] = ()
    benchmark_security_id: UUID | None = None

    @model_validator(mode="after")
    def validate_measurement(self):
        if self.definition not in METRICS_V1 or self.interval not in self.definition.intervals:
            raise ValueError("unsupported metric definition or interval")
        if self.status == "READY":
            if self.value is None or self.sample_count < self.definition.minimum_samples or self.reason_codes:
                raise ValueError("ready metric requires a value, full sample and no missing reasons")
            if self.definition.minimum is not None and self.value < self.definition.minimum or self.definition.maximum is not None and self.value > self.definition.maximum:
                raise ValueError("metric value outside its declared range")
            if self.definition.factor == "RELATIVE_STRENGTH" and self.benchmark_security_id is None:
                raise ValueError("relative strength requires a dated benchmark identity")
        elif self.value is not None or not self.reason_codes:
            raise ValueError("unready metric requires null value and explicit reasons")
        return self


class BehaviorSource(Contract):
    evidence_id: UUID
    security_id: UUID
    interval: Interval
    payload_sha256: Sha256
    policy_sha256: Sha256
    market_time: AwareDatetime
    observed_at: AwareDatetime
    recorded_at: AwareDatetime
    published_at: AwareDatetime | None = None
    received_at: AwareDatetime
    valid_until: AwareDatetime
    price_basis: Literal["RAW_ACTION_GATED", "PROVIDER_SPLIT_ADJUSTED", "REVIEWED_SPLIT_ADJUSTED"]
    action_review_ids: tuple[Name, ...] = ()
    source_manifest_sha256: Sha256 | None = None
    availability_mode: Literal["PROSPECTIVE_RECEIPT", "RECONSTRUCTED"]
    history_mode: Literal["LIVE_OBSERVED_HISTORY", "RECONSTRUCTED_HISTORY"] = "LIVE_OBSERVED_HISTORY"
    history_available_at: AwareDatetime | None = None
    session_scope: Literal["RTH"] = "RTH"
    completed_bars_only: Literal[True] = True

    @model_validator(mode="after")
    def validate_clocks(self):
        if self.observed_at < self.market_time or self.received_at < max(self.market_time, self.observed_at, self.recorded_at, self.published_at or self.recorded_at):
            raise ValueError("source receipt must follow all source clocks")
        if self.valid_until <= self.market_time:
            raise ValueError("source validity must follow market time")
        if self.price_basis == "REVIEWED_SPLIT_ADJUSTED" and not self.action_review_ids:
            raise ValueError("adjusted sources require action review IDs")
        if self.price_basis == "PROVIDER_SPLIT_ADJUSTED" and self.source_manifest_sha256 is None:
            raise ValueError("provider-adjusted sources require an exact source manifest")
        if self.history_mode == "RECONSTRUCTED_HISTORY":
            if self.history_available_at is None or not self.market_time <= self.history_available_at <= self.received_at:
                raise ValueError("reconstructed history requires causal availability")
        elif self.history_available_at is not None:
            raise ValueError("live-observed history does not use a replay availability clock")
        if self.availability_mode == "RECONSTRUCTED" and self.history_mode != "RECONSTRUCTED_HISTORY":
            raise ValueError("reconstructed assembly requires reconstructed history")
        return self


STATES = {
    "TREND": {"UP", "DOWN", "MIXED", "FLAT"},
    "MOMENTUM": {"POSITIVE", "NEGATIVE", "MIXED", "FLAT"},
    "VOLATILITY": {"QUIET", "NORMAL", "ELEVATED", "COMPRESSED", "EXPANDING", "MIXED"},
    "PARTICIPATION": {"ELEVATED", "NORMAL", "WEAK"},
    "LOCATION": {"INSIDE_RANGE", "ABOVE_BOUNDARY", "BELOW_BOUNDARY", "EXTENDED", "MIXED"},
    "RELATIVE_STRENGTH": {"LEADING", "LAGGING", "MIXED"},
}


class BehaviorComponent(Contract):
    factor: Factor
    interval: Interval
    status: Status
    state: Name | None = None
    classification_policy_sha256: Sha256 | None = None
    metrics: tuple[BehaviorMetric, ...] = Field(default=(), max_length=32)
    sources: tuple[BehaviorSource, ...] = Field(default=(), max_length=32)
    reason_codes: tuple[Name, ...] = ()

    @property
    def key(self) -> str:
        return f"{self.factor}.{self.interval}"

    @model_validator(mode="after")
    def validate_component(self):
        if len({metric.definition.metric_id for metric in self.metrics}) != len(self.metrics):
            raise ValueError("component metric IDs must be distinct")
        if any(metric.interval != self.interval or metric.definition.factor != self.factor for metric in self.metrics):
            raise ValueError("metric must match component factor and interval")
        if len({source.evidence_id for source in self.sources}) != len(self.sources) or any(source.interval != self.interval for source in self.sources):
            raise ValueError("source IDs must be distinct and match the component interval")
        if self.status == "READY":
            if not self.metrics or any(metric.status != "READY" for metric in self.metrics) or not self.sources or self.reason_codes:
                raise ValueError("ready component requires usable measurements and provenance")
            if self.state is not None and (self.state not in STATES[self.factor] or not self.classification_policy_sha256):
                raise ValueError("state requires a supported factor value and classification policy")
        elif self.state is not None or not self.reason_codes:
            raise ValueError("unready component cannot declare a usable state")
        return self


class StockBehaviorSnapshot(Contract):
    schema_version: Literal["stock_behavior_v1"] = SCHEMA_VERSION
    definition_sha256: Literal[DEFINITION_V1_SHA256] = DEFINITION_V1_SHA256
    security_id: UUID
    security_revision_id: UUID
    ticker: Annotated[str, Field(pattern=r"^[A-Z0-9][A-Z0-9.\-]{0,31}$")]
    profile: Literal["OPTIONS_SWING_V1"] = "OPTIONS_SWING_V1"
    policy_sha256: Sha256
    market_time: AwareDatetime
    computed_at: AwareDatetime
    available_at: AwareDatetime
    valid_until: AwareDatetime
    availability_mode: Literal["PROSPECTIVE_RECEIPT", "RECONSTRUCTED"]
    components: tuple[BehaviorComponent, ...] = Field(min_length=1, max_length=18)
    required_components: tuple[Name, ...] = Field(min_length=1, max_length=18)
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_snapshot(self):
        if not self.market_time <= self.computed_at <= self.available_at < self.valid_until:
            raise ValueError("snapshot availability/validity clocks are not causal")
        by_key = {component.key: component for component in self.components}
        profile = resolve_behavior_profile(self.profile, self.definition_sha256, self.policy_sha256)
        if self.policy_sha256 != profile.sha256 or self.required_components != tuple(key for key, _ in profile.required_metrics):
            raise ValueError("snapshot must match its registered profile policy and requirements")
        if len(by_key) != len(self.components) or len(set(self.required_components)) != len(self.required_components) or not set(self.required_components) <= by_key.keys():
            raise ValueError("components must be distinct and include all declared requirements")
        for key, metric_ids in profile.required_metrics:
            component = by_key[key]
            if component.status == "READY" and not set(metric_ids) <= {metric.definition.metric_id for metric in component.metrics}:
                raise ValueError("ready component lacks required profile metrics")
        for component in self.components:
            if component.metrics and not component.sources:
                raise ValueError("measurements require source lineage")
            for source in component.sources:
                benchmarks = {metric.benchmark_security_id for metric in component.metrics if metric.benchmark_security_id}
                if source.security_id != self.security_id and not (component.factor == "RELATIVE_STRENGTH" and source.security_id in benchmarks):
                    raise ValueError("source security identity mismatch")
                if source.market_time > self.market_time or source.received_at > self.computed_at:
                    raise ValueError("source is not available at snapshot cutoff")
                if self.availability_mode == "PROSPECTIVE_RECEIPT" and source.availability_mode != self.availability_mode:
                    raise ValueError("reconstructed input cannot become prospective evidence")
                if component.status == "READY" and source.valid_until <= self.available_at:
                    raise ValueError("expired source cannot be ready")
                if component.key in self.required_components and component.status == "READY" and self.valid_until > source.valid_until:
                    raise ValueError("snapshot cannot extend required source validity")
            if component.status == "READY" and not any(source.security_id == self.security_id for source in component.sources):
                raise ValueError("ready component requires own-security evidence")
            if component.factor == "RELATIVE_STRENGTH" and component.status == "READY":
                source_securities = {source.security_id for source in component.sources}
                if any(metric.benchmark_security_id == self.security_id or metric.benchmark_security_id not in source_securities for metric in component.metrics):
                    raise ValueError("relative strength requires distinct benchmark evidence")
                if len({(source.market_time, source.price_basis) for source in component.sources}) != 1:
                    raise ValueError("relative strength requires paired market cutoffs and price basis")
        if len(self.canonical_json().encode("ascii")) > 262144:
            raise ValueError("behavior payload exceeds contract size")
        return self

    def assess_at(self, decision_at: datetime) -> BehaviorAssessment:
        if decision_at.tzinfo is None or decision_at.utcoffset() is None:
            raise ValueError("decision time must be timezone-aware")
        components = []
        for component in self.components:
            status, reasons = component.status, component.reason_codes
            temporally_masked = False
            if status == "READY":
                if decision_at < self.available_at:
                    status, reasons = "UNAVAILABLE", ("SNAPSHOT_NOT_YET_AVAILABLE",)
                    temporally_masked = True
                elif decision_at >= self.valid_until or any(decision_at >= source.valid_until for source in component.sources):
                    status, reasons = "STALE", ("EVIDENCE_EXPIRED_AT_DECISION",)
                    temporally_masked = True
            usable_metrics = () if temporally_masked else tuple(
                metric for metric in component.metrics if metric.status == "READY"
            )
            components.append(BehaviorComponentAssessment(
                key=component.key, status=status, state=component.state if status == "READY" else None,
                metrics=usable_metrics, reason_codes=reasons,
            ))
        required = [component for component in components if component.key in self.required_components]
        ready = sum(component.status == "READY" for component in required)
        data_status = "READY" if ready == len(required) else "PARTIAL" if ready else "UNAVAILABLE"
        trend = [component for component in components if component.key.startswith("TREND.")]
        alignment = "PARTIAL"
        if len(trend) == 3 and all(component.status == "READY" and component.state is not None for component in trend):
            directions = {component.state for component in trend}
            alignment = "ALIGNED_UP" if directions == {"UP"} else "ALIGNED_DOWN" if directions == {"DOWN"} else "ALL_FLAT" if directions == {"FLAT"} else "MIXED"
        return BehaviorAssessment(snapshot_id=self.snapshot_id, payload_sha256=self.sha256, decision_at=decision_at,
            data_status=data_status, alignment_state=alignment, components=tuple(components))

    @property
    def snapshot_id(self) -> UUID:
        return uuid5(NAMESPACE_URL, f"stock-behavior:{self.identity_sha256}")

    @property
    def identity_sha256(self) -> str:
        payload = self.model_dump(mode="json", exclude={"computed_at", "available_at"})
        for component in payload["components"]:
            for source in component["sources"]:
                source.pop("received_at", None)
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("ascii")).hexdigest()


class BehaviorComponentAssessment(Contract):
    key: Name
    status: Status
    state: Name | None
    metrics: tuple[BehaviorMetric, ...]
    reason_codes: tuple[Name, ...]


class BehaviorAssessment(Contract):
    snapshot_id: UUID
    payload_sha256: Sha256
    decision_at: AwareDatetime
    data_status: Literal["READY", "PARTIAL", "UNAVAILABLE"]
    alignment_state: Literal["ALIGNED_UP", "ALIGNED_DOWN", "ALL_FLAT", "MIXED", "PARTIAL"]
    components: tuple[BehaviorComponentAssessment, ...]
    execution_permission: Literal[False] = False


BEHAVIOR_SNAPSHOT_CONTRACTS = MappingProxyType({
    ("stock_behavior_v1", DEFINITION_V1_SHA256): StockBehaviorSnapshot,
})


def load_stock_behavior_snapshot(payload_text: str) -> StockBehaviorSnapshot:
    payload = json.loads(payload_text)
    if not isinstance(payload, dict):
        raise ValueError("behavior snapshot payload must be an object")
    contract = BEHAVIOR_SNAPSHOT_CONTRACTS.get((
        payload.get("schema_version"), payload.get("definition_sha256"),
    ))
    if contract is None:
        raise ValueError("unsupported behavior schema/definition")
    return contract.model_validate(payload)