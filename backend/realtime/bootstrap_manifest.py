from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
import json
from pathlib import Path
import re
from typing import Mapping

from .contracts import UniverseManifest, canonical_sha256
from .equity_portal_history import (
    RealtimeEquityPortalHistoryPlan,
    RealtimeEquityPortalHistoryPolicy,
    build_realtime_equity_portal_history_plan,
)


BOOTSTRAP_SCHEMA_VERSION = "realtime-bootstrap-manifest-v1"
MAXIMUM_MANIFEST_BYTES = 64 * 1024
INTEGRATION_DATABASE_NAME = "stocks_realtime_v1"
_FORBIDDEN_DATABASE_NAMES = frozenset({"stocks_db", "stocks_db_fresh", "postgres"})
_DATABASE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,62}$")
_ROLE_PREFIX_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,48}$")
_SECRET_KEY_PATTERN = re.compile(r"(password|secret|token|api[_-]?key|dsn)", re.IGNORECASE)


class BootstrapTargetKind(str, Enum):
    INTEGRATION_DISPOSABLE = "INTEGRATION_DISPOSABLE"
    PRODUCTION_FRESH = "PRODUCTION_FRESH"


class BootstrapStageName(str, Enum):
    PROVISION = "PROVISION"
    MIGRATIONS = "MIGRATIONS"
    SECURITY = "SECURITY"
    REFERENCES = "REFERENCES"
    CONTROL_PLANE = "CONTROL_PLANE"
    HISTORY = "HISTORY"
    FEATURES = "FEATURES"
    SCREENING = "SCREENING"
    PORTAL = "PORTAL"
    ACCEPTANCE = "ACCEPTANCE"
    LOCK = "LOCK"


BOOTSTRAP_STAGE_ORDER = tuple(BootstrapStageName)


@dataclass(frozen=True, slots=True)
class RealtimeBootstrapManifest:
    schema_version: str
    version: str
    target_kind: BootstrapTargetKind
    provider_tier: str
    universe: UniverseManifest
    history_policy: RealtimeEquityPortalHistoryPolicy
    required_split_tickers: tuple[str, ...]
    maximum_reference_requests: int
    maximum_history_requests: int

    def __post_init__(self) -> None:
        if self.schema_version != BOOTSTRAP_SCHEMA_VERSION:
            raise ValueError("unsupported realtime bootstrap manifest schema")
        if not self.version.strip():
            raise ValueError("realtime bootstrap manifest version cannot be blank")
        if self.provider_tier not in {"MASSIVE_DEVELOPER", "MASSIVE_ADVANCED"}:
            raise ValueError("unsupported realtime bootstrap provider tier")
        if self.target_kind is BootstrapTargetKind.INTEGRATION_DISPOSABLE and len(self.universe.equity_tickers) > 50:
            raise ValueError("integration bootstrap universe exceeds 50 equities")
        required = tuple(sorted(ticker.strip().upper() for ticker in self.required_split_tickers))
        if not set(required).issubset(self.universe.equity_tickers):
            raise ValueError("required split ticker is outside the bootstrap universe")
        if required != self.history_policy.required_split_tickers:
            raise ValueError("bootstrap and history split requirements differ")
        if self.maximum_reference_requests <= 0 or self.maximum_history_requests <= 0:
            raise ValueError("bootstrap provider request bounds must be positive")
        object.__setattr__(self, "required_split_tickers", required)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class RealtimeBootstrapStagePlan:
    name: BootstrapStageName
    ordinal: int
    provider_request_limit: int
    side_effect: str


@dataclass(frozen=True, slots=True)
class RealtimeBootstrapPlan:
    database: str
    role_prefix: str
    session_date: date
    manifest_sha256: str
    universe_sha256: str
    history: RealtimeEquityPortalHistoryPlan
    stages: tuple[RealtimeBootstrapStagePlan, ...]
    maximum_provider_requests: int

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def _reject_secret_keys(value: object, path: str = "manifest") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            if _SECRET_KEY_PATTERN.search(name):
                raise ValueError(f"realtime bootstrap manifest contains secret key: {path}.{name}")
            _reject_secret_keys(item, f"{path}.{name}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_secret_keys(item, f"{path}[{index}]")


def _exact_keys(value: Mapping[str, object], expected: frozenset[str], name: str) -> None:
    actual = frozenset(value)
    if actual != expected:
        raise ValueError(
            f"{name} keys changed: missing={sorted(expected - actual)} "
            f"unexpected={sorted(actual - expected)}"
        )


def load_realtime_bootstrap_manifest(path: Path) -> RealtimeBootstrapManifest:
    if not path.is_file():
        raise ValueError("realtime bootstrap manifest is unavailable")
    payload = path.read_bytes()
    if not 0 < len(payload) <= MAXIMUM_MANIFEST_BYTES:
        raise ValueError("realtime bootstrap manifest exceeds its size bound")
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ValueError("realtime bootstrap manifest is invalid JSON") from error
    if not isinstance(value, dict):
        raise ValueError("realtime bootstrap manifest must be an object")
    _reject_secret_keys(value)
    _exact_keys(value, frozenset({
        "schema_version", "version", "target_kind", "provider_tier", "universe",
        "history", "required_split_tickers", "maximum_reference_requests",
        "maximum_history_requests",
    }), "realtime bootstrap manifest")
    universe_value = value["universe"]
    history_value = value["history"]
    if not isinstance(universe_value, dict) or not isinstance(history_value, dict):
        raise ValueError("realtime bootstrap manifest sections must be objects")
    _exact_keys(universe_value, frozenset({
        "version", "equity_tickers", "option_underlyings",
    }), "realtime bootstrap universe")
    history_keys = {
        "version", "adjusted_daily_sessions", "raw_intraday_sessions",
        "native_one_minute_sessions", "native_adjusted_monthly_periods",
        "native_raw_interval", "derived_raw_intervals", "derived_adjusted_intervals",
        "maximum_rows_per_provider_response", "maximum_persist_batch",
        "required_split_tickers",
    }
    if history_value.get("version") == "realtime-equity-portal-history-v3":
        history_keys.add("native_adjusted_weekly_periods")
    _exact_keys(history_value, frozenset(history_keys), "realtime bootstrap history")
    try:
        target_kind = BootstrapTargetKind(str(value["target_kind"]))
        universe = UniverseManifest(
            version=str(universe_value["version"]),
            equity_tickers=tuple(universe_value["equity_tickers"]),
            option_underlyings=tuple(universe_value["option_underlyings"]),
        )
        history_policy = RealtimeEquityPortalHistoryPolicy(
            version=str(history_value["version"]),
            adjusted_daily_sessions=int(history_value["adjusted_daily_sessions"]),
            raw_intraday_sessions=int(history_value["raw_intraday_sessions"]),
            native_one_minute_sessions=int(history_value["native_one_minute_sessions"]),
            native_adjusted_monthly_periods=int(history_value["native_adjusted_monthly_periods"]),
            native_adjusted_weekly_periods=int(history_value.get("native_adjusted_weekly_periods", 261)),
            native_raw_interval=str(history_value["native_raw_interval"]),
            derived_raw_intervals=tuple(history_value["derived_raw_intervals"]),
            derived_adjusted_intervals=tuple(history_value["derived_adjusted_intervals"]),
            maximum_rows_per_provider_response=int(history_value["maximum_rows_per_provider_response"]),
            maximum_persist_batch=int(history_value["maximum_persist_batch"]),
            required_split_tickers=tuple(history_value["required_split_tickers"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("realtime bootstrap manifest contract is invalid") from error
    return RealtimeBootstrapManifest(
        schema_version=str(value["schema_version"]),
        version=str(value["version"]),
        target_kind=target_kind,
        provider_tier=str(value["provider_tier"]),
        universe=universe,
        history_policy=history_policy,
        required_split_tickers=tuple(value["required_split_tickers"]),
        maximum_reference_requests=int(value["maximum_reference_requests"]),
        maximum_history_requests=int(value["maximum_history_requests"]),
    )


def validate_bootstrap_target(
    database: str,
    confirmation: str,
    target_kind: BootstrapTargetKind,
) -> str:
    target = database.strip()
    if not target or target != confirmation.strip() or not _DATABASE_PATTERN.fullmatch(target):
        raise ValueError("bootstrap database confirmation or name is invalid")
    if target in _FORBIDDEN_DATABASE_NAMES:
        raise ValueError("canonical or maintenance database cannot be a realtime target")
    if target_kind is BootstrapTargetKind.INTEGRATION_DISPOSABLE:
        if target != INTEGRATION_DATABASE_NAME and not target.startswith("test_realtime_"):
            raise ValueError("integration bootstrap requires the owned disposable target")
    elif target == INTEGRATION_DATABASE_NAME or target.startswith("test_realtime_"):
        raise ValueError("production bootstrap requires a separately named database")
    return target


def build_realtime_bootstrap_plan(
    manifest: RealtimeBootstrapManifest,
    *,
    database: str,
    confirmation: str,
    role_prefix: str,
    session_date: date,
) -> RealtimeBootstrapPlan:
    target = validate_bootstrap_target(database, confirmation, manifest.target_kind)
    normalized_prefix = role_prefix.strip()
    if not _ROLE_PREFIX_PATTERN.fullmatch(normalized_prefix):
        raise ValueError("realtime bootstrap role prefix is invalid")
    history = build_realtime_equity_portal_history_plan(
        session_date,
        policy=manifest.history_policy,
        equity_ticker_count=len(manifest.universe.equity_tickers),
    )
    if history.expected_provider_requests > manifest.maximum_history_requests:
        raise ValueError("bootstrap history plan exceeds its manifest request bound")
    request_limits = {
        BootstrapStageName.REFERENCES: manifest.maximum_reference_requests,
        BootstrapStageName.HISTORY: manifest.maximum_history_requests,
    }
    side_effects = {
        BootstrapStageName.PROVISION: "DATABASE_ADMIN",
        BootstrapStageName.MIGRATIONS: "DATABASE_WRITE",
        BootstrapStageName.SECURITY: "DATABASE_ADMIN",
        BootstrapStageName.REFERENCES: "PROVIDER_AND_DATABASE",
        BootstrapStageName.CONTROL_PLANE: "DATABASE_WRITE",
        BootstrapStageName.HISTORY: "PROVIDER_AND_DATABASE",
        BootstrapStageName.FEATURES: "DATABASE_WRITE",
        BootstrapStageName.SCREENING: "DATABASE_WRITE",
        BootstrapStageName.PORTAL: "DATABASE_WRITE",
        BootstrapStageName.ACCEPTANCE: "READ_ONLY",
        BootstrapStageName.LOCK: "ROLE_ADMIN",
    }
    stages = tuple(
        RealtimeBootstrapStagePlan(
            name=name,
            ordinal=ordinal,
            provider_request_limit=request_limits.get(name, 0),
            side_effect=side_effects[name],
        )
        for ordinal, name in enumerate(BOOTSTRAP_STAGE_ORDER, start=1)
    )
    return RealtimeBootstrapPlan(
        database=target,
        role_prefix=normalized_prefix,
        session_date=session_date,
        manifest_sha256=manifest.sha256,
        universe_sha256=manifest.universe.sha256,
        history=history,
        stages=stages,
        maximum_provider_requests=(
            manifest.maximum_reference_requests + manifest.maximum_history_requests
        ),
    )
