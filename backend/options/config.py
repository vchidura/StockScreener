from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator


class DataEngine(str, Enum):
    POLYGON_DEVELOPER = "polygon_developer"
    POLYGON_ADVANCED = "polygon_advanced"


class ExecutionEngine(str, Enum):
    PAPER_PROXY = "paper_proxy"
    ADVANCED_SHADOW = "advanced_shadow"
    ALPACA = "alpaca"
    TRADIER = "tradier"


class UniverseMode(str, Enum):
    FIXED = "fixed"
    RANKED = "ranked"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", protected_namespaces=())


class OptionSettings(_FrozenModel):
    polygon_api_key: SecretStr
    data_engine: DataEngine = DataEngine.POLYGON_DEVELOPER
    underlying_data_provider: str = "polygon_stocks"
    event_calendar_provider: str | None = None
    equity_context_enabled: bool = False
    execution_engine: ExecutionEngine = ExecutionEngine.PAPER_PROXY
    universe_mode: UniverseMode = UniverseMode.FIXED
    fixed_stock_underlyers: tuple[str, ...] = (
        "AAPL",
        "AMD",
        "AMZN",
        "GOOGL",
        "META",
        "MSFT",
        "NVDA",
        "PLTR",
        "SOFI",
        "TSLA",
    )
    fixed_etf_underlyers: tuple[str, ...] = ("SPY", "QQQ", "IWM")
    stock_universe_size: int = Field(default=10, gt=0)
    etf_universe_size: int = Field(default=3, gt=0)
    poll_seconds: int = Field(default=900, ge=60)
    starting_cash: Decimal = Field(default=Decimal("250000"), gt=0)
    risk_free_rate: Decimal = Field(default=Decimal("0.04"), ge=Decimal("-0.10"), le=1)
    risk_free_rate_source: str = "manual_config_v1"
    default_dividend_yield: Decimal = Field(default=Decimal("0"), ge=0, le=1)
    policy_file: Path = Path("options/policies/developer_v1.json")
    strategy_policy_file: Path = Path("options/policies/strategy_v1.json")
    raw_archive_enabled: bool = False
    raw_archive_root: Path = Path("option-raw")
    start_read_only: bool = True

    @field_validator("fixed_stock_underlyers", "fixed_etf_underlyers", mode="before")
    @classmethod
    def _parse_underlyers(cls, value: object) -> object:
        if isinstance(value, str):
            value = tuple(part.strip() for part in value.split(",") if part.strip())
        if isinstance(value, (list, tuple)):
            normalized = tuple(str(part).strip().upper() for part in value)
            if any(not ticker or not ticker.replace(".", "").isalnum() for ticker in normalized):
                raise ValueError("underlyers must be non-empty ticker symbols")
            if len(normalized) != len(set(normalized)):
                raise ValueError("underlyers must not contain duplicates")
            return normalized
        return value

    @field_validator("underlying_data_provider", "event_calendar_provider")
    @classmethod
    def _normalize_provider(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("provider names cannot be blank")
        return normalized

    @field_validator("risk_free_rate_source")
    @classmethod
    def _normalize_rate_source(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("risk_free_rate_source cannot be blank")
        return normalized

    @model_validator(mode="after")
    def _validate_universe(self) -> "OptionSettings":
        overlap = set(self.fixed_stock_underlyers) & set(self.fixed_etf_underlyers)
        if overlap:
            raise ValueError(f"stock and ETF universes overlap: {sorted(overlap)}")
        if self.universe_mode is UniverseMode.FIXED:
            if len(self.fixed_stock_underlyers) != self.stock_universe_size:
                raise ValueError("fixed stock underlyers must match stock_universe_size")
            if len(self.fixed_etf_underlyers) != self.etf_universe_size:
                raise ValueError("fixed ETF underlyers must match etf_universe_size")
        return self

    @property
    def underlyers(self) -> tuple[str, ...]:
        return self.fixed_stock_underlyers + self.fixed_etf_underlyers

    def fingerprint_payload(self) -> dict[str, object]:
        return {
            "data_engine": self.data_engine.value,
            "underlying_data_provider": self.underlying_data_provider,
            "event_calendar_provider": self.event_calendar_provider,
            "equity_context_enabled": self.equity_context_enabled,
            "execution_engine": self.execution_engine.value,
            "universe_mode": self.universe_mode.value,
            "fixed_stock_underlyers": self.fixed_stock_underlyers,
            "fixed_etf_underlyers": self.fixed_etf_underlyers,
            "stock_universe_size": self.stock_universe_size,
            "etf_universe_size": self.etf_universe_size,
            "poll_seconds": self.poll_seconds,
            "starting_cash": str(self.starting_cash),
            "risk_free_rate": str(self.risk_free_rate),
            "risk_free_rate_source": self.risk_free_rate_source,
            "default_dividend_yield": str(self.default_dividend_yield),
            "raw_archive_enabled": self.raw_archive_enabled,
            "raw_archive_root": str(self.raw_archive_root),
            "start_read_only": self.start_read_only,
        }


class ContractFilterPolicy(_FrozenModel):
    minimum_dte: int = Field(ge=0)
    maximum_dte: int = Field(ge=0)
    strike_corridor_fraction: Decimal = Field(gt=0, lt=1)
    minimum_day_volume: int = Field(ge=0)
    minimum_open_interest: int = Field(ge=0)
    maximum_unknown_references: int = Field(gt=0)
    maximum_unknown_reference_fraction: Decimal = Field(gt=0, le=1)
    reference_admission_budget_seconds: int = Field(gt=0)
    required_exercise_style: str
    required_shares_per_contract: int = Field(gt=0)

    @model_validator(mode="after")
    def _validate_dte_range(self) -> "ContractFilterPolicy":
        if self.maximum_dte < self.minimum_dte:
            raise ValueError("maximum_dte must not be less than minimum_dte")
        return self


class ModelQualityPolicy(_FrozenModel):
    maximum_developer_source_age_seconds: int = Field(gt=0)
    maximum_option_spot_skew_seconds: int = Field(ge=0)
    intrinsic_price_tolerance: Decimal = Field(ge=0)
    minimum_iv_success_fraction: Decimal = Field(ge=0, le=1)
    minimum_iv: Decimal = Field(gt=0)
    maximum_iv: Decimal = Field(gt=0)
    newton_iterations: int = Field(gt=0)
    price_error_tolerance: float = Field(gt=0)
    minimum_vega: float = Field(gt=0)
    use_brent_fallback: bool
    allowed_model_mark_sources: tuple[str, ...]
    display_only_mark_sources: tuple[str, ...]

    @model_validator(mode="after")
    def _validate_iv_range(self) -> "ModelQualityPolicy":
        if self.maximum_iv <= self.minimum_iv:
            raise ValueError("maximum_iv must be greater than minimum_iv")
        if set(self.allowed_model_mark_sources) & set(self.display_only_mark_sources):
            raise ValueError("model and display-only mark sources must not overlap")
        return self


class CapacityPolicy(_FrozenModel):
    maximum_contracts_per_order: int = Field(gt=0)
    maximum_positions: int = Field(gt=0)
    maximum_orders_per_cycle: int = Field(gt=0)
    maximum_pages_per_batch: int = Field(gt=0)
    maximum_contracts_per_batch: int = Field(gt=0)
    maximum_page_bytes: int = Field(gt=0)
    maximum_batch_bytes: int = Field(gt=0)
    maximum_trade_events_per_request: int = Field(gt=0)
    maximum_work_attempts: int = Field(gt=0)


class RetentionPolicy(_FrozenModel):
    raw_troubleshooting_days: int = Field(gt=0)
    normalized_intraday_days: int = Field(gt=0)
    one_second_aggregate_days: int = Field(gt=0)
    one_minute_aggregate_years: int = Field(gt=0)
    daily_research_years: int = Field(gt=0)
    ledger_audit_years: int = Field(gt=0)
    contract_metadata_indefinite: bool


class ProviderRequestPolicy(_FrozenModel):
    request_timeout_seconds: float = Field(gt=0)
    maximum_rate_limit_retries: int = Field(ge=0)
    default_retry_after_seconds: float = Field(gt=0)


class OiWallPolicy(_FrozenModel):
    percentile: float = Field(gt=0, lt=1)
    minimum_robust_z: float = Field(gt=0)
    maximum_clusters_per_expiration_type: int = Field(gt=0)
    allow_zero_mad_fallback: bool


class AnalysisPolicy(_FrozenModel):
    maximum_delta_interpolation_gap: float = Field(gt=0, le=1)


class ArchivePolicy(_FrozenModel):
    maximum_queue_items: int = Field(gt=0)
    maximum_queue_bytes: int = Field(gt=0)
    maximum_rows_per_file: int = Field(gt=0)
    maximum_buffer_age_seconds: float = Field(gt=0)
    stale_partial_grace_seconds: float = Field(gt=0)


class GammaSqueezePolicy(_FrozenModel):
    maximum_moneyness_fraction: float = Field(gt=0, lt=1)
    minimum_volume_oi_ratio: float = Field(gt=0)
    minimum_gamma: float = Field(gt=0)
    maximum_per_side: int = Field(gt=0)
    stop_loss_fraction: float = Field(gt=0, lt=1)
    take_profit_fraction: float = Field(gt=0)
    trailing_activation_fraction: float = Field(gt=0)
    trailing_distance_fraction: float = Field(gt=0, lt=1)


class IncomeWheelPolicy(_FrozenModel):
    minimum_dte: int = Field(ge=0)
    maximum_dte: int = Field(gt=0)
    exit_dte: int = Field(ge=0)
    maximum_candidates: int = Field(gt=0)
    take_profit_fraction: float = Field(gt=0, le=1)
    stop_loss_multiple: float = Field(gt=1)

    @model_validator(mode="after")
    def _validate_dte_range(self) -> "IncomeWheelPolicy":
        if self.maximum_dte < self.minimum_dte:
            raise ValueError("wheel maximum_dte must not be less than minimum_dte")
        return self


class SpreadStrategyPolicy(_FrozenModel):
    maximum_wings_per_short_strike: int = Field(gt=0)
    maximum_per_structure_expiration: int = Field(gt=0)
    maximum_center_distance_fraction: float = Field(gt=0, lt=1)


class FlowStrategyPolicy(_FrozenModel):
    minimum_print_notional: Decimal = Field(gt=0)
    minimum_sweep_prints: int = Field(gt=0)
    minimum_distinct_exchanges: int = Field(gt=0)
    sweep_window_seconds: int = Field(gt=0)
    minimum_volume_oi_ratio: float = Field(gt=0)
    maximum_candidates: int = Field(gt=0)


class SmileStrategyPolicy(_FrozenModel):
    minimum_strikes: int = Field(ge=7)
    minimum_absolute_robust_z: float = Field(gt=0)
    maximum_candidates_per_expiration_type: int = Field(gt=0)


class ScenarioPolicy(_FrozenModel):
    spot_shock_fractions: tuple[float, ...]
    iv_shock_fractions: tuple[float, ...]
    time_fractions_remaining: tuple[float, ...]

    @model_validator(mode="after")
    def _validate_grid(self) -> "ScenarioPolicy":
        if not self.spot_shock_fractions or not self.iv_shock_fractions:
            raise ValueError("scenario spot and IV shocks cannot be empty")
        if not self.time_fractions_remaining:
            raise ValueError("scenario time fractions cannot be empty")
        if any(not -1 < value for value in self.spot_shock_fractions):
            raise ValueError("scenario spot shocks must keep spot positive")
        if any(not -1 < value for value in self.iv_shock_fractions):
            raise ValueError("scenario IV shocks must keep volatility positive")
        if any(not 0 <= value <= 1 for value in self.time_fractions_remaining):
            raise ValueError("scenario time fractions must be in [0, 1]")
        return self


class StrategyPolicy(_FrozenModel):
    strategy_version: str = Field(min_length=1)
    gamma_squeeze: GammaSqueezePolicy
    income_wheel: IncomeWheelPolicy
    spreads: SpreadStrategyPolicy
    flow: FlowStrategyPolicy
    smile: SmileStrategyPolicy
    scenarios: ScenarioPolicy


class DeveloperPolicy(_FrozenModel):
    schema_version: int = Field(gt=0)
    policy_version: str = Field(min_length=1)
    contract_filter: ContractFilterPolicy
    model_quality: ModelQualityPolicy
    provider_requests: ProviderRequestPolicy
    oi_walls: OiWallPolicy
    analysis: AnalysisPolicy
    archive: ArchivePolicy
    capacity: CapacityPolicy
    retention: RetentionPolicy


@dataclass(frozen=True, slots=True)
class PolicyArtifact:
    policy: DeveloperPolicy
    sha256: str
    path: Path


@dataclass(frozen=True, slots=True)
class StrategyPolicyArtifact:
    policy: StrategyPolicy
    sha256: str
    path: Path


@dataclass(frozen=True, slots=True)
class OptionRuntimeConfiguration:
    settings: OptionSettings
    policy: DeveloperPolicy
    policy_sha256: str
    strategy_policy: StrategyPolicy
    strategy_policy_sha256: str
    configuration_sha256: str

    def metadata(self) -> dict[str, object]:
        return {
            **self.settings.fingerprint_payload(),
            "policy_version": self.policy.policy_version,
            "policy_schema_version": self.policy.schema_version,
            "policy_sha256": self.policy_sha256,
            "strategy_policy_version": self.strategy_policy.strategy_version,
            "strategy_policy_sha256": self.strategy_policy_sha256,
            "configuration_sha256": self.configuration_sha256,
        }


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def load_developer_policy(path: Path) -> PolicyArtifact:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load option policy from {path}") from exc
    policy = DeveloperPolicy.model_validate(payload)
    canonical_payload = policy.model_dump(mode="json")
    return PolicyArtifact(policy=policy, sha256=_sha256(canonical_payload), path=path)


def load_strategy_policy(path: Path) -> StrategyPolicyArtifact:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load option strategy policy from {path}") from exc
    policy = StrategyPolicy.model_validate(payload)
    canonical_payload = policy.model_dump(mode="json")
    return StrategyPolicyArtifact(policy=policy, sha256=_sha256(canonical_payload), path=path)


def load_option_runtime_configuration(
    environ: Mapping[str, str] | None = None,
    backend_dir: Path | None = None,
) -> OptionRuntimeConfiguration:
    backend_dir = (backend_dir or Path(__file__).resolve().parents[1]).resolve()
    if environ is None:
        load_dotenv(backend_dir / ".env", override=False)
        environ = os.environ

    values: dict[str, Any] = {
        "polygon_api_key": environ.get("POLYGON_API_KEY"),
        "data_engine": environ.get("OPTION_DATA_ENGINE", DataEngine.POLYGON_DEVELOPER.value),
        "underlying_data_provider": environ.get(
            "OPTION_UNDERLYING_DATA_PROVIDER", "polygon_stocks"
        ),
        "event_calendar_provider": environ.get("OPTION_EVENT_CALENDAR_PROVIDER"),
        "equity_context_enabled": environ.get("OPTION_EQUITY_CONTEXT_ENABLED", "false"),
        "execution_engine": environ.get(
            "OPTION_EXECUTION_ENGINE", ExecutionEngine.PAPER_PROXY.value
        ),
        "universe_mode": environ.get("OPTION_UNIVERSE_MODE", UniverseMode.FIXED.value),
        "fixed_stock_underlyers": environ.get(
            "OPTION_FIXED_STOCK_UNDERLYERS",
            "AAPL,AMD,AMZN,GOOGL,META,MSFT,NVDA,PLTR,SOFI,TSLA",
        ),
        "fixed_etf_underlyers": environ.get(
            "OPTION_FIXED_ETF_UNDERLYERS", "SPY,QQQ,IWM"
        ),
        "stock_universe_size": environ.get("OPTION_STOCK_UNIVERSE_SIZE", "10"),
        "etf_universe_size": environ.get("OPTION_ETF_UNIVERSE_SIZE", "3"),
        "poll_seconds": environ.get("OPTION_POLL_SECONDS", "900"),
        "starting_cash": environ.get("OPTION_STARTING_CASH", "250000"),
        "risk_free_rate": environ.get("OPTION_RISK_FREE_RATE", "0.04"),
        "risk_free_rate_source": environ.get(
            "OPTION_RISK_FREE_RATE_SOURCE", "manual_config_v1"
        ),
        "default_dividend_yield": environ.get("OPTION_DEFAULT_DIVIDEND_YIELD", "0"),
        "policy_file": environ.get(
            "OPTION_POLICY_FILE", "options/policies/developer_v1.json"
        ),
        "strategy_policy_file": environ.get(
            "OPTION_STRATEGY_POLICY_FILE", "options/policies/strategy_v1.json"
        ),
        "raw_archive_enabled": environ.get("OPTION_RAW_ARCHIVE_ENABLED", "false"),
        "raw_archive_root": environ.get("OPTION_RAW_ARCHIVE_ROOT", "option-raw"),
        "start_read_only": environ.get("OPTION_START_READ_ONLY", "true"),
    }
    settings = OptionSettings.model_validate(values)
    policy_path = settings.policy_file
    if not policy_path.is_absolute():
        policy_path = (backend_dir / policy_path).resolve()
    strategy_policy_path = settings.strategy_policy_file
    if not strategy_policy_path.is_absolute():
        strategy_policy_path = (backend_dir / strategy_policy_path).resolve()
    archive_root = settings.raw_archive_root
    if not archive_root.is_absolute():
        archive_root = (backend_dir / archive_root).resolve()
    settings = settings.model_copy(
        update={
            "policy_file": policy_path,
            "strategy_policy_file": strategy_policy_path,
            "raw_archive_root": archive_root,
        }
    )

    artifact = load_developer_policy(policy_path)
    strategy_artifact = load_strategy_policy(strategy_policy_path)
    configuration_payload = {
        **settings.fingerprint_payload(),
        "policy_version": artifact.policy.policy_version,
        "policy_schema_version": artifact.policy.schema_version,
        "policy_sha256": artifact.sha256,
    }
    return OptionRuntimeConfiguration(
        settings=settings,
        policy=artifact.policy,
        policy_sha256=artifact.sha256,
        strategy_policy=strategy_artifact.policy,
        strategy_policy_sha256=strategy_artifact.sha256,
        configuration_sha256=_sha256(configuration_payload),
    )