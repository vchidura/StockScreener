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

from options.domain import (
    DealerConvention,
    GammaRegime,
    GammaScope,
    MarkSource,
    VarianceEstimator,
    VarianceSource,
    VolatilityAssumption,
)


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
    event_calendar_max_age_seconds: int = Field(default=43200, ge=3600)
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
    dividend_input_source: str | None = None
    dividend_input_max_age_seconds: int = Field(default=43200, ge=3600)
    policy_file: Path = Path("options/policies/developer_v1.json")
    valuation_policy_file: Path = Path("options/policies/valuation_v1.json")
    settlement_valuation_policy_file: Path = Path(
        "options/policies/settlement_valuation_v1.json"
    )
    strategy_policy_file: Path = Path("options/policies/strategy_v1.json")
    gamma_policy_file: Path = Path("options/policies/gamma_policy_v1.json")
    raw_archive_enabled: bool = False
    raw_archive_root: Path = Path("option-raw")
    start_read_only: bool = True
    # Beyond this the run is behind its slot far enough that marks begin falling outside
    # the source-age window, so the matrix is degraded rather than trusted.
    maximum_execution_lag_seconds: int = Field(default=1800, gt=0)
    # Trade ingestion is opt-in because enabling it multiplies provider requests per
    # cycle. These knobs are intentionally absent from fingerprint_payload: they bound
    # operational cost, and the watchlist rule must be versioned separately before any
    # measured study depends on sweep detections.
    trade_ingestion_enabled: bool = False
    trade_watchlist_per_underlyer: int = Field(default=15, gt=0, le=200)
    trade_lookback_seconds: int = Field(default=3600, gt=0)
    trade_ingestion_budget_seconds: int = Field(default=120, gt=0)

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

    @field_validator(
        "underlying_data_provider", "event_calendar_provider", "dividend_input_source"
    )
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
            "event_calendar_max_age_seconds": self.event_calendar_max_age_seconds,
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
            "dividend_input_source": self.dividend_input_source,
            "dividend_input_max_age_seconds": self.dividend_input_max_age_seconds,
            "raw_archive_enabled": self.raw_archive_enabled,
            "raw_archive_root": str(self.raw_archive_root),
            "start_read_only": self.start_read_only,
            "maximum_execution_lag_seconds": self.maximum_execution_lag_seconds,
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
    minimum_iv_success_fraction: Decimal = Field(ge=0, le=1)
    minimum_iv: Decimal = Field(gt=0)
    maximum_iv: Decimal = Field(gt=0)
    newton_iterations: int = Field(gt=0)
    price_error_tolerance: float = Field(gt=0)
    minimum_vega: float = Field(gt=0)
    use_brent_fallback: bool

    @model_validator(mode="after")
    def _validate_iv_range(self) -> "ModelQualityPolicy":
        if self.maximum_iv <= self.minimum_iv:
            raise ValueError("maximum_iv must be greater than minimum_iv")
        return self


class ValuationPolicy(_FrozenModel):
    schema_version: int = Field(gt=0)
    policy_version: str = Field(min_length=1)
    primary_model_mark_source: MarkSource
    maximum_source_age_seconds: int = Field(gt=0)
    maximum_option_spot_skew_seconds: int = Field(ge=0)
    intrinsic_price_tolerance: Decimal = Field(ge=0)
    allowed_entry_mark_sources: tuple[MarkSource, ...]
    allowed_exit_mark_sources: tuple[MarkSource, ...]
    display_only_mark_sources: tuple[MarkSource, ...]
    commission_per_contract_per_side: Decimal = Field(ge=0)
    slippage_model: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_sources(self) -> "ValuationPolicy":
        entry_sources = set(self.allowed_entry_mark_sources)
        exit_sources = set(self.allowed_exit_mark_sources)
        display_sources = set(self.display_only_mark_sources)
        if self.primary_model_mark_source not in entry_sources:
            raise ValueError("primary model mark source must be allowed for entry")
        if self.primary_model_mark_source not in exit_sources:
            raise ValueError("primary model mark source must be allowed for exit")
        if (entry_sources | exit_sources) & display_sources:
            raise ValueError("valuation and display-only mark sources must not overlap")
        return self

    @property
    def policy_sha256(self) -> str:
        return _sha256(self.model_dump(mode="json"))


class SettlementValuationPolicy(_FrozenModel):
    schema_version: int = Field(gt=0)
    policy_version: str = Field(min_length=1)
    mark_source: str = Field(min_length=1)
    option_aggregates_adjusted: bool
    underlying_closes_adjusted: bool
    price_field: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    iv_context_calculation_version: str = Field(min_length=1)
    iv_lookback_sessions: int = Field(gt=0)
    minimum_iv_sample_sessions: int = Field(gt=0)
    minimum_iv_coverage_fraction: float = Field(gt=0, le=1)

    @model_validator(mode="after")
    def _require_nominal_price_basis(self) -> "SettlementValuationPolicy":
        if self.option_aggregates_adjusted or self.underlying_closes_adjusted:
            raise ValueError("settlement IV requires unadjusted option and underlying prices")
        if self.price_field != "close":
            raise ValueError("settlement valuation price_field must be close")
        if self.minimum_iv_sample_sessions > self.iv_lookback_sessions:
            raise ValueError("minimum IV samples cannot exceed lookback sessions")
        return self

    @property
    def policy_sha256(self) -> str:
        return _sha256(self.model_dump(mode="json"))


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


class LongPremiumPolicy(_FrozenModel):
    minimum_dte: int = Field(ge=1)
    maximum_dte: int = Field(gt=1)
    near_lane_maximum_dte: int = Field(gt=0)
    short_lane_maximum_dte: int = Field(gt=0)
    minimum_absolute_delta: float = Field(gt=0, lt=1)
    maximum_absolute_delta: float = Field(gt=0, lt=1)
    # Breakeven distance divided by the one-sigma move implied over the option's life.
    # At 1.0 the contract only breaks even on a one-standard-deviation move.
    maximum_breakeven_expected_move_ratio: float = Field(gt=0)
    minimum_open_interest: int = Field(ge=0)
    minimum_day_volume: int = Field(ge=0)
    maximum_candidates_per_lane_side: int = Field(gt=0)

    @model_validator(mode="after")
    def _validate_bands(self) -> "LongPremiumPolicy":
        if self.maximum_dte < self.minimum_dte:
            raise ValueError("long premium maximum_dte must not be less than minimum_dte")
        if self.maximum_absolute_delta <= self.minimum_absolute_delta:
            raise ValueError("long premium delta band must be increasing")
        if not self.minimum_dte <= self.near_lane_maximum_dte <= self.short_lane_maximum_dte:
            raise ValueError("long premium lane boundaries must be ordered")
        if self.short_lane_maximum_dte > self.maximum_dte:
            raise ValueError("long premium lanes must fit inside the DTE range")
        return self


class DebitSpreadPolicy(_FrozenModel):
    minimum_dte: int = Field(ge=1)
    maximum_dte: int = Field(gt=1)
    near_lane_maximum_dte: int = Field(gt=0)
    short_lane_maximum_dte: int = Field(gt=0)
    minimum_long_absolute_delta: float = Field(gt=0, lt=1)
    maximum_long_absolute_delta: float = Field(gt=0, lt=1)
    minimum_width_fraction: float = Field(gt=0, lt=1)
    maximum_width_fraction: float = Field(gt=0, lt=1)
    # Same metric as the long-premium module so the two directional families rank
    # on one comparable scale, measured against the spread breakeven.
    maximum_breakeven_expected_move_ratio: float = Field(gt=0)
    # Distance to the short strike divided by the implied move. Above 1.0 the
    # maximum profit is only reached on a larger than one-sigma move.
    maximum_target_expected_move_ratio: float = Field(gt=0)
    minimum_return_on_risk: float = Field(gt=0)
    minimum_open_interest: int = Field(ge=0)
    minimum_day_volume: int = Field(ge=0)
    maximum_candidates_per_lane_side: int = Field(gt=0)

    @model_validator(mode="after")
    def _validate_bands(self) -> "DebitSpreadPolicy":
        if self.maximum_dte < self.minimum_dte:
            raise ValueError("debit spread maximum_dte must not be less than minimum_dte")
        if self.maximum_long_absolute_delta <= self.minimum_long_absolute_delta:
            raise ValueError("debit spread long delta band must be increasing")
        if self.maximum_width_fraction <= self.minimum_width_fraction:
            raise ValueError("debit spread width band must be increasing")
        if not self.minimum_dte <= self.near_lane_maximum_dte <= self.short_lane_maximum_dte:
            raise ValueError("debit spread lane boundaries must be ordered")
        if self.short_lane_maximum_dte > self.maximum_dte:
            raise ValueError("debit spread lanes must fit inside the DTE range")
        return self


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
    long_premium: LongPremiumPolicy
    debit_spread: DebitSpreadPolicy
    flow: FlowStrategyPolicy
    smile: SmileStrategyPolicy
    scenarios: ScenarioPolicy


class GammaExposurePolicy(_FrozenModel):
    schema_version: int = Field(gt=0)
    gamma_policy_version: str = Field(min_length=1)
    shares_per_contract: int = Field(gt=0)
    minimum_open_interest: int = Field(ge=0)
    maximum_dte: int = Field(ge=0)
    minimum_contracts_for_profile: int = Field(gt=0)
    volatility_assumption: VolatilityAssumption
    flip_search_fraction: float = Field(gt=0, lt=1)
    flip_grid_points: int = Field(ge=3)
    default_convention: DealerConvention
    asset_type_conventions: dict[str, DealerConvention]
    underlyer_conventions: dict[str, DealerConvention]
    # Squeeze-detector gates. These live here rather than on the strategy policy so
    # revising them cannot alter an already-published strategy policy identity.
    require_gamma_wall: bool = False
    gamma_wall_scope: GammaScope = GammaScope.ZERO_DTE
    wall_proximity_fraction: float = Field(default=0.01, gt=0, lt=1)
    minimum_wall_gamma_share: float = Field(default=0.05, gt=0, le=1)
    minimum_volume_surge_ratio: float = Field(default=2.0, gt=0)
    required_regime: GammaRegime | None = None

    @field_validator("asset_type_conventions", "underlyer_conventions", mode="before")
    @classmethod
    def _normalize_keys(cls, value: object) -> object:
        if isinstance(value, Mapping):
            return {str(key).strip().upper(): item for key, item in value.items()}
        return value

    @model_validator(mode="after")
    def _validate_grid(self) -> "GammaExposurePolicy":
        if self.flip_grid_points % 2 == 0:
            raise ValueError("flip_grid_points must be odd so the grid includes spot")
        return self

    def convention_for(self, underlyer: str, asset_type: str | None) -> DealerConvention:
        override = self.underlyer_conventions.get(underlyer.strip().upper())
        if override is not None:
            return override
        if asset_type is not None:
            by_asset = self.asset_type_conventions.get(asset_type.strip().upper())
            if by_asset is not None:
                return by_asset
        return self.default_convention


class VolatilityForecastPolicy(_FrozenModel):
    """Specification of the realized-volatility forecast.

    Carries its own hash, deliberately outside `configuration_sha256`, so revising the
    forecast cannot invalidate ingestion identity or orphan the strategy cohort.
    """

    schema_version: int = Field(gt=0)
    forecast_policy_version: str = Field(min_length=1)

    variance_source: VarianceSource
    daily_estimator: VarianceEstimator
    intraday_interval: str = Field(min_length=2)
    # Summing the overnight gap into session variance adds a single squared return to an
    # otherwise low-noise measurement, and that noise measurably degrades the forecast.
    include_overnight_variance: bool

    har_lags: tuple[int, ...]
    horizon_sessions: int = Field(gt=0)
    minimum_training_sessions: int = Field(gt=0)
    refit_every_sessions: int = Field(gt=0)

    minimum_session_bars: int = Field(gt=0)
    maximum_session_gap_days: int = Field(gt=0)
    session_scope: str = Field(min_length=1)
    adjusted_bars_for_label: bool
    trading_days_per_year: int = Field(gt=0)

    @field_validator("har_lags")
    @classmethod
    def _validate_lags(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if len(value) < 2:
            raise ValueError("har_lags needs at least two horizons")
        if any(lag <= 0 for lag in value):
            raise ValueError("har_lags must be positive")
        if list(value) != sorted(set(value)):
            raise ValueError("har_lags must be strictly increasing")
        return value

    @model_validator(mode="after")
    def _validate_source(self) -> "VolatilityForecastPolicy":
        if (
            self.variance_source is VarianceSource.INTRADAY_PATH
            and self.include_overnight_variance
        ):
            # Permitted by the model but measured to be worse; require it to be explicit
            # rather than an accident of copying the daily configuration.
            raise ValueError(
                "intraday variance with overnight summed in measured worse than "
                "open-to-close alone; set include_overnight_variance false"
            )
        return self


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
class GammaPolicyArtifact:
    policy: GammaExposurePolicy
    sha256: str
    path: Path


@dataclass(frozen=True, slots=True)
class VolatilityForecastPolicyArtifact:
    policy: VolatilityForecastPolicy
    sha256: str
    path: Path


@dataclass(frozen=True, slots=True)
class OptionRuntimeConfiguration:
    settings: OptionSettings
    policy: DeveloperPolicy
    developer_policy_sha256: str
    policy_sha256: str
    valuation_policy: ValuationPolicy
    valuation_policy_sha256: str
    settlement_valuation_policy: SettlementValuationPolicy
    settlement_valuation_policy_sha256: str
    strategy_policy: StrategyPolicy
    strategy_policy_sha256: str
    gamma_policy: GammaExposurePolicy
    gamma_policy_sha256: str
    configuration_sha256: str

    def metadata(self) -> dict[str, object]:
        return {
            **self.settings.fingerprint_payload(),
            "policy_version": self.policy.policy_version,
            "policy_schema_version": self.policy.schema_version,
            "developer_policy_sha256": self.developer_policy_sha256,
            "policy_sha256": self.policy_sha256,
            "valuation_policy_version": self.valuation_policy.policy_version,
            "valuation_policy_sha256": self.valuation_policy_sha256,
            "settlement_valuation_policy_version": (
                self.settlement_valuation_policy.policy_version
            ),
            "settlement_valuation_policy_sha256": (
                self.settlement_valuation_policy_sha256
            ),
            "strategy_policy_version": self.strategy_policy.strategy_version,
            "strategy_policy_sha256": self.strategy_policy_sha256,
            "gamma_policy_version": self.gamma_policy.gamma_policy_version,
            "gamma_policy_sha256": self.gamma_policy_sha256,
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


def load_valuation_policy(path: Path) -> ValuationPolicy:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load option valuation policy from {path}") from exc
    return ValuationPolicy.model_validate(payload)


def load_settlement_valuation_policy(path: Path) -> SettlementValuationPolicy:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"unable to load option settlement valuation policy from {path}"
        ) from exc
    return SettlementValuationPolicy.model_validate(payload)


def load_strategy_policy(path: Path) -> StrategyPolicyArtifact:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load option strategy policy from {path}") from exc
    policy = StrategyPolicy.model_validate(payload)
    canonical_payload = policy.model_dump(mode="json")
    return StrategyPolicyArtifact(policy=policy, sha256=_sha256(canonical_payload), path=path)


def load_gamma_policy(path: Path) -> GammaPolicyArtifact:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load option gamma policy from {path}") from exc
    policy = GammaExposurePolicy.model_validate(payload)
    canonical_payload = policy.model_dump(mode="json")
    return GammaPolicyArtifact(policy=policy, sha256=_sha256(canonical_payload), path=path)


def load_volatility_forecast_policy(path: Path) -> VolatilityForecastPolicyArtifact:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to load volatility forecast policy from {path}") from exc
    policy = VolatilityForecastPolicy.model_validate(payload)
    canonical_payload = policy.model_dump(mode="json")
    return VolatilityForecastPolicyArtifact(
        policy=policy, sha256=_sha256(canonical_payload), path=path
    )


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
        "event_calendar_max_age_seconds": environ.get(
            "OPTION_EVENT_CALENDAR_MAX_AGE_SECONDS", "43200"
        ),
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
        "dividend_input_source": environ.get("OPTION_DIVIDEND_INPUT_SOURCE"),
        "dividend_input_max_age_seconds": environ.get(
            "OPTION_DIVIDEND_INPUT_MAX_AGE_SECONDS", "43200"
        ),
        "policy_file": environ.get(
            "OPTION_POLICY_FILE", "options/policies/developer_v1.json"
        ),
        "valuation_policy_file": environ.get(
            "OPTION_VALUATION_POLICY_FILE", "options/policies/valuation_v1.json"
        ),
        "settlement_valuation_policy_file": environ.get(
            "OPTION_SETTLEMENT_VALUATION_POLICY_FILE",
            "options/policies/settlement_valuation_v1.json",
        ),
        "strategy_policy_file": environ.get(
            "OPTION_STRATEGY_POLICY_FILE", "options/policies/strategy_v1.json"
        ),
        "gamma_policy_file": environ.get(
            "OPTION_GAMMA_POLICY_FILE", "options/policies/gamma_policy_v1.json"
        ),
        "raw_archive_enabled": environ.get("OPTION_RAW_ARCHIVE_ENABLED", "false"),
        "raw_archive_root": environ.get("OPTION_RAW_ARCHIVE_ROOT", "option-raw"),
        "start_read_only": environ.get("OPTION_START_READ_ONLY", "true"),
        "maximum_execution_lag_seconds": environ.get(
            "OPTION_MAXIMUM_EXECUTION_LAG_SECONDS", "1800"
        ),
        "trade_ingestion_enabled": environ.get(
            "OPTION_TRADE_INGESTION_ENABLED", "false"
        ),
        "trade_watchlist_per_underlyer": environ.get(
            "OPTION_TRADE_WATCHLIST_PER_UNDERLYER", "15"
        ),
        "trade_lookback_seconds": environ.get("OPTION_TRADE_LOOKBACK_SECONDS", "3600"),
        "trade_ingestion_budget_seconds": environ.get(
            "OPTION_TRADE_INGESTION_BUDGET_SECONDS", "120"
        ),
    }
    settings = OptionSettings.model_validate(values)
    policy_path = settings.policy_file
    if not policy_path.is_absolute():
        policy_path = (backend_dir / policy_path).resolve()
    valuation_policy_path = settings.valuation_policy_file
    if not valuation_policy_path.is_absolute():
        valuation_policy_path = (backend_dir / valuation_policy_path).resolve()
    settlement_valuation_policy_path = settings.settlement_valuation_policy_file
    if not settlement_valuation_policy_path.is_absolute():
        settlement_valuation_policy_path = (
            backend_dir / settlement_valuation_policy_path
        ).resolve()
    strategy_policy_path = settings.strategy_policy_file
    if not strategy_policy_path.is_absolute():
        strategy_policy_path = (backend_dir / strategy_policy_path).resolve()
    gamma_policy_path = settings.gamma_policy_file
    if not gamma_policy_path.is_absolute():
        gamma_policy_path = (backend_dir / gamma_policy_path).resolve()
    archive_root = settings.raw_archive_root
    if not archive_root.is_absolute():
        archive_root = (backend_dir / archive_root).resolve()
    settings = settings.model_copy(
        update={
            "policy_file": policy_path,
            "valuation_policy_file": valuation_policy_path,
            "settlement_valuation_policy_file": settlement_valuation_policy_path,
            "strategy_policy_file": strategy_policy_path,
            "gamma_policy_file": gamma_policy_path,
            "raw_archive_root": archive_root,
        }
    )

    artifact = load_developer_policy(policy_path)
    valuation_policy = load_valuation_policy(valuation_policy_path)
    settlement_valuation_policy = load_settlement_valuation_policy(
        settlement_valuation_policy_path
    )
    strategy_artifact = load_strategy_policy(strategy_policy_path)
    gamma_artifact = load_gamma_policy(gamma_policy_path)
    configuration_payload = {
        **settings.fingerprint_payload(),
        "policy_version": artifact.policy.policy_version,
        "policy_schema_version": artifact.policy.schema_version,
        "developer_policy_sha256": artifact.sha256,
        "valuation_policy_sha256": valuation_policy.policy_sha256,
    }
    policy_sha256 = _sha256(
        {
            "developer_policy_sha256": artifact.sha256,
            "valuation_policy_sha256": valuation_policy.policy_sha256,
        }
    )
    return OptionRuntimeConfiguration(
        settings=settings,
        policy=artifact.policy,
        developer_policy_sha256=artifact.sha256,
        policy_sha256=policy_sha256,
        valuation_policy=valuation_policy,
        valuation_policy_sha256=valuation_policy.policy_sha256,
        settlement_valuation_policy=settlement_valuation_policy,
        settlement_valuation_policy_sha256=(
            settlement_valuation_policy.policy_sha256
        ),
        strategy_policy=strategy_artifact.policy,
        strategy_policy_sha256=strategy_artifact.sha256,
        gamma_policy=gamma_artifact.policy,
        gamma_policy_sha256=gamma_artifact.sha256,
        configuration_sha256=_sha256(configuration_payload),
    )