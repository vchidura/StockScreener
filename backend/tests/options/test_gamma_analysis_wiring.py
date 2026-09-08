import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.analysis_engine import OptionAnalysisEngine
from options.analytics.gamma_exposure import scope_contains
from options.config import GammaExposurePolicy, load_developer_policy
from options.domain import (
    AssetType,
    ContractType,
    DataQualityFlag,
    DecisionContext,
    ExerciseStyle,
    GammaScope,
    MarkSource,
    OptionContractSnapshot,
)

UTC = timezone.utc
MARKET_TIME = datetime(2026, 9, 4, 20, 0, tzinfo=UTC)
OBSERVED_TIME = datetime(2026, 9, 4, 20, 15, tzinfo=UTC)
BATCH_ID = uuid4()


def _gamma_policy(**overrides) -> GammaExposurePolicy:
    payload = {
        "schema_version": 1,
        "gamma_policy_version": "gamma_v1",
        "shares_per_contract": 100,
        "minimum_open_interest": 1,
        "maximum_dte": 45,
        "minimum_contracts_for_profile": 4,
        "volatility_assumption": "STICKY_STRIKE",
        "flip_search_fraction": 0.2,
        "flip_grid_points": 41,
        "default_convention": "DEALER_LONG_CALLS_SHORT_PUTS",
        "asset_type_conventions": {
            "ETF": "DEALER_LONG_CALLS_SHORT_PUTS",
            "STOCK": "DEALER_SHORT_CALLS_LONG_PUTS",
        },
        "underlyer_conventions": {},
    }
    payload.update(overrides)
    return GammaExposurePolicy.model_validate(payload)


def _developer_policy():
    return load_developer_policy(
        BACKEND_DIR / "options" / "policies" / "developer_v1.json"
    ).policy


def _snapshot(contract_id, contract_type, strike, calendar_dte, open_interest=500):
    expiration = MARKET_TIME.date() + timedelta(days=calendar_dte)
    return OptionContractSnapshot(
        snapshot_id=uuid4(),
        contract_id=contract_id,
        contract_ticker=f"O:SPY{contract_id:08d}",
        underlyer="SPY",
        provider="polygon",
        contract_type=contract_type,
        expiration_date=expiration,
        expiration_cutoff=datetime(
            expiration.year, expiration.month, expiration.day, 20, 0, tzinfo=UTC
        ),
        calendar_dte=calendar_dte,
        time_to_expiration_years=max(calendar_dte, 1) / 365,
        strike=Decimal(str(strike)),
        shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN,
        spot=Decimal("100"),
        spot_market_data_time=MARKET_TIME,
        bid=None,
        ask=None,
        midpoint=None,
        display_mark=Decimal("2.50"),
        model_mark=Decimal("2.50"),
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=100,
        open_interest=open_interest,
        market_data_time=MARKET_TIME,
        first_observed_at=OBSERVED_TIME,
        revised_observed_at=None,
        local_iv=0.25,
        local_gamma=0.02,
        local_delta=0.5,
        local_theta_per_day=-0.01,
        local_vega_per_vol_point=0.1,
        local_rho_per_rate_point=0.01,
        intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal("2.50"),
        single_contract_breakeven=Decimal("102.50"),
        provider_iv=None,
        provider_gamma=None,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        iv_converged=True,
        iv_solver="NEWTON",
        iv_iteration_count=3,
        iv_price_error=1e-9,
        iv_failure_reason=None,
        model_version="bs-1",
        quality_flags=(),
        batch_id=BATCH_ID,
        raw_payload_sha256="a" * 64,
        normalized_payload_sha256="b" * 64,
        revision=1,
    )


def _chain():
    snapshots = []
    contract_id = 1
    for calendar_dte in (0, 7, 30):
        for strike in (95, 98, 100, 102, 105):
            for contract_type in (ContractType.CALL, ContractType.PUT):
                snapshots.append(
                    _snapshot(contract_id, contract_type, strike, calendar_dte)
                )
                contract_id += 1
    return tuple(snapshots)


def _analyze(engine, snapshots=None, asset_type=AssetType.ETF):
    snapshots = snapshots if snapshots is not None else _chain()
    return engine.analyze(
        uuid4(),
        snapshots,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        received_count=len(snapshots),
        catalog_matched_count=len(snapshots),
        unknown_reference_count=0,
        reference_drift_failed=False,
        batch_complete=True,
        asset_type=asset_type,
    )


def test_engine_without_gamma_policy_produces_no_profiles():
    engine = OptionAnalysisEngine(_developer_policy())
    assert _analyze(engine).gamma_profiles == ()


def test_engine_emits_one_profile_per_scope():
    engine = OptionAnalysisEngine(_developer_policy(), _gamma_policy())
    profiles = _analyze(engine).gamma_profiles
    assert tuple(scoped.scope for scoped in profiles) == tuple(GammaScope)


def test_scope_partitions_contracts_without_overlap():
    engine = OptionAnalysisEngine(_developer_policy(), _gamma_policy())
    by_scope = {
        scoped.scope: scoped.profile for scoped in _analyze(engine).gamma_profiles
    }
    total = by_scope[GammaScope.TOTAL].contributing_contract_count
    partitioned = sum(
        by_scope[scope].contributing_contract_count
        for scope in (GammaScope.ZERO_DTE, GammaScope.WEEKLY, GammaScope.MONTHLY)
    )
    assert total == partitioned == 30


def test_zero_dte_scope_isolates_same_day_expiry():
    engine = OptionAnalysisEngine(_developer_policy(), _gamma_policy())
    by_scope = {
        scoped.scope: scoped.profile for scoped in _analyze(engine).gamma_profiles
    }
    assert by_scope[GammaScope.ZERO_DTE].contributing_contract_count == 10
    assert by_scope[GammaScope.WEEKLY].contributing_contract_count == 10
    assert by_scope[GammaScope.MONTHLY].contributing_contract_count == 10


def test_asset_type_selects_the_dealer_convention():
    engine = OptionAnalysisEngine(_developer_policy(), _gamma_policy())
    etf = _analyze(engine, asset_type=AssetType.ETF).gamma_profiles[0].profile
    stock = _analyze(engine, asset_type=AssetType.STOCK).gamma_profiles[0].profile
    assert etf.convention.value == "DEALER_LONG_CALLS_SHORT_PUTS"
    assert stock.convention.value == "DEALER_SHORT_CALLS_LONG_PUTS"


def test_underlyer_override_beats_asset_type():
    policy = _gamma_policy(
        underlyer_conventions={"SPY": "DEALER_SHORT_CALLS_LONG_PUTS"}
    )
    engine = OptionAnalysisEngine(_developer_policy(), policy)
    profile = _analyze(engine, asset_type=AssetType.ETF).gamma_profiles[0].profile
    assert profile.convention.value == "DEALER_SHORT_CALLS_LONG_PUTS"


def test_missing_open_interest_is_excluded_not_substituted():
    snapshots = list(_chain())
    snapshots[0] = _snapshot(
        snapshots[0].contract_id, ContractType.CALL, 95, 0, open_interest=None
    )
    engine = OptionAnalysisEngine(_developer_policy(), _gamma_policy())
    total = _analyze(engine, tuple(snapshots)).gamma_profiles[0].profile
    assert total.contributing_contract_count == 29
    assert total.eligible_contract_count == 30
    assert total.coverage_fraction == pytest.approx(29 / 30)


def test_scope_contains_boundaries():
    assert scope_contains(GammaScope.ZERO_DTE, 0)
    assert not scope_contains(GammaScope.ZERO_DTE, 1)
    assert scope_contains(GammaScope.WEEKLY, 1)
    assert scope_contains(GammaScope.WEEKLY, 14)
    assert not scope_contains(GammaScope.WEEKLY, 15)
    assert scope_contains(GammaScope.MONTHLY, 15)
    assert all(scope_contains(GammaScope.TOTAL, dte) for dte in (0, 1, 14, 15, 45))


def test_non_total_scope_requires_a_market_date():
    from options.analytics.gamma_exposure import GammaExposureInput, build_gamma_profile

    rows = (
        GammaExposureInput(
            contract_id=1,
            contract_type=ContractType.CALL,
            expiration_date=date(2026, 10, 2),
            strike=Decimal("100"),
            open_interest=100,
            local_iv=0.25,
            time_to_expiration_years=0.08,
            risk_free_rate=0.04,
            dividend_yield=0.0,
        ),
    )
    with pytest.raises(ValueError):
        build_gamma_profile(
            rows,
            Decimal("100"),
            convention=_gamma_policy().default_convention,
            scope=GammaScope.ZERO_DTE,
        )


def test_pipeline_persists_one_record_per_scope():
    from options.orchestration import ManualOptionPipeline

    engine = OptionAnalysisEngine(_developer_policy(), _gamma_policy())
    analysis = _analyze(engine)
    repository = MagicMock()
    pipeline = ManualOptionPipeline.__new__(ManualOptionPipeline)
    pipeline.gamma_repository = repository
    pipeline.configuration = MagicMock()
    pipeline.configuration.gamma_policy.gamma_policy_version = "gamma_v1"
    pipeline.configuration.gamma_policy_sha256 = "c" * 64

    pipeline._persist_gamma_profiles(analysis, "SPY", MARKET_TIME, OBSERVED_TIME)

    records = repository.persist.call_args[0][0]
    assert [record.scope for record in records] == list(GammaScope)
    assert len({record.gamma_profile_id for record in records}) == len(GammaScope)
    assert all(record.underlying == "SPY" for record in records)


def test_pipeline_skips_persistence_when_no_profiles():
    from options.orchestration import ManualOptionPipeline

    engine = OptionAnalysisEngine(_developer_policy())
    analysis = _analyze(engine)
    repository = MagicMock()
    pipeline = ManualOptionPipeline.__new__(ManualOptionPipeline)
    pipeline.gamma_repository = repository

    pipeline._persist_gamma_profiles(analysis, "SPY", MARKET_TIME, OBSERVED_TIME)
    repository.persist.assert_not_called()
