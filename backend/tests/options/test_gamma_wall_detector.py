import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.chain_analysis import ChainHealth
from options.analytics.gamma_exposure import (
    GammaExposureInput,
    ScopedGammaProfile,
    build_gamma_profile,
    detect_gamma_walls,
)
from options.config import load_gamma_policy, load_strategy_policy
from options.domain import (
    ContractType,
    DealerConvention,
    ExerciseStyle,
    GammaScope,
    MarkSource,
    OptionContractSnapshot,
)
from options.strategies.context import StrategyContextSnapshot
from options.strategies.domain import StrategyContextStatus
from options.strategies.engine import OptionStrategyEngine

UTC = timezone.utc
MARKET_TIME = datetime(2026, 9, 4, 18, 0, tzinfo=UTC)
OBSERVED_TIME = datetime(2026, 9, 4, 18, 15, tzinfo=UTC)
BATCH_ID = uuid4()
POLICY_DIR = BACKEND_DIR / "options" / "policies"


def _policy(name):
    artifact = load_strategy_policy(POLICY_DIR / name)
    return artifact.policy, artifact.sha256


def _gamma_policy(name):
    artifact = load_gamma_policy(POLICY_DIR / name)
    return artifact.policy, artifact.sha256


def _snapshot(contract_id, contract_type, strike, day_volume, open_interest=100):
    return OptionContractSnapshot(
        snapshot_id=uuid4(),
        contract_id=contract_id,
        contract_ticker=f"O:SPY{contract_id:08d}",
        underlyer="SPY",
        provider="polygon",
        contract_type=contract_type,
        expiration_date=MARKET_TIME.date(),
        expiration_cutoff=datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        calendar_dte=0,
        time_to_expiration_years=2 / (365 * 24),
        strike=Decimal(str(strike)),
        shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN,
        spot=Decimal("100"),
        spot_market_data_time=MARKET_TIME,
        bid=None,
        ask=None,
        midpoint=None,
        display_mark=Decimal("0.50"),
        model_mark=Decimal("0.50"),
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=day_volume,
        open_interest=open_interest,
        market_data_time=MARKET_TIME,
        first_observed_at=OBSERVED_TIME,
        revised_observed_at=None,
        local_iv=0.30,
        local_gamma=0.20,
        local_delta=0.5,
        local_theta_per_day=-0.05,
        local_vega_per_vol_point=0.01,
        local_rho_per_rate_point=0.001,
        intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal("0.50"),
        single_contract_breakeven=Decimal("100.50"),
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
    """Strike 100 is the wall: heavy volume; neighbours are quiet."""
    rows = []
    contract_id = 1
    for strike, volume in ((99, 40), (100, 4000), (101, 40)):
        for contract_type in (ContractType.CALL, ContractType.PUT):
            rows.append(_snapshot(contract_id, contract_type, strike, volume))
            contract_id += 1
    return tuple(rows)


def _gamma_input(contract_id, contract_type, strike, open_interest):
    return GammaExposureInput(
        contract_id=contract_id,
        contract_type=contract_type,
        expiration_date=MARKET_TIME.date(),
        strike=Decimal(str(strike)),
        open_interest=open_interest,
        local_iv=0.30,
        time_to_expiration_years=2 / (365 * 24),
        risk_free_rate=0.04,
        dividend_yield=0.0,
    )


def _profile(convention=DealerConvention.DEALER_SHORT_CALLS_LONG_PUTS):
    # Calls concentrated at the money produce negative dealer gamma under the
    # single-stock convention.
    rows = (
        _gamma_input(1, ContractType.CALL, 100, 20000),
        _gamma_input(2, ContractType.CALL, 99, 200),
        _gamma_input(3, ContractType.PUT, 101, 200),
    )
    return build_gamma_profile(
        rows,
        Decimal("100"),
        convention=convention,
        scope=GammaScope.ZERO_DTE,
        market_date=MARKET_TIME.date(),
        minimum_contracts_for_profile=1,
    )


def _scoped(profile=None, scope=GammaScope.ZERO_DTE):
    return (ScopedGammaProfile(scope=scope, profile=profile or _profile()),)


def _context(matrix_id):
    return StrategyContextSnapshot(
        context_snapshot_id=uuid4(),
        matrix_id=matrix_id,
        underlyer="SPY",
        market_data_time=MARKET_TIME,
        observed_time=OBSERVED_TIME,
        status=StrategyContextStatus.COMPLETE,
        daily_close=None,
        daily_ema_50=None,
        daily_input_bars=0,
        hourly_close=None,
        hourly_ema_20=None,
        hourly_input_bars=0,
        trend_state="NEUTRAL",
        earnings_blackout_state="UNAVAILABLE",
        fed_blackout_state="UNAVAILABLE",
        quote_spread_state="UNAVAILABLE",
        reason_codes=(),
        source_bar_keys=(),
        policy_version="phase2",
        policy_sha256="d" * 64,
    )


def _health():
    return ChainHealth(
        received_count=6,
        retained_count=6,
        catalog_coverage_fraction=1.0,
        mark_alignment_fraction=1.0,
        iv_convergence_fraction=1.0,
        unknown_reference_fraction=0.0,
        rejection_fraction=0.0,
        status="COMPLETE",
        reasons=(),
    )


def _engine(gamma_policy_name):
    policy, sha = _policy("strategy_v1.json")
    gamma_policy, gamma_sha = _gamma_policy(gamma_policy_name)
    return OptionStrategyEngine(policy, sha, gamma_policy, gamma_sha)


def _scan(gamma_policy_name, gamma_profiles):
    engine = _engine(gamma_policy_name)
    matrix_id = uuid4()
    return engine.scan(
        matrix_id,
        _chain(),
        _health(),
        (),
        _context(matrix_id),
        (),
        gamma_profiles,
    )


def _squeeze(result):
    return tuple(
        candidate
        for candidate in result.candidates
        if candidate.strategy_name == "ZERO_DTE_GAMMA_SQUEEZE"
    )


def test_v1_policy_keeps_per_contract_selection_without_a_profile():
    candidates = _squeeze(_scan("gamma_policy_v1.json", ()))
    assert any(candidate.status.value == "SELECTED" for candidate in candidates)


def test_strategy_policy_identity_is_pinned():
    # Deliberate bumps go here together with a strategy_version change; the repository
    # rejects a hash move without one. Last bumped for phase2_v4 (wheel entry boundary).
    policy, sha = _policy("strategy_v1.json")
    assert policy.strategy_version == "phase2_v4"
    assert sha == "c4a46268cbef4cb44994fe5524dc2499ef90d36853cd3d50b07a3126a31e6bc6"


def test_wall_gates_are_absent_from_the_strategy_policy():
    policy, _ = _policy("strategy_v1.json")
    assert not hasattr(policy.gamma_squeeze, "require_gamma_wall")


def test_default_gamma_policy_leaves_the_gates_off():
    policy, _ = _gamma_policy("gamma_policy_v1.json")
    assert policy.require_gamma_wall is False
    assert policy.required_regime is None


def test_wall_gamma_policy_enables_the_gates():
    policy, _ = _gamma_policy("gamma_policy_v2_walls.json")
    assert policy.require_gamma_wall is True
    assert policy.required_regime.value == "NEGATIVE_GAMMA"
    assert policy.gamma_wall_scope is GammaScope.ZERO_DTE


def test_disabled_gates_leave_the_strategy_identity_unchanged():
    _, sha = _policy("strategy_v1.json")
    assert _engine("gamma_policy_v1.json").policy_sha256 == sha


def test_enabled_gates_change_the_strategy_identity():
    _, sha = _policy("strategy_v1.json")
    engine = _engine("gamma_policy_v2_walls.json")
    assert engine.policy_sha256 != sha
    assert len(engine.policy_sha256) == 64


def test_enabled_gates_require_the_gamma_policy_hash():
    policy, sha = _policy("strategy_v1.json")
    gamma_policy, _ = _gamma_policy("gamma_policy_v2_walls.json")
    with pytest.raises(ValueError, match="gamma policy hash"):
        OptionStrategyEngine(policy, sha, gamma_policy)


def test_gamma_policies_have_distinct_identities():
    v1 = load_gamma_policy(POLICY_DIR / "gamma_policy_v1.json").sha256
    v2 = load_gamma_policy(POLICY_DIR / "gamma_policy_v2_walls.json").sha256
    assert v1 != v2


def test_engine_without_a_gamma_policy_keeps_v1_behaviour():
    policy, sha = _policy("strategy_v1.json")
    engine = OptionStrategyEngine(policy, sha)
    matrix_id = uuid4()
    result = engine.scan(
        matrix_id, _chain(), _health(), (), _context(matrix_id), (), ()
    )
    assert any(
        candidate.status.value == "SELECTED" for candidate in _squeeze(result)
    )


def test_v2_suppresses_when_no_profile_is_supplied():
    candidates = _squeeze(_scan("gamma_policy_v2_walls.json", ()))
    assert len(candidates) == 1
    assert candidates[0].status.value == "SUPPRESSED"
    assert "GAMMA_PROFILE_UNAVAILABLE" in candidates[0].reason_codes


def test_v2_suppresses_when_the_regime_is_not_squeeze_prone():
    # The ETF convention flips the sign, producing positive dealer gamma.
    positive = _profile(convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS)
    candidates = _squeeze(_scan("gamma_policy_v2_walls.json", _scoped(positive)))
    assert candidates[0].status.value == "SUPPRESSED"
    assert "GAMMA_REGIME_NOT_SQUEEZE_PRONE" in candidates[0].reason_codes


def test_v2_suppresses_when_the_requested_scope_is_missing():
    candidates = _squeeze(
        _scan("gamma_policy_v2_walls.json", _scoped(scope=GammaScope.MONTHLY))
    )
    assert "GAMMA_PROFILE_UNAVAILABLE" in candidates[0].reason_codes


def test_v2_selects_at_a_proximate_wall_with_a_volume_surge():
    candidates = _squeeze(_scan("gamma_policy_v2_walls.json", _scoped()))
    selected = [c for c in candidates if c.status.value == "SELECTED"]
    assert selected
    for candidate in selected:
        assert candidate.legs[0].strike == Decimal("100")


def test_v2_records_wall_evidence_on_the_candidate():
    candidates = _squeeze(_scan("gamma_policy_v2_walls.json", _scoped()))
    evidence = [c for c in candidates if c.status.value == "SELECTED"][0].primary_evidence
    assert evidence["gamma_wall_strike"] == "100"
    assert evidence["gamma_regime"] == "NEGATIVE_GAMMA"
    assert evidence["gamma_scope"] == "ZERO_DTE"
    assert evidence["dealer_convention"] == "DEALER_SHORT_CALLS_LONG_PUTS"
    assert evidence["gamma_wall_share"] > 0.05
    assert evidence["volume_surge_ratio"] >= 2.0


def test_wall_detection_is_convention_independent():
    negative = _profile(DealerConvention.DEALER_SHORT_CALLS_LONG_PUTS)
    positive = _profile(DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS)
    assert detect_gamma_walls(negative, minimum_share=0.05) == detect_gamma_walls(
        positive, minimum_share=0.05
    )


def test_wall_proximity_filter_excludes_distant_strikes():
    # A 5% away strike only carries meaningful gamma with real time left, so this
    # uses a monthly maturity rather than the 0-DTE fixture.
    rows = tuple(
        GammaExposureInput(
            contract_id=index,
            contract_type=ContractType.CALL,
            expiration_date=MARKET_TIME.date() + timedelta(days=30),
            strike=Decimal(str(strike)),
            open_interest=10000,
            local_iv=0.30,
            time_to_expiration_years=30 / 365,
            risk_free_rate=0.04,
            dividend_yield=0.0,
        )
        for index, strike in enumerate((100, 105), start=1)
    )
    profile = build_gamma_profile(
        rows,
        Decimal("100"),
        convention=DealerConvention.DEALER_SHORT_CALLS_LONG_PUTS,
        minimum_contracts_for_profile=1,
    )
    far = detect_gamma_walls(profile, minimum_share=0.01)
    near = detect_gamma_walls(
        profile, minimum_share=0.01, maximum_distance_fraction=0.01
    )
    assert len(far) == 2
    assert len(near) == 1
    assert near[0].strike == Decimal("100")


def test_wall_share_is_rejected_outside_the_unit_interval():
    with pytest.raises(ValueError):
        detect_gamma_walls(_profile(), minimum_share=0.0)
    with pytest.raises(ValueError):
        detect_gamma_walls(_profile(), minimum_share=1.5)
