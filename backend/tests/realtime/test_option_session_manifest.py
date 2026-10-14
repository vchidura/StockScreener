from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from options.domain import (
    AssetType,
    CatalogEligibility,
    ContractType,
    ExerciseStyle,
    MarkSource,
    OptionContractCatalogEntry,
    OptionContractSnapshot,
)
from realtime.contracts import CANARY_UNIVERSE, DEVELOPER_FORWARD_UNIVERSE, AssetClass, MarketChannel, UniverseManifest
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_session_manifest import (
    BoundedOptionManifestPolicy,
    ContractSelectionStatus,
    ManifestSelectionStatus,
    OptionManifestCandidate,
    build_bounded_option_session_manifest,
)
from realtime.preflight import DetectorFamily, ProviderTier


UTC = timezone.utc
MARKET_TIME = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
SESSION_DATE = MARKET_TIME.date()


def _scope(provider_tier, universe):
    return RealtimeOptionDiscoveryScope(
        provider_tier,
        uuid5(NAMESPACE_URL, f"selection-profile:{provider_tier.value}:{universe.sha256}"),
        ("d" if provider_tier is ProviderTier.MASSIVE_DEVELOPER else "a") * 64,
        universe,
    )


def _developer_policy():
    return BoundedOptionManifestPolicy(
        version="developer-strategy-coverage-v1",
        target_dtes=(7, 21, 45, 60),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=2,
        minimum_dte=1,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.15"),
        minimum_day_volume=20,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=16,
        maximum_option_quote_contracts=1_000,
        reserved_option_quote_contracts=200,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O3,
        ),
        unavailable_detectors=(DetectorFamily.O2,),
    )


def _advanced_policy():
    return BoundedOptionManifestPolicy(
        version="advanced-initial-smile-v1",
        target_dtes=(21,),
        maximum_target_dte_deviation=0,
        strikes_per_side_per_expiry=8,
        minimum_dte=1,
        maximum_dte=60,
        strike_corridor_fraction=Decimal("0.20"),
        minimum_day_volume=20,
        minimum_open_interest=100,
        maximum_contracts_per_underlying=16,
        maximum_option_quote_contracts=1_000,
        reserved_option_quote_contracts=200,
        required_detectors=(
            DetectorFamily.S1,
            DetectorFamily.S2,
            DetectorFamily.O1,
            DetectorFamily.O2,
            DetectorFamily.O3,
        ),
    )


def _candidate(
    ticker,
    dte,
    contract_type,
    strike,
    identity,
    *,
    spot=Decimal("100"),
    day_volume=50,
    open_interest=200,
):
    expiration = SESSION_DATE + timedelta(days=dte)
    side = "C" if contract_type is ContractType.CALL else "P"
    strike_code = int(strike * 1000)
    contract_ticker = f"O:{ticker}{dte:03d}{side}{strike_code:08d}"
    contract_id = identity + 1
    observed_at = MARKET_TIME + timedelta(seconds=1)
    catalog = OptionContractCatalogEntry(
        contract_id=contract_id,
        contract_ticker=contract_ticker,
        underlyer=ticker,
        asset_type=AssetType.STOCK,
        provider="polygon",
        provider_version="1",
        contract_type=contract_type,
        expiration_date=expiration,
        strike=strike,
        exercise_style=ExerciseStyle.AMERICAN,
        shares_per_contract=100,
        primary_exchange="X",
        eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
        exclusion_reasons=(),
        valid_from=MARKET_TIME - timedelta(days=1),
        valid_to=None,
        first_observed_at=MARKET_TIME - timedelta(days=1),
        revised_observed_at=None,
        payload_sha256=f"{contract_id:064x}",
    )
    model_mark = Decimal("2.50")
    snapshot = OptionContractSnapshot(
        snapshot_id=uuid5(NAMESPACE_URL, f"selection-snapshot:{contract_id}"),
        contract_id=contract_id,
        contract_ticker=contract_ticker,
        underlyer=ticker,
        provider="polygon",
        contract_type=contract_type,
        expiration_date=expiration,
        expiration_cutoff=datetime.combine(expiration, time(20), tzinfo=UTC),
        calendar_dte=dte,
        time_to_expiration_years=dte / 365,
        strike=strike,
        shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN,
        spot=spot,
        spot_market_data_time=MARKET_TIME,
        bid=None,
        ask=None,
        midpoint=None,
        display_mark=model_mark,
        model_mark=model_mark,
        mark_market_data_time=MARKET_TIME,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=day_volume,
        open_interest=open_interest,
        market_data_time=MARKET_TIME,
        first_observed_at=observed_at,
        revised_observed_at=None,
        local_iv=0.30,
        local_gamma=0.02,
        local_delta=0.50,
        local_theta_per_day=-0.01,
        local_vega_per_vol_point=0.10,
        local_rho_per_rate_point=0.001,
        intrinsic_value=Decimal("0"),
        extrinsic_value=model_mark,
        single_contract_breakeven=strike + model_mark,
        provider_iv=None,
        provider_gamma=None,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        iv_converged=True,
        iv_solver="NEWTON",
        iv_iteration_count=3,
        iv_price_error=1e-9,
        iv_failure_reason=None,
        model_version="selection-fixture-v1",
        quality_flags=(),
        batch_id=uuid5(NAMESPACE_URL, f"selection-batch:{ticker}"),
        raw_payload_sha256=f"{contract_id + 100_000:064x}",
        normalized_payload_sha256=f"{contract_id + 200_000:064x}",
    )
    return OptionManifestCandidate(
        catalog_version_id=contract_id + 300_000,
        catalog=catalog,
        snapshot=snapshot,
        discovery_evidence_sha256=f"{identity + 400_000:064x}",
    )


def _candidates(universe, target_dtes, strikes_per_side, *, spot=Decimal("100")):
    rows = []
    identity = 1
    offsets = tuple(
        Decimal(index) - Decimal(strikes_per_side - 1) / Decimal("2")
        for index in range(strikes_per_side)
    )
    for ticker in universe.option_underlyings:
        for dte in target_dtes:
            for contract_type in (ContractType.CALL, ContractType.PUT):
                for offset in offsets:
                    rows.append(_candidate(
                        ticker,
                        dte,
                        contract_type,
                        spot + offset,
                        identity,
                        spot=spot,
                    ))
                    identity += 1
    return tuple(rows)


def test_forward_liquidity_policy_uses_reviewed_floors_and_preserves_legacy_hashes():
    from scripts.run_realtime_option_pre_session import build_process_plan

    legacy = build_process_plan("stocks_realtime_v1", environ={
        "OPTION_VALUATION_POLICY_FILE": "options/policies/valuation_raw_spot_v2.json"},
        universe=DEVELOPER_FORWARD_UNIVERSE)
    strict = build_process_plan("stocks_realtime_v1", environ={
        "OPTION_POLICY_FILE": "options/policies/developer_forward_liquidity_v2.json",
        "OPTION_VALUATION_POLICY_FILE": "options/policies/valuation_raw_spot_v2.json"},
        universe=DEVELOPER_FORWARD_UNIVERSE)
    assert legacy.discovery_policy_sha256 == "c2e0b422384688378b3dc064e50852be12ea7ed84495117277fe1d0e1d37dd6d"
    assert legacy.manifest_policy.sha256 == "65bf73ea598201d68b55371afbe6024adf08d444ea7bff7a540060a9f825109e"
    assert legacy.manifest_policy.liquidity_rule == "OR"
    assert strict.manifest_policy.minimum_day_volume == 250
    assert strict.manifest_policy.minimum_open_interest == 500
    assert strict.manifest_policy.liquidity_rule == "AND"
    assert strict.manifest_policy.target_dtes == legacy.manifest_policy.target_dtes
    assert strict.manifest_policy.strike_corridor_fraction == legacy.manifest_policy.strike_corridor_fraction
    assert strict.manifest_policy.sha256 != legacy.manifest_policy.sha256


def test_strict_liquidity_manifest_requires_both_floors_for_all_nine_names():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    policy = replace(_developer_policy(), minimum_day_volume=250, minimum_open_interest=500, liquidity_rule="AND")
    candidates = tuple(replace(row, snapshot=replace(row.snapshot, day_volume=250, open_interest=500))
        for row in _candidates(scope.universe, policy.target_dtes, policy.strikes_per_side_per_expiry))
    rejected = (
        _candidate("AAPL", 7, ContractType.CALL, Decimal("101.5"), 50_001, day_volume=249, open_interest=500),
        _candidate("AAPL", 7, ContractType.PUT, Decimal("102.5"), 50_002, day_volume=250, open_interest=499),
    )
    result = build_bounded_option_session_manifest(scope, policy, candidates + rejected, session_date=SESSION_DATE)
    assert result.status is ManifestSelectionStatus.READY
    assert result.selected_contract_count == len(result.policy_admitted_contract_ids) == 144
    assert result.policy_admitted_contract_ids == {row.catalog.contract_id for row in candidates}
    assert all(row.reason_codes == ("LIQUIDITY_FLOOR",) for row in result.dispositions
        if row.contract_id in {candidate.catalog.contract_id for candidate in rejected})


def test_bounded_expiry_profile_uses_actual_listed_dates_without_weakening_liquidity(monkeypatch):
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    policy = replace(_developer_policy(), minimum_day_volume=250, minimum_open_interest=500,
        liquidity_rule="AND", maximum_target_dte_deviation=4)
    candidates = tuple(replace(row, snapshot=replace(row.snapshot, day_volume=250, open_interest=500))
        for row in _candidates(scope.universe, (7, 21, 42, 56), 2))
    result = build_bounded_option_session_manifest(scope, policy, candidates, session_date=SESSION_DATE)
    assert result.status is ManifestSelectionStatus.READY and result.selected_contract_count == 144
    assert {row.target_dte: (row.expiration_date - SESSION_DATE).days for row in result.coverage} == {7: 7, 21: 21, 45: 42, 60: 56}
    exact = build_bounded_option_session_manifest(scope, replace(policy, maximum_target_dte_deviation=0),
        candidates, session_date=SESSION_DATE)
    assert exact.status is ManifestSelectionStatus.BLOCKED
    assert result.policy_admitted_contract_ids == exact.policy_admitted_contract_ids
    from scripts.run_realtime_option_pre_session import build_process_plan
    with pytest.raises(ValueError, match="reviewed forward allocations"):
        build_process_plan("stocks_realtime_v1", environ={}, maximum_target_dte_deviation=4)
    with pytest.raises(ValueError, match="0-4 days"):
        build_process_plan("stocks_realtime_v1", environ={}, universe=DEVELOPER_FORWARD_UNIVERSE, maximum_target_dte_deviation=5)
    from scripts.run_realtime_option_pre_session import parse_args
    monkeypatch.setattr("sys.argv", ["run_realtime_option_pre_session.py", "--database", "stocks_realtime_v1",
        "--confirm-database-name", "stocks_realtime_v1", "--maximum-target-dte-deviation", "4"])
    with pytest.raises(SystemExit):
        parse_args()


def test_three_lane_profile_samples_108_but_retains_admitted_longer_expiries():
    from scripts.run_realtime_option_pre_session import build_process_plan

    process_plan = build_process_plan("stocks_realtime_v1", environ={
        "OPTION_POLICY_FILE": "options/policies/developer_forward_liquidity_v2.json",
        "OPTION_VALUATION_POLICY_FILE": "options/policies/valuation_raw_spot_v2.json"},
        universe=DEVELOPER_FORWARD_UNIVERSE, maximum_target_dte_deviation=4, target_dtes=(7, 21, 45))
    policy = process_plan.manifest_policy
    assert policy.maximum_contracts_per_underlying == 12
    assert policy.liquidity_rule == "AND" and policy.minimum_open_interest == 500 and policy.minimum_day_volume == 250
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    candidates = tuple(replace(row, snapshot=replace(row.snapshot, day_volume=250, open_interest=500))
        for row in _candidates(scope.universe, (7, 21, 42, 56), 2))
    result = build_bounded_option_session_manifest(scope, policy, candidates, session_date=SESSION_DATE)
    assert result.status is ManifestSelectionStatus.READY and result.selected_contract_count == 108
    assert len(result.policy_admitted_contract_ids) == len(candidates) == 144
    assert {row.target_dte: (row.expiration_date - SESSION_DATE).days for row in result.coverage} == {7: 7, 21: 21, 45: 42}
    assert all(row.reason_codes == ("EXPIRY_NOT_SELECTED",) for row in result.dispositions if row.calendar_dte == 56)


def test_liquidity_artifacts_preserve_legacy_encoding_and_pin_new_rule():
    import json
    from realtime.contracts import canonical_sha256
    from realtime.forward_storage import decode_forward_artifact, encode_forward_artifact

    legacy = _developer_policy()
    text, digest = encode_forward_artifact(legacy)
    assert "liquidity_rule" not in text
    assert decode_forward_artifact(text, digest) == legacy
    strict = replace(legacy, minimum_day_volume=250, minimum_open_interest=500, liquidity_rule="AND")
    strict_text, strict_digest = encode_forward_artifact(strict)
    assert json.loads(strict_text)["fields"]["liquidity_rule"] == "AND"
    assert decode_forward_artifact(strict_text, strict_digest) == strict
    incomplete = json.loads(text)
    del incomplete["fields"]["minimum_open_interest"]
    with pytest.raises(ValueError, match="domain fields changed"):
        decode_forward_artifact(json.dumps(incomplete), canonical_sha256(incomplete))


def test_developer_manifest_covers_exact_strategy_lanes_and_disables_o2():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(
        CANARY_UNIVERSE,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )

    result = build_bounded_option_session_manifest(
        scope,
        policy,
        candidates,
        session_date=SESSION_DATE,
    )

    assert result.status is ManifestSelectionStatus.READY
    assert result.manifest is not None
    assert result.selected_contract_count == 8 * 16 == 128
    assert len(result.coverage) == 8 * 4 * 2 == 64
    assert all(row.status == "READY" for row in result.coverage)
    assert all(row.status is ContractSelectionStatus.INCLUDED for row in result.dispositions)
    option_quotes = {
        row.provider_symbol
        for row in result.manifest.members
        if row.asset_class is AssetClass.OPTION and row.channel is MarketChannel.QUOTE
    }
    assert len(option_quotes) == 128
    assert DetectorFamily.O2 in policy.unavailable_detectors
    assert set(policy.required_detectors) >= {
        DetectorFamily.S1,
        DetectorFamily.S2,
        DetectorFamily.O1,
        DetectorFamily.O3,
    }


def test_selection_is_deterministic_and_records_every_exclusion_reason():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = list(_candidates(
        CANARY_UNIVERSE,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    ))
    candidates.extend((
        _candidate("AAPL", 7, ContractType.CALL, Decimal("101.5"), 50_001),
        _candidate(
            "AAPL", 7, ContractType.CALL, Decimal("102.5"), 50_002,
            day_volume=0, open_interest=0,
        ),
        _candidate("AAPL", 7, ContractType.CALL, Decimal("130"), 50_003),
        _candidate("AAPL", 30, ContractType.CALL, Decimal("100"), 50_004),
    ))
    rejected_catalog = _candidate(
        "AAPL", 21, ContractType.PUT, Decimal("103"), 50_005
    )
    candidates.append(replace(
        rejected_catalog,
        catalog=replace(
            rejected_catalog.catalog,
            eligibility_status=CatalogEligibility.REJECTED_UNSUPPORTED,
            exclusion_reasons=("UNSUPPORTED_MULTIPLIER",),
        ),
    ))

    first = build_bounded_option_session_manifest(
        scope, policy, tuple(candidates), session_date=SESSION_DATE
    )
    repeated = build_bounded_option_session_manifest(
        scope, policy, tuple(reversed(candidates)), session_date=SESSION_DATE
    )

    assert first.selection_sha256 == repeated.selection_sha256
    assert first.manifest.sha256 == repeated.manifest.sha256
    excluded = {
        row.contract_id: row.reason_codes
        for row in first.dispositions
        if row.status is ContractSelectionStatus.EXCLUDED
    }
    assert excluded[50_002 + 1] == ("LIQUIDITY_FLOOR",)
    assert excluded[50_003 + 1] == ("OUTSIDE_STRIKE_CORRIDOR",)
    assert excluded[50_001 + 1] == ("SIDE_DEPTH_LIMIT",)
    assert excluded[50_004 + 1] == ("EXPIRY_NOT_SELECTED",)
    assert excluded[50_005 + 1] == ("UNSUPPORTED_MULTIPLIER",)
    assert len(first.dispositions) == len(candidates)


def test_missing_required_side_depth_blocks_partial_manifest():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = tuple(
        row
        for row in _candidates(
            CANARY_UNIVERSE,
            policy.target_dtes,
            policy.strikes_per_side_per_expiry,
        )
        if not (
            row.catalog.underlyer == "AAPL"
            and row.snapshot.calendar_dte == 60
            and row.catalog.contract_type is ContractType.PUT
        )
    )

    result = build_bounded_option_session_manifest(
        scope, policy, candidates, session_date=SESSION_DATE
    )

    assert result.status is ManifestSelectionStatus.BLOCKED
    assert result.manifest is None
    assert result.reason_codes == ("REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE",)
    missing = [
        row for row in result.coverage
        if row.underlyer == "AAPL" and row.target_dte == 60
    ]
    assert {row.contract_type for row in missing} == {ContractType.CALL, ContractType.PUT}
    assert all(row.status == "UNAVAILABLE" for row in missing)


def _advanced_universe(count):
    tickers = tuple(f"T{index:03d}" for index in range(count))
    return UniverseManifest(f"advanced-{count}-underlyings-v1", tickers, tickers)


def test_advanced_initial_budget_selects_800_and_preserves_200_reserved_slots():
    universe = _advanced_universe(50)
    scope = _scope(ProviderTier.MASSIVE_ADVANCED, universe)
    policy = _advanced_policy()
    candidates = _candidates(
        universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
        spot=Decimal("350"),
    )

    result = build_bounded_option_session_manifest(
        scope, policy, candidates, session_date=SESSION_DATE
    )

    assert result.status is ManifestSelectionStatus.READY
    assert result.selected_contract_count == 800
    assert policy.primary_option_quote_contracts == 800
    assert policy.reserved_option_quote_contracts == 200
    assert result.manifest.maximum_option_quote_contracts == 1_000
    option_quotes = {
        row.provider_symbol
        for row in result.manifest.members
        if row.asset_class is AssetClass.OPTION and row.channel is MarketChannel.QUOTE
    }
    option_trades = {
        row.provider_symbol
        for row in result.manifest.members
        if row.asset_class is AssetClass.OPTION and row.channel is MarketChannel.TRADE
    }
    assert len(option_quotes) == 800
    assert option_trades == option_quotes
    assert all(row.selected_count == 8 for row in result.coverage)


def test_advanced_over_budget_plan_blocks_without_silent_underlying_drop():
    universe = _advanced_universe(51)
    scope = _scope(ProviderTier.MASSIVE_ADVANCED, universe)
    policy = _advanced_policy()
    candidates = _candidates(
        universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
        spot=Decimal("350"),
    )

    result = build_bounded_option_session_manifest(
        scope, policy, candidates, session_date=SESSION_DATE
    )

    assert result.status is ManifestSelectionStatus.BLOCKED
    assert result.manifest is None
    assert result.reason_codes == ("PRIMARY_SLOT_BUDGET_EXCEEDED",)
    assert all(
        row.reason_codes == ("PRIMARY_SLOT_BUDGET_EXCEEDED",)
        for row in result.dispositions
    )


def test_tier_detector_contracts_cannot_be_relabelled():
    candidates = _candidates(CANARY_UNIVERSE, (21,), 8)
    advanced_policy = _advanced_policy()
    advanced_disabled = replace(
        advanced_policy,
        required_detectors=tuple(
            detector
            for detector in advanced_policy.required_detectors
            if detector is not DetectorFamily.O2
        ),
        unavailable_detectors=(DetectorFamily.O2,),
    )

    with pytest.raises(ValueError, match="Advanced manifest cannot disable O2"):
        build_bounded_option_session_manifest(
            _scope(ProviderTier.MASSIVE_ADVANCED, CANARY_UNIVERSE),
            advanced_disabled,
            candidates,
            session_date=SESSION_DATE,
        )
