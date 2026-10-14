from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5


OPTIONS_TESTS = Path(__file__).resolve().parents[1] / "options"
if str(OPTIONS_TESTS) not in sys.path:
    sys.path.insert(0, str(OPTIONS_TESTS))

from options.calendar import OptionExchangeCalendar
from options.domain import ContractType
from options.dual_origin import StockFirstPolicy
from options.strategies.domain import (
    CandidateKind,
    CandidateLeg,
    CandidateStatus,
    OptionCandidate,
    OptionSide,
    StructureRiskClass,
    StructureType,
)
from options.strategies.payoff import evaluate_terminal_payoff
from realtime.contracts import CANARY_UNIVERSE
from realtime.detector_adapters import (
    run_o1_adapter,
    run_o2_adapter,
    run_o3_adapter,
    run_stock_first_adapter,
)
from realtime.developer_cycle_compute import (
    DEVELOPER_DETECTORS,
    DeveloperCycleComputePlan,
    DeveloperDetectorInvocation,
    DeveloperTickerComputeInput,
    persist_developer_cycle_values,
    produce_developer_cycle_values,
)
from realtime.database_access import RealtimeDatabaseRole
from realtime.portal_projection_repository import PortalReleaseReceipt
from realtime.preflight import ProviderTier
from tests.options.test_dual_origin import NOW, activity_source
from tests.options.test_stock_setup_binding import binding_inputs
from tests.realtime.test_equity_evidence_repository import _definition
from tests.realtime.test_equity_materialization_adapter import (
    COHORT,
    _facts,
    _security,
)
from tests.realtime.test_acquisition_worker import _database


def _confirmation_inputs():
    inputs = binding_inputs()
    clock = inputs["decision_at"]
    market = clock - timedelta(minutes=5)
    source = activity_source(
        security_id=inputs["security"].security_id,
        volume_session=market.date(),
        market_time=market,
        observed_at=clock - timedelta(minutes=2),
        recorded_at=clock - timedelta(minutes=1),
        received_at=clock,
        oi_observed_at=clock - timedelta(minutes=2),
        oi_settlement_session=OptionExchangeCalendar().previous_session(market.date()),
        expiration_date=date(2026, 10, 16),
        expiration_cutoff=clock + timedelta(days=28),
    )
    return inputs, source


def _leg(index, strike, mark, side):
    return CandidateLeg(
        leg_index=index,
        snapshot_id=uuid4(),
        contract_id=index + 1,
        contract_ticker=f"O:TEST{index}",
        side=side,
        ratio=1,
        multiplier=100,
        expiration_date=date(2026, 10, 8),
        strike=Decimal(strike),
        contract_type=ContractType.PUT,
        spot=Decimal("100"),
        time_to_expiration_years=30 / 365,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        model_mark=Decimal(mark),
        local_iv=0.25,
        local_delta=0.30,
        local_gamma=0.02,
        local_theta_per_day=-0.05,
        local_vega_per_vol_point=0.10,
        local_rho_per_rate_point=0.02,
        source_market_time=NOW,
        mark_source="DEVELOPER_ALIGNED_AGG_CLOSE",
        model_version="black_scholes_european_v1",
        quality_flags=(),
        valuation_policy_version="option_valuation_v1",
        valuation_policy_sha256="e" * 64,
    )


def _package_candidate(legs):
    payoff = evaluate_terminal_payoff(legs)
    return OptionCandidate(
        candidate_id=uuid4(),
        identity_sha256="a" * 64,
        matrix_id=uuid4(),
        strategy_name="SPREAD_RANGE_LOCATOR",
        strategy_version="strategy_v1",
        underlyer="AAPL",
        candidate_kind=CandidateKind.MULTI_LEG,
        strategy_archetype="REWARD_RISK",
        persona_tags=("RESEARCH",),
        structure_type=StructureType.PUT_CREDIT_VERTICAL,
        structure_risk_class=StructureRiskClass.DEFINED_RISK_CREDIT,
        expiration_date=legs[0].expiration_date,
        rank=1,
        status=CandidateStatus.SELECTED,
        primary_metric_name=None,
        primary_metric_value=None,
        rank_components={},
        primary_evidence={
            "wall_center": "95",
            "wall_strength": 4.0,
            "wall_open_interest": 5000,
        },
        legs=legs,
        net_premium=payoff.net_premium,
        collateral_required=None,
        capital_at_risk=payoff.maximum_loss,
        maximum_profit=payoff.maximum_profit,
        maximum_loss=payoff.maximum_loss,
        return_on_collateral=None,
        return_on_risk=None,
        breakevens=payoff.breakevens,
        execution_eligibility=None,
        reason_codes=(),
        management_policy_version=None,
        management_policy={},
        policy_sha256="b" * 64,
        model_version="black_scholes_european_v1",
        context_snapshot_id=None,
        iv_context_id=None,
        market_data_time=NOW,
        observed_time=NOW + timedelta(seconds=1),
        valid_until=NOW + timedelta(minutes=15),
    )


def _credit_fixture():
    ordered = ((90, "1", OptionSide.BUY), (95, "3", OptionSide.SELL))
    legs = tuple(
        _leg(index, str(strike), mark, side)
        for index, (strike, mark, side) in enumerate(ordered)
    )
    candidate = _package_candidate(legs)
    snapshots = tuple(
        SimpleNamespace(
            snapshot_id=item.snapshot_id,
            open_interest=1000,
            day_volume=100,
            bid=None,
            ask=None,
        )
        for item in legs
    )
    decision = SimpleNamespace(
        detector_id="O1",
        origin="OPTIONS_FIRST",
        disposition="CONFIRMED",
        direction=1,
        decision_at=NOW + timedelta(seconds=2),
        valid_until=NOW + timedelta(minutes=10),
        activity=SimpleNamespace(episode_id=uuid5(NAMESPACE_URL, "credit-episode")),
        sha256="d" * 64,
    )
    return candidate, decision, snapshots, Decimal("96")


def _detectors(ticker):
    inputs, source = _confirmation_inputs()
    setup = inputs["setup"]
    candidate, decision, snapshots, invalidation = _credit_fixture()
    hashes = {
        detector: uuid5(NAMESPACE_URL, f"detector-input:{ticker}:{detector}").hex * 2
        for detector in DEVELOPER_DETECTORS
    }

    def stock(detector):
        return lambda: run_stock_first_adapter(
            setup,
            source,
            cohort_id=COHORT,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            input_sha256=hashes[detector],
            policy=StockFirstPolicy(
                source_policy_sha256=setup.source_policy.sha256
            ),
            trusted_source_policy=setup.source_policy,
            market_cutoff=inputs["decision_at"],
            decision_at=inputs["decision_at"],
        )

    return (
        DeveloperDetectorInvocation("S1", hashes["S1"], stock("S1")),
        DeveloperDetectorInvocation("S2", hashes["S2"], stock("S2")),
        DeveloperDetectorInvocation(
            "O1",
            hashes["O1"],
            lambda: run_o1_adapter(
                source,
                cohort_id=COHORT,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes["O1"],
                market_cutoff=source.market_time,
                decision_at=source.received_at,
            ),
        ),
        DeveloperDetectorInvocation(
            "O2",
            hashes["O2"],
            lambda: run_o2_adapter(
                object(),
                None,
                cohort_id=COHORT,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes["O2"],
                market_cutoff=source.market_time,
                decision_at=source.received_at,
            ),
        ),
        DeveloperDetectorInvocation(
            "O3",
            hashes["O3"],
            lambda: run_o3_adapter(
                candidate,
                decision,
                cohort_id=COHORT,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                input_sha256=hashes["O3"],
                activity_contract_id=candidate.legs[0].contract_id,
                snapshots=snapshots,
                structural_invalidation=invalidation,
                scheduled_cycle=NOW,
            ),
        ),
    )


def _ticker_input(ticker, index):
    security_id = uuid5(NAMESPACE_URL, f"developer-cycle-security:{ticker}")
    security = replace(
        _security(),
        security_revision_id=uuid5(
            NAMESPACE_URL, f"developer-cycle-security-revision:{ticker}"
        ),
        security_id=security_id,
        ticker=ticker,
    )
    facts = tuple(
        replace(
            fact,
            instrument_id=index,
            revision=replace(
                fact.revision,
                security_id=security_id,
                ticker=ticker,
            ),
        )
        for fact in _facts()
    )
    return DeveloperTickerComputeInput(
        ticker,
        security,
        facts,
        (),
        _detectors(ticker),
    )


def test_cycle_compute_runs_real_stable_functions_for_exact_portal_values():
    tickers = tuple(
        _ticker_input(ticker, index)
        for index, ticker in enumerate(CANARY_UNIVERSE.equity_tickers, start=1)
    )
    checked_at = max(row.bars[-1].revision.bar_end for row in tickers)
    values = produce_developer_cycle_values(DeveloperCycleComputePlan(
        cohort_id=COHORT,
        source_manifest_sha256="e" * 64,
        source_generation=1,
        checked_at=checked_at,
        feature_definition=_definition(),
        tickers=tickers,
    ))

    assert len(values.features) == 8
    assert len(values.screening.rows) == 8
    assert len(values.detector_dataset.evaluations) == 40
    assert values.detector_dataset.detector_ids == DEVELOPER_DETECTORS
    assert len(values.portal_snapshots) == 9
    ticker_pages = {
        row.snapshot_key: row for row in values.portal_snapshots
        if row.snapshot_type == "TICKER_PAGE"
    }
    assert set(ticker_pages) == set(CANARY_UNIVERSE.equity_tickers)
    assert all(
        row.payload["detectors"]["O2"]["status"] == "UNAVAILABLE"
        for row in ticker_pages.values()
    )
    assert all(row.payload["provider_fetch_performed"] is False for row in ticker_pages.values())


def test_cycle_persists_values_under_compute_and_publication_roles():
    tickers = tuple(
        _ticker_input(ticker, index)
        for index, ticker in enumerate(CANARY_UNIVERSE.equity_tickers, start=1)
    )
    checked_at = max(row.bars[-1].revision.bar_end for row in tickers)
    plan = DeveloperCycleComputePlan(
        cohort_id=COHORT,
        source_manifest_sha256="e" * 64,
        source_generation=1,
        checked_at=checked_at,
        feature_definition=_definition(),
        tickers=tickers,
    )
    values = produce_developer_cycle_values(plan)
    compute, compute_connection, compute_cursor = _database()
    publication, publication_connection, publication_cursor = _database(
        role=RealtimeDatabaseRole.PUBLICATION
    )
    writes = []

    def feature_repository(cursor, feature, definition, *, generation):
        assert cursor is compute_cursor
        writes.append(("FEATURE", feature.instrument_id, generation))
        return uuid5(NAMESPACE_URL, f"feature:{feature.instrument_id}")

    def screening_repository(cursor, generation):
        assert cursor is compute_cursor
        writes.append(("SCREENING", len(generation.rows)))

    def detector_repository(cursor, dataset):
        assert cursor is compute_cursor
        writes.append(("DETECTORS", len(dataset.evaluations)))

    def portal_repository(cursor, snapshot):
        assert cursor is publication_cursor
        writes.append(("PORTAL", snapshot.snapshot_type, snapshot.snapshot_key))

    stored = persist_developer_cycle_values(
        compute,
        publication,
        plan,
        values,
        feature_repository=feature_repository,
        screening_repository=screening_repository,
        detector_repository=detector_repository,
        portal_repository=portal_repository,
    )

    assert len(stored.feature_snapshot_ids) == 8
    assert len(stored.portal_snapshot_ids) == 9
    assert writes[:8] == [
        ("FEATURE", index, 1) for index in range(1, 9)
    ]
    assert writes[8:10] == [("SCREENING", 8), ("DETECTORS", 40)]
    assert len(writes[10:]) == 9
    assert compute_connection.commit.call_count == 1
    assert publication_connection.commit.call_count == 1

    releases = []
    receipt = PortalReleaseReceipt(
        uuid5(NAMESPACE_URL, "receipt"), COHORT, ProviderTier.MASSIVE_DEVELOPER, checked_at, "9" * 64,
    )

    def release_repository(cursor, active_receipt, snapshots):
        assert cursor is publication_cursor and active_receipt is receipt
        releases.append((len(snapshots), publication_connection.commit.call_count))

    persist_developer_cycle_values(
        compute,
        publication,
        plan,
        values,
        feature_repository=feature_repository,
        screening_repository=screening_repository,
        detector_repository=detector_repository,
        portal_repository=portal_repository,
        receipt=receipt,
        release_repository=release_repository,
    )
    assert releases == [(9, 1)]
    assert publication_connection.commit.call_count == 2