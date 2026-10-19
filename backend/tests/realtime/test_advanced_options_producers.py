from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from test_advanced_options_control_plane import (
    _pins,
    _plan as _control_plane,
)
from test_option_portal_adapter import (
    NOW as MARK_TIME,
    _package,
    _policy,
    _quote,
)
from options.model_inputs import ExpirationValuationCandidate
from realtime.acquisition_ledger import ProviderRequestLane
from realtime.advanced_options_producers import (
    DISCOVERY_ENDPOINT,
    OPEN_INTEREST_ENDPOINT,
    REPAIR_ENDPOINT,
    AdvancedOptionsOpenInterestObservation,
    AdvancedOptionsReferenceClocks,
    AdvancedOptionsRepairWindow,
    AdvancedOptionsSpotEvidence,
    build_advanced_options_oi_facts,
    build_advanced_options_producer_plan,
    build_advanced_options_valuation_batch,
)
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.preflight import ProviderTier
from realtime.reference_facts import ReferenceFactRelation


UTC = timezone.utc
CUTOFF = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
KNOWN = CUTOFF + timedelta(minutes=1)
RATE_ID = UUID("12121212-1212-1212-1212-121212121212")
GAP = UUID("34343434-3434-3434-3434-343434343434")


def _clocks(control=None):
    control = control or _control_plane()
    return AdvancedOptionsReferenceClocks(
        session_date=control.session_date,
        planned_at=control.planned_at,
        market_cutoff=CUTOFF,
        known_at=KNOWN,
        authorized_until=control.provider_budget.authorized_until,
        prior_oi_session=control.session_date - timedelta(days=1),
    )


def _discovery_scope(control):
    return RealtimeOptionDiscoveryScope(
        ProviderTier.MASSIVE_ADVANCED,
        control.profile_id,
        control.profile_sha256,
        control.universe,
    )


def _limits(**changes):
    values = {
        DISCOVERY_ENDPOINT: 5_000_000,
        OPEN_INTEREST_ENDPOINT: 5_000_000,
        REPAIR_ENDPOINT: 5_000_000,
    }
    values.update(changes)
    return values


def _producer_plan(*, repair_windows=()):
    control = _control_plane()
    return build_advanced_options_producer_plan(
        control_plane=control,
        discovery_scope=_discovery_scope(control),
        provider_budget=control.provider_budget,
        clocks=_clocks(control),
        endpoint_response_byte_limits=_limits(),
        repair_windows=repair_windows,
    )


def test_producer_plan_is_deterministic_budget_bound_and_inert(
    monkeypatch,
):
    repair = AdvancedOptionsRepairWindow(
        GAP,
        "SPY",
        CUTOFF - timedelta(minutes=2),
        CUTOFF - timedelta(minutes=1),
        "UNRESOLVED_STREAM_GAP",
    )

    def reject_effect(*_args, **_kwargs):
        raise AssertionError("producer planning attempted a runtime effect")

    monkeypatch.setattr("socket.socket", reject_effect)
    monkeypatch.setattr("subprocess.Popen", reject_effect)
    first = _producer_plan(repair_windows=(repair,))
    repeated = _producer_plan(repair_windows=(repair,))

    assert first == repeated
    assert not first.acquisition_authorized
    assert not first.repair_execution_authorized
    assert tuple(request.endpoint_class for request in first.requests) == (
        DISCOVERY_ENDPOINT,
        OPEN_INTEREST_ENDPOINT,
        REPAIR_ENDPOINT,
    )
    assert tuple(request.lane for request in first.requests) == (
        ProviderRequestLane.OPTIONS_CHAIN,
        ProviderRequestLane.REFERENCE,
        ProviderRequestLane.OPTIONS_ALIGNMENT,
    )
    assert all(
        request.budget_scope_sha256 == first.provider_budget_sha256
        for request in first.requests
    )


def test_producer_plan_rejects_scope_clock_and_budget_drift():
    control = _control_plane()
    wrong_scope = RealtimeOptionDiscoveryScope(
        ProviderTier.MASSIVE_DEVELOPER,
        control.profile_id,
        control.profile_sha256,
        control.universe,
    )
    with pytest.raises(ValueError, match="discovery scope"):
        build_advanced_options_producer_plan(
            control_plane=control,
            discovery_scope=wrong_scope,
            provider_budget=control.provider_budget,
            clocks=_clocks(control),
            endpoint_response_byte_limits=_limits(),
        )
    with pytest.raises(ValueError, match="reference clocks"):
        replace(_clocks(control), known_at=CUTOFF - timedelta(seconds=1))
    with pytest.raises(ValueError, match="exceeds provider budget"):
        build_advanced_options_producer_plan(
            control_plane=control,
            discovery_scope=_discovery_scope(control),
            provider_budget=control.provider_budget,
            clocks=_clocks(control),
            endpoint_response_byte_limits=_limits(
                **{
                    DISCOVERY_ENDPOINT: 30_000_000,
                    OPEN_INTEREST_ENDPOINT: 30_000_000,
                }
            ),
        )


def test_oi_facts_bind_prior_session_original_clocks_and_payload():
    plan = _producer_plan()
    observation = AdvancedOptionsOpenInterestObservation(
        contract_id=101,
        contract_ticker="O:SPY261218C00500000",
        underlyer="SPY",
        open_interest=1_234,
        settlement_session=plan.clocks.prior_oi_session,
        observed_session=plan.clocks.session_date,
        observed_at=plan.clocks.planned_at,
        received_at=plan.clocks.known_at,
        payload_sha256="a" * 64,
    )

    first = build_advanced_options_oi_facts(plan, (observation,))
    repeated = build_advanced_options_oi_facts(plan, (observation,))
    assert first == repeated
    assert first[0].relation is ReferenceFactRelation.OI_BASELINE
    values = dict(first[0].values)
    assert values["settlement_session"] == plan.clocks.prior_oi_session
    assert values["observed_at"] == plan.clocks.planned_at
    assert values["received_at"] == plan.clocks.known_at
    assert values["date_basis"] == "PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION"
    with pytest.raises(ValueError, match="clock or scope"):
        build_advanced_options_oi_facts(
            plan,
            (replace(
                observation,
                settlement_session=plan.clocks.session_date,
            ),),
        )


def _valuation_candidate(spot):
    expiration = date(2026, 11, 20)
    return ExpirationValuationCandidate(
        valuation_date=MARK_TIME.date(),
        expiration_date=expiration,
        spot=spot,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        rate_date=MARK_TIME.date(),
        rate_observation_ids=(RATE_ID,),
        rate_payload_sha256=("b" * 64,),
        dividend_cash_flows=(),
        dividend_coverage_start=MARK_TIME.date(),
        dividend_coverage_end=expiration,
        dividend_source_key="fixture-dividend-coverage",
        dividend_payload_sha256="c" * 64,
        selected_at=MARK_TIME + timedelta(milliseconds=50),
    )


def test_advanced_nbbo_marks_and_local_greeks_retain_exact_inputs():
    policy = _policy()
    package = _package(policy.policy_sha256)
    quotes = {
        1: _quote(
            1,
            package.legs[0].contract_ticker,
            "6.00",
            "6.20",
            10,
            100,
        ),
        2: _quote(
            2,
            package.legs[1].contract_ticker,
            "2.40",
            "2.60",
            11,
            150,
        ),
    }
    spot = Decimal("450")
    spots = {
        leg.instrument_id: AdvancedOptionsSpotEvidence(
            instrument_id=leg.instrument_id,
            spot=spot,
            market_time=MARK_TIME + timedelta(milliseconds=100),
            observed_time=MARK_TIME + timedelta(milliseconds=101),
            payload_sha256=f"{leg.instrument_id + 20:064x}",
        )
        for leg in package.legs
    }
    candidate = _valuation_candidate(spot)
    pins = replace(
        _pins(),
        valuation_policy_sha256=policy.policy_sha256,
    )

    first = build_advanced_options_valuation_batch(
        package=package,
        quotes=quotes,
        spot_evidence=spots,
        valuation_inputs={candidate.expiration_date: candidate},
        checked_at=MARK_TIME + timedelta(milliseconds=200),
        policy=policy,
        pins=pins,
    )
    repeated = build_advanced_options_valuation_batch(
        package=package,
        quotes=quotes,
        spot_evidence=spots,
        valuation_inputs={candidate.expiration_date: candidate},
        checked_at=MARK_TIME + timedelta(milliseconds=200),
        policy=policy,
        pins=pins,
    )

    assert first == repeated
    assert first.status == "READY"
    assert first.package_mark is not None
    assert len(first.greeks) == len(package.legs)
    assert all(row.model_version == "black_scholes_european_v2" for row in first.greeks)
    assert all(row.rate_observation_ids == (RATE_ID,) for row in first.greeks)
    assert all(row.valuation_policy_sha256 == policy.policy_sha256 for row in first.greeks)
    assert all(row.result.failure_reason is None for row in first.greeks)


def test_stale_nbbo_and_spot_clock_skew_remain_explicit():
    policy = _policy()
    package = _package(policy.policy_sha256)
    quotes = {
        1: _quote(1, package.legs[0].contract_ticker, "6", "6.2", 10, 100),
        2: _quote(2, package.legs[1].contract_ticker, "2.4", "2.6", 11, 150),
    }
    pins = replace(
        _pins(),
        valuation_policy_sha256=policy.policy_sha256,
    )
    stale = build_advanced_options_valuation_batch(
        package=package,
        quotes=quotes,
        spot_evidence={},
        valuation_inputs={},
        checked_at=MARK_TIME + timedelta(seconds=2),
        policy=policy,
        pins=pins,
    )
    assert stale.status == "UNAVAILABLE"
    assert stale.reason_codes == ("PACKAGE_LEG_QUOTE_STALE:101",)

    spot = Decimal("450")
    spots = {
        leg.instrument_id: AdvancedOptionsSpotEvidence(
            leg.instrument_id,
            spot,
            MARK_TIME - timedelta(seconds=2),
            MARK_TIME - timedelta(seconds=2) + timedelta(milliseconds=1),
            "d" * 64,
        )
        for leg in package.legs
    }
    candidate = _valuation_candidate(spot)
    with pytest.raises(ValueError, match="references are incomplete"):
        build_advanced_options_valuation_batch(
            package=package,
            quotes=quotes,
            spot_evidence=spots,
            valuation_inputs={candidate.expiration_date: candidate},
            checked_at=MARK_TIME + timedelta(milliseconds=200),
            policy=policy,
            pins=pins,
        )
